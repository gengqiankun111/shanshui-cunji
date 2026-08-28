//! 极简 JSON-over-TCP RPC（design 9.8 `cluster.internal_rpc_port`，阶段 2）。
//!
//! 网关 ↔ 分片节点之间的内部通信协议，与单机内核一致采用同步 `std::net`
//! （不引入异步运行时依赖）：
//! - **帧格式**：`[u32 LE 长度][JSON]`，每帧一个请求/响应；
//! - **请求**：`{"method": "...", "params": {...}}`；
//! - **响应**：`{"ok": bool, "result": {...}, "error": "..."}`；
//! - **服务端**：每连接一个线程（简单、同步）；handler 注册表按 method 分发；
//! - **分片节点处理器**：`register_shard_handlers` 将 Engine 的
//!   put / get / 倒排 chunk 检索 / ping 暴露为 RPC 方法（单机内核零修改复用）。
//!
//! 安全：全程无 unsafe；帧长度上限防内存放大（64MB）；客户端读写超时防挂死。

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::info;

use crate::engine::Engine;
use crate::error::{Error, Result};

/// 单帧长度上限（64MB），防恶意长度放大内存。
const MAX_FRAME: usize = 64 * 1024 * 1024;
/// 客户端读写超时。
const CLIENT_TIMEOUT_MS: u64 = 15_000;

/// RPC 请求帧。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

/// RPC 响应帧。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub ok: bool,
    #[serde(default)]
    pub result: Value,
    #[serde(default)]
    pub error: Option<String>,
}

/// RPC 处理器：`params` → 结果 / 错误消息。
pub type Handler = Arc<dyn Fn(&Value) -> std::result::Result<Value, String> + Send + Sync>;

/// 极简 RPC 服务端（线程池：每连接一个线程）。
pub struct RpcServer {
    /// 处理器注册表（`pub(crate)` 供测试 / 网关端到端复用）。
    pub(crate) handlers: Mutex<HashMap<String, Handler>>,
}

impl Default for RpcServer {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcServer {
    pub fn new() -> Self {
        Self {
            handlers: Mutex::new(HashMap::new()),
        }
    }

    /// 注册方法处理器。
    pub fn register<F>(&self, method: &str, f: F)
    where
        F: Fn(&Value) -> std::result::Result<Value, String> + Send + Sync + 'static,
    {
        self.handlers.lock().unwrap().insert(method.to_string(), Arc::new(f));
    }

    /// 阻塞监听；每连接一个线程处理（循环读取帧，直到断开或出错）。
    pub fn serve(&self, addr: &str) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        let local = listener.local_addr()?;
        info!("内部 RPC 服务已启动: {local}");
        let handlers = self.handlers.lock().unwrap().clone();
        for stream in listener.incoming() {
            match stream {
                Ok(s) => {
                    let handlers = handlers.clone();
                    std::thread::spawn(move || handle_connection(handlers, s));
                }
                Err(e) => tracing::warn!("RPC 接受连接失败: {e}"),
            }
        }
        Ok(())
    }
}

/// 处理单连接：循环读请求帧 → 分发 → 写响应帧。
/// `pub(crate)`：供网关端到端测试直接启动分片节点。
pub(crate) fn handle_connection(handlers: HashMap<String, Handler>, mut stream: TcpStream) {
    loop {
        let payload = match read_frame(&mut stream) {
            Ok(p) => p,
            Err(_) => break, // 连接关闭 / 超时 / 坏帧
        };
        let response = match serde_json::from_slice::<RpcRequest>(&payload) {
            Ok(req) => {
                let handler = handlers.get(&req.method).cloned();
                match handler {
                    Some(f) => match f(&req.params) {
                        Ok(result) => RpcResponse {
                            ok: true,
                            result,
                            error: None,
                        },
                        Err(e) => RpcResponse {
                            ok: false,
                            result: Value::Null,
                            error: Some(e),
                        },
                    },
                    None => RpcResponse {
                        ok: false,
                        result: Value::Null,
                        error: Some(format!("未知方法: {}", req.method)),
                    },
                }
            }
            Err(e) => RpcResponse {
                ok: false,
                result: Value::Null,
                error: Some(format!("请求解析失败: {e}")),
            },
        };
        let out = match serde_json::to_vec(&response) {
            Ok(v) => v,
            Err(_) => break,
        };
        if write_frame(&mut stream, &out).is_err() {
            break;
        }
    }
}

/// 读取一帧：`[u32 LE 长度][payload]`。
fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(Error::Rpc(format!("帧长度超限: {len}")));
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

/// 写一帧：`[u32 LE 长度][payload]`。
fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_FRAME {
        return Err(Error::Rpc("帧长度超限".into()));
    }
    stream.write_all(&(payload.len() as u32).to_le_bytes())?;
    stream.write_all(payload)?;
    stream.flush()?;
    Ok(())
}

/// 阻塞 RPC 客户端（单连接、请求-响应串行）。
pub struct RpcClient {
    stream: TcpStream,
}

impl RpcClient {
    pub fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_read_timeout(Some(std::time::Duration::from_millis(CLIENT_TIMEOUT_MS)))?;
        stream.set_write_timeout(Some(std::time::Duration::from_millis(CLIENT_TIMEOUT_MS)))?;
        Ok(Self { stream })
    }

    /// 发起一次调用，返回 result。
    pub fn call(&mut self, method: &str, params: Value) -> Result<Value> {
        let req = RpcRequest {
            method: method.to_string(),
            params,
        };
        let payload = serde_json::to_vec(&req)
            .map_err(|e| Error::Rpc(format!("请求序列化失败: {e}")))?;
        write_frame(&mut self.stream, &payload)?;
        let resp = read_frame(&mut self.stream)?;
        let resp: RpcResponse = serde_json::from_slice(&resp)
            .map_err(|e| Error::Rpc(format!("响应解析失败: {e}")))?;
        if resp.ok {
            Ok(resp.result)
        } else {
            Err(Error::Rpc(resp.error.unwrap_or_else(|| "未知错误".into())))
        }
    }
}

// ---------------------------------------------------------------------------
// 分片节点 RPC 处理器（Engine 零修改复用，design 9.3）
// ---------------------------------------------------------------------------

/// 将 Engine 的分片能力注册为 RPC 方法（Arc<Mutex<Engine>> 供跨线程安全访问）：
/// - `shard.put {docid, data, terms}` → `{"ok": true}`
/// - `shard.get {docid}` → `{"found": bool, "data": "..."}`
/// - `shard.search_docids {term}` → `{"docids": [...]}`（本节点倒排 Chunk）
/// - `shard.ping` → `{"pong": true}`（主从心跳 / 健康探活）
pub fn register_shard_handlers(server: &RpcServer, engine: Arc<Mutex<Engine>>) {
    server.register("shard.ping", move |_params| {
        Ok(json!({"pong": true, "node_id": "?"}))
    });

    server.register("shard.put", {
        let engine = engine.clone();
        move |params| {
            let docid = params["docid"]
                .as_u64()
                .ok_or_else(|| "shard.put 缺少 docid".to_string())?;
            let data = params["data"]
                .as_str()
                .ok_or_else(|| "shard.put 缺少 data".to_string())?
                .to_string();
            let terms: Vec<String> = params["terms"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|t| t.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            let term_refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            let mut eng = engine.lock().unwrap();
            eng.put(docid, data.into_bytes(), &term_refs)
                .map_err(|e| e.to_string())?;
            Ok(json!({"ok": true}))
        }
    });

    server.register("shard.get", {
        let engine = engine.clone();
        move |params| {
            let docid = params["docid"]
                .as_u64()
                .ok_or_else(|| "shard.get 缺少 docid".to_string())?;
            let mut eng = engine.lock().unwrap();
            match eng.get(docid).map_err(|e| e.to_string())? {
                Some(bytes) => Ok(json!({
                    "found": true,
                    "data": String::from_utf8_lossy(&bytes).into_owned(),
                })),
                None => Ok(json!({"found": false})),
            }
        }
    });

    server.register("shard.search_docids", {
        let engine = engine.clone();
        move |params| {
            let term = params["term"]
                .as_str()
                .ok_or_else(|| "shard.search_docids 缺少 term".to_string())?;
            let eng = engine.lock().unwrap();
            let posting = eng.inverted_posting(term).map_err(|e| e.to_string())?;
            let docids: Vec<u32> = posting.iter().collect();
            Ok(json!({"docids": docids}))
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_roundtrip_over_tcp() {
        let server = RpcServer::new();
        server.register("echo.add", |p| {
            let a = p["a"].as_i64().unwrap_or(0);
            let b = p["b"].as_i64().unwrap_or(0);
            Ok(json!({"sum": a + b}))
        });
        server.register("echo.fail", |_| Err("故意的失败".into()));

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handlers = server.handlers.lock().unwrap().clone();
        let _thread = std::thread::spawn(move || {
            for s in listener.incoming() {
                if let Ok(s) = s {
                    let h = handlers.clone();
                    std::thread::spawn(move || handle_connection(h, s));
                }
            }
        });

        let mut client = RpcClient::connect(&addr).unwrap();
        let r = client.call("echo.add", json!({"a": 2, "b": 3})).unwrap();
        assert_eq!(r["sum"], 5);
        // 多请求复用连接
        let r = client.call("echo.add", json!({"a": 10, "b": 20})).unwrap();
        assert_eq!(r["sum"], 30);
        // 处理器内错误 → Rpc 错误
        let err = client.call("echo.fail", json!({})).unwrap_err();
        assert!(matches!(err, Error::Rpc(_)));
        assert!(err.to_string().contains("故意的失败"));
        // 未知方法 → Rpc 错误
        let err = client.call("nope.method", json!({})).unwrap_err();
        assert!(err.to_string().contains("未知方法"));
    }

    #[test]
    fn rpc_client_connect_refused() {
        // 无人监听的端口
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        assert!(RpcClient::connect(&addr.to_string()).is_err());
    }

    #[test]
    fn shard_handlers_roundtrip_via_engine() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let engine = Arc::new(Mutex::new(Engine::open(dir.path(), &cfg).unwrap()));

        let server = RpcServer::new();
        register_shard_handlers(&server, engine);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handlers = server.handlers.lock().unwrap().clone();
        std::thread::spawn(move || {
            for s in listener.incoming() {
                if let Ok(s) = s {
                    let h = handlers.clone();
                    std::thread::spawn(move || handle_connection(h, s));
                }
            }
        });

        let mut client = RpcClient::connect(&addr).unwrap();
        // ping
        assert_eq!(client.call("shard.ping", json!({})).unwrap()["pong"], true);
        // put + get
        let terms = json!(["status=active", "city=beijing"]);
        client
            .call("shard.put", json!({"docid": 1001, "data": "{\"status\":\"active\"}", "terms": terms}))
            .unwrap();
        let r = client.call("shard.get", json!({"docid": 1001})).unwrap();
        assert_eq!(r["found"], true);
        assert!(r["data"].as_str().unwrap().contains("active"));
        // 未命中
        let r = client.call("shard.get", json!({"docid": 9999})).unwrap();
        assert_eq!(r["found"], false);
        // 倒排 chunk 检索（本地 posting）
        let r = client.call("shard.search_docids", json!({"term": "status=active"})).unwrap();
        assert_eq!(r["docids"].as_array().unwrap(), &[json!(1001)]);
        let r = client.call("shard.search_docids", json!({"term": "status=pending"})).unwrap();
        assert!(r["docids"].as_array().unwrap().is_empty());
    }
}
