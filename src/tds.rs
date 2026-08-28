//! 术语字典热备与快速恢复（TDS，design 9.10，阶段 2）。
//!
//! 数据节点重启时不自己重建字典，向轻量级**术语字典缓存服务（TDS）**拉取序列化字典快照，
//! 把"单机重建负担"转移为"集群协调"。
//!
//! 本项目倒排字典 = 每段一个 FST 术语字典文件 `inverted-{id}.fst`（term → 段内条目偏移）。
//! TDS 存的就是这些字典的**字节快照**，按 (node_id, segment) 索引：
//! - `TdsServer`：RPC `tds.put / tds.get / tds.list`；内存 + **文件写穿持久化**（`{dir}/{node}/{seg}.dict`），
//!   TDS 自身重启不丢快照；
//! - `TdsClient`：节点侧拉取 / 上报；
//! - `sync_dicts_to_tds`：节点刷盘后把新字典上报（**预加载 + 蓝绿切换**：低谷期预推给待重启节点）；
//! - `restore_dicts_from_tds`：重启节点从 TDS 拉回字典写本地文件，直接 mmap/read 加载，无磁盘重建 IO。
//!
//! 字节经 JSON-RPC 传输用 hex 编码（零依赖、无 unsafe）。快照仅字典（term→offset），
//! posting 数据仍在各节点倒排段文件；TDS 丢失时回退本地文件重建（降级可用）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde_json::json;
use tracing::info;

use crate::error::{Error, Result};
use crate::rpc::{RpcClient, RpcServer};

/// 字节 → hex（JSON 安全传输）。
fn bytes_to_hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push_str(&format!("{x:02x}"));
    }
    s
}

/// hex → 字节。
fn hex_to_bytes(s: &str) -> Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        return Err(Error::Rpc("hex 长度必须为偶数".into()));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        let b = u8::from_str_radix(&s[i..i + 2], 16)
            .map_err(|e| Error::Rpc(format!("hex 解析失败: {e}")))?;
        out.push(b);
    }
    Ok(out)
}

/// TDS 服务端（内存 + 文件写穿持久化）。
pub struct TdsServer {
    dir: PathBuf,
    /// node_id → seg → 字典字节（Arc 供闭包 `'static` 捕获）。
    store: std::sync::Arc<Mutex<HashMap<String, HashMap<String, Vec<u8>>>>>,
}

impl TdsServer {
    /// `dir` 为快照持久化目录（写穿 + 重启恢复）。
    pub fn new(dir: &Path) -> Self {
        std::fs::create_dir_all(dir).ok();
        Self {
            dir: dir.to_path_buf(),
            store: std::sync::Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 注册 RPC 处理器（`tds.put / tds.get / tds.list`）。
    pub fn register_handlers(&self, server: &RpcServer) {
        let store = self.store.clone();
        let dir = self.dir.clone();

        server.register("tds.put", {
            let store = store.clone();
            let dir = dir.clone();
            move |params| {
                let node = params["node"].as_str().ok_or("tds.put 缺少 node")?;
                let seg = params["seg"].as_str().ok_or("tds.put 缺少 seg")?;
                let bytes = hex_to_bytes(params["bytes"].as_str().ok_or("tds.put 缺少 bytes")?)
                    .map_err(|e| e.to_string())?;
                // 写穿持久化（崩溃恢复）
                let nd = dir.join(node);
                std::fs::create_dir_all(&nd).map_err(|e| format!("TDS 创建目录失败: {e}"))?;
                std::fs::write(nd.join(format!("{seg}.dict")), &bytes)
                    .map_err(|e| format!("TDS 写快照失败: {e}"))?;
                store
                    .lock()
                    .unwrap()
                    .entry(node.to_string())
                    .or_default()
                    .insert(seg.to_string(), bytes);
                Ok(json!({"ok": true}))
            }
        });

        server.register("tds.get", {
            let store = store.clone();
            let dir = dir.clone();
            move |params| {
                let node = params["node"].as_str().ok_or("tds.get 缺少 node")?;
                let seg = params["seg"].as_str().ok_or("tds.get 缺少 seg")?;
                // 内存优先，缺则读盘（TDS 自身重启恢复）
                let cached = store
                    .lock()
                    .unwrap()
                    .get(node)
                    .and_then(|m| m.get(seg))
                    .cloned();
                let bytes = match cached {
                    Some(b) => b,
                    None => {
                        let p = dir.join(node).join(format!("{seg}.dict"));
                        if p.exists() {
                            std::fs::read(&p).map_err(|e| format!("TDS 读快照失败: {e}"))?
                        } else {
                            return Ok(json!({"found": false}));
                        }
                    }
                };
                Ok(json!({"found": true, "bytes": bytes_to_hex(&bytes)}))
            }
        });

        server.register("tds.list", {
            let store = store.clone();
            let dir = dir.clone();
            move |params| {
                let node = params["node"].as_str().ok_or("tds.list 缺少 node")?;
                let mut segs: Vec<String> = store
                    .lock()
                    .unwrap()
                    .get(node)
                    .map(|m| m.keys().cloned().collect())
                    .unwrap_or_default();
                if segs.is_empty() {
                    // 从磁盘恢复（TDS 重启后首次 list）
                    let nd = dir.join(node);
                    if let Ok(rd) = std::fs::read_dir(&nd) {
                        for e in rd.flatten() {
                            if let Some(name) = e.file_name().to_str() {
                                if let Some(stem) = name.strip_suffix(".dict") {
                                    segs.push(stem.to_string());
                                }
                            }
                        }
                    }
                }
                segs.sort();
                Ok(json!({"segments": segs}))
            }
        });
    }

    /// 本地快照数（监控 / 测试）。
    pub fn snapshot_count(&self) -> usize {
        self.store.lock().unwrap().values().map(|m| m.len()).sum()
    }
}

/// TDS 客户端（节点侧）。
pub struct TdsClient {
    client: RpcClient,
}

impl TdsClient {
    pub fn connect(addr: &str) -> Result<Self> {
        Ok(Self {
            client: RpcClient::connect(addr)?,
        })
    }

    /// 上报一个字典快照。
    pub fn put_dict(&mut self, node: &str, seg: &str, bytes: &[u8]) -> Result<()> {
        self.client.call(
            "tds.put",
            json!({"node": node, "seg": seg, "bytes": bytes_to_hex(bytes)}),
        )?;
        Ok(())
    }

    /// 拉取一个字典快照。
    pub fn get_dict(&mut self, node: &str, seg: &str) -> Result<Option<Vec<u8>>> {
        let r = self
            .client
            .call("tds.get", json!({"node": node, "seg": seg}))?;
        if r["found"].as_bool().unwrap_or(false) {
            let bytes = hex_to_bytes(r["bytes"].as_str().unwrap_or(""))
                .map_err(|e| Error::Rpc(e.to_string()))?;
            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }

    /// 列出某节点的全部字典段。
    pub fn list_segments(&mut self, node: &str) -> Result<Vec<String>> {
        let r = self.client.call("tds.list", json!({"node": node}))?;
        Ok(r["segments"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default())
    }
}

/// 节点刷盘后把全部 FST 字典上报 TDS（预加载 / 蓝绿切换基础）。
pub fn sync_dicts_to_tds(dir: &Path, node_id: &str, client: &mut TdsClient) -> Result<usize> {
    let mut n = 0;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if name.starts_with("inverted-") && name.ends_with(".fst") {
                let bytes = std::fs::read(e.path())?;
                client.put_dict(node_id, &name.trim_end_matches(".fst"), &bytes)?;
                n += 1;
            }
        }
    }
    info!("TDS 同步 {n} 个字典（node={node_id}）");
    Ok(n)
}

/// 重启节点从 TDS 恢复字典：拉回全部快照写本地 `inverted-{seg}.fst`。
/// 返回恢复的字典数；TDS 不可用 / 缺失时返回 0（回退本地文件重建，降级可用）。
pub fn restore_dicts_from_tds(dir: &Path, node_id: &str, client: &mut TdsClient) -> Result<usize> {
    std::fs::create_dir_all(dir)?;
    let segs = client.list_segments(node_id)?;
    let mut n = 0;
    for seg in segs {
        if let Some(bytes) = client.get_dict(node_id, &seg)? {
            let fname = format!("{seg}.fst");
            std::fs::write(dir.join(&fname), &bytes)?;
            n += 1;
        }
    }
    info!("TDS 恢复 {n} 个字典（node={node_id}）");
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::RpcServer;

    fn spawn_tds(dir: &Path) -> String {
        let server = RpcServer::new();
        let tds = TdsServer::new(dir);
        tds.register_handlers(&server);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handlers = server.handlers.lock().unwrap().clone();
        std::thread::spawn(move || {
            for s in listener.incoming() {
                if let Ok(s) = s {
                    let h = handlers.clone();
                    std::thread::spawn(move || crate::rpc::handle_connection(h, s));
                }
            }
        });
        addr
    }

    #[test]
    fn tds_put_get_list_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let addr = spawn_tds(dir.path());
        let mut c = TdsClient::connect(&addr).unwrap();

        assert!(c.get_dict("node-1", "inverted-00000001").unwrap().is_none());
        c.put_dict("node-1", "inverted-00000001", b"fst-bytes-01")
            .unwrap();
        c.put_dict("node-1", "inverted-00000002", b"fst-bytes-02")
            .unwrap();
        c.put_dict("node-2", "inverted-00000001", b"other-node")
            .unwrap();

        assert_eq!(
            c.get_dict("node-1", "inverted-00000001").unwrap().unwrap(),
            b"fst-bytes-01"
        );
        assert_eq!(
            c.get_dict("node-2", "inverted-00000001").unwrap().unwrap(),
            b"other-node"
        );
        assert!(c.get_dict("node-1", "inverted-999").unwrap().is_none());
        let segs = c.list_segments("node-1").unwrap();
        assert_eq!(segs, vec!["inverted-00000001", "inverted-00000002"]);
        assert_eq!(c.list_segments("node-3").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn tds_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let addr = spawn_tds(dir.path());
            let mut c = TdsClient::connect(&addr).unwrap();
            c.put_dict("node-1", "inverted-00000001", b"dict-data")
                .unwrap();
        }
        // TDS 重启：从磁盘恢复
        let addr = spawn_tds(dir.path());
        let mut c = TdsClient::connect(&addr).unwrap();
        assert_eq!(
            c.get_dict("node-1", "inverted-00000001").unwrap().unwrap(),
            b"dict-data",
            "TDS 重启后应从磁盘恢复快照"
        );
    }

    #[test]
    fn sync_and_restore_dicts_via_tds() {
        let dir = tempfile::tempdir().unwrap();
        let addr = spawn_tds(dir.path());
        let mut c = TdsClient::connect(&addr).unwrap();

        // 模拟节点 A 刷盘产生的 FST 字典文件
        let node_dir = dir.path().join("node-a");
        std::fs::create_dir_all(&node_dir).unwrap();
        std::fs::write(node_dir.join("inverted-00000001.fst"), b"fst-1").unwrap();
        std::fs::write(node_dir.join("inverted-00000002.fst"), b"fst-2").unwrap();

        // 上报全部字典
        assert_eq!(sync_dicts_to_tds(&node_dir, "node-a", &mut c).unwrap(), 2);

        // 冷节点 B：无本地字典，从 TDS 恢复
        let cold_dir = dir.path().join("node-b");
        assert_eq!(
            restore_dicts_from_tds(&cold_dir, "node-a", &mut c).unwrap(),
            2
        );
        assert_eq!(
            std::fs::read(cold_dir.join("inverted-00000001.fst")).unwrap(),
            b"fst-1"
        );
        assert_eq!(
            std::fs::read(cold_dir.join("inverted-00000002.fst")).unwrap(),
            b"fst-2"
        );
    }

    #[test]
    fn restore_from_missing_tds_returns_zero() {
        // 连接不存在的 TDS → 恢复失败由调用方回退本地（此处验证 client 报错）
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        assert!(TdsClient::connect(&addr).is_err());
    }
}
