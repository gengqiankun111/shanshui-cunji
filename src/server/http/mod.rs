//! HTTP-JSON 服务（development 步骤 15 / 5.12，design 5.12）。
//!
//! 基于 `std::net` 的最小 HTTP/1.1 实现（不引入异步运行时依赖，与同步内核一致）。
//! 单机 MVP：串行处理连接、无鉴权/多租户；CLI 与 HTTP 共享同一内核调用路径。
//!
//! 接口（对齐 Readme「HTTP-JSON 接口」）：
//! - `POST /put` `{"docid":1001,"status":"active","type":"order",...}` → 写入
//!   （文档原样存储；字符串字段值自动作为倒排词条）
//! - `GET /get?docid=1001` → `{"docid":1001,"value":{...}}`
//! - `GET /search?filter=...` → `{"total":N,"rows":[{"docid":D,"value":{...}},...]}`
//! - `GET /range?start=S&end=E` → 同上（主键范围）
//! - `POST /delete` `{"docid":1001}` 或 `GET /delete?docid=1001` → `{"ok":true}`
//!
//! filter 语法（MVP 子集）：`field=value`，多条件用 ` AND ` 连接（位图交集）；
//! `docid=...` 走主键点查。
//!
//! 目录化（reconstruct）：HTTP 网关按主题拆为 `http/` 包——本文件（mod.rs）为网关
//! 入口（serve/监听/路由 route_request/响应 + pub use 汇总）；子模块：saga_api.rs
//! （SAGA 网关端点 + 对账线程）、doc_api.rs（文档端点 + 查询执行）、admin_api.rs
//! （管理端点）、json.rs（参数/filter/词条解析簇）、tokenize.rs（分词工具簇）、
//! tests.rs（网关端到端测试）。公开面（server/mod.rs 的 `pub use http::{...}`）不变。

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use crate::engine::Engine;
use crate::error::{Error, Result};

mod admin_api;
mod doc_api;
mod json;
mod saga_api;
mod tokenize;
#[cfg(test)]
mod tests;

// ---- 兼容 re-export：http 根公开面与拆分前一致（server/mod.rs 的 pub use http::{...} 不变）----
pub use doc_api::{execute_count, execute_filter, execute_filter_paged, execute_group_by, execute_spec};
pub use json::{
    extract_terms, extract_terms_filtered, extract_terms_with_fulltext,
    extract_terms_with_fulltext_seg, parse_filter, url_decode,
};
pub use tokenize::{fulltext_terms, fulltext_terms_seg, tokenize, tokenize_bigram, tokenize_seg};

// 拆分后路由/入口所需的子模块端点（原同文件私有函数跨文件可见性升至 pub(crate)）
use self::admin_api::{handle_admin_status, handle_metrics};
use self::doc_api::{
    handle_count, handle_delete, handle_explain, handle_fulltext, handle_get, handle_group_by,
    handle_join, handle_patch, handle_put, handle_range, handle_search, handle_sql,
};
use self::saga_api::{
    handle_saga_compensate, handle_saga_start, handle_saga_status, spawn_reconciler,
};

/// SAGA 网关协调器共享句柄（13.7 对账线程与请求处理串行共享）。
type SagaShared = Arc<Mutex<crate::saga::SagaCoordinator>>;
/// SAGA 步骤定义缓存（tx_id → /saga/start 原始 steps JSON；对账重试重建步骤用）。
type SagaStepsCache = Arc<Mutex<HashMap<String, Value>>>;

/// 对账周期（13.7，秒）。
const SAGA_RECONCILE_INTERVAL_SECS: u64 = 60;
/// Executing 挂起阈值（13.7，毫秒）。
const SAGA_STALL_MS: u64 = 60_000;
/// 补偿重试指数退避上限（13.7，毫秒）。
const SAGA_MAX_BACKOFF_MS: u64 = 300_000;

/// 当前纪元毫秒（13.7 对账时间基准，与 saga.rs 同源）。
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 启动 HTTP 服务（阻塞运行，进程终止即退出）。串行处理连接（MVP）。
/// `broadcast`：小表广播 JOIN 选项（design 19.3，阶段 3），None 表示关闭广播。
/// SAGA 网关（Ex-2.5）：协调器持久化目录 = `{data_dir}/saga`，`/saga/*` 端点由此服务；
/// 13.7：spawn 后台对账线程（Failed/Compensating 自动续补偿、Executing 挂起检测）。
pub fn serve(
    engine: &mut Engine,
    addr: &str,
    broadcast: Option<crate::join::JoinBroadcast>,
) -> Result<()> {
    // Ex-7.2：server 主线程绑网络核（绑定失败忽略——单核/受限环境 no-op）
    crate::affinity::bind_current(&engine.network_cores());
    let saga_dir = engine.data_dir().join("saga");
    let saga: SagaShared = Arc::new(Mutex::new(crate::saga::SagaCoordinator::open(&saga_dir)?));
    let steps_cache: SagaStepsCache = Arc::new(Mutex::new(HashMap::new()));
    spawn_reconciler(saga.clone(), steps_cache.clone());
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    tracing::info!("HTTP-JSON 服务已启动: http://{local}（SAGA 网关目录 {}）", saga_dir.display());
    serve_listener(engine, listener, broadcast, Some(saga), Some(steps_cache))
}

/// 接受连接并分发请求（供 `serve` 与测试复用）。
fn serve_listener(
    engine: &mut Engine,
    listener: TcpListener,
    broadcast: Option<crate::join::JoinBroadcast>,
    saga: Option<SagaShared>,
    steps_cache: Option<SagaStepsCache>,
) -> Result<()> {
    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("接受连接失败: {e}");
                continue;
            }
        };
        if let Err(e) =
            handle_connection(engine, &mut stream, broadcast, saga.as_ref(), steps_cache.as_ref())
        {
            tracing::warn!("请求处理失败: {e}");
        }
    }
    Ok(())
}

/// 处理单个 HTTP 请求：读取 → 路由 → 响应。
fn handle_connection(
    engine: &mut Engine,
    stream: &mut TcpStream,
    broadcast: Option<crate::join::JoinBroadcast>,
    saga: Option<&SagaShared>,
    steps_cache: Option<&SagaStepsCache>,
) -> Result<()> {
    let (method, path, query, body) = read_http_request(stream)?;
    let (status, payload) =
        route_request(engine, &method, &path, &query, &body, broadcast, saga, steps_cache);
    write_http_response(stream, status, &payload)
}

// ---------------------------------------------------------------------------
// 请求解析
// ---------------------------------------------------------------------------

/// 读取 HTTP 请求：请求行 + 头部 + Content-Length 对应的 body。
fn read_http_request(stream: &mut TcpStream) -> Result<(String, String, String, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let mut header_end = None;
    loop {
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            header_end = Some(pos);
            break;
        }
        if buf.len() > 64 * 1024 {
            return Err(Error::Unsupported("请求头部过大".into()));
        }
    }
    let Some(end) = header_end else {
        return Err(Error::Unsupported("未收到完整请求头".into()));
    };
    let header = String::from_utf8_lossy(&buf[..end]).to_string();
    let mut lines = header.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.clone(), String::new()),
    };

    let mut content_length = 0usize;
    for line in lines {
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            content_length = v.trim().parse().unwrap_or(0);
        }
    }

    let mut body = buf[end + 4..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);
    Ok((method, path, query, body))
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

// ---------------------------------------------------------------------------
// 路由与处理器
// ---------------------------------------------------------------------------

fn route_request(
    engine: &mut Engine,
    method: &str,
    path: &str,
    query: &str,
    body: &[u8],
    broadcast: Option<crate::join::JoinBroadcast>,
    saga: Option<&SagaShared>,
    steps_cache: Option<&SagaStepsCache>,
) -> (u16, String) {
    match (method, path) {
        ("POST", "/put") => handle_put(engine, body),
        ("POST", "/patch") => handle_patch(engine, body),
        ("GET", "/get") => handle_get(engine, query),
        ("GET", "/search") => handle_search(engine, query),
        ("GET", "/sql") => handle_sql(engine, query),
        ("GET", "/fulltext") => handle_fulltext(engine, query),
        ("GET", "/range") => handle_range(engine, query),
        ("GET", "/count") => handle_count(engine, query),
        ("GET", "/groupby") => handle_group_by(engine, query),
        ("GET", "/join") => handle_join(engine, query, broadcast),
        ("GET", "/admin/status") => handle_admin_status(engine),
        ("GET", "/metrics") => handle_metrics(engine),
        ("GET", "/explain") => handle_explain(engine, query),
        ("POST", "/delete") => handle_delete(engine, body, query),
        ("GET", "/delete") => handle_delete(engine, body, query),
        // Ex-2.5 SAGA 网关（无协调器挂载时 501）
        ("POST", "/saga/start") => handle_saga_start(saga, steps_cache, body),
        ("GET", "/saga/status") => handle_saga_status(saga, query),
        ("POST", "/saga/compensate") => handle_saga_compensate(saga, body),
        _ => (
            404,
            json!({"error": format!("接口不存在: {method} {path}")}).to_string(),
        ),
    }
}

// ---------------------------------------------------------------------------
// 响应
// ---------------------------------------------------------------------------

fn write_http_response(stream: &mut TcpStream, status: u16, payload: &str) -> Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        _ => "OK",
    };
    let body = payload.as_bytes();
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()?;
    Ok(())
}
