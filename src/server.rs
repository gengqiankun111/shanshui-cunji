//! HTTP-JSON 服务（development 步骤 15 / 5.12，design 5.12）。
//!
//! 基于 `std::net` 的最小 HTTP/1.1 实现（不引入异步运行时依赖，与同步内核一致）。
//! 单机 MVP：串行处理连接、无鉴权/多租户；CLI 与 HTTP 共享同一内核调用路径。
//!
//! 接口（对齐 Readme「HTTP-JSON 接口」）：
//! - `POST /put`     `{"docid":1001,"status":"active","type":"order",...}` → 写入
//!                   （文档原样存储；字符串字段值自动作为倒排词条）
//! - `GET  /get?docid=1001`            → `{"docid":1001,"value":{...}}`
//! - `GET  /search?filter=...`         → `{"total":N,"rows":[{"docid":D,"value":{...}},...]}`
//! - `GET  /range?start=S&end=E`       → 同上（主键范围）
//! - `POST /delete` `{"docid":1001}` 或 `GET /delete?docid=1001` → `{"ok":true}`
//!
//! filter 语法（MVP 子集）：`field=value`，多条件用 ` AND ` 连接（位图交集）；
//! `docid=...` 走主键点查。

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

use serde_json::{json, Value};

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::optimizer::QuerySpec;

/// 启动 HTTP 服务（阻塞运行，进程终止即退出）。串行处理连接（MVP）。
pub fn serve(engine: &mut Engine, addr: &str) -> Result<()> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    tracing::info!("HTTP-JSON 服务已启动: http://{local}");
    serve_listener(engine, listener)
}

/// 接受连接并分发请求（供 `serve` 与测试复用）。
fn serve_listener(engine: &mut Engine, listener: TcpListener) -> Result<()> {
    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("接受连接失败: {e}");
                continue;
            }
        };
        if let Err(e) = handle_connection(engine, &mut stream) {
            tracing::warn!("请求处理失败: {e}");
        }
    }
    Ok(())
}

/// 处理单个 HTTP 请求：读取 → 路由 → 响应。
fn handle_connection(engine: &mut Engine, stream: &mut TcpStream) -> Result<()> {
    let (method, path, query, body) = read_http_request(stream)?;
    let (status, payload) = route_request(engine, &method, &path, &query, &body);
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

/// 解析 query string（`k=v&k2=v2`，值做百分号解码，`+` → 空格）。
fn parse_query(query: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (url_decode(k), url_decode(v)),
            None => (url_decode(pair), String::new()),
        };
        out.push((k, v));
    }
    out
}

/// 百分号解码（UTF-8）。
pub fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                    if let Ok(b) = u8::from_str_radix(hex, 16) {
                        out.push(b);
                        i += 3;
                        continue;
                    }
                }
                out.push(bytes[i]);
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

// ---------------------------------------------------------------------------
// filter 解析（CLI 与 HTTP 共用）
// ---------------------------------------------------------------------------

/// 解析 filter：`field=value`，多条件 ` AND ` 连接（大小写不敏感分隔）。
/// 返回 (字段, 值) 列表。支持 `docid=...` 主键条件。
pub fn parse_filter(filter: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for cond in filter.split(" AND ") {
        let cond = cond.trim();
        if cond.is_empty() {
            continue;
        }
        if let Some((k, v)) = cond.split_once('=') {
            let k = k.trim();
            let v = v.trim();
            if !k.is_empty() {
                out.push((k.to_string(), v.to_string()));
            }
        }
    }
    out
}

/// 提取 JSON 中全部字符串字段值作为倒排词条（递归，含顶层与嵌套对象）。
pub fn extract_terms(val: &Value) -> Vec<String> {
    let mut terms = Vec::new();
    collect_strings(val, &mut terms);
    terms
}

fn collect_strings(val: &Value, out: &mut Vec<String>) {
    match val {
        Value::String(s) => out.push(s.clone()),
        Value::Object(map) => {
            for (k, v) in map {
                if k == "docid" {
                    continue; // 主键不作为词条
                }
                if let Value::String(s) = v {
                    out.push(s.clone());
                } else if v.is_object() || v.is_array() {
                    collect_strings(v, out);
                }
            }
        }
        Value::Array(arr) => {
            for v in arr {
                collect_strings(v, out);
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// 路由与处理器
// ---------------------------------------------------------------------------

fn route_request(engine: &mut Engine, method: &str, path: &str, query: &str, body: &[u8]) -> (u16, String) {
    match (method, path) {
        ("POST", "/put") => handle_put(engine, body),
        ("POST", "/patch") => handle_patch(engine, body),
        ("GET", "/get") => handle_get(engine, query),
        ("GET", "/search") => handle_search(engine, query),
        ("GET", "/range") => handle_range(engine, query),
        ("POST", "/delete") => handle_delete(engine, body, query),
        ("GET", "/delete") => handle_delete(engine, body, query),
        _ => (404, json!({"error": format!("接口不存在: {method} {path}")}).to_string()),
    }
}

/// 部分更新（阶段 1.5 Delta CF）：body `{"docid":N,"fields":{"status":"inactive","note":null}}`，
/// null 值删除字段，Merge-on-Read 覆盖。
fn handle_patch(engine: &mut Engine, body: &[u8]) -> (u16, String) {
    let val: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return (400, json!({"error": format!("JSON 解析失败: {e}")}).to_string()),
    };
    let Some(docid) = val.get("docid").and_then(|d| d.as_u64()) else {
        return (400, json!({"error": "缺少 docid 字段"}).to_string());
    };
    let Some(fields_obj) = val.get("fields").and_then(|f| f.as_object()) else {
        return (400, json!({"error": "缺少 fields 对象"}).to_string());
    };
    let fields: Vec<(&str, serde_json::Value)> =
        fields_obj.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
    match engine.patch(docid, &fields) {
        Ok(()) => (200, json!({"ok": true, "docid": docid}).to_string()),
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

fn handle_put(engine: &mut Engine, body: &[u8]) -> (u16, String) {
    let val: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return (400, json!({"error": format!("JSON 解析失败: {e}")}).to_string()),
    };
    let Some(docid) = val.get("docid").and_then(|d| d.as_u64()) else {
        return (400, json!({"error": "缺少 docid 字段"}).to_string());
    };
    if docid >= u32::MAX as u64 {
        return (400, json!({"error": "docid 超出倒排索引支持范围（< 2^32）"}).to_string());
    }
    let terms = extract_terms(&val);
    let term_refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
    match engine.put(docid, body.to_vec(), &term_refs) {
        Ok(()) => (200, json!({"ok": true, "docid": docid, "terms": terms.len()}).to_string()),
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

fn handle_get(engine: &mut Engine, query: &str) -> (u16, String) {
    let params = parse_query(query);
    let Some(docid) = params.iter().find(|(k, _)| k == "docid").and_then(|(_, v)| v.parse::<u64>().ok()) else {
        return (400, json!({"error": "缺少 docid 参数"}).to_string());
    };
    match engine.get(docid) {
        Ok(Some(v)) => (200, value_row(docid, &v).to_string()),
        Ok(None) => (404, json!({"error": "not found"}).to_string()),
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

fn handle_search(engine: &mut Engine, query: &str) -> (u16, String) {
    let params = parse_query(query);
    let Some(filter) = params.iter().find(|(k, _)| k == "filter").map(|(_, v)| v.clone()) else {
        return (400, json!({"error": "缺少 filter 参数"}).to_string());
    };
    match execute_filter(engine, &filter) {
        Ok(rows) => (200, rows_payload(&rows).to_string()),
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

fn handle_range(engine: &mut Engine, query: &str) -> (u16, String) {
    let params = parse_query(query);
    let start = params.iter().find(|(k, _)| k == "start").and_then(|(_, v)| v.parse::<u64>().ok());
    let end = params.iter().find(|(k, _)| k == "end").and_then(|(_, v)| v.parse::<u64>().ok());
    match engine.scan_range(start, end) {
        Ok(rows) => (200, rows_payload(&rows).to_string()),
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

fn handle_delete(engine: &mut Engine, body: &[u8], query: &str) -> (u16, String) {
    // 支持 POST body {"docid":N} 或 GET ?docid=N
    let docid: Option<u64> = if !body.is_empty() {
        serde_json::from_slice::<Value>(body).ok().and_then(|v| v.get("docid").and_then(|d| d.as_u64()))
    } else {
        parse_query(query).iter().find(|(k, _)| k == "docid").and_then(|(_, v)| v.parse().ok())
    };
    let Some(docid) = docid else {
        return (400, json!({"error": "缺少 docid"}).to_string());
    };
    match engine.delete(docid) {
        Ok(()) => (200, json!({"ok": true, "docid": docid}).to_string()),
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

/// 将 (docid, 原始字节) 组装为 `{"docid":D,"value":...}`（value 为 JSON 时嵌入对象）。
fn value_row(docid: u64, raw: &[u8]) -> Value {
    match serde_json::from_slice::<Value>(raw) {
        Ok(v) => json!({"docid": docid, "value": v}),
        Err(_) => json!({"docid": docid, "value": String::from_utf8_lossy(raw)}),
    }
}

/// 将查询结果组装为 `{"total":N,"rows":[...]}`。
fn rows_payload(rows: &[(u64, Vec<u8>)]) -> Value {
    let items: Vec<Value> = rows.iter().map(|(d, v)| value_row(*d, v)).collect();
    json!({"total": items.len(), "rows": items})
}

// ---------------------------------------------------------------------------
// 查询执行（CLI 与 HTTP 共用内核路径）
// ---------------------------------------------------------------------------

/// 按 filter 执行查询：
/// - `docid=N` → 主键点查；
/// - 单条件 → 倒排词条查询；
/// - 多条件（AND）→ 各词条位图交集后回表。
pub fn execute_filter(engine: &mut Engine, filter: &str) -> Result<Vec<(u64, Vec<u8>)>> {
    let conds = parse_filter(filter);
    if conds.is_empty() {
        return engine.scan_range(None, None);
    }
    // 主键条件优先
    if let Some((_, v)) = conds.iter().find(|(f, _)| f == "docid") {
        let docid: u64 = v
            .parse()
            .map_err(|_| Error::Unsupported(format!("docid 非法: {v}")))?;
        return Ok(engine.get(docid)?.map(|val| (docid, val)).into_iter().collect());
    }
    let values: Vec<&str> = conds.iter().map(|(_, v)| v.as_str()).collect();
    if values.len() == 1 {
        return engine.search_term(values[0]);
    }
    // 多条件 AND：位图交集（RoaringBitmap）→ 回表
    let mut bitmap = engine.inverted_posting(values[0])?;
    for v in &values[1..] {
        bitmap &= engine.inverted_posting(v)?;
    }
    let mut out = Vec::new();
    for docid in bitmap {
        if let Some(val) = engine.get(docid as u64)? {
            out.push((docid as u64, val));
        }
    }
    Ok(out)
}

/// 按 QuerySpec 执行（保留给协议层使用，与 execute_filter 同源）。
pub fn execute_spec(engine: &mut Engine, spec: &QuerySpec) -> Result<Vec<(u64, Vec<u8>)>> {
    engine.execute(spec)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;
    use std::time::Duration;

    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let name = format!("srv-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        let p = DIR.get_or_init(|| tempfile::tempdir().unwrap()).path().join(name);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn cfg() -> crate::config::Config {
        let mut c = crate::config::Config::default();
        c.sstable.compression = "none".into();
        c
    }

    /// 启动服务线程（引擎所有权移入），返回监听地址。
    fn spawn_server(engine: Engine) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let mut engine = engine;
            serve_listener(&mut engine, listener).unwrap();
        });
        addr
    }

    /// 极简 HTTP 客户端：发送请求并返回 (状态码, body)。
    fn http_req(addr: std::net::SocketAddr, method: &str, target: &str, body: &[u8]) -> (u16, String) {
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        write!(
            s,
            "{method} {target} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        s.write_all(body).unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).unwrap();
        let text = String::from_utf8_lossy(&buf).to_string();
        let status = text
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(0);
        let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        (status, body)
    }

    #[test]
    fn parse_filter_basic() {
        assert_eq!(
            parse_filter("status=active AND type=order"),
            vec![("status".to_string(), "active".to_string()), ("type".to_string(), "order".to_string())]
        );
        assert_eq!(parse_filter("city=beijing"), vec![("city".to_string(), "beijing".to_string())]);
        assert!(parse_filter("").is_empty());
        assert!(parse_filter("no-equals-here").is_empty());
    }

    #[test]
    fn url_decode_handles_percent_and_plus() {
        assert_eq!(url_decode("status%3Dactive%20AND%20type%3Dorder"), "status=active AND type=order");
        assert_eq!(url_decode("a+b"), "a b");
        assert_eq!(url_decode("100%"), "100%");
    }

    #[test]
    fn extract_terms_collects_string_values() {
        let v: Value = serde_json::from_str(r#"{"docid":1,"status":"active","n":3,"fields":{"type":"click","flag":true}}"#).unwrap();
        let terms = extract_terms(&v);
        assert!(terms.contains(&"active".to_string()));
        assert!(terms.contains(&"click".to_string()));
        assert!(!terms.contains(&"1".to_string()));
        assert!(!terms.contains(&"true".to_string()));
    }

    #[test]
    fn http_end_to_end_crud_and_search() {
        let dir = tmp();
        let engine = Engine::open(&dir, &cfg()).unwrap();
        let addr = spawn_server(engine);

        // PUT
        let (st, body) = http_req(addr, "POST", "/put", br#"{"docid":1001,"status":"active","type":"order","device":"android"}"#);
        assert_eq!(st, 200, "put 失败: {body}");
        assert!(body.contains("\"ok\":true"));

        // PUT 第二条
        http_req(addr, "POST", "/put", br#"{"docid":2002,"status":"active","type":"view","device":"ios"}"#);

        // GET
        let (st, body) = http_req(addr, "GET", "/get?docid=1001", b"");
        assert_eq!(st, 200, "get 失败: {body}");
        assert!(body.contains("android"), "get 应返回存储文档: {body}");

        // GET 未命中
        let (st, _) = http_req(addr, "GET", "/get?docid=9999", b"");
        assert_eq!(st, 404);

        // SEARCH 单条件
        let (st, body) = http_req(addr, "GET", "/search?filter=status%3Dactive", b"");
        assert_eq!(st, 200, "search 失败: {body}");
        assert!(body.contains("\"total\":2"), "应为 2 条: {body}");

        // SEARCH 多条件 AND（位图交集）
        let (st, body) = http_req(addr, "GET", "/search?filter=status%3Dactive%20AND%20type%3Dorder", b"");
        assert_eq!(st, 200, "search-and 失败: {body}");
        assert!(body.contains("\"total\":1"), "交集应为 1 条: {body}");
        assert!(body.contains("1001"));

        // RANGE（[1000,2000] 仅含 1001；2002 在外）
        let (st, body) = http_req(addr, "GET", "/range?start=1000&end=2000", b"");
        assert_eq!(st, 200, "range 失败: {body}");
        assert!(body.contains("\"total\":1"), "范围应命中 1 条: {body}");

        // PATCH（阶段 1.5 部分更新）：覆盖 device + 新增 note
        let (st, body) = http_req(addr, "POST", "/patch", br#"{"docid":2002,"fields":{"device":"linux","note":"patched"}}"#);
        assert_eq!(st, 200, "patch 失败: {body}");
        let (st, body) = http_req(addr, "GET", "/get?docid=2002", b"");
        assert_eq!(st, 200, "patch 后 get 失败: {body}");
        assert!(body.contains("linux"), "device 应被覆盖: {body}");
        assert!(body.contains("patched"), "note 应新增: {body}");
        assert!(!body.contains("ios"), "旧 device 不应残留: {body}");

        // DELETE
        let (st, body) = http_req(addr, "POST", "/delete", br#"{"docid":1001}"#);
        assert_eq!(st, 200, "delete 失败: {body}");
        let (st, _) = http_req(addr, "GET", "/get?docid=1001", b"");
        assert_eq!(st, 404, "删除后应 404");

        // 非法 JSON → 400
        let (st, _) = http_req(addr, "POST", "/put", br#"{"no-docid":1}"#);
        assert_eq!(st, 400);
    }
}
