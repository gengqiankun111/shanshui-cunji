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

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::optimizer::QuerySpec;

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

/// 13.7 后台对账线程：周期扫描未终态事务，按指数退避自动续补偿（无步骤定义则跳过留人工）。
fn spawn_reconciler(saga: SagaShared, steps_cache: SagaStepsCache) {
    std::thread::spawn(move || loop {
        std::thread::sleep(std::time::Duration::from_secs(SAGA_RECONCILE_INTERVAL_SECS));
        let now = now_ms();
        let (mut coord, cache) = match (saga.lock(), steps_cache.lock()) {
            (Ok(c), Ok(s)) => (c, s),
            _ => continue, // 中毒/其他：下周期再试
        };
        let retried = coord.retry_pending(
            |tx| {
                cache
                    .get(tx)
                    .map(http_steps_from_json)
                    .unwrap_or_default()
            },
            now,
            SAGA_STALL_MS,
            SAGA_MAX_BACKOFF_MS,
        );
        if retried > 0 {
            tracing::info!("SAGA 对账器触发 {retried} 个事务续补偿");
        }
    });
}

/// 从 steps JSON 数组重建 HTTP 步骤（对账重试与 start 解析共用；非法项跳过）。
fn http_steps_from_json(arr: &Value) -> Vec<Box<dyn crate::saga::SagaStep>> {
    let mut out: Vec<Box<dyn crate::saga::SagaStep>> = Vec::new();
    if let Some(items) = arr.as_array() {
        for st in items {
            if let (Some(name), Some(compensate_url)) = (
                st.get("name").and_then(|x| x.as_str()),
                st.get("compensate_url").and_then(|x| x.as_str()),
            ) {
                let action_url = st.get("action_url").and_then(|x| x.as_str()).unwrap_or("");
                let payload = st
                    .get("payload")
                    .map(|p| serde_json::to_string(p).unwrap_or_default())
                    .unwrap_or_default()
                    .into_bytes();
                out.push(Box::new(crate::saga::HttpStep::new(
                    name, action_url, compensate_url, payload,
                )));
            }
        }
    }
    out
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

/// 提取 JSON 中全部字符串字段值作为倒排词条（递归，含顶层与嵌套对象/数组）。
/// term 编码：`{字段路径}={值}`（如 `status=active`、`meta.device=ios`），
/// 路径用 `.` 连接——带字段维度，供 COUNT / GROUP BY 按字段聚合（development 5.17）。
pub fn extract_terms(val: &Value) -> Vec<String> {
    extract_terms_filtered(val, None)
}

/// 提取倒排词条并按字段白名单过滤（M8-P4）：`include` 非空时只生成声明字段的 term
/// （不匹配字段的 term **不分配**——长文本/高基数字段整串 term 的分配浪费在生成前消除）。
pub fn extract_terms_filtered(
    val: &Value,
    include: Option<&std::collections::HashSet<String>>,
) -> Vec<String> {
    extract_terms_with_fulltext(val, include, None)
}

/// fulltext 分词索引（M8-P7）：`extract_terms_filtered` 的超集——
/// `fulltext` 集合中声明的字段做**分词建词 term**（`ft:{field}:{token}`）**取代整串 term**：
/// 长文本整串（>max_term_len）会被跳过无法检索；分词后 token 短可建索引、支持关键词检索。
/// 其余字段维持整串 term（受 include 白名单过滤）；两者命名空间不冲突（`ft:` 前缀独立）。
/// 中文分词用 bigram（M8-P9）；需 jieba 词典分词请用 `extract_terms_with_fulltext_seg`。
pub fn extract_terms_with_fulltext(
    val: &Value,
    include: Option<&std::collections::HashSet<String>>,
    fulltext: Option<&std::collections::HashSet<String>>,
) -> Vec<String> {
    extract_terms_with_fulltext_seg(val, include, fulltext, false)
}

/// 同 `extract_terms_with_fulltext`，但可指定中文分词器（M8-P13）：
/// `use_jieba=true` 时 fulltext 字段中文用 jieba 完整词典分词（语义词，非 bigram 碎片）。
pub fn extract_terms_with_fulltext_seg(
    val: &Value,
    include: Option<&std::collections::HashSet<String>>,
    fulltext: Option<&std::collections::HashSet<String>>,
    use_jieba: bool,
) -> Vec<String> {
    let mut terms = Vec::new();
    collect_strings(val, &mut terms, &[], include, fulltext, use_jieba);
    terms
}

/// 分词（bigram，M8-P7 + M8-P9）：ASCII 字母数字 → 单词（小写归一）；
/// 连续中文/非 ASCII → bigram（相邻 2 字，单字回退 unigram），零依赖。
/// jieba 词典分词请用 `tokenize_seg(text, true)`。
pub fn tokenize(text: &str) -> Vec<String> {
    tokenize_seg(text, false)
}

/// 分词并按中文分词器选择（M8-P13）：`use_jieba` → jieba 完整词典分词（需 cjk-jieba feature，
/// 关闭时回退 bigram）；否则 bigram。
pub fn tokenize_seg(text: &str, use_jieba: bool) -> Vec<String> {
    #[cfg(feature = "cjk-jieba")]
    {
        if use_jieba {
            return tokenize_jieba(text);
        }
    }
    tokenize_bigram(text)
}

#[cfg(feature = "cjk-jieba")]
static JIEBA: std::sync::OnceLock<jieba_rs::Jieba> = std::sync::OnceLock::new();

#[cfg(feature = "cjk-jieba")]
fn jieba() -> &'static jieba_rs::Jieba {
    JIEBA.get_or_init(jieba_rs::Jieba::new)
}

/// jieba 完整中文词典分词（M8-P13）：ASCII 字母数字 → 单词（小写归一，同 bigram 规则）；
/// 中文/非 ASCII 块 → jieba 词典切分（语义词，非 bigram 碎片）；过滤标点/空白 token。
#[cfg(feature = "cjk-jieba")]
fn tokenize_jieba(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut ascii_word = String::new();
    let mut cjk = String::new();
    let j = jieba();
    for c in text.chars() {
        if c.is_ascii_alphanumeric() {
            if !cjk.is_empty() {
                out.extend(jieba_cut(j, &cjk));
                cjk.clear();
            }
            ascii_word.push(c.to_ascii_lowercase());
        } else if c.is_alphanumeric() {
            // 非 ASCII 字母数字（CJK 等）：结束 ASCII 单词，进入中文块
            if !ascii_word.is_empty() {
                out.push(std::mem::take(&mut ascii_word));
            }
            cjk.push(c);
        } else {
            // 分隔符
            if !ascii_word.is_empty() {
                out.push(std::mem::take(&mut ascii_word));
            }
            if !cjk.is_empty() {
                out.extend(jieba_cut(j, &cjk));
                cjk.clear();
            }
        }
    }
    if !ascii_word.is_empty() {
        out.push(ascii_word);
    }
    if !cjk.is_empty() {
        out.extend(jieba_cut(j, &cjk));
    }
    out
}

#[cfg(feature = "cjk-jieba")]
fn jieba_cut(j: &jieba_rs::Jieba, text: &str) -> Vec<String> {
    j.cut(text, true)
        .into_iter()
        .map(|t| t.word.to_string())
        .filter(|w| w.chars().any(|c| c.is_alphanumeric()))
        .collect()
}

/// bigram 分词（M8-P7 + 中文 bigram M8-P9）：按字符类分段——
/// **ASCII 字母数字** → 单词 token（按非字母数字边界切分 + 小写归一，原有行为）；
/// **连续中文/非 ASCII 字母数字** → bigram（相邻 2 字一个 token，单字回退 unigram）——
/// 中文整串当单 token 无法检索（7.14 已知限制），bigram 与 Elasticsearch ngram /
/// Lucene CJKAnalyzer 同款（中文检索事实标准），零依赖、无词典、索引膨胀 ≈ 字数。
pub fn tokenize_bigram(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut ascii_word = String::new();
    let mut cjk = String::new();
    for c in text.chars() {
        if c.is_ascii_alphanumeric() {
            if !cjk.is_empty() {
                out.extend(cjk_bigram(&cjk));
                cjk.clear();
            }
            ascii_word.push(c.to_ascii_lowercase());
        } else if c.is_alphanumeric() {
            // 非 ASCII 字母数字（CJK 等）：结束 ASCII 单词，进入中文块
            if !ascii_word.is_empty() {
                out.push(std::mem::take(&mut ascii_word));
            }
            cjk.push(c);
        } else {
            // 分隔符
            if !ascii_word.is_empty() {
                out.push(std::mem::take(&mut ascii_word));
            }
            if !cjk.is_empty() {
                out.extend(cjk_bigram(&cjk));
                cjk.clear();
            }
        }
    }
    if !ascii_word.is_empty() {
        out.push(ascii_word);
    }
    if !cjk.is_empty() {
        out.extend(cjk_bigram(&cjk));
    }
    out
}

/// 中文块 bigram：长度 1 → 单字 unigram（保证单字可查）；≥2 → 相邻 2 字。
fn cjk_bigram(s: &str) -> Vec<String> {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() == 1 {
        return vec![chars[0].to_string()];
    }
    chars.windows(2).map(|w| w.iter().collect()).collect()
}

/// 由分词结果生成 fulltext 词 term：`ft:{field}:{token}`；同一字段值内重复 token 去重
/// （避免 posting 重复 docid 浪费内存）。中文分词 bigram（M8-P9）。
pub fn fulltext_terms(field: &str, text: &str) -> Vec<String> {
    fulltext_terms_seg(field, text, false)
}

/// 同 `fulltext_terms`，可指定中文分词器（M8-P13）：`use_jieba` → jieba 词典分词。
pub fn fulltext_terms_seg(field: &str, text: &str, use_jieba: bool) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    tokenize_seg(text, use_jieba)
        .into_iter()
        .filter(|t| seen.insert(t.clone()))
        .map(|t| format!("ft:{field}:{t}"))
        .collect()
}

/// 单字段 term 生成：fulltext 字段 → 分词词 term（不建整串）；否则整串 term（受白名单过滤）。
fn push_field_term(
    out: &mut Vec<String>,
    field: &str,
    s: &str,
    include: Option<&std::collections::HashSet<String>>,
    fulltext: Option<&std::collections::HashSet<String>>,
    use_jieba: bool,
) {
    if let Some(ft) = fulltext {
        if ft.contains(field) {
            out.extend(fulltext_terms_seg(field, s, use_jieba));
            return;
        }
    }
    if include.map_or(true, |inc| inc.contains(field)) {
        out.push(format!("{field}={s}"));
    }
}

fn collect_strings(
    val: &Value,
    out: &mut Vec<String>,
    path: &[&str],
    include: Option<&std::collections::HashSet<String>>,
    fulltext: Option<&std::collections::HashSet<String>>,
    use_jieba: bool,
) {
    match val {
        Value::String(s) => {
            // 数组元素等叶子字符串：用完整路径生成 term（白名单非空时仅保留声明字段）
            push_field_term(out, &path.join("."), s, include, fulltext, use_jieba);
        }
        Value::Object(map) => {
            for (k, v) in map {
                if path.is_empty() && k == "docid" {
                    continue; // 主键不作为词条
                }
                let mut p = path.to_vec();
                p.push(k.as_str());
                if let Value::String(s) = v {
                    push_field_term(out, &p.join("."), s, include, fulltext, use_jieba);
                } else {
                    collect_strings(v, out, &p, include, fulltext, use_jieba);
                }
            }
        }
        Value::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                let mut p = path.to_vec();
                let idx = i.to_string();
                p.push(&idx);
                collect_strings(v, out, &p, include, fulltext, use_jieba);
            }
        }
        _ => {}
    }
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
// SAGA 网关（Ex-2.5）：/saga/start /saga/status /saga/compensate
// 参照 design_extension 13.1：协调器状态持久化 `{data_dir}/saga/saga-{tx_id}.json`，
// 崩溃恢复续跑；业务步骤为 HTTP 端点（HttpStep），非 2xx/超时 → 失败逆序补偿。
// ---------------------------------------------------------------------------

/// `POST /saga/start` `{"tx_id":"t1","steps":[{"name":"扣款","action_url":"...",
/// "compensate_url":"...","payload":"...","depends_on":["..."]}]}` → 执行（失败自动逆序补偿）。
/// 13.6：steps[i] 可选 `depends_on`（依赖步骤名数组）→ 拓扑并行执行；无依赖 → 原串行 `run`。
/// 13.7：成功后缓存步骤定义（对账重试重建用）。
fn handle_saga_start(
    saga: Option<&SagaShared>,
    steps_cache: Option<&SagaStepsCache>,
    body: &[u8],
) -> (u16, String) {
    let Some(coord_arc) = saga else {
        return (501, json!({"error": "SAGA 协调器未挂载"}).to_string());
    };
    let v: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return (400, json!({"error": format!("请求体解析失败: {e}")}).to_string()),
    };
    let Some(tx_id) = v.get("tx_id").and_then(|x| x.as_str()) else {
        return (400, json!({"error": "缺少 tx_id"}).to_string());
    };
    let Some(steps_arr) = v.get("steps").and_then(|x| x.as_array()) else {
        return (400, json!({"error": "缺少 steps 数组"}).to_string());
    };
    // 解析步骤：name → 索引（depends_on 引用解析）
    let mut name_idx: HashMap<&str, usize> = HashMap::new();
    for (i, st) in steps_arr.iter().enumerate() {
        let Some(name) = st.get("name").and_then(|x| x.as_str()) else {
            return (400, json!({"error": format!("steps[{i}] 缺少 name")}).to_string());
        };
        name_idx.insert(name, i);
    }
    let mut steps: Vec<Box<dyn crate::saga::SagaStep>> = Vec::new();
    let mut deps: Vec<Vec<usize>> = Vec::new();
    for (i, st) in steps_arr.iter().enumerate() {
        let (Some(name), Some(action_url), Some(compensate_url)) = (
            st.get("name").and_then(|x| x.as_str()),
            st.get("action_url").and_then(|x| x.as_str()),
            st.get("compensate_url").and_then(|x| x.as_str()),
        ) else {
            return (
                400,
                json!({"error": format!("steps[{i}] 缺少 name/action_url/compensate_url")}).to_string(),
            );
        };
        let payload = st
            .get("payload")
            .map(|p| serde_json::to_string(p).unwrap_or_default())
            .unwrap_or_default()
            .into_bytes();
        // 13.6 depends_on：依赖步骤名数组 → 索引（未知名/自依赖 → 400）
        let mut di = Vec::new();
        if let Some(dep) = st.get("depends_on").and_then(|x| x.as_array()) {
            for d in dep {
                let Some(dn) = d.as_str() else {
                    return (400, json!({"error": format!("steps[{i}].depends_on 项须为字符串")}).to_string());
                };
                match name_idx.get(dn) {
                    Some(&idx) => di.push(idx),
                    None => {
                        return (
                            400,
                            json!({"error": format!("steps[{i}].depends_on 引用未知步骤: {dn}")}).to_string(),
                        )
                    }
                }
            }
        }
        deps.push(di);
        steps.push(Box::new(crate::saga::HttpStep::new(name, action_url, compensate_url, payload)));
    }
    let refs: Vec<&dyn crate::saga::SagaStep> = steps.iter().map(|s| s.as_ref()).collect();
    // 13.7：缓存步骤定义（对账重试重建；失败仅告警不阻断）
    if let Some(cache) = steps_cache {
        if let Ok(mut c) = cache.lock() {
            c.insert(tx_id.to_string(), Value::Array(steps_arr.clone()));
        }
    }
    let outcome = (|| -> crate::error::Result<crate::saga::SagaStatus> {
        let mut coord = coord_arc.lock().unwrap();
        match coord.status(tx_id) {
            Some(st) if st.status.is_terminal() => return Ok(st.status), // 终态幂等
            Some(_) => {} // 已登记：run 续跑（含崩溃恢复）
            None => {
                coord.start(tx_id)?;
            }
        }
        // 13.6：有依赖声明 → 拓扑并行；否则原串行 run（兼容旧请求）
        let has_deps = deps.iter().any(|d| !d.is_empty());
        if has_deps {
            // 提前环/非法依赖校验 → 400（run_parallel 内部同样校验，双保险）
            if let Err(e) = crate::saga::topo_layers(steps.len(), &deps) {
                return Err(e);
            }
        }
        if has_deps {
            coord.run_parallel(tx_id, &refs, &deps)
        } else {
            coord.run(tx_id, &refs)
        }
    })();
    match outcome {
        Ok(status) => {
            let st = coord_arc.lock().unwrap().status(tx_id).unwrap().clone();
            (200, json!({"tx_id": tx_id, "status": status, "executed_steps": st.executed_steps, "last_error": st.last_error}).to_string())
        }
        Err(e) => (400, json!({"error": e.to_string()}).to_string()),
    }
}

/// `GET /saga/status?tx_id=` → transactionId → status 回查（屏障接口依据）。
fn handle_saga_status(saga: Option<&SagaShared>, query: &str) -> (u16, String) {
    let Some(coord) = saga else {
        return (501, json!({"error": "SAGA 协调器未挂载"}).to_string());
    };
    let params = parse_query(query);
    let Some(tx_id) = params.iter().find(|(k, _)| k == "tx_id").map(|(_, v)| v.clone()) else {
        return (400, json!({"error": "缺少 tx_id 参数"}).to_string());
    };
    let st = coord.lock().unwrap().status(&tx_id).cloned();
    match st {
        Some(st) => (200, json!({"tx_id": tx_id, "status": st.status, "executed_steps": st.executed_steps, "compensated_steps": st.compensated_steps, "last_error": st.last_error, "retry_count": st.retry_count}).to_string()),
        None => (404, json!({"error": format!("SAGA 事务不存在: {tx_id}")}).to_string()),
    }
}

/// `POST /saga/compensate` `{"tx_id":"t1"}` → 强制对已登记分支逆序补偿（重试/人工干预）。
/// 步骤定义从持久化状态无从恢复，故请求可带可选 `steps`（缺省按已登记分支续补偿）。
fn handle_saga_compensate(saga: Option<&SagaShared>, body: &[u8]) -> (u16, String) {
    let Some(coord) = saga else {
        return (501, json!({"error": "SAGA 协调器未挂载"}).to_string());
    };
    let v: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return (400, json!({"error": format!("请求体解析失败: {e}")}).to_string()),
    };
    let Some(tx_id) = v.get("tx_id").and_then(|x| x.as_str()) else {
        return (400, json!({"error": "缺少 tx_id"}).to_string());
    };
    // 可选 steps（缺省空：仅把持久化状态置 Compensating；后续 run 续补偿）
    let steps = v.get("steps").map(http_steps_from_json).unwrap_or_default();
    let refs: Vec<&dyn crate::saga::SagaStep> = steps.iter().map(|s| s.as_ref()).collect();
    let mut coord = coord.lock().unwrap();
    if steps.is_empty() {
        // 无步骤定义：无法发起网络补偿，返回当前状态（续跑依赖 /saga/start 带步骤）
        return match coord.status(tx_id) {
            Some(st) => (200, json!({"tx_id": tx_id, "status": st.status, "note": "无步骤定义，仅置待补偿（可用 /saga/start 携带步骤续跑）"}).to_string()),
            None => (404, json!({"error": format!("SAGA 事务不存在: {tx_id}")}).to_string()),
        };
    }
    match coord.compensate(tx_id, &refs) {
        Ok(status) => (200, json!({"tx_id": tx_id, "status": status}).to_string()),
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

/// COUNT(field=value)：`GET /count?field=status&value=active` → `{"count":N}`。
fn handle_count(engine: &mut Engine, query: &str) -> (u16, String) {
    let params = parse_query(query);
    let (Some(field), Some(value)) = (
        params
            .iter()
            .find(|(k, _)| k == "field")
            .map(|(_, v)| v.clone()),
        params
            .iter()
            .find(|(k, _)| k == "value")
            .map(|(_, v)| v.clone()),
    ) else {
        return (400, json!({"error": "缺少 field/value 参数"}).to_string());
    };
    match execute_count(engine, &field, &value) {
        Ok(count) => (
            200,
            json!({"field": field, "value": value, "count": count}).to_string(),
        ),
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

/// 引擎状态（design 20）：`GET /admin/status` → StatusReport JSON。
fn handle_admin_status(engine: &mut Engine) -> (u16, String) {
    let rep = crate::admin::status(engine);
    match serde_json::to_string(&rep) {
        Ok(s) => (200, s),
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

/// Prometheus 指标（X 项）：`GET /metrics` → 文本格式（计数/直方图/gauge 分层埋点）。
fn handle_metrics(engine: &mut Engine) -> (u16, String) {
    let s = engine.stats();
    let l0 = engine.primary_l0_count() as u64;
    let flush = engine.total_flush_count();
    (
        200,
        engine
            .metrics
            .render(s.sst_file_count as u64, l0, s.mem_ratio, s.disk_ratio, flush),
    )
}

/// 执行计划推演（development 5.26）：`GET /explain?filter=status%3Dactive` → ExplainPlan JSON。
fn handle_explain(engine: &mut Engine, query: &str) -> (u16, String) {
    let params = parse_query(query);
    let Some(filter) = params
        .iter()
        .find(|(k, _)| k == "filter")
        .map(|(_, v)| v.clone())
    else {
        return (400, json!({"error": "缺少 filter 参数"}).to_string());
    };
    match crate::explain::explain(engine, &filter) {
        Ok(plan) => match serde_json::to_string(&plan) {
            Ok(s) => (200, s),
            Err(e) => (500, json!({"error": e.to_string()}).to_string()),
        },
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

/// GROUP BY field：`GET /groupby?field=status` → `{"field":"status","groups":[{"value":"active","count":2},...]}`。
fn handle_group_by(engine: &mut Engine, query: &str) -> (u16, String) {
    let params = parse_query(query);
    let Some(field) = params
        .iter()
        .find(|(k, _)| k == "field")
        .map(|(_, v)| v.clone())
    else {
        return (400, json!({"error": "缺少 field 参数"}).to_string());
    };
    match execute_group_by(engine, &field) {
        Ok(groups) => {
            let arr: Vec<Value> = groups
                .iter()
                .map(|(term, count)| {
                    let value = term.split_once('=').map(|(_, v)| v).unwrap_or(term);
                    json!({"value": value, "count": count})
                })
                .collect();
            (200, json!({"field": field, "groups": arr}).to_string())
        }
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

/// queryAndJoin（design 19）：`GET /join?filter=type=order&from=user_id&to=docid&type=inner`
/// → `{"rows":[{"left":{...},"right":{...},"matched":true},...]}`。
/// `broadcast`：小表广播 JOIN 选项（design 19.3，阶段 3，来自 `[join]` 配置）。
fn handle_join(
    engine: &mut Engine,
    query: &str,
    broadcast: Option<crate::join::JoinBroadcast>,
) -> (u16, String) {
    let params = parse_query(query);
    let p = |k: &str| params.iter().find(|(x, _)| x == k).map(|(_, v)| v.clone());
    let (Some(filter), Some(from), Some(to)) = (p("filter"), p("from"), p("to")) else {
        return (
            400,
            json!({"error": "缺少 filter / from / to 参数"}).to_string(),
        );
    };
    let join_type = match p("type").as_deref() {
        Some("left") => crate::join::JoinType::Left,
        Some("right") => crate::join::JoinType::Right,
        _ => crate::join::JoinType::Inner,
    };
    let max_rows: usize = p("max")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_000_000);
    let spec = crate::join::JoinSpec {
        filter: &filter,
        from_field: &from,
        to_field: &to,
        join_type,
    };
    match crate::join::query_and_join(engine, &spec, max_rows, broadcast) {
        Ok(rows) => {
            let arr: Vec<Value> = rows
                .iter()
                .map(|r| {
                    json!({
                        "left": r.left,
                        "right": r.right,
                        "matched": r.right.is_some(),
                    })
                })
                .collect();
            (200, json!({"total": arr.len(), "rows": arr}).to_string())
        }
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

/// 部分更新（阶段 1.5 Delta CF）：body `{"docid":N,"fields":{"status":"inactive","note":null}}`，
/// null 值删除字段，Merge-on-Read 覆盖。
fn handle_patch(engine: &mut Engine, body: &[u8]) -> (u16, String) {
    let val: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            return (
                400,
                json!({"error": format!("JSON 解析失败: {e}")}).to_string(),
            )
        }
    };
    let Some(docid) = val.get("docid").and_then(|d| d.as_u64()) else {
        return (400, json!({"error": "缺少 docid 字段"}).to_string());
    };
    let Some(fields_obj) = val.get("fields").and_then(|f| f.as_object()) else {
        return (400, json!({"error": "缺少 fields 对象"}).to_string());
    };
    let fields: Vec<(&str, serde_json::Value)> = fields_obj
        .iter()
        .map(|(k, v)| (k.as_str(), v.clone()))
        .collect();
    match engine.patch(docid, &fields) {
        Ok(()) => (200, json!({"ok": true, "docid": docid}).to_string()),
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

fn handle_put(engine: &mut Engine, body: &[u8]) -> (u16, String) {
    let val: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            return (
                400,
                json!({"error": format!("JSON 解析失败: {e}")}).to_string(),
            )
        }
    };
    let Some(docid) = val.get("docid").and_then(|d| d.as_u64()) else {
        return (400, json!({"error": "缺少 docid 字段"}).to_string());
    };
    if docid >= u32::MAX as u64 {
        return (
            400,
            json!({"error": "docid 超出倒排索引支持范围（< 2^32）"}).to_string(),
        );
    }
    let terms =
        extract_terms_with_fulltext_seg(&val, None, Some(engine.fulltext_fields()), engine.use_jieba());
    let term_refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
    // 写入 Enrich（design 19 / development 5.21）：`[enrich] enabled && source=local` 时
    // WAL 写入前展开关联文档（join::put_with_enrich，fail_policy reject/degrade）
    let result = if let Some((fail_policy, from_field, to_field)) = engine.enrich_config() {
        let fp = fail_policy.to_string();
        let from = from_field.to_string();
        let to = to_field.to_string();
        crate::join::put_with_enrich(engine, docid, body.to_vec(), &term_refs, &fp, |e, v| {
            crate::join::enrich_check_local(e, v, &from, &to)
        })
    } else {
        engine.put(docid, body.to_vec(), &term_refs)
    };
    match result {
        Ok(()) => (
            200,
            json!({"ok": true, "docid": docid, "terms": terms.len()}).to_string(),
        ),
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

fn handle_get(engine: &mut Engine, query: &str) -> (u16, String) {
    let params = parse_query(query);
    let Some(docid) = params
        .iter()
        .find(|(k, _)| k == "docid")
        .and_then(|(_, v)| v.parse::<u64>().ok())
    else {
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
    let Some(filter) = params
        .iter()
        .find(|(k, _)| k == "filter")
        .map(|(_, v)| v.clone())
    else {
        return (400, json!({"error": "缺少 filter 参数"}).to_string());
    };
    let (limit, offset) = parse_paging(query);
    match execute_filter_paged(engine, &filter, limit, offset) {
        Ok(page) => (200, rows_payload_total(page.total, &page.rows).to_string()),
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

/// 类 SQL 查询（sqlish，design 157/1358 行）：`GET /sql?q=SELECT * FROM t WHERE status='active' LIMIT 10`
/// → `{"total":N,"rows":[...]}`（复用 rows_payload_total；total=命中总数，rows 受 LIMIT/OFFSET 截断）。
fn handle_sql(engine: &mut Engine, query: &str) -> (u16, String) {
    let params = parse_query(query);
    let Some(q) = params.iter().find(|(k, _)| k == "q").map(|(_, v)| v.clone()) else {
        return (400, json!({"error": "缺少 q 参数"}).to_string());
    };
    match crate::sqlish::execute(engine, &q, 10_000) {
        Ok(rows) => (200, rows_payload_total(rows.len() as u64, &rows).to_string()),
        Err(e) => (400, json!({"error": e.to_string()}).to_string()),
    }
}

/// fulltext 分词检索（M8-P7）：`GET /fulltext?field=big_text_a&word=rec` →
/// 命中该字段分词 term `ft:{field}:{word}` 的文档列表（posting 合并 → 回表）。
/// M8-P8：支持 `limit`/`offset` 分页（大结果集防内存爆炸），`total` = 全量命中数。
fn handle_fulltext(engine: &mut Engine, query: &str) -> (u16, String) {
    let params = parse_query(query);
    let field = params
        .iter()
        .find(|(k, _)| k == "field")
        .map(|(_, v)| v.clone());
    let word = params
        .iter()
        .find(|(k, _)| k == "word")
        .map(|(_, v)| v.clone());
    let (Some(field), Some(word)) = (field, word) else {
        return (400, json!({"error": "缺少 field/word 参数"}).to_string());
    };
    let (limit, offset) = parse_paging(query);
    match engine.fulltext_search_paged(&field, &word, limit, offset) {
        Ok(page) => (200, rows_payload_total(page.total, &page.rows).to_string()),
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

fn handle_range(engine: &mut Engine, query: &str) -> (u16, String) {
    let params = parse_query(query);
    let end = params
        .iter()
        .find(|(k, _)| k == "end")
        .and_then(|(_, v)| v.parse::<u64>().ok());
    let after = params
        .iter()
        .find(|(k, _)| k == "after")
        .and_then(|(_, v)| v.parse::<u64>().ok());
    let (limit, offset) = parse_paging(query);
    // 游标续扫模式（M8-P11）：`GET /range?after=LAST&limit=N`——取满即止、无 total 全扫，
    // 每页返回 rows（用末条 docid 作下一页 after），全库遍历每页 O(limit)。
    if let Some(after) = after {
        let cap = limit.unwrap_or(u64::MAX);
        match engine.scan_after(Some(after), end, cap) {
            Ok(rows) => (
                200,
                json!({
                    "rows": rows.iter().map(|(d, v)| value_row(*d, v)).collect::<Vec<_>>()
                })
                .to_string(),
            ),
            Err(e) => (500, json!({"error": e.to_string()}).to_string()),
        }
    } else {
        let start = params
            .iter()
            .find(|(k, _)| k == "start")
            .and_then(|(_, v)| v.parse::<u64>().ok());
        match engine.scan_range_paged(start, end, limit, offset) {
            Ok(page) => (200, rows_payload_total(page.total, &page.rows).to_string()),
            Err(e) => (500, json!({"error": e.to_string()}).to_string()),
        }
    }
}

fn handle_delete(engine: &mut Engine, body: &[u8], query: &str) -> (u16, String) {
    // 支持 POST body {"docid":N} 或 GET ?docid=N
    let docid: Option<u64> = if !body.is_empty() {
        serde_json::from_slice::<Value>(body)
            .ok()
            .and_then(|v| v.get("docid").and_then(|d| d.as_u64()))
    } else {
        parse_query(query)
            .iter()
            .find(|(k, _)| k == "docid")
            .and_then(|(_, v)| v.parse().ok())
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

/// 分页响应（M8-P8）：`total` = 全量命中数（≠ 当前页行数，供客户端计算总页数）。
fn rows_payload_total(total: u64, rows: &[(u64, Vec<u8>)]) -> Value {
    let items: Vec<Value> = rows.iter().map(|(d, v)| value_row(*d, v)).collect();
    json!({"total": total, "rows": items})
}

// ---------------------------------------------------------------------------
// 查询执行（CLI 与 HTTP 共用内核路径）
// ---------------------------------------------------------------------------

/// 解析分页参数：`limit`（>0 生效，缺省/≤0 = 不限制）、`offset`（默认 0）。
fn parse_paging(query: &str) -> (Option<u64>, u64) {
    let params = parse_query(query);
    let limit = params
        .iter()
        .find(|(k, _)| k == "limit")
        .and_then(|(_, v)| v.parse::<u64>().ok())
        .filter(|l| *l > 0);
    let offset = params
        .iter()
        .find(|(k, _)| k == "offset")
        .and_then(|(_, v)| v.parse::<u64>().ok())
        .unwrap_or(0);
    (limit, offset)
}

/// 按 filter 执行查询：
/// - `docid=N` → 主键点查；
/// - 单条件 → 倒排词条查询（term 编码 `field=value`，与 extract_terms 一致）；
/// - 多条件（AND）→ 各词条位图交集后回表。
pub fn execute_filter(engine: &mut Engine, filter: &str) -> Result<Vec<(u64, Vec<u8>)>> {
    Ok(execute_filter_paged(engine, filter, None, 0)?.rows)
}

/// 按 filter 分页执行（M8-P8）：倒排命中数很大时只回表当前页（`limit`/`offset`），
/// `total` = 全量命中数——防大结果集全量回表 + JSON 构造内存爆炸。
pub fn execute_filter_paged(
    engine: &mut Engine,
    filter: &str,
    limit: Option<u64>,
    offset: u64,
) -> Result<crate::engine::PagedRows> {
    let conds = parse_filter(filter);
    if conds.is_empty() {
        return engine.scan_range_paged(None, None, limit, offset);
    }
    // 主键条件优先
    if let Some((_, v)) = conds.iter().find(|(f, _)| f == "docid") {
        let docid: u64 = v
            .parse()
            .map_err(|_| Error::Unsupported(format!("docid 非法: {v}")))?;
        let rows = engine
            .get(docid)?
            .map(|val| (docid, val))
            .into_iter()
            .collect::<Vec<_>>();
        return Ok(crate::engine::PagedRows {
            total: rows.len() as u64,
            rows,
        });
    }
    // field=value 编码成倒排 term
    let terms: Vec<String> = conds.iter().map(|(f, v)| format!("{f}={v}")).collect();
    if terms.len() == 1 {
        return engine.search_term_paged(&terms[0], limit, offset);
    }
    // 多条件 AND：位图交集（RoaringBitmap）→ 分页回表
    let mut bitmap = engine.inverted_posting(&terms[0])?;
    for t in &terms[1..] {
        bitmap &= engine.inverted_posting(t)?;
    }
    let total = bitmap.len() as u64;
    let mut rows = Vec::new();
    let cap = limit.unwrap_or(u64::MAX);
    let mut skipped = 0u64;
    for docid in bitmap {
        if skipped < offset {
            skipped += 1;
            continue;
        }
        if rows.len() as u64 >= cap {
            break;
        }
        if let Some(val) = engine.get(docid as u64)? {
            rows.push((docid as u64, val));
        }
    }
    Ok(crate::engine::PagedRows { total, rows })
}

/// COUNT(field=value)：读倒排 term 的 doc_count 直接返回（<0.1ms，development 5.17）。
pub fn execute_count(engine: &mut Engine, field: &str, value: &str) -> Result<u64> {
    engine.inverted_doc_count(&format!("{field}={value}"))
}

/// GROUP BY field：遍历该字段倒排 Term 集合，取各 value 的 doc_count 构造分组（不访问文档数据）。
/// 返回 (term, count) 列表，term 为 `field=value` 编码。
pub fn execute_group_by(engine: &mut Engine, field: &str) -> Result<Vec<(String, u64)>> {
    engine.inverted_group_by(field)
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
        let p = DIR
            .get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn cfg() -> crate::config::Config {
        let mut c = crate::config::Config::default();
        c.sstable.compression = "none".into();
        c
    }

    /// 启动服务线程（引擎所有权移入），返回监听地址。`saga` = SAGA 网关共享句柄（可 None）。
    fn spawn_server(
        engine: Engine,
        saga: Option<SagaShared>,
        steps_cache: Option<SagaStepsCache>,
    ) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let mut engine = engine;
            serve_listener(&mut engine, listener, None, saga, steps_cache).unwrap();
        });
        addr
    }

    /// 极简 HTTP 客户端：发送请求并返回 (状态码, body)。
    fn http_req(
        addr: std::net::SocketAddr,
        method: &str,
        target: &str,
        body: &[u8],
    ) -> (u16, String) {
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
            vec![
                ("status".to_string(), "active".to_string()),
                ("type".to_string(), "order".to_string())
            ]
        );
        assert_eq!(
            parse_filter("city=beijing"),
            vec![("city".to_string(), "beijing".to_string())]
        );
        assert!(parse_filter("").is_empty());
        assert!(parse_filter("no-equals-here").is_empty());
    }

    #[test]
    fn url_decode_handles_percent_and_plus() {
        assert_eq!(
            url_decode("status%3Dactive%20AND%20type%3Dorder"),
            "status=active AND type=order"
        );
        assert_eq!(url_decode("a+b"), "a b");
        assert_eq!(url_decode("100%"), "100%");
    }

    #[test]
    fn extract_terms_collects_string_values() {
        let v: Value = serde_json::from_str(
            r#"{"docid":1,"status":"active","n":3,"fields":{"type":"click","flag":true}}"#,
        )
        .unwrap();
        let terms = extract_terms(&v);
        // term 编码为 field=value（development 5.17 字段维度）
        assert!(terms.contains(&"status=active".to_string()));
        assert!(terms.contains(&"fields.type=click".to_string()));
        assert!(!terms.contains(&"1".to_string()));
        assert!(!terms.contains(&"true".to_string()));
        assert!(!terms.iter().any(|t| t == "active"), "裸值不应作为词条");
    }

    #[test]
    fn extract_terms_array_path() {
        let v: Value = serde_json::from_str(r#"{"docid":1,"tags":["hot","new"]}"#).unwrap();
        let terms = extract_terms(&v);
        assert!(terms.contains(&"tags.0=hot".to_string()));
        assert!(terms.contains(&"tags.1=new".to_string()));
    }

    // ---------- fulltext 分词索引（M8-P7） ----------

    #[test]
    fn tokenize_fulltext_boundaries() {
        // ASCII 典型文本（gen_dataset big_text 格式）
        assert_eq!(
            tokenize("rec-00000001-msg-73-tag42"),
            vec!["rec", "00000001", "msg", "73", "tag42"]
        );
        // 大小写归一 / 空串 / 纯分隔符 / 重复分隔符
        assert_eq!(tokenize("Hello World!"), vec!["hello", "world"]);
        assert_eq!(tokenize("ABC-def"), vec!["abc", "def"]);
        assert!(tokenize("").is_empty());
        assert!(tokenize("---").is_empty());
        assert_eq!(tokenize("a--b--"), vec!["a", "b"]);
        // Unicode：连续中文字符 → bigram（M8-P9；单字回退 unigram）
        assert_eq!(
            tokenize("山水存迹数据库"),
            vec!["山水", "水存", "存迹", "迹数", "数据", "据库"]
        );
        assert_eq!(tokenize("山"), vec!["山"], "单字中文回退 unigram");
        // 混合：ASCII 单词独立 + 中文 bigram
        assert_eq!(tokenize("Rust数据库"), vec!["rust", "数据", "据库"]);
        assert_eq!(tokenize("hello山水world"), vec!["hello", "山水", "world"]);
        // 中文 + 标点分隔
        assert_eq!(tokenize("山水，存迹"), vec!["山水", "存迹"]);
    }

    #[test]
    fn jieba_tokenize_seg_meaningful_words() {
        // M8-P13：jieba 完整词典分词——语义词整体切出（非 bigram 碎片）
        let words = tokenize_seg("山水存迹数据库存储引擎", true);
        assert!(
            words.contains(&"数据库".to_string()),
            "词典词应整体切出: {words:?}"
        );
        // 同文本 bigram 对比：碎片更多
        let bg = tokenize_seg("山水存迹数据库存储引擎", false);
        assert!(bg.contains(&"数据".to_string()) && bg.contains(&"据库".to_string()));
        assert!(bg.len() >= words.len(), "jieba 词数应 ≤ bigram 碎片数");
        // 中英混合：英文单词保留 + 中文词典词
        let mixed = tokenize_seg("基于Rust的LSM树文档数据库", true);
        assert!(mixed.contains(&"rust".to_string()), "英文单词应保留: {mixed:?}");
        assert!(mixed.contains(&"数据库".to_string()));
        // 标点/空白过滤
        assert!(tokenize_seg("，。！ ", true).is_empty());
        // 默认 tokenize = bigram（不受 jieba 影响）
        assert_eq!(tokenize("数据库"), vec!["数据", "据库"]);
    }

    #[test]
    fn extract_fulltext_terms_field_precedence() {
        let v: Value =
            serde_json::from_str(r#"{"docid":1,"status":"active","big_text":"rec-0001-msg-77"}"#)
                .unwrap();
        let ft: std::collections::HashSet<String> =
            ["big_text".to_string()].into_iter().collect();
        let terms = extract_terms_with_fulltext(&v, None, Some(&ft));
        // fulltext 字段：分词建词 term（含去重），不建整串
        assert!(terms.contains(&"ft:big_text:rec".to_string()));
        assert!(terms.contains(&"ft:big_text:0001".to_string()));
        assert!(terms.contains(&"ft:big_text:77".to_string()));
        assert!(
            !terms.iter().any(|t| t == "big_text=rec-0001-msg-77"),
            "fulltext 字段不应建整串 term"
        );
        // 非 fulltext 字段整串 term 不受影响
        assert!(terms.contains(&"status=active".to_string()));
    }

    #[test]
    fn extract_fulltext_orthogonal_to_whitelist() {
        let v: Value =
            serde_json::from_str(r#"{"docid":1,"status":"active","big_text":"rec-0001"}"#).unwrap();
        let include: std::collections::HashSet<String> =
            ["status".to_string()].into_iter().collect();
        let ft: std::collections::HashSet<String> =
            ["big_text".to_string()].into_iter().collect();
        let terms = extract_terms_with_fulltext(&v, Some(&include), Some(&ft));
        // fulltext 字段不受白名单影响（正交）：分词词 term 保留
        assert!(terms.iter().any(|t| t.starts_with("ft:big_text:")));
        // 非 fulltext 字段受白名单过滤：big_text 整串 / 其他字段整串被剔除
        assert!(!terms.iter().any(|t| t.starts_with("big_text=")));
    }

    // ---------- 分页查询（M8-P8） ----------

    #[test]
    fn execute_filter_paged_returns_total_and_limit() {
        let dir = tmp();
        let mut e = crate::engine::Engine::open(&dir, &cfg()).unwrap();
        for i in 0..100u64 {
            let status = ["active", "inactive", "pending"][(i % 3) as usize];
            let val = serde_json::json!({"docid": i, "status": status});
            let terms = extract_terms(&val);
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put_nosync(i, serde_json::to_vec(&val).unwrap(), &t).unwrap();
        }
        e.flush_inverted().unwrap();
        // 单条件分页：total = 全量命中，rows = 当前页
        let p = execute_filter_paged(&mut e, "status=active", Some(10), 0).unwrap();
        assert_eq!(p.total, 34);
        assert_eq!(p.rows.len(), 10);
        let last = execute_filter_paged(&mut e, "status=active", Some(10), 30).unwrap();
        assert_eq!(last.rows.len(), 4, "尾页不足一页");
        // 多条件 AND 分页
        let and = execute_filter_paged(&mut e, "status=active AND status=active", Some(5), 0).unwrap();
        assert_eq!(and.total, 34);
        assert_eq!(and.rows.len(), 5);
        // docid 点查不受分页影响
        let point = execute_filter_paged(&mut e, "docid=7", Some(1), 0).unwrap();
        assert_eq!(point.total, 1);
        assert_eq!(point.rows.len(), 1);
    }

    #[test]
    fn parse_paging_query_params() {
        assert_eq!(parse_paging(""), (None, 0));
        assert_eq!(parse_paging("limit=10"), (Some(10), 0));
        assert_eq!(parse_paging("limit=0"), (None, 0), "limit=0 视为不限制");
        assert_eq!(parse_paging("limit=10&offset=20"), (Some(10), 20));
        assert_eq!(parse_paging("offset=5"), (None, 5));
        assert_eq!(parse_paging("limit=abc&offset=xyz"), (None, 0), "非法参数忽略");
    }

    #[test]
    fn http_put_with_enrich_expands_related_doc() {
        // 写入 Enrich（design 19）：`[enrich] enabled && source=local` → /put WAL 前展开关联文档
        let dir = tmp();
        let mut c = cfg();
        c.enrich.enabled = true;
        c.enrich.source = "local".into();
        c.enrich.fail_policy = "degrade".into(); // 关联缺失/无关联字段 → 降级写原文档
        c.enrich.from_field = "user_id".into();
        c.enrich.to_field = "docid".into();
        let engine = Engine::open(&dir, &c).unwrap();
        assert!(engine.enrich_config().is_some(), "enrich 配置生效");
        let addr = spawn_server(engine, None, None);

        // 关联文档（user 档案 docid=7，无 user_id → 降级正常写入）
        let (st, body) = http_req(addr, "POST", "/put", br#"{"docid":7,"name":"alice","city":"beijing"}"#);
        assert_eq!(st, 200, "关联文档写入失败: {body}");

        // 主文档：order 引用 user_id=7 → 写入时展开 _enrich.related
        let (st, body) = http_req(addr, "POST", "/put", br#"{"docid":1001,"user_id":7,"amount":99}"#);
        assert_eq!(st, 200, "主文档写入失败: {body}");
        let (st, body) = http_req(addr, "GET", "/get?docid=1001", b"");
        assert_eq!(st, 200);
        assert!(body.contains("_enrich"), "应展开关联文档: {body}");
        assert!(body.contains("alice"), "关联字段展开: {body}");

        // 关联缺失（user_id=999）：degrade 策略 → 降级写入原文档（不展开）
        let (st, _) = http_req(addr, "POST", "/put", br#"{"docid":2002,"user_id":999,"amount":1}"#);
        assert_eq!(st, 200, "degrade 应降级写入");
        let (st, body) = http_req(addr, "GET", "/get?docid=2002", b"");
        assert!(st == 200 && !body.contains("_enrich"), "降级文档不展开: {body}");
    }

    #[test]
    fn http_end_to_end_crud_and_search() {
        let dir = tmp();
        let engine = Engine::open(&dir, &cfg()).unwrap();
        let addr = spawn_server(engine, None, None);

        // PUT
        let (st, body) = http_req(
            addr,
            "POST",
            "/put",
            br#"{"docid":1001,"status":"active","type":"order","device":"android"}"#,
        );
        assert_eq!(st, 200, "put 失败: {body}");
        assert!(body.contains("\"ok\":true"));

        // PUT 第二条
        http_req(
            addr,
            "POST",
            "/put",
            br#"{"docid":2002,"status":"active","type":"view","device":"ios"}"#,
        );

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
        let (st, body) = http_req(
            addr,
            "GET",
            "/search?filter=status%3Dactive%20AND%20type%3Dorder",
            b"",
        );
        assert_eq!(st, 200, "search-and 失败: {body}");
        assert!(body.contains("\"total\":1"), "交集应为 1 条: {body}");
        assert!(body.contains("1001"));

        // COUNT（阶段 1.5 M4 聚合：status=active 共 2 条）
        let (st, body) = http_req(addr, "GET", "/count?field=status&value=active", b"");
        assert_eq!(st, 200, "count 失败: {body}");
        assert!(body.contains("\"count\":2"), "count 应为 2: {body}");

        // GROUP BY（阶段 1.5 M4 聚合：status 分组 active=2 / view? —— 2002 为 active）
        let (st, body) = http_req(addr, "GET", "/groupby?field=status", b"");
        assert_eq!(st, 200, "groupby 失败: {body}");
        assert!(
            body.contains("\"value\":\"active\""),
            "groupby 缺 active 组: {body}"
        );
        assert!(body.contains("\"count\":2"), "active 组应为 2: {body}");

        // 缺失参数 → 400
        let (st, _) = http_req(addr, "GET", "/count?field=status", b"");
        assert_eq!(st, 400);
        let (st, _) = http_req(addr, "GET", "/groupby", b"");
        assert_eq!(st, 400);

        // JOIN（design 19）：user 文档 + order 文档按 username 关联
        let (st, body) = http_req(
            addr,
            "POST",
            "/put",
            br#"{"docid":9001,"type":"user","username":"alice"}"#,
        );
        assert_eq!(st, 200, "put user 失败: {body}");
        let (st, body) = http_req(
            addr,
            "POST",
            "/put",
            br#"{"docid":9002,"type":"order","buyer":"alice","amount":99}"#,
        );
        assert_eq!(st, 200, "put order 失败: {body}");
        let (st, body) = http_req(
            addr,
            "GET",
            "/join?filter=type%3Dorder&from=buyer&to=username",
            b"",
        );
        assert_eq!(st, 200, "join 失败: {body}");
        assert!(body.contains("\"total\":1"), "join 应命中 1 行: {body}");
        assert!(body.contains("alice"), "应包含关联用户: {body}");
        let (st, _) = http_req(addr, "GET", "/join?filter=type%3Dorder", b"");
        assert_eq!(st, 400, "缺 from/to 应 400");

        // RANGE（[1000,2000] 仅含 1001；2002 在外）
        let (st, body) = http_req(addr, "GET", "/range?start=1000&end=2000", b"");
        assert_eq!(st, 200, "range 失败: {body}");
        assert!(body.contains("\"total\":1"), "范围应命中 1 条: {body}");

        // PATCH（阶段 1.5 部分更新）：覆盖 device + 新增 note
        let (st, body) = http_req(
            addr,
            "POST",
            "/patch",
            br#"{"docid":2002,"fields":{"device":"linux","note":"patched"}}"#,
        );
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

    // -----------------------------------------------------------------------
    // Ex-2.5 SAGA 网关端到端测试：模拟业务节点 + 网关 /saga/* 端点
    // -----------------------------------------------------------------------

    /// 模拟业务节点（HTTP 步骤端点）：
    /// - POST 路径含 `compensate` → 补偿计数 + 200（幂等）；
    /// - POST 路径含 `action` → 路径含 `fail_on` 则 500（业务失败），否则 200 + 计数；
    /// - 其余 404。
    fn mock_biz_node(
        fail_on: &str,
    ) -> (
        std::net::SocketAddr,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let action_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let comp_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (a2, c2) = (action_calls.clone(), comp_calls.clone());
        let fail_on = fail_on.to_string();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut s) = stream else { break };
                let mut buf = Vec::new();
                let mut tmp = [0u8; 1024];
                loop {
                    match s.read(&mut tmp) {
                        Ok(0) => break,
                        Ok(n) => {
                            buf.extend_from_slice(&tmp[..n]);
                            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                let req = String::from_utf8_lossy(&buf).to_string();
                let path = req.split_whitespace().nth(1).unwrap_or("/").to_string();
                let (status, body) = if path.contains("compensate") {
                    c2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    (200, "compensated")
                } else if path.contains("action") {
                    if fail_on.is_empty() || !path.contains(&fail_on) {
                        a2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        (200, "ok")
                    } else {
                        (500, "business fail")
                    }
                } else {
                    (404, "not found")
                };
                let resp = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(resp.as_bytes());
            }
        });
        (addr, action_calls, comp_calls)
    }

    #[test]
    fn saga_gateway_forward_success_no_compensate() {
        let dir = tmp();
        let (n1, a1, c1) = mock_biz_node("");
        let (n2, a2, c2) = mock_biz_node("");
        let engine = Engine::open(&dir, &cfg()).unwrap();
        let coord = crate::saga::SagaCoordinator::open(&dir.join("saga")).unwrap();
        let addr = spawn_server(engine, Some(Arc::new(Mutex::new(coord))), None);

        let body = format!(
            r#"{{"tx_id":"t1","steps":[{{"name":"debit","action_url":"http://{n1}/debit/action","compensate_url":"http://{n1}/debit/compensate"}},{{"name":"credit","action_url":"http://{n2}/credit/action","compensate_url":"http://{n2}/credit/compensate"}}]}}"#
        );
        let (st, resp) = http_req(addr, "POST", "/saga/start", body.as_bytes());
        assert_eq!(st, 200, "start 失败: {resp}");
        assert!(resp.contains("Succeeded"), "正向应成功: {resp}");
        assert_eq!(a1.load(Ordering::SeqCst), 1, "debit 正向 1 次");
        assert_eq!(a2.load(Ordering::SeqCst), 1, "credit 正向 1 次");
        assert_eq!(c1.load(Ordering::SeqCst), 0, "成功路径无补偿");
        assert_eq!(c2.load(Ordering::SeqCst), 0);

        // 状态回查（屏障接口依据）
        let (st, resp) = http_req(addr, "GET", "/saga/status?tx_id=t1", b"");
        assert_eq!(st, 200);
        assert!(resp.contains("Succeeded"), "回查状态: {resp}");

        // 持久化状态文件（崩溃恢复依据）
        let saved = dir.join("saga").join("saga-t1.json");
        assert!(saved.exists(), "状态应持久化: {}", saved.display());
    }

    #[test]
    fn saga_gateway_mid_failure_reverse_compensate() {
        let dir = tmp();
        let (n1, a1, c1) = mock_biz_node("");
        let (n2, _a2, _c2) = mock_biz_node("credit/action"); // credit 业务失败
        let engine = Engine::open(&dir, &cfg()).unwrap();
        let coord = crate::saga::SagaCoordinator::open(&dir.join("saga")).unwrap();
        let addr = spawn_server(engine, Some(Arc::new(Mutex::new(coord))), None);

        let body = format!(
            r#"{{"tx_id":"t2","steps":[{{"name":"debit","action_url":"http://{n1}/debit/action","compensate_url":"http://{n1}/debit/compensate"}},{{"name":"credit","action_url":"http://{n2}/credit/action","compensate_url":"http://{n2}/credit/compensate"}}]}}"#
        );
        let (st, resp) = http_req(addr, "POST", "/saga/start", body.as_bytes());
        assert_eq!(st, 200, "start 失败: {resp}");
        assert!(resp.contains("Compensated"), "中段失败应补偿完成: {resp}");
        assert_eq!(a1.load(Ordering::SeqCst), 1, "debit 正向 1 次");
        assert_eq!(c1.load(Ordering::SeqCst), 1, "仅已登记分支（debit）被补偿");
        assert_eq!(c1.load(Ordering::SeqCst), 1, "补偿幂等（不重复）");

        // 重发同 tx（终态幂等）：不重复执行/补偿
        let (st, resp) = http_req(addr, "POST", "/saga/start", body.as_bytes());
        assert_eq!(st, 200);
        assert!(resp.contains("Compensated"));
        assert_eq!(a1.load(Ordering::SeqCst), 1, "终态拒绝重复正向");
        assert_eq!(c1.load(Ordering::SeqCst), 1, "终态拒绝重复补偿");
    }

    #[test]
    fn saga_gateway_state_persists_across_restart() {
        let dir = tmp();
        let (n1, a1, c1) = mock_biz_node("");
        let (n2, a2, _c2) = mock_biz_node("");
        {
            let engine = Engine::open(&dir, &cfg()).unwrap();
            let coord = crate::saga::SagaCoordinator::open(&dir.join("saga")).unwrap();
            let addr = spawn_server(engine, Some(Arc::new(Mutex::new(coord))), None);
            let body = format!(
                r#"{{"tx_id":"t3","steps":[{{"name":"a","action_url":"http://{n1}/a/action","compensate_url":"http://{n1}/a/compensate"}},{{"name":"b","action_url":"http://{n2}/b/action","compensate_url":"http://{n2}/b/compensate"}}]}}"#
            );
            let (st, resp) = http_req(addr, "POST", "/saga/start", body.as_bytes());
            assert_eq!(st, 200, "{resp}");
            assert!(resp.contains("Succeeded"), "{resp}");
        } // 网关线程随测试作用域结束丢弃 = 服务重启
        // 重开协调器：从磁盘恢复终态（崩溃恢复续跑依据）
        let coord2 = crate::saga::SagaCoordinator::open(&dir.join("saga")).unwrap();
        let st = coord2.status("t3").unwrap();
        assert_eq!(st.status, crate::saga::SagaStatus::Succeeded, "重启恢复终态");
        assert_eq!(st.executed_steps, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(a1.load(Ordering::SeqCst), 1);
        assert_eq!(a2.load(Ordering::SeqCst), 1);
        assert_eq!(c1.load(Ordering::SeqCst), 0, "成功路径无补偿");
    }

    #[test]
    fn saga_gateway_depends_on_parallel_and_cycle_rejected() {
        // 13.6 网关：depends_on 拓扑并行成功；环 → 400；未知依赖 → 400
        let dir = tmp();
        let (n1, a1, _c1) = mock_biz_node("");
        let (n2, a2, _c2) = mock_biz_node("");
        let (n3, a3, _c3) = mock_biz_node("");
        let engine = Engine::open(&dir, &cfg()).unwrap();
        let coord = crate::saga::SagaCoordinator::open(&dir.join("saga")).unwrap();
        let addr = spawn_server(engine, Some(Arc::new(Mutex::new(coord))), None);

        // b 依赖 a；c 无依赖 → 拓扑层 [a,c] → [b]（c 与 a 并行）
        let body = format!(
            r#"{{"tx_id":"d1","steps":[
                {{"name":"a","action_url":"http://{n1}/a/action","compensate_url":"http://{n1}/a/compensate"}},
                {{"name":"b","action_url":"http://{n2}/b/action","compensate_url":"http://{n2}/b/compensate","depends_on":["a"]}},
                {{"name":"c","action_url":"http://{n3}/c/action","compensate_url":"http://{n3}/c/compensate"}}
            ]}}"#
        );
        let (st, resp) = http_req(addr, "POST", "/saga/start", body.as_bytes());
        assert_eq!(st, 200, "依赖并行 start 失败: {resp}");
        assert!(resp.contains("Succeeded"), "{resp}");
        assert_eq!(a1.load(Ordering::SeqCst), 1);
        assert_eq!(a2.load(Ordering::SeqCst), 1, "依赖者 b 已执行");
        assert_eq!(a3.load(Ordering::SeqCst), 1);

        // 环 x→y→x → 400
        let body2 = format!(
            r#"{{"tx_id":"d2","steps":[
                {{"name":"x","action_url":"http://{n1}/x/action","compensate_url":"http://{n1}/x/compensate","depends_on":["y"]}},
                {{"name":"y","action_url":"http://{n2}/y/action","compensate_url":"http://{n2}/y/compensate","depends_on":["x"]}}
            ]}}"#
        );
        let (st, resp) = http_req(addr, "POST", "/saga/start", body2.as_bytes());
        assert_eq!(st, 400, "环依赖应 400: {resp}");
        assert!(resp.contains("构成环"), "{resp}");

        // 未知依赖 → 400
        let body3 = format!(
            r#"{{"tx_id":"d3","steps":[
                {{"name":"a","action_url":"http://{n1}/a/action","compensate_url":"http://{n1}/a/compensate","depends_on":["ghost"]}}
            ]}}"#
        );
        let (st, resp) = http_req(addr, "POST", "/saga/start", body3.as_bytes());
        assert_eq!(st, 400, "未知依赖应 400: {resp}");
        assert!(resp.contains("未知步骤"), "{resp}");
    }
}
