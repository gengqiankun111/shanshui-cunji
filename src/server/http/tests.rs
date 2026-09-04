//! HTTP 网关测试（原 http.rs 内 `#[cfg(test)] mod tests` 整体迁出）：
//! parse/tokenize 单元测试 + HTTP 端到端（CRUD/搜索/JOIN/RANGE/PATCH + Enrich）+ Ex-2.5
//! SAGA 网关端到端（模拟业务节点 / 跨分片真实节点）。经 `use super::*` 访问 http 根
//! 公开面与内部项，行为与拆分前一致。

use super::doc_api::parse_paging;
use super::*;
use crate::engine::Engine;
use serde_json::Value;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
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

/// 跨分片真实业务节点（design_extension 13.2）：分片 = 独立 Engine（本地事务），
/// HTTP 端点执行真实余额变更——`/debit/action` 扣 100、`/debit/compensate` 加 100 回滚、
/// `/credit/action` 加 100（`fail_credit=true` 注入业务失败）、`/credit/compensate` 减 100 冲销、
/// `/balance` 返回两账户余额（docid 1=debit、2=credit）。补偿幂等：值型回退，重复调用余额不变。
fn spawn_shard_node(dir: &std::path::Path, fail_credit: std::sync::Arc<std::sync::atomic::AtomicBool>) -> std::net::SocketAddr {
    let mut engine = Engine::open(dir, &cfg()).unwrap();
    engine.put(1, b"1000".to_vec(), &[]).unwrap(); // debit 账户初始 1000
    engine.put(2, b"0".to_vec(), &[]).unwrap(); // credit 账户初始 0
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
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
            let bal = |id: u64| -> i64 {
                engine
                    .get(id)
                    .unwrap()
                    .map(|v| String::from_utf8_lossy(&v).parse().unwrap_or(0))
                    .unwrap_or(0)
            };
            let (status, body) = if path.starts_with("/balance") {
                (200, format!("{{\"debit\":{},\"credit\":{}}}", bal(1), bal(2)))
            } else if path.contains("/action") {
                if path.contains("debit") {
                    engine.put(1, format!("{}", bal(1) - 100).into_bytes(), &[]).unwrap();
                    (200, "ok".into())
                } else if fail_credit.load(Ordering::SeqCst) {
                    (500, "credit business fail".into()) // 注入：credit 节点业务失败
                } else {
                    engine.put(2, format!("{}", bal(2) + 100).into_bytes(), &[]).unwrap();
                    (200, "ok".into())
                }
            } else if path.contains("/compensate") {
                if path.contains("debit") {
                    engine.put(1, format!("{}", bal(1) + 100).into_bytes(), &[]).unwrap();
                } else {
                    engine.put(2, format!("{}", bal(2) - 100).into_bytes(), &[]).unwrap();
                }
                (200, "compensated".into())
            } else {
                (404, "not found".into())
            };
            let resp = format!(
                "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = s.write_all(resp.as_bytes());
        }
    });
    addr
}

/// 查询分片节点当前余额（debit/credit）。
fn shard_balance(addr: std::net::SocketAddr) -> (i64, i64) {
    let (st, resp) = http_req(addr, "GET", "/balance", b"");
    assert_eq!(st, 200, "{resp}");
    let v: Value = serde_json::from_str(&resp).unwrap();
    (
        v["debit"].as_i64().unwrap(),
        v["credit"].as_i64().unwrap(),
    )
}

#[test]
fn saga_cross_shard_transfer_end_to_end() {
    // 跨分片 2 节点真实联调（design_extension 13.2）：分片 A（debit）+ 分片 B（credit）
    // + 网关编排转账；验证正向 / 中段失败逆序补偿 / 补偿幂等重试。
    let gw_dir = tmp();
    let (sa_dir, sb_dir) = (tmp(), tmp());
    let fail_credit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let n1 = spawn_shard_node(&sa_dir, fail_credit.clone());
    let n2 = spawn_shard_node(&sb_dir, fail_credit.clone());
    let engine = Engine::open(&gw_dir, &cfg()).unwrap();
    let coord = crate::saga::SagaCoordinator::open(&gw_dir.join("saga")).unwrap();
    let addr = spawn_server(engine, Some(Arc::new(Mutex::new(coord))), None);

    // ---- 场景 1：正向转账（debit 扣 100 → credit 加 100）→ Succeeded ----
    let body = format!(
        r#"{{"tx_id":"tfr","steps":[
            {{"name":"debit","action_url":"http://{n1}/debit/action","compensate_url":"http://{n1}/debit/compensate"}},
            {{"name":"credit","action_url":"http://{n2}/credit/action","compensate_url":"http://{n2}/credit/compensate"}}
        ]}}"#
    );
    let (st, resp) = http_req(addr, "POST", "/saga/start", body.as_bytes());
    assert_eq!(st, 200, "{resp}");
    assert!(resp.contains("Succeeded"), "{resp}");
    assert_eq!(shard_balance(n1), (900, 0), "正向：debit 节点扣 100（credit 不归它管）");
    assert_eq!(shard_balance(n2), (1000, 100), "正向：credit 节点加 100（debit 不归它管）");

    // ---- 场景 2：中段失败（credit 业务失败）→ 逆序补偿 debit ----
    fail_credit.store(true, Ordering::SeqCst);
    let body2 = format!(
        r#"{{"tx_id":"tfail","steps":[
            {{"name":"debit","action_url":"http://{n1}/debit/action","compensate_url":"http://{n1}/debit/compensate"}},
            {{"name":"credit","action_url":"http://{n2}/credit/action","compensate_url":"http://{n2}/credit/compensate"}}
        ]}}"#
    );
    let (st, resp) = http_req(addr, "POST", "/saga/start", body2.as_bytes());
    assert_eq!(st, 200, "{resp}");
    assert!(resp.contains("Compensated"), "{resp}");
    assert_eq!(shard_balance(n1), (900, 0), "debit 已补偿回滚（tfail 扣 100 后又加回）");
    assert_eq!(shard_balance(n2), (1000, 100), "credit 节点未写入（action 失败未登记）");

    // ---- 场景 3：补偿幂等（对已补偿终态重复 compensate → no-op，余额不变）----
    let (st, resp) = http_req(
        addr,
        "POST",
        "/saga/compensate",
        format!(r#"{{"tx_id":"tfail","steps":[]}}"#).as_bytes(),
    );
    assert_eq!(st, 200, "{resp}");
    assert_eq!(shard_balance(n1), (900, 0), "补偿幂等：余额不重复回退");
}
