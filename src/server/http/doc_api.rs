//! 文档端点与查询执行：`/get` `/put` `/patch` `/search` `/sql` `/fulltext` `/range`
//! `/delete` `/count` `/groupby` `/join` `/explain` 各处理器；响应组装（value_row /
//! rows_payload_total / parse_paging）；CLI 与 HTTP 共用的查询执行路径（execute_*）。
//!
//! 解析辅助（parse_query/parse_filter/词条提取）在 json.rs，分词在 tokenize.rs。

use serde_json::{json, Value};

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::optimizer::QuerySpec;

use super::json::{extract_terms_with_fulltext_seg, parse_filter, parse_query};

/// COUNT(field=value)：`GET /count?field=status&value=active` → `{"count":N}`。
pub(crate) fn handle_count(engine: &mut Engine, query: &str) -> (u16, String) {
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

/// 执行计划推演（development 5.26）：`GET /explain?filter=status%3Dactive` → ExplainPlan JSON。
pub(crate) fn handle_explain(engine: &mut Engine, query: &str) -> (u16, String) {
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
pub(crate) fn handle_group_by(engine: &mut Engine, query: &str) -> (u16, String) {
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
pub(crate) fn handle_join(
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
pub(crate) fn handle_patch(engine: &mut Engine, body: &[u8]) -> (u16, String) {
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

pub(crate) fn handle_put(engine: &mut Engine, body: &[u8]) -> (u16, String) {
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

pub(crate) fn handle_get(engine: &mut Engine, query: &str) -> (u16, String) {
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

pub(crate) fn handle_search(engine: &mut Engine, query: &str) -> (u16, String) {
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
pub(crate) fn handle_sql(engine: &mut Engine, query: &str) -> (u16, String) {
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
pub(crate) fn handle_fulltext(engine: &mut Engine, query: &str) -> (u16, String) {
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

pub(crate) fn handle_range(engine: &mut Engine, query: &str) -> (u16, String) {
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

pub(crate) fn handle_delete(engine: &mut Engine, body: &[u8], query: &str) -> (u16, String) {
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

/// 解析分页参数：`limit`（>0 生效，缺省/≤0 = 不限制）、`offset`（默认 0）。
pub(crate) fn parse_paging(query: &str) -> (Option<u64>, u64) {
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

// ---------------------------------------------------------------------------
// 查询执行（CLI 与 HTTP 共用内核路径）
// ---------------------------------------------------------------------------

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
