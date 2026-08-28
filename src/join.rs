//! 数据关联基础（development 5.20 sdk::join / 5.21 写入 Enrich，design 19）。
//!
//! - **queryAndJoin**：主表倒排筛选 → 批量回表 → 从表批量主键点查 → 内存 Hash 合并
//!   （Inner / Left / Right），结果集上限 `join.max_rows` 熔断；
//! - **写入 Enrich**：网络层接收后、WAL 写入前执行回调展开关联数据到单文档，
//!   失败策略 reject（拒绝写入）/ degrade（降级写入原文档）。

use serde_json::{json, Value};

use crate::engine::Engine;
use crate::error::{Error, Result};

/// JOIN 类型（design 19）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

/// queryAndJoin 执行规格。
#[derive(Debug, Clone)]
pub struct JoinSpec<'a> {
    /// 主表倒排筛选条件（execute_filter 语义，如 `type=order`）。
    pub filter: &'a str,
    /// 主表关联字段（取该字段值作为关联 key）。
    pub from_field: &'a str,
    /// 从表关联字段："docid"（主键点查）或其他字段（倒排 term `field=key` 查询）。
    pub to_field: &'a str,
    /// JOIN 类型。
    pub join_type: JoinType,
}

/// 一行 JOIN 结果：左表文档 + 右表文档（均保留原字段，避免命名冲突）。
#[derive(Debug, Clone)]
pub struct JoinRow {
    pub left: Value,
    pub right: Option<Value>,
}

/// 从表按关联 key 查询：`docid` 主键点查；否则倒排 `field=key` 取首个文档。
fn fetch_related(engine: &mut Engine, to_field: &str, key: &str) -> Result<Option<Value>> {
    if to_field == "docid" || to_field == "id" {
        let docid: u64 = key
            .parse()
            .map_err(|_| Error::Unsupported(format!("关联 key 非数字，无法主键点查: {key}")))?;
        return match engine.get(docid)? {
            Some(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| Error::Serialize(format!("从表文档解析失败: {e}"))),
            None => Ok(None),
        };
    }
    let term = format!("{to_field}={key}");
    let rows = engine.search_term(&term)?;
    if let Some((docid, bytes)) = rows.into_iter().next() {
        let _ = docid;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| Error::Serialize(format!("从表文档解析失败: {e}")))
    } else {
        Ok(None)
    }
}

/// 提取文档中字段值并转为字符串关联 key（字符串 / 数字 / 布尔）。
fn field_to_key(val: &Value, field: &str) -> Option<String> {
    match val.get(field) {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Number(n)) => Some(n.to_string()),
        Some(Value::Bool(b)) => Some(b.to_string()),
        _ => None,
    }
}

/// queryAndJoin：主表倒排筛选 → 回表 → 从表批量点查 → 内存 Hash 合并。
/// 结果集超过 `max_rows` 熔断（design 5.20 / design 19）。
pub fn query_and_join(
    engine: &mut Engine,
    spec: &JoinSpec,
    max_rows: usize,
) -> Result<Vec<JoinRow>> {
    let t = std::time::Instant::now();
    // ① 主表倒排筛选 + 回表
    let left_rows = crate::server::execute_filter(engine, spec.filter)?;
    if left_rows.len() > max_rows {
        return Err(Error::QueryTooExpensive(format!(
            "JOIN 主表结果 {} 行超过上限 {max_rows}，熔断（可缩小 filter 或改用导出）",
            left_rows.len()
        )));
    }
    // ② 主表文档 → (关联 key, 文档)
    let mut lefts: Vec<(Option<String>, Value)> = Vec::with_capacity(left_rows.len());
    for (_, bytes) in &left_rows {
        let val: Value = serde_json::from_slice(bytes)
            .map_err(|e| Error::Serialize(format!("主表文档解析失败: {e}")))?;
        let key = field_to_key(&val, spec.from_field);
        lefts.push((key, val));
    }
    // ③ 从表批量点查（去重 key，避免重复 IO）
    let mut right_cache: std::collections::HashMap<String, Option<Value>> =
        std::collections::HashMap::new();
    let unique_keys: std::collections::HashSet<String> =
        lefts.iter().filter_map(|(k, _)| k.clone()).collect();
    for key in unique_keys {
        right_cache.insert(key.clone(), fetch_related(engine, spec.to_field, &key)?);
    }
    // ④ 合并
    let mut out = Vec::new();
    for (key, left) in lefts {
        let right = match &key {
            Some(k) => right_cache.get(k).cloned().flatten(),
            None => None,
        };
        let has_right = right.is_some();
        match spec.join_type {
            JoinType::Inner => {
                if has_right {
                    out.push(JoinRow { left, right });
                }
            }
            JoinType::Left => out.push(JoinRow { left, right }),
            // Right（基础版）：从表无独立筛选，等价于 Inner 输出的行（右表需左命中才有意义）
            JoinType::Right => {
                if has_right {
                    out.push(JoinRow { left, right });
                }
            }
        }
    }
    let _ = t;
    Ok(out)
}

/// 写入 Enrich（development 5.21）：WAL 写入前执行回调修改文档。
/// - 回调成功 → 修改后文档写入；
/// - 回调失败 → `reject` 拒绝写入 / `degrade` 用原文档降级写入。
pub fn put_with_enrich<F>(
    engine: &mut Engine,
    docid: u64,
    value: Vec<u8>,
    terms: &[&str],
    fail_policy: &str,
    enrich: F,
) -> Result<()>
where
    F: FnOnce(&mut Engine, &mut Value) -> Result<()>,
{
    let mut val: Value = serde_json::from_slice(&value)
        .map_err(|e| Error::Serialize(format!("Enrich 前置文档解析失败: {e}")))?;
    match enrich(engine, &mut val) {
        Ok(()) => {
            let bytes = serde_json::to_vec(&val)
                .map_err(|e| Error::Serialize(format!("Enrich 后文档序列化失败: {e}")))?;
            engine.put(docid, bytes, terms)
        }
        Err(e) => match fail_policy {
            "reject" => Err(Error::Unsupported(format!("Enrich 失败已拒绝写入: {e}"))),
            _ => {
                // degrade：降级写入原文档（不展开关联数据）
                engine.put(docid, value, terms)
            }
        },
    }
}

/// local 数据源 Enrich（基础版）：把关联文档读取为 JSON 对象（不入主文档，仅验证关联存在性）。
/// 供 put_with_enrich 回调使用：若关联缺失则返回错误（由 fail_policy 决定 reject / degrade）。
pub fn enrich_check_local(
    engine: &mut Engine,
    val: &mut Value,
    from_field: &str,
    to_field: &str,
) -> Result<()> {
    let key = field_to_key(val, from_field)
        .ok_or_else(|| Error::Unsupported(format!("主文档缺少关联字段 {from_field}")))?;
    match fetch_related(engine, to_field, &key)? {
        Some(related) => {
            // 展开：`_enrich` 子对象保留关联文档（避免字段冲突）
            val.as_object_mut()
                .ok_or_else(|| Error::Unsupported("Enrich 目标非 JSON 对象".into()))?
                .insert("_enrich".into(), json!({ "related": related }));
            Ok(())
        }
        None => Err(Error::Unsupported(format!(
            "关联文档缺失: {to_field}={key}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine_with(dir: &std::path::Path) -> Engine {
        Engine::open(dir, &crate::config::Config::default()).unwrap()
    }

    fn put(engine: &mut Engine, docid: u64, val: Value) {
        let bytes = serde_json::to_vec(&val).unwrap();
        let terms = crate::server::extract_terms(&val);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        engine.put(docid, bytes, &t).unwrap();
    }

    #[test]
    fn inner_join_matches_related_docs() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = engine_with(&dir.path());
        // 从表：user 文档
        put(
            &mut e,
            100,
            json!({"docid":100,"type":"user","name":"alice"}),
        );
        put(&mut e, 200, json!({"docid":200,"type":"user","name":"bob"}));
        // 主表：order 文档（user_id 关联）
        put(
            &mut e,
            1,
            json!({"docid":1,"type":"order","user_id":"100","amount":10}),
        );
        put(
            &mut e,
            2,
            json!({"docid":2,"type":"order","user_id":"200","amount":20}),
        );
        put(
            &mut e,
            3,
            json!({"docid":3,"type":"order","user_id":"999","amount":30}),
        ); // 无关联

        let spec = JoinSpec {
            filter: "type=order",
            from_field: "user_id",
            to_field: "docid",
            join_type: JoinType::Inner,
        };
        let rows = query_and_join(&mut e, &spec, 1000).unwrap();
        assert_eq!(rows.len(), 2, "Inner 应只保留有关联的行");
        assert_eq!(rows[0].right.as_ref().unwrap()["name"], "alice");
        assert_eq!(rows[1].right.as_ref().unwrap()["name"], "bob");
    }

    #[test]
    fn left_join_keeps_all_left_rows() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = engine_with(&dir.path());
        put(
            &mut e,
            100,
            json!({"docid":100,"type":"user","name":"alice"}),
        );
        put(&mut e, 1, json!({"docid":1,"type":"order","user_id":"100"}));
        put(&mut e, 2, json!({"docid":2,"type":"order","user_id":"999"}));

        let spec = JoinSpec {
            filter: "type=order",
            from_field: "user_id",
            to_field: "docid",
            join_type: JoinType::Left,
        };
        let rows = query_and_join(&mut e, &spec, 1000).unwrap();
        assert_eq!(rows.len(), 2, "Left 应保留全部主表行");
        assert!(rows[0].right.is_some());
        assert!(rows[1].right.is_none(), "缺失关联的行 right=None");
    }

    #[test]
    fn join_to_field_via_inverted() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = engine_with(&dir.path());
        // 从表用非主键字段关联：user 文档的 username 字段
        put(
            &mut e,
            100,
            json!({"docid":100,"type":"user","username":"alice"}),
        );
        put(&mut e, 1, json!({"docid":1,"type":"order","buyer":"alice"}));

        let spec = JoinSpec {
            filter: "type=order",
            from_field: "buyer",
            to_field: "username",
            join_type: JoinType::Inner,
        };
        let rows = query_and_join(&mut e, &spec, 1000).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].right.as_ref().unwrap()["docid"], 100);
    }

    #[test]
    fn join_max_rows_fuse() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = engine_with(&dir.path());
        for i in 1..=5u64 {
            put(&mut e, i, json!({"docid": i, "type": "order"}));
        }
        let spec = JoinSpec {
            filter: "type=order",
            from_field: "user_id",
            to_field: "docid",
            join_type: JoinType::Left,
        };
        let err = query_and_join(&mut e, &spec, 3).unwrap_err();
        assert!(err.to_string().contains("熔断"), "超限应熔断: {err}");
    }

    #[test]
    fn enrich_degrade_writes_original_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = engine_with(&dir.path());
        let val = json!({"docid":1,"type":"order","user_id":"999"});
        let bytes = serde_json::to_vec(&val).unwrap();
        let terms = crate::server::extract_terms(&val);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        // 关联缺失 + degrade → 原文档写入成功
        put_with_enrich(&mut e, 1, bytes.clone(), &t, "degrade", |eng, v| {
            enrich_check_local(eng, v, "user_id", "docid")
        })
        .unwrap();
        let got = e.get(1).unwrap().expect("文档应写入");
        assert!(
            String::from_utf8_lossy(&got).contains("user_id"),
            "降级写入原文档"
        );
    }

    #[test]
    fn enrich_reject_denies_write_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = engine_with(&dir.path());
        let val = json!({"docid":1,"type":"order","user_id":"999"});
        let bytes = serde_json::to_vec(&val).unwrap();
        let terms = crate::server::extract_terms(&val);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        let err = put_with_enrich(&mut e, 1, bytes, &t, "reject", |eng, v| {
            enrich_check_local(eng, v, "user_id", "docid")
        })
        .unwrap_err();
        assert!(err.to_string().contains("拒绝写入"), "reject 应拒绝: {err}");
        assert!(e.get(1).unwrap().is_none(), "拒绝后不应写入");
    }

    #[test]
    fn enrich_appends_related_doc() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = engine_with(&dir.path());
        put(
            &mut e,
            100,
            json!({"docid":100,"type":"user","name":"alice"}),
        );
        let val = json!({"docid":1,"type":"order","user_id":"100"});
        let bytes = serde_json::to_vec(&val).unwrap();
        let terms = crate::server::extract_terms(&val);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        put_with_enrich(&mut e, 1, bytes, &t, "reject", |eng, v| {
            enrich_check_local(eng, v, "user_id", "docid")
        })
        .unwrap();
        let got = e.get(1).unwrap().expect("文档应写入");
        let got_val: Value = serde_json::from_slice(&got).unwrap();
        assert_eq!(
            got_val["_enrich"]["related"]["name"], "alice",
            "应展开关联文档"
        );
    }
}
