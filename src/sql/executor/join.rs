//! JOIN 执行（原 sqlish.rs P0-D 段）：`execute_join`（阶段 1 主表 WHERE 候选集 →
//! 阶段 2 关联 key 点查/倒排查 → 1:N 展开 Hash 合并 → 阶段 3 LIMIT 下推）+
//! 从表 key 提取 / 结果文档嵌套合并辅助。

use crate::engine::{Engine, QueryRow};
use crate::error::{Error, Result};
use crate::sql::parser::{JoinKind, Select};

use super::eval::full_docids;
use super::select::get_docid_set;

/// P0-D：JOIN 执行（参考 research/optimizer_proces.md 8 阶段流程）。
/// 阶段 1：主表 WHERE 独立产出候选集（eval → bitmap）
/// 阶段 2：JOIN 路径——主表候选 batch_get → 提取关联 key → 从表点查/倒排查 → Hash 合并
/// 阶段 3：LIMIT 下推
/// 安全阀：非等值 JOIN 拒绝、表数 ≥3 拒绝（解析期）、从表非 docid 字段倒排空 = 无匹配
pub(crate) fn execute_join(engine: &Engine, sel: &Select, cap: u64) -> Result<Vec<QueryRow>> {
    let join = sel.join.as_ref().unwrap();
    // 剥离表前缀（`orders.user_id` → `user_id`）
    let left_field = join.left_field.rsplit('.').next().unwrap_or(&join.left_field);
    let right_field = join.right_field.rsplit('.').next().unwrap_or(&join.right_field);
    let limit = sel.limit.unwrap_or(cap).min(cap);
    let guard = engine.query_guard();

    // 阶段 1：主表 WHERE → 统一 DocIdSet（get_docid_set = eval 形态包装）。
    //   - 有 WHERE → Bitmap/Empty（倒排/AND 快路径/LIKE 收敛）；
    //   - 无 WHERE → All（全表——JOIN 须全量左表候选，不做 LIMIT 截断）。
    let left_set = get_docid_set(engine, sel.where_expr.as_ref(), None, &guard)?;
    if left_set.is_empty() {
        return Ok(Vec::new());
    }
    // DocIdSet → 有序 docid 列表（All 走 full_docids 全库语义，同原路径）
    let left_docids: Vec<u64> = match &left_set {
        crate::docset::DocIdSet::All => full_docids(engine, &guard)?.iter().collect(),
        other => other.to_vec(),
    };
    let left_docs = engine.batch_get(&left_docids)?;
    // review 修复（2026-09-04）：right_cache 值改为 Vec——从表 1:N（同一关联 key 多行）
    // 时逐右行展开产出多结果行；修复前只取 posting 首行 → INNER 缺行 / LEFT 只拼首行。
    let mut right_cache: std::collections::HashMap<String, Vec<Vec<u8>>> =
        std::collections::HashMap::new();
    let mut keys: Vec<Option<String>> = Vec::with_capacity(left_docs.len());
    for doc in &left_docs {
        if let Some(d) = doc {
            let key = extract_join_key(d, left_field);
            keys.push(key.clone());
            if let Some(k) = &key {
                if !right_cache.contains_key(k) {
                    right_cache.insert(k.clone(), Vec::new());
                }
            }
        } else {
            keys.push(None);
        }
    }
    // 从表关联查询（review：逐批 watchdog 熔断）：
    //   right_field = "docid" → 主键点查（1:1）；
    //   否则 → 倒排 term 全 posting 展开（1:N），batch_get 批量回表
    let unique_keys: Vec<String> = right_cache.keys().cloned().collect();
    for (ki, k) in unique_keys.iter().enumerate() {
        if ki % 64 == 0 && guard.is_expired() {
            return Err(Error::QueryTooExpensive(format!(
                "JOIN 从表关联查询超时（已查 {ki} 个关联 key，熔断中止）"
            )));
        }
        if right_field == "docid" || right_field == "id" {
            if let Ok(docid) = k.parse::<u64>() {
                if let Some(v) = engine.get(docid)? {
                    right_cache.insert(k.clone(), vec![v]);
                }
            }
        } else {
            let term = format!("{}={}", right_field, k);
            let posting = engine.inverted_posting(&term)?;
            if !posting.is_empty() {
                let docids: Vec<u64> = posting.iter().map(|d| d as u64).collect();
                let docs = engine.batch_get(&docids)?;
                right_cache.insert(
                    k.clone(),
                    docs.into_iter().flatten().collect(),
                );
            }
        }
    }
    // 合并（review：1:N 展开——每左行 × 每右行；watchdog 逐批熔断）
    let mut out = Vec::new();
    let mut done = 0u64;
    for (doc_opt, key) in left_docs.into_iter().zip(keys.into_iter()) {
        let Some(doc) = doc_opt else { continue };
        let rights: Vec<Vec<u8>> = match &key {
            Some(k) => right_cache.get(k).cloned().unwrap_or_default(),
            None => Vec::new(),
        };
        match join.join_type {
            JoinKind::Inner => {
                for rv in &rights {
                    let merged = merge_join_doc(&doc, rv, &sel.table, &join.right_table);
                    out.push((0, merged)); // docid 在 JOIN 结果中不直接有意义
                    done += 1;
                    if done % 4096 == 0 && guard.is_expired() {
                        return Err(Error::QueryTooExpensive(format!(
                            "JOIN 合并超时（已产出 {done} 行，熔断中止）"
                        )));
                    }
                    if out.len() as u64 >= limit {
                        return Ok(out);
                    }
                }
            }
            JoinKind::Left => {
                if rights.is_empty() {
                    out.push((0, doc));
                    if out.len() as u64 >= limit {
                        return Ok(out);
                    }
                } else {
                    for rv in &rights {
                        let merged = merge_join_doc(&doc, rv, &sel.table, &join.right_table);
                        out.push((0, merged));
                        if out.len() as u64 >= limit {
                            return Ok(out);
                        }
                    }
                }
            }
        }
    }
    Ok(out)
}

/// P0-D：从文档 JSON 提取 JOIN 关联 key。
fn extract_join_key(doc: &[u8], field: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(doc).ok()?;
    match v.get(field) {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
        Some(serde_json::Value::Bool(b)) => Some(b.to_string()),
        _ => None,
    }
}

/// P0-D：合并 JOIN 结果文档（左表字段 + 右表字段嵌套）。
fn merge_join_doc(left: &[u8], right: &[u8], left_table: &str, right_table: &str) -> Vec<u8> {
    let lv: serde_json::Value = serde_json::from_slice(left).unwrap_or(serde_json::Value::Null);
    let rv: serde_json::Value = serde_json::from_slice(right).unwrap_or(serde_json::Value::Null);
    serde_json::to_vec(&serde_json::json!({
        left_table: lv,
        right_table: rv,
    }))
    .unwrap_or_else(|_| left.to_vec())
}
