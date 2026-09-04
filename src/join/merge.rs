//! queryAndJoin 合并主流程（design 19）：主表倒排筛选 → 回表 → 从表批量点查
//! （或小表广播索引）→ 内存 Hash 合并，以及广播决策 `should_broadcast`。

use serde_json::Value;

use crate::engine::Engine;
use crate::error::{Error, Result};
use super::route::{fetch_related, field_to_key};
use super::{JoinBroadcast, JoinRow, JoinSpec, JoinType};

/// 是否走小表广播 JOIN：未启用或无广播选项则回退逐 key 点查。
pub(super) fn should_broadcast(distinct_keys: usize, opt: Option<JoinBroadcast>) -> bool {
    match opt {
        Some(b) => b.enabled && distinct_keys <= b.threshold,
        None => false,
    }
}

/// queryAndJoin：主表倒排筛选 → 回表 → 从表批量点查（或小表广播索引）→ 内存 Hash 合并。
/// 结果集超过 `max_rows` 熔断（design 5.20 / design 19）。
pub fn query_and_join(
    engine: &mut Engine,
    spec: &JoinSpec,
    max_rows: usize,
    broadcast: Option<JoinBroadcast>,
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
    // ③ 从表批量取关联（去重 key，避免重复 IO）
    let mut right_cache: std::collections::HashMap<String, Option<Value>> =
        std::collections::HashMap::new();
    let unique_keys: std::collections::HashSet<String> =
        lefts.iter().filter_map(|(k, _)| k.clone()).collect();
    if should_broadcast(unique_keys.len(), broadcast) {
        // ③-a 小表广播 JOIN（design 19.3）：一次全量扫描从表建内存索引复用。
        //     docid/id 关联 → 主键即 key；其他字段 → 提取字段值；缺字段文档跳过。
        //     首个命中优先（与倒排 term 查询"取首个文档"语义一致）。
        let mut idx: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
        for (docid, bytes) in engine.scan_range(None, None)? {
            let val: Value = serde_json::from_slice(&bytes)
                .map_err(|e| Error::Serialize(format!("从表广播扫描解析失败: {e}")))?;
            let key = if spec.to_field == "docid" || spec.to_field == "id" {
                docid.to_string()
            } else {
                match field_to_key(&val, spec.to_field) {
                    Some(k) => k,
                    None => continue,
                }
            };
            idx.entry(key).or_insert(val);
        }
        for key in unique_keys {
            right_cache.insert(key.clone(), idx.get(&key).cloned());
        }
    } else {
        // ③-b 逐 key 点查（默认路径）
        for key in unique_keys {
            right_cache.insert(key.clone(), fetch_related(engine, spec.to_field, &key)?);
        }
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
