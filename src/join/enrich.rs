//! 写入 Enrich（development 5.21）：WAL 写入前执行回调展开关联数据到单文档，
//! 失败策略 reject（拒绝写入）/ degrade（降级写入原文档）。

use serde_json::{json, Value};

use crate::engine::Engine;
use crate::error::{Error, Result};
use super::route::{fetch_related, field_to_key};

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
