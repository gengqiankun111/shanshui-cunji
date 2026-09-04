//! 从表关联查询路由（fetch_related）与关联 key 提取（field_to_key）。
//!
//! - `fetch_related`：按关联 key 从从表取文档 —— `docid`/`id` 走主键点查，
//!   其他字段走倒排 term `field=key` 查询并取首个文档；
//! - `field_to_key`：把文档中字段值（字符串 / 数字 / 布尔）转为字符串关联 key。

use serde_json::Value;

use crate::engine::Engine;
use crate::error::{Error, Result};

/// 从表按关联 key 查询：`docid` 主键点查；否则倒排 `field=key` 取首个文档。
pub(super) fn fetch_related(engine: &mut Engine, to_field: &str, key: &str) -> Result<Option<Value>> {
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
pub(super) fn field_to_key(val: &Value, field: &str) -> Option<String> {
    match val.get(field) {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Number(n)) => Some(n.to_string()),
        Some(Value::Bool(b)) => Some(b.to_string()),
        _ => None,
    }
}
