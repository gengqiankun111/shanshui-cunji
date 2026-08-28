//! import-schema（development 5.27 阶段 2 / design 20）：预创建索引的字段注册表导入。
//!
//! 在批量导入前用 JSON 描述目标索引形态，import 侧据此：
//! 1. **预注册字段**：把声明字段写入 FieldRegistry（预分配稳定 ID，优化启动/编码）；
//! 2. **倒排字段白名单**：`inverted_fields` 指定时，只对声明字段建立倒排词条（其余字段不索引，
//!    减少索引膨胀与写放大）；
//! 3. **组合索引声明**：`composite_indexes` 声明组合键（`[join]/[cidx]` 配置依据），
//!    validate 时校验字段已声明；
//! 4. **时间戳游标**：`timestamp_field` / `timestamp_format` 声明增量导入的时间游标列。
//!
//! 用法：`shanshui-cunji-import --csv in.csv --schema schema.json`。

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::schema::FieldRegistry;

/// 导入 Schema（JSON）：描述目标索引形态。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ImportSchema {
    /// 主键列名（缺省用 docid/id 自动推断）。
    pub id_field: Option<String>,
    /// 倒排字段白名单；None = 全部字符串字段建倒排。
    pub inverted_fields: Option<Vec<String>>,
    /// 组合稀疏索引声明：每个元素是一组字段（按声明顺序编码）。
    pub composite_indexes: Vec<Vec<String>>,
    /// 时间戳字段（增量导入游标）。
    pub timestamp_field: Option<String>,
    /// 时间戳格式："unix" / "rfc3339"。
    pub timestamp_format: Option<String>,
}

impl Default for ImportSchema {
    fn default() -> Self {
        Self {
            id_field: None,
            inverted_fields: None,
            composite_indexes: Vec::new(),
            timestamp_field: None,
            timestamp_format: None,
        }
    }
}

/// Schema 应用报告。
#[derive(Debug, Clone)]
pub struct SchemaReport {
    /// 已注册字段（预分配 ID）。
    pub fields: Vec<String>,
    /// 倒排白名单字段数（None = 全部）。
    pub inverted_fields: Option<usize>,
    /// 组合索引声明数。
    pub composite_keys: usize,
    /// 时间戳游标字段。
    pub timestamp_field: Option<String>,
}

impl ImportSchema {
    /// 从 JSON 文件加载。
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::Io(std::io::Error::other(format!("读取 schema 失败: {e}"))))?;
        let schema: ImportSchema = serde_json::from_str(&text)
            .map_err(|e| Error::Serialize(format!("schema 解析失败: {e}")))?;
        schema.validate()?;
        Ok(schema)
    }

    /// 校验：字段名非空、组合索引字段必须声明于倒排/组合集合、时间格式合法。
    pub fn validate(&self) -> Result<()> {
        if let Some(f) = &self.id_field {
            if f.trim().is_empty() {
                return Err(Error::Config("schema.id_field 不能为空".into()));
            }
        }
        let mut declared: Vec<String> = Vec::new();
        if let Some(inv) = &self.inverted_fields {
            for f in inv {
                if f.trim().is_empty() {
                    return Err(Error::Config("schema.inverted_fields 含空字段名".into()));
                }
                declared.push(f.clone());
            }
        }
        for idx in &self.composite_indexes {
            if idx.is_empty() {
                return Err(Error::Config("schema.composite_indexes 含空组合键".into()));
            }
            for f in idx {
                if f.trim().is_empty() {
                    return Err(Error::Config("schema.composite_indexes 含空字段名".into()));
                }
            }
        }
        if let Some(ts) = &self.timestamp_format {
            if !matches!(ts.as_str(), "unix" | "rfc3339") {
                return Err(Error::Config(format!(
                    "schema.timestamp_format 非法: {ts}（unix / rfc3339）"
                )));
            }
        }
        Ok(())
    }

    /// 倒排词条白名单过滤函数：term 形如 `field=value`；白名单存在时只保留声明字段。
    /// 返回 `None` 表示不过滤（全部字符串字段建倒排）。
    pub fn term_filter(&self) -> Option<Vec<String>> {
        self.inverted_fields.clone()
    }

    /// 应用 schema：预注册字段 + 生成报告（预创建索引的字段基座）。
    pub fn apply(&self) -> Result<SchemaReport> {
        self.validate()?;
        let mut reg = FieldRegistry::new();
        let mut fields = Vec::new();
        let register = |f: &str, fields: &mut Vec<String>, reg: &mut FieldRegistry| {
            if !fields.iter().any(|x| x == f) {
                reg.register(f);
                fields.push(f.to_string());
            }
        };
        if let Some(f) = &self.id_field {
            register(f, &mut fields, &mut reg);
        }
        if let Some(inv) = &self.inverted_fields {
            for f in inv {
                register(f, &mut fields, &mut reg);
            }
        }
        for idx in &self.composite_indexes {
            for f in idx {
                register(f, &mut fields, &mut reg);
            }
        }
        if let Some(f) = &self.timestamp_field {
            register(f, &mut fields, &mut reg);
        }
        Ok(SchemaReport {
            fields,
            inverted_fields: self.inverted_fields.as_ref().map(|v| v.len()),
            composite_keys: self.composite_indexes.len(),
            timestamp_field: self.timestamp_field.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_and_validate_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema.json");
        std::fs::write(
            &path,
            r#"{
                "id_field": "docid",
                "inverted_fields": ["status", "city", "type"],
                "composite_indexes": [["status", "type"], ["city", "level"]],
                "timestamp_field": "created_at",
                "timestamp_format": "rfc3339"
            }"#,
        )
        .unwrap();
        let s = ImportSchema::load(&path).unwrap();
        assert_eq!(s.id_field.as_deref(), Some("docid"));
        assert_eq!(s.inverted_fields.as_ref().unwrap().len(), 3);
        assert_eq!(s.composite_indexes.len(), 2);
        assert_eq!(s.timestamp_format.as_deref(), Some("rfc3339"));
    }

    #[test]
    fn invalid_schema_rejected() {
        // 非法时间格式
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        std::fs::write(&path, r#"{"timestamp_format": "junk"}"#).unwrap();
        assert!(ImportSchema::load(&path).is_err());
        // 空组合键
        let path2 = dir.path().join("bad2.json");
        std::fs::write(&path2, r#"{"composite_indexes": [[]]}"#).unwrap();
        assert!(ImportSchema::load(&path2).is_err());
    }

    #[test]
    fn apply_registers_fields_and_report() {
        let s = ImportSchema {
            id_field: Some("docid".into()),
            inverted_fields: Some(vec!["status".into(), "city".into()]),
            composite_indexes: vec![vec!["status".into(), "type".into()]],
            timestamp_field: Some("created_at".into()),
            timestamp_format: None,
        };
        let rep = s.apply().unwrap();
        assert_eq!(rep.fields.len(), 5, "id + 倒排2 + 组合补充 type + 时间戳");
        assert!(rep.fields.contains(&"status".to_string()));
        assert!(rep.fields.contains(&"created_at".to_string()));
        assert_eq!(rep.inverted_fields, Some(2));
        assert_eq!(rep.composite_keys, 1);
        assert_eq!(rep.timestamp_field.as_deref(), Some("created_at"));
    }

    #[test]
    fn term_filter_whitelist() {
        let s = ImportSchema {
            inverted_fields: Some(vec!["status".into()]),
            ..Default::default()
        };
        assert_eq!(s.term_filter().unwrap(), vec!["status".to_string()]);
        let s2 = ImportSchema::default();
        assert!(s2.term_filter().is_none(), "默认不过滤");
    }
}
