//! 库内 schema（`cj.schema.json`）：按表声明的显式索引（倒排白名单 / 位图 / fulltext /
//! stats / 组合索引）。schema 与数据目录绑定：服务打开库时自动读取，随目录整体迁移。
//!
//! 原则（无内置默认）：**一切索引皆声明**——
//! - `inverted_fields` 空 = 该表**不建任何倒排词条**（不再有"空 = 全部字符串字段建倒排"的
//!   隐式默认；未声明字段的等值/范围查询只能走主键/组合索引/全表扫描）；
//! - `composite_indexes` 空 = 无组合索引；
//! - 未命中任何表声明 → 零索引装配（由 `cjserver --config` 提供的运行参数仍生效，
//!   索引声明一律以库内 schema 为准）。

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// 库内 schema 文件名（存放于数据目录根）。
pub const SCHEMA_FILE: &str = "cj.schema.json";

/// 单表索引声明。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// 表名（docid 高位表命名空间对应的逻辑表名）。
    pub name: String,
    /// 主键列名（记录用途；docid 分配仍由引擎/docid_alloc 负责）。
    #[serde(default)]
    pub id_field: Option<String>,
    /// 倒排字段白名单：显式声明才建词条；空 = 无倒排。
    #[serde(default)]
    pub inverted_fields: Vec<String>,
    /// 位图索引字段白名单（低基数枚举常驻内存位图：COUNT/GROUP BY/AND 快速路径）。
    #[serde(default)]
    pub bitmap_fields: Vec<String>,
    /// fulltext 分词字段（`ft:{field}:{token}` 词 term，与倒排白名单正交）。
    #[serde(default)]
    pub fulltext_fields: Vec<String>,
    /// 倒排统计载荷字段（随 term 累积 sum/min/max/avg，支撑聚合免全扫）。
    #[serde(default)]
    pub stats_fields: Vec<String>,
    /// 组合索引声明：每个元素是一组字段（按声明顺序编码，最左前缀命中）。
    #[serde(default)]
    pub composite_indexes: Vec<Vec<String>>,
}

/// 库 schema 容器（多表预留：tables 数组；引擎侧当前以单逻辑表装配，
/// 后续多表引擎直接按 name 逐表取用）。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DbSchema {
    #[serde(default)]
    pub tables: Vec<TableSchema>,
}

impl DbSchema {
    /// 从数据目录加载库内 schema；文件不存在 → Ok(None)（零索引，合法状态）。
    pub fn load_dir(data_dir: &Path) -> Result<Option<Self>> {
        let path = data_dir.join(SCHEMA_FILE);
        if !path.exists() {
            return Ok(None);
        }
        let text = std::fs::read_to_string(&path)
            .map_err(|e| Error::Io(std::io::Error::other(format!("读取 {SCHEMA_FILE} 失败: {e}"))))?;
        let s: DbSchema = serde_json::from_str(&text)
            .map_err(|e| Error::Serialize(format!("解析 {SCHEMA_FILE} 失败: {e}")))?;
        s.validate()?;
        Ok(Some(s))
    }

    /// 写入数据目录（失败传播；父目录须已存在）。
    pub fn save_dir(&self, data_dir: &Path) -> Result<()> {
        let path = data_dir.join(SCHEMA_FILE);
        let text = serde_json::to_string_pretty(self)
            .map_err(|e| Error::Serialize(format!("序列化 {SCHEMA_FILE} 失败: {e}")))?;
        std::fs::write(&path, text)
            .map_err(|e| Error::Io(std::io::Error::other(format!("写入 {SCHEMA_FILE} 失败: {e}"))))?;
        Ok(())
    }

    /// 按表名取声明。
    pub fn table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.iter().find(|t| t.name == name)
    }

    /// 校验：表名/字段名非空、组合索引字段非空。空库 schema（无 tables）合法（零索引）。
    pub fn validate(&self) -> Result<()> {
        for t in &self.tables {
            if t.name.trim().is_empty() {
                return Err(Error::Config("cj.schema.json: 表名不能为空".into()));
            }
            for f in t.inverted_fields.iter().chain(t.bitmap_fields.iter())
                .chain(t.fulltext_fields.iter())
                .chain(t.stats_fields.iter())
            {
                if f.trim().is_empty() {
                    return Err(Error::Config(format!(
                        "cj.schema.json: 表 {} 含空字段名",
                        t.name
                    )));
                }
            }
            for idx in &t.composite_indexes {
                if idx.is_empty() {
                    return Err(Error::Config(format!(
                        "cj.schema.json: 表 {} 含空组合键",
                        t.name
                    )));
                }
                for f in idx {
                    if f.trim().is_empty() {
                        return Err(Error::Config(format!(
                            "cj.schema.json: 表 {} 组合索引含空字段名",
                            t.name
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

impl TableSchema {
    /// 将该表声明装配进引擎配置（覆盖 cfg 中的索引声明项；其余运行参数不动）。
    /// schema 是索引声明的**权威来源**——即使 `--config` 也给了索引项，以库内为准。
    pub fn apply_to_cfg(&self, cfg: &mut crate::config::Config) {
        // 声明制恒开（P131b）：空 inverted_fields = 零倒排，绝不回退 legacy 全字段
        cfg.inverted.declared_only = true;
        cfg.inverted.inverted_fields = self.inverted_fields.clone();
        cfg.inverted.bitmap_fields = self.bitmap_fields.clone();
        cfg.inverted.fulltext_fields = self.fulltext_fields.clone();
        cfg.inverted.stats_fields = self.stats_fields.clone();
        cfg.storage.composite_indexes = self.composite_indexes.clone();
    }

    /// 导入工具 `--schema` 落库：由 ImportSchema + 当前 cfg 派生单表声明（库自描述）。
    /// 语义与无内置默认一致——`ImportSchema.inverted_fields` 未声明（None）→ 空（无倒排），
    /// 不再有"None = 全部字符串字段建倒排"的旧默认。
    pub fn from_import(
        name: &str,
        is: &crate::import_schema::ImportSchema,
        cfg: &crate::config::Config,
    ) -> Self {
        TableSchema {
            name: name.to_string(),
            id_field: is.id_field.clone(),
            inverted_fields: is.inverted_fields.clone().unwrap_or_default(),
            bitmap_fields: cfg.inverted.bitmap_fields.clone(),
            fulltext_fields: cfg.inverted.fulltext_fields.clone(),
            stats_fields: cfg.inverted.stats_fields.clone(),
            composite_indexes: is.composite_indexes.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn roundtrip_save_load() {
        let dir = tempdir().unwrap();
        let s = DbSchema {
            tables: vec![TableSchema {
                name: "documents".into(),
                id_field: Some("id".into()),
                inverted_fields: vec!["status".into(), "city".into()],
                bitmap_fields: vec!["status".into()],
                fulltext_fields: vec![],
                stats_fields: vec!["amount".into()],
                composite_indexes: vec![vec!["status".into(), "ts".into()], vec!["ts".into()]],
            }],
        };
        s.save_dir(dir.path()).unwrap();
        let loaded = DbSchema::load_dir(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.tables.len(), 1);
        let t = loaded.table("documents").unwrap();
        assert_eq!(t.inverted_fields, vec!["status", "city"]);
        assert_eq!(t.composite_indexes, vec![vec!["status", "ts"], vec!["ts"]]);
        assert_eq!(t.stats_fields, vec!["amount"]);
        // 缺文件 → Ok(None)（零索引合法态）
        let dir2 = tempdir().unwrap();
        assert!(DbSchema::load_dir(dir2.path()).unwrap().is_none());
    }

    #[test]
    fn invalid_schema_rejected() {
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path().join(SCHEMA_FILE),
            r#"{ "tables": [ { "name": "", "inverted_fields": ["x"] } ] }"#,
        )
        .unwrap();
        assert!(DbSchema::load_dir(dir.path()).is_err());
        let dir3 = tempdir().unwrap();
        std::fs::write(
            dir3.path().join(SCHEMA_FILE),
            r#"{ "tables": [ { "name": "t", "composite_indexes": [ [] ] } ] }"#,
        )
        .unwrap();
        assert!(DbSchema::load_dir(dir3.path()).is_err());
    }
}
