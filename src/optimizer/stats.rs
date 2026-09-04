//! 统计信息：字段级（`ColumnStatistics`）与表级（`TableStatistics`），
//! 由 ANALYZE TABLE 采集，供基于代价的动态路由估算各访问路径行数/代价。

use serde::{Deserialize, Serialize};

/// 字段级统计信息（由 ANALYZE TABLE 采集）。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnStatistics {
    /// 字段名。
    pub field: String,
    /// 估算基数（distinct values 数）。
    pub cardinality: u64,
    /// 最小值（JSON 序列化字节；None = 未知/非数值/非字符串）。
    pub min: Option<Vec<u8>>,
    /// 最大值（JSON 序列化字节；None = 未知/非数值/非字符串）。
    pub max: Option<Vec<u8>>,
    /// 非空值计数（NULL 值不计入）。
    pub non_null_count: u64,
    /// 可为空（true = 该字段有 NULL 值）。
    pub nullable: bool,
}

/// 表级统计信息。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableStatistics {
    /// 表名。
    pub table_name: String,
    /// 总行数（估算）。
    pub row_count: u64,
    /// 各字段统计。
    pub columns: Vec<ColumnStatistics>,
}
