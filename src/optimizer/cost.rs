//! 代价模型：可调参数（`CostParams`）与某访问路径的代价估算结果（`CostEstimate`）。

use serde::{Deserialize, Serialize};

use super::AccessPath;

/// 代价模型参数（可配置调优）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostParams {
    /// 一次主键点查的代价（单位：微秒）。
    pub point_lookup_cost: f64,
    /// 每行全表扫描的代价（单位：微秒/行）。
    pub full_scan_row_cost: f64,
    /// 倒排每行回表的代价（单位：微秒/行）。
    pub inverted_fetch_cost: f64,
    /// 倒排 bitmap 合并的固定开销（单位：微秒）。
    pub inverted_merge_fixed: f64,
    /// 组合索引每行回表的代价（单位：微秒/行）。
    pub composite_fetch_cost: f64,
    /// Zone Map 剪枝效率系数（0~1）：1 = 完全剪枝，0 = 无剪枝。
    pub zone_map_effectiveness: f64,
    /// 范围查询扫描行到回表行的转换系数（SST 扫描行数 ≤ 估计行数 × 系数）。
    pub scan_row_factor: f64,
    /// 倒排查询阈值：doc_count 超过此值时考虑全扫替代（0 = 使用代价估算）。
    pub inverted_fallback_threshold: u64,
}

impl Default for CostParams {
    fn default() -> Self {
        Self {
            point_lookup_cost: 1.0,          // 点查单行 ~1µs
            full_scan_row_cost: 0.1,         // 全扫单行 ~0.1µs（流式，免回表）
            inverted_fetch_cost: 2.0,        // 回表单行 ~2µs（批量 get + 反序列化）
            inverted_merge_fixed: 50.0,      // 合并固定 ~50µs
            composite_fetch_cost: 1.5,       // 组合索引回表 ~1.5µs
            zone_map_effectiveness: 0.85,    // 默认 Zone Map 剪枝 85% 块（偏乐观）
            scan_row_factor: 1.5,            // 扫描行数 = 估计行 × 1.5（SST 块内无效行代价）
            inverted_fallback_threshold: 100_000, // 10 万行以上考虑替代路径
        }
    }
}

/// 某访问路径的代价估算结果。
#[derive(Debug, Clone, PartialEq)]
pub struct CostEstimate {
    /// 总代价（微秒）。
    pub total_us: f64,
    /// 估算命中行数。
    pub estimated_rows: u64,
    /// 访问路径。
    pub path: AccessPath,
}

impl CostEstimate {
    pub fn new(path: AccessPath, estimated_rows: u64, total_us: f64) -> Self {
        Self { path, estimated_rows, total_us }
    }
}
