//! 查询优化器配置（P4-C 代价模型参数）。

use serde::{Deserialize, Serialize};

/// P4-C：优化器配置（代价模型参数）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OptimizerConfig {
    /// 启用基于代价的动态路由（true = P4-C 动态路由；false = 保留静态路由兼容）。
    pub cost_based_enabled: bool,
    /// 一次主键点查的代价（微秒）。
    pub point_lookup_cost: f64,
    /// 每行全表扫描的代价（微秒/行）。
    pub full_scan_row_cost: f64,
    /// 倒排每行回表的代价（微秒/行）。
    pub inverted_fetch_cost: f64,
    /// 倒排 bitmap 合并的固定开销（微秒）。
    pub inverted_merge_fixed: f64,
    /// 组合索引每行回表的代价（微秒/行）。
    pub composite_fetch_cost: f64,
    /// Zone Map 剪枝效率系数（0~1）：1 = 完全剪枝，0 = 无剪枝。
    pub zone_map_effectiveness: f64,
    /// 倒排查询阈值：doc_count 超过此值时考虑全扫替代（0 = 使用代价估算）。
    pub inverted_fallback_threshold: u64,
    /// 支持 Zone Map 剪枝的字段列表（空 = 所有字段均支持）。
    pub zone_map_fields: Vec<String>,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            cost_based_enabled: true,
            point_lookup_cost: 1.0,
            full_scan_row_cost: 0.1,
            inverted_fetch_cost: 2.0,
            inverted_merge_fixed: 50.0,
            composite_fetch_cost: 1.5,
            zone_map_effectiveness: 0.85,
            inverted_fallback_threshold: 100_000,
            zone_map_fields: Vec::new(),
        }
    }
}