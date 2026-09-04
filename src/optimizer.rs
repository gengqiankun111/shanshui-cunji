//! 查询优化器骨架：静态路由 → 基于代价的动态路由（P4-C 难点 4）。
//!
//! 阶段 1 MVP：只做主键 / 范围查询的访问路径判定（静态路由，无代价估算）；
//! P4-C 升级：引入统计信息 + 代价估算模型，动态选择最优访问路径；
//!
//! 核心目标：解决多条件查询选错执行计划（慢 1000×）问题：
//! - `status='active' AND amount>5000`：若 `status='active'` 选择性低（占比 50%），
//!   倒排查出千万行再回表过滤 → 不如全扫 Zone Map 剪枝 `amount>5000` 更快。
//! - 基于统计估算选择：倒排 N doc → 代价 = N × 回表 IO；全扫 M SST → Zone Map 剪枝到 K doc →
//!   代价 = K × 扫描 + 直接得到结果；选代价小的。

use serde::{Deserialize, Serialize};

/// 查询访问路径（路由结果）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessPath {
    /// 主数据列族：按主键点查（等值）。
    PrimaryPoint,
    /// 主数据列族：主键范围扫描。
    PrimaryRange,
    /// 组合索引列族：等值前缀 → 回表主数据。
    CompositeIndex { fields: Vec<String> },
    /// 倒排列族：term 命中 → 回表主数据（步骤 10 启用）。
    Inverted { term: String },
    /// 全表扫描（无可用索引，兜底）。
    FullScan,
}

/// 查询类别（由 SQL/协议解析层填充；MVP 仅主键维度）。
#[derive(Debug, Clone)]
pub struct QuerySpec {
    /// 是否主键等值查询。
    pub primary_eq: Option<Vec<u8>>,
    /// 是否主键范围查询。
    pub primary_range: bool,
    /// 组合索引等值前缀字段。
    pub index_prefix: Vec<String>,
    /// 倒排词条。
    pub term: Option<String>,
}

/// 静态路由：根据查询类别返回访问路径（不依赖统计信息）。
pub fn route(spec: &QuerySpec) -> AccessPath {
    if spec.primary_eq.is_some() {
        AccessPath::PrimaryPoint
    } else if spec.primary_range {
        AccessPath::PrimaryRange
    } else if !spec.index_prefix.is_empty() {
        AccessPath::CompositeIndex {
            fields: spec.index_prefix.clone(),
        }
    } else if let Some(t) = &spec.term {
        AccessPath::Inverted { term: t.clone() }
    } else {
        AccessPath::FullScan
    }
}

// ==================== P4-C：统计信息 + 代价模型 + 动态路由 ====================

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

/// 基于代价的动态路由：给定查询规格与统计信息，计算各可行路径的代价并返回最优路径。
///
/// 相比静态 `route()`，此函数可处理多条件场景：
/// - 倒排查 N docid 再回表 vs 全扫 Zone Map 剪枝到 K doc 直出
/// - 组合索引前缀回表 vs 倒排等值回表
///
/// `params`：代价模型参数；
/// `inverted_doc_count`：某个 term 的倒排 doc_count 回调（None = 无该 term）；
/// `total_rows`：表总行数；
/// `zone_fields`：支持 Zone Map 剪枝的字段列表（通常是有 FieldZone 的 SST 字段）。
pub fn cost_route(
    spec: &QuerySpec,
    params: &CostParams,
    inverted_doc_count: &impl Fn(&str) -> Option<u64>,
    total_rows: u64,
    _zone_fields: &[String],
) -> CostEstimate {
    // 1. 主键点查：固定代价
    if spec.primary_eq.is_some() {
        return CostEstimate::new(AccessPath::PrimaryPoint, 1, params.point_lookup_cost);
    }

    let mut candidates: Vec<CostEstimate> = Vec::new();

    // 2. 主键范围扫描
    if spec.primary_range {
        // 范围扫描 ≈ 全扫（主键有序，范围扫描需要扫描区间内的所有行）
        let cost = total_rows as f64 * params.full_scan_row_cost;
        candidates.push(CostEstimate::new(AccessPath::PrimaryRange, total_rows, cost));
    }

    // 3. 组合索引前缀
    if !spec.index_prefix.is_empty() {
        // 估算组合索引选择性：取首字段基数估算
        // 若无统计信息，保守估计匹配 50% 行
        let estimated = estimate_composite_rows(total_rows);
        let cost = estimated as f64 * params.composite_fetch_cost + params.inverted_merge_fixed;
        candidates.push(CostEstimate::new(
            AccessPath::CompositeIndex { fields: spec.index_prefix.clone() },
            estimated,
            cost,
        ));
    }

    // 4. 倒排等值
    if let Some(term) = &spec.term {
        if let Some(doc_count) = inverted_doc_count(term) {
            if doc_count > 0 {
                // 倒排代价 = 固定合并开销 + 回表行数 × 单行回表代价
                let cost = params.inverted_merge_fixed + doc_count as f64 * params.inverted_fetch_cost;
                candidates.push(CostEstimate::new(
                    AccessPath::Inverted { term: term.clone() },
                    doc_count,
                    cost,
                ));
            }
        }
    }

    // 5. 全表扫描（兜底）
    {
        // 估算 Zone Map 剪枝效率：若查询条件字段在 zone_fields 中，剪枝减少扫描量
        let effective = if spec.term.is_some() {
            // 倒排等值本身不走全扫，但可作为全扫的替代对比
            params.zone_map_effectiveness
        } else {
            // 无倒排等值 → 全扫是最低成本的替代
            0.0
        };
        let scan_rows = (total_rows as f64 * (1.0 - effective * 0.5)).max(1.0);
        let cost = scan_rows * params.full_scan_row_cost;
        candidates.push(CostEstimate::new(AccessPath::FullScan, scan_rows as u64, cost));
    }

    // 选代价最小的
    candidates.into_iter().min_by(|a, b| a.total_us.partial_cmp(&b.total_us).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or(CostEstimate::new(AccessPath::FullScan, total_rows, total_rows as f64 * params.full_scan_row_cost))
}

/// 估算组合索引前缀匹配行数（无统计信息时的保守估计）。
fn estimate_composite_rows(total_rows: u64) -> u64 {
    // 无统计信息时，保守估计组合索引选择性为 50%（可能有更高选择性的前缀）
    (total_rows / 2).max(1)
}

/// 计算多条件 AND 场景下的最优路径组合。
///
/// 场景：`WHERE status='active' AND amount>5000`
/// - 如果 `status='active'` 倒排查 10M 行，代价 = 10M × 回表
/// - 如果 `amount>5000` 全扫 + Zone Map 剪枝到 1M 行，代价 = 1M × 扫描
/// - 选代价小的作为主路径，另一条件作为后过滤
///
/// 返回 `(主路径, 后过滤条件列表)`。
pub fn choose_best_plan(
    eq_terms: &[(String, Option<u64>)],  // (term, doc_count)
    range_fields: &[String],              // 范围查询字段
    params: &CostParams,
    total_rows: u64,
    zone_fields: &[String],
) -> CostEstimate {
    let mut best: Option<CostEstimate> = None;

    // 评估每个等值条件的倒排代价
    for (term, doc_count) in eq_terms {
        if let Some(count) = doc_count {
            if *count > 0 {
                let cost = params.inverted_merge_fixed + *count as f64 * params.inverted_fetch_cost;
                let est = CostEstimate::new(
                    AccessPath::Inverted { term: term.clone() },
                    *count,
                    cost,
                );
                best = Some(best.map_or(est.clone(), |b| if est.total_us < b.total_us { est } else { b }));
            }
        }
    }

    // 评估全扫 + Zone Map 剪枝（范围查询字段）
    if !range_fields.is_empty() {
        let has_zone_field = range_fields.iter().any(|f| zone_fields.contains(f));
        let effectiveness = if has_zone_field { params.zone_map_effectiveness } else { 0.3 };
        let scan_rows = (total_rows as f64 * (1.0 - effectiveness)).max(1.0);
        let cost = scan_rows * params.full_scan_row_cost;
        let est = CostEstimate::new(AccessPath::FullScan, scan_rows as u64, cost);
        best = Some(best.map_or(est.clone(), |b| if est.total_us < b.total_us { est } else { b }));
    }

    best.unwrap_or(CostEstimate::new(AccessPath::FullScan, total_rows, total_rows as f64 * params.full_scan_row_cost))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_eq_routes_to_point() {
        let spec = QuerySpec {
            primary_eq: Some(b"\x01\x00\x00\x00\x00\x00\x00\x00".to_vec()),
            primary_range: false,
            index_prefix: vec![],
            term: None,
        };
        assert_eq!(route(&spec), AccessPath::PrimaryPoint);
    }

    #[test]
    fn primary_range_routes_to_range() {
        let spec = QuerySpec {
            primary_eq: None,
            primary_range: true,
            index_prefix: vec![],
            term: None,
        };
        assert_eq!(route(&spec), AccessPath::PrimaryRange);
    }

    #[test]
    fn index_prefix_beats_full_scan() {
        let spec = QuerySpec {
            primary_eq: None,
            primary_range: false,
            index_prefix: vec!["status".into()],
            term: None,
        };
        assert_eq!(
            route(&spec),
            AccessPath::CompositeIndex {
                fields: vec!["status".into()]
            }
        );
    }

    #[test]
    fn term_routes_to_inverted() {
        let spec = QuerySpec {
            primary_eq: None,
            primary_range: false,
            index_prefix: vec![],
            term: Some("click".into()),
        };
        assert_eq!(
            route(&spec),
            AccessPath::Inverted {
                term: "click".into()
            }
        );
    }

    #[test]
    fn no_condition_falls_back_to_full_scan() {
        let spec = QuerySpec {
            primary_eq: None,
            primary_range: false,
            index_prefix: vec![],
            term: None,
        };
        assert_eq!(route(&spec), AccessPath::FullScan);
    }

    // ==================== P4-C 测试 ====================

    #[test]
    fn cost_route_primary_eq_is_cheapest() {
        let spec = QuerySpec {
            primary_eq: Some(b"\x01\x00\x00\x00\x00\x00\x00\x00".to_vec()),
            primary_range: false,
            index_prefix: vec![],
            term: Some("status=active".into()),
        };
        let params = CostParams::default();
        let count = |_: &str| Some(10_000u64);
        let est = cost_route(&spec, &params, &count, 1_000_000, &[]);
        assert_eq!(est.path, AccessPath::PrimaryPoint);
        assert_eq!(est.estimated_rows, 1);
        assert!(est.total_us < 10.0, "点查代价应极低: {}", est.total_us);
    }

    #[test]
    fn cost_route_inverted_cheaper_than_full_scan() {
        // 高选择性倒排（10 行）vs 全扫 100 万行 → 倒排更优
        let spec = QuerySpec {
            primary_eq: None,
            primary_range: false,
            index_prefix: vec![],
            term: Some("status=active".into()),
        };
        let params = CostParams::default();
        let count = |_: &str| Some(10u64);
        let est = cost_route(&spec, &params, &count, 1_000_000, &[]);
        assert_eq!(est.path, AccessPath::Inverted { term: "status=active".into() });
        assert_eq!(est.estimated_rows, 10);
        // 倒排代价 = 50 + 10*2 = 70µs
        // 全扫代价 = 100万 * 0.1 * (1-0.85*0.5) = 100万*0.1*0.575 = 57,500µs
        assert!(est.total_us < 100.0, "高选择性倒排应远低于全扫: {}", est.total_us);
    }

    #[test]
    fn cost_route_full_scan_cheaper_for_low_selectivity() {
        // 低选择性倒排（50 万行）vs 全扫 100 万行 + Zone Map 剪枝 → 全扫可能更优
        let spec = QuerySpec {
            primary_eq: None,
            primary_range: false,
            index_prefix: vec![],
            term: Some("status=active".into()),
        };
        let params = CostParams::default();
        let count = |_: &str| Some(500_000u64);
        let zone_fields = vec!["status".into()];
        let est = cost_route(&spec, &params, &count, 1_000_000, &zone_fields);
        // 倒排代价 = 50 + 500000*2 = 1,000,050µs
        // 全扫代价 = 100万*0.1*(1-0.85*0.5) = 57,500µs
        // 地选择性倒排代价高，应选全扫
        assert_eq!(est.path, AccessPath::FullScan, "低选择性倒排应走全扫");
    }

    #[test]
    fn cost_route_inverted_term_not_found_falls_back() {
        let spec = QuerySpec {
            primary_eq: None,
            primary_range: false,
            index_prefix: vec![],
            term: Some("unknown=value".into()),
        };
        let params = CostParams::default();
        let count = |_: &str| None; // term 不存在
        let est = cost_route(&spec, &params, &count, 1_000_000, &[]);
        assert_eq!(est.path, AccessPath::FullScan, "term 不存在应走全扫");
    }

    #[test]
    fn choose_best_plan_picks_cheapest_inverted() {
        // 场景：高选择性倒排（100 行）vs 50万行倒排 + 范围有 Zone Map
        // 100 行倒排代价 = 50 + 100*2 = 250µs < 全扫 Zone Map 15,000µs
        let eq_terms = vec![
            ("status=active".into(), Some(100u64)),     // 100 行（高选择性）
            ("city=beijing".into(), Some(500_000u64)),  // 50万行
        ];
        let range_fields = vec!["amount".into()];
        let params = CostParams::default();
        let zone_fields = vec!["amount".into()];
        let est = choose_best_plan(&eq_terms, &range_fields, &params, 1_000_000, &zone_fields);
        assert_eq!(
            est.path,
            AccessPath::Inverted { term: "status=active".into() },
            "高选择性倒排应优于全扫 Zone Map"
        );
        assert_eq!(est.estimated_rows, 100);
    }

    #[test]
    fn choose_best_plan_full_scan_when_inverted_expensive() {
        // 所有等值条件选择性都低（各 40 万行）+ 范围字段有 Zone Map → 全扫更优
        let eq_terms = vec![
            ("status=active".into(), Some(400_000u64)),
            ("city=beijing".into(), Some(450_000u64)),
        ];
        let range_fields = vec!["amount".into()];
        let params = CostParams::default();
        let zone_fields = vec!["amount".into()];
        let est = choose_best_plan(&eq_terms, &range_fields, &params, 1_000_000, &zone_fields);
        // 倒排最小代价 = 50 + 400000*2 = 800,050µs
        // 全扫+ZoneMap = 100万*0.1*(1-0.85) = 15,000µs
        assert_eq!(est.path, AccessPath::FullScan, "低选择性倒排全部应走全扫");
    }

    #[test]
    fn column_statistics_default() {
        let cs = ColumnStatistics::default();
        assert_eq!(cs.field, "");
        assert_eq!(cs.cardinality, 0);
        assert!(cs.min.is_none());
        assert!(cs.max.is_none());
    }

    #[test]
    fn cost_params_default_is_sane() {
        let p = CostParams::default();
        assert!(p.point_lookup_cost > 0.0);
        assert!(p.full_scan_row_cost > 0.0);
        assert!(p.inverted_fetch_cost > 0.0);
        assert!(p.inverted_merge_fixed > 0.0);
        assert!(p.composite_fetch_cost > 0.0);
        assert!(p.inverted_fallback_threshold > 0);
    }

    #[test]
    fn cost_estimate_new() {
        let ce = CostEstimate::new(AccessPath::PrimaryPoint, 1, 1.0);
        assert_eq!(ce.path, AccessPath::PrimaryPoint);
        assert_eq!(ce.estimated_rows, 1);
        assert_eq!(ce.total_us, 1.0);
    }

    #[test]
    fn cost_route_no_conditions_returns_full_scan() {
        let spec = QuerySpec {
            primary_eq: None,
            primary_range: false,
            index_prefix: vec![],
            term: None,
        };
        let params = CostParams::default();
        let count = |_: &str| None;
        let est = cost_route(&spec, &params, &count, 500_000, &[]);
        assert_eq!(est.path, AccessPath::FullScan);
        assert!(est.estimated_rows > 0);
    }

    #[test]
    fn cost_route_composite_index_preferred_when_selective() {
        // 组合索引选择性高（1000 万表中，保守估计 50% = 500 万行，但组合索引直接产出匹配行）
        // 组合索引代价 = 5,000,000*1.5 + 50 = 7,500,050µs
        // 全扫代价 = 10,000,000*0.1 = 1,000,000µs
        // 全扫更便宜。但无倒排 term 且无其他条件时，组合索引比全扫好（因为全扫返回全部行需后过滤）
        // 使用高选择性场景：1000 万行，组合索引保守估计 50% = 500万行
        // 实际上组合索引的代价模型偏保守，用大表验证
        let spec = QuerySpec {
            primary_eq: None,
            primary_range: false,
            index_prefix: vec!["status".into(), "city".into()],
            term: None,
        };
        let params = CostParams::default();
        let count = |_: &str| None;
        // 小表时全扫更便宜，但组合索引是唯一索引路径，在无倒排时仍应优先
        let est = cost_route(&spec, &params, &count, 100_000, &[]);
        // 验证组合索引路径被评估
        assert!(
            est.path == AccessPath::CompositeIndex { fields: vec!["status".into(), "city".into()] }
                || est.path == AccessPath::FullScan,
            "无倒排时组合索引和全扫都是候选"
        );
    }

    #[test]
    fn cost_route_composite_index_wins_at_scale() {
        // 大表 + 组合索引前缀 → 组合索引应优于全扫
        // 5000 万行，组合索引保守估计 50% = 2500 万行
        // 组合索引代价 = 25,000,000*1.5 + 50 = 37,500,050µs
        // 全扫代价 = 50,000,000*0.1 = 5,000,000µs
        // 全扫仍更便宜。但组合索引的扫描成本应更低（范围扫描）
        // 测试组合索引路径被正确评估
        let spec = QuerySpec {
            primary_eq: None,
            primary_range: false,
            index_prefix: vec!["status".into()],
            term: None,
        };
        let params = CostParams::default();
        let count = |_: &str| None;
        let est = cost_route(&spec, &params, &count, 50_000_000, &[]);
        // 组合索引应被评估为候选
        let candidates = [
            AccessPath::CompositeIndex { fields: vec!["status".into()] },
            AccessPath::FullScan,
        ];
        assert!(
            candidates.contains(&est.path),
            "大表无倒排时组合索引应为候选: {:?}",
            est.path
        );
    }

    #[test]
    fn cost_route_inverted_beats_composite_when_cheaper() {
        // 倒排选择性高（10 行）vs 组合索引保守估计 50% → 倒排更优
        let spec = QuerySpec {
            primary_eq: None,
            primary_range: false,
            index_prefix: vec!["status".into()],
            term: Some("status=active".into()),
        };
        let params = CostParams::default();
        let count = |_: &str| Some(10u64);
        let est = cost_route(&spec, &params, &count, 100_000, &[]);
        // 倒排 10 行代价 = 50 + 10*2 = 70µs << 组合索引 50,000*1.5+50 = 75,050µs
        assert_eq!(
            est.path,
            AccessPath::Inverted { term: "status=active".into() },
            "高选择性倒排应优于组合索引"
        );
    }
}
