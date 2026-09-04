//! 基于代价的动态路由决策：单查询规格（`cost_route`）与多条件 AND 场景
//! （`choose_best_plan`），比较各可行访问路径的估算代价并返回最优者。

use super::{AccessPath, CostEstimate, CostParams, QuerySpec};

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
