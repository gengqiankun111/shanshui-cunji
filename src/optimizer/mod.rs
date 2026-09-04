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
//!
//! 模块结构（按主题拆分，对外路径经 `pub use` 汇总保持不变）：
//! - 本文件：查询访问路径类型与静态路由（`AccessPath` / `QuerySpec` / `route`）；
//! - `stats`：字段/表级统计信息（`ColumnStatistics` / `TableStatistics`）；
//! - `cost`：代价模型参数与估算结果（`CostParams` / `CostEstimate`）；
//! - `route`：基于代价的动态路由决策（`cost_route` / `choose_best_plan`）；
//! - `tests`：单元测试。

mod cost;
mod route;
mod stats;

pub use cost::{CostEstimate, CostParams};
pub use route::{choose_best_plan, cost_route};
pub use stats::{ColumnStatistics, TableStatistics};

#[cfg(test)]
mod tests;

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
