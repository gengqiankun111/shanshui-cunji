//! 查询优化器骨架：静态路由（development 步骤 6）。
//!
//! 阶段 1 只做主键 / 范围查询的访问路径判定（静态路由，无代价估算）；
//! 阶段 1.5 起引入统计载荷 + 代价估算（design 7.1），动态路由在步骤 12 落地。
//!
//! 路由规则（MVP）：
//! - 等值主键查询 → 主数据列族点查（布隆 + 稀疏索引 + 块缓存）；
//! - 主键范围查询 → 主数据列族范围扫描；
//! - 组合索引等值前缀 → 组合索引列族（命中索引键后回表）；
//! - 全文 / 字段条件 → 倒排列族（步骤 10 落地后启用）。

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
}
