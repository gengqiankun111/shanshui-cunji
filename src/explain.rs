//! 执行计划推演（design 20 / development 5.26）。
//!
//! 复用优化器路由逻辑，**只推演不读数据**：
//! - 输出访问路径 / 索引键 / 估算行数（倒排 doc_count）/ 告警；
//! - CLI `explain --filter '...'` 与 HTTP `/explain` 共用，与 7.1 优化器同源避免两套逻辑漂移。

use serde::Serialize;

use crate::engine::Engine;
use crate::error::Result;
use crate::optimizer::{route, AccessPath, QuerySpec};

/// 执行计划。
#[derive(Debug, Clone, Serialize)]
pub struct ExplainPlan {
    /// 访问路径（AccessPath 字符串化）。
    pub access: String,
    /// 索引键（倒排 term / 主键 / 组合前缀）。
    pub key: String,
    /// 估算行数：倒排 = doc_count；主键点查 = 1；范围/全扫 = 未知（None）。
    pub estimated_rows: Option<u64>,
    /// 告警（如全表扫描、超长倒排列表）。
    pub warning: Option<String>,
}

/// 将 AccessPath 转为可读计划（估算行数需查引擎统计，只推演不读数据）。
pub fn explain(engine: &mut Engine, filter: &str) -> Result<ExplainPlan> {
    let conds = crate::server::parse_filter(filter);
    if conds.is_empty() {
        return Ok(ExplainPlan {
            access: format!("{:?}", AccessPath::FullScan),
            key: String::new(),
            estimated_rows: None,
            warning: Some("无过滤条件，走全表扫描".into()),
        });
    }
    // 主键点查
    if let Some((_, v)) = conds.iter().find(|(f, _)| f == "docid") {
        return Ok(ExplainPlan {
            access: format!("{:?}", AccessPath::PrimaryPoint),
            key: format!("docid={v}"),
            estimated_rows: Some(1),
            warning: None,
        });
    }
    // 单条件倒排：估算 = doc_count
    if conds.len() == 1 {
        let (f, v) = &conds[0];
        let term = format!("{f}={v}");
        let spec = QuerySpec {
            primary_eq: None,
            primary_range: false,
            index_prefix: vec![],
            term: Some(term.clone()),
        };
        let access = route(&spec);
        let count = engine.inverted_doc_count(&term)?;
        return Ok(ExplainPlan {
            access: format!("{access:?}"),
            key: term,
            estimated_rows: Some(count),
            warning: None,
        });
    }
    // 多条件 AND：倒排交集，估算取最小 doc_count
    let mut min_count: Option<u64> = None;
    let mut keys = Vec::new();
    for (f, v) in &conds {
        let term = format!("{f}={v}");
        keys.push(term.clone());
        let c = engine.inverted_doc_count(&term)?;
        min_count = Some(min_count.map_or(c, |m| m.min(c)));
    }
    let spec = QuerySpec {
        primary_eq: None,
        primary_range: false,
        index_prefix: vec![],
        term: keys.first().cloned(),
    };
    let access = route(&spec);
    Ok(ExplainPlan {
        access: format!("{access:?}"),
        key: keys.join(" AND "),
        estimated_rows: min_count,
        warning: Some("多条件 AND：估算行数取最小 Term 集合".into()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explain_empty_filter_is_full_scan() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(&dir.path(), &crate::config::Config::default()).unwrap();
        let plan = explain(&mut e, "").unwrap();
        assert!(plan.access.contains("FullScan"));
        assert!(plan.warning.is_some());
    }

    #[test]
    fn explain_docid_is_point_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(&dir.path(), &crate::config::Config::default()).unwrap();
        let plan = explain(&mut e, "docid=100").unwrap();
        assert!(plan.access.contains("PrimaryPoint"));
        assert_eq!(plan.estimated_rows, Some(1));
    }

    #[test]
    fn explain_single_term_estimates_doc_count() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(&dir.path(), &crate::config::Config::default()).unwrap();
        // 写 3 条 status=active
        for i in 1..=3u64 {
            let val = serde_json::json!({"docid": i, "status": "active"});
            let bytes = serde_json::to_vec(&val).unwrap();
            let terms = crate::server::extract_terms(&val);
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put(i, bytes, &t).unwrap();
        }
        e.flush_inverted().unwrap();
        let plan = explain(&mut e, "status=active").unwrap();
        assert!(plan.access.contains("Inverted"));
        assert_eq!(plan.estimated_rows, Some(3), "估算行数 = doc_count");
        assert_eq!(plan.key, "status=active");
    }

    #[test]
    fn explain_multi_cond_takes_min() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(&dir.path(), &crate::config::Config::default()).unwrap();
        for i in 1..=5u64 {
            let val = serde_json::json!({"docid": i, "status": "active", "city": "bj"});
            let bytes = serde_json::to_vec(&val).unwrap();
            let terms = crate::server::extract_terms(&val);
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put(i, bytes, &t).unwrap();
        }
        // 多一条 city=sh
        let val = serde_json::json!({"docid": 9, "status": "active", "city": "sh"});
        let bytes = serde_json::to_vec(&val).unwrap();
        let terms = crate::server::extract_terms(&val);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        e.put(9, bytes, &t).unwrap();
        e.flush_inverted().unwrap();
        let plan = explain(&mut e, "status=active AND city=bj").unwrap();
        assert_eq!(plan.estimated_rows, Some(5), "取最小 term 集合");
    }
}
