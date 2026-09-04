//! 查询与统计子命令（本地引擎直连）：`search` / `range` / `explain`、
//! 倒排统计 `count` / `groupby`（COUNT / GROUP BY，阶段 1.5 M4）。

use std::path::Path;

use super::open_engine;

/// `search --filter 'status=active AND type=order'`
pub(crate) fn run_cli_search(config_path: &Path, filter: &str) {
    if filter.is_empty() {
        eprintln!("❌ search 需要 --filter 'field=value [AND field2=value2]'");
        std::process::exit(1);
    }
    let mut engine = open_engine(config_path);
    match shanshui_cunji::server::execute_filter(&mut engine, filter) {
        Ok(rows) => {
            println!("命中 {} 条：", rows.len());
            for (docid, v) in &rows {
                println!("  docid={docid}  {}", String::from_utf8_lossy(v));
            }
        }
        Err(e) => {
            eprintln!("❌ 查询失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `range --start 1000 --end 2000`
pub(crate) fn run_cli_range(config_path: &Path, start: Option<u64>, end: Option<u64>) {
    let mut engine = open_engine(config_path);
    let desc = match (start, end) {
        (Some(s), Some(e)) => format!("[{s}..{e}]"),
        (Some(s), None) => format!("[{s}..]"),
        (None, Some(e)) => format!("[..{e}]"),
        (None, None) => "[全量]".into(),
    };
    match engine.scan_range(start, end) {
        Ok(rows) => {
            println!("范围 {desc} 命中 {} 条：", rows.len());
            for (docid, v) in &rows {
                println!("  docid={docid}  {}", String::from_utf8_lossy(v));
            }
        }
        Err(e) => {
            eprintln!("❌ 查询失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `explain --filter 'status=active'`（development 5.26）：执行计划推演，不读数据。
pub(crate) fn run_cli_explain(config_path: &Path, filter: &str) {
    if filter.is_empty() {
        eprintln!("❌ explain 需要 --filter 'field=value'");
        std::process::exit(1);
    }
    let mut engine = open_engine(config_path);
    match shanshui_cunji::explain::explain(&mut engine, filter) {
        Ok(plan) => {
            println!("访问路径: {}", plan.access);
            println!("索引键: {}", plan.key);
            match plan.estimated_rows {
                Some(n) => println!("估算行数: {n}"),
                None => println!("估算行数: 未知"),
            }
            if let Some(w) = plan.warning {
                println!("告警: {w}");
            }
        }
        Err(e) => {
            eprintln!("❌ 推演失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `count --field status --value active`（倒排 doc_count，development 5.17 COUNT）
pub(crate) fn run_cli_count(config_path: &Path, field: &str, value: &str) {
    if field.is_empty() || value.is_empty() {
        eprintln!("❌ count 需要 --field <字段> --value <值>");
        std::process::exit(1);
    }
    let mut engine = open_engine(config_path);
    match shanshui_cunji::server::execute_count(&mut engine, field, value) {
        Ok(n) => println!("{field}={value} → {n} 条"),
        Err(e) => {
            eprintln!("❌ 计数失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `groupby --field status`（遍历字段倒排 Term 集合构造分组，development 5.17 GROUP BY）
pub(crate) fn run_cli_group_by(config_path: &Path, field: &str) {
    if field.is_empty() {
        eprintln!("❌ groupby 需要 --field <字段>");
        std::process::exit(1);
    }
    let mut engine = open_engine(config_path);
    match shanshui_cunji::server::execute_group_by(&mut engine, field) {
        Ok(groups) => {
            println!("字段 {field} 分组（{} 组）：", groups.len());
            for (term, count) in &groups {
                let val = term.split_once('=').map(|(_, v)| v).unwrap_or(term);
                println!("  {val:<24} {count}");
            }
        }
        Err(e) => {
            eprintln!("❌ 分组失败: {e}");
            std::process::exit(1);
        }
    }
}
