//! 执行层（原 sqlish.rs 求值/执行段拆分）：
//!
//! - `eval.rs`：求值/轻量字节扫描基建（full_docids/LightVal/leaf 扫描/eval 位图）；
//! - `select.rs`：通用 SELECT 执行（execute/doc_matches_where/get_docid_set/排序/Top-K）；
//! - `join.rs`：JOIN 执行（execute_join）；
//! - `aggregate.rs`：标量聚合（execute_aggregate_window/execute_aggregate/AggScalar）；
//! - `group_by.rs`：分组聚合（execute_group_by_window/GroupResult/HAVING）。

pub mod aggregate;
pub mod eval;
pub mod group_by;
pub mod join;
pub mod select;
