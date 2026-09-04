//! 数据关联基础（development 5.20 sdk::join / 5.21 写入 Enrich，design 19）。
//!
//! - **queryAndJoin**：主表倒排筛选 → 批量回表 → 从表批量主键点查 → 内存 Hash 合并
//!   （Inner / Left / Right），结果集上限 `join.max_rows` 熔断；
//!   关联侧 key 数 ≤ `broadcast_threshold` 时走**小表广播 JOIN**（design 19.3，阶段 3）：
//!   一次全量扫描从表建立内存索引复用，避免逐 key 点查；
//! - **写入 Enrich**：网络层接收后、WAL 写入前执行回调展开关联数据到单文档，
//!   失败策略 reject（拒绝写入）/ degrade（降级写入原文档）。

use serde_json::Value;

mod merge;
mod route;
mod enrich;

pub use enrich::{enrich_check_local, put_with_enrich};
pub use merge::query_and_join;

/// JOIN 类型（design 19）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

/// queryAndJoin 执行规格。
#[derive(Debug, Clone)]
pub struct JoinSpec<'a> {
    /// 主表倒排筛选条件（execute_filter 语义，如 `type=order`）。
    pub filter: &'a str,
    /// 主表关联字段（取该字段值作为关联 key）。
    pub from_field: &'a str,
    /// 从表关联字段："docid"（主键点查）或其他字段（倒排 term `field=key` 查询）。
    pub to_field: &'a str,
    /// JOIN 类型。
    pub join_type: JoinType,
}

/// 一行 JOIN 结果：左表文档 + 右表文档（均保留原字段，避免命名冲突）。
#[derive(Debug, Clone)]
pub struct JoinRow {
    pub left: Value,
    pub right: Option<Value>,
}

/// 小表广播 JOIN 选项（design 19.3，阶段 3）：启用且主表筛选后去重关联 key 数
/// ≤ `threshold` 时，一次性全量扫描从表建立 `(关联 key → 文档)` 内存索引复用，
/// 替代逐 key 点查（IO 次数从 O(distinct_keys) 降为 1 次顺序扫描）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinBroadcast {
    /// 是否启用广播 JOIN。
    pub enabled: bool,
    /// 广播阈值（去重关联 key 数），超过则回退逐 key 点查。
    pub threshold: usize,
}

#[cfg(test)]
mod tests;
