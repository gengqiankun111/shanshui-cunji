//! 类 SQL 解析器（design 157/1358 行：SELECT ... WHERE AND/OR 子集，走倒排/组合索引）。
//!
//! 语法子集（递归下降，零外部依赖——不引入 sqlparser-rs 大依赖）：
//!   SELECT [*|col1,col2] FROM <表名> [WHERE <expr>] [LIMIT n [OFFSET m]]
//!   expr  := and (OR and)*   and := unary (AND unary)*   unary := NOT unary | '(' expr ')' | cond
//!   cond  := 字段 op 值     op := = != > < >= <= | BETWEEN low AND high（闭区间，数值）
//!        值 := '字面量' | "字面量" | 裸词 | 数字
//!
//! 求值语义（内部走倒排，与 search_term_paged 同源）：
//!   - `field=value` → 倒排 posting（Roaring 位图）；AND=交集 / OR=并集 / NOT=补集（相对全量）；
//!   - `field!=value` → 全量 − posting；
//!   - 比较（>/</>=/<=）与 BETWEEN → 倒排无法表达，扫描过滤；**AND 快路径**：作为后过滤
//!     只检查另一分支（倒排等值）已命中的文档，避免全量扫描（推荐写法：
//!     `WHERE 枚举等值 AND 数值 BETWEEN ...`）；
//!   - `docid=123` 特例 → 主键点查单例位图；
//!   - LIMIT/OFFSET 作用于最终位图（与分页语义一致）。
//!
//! 不承诺 MySQL 方言：不支持 JOIN / GROUP BY / 子查询 / 事务（design 157 行明确）。
//!
//! 目录规划（reconstruct.md sql 层）：本文件由原 src/sqlish.rs 按主题拆分而来——
//! 解析（词法/AST/递归下降）归 `parser/`，求值/扫描/执行归 `executor/`；
//! 顶部 re-export 保持原 `crate::sqlish::*` 既有调用路径不变（lib.rs 以
//! `pub use sql as sqlish` 提供别名；全仓既有引用 db_adapter/demo/server 无需改动）。

pub mod executor;
pub mod parser;

// 兼容 re-export：原 sqlish.rs 模块根部的 pub 项（全仓既有 crate::sqlish::X 路径）。
pub use executor::aggregate::{execute_aggregate, execute_aggregate_window, AggScalar};
pub use executor::group_by::{execute_group_by, execute_group_by_window, GroupResult, GroupRow};
pub use executor::select::{doc_matches_where, docset_to_sorted, execute, get_docid_set};
pub use parser::{
    parse_select, parse_where_expr, CmpOp, Cond, HavingCond, HavingExpr, JoinClause, JoinKind,
    Select, WhereExpr,
};

// 原 sqlish.rs 底部 mod tests（约 1700 行）整体迁移至 tests.rs；`use super::*` 取本模块
// re-export + 私有 helper 显式导入（见 tests.rs 顶部）。
#[cfg(test)]
mod tests;
