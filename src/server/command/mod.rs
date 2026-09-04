//! MySQL 命令分发与执行（reconstruct.md server/command/ 规划）：COM_QUERY/COM_STMT 命令
//! 入口与 SQL 映射执行族。内容拆分自原 src/db_adapter.rs，按主题聚类为：
//!
//! ```text
//! query.rs       handle_command / dispatch_query / dispatch_query_read* / show_response
//! select.rs      select_response / COUNT 快路径（单文件 ~370 行）
//! dml.rs         非事务 INSERT/REPLACE/UPDATE/DELETE 响应 + 写定位
//! transaction.rs 事务内 SELECT / parse_isolation_level / extract_* 窗口解析
//! txn_dml.rs     事务内 INSERT/UPDATE/DELETE/REPLACE
//! stmt.rs        COM_STMT_PREPARE / COM_STMT_EXECUTE
//! ```

mod dml;
mod query;
mod select;
mod stmt;
mod transaction;
mod txn_dml;

pub(crate) use dml::*;
pub(crate) use query::*;
pub(crate) use select::*;
pub(crate) use stmt::*;
pub(crate) use transaction::*;
pub(crate) use txn_dml::*;
