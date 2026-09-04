//! 网络服务层（reconstruct.md server/ 规划）：MySQL wire 协议服务器（内容拆分自原
//! src/db_adapter.rs）与 HTTP-JSON 网关（原 src/server.rs 因与目录同名冲突整体迁入）。
//!
//! ```text
//! server.rs      DbServer 主结构体 + 生命周期/后台 worker/连接处理（server/server.rs）
//! session.rs     Session 会话状态 + new_session
//! client.rs      MysqlWireClient（JDBC 直连客户端）+ escape_sql
//! sqlparse.rs    SQL 解析辅助 + 表/docid 映射 + WHERE 写定位
//! http/          HTTP-JSON 网关包（原 src/server.rs 整体迁入后按主题拆分：mod.rs 入口/
//!                路由 + saga_api.rs + doc_api.rs + admin_api.rs + json.rs + tokenize.rs +
//!                tests.rs；公开面经下方 re-export）
//! protocol/      packet.rs 包编解码与协议常量；handshake.rs 握手认证；
//!                response.rs QueryResponse 与结果集构造
//! command/       query.rs 命令入口/SQL 分发/SHOW；select.rs SELECT 执行族；
//!                dml.rs 非事务 DML；transaction.rs 事务读与隔离级别；
//!                txn_dml.rs 事务内 DML；stmt.rs 预处理语句
//! tests.rs       原 src/db_adapter.rs mod tests（约 2880 行）整体迁移
//! ```
//!
//! lib.rs 以 `pub use server as db_adapter` 提供别名：原 `crate::db_adapter::*` 既有
//! 公开路径（DbServer / MysqlWireClient / escape_sql / check_native_password /
//! DEFAULT_TABLE / DEFAULT_DB）经下方 re-export 原路径不变。

mod client;
mod command;
mod http;
mod protocol;
mod server;
mod session;
mod sqlparse;
#[cfg(test)]
mod tests;

// ---- 兼容 re-export：原 crate::server（HTTP 网关 + 文本检索工具）公开面不变 ----
pub use http::{
    execute_count, execute_filter, execute_filter_paged, execute_group_by, execute_spec,
    extract_terms, extract_terms_filtered, extract_terms_with_fulltext,
    extract_terms_with_fulltext_seg, fulltext_terms, fulltext_terms_seg, parse_filter, serve,
    tokenize, tokenize_bigram, tokenize_seg, url_decode,
};

// ---- 兼容 re-export：原 src/db_adapter.rs 的 pub 项（crate::db_adapter 经 lib.rs 别名）----
pub use client::{escape_sql, MysqlWireClient};
pub use protocol::handshake::check_native_password;
pub use protocol::packet::{DEFAULT_DB, DEFAULT_TABLE};
pub use server::DbServer;

// 内部重组：db_adapter.rs 拆分后各子模块项聚合到 server 根，供子模块 / tests 以
// `use crate::server::*` 或 `use super::*` 跨文件引用（可见性升到 pub(crate)，语义零变化）。
pub(crate) use client::*;
pub(crate) use command::*;
pub(crate) use protocol::*;
pub(crate) use server::*;
pub(crate) use session::*;
pub(crate) use sqlparse::*;
