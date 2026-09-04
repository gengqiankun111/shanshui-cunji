//! MySQL 单连接会话状态（server/session.rs）：内容拆分自原 src/db_adapter.rs——Session
//! 结构体（认证状态 / 活动事务 / 隔离级别 / 预处理语句表 / auto_id 分配器）与 new_session。

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// 单连接会话状态。
pub(crate) struct Session {
    pub(crate) user: String,
    pub(crate) authenticated: bool,
    /// H-4：活动事务（BEGIN 后创建，COMMIT/ROLLBACK 结束；连接断开自动回滚 = drop）。
    pub(crate) txn: Option<crate::txn::Transaction>,
    /// 会话级隔离级别（SET TRANSACTION ISOLATION LEVEL 设置；BEGIN 时生效，默认 RR）。
    pub(crate) isolation: crate::txn::Isolation,
    /// H-5：预处理语句表（stmt_id → 原始 SQL，占位符 `?`）。
    pub(crate) statements: std::collections::HashMap<u32, String>,
    pub(crate) next_stmt_id: u32,
    /// sysbench 兼容：无 id 列的 INSERT（auto_increment 语义）共享递增分配器。
    pub(crate) auto_id: Arc<AtomicU64>,
}

/// 新建会话（同步/异步连接共用）。
pub(crate) fn new_session(auto_id: Arc<AtomicU64>) -> Session {
    Session {
        user: String::new(),
        authenticated: false,
        txn: None,
        isolation: crate::txn::Isolation::RepeatableRead,
        statements: std::collections::HashMap::new(),
        next_stmt_id: 1,
        auto_id,
    }
}
