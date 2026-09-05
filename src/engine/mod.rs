//! 文档引擎外观层（reconstruct.md engine/ 规划）：Engine 主结构体 + 对外整合 API，
//! 按主题拆分为（目录不变，均为 src/engine/ 下文件）：
//!
//! ```text
//! engine.rs  核心：Engine 结构体 + 共享类型（QueryRow/PagedRows/EngineStats）+ 核心协调/可观测 API
//! read.rs    读族：get / batch_get / batch_get_fields / zone_field_aggregate
//! scan.rs    扫描族：主键范围/流式/keys-only/分页/游标 + 水位/活跃集/COUNT(*)
//! write.rs   写族：put / put_nosync / delete / delete_batch / patch / purge / outbox
//! query.rs   查询协调：倒排/组合索引查询、execute、倒排刷盘/GC/统计
//! compact.rs 压缩协调：compact / compact_inner / compaction_targets / 删除密度触发
//! open.rs    生命周期：open / 备份恢复 / prepare_backup / Drop 清理
//! txn.rs     事务：D 阶段 WriteBatch 原子写 + E/F 事务状态机（begin/commit/rollback）/
//!            锁表与写集协调 / 事务内读（scan_range_txn）
//! mvcc.rs    快照：begin_snapshot / active_snapshots 注册注销 / 快照读 get_at /
//!            compact MVCC 保活水位（apply/clear_mvcc_floor）
//! tests.rs   （#[cfg(test)]）引擎全量单元测试
//! ```
//!
//! 本文件负责声明子模块并 re-export：`crate::engine::*` 原有 pub 项路径保持不变
//! （lib.rs 的 `pub mod engine;` 自动指向本目录，无需改动）。

mod compact;
mod engine;
mod mvcc;
mod open;
mod percpu_wal;
mod query;
mod read;
mod scan;
mod txn;
mod write;

#[cfg(test)]
mod tests;

pub use compact::CompactTargets;
pub use engine::{Engine, EngineStats, PagedRows, QueryRow};
pub use open::BackupReport;
