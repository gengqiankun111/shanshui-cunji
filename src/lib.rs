//! shanshui-cunji 库入口：内核各模块对外暴露，供二进制与集成测试使用。

// 质量承诺：全库零 unsafe，编译期强制（cargo-geiger 实测 0 处；防止未来回归）
#![forbid(unsafe_code)]

// 全局分配器：mimalloc（design 14 分配器策略，消除 musl 默认 malloc 全局锁瓶颈；
// `#[global_allocator]` 声明无 unsafe，unsafe 实现在 mimalloc crate 内部，不违反零 unsafe 承诺）。
// - 默认 feature `alloc-mimalloc`：mimalloc；
// - `alloc-jemalloc`：tikv-jemallocator（mallctl purge + stats）；
// - `--no-default-features`：不设置 global_allocator，用系统默认分配器（压测对比基线）。
#[cfg(feature = "alloc-mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "alloc-jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub mod admin;
pub mod affinity;
pub mod backup;
pub mod bitmap;
pub mod blockcache;
pub mod bloom;
pub mod config;
// 网络服务层（原 src/db_adapter.rs 拆分至 src/server/，见 reconstruct.md）；别名保持
// 全仓既有 crate::db_adapter::* 引用路径不变（bin/export.rs、bin/mysql_server.rs）。
pub mod server;
pub use server as db_adapter;
pub mod demo;
pub mod docid_alloc;
pub mod docset;
pub mod engine;
pub mod error;
pub mod explain;
pub mod export_pipeline;
pub mod external_cache;
pub mod gateway;
pub mod hotcache;
pub mod import_schema;
pub mod indexer_proxy;
pub mod inverted;
pub mod io_queue;
pub mod io_scheduler;
pub mod join;
pub mod keys;
pub mod meta;
pub mod metrics;
pub mod migrate;
pub mod mv;
pub mod multitable;
pub mod optimizer;
pub mod outbox;
pub mod per_cpu;
pub mod redis;
pub mod replication;
pub mod reshard;
pub mod rpc;
pub mod raft_meta;
pub mod raft_rpc;
pub mod saga;
pub mod scale_out;
pub mod schema;
pub mod sdk_cache;
pub mod seqlock;
pub mod shard_build;
pub mod shard_inverted;
pub mod shard_metrics;
pub mod sharding;
// sql 层（原 src/sqlish.rs 拆分至 src/sql/，见 reconstruct.md）；sqlish 别名保持
// 全仓既有 crate::sqlish::* 引用路径不变（db_adapter/demo/server 无需改动）。
pub mod sql;
pub use sql as sqlish;
pub mod storage;
pub mod tds;
pub mod term_cache;
pub mod txn;
pub mod value;
pub mod watchdog;
pub mod timing_wheel;

// reconstruct.md 目录规划：storage 引擎层已归入 src/storage/ 目录。
// 根部 re-export 保持既有调用路径（crate::column_family / crate::sstable / ...）不变。
pub use storage::{column_family, memtable, sstable, wal};

pub use error::{Error, Result};
