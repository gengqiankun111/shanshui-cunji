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
pub mod blockcache;
pub mod bloom;
pub mod column_family;
pub mod config;
pub mod demo;
pub mod engine;
pub mod error;
pub mod explain;
pub mod hotcache;
pub mod inverted;
pub mod join;
pub mod keys;
pub mod memtable;
pub mod migrate;
pub mod optimizer;
pub mod schema;
pub mod server;
pub mod sstable;
pub mod storage;
pub mod value;
pub mod wal;
pub mod watchdog;

pub use error::{Error, Result};
