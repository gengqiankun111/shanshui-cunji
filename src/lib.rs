//! shanshui-cunji 库入口：内核各模块对外暴露，供二进制与集成测试使用。

// 质量承诺：全库零 unsafe，编译期强制（cargo-geiger 实测 0 处；防止未来回归）
#![forbid(unsafe_code)]

pub mod blockcache;
pub mod bloom;
pub mod column_family;
pub mod config;
pub mod demo;
pub mod engine;
pub mod error;
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
