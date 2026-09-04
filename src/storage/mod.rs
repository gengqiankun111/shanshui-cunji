//! 存储引擎层（reconstruct.md 目录规划）：列族框架 + 磁盘有序文件（SST）+ WAL + MemTable。
//!
//! ```text
//! column_family.rs  (ColumnFamily 主结构体，对外 CRUD/scan API)
//!   ├── memtable.rs      内存跳表 + 双缓冲
//!   ├── sstable/         磁盘有序文件（reader/writer/block/index/compaction）
//!   └── wal/             预写日志（writer/reader/ring）
//! ```

pub mod column_family;
pub mod manifest;
pub mod memtable;
pub mod sstable;
pub mod wal;
