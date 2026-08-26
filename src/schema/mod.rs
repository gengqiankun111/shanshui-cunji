//! 字段注册表（FieldRegistry）：字段名 ↔ u16 ID 映射的持久化与演进（D1）。
//!
//! - 解决 D1：进程重启后，磁盘上可能存有旧版本未注册的新字段，若注册表无法重建将导致反序列化失败；
//! - 启动时先读 `metadata/fields.idx`，遇文档中的未注册字段自动扩展映射并持久化。

pub mod registry;

pub use registry::FieldRegistry;
