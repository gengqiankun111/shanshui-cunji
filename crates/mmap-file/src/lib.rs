//! 只读 mmap 文件视图（安全封装）。
//!
//! 背景（P23 决策）：主库 `#![forbid(unsafe_code)]`，而 memmap2 的 mmap 为 unsafe API——
//! 故将 mmap 隔离到**本独立 crate**（唯一的 unsafe 白名单位置），对外只暴露安全 API
//! （`MmapFile::open` → `Deref<[u8]>`），主库源码保持零 unsafe 承诺不变。
//! 用途：倒排 FST 术语字典（Ex-5.7，design 5.2.4.1）按需加载——mmap 仅建虚拟地址映射，
//! 物理页由 OS 缺页中断按需加载：冷启动零堆分配（demo 实测 17MB FST：fs::read 全量 vs mmap 0B）、
//! RSS 与访问量成正比（OS Page Cache LRU 管理冷热）。
//!
//! # Safety 论证（本 crate unsafe 白名单的完整依据）
//!
//! `unsafe { memmap2::Mmap::map(&file) }` 的前置条件与生命周期（对齐 memmap2 文档）：
//! 1. **只读映射（PROT_READ）**：`Mmap::map` 建立只读映射，不存在写指针逃逸，
//!    无法通过映射修改文件 → 无数据竞争源；
//! 2. **fd 生命周期解耦**：映射建立后 `File` 句柄即可释放（mmap 与 fd 无生命周期依赖），
//!    `MmapFile::open` 中 File 为局部变量，drop 在映射建立后 → 满足要求；
//! 3. **文件不可变性**：mmap 的文件在映射期间**不得被截断/改写**（否则属 UB）。
//!    调用约定：仅映射 FST 字典文件——发布流程为「写 tmp → fsync → 原子 rename」，
//!    映射后文件内容恒定；GC 删除旧文件前先释放映射（`inverted.rs` gc 先清 dicts 再删文件，
//!    Windows 下已映射文件亦无法删除，顺序保证双端一致）；
//! 4. **Send/Sync 推导**：memmap2::Mmap 为 `!Send + !Sync`（保守标记——同一进程内若另一
//!    线程截断文件会产生 UB）。本类型补充 `unsafe impl Send + Sync`，依据：
//!    - 只读映射无内部可变状态，`&MmapFile` 并发读等价于 `&[u8]` 并发读（天然安全）；
//!    - 文件不可变（见 3）→ 不存在并发截断/改写路径 → 无 UB 前提成立。
//!    该模式是只读 mmap 的行业标准封装（memmap2 文档推荐的自定义 wrapper 方案）。

use std::fs::File;
use std::io;
use std::ops::Deref;
use std::path::Path;

/// 只读 mmap 文件视图：`Deref<[u8]>` + `AsRef<[u8]>`（与 `Vec<u8>` 同接口，可无缝替换）。
pub struct MmapFile {
    map: memmap2::Mmap,
}

impl MmapFile {
    /// 打开并只读映射整个文件（mmap 仅建虚拟地址映射，物理页按需缺页加载，零堆分配）。
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        // SAFETY: 本 crate 为 unsafe 白名单，论证见模块头文档——
        // 只读映射 + fd 生命周期解耦 + 调用方保证文件不可变（FST 字典发布约定）。
        let map = unsafe { memmap2::Mmap::map(&file) }?;
        Ok(Self { map })
    }
}

// SAFETY: 只读映射、文件不可变（见模块头第 3/4 点），跨线程共享读安全。
unsafe impl Send for MmapFile {}
unsafe impl Sync for MmapFile {}

impl Deref for MmapFile {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.map
    }
}

impl AsRef<[u8]> for MmapFile {
    fn as_ref(&self) -> &[u8] {
        &self.map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_maps_file_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.bin");
        std::fs::write(&path, b"hello mmap").unwrap();
        let m = MmapFile::open(&path).unwrap();
        assert_eq!(&m[..], b"hello mmap");
        assert_eq!(m.as_ref(), b"hello mmap");
    }

    #[test]
    fn open_missing_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        assert!(MmapFile::open(&dir.path().join("nope.bin")).is_err());
    }

    #[test]
    fn send_and_sync_compiles() {
        // 主库以 Arc<Mutex<Engine>> 跨线程共享（Engine: Send 要求）——此处仅编译期验证
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MmapFile>();
    }
}
