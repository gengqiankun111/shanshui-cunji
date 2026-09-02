//! 删除位图（Ex-5.6，design 4.6 / 4.8.3 阶段二）：独立于 LSM 的按 DocId 1bit 删除位图。
//!
//! 目标：删除仅写 1bit + fsync 1 页（对比 LSM Tombstone 全链路 -99% IO）、查询 O(1) 跳过已删
//! 文档、墓碑不再污染 LSM 层级；compaction 按位图物理删除后位图标记保留（docid 视为已删），
//! 重新写入（put）时清位复活。
//!
//! 存储布局：文件 = 纯位数组（无头），4KB 页对齐（对齐 SSD 页）；每页容纳 4096×8=32768 个
//! docid；文件按 4KB 页粒度增长（不产生子页写）。内存为稠密位图（`Vec<u64>`，1.5 亿 docid
//! ≈ 19MB），查询 O(1) 纯内存裁决、零磁盘 IO。
//!
//! 崩溃语义：置位/清位先改内存 + 标记脏页，`flush` 时只写脏页 + 一次 fsync（与 WAL flush 同点
//! 调用，见 Engine::flush_wal——先刷位图后刷 WAL，位图持久早于 WAL 截断推进，删除不丢失）。
//! 未 flush 的置位在崩溃后丢失 → 由 WAL 回放重删重建（幂等）。
//!
//! 零 unsafe 说明：design 的 mmap 方案与 inverted FST 字典同理（memmap2 为 unsafe API），
//! 此处用 `File` seek+write 页粒度落盘，保留"1bit/DocId + 4KB 页对齐 + 页粒度 IO"本质。

use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};

use crate::error::Result;

/// 页大小（对齐 SSD 页，design 4.8）。
pub const PAGE_BYTES: usize = 4096;
/// 每页容纳的 docid 数：4KB × 8bit = 32768。
pub const BITS_PER_PAGE: u64 = (PAGE_BYTES * 8) as u64;

/// 删除位图：稠密内存位图 + 4KB 页对齐持久化文件。
/// P72（无锁合并）：内部 `RwLock`（bits）+ `Mutex`（dirty）——Engine 字段 `Arc<DeletionBitmap>`
/// 共享（worker 无锁合并克隆 Arc 后读位图过滤；写路径在 Engine 写锁内，锁仅护结构内部一致性）。
#[derive(Debug, Default)]
pub struct DeletionBitmap {
    /// 稠密位图：docid 的 bit 位（LSB-first，byte = docid/8，bit = docid%8）。
    bits: RwLock<Vec<u64>>,
    /// 脏页索引（docid/BITS_PER_PAGE）：`flush` 时只写这些页。
    dirty: Mutex<HashSet<u64>>,
    /// 位图文件路径（`<data_dir>/deletion.bitmap`）。
    path: PathBuf,
}

impl DeletionBitmap {
    /// 打开（或创建）删除位图：文件存在则加载已有位数组，否则为空位图。
    pub fn open(path: &Path) -> Result<Self> {
        let mut bits = Vec::new();
        if path.exists() {
            let mut f = File::open(path)?;
            let mut buf = Vec::new();
            f.read_to_end(&mut buf)?;
            // 逐字节展开为 u64 位组（零 unsafe）；文件恒为 4KB 倍数（含尾部对齐页）
            bits.reserve(buf.len());
            for byte in buf {
                bits.push(byte as u64);
            }
        }
        Ok(Self {
            bits: RwLock::new(bits),
            dirty: Mutex::new(HashSet::new()),
            path: path.to_path_buf(),
        })
    }

    /// 删除：置位（O(1) 内存）+ 标记脏页；仅当位确实翻转才写页（重复删除零 IO）。
    pub fn mark_deleted(&self, docid: u64) {
        if !self.is_deleted(docid) {
            let mut bits = self.bits.write().unwrap();
            Self::ensure_capacity_bits(&mut bits, docid);
            let byte = (docid / 8) as usize;
            let bit = docid % 8;
            bits[byte] |= 1 << bit;
            self.dirty.lock().unwrap().insert(docid / BITS_PER_PAGE);
        }
    }

    /// 复活（put 重新写入）：清位（O(1)）+ 标记脏页；位本就未置时零 IO。
    pub fn clear(&self, docid: u64) {
        if self.is_deleted(docid) {
            let mut bits = self.bits.write().unwrap();
            Self::ensure_capacity_bits(&mut bits, docid);
            let byte = (docid / 8) as usize;
            let bit = docid % 8;
            bits[byte] &= !(1 << bit);
            self.dirty.lock().unwrap().insert(docid / BITS_PER_PAGE);
        }
    }

    /// 查询（O(1)）：已删返回 true；越界（从未删除）返回 false。
    pub fn is_deleted(&self, docid: u64) -> bool {
        let byte = docid / 8;
        let bits = self.bits.read().unwrap();
        if byte >= bits.len() as u64 {
            return false;
        }
        let bit = docid % 8;
        (bits[byte as usize] >> bit) & 1 == 1
    }

    /// 主数据键（8 字节大端 docid）的已删判定（compaction 过滤用）。
    pub fn is_deleted_key(&self, key: &[u8]) -> bool {
        if key.len() != 8 {
            return false; // 非主键键（组合索引等）不参与
        }
        self.is_deleted(u64::from_be_bytes(key[..8].try_into().unwrap()))
    }

    /// 当前已删 docid 总数。
    pub fn deleted_count(&self) -> u64 {
        self.bits
            .read()
            .unwrap()
            .iter()
            .map(|w| w.count_ones() as u64)
            .sum()
    }

    /// 是否有待落盘脏页。
    pub fn has_pending(&self) -> bool {
        !self.dirty.lock().unwrap().is_empty()
    }

    /// 脏页全部落盘：每页 4KB 对齐写一次 + 一次 fsync（同页 N 次删除 = 1 页写 + 1 fsync）。
    pub fn flush(&self) -> Result<()> {
        let dirty_pages: Vec<u64> = self.dirty.lock().unwrap().iter().copied().collect();
        if dirty_pages.is_empty() {
            return Ok(());
        }
        let mut f = File::options()
            .create(true)
            .truncate(false) // 覆盖写：不截断（保留既有页）
            .write(true)
            .open(&self.path)?;
        let mut pages = dirty_pages;
        pages.sort_unstable();
        let bits = self.bits.read().unwrap();
        for p in pages {
            let start = (p * BITS_PER_PAGE) as usize;
            let mut buf = vec![0u8; PAGE_BYTES];
            for (cell, bit_cell) in buf.iter_mut().enumerate() {
                let global = start + cell * 8;
                let mut byte = 0u8;
                for b in 0..8 {
                    if global + b < bits.len() * 8
                        && (bits[(global + b) / 8] >> ((global + b) % 8)) & 1 == 1
                    {
                        byte |= 1 << b;
                    }
                }
                *bit_cell = byte;
            }
            let offset = p * PAGE_BYTES as u64;
            f.seek(SeekFrom::Start(offset))?;
            f.write_all(&buf)?; // 越界页写入自动扩展文件（4KB 页粒度）
        }
        f.sync_all()?;
        self.dirty.lock().unwrap().clear();
        Ok(())
    }

    fn ensure_capacity_bits(bits: &mut Vec<u64>, docid: u64) {
        let need = (docid / 8) as usize + 1;
        if bits.len() < need {
            bits.resize(need, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let mut bm = DeletionBitmap::open(&dir.path().join("del.bitmap")).unwrap();
        for d in [0u64, 7, 8, 15, 16, 1023, 1_000_000] {
            assert!(!bm.is_deleted(d));
            bm.mark_deleted(d);
            assert!(bm.is_deleted(d));
        }
        bm.clear(7);
        assert!(!bm.is_deleted(7));
        assert!(bm.is_deleted(8));
        assert_eq!(bm.deleted_count(), 6);
    }

    #[test]
    fn page_alignment_and_reopen_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("del.bitmap");
        {
            let mut bm = DeletionBitmap::open(&path).unwrap();
            bm.mark_deleted(BITS_PER_PAGE - 1); // 页 0 尾
            bm.mark_deleted(BITS_PER_PAGE); // 页 1 首
            bm.mark_deleted(BITS_PER_PAGE * 2 + 5); // 页 2
            bm.flush().unwrap();
        }
        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len() as usize, 3 * PAGE_BYTES, "文件 4KB 对齐");
        let bm = DeletionBitmap::open(&path).unwrap();
        assert!(bm.is_deleted(BITS_PER_PAGE - 1));
        assert!(bm.is_deleted(BITS_PER_PAGE));
        assert!(bm.is_deleted(BITS_PER_PAGE * 2 + 5));
        assert!(!bm.is_deleted(0));
    }

    #[test]
    fn unflushed_marks_are_lost_on_reopen() {
        // 未 flush 的置位重开（模拟崩溃）丢失 → 由 WAL 回放重建（幂等语义）
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("del.bitmap");
        {
            let mut bm = DeletionBitmap::open(&path).unwrap();
            bm.mark_deleted(42);
            bm.flush().unwrap();
            bm.mark_deleted(43); // 未 flush
        }
        let bm = DeletionBitmap::open(&path).unwrap();
        assert!(bm.is_deleted(42));
        assert!(!bm.is_deleted(43));
    }

    #[test]
    fn redundant_ops_do_not_dirty_pages() {
        // 重复删除 / 从未删除的 put 清位 → 零页写出（put 路径不产生额外 IO）
        let dir = tempfile::tempdir().unwrap();
        let bm = DeletionBitmap::open(&dir.path().join("del.bitmap")).unwrap();
        bm.mark_deleted(5);
        bm.mark_deleted(5); // 重复删除：位已置 → 不新增脏页
        assert_eq!(bm.dirty.lock().unwrap().len(), 1);
        bm.clear(99); // 从未删除 → 清位是 no-op → 不新增脏页
        assert_eq!(bm.dirty.lock().unwrap().len(), 1);
        bm.flush().unwrap();
        assert_eq!(std::fs::metadata(dir.path().join("del.bitmap")).unwrap().len(), PAGE_BYTES as u64);
    }
}
