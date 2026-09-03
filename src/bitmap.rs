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
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Mutex;

use arc_swap::ArcSwap;

use crate::error::Result;

/// 页大小（对齐 SSD 页，design 4.8）。
pub const PAGE_BYTES: usize = 4096;
/// 每页容纳的 docid 数：4KB × 8bit = 32768。
pub const BITS_PER_PAGE: u64 = (PAGE_BYTES * 8) as u64;

/// 删除位图：稠密内存位图 + 4KB 页对齐持久化文件。
/// Ex-8.14：**读路径无锁化**——位组改 `ArcSwap<Box<[AtomicU8]>>`：`is_deleted` 取快照（ArcSwap
/// guard，无 RwLock）后单字节原子读，删除/扫描/计数并发互不阻塞（此前逐行 RwLock 读锁在
/// 50m 行级判定上竞争真实存在）；写路径（mark/clear）短 Mutex 串行（引擎本就单写者，锁仅护
/// 扩容换代与 in-place 位翻转的自我健全）；扩容（新 docid 越界）时倍增复制后换代。
#[derive(Debug, Default)]
pub struct DeletionBitmap {
    /// 稠密位组（**每元素 = 1 文件字节**，值 0..=255；LSB-first，byte=docid/8，bit=docid%8）——
    /// 读无锁快照载体；元素为 AtomicU8 支持并发原子读。
    bytes: ArcSwap<Box<[AtomicU8]>>,
    /// 脏页索引（docid/BITS_PER_PAGE）：`flush` 时只写这些页。
    dirty: Mutex<HashSet<u64>>,
    /// 写者串行锁（mark/clear/扩容换代）——引擎写路径本就单写者；读路径不经过此锁。
    write: Mutex<()>,
    /// 位图文件路径（`<data_dir>/deletion.bitmap`）。
    path: PathBuf,
}

impl DeletionBitmap {
    /// 打开（或创建）删除位图：文件存在则加载已有位数组，否则为空位图。
    pub fn open(path: &Path) -> Result<Self> {
        let mut bits: Vec<AtomicU8> = Vec::new();
        if path.exists() {
            let mut f = File::open(path)?;
            let mut buf = Vec::new();
            f.read_to_end(&mut buf)?;
            // 逐字节展开（文件恒为 4KB 倍数，含尾部对齐页）
            bits.reserve(buf.len());
            for byte in buf {
                bits.push(AtomicU8::new(byte));
            }
        }
        Ok(Self {
            bytes: ArcSwap::from_pointee(bits.into_boxed_slice()),
            dirty: Mutex::new(HashSet::new()),
            write: Mutex::new(()),
            path: path.to_path_buf(),
        })
    }

    /// 删除：置位（O(1) 内存）+ 标记脏页；仅当位确实翻转才写页（重复删除零 IO）。
    /// Ex-8.7：返回是否**新置位**（此前未删）——调用方据此维护删除密度计数器（幂等重删不重计）。
    pub fn mark_deleted(&self, docid: u64) -> bool {
        if !self.is_deleted(docid) {
            self.mutate(docid, true);
            true
        } else {
            false
        }
    }

    /// 复活（put 重新写入）：清位（O(1)）+ 标记脏页；位本就未置时零 IO。
    /// Ex-8.7：返回是否**实际清位**（此前已删）——调用方据此减除删除密度计数（复活才减）。
    pub fn clear(&self, docid: u64) -> bool {
        if self.is_deleted(docid) {
            self.mutate(docid, false);
            true
        } else {
            false
        }
    }

    /// 查询（O(1)，**无锁**）：已删返回 true；越界（从未删除）返回 false。
    /// 每次调用取 ArcSwap 快照（~10ns，无 RwLock/无互斥），与写路径（delete/put 复活/扩容）
    /// 及并发扫描完全并行。
    pub fn is_deleted(&self, docid: u64) -> bool {
        let bytes = self.bytes.load();
        let byte = (docid / 8) as usize;
        if byte >= bytes.len() {
            return false;
        }
        let bit = docid % 8;
        (bytes[byte].load(Ordering::Acquire) >> bit) & 1 == 1
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
        self.bytes
            .load()
            .iter()
            .map(|b| (b.load(Ordering::Acquire) as u64).count_ones() as u64)
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
        let bytes = self.bytes.load();
        for p in pages {
            let start = (p * BITS_PER_PAGE) as usize;
            let mut buf = vec![0u8; PAGE_BYTES];
            for (cell, bit_cell) in buf.iter_mut().enumerate() {
                let global = start + cell * 8;
                let mut byte = 0u8;
                for b in 0..8 {
                    let g = global + b;
                    let cell_idx = g / 8; // 元素 = 1 字节（docid/8）
                    if cell_idx < bytes.len()
                        && (bytes[cell_idx].load(Ordering::Acquire) >> (g % 8)) & 1 == 1
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

    /// DROP TABLE purge：清空位图内存态并删除持久化文件（等价从未删除；重启后重建为空）。
    pub fn purge(&self) {
        let _w = self.write.lock().unwrap();
        self.bytes
            .store(std::sync::Arc::new(Box::new([] as [AtomicU8; 0])));
        self.dirty.lock().unwrap().clear();
        if self.path.exists() {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    /// 置位/清位公共路径（写者串行 + 必要时扩容换代）。
    fn mutate(&self, docid: u64, set: bool) {
        let _g = self.write.lock().unwrap(); // 引擎单写者；锁护扩容换代与位翻转的自健全
        let byte = (docid / 8) as usize;
        if byte >= self.bytes.load().len() {
            let bytes = self.bytes.load();
            let old = bytes.len();
            let need = byte + 1;
            let mut next: Vec<AtomicU8> = Vec::with_capacity(need.max(old.max(1) * 2));
            for b in bytes.iter() {
                next.push(AtomicU8::new(b.load(Ordering::Acquire)));
            }
            next.resize_with(need.max(old.max(1) * 2), || AtomicU8::new(0));
            self.bytes.store(std::sync::Arc::new(next.into_boxed_slice()));
        }
        let bit = docid % 8;
        let cell = &self.bytes.load()[byte];
        if set {
            cell.fetch_or(1 << bit, Ordering::AcqRel);
        } else {
            cell.fetch_and(!(1 << bit), Ordering::AcqRel);
        }
        self.dirty.lock().unwrap().insert(docid / BITS_PER_PAGE);
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
    fn lock_free_reads_under_writer_growth_and_flips() {
        // Ex-8.14：读者（is_deleted 无锁）与写者（翻转 + 扩容换代）并发——不崩溃、最终态正确
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let bm = Arc::new(DeletionBitmap::open(&dir.path().join("del.bitmap")).unwrap());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let bm = Arc::clone(&bm);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut seen = 0u64;
                    let mut d = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let _ = bm.is_deleted(d % 300_000);
                        seen += 1;
                        d = d.wrapping_add(7);
                    }
                    seen
                })
            })
            .collect();
        for d in (0..300_000u64).step_by(2) {
            bm.mark_deleted(d); // 含扩容（300k docid → 37.5KB → 倍增多次）
        }
        for d in (0..300_000u64).step_by(6) {
            bm.clear(d); // 复活部分偶数
        }
        // 最终态：2|6 → (2|6 mod 6==0 → cleared) evens step6 cleared → 剩 2 的倍数非 6 倍数
        for d in (0..300_000u64).step_by(6) {
            assert!(!bm.is_deleted(d), "{d} 应被复活清除");
        }
        for d in 0..300_000u64 {
            let expect = d % 2 == 0 && d % 6 != 0;
            assert_eq!(bm.is_deleted(d), expect, "docid {d} 最终态不符");
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }
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
