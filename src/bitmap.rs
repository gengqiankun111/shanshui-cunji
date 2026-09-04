//! 删除位图（Ex-5.6，design 4.6 / 4.8.3 阶段二）：独立于 LSM 的按 DocId 1bit 删除位图。
//!
//! 目标：删除仅写 1bit + fsync（对比 LSM Tombstone 全链路 -99% IO）、查询 O(1) 跳过已删
//! 文档、墓碑不再污染 LSM 层级；compaction 按位图物理删除后位图标记保留（docid 视为已删），
//! 重新写入（put）时清位复活。
//!
//! P2-B（2026-09-04）：稀疏化重构——稠密 `Vec<AtomicU8>` → `RoaringTreemap` 稀疏位图。
//! 多表 docid = table_id<<48 | row，非默认表 docid ≥ 2^48，稠密数组需 32TB 物理不可行。
//! RoaringTreemap 按高 32 位分桶（天然按 table_id 隔离），内存 = O(删除集大小) 而非 O(docid 空间)。
//!
//! 持久化：全量序列化 → 覆写文件 → fsync（与 WAL flush 同点调用，见 Engine::flush_wal——
//! 先刷位图后刷 WAL，位图持久早于 WAL 截断推进，删除不丢失）。
//! 未 flush 的置位在崩溃后丢失 → 由 WAL 回放重删重建（幂等）。
//!
//! Ex-8.14 读路径无锁化：ArcSwap<RoaringTreemap> COW 写——`is_deleted` 取快照（ArcSwap
//! guard，无 RwLock/无互斥）后 `contains`，删除/扫描/计数并发互不阻塞；写路径（mark/clear）
//! COW（clone → mutate → store），读路径不经过写锁。

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use arc_swap::ArcSwap;
use roaring::treemap::RoaringTreemap;

use crate::error::Result;

/// 删除位图：稀疏 RoaringTreemap + ArcSwap 无锁读 + 全量序列化持久化。
///
/// P2-B：替代原稠密 `Vec<AtomicU8>` 方案——多表高位 docid（tid<<48）下稠密数组物理不可行
/// （32TB），稀疏位图内存 = O(删除集大小)。读路径 O(log n) ~22ns/op（demo 实测 13.3× 稠密
/// 但全扫瓶颈下可忽略）。
///
/// Ex-8.14：读路径无锁——ArcSwap 快照 + RoaringTreemap::contains（不可变引用）；
/// 写路径 COW——clone 快照 → insert/remove → store（引擎本就单写者，锁仅护 COW 代际健全）。
#[derive(Debug, Default)]
pub struct DeletionBitmap {
    /// 稀疏位图（RoaringTreemap = BTreeMap<u32, RoaringBitmap>，高 32 位按表分桶）。
    /// ArcSwap 无锁读快照；COW 写（clone → mutate → store）。
    inner: ArcSwap<RoaringTreemap>,
    /// 是否有未落盘的变更（mark/clear 置 true，flush 后清 false）。
    dirty: AtomicBool,
    /// 写者串行锁（mark/clear COW 代际健全）——引擎写路径本就单写者；读路径不经过此锁。
    write: Mutex<()>,
    /// 位图文件路径（`<data_dir>/deletion.bitmap`）。
    path: PathBuf,
}

impl DeletionBitmap {
    /// 打开（或创建）删除位图：文件存在则反序列化加载，否则为空位图。
    pub fn open(path: &Path) -> Result<Self> {
        let treemap = if path.exists() && std::fs::metadata(path)?.len() > 0 {
            let f = File::open(path)?;
            let mut reader = BufReader::new(f);
            RoaringTreemap::deserialize_from(&mut reader)
                .map_err(|e| crate::error::Error::Config(format!("删除位图反序列化失败: {e}")))?
        } else {
            RoaringTreemap::new()
        };
        Ok(Self {
            inner: ArcSwap::from_pointee(treemap),
            dirty: AtomicBool::new(false),
            write: Mutex::new(()),
            path: path.to_path_buf(),
        })
    }

    /// 删除：置位 + 标记脏；仅当位确实翻转才返回 true（重复删除幂等，不重计）。
    /// Ex-8.7：返回是否**新置位**——调用方据此维护删除密度计数器。
    pub fn mark_deleted(&self, docid: u64) -> bool {
        let cur = self.inner.load();
        if cur.contains(docid) {
            return false;
        }
        let _g = self.write.lock().unwrap();
        // COW：clone → insert → store
        let mut next = (**cur).clone();
        next.insert(docid);
        self.inner.store(std::sync::Arc::new(next));
        self.dirty.store(true, Ordering::Release);
        true
    }

    /// 复活（put 重新写入）：清位 + 标记脏；位本就未置时返回 false（no-op）。
    /// Ex-8.7：返回是否**实际清位**——调用方据此减除删除密度计数。
    pub fn clear(&self, docid: u64) -> bool {
        let cur = self.inner.load();
        if !cur.contains(docid) {
            return false;
        }
        let _g = self.write.lock().unwrap();
        let mut next = (**cur).clone();
        next.remove(docid);
        self.inner.store(std::sync::Arc::new(next));
        self.dirty.store(true, Ordering::Release);
        true
    }

    /// 查询（**无锁**）：已删返回 true；未删/不存在返回 false。
    /// ArcSwap 快照 + RoaringTreemap::contains（O(log n)，~22ns/op demo 实测）。
    pub fn is_deleted(&self, docid: u64) -> bool {
        self.inner.load().contains(docid)
    }

    /// 主数据键（8 字节大端 docid）的已删判定（compaction 过滤用）。
    pub fn is_deleted_key(&self, key: &[u8]) -> bool {
        if key.len() != 8 {
            return false; // 非主键键（组合索引等）不参与
        }
        self.is_deleted(u64::from_be_bytes(key[..8].try_into().unwrap()))
    }

    /// 当前已删 docid 总数（O(1)，RoaringTreemap::len）。
    pub fn deleted_count(&self) -> u64 {
        self.inner.load().len()
    }

    /// 是否有待落盘变更。
    pub fn has_pending(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// 全部变更落盘：全量序列化 → 覆写文件 → fsync。
    /// 崩溃语义：写盘中崩溃文件损坏 → WAL 回放重建（幂等）。
    pub fn flush(&self) -> Result<()> {
        if !self.dirty.load(Ordering::Acquire) {
            return Ok(());
        }
        let f = File::create(&self.path)?;
        let mut writer = BufWriter::new(f);
        let cur = self.inner.load();
        (**cur)
            .serialize_into(&mut writer)
            .map_err(|e| crate::error::Error::Config(format!("删除位图序列化失败: {e}")))?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        self.dirty.store(false, Ordering::Release);
        Ok(())
    }

    /// DROP TABLE purge：清空位图内存态并删除持久化文件（等价从未删除；重启后重建为空）。
    pub fn purge(&self) {
        let _w = self.write.lock().unwrap();
        self.inner.store(std::sync::Arc::new(RoaringTreemap::new()));
        self.dirty.store(false, Ordering::Release);
        if self.path.exists() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let bm = DeletionBitmap::open(&dir.path().join("del.bitmap")).unwrap();
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
    fn flush_and_reopen_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("del.bitmap");
        {
            let bm = DeletionBitmap::open(&path).unwrap();
            bm.mark_deleted(42);
            bm.mark_deleted(1_000_000);
            bm.flush().unwrap();
        }
        let bm = DeletionBitmap::open(&path).unwrap();
        assert!(bm.is_deleted(42));
        assert!(bm.is_deleted(1_000_000));
        assert!(!bm.is_deleted(0));
        assert!(!bm.is_deleted(43));
    }

    #[test]
    fn unflushed_marks_are_lost_on_reopen() {
        // 未 flush 的置位重开（模拟崩溃）丢失 → 由 WAL 回放重建（幂等语义）
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("del.bitmap");
        {
            let bm = DeletionBitmap::open(&path).unwrap();
            bm.mark_deleted(42);
            bm.flush().unwrap();
            bm.mark_deleted(43); // 未 flush
        }
        let bm = DeletionBitmap::open(&path).unwrap();
        assert!(bm.is_deleted(42));
        assert!(!bm.is_deleted(43));
    }

    #[test]
    fn lock_free_reads_under_writer_cow() {
        // Ex-8.14：读者（is_deleted 无锁）与写者（COW 翻转）并发——不崩溃、最终态正确
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
            bm.mark_deleted(d);
        }
        for d in (0..300_000u64).step_by(6) {
            bm.clear(d);
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
    fn redundant_ops_do_not_dirty() {
        // 重复删除 / 从未删除的 put 清位 → 不新增脏标记
        let dir = tempfile::tempdir().unwrap();
        let bm = DeletionBitmap::open(&dir.path().join("del.bitmap")).unwrap();
        bm.mark_deleted(5);
        assert!(bm.has_pending());
        bm.flush().unwrap();
        assert!(!bm.has_pending());

        // 重复删除已删位 → no-op，不脏
        assert!(!bm.mark_deleted(5));
        assert!(!bm.has_pending());

        // 从未删除的清位 → no-op，不脏
        assert!(!bm.clear(99));
        assert!(!bm.has_pending());
    }

    // ---- P2-B 多表高位 docid 测试 ----

    #[test]
    fn multi_table_high_docid_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let bm = DeletionBitmap::open(&dir.path().join("multi.bitmap")).unwrap();

        // 表 1 删 row=1..100，表 2 删 row=50..150
        let mk = |tid: u64, row: u64| (tid << 48) | row;
        for row in 1..=100u64 {
            bm.mark_deleted(mk(1, row));
        }
        for row in 50..=150u64 {
            bm.mark_deleted(mk(2, row));
        }

        // 表 1：row 1..100 已删，row 101+ 未删
        assert!(bm.is_deleted(mk(1, 1)));
        assert!(bm.is_deleted(mk(1, 100)));
        assert!(!bm.is_deleted(mk(1, 101)));

        // 表 2：row 50..150 已删，row 1..49 未删
        assert!(!bm.is_deleted(mk(2, 49)));
        assert!(bm.is_deleted(mk(2, 50)));
        assert!(bm.is_deleted(mk(2, 150)));
        assert!(!bm.is_deleted(mk(2, 151)));

        // 表 3：完全未删
        assert!(!bm.is_deleted(mk(3, 1)));

        // 交叉：只清表 1 row=50 → 表 2 row=50 仍删
        bm.clear(mk(1, 50));
        assert!(!bm.is_deleted(mk(1, 50)));
        assert!(bm.is_deleted(mk(2, 50)), "表 2 不受表 1 清位影响");
    }

    #[test]
    fn multi_table_flush_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("high.bitmap");
        {
            let bm = DeletionBitmap::open(&path).unwrap();
            let mk = |tid: u64, row: u64| (tid << 48) | row;
            bm.mark_deleted(mk(0, 42)); // 默认表
            bm.mark_deleted(mk(1, 100)); // 非默认表
            bm.mark_deleted(mk(2, 999)); // 另一非默认表
            bm.flush().unwrap();
            bm.mark_deleted(mk(1, 200)); // 未 flush（崩溃丢失）
        }
        let bm = DeletionBitmap::open(&path).unwrap();
        let mk = |tid: u64, row: u64| (tid << 48) | row;
        assert!(bm.is_deleted(mk(0, 42)), "flush 的恢复");
        assert!(bm.is_deleted(mk(1, 100)), "flush 的恢复");
        assert!(bm.is_deleted(mk(2, 999)), "flush 的恢复");
        assert!(!bm.is_deleted(mk(1, 200)), "未 flush 的丢失");
    }

    #[test]
    fn purge_clears_all() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("purge.bitmap");
        let bm = DeletionBitmap::open(&path).unwrap();
        bm.mark_deleted(1);
        bm.mark_deleted(1u64 << 48);
        bm.flush().unwrap();
        assert!(path.exists());

        bm.purge();
        assert!(!path.exists());
        assert_eq!(bm.deleted_count(), 0);
        assert!(!bm.is_deleted(1));
        assert!(!bm.is_deleted(1u64 << 48));
    }
}
