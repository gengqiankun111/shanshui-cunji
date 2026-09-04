//! MemTable：内存跳表 + 双缓冲切换（design 4.2 / development 步骤 5）。
//!
//! - 写入路径：WAL → MemTable（跳表有序，范围查询友好）；
//! - 双 MemTable 切换：Mutable 接收新写入，Immutable 冻结待刷盘，刷盘不阻塞写入；
//! - `value: Option<Vec<u8>>`：`None` 表示 Tombstone 删除标记（为步骤 9 预留）；
//! - S 项（严格 MVCC）：每 key 保留**多版本**（seq 升序版本链）——RR 快照读在
//!   事务活跃期 + 未刷盘时也能读到快照点版本（旧实现仅保最新，快照语义不严格）。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;

use crossbeam_skiplist::SkipMap;

use crate::error::Result;

/// 单条内存记录：`None` = Tombstone（删除标记）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemTableEntry {
    /// WAL 单调序号，保证崩溃回放后与磁盘序列一致。
    pub seq: u64,
    /// 值；`None` 表示删除。
    pub value: Option<Vec<u8>>,
}

/// 单 key 多版本（S 项）：`versions` 按 seq 升序，`last()` = 最新版本。
/// 非快照读取 last；快照读（`get_at`）取最大 seq ≤ snapshot 的版本。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemTableVersions {
    pub versions: Vec<MemTableEntry>,
}

/// MemTable：并发跳表 + 近似字节计数。
///
/// 字节计数用于触发冻结切换（`memtable.max_size_mb`，design 14.1.1）；
/// 计数为近似值（key + value 原始长度，不含跳表节点与对齐开销）。
pub struct MemTable {
    inner: SkipMap<Vec<u8>, MemTableVersions>,
    approx_bytes: AtomicUsize,
    len: AtomicUsize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            inner: SkipMap::new(),
            approx_bytes: AtomicUsize::new(0),
            len: AtomicUsize::new(0),
        }
    }

    /// 写入（追加版本，保留旧版本供快照读）。
    pub fn put(&self, key: Vec<u8>, seq: u64, value: Vec<u8>) {
        let vlen = value.len();
        match self.inner.get(&key) {
            Some(prev) => {
                let mut versions = prev.value().clone();
                versions
                    .versions
                    .push(MemTableEntry { seq, value: Some(value) });
                self.approx_bytes.fetch_add(vlen, Ordering::Relaxed);
                self.inner.insert(key, versions);
            }
            None => {
                let versions = MemTableVersions {
                    versions: vec![MemTableEntry { seq, value: Some(value) }],
                };
                self.approx_bytes.fetch_add(vlen, Ordering::Relaxed);
                self.len.fetch_add(1, Ordering::Relaxed);
                self.inner.insert(key, versions);
            }
        }
    }

    /// 删除标记（Tombstone，追加为最新版本；旧版本仍供快照读）。
    pub fn delete(&self, key: Vec<u8>, seq: u64) {
        match self.inner.get(&key) {
            Some(prev) => {
                let mut versions = prev.value().clone();
                versions.versions.push(MemTableEntry { seq, value: None });
                self.inner.insert(key, versions);
            }
            None => {
                self.len.fetch_add(1, Ordering::Relaxed);
                let versions = MemTableVersions {
                    versions: vec![MemTableEntry { seq, value: None }],
                };
                self.inner.insert(key, versions);
            }
        }
    }

    /// 非快照读：返回最新版本（Tombstone → value=None）。
    pub fn get(&self, key: &[u8]) -> Option<MemTableEntry> {
        self.inner
            .get(key)
            .and_then(|e| e.value().versions.last().cloned())
    }

    /// 快照读（S 项）：返回 **seq ≤ snapshot_seq** 的最新版本；
    /// 该版本为 Tombstone → value=None（快照点已删除）；无 ≤ 快照版本 → None。
    pub fn get_at(&self, key: &[u8], snapshot_seq: u64) -> Option<MemTableEntry> {
        self.inner.get(key).and_then(|e| {
            e.value()
                .versions
                .iter()
                .rev()
                .find(|v| v.seq <= snapshot_seq)
                .cloned()
        })
    }

    /// 是否存在（含 Tombstone 条目）。
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.inner.contains_key(key)
    }

    /// 近似内存占用（字节）。
    pub fn approx_bytes(&self) -> usize {
        self.approx_bytes.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 有序扫描（供刷盘 / 范围查询）：闭包按 key 升序收到**每条版本** `(key, entry)`
    /// （同 key 多条版本 = 同一 key 多次收到；seq 升序）。调用方按 seq 去重/取版本。
    pub fn scan<F: FnMut(&[u8], &MemTableEntry)>(&self, mut f: F) {
        for e in self.inner.iter() {
            for v in &e.value().versions {
                f(e.key(), v);
            }
        }
    }

    /// 范围扫描：[start, end]（两端包含）。start/end 传 None 表示无边界。
    /// 同 `scan`：每条版本回调一次（同 key 多版本重复回调，seq 升序）。
    pub fn scan_range<F: FnMut(&[u8], &MemTableEntry)>(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        mut f: F,
    ) {
        use std::ops::Bound;
        let mut iter = match start {
            Some(s) => self
                .inner
                .range::<[u8], _>((Bound::Included(s), Bound::Unbounded)),
            None => self
                .inner
                .range::<[u8], _>((Bound::Unbounded, Bound::Unbounded)),
        };
        if let Some(e) = end {
            for entry in iter.by_ref() {
                if entry.key().as_slice() > e {
                    break;
                }
                for v in &entry.value().versions {
                    f(entry.key(), v);
                }
            }
        } else {
            for entry in iter {
                for v in &entry.value().versions {
                    f(entry.key(), v);
                }
            }
        }
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

/// MemTable 范围扫描**流式迭代器**（M8-P10 scan 流式化）：skiplist range 惰性迭代，
/// 逐条 yield `(key, value, seq)`（value=None = Tombstone），内存 O(1)。
pub struct MemRangeIter<'a> {
    iter: Box<dyn Iterator<Item = Result<(Vec<u8>, Option<Vec<u8>>, u64)>> + 'a>,
    end: Option<Vec<u8>>,
}

impl<'a> MemRangeIter<'a> {
    pub fn new(mem: &'a MemTable, start: Option<&[u8]>, end: Option<&[u8]>) -> Self {
        use std::ops::Bound;
        // owned Vec 作为 range 查询键：Range 持有 owned 值，迭代器生命周期只绑 mem（'a）
        let range: (Bound<Vec<u8>>, Bound<Vec<u8>>) = match start {
            Some(s) => (Bound::Included(s.to_vec()), Bound::Unbounded),
            None => (Bound::Unbounded, Bound::Unbounded),
        };
        // S 项：每条版本一条 (key, value, seq)（同 key 多版本重复产出，seq 升序）
        let map_rows =
            |e: crossbeam_skiplist::map::Entry<'a, Vec<u8>, MemTableVersions>| {
                let key = e.key().clone();
                let versions = e.value().versions.clone();
                versions
                    .into_iter()
                    .map(move |v| Ok((key.clone(), v.value, v.seq)))
            };
        let iter: Box<dyn Iterator<Item = Result<(Vec<u8>, Option<Vec<u8>>, u64)>> + 'a> =
            Box::new(mem.inner.range::<Vec<u8>, _>(range).flat_map(map_rows));
        Self {
            iter,
            end: end.map(|e| e.to_vec()),
        }
    }
}

impl<'a> Iterator for MemRangeIter<'a> {
    type Item = Result<(Vec<u8>, Option<Vec<u8>>, u64)>;
    fn next(&mut self) -> Option<Self::Item> {
        let row = self.iter.next()?;
        let (k, v, seq) = match row {
            Ok(r) => r,
            Err(e) => return Some(Err(e)),
        };
        if let Some(en) = &self.end {
            if k.as_slice() > en.as_slice() {
                return None; // 升序，后续更大
            }
        }
        Some(Ok((k, v, seq)))
    }
}

/// 双缓冲：Mutable 接收写入，Immutable 冻结待刷盘。
/// P72（无锁合并）：内部 `RwLock<BufferInner>`——`switch_and_flush` `&self` 化（Engine 字段
/// `Arc<ColumnFamily>` 化后 flush 无法取 `&mut`）的支撑：冻结切换经**写锁**，读/写经**读锁**
/// （SkipMap 本身并发安全，锁仅护"双缓冲结构不变性"）。put/delete 只取读锁（写路径在 Engine
/// 写锁内与 switch 互斥，双保险）；scan 期间持有读锁与 switch（Engine 写锁）天然互斥。
pub struct MemTableBuffer {
    inner: RwLock<BufferInner>,
}

/// 双缓冲内部态：mutable 接收写入，immutable 冻结待刷盘。
struct BufferInner {
    mutable: MemTable,
    immutable: Option<MemTable>,
}

impl MemTableBuffer {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(BufferInner {
                mutable: MemTable::new(),
                immutable: None,
            }),
        }
    }

    /// 写入当前 Mutable 表。
    pub fn put(&self, key: Vec<u8>, seq: u64, value: Vec<u8>) {
        self.inner.read().unwrap().mutable.put(key, seq, value);
    }

    pub fn delete(&self, key: Vec<u8>, seq: u64) {
        self.inner.read().unwrap().mutable.delete(key, seq);
    }

    /// 读路径：先查 Mutable，再查 Immutable（Immutable 冻结期间仍可读，保证内存一致性）。
    pub fn get(&self, key: &[u8]) -> Option<MemTableEntry> {
        let g = self.inner.read().unwrap();
        g.mutable
            .get(key)
            .or_else(|| g.immutable.as_ref().and_then(|m| m.get(key)))
    }

    /// 快照读（S 项）：Mutable/Immutable 各取 **seq ≤ snapshot** 的最新版本，
    /// 再取两者中 seq 更大者（跨表版本合并，语义与 `get` 一致 + 快照过滤）。
    pub fn get_at(&self, key: &[u8], snapshot_seq: u64) -> Option<MemTableEntry> {
        let g = self.inner.read().unwrap();
        let mut best: Option<MemTableEntry> = g.mutable.get_at(key, snapshot_seq);
        if let Some(imm) = &g.immutable {
            if let Some(e) = imm.get_at(key, snapshot_seq) {
                if best.as_ref().map_or(true, |b| e.seq > b.seq) {
                    best = Some(e);
                }
            }
        }
        best
    }

    /// 冻结切换：当前 Mutable 变为 Immutable，新建空 Mutable 承接写入。
    /// 前提：上一轮 Immutable 已被取走（刷盘完成），否则 debug 断言失败。
    /// P72：`&self`（写锁内交换，Engine 写锁下与写路径互斥）。
    pub fn switch(&self) {
        let mut g = self.inner.write().unwrap();
        debug_assert!(g.immutable.is_none(), "上一轮 Immutable 尚未刷盘完成");
        g.immutable = Some(std::mem::take(&mut g.mutable));
    }

    /// 取走 Immutable（刷盘完成后调用），释放内存。
    pub fn take_immutable(&self) -> Option<MemTable> {
        self.inner.write().unwrap().immutable.take()
    }

    /// 清空双缓冲全部键（DROP TABLE purge）：Mutable/Immutable 直接丢弃、不刷盘——
    /// WAL 已由上层截断，数据语义为整表删除。
    pub fn reset(&self) {
        let mut g = self.inner.write().unwrap();
        g.mutable = MemTable::new();
        g.immutable = None;
    }

    /// 双缓冲范围流式迭代（M8-P10）：immutable + mutable 各一个 `MemRangeIter`
    /// （同 key 以 seq 去重由 k-way merge 调用方负责）。
    /// P72：迭代器借用 RwLock 读锁内数据——改为 HRTB 闭包式（`f` 在锁作用域内消费迭代器，
    /// 调用方把 k-way merge 主体放入闭包），避免返回锁外悬垂借用。
    pub fn with_iter_range<R>(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        f: impl for<'a> FnOnce(Vec<MemRangeIter<'a>>) -> R,
    ) -> R {
        let g = self.inner.read().unwrap();
        let mut out = Vec::new();
        if let Some(imm) = &g.immutable {
            out.push(MemRangeIter::new(imm, start, end));
        }
        out.push(MemRangeIter::new(&g.mutable, start, end));
        f(out)
    }

    /// 范围扫描：遍历 Immutable 与 Mutable 两表（同 key 以 seq 去重由调用方负责）。
    pub fn scan_range<F: FnMut(&[u8], &MemTableEntry)>(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        mut f: F,
    ) {
        let g = self.inner.read().unwrap();
        if let Some(imm) = &g.immutable {
            imm.scan_range(start, end, &mut f);
        }
        g.mutable.scan_range(start, end, &mut f);
    }

    pub fn mutable_bytes(&self) -> usize {
        self.inner.read().unwrap().mutable.approx_bytes()
    }

    pub fn immutable_bytes(&self) -> usize {
        let g = self.inner.read().unwrap();
        g.immutable.as_ref().map_or(0, |m| m.approx_bytes())
    }
}

impl Default for MemTableBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_roundtrip() {
        let m = MemTable::new();
        m.put(b"a".to_vec(), 1, b"v1".to_vec());
        m.put(b"b".to_vec(), 2, b"v2".to_vec());
        assert_eq!(m.get(b"a").unwrap().value.unwrap(), b"v1");
        assert_eq!(m.get(b"b").unwrap().value.unwrap(), b"v2");
        assert_eq!(m.len(), 2);
        assert!(m.approx_bytes() >= 4);
    }

    #[test]
    fn overwrite_updates_value_and_seq() {
        let m = MemTable::new();
        m.put(b"k".to_vec(), 1, b"old".to_vec());
        m.put(b"k".to_vec(), 2, b"new".to_vec());
        assert_eq!(m.len(), 1); // 覆盖不增加条目
        let e = m.get(b"k").unwrap();
        assert_eq!(e.seq, 2);
        assert_eq!(e.value.unwrap(), b"new");
    }

    #[test]
    fn delete_marks_tombstone() {
        let m = MemTable::new();
        m.put(b"k".to_vec(), 1, b"v".to_vec());
        m.delete(b"k".to_vec(), 2);
        assert!(m.get(b"k").unwrap().value.is_none());
        assert!(m.contains_key(b"k"));
    }

    // ---------- S 项：多版本 / 快照读 ----------

    #[test]
    fn get_at_reads_snapshot_version_in_memtable() {
        let m = MemTable::new();
        m.put(b"k".to_vec(), 5, b"v1".to_vec());
        m.put(b"k".to_vec(), 20, b"v2".to_vec());
        // 快照落在 5..20 → 应读到 v1（旧实现仅保最新版本，会漏掉）
        assert_eq!(m.get_at(b"k", 5).unwrap().value.unwrap(), b"v1");
        assert_eq!(m.get_at(b"k", 10).unwrap().value.unwrap(), b"v1");
        assert_eq!(m.get_at(b"k", 20).unwrap().value.unwrap(), b"v2");
        assert!(m.get_at(b"k", 4).is_none(), "快照早于首个版本 → 不可见");
        assert_eq!(m.get(b"k").unwrap().seq, 20, "非快照读取最新版本");
    }

    #[test]
    fn delete_keeps_history_for_snapshot() {
        let m = MemTable::new();
        m.put(b"k".to_vec(), 5, b"v".to_vec());
        m.delete(b"k".to_vec(), 20);
        assert!(m.get(b"k").unwrap().value.is_none(), "最新版本为删除");
        assert_eq!(
            m.get_at(b"k", 10).unwrap().value.unwrap(),
            b"v",
            "快照点前仍可见旧值"
        );
        assert!(m.get_at(b"k", 20).unwrap().value.is_none(), "快照点后为删除");
    }

    #[test]
    fn scan_yields_all_versions() {
        let m = MemTable::new();
        m.put(b"k".to_vec(), 1, b"a".to_vec());
        m.put(b"k".to_vec(), 3, b"b".to_vec());
        m.put(b"j".to_vec(), 2, b"x".to_vec());
        let mut rows: Vec<(Vec<u8>, u64, Option<Vec<u8>>)> = Vec::new();
        m.scan(|k, e| rows.push((k.to_vec(), e.seq, e.value.clone())));
        // key 升序；同 key 版本 seq 升序
        assert_eq!(
            rows,
            vec![
                (b"j".to_vec(), 2, Some(b"x".to_vec())),
                (b"k".to_vec(), 1, Some(b"a".to_vec())),
                (b"k".to_vec(), 3, Some(b"b".to_vec())),
            ]
        );
    }

    #[test]
    fn scan_is_sorted_ascending() {
        let m = MemTable::new();
        m.put(b"c".to_vec(), 1, b"3".to_vec());
        m.put(b"a".to_vec(), 2, b"1".to_vec());
        m.put(b"b".to_vec(), 3, b"2".to_vec());
        let mut keys = Vec::new();
        m.scan(|k, _| keys.push(k.to_vec()));
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn scan_range_respects_bounds() {
        let m = MemTable::new();
        for k in [b"a", b"b", b"c", b"d", b"e"] {
            m.put(k.to_vec(), 1, b"v".to_vec());
        }
        let mut keys = Vec::new();
        m.scan_range(Some(b"b"), Some(b"d"), |k, _| keys.push(k.to_vec()));
        assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
    }

    #[test]
    fn buffer_switch_freezes_and_writes_continue() {
        let buf = MemTableBuffer::new();
        buf.put(b"x".to_vec(), 1, b"1".to_vec());
        buf.switch(); // Mutable 冻结，新写入进新表

        assert!(buf.immutable_bytes() >= 1, "Immutable 冻结了 x");
        buf.put(b"y".to_vec(), 2, b"2".to_vec());
        // 新写入只进 Mutable，不可见 frozen
        assert_eq!(buf.get(b"x").unwrap().value.unwrap(), b"1"); // Immutable 仍可读
        assert_eq!(buf.get(b"y").unwrap().value.unwrap(), b"2");
        assert_eq!(buf.mutable_bytes(), 1); // 新表仅 y
        assert!(buf.immutable_bytes() >= 1);

        let taken = buf.take_immutable().unwrap();
        assert_eq!(taken.len(), 1);
        assert!(buf.take_immutable().is_none());
    }

    #[test]
    fn switch_preserves_data_for_flush() {
        // 模拟刷盘流程：写入 → switch → 取走 Immutable 刷盘 → 数据完整
        let buf = MemTableBuffer::new();
        for i in 0..100u64 {
            buf.put(format!("k{i}").as_bytes().to_vec(), i, b"v".to_vec());
        }
        buf.switch();
        let imm = buf.take_immutable().unwrap();
        let mut count = 0;
        imm.scan(|_, _| count += 1);
        assert_eq!(count, 100);
    }

    proptest::proptest! {
        #[test]
        fn prop_get_after_put(key in proptest::collection::vec(proptest::arbitrary::any::<u8>(), 0..32)) {
            let m = MemTable::new();
            m.put(key.clone(), 1, b"v".to_vec());
            assert_eq!(m.get(&key).unwrap().value.unwrap(), b"v");
        }
    }
}
