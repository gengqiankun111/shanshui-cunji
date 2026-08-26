//! MemTable：内存跳表 + 双缓冲切换（design 4.2 / development 步骤 5）。
//!
//! - 写入路径：WAL → MemTable（跳表有序，范围查询友好）；
//! - 双 MemTable 切换：Mutable 接收新写入，Immutable 冻结待刷盘，刷盘不阻塞写入；
//! - `value: Option<Vec<u8>>`：`None` 表示 Tombstone 删除标记（为步骤 9 预留）。

use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_skiplist::SkipMap;

/// 单条内存记录：`None` = Tombstone（删除标记）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemTableEntry {
    /// WAL 单调序号，保证崩溃回放后与磁盘序列一致。
    pub seq: u64,
    /// 值；`None` 表示删除。
    pub value: Option<Vec<u8>>,
}

/// MemTable：并发跳表 + 近似字节计数。
///
/// 字节计数用于触发冻结切换（`memtable.max_size_mb`，design 14.1.1）；
/// 计数为近似值（key + value 原始长度，不含跳表节点与对齐开销）。
pub struct MemTable {
    inner: SkipMap<Vec<u8>, MemTableEntry>,
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

    /// 写入（覆盖语义）。返回被覆盖的旧值（若有），供调用方做差值计数修正。
    pub fn put(&self, key: Vec<u8>, seq: u64, value: Vec<u8>) {
        let entry = MemTableEntry { seq, value: Some(value) };
        if let Some(prev) = self.inner.get(&key) {
            self.approx_bytes.fetch_sub(prev.value().value.as_ref().map_or(0, |v| v.len()), Ordering::Relaxed);
        } else {
            self.len.fetch_add(1, Ordering::Relaxed);
        }
        self.approx_bytes.fetch_add(entry.value.as_ref().map_or(0, |v| v.len()), Ordering::Relaxed);
        self.inner.insert(key, entry);
    }

    /// 删除标记（Tombstone）。
    pub fn delete(&self, key: Vec<u8>, seq: u64) {
        if self.inner.get(&key).is_some() {
            let entry = MemTableEntry { seq, value: None };
            // 保留已存在的 value 长度修正
            self.inner.insert(key, entry);
        } else {
            self.len.fetch_add(1, Ordering::Relaxed);
            self.inner.insert(key, MemTableEntry { seq, value: None });
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<MemTableEntry> {
        self.inner.get(key).map(|e| e.value().clone())
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

    /// 有序扫描（供刷盘 / 范围查询）：闭包按 key 升序收到 (key, entry)。
    pub fn scan<F: FnMut(&[u8], &MemTableEntry)>(&self, mut f: F) {
        for e in self.inner.iter() {
            f(e.key(), e.value());
        }
    }

    /// 范围扫描：[start, end]（两端包含）。start/end 传 None 表示无边界。
    pub fn scan_range<F: FnMut(&[u8], &MemTableEntry)>(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        mut f: F,
    ) {
        use std::ops::Bound;
        let mut iter = match start {
            Some(s) => self.inner.range::<[u8], _>((Bound::Included(s), Bound::Unbounded)),
            None => self.inner.range::<[u8], _>((Bound::Unbounded, Bound::Unbounded)),
        };
        if let Some(e) = end {
            while let Some(entry) = iter.next() {
                if entry.key().as_slice() > e {
                    break;
                }
                f(entry.key(), entry.value());
            }
        } else {
            while let Some(entry) = iter.next() {
                f(entry.key(), entry.value());
            }
        }
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

/// 双缓冲：Mutable 接收写入，Immutable 冻结待刷盘。
pub struct MemTableBuffer {
    mutable: MemTable,
    immutable: Option<MemTable>,
}

impl MemTableBuffer {
    pub fn new() -> Self {
        Self { mutable: MemTable::new(), immutable: None }
    }

    /// 写入当前 Mutable 表。
    pub fn put(&self, key: Vec<u8>, seq: u64, value: Vec<u8>) {
        self.mutable.put(key, seq, value);
    }

    pub fn delete(&self, key: Vec<u8>, seq: u64) {
        self.mutable.delete(key, seq);
    }

    /// 读路径：先查 Mutable，再查 Immutable（Immutable 冻结期间仍可读，保证内存一致性）。
    pub fn get(&self, key: &[u8]) -> Option<MemTableEntry> {
        self.mutable.get(key).or_else(|| self.immutable.as_ref().and_then(|m| m.get(key)))
    }

    /// 冻结切换：当前 Mutable 变为 Immutable，新建空 Mutable 承接写入。
    /// 前提：上一轮 Immutable 已被取走（刷盘完成），否则 debug 断言失败。
    pub fn switch(&mut self) {
        debug_assert!(self.immutable.is_none(), "上一轮 Immutable 尚未刷盘完成");
        self.immutable = Some(std::mem::replace(&mut self.mutable, MemTable::new()));
    }

    /// 取走 Immutable（刷盘完成后调用），释放内存。
    pub fn take_immutable(&mut self) -> Option<MemTable> {
        self.immutable.take()
    }

    /// 当前 Immutable（冻结中待刷盘）引用；无则返回 None。
    pub fn immutable(&self) -> Option<&MemTable> {
        self.immutable.as_ref()
    }

    /// 范围扫描：遍历 Immutable 与 Mutable 两表（同 key 以 seq 去重由调用方负责）。
    pub fn scan_range<F: FnMut(&[u8], &MemTableEntry)>(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        mut f: F,
    ) {
        if let Some(imm) = &self.immutable {
            imm.scan_range(start, end, &mut f);
        }
        self.mutable.scan_range(start, end, &mut f);
    }

    pub fn mutable_bytes(&self) -> usize {
        self.mutable.approx_bytes()
    }

    pub fn immutable_bytes(&self) -> usize {
        self.immutable.as_ref().map_or(0, |m| m.approx_bytes())
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
        let mut buf = MemTableBuffer::new();
        buf.put(b"x".to_vec(), 1, b"1".to_vec());
        buf.switch(); // Mutable 冻结，新写入进新表

        assert_eq!(buf.immutable().unwrap().len(), 1);
        buf.put(b"y".to_vec(), 2, b"2".to_vec());
        // 新写入只进 Mutable，不可见 frozen
        assert_eq!(buf.get(b"x").unwrap().value.unwrap(), b"1"); // Immutable 仍可读
        assert_eq!(buf.get(b"y").unwrap().value.unwrap(), b"2");
        assert_eq!(buf.mutable_bytes() as usize, 1); // 新表仅 y
        assert!(buf.immutable_bytes() >= 1);

        let taken = buf.take_immutable().unwrap();
        assert_eq!(taken.len(), 1);
        assert!(buf.take_immutable().is_none());
    }

    #[test]
    fn switch_preserves_data_for_flush() {
        // 模拟刷盘流程：写入 → switch → 取走 Immutable 刷盘 → 数据完整
        let mut buf = MemTableBuffer::new();
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
