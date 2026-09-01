//! 全局 docid 分配器（分片前缀方案，AD 项：10 亿库扩展阶段 A）。
//!
//! 设计（design-10b-extension.md §5.1）：`docid = shard_id << 40 | local_id`——
//! 高 N 位 = 分片号，低 40 位 = 分片内自增。
//! - **路由 O(1)**：`shard_of(docid)` 高位直取分片（无需 hash64 重映射）；
//! - **跨分片唯一**：不同 shard_id 前缀天然不重叠（无集中分配器、无锁）；
//! - **无集中瓶颈**：每分片 `AtomicU64` 自增（gseq demo 验证 1-2 亿/s 余量充足）；
//! - **扩容归属不变**：`resize` 新增分片号段，已有 docid 高 N 位不变；
//! - **边界保护**：local_id 达 40 位上限（1 万亿/分片）时分配器拒绝（防分片号污染）。
//!
//! 兼容：现有 `hash64(docid)` 路由保留（旧写路径）；新写路径（分片构建/批量导入）用前缀。

use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};

/// 分片内 local_id 位宽（低 40 位 = 每分片 1 万亿 docid）。
pub const LOCAL_BITS: u32 = 40;
/// local_id 掩码。
pub const LOCAL_MASK: u64 = (1u64 << LOCAL_BITS) - 1;
/// local_id 容量上限（1<<40）。
pub const LOCAL_CAPACITY: u64 = 1u64 << LOCAL_BITS;

/// 编码：`docid = shard_id << 40 | local_id`。
#[inline]
pub fn encode(shard_id: u16, local_id: u64) -> u64 {
    (shard_id as u64) << LOCAL_BITS | local_id
}

/// 路由 O(1)：解析 docid 所属分片（高 N 位直取，非 hash）。
#[inline]
pub fn shard_of(docid: u64) -> u16 {
    (docid >> LOCAL_BITS) as u16
}

/// 分片内 local_id。
#[inline]
pub fn local_of(docid: u64) -> u64 {
    docid & LOCAL_MASK
}

/// 分片内 docid 分配器（AtomicU64 无锁自增；溢出保护防分片号污染）。
#[derive(Debug, Default)]
pub struct ShardLocalAllocator {
    next: AtomicU64,
}

impl ShardLocalAllocator {
    pub fn new() -> Self {
        Self::default()
    }

    /// 以指定起点创建（恢复崩溃续跑：从持久化 watermark 继续）。
    pub fn with_start(next: u64) -> Result<Self> {
        if next >= LOCAL_CAPACITY {
            return Err(Error::Unsupported(
                format!("分片内 local_id 分配超限（next={next} >= 1<<40，需扩展 LOCAL_BITS）").into(),
            ));
        }
        Ok(Self {
            next: AtomicU64::new(next),
        })
    }

    /// 分配下一个 docid（分片内唯一）。溢出（达到 1<<40）时拒绝。
    pub fn alloc(&self, shard_id: u16) -> Result<u64> {
        let local = self.next.fetch_add(1, Ordering::Relaxed);
        if local >= LOCAL_CAPACITY {
            return Err(Error::Unsupported(
                format!("分片 {shard_id} local_id 分配超限（{local} >= 1<<40）").into(),
            ));
        }
        Ok(encode(shard_id, local))
    }

    /// 当前水位（已分配数；崩溃恢复续跑用）。
    pub fn watermark(&self) -> u64 {
        self.next.load(Ordering::Relaxed)
    }
}

/// 全局 docid 分配器：每分片一个 `ShardLocalAllocator`（无集中瓶颈）。
#[derive(Debug)]
pub struct DocIdAllocator {
    shards: Vec<ShardLocalAllocator>,
}

impl DocIdAllocator {
    /// 创建 n 分片（1..=65535，u16 前缀；10 亿库建议 10 分片）。
    pub fn new(n_shards: u16) -> Result<Self> {
        if n_shards == 0 {
            return Err(Error::Config("分片数必须 ≥1".into()));
        }
        Ok(Self {
            shards: (0..n_shards).map(|_| ShardLocalAllocator::new()).collect(),
        })
    }

    /// 从持久化水位恢复（崩溃续跑；next 数组与分片数一致）。
    pub fn from_watermarks(shards: &[u64]) -> Result<Self> {
        let mut alloc = Self::new(shards.len() as u16)?;
        for (i, &w) in shards.iter().enumerate() {
            alloc.shards[i] = ShardLocalAllocator::with_start(w)?;
        }
        Ok(alloc)
    }

    /// 分配 docid（归属 shard_id；跨分片天然唯一）。
    pub fn alloc(&self, shard_id: u16) -> Result<u64> {
        let idx = shard_id as usize;
        if idx >= self.shards.len() {
            return Err(Error::Unsupported(
                format!("分片 {shard_id} 超出当前分片数 {}", self.shards.len()).into(),
            ));
        }
        self.shards[idx].alloc(shard_id)
    }

    /// 扩容：新增分片号段（已有 docid 高 N 位不变 → 归属不变）。
    pub fn resize(&mut self, n_shards: u16) {
        while (self.shards.len() as u16) < n_shards {
            self.shards.push(ShardLocalAllocator::new());
        }
    }

    pub fn shard_count(&self) -> u16 {
        self.shards.len() as u16
    }

    /// 各分片水位（持久化：崩溃恢复 `from_watermarks` 续跑）。
    pub fn watermarks(&self) -> Vec<u64> {
        self.shards.iter().map(|s| s.watermark()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        for (sid, lid) in [(0u16, 0u64), (0, 5), (3, 1_000_000), (1023, LOCAL_MASK)] {
            let d = encode(sid, lid);
            assert_eq!(shard_of(d), sid);
            assert_eq!(local_of(d), lid);
        }
    }

    #[test]
    fn cross_shard_unique_no_overlap() {
        // 不同 shard 的 docid 区间不重叠（天然唯一）
        assert!(encode(0, LOCAL_MASK) < encode(1, 0));
        assert!(encode(5, LOCAL_MASK) < encode(6, 0));
    }

    #[test]
    fn concurrent_alloc_unique() {
        // 并发分配唯一性：8 线程 × 50k/分片（无锁 AtomicU64）
        let a = std::sync::Arc::new(DocIdAllocator::new(4).unwrap());
        let mut hs = Vec::new();
        for sid in 0..4u16 {
            for _ in 0..8 {
                let a = a.clone();
                hs.push(std::thread::spawn(move || {
                    for _ in 0..50_000u64 {
                        a.alloc(sid).unwrap();
                    }
                }));
            }
        }
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(a.watermarks()[0], 50_000 * 8);
        assert_eq!(a.watermarks()[3], 50_000 * 8);
    }

    #[test]
    fn alloc_out_of_range_shard_rejected() {
        let a = DocIdAllocator::new(4).unwrap();
        assert!(a.alloc(4).is_err(), "超出分片数拒绝");
        assert!(a.alloc(65535).is_err());
    }

    #[test]
    fn resize_keeps_ownership_and_adds_shards() {
        let mut a = DocIdAllocator::new(4).unwrap();
        let existing: Vec<u64> = (0..4).map(|s| a.alloc(s).unwrap()).collect();
        a.resize(10);
        assert_eq!(a.shard_count(), 10);
        for (i, d) in existing.iter().enumerate() {
            assert_eq!(shard_of(*d), i as u16, "扩容后归属不变");
        }
        for sid in 4..10u16 {
            assert_eq!(shard_of(a.alloc(sid).unwrap()), sid);
        }
    }

    #[test]
    fn watermark_restart_resumes_without_duplicate() {
        // 崩溃恢复：水位持久化 → from_watermarks 续跑不重复
        let a = DocIdAllocator::new(2).unwrap();
        for _ in 0..10 {
            a.alloc(0).unwrap();
        }
        let w = a.watermarks();
        let b = DocIdAllocator::from_watermarks(&w).unwrap();
        // 续跑分配不重复（水位 10 起）
        let d = b.alloc(0).unwrap();
        assert_eq!(local_of(d), 10, "从水位续跑，不重复");
        assert_eq!(shard_of(d), 0);
    }

    #[test]
    fn local_overflow_rejected() {
        // 边界：local_id 达 1<<40 → 拒绝（防分片号污染）
        let a = ShardLocalAllocator::with_start(LOCAL_CAPACITY - 1).unwrap();
        assert_eq!(local_of(a.alloc(0).unwrap()), LOCAL_CAPACITY - 1, "最大 local 可用");
        assert!(ShardLocalAllocator::with_start(LOCAL_CAPACITY).is_err(), "超限创建拒绝");
    }
}
