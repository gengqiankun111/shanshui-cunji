//! 分片构建工具（10 亿库扩展阶段 A，design-10b-extension.md §5.3 / §7.2）。
//!
//! 把一个大源文件构建为 N 个分片数据目录（每分片独立 Engine）：
//! - **行分配**：无显式主键时 `row_idx % n_shards` 均匀分布（各分片行数差 ≤1）；
//! - **docid 前缀**：`docid = shard_id<<40 | local_id`（docid_alloc），分片内 AtomicU64 自增，
//!   跨分片天然唯一、无集中分配器；
//! - **显式 docid 路由**：源含 docid/id 列时 `shard_of(docid)` 高 N 位直取分片（O(1)）；
//! - **崩溃续跑**：每分片 watermark 持久化 → `with_watermarks` 续跑不重复；
//! - **扩容归属不变**：`resize` 只新增分片号段。

use crate::docid_alloc::{DocIdAllocator, LOCAL_MASK};
use crate::error::{Error, Result};

/// 分片构建规划器：行分配 + 分片前缀 docid 分配（纯逻辑，不碰 IO，可单测）。
#[derive(Debug)]
pub struct ShardBuildPlanner {
    n_shards: u16,
    allocator: DocIdAllocator,
}

impl ShardBuildPlanner {
    /// 创建 n 分片构建规划器（1..=65535；10 亿库建议 10 分片）。
    pub fn new(n_shards: u16) -> Result<Self> {
        Ok(Self {
            n_shards,
            allocator: DocIdAllocator::new(n_shards)?,
        })
    }

    /// 从持久化水位恢复（崩溃续跑；watermarks 长度必须 = 分片数）。
    pub fn with_watermarks(watermarks: &[u64]) -> Result<Self> {
        let n_shards = watermarks.len() as u16;
        if n_shards == 0 {
            return Err(Error::Config("分片水位为空".into()));
        }
        Ok(Self {
            n_shards,
            allocator: DocIdAllocator::from_watermarks(watermarks)?,
        })
    }

    /// 分片数。
    pub fn shard_count(&self) -> u16 {
        self.n_shards
    }

    /// 行 → 分片（无显式主键时取模均匀分布；各分片行数差 ≤1）。
    #[inline]
    pub fn row_to_shard(&self, row_idx: u64) -> u16 {
        (row_idx % self.n_shards as u64) as u16
    }

    /// 显式 docid 路由：高 N 位直取分片（O(1)，非 hash 重映射）。
    #[inline]
    pub fn route(&self, docid: u64) -> u16 {
        crate::docid_alloc::shard_of(docid)
    }

    /// 按行号分配 docid（组合：行号路由 + 分片内自增）。
    pub fn alloc(&self, row_idx: u64) -> Result<u64> {
        let sid = self.row_to_shard(row_idx);
        self.alloc_on(sid)
    }

    /// 在指定分片分配 docid（分片内唯一）。
    pub fn alloc_on(&self, shard_id: u16) -> Result<u64> {
        self.allocator.alloc(shard_id)
    }

    /// 各分片水位（持久化：崩溃恢复 `with_watermarks` 续跑）。
    pub fn watermarks(&self) -> Vec<u64> {
        self.allocator.watermarks()
    }

    /// 扩容：新增分片号段（已有 docid 高 N 位不变 → 归属不变）。
    pub fn resize(&mut self, n_shards: u16) {
        if n_shards > self.n_shards {
            self.allocator.resize(n_shards);
            self.n_shards = n_shards;
        }
    }

    /// 校验显式 docid 归属分片 = 期望分片（不一致 = 数据路由错误，构建期拒绝）。
    pub fn validate_routed(docid: u64, expected_shard: u16) -> Result<()> {
        let actual = crate::docid_alloc::shard_of(docid);
        if actual != expected_shard {
            return Err(Error::Cluster(format!(
                "docid {docid} 路由分片 {actual} ≠ 期望 {expected_shard}"
            )));
        }
        Ok(())
    }

    /// 分片内 local_id 上限（1<<40 - 1；构建前可提前预警）。
    pub const fn local_capacity() -> u64 {
        LOCAL_MASK
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_distribution_even() {
        let p = ShardBuildPlanner::new(4).unwrap();
        let mut counts = [0u64; 4];
        for row in 0..200 {
            let sid = p.row_to_shard(row);
            counts[sid as usize] += 1;
        }
        assert_eq!(counts, [50, 50, 50, 50], "行均匀分布");
    }

    #[test]
    fn alloc_prefix_and_uniqueness() {
        let p = ShardBuildPlanner::new(10).unwrap();
        let mut seen = std::collections::HashSet::new();
        for row in 0..10_000u64 {
            let d = p.alloc(row).unwrap();
            assert_eq!(crate::docid_alloc::shard_of(d), p.row_to_shard(row));
            assert!(seen.insert(d), "跨分片 docid 重复");
        }
        assert_eq!(seen.len(), 10_000);
    }

    #[test]
    fn explicit_docid_route_ok() {
        let p = ShardBuildPlanner::new(4).unwrap();
        for sid in 0..4u16 {
            let d = crate::docid_alloc::encode(sid, 7);
            assert_eq!(p.route(d), sid);
            ShardBuildPlanner::validate_routed(d, sid).unwrap();
        }
        // 错误期望分片被拒绝
        let d = crate::docid_alloc::encode(3, 7);
        assert!(ShardBuildPlanner::validate_routed(d, 2).is_err());
    }

    #[test]
    fn watermark_restart_resumes_without_duplicate() {
        let p = ShardBuildPlanner::new(4).unwrap();
        for _ in 0..10 {
            p.alloc_on(0).unwrap();
            p.alloc_on(1).unwrap();
        }
        let wm = p.watermarks();
        assert_eq!(wm, vec![10, 10, 0, 0]);
        let resumed = ShardBuildPlanner::with_watermarks(&wm).unwrap();
        // 续跑：分片 0/1 从水位 10 起，不重复
        assert_eq!(crate::docid_alloc::local_of(resumed.alloc_on(0).unwrap()), 10);
        assert_eq!(crate::docid_alloc::local_of(resumed.alloc_on(1).unwrap()), 10);
    }

    #[test]
    fn resize_adds_shards_keeps_ownership() {
        let mut p = ShardBuildPlanner::new(4).unwrap();
        let existing: Vec<u64> = (0..40).map(|r| p.alloc(r).unwrap()).collect();
        p.resize(8);
        assert_eq!(p.shard_count(), 8);
        for d in existing {
            assert!(crate::docid_alloc::shard_of(d) < 4, "扩容后归属不变");
        }
        for sid in 4..8u16 {
            let d = p.alloc_on(sid).unwrap();
            assert_eq!(crate::docid_alloc::shard_of(d), sid);
        }
    }

    #[test]
    fn invalid_watermarks_rejected() {
        assert!(ShardBuildPlanner::with_watermarks(&[]).is_err());
        // 分片 0 水位超限拒绝
        let mut wm = vec![0u64; 4];
        wm[1] = crate::docid_alloc::LOCAL_CAPACITY;
        assert!(ShardBuildPlanner::with_watermarks(&wm).is_err());
    }
}
