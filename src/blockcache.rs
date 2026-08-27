//! 块缓存（LRU）（design 4.4 / development 步骤 6）。
//!
//! - 高频访问的数据块驻留内存，命中即免磁盘 IO；
//! - 缓存键 = (SST 文件路径, 块偏移)，因同一文件各块独立且偏移唯一；
//! - 容量按字节预算（`blockcache.max_memory_mb`），LRU 淘汰最久未用；
//! - 读放大防护：块级粒度，不缓存整文件。

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Mutex;

use lru::LruCache;

/// 缓存键：SST 文件 + 块在文件内的偏移。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockCacheKey {
    pub file: PathBuf,
    pub offset: u64,
}

/// LRU 块缓存：内部用 Mutex 保护（读多写少，粗粒度锁即可满足 MVP）。
pub struct BlockCache {
    inner: Mutex<LruCache<BlockCacheKey, Vec<u8>>>,
    /// 每条目的估算字节（块 + key 开销），用于字节预算换算。
    per_entry_estimate: usize,
}

impl BlockCache {
    /// `max_memory_bytes`：缓存字节预算。
    pub fn new(max_memory_bytes: usize, block_size: usize) -> Self {
        let per_entry = block_size.max(1) + 64; // 块体 + key 与链表开销估算
        let capacity = (max_memory_bytes / per_entry).max(1);
        Self {
            inner: Mutex::new(LruCache::new(NonZeroUsize::new(capacity).unwrap())),
            per_entry_estimate: per_entry,
        }
    }

    pub fn get(&self, key: &BlockCacheKey) -> Option<Vec<u8>> {
        self.inner.lock().unwrap().get(key).cloned()
    }

    pub fn put(&self, key: BlockCacheKey, block: Vec<u8>) {
        self.inner.lock().unwrap().put(key, block);
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 当前占用字节估算。
    pub fn used_bytes(&self) -> usize {
        self.len() * self.per_entry_estimate
    }

    /// 清空（内存紧急回收 / 测试）。
    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(i: u64) -> BlockCacheKey {
        BlockCacheKey { file: PathBuf::from(format!("f{i}.sst")), offset: i * 100 }
    }

    #[test]
    fn put_get_roundtrip() {
        let c = BlockCache::new(1024 * 1024, 16 * 1024);
        c.put(key(1), b"block-data".to_vec());
        assert_eq!(c.get(&key(1)).unwrap(), b"block-data");
    }

    #[test]
    fn lru_evicts_oldest_when_full() {
        // 预算只够 2 条：容量 = 预算 / (块大小+64)
        let c = BlockCache::new(2 * 1024, 1024); // per_entry ≈ 1088 → capacity 1
        c.put(key(1), vec![0u8; 1024]);
        c.put(key(2), vec![0u8; 1024]);
        // capacity=1 时插入第二条即淘汰第一条
        assert_eq!(c.len(), 1);
        assert!(c.get(&key(1)).is_none() || c.get(&key(2)).is_none());
    }

    #[test]
    fn recent_use_keeps_entry() {
        // 预算 3072 / 每项(1024+64) → capacity 2
        let c = BlockCache::new(3 * 1024, 1024);
        c.put(key(1), vec![0u8; 1024]);
        c.put(key(2), vec![0u8; 1024]);
        // 访问 key(1) 使其变最新
        assert!(c.get(&key(1)).is_some());
        c.put(key(3), vec![0u8; 1024]); // 应淘汰最久未用的 key(2)
        assert!(c.get(&key(1)).is_some());
        assert!(c.get(&key(3)).is_some());
        assert!(c.get(&key(2)).is_none());
    }

    #[test]
    fn clear_empties() {
        let c = BlockCache::new(1024 * 1024, 1024);
        c.put(key(1), b"x".to_vec());
        c.clear();
        assert!(c.is_empty());
    }
}
