//! HotCache：文档热缓存 + 写失效链（design 6.5 / 6.6 / development 步骤 11）。
//!
//! - 按 DocId 缓存序列化文档，命中直接返回（热点查询亚毫秒，design 7）；
//! - 字节预算硬上限 + 软水位主动淘汰（85% → 75%，防突发卡顿）；
//! - `max_document_size_bytes`：大文档不缓存，防挤占内存；
//! - 写失效链第①步：Put 时 `invalidate(docid)`，保证查询不读到旧版本；
//! - 淘汰策略：MVP 支持 lru / lfu（lfu 为计数近似，tiny-lfu 留待阶段 1.5）。

use std::collections::HashMap;
use std::num::NonZeroUsize;

use lru::LruCache;
use tracing::warn;

use crate::config::model::HotCacheConfig;

/// 热点统计：docid → 访问计数（供 LFU 淘汰与 hot_threshold 预热判断）。
struct HotEntry {
    count: u64,
}

/// 文档热缓存。
pub struct HotCache {
    config: HotCacheConfig,
    /// 主缓存：docid → 文档字节（LRU 序）。
    cache: LruCache<u64, Vec<u8>>,
    /// 访问统计（LFU 淘汰依据）。
    stats: HashMap<u64, HotEntry>,
    /// 当前占用字节。
    used_bytes: usize,
}

impl HotCache {
    pub fn new(config: HotCacheConfig) -> Self {
        let capacity = (config.max_memory_mb.saturating_mul(1024 * 1024) / 1024).max(1);
        Self {
            config,
            cache: LruCache::new(NonZeroUsize::new(capacity).unwrap()),
            stats: HashMap::new(),
            used_bytes: 0,
        }
    }

    /// 读取：命中则计数 +1（LFU / 预热判断），并返回克隆文档。
    pub fn get(&mut self, docid: u64) -> Option<Vec<u8>> {
        if let Some(v) = self.cache.get(&docid) {
            if let Some(e) = self.stats.get_mut(&docid) {
                e.count = e.count.saturating_add(1);
            }
            return Some(v.clone());
        }
        None
    }

    /// 写入：超过 max_document_size_bytes 不缓存；写入后按需淘汰至预算内。
    pub fn put(&mut self, docid: u64, value: Vec<u8>) {
        if value.len() > self.config.max_document_size_bytes {
            return; // 大对象不缓存
        }
        if let Some(old) = self.cache.get(&docid) {
            self.used_bytes = self.used_bytes.saturating_sub(old.len());
        }
        self.cache.put(docid, value.clone());
        self.used_bytes += value.len();
        // 仅新条目计 1；已存在条目保留既有热度（不被 put 重置）
        self.stats.entry(docid).or_insert(HotEntry { count: 1 });
        self.evict_to_budget();
    }

    /// 写失效链：删除该 docid 缓存（保证不读到旧版本）。
    pub fn invalidate(&mut self, docid: u64) {
        if let Some(old) = self.cache.pop(&docid) {
            self.used_bytes = self.used_bytes.saturating_sub(old.len());
        }
        self.stats.remove(&docid);
    }

    /// 淘汰至硬预算内；若到达软水位也主动清出冷数据至低水位。
    fn evict_to_budget(&mut self) {
        let hard = self.config.max_memory_mb.saturating_mul(1024 * 1024);
        let high = (hard as f64 * self.config.eviction_high_water) as usize;
        let low = (hard as f64 * self.config.eviction_low_water) as usize;
        // 硬预算保护
        while self.used_bytes > hard {
            self.evict_one();
        }
        // 软水位主动淘汰：达 85% 淘汰至 75%（防突发卡顿，design 6.5）
        if self.used_bytes > high {
            warn!("HotCache 达软水位 {}/{}，主动淘汰冷数据", self.used_bytes, hard);
            while self.used_bytes > low {
                if !self.evict_one() {
                    break;
                }
            }
        }
    }

    /// 淘汰一个条目：按策略选择。
    fn evict_one(&mut self) -> bool {
        let victim = match self.config.eviction_policy.as_str() {
            "lfu" => self.pick_lfu_victim(),
            _ => self.cache.peek_lru().map(|(k, _)| *k),
        };
        let Some(victim) = victim else { return false };
        if let Some(old) = self.cache.pop(&victim) {
            self.used_bytes = self.used_bytes.saturating_sub(old.len());
            self.stats.remove(&victim);
            true
        } else {
            false
        }
    }

    /// LFU：选择访问计数最低者（同计数取 LRU 最久未用）。
    fn pick_lfu_victim(&self) -> Option<u64> {
        let mut best: Option<(u64, u64)> = None;
        for (k, e) in &self.stats {
            match best {
                Some((_, bc)) if bc <= e.count => {}
                _ => best = Some((*k, e.count)),
            }
        }
        best.map(|(k, _)| k)
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes
    }

    /// 某 docid 的访问计数（预热判断 / 测试）。
    pub fn access_count(&self, docid: u64) -> u64 {
        self.stats.get(&docid).map_or(0, |e| e.count)
    }

    /// 清空（紧急内存回收 / 测试）。
    pub fn clear(&mut self) {
        self.cache.clear();
        self.stats.clear();
        self.used_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_cfg(max_mb: usize) -> HotCacheConfig {
        let mut c = HotCacheConfig::default();
        c.max_memory_mb = max_mb;
        c.eviction_policy = "lru".into();
        c
    }

    #[test]
    fn put_get_roundtrip() {
        let mut c = HotCache::new(small_cfg(4));
        c.put(1, b"doc-1".to_vec());
        c.put(2, b"doc-2".to_vec());
        assert_eq!(c.get(1).unwrap(), b"doc-1");
        assert_eq!(c.get(2).unwrap(), b"doc-2");
        assert!(c.get(99).is_none());
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn large_document_not_cached() {
        let mut cfg = small_cfg(4);
        cfg.max_document_size_bytes = 10;
        let mut c = HotCache::new(cfg);
        c.put(1, vec![0u8; 20]); // 超过 10 字节 → 不缓存
        assert!(c.get(1).is_none());
    }

    #[test]
    fn invalidate_removes() {
        let mut c = HotCache::new(small_cfg(4));
        c.put(1, b"x".to_vec());
        assert!(c.get(1).is_some());
        c.invalidate(1);
        assert!(c.get(1).is_none());
    }

    #[test]
    fn lru_evicts_oldest() {
        // 预算小，插入多个触发淘汰
        let mut c = HotCache::new(small_cfg(1));
        for i in 0..500u64 {
            c.put(i, vec![0u8; 4096]); // 4KB × 500 = 2MB > 1MB 预算
        }
        assert!(c.used_bytes() <= 1024 * 1024, "超预算: {}", c.used_bytes());
    }

    #[test]
    fn lfu_evicts_coldest() {
        // 可控场景：512KB×3，预算 1MB；先提升 key1 热度，淘汰必须避让
        let mut cfg = small_cfg(1);
        cfg.eviction_policy = "lfu".into();
        cfg.max_document_size_bytes = 1024 * 1024; // 允许 512KB 文档
        let mut c = HotCache::new(cfg);
        c.put(1, vec![0u8; 512 * 1024]); // 512KB
        for _ in 0..10 {
            c.get(1); // key1 count 提升至 11
        }
        c.put(2, vec![0u8; 512 * 1024]); // 超软水位 → 应淘汰 key2（count=1）
        assert!(c.get(1).is_some(), "热 key1 不应被 LFU 淘汰");
        c.put(3, vec![0u8; 512 * 1024]); // 再超 → 淘汰 key3（count=1）
        assert!(c.get(1).is_some(), "热 key1 仍不应被淘汰");
        assert!(c.used_bytes() <= 1024 * 1024, "超预算: {}", c.used_bytes());
    }

    #[test]
    fn access_count_tracks_hotness() {
        let mut c = HotCache::new(small_cfg(4));
        c.put(7, b"v".to_vec()); // put 计 1 次
        c.get(7);
        c.get(7);
        c.get(7);
        assert_eq!(c.access_count(7), 4);
    }

    #[test]
    fn overwrite_updates_value() {
        let mut c = HotCache::new(small_cfg(4));
        c.put(1, b"old".to_vec());
        c.put(1, b"new".to_vec());
        assert_eq!(c.get(1).unwrap(), b"new");
    }
}
