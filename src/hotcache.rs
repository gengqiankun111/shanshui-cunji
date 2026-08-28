//! HotCache：文档热缓存 + 写失效链（design 6.5 / 6.6 / development 步骤 11）。
//!
//! - 按 DocId 缓存序列化文档，命中直接返回（热点查询亚毫秒，design 7）；
//! - 字节预算硬上限 + 软水位主动淘汰（85% → 75%，防突发卡顿）；
//! - `max_document_size_bytes`：大文档不缓存，防挤占内存；
//! - 写失效链第①步：Put 时 `invalidate(docid)`，保证查询不读到旧版本；
//! - 淘汰策略：MVP 支持 lru / lfu（lfu 为计数近似，tiny-lfu 留待阶段 1.5）；
//! - **热点 key 自动缓存（design 14.1.2，M6-4）**：访问计数达到 `hot_threshold` 自动晋升到
//!   **保护区**（独立段，普通淘汰避让；写失效 / 硬预算兜底仍可清除），热点不被冷数据挤掉。

use std::collections::HashMap;
use std::num::NonZeroUsize;

use lru::LruCache;
use tracing::warn;

use crate::config::model::HotCacheConfig;

/// 热点统计：docid → 访问计数（供 LFU 淘汰与 hot_threshold 预热判断）。
struct HotEntry {
    count: u64,
}

/// 保护区容量占主缓存容量比例（1/5：热点数量通常远小于全量）。
const PROTECTED_RATIO: usize = 5;

/// 文档热缓存。
pub struct HotCache {
    config: HotCacheConfig,
    /// 主缓存：docid → 文档字节（LRU 序；普通淘汰域）。
    cache: LruCache<u64, Vec<u8>>,
    /// 热点保护区：docid → 文档字节（自动晋升，design 14.1.2；普通淘汰避让）。
    protected: LruCache<u64, Vec<u8>>,
    /// 访问统计（LFU 淘汰依据 + 热点晋升判断）。
    stats: HashMap<u64, HotEntry>,
    /// 当前占用字节（主缓存 + 保护区）。
    used_bytes: usize,
    /// 晋升热点次数（监控 / 测试）。
    promotions: u64,
}

impl HotCache {
    pub fn new(config: HotCacheConfig) -> Self {
        let capacity = (config.max_memory_mb.saturating_mul(1024 * 1024) / 1024).max(1);
        let protected_cap = (capacity / PROTECTED_RATIO).max(1);
        Self {
            config,
            cache: LruCache::new(NonZeroUsize::new(capacity).unwrap()),
            protected: LruCache::new(NonZeroUsize::new(protected_cap).unwrap()),
            stats: HashMap::new(),
            used_bytes: 0,
            promotions: 0,
        }
    }

    /// 读取：命中则计数 +1；主缓存计数达 `hot_threshold` 自动晋升保护区；返回克隆文档。
    pub fn get(&mut self, docid: u64) -> Option<Vec<u8>> {
        // 保护区命中
        if let Some(v) = self.protected.get(&docid) {
            if let Some(e) = self.stats.get_mut(&docid) {
                e.count = e.count.saturating_add(1);
            }
            return Some(v.clone());
        }
        if let Some(v) = self.cache.get(&docid) {
            let out = v.clone(); // 返回副本（提前克隆，避免与晋升的 &mut self 冲突）
            let hot = match self.stats.get_mut(&docid) {
                Some(e) => {
                    e.count = e.count.saturating_add(1);
                    // 热点 key 自动缓存（design 14.1.2）：达到阈值即晋升，此后淘汰避让
                    e.count >= self.config.hot_threshold as u64
                }
                None => false,
            };
            if hot {
                self.promote(docid, out.clone());
            }
            return Some(out);
        }
        None
    }

    /// 晋升到保护区：从主缓存移出（避免双份），插入保护区。
    fn promote(&mut self, docid: u64, value: Vec<u8>) {
        if let Some(old) = self.cache.pop(&docid) {
            self.used_bytes = self.used_bytes.saturating_sub(old.len());
        }
        if let Some(old) = self.protected.get(&docid) {
            self.used_bytes = self.used_bytes.saturating_sub(old.len());
        }
        self.protected.put(docid, value.clone());
        self.used_bytes += value.len();
        self.promotions += 1;
    }

    /// 写入：超过 max_document_size_bytes 不缓存；热点 key 直接更新保护区（保留热度）；
    /// 写入后按需淘汰至预算内。
    pub fn put(&mut self, docid: u64, value: Vec<u8>) {
        if value.len() > self.config.max_document_size_bytes {
            return; // 大对象不缓存
        }
        if self.protected.contains(&docid) {
            // 热点 key 更新：留在保护区（热度不重置）
            if let Some(old) = self.protected.get(&docid) {
                self.used_bytes = self.used_bytes.saturating_sub(old.len());
            }
            self.protected.put(docid, value.clone());
            self.used_bytes += value.len();
            self.stats.entry(docid).or_insert(HotEntry { count: 1 });
            self.evict_to_budget();
            return;
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

    /// 写失效链：删除该 docid 缓存（主缓存 + 保护区），保证不读到旧版本。
    pub fn invalidate(&mut self, docid: u64) {
        if let Some(old) = self.cache.pop(&docid) {
            self.used_bytes = self.used_bytes.saturating_sub(old.len());
        }
        if let Some(old) = self.protected.pop(&docid) {
            self.used_bytes = self.used_bytes.saturating_sub(old.len());
        }
        self.stats.remove(&docid);
    }

    /// 淘汰至硬预算内；若到达软水位也主动清出冷数据至低水位。主缓存优先，保护区兜底。
    fn evict_to_budget(&mut self) {
        let hard = self.config.max_memory_mb.saturating_mul(1024 * 1024);
        let high = (hard as f64 * self.config.eviction_high_water) as usize;
        let low = (hard as f64 * self.config.eviction_low_water) as usize;
        // 硬预算保护（主缓存淘汰完再淘汰保护区）
        while self.used_bytes > hard {
            if !self.evict_one() {
                break;
            }
        }
        // 软水位主动淘汰：达 85% 淘汰至 75%（防突发卡顿，design 6.5）
        if self.used_bytes > high {
            warn!(
                "HotCache 达软水位 {}/{}，主动淘汰冷数据",
                self.used_bytes, hard
            );
            while self.used_bytes > low {
                if !self.evict_one() {
                    break;
                }
            }
        }
    }

    /// 淘汰一个条目：主缓存按策略选；主缓存空时淘汰保护区 LRU（硬预算兜底）。
    fn evict_one(&mut self) -> bool {
        if self.evict_from_main() {
            return true;
        }
        // 主缓存已空仍超预算（极端：超大热点集）→ 淘汰保护区最久未用
        let victim = self.protected.peek_lru().map(|(k, _)| *k);
        if let Some(victim) = victim {
            if let Some(old) = self.protected.pop(&victim) {
                self.used_bytes = self.used_bytes.saturating_sub(old.len());
                self.stats.remove(&victim);
                return true;
            }
        }
        false
    }

    /// 从主缓存淘汰一个条目。
    fn evict_from_main(&mut self) -> bool {
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

    /// LFU：选择访问计数最低者（同计数取 LRU 最久未用）；跳过保护区 key（不在主缓存）。
    fn pick_lfu_victim(&self) -> Option<u64> {
        let mut best: Option<(u64, u64)> = None;
        for (k, e) in &self.stats {
            if self.protected.contains(k) {
                continue; // 热点保护区不参与主缓存淘汰
            }
            match best {
                Some((_, bc)) if bc <= e.count => {}
                _ => best = Some((*k, e.count)),
            }
        }
        best.map(|(k, _)| k)
    }

    pub fn len(&self) -> usize {
        self.cache.len() + self.protected.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty() && self.protected.is_empty()
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes
    }

    /// 保护区条目数（监控 / 测试）。
    pub fn protected_len(&self) -> usize {
        self.protected.len()
    }

    /// 热点晋升累计次数（监控 / 测试）。
    pub fn promotions(&self) -> u64 {
        self.promotions
    }

    /// 某 docid 的访问计数（预热判断 / 测试）。
    pub fn access_count(&self, docid: u64) -> u64 {
        self.stats.get(&docid).map_or(0, |e| e.count)
    }

    /// 清空（紧急内存回收 / 测试）。
    pub fn clear(&mut self) {
        self.cache.clear();
        self.protected.clear();
        self.stats.clear();
        self.used_bytes = 0;
        self.promotions = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_cfg(max_mb: usize) -> HotCacheConfig {
        HotCacheConfig {
            max_memory_mb: max_mb,
            eviction_policy: "lru".into(),
            ..Default::default()
        }
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

    // ---------- 热点 key 自动缓存（design 14.1.2，M6-4） ----------

    fn hot_cfg(max_mb: usize, threshold: u32) -> HotCacheConfig {
        let mut cfg = small_cfg(max_mb);
        cfg.hot_threshold = threshold;
        cfg.eviction_policy = "lfu".into();
        cfg.max_document_size_bytes = 1024 * 1024;
        cfg
    }

    #[test]
    fn hot_key_promoted_and_survives_cold_pressure() {
        let mut c = HotCache::new(hot_cfg(1, 3)); // 阈值 3 次
                                                  // 512KB 文档：预算 1MB
        c.put(1, vec![0u8; 512 * 1024]);
        for _ in 0..2 {
            c.get(1); // count 1→3 触发晋升
        }
        assert_eq!(c.promotions(), 1, "达阈值应晋升一次");
        assert_eq!(c.protected_len(), 1);
        // 大量冷写入挤压主缓存：热点 key 必须存活
        for i in 0..50u64 {
            c.put(100 + i, vec![0u8; 200 * 1024]);
        }
        assert!(c.get(1).is_some(), "热点 key 不应被冷数据淘汰");
        assert!(c.used_bytes() <= 1024 * 1024, "超预算: {}", c.used_bytes());
    }

    #[test]
    fn hot_key_invalidate_removes_from_protected() {
        let mut c = HotCache::new(hot_cfg(4, 2));
        c.put(7, b"v".to_vec());
        c.get(7); // count 2 → 晋升
        assert_eq!(c.protected_len(), 1);
        c.invalidate(7);
        assert!(c.get(7).is_none(), "写失效应清除保护区缓存");
        assert_eq!(c.protected_len(), 0);
    }

    #[test]
    fn hot_key_put_updates_in_place() {
        let mut c = HotCache::new(hot_cfg(4, 2));
        c.put(7, b"old".to_vec());
        c.get(7); // 晋升
        c.put(7, b"new".to_vec()); // 热点更新：留在保护区
        assert_eq!(c.get(7).unwrap(), b"new");
        assert_eq!(c.protected_len(), 1);
    }

    #[test]
    fn promotion_requires_threshold() {
        let mut c = HotCache::new(hot_cfg(4, 5)); // 阈值 5
        c.put(7, b"v".to_vec());
        c.get(7);
        c.get(7); // count 3 < 5
        assert_eq!(c.promotions(), 0, "未达阈值不应晋升");
        assert_eq!(c.protected_len(), 0);
    }

    #[test]
    fn overwrite_updates_value() {
        let mut c = HotCache::new(small_cfg(4));
        c.put(1, b"old".to_vec());
        c.put(1, b"new".to_vec());
        assert_eq!(c.get(1).unwrap(), b"new");
    }
}
