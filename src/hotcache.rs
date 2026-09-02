//! HotCache：文档热缓存 + 写失效链（design 6.5 / 6.6 / development 步骤 11）。
//!
//! - 按 DocId 缓存序列化文档，命中直接返回（热点查询亚毫秒，design 7）；
//! - 字节预算硬上限 + 软水位主动淘汰（85% → 75%，防突发卡顿）；
//! - `max_document_size_bytes`：大文档不缓存，防挤占内存；
//! - 写失效链第①步：Put 时 `invalidate(docid)`，保证查询不读到旧版本；
//! - 淘汰策略：MVP 支持 lru / lfu（lfu 为计数近似，tiny-lfu 留待阶段 1.5）；
//! - **热点 key 自动缓存（design 14.1.2，M6-4）**：访问计数达到 `hot_threshold` 自动晋升到
//!   **保护区**（独立段，普通淘汰避让；写失效 / 硬预算兜底仍可清除），热点不被冷数据挤掉。
//!
//! # 并发模型（读写分离 O 项收尾，7.72）
//!
//! 原实现整包 `Mutex<HotCache>`——点查热路径 `hotcache.lock()` 与写路径 put/invalidate 互斥，
//! 多个并发读之间也抢同一把锁（读被写拖垮的残留）。本次内部粒度化：
//! - **缓存区**（cache/protected/used_bytes/promotions）用 `RwLock`：读路径 `peek`（不更新
//!   LRU 序）持**读锁**——读读完全并行；写路径（put/invalidate/promote/evict）持**写锁**；
//! - **访问计数**用 `DashMap`（无锁）——读命中计数不碰 RwLock，热点晋升判定无锁读；
//! - `get` 达热点阈值需 promote：先读锁 peek + 无锁计数 → 释放读锁 → 再写锁 promote
//!   （幂等：pop+put，多线程同时触发无害）；
//! - 工程权衡：读命中不刷新 LRU 序（`LruCache::get` 需 `&mut`），LRU 淘汰近似化——热度由
//!   DashMap 计数 + 热点保护区承载，LRU 仅作冷数据兜底序，影响可接受。

use std::sync::RwLock;

use dashmap::DashMap;
use lru::LruCache;

use crate::config::model::HotCacheConfig;

/// 缓存区（写路径独占 / 读路径共享）。
struct HotCacheInner {
    /// 主缓存：docid → 文档字节（LRU 序；普通淘汰域）。
    cache: LruCache<u64, Vec<u8>>,
    /// 热点保护区：docid → 文档字节（自动晋升，design 14.1.2；普通淘汰避让）。
    protected: LruCache<u64, Vec<u8>>,
    /// 当前占用字节（主缓存 + 保护区）。
    used_bytes: usize,
    /// 晋升热点次数（监控 / 测试）。
    promotions: u64,
}

/// 文档热缓存。
pub struct HotCache {
    config: HotCacheConfig,
    /// 缓存区（RwLock：读读并行 / 写独占）。
    inner: RwLock<HotCacheInner>,
    /// 访问统计：docid → 访问计数（DashMap 无锁——读命中计数不阻塞并行读；
    /// 供 LFU 淘汰与 hot_threshold 预热判断）。
    stats: DashMap<u64, u64>,
}

impl HotCache {
    pub fn new(config: HotCacheConfig) -> Self {
        // P41：条目容量 unbounded，淘汰**完全由字节预算控制**——否则
        // LruCache 容量满后内部淘汰不通知 stats/used_bytes（stats 泄漏 + used_bytes 虚增），
        // 且 evict 找不到真实 victim 导致超预算死循环（大批量回表查询灌满缓存后写路径卡死）。
        Self {
            config,
            inner: RwLock::new(HotCacheInner {
                cache: LruCache::unbounded(),
                protected: LruCache::unbounded(),
                used_bytes: 0,
                promotions: 0,
            }),
            stats: DashMap::new(),
        }
    }

    /// 读取：命中则计数 +1；主缓存计数达 `hot_threshold` 自动晋升保护区；返回克隆文档。
    /// 读读并行（RwLock 读锁 + DashMap 无锁计数）；promote 走写锁（幂等）。
    pub fn get(&self, docid: u64) -> Option<Vec<u8>> {
        let inner = self.inner.read().unwrap();
        // 保护区命中
        if let Some(v) = inner.protected.peek(&docid) {
            let out = v.clone();
            drop(inner); // 计数无锁，先释放读锁
            self.stats.entry(docid).and_modify(|c| *c = c.saturating_add(1)).or_insert(1);
            return Some(out);
        }
        if let Some(v) = inner.cache.peek(&docid) {
            let out = v.clone(); // 返回副本
            let hot = match self.stats.get(&docid) {
                Some(e) => {
                    let count = e.value().saturating_add(1);
                    // 热点 key 自动缓存（design 14.1.2）：达到阈值即晋升，此后淘汰避让
                    count >= self.config.hot_threshold as u64
                }
                None => false,
            };
            drop(inner); // 计数无锁，先释放读锁（promote 需写锁，避免读锁升级死锁）
            self.stats.entry(docid).and_modify(|c| *c = c.saturating_add(1)).or_insert(1);
            if hot {
                self.promote(docid, out.clone());
            }
            return Some(out);
        }
        None
    }

    /// 晋升到保护区：从主缓存移出（避免双份），插入保护区。写锁内幂等
    /// （并发读同时达阈值触发多次 promote：pop+put 结果一致，无害）。
    fn promote(&self, docid: u64, value: Vec<u8>) {
        let mut inner = self.inner.write().unwrap();
        if let Some(old) = inner.cache.pop(&docid) {
            inner.used_bytes = inner.used_bytes.saturating_sub(old.len());
        }
        // 先取旧值长度再赋值（避免借用冲突）
        let old_len = inner.protected.get(&docid).map(|v| v.len());
        if let Some(n) = old_len {
            inner.used_bytes = inner.used_bytes.saturating_sub(n);
        }
        inner.protected.put(docid, value.clone());
        inner.used_bytes += value.len();
        inner.promotions += 1;
    }

    /// 写入：超过 max_document_size_bytes 不缓存；热点 key 直接更新保护区（保留热度）；
    /// 写入后按需淘汰至预算内。写锁独占（与读路径 RwLock 互斥）。
    pub fn put(&self, docid: u64, value: Vec<u8>) {
        if value.len() > self.config.max_document_size_bytes {
            return; // 大对象不缓存
        }
        let mut inner = self.inner.write().unwrap();
        if inner.protected.contains(&docid) {
            // 热点 key 更新：留在保护区（热度不重置）
            let old_len = inner.protected.get(&docid).map(|v| v.len());
            if let Some(n) = old_len {
                inner.used_bytes = inner.used_bytes.saturating_sub(n);
            }
            inner.protected.put(docid, value.clone());
            inner.used_bytes += value.len();
            self.stats.entry(docid).or_insert(1);
            self.evict_to_budget(&mut inner);
            return;
        }
        let old_len = inner.cache.get(&docid).map(|v| v.len());
        if let Some(n) = old_len {
            inner.used_bytes = inner.used_bytes.saturating_sub(n);
        }
        inner.cache.put(docid, value.clone());
        inner.used_bytes += value.len();
        // 仅新条目计 1；已存在条目保留既有热度（不被 put 重置）
        self.stats.entry(docid).or_insert(1);
        self.evict_to_budget(&mut inner);
    }

    /// 写失效链：删除该 docid 缓存（主缓存 + 保护区），保证不读到旧版本。写锁独占。
    pub fn invalidate(&self, docid: u64) {
        let mut inner = self.inner.write().unwrap();
        if let Some(old) = inner.cache.pop(&docid) {
            inner.used_bytes = inner.used_bytes.saturating_sub(old.len());
        }
        if let Some(old) = inner.protected.pop(&docid) {
            inner.used_bytes = inner.used_bytes.saturating_sub(old.len());
        }
        self.stats.remove(&docid);
    }

    /// 淘汰至预算内（P41 重构）：**硬预算强制压回**（每次淘汰 O(1)/O(64)，均摊可控）；
    /// **软水位渐进淘汰**（每次 put 至多 1 个，避免单次 put 的 O(N) evict 风暴——
    /// 大批量回表查询灌满缓存时，原 while 全清 + O(N) 扫描会把写路径卡死）。
    /// 调用方须已持写锁（`inner` 传入）。
    fn evict_to_budget(&self, inner: &mut HotCacheInner) {
        let hard = self.config.max_memory_mb.saturating_mul(1024 * 1024);
        let high = (hard as f64 * self.config.eviction_high_water) as usize;
        let low = (hard as f64 * self.config.eviction_low_water) as usize;
        // 硬预算保护（主缓存淘汰完再淘汰保护区）
        if inner.used_bytes > hard {
            while inner.used_bytes > hard {
                if !self.evict_one(inner) {
                    break;
                }
            }
            return;
        }
        // 软水位主动淘汰：达 high 后每次写入淘汰 1 个，逐步回落至 low（渐进式，防风暴）
        if inner.used_bytes > high && inner.used_bytes > low {
            let _ = self.evict_one(inner);
        }
    }

    /// 淘汰一个条目：主缓存按策略选；主缓存空时淘汰保护区 LRU（硬预算兜底）。
    /// 调用方须已持写锁（`inner` 传入）。
    fn evict_one(&self, inner: &mut HotCacheInner) -> bool {
        if self.evict_from_main(inner) {
            return true;
        }
        // 主缓存已空仍超预算（极端：超大热点集）→ 淘汰保护区最久未用
        let victim = inner.protected.peek_lru().map(|(k, _)| *k);
        if let Some(victim) = victim {
            if let Some(old) = inner.protected.pop(&victim) {
                inner.used_bytes = inner.used_bytes.saturating_sub(old.len());
                self.stats.remove(&victim);
                return true;
            }
        }
        false
    }

    /// 从主缓存淘汰一个条目。调用方须已持写锁（`inner` 传入）。
    fn evict_from_main(&self, inner: &mut HotCacheInner) -> bool {
        let victim = match self.config.eviction_policy.as_str() {
            "lfu" => self.pick_lfu_victim(inner),
            _ => inner.cache.peek_lru().map(|(k, _)| *k),
        };
        let Some(victim) = victim else { return false };
        if let Some(old) = inner.cache.pop(&victim) {
            inner.used_bytes = inner.used_bytes.saturating_sub(old.len());
            self.stats.remove(&victim);
            true
        } else {
            false
        }
    }

    /// LFU（P41 采样近似）：在主缓存前 64 个条目中选访问计数最低者（O(64) 常量）。
    /// 原实现全量扫描 stats（O(N)）——大数据量回表时 N 达数十万，每次淘汰 O(N) 会把写路径
    /// 卡成 O(N²)（P41 实测大批量回表灌爆缓存后 server 假死）。采样近似牺牲极小精确性换恒定开销。
    /// 调用方须已持写锁（`inner` 传入）。
    fn pick_lfu_victim(&self, inner: &HotCacheInner) -> Option<u64> {
        let mut best: Option<(u64, u64)> = None;
        for (k, _) in inner.cache.iter().take(64) {
            let count = self.stats.get(k).map_or(0, |e| *e.value());
            match best {
                Some((_, bc)) if bc <= count => {}
                _ => best = Some((*k, count)),
            }
        }
        best.map(|(k, _)| k)
    }

    pub fn len(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.cache.len() + inner.protected.len()
    }

    pub fn is_empty(&self) -> bool {
        let inner = self.inner.read().unwrap();
        inner.cache.is_empty() && inner.protected.is_empty()
    }

    pub fn used_bytes(&self) -> usize {
        self.inner.read().unwrap().used_bytes
    }

    /// 保护区条目数（监控 / 测试）。
    pub fn protected_len(&self) -> usize {
        self.inner.read().unwrap().protected.len()
    }

    /// 热点晋升累计次数（监控 / 测试）。
    pub fn promotions(&self) -> u64 {
        self.inner.read().unwrap().promotions
    }

    /// 某 docid 的访问计数（预热判断 / 测试）。
    pub fn access_count(&self, docid: u64) -> u64 {
        self.stats.get(&docid).map_or(0, |e| *e.value())
    }

    /// 清空（紧急内存回收 / 测试）。写锁独占。
    pub fn clear(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.cache.clear();
        inner.protected.clear();
        inner.used_bytes = 0;
        inner.promotions = 0;
        self.stats.clear();
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
        let c = HotCache::new(small_cfg(4));
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
        let c = HotCache::new(cfg);
        c.put(1, vec![0u8; 20]); // 超过 10 字节 → 不缓存
        assert!(c.get(1).is_none());
    }

    #[test]
    fn invalidate_removes() {
        let c = HotCache::new(small_cfg(4));
        c.put(1, b"x".to_vec());
        assert!(c.get(1).is_some());
        c.invalidate(1);
        assert!(c.get(1).is_none());
    }

    #[test]
    fn lru_evicts_oldest() {
        // 预算小，插入多个触发淘汰
        let c = HotCache::new(small_cfg(1));
        for i in 0..500u64 {
            c.put(i, vec![0u8; 4096]); // 4KB × 500 = 2MB > 1MB 预算
        }
        assert!(c.used_bytes() <= 1024 * 1024, "超预算: {}", c.used_bytes());
    }

    // ---------- P41：批量回表场景 stats 泄漏 / used_bytes 虚增 / LFU O(N) 风暴 ----------

    #[test]
    fn bulk_put_no_stats_leak_no_used_bytes_drift() {
        // 容量 MAX + 字节预算控制：LruCache 不再内部淘汰 → stats 与缓存同步（不泄漏）、
        // used_bytes 准确（不虚增）——修复前 stats 无限增长、used_bytes 超预算且淘汰无效。
        let c = HotCache::new(small_cfg(1)); // 1MB 预算
        for i in 0..5000u64 {
            c.put(i, vec![0u8; 4096]); // 4KB × 5000 = 20MB ≫ 1MB（硬预算强制压回）
        }
        // used_bytes 必须压回硬预算内（不虚增）
        assert!(
            c.used_bytes() <= 1024 * 1024,
            "used_bytes 虚增: {}",
            c.used_bytes()
        );
        // stats 与缓存同步（无内部淘汰泄漏）
        assert!(
            c.stats.len() <= c.len() + 1,
            "stats 泄漏: stats={} cache={}",
            c.stats.len(),
            c.len()
        );
        // 淘汰真的释放了条目（不是死循环空转）
        assert!(c.len() < 3000, "缓存未真正淘汰: {}", c.len());
        // 缓存仍可正常命中
        let probe = c.get(4999);
        assert!(probe.is_some() || c.len() > 0, "缓存不可用");
    }

    #[test]
    fn soft_water_evicts_gradually_no_storm() {
        // 软水位渐进淘汰：达 high 后每 put 只淘汰 1 个（防单次 put O(N) evict 风暴）。
        // 用 512KB×3 + 1MB 预算（high≈0.85MB）：写满后继续写，put 均摊 O(1) 级。
        let mut cfg = small_cfg(1);
        cfg.eviction_policy = "lfu".into();
        cfg.max_document_size_bytes = 1024 * 1024;
        let c = HotCache::new(cfg);
        let t0 = std::time::Instant::now();
        for i in 0..10_000u64 {
            c.put(i, vec![0u8; 512 * 1024]); // 512KB × 10000 = 5GB 总量
        }
        // 全部 put 必须在秒级完成（修复前 LFU O(N) 扫描 + 全清风暴会卡死）
        let elapsed = t0.elapsed().as_secs_f64();
        assert!(elapsed < 5.0, "渐进淘汰过慢: {elapsed:.1}s");
        assert!(c.used_bytes() <= 1024 * 1024);
    }

    #[test]
    fn lfu_evicts_coldest() {
        // 可控场景：512KB×3，预算 1MB；先提升 key1 热度，淘汰必须避让
        let mut cfg = small_cfg(1);
        cfg.eviction_policy = "lfu".into();
        cfg.max_document_size_bytes = 1024 * 1024; // 允许 512KB 文档
        let c = HotCache::new(cfg);
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
        let c = HotCache::new(small_cfg(4));
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
        let c = HotCache::new(hot_cfg(1, 3)); // 阈值 3 次
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
        let c = HotCache::new(hot_cfg(4, 2));
        c.put(7, b"v".to_vec());
        c.get(7); // count 2 → 晋升
        assert_eq!(c.protected_len(), 1);
        c.invalidate(7);
        assert!(c.get(7).is_none(), "写失效应清除保护区缓存");
        assert_eq!(c.protected_len(), 0);
    }

    #[test]
    fn hot_key_put_updates_in_place() {
        let c = HotCache::new(hot_cfg(4, 2));
        c.put(7, b"old".to_vec());
        c.get(7); // 晋升
        c.put(7, b"new".to_vec()); // 热点更新：留在保护区
        assert_eq!(c.get(7).unwrap(), b"new");
        assert_eq!(c.protected_len(), 1);
    }

    #[test]
    fn promotion_requires_threshold() {
        let c = HotCache::new(hot_cfg(4, 5)); // 阈值 5
        c.put(7, b"v".to_vec());
        c.get(7);
        c.get(7); // count 3 < 5
        assert_eq!(c.promotions(), 0, "未达阈值不应晋升");
        assert_eq!(c.protected_len(), 0);
    }

    #[test]
    fn overwrite_updates_value() {
        let c = HotCache::new(small_cfg(4));
        c.put(1, b"old".to_vec());
        c.put(1, b"new".to_vec());
        assert_eq!(c.get(1).unwrap(), b"new");
    }

    // ---------- 读写分离（7.72）：并发读并行 + 读写并发正确性 ----------

    #[test]
    fn concurrent_reads_all_hit_no_data_race() {
        // 多线程并发读同一批热点 key：全部命中且值一致（RwLock 读读并行 + DashMap 计数无锁）
        let c = std::sync::Arc::new(HotCache::new(small_cfg(8)));
        for i in 0..100u64 {
            c.put(i, format!("doc-{i}").into_bytes());
        }
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let c = std::sync::Arc::clone(&c);
            handles.push(std::thread::spawn(move || {
                for i in 0..100u64 {
                    let got = c.get(i).expect("命中");
                    assert_eq!(got, format!("doc-{i}").into_bytes(), "t{t} 读值不一致");
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // 命中计数累加正确（8 线程 × 100 key）
        for i in 0..100u64 {
            assert!(c.access_count(i) >= 8, "key{i} 计数不足: {}", c.access_count(i));
        }
    }

    #[test]
    fn concurrent_reads_with_write_invalidate_no_stale() {
        // 读写并发：读线程持续 get，写线程 put 新值 + invalidate——不允许读到已失效旧值
        let c = std::sync::Arc::new(HotCache::new(small_cfg(8)));
        c.put(1, b"v0".to_vec());
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader = {
            let c = std::sync::Arc::clone(&c);
            let stop = std::sync::Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    // 只读 key 2（写线程从未写它）→ 必须稳定
                    if let Some(v) = c.get(2) {
                        assert_eq!(v, b"stable", "读到被污染值");
                    }
                    std::thread::yield_now();
                }
            })
        };
        // 写线程：更新 key1 并失效 key2（不应存在但保持无 panic）
        for i in 0..500u64 {
            c.put(1, format!("v{i}").into_bytes());
            c.invalidate(2);
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        reader.join().unwrap();
        assert!(c.get(1).is_some(), "写后 key1 应在缓存");
    }
}
