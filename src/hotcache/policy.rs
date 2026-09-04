//! 淘汰策略与热点保护区晋升（design 14.1.2 / P41 重构）。
//!
//! - [`HotCache::promote`]：访问计数达 `hot_threshold` 的 key 从主缓存晋升保护区（普通淘汰
//!   避让，写失效 / 硬预算兜底仍可清除）；幂等（并发读同时触发无害）。
//! - [`HotCache::evict_to_budget`]：**硬预算强制压回** + **软水位渐进淘汰**（每次 put 至多
//!   淘汰 1 个，防 P41 大批量回表的 O(N) evict 风暴）。
//! - [`HotCache::evict_one`] → [`HotCache::evict_from_main`] → [`HotCache::pick_lfu_victim`]：
//!   淘汰链路；主缓存按策略选 victim（lfu 为前 64 条采样近似 O(64)，其余按 lru），主缓存
//!   空仍超预算时兜底淘汰保护区 LRU。
//!
//! 除 `promote`/`evict_to_budget`（供 `get`/`put` 调用，`pub(super)`）外均为本文件内部链路；
//! 所有方法调用方须已持相应写锁（inner 由调用方传入）。

use super::{HotCache, HotCacheInner};

impl HotCache {
    /// 晋升到保护区：从主缓存移出（避免双份），插入保护区。写锁内幂等
    /// （并发读同时达阈值触发多次 promote：pop+put 结果一致，无害）。
    pub(super) fn promote(&self, docid: u64, value: Vec<u8>) {
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

    /// 淘汰至预算内（P41 重构）：**硬预算强制压回**（每次淘汰 O(1)/O(64)，均摊可控）；
    /// **软水位渐进淘汰**（每次 put 至多 1 个，避免单次 put 的 O(N) evict 风暴——
    /// 大批量回表查询灌满缓存时，原 while 全清 + O(N) 扫描会把写路径卡死）。
    /// 调用方须已持写锁（`inner` 传入）。
    pub(super) fn evict_to_budget(&self, inner: &mut HotCacheInner) {
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
}
