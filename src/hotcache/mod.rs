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
//!
//! # 文件组织（按主题拆分）
//!
//! - `mod.rs`：主类型 `HotCache`/`HotCacheInner` + 读写路径（new/get/put/invalidate）；
//! - `policy.rs`：淘汰策略（LFU 采样近似/硬预算与软水位）+ 热点保护区晋升（promote）；
//! - `entry.rs`：条目/水位/热度统计与清空（len/used_bytes/promotions/access_count/clear）；
//! - `tests.rs`：单元测试（原 hotcache.rs `#[cfg(test)] mod tests` 原样迁移）。

use std::sync::RwLock;

use dashmap::DashMap;
use lru::LruCache;

use crate::config::model::HotCacheConfig;

mod entry;
mod policy;
#[cfg(test)]
mod tests;

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
}
