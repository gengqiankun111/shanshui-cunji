//! 网关全局 Term 缓存（design 9.9，阶段 2）。
//!
//! 痛点：高频筛选条件（如 `status='active'`）每次广播查询都要打满全部分片合并几百万 DocId。
//! 方案：网关层全局 LRU 缓存——**Key = (节点 ID, Term)**，**Value = 压缩 DocId 列表（RoaringBitmap）**。
//!
//! - **命中**：广播查询先问缓存，命中直接返回该节点 DocId 列表，不透传后端分片；
//! - **TTL 兜底（默认 5s）**：超过 TTL 的条目视为过期，命中时失效重查（双保险防脏读）；
//! - **写计数失效**：写入路径 `record_write(term)`——某 Term 在 1 秒窗口内写入超过阈值
//!   （默认 100 次）→ `invalidate(term)` 主动失效其全局缓存；
//! - **LRU 容量**：`term_cache_max_entries` 上限，防缓存无限增长。
//!
//! 与 5.2.1 预分片 Chunk 配合：缓存命中直出本片 Bitmap，广播合并进一步降为 O(1) 拼接。

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use lru::LruCache;
use roaring::RoaringBitmap;

/// 写计数窗口：某 Term 在此窗口内写入超阈值 → 主动失效（design 9.9）。
const WRITE_WINDOW: Duration = Duration::from_secs(1);

/// 缓存条目。
struct CachedChunk {
    bitmap: RoaringBitmap,
    at: Instant,
}

/// 网关全局 Term 缓存（线程安全，供网关 &mut 独占使用亦可跨线程）。
pub struct TermCache {
    inner: Mutex<LruCache<(String, String), CachedChunk>>,
    ttl: Duration,
    /// term → (窗口内写入数, 窗口起点)。
    write_counts: Mutex<HashMap<String, (u32, Instant)>>,
    invalid_threshold: u32,
}

impl TermCache {
    /// `ttl` 为 TTL 兜底过期时长；`invalid_threshold` 为 1 秒窗口写计数失效阈值。
    pub fn new(max_entries: usize, ttl: Duration, invalid_threshold: u32) -> Self {
        Self {
            inner: Mutex::new(LruCache::new(
                NonZeroUsize::new(max_entries.max(1)).unwrap(),
            )),
            ttl,
            write_counts: Mutex::new(HashMap::new()),
            invalid_threshold,
        }
    }

    /// 命中查询：TTL 内返回缓存的节点 Chunk；过期条目视为未命中并移除。
    pub fn get(&self, node: &str, term: &str) -> Option<RoaringBitmap> {
        let mut inner = self.inner.lock().unwrap();
        let key = (node.to_string(), term.to_string());
        match inner.get(&key) {
            Some(c) if c.at.elapsed() < self.ttl => Some(c.bitmap.clone()),
            Some(_) => {
                inner.pop(&key);
                None
            }
            None => None,
        }
    }

    /// 回填缓存。
    pub fn insert(&self, node: &str, term: &str, bitmap: RoaringBitmap) {
        let key = (node.to_string(), term.to_string());
        self.inner.lock().unwrap().put(
            key,
            CachedChunk {
                bitmap,
                at: Instant::now(),
            },
        );
    }

    /// 写入路径记录：某 Term 1 秒窗口内写入超阈值 → 主动失效该 Term 全部节点的缓存。
    pub fn record_write(&self, term: &str) {
        let now = Instant::now();
        let mut counts = self.write_counts.lock().unwrap();
        let (count, window_start) = counts.get(term).copied().unwrap_or((0, now));
        let (count, window_start) = if now.duration_since(window_start) > WRITE_WINDOW {
            (1, now)
        } else {
            (count + 1, window_start)
        };
        if count > self.invalid_threshold {
            counts.remove(term);
            drop(counts);
            self.invalidate(term);
            return;
        }
        counts.insert(term.to_string(), (count, window_start));
    }

    /// 主动失效某 Term（全部节点的该 term 缓存）。
    pub fn invalidate(&self, term: &str) {
        let mut inner = self.inner.lock().unwrap();
        // LRU 遍历移除所有该 term 条目（key 第二元素匹配）
        let keys: Vec<(String, String)> = inner.iter().map(|(k, _)| k.clone()).collect();
        for k in keys {
            if k.1 == term {
                inner.pop(&k);
            }
        }
    }

    /// 当前缓存条目数（监控 / 测试）。
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tc() -> TermCache {
        TermCache::new(1000, Duration::from_millis(60), 3)
    }

    #[test]
    fn hit_returns_cached_chunk() {
        let cache = tc();
        assert!(cache.get("n1", "status=active").is_none());
        cache.insert("n1", "status=active", RoaringBitmap::from_iter([1, 2, 3]));
        let hit = cache.get("n1", "status=active").unwrap();
        assert_eq!(hit, RoaringBitmap::from_iter([1, 2, 3]));
        // 不同节点 / 不同 term 独立
        assert!(cache.get("n2", "status=active").is_none());
        assert!(cache.get("n1", "status=pending").is_none());
    }

    #[test]
    fn ttl_expiry_revalidates() {
        let cache = TermCache::new(1000, Duration::from_millis(20), 3);
        cache.insert("n1", "t=1", RoaringBitmap::from_iter([1]));
        assert!(cache.get("n1", "t=1").is_some());
        std::thread::sleep(Duration::from_millis(40));
        assert!(cache.get("n1", "t=1").is_none(), "TTL 过期应重新拉取");
        assert_eq!(cache.len(), 0, "过期条目应被移除");
    }

    #[test]
    fn write_count_over_threshold_invalidates() {
        let cache = tc();
        cache.insert("n1", "status=active", RoaringBitmap::from_iter([1]));
        cache.insert("n2", "status=active", RoaringBitmap::from_iter([2]));
        // 阈值 3：第 4 次写入触发失效
        for _ in 0..4 {
            cache.record_write("status=active");
        }
        assert!(
            cache.get("n1", "status=active").is_none(),
            "写超阈值应主动失效该 term"
        );
        assert!(cache.get("n2", "status=active").is_none());
        // 其他 term 不受影响
        cache.insert("n1", "status=pending", RoaringBitmap::from_iter([9]));
        assert!(cache.get("n1", "status=pending").is_some());
    }

    #[test]
    fn write_count_window_resets_after_second() {
        let cache = tc();
        cache.insert("n1", "t=1", RoaringBitmap::from_iter([1]));
        // 第一秒窗口内写入 2 次（未超阈值 3）
        cache.record_write("t=1");
        cache.record_write("t=1");
        assert!(cache.get("n1", "t=1").is_some());
        // 超过 1 秒窗口后重新计数：再写 1 次也不应误失效
        std::thread::sleep(Duration::from_millis(20));
        cache.record_write("t=1");
        assert!(cache.get("n1", "t=1").is_some());
    }

    #[test]
    fn lru_evicts_oldest_entries() {
        let cache = TermCache::new(2, Duration::from_secs(60), 100);
        cache.insert("n1", "a=1", RoaringBitmap::from_iter([1]));
        cache.insert("n1", "b=2", RoaringBitmap::from_iter([2]));
        assert_eq!(cache.len(), 2);
        cache.insert("n1", "c=3", RoaringBitmap::from_iter([3]));
        assert_eq!(cache.len(), 2, "LRU 容量 2，最旧条目被淘汰");
        assert!(cache.get("n1", "a=1").is_none());
        assert!(cache.get("n1", "b=2").is_some());
        assert!(cache.get("n1", "c=3").is_some());
    }
}
