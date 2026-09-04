//! 条目 / 内存水位 / 热度统计查询与清空（监控、预热判断、测试）。
//!
//! - 条目数：`len` / `is_empty` / `protected_len`（保护区条目数）；
//! - 内存水位：`used_bytes`（主缓存 + 保护区当前占用字节）；
//! - 热度：`promotions`（晋升累计次数）/ `access_count`（某 docid 访问计数）；
//! - `clear`：紧急内存回收（写锁独占，全量清空）。

use super::HotCache;

impl HotCache {
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
