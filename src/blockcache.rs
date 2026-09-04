//! 块缓存（LRU + 自适应表分区水位，P3-C）（design 4.4 / development 步骤 6）。
//!
//! - 高频访问的数据块驻留内存，命中即免磁盘 IO；
//! - **P3-C 优化**：按表分区（table_id → LRU 子缓存），热点表自动分配更多缓存；
//! - 缓存键 = (SST 文件路径, 块偏移)，因同一文件各块独立且偏移唯一；
//! - 容量按字节预算（`blockcache.max_memory_mb`），LRU 淘汰最久未用；
//! - 读放大防护：块级粒度，不缓存整文件。
//! - 自适应分配：访问频次高的表自动获得更多缓存配额，冷表配额收缩。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use dashmap::DashMap;
use lru::LruCache;

/// 缓存键：SST 文件 + 块在文件内的偏移。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockCacheKey {
    pub file: PathBuf,
    pub offset: u64,
    /// P3-C：表 ID，用于按表分区
    pub table_id: u16,
}

impl BlockCacheKey {
    pub fn new(file: PathBuf, offset: u64, table_id: u16) -> Self {
        Self { file, offset, table_id }
    }
}

/// 单表分区缓存。
struct TablePartition {
    cache: LruCache<BlockCacheKey, Vec<u8>>,
    /// 当前估算占用字节。
    used_bytes: usize,
    /// 访问计数（命中次数，用于自适应分配）。
    access_count: u64,
}

/// LRU 块缓存 + P3-C 自适应按表分区水位。
pub struct BlockCache {
    /// 按表分区缓存：table_id → 分区缓存。
    partitions: Mutex<HashMap<u16, TablePartition>>,
    /// 全局访问统计：table_id → 累计命中次数（无锁，供自适应重分配参考）。
    access_stats: DashMap<u16, u64>,
    /// 总预算字节。
    total_budget_bytes: usize,
    /// 块大小（用于估算每条目开销）。
    block_size: usize,
    /// 每条目的估算字节（块 + key 开销）。
    per_entry_estimate: usize,
    /// 软水位：触发全局重分配阈值（占总预算比例）。
    eviction_high_water: f64,
}

impl BlockCache {
    /// `max_memory_bytes`：缓存字节预算。
    pub fn new(max_memory_bytes: usize, block_size: usize) -> Self {
        let per_entry = block_size.max(1) + 64; // 块体 + key 与链表开销估算
        Self {
            partitions: Mutex::new(HashMap::new()),
            access_stats: DashMap::new(),
            total_budget_bytes: max_memory_bytes,
            block_size,
            per_entry_estimate: per_entry,
            eviction_high_water: 0.85,
        }
    }

    /// 获取或创建表的缓存分区。
    fn get_or_create_partition<'a>(partitions: &'a mut HashMap<u16, TablePartition>, table_id: u16) -> &'a mut TablePartition {
        partitions.entry(table_id).or_insert_with(|| {
            // 新表初始分配：按预算比例均分（空表分区大小 = 0，稍后自适应分配）
            TablePartition {
                cache: LruCache::unbounded(), // 条目数由字节预算控制
                used_bytes: 0,
                access_count: 0,
            }
        })
    }

    pub fn get(&self, key: &BlockCacheKey) -> Option<Vec<u8>> {
        let mut partitions = self.partitions.lock().unwrap();
        let partition = partitions.get_mut(&key.table_id)?;
        let result = partition.cache.get(key).cloned();
        if result.is_some() {
            partition.access_count += 1;
            // 无锁记录全局统计
            self.access_stats
                .entry(key.table_id)
                .and_modify(|c| *c = c.saturating_add(1))
                .or_insert(1);
        }
        result
    }

    pub fn put(&self, key: BlockCacheKey, block: Vec<u8>) {
        let block_bytes = self.per_entry_estimate; // 估算
        let mut partitions = self.partitions.lock().unwrap();
        let partition = Self::get_or_create_partition(&mut partitions, key.table_id);
        // 淘汰旧值（如有）
        if let Some(old) = partition.cache.get(&key) {
            partition.used_bytes = partition.used_bytes.saturating_sub(old.len().max(1) + 64);
        }
        partition.cache.put(key, block.clone());
        partition.used_bytes += block_bytes;
        // P3-C：全局超预算 → 触发自适应重分配（淘汰最冷表条目）
        self.adaptive_evict(&mut partitions);
    }

    /// P3-C 自适应淘汰：按表访问频次分配预算，冷表多淘汰。
    fn adaptive_evict(&self, partitions: &mut HashMap<u16, TablePartition>) {
        let total_used: usize = partitions.values().map(|p| p.used_bytes).sum();
        if total_used <= self.total_budget_bytes {
            return;
        }
        // 需要淘汰超预算部分
        let excess = total_used.saturating_sub(self.total_budget_bytes);
        // 收集各表访问统计，按 access_count 排序（冷表优先淘汰）
        let mut table_order: Vec<(u16, u64, usize)> = partitions
            .iter()
            .map(|(tid, p)| {
                let stats = self.access_stats.get(tid).map(|e| *e.value()).unwrap_or(0);
                (*tid, stats, p.used_bytes)
            })
            .collect();
        // 升序按访问计数（冷表在前），同访问计数按 used_bytes（大表优先）
        table_order.sort_by(|a, b| a.1.cmp(&b.1).then(b.2.cmp(&a.2)));
        let mut evicted = 0usize;
        for (tid, _, _) in &table_order {
            if evicted >= excess {
                break;
            }
            let partition = match partitions.get_mut(tid) {
                Some(p) => p,
                None => continue,
            };
            // 从该表淘汰条目，直到该表降为均值或已淘汰足够
            while evicted < excess {
                let victim = match partition.cache.peek_lru() {
                    Some((k, _)) => k.clone(),
                    None => break,
                };
                if let Some(old) = partition.cache.pop(&victim) {
                    let sz = old.len().max(1) + 64;
                    partition.used_bytes = partition.used_bytes.saturating_sub(sz);
                    evicted = evicted.saturating_add(sz);
                } else {
                    break;
                }
            }
        }
    }

    /// 当前缓存条目数（所有分区累计）。
    pub fn len(&self) -> usize {
        let partitions = self.partitions.lock().unwrap();
        partitions.values().map(|p| p.cache.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 当前占用字节估算（所有分区累计）。
    pub fn used_bytes(&self) -> usize {
        let partitions = self.partitions.lock().unwrap();
        partitions.values().map(|p| p.used_bytes).sum()
    }

    /// 清空（内存紧急回收 / 测试）。
    pub fn clear(&self) {
        let mut partitions = self.partitions.lock().unwrap();
        partitions.clear();
        self.access_stats.clear();
    }

    /// 返回活跃表分区数（监控/测试）。
    pub fn partition_count(&self) -> usize {
        self.partitions.lock().unwrap().len()
    }

    /// 返回某表分区当前占用字节（监控/测试）。
    pub fn table_used_bytes(&self, table_id: u16) -> usize {
        let partitions = self.partitions.lock().unwrap();
        partitions.get(&table_id).map(|p| p.used_bytes).unwrap_or(0)
    }

    /// 返回某表分区访问计数（监控/测试）。
    pub fn table_access_count(&self, table_id: u16) -> u64 {
        self.access_stats.get(&table_id).map(|e| *e.value()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(i: u64, table_id: u16) -> BlockCacheKey {
        BlockCacheKey::new(PathBuf::from(format!("f{i}.sst")), i * 100, table_id)
    }

    #[test]
    fn put_get_roundtrip() {
        let c = BlockCache::new(1024 * 1024, 16 * 1024);
        c.put(key(1, 0), b"block-data".to_vec());
        assert_eq!(c.get(&key(1, 0)).unwrap(), b"block-data");
    }

    #[test]
    fn adaptive_evicts_cold_table_first() {
        // 预算 2KB：只能容纳 1 个条目（per_entry ≈ 1088）
        let c = BlockCache::new(2 * 1024, 1024);
        // 表 0：3 次访问（热表）
        c.put(key(1, 0), vec![0u8; 1024]);
        c.get(&key(1, 0)); // 命中+1
        c.get(&key(1, 0)); // 命中+1
        c.get(&key(1, 0)); // 命中+1
        // 表 1：0 次访问（冷表）
        c.put(key(2, 1), vec![0u8; 1024]);
        // 预算不足 → 应淘汰冷表（表1）条目
        assert!(c.get(&key(1, 0)).is_some(), "热表条目应保留");
        assert!(c.get(&key(2, 1)).is_none(), "冷表条目应被淘汰");
    }

    #[test]
    fn multi_table_isolation() {
        let c = BlockCache::new(1024 * 1024, 16 * 1024);
        c.put(key(1, 0), b"table0-data".to_vec());
        c.put(key(2, 1), b"table1-data".to_vec());
        assert_eq!(c.get(&key(1, 0)).unwrap(), b"table0-data");
        assert_eq!(c.get(&key(2, 1)).unwrap(), b"table1-data");
        assert_eq!(c.partition_count(), 2);
    }

    #[test]
    fn clear_empties() {
        let c = BlockCache::new(1024 * 1024, 1024);
        c.put(key(1, 0), b"x".to_vec());
        c.clear();
        assert!(c.is_empty());
        assert_eq!(c.partition_count(), 0);
    }

    #[test]
    fn hot_table_gets_more_cache() {
        let c = BlockCache::new(4 * 1024, 1024); // per_entry ≈ 1088, budget ≈ 3 entries
        // 表 0（热）：多次访问
        for i in 0..3u64 {
            c.put(key(i, 0), vec![0u8; 1024]);
            c.get(&key(i, 0));
        }
        // 表 1（冷）：放入 3 个条目，但应被淘汰
        for i in 0..3u64 {
            c.put(key(10 + i, 1), vec![0u8; 1024]);
        }
        // 热表 0 条目应保留，冷表 1 条目应被淘汰
        let table0 = (0..3u64).filter(|i| c.get(&key(*i, 0)).is_some()).count();
        let table1 = (0..3u64).filter(|i| c.get(&key(10 + i, 1)).is_some()).count();
        assert!(table0 > table1, "热表应保留更多条目: table0={table0} table1={table1}");
    }
}
