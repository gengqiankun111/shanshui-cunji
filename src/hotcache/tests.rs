//! HotCache 单元测试（原 hotcache.rs `#[cfg(test)] mod tests` 原样迁移，
//! 覆盖：读写往返 / 大文档不缓存 / 失效 / LRU 淘汰 / P41 批量回归 / 热点晋升 / 并发读写）。

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

// ---------- Task-027：TinyLFU 读回填准入 ----------

#[test]
fn task027_scan_single_touch_never_backfills() {
    // 扫描型：每个 docid 只读一次 → 首访只入门卫（不计数不入缓存），缓存不被全表扫污染
    let c = HotCache::new(small_cfg(16));
    for i in 0..2000u64 {
        c.read_backfill(i, format!("doc-{i}").into_bytes());
    }
    assert_eq!(c.len(), 0, "首访单次访问不应回填缓存");
}

#[test]
fn task027_hot_doc_admitted_after_repeats() {
    let c = HotCache::new(small_cfg(16));
    // 首访入门卫；第 5 次访问起（est≥4）准入回填
    for _ in 0..6 {
        c.read_backfill(7, b"hot".to_vec());
    }
    assert!(c.get(7).is_some(), "重复热读应准入回填并命中");
}

#[test]
fn task027_write_halves_heat_and_needs_reheat() {
    let c = HotCache::new(small_cfg(16));
    for _ in 0..6 {
        c.read_backfill(9, b"hot".to_vec());
    }
    assert!(c.get(9).is_some());
    // 写操作：计数减半 + 清 doorkeeper → 一次读不再准入（重新积累热度），但部分热度保留
    c.put(9, b"new".to_vec());
    assert!(c.get(9).is_some(), "写后直写回填应命中");
    c.invalidate(9);
    assert!(c.get(9).is_none());
    // 写后首访只入门卫（不入缓存）
    c.read_backfill(9, b"after".to_vec());
    assert!(c.get(9).is_none(), "写后首访不应立即回填（需重新热读）");
}

#[test]
fn task027_disabled_falls_back_to_unconditional() {
    let mut cfg = small_cfg(16);
    cfg.tiny_lfu_enabled = false;
    let c = HotCache::new(cfg);
    c.read_backfill(42, b"v".to_vec());
    assert!(c.get(42).is_some(), "关闭准入应回退无条件读回填");
}

#[test]
fn task027_tinylfu_decay_keeps_hot_and_drops_cold() {
    use super::tinylfu::TinyLfu;
    // 小衰减窗口（8 次 Record）验证：热 key 持续被识别；冷 key（单次）恒 0
    let lf = TinyLfu::new(8);
    let mut hot_admits = 0u32;
    for _ in 0..60 {
        if lf.record_admit(1, 4) {
            hot_admits += 1;
        }
    }
    assert!(hot_admits >= 2, "衰减窗口下热 key 仍应多次准入（实际 {hot_admits}）");
    // 冷/扫描 key：每次都是新 key → 只入门卫，恒不入 CMS
    for i in 0..500u64 {
        assert!(!lf.record_admit(1000 + i, 4), "首次访问不应准入（key{}）", 1000 + i);
    }
}
