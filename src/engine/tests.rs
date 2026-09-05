//! 文档引擎全量单元测试（reconstruct.md engine/tests.rs）：原 engine.rs 底部 `mod tests`
//! 整体外移，入口见 mod.rs `#[cfg(test)] mod tests;`。测试内部依赖以 pub(crate) 提升 + 显式 use。

use super::*;
use crate::config::model::Config;
use crate::optimizer::QuerySpec;

    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;

    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let name = format!("eng-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    /// Task-028：cidx 存量补齐（open 期重建）——无 composite_indexes 写入的存量库，
    /// 后加声明重开 → 前缀查询不再静默空；签名变更/重复重开不丢行。
    #[test]
    fn task028_cidx_rebuild_on_open_after_config_add() {
        let dir = tmp();
        // 阶段 1：无 composite_indexes 配置写入存量（模拟旧库/配置后加）
        let cfg0 = Config::default();
        let exp_active_bj;
        {
            let mut e0 = Engine::open(&dir, &cfg0).unwrap();
            let mut cnt = 0usize;
            for i in 0..2000u64 {
                let status = if i % 3 == 0 { "active" } else { "closed" };
                let region = if i % 2 == 0 { "beijing" } else { "shanghai" };
                let doc = serde_json::to_vec(&serde_json::json!({
                    "status": status, "region": region, "note": format!("n{i}")
                }))
                .unwrap();
                let t: &[&str] = &[];
                e0.put(i, doc, t).unwrap();
                if status == "active" && region == "beijing" {
                    cnt += 1;
                }
            }
            e0.flush_wal().unwrap();
            exp_active_bj = cnt;
        }
        assert!(exp_active_bj > 0);
        // 阶段 2：声明 (status,region) 重开 → open 期重建 → 前缀查询非空且计数正确
        let mut cfg2 = Config::default();
        cfg2.storage.composite_indexes = vec![vec!["status".into(), "region".into()]];
        {
            let e2 = Engine::open(&dir, &cfg2).unwrap();
            let hits = e2.query_by_composite_prefix(&[b"active", b"beijing"]).unwrap();
            assert_eq!(hits.len(), exp_active_bj, "cidx 重建后前缀查询计数须等于存量");
            assert!(hits.len() > 0, "Task-028 修复点：重建后不得静默空");
            let sig = std::fs::read_to_string(dir.join("cidx.sig")).unwrap();
            assert_eq!(sig, "status.region", "签名标记应落盘");
        }
        // 阶段 3：正常重开（标记一致 + cidx 非空）→ 结果不回退
        {
            let e3 = Engine::open(&dir, &cfg2).unwrap();
            let hits3 = e3.query_by_composite_prefix(&[b"active", b"beijing"]).unwrap();
            assert_eq!(hits3.len(), exp_active_bj, "正常重开后结果不回退");
        }
        // 阶段 4：索引字段变更（签名不符）→ 触发重建并覆盖新索引
        let mut cfg4 = Config::default();
        cfg4.storage.composite_indexes = vec![vec!["note".into()]];
        {
            let e4 = Engine::open(&dir, &cfg4).unwrap();
            let sig = std::fs::read_to_string(dir.join("cidx.sig")).unwrap();
            assert_eq!(sig, "note", "签名应更新为新索引");
            let hits = e4.query_by_composite_prefix(&[b"n1999"]).unwrap();
            assert_eq!(hits.len(), 1);
        }
    }

    #[test]
    fn shard_metrics_attach_and_render() {
        // 10 亿库阶段 D：挂载分片指标 → 水位上报 → /metrics 渲染 + 预警
        let cfg = Config::default();
        let e = Engine::open(&tmp(), &cfg).unwrap();
        assert!(e.shard_metrics_render().is_empty(), "未挂载渲染为空");
        e.attach_shard_metrics(10);
        e.update_shard_watermark(0, 100_000_000); // 每分片 1 亿（10 亿库）
        e.record_shard_write(0);
        e.record_shard_read(0);
        let out = e.shard_metrics_render();
        assert!(out.contains("shanshui_shard_docid_watermark{shard=\"0\"} 100000000"));
        assert!(out.contains("shanshui_shard_writes_total{shard=\"0\"} 1"));
        assert!(e.shard_watermark_alerts().is_empty(), "10 亿库水位无预警");
        // 高水位（≈82%）→ Warn 预警
        e.update_shard_watermark(1, 900_000_000_000);
        let alerts = e.shard_watermark_alerts();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].0, 1);
        assert_eq!(alerts[0].1, crate::shard_metrics::WatermarkLevel::Warn);
    }

    #[test]
    fn compact_targets_run_matches_engine_compact() {
        // P72（无锁合并）：`Engine::compaction_targets` + `CompactTargets::run`（worker 无锁路径）
        // 与 `Engine::compact`（读锁内串行）收敛结果一致——多列族压力 + 删除位图过滤双路径。
        let mut cfg = Config::default();
        cfg.memtable.max_size_mb = 1; // 小 MemTable → 写入快速 flush 多段
        cfg.storage.l0_stall_threshold = 2; // 低 L0 阈值 → 2 段即触发合并
        let mut e1 = Engine::open(&tmp(), &cfg).unwrap();
        let mut e2 = Engine::open(&tmp(), &cfg).unwrap();
        let val = vec![b'x'; 1024];
        for seg in 0..4u64 {
            for i in seg * 500..seg * 500 + 500 {
                let t: &[&str] = &["tag_a"];
                e1.put(i, val.clone(), t).unwrap();
                e2.put(i, val.clone(), t).unwrap();
            }
        }
        // 删除一部分（删除位图开启）→ 合并需物理丢弃
        for i in (0..2_000u64).step_by(3) {
            e1.delete(i).unwrap();
            e2.delete(i).unwrap();
        }
        e1.flush_wal().unwrap();
        e2.flush_wal().unwrap();
        // e1：Engine::compact 读锁路径收敛；e2：无锁路径（compaction_targets 循环 run）收敛
        while e1.needs_compact() {
            let _ = e1.compact().unwrap();
        }
        while e2.needs_compact() {
            let Some(t) = e2.compaction_targets() else { break };
            t.run().unwrap();
        }
        assert!(!e1.needs_compact());
        assert!(!e2.needs_compact());
        // 收敛后数据一致（存活 docid 全部命中；已删不可见）
        for i in 0..2_000u64 {
            let v1 = e1.get(i).unwrap();
            let v2 = e2.get(i).unwrap();
            assert_eq!(v1, v2, "docid={i}");
        }
        // 段数收敛一致（同输入 → 同压实结果）
        assert_eq!(e1.primary_l0_count(), e2.primary_l0_count());
        assert_eq!(e1.primary.sst_count(), e2.primary.sst_count());
    }

    #[test]
    fn persist_manifest_reflects_memory_snapshot_only() {
        // P73：persist_manifest 基于内存快照（ssts ArcSwap）重建清单——磁盘上存在但不在
        // 内存快照中的文件（如无锁合并并发时"正在写入的半写段"）不得写入 manifest，
        // 否则重启加载失败。确定性验证：放置幽灵段后 flush，manifest 不含它。
        let mut cfg = Config::default();
        cfg.memtable.max_size_mb = 1; // 小 MemTable → flush 触发 manifest 重写
        let data_dir = tmp();
        let mut engine = Engine::open(&data_dir, &cfg).unwrap();
        for i in 0..2000u64 {
            engine.put(i, format!("v{i}").into_bytes(), &[]).unwrap();
        }
        engine.flush_primary().unwrap();
        // 磁盘放"幽灵"段（模拟并发写入中的半写段 / 残留文件）
        let ghost = data_dir.join("primary").join("sst-99999999.sst");
        std::fs::write(&ghost, b"partial-written-not-in-snapshot").unwrap();
        // 再写并 flush → persist 重写 manifest
        for i in 2000..3000u64 {
            engine.put(i, format!("v{i}").into_bytes(), &[]).unwrap();
        }
        engine.flush_primary().unwrap();
        let m: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(data_dir.join("primary").join("manifest.json")).unwrap(),
        )
        .unwrap();
        let files = m["sst_files"].as_array().expect("manifest sst_files");
        assert!(
            !files
                .iter()
                .any(|f| f.as_str().unwrap().contains("99999999")),
            "manifest 不得引用非内存快照段（幽灵段）"
        );
        assert!(files.len() >= 2, "flush 段应在 manifest 中");
        // 重开：数据完整（manifest 与磁盘一致可加载）
        drop(engine);
        let engine = Engine::open(&data_dir, &cfg).unwrap();
        for i in (0..3000u64).step_by(997) {
            let v = engine.get(i).unwrap();
            assert_eq!(v.as_deref(), Some(format!("v{i}").as_bytes()), "docid={i}");
        }
    }

    #[test]
    fn scan_rate_limit_slows_stream() {
        // design 20.5：导出共享后台 IO 限速——启用限速后顺序扫描显著变慢（Token Bucket 生效），
        // 关闭后恢复；前台点查不受限速影响（scan_limiter 只作用于 scan_stream）。
        let mut cfg = Config::default();
        cfg.memtable.max_size_mb = 1;
        let mut engine = Engine::open(&tmp(), &cfg).unwrap();
        for i in 0..10u64 {
            engine.put(i, vec![b'x'; 200_000], &[]).unwrap(); // 200KB × 10 = 2MB
        }
        engine.flush_primary().unwrap();
        // 无限制：扫描快
        let t0 = std::time::Instant::now();
        let mut n = 0u64;
        engine
            .scan_stream(None, None, |_, v| {
                n += 1;
                assert_eq!(v.len(), 200_000);
                Ok(true)
            })
            .unwrap();
        let fast = t0.elapsed();
        assert_eq!(n, 10);
        // 限速 1MB/s：2MB 数据（1s 突发桶 + 1MB 需补桶）→ 显著慢于无限速
        engine.set_scan_rate_limit(1);
        let t1 = std::time::Instant::now();
        engine.scan_stream(None, None, |_, _| Ok(true)).unwrap();
        let slow = t1.elapsed();
        assert!(slow > fast, "限速后扫描应更慢（fast={fast:?} slow={slow:?}）");
        assert!(
            slow.as_millis() >= 300,
            "限速 1MB/s 扫描 2MB 应 ≥300ms（实际 {slow:?}）"
        );
        // 关闭限速恢复
        engine.set_scan_rate_limit(0);
        let t2 = std::time::Instant::now();
        engine.scan_stream(None, None, |_, _| Ok(true)).unwrap();
        assert!(t2.elapsed() < slow, "关闭限速后应恢复快速扫描");
    }

    fn cfg() -> Config {
        let mut c = Config::default();
        c.sstable.compression = "none".into();
        c
    }

    fn gc_cfg(window_us: u64) -> Config {
        // 组提交（M8）机制本体测试：Task-026 默认开启 per-CPU WAL 会替代组提交线程 →
        // 这些 legacy 机制测试显式关闭 per-CPU（Engine 全量默认路径另由 percpu_tests 覆盖）
        let mut c = cfg();
        c.storage.per_cpu_enabled = false;
        c.storage.group_commit_us = window_us;
        c
    }

    // ---------- D/E/F LSM 事务三阶段 ----------

    #[test]
    fn write_batch_atomic_commit_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        let mut wb = crate::txn::WriteBatch::new();
        wb.put(1, b"doc-1".to_vec(), vec!["t1".into()]);
        wb.put(2, b"doc-2".to_vec(), vec!["t2".into()]);
        wb.delete(3); // 删除不存在的 docid 应合法（幂等）
        e.write(&wb).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"doc-1");
        assert_eq!(e.get(2).unwrap().unwrap(), b"doc-2");
        assert!(e.get(3).unwrap().is_none());
    }

    #[test]
    fn write_batch_validate_rejects_zero_docid_before_apply() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        let mut wb = crate::txn::WriteBatch::new();
        wb.put(0, b"x".to_vec(), vec![]);
        wb.put(10, b"ok".to_vec(), vec![]);
        assert!(e.write(&wb).is_err(), "预校验失败 → 拒绝提交");
        assert!(e.get(10).unwrap().is_none(), "预校验失败不得应用任何 op（失败回滚语义）");
    }

    #[test]
    fn write_batch_rollback_discards_ops() {
        let mut wb = crate::txn::WriteBatch::new();
        wb.put(1, b"x".to_vec(), vec![]);
        wb.delete(2);
        wb.rollback();
        assert!(wb.is_empty());
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.write(&wb).unwrap(); // 空批合法提交
        assert!(e.get(1).unwrap().is_none());
    }

    #[test]
    fn txn_rr_snapshot_read_ignores_concurrent_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v0".to_vec(), &["t"]).unwrap();
        // MemTable 不保留多版本（design 4.7 已知局限）：快照读需旧版本已落 SST
        e.flush_primary().unwrap();
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        // 快照后并发写入（模拟其他事务已提交）
        e.put(1, b"v1".to_vec(), &["t"]).unwrap();
        let got = e.txn_get(&mut txn, 1).unwrap();
        assert_eq!(got.unwrap(), b"v0", "RR 应读事务开始前的快照值");
        e.txn_rollback(txn);
    }

    #[test]
    fn txn_rr_snapshot_sees_old_version_in_memtable_without_flush() {
        // S 项（严格 MVCC）：旧实现快照读需旧版本已落 SST（MemTable 仅保最新）——
        // 事务活跃期 + 并发写同 key 且未 flush 时，快照读会读到新版本（正确性缺陷）。
        // 修复后 MemTable 保留版本链，未刷盘也能读到快照点版本。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v0".to_vec(), &["t"]).unwrap();
        // 不 flush：v0 留在 MemTable
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        // 快照后并发写同 key（仍不 flush）→ MemTable 出现新版本
        e.put(1, b"v1".to_vec(), &["t"]).unwrap();
        let got = e.txn_get(&mut txn, 1).unwrap();
        assert_eq!(
            got.unwrap(),
            b"v0",
            "RR 快照应读到旧版本（无需 flush 落 SST）"
        );
        // 同事务写覆盖（read_own 优先）
        txn.put(1, b"own".to_vec(), vec![]);
        assert_eq!(e.txn_get(&mut txn, 1).unwrap().unwrap(), b"own");
        e.txn_rollback(txn);
        // 提交后最新可见
        assert_eq!(e.get(1).unwrap().unwrap(), b"v1");
    }

    #[test]
    fn txn_snapshot_cache_repeated_get_hits_without_stale() {
        // T 项：RR 快照读事务内点查小缓存——同 key 二次读直达（snap_get 命中）且结果一致
        // （快照 seq 恒定）；RC 不缓存（读最新语义）；事务 drop 即弃（重新 begin 缓存为空）。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v0".to_vec(), &["t"]).unwrap();
        e.flush_primary().unwrap();

        // RR：第一次读写入缓存，第二次读命中缓存（外部已改但快照一致）
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        assert_eq!(txn.snap_get(1), None, "缓存初始为空");
        let first = e.txn_get(&mut txn, 1).unwrap().unwrap();
        assert_eq!(first, b"v0");
        assert_eq!(txn.snap_get(1), Some(Some(b"v0".to_vec())), "首读后已缓存");
        // 外部并发写（seq > 快照）→ 二次读仍走缓存返回快照值
        e.put(1, b"v1".to_vec(), &["t"]).unwrap();
        assert_eq!(e.txn_get(&mut txn, 1).unwrap().unwrap(), b"v0", "缓存命中=快照一致");
        assert_eq!(txn.snap_get(2), None, "未读过的 key 不在缓存");
        // 同事务写后读 → read_own 优先于缓存
        txn.put(1, b"own".to_vec(), vec![]);
        assert_eq!(e.txn_get(&mut txn, 1).unwrap().unwrap(), b"own");
        e.txn_rollback(txn);

        // 新事务缓存为空（随 Transaction drop 即弃）
        let mut txn2 = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        assert_eq!(txn2.snap_get(1), None, "新事务缓存应为空");
        assert_eq!(e.txn_get(&mut txn2, 1).unwrap().unwrap(), b"v1", "读最新已提交");
        e.txn_rollback(txn2);

        // RC：不缓存（每次读最新，缓存会破坏语义）
        let mut txn3 = e.txn_begin(crate::txn::Isolation::ReadCommitted);
        assert_eq!(e.txn_get(&mut txn3, 1).unwrap().unwrap(), b"v1");
        assert_eq!(txn3.snap_get(1), None, "RC 不写缓存");
        e.txn_rollback(txn3);
    }

    #[test]
    fn scan_range_txn_snapshot_filter_and_own_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=5u64 {
            e.put(i, format!("v0-{i}").into_bytes(), &["t"]).unwrap();
        }
        e.flush_primary().unwrap(); // 旧版本落 SST（MemTable 不保留多版本）
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        // 快照后并发写 docid 3 → 扫描应显示快照值 v0-3（隔离并发写）
        e.put(3, b"v1-3".to_vec(), &["t"]).unwrap();
        // 事务内写 docid 2 / 事务内删除 docid 4 → 扫描应覆盖/排除
        txn.put(2, b"own-2".to_vec(), vec!["t".into()]);
        txn.delete(4);
        let rows = e.scan_range_txn(&mut txn, Some(1), Some(5)).unwrap();
        let map: std::collections::HashMap<u64, Vec<u8>> = rows.into_iter().collect();
        assert_eq!(map.get(&1).unwrap(), b"v0-1");
        assert_eq!(map.get(&2).unwrap(), b"own-2", "同事务写应覆盖扫描结果");
        assert_eq!(map.get(&3).unwrap(), b"v0-3", "快照隔离：并发写不可见");
        assert_eq!(map.len(), 4, "事务内删除 docid 4 应从扫描排除");
        assert!(!map.contains_key(&4));
        e.txn_rollback(txn);
    }

    #[test]
    fn txn_scan_respects_deletion_bitmap_revival_and_insert() {
        // Ex-8.10：事务扫描删除位图过滤（与 txn_get/get_at 对齐）+ 事务内自写复活已删 docid / 新插入
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=10u64 {
            e.put(i, format!("v-{i}").into_bytes(), &["t"]).unwrap();
        }
        e.flush_primary().unwrap();
        e.delete(5).unwrap(); // 位图删除
        // (1) 快照扫描排除位图已删（与 txn_get 一致）
        {
            let mut t = e.txn_begin(crate::txn::Isolation::RepeatableRead);
            let rows = e.scan_range_txn(&mut t, None, None).unwrap();
            assert_eq!(rows.len(), 9, "位图已删 docid 5 应从扫描排除");
            assert!(!rows.iter().any(|r| r.0 == 5));
            assert!(e.txn_get(&mut t, 5).unwrap().is_none(), "与 txn_get 语义一致");
            e.txn_rollback(t);
        }
        // (2) 事务内复活已删 docid 5（未提交）→ 扫描应含自写值（位图过滤后 read_own 复活）
        {
            let mut t = e.txn_begin(crate::txn::Isolation::RepeatableRead);
            t.put(5, b"revived".to_vec(), vec!["t".into()]);
            let rows = e.scan_range_txn(&mut t, None, None).unwrap();
            let map: std::collections::HashMap<u64, Vec<u8>> = rows.into_iter().collect();
            assert_eq!(map.len(), 10);
            assert_eq!(map.get(&5).unwrap(), b"revived", "自写复活已删 docid 应可见");
            e.txn_rollback(t);
        }
        // (3) 事务内新插入 docid 11 → 窗口含自写
        {
            let mut t = e.txn_begin(crate::txn::Isolation::RepeatableRead);
            t.put(11, b"new".to_vec(), vec!["t".into()]);
            let rows = e.scan_range_txn(&mut t, Some(9), Some(11)).unwrap();
            let map: std::collections::HashMap<u64, Vec<u8>> = rows.into_iter().collect();
            assert_eq!(map.len(), 3, "docid 9,10 已有 + 11 自写");
            assert_eq!(map.get(&11).unwrap(), b"new");
            e.txn_rollback(t);
        }
    }

    #[test]
    fn txn_range_scan_hides_phantom_after_flush_c4() {
        // 缺陷 B（C4 变体）：快照后他事务插入且已 flush 落 SST——范围快照扫仍须隐藏幻影行
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        // pre：900401 已提交并落盘（快照点 0）
        e.put(
            900401,
            serde_json::json!({"k": 0}).to_string().into_bytes(),
            &[],
        )
        .unwrap();
        e.flush_primary().unwrap();
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        // 他事务在区间内插入 900400（快照后）→ flush 落 SST（幻影落盘）
        e.put(
            900400,
            serde_json::json!({"k": 1}).to_string().into_bytes(),
            &[],
        )
        .unwrap();
        e.flush_primary().unwrap();
        // 范围快照扫：仅 900401（900400 seq 在快照后，即使已落盘也不可见）
        let rows = e.scan_range_txn(&mut txn, Some(900400), Some(900402)).unwrap();
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![900401], "flush 后幻影行仍不可见");
        assert!(e.get(900400).unwrap().is_some(), "最新视图应含他事务插入（写入在）");
        e.txn_rollback(txn);
    }

    #[test]
    fn txn_scan_hides_phantom_multi_segment_heap_c4() {
        // 缺陷 B（C4 多段变体）：>4 源走 heap 归并分支——快照后他事务插入（多段场景）仍须隐藏
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        // 6 段 × 100 行 pre 数据（全量窗口内 → 每段都建迭代器 → mem+6sst > 4 → heap 分支）
        for g in 0..6u64 {
            for i in 0..100u64 {
                let d = g * 100 + i;
                e.put(d, format!("v{d}").into_bytes(), &["t"]).unwrap();
            }
            e.flush_primary().unwrap();
        }
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        // 他事务在快照后插入 900400 → 最新视图可见、快照不可见
        e.put(900400, b"phantom".to_vec(), &["t"]).unwrap();
        let rows = e.scan_range_txn(&mut txn, None, None).unwrap();
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(rows.len(), 600, "快照全量扫描应仅含 6×100 行 pre 数据");
        assert!(!ids.contains(&900400), "heap 分支幻影行 900400 不可见");
        assert!(e.get(900400).unwrap().is_some(), "最新视图应含幻影行（写入在）");
        e.txn_rollback(txn);
    }

    #[test]
    fn txn_range_scan_hides_revived_row_c4() {
        // 缺陷 B（C4 根因）：delete（位图）→ 他事务 put 复活（清位图）后，快照点位于
        // [删除, 复活) 的主事务范围快照扫不得见复活行（回读到复活前旧版本 = 幻影）
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        // 前轮已提交行 900400（历史版本 v1）
        e.put(900400, b"v1".to_vec(), &[]).unwrap();
        // 本轮 case 清理：delete 900400（位图置位 + 版本化 tombstone）
        e.delete(900400).unwrap();
        // pre 900401（BEGIN 前 autocommit 提交，快照可见）
        e.put(900401, b"v0".to_vec(), &[]).unwrap();
        // main BEGIN：快照点在删除之后、复活之前
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        // aux INSERT 复活 900400（put 清位 + 新版本 seq > snapshot）
        e.put(900400, b"v2".to_vec(), &[]).unwrap();
        // 范围快照扫：仅 900401（900400 快照点在删除期 → 不可见）
        let rows = e.scan_range_txn(&mut txn, Some(900400), Some(900402)).unwrap();
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![900401], "复活行在快照(删除期)不可见");
        assert!(e.get(900400).unwrap().is_some(), "最新视图应见复活行 v2");
        e.txn_rollback(txn);
    }

    #[test]
    fn count_all_docs_matches_scan_stream() {
        // 7.100：key-only 免值计数与 scan_stream 全表可见行一致（覆盖 + 删除 + flush 混合）
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 0..5000u64 {
            let doc = serde_json::json!({"k": format!("d{i}"), "n": i});
            e.put(i, serde_json::to_vec(&doc).unwrap(), &["k"]).unwrap();
        }
        // 覆盖（同 docid 二次 put 不增行）+ 删除
        e.put(0, serde_json::to_vec(&serde_json::json!({"k": "d0-v2"})).unwrap(), &["k"]).unwrap();
        e.delete(1).unwrap();
        e.delete(2).unwrap();
        e.delete(3).unwrap();
        let fast = e.count_all_docs().unwrap();
        let mut slow = 0u64;
        e.scan_stream(None, None, |_d, _v| {
            slow += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(fast, slow, "count_keys_range 应与 scan_stream 一致（memtable 期）");
        // flush 后（SST keys-only 解码路径）再验
        e.flush_primary().unwrap();
        let fast2 = e.count_all_docs().unwrap();
        let mut slow2 = 0u64;
        e.scan_stream(None, None, |_d, _v| {
            slow2 += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(fast2, slow2, "count_keys_range 应与 scan_stream 一致（flush 后）");
        assert_eq!(fast, fast2, "flush 前后计数一致");
    }

    #[test]
    fn count_all_docs_incremental_tracks_put_delete_batch_purge() {
        // P1-C：COUNT(*) O(1) 增量记账——首查懒建基线后，put/覆盖/删除/复活/
        // delete_batch/purge/重开全路径与 scan 口径一致
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        assert_eq!(e.count_all_docs().unwrap(), 0, "空库基线 0");
        for i in 0..3000u64 {
            let doc = serde_json::json!({"n": i});
            e.put(i, serde_json::to_vec(&doc).unwrap(), &[]).unwrap();
        }
        assert_eq!(e.count_all_docs().unwrap(), 3000, "3000 行");
        // 覆盖同 docid → 不增
        e.put(5, br#"{"n":99}"#.to_vec(), &[]).unwrap();
        assert_eq!(e.count_all_docs().unwrap(), 3000, "覆盖不增");
        // 单点删除 → 减；删不存在 → 幂等不减
        e.delete(7).unwrap();
        e.delete(8).unwrap();
        assert_eq!(e.count_all_docs().unwrap(), 2998);
        e.delete(999_999).unwrap();
        assert_eq!(e.count_all_docs().unwrap(), 2998, "删不存在幂等");
        // 复活（put 已删 docid）→ 增
        e.put(7, br#"{"n":7}"#.to_vec(), &[]).unwrap();
        assert_eq!(e.count_all_docs().unwrap(), 2999);
        // delete_batch（0..99 除 5/7；其中 8 已删 → 返回 98 项、live 减 97）
        let n = e.delete_batch((0..100).filter(|d| *d != 5 && *d != 7)).unwrap();
        assert_eq!(n, 98);
        assert_eq!(e.count_all_docs().unwrap(), 2902, "delete_batch 增量递减");
        // 与 scan 口径一致
        let mut slow = 0u64;
        e.scan_stream(None, None, |_d, _v| {
            slow += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(e.count_all_docs().unwrap(), slow, "与 scan 可见行一致");
        // flush + 重开（重启懒建基线恢复）
        e.flush_wal().unwrap();
        e.flush_primary().unwrap();
        let before = e.count_all_docs().unwrap();
        drop(e);
        let e2 = Engine::open(dir.path(), &cfg()).unwrap();
        assert_eq!(e2.count_all_docs().unwrap(), before, "重开基线一致");
        // purge → 0 复位，再写增量
        let mut e3 = Engine::open(dir.path(), &cfg()).unwrap();
        e3.purge_all().unwrap();
        assert_eq!(e3.count_all_docs().unwrap(), 0, "purge 后复位 0");
        e3.put(1, br#"{"a":1}"#.to_vec(), &[]).unwrap();
        e3.put(2, br#"{"a":2}"#.to_vec(), &[]).unwrap();
        assert_eq!(e3.count_all_docs().unwrap(), 2, "purge 后续写增量");
    }

    #[test]
    fn gap2_count_o1_fresh_load_empty_open_multi_l0() {
        // 缺口②（P105-②）：**空库打开即播种活跃集**（Some(空)）→ fresh-load（小 memtable
        // 多次 flush → 多 L0 + PAX 块，复现 P105 10万 fresh-load 形态）全程 put/delete/
        // delete_batch 增量记账，首个 COUNT(*) 即 O(1)（活跃集 rank，无需一次性全键扫基线）。
        // 正确性 = keys-only 扫描口径；覆盖覆盖写/删除/复活/多表高位隔离。
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.memtable.max_size_mb = 1; // 小 memtable → 多次 flush 成多 L0 段
        c.storage.hot_fields = vec!["a".into()]; // PAX 块（fresh-load 形态）
        let mut e = Engine::open(dir.path(), &c).unwrap();
        assert!(e.primary.data_empty(), "空库打开时 primary 应无数据");
        for i in 0..5000u64 {
            let doc = serde_json::json!({"a": i, "b": i * 2});
            e.put(i, serde_json::to_vec(&doc).unwrap(), &["a"]).unwrap();
            if i % 1000 == 999 {
                e.flush_primary().unwrap(); // 分段落盘 → 多 L0
            }
        }
        e.flush_primary().unwrap();
        e.delete(3).unwrap();
        e.delete(2999).unwrap();
        e.put(3, br#"{"a":3,"b":0}"#.to_vec(), &["a"]).unwrap(); // 复活
        e.flush_primary().unwrap();
        let mut scan = 0u64;
        e.scan_stream_ids(None, None, |_| {
            scan += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(
            e.count_all_docs().unwrap(),
            scan,
            "空库播种 + load 期增量记账 = keys-only 口径（含删除/复活）"
        );
        let mask = (1u64 << 48) - 1;
        assert_eq!(e.count_docs_range(0, mask).unwrap(), scan, "整表窗口区间基数一致");
        // 多表高位隔离：t7 行不串入默认表窗口；全库含他表
        let tid2 = crate::multitable::table_base(7);
        e.put(tid2 | 1, br#"{"a":1}"#.to_vec(), &["a"]).unwrap();
        e.put(tid2 | 2, br#"{"a":2}"#.to_vec(), &["a"]).unwrap();
        e.delete(tid2 | 1).unwrap();
        assert_eq!(e.count_docs_range(0, mask).unwrap(), scan, "默认表窗口不受他表高位影响");
        assert_eq!(e.count_all_docs().unwrap(), scan + 1, "全库计数含他表存活行");
    }

    #[test]
    fn scan_collapses_within_source_multi_versions() {
        // Ex-8.1（demo range-window 发现的折叠缺口）：同 docid 覆盖写后未 compaction 收敛刷盘，
        // 同源（memtable/SST）连续同 key 多版本行——scan/流式/count 均应折叠为最新版本
        // （修复前：收集 100 行 vs 流式/计数 110 行）。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=100u64 {
            e.put(i, format!("v0-{i}").into_bytes(), &["t"]).unwrap();
        }
        for i in 10..=20u64 {
            e.put(i, format!("v1-{i}").into_bytes(), &["t"]).unwrap();
        }
        // 不 compaction，直接刷盘 → 同 key 新旧两行同落文件
        e.flush_primary().unwrap();
        let c = e.scan_range(None, None).unwrap();
        assert_eq!(c.len(), 100, "scan_range（收集路径）应折叠同源多版本");
        let mut s = 0u64;
        e.scan_stream(None, None, |_d, _v| {
            s += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(s, 100, "scan_stream（流式 merge）应折叠同源同 key 旧版本");
        assert_eq!(e.count_all_docs().unwrap(), 100, "count 应折叠同源同 key 旧版本");
        // 值取最新版本
        let rows: std::collections::HashMap<u64, Vec<u8>> = e.scan_range(None, None).unwrap().into_iter().collect();
        assert_eq!(rows.get(&15).unwrap(), b"v1-15", "覆盖写应返回最新版本");
    }

    #[test]
    fn p91_scan_stream_fields_matches_scan_stream_row_and_pax() {
        // P91：投影列扫描（scan_stream_fields）语义与全量 scan_stream + 按需取列一致——
        // 行式块直通原 JSON；PAX 块（storage.hot_fields）只解请求列并组装子集 JSON。
        // 覆盖 memtable / flush(SST) / 覆盖写 / 删除 / 重开 两布局。
        for pax in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut c = cfg();
            if pax {
                c.storage.hot_fields = vec!["a".into(), "c".into()];
            }
            let mut e = Engine::open(dir.path(), &c).unwrap();
            for i in 0..3000u64 {
                let doc = serde_json::json!({"a": format!("a{i}"), "b": i, "c": i % 7});
                e.put(i, serde_json::to_vec(&doc).unwrap(), &["b"]).unwrap();
            }
            // 覆盖写（同 docid 二次 put）+ 删除（位图）
            e.put(5, br#"{"a":"a5v2","b":-1,"c":99}"#.to_vec(), &["b"]).unwrap();
            e.delete(9).unwrap();
            // 与 scan_stream 全量扫描对比（memtable 期）
            let full: std::collections::HashMap<u64, serde_json::Value> = {
                let mut m = std::collections::HashMap::new();
                e.scan_stream(None, None, |d, v| {
                    m.insert(d, serde_json::from_slice(v).unwrap());
                    Ok(true)
                })
                .unwrap();
                m
            };
            let proj: Vec<(u64, serde_json::Value)> = {
                let mut v = Vec::new();
                e.scan_stream_fields(None, None, vec!["a".into(), "c".into()], |d, s| {
                    v.push((d, serde_json::from_slice(s).unwrap()));
                    Ok(true)
                })
                .unwrap();
                v
            };
            assert_eq!(full.len(), proj.len(), "pax={pax} 投影扫描行数与全扫一致（memtable）");
            for (d, sub) in proj {
                let f = &full[&d];
                assert_eq!(sub.get("a"), f.get("a"), "pax={pax} docid={d} 列 a 一致");
                assert_eq!(sub.get("c"), f.get("c"), "pax={pax} docid={d} 列 c 一致");
            }
            // flush 后（SST 行式/PAX 块路径）再验
            e.flush_primary().unwrap();
            let full2: std::collections::HashMap<u64, serde_json::Value> = {
                let mut m = std::collections::HashMap::new();
                e.scan_stream(None, None, |d, v| {
                    m.insert(d, serde_json::from_slice(v).unwrap());
                    Ok(true)
                })
                .unwrap();
                m
            };
            let proj2: Vec<(u64, serde_json::Value)> = {
                let mut v = Vec::new();
                e.scan_stream_fields(None, None, vec!["a".into(), "c".into()], |d, s| {
                    v.push((d, serde_json::from_slice(s).unwrap()));
                    Ok(true)
                })
                .unwrap();
                v
            };
            assert_eq!(full2.len(), proj2.len(), "pax={pax} 投影扫描行数与全扫一致（flush 后）");
            for (d, sub) in proj2 {
                let f = &full2[&d];
                assert_eq!(sub.get("a"), f.get("a"), "pax={pax} flush 后 docid={d} 列 a 一致");
                assert_eq!(sub.get("c"), f.get("c"), "pax={pax} flush 后 docid={d} 列 c 一致");
            }
            drop(e);
            // 重开再验（SST 路径 + 懒基线）
            let e2 = Engine::open(dir.path(), &c).unwrap();
            let full3: std::collections::HashMap<u64, serde_json::Value> = {
                let mut m = std::collections::HashMap::new();
                e2.scan_stream(None, None, |d, v| {
                    m.insert(d, serde_json::from_slice(v).unwrap());
                    Ok(true)
                })
                .unwrap();
                m
            };
            let mut proj3: Vec<(u64, serde_json::Value)> = Vec::new();
            e2.scan_stream_fields(None, None, vec!["a".into(), "c".into()], |d, s| {
                proj3.push((d, serde_json::from_slice(s).unwrap()));
                Ok(true)
            })
            .unwrap();
            assert_eq!(full3.len(), proj3.len(), "pax={pax} 重开后行数一致");
            for (d, sub) in proj3 {
                assert_eq!(sub.get("a"), full3[&d].get("a"), "pax={pax} 重开 docid={d} a 一致");
                assert_eq!(sub.get("c"), full3[&d].get("c"), "pax={pax} 重开 docid={d} c 一致");
            }
        }
    }

    #[test]
    fn scan_excludes_deleted_and_revive() {
        // Ex-8.1：删除位图语义对齐——delete 后 scan/流式/count 与 get 一致不可见；
        // put 清位复活后重新可见。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=100u64 {
            e.put(i, format!("d{i}").into_bytes(), &["t"]).unwrap();
        }
        e.flush_primary().unwrap();
        for i in 30..=40u64 {
            e.delete(i).unwrap(); // 11 个（30..=40 闭区间）
        }
        let rows = e.scan_range(None, None).unwrap();
        assert_eq!(rows.len(), 89, "scan_range 应排除位图已删 docid");
        assert!(rows.iter().all(|(d, _)| !(30..=40).contains(d)));
        let mut s = 0u64;
        let mut has_del = false;
        e.scan_stream(None, None, |d, _v| {
            s += 1;
            if (30..=40).contains(&d) {
                has_del = true;
            }
            Ok(true)
        })
        .unwrap();
        assert_eq!(s, 89, "scan_stream 应排除位图已删 docid");
        assert!(!has_del);
        assert_eq!(e.count_all_docs().unwrap(), 89, "count 应排除位图已删 docid");
        assert!(e.get(35).unwrap().is_none(), "get 应不可见已删");
        // put 复活：清位后重新可见
        e.put(35, b"revived".to_vec(), &["t"]).unwrap();
        assert_eq!(e.get(35).unwrap().unwrap(), b"revived");
        let rows2 = e.scan_range(None, None).unwrap();
        assert_eq!(rows2.len(), 90, "put 复活后 scan 应重新可见");
        assert!(rows2.iter().any(|(d, _)| *d == 35));
    }

    #[test]
    fn scan_prunes_disjoint_ssts_windows() {
        // Ex-8.2：scan 路径段级 key 范围剪枝——3 个不相交文件下，窗口只命中相交文件，
        // 结果与全建迭代器一致（收集 == 流式），全扫与边界窗口正确。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for (lo, hi) in [(1u64, 4000u64), (4001, 8000), (8001, 12000)] {
            for i in lo..=hi {
                e.put(i, format!("d{i}").into_bytes(), &["t"]).unwrap();
            }
            e.flush_primary().unwrap(); // 三个不相交 docid 范围的 SST
        }
        let wins: Vec<(Option<u64>, Option<u64>)> = vec![
            (None, None),
            (Some(3990), Some(4010)),   // 跨文件边界
            (Some(7000), Some(7200)),   // 只命中中段
            (Some(11000), Some(12000)), // 尾段含端点
            (Some(1), Some(1)),
            (Some(12001), Some(13000)), // 越界空
            (None, Some(4000)),
            (Some(8001), None),
        ];
        for &(a, b) in &wins {
            let c = e.scan_range(a, b).unwrap();
            let mut s: Vec<(u64, Vec<u8>)> = Vec::new();
            e.scan_stream(a, b, |d, v| {
                s.push((d, v.to_vec()));
                Ok(true)
            })
            .unwrap();
            assert_eq!(c.len(), s.len(), "窗口 {a:?}..{b:?} 行数不一致 {} vs {}", c.len(), s.len());
            for (i, (cr, sr)) in c.iter().zip(s.iter()).enumerate() {
                assert_eq!(cr, sr, "窗口 {a:?}..{b:?} 第 {i} 行不一致");
            }
        }
        assert_eq!(e.scan_range(None, None).unwrap().len(), 12000, "全扫应 12000 行");
        assert_eq!(e.count_all_docs().unwrap(), 12000, "全库计数应 12000");
        assert_eq!(
            e.scan_range(Some(7000), Some(7200)).unwrap().len(),
            201,
            "中段窗口应 201 行（7000..=7200）"
        );
        // 剪枝不丢跨文件边界行
        let cross = e.scan_range(Some(3998), Some(4003)).unwrap();
        assert_eq!(cross.len(), 6, "跨文件窗口应 6 行");
    }

    #[test]
    fn scan_block_cache_warm_repeat_consistent() {
        // Ex-8.3：扫描路径块缓存（写穿 + 全组命中免 IO/解压）——首扫预热后重复窗口结果一致
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=20_000u64 {
            e.put(i, format!("d{i}").into_bytes(), &["t"]).unwrap();
        }
        e.flush_primary().unwrap();
        let w = (Some(9000u64), Some(9300u64));
        let first = e.scan_range(w.0, w.1).unwrap();
        assert_eq!(first.len(), 301);
        // 二次（应全块缓存命中）与流式/计数一致
        for _ in 0..3 {
            assert_eq!(e.scan_range(w.0, w.1).unwrap(), first, "缓存后扫描应一致");
        }
        let mut s = 0u64;
        e.scan_stream(w.0, w.1, |_d, _v| {
            s += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(s, 301, "流式窗口应一致");
        // 计数（keys-only 缓存路径）重复一致
        let c1 = e.count_all_docs().unwrap();
        let c2 = e.count_all_docs().unwrap();
        assert_eq!(c1, c2);
        assert_eq!(c1, 20_000);
    }

    #[test]
    fn auto_watermark_resumes_after_reopen() {
        // §27 P0：auto docid 水位 = 已写入最大 docid + 1；重启后惰性全库扫描恢复
        // （auto 分配续接不撞已提交行）；loaded 后幂等不重扫
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        // 运行期：put 的 fetch_max 维护 → 水位 = 曾写最大 + 1
        e.put(5, b"v5".to_vec(), &[]).unwrap();
        e.put(2, b"v2".to_vec(), &[]).unwrap();
        assert_eq!(e.auto_watermark(), 6, "运行期水位 = max+1");
        assert_eq!(e.auto_watermark(), 6, "重复调用幂等");
        e.put(100, b"v100".to_vec(), &[]).unwrap();
        assert_eq!(e.auto_watermark(), 101, "显式大 id 抬水位");
        // 重启：max_docid 归零 → 首次 auto_watermark 扫现存最大恢复（不含新引擎的 put 前）
        drop(e);
        let e2 = Engine::open(dir.path(), &cfg()).unwrap();
        assert_eq!(e2.auto_watermark(), 101, "重启后惰性恢复现存最大+1");
        assert_eq!(e2.auto_watermark(), 101);
        // 恢复后的水位保证 auto 分配不撞已提交行（分配 ≥ 101）
        assert!(e2.auto_watermark() > 100);
    }

    #[test]
    fn scan_stream_ids_matches_scan_stream() {
        // Ex-8.3 Part B：keys-only id 流式与全值 scan_stream 的 docid 集一致
        // （覆盖折叠 / 删除位图 / 多文件 / 越界空窗口）
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=5000u64 {
            e.put(i, format!("v0-{i}").into_bytes(), &["t"]).unwrap();
        }
        for i in 10..=20u64 {
            e.put(i, format!("v1-{i}").into_bytes(), &["t"]).unwrap(); // 覆盖（折叠验证）
        }
        e.flush_primary().unwrap();
        for i in 5001..=10_000u64 {
            e.put(i, format!("d{i}").into_bytes(), &["t"]).unwrap();
        }
        e.flush_primary().unwrap();
        for i in 3000..=3005u64 {
            e.delete(i).unwrap(); // 位图删除（6 个）
        }
        let wins: Vec<(Option<u64>, Option<u64>)> = vec![
            (None, None),
            (Some(1), Some(100)),
            (Some(2990), Some(3010)), // 覆盖段 + 删除段混合
            (Some(6000), Some(7000)),
            (Some(9990), Some(10010)), // 端点 + 越界
            (Some(20000), Some(30000)),
        ];
        for &(a, b) in &wins {
            let mut full: Vec<u64> = Vec::new();
            e.scan_stream(a, b, |d, _v| {
                full.push(d);
                Ok(true)
            })
            .unwrap();
            let mut ids: Vec<u64> = Vec::new();
            e.scan_stream_ids(a, b, |d| {
                ids.push(d);
                Ok(true)
            })
            .unwrap();
            assert_eq!(ids, full, "窗口 {a:?}..{b:?} keys-only 与全值 docid 集不一致");
        }
        let all: Vec<u64> = {
            let mut v = Vec::new();
            e.scan_stream_ids(None, None, |d| {
                v.push(d);
                Ok(true)
            })
            .unwrap();
            v
        };
        assert_eq!(all.len(), 9994, "删除 6 个后应有 9994 行");
        assert!(!all.contains(&3000));
        assert!(all.contains(&10));
    }

    #[test]
    fn delayed_l1_promotion_converges_and_preserves() {
        // Ex-8.11：L1 延迟大合并配置下，burst+flush+drain 工作负载**收敛**且数据完整
        // （选择级延迟语义由 column_family::tests::select_compaction_inputs_picks_levels 覆盖）
        fn run(l1_trigger: usize, l2_trigger: usize) -> (u64, u64) {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = cfg();
            cfg.storage.auto_compact = false;
            cfg.storage.group_commit_us = 2000;
            cfg.storage.l0_stall_threshold = 2;
            cfg.storage.l0_stall_min = 2;
            cfg.storage.l0_stall_max = 2;
            cfg.storage.l1_trigger_files = l1_trigger;
            cfg.storage.l2_trigger_files = l2_trigger;
            let mut e = Engine::open(dir.path(), &cfg).unwrap();
            let mut rounds = 0u64;
            let mut id = 0u64;
            for _b in 0..4 {
                for _f in 0..2 {
                    for _ in 0..30u64 {
                        id += 1;
                        e.put_nosync(id, format!("d{id}").into_bytes(), &["t"]).unwrap();
                    }
                    e.flush_primary().unwrap();
                }
                while e.needs_compact() && rounds < 500 {
                    e.compact().unwrap();
                    rounds += 1;
                }
            }
            // 收敛护栏：不应出现 needs_compact 恒真空转
            assert!(
                !e.needs_compact() || rounds >= 500,
                "l1_trigger={l1_trigger} 应在护栏内收敛，rounds={rounds}"
            );
            let total = id;
            assert_eq!(e.count_all_docs().unwrap(), total, "数据应完整");
            (rounds, total)
        }
        let (r0, t0) = run(0, 0); // 现行为
        let (rd, t1) = run(3, 2); // 延迟（攒 3 才下沉）
        assert_eq!(t0, t1);
        assert!(rd <= r0 + 2, "延迟模式收敛轮数不应显著劣化：default={r0} delayed={rd}");
        eprintln!("[Ex-8.11] rounds default={r0} delayed(l1=3)={rd} total={t0}");
    }

    #[test]
    fn seq_prune_snapshot_reads_stable_across_reopen() {
        // Ex-8.6：段级 min seq 快照剪枝——旧快照读对新文件整段跳过；语义与版本过滤一致；
        // 重开后惰性推导重建（无 manifest 扩展）结果不变。
        let dir = tempfile::tempdir().unwrap();
        let run = |e: &Engine, old_snap: u64| {
            // 旧快照：只应见文件1（1..=50），文件2（101..=150）整段 seq > 快照 → 剪枝
            assert!(e.get_at(5, old_snap).unwrap().is_some(), "文件1 行在快照内");
            assert!(e.get_at(101, old_snap).unwrap().is_none(), "文件2 行在快照后应不可见");
            assert!(e.get_at(1, old_snap).unwrap().is_some());
            // 最新视图（MAX）：全部可见
            assert!(e.get_at(150, u64::MAX).unwrap().is_some());
            assert_eq!(e.count_all_docs().unwrap(), 100);
            // 最新流式全扫：文件2 贡献（MAX 不剪枝）
            let mut n = 0u64;
            e.scan_stream(None, None, |_d, _v| {
                n += 1;
                Ok(true)
            })
            .unwrap();
            assert_eq!(n, 100);
        };
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=50u64 {
            e.put(i, format!("v-{i}").into_bytes(), &["t"]).unwrap();
        }
        e.flush_primary().unwrap(); // 文件1
        // 旧快照（在文件2 写入前）：
        let mut t = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        let old_snap = t.snapshot();
        e.txn_rollback(t);
        for i in 101..=150u64 {
            e.put(i, format!("v-{i}").into_bytes(), &["t"]).unwrap();
        }
        e.flush_primary().unwrap(); // 文件2（全部行 seq > old_snap）
        run(&e, old_snap);
        drop(e);
        // 重开：seq_min 记忆清空 → 首次快照读惰性 keys-only 推导重建
        let e2 = Engine::open(dir.path(), &cfg()).unwrap();
        run(&e2, old_snap);
    }

    #[test]
    fn batch_get_matches_get_with_delta_and_deletion_bitmap() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 0..40u64 {
            let doc = serde_json::json!({"k": format!("d{i}"), "n": i});
            e.put(i, serde_json::to_vec(&doc).unwrap(), &["t"]).unwrap();
        }
        // Delta 字段级覆盖（patch）+ 删除位图删除
        e.patch(1, &[("k", serde_json::Value::String("patched".into()))]).unwrap();
        e.patch(2, &[("extra", serde_json::Value::from(42))]).unwrap();
        e.delete(4).unwrap();
        let ids: Vec<u64> = (0..45).filter(|i| i % 2 == 0).collect();
        let batch = e.batch_get(&ids).unwrap();
        assert_eq!(batch.len(), ids.len());
        for (i, &d) in ids.iter().enumerate() {
            let single = e.get(d).unwrap();
            assert_eq!(batch[i], single, "docid {d} batch_get 与 get 结果不一致");
        }
        // 删除语义：docid 4 在两条路径均为 None
        assert!(e.get(4).unwrap().is_none());
        let idx4 = ids.iter().position(|&x| x == 4).unwrap();
        assert!(batch[idx4].is_none());
    }

    #[test]
    fn batch_get_no_delta_short_circuit_returns_stored_bytes() {
        // P86①：无 Delta 覆盖 → 直通短路（原字节返回，跳 parse/reserialize 等值空转）
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        // 非规范键序原始字节（若走 parse+reserialize，serde Map 会重排序/规范化）
        let raw: &[u8] = br#"{"z":1,"a":2}"#;
        e.put(7, raw.to_vec(), &[]).unwrap();
        // docid 8 有 Delta 覆盖 → 仍走合并路径
        e.put(8, br#"{"z":1}"#.to_vec(), &[]).unwrap();
        e.patch(8, &[("w", serde_json::json!(9))]).unwrap();
        let out = e.batch_get(&[7, 8]).unwrap();
        assert_eq!(
            out[0].as_deref(),
            Some(&raw[..]),
            "无覆盖 → 原字节直通（未 parse/reserialize 重排）"
        );
        let v8: &serde_json::Value = &serde_json::from_slice(out[1].as_ref().unwrap()).unwrap();
        assert_eq!(v8["w"], 9, "覆盖行合并 w=9");
        assert_eq!(v8["z"], 1);
        // JSON 语义与单行 get 等值
        let single = e.get(7).unwrap().unwrap();
        let sm: serde_json::Value = serde_json::from_slice(&single).unwrap();
        let om: serde_json::Value = serde_json::from_slice(raw).unwrap();
        assert_eq!(sm, om, "短路返回与 get 规范化字节 JSON 等值");
    }

    #[test]
    fn batch_get_fields_matches_get_semantics_with_delta_and_delete() {
        // P87②：投影字段批量回表与 get 整行语义一致（含 Delta 覆盖 / null 删字段 / 删除位图）
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 0..30u64 {
            let doc = serde_json::json!({"k": format!("d{i}"), "n": i, "s": format!("s{i}")});
            e.put(i, serde_json::to_vec(&doc).unwrap(), &["t"]).unwrap();
        }
        // Delta 覆盖请求字段 k + 覆盖非请求字段 s（不应影响 n 提取）；null 删除字段 k
        e.patch(1, &[("k", serde_json::json!("patched")), ("s", serde_json::json!("s-patched"))])
            .unwrap();
        e.patch(2, &[("k", serde_json::Value::Null)]).unwrap();
        e.delete(3).unwrap();
        let ids: Vec<u64> = (0..30).collect();
        let fields: Vec<String> = vec!["k".into(), "n".into(), "missing".into()];
        // 两轮：① MemTable 命中；② flush 后 SST 行式块按需字段提取路径
        for round in 0..2 {
            if round == 1 {
                e.flush_primary().unwrap();
            }
            let got = e.batch_get_fields(&ids, &fields).unwrap();
            assert_eq!(got.len(), 30);
            for (i, &d) in ids.iter().enumerate() {
                let expect = e.get(d).unwrap().map(|v| {
                    let m: serde_json::Map<String, serde_json::Value> =
                        serde_json::from_slice(&v).unwrap();
                    fields
                        .iter()
                        .map(|f| m.get(f).cloned())
                        .collect::<Vec<_>>()
                });
                match (&got[i], &expect) {
                    (None, None) => {}
                    (Some(g), Some(ex)) => {
                        assert_eq!(g.len(), ex.len());
                        for (gi, ev) in g.iter().zip(ex.iter()) {
                            match (gi, ev) {
                                (Some(gb), Some(ev)) => {
                                    let gv: serde_json::Value =
                                        serde_json::from_slice(gb).unwrap();
                                    assert_eq!(gv, *ev, "round{round} docid {d} 字段值");
                                }
                                (None, None) => {}
                                (g, ev) => panic!(
                                    "round{round} docid {d} 字段存在性不一致 got={g:?} exp={ev:?}"
                                ),
                            }
                        }
                    }
                    (g, ev) => panic!(
                        "round{round} docid {d} 行存在性不一致 got={g:?} exp={ev:?}"
                    ),
                }
            }
            // 语义抽查
            let k1: serde_json::Value =
                serde_json::from_slice(got[1].as_ref().unwrap()[0].as_ref().unwrap()).unwrap();
            assert_eq!(k1, "patched", "round{round} docid1 k 覆盖生效");
            assert!(
                got[2].as_ref().unwrap()[0].is_none(),
                "round{round} docid2 k 被 delta null 删除"
            );
            assert!(got[3].is_none(), "round{round} docid3 已删");
            assert!(got[0].as_ref().unwrap()[2].is_none(), "round{round} 缺字段");
            let n1: serde_json::Value =
                serde_json::from_slice(got[1].as_ref().unwrap()[1].as_ref().unwrap()).unwrap();
            assert_eq!(n1, 1, "round{round} 非请求字段 s 覆盖不影响 n");
        }
    }

    #[test]
    fn search_term_paged_batch_backfill_matches_individual_get() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 0..30u64 {
            let doc = serde_json::json!({"city": "beijing", "i": i});
            e.put(i, serde_json::to_vec(&doc).unwrap(), &["city=beijing"]).unwrap();
        }
        // 倒排回表：批量路径应与逐条 get 一致（含分页 offset/limit 语义）
        let p1 = e.search_term_paged("city=beijing", Some(10), 5).unwrap();
        assert_eq!(p1.total, 30);
        assert_eq!(p1.rows.len(), 10);
        for (d, v) in &p1.rows {
            assert_eq!(e.get(*d).unwrap().unwrap(), *v, "docid {d} 回表值不一致");
        }
        let p2 = e.search_term_paged("city=beijing", Some(5), 25).unwrap();
        assert_eq!(p2.rows.len(), 5, "末页应返回剩余 5 行");
        // 删除后回表应过滤（Tombstone / 删除位图）
        e.delete(3).unwrap();
        let p3 = e.search_term_paged("city=beijing", None, 0).unwrap();
        assert_eq!(p3.total, 30, "posting 含已删 docid（回表过滤）");
        assert!(!p3.rows.iter().any(|(d, _)| *d == 3), "已删 docid 不应回表");
    }

    // ---------- P 项：事件驱动自动 Compaction ----------

    fn p_compact_cfg() -> Config {
        let mut c = cfg();
        c.storage.auto_compact = true;
        c.storage.l0_stall_min = 2;
        c.storage.l0_stall_max = 3;
        c.storage.l0_stall_threshold = 2; // L0 > 2 即触发
        c.memtable.max_size_mb = 1; // 1MB MemTable → 快速多次 flush
        c.storage.group_commit_us = 2000; // 组提交避免逐条 fsync 拖慢测试
        c
    }

    fn fill_small_docs(e: &mut Engine, rounds: u32, per_round: u32) {
        let mut id = 0u64;
        for _ in 0..rounds {
            for _ in 0..per_round {
                let doc = serde_json::json!({"k": id, "c": "x".repeat(8000)});
                e.put(id, serde_json::to_vec(&doc).unwrap(), &[]).unwrap();
                id += 1;
            }
        }
    }

    #[test]
    fn auto_compact_keeps_l0_bounded_on_flush() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &p_compact_cfg()).unwrap();
        // ~6MB 数据 → 多次 MemTable flush → 写路径自触发合并收敛 L0
        fill_small_docs(&mut e, 12, 64);
        assert!(
            e.primary_l0_count() <= 3,
            "auto_compact 应把 L0 收敛到阈值内（实际 {}）",
            e.primary_l0_count()
        );
        assert!(!e.needs_compact(), "收敛后不应再需要合并");
        assert!(e.get(0).unwrap().is_some(), "数据应完整可读");
        assert!(e.get(12 * 64 - 1).unwrap().is_some());
    }

    #[test]
    fn background_trigger_sets_pending_and_readlock_compact_converges() {
        // O 项第③步：挂载后台 worker（compact_worker=true）后，写路径只置信号不阻塞；
        // Engine::compact 可在 &self（读锁语义）下执行并收敛 L0。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &p_compact_cfg()).unwrap();
        e.compact_worker.store(true, Ordering::Release);
        fill_small_docs(&mut e, 12, 64);
        assert!(
            e.compact_pending.load(Ordering::Acquire),
            "写路径应置 pending 信号（后台合并触发）"
        );
        assert!(e.needs_compact(), "L0 超阈值应判需要合并");
        // 后台语义：合并经 &Engine（读锁）执行——验证 &self 路径收敛
        let eng = &e;
        let _ = eng.compact().unwrap();
        assert!(!eng.needs_compact(), "读锁合并后应收敛");
        assert!(eng.get(0).unwrap().is_some(), "数据应完整可读");
        assert!(eng.get(12 * 64 - 1).unwrap().is_some());
    }

    #[test]
    fn ex813_inverted_write_accounting_and_io_budget() {
        // Ex-8.13 切片 1：倒排 seg 写盘统一记账（inverted_written_bytes）+ 后台 IO 预算
        // 接线（attach/set_io_rate_bytes 不改变写入语义；默认 rate0=不启用）
        // 1) 默认无预算：flush 后写盘字节 >0（记账累计）
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        assert_eq!(e.inverted_written_bytes(), 0);
        for i in 0..500u64 {
            let d = format!("{{\"a\":{i}}}").into_bytes();
            e.put_nosync(i, d, &["a=1"]).unwrap();
        }
        e.flush_inverted().unwrap();
        let w0 = e.inverted_written_bytes();
        assert!(w0 > 0, "倒排刷段应累计写盘字节");
        // 2) 启用预算（rate>0）attach 不报错、写入语义不变（写盘仍累计、可读回）
        let dir2 = tempfile::tempdir().unwrap();
        let mut cfg2 = crate::config::Config::default();
        cfg2.storage.io_rate_limit_mb = 1024; // 大预算：acquire 不阻塞
        let mut e2 = Engine::open(dir2.path(), &cfg2).unwrap();
        for i in 0..500u64 {
            let d = format!("{{\"a\":{i}}}").into_bytes();
            e2.put_nosync(i, d, &["a=1"]).unwrap();
        }
        e2.flush_inverted().unwrap();
        assert!(e2.inverted_written_bytes() > 0, "预算开启下刷段仍记账");
        assert!(e2.get(100).unwrap().is_some(), "写路径不受预算影响");
        // 3) 再次刷段 → 记账单调累计（多次 seg 写盘字节和）
        e2.put_nosync(999, b"{}".to_vec(), &["a=1"]).unwrap();
        e2.flush_inverted().unwrap();
        assert!(e2.inverted_written_bytes() > w0);
    }

    #[test]
    fn ex89_write_pressure_proxy() {
        // Ex-8.9：前台写压力代理（主 MemTable 水位）——空库 0，写入上升 >0
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.memtable.max_size_mb = 8; // 大 MemTable：写入不触发自动 flush，水位可控
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        assert_eq!(e.write_pressure(), 0.0, "空库无压力");
        let chunk = vec![b'x'; 512];
        let mut d = 0u64;
        for _ in 0..4000u64 {
            e.put_nosync(d, chunk.clone(), &[]).unwrap();
            d += 1;
        }
        assert!(e.write_pressure() > 0.0, "memtable 超水位 → 压力>0");
        assert!(e.write_pressure() <= 1.0);
    }

    #[test]
    fn metrics_count_reads_writes_flush_compact() {
        // X 项：读写操作计数 + 延迟直方图 + flush/compact 计数埋点
        let dir = tempfile::tempdir().unwrap();
        let mut c = p_compact_cfg();
        c.memtable.max_size_mb = 1; // 小 MemTable → 写入过程触发 flush
        let mut e = Engine::open(dir.path(), &c).unwrap();
        assert_eq!(e.metrics.read_ops.load(Ordering::Relaxed), 0);
        assert_eq!(e.metrics.write_ops.load(Ordering::Relaxed), 0);
        for i in 0..64u64 {
            e.put(i, format!("v{i}").into_bytes(), &["t"]).unwrap();
        }
        assert!(e.metrics.write_ops.load(Ordering::Relaxed) >= 64, "put 应计数");
        let _ = e.get(0).unwrap();
        assert_eq!(e.metrics.read_ops.load(Ordering::Relaxed), 1, "get 应计数");
        // flush：64 条 × ~12B 远小于 1MB → 显式刷盘触发计数
        let f0 = e.total_flush_count();
        e.flush_primary().unwrap();
        assert!(e.total_flush_count() >= f0 + 1, "flush 应计数");
        // 延迟直方图：64 次 put 记录延迟（get 命中 hotcache 提前返回不计延迟）
        let sum: u64 = e
            .metrics
            .latency_buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .sum();
        assert!(sum >= 64, "put 应记录延迟（实际 {sum}）");
        // compact 计数
        let c0 = e.metrics.compact_count.load(Ordering::Relaxed);
        if e.needs_compact() {
            let _ = e.compact().unwrap();
            assert!(
                e.metrics.compact_count.load(Ordering::Relaxed) >= c0 + 1,
                "compact 应计数"
            );
        }
    }

    #[test]
    fn auto_compact_off_leaves_l0_accumulated() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = p_compact_cfg();
        c.storage.auto_compact = false;
        let mut e = Engine::open(dir.path(), &c).unwrap();
        fill_small_docs(&mut e, 12, 64);
        assert!(
            e.primary_l0_count() >= 4,
            "关闭 auto_compact 后 L0 应累积（实际 {}）",
            e.primary_l0_count()
        );
        assert!(e.needs_compact(), "L0 超阈值应判需要合并");
    }

    #[test]
    fn rwlock_concurrent_reads_and_writes() {
        // O 项第②步：Engine 跨线程共享（Arc<RwLock<Engine>>）——多读线程读锁并行 +
        // 写线程写锁互斥；验证 SstReader Sync 化后读路径无数据竞争。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 0..100u64 {
            e.put(i, format!("v{i}").into_bytes(), &[]).unwrap();
        }
        let engine = std::sync::Arc::new(std::sync::RwLock::new(e));
        let mut handles = Vec::new();
        // 4 个读线程：各自并发点查固定 docid（读读并行）
        for t in 0..4u64 {
            let eng = engine.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..200 {
                    let g = eng.read().unwrap();
                    let v = g.get(t).unwrap().expect("读线程应命中");
                    assert_eq!(v, format!("v{t}").into_bytes());
                }
            }));
        }
        // 1 个写线程：并发写入新 docid（写锁互斥）
        let w = engine.clone();
        handles.push(std::thread::spawn(move || {
            for i in 100..110u64 {
                let mut g = w.write().unwrap();
                g.put(i, format!("w{i}").into_bytes(), &[]).unwrap();
            }
        }));
        for h in handles {
            h.join().unwrap();
        }
        // 写线程提交后可见
        let g = engine.read().unwrap();
        assert!(g.get(105).unwrap().is_some());
        assert!(g.get(4).unwrap().is_some());
    }

    #[test]
    fn txn_rr_write_conflict_detected_on_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v0".to_vec(), &["t"]).unwrap();
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        e.put(1, b"v1".to_vec(), &["t"]).unwrap(); // 并发事务在快照后修改 docid=1
        txn.put(1, b"txn-write".to_vec(), vec![]);
        assert!(
            e.txn_commit(txn).is_err(),
            "RR 提交应因写写冲突 abort（last_write_seq > snapshot）"
        );
        // 冲突 abort 后引擎保持并发写结果，且锁已释放
        assert_eq!(e.get(1).unwrap().unwrap(), b"v1");
        assert_eq!(e.txn_locks.lock().unwrap().lock_count(), 0, "abort 后锁应全部释放");
    }

    #[test]
    fn txn_rc_reads_latest_committed() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v0".to_vec(), &["t"]).unwrap();
        let mut txn = e.txn_begin(crate::txn::Isolation::ReadCommitted);
        e.put(1, b"v1".to_vec(), &["t"]).unwrap();
        let got = e.txn_get(&mut txn, 1).unwrap();
        assert_eq!(got.unwrap(), b"v1", "RC 应读最新已提交版本");
        e.txn_rollback(txn);
    }

    #[test]
    fn txn_serializable_read_lock_blocks_concurrent_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v0".to_vec(), &["t"]).unwrap();
        // txn1 读 docid=1（共享锁，SERIALIZABLE 持有至提交）
        let mut t1 = e.txn_begin(crate::txn::Isolation::Serializable);
        let v = e.txn_get(&mut t1, 1).unwrap();
        assert_eq!(v.unwrap(), b"v0");
        // txn2 写 docid=1 → 共享读锁未释放 → 排他请求冲突
        let mut t2 = e.txn_begin(crate::txn::Isolation::Serializable);
        t2.put(1, b"v2".to_vec(), vec![]);
        assert!(e.txn_commit(t2).is_err(), "SERIALIZABLE 读锁持有期间排他写应冲突");
        // txn1 提交释放读锁后，新事务可写
        e.txn_commit(t1).unwrap();
        let mut t3 = e.txn_begin(crate::txn::Isolation::Serializable);
        t3.put(1, b"v3".to_vec(), vec![]);
        e.txn_commit(t3).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"v3");
    }

    #[test]
    fn txn_serializable_upgrades_read_lock_on_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v0".to_vec(), &["t"]).unwrap();
        // 同一事务先读后写同一 docid：共享 → 排他升级（2PL 合法）
        let mut t1 = e.txn_begin(crate::txn::Isolation::Serializable);
        let v = e.txn_get(&mut t1, 1).unwrap();
        assert_eq!(v.unwrap(), b"v0");
        t1.put(1, b"v1".to_vec(), vec![]);
        e.txn_commit(t1).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"v1");
    }

    #[test]
    fn txn_deadlock_detected_at_engine_level() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        // 通过锁表构造 wait-for 环：txn9001 持 10 等 20；txn9002 持 20 等 10
        e.txn_locks.lock().unwrap().acquire_exclusive(9001, 10).unwrap();
        e.txn_locks.lock().unwrap().acquire_exclusive(9002, 20).unwrap();
        let r1 = e.txn_locks.lock().unwrap().acquire_exclusive(9001, 20);
        assert!(matches!(r1, Err(crate::error::Error::TxnConflict(_))), "无环 → 冲突");
        let r2 = e.txn_locks.lock().unwrap().acquire_exclusive(9002, 10);
        assert!(
            matches!(r2, Err(crate::error::Error::TxnDeadlock(_))),
            "环 → 检测死锁（victim abort）"
        );
        e.txn_locks.lock().unwrap().release(9001);
        e.txn_locks.lock().unwrap().release(9002);
        assert_eq!(e.txn_locks.lock().unwrap().lock_count(), 0);
    }

    #[test]
    fn txn_delete_and_mixed_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(5, b"keep".to_vec(), &["t"]).unwrap();
        e.put(6, b"del".to_vec(), &["t"]).unwrap();
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        txn.delete(6);
        txn.put(7, b"new".to_vec(), vec!["t".into()]);
        e.txn_commit(txn).unwrap();
        assert!(e.get(6).unwrap().is_none(), "事务删除生效");
        assert_eq!(e.get(7).unwrap().unwrap(), b"new");
        assert_eq!(e.get(5).unwrap().unwrap(), b"keep");
    }

    #[test]
    fn txn_rollback_leaves_no_trace() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        txn.put(1, b"x".to_vec(), vec![]);
        txn.delete(2);
        e.txn_rollback(txn);
        assert!(e.get(1).unwrap().is_none(), "回滚后无写入");
        assert_eq!(e.txn_locks.lock().unwrap().lock_count(), 0, "回滚释放全部锁");
    }

    #[test]
    fn txn_snapshot_advances_with_committed_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        let s0 = e.txn_begin(crate::txn::Isolation::RepeatableRead).snapshot();
        e.put(1, b"v1".to_vec(), &["t"]).unwrap();
        let s1 = e.txn_begin(crate::txn::Isolation::RepeatableRead).snapshot();
        assert!(s1 > s0, "提交后新事务快照 seq 应推进");
        // 旧快照仍读到写前状态（历史版本由 WAL/MemTable seq 过滤保证）
        let got = e.get_at(1, s0).unwrap();
        assert!(got.is_none(), "旧快照点 docid=1 尚不存在");
        assert_eq!(e.get_at(1, s1).unwrap().unwrap(), b"v1");
    }

    // ---------- 组提交（M8） ----------

    #[test]
    fn group_commit_disabled_fallback_persists_each_put() {
        // 显式关闭组提交（旧默认，强安全）：put 逐条 fsync，drop 后重开数据完整
        let dir = tempfile::tempdir().unwrap();
        {
            let mut c = cfg();
            c.storage.group_commit_us = 0;
            let mut e = Engine::open(dir.path(), &c).unwrap();
            assert!(e.group_commit.is_none(), "group_commit_us=0 应关闭组提交");
            for i in 0..50u64 {
                e.put(i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
            }
        }
        let mut e2 = Engine::open(dir.path(), &cfg()).unwrap();
        assert_eq!(e2.get(49).unwrap().unwrap(), b"doc-49");
    }

    #[test]
    fn group_commit_default_enabled_drop_persists_tail() {
        // 组提交默认开（1000µs，Task-005 采纳）：drop 时窗口尾批必须 flush，
        // 重开数据完整（崩溃恢复语义：进程内正常关闭不丢尾批）
        // Task-026：本测试验证**既有组提交机制**（per-CPU WAL 默认开启时代替该线程）
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.storage.per_cpu_enabled = false; // 组提交机制本体（legacy 路径）
        {
            assert_eq!(c.storage.group_commit_us, 1000, "默认组提交窗口应为 1000µs");
            let mut e = Engine::open(dir.path(), &c).unwrap();
            assert!(e.group_commit.is_some(), "默认应开启组提交");
            for i in 0..50u64 {
                e.put(i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
            }
        }
        let mut e2 = Engine::open(dir.path(), &c).unwrap();
        assert_eq!(e2.get(49).unwrap().unwrap(), b"doc-49");
    }

    // ---------- Task-023：主键 IN 稠密/稀疏批量取行 ----------

    #[test]
    fn task023_pk_in_dense_sparse_matches_individual_get() {
        // 稠密（区间顺序读+集合过滤）与稀疏（batch_get）均须与逐条 get 一致：
        // 删除位图隐藏行跳过、不存在 id 跳过、重复 id 去重。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=200u64 {
            e.put(i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
        }
        for i in 0..5u64 {
            e.put(5000 + i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
        }
        e.delete(7).unwrap(); // 删除位图隐藏
        e.flush_primary().unwrap();

        // 稠密：10..=60 每隔 3 取一个（17 个 id，跨度 51 ≤ 4×17=68 → 区间顺序读）
        let dense: Vec<u64> = (10..=60).step_by(3).collect();
        // 稀疏：跨大跨度（含高位 5002 / 不存在 9000 / 重复 7）
        let sparse: Vec<u64> = vec![1, 5002, 9000, 2, 7, 7, 55, 1];
        let expect = |ids: &[u64]| -> std::collections::HashMap<u64, Vec<u8>> {
            let mut m = std::collections::HashMap::new();
            for id in ids {
                if let Ok(Some(v)) = e.get(*id) {
                    m.entry(*id).or_insert(v);
                }
            }
            m
        };
        for ids in [dense.clone(), sparse.clone()] {
            let got = e.get_many_pk_in(&ids).unwrap();
            let want = expect(&ids);
            assert_eq!(got.len(), want.len(), "命中数一致 ids={ids:?}");
            for (d, v) in &want {
                assert_eq!(got.get(d), Some(v), "docid={d} 值一致（Task-023）");
            }
        }
    }

    /// Task-032：稀疏主键 IN（跨度 4×~64×计数）区间顺序读路径 = 逐条 get
    /// （含删除位图隐藏跳过、缺失 id 跳过）；投影字段变体同构。
    #[test]
    fn task032_pk_in_window_scan_matches_individual_get() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=2000u64 {
            let doc = serde_json::json!({ "docid": i, "note": format!("n{i}") });
            let t: &[&str] = &[];
            e.put(i, serde_json::to_vec(&doc).unwrap(), t).unwrap();
        }
        // 删除位图隐藏部分行（5 的倍数 → 400 行删除）
        for i in (1..=2000u64).step_by(5) {
            e.delete(i).unwrap();
        }
        e.flush_primary().unwrap();
        // 每 20 取 1：约 80 个冷行（去 5 倍数），span≈2000 ≤ 64×80 → 区间扫路径
        let ids: Vec<u64> = (1..=2000).step_by(20).collect();
        let expect = |ids: &[u64]| -> std::collections::HashMap<u64, Vec<u8>> {
            let mut m = std::collections::HashMap::new();
            for id in ids {
                if let Ok(Some(v)) = e.get(*id) {
                    m.entry(*id).or_insert(v);
                }
            }
            m
        };
        let want = expect(&ids);
        let got = e.get_many_pk_in(&ids).unwrap();
        assert_eq!(got.len(), want.len());
        for (d, v) in &want {
            assert_eq!(got.get(d), Some(v), "整行变体 docid={d}");
        }
        let flds = vec!["note".into()];
        let gotf = e.get_many_pk_in_fields(&ids, &flds).unwrap();
        assert_eq!(gotf.len(), want.len(), "投影变体命中数一致");
        for (d, v) in &want {
            let full: serde_json::Value = serde_json::from_slice(v).unwrap();
            let sub: serde_json::Value = serde_json::from_slice(gotf.get(d).unwrap()).unwrap();
            assert_eq!(sub["note"], full["note"], "投影子集 note 一致 docid={d}");
        }
    }

    // ---------- 缺口①：主键 IN 投影批量取行（get_many_pk_in_fields） ----------

    #[test]
    fn gap1_pk_in_fields_subset_matches_full_row_both_layouts() {
        // 缺口①（P105-①）：get_many_pk_in_fields（投影批量取行）子集 JSON 的字段值/命中集
        // 须与 get_many_pk_in 整行路径一致——行式与 PAX(hot_fields) 两布局 × memtable/flush/重开，
        // 覆盖缺字段 / JSON null / 转义字符串 / 浮点 / 删除位图隐藏 / 重复与缺失 id。
        for pax in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut c = cfg();
            if pax {
                c.storage.hot_fields = vec!["a".into(), "b".into()];
            }
            let mut e = Engine::open(dir.path(), &c).unwrap();
            let fields: Vec<String> = vec![
                "a".into(),
                "b".into(),
                "c".into(),
                "n".into(),
                "t".into(),
                "f".into(),
            ];
            for i in 0..3000u64 {
                let mut m = serde_json::Map::new();
                m.insert("a".into(), json!(format!("a{i}")));
                m.insert("b".into(), json!(i as i64));
                if i % 3 != 1 {
                    m.insert("c".into(), json!(i % 7)); // 部分行缺 c
                }
                if i == 42 {
                    m.insert("n".into(), serde_json::Value::Null); // JSON null
                }
                if i == 43 {
                    m.insert("t".into(), json!("s\"q\\w")); // 转义字符串
                }
                if i == 44 {
                    m.insert("f".into(), json!(1.5)); // 浮点
                }
                let doc = serde_json::Value::Object(m);
                e.put(i, serde_json::to_vec(&doc).unwrap(), &["t"]).unwrap();
            }
            e.delete(9).unwrap(); // 删除位图隐藏
            let dense: Vec<u64> = (10..=60).step_by(3).collect();
            let sparse: Vec<u64> = vec![1, 2999, 5002, 2, 43, 42, 44, 9000, 9, 7, 1];
            let check = |e: &Engine, ids: &[u64]| {
                let full = e.get_many_pk_in(ids).unwrap();
                let sub = e.get_many_pk_in_fields(ids, &fields).unwrap();
                assert_eq!(sub.len(), full.len(), "pax={pax} 命中集须一致 ids={ids:?}");
                for (d, row) in &full {
                    let sub_doc: serde_json::Value = serde_json::from_slice(
                        sub.get(d).unwrap_or_else(|| {
                            panic!("pax={pax} 整行命中 docid {d} 须在投影命中集")
                        }),
                    )
                    .unwrap();
                    let full_doc: serde_json::Value = serde_json::from_slice(row).unwrap();
                    for f in &fields {
                        assert_eq!(
                            sub_doc.get(f),
                            full_doc.get(f),
                            "pax={pax} docid={d} 字段 {f} 值须与整行路径一致"
                        );
                    }
                }
            };
            check(&e, &dense); // memtable 期（稠密区间扫）
            check(&e, &sparse);
            e.flush_primary().unwrap();
            drop(e);
            let e2 = Engine::open(dir.path(), &c).unwrap();
            check(&e2, &dense); // SST 期（PAX 列解码 / 行式块）
            check(&e2, &sparse);
        }
    }

    // ---------- Task-025b 阶段③：条带并行全扫（导出构建块） ----------

    #[test]
    fn task025b_parallel_scan_range_matches_serial() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=20_000u64 {
            e.put(i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
        }
        e.flush_primary().unwrap();
        for d in [3u64, 700, 19_999] {
            e.delete(d).unwrap();
        }
        // flush 后追加（memtable 未刷盘行混入条带）
        for i in 20_001..=20_010u64 {
            e.put(i, format!("tail-{i}").into_bytes(), &["t"]).unwrap();
        }
        let serial = e.scan_range(Some(1), Some(20_010)).unwrap();
        let parallel = e.scan_range_parallel(1, 20_010, 8).unwrap();
        assert_eq!(serial.len(), parallel.len(), "并行条带行数须与串行一致");
        assert!(serial.iter().all(|(d, v)| *d != 3 && *d != 700 && *d != 19_999), "删除位图隐藏");
        assert_eq!(serial, parallel, "并行 K 路归并须与串行逐行一致（含删除/尾批）");
        assert!(
            parallel.windows(2).all(|w| w[0].0 < w[1].0),
            "并行结果须全局升序"
        );
    }

    // ---------- Task-025b 阶段④：跨文件扇出并行（逐 SST 线程 + k-way 归并） ----------

    #[test]
    fn task025b4_scan_stream_parallel_matches_serial_multi_l0() {
        // 阶段④：Engine::scan_stream_parallel（CF scan_stream_at_parallel 扇出）须与串行
        // scan_stream 逐行一致——多 L0 重叠 + memtable 未刷盘行 + 覆盖写（跨段同 key 多版本）
        // + 删除位图隐藏 + 投影列 + 回调早停。auto_compact 关 → flush 逐段保留多 L0 文件。
        for pax in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut c = cfg();
            c.storage.auto_compact = false;
            c.memtable.max_size_mb = 1;
            if pax {
                c.storage.hot_fields = vec!["v".into(), "s".into()];
            }
            let mut e = Engine::open(dir.path(), &c).unwrap();
            for seg in 0..4u64 {
                for i in seg * 1000..seg * 1000 + 1000 {
                    let doc = serde_json::json!({"v": (i as i64) * 2, "s": (i % 7) as i64});
                    e.put(i, serde_json::to_vec(&doc).unwrap(), &["v"]).unwrap();
                }
                e.flush_primary().unwrap(); // 每段一 L0 文件
            }
            // 覆盖写（L0 间同 key 多版本）+ 删除 + memtable 尾行
            for i in (500..700).step_by(2) {
                let doc = serde_json::json!({"v": 9999i64, "s": 1i64});
                e.put(i, serde_json::to_vec(&doc).unwrap(), &["v"]).unwrap();
            }
            for d in [150u64, 2500, 3901] {
                e.delete(d).unwrap();
            }
            for i in 4000..4020u64 {
                let doc = serde_json::json!({"v": (i as i64), "s": 0i64});
                e.put(i, serde_json::to_vec(&doc).unwrap(), &["v"]).unwrap();
            }
            let collect = |e: &Engine, workers: usize, project: Option<Vec<String>>, stop_at: Option<usize>| {
                let mut out: Vec<(u64, Vec<u8>)> = Vec::new();
                e.scan_stream_parallel(Some(0), Some(5000), workers, project, None, |d, v| {
                    out.push((d, v.to_vec()));
                    Ok(stop_at.map(|n| out.len() < n).unwrap_or(true))
                })
                .unwrap();
                out
            };
            for workers in [2usize, 4, 8] {
                let par = collect(&e, workers, None, None);
                let ser = collect(&e, 1, None, None);
                assert_eq!(
                    par.len(),
                    ser.len(),
                    "pax={pax} workers={workers} 扇出行数须与串行一致"
                );
                assert_eq!(par, ser, "pax={pax} workers={workers} 须逐行一致（含覆盖/删除/尾行）");
                assert!(
                    par.windows(2).all(|w| w[0].0 < w[1].0),
                    "pax={pax} workers={workers} 须全局升序"
                );
                // 回调 false 早停：扇出与串行截断点一致
                let par5 = collect(&e, workers, None, Some(5));
                let ser5 = collect(&e, 1, None, Some(5));
                assert_eq!(par5, ser5, "pax={pax} workers={workers} 早停前缀须一致");
            }
            // 投影列并行（PAX 列解码 / 行式按需）== 串行整行子集（值层等值）
            let fields = vec!["v".into()];
            let par_f: Vec<(u64, Vec<u8>)> = collect(&e, 4, Some(fields.clone()), None);
            let full: std::collections::HashMap<u64, Vec<u8>> = collect(&e, 1, None, None)
                .into_iter()
                .collect();
            assert_eq!(par_f.len(), full.len(), "pax={pax} 投影行数与整行一致");
            for (d, sub_bytes) in par_f {
                let sub: serde_json::Value = serde_json::from_slice(&sub_bytes).unwrap();
                let whole: serde_json::Value =
                    serde_json::from_slice(full.get(&d).unwrap()).unwrap();
                assert_eq!(
                    sub.get("v"),
                    whole.get("v"),
                    "pax={pax} docid={d} 投影列 v 值一致"
                );
            }
        }
    }


    // ---------- 批量导入模式（P40） ----------

    #[test]
    fn bulk_import_skips_hotcache() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        // 默认：写后回填 HotCache
        e.put_nosync(1, b"doc-1".to_vec(), &["t"]).unwrap();
        assert_eq!(e.hotcache.len(), 1, "默认写后应回填热缓存");
        // 批量导入模式：只写不读，跳过回填（P40 防缓存膨胀挤爆内存）
        e.set_bulk_import(true);
        e.put_nosync(2, b"doc-2".to_vec(), &["t"]).unwrap();
        assert_eq!(e.hotcache.len(), 1, "批量导入模式不应回填热缓存");
        // 关闭后恢复常规语义
        e.set_bulk_import(false);
        e.put_nosync(3, b"doc-3".to_vec(), &["t"]).unwrap();
        assert_eq!(e.hotcache.len(), 2, "关闭后应恢复回填");
        // 主数据不受影响：批量写入的文档仍可正常读取
        e.set_bulk_import(true);
        e.put_nosync(4, b"doc-4".to_vec(), &["t"]).unwrap();
        assert_eq!(e.hotcache.len(), 2);
        assert_eq!(e.get(4).unwrap().unwrap(), b"doc-4");
    }

    // ---------- fulltext 分词索引（M8-P7） ----------

    #[test]
    fn fulltext_search_finds_long_text_and_persists() {
        // 核心动机：>96B 长文本整串被 max_term_len 跳过，分词词 term 短 → 长文本可检索
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.inverted.fulltext_fields = vec!["big_text".into()];
        let val = serde_json::json!({"docid": 42, "status": "active", "big_text": format!("{:<300}", "rec-00000042-msg-777")});
        let ft = c.inverted.fulltext_fields.iter().cloned().collect();
        let terms =
            crate::server::extract_terms_with_fulltext(&val, None, Some(&ft));
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        {
            let mut e = Engine::open(dir.path(), &c).unwrap();
            e.put(42, serde_json::to_vec(&val).unwrap(), &t).unwrap();
            e.flush_inverted().unwrap();
            // 词 term 可检索（整串 300B > max_term_len=96 会被跳过，词 term 不受影响）
            assert_eq!(e.fulltext_search("big_text", "rec").unwrap().len(), 1);
            assert_eq!(e.fulltext_search("big_text", "777").unwrap().len(), 1);
            // 非 fulltext 字段整串 term 照常
            assert_eq!(e.inverted_doc_count("status=active").unwrap(), 1);
        }
        // 刷盘 + 重开：词 term 持久化可查（倒排段已落盘）
        let mut e2 = Engine::open(dir.path(), &c).unwrap();
        assert_eq!(e2.fulltext_search("big_text", "777").unwrap().len(), 1);
        let hits = e2.fulltext_search("big_text", "00000042").unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, 42);
    }

    #[test]
    fn cjk_fulltext_searchable_via_bigram() {
        // M8-P9：中文整串不再当单 token（无法检索）——bigram 分词后 2-4 字关键词可检索
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.inverted.fulltext_fields = vec!["content".into()];
        let mut e = Engine::open(dir.path(), &c).unwrap();
        let docs = [
            (1u64, "山水存迹数据库存储引擎"),
            (2u64, "基于Rust的LSM树文档数据库"),
            (3u64, "分布式缓存系统设计"),
        ];
        for (id, content) in docs {
            let val = serde_json::json!({"docid": id, "content": content});
            let ft = e.fulltext_fields().clone();
            let terms = crate::server::extract_terms_with_fulltext(&val, None, Some(&ft));
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put_nosync(id, serde_json::to_vec(&val).unwrap(), &t).unwrap();
        }
        e.flush_inverted().unwrap();

        // 3 字关键词"数据库" → bigram 数据/据库（AND 交集精确命中含该词的文档 1、2）
        let d1 = e.inverted_posting("ft:content:数据").unwrap();
        let d2 = e.inverted_posting("ft:content:据库").unwrap();
        let and = d1 & d2;
        assert_eq!(and.len(), 2);
        assert!(and.contains(1) && and.contains(2));
        // 4 字关键词"山水存迹" → 3 个 bigram AND → 只命中 doc 1
        let inter = e.inverted_posting("ft:content:山水").unwrap()
            & e.inverted_posting("ft:content:水存").unwrap()
            & e.inverted_posting("ft:content:存迹").unwrap();
        assert_eq!(inter.len(), 1);
        assert!(inter.contains(1));
        // fulltext_search 回表
        let hits = e.fulltext_search("content", "数据").unwrap();
        assert!(hits.iter().any(|(id, _)| *id == 1 || *id == 2));
    }

    #[test]
    fn fulltext_survives_inverted_whitelist() {
        // M8-P4/P7 正交：inverted_fields 白名单非空时，ft: 词 term 不受白名单过滤
        // （否则长文本分词索引被白名单误滤，fulltext 检索恒空）
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.inverted.inverted_fields = vec!["status".into()]; // 白名单只建 status
        c.inverted.fulltext_fields = vec!["content".into()];
        let mut e = Engine::open(dir.path(), &c).unwrap();
        let val = serde_json::json!({"docid": 1, "status": "active", "content": "山水存迹"});
        let ft = e.fulltext_fields().clone();
        let terms = crate::server::extract_terms_with_fulltext(&val, None, Some(&ft));
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        e.put_nosync(1, serde_json::to_vec(&val).unwrap(), &t).unwrap();
        e.flush_inverted().unwrap();
        // 白名单字段 term 建了；ft: 词 term 也建了（不被滤掉）
        assert!(e.inverted_posting("status=active").unwrap().contains(1));
        assert!(e.inverted_posting("ft:content:山水").unwrap().contains(1));
        assert_eq!(e.fulltext_search("content", "山水").unwrap().len(), 1);
    }

    #[test]
    fn jieba_fulltext_meaningful_word_hit() {
        // M8-P13：jieba 词典分词——"数据库"单 term 精确命中（bigram 需 数据+据库 AND）
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.inverted.fulltext_fields = vec!["content".into()];
        c.inverted.cjk_segmenter = "jieba".into();
        let mut e = Engine::open(dir.path(), &c).unwrap();
        assert!(e.use_jieba(), "cjk_segmenter=jieba 应启用 jieba 分词");
        let docs = [
            (1u64, "山水存迹数据库存储引擎"),
            (2u64, "基于Rust的LSM树文档数据库"),
            (3u64, "分布式缓存系统设计"),
        ];
        for (id, content) in docs {
            let val = serde_json::json!({"docid": id, "content": content});
            let ft = e.fulltext_fields().clone();
            let terms =
                crate::server::extract_terms_with_fulltext_seg(&val, None, Some(&ft), e.use_jieba());
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put_nosync(id, serde_json::to_vec(&val).unwrap(), &t).unwrap();
        }
        e.flush_inverted().unwrap();
        // jieba 词典词"数据库"单 term 精确命中 2 个文档
        assert_eq!(e.inverted_posting("ft:content:数据库").unwrap().len(), 2);
        let hits = e.fulltext_search("content", "数据库").unwrap();
        assert_eq!(hits.len(), 2);
        // 无该词 → 0
        assert_eq!(e.fulltext_search("content", "缓存系统").unwrap().len(), 0);
    }

    // ---------- 分页查询（M8-P8） ----------

    #[test]
    fn paged_queries_semantics_and_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.inverted.fulltext_fields = vec!["big_text".into()];
        let mut e = Engine::open(dir.path(), &c).unwrap();
        // 100 docs：status 三态（active 34，docid 等差 3），big_text 每行唯一 token
        for i in 0..100u64 {
            let status = ["active", "inactive", "pending"][(i % 3) as usize];
            let val = serde_json::json!({
                "docid": i,
                "status": status,
                "big_text": format!("rec-{i:08}-msg-{i}"),
            });
            let ft = e.fulltext_fields().clone();
            let terms = crate::server::extract_terms_with_fulltext(&val, None, Some(&ft));
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put_nosync(i, serde_json::to_vec(&val).unwrap(), &t).unwrap();
        }
        e.flush_inverted().unwrap();

        // 全量 total + 行数
        let all = e.search_term_paged("status=active", None, 0).unwrap();
        assert_eq!(all.total, 34);
        assert_eq!(all.rows.len(), 34);
        // 分页：total 恒为全量命中数，rows 只含当前页，docid 升序接续
        let p1 = e.search_term_paged("status=active", Some(10), 0).unwrap();
        let p2 = e.search_term_paged("status=active", Some(10), 10).unwrap();
        assert_eq!(p1.total, 34);
        assert_eq!(p1.rows.len(), 10);
        assert_eq!(p2.rows.len(), 10);
        assert_eq!(p2.rows[0].0, p1.rows[9].0 + 3, "active docid 等差 3 接续");
        // 拼接 == 全量（有序稳定）
        let mut merged = p1.rows.clone();
        merged.extend(p2.rows);
        for off in [20u64, 30] {
            let p = e.search_term_paged("status=active", Some(10), off).unwrap();
            merged.extend(p.rows);
        }
        assert_eq!(merged.len(), 34);
        for (a, b) in merged.iter().zip(all.rows.iter()) {
            assert_eq!(a.0, b.0, "分页拼接必须与全量一致");
        }
        // 边界：limit=0 → 空页 total 不变；offset > total → 空页；limit > total → 全部
        let z = e.search_term_paged("status=active", Some(0), 0).unwrap();
        assert_eq!(z.total, 34);
        assert!(z.rows.is_empty());
        let o = e.search_term_paged("status=active", Some(5), 100).unwrap();
        assert!(o.rows.is_empty());
        let big = e.search_term_paged("status=active", Some(10_000), 0).unwrap();
        assert_eq!(big.rows.len(), 34);

        // fulltext 分页同语义
        let ft = e.fulltext_search_paged("big_text", "rec", Some(10), 90).unwrap();
        assert_eq!(ft.total, 100);
        assert_eq!(ft.rows.len(), 10);
        assert_eq!(ft.rows[0].0, 90);
        // scan 分页
        let sc = e.scan_range_paged(Some(10), Some(50), Some(5), 0).unwrap();
        assert_eq!(sc.total, 41);
        assert_eq!(sc.rows.len(), 5);
        assert_eq!(sc.rows[0].0, 10);
    }

    #[test]
    fn scan_after_cursor_traversal() {
        // M8-P11：游标续扫——遍历一致性 / 边界 / 提前终止（无 total 全扫）
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=100u64 {
            let val = serde_json::json!({"docid": i, "v": i});
            e.put_nosync(i, serde_json::to_vec(&val).unwrap(), &[]).unwrap();
            if i % 25 == 0 {
                e.flush_primary().unwrap();
            }
        }
        e.flush_primary().unwrap();
        // 游标遍历：limit=10 逐页续扫，拼接 == 全量升序
        let mut merged: Vec<u64> = Vec::new();
        let mut after: Option<u64> = None;
        loop {
            let page: Vec<u64> = e
                .scan_after(after, None, 10)
                .unwrap()
                .into_iter()
                .map(|(d, _)| d)
                .collect();
            if page.is_empty() {
                break;
            }
            merged.extend(page.iter().copied());
            after = Some(*page.last().unwrap());
        }
        assert_eq!(merged.len(), 100);
        assert_eq!(merged, (1..=100).collect::<Vec<u64>>(), "游标遍历覆盖全部且升序");
        // 边界：after=0 → 从 1 起；after=100 → 空；尾部不足 limit
        let from0: Vec<u64> = e
            .scan_after(Some(0), None, 3)
            .unwrap()
            .into_iter()
            .map(|(d, _)| d)
            .collect();
        assert_eq!(from0, vec![1, 2, 3]);
        assert!(e.scan_after(Some(100), None, 10).unwrap().is_empty());
        assert_eq!(
            e.scan_after(Some(97), None, 10).unwrap().len(),
            3,
            "尾部不足一页取剩余"
        );
        // 上界限定：after=0 & end=50 → 1..=50
        let bounded: Vec<u64> = e
            .scan_after(Some(0), Some(50), 100)
            .unwrap()
            .into_iter()
            .map(|(d, _)| d)
            .collect();
        assert_eq!(bounded, (1..=50).collect::<Vec<u64>>());
    }

    #[test]
    fn group_commit_drop_persists_all() {
        // 开启 2ms 窗口：快速 put 全部攒批，drop 最终落盘 → 重开数据完整
        let dir = tempfile::tempdir().unwrap();
        {
            let mut e = Engine::open(dir.path(), &gc_cfg(2_000)).unwrap();
            assert!(e.group_commit.is_some());
            for i in 0..100u64 {
                e.put(i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
            }
        }
        let mut e2 = Engine::open(dir.path(), &cfg()).unwrap();
        assert_eq!(e2.get(99).unwrap().unwrap(), b"doc-99");
        assert_eq!(e2.get(0).unwrap().unwrap(), b"doc-0");
    }

    #[test]
    fn gc_thread_flushes_tail_without_new_writes() {
        // 后台兜底线程：单条写后无新写，窗口到期也应落盘（WAL 待刷缓冲清零）
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &gc_cfg(2_000)).unwrap();
        e.put(1, b"doc-1".to_vec(), &["t"]).unwrap();
        // 窗口内第 1 条通常不触发 fsync（有待刷缓冲）；有界轮询等后台线程兜底落盘
        // （并行测试负载下后台线程调度可能延迟，固定 sleep 易 flaky）
        let mut pending = 1usize;
        for _ in 0..200 {
            std::thread::sleep(std::time::Duration::from_millis(10));
            pending = e.primary.wal_handle().lock().unwrap().pending_bytes()
                + e.delta.wal_handle().lock().unwrap().pending_bytes();
            if pending == 0 {
                break;
            }
        }
        assert_eq!(pending, 0, "后台线程应在窗口内兜底落盘");
    }

    #[test]
    fn backup_incremental_flushes_before_export() {
        // 组提交开启下 backup_incremental 前置 flush：未手动 flush 也应导出全部记录
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &gc_cfg(2_000)).unwrap();
        for i in 0..3u64 {
            e.put(i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
        }
        let bak = dir.path().join("incr.json");
        let rep = e.backup_incremental(0, &bak).unwrap();
        assert_eq!(rep.records, 3, "组提交开启下备份应前置落盘并导出全部");
    }

    #[test]
    fn group_commit_window_batches_fsync() {
        // 行为验证：窗口内连续 put 不逐条 fsync（WAL 待刷字节在窗口内累积，
        // 直到窗口到期或字节阈值触发一次性落盘）。写 100 条后窗口未到期时缓冲非空。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &gc_cfg(60_000)).unwrap(); // 60ms 大窗口
        let mut synced = 0usize;
        let mut missed = 0usize;
        for i in 0..50u64 {
            e.put(i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
            let pending = e.primary.wal_handle().lock().unwrap().pending_bytes();
            if pending == 0 {
                synced += 1; // 已落盘（窗口到期/阈值触发）
            } else {
                missed += 1; // 攒批中（未 fsync）
            }
        }
        // 60ms 窗口 + 4KB 阈值：快速 50 条 put（远快于窗口）绝大部分应攒批
        assert!(missed >= 40, "窗口内应攒批（攒批 {missed}，同步 {synced}）");
    }

    // ---------- P2-A：事务 COMMIT 耐久档位（flush_log_at_trx_commit）----------

    fn wal_pending(e: &Engine) -> usize {
        e.primary.wal_handle().lock().unwrap().pending_bytes()
            + e.delta.wal_handle().lock().unwrap().pending_bytes()
    }

    #[test]
    fn txn_commit_durability1_fsyncs_each_commit_even_with_group_commit() {
        // P2-A ① 核对结论的代码证据（原逐 COMMIT 等 fsync 的根因）：档位 1（默认）下
        // 事务 COMMIT 不落组提交攒批——即使组提交开启（后台窗口 60ms），COMMIT 后
        // WAL 待刷缓冲必为 0（每次显式 fsync，组提交仅并发摊薄）。
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = gc_cfg(60_000);
        cfg.storage.flush_log_at_trx_commit = 1;
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let mut txn = e.txn_begin(crate::txn::Isolation::ReadCommitted);
        txn.put(7, b"txn-7".to_vec(), vec!["t".into()]);
        e.txn_commit(txn).unwrap();
        assert_eq!(
            wal_pending(&e),
            0,
            "档位 1：COMMIT 必须显式 fsync（绕开组提交攒批，逐 COMMIT 落盘）"
        );
    }

    #[test]
    fn txn_commit_durability2_defers_to_group_commit_window() {
        // P2-A ②：档位 2 下 COMMIT 落组提交路径（不再逐 COMMIT 等 fsync）——大窗口内
        // COMMIT 后 WAL 仍攒批（未 fsync），由后台线程窗口到期兜底落盘；窗口后重开完整。
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = gc_cfg(60_000);
        cfg.storage.flush_log_at_trx_commit = 2;
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let mut txn = e.txn_begin(crate::txn::Isolation::ReadCommitted);
        txn.put(7, b"txn-7".to_vec(), vec!["t".into()]);
        e.txn_commit(txn).unwrap();
        let pending = wal_pending(&e);
        assert!(pending > 0, "档位 2：COMMIT 应落组提交攒批（实际 pending={pending}）");
        // 有界轮询等后台线程兜底落盘（避免固定 sleep flaky）
        let mut pending = pending;
        for _ in 0..200 {
            std::thread::sleep(std::time::Duration::from_millis(10));
            pending = wal_pending(&e);
            if pending == 0 {
                break;
            }
        }
        assert_eq!(pending, 0, "后台线程应在窗口内兜底落盘");
        drop(e);
        let mut e2 = Engine::open(dir.path(), &cfg).unwrap();
        assert_eq!(e2.get(7).unwrap().unwrap(), b"txn-7", "窗口落盘后重开数据完整");
    }

    #[test]
    fn txn_commit_durability2_falls_back_when_group_commit_off() {
        // P2-A ②：档位 2 但组提交关闭（无后台落盘线程）——maybe_group_commit 回退
        // flush_wal → COMMIT 仍显式 fsync（强安全兜底，语义不劣化）。
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.storage.group_commit_us = 0; // 显式关组提交（兜底路径前提）
        cfg.storage.flush_log_at_trx_commit = 2;
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let mut txn = e.txn_begin(crate::txn::Isolation::ReadCommitted);
        txn.put(7, b"txn-7".to_vec(), vec!["t".into()]);
        e.txn_commit(txn).unwrap();
        assert_eq!(wal_pending(&e), 0, "档位 0/2 + 组提交关：应回退显式 fsync");
    }

    #[test]
    fn flush_log_at_trx_commit_invalid_rejected_by_validate() {
        // P2-A ②：config 校验——档位仅接受 0/1/2（对齐 MySQL innodb_flush_log_at_trx_commit）
        let mut bad = Config::default();
        bad.storage.flush_log_at_trx_commit = 3;
        assert!(bad.validate().is_err(), "档位 3 应被校验拒绝");
        let mut ok2 = Config::default();
        ok2.storage.flush_log_at_trx_commit = 2;
        assert!(ok2.validate().is_ok());
        let mut ok0 = Config::default();
        ok0.storage.flush_log_at_trx_commit = 0;
        assert!(ok0.validate().is_ok());
    }

    // ---------- 倒排字段白名单/黑名单/长文本（M8-P4） ----------

    #[test]
    fn inverted_whitelist_only_indexes_declared_fields() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.inverted.inverted_fields = vec!["status".into(), "city".into()];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let doc = json!({"docid": 1, "status": "active", "city": "beijing", "name": "alice"});
        let terms = crate::server::extract_terms(&doc);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        e.put(1, serde_json::to_vec(&doc).unwrap(), &t).unwrap();
        e.flush_inverted().unwrap();
        // 白名单字段可查
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 1);
        assert_eq!(e.inverted_doc_count("city=beijing").unwrap(), 1);
        // 非白名单字段不建倒排
        assert_eq!(e.inverted_doc_count("name=alice").unwrap(), 0);
    }

    #[test]
    fn inverted_exclude_skips_fields() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.inverted.exclude_fields = vec!["big_text".into()];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let doc = json!({"docid": 1, "status": "active", "big_text": "hello world"});
        let terms = crate::server::extract_terms(&doc);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        e.put(1, serde_json::to_vec(&doc).unwrap(), &t).unwrap();
        e.flush_inverted().unwrap();
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 1);
        assert_eq!(
            e.inverted_doc_count("big_text=hello world").unwrap(),
            0,
            "黑名单字段不建倒排"
        );
    }

    #[test]
    fn inverted_max_term_len_skips_long_text() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.inverted.max_term_len = 16; // 16 字节以上 term 自动跳过（长文本整串保护）
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let long = "x".repeat(100);
        let doc = json!({"docid": 1, "status": "active", "payload": long});
        let terms = crate::server::extract_terms(&doc);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        e.put(1, serde_json::to_vec(&doc).unwrap(), &t).unwrap();
        e.flush_inverted().unwrap();
        assert_eq!(
            e.inverted_doc_count("status=active").unwrap(),
            1,
            "短字段仍建"
        );
        assert_eq!(
            e.inverted_doc_count("payload=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx").unwrap(),
            0,
            "超长 term 自动跳过（防长文本膨胀）"
        );
    }

    #[test]
    fn inverted_default_all_fields_built() {
        // 默认配置（白名单空）：短字符串字段全建；仅超长 term（>96）自动跳过
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        let doc = json!({"docid": 1, "status": "active", "city": "beijing"});
        let terms = crate::server::extract_terms(&doc);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        e.put(1, serde_json::to_vec(&doc).unwrap(), &t).unwrap();
        e.flush_inverted().unwrap();
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 1);
        assert_eq!(e.inverted_doc_count("city=beijing").unwrap(), 1);
    }

    #[test]
    fn put_get_roundtrip() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"doc-1".to_vec(), &["rust"]).unwrap();
        e.put(2, b"doc-2".to_vec(), &["go"]).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"doc-1");
        assert_eq!(e.get(2).unwrap().unwrap(), b"doc-2");
        assert!(e.get(99).unwrap().is_none());
    }

    #[test]
    fn put_batch_atomic_batch_visible_and_indexed() {
        // put_batch：批量原子提交——全部可见、倒排可查、覆盖语义正确（D 项 WriteBatch 前置）
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        let items: Vec<(u64, Vec<u8>, Vec<String>)> = (1..=100u64)
            .map(|i| {
                (
                    i,
                    format!("doc-{i}").into_bytes(),
                    vec![format!("status={}", if i % 2 == 0 { "active" } else { "inactive" })],
                )
            })
            .collect();
        e.put_batch(&items).unwrap();
        // 全部可见
        assert_eq!(e.get(1).unwrap().unwrap(), b"doc-1");
        assert_eq!(e.get(100).unwrap().unwrap(), b"doc-100");
        assert!(e.get(101).unwrap().is_none());
        // 倒排可查（查询自动刷 pending）
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 50);
        assert_eq!(e.inverted_doc_count("status=inactive").unwrap(), 50);
        // 覆盖：同批内后写覆盖前写
        let overwrite: Vec<(u64, Vec<u8>, Vec<String>)> =
            vec![(1, b"doc-1-v2".to_vec(), vec![])];
        e.put_batch(&overwrite).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"doc-1-v2");
    }

    #[test]
    fn inverted_batch_pending_flush() {
        // Ex-5.3：put 攒批缓冲——未达阈值时 term 留在 pending，查询自动刷入（一致性）；
        // inverted_mem_docids 统计含 pending；flush_inverted 后落盘、统计归零。
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        for i in 0..100u64 {
            e.put(i, b"v".to_vec(), &["status=active"]).unwrap();
        }
        // 未达阈值（8192）→ pending 未刷入，但统计应含 pending
        assert_eq!(e.inverted_mem_docids(), 100, "统计应含攒批缓冲");
        // 查询自动刷入 → 立即可见
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 100);
        // 查询后 pending 已清空
        assert_eq!(e.pending_inverted.lock().unwrap().len(), 0, "查询后缓冲应清空");
        // flush_inverted 落盘：内存归零、计数仍正确
        e.flush_inverted().unwrap();
        assert_eq!(e.inverted_mem_docids(), 0, "落盘后内存统计归零");
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 100);
    }

    #[test]
    fn delete_hides_doc() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(7, b"x".to_vec(), &["k"]).unwrap();
        e.delete(7).unwrap();
        assert!(e.get(7).unwrap().is_none());
    }

    #[test]
    fn delete_batch_range_removes_all_and_idempotent() {
        // delete_range50 修复：批量删语义与逐行 delete 一致（位图路径 + Tombstone 路径双验证）：
        // ① 命中行全部不可见；② 重复批量删幂等（不报错、计数不膨胀）；
        // ③ 与 scan_stream_ids 可见集一致（无残留、无复活）。
        for bitmap_on in [true, false] {
            let mut c = cfg();
            c.storage.deletion_bitmap_enabled = bitmap_on;
            let mut e = Engine::open(&tmp(), &c).unwrap();
            let items: Vec<(u64, Vec<u8>, Vec<String>)> = (0..50u64)
                .map(|i| {
                    (
                        i,
                        format!("doc-{i}").into_bytes(),
                        vec![format!("status={}", if i % 2 == 0 { "active" } else { "inactive" })],
                    )
                })
                .collect();
            e.put_batch(&items).unwrap();
            // 统计可见行数（keys-only 计数）
            let mut visible = 0u64;
            e.scan_stream_ids(None, None, |_| {
                visible += 1;
                Ok(true)
            })
            .unwrap();
            assert_eq!(visible, 50, "批量删前可见 50 行");
            // ① 范围批量删 [10, 30) → 20 行
            let n = e.delete_batch((10..30u64).into_iter()).unwrap();
            assert_eq!(n, 20);
            for i in 0..50u64 {
                let expect_none = (10..30).contains(&i);
                assert_eq!(e.get(i).unwrap().is_none(), expect_none, "docid={i} 可见性");
            }
            // 与逐行 delete 对比可见集：删 [30,50) 逐行 → 与 delete_batch 删除不可区分
            for i in 30..50u64 {
                e.delete(i).unwrap();
            }
            let mut left = 0u64;
            e.scan_stream_ids(None, None, |_| {
                left += 1;
                Ok(true)
            })
            .unwrap();
            assert_eq!(left, 10, "批量删+逐行删后仅剩 [0,10)");
            // ② 重复批量删幂等
            let n2 = e.delete_batch((0..50u64).into_iter()).unwrap();
            assert_eq!(n2, 50, "重复删仍消费全部迭代项（幂等，不报错）");
            let mut left2 = 0u64;
            e.scan_stream_ids(None, None, |_| {
                left2 += 1;
                Ok(true)
            })
            .unwrap();
            assert_eq!(left2, 0, "重复删后可见集为空");
        }
    }

    #[test]
    fn delete_batch_revive_put_clears_bitmap() {
        // 批量删后 put 复活（Ex-5.6 语义）：删除位图清位 + 墓碑版本链被覆盖 → get 恢复可见。
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"v1".to_vec(), &[]).unwrap();
        e.put(2, b"v2".to_vec(), &[]).unwrap();
        e.delete_batch(vec![1u64, 2].into_iter()).unwrap();
        assert!(e.get(1).unwrap().is_none());
        assert!(e.get(2).unwrap().is_none());
        // 复活
        e.put(1, b"v1-new".to_vec(), &[]).unwrap();
        e.put(2, b"v2-new".to_vec(), &[]).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"v1-new");
        assert_eq!(e.get(2).unwrap().unwrap(), b"v2-new");
    }

    #[test]
    fn search_term_returns_docs() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"about rust".to_vec(), &["rust"]).unwrap();
        e.put(2, b"rust rocks".to_vec(), &["rust"]).unwrap();
        e.put(3, b"go is cool".to_vec(), &["go"]).unwrap();
        let rows = e.search_term("rust").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[1].0, 2);
        assert!(e.search_term("go").unwrap().len() == 1);
        assert!(e.search_term("absent").unwrap().is_empty());
    }

    #[test]
    fn deleted_doc_excluded_from_search() {
        // 倒排残留 docid 回表时被主数据 Tombstone 过滤
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"rust".to_vec(), &["rust"]).unwrap();
        e.put(2, b"rust2".to_vec(), &["rust"]).unwrap();
        e.delete(1).unwrap();
        let rows = e.search_term("rust").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 2);
    }

    // ---------- MVCC 快照读（design 4.7 二期，M6-3） ----------

    #[test]
    fn get_at_reads_historical_version_after_flush() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        let put_doc = |e: &mut Engine, v: i64| {
            e.put(
                1,
                serde_json::to_vec(&json!({"docid": 1, "v": v})).unwrap(),
                &["v"],
            )
            .unwrap();
        };
        put_doc(&mut e, 1);
        let s1 = e.begin_snapshot();
        e.flush_primary().unwrap(); // v1 落 SST（历史版本保留）
        put_doc(&mut e, 2);
        // 快照读 → v1；最新读 → v2
        let snap: serde_json::Value =
            serde_json::from_slice(&e.get_at(1, s1).unwrap().unwrap()).unwrap();
        assert_eq!(snap["v"], 1, "快照应读回 v1");
        let cur: serde_json::Value = serde_json::from_slice(&e.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(cur["v"], 2);
    }

    #[test]
    fn get_at_ignores_writes_after_snapshot() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, br#"{"k":"base"}"#.to_vec(), &["k"]).unwrap();
        e.flush_primary().unwrap(); // base 落 SST，快照后可回读
        let s = e.begin_snapshot();
        // 快照之后主数据覆盖 + Delta 热更
        e.put(1, br#"{"k":"later"}"#.to_vec(), &["k"]).unwrap();
        e.patch(1, &[("extra", json!("x"))]).unwrap();
        // 快照读：主数据仍为 base，Delta 增量也隔离（M7-1 全局 seq）
        let snap: serde_json::Value =
            serde_json::from_slice(&e.get_at(1, s).unwrap().unwrap()).unwrap();
        assert_eq!(snap["k"], "base", "快照应隔离主数据后续覆盖");
        assert!(snap.get("extra").is_none(), "快照应隔离快照后的 Delta 热更");
        // 最新读：later + delta 叠加
        let cur: serde_json::Value = serde_json::from_slice(&e.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(cur["k"], "later");
        assert_eq!(cur["extra"], "x");
    }

    #[test]
    fn get_at_delta_isolated_by_global_seq() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, br#"{"a":1,"b":1}"#.to_vec(), &["k"]).unwrap();
        let s = e.begin_snapshot();
        // 快照后 Delta 修改字段 a（null 删除 b）
        e.patch(1, &[("a", json!(2)), ("b", json!(null))]).unwrap();
        // 快照读：a=1、b 仍存在（快照后增量不可见）
        let snap: serde_json::Value =
            serde_json::from_slice(&e.get_at(1, s).unwrap().unwrap()).unwrap();
        assert_eq!(snap["a"], 1, "快照应读回 a=1");
        assert_eq!(snap["b"], 1, "快照应保留被删除的 b");
        // 最新读：a=2、b 被 null 删除
        let cur: serde_json::Value = serde_json::from_slice(&e.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(cur["a"], 2);
        assert!(cur.get("b").is_none(), "最新读 b 应被 null 删除");
    }

    #[test]
    fn global_seq_resumes_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        e.put(1, br#"{"a":1}"#.to_vec(), &["k"]).unwrap();
        let s = e.begin_snapshot();
        e.patch(1, &[("a", json!(2))]).unwrap();
        // 重启：全局 seq 从 WAL 恢复
        drop(e);
        let mut e2 = Engine::open(dir.path(), &cfg).unwrap();
        // 快照点之前的读不受影响（重启后快照序号语义保持）
        let snap: serde_json::Value =
            serde_json::from_slice(&e2.get_at(1, s).unwrap().unwrap()).unwrap();
        assert_eq!(snap["a"], 1);
        let cur: serde_json::Value = serde_json::from_slice(&e2.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(cur["a"], 2);
        assert!(e2.begin_snapshot() >= s, "重启后全局 seq 应接续");
    }

    #[test]
    fn get_at_returns_none_after_delete_before_snapshot() {
        // P0-C 方案 B（2026-09-04）：快照读跳过删除位图，走 LSM 版本裁决。
        // ① 删除位图开启：快照在删除前 → tombstone seq > snapshot_seq → 返回旧值（RR 正确）
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"v1".to_vec(), &["k"]).unwrap();
        let s_before_delete = e.begin_snapshot();
        e.flush_primary().unwrap();
        e.delete(1).unwrap(); // tombstone seq > s_before_delete
        assert_eq!(
            e.get_at(1, s_before_delete).unwrap().unwrap().as_slice(),
            b"v1",
            "P0-C：快照在删除前应看到旧值（RR 正确，位图不短路）"
        );
        assert!(e.get(1).unwrap().is_none(), "最新读已删 → None");

        // ② 删除位图关闭：Tombstone + MVCC 语义——删除前快照仍可见 v1
        let mut c = cfg();
        c.storage.deletion_bitmap_enabled = false;
        let mut e2 = Engine::open(&tmp(), &c).unwrap();
        e2.put(1, b"v1".to_vec(), &["k"]).unwrap();
        let s2 = e2.begin_snapshot();
        e2.flush_primary().unwrap();
        e2.delete(1).unwrap(); // Tombstone seq > s2
        assert_eq!(e2.get_at(1, s2).unwrap().unwrap(), b"v1", "关闭位图保留 Tombstone 快照语义");
        assert!(e2.get(1).unwrap().is_none(), "删除后最新读 → 不存在");
    }

    /// R4（review 2026-09-04）：RR 快照读跨 compaction 保活——活跃快照在删除之前时，
    /// compact 保留旧版本（mvcc_keep_floor），快照读仍见 v1；无活跃快照时 compact 物理回收。
    #[test]
    fn rr_snapshot_survives_compaction_with_active_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v1".to_vec(), &["k"]).unwrap();
        e.flush_primary().unwrap(); // v1 落 SST（seq s1）

        // RR 事务 A 开始（活跃快照在删除之前）
        let mut txn_a = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        let snap_a = txn_a.snapshot();

        // 并发事务 B 删除 docid=1 并提交
        {
            let mut txn_b = e.txn_begin(crate::txn::Isolation::ReadCommitted);
            txn_b.delete(1);
            e.txn_commit(txn_b).unwrap();
        }
        // 删除 tombstone 落盘 + compaction（修复前：旧版本被收敛丢弃 → A 读 None）
        e.flush_primary().unwrap();
        assert!(e.compact().unwrap().merged_ssts > 0, "compact 应执行多段合并");

        // A 快照读（snapshot < tombstone seq）→ compact 保活后仍见 v1
        assert!(snap_a <= e.begin_snapshot(), "快照 seq 有效");
        assert_eq!(
            e.txn_get(&mut txn_a, 1).unwrap().as_deref(),
            Some(b"v1".as_slice()),
            "R4：活跃快照期间 compact 保活旧版本，快照读仍见 v1"
        );
        e.txn_commit(txn_a).unwrap();

        // 事务 A 结束（无活跃快照）后 GC：compact 物理回收 → 最新读仍 None
        let mut txn_c = e.txn_begin(crate::txn::Isolation::ReadCommitted);
        assert!(e.txn_get(&mut txn_c, 1).unwrap().is_none(), "删除后最新读 None");
        e.txn_commit(txn_c).unwrap();
    }

    /// R4：无活跃快照时 compact 收敛丢旧版本（现状语义）——旧 seq 快照读返回 None。
    #[test]
    fn rr_no_active_snapshot_compaction_drops_old_versions() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v1".to_vec(), &["k"]).unwrap();
        let s_old = e.begin_snapshot();
        e.flush_primary().unwrap();
        e.delete(1).unwrap();
        e.flush_primary().unwrap();
        // 无活跃快照 → compact 物理回收（删除位图 GC 路径）
        assert!(e.compact().unwrap().merged_ssts > 0);
        // 修复语义边界：GC 后旧版本已回收；快照读走 LSM 无旧版本 → None
        assert!(e.get_at(1, s_old).unwrap().is_none());
    }

    // ---------- 增量备份（design 20，M6-5） ----------

    #[test]
    fn incremental_backup_restore_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &crate::config::Config::default()).unwrap();
        let put_doc = |e: &mut Engine, docid: u64, v: &str| {
            let val = json!({"docid": docid, "v": v});
            let bytes = serde_json::to_vec(&val).unwrap();
            let terms = crate::server::extract_terms(&val);
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put(docid, bytes, &t).unwrap();
        };
        put_doc(&mut e, 1, "v1");
        put_doc(&mut e, 2, "v1");
        let full_point = e.current_seq(); // 全量备份点
        put_doc(&mut e, 3, "v3"); // 增量 1
        e.delete(1).unwrap(); // 增量 2（Tombstone）
        let bak = dir.path().join("incr.json");
        let rep = e.backup_incremental(full_point, &bak).unwrap();
        assert_eq!(rep.since_seq, full_point);
        assert_eq!(rep.records, 2, "应导出 2 条增量记录");

        // 模拟"全量还原"后的新引擎：先恢复全量点数据，再应用增量
        let dir2 = tempfile::tempdir().unwrap();
        let mut e2 = Engine::open(dir2.path(), &crate::config::Config::default()).unwrap();
        put_doc(&mut e2, 1, "v1");
        put_doc(&mut e2, 2, "v1");
        let n = e2.restore_incremental(&bak).unwrap();
        assert_eq!(n, 2);
        assert!(e2.get(1).unwrap().is_none(), "增量 Tombstone 应删除 doc1");
        assert!(e2.get(2).unwrap().is_some(), "doc2 保留（全量部分）");
        let v3: serde_json::Value = serde_json::from_slice(&e2.get(3).unwrap().unwrap()).unwrap();
        assert_eq!(v3["v"], "v3", "增量 PUT 应恢复 doc3");
    }

    #[test]
    fn incremental_backup_since_zero_exports_all() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &crate::config::Config::default()).unwrap();
        e.put(1, b"a".to_vec(), &["a"]).unwrap();
        e.flush_primary().unwrap(); // flush → WAL 截断（M8-P5）：已刷盘记录 1 从 WAL 删除
        e.put(2, b"b".to_vec(), &["b"]).unwrap();
        let bak = dir.path().join("incr-all.json");
        let rep = e.backup_incremental(0, &bak).unwrap();
        assert_eq!(
            rep.records, 1,
            "since=0 导出当前 WAL 全部记录（截断后 = 未刷盘记录 2；记录 1 已入 SST 由全量备份覆盖）"
        );
    }

    // ---------- Ex-9.3 第①步：写路径随 term 累积 stats（内存段） ----------

    #[test]
    fn inverted_stats_fields_accumulate_per_term() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.inverted.stats_fields = vec!["amount".to_string()];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let put = |e: &mut Engine, docid: u64, status: &str, amount: Option<f64>| {
            let mut doc = serde_json::json!({"status": status});
            if let Some(a) = amount {
                doc["amount"] = serde_json::json!(a);
            }
            let bytes = serde_json::to_vec(&doc).unwrap();
            let term: &[&str] = &[if status == "active" { "status=active" } else { "status=inactive" }];
            e.put_nosync(docid, bytes, term).unwrap();
        };
        put(&mut e, 1, "active", Some(10.0));
        put(&mut e, 2, "active", Some(20.0));
        put(&mut e, 3, "active", None); // 缺 amount → 跳过不计入
        put(&mut e, 4, "inactive", Some(5.0));
        // active：数值文档 10/20 → n2 sum30 min10 max20
        let a = e.inverted_term_stats("status=active").expect("active 应有统计");
        assert_eq!(a.len(), 1, "stats_fields 单字段");
        assert_eq!(a[0].n, 2, "缺字段文档不计入");
        assert_eq!(a[0].sum, 30.0);
        assert_eq!(a[0].min, 10.0);
        assert_eq!(a[0].max, 20.0);
        let b = e.inverted_term_stats("status=inactive").unwrap();
        assert_eq!(b[0].n, 1);
        assert_eq!(b[0].sum, 5.0);
        // 未声明 stats_fields（空）→ 不产生统计
        let dir2 = tempfile::tempdir().unwrap();
        let mut e2 = Engine::open(dir2.path(), &crate::config::Config::default()).unwrap();
        e2.put_nosync(1, br#"{"status":"active","amount":10}"#.to_vec(), &["status=active"])
            .unwrap();
        assert!(e2.inverted_term_stats("status=active").is_none(), "未配置则无统计");
    }

    #[test]
    fn inverted_stats_persist_across_flush_and_reopen_v5() {
        // Ex-9.3 第②步：stats 随段 v5 载荷落盘 → flush 后 / 重开库后仍可读（不再依赖内存）。
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.inverted.stats_fields = vec!["amount".to_string()];
        let put = |e: &mut Engine, docid: u64, status: &str, amount: Option<f64>| {
            let mut doc = serde_json::json!({"status": status});
            if let Some(a) = amount {
                doc["amount"] = serde_json::json!(a);
            }
            let bytes = serde_json::to_vec(&doc).unwrap();
            let term: &[&str] = &[if status == "active" { "status=active" } else { "status=inactive" }];
            e.put_nosync(docid, bytes, term).unwrap();
        };
        let check = |e: &mut Engine, label: &str| {
            let a = e.inverted_term_stats("status=active").expect(label);
            assert_eq!(a[0].n, 2, "{label}: active n");
            assert_eq!(a[0].sum, 30.0, "{label}: active sum");
            assert_eq!(a[0].min, 10.0, "{label}: min");
            assert_eq!(a[0].max, 20.0, "{label}: max");
            let b = e.inverted_term_stats("status=inactive").unwrap();
            assert_eq!(b[0].sum, 5.0, "{label}: inactive sum");
            assert_eq!(e.inverted_doc_count("status=active").unwrap(), 3, "{label}: count");
        };
        {
            let mut e = Engine::open(dir.path(), &cfg).unwrap();
            put(&mut e, 1, "active", Some(10.0));
            put(&mut e, 2, "active", Some(20.0));
            put(&mut e, 3, "active", None);
            put(&mut e, 4, "inactive", Some(5.0));
            check(&mut e, "flush 前（mem）");
            e.flush_inverted().unwrap(); // 段落盘 v5：含统计载荷
            check(&mut e, "flush 后（段）");
        }
        // 重开（内存清空，只读段）：载荷须从 v5 段读出
        let mut e2 = Engine::open(dir.path(), &cfg).unwrap();
        check(&mut e2, "重开后（v5 段）");
    }

    // ---------- 位图索引（design 5.2.4，M7-2） ----------

    #[test]
    fn bitmap_index_fast_path_for_count_group_and() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.inverted.bitmap_fields = vec!["status".into(), "city".into()];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let put_doc = |e: &mut Engine, docid: u64, status: &str, city: &str| {
            let val = json!({"docid": docid, "status": status, "city": city});
            let bytes = serde_json::to_vec(&val).unwrap();
            let terms = crate::server::extract_terms(&val);
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put(docid, bytes, &t).unwrap();
        };
        put_doc(&mut e, 1, "active", "beijing");
        put_doc(&mut e, 2, "inactive", "beijing");
        put_doc(&mut e, 3, "active", "shanghai");
        // COUNT 快速路径（内存位图）
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 2);
        assert_eq!(e.inverted_doc_count("city=beijing").unwrap(), 2);
        // AND 交集快速路径
        assert_eq!(
            e.inverted_bitmap_and_count(&["status=active", "city=beijing"])
                .unwrap(),
            1
        );
        // GROUP BY 快速路径
        let g = e.inverted_group_by("status").unwrap();
        assert!(g.contains(&("active".to_string(), 2)));
        assert!(g.contains(&("inactive".to_string(), 1)));
        // 重启后位图从段重建，COUNT 仍正确（drop 前刷盘倒排，保证段自包含）
        e.flush_inverted().unwrap();
        drop(e);
        let mut e2 = Engine::open(dir.path(), &cfg).unwrap();
        assert_eq!(e2.inverted_doc_count("status=active").unwrap(), 2);
    }

    #[test]
    fn execute_routes_by_spec() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(42, b"hello world".to_vec(), &["hello"]).unwrap();

        // 主键点查
        let spec = QuerySpec {
            primary_eq: Some(crate::keys::encode_docid(42).to_vec()),
            primary_range: false,
            index_prefix: vec![],
            term: None,
        };
        let rows = e.execute(&spec).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 42);

        // 倒排词条查询
        let spec2 = QuerySpec {
            primary_eq: None,
            primary_range: false,
            index_prefix: vec![],
            term: Some("hello".into()),
        };
        let rows2 = e.execute(&spec2).unwrap();
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0].1, b"hello world");
    }

    #[test]
    fn composite_prefix_query() {
        // 直接向 cidx CF 注入索引条目的夹具（绕过 engine 写路径）→ legacy 模式
        let mut c = cfg();
        c.storage.per_cpu_enabled = false;
        let mut e = Engine::open(&tmp(), &c).unwrap();
        // 组合索引：写入索引条目（key = composite(fields, docid)）
        if let Some(cidx) = &mut e.cidx {
            for docid in [10u64, 20, 30] {
                let key = crate::keys::encode_composite_key(&[b"active"], docid);
                cidx.put_bytes(key, docid.to_le_bytes().to_vec()).unwrap();
            }
            let key = crate::keys::encode_composite_key(&[b"inactive"], 99);
            cidx.put_bytes(key, 99u64.to_le_bytes().to_vec()).unwrap();
        }
        // 主数据写文档（回表需要）
        e.put(10, b"d10".to_vec(), &[]).unwrap();
        e.put(20, b"d20".to_vec(), &[]).unwrap();
        e.put(30, b"d30".to_vec(), &[]).unwrap();
        e.put(99, b"d99".to_vec(), &[]).unwrap();

        let rows = e.query_by_composite_prefix(&[b"active"]).unwrap();
        let mut ids: Vec<u64> = rows.iter().map(|(d, _)| *d).collect();
        ids.sort();
        assert_eq!(ids, vec![10, 20, 30]);
    }

    /// P0-A：声明式组合索引——put 自动写 cidx + query_by_composite_prefix 前缀扫描。
    #[test]
    fn composite_index_auto_write_and_query() {
        let mut c = cfg();
        c.storage.composite_indexes = vec![vec!["status".into(), "region".into()]];
        let mut e = Engine::open(&tmp(), &c).unwrap();

        // 写入文档（put 自动提取 status/region 写入 cidx）
        e.put(
            1,
            br#"{"status":"active","region":"east","amount":100}"#.to_vec(),
            &[],
        )
        .unwrap();
        e.put(
            2,
            br#"{"status":"active","region":"west","amount":200}"#.to_vec(),
            &[],
        )
        .unwrap();
        e.put(
            3,
            br#"{"status":"inactive","region":"east","amount":300}"#.to_vec(),
            &[],
        )
        .unwrap();
        e.flush_wal().unwrap();

        // 前缀 [active] → docid 1, 2
        let rows = e.query_by_composite_prefix(&[b"active"]).unwrap();
        let mut ids: Vec<u64> = rows.iter().map(|(d, _)| *d).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2]);

        // 前缀 [active, east] → docid 1
        let rows = e.query_by_composite_prefix(&[b"active", b"east"]).unwrap();
        let ids: Vec<u64> = rows.iter().map(|(d, _)| *d).collect();
        assert_eq!(ids, vec![1]);

        // 前缀 [inactive] → docid 3
        let rows = e.query_by_composite_prefix(&[b"inactive"]).unwrap();
        let ids: Vec<u64> = rows.iter().map(|(d, _)| *d).collect();
        assert_eq!(ids, vec![3]);
    }

    #[test]
    fn oom_guardian_blocks_writes_at_stall() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.set_mem_ratio(0.5);
        e.put(1, b"ok".to_vec(), &[]).unwrap(); // 正常写入

        e.set_mem_ratio(1.0); // 模拟 RSS 打满
        let err = e.put(2, b"blocked".to_vec(), &[]).unwrap_err();
        assert!(matches!(err, crate::error::Error::MemoryOverload(_)));
        // 被拒写入不生效
        assert!(e.get(2).unwrap().is_none());
        assert!(e.get(1).unwrap().is_some());
    }

    #[test]
    fn patch_merge_on_read() {
        // 阶段 1.5 Delta CF：patch 部分更新 → get 合并覆盖；重启后 WAL 恢复仍生效
        let dir = tmp();
        let mut e = Engine::open(&dir, &cfg()).unwrap();
        e.put(
            1,
            br#"{"status":"active","amount":100,"device":"android"}"#.to_vec(),
            &[],
        )
        .unwrap();
        e.patch(
            1,
            &[
                ("status", serde_json::json!("inactive")),
                ("note", serde_json::json!("updated")),
                ("amount", serde_json::Value::Null), // null = 删除字段
            ],
        )
        .unwrap();
        let v = e.get(1).unwrap().unwrap();
        let obj: serde_json::Value = serde_json::from_slice(&v).unwrap();
        assert_eq!(obj["status"], serde_json::json!("inactive"), "patch 应覆盖");
        assert_eq!(obj["note"], serde_json::json!("updated"), "patch 应新增");
        assert!(obj.get("amount").is_none(), "null patch 应删除字段");
        assert_eq!(
            obj["device"],
            serde_json::json!("android"),
            "未 patch 字段保留"
        );
        // 重启后 Delta 仍生效（WAL 恢复）
        drop(e);
        let mut e2 = Engine::open(&dir, &cfg()).unwrap();
        let obj2: serde_json::Value = serde_json::from_slice(&e2.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(obj2["status"], serde_json::json!("inactive"));
        assert_eq!(obj2["note"], serde_json::json!("updated"));
    }

    #[test]
    fn full_put_clears_delta() {
        // 全量 put 覆盖 → 清空该 docid 增量，避免旧 patch 覆盖新数据
        let dir = tmp();
        let mut e = Engine::open(&dir, &cfg()).unwrap();
        e.put(1, br#"{"status":"active","amount":100}"#.to_vec(), &[])
            .unwrap();
        e.patch(1, &[("status", serde_json::json!("patched"))])
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&e.get(1).unwrap().unwrap()),
            r#"{"status":"patched","amount":100}"#
        );
        e.put(1, br#"{"status":"fresh","amount":200}"#.to_vec(), &[])
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&e.get(1).unwrap().unwrap()),
            r#"{"status":"fresh","amount":200}"#
        );
    }

    #[test]
    fn delete_clears_delta() {
        // 删除文档 → Delta 清空，避免复活
        let dir = tmp();
        let mut e = Engine::open(&dir, &cfg()).unwrap();
        e.put(1, br#"{"status":"active"}"#.to_vec(), &[]).unwrap();
        e.patch(1, &[("note", serde_json::json!("x"))]).unwrap();
        e.delete(1).unwrap();
        assert!(e.get(1).unwrap().is_none(), "删除后 Delta 不应复活文档");
    }

    // ---------- Ex-5.6 删除位图 ----------

    #[test]
    fn multi_ssd_striping_places_files() {
        // Ex-5.10 多 SSD 条带化：wal_dir/sst_dir/inverted_dir 分盘——WAL/SST/倒排文件
        // 落位正确 + 数据跨重启恢复（默认单盘布局由既有测试覆盖）
        let data_dir = tempfile::tempdir().unwrap();
        let wal_dir = tempfile::tempdir().unwrap();
        let sst_dir = tempfile::tempdir().unwrap();
        let inv_dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.storage.wal_dir = Some(wal_dir.path().to_string_lossy().to_string());
        cfg.storage.sst_dir = Some(sst_dir.path().to_string_lossy().to_string());
        cfg.storage.inverted_dir = Some(inv_dir.path().to_string_lossy().to_string());
        {
            let mut e = Engine::open(data_dir.path(), &cfg).unwrap();
            e.put(1, b"doc1".to_vec(), &["rust"]).unwrap();
            e.put(2, b"doc2".to_vec(), &["go"]).unwrap();
            e.flush_primary().unwrap();
            e.flush_inverted().unwrap();
            assert_eq!(e.get(1).unwrap().unwrap(), b"doc1");
        }
        // 落位验证：WAL / SST / 倒排各在其盘
        assert!(
            wal_dir.path().join("primary").join("wal.log").exists(),
            "primary WAL 应落在 wal_dir"
        );
        let sst_files: Vec<_> = std::fs::read_dir(sst_dir.path().join("primary"))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(!sst_files.is_empty(), "SST 应落在 sst_dir");
        assert!(
            inv_dir.path().join("inverted").join("inverted-manifest.json").exists(),
            "倒排应在 inverted_dir"
        );
        assert!(!data_dir.path().join("primary").exists(), "列族目录已外移 sst_dir");
        // 跨重启恢复（多盘布局持久）
        let mut e2 = Engine::open(data_dir.path(), &cfg).unwrap();
        assert_eq!(e2.get(1).unwrap().unwrap(), b"doc1");
        assert_eq!(e2.get(2).unwrap().unwrap(), b"doc2");
        assert!(e2.search_term("rust").unwrap().len() == 1);
    }

    #[test]
    fn dynamic_io_rate_backs_off_with_write_pressure() {
        // Ex-7.4：MemTable 水位（写压力代理）升高 → Compaction 限速下调让路
        // （压力 p → base×(1-0.5p)，最低 50% 基准）
        let mut c = cfg();
        c.storage.io_rate_limit_mb = 100; // 100MB/s 基准
        c.memtable.max_size_mb = 1; // 1MB 小 MemTable → 快速产生写压力
        let mut e = Engine::open(&tmp(), &c).unwrap();
        let base = 100u64 * 1024 * 1024;
        assert_eq!(e.primary.io_rate(), base, "空 MemTable 压力 0 全速");
        // 写入 8 个 64KB 文档 ≈ 512KB（memtable 50% 水位）
        let big = vec![b'x'; 64 * 1024];
        for i in 0..8u64 {
            e.put(i, big.clone(), &[]).unwrap();
        }
        let rate = e.primary.io_rate();
        assert!(rate < base, "写压力下限速应下调: {rate} < {base}");
        assert!(rate >= base / 2, "不低于 50% 基准: {rate}");
        // flush 后水位回落 → 限速回升
        e.flush_primary().unwrap();
        let rate2 = e.primary.io_rate();
        assert!(rate2 >= rate, "flush 后水位降、限速回升: {rate2} >= {rate}");
    }

    #[test]
    fn outbox_e2e_enqueue_dispatch_drain() {
        // Ex-1 端到端：enqueue（与业务写同 seq 空间）→ 重启保留 → 幂等投递 → 排空
        let mut c = cfg();
        c.outbox.enabled = true;
        let dir = tmp();
        let mut consumer = crate::outbox::IdempotentConsumer::new();
        {
            let mut e = Engine::open(&dir, &c).unwrap();
            e.put(1, b"doc1".to_vec(), &["k"]).unwrap();
            e.enqueue_outbox(1, b"msg-1").unwrap();
            e.enqueue_outbox(2, b"msg-2").unwrap();
            e.flush_wal().unwrap();
            assert_eq!(e.outbox_pending().unwrap(), 2);
            // 幂等投递
            let n = e
                .dispatch_outbox(|k, p| {
                    assert!(p.starts_with(b"msg-"));
                    consumer.apply(k)
                })
                .unwrap();
            assert_eq!(n, 2);
            assert!(e.outbox_drained().unwrap(), "投递后排空");
        }
        // 重启：pending 保持 0（done 状态持久）
        let mut e2 = Engine::open(&dir, &c).unwrap();
        assert!(e2.outbox_drained().unwrap());
        assert_eq!(consumer.received(), 2);
    }

    #[test]
    fn outbox_disabled_by_default() {
        // outbox 默认关闭（零开销）：enqueue 返回 Unsupported、pending=0
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        assert!(e.enqueue_outbox(1, b"x").is_err(), "未启用应拒绝入队");
        assert_eq!(e.outbox_pending().unwrap(), 0);
        assert!(e.outbox_drained().unwrap());
    }

    #[test]
    fn outbox_pending_survives_restart() {
        // 崩溃恢复：enqueue 未投递 → 重开 pending 保留（WAL 回放重建）
        let mut c = cfg();
        c.outbox.enabled = true;
        let dir = tmp();
        {
            let mut e = Engine::open(&dir, &c).unwrap();
            e.enqueue_outbox(7, b"keep").unwrap();
            e.flush_wal().unwrap();
        }
        let mut e2 = Engine::open(&dir, &c).unwrap();
        assert_eq!(e2.outbox_pending().unwrap(), 1, "重开 pending 保留");
    }

    #[test]
    fn scale_out_coordinator_e2e_with_outbox() {
        // Ex-1.5 端到端：扩容协调器（状态机 + 路由切换）衔接 engine outbox——
        // 写主（业务+outbox 本地原子）→ 追平投递到新节点 → 排空校验 → 切换 → 新节点接管
        use crate::scale_out::{Phase, ScaleOutCoordinator};
        use std::sync::Mutex;
        let da = tempfile::tempdir().unwrap();
        let db = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.outbox.enabled = true;
        let a = Mutex::new(Engine::open(da.path(), &c).unwrap());
        let b = Mutex::new(Engine::open(db.path(), &c).unwrap());
        // 写主节点 + outbox 入队（本地原子：业务写与消息同 fsync 点）
        for i in 0..20u64 {
            let val = format!("doc-{i}").into_bytes();
            let mut a = a.lock().unwrap();
            a.put(i, val.clone(), &["status=active"]).unwrap();
            a.enqueue_outbox(i, &val).unwrap();
        }
        // 扩容编排开始（新节点注册为 slave）
        let mut meta = crate::meta::MetaCenter::new(4);
        meta.register("node-a", "127.0.0.1:9001", "master").unwrap();
        let mut coord = ScaleOutCoordinator::begin(
            &da.path().join("scale-out.json"),
            meta,
            "node-a",
            "node-b",
            "127.0.0.1:9002",
        )
        .unwrap();
        assert_eq!(coord.phase(), Phase::Adding);
        coord.begin_catch_up().unwrap();
        // 追平：dispatch_outbox 投递到新节点（put 覆盖 = 幂等 apply）
        {
            let mut a = a.lock().unwrap();
            let mut b = b.lock().unwrap();
            let n = a
                .dispatch_outbox(|key, payload| {
                    let docid = u64::from_be_bytes(key[..8].try_into().unwrap());
                    b.put(docid, payload.to_vec(), &[]).unwrap();
                    true
                })
                .unwrap();
            assert_eq!(n, 20, "20 条 outbox 全部投递");
            assert!(a.outbox_drained().unwrap(), "排空校验通过");
        }
        coord.mark_drained().unwrap();
        coord.switch().unwrap();
        assert_eq!(coord.phase(), Phase::Done);
        assert_eq!(coord.master_node().as_deref(), Some("node-b"), "路由切到新节点");
        // 新节点数据完整（与主节点一致）
        let b = b.lock().unwrap();
        for i in 0..20u64 {
            assert_eq!(
                b.get(i).unwrap().as_deref(),
                Some(format!("doc-{i}").as_bytes()),
                "docid {i} 追平一致"
            );
        }
    }

    // ============ Ex-8.7 删除密度 Compaction ============

    #[test]
    fn delete_gc_pending_gates_on_ratio_and_min_docs() {
        // 边界：min_docs（新增置位门槛）与 min_ratio（置位率门槛）独立生效——
        // 小批量删除不触发；达到 min_docs 但密度不足不触发；两者满足才就绪。
        let dir = tmp();
        let mut cfg = cfg();
        cfg.storage.auto_compact = false;
        cfg.storage.delete_density_min_docs = 10;
        cfg.storage.delete_density_min_ratio = 0.10;
        let mut e = Engine::open(&dir, &cfg).unwrap();
        for d in 1..=100u64 {
            e.put(d, format!("v{d}").into_bytes(), &[]).unwrap();
        }
        assert!(!e.delete_garbage_pending(), "无删除不就绪");
        for d in 1..=9u64 {
            e.delete(d).unwrap();
        }
        assert!(
            !e.delete_garbage_pending(),
            "密度 9%<10% 不就绪（虽已满足 min_docs 阈值语义由另一例覆盖）"
        );
        e.delete(10).unwrap(); // 置位率 10%
        assert!(
            e.delete_garbage_pending(),
            "置位率 10% ≥ 阈值 + 新增 ≥ min_docs → 就绪"
        );
        assert!(e.needs_compact(), "就绪 → needs_compact 置位");
        // min_docs 门槛独立验证：置位率足够但新增不足 → 不就绪
        cfg.storage.delete_density_min_docs = 50;
        let mut e2 = Engine::open(&tmp(), &cfg).unwrap();
        for d in 1..=100u64 {
            e2.put(d, format!("v{d}").into_bytes(), &[]).unwrap();
        }
        for d in 1..=20u64 {
            e2.delete(d).unwrap(); // 密度 20% ≥10%，但新增 20 < min_docs 50
        }
        assert!(!e2.delete_garbage_pending(), "新增置位不足 min_docs 不就绪");
    }

    #[test]
    fn delete_density_gc_drains_converged_primary_and_reclaims_space() {
        // Ex-8.7 核心：删除密集负载（位图开启）收敛为单底层段后——常规 select 无多段候选，
        // 删除密度 urgency 触发 GC **单段重写**物理回收已删数据；排空（0 丢弃轮）后收敛。
        let dir = tmp();
        let mut cfg = cfg();
        cfg.storage.auto_compact = false;
        let n = 4_000u64;
        let delete_every = 3u64; // 33% 删除密集
        let mut e = Engine::open(&dir, &cfg).unwrap();
        // 批量灌入（put_nosync 免逐条 fsync）+ 分批 flush → 多 L0
        let chunk = 1_000u64;
        for (c, d) in (1..=n).enumerate() {
            e.put_nosync(d, format!("v{d}").into_bytes(), &[]).unwrap();
            if (c as u64 + 1) % chunk == 0 {
                e.flush_primary().unwrap();
            }
        }
        e.flush_wal().unwrap();
        // 常规压实收敛（无删除 → 位图无关路径）：L0 多段 → 底层单段
        let mut guard = 0;
        while e.primary.sst_count() > 1 && guard < 20 {
            let _ = e.compact().unwrap();
            guard += 1;
        }
        assert_eq!(e.primary.sst_count(), 1, "已收敛为单段");
        let bytes_before = e.primary.sst_bytes();
        assert!(bytes_before > 0);
        // 删除密集（均匀 1/3：3,6,9,…）
        let mut deleted = 0u64;
        for d in (delete_every..=n).step_by(delete_every as usize) {
            e.delete(d).unwrap();
            deleted += 1;
        }
        e.flush_wal().unwrap();
        assert!(
            e.delete_garbage_pending(),
            "置位率≈1/3 ≥ 阈值、新增≥min_docs → 删除密度就绪"
        );
        // GC 排空：逐轮压实直至某轮 0 丢弃收敛
        let mut total_dropped = 0u64;
        let mut rounds = 0;
        while e.delete_garbage_pending() && rounds < 20 {
            let rep = e.compact().unwrap();
            total_dropped += rep.dropped_keys as u64;
            rounds += 1;
        }
        assert!(!e.needs_compact(), "排空收敛后不再需要合并");
        assert_eq!(total_dropped, deleted, "全部已删数据物理丢弃");
        let bytes_after = e.primary.sst_bytes();
        assert!(
            bytes_after < bytes_before,
            "删除密集段重写应回收空间: {} → {}",
            bytes_before,
            bytes_after
        );
        // 语义：存活可见、已删不可见、全表计数一致
        for d in 1..=n {
            let expect = d % delete_every != 0;
            assert_eq!(e.get(d).unwrap().is_none(), !expect, "docid {d}");
        }
        let live = e.count_all_docs().unwrap();
        assert_eq!(live, n - deleted, "可见行数 = 总行 - 已删");
        // 重启：done 基准 = 打开时置位数 → 历史置位不重复触发 GC 重写
        drop(e);
        let mut e2 = Engine::open(&dir, &cfg).unwrap();
        assert!(!e2.delete_garbage_pending(), "历史置位不重复触发");
        assert!(e2.get(3).unwrap().is_none(), "已删 docid3 重启后仍不可见");
        assert_eq!(e2.get(2).unwrap().unwrap(), b"v2");
    }

    #[test]
    fn delete_density_demo_dense_vs_uniform_reclaim_contrast() {
        // Ex-8.7 demo：删除密集 vs 均匀（少量删除）负载——同样载入量下，删除密集
        // 经删除密度 GC 回收 ≈删除比例的空间；均匀负载不触发（置位率/新增均低于门槛），
        // 段不重写、空间保持（对照"删除密集段优先被合并以释放空间"的收益）。
        let dense_dir = tmp();
        let uniform_dir = tmp();
        let mut cfg = cfg();
        cfg.storage.auto_compact = false;
        cfg.storage.delete_density_min_docs = 100; // 演示用小阈值
        cfg.storage.delete_density_min_ratio = 0.05;
        let n = 2_000u64;
        let load = |dir: &std::path::Path, e: &mut Engine| {
            for (c, d) in (1..=n).enumerate() {
                e.put_nosync(d, format!("doc-{d:05}").into_bytes(), &[]).unwrap();
                if (c as u64 + 1) % 500 == 0 {
                    e.flush_primary().unwrap();
                }
            }
            e.flush_wal().unwrap();
            let mut g = 0;
            while e.primary.sst_count() > 1 && g < 20 {
                let _ = e.compact().unwrap();
                g += 1;
            }
        };
        let mut dense = Engine::open(&dense_dir, &cfg).unwrap();
        load(&dense_dir, &mut dense);
        let mut uniform = Engine::open(&uniform_dir, &cfg).unwrap();
        load(&uniform_dir, &mut uniform);
        // 删除密集：删除 50%（step 2）→ GC 回收；均匀：仅删 2% 且低于增量门槛 → 不触发
        let mut del = 0u64;
        for d in (1..=n).step_by(2) {
            dense.delete(d).unwrap();
            del += 1;
        }
        for d in (1..=n).step_by(50) {
            uniform.delete(d).unwrap();
        }
        dense.flush_wal().unwrap();
        uniform.flush_wal().unwrap();
        let mut rounds = 0;
        while dense.delete_garbage_pending() && rounds < 20 {
            let _ = dense.compact().unwrap();
            rounds += 1;
        }
        assert!(!uniform.delete_garbage_pending(), "均匀少量删除不触发 GC");
        let dense_bytes = dense.primary.sst_bytes();
        let uniform_bytes = uniform.primary.sst_bytes();
        println!(
            "[Ex-8.7 demo] 载入 {n} 行：删除密集(-50%) GC 后 {} bytes vs 均匀(-2%) {} bytes；排空 {} 轮",
            dense_bytes, uniform_bytes, rounds
        );
        assert_eq!(dense.primary.sst_count(), 1, "删除密集收敛单段");
        assert_eq!(uniform.primary.sst_count(), 1, "均匀收敛单段");
        assert!(
            dense_bytes < uniform_bytes,
            "删除密集应显著回收空间: dense={} uniform={}",
            dense_bytes,
            uniform_bytes
        );
        assert_eq!(dense.count_all_docs().unwrap(), n - del, "可见行一致");
    }

    #[test]
    fn deletion_bitmap_persists_across_restart() {
        // 删除 → flush_wal（位图落盘）→ 重启 → 位图文件加载，已删 docid 仍不可见
        let dir = tmp();
        let cfg = cfg();
        let bitmap_path = dir.join("deletion.bitmap");
        {
            let mut e = Engine::open(&dir, &cfg).unwrap();
            e.put(1, b"v1".to_vec(), &["k"]).unwrap();
            e.put(2, b"v2".to_vec(), &["k"]).unwrap();
            e.flush_wal().unwrap();
            e.delete(1).unwrap();
            e.flush_wal().unwrap(); // 位图脏页落盘
        }
        let meta = std::fs::metadata(&bitmap_path).unwrap();
        assert!(meta.len() > 0, "位图文件非空（已序列化落盘）");
        let mut e2 = Engine::open(&dir, &cfg).unwrap();
        assert!(e2.get(1).unwrap().is_none(), "重启后位图加载，删除持久");
        assert!(e2.get(2).unwrap().is_some(), "未删文档不受影响");
    }

    #[test]
    fn deletion_bitmap_put_resurrects() {
        // delete → put 复活：put 清位，文档重新可见
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"v1".to_vec(), &["k"]).unwrap();
        e.delete(1).unwrap();
        assert!(e.get(1).unwrap().is_none());
        e.put(1, b"v2".to_vec(), &["k"]).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"v2", "put 复活后可见新值");
    }

    #[test]
    fn deletion_bitmap_compaction_drops_deleted_data() {
        // 位图开启：delete 不写 LSM 墓碑 → compaction 按位图物理丢弃已删 docid 旧数据；
        // 重启后位图 + 压实结果均一致（已删不可见、存活可见）
        let dir = tmp();
        let cfg = cfg();
        {
            let mut e = Engine::open(&dir, &cfg).unwrap();
            for d in 1..=4u64 {
                e.put(d, format!("doc{d}").into_bytes(), &["k"]).unwrap();
            }
            e.flush_primary().unwrap(); // L0 第 1 段
            e.put(5, b"doc5".to_vec(), &["k"]).unwrap();
            e.flush_primary().unwrap(); // L0 第 2 段 → 触发 compaction 条件
            e.delete(2).unwrap();
            e.delete(4).unwrap();
            let rep = e.compact().unwrap();
            assert!(rep.merged_ssts >= 2, "L0 压实应合并多段");
            assert!(e.get(2).unwrap().is_none());
            assert!(e.get(4).unwrap().is_none());
            assert!(e.get(1).unwrap().is_some());
            assert!(e.get(5).unwrap().is_some());
        }
        let mut e2 = Engine::open(&dir, &cfg).unwrap();
        for d in 1..=5u64 {
            let deleted = matches!(d, 2 | 4);
            assert_eq!(e2.get(d).unwrap().is_none(), deleted, "重启后 docid {d} 状态一致");
        }
    }

    #[test]
    fn deletion_bitmap_disabled_uses_tombstone_path() {
        // 位图关闭：delete 走传统 Tombstone（primary.delete + 逐条 fsync），get 仍返回 None
        let mut c = cfg();
        c.storage.deletion_bitmap_enabled = false;
        let dir = tmp();
        {
            let mut e = Engine::open(&dir, &c).unwrap();
            e.put(1, b"v1".to_vec(), &["k"]).unwrap();
            e.delete(1).unwrap();
            assert!(e.get(1).unwrap().is_none());
        }
        assert!(
            !dir.join("deletion.bitmap").exists(),
            "位图关闭时不应生成位图文件"
        );
        let mut e2 = Engine::open(&dir, &c).unwrap();
        assert!(e2.get(1).unwrap().is_none(), "Tombstone 路径跨重启删除一致");
    }

    #[test]
    fn deletion_bitmap_incremental_backup_captures_delete() {
        // 位图开启：delete 写 primary WAL 删除记录 → 增量备份导出含删除 → 恢复后删除保持
        let dir = tmp();
        let cfg = cfg();
        let mut e = Engine::open(&dir, &cfg).unwrap();
        let put_doc = |e: &mut Engine, docid: u64, v: &str| {
            e.put(docid, v.as_bytes().to_vec(), &["k"]).unwrap();
        };
        put_doc(&mut e, 1, "v1");
        put_doc(&mut e, 2, "v2");
        let since = e.current_seq();
        e.delete(1).unwrap();
        let bak_dir = tempfile::tempdir().unwrap();
        let bak = bak_dir.path().join("incr.json");
        let rep = e.backup_incremental(since, &bak).unwrap();
        assert!(rep.records >= 1, "增量备份应包含删除记录");
        drop(e);

        // 恢复到全新引擎（位图开启）：删除记录回放 → 重新置位 → doc 1 不可见
        let dir2 = tmp();
        let mut e2 = Engine::open(&dir2, &cfg).unwrap();
        put_doc(&mut e2, 1, "v1");
        put_doc(&mut e2, 2, "v2");
        let n = e2.restore_incremental(&bak).unwrap();
        assert!(n >= 1);
        assert!(e2.get(1).unwrap().is_none(), "增量恢复后删除保持");
        assert!(e2.get(2).unwrap().is_some());
    }

    #[test]
    fn oom_guardian_throttles_in_soft_range() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.set_mem_ratio(0.9); // 软水位区间
        e.put(1, b"throttled-but-allowed".to_vec(), &[]).unwrap();
        assert!(e.get(1).unwrap().is_some());
        // 限流计数已记录
        assert!(e.watchdog.memory().throttled_count() >= 1);
    }

    #[test]
    fn restart_preserves_data_and_inverted() {
        // 崩溃恢复全链路：写入（未强制刷盘）→ 进程退出 → 重开 → 主数据与倒排均完好
        let dir = tmp();
        let cfg = cfg();
        {
            let mut e = Engine::open(&dir, &cfg).unwrap();
            e.put(1, b"doc-1".to_vec(), &["rust"]).unwrap();
            e.put(2, b"doc-2".to_vec(), &["go"]).unwrap();
            e.put(3, b"doc-3".to_vec(), &["rust", "async"]).unwrap();
            e.flush_inverted().unwrap();
        } // 模拟进程退出
        {
            let mut e2 = Engine::open(&dir, &cfg).unwrap();
            // 主数据（WAL 回放恢复）
            assert_eq!(e2.get(1).unwrap().unwrap(), b"doc-1");
            assert_eq!(e2.get(2).unwrap().unwrap(), b"doc-2");
            assert_eq!(e2.get(3).unwrap().unwrap(), b"doc-3");
            // 倒排跨重启可查（段文件 + Manifest）
            let rows = e2.search_term("rust").unwrap();
            let mut ids: Vec<u64> = rows.iter().map(|(d, _)| *d).collect();
            ids.sort();
            assert_eq!(ids, vec![1, 3]);
            assert_eq!(e2.search_term("go").unwrap().len(), 1);
        }
    }

    // ---------- P2 Bloom 分层计量 ----------

    #[test]
    fn bloom_layer_counters_move_on_flushed_miss_queries() {
        // 只写偶数 docid 并分批 flush → SST（v5 分区布隆）；对段内不存在的奇数 key 点查/
        // 批量点查 → 布隆 skip 增长；对段外 key 少量查询 → minmax skip 增长。
        let dir = tmp();
        let mut e = Engine::open(&dir, &cfg()).unwrap();
        let n = 4_000u64; // 最大偶数
        let chunk = 1_000u64;
        let mut flushed = 0u64;
        for (c, d) in (1..=n).enumerate() {
            if d % 2 != 0 {
                continue;
            }
            e.put_nosync(d, format!("v{d}").into_bytes(), &[]).unwrap();
            flushed += 1;
            if flushed % chunk == 0 {
                e.flush_primary().unwrap();
            }
        }
        e.flush_wal().unwrap();
        assert!(e.primary.sst_count() > 0, "应已有 SST（flush 成功）");
        let before = e.primary.bloom_counts();
        // 段内不存在的奇数 key（介于既有偶数之间 → 定位块 + 分区布隆）
        let mut odd_hits = 0u64;
        for d in (1..=n).step_by(2) {
            if e.get(d).unwrap().is_none() {
                odd_hits += 1;
            }
        }
        assert!(odd_hits > 0);
        // 批量点查 miss（batch_get 走 get_many_from_sst 分区布隆路径）
        let miss: Vec<u64> = (1..=n).step_by(2).collect();
        let out = e.batch_get(&miss).unwrap();
        assert!(out.iter().all(Option::is_none));
        // 段外 key（> 最大）→ 段级 min/max 粗筛跳过
        for d in (n + 1)..=(n + 200) {
            assert!(e.get(d).unwrap().is_none());
        }
        let after = e.primary.bloom_counts();
        assert!(
            after.3 > before.3,
            "分区布隆 skip 应增长（奇数 key 段内 miss）: {before:?} → {after:?}"
        );
        assert!(after.0 > before.0, "minmax skip 应增长（段外 key）");
        assert!(after.2 > before.2, "分区布隆 probe 应增长");
    }
