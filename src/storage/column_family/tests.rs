//! 单元测试（自 column_family.rs 底部 `mod tests` 整体外移；行为不变）。

    use super::*;
    use super::open::{epoch_days, parse_sst_date, today_epoch_days};
    use crate::storage::sstable::compaction::{
        cap_by_size, select_compaction_inputs, select_compaction_inputs_ex,
    };
    use std::sync::OnceLock;

    use crate::config::model::Config;
    use crate::error::Error;
    use crate::keys::encode_docid;
    use crate::storage::manifest::MANIFEST_FILE;
    use crate::wal::WalReader;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let name = format!("cf-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    fn small_cfg(max_mb: usize) -> Config {
        let mut cfg = Config::default();
        cfg.memtable.max_size_mb = max_mb;
        cfg.blockcache.block_size_kb = 1;
        cfg.sstable.compression = "none".into();
        cfg
    }

    // ---------- 流式 scan（M8-P10） ----------

    #[test]
    fn scan_stream_matches_scan_raw_range() {
        let dir = tmp();
        let cfg = small_cfg(16); // 小阈值 → 多次 flush → 多 SST 源
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 批量写入并周期性 flush（产生多个 SST：k-way merge 多源）
        for i in 0..2_000u64 {
            cf.put_bytes_nosync(
                format!("k{i:08}").into_bytes(),
                format!("v{i}").into_bytes(),
            )
            .unwrap();
            if i % 500 == 499 {
                cf.switch_and_flush().unwrap();
            }
        }
        // memtable 新版本：覆盖 + 删除
        cf.put_bytes_nosync(b"k00000042".to_vec(), b"updated".to_vec())
            .unwrap();
        cf.delete_bytes(b"k00000100".to_vec()).unwrap();
        cf.sync_wal().unwrap();

        // 全量（旧路径）vs 流式（新路径）：结果完全一致
        let all = cf.scan_raw_range(None, None).unwrap();
        let mut streamed: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        cf.scan_stream(None, None, |k, v| {
            streamed.push((k.to_vec(), v.to_vec()));
            Ok(true)
        })
        .unwrap();
        assert_eq!(streamed.len(), all.len(), "流式行数应与全量一致");
        for (a, b) in streamed.iter().zip(all.iter()) {
            assert_eq!(a.0, b.0, "key 顺序一致");
            assert_eq!(a.1, b.1, "value 一致（含覆盖后的新值）");
        }
        // 语义校验：覆盖生效、删除隐藏
        let hit = all.iter().find(|(k, _)| k == b"k00000042").unwrap();
        assert_eq!(hit.1, b"updated");
        assert!(
            !all.iter().any(|(k, _)| k == b"k00000100"),
            "被删除的 key 不应出现"
        );
        // 范围过滤：流式与全量一致
        let ra = cf
            .scan_raw_range(Some(&b"k00001000"[..]), Some(&b"k00001010"[..]))
            .unwrap();
        let mut rs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        cf.scan_stream(
            Some(&b"k00001000"[..]),
            Some(&b"k00001010"[..]),
            |k, v| {
                rs.push((k.to_vec(), v.to_vec()));
                Ok(true)
            },
        )
        .unwrap();
        assert_eq!(rs, ra, "范围过滤流式应与全量一致");
    }

    // ---------- WAL 截断（M8-P5） ----------

    #[test]
    fn wal_truncated_after_flush_keeps_data() {
        let dir = tmp();
        let cfg = small_cfg(64);
        let wal_path = dir.join(WAL_FILE);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..5_000u64 {
                cf.put_bytes_nosync(i.to_be_bytes().to_vec(), format!("doc-{i}").into_bytes())
                    .unwrap();
            }
            cf.sync_wal().unwrap();
            let before = std::fs::metadata(&wal_path).unwrap().len();
            cf.switch_and_flush().unwrap(); // flush → WAL 截断
            let after = std::fs::metadata(&wal_path).unwrap().len();
            assert!(
                after < before && after < 64,
                "flush 后 WAL 应截断为小文件（before={before} after={after}）"
            );
        }
        // 重开：数据完整（从 SST + WAL 恢复），seq 接续
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.get(0).unwrap().unwrap().0, b"doc-0");
        assert_eq!(cf2.get(4_999).unwrap().unwrap().0, b"doc-4999");
        // 新写入 seq 接续（不回到 1）：头持久化 next_seq
        let next = cf2.wal_next_seq();
        assert!(
            next >= 5_001,
            "重开后 next_seq 应接续（>=5001），实际 {next}"
        );
    }

    #[test]
    fn wal_header_persists_next_seq_across_restart() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let wal_path = dir.join(WAL_FILE);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in 0..300u64 {
            cf.put_bytes_nosync(i.to_be_bytes().to_vec(), format!("v{i}").into_bytes())
                .unwrap();
        }
        cf.sync_wal().unwrap();
        cf.switch_and_flush().unwrap(); // 截断，头写入 next_seq=301
        let first_after_flush = cf.wal_next_seq();
        drop(cf);
        // 重开：next_seq 从头恢复（>300），而非 1
        let cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(
            cf2.wal_next_seq(),
            first_after_flush,
            "重开 next_seq 应接续（头持久化）"
        );
        assert!(
            std::fs::metadata(&wal_path).unwrap().len() < 64,
            "WAL 保持小文件"
        );
    }

    #[test]
    fn old_wal_without_header_still_recovers() {
        // 兼容：旧格式 WAL（无头，M8-P5 之前）照常回放恢复
        let dir = tmp();
        let cfg = small_cfg(64);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..100u64 {
                cf.put_bytes_nosync(i.to_be_bytes().to_vec(), format!("v{i}").into_bytes())
                    .unwrap();
            }
            cf.sync_wal().unwrap();
            // 不 flush：WAL 保留记录（旧格式无头）
        }
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(
            cf2.get(99).unwrap().unwrap().0,
            b"v99",
            "旧格式 WAL 应回放恢复"
        );
        assert!(cf2.wal_next_seq() >= 101, "旧 WAL 回放后 seq 接续");
    }

    #[test]
    fn put_get_roundtrip() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(1, b"doc-1".to_vec()).unwrap();
        cf.put(2, b"doc-2".to_vec()).unwrap();
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"doc-1");
        assert_eq!(cf.get(2).unwrap().unwrap().0, b"doc-2");
        assert!(cf.get(99).unwrap().is_none());
    }

    #[test]
    fn overwrite_returns_latest() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(7, b"v1".to_vec()).unwrap();
        cf.put(7, b"v2".to_vec()).unwrap();
        assert_eq!(cf.get(7).unwrap().unwrap().0, b"v2");
    }

    #[test]
    fn delete_hides_key() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(3, b"x".to_vec()).unwrap();
        cf.delete(3).unwrap();
        assert!(cf.get(3).unwrap().is_none());
    }

    #[test]
    fn delete_survives_flush_and_restart() {
        // 步骤 9：Tombstone 必须落盘——删除后刷盘 + 重启，key 依然不存在
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            cf.put(1, b"v1".to_vec()).unwrap();
            cf.put(2, b"v2".to_vec()).unwrap();
            cf.put(3, b"v3".to_vec()).unwrap();
            cf.switch_and_flush().unwrap(); // 全部落盘
            cf.delete(2).unwrap();
            cf.delete(3).unwrap();
            cf.switch_and_flush().unwrap(); // Tombstone 落盘
        }
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.get(1).unwrap().unwrap().0, b"v1");
        assert!(cf2.get(2).unwrap().is_none(), "删除后重启应不存在");
        assert!(cf2.get(3).unwrap().is_none(), "删除后重启应不存在");
        // 范围扫描同样过滤
        let rows = cf2.scan_range(None, None).unwrap();
        assert_eq!(rows, vec![(1, b"v1".to_vec())]);
    }

    #[test]
    fn delete_overrides_older_sst_value() {
        // 旧 SST 有值、新 SST 有 Tombstone：读路径按新→旧应命中 Tombstone → 不存在
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(9, b"old-value".to_vec()).unwrap();
        cf.switch_and_flush().unwrap();
        // 模拟"删除发生在更晚的时刻"：直接写 tombstone 到新 memtable 并刷盘
        cf.delete(9).unwrap();
        cf.switch_and_flush().unwrap();
        assert!(cf.get(9).unwrap().is_none());
        assert!(cf.scan_range(None, None).unwrap().is_empty());
    }

    #[test]
    fn compact_merges_ssts_preserving_overwrite_and_delete() {
        // design 4.5 阶段 3：多次刷盘 → 全量合并，覆盖/删除语义保留
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 3 个 SST：含跨段覆盖 + 删除
        cf.put(1, b"v1".to_vec()).unwrap();
        cf.put(2, b"v2".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST1
        cf.put(2, b"v2b".to_vec()).unwrap(); // 跨段覆盖
        cf.put(3, b"v3".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST2
        cf.delete(3).unwrap(); // 删除
        cf.put(4, b"v4".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST3

        let before = cf.sst_count();
        assert!(before >= 3, "应产生多个 SST: {before}");
        let rep = cf.compact().unwrap();
        assert_eq!(rep.merged_ssts, before);
        assert!(rep.freed_bytes > 0, "合并应释放空间");
        assert_eq!(cf.sst_count(), 1, "合并后只剩 1 个 SST");

        // 语义保持
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"v1");
        assert_eq!(cf.get(2).unwrap().unwrap().0, b"v2b", "后写覆盖先写");
        assert!(cf.get(3).unwrap().is_none(), "删除后不可见");
        assert_eq!(cf.get(4).unwrap().unwrap().0, b"v4");
        let rows = cf.scan_range(None, None).unwrap();
        assert_eq!(
            rows,
            vec![
                (1, b"v1".to_vec()),
                (2, b"v2b".to_vec()),
                (4, b"v4".to_vec())
            ]
        );

        // 重启后 Manifest 只含新段，数据完整
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.sst_count(), 1);
        assert_eq!(cf2.get(2).unwrap().unwrap().0, b"v2b");
        assert!(cf2.get(3).unwrap().is_none());
    }

    #[test]
    fn compact_filtered_physically_drops_deleted_keys() {
        // Ex-5.6：删除位图过滤——合并时按 drop_key 物理丢弃（不保留数据、不写 Tombstone），
        // 位图已删 docid 的旧数据随压实直接回收（墓碑不污染层级）。
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(1, b"v1".to_vec()).unwrap();
        cf.put(2, b"v2".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST1
        cf.put(3, b"v3".to_vec()).unwrap();
        cf.put(4, b"v4".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST2

        // 模拟删除位图：docid 2、4 已删（主键为 8 字节大端）
        let deleted = |k: &[u8]| {
            k.len() == 8
                && matches!(u64::from_be_bytes(k.try_into().unwrap()), 2 | 4)
        };
        let rep = cf.compact_filtered(&deleted).unwrap();
        assert!(rep.merged_ssts >= 2, "应合并多段: {}", rep.merged_ssts);
        assert_eq!(cf.sst_count(), 1);

        // 已删 key 物理消失：读不到、扫描无记录（数据不在磁盘，非内存过滤）
        assert!(cf.get(2).unwrap().is_none());
        assert!(cf.get(4).unwrap().is_none());
        let rows = cf.scan_range(None, None).unwrap();
        assert_eq!(rows, vec![(1, b"v1".to_vec()), (3, b"v3".to_vec())]);

        // 重启后同样物理消失（新段 Manifest 不含已删键）
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        let rows = cf2.scan_range(None, None).unwrap();
        assert_eq!(rows, vec![(1, b"v1".to_vec()), (3, b"v3".to_vec())]);
        assert!(cf2.get(2).unwrap().is_none());
    }

    #[test]
    fn compact_filtered_keeps_other_tombstones() {
        // Ex-5.6：过滤只丢弃位图已删 key；其他 Tombstone（非位图路径写入）语义保留
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(1, b"v1".to_vec()).unwrap();
        cf.put(2, b"v2".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST1
        cf.delete(2).unwrap(); // 传统 Tombstone（位图未删 docid 2 的场景不在此测——仅验证 Tombstone 保留）
        cf.put(3, b"v3".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST2

        let deleted = |_k: &[u8]| false; // 空过滤（无位图删除）
        cf.compact_filtered(&deleted).unwrap();
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"v1");
        assert!(cf.get(2).unwrap().is_none(), "Tombstone 语义保留");
        assert_eq!(cf.get(3).unwrap().unwrap().0, b"v3");
    }

    #[test]
    fn compact_gc_rewrites_single_segment_dropping_deleted_keys() {
        // Ex-8.7：收敛为单段（常规 select 无多段候选）——`compact_gc(allow_single=true)`
        // 单段重写按位图已删键物理丢弃回收空间；`allow_single=false`（传统路径）不重写。
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for d in 1..=40u64 {
            cf.put(d, format!("v{d}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        for d in 41..=80u64 {
            cf.put(d, format!("v{d}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        let rep = cf.compact().unwrap();
        assert_eq!(rep.merged_ssts, 2, "常规合并收敛");
        assert_eq!(cf.sst_count(), 1, "已收敛为单段");

        let deleted = |k: &[u8]| {
            k.len() == 8 && {
                let id = u64::from_be_bytes(k.try_into().unwrap());
                id % 3 == 0
            }
        };
        // 传统路径（allow_single=false）：单段不重写（空转）
        let rep0 = cf.compact_gc(&deleted, false).unwrap();
        assert_eq!(rep0.merged_ssts, 0, "allow_single=false 不重写单段");
        // 删除密度 GC：单段重写 → 1/3 键物理丢弃
        let rep = cf.compact_gc(&deleted, true).unwrap();
        assert_eq!(rep.merged_ssts, 1, "单段重写 1 段");
        assert!(rep.dropped_keys > 0, "应物理丢弃已删键: {}", rep.dropped_keys);
        assert!(rep.freed_bytes > 0, "重写应释放空间");
        let rows = cf.scan_range(None, None).unwrap();
        assert!(rows.iter().all(|(d, _)| d % 3 != 0), "已删键全部消失");
        assert_eq!(rows.len(), 54, "80 - ⌊80/3⌋ = 54 存活");
        // 二次 GC：无垃圾可丢 → 0 丢弃（引擎排空收敛判定依据）
        let rep2 = cf.compact_gc(&deleted, true).unwrap();
        assert_eq!(rep2.dropped_keys, 0, "无已删键 → 0 丢弃收敛");
        // 重启后物理态一致
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.scan_range(None, None).unwrap().len(), 54);
    }

    #[test]
    fn compact_reuses_blocks_when_no_overlap() {
        // Ex-5.8 元数据-数据解耦：无重叠 L0 段合并走数据块级复用（只重建元数据区，
        // 数据块零解压）——合并后数据完整、跨重启一致。
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 段 A：键 0..500；段 B：键 500..1000（key 范围无重叠）
        for i in 0..500u64 {
            cf.put(i, format!("v{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        for i in 500..1000u64 {
            cf.put(i, format!("v{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.sst_count(), 2, "两个无重叠 L0 段");

        let rep = cf.compact().unwrap();
        assert_eq!(rep.merged_ssts, 2, "应合并 2 段（块级复用）");
        assert_eq!(cf.sst_count(), 1, "合并后单段");
        // 数据完整（块级复用后所有键可读）
        for i in [0u64, 1, 499, 500, 501, 999] {
            assert_eq!(
                cf.get(i).unwrap().unwrap().0,
                format!("v{i}").into_bytes(),
                "键 {i} 复用后仍可读"
            );
        }
        assert!(cf.get(1000).unwrap().is_none());

        // 跨重启：Manifest 只含新段，数据完整
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.sst_count(), 1);
        assert_eq!(cf2.get(0).unwrap().unwrap().0, b"v0");
        assert_eq!(cf2.get(999).unwrap().unwrap().0, b"v999");
    }

    #[test]
    fn compact_full_merge_when_overlap() {
        // Ex-5.8 回退验证：有重叠 L0 段合并必须走全量路径（覆盖/去重语义保留），
        // 块级复用检测应返回 None 且结果与旧全量合并一致。
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 段 A：键 0..300；段 B：键 200..500（[200,300) 重叠）
        for i in 0..300u64 {
            cf.put(i, format!("va-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        for i in 200..500u64 {
            cf.put(i, format!("vb-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.sst_count(), 2);

        let rep = cf.compact().unwrap();
        assert_eq!(rep.merged_ssts, 2);
        assert_eq!(cf.sst_count(), 1);
        // 重叠区后写覆盖先写（B 段更新）
        assert_eq!(cf.get(250).unwrap().unwrap().0, b"vb-250");
        assert_eq!(cf.get(100).unwrap().unwrap().0, b"va-100");
        assert_eq!(cf.get(499).unwrap().unwrap().0, b"vb-499");
        // 全量合并（kept_keys > 0 表示有去重消除）
        assert_eq!(cf.get(300).unwrap().unwrap().0, b"vb-300");
    }

    #[test]
    fn compact_noop_when_single_sst() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(1, b"x".to_vec()).unwrap();
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.sst_count(), 1);
        let rep = cf.compact().unwrap();
        assert_eq!(rep.merged_ssts, 0, "单段不需要合并");
        assert_eq!(cf.sst_count(), 1);
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"x");
    }

    // ---------- Leveled-Compaction（design 4.5 二期，M6-2） ----------

    #[test]
    fn leveled_compact_promotes_l0_to_l1_and_persists() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 两批写入并强制刷盘 → 2 个 L0 段
        for i in 0..100u64 {
            cf.put(i, format!("a{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        for i in 100..200u64 {
            cf.put(i, format!("a{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.ssts.load().levels, vec![0, 0], "刷盘产物均为 L0");
        assert!(!cf.needs_compact(), "2 个 L0 未超阈值");
        // 手动压实 → L1
        let rep = cf.compact().unwrap();
        assert_eq!(rep.out_level, 1);
        assert_eq!(cf.sst_count(), 1);
        assert_eq!(cf.ssts.load().levels, vec![1]);
        // 数据完整
        for i in (0..200u64).step_by(7) {
            assert!(cf.get(i).unwrap().is_some());
        }
        // 重启：Manifest 持久化层号
        drop(cf);
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.ssts.load().levels, vec![1], "Manifest 应持久化层号");
        assert!(cf2.get(150).unwrap().is_some());
    }

    #[test]
    fn leveled_compact_sinks_l1_to_l2() {
        let dir = tmp();
        let mut cfg = small_cfg(256);
        cfg.storage.l1_trigger_files = 0; // 本测针对 L1→L2 下沉语义（不受 Ex-8.11 默认 8 延迟影响）
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 每轮刷 2 个 L0 段并压实 → L0 合并下沉为 1 个 L1 段；4 轮后 L1 累计 4 个文件
        for round in 0..4u64 {
            for _ in 0..2u64 {
                for i in 0..50u64 {
                    cf.put(round * 100 + i, format!("v{round}-{i}").into_bytes())
                        .unwrap();
                }
                cf.switch_and_flush().unwrap();
            }
            let r = cf.compact().unwrap();
            assert_eq!(r.out_level, 1, "第 {round} 轮 L0→L1");
        }
        assert_eq!(cf.ssts.load().levels.iter().filter(|l| **l == 1).count(), 4);
        // L0 空、L1 > 1 → L1 → L2（压实下沉）。L 项合并冷却：新生成段 N 轮内不参与下一轮
        // 合并 → 收敛需多轮（冷却段到期后正常参与），循环 compact 直到收敛
        assert!(cf.needs_compact(), "L1 多段应触发 L1→L2");
        let mut guard = 0;
        while cf.needs_compact() && guard < 8 {
            cf.compact().unwrap();
            guard += 1;
        }
        assert!(guard < 8, "冷却后多轮应收敛");
        assert_eq!(cf.sst_count(), 1);
        assert_eq!(cf.ssts.load().levels, vec![2]);
        // 全量数据仍完整
        for round in 0..4u64 {
            for i in (0..50u64).step_by(9) {
                assert!(
                    cf.get(round * 100 + i).unwrap().is_some(),
                    "round {round} key {i} 丢失"
                );
            }
        }
        // 重启：L2 持久化 + 数据完整
        drop(cf);
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.ssts.load().levels, vec![2]);
        assert!(cf2.get(300 + 10).unwrap().is_some());
    }

    #[test]
    fn tiered_compression_level_for_and_l2_cold_preserves_data() {
        // Ex-8.12：分层压缩——L2+ 用冷档 zstd level，L0/L1 用热档；跨档合并禁块级复用
        // （L1 热档段下沉 L2 必须全量重压缩）；收敛后 L2 单段 + 数据完整 + 重启可读
        let dir = tmp();
        let mut cfg = Config::default(); // compression=zstd level3
        cfg.storage.l1_trigger_files = 0; // 本测针对 L1→L2 分层收敛语义（默认 8 延迟不影响）
        cfg.sstable.compression_level_l2 = 19;
        cfg.blockcache.block_size_kb = 1;
        cfg.memtable.max_size_mb = 256;
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 档位选择逻辑
        assert_eq!(cf.compression_level_for(0), 3, "L0 输出用热档");
        assert_eq!(cf.compression_level_for(1), 3, "L1 输出用热档");
        assert_eq!(cf.compression_level_for(2), 19, "L2 输出用冷档");
        // 未启用分层 → 恒热档
        let mut cfg_flat = Config::default();
        cfg_flat.sstable.compression_level_l2 = 0;
        let dir2 = tmp();
        let cf2 = ColumnFamily::open("primary", &dir2, &cfg_flat).unwrap();
        assert_eq!(cf2.compression_level_for(2), 3, "不分层：L2 也用热档");
        // 端到端：4 轮 L0→L1 → L1×4 下沉 L2（输入 L1 热档、输出 L2 冷档 → meta_only 门控
        // 回退全量重压缩，同时覆盖 compact_merge 冷档写路径）
        for round in 0..4u64 {
            for _ in 0..2u64 {
                for i in 0..50u64 {
                    cf.put(round * 100 + i, format!("v{round}-{i}").into_bytes())
                        .unwrap();
                }
                cf.switch_and_flush().unwrap();
            }
            let r = cf.compact().unwrap();
            assert_eq!(r.out_level, 1, "第 {round} 轮 L0→L1");
        }
        assert_eq!(cf.ssts.load().levels.iter().filter(|l| **l == 1).count(), 4);
        assert!(cf.needs_compact(), "L1 多段应触发 L1→L2");
        let mut guard = 0;
        while cf.needs_compact() && guard < 8 {
            cf.compact().unwrap();
            guard += 1;
        }
        assert!(guard < 8, "冷却后多轮应收敛");
        assert_eq!(cf.sst_count(), 1);
        assert_eq!(cf.ssts.load().levels, vec![2], "收敛为单个 L2 冷档段");
        for round in 0..4u64 {
            for i in (0..50u64).step_by(9) {
                assert!(cf.get(round * 100 + i).unwrap().is_some(), "key 丢失");
            }
        }
        drop(cf);
        let mut cf3 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf3.ssts.load().levels, vec![2]);
        assert!(cf3.get(300 + 10).unwrap().is_some(), "重启后 L2 冷档段可读");
    }

    #[test]
    fn select_compaction_inputs_picks_levels() {
        let no_heat = &[];
        let no_cooling = &std::collections::HashSet::new();
        let no_sizes = &[0u64; 8];
        // 2 个 L0 + L1 未满 → 仅 L0
        assert_eq!(
            select_compaction_inputs_ex(&[0, 0, 1], 8, 0, 0, no_heat, no_cooling, no_sizes, 0),
            (vec![0, 1], 1)
        );
        // 单个 L0 → 暂不压实
        assert_eq!(
            select_compaction_inputs_ex(&[0, 1], 8, 0, 0, no_heat, no_cooling, no_sizes, 0),
            (Vec::new(), 0)
        );
        // L0 ≥ 2 且 L1 已满 → L0 + 全部 L1 收敛
        assert_eq!(
            select_compaction_inputs_ex(&[0, 0, 1, 1, 1, 1], 2, 0, 0, no_heat, no_cooling, no_sizes, 0),
            (vec![0, 1, 2, 3, 4, 5], 1)
        );
        // L0 空、L1 > 1 → L1 → L2
        assert_eq!(
            select_compaction_inputs_ex(&[1, 1], 8, 0, 0, no_heat, no_cooling, no_sizes, 0),
            (vec![0, 1], 2)
        );
        // L0 空、L1 单段 → 无压实
        assert_eq!(
            select_compaction_inputs_ex(&[1], 8, 0, 0, no_heat, no_cooling, no_sizes, 0),
            (Vec::new(), 0)
        );
        // L0/L1 空、L2 > 1 → 收敛 L2
        assert_eq!(
            select_compaction_inputs_ex(&[2, 2], 8, 0, 0, no_heat, no_cooling, no_sizes, 0),
            (vec![0, 1], 2)
        );
        // Ex-8.11：延迟大合并——L0 空、L1=3 < l1_trigger=4 → 暂不下沉
        assert_eq!(
            select_compaction_inputs_ex(&[1, 1, 1], 4, 4, 0, no_heat, no_cooling, no_sizes, 0),
            (Vec::new(), 0)
        );
        // Ex-8.11：L1 攒够 4 → 一次下沉 L2
        assert_eq!(
            select_compaction_inputs_ex(&[1, 1, 1, 1], 4, 4, 0, no_heat, no_cooling, no_sizes, 0),
            (vec![0, 1, 2, 3], 2)
        );
        // Ex-8.11：L2 攒够 l2_trigger=2 → 收敛
        assert_eq!(
            select_compaction_inputs_ex(&[2, 2], 8, 0, 2, no_heat, no_cooling, no_sizes, 0),
            (vec![0, 1], 2)
        );
    }

    #[test]
    fn select_compaction_excludes_cooling_segments() {
        // L 项：冷却段优先不参与合并；候选不足时回退（冷却为软约束，保证收敛）
        let no_heat = &[];
        let no_sizes = &[0u64; 8];
        let mut cooling = std::collections::HashSet::new();
        cooling.insert(0);
        // L0 段 0 冷却 + 段 1 → 冷却后不足 2 → 回退含冷却段（全量合并，防收敛死循环）
        assert_eq!(
            select_compaction_inputs(&[0, 0], 8, no_heat, &cooling, no_sizes, 0),
            (vec![0, 1], 1)
        );
        // L0 段 1 冷却 → 段 0 + 段 2 足够 → 冷却生效，排除段 1
        let mut cooling2 = std::collections::HashSet::new();
        cooling2.insert(1);
        assert_eq!(
            select_compaction_inputs(&[0, 0, 0], 8, no_heat, &cooling2, no_sizes, 0),
            (vec![0, 2], 1)
        );
    }

    #[test]
    fn select_compaction_hot_first_when_l0_over_limit() {
        // Ex-5.9：L0 段数超过 limit 且存在热度 → 优先合并最热的 limit 段（热段先下沉 L1）
        let levels = [0u32, 0, 0, 0, 0]; // 5 个 L0，limit=3
        let heat = [0u64, 0, 100, 50, 10]; // 段 2 最热
        let no_cooling = &std::collections::HashSet::new();
        let (sel, out) = select_compaction_inputs(&levels, 3, &heat, no_cooling, &[0u64; 8], 0);
        assert_eq!(out, 1);
        assert_eq!(sel, vec![2, 3, 4], "应选最热 3 段（100/50/10）");
        // 无热度数据 → 维持全量合并
        let (sel2, _) = select_compaction_inputs(&levels, 3, &[], no_cooling, &[0u64; 8], 0);
        assert_eq!(sel2, vec![0, 1, 2, 3, 4], "无热度全量合并");
        // L0 未超阈值 → 全量（热度不参与）
        let (sel3, _) = select_compaction_inputs(&[0, 0], 3, &heat, no_cooling, &[0u64; 8], 0);
        assert_eq!(sel3, vec![0, 1]);
    }

    #[test]
    fn select_compaction_caps_l0_input_by_size() {
        // 分批合并（compact_input_max_mb）：L0 总大小超上限 → 只合并 ≤ 上限的部分段
        //（防大 L0 一次全合并长时间阻塞写）；L1→L2 不受限（层内不重叠需全选）。
        let no_heat = &[];
        let no_cooling = &std::collections::HashSet::new();
        // L0 三段各 600MB（总 1.8GB），上限 1GB → 合并 2 段（600+600=1.2GB 仍超 → 只留 2 段保底）
        let sizes = [600u64, 600, 600, 100, 100];
        let (sel, out) = select_compaction_inputs(&[0, 0, 0, 1, 1], 8, no_heat, no_cooling, &sizes, 1024);
        assert_eq!(out, 1);
        assert_eq!(sel.len(), 2, "超限应截断到保底 2 段: {sel:?}");
        // L1→L2（L0 空）：大小上限不生效（全选，保证 L2 无重叠）
        let (sel2, out2) = select_compaction_inputs(&[1, 1], 8, no_heat, no_cooling, &sizes, 1024);
        assert_eq!(out2, 2);
        assert_eq!(sel2, vec![0, 1], "L1→L2 全选不受上限限制");
        // 总大小未超限 → 全量合并
        let small = [100u64, 200, 300];
        let (sel3, _) = select_compaction_inputs(&[0, 0, 0], 8, no_heat, no_cooling, &small, 1024);
        assert_eq!(sel3, vec![0, 1, 2], "未超限全量合并");
        // max=0（不限）→ 全量合并（旧行为）
        let (sel4, _) = select_compaction_inputs(&[0, 0, 0], 8, no_heat, no_cooling, &sizes, 0);
        assert_eq!(sel4, vec![0, 1, 2], "max=0 不限");
    }

    #[test]
    fn cap_by_size_truncates_and_keeps_min_two() {
        // cap_by_size 函数级：未超限全保留；超限从尾部移除到 ≤ max；保底 2 段
        // 未超限 → 全保留
        assert_eq!(cap_by_size(vec![0, 1, 2], &[100, 200, 300], 1024), vec![0, 1, 2]);
        // 超限 → 从尾部移除到 ≤ max（保底 2 段：总 800 > 700 但只剩 2 段即停）
        assert_eq!(cap_by_size(vec![0, 1, 2], &[400, 400, 400], 700), vec![0, 1]);
        // 超限且移除一段即满足 → 保留前缀
        assert_eq!(cap_by_size(vec![0, 1, 2], &[300, 300, 500], 600), vec![0, 1]);
        // 仅 2 段 → 直接返回（保底下限）
        assert_eq!(cap_by_size(vec![0, 1], &[2000, 2000], 100), vec![0, 1]);
        // 单段 → 直接返回
        assert_eq!(cap_by_size(vec![0], &[5000], 100), vec![0]);
        // 剩余段由后续轮次再选（前缀保留 = 本轮输入；未选段仍在 levels 中可再被选）
        let no_heat = &[];
        let no_cooling = &std::collections::HashSet::new();
        let sizes = [400u64, 400, 400];
        let (sel, _) = select_compaction_inputs(&[0, 0, 0], 8, no_heat, no_cooling, &sizes, 700);
        assert_eq!(sel, vec![0, 1], "本轮分批 2 段");
        // 下一轮（同输入，无冷却）：未选段 2 参与（冷却回退/再次全选后 cap）→ 仍能推进收敛
        let (sel2, _) = select_compaction_inputs(&[0, 0, 0], 8, no_heat, no_cooling, &sizes, 700);
        assert_eq!(sel2.len(), 2, "下轮仍可选 2 段（含段 2）");
    }

    #[test]
    fn compact_batches_l0_with_small_input_cap() {
        // 端到端：compact_input_max_mb=1MB，flush 5 段（各 ~4MB）→ 单次 compact 输入被 cap
        //（保底 2 段 < 全部 5 段）→ 分批合并；多轮收敛后数据完整。
        let dir = tmp();
        let mut cfg = small_cfg(64);
        cfg.storage.compact_input_max_mb = 1; // 1MB 上限
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        let big = vec![b'x'; 2048]; // 2KB/行 → 每段 2000 行 ≈ 4MB > 1MB 上限
        for seg in 0..5u64 {
            for i in seg * 2000..seg * 2000 + 2000 {
                cf.put(i, big.clone()).unwrap();
            }
            cf.switch_and_flush().unwrap();
        }
        let before = cf.sst_count();
        assert!(before >= 5, "应产生 5+ SST: {before}");
        let rep = cf.compact().unwrap();
        assert!(
            rep.merged_ssts >= 2 && rep.merged_ssts < before,
            "单轮分批：输入被 cap（保底 2 段 < 全部 {before}），实际合并 {} 段",
            rep.merged_ssts
        );
        assert!(cf.sst_count() < before, "本轮合并后段数减少");
        // 分批合并不丢数据（全部可读）
        for i in 0..10_000u64 {
            let v = cf.get(i).unwrap();
            assert_eq!(v.as_ref().map(|r| r.0.as_slice()), Some(big.as_slice()), "docid={i} 数据完整");
        }
        // 多轮收敛（worker while 语义）：最终 needs_compact=false 且数据仍完整
        let mut rounds = 0;
        while cf.needs_compact() && rounds < 30 {
            cf.compact().unwrap();
            rounds += 1;
        }
        assert!(!cf.needs_compact(), "多轮 {rounds} 轮内收敛");
        for i in (0..10_000u64).step_by(997) {
            let v = cf.get(i).unwrap();
            assert_eq!(v.as_ref().map(|r| r.0.as_slice()), Some(big.as_slice()), "收敛后 docid={i}");
        }
    }

    #[test]
    fn sst_heat_tracks_point_reads() {
        // Ex-5.9：点查命中递增 SST 热度；跨 flush/compact 保持
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in 0..50u64 {
            cf.put(i, format!("v{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.sst_count(), 1);
        assert_eq!(cf.sst_heat(0), 0, "初始零热度");
        // 多次点查（含未命中——未命中不计数）
        for _ in 0..10 {
            cf.get(5).unwrap();
        }
        cf.get(1000).unwrap(); // 未命中（键序在范围外，布隆拦截不计数）
        let h = cf.sst_heat(0);
        assert_eq!(h, 10, "10 次命中计数，未命中不计数");
        // 热度读取不重置（累积）
        cf.get(6).unwrap();
        assert_eq!(cf.sst_heat(0), 11);
    }

    #[test]
    fn flush_then_read_back() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 写入一批触发显式刷盘，验证落盘后可读
        for i in 0..100u64 {
            cf.put(i, format!("value-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert!(cf.sst_count() >= 1);
        // 落盘后仍可读（读路径先内存后磁盘）
        assert_eq!(cf.get(0).unwrap().unwrap().0, b"value-0");
        assert_eq!(cf.get(99).unwrap().unwrap().0, b"value-99");
    }

    #[test]
    fn restart_recovers_data_from_wal_and_sst() {
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..150u64 {
                cf.put(i, format!("v-{i}").into_bytes()).unwrap();
            }
            // 前 100 条刷盘，后 50 条留在 WAL/MemTable
            for _ in 0..2 {
                cf.switch_and_flush().unwrap();
            }
        } // 模拟进程退出（drop 不执行额外清理）

        // 重启：manifest 加载 SST + WAL 回放
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in 0..150u64 {
            assert_eq!(
                cf2.get(i).unwrap().unwrap().0,
                format!("v-{i}").into_bytes(),
                "key {i} 丢失"
            );
        }
    }

    #[test]
    fn get_many_matches_individual_get_across_flush_and_tombstone() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in 0..60u64 {
            cf.put(i, format!("v-{i}").into_bytes()).unwrap();
        }
        cf.delete(7).unwrap();
        // 刷盘：数据落 SST（多 key 共享数据块 → 按块分组批量命中路径）
        cf.switch_and_flush().unwrap();
        // 混入 MemTable 层新写 + 新删除（tombstone 终结路径）
        cf.put(100, b"v-100".to_vec()).unwrap();
        cf.delete(100).unwrap();
        let ids = [0u64, 7, 30, 59, 60, 100, 101];
        let got = cf.get_many(&ids).unwrap();
        assert_eq!(got.len(), ids.len());
        for (i, &d) in ids.iter().enumerate() {
            let expect = cf.get(d).unwrap();
            assert_eq!(
                got[i].as_ref().map(|(v, _)| v.as_slice()),
                expect.as_ref().map(|(v, _)| v.as_slice()),
                "docid {d} get_many 与 get 结果不一致"
            );
        }
        // 删除语义：7（SST tombstone）与 100（MemTable tombstone）均为 None
        assert!(got[1].is_none(), "SST tombstone 应视为不存在");
        assert!(got[5].is_none(), "MemTable tombstone 应视为不存在");
        assert!(got[6].is_none(), "不存在的 key 应为 None");
        assert!(got[2].is_some() && got[3].is_some(), "未删除 key 应命中");
        // 空输入
        assert!(cf.get_many(&[]).unwrap().is_empty());
    }

    #[test]
    fn layered_range_skip_preserves_reads_across_levels() {
        // R 项：层/段两级 Zone Map 粗筛——多段多层（L0/L1 混合）下点查/get_at/get_many
        // 跨层命中正确、越界 key 不假阴性（层范围 = 段范围精确并集）。
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 3 个 L0 段：段 i 覆盖 [i*100, i*100+100)
        for seg in 0..3u64 {
            for i in seg * 100..seg * 100 + 100 {
                cf.put(i, format!("v{seg}-{i}").into_bytes()).unwrap();
            }
            cf.switch_and_flush().unwrap();
        }
        // 3 段 compact → L1（覆盖 [0,299]）；再 flush 第 4 段 → L0（覆盖 [300,399]）混合
        let rep = cf.compact().unwrap();
        assert_eq!(rep.out_level, 1);
        for i in 300..400u64 {
            cf.put(i, format!("v3-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        // 快照层元数据：L0 1 段（[300,399]）、L1 1 段（[0,299]）
        let snap = cf.ssts.load();
        assert_eq!(snap.layer_indices[0].len(), 1, "L0 为第 4 段");
        assert_eq!(snap.layer_indices[1].len(), 1, "L1 为合并输出");
        assert_eq!(
            snap.layer_ranges[1],
            Some((
                crate::keys::encode_docid(0).to_vec(),
                crate::keys::encode_docid(299).to_vec()
            )),
            "L1 范围应覆盖 [0,299]"
        );
        assert_eq!(
            snap.layer_ranges[0],
            Some((
                crate::keys::encode_docid(300).to_vec(),
                crate::keys::encode_docid(399).to_vec()
            )),
            "L0 范围应覆盖 [300,399]"
        );
        drop(snap);
        // 跨层/边界点查（get / get_at / get_many 一致）
        for i in [0u64, 50, 99, 100, 199, 299, 300, 350, 399] {
            let expect = format!("v{}-{i}", i / 100).into_bytes();
            assert_eq!(cf.get(i).unwrap().unwrap().0, expect, "get key {i}");
            let at = cf
                .get_bytes_at(&crate::keys::encode_docid(i), u64::MAX)
                .unwrap()
                .unwrap()
                .0;
            assert_eq!(at, expect, "get_at key {i}");
        }
        let got = cf.get_many(&[0, 100, 299, 300, 399]).unwrap();
        assert_eq!(got[0].as_ref().unwrap().0, b"v0-0".to_vec());
        assert_eq!(got[1].as_ref().unwrap().0, b"v1-100".to_vec());
        assert_eq!(got[2].as_ref().unwrap().0, b"v2-299".to_vec());
        assert_eq!(got[3].as_ref().unwrap().0, b"v3-300".to_vec());
        assert_eq!(got[4].as_ref().unwrap().0, b"v3-399".to_vec());
        // 越界 key（层范围外）→ None，不假阴性
        for miss in [400u64, 999, 10000] {
            assert!(cf.get(miss).unwrap().is_none(), "get miss {miss}");
            assert!(
                cf.get_bytes_at(&crate::keys::encode_docid(miss), u64::MAX)
                    .unwrap()
                    .is_none(),
                "get_at miss {miss}"
            );
        }
    }

    #[test]
    fn compaction_urgency_grows_with_l0_pressure() {
        // W 项：紧迫度 = L0 段数 ×10 + 大小超限 +8——多段 flush 后递增（跨列族调度主因子）
        let dir = tmp();
        let cfg = small_cfg(64);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf.compaction_urgency(), 0, "空库无压力");
        for seg in 0..3u64 {
            for i in seg * 50..seg * 50 + 50 {
                cf.put(i, format!("v-{i}").into_bytes()).unwrap();
            }
            cf.switch_and_flush().unwrap();
            let u = cf.compaction_urgency();
            let expect = (seg + 1) as u32 * 10;
            assert_eq!(u, expect, "L0 段数 {} → urgency {u}", seg + 1);
        }
        // 大小软阈值：l0_max_size_bytes 配小 → 超限追加 +8
        let dir2 = tmp();
        let mut cfg2 = small_cfg(64);
        cfg2.storage.l0_max_size_mb = 1; // 1MB 大小软阈值
        let mut cf2 = ColumnFamily::open("primary", &dir2, &cfg2).unwrap();
        // 单段 ~3.2MB（50×64KB）→ 超 1MB 软阈值
        for i in 0..50u64 {
            cf2.put(i, vec![0x55u8; 64 * 1024]).unwrap();
        }
        cf2.switch_and_flush().unwrap();
        assert!(
            cf2.compaction_urgency() >= 18,
            "大小超限应追加 +8（l0=1 → 10+8，实际 {}）",
            cf2.compaction_urgency()
        );
    }

    #[test]
    fn needs_compact_by_l0_size_threshold() {
        let dir = tmp();
        let mut cfg = small_cfg(256);
        cfg.storage.l0_max_size_mb = 1; // 1MB 大小软阈值（叠加在段数阈值之上）
        cfg.memtable.max_size_mb = 1; // 1MB MemTable → 写入过程自动多次 flush
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        let val = vec![0x42u8; 64 * 1024];
        for i in 0..64u64 {
            cf.put(i, val.clone()).unwrap(); // 64KB×64 ≈ 4MB → ~4 次 flush
        }
        cf.switch_and_flush().unwrap(); // 清空残余 MemTable
        assert!(
            cf.l0_count() >= 2,
            "应产生多个 L0 段（实际 {}）",
            cf.l0_count()
        );
        assert!(
            cf.needs_compact(),
            "L0 总大小超阈值（{} B > 1MB）应触发合并",
            cf.l0_bytes()
        );
        // 多段合并收敛后大小阈值不再触发
        cf.compact().unwrap();
        assert!(!cf.needs_compact());
        // 单段超大小阈值不触发合并（单段为已排序文件，合并是纯无收益重写）
        let dir2 = tmp();
        let mut cfg2 = small_cfg(256);
        cfg2.storage.l0_max_size_mb = 1;
        cfg2.memtable.max_size_mb = 8; // 大 MemTable：2MB 值不触发自动 flush
        let mut cf2 = ColumnFamily::open("primary", &dir2, &cfg2).unwrap();
        cf2.put(0, vec![0x42u8; 2 * 1024 * 1024]).unwrap(); // 单条 2MB > 1MB
        cf2.switch_and_flush().unwrap();
        assert_eq!(cf2.l0_count(), 1);
        assert!(!cf2.needs_compact(), "单段超大小阈值不应触发合并");
    }

    #[test]
    fn l0_bytes_and_sst_bytes_match_disk_sizes() {
        // 修复（96ac6bc）：l0_bytes/sst_bytes 读快照 sizes 缓存（零 fs::metadata），
        // 验证缓存值与磁盘实际文件大小一致——open/flush/compact 三个构建点均正确。
        let dir = tmp();
        let cfg = small_cfg(64);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // flush 3 段（L0）
        for seg in 0..3u64 {
            for i in seg * 50..seg * 50 + 50 {
                cf.put(i, format!("v-{i}").into_bytes()).unwrap();
            }
            cf.switch_and_flush().unwrap();
        }
        let disk_l0: u64 = {
            let snap = cf.ssts.load();
            snap.ssts
                .iter()
                .enumerate()
                .filter(|(i, _)| snap.levels[*i] == 0)
                .map(|(_, s)| std::fs::metadata(s.path()).unwrap().len())
                .sum()
        };
        let disk_all: u64 = {
            let snap = cf.ssts.load();
            snap.ssts
                .iter()
                .map(|s| std::fs::metadata(s.path()).unwrap().len())
                .sum()
        };
        assert_eq!(cf.l0_bytes(), disk_l0, "L0 字节 = 磁盘实际和（flush 构建点 sizes 缓存）");
        assert_eq!(cf.sst_bytes(), disk_all, "全部字节 = 磁盘实际和");
        // sizes 与每段 file_len 一致（修复双缓存同源）
        let snap = cf.ssts.load();
        for (i, s) in snap.ssts.iter().enumerate() {
            assert_eq!(snap.sizes[i], s.file_len(), "sizes[{i}] = file_len");
        }
        // compact 发布点：新快照 sizes 随合并更新，仍与磁盘一致
        cf.compact().unwrap();
        let disk_all2: u64 = {
            let snap = cf.ssts.load();
            snap.ssts
                .iter()
                .map(|s| std::fs::metadata(s.path()).unwrap().len())
                .sum()
        };
        assert_eq!(cf.sst_bytes(), disk_all2, "compact 后 sizes 仍与磁盘一致");
    }

    #[test]
    fn sizes_cache_survives_reopen() {
        // 修复：load（重开）构建点 sizes 正确——重开后 l0_bytes/sst_bytes 仍与磁盘一致
        let dir = tmp();
        let cfg = small_cfg(64);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..50u64 {
                cf.put(i, format!("v-{i}").into_bytes()).unwrap();
            }
            cf.switch_and_flush().unwrap();
        } // drop = 关闭
        let cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        let disk_all: u64 = {
            let snap = cf2.ssts.load();
            snap.ssts
                .iter()
                .map(|s| std::fs::metadata(s.path()).unwrap().len())
                .sum()
        };
        assert_eq!(cf2.sst_bytes(), disk_all, "重开后 sizes 缓存仍正确");
        assert_eq!(cf2.l0_bytes(), disk_all, "单层全 L0 → l0_bytes = 全部字节");
    }

    // ---------- S 项：MemTable 多版本（严格 MVCC 快照读） ----------

    #[test]
    fn snapshot_read_sees_old_version_while_both_in_memtable() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 同一 key 连续写入两次（均未刷盘，旧实现仅保留最新 → 快照读漏掉旧版本）
        let s1 = cf.put(1, b"v1".to_vec()).unwrap();
        let s2 = cf.put(1, b"v2".to_vec()).unwrap();
        assert!(s2 > s1);
        // 快照落在 s1..s2 → 应读到 v1（S 项修复点）
        assert_eq!(cf.get_bytes_at(&encode_docid(1), s1).unwrap().unwrap().0, b"v1");
        assert_eq!(cf.get_bytes_at(&encode_docid(1), s2).unwrap().unwrap().0, b"v2");
        assert!(cf.get_bytes_at(&encode_docid(1), s1 - 1).unwrap().is_none());
        // 非快照读最新
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"v2");
        // 刷盘后快照仍可读旧版本（SST 多版本落盘）
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.get_bytes_at(&encode_docid(1), s1).unwrap().unwrap().0, b"v1");
        assert_eq!(cf.get_bytes_at(&encode_docid(1), s2).unwrap().unwrap().0, b"v2");
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"v2");
    }

    #[test]
    fn snapshot_read_sees_deleted_as_tombstone_after_memtable_delete() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        let s1 = cf.put(1, b"v1".to_vec()).unwrap();
        let sd = cf.delete(1).unwrap();
        // 快照在删除前 → 可见 v1；删除点后 → None
        assert_eq!(cf.get_bytes_at(&encode_docid(1), s1).unwrap().unwrap().0, b"v1");
        assert!(cf.get_bytes_at(&encode_docid(1), sd).unwrap().is_none());
        assert!(cf.get(1).unwrap().is_none(), "非快照读：当前已删除");
        // 刷盘后语义保持
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.get_bytes_at(&encode_docid(1), s1).unwrap().unwrap().0, b"v1");
        assert!(cf.get_bytes_at(&encode_docid(1), sd).unwrap().is_none());
    }

    #[test]
    fn reopen_preserves_wal_only_data() {
        // 步骤 15 暴露的预存 bug：reopen 不得截断 WAL，未刷盘数据必须经回放恢复
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 1..=5u64 {
                cf.put(i, format!("v-{i}").into_bytes()).unwrap();
            }
            // 不刷盘：数据仅存在于 WAL + MemTable
        }
        {
            let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 1..=5u64 {
                assert_eq!(
                    cf2.get(i).unwrap().unwrap().0,
                    format!("v-{i}").into_bytes(),
                    "key {i} 丢失（WAL 被截断？）"
                );
            }
            // 新写入 seq 接续，同 key 覆盖正确
            cf2.put(3, b"v-3-updated".to_vec()).unwrap();
            assert_eq!(cf2.get(3).unwrap().unwrap().0, b"v-3-updated");
        }
        // 再次重开：追加写入与覆盖均正确
        {
            let mut cf3 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            assert_eq!(cf3.get(3).unwrap().unwrap().0, b"v-3-updated");
            assert_eq!(cf3.get(5).unwrap().unwrap().0, b"v-5");
            assert_eq!(cf3.get(1).unwrap().unwrap().0, b"v-1");
        }
    }

    // ---------- 环形 WAL 集成（design 4.3，M6-1） ----------

    fn ring_cfg(mem_mb: usize) -> Config {
        let mut cfg = small_cfg(mem_mb);
        cfg.storage.wal_mode = "ring".into();
        cfg.storage.wal_ring_size_mb = 1;
        cfg
    }

    #[test]
    fn ring_wal_mode_persists_and_recovers() {
        let dir = tmp();
        let cfg = ring_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            cf.put(1, b"v1".to_vec()).unwrap();
            cf.put(2, b"v2".to_vec()).unwrap();
            assert_eq!(cf.get(1).unwrap().unwrap().0, b"v1");
        }
        // 重启：环形 WAL 回放恢复未刷盘数据
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"v1");
        assert_eq!(cf.get(2).unwrap().unwrap().0, b"v2");
        // 覆盖 + 追加后再次重启
        cf.put(2, b"v2-updated".to_vec()).unwrap();
        drop(cf);
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.get(2).unwrap().unwrap().0, b"v2-updated");
    }

    #[test]
    fn ring_wal_full_forces_flush_keeps_data() {
        // 写入量超过 1MB 环形容量 → 强制 Flush 腾空，数据不丢
        let dir = tmp();
        let cfg = ring_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        const N: u64 = 60_000;
        for chunk in 0..6u64 {
            for i in chunk * 10_000..(chunk + 1) * 10_000 {
                cf.put_bytes_nosync(i.to_le_bytes().to_vec(), format!("value-{i}").into_bytes())
                    .unwrap();
            }
            cf.sync_wal().unwrap();
        }
        // 抽查全量数据（跨 MemTable / SST / 环形覆盖后均完整）
        for i in (0..N).step_by(997) {
            assert_eq!(
                cf.get_bytes(&i.to_le_bytes()).unwrap().unwrap().0,
                format!("value-{i}").into_bytes(),
                "key {i} 丢失（环形覆盖未刷盘记录？）"
            );
        }
        // 重启后仍完整（环形恢复 + SST 合并读）
        drop(cf);
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in (0..N).step_by(5003) {
            assert_eq!(
                cf2.get_bytes(&i.to_le_bytes()).unwrap().unwrap().0,
                format!("value-{i}").into_bytes(),
                "重启后 key {i} 丢失"
            );
        }
    }

    #[test]
    fn ttl_helpers() {
        assert_eq!(epoch_days(0), 0);
        assert_eq!(epoch_days(86_400), 1);
        assert_eq!(epoch_days(-1), -1); // div_euclid 向负无穷取整
        assert_eq!(parse_sst_date("sst-00020697-00000001.sst"), Some(20_697));
        assert_eq!(parse_sst_date("sst-00000001.sst"), None);
        assert_eq!(parse_sst_date("manifest.json"), None);
    }

    #[test]
    fn ttl_buckets_and_expiry() {
        // 阶段 1.5 TTL：按天分桶写 SST；重启时过期桶整文件删除，默认桶永不过期
        let dir = tmp();
        let mut cfg = small_cfg(256);
        cfg.storage.ttl_days = Some(2);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            let today = format!(r#"{{"v":"t","timestamp":{now}}}"#).into_bytes();
            let yesterday = format!(r#"{{"v":"y","timestamp":{}}}"#, now - 86_400).into_bytes();
            let old = format!(r#"{{"v":"o","timestamp":{}}}"#, now - 86_400 * 10).into_bytes();
            cf.put(1, today).unwrap();
            cf.put(2, yesterday).unwrap();
            cf.put(3, old).unwrap();
            cf.put(4, b"no-timestamp-raw".to_vec()).unwrap();
            cf.switch_and_flush().unwrap();
            assert!(cf.sst_count() >= 4, "应分 4 个桶，实际 {}", cf.sst_count());
            // 未过期前全部可读
            assert!(cf.get(1).unwrap().is_some());
            assert!(cf.get(3).unwrap().is_some());
        }
        // 重启：10 天前的桶过期删除
        {
            let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            assert!(cf2.get(1).unwrap().is_some(), "今天桶应保留");
            assert!(cf2.get(2).unwrap().is_some(), "昨天桶应保留");
            assert!(cf2.get(3).unwrap().is_none(), "10 天前的桶应过期删除");
            assert!(
                cf2.get(4).unwrap().is_some(),
                "默认桶（无时间字段）永不过期"
            );
            // 过期文件已物理删除
            let names: Vec<String> = std::fs::read_dir(&dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().to_string())
                .filter(|f| f.starts_with(SST_PREFIX))
                .collect();
            assert!(
                names
                    .iter()
                    .any(|f| parse_sst_date(f).is_none_or(|d| d >= today_epoch_days() - 2)),
                "过期 SST 应已删除，剩余: {names:?}"
            );
        }
    }

    #[test]
    fn pax_cf_flush_read_roundtrip() {
        // 阶段 1.5 PAX：配置 hot_fields 后 flush 落盘列式块，读回语义等值；非 JSON 值回退行式
        let dir = tmp();
        let mut cfg = small_cfg(256);
        cfg.storage.hot_fields = vec!["status".to_string(), "city".to_string()];
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(
            1,
            br#"{"status":"active","city":"beijing","amount":10}"#.to_vec(),
        )
        .unwrap();
        cf.put(2, br#"{"status":"inactive","city":"shanghai"}"#.to_vec())
            .unwrap();
        cf.switch_and_flush().unwrap();
        assert!(cf.sst_count() >= 1);
        assert_eq!(
            String::from_utf8_lossy(&cf.get(1).unwrap().unwrap().0),
            r#"{"status":"active","city":"beijing","amount":10}"#
        );
        assert_eq!(
            String::from_utf8_lossy(&cf.get(2).unwrap().unwrap().0),
            r#"{"status":"inactive","city":"shanghai"}"#
        );
        // 非 JSON 值 → 行式块（同一 v4 文件体系，Reader 兼容）
        cf.put(3, b"raw-bytes".to_vec()).unwrap();
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.get(3).unwrap().unwrap().0, b"raw-bytes");
    }

    #[test]
    fn manifest_persists_sst_list() {
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..50u64 {
                cf.put(i, b"v".to_vec()).unwrap();
            }
            cf.switch_and_flush().unwrap();
        }
        let text = std::fs::read_to_string(dir.join(MANIFEST_FILE)).unwrap();
        assert!(text.contains(SST_PREFIX));
        let m: crate::storage::manifest::Manifest = serde_json::from_str(&text).unwrap();
        assert!(!m.sst_files.is_empty());
    }

    #[test]
    fn corrupted_sst_rejected_on_open() {
        // 损坏注入：SST 头部魔数被破坏 → 启动必须报 Corrupted 而非 panic（development 9.3）
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..100u64 {
                cf.put(i, b"v".to_vec()).unwrap();
            }
            cf.switch_and_flush().unwrap();
        }
        let sst_path = dir.join(format!("{SST_PREFIX}00000001.sst"));
        assert!(sst_path.exists(), "SST 文件应已生成");
        let mut data = std::fs::read(&sst_path).unwrap();
        data[0..8].copy_from_slice(&[0xFF; 8]); // 破坏魔数
        std::fs::write(&sst_path, &data).unwrap();

        let err = match ColumnFamily::open("primary", &dir, &cfg) {
            Ok(_) => panic!("损坏 SST 应打开失败"),
            Err(e) => e,
        };
        assert!(
            matches!(err, Error::Corrupted(_)),
            "损坏 SST 应报 Corrupted，实际 {err:?}"
        );
    }

    #[test]
    fn wal_partial_tail_recovers_cleanly() {
        // 崩溃恢复：WAL 尾部半条记录（模拟断电时只写入一半）→ 重启只恢复完整记录、不 panic
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 1..=10u64 {
                cf.put(i, format!("v-{i}").into_bytes()).unwrap();
            }
        }
        // 截断 WAL 至一半字节（人为制造半条尾部记录）
        let wal_path = dir.join(WAL_FILE);
        let mut data = std::fs::read(&wal_path).unwrap();
        assert!(data.len() > 40);
        data.truncate(data.len() / 2);
        std::fs::write(&wal_path, &data).unwrap();

        // 回放：完整记录应恢复，半条记录被丢弃，不 panic
        let recs = WalReader::recover(&wal_path).unwrap();
        assert!(!recs.is_empty(), "至少应恢复一条完整记录");
        assert!(recs.len() <= 10);

        // reopen 全链路可用
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for r in &recs {
            assert!(cf2.get_bytes(&r.key).unwrap().is_some(), "恢复的记录应可读");
        }
    }
    #[test]
    fn scan_range_covers_memtable_and_sst() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 部分写入并刷盘，部分留在 MemTable
        for i in 0..30u64 {
            cf.put(i, format!("v-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        for i in 30..40u64 {
            cf.put(i, format!("v-{i}").into_bytes()).unwrap();
        }
        // 更新一个已落盘 key，验证去重取最新
        cf.put(5, b"v-updated".to_vec()).unwrap();

        let rows = cf.scan_range(Some(0), Some(39)).unwrap();
        assert_eq!(rows.len(), 40);
        assert_eq!(rows[0], (0, b"v-0".to_vec()));
        assert_eq!(rows[39], (39, b"v-39".to_vec()));
        assert_eq!(rows[5], (5, b"v-updated".to_vec()));

        // 无边界扫描
        let all = cf.scan_range(None, None).unwrap();
        assert_eq!(all.len(), 40);
    }

    #[test]
    fn scan_range_empty_window() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in 0..10u64 {
            cf.put(i, b"v".to_vec()).unwrap();
        }
        // 无交集区间 → 空
        assert!(cf.scan_range(Some(100), Some(200)).unwrap().is_empty());
    }

    #[test]
    fn composite_index_prefix_query() {
        // 步骤 10：组合索引 = ColumnFamily + encode_composite_key(fields, docid)
        use crate::keys::{decode_composite_key, encode_composite_key};
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("cidx", &dir, &cfg).unwrap();
        // 写入 (status=active, type=click, docid)
        for docid in [1u64, 5, 9, 20] {
            let key = encode_composite_key(&[b"active", b"click"], docid);
            cf.put_bytes(key, docid.to_le_bytes().to_vec()).unwrap();
        }
        // 干扰项：status=active 但 type=view
        let key = encode_composite_key(&[b"active", b"view"], 100);
        cf.put_bytes(key, 100u64.to_le_bytes().to_vec()).unwrap();

        // 前缀查询 active/click：组合键有序，范围 [active/click/0, active/click/FFFF]
        let start = encode_composite_key(&[b"active", b"click"], 0);
        let end = encode_composite_key(&[b"active", b"click"], u64::MAX);
        let mut hits = Vec::new();
        let rows = cf.scan_raw_range(Some(&start), Some(&end)).unwrap();
        for (k, _v) in rows {
            let (fields, docid) = decode_composite_key(&k).unwrap();
            assert_eq!(fields, vec![b"active".to_vec(), b"click".to_vec()]);
            hits.push(docid);
        }
        hits.sort();
        assert_eq!(hits, vec![1, 5, 9, 20]);
    }

    // ---------- M3（§26 多表）：Flush / Compaction 按表切分（同表合并） ----------

    /// 表内 docid：docid = table<<48 | row
    fn tdoc(table: u16, row: u64) -> u64 {
        ((table as u64) << 48) | row
    }
    /// 单表 row 容量 2^48：表内最大 row_id（窗口上界用，勿用 u64::MAX——OR 全 1 会越出本表区间）
    const TROW_MAX: u64 = (1u64 << 48) - 1;

    /// 快照内各 SST 归属表 id（单表段 → Some(tid)；混表/空段 → None），保持快照顺序。
    fn sst_tables(cf: &ColumnFamily) -> Vec<Option<u16>> {
        let snap = cf.ssts.load();
        snap.ssts
            .iter()
            .map(|s| match s.key_range() {
                Some((mn, mx)) => match (key_table_id(mn), key_table_id(mx)) {
                    (Some(a), Some(b)) if a == b => Some(a),
                    _ => None,
                },
                None => None,
            })
            .collect()
    }

    #[test]
    fn m3_flush_splits_imm_by_table() {
        // 实施清单①：单次 flush 内多表 docid 同落 Immutable → 按表边界切多个 SST
        let dir = tmp();
        let cfg = small_cfg(64);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.enable_table_split();
        // 默认表 0 / 表 1 / 表 7 各 3 行（row 同号共存）
        for row in [1u64, 2, 3] {
            cf.put(tdoc(0, row), format!("t0-r{row}").into_bytes()).unwrap();
            cf.put(tdoc(1, row), format!("t1-r{row}").into_bytes()).unwrap();
            cf.put(tdoc(7, row), format!("t7-r{row}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.sst_count(), 3, "3 张表应切 3 个 SST，实际 {}", cf.sst_count());
        // 每个 SST 均为单表段，且三表齐全
        let mut ts: Vec<u16> = sst_tables(&cf).into_iter().map(|t| t.unwrap()).collect();
        ts.sort();
        assert_eq!(ts, vec![0, 1, 7], "切分后每文件单表，实际 {ts:?}");
        // 全部行读回
        for row in [1u64, 2, 3] {
            for t in [0u16, 1, 7] {
                let got = cf.get(tdoc(t, row)).unwrap().expect("行应可读");
                assert_eq!(got.0, format!("t{t}-r{row}").into_bytes());
            }
        }
    }

    #[test]
    fn m3_compact_merges_per_table_and_converges() {
        // 实施清单② + 同表合并：flush 按表切分后，压缩只合并同表段；
        // 跨表每表 1 段即"按表收敛"——不反复重写（needs_compact 归零、重复 compact 无空转）
        let dir = tmp();
        let cfg = small_cfg(64);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.enable_table_split();
        // 表 1：两批 flush（L0 同表 2 段 → 需合并去重）
        for row in 1..=3u64 {
            cf.put(tdoc(1, row), format!("t1-a{row}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        for row in 2..=4u64 {
            cf.put(tdoc(1, row), format!("t1-b{row}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        // 表 2：一批 flush（L0 单段）
        for row in 1..=4u64 {
            cf.put(tdoc(2, row), format!("t2-r{row}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        // L0 = 表1×2 + 表2×1
        assert_eq!(cf.sst_count(), 3);
        // 压缩至收敛（直接驱动：默认 L0 阈值较高，逐轮 compact 直至 no-op）
        let mut guard = 0;
        loop {
            let rep = cf.compact().unwrap();
            if rep.merged_ssts == 0 {
                break;
            }
            guard += 1;
            assert!(guard < 8, "压缩应快速收敛（当前 {} 轮）", guard);
        }
        // 收敛：跨表每表 1 段（表1 去重为 1 段、表2 1 段），不再需要压缩
        eprintln!("DEBUG: sst_count={}, needs_compact={}, levels={:?}, l1_tf={}, effective_l0={}, l0_max_size={}",
            cf.sst_count(),
            cf.needs_compact(),
            { let snap = cf.ssts.load(); snap.levels.clone() },
            cf.l1_trigger_files.load(std::sync::atomic::Ordering::Relaxed),
            cf.effective_l0_threshold(),
            cf.l0_max_size_bytes,
        );
        assert!(!cf.needs_compact(), "按表收敛后不应再触发压缩");
        let mut ts: Vec<u16> = sst_tables(&cf).into_iter().map(|t| t.unwrap()).collect();
        ts.sort();
        assert_eq!(ts, vec![1, 2], "收敛后每表 1 段，实际 {ts:?}");
        assert_eq!(cf.sst_count(), 2, "收敛后文件数 = 表数");
        // 重复 compact 不应空转重写（无新工作 → merged=0）
        let rep = cf.compact().unwrap();
        assert_eq!(rep.merged_ssts, 0, "收敛态重复 compact 应为 no-op");
        // 语义：表 1 后写覆盖先写、行 1..4 各 1 条；表 2 独立不受影响
        let t1 = cf.scan_range(Some(tdoc(1, 0)), Some(tdoc(1, TROW_MAX))).unwrap();
        assert_eq!(t1.len(), 4);
        let b4 = cf.get(tdoc(1, 4)).unwrap().unwrap();
        assert_eq!(b4.0, b"t1-b4");
        let a1 = cf.get(tdoc(1, 1)).unwrap().unwrap();
        assert_eq!(a1.0, b"t1-a1"); // 未覆盖的行保持 a 批次
        let b2 = cf.get(tdoc(1, 2)).unwrap().unwrap();
        assert_eq!(b2.0, b"t1-b2"); // 覆盖生效
        let t2 = cf.scan_range(Some(tdoc(2, 0)), Some(tdoc(2, TROW_MAX))).unwrap();
        assert_eq!(t2.len(), 4);
    }

    #[test]
    fn m3_single_table_regression_stays_single_file() {
        // 实施清单⑤：table_id=0（含旧库全量）flush/compact 均单文件输出，行为与旧一致
        let dir = tmp();
        let cfg = small_cfg(64);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.enable_table_split();
        for i in 1..=500u64 {
            cf.put(i, format!("doc-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.sst_count(), 1, "单表 flush 应 1 个 SST，实际 {}", cf.sst_count());
        // 多批 flush → L0 多段 → 压缩收敛单段
        for i in 501..=1000u64 {
            cf.put(i, format!("doc-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        let mut guard = 0;
        loop {
            let rep = cf.compact().unwrap();
            if rep.merged_ssts == 0 {
                break;
            }
            guard += 1;
            assert!(guard < 8, "单表压缩应快速收敛（{} 轮）", guard);
        }
        assert_eq!(cf.sst_count(), 1, "单表压缩后应收敛为 1 段，实际 {}", cf.sst_count());
        assert_eq!(cf.scan_range(None, None).unwrap().len(), 1000);
    }

    #[test]
    fn m3_drop_table_range_files_physically_removes_table_ssts() {
        // 实施清单④：表切分后 DROP TABLE 物理删该表区间专属文件，manifest 同步、他表不受影响
        let dir = tmp();
        let cfg = small_cfg(64);
        let sst_dir = dir.clone();
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            cf.enable_table_split();
            for row in 1..=3u64 {
                cf.put(tdoc(1, row), format!("t1-r{row}").into_bytes()).unwrap();
            }
            cf.switch_and_flush().unwrap();
            for row in 1..=3u64 {
                cf.put(tdoc(2, row), format!("t2-r{row}").into_bytes()).unwrap();
            }
            cf.switch_and_flush().unwrap();
            assert_eq!(cf.sst_count(), 2);
            // 模拟"先逻辑删再物理回收"（引擎 drop_table_range 先逐 docid 墓碑）
            for row in 1..=3u64 {
                cf.delete(tdoc(1, row)).unwrap();
            }
            cf.sync_wal().unwrap();
            let n = cf.drop_table_range_files(1).unwrap();
            assert_eq!(n, 1, "表 1 专属 SST 应被物理删除 1 个");
            assert_eq!(cf.sst_count(), 1, "表 2 SST 保留");
            assert!(cf.get(tdoc(2, 1)).unwrap().is_some(), "他表数据不受影响");
            assert!(cf.get(tdoc(1, 1)).unwrap().is_none(), "被删表行不可见");
        }
        // 重启：manifest 不悬空（被删文件不加载），表 2 数据仍在
        let mut cf2 = ColumnFamily::open("primary", &sst_dir, &cfg).unwrap();
        assert_eq!(cf2.sst_count(), 1);
        assert!(cf2.get(tdoc(2, 2)).unwrap().is_some());
        // 磁盘上表 1 的 SST 已消失
        let names: Vec<String> = std::fs::read_dir(&sst_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|f| f.starts_with(SST_PREFIX))
            .collect();
        assert_eq!(names.len(), 1, "磁盘应只剩表 2 的 SST，实际 {names:?}");
    }
