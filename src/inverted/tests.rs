use super::segment::{MANIFEST_FILE, SegmentManifest};
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;

    #[test]
    fn posting_lru_dual_zone_protects_hot_terms() {
        // Ex-8.8：双区 LRU——命中提升进 protected 后，低频 term 突发不再逐出热点；
        // 冷 term 的缓存仍被有界约束（总量不超预算）
        let mut lru = PostingLru::new(20); // protected 12 + probation 8
        let bm = |n: u64| {
            let mut b = Posting::new();
            b.insert(n);
            b
        };
        // 热点 term：入缓存并命中一次 → 提升 protected
        lru.put("hot".to_string(), Arc::new(bm(1)));
        assert!(lru.get("hot").is_some(), "首次 probation 命中应提升");
        assert!(lru.get("hot").is_some(), "protected 直返");
        // 低频突发 40 个冷 term（总量远超预算，均为 miss 入缓存不命中）→ 只驱逐 probation 冷项
        for i in 0..40u64 {
            let t = format!("cold-{i}");
            lru.put(t, Arc::new(bm(i + 10)));
        }
        assert!(
            lru.get("hot").is_some(),
            "protected 热点不应被冷 term 突发逐出"
        );
        // 有界性：protected ≤ 12 且 probation ≤ 8（总项数受 20 预算约束）
        assert!(lru.protected.len() <= 12);
        assert!(lru.probation.len() <= 8);
        // 最老冷项已被逐出（缓存有界生效）
        assert!(lru.get("cold-0").is_none() || lru.get("cold-39").is_none());
        lru.clear();
        assert!(lru.get("hot").is_none(), "写路径清空应双区全清");
    }

    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let name = format!("inv-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    #[test]
    fn add_and_search_in_memory() {
        let idx = InvertedIndex::open(&tmp(), 10_000).unwrap();
        idx.add("rust", 1);
        idx.add("rust", 3);
        idx.add("rust", 5);
        idx.add("go", 2);
        let r = idx.search("rust").unwrap();
        assert!(r.contains(1) && r.contains(3) && r.contains(5));
        assert!(!r.contains(2));
        assert!(idx.search("go").unwrap().contains(2));
        assert!(idx.search("absent").unwrap().is_empty());
    }

    // ---------- G 补充：段数据 mmap 化（只读新段免全文件读取） ----------

    #[test]
    fn segment_data_mmap_registered_on_flush_and_queryable() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 10_000).unwrap();
        for d in 1..=50u64 {
            idx.add("term-a", d);
        }
        idx.flush_segment().unwrap();
        // flush 预注册 mmap：data_files 含新段
        let seg = idx.segments.load()[0].clone();
        assert!(
            idx.data_files.load().contains_key(&seg),
            "flush 后应预注册段数据 mmap"
        );
        // 查询命中（mmap 切片反序列化路径）
        let r = idx.search("term-a").unwrap();
        assert_eq!(r.len(), 50);
        // 未命中 term 走 FST None 分支
        assert!(idx.search("absent-term").unwrap().is_empty());
    }

    #[test]
    fn segment_data_mmap_lazy_load_after_reopen() {
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open(&dir, 10_000).unwrap();
            for d in 1..=30u64 {
                idx.add("lazy", d);
            }
            idx.flush_segment().unwrap();
        }
        // 重开：data_files 空（运行期缓存不持久化），首次查询懒加载注册
        let idx = InvertedIndex::open(&dir, 10_000).unwrap();
        assert!(idx.data_files.load().is_empty(), "重开应懒加载");
        let r = idx.search("lazy").unwrap();
        assert_eq!(r.len(), 30);
        assert!(!idx.data_files.load().is_empty(), "首次查询后应注册 mmap");
    }

    #[test]
    fn segment_data_mmap_survives_gc() {
        let dir = tmp();
        let mut idx = InvertedIndex::open_with_gc(&dir, 10_000, "fst", 1).unwrap();
        for d in 1..=100u64 {
            idx.add("gc-a", d);
        }
        idx.flush_segment().unwrap();
        for d in 1..=100u64 {
            idx.add("gc-b", d);
        }
        idx.flush_segment().unwrap();
        assert_eq!(idx.segment_count(), 2);
        let g = idx.gc().unwrap();
        assert_eq!(g.merged, 2);
        assert_eq!(idx.segment_count(), 1);
        // GC 后：新段已映射，旧段映射已释放，查询仍正确
        let seg = idx.segments.load()[0].clone();
        assert!(
            idx.data_files.load().contains_key(&seg),
            "GC 后应映射合并新段"
        );
        let ra = idx.search("gc-a").unwrap();
        let rb = idx.search("gc-b").unwrap();
        assert_eq!(ra.len(), 100);
        assert_eq!(rb.len(), 100);
    }

    // ---------- 位图索引（design 5.2.4，M7-2） ----------

    #[test]
    fn gc_merges_preserve_stats_payload_v5() {
        // Ex-9.3 第②步：GC 合并段时须保留/合并 v5 统计载荷（term_stats 前后一致）。
        let dir = tmp();
        let mut idx = InvertedIndex::open_with_gc(&dir, 10_000, "fst", 1).unwrap();
        for d in 1..=5u64 {
            idx.add("gc-a", d);
            idx.add_stats("gc-a", &[Some(d as f64)]);
        }
        idx.flush_segment().unwrap();
        for d in 6..=10u64 {
            idx.add("gc-a", d);
            idx.add_stats("gc-a", &[Some(d as f64)]);
        }
        idx.flush_segment().unwrap();
        assert_eq!(idx.segment_count(), 2);
        let before = idx.term_stats("gc-a").unwrap().unwrap()[0];
        assert_eq!(before.n, 10);
        assert_eq!(before.sum, 55.0); // 1..=10
        assert_eq!(before.min, 1.0);
        assert_eq!(before.max, 10.0);
        let g = idx.gc().unwrap();
        assert_eq!(g.merged, 2);
        assert_eq!(idx.segment_count(), 1);
        let after = idx.term_stats("gc-a").unwrap().unwrap()[0];
        assert_eq!(after.n, before.n, "GC 后 n 保持");
        assert_eq!(after.sum, before.sum, "GC 后 sum 保持");
        assert_eq!(after.min, before.min);
        assert_eq!(after.max, before.max);
        assert_eq!(idx.search("gc-a").unwrap().len(), 10, "GC 后 posting 完整");
    }

    #[test]
    fn group_stats_enumerates_field_values_with_counts_and_stats() {
        // Ex-9.3 第④步：词典枚举 GROUP BY 行（值, 组行数, 数值统计），mem 与落盘段结果一致。
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 10_000).unwrap();
        for (term, d) in [
            ("status=active", 1u64),
            ("status=active", 2),
            ("status=active", 3),
            ("status=inactive", 4),
        ] {
            idx.add(term, d);
            let v = if term.ends_with("active") { d as f64 } else { 5.0 };
            idx.add_stats(term, &[Some(v)]);
        }
        let gs = idx.group_stats("status").unwrap();
        assert_eq!(gs.len(), 2, "active/inactive 两组");
        assert_eq!(gs[0].0, "active");
        assert_eq!(gs[0].1, 3, "active 组行数");
        assert_eq!(gs[0].2[0].n, 3);
        assert_eq!(gs[0].2[0].sum, 6.0); // 1+2+3
        assert_eq!(gs[0].2[0].min, 1.0);
        assert_eq!(gs[0].2[0].max, 3.0);
        assert_eq!(gs[1].0, "inactive");
        assert_eq!(gs[1].1, 1);
        assert_eq!(gs[1].2[0].sum, 4.0); // inactive 文档 d=4（ends_with("active") 亦真）
        // flush 落盘后：结果一致（段枚举路径）
        idx.flush_segment().unwrap();
        let gs2 = idx.group_stats("status").unwrap();
        assert_eq!(gs2, gs, "flush 后组枚举不变");
    }

    // ---------- G 项：posting 检索优化（term→bitmap 缓存） ----------

    #[test]
    fn search_hits_bitmap_whitelist_fast_path() {
        // 白名单字段 term：search 直接返回全量内存位图（与段遍历结果一致）
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 10_000).unwrap();
        idx.with_bitmap_fields(&["status".to_string(), "city".to_string()])
            .unwrap();
        for (term, d) in [
            ("status=active", 1u64),
            ("status=active", 3),
            ("status=inactive", 2),
            ("status=active", 5),
            ("city=beijing", 1),
            ("city=beijing", 2),
        ] {
            idx.add(term, d);
        }
        idx.flush_segment().unwrap(); // 落盘后白名单路径仍应全量
        let r = idx.search("status=active").unwrap();
        assert!(r.contains(1) && r.contains(3) && r.contains(5));
        assert!(!r.contains(2), "白名单路径不应含 inactive docid");
        let bj = idx.search("city=beijing").unwrap();
        assert_eq!(bj.len(), 2);
    }

    #[test]
    fn posting_cache_serves_repeat_terms_and_invalidates_on_write() {
        // 非白名单 term：首次反序列化入缓存，重复查询命中；写路径失效后查询更新
        let mut idx = InvertedIndex::open(&tmp(), 10_000).unwrap();
        idx.add("ft:content:山水", 1);
        idx.add("ft:content:山水", 3);
        idx.flush_segment().unwrap();
        let r1 = idx.search("ft:content:山水").unwrap();
        assert_eq!(r1.len(), 2);
        // 缓存已填充
        assert!(idx.posting_cache.lock().unwrap().contains("ft:content:山水"));
        // 重复查询命中缓存，结果一致
        let r2 = idx.search("ft:content:山水").unwrap();
        assert_eq!(r2, r1);
        // 写路径 → 缓存失效 → 新 docid 可查
        idx.add("ft:content:山水", 7);
        assert!(
            !idx.posting_cache.lock().unwrap().contains("ft:content:山水"),
            "写入后缓存应失效"
        );
        let r3 = idx.search("ft:content:山水").unwrap();
        assert_eq!(r3.len(), 3);
        assert!(r3.contains(7));
    }

    // ---------- 位图索引（design 5.2.4，M7-2） ----------

    #[test]
    fn bitmap_count_and_and_after_adds() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 10_000).unwrap();
        idx.with_bitmap_fields(&["status".to_string(), "city".to_string()])
            .unwrap();
        // status=active: 1,3,5；status=inactive: 2,4；city=beijing: 1,2
        idx.add("status=active", 1);
        idx.add("status=inactive", 2);
        idx.add("status=active", 3);
        idx.add("status=inactive", 4);
        idx.add("status=active", 5);
        idx.add("city=beijing", 1);
        idx.add("city=beijing", 2);
        // COUNT 快速路径
        assert_eq!(idx.bitmap_count("status", "active").unwrap(), 3);
        assert_eq!(idx.bitmap_count("city", "beijing").unwrap(), 2);
        // AND 交集：active AND beijing = {1}
        let and = idx.bitmap_and(&["status=active", "city=beijing"]).unwrap();
        assert_eq!(and.len(), 1);
        assert!(and.contains(1));
        // GROUP BY
        let g = idx.bitmap_group_by("status").unwrap();
        assert_eq!(
            g,
            vec![("active".to_string(), 3), ("inactive".to_string(), 2)]
        );
    }

    #[test]
    fn bitmap_off_by_default_returns_none() {
        let idx = InvertedIndex::open(&tmp(), 10_000).unwrap();
        idx.add("status=active", 1);
        assert!(idx.bitmap_count("status", "active").is_none(), "默认关闭");
        assert!(idx.bitmap_and(&["status=active"]).is_none());
        assert!(idx.bitmap_group_by("status").is_none());
    }

    #[test]
    fn bitmap_rebuilds_from_segments_on_reopen() {
        let dir = tmp();
        let fields = vec!["status".to_string()];
        {
            let mut idx = InvertedIndex::open(&dir, 1).unwrap();
            idx.with_bitmap_fields(&fields).unwrap();
            idx.add("status=active", 1);
            idx.flush_segment().unwrap(); // 落盘段
            idx.add("status=inactive", 2);
            idx.flush_segment().unwrap(); // 两条均落盘（重开才能重建）
        }
        // 重开：位图从段全量重建
        let mut idx = InvertedIndex::open(&dir, 1).unwrap();
        idx.with_bitmap_fields(&fields).unwrap();
        assert_eq!(idx.bitmap_count("status", "active").unwrap(), 1);
        assert_eq!(idx.bitmap_count("status", "inactive").unwrap(), 1);
    }

    #[test]
    fn flush_and_search_across_segments() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap(); // 阈值 1，立即刷盘
        idx.add("a", 1);
        idx.flush_segment().unwrap();
        idx.add("a", 2);
        idx.add("b", 7);
        idx.flush_segment().unwrap();
        assert_eq!(idx.segment_count(), 2);

        // 跨段合并
        let r = idx.search("a").unwrap();
        assert!(r.contains(1) && r.contains(2));
        assert_eq!(r.len(), 2);
        assert!(idx.search("b").unwrap().contains(7));
    }

    #[test]
    fn restart_loads_manifest_and_segments() {
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open(&dir, 1).unwrap();
            idx.add("term-x", 10);
            idx.flush_segment().unwrap();
            idx.add("term-x", 20);
            idx.add("term-y", 30);
            idx.flush_segment().unwrap();
        }
        // 重启：Manifest 恢复段列表
        let idx2 = InvertedIndex::open(&dir, 1).unwrap();
        assert_eq!(idx2.segment_count(), 2);
        let r = idx2.search("term-x").unwrap();
        assert!(r.contains(10) && r.contains(20));
        assert!(idx2.search("term-y").unwrap().contains(30));
    }

    #[test]
    fn manifest_is_atomic_and_persists_next_id() {
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open(&dir, 1).unwrap();
            idx.add("k", 1);
            idx.flush_segment().unwrap();
            assert_eq!(idx.next_seg_id.load(Ordering::Relaxed), 2);
        }
        let text = std::fs::read_to_string(dir.join(MANIFEST_FILE)).unwrap();
        assert!(text.contains("inverted-00000001.seg"));
        let m: SegmentManifest = serde_json::from_str(&text).unwrap();
        assert_eq!(m.next_seg_id, 2);
    }

    #[test]
    fn needs_flush_obeys_threshold() {
        let idx = InvertedIndex::open(&tmp(), 3).unwrap();
        assert!(!idx.needs_flush());
        idx.add("t", 1);
        idx.add("t", 2);
        assert!(!idx.needs_flush());
        idx.add("t", 3);
        assert!(idx.needs_flush());
    }

    #[test]
    fn orphan_segment_not_loaded_after_restart() {
        // 崩溃恢复：GC 中途崩溃可能残留"孤儿段"（不在 Manifest 中）——
        // 启动只按 Manifest 加载，孤儿段不得污染查询结果（development 4.5）
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open(&dir, 1).unwrap();
            idx.add("term-a", 1);
            idx.flush_segment().unwrap();
        }
        // 制造孤儿段：直接写一个 .seg 文件，但不更新 Manifest
        std::fs::write(dir.join("inverted-99999999.seg"), b"orphan-garbage").unwrap();

        let idx2 = InvertedIndex::open(&dir, 1).unwrap();
        assert_eq!(idx2.segment_count(), 1, "只应加载 Manifest 记录的段");
        // 正常段不受影响；孤儿段内容不参与查询
        assert!(idx2.search("term-a").unwrap().contains(1));
        assert!(idx2.search("orphan-garbage").unwrap().is_empty());
    }

    #[test]
    fn doc_count_counts_unique_docs() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap();
        idx.add("status=active", 1);
        idx.add("status=active", 2);
        idx.flush_segment().unwrap(); // 段 1
        idx.add("status=active", 2); // 重复 docid（更新）→ 跨段合并去重
        idx.add("status=pending", 3);
        idx.flush_segment().unwrap(); // 段 2
        assert_eq!(idx.doc_count("status=active").unwrap(), 2, "跨段合并去重");
        assert_eq!(idx.doc_count("status=pending").unwrap(), 1);
        assert_eq!(idx.doc_count("status=absent").unwrap(), 0);
    }

    #[test]
    fn group_by_aggregates_by_field_prefix() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap();
        idx.add("status=active", 1);
        idx.add("status=active", 2);
        idx.add("status=pending", 3);
        idx.flush_segment().unwrap();
        idx.add("status=active", 4); // 内存态
        idx.add("type=order", 1);

        let groups = idx.group_by("status").unwrap();
        let map: std::collections::HashMap<String, u64> = groups.into_iter().collect();
        assert_eq!(map.get("status=active").copied(), Some(3), "内存+段合并");
        assert_eq!(map.get("status=pending").copied(), Some(1));
        assert!(!map.contains_key("type=order"), "只返回指定字段分组");
        assert_eq!(idx.group_by("type").unwrap().len(), 1);
    }

    #[test]
    fn iter_terms_merges_memory_and_segments() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap();
        idx.add("a=1", 1);
        idx.flush_segment().unwrap();
        idx.add("b=2", 2);
        idx.add("a=1", 3);
        let terms = idx.iter_terms().unwrap();
        let map: std::collections::BTreeMap<String, u64> =
            terms.into_iter().map(|(t, b)| (t, b.len())).collect();
        assert_eq!(map.get("a=1").copied(), Some(2), "跨段+内存合并");
        assert_eq!(map.get("b=2").copied(), Some(1));
    }

    #[test]
    fn v4_doc_count_fast_matches_exact_across_flushes() {
        // Ex-9.1b：v4 段计数载荷——多段（docid 不重叠）flush 后 doc_count_fast（求和）
        // == doc_count（精确去重）== 实际总数；重启加载后载荷落盘仍一致。
        let dir = tempfile::tempdir().unwrap();
        let mut idx = InvertedIndex::open(dir.path(), 10_000).unwrap();
        let mut items = Vec::new();
        for d in 1..=100u64 {
            items.push(("status=active", d));
        }
        for d in 1_000..=1_100u64 {
            items.push(("status=active", d));
        }
        idx.add_batch(&items);
        idx.flush_segment().unwrap(); // v4 段 1
        let mut items2 = Vec::new();
        for d in 5_000..=5_200u64 {
            items2.push(("status=active", d));
        }
        idx.add_batch(&items2);
        idx.flush_segment().unwrap(); // v4 段 2
        let expect = 100 + 101 + 201;
        assert_eq!(idx.doc_count("status=active").unwrap(), expect, "精确去重");
        assert_eq!(
            idx.doc_count_fast("status=active").unwrap(),
            Some(expect),
            "fast 载荷求和 == 精确（无跨段重叠）"
        );
        // 重启加载：段 v4 解析 + 载荷求和一致
        drop(idx);
        let idx2 = InvertedIndex::open(dir.path(), 10_000).unwrap();
        assert_eq!(idx2.doc_count("status=active").unwrap(), expect);
        assert_eq!(idx2.doc_count_fast("status=active").unwrap(), Some(expect));
        // search 走 v4 解析也一致（posting 完整可读）
        assert_eq!(idx2.search("status=active").unwrap().len(), expect as u64);
    }

    #[test]
    fn v4_doc_count_fast_overlap_upper_bound_documented() {
        // 语义注：同 docid 同 term 跨段覆盖（update 场景）→ 求和（段间重复）高估，
        // 精确去重 doc_count 仍正确——文档化差异，调用方按场景选择。
        let dir = tempfile::tempdir().unwrap();
        let mut idx = InvertedIndex::open(dir.path(), 10_000).unwrap();
        idx.add_batch(&[("status=active", 7)]);
        idx.flush_segment().unwrap();
        idx.add_batch(&[("status=active", 7)]); // 同 docid 同 term 再写 → 跨段重复
        idx.flush_segment().unwrap();
        assert_eq!(idx.doc_count("status=active").unwrap(), 1, "精确去重 = 1");
        assert_eq!(
            idx.doc_count_fast("status=active").unwrap(),
            Some(2),
            "求和 = 2（跨段重叠高估，文档化近似）"
        );
    }

    fn fst_dict_built_on_flush_and_lookup() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap(); // 默认 fst 引擎
        idx.add("status=active", 1);
        idx.add("status=active", 2);
        idx.add("type=order", 3);
        idx.flush_segment().unwrap();
        // .fst 文件已生成，重启后字典加载
        assert!(
            dir.join("inverted-00000001.fst").exists(),
            "应生成 FST 字典文件"
        );
        let idx2 = InvertedIndex::open(&dir, 1).unwrap();
        assert_eq!(idx2.fst_dict_count(), 1, "FST 字典应加载");
        // 走 FST 精确定位路径
        assert!(idx2.search("status=active").unwrap().contains(1));
        assert!(idx2.search("status=active").unwrap().contains(2));
        assert!(!idx2.search("status=active").unwrap().contains(3));
        assert!(idx2.search("absent").unwrap().is_empty());
        // 聚合走 FST 段数据
        assert_eq!(idx2.doc_count("type=order").unwrap(), 1);
    }

    #[test]
    fn fst_and_hash_engines_return_same_results() {
        let dir_a = tmp();
        let dir_b = tmp();
        let mut fst = InvertedIndex::open_with_engine(&dir_a, 1, "fst").unwrap();
        let mut hash = InvertedIndex::open_with_engine(&dir_b, 1, "hash").unwrap();
        for i in 1..=20u64 {
            fst.add(&format!("f={}", i % 5), i);
            hash.add(&format!("f={}", i % 5), i);
            if i % 7 == 0 {
                fst.flush_segment().unwrap();
                hash.flush_segment().unwrap();
            }
        }
        fst.flush_segment().unwrap();
        hash.flush_segment().unwrap();
        for i in 0..5u64 {
            let term = format!("f={i}");
            assert_eq!(
                fst.doc_count(&term).unwrap(),
                hash.doc_count(&term).unwrap(),
                "引擎结果应一致: {term}"
            );
        }
        assert_eq!(
            fst.fst_dict_count(),
            hash.segment_count(),
            "fst 每段一个字典"
        );
    }

    #[test]
    fn fst_missing_dict_falls_back_to_linear_scan() {
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open(&dir, 1).unwrap();
            idx.add("a=1", 1);
            idx.add("b=2", 2);
            idx.flush_segment().unwrap();
        }
        // 删除 FST 字典（模拟旧段 / 字典损坏），应回退线性扫描
        std::fs::remove_file(dir.join("inverted-00000001.fst")).unwrap();
        let idx = InvertedIndex::open(&dir, 1).unwrap();
        assert_eq!(idx.fst_dict_count(), 0, "字典缺失应回退");
        assert!(idx.search("a=1").unwrap().contains(1));
        assert!(idx.search("b=2").unwrap().contains(2));
        assert_eq!(idx.doc_count("a=1").unwrap(), 1);
    }

    #[test]
    fn hash_engine_writes_no_fst_dict() {
        let dir = tmp();
        let mut idx = InvertedIndex::open_with_engine(&dir, 1, "hash").unwrap();
        idx.add("k=1", 1);
        idx.flush_segment().unwrap();
        assert!(
            !dir.join("inverted-00000001.fst").exists(),
            "hash 引擎不生成 FST"
        );
        let idx2 = InvertedIndex::open(&dir, 1).unwrap();
        assert_eq!(idx2.fst_dict_count(), 0);
        assert!(idx2.search("k=1").unwrap().contains(1));
    }

    // ---- Ex-6.2/6.3 并发读优化（ArcSwap 原子发布）----

    #[test]
    fn arc_swap_snapshot_consistency_after_flush() {
        // Ex-6.2：load_full 旧快照在 flush 发布新快照后仍有效（快照一致性：
        // 旧段文件未删前旧快照仍可查）
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap();
        idx.add("a=1", 1);
        idx.add("b=2", 2);
        idx.flush_segment().unwrap();
        assert_eq!(idx.segment_count(), 1);
        let old_snapshot = idx.segments.load_full(); // 旧快照（段 1）
        // 再 flush 一段 → 发布新快照
        idx.add("c=3", 3);
        idx.flush_segment().unwrap();
        assert_eq!(idx.segment_count(), 2);
        // 旧快照独立不变、新快照已更新
        assert_eq!(old_snapshot.len(), 1, "旧快照发布后不变");
        assert_eq!(idx.segments.load().len(), 2, "新快照已更新");
        // 旧快照中的段仍可查（文件未删）
        assert_eq!(idx.search("a=1").unwrap().len(), 1);
        assert_eq!(idx.search("c=3").unwrap().len(), 1);
        // FST 字典同样快照化（Ex-6.3）：flush 后新段字典可见
        assert!(idx.dicts.load().contains_key("inverted-00000002.seg"));
    }

    #[test]
    fn concurrent_readers_safe_on_shared_index() {
        // Ex-6.2/6.3：&self 读方法（search/segment_count/fst_dict_count）可被多线程
        // 同时调用——ArcSwap 读路径无锁（InvertedIndex: Sync）
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap();
        for i in 0..1000u64 {
            idx.add(&format!("f={}", i % 10), i);
        }
        idx.flush_segment().unwrap();
        let shared = Arc::new(idx);
        let mut hs = Vec::new();
        for t in 0..8u64 {
            let s = Arc::clone(&shared);
            hs.push(std::thread::spawn(move || {
                for i in 0..200u64 {
                    let _ = s.search(&format!("f={}", (i + t) % 10)).unwrap();
                    let _ = s.segment_count();
                    let _ = s.fst_dict_count();
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        // 并发读后状态一致
        assert_eq!(shared.segment_count(), 1);
        assert_eq!(shared.search("f=3").unwrap().len(), 100);
    }

    #[test]
    fn flush_interleaved_with_reads_consistent() {
        // Ex-6.2/6.3：flush 发布新快照与读交替执行，读结果始终一致（旧快照期间
        // 旧段仍可查——发布不破坏进行中的读）
        let dir = tmp();
        let idx = Arc::new(std::sync::Mutex::new(
            InvertedIndex::open(&dir, 1).unwrap(),
        ));
        {
            let mut g = idx.lock().unwrap();
            for i in 0..100u64 {
                g.add(&format!("k={}", i % 10), i);
            }
            g.flush_segment().unwrap();
        }
        let mut hs = Vec::new();
        for t in 0..4u64 {
            let idx = Arc::clone(&idx);
            hs.push(std::thread::spawn(move || {
                for i in 0..60u64 {
                    if i % 4 == 0 {
                        let mut g = idx.lock().unwrap();
                        g.add(&format!("new={}", (i + t) % 10), i + t);
                        let _ = g.flush_segment();
                    } else {
                        let g = idx.lock().unwrap();
                        let _ = g.search(&format!("k={}", (i + t) % 10));
                    }
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        let g = idx.lock().unwrap();
        assert!(g.segment_count() >= 1, "读写交替后段数正常");
        assert_eq!(g.search("k=5").unwrap().len(), 10, "历史段数据保持可查");
    }

    // ---- 预分片 Chunk（design 5.2.1，阶段 2）----

    #[test]
    fn chunks_partition_posting_and_concatenate() {
        let dir = tmp();
        let idx = InvertedIndex::open(&dir, 10_000).unwrap();
        // 散布大量 docid，覆盖全部分片
        for i in 1..=10_000u64 {
            idx.add("status=active", i);
        }
        let shard_count = 4u32;
        let full = idx.search("status=active").unwrap();
        let mut chunks = Vec::new();
        for s in 0..shard_count {
            chunks.push(
                idx.chunk_for_shard("status=active", s, shard_count)
                    .unwrap(),
            );
        }
        // ① 分片 Chunk 互不相交（partition 性质）
        for i in 0..shard_count {
            for j in (i + 1)..shard_count {
                let inter = &chunks[i as usize] & &chunks[j as usize];
                assert!(inter.is_empty(), "分片 Chunk 不得重叠");
            }
        }
        // ② 每个 Chunk 内 docid 确实属于对应分片
        for s in 0..shard_count {
            for d in chunks[s as usize].iter() {
                let vs = (crate::sharding::hash64(d as u64) % shard_count as u64) as u32;
                assert_eq!(vs, s, "Chunk 内 docid 分片归属错误");
            }
        }
        // ③ 按序直拼 = 全集（design 5.2.1：O(1) 合并）
        let merged = InvertedIndex::concatenate_chunks(&chunks);
        assert_eq!(merged, full, "直拼结果必须等于全集");
        assert_eq!(merged.len(), 10_000);
        // ④ 无 docid 落点的分片 → 空 Chunk：单 docid term，其它分片必为空
        idx.add("rare=1", 1);
        let owner_shard = (crate::sharding::hash64(1) % 4) as u32; // docid=1 归属的分片
        let mut empty_count = 0;
        for s in 0..4 {
            let chunk = idx.chunk_for_shard("rare=1", s, 4).unwrap();
            if s == owner_shard {
                assert_eq!(chunk.len(), 1, "归属分片应含该 docid");
            } else {
                assert!(chunk.is_empty(), "非归属分片应为空 Chunk");
                empty_count += 1;
            }
        }
        assert_eq!(empty_count, 3, "其余 3 个分片应为空 Chunk");
    }

    #[test]
    fn chunk_works_across_segments_and_memory() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 3).unwrap();
        idx.add("city=beijing", 1);
        idx.add("city=beijing", 2);
        idx.flush_segment().unwrap();
        idx.add("city=beijing", 3);
        idx.add("city=beijing", 4);
        idx.flush_segment().unwrap();
        idx.add("city=beijing", 5); // 内存态
        let shard_count = 2u32;
        let mut chunks = Vec::new();
        for s in 0..shard_count {
            chunks.push(idx.chunk_for_shard("city=beijing", s, shard_count).unwrap());
        }
        let full = idx.search("city=beijing").unwrap();
        assert_eq!(full.len(), 5);
        assert_eq!(InvertedIndex::concatenate_chunks(&chunks), full);
    }

    // ---- 倒排段 GC（design 5.2.2 + 5.2.4⑤，阶段 2）----

    #[test]
    fn gc_merges_segments_preserving_data() {
        let dir = tmp();
        // 极小 GC 阈值（1 字节）：任意 >1 段即触发
        let mut idx = InvertedIndex::open_with_gc(&dir, 2, "fst", 1).unwrap();
        // 4 段，term 跨段重复
        for seg in 0..4u64 {
            idx.add("status=active", 1 + seg * 2);
            idx.add("status=active", 2 + seg * 2);
            idx.add("status=pending", 100 + seg);
            idx.flush_segment().unwrap();
        }
        assert_eq!(idx.segment_count(), 4);
        assert!(idx.should_gc());
        let before = idx.segment_bytes();

        let report = idx.gc().unwrap();
        assert_eq!(report.merged, 4);
        assert_eq!(idx.segment_count(), 1, "GC 后应合并为 1 段");
        assert!(report.segment_count == 1);
        // 数据保持完整
        let active = idx.search("status=active").unwrap();
        assert_eq!(active.len(), 8, "跨段合并去重后应有 8 个 docid");
        let pending = idx.search("status=pending").unwrap();
        assert_eq!(pending.len(), 4);
        // FST 字典重建（1 段 1 字典）
        assert_eq!(idx.fst_dict_count(), 1);
        // 旧段文件已删除
        for seg in ["inverted-00000001.seg", "inverted-00000002.seg"] {
            assert!(!dir.join(seg).exists(), "旧段 {seg} 应被删除");
        }
        // 新段存在（id=5）
        assert!(dir.join("inverted-00000005.seg").exists());
        assert!(report.freed_bytes >= before.saturating_sub(idx.segment_bytes()));
    }

    #[test]
    fn gc_disabled_when_threshold_zero() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 2).unwrap(); // gc 阈值 0 = 禁用
        for _ in 0..3 {
            idx.add("k=1", 1);
            idx.flush_segment().unwrap();
        }
        assert!(!idx.should_gc());
        let report = idx.gc().unwrap();
        assert_eq!(report.merged, 0, "禁用时 GC 应为空操作");
        assert_eq!(idx.segment_count(), 3);
    }

    #[test]
    fn gc_then_restart_loads_manifest_correctly() {
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open_with_gc(&dir, 2, "fst", 1).unwrap();
            idx.add("a=1", 1);
            idx.add("a=1", 2);
            idx.flush_segment().unwrap();
            idx.add("a=1", 3);
            idx.add("b=2", 9);
            idx.flush_segment().unwrap();
            idx.gc().unwrap();
        }
        // 重启：只加载 Manifest 中的新段
        let idx2 = InvertedIndex::open(&dir, 2).unwrap();
        assert_eq!(idx2.segment_count(), 1);
        let a = idx2.search("a=1").unwrap();
        assert!(a.contains(1) && a.contains(2) && a.contains(3));
        assert!(idx2.search("b=2").unwrap().contains(9));
        // Manifest 不含孤儿
        let text = std::fs::read_to_string(dir.join(MANIFEST_FILE)).unwrap();
        assert!(
            !text.contains("inverted-00000001.seg"),
            "旧段不得出现在 Manifest"
        );
        assert!(text.contains("inverted-00000003.seg"));
    }

    #[test]
    fn gc_not_triggered_below_threshold() {
        let dir = tmp();
        // 阈值极大（1TB）：段再多也不 GC
        let mut idx =
            InvertedIndex::open_with_gc(&dir, 2, "fst", 1024 * 1024 * 1024 * 1024).unwrap();
        for _ in 0..3 {
            idx.add("k=1", 1);
            idx.flush_segment().unwrap();
        }
        assert!(!idx.should_gc(), "低于阈值不应触发 GC");
        let report = idx.gc().unwrap();
        assert_eq!(report.merged, 0);
        assert_eq!(idx.segment_count(), 3);
    }

    // ---------- J 项（7.73）：后台 GC 线程化 —— flush/gc 并发安全（mutate 锁） ----------

    #[test]
    fn concurrent_flush_and_gc_no_lost_segment() {
        // 后台 GC 线程化后写路径 flush 与后台 gc **并发**：mutate 锁保证 Manifest 无丢失更新
        // （demo inverted-gc-bg 确定性复现：无锁时 Manifest 引用已删段 → 数据丢失）。
        // 写路径为主库真实形态：单写者 flush（Engine 写锁内）+ 后台 gc 线程并发。
        let dir = tmp();
        // gc 阈值极小（2 段即合并）；flush_threshold=1（add 即达阈值，写线程持续刷盘）
        let idx = Arc::new(InvertedIndex::open_with_gc(&dir, 1, "hash", 1).unwrap());
        // 写线程×1（单写者形态）：add + flush 循环（模拟写路径持续刷盘）
        let writer = {
            let idx = Arc::clone(&idx);
            std::thread::spawn(move || {
                for batch in 0..60u64 {
                    for k in 0..5u64 {
                        idx.add(&format!("w-b{batch}-k{k}"), batch * 5 + k);
                    }
                    let _ = idx.flush_segment(); // 刷盘（写路径）
                }
            })
        };
        // gc 线程×1：周期执行（后台 GC 语义，与写路径并发）
        let gc_thread = {
            let idx = Arc::clone(&idx);
            std::thread::spawn(move || {
                for _ in 0..120 {
                    let _ = idx.gc();
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            })
        };
        writer.join().unwrap();
        gc_thread.join().unwrap();
        // 完整性：Manifest 引用的段文件全部存在（mutate 锁 → 无丢失更新）
        let text = std::fs::read_to_string(dir.join(MANIFEST_FILE)).unwrap();
        let m: SegmentManifest = serde_json::from_str(&text).unwrap();
        assert!(!m.segments.is_empty(), "Manifest 不应为空");
        for seg in &m.segments {
            assert!(dir.join(seg).exists(), "Manifest 引用已删段: {seg}");
        }
        // 数据不丢：所有写入 term 均可检索（含被合并进新段的旧段数据）
        for batch in 0..60u64 {
            for k in 0..5u64 {
                let d = batch * 5 + k;
                assert!(
                    idx.search(&format!("w-b{batch}-k{k}"))
                        .unwrap()
                        .contains(d),
                    "w-b{batch}-k{k} 数据丢失（docid {d}）"
                );
            }
        }
    }

    #[test]
    fn gc_self_ref_reentrant_safe() {
        // J 项：gc 改 &self 后——Arc 共享下可直接调用（后台 worker 无锁执行形态），
        // 且 gc 内部 mutate 锁与 flush_segment 互斥不 panic。
        let dir = tmp();
        let idx = Arc::new(InvertedIndex::open_with_gc(&dir, 1, "hash", 1).unwrap());
        idx.add("a=1", 1);
        idx.flush_segment().unwrap();
        idx.add("b=2", 2);
        idx.flush_segment().unwrap();
        assert!(idx.should_gc());
        let report = idx.gc().unwrap();
        assert_eq!(report.merged, 2);
        // 合并后仍可检索
        assert!(idx.search("a=1").unwrap().contains(1));
        assert!(idx.search("b=2").unwrap().contains(2));
    }

    // ---------- K 项（7.74）：v3 posting 分块布局（分页/COUNT 按容器延迟加载） ----------

    #[test]
    fn v3_paged_matches_full_search_across_segments() {
        // 大 posting 分页快速路径与全量 search 窗口一致（多段、hash 引擎线性扫描定位）
        let dir = tmp();
        let idx = InvertedIndex::open_with_gc(&dir, 10, "hash", 0).unwrap();
        for seg in 0..3u64 {
            for i in seg * 2000..seg * 2000 + 2000 {
                idx.add("hot", i);
            }
            idx.flush_segment().unwrap();
        }
        let full = idx.search("hot").unwrap();
        assert_eq!(full.len(), 6000);
        for (off, lim) in [
            (0u64, 10u64),
            (5, 10),
            (1000, 100),
            (5990, 20),
            (0, 100_000),
        ] {
            let (total, ids) = idx.search_paged("hot", off, lim).unwrap();
            assert_eq!(total, 6000, "total 应一致");
            let expect: Vec<u64> = full.iter().skip(off as usize).take(lim as usize).collect();
            assert_eq!(ids, expect, "窗口 ({off},{lim}) 应与全量 search 一致");
        }
        // COUNT 快速路径精确
        assert_eq!(idx.doc_count("hot").unwrap(), 6000);
    }

    #[test]
    fn v3_paged_dedups_docids_across_segments() {
        // 跨段重复 docid（同主键更新未 GC）：窗口去重、doc_count 精确
        let dir = tmp();
        let idx = InvertedIndex::open_with_gc(&dir, 10, "hash", 0).unwrap();
        idx.add("dup", 1);
        idx.add("dup", 2);
        idx.flush_segment().unwrap();
        idx.add("dup", 2); // 跨段重复
        idx.add("dup", 3);
        idx.flush_segment().unwrap();
        assert_eq!(idx.doc_count("dup").unwrap(), 3, "COUNT 必须跨段去重");
        let (_, ids) = idx.search_paged("dup", 0, 100).unwrap();
        assert_eq!(ids, vec![1, 2, 3], "分页窗口必须去重");
        // 深页 offset 基于去重后流
        let (_, ids2) = idx.search_paged("dup", 1, 100).unwrap();
        assert_eq!(ids2, vec![2, 3], "offset 应基于去重后流");
    }

    #[test]
    fn v3_full_decode_equals_compact_roundtrip() {
        // v3 编码 → 全量解码与原始 bitmap 一致（search 全量路径正确性）
        let dir = tmp();
        let idx = InvertedIndex::open_with_gc(&dir, 10, "hash", 0).unwrap();
        for i in (0..100_000u64).step_by(3) {
            idx.add("sparse", i);
        }
        idx.flush_segment().unwrap();
        let bm = idx.search("sparse").unwrap();
        assert_eq!(bm.len(), 33_334);
        // 命中集合抽查（稀疏跨多容器）
        assert!(bm.contains(0) && bm.contains(99_999) && bm.contains(50_001));
        assert!(!bm.contains(1));
    }
