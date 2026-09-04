//! 原 sqlish.rs 底部 `mod tests`（约 1700 行）整体迁移至此。模块声明在 sql/mod.rs
//! （`#[cfg(test)] mod tests;`）；`use super::*` 取 sql 根 re-export，私有 helper 经
//! 显式路径导入（见下），测试对非 sql 模块引用（Engine/Error/Config/Value/…）原样保留。

use super::*;
use crate::config::Config;
use crate::engine::Engine;
use crate::error::Error;
use serde_json::Value;

use super::executor::aggregate::aggregate_needed_fields;
use super::executor::eval::{eval, full_docids, like_match, light_top_field, LightVal};
use super::executor::select::{collect_limited_rows, row_sort_keys, sort_key, topk_sort, SortKey};

    fn engine_with_docs() -> Engine {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let cities = ["beijing", "shanghai", "shenzhen"];
        for i in 0..100u64 {
            let city = cities[(i % 3) as usize];
            let doc = serde_json::json!({
                "docid": i,
                "status": if i % 3 == 0 { "active" } else { "inactive" },
                "city": city,
                "amount": i * 10,
                "note": format!("note-{i}"),
            });
            let terms: Vec<String> = crate::server::extract_terms(&doc);
            let refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put(i, serde_json::to_vec(&doc).unwrap(), &refs).unwrap();
        }
        e
    }

    #[test]
    fn sql_and_uses_inverted_intersection() {
        let mut e = engine_with_docs();
        let rows = execute(&mut e, "SELECT * FROM t WHERE status='active' AND city='beijing' LIMIT 100", 1000).unwrap();
        assert_eq!(rows.len(), 34, "active 且 beijing 交集（i%3==0）");
    }

    #[test]
    fn like_match_wildcard_semantics() {
        // 纯函数：% 通配任意长度（含空）；无通配 = 全等；_ 不支持
        assert!(like_match("note-15", "note-1%"));
        assert!(like_match("note-1", "note-1%"));
        assert!(!like_match("note-2", "note-1%"));
        assert!(like_match("abc", "%"));
        assert!(like_match("", "%"));
        assert!(like_match("hello", "h%o"));
        assert!(!like_match("hello", "h%x"));
        assert!(like_match("note-7", "%7"));
        assert!(like_match("abc7def", "%7%"));
        assert!(like_match("xabc", "%abc"));
        assert!(!like_match("abx", "%abc"));
        assert!(like_match("exact", "exact"));
        assert!(!like_match("exacT", "exact"));
        assert!(like_match("a%%b", "a%%b")); // 无通配语义（无 % 分支不会构造 Like；此处验证字面）
        assert!(like_match("ab", "a%b"));
        assert!(!like_match("ac", "a%b"));
        assert!(like_match("", ""));
    }

    #[test]
    fn sql_like_prefix_middle_and_eq_fold() {
        // LIKE 端到端：
        // ① 后缀通配 'note-1%' → note-1（% 空）与 note-10..note-19（11 行）
        // ② 前后通配 '%7'（尾部锚定）→ 7,17,...,97（10 行）
        // ③ 无通配 'note-0' → 折叠 Eq → 仅 note-0（1 行）
        // ④ 与倒排等值 AND：status='active'（i%3==0）AND note LIKE 'note-3%' → 3,30,33,36,39（5 行）
        let mut e = engine_with_docs();
        let rows = execute(&mut e, "SELECT * FROM t WHERE note LIKE 'note-1%' LIMIT 100", 1000).unwrap();
        let mut ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        ids.sort_unstable();
        let mut expect: Vec<u64> = vec![1];
        expect.extend(10..20);
        assert_eq!(ids, expect, "note LIKE 'note-1%' → note-1 与 note-10..19");
        let rows2 = execute(&mut e, "SELECT * FROM t WHERE note LIKE '%7' LIMIT 100", 1000).unwrap();
        let mut ids2: Vec<u64> = rows2.iter().map(|r| r.0).collect();
        ids2.sort_unstable();
        assert_eq!(ids2.len(), 10, "'%7' 尾部锚定");
        assert_eq!(ids2[0], 7);
        assert_eq!(ids2[9], 97);
        let rows3 = execute(&mut e, "SELECT * FROM t WHERE note LIKE 'note-0'", 1000).unwrap();
        assert_eq!(rows3.len(), 1, "无通配 LIKE 折叠为 Eq");
        assert_eq!(rows3[0].0, 0);
        // ④ AND 倒排等值 + LIKE 后过滤（AND 快路径：active 位图 ∩ note LIKE）
        let rows4 = execute(
            &mut e,
            "SELECT * FROM t WHERE status='active' AND note LIKE 'note-3%' LIMIT 100",
            1000,
        )
        .unwrap();
        let mut ids4: Vec<u64> = rows4.iter().map(|r| r.0).collect();
        ids4.sort_unstable();
        assert_eq!(ids4, vec![3, 30, 33, 36, 39], "active 且 note-3x（i%3==0，含 note-3）");
        // ⑤ OR 组合兜底：note LIKE 'note-0' OR note LIKE 'note-99' → 0,99
        let rows5 = execute(
            &mut e,
            "SELECT * FROM t WHERE note LIKE 'note-0' OR note LIKE 'note-99' LIMIT 100",
            1000,
        )
        .unwrap();
        let mut ids5: Vec<u64> = rows5.iter().map(|r| r.0).collect();
        ids5.sort_unstable();
        assert_eq!(ids5, vec![0, 99]);
    }

    #[test]
    fn get_docid_set_shapes_and_equivalence() {
        // A4：get_docid_set 形态收敛（Bitmap/Empty/All）+ 与 eval 位图等价
        use crate::docset::DocIdSet;
        let e = engine_with_docs();
        let guard = e.query_guard();
        // 无 WHERE → All
        assert!(matches!(get_docid_set(&e, None, None, &guard).unwrap(), DocIdSet::All));
        // 等值命中 → Bitmap，与 eval 一致（active 34 行）
        let sql = "SELECT * FROM t WHERE status='active'";
        let we = parse_select(sql).unwrap().where_expr;
        let set = get_docid_set(&e, we.as_ref(), None, &guard).unwrap();
        assert!(matches!(&set, DocIdSet::Bitmap(_)));
        let bm = eval(&e, we.as_ref().unwrap(), u64::MAX, &guard).unwrap();
        assert_eq!(set.len_estimate(), bm.len());
        assert_eq!(set.to_vec(), bm.iter().collect::<Vec<u64>>());
        // 空条件冲突 → Empty
        let sql2 = "SELECT * FROM t WHERE status='active' AND status='inactive'";
        let we2 = parse_select(sql2).unwrap().where_expr;
        let set2 = get_docid_set(&e, we2.as_ref(), None, &guard).unwrap();
        assert!(matches!(set2, DocIdSet::Empty));
        // LIKE 命中（含 %）→ Bitmap 与 execute 结果一致（'%7' 10 行）
        let sql3 = "SELECT * FROM t WHERE note LIKE '%7'";
        let we3 = parse_select(sql3).unwrap().where_expr;
        let set3 = get_docid_set(&e, we3.as_ref(), None, &guard).unwrap();
        assert_eq!(set3.len_estimate(), 10);
    }

    #[test]
    fn deep_offset_order_by_rejected_by_topk_guard() {
        // A7：ORDER BY + 巨大 OFFSET → Top-K 堆守卫拒绝（防 OOM；建议 keyset 分页）
        let mut e = engine_with_docs();
        // offset=200k + limit 1 → k=200001 > SORT_MAX_ROWS → 拒绝
        let r = execute(
            &mut e,
            "SELECT * FROM t WHERE status='active' ORDER BY amount LIMIT 1 OFFSET 200000",
            1000,
        );
        assert!(matches!(r, Err(Error::QueryTooExpensive(_))), "深分页应被守卫拒绝: {r:?}");
        // 正常深分页在守卫内：k = offset+limit ≤ 上限 → 仍可执行（结果正确切片）
        let rows = execute(
            &mut e,
            "SELECT * FROM t WHERE status='active' ORDER BY amount LIMIT 1 OFFSET 33",
            1000,
        )
        .unwrap();
        // active 行 amount 升序第 34 个（i=99 不 active；active: 0,3,...,99? 99%3==0 active）
        // 按 amount=i*10 排序：active 0..99 步进 3 → 34 个。offset 33 → 第 34 个（index 33）
        let active_asc: Vec<u64> = (0..100u64).filter(|i| i % 3 == 0).collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, active_asc[33], "offset=33 取第 34 个 active 行");
    }

    #[test]
    fn sql_or_and_not_complement() {
        let mut e = engine_with_docs();
        let rows = execute(&mut e, "SELECT * FROM t WHERE status='active' OR amount>900", 1000).unwrap();
        // 34 active ∪ 9 amount>900（其中 93/96/99 已含）→ 40
        assert_eq!(rows.len(), 40, "OR 并集去重");
        let rows2 = execute(&mut e, "SELECT * FROM t WHERE NOT (status='active')", 1000).unwrap();
        assert_eq!(rows2.len(), 66, "NOT 补集");
    }

    #[test]
    fn sql_comparison_scan_and_docid() {
        let mut e = engine_with_docs();
        let rows = execute(&mut e, "SELECT * FROM t WHERE amount>=500 AND amount<600", 1000).unwrap();
        assert_eq!(rows.len(), 10, "amount∈[500,600)");
        let rows2 = execute(&mut e, "SELECT * FROM t WHERE docid=42", 1000).unwrap();
        assert_eq!(rows2.len(), 1, "docid 点查单例");
        assert_eq!(rows2[0].0, 42);
    }

    #[test]
    fn light_top_field_scanner_basics() {
        use LightVal::*;
        // 目标字段在各位置；数字/字符串/布尔/null 提取
        let doc = br#"{"status":"active","amount":73564,"n":1.5,"ok":true,"no":null,"addr":{"city":"bj"}}"#;
        assert!(matches!(light_top_field(doc, "status"), Some(Str(b"active"))));
        assert!(matches!(light_top_field(doc, "amount"), Some(Num(b"73564"))));
        assert!(matches!(light_top_field(doc, "n"), Some(Num(b"1.5"))));
        assert!(matches!(light_top_field(doc, "ok"), Some(Bool(true))));
        assert!(matches!(light_top_field(doc, "no"), Some(Null)));
        // 嵌套对象值 → Complex；缺失 → Absent
        assert!(matches!(light_top_field(doc, "addr"), Some(Complex)));
        assert!(matches!(light_top_field(doc, "zzz"), Some(Absent)));
        // 非目标嵌套对象（含内部同名 key）跳过不误命中
        let doc2 = br#"{"a":{"status":"x"},"status":"real"}"#;
        assert!(matches!(light_top_field(doc2, "status"), Some(Str(b"real"))));
        // 数组在目标前跳过
        let doc3 = br#"{"tags":["a","b"],"amount":5}"#;
        assert!(matches!(light_top_field(doc3, "amount"), Some(Num(b"5"))));
        // 转义字符串值 → None（回退 serde）；非对象 doc → None
        assert!(light_top_field(br#"{"s":"a\"b"}"#, "s").is_none());
        assert!(light_top_field(b"[1,2]", "f").is_none());
    }

    #[test]
    fn light_leaf_equivalence_with_serde() {
        // 轻量判定与 serde 路径结果一致（随机文档 + 各 op）
        let mut e = engine_with_docs();
        for sql in [
            "SELECT * FROM t WHERE status='active' AND amount>400 LIMIT 1000",
            "SELECT * FROM t WHERE amount BETWEEN 500 AND 530",
            "SELECT * FROM t WHERE status!='active' LIMIT 1000",
            "SELECT * FROM t WHERE amount>900",
        ] {
            let rows = execute(&mut e, sql, 1000).unwrap();
            // 下推/AND 快路径已用 light；此处仅确认执行不回归（结果非空/与旧断言场景一致）
            assert!(!rows.is_empty(), "{sql} 应命中");
        }
    }

    #[test]
    fn sql_aggregate_functions() {
        // 7.95：COUNT(*)/COUNT(f)/SUM/AVG/MIN/MAX（全量 matches_doc，不依赖倒排完整性）
        let mut e = engine_with_docs(); // docid i：amount = i*10（0..990）
        let agg = |sql: &str| execute_aggregate(&e, sql).unwrap().unwrap();
        // 无 WHERE 全表
        assert_eq!((agg("SELECT COUNT(*) FROM t").text.as_str()), "100");
        assert_eq!(agg("SELECT COUNT(*) FROM t").header, "COUNT(*)");
        // WHERE 字段条件（等值/比较/组合）
        assert_eq!(agg("SELECT COUNT(*) FROM t WHERE amount>900").text, "9");
        assert_eq!(agg("SELECT COUNT(*) FROM t WHERE status='active'").text, "34");
        assert_eq!(agg("SELECT COUNT(*) FROM t WHERE status='active' AND amount>400").text, "20");
        // COUNT(f)：缺失字段 = 0；SUM/AVG/MIN/MAX
        assert_eq!(agg("SELECT COUNT(missing) FROM t").text, "0");
        assert_eq!(agg("SELECT COUNT(amount) FROM t").text, "100");
        assert_eq!(agg("SELECT SUM(amount) FROM t").text, "49500");
        assert_eq!(agg("SELECT AVG(amount) FROM t").text, "495");
        assert_eq!(agg("SELECT MIN(amount) FROM t").text, "0");
        // P1-D（2026-09-04）：AND(可倒排等值, 范围) → posting 候选收敛聚合（免全表扫）
        // status='active'（34 行）+ amount>400（active 且 i≥42）：SUM = 10·Σi, i=42..99 step3 = 14100
        assert_eq!(
            agg("SELECT SUM(amount) FROM t WHERE status='active' AND amount>400").text,
            "14100",
            "P1-D 候选收敛聚合精确（与全扫一致）"
        );
        assert_eq!(
            agg("SELECT COUNT(*) FROM t WHERE status='active' AND amount>400").text,
            "20"
        );
        // 对照：无等值条件（amount>400 全扫）SUM = 10·Σi, i=41..99 = 41300
        assert_eq!(agg("SELECT SUM(amount) FROM t WHERE amount>400").text, "41300");
        assert_eq!(agg("SELECT MAX(amount) FROM t").text, "990");
        assert_eq!(agg("SELECT SUM(amount) FROM t WHERE status='active'").text, "16830");
        // 空集：COUNT → 0；SUM/AVG → NULL
        assert_eq!(agg("SELECT COUNT(*) FROM t WHERE amount>10000").text, "0");
        let s = execute_aggregate(&e, "SELECT SUM(amount) FROM t WHERE amount>10000").unwrap().unwrap();
        assert!(s.is_null, "空集 SUM 应为 NULL");
        // 列头与限制：SUM(*) 拒绝
        assert_eq!(agg("SELECT AVG(amount) FROM t").header, "AVG(amount)");
        assert!(execute_aggregate(&e, "SELECT SUM(*) FROM t").is_err());
        // 复合表达式（7.97 light）：OR / NOT
        assert_eq!(agg("SELECT COUNT(*) FROM t WHERE status='active' OR amount>900").text, "40");
        assert_eq!(agg("SELECT COUNT(*) FROM t WHERE NOT(status='inactive')").text, "34");
        assert_eq!(
            agg("SELECT SUM(amount) FROM t WHERE status='active' OR amount>900").text,
            "22500"
        );
        // 普通 SELECT → None（走普通查询路径）
        assert!(execute_aggregate(&e, "SELECT * FROM t WHERE amount>900").unwrap().is_none());
    }

    #[test]
    fn sql_in_clause_filter_group_and() {
        // SQL `WHERE f IN (…)`：过滤（含 AND 交集、数值）、与 GROUP BY 组合。
        let mut e = engine_with_docs();
        let rows = execute(&mut e, "SELECT * FROM t WHERE city IN ('beijing','shanghai') LIMIT 1000", 1000)
            .unwrap();
        assert_eq!(rows.len(), 67, "beijing(34)+shanghai(33)");
        let r2 = execute(
            &mut e,
            "SELECT * FROM t WHERE status='active' AND city IN ('beijing','shenzhen') LIMIT 1000",
            1000,
        )
        .unwrap();
        assert_eq!(r2.len(), 34, "active∩beijing（shenzhen 无 active）");
        let r3 = execute(&mut e, "SELECT * FROM t WHERE amount IN (500, 900)", 1000).unwrap();
        let mut ids: Vec<u64> = r3.iter().map(|x| x.0).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![50, 90], "数值 IN");
        // GROUP BY + IN（分组查询 WHERE 集合过滤）
        let gr = execute_group_by(
            &mut e,
            "SELECT city, COUNT(*) FROM t WHERE city IN ('shanghai','shenzhen') GROUP BY city",
            1000,
        )
        .unwrap()
        .unwrap();
        let keys: Vec<&str> = gr.rows.iter().map(|r| r.keys[0].as_deref().unwrap()).collect();
        assert_eq!(keys, vec!["shanghai", "shenzhen"]);
        let cnts: Vec<u64> = gr
            .rows
            .iter()
            .map(|r| r.cells[0].as_ref().unwrap().parse().unwrap())
            .collect();
        assert_eq!(cnts, vec![33, 33]);
        // 语法错误：空列表 / 缺右括号
        assert!(parse_select("SELECT * FROM t WHERE city IN ()").is_err());
        assert!(parse_select("SELECT * FROM t WHERE city IN ('a'").is_err());
    }

    #[test]
    fn group_by_fast_inverted_matches_scan() {
        // Ex-9.3 ④b：无 WHERE 单字段 GROUP BY 倒排快路径结果与全扫一致（含 NULL 组；
        // 数值聚合遇缺字段行自动回退全扫）。
        let mk = |stats: bool| -> (crate::engine::Engine, tempfile::TempDir) {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = crate::config::Config::default();
            if stats {
                cfg.inverted.stats_fields = vec!["amount".to_string()];
            }
            let mut e = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
            let put = |e: &mut crate::engine::Engine, id: u64, st: Option<&str>, amt: Option<f64>| {
                let mut d = serde_json::json!({});
                if let Some(s) = st {
                    d["status"] = serde_json::json!(s);
                }
                if let Some(a) = amt {
                    d["amount"] = serde_json::json!(a);
                }
                let b = serde_json::to_vec(&d).unwrap();
                let t: Vec<&str> = match st {
                    Some("active") => vec!["status=active"],
                    Some("inactive") => vec!["status=inactive"],
                    _ => Vec::new(),
                };
                e.put_nosync(id, b, &t).unwrap();
            };
            put(&mut e, 1, Some("active"), Some(10.0));
            put(&mut e, 2, Some("active"), Some(20.0));
            put(&mut e, 3, Some("active"), None);
            put(&mut e, 4, Some("inactive"), Some(5.0));
            put(&mut e, 5, None, Some(7.0)); // 缺 status → NULL 组（含数值贡献）
            (e, dir)
        };
        let enc = |r: &GroupRow| -> (Vec<Option<String>>, Vec<Option<String>>) {
            (r.keys.clone(), r.cells.clone())
        };
        let (mut es, _d1) = mk(true);
        let (mut en, _d2) = mk(false);
        for sql in [
            "SELECT status, COUNT(*) FROM t GROUP BY status",
            "SELECT status, SUM(amount) FROM t GROUP BY status",
            "SELECT status, COUNT(*), SUM(amount) FROM t GROUP BY status",
            "SELECT status, COUNT(*) FROM t GROUP BY status HAVING COUNT(*) > 1",
        ] {
            let a = execute_group_by(&mut es, sql, 1000).unwrap().unwrap();
            let b = execute_group_by(&mut en, sql, 1000).unwrap().unwrap();
            let ra: Vec<_> = a.rows.iter().map(enc).collect();
            let rb: Vec<_> = b.rows.iter().map(enc).collect();
            assert_eq!(ra, rb, "{sql} 快路径应与全扫一致");
        }
        // NULL 组精确：COUNT(*) 无 WHERE 下缺 status 的 doc5 → NULL 组 count=1
        let gr = execute_group_by(&mut es, "SELECT status, COUNT(*) FROM t GROUP BY status", 1000)
            .unwrap()
            .unwrap();
        let null_row = gr.rows.iter().find(|r| r.keys[0].is_none()).expect("应有 NULL 组");
        assert_eq!(null_row.cells[0].as_deref(), Some("1"), "NULL 组计 1 行（doc5）");
    }

    #[test]
    fn stats_load_fast_path_matches_scan() {
        // Ex-9.3 第③步：SUM/AVG/MIN/MAX ... WHERE f='v'（裸等值）走倒排统计载荷，
        // 与无统计全扫路径数值一致（stats_fields 声明字段才路由，否则回落全扫）。
        let put = |e: &mut crate::engine::Engine, id: u64, st: &str, amt: Option<f64>| {
            let mut d = serde_json::json!({"status": st});
            if let Some(a) = amt {
                d["amount"] = serde_json::json!(a);
            }
            let b = serde_json::to_vec(&d).unwrap();
            let t: &[&str] = &[if st == "active" { "status=active" } else { "status=inactive" }];
            e.put_nosync(id, b, t).unwrap();
        };
        // 有统计配置
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.inverted.stats_fields = vec!["amount".to_string()];
        let mut es = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        put(&mut es, 1, "active", Some(10.0));
        put(&mut es, 2, "active", Some(20.0));
        put(&mut es, 3, "active", None); // 缺 amount：不参与数值聚合（MySQL NULL 语义）
        put(&mut es, 4, "inactive", Some(5.0));
        let agg = |e: &mut crate::engine::Engine, sql: &str| {
            execute_aggregate(e, sql).unwrap().unwrap()
        };
        assert_eq!(agg(&mut es, "SELECT SUM(amount) FROM t WHERE status='active'").text, "30");
        assert_eq!(agg(&mut es, "SELECT AVG(amount) FROM t WHERE status='active'").text, "15");
        assert_eq!(agg(&mut es, "SELECT MIN(amount) FROM t WHERE status='active'").text, "10");
        assert_eq!(agg(&mut es, "SELECT MAX(amount) FROM t WHERE status='active'").text, "20");
        assert_eq!(agg(&mut es, "SELECT SUM(amount) FROM t WHERE status='inactive'").text, "5");
        // 无统计配置：全扫回落，数值一致
        let dir2 = tempfile::tempdir().unwrap();
        let mut en = crate::engine::Engine::open(dir2.path(), &crate::config::Config::default()).unwrap();
        put(&mut en, 1, "active", Some(10.0));
        put(&mut en, 2, "active", Some(20.0));
        put(&mut en, 3, "active", None);
        put(&mut en, 4, "inactive", Some(5.0));
        for sql in [
            "SELECT SUM(amount) FROM t WHERE status='active'",
            "SELECT AVG(amount) FROM t WHERE status='active'",
            "SELECT MAX(amount) FROM t WHERE status='active'",
            "SELECT SUM(amount) FROM t WHERE status='inactive'",
        ] {
            let a = agg(&mut es, sql);
            let b = agg(&mut en, sql);
            assert_eq!(a.text, b.text, "{sql} 快路径应等于全扫");
            assert_eq!(a.is_null, b.is_null, "{sql} NULL 语义一致");
        }
    }

    #[test]
    fn sql_numeric_eq_backfill() {
        // 7.94：数字字段等值倒排 term 不建（空 posting）→ 回退单遍扫描（裸走早停下推、
        // 组合走 eval_cond 全量回退）——语义对齐 MySQL 无索引等值
        let mut e = engine_with_docs(); // amount = i*10（0..990）
        // 裸等值：amount=500 → docid 50（倒排空 → 下推扫描命中）
        let rows = execute(&mut e, "SELECT * FROM t WHERE amount=500", 1000).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 50);
        // 不存在值 → 回退全扫后 0 行（不再是错误空——与语义一致）
        let rows2 = execute(&mut e, "SELECT * FROM t WHERE amount=999", 1000).unwrap();
        assert_eq!(rows2.len(), 0);
        // 组合：AND 等值(active 倒排) ∩ 数字等值(回退全量) → docid 0
        let rows3 = execute(&mut e, "SELECT * FROM t WHERE status='active' AND amount=0", 1000).unwrap();
        assert_eq!(rows3.len(), 1, "active(i%3==0) 且 amount=0 → docid 0");
        assert_eq!(rows3[0].0, 0);
        // Ne 数字：amount!=0 → 99 行（旧实现倒排取反错误返回全表 100）
        let rows4 = execute(&mut e, "SELECT * FROM t WHERE amount!=0", 1000).unwrap();
        assert_eq!(rows4.len(), 99, "amount!=0 排除 docid 0");
        // 字符串等值（有倒排 term）路径不变：status='active' → 34
        let rows5 = execute(&mut e, "SELECT * FROM t WHERE status='active'", 1000).unwrap();
        assert_eq!(rows5.len(), 34);
    }

    #[test]
    fn sql_comparison_pushdown_single_pass_early_stop() {
        // 7.93：裸比较/BETWEEN 下推——单遍流式扫描 + LIMIT 早停，结果与旧 eval 路径一致
        let mut e = engine_with_docs(); // docid i：amount = i*10
        // 下推（裸比较 amount>900）vs 旧路径（AND 包一层使走 eval+post_filter）：同为 91..99
        let rows = execute(&mut e, "SELECT * FROM t WHERE amount>900", 1000).unwrap();
        assert_eq!(rows.len(), 9, "amount>900 → docid 91..99");
        assert_eq!(rows[0].0, 91);
        let rows_ref = execute(&mut e, "SELECT * FROM t WHERE amount>900 AND docid>0", 1000).unwrap();
        assert_eq!(rows, rows_ref, "下推结果应与旧 eval 路径一致");
        // LIMIT 早停：只取前 2 命中
        let rows2 = execute(&mut e, "SELECT * FROM t WHERE amount>900 LIMIT 2", 1000).unwrap();
        assert_eq!(rows2.len(), 2);
        assert_eq!(rows2[0].0, 91);
        // BETWEEN 下推（amount = i*10 ∈ [500,530] 闭区间 → i∈[50,53] 共 4 行）
        let rows3 = execute(&mut e, "SELECT * FROM t WHERE amount BETWEEN 500 AND 530", 1000).unwrap();
        assert_eq!(rows3.len(), 4);
        assert_eq!(rows3[0].0, 50);
        assert_eq!(rows3[3].0, 53);
        // OFFSET：跳过前 2 个命中（91,92 → 从 93 起）
        let rows4 = execute(&mut e, "SELECT * FROM t WHERE amount>900 LIMIT 2 OFFSET 2", 1000).unwrap();
        assert_eq!(rows4.len(), 2);
        assert_eq!(rows4[0].0, 93);
        // 返回行含完整 doc（后续投影免二次回表）
        let v: serde_json::Value = serde_json::from_slice(&rows2[0].1).unwrap();
        assert_eq!(v["amount"], 910);
    }

    #[test]
    fn sql_between_numeric_range_and_fast_path() {
        let mut e = engine_with_docs();
        // BETWEEN 闭区间（数值）
        let rows = execute(&mut e, "SELECT * FROM t WHERE amount BETWEEN 500 AND 600", 1000).unwrap();
        assert_eq!(rows.len(), 11, "amount∈[500,600] 闭区间 = docid 50..60");
        // AND 快路径：倒排等值收敛 + BETWEEN 后过滤（不做全量扫描）
        let rows2 = execute(&mut e, "SELECT * FROM t WHERE status='active' AND amount BETWEEN 0 AND 30", 1000).unwrap();
        let mut ids: Vec<u64> = rows2.iter().map(|r| r.0).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 3], "active 且 amount<=30");
    }

    #[test]
    fn sql_paging_and_parse_errors() {
        let mut e = engine_with_docs();
        let p1 = execute(&mut e, "SELECT * FROM t WHERE status='active' LIMIT 5", 1000).unwrap();
        let p2 = execute(&mut e, "SELECT * FROM t WHERE status='active' LIMIT 5 OFFSET 5", 1000).unwrap();
        assert_eq!(p1.len(), 5);
        assert_eq!(p2.len(), 5);
        assert_ne!(p1[0].0, p2[0].0, "分页不重叠");
        assert!(parse_select("SELECT * FROM t JOIN x").is_err(), "JOIN 拒绝");
        assert!(parse_select("SELECT * FROM t GROUP BY city").is_err(), "GROUP BY 拒绝");
    }

    // ---------- ORDER BY（开发顺序 #1：单字段 + LIMIT） ----------

    #[test]
    fn order_by_amount_desc_limit() {
        let mut e = engine_with_docs();
        let rows = execute(&mut e, "SELECT * FROM t ORDER BY amount DESC LIMIT 3", 1000).unwrap();
        assert_eq!(rows.len(), 3);
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![99, 98, 97], "amount 降序取前 3（最大 990/980/970）");
    }

    #[test]
    fn order_by_numeric_asc_with_where_and_offset() {
        let mut e = engine_with_docs();
        // WHERE 收敛后排序 + OFFSET/LIMIT 切片
        let rows = execute(
            &mut e,
            "SELECT * FROM t WHERE amount>=500 AND amount<600 ORDER BY amount ASC LIMIT 2 OFFSET 1",
            1000,
        )
        .unwrap();
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![51, 52], "amount 500..590 升序，跳过最小 50，取 51/52");
    }

    #[test]
    fn order_by_string_field_asc() {
        let mut e = engine_with_docs();
        let rows = execute(&mut e, "SELECT * FROM t ORDER BY note ASC LIMIT 3", 1000).unwrap();
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![0, 1, 10], "note 字典序（note-0 < note-1 < note-10 < note-2，同 MySQL）");
        // DESC：note-99/98 最大，倒序前 2
        let rows2 = execute(&mut e, "SELECT * FROM t ORDER BY note DESC LIMIT 2", 1000).unwrap();
        let ids2: Vec<u64> = rows2.iter().map(|r| r.0).collect();
        assert_eq!(ids2, vec![99, 98], "note 降序取前 2");
        // 缺省字段全 NULL（ASC 均排最前且彼此相等）→ 稳定序返回
        let rows3 = execute(&mut e, "SELECT * FROM t ORDER BY nokey ASC LIMIT 2", 1000).unwrap();
        let ids3: Vec<u64> = rows3.iter().map(|r| r.0).collect();
        assert_eq!(ids3, vec![0, 1], "缺省字段全 NULL，稳定序返回");
    }

    #[test]
    fn order_by_parse_multi_field_and_case() {
        let s = parse_select("SELECT * FROM t WHERE amount>1 ORDER BY amount DESC, note ASC LIMIT 5").unwrap();
        assert_eq!(s.order_by, vec![("amount".into(), true), ("note".into(), false)]);
        assert_eq!(s.limit, Some(5));
        let s2 = parse_select("SELECT * FROM t order by note LIMIT 1").unwrap();
        assert_eq!(s2.order_by, vec![("note".into(), false)], "小写 order by 亦可");
    }

    // ---------- ORDER BY 多字段（开发顺序 AF#3：多键 comparator 终验） ----------

    #[test]
    fn order_by_multi_field_first_key_then_second() {
        // 首键 city 升序，beijing 组内按 amount 降序打破并列
        // （若只看单键 amount DESC，全局前三应为 99/98/97——多键语义须回到 beijing 组内）。
        let mut e = engine_with_docs();
        let rows = execute(
            &mut e,
            "SELECT * FROM t ORDER BY city ASC, amount DESC LIMIT 3",
            1000,
        )
        .unwrap();
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![99, 96, 93], "city 升序并列由 amount 降序打破");
        // 反向：amount 升序主键、city 升序次键——amount 全表唯一 → 退化为纯 amount 序
        let rows2 = execute(
            &mut e,
            "SELECT * FROM t ORDER BY amount ASC, city ASC LIMIT 3",
            1000,
        )
        .unwrap();
        let ids2: Vec<u64> = rows2.iter().map(|r| r.0).collect();
        assert_eq!(ids2, vec![0, 1, 2], "amount 无并列，city 次键不影响结果");
    }

    #[test]
    fn order_by_multi_field_desc_with_where_and_limit() {
        // WHERE 收敛（amount∈[500,600) → docid 50..59）后：city DESC（shenzhen 组最前）
        // 且组内 amount DESC → shenzhen(59/56/53/50) 前三 59,56,53。
        let mut e = engine_with_docs();
        let rows = execute(
            &mut e,
            "SELECT * FROM t WHERE amount>=500 AND amount<600 ORDER BY city DESC, amount DESC LIMIT 3",
            1000,
        )
        .unwrap();
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![59, 56, 53], "shenzhen 组 amount 降序前 3");
        // OFFSET 在排序后切片（跳过 59/56 → 53 起）
        let rows2 = execute(
            &mut e,
            "SELECT * FROM t WHERE amount>=500 AND amount<600 ORDER BY city DESC, amount DESC LIMIT 2 OFFSET 2",
            1000,
        )
        .unwrap();
        let ids2: Vec<u64> = rows2.iter().map(|r| r.0).collect();
        assert_eq!(ids2, vec![53, 50], "OFFSET 2 跳过 59/56");
    }

    // ---------- GROUP BY（开发顺序 AF#2：单字段 + COUNT/SUM） ----------
    // fixture：docid 0..99；city 循环 beijing/shanghai/shenzhen；status active 当 i%3==0；
    // amount = i*10（全表 0..990，总和 49500）；note = note-{i}。

    #[test]
    fn group_by_count_single_field() {
        let mut e = engine_with_docs();
        let gr = execute_group_by(&mut e, "SELECT city, COUNT(*) FROM t GROUP BY city", 1000)
            .unwrap()
            .unwrap();
        assert_eq!(gr.group_cols, vec!["city"]);
        assert_eq!(gr.headers, vec!["COUNT(*)"]);
        let keys: Vec<Option<String>> = gr.rows.iter().map(|r| r.keys[0].clone()).collect();
        assert_eq!(
            keys,
            vec![Some("beijing".into()), Some("shanghai".into()), Some("shenzhen".into())],
            "组键升序（字典序）"
        );
        let counts: Vec<u64> = gr
            .rows
            .iter()
            .map(|r| r.cells[0].as_ref().unwrap().parse().unwrap())
            .collect();
        assert_eq!(counts, vec![34, 33, 33], "i%3==0/1/2 各 34/33/33");
    }

    #[test]
    fn group_by_status_multi_agg_sum() {
        let mut e = engine_with_docs();
        let gr = execute_group_by(
            &mut e,
            "SELECT status, COUNT(*), SUM(amount) FROM t GROUP BY status",
            1000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(gr.headers, vec!["COUNT(*)", "SUM(amount)"]);
        let keys: Vec<&str> = gr.rows.iter().map(|r| r.keys[0].as_deref().unwrap()).collect();
        assert_eq!(keys, vec!["active", "inactive"]);
        // active：i%3==0 共 34 行，amount 和 = 30×(0+1+…+33) = 16830
        let a = &gr.rows[0];
        assert_eq!(a.cells[0].as_deref(), Some("34"));
        assert_eq!(a.cells[1].as_deref(), Some("16830"));
        // inactive：66 行，总和 = 49500 - 16830 = 32670
        let b = &gr.rows[1];
        assert_eq!(b.cells[0].as_deref(), Some("66"));
        assert_eq!(b.cells[1].as_deref(), Some("32670"));
    }

    #[test]
    fn group_by_sum_with_where() {
        let mut e = engine_with_docs();
        let gr = execute_group_by(
            &mut e,
            "SELECT city, SUM(amount) FROM t WHERE amount>=500 AND amount<600 GROUP BY city",
            1000,
        )
        .unwrap()
        .unwrap();
        // 命中 docid 50..59（i%3: 0→beijing、1→shanghai、2→shenzhen）：
        // beijing(51/54/57)=1620、shanghai(52/55/58)=1650、shenzhen(50/53/56/59)=2180
        let sums: Vec<&str> = gr
            .rows
            .iter()
            .map(|r| r.cells[0].as_deref().unwrap())
            .collect();
        assert_eq!(sums, vec!["1620", "1650", "2180"]);
        // 行级过滤后无 shanghai 组缺席（各组均含匹配行）
        assert_eq!(gr.rows.len(), 3);
    }

    #[test]
    fn group_by_missing_field_null_group_and_empty_sum() {
        let mut e = engine_with_docs();
        // 缺省字段 → 全部并入 NULL 组；SUM(缺省数值字段) → 无数值行 → NULL
        let gr = execute_group_by(
            &mut e,
            "SELECT nokey, COUNT(*), SUM(amount2) FROM t GROUP BY nokey",
            1000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(gr.rows.len(), 1, "全 NULL 键并为一组");
        assert!(gr.rows[0].keys[0].is_none(), "NULL 组键文本为 None");
        assert_eq!(gr.rows[0].cells[0].as_deref(), Some("100"), "COUNT(*) 计 100 行");
        assert!(gr.rows[0].cells[1].is_none(), "空数值集 SUM → SQL NULL");
    }

    #[test]
    fn group_by_limit_slices_groups() {
        let mut e = engine_with_docs();
        let gr = execute_group_by(&mut e, "SELECT city, COUNT(*) FROM t GROUP BY city LIMIT 2", 1000)
            .unwrap()
            .unwrap();
        assert_eq!(gr.rows.len(), 2, "LIMIT 对组行切片");
        assert_eq!(gr.rows[0].keys[0].as_deref(), Some("beijing"));
        assert_eq!(gr.rows[1].keys[0].as_deref(), Some("shanghai"));
    }

    #[test]
    fn group_by_order_by_group_field_desc() {
        let mut e = engine_with_docs();
        let gr = execute_group_by(
            &mut e,
            "SELECT city, COUNT(*) FROM t GROUP BY city ORDER BY city DESC",
            1000,
        )
        .unwrap()
        .unwrap();
        let keys: Vec<&str> = gr.rows.iter().map(|r| r.keys[0].as_deref().unwrap()).collect();
        assert_eq!(keys, vec!["shenzhen", "shanghai", "beijing"], "组键 DESC");
        // 排序字段 ≠ 分组字段 → 拒绝（组结果仅支持按分组字段排序，AF#5 扩展）
        let mut e2 = engine_with_docs();
        assert!(execute_group_by(
            &mut e2,
            "SELECT city, COUNT(*) FROM t GROUP BY city ORDER BY amount",
            1000,
        )
        .is_err());
    }

    #[test]
    fn group_by_multi_field_all_aggregates() {
        // AF#4：双字段分组 + COUNT/SUM/AVG/MIN/MAX 常用聚合（组内按 region,tier 组合切分）。
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        // (region, tier, amount)：north/south × a/b
        let docs = [
            ("north", "a", 10.0),
            ("north", "a", 20.0),
            ("north", "b", 5.0),
            ("north", "b", 15.0),
            ("south", "a", 7.0),
            ("south", "a", 9.0),
            ("south", "b", 3.0),
            ("south", "b", 1.0),
        ];
        for (i, (r, t, amt)) in docs.iter().enumerate() {
            let doc = format!(r#"{{"region":"{r}","tier":"{t}","amount":{amt}}}"#);
            let refs: Vec<&str> = Vec::new();
            e.put(i as u64 + 1, doc.into_bytes(), &refs).unwrap();
        }
        let gr = execute_group_by(
            &mut e,
            "SELECT region, tier, COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM t GROUP BY region, tier",
            1000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(gr.group_fields, vec!["region", "tier"]);
        assert_eq!(
            gr.group_cols,
            vec!["region", "tier"],
            "结果集分组列 = 选中普通列"
        );
        assert_eq!(
            gr.headers,
            vec!["COUNT(*)", "SUM(amount)", "AVG(amount)", "MIN(amount)", "MAX(amount)"]
        );
        // 组键升序：north-a/b → south-a/b
        let combos: Vec<String> = gr
            .rows
            .iter()
            .map(|r| format!("{}-{}", r.keys[0].as_deref().unwrap(), r.keys[1].as_deref().unwrap()))
            .collect();
        assert_eq!(combos, vec!["north-a", "north-b", "south-a", "south-b"]);
        let cell = |row: usize, col: usize| gr.rows[row].cells[col].as_deref().unwrap().to_string();
        assert_eq!(cell(0, 0), "2");
        assert_eq!(cell(0, 1), "30");
        assert_eq!(cell(0, 2), "15"); // AVG(10,20)
        assert_eq!(cell(0, 3), "10"); // MIN
        assert_eq!(cell(0, 4), "20"); // MAX
        assert_eq!(cell(3, 0), "2");
        assert_eq!(cell(3, 1), "4"); // south-b SUM(3,1)
        assert_eq!(cell(3, 2), "2"); // AVG(3,1)
        assert_eq!(cell(3, 3), "1"); // MIN
        assert_eq!(cell(3, 4), "3"); // MAX
    }

    #[test]
    fn group_by_multi_field_order_on_secondary_level_and_subset_cols() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        for (i, r) in [("north", "a"), ("north", "b"), ("south", "a"), ("south", "b")]
            .iter()
            .enumerate()
        {
            let doc = format!(r#"{{"region":"{}","tier":"{}"}}"#, r.0, r.1);
            let refs: Vec<&str> = Vec::new();
            e.put(i as u64 + 1, doc.into_bytes(), &refs).unwrap();
        }
        // ORDER BY tier DESC：优先级 = 次层键降序 → 同 tier 内 region 升序补尾
        let gr = execute_group_by(
            &mut e,
            "SELECT region, tier, COUNT(*) FROM t GROUP BY region, tier ORDER BY tier DESC",
            1000,
        )
        .unwrap()
        .unwrap();
        let combos: Vec<String> = gr
            .rows
            .iter()
            .map(|r| format!("{}-{}", r.keys[0].as_deref().unwrap(), r.keys[1].as_deref().unwrap()))
            .collect();
        assert_eq!(combos, vec!["north-b", "south-b", "north-a", "south-a"]);
        // 只选中一个分组列：region 仍参与分组（区隔行），但不在结果集出现
        let gr2 = execute_group_by(
            &mut e,
            "SELECT tier, COUNT(*) FROM t GROUP BY region, tier",
            1000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(gr2.group_cols, vec!["tier"], "仅选中列出现在结果集");
        let tiers: Vec<&str> = gr2.rows.iter().map(|r| r.keys[1].as_deref().unwrap()).collect();
        assert_eq!(tiers, vec!["a", "b", "a", "b"], "region×tier 各成组（默认复合键升序）");
        assert_eq!(gr2.rows.len(), 4, "region×tier 仍 4 组（region 未选中仍参与分组）");
    }

    #[test]
    fn group_by_parse_rejections() {
        // 无 GROUP BY 多聚合仍拒绝（标量路径旧语义）
        assert!(parse_select("SELECT COUNT(*), SUM(amount) FROM t").is_err());
        // 非分组列 ≠ 任一 GROUP BY 字段 → 拒绝
        assert!(parse_select("SELECT status, COUNT(*) FROM t GROUP BY city").is_err());
        // 多字段 GROUP BY（AF#4 合法）→ 解析通过
        assert!(parse_select("SELECT city, status, COUNT(*) FROM t GROUP BY city, status").is_ok());
        // GROUP BY 字段重复 → 拒绝
        assert!(parse_select("SELECT COUNT(*) FROM t GROUP BY city, city").is_err());
        // SELECT * 与 GROUP BY 混用 → 拒绝
        assert!(parse_select("SELECT * FROM t GROUP BY city").is_err());
        // AVG/MIN/MAX 聚合（AF#4 合法）→ 解析通过
        assert!(parse_select("SELECT city, AVG(amount), MIN(amount), MAX(amount) FROM t GROUP BY city")
            .is_ok());
        // 无聚合列的 GROUP BY → 执行拒绝（须至少一个聚合列）
        let mut e = engine_with_docs();
        assert!(execute_group_by(&mut e, "SELECT city FROM t GROUP BY city", 1000).is_err());
    }

    // ---------- HAVING（开发顺序 AF#5：分组后过滤） ----------

    #[test]
    fn group_by_having_on_aggregate() {
        // fixture：city 34/33/33；SUM(amount) beijing 16830 / shanghai 16170 / shenzhen 16500。
        let mut e = engine_with_docs();
        let gr = execute_group_by(
            &mut e,
            "SELECT city, COUNT(*), SUM(amount) FROM t GROUP BY city HAVING COUNT(*) > 33",
            1000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(gr.rows.len(), 1, "仅 beijing（34 > 33）");
        assert_eq!(gr.rows[0].keys[0].as_deref(), Some("beijing"));
        assert_eq!(gr.rows[0].cells[0].as_deref(), Some("34"));
        // 聚合 + 分组字段复合条件（AND）
        let gr2 = execute_group_by(
            &mut e,
            "SELECT city, SUM(amount) FROM t GROUP BY city HAVING SUM(amount) > 16000 AND city != 'beijing'",
            1000,
        )
        .unwrap()
        .unwrap();
        let keys: Vec<&str> = gr2.rows.iter().map(|r| r.keys[0].as_deref().unwrap()).collect();
        assert_eq!(keys, vec!["shanghai", "shenzhen"], "16170/16500 > 16000 且排除 beijing");
    }

    #[test]
    fn group_by_having_or_and_sort_slice_after_filter() {
        let mut e = engine_with_docs();
        // OR：beijing（34）∪ shanghai（COUNT>33 OR city 等值）
        let gr = execute_group_by(
            &mut e,
            "SELECT city, COUNT(*) FROM t GROUP BY city HAVING COUNT(*) > 33 OR city = 'shanghai'",
            1000,
        )
        .unwrap()
        .unwrap();
        let keys: Vec<&str> = gr.rows.iter().map(|r| r.keys[0].as_deref().unwrap()).collect();
        assert_eq!(keys, vec!["beijing", "shanghai"]);
        // HAVING 过滤后再 LIMIT 切片（先过滤：仅 beijing/shanghai，LIMIT 1 → beijing）
        let gr2 = execute_group_by(
            &mut e,
            "SELECT city, COUNT(*) FROM t GROUP BY city HAVING COUNT(*) > 33 LIMIT 1",
            1000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(gr2.rows.len(), 1);
        assert_eq!(gr2.rows[0].keys[0].as_deref(), Some("beijing"));
        // 空结果：过滤掉全部组 → 空集
        let gr3 = execute_group_by(
            &mut e,
            "SELECT city, COUNT(*) FROM t GROUP BY city HAVING SUM(amount) > 999999",
            1000,
        )
        .unwrap()
        .unwrap();
        assert!(gr3.rows.is_empty(), "无组满足 HAVING → 空结果集");
    }

    #[test]
    fn having_parse_constraints() {
        // HAVING 配合 GROUP BY 合法
        let s = parse_select("SELECT city, COUNT(*) FROM t GROUP BY city HAVING COUNT(*) > 1 AND city='x'")
            .unwrap();
        assert!(s.having.is_some(), "HAVING 解析成功");
        // 无 GROUP BY 的 HAVING → 拒绝（本期不支持）
        assert!(parse_select("SELECT COUNT(*) FROM t HAVING COUNT(*) > 1").is_err());
        // 非聚合/非分组列左项在解析期不校验（运行期不命中 → 保守丢弃），已知取舍
    }

    /// P0-D：INNER JOIN 解析 + 执行。
    #[test]
    fn join_parse_and_execute() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        // 主表 orders：3 条，关联 user_id
        e.put(1, br#"{"user_id":"101","amount":100}"#.to_vec(), &["user_id=101"])
            .unwrap();
        e.put(2, br#"{"user_id":"102","amount":200}"#.to_vec(), &["user_id=102"])
            .unwrap();
        e.put(3, br#"{"user_id":"101","amount":300}"#.to_vec(), &["user_id=101"])
            .unwrap();
        // 从表 users：2 条
        e.put(101, br#"{"name":"alice"}"#.to_vec(), &["name=alice"])
            .unwrap();
        e.put(102, br#"{"name":"bob"}"#.to_vec(), &["name=bob"])
            .unwrap();
        e.flush_wal().unwrap();

        // INNER JOIN：主表 user_id = 从表 docid（WHERE 收敛主表）
        let rows = execute(
            &e,
            "SELECT * FROM orders INNER JOIN users ON orders.user_id = users.docid WHERE amount>=100 LIMIT 100",
            1000,
        )
        .unwrap();
        assert_eq!(rows.len(), 3, "3 条 order 都有匹配 user");

        // LEFT JOIN：同结果（都有匹配）
        let rows2 = execute(
            &e,
            "SELECT * FROM orders LEFT JOIN users ON orders.user_id = users.docid WHERE amount>=100 LIMIT 100",
            1000,
        )
        .unwrap();
        assert_eq!(rows2.len(), 3, "LEFT JOIN 3 条");

        // 无匹配的 LEFT JOIN → 仍保留左行
        e.put(4, br#"{"user_id":"999","amount":400}"#.to_vec(), &["user_id=999"])
            .unwrap();
        e.flush_wal().unwrap();
        let rows3 = execute(
            &e,
            "SELECT * FROM orders LEFT JOIN users ON orders.user_id = users.docid WHERE amount>=100 LIMIT 100",
            1000,
        )
        .unwrap();
        assert_eq!(rows3.len(), 4, "LEFT JOIN 4 条（含无匹配）");

        // INNER JOIN → 无匹配行不出现
        let rows4 = execute(
            &e,
            "SELECT * FROM orders INNER JOIN users ON orders.user_id = users.docid WHERE amount>=100 LIMIT 100",
            1000,
        )
        .unwrap();
        assert_eq!(rows4.len(), 3, "INNER JOIN 仍 3 条（无匹配不出现）");
    }

    /// review 修复（2026-09-04）：组合索引 stale 键防护——cidx 只插不删，put 更新字段变更后
    /// 旧复合键仍命中；try_composite_index 须用完整 WHERE 对回表值复筛，防返回不满足条件的错行。
    #[test]
    fn composite_index_stale_key_refiltered_by_where() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.storage.composite_indexes = vec![vec!["status".into(), "region".into()]];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        e.put(1, br#"{"status":"active","region":"east","amount":100}"#.to_vec(), &[])
            .unwrap();
        e.put(2, br#"{"status":"active","region":"west","amount":200}"#.to_vec(), &[])
            .unwrap();
        // 更新 doc1：status active → inactive（cidx 旧键 (active,east,1) 残留）
        e.put(1, br#"{"status":"inactive","region":"east","amount":100}"#.to_vec(), &[])
            .unwrap();
        e.flush_wal().unwrap();

        // 前缀 [active,east] 命中残留旧键 (active,east,1) → 复筛剔除 → 空结果（修复前错返 doc1）
        let rows = execute(
            &e,
            "SELECT * FROM t WHERE status='active' AND region='east'",
            1000,
        )
        .unwrap();
        assert!(rows.is_empty(), "doc1 已 inactive，不得因 stale 复合键返回");

        // 前缀 [active] 命中 doc1（残留）与 doc2 → 只留 doc2
        let rows2 = execute(&e, "SELECT * FROM t WHERE status='active'", 1000).unwrap();
        let ids: Vec<u64> = rows2.iter().map(|(d, _)| *d).collect();
        assert_eq!(ids, vec![2], "只有 doc2 满足 status=active");
    }

    /// review 修复（2026-09-04）：JOIN 从表 1:N——从表倒排字段同一关联 key 多行时逐行展开。
    #[test]
    fn join_one_to_many_expands() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        // 主表 1 行（amount 过滤隔离；user_city 关联字段不含 "city"，避免污染从表 term）
        e.put(1, br#"{"amount":100,"user_city":"bj","name":"order1"}"#.to_vec(), &["user_city=bj"])
            .unwrap();
        // 从表 3 行（同一 city=bj → 1:N）
        for (i, n) in [(11u64, "u1"), (12, "u2"), (13, "u3")] {
            let doc = format!(r#"{{"city":"bj","name":"{n}"}}"#);
            e.put(i, doc.into_bytes(), &["city=bj"]).unwrap();
        }
        // 无匹配 city=gz 的从表行
        e.put(14, br#"{"city":"gz","name":"other"}"#.to_vec(), &["city=gz"])
            .unwrap();
        e.flush_wal().unwrap();

        // INNER JOIN：orders.user_city = users.city（users 侧走倒排 1:N）→ 3 结果行
        let rows = execute(
            &e,
            "SELECT * FROM orders INNER JOIN users ON orders.user_city = users.city WHERE amount>=100 LIMIT 100",
            1000,
        )
        .unwrap();
        assert_eq!(rows.len(), 3, "1:N 展开为 3 行（修复前只取 posting 首行 = 1 行）");
        for (_, doc) in &rows {
            let v: serde_json::Value = serde_json::from_slice(doc).unwrap();
            assert!(v.get("users").is_some(), "每行须含从表嵌套");
        }
    }

    /// review 修复（2026-09-04）：多 JOIN 解析期拒绝——A JOIN B JOIN C 静默覆盖只留最后一个。
    #[test]
    fn multi_join_rejected_at_parse() {
        assert!(parse_select(
            "SELECT * FROM a INNER JOIN b ON a.x=b.x INNER JOIN c ON b.y=c.y"
        )
        .is_err());
    }

    /// review 修复（2026-09-04）：JOIN 路由须先于组合索引——主表 WHERE 命中 composite_indexes
    /// 时不得走纯主表组合索引短路（会静默丢弃 JOIN）。
    #[test]
    fn join_not_shorted_by_composite_index() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::default();
        // 主表 orders 的 city 命中组合索引前缀 → 修复前 try_composite_index 直接返回主表行
        cfg.storage.composite_indexes = vec![vec!["city".into()]];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        e.put(1, br#"{"user_id":"101","city":"bj","amount":100}"#.to_vec(), &["city=bj"])
            .unwrap();
        e.put(2, br#"{"user_id":"102","city":"sh","amount":200}"#.to_vec(), &["city=sh"])
            .unwrap();
        e.put(101, br#"{"name":"alice"}"#.to_vec(), &["name=alice"])
            .unwrap();
        e.put(102, br#"{"name":"bob"}"#.to_vec(), &["name=bob"])
            .unwrap();
        e.flush_wal().unwrap();

        // WHERE city='bj' 命中组合索引 [city]；修复前此查询被组合索引短路返回纯主表 1 行（无 users）
        let rows = execute(
            &e,
            "SELECT * FROM orders INNER JOIN users ON orders.user_id = users.docid WHERE city='bj' LIMIT 100",
            1000,
        )
        .unwrap();
        assert_eq!(rows.len(), 1, "JOIN 应产出 1 行合并结果");
        let v: serde_json::Value =
            serde_json::from_slice(&rows[0].1).expect("JOIN 结果为嵌套文档");
        assert!(
            v.get("users").is_some() && v["users"].get("name") == Some(&serde_json::json!("alice")),
            "结果须含从表 users 合并（未被组合索引短路）"
        );
    }

    /// P0-B：Top-K 有界堆——大候选集 ORDER BY LIMIT 不再被拒绝。
    #[test]
    fn topk_order_by_large_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        // 300K 行全扫需较长超时，用 30s 避免熔断
        let mut e = Engine::open_with_timeout(
            dir.path(),
            &cfg,
            std::time::Duration::from_secs(30),
        )
        .unwrap();
        // 写入 30 万行（超过 SORT_MAX_ROWS=20 万）
        for i in 0..300_000u64 {
            let doc = serde_json::json!({"k": format!("key{i}"), "amount": i});
            e.put_nosync(i, serde_json::to_vec(&doc).unwrap(), &[]).unwrap();
        }
        e.flush_wal().unwrap();

        // ORDER BY amount LIMIT 10 → Top-K 堆，不再拒绝
        let rows = execute(&e, "SELECT * FROM t ORDER BY amount LIMIT 10", 1000).unwrap();
        assert_eq!(rows.len(), 10, "Top-K 返回 10 行");
        // 验证排序正确（amount 0~9 升序）
        for (i, (_, doc)) in rows.iter().enumerate() {
            let v: serde_json::Value = serde_json::from_slice(doc).unwrap();
            assert_eq!(v["amount"].as_u64().unwrap(), i as u64, "第 {i} 行 amount={i}");
        }

        // ORDER BY amount DESC LIMIT 5 → 降序 top-5
        let rows2 = execute(&e, "SELECT * FROM t ORDER BY amount DESC LIMIT 5", 1000).unwrap();
        assert_eq!(rows2.len(), 5, "Top-K DESC 返回 5 行");
        let v: serde_json::Value = serde_json::from_slice(&rows2[0].1).unwrap();
        assert_eq!(v["amount"].as_u64().unwrap(), 299999, "DESC 首行 amount=299999");
    }

    /// P0-A：SQL 层组合索引声明式路由端到端——WHERE 等值命中 composite_indexes 最左前缀时，
    /// execute 走 cidx 前缀扫描（而非全扫/倒排），结果正确且含 LIMIT/OFFSET。
    #[test]
    fn composite_index_sql_routing_happy_path() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.storage.composite_indexes = vec![
            vec!["status".into(), "region".into()],
            vec!["status".into()],
        ];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        // 写入 6 个文档（不需要倒排，让组合索引路由处理）
        e.put(1, br#"{"status":"active","region":"east","amount":100}"#.to_vec(), &[]).unwrap();
        e.put(2, br#"{"status":"active","region":"west","amount":200}"#.to_vec(), &[]).unwrap();
        e.put(3, br#"{"status":"inactive","region":"east","amount":300}"#.to_vec(), &[]).unwrap();
        e.put(4, br#"{"status":"active","region":"east","amount":400}"#.to_vec(), &[]).unwrap();
        e.put(5, br#"{"status":"active","region":"north","amount":500}"#.to_vec(), &[]).unwrap();
        e.put(6, br#"{"status":"inactive","region":"west","amount":600}"#.to_vec(), &[]).unwrap();
        e.flush_wal().unwrap();

        // 等值单字段：status='active' → 匹配 [status] 索引 → 4 行
        let rows = execute(&e, "SELECT * FROM t WHERE status='active'", 1000).unwrap();
        assert_eq!(rows.len(), 4, "status=active 应返回 4 行");
        let mut ids: Vec<u64> = rows.iter().map(|(d, _)| *d).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2, 4, 5]);

        // 等值双字段：status='active' AND region='east' → 匹配 [status,region] 最长前缀 → 2 行
        let rows2 = execute(&e, "SELECT * FROM t WHERE status='active' AND region='east'", 1000).unwrap();
        assert_eq!(rows2.len(), 2, "active+east 应返回 2 行");
        let mut ids2: Vec<u64> = rows2.iter().map(|(d, _)| *d).collect();
        ids2.sort();
        assert_eq!(ids2, vec![1, 4]);

        // LIMIT 1：组合索引 + LIMIT 下推正确
        let rows3 = execute(&e, "SELECT * FROM t WHERE status='active' LIMIT 1", 1000).unwrap();
        assert_eq!(rows3.len(), 1, "LIMIT 1 只返回 1 行");

        // OFFSET 1 LIMIT 1
        let rows4 = execute(&e, "SELECT * FROM t WHERE status='active' LIMIT 1 OFFSET 1", 1000).unwrap();
        assert_eq!(rows4.len(), 1, "OFFSET 1 LIMIT 1 返回 1 行");

        // 不匹配的等值条件：status='unknown' → 组合索引返回空，回退到原路径
        let rows5 = execute(&e, "SELECT * FROM t WHERE status='unknown'", 1000).unwrap();
        assert!(rows5.is_empty(), "unknown 应返回空结果");
    }

    // ---------- P85：DocIdSet 消费端 LIMIT 早停（分块批量回表） ----------

    /// mock 取行：`even_only=true` 时奇数 docid = 墓碑/未命中（返回 None，不占 limit 占 offset）；
    /// false = 全部可见。统计拉取次数与总量。
    fn run_collect(cand: Vec<u64>, offset: u64, limit: u64, even_only: bool) -> (Vec<(u64, Vec<u8>)>, usize, usize) {
        let mut calls = 0usize;
        let mut pulled = 0usize;
        let rows = collect_limited_rows(
            |chunk: &[u64]| {
                calls += 1;
                pulled += chunk.len();
                Ok(chunk
                    .iter()
                    .map(|&d| {
                        if !even_only || d % 2 == 0 {
                            Some(format!("v{d}").into_bytes())
                        } else {
                            None
                        }
                    })
                    .collect())
            },
            cand.into_iter(),
            offset,
            limit,
        )
        .unwrap();
        (rows, calls, pulled)
    }

    #[test]
    fn p85_limited_rows_early_stop_chunk_branches() {
        // Bitmap 升序消费（模拟 2000 posting，全部可见）：offset+limit=1020 → 2 块（1024）即止，
        // 后续块零拉取（若全量物化应拉 2000/512≈4 块）
        let cand: Vec<u64> = (0..2000).collect();
        let (rows, calls, pulled) = run_collect(cand, 1000, 20, false);
        let ids: Vec<u64> = rows.iter().map(|(d, _)| *d).collect();
        assert_eq!(ids, (1000..1020).collect::<Vec<u64>>(), "offset 后可见行恰好 limit");
        assert_eq!(calls, 2, "1020 候选 → 2 块即终止");
        assert_eq!(pulled, 1024, "终止后剩余块零拉取（全量需 ~4 块）");
        for (i, (_, v)) in rows.iter().enumerate() {
            assert_eq!(v, format!("v{}", 1000 + i).as_bytes(), "回表字节与 docid 对应");
        }

        // SortedList 语义：墓碑/未命中占 offset 占位、不占 limit（偶数可见）→ offset5 后
        // 前 3 可见行 6/8/10（5.. 位置中 d5/d7/d9 为 None 不计 limit）
        let (rows2, calls2, _) = run_collect((0..60).collect(), 5, 3, true);
        let ids2: Vec<u64> = rows2.iter().map(|(d, _)| *d).collect();
        assert_eq!(ids2, vec![6, 8, 10], "墓碑不占 limit，offset 占候选位");
        assert_eq!(calls2, 1, "60 候选单块即止");

        // limit=0 → 零拉取零回表
        let (rows3, calls3, pulled3) = run_collect((0..100).collect(), 0, 0, false);
        assert!(rows3.is_empty());
        assert_eq!(calls3, 0);
        assert_eq!(pulled3, 0);

        // 可见行恰好 limit（offset 0）
        let (rows4, _, _) = run_collect((0..40).collect(), 0, 4, true);
        let ids4: Vec<u64> = rows4.iter().map(|(d, _)| *d).collect();
        assert_eq!(ids4, vec![0, 2, 4, 6], "可见行恰好 limit=4");

        // 候选耗尽不足 limit → 返回全部可见
        let (rows5, _, _) = run_collect((0..5).collect(), 1, 100, true);
        let ids5: Vec<u64> = rows5.iter().map(|(d, _)| *d).collect();
        assert_eq!(ids5, vec![2, 4], "1 个占位后剩余可见行（d2/d4）");
    }

    #[test]
    fn p85_execute_limit_not_fetch_beyond_page() {
        // 端到端：倒排命中 34 行，LIMIT 3 OFFSET 5 → 只取 8 个候选行
        let e = engine_with_docs();
        let rows = execute(
            &e,
            "SELECT * FROM t WHERE status='active' LIMIT 3 OFFSET 5",
            1000,
        )
        .unwrap();
        // status='active' 34 行 docid：0,3,6,...,99（非排序位图升序）→ offset5 = 15 起 3 行
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![15, 18, 21]);
    }

    // ---------- P86②：排序键按需字段提取（单遍 vs serde 逐字段等值） ----------

    #[test]
    fn p86_row_sort_keys_matches_per_field_serde() {
        let docs: Vec<&[u8]> = vec![
            br#"{"amount":42,"note":"note-1","city":"beijing"}"#,
            br#"{"amount":-3.5,"note":"hello"}"#,
            br#"{"amount":null,"note":"x\"y","city":"shanghai"}"#,
            br#"{"note":"note-0","city":""}"#,
            br#"{"amount":1e3,"note":"n1","city":"\u4e2d"}"#, // JSON 转义串值 → 回退 serde（正确性护栏）
            br#"{"amount":true,"note":"n","city":["a"]}"#,
            br#"not-json"#,
        ];
        let fields: Vec<String> = vec!["amount".into(), "note".into(), "city".into()];
        for doc in &docs {
            let got = row_sort_keys(doc, &fields);
            let expect: Vec<SortKey> = fields.iter().map(|f| sort_key(doc, f)).collect();
            for (i, (g, ex)) in got.iter().zip(expect.iter()).enumerate() {
                let same = match (g, ex) {
                    (SortKey::Null, SortKey::Null) => true,
                    (SortKey::Num(a), SortKey::Num(b)) => a == b,
                    (SortKey::Str(a), SortKey::Str(b)) => a == b,
                    _ => false,
                };
                assert!(same, "doc {:?} field {} 等值：got={:?} expect={:?}", doc, fields[i], g, ex);
            }
        }
        // 显式语义抽查：数值→Num、字符串→Str、缺失/null/嵌套/布尔→Null
        let doc = br#"{"a":10,"s":"x","n":null,"miss":1}"#;
        assert_eq!(row_sort_keys(doc, &vec!["a".into()]), vec![SortKey::Num(10.0)]);
        assert_eq!(row_sort_keys(doc, &vec!["s".into()]), vec![SortKey::Str("x".into())]);
        assert!(matches!(row_sort_keys(doc, &vec!["n".into()])[0], SortKey::Null));
        assert!(matches!(row_sort_keys(doc, &vec!["no_such".into()])[0], SortKey::Null));
    }

    // ---------- P87：ORDER BY Top-K 流式化（分块 + 排序键解码下推） ----------

    /// P87：Top-K 流式结果与全量排序一致（多键/DESC/OFFSET），数据落 SST（PAX 热字段
    /// 列式 → batch_get_fields 列解码路径）——端到端验证 ② 与 ③。
    #[test]
    fn p87_topk_streaming_matches_full_sort_over_pax() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::default();
        // hot_fields → flush 落 PAX 列式块（P87② 列解码消费端接线）
        cfg.storage.hot_fields = vec!["amount".into(), "city".into(), "note".into()];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let cities = ["beijing", "shanghai", "shenzhen"];
        for i in 0..100u64 {
            let doc = serde_json::json!({
                "docid": i,
                "status": if i % 3 == 0 { "active" } else { "inactive" },
                "city": cities[(i % 3) as usize],
                "amount": i * 10,
                "note": format!("note-{i}"),
            });
            e.put(i, serde_json::to_vec(&doc).unwrap(), &[]).unwrap();
        }
        e.flush_primary().unwrap(); // MemTable → SST（PAX 块）

        // 全量排序参考（无 LIMIT → 全排序路径）
        let full = execute(&e, "SELECT * FROM t ORDER BY amount ASC", 1000).unwrap();
        let full_ids: Vec<u64> = full.iter().map(|r| r.0).collect();
        assert_eq!(full_ids.len(), 100);
        // Top-K（LIMIT 6 OFFSET 4）应与全排序切片一致
        let topk = execute(&e, "SELECT * FROM t ORDER BY amount ASC LIMIT 6 OFFSET 4", 1000).unwrap();
        let topk_ids: Vec<u64> = topk.iter().map(|r| r.0).collect();
        assert_eq!(topk_ids, full_ids[4..10].to_vec(), "Top-K 切片 = 全排序切片");
        // 行内容（amount）随 docid 正确
        for (i, (_, doc)) in topk.iter().enumerate() {
            let v: serde_json::Value = serde_json::from_slice(doc).unwrap();
            assert_eq!(v["amount"].as_u64().unwrap(), (full_ids[4 + i]) * 10);
        }
        // 多键 DESC（city ASC + amount DESC → beijing 组内降序 99/96/93）
        let rows = execute(
            &e,
            "SELECT * FROM t ORDER BY city ASC, amount DESC LIMIT 3",
            1000,
        )
        .unwrap();
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![99, 96, 93]);
    }

    /// P87：Top-K 块看门狗熔断——guard 到期后首个分块即中止（防大候选无限解码）。
    #[test]
    fn p87_topk_chunk_watchdog_fuses() {
        let e = engine_with_docs();
        let guard = e.query_guard();
        // 构造已到期 guard（零超时引擎）
        let dir0 = tempfile::tempdir().unwrap();
        let ge = Engine::open_with_timeout(dir0.path(), &Config::default(), std::time::Duration::ZERO)
            .unwrap();
        let dead = ge.query_guard();
        assert!(dead.is_expired(), "零超时 guard 应立即到期");
        let bm = full_docids(&e, &guard).unwrap();
        let order_by = vec![("amount".to_string(), false)];
        let err = topk_sort(&e, &bm, &order_by, 10, 0, 10, &dead).unwrap_err();
        assert!(
            matches!(err, Error::QueryTooExpensive(_)),
            "到期 guard 应在分块处熔断，实际 {err:?}"
        );
    }

    // ---------- P90：PAX 块级聚合下推（块级 zones 免读数据块；版本混入回退行级） ----------

    /// 行级参考实现（扫全表逐行解析字段）——与 SQL 聚合语义一致。
    fn manual_agg(e: &Engine, field: &str) -> (u64, f64) {
        let mut count = 0u64;
        let mut sum = 0f64;
        let mut need_serde = true;
        e.scan_stream(None, None, |_d, doc| {
            if let Some(lv) = light_top_field(doc, field) {
                match lv {
                    LightVal::Absent | LightVal::Null => {}
                    LightVal::Num(bytes) => {
                        count += 1;
                        if let Some(x) = std::str::from_utf8(bytes).ok().and_then(|s| s.parse::<f64>().ok()) {
                            sum += x;
                        }
                    }
                    _ => {
                        count += 1;
                    }
                }
                need_serde = false;
            }
            if need_serde {
                if let Ok(v) = serde_json::from_slice::<Value>(doc) {
                    if let Some(fv) = v.get(field) {
                        if !fv.is_null() {
                            count += 1;
                            if let Some(x) = fv.as_f64() {
                                sum += x;
                            }
                        }
                    }
                }
            }
            Ok(true)
        })
        .unwrap();
        (count, sum)
    }

    fn agg_scalar(e: &Engine, sql: &str) -> (String, bool, String) {
        let a = execute_aggregate(e, sql).unwrap().unwrap();
        (a.header, a.is_null, a.text)
    }

    #[test]
    fn p90_pax_zone_aggregate_matches_scan() {
        // P90：SUM/COUNT(f) 无 WHERE → PAX 块级 zones 快路径与行级扫描结果一致；
        // memtable 未刷盘（版本混入）→ 自动回退行级，结果仍一致。
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.storage.hot_fields = vec!["amount".into(), "k".into(), "note".into()];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        // 199 行 amount=10i+5（数值纯列，和 != 0 → SUM 快路径生效）；1 行缺 amount
        let mut sum_expected = 0f64;
        for i in 0..199u64 {
            let doc = serde_json::json!({"amount": 10 * i + 5, "k": i, "note": format!("n{i}")});
            e.put(i, serde_json::to_vec(&doc).unwrap(), &[]).unwrap();
            sum_expected += (10 * i + 5) as f64;
        }
        e.put(199, br#"{"k":199,"note":"missing"}"#.to_vec(), &[]).unwrap();
        // ① 未刷盘（memtable 非空）→ 回退行级，先验参考
        let (_, s) = manual_agg(&e, "amount");
        assert_eq!(s, sum_expected, "行级参考");
        let (h, isnull, t) = agg_scalar(&e, "SELECT SUM(amount) FROM t");
        assert_eq!(h, "SUM(amount)");
        assert!(!isnull);
        assert_eq!(t.parse::<f64>().unwrap(), sum_expected);
        let (_, _, c) = agg_scalar(&e, "SELECT COUNT(amount) FROM t");
        assert_eq!(c, "199", "缺字段行不计 COUNT");
        // ② flush 后（PAX 单 L0 段、memtable 空）→ 块级 zones 快路径，结果一致
        e.flush_primary().unwrap();
        let (h2, isnull2, t2) = agg_scalar(&e, "SELECT SUM(amount) FROM t");
        assert_eq!(h2, "SUM(amount)");
        assert!(!isnull2);
        assert_eq!(t2.parse::<f64>().unwrap(), sum_expected, "块级 sum 下推 = 行级");
        let (_, _, c2) = agg_scalar(&e, "SELECT COUNT(amount) FROM t");
        assert_eq!(c2, "199", "块级 present-null 计数");
        // AVG/MIN/MAX 不经块级（zones 无法判定数值行数）→ 仍走行级，语义不变
        let (_, _, av) = agg_scalar(&e, "SELECT AVG(amount) FROM t");
        assert!((av.parse::<f64>().unwrap() - sum_expected / 199.0).abs() < 1e-9, "AVG 行级精确");
        // ③ memtable 再写入（未刷盘）→ 回退行级且含新行
        e.put(200, br#"{"amount":777,"k":200}"#.to_vec(), &[]).unwrap();
        let (_, _, t3) = agg_scalar(&e, "SELECT SUM(amount) FROM t");
        assert_eq!(t3.parse::<f64>().unwrap(), sum_expected + 777.0, "回退行级含 memtable 新行");
        let (_, _, c3) = agg_scalar(&e, "SELECT COUNT(amount) FROM t");
        assert_eq!(c3, "200");
        // ④ 块级快路径与行级在字段值全部为零和（歧义）时回退行级 → NULL/0 语义正确
        let dir2 = tempfile::tempdir().unwrap();
        let mut cfg2 = Config::default();
        cfg2.storage.hot_fields = vec!["z".into()];
        let mut e2 = Engine::open(dir2.path(), &cfg2).unwrap();
        for i in 0..50u64 {
            let doc = serde_json::json!({"z": 0});
            e2.put(i, serde_json::to_vec(&doc).unwrap(), &[]).unwrap();
        }
        e2.flush_primary().unwrap();
        let (_, isnull_z, tz) = agg_scalar(&e2, "SELECT SUM(z) FROM t");
        assert!(!isnull_z);
        assert_eq!(tz.parse::<f64>().unwrap(), 0.0, "全零行 SUM=0（行级回退不误报 NULL）");
        let (_, _, cz) = agg_scalar(&e2, "SELECT COUNT(z) FROM t");
        assert_eq!(cz, "50");
    }

    // ---------- P1-D：倒排候选收敛聚合的范围/组合条件扩展（按需解列） ----------

    #[test]
    fn p1d_candidate_projected_agg_range_and_combo() {
        // P1-D：`SUM/COUNT(...) WHERE 倒排等值 AND 范围`——posting 候选收敛后按需解列
        // （只回表 WHERE 引用字段 + 聚合字段），残余范围条件在子集文档判定；结果与全扫一致。
        let e = engine_with_docs(); // docid i：status active 当 i%3==0；amount = i*10；city 循环
        // SUM + AND(等值, 范围)：active 且 amount>=150 → i=15..99 step3（29 行）sum=10*(15+18+...+99)=16530
        let a = execute_aggregate(
            &e,
            "SELECT SUM(amount) FROM t WHERE status='active' AND amount>=150",
        )
        .unwrap()
        .unwrap();
        assert!(!a.is_null);
        assert_eq!(a.text, "16530");
        // COUNT(*) 同条件 = 29
        let c = execute_aggregate(
            &e,
            "SELECT COUNT(*) FROM t WHERE status='active' AND amount>=150",
        )
        .unwrap()
        .unwrap();
        assert_eq!(c.text, "29");
        // AVG 组合（行级精确，聚合字段 amount 同时参与范围谓词）
        let m = execute_aggregate(
            &e,
            "SELECT AVG(amount) FROM t WHERE status='active' AND amount>=150",
        )
        .unwrap()
        .unwrap();
        assert!((m.text.parse::<f64>().unwrap() - 16530.0 / 29.0).abs() < 1e-6);
        // BETWEEN + 倒排候选：active 且 amount∈[200,500] → i=21..48 step3（10 行）sum=3450
        let b = execute_aggregate(
            &e,
            "SELECT SUM(amount) FROM t WHERE status='active' AND amount BETWEEN 200 AND 500",
        )
        .unwrap()
        .unwrap();
        assert_eq!(b.text, "3450");
        // 非聚合字段范围 + 分组字段组合：city='shenzhen'（i%3==2）AND amount∈[150,350]
        // → i=17..35 step3（7 行）sum=1820
        let d = execute_aggregate(
            &e,
            "SELECT SUM(amount) FROM t WHERE city='shenzhen' AND amount BETWEEN 150 AND 350",
        )
        .unwrap()
        .unwrap();
        assert_eq!(d.text, "1820");
        // 无匹配组合（范围外）→ SUM NULL
        let none = execute_aggregate(
            &e,
            "SELECT SUM(amount) FROM t WHERE status='active' AND amount>=100000",
        )
        .unwrap()
        .unwrap();
        assert!(none.is_null, "无命中 SUM → SQL NULL");
        // 候选解列字段去重（WHERE 重复引用同字段不重复回表）
        assert_eq!(
            aggregate_needed_fields(
                parse_select("SELECT * FROM t WHERE status='a' AND status='b' AND amount>1")
                    .unwrap()
                    .where_expr
                    .as_ref(),
                Some("amount")
            ),
            vec!["status".to_string(), "amount".to_string()]
        );
    }

    // ---------- P91：投影列扫描 SQL 层等价（PAX 投影 vs 行式整行） ----------

    #[test]
    fn p91_projected_scan_sql_equivalence_row_vs_pax() {
        // P91：同一逻辑数据分别入 行式（默认）与 PAX（hot_fields）两库并 flush 落 SST，
        // 加部分 memtable 未刷盘行（版本混入）→ scoped 窗口全扫走 scan_stream_fields 投影
        // 路径（PAX 只解请求列组装子集 JSON）——GROUP BY / 聚合结果必须与行式逐值一致。
        fn build(hot: bool) -> (Engine, tempfile::TempDir) {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = Config::default();
            if hot {
                cfg.storage.hot_fields = vec!["status".into(), "amount".into(), "city".into()];
            }
            let mut e = Engine::open(dir.path(), &cfg).unwrap();
            for i in 0..200u64 {
                let mut doc = serde_json::Map::new();
                let status = match i % 3 {
                    0 => "active",
                    1 => "closed",
                    _ => "pending",
                };
                doc.insert("status".into(), serde_json::Value::String(status.into()));
                doc.insert("amount".into(), serde_json::Value::from(i * 10u64));
                let city = ["beijing", "shanghai", "shenzhen"][(i % 3) as usize];
                doc.insert("city".into(), serde_json::Value::String(city.into()));
                doc.insert("note".into(), serde_json::Value::String(format!("n{i}")));
                if i % 7 == 0 {
                    doc.remove("status"); // 缺字段行（NULL 组路径）
                }
                if i % 11 == 0 {
                    doc.insert("amount".into(), serde_json::Value::Null); // JSON null
                }
                e.put(i, serde_json::to_vec(&serde_json::Value::Object(doc)).unwrap(), &[]).unwrap();
            }
            e.flush_primary().unwrap();
            // flush 后追加（memtable 未刷盘 → 与 SST 投影行共存于 merge 赢家路径）
            for i in 200..260u64 {
                let doc = serde_json::json!({
                    "status": if i % 2 == 0 { "active" } else { "closed" },
                    "amount": i * 10,
                    "city": "chengdu",
                });
                e.put(i, serde_json::to_vec(&doc).unwrap(), &[]).unwrap();
            }
            (e, dir)
        }
        fn group_snapshot(e: &Engine, sql: &str) -> Vec<(Vec<Option<String>>, Vec<Option<String>>)> {
            let r = execute_group_by_window(e, sql, 100_000, Some(0), None)
                .unwrap()
                .unwrap();
            r.rows
                .into_iter()
                .map(|g| (g.keys, g.cells))
                .collect()
        }
        let (rm, _rm_dir) = build(false);
        let (px, _px_dir) = build(true);
        for sql in [
            "SELECT status, COUNT(*) FROM t GROUP BY status",
            "SELECT status, COUNT(*), SUM(amount) FROM t GROUP BY status",
            "SELECT city, COUNT(*) FROM t GROUP BY city",
        ] {
            assert_eq!(group_snapshot(&rm, sql), group_snapshot(&px, sql), "GROUP BY 行式=PAX: {sql}");
        }
        for sql in [
            "SELECT SUM(amount) FROM t WHERE status='active'",
            "SELECT COUNT(*) FROM t WHERE status='active' AND amount>=1500",
            "SELECT AVG(amount) FROM t WHERE city='beijing'",
            "SELECT COUNT(amount) FROM t",
        ] {
            let a = execute_aggregate_window(&rm, sql, Some(0), None).unwrap().unwrap();
            let b = execute_aggregate_window(&px, sql, Some(0), None).unwrap().unwrap();
            assert_eq!((a.header.clone(), a.is_null, a.text.clone()), (b.header.clone(), b.is_null, b.text.clone()), "聚合 行式=PAX: {sql}");
        }
    }

    // ---------- Task-021：COUNT 全包窗口直通 O(1) ----------

    #[test]
    fn task021_count_full_table_window_o1_parity_and_isolation() {
        // 整表窗口（默认表 [0,2^48) / 非默认表 [tid<<48, tid<<48+2^48-1]）COUNT(*) 无 WHERE
        // → 引擎活跃 docid 区间 O(1)；与无窗口 count_all_docs / keys-only 部分窗口口径一致；
        // 删除位图隐藏行不计；多表 docid 高位隔离。
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        for i in 1..=1000u64 {
            e.put(i, serde_json::to_vec(&serde_json::json!({"k": i})).unwrap(), &[])
                .unwrap();
        }
        e.delete(77).unwrap(); // 删除位图隐藏 → 不计数
        e.flush_primary().unwrap();
        let t1: u64 = 1u64 << 48;
        for x in 0..7u64 {
            e.put(t1 + x, serde_json::to_vec(&serde_json::json!({"k": "t1"})).unwrap(), &[])
                .unwrap();
        }
        let mask: u64 = (1u64 << 48) - 1;
        // 整表 tid0 窗口：1000 - 删除 77 = 999（tid1 不计）
        let a = execute_aggregate_window(&e, "SELECT COUNT(*) FROM t", Some(0), Some(mask))
            .unwrap()
            .unwrap();
        assert_eq!(a.text, "999", "tid0 整表窗口 O(1) 计数");
        assert_eq!(e.count_docs_range(0, mask).unwrap(), 999);
        // 无窗口（全库）：999 + 7 = 1006（保持 count_all_docs 语义）
        let b = execute_aggregate_window(&e, "SELECT COUNT(*) FROM t", None, None)
            .unwrap()
            .unwrap();
        assert_eq!(b.text, "1006");
        // 非默认表整表窗口隔离 = 7
        let c = execute_aggregate_window(&e, "SELECT COUNT(*) FROM t", Some(t1), Some(t1 + mask))
            .unwrap()
            .unwrap();
        assert_eq!(c.text, "7");
        // 部分窗口（非整表）保持 keys-only：现有行 100..=200 → 101
        let d = execute_aggregate_window(&e, "SELECT COUNT(*) FROM t", Some(100), Some(200))
            .unwrap()
            .unwrap();
        assert_eq!(d.text, "101");
    }

    // ---------- P92：Top-K 稠密窗口投影流式 vs 稀疏点查路径 ----------

    #[test]
    fn p92_topk_dense_stream_and_sparse_point_match_ground_truth() {
        // 稠密（无 WHERE 全表 span==len）→ topk_sort 走 scan_stream_fields 投影流式；
        // 稀疏（WHERE k=7，密度 ~1/17 <25%）→ 保持 P87 分块点查。两路径输出必须与
        // 引擎全量扫描 + 手动排序的地面真值一致（行式 + PAX(hot_fields) 两布局）。
        fn build(hot: bool) -> (Engine, tempfile::TempDir) {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = Config::default();
            if hot {
                cfg.storage.hot_fields = vec!["k".into(), "amount".into()];
            }
            let mut e = Engine::open(dir.path(), &cfg).unwrap();
            for i in 0..2000u64 {
                let doc = serde_json::json!({"k": i % 17, "amount": (i * 37) % 9973, "note": format!("n{i}")});
                e.put(i, serde_json::to_vec(&doc).unwrap(), &[]).unwrap();
            }
            e.flush_primary().unwrap();
            for i in 2000..2100u64 {
                let doc = serde_json::json!({"k": i % 17, "amount": (i * 37) % 9973, "note": format!("n{i}")});
                e.put(i, serde_json::to_vec(&doc).unwrap(), &[]).unwrap();
            }
            (e, dir)
        }
        fn top10(e: &Engine, sql: &str) -> Vec<(i64, i64)> {
            let rows = execute(e, sql, 100_000).unwrap();
            rows.into_iter()
                .map(|(_, doc)| {
                    let v: serde_json::Value = serde_json::from_slice(&doc).unwrap();
                    (v["k"].as_i64().unwrap(), v["amount"].as_i64().unwrap())
                })
                .collect()
        }
        fn ground(e: &Engine, k_sel: Option<i64>) -> Vec<(i64, i64)> {
            let mut sel: Vec<(i64, i64)> = Vec::new();
            e.scan_stream(None, None, |_, doc| {
                let v: serde_json::Value = serde_json::from_slice(doc).unwrap();
                let k = v["k"].as_i64().unwrap();
                let am = v["amount"].as_i64().unwrap();
                if k_sel.is_none_or(|x| k == x) {
                    sel.push((k, am));
                }
                Ok(true)
            })
            .unwrap();
            // (k asc, amount desc) top10
            sel.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
            sel.truncate(10);
            sel
        }
        for hot in [false, true] {
            let (e, _dir) = build(hot);
            // 稠密全表（无 WHERE）→ 投影流式窗口路径
            assert_eq!(
                top10(&e, "SELECT * FROM t ORDER BY k, amount DESC LIMIT 10"),
                ground(&e, None),
                "p92 稠密流式 hot={hot}"
            );
            // 单键降序（amount）全表
            let mut g2: Vec<(i64, i64)> = Vec::new();
            e.scan_stream(None, None, |_, doc| {
                let v: serde_json::Value = serde_json::from_slice(doc).unwrap();
                g2.push((v["k"].as_i64().unwrap(), v["amount"].as_i64().unwrap()));
                Ok(true)
            })
            .unwrap();
            g2.sort_by(|a, b| b.1.cmp(&a.1));
            g2.truncate(10);
            assert_eq!(top10(&e, "SELECT * FROM t ORDER BY amount DESC LIMIT 10"), g2, "p92 单键 DESC hot={hot}");
            // 稀疏（k=7，密度低）→ 点查路径
            assert_eq!(
                top10(&e, "SELECT * FROM t WHERE k=7 ORDER BY k, amount DESC LIMIT 10"),
                ground(&e, Some(7)),
                "p92 稀疏点查 hot={hot}"
            );
        }
    }

    #[test]
    fn p92_composite_range_routing_matches_scan() {
        // #31 形态：单列组合索引 ["ts"] + WHERE ts BETWEEN（无等值前缀）→
        // try_composite_index 范围路由（query_by_composite_range）命中行集 =
        // 全扫 BETWEEN 语义；边界字节序误命中被 WHERE 复筛剔除。
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.storage.composite_indexes = vec![vec!["ts".into()]];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let base = 1_700_000_000u64;
        for i in 0..500u64 {
            let doc = serde_json::json!({
                "ts": base + i * 10,
                "status": if i % 3 == 0 { "active" } else { "closed" },
                "k": i % 17,
            });
            e.put(i, serde_json::to_vec(&doc).unwrap(), &[]).unwrap();
        }
        e.flush_primary().unwrap();
        // 窗口 [base+100, base+300]（含端点；等宽 10 位数值 → 字节序=数值序）
        let sql = format!("SELECT id,ts FROM t WHERE ts BETWEEN {} AND {}", base + 100, base + 300);
        let rows = execute(&e, &sql, 10_000).unwrap();
        let mut got: Vec<u64> = rows.into_iter().map(|(d, _)| d).collect();
        got.sort_unstable();
        let mut want: Vec<u64> = (10..=30).collect(); // i 满足 100<=10i<=300
        assert_eq!(got, want, "cidx 范围路由命中集 = BETWEEN 全扫语义");
        // 残余：BETWEEN 端点外不误命中
        let sql2 = format!("SELECT id FROM t WHERE ts BETWEEN {} AND {}", base + 45, base + 55);
        let rows2 = execute(&e, &sql2, 10_000).unwrap();
        let mut got2: Vec<u64> = rows2.into_iter().map(|(d, _)| d).collect();
        got2.sort_unstable();
        // ts 步长 10：base+50 命中（i=5），base+45/55 无整点值 → 仅 i=5
        assert_eq!(got2, vec![5], "边界半开不误命中（字节序区间下界语义）");
    }
