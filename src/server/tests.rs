
//! 原 src/db_adapter.rs 底部 `mod tests`（约 2880 行）整体迁移至此（reconstruct.md
//! server/ 规划：db_adapter 测试集中放 server/tests.rs）。模块声明在 server/mod.rs
//! （`#[cfg(test)] mod tests;`）；`use super::*` 取 server 根聚合 re-export（拆分后各
//! 子模块项在 server 根可见，行为与拆分前完全一致）。原 db_adapter.rs 顶部对 std /
//! 外部 crate / 其他顶层模块的 import 在此显式补充（原文件单模块作用域天然可见）。

use super::*;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use sha1::{Digest, Sha1};

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::multitable::drop_table_range;

    fn test_engine() -> Engine {
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        Engine::open(dir.path(), &cfg).unwrap()
    }

    /// Task-031：`frame_response` 单帧编码 = 逐包 `write_packet` 字节流（含 seq 回绕）——
    /// 批量写不改变协议包边界/序号，客户端逐包读取语义不变。
    #[test]
    fn task031_frame_response_equals_sequential_packets() {
        let payloads: Vec<Vec<u8>> = vec![
            vec![1u8; 5],
            vec![2u8; 300],
            vec![3u8; 0x100],
            vec![4u8; 0x1_0000],
        ];
        for cmd_seq in [0u8, 250, 254] {
            let frame = frame_response(payloads.clone(), cmd_seq);
            let mut expect: Vec<u8> = Vec::new();
            let mut seq = cmd_seq;
            for p in &payloads {
                let len = p.len() as u32;
                expect.extend_from_slice(&[
                    (len & 0xff) as u8,
                    ((len >> 8) & 0xff) as u8,
                    ((len >> 16) & 0xff) as u8,
                    seq,
                ]);
                expect.extend_from_slice(p);
                seq = seq.wrapping_add(1);
            }
            assert_eq!(frame, expect, "cmd_seq={cmd_seq} 单帧字节流应与逐包一致");
            // 按长度前缀逐包还原，验证包边界/序号保持
            let mut i = 0usize;
            let mut got: Vec<Vec<u8>> = Vec::new();
            while i + 4 <= frame.len() {
                let len = (frame[i] as usize)
                    | ((frame[i + 1] as usize) << 8)
                    | ((frame[i + 2] as usize) << 16);
                got.push(frame[i + 4..i + 4 + len].to_vec());
                i += 4 + len;
            }
            assert_eq!(got, payloads, "cmd_seq={cmd_seq} 还原包序列一致");
        }
    }

    // ---------- 单元：>16MB 命令多包拼接（P 项：超大单语句 INSERT 分包） ----------

    #[test]
    fn read_command_joins_multi_packet_and_returns_next_seq() {
        use std::io::{Read, Write};
        use std::net::{TcpListener, TcpStream};
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let h = std::thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            read_command(&mut s).unwrap()
        });
        let mut c = TcpStream::connect(addr).unwrap();
        let big = vec![7u8; 0xFFFFFF]; // 满包（16MB-1）→ 触发续包
        let tail = vec![9u8; 100];
        write_packet(&mut c, 0, &big).unwrap();
        write_packet(&mut c, 1, &tail).unwrap();
        let (seq, payload) = h.join().unwrap();
        assert_eq!(payload.len(), 0xFFFFFF + 100, "应拼接两包为完整命令");
        assert_eq!(&payload[..3], &[7, 7, 7]);
        assert_eq!(payload[0xFFFFFF], 9, "续包内容应接在第一包后");
        assert_eq!(seq, 2, "响应 seq 应接在请求最后一包(1)之后");
        // 单包命令（seq 0）→ 响应从 1 起（既有行为不回退）
        let l2 = TcpListener::bind("127.0.0.1:0").unwrap();
        let a2 = l2.local_addr().unwrap();
        let h2 = std::thread::spawn(move || {
            let (mut s, _) = l2.accept().unwrap();
            read_command(&mut s).unwrap()
        });
        let mut c2 = TcpStream::connect(a2).unwrap();
        write_packet(&mut c2, 0, &vec![1u8; 50]).unwrap();
        let (seq2, p2) = h2.join().unwrap();
        assert_eq!(seq2, 1);
        assert_eq!(p2.len(), 50);
    }

    // ---------- 单元：认证 ----------

    #[test]
    fn native_password_accepts_correct_and_rejects_wrong() {
        let scramble = [7u8; 20];
        let pw = "secret";
        // 构造合法 token
        let stage1 = Sha1::digest(pw.as_bytes());
        let stage2 = Sha1::digest(&stage1);
        let mut h = Sha1::new();
        h.update(&scramble);
        h.update(stage2);
        let crypto = h.finalize();
        let mut token = [0u8; 20];
        for i in 0..20 {
            token[i] = stage1[i] ^ crypto[i];
        }
        assert!(check_native_password(&token, &scramble, pw));
        token[0] ^= 0xff;
        assert!(!check_native_password(&token, &scramble, pw));
        // 空密码：响应为空 → 接受
        assert!(check_native_password(&[], &scramble, ""));
    }

    // ---------- 单元：SELECT 列投影（结果集裁剪） ----------

    #[test]
    fn parse_projection_variants() {
        use ProjCol::{Doc, Field, Id};
        // id 单列 → 主键点查/范围只回 id（响应最小化）
        assert_eq!(parse_projection("SELECT id FROM orders WHERE id=1"), Some(vec![Id]));
        // 反引号 / 大小写 / docid 别名
        assert_eq!(
            parse_projection("select `id` from orders where id=2"),
            Some(vec![Id])
        );
        assert_eq!(parse_projection("SELECT docid FROM orders WHERE id=3"), Some(vec![Id]));
        // 显式双列 / 保书写顺序
        assert_eq!(parse_projection("SELECT id, doc FROM orders"), Some(vec![Id, Doc]));
        assert_eq!(parse_projection("SELECT doc, id FROM orders"), Some(vec![Doc, Id]));
        // * → id + doc
        assert_eq!(parse_projection("SELECT * FROM orders WHERE id=4"), Some(vec![Id, Doc]));
        // 字段级：doc 顶层 JSON 字段按书写顺序裁剪（不再回退双列）
        assert_eq!(
            parse_projection("SELECT status, city FROM orders WHERE id=5"),
            Some(vec![Field("status".into()), Field("city".into())])
        );
        assert_eq!(
            parse_projection("SELECT id, status FROM orders WHERE id=6"),
            Some(vec![Id, Field("status".into())])
        );
        // 回退场景：函数 / DISTINCT / 别名 / 无 FROM
        assert_eq!(parse_projection("SELECT COUNT(id) FROM orders"), None);
        assert_eq!(
            parse_projection("SELECT DISTINCT c FROM sbtest WHERE id BETWEEN 1 AND 9"),
            None
        );
        assert_eq!(parse_projection("SELECT c AS x FROM sbtest WHERE id=1"), None);
        assert_eq!(parse_projection("SELECT id"), None);
        // 解析只看 SELECT 与 FROM 之间（带 ORDER BY / LIMIT 不影响）
        assert_eq!(
            parse_projection("SELECT id FROM orders WHERE id BETWEEN 1 AND 9 ORDER BY id LIMIT 5"),
            Some(vec![Id])
        );
    }

    #[test]
    fn projection_row_and_columns() {
        use ProjCol::{Doc, Field, Id};
        let doc = br#"{"status":"active","amount":10}"#;
        // id-only：1 列
        let QueryResponse::Set { columns, rows } =
            build_result_set(Some(&[Id]), vec![(7, doc.to_vec())], false, None)
        else {
            panic!("应为 ResultSet");
        };
        assert_eq!(columns.len(), 1);
        assert_eq!(rows[0], vec![b"7".to_vec()]);
        // doc+id 保序
        let QueryResponse::Set { rows: rows2, .. } =
            build_result_set(Some(&[Doc, Id]), vec![(7, doc.to_vec())], false, None)
        else {
            panic!("应为 ResultSet");
        };
        assert_eq!(rows2[0], vec![doc.to_vec(), b"7".to_vec()]);
        // None（* / 回退）→ id + doc
        let QueryResponse::Set { rows: rows3, .. } =
            build_result_set(None, vec![(7, doc.to_vec())], false, None)
        else {
            panic!("应为 ResultSet");
        };
        assert_eq!(rows3[0], vec![b"7".to_vec(), doc.to_vec()]);
        // 字段级取值（status 字符串 / amount 数字文本）
        let QueryResponse::Set { rows: rows4, .. } = build_result_set(
            Some(&[Field("status".into()), Field("amount".into())]),
            vec![(7, doc.to_vec())],
            false,
            None,
        ) else {
            panic!("应为 ResultSet");
        };
        assert_eq!(rows4[0], vec![b"active".to_vec(), b"10".to_vec()]);
        // 列数 = 投影列数
        let QueryResponse::Set { columns: c5, .. } =
            build_result_set(Some(&[Field("status".into())]), Vec::new(), false, None)
        else {
            panic!("应为 ResultSet");
        };
        assert_eq!(c5.len(), 1);
    }

    /// 解析 ColumnDefinition41 中的列类型字节（测试辅助：跳过 6 个 lenenc 字符串
    /// + 0x0c 固定字段长度标记 + charset + column_length）。
    fn col_payload_type(col: &[u8]) -> u8 {
        let mut pos = 0usize;
        for _ in 0..6 {
            let l = col[pos] as usize; // 名称均为短 ASCII（<251）
            pos += 1 + l;
        }
        pos += 1; // 0x0c fixed-fields length 标记
        pos += 2; // charset
        pos += 4; // column_length
        col[pos] // 其后为 type
    }

    #[test]
    fn field_column_type_inference() {
        use ProjCol::Field;
        // 整数字段 → LONGLONG
        let QueryResponse::Set { columns, .. } = build_result_set(
            Some(&[Field("amount".into())]),
            vec![(1, br#"{"amount":10}"#.to_vec()), (2, br#"{"amount":99}"#.to_vec())],
            false,
            None,
        ) else {
            panic!("应为 ResultSet");
        };
        assert_eq!(col_payload_type(&columns[0]), MYSQL_TYPE_LONGLONG);
        // 浮点字段 → DOUBLE
        let QueryResponse::Set { columns: c2, .. } = build_result_set(
            Some(&[Field("price".into())]),
            vec![(1, br#"{"price":1.5}"#.to_vec()), (2, br#"{"price":2.75}"#.to_vec())],
            false,
            None,
        ) else {
            panic!("应为 ResultSet");
        };
        assert_eq!(col_payload_type(&c2[0]), MYSQL_TYPE_DOUBLE);
        // 字符串字段 → VAR_STRING
        let QueryResponse::Set { columns: c3, .. } = build_result_set(
            Some(&[Field("status".into())]),
            vec![(1, br#"{"status":"a"}"#.to_vec()), (2, br#"{"status":"b"}"#.to_vec())],
            false,
            None,
        ) else {
            panic!("应为 ResultSet");
        };
        assert_eq!(col_payload_type(&c3[0]), MYSQL_TYPE_VAR_STRING);
        // 全缺失（NULL）→ VAR_STRING（最保守）
        let QueryResponse::Set { columns: c4, .. } = build_result_set(
            Some(&[Field("nope".into())]),
            vec![(1, br#"{"a":1}"#.to_vec()), (2, br#"{"a":2}"#.to_vec())],
            false,
            None,
        ) else {
            panic!("应为 ResultSet");
        };
        assert_eq!(col_payload_type(&c4[0]), MYSQL_TYPE_VAR_STRING);
        // 数字 + 缺失混合：缺失行 NULL 不参与降级 → 仍 LONGLONG
        let QueryResponse::Set { columns: c5, .. } = build_result_set(
            Some(&[Field("amount".into())]),
            vec![(1, br#"{"amount":10}"#.to_vec()), (2, br#"{"other":1}"#.to_vec())],
            false,
            None,
        ) else {
            panic!("应为 ResultSet");
        };
        assert_eq!(col_payload_type(&c5[0]), MYSQL_TYPE_LONGLONG);
    }

    #[test]
    fn doc_field_missing_null_and_case() {
        // 缺失字段 → NULL 哨兵（0xfb）+ Null kind
        let obj = |b: &[u8]| -> Option<serde_json::Map<String, serde_json::Value>> {
            match serde_json::from_slice::<serde_json::Value>(b) {
                Ok(serde_json::Value::Object(m)) => Some(m),
                _ => None,
            }
        };
        let (k, cell) = doc_field_kind_cell(obj(br#"{"a":1}"#).as_ref(), "b");
        assert_eq!(k, ValKind::Null);
        assert_eq!(cell, vec![MYSQL_NULL_CELL]);
        // JSON null → NULL；int/float/bool/str kind 归类
        assert_eq!(doc_field_kind_cell(obj(br#"{"a":null}"#).as_ref(), "a").0, ValKind::Null);
        assert_eq!(doc_field_kind_cell(obj(br#"{"a":5}"#).as_ref(), "a").0, ValKind::Int);
        assert_eq!(doc_field_kind_cell(obj(br#"{"a":5.5}"#).as_ref(), "a").0, ValKind::Float);
        assert_eq!(doc_field_kind_cell(obj(br#"{"a":true}"#).as_ref(), "a").0, ValKind::Bool);
        assert_eq!(doc_field_kind_cell(obj(br#"{"a":"x"}"#).as_ref(), "a").0, ValKind::Str);
        // 大小写容错
        assert_eq!(
            doc_field_kind_cell(obj(br#"{"status":"ok"}"#).as_ref(), "status").1,
            b"ok".to_vec()
        );
        assert_eq!(
            doc_field_kind_cell(obj(br#"{"Name":"x"}"#).as_ref(), "name").1,
            b"x".to_vec()
        );
        // 布尔 → 1/0；数组 → JSON 文本
        assert_eq!(
            doc_field_kind_cell(obj(br#"{"on":true,"off":false}"#).as_ref(), "on").1,
            b"1".to_vec()
        );
        assert_eq!(
            doc_field_kind_cell(obj(br#"{"on":true,"off":false}"#).as_ref(), "off").1,
            b"0".to_vec()
        );
        assert_eq!(
            doc_field_kind_cell(obj(br#"{"tags":["a","b"]}"#).as_ref(), "tags").1,
            b"[\"a\",\"b\"]".to_vec()
        );
        // 非对象 doc → NULL
        assert_eq!(doc_field_kind_cell(None, "f").0, ValKind::Null);
        // 嵌套点路径：逐层下钻（大小写容错逐层生效）
        assert_eq!(
            doc_field_kind_cell(
                obj(br#"{"addr":{"city":"bj","geo":{"lat":1.5}}}"#).as_ref(),
                "addr.city"
            )
            .1,
            b"bj".to_vec()
        );
        assert_eq!(
            doc_field_kind_cell(
                obj(br#"{"addr":{"city":"bj","geo":{"lat":1.5}}}"#).as_ref(),
                "addr.geo.lat"
            )
            .0,
            ValKind::Float
        );
        // 缺失嵌套 / 中间非对象（下钻到字符串值）→ NULL
        assert_eq!(
            doc_field_kind_cell(obj(br#"{"addr":{"city":"bj"}}"#).as_ref(), "addr.zip").0,
            ValKind::Null
        );
        assert_eq!(
            doc_field_kind_cell(obj(br#"{"addr":{"city":"bj"}}"#).as_ref(), "addr.city.deep").0,
            ValKind::Null
        );
    }

    #[test]
    fn projection_end_to_end_nested_field() {
        let mut engine = test_engine();
        engine
            .put(1, br#"{"addr":{"city":"bj","geo":{"lat":1.5}},"name":"n1"}"#.to_vec(), &[])
            .unwrap();
        // 嵌套字段投影：SELECT name, addr.city → 列名取最后一段（city）
        let resp = select_response(&engine, "SELECT name, addr.city FROM documents WHERE id=1");
        let QueryResponse::Set { columns, rows } = resp else {
            panic!("应为 ResultSet");
        };
        assert_eq!(columns.len(), 2);
        assert!(
            columns[1].windows(4).any(|w| w == b"city"),
            "嵌套列头应为最后一段 city"
        );
        assert_eq!(rows[0][0], b"n1");
        assert_eq!(rows[0][1], b"bj");
        // 缺失深层 → NULL
        let resp2 = select_response(&engine, "SELECT addr.zip FROM documents WHERE id=1");
        let QueryResponse::Set { rows: r2, .. } = resp2 else {
            panic!("应为 ResultSet");
        };
        assert_eq!(r2[0][0], vec![MYSQL_NULL_CELL]);
        // 嵌套浮点 → DOUBLE 类型 + 值
        let resp3 = select_response(&engine, "SELECT addr.geo.lat FROM documents WHERE id=1");
        let QueryResponse::Set { columns: c3, rows: r3 } = resp3 else {
            panic!("应为 ResultSet");
        };
        assert_eq!(col_payload_type(&c3[0]), MYSQL_TYPE_DOUBLE);
        assert_eq!(r3[0][0], b"1.5");
    }

    #[test]
    fn projection_end_to_end_point_query_returns_single_id_column() {
        // 端到端：真实引擎 + `SELECT id` 点查 → 结果集仅 1 列 id（不再夹带整 doc 回包）
        let mut engine = test_engine();
        let doc = br#"{"status":"active","city":"beijing","amount":88}"#;
        engine.put(42, doc.to_vec(), &[]).unwrap();
        let resp = select_response(&engine, "SELECT id FROM documents WHERE id=42");
        let QueryResponse::Set { columns, rows } = resp else {
            panic!("应为 ResultSet");
        };
        assert_eq!(columns.len(), 1, "SELECT id 只应声明 1 列");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 1);
        assert_eq!(rows[0][0], b"42");
        // 对照：SELECT * 仍 id+doc 双列（行为不变）
        let resp2 = select_response(&engine, "SELECT * FROM documents WHERE id=42");
        let QueryResponse::Set { columns: c2, rows: r2 } = resp2 else {
            panic!("应为 ResultSet");
        };
        assert_eq!(c2.len(), 2);
        assert_eq!(r2[0].len(), 2);
        assert_eq!(r2[0][0], b"42");
        assert_eq!(r2[0][1], doc);
    }

    #[test]
    fn projection_end_to_end_field_columns() {
        // 端到端：字段级投影——`SELECT status, city` 只回两字段列（缺 id/doc）
        let mut engine = test_engine();
        engine
            .put(42, br#"{"status":"active","city":"beijing","amount":88}"#.to_vec(), &[])
            .unwrap();
        let resp = select_response(&engine, "SELECT status, city FROM documents WHERE id=42");
        let QueryResponse::Set { columns, rows } = resp else {
            panic!("应为 ResultSet");
        };
        assert_eq!(columns.len(), 2);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![b"active".to_vec(), b"beijing".to_vec()]);
        // 缺失字段 → NULL（0xfb），不报错
        let resp2 = select_response(&engine, "SELECT title, city FROM documents WHERE id=42");
        let QueryResponse::Set { rows: r2, .. } = resp2 else {
            panic!("应为 ResultSet");
        };
        assert_eq!(r2[0][0], vec![MYSQL_NULL_CELL], "缺失字段应为 NULL");
        assert_eq!(r2[0][1], b"beijing".to_vec());
        // 混合：id + 字段
        let resp3 = select_response(&engine, "SELECT id, amount FROM documents WHERE id=42");
        let QueryResponse::Set { columns: c3, rows: r3 } = resp3 else {
            panic!("应为 ResultSet");
        };
        assert_eq!(c3.len(), 2);
        assert_eq!(r3[0], vec![b"42".to_vec(), b"88".to_vec()]);
    }

    #[test]
    fn projection_between_order_by_limit() {
        let mut engine = test_engine();
        for i in 0..3u64 {
            engine
                .put(i, format!(r#"{{"st":"s{i}"}}"#).into_bytes(), &[])
                .unwrap();
        }
        // SELECT id ... BETWEEN 1 AND 2 ORDER BY id → 只回 id、按 docid 升序
        let resp = select_response(
            &engine,
            "SELECT id FROM documents WHERE id BETWEEN 1 AND 2 ORDER BY id",
        );
        let QueryResponse::Set { columns, rows } = resp else {
            panic!("应为 ResultSet");
        };
        assert_eq!(columns.len(), 1);
        let got: Vec<&[u8]> = rows.iter().map(|r| r[0].as_slice()).collect();
        assert_eq!(got, vec![b"1", b"2"]);
    }

    #[test]
    fn gap1_point_and_in_projection_pushdown_matches_whole_row_path() {
        // 缺口①（P105-①）：点查/IN 投影下推（batch_get_fields / get_many_pk_in_fields）
        // 输出结果集须与整行路径（engine.get / get_many_pk_in → build_result_set）逐行一致——
        // PAX(hot_fields) 布局 + flush 落盘（SST PAX 列解码路径；非 hot 冷列 / 缺字段 /
        // JSON null / 转义字符串 / 删除位图隐藏 / 不存在的 id）。
        let dir = tempfile::tempdir().unwrap();
        let mut c = crate::config::Config::default();
        c.storage.hot_fields = vec!["status".into(), "city".into(), "amount".into()];
        let mut engine = Engine::open(dir.path(), &c).unwrap();
        for i in 1..=60u64 {
            let doc = serde_json::json!({
                "status": format!("s{}", i % 5),
                "city": format!("c{}", i),
                "amount": (i as i64) * 10,
                "k": i, // 非 hot 冷列（SST 冷列解码路径）
                "pad": format!("pad-{i}"),
            });
            engine
                .put(i, serde_json::to_vec(&doc).unwrap(), &["status"])
                .unwrap();
        }
        // 转义字符串 + 缺若干投影列 + JSON null（62 缺 city/amount）
        engine
            .put(61, br#"{"status":"active","note":"x\"y\\z","extra":null}"#.to_vec(), &[])
            .unwrap();
        engine.delete(7).unwrap();
        engine.flush_primary().unwrap();

        let proj = |sql: &str| -> Option<Vec<ProjCol>> { parse_projection(sql) };
        // 参考路径 = 旧实现同构：整行取回 → build_result_set 常规裁剪
        let ref_rows = |engine: &Engine, sql: &str, raw: Vec<(u64, Vec<u8>)>| {
            build_result_set(
                proj(sql).as_deref(),
                raw,
                sql.to_uppercase().contains("ORDER BY"),
                extract_limit(sql),
            )
        };
        let assert_same_set = |a: QueryResponse, b: QueryResponse, tag: &str| {
            let (c1, r1) = match a {
                QueryResponse::Set { columns, rows } => (columns, rows),
                _ => panic!("{tag}: a 应为 ResultSet"),
            };
            let (c2, r2) = match b {
                QueryResponse::Set { columns, rows } => (columns, rows),
                _ => panic!("{tag}: b 应为 ResultSet"),
            };
            assert_eq!(c1, c2, "{tag}: 列定义须一致");
            assert_eq!(r1, r2, "{tag}: 行内容须一致");
        };

        // 点查（单行）
        for id in [1u64, 42, 7, 61, 62, 9000] {
            let sql = format!(
                "SELECT id, status, city, amount, k FROM documents WHERE id={id}"
            );
            let push = select_response(&engine, &sql);
            let raw = match engine.get(id) {
                Ok(Some(v)) => vec![(id, v)],
                _ => Vec::new(),
            };
            let whole = ref_rows(&engine, &sql, raw);
            assert_same_set(push, whole, &format!("点查投影 id={id}"));
        }
        // IN（多行；含缺失 id 9000 / 已删 7 / 转义 61 / 缺列 62 / 无 extra 投影列）
        let sql =
            "SELECT id, status, city, amount, k FROM documents WHERE id IN (42,7,61,62,9000,42)";
        let push = select_response(&engine, sql);
        let raw = {
            let docids: Vec<u64> = extract_target_ids(sql)
                .unwrap()
                .into_iter()
                .map(|i| i) // 默认表 docid = row id
                .collect();
            let mut raw: Vec<(u64, Vec<u8>)> = Vec::new();
            if let Ok(found) = engine.get_many_pk_in(&docids) {
                for d in docids {
                    if let Some(v) = found.get(&d) {
                        raw.push((d, v.clone()));
                    }
                }
            }
            raw
        };
        let whole = ref_rows(&engine, sql, raw);
        assert_same_set(push, whole, "IN 投影");
        // 回退护栏：整 doc 列 / 嵌套路径保持整行路径（语义不变）
        let sql2 = "SELECT id, doc FROM documents WHERE id=42";
        let a = select_response(&engine, sql2);
        let b = {
            let raw = match engine.get(42) {
                Ok(Some(v)) => vec![(42, v)],
                _ => Vec::new(),
            };
            ref_rows(&engine, sql2, raw)
        };
        assert_same_set(a, b, "SELECT id,doc 整行直通");
        let sql3 = "SELECT note, extra FROM documents WHERE id=61";
        let a3 = select_response(&engine, sql3);
        let b3 = {
            let raw = match engine.get(61) {
                Ok(Some(v)) => vec![(61, v)],
                _ => Vec::new(),
            };
            ref_rows(&engine, sql3, raw)
        };
        assert_same_set(a3, b3, "JSON null / 转义投影");
    }

    // ---------- 单元：SQL 解析 ----------

    #[test]
    fn parse_insert_with_columns_and_plain() {
        let (id, doc) = parse_insert(
            "INSERT INTO documents (id, doc) VALUES (42, '{\"a\":1}')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(id, 42);
        assert_eq!(doc, r#"{"a":1}"#);
        let (id2, doc2) = parse_insert("INSERT INTO documents VALUES (7, 'x')")
            .unwrap()
            .unwrap();
        assert_eq!(id2, 7);
        assert_eq!(doc2, "x");
        // H-6：sysbench 风格多列（非 id 列组装 JSON 文档）
        let (id3, doc3) = parse_insert(
            "INSERT INTO sbtest1 (id, k, c, pad) VALUES (3, 500, 'hello', 'world')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(id3, 3);
        let v: serde_json::Value = serde_json::from_str(&doc3).unwrap();
        assert_eq!(v["k"], 500);
        assert_eq!(v["c"], "hello");
        assert_eq!(v["pad"], "world");
    }

    #[test]
    fn parse_insert_multi_rows() {
        // 多行 VALUES（sysbench --insert-multiple-rows 风格）
        let rows = parse_insert_multi(
            "INSERT INTO sbtest1 (id, k, c, pad) VALUES (1, 100, 'a', 'x'),(2, 200, 'b', 'y'),(3, 300, 'c', 'z')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[2].0, 3);
        let v2: serde_json::Value = serde_json::from_str(&rows[1].1).unwrap();
        assert_eq!(v2["k"], 200);
        assert_eq!(v2["c"], "b");
        // 单行兼容：parse_insert 取首行
        let (id, _) = parse_insert("INSERT INTO sbtest1 (id, k, c, pad) VALUES (9, 1, 'a', 'b')")
            .unwrap()
            .unwrap();
        assert_eq!(id, 9);
        // 多行内嵌逗号/引号不拆错
        let rows2 = parse_insert_multi(
            "INSERT INTO t (id, doc) VALUES (1, '{\"a\":1,\"b\":2}'),(2, 'hello, world')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(rows2.len(), 2);
        assert_eq!(rows2[0].1, r#"{"a":1,"b":2}"#);
        assert_eq!(rows2[1].1, "hello, world");
        // sysbench 兼容：无 id 列（auto_increment）→ id=0 标记，调用方自动分配
        let rows3 = parse_insert_multi(
            "INSERT INTO sbtest1 (k, c, pad) VALUES (1, 'a', 'b'),(2, 'c', 'd')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(rows3.len(), 2);
        assert_eq!(rows3[0].0, 0);
        assert_eq!(rows3[1].0, 0);
        let v3: serde_json::Value = serde_json::from_str(&rows3[0].1).unwrap();
        assert_eq!(v3["k"], 1);
        assert_eq!(v3["c"], "a");
    }

    #[test]
    fn ex91_single_eq_count_field_parses() {
        // 模板命中
        assert_eq!(
            single_eq_count_field("SELECT COUNT(*) FROM orders WHERE status='active'"),
            Some(("status".into(), "active".into()))
        );
        assert_eq!(
            single_eq_count_field("SELECT COUNT(*) FROM orders WHERE city = 'beijing'"),
            Some(("city".into(), "beijing".into()))
        );
        // 引号转义（'' → '）
        assert_eq!(
            single_eq_count_field("SELECT COUNT(*) FROM t WHERE name='o''brien'")
                .unwrap()
                .1,
            "o'brien"
        );
        // 边界否定 → None（回落全扫，语义不变）
        assert!(single_eq_count_field("SELECT COUNT(*) FROM orders").is_none(), "无 WHERE");
        assert!(single_eq_count_field("SELECT COUNT(*) FROM orders WHERE id=5").is_none(), "主键");
        assert!(single_eq_count_field("SELECT COUNT(*) FROM orders WHERE amount>90000").is_none(), "比较");
        assert!(single_eq_count_field("SELECT COUNT(*) FROM orders WHERE status='active' AND amount>1").is_none(), "多条件");
        assert!(single_eq_count_field("SELECT SUM(amount) FROM orders WHERE status='active'").is_none(), "非 COUNT");
        assert!(single_eq_count_field("SELECT COUNT(*) FROM orders WHERE status=active").is_none(), "无引号值");
    }

    #[test]
    fn ex91_inverted_count_fast_matches_full_scan() {
        // Ex-9.1：单字段等值 COUNT 走倒排计数（flush pending + doc_count，亚毫秒）
        // 数值与 7.95 全扫聚合一致；未声明字段不可路由（防把"未建索引"误报为 0）。
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.sstable.compression = "none".into();
        cfg.inverted.inverted_fields = vec!["status".to_string(), "city".to_string()];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let statuses = ["active", "pending", "active", "closed", "active", "pending"];
        for (i, s) in statuses.iter().enumerate() {
            let doc = format!(r#"{{"status":"{s}","city":"beijing","amount":{}}}"#, i * 10);
            let terms = vec![format!("status={s}")];
            let refs: Vec<&str> = terms.iter().map(|t| t.as_str()).collect();
            e.put(i as u64 + 1, doc.into_bytes(), &refs).unwrap();
        }
        let q = "SELECT COUNT(*) FROM orders WHERE status='active'";
        let resp = try_count_fast(&mut e, q).expect("白名单字段应命中快路径");
        let n_fast = match resp {
            QueryResponse::Set { rows, .. } => {
                String::from_utf8(rows[0][0].clone()).unwrap().parse::<u64>().unwrap()
            }
            _ => panic!("快路径应返回结果集"),
        };
        assert_eq!(n_fast, 3, "active 计数 = 3");
        // eligible 判定：声明字段可路由、未声明不可
        assert!(e.inverted_count_eligible("status"));
        assert!(e.inverted_count_eligible("city"));
        assert!(!e.inverted_count_eligible("title"), "未声明字段不可路由");
        // 与全扫聚合一致（7.95 路径）
        let agg = crate::sqlish::execute_aggregate(&e, q)
            .unwrap()
            .expect("全扫聚合");
        assert_eq!(agg.text, "3", "快路径与全扫数值一致");
    }

    #[test]
    fn group_by_select_response_multi_row_result_set() {
        // AF#2 协议层：GROUP BY 走 select_response → 多行分组结果集
        //（首列组字段 + COUNT/SUM 聚合列；NULL 组键 → 0xfb 标记单元格）。
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.sstable.compression = "none".into();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let rows_in = [("bj", 10), ("bj", 20), ("sh", 5), ("sh", 15), ("gz", 99)];
        for (i, (city, amount)) in rows_in.iter().enumerate() {
            let doc = format!(r#"{{"city":"{city}","amount":{amount}}}"#);
            let refs: Vec<&str> = Vec::new();
            e.put(i as u64 + 1, doc.into_bytes(), &refs).unwrap();
        }
        match select_response(&e, "SELECT city, COUNT(*), SUM(amount) FROM documents GROUP BY city") {
            QueryResponse::Set { columns, rows } => {
                assert_eq!(columns.len(), 3, "组字段列 + 2 聚合列");
                assert_eq!(rows.len(), 3, "bj/sh/gz 三组");
                let keys: Vec<String> =
                    rows.iter().map(|r| String::from_utf8(r[0].clone()).unwrap()).collect();
                assert_eq!(keys, vec!["bj", "gz", "sh"], "组键字符串升序");
                assert_eq!(String::from_utf8(rows[0][1].clone()).unwrap(), "2");
                assert_eq!(String::from_utf8(rows[0][2].clone()).unwrap(), "30");
                assert_eq!(String::from_utf8(rows[1][1].clone()).unwrap(), "1");
                assert_eq!(String::from_utf8(rows[1][2].clone()).unwrap(), "99");
                assert_eq!(String::from_utf8(rows[2][1].clone()).unwrap(), "2");
                assert_eq!(String::from_utf8(rows[2][2].clone()).unwrap(), "20");
            }
            _ => panic!("GROUP BY 应返回多行结果集"),
        }
        // 缺省字段 → 单 NULL 组；组键单元格 = NULL 标记
        match select_response(&e, "SELECT missing, COUNT(*) FROM documents GROUP BY missing") {
            QueryResponse::Set { columns, rows } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(rows.len(), 1, "全缺省并为一组");
                assert_eq!(rows[0][0], vec![MYSQL_NULL_CELL]);
                assert_eq!(String::from_utf8(rows[0][1].clone()).unwrap(), "5");
            }
            _ => panic!("GROUP BY 缺省字段应返回结果集"),
        }
    }

    #[test]
    fn ex91_bitmap_field_route_precise_small_values() {
        // Ex-9.1：bitmap_fields 字段走内存位图（写路径同步维护）——多值各自精确、未建字段不路由。
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.sstable.compression = "none".into();
        cfg.inverted.bitmap_fields = vec!["status".to_string()];
        cfg.inverted.inverted_fields = vec!["status".to_string(), "city".to_string()];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        for (i, s) in ["active", "active", "pending", "active"].iter().enumerate() {
            let doc = format!(r#"{{"status":"{s}"}}"#);
            let terms = vec![format!("status={s}")];
            let refs: Vec<&str> = terms.iter().map(|t| t.as_str()).collect();
            e.put(i as u64 + 1, doc.into_bytes(), &refs).unwrap();
        }
        assert!(e.inverted_count_eligible("status"), "bitmap 字段可路由");
        let q = "SELECT COUNT(*) FROM orders WHERE status='active'";
        let resp = try_count_fast(&mut e, q).expect("bitmap 字段命中快路径");
        let n = match resp {
            QueryResponse::Set { rows, .. } => {
                String::from_utf8(rows[0][0].clone()).unwrap().parse::<u64>().unwrap()
            }
            _ => panic!("结果集"),
        };
        assert_eq!(n, 3, "bitmap 计数 = active 3（写入即精确，无 pending 延迟）");
    }

    #[test]
    fn extract_target_ids_point_range_in() {
        // 点查
        assert_eq!(
            extract_target_ids("SELECT c FROM sbtest1 WHERE id=42").unwrap(),
            vec![42]
        );
        assert_eq!(
            extract_target_ids("SELECT c FROM sbtest WHERE id=7 ORDER BY c").unwrap(),
            vec![7]
        );
        // BETWEEN 闭区间
        let ids = extract_target_ids(
            "SELECT c FROM sbtest WHERE id BETWEEN 100 AND 103",
        )
        .unwrap();
        assert_eq!(ids, vec![100, 101, 102, 103]);
        // IN 多点
        let ids2 = extract_target_ids(
            "SELECT c FROM sbtest WHERE id IN (5, 9, 12)",
        )
        .unwrap();
        assert_eq!(ids2, vec![5, 9, 12]);
        // LIMIT 提取
        assert_eq!(extract_limit("SELECT c FROM sbtest WHERE id BETWEEN 1 AND 5 ORDER BY c LIMIT 10"), Some(10));
        // 不支持 → None
        assert!(extract_target_ids("SELECT * FROM sbtest WHERE status='a'").is_none());
        // 7.93 回归：非 id 字段 BETWEEN 不得被当 docid 窗口（旧实现 find("between") 吞掉 amount/其他列）
        assert!(
            extract_target_ids("SELECT id FROM orders WHERE amount BETWEEN 50000 AND 50005").is_none(),
            "amount BETWEEN 应落到 sqlish 字段过滤，而非 docid 窗口"
        );
        assert!(
            extract_target_ids("SELECT id FROM orders WHERE status='a' AND amount BETWEEN 1 AND 2").is_none()
        );
    }

    // ---------- 单元：SET TRANSACTION ISOLATION LEVEL ----------

    #[test]
    fn parse_isolation_level_variants() {
        use crate::txn::Isolation;
        assert_eq!(
            parse_isolation_level("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
            Some(Isolation::RepeatableRead)
        );
        assert_eq!(
            parse_isolation_level("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
            Some(Isolation::Serializable)
        );
        assert_eq!(
            parse_isolation_level("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"),
            Some(Isolation::ReadCommitted)
        );
        // READ UNCOMMITTED 未单独实现 → 映射 READ COMMITTED（无脏读语义）
        assert_eq!(
            parse_isolation_level("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"),
            Some(Isolation::ReadCommitted)
        );
        // 非隔离级别 SET → None（调用方忽略返回 OK）
        assert_eq!(parse_isolation_level("SET autocommit=1"), None);
        assert_eq!(parse_isolation_level("SET NAMES utf8mb4"), None);
    }

    #[test]
    fn unquote_reverses_sql_escape_sequences() {
        // H 项遗留缺陷修复：pymysql 参数化把 `"` `\` 转义为 `\"` `\\`，须反转义回原值
        assert_eq!(unquote(r#"'{\"v\":1}'"#), r#"{"v":1}"#);
        assert_eq!(unquote(r"'a\\b'"), r"a\b");
        assert_eq!(unquote(r"'it\'s'"), "it's");
        assert_eq!(unquote(r#""hello""#), "hello");
        assert_eq!(unquote("'a\\nb'"), "a\nb");
        // 未加引号原样（去除首尾空白）
        assert_eq!(unquote("plain"), "plain");
    }

    fn extract_between_range_variants() {
        // 标准 BETWEEN
        assert_eq!(
            extract_between_range("SELECT c FROM sbtest WHERE id BETWEEN 100 AND 200"),
            Some((100, 200))
        );
        // 带 ORDER BY / LIMIT
        assert_eq!(
            extract_between_range("SELECT c FROM sbtest WHERE id BETWEEN 5 AND 9 ORDER BY c LIMIT 10"),
            Some((5, 9))
        );
        // 非 BETWEEN → None
        assert_eq!(extract_between_range("SELECT c FROM sbtest WHERE id=42"), None);
        assert_eq!(extract_between_range("SELECT c FROM sbtest WHERE id IN (1,2,3)"), None);
        assert_eq!(extract_between_range("SELECT c FROM sbtest WHERE k BETWEEN 1 AND 5"), None); // k 列非 id
    }

    #[test]
    fn update_delete_where_in() {
        // UPDATE/DELETE … WHERE id IN / <字段条件>（MySQL 命令名一致，逐行执行，返回影响行数）
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut e = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        let put = |e: &mut crate::engine::Engine, id: u64, status: &str| {
            let doc = format!("{{\"status\":\"{status}\",\"v\":{id}}}");
            let terms = super::doc_terms(&doc).unwrap();
            let refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put(id, doc.as_bytes().to_vec(), &refs).unwrap();
        };
        for i in 1..=4u64 {
            put(&mut e, i, &format!("a{i}"));
        }
        // UPDATE … WHERE id IN (…) → 影响 2 行
        match super::update_response(&mut e, "UPDATE documents SET status='x' WHERE id IN (1, 3)") {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 2),
            super::QueryResponse::Err(_, _) => panic!("update IN 失败"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        for id in [1u64, 3] {
            let d = e.get(id).unwrap().unwrap();
            assert!(String::from_utf8(d).unwrap().contains("\"status\":\"x\""));
        }
        assert!(!String::from_utf8(e.get(2).unwrap().unwrap()).unwrap().contains("\"status\":\"x\""));
        // DELETE … WHERE <字段条件>（sqlish 解析命中 docid=1,3）→ 影响 2 行
        match super::delete_response(&mut e, "DELETE FROM documents WHERE status='x'") {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 2),
            super::QueryResponse::Err(_, _) => panic!("delete 字段条件失败"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        assert!(e.get(1).unwrap().is_none());
        assert!(e.get(3).unwrap().is_none());
        assert!(e.get(2).unwrap().is_some());
        // DELETE … WHERE id IN (…) → 影响 2 行；无匹配字段条件 → 0 行
        match super::delete_response(&mut e, "DELETE FROM documents WHERE id IN (2, 4, 99)") {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 3), // 直解语义：同单 id 不检查存在性
            super::QueryResponse::Err(_, _) => panic!("delete id IN 失败"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        match super::delete_response(&mut e, "DELETE FROM documents WHERE status='gone'") {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 0),
            super::QueryResponse::Err(_, _) => panic!("delete 空匹配失败"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        assert!(e.get(2).unwrap().is_none());
    }

    #[test]
    fn delete_pk_between_range_batch() {
        // B2（delete_range50 修复）：`DELETE … WHERE id BETWEEN a AND b` → 主键区间
        // keys-only 扫描 + delete_batch（单次提交）。验证：
        // ① 影响行数 = 区间内现存行；② 删除后区间外行保留；③ 缺行不计数（对齐 MySQL）；
        // ④ 多表场景只删目标表区间（他表同 row_id 不受影响）。
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut e = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        let put = |e: &mut crate::engine::Engine, id: u64| {
            let doc = format!("{{\"v\":{id}}}");
            e.put(id, doc.as_bytes().to_vec(), &[]).unwrap();
        };
        // documents（tid=0）：1..=10 与 21..=30，中间 11..=20 缺行
        for i in (1..=10u64).chain(21..=30u64) {
            put(&mut e, i);
        }
        // ① 区间 [5,15]：现存 5..=10（6 行）→ 影响 6
        match super::delete_response(&mut e, "DELETE FROM documents WHERE id BETWEEN 5 AND 15") {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 6, "区间内现存行数"),
            super::QueryResponse::Err(c, m) => panic!("delete BETWEEN 失败: {c} {m}"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        for i in 5..=10u64 {
            assert!(e.get(i).unwrap().is_none(), "docid={i} 应被区间删");
        }
        for i in (1..5u64).chain(21..=30u64) {
            assert!(e.get(i).unwrap().is_some(), "docid={i} 应保留");
        }
        // ② 空区间 [50,60] → 0 行
        match super::delete_response(&mut e, "DELETE FROM documents WHERE id BETWEEN 50 AND 60") {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 0, "空区间应删 0 行"),
            super::QueryResponse::Err(c, m) => panic!("delete 空区间失败: {c} {m}"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        // ③ 大写/空白容错：`ID BETWEEN  1 AND 4`
        match super::delete_response(&mut e, "DELETE FROM documents WHERE ID BETWEEN  1 AND 4") {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 4),
            super::QueryResponse::Err(c, m) => panic!("delete 大写 BETWEEN 失败: {c} {m}"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        assert!(e.get(1).unwrap().is_none() && e.get(4).unwrap().is_none());
        // ④ docid BETWEEN（引擎 docid 直解区间）
        match super::delete_response(&mut e, "DELETE FROM documents WHERE docid BETWEEN 21 AND 25") {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 5),
            super::QueryResponse::Err(c, m) => panic!("delete docid BETWEEN 失败: {c} {m}"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        // ⑤ 多表：t_a 同 row_id 区间（tid≠0），documents 已删的不受影响
        let tid_a = super::table_id_for("t_a");
        assert_ne!(tid_a, 0);
        for r in 1..=30u64 {
            let doc = format!("{{\"v\":{r}}}");
            e.put(super::docid_for(tid_a, r), doc.as_bytes().to_vec(), &[]).unwrap();
        }
        // t_a 区间 [8,12] → t_a 行 8..=12 删（5 行）；documents 的 8..=10 已删、11..=20 本就缺 —— 不干扰
        match super::delete_response(&mut e, "DELETE FROM t_a WHERE id BETWEEN 8 AND 12") {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 5, "仅删 t_a 区间现存行"),
            super::QueryResponse::Err(c, m) => panic!("delete t_a BETWEEN 失败: {c} {m}"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        for r in 8..=12u64 {
            assert!(e.get(super::docid_for(tid_a, r)).unwrap().is_none(), "t_a row={r} 应删");
        }
        // documents 21..=30 仍可见（BETWEEN 表区间隔离）
        for i in 26..=30u64 {
            assert!(e.get(i).unwrap().is_some(), "documents docid={i} 应保留");
        }
        // ⑥ 反向/单值 BETWEEN（lo==hi 等价点删）
        match super::delete_response(&mut e, "DELETE FROM documents WHERE id BETWEEN 26 AND 26") {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 1),
            super::QueryResponse::Err(c, m) => panic!("delete 单点 BETWEEN 失败: {c} {m}"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
    }

    // ---------- P88/P89：写路径 DocIdSet 定位整合 + UPDATE 批量管道 ----------

    #[test]
    fn p88_write_locate_field_cond_update_delete_full_cover() {
        // P88：字段条件写定位走 get_docid_set（limit=None 不截断）；P89：UPDATE 批量管道
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut e = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        // 40 行（无倒排 term——字段等值走回退扫描定位，验证定位语义与不截断）
        for i in 0..40u64 {
            let status = if i % 2 == 0 { "active" } else { "idle" };
            let doc = serde_json::json!({"i": i, "status": status, "n": i});
            e.put(i, serde_json::to_vec(&doc).unwrap(), &[]).unwrap();
        }
        e.flush_wal().unwrap();
        // 字段条件 DELETE status='idle' → 20 行全删（不截断）
        match super::delete_response(&mut e, "DELETE FROM documents WHERE status='idle'") {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 20, "idle 20 行全删（写定位不截断）"),
            super::QueryResponse::Err(c, m) => panic!("delete field 失败: {c} {m}"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        for i in 0..40u64 {
            if i % 2 == 1 {
                assert!(e.get(i).unwrap().is_none(), "docid={i} 已删");
            } else {
                assert!(e.get(i).unwrap().is_some(), "docid={i} 保留");
            }
        }
        // 字段条件 UPDATE status='done'（active 20 行）→ P89 批量管道
        match super::update_response(&mut e, "UPDATE documents SET status='done' WHERE status='active'") {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 20),
            super::QueryResponse::Err(c, m) => panic!("update field 失败: {c} {m}"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        for i in (0..40u64).step_by(2) {
            let v: serde_json::Value = serde_json::from_slice(&e.get(i).unwrap().unwrap()).unwrap();
            assert_eq!(v["status"], "done", "docid={i} 应更新为 done");
            assert_eq!(v["n"], serde_json::json!(i), "docid={i} 其它字段保留");
        }
        // doc= 整文档整体替换分支（批量管道 doc 分支：AND 复合条件收敛定位）
        match super::update_response(
            &mut e,
            "UPDATE documents SET doc='{\"i\":0,\"status\":\"replaced\"}' WHERE status='done' AND n<4",
        ) {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 2, "n<4 的 active 行（0,2）整体替换"),
            super::QueryResponse::Err(c, m) => panic!("update doc= 失败: {c} {m}"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        for i in 0..4u64 {
            if i % 2 == 0 {
                let v: serde_json::Value = serde_json::from_slice(&e.get(i).unwrap().unwrap()).unwrap();
                assert_eq!(v["status"], "replaced", "docid={i} doc= 整体替换");
                assert!(v.get("n").is_none(), "docid={i} 旧字段随整文档替换清除");
            }
        }
    }

    #[test]
    fn p88_field_cond_write_isolated_by_table_and_multi_chunk() {
        // P88：同字段值跨表 → 写定位按表隔离（只动目标表）；P89：>1000 命中跨多个
        // batch_get/put_batch chunk（chunks(1000) 循环）不丢行不漏改。
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut e = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        let tid_a = super::table_id_for("t_a");
        let tid_b = super::table_id_for("t_b");
        assert_ne!(tid_a, 0);
        assert_ne!(tid_a, tid_b);
        // ① 两表同字段值：DELETE FROM t_a WHERE status='x' → 只删 t_a（t_b 保留）
        for i in 0..6u64 {
            let doc_a = serde_json::json!({"status": "x", "v": i});
            let doc_b = serde_json::json!({"status": "x", "v": i});
            e.put(super::docid_for(tid_a, i), serde_json::to_vec(&doc_a).unwrap(), &[]).unwrap();
            e.put(super::docid_for(tid_b, i), serde_json::to_vec(&doc_b).unwrap(), &[]).unwrap();
        }
        e.flush_wal().unwrap();
        match super::delete_response(&mut e, "DELETE FROM t_a WHERE status='x'") {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 6, "仅删 t_a 6 行"),
            super::QueryResponse::Err(c, m) => panic!("delete t_a field 失败: {c} {m}"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        for i in 0..6u64 {
            assert!(e.get(super::docid_for(tid_a, i)).unwrap().is_none(), "t_a row={i} 已删");
            assert!(e.get(super::docid_for(tid_b, i)).unwrap().is_some(), "t_b row={i} 保留");
        }
        // ② 2200 行字段条件 UPDATE（跨 3 个 chunk=1000）→ 全改不丢
        let mut e2 = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        for i in 0..2200u64 {
            let doc = serde_json::json!({"status": "s", "n": i});
            e2.put_nosync(i, serde_json::to_vec(&doc).unwrap(), &[]).unwrap();
        }
        e2.flush_wal().unwrap();
        match super::update_response(&mut e2, "UPDATE documents SET n=0 WHERE status='s'") {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 2200, "2200 行跨 chunk 全改"),
            super::QueryResponse::Err(c, m) => panic!("update 2200 失败: {c} {m}"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        for i in (0..2200u64).step_by(373) {
            let v: serde_json::Value = serde_json::from_slice(&e2.get(i).unwrap().unwrap()).unwrap();
            assert_eq!(v["n"], "0", "docid={i} 应更新 n=0（字段赋值 SQL 字面量为字符串）");
        }
    }

    #[test]
    fn parse_pk_between_forms() {
        // B2 解析器：主键闭区间形态识别；其余形态（字段 BETWEEN / 复合条件 / 非 id 前缀）返回 None
        assert_eq!(super::parse_pk_between("id BETWEEN 100 AND 200"), Some((100, 200)));
        assert_eq!(super::parse_pk_between("docid BETWEEN 1 AND 9"), Some((1, 9)));
        assert_eq!(super::parse_pk_between("  ID BETWEEN  5 AND  9 "), Some((5, 9)));
        assert_eq!(super::parse_pk_between("id between 5 and 9;"), Some((5, 9)));
        assert_eq!(super::parse_pk_between("id = 42"), None);
        assert_eq!(super::parse_pk_between("id IN (1,2,3)"), None);
        assert_eq!(super::parse_pk_between("amount BETWEEN 1 AND 5"), None); // 非主键字段
        assert_eq!(
            super::parse_pk_between("id BETWEEN 5 AND 9 AND status='x'"),
            None, // 复合条件交回通用路径
        );
    }

    // ---------- P127 组合 WHERE 收敛（主键区间 ∩ 等值定位 + LIMIT 早停） ----------

    /// 偶数 active / 奇数 closed，文档含 id 字段（值 = docid）+ status + n。
    fn p127_lib(e: &mut crate::engine::Engine) {
        for i in 1..=4000u64 {
            let (status, term): (&str, &str) = if i % 2 == 0 {
                ("active", "status=active")
            } else {
                ("closed", "status=closed")
            };
            let doc = serde_json::json!({"id": i, "status": status, "n": i});
            e.put(i, serde_json::to_vec(&doc).unwrap(), &[term]).unwrap();
        }
        e.flush_wal().unwrap();
    }

    #[test]
    fn p127_combo_pk_range_update_limit_and_full() {
        // 组合 WHERE：id BETWEEN ∩ status='active' → docid 区间 keys-only ∩ active 位图。
        // LIMIT 50 只改升序前 50 命中（id 2..=100 偶），无 LIMIT 全区间 active 全改（不截断）。
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut e = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        p127_lib(&mut e);
        match super::update_response(
            &mut e,
            "UPDATE documents SET n='-1' WHERE id BETWEEN 1 AND 1000 AND status='active' LIMIT 50",
        ) {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 50, "LIMIT 50 只改 50 行"),
            super::QueryResponse::Err(c, m) => panic!("p127 update limit 失败: {c} {m}"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        let mut changed = 0u64;
        for i in 1..=1000u64 {
            let v: serde_json::Value =
                serde_json::from_slice(&e.get(i).unwrap().unwrap()).unwrap();
            if i % 2 == 0 && i <= 100 {
                assert_eq!(v["n"], "-1", "docid={i} 应被改（前 50 active）");
                changed += 1;
            } else {
                assert_eq!(v["n"], serde_json::json!(i), "docid={i} 不应被改");
            }
        }
        assert_eq!(changed, 50);
        // 无 LIMIT：区间 1001..2000 active 500 行全改（不截断）
        match super::update_response(
            &mut e,
            "UPDATE documents SET n='-2' WHERE id BETWEEN 1001 AND 2000 AND status='active'",
        ) {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 500, "区间内 active 500 行全改"),
            super::QueryResponse::Err(c, m) => panic!("p127 update full 失败: {c} {m}"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        for i in 1001..=2000u64 {
            let v: serde_json::Value =
                serde_json::from_slice(&e.get(i).unwrap().unwrap()).unwrap();
            if i % 2 == 0 {
                assert_eq!(v["n"], "-2", "docid={i} 应全量改");
            } else {
                assert_eq!(v["n"], serde_json::json!(i), "docid={i} closed 不改");
            }
        }
    }

    #[test]
    fn p127_combo_pk_range_delete_limit_and_full() {
        // DELETE 组合主键区间收敛：LIMIT 100 只删前 100 命中（id 2..=200 偶），
        // 无 LIMIT 全区间 active 全删。
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut e = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        p127_lib(&mut e);
        match super::delete_response(
            &mut e,
            "DELETE FROM documents WHERE id BETWEEN 1 AND 400 AND status='active' LIMIT 100",
        ) {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 100, "LIMIT 100 只删 100 行"),
            super::QueryResponse::Err(c, m) => panic!("p127 delete limit 失败: {c} {m}"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        for i in 1..=400u64 {
            let exists = e.get(i).unwrap().is_some();
            if i % 2 == 0 && i <= 200 {
                assert!(!exists, "docid={i} 应被删（前 100 active）");
            } else {
                assert!(exists, "docid={i} 应保留");
            }
        }
        // 无 LIMIT：区间 401..800 active 全删（200 行）
        match super::delete_response(
            &mut e,
            "DELETE FROM documents WHERE id BETWEEN 401 AND 800 AND status='active'",
        ) {
            super::QueryResponse::Ok(n, _) => assert_eq!(n, 200, "区间 active 全删"),
            super::QueryResponse::Err(c, m) => panic!("p127 delete full 失败: {c} {m}"),
            super::QueryResponse::Set { .. } => panic!("DML 不应返回 Set"),
        }
        for i in 401..=800u64 {
            let exists = e.get(i).unwrap().is_some();
            if i % 2 == 0 {
                assert!(!exists, "docid={i} 偶数应全删");
            } else {
                assert!(exists, "docid={i} closed 保留");
            }
        }
    }

    #[test]
    fn p127_combo_select_avoids_composite_full_rescan() {
        // composite 前缀路由守卫：WHERE = 等值(status=active) + 主键区间(id BETWEEN) →
        // 回退 eval 收敛（此前 composite 前缀命中会全 active 物化+复筛：100k 62ms→1100k
        // 582ms ≈9.4× 恒定窗口的根因）。正确性：LIMIT 200 = 升序前 200 active（even 2..=400）。
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.storage.composite_indexes = vec![vec!["status".into()]];
        let mut e = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        p127_lib(&mut e);
        let rows = crate::sqlish::execute(
            &e,
            "SELECT id,status FROM t WHERE id BETWEEN 1 AND 20000 AND status='active' LIMIT 200",
            10_000,
        )
        .unwrap();
        assert_eq!(rows.len(), 200, "组合 SELECT LIMIT 200 行数");
        for (k, (d, _)) in rows.iter().enumerate() {
            assert_eq!(*d, 2 + 2 * k as u64, "第 {k} 行应 = active 升序第 {k}");
        }
    }

    #[test]
    fn insert_dup_pk_1062() {
        // a：INSERT 主键重复 → MySQL 1062（同语句重复 / 库中已存在；预校验 → 无部分写入）
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut e = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        let put = |e: &mut crate::engine::Engine, id: u64| {
            let doc = format!("{{\"a\":1}}");
            e.put(id, doc.as_bytes().to_vec(), &[]).unwrap();
        };
        put(&mut e, 2);
        let auto = std::sync::atomic::AtomicU64::new(100);
        let ins = |e: &mut crate::engine::Engine, sql: &str| super::insert_response(e, sql, &auto);
        // 库中已存在 → 1062
        match ins(&mut e, "INSERT INTO documents VALUES (2,'x')") {
            super::QueryResponse::Err(1062, m) => assert!(m.contains("Duplicate entry '2'")),
            _ => panic!("应报 1062"),
        }
        // 同语句内重复 → 1062 且前序行不落（预校验，无部分写入）
        match ins(&mut e, "INSERT INTO documents VALUES (3,'x'),(3,'y')") {
            super::QueryResponse::Err(1062, _) => {}
            _ => panic!("应报 1062"),
        }
        assert!(e.get(3).unwrap().is_none(), "语句失败不得部分写入");
        // 多行含已存在键 → 1062 且新键不落
        match ins(&mut e, "INSERT INTO documents VALUES (4,'w'),(2,'z')") {
            super::QueryResponse::Err(1062, _) => {}
            _ => panic!("应报 1062"),
        }
        assert!(e.get(4).unwrap().is_none(), "含重复的语句失败不得部分写入");
        // 正常显式 id 与 auto(id=0) 不受影响
        match ins(&mut e, "INSERT INTO documents VALUES (5,'{\"a\":1}')") {
            super::QueryResponse::Ok(n, last) => {
                assert_eq!(n, 1);
                assert_eq!(last, 5);
            }
            _ => panic!("正常插入失败"),
        }
        match ins(&mut e, "INSERT INTO documents VALUES (0,'{\"auto\":1}')") {
            super::QueryResponse::Ok(1, last) => assert_eq!(last, 100),
            _ => panic!("auto 插入失败"),
        }
    }

    #[test]
    fn auto_multirow_block_allocation() {
        // §27 P1：多行 INSERT 中 auto 行一次性申请连续块（语句级一次 fetch_add），
        // 与显式 id 混合按行序分配、块内唯一递增；下一条语句按引擎水位续接
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut e = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        let auto = std::sync::atomic::AtomicU64::new(1);
        let ins = |e: &mut crate::engine::Engine, sql: &str| {
            super::insert_response(e, sql, &auto)
        };
        // 显式基准 100
        match ins(&mut e, "INSERT INTO documents VALUES (100,'{\"x\":1}')") {
            super::QueryResponse::Ok(1, _) => {}
            _ => panic!("显式插入失败"),
        }
        // 混合多行：auto×3 与显式 200/300 交错 → auto 块起点 101（> max 100），
        // 按行序取 101,102,103；last_id = 最后一行（显式 300）
        match ins(
            &mut e,
            "INSERT INTO documents VALUES (0,'{\"a\":1}'),(200,'{\"b\":1}'),(0,'{\"a\":2}'),(0,'{\"a\":3}'),(300,'{\"c\":1}')",
        ) {
            super::QueryResponse::Ok(5, last) => assert_eq!(last, 300),
            _ => panic!("多行混合插入失败"),
        }
        for id in [101u64, 102, 103] {
            assert!(e.get(id).unwrap().is_some(), "auto 块内 {id} 应存在");
        }
        assert!(e.get(200).unwrap().is_some());
        assert!(e.get(300).unwrap().is_some());
        // 下一条单行 auto：按引擎水位（max=300）续接 → 301，不撞不重复
        match ins(&mut e, "INSERT INTO documents VALUES (0,'{\"a\":4}')") {
            super::QueryResponse::Ok(1, last) => assert_eq!(last, 301, "新语句按水位续接"),
            super::QueryResponse::Err(1062, m) => panic!("续接撞库 1062: {m}"),
            _ => panic!("单行 auto 失败"),
        }
    }

    #[test]
    fn auto_insert_resumes_above_explicit_ids() {
        // §27 P0：auto(id=0) 分配从引擎 max docid+1 起——显式大 id 后自动 id 抬位不撞；
        // 重启续接（惰性水位恢复）不撞已提交行（不再从 1 起 1062）
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut e = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        let auto = std::sync::atomic::AtomicU64::new(1);
        // 显式大 id
        match super::insert_response(&mut e, "INSERT INTO documents VALUES (900100,'{\"k\":0}')", &auto) {
            super::QueryResponse::Ok(1, _) => {}
            _ => panic!("显式插入失败"),
        }
        // auto：从 max+1 = 900101 分配
        match super::insert_response(&mut e, "INSERT INTO documents VALUES (0,'{\"auto\":1}')", &auto) {
            super::QueryResponse::Ok(1, last) => assert_eq!(last, 900101, "显式 900100 后 auto 应抬位"),
            super::QueryResponse::Err(1062, m) => panic!("auto 撞已存在行 1062: {m}"),
            _ => panic!("auto 插入失败"),
        }
        assert!(e.get(900101).unwrap().is_some());
        // 连续 auto → 900102（唯一递增）
        match super::insert_response(&mut e, "INSERT INTO documents VALUES (0,'{\"auto\":2}')", &auto) {
            super::QueryResponse::Ok(1, last) => assert_eq!(last, 900102),
            _ => panic!("auto#2 失败"),
        }
        assert!(e.get(900102).unwrap().is_some());
        // 重启续接：auto_id 计数仍 < 水位 → 抬到现存最大+1（900103），不撞已提交行
        drop(e);
        let mut e2 = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        match super::insert_response(&mut e2, "INSERT INTO documents VALUES (0,'{\"auto\":3}')", &auto) {
            super::QueryResponse::Ok(1, last) => assert_eq!(last, 900103, "重启后 auto 续接不撞库"),
            super::QueryResponse::Err(1062, m) => panic!("重启后 auto 撞已提交行 1062: {m}"),
            _ => panic!("重启 auto 插入失败"),
        }
    }

    #[test]
    fn auto_increment_ddl_and_idless_insert() {
        // §27：AUTO_INCREMENT 列属性接受（CREATE TABLE 空操作，属性解析容忍）+
        // 无 id 列 INSERT（id 列省略 → auto 分配连续）端到端
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let _srv = std::thread::spawn(move || {
            server.serve(&addr.to_string()).expect("serve 失败");
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect(addr).is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let mut c = TestClient::connect(addr);
        let (scramble, _) = c.handshake();
        c.authenticate("root", "secret", &scramble);
        let sum = |c: &mut TestClient, sql: &str| -> String {
            let r = c.query(sql);
            let row = &r[r.len() - 2];
            let mut p = 0usize;
            let n = read_lenenc(row, &mut p).unwrap() as usize;
            String::from_utf8(row[p..p + n].to_vec()).unwrap()
        };
        // CREATE TABLE 含 AUTO_INCREMENT/PRIMARY KEY 列属性 → 空操作接受（不报错）
        assert_eq!(
            c.query("CREATE TABLE documents(id INT AUTO_INCREMENT PRIMARY KEY, k INT, c CHAR(50))")[0][0],
            OK_PACKET
        );
        // 无 id 列 INSERT（id 省略 → auto 分配）；非事务 SUM 验证内容（修复后正确）
        assert_eq!(c.query("INSERT INTO documents(k, c) VALUES(7, 'x')")[0][0], OK_PACKET);
        assert_eq!(sum(&mut c, "SELECT SUM(k) FROM documents WHERE id=1"), "7", "auto id=1 行可见");
        assert_eq!(c.query("INSERT INTO documents(k) VALUES(8)")[0][0], OK_PACKET);
        assert_eq!(sum(&mut c, "SELECT SUM(k) FROM documents WHERE id=2"), "8", "auto id=2 行可见");
        // 多行无 id → 语句级块分配 3,4
        assert_eq!(
            c.query("INSERT INTO documents(k) VALUES(9),(10)")[0][0],
            OK_PACKET
        );
        assert_eq!(
            sum(&mut c, "SELECT SUM(k) FROM documents WHERE id BETWEEN 3 AND 4"),
            "19",
            "多行无 id 连续分配 id=3,4"
        );
    }

    #[test]
    fn drop_table_purges_data_and_restart_empty() {
        // c：DROP/TRUNCATE TABLE 真正清库（内存行 + 磁盘段 + 倒排），重启后目录为空库；
        // 同主键再插不再 1062 —— 对齐 MySQL 整表删除语义（cleanup / 反复 --init 基线可比）
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut e = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        let auto = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1));
        let mut s = super::new_session(std::sync::Arc::clone(&auto));
        // 引擎直写 3 行（显式倒排 term）+ 强制落盘覆盖持久态（SST / WAL / 倒排段）
        for id in 1u64..=3 {
            let doc = format!("{{\"a\":{id}}}");
            let term = format!("a={id}");
            e.put(id, doc.as_bytes().to_vec(), &[term.as_str()]).unwrap();
        }
        e.flush_primary().unwrap();
        e.flush_wal().unwrap();
        e.flush_inverted().unwrap();
        assert_eq!(e.count_all_docs().unwrap(), 3);
        assert_eq!(e.inverted_doc_count("a=1").unwrap(), 1);
        // CREATE TABLE 仍为空操作
        match super::dispatch_query(&mut e, "CREATE TABLE t1(id INT, a INT)", &mut s) {
            super::QueryResponse::Ok(0, 0) => {}
            _ => panic!("CREATE 应返回 Ok(0,0) 空操作"),
        }
        // DROP TABLE（默认表 documents）→ 整库清空（行 + 倒排）
        // M3：表名路由已按 DROP/TRUNCATE 真实表名解析——documents 才 purge 全库；
        // 非默认表 DROP 走本表区间删除（见 m3_multitable_flush_compact_drop）
        match super::dispatch_query(&mut e, "DROP TABLE documents", &mut s) {
            super::QueryResponse::Ok(0, 0) => {}
            _ => panic!("DROP 应返回 Ok(0,0)"),
        }
        assert_eq!(e.count_all_docs().unwrap(), 0, "DROP 后行应清零");
        assert!(e.scan_range(None, None).unwrap().is_empty());
        assert_eq!(e.inverted_doc_count("a=1").unwrap(), 0, "DROP 后倒排应清零");
        // 同主键再插（mysql insert 路径）→ 不再 1062（无残留主键）
        match super::insert_response(
            &mut e,
            "INSERT INTO documents VALUES (1,'{\"a\":1}')",
            &auto,
        ) {
            super::QueryResponse::Ok(1, _) => {}
            super::QueryResponse::Err(1062, m) => panic!("DROP 后残留主键 → 1062: {m}"),
            _ => panic!("再插入失败"),
        }
        // TRUNCATE TABLE（默认表 documents）→ 同样清空
        match super::dispatch_query(&mut e, "TRUNCATE TABLE documents", &mut s) {
            super::QueryResponse::Ok(0, 0) => {}
            _ => panic!("TRUNCATE 应返回 Ok(0,0)"),
        }
        assert_eq!(e.count_all_docs().unwrap(), 0);
        // 磁盘持久态：重启 open 同目录 → 空库（Manifest/SST/WAL 一致，open 不报错）
        drop(e);
        drop(s);
        let e2 = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        assert_eq!(e2.count_all_docs().unwrap(), 0, "重启后仍为空库");
        assert!(e2.scan_range(None, None).unwrap().is_empty());
    }

    #[test]
    fn parse_update_and_delete() {
        // 整体替换：SET doc='{json}'
        let (id, field, expr) =
            parse_update("UPDATE documents SET doc='{\"b\":2}' WHERE id=9").unwrap();
        assert_eq!(id, 9);
        assert_eq!(field, "doc");
        assert_eq!(expr, r#"'{"b":2}'"#);
        assert_eq!(unquote(&expr), r#"{"b":2}"#);
        // 字段自增：SET k=k+1
        let (id2, f2, e2) = parse_update("UPDATE sbtest1 SET k=k+1 WHERE id=49873363").unwrap();
        assert_eq!((id2, f2.as_str(), e2.as_str()), (49873363, "k", "k+1"));
        assert_eq!(parse_increment_expr(&f2, &e2), Some(1));
        // 字符串赋值：SET c='str'
        let (id3, f3, e3) = parse_update("UPDATE sbtest1 SET c='abc123' WHERE id=1").unwrap();
        assert_eq!((id3, f3.as_str()), (1, "c"));
        assert_eq!(parse_increment_expr(&f3, &e3), None);
        assert_eq!(unquote(&e3), "abc123");
        assert_eq!(parse_delete("DELETE FROM documents WHERE id=5").unwrap(), 5);
    }

    #[test]
    fn split_values_respects_quoted_commas() {
        let parts = split_values("1, '{\"a\":1,\"b\":2}', 3");
        assert_eq!(parts.len(), 3);
        assert_eq!(unquote(&parts[1]), r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn compaction_worker_converges_l0_with_single_round_per_wake() {
        // 9e77872（P71 阶段一）：worker 锁内**单轮**合并 + 100ms 循环——写触发信号后，
        // worker 多轮单轮合并收敛 L0（旧实现锁内 while 8 轮连续合并阻塞写）。
        // 覆盖空白：engine 级测试只手动调 compact()，此处验证 worker 线程真实运行收敛。
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.memtable.max_size_mb = 1; // 小 MemTable → 写入过程快速 flush 多段
        cfg.storage.l0_stall_threshold = 2; // 低 L0 阈值 → 2 段 L0 即触发合并
        let engine = Engine::open(dir.path(), &cfg).unwrap();
        let server = DbServer::new(engine, "root", "");
        server.spawn_compaction_worker();
        // 写 5 段（各 ~2MB）→ L0 超阈值 → auto_compact 置信号（worker 挂载：写不阻塞）
        let val = vec![b'x'; 2048];
        for seg in 0..5u64 {
            for i in seg * 1000..seg * 1000 + 1000 {
                server.engine.write().unwrap().put(i, val.clone(), &[]).unwrap();
            }
        }
        // 等待 worker 多轮单轮合并收敛（100ms 轮询 + 单轮合并；10s 上限）
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if !server.engine.read().unwrap().needs_compact()
                || std::time::Instant::now() > deadline
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(
            !server.engine.read().unwrap().needs_compact(),
            "worker 多轮单轮合并后 L0 应收敛（10s 内）"
        );
        // 数据完整
        for i in (0..5_000u64).step_by(997) {
            let v = server.engine.read().unwrap().get(i).unwrap();
            assert_eq!(v.as_deref(), Some(val.as_slice()), "docid={i}");
        }
    }

    #[test]
    fn inverted_gc_worker_converges_segments_after_flush() {
        // J 项（7.73）：后台倒排段 GC worker——写路径刷盘置 `inverted_gc_pending` 信号 →
        // worker 检查 `should_gc()` 并执行合并，段数收敛（不再依赖显式 inverted_gc 调用）。
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.inverted.segment_max_size_mb = 1; // GC 阈值 1MB（engine 打开时 ×1MB 换算）
        let engine = Engine::open(dir.path(), &cfg).unwrap();
        let server = DbServer::new(engine, "root", "");
        server.spawn_inverted_gc_worker();
        // 写 12 批（每批 1000 doc × 10 唯一 term 对）+ 刷盘 → 12 段 ≈ 1.8MB > 1MB → 置信号
        {
            let mut eng = server.engine.write().unwrap();
            for batch in 0..12u64 {
                for i in batch * 1000..batch * 1000 + 1000 {
                    let terms: Vec<String> = (0..10).map(|j| format!("k{i}-{j}")).collect();
                    let refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
                    eng.put(i, format!("{{\"id\":{i}}}").into_bytes(), &refs).unwrap();
                }
                eng.flush_inverted().unwrap();
            }
            assert!(
                eng.inverted.should_gc(),
                "12 段总字节应超 GC 阈值（触发后台信号）"
            );
        }
        // 等待 worker 后台 GC 收敛（100ms 轮询；10s 上限）
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let n = server.engine.read().unwrap().inverted.segment_count();
            if n <= 1 || std::time::Instant::now() > deadline {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        let n = server.engine.read().unwrap().inverted.segment_count();
        assert!(n <= 2, "后台 GC worker 未收敛段数: {n}");
        // 数据完整可检索（合并后旧段数据不丢）
        let r = server.engine.read().unwrap().inverted_posting("k0-0").unwrap();
        assert!(r.contains(0), "k0-0 应含 docid 0");
        let r2 = server.engine.read().unwrap().inverted_posting("k11999-9").unwrap();
        assert!(r2.contains(11999), "k11999-9 应含 docid 11999");
    }

    // ---------- Ex-8.9 切片 2A 验收：交变负载 A/B ----------

    /// 交变负载 A/B（倒排落盘 worker）：写突发（倒排 mem 积压）与短空闲窗交替。
    /// A（idle_aware）：空闲窗内 50ms tick + 1s 密集落盘 → 每轮 mem 清空，积压不跨窗累积；
    /// B（旧固定节奏 200ms）：非硬阈值（1M）/30s 兜底不落盘 → mem 随轮次单调累积。
    /// 验收判据：A 最终 mem=0，B 最终 mem>0（积压滞留）。
    #[test]
    #[ignore = "Ex-8.9 交变负载 A/B 验收（真实时钟，--ignored 手动跑）"]
    fn ex89_ab_alternating_load_inverted_flush() {
        for (label, aware) in [("B 旧固定节奏(aware=off)", false), ("A 空闲感知(aware=on)", true)] {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = crate::config::Config::default();
            cfg.memtable.max_size_mb = 16; // 突发远小于 MemTable：无主数据 flush，聚焦倒排 mem
            cfg.inverted.segment_max_size_mb = 1;
            cfg.storage.group_commit_us = 2000; // 组提交，避免逐条 fsync 拖慢突发
            let engine = Engine::open(dir.path(), &cfg).unwrap();
            let mut server = DbServer::new(engine, "root", "");
            server.set_idle_aware(aware);
            // 真实集成形态：三个后台 worker 全部挂载
            server.spawn_compaction_worker();
            server.spawn_inverted_gc_worker();
            server.spawn_inverted_flush_worker();
            let mut d = 0u64;
            for round in 0..3u64 {
                // 突发：4 万行（每行 2 term → 倒排 mem 记 docid；主数据不触发 flush）
                let t0 = std::time::Instant::now();
                {
                    let mut g = server.engine.write().unwrap();
                    for _ in 0..40_000u64 {
                        let v = format!("{{\"id\":{d}}}").into_bytes();
                        let terms = ["k0", "k1"];
                        g.put(d, v, &terms).unwrap();
                        d += 1;
                    }
                    // 排空 MemTable → write_pressure=0：worker 才能判 Idle（写压力代理口径）
                    g.flush_primary().unwrap();
                    g.flush_wal().unwrap();
                }
                // 空闲窗 2.5s：A 应 ~1s 密集落盘清空 mem；B 不落盘（未到 30s/1M 阈值）
                std::thread::sleep(std::time::Duration::from_millis(2500));
                let mem = server.engine.read().unwrap().inverted_mem_docids();
                eprintln!(
                    "[{label}] round{round}: busy_ms={} mem_after_idle={mem}",
                    t0.elapsed().as_millis()
                );
            }
            let mem_final = server.engine.read().unwrap().inverted_mem_docids();
            eprintln!("[{label}] 终态 inverted_mem_docids={mem_final}");
            if aware {
                assert_eq!(mem_final, 0, "A：空闲窗应把倒排 mem 落盘清空（积压不跨窗累积）");
            } else {
                assert!(
                    mem_final > 50_000,
                    "B：无空闲密集落盘 → mem 积压应滞留（实际 {mem_final}）"
                );
            }
        }
    }

    /// 交变负载 A/B（compaction idle_run）：auto_compact 关闭制造无信号的 L0 积压后进入空闲。
    /// A（idle_aware）：≥5s 连续空闲触发 idle_run → 强制 `targets.run()` 收敛 L0；
    /// B（旧固定节奏 100ms）：无信号不动作（600s 兜底前积压滞留）。
    /// 验收判据：A 在 idle_run 后 needs_compact=false；B 同等待时间积压原样。
    #[test]
    #[ignore = "Ex-8.9 交变负载 A/B 验收（真实时钟，--ignored 手动跑）"]
    fn ex89_ab_compaction_idle_run_drains_l0_backlog() {
        for (label, aware) in [("B 旧固定节奏(aware=off)", false), ("A 空闲感知(aware=on)", true)] {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = crate::config::Config::default();
            cfg.storage.auto_compact = false; // 突发不收敛 → 纯 idle_run 判定（无信号路径）
            cfg.storage.l0_stall_min = 2;
            cfg.storage.l0_stall_max = 64;
            cfg.storage.l0_stall_threshold = 2;
            cfg.memtable.max_size_mb = 2;
            cfg.storage.group_commit_us = 2000;
            let engine = Engine::open(dir.path(), &cfg).unwrap();
            let mut server = DbServer::new(engine, "root", "");
            // 突发：8MB（8000 × 1KB 不同 docid）→ 4 次 MemTable flush → L0=4 积压
            //（auto_compact 关：写路径不置信号 → 纯 idle_run 判定）
            {
                let mut g = server.engine.write().unwrap();
                for i in 0..8_000u64 {
                    g.put_nosync(i, vec![b'x'; 1024], &[]).unwrap();
                }
                g.flush_primary().unwrap(); // 排空 MemTable → pressure=0
                g.flush_wal().unwrap();
            }
            let l0 = server.engine.read().unwrap().primary_l0_count();
            assert!(l0 >= 4, "{label}: 突发应积压 L0≥4（实际 {l0}）");
            assert!(server.engine.read().unwrap().needs_compact(), "{label}: 积压应判需要合并");
            server.set_idle_aware(aware);
            server.spawn_compaction_worker();
            if aware {
                // A：idle_run（≥5s 连续空闲）→ targets.run 收敛；12s 上限
                let t0 = std::time::Instant::now();
                let deadline = t0 + std::time::Duration::from_secs(12);
                loop {
                    if !server.engine.read().unwrap().needs_compact() {
                        break;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "A：idle_run 未能在时限内收敛 L0 积压"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(200));
                }
                assert!(!server.engine.read().unwrap().needs_compact());
                eprintln!("[{label}] L0 积压经 idle_run 收敛（{:.1}s）", t0.elapsed().as_secs_f64());
            } else {
                // B：无信号 → 仅 600s 兜底；等 6s 验证积压滞留
                std::thread::sleep(std::time::Duration::from_secs(6));
                assert!(
                    server.engine.read().unwrap().needs_compact(),
                    "B：旧固定节奏无信号不应收敛（600s 兜底前滞留）"
                );
                eprintln!("[{label}] 6s 后 L0 积压滞留（needs_compact=true）");
            }
        }
    }

    /// 交变负载 A/B（倒排 GC idle_run）：预建 12 段可 GC 但清空 GC 信号 → 进入空闲。
    /// A（idle_aware）：≥5s 连续空闲 idle_run → 检查 should_gc 并执行段回收；
    /// B（旧固定节奏 100ms）：无信号不动作。
    /// 验收判据：A 段数收敛 ≤2；B 段数原样。
    #[test]
    #[ignore = "Ex-8.9 交变负载 A/B 验收（真实时钟，--ignored 手动跑）"]
    fn ex89_ab_gc_worker_idle_run_reclaims_segments() {
        for (label, aware) in [("B 旧固定节奏(aware=off)", false), ("A 空闲感知(aware=on)", true)] {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = crate::config::Config::default();
            cfg.inverted.segment_max_size_mb = 1;
            let engine = Engine::open(dir.path(), &cfg).unwrap();
            let mut server = DbServer::new(engine, "root", "");
            // 写 12 批（1000 doc × 10 term）+ 显式刷段 → 12 段超 GC 阈值；清信号 → 纯 idle_run 判定
            {
                let mut eng = server.engine.write().unwrap();
                for batch in 0..12u64 {
                    for i in batch * 1000..batch * 1000 + 1000 {
                        let terms: Vec<String> = (0..10).map(|j| format!("k{i}-{j}")).collect();
                        let refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
                        eng.put_nosync(i, format!("{{\"id\":{i}}}").into_bytes(), &refs)
                            .unwrap();
                    }
                    eng.flush_inverted().unwrap();
                }
                assert!(eng.inverted.should_gc(), "12 段应超 GC 阈值");
                eng.flush_primary().unwrap(); // 排空 MemTable → pressure=0（Idle 判定前提）
                eng.inverted_gc_pending.store(false, Ordering::Release);
            }
            let n_build = server.engine.read().unwrap().inverted.segment_count();
            server.set_idle_aware(aware);
            server.spawn_inverted_gc_worker();
            if aware {
                // A：idle_run（≥5s 连续空闲）→ should_gc → gc()；9s 上限
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(9);
                loop {
                    let n = server.engine.read().unwrap().inverted.segment_count();
                    if n <= 2 || std::time::Instant::now() > deadline {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                let n = server.engine.read().unwrap().inverted.segment_count();
                assert!(n <= 2, "A：idle_run 应触发 GC 收敛段（{n_build} → {n}）");
                eprintln!("[{label}] 段数经 idle_run GC 收敛 {n_build} → {n}");
            } else {
                // B：无信号 → idle_run 禁用；等 6s 验证段原样
                std::thread::sleep(std::time::Duration::from_secs(6));
                let n = server.engine.read().unwrap().inverted.segment_count();
                assert_eq!(n, n_build, "B：无信号 → 段不应被回收（{n_build}）");
                eprintln!("[{label}] 6s 后段数滞留 {n_build}（无回收）");
            }
        }
    }

    // ---------- 集成：协议往返 ----------

    /// 测试客户端：连接 → 握手 → 认证 → 查询。
    struct TestClient {
        stream: TcpStream,
    }

    impl TestClient {
        fn connect(addr: std::net::SocketAddr) -> Self {
            Self {
                stream: TcpStream::connect(addr).unwrap(),
            }
        }

        /// 读握手包并返回 (scramble, auth_plugin)。
        fn handshake(&mut self) -> (Vec<u8>, String) {
            let (_, payload) = read_packet(&mut self.stream).unwrap();
            assert_eq!(payload[0], PROTOCOL_VERSION);
            // 解析 auth plugin data：位置由 cap 高低位 + len 字段决定
            let mut pos = 1usize;
            while pos < payload.len() && payload[pos] != 0 {
                pos += 1;
            }
            pos += 1; // server version NUL
            pos += 4; // conn id
            let mut scramble = payload[pos..pos + 8].to_vec();
            pos += 8 + 1 + 2 + 1 + 2 + 2;
            let auth_len = payload[pos] as usize;
            pos += 1;
            pos += 10; // reserved
            // part2 = auth_len - 8 - 1（去掉终止 NUL）= 12 字节有效 scramble
            let part2_len = auth_len.saturating_sub(9);
            scramble.extend_from_slice(&payload[pos..pos + part2_len]);
            // auth plugin name（part2 后 + NUL 终止 → 跳过）
            let mut p = pos + part2_len + 1;
            let start = p;
            while p < payload.len() && payload[p] != 0 {
                p += 1;
            }
            let plugin = String::from_utf8_lossy(&payload[start..p]).to_string();
            (scramble, plugin)
        }

        /// 发送认证（native_password token）。
        fn authenticate(&mut self, user: &str, password: &str, scramble: &[u8]) {
            let mut b = Vec::new();
            b.extend_from_slice(&CAPABILITIES.to_le_bytes());
            b.extend_from_slice(&0x100_0000u32.to_le_bytes()); // max packet
            b.push(CHARSET_UTF8MB4);
            b.extend_from_slice(&[0u8; 23]);
            b.extend_from_slice(user.as_bytes());
            b.push(0);
            let token = if password.is_empty() {
                Vec::new()
            } else {
                let stage1 = Sha1::digest(password.as_bytes());
                let stage2 = Sha1::digest(&stage1);
                let mut h = Sha1::new();
                h.update(scramble);
                h.update(stage2);
                let crypto = h.finalize();
                (0..20).map(|i| stage1[i] ^ crypto[i]).collect()
            };
            write_lenenc(&mut b, token.len() as u64);
            b.extend_from_slice(&token);
            write_packet(&mut self.stream, 1, &b).unwrap();
            let (_, resp) = read_packet(&mut self.stream).unwrap();
            assert_eq!(resp[0], OK_PACKET, "认证应成功");
        }

        /// 发 COM_QUERY，收全部响应包（OK/ERR 单包；ResultSet 直到 EOF 包）。
        fn query(&mut self, sql: &str) -> Vec<Vec<u8>> {
            let mut cmd = vec![COM_QUERY];
            cmd.extend_from_slice(sql.as_bytes());
            write_packet(&mut self.stream, 0, &cmd).unwrap();
            let (_, first) = read_packet(&mut self.stream).unwrap();
            if first.first() == Some(&OK_PACKET) || first.first() == Some(&ERR_PACKET) {
                return vec![first];
            }
            let mut packets = vec![first];
            let mut saw_eof = false; // 列定义后第一个 EOF（非终止）
            loop {
                let (_, payload) = read_packet(&mut self.stream).unwrap();
                let is_eof = payload.first() == Some(&EOF_PACKET) && payload.len() < 9;
                let last = is_eof && saw_eof; // 第二个 EOF（行尾）终止
                if is_eof {
                    saw_eof = true;
                }
                packets.push(payload);
                if last {
                    break;
                }
            }
            packets
        }

        /// 发 COM_STMT_PREPARE，收全部响应包（PREPARE_OK + 参数定义 + EOF + 列定义 + EOF）。
        fn stmt_prepare(&mut self, sql: &str) -> Vec<Vec<u8>> {
            let mut cmd = vec![COM_STMT_PREPARE];
            cmd.extend_from_slice(sql.as_bytes());
            write_packet(&mut self.stream, 0, &cmd).unwrap();
            let mut packets = Vec::new();
            let (_, first) = read_packet(&mut self.stream).unwrap();
            packets.push(first);
            // PREPARE_OK 后：参数定义 + EOF + 列定义 + EOF（直到第二个 EOF）
            let mut saw_eof = false;
            loop {
                let (_, payload) = read_packet(&mut self.stream).unwrap();
                let is_eof = payload.first() == Some(&EOF_PACKET) && payload.len() < 9;
                let last = is_eof && saw_eof;
                if is_eof {
                    saw_eof = true;
                }
                packets.push(payload);
                if last {
                    break;
                }
            }
            packets
        }

        /// 发 COM_STMT_EXECUTE（LONGLONG 单参数），收全部响应包。
        fn stmt_execute(&mut self, stmt_id: u32, param: u64) -> Vec<Vec<u8>> {
            let mut cmd = vec![COM_STMT_EXECUTE];
            cmd.extend_from_slice(&stmt_id.to_le_bytes());
            cmd.push(0); // flags
            cmd.extend_from_slice(&1u32.to_le_bytes()); // iteration
            cmd.push(0); // null_bitmap（无 NULL）
            cmd.push(1); // new_params_bound_flag
            cmd.push(MYSQL_TYPE_LONGLONG);
            cmd.push(0); // unsigned
            cmd.extend_from_slice(&param.to_le_bytes());
            self.send_command_and_read(&cmd)
        }

        /// 发 COM_STMT_EXECUTE（LONGLONG + 字符串参数）。
        fn stmt_execute_str(&mut self, stmt_id: u32, id: u64, doc: &str) -> Vec<Vec<u8>> {
            let mut cmd = vec![COM_STMT_EXECUTE];
            cmd.extend_from_slice(&stmt_id.to_le_bytes());
            cmd.push(0);
            cmd.extend_from_slice(&1u32.to_le_bytes());
            cmd.push(0); // null_bitmap
            cmd.push(1); // new_params_bound_flag
            cmd.push(MYSQL_TYPE_LONGLONG);
            cmd.push(0);
            cmd.push(MYSQL_TYPE_VAR_STRING);
            cmd.push(0);
            cmd.extend_from_slice(&id.to_le_bytes());
            cmd.push(doc.len() as u8); // lenenc（短串）
            cmd.extend_from_slice(doc.as_bytes());
            self.send_command_and_read(&cmd)
        }

        /// 发命令并读取响应（OK/ERR 单包；ResultSet 直到第二个 EOF）。
        fn send_command_and_read(&mut self, cmd: &[u8]) -> Vec<Vec<u8>> {
            write_packet(&mut self.stream, 0, cmd).unwrap();
            let (_, first) = read_packet(&mut self.stream).unwrap();
            if first.first() == Some(&OK_PACKET) || first.first() == Some(&ERR_PACKET) {
                return vec![first];
            }
            let mut packets = vec![first];
            let mut saw_eof = false;
            loop {
                let (_, payload) = read_packet(&mut self.stream).unwrap();
                let is_eof = payload.first() == Some(&EOF_PACKET) && payload.len() < 9;
                let last = is_eof && saw_eof;
                if is_eof {
                    saw_eof = true;
                }
                packets.push(payload);
                if last {
                    break;
                }
            }
            packets
        }
    }

    #[test]
    fn handshake_auth_and_query_roundtrip() {
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        let addr = server.serve_once("127.0.0.1:0").unwrap();

        let mut c = TestClient::connect(addr);
        let (scramble, plugin) = c.handshake();
        assert_eq!(plugin, "mysql_native_password");
        c.authenticate("root", "secret", &scramble);

        // SHOW DATABASES → ResultSet 含 cjserver
        let packets = c.query("SHOW DATABASES");
        assert_eq!(packets[0][0], 1, "列数 = 1");
        // 解析末行首列
        let row = &packets[packets.len() - 2];
        let mut pos = 0usize;
        let _ = read_lenenc(&row, &mut pos).unwrap();
        let name = String::from_utf8_lossy(&row[pos..]).to_string();
        assert_eq!(name, DEFAULT_DB);

        // INSERT + SELECT 往返
        let ok = c.query("INSERT INTO documents (id, doc) VALUES (1, '{\"k\":1}')");
        assert_eq!(ok[0][0], OK_PACKET);
        let sel = c.query("SELECT * FROM documents WHERE id=1");
        assert_eq!(sel[0][0], 2, "两列 id/doc");
        // 末行含 doc 内容
        let data = sel[sel.len() - 2].clone();
        let mut p = 0usize;
        let n = read_lenenc(&data, &mut p).unwrap();
        assert_eq!(n, 1, "第一列 id 值长度 = 1");
        assert_eq!(&data[p..p + n as usize], b"1", "id = 1");
        p += n as usize;
        let dlen = read_lenenc(&data, &mut p).unwrap() as usize;
        assert_eq!(&data[p..p + dlen], br#"{"k":1}"#);

        // UPDATE / DELETE
        let upd = c.query("UPDATE documents SET doc='{\"k\":2}' WHERE id=1");
        assert_eq!(upd[0][0], OK_PACKET);
        let del = c.query("DELETE FROM documents WHERE id=1");
        assert_eq!(del[0][0], OK_PACKET);
        let sel2 = c.query("SELECT * FROM documents WHERE id=1");
        // 空结果集：列数 + 2 列定义 + 2 EOF（无数据行）
        assert_eq!(sel2.len(), 5, "删除后应返回空结果集（无数据行）");
    }

    #[test]
    fn wrong_password_rejected() {
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        let addr = server.serve_once("127.0.0.1:0").unwrap();
        let mut c = TestClient::connect(addr);
        let (scramble, _) = c.handshake();
        // 错误密码：直接验证 check 函数（连接级拒绝在 serve_once 单连接后关闭）
        assert!(!check_native_password(&[0u8; 20], &scramble, "secret"));
    }

    // ---------- H-4：事务语句 ----------

    #[test]
    fn txn_begin_rollback_and_commit_roundtrip() {
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        let addr = server.serve_once("127.0.0.1:0").unwrap();
        let mut c = TestClient::connect(addr);
        let (scramble, _) = c.handshake();
        c.authenticate("root", "secret", &scramble);

        // BEGIN → 事务内 INSERT（攒批）→ 同事务 SELECT 可见 → ROLLBACK → 无数据
        assert_eq!(c.query("BEGIN")[0][0], OK_PACKET);
        let ins = c.query("INSERT INTO documents (id, doc) VALUES (5, '{\"tx\":1}')");
        assert_eq!(ins[0][0], OK_PACKET);
        // 同事务读可见（read_own）
        let sel = c.query("SELECT * FROM documents WHERE id=5");
        let row = &sel[sel.len() - 2];
        let mut p = 0usize;
        let _ = read_lenenc(&row, &mut p).unwrap();
        assert_eq!(&row[p..p + 1], b"5", "事务内读到自己未提交的写");
        // ROLLBACK 后无数据
        assert_eq!(c.query("ROLLBACK")[0][0], OK_PACKET);
        let sel2 = c.query("SELECT * FROM documents WHERE id=5");
        assert_eq!(sel2.len(), 5, "回滚后空结果集");

        // BEGIN → INSERT → COMMIT → 持久可见
        assert_eq!(c.query("BEGIN")[0][0], OK_PACKET);
        assert_eq!(
            c.query("INSERT INTO documents (id, doc) VALUES (6, '{\"tx\":2}')")[0][0],
            OK_PACKET
        );
        assert_eq!(c.query("COMMIT")[0][0], OK_PACKET);
        let sel3 = c.query("SELECT * FROM documents WHERE id=6");
        assert_eq!(sel3[0][0], 2, "提交后两列");
        let row3 = &sel3[sel3.len() - 2];
        let mut p3 = 0usize;
        let _ = read_lenenc(&row3, &mut p3).unwrap();
        assert_eq!(&row3[p3..p3 + 1], b"6");
    }

    #[test]
    fn txn_select_non_pk_predicate_overlay() {
        // b：事务内非主键列谓词 SELECT——同事务 UPDATE/INSERT 覆盖可见、被改行正确排除
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        let addr = server.serve_once("127.0.0.1:0").unwrap();
        let mut c = TestClient::connect(addr);
        let (scramble, _) = c.handshake();
        c.authenticate("root", "secret", &scramble);
        let mut base = |id: u64, k: i64, s: &str| {
            let doc = format!("{{\"k\":{k},\"s\":\"{s}\"}}");
            let sql = format!("INSERT INTO documents (id, doc) VALUES ({id}, '{doc}')");
            assert_eq!(c.query(&sql)[0][0], OK_PACKET);
        };
        base(1, 1, "a");
        base(2, 2, "b");
        base(3, 3, "a");
        base(4, 4, "b");
        assert_eq!(c.query("BEGIN")[0][0], OK_PACKET);
        // 同事务 UPDATE k=k+1（doc2: 2→3）
        assert_eq!(c.query("UPDATE documents SET k=k+1 WHERE id=2")[0][0], OK_PACKET);
        // 同事务 INSERT doc5 (k=5, s='b')
        assert_eq!(
            c.query("INSERT INTO documents (id, doc) VALUES (5, '{\"k\":5,\"s\":\"b\"}')")[0][0],
            OK_PACKET
        );
        let sum_col = |r: &Vec<Vec<u8>>| -> String {
            let row = &r[r.len() - 2];
            let mut p = 0usize;
            let n = read_lenenc(row, &mut p).unwrap() as usize;
            String::from_utf8(row[p..p + n].to_vec()).unwrap()
        };
        // s='a'：doc1(1)+doc3(3)=4（doc2 已改 s=b 不变、doc2 k=3 计入 b 组）
        let ra = c.query("SELECT SUM(k) FROM documents WHERE s='a'");
        assert_eq!(sum_col(&ra), "4");
        // s='b'：doc2(3,自增后)+doc4(4)+doc5(5,同事务插入)=12
        let rb = c.query("SELECT SUM(k) FROM documents WHERE s='b'");
        assert_eq!(sum_col(&rb), "12");
        assert_eq!(c.query("ROLLBACK")[0][0], OK_PACKET);
    }

    #[test]
    fn txn_for_update_current_read_c1() {
        // 缺陷 A（C1）：FOR UPDATE = 当前读，见最新已提交；一致读仍快照
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let _srv = std::thread::spawn(move || {
            server.serve(&addr.to_string()).expect("serve 失败");
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect(addr).is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let mut connect = || {
            let mut c = TestClient::connect(addr);
            let (scramble, _) = c.handshake();
            c.authenticate("root", "secret", &scramble);
            c
        };
        let mut main = connect();
        let mut aux = connect();
        let sum = |c: &mut TestClient, sql: &str| -> String {
            let r = c.query(sql);
            let row = &r[r.len() - 2];
            let mut p = 0usize;
            let n = read_lenenc(row, &mut p).unwrap() as usize;
            String::from_utf8(row[p..p + n].to_vec()).unwrap()
        };
        assert_eq!(
            main.query("INSERT INTO documents (id, doc) VALUES (900100, '{\"k\":0}')")[0][0],
            OK_PACKET
        );
        assert_eq!(main.query("BEGIN")[0][0], OK_PACKET);
        assert_eq!(sum(&mut main, "SELECT SUM(k) FROM documents WHERE id=900100"), "0");
        assert_eq!(
            aux.query("UPDATE documents SET k=k+1 WHERE id=900100")[0][0],
            OK_PACKET
        );
        // RR 一致读仍见快照旧值 0；FOR UPDATE 见最新已提交 1；当前读不污染快照
        assert_eq!(sum(&mut main, "SELECT SUM(k) FROM documents WHERE id=900100"), "0");
        assert_eq!(
            sum(
                &mut main,
                "SELECT SUM(k) FROM documents WHERE id=900100 FOR UPDATE"
            ),
            "1"
        );
        assert_eq!(sum(&mut main, "SELECT SUM(k) FROM documents WHERE id=900100"), "0");
        assert_eq!(main.query("COMMIT")[0][0], OK_PACKET);
    }

    #[test]
    fn txn_for_update_current_read_c3() {
        // 缺陷 A（C3）：他事务 DELETE 已提交后 FOR UPDATE 当前读不可见；快照仍见已删行
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let _srv = std::thread::spawn(move || {
            server.serve(&addr.to_string()).expect("serve 失败");
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect(addr).is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let mut connect = || {
            let mut c = TestClient::connect(addr);
            let (scramble, _) = c.handshake();
            c.authenticate("root", "secret", &scramble);
            c
        };
        let mut main = connect();
        let mut aux = connect();
        let sum = |c: &mut TestClient, sql: &str| -> String {
            let r = c.query(sql);
            let row = &r[r.len() - 2];
            let mut p = 0usize;
            let n = read_lenenc(row, &mut p).unwrap() as usize;
            String::from_utf8(row[p..p + n].to_vec()).unwrap()
        };
        assert_eq!(
            main.query("INSERT INTO documents (id, doc) VALUES (900300, '{\"k\":5}')")[0][0],
            OK_PACKET
        );
        assert_eq!(main.query("BEGIN")[0][0], OK_PACKET);
        assert_eq!(sum(&mut main, "SELECT SUM(k) FROM documents WHERE id=900300"), "5");
        assert_eq!(aux.query("DELETE FROM documents WHERE id=900300")[0][0], OK_PACKET);
        // 快照仍见已删行；FOR UPDATE 当前读行已删 → 聚合为 0
        assert_eq!(sum(&mut main, "SELECT SUM(k) FROM documents WHERE id=900300"), "5");
        assert_eq!(
            sum(
                &mut main,
                "SELECT SUM(k) FROM documents WHERE id=900300 FOR UPDATE"
            ),
            "0"
        );
        assert_eq!(main.query("ROLLBACK")[0][0], OK_PACKET);
    }

    #[test]
    fn txn_between_consistent_read_no_phantom_c4() {
        // 缺陷 B（C4 复现）：BETWEEN 一致读须快照隔离——他事务在区间内插入并提交后，
        // 同事务重复一致读不得出现幻影行
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let _srv = std::thread::spawn(move || {
            server.serve(&addr.to_string()).expect("serve 失败");
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect(addr).is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let mut connect = || {
            let mut c = TestClient::connect(addr);
            let (scramble, _) = c.handshake();
            c.authenticate("root", "secret", &scramble);
            c
        };
        let mut main = connect();
        let mut aux = connect();
        // 数据行 id 提取（结果集：列数+列定义×2+EOF 后为数据行，末尾 EOF）
        let row_ids = |r: &Vec<Vec<u8>>| -> Vec<String> {
            let mut out = Vec::new();
            for row in r.iter().skip(4).take(r.len().saturating_sub(5)) {
                let mut p = 0usize;
                let n = read_lenenc(row, &mut p).unwrap() as usize;
                out.push(String::from_utf8(row[p..p + n].to_vec()).unwrap());
            }
            out
        };
        let sql = "SELECT * FROM documents WHERE id BETWEEN 900400 AND 900402 ORDER BY id";
        assert_eq!(
            main.query("INSERT INTO documents (id, doc) VALUES (900401, '{\"k\":0}')")[0][0],
            OK_PACKET
        );
        assert_eq!(main.query("BEGIN")[0][0], OK_PACKET);
        // 第一次一致读：仅 900401
        let first = main.query(sql);
        assert_eq!(row_ids(&first), vec!["900401"], "快照仅含 900401");
        // aux 他事务在区间内插入 900400 并提交
        assert_eq!(
            aux.query("INSERT INTO documents (id, doc) VALUES (900400, '{\"k\":1}')")[0][0],
            OK_PACKET
        );
        // 同事务重复一致读：快照不得见幻影 900400
        let second = main.query(sql);
        assert_eq!(
            row_ids(&second),
            vec!["900401"],
            "RR 快照隔离：区间内他事务插入不可见"
        );
        assert_eq!(main.query("ROLLBACK")[0][0], OK_PACKET);
    }

    #[test]
    fn txn_between_no_phantom_col_insert_c4b() {
        // 缺陷 B（权威形态复现）：列式 INSERT (id,val) + SELECT id,val BETWEEN——快照一致读须隐藏幻影
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let _srv = std::thread::spawn(move || {
            server.serve(&addr.to_string()).expect("serve 失败");
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect(addr).is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let mut connect = || {
            let mut c = TestClient::connect(addr);
            let (scramble, _) = c.handshake();
            c.authenticate("root", "secret", &scramble);
            c
        };
        let mut main = connect();
        let mut aux = connect();
        let row_ids = |r: &Vec<Vec<u8>>| -> Vec<String> {
            let mut out = Vec::new();
            for row in r.iter().skip(4).take(r.len().saturating_sub(5)) {
                let mut p = 0usize;
                let n = read_lenenc(row, &mut p).unwrap() as usize;
                out.push(String::from_utf8(row[p..p + n].to_vec()).unwrap());
            }
            out
        };
        let sql = "SELECT id,val FROM t_test WHERE id BETWEEN 900400 AND 900402 ORDER BY id";
        // pre（aux autocommit，BEGIN 前）：900401 val=0
        assert_eq!(
            aux.query("INSERT INTO t_test(id,val) VALUES(900401,0)")[0][0],
            OK_PACKET
        );
        assert_eq!(main.query("BEGIN")[0][0], OK_PACKET);
        // 首读（一致读）建立快照：仅 900401
        assert_eq!(row_ids(&main.query(sql)), vec!["900401"]);
        // 他事务区间内插入 900400 并提交
        assert_eq!(
            aux.query("INSERT INTO t_test(id,val) VALUES(900400,1)")[0][0],
            OK_PACKET
        );
        // 重复一致读：快照不得见幻影 900400
        assert_eq!(row_ids(&main.query(sql)), vec!["900401"], "RR 快照隔离：列式插入幻影不可见");
        assert_eq!(main.query("ROLLBACK")[0][0], OK_PACKET);
    }

    #[test]
    fn txn_update_delete_where_in_and_predicate() {
        // d txn 路径：事务内 UPDATE/DELETE … WHERE id IN(...) 与字段条件——攒批可见 + 回滚原子
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let _srv = std::thread::spawn(move || {
            server.serve(&addr.to_string()).expect("serve 失败");
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect(addr).is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let mut c = TestClient::connect(addr);
        let (scramble, _) = c.handshake();
        c.authenticate("root", "secret", &scramble);
        let mut base = |id: u64, k: i64, s: &str| {
            let doc = format!("{{\"k\":{k},\"s\":\"{s}\"}}");
            let sql = format!("INSERT INTO documents (id, doc) VALUES ({id}, '{doc}')");
            assert_eq!(c.query(&sql)[0][0], OK_PACKET);
        };
        base(1, 1, "a");
        base(2, 2, "b");
        base(3, 3, "a");
        base(4, 4, "b");
        let sum = |c: &mut TestClient, sql: &str| -> String {
            let r = c.query(sql);
            let row = &r[r.len() - 2];
            let mut p = 0usize;
            let n = read_lenenc(row, &mut p).unwrap() as usize;
            String::from_utf8(row[p..p + n].to_vec()).unwrap()
        };
        assert_eq!(c.query("BEGIN")[0][0], OK_PACKET);
        // WHERE id IN (2,3)：k+1 → doc2 3、doc3 4（事务内立即可见）
        assert_eq!(
            c.query("UPDATE documents SET k=k+1 WHERE id IN (2, 3)")[0][0],
            OK_PACKET
        );
        assert_eq!(sum(&mut c, "SELECT SUM(k) FROM documents WHERE s='b'"), "7"); // 2→3 + 4
        assert_eq!(sum(&mut c, "SELECT SUM(k) FROM documents WHERE s='a'"), "5"); // 1 + 3→4
        // 字段条件 UPDATE：s='a' 全部 +1 → doc1 2、doc3 5（含上一步自增后的自写值）
        assert_eq!(
            c.query("UPDATE documents SET k=k+1 WHERE s='a'")[0][0],
            OK_PACKET
        );
        assert_eq!(sum(&mut c, "SELECT SUM(k) FROM documents WHERE s='a'"), "7");
        // DELETE WHERE id IN (1,4)（含字段自写 doc1）：事务视图排除
        assert_eq!(
            c.query("DELETE FROM documents WHERE id IN (1, 4)")[0][0],
            OK_PACKET
        );
        assert_eq!(sum(&mut c, "SELECT SUM(k) FROM documents WHERE s='b'"), "3"); // 仅 doc2
        assert_eq!(sum(&mut c, "SELECT SUM(k) FROM documents WHERE s='a'"), "5"); // 仅 doc3
        // 字段条件 DELETE：s='b' 剩余 → doc2 删除
        assert_eq!(
            c.query("DELETE FROM documents WHERE s='b'")[0][0],
            OK_PACKET
        );
        assert_eq!(sum(&mut c, "SELECT SUM(k) FROM documents WHERE s='b'"), "0");
        assert_eq!(c.query("ROLLBACK")[0][0], OK_PACKET);
        // 回滚原子：全部恢复原值
        assert_eq!(sum(&mut c, "SELECT SUM(k) FROM documents WHERE s='a'"), "4");
        assert_eq!(sum(&mut c, "SELECT SUM(k) FROM documents WHERE s='b'"), "6");
    }

    #[test]
    fn txn_nested_begin_is_error_and_idle_commit_ok() {
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        let addr = server.serve_once("127.0.0.1:0").unwrap();
        let mut c = TestClient::connect(addr);
        let (scramble, _) = c.handshake();
        c.authenticate("root", "secret", &scramble);
        // 无活动事务 COMMIT → OK（MySQL 空提交语义）
        assert_eq!(c.query("COMMIT")[0][0], OK_PACKET);
        assert_eq!(c.query("ROLLBACK")[0][0], OK_PACKET);
        // 嵌套 BEGIN → 错误
        assert_eq!(c.query("BEGIN")[0][0], OK_PACKET);
        let r2 = c.query("BEGIN");
        assert_eq!(r2[0][0], ERR_PACKET);
        assert_eq!(c.query("ROLLBACK")[0][0], OK_PACKET);
    }

    // ---------- H-5：预处理语句 ----------

    #[test]
    fn stmt_prepare_execute_roundtrip() {
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        let addr = server.serve_once("127.0.0.1:0").unwrap();
        let mut c = TestClient::connect(addr);
        let (scramble, _) = c.handshake();
        c.authenticate("root", "secret", &scramble);

        // 预插入一条
        assert_eq!(
            c.query("INSERT INTO documents (id, doc) VALUES (7, '{\"p\":1}')")[0][0],
            OK_PACKET
        );
        // PREPARE：SELECT * FROM documents WHERE id=?
        let prep = c.stmt_prepare("SELECT * FROM documents WHERE id=?");
        assert_eq!(prep[0][0], OK_PACKET, "PREPARE_OK 头");
        let stmt_id = u32::from_le_bytes(prep[0][1..5].try_into().unwrap());
        assert_eq!(stmt_id, 1);
        assert_eq!(u16::from_le_bytes(prep[0][7..9].try_into().unwrap()), 1, "1 个参数");

        // EXECUTE：参数 LONGLONG=7 → 结果集
        let packets = c.stmt_execute(stmt_id, 7u64);
        assert_eq!(packets[0][0], 2, "两列 id/doc");
        let row = &packets[packets.len() - 2];
        let mut p = 0usize;
        let n = read_lenenc(&row, &mut p).unwrap();
        assert_eq!(n, 1);
        assert_eq!(&row[p..p + 1], b"7", "EXECUTE 参数 7 命中");

        // EXECUTE 字符串参数（doc 搜索参数场景：INSERT 占位）
        let prep2 = c.stmt_prepare("INSERT INTO documents (id, doc) VALUES (?, ?)");
        assert_eq!(
            u16::from_le_bytes(prep2[0][7..9].try_into().unwrap()),
            2,
            "2 个参数"
        );
        let stmt_id2 = u32::from_le_bytes(prep2[0][1..5].try_into().unwrap());
        // EXECUTE：LONGLONG=8 + 字符串 '{"x":1}'
        let ok = c.stmt_execute_str(stmt_id2, 8u64, r#"{"x":1}"#);
        assert_eq!(ok[0][0], OK_PACKET);
        let sel = c.query("SELECT * FROM documents WHERE id=8");
        let row8 = &sel[sel.len() - 2];
        let mut p8 = 0usize;
        let _ = read_lenenc(&row8, &mut p8).unwrap();
        assert_eq!(&row8[p8..p8 + 1], b"8", "预处理 INSERT 生效");
    }

    #[test]
    fn stmt_execute_unknown_id_is_error() {
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        let addr = server.serve_once("127.0.0.1:0").unwrap();
        let mut c = TestClient::connect(addr);
        let (scramble, _) = c.handshake();
        c.authenticate("root", "secret", &scramble);
        let r = c.stmt_execute(999, 1u64);
        assert_eq!(r[0][0], ERR_PACKET, "未知 stmt_id → 错误");
    }

    #[test]
    fn stmt_execute_concurrent_selects_all_succeed() {
        // I 项高并发：预处理 SELECT 走 RwLock 读锁（旧实现全走写锁串行）——多连接并发
        // PREPARE/EXECUTE point_select 全部成功、无死锁（读读并行路径正确性）
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        // serve 后台阻塞 accept（I 项小栈连接线程）；预取随机端口（bind-drop 竞态概率极低）
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let _srv = std::thread::spawn(move || {
            server.serve(&addr.to_string()).expect("serve 失败");
        });
        // 等待 accept 就绪（探测连接成功即就绪，连接随即被关闭）
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect(addr).is_ok() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "server 5s 内未就绪"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let n_threads = 8;
        let mut handles = Vec::new();
        for t in 0..n_threads {
            handles.push(std::thread::spawn(move || {
                let mut c = TestClient::connect(addr);
                let (scramble, _) = c.handshake();
                c.authenticate("root", "secret", &scramble);
                let prep = c.stmt_prepare("SELECT * FROM documents WHERE id=?");
                let stmt_id = u32::from_le_bytes(prep[0][1..5].try_into().unwrap());
                let mut ok = 0u32;
                for i in 1..=30u64 {
                    let pk = c.stmt_execute(stmt_id, i);
                    if pk[0][0] != ERR_PACKET {
                        ok += 1;
                    }
                }
                (t, ok)
            }));
        }
        let mut total = 0u32;
        for h in handles {
            let (t, ok) = h.join().expect("并发 EXECUTE 线程应正常结束");
            assert!(ok > 0, "线程 {t} 的 EXECUTE 全部失败");
            total += ok;
        }
        assert_eq!(total, n_threads * 30, "并发预处理读全部成功（无死锁）");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_server_protocol_roundtrip() {
        // I 项异步协程运行时：serve_async 协议往返（握手 + 认证 + SELECT + PREPARE/EXECUTE）
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let _srv = tokio::spawn(async move {
            server
                .serve_async(&addr.to_string())
                .await
                .expect("async serve 失败");
        });
        // 等待 accept 就绪（探测连接成功即就绪，连接随即被关闭）
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect(addr).is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "async server 未就绪");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // 同步 TestClient 连异步 server：握手 + 认证 + SELECT 往返
        let mut c = TestClient::connect(addr);
        let (scramble, _) = c.handshake();
        c.authenticate("root", "secret", &scramble);
        let packets = c.query("SELECT * FROM documents WHERE id=1");
        assert!(packets.len() >= 3, "结果集多包（列定义+EOF+行尾）");
        // 预处理往返（异步路径 spawn_blocking 执行查询）
        let prep = c.stmt_prepare("SELECT * FROM documents WHERE id=?");
        let stmt_id = u32::from_le_bytes(prep[0][1..5].try_into().unwrap());
        let pk = c.stmt_execute(stmt_id, 7u64);
        assert_ne!(pk[0][0], ERR_PACKET, "异步路径预处理 EXECUTE 成功");
        // 写语句（走写锁）
        let ins = c.query("INSERT INTO documents (id, doc) VALUES (42, '{\"a\":1}')");
        assert_eq!(ins[0][0], OK_PACKET);
        let sel = c.query("SELECT * FROM documents WHERE id=42");
        let row = &sel[sel.len() - 2];
        let mut p = 0usize;
        let _ = read_lenenc(&row, &mut p).unwrap();
        assert_eq!(&row[p..p + 2], b"42", "异步路径写入可见");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn async_server_concurrent_clients_all_succeed() {
        // I 项异步协程：serve_async + 8 并发客户端查询全成功（连接 task 不占 OS 线程）
        let engine = test_engine();
        let server = DbServer::new(engine, "root", "secret");
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let _srv = tokio::spawn(async move {
            server
                .serve_async(&addr.to_string())
                .await
                .expect("async serve 失败");
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect(addr).is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "async server 未就绪");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let n_threads = 8;
        let mut handles = Vec::new();
        for t in 0..n_threads {
            handles.push(std::thread::spawn(move || {
                let mut c = TestClient::connect(addr);
                let (scramble, _) = c.handshake();
                c.authenticate("root", "secret", &scramble);
                let mut ok = 0u32;
                for i in 1..=20u64 {
                    let pk = c.query(&format!("SELECT * FROM documents WHERE id={i}"));
                    if pk[0][0] != ERR_PACKET {
                        ok += 1;
                    }
                }
                (t, ok)
            }));
        }
        let mut total = 0u32;
        for h in handles {
            let (t, ok) = h.join().expect("并发线程应正常结束");
            assert!(ok > 0, "线程 {t} 查询全部失败");
            total += ok;
        }
        assert_eq!(total, n_threads * 20, "异步服务并发查询全部成功");
    }
    #[test]
    fn m1_multitable_same_id_isolation() {
        // §26 M1：双表同 SQL id 共存（docid = table_id<<48 | row 隔离）+ 点查路由 +
        // 更新隔离 + 同表重复 1062（默认表 documents 之外的隔离语义核心）
        let mut engine = test_engine();
        let auto = AtomicU64::new(1);
        let r1 = insert_response(
            &mut engine,
            "INSERT INTO t_a(id, doc) VALUES(7, '{\"v\":\"a7\"}')",
            &auto,
        );
        let r2 = insert_response(
            &mut engine,
            "INSERT INTO t_b(id, doc) VALUES(7, '{\"v\":\"b7\"}')",
            &auto,
        );
        assert!(
            matches!(r1, QueryResponse::Ok(1, 7)) && matches!(r2, QueryResponse::Ok(1, 7)),
            "双表同 id 7 均成功且 last insert id = 7"
        );
        // 同表重复 id → 1062（表级主键空间内判重）
        let dup = insert_response(&mut engine, "INSERT INTO t_a(id, doc) VALUES(7, '{}')", &auto);
        assert!(matches!(dup, QueryResponse::Err(1062, _)), "同表重复应 1062");
        // 他表同 id 仍可插（跨表不判重）
        let okb = insert_response(&mut engine, "INSERT INTO t_b(id, doc) VALUES(8, '{\"v\":\"b8\"}')", &auto);
        assert!(matches!(okb, QueryResponse::Ok(1, 8)));
        // 点查路由：各表只返回自属行
        let rows_of = |e: &Engine, sql: &str| -> Vec<Vec<Vec<u8>>> {
            match select_response(e, sql) {
                QueryResponse::Set { rows, .. } => rows,
                _ => panic!("应为 ResultSet: {sql}"),
            }
        };
        let ra = rows_of(&engine, "SELECT id, v FROM t_a WHERE id=7");
        let rb = rows_of(&engine, "SELECT id, v FROM t_b WHERE id=7");
        assert_eq!(ra.len(), 1);
        assert_eq!(rb.len(), 1);
        assert_eq!(ra[0], vec![b"7".to_vec(), b"a7".to_vec()], "t_a 自属行");
        assert_eq!(rb[0], vec![b"7".to_vec(), b"b7".to_vec()], "t_b 自属行");
        // 更新 t_a 不影响 t_b（同 id 不同表互不可见）
        let u = update_response(&mut engine, "UPDATE t_a SET doc='{\"v\":\"a7b\"}' WHERE id=7");
        assert!(matches!(u, QueryResponse::Ok(1, _)));
        let rb2 = rows_of(&engine, "SELECT v FROM t_b WHERE id=7");
        assert_eq!(rb2[0][0], b"b7", "t_b 行不受 t_a 更新影响");
        // 表级范围窗口不跨表：t_a BETWEEN 不含 t_b 行
        let win = rows_of(&engine, "SELECT id FROM t_b WHERE id BETWEEN 1 AND 10");
        assert_eq!(win.len(), 2, "t_b 区间窗口仅本表 2 行（7、8）");
    }
    #[test]
    fn m3_multitable_flush_compact_drop() {
        // M3 e2e：主数据列族 flush/compaction 按表切分后，双表同 id 仍隔离；
        // 强制落盘 + 收敛压缩后读回一致；DROP 单表（逻辑删 + 表文件物理回收）不影响他表
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        // 删除位图按 docid 稠密寻址——多表高位 docid（table<<48）下不可用（爆内存）；
        // 该 e2e 聚焦表切分/同表合并/DROP 语义，用传统 Tombstone 路径（生产多表单删同此前提）
        cfg.storage.deletion_bitmap_enabled = false;
        let mut engine = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        let auto = AtomicU64::new(1);
        let rows_of = |e: &Engine, sql: &str| -> Vec<Vec<Vec<u8>>> {
            match select_response(e, sql) {
                QueryResponse::Set { rows, .. } => rows,
                _ => panic!("应为 ResultSet: {sql}"),
            }
        };
        // 两表各 400 行（row 号对齐，验证跨表同 id 共存与表级切分落盘）
        for i in 1..=400u64 {
            let ra = insert_response(
                &mut engine,
                &format!("INSERT INTO t_a(id, doc) VALUES({i}, '{{\"v\":\"a{i}\"}}')"),
                &auto,
            );
            let rb = insert_response(
                &mut engine,
                &format!("INSERT INTO t_b(id, doc) VALUES({i}, '{{\"v\":\"b{i}\"}}')"),
                &auto,
            );
            assert!(matches!(ra, QueryResponse::Ok(1, _)) && matches!(rb, QueryResponse::Ok(1, _)));
        }
        // 覆盖 50 行（压缩跨版本合并需去重）
        for i in 1..=50u64 {
            let u = update_response(
                &mut engine,
                &format!("UPDATE t_a SET doc='{{\"v\":\"a{i}x\"}}' WHERE id={i}"),
            );
            assert!(matches!(u, QueryResponse::Ok(1, _)));
        }
        // 强制 flush（按表切分落盘）→ 压缩收敛（同表合并）
        engine.flush_primary().unwrap();
        let mut guard = 0;
        while engine.needs_compact() && guard < 50 {
            let _ = engine.compact().unwrap();
            guard += 1;
        }
        // 双表数据完整且隔离：同 id 各自归属
        let ta = rows_of(&engine, "SELECT id, v FROM t_a WHERE id BETWEEN 1 AND 400");
        let tb = rows_of(&engine, "SELECT id, v FROM t_b WHERE id BETWEEN 1 AND 400");
        assert_eq!(ta.len(), 400, "t_a 全量读回");
        assert_eq!(tb.len(), 400, "t_b 全量读回");
        assert_eq!(ta[0], vec![b"1".to_vec(), b"a1x".to_vec()], "覆盖生效");
        assert_eq!(ta[51], vec![b"52".to_vec(), b"a52".to_vec()], "未覆盖行保持原值");
        assert_eq!(tb[0], vec![b"1".to_vec(), b"b1".to_vec()], "t_b 不受 t_a 覆盖影响");
        assert_eq!(ta[399], vec![b"400".to_vec(), b"a400".to_vec()]);
        // DROP 单表：仅 t_b 清空，t_a 完好（逻辑删 + 该表文件物理回收）
        let s_auto = Arc::new(AtomicU64::new(1));
        let mut s = super::new_session(Arc::clone(&s_auto));
        match super::dispatch_query(&mut engine, "DROP TABLE t_b", &mut s) {
            QueryResponse::Ok(0, 0) => {}
            _ => panic!("DROP TABLE t_b 应成功"),
        }
        assert!(
            rows_of(&engine, "SELECT id FROM t_b WHERE id BETWEEN 1 AND 400").is_empty(),
            "t_b 行应清空"
        );
        assert_eq!(
            rows_of(&engine, "SELECT id FROM t_a WHERE id BETWEEN 1 AND 400").len(),
            400,
            "t_a 行不受 DROP t_b 影响"
        );
        // flush 后仍一致（表文件回收后重启级一致性由 CF 单测覆盖）
        engine.flush_primary().unwrap();
        assert_eq!(
            rows_of(&engine, "SELECT id FROM t_a WHERE id BETWEEN 1 AND 400").len(),
            400
        );
        assert!(rows_of(&engine, "SELECT id FROM t_b WHERE id BETWEEN 1 AND 400").is_empty());
    }
    #[test]
    fn p1_insert_ignore_and_on_duplicate_key() {
        // P1-1：INSERT IGNORE（冲突跳过不报 1062）/ ON DUPLICATE KEY UPDATE（冲突改更新）
        // —— 非事务 + 事务内一致；Plain 重复仍 1062（回归）
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut engine = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        let auto = AtomicU64::new(1);
        let rows_of = |e: &Engine, sql: &str| -> Vec<Vec<Vec<u8>>> {
            match select_response(e, sql) {
                QueryResponse::Set { rows, .. } => rows,
                _ => panic!("应为 ResultSet: {sql}"),
            }
        };
        // 种子
        assert!(matches!(
            insert_response(&mut engine, "INSERT INTO t_a(id, doc) VALUES(7, '{\"v\":\"a7\"}')", &auto),
            QueryResponse::Ok(1, 7)
        ));
        // Plain 重复仍 1062（回归）
        assert!(matches!(
            insert_response(&mut engine, "INSERT INTO t_a(id, doc) VALUES(7, '{}')", &auto),
            QueryResponse::Err(1062, _)
        ));
        // INSERT IGNORE 重复 → 成功跳过（affected 0），行不变
        match insert_response(
            &mut engine,
            "INSERT IGNORE INTO t_a(id, doc) VALUES(7, '{\"v\":\"ignored\"}')",
            &auto,
        ) {
            QueryResponse::Ok(n, _) => assert_eq!(n, 0, "重复行应被忽略（不计 affected）"),
            _ => panic!("INSERT IGNORE 重复应 Ok"),
        }
        assert_eq!(rows_of(&engine, "SELECT v FROM t_a WHERE id=7")[0][0], b"a7");
        // INSERT IGNORE 新行 → 正常插入
        assert!(matches!(
            insert_response(&mut engine, "INSERT IGNORE INTO t_a(id, doc) VALUES(8, '{\"v\":\"a8\"}')", &auto),
            QueryResponse::Ok(1, 8)
        ));
        // 多行 IGNORE：重复(7)跳过、新(9)插入 → affected=1
        match insert_response(
            &mut engine,
            "INSERT IGNORE INTO t_a(id, doc) VALUES(7, '{}'), (9, '{\"v\":\"a9\"}')",
            &auto,
        ) {
            QueryResponse::Ok(n, _) => assert_eq!(n, 1, "多行 IGNORE 仅新行计 affected"),
            _ => panic!("多行 INSERT IGNORE 应 Ok"),
        }
        assert_eq!(rows_of(&engine, "SELECT v FROM t_a WHERE id=9")[0][0], b"a9");
        // ODKU 冲突整 doc 覆盖（doc=VALUES(doc)）→ affected 2、行更新
        match insert_response(
            &mut engine,
            "INSERT INTO t_a(id, doc) VALUES(7, '{\"v\":\"a7b\"}') ON DUPLICATE KEY UPDATE doc=VALUES(doc)",
            &auto,
        ) {
            QueryResponse::Ok(n, _) => assert_eq!(n, 2, "ODKU 更新 affected=2"),
            _ => panic!("ODKU 冲突应 Ok"),
        }
        assert_eq!(rows_of(&engine, "SELECT v FROM t_a WHERE id=7")[0][0], b"a7b");
        // ODKU 无冲突 → 普通插入 affected=1
        match insert_response(
            &mut engine,
            "INSERT INTO t_a(id, doc) VALUES(10, '{\"v\":\"a10\"}') ON DUPLICATE KEY UPDATE doc=VALUES(doc)",
            &auto,
        ) {
            QueryResponse::Ok(n, _) => assert_eq!(n, 1, "ODKU 无冲突 = 插入"),
            _ => panic!("ODKU 无冲突应 Ok"),
        }
        // ODKU 字段级：k=k+5 自增 + v='changed' 字面量（列名形态组装 doc：业务列=JSON 字段）
        match insert_response(
            &mut engine,
            "INSERT INTO t_a(id, doc) VALUES(7, '{\"v\":\"seed\",\"k\":1}') ON DUPLICATE KEY UPDATE k=k+5, v='changed'",
            &auto,
        ) {
            QueryResponse::Ok(n, _) => assert_eq!(n, 2),
            _ => panic!("ODKU 字段级应 Ok"),
        }
        {
            let v = rows_of(&engine, "SELECT v, k FROM t_a WHERE id=7");
            assert_eq!(v[0][0], b"changed", "ODKU 字面量赋值生效");
            assert_eq!(v[0][1], b"5", "ODKU 自增 k=k+5（旧无 k → 0+5）");
        }
        // VALUES(col) 引用插入 doc 字段（无 doc 列形态：业务列组装 JSON）
        match insert_response(
            &mut engine,
            "INSERT INTO t_a(id, k, v) VALUES(8, 99, 'x') ON DUPLICATE KEY UPDATE k=VALUES(k)",
            &auto,
        ) {
            QueryResponse::Ok(n, _) => assert_eq!(n, 2, "id=8 已存在（IGNORE 段插入）"),
            _ => panic!("ODKU VALUES(col) 应 Ok"),
        }
        {
            let v = rows_of(&engine, "SELECT k FROM t_a WHERE id=8");
            assert_eq!(v[0][0], b"99", "VALUES(k) 从插入 doc 取值生效");
        }
        // 事务内：IGNORE 跳过 + ODKU 更新（BEGIN/COMMIT）
        let s_auto = Arc::new(AtomicU64::new(1));
        let mut s = super::new_session(Arc::clone(&s_auto));
        assert!(matches!(
            super::dispatch_query(&mut engine, "BEGIN", &mut s),
            QueryResponse::Ok(0, 0)
        ));
        match super::dispatch_query(
            &mut engine,
            "INSERT IGNORE INTO t_a(id, doc) VALUES(7, '{\"v\":\"txn-ignored\"}')",
            &mut s,
        ) {
            QueryResponse::Ok(n, _) => assert_eq!(n, 0, "事务内 IGNORE 重复跳过"),
            _ => panic!("事务内 IGNORE 应 Ok"),
        }
        match super::dispatch_query(
            &mut engine,
            "INSERT INTO t_a(id, doc) VALUES(7, '{\"v\":\"txn-odku\"}') ON DUPLICATE KEY UPDATE doc=VALUES(doc)",
            &mut s,
        ) {
            QueryResponse::Ok(n, _) => assert_eq!(n, 2, "事务内 ODKU 更新"),
            _ => panic!("事务内 ODKU 应 Ok"),
        }
        assert!(matches!(
            super::dispatch_query(&mut engine, "COMMIT", &mut s),
            QueryResponse::Ok(0, 0)
        ));
        assert_eq!(rows_of(&engine, "SELECT v FROM t_a WHERE id=7")[0][0], b"txn-odku");
    }
    #[test]
    fn p1_delete_from_table_without_where() {
        // P1-2：`DELETE FROM <表>`（无 WHERE）= 整表删除（仅本表区间，其它表/默认表不受影响）
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.storage.deletion_bitmap_enabled = false; // 多表高位 docid 删除需关位图（§26 约束）
        let mut engine = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        let auto = AtomicU64::new(1);
        let rows_of = |e: &Engine, sql: &str| -> Vec<Vec<Vec<u8>>> {
            match select_response(e, sql) {
                QueryResponse::Set { rows, .. } => rows,
                _ => panic!("应为 ResultSet: {sql}"),
            }
        };
        // 三表同 id 共存
        for i in 1..=5u64 {
            insert_response(&mut engine, &format!("INSERT INTO t_a(id, doc) VALUES({i}, '{{\"v\":\"a{i}\"}}')"), &auto);
            insert_response(&mut engine, &format!("INSERT INTO t_b(id, doc) VALUES({i}, '{{\"v\":\"b{i}\"}}')"), &auto);
        }
        for i in 1..=3u64 {
            insert_response(&mut engine, &format!("INSERT INTO documents(id, doc) VALUES({i}, '{{\"v\":\"d{i}\"}}')"), &auto);
        }
        // DELETE FROM t_a（无 WHERE）→ 仅清 t_a，t_b / documents 保留
        match delete_response(&mut engine, "DELETE FROM t_a") {
            QueryResponse::Ok(n, 0) => assert_eq!(n, 5, "t_a 全表删除 affected=5"),
            r => panic!("DELETE 全表应 Ok"),
        }
        assert!(rows_of(&engine, "SELECT id FROM t_a WHERE id BETWEEN 1 AND 10").is_empty());
        assert_eq!(rows_of(&engine, "SELECT id FROM t_b WHERE id BETWEEN 1 AND 10").len(), 5, "t_b 不受影响");
        assert_eq!(rows_of(&engine, "SELECT id FROM documents WHERE id BETWEEN 1 AND 10").len(), 3, "documents 不受影响");
        // 再插同 id 不 1062（表已清）
        assert!(matches!(
            insert_response(&mut engine, "INSERT INTO t_a(id, doc) VALUES(1, '{\"v\":\"a1b\"}')", &auto),
            QueryResponse::Ok(1, 1)
        ));
        // DELETE FROM documents（默认表，无 WHERE）→ 只清默认表
        match delete_response(&mut engine, "DELETE FROM documents") {
            QueryResponse::Ok(n, 0) => assert_eq!(n, 3),
            r => panic!("DELETE documents 全表应 Ok"),
        }
        assert!(rows_of(&engine, "SELECT id FROM documents WHERE id BETWEEN 1 AND 10").is_empty());
        assert_eq!(rows_of(&engine, "SELECT id FROM t_b WHERE id BETWEEN 1 AND 10").len(), 5);
        // 事务内 DELETE FROM t_b（无 WHERE）：快照删除，commit 原子
        let s_auto = Arc::new(AtomicU64::new(1));
        let mut s = super::new_session(Arc::clone(&s_auto));
        assert!(matches!(super::dispatch_query(&mut engine, "BEGIN", &mut s), QueryResponse::Ok(0, 0)));
        match super::dispatch_query(&mut engine, "DELETE FROM t_b", &mut s) {
            QueryResponse::Ok(n, _) => assert_eq!(n, 5, "事务内全表删除 affected=5"),
            r => panic!("事务内 DELETE 全表应 Ok"),
        }
        // 事务内 DELETE（未提交）同事务 SELECT：本事务视图已删除 → 空
        match super::dispatch_query(&mut engine, "SELECT id FROM t_b WHERE id BETWEEN 1 AND 10", &mut s) {
            QueryResponse::Set { rows, .. } => assert!(rows.is_empty(), "事务内 DELETE 后同事务读不可见"),
            r => panic!("事务内 SELECT 应 Set"),
        }
        // 外部引擎读（未提交）→ 仍可见（原子性）
        assert_eq!(rows_of(&engine, "SELECT id FROM t_b WHERE id BETWEEN 1 AND 10").len(), 5);
        assert!(matches!(super::dispatch_query(&mut engine, "COMMIT", &mut s), QueryResponse::Ok(0, 0)));
        assert!(rows_of(&engine, "SELECT id FROM t_b WHERE id BETWEEN 1 AND 10").is_empty(), "commit 后 t_b 清空");
        // 事务内回滚恢复
        assert!(matches!(super::dispatch_query(&mut engine, "BEGIN", &mut s), QueryResponse::Ok(0, 0)));
        assert!(matches!(super::dispatch_query(&mut engine, "DELETE FROM t_a", &mut s), QueryResponse::Ok(n, _) if n == 1));
        assert!(matches!(super::dispatch_query(&mut engine, "ROLLBACK", &mut s), QueryResponse::Ok(0, 0)));
        assert_eq!(rows_of(&engine, "SELECT id FROM t_a WHERE id BETWEEN 1 AND 10").len(), 1, "回滚后恢复");
    }
    #[test]
    fn p1_nondefault_aggregates_scoped_to_table() {
        // P1-3：非默认表 COUNT/SUM/GROUP BY 按**本表 docid 区间**执行（窗口聚合，不跨表串表）
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut engine = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        let auto = AtomicU64::new(1);
        let rows_of = |e: &Engine, sql: &str| -> Vec<Vec<Vec<u8>>> {
            match select_response(e, sql) {
                QueryResponse::Set { rows, .. } => rows,
                r => panic!("应为 ResultSet: {sql}"),
            }
        };
        // t_a：k=10*i（4 行，v='a'）；t_b：k=1000+i（同 id 不同值）；documents：k=7
        for i in 1..=4u64 {
            insert_response(
                &mut engine,
                &format!("INSERT INTO t_a(id, doc) VALUES({i}, '{{\"v\":\"a\",\"k\":{}}}')", i * 10),
                &auto,
            );
            insert_response(
                &mut engine,
                &format!("INSERT INTO t_b(id, doc) VALUES({i}, '{{\"v\":\"b\",\"k\":{}}}')", 1000 + i),
                &auto,
            );
        }
        insert_response(&mut engine, "INSERT INTO documents(id, doc) VALUES(1, '{\"v\":\"d\",\"k\":7}')", &auto);
        // COUNT(*) 无 WHERE：仅本表 4 行（不含 t_b / documents）
        assert_eq!(rows_of(&engine, "SELECT COUNT(*) FROM t_a")[0][0], b"4");
        assert_eq!(rows_of(&engine, "SELECT COUNT(*) FROM t_b")[0][0], b"4");
        assert_eq!(rows_of(&engine, "SELECT COUNT(*) FROM documents")[0][0], b"1", "默认表不受影响");
        // SUM(k) 按表区间（t_a=10+20+30+40=100；t_b 的 1001.. 不得混入）
        assert_eq!(rows_of(&engine, "SELECT SUM(k) FROM t_a")[0][0], b"100");
        assert_eq!(rows_of(&engine, "SELECT SUM(k) FROM t_b")[0][0], b"4010");
        // 字段谓词聚合（窗口内过滤）
        assert_eq!(rows_of(&engine, "SELECT COUNT(*) FROM t_a WHERE v='a'")[0][0], b"4");
        assert_eq!(rows_of(&engine, "SELECT COUNT(*) FROM t_b WHERE v='a'")[0][0], b"0", "t_b 无 v=a 行");
        assert_eq!(rows_of(&engine, "SELECT SUM(k) FROM t_a WHERE v='a'")[0][0], b"100");
        // GROUP BY 字符串字段 + COUNT（非默认表分组）
        let g = rows_of(&engine, "SELECT v, COUNT(*) FROM t_a GROUP BY v");
        assert_eq!(g.len(), 1, "t_a 仅一组");
        assert_eq!(g[0], vec![b"a".to_vec(), b"4".to_vec()]);
        let gb = rows_of(&engine, "SELECT v, COUNT(*) FROM t_b GROUP BY v");
        assert_eq!(gb.len(), 1);
        assert_eq!(gb[0], vec![b"b".to_vec(), b"4".to_vec()]);
        // GROUP BY 数值字段 + SUM（窗口内按表）
        let gk = rows_of(&engine, "SELECT k, COUNT(*) FROM t_a GROUP BY k ORDER BY k");
        assert_eq!(gk.len(), 4, "t_a 每组 k 各 1 行（4 组）");
        // MIN/MAX/AVG 同口径
        assert_eq!(rows_of(&engine, "SELECT MAX(k) FROM t_a")[0][0], b"40");
        assert_eq!(rows_of(&engine, "SELECT MIN(k) FROM t_b")[0][0], b"1001");
    }
    #[test]
    fn p1_for_update_current_read_semantics() {
        // P1-4 验证：SELECT … FOR UPDATE = 事务内**当前读**（最新已提交 + 自写），
        // 快照读不受影响（RR 幻影/不可重复读由当前读排除）；放行形态核对
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut engine = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        let a1 = Arc::new(AtomicU64::new(1));
        let a2 = Arc::new(AtomicU64::new(1));
        let mut s1 = super::new_session(Arc::clone(&a1));
        let mut s2 = super::new_session(Arc::clone(&a2));
        let rows_of = |e: &Engine, sql: &str| -> Vec<Vec<Vec<u8>>> {
            match select_response(e, sql) {
                QueryResponse::Set { rows, .. } => rows,
                _ => panic!("应为 ResultSet: {sql}"),
            }
        };
        // 基表：documents id=1..3；t_x id=1..2
        for i in 1..=3u64 {
            insert_response(&mut engine, &format!("INSERT INTO documents(id, doc) VALUES({i}, '{{\"k\":{i}}}')"), &a1);
        }
        for i in 1..=2u64 {
            insert_response(&mut engine, &format!("INSERT INTO t_x(id, doc) VALUES({i}, '{{\"k\":{}}}')", i * 100), &a1);
        }
        // s1 事务建立快照（RR）
        assert!(matches!(super::dispatch_query(&mut engine, "BEGIN", &mut s1), QueryResponse::Ok(0, 0)));
        // s2 并发事务插入 documents id=4 并提交（seq 高于 s1 快照）
        assert!(matches!(super::dispatch_query(&mut engine, "BEGIN", &mut s2), QueryResponse::Ok(0, 0)));
        match super::dispatch_query(&mut engine, "INSERT INTO documents(id, doc) VALUES(4, '{\"k\":4}')", &mut s2) {
            QueryResponse::Ok(1, _) => {}
            r => panic!("s2 插入失败"),
        }
        assert!(matches!(super::dispatch_query(&mut engine, "COMMIT", &mut s2), QueryResponse::Ok(0, 0)));
        // s1 普通快照读：BETWEEN 窗口仍 3 行（新行对快照不可见）
        match super::dispatch_query(&mut engine, "SELECT id FROM documents WHERE id BETWEEN 1 AND 10", &mut s1) {
            QueryResponse::Set { rows, .. } => assert_eq!(rows.len(), 3, "快照读不含 s2 已提交新行"),
            r => panic!("快照读失败"),
        }
        // s1 FOR UPDATE（当前读）：可见最新已提交（含 id=4）——RR 下由当前读排除幻影
        match super::dispatch_query(
            &mut engine,
            "SELECT id FROM documents WHERE id BETWEEN 1 AND 10 FOR UPDATE",
            &mut s1,
        ) {
            QueryResponse::Set { rows, .. } => assert_eq!(rows.len(), 4, "FOR UPDATE 当前读应含 s2 已提交新行"),
            r => panic!("FOR UPDATE 范围读失败"),
        }
        // 点查 FOR UPDATE 同语义 + 自写覆盖可见
        match super::dispatch_query(&mut engine, "SELECT id FROM documents WHERE id=4 FOR UPDATE", &mut s1) {
            QueryResponse::Set { rows, .. } => assert_eq!(rows.len(), 1),
            r => panic!("FOR UPDATE 点查失败"),
        }
        // P1-4 修复：FOR UPDATE 读到"快照外新提交行"后同事务 UPDATE + COMMIT 应成功
        //（当前读锁定版本期间无并发再改 → 放行，对齐 MySQL RR 当前读后写语义）
        match super::dispatch_query(&mut engine, "UPDATE documents SET doc='{\"k\":44}' WHERE id=4", &mut s1) {
            QueryResponse::Ok(1, _) => {}
            r => panic!("FOR UPDATE 后同事务 UPDATE 失败"),
        }
        assert!(matches!(super::dispatch_query(&mut engine, "COMMIT", &mut s1), QueryResponse::Ok(0, 0)));
        // 非默认表 FOR UPDATE：主键窗口放行
        assert!(matches!(super::dispatch_query(&mut engine, "BEGIN", &mut s1), QueryResponse::Ok(0, 0)));
        match super::dispatch_query(&mut engine, "SELECT id FROM t_x WHERE id BETWEEN 1 AND 10 FOR UPDATE", &mut s1) {
            QueryResponse::Set { rows, .. } => assert_eq!(rows.len(), 2, "非默认表窗口 FOR UPDATE 放行"),
            r => panic!("非默认表 FOR UPDATE 窗口失败"),
        }
        assert!(matches!(super::dispatch_query(&mut engine, "ROLLBACK", &mut s1), QueryResponse::Ok(0, 0)));
        // 边界核对：非默认表**字段谓词**事务查询仍 1064（§26 边界，主键/窗口可用）
        assert!(matches!(super::dispatch_query(&mut engine, "BEGIN", &mut s1), QueryResponse::Ok(0, 0)));
        assert!(matches!(
            super::dispatch_query(&mut engine, "SELECT id FROM t_x WHERE k=100", &mut s1),
            QueryResponse::Err(1064, _)
        ));
        assert!(matches!(super::dispatch_query(&mut engine, "ROLLBACK", &mut s1), QueryResponse::Ok(0, 0)));
        // 已提交数据核对：FOR UPDATE 读到并同事务 UPDATE 的 id=4 → k=44；未写的 id=1 保持原值
        assert_eq!(rows_of(&engine, "SELECT k FROM documents WHERE id=4")[0][0], b"44");
        assert_eq!(rows_of(&engine, "SELECT k FROM documents WHERE id=1")[0][0], b"1");
    }
    #[test]
    fn p1_for_update_conflict_on_concurrent_modify() {
        // P1-4：FOR UPDATE 当前读锁定集的乐观正确性——
        // ① 正例：FOR UPDATE 读到（快照外）行后，若期间无并发再改 → 同事务写 commit 成功；
        // ② 负例：FOR UPDATE 之后、commit 前，他事务又改了同键（seq 前进）→ 提交冲突；
        // ③ 回归：未 FOR UPDATE 的快照写仍按旧冲突判定（锁定集不误放行）。
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut engine = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        let a1 = Arc::new(AtomicU64::new(1));
        let a2 = Arc::new(AtomicU64::new(1));
        let a3 = Arc::new(AtomicU64::new(1));
        let mut s1 = super::new_session(Arc::clone(&a1));
        let mut s2 = super::new_session(Arc::clone(&a2));
        let mut s3 = super::new_session(Arc::clone(&a3));
        let rows_of = |e: &Engine, sql: &str| -> Vec<Vec<Vec<u8>>> {
            match select_response(e, sql) {
                QueryResponse::Set { rows, .. } => rows,
                _ => panic!("应为 ResultSet: {sql}"),
            }
        };
        for i in 1..=2u64 {
            insert_response(&mut engine, &format!("INSERT INTO documents(id, doc) VALUES({i}, '{{\"k\":{i}}}')"), &a1);
        }
        // ① 正例（锁定期无并发再改 → 放行）
        assert!(matches!(super::dispatch_query(&mut engine, "BEGIN", &mut s1), QueryResponse::Ok(0, 0)));
        assert!(matches!(
            super::dispatch_query(&mut engine, "SELECT id FROM documents WHERE id=2 FOR UPDATE", &mut s1),
            QueryResponse::Set { rows, .. } if rows.len() == 1
        ));
        assert!(matches!(super::dispatch_query(&mut engine, "UPDATE documents SET doc='{\"k\":20}' WHERE id=2", &mut s1), QueryResponse::Ok(1, _)));
        assert!(matches!(super::dispatch_query(&mut engine, "COMMIT", &mut s1), QueryResponse::Ok(0, 0)), "无并发再改 → 当前读后写应提交成功");
        assert_eq!(rows_of(&engine, "SELECT k FROM documents WHERE id=2")[0][0], b"20");
        // ② 负例：s1 FOR UPDATE 读锁后，s2 再改同键并提交 → s1 写同键 commit 冲突
        assert!(matches!(super::dispatch_query(&mut engine, "BEGIN", &mut s1), QueryResponse::Ok(0, 0)));
        assert!(matches!(
            super::dispatch_query(&mut engine, "SELECT id FROM documents WHERE id=1 FOR UPDATE", &mut s1),
            QueryResponse::Set { rows, .. } if rows.len() == 1
        ));
        assert!(matches!(super::dispatch_query(&mut engine, "BEGIN", &mut s2), QueryResponse::Ok(0, 0)));
        assert!(matches!(super::dispatch_query(&mut engine, "UPDATE documents SET doc='{\"k\":100}' WHERE id=1", &mut s2), QueryResponse::Ok(1, _)));
        assert!(matches!(super::dispatch_query(&mut engine, "COMMIT", &mut s2), QueryResponse::Ok(0, 0)));
        assert!(matches!(super::dispatch_query(&mut engine, "UPDATE documents SET doc='{\"k\":9}' WHERE id=1", &mut s1), QueryResponse::Ok(1, _)));
        match super::dispatch_query(&mut engine, "COMMIT", &mut s1) {
            QueryResponse::Err(_, _) => {} // 并发再改（seq 前进）→ 乐观锁冲突
            r => panic!("并发再改后提交应冲突"),
        }
        assert_eq!(rows_of(&engine, "SELECT k FROM documents WHERE id=1")[0][0], b"100", "冲突回滚不得覆盖 s2 新值");
        // ③ 回归：未 FOR UPDATE 的快照写 → 他事务先提交 → 提交冲突（锁定集不放行未锁键）
        assert!(matches!(super::dispatch_query(&mut engine, "BEGIN", &mut s3), QueryResponse::Ok(0, 0)));
        assert!(matches!(super::dispatch_query(&mut engine, "BEGIN", &mut s2), QueryResponse::Ok(0, 0)));
        assert!(matches!(super::dispatch_query(&mut engine, "UPDATE documents SET doc='{\"k\":200}' WHERE id=1", &mut s2), QueryResponse::Ok(1, _)));
        assert!(matches!(super::dispatch_query(&mut engine, "COMMIT", &mut s2), QueryResponse::Ok(0, 0)));
        assert!(matches!(super::dispatch_query(&mut engine, "UPDATE documents SET doc='{\"k\":300}' WHERE id=1", &mut s3), QueryResponse::Ok(1, _)));
        match super::dispatch_query(&mut engine, "COMMIT", &mut s3) {
            QueryResponse::Err(_, _) => {} // 未当前读 → 快照后并发写仍冲突（回归）
            r => panic!("未 FOR UPDATE 的快照写应冲突"),
        }
        assert_eq!(rows_of(&engine, "SELECT k FROM documents WHERE id=1")[0][0], b"200");
    }
    #[test]
    fn m1_p0_replace_and_null_auto() {
        // P0-2：REPLACE 覆盖写（存在删后插，不再 1062）；P0-1：VALUES(NULL, ...) → auto
        let mut engine = test_engine();
        let auto = AtomicU64::new(1);
        assert!(matches!(
            insert_response(&mut engine, "INSERT INTO documents VALUES (5, '{\"v\":\"old\"}')", &auto),
            QueryResponse::Ok(1, 5)
        ));
        let r5 = replace_response(&mut engine, "REPLACE INTO documents VALUES (5, '{\"v\":\"new\"}')", &auto);
        match r5 {
            QueryResponse::Ok(1, 5) => {}
            QueryResponse::Err(c, m) => panic!("replace err {c}: {m}"),
            _ => panic!("replace 返回异常"),
        }
        assert!(matches!(
            replace_response(&mut engine, "REPLACE INTO documents VALUES (6, '{\"v\":\"six\"}')", &auto),
            QueryResponse::Ok(1, 6)
        ));
        let rows = match select_response(&engine, "SELECT id, v FROM documents WHERE id BETWEEN 5 AND 6") {
            QueryResponse::Set { rows, .. } => rows,
            _ => panic!("应为 ResultSet"),
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], b"new", "REPLACE 覆盖生效");
        assert!(matches!(
            insert_response(&mut engine, "INSERT INTO documents VALUES (6, '{}')", &auto),
            QueryResponse::Err(1062, _)
        ));
        assert!(matches!(
            insert_response(&mut engine, "INSERT INTO documents VALUES (NULL, '{\"auto\":1}')", &auto),
            QueryResponse::Ok(1, _)
        ));
        let row = match select_response(&engine, "SELECT id FROM documents WHERE id=7") {
            QueryResponse::Set { rows, .. } => rows,
            _ => panic!("应为 ResultSet"),
        };
        assert_eq!(row.len(), 1);
        assert_eq!(row[0][0], b"7", "NULL → auto 落 7");
    }
    #[test]
    fn m2_per_table_auto_row_id() {
        // §26 M2：非默认表 auto（无 id 列 / VALUES(NULL)）——表区间内唯一、显式 id 不撞
        let mut engine = test_engine();
        let auto = AtomicU64::new(1);
        for i in 1..=3u64 {
            insert_response(
                &mut engine,
                &format!("INSERT INTO t_x(id, doc) VALUES ({i}, '{{\"v\":\"e{i}\"}}')"),
                &auto,
            );
        }
        // 无 id 列（parse id=0 → auto 探测）：全局计数低位 1/2/3 被显式占 → 跳至 4
        let r = insert_response(&mut engine, "INSERT INTO t_x(k, c) VALUES(1, 'a')", &auto);
        assert!(matches!(r, QueryResponse::Ok(1, 4)), "auto 跳显式占位落 id=4");
        let r2 = insert_response(&mut engine, "INSERT INTO t_x(k) VALUES(2)", &auto);
        assert!(matches!(r2, QueryResponse::Ok(1, 5)), "auto 续 5");
        let rows = match select_response(&engine, "SELECT id, k FROM t_x WHERE id BETWEEN 4 AND 5") {
            QueryResponse::Set { rows, .. } => rows,
            _ => panic!("应为 ResultSet"),
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], b"4");
        assert_eq!(rows[1][0], b"5");
        // 他表显式同 id=4 独立（不同表区间，不冲突）
        insert_response(
            &mut engine,
            "INSERT INTO t_y(id, doc) VALUES (4, '{\"v\":\"y\"}')",
            &auto,
        );
        let qy = match select_response(&engine, "SELECT id FROM t_y WHERE id=4") {
            QueryResponse::Set { rows, .. } => rows,
            _ => panic!("应为 ResultSet"),
        };
        assert_eq!(qy.len(), 1);
    }

    // ---------- 2026-09-05：SELECT 投影列表达式/函数值（阶段 A） ----------
    #[test]
    fn expr_projection_response_arithmetic_and_funcs() {
        // 端到端（协议层 select_response）：算术/拼接/字符串函数投影输出计算列
        let mut engine = test_engine();
        let auto = AtomicU64::new(1);
        for i in 1..=3u64 {
            let sql = format!(
                "INSERT INTO documents(id, doc) VALUES ({i}, '{{\"k\":{i},\"name\":\"n{i}\",\"flag\":true}}')"
            );
            insert_response(&mut engine, &sql, &auto);
        }
        // ① 算术投影（整型保持）；列名 = 表达式规范文本
        let resp = select_response(&engine, "SELECT k * 2 FROM documents LIMIT 2");
        let QueryResponse::Set { columns, rows } = resp else { panic!("应为 Set") };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], b"2");
        assert_eq!(rows[1][0], b"4");
        let colname = String::from_utf8_lossy(&columns[0]).to_string();
        assert!(colname.contains("k * 2"), "表达式列名应为其规范文本: {colname}");
        // ② 字符串函数 + CONCAT（NULL 不出现于此数据）
        let resp2 = select_response(&engine, "SELECT CONCAT(name, '-x'), UPPER(name) FROM documents LIMIT 1");
        let QueryResponse::Set { rows: rows2, .. } = resp2 else { panic!("应为 Set") };
        assert_eq!(rows2[0][0], b"n1-x");
        assert_eq!(rows2[0][1], b"N1");
        // ③ 普通字段与表达式混排（plain 列 + 表达式列）
        let resp3 = select_response(&engine, "SELECT name, k + 1 FROM documents LIMIT 1");
        let QueryResponse::Set { rows: rows3, .. } = resp3 else { panic!("应为 Set") };
        assert_eq!(rows3[0][0], b"n1");
        assert_eq!(rows3[0][1], b"2");
        // ④ 表达式列与主键点查/区间组合 → 阶段 B 1064（防静默错位）
        assert!(matches!(
            select_response(&engine, "SELECT k * 2 FROM documents WHERE id=2"),
            QueryResponse::Err(1064, _)
        ));
        assert!(matches!(
            select_response(&engine, "SELECT k * 2 FROM documents WHERE id BETWEEN 1 AND 2"),
            QueryResponse::Err(1064, _)
        ));
    }

    #[test]
    fn expr_projection_division_null_and_guards() {
        let mut engine = test_engine();
        let auto = AtomicU64::new(1);
        insert_response(&mut engine, "INSERT INTO documents(id, doc) VALUES (1, '{\"k\":10,\"b\":0,\"v\":null,\"s\":\"x\"}')", &auto);
        insert_response(&mut engine, "INSERT INTO documents(id, doc) VALUES (2, '{\"k\":3}')", &auto);
        // 除法 → 浮点；缺字段行 → NULL 传播（0xfb 哨兵 cell）
        let rows = match select_response(&engine, "SELECT k / 2, k * 2 FROM documents ORDER BY id LIMIT 2") {
            QueryResponse::Set { rows, .. } => rows,
            QueryResponse::Err(c, m) => panic!("query err {c}: {m}"),
            _ => panic!("非 Set 响应"),
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], b"5.0"); // 10/2 → DOUBLE 文本（serde f64 5.0 → "5.0"）
        assert_eq!(rows[0][1], b"20");
        assert_eq!(rows[1][0], b"1.5");
        // 除零 → NULL（首行 doc1 k/b，b=0）；缺字段行同样 NULL 传播
        let r2 = select_response(&engine, "SELECT k / b FROM documents LIMIT 1");
        assert!(matches!(r2, QueryResponse::Set { rows, .. } if rows.len() == 1 && rows[0][0] == vec![0xfb]));
        // NULL 字段引用 → NULL
        let r3 = select_response(&engine, "SELECT v + 1 FROM documents LIMIT 1");
        assert!(matches!(r3, QueryResponse::Set { rows, .. } if rows.len() == 1 && rows[0][0] == vec![0xfb]));
        // 表达式 + 聚合 / GROUP BY → parser 1064
        assert!(matches!(
            select_response(&engine, "SELECT COUNT(*), k * 2 FROM documents"),
            QueryResponse::Err(_, _)
        ));
    }
