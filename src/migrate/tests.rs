//! 迁移工具测试：SQL 行解析、CSV / mysqldump / JSONL 导入、增量 checkpoint 语义。

use super::*;
use crate::engine::Engine;

#[test]
fn parse_sql_line_with_columns_and_values() {
    let line =
        "INSERT INTO `t` (`name`,`age`,`note`) VALUES ('alice',30,NULL),('bob',25,'a\\'b');";
    let (cols, tuples) = parse_mysql_insert_line(line).unwrap();
    assert_eq!(cols, vec!["name", "age", "note"]);
    assert_eq!(tuples.len(), 2);
    assert_eq!(tuples[0][0], SqlValue::Str("alice".into()));
    assert_eq!(tuples[0][1], SqlValue::Num("30".into()));
    assert_eq!(tuples[0][2], SqlValue::Null);
    assert_eq!(tuples[1][2], SqlValue::Str("a'b".into()), "转义引号");
}

#[test]
fn parse_sql_line_without_columns() {
    let line = "INSERT INTO `t` VALUES (1,'x'),(2,'y');";
    let (cols, tuples) = parse_mysql_insert_line(line).unwrap();
    assert!(cols.is_empty());
    assert_eq!(tuples.len(), 2);
    assert_eq!(tuples[0][0], SqlValue::Num("1".into()));
    assert_eq!(tuples[1][1], SqlValue::Str("y".into()));
}

#[test]
fn parse_non_insert_line_returns_none() {
    assert!(parse_mysql_insert_line("CREATE TABLE t (id int);").is_none());
    assert!(parse_mysql_insert_line("-- comment").is_none());
}

#[test]
fn csv_import_creates_documents() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("in.csv");
    std::fs::write(
        &csv_path,
        "docid,status,type\n1,active,order\n2,active,view\n3,pending,order\n",
    )
    .unwrap();
    let cfg = crate::config::Config::default();
    let data_dir = dir.path().join("data");
    let mut engine = Engine::open(&data_dir, &cfg).unwrap();
    let rep = import_csv(&mut engine, &csv_path).unwrap();
    assert_eq!(rep.rows, 3);
    assert_eq!(rep.failed, 0);
    // 查询验证
    assert_eq!(
        engine.search_term("status=active").unwrap().len(),
        2,
        "status=active 应命中 2 条"
    );
    assert_eq!(
        engine.search_term("type=order").unwrap().len(),
        2,
        "type=order 应命中 2 条"
    );
}

#[test]
fn sql_import_creates_documents() {
    let dir = tempfile::tempdir().unwrap();
    let sql_path = dir.path().join("dump.sql");
    std::fs::write(
        &sql_path,
        "INSERT INTO `users` (`id`,`status`,`city`) VALUES (10,'active','bj'),(20,'pending','sh');\n",
    )
    .unwrap();
    let cfg = crate::config::Config::default();
    let data_dir = dir.path().join("data");
    let mut engine = Engine::open(&data_dir, &cfg).unwrap();
    let rep = import_mysqldump(&mut engine, &sql_path).unwrap();
    assert_eq!(rep.rows, 2);
    assert_eq!(rep.failed, 0);
    assert_eq!(
        engine.search_term("status=active").unwrap().len(),
        1,
        "docid 用 id 列"
    );
    let val = engine.get(10).unwrap().expect("docid=10 存在");
    assert!(String::from_utf8_lossy(&val).contains("bj"));
}

#[test]
fn sql_import_without_docid_column_auto_assigns() {
    let dir = tempfile::tempdir().unwrap();
    let sql_path = dir.path().join("dump.sql");
    std::fs::write(
        &sql_path,
        "INSERT INTO `t` (`name`) VALUES ('a'),('b'),('c');\n",
    )
    .unwrap();
    let cfg = crate::config::Config::default();
    let data_dir = dir.path().join("data");
    let mut engine = Engine::open(&data_dir, &cfg).unwrap();
    let rep = import_mysqldump(&mut engine, &sql_path).unwrap();
    assert_eq!(rep.rows, 3);
    assert_eq!(engine.search_term("name=a").unwrap().len(), 1);
    assert_eq!(engine.search_term("name=c").unwrap().len(), 1);
}

#[test]
fn json_import_creates_documents() {
    let dir = tempfile::tempdir().unwrap();
    let json_path = dir.path().join("in.jsonl");
    std::fs::write(
        &json_path,
        "{\"docid\":1,\"status\":\"active\"}\n{\"status\":\"pending\"}\n{\"docid\":3,\"city\":\"bj\"}\n",
    )
    .unwrap();
    let cfg = crate::config::Config::default();
    let data_dir = dir.path().join("data");
    let mut engine = Engine::open(&data_dir, &cfg).unwrap();
    let rep = import_json(&mut engine, &json_path).unwrap();
    assert_eq!(rep.rows, 3);
    assert_eq!(rep.failed, 0);
    // 第 2 行无 docid → 自动分配（递增）
    assert_eq!(engine.search_term("status=active").unwrap().len(), 1);
    assert_eq!(engine.search_term("status=pending").unwrap().len(), 1);
    assert_eq!(engine.search_term("city=bj").unwrap().len(), 1);
    // 自动分配的 docid 落在 1/3 之外（=2）
    let val = engine.get(2).unwrap().expect("自动分配 docid=2");
    assert!(String::from_utf8_lossy(&val).contains("pending"));
}

// ---- 增量导入（design 5.16 阶段 3：docid 游标断点续传）----

#[test]
fn incremental_json_import_resumes_from_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let json_path = dir.path().join("in.jsonl");
    let cp_path = dir.path().join("checkpoint.cp");
    // 首轮：3 条
    std::fs::write(
        &json_path,
        "{\"docid\":1,\"s\":\"a\"}\n{\"docid\":2,\"s\":\"a\"}\n{\"docid\":3,\"s\":\"b\"}\n",
    )
    .unwrap();
    let cfg = crate::config::Config::default();
    let data_dir = dir.path().join("data");
    let mut engine = Engine::open(&data_dir, &cfg).unwrap();
    let rep1 = import_json_incremental(&mut engine, &json_path, None, &cp_path).unwrap();
    assert_eq!(rep1.rows, 3);
    assert_eq!(rep1.skipped, 0);
    assert_eq!(load_checkpoint(&cp_path).unwrap(), 3, "checkpoint 推进到 3");

    // 追加 2 条新数据（docid 4、5）
    std::fs::write(
        &json_path,
        "{\"docid\":1,\"s\":\"a\"}\n{\"docid\":2,\"s\":\"a\"}\n{\"docid\":3,\"s\":\"b\"}\n{\"docid\":4,\"s\":\"c\"}\n{\"docid\":5,\"s\":\"c\"}\n",
    )
    .unwrap();
    // 续跑：只处理新行，旧行跳过
    let rep2 = import_json_incremental(&mut engine, &json_path, None, &cp_path).unwrap();
    assert_eq!(rep2.rows, 2, "只导入新增 2 条");
    assert_eq!(rep2.skipped, 3, "旧 3 条跳过");
    assert_eq!(load_checkpoint(&cp_path).unwrap(), 5);
    // 数据正确
    assert_eq!(
        engine.get(4).unwrap().unwrap(),
        b"{\"docid\":4,\"s\":\"c\"}"
    );
    assert_eq!(engine.search_term("s=c").unwrap().len(), 2);
    // 再次续跑：全部跳过
    let rep3 = import_json_incremental(&mut engine, &json_path, None, &cp_path).unwrap();
    assert_eq!(rep3.rows, 0);
    assert_eq!(rep3.skipped, 5);
}

#[test]
fn incremental_csv_requires_docid_column() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("in.csv");
    std::fs::write(&csv_path, "status,type\nactive,order\n").unwrap();
    let cfg = crate::config::Config::default();
    let data_dir = dir.path().join("data");
    let mut engine = Engine::open(&data_dir, &cfg).unwrap();
    let cp = dir.path().join("cp");
    let err = import_csv_incremental(&mut engine, &csv_path, None, &cp).unwrap_err();
    assert!(
        err.to_string().contains("docid"),
        "无 docid 列增量导入应报错: {err}"
    );
}

#[test]
fn checkpoint_atomic_persist() {
    let dir = tempfile::tempdir().unwrap();
    let cp = dir.path().join("cp");
    save_checkpoint(&cp, 42).unwrap();
    assert_eq!(load_checkpoint(&cp).unwrap(), 42);
    // 覆盖更新
    save_checkpoint(&cp, 100).unwrap();
    assert_eq!(load_checkpoint(&cp).unwrap(), 100);
    // 缺失 → 0
    assert_eq!(load_checkpoint(&dir.path().join("nope")).unwrap(), 0);
}

#[test]
fn incremental_export_cursor_progresses() {
    // 复现 export.rs 增量语义（design 20.5）：docid 游标断点续传——
    // 只导 docid > checkpoint 的新数据，max_docid 单调推进，无新数据不写 cp
    let dir = tempfile::tempdir().unwrap();
    let cp = dir.path().join("cp");
    let cfg = crate::config::Config::default();
    let data_dir = dir.path().join("data");
    let mut engine = Engine::open(&data_dir, &cfg).unwrap();

    // 首次全量：base=0 → scan 全部，游标推进到最大 docid
    for d in 1..=3u64 {
        engine.put(d, format!("{{\"d\":{d}}}").into_bytes(), &[]).unwrap();
    }
    let base0 = load_checkpoint(&cp).unwrap(); // 缺失 → 0
    let all = engine.scan_range(Some(base0 + 1), None).unwrap();
    let max0 = all.iter().map(|r| r.0).max().unwrap_or(base0);
    assert_eq!(all.len(), 3, "首轮全量 3 行");
    assert_eq!(max0, 3);
    save_checkpoint(&cp, max0).unwrap();

    // 追加 2 条 → 二次增量只导 docid 4、5，游标推进到 5
    for d in 4..=5u64 {
        engine.put(d, format!("{{\"d\":{d}}}").into_bytes(), &[]).unwrap();
    }
    let base1 = load_checkpoint(&cp).unwrap();
    let inc = engine.scan_range(Some(base1 + 1), None).unwrap();
    let max1 = inc.iter().map(|r| r.0).max().unwrap_or(base1);
    assert_eq!(inc.len(), 2, "二次增量只导新增 2 行");
    assert_eq!(inc.iter().map(|r| r.0).collect::<Vec<_>>(), vec![4, 5]);
    assert_eq!(max1, 5);
    save_checkpoint(&cp, max1).unwrap();

    // 无新数据 → max_docid == base，不写 cp（游标不推进）
    let base2 = load_checkpoint(&cp).unwrap();
    let none = engine.scan_range(Some(base2 + 1), None).unwrap();
    let max2 = none.iter().map(|r| r.0).max().unwrap_or(base2);
    assert!(none.is_empty(), "无新数据");
    assert_eq!(max2, base2, "max_docid 不越过 base");
    assert_eq!(load_checkpoint(&cp).unwrap(), 5, "cp 保持 5 不变");
}
