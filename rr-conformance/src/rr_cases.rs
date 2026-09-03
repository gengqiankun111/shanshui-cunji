//! 确定性 RR（REPEATABLE READ）语义场景集（对拍：每步在 MySQL 与自研库各执行一次，
//! 结果互比 + 对照 RR 语义期望）。
//!
//! 会话模型：
//! - main：主事务（BEGIN … COMMIT/ROLLBACK）——测快照稳定/写可见性/当前读；
//! - aux：辅助会话（autocommit）——承担"其它已提交事务"（inter）与提交后 verify 两种角色。
//! 时序在同一线程内确定：main 首读 → aux 提交干扰 → main 一致读再查（快照应稳定）→
//! main 当前读 FOR UPDATE（应见新值）。MySQL 的 RR 行为作为 oracle；自研库每步必须与
//! MySQL 一致才算 conformance。
//!
//! 判定：每步可选期望行集（RR 语义推导）；先校验 MySQL=期望，再校验自研库=MySQL。
//! 行集经 SQL ORDER BY 保证确定性；错误按 mysql 错误文本分类（1062/1205/1213 等）。
//!
//! 说明（2026-09-03 Stage3 调研后收敛）：仅使用主键 id 谓词（SCC 事务内只支持
//! WHERE id= / BETWEEN / IN）+ 点 INSERT/UPDATE/DELETE；键空间 9000xx 高位段避开
//! 随机压测与 seed 数据；每用例结束 ROLLBACK 并把用到的键清理干净（SCC DROP TABLE
//! 不 purge，故用 DELETE 清理）。

use mysql::prelude::*;
use mysql::Conn;

use crate::compare;
use crate::init::DDL_T_TEST;
use crate::tx::conn;

const K: u64 = 900000; // 用例键基址（远离 seed 与新行池）

// ---------------- 单步执行 ----------------

#[derive(Clone, Copy, PartialEq)]
enum Who {
    Main, // 主事务连接
    Aux,  // autocommit 辅助连接（干扰 / 校验 / 清理）
}

struct Step {
    who: Who,
    note: &'static str,
    sql: String,
    /// 期望（Some = 必须等于这些行；Some(vec![]) = 期望空行集；
    /// Some(["ERR <TAG>"]) = 期望报错且错误文本含 TAG；None = 只要求两端一致）
    exp: Option<Vec<String>>,
}

struct Case {
    name: &'static str,
    /// 用例编号（1 起）：决定专属键区 key(no,*) 与清理范围。
    no: usize,
    /// BEGIN 之前的准备步骤（一律 Aux/autocommit）：SCC 快照在 BEGIN 时建立，预插行
    /// 必须先于 BEGIN 提交，否则快照不可见（MySQL 快照在首次一致读才建立——两者在此
    /// 有语义差，用例统一"先提交准备行、再开主事务"，把差异排除在确定性验证之外）。
    pre: Vec<Step>,
    steps: Vec<Step>,
    commit: bool, // 主事务以 COMMIT（true）还是 ROLLBACK（false）结束
    final_sql: Option<(String, Option<Vec<String>>)>, // 提交/回滚后的终态校验（aux）
}

// ---------------- 场景定义 ----------------

fn key(c: usize, i: u64) -> u64 {
    K + c as u64 * 100 + i
}

fn del(id: u64) -> String {
    format!("DELETE FROM t_test WHERE id={id}")
}
fn ins(id: u64, val: i64) -> String {
    format!("INSERT INTO t_test(id,val) VALUES({id},{val})")
}
fn prep_row(id: u64, val: i64) -> Step {
    Step { who: Who::Aux, note: "准备行", sql: ins(id, val), exp: None }
}
fn sel(id: u64) -> String {
    format!("SELECT id,val FROM t_test WHERE id={id}")
}
fn sel_fu(id: u64) -> String {
    format!("SELECT id,val FROM t_test WHERE id={id} FOR UPDATE")
}
fn upd(id: u64) -> String {
    format!("UPDATE t_test SET val=val+1 WHERE id={id}")
}
fn sel_rng(a: u64, b: u64, fu: bool) -> String {
    let fu = if fu { " FOR UPDATE" } else { "" };
    format!("SELECT id,val FROM t_test WHERE id BETWEEN {a} AND {b} ORDER BY id{fu}")
}

/// C1 快照稳定：他事务 UPDATE 提交后，主事务一致读仍见旧值；当前读见新值。
fn case1() -> Case {
    let id = key(1, 0);
    Case {
        name: "C1 snapshot-stable (UPDATE by other committed)",
        no: 1,
        pre: vec![prep_row(id, 0)],
        steps: vec![
            Step { who: Who::Main, note: "首读(一致读)建立快照", sql: sel(id), exp: Some(vec![format!("{id}|0")]) },
            Step { who: Who::Aux, note: "他事务更新并提交", sql: upd(id), exp: None },
            Step { who: Who::Main, note: "一致读→快照仍旧 0", sql: sel(id), exp: Some(vec![format!("{id}|0")]) },
            Step { who: Who::Main, note: "当前读→见新值 1", sql: sel_fu(id), exp: Some(vec![format!("{id}|1")]) },
        ],
        commit: false,
        final_sql: None,
    }
}

/// C2 写可见：主事务内自插/自改后立即可见；读不到他会话未提交（此处经 aux 提交后）。
fn case2() -> Case {
    let a = key(2, 0);
    let b = key(2, 1);
    Case {
        name: "C2 own writes visible in txn",
        no: 2,
        pre: vec![prep_row(a, 0)],
        steps: vec![
            Step { who: Who::Main, note: "事务内自插新行", sql: ins(b, 1), exp: None },
            Step { who: Who::Main, note: "自插立即可见", sql: sel(b), exp: Some(vec![format!("{b}|1")]) },
            Step { who: Who::Main, note: "事务内更新既有行", sql: upd(a), exp: None },
            Step { who: Who::Main, note: "自更立即可见", sql: sel(a), exp: Some(vec![format!("{a}|1")]) },
        ],
        commit: false,
        final_sql: None,
    }
}

/// C3 已删行仍在快照：他事务 DELETE 提交后，主事务一致读仍见旧行；当前读不见。
fn case3() -> Case {
    let id = key(3, 0);
    Case {
        name: "C3 deleted row still visible in snapshot",
        no: 3,
        pre: vec![prep_row(id, 0)],
        steps: vec![
            Step { who: Who::Main, note: "首读(一致读)建立快照", sql: sel(id), exp: Some(vec![format!("{id}|0")]) },
            Step { who: Who::Aux, note: "他事务删除并提交", sql: del(id), exp: None },
            Step { who: Who::Main, note: "一致读→快照仍见旧行", sql: sel(id), exp: Some(vec![format!("{id}|0")]) },
            Step { who: Who::Main, note: "当前读→已删除不见", sql: sel_fu(id), exp: Some(vec![]) },
        ],
        commit: false,
        final_sql: None,
    }
}

/// C4 防幻读（快照侧）：他事务在区间内 INSERT 提交后，主事务范围一致读仍不见；
/// 当前读（FOR UPDATE）可见幻影。
fn case4() -> Case {
    let (a, b, c) = (key(4, 0), key(4, 1), key(4, 2));
    Case {
        name: "C4 phantom insert invisible in snapshot range",
        no: 4,
        pre: vec![prep_row(b, 0)],
        steps: vec![
            Step { who: Who::Main, note: "范围首读(一致读)建立快照", sql: sel_rng(a, c, false), exp: Some(vec![format!("{b}|0")]) },
            Step { who: Who::Aux, note: "他事务区间内插入并提交", sql: ins(a, 1), exp: None },
            Step { who: Who::Main, note: "范围一致读→幻影不可见", sql: sel_rng(a, c, false), exp: Some(vec![format!("{b}|0")]) },
            Step { who: Who::Main, note: "范围当前读→见幻影", sql: sel_rng(a, c, true), exp: Some(vec![format!("{a}|1"), format!("{b}|0")]) },
        ],
        commit: false,
        final_sql: None,
    }
}

/// C5 COMMIT 持久：主事务提交后 aux 会话可见。
fn case5() -> Case {
    let id = key(5, 0);
    Case {
        name: "C5 COMMIT persists (verify via aux)",
        no: 5,
        pre: vec![prep_row(id, 0)],
        steps: vec![
            Step { who: Who::Main, note: "事务内更新", sql: upd(id), exp: None },
            Step { who: Who::Main, note: "事务内自见 1", sql: sel(id), exp: Some(vec![format!("{id}|1")]) },
        ],
        commit: true,
        final_sql: Some((sel(id), Some(vec![format!("{id}|1")]))),
    }
}

/// C6 ROLLBACK 丢弃：主事务回滚后 aux 会话应见原值。
fn case6() -> Case {
    let id = key(6, 0);
    Case {
        name: "C6 ROLLBACK discards writes",
        no: 6,
        pre: vec![prep_row(id, 0)],
        steps: vec![
            Step { who: Who::Main, note: "事务内更新", sql: upd(id), exp: None },
            Step { who: Who::Main, note: "事务内自见 1", sql: sel(id), exp: Some(vec![format!("{id}|1")]) },
        ],
        commit: false,
        final_sql: Some((sel(id), Some(vec![format!("{id}|0")]))),
    }
}

/// C7 缺口 a：主键重复 INSERT → 两侧均报 1062（非事务/autocommit 路径）。
fn case7() -> Case {
    let id = key(7, 0);
    Case {
        name: "C7 duplicate-key INSERT -> 1062 both sides",
        no: 7,
        pre: vec![],
        steps: vec![
            Step { who: Who::Main, note: "事务内插入新行", sql: ins(id, 1), exp: None },
        ],
        commit: true,
        // 已提交后另一会话重复插同主键 → 两侧 DUPLICATE
        final_sql: Some((ins(id, 2), Some(vec!["ERR DUPLICATE".to_string()]))),
    }
}

/// C8 缺口 b：事务内**非主键列**谓词 SELECT（主库候选 ∪ 同事务写集）。
fn case8() -> Case {
    let id = key(8, 0);
    Case {
        name: "C8 txn SELECT with non-PK column predicate",
        no: 8,
        pre: vec![prep_row(id, 7)],
        steps: vec![
            Step { who: Who::Main, note: "事务内按 val 列谓词查", sql: format!("SELECT id,val FROM t_test WHERE val=7"), exp: Some(vec![format!("{id}|7")]) },
            Step { who: Who::Main, note: "事务内按 val 谓词+IN 复查", sql: format!("SELECT id,val FROM t_test WHERE val=7 AND id IN ({id})"), exp: Some(vec![format!("{id}|7")]) },
        ],
        commit: false,
        final_sql: None,
    }
}

/// C9 缺口 d：非事务（autocommit）`UPDATE/DELETE … WHERE id IN (...)`。
fn case9() -> Case {
    let x = key(9, 0);
    let y = key(9, 1);
    Case {
        name: "C9 non-txn UPDATE/DELETE WHERE id IN (...)",
        no: 9,
        pre: vec![prep_row(x, 0), prep_row(y, 0)],
        steps: vec![
            Step { who: Who::Aux, note: "IN 列表批量 UPDATE", sql: format!("UPDATE t_test SET val=val+1 WHERE id IN ({x},{y})"), exp: None },
            Step { who: Who::Aux, note: "验证两行均 +1", sql: format!("SELECT id,val FROM t_test WHERE id IN ({x},{y}) ORDER BY id"), exp: Some(vec![format!("{x}|1"), format!("{y}|1")]) },
            Step { who: Who::Aux, note: "IN 列表批量 DELETE", sql: format!("DELETE FROM t_test WHERE id IN ({y})"), exp: None },
            Step { who: Who::Aux, note: "验证 y 已删 x 仍在", sql: format!("SELECT id,val FROM t_test WHERE id IN ({x},{y}) ORDER BY id"), exp: Some(vec![format!("{x}|1")]) },
        ],
        commit: false,
        final_sql: None,
    }
}

// ---------------- 执行引擎 ----------------

fn one(c: &mut Conn, sql: &str) -> (Vec<String>, String) {
    match c.query_iter(sql) {
        Ok(mut qr) => {
            let _ = qr.affected_rows();
            let mut rows = Vec::new();
            let mut err = String::new();
            for r in qr.by_ref() {
                match r {
                    Ok(row) => {
                        let vals = row.unwrap();
                        rows.push(compare::value_row(&vals).join("|"))
                    }
                    Err(e) => {
                        err = compare::classify_err(&e).1;
                        break;
                    }
                }
            }
            (rows, err)
        }
        Err(e) => (Vec::new(), compare::classify_err(&e).1),
    }
}

/// 双端执行单条 SQL：返回 mysql/mydb 是否一致（行全等；错误按**类别**比对——
/// 两侧 1062 等文案可能不同，只要求类别一致）。
fn both(c_m: &mut Conn, c_d: &mut Conn, sql: &str) -> (bool, Vec<String>, Vec<String>, String, String) {
    let (mr, me) = one(c_m, sql);
    let (dr, de) = one(c_d, sql);
    let cat = |e: &str| -> String { e.split_whitespace().next().unwrap_or("").to_string() };
    let same = (cat(&me) == cat(&de)) && (mr == dr);
    (same, mr, dr, me, de)
}

fn row_s(vec: &[String]) -> String {
    if vec.is_empty() { "∅".to_string() } else { vec.join("; ") }
}

/// 校验单步：返回是否通过。exp 规则见 Step 注释。
fn check_exp(name: &str, m_rows: &[String], m_err: &str, exp: &Option<Vec<String>>) -> bool {
    let Some(want) = exp else { return true };
    if want.len() == 1 && want[0].starts_with("ERR ") {
        let tag = want[0].trim_start_matches("ERR ").to_uppercase();
        return !m_err.is_empty() && (m_err.to_uppercase().contains(&tag) || tag == "OTHER");
    }
    if !m_err.is_empty() {
        println!("  !! {name}: mysql 报错但期望行集: {m_err}");
        return false;
    }
    if m_rows != want {
        println!("  !! {name}: mysql 偏离 RR 期望: got=[{}] want=[{}]", row_s(m_rows), row_s(want));
        return false;
    }
    true
}

pub fn run_all(m_url: &str, m_db: &str, d_url: &str, d_db: &str, out: &str, only: Option<usize>, reinit: bool) -> i32 {
    let cases: Vec<Case> = vec![
        case1(), case2(), case3(), case4(), case5(), case6(), case7(), case8(), case9(),
    ];
    let mut log = String::new();
    let mut fails = 0usize;
    std::fs::create_dir_all(out).ok();

    for (ci, cs) in cases.iter().enumerate() {
        // 单用例过滤（--rr-case <N>，按 cs.no 匹配）
        if only.is_some_and(|n| cs.no != n) {
            continue;
        }
        println!("===== CASE {ci} {} =====", cs.name);
        // 双端 × 双会话连接
        let mut m_main = conn(m_url).expect("连接 mysql(main)");
        let mut d_main = conn(d_url).expect("连接 mydb(main)");
        let mut m_aux = conn(m_url).expect("连接 mysql(aux)");
        let mut d_aux = conn(d_url).expect("连接 mydb(aux)");
        // MySQL 侧选库（SCC 无多库概念，USE/CREATE 忽略错误）
        for c in [&mut m_main, &mut m_aux] {
            let _ = c.query_drop(&format!("CREATE DATABASE IF NOT EXISTS `{m_db}`"));
            let _ = c.query_drop(&format!("USE `{m_db}`"));
            if reinit {
                let _ = c.query_drop(crate::init::DDL_DROP_T_TEST);
            }
            let _ = c.query_drop(DDL_T_TEST);
            let _ = c.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        }
        for c in [&mut d_main, &mut d_aux] {
            let _ = c.query_drop(&format!("CREATE DATABASE IF NOT EXISTS `{d_db}`"));
            let _ = c.query_drop(&format!("USE `{d_db}`"));
            if reinit {
                // mydb：DROP TABLE = 整库 purge（缺口 c 修复后语义）→ 重建空表（CREATE 空操作）
                let _ = c.query_drop(crate::init::DDL_DROP_T_TEST);
            }
            let _ = c.query_drop(DDL_T_TEST);
            let _ = c.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        }
        // 清理本用例键（aux autocommit；键区 = 用例编号专属段）
        let used: Vec<u64> = (0..5).map(|i| key(cs.no, i)).collect();
        for id in &used {
            let _ = one(&mut m_aux, &del(*id));
            let _ = one(&mut d_aux, &del(*id));
        }
        let mut case_ok = true;
        // 准备阶段（BEGIN 之前；aux 会话 autocommit）
        for st in &cs.pre {
            let (same, mr, dr, me, de) = both(&mut m_aux, &mut d_aux, &st.sql);
            let same_s = if same { "ok" } else { "DIFF" };
            let exp_ok = check_exp(&format!("CASE{ci} {}", st.note), &mr, &me, &st.exp);
            if !same || !exp_ok {
                case_ok = false;
            }
            let line = format!(
                "  {same_s} [pre:{note}] {sql}\n      mysql: rows=[{mr}] err={me}\n      mydb:  rows=[{dr}] err={de}\n",
                note = st.note,
                sql = st.sql,
                mr = row_s(&mr),
                dr = row_s(&dr)
            );
            println!("{}", line.trim_end());
            log.push_str(&line);
            if !same {
                log.push_str("      !! DIFF mysql↔mydb (pre)\n");
            }
        }
        // 主事务 BEGIN
        if let Err(e) = m_main.query_drop("BEGIN") {
            println!("  BEGIN mysql 失败: {e}");
        }
        if let Err(e) = d_main.query_drop("BEGIN") {
            println!("  BEGIN mydb 失败: {e}");
        }
        for st in &cs.steps {
            let (c_m, c_d) = match st.who {
                Who::Main => (&mut m_main, &mut d_main),
                Who::Aux => (&mut m_aux, &mut d_aux),
            };
            let (same, mr, dr, me, de) = both(c_m, c_d, &st.sql);
            let same_s = if same { "ok" } else { "DIFF" };
            let exp_ok = check_exp(&format!("CASE{ci} {}", st.note), &mr, &me, &st.exp);
            if !same || !exp_ok {
                case_ok = false;
            }
            let line = format!(
                "  {same_s} [{note}] {sql}\n      mysql: rows=[{mr}] err={me}\n      mydb:  rows=[{dr}] err={de}\n",
                note = st.note,
                sql = st.sql,
                mr = row_s(&mr),
                dr = row_s(&dr)
            );
            println!("{}", line.trim_end());
            log.push_str(&line);
            if !same {
                log.push_str("      !! DIFF mysql↔mydb\n");
            }
        }
        // 结束主事务
        let end_sql = if cs.commit { "COMMIT" } else { "ROLLBACK" };
        let _ = m_main.query_drop(end_sql);
        let _ = d_main.query_drop(end_sql);
        // 终态校验（aux）
        if let Some((sql, exp)) = &cs.final_sql {
            let (same, mr, dr, me, de) = both(&mut m_aux, &mut d_aux, sql);
            let exp_ok = check_exp(&format!("CASE{ci} final"), &mr, &me, exp);
            if !same || !exp_ok {
                case_ok = false;
            }
            let line = format!(
                "  final [{sql}] mysql=[{}] err={me} mydb=[{}] err={de}\n",
                row_s(&mr),
                row_s(&dr)
            );
            println!("{}", line.trim_end());
            log.push_str(&line);
            if !same {
                log.push_str("      !! DIFF mysql↔mydb (final)\n");
            }
        }
        // 清理（恢复现场）
        for id in &used {
            let _ = one(&mut m_aux, &del(*id));
            let _ = one(&mut d_aux, &del(*id));
        }
        if case_ok {
            println!("  => PASS");
        } else {
            fails += 1;
            println!("  => FAIL");
        }
        log.push_str(&format!("===== CASE {ci} {} => {}\n", cs.name, if case_ok { "PASS" } else { "FAIL" }));
    }
    let _ = std::fs::write(format!("{out}/rr-cases.log"), &log);
    println!("rr-cases 汇总: pass={} fail={} 详情见 {out}/rr-cases.log", cases.len() - fails, fails);
    if fails > 0 { 1 } else { 0 }
}
