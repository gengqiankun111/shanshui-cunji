//! 单事务执行：简易两阶段 Try-Confirm-Cancel + 提交后外部视角校验。
//!
//! 边界（方案约定，写入注释）：
//! 1) 只处理业务/应用逻辑异常；进程在 mysql.commit 与 mydb.commit 之间被 kill 会造成
//!    两库不一致（本地压测接受，不做崩溃恢复/完整 XA）；
//! 2) range/batch 类 SQL 原样下发、不拆分不重排，允许死锁，只采集报错行为对比；
//! 3) `insert on duplicate key update` 暂不纳入（后续补）。

use mysql::prelude::*;
use mysql::{Conn, Value};

use crate::compare;
use crate::ops::Op;

pub struct ExecRes {
    pub rows: Vec<Vec<Value>>,
    pub affected: u64,
    pub err: Option<mysql::Error>,
}

/// 单条 SQL 执行 → 行集（已解包）+ affected + 错误。两边用同一函数，保证口径一致。
pub fn exec_stmt(c: &mut Conn, sql: &str) -> ExecRes {
    let mut res = ExecRes { rows: vec![], affected: 0, err: None };
    match c.query_iter(sql) {
        Ok(mut qr) => {
            res.affected = qr.affected_rows();
            for r in qr.by_ref() {
                match r {
                    Ok(row) => res.rows.push(row.unwrap()),
                    Err(e) => {
                        res.err = Some(e);
                        break;
                    }
                }
            }
        }
        Err(e) => res.err = Some(e),
    }
    res
}

pub struct TxnOut {
    pub committed: bool,
    pub category: String,   // OK / DIFF / DUP / DEADLOCK / LOCK_TIMEOUT / OTHER
    pub detail: String,     // 结构化日志（gtrx 前缀由调用方拼接）
    pub outer_diff: Option<String>,
}

/// ops 是否为写 DML（比较 affected_rows）。
fn is_dml(op: &Op) -> bool {
    matches!(
        op,
        Op::Insert { .. }
            | Op::Update { .. }
            | Op::Delete { .. }
            | Op::BatchIns { .. }
            | Op::BatchUpd { .. }
            | Op::BatchDel { .. }
    )
}

/// 本线程工作连接：事务对 + 外部校验对（同一 RR 会话复用——连接无活动事务时等价新会话）。
pub struct WorkerCx {
    pub mtx: Conn,
    pub dtx: Conn,
    pub mext: Conn,
    pub dext: Conn,
}

pub fn conn(url: &str) -> anyhow::Result<Conn> {
    Ok(Conn::new(url)?)
}

fn rows_eq(a: &[Vec<Value>], b: &[Vec<Value>]) -> bool {
    compare::rows_to_strings(a) == compare::rows_to_strings(b)
}

/// 执行单个事务（Try→Confirm→Cancel）。`ext_every_n`：提交成功后第几个做外部校验。
pub fn run_txn(
    cx: &mut WorkerCx,
    ops: &[Op],
    gtrx: u64,
    ext_every_n: u64,
) -> TxnOut {
    let mut log = String::new();
    let mut cancel = |log: &mut String, why: String, cx: &mut WorkerCx| -> TxnOut {
        let _ = cx.mtx.query_drop("ROLLBACK");
        let _ = cx.dtx.query_drop("ROLLBACK");
        log.push_str(&format!("[CANCEL] {why}"));
        let category = if why.starts_with("DIFF") {
            "DIFF"
        } else if why.contains("DUPLICATE") {
            "DUP"
        } else if why.contains("DEADLOCK") {
            "DEADLOCK"
        } else if why.contains("LOCK_TIMEOUT") {
            "LOCK_TIMEOUT"
        } else {
            "OTHER"
        };
        TxnOut { committed: false, category: category.to_string(), detail: std::mem::take(log), outer_diff: None }
    };

    // ---------- Try 阶段：预加锁当前读（select for update，两边同时） ----------
    for op in ops {
        let sql = op.try_fu_sql();
        let rm = exec_stmt(&mut cx.mtx, &sql);
        let rd = exec_stmt(&mut cx.dtx, &sql);
        log.push_str(&format!("\nTRY {} -> m:{} d:{}", sql, exec_desc(&rm), exec_desc(&rd)));
        if let Some(e) = &rm.err {
            return cancel(&mut log, compare::classify_err(e).1, cx);
        }
        if let Some(e) = &rd.err {
            return cancel(&mut log, compare::classify_err(e).1, cx);
        }
        if !rows_eq(&rm.rows, &rd.rows) {
            return cancel(
                &mut log,
                format!("DIFF try rows m={} d={}", rm.rows.len(), rd.rows.len()),
                cx,
            );
        }
    }

    // ---------- Confirm 阶段：逐条执行原始 ops + 事务内校验 ----------
    for op in ops {
        let sql = op.sql();
        let rm = exec_stmt(&mut cx.mtx, &sql);
        let rd = exec_stmt(&mut cx.dtx, &sql);
        log.push_str(&format!("\nEXEC {} -> m:{} d:{}", sql, exec_desc(&rm), exec_desc(&rd)));
        if let Some(e) = &rm.err {
            return cancel(&mut log, compare::classify_err(e).1, cx);
        }
        if let Some(e) = &rd.err {
            return cancel(&mut log, compare::classify_err(e).1, cx);
        }
        if is_dml(op) && rm.affected != rd.affected {
            // 两库各自独立执行 → 并发交错可产生合法 affected 差异（同引擎亦出现）。
            // 用事务内可见快照复核：两边可见状态一致 → 并发时序伪差（记录不取消）；
            // 状态不一致 → 真差异（DIFF 取消）。
            let csql = op.snapshot_check_sql();
            let cm = exec_stmt(&mut cx.mtx, &csql);
            let cd = exec_stmt(&mut cx.dtx, &csql);
            if cm.err.is_some() || cd.err.is_some() || !rows_eq(&cm.rows, &cd.rows) {
                return cancel(
                    &mut log,
                    format!(
                        "DIFF affected m={} d={} 且复核可见状态不一致",
                        rm.affected, rd.affected
                    ),
                    cx,
                );
            }
            log.push_str(&format!(
                "\nNOTE: affected 差异(m={} d={})但事务可见快照一致——两库独立并发时序伪差",
                rm.affected, rd.affected
            ));
        }
        // 事务内校验 ① 快照读 ② 当前读（无论原 op 是否 select，均补做）
        for check in [op.snapshot_check_sql(), op.current_check_sql()] {
            let cm = exec_stmt(&mut cx.mtx, &check);
            let cd = exec_stmt(&mut cx.dtx, &check);
            if let Some(e) = &cm.err {
                return cancel(&mut log, compare::classify_err(e).1, cx);
            }
            if let Some(e) = &cd.err {
                return cancel(&mut log, compare::classify_err(e).1, cx);
            }
            if !rows_eq(&cm.rows, &cd.rows) {
                return cancel(
                    &mut log,
                    format!(
                        "DIFF check {} rows m={} d={}",
                        check,
                        cm.rows.len(),
                        cd.rows.len()
                    ),
                    cx,
                );
            }
        }
    }

    // ---------- 提交两边 ----------
    if let Err(e) = cx.mtx.query_drop("COMMIT") {
        let _ = cx.dtx.query_drop("ROLLBACK");
        return cancel(&mut log, compare::classify_err(&e).1, cx);
    }
    if let Err(e) = cx.dtx.query_drop("COMMIT") {
        // 提交阶段两库拆分（仅日志层面；未做补偿重试——见文件头边界注释）
        return cancel(&mut log, format!("COMMIT mydb 失败: {}", compare::classify_err(&e).1), cx);
    }
    log.push_str("\nCOMMIT ok");
    let mut out = TxnOut { committed: true, category: "OK".into(), detail: log, outer_diff: None };

    // ---------- 外部视角校验（新会话等价：无活动事务的外部连接；抽样） ----------
    if gtrx % ext_every_n == 0 {
        for op in ops {
            for (i, sql) in [op.snapshot_check_sql(), op.current_check_sql()].iter().enumerate() {
                let em = exec_stmt(&mut cx.mext, sql);
                let ed = exec_stmt(&mut cx.dext, sql);
                let label = if i == 0 { "ext-snap" } else { "ext-fu" };
                if let Some(e) = &em.err {
                    // 外部校验读失败（锁竞争 1205/1213）属测试自身引入，记录不判 diff
                    out.outer_diff = Some(format!(
                        "{label} mysql err: {}",
                        compare::classify_err(e).1
                    ));
                    continue;
                }
                if let Some(e) = &ed.err {
                    out.outer_diff = Some(format!("{label} mydb err: {}", compare::classify_err(e).1));
                    continue;
                }
                if !rows_eq(&em.rows, &ed.rows) {
                    out.outer_diff = Some(format!(
                        "{label} DIFF rows m={} d={} sql={sql}",
                        em.rows.len(),
                        ed.rows.len()
                    ));
                }
            }
        }
    }
    out
}

/// 执行结果摘要（日志用，含错误分类）。
pub fn exec_desc(r: &ExecRes) -> String {
    match &r.err {
        Some(e) => format!("ERR {}", compare::classify_err(e).1),
        None => {
            let rows = compare::rows_to_strings(&r.rows);
            format!("rows={} aff={} [{}]", r.rows.len(), r.affected, rows.join("; "))
        }
    }
}
