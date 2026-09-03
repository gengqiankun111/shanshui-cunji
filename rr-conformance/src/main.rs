//! rr-conformance：RR 隔离级别行为对照压测（MySQL ↔ 自研库）。
//!
//! 用法示例：
//!   cargo run --release -- --init --rows 2000 --txns 20000 --threads 8
//!   cargo run --release -- --txns 1000000 --threads 16 --out results
//!
//! 流程：多线程随机生成事务 ops（70% 单点/30% 范围·批量）→ 每事务在两边执行
//! Try(预锁 for update)→Confirm(逐 op+事务内快照/当前读校验)→Cancel(异常双回滚)；
//! 提交后抽样做外部视角校验（新 RR 会话等价连接）；全部跑完做全表终态 dump 逐行比对。

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use mysql::prelude::*;
use mysql::Conn;
use rand::rngs::StdRng;
use rand::SeedableRng;

mod compare;
mod init;
mod ops;
mod tx;

use ops::{Table, Op};
use tx::{conn, exec_stmt, run_txn, WorkerCx};

#[derive(Clone)]
struct Cfg {
    mysql_url: String,
    my_url: String,
    mysql_db: String,
    my_db: String,
    txns: u64,
    threads: usize,
    rows: u32,
    seed: u64,
    out: String,
    ext_every: u64,
    p_dup: f64,
    init: bool,
}

#[derive(Default)]
struct Counts {
    txn: u64,
    ok: u64,
    rollback: HashMap<String, u64>,
    outer_diff: u64,
    conn_err: u64,
}

fn arg(args: &[String], key: &str, dft: &str) -> String {
    args.iter()
        .position(|a| a == key)
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| dft.to_string())
}
fn has(args: &[String], key: &str) -> bool {
    args.iter().any(|a| a == key)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cfg = Cfg {
        mysql_url: arg(&args, "--mysql-url", "mysql://root:123456@127.0.0.1:3306"),
        my_url: arg(&args, "--my-url", "mysql://root@127.0.0.1:3308"),
        mysql_db: arg(&args, "--mysql-db", "mysql"),
        my_db: arg(&args, "--my-db", "scc"),
        txns: arg(&args, "--txns", "20000").parse().unwrap_or(20_000),
        threads: arg(&args, "--threads", "8").parse().unwrap_or(8),
        rows: arg(&args, "--rows", "2000").parse().unwrap_or(2000),
        seed: arg(&args, "--seed", "1").parse().unwrap_or(1),
        out: arg(&args, "--out", "results"),
        ext_every: arg(&args, "--ext-every", "64").parse().unwrap_or(64),
        p_dup: arg(&args, "--p-dup", "0.1").parse().unwrap_or(0.1),
        init: has(&args, "--init"),
    };

    std::fs::create_dir_all(&cfg.out).expect("创建输出目录");
    if cfg.init {
        match init_dbs(&cfg) {
            Ok(_) => println!("[init] 双库建表+种子完成 rows={}", cfg.rows),
            Err(e) => {
                println!("[init] 失败: {e:#}（若为权限/连接问题请检查 --mysql-url/--my-url）");
                std::process::exit(2);
            }
        }
    }

    // RR 会话设定（MySQL 必需；自研库若不支持该语句则忽略——由库自身隔离默认保证）
    let t0 = Instant::now();
    let gtrx = Arc::new(AtomicU64::new(0));
    let counts = Arc::new(Mutex::new(Counts::default()));
    let diff_log = Arc::new(Mutex::new(BufWriter::new(
        File::create(format!("{}/diff.log", cfg.out)).expect("创建 diff.log"),
    )));
    let detail_dir = format!("{}/txn", cfg.out);
    std::fs::create_dir_all(&detail_dir).ok();

    let mut handles = Vec::new();
    for w in 0..cfg.threads {
        let cfg = cfg.clone();
        let gtrx = gtrx.clone();
        let counts = counts.clone();
        let diff_log = diff_log.clone();
        let detail_dir = detail_dir.clone();
        handles.push(std::thread::spawn(move || {
            worker(w, &cfg, gtrx, counts, diff_log, &detail_dir);
        }));
    }
    for h in handles {
        let _ = h.join();
    }

    // ---------- 终态全表 dump 逐行比对 ----------
    let final_ok = dump_compare(&cfg);
    let el = t0.elapsed();

    let c = counts.lock().unwrap();
    let mut summary = String::new();
    summary.push_str(&format!("== rr-conformance summary ==\n"));
    summary.push_str(&format!("txns={} threads={} elapsed={:.1}s\n", cfg.txns, cfg.threads, el.as_secs_f64()));
    summary.push_str(&format!("committed={} outer_diff={}\n", c.ok, c.outer_diff));
    let mut rb: Vec<(&String, &u64)> = c.rollback.iter().collect();
    rb.sort_by(|a, b| b.1.cmp(a.1));
    for (k, v) in rb {
        summary.push_str(&format!("rollback[{k}]={v}\n"));
    }
    summary.push_str(&format!("conn_err={}\n", c.conn_err));
    summary.push_str(&format!("final_dump_equal={final_ok}\n"));
    println!("{summary}");
    let _ = std::fs::write(format!("{}/summary.txt", cfg.out), summary);
    if !final_ok {
        println!("[FAIL] 终态不一致，详见 {}/dump.diff", cfg.out);
    }
}

fn init_dbs(cfg: &Cfg) -> anyhow::Result<()> {
    let mut m = conn(&cfg.mysql_url)?;
    let mut d = conn(&cfg.my_url)?;
    ensure_db(&mut m, &cfg.mysql_db);
    ensure_db(&mut d, &cfg.my_db);
    init::init(&mut m, &mut d, cfg.rows)
}

fn setup_rr(c: &mut Conn) {
    // 自研库暂不支持该语句时忽略错误（隔离语义由库保证），MySQL 端强制 RR。
    let _ = c.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
}

/// 确保库存在并选中（MySQL 语义）。SCC 无多库/部分语句不支持 → 错误一律忽略，
/// 其数据本就落在单一默认库中。
fn ensure_db(c: &mut Conn, db: &str) {
    let _ = c.query_drop(&format!("CREATE DATABASE IF NOT EXISTS `{db}`"));
    let _ = c.query_drop(&format!("USE `{db}`"));
}

fn worker(
    w: usize,
    cfg: &Cfg,
    gtrx: Arc<AtomicU64>,
    counts: Arc<Mutex<Counts>>,
    diff_log: Arc<Mutex<BufWriter<File>>>,
    detail_dir: &str,
) {
    let _ = detail_dir;
    let (mtx, dtx, mext, dext) = match (
        conn(&cfg.mysql_url),
        conn(&cfg.my_url),
        conn(&cfg.mysql_url),
        conn(&cfg.my_url),
    ) {
        (Ok(a), Ok(b), Ok(c), Ok(d)) => (a, b, c, d),
        (e, _, _, _) => {
            counts.lock().unwrap().conn_err += 1;
            if let Err(err) = e {
                println!("[worker {w}] 连接失败: {err}");
            }
            return;
        }
    };
    let mut wc = WorkerCx { mtx, dtx, mext, dext };
    ensure_db(&mut wc.mtx, &cfg.mysql_db);
    ensure_db(&mut wc.dtx, &cfg.my_db);
    ensure_db(&mut wc.mext, &cfg.mysql_db);
    ensure_db(&mut wc.dext, &cfg.my_db);
    for c in [&mut wc.mtx, &mut wc.dtx, &mut wc.mext, &mut wc.dext] {
        setup_rr(c);
    }
    let mut rng = StdRng::seed_from_u64(cfg.seed.wrapping_add(w as u64));
    loop {
        let id = gtrx.fetch_add(1, Ordering::Relaxed);
        if id >= cfg.txns {
            break;
        }
        let ops = ops::gen_txn(&mut rng, cfg.rows, cfg.p_dup);
        let out = run_txn(&mut wc, &ops, id, cfg.ext_every);
        let mut cnt = counts.lock().unwrap();
        cnt.txn += 1;
        if out.committed {
            cnt.ok += 1;
            if out.outer_diff.is_some() {
                cnt.outer_diff += 1;
            }
        } else {
            *cnt.rollback.entry(out.category.clone()).or_default() += 1;
        }
        // 异常 / 差异 / 外部视角不一致 → 落详情日志（gtrx、ops、每步返回）
        if !out.committed || out.outer_diff.is_some() {
            let sqls: Vec<String> = ops.iter().map(|o| o.sql()).collect();
            let mut txt = format!(
                "gtrx={id} committed={} category={} ops={}\n{}\n",
                out.committed,
                out.category,
                sqls.join(" || "),
                out.detail
            );
            if let Some(od) = &out.outer_diff {
                txt.push_str(&format!("OUTER: {od}\n"));
            }
            let mut dl = diff_log.lock().unwrap();
            let _ = writeln!(dl, "{txt}");
            let _ = dl.flush();
        }
        drop(cnt);
    }
}

/// 终态：新开连接全表 dump，逐行比对两库（MySQL 顺序与自研库一致则全等）。
fn dump_compare(cfg: &Cfg) -> bool {
    let mut m = match conn(&cfg.mysql_url) {
        Ok(c) => c,
        Err(e) => {
            println!("dump 连接失败 mysql: {e}");
            return false;
        }
    };
    ensure_db(&mut m, &cfg.mysql_db);
    let mut d = match conn(&cfg.my_url) {
        Ok(c) => c,
        Err(e) => {
            println!("dump 连接失败 mydb: {e}");
            return false;
        }
    };
    ensure_db(&mut d, &cfg.my_db);
    let mut all_ok = true;
    for (tbl, sql) in [
        ("t_test", "SELECT id,val FROM t_test ORDER BY id"),
        ("t_combo", "SELECT id,a,b,val FROM t_combo ORDER BY id"),
    ] {
        let rm = exec_stmt(&mut m, sql);
        let rd = exec_stmt(&mut d, sql);
        if rm.err.is_some() || rd.err.is_some() {
            println!("dump {tbl} 执行失败 m={:?} d={:?}", rm.err, rd.err);
            all_ok = false;
            continue;
        }
        let sm = compare::rows_to_strings(&rm.rows);
        let sd = compare::rows_to_strings(&rd.rows);
        if sm == sd {
            println!("dump {tbl}: 一致 rows={}", sm.len());
        } else {
            all_ok = false;
            let mut f = String::new();
            f.push_str(&format!("table {tbl} 不一致 m={} d={}\n", sm.len(), sd.len()));
            for (i, (a, b)) in sm.iter().zip(sd.iter()).enumerate() {
                if a != b {
                    f.push_str(&format!("row{i} m=[{a}] d=[{b}]\n"));
                    if i > 9 {
                        break;
                    }
                }
            }
            let _ = std::fs::write(format!("{}/dump.diff", cfg.out), f);
            println!("dump {tbl}: 不一致 -> {}/dump.diff", cfg.out);
        }
    }
    all_ok
}
