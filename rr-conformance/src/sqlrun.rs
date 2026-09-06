//! sqlrun：对单端（MySQL 或 SCC）执行宽表典型 SQL 性能探针，输出分位数。
//!
//! 用法：rr-conformance --sql-run --table t --url mysql://root@127.0.0.1:3316/wide --out results-sqlrun-mysql-2g
//!       rr-conformance --sql-run --table documents --url mysql://root@127.0.0.1:3317 --out results-sqlrun-scc-2g
//! 说明：--table 默认 t；SCC 多表内核下非默认表 docid 高位编码会撑爆 32 位位图
//!       （inverted.rs 断言），宽表装载/探针需以默认表 documents 为目标。
//! 探针集两侧完全一致（37 项）；写操作只作用 id > N 的预留区（N 动态 = COUNT）：
//!   upd 区 base+1..+200（保留）   del 区 base+501..+600（单删）
//!   delb 区 base+601..+1100（批量删 50×10）   ins 区 base+1501..+33000（单/批/万行插）
//! 输出：stdout 逐项 + <out>/summary.md。

use std::sync::OnceLock;
use std::time::Instant;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::tx::exec_stmt;

/// 目标表名（--table，默认 t；SCC 用 documents）。
static TAB: OnceLock<String> = OnceLock::new();

/// 返回当前目标表名。
fn t() -> &'static str {
    TAB.get().map(|s| s.as_str()).unwrap_or("t")
}

#[derive(Clone, Copy)]
enum Kind {
    /// 结果集（按行数统计）
    Rows,
    /// 写（按 affected_rows 统计）
    Exec,
    /// 事务块：BEGIN → 语句 → COMMIT（整块计时）
    Block,
}

struct Probe {
    cat: &'static str,
    name: &'static str,
    kind: Kind,
    n: usize,
    sql: fn(rng: &mut StdRng, ctx: &Ctx, i: usize) -> String,
    note: &'static str,
}

pub struct Ctx {
    pub n: u64,     // 表总行数（启动 COUNT）
    pub base: u64,  // 预留区起点 = n + 200_000
    pub upd_lo: u64,   // upd 区（持久 200 行）
    pub upd_hi: u64,
    pub del_lo: u64,   // 单删区 100 行
    pub delb_lo: u64,  // 批量删区 500 行（delete_range50 用）
    pub delc_lo: u64,  // 大批量删区 1000 行（delete_range_1000 用）
    pub ins_lo: u64,   // 批量插区起点
}

const COLS_FULL: &str = "id,k,amount,score,ts,status,region,channel,user_id,age,active_days,\
     visit_count,balance,flag,tag,note,title,url,email,phone,ip,desc_a,desc_b,txt_a,txt_b";

fn ins_vals(id: u64, tag: &str) -> String {
    format!(
        "({id},1,1.00,0.5,1700000000,'active','beijing','web',1,20,1,1,1.00,0,'{tag}',\
         'n','t','u','e','p','i','a','b','x','y')"
    )
}

/// 构造多行 VALUES 批量插入语句（连续 size 行，从 lo 起）。
fn ins_multi(lo: u64, size: u64, tag: &str) -> String {
    let tb = t();
    let mut vals = Vec::with_capacity(size as usize);
    for id in lo..lo + size {
        vals.push(ins_vals(id, tag));
    }
    format!("INSERT INTO {tb} ({COLS_FULL}) VALUES {}", vals.join(","))
}

/// id IN (size 个不重复随机主键) 的批量查询。
fn in_sql(rng: &mut StdRng, c: &Ctx, size: usize) -> String {
    let tb = t();
    let mut ids: Vec<u64> = Vec::with_capacity(size);
    while ids.len() < size {
        let v = rng.gen_range(1..=c.n);
        if !ids.contains(&v) {
            ids.push(v);
        }
    }
    ids.sort_unstable();
    format!("SELECT id,k,status FROM {tb} WHERE id IN ({})", ids.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","))
}

/// P141 A1-1：txn_agg 探针 body 占位（脚本由 run_txn_agg 按 name 分发，body 无实义）。
fn txn_agg_mk(_r: &mut StdRng, _c: &Ctx, _i: usize) -> String {
    String::new()
}

pub fn run(url: &str, out: &str, table: &str, only: &str) -> i32 {
    let _ = TAB.set(table.to_string());
    // --only 过滤：空 = 全量；支持逗号分隔（如 "txn_lock_wait,txn_lock_mid_contend"）
    let want: Vec<&str> = only.split(',').filter(|s| !s.is_empty()).collect();
    let filtered = !want.is_empty();
    let mut conn = match mysql::Conn::new(url) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[sqlrun] 连接失败 {url}: {e}");
            return 2;
        }
    };
    let tb = t();
    let cnt = exec_stmt(&mut conn, &format!("SELECT COUNT(*) FROM {tb}"));
    let n: u64 = if cnt.rows.is_empty() || cnt.err.is_some() {
        0
    } else {
        mysql::from_value(cnt.rows[0][0].clone())
    };
    if n == 0 {
        eprintln!("[sqlrun] COUNT(*) 为 0（表空或宽表不存在？表={tb}）");
        return 2;
    }
    let ctx = Ctx {
        n,
        base: n + 200_000,
        upd_lo: n + 200_001,
        upd_hi: n + 200_200,
        del_lo: n + 200_501,
        delb_lo: n + 200_601,
        delc_lo: n + 246_001, // 大批量删预留（1000 行，base+46001..，与插区分离）
        ins_lo: n + 201_501,
    };
    println!("[sqlrun] url={url} 表={tb} N={n} base={} out={out}", ctx.base);

    // 预留行准备：upd 200（保留不删）、del 100（单删）、delb 500（批量删）
    let mut vals = Vec::new();
    for id in ctx.upd_lo..=ctx.upd_hi {
        vals.push(ins_vals(id, &format!("u{id}")));
    }
    let r = exec_stmt(&mut conn, &format!("INSERT INTO {tb} ({COLS_FULL}) VALUES {}", vals.join(",")));
    let upd_ok = r.err.is_none();
    vals.clear();
    for id in ctx.del_lo..=ctx.del_lo + 99 {
        vals.push(ins_vals(id, &format!("d{id}")));
    }
    let r = exec_stmt(&mut conn, &format!("INSERT INTO {tb} ({COLS_FULL}) VALUES {}", vals.join(",")));
    let del_ok = r.err.is_none();
    vals.clear();
    for id in ctx.delb_lo..=ctx.delb_lo + 499 {
        vals.push(ins_vals(id, &format!("q{id}")));
    }
    let r = exec_stmt(&mut conn, &format!("INSERT INTO {tb} ({COLS_FULL}) VALUES {}", vals.join(",")));
    let delb_ok = r.err.is_none();
    vals.clear();
    for id in ctx.delc_lo..=ctx.delc_lo + 999 {
        vals.push(ins_vals(id, &format!("r{id}")));
    }
    let r = exec_stmt(&mut conn, &format!("INSERT INTO {tb} ({COLS_FULL}) VALUES {}", vals.join(",")));
    let delc_ok = r.err.is_none();
    println!("[sqlrun] 预留行：upd {}-{}、del {}-{}、delb {}-{}、delc {}-{}（{}{}{}{}）",
             ctx.upd_lo, ctx.upd_hi, ctx.del_lo, ctx.del_lo + 99,
             ctx.delb_lo, ctx.delb_lo + 499, ctx.delc_lo, ctx.delc_lo + 999,
             if upd_ok { "OK " } else { "UPD-FAIL " },
             if del_ok { "OK " } else { "DEL-FAIL " },
             if delb_ok { "OK " } else { "DELB-FAIL " },
             if delc_ok { "OK" } else { "DELC-FAIL" });

    // ---------- 各探针 SQL 生成（两侧方言统一，不使用列别名） ----------
    let sql_pk = |_r: &mut StdRng, c: &Ctx, _i: usize| format!("SELECT * FROM {tb} WHERE id={}", _r.gen_range(1..=c.n), tb = t());
    let sql_proj = |_r: &mut StdRng, c: &Ctx, _i: usize| {
        format!("SELECT id,k,amount,score,ts,status,region,channel,user_id,age FROM {tb} WHERE id={}", _r.gen_range(1..=c.n), tb = t())
    };
    let sql_in5 = |r: &mut StdRng, c: &Ctx, _i: usize| in_sql(r, c, 5);
    let sql_in50 = |r: &mut StdRng, c: &Ctx, _i: usize| in_sql(r, c, 50);
    let sql_range = |_r: &mut StdRng, c: &Ctx, _i: usize| {
        let a = _r.gen_range(1..c.n.saturating_sub(2000));
        format!("SELECT id,k,status FROM {tb} WHERE id BETWEEN {a} AND {}", a + 100, tb = t())
    };
    let sql_enum = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        let s = ["active", "closed", "pending", "failed", "archived"][_r.gen_range(0..5)];
        format!("SELECT id,status FROM {tb} WHERE status='{s}' LIMIT 100", tb = t())
    };
    let sql_count = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        let s = ["active", "closed", "pending", "failed", "archived"][_r.gen_range(0..5)];
        format!("SELECT COUNT(*) FROM {tb} WHERE status='{s}'", tb = t())
    };
    let sql_combo = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        let s = ["active", "closed"][_r.gen_range(0..2)];
        let g = ["beijing", "shanghai", "shenzhen", "guangzhou"][_r.gen_range(0..4)];
        format!("SELECT id FROM {tb} WHERE status='{s}' AND region='{g}' LIMIT 100", tb = t())
    };
    let sql_fieldin = |_r: &mut StdRng, _c: &Ctx, _i: usize| format!("SELECT id FROM {tb} WHERE status IN ('active','closed') LIMIT 100", tb = t());
    let sql_cmpgt = |_r: &mut StdRng, _c: &Ctx, _i: usize| format!("SELECT id,amount FROM {tb} WHERE amount > 900000 LIMIT 50", tb = t());
    let sql_cmpbetween = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        let a = 400_000 + _r.gen_range(0..100);
        format!("SELECT id,amount FROM {tb} WHERE amount BETWEEN {a} AND {}", a + 50, tb = t())
    };
    let sql_cntall = |_r: &mut StdRng, _c: &Ctx, _i: usize| format!("SELECT COUNT(*) FROM {tb}", tb = t());
    let sql_sumwhere = |_r: &mut StdRng, _c: &Ctx, _i: usize| format!("SELECT SUM(amount) FROM {tb} WHERE status='active'", tb = t());
    let sql_gb = |_r: &mut StdRng, _c: &Ctx, _i: usize| format!("SELECT status, COUNT(*) FROM {tb} GROUP BY status", tb = t());
    let sql_gbsum = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT status, COUNT(*), SUM(amount) FROM {tb} GROUP BY status HAVING COUNT(*) > 0", tb = t())
    };
    let sql_orderwin = |_r: &mut StdRng, c: &Ctx, _i: usize| {
        let a = _r.gen_range(1..c.n.saturating_sub(2000));
        format!("SELECT id,amount FROM {tb} WHERE id BETWEEN {a} AND {} ORDER BY amount DESC LIMIT 20", a + 1000, tb = t())
    };
    // 写区探针
    let sql_upd = |_r: &mut StdRng, c: &Ctx, _i: usize| {
        let id = c.upd_lo + (_r.gen_range(0..200) as u64);
        format!("UPDATE {tb} SET note='x9' WHERE id={id}", tb = t())
    };
    let sql_upd_in2 = |_r: &mut StdRng, c: &Ctx, _i: usize| {
        let a = c.upd_lo + (_r.gen_range(0..180) as u64);
        format!("UPDATE {tb} SET note='x9' WHERE id IN ({a},{})", a + 10, tb = t())
    };
    let sql_upd_in50 = |_r: &mut StdRng, c: &Ctx, _i: usize| {
        let mut ids: Vec<u64> = Vec::with_capacity(50);
        while ids.len() < 50 {
            let v = c.upd_lo + (_r.gen_range(0..200) as u64);
            if !ids.contains(&v) {
                ids.push(v);
            }
        }
        format!("UPDATE {tb} SET note='x9' WHERE id IN ({})", ids.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","), tb = t())
    };
    let sql_ins_s = |_r: &mut StdRng, c: &Ctx, i: usize| {
        let id = c.base + 2801 + i as u64; // ins 区尾部单插窗
        format!("INSERT INTO {tb} ({COLS_FULL}) VALUES {}", ins_vals(id, &format!("w{id}")), tb = t())
    };
    let sql_insb10 = |_r: &mut StdRng, c: &Ctx, i: usize| ins_multi(c.ins_lo + (i as u64) * 10, 10, "b10");
    let sql_insb100 = |_r: &mut StdRng, c: &Ctx, i: usize| ins_multi(c.ins_lo + 300 + (i as u64) * 100, 100, "b100");
    let sql_del = |_r: &mut StdRng, c: &Ctx, _i: usize| {
        let id = c.del_lo + (_r.gen_range(0..100) as u64);
        format!("DELETE FROM {tb} WHERE id={id}", tb = t())
    };
    let sql_del_range50 = |_r: &mut StdRng, c: &Ctx, i: usize| {
        let lo = c.delb_lo + (i as u64) * 50;
        format!("DELETE FROM {tb} WHERE id BETWEEN {lo} AND {}", lo + 49, tb = t())
    };
    let txn_upd = |_r: &mut StdRng, c: &Ctx, _i: usize| {
        let id = c.upd_lo + (_r.gen_range(0..200) as u64);
        format!("UPDATE {tb} SET score=0.5 WHERE id={id}", tb = t())
    };
    let sql_fu = |_r: &mut StdRng, c: &Ctx, _i: usize| {
        let id = c.upd_lo + (_r.gen_range(0..200) as u64);
        format!("SELECT k,amount FROM {tb} WHERE id={id} FOR UPDATE", tb = t())
    };
    // ---------- 新增 11 项探针（2026-09-04 第二批） ----------
    // 大批量插入：单语句 1 万行（预留大区 base+3001..，每次 i 偏移 1 万）
    let sql_insb10000 = |_r: &mut StdRng, c: &Ctx, i: usize| {
        ins_multi(c.base + 3001 + (i as u64) * 10_000, 10_000, "b10k")
    };
    const UPS_SUF: &str = " ON DUPLICATE KEY UPDATE note='x9'";
    // upsert 单行：打 upd 区（已存在 → 走 UPDATE 分支），验证唯一键检查开销
    let sql_ups1 = |r: &mut StdRng, c: &Ctx, _i: usize| {
        let id = c.upd_lo + (r.gen_range(0..200) as u64);
        format!("INSERT INTO {tb} ({COLS_FULL}) VALUES {}{UPS_SUF}", ins_vals(id, &format!("up{id}")), tb = t())
    };
    // upsert 批量 100 行/语句：upd 区 200 行两窗交替重复 upsert
    let sql_ups100 = |_r: &mut StdRng, c: &Ctx, i: usize| {
        let s = c.upd_lo + ((i as u64 % 2) * 100);
        let mut vals = Vec::with_capacity(100);
        for id in s..s + 100 {
            vals.push(ins_vals(id, &format!("u{id}")));
        }
        format!("INSERT INTO {tb} ({COLS_FULL}) VALUES {}{UPS_SUF}", vals.join(","), tb = t())
    };
    // 多字段 GROUP BY status, region（40 组）
    let sql_gbm = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT status, region, COUNT(*) FROM {tb} GROUP BY status, region", tb = t())
    };
    // GROUP BY + HAVING AVG(amount) > 阈值（聚合过滤）
    let sql_havg = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT region, AVG(amount) FROM {tb} GROUP BY region HAVING AVG(amount) > 500000", tb = t())
    };
    // 多列排序（k, amount），全表 filesort
    let sql_orderm = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT id, k, amount FROM {tb} ORDER BY k, amount DESC LIMIT 100", tb = t())
    };
    // 组合索引前置列点查：WHERE status='active' AND ts=v
    let sql_cpt = |r: &mut StdRng, _c: &Ctx, _i: usize| {
        let v = 1_700_000_000u64 + r.gen_range(0..=30_000_000u64);
        format!("SELECT id,status,ts FROM {tb} WHERE status='active' AND ts={v}", tb = t())
    };
    // 组合索引非前置列范围：WHERE ts BETWEEN ...
    let sql_crng = |r: &mut StdRng, _c: &Ctx, _i: usize| {
        let v = 1_700_000_000u64 + r.gen_range(0..29_000_000u64);
        format!("SELECT id,ts FROM {tb} WHERE ts BETWEEN {v} AND {}", v + 10_000, tb = t())
    };
    // 锁等待探针：主会话 FOR UPDATE 持锁 sleep 后提交；副会话同 id UPDATE 等锁（超时 3s）
    let sql_lock = |r: &mut StdRng, c: &Ctx, _i: usize| {
        let id = c.upd_lo + (r.gen_range(0..200) as u64);
        format!("SELECT k,amount FROM {tb} WHERE id={id} FOR UPDATE", tb = t())
    };
    // ---------- 第三批（2026-09-05，A~J 44 项性能分档探针） ----------
    // 档位语义 = 每查询目标结果行数（LIMIT/窗口宽度）；执行次数按类别控制（点查/倒排/写偏多、
    // 聚合/排序偏少）。MySQL 端同 SQL（同宽表列），数据量不强制与 SCC 一致（瓶颈快扫用）。
    let sql_in200 = |r: &mut StdRng, c: &Ctx, _i: usize| in_sql(r, c, 200);
    let sql_in1000 = |r: &mut StdRng, c: &Ctx, _i: usize| in_sql(r, c, 1000);
    let sql_in5000 = |r: &mut StdRng, c: &Ctx, _i: usize| in_sql(r, c, 5000);
    let sql_win500 = |r: &mut StdRng, c: &Ctx, _i: usize| {
        let a = r.gen_range(1..c.n.saturating_sub(509));
        format!("SELECT id,k,status FROM {tb} WHERE id BETWEEN {a} AND {}", a + 499, tb = t())
    };
    let sql_win3000 = |r: &mut StdRng, c: &Ctx, _i: usize| {
        let a = r.gen_range(1..c.n.saturating_sub(3009));
        format!("SELECT id,k,status FROM {tb} WHERE id BETWEEN {a} AND {}", a + 2999, tb = t())
    };
    let sql_win10000 = |r: &mut StdRng, c: &Ctx, _i: usize| {
        let a = r.gen_range(1..c.n.saturating_sub(10009));
        format!("SELECT id,k,status FROM {tb} WHERE id BETWEEN {a} AND {}", a + 9999, tb = t())
    };
    let sql_enum500 = |r: &mut StdRng, _c: &Ctx, _i: usize| {
        let s = ["active", "closed", "pending", "failed", "archived"][r.gen_range(0..5)];
        format!("SELECT id,status FROM {tb} WHERE status='{s}' LIMIT 500", tb = t())
    };
    let sql_enum3000 = |r: &mut StdRng, _c: &Ctx, _i: usize| {
        let s = ["active", "closed", "pending", "failed", "archived"][r.gen_range(0..5)];
        format!("SELECT id,status FROM {tb} WHERE status='{s}' LIMIT 3000", tb = t())
    };
    let sql_enum10000 = |r: &mut StdRng, _c: &Ctx, _i: usize| {
        let s = ["active", "closed", "pending", "failed", "archived"][r.gen_range(0..5)];
        format!("SELECT id,status FROM {tb} WHERE status='{s}' LIMIT 10000", tb = t())
    };
    let sql_combo500 = |r: &mut StdRng, _c: &Ctx, _i: usize| {
        let s = ["active", "closed"][r.gen_range(0..2)];
        let g = ["beijing", "shanghai", "shenzhen", "hangzhou"][r.gen_range(0..4)];
        format!("SELECT id FROM {tb} WHERE status='{s}' AND region='{g}' LIMIT 500", tb = t())
    };
    let sql_combo3000 = |r: &mut StdRng, _c: &Ctx, _i: usize| {
        let s = ["active", "closed"][r.gen_range(0..2)];
        let g = ["beijing", "shanghai", "shenzhen", "hangzhou"][r.gen_range(0..4)];
        format!("SELECT id FROM {tb} WHERE status='{s}' AND region='{g}' LIMIT 3000", tb = t())
    };
    let sql_fieldin500 = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT id FROM {tb} WHERE status IN ('active','closed','pending') LIMIT 500", tb = t())
    };
    let sql_fieldin3000 = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT id FROM {tb} WHERE status IN ('active','closed','pending') LIMIT 3000", tb = t())
    };
    let sql_three = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT id FROM {tb} WHERE status='active' AND region='beijing' AND channel='web' LIMIT 100", tb = t())
    };
    let sql_uid = |r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT id,user_id FROM {tb} WHERE user_id={} LIMIT 100", r.gen_range(1..=5_000_000u32), tb = t())
    };
    let sql_gt500 = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT id,amount FROM {tb} WHERE amount > 900000 LIMIT 500", tb = t())
    };
    let sql_gt3000 = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT id,amount FROM {tb} WHERE amount > 900000 LIMIT 3000", tb = t())
    };
    let sql_between_wide = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        let a = 200_000 + _r.gen_range(0..200);
        format!("SELECT id,amount FROM {tb} WHERE amount BETWEEN {a} AND {}", a + 1000, tb = t())
    };
    let sql_like = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT id,title FROM {tb} WHERE title LIKE 'a%' LIMIT 100", tb = t())
    };
    let sql_cntenum = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        let s = ["active", "closed", "pending", "failed", "archived"][_r.gen_range(0..5)];
        format!("SELECT COUNT(*) FROM {tb} WHERE status='{s}'", tb = t())
    };
    let sql_gbs = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT status, COUNT(*) FROM {tb} GROUP BY status LIMIT 20", tb = t())
    };
    let sql_gb2 = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT region, channel, COUNT(*) FROM {tb} WHERE status='active' GROUP BY region, channel", tb = t())
    };
    let sql_dst_e = |_r: &mut StdRng, _c: &Ctx, _i: usize| format!("SELECT COUNT(DISTINCT status) FROM {tb}", tb = t());
    let sql_dst_h = |_r: &mut StdRng, _c: &Ctx, _i: usize| format!("SELECT COUNT(DISTINCT user_id) FROM {tb}", tb = t());
    let sql_om500 = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT id, k, amount FROM {tb} ORDER BY k, amount DESC LIMIT 500", tb = t())
    };
    let sql_om3000 = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT id, k, amount FROM {tb} ORDER BY k, amount DESC LIMIT 3000", tb = t())
    };
    let sql_os10k = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT id, score FROM {tb} ORDER BY score DESC LIMIT 10000", tb = t())
    };
    let sql_owoff = |_r: &mut StdRng, c: &Ctx, _i: usize| {
        let a = _r.gen_range(1..c.n.saturating_sub(30_000));
        format!("SELECT id,amount FROM {tb} WHERE id BETWEEN {a} AND {} ORDER BY amount DESC LIMIT 100 OFFSET 1000", a + 20_000, tb = t())
    };
    // ts 范围窗：30s/行（wide_load ts 均匀 3000 万秒/100 万行）→ 宽 15000s ≈ 500 行、30000s ≈ 1000 行
    let sql_crng500 = |r: &mut StdRng, _c: &Ctx, _i: usize| {
        let v = 1_700_000_000u64 + r.gen_range(0..(30_000_000u64 - 15_000));
        format!("SELECT id,ts FROM {tb} WHERE ts BETWEEN {v} AND {}", v + 15_000, tb = t())
    };
    let sql_crng1000 = |r: &mut StdRng, _c: &Ctx, _i: usize| {
        let v = 1_700_000_000u64 + r.gen_range(0..(30_000_000u64 - 30_000));
        format!("SELECT id,ts FROM {tb} WHERE ts BETWEEN {v} AND {}", v + 30_000, tb = t())
    };
    let sql_cidx_eq = |r: &mut StdRng, _c: &Ctx, _i: usize| {
        let v = 1_700_000_000u64 + r.gen_range(0..=30_000_000u64);
        format!("SELECT id,status,ts FROM {tb} WHERE status='active' AND ts={v} LIMIT 100", tb = t())
    };
    // H 档插入窗：base+33001 起按档位长递增（cleanup 统一删 base+1501..46001）
    let sql_ins500 = |_r: &mut StdRng, c: &Ctx, i: usize| ins_multi(c.base + 33_001 + (i as u64) * 500, 500, "h1");
    let sql_ins2000 = |_r: &mut StdRng, c: &Ctx, i: usize| ins_multi(c.base + 33_001 + 3_000 + (i as u64) * 2000, 2000, "h2");
    let sql_ups500 = |_r: &mut StdRng, c: &Ctx, i: usize| {
        let s = c.delc_lo + ((i as u64 % 2) * 500);
        let mut vals = Vec::with_capacity(500);
        for id in s..s + 500 {
            vals.push(ins_vals(id, &format!("u{id}")));
        }
        format!("INSERT INTO {tb} ({COLS_FULL}) VALUES {}{UPS_SUF}", vals.join(","), tb = t())
    };
    let sql_hotupd = |_r: &mut StdRng, c: &Ctx, _i: usize| {
        format!("UPDATE {tb} SET note='x9' WHERE id={}", c.upd_lo, tb = t()) // 热点同一主键反复 update
    };
    let sql_delc = |_r: &mut StdRng, c: &Ctx, _i: usize| {
        let lo = c.delc_lo;
        format!("DELETE FROM {tb} WHERE id BETWEEN {lo} AND {}", lo + 999, tb = t())
    };
    let sql_upd_range = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("UPDATE {tb} SET note='x9' WHERE id BETWEEN 1 AND 20000 AND status='active' LIMIT 200", tb = t())
    };
    // P131（2026-09-06）：值变形态 #75 —— note 每次赋不同字面量（p131c{i}），保证每轮
    // affected=200（官方探针 note='x9' 因 loader 同值恒 rows=0 = 读现值路径，测不出写链）。
    // 声明配置（bitmap status/region）下 note 非索引列 → SCC 走 Delta CF patch_batch。
    let sql_upd_range_chg = |_r: &mut StdRng, _c: &Ctx, i: usize| {
        format!("UPDATE {tb} SET note='p131c{i}' WHERE id BETWEEN 1 AND 20000 AND status='active' LIMIT 200", tb = t())
    };
    let sql_longread = |_r: &mut StdRng, c: &Ctx, _i: usize| {
        // 窗宽 100k：干净 100k 库上 n-100000=0 → 采 [1..=1]（整表窗），避免空区间 panic
        let lo = c.n.saturating_sub(100_000).max(1);
        let a = _r.gen_range(1..=lo);
        format!("SELECT id,k,amount FROM {tb} WHERE id BETWEEN {a} AND {}", a + 100_000, tb = t())
    };
    let sql_biz500 = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT id,k,amount,ts FROM {tb} WHERE status='active' ORDER BY ts DESC LIMIT 500 OFFSET 0", tb = t())
    };
    let sql_bizpage = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT id,k,amount,ts FROM {tb} WHERE status='active' ORDER BY ts DESC LIMIT 100 OFFSET 2000", tb = t())
    };
    let sql_bizagg = |_r: &mut StdRng, _c: &Ctx, _i: usize| {
        format!("SELECT region, COUNT(*) FROM {tb} WHERE status='active' GROUP BY region ORDER BY COUNT(*) DESC LIMIT 20", tb = t())
    };
    let sql_multi = |_r: &mut StdRng, _c: &Ctx, _i: usize| "--multi".to_string();

    let list: Vec<Probe> = vec![
        Probe { cat: "点查", name: "pk_point_star", kind: Kind::Rows, n: 300, sql: sql_pk, note: "SELECT *" },
        Probe { cat: "点查", name: "pk_point_proj10", kind: Kind::Rows, n: 300, sql: sql_proj, note: "10 列投影" },
        Probe { cat: "点查", name: "pk_in_5", kind: Kind::Rows, n: 150, sql: sql_in5, note: "id IN 5 点" },
        Probe { cat: "点查", name: "pk_in_50", kind: Kind::Rows, n: 30, sql: sql_in50, note: "id IN 50 点（批量查询）" },
        Probe { cat: "范围", name: "pk_between_100", kind: Kind::Rows, n: 150, sql: sql_range, note: "100 行窗口" },
        Probe { cat: "倒排", name: "enum_sel_limit100", kind: Kind::Rows, n: 60, sql: sql_enum, note: "枚举等值 bitmap" },
        Probe { cat: "倒排", name: "enum_count", kind: Kind::Rows, n: 60, sql: sql_count, note: "COUNT 倒排载荷" },
        Probe { cat: "倒排", name: "combo_and", kind: Kind::Rows, n: 60, sql: sql_combo, note: "枚举×枚举 AND" },
        Probe { cat: "倒排", name: "field_in", kind: Kind::Rows, n: 60, sql: sql_fieldin, note: "字段 IN 列表" },
        Probe { cat: "扫描", name: "cmp_gt_limit50", kind: Kind::Rows, n: 20, sql: sql_cmpgt, note: "数值> LIMIT 早停" },
        Probe { cat: "扫描", name: "cmp_between", kind: Kind::Rows, n: 20, sql: sql_cmpbetween, note: "数值 BETWEEN（全扫）" },
        Probe { cat: "聚合", name: "count_all", kind: Kind::Rows, n: 5, sql: sql_cntall, note: "无条件 COUNT" },
        Probe { cat: "聚合", name: "sum_where_enum", kind: Kind::Rows, n: 3, sql: sql_sumwhere, note: "SUM WHERE（全扫）" },
        Probe { cat: "聚合", name: "group_by_status", kind: Kind::Rows, n: 3, sql: sql_gb, note: "全扫分组" },
        Probe { cat: "聚合", name: "group_by_sum_having", kind: Kind::Rows, n: 3, sql: sql_gbsum, note: "多聚合+HAVING(函数式)" },
        Probe { cat: "排序", name: "orderby_win_1000", kind: Kind::Rows, n: 20, sql: sql_orderwin, note: "窗口 1000 ORDER BY" },
        Probe { cat: "写", name: "update_id", kind: Kind::Exec, n: 100, sql: sql_upd, note: "UPDATE id=" },
        Probe { cat: "写", name: "update_in2", kind: Kind::Exec, n: 50, sql: sql_upd_in2, note: "UPDATE id IN 2" },
        Probe { cat: "写", name: "update_in50", kind: Kind::Exec, n: 20, sql: sql_upd_in50, note: "UPDATE id IN 50（批量更新）" },
        Probe { cat: "写", name: "insert_single", kind: Kind::Exec, n: 100, sql: sql_ins_s, note: "INSERT 单行" },
        Probe { cat: "写", name: "insert_batch10", kind: Kind::Exec, n: 30, sql: sql_insb10, note: "INSERT 10 行/语句" },
        Probe { cat: "写", name: "insert_batch100", kind: Kind::Exec, n: 10, sql: sql_insb100, note: "INSERT 100 行/语句" },
        Probe { cat: "写", name: "delete_id", kind: Kind::Exec, n: 100, sql: sql_del, note: "DELETE id=" },
        Probe { cat: "写", name: "delete_range50", kind: Kind::Exec, n: 10, sql: sql_del_range50, note: "DELETE 50 行区间（批量删除）" },
        Probe { cat: "事务", name: "txn_begin_upd_commit", kind: Kind::Block, n: 100, sql: txn_upd, note: "BEGIN→UPDATE→COMMIT" },
        Probe { cat: "事务", name: "txn_for_update_read", kind: Kind::Block, n: 50, sql: sql_fu, note: "BEGIN→FOR UPDATE→COMMIT" },
        Probe { cat: "聚合", name: "group_by_multi", kind: Kind::Rows, n: 3, sql: sql_gbm, note: "GROUP BY status,region" },
        Probe { cat: "聚合", name: "having_avg_gt", kind: Kind::Rows, n: 3, sql: sql_havg, note: "GROUP BY+HAVING AVG>阈值" },
        Probe { cat: "排序", name: "orderby_multi", kind: Kind::Rows, n: 10, sql: sql_orderm, note: "ORDER BY k,amount LIMIT 100" },
        Probe { cat: "索引", name: "composite_idx_point", kind: Kind::Rows, n: 100, sql: sql_cpt, note: "status= + ts= 点查" },
        Probe { cat: "索引", name: "composite_idx_range", kind: Kind::Rows, n: 20, sql: sql_crng, note: "ts 范围（非前置列）" },
        Probe { cat: "批量写", name: "insert_batch_10000", kind: Kind::Exec, n: 3, sql: sql_insb10000, note: "INSERT 10000 行/语句" },
        Probe { cat: "批量写", name: "upsert_duplicate_key", kind: Kind::Exec, n: 100, sql: sql_ups1, note: "INSERT..ON DUPLICATE KEY 单行" },
        Probe { cat: "批量写", name: "upsert_batch_100", kind: Kind::Exec, n: 10, sql: sql_ups100, note: "INSERT..ON DUP KEY 100行/语句" },
        Probe { cat: "事务", name: "txn_rr_readwrite", kind: Kind::Block, n: 50, sql: txn_upd, note: "RR 读写事务" },
        Probe { cat: "事务", name: "txn_serializable", kind: Kind::Block, n: 50, sql: txn_upd, note: "SERIALIZABLE 读写事务" },
        Probe { cat: "事务", name: "txn_lock_wait", kind: Kind::Block, n: 5, sql: sql_lock, note: "并发 FOR UPDATE 锁等待" },
        // ---- 第三批（2026-09-05，A~J 44 项性能分档；档位 = 结果行数） ----
        Probe { cat: "点查", name: "pk_in_200", kind: Kind::Rows, n: 60, sql: sql_in200, note: "id IN 200 点" },
        Probe { cat: "点查", name: "pk_in_1000", kind: Kind::Rows, n: 40, sql: sql_in1000, note: "id IN 1000 点" },
        Probe { cat: "点查", name: "pk_in_5000", kind: Kind::Rows, n: 15, sql: sql_in5000, note: "id IN 5000 点" },
        Probe { cat: "范围", name: "pk_between_500", kind: Kind::Rows, n: 60, sql: sql_win500, note: "500 行窗口" },
        Probe { cat: "范围", name: "pk_between_3000", kind: Kind::Rows, n: 30, sql: sql_win3000, note: "3000 行窗口" },
        Probe { cat: "范围", name: "pk_between_10000", kind: Kind::Rows, n: 15, sql: sql_win10000, note: "10000 行窗口" },
        Probe { cat: "倒排", name: "enum_sel_limit500", kind: Kind::Rows, n: 40, sql: sql_enum500, note: "枚举等值 bitmap limit 500" },
        Probe { cat: "倒排", name: "enum_sel_limit3000", kind: Kind::Rows, n: 30, sql: sql_enum3000, note: "枚举等值 bitmap limit 3000" },
        Probe { cat: "倒排", name: "enum_sel_limit10000", kind: Kind::Rows, n: 15, sql: sql_enum10000, note: "枚举等值 bitmap limit 10000" },
        Probe { cat: "倒排", name: "combo_and_limit500", kind: Kind::Rows, n: 40, sql: sql_combo500, note: "枚举×枚举 AND limit 500" },
        Probe { cat: "倒排", name: "combo_and_limit3000", kind: Kind::Rows, n: 30, sql: sql_combo3000, note: "枚举×枚举 AND limit 3000" },
        Probe { cat: "倒排", name: "field_in_limit500", kind: Kind::Rows, n: 40, sql: sql_fieldin500, note: "字段 IN 列表 limit 500" },
        Probe { cat: "倒排", name: "field_in_limit3000", kind: Kind::Rows, n: 30, sql: sql_fieldin3000, note: "字段 IN 列表 limit 3000" },
        Probe { cat: "倒排", name: "combo_three_and", kind: Kind::Rows, n: 40, sql: sql_three, note: "三枚举 AND limit 100" },
        Probe { cat: "倒排", name: "enum_card_high_sel100", kind: Kind::Rows, n: 40, sql: sql_uid, note: "高基数字段过滤 limit 100" },
        Probe { cat: "扫描", name: "cmp_gt_limit500", kind: Kind::Rows, n: 20, sql: sql_gt500, note: "数值> limit 500 早停" },
        Probe { cat: "扫描", name: "cmp_gt_limit3000", kind: Kind::Rows, n: 10, sql: sql_gt3000, note: "数值> limit 3000 早停" },
        Probe { cat: "扫描", name: "cmp_between_nolimit", kind: Kind::Rows, n: 20, sql: sql_between_wide, note: "数值 between 无 limit（宽窗返回~千行）" },
        Probe { cat: "扫描", name: "cmp_like_prefix", kind: Kind::Rows, n: 20, sql: sql_like, note: "前缀 like 无索引 limit 100" },
        Probe { cat: "聚合", name: "count_where_enum", kind: Kind::Rows, n: 10, sql: sql_cntenum, note: "COUNT WHERE 枚举（倒排命中）" },
        Probe { cat: "聚合", name: "sum_where_idx", kind: Kind::Rows, n: 10, sql: sql_sumwhere, note: "SUM WHERE 倒排过滤后聚合" },
        Probe { cat: "聚合", name: "group_by_status_limit", kind: Kind::Rows, n: 5, sql: sql_gbs, note: "group by status limit 20" },
        Probe { cat: "聚合", name: "group_by_two_where", kind: Kind::Rows, n: 5, sql: sql_gb2, note: "双字段 group by + where 过滤" },
        Probe { cat: "聚合", name: "count_distinct_enum", kind: Kind::Rows, n: 5, sql: sql_dst_e, note: "COUNT(DISTINCT 枚举)" },
        Probe { cat: "聚合", name: "count_distinct_highcard", kind: Kind::Rows, n: 5, sql: sql_dst_h, note: "COUNT(DISTINCT 高基数 user_id)" },
        Probe { cat: "排序", name: "orderby_multi_limit500", kind: Kind::Rows, n: 8, sql: sql_om500, note: "多字段 ORDER BY limit 500" },
        Probe { cat: "排序", name: "orderby_multi_limit3000", kind: Kind::Rows, n: 5, sql: sql_om3000, note: "多字段 ORDER BY limit 3000" },
        Probe { cat: "排序", name: "orderby_single_desc_10000", kind: Kind::Rows, n: 5, sql: sql_os10k, note: "单字段排序 limit 10000" },
        Probe { cat: "排序", name: "orderby_win_offset_1000", kind: Kind::Rows, n: 10, sql: sql_owoff, note: "窗口深分页 offset 1000 limit 100" },
        Probe { cat: "索引", name: "composite_idx_range_limit500", kind: Kind::Rows, n: 20, sql: sql_crng500, note: "联合索引非前置列范围（~500 行）" },
        Probe { cat: "索引", name: "composite_idx_multi_eq", kind: Kind::Rows, n: 60, sql: sql_cidx_eq, note: "联合索引多列全等值 limit 100" },
        Probe { cat: "索引", name: "idx_range_scan_1000", kind: Kind::Rows, n: 20, sql: sql_crng1000, note: "二级索引范围扫描读 ~1000 行" },
        Probe { cat: "批量写", name: "insert_batch_500", kind: Kind::Exec, n: 6, sql: sql_ins500, note: "单语句插入 500 行" },
        Probe { cat: "批量写", name: "insert_batch_2000", kind: Kind::Exec, n: 4, sql: sql_ins2000, note: "单语句插入 2000 行" },
        Probe { cat: "批量写", name: "upsert_batch_500", kind: Kind::Exec, n: 10, sql: sql_ups500, note: "ON DUPLICATE KEY 500 行/语句（delc 区交替窗）" },
        Probe { cat: "写", name: "update_hotrow_single", kind: Kind::Exec, n: 200, sql: sql_hotupd, note: "热点同一主键反复 update" },
        Probe { cat: "写", name: "delete_range_1000", kind: Kind::Exec, n: 1, sql: sql_delc, note: "范围删除 1000 行" },
        Probe { cat: "写", name: "update_range_idx", kind: Kind::Exec, n: 20, sql: sql_upd_range, note: "索引条件批量 update 200 行" },
        Probe { cat: "写", name: "update_range_idx_chg", kind: Kind::Exec, n: 20, sql: sql_upd_range_chg, note: "索引条件批量 update 200 行（值变形态，P131 delta 写链）" },
        Probe { cat: "事务", name: "txn_lock_mid_contend", kind: Kind::Block, n: 5, sql: sql_lock, note: "中等并发 for update 部分锁冲突（双连接）" },
        Probe { cat: "事务", name: "txn_long_read", kind: Kind::Block, n: 5, sql: sql_longread, note: "长只读快照事务（读 10 万行窗）" },
        Probe { cat: "事务", name: "txn_multi_stat", kind: Kind::Block, n: 20, sql: sql_multi, note: "事务内多条 DML 混合" },
        Probe { cat: "混合", name: "biz_list_query", kind: Kind::Rows, n: 10, sql: sql_biz500, note: "倒排过滤+时间排序 limit 500" },
        Probe { cat: "混合", name: "biz_page_offset", kind: Kind::Rows, n: 10, sql: sql_bizpage, note: "where+order by offset 2000 limit 100" },
        Probe { cat: "混合", name: "biz_agg_filter", kind: Kind::Rows, n: 5, sql: sql_bizagg, note: "where+group by+order by 聚合 limit 20" },
        // ---- P141 A1-1（2026-09-07）：事务内聚合权威探针（与 MySQL 双端对拍；块内 ROLLBACK 零污染） ----
        Probe { cat: "事务聚合", name: "txn_agg_cnt_all", kind: Kind::Block, n: 5, sql: txn_agg_mk, note: "BEGIN→自插→COUNT(*) 全表→ROLLBACK（快照含自插）" },
        Probe { cat: "事务聚合", name: "txn_agg_scalar", kind: Kind::Block, n: 10, sql: txn_agg_mk, note: "BEGIN→同事务改 k/status→标量多函数窗口→ROLLBACK" },
        Probe { cat: "事务聚合", name: "txn_agg_avg", kind: Kind::Block, n: 10, sql: txn_agg_mk, note: "BEGIN→同事务改 k/amount→AVG 数值语义→ROLLBACK" },
        Probe { cat: "事务聚合", name: "txn_agg_group", kind: Kind::Block, n: 10, sql: txn_agg_mk, note: "BEGIN→改分布→GROUP BY/HAVING→自删全窗→空集 SUM NULL→ROLLBACK" },
        Probe { cat: "事务聚合", name: "txn_agg_fu", kind: Kind::Block, n: 8, sql: txn_agg_mk, note: "BEGIN→FOR UPDATE 聚合（当前读锁定/自写/空集）→ROLLBACK" },
    ];

    let env_note = if url.contains("3316") {
        "MySQL 8.0（独立实例 3316，innodb_buffer_pool_size = 2G）"
    } else if url.contains("3317") {
        "cjserver（shanshui-cunji，3317，2G 内存预算 = hotcache 1024 + blockcache 512 + inverted 256 + memtable 256 MB）"
    } else {
        "目标端"
    };

    std::fs::create_dir_all(out).expect("创建输出目录");
    let mut md = String::new();
    md.push_str(&format!("# SQL 性能探针（sqlrun）\n\nurl={url}  表={tb}  N={n}  宽表 25 列  环境={env_note}\n\n"));
    md.push_str("| # | 类别 | 探针 | 说明 | OK/n | 行/影响 | mean ms | p50 ms | p99 ms | max ms |\n|---|---|---|---|---|---|---|---|---|---|\n");

    for (idx, p) in list.iter().enumerate() {
        if filtered && !want.contains(&p.name) {
            continue;
        }
        let mut rng = StdRng::seed_from_u64(100 + idx as u64);
        let mut ms: Vec<f64> = Vec::new();
        let mut ok = 0usize;
        let mut rows = 0usize;
        let mut detail = String::new();
        let mut err_txt = String::new();
        for i in 0..p.n {
            let sql = (p.sql)(&mut rng, &ctx, i);
            let t0 = Instant::now();
            let r = match p.kind {
                Kind::Rows => exec_stmt(&mut conn, &sql).let_rows(),
                Kind::Exec => exec_stmt(&mut conn, &sql).let_exec(),
                Kind::Block => run_block(&mut conn, &url, &sql, p.name, ctx.upd_lo),
            };
            let dt = t0.elapsed().as_secs_f64() * 1000.0;
            match r {
                Ok((rn, d)) => {
                    ms.push(dt);
                    ok += 1;
                    rows = rn;
                    detail = d;
                }
                Err(e) => {
                    err_txt = e;
                    break;
                }
            }
        }
        let (mean, p50, p99, mx) = if ms.is_empty() {
            (0.0, 0.0, 0.0, 0.0)
        } else {
            let mut s = ms.clone();
            s.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let q = |f: f64| s[((s.len() as f64 * f).floor() as usize).min(s.len() - 1)];
            (s.iter().sum::<f64>() / s.len() as f64, q(0.5), q(0.99), s[s.len() - 1])
        };
        let extra = if !detail.is_empty()
            && detail != "ok"
            && detail != "block"
            && detail != "multi-dml"
        {
            format!("  ⚑ {detail}")
        } else {
            String::new()
        };
        let tail = if err_txt.is_empty() {
            extra
        } else {
            format!("  ❌ {err_txt}")
        };
        let note = format!("| {} | {} | {} | {} | {}/{} | {} | {:.2} | {:.2} | {:.2} | {:.2} |{}",
                           idx + 1, p.cat, p.name, p.note, ok, p.n, rows, mean, p50, p99, mx, tail);
        println!("[{:02}] {} {:<22} ok={}/{} rows={} mean={:.2}ms p50={:.2} p99={:.2} max={:.2}{}",
                 idx + 1, p.cat, p.name, ok, p.n, rows, mean, p50, p99, mx, tail);
        md.push_str(&note);
        md.push('\n');
    }
    // 先落 summary（保证结果不依赖清理是否成功；SCC 大区间清理可能长时间挂起）
    std::fs::write(format!("{out}/summary.md"), md).expect("写 summary");
    println!("\n[sqlrun] 完成。summary: {out}/summary.md（环境={env_note}）");
    // 清理写区（upd 区保留；ins/del/delb/delc + 大插区 base+1501..46001 整区删除，恢复初始行数；
    // best-effort：失败/挂起不影响已落盘的 summary）
    let _ = exec_stmt(&mut conn, &format!("DELETE FROM {tb} WHERE id BETWEEN {} AND {}", ctx.base + 1501, ctx.base + 46001, tb = t()));
    let _ = exec_stmt(&mut conn, &format!("DELETE FROM {tb} WHERE id BETWEEN {} AND {}", ctx.delc_lo, ctx.delc_lo + 999, tb = t()));
    let _ = exec_stmt(&mut conn, &format!("DELETE FROM {tb} WHERE id BETWEEN {} AND {}", ctx.delb_lo, ctx.delb_lo + 499, tb = t()));
    let _ = exec_stmt(&mut conn, &format!("DELETE FROM {tb} WHERE id BETWEEN {} AND {}", ctx.del_lo, ctx.del_lo + 99, tb = t()));
    0
}

/// 事务块：按探针 name 分发行为 + BEGIN → 体内语句 → COMMIT。
/// - name "txn_lock_wait"：双连接锁等待（主连接持锁 sleep 后提交，副连接等锁超时 3s）
/// - name "txn_rr_readwrite"/"txn_serializable"：先 SET SESSION 对应隔离级
/// - name "txn_for_update_read"：按结果行数计；其余写按 affected 计
fn run_block(conn: &mut mysql::Conn, url: &str, body: &str, name: &'static str, upd_lo: u64) -> Result<(usize, String), String> {
    if name == "txn_lock_wait" || name == "txn_lock_mid_contend" {
        return run_lock_wait(conn, url, body);
    }
    if name == "txn_multi_stat" {
        return run_multi_stat(conn, upd_lo);
    }
    if name.starts_with("txn_agg") {
        return run_txn_agg(conn, name, upd_lo);
    }
    if name == "txn_rr_readwrite" {
        let _ = exec_stmt(conn, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    } else if name == "txn_serializable" {
        let _ = exec_stmt(conn, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    }
    let r0 = exec_stmt(conn, "BEGIN");
    if r0.err.is_some() {
        return Err(format!("BEGIN: {}", r0.err.unwrap()));
    }
    let r = exec_stmt(conn, body);
    let rn = if name == "txn_for_update_read" || name == "txn_long_read" {
        r.rows.len()
    } else {
        r.affected as usize
    };
    if r.err.is_some() {
        let _ = exec_stmt(conn, "ROLLBACK");
        return Err(format!("body: {}", r.err.unwrap()));
    }
    let rc = exec_stmt(conn, "COMMIT");
    if rc.err.is_some() {
        return Err(format!("COMMIT: {}", rc.err.unwrap()));
    }
    Ok((rn, format!("block")))
}

/// 事务内多条 DML 混合（txn_multi_stat）：BEGIN → 单点 UPDATE → 批量 UPDATE IN 10 →
/// 快照聚合读 → COMMIT（整块计时）。写作用于 upd 预留区，保证每次可重跑。
fn run_multi_stat(conn: &mut mysql::Conn, upd_lo: u64) -> Result<(usize, String), String> {
    let tb = t();
    let r0 = exec_stmt(conn, "BEGIN");
    if r0.err.is_some() {
        return Err(format!("BEGIN: {}", r0.err.unwrap()));
    }
    let mut affected = 0usize;
    let lo = upd_lo;
    for stmt in [
        format!("UPDATE {tb} SET score=0.5 WHERE id={lo}", tb = t()),
        format!("UPDATE {tb} SET note='x9' WHERE id IN ({lo},{})", lo + 1, tb = t()),
    ] {
        let r = exec_stmt(conn, &stmt);
        if r.err.is_some() {
            let _ = exec_stmt(conn, "ROLLBACK");
            return Err(format!("body: {}", r.err.unwrap()));
        }
        affected += r.affected as usize;
    }
    // 快照聚合读（不参与 affected）
    let _ = exec_stmt(conn, &format!("SELECT COUNT(*) FROM {tb}", tb = t()));
    let rc = exec_stmt(conn, "COMMIT");
    if rc.err.is_some() {
        return Err(format!("COMMIT: {}", rc.err.unwrap()));
    }
    Ok((affected, "multi-dml".to_string()))
}

/// P141 A1-1（2026-09-07）：事务内聚合探针执行族。每个 txn_agg_* 执行**确定性脚本**
/// （BEGIN → 同事务写覆盖/自插/自删 + 聚合读 → ROLLBACK，零持久污染、逐轮可重放），
/// 每步 SELECT 行集与**硬编码期望真值**比对（`compare::rows_to_keys` 数值规范化键——
/// 屏蔽 MySQL DECIMAL 尾零 / 整浮同值文本差异）。**本端断言通过（exp-ok）⇔ 结果符合
/// 权威真值；MySQL 与 SCC 各自 exp-ok 即行集数值语义等值。**
/// 覆盖三态：FOR UPDATE 当前读 / 同事务写自见（插·改·删）/ 空集数值聚合 SQL NULL；
/// 标量 COUNT/SUM/MIN/MAX/COUNT(DISTINCT)/AVG 与 GROUP BY/HAVING、无 WHERE 全表 COUNT。
fn run_txn_agg(conn: &mut mysql::Conn, name: &str, upd_lo: u64) -> Result<(usize, String), String> {
    let tb = t();
    let lo = upd_lo;
    let hi = lo + 4; // 窗 = upd 预留 5 行（预插值 k=1 / amount=1.00 / score=0.5 / status='active'）
    let w = format!("id BETWEEN {lo} AND {hi}");
    let x = lo + 200; // 自插空闲 id = upd_hi+1（upd 200 行之外，永不被其它探针占用）
    let steps: Vec<(&str, String, Option<&str>)> = match name {
        // 无 WHERE 全表 COUNT + 同事务自插可见（COUNT(*) 期望 = 加载 N+1，两侧同基线；
        // 自插局部断言恒 1）
        "txn_agg_cnt_all" => vec![
            ("ins", format!("INSERT INTO {tb} ({COLS_FULL}) VALUES {}", ins_vals(x, "txnagg")), None),
            ("cnt_all", format!("SELECT COUNT(*) FROM {tb}"), None),
            ("cnt_self", format!("SELECT COUNT(*) FROM {tb} WHERE id={x}"), Some("1")),
        ],
        // 同事务改 k（1→2 整窗）与 status 分布 → 整型标量多函数（单语句单聚合，逐条断言）
        "txn_agg_scalar" => vec![
            ("upd_k", format!("UPDATE {tb} SET k=2 WHERE {w}"), None),
            (
                "upd_s",
                format!("UPDATE {tb} SET status='b' WHERE id IN ({lo},{},{hi})", lo + 2),
                None,
            ),
            ("cnt", format!("SELECT COUNT(*) FROM {tb} WHERE {w}"), Some("5")),
            ("sum", format!("SELECT SUM(k) FROM {tb} WHERE {w}"), Some("10")),
            ("min", format!("SELECT MIN(k) FROM {tb} WHERE {w}"), Some("2")),
            ("max", format!("SELECT MAX(k) FROM {tb} WHERE {w}"), Some("2")),
            ("cdk", format!("SELECT COUNT(DISTINCT k) FROM {tb} WHERE {w}"), Some("1")),
            (
                "cds",
                format!("SELECT COUNT(DISTINCT status) FROM {tb} WHERE {w}"),
                Some("2"),
            ),
        ],
        // AVG 浮点数值语义（MySQL DECIMAL 尾零 vs SCC DOUBLE 文本经 value_key 归一；单聚合逐条）。
        // 注：SET 拆两条单列——单语句复合 SET（SET k=2, amount=2.50）超出 SCC 事务 UPDATE 单字段
        // 解析（M-7 已知缺口，非 P141 事务聚合面）；分步单列赋值两侧语义一致。
        "txn_agg_avg" => vec![
            ("upd_k", format!("UPDATE {tb} SET k=2 WHERE {w}"), None),
            ("upd_a", format!("UPDATE {tb} SET amount=2.50 WHERE {w}"), None),
            ("avg_k", format!("SELECT AVG(k) FROM {tb} WHERE {w}"), Some("2")),
            (
                "avg_amt",
                format!("SELECT AVG(amount) FROM {tb} WHERE {w}"),
                Some("2.5"),
            ),
            (
                "avg_score",
                format!("SELECT AVG(score) FROM {tb} WHERE {w}"),
                Some("0.5"),
            ),
        ],
        // 同事务改 status 分布 + k 分布 → GROUP BY(COUNT+SUM 双聚合)/HAVING → 自删整窗 →
        // 空集 SUM/AVG = SQL NULL
        "txn_agg_group" => vec![
            ("upd_b", format!("UPDATE {tb} SET status='b' WHERE id IN ({lo},{})", lo + 2), None),
            ("upd_c", format!("UPDATE {tb} SET status='c' WHERE id={hi}"), None),
            (
                "upd_ksum",
                format!("UPDATE {tb} SET k=2 WHERE id IN ({lo},{})", lo + 1),
                None,
            ),
            (
                "gb",
                format!(
                    "SELECT status, COUNT(*), SUM(k) FROM {tb} WHERE {w} GROUP BY status ORDER BY status"
                ),
                // active(lo+1 k2, lo+3 k1)=count2 sum3；b(lo k2, lo+2 k1)=count2 sum3；c(lo+4 k1)
                Some("active|2|3\nb|2|3\nc|1|1"),
            ),
            (
                "having",
                format!(
                    "SELECT status, COUNT(*) FROM {tb} WHERE {w} GROUP BY status HAVING COUNT(*) >= 2 ORDER BY status"
                ),
                Some("active|2\nb|2"),
            ),
            ("del_all", format!("DELETE FROM {tb} WHERE {w}"), None),
            (
                "empty_sum",
                format!("SELECT SUM(k) FROM {tb} WHERE {w}"),
                Some("NULL"),
            ),
            (
                "empty_avg",
                format!("SELECT AVG(k) FROM {tb} WHERE {w}"),
                Some("NULL"),
            ),
        ],
        // FOR UPDATE 当前读聚合：窗口锁定 → 同事务自改（当前读见自写）→ 自删行空集 NULL
        "txn_agg_fu" => vec![
            ("fu5", format!("SELECT SUM(k) FROM {tb} WHERE {w} FOR UPDATE"), Some("5")),
            ("upd9", format!("UPDATE {tb} SET k=9 WHERE id={lo}"), None),
            ("fu13", format!("SELECT SUM(k) FROM {tb} WHERE {w} FOR UPDATE"), Some("13")),
            ("del1", format!("DELETE FROM {tb} WHERE id={}", lo + 1), None),
            (
                "fu_lo",
                format!("SELECT SUM(k) FROM {tb} WHERE id BETWEEN {lo} AND {lo} FOR UPDATE"),
                Some("9"),
            ),
            (
                "fu_null",
                format!("SELECT SUM(k) FROM {tb} WHERE id BETWEEN {} AND {} FOR UPDATE", lo + 1, lo + 1),
                Some("NULL"),
            ),
        ],
        _ => return Err(format!("未知 txn_agg 探针: {name}")),
    };

    let r0 = exec_stmt(conn, "BEGIN");
    if r0.err.is_some() {
        return Err(format!("BEGIN: {}", r0.err.unwrap()));
    }
    let rollback = |conn: &mut mysql::Conn| {
        let _ = exec_stmt(conn, "ROLLBACK");
    };
    let mut sig = String::new();
    let mut last_rows = 0usize;
    for (label, sql, exp) in steps {
        let r = exec_stmt(conn, &sql);
        if let Some(e) = &r.err {
            rollback(conn);
            return Err(format!("{label}: {e}"));
        }
        // 聚合读步骤：记录规范化行集签名（有期望则断言）
        if !r.rows.is_empty() {
            let keys = crate::compare::rows_to_keys(&r.rows);
            sig.push_str(&format!("{label}=[{}];", keys.join(";")));
            last_rows = r.rows.len();
            if let Some(expk) = exp {
                let got = keys.join("\n");
                if got != expk {
                    rollback(conn);
                    return Err(format!(
                        "{label} 行集 != 期望真值\n期望:\n{expk}\n实际:\n{got}"
                    ));
                }
            }
        }
    }
    rollback(conn);
    Ok((last_rows, format!("txn-agg exp-ok {sig}")))
}

/// 锁等待探针：主连接 BEGIN + SELECT..FOR UPDATE 持锁 → 副连接对同 id UPDATE 等锁
/// → 主连接 sleep 4s 后 COMMIT。超时语义对齐 MySQL：`innodb_lock_wait_timeout=3` 必须作用于
/// **等待方会话**（副连接）——原实现 SET 在主连接上，副连接走 MySQL 默认 50s，探针永不触发
/// 1205（两侧都只是等主提交后拿到），Task-033 实测收敛后修正于此。
/// outcome 一律以 Ok 携带（waiter-ok / waiter-1205 / waiter-err），供套件逐轮记录对比。
fn run_lock_wait(conn: &mut mysql::Conn, url: &str, body: &str) -> Result<(usize, String), String> {
    let tb = t();
    let r0 = exec_stmt(conn, "BEGIN");
    if r0.err.is_some() {
        return Err(format!("BEGIN: {}", r0.err.unwrap()));
    }
    let r = exec_stmt(conn, body);
    if r.err.is_some() {
        let _ = exec_stmt(conn, "ROLLBACK");
        return Err(format!("for-update: {}", r.err.unwrap()));
    }
    // 从 body（...WHERE id=<N> FOR UPDATE）中提取目标行 id
    let id: u64 = body
        .split("id=")
        .nth(1)
        .and_then(|s| s.split_whitespace().next())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| "无法解析 FOR UPDATE id".to_string())?;
    let url2 = url.to_string();
    let waiter = std::thread::spawn(move || {
        let mut c2 = match mysql::Conn::new(url2.as_str()) {
            Ok(c) => c,
            Err(e) => return Err(format!("副连接失败: {e}")),
        };
        // 超时作用于等待方（副连接）会话：3s 未获锁 → MySQL 1205
        let _ = exec_stmt(&mut c2, "SET SESSION innodb_lock_wait_timeout=3");
        let w0 = std::time::Instant::now();
        let w = exec_stmt(&mut c2, &format!("UPDATE {tb} SET note='x9' WHERE id={id}", tb = t()));
        let w_ms = w0.elapsed().as_secs_f64() * 1000.0;
        match w.err {
            Some(e) => Err(format!("{e} (waiter-t={w_ms:.0}ms)")),
            None => Ok(format!("waiter-t={w_ms:.0}ms")),
        }
    });
    std::thread::sleep(std::time::Duration::from_secs(4));
    let rc = exec_stmt(conn, "COMMIT");
    if rc.err.is_some() {
        let _ = exec_stmt(conn, "ROLLBACK");
        return Err(format!("COMMIT: {}", rc.err.unwrap()));
    }
    let w = waiter.join().unwrap_or_else(|_| Err("waiter 线程异常".to_string()));
    let outcome = match &w {
        Ok(d) => format!("waiter-ok(先等锁后拿到 {d})"),
        Err(e) if e.contains("1205") => format!("waiter-1205锁等待超时({e})"),
        Err(e) => format!("waiter-err({e})"),
    };
    // 1205 是 MySQL 预期收敛结果而非探针失败：以 Ok 返回并把 outcome 带给套件记录
    Ok((0, outcome))
}

// 小工具：把 exec_stmt 结果折叠为 Result<(usize,String),String>，保持 match 分支类型统一。
trait FoldRes {
    fn let_rows(self) -> Result<(usize, String), String>;
    fn let_exec(self) -> Result<(usize, String), String>;
}
impl FoldRes for crate::tx::ExecRes {
    fn let_rows(self) -> Result<(usize, String), String> {
        match self.err {
            Some(e) => Err(e.to_string()),
            None => Ok((self.rows.len(), "ok".into())),
        }
    }
    fn let_exec(self) -> Result<(usize, String), String> {
        match self.err {
            Some(e) => Err(e.to_string()),
            None => Ok((self.affected as usize, "ok".into())),
        }
    }
}
