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

pub fn run(url: &str, out: &str, table: &str) -> i32 {
    let _ = TAB.set(table.to_string());
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
    println!("[sqlrun] 预留行：upd {}-{}、del {}-{}、delb {}-{}（{}{}{}）",
             ctx.upd_lo, ctx.upd_hi, ctx.del_lo, ctx.del_lo + 99,
             ctx.delb_lo, ctx.delb_lo + 499,
             if upd_ok { "OK " } else { "UPD-FAIL " },
             if del_ok { "OK " } else { "DEL-FAIL " },
             if delb_ok { "OK" } else { "DELB-FAIL" });

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
        let mut rng = StdRng::seed_from_u64(100 + idx as u64);
        let mut ms: Vec<f64> = Vec::new();
        let mut ok = 0usize;
        let mut rows = 0usize;
        let mut err_txt = String::new();
        for i in 0..p.n {
            let sql = (p.sql)(&mut rng, &ctx, i);
            let t0 = Instant::now();
            let r = match p.kind {
                Kind::Rows => exec_stmt(&mut conn, &sql).let_rows(),
                Kind::Exec => exec_stmt(&mut conn, &sql).let_exec(),
                Kind::Block => run_block(&mut conn, &url, &sql, p.name),
            };
            let dt = t0.elapsed().as_secs_f64() * 1000.0;
            match r {
                Ok((rn, _)) => {
                    ms.push(dt);
                    ok += 1;
                    rows = rn;
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
        let note = format!("| {} | {} | {} | {} | {}/{} | {} | {:.2} | {:.2} | {:.2} | {:.2} |{}",
                           idx + 1, p.cat, p.name, p.note, ok, p.n, rows, mean, p50, p99, mx,
                           if err_txt.is_empty() { String::new() } else { format!("  ❌ {err_txt}") });
        println!("[{:02}] {} {:<22} ok={}/{} rows={} mean={:.2}ms p50={:.2} p99={:.2} max={:.2}{}",
                 idx + 1, p.cat, p.name, ok, p.n, rows, mean, p50, p99, mx,
                 if err_txt.is_empty() { String::new() } else { format!("  ERR: {err_txt}") });
        md.push_str(&note);
        md.push('\n');
    }
    // 清理写区（upd 区保留；ins/del/delb + 大插区 base+1501..33000 整区删除，恢复初始行数）
    let _ = exec_stmt(&mut conn, &format!("DELETE FROM {tb} WHERE id BETWEEN {} AND {}", ctx.base + 1501, ctx.base + 33000, tb = t()));
    let _ = exec_stmt(&mut conn, &format!("DELETE FROM {tb} WHERE id BETWEEN {} AND {}", ctx.delb_lo, ctx.delb_lo + 499, tb = t()));
    let _ = exec_stmt(&mut conn, &format!("DELETE FROM {tb} WHERE id BETWEEN {} AND {}", ctx.del_lo, ctx.del_lo + 99, tb = t()));
    std::fs::write(format!("{out}/summary.md"), md).expect("写 summary");
    println!("\n[sqlrun] 完成。summary: {out}/summary.md（环境={env_note}）");
    0
}

/// 事务块：按探针 name 分发行为 + BEGIN → 体内语句 → COMMIT。
/// - name "txn_lock_wait"：双连接锁等待（主连接持锁 sleep 后提交，副连接等锁超时 3s）
/// - name "txn_rr_readwrite"/"txn_serializable"：先 SET SESSION 对应隔离级
/// - name "txn_for_update_read"：按结果行数计；其余写按 affected 计
fn run_block(conn: &mut mysql::Conn, url: &str, body: &str, name: &'static str) -> Result<(usize, String), String> {
    if name == "txn_lock_wait" {
        return run_lock_wait(conn, url, body);
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
    let rn = if name == "txn_for_update_read" { r.rows.len() } else { r.affected as usize };
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

/// 锁等待探针：主连接 BEGIN + SELECT..FOR UPDATE 持锁 → 副连接对同 id UPDATE 等锁
/// （innodb_lock_wait_timeout=3s → 预计 1205 超时）→ 主连接 sleep 4s 后 COMMIT。
fn run_lock_wait(conn: &mut mysql::Conn, url: &str, body: &str) -> Result<(usize, String), String> {
    let tb = t();
    let _ = exec_stmt(conn, "SET SESSION innodb_lock_wait_timeout=3");
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
        let w = exec_stmt(&mut c2, &format!("UPDATE {tb} SET note='x9' WHERE id={id}", tb = t()));
        match w.err {
            Some(e) => Err(e.to_string()),
            None => Ok(()),
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
        Ok(()) => "waiter-ok(先等锁后拿到)".to_string(),
        Err(e) if e.contains("1205") => "waiter-1205锁等待超时".to_string(),
        Err(e) => format!("waiter-err({e})"),
    };
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
