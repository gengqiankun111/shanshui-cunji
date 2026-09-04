//! wide_load：用 rust mysql crate 向 SCC（或 MySQL）批量装载宽表数据。
//!
//! 用途：pymysql 与 SCC 握手不兼容（CONNECT_ATTRS 解析越界），python 版
//! tmp_wide_load.py 无法给 SCC 装载；本模块用与脚本相同的确定性参数重建数据集：
//!   4 个"进程"（线程）各自 StdRng::seed(42 + w*7919)、id 步进 procs、批 500 行/语句
//! 用法：rr-conformance --wide-load --table documents --url mysql://root@127.0.0.1:3317 --rows 1098342 --procs 4
//! 注意：SCC 位图倒排（Roaring 32 位）只兼容默认表 documents；非默认表 docid 高位
//!       （tid<<48）会触发 inverted.rs 断言——宽表装载请用 documents。

use std::sync::OnceLock;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::tx::exec_stmt;

/// 目标表名（--table，默认 t；SCC 用 documents）。
static TAB: OnceLock<String> = OnceLock::new();
fn t() -> &'static str {
    TAB.get().map(|s| s.as_str()).unwrap_or("t")
}

const ALPHA: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
const STATUSES: [&str; 5] = ["active", "closed", "pending", "failed", "archived"];
const REGIONS: [&str; 8] = ["beijing", "shanghai", "shenzhen", "hangzhou", "guangzhou",
                            "chengdu", "wuhan", "nanjing"];
const CHANNELS: [&str; 5] = ["web", "app", "api", "mobile", "wechat"];
const TAGS: [&str; 5] = ["free", "basic", "gold", "vip", "new"];

const COLS_FULL: &str = "id,k,amount,score,ts,status,region,channel,user_id,age,active_days,\
     visit_count,balance,flag,tag,note,title,url,email,phone,ip,desc_a,desc_b,txt_a,txt_b";

fn rs(rng: &mut StdRng, n: usize) -> String {
    (0..n).map(|_| ALPHA[rng.gen_range(0..ALPHA.len())] as char).collect()
}

/// 与 tmp_wide_load.py gen() 相同的行生成（值分布一致即可，不要求逐位相同）。
fn gen(rng: &mut StdRng, i: u64) -> String {
    let f2 = |x: f64| format!("{:.2}", (x * 100.0).round() / 100.0);
    let q = |s: String| format!("'{s}'");
    let vals: Vec<String> = vec![
        i.to_string(),
        rng.gen_range(1..=20_000_000u32).to_string(),
        f2(rng.gen_range(1.0f64..1_000_000.0)),
        f2(rng.gen_range(0.0f64..100.0)),
        (1_700_000_000u64 + rng.gen_range(0..=30_000_000u64)).to_string(),
        q(STATUSES[rng.gen_range(0..STATUSES.len())].to_string()),
        q(REGIONS[rng.gen_range(0..REGIONS.len())].to_string()),
        q(CHANNELS[rng.gen_range(0..CHANNELS.len())].to_string()),
        rng.gen_range(1..=5_000_000u32).to_string(),
        rng.gen_range(18..=70u8).to_string(),
        rng.gen_range(0..=365u16).to_string(),
        rng.gen_range(0..=100_000u32).to_string(),
        f2(rng.gen_range(0.0f64..1_000_000.0)),
        rng.gen_range(0..=1u8).to_string(),
        q(TAGS[rng.gen_range(0..TAGS.len())].to_string()),
        q(rs(rng, 35)), q(rs(rng, 50)), q(rs(rng, 80)), q(rs(rng, 28)),
        q(rs(rng, 11)), q(rs(rng, 20)), q(rs(rng, 200)), q(rs(rng, 160)),
        q(rs(rng, 140)), q(rs(rng, 120)),
    ];
    format!("({})", vals.join(","))
}

pub fn run(url: &str, rows: u64, procs: usize, table: &str) -> i32 {
    let _ = TAB.set(table.to_string());
    println!("[wide_load] url={url} 表={} rows={rows} procs={procs}", t());
    let start = std::time::Instant::now();
    let handles: Vec<_> = (0..procs)
        .map(|w| {
            let url = url.to_string();
            std::thread::spawn(move || worker(w, procs, rows, &url))
        })
        .collect();
    let mut total = 0u64;
    for h in handles {
        total += h.join().unwrap_or(0);
    }
    println!("[wide_load] 完成 total={total} · {:.1}s", start.elapsed().as_secs_f64());
    0
}

fn worker(w: usize, procs: usize, rows: u64, url: &str) -> u64 {
    let tb = t();
    let mut conn = match mysql::Conn::new(url) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[wide_load w{w}] 连接失败: {e}");
            return 0;
        }
    };
    let mut rng = StdRng::seed_from_u64(42 + (w as u64) * 7919);
    let batch = 500u64;
    let mut buf: Vec<String> = Vec::with_capacity(batch as usize);
    let mut done = 0u64;
    let mut i = 1 + w as u64; // python: i = start(1) + w，步进 procs
    while i <= rows {
        buf.push(gen(&mut rng, i));
        if buf.len() as u64 >= batch {
            let sql = format!("INSERT INTO {tb} ({COLS_FULL}) VALUES {}", buf.join(","));
            let r = exec_stmt(&mut conn, &sql);
            if r.err.is_some() {
                eprintln!("[wide_load w{w}] INSERT 失败 @i={i}: {}", r.err.unwrap());
                return done;
            }
            done += buf.len() as u64;
            buf.clear();
            if done % 500_000 < 500 {
                println!("  [w{w}] {done} rows");
            }
        }
        i += procs as u64;
    }
    if !buf.is_empty() {
        let sql = format!("INSERT INTO {tb} ({COLS_FULL}) VALUES {}", buf.join(","));
        let r = exec_stmt(&mut conn, &sql);
        if r.err.is_some() {
            eprintln!("[wide_load w{w}] INSERT 收尾失败: {}", r.err.unwrap());
            return done;
        }
        done += buf.len() as u64;
    }
    println!("  [w{w}] done {done} rows");
    done
}
