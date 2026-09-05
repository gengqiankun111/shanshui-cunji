//! shanshui-cunji-baseline：Task-005 性能基准基线采集探针（2026-09-05）。
//!
//! 与既有基准体系同口径：复用引擎公开 API；多线程点查/混合写由 `shanshui-cunji-ycsb` 补充
//! （本工具输出即 JSON 单点快照，数值以本机为准，正式回填随基准轮）。采集项：
//!   1. bulk 写吞吐（put_nosync + flush_wal，非逐条 fsync）；
//!   2. 冷启动时间（Engine::open：SST 全量加载重开）；
//!   3. 点查 QPS / P50/P95/P99（warm HotCache 路径；冷读另跑 ycsb workload c）；
//!   4. 持久写 TPS / P50/P95/P99（`put`，Per-CPU 队列窗口落盘）；
//!   5. 倒排单 Term 查询 QPS / 延迟（`search_term`，含回表——真实 term 查询口径）；
//!   6. TTL 过期桶删除扫描耗时（ttl_days 库重开，open 期按天整目录清理）。
//!
//! 输出：stdout 文本行 + `benchmarks/baseline_20260904.json`（验收路径；`BASELINE_OUT` 可覆盖）。

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::json;
use shanshui_cunji::config::Config;
use shanshui_cunji::engine::Engine;

fn get(args: &[String], name: &str, def: u64) -> u64 {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(def)
}

/// 延迟分位（µs）：返回 (min, p50, p95, p99)。
fn percentiles_us(mut lat: Vec<Duration>) -> (f64, f64, f64, f64) {
    lat.sort_unstable();
    let q = |p: f64| {
        let i = ((lat.len() as f64) * p).floor() as usize;
        lat[i.min(lat.len() - 1)].as_secs_f64() * 1e6
    };
    (
        lat[0].as_secs_f64() * 1e6,
        q(0.50),
        q(0.95),
        q(0.99),
    )
}

fn doc(i: u64) -> serde_json::Value {
    serde_json::json!({
        "docid": i,
        "status": "active",
        "city": format!("c{}", i % 10),
        "name": format!("user-{i}"),
        "n": i % 1000,
    })
}

fn terms(i: u64) -> Vec<String> {
    vec![
        "status=active".to_string(),
        format!("city=c{}", i % 10),
        format!("n={}", i % 1000),
    ]
}

fn tmp_dir(tag: &str) -> PathBuf {
    let base = std::env::temp_dir().join(format!("baseline-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    base
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let records = get(&args, "--records", 100_000);
    let point_n = get(&args, "--point-n", 50_000);
    let write_n = get(&args, "--write-n", 20_000);
    let term_n = get(&args, "--term-n", 200);
    let out_path = std::env::var("BASELINE_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("benchmarks/baseline_20260904.json"));
    println!(
        "[baseline] records={records} point_n={point_n} write_n={write_n} term_n={term_n} out={}",
        out_path.display()
    );

    // ---------- 建库（含倒排 term，供点查/term/写探针共用） ----------
    let dir = tmp_dir("main");
    let mut cfg = Config::default();
    let mut engine = Engine::open(&dir, &cfg).unwrap();
    let t0 = Instant::now();
    {
        for i in 0..records {
            let d = doc(i);
            let ts = terms(i);
            let t: Vec<&str> = ts.iter().map(|s| s.as_str()).collect();
            engine
                .put_nosync(i, serde_json::to_vec(&d).unwrap(), &t)
                .unwrap();
        }
        engine.flush_wal().unwrap();
        engine.flush_inverted().unwrap();
    }
    let load_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let load_w_s = records as f64 / (load_ms / 1000.0);
    println!("[probe] bulk_write {load_w_s:.0} w/s（put_nosync+flush_wal）");

    // ---------- 1. 冷启动时间（SST 全量加载重开） ----------
    drop(engine);
    let t = Instant::now();
    let mut engine = Engine::open(&dir, &cfg).unwrap();
    let cold_ms = t.elapsed().as_secs_f64() * 1000.0;
    println!("[probe] cold_start_ms={cold_ms:.2}");

    // ---------- 2. 点查（warm HotCache 路径；冷读见 ycsb workload c） ----------
    let mut lat = Vec::with_capacity(point_n as usize);
    let t = Instant::now();
    for i in 0..point_n {
        let s = Instant::now();
        let _ = engine.get(i % records).unwrap();
        lat.push(s.elapsed());
    }
    let point_qps = point_n as f64 / t.elapsed().as_secs_f64();
    let (point_min, point_p50, point_p95, point_p99) = percentiles_us(lat);
    println!(
        "[probe] point_read qps={point_qps:.0} p50_us={point_p50:.1} p95_us={point_p95:.1} p99_us={point_p99:.1} min_us={point_min:.1}（warm-hotcache）"
    );

    // ---------- 3. 倒排单 Term 查询（search_term 含回表） ----------
    let term = "city=c3"; // 10 万库 ≈ 1 万命中回表
    let mut lat = Vec::with_capacity(term_n as usize);
    let t = Instant::now();
    for _ in 0..term_n {
        let s = Instant::now();
        let rows = engine.search_term(term).unwrap();
        assert!(!rows.is_empty());
        lat.push(s.elapsed());
    }
    let term_qps = term_n as f64 / t.elapsed().as_secs_f64();
    let (term_min, term_p50, term_p95, term_p99) = percentiles_us(lat);
    println!(
        "[probe] term_query term={term} qps={term_qps:.1} p50_us={term_p50:.0} p95_us={term_p95:.0} p99_us={term_p99:.0} min_us={term_min:.0}（含回表）"
    );

    // ---------- 4. 持久写（put：Per-CPU 队列窗口落盘 ack） ----------
    let mut lat = Vec::with_capacity(write_n as usize);
    let t = Instant::now();
    for i in 0..write_n {
        let id = records + i;
        let d = doc(id);
        let t0 = Instant::now();
        engine
            .put(id, serde_json::to_vec(&d).unwrap(), &["status=active"])
            .unwrap();
        lat.push(t0.elapsed());
    }
    engine.flush_wal().unwrap();
    let write_tps = write_n as f64 / t.elapsed().as_secs_f64();
    let (write_min, write_p50, write_p95, write_p99) = percentiles_us(lat);
    println!(
        "[probe] sync_write tps={write_tps:.0} p50_us={write_p50:.1} p95_us={write_p95:.1} p99_us={write_p99:.1} min_us={write_min:.1}（put 队列窗口落盘）"
    );
    drop(engine);

    // ---------- 5. TTL 过期桶删除扫描耗时（ttl_days 库重开 open 期清理） ----------
    let ttl_dir = tmp_dir("ttl");
    let mut ttl_cfg = Config::default();
    ttl_cfg.storage.ttl_days = Some(2); // ttl_field 默认 "timestamp"
    let past = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .saturating_sub(3 * 86_400);
    {
        let mut e = Engine::open(&ttl_dir, &ttl_cfg).unwrap();
        for i in 0..records {
            let d = json!({"docid": i, "timestamp": past, "name": format!("ttl-{i}")});
            e.put_nosync(i, serde_json::to_vec(&d).unwrap(), &[])
                .unwrap();
        }
        e.flush_wal().unwrap();
        e.flush_primary().unwrap(); // 落 SST（按 timestamp 分天桶）
    }
    let t = Instant::now();
    let e2 = Engine::open(&ttl_dir, &ttl_cfg).unwrap();
    let ttl_ms = t.elapsed().as_secs_f64() * 1000.0;
    let survived = e2.count_all_docs().unwrap();
    drop(e2);
    println!(
        "[probe] ttl_scan_ms={ttl_ms:.2} survived={survived}/{records}（全过期桶 open 期整目录清理）"
    );

    // ---------- 汇总 JSON ----------
    let cpu = std::env::var("PROCESSOR_IDENTIFIER").unwrap_or_default();
    let now_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let summary = json!({
        "name": "shanshui-cunji Task-005 baseline（2026-09-05）",
        "generated_at_epoch": now_epoch,
        "env": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "logical_cores": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
            "cpu": cpu
        },
        "tool": "shanshui-cunji-baseline（单机探针）；多线程点查/混合写另由 shanshui-cunji-ycsb 采集并入",
        "config": { "per_cpu_enabled": true, "records": records },
        "metrics": {
            "bulk_write_w_s": load_w_s.round(),
            "cold_start_ms": cold_ms,
            "point_read_warm": { "qps": point_qps.round(), "p50_us": point_p50, "p95_us": point_p95, "p99_us": point_p99 },
            "sync_write": { "tps": write_tps.round(), "p50_us": write_p50, "p95_us": write_p95, "p99_us": write_p99 },
            "term_query": { "term": term, "qps": term_qps, "p50_us": term_p50, "p95_us": term_p95, "p99_us": term_p99, "note": "含回表" },
            "ttl_scan_ms": ttl_ms,
            "ttl_survived": survived
        }
    });
    let text = serde_json::to_string_pretty(&summary).unwrap();
    if let Some(p) = out_path.parent() {
        let _ = std::fs::create_dir_all(p);
    }
    std::fs::write(&out_path, &text).unwrap();
    println!("[baseline] json -> {}", out_path.display());
}
