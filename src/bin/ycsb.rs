//! shanshui-cunji-ycsb：YCSB 标准负载压测（design 22.2 / M7-3）。
//!
//! 用法：`shanshui-cunji-ycsb --workload a --records 100000 --ops 50000 --threads 4`
//!
//! 工作负载（对齐 YCSB 规范）：
//!   a = Update Heavy（50% 读 / 50% 更新）
//!   b = Read Mostly（95% 读 / 5% 更新）
//!   c = Read Only（100% 读）
//!   f = Read Modify Write（50% 读 / 50% 读改写）
//!
//! 度量：load 吞吐（w/s）、混合吞吐（ops/s）、读/写延迟分位数（P50/P95/P99/P999）。
//! 读默认走冷缓存（load 后重开引擎），`--warm` 保留 HotCache 观察缓存命中路径。
//! `--no-fsync` 用 put_nosync 观察无 fsync 上限（对照组）。

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use shanshui_cunji::config::Config;
use shanshui_cunji::engine::Engine;

/// splitmix64 伪随机（不引入 rand 依赖，确定性可复现）。
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let get = |name: &str, def: u64| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(def)
    };
    let records = get("--records", 100_000);
    let ops = get("--ops", 50_000);
    let threads = get("--threads", 4).max(1) as usize;
    let workload = args
        .iter()
        .position(|a| a == "--workload")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.to_lowercase())
        .unwrap_or_else(|| "a".into());
    let warm = args.iter().any(|a| a == "--warm");
    let no_fsync = args.iter().any(|a| a == "--no-fsync");
    let dir = args
        .iter()
        .position(|a| a == "--dir")
        .and_then(|i| args.get(i + 1))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join(format!("ycsb-{}", std::process::id())));
    if dir.exists() {
        let _ = std::fs::remove_dir_all(&dir);
    }
    println!(
        "[ycsb] workload={workload} records={records} ops/thread={ops} threads={threads} \
         warm={warm} fsync={} dir={}",
        !no_fsync,
        dir.display()
    );

    let cfg = Config::default();
    let mut engine = Engine::open(&dir, &cfg).unwrap();

    // ---------- load 阶段 ----------
    let t0 = Instant::now();
    {
        for i in 0..records {
            let doc = make_doc(i);
            let terms = shanshui_cunji::server::extract_terms(&doc);
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            engine
                .put_nosync(i, serde_json::to_vec(&doc).unwrap(), &t)
                .unwrap();
        }
        engine.flush_wal().unwrap();
    }
    let load_elapsed = t0.elapsed();
    println!(
        "[load] records={records} elapsed={:.3}s write_throughput={:.0} w/s",
        load_elapsed.as_secs_f64(),
        records as f64 / load_elapsed.as_secs_f64()
    );

    // 冷读：重开引擎（HotCache 清空，读走 LSM 真实路径）
    if !warm {
        drop(engine);
        engine = Engine::open(&dir, &cfg).unwrap();
    }

    // ---------- 测量阶段（多线程共享引擎） ----------
    let engine = Arc::new(Mutex::new(engine));
    let lat: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::new()));
    let t_begin = Instant::now();
    let mut handles = Vec::new();
    for t in 0..threads {
        let engine = Arc::clone(&engine);
        let lat = Arc::clone(&lat);
        let wl = workload.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = Rng::new(1000 + t as u64);
            for _ in 0..ops {
                let op_start = Instant::now();
                run_op(&engine, &wl, records, &mut rng, no_fsync);
                lat.lock().unwrap().push(op_start.elapsed());
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let total_ops = threads as u64 * ops;

    // 汇总：吞吐 + 延迟分位数
    let run_elapsed = t_begin.elapsed();
    let mut all: Vec<Duration> = lat.lock().unwrap().clone();
    all.sort_unstable();
    let q = |p: f64| {
        let idx = ((all.len() as f64) * p).floor() as usize;
        all[idx.min(all.len() - 1)].as_secs_f64() * 1e6
    };
    println!(
        "[run] total_ops={total_ops} elapsed={:.3}s throughput={:.0} ops/s",
        run_elapsed.as_secs_f64(),
        total_ops as f64 / run_elapsed.as_secs_f64()
    );
    println!(
        "[run] latency_us p50={:.1} p95={:.1} p99={:.1} p999={:.1}",
        q(0.50),
        q(0.95),
        q(0.99),
        q(0.999)
    );
    let _ = engine;
}

/// 生成 YCSB 风格记录：`{"docid","name","status","city","tags","n"}` + 倒排词条派生。
fn make_doc(i: u64) -> Value {
    let status = if i % 3 == 0 { "inactive" } else { "active" };
    let city = ["beijing", "shanghai", "shenzhen", "hangzhou"][(i % 4) as usize];
    json!({
        "docid": i,
        "name": format!("user-{i}"),
        "status": status,
        "city": city,
        "tags": ["a", "b", "c"],
        "n": i % 1000,
    })
}

/// 执行一次操作（按工作负载混合比）。返回操作类型供延迟统计（当前合并统计）。
fn run_op(
    engine: &Arc<Mutex<Engine>>,
    workload: &str,
    records: u64,
    rng: &mut Rng,
    no_fsync: bool,
) {
    let mut e = engine.lock().unwrap();
    match workload {
        "c" => {
            read(&mut e, rng.below(records));
        }
        "b" => {
            if rng.below(100) < 95 {
                read(&mut e, rng.below(records));
            } else {
                update(&mut e, rng.below(records), no_fsync);
            }
        }
        "f" => {
            if rng.below(100) < 50 {
                read(&mut e, rng.below(records));
            } else {
                // 读改写：先读后写
                let id = rng.below(records);
                read(&mut e, id);
                update(&mut e, id, no_fsync);
            }
        }
        _ => {
            // a = 50/50 读/更新
            if rng.below(100) < 50 {
                read(&mut e, rng.below(records));
            } else {
                update(&mut e, rng.below(records), no_fsync);
            }
        }
    }
}

fn read(e: &mut Engine, id: u64) {
    let _ = e.get(id).unwrap();
}

fn update(e: &mut Engine, id: u64, no_fsync: bool) {
    let doc = make_doc(id + 1_000_000); // 内容变化模拟更新
    let terms = shanshui_cunji::server::extract_terms(&doc);
    let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
    if no_fsync {
        e.put_nosync(id, serde_json::to_vec(&doc).unwrap(), &t)
            .unwrap();
    } else {
        e.put(id, serde_json::to_vec(&doc).unwrap(), &t).unwrap();
    }
}
