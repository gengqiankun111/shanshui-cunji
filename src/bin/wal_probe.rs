//! shanshui-cunji-wal-probe：per-CPU WAL checkpoint / 重启回放探针（P144-② 验证）。
//!
//! 目的：量化 composite（cidx）库在各阶段的 checkpoint 推进与"下次重启回放量"，两端
//! （Windows / Ubuntu）同输出格式可直接 diff。模式：
//!   open       打开引擎（计时）→ 打印 WalReplayReport + count_all_docs + 抽样点查 → 正常关闭
//!   resetcp    把 `percpu-wal/checkpoint.json` 覆写为 0（模拟修复前旧库：cp 从未随刷盘持久化）
//!   writetail  追加 N 行（含 composite 字段）→ 等一个批窗口让队列落盘 → process::exit 模拟崩溃
//!              （不跑 Drop/shutdown，等价强制 kill：cp 不落盘、memtable 不刷）
//! 脚本编排：import → open#1（正常重启：回放应≈0）→ resetcp → open#2（旧库态首启：全量回放一次
//! 收敛）→ writetail N → open#3（崩溃后重启：仅回放尾部 N 行级，不重复全量）。
//!
//! 用法：
//!   shanshui-cunji-wal-probe --config cfg.toml [--dir db] --mode open [--probe-id 500000]
//!   shanshui-cunji-wal-probe --config cfg.toml [--dir db] --mode resetcp
//!   shanshui-cunji-wal-probe --config cfg.toml [--dir db] --mode writetail --n 2000 [--base 500001]
//!   输出前缀 [probe]，形如 label=k1=v1 k2=v2 ...（供 grep/diff）

use std::path::PathBuf;
use std::time::Instant;

use shanshui_cunji::config::Config;
use shanshui_cunji::engine::Engine;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut config_path = PathBuf::from("config.toml");
    let mut dir_override: Option<PathBuf> = None;
    let mut mode = String::new();
    let mut n: u64 = 2000;
    let mut base: u64 = 500_001;
    let mut probe_id: u64 = 500_000;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                i += 1;
                if i < args.len() {
                    config_path = PathBuf::from(&args[i]);
                }
            }
            "--dir" => {
                i += 1;
                if i < args.len() {
                    dir_override = Some(PathBuf::from(&args[i]));
                }
            }
            "--mode" => {
                i += 1;
                if i < args.len() {
                    mode = args[i].clone();
                }
            }
            "--n" => {
                i += 1;
                if i < args.len() {
                    n = args[i].parse().unwrap_or(n);
                }
            }
            "--base" => {
                i += 1;
                if i < args.len() {
                    base = args[i].parse().unwrap_or(base);
                }
            }
            "--probe-id" => {
                i += 1;
                if i < args.len() {
                    probe_id = args[i].parse().unwrap_or(probe_id);
                }
            }
            _ => {}
        }
        i += 1;
    }
    let mut cfg = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[probe] ❌ 配置加载失败 {config_path:?}: {e}");
            std::process::exit(2);
        }
    };
    if let Some(d) = &dir_override {
        cfg.storage.data_dir = d.display().to_string();
    }
    let dir = PathBuf::from(&cfg.storage.data_dir);
    match mode.as_str() {
        "open" => open_and_report(&dir, &cfg, probe_id),
        "resetcp" => reset_cp(&dir),
        "writetail" => write_tail_crash(&dir, &cfg, base, n),
        other => {
            eprintln!("[probe] ❌ 未知 mode: {other}（open|resetcp|writetail）");
            std::process::exit(2);
        }
    }
}

fn open_and_report(dir: &std::path::Path, cfg: &Config, probe_id: u64) {
    let t0 = Instant::now();
    let e = match Engine::open(dir, cfg) {
        Ok(e) => e,
        Err(err) => {
            eprintln!("[probe] ❌ open 失败 {dir:?}: {err}");
            std::process::exit(2);
        }
    };
    let t_open_ms = t0.elapsed().as_millis();
    let r = e.wal_replay_report();
    let count = e.count_all_docs().unwrap_or(u64::MAX);
    let g1 = e.get(1).unwrap_or(None).is_some();
    let g_last = e.get(probe_id).unwrap_or(None).is_some();
    println!(
        "[probe] mode=open t_open_ms={t_open_ms} per_cpu={} persisted_cp={} cp={} \
         cidx_wm={} cidx_ssts={} replay_pending={} count={count} g1={g1} g_last={g_last}",
        r.per_cpu,
        r.persisted_cp,
        r.cp,
        r.cidx_watermark,
        r.cidx_ssts,
        r.replay_pending,
    );
    // 正常关闭（Drop → shutdown 排空 + 持久化；cp 不因关闭额外推进——flush_all 不刷 memtable）
}

fn reset_cp(dir: &std::path::Path) {
    let cp_path = dir.join("percpu-wal").join("checkpoint.json");
    let old = std::fs::read_to_string(&cp_path).unwrap_or_default();
    std::fs::write(&cp_path, "0").expect("写 checkpoint=0 失败（模拟旧库 cp 恒 0）");
    println!(
        "[probe] mode=resetcp path={} old={} new=0",
        cp_path.display(),
        old.trim()
    );
}

fn write_tail_crash(dir: &std::path::Path, cfg: &Config, base: u64, n: u64) {
    let mut e = match Engine::open(dir, cfg) {
        Ok(e) => e,
        Err(err) => {
            eprintln!("[probe] ❌ open 失败 {dir:?}: {err}");
            std::process::exit(2);
        }
    };
    for d in base..base + n {
        let doc = serde_json::json!({
            "k": d * 2,
            "amount": d as f64,
            "score": 1.0,
            "ts": 1_700_000_000u64 + (d % 1_000_000),
            "status": "active",
            "region": "beijing",
            "note": format!("tail-{d}"),
        });
        if let Err(err) = e.put(d, serde_json::to_vec(&doc).unwrap(), &[]) {
            eprintln!("[probe] ❌ writetail put {d} 失败: {err}");
            std::process::exit(2);
        }
    }
    println!("[probe] mode=writetail rows={n} base={base}（等批窗口落盘后崩溃退出）");
    // 等一个 per_cpu 批窗口（cfg 100ms）+ 余量，确保尾部已写盘可被回放（崩溃不丢已 fsync 尾）
    std::thread::sleep(std::time::Duration::from_millis(250));
    // 模拟强制 kill：绕过 Drop（不 shutdown / 不 flush_all / 不刷 memtable），cp 停留上次持久化值
    std::process::exit(0);
}
