//! novosdb 二进制入口：子命令分发（development 步骤 1 / 5.13）。
//!
//! 子命令：
//! - `server`：启动存储服务（默认，暂未实现，见步骤 15）；
//! - `check`：校验配置与数据目录；
//! - `demo`：功能冒烟测试（构造数据/插入/查询主键/缓存/组合索引/倒排/分片/删除）并输出 HTML 报告；
//! - `version`：版本信息。

use std::path::PathBuf;

use novosdb::config::Config;
use tracing::info;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() {
    init_tracing();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut config_path = PathBuf::from("config.toml");
    // demo 子命令参数：--scale <条数>（默认 10 万） / --out <输出目录> / --gen-only
    let mut scale: u64 = 100_000;
    let mut out_dir = PathBuf::from("images");
    let mut gen_only = false;

    // 解析 `--config <path>` / `--scale <n>` / `--out <dir>` / `--gen-only`（允许出现在任意位置）
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--config" | "-c" => {
                i += 1;
                if i < args.len() {
                    config_path = PathBuf::from(&args[i]);
                }
            }
            "--scale" | "-s" => {
                i += 1;
                if i < args.len() {
                    scale = args[i].parse().unwrap_or(100_000);
                }
            }
            "--out" | "-o" => {
                i += 1;
                if i < args.len() {
                    out_dir = PathBuf::from(&args[i]);
                }
            }
            "--gen-only" => {
                gen_only = true;
            }
            _ => {}
        }
        i += 1;
    }

    let subcommand = args
        .iter()
        .find(|a| !a.starts_with('-'))
        .map(|s| s.as_str())
        .unwrap_or("server");

    match subcommand {
        "check" => run_check(&config_path),
        "demo" => run_demo(&config_path, scale, &out_dir, gen_only),
        "version" | "-V" | "--version" => {
            println!("novosdb {VERSION}");
        }
        _ => run_server(&config_path),
    }
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "novosdb=info".into()),
        )
        .init();
}

fn run_check(config_path: &PathBuf) {
    println!("novosdb {VERSION} 配置检查");
    match Config::load(config_path) {
        Ok(cfg) => {
            println!("✅ 配置校验通过");
            println!("   监听地址: {}", cfg.server.listen_addr);
            println!("   数据目录: {}", cfg.storage.data_dir);
            println!("   HotCache: {}MB ({}), BlockCache: {}MB",
                cfg.hotcache.max_memory_mb, cfg.hotcache.eviction_policy,
                cfg.blockcache.max_memory_mb);
            println!("   倒排引擎: {}, SST 压缩: {}",
                cfg.inverted.engine, cfg.sstable.compression);
        }
        Err(e) => {
            eprintln!("❌ 配置校验失败: {e}");
            std::process::exit(1);
        }
    }
}

/// 功能冒烟测试：运行 demo 并输出终端表格 + HTML 报告（输出目录由 `out_dir` 指定）。
/// `--gen-only` 时仅构造数据到 `out_dir/data.jsonl`，不执行测试。
fn run_demo(config_path: &PathBuf, scale: u64, out_dir: &PathBuf, gen_only: bool) {
    let cfg = match Config::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ 配置加载失败: {e}");
            std::process::exit(1);
        }
    };
    std::fs::create_dir_all(out_dir).expect("创建输出目录失败");

    if gen_only {
        let path = out_dir.join("data.jsonl");
        let t = std::time::Instant::now();
        match novosdb::demo::generate(scale, &path) {
            Ok(n) => {
                println!("✅ 构造数据完成：{n} 条 → {}（{:.1} ms）", path.display(), t.elapsed().as_secs_f64() * 1000.0);
                return;
            }
            Err(e) => {
                eprintln!("❌ 构造数据失败: {e}");
                std::process::exit(1);
            }
        }
    }

    // 临时数据目录（D 盘，避免撑爆 C 盘系统盘）
    let data_dir = std::path::PathBuf::from("D:\\novosdb-tmp").join(format!("demo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("创建临时数据目录失败");

    println!("\n═══ novosdb {VERSION} 功能冒烟测试（scale={scale}）═══\n");
    let results = match novosdb::demo::run(&data_dir, &cfg, scale) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("❌ demo 运行失败: {e}");
            std::process::exit(1);
        }
    };

    // 终端表格
    let passed_total = results.iter().filter(|r| r.passed).count();
    println!("{:<16} {:<4} {:>10}  {}", "功能", "结果", "耗时(ms)", "说明");
    println!("{}", "-".repeat(100));
    for r in &results {
        let mark = if r.passed { "✅" } else { "❌" };
        println!("{:<16} {:<4} {:>10.2}  {}", r.name, mark, r.elapsed_ms, r.detail);
    }
    println!("{}", "-".repeat(100));
    println!("总计：{passed_total}/{} 通过", results.len());

    // HTML 报告（按功能归类，供截图）
    let html = build_html_report(&results, scale);
    let report_path = out_dir.join("report.html");
    std::fs::write(&report_path, html).expect("写 HTML 报告失败");
    println!("\n📄 HTML 报告已生成: {}", report_path.display());
}

/// 生成按功能归类的 HTML 报告（每个功能一个独立 section，供逐块截图）。
fn build_html_report(results: &[novosdb::demo::TestResult], scale: u64) -> String {
    let mut sections = String::new();
    // 固定 slug（按功能顺序），与截图脚本一一对应
    const SLUGS: [&str; 9] = [
        "01-data", "02-insert", "03-query-primary", "04-query-cache",
        "05-query-composite", "06-query-inverted", "07-sharding", "08-delete", "09-optimizer",
    ];
    for (i, r) in results.iter().enumerate() {
        let cls = if r.passed { "pass" } else { "fail" };
        let badge = if r.passed { "通过" } else { "失败" };
        let slug = SLUGS.get(i).copied().unwrap_or("other");
        sections.push_str(&format!(
            r#"<section class="card {cls}" id="{slug}">
                <div class="head">
                    <span class="idx">{:02}</span>
                    <h2>{}</h2>
                    <span class="badge {cls}">{}</span>
                </div>
                <p class="detail">{}</p>
                <div class="meta">耗时 <b>{:.2}</b> ms</div>
            </section>"#,
            i + 1, r.name, badge, r.detail, r.elapsed_ms
        ));
    }
    let passed = results.iter().filter(|r| r.passed).count();
    let total = results.len();
    let bar_pct = (passed as f64 / total as f64) * 100.0;
    format!(
        r#"<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"><title>novosdb v{VERSION} 功能测试报告</title>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family:'Segoe UI','Microsoft YaHei',sans-serif; background:#0f172a; color:#e2e8f0; padding:32px; }}
  .wrap {{ max-width:860px; margin:0 auto; }}
  h1 {{ font-size:22px; margin-bottom:4px; color:#f8fafc; }}
  .sub {{ color:#94a3b8; font-size:13px; margin-bottom:24px; }}
  .summary {{ background:#1e293b; border-radius:12px; padding:20px 24px; margin-bottom:28px; }}
  .summary .big {{ font-size:28px; font-weight:700; color:#4ade80; }}
  .bar {{ height:8px; background:#334155; border-radius:4px; margin-top:12px; overflow:hidden; }}
  .bar i {{ display:block; height:100%; background:linear-gradient(90deg,#34d399,#22d3ee); width:{bar_pct}%; }}
  .card {{ background:#1e293b; border-radius:12px; padding:18px 22px; margin-bottom:16px;
           border-left:4px solid #475569; box-shadow:0 2px 8px rgba(0,0,0,.25); }}
  .card.pass {{ border-left-color:#34d399; }}
  .card.fail {{ border-left-color:#f87171; }}
  .head {{ display:flex; align-items:center; gap:12px; }}
  .idx {{ font-size:12px; color:#64748b; }}
  .head h2 {{ font-size:16px; flex:1; }}
  .badge {{ font-size:12px; padding:2px 10px; border-radius:999px; }}
  .badge.pass {{ background:rgba(52,211,153,.15); color:#4ade80; }}
  .badge.fail {{ background:rgba(248,113,113,.15); color:#f87171; }}
  .detail {{ color:#94a3b8; font-size:13px; margin-top:10px; line-height:1.6; }}
  .meta {{ margin-top:10px; font-size:12px; color:#64748b; }}
  .meta b {{ color:#e2e8f0; }}
</style></head><body><div class="wrap">
  <h1>novosdb v{VERSION} 功能冒烟测试报告</h1>
   <div class="sub">LSM-Tree 单机内核 · 2026-08-27 · 数据量 {scale} 条 · 按功能归类</div>
   <div class="summary">
     <span class="big">{passed}/{total}</span> 项通过
     <div class="bar"><i></i></div>
   </div>
   {sections}
 </div></body></html>"#,
        VERSION = VERSION,
        scale = scale,
        passed = passed,
        total = total,
        bar_pct = bar_pct,
        sections = sections
    )
}

fn run_server(config_path: &PathBuf) {
    // 阶段 1 步骤 15 实现 HTTP/TCP 服务；当前仅加载配置并提示
    match Config::load(config_path) {
        Ok(cfg) => {
            info!(
                "novosdb {} 启动（服务层将在阶段 1 步骤 15 实现）: listen={}, data_dir={}",
                VERSION,
                cfg.server.listen_addr,
                cfg.storage.data_dir
            );
        }
        Err(e) => {
            eprintln!("❌ 配置加载失败: {e}");
            std::process::exit(1);
        }
    }
}
