//! shanshui-cunji 二进制入口：子命令分发（development 步骤 1 / 5.13）。
//!
//! 子命令：
//! - `server`：启动 HTTP-JSON 服务（默认，步骤 15）；
//! - `put / get / search / range / delete / patch`：数据操作（本地引擎直连，与 HTTP 共享内核路径）；
//! - `count / groupby`：倒排统计聚合（COUNT / GROUP BY，阶段 1.5 M4）；
//! - `backup / restore`：备份还原（步骤 14）；
//! - `check`：校验配置与数据目录；
//! - `demo`：功能冒烟测试（构造数据/插入/查询主键/缓存/组合索引/倒排/分片/删除/备份还原）并输出 HTML 报告；
//! - `version`：版本信息。

use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use shanshui_cunji::config::Config;
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
    // 数据操作子命令参数：--id / --data / --filter / --start / --end / --field / --value
    let mut id: u64 = 0;
    let mut data = String::new();
    let mut filter = String::new();
    let mut start: Option<u64> = None;
    let mut end: Option<u64> = None;
    let mut field = String::new();
    let mut value = String::new();

    // 解析 `--config <path>` 与各子命令参数（允许出现在任意位置）
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
            "--id" => {
                i += 1;
                if i < args.len() {
                    id = args[i].parse().unwrap_or(0);
                }
            }
            "--data" | "-d" => {
                i += 1;
                if i < args.len() {
                    data = args[i].clone();
                }
            }
            "--filter" | "-f" => {
                i += 1;
                if i < args.len() {
                    filter = args[i].clone();
                }
            }
            "--start" => {
                i += 1;
                if i < args.len() {
                    start = args[i].parse().ok();
                }
            }
            "--end" => {
                i += 1;
                if i < args.len() {
                    end = args[i].parse().ok();
                }
            }
            "--field" => {
                i += 1;
                if i < args.len() {
                    field = args[i].clone();
                }
            }
            "--value" => {
                i += 1;
                if i < args.len() {
                    value = args[i].clone();
                }
            }
            _ => {}
        }
        i += 1;
    }

    // 位置参数：第一个为子命令，第二个为备份文件路径（backup/restore 使用）
    let positionals: Vec<&String> = args.iter().filter(|a| !a.starts_with('-')).collect();
    let subcommand = positionals.first().map(|s| s.as_str()).unwrap_or("server");
    let backup_file = positionals
        .get(1)
        .map(|s| PathBuf::from(s.as_str()))
        .unwrap_or_else(|| PathBuf::from("shanshui-cunji.bak"));

    match subcommand {
        "check" => run_check(&config_path),
        "demo" => run_demo(&config_path, scale, &out_dir, gen_only),
        "backup" => run_backup(&config_path, &backup_file),
        "restore" => run_restore(&config_path, &backup_file),
        "put" => run_cli_put(&config_path, id, &data),
        "get" => run_cli_get(&config_path, id),
        "patch" => run_cli_patch(&config_path, id, &data),
        "search" => run_cli_search(&config_path, &filter),
        "range" => run_cli_range(&config_path, start, end),
        "count" => run_cli_count(&config_path, &field, &value),
        "groupby" => run_cli_group_by(&config_path, &field),
        "admin" => run_cli_admin(&config_path),
        "explain" => run_cli_explain(&config_path, &filter),
        "delete" => run_cli_delete(&config_path, id),
        "version" | "-V" | "--version" => {
            println!("shanshui-cunji {VERSION}");
        }
        _ => run_server(&config_path),
    }
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "shanshui_cunji=info".into()),
        )
        .init();
}

fn run_check(config_path: &Path) {
    println!("shanshui-cunji {VERSION} 配置检查");
    match Config::load(config_path) {
        Ok(cfg) => {
            println!("✅ 配置校验通过");
            println!("   监听地址: {}", cfg.server.listen_addr);
            println!("   数据目录: {}", cfg.storage.data_dir);
            println!(
                "   HotCache: {}MB ({}), BlockCache: {}MB",
                cfg.hotcache.max_memory_mb,
                cfg.hotcache.eviction_policy,
                cfg.blockcache.max_memory_mb
            );
            println!(
                "   倒排引擎: {}, SST 压缩: {}",
                cfg.inverted.engine, cfg.sstable.compression
            );
        }
        Err(e) => {
            eprintln!("❌ 配置校验失败: {e}");
            std::process::exit(1);
        }
    }
}

/// 备份：打开引擎做一致性准备（刷 WAL + MemTable + 倒排）→ 打包数据目录为单个备份文件。
fn run_backup(config_path: &Path, backup_file: &Path) {
    let cfg = match Config::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ 配置加载失败: {e}");
            std::process::exit(1);
        }
    };
    let data_dir = PathBuf::from(&cfg.storage.data_dir);

    // 打开引擎执行 prepare_backup：保证 WAL/MemTable/倒排内存全部落盘，磁盘态自包含
    let mut engine = match shanshui_cunji::engine::Engine::open(&data_dir, &cfg) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ 打开引擎失败（备份前一致性准备需要）: {e}");
            std::process::exit(1);
        }
    };
    if let Err(e) = engine.prepare_backup() {
        eprintln!("❌ 备份前一致性准备失败: {e}");
        std::process::exit(1);
    }
    drop(engine);

    match shanshui_cunji::storage::backup(&data_dir, backup_file) {
        Ok(rep) => {
            println!("✅ 备份完成: {}", backup_file.display());
            println!(
                "   {} 个文件，{} 字节（{:.0} ms）",
                rep.entry_count, rep.total_bytes, rep.elapsed_ms
            );
        }
        Err(e) => {
            eprintln!("❌ 备份失败: {e}");
            std::process::exit(1);
        }
    }
}

/// 还原：停止服务后执行——清空数据目录 → 校验魔数/版本/CRC → 解压全部文件。
fn run_restore(config_path: &Path, backup_file: &Path) {
    let cfg = match Config::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ 配置加载失败: {e}");
            std::process::exit(1);
        }
    };
    let data_dir = PathBuf::from(&cfg.storage.data_dir);
    match shanshui_cunji::storage::restore(backup_file, &data_dir) {
        Ok(rep) => {
            println!(
                "✅ 还原完成: {} 个文件，{} 字节（{:.0} ms）",
                rep.entry_count, rep.total_bytes, rep.elapsed_ms
            );
            println!(
                "   数据目录: {}（重启 server 即可加载还原的数据）",
                data_dir.display()
            );
        }
        Err(e) => {
            eprintln!("❌ 还原失败: {e}");
            std::process::exit(1);
        }
    }
}

/// 功能冒烟测试：运行 demo 并输出终端表格 + HTML 报告（输出目录由 `out_dir` 指定）。
/// `--gen-only` 时仅构造数据到 `out_dir/data.jsonl`，不执行测试。
fn run_demo(config_path: &Path, scale: u64, out_dir: &Path, gen_only: bool) {
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
        match shanshui_cunji::demo::generate(scale, &path) {
            Ok(n) => {
                println!(
                    "✅ 构造数据完成：{n} 条 → {}（{:.1} ms）",
                    path.display(),
                    t.elapsed().as_secs_f64() * 1000.0
                );
                return;
            }
            Err(e) => {
                eprintln!("❌ 构造数据失败: {e}");
                std::process::exit(1);
            }
        }
    }

    // 临时数据目录：默认系统临时目录；可用 SHANSHUI_CUNJI_TMP 覆盖
    // （Windows 上 C 盘空间紧张时可指向 D 盘，如 D:\shanshui-cunji-tmp）
    let data_dir = std::env::var("SHANSHUI_CUNJI_TMP")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::temp_dir().join(format!("shanshui-cunji-demo-{}", std::process::id()))
        });
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("创建临时数据目录失败");

    println!("\n═══ shanshui-cunji {VERSION} 功能冒烟测试（scale={scale}）═══\n");
    let results = match shanshui_cunji::demo::run(&data_dir, &cfg, scale) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("❌ demo 运行失败: {e}");
            std::process::exit(1);
        }
    };

    // 终端表格
    let passed_total = results.iter().filter(|r| r.passed).count();
    println!("{:<16} {:<4} {:>10}  说明", "功能", "结果", "耗时(ms)");
    println!("{}", "-".repeat(100));
    for r in &results {
        let mark = if r.passed { "✅" } else { "❌" };
        println!(
            "{:<16} {:<4} {:>10.2}  {}",
            r.name, mark, r.elapsed_ms, r.detail
        );
    }
    println!("{}", "-".repeat(100));
    println!("总计：{passed_total}/{} 通过", results.len());

    // HTML 报告（按功能归类，供截图）
    let html = build_html_report(&results, scale);
    std::fs::create_dir_all(out_dir).expect("创建报告目录失败");
    let report_path = out_dir.join("report.html");
    std::fs::write(&report_path, html).expect("写 HTML 报告失败");
    println!("\n📄 HTML 报告已生成: {}", report_path.display());
}

/// 生成按功能归类的 HTML 报告（每个功能一个独立 section，供逐块截图）。
fn build_html_report(results: &[shanshui_cunji::demo::TestResult], scale: u64) -> String {
    let mut sections = String::new();
    // 固定 slug（按功能顺序），与截图脚本一一对应
    const SLUGS: [&str; 10] = [
        "01-data",
        "02-insert",
        "03-query-primary",
        "04-query-cache",
        "05-query-composite",
        "06-query-inverted",
        "07-sharding",
        "08-delete",
        "09-optimizer",
        "10-backup",
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
            i + 1,
            r.name,
            badge,
            r.detail,
            r.elapsed_ms
        ));
    }
    let passed = results.iter().filter(|r| r.passed).count();
    let total = results.len();
    let bar_pct = (passed as f64 / total as f64) * 100.0;
    format!(
        r#"<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"><title>山水存迹数据库（shanshui-cunji）v{VERSION} 功能测试报告</title>
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
  <h1>山水存迹数据库（shanshui-cunji）v{VERSION} 功能冒烟测试报告</h1>
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

// ---------------------------------------------------------------------------
// CLI 数据操作（与 HTTP 共享同一内核调用路径；本地引擎直连，勿与 server 同目录并发）
// ---------------------------------------------------------------------------

fn load_config(config_path: &Path) -> Config {
    match Config::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ 配置加载失败: {e}");
            std::process::exit(1);
        }
    }
}

/// 打开本地引擎（数据目录取自配置）。
fn open_engine(config_path: &Path) -> shanshui_cunji::engine::Engine {
    let cfg = load_config(config_path);
    let data_dir = PathBuf::from(&cfg.storage.data_dir);
    match shanshui_cunji::engine::Engine::open(&data_dir, &cfg) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ 打开引擎失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `put --id 1001 --data '{"status":"active","type":"order"}'`
fn run_cli_put(config_path: &Path, id: u64, data: &str) {
    if id == 0 {
        eprintln!("❌ put 需要 --id <docid>（>0）");
        std::process::exit(1);
    }
    if id >= u32::MAX as u64 {
        eprintln!("❌ docid 超出倒排索引支持范围（< 2^32）");
        std::process::exit(1);
    }
    let mut val: Value = match serde_json::from_str(data) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("❌ --data 不是合法 JSON: {e}");
            std::process::exit(1);
        }
    };
    if !val.is_object() {
        eprintln!("❌ --data 必须是 JSON 对象（如 '{{\"status\":\"active\"}}'）");
        std::process::exit(1);
    }
    // 与 HTTP /put 同构：文档对象含 docid，字符串字段值自动建倒排词条
    if let Some(obj) = val.as_object_mut() {
        obj.insert("docid".into(), json!(id));
    }
    let terms = shanshui_cunji::server::extract_terms(&val);
    let bytes = val.to_string().into_bytes();
    let term_refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
    let mut engine = open_engine(config_path);
    match engine.put(id, bytes, &term_refs) {
        Ok(()) => {
            // 倒排词条刷盘落盘：CLI 为独立进程，不刷盘则后续进程查不到（长驻 server 无需）
            if engine.inverted_mem_docids() > 0 {
                if let Err(e) = engine.flush_inverted() {
                    eprintln!("❌ 倒排刷盘失败: {e}");
                    std::process::exit(1);
                }
            }
            println!("✅ 已写入 docid={id}（倒排词条 {} 个）", terms.len());
        }
        Err(e) => {
            eprintln!("❌ 写入失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `patch --id 1001 --data '{"status":"inactive","note":null}'`（null = 删除字段，阶段 1.5 Delta CF）
fn run_cli_patch(config_path: &Path, id: u64, data: &str) {
    if id == 0 {
        eprintln!("❌ patch 需要 --id <docid>（>0）");
        std::process::exit(1);
    }
    let val: Value = match serde_json::from_str(data) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("❌ --data 不是合法 JSON: {e}");
            std::process::exit(1);
        }
    };
    let Some(obj) = val.as_object() else {
        eprintln!("❌ --data 必须是 JSON 对象（如 '{{\"status\":\"inactive\"}}'）");
        std::process::exit(1);
    };
    let fields: Vec<(&str, serde_json::Value)> =
        obj.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
    let mut engine = open_engine(config_path);
    match engine.patch(id, &fields) {
        Ok(()) => println!("✅ 已更新 docid={id}（字段 {} 个）", fields.len()),
        Err(e) => {
            eprintln!("❌ 更新失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `get --id 1001`
fn run_cli_get(config_path: &Path, id: u64) {
    if id == 0 {
        eprintln!("❌ get 需要 --id <docid>（>0）");
        std::process::exit(1);
    }
    let mut engine = open_engine(config_path);
    match engine.get(id) {
        Ok(Some(v)) => println!("{}", String::from_utf8_lossy(&v)),
        Ok(None) => println!("（未找到 docid={id}）"),
        Err(e) => {
            eprintln!("❌ 查询失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `search --filter 'status=active AND type=order'`
fn run_cli_search(config_path: &Path, filter: &str) {
    if filter.is_empty() {
        eprintln!("❌ search 需要 --filter 'field=value [AND field2=value2]'");
        std::process::exit(1);
    }
    let mut engine = open_engine(config_path);
    match shanshui_cunji::server::execute_filter(&mut engine, filter) {
        Ok(rows) => {
            println!("命中 {} 条：", rows.len());
            for (docid, v) in &rows {
                println!("  docid={docid}  {}", String::from_utf8_lossy(v));
            }
        }
        Err(e) => {
            eprintln!("❌ 查询失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `range --start 1000 --end 2000`
fn run_cli_range(config_path: &Path, start: Option<u64>, end: Option<u64>) {
    let mut engine = open_engine(config_path);
    let desc = match (start, end) {
        (Some(s), Some(e)) => format!("[{s}..{e}]"),
        (Some(s), None) => format!("[{s}..]"),
        (None, Some(e)) => format!("[..{e}]"),
        (None, None) => "[全量]".into(),
    };
    match engine.scan_range(start, end) {
        Ok(rows) => {
            println!("范围 {desc} 命中 {} 条：", rows.len());
            for (docid, v) in &rows {
                println!("  docid={docid}  {}", String::from_utf8_lossy(v));
            }
        }
        Err(e) => {
            eprintln!("❌ 查询失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `admin status`（design 20）：引擎状态指标（分配器 / LSM / 倒排 / 内存水位）。
fn run_cli_admin(config_path: &Path) {
    let engine = open_engine(config_path);
    let rep = shanshui_cunji::admin::status(&engine);
    println!("shanshui-cunji 状态：");
    println!("  分配器: {}", rep.allocator);
    println!("  SST 文件数: {}", rep.sst_file_count);
    println!("  倒排内存 posting: {}", rep.inverted_mem_docids);
    println!("  倒排段数: {}", rep.inverted_segments);
    println!("  内存水位: {:.0}%", rep.mem_ratio * 100.0);
    println!("  内存上限: {} MB", rep.max_memory_mb);
}

/// `explain --filter 'status=active'`（development 5.26）：执行计划推演，不读数据。
fn run_cli_explain(config_path: &Path, filter: &str) {
    if filter.is_empty() {
        eprintln!("❌ explain 需要 --filter 'field=value'");
        std::process::exit(1);
    }
    let mut engine = open_engine(config_path);
    match shanshui_cunji::explain::explain(&mut engine, filter) {
        Ok(plan) => {
            println!("访问路径: {}", plan.access);
            println!("索引键: {}", plan.key);
            match plan.estimated_rows {
                Some(n) => println!("估算行数: {n}"),
                None => println!("估算行数: 未知"),
            }
            if let Some(w) = plan.warning {
                println!("告警: {w}");
            }
        }
        Err(e) => {
            eprintln!("❌ 推演失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `delete --id 1001`
fn run_cli_delete(config_path: &Path, id: u64) {
    if id == 0 {
        eprintln!("❌ delete 需要 --id <docid>（>0）");
        std::process::exit(1);
    }
    let mut engine = open_engine(config_path);
    match engine.delete(id) {
        Ok(()) => println!("✅ 已删除 docid={id}"),
        Err(e) => {
            eprintln!("❌ 删除失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `count --field status --value active`（倒排 doc_count，development 5.17 COUNT）
fn run_cli_count(config_path: &Path, field: &str, value: &str) {
    if field.is_empty() || value.is_empty() {
        eprintln!("❌ count 需要 --field <字段> --value <值>");
        std::process::exit(1);
    }
    let mut engine = open_engine(config_path);
    match shanshui_cunji::server::execute_count(&mut engine, field, value) {
        Ok(n) => println!("{field}={value} → {n} 条"),
        Err(e) => {
            eprintln!("❌ 计数失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `groupby --field status`（遍历字段倒排 Term 集合构造分组，development 5.17 GROUP BY）
fn run_cli_group_by(config_path: &Path, field: &str) {
    if field.is_empty() {
        eprintln!("❌ groupby 需要 --field <字段>");
        std::process::exit(1);
    }
    let mut engine = open_engine(config_path);
    match shanshui_cunji::server::execute_group_by(&mut engine, field) {
        Ok(groups) => {
            println!("字段 {field} 分组（{} 组）：", groups.len());
            for (term, count) in &groups {
                let val = term.split_once('=').map(|(_, v)| v).unwrap_or(term);
                println!("  {val:<24} {count}");
            }
        }
        Err(e) => {
            eprintln!("❌ 分组失败: {e}");
            std::process::exit(1);
        }
    }
}

/// 启动 HTTP-JSON 服务（development 步骤 15）。
fn run_server(config_path: &Path) {
    let cfg = load_config(config_path);
    let data_dir = PathBuf::from(&cfg.storage.data_dir);
    let mut engine = match shanshui_cunji::engine::Engine::open(&data_dir, &cfg) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ 打开引擎失败: {e}");
            std::process::exit(1);
        }
    };
    info!(
        "shanshui-cunji {VERSION} 启动: data_dir={}",
        data_dir.display()
    );
    if let Err(e) = shanshui_cunji::server::serve(&mut engine, &cfg.server.listen_addr) {
        eprintln!("❌ 服务异常退出: {e}");
        std::process::exit(1);
    }
}
