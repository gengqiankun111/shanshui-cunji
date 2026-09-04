//! `demo` 子命令：功能冒烟测试（构造数据/插入/查询主键/缓存/组合索引/倒排/分片/删除/备份还原）
//! 并输出终端表格 + HTML 报告；`--gen-only` 时仅构造数据，不执行测试。

use std::path::{Path, PathBuf};

use shanshui_cunji::config::Config;

use super::VERSION;

/// 功能冒烟测试：运行 demo 并输出终端表格 + HTML 报告（输出目录由 `out_dir` 指定）。
/// `--gen-only` 时仅构造数据到 `out_dir/data.jsonl`，不执行测试。
pub(crate) fn run_demo(config_path: &Path, scale: u64, out_dir: &Path, gen_only: bool) {
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
    const SLUGS: [&str; 13] = [
        "01-data",
        "02-insert",
        "03-batch-insert",
        "04-query-primary",
        "05-query-cache",
        "06-query-composite",
        "07-query-inverted",
        "08-query-fulltext",
        "09-query-sql",
        "10-sharding",
        "11-delete",
        "12-optimizer",
        "13-backup",
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
