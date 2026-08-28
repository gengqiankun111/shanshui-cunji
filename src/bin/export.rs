//! shanshui-cunji-export：数据导出（design 20 / development 5.23 基础版）。
//!
//! 用法：`shanshui-cunji-export --csv out.csv [--config config.toml]`
//! 输出两列 CSV：docid, json（RFC 4180 转义，csv crate）。Parquet / 增量 / JDBC 留阶段 2+。

use std::path::PathBuf;

use shanshui_cunji::config::Config;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut out_path: Option<PathBuf> = None;
    let mut config_path = PathBuf::from("config.toml");
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--csv" | "-o" => {
                i += 1;
                if i < args.len() {
                    out_path = Some(PathBuf::from(&args[i]));
                }
            }
            "--config" | "-c" => {
                i += 1;
                if i < args.len() {
                    config_path = PathBuf::from(&args[i]);
                }
            }
            _ => {}
        }
        i += 1;
    }
    let Some(out_path) = out_path else {
        eprintln!("❌ 用法: shanshui-cunji-export --csv <out.csv> [--config config.toml]");
        std::process::exit(1);
    };

    let cfg = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ 配置加载失败: {e}");
            std::process::exit(1);
        }
    };
    let data_dir = PathBuf::from(&cfg.storage.data_dir);
    let mut engine = match shanshui_cunji::engine::Engine::open(&data_dir, &cfg) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ 打开引擎失败: {e}");
            std::process::exit(1);
        }
    };

    let t = std::time::Instant::now();
    let mut wtr = match csv::WriterBuilder::new().from_path(&out_path) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("❌ 打开输出文件失败: {e}");
            std::process::exit(1);
        }
    };
    let _ = wtr.write_record(["docid", "json"]);
    let mut rows = 0u64;
    match engine.scan_range(None, None) {
        Ok(all) => {
            for (docid, bytes) in all {
                let json_str = String::from_utf8_lossy(&bytes);
                if wtr
                    .write_record([docid.to_string(), json_str.into_owned()])
                    .is_err()
                {
                    break;
                }
                rows += 1;
            }
        }
        Err(e) => {
            eprintln!("❌ 扫描失败: {e}");
            std::process::exit(1);
        }
    }
    let _ = wtr.flush();
    println!(
        "✅ 导出完成: {} 行 → {}（{:.0} ms）",
        rows,
        out_path.display(),
        t.elapsed().as_secs_f64() * 1000.0
    );
}
