//! shanshui-cunji-import：数据导入（design 20 / development 5.27 基础版）。
//!
//! 用法：
//!   shanshui-cunji-import --csv <in.csv> [--config config.toml]
//!   shanshui-cunji-import --json <in.jsonl> [--config config.toml]
//!
//! 与迁移工具（shanshui-cunji-migrate）复用内核：import 是通用格式入口（CSV / JSONL），
//! migrate 是 MySQL 专用出口。

use std::path::PathBuf;

use shanshui_cunji::config::Config;
use shanshui_cunji::migrate::{import_csv, import_json};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut csv_path: Option<PathBuf> = None;
    let mut json_path: Option<PathBuf> = None;
    let mut config_path = PathBuf::from("config.toml");
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--csv" => {
                i += 1;
                if i < args.len() {
                    csv_path = Some(PathBuf::from(&args[i]));
                }
            }
            "--json" => {
                i += 1;
                if i < args.len() {
                    json_path = Some(PathBuf::from(&args[i]));
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
    if csv_path.is_none() && json_path.is_none() {
        eprintln!("❌ 用法: shanshui-cunji-import --csv <in.csv> | --json <in.jsonl> [--config config.toml]");
        std::process::exit(1);
    }

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

    let rep = if let Some(p) = &csv_path {
        println!("CSV 导入: {}", p.display());
        import_csv(&mut engine, p)
    } else if let Some(p) = &json_path {
        println!("JSONL 导入: {}", p.display());
        import_json(&mut engine, p)
    } else {
        unreachable!()
    };

    match rep {
        Ok(rep) => {
            println!(
                "✅ 导入完成: {} 行成功, {} 行失败（{:.0} ms）",
                rep.rows, rep.failed, rep.elapsed_ms as f64
            );
            if rep.failed > 0 {
                std::process::exit(2);
            }
        }
        Err(e) => {
            eprintln!("❌ 导入失败: {e}");
            std::process::exit(1);
        }
    }
}
