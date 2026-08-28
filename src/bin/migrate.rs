//! shanshui-cunji-migrate：迁移工具基础版（development 5.16）。
//!
//! 用法：
//!   shanshui-cunji-migrate --csv <file.csv> [--config <config.toml>]
//!   shanshui-cunji-migrate --sql <dump.sql> [--config <config.toml>]
//!
//! 全量导入（单线程）：CSV 首行表头或 mysqldump INSERT 行为 JSON 字段；
//! 含 docid 列则作为主键，否则从 1 递增；导入完成输出迁移报告。

use std::path::PathBuf;

use shanshui_cunji::config::Config;
use shanshui_cunji::migrate::{import_csv, import_mysqldump};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut csv_path: Option<PathBuf> = None;
    let mut sql_path: Option<PathBuf> = None;
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
            "--sql" => {
                i += 1;
                if i < args.len() {
                    sql_path = Some(PathBuf::from(&args[i]));
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

    if csv_path.is_none() && sql_path.is_none() {
        eprintln!("❌ 用法: shanshui-cunji-migrate --csv <file.csv> | --sql <dump.sql> [--config config.toml]");
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
        println!("CSV 全量导入: {}", p.display());
        import_csv(&mut engine, p)
    } else if let Some(p) = &sql_path {
        println!("mysqldump 全量导入: {}", p.display());
        import_mysqldump(&mut engine, p)
    } else {
        unreachable!()
    };

    match rep {
        Ok(rep) => {
            println!(
                "✅ 迁移完成: {} 行成功, {} 行失败（{:.0} ms）",
                rep.rows, rep.failed, rep.elapsed_ms as f64
            );
            if rep.failed > 0 {
                std::process::exit(2);
            }
        }
        Err(e) => {
            eprintln!("❌ 迁移失败: {e}");
            std::process::exit(1);
        }
    }
}
