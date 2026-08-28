//! shanshui-cunji-import：数据导入（design 20 / development 5.27）。
//!
//! 用法：
//!   shanshui-cunji-import --csv <in.csv> [--config config.toml]
//!   shanshui-cunji-import --json <in.jsonl> [--config config.toml]
//!   shanshui-cunji-import --csv <in.csv> --schema schema.json  # import-schema（预创建索引字段基座）
//!
//! 与迁移工具（shanshui-cunji-migrate）复用内核：import 是通用格式入口（CSV / JSONL），
//! migrate 是 MySQL 专用出口。`--schema` 指定导入 Schema（development 5.27 阶段 2）：
//! 预注册字段 + 倒排字段白名单（只对声明字段建索引）+ 组合索引声明。

use std::path::PathBuf;

use shanshui_cunji::config::Config;
use shanshui_cunji::import_schema::ImportSchema;
use shanshui_cunji::migrate::{
    import_csv_filtered, import_csv_incremental, import_json_filtered, import_json_incremental,
    load_checkpoint,
};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut csv_path: Option<PathBuf> = None;
    let mut json_path: Option<PathBuf> = None;
    let mut schema_path: Option<PathBuf> = None;
    let mut checkpoint_path: Option<PathBuf> = None;
    let mut incremental = false;
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
            "--schema" => {
                i += 1;
                if i < args.len() {
                    schema_path = Some(PathBuf::from(&args[i]));
                }
            }
            "--incremental" => incremental = true,
            "--checkpoint" => {
                i += 1;
                if i < args.len() {
                    checkpoint_path = Some(PathBuf::from(&args[i]));
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
        eprintln!("❌ 用法: shanshui-cunji-import --csv <in.csv> | --json <in.jsonl> [--schema schema.json] [--incremental --checkpoint cp] [--config config.toml]");
        std::process::exit(1);
    }
    if incremental && checkpoint_path.is_none() {
        eprintln!("❌ 增量导入必须指定 --checkpoint <文件>");
        std::process::exit(1);
    }

    // import-schema（design 20）：预注册字段 + 倒排白名单
    let whitelist: Option<Vec<String>> = if let Some(sp) = &schema_path {
        match ImportSchema::load(sp) {
            Ok(schema) => {
                match schema.apply() {
                    Ok(rep) => {
                        println!(
                            "✅ schema 应用: 字段 {} 个（倒排白名单 {}，组合索引 {} 个，时间游标 {:?}）",
                            rep.fields.len(),
                            rep.inverted_fields
                                .map(|n| n.to_string())
                                .unwrap_or_else(|| "全部".into()),
                            rep.composite_keys,
                            rep.timestamp_field
                        );
                    }
                    Err(e) => {
                        eprintln!("❌ schema 应用失败: {e}");
                        std::process::exit(1);
                    }
                }
                schema.term_filter()
            }
            Err(e) => {
                eprintln!("❌ schema 加载失败: {e}");
                std::process::exit(1);
            }
        }
    } else {
        None
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

    let rep = if let Some(p) = &csv_path {
        println!("CSV 导入: {}", p.display());
        if incremental {
            let cp = checkpoint_path.as_ref().unwrap();
            let base = load_checkpoint(cp).unwrap_or(0);
            println!("增量导入（checkpoint={base}）: {}", cp.display());
            import_csv_incremental(&mut engine, p, whitelist.as_deref(), cp)
        } else {
            import_csv_filtered(&mut engine, p, whitelist.as_deref())
        }
    } else if let Some(p) = &json_path {
        println!("JSONL 导入: {}", p.display());
        if incremental {
            let cp = checkpoint_path.as_ref().unwrap();
            let base = load_checkpoint(cp).unwrap_or(0);
            println!("增量导入（checkpoint={base}）: {}", cp.display());
            import_json_incremental(&mut engine, p, whitelist.as_deref(), cp)
        } else {
            import_json_filtered(&mut engine, p, whitelist.as_deref())
        }
    } else {
        unreachable!()
    };

    match rep {
        Ok(rep) => {
            println!(
                "✅ 导入完成: {} 行成功, {} 行失败（跳过 {} 行）· {:.0} ms",
                rep.rows, rep.failed, rep.skipped, rep.elapsed_ms as f64
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
