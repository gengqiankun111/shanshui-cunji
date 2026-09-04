//! shanshui-cunji CLI：参数解析与子命令分发（development 步骤 1 / 5.13）。
//!
//! 子命令：
//! - `server`：启动 HTTP-JSON 服务（默认，步骤 15）；
//! - `put / get / search / range / delete / patch`：数据操作（本地引擎直连，与 HTTP 共享内核路径）；
//! - `count / groupby`：倒排统计聚合（COUNT / GROUP BY，阶段 1.5 M4）；
//! - `backup / restore`：备份还原（步骤 14）；
//! - `check`：校验配置与数据目录；
//! - `demo`：功能冒烟测试（构造数据/插入/查询主键/缓存/组合索引/倒排/分片/删除/备份还原）并输出 HTML 报告；
//! - `version`：版本信息。
//!
//! 各子命令处理函数按主题拆分到子模块：
//! [`serve`]（server）/ [`ops`]（check、admin、reload、compact）/ [`backup_restore`]
//! / [`demo`] / [`data`]（put、patch、get、delete）/ [`query`]（search、range、explain、count、groupby）。

mod backup_restore;
mod data;
mod demo;
mod ops;
mod query;
mod serve;

use std::path::{Path, PathBuf};

use shanshui_cunji::config::Config;

use backup_restore::{run_backup, run_restore};
use data::{run_cli_delete, run_cli_get, run_cli_patch, run_cli_put};
use demo::run_demo;
use ops::{run_check, run_cli_admin, run_cli_compact, run_cli_reload_config};
use query::{run_cli_count, run_cli_explain, run_cli_group_by, run_cli_range, run_cli_search};
use serve::run_server;

pub(crate) const VERSION: &str = env!("CARGO_PKG_VERSION");

/// CLI 入口：解析 `--config <path>` 与各子命令参数（允许出现在任意位置）并分发执行。
pub(crate) fn run() -> Result<(), String> {
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
        "reload" => run_cli_reload_config(&config_path),
        "compact" => run_cli_compact(&config_path),
        "explain" => run_cli_explain(&config_path, &filter),
        "delete" => run_cli_delete(&config_path, id),
        "version" | "-V" | "--version" => {
            println!("shanshui-cunji {VERSION}");
        }
        _ => run_server(&config_path),
    }
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "shanshui_cunji=info".into()),
        )
        .init();
}

/// 加载配置（失败即打印错误并终止进程）。
pub(crate) fn load_config(config_path: &Path) -> Config {
    match Config::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ 配置加载失败: {e}");
            std::process::exit(1);
        }
    }
}

/// 打开本地引擎（数据目录取自配置；失败即打印错误并终止进程）。
pub(crate) fn open_engine(config_path: &Path) -> shanshui_cunji::engine::Engine {
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
