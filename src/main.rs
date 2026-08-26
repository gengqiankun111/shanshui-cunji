//! novosdb 二进制入口：子命令分发（development 步骤 1 / 5.13）。
//!
//! 子命令：
//! - `server`：启动存储服务（默认，暂未实现，见步骤 15）；
//! - `check`：校验配置与数据目录；
//! - `version`：版本信息。

use std::path::PathBuf;

use novosdb::config::Config;
use tracing::info;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() {
    init_tracing();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut config_path = PathBuf::from("config.toml");

    // 解析 `--config <path>`（允许出现在任意位置）
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
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

    let subcommand = args
        .iter()
        .find(|a| !a.starts_with('-'))
        .map(|s| s.as_str())
        .unwrap_or("server");

    match subcommand {
        "check" => run_check(&config_path),
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
