//! MySQL 协议服务器（development_process_order H 项）：让 MySQL 客户端 / 生态工具
//! （mysql cli、JDBC、Navicat、sysbench）直接接入本数据库。
//!
//! 用法：
//!   shanshui-cunji-mysql-server --data-dir <dir> [--config config.toml] [--bind 0.0.0.0:3307] [--user root] [--password 密码]
//!
//! 数据模型：库 `scc`，表 `documents`，列 `id`（BIGINT 主键）+ `doc`（JSON 文档）。
//! 支持：握手 + mysql_native_password 认证 + SHOW DATABASES/TABLES/VARIABLES +
//! SELECT/INSERT/UPDATE/DELETE（映射到文档引擎）+ SET/BEGIN/COMMIT/ROLLBACK（放行）。

use std::path::PathBuf;

use shanshui_cunji::config::Config;
use shanshui_cunji::engine::Engine;
use shanshui_cunji::mysql::MySqlServer;

fn usage() -> ! {
    eprintln!(
        "用法: --data-dir <dir> [--config config.toml] [--bind 0.0.0.0:3307] [--user root] [--password 密码]"
    );
    std::process::exit(2);
}

fn arg(args: &[String], key: &str) -> String {
    let mut it = args.iter();
    while let Some(k) = it.next() {
        if k == key {
            if let Some(v) = it.next() {
                return v.clone();
            }
        }
    }
    usage();
}

fn opt_arg(args: &[String], key: &str, default: &str) -> String {
    let mut it = args.iter();
    while let Some(k) = it.next() {
        if k == key {
            if let Some(v) = it.next() {
                return v.clone();
            }
        }
    }
    default.to_string()
}

fn main() {
    tracing_subscriber::fmt::init();
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        usage();
    }
    let data_dir = PathBuf::from(arg(&args, "--data-dir"));
    let config_path = opt_arg(&args, "--config", "");
    let bind = opt_arg(&args, "--bind", "0.0.0.0:3307");
    let user = opt_arg(&args, "--user", "root");
    let password = opt_arg(&args, "--password", "");

    let cfg = if config_path.is_empty() {
        Config::default()
    } else {
        match Config::load(std::path::Path::new(&config_path)) {
            Ok(c) => {
                println!("[mysql-server] 配置加载: {config_path}");
                c
            }
            Err(e) => {
                eprintln!("❌ 配置加载失败: {e}");
                std::process::exit(1);
            }
        }
    };
    let engine = Engine::open(&data_dir, &cfg).expect("打开引擎失败");
    println!(
        "[mysql-server] 数据目录 {} 打开完成，启动 MySQL 协议服务: {bind}",
        data_dir.display()
    );
    MySqlServer::new(engine, user, password)
        .serve(&bind)
        .expect("MySQL 服务失败");
}
