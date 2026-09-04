//! MySQL 协议服务器（development_process_order H 项）：让 MySQL 客户端 / 生态工具
//! （mysql cli、JDBC、Navicat、sysbench）直接接入本数据库。
//!
//! 用法：
//!   cjserver --data-dir <dir> [--config config.toml] [--bind 0.0.0.0:3307] [--user root] [--password 密码]
//!
//! 数据模型：库 `cjserver`，表 `documents`，列 `id`（BIGINT 主键）+ `doc`（JSON 文档）。
//! 支持：握手 + mysql_native_password 认证 + SHOW DATABASES/TABLES/VARIABLES +
//! SELECT/INSERT/UPDATE/DELETE（映射到文档引擎）+ SET/BEGIN/COMMIT/ROLLBACK（放行）。

use std::path::PathBuf;

use shanshui_cunji::config::Config;
use shanshui_cunji::engine::Engine;
use shanshui_cunji::db_adapter::DbServer;

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
    // 全扫/聚合查询看门狗（秒，默认 30）：无索引全表类负载在大库（GB~百 GB）上单次扫描
    // 需数分钟，默认 30s 会熔断中止；--watchdog-secs 供超大库测试/运维放行。
    let watchdog_secs: u64 = opt_arg(&args, "--watchdog-secs", "30")
        .parse()
        .unwrap_or(30);
    // I 项异步协程运行时（design 9.5 10k 连接目标）：--async 切换 tokio 网络层
    let async_mode = args.iter().any(|a| a == "--async");

    let mut cfg = if config_path.is_empty() {
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
    // MySQL 协议接入默认开启组提交：`group_commit_us=0`（默认关）→ 每次 put 独立 WAL fsync，
    // 实测插入仅 ~1k rows/s（vs 引擎原生/组提交 14 万+）；2ms 攒批窗口一次 fsync 消除逐行落盘。
    // 用户可在 config 显式指定（0 关闭需在 config 文件显式声明）。
    if cfg.storage.group_commit_us == 0 {
        cfg.storage.group_commit_us = 2000;
        println!("[cjserver] 默认开启组提交（group_commit_us=2000µs，config 可覆盖）");
    }
    // P2-A：事务 COMMIT 耐久档位（对齐 MySQL innodb_flush_log_at_trx_commit）。
    // 1 = 每次 COMMIT 显式 fsync（强安全默认）；0/2 = COMMIT 交组提交窗口（延迟耐久，
    // 并发事务基准建议 2，config `storage.flush_log_at_trx_commit` 可覆盖）。
    println!(
        "[cjserver] 事务 COMMIT 耐久档位 flush_log_at_trx_commit={}（1 = 逐 COMMIT fsync 强安全；0/2 = 组提交窗口延迟落盘）",
        cfg.storage.flush_log_at_trx_commit
    );
    let engine = Engine::open_with_timeout(
        &data_dir,
        &cfg,
        // 字段过滤/数字等值回退/比较扫描为无索引全表类负载，默认看门狗 30s 只够扫 ~3GB
        // （MySQL 无索引等值/全扫 2.5s+ 量级）；--watchdog-secs 可放行超大库全扫（对齐语义），
        // 防真挂起仍有效。
        std::time::Duration::from_secs(watchdog_secs),
    )
    .expect("打开引擎失败");
    println!(
        "[cjserver] 数据目录 {} 打开完成，启动 MySQL 协议服务{}: {bind}",
        data_dir.display(),
        if async_mode { "（异步协程）" } else { "" }
    );
    let server = DbServer::new(engine, user, password);
    if async_mode {
        // 异步网络层：连接 idle 不占 OS 线程；查询经 spawn_blocking 复用同步引擎
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime 构建失败");
        rt.block_on(async move {
            server.serve_async(&bind).await.expect("MySQL 异步服务失败");
        });
    } else {
        server.serve(&bind).expect("MySQL 服务失败");
    }
}
