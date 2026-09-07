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
    // 库内 schema（cj.schema.json）：按表的**显式索引声明**（无内置默认）。
    // schema 是索引声明的权威来源——存在时覆盖 cfg 中的倒排/组合/位图等声明项
    // （--config 只承载运行参数：缓存/组提交/看门狗等）。引擎当前为单逻辑表，
    // 目标取 documents，缺失则用首张声明表。
    match shanshui_cunji::schema_store::DbSchema::load_dir(&data_dir) {
        Ok(Some(db_schema)) => {
            let target = db_schema
                .table("documents")
                .or_else(|| db_schema.tables.first());
            match target {
                Some(t) => {
                    t.apply_to_cfg(&mut cfg);
                    println!(
                        "[cjserver] 库内 schema 装配表 '{}': 倒排 {} 字段, 组合 {} 组, 位图 {} 字段, fulltext {} 字段",
                        t.name,
                        t.inverted_fields.len(),
                        t.composite_indexes.len(),
                        t.bitmap_fields.len(),
                        t.fulltext_fields.len()
                    );
                }
                None => println!("[cjserver] cj.schema.json 无表声明 → 零索引模式"),
            }
        }
        Ok(None) => {
            // 无库内 schema：全凭 --config / 声明制（P131b，缺省 inverted_fields = 零索引，
            // 绝不回退 legacy 全字段隐式建倒排）。
            cfg.inverted.declared_only = true;
            if cfg.inverted.inverted_fields.is_empty() && cfg.storage.composite_indexes.is_empty()
            {
                println!(
                    "[cjserver] 数据目录无 cj.schema.json 且未声明索引 → 零索引模式（可写 cj.schema.json 声明倒排/组合索引）"
                );
            }
        }
        Err(e) => {
            eprintln!("❌ 库内 schema 读取失败: {e}");
            std::process::exit(1);
        }
    }
    // 组提交默认已开（config `storage.group_commit_us` 默认 1000µs，2026-09-05 起）——
    // MySQL 协议接入无需再强制：逐行 put 走组提交窗口一次 fsync（P75 根因修复随默认化生效）。
    // 显式 `group_commit_us = 0` 表示用户选择逐条 fsync 强安全（尊重配置，不再覆盖）。
    // P2-A：事务 COMMIT 耐久档位（对齐 MySQL innodb_flush_log_at_trx_commit）。
    // 1 = 每次 COMMIT 显式 fsync（强安全默认）；0/2 = COMMIT 交组提交窗口（延迟耐久，
    // 并发事务基准建议 2，config `storage.flush_log_at_trx_commit` 可覆盖）。
    println!(
        "[cjserver] 组提交 group_commit_us={}µs（0 = 逐条 fsync 强安全）",
        cfg.storage.group_commit_us
    );
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
