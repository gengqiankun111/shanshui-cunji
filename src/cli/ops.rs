//! 运维维护子命令：`check`（校验配置与数据目录）、`admin`（引擎状态 + 集群配置）、
//! `reload`（配置热加载校验）、`compact`（主数据列族全量合并）。

use std::path::Path;

use shanshui_cunji::config::Config;

use super::{load_config, open_engine, VERSION};

pub(crate) fn run_check(config_path: &Path) {
    println!("shanshui-cunji {VERSION} 配置检查");
    match Config::load(config_path) {
        Ok(cfg) => {
            println!("✅ 配置校验通过");
            println!("   监听地址: {}", cfg.server.listen_addr);
            println!("   数据目录: {}", cfg.storage.data_dir);
            println!(
                "   HotCache: {}MB ({}), BlockCache: {}MB",
                cfg.hotcache.max_memory_mb,
                cfg.hotcache.eviction_policy,
                cfg.blockcache.max_memory_mb
            );
            println!(
                "   倒排引擎: {}, SST 压缩: {}",
                cfg.inverted.engine, cfg.sstable.compression
            );
        }
        Err(e) => {
            eprintln!("❌ 配置校验失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `admin status`（design 20）：引擎状态指标（分配器 / LSM / 倒排 / 内存水位）+ 集群配置（design 9.8）。
pub(crate) fn run_cli_admin(config_path: &Path) {
    let engine = open_engine(config_path);
    let rep = shanshui_cunji::admin::status(&engine);
    println!("shanshui-cunji 状态：");
    println!("  分配器: {}", rep.allocator);
    println!("  SST 文件数: {}", rep.sst_file_count);
    println!("  倒排内存 posting: {}", rep.inverted_mem_docids);
    println!("  倒排段数: {}", rep.inverted_segments);
    println!("  内存水位: {:.0}%", rep.mem_ratio * 100.0);
    println!("  内存上限: {} MB", rep.max_memory_mb);
    // 集群配置（design 9.8，standalone 模式也输出以确认分布式开关状态）
    let cfg = load_config(config_path);
    let cs = shanshui_cunji::admin::cluster_status(&cfg);
    println!("集群配置：");
    println!(
        "  模式: {}（节点 {}，内部 RPC {}）",
        cs.mode, cs.node_id, cs.internal_rpc_port
    );
    println!(
        "  分片: {}（虚拟分片 {}，总物理分片 {}，一致性哈希 {}）",
        if cs.sharding_enabled {
            "开启"
        } else {
            "关闭"
        },
        cs.virtual_shards,
        cs.total_shards,
        if cs.consistent_hash { "是" } else { "否" }
    );
    println!(
        "  副本: {}（角色 {}，模式 {}，Master {}）",
        if cs.replication_enabled {
            "开启"
        } else {
            "关闭"
        },
        cs.replication_role,
        cs.sync_mode,
        if cs.master_addr.is_empty() {
            "-"
        } else {
            &cs.master_addr
        }
    );
    println!("  广播查询并发上限: {}", cs.broadcast_max_concurrent);
}

/// `reload`（design 7.4 / 阶段 3）：配置热加载校验——重新读取并校验配置文件，
/// 输出相对默认配置的变更区块（运行中服务可据此决定重建哪些组件）。
pub(crate) fn run_cli_reload_config(config_path: &Path) {
    let mut cfg = match Config::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ 配置校验失败（保持原配置不变）: {e}");
            std::process::exit(1);
        }
    };
    // 热加载语义：原地 reload（读取→校验→替换），此处加载即校验通过
    let rep = match cfg.reload(config_path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("❌ 热加载失败（保持原配置不变）: {e}");
            std::process::exit(1);
        }
    };
    println!("✅ 配置热加载通过: {}", config_path.display());
    println!(
        "  变更区块: {}",
        if rep.changed_sections.is_empty() {
            "无（配置未变化）".into()
        } else {
            rep.changed_sections.join(", ")
        }
    );
    println!(
        "  运行模式: {}（节点 {}）",
        cfg.server.mode, cfg.cluster.node_id
    );
    println!(
        "  监听: {} · 分配器: {}",
        cfg.server.listen_addr,
        if cfg!(feature = "alloc-jemalloc") {
            "jemalloc"
        } else if cfg!(feature = "alloc-mimalloc") {
            "mimalloc"
        } else {
            "system"
        }
    );
}

/// `compact`（design 4.5 / 阶段 3）：主数据列族全量合并（L0 多 SST → 1）。
pub(crate) fn run_cli_compact(config_path: &Path) {
    let mut engine = open_engine(config_path);
    match engine.compact() {
        Ok(rep) => {
            if rep.merged_ssts <= 1 {
                println!("无需 Compaction（SST 段数 ≤ 1）");
                return;
            }
            println!(
                "✅ Compaction 完成: 合并 {} 个 SST → 消除 {} 个旧版本键，释放 {} 字节",
                rep.merged_ssts, rep.kept_keys, rep.freed_bytes
            );
            if engine.needs_compact() {
                println!("⚠️ 提示: 段数仍超阈值，建议再次 compact");
            }
        }
        Err(e) => {
            eprintln!("❌ Compaction 失败: {e}");
            std::process::exit(1);
        }
    }
}
