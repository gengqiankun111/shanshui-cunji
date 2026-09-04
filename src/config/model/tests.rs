use super::*;

#[test]
fn defaults_are_valid() {
    let mut cfg = Config::default();
    assert!(cfg.validate().is_ok());
    assert_eq!(cfg.hotcache.eviction_policy, "lfu");
    assert_eq!(cfg.sstable.compression, "zstd");
    assert_eq!(cfg.storage.l0_stall_threshold, 12);
    assert!(cfg.storage.deletion_bitmap_enabled, "删除位图默认开启（Ex-5.6）");
}

#[test]
fn toml_load_with_partial_section() {
    let text = r#"
[hotcache]
max_memory_mb = 2048

[inverted]
engine = "fst"
"#;
    let cfg: Config = toml::from_str(text).unwrap();
    assert_eq!(cfg.hotcache.max_memory_mb, 2048);
    assert_eq!(cfg.inverted.engine, "fst");
    // 未提及字段取默认
    assert_eq!(cfg.server.listen_addr, "0.0.0.0:8080");
    assert_eq!(cfg.sstable.compression, "zstd");
}

#[test]
fn invalid_watermark_rejected() {
    let mut cfg = Config::default();
    cfg.memory.watermark_high = 0.0;
    assert!(cfg.validate().is_err());
}

#[test]
fn invalid_inverted_engine_rejected() {
    let mut cfg = Config::default();
    cfg.inverted.engine = "bogus".into();
    assert!(cfg.validate().is_err());
}

#[test]
fn oversized_cache_budget_is_degraded() {
    let mut cfg = Config::default();
    cfg.hotcache.max_memory_mb = 40 * 1024; // 40GB
    cfg.blockcache.max_memory_mb = 20 * 1024; // 20GB，合计 60GB > 64GB*0.7
    cfg.validate().unwrap();
    let total = cfg.hotcache.max_memory_mb + cfg.blockcache.max_memory_mb;
    assert!(total as f64 <= 64.0 * 1024.0 * MEMORY_BUDGET_RATIO + 1.0);
}

#[test]
fn load_from_toml_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "[storage]\ndata_dir = \"/tmp/shanshui-cunji\"\n").unwrap();
    let cfg = Config::load(&path).unwrap();
    assert_eq!(cfg.storage.data_dir, "/tmp/shanshui-cunji");
}

#[test]
fn load_missing_file_uses_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Config::load(&dir.path().join("nope.toml")).unwrap();
    assert_eq!(cfg.storage.data_dir, "./data");
}

#[test]
fn reload_applies_changes_and_reports_sections() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(
        &path,
        "[hotcache]\nmax_memory_mb = 1024\n[sstable]\ncompression = \"zstd\"\n",
    )
    .unwrap();
    let mut cfg = Config::load(&path).unwrap();
    assert_eq!(cfg.hotcache.max_memory_mb, 1024);

    // 修改配置：hotcache 与 sstable 区块
    std::fs::write(
        &path,
        "[hotcache]\nmax_memory_mb = 2048\n[sstable]\ncompression = \"lz4\"\n",
    )
    .unwrap();
    let rep = cfg.reload(&path).unwrap();
    assert!(rep.applied);
    assert_eq!(cfg.hotcache.max_memory_mb, 2048, "热加载应替换生效");
    assert_eq!(cfg.sstable.compression, "lz4");
    assert!(rep.changed_sections.contains(&"hotcache".to_string()));
    assert!(rep.changed_sections.contains(&"sstable".to_string()));

    // 无变更 → 空区块列表
    let rep = cfg.reload(&path).unwrap();
    assert!(rep.changed_sections.is_empty(), "无变更时不应报告区块");
}

#[test]
fn reload_failure_keeps_current_config() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "[hotcache]\nmax_memory_mb = 512\n").unwrap();
    let mut cfg = Config::load(&path).unwrap();
    // 写入非法配置（watermark 越界）
    std::fs::write(&path, "[memory]\nwatermark_high = 0.0\n").unwrap();
    assert!(cfg.reload(&path).is_err(), "非法配置应拒绝热加载");
    assert_eq!(cfg.hotcache.max_memory_mb, 512, "失败后保持原配置");
}

#[test]
fn env_override_applies() {
    // 用 std::env::set_var 注入，测试后清理（测试内串行）
    std::env::set_var("SHANSHUI_CUNJI__HOTCACHE__MAX_MEMORY_MB", "2048");
    std::env::set_var("SHANSHUI_CUNJI__SERVER__LISTEN_ADDR", "127.0.0.1:9000");
    let mut cfg = Config::default();
    cfg.apply_env_overrides();
    std::env::remove_var("SHANSHUI_CUNJI__HOTCACHE__MAX_MEMORY_MB");
    std::env::remove_var("SHANSHUI_CUNJI__SERVER__LISTEN_ADDR");
    assert_eq!(cfg.hotcache.max_memory_mb, 2048);
    assert_eq!(cfg.server.listen_addr, "127.0.0.1:9000");
}

#[test]
fn cluster_config_parses_with_defaults() {
    let text = r#"
[server]
mode = "cluster"

[cluster]
node_id = "node-2"
internal_rpc_port = 9091

[sharding]
enabled = true
virtual_shards = 2048

[replication]
enabled = true
role = "slave"
master_addr = "node-1:9090"
sync_mode = "sync"

[broadcast_query]
max_concurrent = 20
timeout_ms = 15000
"#;
    let mut cfg: Config = toml::from_str(text).unwrap();
    assert_eq!(cfg.server.mode, "cluster");
    assert_eq!(cfg.cluster.node_id, "node-2");
    assert_eq!(cfg.cluster.internal_rpc_port, 9091);
    assert!(cfg.sharding.enabled);
    assert_eq!(cfg.sharding.virtual_shards, 2048);
    // 未提及字段取默认
    assert!(cfg.sharding.consistent_hash);
    assert_eq!(cfg.sharding.shard_key, "docid");
    assert_eq!(cfg.replication.role, "slave");
    assert_eq!(cfg.replication.master_addr, "node-1:9090");
    assert_eq!(cfg.replication.sync_mode, "sync");
    assert_eq!(cfg.replication.ack_timeout_ms, 1000);
    assert_eq!(cfg.broadcast_query.max_concurrent, 20);
    cfg.validate().unwrap();
}

#[test]
fn standalone_forces_sharding_and_replication_off() {
    let mut cfg = Config::default();
    cfg.sharding.enabled = true;
    cfg.replication.enabled = true;
    cfg.read_write_separation.enabled = true;
    cfg.validate().unwrap();
    assert_eq!(cfg.server.mode, "standalone");
    assert!(!cfg.sharding.enabled, "standalone 必须强制关闭分片");
    assert!(!cfg.replication.enabled, "standalone 必须强制关闭副本");
    assert!(
        !cfg.read_write_separation.enabled,
        "standalone 必须强制关闭读写分离"
    );
}

#[test]
fn invalid_cluster_config_rejected() {
    // 非法角色
    let mut cfg = Config::default();
    cfg.server.mode = "cluster".into();
    cfg.sharding.enabled = true;
    cfg.replication.role = "follower".into();
    assert!(cfg.validate().is_err());

    // slave 缺少 master_addr
    let mut cfg = Config::default();
    cfg.server.mode = "cluster".into();
    cfg.sharding.enabled = true;
    cfg.replication.role = "slave".into();
    cfg.replication.master_addr = String::new();
    assert!(cfg.validate().is_err());

    // 非法 sync_mode
    let mut cfg = Config::default();
    cfg.server.mode = "cluster".into();
    cfg.sharding.enabled = true;
    cfg.replication.sync_mode = "raft".into();
    assert!(cfg.validate().is_err());

    // 非法 mode
    let mut cfg = Config::default();
    cfg.server.mode = "hybrid".into();
    assert!(cfg.validate().is_err());

    // virtual_shards = 0
    let mut cfg = Config::default();
    cfg.server.mode = "cluster".into();
    cfg.sharding.enabled = true;
    cfg.sharding.virtual_shards = 0;
    assert!(cfg.validate().is_err());
}