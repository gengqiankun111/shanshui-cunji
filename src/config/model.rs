//! 配置模型与加载逻辑（design 13 / development 步骤 1）。
//!
//! 覆盖单机最小配置：server / memory / memtable / hotcache / blockcache / runtime / sstable / storage / inverted。

use std::path::Path;

use serde::Deserialize;
use tracing::warn;

use crate::error::{Error, Result};

/// 缓存软水位：达此比例触发主动淘汰，永不到达 100%（design 14.1.1）。
pub const DEFAULT_EVICTION_HIGH_WATER: f64 = 0.85;
/// 淘汰目标水位。
pub const DEFAULT_EVICTION_LOW_WATER: f64 = 0.75;
/// 内存硬上限占可用内存比例（启动校验红线，design 13）。
pub const MEMORY_BUDGET_RATIO: f64 = 0.7;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct Config {
    pub server: ServerConfig,
    pub memory: MemoryConfig,
    pub memtable: MemtableConfig,
    pub hotcache: HotCacheConfig,
    pub blockcache: BlockCacheConfig,
    pub runtime: RuntimeConfig,
    pub sstable: SstableConfig,
    pub storage: StorageConfig,
    pub inverted: InvertedConfig,
    pub join: JoinConfig,
    pub enrich: EnrichConfig,
    pub cluster: ClusterConfig,
    pub sharding: ShardingConfig,
    pub replication: ReplicationConfig,
    pub read_write_separation: ReadWriteSeparationConfig,
    pub broadcast_query: BroadcastQueryConfig,
    pub compaction: CompactionConfig,
    pub sidecar: SidecarConfig,
    pub cache_external: CacheExternalConfig,
}

impl Config {
    /// 从 `config.toml` 加载；文件不存在则使用全部默认值。
    pub fn load(path: &Path) -> Result<Self> {
        let mut cfg = if path.exists() {
            let text = std::fs::read_to_string(path)
                .map_err(|e| Error::Config(format!("读取配置失败: {e}")))?;
            toml::from_str(&text).map_err(|e| Error::Config(format!("解析配置失败: {e}")))?
        } else {
            warn!("配置文件 {} 不存在，使用默认配置", path.display());
            Self::default()
        };
        cfg.apply_env_overrides();
        cfg.validate()?;
        Ok(cfg)
    }

    /// 环境变量覆盖：`SHANSHUI_CUNJI__SECTION__KEY=VALUE`。
    fn apply_env_overrides(&mut self) {
        for (key, value) in std::env::vars() {
            let Some(rest) = key.strip_prefix("SHANSHUI_CUNJI__") else {
                continue;
            };
            let parts: Vec<&str> = rest.split("__").collect();
            let val = value.trim().to_string();
            match parts.as_slice() {
                ["HOTCACHE", "MAX_MEMORY_MB"] => {
                    self.hotcache.max_memory_mb = parse_override("hotcache.max_memory_mb", &val)
                }
                ["HOTCACHE", "EVICTION_POLICY"] => self.hotcache.eviction_policy = val,
                ["BLOCKCACHE", "MAX_MEMORY_MB"] => {
                    self.blockcache.max_memory_mb = parse_override("blockcache.max_memory_mb", &val)
                }
                ["SERVER", "LISTEN_ADDR"] => self.server.listen_addr = val,
                ["CLUSTER", "NODE_ID"] => self.cluster.node_id = val,
                ["CLUSTER", "INTERNAL_RPC_PORT"] => {
                    self.cluster.internal_rpc_port = val
                        .parse::<u16>()
                        .unwrap_or_else(|_| {
                            warn!("环境变量 CLUSTER__INTERNAL_RPC_PORT 解析失败，忽略");
                            self.cluster.internal_rpc_port
                        });
                }
                ["SHARDING", "ENABLED"] => {
                    self.sharding.enabled = val == "true" || val == "1";
                }
                ["SHARDING", "VIRTUAL_SHARDS"] => {
                    self.sharding.virtual_shards = parse_override_u32(
                        "sharding.virtual_shards",
                        &val,
                        self.sharding.virtual_shards,
                    );
                }
                ["REPLICATION", "ROLE"] => self.replication.role = val,
                ["BROADCAST_QUERY", "MAX_CONCURRENT"] => {
                    self.broadcast_query.max_concurrent = parse_override(
                        "broadcast_query.max_concurrent",
                        &val,
                    );
                }
                ["MEMORY", "WATERMARK_HIGH"] => {
                    self.memory.watermark_high = parse_override_f64("memory.watermark_high", &val)
                }
                ["MEMORY", "WATERMARK_STALL"] => {
                    self.memory.watermark_stall = parse_override_f64("memory.watermark_stall", &val)
                }
                _ => {
                    warn!("未知环境变量覆盖: {key}");
                }
            }
        }
    }

    /// 启动校验：缓存预算不得吃满可用内存（design 13）；越界自动降级并告警。
    pub fn validate(&mut self) -> Result<()> {
        let total = self.hotcache.max_memory_mb as f64 + self.blockcache.max_memory_mb as f64;
        // 可用内存探测由部署层注入（阶段 1 先用预算比阈值：总缓存不应超过 64GB 典型机型的 70% 预算线）
        const REFERENCE_AVAILABLE_MB: f64 = 64.0 * 1024.0;
        let limit = REFERENCE_AVAILABLE_MB * MEMORY_BUDGET_RATIO;
        if total > limit {
            warn!(
                "缓存预算 {total:.0}MB 超过参考可用内存预算 {limit:.0}MB，自动降级至 {limit:.0}MB"
            );
            // 按比例降级（design 14.1.1：缓存缩容）
            let ratio = limit / total;
            self.blockcache.max_memory_mb = (self.blockcache.max_memory_mb as f64 * ratio) as usize;
            self.hotcache.max_memory_mb = (self.hotcache.max_memory_mb as f64 * ratio) as usize;
        }
        if self.memory.watermark_high <= 0.0
            || self.memory.watermark_stall < self.memory.watermark_high
        {
            return Err(Error::Config(
                "memory.watermark_high 须 > 0，且 watermark_stall 须 >= watermark_high".into(),
            ));
        }
        if self.memtable.max_size_mb == 0 {
            return Err(Error::Config("memtable.max_size_mb 必须 > 0".into()));
        }
        if !matches!(self.inverted.engine.as_str(), "hash" | "fst") {
            return Err(Error::Config(format!(
                "inverted.engine 非法: {}",
                self.inverted.engine
            )));
        }
        if self.inverted.segment_max_size_mb == 0 {
            return Err(Error::Config(
                "inverted.segment_max_size_mb 必须 > 0".into(),
            ));
        }
        if self.join.max_rows == 0 {
            return Err(Error::Config("join.max_rows 必须 > 0".into()));
        }
        if !matches!(self.enrich.fail_policy.as_str(), "reject" | "degrade") {
            return Err(Error::Config(format!(
                "enrich.fail_policy 非法: {}（reject / degrade）",
                self.enrich.fail_policy
            )));
        }
        self.validate_cluster()?;
        Ok(())
    }

    /// 分布式配置校验（design 9.8）：模式互斥、分片/复制参数边界。
    fn validate_cluster(&mut self) -> Result<()> {
        if !matches!(self.server.mode.as_str(), "standalone" | "cluster") {
            return Err(Error::Config(format!(
                "server.mode 非法: {}（standalone / cluster）",
                self.server.mode
            )));
        }
        if self.server.mode == "standalone" {
            // 单机模式强制关闭分片 / 副本 / 读写分离（design 9.8 "单机模式强制 false"）。
            for (name, set) in [
                ("sharding.enabled", &mut self.sharding.enabled),
                ("replication.enabled", &mut self.replication.enabled),
                (
                    "read_write_separation.enabled",
                    &mut self.read_write_separation.enabled,
                ),
            ] {
                if *set {
                    warn!("standalone 模式强制关闭 {name}，请改用 server.mode = \"cluster\"");
                    *set = false;
                }
            }
            return Ok(());
        }
        // cluster 模式
        if !self.sharding.enabled {
            warn!("cluster 模式建议开启分片（sharding.enabled = true）");
        }
        if self.sharding.virtual_shards == 0 {
            return Err(Error::Config("sharding.virtual_shards 必须 > 0".into()));
        }
        if self.sharding.shard_key != "docid" {
            return Err(Error::Config(format!(
                "sharding.shard_key 仅支持 \"docid\"，当前: {}",
                self.sharding.shard_key
            )));
        }
        if self.cluster.internal_rpc_port == 0 {
            return Err(Error::Config("cluster.internal_rpc_port 必须 > 0".into()));
        }
        if !matches!(self.replication.role.as_str(), "master" | "slave") {
            return Err(Error::Config(format!(
                "replication.role 非法: {}（master / slave）",
                self.replication.role
            )));
        }
        if !matches!(self.replication.sync_mode.as_str(), "async" | "sync") {
            return Err(Error::Config(format!(
                "replication.sync_mode 非法: {}（async / sync）",
                self.replication.sync_mode
            )));
        }
        if self.replication.role == "slave" && self.replication.master_addr.is_empty() {
            return Err(Error::Config(
                "replication.role=slave 必须配置 replication.master_addr".into(),
            ));
        }
        if self.replication.sync_mode == "sync" && self.replication.ack_timeout_ms == 0 {
            return Err(Error::Config("replication.ack_timeout_ms 必须 > 0".into()));
        }
        if self.replication.batch_size == 0 || self.replication.heartbeat_interval_sec == 0 {
            return Err(Error::Config(
                "replication.batch_size / heartbeat_interval_sec 必须 > 0".into(),
            ));
        }
        if self.broadcast_query.max_concurrent == 0 || self.broadcast_query.timeout_ms == 0 {
            return Err(Error::Config(
                "broadcast_query.max_concurrent / timeout_ms 必须 > 0".into(),
            ));
        }
        if self.broadcast_query.term_cache_invalid_threshold == 0
            || self.broadcast_query.term_cache_max_entries == 0
        {
            return Err(Error::Config(
                "broadcast_query.term_cache_invalid_threshold / term_cache_max_entries 必须 > 0"
                    .into(),
            ));
        }
        if self.compaction.stall_timeout_secs == 0 || self.compaction.max_consecutive_failures == 0 {
            return Err(Error::Config(
                "compaction.stall_timeout_secs / max_consecutive_failures 必须 > 0".into(),
            ));
        }
        if self.sidecar.ping_interval_sec == 0 || self.sidecar.max_missed_pings == 0 {
            return Err(Error::Config(
                "sidecar.ping_interval_sec / max_missed_pings 必须 > 0".into(),
            ));
        }
        if !matches!(
            self.cache_external.write_policy.as_str(),
            "invalidate" | "double_delete" | "none"
        ) {
            return Err(Error::Config(format!(
                "cache.external.write_policy 非法: {}（invalidate / double_delete / none）",
                self.cache_external.write_policy
            )));
        }
        if self.cache_external.ttl_seconds == 0 || self.cache_external.timeout_ms == 0 {
            return Err(Error::Config(
                "cache.external.ttl_seconds / timeout_ms 必须 > 0".into(),
            ));
        }
        if self.cache_external.enabled && self.cache_external.redis_addrs.is_empty() {
            return Err(Error::Config(
                "cache.external.enabled 时必须配置 redis_addrs".into(),
            ));
        }
        Ok(())
    }
}

fn parse_override(name: &str, v: &str) -> usize {
    v.parse::<usize>().unwrap_or_else(|_| {
        warn!("环境变量 {name} 解析失败，忽略");
        0
    })
}

fn parse_override_u32(name: &str, v: &str, default: u32) -> u32 {
    v.parse::<u32>().unwrap_or_else(|_| {
        warn!("环境变量 {name} 解析失败，忽略");
        default
    })
}

fn parse_override_f64(name: &str, v: &str) -> f64 {
    v.parse::<f64>().unwrap_or_else(|_| {
        warn!("环境变量 {name} 解析失败，忽略");
        0.0
    })
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// 运行模式（design 9.8）："standalone"（默认）/ "cluster"。
    pub mode: String,
    pub listen_addr: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            mode: "standalone".into(),
            listen_addr: "0.0.0.0:8080".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MemoryConfig {
    /// RSS 软限流水位，触发写限流（OOM Guardian）。
    pub watermark_high: f64,
    /// RSS 硬限流水位，触发 503 + 紧急止损。
    pub watermark_stall: f64,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            watermark_high: 0.85,
            watermark_stall: 1.0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MemtableConfig {
    /// 跳表上限，达阈值冻结切换并后台刷盘。
    pub max_size_mb: usize,
}

impl Default for MemtableConfig {
    fn default() -> Self {
        Self { max_size_mb: 256 }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct HotCacheConfig {
    pub enabled: bool,
    pub max_memory_mb: usize,
    pub initial_capacity_mb: usize,
    /// lru / lfu / tiny-lfu。
    pub eviction_policy: String,
    /// 每秒访问几次算"热"，触发联动预热。
    pub hot_threshold: u32,
    pub prewarm_on_startup: bool,
    /// 超过此大小的文档不缓存，防大对象挤占内存。
    pub max_document_size_bytes: usize,
    pub eviction_high_water: f64,
    pub eviction_low_water: f64,
}

impl Default for HotCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_memory_mb: 4096,
            initial_capacity_mb: 256,
            eviction_policy: "lfu".into(),
            hot_threshold: 5,
            prewarm_on_startup: false,
            max_document_size_bytes: 102_400,
            eviction_high_water: DEFAULT_EVICTION_HIGH_WATER,
            eviction_low_water: DEFAULT_EVICTION_LOW_WATER,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BlockCacheConfig {
    pub max_memory_mb: usize,
    pub block_size_kb: usize,
    pub eviction_high_water: f64,
}

impl Default for BlockCacheConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: 2048,
            block_size_kb: 16,
            eviction_high_water: DEFAULT_EVICTION_HIGH_WATER,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    /// multi-thread / current-thread。
    pub async_mode: String,
    /// 0 = 自动检测物理核数。
    pub cpu_cores_total: usize,
    pub async_worker_threads: usize,
    pub async_max_tasks: usize,
    pub compute_pool_size: usize,
    pub compute_queue_max: usize,
    pub io_background_threads: usize,
    pub io_uring_enabled: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            async_mode: "multi-thread".into(),
            cpu_cores_total: 0,
            async_worker_threads: 0,
            async_max_tasks: 10_000,
            compute_pool_size: 8,
            compute_queue_max: 1000,
            io_background_threads: 4,
            io_uring_enabled: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SstableConfig {
    /// none / snappy / lz4 / zstd。
    pub compression: String,
    /// zstd 专用 1~22。
    pub compression_level: u32,
    /// 布隆假阳性率。
    pub bloom_fpr: f64,
    /// 两级索引：每 N 个 Block 一条摘要。
    pub index_granularity: u32,
}

impl Default for SstableConfig {
    fn default() -> Self {
        Self {
            compression: "zstd".into(),
            compression_level: 3,
            bloom_fpr: 0.01,
            index_granularity: 16,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// L0 文件数阈值，超过则写 Stall 限流。
    pub l0_stall_threshold: usize,
    /// TTL 天数（时间分区过期整删）。
    pub ttl_days: Option<u32>,
    /// 数据目录。
    pub data_dir: String,
    /// 热字段白名单（阶段 1.5 PAX 列式块）：高频查询字段进热列组（块头），其余进冷列组（块尾）。
    pub hot_fields: Vec<String>,
    /// TTL 时间分桶粒度：`day`（MVP）/ `hour`（预留，阶段 1.5 仅 day）。
    pub time_bucket: String,
    /// TTL 时间字段名（文档 JSON 内，数值秒级时间戳）。
    pub ttl_field: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            l0_stall_threshold: 8,
            ttl_days: None,
            data_dir: "./data".into(),
            hot_fields: Vec::new(),
            time_bucket: "day".into(),
            ttl_field: "timestamp".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct InvertedConfig {
    /// hash（MVP）/ fst（阶段 1.5，mmap 亚秒冷启动）。
    pub engine: String,
    /// 倒排字典内存硬上限（MB）。
    pub max_memory_bytes: usize,
    /// 魔鬼倒排列表门控：超过则不展开，降级全表扫描 + Zone Map。
    pub max_posting_scan: u64,
    /// 倒排段 GC 阈值（MB，design 5.2.2 / 5.2.4⑤）：段文件总量超此值触发后台合并。
    pub segment_max_size_mb: u64,
}

impl Default for InvertedConfig {
    fn default() -> Self {
        Self {
            // 阶段 1.5 起默认 FST + mmap 字典（design 5.2.4.1：亚秒冷启动、按需加载）
            engine: "fst".into(),
            max_memory_bytes: 12 * 1024 * 1024 * 1024,
            max_posting_scan: 1_000_000,
            segment_max_size_mb: 1024,
        }
    }
}

/// 数据关联（sdk::join，development 5.20 / design 19）。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct JoinConfig {
    /// queryAndJoin 结果集上限，超限熔断（默认 100 万）。
    pub max_rows: usize,
}

impl Default for JoinConfig {
    fn default() -> Self {
        Self {
            max_rows: 1_000_000,
        }
    }
}

/// 写入 Enrich（预连接，development 5.21 / design 19）。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct EnrichConfig {
    /// 是否启用 Enrich。
    pub enabled: bool,
    /// 数据源：redis / mysql / http / local（基础版仅 local）。
    pub source: String,
    /// 失败策略："reject"（拒绝写入）/ "degrade"（降级写入原文档）。
    pub fail_policy: String,
}

impl Default for EnrichConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            source: "local".into(),
            fail_policy: "degrade".into(),
        }
    }
}

/// 集群节点（design 9.8）：节点标识与内部 RPC。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    /// 集群唯一标识（默认 "node-1"）。
    pub node_id: String,
    /// 对外服务端口（HTTP/TCP）。
    pub listen_addr: String,
    /// 分片节点间内部 RPC 端口（数据同步、心跳）。
    pub internal_rpc_port: u16,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: "node-1".into(),
            listen_addr: "0.0.0.0:8080".into(),
            internal_rpc_port: 9090,
        }
    }
}

/// 分片路由（design 9.1 / 9.8）：DocId 一致性哈希两级路由。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ShardingConfig {
    /// 是否开启分片（单机模式强制 false）。
    pub enabled: bool,
    /// 物理分片总数（0 = 按节点数自动；扩容用虚拟分片，不可变）。
    pub total_shards: u32,
    /// 虚拟分片数（推荐 1024/2048，扩容只迁移部分）。
    pub virtual_shards: u32,
    /// 分片键，固定 "docid"（暂不支持自定义，留扩展）。
    pub shard_key: String,
    /// 一致性哈希（true）/ 直接取模（false）。推荐一致性哈希减少扩容抖动。
    pub consistent_hash: bool,
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            total_shards: 0,
            virtual_shards: 1024,
            shard_key: "docid".into(),
            consistent_hash: true,
        }
    }
}

/// 主从与副本（design 9.3 / 9.8）：一主多从异步/同步复制。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ReplicationConfig {
    /// 是否开启副本（单机模式强制 false）。
    pub enabled: bool,
    /// 角色："master"（默认）/ "slave"。
    pub role: String,
    /// Slave 填写 Master 的 RPC 地址（host:port）。
    pub master_addr: String,
    /// 同步模式："async"（默认，写入延迟≈单机）/ "sync"（强一致，等 Slave ACK）。
    pub sync_mode: String,
    /// sync 模式等待 Slave ACK 超时（ms）。
    pub ack_timeout_ms: u64,
    /// 异步复制攒批发送条数。
    pub batch_size: usize,
    /// 主从心跳间隔（秒），用于探活。
    pub heartbeat_interval_sec: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            role: "master".into(),
            master_addr: String::new(),
            sync_mode: "async".into(),
            ack_timeout_ms: 1000,
            batch_size: 1000,
            heartbeat_interval_sec: 5,
        }
    }
}

/// 读写分离（design 9.8）：普通查询优先路由 Slave，超滞后降级读 Master。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ReadWriteSeparationConfig {
    /// 是否开启读写分离。
    pub enabled: bool,
    /// true 时普通查询（非主键点查）优先路由 Slave。
    pub read_from_replica: bool,
    /// Slave 延迟超此秒数则降级读 Master。
    pub replica_lag_threshold_sec: u64,
    /// 主键点查永远走 Master（避免读到旧数据）。
    pub force_master_for_primary_get: bool,
}

impl Default for ReadWriteSeparationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            read_from_replica: false,
            replica_lag_threshold_sec: 10,
            force_master_for_primary_get: true,
        }
    }
}

/// 广播查询熔断（design 9.2 / 9.8）：不带分片键的倒排检索保护。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BroadcastQueryConfig {
    /// 同时进行广播检索的最大并发数。
    pub max_concurrent: usize,
    /// 单次广播查询最大等待时间（ms）。
    pub timeout_ms: u64,
    /// true 时拒绝不带 DocId 的查询（纯主键场景，防广播慢查询）。
    pub reject_without_shard_key: bool,
    /// 网关全局 Term 缓存开关（design 9.9）。
    pub term_cache_enabled: bool,
    /// Term 缓存 TTL 兜底过期（秒，design 9.9 默认 5s，防脏读双保险）。
    pub term_cache_ttl_secs: u64,
    /// 某 Term 1 秒内写入超过此阈值 → 主动失效其全局缓存（design 9.9 默认 100）。
    pub term_cache_invalid_threshold: u32,
    /// Term 缓存最大条目数（LRU）。
    pub term_cache_max_entries: usize,
}

impl Default for BroadcastQueryConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 10,
            timeout_ms: 30000,
            reject_without_shard_key: false,
            term_cache_enabled: true,
            term_cache_ttl_secs: 5,
            term_cache_invalid_threshold: 100,
            term_cache_max_entries: 10_000,
        }
    }
}

/// Compaction 看门狗（design 14.2 / 14.5）：写停滞假死检测与自愈。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CompactionConfig {
    /// L0 数量在该时间内无减少 → 判定 Compaction 假死（默认 60s）。
    pub stall_timeout_secs: u64,
    /// 连续假死次数上限，超出主动退出进程（由外部重启）。
    pub max_consecutive_failures: u32,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            stall_timeout_secs: 60,
            max_consecutive_failures: 3,
        }
    }
}

/// 内嵌 Sidecar 进程探针（design 14.4 / 14.5）：文件锁心跳兜底。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SidecarConfig {
    /// 探针心跳间隔（秒，默认 5s）。
    pub ping_interval_sec: u64,
    /// 连续丢 ping 上限（默认 3），超出判定主进程死锁。
    pub max_missed_pings: u32,
}

impl Default for SidecarConfig {
    fn default() -> Self {
        Self {
            ping_interval_sec: 5,
            max_missed_pings: 3,
        }
    }
}

/// Redis 外部缓存（design 21，阶段 2）：Cache-Aside + Write-Invalidate + 熔断降级。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CacheExternalConfig {
    /// 是否启用外部 Redis 缓存（默认关闭，保持单机纯净）。
    pub enabled: bool,
    /// Redis 地址（单机取第一个；sentinel/cluster 留阶段 2.5）。
    pub redis_addrs: Vec<String>,
    /// 缓存 TTL（秒，建议 60~600）。
    pub ttl_seconds: u64,
    /// 是否缓存空值（防穿透，null_ttl_seconds）。
    pub cache_null_values: bool,
    /// 空值缓存 TTL（秒）。
    pub null_ttl_seconds: u64,
    /// 写策略："invalidate"（推荐）/ "double_delete" / "none"。
    pub write_policy: String,
    /// Redis 操作超时（毫秒），超时自动降级。
    pub timeout_ms: u64,
    /// 失败重试次数。
    pub retry_attempts: u32,
    /// 写入后是否主动预热（增加写入延迟）。
    pub preheat_on_write: bool,
}

impl Default for CacheExternalConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            redis_addrs: vec!["127.0.0.1:6379".into()],
            ttl_seconds: 300,
            cache_null_values: false,
            null_ttl_seconds: 60,
            write_policy: "invalidate".into(),
            timeout_ms: 100,
            retry_attempts: 3,
            preheat_on_write: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        let mut cfg = Config::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.hotcache.eviction_policy, "lfu");
        assert_eq!(cfg.sstable.compression, "zstd");
        assert_eq!(cfg.storage.l0_stall_threshold, 8);
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
        assert!(!cfg.read_write_separation.enabled, "standalone 必须强制关闭读写分离");
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
}
