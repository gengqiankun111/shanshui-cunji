//! 配置模型与加载逻辑（design 13 / development 步骤 1）。
//!
//! 覆盖单机最小配置：server / memory / memtable / hotcache / blockcache / runtime / sstable / storage / inverted。
//!
//! 模块布局：本文件承载主 `Config` 结构（加载 / 热加载 / 环境变量覆盖 / 启动校验）与汇总
//! re-export；各配置区块按主题拆分为子模块（cache / cluster / inverted / join / optimizer /
//! runtime / server / sstable / storage / watchdog）。全部子模块公共项经 `pub use` 汇总到本
//! 模块，保持 `crate::config::model::*` 原有路径与可见性不变。

use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::error::{Error, Result};

mod cache;
mod cluster;
mod inverted;
mod join;
mod optimizer;
mod runtime;
mod server;
mod sstable;
mod storage;
mod watchdog;

pub use cache::*;
pub use cluster::*;
pub use inverted::*;
pub use join::*;
pub use optimizer::*;
pub use runtime::*;
pub use server::*;
pub use sstable::*;
pub use storage::*;
pub use watchdog::*;

#[cfg(test)]
mod tests;

/// 缓存软水位：达此比例触发主动淘汰，永不到达 100%（design 14.1.1）。
pub const DEFAULT_EVICTION_HIGH_WATER: f64 = 0.85;
/// 淘汰目标水位。
pub const DEFAULT_EVICTION_LOW_WATER: f64 = 0.75;
/// 内存硬上限占可用内存比例（启动校验红线，design 13）。
pub const MEMORY_BUDGET_RATIO: f64 = 0.7;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct Config {
    pub server: ServerConfig,
    pub memory: MemoryConfig,
    pub memtable: MemtableConfig,
    pub hotcache: HotCacheConfig,
    pub blockcache: BlockCacheConfig,
    pub runtime: RuntimeConfig,
    pub affinity: AffinityConfig,
    pub sstable: SstableConfig,
    pub storage: StorageConfig,
    pub inverted: InvertedConfig,
    pub join: JoinConfig,
    pub enrich: EnrichConfig,
    pub outbox: OutboxConfig,
    pub cluster: ClusterConfig,
    pub sharding: ShardingConfig,
    pub replication: ReplicationConfig,
    pub read_write_separation: ReadWriteSeparationConfig,
    pub broadcast_query: BroadcastQueryConfig,
    pub compaction: CompactionConfig,
    pub optimizer: OptimizerConfig,
    pub sidecar: SidecarConfig,
    pub cache_external: CacheExternalConfig,
    pub watchdog: WatchdogConfig,
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

    /// 配置热加载（design 7.4 / development 7.4 阶段 3）：重新读取、校验并原地替换。
    /// 返回变更的配置区块（供运行中服务输出 / 决定哪些组件需重建）。
    /// 失败时保持当前配置不变（热加载失败不破坏运行态）。
    pub fn reload(&mut self, path: &Path) -> Result<ReloadReport> {
        let fresh = Self::load(path)?;
        let changed = self.changed_sections(&fresh);
        *self = fresh;
        Ok(ReloadReport {
            applied: true,
            changed_sections: changed,
        })
    }

    /// 与另一配置比较，返回发生变更的顶层区块名（序列化后按顶层 key 对比）。
    fn changed_sections(&self, other: &Self) -> Vec<String> {
        let ser = |c: &Self| {
            serde_json::to_value(c)
                .ok()
                .and_then(|v| v.as_object().cloned())
                .unwrap_or_default()
        };
        let a = ser(self);
        let b = ser(other);
        let mut changed: Vec<String> = b
            .keys()
            .filter(|k| a.get(*k) != b.get(*k))
            .cloned()
            .collect();
        changed.sort();
        changed
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
                    self.cluster.internal_rpc_port = val.parse::<u16>().unwrap_or_else(|_| {
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
                    self.broadcast_query.max_concurrent =
                        parse_override("broadcast_query.max_concurrent", &val);
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
        if self.watchdog.disk_throttle_ratio >= self.watchdog.disk_warn_ratio
            || self.watchdog.disk_stall_ratio > self.watchdog.disk_throttle_ratio
            || self.watchdog.disk_warn_ratio <= 0.0
        {
            return Err(Error::Config(
                "watchdog 磁盘水位须 0 < warn，且 warn > throttle >= stall".into(),
            ));
        }
        if self.watchdog.cpu_query_limit == 0 || self.watchdog.disk_sample_secs == 0 {
            return Err(Error::Config(
                "watchdog.cpu_query_limit / disk_sample_secs 必须 > 0".into(),
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
        if !matches!(self.storage.wal_mode.as_str(), "append" | "ring") {
            return Err(Error::Config(format!(
                "storage.wal_mode 非法: {}（append / ring）",
                self.storage.wal_mode
            )));
        }
        if self.storage.wal_ring_size_mb == 0 {
            return Err(Error::Config("storage.wal_ring_size_mb 必须 > 0".into()));
        }
        if !(0..=2).contains(&self.storage.flush_log_at_trx_commit) {
            return Err(Error::Config(format!(
                "storage.flush_log_at_trx_commit 非法: {}（0/1/2，对齐 MySQL innodb_flush_log_at_trx_commit）",
                self.storage.flush_log_at_trx_commit
            )));
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
        if self.compaction.stall_timeout_secs == 0 || self.compaction.max_consecutive_failures == 0
        {
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

/// 配置热加载报告（design 7.4 / 阶段 3）。
#[derive(Debug, Clone)]
pub struct ReloadReport {
    pub applied: bool,
    /// 发生变更的顶层配置区块（如 ["hotcache", "sstable"]）。
    pub changed_sections: Vec<String>,
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