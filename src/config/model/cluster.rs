//! 集群与分布式配置：节点 / 分片 / 复制 / 读写分离 / 广播查询熔断（design 9.x）。

use serde::{Deserialize, Serialize};

/// 集群节点（design 9.8）：节点标识与内部 RPC。
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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