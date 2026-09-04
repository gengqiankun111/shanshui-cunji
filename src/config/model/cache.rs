//! 缓存配置：热缓存 / 块缓存 / 外部 Redis 缓存（design 14.1.1 / 4.8 / 21）。

use serde::{Deserialize, Serialize};

use super::{DEFAULT_EVICTION_HIGH_WATER, DEFAULT_EVICTION_LOW_WATER};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
            // Ex-5.1（design 4.8）：SSD 原生 4KB 块（对齐 SSD 页），点查读放大 16×→4×。
            // 压缩率略降（小块 zstd 窗口小，重复数据下压缩后体积 +~30%），SSD 空间便宜可接受。
            block_size_kb: 4,
            eviction_high_water: DEFAULT_EVICTION_HIGH_WATER,
        }
    }
}

/// Redis 外部缓存（design 21，阶段 2）：Cache-Aside + Write-Invalidate + 熔断降级。
#[derive(Debug, Clone, Serialize, Deserialize)]
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