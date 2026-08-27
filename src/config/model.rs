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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            memory: MemoryConfig::default(),
            memtable: MemtableConfig::default(),
            hotcache: HotCacheConfig::default(),
            blockcache: BlockCacheConfig::default(),
            runtime: RuntimeConfig::default(),
            sstable: SstableConfig::default(),
            storage: StorageConfig::default(),
            inverted: InvertedConfig::default(),
        }
    }
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

    /// 环境变量覆盖：`NOVOSDB__SECTION__KEY=VALUE`。
    fn apply_env_overrides(&mut self) {
        for (key, value) in std::env::vars() {
            let Some(rest) = key.strip_prefix("NOVOSDB__") else {
                continue;
            };
            let parts: Vec<&str> = rest.split("__").collect();
            let val = value.trim().to_string();
            match parts.as_slice() {
                ["HOTCACHE", "MAX_MEMORY_MB"] => self.hotcache.max_memory_mb = parse_override("hotcache.max_memory_mb", &val),
                ["HOTCACHE", "EVICTION_POLICY"] => self.hotcache.eviction_policy = val,
                ["BLOCKCACHE", "MAX_MEMORY_MB"] => self.blockcache.max_memory_mb = parse_override("blockcache.max_memory_mb", &val),
                ["SERVER", "LISTEN_ADDR"] => self.server.listen_addr = val,
                ["MEMORY", "WATERMARK_HIGH"] => self.memory.watermark_high = parse_override_f64("memory.watermark_high", &val),
                ["MEMORY", "WATERMARK_STALL"] => self.memory.watermark_stall = parse_override_f64("memory.watermark_stall", &val),
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
        if self.memory.watermark_high <= 0.0 || self.memory.watermark_stall < self.memory.watermark_high {
            return Err(Error::Config(
                "memory.watermark_high 须 > 0，且 watermark_stall 须 >= watermark_high".into(),
            ));
        }
        if self.memtable.max_size_mb == 0 {
            return Err(Error::Config("memtable.max_size_mb 必须 > 0".into()));
        }
        if !matches!(self.inverted.engine.as_str(), "hash" | "fst") {
            return Err(Error::Config(format!("inverted.engine 非法: {}", self.inverted.engine)));
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

fn parse_override_f64(name: &str, v: &str) -> f64 {
    v.parse::<f64>().unwrap_or_else(|_| {
        warn!("环境变量 {name} 解析失败，忽略");
        0.0
    })
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub listen_addr: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { listen_addr: "0.0.0.0:8080".into() }
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
        Self { watermark_high: 0.85, watermark_stall: 1.0 }
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
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            l0_stall_threshold: 8,
            ttl_days: None,
            data_dir: "./data".into(),
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
}

impl Default for InvertedConfig {
    fn default() -> Self {
        Self {
            engine: "hash".into(),
            max_memory_bytes: 12 * 1024 * 1024 * 1024,
            max_posting_scan: 1_000_000,
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
        std::env::set_var("NOVOSDB__HOTCACHE__MAX_MEMORY_MB", "2048");
        std::env::set_var("NOVOSDB__SERVER__LISTEN_ADDR", "127.0.0.1:9000");
        let mut cfg = Config::default();
        cfg.apply_env_overrides();
        std::env::remove_var("NOVOSDB__HOTCACHE__MAX_MEMORY_MB");
        std::env::remove_var("NOVOSDB__SERVER__LISTEN_ADDR");
        assert_eq!(cfg.hotcache.max_memory_mb, 2048);
        assert_eq!(cfg.server.listen_addr, "127.0.0.1:9000");
    }
}
