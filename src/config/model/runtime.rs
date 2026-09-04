//! 运行时与资源配置：内存水位 / 异步运行时 / CPU 绑核（design 13 / Ex-7.2）。

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// CPU 绑核（Ex-7.2，design_extension v0.5 第 12.2）：网络/计算/IO 三池物理核分区。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AffinityConfig {
    /// 是否启用绑核（默认 true：多核机器自动分区；1 核机器自动退化为 no-op）。
    pub enabled: bool,
    /// 网络线程绑定的核列表（空 = 自动：核 0 起最低编号核）。
    pub network_cores: Vec<usize>,
    /// 计算线程（Compaction 并行等）绑定的核列表（空 = 自动：中间段核）。
    pub compute_cores: Vec<usize>,
    /// IO 后台线程（组提交刷盘等）绑定的核列表（空 = 自动：尾部核）。
    pub io_cores: Vec<usize>,
}

impl Default for AffinityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            network_cores: Vec::new(),
            compute_cores: Vec::new(),
            io_cores: Vec::new(),
        }
    }
}