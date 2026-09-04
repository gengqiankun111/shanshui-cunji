//! 看门狗与自愈配置：磁盘/CPU 水位、Compaction 假死检测、Sidecar 探针（design 14.x）。

use serde::{Deserialize, Serialize};

/// 看门狗扩展配置（P52 落地：CPU / 硬盘超限三级响应）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WatchdogConfig {
    /// 磁盘剩余空间预警水位（剩余/总量 低于此值 → 预警 + 触发回收）。
    pub disk_warn_ratio: f64,
    /// 磁盘剩余空间限流水位（低于 → 拒绝新写入，只读保持）。
    pub disk_throttle_ratio: f64,
    /// 磁盘剩余空间熔断水位（低于 → 强制只读，返回 Stalled）。
    pub disk_stall_ratio: f64,
    /// 磁盘熔断绝对下限（MB）：剩余空间同时低于 stall_ratio 且低于此绝对量才熔断
    /// （避免小比例但剩余空间仍充裕的盘误熔断；对齐 MySQL 预留空间思想）。
    pub disk_stall_min_mb: usize,
    /// 磁盘可用空间采样间隔（秒）：避免写路径每次 syscall 查询。
    pub disk_sample_secs: u64,
    /// CPU 并发查询上限（代理信号：active 查询数超限 → Stalled 拒绝新查询）。
    pub cpu_query_limit: usize,
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self {
            disk_warn_ratio: 0.20,
            disk_throttle_ratio: 0.10,
            disk_stall_ratio: 0.05,
            disk_stall_min_mb: 1024,
            disk_sample_secs: 1,
            cpu_query_limit: 64,
        }
    }
}

/// Compaction 看门狗（design 14.2 / 14.5）：写停滞假死检测与自愈。
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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