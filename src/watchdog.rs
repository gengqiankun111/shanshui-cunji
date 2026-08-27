//! 看门狗 MVP 子集：查询超时熔断 + OOM 内存限流（development 步骤 13 / design 14.1.1）。
//!
//! - **查询超时熔断**：`QueryGuard` 记录查询截止时间，执行器逐步检查，
//!   超时即返回 `QueryTooExpensive`，防止慢查询拖垮引擎；
//! - **OOM 内存限流**：`MemoryGuardian` 按 `memory.watermark_high`（软水位）
//!   与 `memory.watermark_stall`（硬水位）分级：
//!   - 低于软水位 → 正常写入；
//!   - 软水位 ~ 硬水位 → 写限流（返回 Stalled 信号）；
//!   - 达硬水位 → 紧急熔断（`MemoryOverload`，拒绝新写入）。
//!
//! 完整看门狗（写停滞假死检测 + Sidecar 探针）在阶段 2 补全（design 14.3）。

use std::time::{Duration, Instant};

use crate::config::model::Config;
use crate::error::{Error, Result};

/// 内存状态分级。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryStatus {
    /// 正常。
    Normal,
    /// 软水位：限流。
    Throttled,
}

/// OOM Guardian：按水位分级限流/熔断（design 14.1.1）。
pub struct MemoryGuardian {
    high_water: f64,
    stall_water: f64,
    /// 统计：被限流的写入次数。
    throttled_count: std::sync::atomic::AtomicU64,
    /// 统计：被熔断的写入次数。
    stalled_count: std::sync::atomic::AtomicU64,
}

impl MemoryGuardian {
    pub fn new(cfg: &Config) -> Self {
        Self {
            high_water: cfg.memory.watermark_high,
            stall_water: cfg.memory.watermark_stall,
            throttled_count: std::sync::atomic::AtomicU64::new(0),
            stalled_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// 内存用量检查：`usage_ratio` ∈ [0, 1]（当前占用 / 预算）。
    /// - `< high` → Ok(Normal)
    /// - `[high, stall)` → Ok(Throttled)（写限流信号，由调用方决定降速）
    /// - `>= stall` → Err(MemoryOverload)（紧急止损，拒绝新写入）
    pub fn check(&self, usage_ratio: f64) -> Result<MemoryStatus> {
        if usage_ratio >= self.stall_water {
            self.stalled_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(Error::MemoryOverload(format!(
                "内存使用率 {:.1}% 达硬水位 {:.1}%，拒绝新写入",
                usage_ratio * 100.0,
                self.stall_water * 100.0
            )));
        }
        if usage_ratio >= self.high_water {
            self.throttled_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(MemoryStatus::Throttled);
        }
        Ok(MemoryStatus::Normal)
    }

    pub fn throttled_count(&self) -> u64 {
        self.throttled_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn stalled_count(&self) -> u64 {
        self.stalled_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// 查询超时守卫：记录截止时刻，供执行器熔断检查。
pub struct QueryGuard {
    deadline: Instant,
    query_id: u64,
    timeout: Duration,
}

impl QueryGuard {
    fn new(query_id: u64, timeout: Duration) -> Self {
        Self {
            deadline: Instant::now() + timeout,
            query_id,
            timeout,
        }
    }

    pub fn query_id(&self) -> u64 {
        self.query_id
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// 是否已超时（执行器在循环/回表间隙检查，超时即熔断）。
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.deadline
    }

    /// 剩余时间。
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

/// 看门狗：管理查询超时熔断与内存限流。
pub struct Watchdog {
    memory: MemoryGuardian,
    query_timeout: Duration,
    next_query_id: std::sync::atomic::AtomicU64,
}

impl Watchdog {
    pub fn new(cfg: &Config, query_timeout: Duration) -> Self {
        Self {
            memory: MemoryGuardian::new(cfg),
            query_timeout,
            next_query_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// 开始一个查询，返回超时守卫。
    pub fn begin_query(&self) -> QueryGuard {
        let id = self
            .next_query_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        QueryGuard::new(id, self.query_timeout)
    }

    /// 内存检查（写入路径调用）。
    pub fn memory_check(&self, usage_ratio: f64) -> Result<MemoryStatus> {
        self.memory.check(usage_ratio)
    }

    pub fn memory(&self) -> &MemoryGuardian {
        &self.memory
    }
}

/// 默认查询超时。
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_millis(500);

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Config {
        Config::default()
    }

    #[test]
    fn memory_below_high_water_is_normal() {
        let w = Watchdog::new(&cfg(), DEFAULT_QUERY_TIMEOUT);
        assert_eq!(w.memory_check(0.5).unwrap(), MemoryStatus::Normal);
        assert_eq!(w.memory_check(0.84).unwrap(), MemoryStatus::Normal);
    }

    #[test]
    fn memory_in_soft_range_throttles() {
        let w = Watchdog::new(&cfg(), DEFAULT_QUERY_TIMEOUT);
        assert_eq!(w.memory_check(0.9).unwrap(), MemoryStatus::Throttled);
        assert!(w.memory().throttled_count() >= 1);
        assert_eq!(w.memory().stalled_count(), 0);
    }

    #[test]
    fn memory_at_stall_rejects_writes() {
        let w = Watchdog::new(&cfg(), DEFAULT_QUERY_TIMEOUT);
        let err = w.memory_check(1.0).unwrap_err();
        assert!(matches!(err, Error::MemoryOverload(_)));
        assert!(w.memory().stalled_count() >= 1);
    }

    #[test]
    fn query_guard_timeout_fires() {
        // 极小超时 + 短暂阻塞 → 熔断
        let w = Watchdog::new(&cfg(), Duration::from_millis(1));
        let guard = w.begin_query();
        std::thread::sleep(Duration::from_millis(10));
        assert!(guard.is_expired());
        assert!(guard.remaining() == Duration::ZERO);
    }

    #[test]
    fn query_guard_not_expired_immediately() {
        let w = Watchdog::new(&cfg(), Duration::from_secs(10));
        let guard = w.begin_query();
        assert!(!guard.is_expired());
        assert!(guard.remaining() > Duration::ZERO);
        assert!(guard.query_id() >= 1);
    }

    #[test]
    fn custom_watermarks_respected() {
        let mut c = cfg();
        c.memory.watermark_high = 0.5;
        c.memory.watermark_stall = 0.6;
        let w = Watchdog::new(&c, DEFAULT_QUERY_TIMEOUT);
        assert_eq!(w.memory_check(0.55).unwrap(), MemoryStatus::Throttled);
        assert!(matches!(
            w.memory_check(0.65),
            Err(Error::MemoryOverload(_))
        ));
    }
}
