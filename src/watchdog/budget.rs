//! OOM 内存预算（水位）分级限流/熔断（design 14.1.1，MVP）。
//!
//! `MemoryGuardian` 按 `memory.watermark_high`（软水位）与 `memory.watermark_stall`
//! （硬水位）分级：
//! - 低于软水位 → 正常写入（`MemoryStatus::Normal`）；
//! - 软水位 ~ 硬水位 → 写限流（`MemoryStatus::Throttled`，由调用方降速）；
//! - 达硬水位 → 紧急熔断（`Error::MemoryOverload`，拒绝新写入）。

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

#[cfg(test)]
mod tests {
    use super::*;

    fn mem() -> MemoryGuardian {
        MemoryGuardian::new(&Config::default())
    }

    #[test]
    fn memory_below_high_water_is_normal() {
        let g = mem();
        assert_eq!(g.check(0.5).unwrap(), MemoryStatus::Normal);
        assert_eq!(g.check(0.84).unwrap(), MemoryStatus::Normal);
    }

    #[test]
    fn memory_in_soft_range_throttles() {
        let g = mem();
        assert_eq!(g.check(0.9).unwrap(), MemoryStatus::Throttled);
        assert!(g.throttled_count() >= 1);
        assert_eq!(g.stalled_count(), 0);
    }

    #[test]
    fn memory_at_stall_rejects_writes() {
        let g = mem();
        let err = g.check(1.0).unwrap_err();
        assert!(matches!(err, Error::MemoryOverload(_)));
        assert!(g.stalled_count() >= 1);
    }

    #[test]
    fn custom_watermarks_respected() {
        let mut c = Config::default();
        c.memory.watermark_high = 0.5;
        c.memory.watermark_stall = 0.6;
        let g = MemoryGuardian::new(&c);
        assert_eq!(g.check(0.55).unwrap(), MemoryStatus::Throttled);
        assert!(matches!(
            g.check(0.65),
            Err(Error::MemoryOverload(_))
        ));
    }
}
