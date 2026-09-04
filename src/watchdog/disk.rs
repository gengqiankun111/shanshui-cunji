//! 磁盘空间看门狗（P52）：按剩余空间水位分级（预警 → 限流/熔断），带采样缓存
//! （避免写路径每次 syscall 查询可用空间）。

use std::time::{Duration, Instant};

use crate::config::model::Config;
use crate::error::{Error, Result};

/// 磁盘空间状态分级（P52）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskStatus {
    /// 剩余空间充足（> warn）。
    Normal,
    /// 预警（warn ~ throttle 之间）：记录计数，写放行（回收由调用方触发）。
    Throttled,
    /// 限流 / 熔断（<= throttle）：拒绝新写入，只读保持。
    Stalled,
}

/// 磁盘空间看门狗（P52）：按剩余空间水位分级（预警 → 限流/熔断），带采样缓存
/// （避免写路径每次 syscall 查询可用空间）。
pub struct DiskGuardian {
    warn_ratio: f64,
    throttle_ratio: f64,
    stall_ratio: f64,
    /// 熔断绝对下限（字节）：剩余同时低于 stall_ratio 与绝对量才熔断。
    stall_min_bytes: u64,
    sample_secs: Duration,
    /// 采样缓存：(上次采样时刻, 可用字节, 总量字节)。
    cache: std::sync::Mutex<Option<(Instant, u64, u64)>>,
    warn_count: std::sync::atomic::AtomicU64,
    throttled_count: std::sync::atomic::AtomicU64,
    stalled_count: std::sync::atomic::AtomicU64,
}

impl DiskGuardian {
    pub fn new(cfg: &Config) -> Self {
        Self {
            warn_ratio: cfg.watchdog.disk_warn_ratio,
            throttle_ratio: cfg.watchdog.disk_throttle_ratio,
            stall_ratio: cfg.watchdog.disk_stall_ratio,
            stall_min_bytes: (cfg.watchdog.disk_stall_min_mb as u64) * 1024 * 1024,
            sample_secs: Duration::from_secs(cfg.watchdog.disk_sample_secs),
            cache: std::sync::Mutex::new(None),
            warn_count: std::sync::atomic::AtomicU64::new(0),
            throttled_count: std::sync::atomic::AtomicU64::new(0),
            stalled_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// 采样 data_dir 所在文件系统的剩余空间水位并分级（带间隔缓存）。
    /// syscall 失败（路径不可用等）→ 返回 Normal（不因检测失败阻塞写）。
    pub fn sample(&self, data_dir: &std::path::Path) -> Result<DiskStatus> {
        let now = Instant::now();
        let cached = {
            let c = self.cache.lock().unwrap();
            c.filter(|(t, _, _)| now.duration_since(*t) < self.sample_secs)
                .map(|(_, a, b)| (a, b))
        };
        let (avail, total) = match cached {
            Some(v) => v,
            None => {
                let v = disk_space::space_info(data_dir)
                    .map_err(Error::from)
                    .unwrap_or((u64::MAX, u64::MAX));
                *self.cache.lock().unwrap() = Some((now, v.0, v.1));
                v
            }
        };
        Ok(self.classify(avail, total))
    }

    /// 按可用/总量分级（核心逻辑，可单测）。
    /// 熔断 = 剩余比例 ≤ stall_ratio **且** 剩余绝对字节 < stall_min_bytes
    /// （防止小比例但剩余空间仍充裕的盘误熔断）。
    pub fn classify(&self, avail: u64, total: u64) -> DiskStatus {
        if total == 0 {
            return DiskStatus::Normal;
        }
        let ratio = avail as f64 / total as f64;
        if ratio <= self.stall_ratio && avail < self.stall_min_bytes {
            self.stalled_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            DiskStatus::Stalled
        } else if ratio <= self.throttle_ratio {
            self.throttled_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            DiskStatus::Throttled
        } else if ratio <= self.warn_ratio {
            self.warn_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            DiskStatus::Throttled
        } else {
            DiskStatus::Normal
        }
    }

    pub fn warn_count(&self) -> u64 {
        self.warn_count.load(std::sync::atomic::Ordering::Relaxed)
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

    fn disk() -> DiskGuardian {
        DiskGuardian::new(&Config::default())
    }

    // ---------- P52：磁盘空间看门狗 ----------

    #[test]
    fn disk_classify_levels_by_ratio() {
        let d = disk();
        // 默认水位：warn=0.20 throttle=0.10 stall=0.05
        assert_eq!(d.classify(0, 10_000), DiskStatus::Stalled, "0% 剩余 → 熔断");
        assert_eq!(d.classify(400, 10_000), DiskStatus::Stalled, "4% → 熔断");
        assert_eq!(d.classify(700, 10_000), DiskStatus::Throttled, "7% → 限流");
        assert_eq!(d.classify(1_500, 10_000), DiskStatus::Throttled, "15% → 预警");
        assert_eq!(d.classify(5_000, 10_000), DiskStatus::Normal, "50% → 正常");
        assert_eq!(d.classify(0, 0), DiskStatus::Normal, "总量 0 → 保守放行");
    }

    #[test]
    fn disk_sample_caches_repeated_query() {
        let d = disk();
        let dir = tempfile::tempdir().unwrap();
        // 采样缓存：1s 内连续调用返回一致结果（真实磁盘状态可能为任意分级，只验证一致性）
        let s1 = d.sample(dir.path()).unwrap();
        let s2 = d.sample(dir.path()).unwrap();
        assert_eq!(s1, s2);
    }
}
