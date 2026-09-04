//! 写停滞看门狗（design 14.2，阶段 2）：监控 L0 文件数量，`stall_timeout` 内无减少
//! 判 Compaction 假死 → 自愈（中断 Compaction + 重置调度器，由调用方执行）；
//! 连续假死达上限 → 主动退出进程（由外部 systemd / Sidecar 重启）。

use std::time::{Duration, Instant};

/// 写停滞看门狗判定结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StallAction {
    /// L0 低于阈值，健康。
    Healthy,
    /// L0 超阈值且持续未减少（观察中）。
    Stalled,
    /// 判定 Compaction 假死 → 触发自愈（中断 Compaction + 重置调度器）。
    Deadlock,
    /// 连续假死超限 → 主动退出进程（由外部 systemd / Sidecar 重启）。
    FatalExit,
}

/// 写停滞看门狗（design 14.2）：监控 L0 文件数量，`stall_timeout` 内无减少判假死。
pub struct StallWatchdog {
    l0_threshold: usize,
    stall_timeout: Duration,
    max_consecutive: u32,
    /// 当前停滞起始时刻（未停滞为 None）。
    stall_since: std::sync::Mutex<Option<Instant>>,
    /// 连续假死次数。
    consecutive_failures: std::sync::atomic::AtomicU32,
    /// 统计：已触发自愈次数。
    heal_count: std::sync::atomic::AtomicU64,
}

impl StallWatchdog {
    /// 从配置构造（design 14.5 `compaction.stall_timeout_secs` / `max_consecutive_failures`）。
    pub fn from_config(cfg: &crate::config::CompactionConfig, l0_threshold: usize) -> Self {
        Self::new(
            Duration::from_secs(cfg.stall_timeout_secs),
            cfg.max_consecutive_failures,
            l0_threshold,
        )
    }

    /// 直接构造（测试可传毫秒级超时）。
    pub fn new(stall_timeout: Duration, max_consecutive: u32, l0_threshold: usize) -> Self {
        Self {
            l0_threshold,
            stall_timeout,
            max_consecutive,
            stall_since: std::sync::Mutex::new(None),
            consecutive_failures: std::sync::atomic::AtomicU32::new(0),
            heal_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// 周期采样（后台探活线程调用）。传入当前 L0 文件数。
    pub fn sample(&self, l0_count: usize) -> StallAction {
        if l0_count < self.l0_threshold {
            *self.stall_since.lock().unwrap() = None;
            return StallAction::Healthy;
        }
        let now = Instant::now();
        let mut since = self.stall_since.lock().unwrap();
        match *since {
            None => {
                *since = Some(now);
                StallAction::Stalled
            }
            Some(t) => {
                if now.duration_since(t) < self.stall_timeout {
                    StallAction::Stalled
                } else {
                    // 判定假死：自愈（中断 Compaction + 重置调度器由调用方执行），重新计时
                    let failures = self
                        .consecutive_failures
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                        + 1;
                    self.heal_count
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    *since = Some(now);
                    if failures >= self.max_consecutive {
                        StallAction::FatalExit
                    } else {
                        StallAction::Deadlock
                    }
                }
            }
        }
    }

    /// 连续假死次数。
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 已触发自愈次数。
    pub fn heal_count(&self) -> u64 {
        self.heal_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- 写停滞看门狗（design 14.2）----

    #[test]
    fn stall_watchdog_healthy_below_threshold() {
        let w = StallWatchdog::new(Duration::from_secs(60), 3, 8);
        // 连续健康采样
        for _ in 0..3 {
            assert_eq!(w.sample(3), StallAction::Healthy);
        }
        assert_eq!(w.consecutive_failures(), 0);
        assert_eq!(w.heal_count(), 0);
    }

    #[test]
    fn stall_watchdog_detects_deadlock_then_fatal() {
        // 30ms 超时模拟停滞观察
        let w = StallWatchdog::new(Duration::from_millis(30), 3, 8);
        // 首次超阈值 → 观察中
        assert_eq!(w.sample(9), StallAction::Stalled);
        // 停滞期间（未超时）→ 仍 Stalled
        assert_eq!(w.sample(9), StallAction::Stalled);
        // 超过超时后 → 判定假死（Deadlock），连续计数 1
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(w.sample(9), StallAction::Deadlock);
        assert_eq!(w.consecutive_failures(), 1);
        assert_eq!(w.heal_count(), 1);
        // 再次假死 → 第 2 次 Deadlock
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(w.sample(9), StallAction::Deadlock);
        assert_eq!(w.consecutive_failures(), 2);
        // 第 3 次 → FatalExit
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(w.sample(9), StallAction::FatalExit);
        assert_eq!(w.consecutive_failures(), 3);
    }

    #[test]
    fn stall_watchdog_recovery_clears_episode() {
        let w = StallWatchdog::new(Duration::from_millis(30), 3, 8);
        assert_eq!(w.sample(9), StallAction::Stalled);
        // L0 回落 → 健康，停滞状态清除
        assert_eq!(w.sample(2), StallAction::Healthy);
        assert_eq!(w.sample(3), StallAction::Healthy);
        assert_eq!(w.consecutive_failures(), 0);
        // 重新停滞要重新计时（首个样本仍为 Stalled 而非 Deadlock）
        assert_eq!(w.sample(9), StallAction::Stalled);
    }
}
