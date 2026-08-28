//! 看门狗：查询超时熔断 + OOM 内存限流（MVP，development 步骤 13 / design 14.1.1）
//! + 写停滞假死检测自愈（阶段 2，design 14.2）+ Sidecar 文件锁心跳（阶段 2，design 14.4）。
//!
//! - **查询超时熔断**：`QueryGuard` 记录查询截止时间，执行器逐步检查，
//!   超时即返回 `QueryTooExpensive`，防止慢查询拖垮引擎；
//! - **OOM 内存限流**：`MemoryGuardian` 按 `memory.watermark_high`（软水位）
//!   与 `memory.watermark_stall`（硬水位）分级：
//!   - 低于软水位 → 正常写入；
//!   - 软水位 ~ 硬水位 → 写限流（返回 Stalled 信号）；
//!   - 达硬水位 → 紧急熔断（`MemoryOverload`，拒绝新写入）。
//! - **写停滞看门狗**（14.2）：`StallWatchdog` 监控 L0 文件数，`stall_timeout` 内无减少
//!   判 Compaction 假死 → 自愈（中断 Compaction + 重置调度器）；连续达上限 → 主动退出；
//! - **Sidecar 心跳**（14.4）：`HeartbeatSidecar` 线程写心跳文件，`HeartbeatProbe`
//!   按 `interval × max_missed` 判定主进程死锁（独立子进程拉起留阶段 2 后续）。

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

// ============ 写停滞看门狗（design 14.2，阶段 2）============

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
                    let failures =
                        self.consecutive_failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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

// ============ Sidecar 文件锁心跳（design 14.4，阶段 2）============

/// 心跳探针侧判定：主进程心跳文件是否新鲜。
///
/// 主进程每 `interval` 写一次心跳文件（unix 毫秒时间戳）；探针（独立线程 / 子进程）
/// 读取文件，若距今超过 `interval × max_missed` 则判定主进程死锁，触发重启。
pub struct HeartbeatProbe {
    path: std::path::PathBuf,
    interval: Duration,
    max_missed_pings: u32,
}

impl HeartbeatProbe {
    pub fn new(path: std::path::PathBuf, interval: Duration, max_missed_pings: u32) -> Self {
        Self {
            path,
            interval,
            max_missed_pings,
        }
    }

    /// 心跳是否新鲜（主进程存活）。
    pub fn is_alive(&self) -> bool {
        let now_ms = now_millis();
        let last_ms = std::fs::read_to_string(&self.path)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok());
        match last_ms {
            Some(last) => {
                let tolerance =
                    self.interval.as_millis() as u64 * self.max_missed_pings as u64;
                now_ms.saturating_sub(last) <= tolerance
            }
            None => false,
        }
    }
}

/// Sidecar 心跳线程：主进程内启动，每 `interval` 更新心跳文件。
pub struct HeartbeatSidecar {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl HeartbeatSidecar {
    /// 启动心跳线程。`path` 为心跳文件路径，`interval` 为心跳间隔。
    pub fn start(path: std::path::PathBuf, interval: Duration) -> Self {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop2 = stop.clone();
        let handle = std::thread::spawn(move || {
            while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                if let Some(p) = path.parent() {
                    let _ = std::fs::create_dir_all(p);
                }
                let _ = std::fs::write(&path, now_millis().to_string());
                std::thread::sleep(interval);
            }
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    /// 停止心跳线程（Drop 语义由调用方保证）。
    pub fn stop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for HeartbeatSidecar {
    fn drop(&mut self) {
        self.stop();
    }
}

/// 当前 unix 毫秒时间戳（探针判定新鲜度用）。
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

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

    // ---- Sidecar 文件锁心跳（design 14.4）----

    #[test]
    fn heartbeat_sidecar_keeps_probe_alive() {
        let dir = tempfile::tempdir().unwrap();
        let hb_path = dir.path().join("heartbeat.hb");
        let mut sidecar = HeartbeatSidecar::start(hb_path.clone(), Duration::from_millis(10));
        let probe = HeartbeatProbe::new(hb_path.clone(), Duration::from_millis(10), 3);
        // 等前几次心跳写入
        std::thread::sleep(Duration::from_millis(50));
        assert!(probe.is_alive(), "心跳线程运行中探针应判定存活");
        // 停止心跳 → 超过 max_missed × interval 后判定死锁
        sidecar.stop();
        std::thread::sleep(Duration::from_millis(60));
        assert!(!probe.is_alive(), "心跳停止后探针应判定死锁");
    }

    #[test]
    fn heartbeat_probe_missing_file_is_dead() {
        let dir = tempfile::tempdir().unwrap();
        let probe = HeartbeatProbe::new(
            dir.path().join("nope.hb"),
            Duration::from_millis(10),
            3,
        );
        assert!(!probe.is_alive(), "心跳文件不存在应判定死锁");
    }
}
