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
/// CPU 并发占用（P52）：`try_begin_query` 构造时携带 `CpuGuardian` Arc，drop 自动释放。
pub struct QueryGuard {
    deadline: Instant,
    query_id: u64,
    timeout: Duration,
    cpu_release: Option<std::sync::Arc<CpuGuardian>>,
}

impl QueryGuard {
    fn new(query_id: u64, timeout: Duration) -> Self {
        Self {
            deadline: Instant::now() + timeout,
            query_id,
            timeout,
            cpu_release: None,
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

impl Drop for QueryGuard {
    fn drop(&mut self) {
        if let Some(c) = &self.cpu_release {
            c.end();
        }
    }
}

/// 看门狗：管理查询超时熔断与内存限流。
pub struct Watchdog {
    memory: MemoryGuardian,
    query_timeout: Duration,
    next_query_id: std::sync::atomic::AtomicU64,
    disk: DiskGuardian,
    cpu: std::sync::Arc<CpuGuardian>,
}

impl Watchdog {
    pub fn new(cfg: &Config, query_timeout: Duration) -> Self {
        Self {
            memory: MemoryGuardian::new(cfg),
            query_timeout,
            next_query_id: std::sync::atomic::AtomicU64::new(1),
            disk: DiskGuardian::new(cfg),
            cpu: std::sync::Arc::new(CpuGuardian::new(cfg.watchdog.cpu_query_limit)),
        }
    }

    /// 开始一个查询，返回超时守卫。
    pub fn begin_query(&self) -> QueryGuard {
        let id = self
            .next_query_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        QueryGuard::new(id, self.query_timeout)
    }

    /// 开始一个查询（CPU 并发限制版）：active 查询数达上限 → `Stalled` 拒绝
    /// （防 CPU 风暴，代理信号 = 并发查询数）。守卫 drop 时自动释放占用。
    pub fn try_begin_query(&self) -> Result<QueryGuard> {
        self.cpu.try_begin()?;
        Ok(QueryGuard {
            deadline: Instant::now() + self.query_timeout,
            query_id: self
                .next_query_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            timeout: self.query_timeout,
            cpu_release: Some(self.cpu.clone()),
        })
    }

    /// 写路径统一入口（P52）：内存 + 磁盘分级检查。
    /// - 任一资源达硬水位（内存 stall / 磁盘 stall）→ Err（熔断拒绝写）；
    /// - 软水位（内存 throttled / 磁盘 throttled）→ Ok（限流信号由调用方降速，MVP 放行）；
    /// - 磁盘预警（warn）→ 记录计数，写放行。
    pub fn check_all(&self, mem_ratio: f64, data_dir: &std::path::Path) -> Result<()> {
        let _ = self.memory.check(mem_ratio)?; // 硬水位 Err(MemoryOverload)
        match self.disk.sample(data_dir)? {
            DiskStatus::Stalled => {
                return Err(Error::Stalled("磁盘剩余空间达熔断水位，拒绝新写入".into()))
            }
            _ => {}
        }
        Ok(())
    }

    /// 磁盘空间状态（admin status / 测试）。
    pub fn disk_status(&self, data_dir: &std::path::Path) -> Result<DiskStatus> {
        self.disk.sample(data_dir)
    }

    /// 当前 CPU 并发查询数（admin status）。
    pub fn cpu_active(&self) -> usize {
        self.cpu.active()
    }

    /// 内存检查（写入路径调用）。
    pub fn memory_check(&self, usage_ratio: f64) -> Result<MemoryStatus> {
        self.memory.check(usage_ratio)
    }

    pub fn memory(&self) -> &MemoryGuardian {
        &self.memory
    }

    pub fn disk(&self) -> &DiskGuardian {
        &self.disk
    }

    pub fn cpu(&self) -> &CpuGuardian {
        &self.cpu
    }
}

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

/// CPU 并发看门狗（P52）：active 查询数代理 CPU 压力，超限拒绝新查询
/// （防 CPU 风暴；物理并发由引擎/服务器线程模型决定，此处为逻辑上限）。
pub struct CpuGuardian {
    limit: usize,
    active: std::sync::atomic::AtomicUsize,
    rejected_count: std::sync::atomic::AtomicU64,
}

impl CpuGuardian {
    pub fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            active: std::sync::atomic::AtomicUsize::new(0),
            rejected_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// 尝试进入一个查询。达到上限 → `Stalled` 拒绝（计数）。
    pub fn try_begin(&self) -> Result<()> {
        loop {
            let cur = self.active.load(std::sync::atomic::Ordering::Relaxed);
            if cur >= self.limit {
                self.rejected_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(Error::Stalled(format!(
                    "并发查询数 {} 达上限 {}{}，拒绝新查询",
                    cur,
                    self.limit,
                    ""
                )));
            }
            if self
                .active
                .compare_exchange_weak(
                    cur,
                    cur + 1,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Relaxed,
                )
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    /// 查询结束释放占用（QueryGuard drop 自动调用）。
    pub fn end(&self) {
        self.active.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }

    pub fn active(&self) -> usize {
        self.active.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn rejected_count(&self) -> u64 {
        self.rejected_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn limit(&self) -> usize {
        self.limit
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
                let tolerance = self.interval.as_millis() as u64 * self.max_missed_pings as u64;
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

    // ---------- P52：磁盘空间看门狗 ----------

    #[test]
    fn disk_classify_levels_by_ratio() {
        let w = Watchdog::new(&cfg(), DEFAULT_QUERY_TIMEOUT);
        let d = w.disk();
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
        let w = Watchdog::new(&cfg(), DEFAULT_QUERY_TIMEOUT);
        let dir = tempfile::tempdir().unwrap();
        // 采样缓存：1s 内连续调用返回一致结果（真实磁盘状态可能为任意分级，只验证一致性）
        let s1 = w.disk_status(dir.path()).unwrap();
        let s2 = w.disk_status(dir.path()).unwrap();
        assert_eq!(s1, s2);
    }

    #[test]
    fn check_all_rejects_when_memory_stall() {
        let w = Watchdog::new(&cfg(), DEFAULT_QUERY_TIMEOUT);
        let dir = tempfile::tempdir().unwrap();
        // 内存硬水位 → 拒绝写（磁盘分级逻辑由 disk_classify_levels_by_ratio 覆盖）
        assert!(w.check_all(1.5, dir.path()).is_err());
    }

    // ---------- P52：CPU 并发看门狗 ----------

    #[test]
    fn cpu_limit_rejects_and_releases_on_drop() {
        let c = CpuGuardian::new(2);
        assert!(c.try_begin().is_ok());
        assert!(c.try_begin().is_ok());
        assert!(c.try_begin().is_err(), "达上限应拒绝");
        assert_eq!(c.active(), 2);
        assert_eq!(c.rejected_count(), 1);
        c.end();
        c.end();
        assert_eq!(c.active(), 0);
        assert!(c.try_begin().is_ok(), "释放后可再进入");
    }

    #[test]
    fn watchdog_try_begin_query_limits_concurrency() {
        let mut c = cfg();
        c.watchdog.cpu_query_limit = 1;
        let w = Watchdog::new(&c, DEFAULT_QUERY_TIMEOUT);
        let g1 = w.try_begin_query().unwrap();
        assert!(w.try_begin_query().is_err(), "并发超限应拒绝");
        drop(g1); // QueryGuard drop → CPU 槽释放
        assert!(w.try_begin_query().is_ok());
        assert!(w.cpu_active() <= 1);
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
        let probe = HeartbeatProbe::new(dir.path().join("nope.hb"), Duration::from_millis(10), 3);
        assert!(!probe.is_alive(), "心跳文件不存在应判定死锁");
    }
}
