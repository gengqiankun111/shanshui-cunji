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
//!
//! 包内按主题拆分子模块（重构后 `crate::watchdog::*` 公开路径不变）：
//! - `budget`：OOM 内存预算（水位）分级限流/熔断 —— `MemoryGuardian` / `MemoryStatus`；
//! - `disk`：磁盘剩余空间熔断 —— `DiskGuardian` / `DiskStatus`；
//! - `cpu`：CPU 并发上限守卫 —— `CpuGuardian`；
//! - `stall`：写停滞假死检测自愈 —— `StallWatchdog` / `StallAction`；
//! - `heartbeat`：Sidecar 文件锁心跳 —— `HeartbeatSidecar` / `HeartbeatProbe`；
//! - 本模块：主类型 `Watchdog` / `QueryGuard` / `DEFAULT_QUERY_TIMEOUT`。

mod budget;
mod cpu;
mod disk;
mod heartbeat;
mod stall;

pub use budget::{MemoryGuardian, MemoryStatus};
pub use cpu::CpuGuardian;
pub use disk::{DiskGuardian, DiskStatus};
pub use heartbeat::{HeartbeatProbe, HeartbeatSidecar};
pub use stall::{StallAction, StallWatchdog};

use std::time::{Duration, Instant};

use crate::config::model::Config;
use crate::error::{Error, Result};

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

/// 默认查询超时。
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_millis(500);

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Config {
        Config::default()
    }

    // ---------- Watchdog 聚合：内存 + 磁盘 + CPU + 查询超时守卫 ----------

    #[test]
    fn check_all_rejects_when_memory_stall() {
        let w = Watchdog::new(&cfg(), DEFAULT_QUERY_TIMEOUT);
        let dir = tempfile::tempdir().unwrap();
        // 内存硬水位 → 拒绝写（磁盘分级逻辑由 disk 模块 disk_classify_levels_by_ratio 覆盖）
        assert!(w.check_all(1.5, dir.path()).is_err());
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
}
