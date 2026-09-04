//! CPU 并发看门狗（P52）：active 查询数代理 CPU 压力，超限拒绝新查询
//! （防 CPU 风暴；物理并发由引擎/服务器线程模型决定，此处为逻辑上限）。

use crate::error::{Error, Result};

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
