//! Task-026 Per-CPU WAL（research/range-scan-percpu-wal-design.md §二）。
//!
//! 目标：多核高频非事务写的 WAL fsync 锁竞争摊薄——现有组提交（M8）为**单后台线程 + 各 CF
//! `WalBackend`（Mutex）全局攒批一次 fsync**；Per-CPU 改为 N 个队列（每队列独立后台消费线程 +
//! 独立 `wal-{queue}-{gseq_start}.log`），写入口按当前 CPU 路由入队（超界/未绑核 → 轮询回退），
//! 队列消费线程按组提交窗口批量写盘并 fsync。
//!
//! **阶段1（本文件，2026-09-05）**：配置解析 + 队列数解析 + 路由 + 队列深度/积压状态与监控
//! 快照（供 SHOW STATUS）。写入线程/文件布局/恢复归并（阶段2/3）后续落地；`per_cpu_enabled`
//! 当前默认 **false**（安全回退现有全局组提交），核心完成并经全量回归后再翻转默认 true。
//!
//! 与现有架构的衔接（不冲突）：全局 `gseq`（Arc<AtomicU64>）语义沿用；跨队列最终写盘可交错，
//! 恢复按文件名解析队列与 gseq 范围后**gseq 全局归并回放**；Manifest `checkpoint_gseq` 判定
//! 失败写跳过编号（洞）不回放；关闭/队列 0 时回退全局模式（等价现状）。

use std::sync::atomic::{AtomicU64, Ordering};

/// Per-CPU WAL 运行配置（Engine 持有；阶段1 仅解析 + 路由，写路径接线见阶段2）。
pub(crate) struct PerCpuWal {
    /// 是否启用（true 才建队列；false = 走现有全局组提交/逐条 fsync）。
    pub enabled: bool,
    /// 已解析队列数（1..=64）：0/未启用时 = 1（等效单队列，便于回退语义统一）。
    pub queues: usize,
    /// 单队列最大缓冲条目数（满时写侧背压）。
    pub depth: usize,
    /// 每队列组提交 fsync 窗口（µs）。
    pub window_us: u64,
    /// 轮询回退计数（未绑核/CPU 超界时 round-robin）。
    rr: AtomicU64,
    /// 各队列积压条目计数（写侧入队 +1，消费出队 -1；监控 SHOW STATUS 用）。
    pub(crate) depth_now: Vec<AtomicU64>,
    /// 各队列累计消费（写盘）条目数（监控）。
    pub(crate) consumed: Vec<AtomicU64>,
}

/// CPU 核数上限（research §二 per_cpu_queues：0 = CPU 核数（上限 64））。
const MAX_QUEUES: usize = 64;

impl PerCpuWal {
    /// 从配置解析（阶段1：仅解析，不启动线程；启用但未接线写路径前保持不落地线程）。
    pub fn resolve(cfg: &crate::config::Config) -> Self {
        let enabled = cfg.storage.per_cpu_enabled;
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let queues = if !enabled {
            1
        } else if cfg.storage.per_cpu_queues == 0 {
            cores.clamp(1, MAX_QUEUES)
        } else {
            cfg.storage.per_cpu_queues.clamp(1, MAX_QUEUES)
        };
        let depth = cfg.storage.per_cpu_queue_depth.max(1);
        let window_us = cfg.storage.per_cpu_batch_window_us.max(1);
        PerCpuWal {
            enabled,
            queues,
            depth,
            window_us,
            rr: AtomicU64::new(0),
            depth_now: (0..queues).map(|_| AtomicU64::new(0)).collect(),
            consumed: (0..queues).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// 路由：优先当前 CPU（未超界），否则轮询回退（`rr % queues`）。
    /// disabled（queues=1）恒返回 0（与全局组提交路径语义一致，路由为零开销）。
    pub fn route(&self, cpu_hint: Option<usize>) -> usize {
        if self.queues <= 1 {
            return 0;
        }
        if let Some(c) = cpu_hint {
            if c < self.queues {
                return c;
            }
        }
        let r = self.rr.fetch_add(1, Ordering::Relaxed) as usize % self.queues;
        r
    }

    /// 当前 CPU（Linux 可用 `sched_getcpu` 感知 affinity；Windows 统一 None → 轮询回退）。
    /// 路由正确性不依赖 affinity（仅负载均衡质量），轮询 fallback 已覆盖未绑核场景。
    pub fn current_cpu() -> Option<usize> {
        None
    }

    /// 入队背压判定（阶段2 消费线程接线前不用）：队列满 = depth_now[q] >= depth。
    pub fn queue_full(&self, q: usize) -> bool {
        self.depth_now.get(q).map(|d| d.load(Ordering::Relaxed) >= self.depth as u64).unwrap_or(false)
    }

    /// 队列积压快照（监控 SHOW STATUS：队列深度/消费速率/积压）。
    pub fn status(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!("per_cpu_enabled={} queues={} depth={} window_us={}", self.enabled, self.queues, self.depth, self.window_us));
        for (q, (d, c)) in self.depth_now.iter().zip(self.consumed.iter()).enumerate() {
            s.push_str(&format!(" | q{q}:depth={} consumed={}", d.load(Ordering::Relaxed), c.load(Ordering::Relaxed)));
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn percpu_resolve_disabled_is_single_queue() {
        let c = Config::default();
        assert!(!c.storage.per_cpu_enabled, "阶段1 默认关闭（安全回退）");
        let w = PerCpuWal::resolve(&c);
        assert_eq!(w.queues, 1);
        assert_eq!(w.route(None), 0);
        assert_eq!(w.route(Some(0)), 0);
        assert_eq!(w.route(Some(5)), 0, "disabled 恒回队列 0");
    }

    #[test]
    fn percpu_resolve_enabled_auto_and_route() {
        let mut c = Config::default();
        c.storage.per_cpu_enabled = true;
        c.storage.per_cpu_queues = 0; // 自动
        let w = PerCpuWal::resolve(&c);
        assert!(w.queues >= 1 && w.queues <= 64, "自动 = 核数 clamp 1..=64，实际 {}", w.queues);
        if w.queues > 1 {
            assert_eq!(w.route(Some(0)), 0);
            assert!(w.route(Some(w.queues)) < w.queues, "超界回退轮询");
            assert!(w.route(None) < w.queues);
        }
        // 指定队列数截断
        c.storage.per_cpu_queues = 100;
        let w2 = PerCpuWal::resolve(&c);
        assert_eq!(w2.queues, 64);
        // 显式 1 = 单队列
        c.storage.per_cpu_queues = 1;
        let w3 = PerCpuWal::resolve(&c);
        assert_eq!(w3.queues, 1);
        assert_eq!(w3.route(None), 0);
    }

    #[test]
    fn percpu_queue_full_and_status() {
        let mut c = Config::default();
        c.storage.per_cpu_enabled = true;
        c.storage.per_cpu_queues = 2;
        c.storage.per_cpu_queue_depth = 4;
        let w = PerCpuWal::resolve(&c);
        assert!(!w.queue_full(0));
        w.depth_now[0].store(4, Ordering::Relaxed);
        assert!(w.queue_full(0));
        assert!(!w.queue_full(1));
        let s = w.status();
        assert!(s.contains("queues=2"));
        assert!(s.contains("q0:depth=4 consumed=0"), "{s}");
    }
}
