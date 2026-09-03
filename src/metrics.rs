//! Metrics（X 项，Prometheus 风格 `/metrics`）：引擎层计数器 + 操作延迟对数直方图 +
//! 网络层连接/语句计数 + Prometheus 文本格式渲染。
//!
//! 埋点分层（架构评审补充）：
//! - **引擎层**（engine.rs）：读写操作计数 + 延迟直方图、Compaction/Flush 次数；
//! - **列族层**（/metrics 时读取快照）：SST 文件数、L0 段数（`engine.stats()` 聚合）；
//! - **网络层**（db_adapter.rs）：活跃/累计连接数、语句计数。

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// 操作延迟对数桶上界（纳秒）：0.1ms / 1ms / 10ms / 100ms / 1s / 10s / +inf。
const BUCKETS_NS: [u64; 7] = [
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    u64::MAX,
];

/// Prometheus 风格指标集合（原子计数，跨线程读锁并行无竞争）。
#[derive(Debug, Default)]
pub struct Metrics {
    /// 读操作总数（get/get_at/scan 等）。
    pub read_ops: AtomicU64,
    /// 写操作总数（put/put_batch/delete 等）。
    pub write_ops: AtomicU64,
    /// Compaction 完成次数。
    pub compact_count: AtomicU64,
    /// 操作延迟累计（纳秒，直方图 _sum 用）。
    pub latency_sum_ns: AtomicU64,
    /// 操作延迟对数直方图桶计数。
    pub latency_buckets: [AtomicU64; 7],
    /// 当前活跃连接数（网络层）。
    pub active_conns: AtomicI64,
    /// 累计连接数（网络层）。
    pub total_conns: AtomicU64,
    /// 累计语句数（网络层，COM_QUERY）。
    pub statements: AtomicU64,
}

impl Metrics {
    /// 记录一次操作延迟（纳秒）到对应对数桶。
    pub fn record_latency(&self, ns: u64) {
        let idx = BUCKETS_NS.iter().position(|b| ns < *b).unwrap_or(6);
        self.latency_buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.latency_sum_ns.fetch_add(ns, Ordering::Relaxed);
    }

    /// Prometheus 文本格式渲染（`GET /metrics` 响应体）。
    /// `sst_files`/`l0_segments`/`mem_ratio`/`disk_ratio`/`flush_count` 由调用方从引擎
    /// 快照读取（flush_count 聚合自各列族 `flush_count()`）。
    pub fn render(
        &self,
        sst_files: u64,
        l0_segments: u64,
        mem_ratio: f64,
        disk_ratio: f64,
        flush_count: u64,
    ) -> String {
        let mut out = String::new();
        let mut counter = |name: &str, help: &str, value: u64| {
            out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"));
        };
        counter(
            "shanshui_read_ops_total",
            "读操作总数（get/get_at/scan 等）",
            self.read_ops.load(Ordering::Relaxed),
        );
        counter(
            "shanshui_write_ops_total",
            "写操作总数（put/put_batch/delete 等）",
            self.write_ops.load(Ordering::Relaxed),
        );
        counter(
            "shanshui_compaction_total",
            "Compaction 完成次数",
            self.compact_count.load(Ordering::Relaxed),
        );
        counter(
            "shanshui_flush_total",
            "MemTable 刷盘次数（各列族聚合）",
            flush_count,
        );
        counter(
            "shanshui_mysql_statements_total",
            "MySQL 语句总数（COM_QUERY）",
            self.statements.load(Ordering::Relaxed),
        );
        counter(
            "shanshui_mysql_connections_total",
            "MySQL 累计连接数",
            self.total_conns.load(Ordering::Relaxed),
        );
        // 操作延迟直方图（Prometheus histogram 语义：le 为累计桶）
        out.push_str(
            "# HELP shanshui_op_latency_ns 操作延迟对数直方图（纳秒）\n# TYPE shanshui_op_latency_ns histogram\n",
        );
        let mut cum = 0u64;
        for (i, b) in BUCKETS_NS.iter().enumerate() {
            cum += self.latency_buckets[i].load(Ordering::Relaxed);
            let le = if *b == u64::MAX {
                "+Inf".to_string()
            } else {
                b.to_string()
            };
            out.push_str(&format!(
                "shanshui_op_latency_ns_bucket{{le=\"{le}\"}} {cum}\n"
            ));
        }
        out.push_str(&format!(
            "shanshui_op_latency_ns_sum {}\n",
            self.latency_sum_ns.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "shanshui_op_latency_ns_count {}\n",
            self.latency_buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum::<u64>()
        ));
        // Gauge（当前状态，/metrics 时实时读取）
        let mut gauge = |name: &str, help: &str, value: String| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}\n"
            ));
        };
        gauge(
            "shanshui_sst_files",
            "SST 文件总数",
            sst_files.to_string(),
        );
        gauge(
            "shanshui_l0_segments",
            "L0 段数（主数据列族）",
            l0_segments.to_string(),
        );
        gauge(
            "shanshui_mem_ratio",
            "内存水位（0~1，OOM Guardian 输入）",
            format!("{mem_ratio:.4}"),
        );
        gauge(
            "shanshui_disk_ratio",
            "磁盘剩余空间占比（0~1）",
            format!("{disk_ratio:.4}"),
        );
        gauge(
            "shanshui_mysql_active_connections",
            "MySQL 当前活跃连接数",
            self.active_conns.load(Ordering::Relaxed).to_string(),
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_bucket_mapping() {
        let m = Metrics::default();
        m.record_latency(50_000); // 0.05ms → 桶 0
        m.record_latency(500_000); // 0.5ms → 桶 1
        m.record_latency(5_000_000); // 5ms → 桶 2
        m.record_latency(50_000_000); // 50ms → 桶 3
        m.record_latency(500_000_000); // 0.5s → 桶 4
        m.record_latency(5_000_000_000); // 5s → 桶 5
        m.record_latency(50_000_000_000); // 50s → 桶 6
        for (i, b) in m.latency_buckets.iter().enumerate() {
            assert_eq!(b.load(Ordering::Relaxed), 1, "桶 {i}");
        }
    }

    #[test]
    fn prometheus_render_wellformed() {
        let m = Metrics::default();
        m.read_ops.store(42, Ordering::Relaxed);
        m.record_latency(1_500_000);
        m.record_latency(2_500_000);
        let text = m.render(3, 1, 0.5, 0.8, 5);
        assert!(text.contains("# TYPE shanshui_read_ops_total counter"));
        assert!(text.contains("shanshui_read_ops_total 42"));
        assert!(text.contains("shanshui_l0_segments 1"));
        assert!(text.contains("shanshui_sst_files 3"));
        assert!(text.contains("shanshui_flush_total 5"));
        assert!(text.contains("shanshui_op_latency_ns_count 2"));
        // le=1ms 桶累计 = 0（两个样本都 ≥1ms），le=10ms 累计 = 2
        assert!(text.contains("shanshui_op_latency_ns_bucket{le=\"1000000\"} 0"));
        assert!(text.contains("shanshui_op_latency_ns_bucket{le=\"10000000\"} 2"));
        // 覆盖 +Inf 桶
        assert!(text.contains("shanshui_op_latency_ns_bucket{le=\"+Inf\"} 2"));
    }
}
