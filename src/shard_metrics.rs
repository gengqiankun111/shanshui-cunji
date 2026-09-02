//! 分片级可观测（10 亿库扩展阶段 D，design-10b-extension.md §6 阶段 D）。
//!
//! 分片 Metrics + docid 水位上限预警：
//! - **每分片**：docid watermark（gauge）、写入/读取计数（counter，分片负载分布）；
//! - **上限预警**：watermark / LOCAL_CAPACITY（1<<40）≥ 80% → Warn、≥ 90% → Critical
//!   （运维告警；10 亿库每分片 1 亿 = 0.009%，余量充足，防分配器失控提前发现）；
//! - **Prometheus 渲染**：分片级指标文本输出（`shard="N"` label），接 `GET /metrics`。
//!
//! 挂载：Engine `shard_metrics` 字段（默认 None，分片部署时 `attach_shard_metrics(n)`）。

use std::sync::atomic::{AtomicU64, Ordering};

/// 分片内 local_id 容量（1<<40，对齐 docid_alloc）。
pub const SHARD_LOCAL_CAPACITY: u64 = 1u64 << 40;
/// 预警阈值（Warn 80% / Critical 90%）。
pub const WARN_RATIO: f64 = 0.8;
pub const CRIT_RATIO: f64 = 0.9;

/// 水位告警级别。
#[derive(PartialEq, Clone, Copy, Debug)]
pub enum WatermarkLevel {
    Normal,
    Warn,
    Critical,
}

/// 单分片指标（原子计数，跨线程无竞争）。
#[derive(Debug, Default)]
pub struct ShardMetrics {
    /// 分片内 docid 水位（已分配 local_id 数）。
    pub docid_watermark: AtomicU64,
    /// 写入计数。
    pub writes: AtomicU64,
    /// 读取计数。
    pub reads: AtomicU64,
}

/// 分片级指标注册表（n 分片；容量固定，水位更新 O(1)）。
#[derive(Debug, Default)]
pub struct ShardMetricsRegistry {
    shards: Vec<ShardMetrics>,
}

impl ShardMetricsRegistry {
    /// 创建 n 分片指标集（1..=65535）。
    pub fn new(n_shards: u16) -> Self {
        Self {
            shards: (0..n_shards).map(|_| ShardMetrics::default()).collect(),
        }
    }

    pub fn shard_count(&self) -> u16 {
        self.shards.len() as u16
    }

    /// 更新分片 docid 水位（分片构建/写入推进时上报）。
    pub fn update_watermark(&self, shard_id: u16, wm: u64) {
        if let Some(s) = self.shards.get(shard_id as usize) {
            s.docid_watermark.store(wm, Ordering::Relaxed);
        }
    }

    pub fn record_write(&self, shard_id: u16) {
        if let Some(s) = self.shards.get(shard_id as usize) {
            s.writes.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_read(&self, shard_id: u16) {
        if let Some(s) = self.shards.get(shard_id as usize) {
            s.reads.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn watermark_of(&self, shard_id: u16) -> u64 {
        self.shards
            .get(shard_id as usize)
            .map(|s| s.docid_watermark.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// 水位告警级别（Warn ≥ 80% / Critical ≥ 90%）。
    pub fn level_of(&self, shard_id: u16) -> WatermarkLevel {
        let ratio = self.watermark_of(shard_id) as f64 / SHARD_LOCAL_CAPACITY as f64;
        if ratio >= CRIT_RATIO {
            WatermarkLevel::Critical
        } else if ratio >= WARN_RATIO {
            WatermarkLevel::Warn
        } else {
            WatermarkLevel::Normal
        }
    }

    /// 当前告警列表：(shard_id, 级别, 水位比率)。
    pub fn alerts(&self) -> Vec<(u16, WatermarkLevel, f64)> {
        (0..self.shards.len() as u16)
            .map(|s| (s, self.level_of(s)))
            .filter(|(_, l)| *l != WatermarkLevel::Normal)
            .map(|(s, l)| (s, l, self.watermark_of(s) as f64 / SHARD_LOCAL_CAPACITY as f64))
            .collect()
    }

    /// Prometheus 文本渲染（分片级 gauge/counter，label=shard）。
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP shanshui_shard_docid_watermark 分片 docid 水位（已分配 local_id 数）\n");
        out.push_str("# TYPE shanshui_shard_docid_watermark gauge\n");
        out.push_str("# HELP shanshui_shard_docid_ratio 分片 docid 水位比率（watermark / 1<<40）\n");
        out.push_str("# TYPE shanshui_shard_docid_ratio gauge\n");
        out.push_str("# HELP shanshui_shard_writes_total 分片写入计数\n");
        out.push_str("# TYPE shanshui_shard_writes_total counter\n");
        out.push_str("# HELP shanshui_shard_reads_total 分片读取计数\n");
        out.push_str("# TYPE shanshui_shard_reads_total counter\n");
        for (sid, s) in self.shards.iter().enumerate() {
            let wm = s.docid_watermark.load(Ordering::Relaxed);
            let ratio = wm as f64 / SHARD_LOCAL_CAPACITY as f64;
            out.push_str(&format!(
                "shanshui_shard_docid_watermark{{shard=\"{sid}\"}} {wm}\n"
            ));
            out.push_str(&format!(
                "shanshui_shard_docid_ratio{{shard=\"{sid}\"}} {ratio:.6}\n"
            ));
            out.push_str(&format!(
                "shanshui_shard_writes_total{{shard=\"{sid}\"}} {}\n",
                s.writes.load(Ordering::Relaxed)
            ));
            out.push_str(&format!(
                "shanshui_shard_reads_total{{shard=\"{sid}\"}} {}\n",
                s.reads.load(Ordering::Relaxed)
            ));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_and_load_counting() {
        let r = ShardMetricsRegistry::new(4);
        r.update_watermark(0, 100);
        r.update_watermark(3, 99_999);
        r.record_write(0);
        r.record_write(0);
        r.record_write(1);
        r.record_read(0);
        assert_eq!(r.watermark_of(0), 100);
        assert_eq!(r.watermark_of(3), 99_999);
        assert_eq!(r.shards[0].writes.load(Ordering::Relaxed), 2);
        assert_eq!(r.shards[1].writes.load(Ordering::Relaxed), 1);
        assert_eq!(r.shards[0].reads.load(Ordering::Relaxed), 1);
        // 越界分片安全忽略
        r.update_watermark(99, 1);
        assert_eq!(r.watermark_of(99), 0);
    }

    #[test]
    fn watermark_alert_thresholds() {
        let r = ShardMetricsRegistry::new(3);
        r.update_watermark(0, SHARD_LOCAL_CAPACITY / 10);
        assert_eq!(r.level_of(0), WatermarkLevel::Normal);
        r.update_watermark(1, (SHARD_LOCAL_CAPACITY as f64 * 0.81) as u64);
        assert_eq!(r.level_of(1), WatermarkLevel::Warn);
        r.update_watermark(2, (SHARD_LOCAL_CAPACITY as f64 * 0.95) as u64);
        assert_eq!(r.level_of(2), WatermarkLevel::Critical);
        let alerts = r.alerts();
        assert_eq!(alerts.len(), 2);
        assert_eq!(alerts[0].0, 1);
        assert_eq!(alerts[0].1, WatermarkLevel::Warn);
        assert!((alerts[0].2 - 0.81).abs() < 1e-3, "Warn 水位比率 ≈0.81");
        assert_eq!(alerts[1].0, 2);
        assert_eq!(alerts[1].1, WatermarkLevel::Critical);
    }

    #[test]
    fn prometheus_render_shard_labels() {
        let r = ShardMetricsRegistry::new(2);
        r.update_watermark(1, 50);
        r.record_write(1);
        let out = r.render();
        assert!(out.contains("shanshui_shard_docid_watermark{shard=\"0\"} 0"));
        assert!(out.contains("shanshui_shard_docid_watermark{shard=\"1\"} 50"));
        assert!(out.contains("shanshui_shard_writes_total{shard=\"1\"} 1"));
        assert!(out.contains("shanshui_shard_docid_ratio{shard=\"1\"}"));
    }

    #[test]
    fn billion_build_no_alert() {
        // 10 分片 10 亿构建：每分片 1 亿 → 全 Normal（余量充足）
        let r = ShardMetricsRegistry::new(10);
        for sid in 0..10u16 {
            r.update_watermark(sid, 100_000_000);
            r.record_write(sid);
        }
        assert!(r.alerts().is_empty());
    }
}
