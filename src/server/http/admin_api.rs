//! 管理端点：`GET /admin/status`（引擎状态 StatusReport，design 20）与
//! `GET /metrics`（Prometheus 文本格式指标，X 项 + 10 亿库阶段 D 分片级指标）。

use serde_json::json;

use crate::engine::Engine;

/// 引擎状态（design 20）：`GET /admin/status` → StatusReport JSON。
pub(crate) fn handle_admin_status(engine: &mut Engine) -> (u16, String) {
    let rep = crate::admin::status(engine);
    match serde_json::to_string(&rep) {
        Ok(s) => (200, s),
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}

/// 2026-09-05：运行时组件 gauge（内存计量 + MVCC 快照生命周期 + Bloom 分层过滤计数）。
fn engine_runtime_gauges(engine: &Engine) -> Vec<(&'static str, &'static str, u64)> {
    let mut g = engine.memory_report();
    g.extend(engine.snapshot_report());
    g.extend(engine.bloom_report());
    g
}

/// Prometheus 指标（X 项）：`GET /metrics` → 文本格式（计数/直方图/gauge 分层埋点）。
pub(crate) fn handle_metrics(engine: &mut Engine) -> (u16, String) {
    let s = engine.stats();
    let l0 = engine.primary_l0_count() as u64;
    let flush = engine.total_flush_count();
    let mut out = engine
        .metrics
        .render(
            s.sst_file_count as u64,
            l0,
            s.mem_ratio,
            s.disk_ratio,
            flush,
            &engine_runtime_gauges(engine),
        );
    // 10 亿库阶段 D：分片级指标（docid 水位 + 读写计数 + 预警）
    out.push_str(&engine.shard_metrics_render());
    if !engine.shard_watermark_alerts().is_empty() {
        out.push_str("# HELP shanshui_shard_docid_alert 分片 docid 水位预警（1=Warn 2=Critical）\n");
        out.push_str("# TYPE shanshui_shard_docid_alert gauge\n");
        for (sid, lvl, ratio) in engine.shard_watermark_alerts() {
            let v = match lvl {
                crate::shard_metrics::WatermarkLevel::Normal => 0u64,
                crate::shard_metrics::WatermarkLevel::Warn => 1,
                crate::shard_metrics::WatermarkLevel::Critical => 2,
            };
            out.push_str(&format!(
                "shanshui_shard_docid_alert{{shard=\"{sid}\",level=\"{lvl:?}\"}} {v} # ratio={ratio:.4}\n"
            ));
        }
    }
    (200, out)
}
