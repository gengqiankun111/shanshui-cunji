//! 数据关联与写入富化配置：sdk::join / 写入 Enrich（design 19）。

use serde::{Deserialize, Serialize};

/// 数据关联（sdk::join，development 5.20 / design 19）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JoinConfig {
    /// queryAndJoin 结果集上限，超限熔断（默认 100 万）。
    pub max_rows: usize,
    /// 小表广播 JOIN（design 19.3，阶段 3）：从表（关联侧）行数 ≤ broadcast_threshold 时，
    /// 一次性全量扫描建立内存索引复用（避免逐 key 点查）；默认关闭。
    pub broadcast_enabled: bool,
    /// 广播 JOIN 阈值（行）：从表行数超过则不广播（回退逐 key 点查）。
    pub broadcast_threshold: usize,
}

impl Default for JoinConfig {
    fn default() -> Self {
        Self {
            max_rows: 1_000_000,
            broadcast_enabled: false,
            broadcast_threshold: 100,
        }
    }
}

/// 写入 Enrich（预连接，development 5.21 / design 19.2 ② / 19.3）。
/// 钩子由业务方注入（Engine::set_enrich 查 Redis/MySQL/HTTP/本地表）；config 控制开关与失败策略。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EnrichConfig {
    /// 是否启用 Enrich。
    pub enabled: bool,
    /// 数据源：redis / mysql / http / local（基础版仅 local）。
    pub source: String,
    /// 失败策略："reject"（拒绝写入）/ "degrade"（降级写入原文档）。
    pub fail_policy: String,
    /// local 数据源关联源字段（主文档中指向关联键的字段，默认 user_id）。
    pub from_field: String,
    /// local 数据源关联目标字段（关联文档中被查找的字段，默认 docid）。
    pub to_field: String,
}

impl Default for EnrichConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            source: "local".into(),
            fail_policy: "degrade".into(),
            from_field: "user_id".into(),
            to_field: "docid".into(),
        }
    }
}