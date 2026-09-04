//! SAGA 编排 + 补偿状态机（Ex-2，design_extension v0.1 L2 跨分片业务事务）。
//!
//! 长事务拆 N 个 docid 级本地事务（步骤），任一步失败反向补偿；状态机持久化
//! （JSON tmp+rename 原子写，复用 MvScheduler 模式）；屏障（Barrier）防空回滚/
//! 悬挂：分支登记（正向成功后记录）先于补偿、补偿幂等键（tx_id+step）、回查接口
//! transactionId→status 持久化（网关 `/saga/status` 依据）。
//!
//! 设计要点（design_extension L2）：
//! - **正向**：按序执行步骤；成功即登记分支（executed_steps）；
//! - **反向补偿**：任一步失败 → 对已登记分支**逆序**补偿（补偿 = 语义相反的新操作，幂等）；
//! - **空回滚防护**：补偿只作用于已登记分支——超时未执行的分支不补偿（宁可多发由屏障空转）；
//! - **悬挂防护**：终态/已补偿分支拒绝迟到正向执行（防重复应用）；
//! - **崩溃恢复**：状态持久化，协调器重建后从持久化进度续跑/续补偿。
//!
//! 包结构（主题拆分）：本文件 = 公共主类型（状态/状态机状态/步骤 trait）+
//! `pub use` 汇总（对外路径保持 `crate::saga::*`）；子模块：
//! - `core`：SagaCoordinator（状态机执行/补偿/对账/持久化）；
//! - `steps`：步骤定义（ClosureStep / HttpStep + 极简 HTTP POST 客户端）；
//! - `topology`：Kahn 拓扑分层（13.6 拓扑并行基础）；
//! - `tests`：单元测试。

mod core;
mod steps;
mod topology;
#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// 当前纪元毫秒（13.7 对账退避/挂起检测时间基准）。
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// SAGA 状态机状态（终态 = Succeeded / Compensated）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SagaStatus {
    /// 已创建（登记，屏障回查起点），待执行。
    Init,
    /// 正向执行中。
    Executing,
    /// 全部正向成功（终态）。
    Succeeded,
    /// 正向失败，待补偿。
    Failed,
    /// 反向补偿中（任一补偿失败保持此态，重试续跑）。
    Compensating,
    /// 补偿完成（终态）。
    Compensated,
}

impl SagaStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Compensated)
    }
}

/// 持久化状态（transactionId → status 回查 + 崩溃恢复续跑）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaState {
    pub tx_id: String,
    pub status: SagaStatus,
    /// 正向已执行（已登记）步骤，顺序 = 执行序，反向补偿依据。
    pub executed_steps: Vec<String>,
    /// 已补偿步骤（补偿幂等：重复补偿 no-op）。
    pub compensated_steps: BTreeSet<String>,
    /// 最后错误（诊断/回查展示）。
    pub last_error: Option<String>,
    /// 13.7 对账诊断：补偿重试计数（指数退避依据）。
    #[serde(default)]
    pub retry_count: u32,
    /// 13.7 对账诊断：最后重试时间戳（自纪元毫秒；None = 未重试过）。
    #[serde(default)]
    pub last_retry_at_ms: Option<u64>,
    /// 13.7 挂起检测：状态最后变更时间（自纪元毫秒，persist 时更新）。
    #[serde(default)]
    pub updated_at_ms: u64,
}

impl SagaState {
    pub fn new(tx_id: &str) -> Self {
        Self {
            tx_id: tx_id.to_string(),
            status: SagaStatus::Init,
            executed_steps: Vec::new(),
            compensated_steps: BTreeSet::new(),
            last_error: None,
            retry_count: 0,
            last_retry_at_ms: None,
            updated_at_ms: now_ms(),
        }
    }
}

/// SAGA 步骤：正向 + 反向补偿（业务方实现；补偿 = 语义相反的新操作，须幂等）。
/// `Send + Sync`：13.6 拓扑并行执行在 scoped 线程中共享步骤引用。
pub trait SagaStep: Send + Sync {
    /// 步骤标识（屏障幂等键 = tx_id + name）。
    fn name(&self) -> &str;
    /// 正向执行（docid 级本地事务）。
    fn forward(&self) -> Result<()>;
    /// 反向补偿（幂等：重复调用不叠加副作用）。
    fn compensate(&self) -> Result<()>;
}

// ---- 子模块公开项汇总（保持原 `crate::saga::*` 路径，供 http.rs 等引用）----

pub use core::SagaCoordinator;
pub use steps::{http_post, ClosureStep, HttpStep};
pub(crate) use topology::topo_layers;
