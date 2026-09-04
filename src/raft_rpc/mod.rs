//! Raft 元数据 RPC 接线（10 亿库扩展阶段 C，design-10b-extension.md §6 阶段 C，raft 阶段二）。
//!
//! 把 raft_meta.rs 的单进程确定性消息路由解耦为 **`RaftTransport` trait**（send/recv 抽象）：
//! - `LocalRaftTransport`：进程内队列（测试 / 单机多节点联调）；
//! - 真实部署：TCP 实现（JSON-over-TCP，复用 rpc.rs 帧格式）——接 `MetaCenter` 节点间通道。
//!
//! `RaftNodeRuntime` = 单节点状态机（term/role/log/commit/votes）+ transport：
//! 收到消息 → handler 处理 → 经 transport 回发；`tick` 心跳超时 → 自动选举（failover）；
//! leader `propose` → 日志追加 + Append 广播 → 提交 → 应用到 MetaCenter 状态机。
//! 多数派语义（N/2+1）与脑裂安全同 raft_meta.rs 阶段一。
//!
//! 模块组织：`mod.rs`（消息类型 `RaftMsg` / 传输抽象 `RaftTransport` + `pub use` 对外汇总）、
//! `transport.rs`（`LocalRaftTransport` / `TcpRaftTransport` 两种传输实现）、
//! `runtime.rs`（`RaftNodeRuntime` 单节点状态机 + 传输驱动）、`tests.rs`（模块测试）。

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::raft_meta::MetaEntry;

mod runtime;
#[cfg(test)]
mod tests;
mod transport;

pub use runtime::RaftNodeRuntime;
pub use transport::{LocalRaftTransport, TcpRaftTransport};

/// 测试辅助（跨模块 e2e，scale_out.rs 测试使用）：强制 `rt[target]` 超时并 pump 至当选 leader。
#[cfg(test)]
pub(crate) use runtime::force_election;

/// Raft 消息（可序列化，经 RPC 通道传输）。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RaftMsg {
    VoteReq { term: u64, cand: u8 },
    VoteResp { term: u64, granted: bool },
    Append { term: u64, leader: u8, entries: Vec<MetaEntry> },
    AppendAck { term: u64, ok: bool },
}

/// 传输抽象：send 到目标节点；recv 从自己收件箱取消息（驱动循环轮询）。
pub trait RaftTransport: Send {
    fn send(&mut self, to: u8, msg: RaftMsg) -> Result<()>;
    fn recv(&mut self) -> Result<Option<(u8, RaftMsg)>>;
}
