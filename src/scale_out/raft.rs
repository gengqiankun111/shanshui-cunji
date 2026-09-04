//! `RouteChannel` 的 raft 元数据驱动适配（7.89 raft 联动）：`RaftRouteChannel`。
//!
//! register/unregister = raft leader propose（进复制日志），follower 经 Append 幂等应用 →
//! 集群 MetaCenter 一致。编排须在 leader 节点执行。被 [`super::RouteChannel`] 抽象掩盖，
//! 由 `scale_out` 根模块经 `pub use` 重新导出，调用方路径不变。

use crate::error::{Error, Result};
use crate::raft_meta::{MetaOp, RaftRole};
use crate::raft_rpc::{RaftNodeRuntime, RaftTransport};

use super::RouteChannel;

/// raft 元数据驱动的路由通道：register/unregister = raft leader propose（进复制日志），
/// follower 经 Append 幂等应用 → 集群 MetaCenter 一致。编排须在 leader 节点执行。
pub struct RaftRouteChannel<T: RaftTransport> {
    raft: RaftNodeRuntime<T>,
}

impl<T: RaftTransport> RaftRouteChannel<T> {
    pub fn new(raft: RaftNodeRuntime<T>) -> Self {
        Self { raft }
    }

    /// propose 元数据操作（仅 leader；Append 广播由集群 pump 驱动 follower 应用）。
    fn submit(&mut self, op: MetaOp) -> Result<()> {
        if self.raft.role() != RaftRole::Leader {
            return Err(Error::Cluster(
                "扩容编排需在 raft leader 节点执行".into(),
            ));
        }
        self.raft.propose(op)
    }
}

impl<T: RaftTransport> RouteChannel for RaftRouteChannel<T> {
    fn register(&mut self, node: &str, addr: &str, role: &str) -> Result<()> {
        self.submit(MetaOp::Register {
            node: node.to_string(),
            addr: addr.to_string(),
            role: role.to_string(),
        })
    }
    fn unregister(&mut self, node: &str) {
        let _ = self.submit(MetaOp::Unregister {
            node: node.to_string(),
        });
    }
    fn master(&self) -> Option<String> {
        self.raft.master()
    }
}
