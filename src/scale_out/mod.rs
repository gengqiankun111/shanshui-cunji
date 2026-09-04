//! 双写扩容协议衔接（Ex-1.5，development 7.43 剩余）：扩容编排协调器。
//!
//! 原 M5 方案 = "双写→追平→切换"（业务同时写新老节点，双写窗口丢数据/抖动的来源）。
//! Ex-1 落地 outbox 后改为 **"本地事务写 + outbox 待办 + 排空校验"**（design_extension v0.1
//! L1 首选，development 7.43）：
//!
//! 1. **业务只写主节点**：业务写 + outbox 消息同全局 seq / 同 fsync 点（本地原子，零双写）；
//! 2. **追平（CATCH_UP）**：调用方 `engine.dispatch_outbox` → 投递回调（生产 = RPC
//!    `repl.apply` 幂等应用到新节点；测试 = 进程内回调）；
//! 3. **排空校验（DRAIN）**：`outbox_drained`（pending=0）+ 数据一致性抽样（主/新节点
//!    逐 docid 对比）——**排空未完成禁止切换**（防切脏数据）；
//! 4. **切换（SWITCH）**：路由更新（新节点接管写，旧节点摘除）；
//! 5. **回滚预案（ROLLBACK）**：任意阶段失败 → 路由不切换/回退，旧节点继续服务（数据不丢，
//!    新节点摘除）；编排状态**持久化**（崩溃恢复续跑，终态幂等）。
//!
//! 路由变更经 **`RouteChannel`** 抽象提交（7.89 raft 联动）：
//! - `MetaCenter` 直写：单机/测试（路由本地生效）；
//! - `RaftRouteChannel`（raft 适配器）：register/unregister **propose 进 raft 复制日志**
//!   （多节点部署下集群 MetaCenter 副本一致，扩容切换不被单机路由掩盖）。
//!
//! 职责划分：协调器只做**状态机 + 状态持久化 + 路由更新**；投递/取数/校验由调用方用
//! engine/meta API 完成并把结果反馈给协调器（低耦合——生产接 RPC repl.apply，测试用
//! 进程内双 Engine）。与 `replication.rs`（repl.apply 幂等）+ `meta.rs`（路由）+ `raft_rpc.rs`
//! （元数据多数派复制）衔接。
//!
//! 本模块按主题拆分：编排核心（阶段状态机/状态/协调器 + `RouteChannel` 抽象与
//! `MetaCenter` 直写实现）留在本文件；raft 元数据驱动的路由适配器见 [`raft`] 子模块。

mod raft;
#[cfg(test)]
mod tests;

pub use raft::RaftRouteChannel;

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::meta::MetaCenter;

/// 路由变更通道（编排副作用提交点）：扩容注册/切换/摘除经它生效。
///
/// - `MetaCenter`：本地直写（单机/测试）；
/// - `RaftRouteChannel`：经 raft leader propose 写入复制日志（多节点部署，follower
///   经 Append 应用后集群 MetaCenter 一致）。
pub trait RouteChannel {
    /// 注册节点（role: master/slave）。
    fn register(&mut self, node: &str, addr: &str, role: &str) -> Result<()>;
    /// 摘除节点。
    fn unregister(&mut self, node: &str);
    /// 当前 master 节点（路由校验）。
    fn master(&self) -> Option<String>;
}

impl RouteChannel for MetaCenter {
    fn register(&mut self, node: &str, addr: &str, role: &str) -> Result<()> {
        MetaCenter::register(self, node, addr, role)?;
        Ok(())
    }
    fn unregister(&mut self, node: &str) {
        MetaCenter::unregister(self, node);
    }
    fn master(&self) -> Option<String> {
        MetaCenter::master_node(self).map(|n| n.node_id.clone())
    }
}

/// 扩容编排阶段（状态机）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    /// 新节点已注册（slave），尚未开始追平。
    Adding,
    /// outbox 增量追平中（投递器投递到新节点）。
    CatchUp,
    /// 排空校验中（pending=0 + 一致性抽样）。
    Drain,
    /// 路由切换（新节点接管写）。
    Switch,
    /// 完成（新节点接管，旧节点摘除）。
    Done,
    /// 回滚（失败终止：路由保持旧节点，新节点摘除）。
    Rollback,
}

impl Phase {
    /// 是否终态（完成/回滚——后续操作拒绝）。
    pub fn is_terminal(&self) -> bool {
        matches!(self, Phase::Done | Phase::Rollback)
    }
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Phase::Adding => "ADDING",
                Phase::CatchUp => "CATCH_UP",
                Phase::Drain => "DRAIN",
                Phase::Switch => "SWITCH",
                Phase::Done => "DONE",
                Phase::Rollback => "ROLLBACK",
            }
        )
    }
}

/// 编排状态（持久化：崩溃恢复续跑）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleOutState {
    pub phase: Phase,
    /// 旧节点（当前服务写）。
    pub source: String,
    /// 新节点（扩容目标）。
    pub target: String,
}

/// 双写扩容编排协调器（Ex-1.5）。
pub struct ScaleOutCoordinator {
    state: ScaleOutState,
    /// 路由变更通道（本地 MetaCenter / raft 复制日志，见 `RouteChannel`）。
    routes: Box<dyn RouteChannel>,
    /// 状态持久化路径（`{data_dir}/scale-out.json`）。
    state_path: std::path::PathBuf,
}

impl ScaleOutCoordinator {
    /// 新建扩容编排（ADDING 阶段）：经路由通道注册新节点为 slave 并持久化状态。
    pub fn begin(
        state_path: &std::path::Path,
        routes: impl RouteChannel + 'static,
        source: &str,
        target: &str,
        target_addr: &str,
    ) -> Result<Self> {
        let mut routes: Box<dyn RouteChannel> = Box::new(routes);
        routes.register(target, target_addr, "slave")?;
        let state = ScaleOutState {
            phase: Phase::Adding,
            source: source.to_string(),
            target: target.to_string(),
        };
        let c = Self {
            state,
            routes,
            state_path: state_path.to_path_buf(),
        };
        c.persist()?;
        Ok(c)
    }

    /// 从持久化状态恢复（崩溃续跑；终态直接返回）。
    pub fn resume(state_path: &std::path::Path, routes: impl RouteChannel + 'static) -> Result<Self> {
        let text = std::fs::read_to_string(state_path)
            .map_err(|e| crate::error::Error::Corrupted(format!("扩容状态读取失败: {e}")))?;
        let state: ScaleOutState = serde_json::from_str(&text)
            .map_err(|e| crate::error::Error::Corrupted(format!("扩容状态解析失败: {e}")))?;
        Ok(Self {
            state,
            routes: Box::new(routes),
            state_path: state_path.to_path_buf(),
        })
    }

    pub fn phase(&self) -> Phase {
        self.state.phase
    }

    /// 追平开始：推进到 CATCH_UP 并持久化（调用方随后用 engine.dispatch_outbox 投递）。
    pub fn begin_catch_up(&mut self) -> Result<()> {
        self.advance(Phase::CatchUp)
    }

    /// 排空校验通过：推进到 DRAIN（调用方先验 `outbox_drained` + 抽样一致性）。
    pub fn mark_drained(&mut self) -> Result<()> {
        self.advance(Phase::Drain)
    }

    /// SWITCH：路由切换（新节点接管写）+ 旧节点摘除 + 完成。路由变更经 RouteChannel
    /// 提交（raft 版 = propose 进复制日志，follower 经 Append 应用后集群一致）。
    pub fn switch(&mut self) -> Result<()> {
        self.advance(Phase::Switch)?;
        self.routes.register(&self.state.target, "", "master")?;
        self.routes.unregister(&self.state.source);
        self.advance(Phase::Done)
    }

    /// 回滚：路由回退旧节点（新节点摘除），编排终止。幂等（终态回滚 no-op）。
    pub fn rollback(&mut self) -> Result<()> {
        if self.state.phase.is_terminal() {
            return Ok(()); // 已终止（Done/Rollback），重复回滚 no-op
        }
        self.routes.register(&self.state.source, "", "master")?;
        self.routes.unregister(&self.state.target);
        self.state.phase = Phase::Rollback;
        self.persist()
    }

    /// 当前 master 节点（路由校验用）。
    pub fn master_node(&self) -> Option<String> {
        self.routes.master()
    }

    /// 推进阶段并持久化（状态机合法性校验：不得跳步/终态后推进）。
    fn advance(&mut self, target: Phase) -> Result<()> {
        if self.state.phase.is_terminal() {
            return Err(crate::error::Error::Unsupported(
                format!("扩容编排已终止（{}），拒绝推进到 {target:?}", self.state.phase).into(),
            ));
        }
        let allowed = match (self.state.phase, target) {
            (Phase::Adding, Phase::CatchUp)
            | (Phase::CatchUp, Phase::Drain)
            | (Phase::Drain, Phase::Switch)
            | (Phase::Switch, Phase::Done) => true,
            _ if target == Phase::Rollback => true,
            _ => false,
        };
        if !allowed {
            return Err(crate::error::Error::Unsupported(
                format!("非法阶段转移 {:?} → {:?}", self.state.phase, target).into(),
            ));
        }
        self.state.phase = target;
        self.persist()
    }

    fn persist(&self) -> Result<()> {
        let tmp = self.state_path.with_extension("json.tmp");
        let text = serde_json::to_string_pretty(&self.state)
            .map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, &self.state_path)?;
        Ok(())
    }
}
