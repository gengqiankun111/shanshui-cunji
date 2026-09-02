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

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::meta::MetaCenter;
use crate::raft_meta::{MetaOp, RaftRole};
use crate::raft_rpc::{RaftNodeRuntime, RaftTransport};

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    fn meta() -> MetaCenter {
        let mut m = MetaCenter::new(4);
        m.register("node-a", "127.0.0.1:9001", "master").unwrap();
        m
    }

    #[test]
    fn happy_path_switch_to_target() {
        // 正常扩容：ADDING → CATCH_UP → DRAIN → SWITCH → DONE，路由切到新节点
        let dir = tempfile::tempdir().unwrap();
        let mut c = ScaleOutCoordinator::begin(
            &dir.path().join("scale-out.json"),
            meta(),
            "node-a",
            "node-b",
            "127.0.0.1:9002",
        )
        .unwrap();
        assert_eq!(c.phase(), Phase::Adding);
        c.begin_catch_up().unwrap();
        assert_eq!(c.phase(), Phase::CatchUp);
        c.mark_drained().unwrap();
        assert_eq!(c.phase(), Phase::Drain);
        c.switch().unwrap();
        assert_eq!(c.phase(), Phase::Done);
        assert_eq!(c.master_node().as_deref(), Some("node-b"), "切换后路由到新节点");
    }

    #[test]
    fn rollback_keeps_source_and_removes_target() {
        // 回滚预案：CATCH_UP 阶段失败 → rollback → 路由保持旧节点、新节点摘除
        let dir = tempfile::tempdir().unwrap();
        let mut c = ScaleOutCoordinator::begin(
            &dir.path().join("scale-out.json"),
            meta(),
            "node-a",
            "node-b",
            "127.0.0.1:9002",
        )
        .unwrap();
        c.begin_catch_up().unwrap();
        c.rollback().unwrap();
        assert_eq!(c.phase(), Phase::Rollback);
        assert_eq!(c.master_node().as_deref(), Some("node-a"), "回滚后路由保持旧节点");
    }

    #[test]
    fn invalid_transition_rejected() {
        // 状态机防跳步：ADDING 直接 mark_drained（跳过 CATCH_UP）→ 拒绝
        let dir = tempfile::tempdir().unwrap();
        let mut c = ScaleOutCoordinator::begin(
            &dir.path().join("scale-out.json"),
            meta(),
            "node-a",
            "node-b",
            "127.0.0.1:9002",
        )
        .unwrap();
        assert!(c.mark_drained().is_err(), "跳步（ADDING→DRAIN）应拒绝");
        assert_eq!(c.phase(), Phase::Adding);
    }

    #[test]
    fn terminal_operations_rejected_and_rollback_idempotent() {
        // 终态后推进拒绝；重复回滚 no-op（幂等）
        let dir = tempfile::tempdir().unwrap();
        let mut c = ScaleOutCoordinator::begin(
            &dir.path().join("scale-out.json"),
            meta(),
            "node-a",
            "node-b",
            "127.0.0.1:9002",
        )
        .unwrap();
        c.rollback().unwrap();
        assert!(c.begin_catch_up().is_err(), "Rollback 终态后推进应拒绝");
        c.rollback().unwrap(); // 幂等 no-op
        assert_eq!(c.phase(), Phase::Rollback);
    }

    #[test]
    fn resume_from_persisted_state() {
        // 崩溃恢复：begin（ADDING 持久化）→ resume 恢复阶段续跑
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scale-out.json");
        {
            let mut c =
                ScaleOutCoordinator::begin(&path, meta(), "node-a", "node-b", "127.0.0.1:9002")
                    .unwrap();
            c.begin_catch_up().unwrap(); // 推进到 CATCH_UP 并持久化
        }
        let c2 = ScaleOutCoordinator::resume(&path, MetaCenter::new(4)).unwrap();
        assert_eq!(c2.phase(), Phase::CatchUp, "崩溃恢复续跑阶段");
        assert_eq!(c2.state.source, "node-a");
        assert_eq!(c2.state.target, "node-b");
    }

    // ============ raft 元数据联动（RouteChannel = RaftRouteChannel，7.89） ============

    #[test]
    fn scale_out_switch_via_raft_route_channel() {
        use crate::raft_rpc::{force_election, LocalRaftTransport, RaftMsg};
        let dir = tempfile::tempdir().unwrap();
        // raft 3 节点（LocalRaftTransport，进程内队列）
        let hub: Arc<Mutex<HashMap<u8, VecDeque<(u8, RaftMsg)>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let mut nodes: Vec<RaftNodeRuntime<LocalRaftTransport>> = (1..=3u8)
            .map(|id| {
                let peers: Vec<u8> = (1..=3u8).filter(|&p| p != id).collect();
                RaftNodeRuntime::new(
                    id,
                    peers,
                    MetaCenter::new(4),
                    LocalRaftTransport::new(id, hub.clone()),
                )
            })
            .collect();
        // 节点 1 当选 leader；初始 master = node-a（raft propose 注册，follower 应用）
        force_election(&mut nodes, 0);
        nodes[0]
            .propose(MetaOp::Register {
                node: "node-a".into(),
                addr: "127.0.0.1:9001".into(),
                role: "master".into(),
            })
            .unwrap();
        for _ in 0..5 {
            for r in nodes.iter_mut() {
                r.pump(Instant::now() + Duration::from_millis(300)).unwrap();
            }
        }
        assert_eq!(nodes[1].master().as_deref(), Some("node-a"), "follower 已应用 master=node-a");
        // 扩容编排：leader（节点 1）持 raft 路由通道，target node-b 接管写
        let leader = nodes.remove(0); // nodes 剩 [n2, n3]
        let mut coord = ScaleOutCoordinator::begin(
            &dir.path().join("scale-out.json"),
            RaftRouteChannel::new(leader),
            "node-a",
            "node-b",
            "127.0.0.1:9002",
        )
        .unwrap();
        assert_eq!(coord.phase(), Phase::Adding);
        coord.begin_catch_up().unwrap();
        coord.mark_drained().unwrap(); // 简化编排：追平/排空由调用方完成（本测仅联动）
        coord.switch().unwrap(); // raft propose：register b master + unregister a
        assert_eq!(coord.phase(), Phase::Done);
        // follower 应用 Append → 集群 MetaCenter 一致指向 node-b
        for _ in 0..20 {
            for r in nodes.iter_mut() {
                r.pump(Instant::now() + Duration::from_millis(300)).unwrap();
            }
        }
        assert_eq!(coord.master_node().as_deref(), Some("node-b"), "leader 路由切到 node-b");
        assert_eq!(nodes[0].master().as_deref(), Some("node-b"), "follower n2 路由切到 node-b");
        assert_eq!(nodes[1].master().as_deref(), Some("node-b"), "follower n3 路由切到 node-b");
    }

    #[test]
    fn scale_out_rollback_via_raft_route_channel() {
        use crate::raft_rpc::{force_election, LocalRaftTransport, RaftMsg};
        let dir = tempfile::tempdir().unwrap();
        let hub: Arc<Mutex<HashMap<u8, VecDeque<(u8, RaftMsg)>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let mut nodes: Vec<RaftNodeRuntime<LocalRaftTransport>> = (1..=3u8)
            .map(|id| {
                let peers: Vec<u8> = (1..=3u8).filter(|&p| p != id).collect();
                RaftNodeRuntime::new(
                    id,
                    peers,
                    MetaCenter::new(4),
                    LocalRaftTransport::new(id, hub.clone()),
                )
            })
            .collect();
        force_election(&mut nodes, 0);
        nodes[0]
            .propose(MetaOp::Register {
                node: "node-a".into(),
                addr: "127.0.0.1:9001".into(),
                role: "master".into(),
            })
            .unwrap();
        let leader = nodes.remove(0);
        let mut coord = ScaleOutCoordinator::begin(
            &dir.path().join("scale-out.json"),
            RaftRouteChannel::new(leader),
            "node-a",
            "node-b",
            "127.0.0.1:9002",
        )
        .unwrap();
        coord.begin_catch_up().unwrap();
        coord.rollback().unwrap(); // CATCH_UP 失败回滚：raft propose 保持 node-a master + 摘 node-b
        assert_eq!(coord.phase(), Phase::Rollback);
        assert_eq!(coord.master_node().as_deref(), Some("node-a"), "回滚后路由保持旧节点");
        for _ in 0..20 {
            for r in nodes.iter_mut() {
                r.pump(Instant::now() + Duration::from_millis(300)).unwrap();
            }
        }
        assert_eq!(nodes[0].master().as_deref(), Some("node-a"), "follower 回滚一致");
        assert_eq!(nodes[1].master().as_deref(), Some("node-a"));
    }

    #[test]
    fn raft_route_channel_rejects_non_leader() {
        use crate::raft_rpc::{force_election, LocalRaftTransport, RaftMsg};
        let hub: Arc<Mutex<HashMap<u8, VecDeque<(u8, RaftMsg)>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let mut nodes: Vec<RaftNodeRuntime<LocalRaftTransport>> = (1..=3u8)
            .map(|id| {
                let peers: Vec<u8> = (1..=3u8).filter(|&p| p != id).collect();
                RaftNodeRuntime::new(
                    id,
                    peers,
                    MetaCenter::new(4),
                    LocalRaftTransport::new(id, hub.clone()),
                )
            })
            .collect();
        force_election(&mut nodes, 0);
        // 用非 leader（节点 2）的路由通道注册 → 拒绝
        let follower = nodes.remove(1); // nodes = [n1, n3]（n1 leader、n3 follower）
        let mut ch = RaftRouteChannel::new(follower);
        assert!(
            ch.register("node-x", "127.0.0.1:9009", "slave").is_err(),
            "非 leader 路由通道 propose 应拒绝"
        );
    }
}
