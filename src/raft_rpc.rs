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

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::meta::MetaCenter;
use crate::raft_meta::{MetaEntry, MetaOp, RaftRole};

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

/// 进程内传输：每节点一个收件箱队列（共享中枢 + Mutex，单机多节点联调/测试）。
#[derive(Clone)]
pub struct LocalRaftTransport {
    id: u8,
    hub: Arc<Mutex<HashMap<u8, VecDeque<(u8, RaftMsg)>>>>,
}

impl LocalRaftTransport {
    pub fn new(id: u8, hub: Arc<Mutex<HashMap<u8, VecDeque<(u8, RaftMsg)>>>>) -> Self {
        hub.lock().unwrap().entry(id).or_default();
        Self { id, hub }
    }
}

impl RaftTransport for LocalRaftTransport {
    fn send(&mut self, to: u8, msg: RaftMsg) -> Result<()> {
        self.hub.lock().unwrap().entry(to).or_default().push_back((self.id, msg));
        Ok(())
    }
    fn recv(&mut self) -> Result<Option<(u8, RaftMsg)>> {
        Ok(self.hub.lock().unwrap().get_mut(&self.id).and_then(|q| q.pop_front()))
    }
}

/// 单节点 Raft 运行时：状态机 + 传输驱动。
pub struct RaftNodeRuntime<T: RaftTransport> {
    id: u8,
    role: RaftRole,
    term: u64,
    voted_for: Option<u8>,
    log: Vec<MetaEntry>,
    /// 已提交日志长度（应用到状态机）。
    commit: u64,
    /// 已应用到状态机的日志长度（状态机顺序推进，幂等）。
    applied: u64,
    votes: u32,
    leader: Option<u8>,
    /// 状态机副本（仅应用已提交条目，幂等）。
    state: MetaCenter,
    /// 最近心跳（failover 检测）。
    last_heartbeat: Instant,
    peers: Vec<u8>,
    quorum: u32,
    heartbeat_timeout: Duration,
    transport: T,
}

impl<T: RaftTransport> RaftNodeRuntime<T> {
    /// 创建节点运行时（peers = 其余节点 id；cluster 总节点数 = peers+1）。
    pub fn new(id: u8, peers: Vec<u8>, seed: MetaCenter, transport: T) -> Self {
        let node_count = peers.len() + 1;
        Self {
            id,
            role: RaftRole::Follower,
            term: 0,
            voted_for: None,
            log: Vec::new(),
            commit: 0,
            applied: 0,
            votes: 0,
            leader: None,
            state: seed,
            last_heartbeat: Instant::now(),
            peers,
            quorum: (node_count as u32) / 2 + 1,
            heartbeat_timeout: Duration::from_millis(100),
            transport,
        }
    }

    /// 当前 master（状态机，仅已提交条目）。
    pub fn master(&self) -> Option<String> {
        self.state.master_node().map(|n| n.node_id.clone())
    }

    pub fn role(&self) -> RaftRole {
        self.role
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    /// 应用已提交但未应用的日志条目（顺序推进，幂等）。
    fn apply_to_state(&mut self) -> Result<()> {
        while self.applied < self.commit {
            let idx = self.applied as usize;
            let e = self.log[idx].clone();
            match &e.op {
                MetaOp::Register { node, addr, role } => {
                    self.state.register(node, addr, role)?;
                }
                MetaOp::Unregister { node } => {
                    self.state.unregister(node);
                }
            }
            self.applied += 1;
        }
        Ok(())
    }

    /// 驱动一轮：处理收件箱全部消息，再检查超时选举。返回新 leader（如有）。
    pub fn pump(&mut self, now: Instant) -> Result<Option<u8>> {
        while let Some((from, msg)) = self.transport.recv()? {
            self.handle(from, msg)?;
        }
        Ok(self.maybe_elect(now))
    }

    fn handle(&mut self, from: u8, msg: RaftMsg) -> Result<()> {
        match msg {
            RaftMsg::VoteReq { term, cand } => {
                let grant = {
                    if term < self.term {
                        false
                    } else {
                        if term > self.term {
                            self.term = term;
                            self.voted_for = None;
                            self.role = RaftRole::Follower;
                        }
                        let g = self.voted_for.is_none() || self.voted_for == Some(cand);
                        if g {
                            self.voted_for = Some(cand);
                        }
                        g
                    }
                };
                self.transport.send(from, RaftMsg::VoteResp { term: self.term, granted: grant })?;
            }
            RaftMsg::VoteResp { term, granted } => {
                if term != self.term || self.role != RaftRole::Candidate || !granted {
                    return Ok(());
                }
                self.votes += 1;
                if self.votes >= self.quorum {
                    self.role = RaftRole::Leader;
                    self.leader = Some(self.id);
                    self.votes = 0;
                }
            }
            RaftMsg::Append { term, leader, entries } => {
                let ok = if term < self.term {
                    false
                } else {
                    if term > self.term {
                        self.term = term;
                        self.role = RaftRole::Follower;
                    }
                    self.leader = Some(leader);
                    self.last_heartbeat = Instant::now();
                    for e in entries {
                        if !self.log.contains(&e) {
                            self.log.push(e);
                        }
                    }
                    self.commit = self.log.len() as u64;
                    self.apply_to_state()?;
                    true
                };
                self.transport.send(from, RaftMsg::AppendAck { term, ok })?;
            }
            RaftMsg::AppendAck { term, ok } => {
                if ok && self.role == RaftRole::Leader && term == self.term {
                    self.commit = self.log.len() as u64;
                    self.apply_to_state()?;
                }
            }
        }
        Ok(())
    }

    fn maybe_elect(&mut self, now: Instant) -> Option<u8> {
        if self.role == RaftRole::Leader {
            return Some(self.id);
        }
        if now.duration_since(self.last_heartbeat) < self.heartbeat_timeout {
            return None;
        }
        // 超时 → 选举：term+1、自投、广播 VoteReq
        self.term += 1;
        self.role = RaftRole::Candidate;
        self.voted_for = Some(self.id);
        self.votes = 1;
        let (term, cand) = (self.term, self.id);
        for &p in &self.peers.clone() {
            let _ = self.transport.send(p, RaftMsg::VoteReq { term, cand });
        }
        if self.role == RaftRole::Leader {
            Some(self.id)
        } else {
            None
        }
    }

    /// leader 提议元数据操作：本地追加日志 + Append 广播 → 提交 → 应用到状态机。
    pub fn propose(&mut self, op: MetaOp) -> Result<()> {
        if self.role != RaftRole::Leader {
            return Err(Error::Cluster(format!("节点 {} 非 leader，无法提议", self.id)));
        }
        let term = self.term;
        let entry = MetaEntry { term, op };
        self.log.push(entry.clone());
        for &p in &self.peers.clone() {
            self.transport.send(p, RaftMsg::Append {
                term,
                leader: self.id,
                entries: vec![entry.clone()],
            })?;
        }
        self.commit = self.log.len() as u64;
        self.apply_to_state()
    }

    /// 刷新心跳（外部心跳源：真实部署中 leader 周期 Append 空条目驱动）。
    pub fn refresh_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
    }

    pub fn set_heartbeat_timeout(&mut self, d: Duration) {
        self.heartbeat_timeout = d;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed() -> MetaCenter {
        // 空状态机：复制后的 master 来自日志条目（验证多数派提交的 register）
        MetaCenter::new(4)
    }

    fn register_op(node: &str) -> MetaOp {
        MetaOp::Register {
            node: node.to_string(),
            addr: "127.0.0.1:9999".to_string(),
            role: "master".to_string(),
        }
    }

    fn cluster3() -> (Arc<Mutex<HashMap<u8, VecDeque<(u8, RaftMsg)>>>>, Vec<RaftNodeRuntime<LocalRaftTransport>>) {
        let hub: Arc<Mutex<HashMap<u8, VecDeque<(u8, RaftMsg)>>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut runtimes = Vec::new();
        for id in 1..=3u8 {
            let peers: Vec<u8> = (1..=3u8).filter(|&p| p != id).collect();
            runtimes.push(RaftNodeRuntime::new(
                id,
                peers,
                seed(),
                LocalRaftTransport::new(id, hub.clone()),
            ));
        }
        (hub, runtimes)
    }

    /// 只让 target 竞选（其余节点心跳刷新不超时）；全节点 pump（响应 VoteReq）。
    fn elect_single(rt: &mut [RaftNodeRuntime<LocalRaftTransport>], target: usize, t0: Instant) {
        let far = t0 + Duration::from_millis(60_000);
        for (i, r) in rt.iter_mut().enumerate() {
            if i != target {
                r.refresh_heartbeat();
                r.last_heartbeat = far;
            }
        }
        for _ in 0..50 {
            for r in rt.iter_mut() {
                r.pump(t0 + Duration::from_millis(200)).unwrap();
            }
            if rt[target].role() == RaftRole::Leader {
                return;
            }
        }
        panic!("target 未当选");
    }

    #[test]
    fn msg_serde_roundtrip() {
        // RaftMsg JSON 序列化往返（RPC 通道传输协议）
        let msgs = vec![
            RaftMsg::VoteReq { term: 3, cand: 2 },
            RaftMsg::VoteResp { term: 3, granted: true },
            RaftMsg::Append {
                term: 3,
                leader: 2,
                entries: vec![MetaEntry { term: 3, op: register_op("n1") }],
            },
            RaftMsg::AppendAck { term: 3, ok: true },
        ];
        for m in &msgs {
            let s = serde_json::to_string(m).unwrap();
            let back: RaftMsg = serde_json::from_str(&s).unwrap();
            assert_eq!(&back, m, "消息序列化往返一致");
        }
    }

    #[test]
    fn election_via_transport() {
        let (_, mut rt) = cluster3();
        let t0 = Instant::now();
        elect_single(&mut rt, 0, t0);
        assert_eq!(rt[0].role(), RaftRole::Leader);
        assert_eq!(rt[1].role(), RaftRole::Follower);
        assert_eq!(rt[2].role(), RaftRole::Follower);
    }

    #[test]
    fn log_replication_via_transport() {
        let (_, mut rt) = cluster3();
        let t0 = Instant::now();
        elect_single(&mut rt, 0, t0);
        rt[0].propose(register_op("node-x")).unwrap();
        for _ in 0..5 {
            for r in rt.iter_mut() {
                r.pump(t0 + Duration::from_millis(200)).unwrap();
            }
        }
        assert_eq!(rt[0].master().as_deref(), Some("node-x"));
        assert_eq!(rt[1].master().as_deref(), Some("node-x"), "follower 状态一致");
        assert_eq!(rt[2].master().as_deref(), Some("node-x"));
    }

    #[test]
    fn automatic_failover() {
        let (_, mut rt) = cluster3();
        let t0 = Instant::now();
        elect_single(&mut rt, 0, t0);
        assert_eq!(rt[0].role(), RaftRole::Leader);
        // leader 宕机：节点 2 心跳置旧触发超时；节点 3 心跳刷新不竞选
        let t1 = t0 + Duration::from_millis(500);
        rt[1].last_heartbeat = t1 - Duration::from_millis(200);
        rt[2].last_heartbeat = t1 + Duration::from_millis(60_000);
        let mut new_leader = None;
        for _ in 0..50 {
            rt[1].pump(t1).unwrap();
            rt[2].pump(t1).unwrap();
            if rt[1].role() == RaftRole::Leader {
                new_leader = Some(2);
                break;
            }
        }
        assert_eq!(new_leader, Some(2), "failover：节点 2 超时当选新 leader");
        rt[1].propose(register_op("node-y")).unwrap();
        for _ in 0..5 {
            rt[1].pump(t1).unwrap();
            rt[2].pump(t1).unwrap();
        }
        assert_eq!(rt[1].master().as_deref(), Some("node-y"));
    }

    #[test]
    fn majority_survives_single_down() {
        let (_, mut rt) = cluster3();
        let t0 = Instant::now();
        elect_single(&mut rt, 0, t0);
        rt[0].propose(register_op("node-z")).unwrap();
        for _ in 0..5 {
            for i in [0usize, 1] {
                rt[i].pump(t0 + Duration::from_millis(200)).unwrap();
            }
        }
        assert_eq!(rt[0].master().as_deref(), Some("node-z"));
        assert_eq!(rt[1].master().as_deref(), Some("node-z"), "存活 follower 复制成功");
    }
}
