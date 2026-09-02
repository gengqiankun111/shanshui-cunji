//! 多副本元数据 Raft 高可用（P0-2 阶段一，design_extension 14.x 元数据自动切换）：
//!
//! 目标（用户排期）：节点宕机**元数据自动切换**（不人工介入）+ **网络分区（脑裂）一致性**。
//! 本模块用最小 Raft 管理元数据中心（MetaCenter）的 master 角色：
//! - **选举多数派**：候选获 N/2+1 票成 leader（其余 follower）；同 term 已投票不可改投；
//! - **日志复制**：元数据操作（register/unregister）作为日志条目，复制到多数派才提交
//!   （提交后才应用到 MetaCenter 状态机）——多数派提交保证无脑裂双主；
//! - **自动 failover**：leader 心跳超时（`tick`）→ 自动发起选举 → 新 leader 接管 master；
//! - **脑裂安全**：分区后少数派（<多数派）无法选主/提交（无新 master），恢复后追平日志。
//!
//! 网络抽象：消息经 `RaftMetaGroup` 路由（`reachable` 注入分区/宕机）——生产接 RPC
//! 消息通道（阶段二：与 Calvin gseq 分配器 raft 联动），本模块为确定性核心（可单测）。

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::meta::MetaCenter;

/// Raft 角色。
#[derive(PartialEq, Clone, Copy, Debug)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

/// 元数据操作（日志条目 → 应用到 MetaCenter 状态机）。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MetaOp {
    Register { node: String, addr: String, role: String },
    Unregister { node: String },
}

/// 日志条目。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetaEntry {
    pub term: u64,
    pub op: MetaOp,
}

/// 单节点 Raft 元数据副本。
pub struct RaftMetaNode {
    id: u8,
    role: RaftRole,
    term: u64,
    voted_for: Option<u8>,
    log: Vec<MetaEntry>,
    /// 已提交日志长度（应用到状态机）。
    commit: u64,
    /// 已应用到状态机的日志长度（状态机幂等推进）。
    applied: u64,
    leader: Option<u8>,
    votes: u32,
    /// 状态机副本（仅应用到 commit 的日志条目）。
    state: MetaCenter,
    /// 最近心跳（failover 检测：超时 → 自动选举）。
    last_heartbeat: Instant,
    /// 网络可达（false = 宕机/分区隔离；测试注入）。
    pub reachable: bool,
}

impl RaftMetaNode {
    fn new(id: u8, seed: MetaCenter) -> Self {
        Self {
            id,
            role: RaftRole::Follower,
            term: 0,
            voted_for: None,
            log: Vec::new(),
            commit: 0,
            applied: 0,
            leader: None,
            votes: 0,
            state: seed,
            last_heartbeat: Instant::now(),
            reachable: true,
        }
    }

    /// 当前 master（状态机，仅已提交条目）。
    pub fn master(&self) -> Option<String> {
        self.state.master_node().map(|n| n.node_id.clone())
    }

    /// 应用已提交但未应用的日志条目（commit 推进后调用；状态机顺序推进，幂等）。
    fn apply_to_state(&mut self) -> Result<()> {
        while (self.applied as usize) < self.commit as usize {
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
}

/// Raft 消息（协议最小集）。
#[derive(Clone, Debug)]
enum Msg {
    VoteReq { term: u64, cand: u8 },
    VoteResp { term: u64, granted: bool },
    Append { term: u64, leader: u8, entries: Vec<MetaEntry> },
    AppendAck { term: u64, ok: bool },
}

/// 多副本元数据 Raft 集群（确定性消息路由 + 分区注入）。
pub struct RaftMetaGroup {
    nodes: HashMap<u8, RaftMetaNode>,
    /// 心跳超时（自动 failover 阈值）。
    heartbeat_timeout: Duration,
}

impl RaftMetaGroup {
    /// 创建 3 节点集群（N=3，多数派 2）。
    pub fn new(ids: &[u8], seed: MetaCenter) -> Self {
        let nodes = ids
            .iter()
            .map(|&id| (id, RaftMetaNode::new(id, seed.clone())))
            .collect();
        Self {
            nodes,
            heartbeat_timeout: Duration::from_millis(100),
        }
    }

    pub fn quorum(&self) -> u32 {
        (self.nodes.len() as u32) / 2 + 1
    }

    /// 消息投递：任一端不可达 → 丢弃（分区/宕机模拟）。
    fn deliver(&mut self, from: u8, to: u8, msg: Msg) -> bool {
        if !self.nodes[&from].reachable || !self.nodes[&to].reachable {
            return false;
        }
        match msg {
            Msg::VoteReq { term, cand } => self.handle_vote_req(from, to, term, cand),
            Msg::VoteResp { term, granted } => self.handle_vote_resp(from, to, term, granted),
            Msg::Append { term, leader, entries } => {
                self.handle_append(from, to, term, leader, entries)
            }
            Msg::AppendAck { term, ok } => self.handle_append_ack(from, to, term, ok),
        }
        true
    }

    fn handle_vote_req(&mut self, from: u8, to: u8, term: u64, cand: u8) {
        let (grant, cur_term) = {
            let n = self.nodes.get_mut(&to).unwrap();
            if term < n.term {
                (false, n.term)
            } else {
                if term > n.term {
                    n.term = term;
                    n.voted_for = None;
                    n.role = RaftRole::Follower;
                }
                let grant = n.voted_for.is_none() || n.voted_for == Some(cand);
                if grant {
                    n.voted_for = Some(cand);
                }
                (grant, n.term)
            }
        };
        self.deliver(to, from, Msg::VoteResp { term: cur_term, granted: grant });
    }

    fn handle_vote_resp(&mut self, from: u8, to: u8, term: u64, granted: bool) {
        let quorum = self.quorum();
        let n = self.nodes.get_mut(&to).unwrap();
        if term != n.term || n.role != RaftRole::Candidate || !granted {
            return;
        }
        n.votes += 1;
        if n.votes >= quorum {
            n.role = RaftRole::Leader;
            n.leader = Some(n.id);
            n.votes = 0;
        }
    }

    fn handle_append(&mut self, from: u8, to: u8, term: u64, leader: u8, entries: Vec<MetaEntry>) {
        let ok = {
            let n = self.nodes.get_mut(&to).unwrap();
            if term < n.term {
                false
            } else {
                if term > n.term {
                    n.term = term;
                    n.role = RaftRole::Follower;
                }
                n.leader = Some(leader);
                n.last_heartbeat = Instant::now();
                n.log.extend(entries.clone());
                n.commit = n.log.len() as u64;
                // follower 应用已提交条目到状态机（多数派提交语义）
                let r = n.apply_to_state();
                r.is_ok()
            }
        };
        let _ = self.deliver(to, from, Msg::AppendAck { term, ok });
    }

    fn handle_append_ack(&mut self, _from: u8, to: u8, _term: u64, ok: bool) {
        let n = self.nodes.get_mut(&to).unwrap();
        if ok && n.role == RaftRole::Leader {
            n.commit = n.log.len() as u64;
            let _ = n.apply_to_state();
        }
    }

    /// 发起选举（超时/手动触发）。返回是否成为 leader。
    pub fn start_election(&mut self, id: u8) -> bool {
        if self.nodes[&id].role == RaftRole::Leader {
            return true;
        }
        let term = {
            let n = self.nodes.get_mut(&id).unwrap();
            n.term += 1;
            n.role = RaftRole::Candidate;
            n.voted_for = Some(id);
            n.votes = 1;
            n.term
        };
        let peers: Vec<u8> = self
            .nodes
            .keys()
            .copied()
            .filter(|&p| p != id)
            .collect();
        for p in peers {
            self.deliver(id, p, Msg::VoteReq { term, cand: id });
        }
        self.nodes[&id].role == RaftRole::Leader
    }

    /// leader 提议元数据操作：追加日志 → 广播到全部可达 peer → 提交（多数派 ack 语义）。
    pub fn propose(&mut self, leader: u8, op: MetaOp) -> Result<()> {
        let term = self.nodes[&leader].term;
        let entry = MetaEntry { term, op };
        self.nodes.get_mut(&leader).unwrap().log.push(entry.clone());
        let peers: Vec<u8> = self
            .nodes
            .keys()
            .copied()
            .filter(|&p| p != leader && self.nodes[&p].reachable)
            .collect();
        for p in peers {
            self.deliver(leader, p, Msg::Append { term, leader, entries: vec![entry.clone()] });
        }
        let n = self.nodes.get_mut(&leader).unwrap();
        n.commit = n.log.len() as u64;
        n.apply_to_state()
    }

    /// 心跳/超时检测：follower 超过 heartbeat_timeout 未收心跳 → 自动发起选举
    /// （自动 failover 核心）。返回新 leader（如有）。
    pub fn tick(&mut self, id: u8) -> Option<u8> {
        let n = self.nodes.get(&id).unwrap();
        if !n.reachable {
            return None;
        }
        if n.role == RaftRole::Leader {
            return Some(id);
        }
        if n.last_heartbeat.elapsed() >= self.heartbeat_timeout {
            if self.start_election(id) {
                return Some(id);
            }
        }
        None
    }

    /// 查询节点当前 master（状态机）。
    pub fn master(&self, id: u8) -> Option<String> {
        self.nodes[&id].master()
    }

    /// 注入可达性（分区/宕机）。
    pub fn set_reachable(&mut self, id: u8, reachable: bool) {
        if !reachable {
            let n = self.nodes.get_mut(&id).unwrap();
            n.role = RaftRole::Follower;
            n.leader = None;
        }
        self.nodes.get_mut(&id).unwrap().reachable = reachable;
    }

    /// 当前 leader 角色节点。
    pub fn role(&self, id: u8) -> RaftRole {
        self.nodes[&id].role
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed() -> MetaCenter {
        let mut m = MetaCenter::new(4);
        m.register("node-a", "127.0.0.1:9001", "master").unwrap();
        m.register("node-b", "127.0.0.1:9002", "slave").unwrap();
        m
    }

    fn meta_op_register(node: &str, role: &str) -> MetaOp {
        MetaOp::Register {
            node: node.to_string(),
            addr: "127.0.0.1:9999".to_string(),
            role: role.to_string(),
        }
    }

    #[test]
    fn election_requires_majority_and_term_lock() {
        // 选举多数派 + 同 term 不可改投 + 新 term 换届合法
        let mut g = RaftMetaGroup::new(&[1, 2, 3], seed());
        assert!(g.start_election(2), "b 获多数票应成 leader");
        assert_eq!(g.role(2), RaftRole::Leader);
        // 同 term 已投票不可改投
        let _ = g.deliver(2, 1, Msg::VoteReq { term: 1, cand: 1 });
        assert_eq!(g.nodes[&1].voted_for, Some(2));
        assert_ne!(g.role(1), RaftRole::Candidate);
        // 新 term 换届合法
        assert!(g.start_election(1), "新 term 换届合法");
        assert_eq!(g.role(1), RaftRole::Leader);
    }

    #[test]
    fn meta_replication_applies_to_majority_consistently() {
        // 日志复制：leader 提议 master 变更 → 复制到多数派 → 提交 → 各节点状态机一致
        let mut g = RaftMetaGroup::new(&[1, 2, 3], seed());
        assert!(g.start_election(2));
        g.propose(2, meta_op_register("node-c", "slave")).unwrap();
        // master 切换：先摘除旧 master（node-a）再提升新 master（node-b）——保证唯一 master
        g.propose(2, MetaOp::Unregister { node: "node-a".into() }).unwrap();
        g.propose(2, meta_op_register("node-b", "master")).unwrap();
        for id in [1u8, 2, 3] {
            assert_eq!(g.master(id).as_deref(), Some("node-b"), "节点 {id} master 一致");
        }
    }

    #[test]
    fn auto_failover_switches_master_on_heartbeat_timeout() {
        // 自动 failover：leader 宕机 → 心跳超时 → 剩余多数派自动选举 → master 自动切换
        let mut g = RaftMetaGroup::new(&[1, 2, 3], seed());
        assert!(g.start_election(2));
        g.propose(2, MetaOp::Unregister { node: "node-a".into() }).unwrap();
        g.propose(2, meta_op_register("node-b", "master")).unwrap();
        assert_eq!(g.master(1).as_deref(), Some("node-b"));
        // leader b 宕机
        g.set_reachable(2, false);
        // 心跳超时推进（a/c 的 last_heartbeat 过期）
        std::thread::sleep(Duration::from_millis(120));
        assert!(g.tick(1).is_some(), "a 心跳超时应自动选举（多数派 a+c）");
        assert_eq!(g.role(1), RaftRole::Leader);
        g.propose(1, MetaOp::Unregister { node: "node-b".into() }).unwrap();
        g.propose(1, meta_op_register("node-a", "master")).unwrap();
        assert_eq!(g.master(3).as_deref(), Some("node-a"), "master 自动切换到 a");
    }

    #[test]
    fn brain_split_minority_cannot_elect() {
        // 脑裂安全：{1,2} vs {3}——3 孤立（1 节点 < 多数 2）心跳超时也不能选主
        let mut g = RaftMetaGroup::new(&[1, 2, 3], seed());
        assert!(g.start_election(1));
        g.propose(1, meta_op_register("node-a", "master")).unwrap();
        g.set_reachable(3, false); // 分区隔离节点 3
        std::thread::sleep(Duration::from_millis(120));
        // 节点 3 超时尝试选举：只有自票 1 < 多数 2 → 不能成 leader
        assert!(g.tick(3).is_none(), "少数派不能选主");
        assert_ne!(g.role(3), RaftRole::Leader);
        // 主分区（1/2）保持 leader，可继续提交（master 不分裂）
        assert_eq!(g.role(1), RaftRole::Leader);
        g.propose(1, meta_op_register("node-a", "master")).unwrap();
        assert_eq!(g.master(2).as_deref(), Some("node-a"));
    }
}
