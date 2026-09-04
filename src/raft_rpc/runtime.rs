//! `RaftNodeRuntime`：单节点 Raft 状态机（term/role/log/commit/votes）+ transport 驱动。

use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::meta::MetaCenter;
use crate::raft_meta::{MetaEntry, MetaOp, RaftRole};

use super::{RaftMsg, RaftTransport};

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
    /// 模块测试（tests.rs 心跳置旧触发超时/竞选）直访 → pub(super)。
    pub(super) last_heartbeat: Instant,
    /// 最近一次发起选举的时刻（选举冷却：candidate 在 timeout 内不重复 term++，
    /// 防 TCP 异步下 VoteResp 到达时 term 已递增被忽略）。
    last_election: Instant,
    peers: Vec<u8>,
    quorum: u32,
    heartbeat_timeout: Duration,
    /// 传输后端（模块测试停机演练 `transport.shutdown()` 直访 → pub(super)）。
    pub(super) transport: T,
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
            last_election: Instant::now(),
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
            self.handle(from, msg, now)?;
        }
        Ok(self.maybe_elect(now))
    }

    fn handle(&mut self, from: u8, msg: RaftMsg, now: Instant) -> Result<()> {
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
                    // 心跳用驱动时钟 now 刷新（与 maybe_elect 同基准；若用真实时钟，
                    // 注入未来 now 的驱动循环会使刚收 Append 的 follower 立即"超时"竞选）
                    self.last_heartbeat = now;
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
        // 选举冷却：距上次发起选举不足 timeout 不重复（candidate 不逐轮 term++，
        // 否则 TCP 异步下 VoteResp 到达时 term 已递增被忽略，永不当选）
        if now.duration_since(self.last_election) < self.heartbeat_timeout {
            return None;
        }
        // 超时 → 选举：term+1、自投、广播 VoteReq
        self.term += 1;
        self.role = RaftRole::Candidate;
        self.voted_for = Some(self.id);
        self.votes = 1;
        self.last_election = now;
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

/// 测试辅助（跨模块 e2e，scale_out.rs 测试使用）：强制 `rt[target]` 超时并 pump 至当选 leader。
/// 置 target 的 last_heartbeat/last_election 为过去（同时满足心跳超时与选举冷却），
/// 其余节点心跳置未来（不竞选）。
#[cfg(test)]
pub(crate) fn force_election<T: RaftTransport>(rt: &mut [RaftNodeRuntime<T>], target: usize) {
    let past = Instant::now() - Duration::from_millis(600);
    rt[target].last_heartbeat = past;
    rt[target].last_election = past;
    for (i, r) in rt.iter_mut().enumerate() {
        if i != target {
            r.last_heartbeat = Instant::now() + Duration::from_millis(60_000);
        }
    }
    for _ in 0..200 {
        let t = Instant::now() + Duration::from_millis(300);
        for r in rt.iter_mut() {
            r.pump(t).unwrap();
        }
        if rt[target].role() == RaftRole::Leader {
            return;
        }
    }
    panic!("force_election 失败: 节点 {target} 未当选");
}
