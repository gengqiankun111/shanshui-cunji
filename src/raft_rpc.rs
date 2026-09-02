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
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
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

// ============================ TCP 传输（raft 阶段二真实接线） ============================

/// 单帧长度上限（64MB，防恶意长度放大内存；对齐 rpc.rs）。
const MAX_RAFT_FRAME: usize = 64 * 1024 * 1024;

/// 写一帧 `[u32 LE 长度][JSON]`（复用 rpc.rs 帧格式；握手帧同格式）。
fn write_raft_frame(stream: &mut TcpStream, payload: &[u8]) -> Result<()> {
    let len = (payload.len() as u32).to_le_bytes();
    stream.write_all(&len)?;
    stream.write_all(payload)?;
    stream.flush()?;
    Ok(())
}

/// 读一帧（EOF/连接关闭 → Io 错误 → 调用方退出）。
fn read_raft_frame(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_RAFT_FRAME {
        return Err(Error::Rpc(format!("帧长度超限: {len}")));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf)?;
    Ok(buf)
}

/// TCP 传输：真实节点间 JSON-over-TCP（每节点一个监听端口）。
///
/// - **连接握手**：出站连接建立后首帧声明本节点 id（`{"raft_peer_id":N}`），
///   接收端据此确定消息来源（inbox 项 `(from, msg)`）；
/// - **接收**：accept 线程 + 每连接一个 reader 线程 → 反序列化 → 推入收件箱；
/// - **发送**：懒连接缓存（peer id → TcpStream），断线清理下次重连；
/// - `recv` 非阻塞（pop 收件箱），驱动循环轮询（RaftNodeRuntime::pump 语义不变）。
pub struct TcpRaftTransport {
    id: u8,
    peers: HashMap<u8, String>,
    outbound: Mutex<HashMap<u8, TcpStream>>,
    inbox: Arc<Mutex<VecDeque<(u8, RaftMsg)>>>,
    listener: Option<TcpListener>,
    stop: Arc<AtomicBool>,
    accept_thread: Option<JoinHandle<()>>,
}

impl TcpRaftTransport {
    /// 绑定监听端口（`listen_addr` 可 `127.0.0.1:0` 自动分配，经 `peer_addr()` 查询）。
    pub fn bind(id: u8, listen_addr: &str) -> Result<Self> {
        let listener = TcpListener::bind(listen_addr)?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let inbox: Arc<Mutex<VecDeque<(u8, RaftMsg)>>> = Arc::new(Mutex::new(VecDeque::new()));
        let inbox_clone = Arc::clone(&inbox);
        let stop_clone = Arc::clone(&stop);
        let listener_clone = listener
            .try_clone()
            .map_err(|e| Error::Io(e))?;
        // accept 线程：非阻塞轮询 + 每连接一个 reader 线程
        let accept_thread = std::thread::spawn(move || {
            let inbox = Arc::clone(&inbox_clone);
            loop {
                if stop_clone.load(AtomicOrdering::Acquire) {
                    break;
                }
                match listener_clone.accept() {
                    Ok((stream, _)) => {
                        let inbox = Arc::clone(&inbox);
                        std::thread::spawn(move || {
                            let mut stream = stream;
                            // 握手：首帧 = 声明对端节点 id
                            let handshake = match read_raft_frame(&mut stream) {
                                Ok(b) => b,
                                Err(_) => return,
                            };
                            let from: u8 = serde_json::from_slice::<serde_json::Value>(&handshake)
                                .ok()
                                .and_then(|v| v.get("raft_peer_id")?.as_u64())
                                .and_then(|v| u8::try_from(v).ok())
                                .unwrap_or(255);
                            loop {
                                match read_raft_frame(&mut stream) {
                                    Ok(b) => {
                                        if let Ok(msg) = serde_json::from_slice::<RaftMsg>(&b) {
                                            inbox.lock().unwrap().push_back((from, msg));
                                        }
                                    }
                                    Err(_) => break, // 连接关闭/对端断开
                                }
                            }
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => {
                        if stop_clone.load(AtomicOrdering::Acquire) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                }
            }
        });
        Ok(Self {
            id,
            peers: HashMap::new(),
            outbound: Mutex::new(HashMap::new()),
            inbox,
            listener: Some(listener),
            stop,
            accept_thread: Some(accept_thread),
        })
    }

    /// 实际监听地址（`127.0.0.1:0` 绑定后查询真实端口）。
    pub fn peer_addr(&self) -> Result<String> {
        Ok(self
            .listener
            .as_ref()
            .ok_or_else(|| Error::Rpc("listener 已关闭".into()))?
            .local_addr()?
            .to_string())
    }

    /// 登记对端节点地址（peer id → 节点监听地址）。
    pub fn add_peer(&mut self, peer_id: u8, addr: String) {
        self.peers.insert(peer_id, addr);
    }

    /// 停止传输（Drop 兜底）：置 stop 标志 + 释放监听端口。
    /// **不 join accept 线程**——Windows 上 std 非阻塞 accept 实为阻塞等待（内部 poll 无
    /// 超时），stop 无法中断其 accept，join 会挂死（实测挂起根因之一）；Linux 非阻塞
    /// accept 返回 EAGAIN 轮询可正常退出。accept 线程在进程退出时回收（测试/节点停机
    /// 场景均可接受）。
    pub fn shutdown(&mut self) {
        self.stop.store(true, AtomicOrdering::Release);
        self.listener = None; // 释放监听端口
        self.accept_thread.take();
    }

    /// 获取（或建立）到 peer 的连接：首次连接发握手帧声明本节点 id。
    /// 网络调用全部带超时（连接 2s / 读写 5s），防对端不可达挂死驱动循环。
    fn conn(&self, to: u8) -> Result<TcpStream> {
        let addr = self
            .peers
            .get(&to)
            .ok_or_else(|| Error::Rpc(format!("未知 peer {to}")))?;
        let sock: std::net::SocketAddr = addr
            .parse()
            .map_err(|_| Error::Rpc(format!("peer 地址非法: {addr}")))?;
        let mut stream = TcpStream::connect_timeout(&sock, Duration::from_secs(2))?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        let handshake = serde_json::json!({"raft_peer_id": self.id}).to_string();
        write_raft_frame(&mut stream, handshake.as_bytes())?;
        Ok(stream)
    }

    /// 序列化 RaftMsg（serde 错误 → Error::Serialize）。
    fn encode(msg: &RaftMsg) -> Result<Vec<u8>> {
        serde_json::to_vec(msg).map_err(|e| Error::Serialize(e.to_string()))
    }
}

impl Drop for TcpRaftTransport {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl RaftTransport for TcpRaftTransport {
    fn send(&mut self, to: u8, msg: RaftMsg) -> Result<()> {
        let payload = Self::encode(&msg)?;
        // 锁作用域关键：remove 的 MutexGuard 必须显式结束——`if let Some(s) =
        // self.outbound.lock().unwrap().remove(&to)` 的 if-let scrutinee 临时 guard 存活到
        // 整个 if 块，内层 insert 同线程二次 lock 同一 Mutex 死锁（实测挂起根因）。
        let cached = self.outbound.lock().unwrap().remove(&to);
        if let Some(mut s) = cached {
            match write_raft_frame(&mut s, &payload) {
                Ok(()) => {
                    self.outbound.lock().unwrap().insert(to, s);
                    return Ok(());
                }
                Err(_) => {} // 断开 → 清理重连
            }
        }
        let mut s = self.conn(to)?;
        write_raft_frame(&mut s, &payload)?;
        self.outbound.lock().unwrap().insert(to, s);
        Ok(())
    }
    fn recv(&mut self) -> Result<Option<(u8, RaftMsg)>> {
        Ok(self.inbox.lock().unwrap().pop_front())
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
    /// 最近一次发起选举的时刻（选举冷却：candidate 在 timeout 内不重复 term++，
    /// 防 TCP 异步下 VoteResp 到达时 term 已递增被忽略）。
    last_election: Instant,
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

/// 测试辅助（跨模块 e2e）：强制 `rt[target]` 超时并 pump 至当选 leader。
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

    // ============ 真实 TCP 三节点（raft 阶段二接线验证） ============

    #[test]
    fn tcp_transport_roundtrip() {
        // 纯传输层往返（不经状态机）：n1 → n2 一条 VoteReq，n2 轮询 recv 应收到
        let mut a = TcpRaftTransport::bind(1, "127.0.0.1:0").unwrap();
        let mut b = TcpRaftTransport::bind(2, "127.0.0.1:0").unwrap();
        a.add_peer(2, b.peer_addr().unwrap());
        b.add_peer(1, a.peer_addr().unwrap());
        a.send(2, RaftMsg::VoteReq { term: 1, cand: 1 }).unwrap();
        let mut got = None;
        for _ in 0..100 {
            if let Some(m) = b.recv().unwrap() {
                got = Some(m);
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(got, Some((1, RaftMsg::VoteReq { term: 1, cand: 1 })), "传输往返");
    }

    /// 真实 TCP 3 节点集群（各自绑定随机端口，互相登记地址）。
    fn cluster3_tcp() -> Vec<RaftNodeRuntime<TcpRaftTransport>> {
        let mut transports: Vec<(u8, TcpRaftTransport)> = Vec::new();
        let mut addrs: Vec<(u8, String)> = Vec::new();
        for id in 1..=3u8 {
            let t = TcpRaftTransport::bind(id, "127.0.0.1:0").unwrap();
            addrs.push((id, t.peer_addr().unwrap()));
            transports.push((id, t));
        }
        let mut runtimes = Vec::new();
        for (id, mut t) in transports {
            let peers_ids: Vec<u8> = (1..=3u8).filter(|&p| p != id).collect();
            for &pid in &peers_ids {
                let addr = addrs.iter().find(|(aid, _)| *aid == pid).unwrap().1.clone();
                t.add_peer(pid, addr);
            }
            runtimes.push(RaftNodeRuntime::new(id, peers_ids, seed(), t));
        }
        runtimes
    }

    /// TCP 版只让 target 竞选（其余节点心跳刷新不超时；网络异步：每轮间小 sleep）。
    fn elect_single_tcp(rt: &mut [RaftNodeRuntime<TcpRaftTransport>], target: usize) {
        let far = Instant::now() + Duration::from_millis(60_000);
        for (i, r) in rt.iter_mut().enumerate() {
            if i != target {
                r.last_heartbeat = far;
            }
        }
        for _ in 0..200 {
            for r in rt.iter_mut() {
                r.pump(Instant::now() + Duration::from_millis(200)).unwrap();
            }
            if rt[target].role() == RaftRole::Leader {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("TCP 选举超时（target 未当选）");
    }

    #[test]
    fn tcp_election_via_network() {
        let mut rt = cluster3_tcp();
        elect_single_tcp(&mut rt, 0);
        assert_eq!(rt[0].role(), RaftRole::Leader, "节点 1 经真实 TCP 当选");
        assert_eq!(rt[1].role(), RaftRole::Follower);
        assert_eq!(rt[2].role(), RaftRole::Follower);
    }

    #[test]
    fn tcp_log_replication_via_network() {
        let mut rt = cluster3_tcp();
        elect_single_tcp(&mut rt, 0);
        rt[0].propose(register_op("tcp-node")).unwrap();
        for _ in 0..100 {
            for r in rt.iter_mut() {
                r.pump(Instant::now() + Duration::from_millis(200)).unwrap();
            }
            if rt[0].master().as_deref() == Some("tcp-node")
                && rt[1].master().as_deref() == Some("tcp-node")
                && rt[2].master().as_deref() == Some("tcp-node")
            {
                return; // 3 节点状态一致
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("TCP 日志复制未达一致: {:?}", (rt[0].master(), rt[1].master(), rt[2].master()));
    }

    #[test]
    fn tcp_failover_leader_down() {
        let mut rt = cluster3_tcp(); // [节点1, 节点2, 节点3]
        elect_single_tcp(&mut rt, 0);
        assert_eq!(rt[0].role(), RaftRole::Leader);
        // leader（节点 1）真实停机：drop 其 transport（listener/连接关闭）
        let mut down = rt.remove(0); // rt 变为 [节点2, 节点3]
        down.transport.shutdown();
        drop(down);
        // 节点 2（现 rt[0]）竞选：心跳置旧触发超时；节点 3（现 rt[1]）刷新不竞选
        rt[0].last_heartbeat = Instant::now() - Duration::from_millis(300);
        rt[1].last_heartbeat = Instant::now() + Duration::from_millis(60_000);
        let mut new_leader = None;
        for _ in 0..200 {
            rt[0].pump(Instant::now() + Duration::from_millis(200)).unwrap();
            rt[1].pump(Instant::now() + Duration::from_millis(200)).unwrap();
            if rt[0].role() == RaftRole::Leader {
                new_leader = Some(2);
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(new_leader, Some(2), "TCP failover：节点 2 超时当选新 leader");
        // 新 leader（节点 2）继续提议
        rt[0].propose(register_op("after-down")).unwrap();
        for _ in 0..100 {
            rt[0].pump(Instant::now() + Duration::from_millis(200)).unwrap();
            rt[1].pump(Instant::now() + Duration::from_millis(200)).unwrap();
            if rt[0].master().as_deref() == Some("after-down")
                && rt[1].master().as_deref() == Some("after-down")
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("failover 后提议未达一致");
    }
}
