//! raft_rpc 模块测试：消息序列化往返、Local 进程内集群选举/复制/failover、
//! 真实 TCP 三节点接线（选举/复制/failover/多数派）。

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::meta::MetaCenter;
use crate::raft_meta::{MetaEntry, MetaOp, RaftRole};

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
