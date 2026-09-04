//! `scale_out` 单元测试（原 `src/scale_out.rs` 内嵌 `mod tests` 移出）。

use super::{Phase, RaftRouteChannel, RouteChannel, ScaleOutCoordinator};
use crate::meta::MetaCenter;
use crate::raft_meta::MetaOp;
use crate::raft_rpc::RaftNodeRuntime;
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
