//! 网关测试（自原 `src/gateway.rs` 的 `#[cfg(test)] mod tests` 原样迁出）。

use super::*;

fn terms(t: &[&str]) -> Vec<String> {
    t.iter().map(|s| s.to_string()).collect()
}

fn meta_with(nodes: &[(&str, &str, &str)]) -> MetaCenter {
    let mut m = MetaCenter::new(1024);
    for (id, addr, role) in nodes {
        m.register(id, addr, role).unwrap();
    }
    m
}

// ---- 路由查询（Local 端点）----

#[test]
fn put_and_get_route_to_owning_node() {
    let meta = meta_with(&[
        ("n1", "127.0.0.1:1", "master"),
        ("n2", "127.0.0.1:2", "slave"),
    ]);
    let mut gw = Gateway::new(meta.clone(), LocalShardEndpoint::with_nodes(&["n1", "n2"]));

    // 写入：必须落在 resolve(docid) 指向的节点
    let docids = [1u64, 2, 42, 777, 123_456, 9_999_999];
    for d in docids {
        let owner = gw.meta().resolve(d).unwrap().node_id.clone();
        let routed = gw
            .put(d, &format!("{{\"d\":{d}}}"), &terms(&["k=v"]))
            .unwrap();
        assert_eq!(routed, owner, "写入必须路由到归属节点");
    }
    // 主键点查：全命中
    for d in docids {
        let v = gw.get(d).unwrap().unwrap();
        assert!(v.contains(&format!("\"d\":{d}")), "读取失败 docid={d}");
    }
    // 未写入的 docid → None
    assert!(gw.get(888_888).unwrap().is_none());
}

// ---- 广播检索（design 9.2）----

#[test]
fn broadcast_search_concatenates_chunks_across_nodes() {
    let meta = meta_with(&[
        ("n1", "127.0.0.1:1", "master"),
        ("n2", "127.0.0.1:2", "slave"),
        ("n3", "127.0.0.1:3", "slave"),
    ]);
    let mut gw = Gateway::new(
        meta.clone(),
        LocalShardEndpoint::with_nodes(&["n1", "n2", "n3"]),
    );

    // 写入 300 条：只给 n1 的 term 加词条，其他节点无该词条
    for d in 1..=300u64 {
        let _ = gw.put(d, "{\"s\":1}", &terms(&["status=active"])).unwrap();
    }
    // 每个节点只该持有自己路由到的 docid（Chunk）
    let mut per_node_total = 0usize;
    for n in ["n1", "n2", "n3"] {
        per_node_total += gw.endpoint.search_docids(n, "status=active").unwrap().len();
    }
    assert_eq!(per_node_total, 300, "各节点 Chunk 总和 = 全部 docid");

    // 广播检索 = 各 Chunk 按序直拼
    let all = gw.broadcast_search("status=active").unwrap();
    assert_eq!(all.len(), 300);
    // 覆盖全部写入 docid
    for d in 1..=300u32 {
        assert!(all.contains(&d), "广播结果缺少 docid={d}");
    }
    // 无命中的 term
    assert!(gw.broadcast_search("status=pending").unwrap().is_empty());
}

#[test]
fn broadcast_on_empty_cluster_errors() {
    let meta = MetaCenter::new(1024);
    let mut gw = Gateway::new(meta, LocalShardEndpoint::default());
    assert!(gw.broadcast_search("t").is_err());
}

// ---- 健康探活 ----

#[test]
fn ping_all_reports_dead_nodes() {
    let meta = meta_with(&[
        ("n1", "127.0.0.1:1", "master"),
        ("n2", "127.0.0.1:2", "slave"),
    ]);
    let mut gw = Gateway::new(meta, LocalShardEndpoint::with_nodes(&["n1"])); // n2 未建 → 失活
    let dead = gw.ping_all();
    assert_eq!(dead, vec!["n2".to_string()]);
}

// ---- 跨进程端到端（真实 TCP，Engine 分片节点）----

fn spawn_shard_node(dir: &std::path::Path) -> String {
    use crate::engine::Engine;
    use crate::rpc::{register_shard_handlers, RpcServer};
    use std::sync::{Arc, Mutex};

    let cfg = crate::config::Config::default();
    let engine = Arc::new(Mutex::new(Engine::open(dir, &cfg).unwrap()));
    let server = RpcServer::new();
    register_shard_handlers(&server, engine);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let handlers = server.handlers.lock().unwrap().clone();
    std::thread::spawn(move || {
        for s in listener.incoming() {
            if let Ok(s) = s {
                let h = handlers.clone();
                std::thread::spawn(move || crate::rpc::handle_connection(h, s));
            }
        }
    });
    addr
}

#[test]
fn gateway_e2e_over_real_tcp_with_engine_nodes() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let addr1 = spawn_shard_node(d1.path());
    let addr2 = spawn_shard_node(d2.path());

    let meta = meta_with(&[("node-a", &addr1, "master"), ("node-b", &addr2, "slave")]);
    let mut gw = Gateway::new(meta.clone(), RpcShardEndpoint::new(meta));

    // 写入（路由到归属节点）
    for d in 1..=200u64 {
        let _ = gw
            .put(d, &format!("{{\"d\":{d}}}"), &terms(&["status=active"]))
            .unwrap();
    }
    // 主键点查
    for d in [1u64, 100, 200] {
        let v = gw.get(d).unwrap().unwrap();
        assert!(v.contains(&format!("\"d\":{d}")));
    }
    assert!(gw.get(9999).unwrap().is_none());
    // 广播检索
    let all = gw.broadcast_search("status=active").unwrap();
    assert_eq!(all.len(), 200);
    for d in 1..=200u32 {
        assert!(all.contains(&d));
    }
    // 探活：两节点均在线
    assert!(gw.ping_all().is_empty());
}

#[test]
fn high_concurrency_writes_strong_consistency_two_nodes() {
    // 两节点真实 TCP 集群 + 高并发写 → 强一致（无丢失/无重复，docid 确定性路由到归属节点）
    use std::sync::{Arc, Mutex};
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let addr1 = spawn_shard_node(d1.path());
    let addr2 = spawn_shard_node(d2.path());
    let meta = meta_with(&[("node-a", &addr1, "master"), ("node-b", &addr2, "slave")]);
    let gw = Arc::new(Mutex::new(Gateway::new(meta.clone(), RpcShardEndpoint::new(meta))));

    // 8 线程 × 2500 = 20000 条并发写（每线程独立 docid 区间，无跨线程竞争）
    let mut hs = Vec::new();
    for t in 0..8u64 {
        let gw = Arc::clone(&gw);
        hs.push(std::thread::spawn(move || {
            for i in 0..2500u64 {
                let d = t * 2500 + i + 1;
                gw.lock()
                    .unwrap()
                    .put(d, &format!("{{\"d\":{d}}}"), &terms(&["status=active"]))
                    .unwrap();
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    let mut gw = gw.lock().unwrap();

    // 强一致：广播检索精确 20000（无丢失无重复）；逐条点查全部可见（跨节点确定性路由）
    let all = gw.broadcast_search("status=active").unwrap();
    assert_eq!(all.len(), 20000, "并发写后广播检索无丢失/重复");
    for d in 1..=20000u64 {
        assert!(gw.get(d).unwrap().is_some(), "docid={d} 强一致可见");
    }
    assert!(gw.ping_all().is_empty(), "两节点在线");
    eprintln!("[高并发强一致] 2 节点真实 TCP × 8 线程 × 2500 并发写：20000 条全部可见、广播精确命中");
}

#[test]
fn gateway_put_batch_routes_and_counts() {
    // C 项②：Gateway::put_batch 按 docid 一致性哈希分组 → 各节点批量写入，
    // 计数 = 输入条数（无丢失/无重复），广播检索精确命中、逐条可见
    let mut meta = MetaCenter::new(128);
    meta.register("n1", "127.0.0.1:19001", "master").unwrap();
    meta.register("n2", "127.0.0.1:19002", "slave").unwrap();
    let mut gw = Gateway::new(meta.clone(), LocalShardEndpoint::with_nodes(&["n1", "n2"]));
    let items: Vec<(u64, String, Vec<String>)> = (1..=1000u64)
        .map(|d| (d, format!("{{\"d\":{d}}}"), terms(&["status=active"])))
        .collect();
    let counts = gw.put_batch(&items).unwrap();
    let total: usize = counts.values().sum();
    assert_eq!(total, 1000, "批量写入合计 = 输入条数");
    assert_eq!(counts.len(), 2, "docid 应路由到两节点");
    for (n, c) in &counts {
        assert!(*c > 0, "节点 {n} 应有写入");
    }
    // 广播检索精确命中 1000（无丢失/无重复）
    let all = gw.broadcast_search("status=active").unwrap();
    assert_eq!(all.len(), 1000);
    // 逐条点查：跨节点确定性路由强一致可见
    for d in 1..=1000u64 {
        assert!(gw.get(d).unwrap().is_some(), "docid={d} 批量后可见");
    }
    eprintln!("[put_batch 分组路由] 1000 条批量 → 节点分布 {counts:?}，广播精确命中、逐条可见");
}

// ---- 网关全局 Term 缓存（design 9.9）----

/// 包装端点：统计后端 search_docids 调用次数（验证缓存是否直出）。
struct CountingEndpoint {
    inner: LocalShardEndpoint,
    searches: usize,
}

impl ShardEndpoint for CountingEndpoint {
    fn put(&mut self, node: &str, docid: u64, data: &str, terms: &[String]) -> Result<()> {
        self.inner.put(node, docid, data, terms)
    }
    fn get(&mut self, node: &str, docid: u64) -> Result<Option<String>> {
        self.inner.get(node, docid)
    }
    fn search_docids(&mut self, node: &str, term: &str) -> Result<Vec<u32>> {
        self.searches += 1;
        self.inner.search_docids(node, term)
    }
    fn scan_all(&mut self, node: &str) -> Result<Vec<(u64, String)>> {
        self.inner.scan_all(node)
    }
    fn ping(&mut self, node: &str) -> Result<()> {
        self.inner.ping(node)
    }
    fn add_node(&mut self, node_id: &str, addr: Option<&str>) -> Result<()> {
        self.inner.add_node(node_id, addr)
    }
}

#[test]
fn term_cache_hit_serves_without_backend_calls() {
    let meta = meta_with(&[
        ("n1", "127.0.0.1:1", "master"),
        ("n2", "127.0.0.1:2", "slave"),
    ]);
    let endpoint = CountingEndpoint {
        inner: LocalShardEndpoint::with_nodes(&["n1", "n2"]),
        searches: 0,
    };
    let cache =
        crate::term_cache::TermCache::new(1000, std::time::Duration::from_secs(60), 100);
    let mut gw = Gateway::new_with_term_cache(meta, endpoint, cache);

    for d in 1..=50u64 {
        let _ = gw.put(d, "{\"s\":1}", &terms(&["status=active"])).unwrap();
    }
    // 首次广播：每节点 1 次后端调用，共 2 次
    let first = gw.broadcast_search("status=active").unwrap();
    assert_eq!(first.len(), 50);
    assert_eq!(gw.endpoint.searches, 2, "首次应全部回源");
    // 二次广播：缓存直出，0 后端调用
    let second = gw.broadcast_search("status=active").unwrap();
    assert_eq!(second, first, "缓存命中结果必须一致");
    assert_eq!(gw.endpoint.searches, 2, "缓存命中不应打后端分片");
    // 不同 term 未命中 → 回源
    let _ = gw.broadcast_search("status=pending").unwrap();
    assert_eq!(gw.endpoint.searches, 4, "新 term 应回源并回填");
}

#[test]
fn term_cache_ttl_revalidates_after_expiry() {
    let meta = meta_with(&[("n1", "127.0.0.1:1", "master")]);
    let endpoint = CountingEndpoint {
        inner: LocalShardEndpoint::with_nodes(&["n1"]),
        searches: 0,
    };
    let cache =
        crate::term_cache::TermCache::new(1000, std::time::Duration::from_millis(30), 100);
    let mut gw = Gateway::new_with_term_cache(meta, endpoint, cache);

    let _ = gw.put(1, "{\"s\":1}", &terms(&["status=active"])).unwrap();
    let _ = gw.broadcast_search("status=active").unwrap();
    assert_eq!(gw.endpoint.searches, 1);
    // TTL 内命中
    let _ = gw.broadcast_search("status=active").unwrap();
    assert_eq!(gw.endpoint.searches, 1);
    // TTL 过期 → 重新拉取
    std::thread::sleep(std::time::Duration::from_millis(50));
    let _ = gw.broadcast_search("status=active").unwrap();
    assert_eq!(gw.endpoint.searches, 2, "TTL 过期后应重新拉取");
}

#[test]
fn term_cache_write_count_invalidates() {
    let meta = meta_with(&[("n1", "127.0.0.1:1", "master")]);
    let endpoint = CountingEndpoint {
        inner: LocalShardEndpoint::with_nodes(&["n1"]),
        searches: 0,
    };
    // 阈值 3：同 term 写入 4 次后缓存应失效
    let cache = crate::term_cache::TermCache::new(1000, std::time::Duration::from_secs(60), 3);
    let mut gw = Gateway::new_with_term_cache(meta, endpoint, cache);

    let _ = gw.put(1, "{\"s\":1}", &terms(&["hot=1"])).unwrap();
    let _ = gw.broadcast_search("hot=1").unwrap();
    assert_eq!(gw.endpoint.searches, 1);
    // 缓存命中（未回源）
    let _ = gw.broadcast_search("hot=1").unwrap();
    assert_eq!(gw.endpoint.searches, 1);

    // 同 term 高频写入触发失效
    for d in 2..=5u64 {
        let _ = gw.put(d, "{\"s\":1}", &terms(&["hot=1"])).unwrap();
    }
    // 失效后广播 → 回源（searches +1），且结果包含新 docid
    let all = gw.broadcast_search("hot=1").unwrap();
    assert_eq!(all.len(), 5, "失效后应看到全部 5 条");
    assert_eq!(gw.endpoint.searches, 2, "写超阈值后应重新回源");
}

// ---- 无损扩容协议（design 9.1.1）----

fn vs_of(docid: u64, vs: u32) -> u32 {
    (crate::sharding::hash64(docid) % vs as u64) as u32
}

#[test]
fn reshard_lifecycle_shadow_catchup_switch() {
    let meta = meta_with(&[
        ("n1", "127.0.0.1:1", "master"),
        ("n2", "127.0.0.1:2", "slave"),
    ]);
    let mut gw = Gateway::new(meta.clone(), LocalShardEndpoint::with_nodes(&["n1", "n2"]));

    // 扩容前存量数据（terms 与 data 字段一致，catch_up 用 extract_terms 重新派生）
    for d in 1..=200u64 {
        let _ = gw
            .put(
                d,
                &format!("{{\"status\":\"active\",\"d\":{d}}}"),
                &terms(&["status=active"]),
            )
            .unwrap();
    }

    // 阶段一：双写（Shadow Writes）
    let moved = gw.begin_migration("n3", "127.0.0.1:3", "slave").unwrap();
    assert!(moved > 0 && moved < 1024, "应只迁移部分虚拟分片: {moved}");
    let moved_set = gw.migration().unwrap().moved_vshards.clone();

    // 迁移期间新写入：迁移分片 docid 双写（老节点 + n3）
    let mut shadowed = 0;
    for d in 201..=220u64 {
        let _ = gw
            .put(
                d,
                &format!("{{\"status\":\"active\",\"d\":{d}}}"),
                &terms(&["status=active"]),
            )
            .unwrap();
        if moved_set.contains(&vs_of(d, 1024)) {
            shadowed += 1;
        }
    }
    assert!(shadowed > 0, "应有迁移分片的新写入");
    // n3 已收到双写数据
    for d in 201..=220u64 {
        if moved_set.contains(&vs_of(d, 1024)) {
            assert!(
                gw.endpoint.get("n3", d).unwrap().is_some(),
                "双写数据必须已落 n3: docid={d}"
            );
        }
    }

    // 阶段二：数据追平（Delta Catch-up）
    let copied = gw.catch_up().unwrap();
    assert!(copied > 0, "应追平迁移分片的存量数据");
    // 追平后 n3 拥有全部迁移分片存量数据
    for d in 1..=200u64 {
        if moved_set.contains(&vs_of(d, 1024)) {
            assert!(
                gw.endpoint.get("n3", d).unwrap().is_some(),
                "追平缺失: docid={d}"
            );
        }
    }

    // 阶段三：原子切换（Atomic Switch）
    let switched = gw.commit_migration().unwrap();
    assert_eq!(switched, moved);
    assert!(gw.migration().is_none(), "切换后双写关闭");
    assert!(
        gw.meta().node_ids().contains(&"n3".to_string()),
        "n3 已入元数据中心"
    );

    // 切换后：迁移分片读路由到 n3 且数据完整（业务零感知）
    for d in 1..=220u64 {
        if moved_set.contains(&vs_of(d, 1024)) {
            let v = gw.get(d).unwrap().unwrap();
            assert!(
                v.contains(&format!("\"d\":{d}")),
                "切换后读取失败 docid={d}"
            );
        }
    }
    // 广播检索全量一致（三节点 Chunk 直拼）
    let all = gw.broadcast_search("status=active").unwrap();
    assert_eq!(all.len(), 220, "扩容后广播结果必须完整");
}

#[test]
fn reshard_abort_keeps_old_routing() {
    let meta = meta_with(&[("n1", "127.0.0.1:1", "master")]);
    let mut gw = Gateway::new(meta.clone(), LocalShardEndpoint::with_nodes(&["n1"]));
    gw.put(1, "{\"status\":\"active\"}", &terms(&["status=active"]))
        .unwrap();

    let moved = gw.begin_migration("n2", "127.0.0.1:2", "slave").unwrap();
    assert!(moved > 0);
    // 回滚：n2 不注册，路由不变（旧数据完好）
    gw.abort_migration();
    assert!(gw.migration().is_none());
    assert!(!gw.meta().node_ids().contains(&"n2".to_string()));
    for d in 0..2000u64 {
        assert_eq!(
            gw.meta().resolve(d).unwrap().node_id,
            "n1",
            "回滚后路由必须不变"
        );
    }
    // 数据完好
    assert!(gw.get(1).unwrap().is_some());
}

#[test]
fn reshard_begin_twice_rejected() {
    let meta = meta_with(&[("n1", "127.0.0.1:1", "master")]);
    let mut gw = Gateway::new(meta, LocalShardEndpoint::with_nodes(&["n1"]));
    gw.begin_migration("n2", "127.0.0.1:2", "slave").unwrap();
    assert!(
        gw.begin_migration("n2", "127.0.0.1:2", "slave").is_err(),
        "重复迁移应拒绝"
    );
}
