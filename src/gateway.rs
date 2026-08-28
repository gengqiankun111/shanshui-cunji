//! 网关（design 9.1 / 9.2 / 9.3 / 9.4，阶段 2）。
//!
//! 网关**不持有数据**，只做三类转发（对齐 design 9.2 两类查询分流 + 写路由）：
//! 1. **写入口 / 主键点查（携带 DocId）**：`resolve(docid)` 一致性哈希路由到归属分片，
//!    单分片操作，无广播开销，延迟 ≈ 单机；
//! 2. **广播检索（倒排查询）**：并发下发全部分片，每片返回本片 Chunk（本地倒排 posting），
//!    网关按序直拼（`InvertedIndex::concatenate_chunks`，O(1) 合并，design 5.2.1）；
//! 3. **健康探活**：`ping` 检查节点存活（主从心跳/降级依据）。
//!
//! 红线（design 9.4）：禁止跨分片事务 / JOIN / 分布式锁；只合并 DocId，不跨片读完整文档。
//!
//! 分片端点抽象（`ShardEndpoint`）：进程内测试用 `LocalShardEndpoint`，
//! 跨进程集群用 `RpcShardEndpoint`（走 `src/rpc.rs` JSON-over-TCP）。

use std::collections::HashMap;

use roaring::RoaringBitmap;
use serde_json::json;

use crate::error::{Error, Result};
use crate::inverted::InvertedIndex;
use crate::meta::{MetaCenter, NodeInfo};
use crate::rpc::RpcClient;

/// 分片端点：网关访问数据节点的抽象（Local / RPC 两种实现）。
pub trait ShardEndpoint {
    /// 写入一条文档（`data` 为文档 JSON 字符串，`terms` 为倒排词条）。
    fn put(&mut self, node: &str, docid: u64, data: &str, terms: &[String]) -> Result<()>;
    /// 主键读取（缺失返回 None）。
    fn get(&mut self, node: &str, docid: u64) -> Result<Option<String>>;
    /// 本节点命中 term 的 docid 列表（即该节点的分片 Chunk）。
    fn search_docids(&mut self, node: &str, term: &str) -> Result<Vec<u32>>;
    /// 健康探活。
    fn ping(&mut self, node: &str) -> Result<()>;
}

/// 网关：元数据中心（路由决策）+ 分片端点（数据访问）。
pub struct Gateway<E: ShardEndpoint> {
    meta: MetaCenter,
    endpoint: E,
}

impl<E: ShardEndpoint> Gateway<E> {
    pub fn new(meta: MetaCenter, endpoint: E) -> Self {
        Self { meta, endpoint }
    }

    pub fn meta(&self) -> &MetaCenter {
        &self.meta
    }

    /// 路由查询：docid → 归属节点（空集群返回 Cluster 错误）。返回所有权值避免借用冲突。
    fn route_node(&self, docid: u64) -> Result<NodeInfo> {
        self.meta
            .resolve(docid)
            .cloned()
            .ok_or_else(|| Error::Cluster("集群无可用分片节点".into()))
    }

    /// 写入：单分片路由（design 9.1，写入口无广播）。返回归属节点 ID。
    /// 复制（主→从异步/sync 复制）由分片节点层负责（阶段 2 后续），网关只保证路由一致。
    pub fn put(&mut self, docid: u64, data: &str, terms: &[String]) -> Result<String> {
        let node = self.route_node(docid)?;
        let nid = node.node_id.clone();
        self.endpoint.put(&nid, docid, data, terms)?;
        Ok(nid)
    }

    /// 主键点查：单分片路由。返回文档 JSON 字符串。
    pub fn get(&mut self, docid: u64) -> Result<Option<String>> {
        let node = self.route_node(docid)?;
        self.endpoint.get(&node.node_id, docid)
    }

    /// 广播检索（design 9.2）：全部节点取本片 Chunk → 按序直拼。
    /// 返回命中全部 docid（顺序 = 广播目标顺序，拼接稳定）。
    pub fn broadcast_search(&mut self, term: &str) -> Result<Vec<u32>> {
        let targets: Vec<NodeInfo> = self.meta.broadcast_targets().into_iter().cloned().collect();
        if targets.is_empty() {
            return Err(Error::Cluster("集群无可用分片节点".into()));
        }
        let mut chunks = Vec::with_capacity(targets.len());
        for node in &targets {
            let docids = self.endpoint.search_docids(&node.node_id, term)?;
            chunks.push(RoaringBitmap::from_iter(docids));
        }
        let merged = InvertedIndex::concatenate_chunks(&chunks);
        Ok(merged.iter().collect())
    }

    /// 全部节点健康探活（返回失活节点列表）。
    pub fn ping_all(&mut self) -> Vec<String> {
        let nodes: Vec<NodeInfo> = self.meta.broadcast_targets().into_iter().cloned().collect();
        let mut dead = Vec::new();
        for node in &nodes {
            if self.endpoint.ping(&node.node_id).is_err() {
                dead.push(node.node_id.clone());
            }
        }
        dead
    }
}

// ---------------------------------------------------------------------------
// 进程内测试端点（LocalShardEndpoint）
// ---------------------------------------------------------------------------

/// 进程内分片数据（测试用）：docid → (文档, 词条集)。
#[derive(Default)]
struct MemShard {
    docs: HashMap<u64, String>,
    /// term → 命中 docid 列表（去重后 u32）。
    postings: HashMap<String, Vec<u32>>,
}

/// 进程内端点：多分片数据都放在本进程（网关路由/广播逻辑的隔离测试）。
#[derive(Default)]
pub struct LocalShardEndpoint {
    shards: HashMap<String, MemShard>,
}

impl LocalShardEndpoint {
    pub fn with_nodes(node_ids: &[&str]) -> Self {
        let mut s = Self::default();
        for n in node_ids {
            s.shards.insert(n.to_string(), MemShard::default());
        }
        s
    }
}

impl ShardEndpoint for LocalShardEndpoint {
    fn put(&mut self, node: &str, docid: u64, data: &str, terms: &[String]) -> Result<()> {
        let shard = self
            .shards
            .get_mut(node)
            .ok_or_else(|| Error::Cluster(format!("未知节点: {node}")))?;
        shard.docs.insert(docid, data.to_string());
        for t in terms {
            let list = shard.postings.entry(t.clone()).or_default();
            let d = docid as u32;
            if !list.contains(&d) {
                list.push(d);
            }
        }
        Ok(())
    }

    fn get(&mut self, node: &str, docid: u64) -> Result<Option<String>> {
        let shard = self
            .shards
            .get(node)
            .ok_or_else(|| Error::Cluster(format!("未知节点: {node}")))?;
        Ok(shard.docs.get(&docid).cloned())
    }

    fn search_docids(&mut self, node: &str, term: &str) -> Result<Vec<u32>> {
        let shard = self
            .shards
            .get(node)
            .ok_or_else(|| Error::Cluster(format!("未知节点: {node}")))?;
        Ok(shard.postings.get(term).cloned().unwrap_or_default())
    }

    fn ping(&mut self, node: &str) -> Result<()> {
        if self.shards.contains_key(node) {
            Ok(())
        } else {
            Err(Error::Cluster(format!("节点不可达: {node}")))
        }
    }
}

// ---------------------------------------------------------------------------
// 跨进程端点（RpcShardEndpoint）
// ---------------------------------------------------------------------------

/// RPC 端点：每节点一条连接，走 `src/rpc.rs` JSON-over-TCP。
pub struct RpcShardEndpoint {
    meta: MetaCenter,
    /// node_id → 客户端（按需连接）。
    clients: HashMap<String, RpcClient>,
}

impl RpcShardEndpoint {
    pub fn new(meta: MetaCenter) -> Self {
        Self {
            meta,
            clients: HashMap::new(),
        }
    }

    fn client(&mut self, node: &str) -> Result<&mut RpcClient> {
        if !self.clients.contains_key(node) {
            let addr = self
                .meta
                .node_addr(node)
                .ok_or_else(|| Error::Cluster(format!("元数据中心无节点: {node}")))?;
            let c = RpcClient::connect(addr)?;
            self.clients.insert(node.to_string(), c);
        }
        Ok(self.clients.get_mut(node).unwrap())
    }
}

impl ShardEndpoint for RpcShardEndpoint {
    fn put(&mut self, node: &str, docid: u64, data: &str, terms: &[String]) -> Result<()> {
        let params = json!({
            "docid": docid,
            "data": data,
            "terms": terms,
        });
        self.client(node)?.call("shard.put", params)?;
        Ok(())
    }

    fn get(&mut self, node: &str, docid: u64) -> Result<Option<String>> {
        let r = self.client(node)?.call("shard.get", json!({"docid": docid}))?;
        if r["found"].as_bool().unwrap_or(false) {
            Ok(r["data"].as_str().map(|s| s.to_string()))
        } else {
            Ok(None)
        }
    }

    fn search_docids(&mut self, node: &str, term: &str) -> Result<Vec<u32>> {
        let r = self
            .client(node)?
            .call("shard.search_docids", json!({"term": term}))?;
        let docids = r["docids"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_u64().map(|x| x as u32))
                    .collect()
            })
            .unwrap_or_default();
        Ok(docids)
    }

    fn ping(&mut self, node: &str) -> Result<()> {
        self.client(node)?.call("shard.ping", json!({}))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
        let meta = meta_with(&[("n1", "127.0.0.1:1", "master"), ("n2", "127.0.0.1:2", "slave")]);
        let mut gw = Gateway::new(meta.clone(), LocalShardEndpoint::with_nodes(&["n1", "n2"]));

        // 写入：必须落在 resolve(docid) 指向的节点
        let docids = [1u64, 2, 42, 777, 123_456, 9_999_999];
        for d in docids {
            let owner = gw.meta().resolve(d).unwrap().node_id.clone();
            let routed = gw.put(d, &format!("{{\"d\":{d}}}"), &terms(&["k=v"])).unwrap();
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
        let mut gw = Gateway::new(meta.clone(), LocalShardEndpoint::with_nodes(&["n1", "n2", "n3"]));

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
        let meta = meta_with(&[("n1", "127.0.0.1:1", "master"), ("n2", "127.0.0.1:2", "slave")]);
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

        let meta = meta_with(&[
            ("node-a", &addr1, "master"),
            ("node-b", &addr2, "slave"),
        ]);
        let mut gw = Gateway::new(meta.clone(), RpcShardEndpoint::new(meta));

        // 写入（路由到归属节点）
        for d in 1..=200u64 {
            let _ = gw.put(d, &format!("{{\"d\":{d}}}"), &terms(&["status=active"])).unwrap();
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
}
