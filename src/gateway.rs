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
use crate::reshard::{compute_moved_vshards, Migration};
use crate::rpc::RpcClient;
use crate::sharding::hash64;
use crate::term_cache::TermCache;

/// 分片端点：网关访问数据节点的抽象（Local / RPC 两种实现）。
pub trait ShardEndpoint {
    /// 写入一条文档（`data` 为文档 JSON 字符串，`terms` 为倒排词条）。
    fn put(&mut self, node: &str, docid: u64, data: &str, terms: &[String]) -> Result<()>;
    /// 主键读取（缺失返回 None）。
    fn get(&mut self, node: &str, docid: u64) -> Result<Option<String>>;
    /// 本节点命中 term 的 docid 列表（即该节点的分片 Chunk）。
    fn search_docids(&mut self, node: &str, term: &str) -> Result<Vec<u32>>;
    /// 全量扫描本节点全部 (docid, 文档) 对（扩容数据追平用，design 9.1.1）。
    fn scan_all(&mut self, node: &str) -> Result<Vec<(u64, String)>>;
    /// 健康探活。
    fn ping(&mut self, node: &str) -> Result<()>;
    /// 注册可直接访问的新节点（双写 / 追平用，不改路由）。
    fn add_node(&mut self, node_id: &str, addr: Option<&str>) -> Result<()>;
}

/// 网关：元数据中心（路由决策）+ 分片端点（数据访问）+ 全局 Term 缓存（design 9.9）。
pub struct Gateway<E: ShardEndpoint> {
    meta: MetaCenter,
    endpoint: E,
    /// 全局 Term 缓存（None = 关闭）。
    term_cache: Option<TermCache>,
    /// 无损扩容迁移状态（None = 无迁移）。
    migration: Option<Migration>,
}

impl<E: ShardEndpoint> Gateway<E> {
    /// 构建网关（不带 Term 缓存）。
    pub fn new(meta: MetaCenter, endpoint: E) -> Self {
        Self {
            meta,
            endpoint,
            term_cache: None,
            migration: None,
        }
    }

    /// 构建网关并启用全局 Term 缓存（design 9.9）。
    pub fn new_with_term_cache(meta: MetaCenter, endpoint: E, term_cache: TermCache) -> Self {
        Self {
            meta,
            endpoint,
            term_cache: Some(term_cache),
            migration: None,
        }
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
    /// 复制（主→从异步/sync 复制）由分片节点层负责，网关只保证路由一致。
    /// 扩容迁移期间（design 9.1.1 双写）：属于迁移虚拟分片的 DocId 同时写入新节点。
    /// 同时记录 Term 写计数（design 9.9：超阈值主动失效全局缓存）。
    pub fn put(&mut self, docid: u64, data: &str, terms: &[String]) -> Result<String> {
        let node = self.route_node(docid)?;
        let nid = node.node_id.clone();
        self.endpoint.put(&nid, docid, data, terms)?;
        // 双写（Shadow Writes）：迁移分片的新 docid 写入老节点后，同时写新节点
        if let Some(m) = &self.migration {
            let vs = self.virtual_shard_of(docid);
            if m.is_migrating(vs) {
                self.endpoint.put(&m.new_node, docid, data, terms)?;
            }
        }
        if let Some(tc) = &self.term_cache {
            for t in terms {
                tc.record_write(t);
            }
        }
        Ok(nid)
    }

    /// docid → 虚拟分片（与 `sharding::route` 同哈希）。
    fn virtual_shard_of(&self, docid: u64) -> u32 {
        (hash64(docid) % self.meta.virtual_shards() as u64) as u32
    }

    /// 主键点查：单分片路由。返回文档 JSON 字符串。
    pub fn get(&mut self, docid: u64) -> Result<Option<String>> {
        let node = self.route_node(docid)?;
        self.endpoint.get(&node.node_id, docid)
    }

    /// 广播检索（design 9.2 + 9.9）：全部节点取本片 Chunk → 按序直拼。
    /// Term 缓存命中直出（不透传后端分片）；未命中拉取后回填。
    pub fn broadcast_search(&mut self, term: &str) -> Result<Vec<u32>> {
        let targets: Vec<NodeInfo> = self.meta.broadcast_targets().into_iter().cloned().collect();
        if targets.is_empty() {
            return Err(Error::Cluster("集群无可用分片节点".into()));
        }
        let term_owned = term.to_string();
        let mut chunks = Vec::with_capacity(targets.len());
        for node in &targets {
            let nid = node.node_id.clone();
            // ① 缓存命中直出（design 9.9）
            let cached = self
                .term_cache
                .as_ref()
                .and_then(|tc| tc.get(&nid, &term_owned));
            let chunk = match cached {
                Some(bm) => bm,
                None => {
                    // ② 未命中 → 拉取后端分片本片 Chunk → 回填
                    let docids = self.endpoint.search_docids(&nid, &term_owned)?;
                    let bm = RoaringBitmap::from_iter(docids);
                    if let Some(tc) = &self.term_cache {
                        tc.insert(&nid, &term_owned, bm.clone());
                    }
                    bm
                }
            };
            chunks.push(chunk);
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

    // ============ 无损扩容协议（design 9.1.1）============

    /// 阶段一：开始迁移（新节点加入，暂不接收读流量）。返回需迁移的虚拟分片数。
    /// 计算新旧节点集合下归属变化的虚拟分片，进入双写（Shadow Writes）。
    pub fn begin_migration(&mut self, new_node: &str, new_addr: &str, new_role: &str) -> Result<usize> {
        if self.migration.is_some() {
            return Err(Error::Cluster("已有迁移进行中，请先 commit 或 abort".into()));
        }
        let old_nodes = self.meta.node_ids();
        if old_nodes.contains(&new_node.to_string()) {
            return Err(Error::Cluster(format!("节点已存在: {new_node}")));
        }
        let mut new_nodes = old_nodes.clone();
        new_nodes.push(new_node.to_string());
        let moved = compute_moved_vshards(&old_nodes, &new_nodes, self.meta.virtual_shards());
        // 新节点接入端点（双写 / 追平），但**不注册进元数据中心**（路由暂不变）
        self.endpoint.add_node(new_node, Some(new_addr))?;
        let n = moved.len();
        self.migration = Some(Migration::new(
            new_node.to_string(),
            new_addr.to_string(),
            new_role.to_string(),
            moved,
        ));
        Ok(n)
    }

    /// 阶段二：数据追平（Delta Catch-up）。全量扫描老节点数据，把属于迁移分片的
    /// 文档拷贝到新节点（物理形态为 SST 拷贝 + WAL 增量，此处为逻辑全量拷贝，语义等价）。
    /// 返回拷贝条数。追平期间的新写入由双写兜底。
    pub fn catch_up(&mut self) -> Result<usize> {
        let Some(m) = &self.migration else {
            return Err(Error::Cluster("未处于迁移状态".into()));
        };
        let moved = m.moved_vshards.clone();
        let new_node = m.new_node.clone();
        let old_nodes: Vec<NodeInfo> = self.meta.broadcast_targets().into_iter().cloned().collect();
        let mut copied = 0usize;
        for old in &old_nodes {
            if old.node_id == new_node {
                continue;
            }
            let rows = self.endpoint.scan_all(&old.node_id)?;
            for (docid, data) in rows {
                let vs = self.virtual_shard_of(docid);
                if !moved.contains(&vs) {
                    continue;
                }
                // 从文档 JSON 重新派生倒排词条（与写入路径 extract_terms 一致）
                let terms = match serde_json::from_str::<serde_json::Value>(&data) {
                    Ok(v) => crate::server::extract_terms(&v),
                    Err(_) => Vec::new(),
                };
                self.endpoint.put(&new_node, docid, &data, &terms)?;
                copied += 1;
            }
        }
        Ok(copied)
    }

    /// 阶段三：原子切换（Atomic Switch）。将新节点注册进元数据中心（路由映射切换），
    /// 关闭双写。返回切换的虚拟分片数。
    pub fn commit_migration(&mut self) -> Result<usize> {
        let Some(m) = &self.migration else {
            return Err(Error::Cluster("未处于迁移状态".into()));
        };
        let new_node = m.new_node.clone();
        let new_addr = m.new_addr.clone();
        let new_role = m.new_role.clone();
        let moved = m.moved_vshards.len();
        self.meta.register(&new_node, &new_addr, &new_role)?;
        self.migration = None; // 双写关闭，路由已切至新节点
        Ok(moved)
    }

    /// 回滚预案：Node-B 启动失败 → 放弃迁移（新节点不注册，路由不变，旧数据完好）。
    pub fn abort_migration(&mut self) {
        self.migration = None;
    }

    /// 当前迁移状态（测试 / 监控）。
    pub fn migration(&self) -> Option<&Migration> {
        self.migration.as_ref()
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

    fn scan_all(&mut self, node: &str) -> Result<Vec<(u64, String)>> {
        let shard = self
            .shards
            .get(node)
            .ok_or_else(|| Error::Cluster(format!("未知节点: {node}")))?;
        let mut rows: Vec<(u64, String)> = shard
            .docs
            .iter()
            .map(|(d, v)| (*d, v.clone()))
            .collect();
        rows.sort_by_key(|(d, _)| *d);
        Ok(rows)
    }

    fn ping(&mut self, node: &str) -> Result<()> {
        if self.shards.contains_key(node) {
            Ok(())
        } else {
            Err(Error::Cluster(format!("节点不可达: {node}")))
        }
    }

    fn add_node(&mut self, node_id: &str, _addr: Option<&str>) -> Result<()> {
        self.shards
            .entry(node_id.to_string())
            .or_insert_with(MemShard::default);
        Ok(())
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

    fn scan_all(&mut self, node: &str) -> Result<Vec<(u64, String)>> {
        let r = self.client(node)?.call("shard.scan_all", json!({}))?;
        let docs = r["docs"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| {
                        Some((
                            v["docid"].as_u64()?,
                            v["data"].as_str()?.to_string(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(docs)
    }

    fn ping(&mut self, node: &str) -> Result<()> {
        self.client(node)?.call("shard.ping", json!({}))?;
        Ok(())
    }

    fn add_node(&mut self, node_id: &str, addr: Option<&str>) -> Result<()> {
        // 迁移新节点：显式地址接入（可能不在元数据中心）；已存在则忽略
        if self.clients.contains_key(node_id) {
            return Ok(());
        }
        let addr = addr
            .or_else(|| self.meta.node_addr(node_id))
            .ok_or_else(|| Error::Cluster(format!("节点无地址: {node_id}")))?;
        let c = RpcClient::connect(addr)?;
        self.clients.insert(node_id.to_string(), c);
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
        let meta = meta_with(&[("n1", "127.0.0.1:1", "master"), ("n2", "127.0.0.1:2", "slave")]);
        let endpoint = CountingEndpoint {
            inner: LocalShardEndpoint::with_nodes(&["n1", "n2"]),
            searches: 0,
        };
        let cache = crate::term_cache::TermCache::new(1000, std::time::Duration::from_secs(60), 100);
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
                .put(d, &format!("{{\"status\":\"active\",\"d\":{d}}}"), &terms(&["status=active"]))
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
                .put(d, &format!("{{\"status\":\"active\",\"d\":{d}}}"), &terms(&["status=active"]))
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
                assert!(gw.endpoint.get("n3", d).unwrap().is_some(), "追平缺失: docid={d}");
            }
        }

        // 阶段三：原子切换（Atomic Switch）
        let switched = gw.commit_migration().unwrap();
        assert_eq!(switched, moved);
        assert!(gw.migration().is_none(), "切换后双写关闭");
        assert!(gw.meta().node_ids().contains(&"n3".to_string()), "n3 已入元数据中心");

        // 切换后：迁移分片读路由到 n3 且数据完整（业务零感知）
        for d in 1..=220u64 {
            if moved_set.contains(&vs_of(d, 1024)) {
                let v = gw.get(d).unwrap().unwrap();
                assert!(v.contains(&format!("\"d\":{d}")), "切换后读取失败 docid={d}");
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
        gw.put(1, "{\"status\":\"active\"}", &terms(&["status=active"])).unwrap();

        let moved = gw.begin_migration("n2", "127.0.0.1:2", "slave").unwrap();
        assert!(moved > 0);
        // 回滚：n2 不注册，路由不变（旧数据完好）
        gw.abort_migration();
        assert!(gw.migration().is_none());
        assert!(!gw.meta().node_ids().contains(&"n2".to_string()));
        for d in 0..2000u64 {
            assert_eq!(gw.meta().resolve(d).unwrap().node_id, "n1", "回滚后路由必须不变");
        }
        // 数据完好
        assert!(gw.get(1).unwrap().is_some());
    }

    #[test]
    fn reshard_begin_twice_rejected() {
        let meta = meta_with(&[("n1", "127.0.0.1:1", "master")]);
        let mut gw = Gateway::new(meta, LocalShardEndpoint::with_nodes(&["n1"]));
        gw.begin_migration("n2", "127.0.0.1:2", "slave").unwrap();
        assert!(gw.begin_migration("n2", "127.0.0.1:2", "slave").is_err(), "重复迁移应拒绝");
    }
}
