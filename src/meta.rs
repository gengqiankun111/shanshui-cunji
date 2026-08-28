//! 元数据中心（MetaCenter，design 9.1 / 9.3 / 9.8，阶段 2）。
//!
//! 维护集群拓扑：节点清单（node_id / RPC 地址 / 主从角色）、
//! 分片映射（一致性哈希，虚拟分片 → 物理节点）、复制拓扑（master / slave）。
//! 网关只与 MetaCenter 交互获取路由决策，不持有数据。
//!
//! 能力：
//! - `register` / `unregister`：节点注册 / 摘除（注册即重建分片映射，天然支持平滑扩容/缩容）；
//! - `resolve(docid)`：写入口 / 主键点查的单分片路由；
//! - `broadcast_targets()`：广播检索的目标节点集合（顺序即分片 Chunk 拼接顺序）；
//! - `save` / `load`：拓扑 JSON 持久化（tmp + rename 原子写），元数据中心重启不丢集群信息。

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::sharding::ShardRouter;

/// 集群节点信息。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeInfo {
    /// 集群唯一标识。
    pub node_id: String,
    /// 内部 RPC 地址（host:port）。
    pub addr: String,
    /// 主从角色："master" / "slave"。
    pub role: String,
}

/// 元数据中心：节点拓扑 + 分片映射 + 复制角色。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaCenter {
    /// 全部节点（master 优先排序，广播顺序稳定）。
    nodes: Vec<NodeInfo>,
    /// 虚拟分片数（固定不可变）。
    virtual_shards: u32,
}

impl MetaCenter {
    /// 空集群。
    pub fn new(virtual_shards: u32) -> Self {
        Self {
            nodes: Vec::new(),
            virtual_shards,
        }
    }

    /// 注册（或更新）节点：重建分片映射。`role` ∈ "master" / "slave"。
    pub fn register(&mut self, node_id: &str, addr: &str, role: &str) -> Result<()> {
        if !matches!(role, "master" | "slave") {
            return Err(Error::Cluster(format!(
                "非法节点角色: {role}（master / slave）"
            )));
        }
        if let Some(n) = self.nodes.iter_mut().find(|n| n.node_id == node_id) {
            n.addr = addr.to_string();
            n.role = role.to_string();
            return Ok(());
        }
        self.nodes.push(NodeInfo {
            node_id: node_id.to_string(),
            addr: addr.to_string(),
            role: role.to_string(),
        });
        // master 优先（广播/写路由稳定性）
        self.nodes
            .sort_by_key(|n| if n.role == "master" { 0 } else { 1 });
        Ok(())
    }

    /// 摘除节点（缩容 / 故障隔离），重建分片映射。
    pub fn unregister(&mut self, node_id: &str) -> bool {
        let before = self.nodes.len();
        self.nodes.retain(|n| n.node_id != node_id);
        self.nodes.len() != before
    }

    /// 全部节点 ID。
    pub fn node_ids(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.node_id.clone()).collect()
    }

    /// 全部节点（顺序稳定）。
    pub fn nodes(&self) -> &[NodeInfo] {
        &self.nodes
    }

    /// 节点数。
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// 节点 RPC 地址。
    pub fn node_addr(&self, node_id: &str) -> Option<&str> {
        self.nodes
            .iter()
            .find(|n| n.node_id == node_id)
            .map(|n| n.addr.as_str())
    }

    /// 主节点（第一个 master；无 master 时回退第一个节点）。
    pub fn master_node(&self) -> Option<&NodeInfo> {
        self.nodes
            .iter()
            .find(|n| n.role == "master")
            .or_else(|| self.nodes.first())
    }

    /// 该节点是否为 slave。
    pub fn is_slave(&self, node_id: &str) -> bool {
        self.nodes
            .iter()
            .find(|n| n.node_id == node_id)
            .map(|n| n.role == "slave")
            .unwrap_or(false)
    }

    /// 虚拟分片总数。
    pub fn virtual_shards(&self) -> u32 {
        self.virtual_shards
    }

    /// 路由：docid → 归属节点（写 / 主键点查单分片定位）。
    /// 无节点时返回 None（集群未就绪）。
    pub fn resolve(&self, docid: u64) -> Option<&NodeInfo> {
        if self.nodes.is_empty() {
            return None;
        }
        let idx = self.router().route(docid);
        self.nodes.get(idx)
    }

    /// 广播目标：全部数据节点（顺序即分片 Chunk 拼接顺序，O(1) 直拼）。
    pub fn broadcast_targets(&self) -> Vec<&NodeInfo> {
        self.nodes.iter().collect()
    }

    /// 虚拟分片 → 节点下标（扩容 / 迁移簿记）。
    pub fn node_of_virtual_shard(&self, vshard: u32) -> Option<usize> {
        if self.nodes.is_empty() {
            return None;
        }
        Some(self.router().node_of_virtual_shard(vshard))
    }

    /// 当前分片路由器（节点集合变化时自动重建）。
    fn router(&self) -> ShardRouter {
        let ids: Vec<String> = self.nodes.iter().map(|n| n.node_id.clone()).collect();
        ShardRouter::new(self.virtual_shards, ids)
    }

    /// 持久化拓扑（tmp + rename 原子写）。
    pub fn save(&self, path: &std::path::Path) -> Result<()> {
        let text = serde_json::to_string_pretty(self)
            .map_err(|e| Error::Serialize(format!("拓扑序列化失败: {e}")))?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// 从文件加载拓扑。
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        serde_json::from_str(&text).map_err(|e| Error::Corrupted(format!("拓扑解析失败: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta3() -> MetaCenter {
        let mut m = MetaCenter::new(1024);
        m.register("node-1", "127.0.0.1:9091", "master").unwrap();
        m.register("node-2", "127.0.0.1:9092", "slave").unwrap();
        m.register("node-3", "127.0.0.1:9093", "slave").unwrap();
        m
    }

    #[test]
    fn register_and_resolve_routes_deterministically() {
        let m = meta3();
        assert_eq!(m.node_count(), 3);
        // 同一 docid 路由稳定，且落在节点集合内
        for docid in [0u64, 1, 42, 999_999, u64::MAX] {
            let a = m.resolve(docid).unwrap();
            let b = m.resolve(docid).unwrap();
            assert_eq!(a.node_id, b.node_id);
            assert!(m.node_ids().contains(&a.node_id));
        }
        // 主节点存在且 role=master
        assert_eq!(m.master_node().unwrap().role, "master");
        assert!(m.is_slave("node-2"));
        assert!(!m.is_slave("node-1"));
    }

    #[test]
    fn empty_cluster_resolve_returns_none() {
        let m = MetaCenter::new(1024);
        assert!(m.resolve(1).is_none());
        assert!(m.node_of_virtual_shard(0).is_none());
        assert!(m.master_node().is_none());
    }

    #[test]
    fn unregister_rebuilds_routing() {
        let mut m = meta3();
        assert!(m.unregister("node-3"));
        assert_eq!(m.node_count(), 2);
        assert!(!m.unregister("node-3"), "重复摘除返回 false");
        // 全部 docid 仍可路由到剩余节点
        for docid in 0..10_000u64 {
            let n = m.resolve(docid).unwrap();
            assert!(n.node_id == "node-1" || n.node_id == "node-2");
        }
    }

    #[test]
    fn adding_node_rebalances_only_fraction() {
        let mut m = MetaCenter::new(1024);
        for i in 1..=3 {
            m.register(&format!("node-{i}"), &format!("127.0.0.1:90{i}"), "slave")
                .unwrap();
        }
        let before: Vec<String> = (0..100_000u64)
            .map(|d| m.resolve(d).unwrap().node_id.clone())
            .collect();
        m.register("node-4", "127.0.0.1:9094", "slave").unwrap();
        let after: Vec<String> = (0..100_000u64)
            .map(|d| m.resolve(d).unwrap().node_id.clone())
            .collect();
        let changed = before
            .iter()
            .zip(after.iter())
            .filter(|(a, b)| a != b)
            .count();
        let ratio = changed as f64 / 100_000.0;
        assert!(ratio < 0.30, "扩容重路由比例 {ratio:.3} 应 ≈0.25");
    }

    #[test]
    fn invalid_role_rejected() {
        let mut m = MetaCenter::new(1024);
        assert!(m.register("node-1", "127.0.0.1:9091", "follower").is_err());
    }

    #[test]
    fn topology_persistence_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.json");
        {
            let m = meta3();
            m.save(&path).unwrap();
        }
        let m2 = MetaCenter::load(&path).unwrap();
        assert_eq!(m2.node_count(), 3);
        assert_eq!(m2.node_addr("node-2"), Some("127.0.0.1:9092"));
        assert_eq!(m2.master_node().unwrap().node_id, "node-1");
        // 路由一致
        for docid in [0u64, 7, 12345] {
            let a = MetaCenter::load(&path)
                .unwrap()
                .resolve(docid)
                .unwrap()
                .node_id
                .clone();
            assert_eq!(a, m2.resolve(docid).unwrap().node_id);
        }
    }

    #[test]
    fn broadcast_targets_include_all_nodes_in_order() {
        let m = meta3();
        let targets = m.broadcast_targets();
        assert_eq!(targets.len(), 3);
        // 顺序与 node_ids 一致（稳定拼接顺序）
        let ids: Vec<String> = targets.iter().map(|n| n.node_id.clone()).collect();
        assert_eq!(ids, m.node_ids());
    }
}
