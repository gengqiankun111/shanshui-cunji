//! 无损扩容协议（design 9.1.1，阶段 2）：双写 → 数据追平 → 原子切换。
//!
//! 三步协议保证扩容期间**业务零感知、数据零丢失**（No Data Loss Resharding）：
//! 1. **阶段一 · 双写（Shadow Writes）**：新节点（Node-B）加入集群，暂不接收读流量；
//!    网关对**属于迁移虚拟分片**的 DocId，同时写入老节点（Node-A）与新节点（Node-B）；
//! 2. **阶段二 · 数据追平（Delta Catch-up）**：全量扫描老节点数据拷贝到 Node-B
//!    （物理形态为 SST 拷贝 + WAL 增量回放，此处为逻辑全量拷贝，语义等价）；
//!    拷贝期间的新写入由双写兜底，追平完成后 Node-B 与老节点一致；
//! 3. **阶段三 · 原子切换（Atomic Switch）**：元数据中心注册 Node-B，路由映射切为 Node-B，
//!    关闭对该分片的双写；**回滚预案**：Node-B 启动失败则直接 `abort`（路由不变，旧数据完好）。
//!
//! 迁移虚拟分片集合 = 新旧节点集合下**一致性哈希归属变化的虚拟分片**（`compute_moved_vshards`，
//! 复用 `ShardRouter`；扩容只迁移约 1/N 虚拟分片，design 9.1）。

use std::collections::HashSet;

use crate::sharding::ShardRouter;

/// 计算扩容（新增节点）后归属变化的虚拟分片集合（需迁移的数据范围）。
pub fn compute_moved_vshards(
    old_nodes: &[String],
    new_nodes: &[String],
    virtual_shards: u32,
) -> HashSet<u32> {
    let old = ShardRouter::new(virtual_shards, old_nodes.to_vec());
    let new = ShardRouter::new(virtual_shards, new_nodes.to_vec());
    let mut moved = HashSet::new();
    for v in 0..virtual_shards {
        if old.node_of_virtual_shard(v) != new.node_of_virtual_shard(v) {
            moved.insert(v);
        }
    }
    moved
}

/// 扩容迁移状态（网关持有）。
pub struct Migration {
    /// 新节点 ID。
    pub new_node: String,
    /// 新节点 RPC 地址。
    pub new_addr: String,
    /// 新节点角色（通常 "slave"，切换后按需提升）。
    pub new_role: String,
    /// 需迁移的虚拟分片集合。
    pub moved_vshards: HashSet<u32>,
    /// 是否处于双写阶段（shadow = false 后进入切换收尾）。
    pub shadow: bool,
}

impl Migration {
    pub fn new(new_node: String, new_addr: String, new_role: String, moved_vshards: HashSet<u32>) -> Self {
        Self {
            new_node,
            new_addr,
            new_role,
            moved_vshards,
            shadow: true,
        }
    }

    /// 某 docid 是否属于迁移范围（需要双写）。
    pub fn is_migrating(&self, vshard: u32) -> bool {
        self.shadow && self.moved_vshards.contains(&vshard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn moved_vshards_is_fraction_on_add() {
        let old: Vec<String> = vec!["a".into(), "b".into(), "c".into()];
        let mut new = old.clone();
        new.push("d".into());
        let moved = compute_moved_vshards(&old, &new, 1024);
        // 3→4 节点：约迁移 1/4 = 256 个虚拟分片
        let n = moved.len();
        assert!(n > 128 && n < 384, "迁移量 {n}");
        // 迁移集合 = 新路由下归属 d 的虚拟分片数（一致）
        let router = ShardRouter::new(1024, new);
        let d_count = (0..1024u32).filter(|&v| router.node_of_virtual_shard(v) == 3).count();
        assert_eq!(moved.len(), d_count);
    }

    #[test]
    fn no_change_when_same_nodes() {
        let nodes: Vec<String> = vec!["a".into(), "b".into()];
        let moved = compute_moved_vshards(&nodes, &nodes, 1024);
        assert!(moved.is_empty());
    }

    #[test]
    fn migration_is_migrating_gates_by_shard() {
        let m = Migration::new(
            "n3".into(),
            "127.0.0.1:3".into(),
            "slave".into(),
            HashSet::from([1u32, 2, 3]),
        );
        assert!(m.is_migrating(1));
        assert!(!m.is_migrating(4));
        // shadow 关闭后不再双写
        let mut m2 = Migration::new("n3".into(), "a".into(), "slave".into(), HashSet::from([1]));
        m2.shadow = false;
        assert!(!m2.is_migrating(1));
    }
}
