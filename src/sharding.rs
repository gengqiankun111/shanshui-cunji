//! 一致性哈希分片路由器（design 9.1 / development 7.3 阶段 2 第 3 项）。
//!
//! 两级路由：
//! 1. `docid → 虚拟分片`：`hash64(docid) % virtual_shards`（虚拟分片数固定不可变，扩容不迁移虚拟分片）；
//! 2. `虚拟分片 → 物理节点`：一致性哈希环（每节点 128 个虚拟点），扩容/减容只移动约 1/N 虚拟分片。
//!
//! 写入口 / 主键查询：单分片操作（`route(docid)` 无广播）；不带分片键的倒排检索：广播到全部节点（`nodes()`）。

/// 每个物理节点在一致性哈希环上的虚拟点数（均匀性足够，128 点 × N 节点）。
const VNODES_PER_NODE: u64 = 128;

/// splitmix64 —— 确定性 64 位混合哈希（无外部依赖、无 unsafe）。
/// `pub(crate)`：倒排预分片 Chunk（design 5.2.1）复用同一哈希，保证分片一致性。
pub(crate) fn hash64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// 字符串确定性哈希（FNV-1a 64 变体）→ 作为节点 ID 的环位置种子。
fn hash_str(s: &str) -> u64 {
    let mut h: u64 = 0xCBF2_9CE4_8422_2325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x1000_0000_01B3);
    }
    h
}

/// 一致性哈希分片路由器（不可变，构建后线程安全只读）。
#[derive(Debug, Clone)]
pub struct ShardRouter {
    /// 虚拟分片数（如 1024/2048，固定不可变）。
    virtual_shards: u32,
    /// 物理节点 ID 列表（下标即节点索引）。
    nodes: Vec<String>,
    /// 一致性哈希环：(position, node_index)，按 position 升序。
    ring: Vec<(u64, usize)>,
}

impl ShardRouter {
    /// 构建路由环。`nodes` 非空；每个节点放置 `VNODES_PER_NODE` 个环点。
    pub fn new(virtual_shards: u32, nodes: Vec<String>) -> Self {
        assert!(!nodes.is_empty(), "ShardRouter 至少需要一个物理节点");
        let mut ring = Vec::with_capacity(nodes.len() * VNODES_PER_NODE as usize);
        for (i, n) in nodes.iter().enumerate() {
            let seed = hash_str(n);
            for v in 0..VNODES_PER_NODE {
                let pos = hash64(seed.wrapping_add(v.wrapping_mul(0x9E37_79B9_7F4A_7C15)));
                ring.push((pos, i));
            }
        }
        ring.sort_unstable_by_key(|&(p, _)| p);
        Self {
            virtual_shards,
            nodes,
            ring,
        }
    }

    /// 物理节点数。
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// 全部物理节点 ID（广播检索目标集合）。
    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    /// 节点下标 → 节点 ID。
    pub fn node_id(&self, idx: usize) -> &str {
        &self.nodes[idx]
    }

    /// 虚拟分片总数。
    pub fn virtual_shard_count(&self) -> u32 {
        self.virtual_shards
    }

    /// 第一级路由：docid → 虚拟分片 id（只依赖 docid 与虚拟分片数，扩容节点不改变）。
    pub fn virtual_shard_of(&self, docid: u64) -> u32 {
        (hash64(docid) % self.virtual_shards as u64) as u32
    }

    /// 第二级路由：虚拟分片 → 物理节点下标（一致性哈希环顺时针首个节点）。
    pub fn node_of_virtual_shard(&self, vshard: u32) -> usize {
        let pos = hash64(vshard as u64);
        match self.ring.binary_search_by_key(&pos, |&(p, _)| p) {
            Ok(i) => self.ring[i].1,
            Err(i) => self.ring[i % self.ring.len()].1,
        }
    }

    /// 完整路由：docid → 物理节点下标（写入 / 主键点查单分片定位）。
    pub fn route(&self, docid: u64) -> usize {
        self.node_of_virtual_shard(self.virtual_shard_of(docid))
    }

    /// 路由到节点 ID（便捷封装）。
    pub fn route_node(&self, docid: u64) -> &str {
        &self.nodes[self.route(docid)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes4() -> Vec<String> {
        ["node-1", "node-2", "node-3", "node-4"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn route_is_deterministic_and_in_range() {
        let r = ShardRouter::new(1024, nodes4());
        for docid in [0u64, 1, 42, 999, 1_000_000, u64::MAX] {
            let a = r.route(docid);
            let b = r.route(docid);
            assert_eq!(a, b, "同 docid 必须路由到同一节点");
            assert!(a < 4);
        }
        // 所有虚拟分片都有归属且不越界
        for v in 0..1024 {
            let n = r.node_of_virtual_shard(v);
            assert!(n < 4);
        }
    }

    #[test]
    fn virtual_shard_mapping_is_evenly_distributed() {
        let r = ShardRouter::new(1024, nodes4());
        let mut counts = [0usize; 4];
        for v in 0..1024 {
            counts[r.node_of_virtual_shard(v)] += 1;
        }
        // 4 节点均匀：每节点应接近 256 个虚拟分片（宽松 200~312）
        for (i, c) in counts.iter().enumerate() {
            assert!(
                (200..=312).contains(c),
                "节点 {i} 虚拟分片数 {c} 偏离均匀分布"
            );
        }
    }

    #[test]
    fn adding_node_moves_only_fraction_of_virtual_shards() {
        // 平滑扩容（design 9.1）：3 → 4 节点，只迁移约 1/4 虚拟分片。
        let before = ShardRouter::new(1024, vec!["a".into(), "b".into(), "c".into()]);
        let after = ShardRouter::new(1024, vec!["a".into(), "b".into(), "c".into(), "d".into()]);
        let mut moved = 0usize;
        for v in 0..1024 {
            if before.node_of_virtual_shard(v) != after.node_of_virtual_shard(v) {
                moved += 1;
            }
        }
        // 期望 ~256（1024/4）；允许抖动，但必须远小于全部迁移（上限 384 = 期望的 1.5 倍）
        assert!(
            moved < 384,
            "扩容迁移虚拟分片过多: {moved}（应 ~256）"
        );
        assert!(moved > 128, "扩容迁移过少: {moved}（应 ~256）");
    }

    #[test]
    fn docid_routes_stay_stable_when_node_added() {
        // 扩容只影响属于迁移分片的 docid：绝大多数 docid 路由不变
        let before = ShardRouter::new(1024, vec!["a".into(), "b".into(), "c".into()]);
        let after = ShardRouter::new(1024, vec!["a".into(), "b".into(), "c".into(), "d".into()]);
        let mut changed = 0usize;
        let n = 100_000u64;
        for docid in 0..n {
            if before.route(docid) != after.route(docid) {
                changed += 1;
            }
        }
        let ratio = changed as f64 / n as f64;
        assert!(
            ratio < 0.30,
            "扩容后 docid 重路由比例过高: {ratio:.3}（应 ≈0.25）"
        );
    }

    #[test]
    fn single_node_ring_is_trivial() {
        let r = ShardRouter::new(1024, vec!["only".into()]);
        for docid in 0..1000 {
            assert_eq!(r.route(docid), 0);
            assert_eq!(r.route_node(docid), "only");
        }
    }
}
