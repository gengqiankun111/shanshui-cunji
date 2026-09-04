//! 批量写入（C 项②，分布式吞吐优化）：按 docid 一致性哈希分组路由 → 每节点一次
//! `endpoint.put_batch`（RPC 场景一次连接一次调用，RTT 分摊到批而非单条）。

use super::{Gateway, ShardEndpoint};
use crate::error::Result;

impl<E: ShardEndpoint> Gateway<E> {
    /// 批量写入（C 项②，分布式吞吐优化）：按 docid 一致性哈希**分组路由** → 每节点一次
    /// `endpoint.put_batch`（RPC 场景一次连接一次调用，RTT 分摊到批而非单条）。
    /// 返回 节点 → 写入条数（合计 = 输入条数，供强一致校验）。
    pub fn put_batch(
        &mut self,
        items: &[(u64, String, Vec<String>)],
    ) -> Result<std::collections::HashMap<String, usize>> {
        let mut groups: std::collections::HashMap<String, Vec<(u64, String, Vec<String>)>> =
            std::collections::HashMap::new();
        for (docid, data, terms) in items {
            let node = self.route_node(*docid)?;
            groups
                .entry(node.node_id.clone())
                .or_default()
                .push((*docid, data.clone(), terms.clone()));
        }
        let mut out = std::collections::HashMap::new();
        for (node, batch) in groups {
            self.endpoint.put_batch(&node, &batch)?;
            out.insert(node, batch.len());
        }
        Ok(out)
    }
}
