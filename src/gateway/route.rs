//! 写入口 / 主键点查：携带 DocId 的单分片路由转发（design 9.1 / 9.2 第 1 类转发）。
//!
//! 复制（主→从异步/sync 复制）由分片节点层负责，网关只保证路由一致；扩容迁移期间
//! （design 9.1.1 双写）属于迁移虚拟分片的 DocId 同时写入新节点。

use super::{Gateway, ShardEndpoint};
use crate::error::Result;

impl<E: ShardEndpoint> Gateway<E> {
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

    /// 主键点查：单分片路由。返回文档 JSON 字符串。
    pub fn get(&mut self, docid: u64) -> Result<Option<String>> {
        let node = self.route_node(docid)?;
        self.endpoint.get(&node.node_id, docid)
    }
}
