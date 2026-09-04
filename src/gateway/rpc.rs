//! 跨进程端点（RpcShardEndpoint）：每节点一条连接，走 `src/rpc.rs` JSON-over-TCP。

use std::collections::HashMap;

use serde_json::json;
use serde_json::Value;

use super::ShardEndpoint;
use crate::error::{Error, Result};
use crate::meta::MetaCenter;
use crate::rpc::RpcClient;

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

    // C 项②：批量写入一次 RPC（RTT 分摊到批；节点 Engine::put_batch 原子批量 + 组提交）
    fn put_batch(&mut self, node: &str, items: &[(u64, String, Vec<String>)]) -> Result<()> {
        let json_items: Vec<Value> = items
            .iter()
            .map(|(docid, data, terms)| {
                json!({"docid": docid, "data": data, "terms": terms})
            })
            .collect();
        self.client(node)?
            .call("shard.put_batch", json!({"items": json_items}))?;
        Ok(())
    }

    fn get(&mut self, node: &str, docid: u64) -> Result<Option<String>> {
        let r = self
            .client(node)?
            .call("shard.get", json!({"docid": docid}))?;
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
                    .filter_map(|v| Some((v["docid"].as_u64()?, v["data"].as_str()?.to_string())))
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
