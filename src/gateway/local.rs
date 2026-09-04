//! 进程内测试端点（LocalShardEndpoint）：多分片数据都放在本进程，
//! 用于网关路由/广播/迁移逻辑的隔离测试。

use std::collections::HashMap;

use super::ShardEndpoint;
use crate::error::{Error, Result};

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
        let mut rows: Vec<(u64, String)> =
            shard.docs.iter().map(|(d, v)| (*d, v.clone())).collect();
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
