//! 广播检索 + 健康探活：向全部节点下发（design 9.2 第 2/3 类转发 + 9.9 Term 缓存）。

use roaring::RoaringBitmap;
use roaring::treemap::RoaringTreemap;

use super::{Gateway, ShardEndpoint};
use crate::error::{Error, Result};
use crate::inverted::InvertedIndex;
use crate::meta::NodeInfo;

impl<E: ShardEndpoint> Gateway<E> {
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
        // 聚合各分片 Chunk（本地缓存为 32 位位图 → 升 64 位后直拼 → 转回 32 位输出，
        // 与分布式单机 docid<2^32 的既有协议一致）
        let mut pchunks: Vec<RoaringTreemap> = Vec::with_capacity(chunks.len());
        for c in &chunks {
            let mut t = RoaringTreemap::new();
            for v in c.iter() {
                t.insert(v as u64);
            }
            pchunks.push(t);
        }
        let merged = InvertedIndex::concatenate_chunks(&pchunks);
        Ok(merged.iter().map(|v| v as u32).collect())
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
