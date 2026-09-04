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
//!
//! # 主题拆分（自原 `src/gateway.rs` 按主题拆分，行为零变化）
//! - `mod.rs`：模块文档、端点抽象 `ShardEndpoint`、`Gateway` 主结构与构造/访问器、
//!   私有路由辅助（`route_node` / `virtual_shard_of`），以及对外路径 `pub use` 汇总；
//! - `route.rs`：写入口 / 主键点查（单分片路由转发）；
//! - `batch.rs`：批量写入分组路由；
//! - `broadcast.rs`：广播检索 + 健康探活（全节点下发）；
//! - `migration.rs`：无损扩容协议（双写 → 追平 → 原子切换 / 回滚）；
//! - `local.rs` / `rpc.rs`：两个分片端点实现（进程内 / JSON-over-TCP）；
//! - `tests.rs`：`#[cfg(test)]` 单元测试。

mod batch;
mod broadcast;
mod local;
mod migration;
mod route;
mod rpc;

// 对外路径保持不变（原 gateway.rs 的 pub 项原样从根模块 re-export）：
// crate::gateway::ShardEndpoint / Gateway 定义于本文件；
// LocalShardEndpoint / RpcShardEndpoint 定义于子模块后 re-export。
pub use local::LocalShardEndpoint;
pub use rpc::RpcShardEndpoint;

#[cfg(test)]
mod tests;

use crate::error::{Error, Result};
use crate::meta::{MetaCenter, NodeInfo};
use crate::reshard::Migration;
use crate::sharding::hash64;
use crate::term_cache::TermCache;

/// 分片端点：网关访问数据节点的抽象（Local / RPC 两种实现）。
pub trait ShardEndpoint {
    /// 写入一条文档（`data` 为文档 JSON 字符串，`terms` 为倒排词条）。
    fn put(&mut self, node: &str, docid: u64, data: &str, terms: &[String]) -> Result<()>;
    /// 批量写入（C 项②，分布式吞吐优化）：一次调用提交 N 条，RTT 分摊到批。
    /// 默认逐条（LocalShardEndpoint 语义不变）；RpcShardEndpoint 覆盖为一次 RPC。
    fn put_batch(&mut self, node: &str, items: &[(u64, String, Vec<String>)]) -> Result<()> {
        for (docid, data, terms) in items {
            self.put(node, *docid, data, terms)?;
        }
        Ok(())
    }
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
    /// 私有路由辅助：route / batch / migration 等子模块的 impl 均经 `self` 调用（同模块子树可见）。
    fn route_node(&self, docid: u64) -> Result<NodeInfo> {
        self.meta
            .resolve(docid)
            .cloned()
            .ok_or_else(|| Error::Cluster("集群无可用分片节点".into()))
    }

    /// docid → 虚拟分片（与 `sharding::route` 同哈希）。
    fn virtual_shard_of(&self, docid: u64) -> u32 {
        (hash64(docid) % self.meta.virtual_shards() as u64) as u32
    }
}
