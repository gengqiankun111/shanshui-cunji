//! 运维管理（design 20 / development 5.24-5.25）。
//!
//! - **QueryRegistry**：查询 / 后台任务注册、注销、列表、KILL 标记；
//!   （真正中断执行依赖看门狗超时熔断 + 阶段 2 CancellationToken，本阶段 KILL 为状态标记）
//! - **Status 聚合**：分配器 / LSM / 倒排 / 内存水位指标，供 CLI `admin status` 与 HTTP `/admin/status`。

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use dashmap::DashMap;
use serde::Serialize;

use crate::engine::Engine;

/// 查询上下文（执行期间注册，结束注销）。
#[derive(Debug, Clone, Serialize)]
pub struct QueryContext {
    pub id: u64,
    /// 命令摘要（如 `search status=active`）。
    pub command: String,
    /// 过滤条件（脱敏）。
    pub filter: String,
    /// 状态：running / done / killed。
    pub state: String,
    /// 已运行毫秒（相对注册时刻）。
    pub elapsed_ms: u64,
    /// 已扫描行数（基础版未接入，恒 0；阶段 2 由执行器回写）。
    pub rows_scanned: u64,
}

/// 查询注册表：`DashMap<QueryID, QueryContext>`（development 5.24）。
pub struct QueryRegistry {
    map: DashMap<u64, QueryContext>,
    next_id: AtomicU64,
    _boot: Instant,
}

impl Default for QueryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryRegistry {
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
            next_id: AtomicU64::new(1),
            _boot: Instant::now(),
        }
    }

    /// 注册并返回查询 id。
    pub fn register(&self, command: String, filter: String) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.map.insert(
            id,
            QueryContext {
                id,
                command,
                filter,
                state: "running".into(),
                elapsed_ms: 0,
                rows_scanned: 0,
            },
        );
        id
    }

    /// 注销查询。
    pub fn unregister(&self, id: u64) {
        self.map.remove(&id);
    }

    /// 全部进行中查询（附 elapsed；基础版 elapsed=0，阶段 2 由执行器回写时间戳）。
    pub fn list(&self) -> Vec<QueryContext> {
        self.map
            .iter()
            .map(|e| {
                let mut ctx = e.value().clone();
                ctx.elapsed_ms = 0;
                ctx
            })
            .collect()
    }

    /// KILL 标记：将查询状态置 killed（真正中断由阶段 2 CancellationToken 实现）。
    pub fn kill(&self, id: u64) -> bool {
        if let Some(mut e) = self.map.get_mut(&id) {
            e.state = "killed".into();
            true
        } else {
            false
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// 引擎状态指标（design 20 / development 5.25）。
#[derive(Debug, Clone, Serialize)]
pub struct StatusReport {
    /// 分配器：mimalloc（默认）/ jemalloc（feature alloc-jemalloc）。
    pub allocator: String,
    /// LSM：SST 文件总数（单层 L0 语义）。
    pub sst_file_count: usize,
    /// 倒排：内存累积 posting 数。
    pub inverted_mem_docids: u64,
    /// 倒排：磁盘段数。
    pub inverted_segments: usize,
    /// 当前序列号。
    pub seq: u64,
    /// 内存水位（0~1，watchdog 维护）。
    pub mem_ratio: f64,
    /// 配置的内存硬上限（MB）。
    pub max_memory_mb: usize,
}

/// 聚合引擎状态（CLI `admin status` / HTTP `/admin/status` 共用）。
pub fn status(engine: &Engine) -> StatusReport {
    let s = engine.stats();
    StatusReport {
        allocator: if cfg!(feature = "alloc-jemalloc") {
            "jemalloc".into()
        } else if cfg!(feature = "alloc-mimalloc") {
            "mimalloc".into()
        } else {
            "system".into()
        },
        sst_file_count: s.sst_file_count,
        inverted_mem_docids: s.inverted_mem_docids,
        inverted_segments: s.inverted_segments,
        seq: s.seq,
        mem_ratio: s.mem_ratio,
        max_memory_mb: s.max_memory_mb,
    }
}

/// 集群配置状态（design 9.8，CLI `admin status` 输出分布式部分）。
#[derive(Debug, Clone, Serialize)]
pub struct ClusterStatus {
    /// 运行模式：standalone / cluster。
    pub mode: String,
    /// 集群节点 ID。
    pub node_id: String,
    /// 内部 RPC 端口。
    pub internal_rpc_port: u16,
    /// 分片是否启用。
    pub sharding_enabled: bool,
    /// 物理分片数（0 = 自动）。
    pub total_shards: u32,
    /// 虚拟分片数。
    pub virtual_shards: u32,
    /// 一致性哈希 / 取模。
    pub consistent_hash: bool,
    /// 副本是否启用。
    pub replication_enabled: bool,
    /// 节点角色：master / slave。
    pub replication_role: String,
    /// 同步模式：async / sync。
    pub sync_mode: String,
    /// Master RPC 地址（slave 填写）。
    pub master_addr: String,
    /// 广播查询并发上限。
    pub broadcast_max_concurrent: usize,
}

/// 聚合集群配置状态（CLI `admin status` 使用；HTTP /admin/status 仅引擎指标，集群状态由网关汇总）。
pub fn cluster_status(cfg: &crate::config::Config) -> ClusterStatus {
    ClusterStatus {
        mode: cfg.server.mode.clone(),
        node_id: cfg.cluster.node_id.clone(),
        internal_rpc_port: cfg.cluster.internal_rpc_port,
        sharding_enabled: cfg.sharding.enabled,
        total_shards: cfg.sharding.total_shards,
        virtual_shards: cfg.sharding.virtual_shards,
        consistent_hash: cfg.sharding.consistent_hash,
        replication_enabled: cfg.replication.enabled,
        replication_role: cfg.replication.role.clone(),
        sync_mode: cfg.replication.sync_mode.clone(),
        master_addr: cfg.replication.master_addr.clone(),
        broadcast_max_concurrent: cfg.broadcast_query.max_concurrent,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_register_list_kill_lifecycle() {
        let reg = QueryRegistry::new();
        let id1 = reg.register("search status=active".into(), "status=active".into());
        let id2 = reg.register("range".into(), "".into());
        assert_eq!(reg.len(), 2);
        // 列表
        let list = reg.list();
        assert_eq!(list.len(), 2);
        assert!(list.iter().any(|c| c.id == id1 && c.state == "running"));
        // KILL 标记
        assert!(reg.kill(id1));
        assert!(!reg.kill(999), "不存在应返回 false");
        let list = reg.list();
        assert!(list.iter().any(|c| c.id == id1 && c.state == "killed"));
        // 注销
        reg.unregister(id1);
        reg.unregister(id2);
        assert_eq!(reg.len(), 0);
    }
}
