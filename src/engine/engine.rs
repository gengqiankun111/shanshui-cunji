//! 文档引擎核心（reconstruct.md engine/engine.rs）：`Engine` 结构体定义、共享类型
//! （QueryRow / PagedRows / EngineStats）与核心协调/可观测 API（数据目录、内存比率、
//! 分片指标、统计读数等）。其余主题见同目录 read.rs（读族）/ scan.rs（扫描族）/
//! write.rs（写族）/ query.rs（倒排/组合查询协调）/ compact.rs（压缩协调）/
//! open.rs（打开/备份恢复）；事务在 txn.rs、MVCC 快照在 mvcc.rs。
//!
//! 拆分说明：本文件与 read/scan/write/query/compact/open 各持独立的 `impl Engine` 块；
//! 跨文件访问的私有 Engine 字段以 `pub(crate)` 提升（语义零变化）。

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use roaring::treemap::RoaringTreemap;

use crate::bitmap::DeletionBitmap;
use crate::column_family::ColumnFamily;
use crate::hotcache::HotCache;
use crate::inverted::InvertedIndex;
use crate::outbox::Outbox;
use crate::watchdog::Watchdog;


/// 引擎：组合主数据 + 组合索引 + Delta 增量 + 倒排 + HotCache。
pub struct Engine {
    /// 主数据列族（value = 序列化文档字节）。
    /// P72（无锁合并）：`Arc<ColumnFamily>`——mysql 后台 worker clone 三 CF Arc 后无锁合并，
    /// flush/compact 的 ssts 变更经 CF 内部 `sst_mutate` 互斥（不再依赖 Engine RwLock 串行）。
    pub(crate) primary: Arc<ColumnFamily>,
    /// 组合索引列族（key = encode_composite_key）。
    pub(crate) cidx: Option<Arc<ColumnFamily>>,
    /// Delta 增量列族（阶段 1.5，key = encode_docid ++ VarLen(field)，Merge-on-Read 覆盖 Base）。
    pub(crate) delta: Arc<ColumnFamily>,
    /// 倒排索引。J 项（7.73）：`Arc`（后台 GC worker 无锁 clone 后执行 gc）。
    pub inverted: Arc<InvertedIndex>,
    /// 文档热缓存（7.72：内部 RwLock+DashMap 粒度化——读路径 `&self` 读读并行、
    /// 写路径（put/invalidate/promote）写锁，不再整包 Mutex 串行热缓存访问）。
    pub(crate) hotcache: HotCache,
    /// 看门狗（OOM 限流 + 查询超时熔断）。
    pub(crate) watchdog: Watchdog,
    /// 内存使用率估算（0~1，由上层注入或监控更新）。
    pub(crate) mem_ratio: f64,
    /// 内存硬上限（MB，`memory.max_memory_mb`，供 admin status）。
    pub(crate) max_memory_mb: usize,
    /// 全局 seq 分配器（MVCC，M7-1）：primary / delta 列族共享，跨列族写入序一致。
    pub(crate) global_seq: Arc<AtomicU64>,
    /// 组提交（M8）：Some((窗口, 字节阈值)) = 开启；None = 关闭（逐条 fsync 强安全）。
    /// 窗口内写入攒批一次 fsync（design 4.3 / M8，`storage.group_commit_us`）。
    pub(crate) group_commit: Option<(Duration, usize)>,
    /// P2-A：事务 COMMIT 落盘档位（`storage.flush_log_at_trx_commit`）——1 = 每次 COMMIT
    /// `flush_wal`（强安全）；0/2 = COMMIT 走组提交窗口（`maybe_group_commit`：延迟耐久，
    /// 组提交关闭时自动回退强安全）。与 `group_commit`（非事务写窗口）正交。
    pub(crate) flush_log_at_trx_commit: u8,
    /// 组提交后台线程停止标志（窗口尾部落盘兜底）。
    pub(crate) gc_stop: Option<Arc<AtomicBool>>,
    /// 组提交后台线程句柄。
    pub(crate) gc_thread: Option<std::thread::JoinHandle<()>>,
    /// 倒排字段白名单（M8-P4）：Some = 只建声明字段倒排；None = 全部（黑名单仍生效）。
    pub(crate) inverted_include: Option<std::collections::HashSet<String>>,
    /// 倒排字段黑名单（M8-P4）：这些字段不建倒排（白名单非空时忽略）。
    pub(crate) inverted_exclude: std::collections::HashSet<String>,
    /// 倒排 term 长度上限（M8-P4）：超过自动跳过（长文本整串不进字典）；0 = 不限。
    pub(crate) max_term_len: usize,
    /// fulltext 分词字段（M8-P7）：声明字段做分词建词 term 索引（`ft:{field}:{token}`）。
    pub(crate) fulltext_fields: std::collections::HashSet<String>,
    /// 中文分词器（M8-P13）：true = jieba 完整词典分词（需 cjk-jieba feature）；
    /// false = bigram（M8-P9）。来自 `[inverted] cjk_segmenter`。
    pub(crate) use_jieba: bool,
    /// Ex-9.3：倒排统计载荷声明字段（`cfg.inverted.stats_fields`，空 = 关闭）。
    pub(crate) stats_fields: Vec<String>,
    /// 写入 Enrich（design 19 / development 5.21）：Some((fail_policy, from_field, to_field)) =
    /// 启用 local 数据源预连接（server /put 走 join::put_with_enrich）；None = 关闭（零开销）。
    pub(crate) enrich: Option<(String, String, String)>,
    /// 批量导入模式（P40）：跳过 HotCache 回填/失效。批量导入只写不读，回填缓存纯浪费内存
    /// （4GB 预算灌满 + stats 泄漏 → 触发页面颠簸 → 行速指数级崩塌，50M 导入 4M 行后卡死）。
    pub(crate) skip_hotcache: bool,
    /// P0-A：声明式组合索引字段组（从 config.storage.composite_indexes 加载）。
    pub composite_indexes: Vec<Vec<String>>,
    /// 倒排更新攒批缓冲（Ex-5.3）：put 时 term 先入缓冲，达阈值/查询/flush 时
    /// 一次性 `add_batch` 批量刷入内存字典——低基数 term 跨行聚合，
    /// 减少 DashMap 锁操作次数（N×字段数 → ~唯一 term 数）。
    /// 崩溃安全：WAL 回放重新走 put 重建倒排，缓冲丢失不丢数据。
    /// O 项第②步：内部 `Mutex`——倒排读路径 `&self` 下也能先刷缓冲再查（一致性）。
    pub(crate) pending_inverted: Mutex<Vec<(String, u64)>>,
    /// Compaction 并行度（Ex-5.4）：并行压实 primary/cidx/delta 三列族；
    /// 0 = 自动（min(4, 核数/2)），1 = 串行，>1 = 指定并行数。
    pub(crate) compaction_parallel: usize,
    /// P4-C：基于代价的优化器是否启用。
    pub cost_based_enabled: bool,
    /// P4-C：代价模型参数。
    pub cost_params: crate::optimizer::CostParams,
    /// P 项：事件驱动自动 Compaction（`storage.auto_compact`）——写入路径自触发：
    /// 写前 L0 达硬顶（l0_stall_max）先合并（背压），写后 L0 超阈值（段数/大小）合并收敛。
    pub(crate) auto_compact: bool,
    /// O 项第③步：后台合并信号——写路径检测 L0 超阈值时置位（AcqRel）；mysql 服务
    /// 的后台 worker 读取后读锁下合并。无 worker 场景（demo/rpc/测试）由同步路径直接消费。
    pub compact_pending: Arc<AtomicBool>,
    /// O 项第③步：后台合并 worker 挂载标记（服务进程 spawn 时置 true）——
    /// true 时写路径只发信号（合并不阻塞读写）；false 保持同步合并（写入退避=背压）。
    pub compact_worker: Arc<AtomicBool>,
    /// J 项（7.73）：倒排段 GC 后台信号——写路径刷盘后检测段超 GC 阈值时置位；mysql 服务
    /// 的后台 GC worker 读取后检查 `should_gc()` 并执行 `inverted.gc()`（无 worker 场景
    /// 由 demo/显式 inverted_gc 消费）。
    pub inverted_gc_pending: Arc<AtomicBool>,
    /// 删除位图（Ex-5.6）：Some = 开启（delete 写 1bit 跳 Tombstone、get O(1) 跳过、
    /// compaction 物理删除）；None = 关闭（传统 Tombstone 路径）。
    /// P72：`Arc`——worker 无锁合并 clone 后并发读位图过滤（写路径在 Engine 写锁内）。
    pub(crate) deletion_bitmap: Option<Arc<DeletionBitmap>>,
    /// Ex-8.7 删除密度（删除位图置位率驱动 Compaction）调度状态——`Arc` 供无锁合并
    /// （`CompactTargets::run`）在 Engine 读锁外按压实结果回写（drop>0 继续排空 / 0 收敛）。
    /// - `garbage_marked`：位图当前置位 docid **净数**（幂等重删不重计、复活即减，精确）；
    ///   打开时 = 位图既有置位数（历史置位）。
    /// - `garbage_done`：最近一次"排空收敛"时的 `garbage_marked` 快照——此后需新增置位
    ///   ≥ `delete_density_min_docs` 才再次进入删除密度触发（历史置位不重复触发重写）。
    /// - `garbage_draining`：排空进行中（最近一轮主列族压实实际物理丢弃 >0 → 继续 GC，
    ///   直至某轮 0 丢弃 → 收敛并刷新 `garbage_done`）。
    pub(crate) garbage_marked: Arc<AtomicU64>,
    pub(crate) garbage_done: Arc<AtomicU64>,
    pub(crate) garbage_draining: Arc<AtomicBool>,
    /// 曾写入的最大 docid（≈ 曾插入文档数，删除置位率分母；put 时 fetch_max）。
    pub(crate) max_docid: AtomicU64,
    /// §27 P0：重启后 max_docid 归零 → `auto_watermark` 首次调用做一次全库 keys-only
    /// 恢复（AtomicBool swap 保证只扫一次；运行期 put 的 fetch_max 持续维护）。
    pub(crate) max_docid_loaded: AtomicBool,
    /// 删除密度触发阈值（`storage.delete_density_min_ratio` / `_min_docs`）。
    pub(crate) dd_min_ratio: f32,
    pub(crate) dd_min_docs: u64,
    /// R4（review 2026-09-04）：活跃快照 seq 集合（RR/Serializable 事务注册，commit/rollback
    /// 注销）——compact 前取其最小值作 MVCC 保活水位（见 ColumnFamily::mvcc_keep_floor）：
    /// 最新 seq > floor 的 key 保留多版本，使删除/覆盖前旧快照在 compaction 后仍可回读。
    pub(crate) active_snapshots: RwLock<std::collections::BTreeSet<u64>>,
    /// P1-C：活跃 docid 集（`COUNT(*)` O(1) 快路径基线）。None = 未初始化（首次
    /// `count_all_docs` 全键扫一次建基线后置 Some）；写路径增量维护（新 docid put 增 /
    /// delete 减 / 覆盖不变 / 复活增），purge 复位空集。语义 = 引擎最新视图（删除位图
    /// 与 Tombstone 双路径一致）；跨线程由 db 层读写锁串行化，此处 Mutex 仅保护懒建/读。
    pub(crate) live_docids: std::sync::Mutex<Option<RoaringTreemap>>,
    /// 三池核分区（Ex-7.2）：network（server 主线程）/ compute（Compaction 并行）/
    /// io（组提交后台）——绑核消除调度抖动；enabled=false 时为空（no-op）。
    pub(crate) affinity: crate::affinity::CpuPartition,
    /// 后台 IO 限速基准（字节/秒，Ex-7.4）：`storage.io_rate_limit_mb` 换算；0 = 不限速。
    pub(crate) io_rate_base_bytes: u64,
    /// MemTable 容量上限（字节，Ex-7.4 写压力代理基准，`memtable.max_size_mb`）。
    pub(crate) memtable_max_bytes: usize,
    /// 本地消息表（Ex-1）：Some = 启用（业务写同一本地事务入队 outbox，后台投递幂等消费）；
    /// None = 关闭（默认零开销）。
    pub(crate) outbox: Option<Outbox>,
    /// 事务锁表（F 阶段三）：docid 级排他写锁 / 共享读锁 + wait-for 死锁检测。
    /// O 项第②步：内部 `Mutex`——RR 快照读只读并行时，SERIALIZABLE 读锁 / 提交写锁经锁内互斥。
    pub(crate) txn_locks: Mutex<crate::txn::LockTable>,
    /// 数据目录（P52 看门狗磁盘水位检测目标）。
    pub(crate) data_dir: std::path::PathBuf,
    /// V 项：io_uring 后端池（Linux + `runtime.io_uring_enabled` 时初始化；Windows 无此字段）。
    /// 按 IoClass 三队列 SQPOLL，read_at/write_at/fsync 经 `io_uring_*` 方法转发；
    /// 已接入热路径——CF 打开时注入（SST 块读 + WAL fsync 走 SQPOLL 队列）。
    #[cfg(target_os = "linux")]
    iou: Option<std::sync::Arc<crate::io_queue::backend::IoUringPool>>,
    /// X 项：Prometheus 风格指标（读写计数 + 延迟直方图 + Compaction/Flush 次数；
    /// 网络层连接/语句由服务进程写入共享 Metrics）。
    pub metrics: crate::metrics::Metrics,
    /// 10 亿库阶段 D：分片级指标（docid 水位 + 读写计数 + 上限预警）；默认 None，
    /// 分片部署时 `attach_shard_metrics(n)` 挂载。
    pub shard_metrics: std::sync::Mutex<Option<crate::shard_metrics::ShardMetricsRegistry>>,
}

/// 查询结果行：docid + 文档字节。
pub type QueryRow = (u64, Vec<u8>);

/// 分页查询结果（M8-P8）：`total` = 全量命中数（倒排 bitmap.len()，O(1)），
/// `rows` = 当前页（只回表 limit 行，内存 O(limit) 不随 total 膨胀——
/// 大结果集命中数百万行时全量回表 + JSON 构造会内存爆炸，实测 5M 行 → 10GB+ 卡死）。
#[derive(Debug, Clone)]
pub struct PagedRows {
    pub total: u64,
    pub rows: Vec<QueryRow>,
}

/// 引擎状态指标（`admin status` 数据源）。
#[derive(Debug, Clone)]
pub struct EngineStats {
    /// LSM：SST 文件总数（primary + cidx + delta）。
    pub sst_file_count: usize,
    /// 倒排内存累积 posting 数。
    pub inverted_mem_docids: u64,
    /// 倒排磁盘段数。
    pub inverted_segments: usize,
    /// 当前序列号（阶段 2 接入）。
    pub seq: u64,
    /// 内存使用率估算（0~1）。
    pub mem_ratio: f64,
    /// 内存硬上限（MB）。
    pub max_memory_mb: usize,
    /// 磁盘剩余空间占比（0~1；检测失败 = 1.0）。
    pub disk_ratio: f64,
    /// 磁盘空间状态（Normal/Throttled/Stalled，P52）。
    pub disk_status: String,
    /// CPU 并发查询数（P52 代理信号）。
    pub cpu_active_queries: usize,
    /// CPU 并发查询上限。
    pub cpu_query_limit: usize,
}

impl Engine {
    /// 数据目录（Ex-2.5 网关 SAGA 状态持久化目录据此派生 `{data_dir}/saga`）。
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// 更新内存使用率估算（OOM Guardian 输入，由监控/统计层刷新）。
    pub fn set_mem_ratio(&mut self, ratio: f64) {
        self.mem_ratio = ratio.clamp(0.0, 1.0);
    }

    /// 批量导入模式开关（P40）：开启后 `put_nosync` 跳过 HotCache 失效/回填。
    /// 批量导入只写不读，回填缓存纯浪费内存（默认 4GB 预算会把文档全部塞入，
    /// 叠加桌面负载触发页面颠簸 → 行速崩塌）。导入结束后应关闭恢复常规缓存语义。
    pub fn set_bulk_import(&mut self, on: bool) {
        self.skip_hotcache = on;
    }

    /// 挂载分片级指标（n 分片；分片部署时调用一次）。
    pub fn attach_shard_metrics(&self, n_shards: u16) {
        *self.shard_metrics.lock().unwrap() =
            Some(crate::shard_metrics::ShardMetricsRegistry::new(n_shards));
    }

    /// 上报分片 docid 水位（构建/写入推进时）。
    pub fn update_shard_watermark(&self, shard_id: u16, wm: u64) {
        if let Some(r) = self.shard_metrics.lock().unwrap().as_ref() {
            r.update_watermark(shard_id, wm);
        }
    }

    pub fn record_shard_write(&self, shard_id: u16) {
        if let Some(r) = self.shard_metrics.lock().unwrap().as_ref() {
            r.record_write(shard_id);
        }
    }

    pub fn record_shard_read(&self, shard_id: u16) {
        if let Some(r) = self.shard_metrics.lock().unwrap().as_ref() {
            r.record_read(shard_id);
        }
    }

    /// 分片级指标 Prometheus 渲染（未挂载返回空串）。
    pub fn shard_metrics_render(&self) -> String {
        self.shard_metrics
            .lock()
            .unwrap()
            .as_ref()
            .map(|r| r.render())
            .unwrap_or_default()
    }

    /// 分片 docid 水位预警列表（Warn/Critical）。
    pub fn shard_watermark_alerts(
        &self,
    ) -> Vec<(u16, crate::shard_metrics::WatermarkLevel, f64)> {
        self.shard_metrics
            .lock()
            .unwrap()
            .as_ref()
            .map(|r| r.alerts())
            .unwrap_or_default()
    }

    /// Ex-7.2：网络核列表（server 主线程绑核用）。
    pub fn network_cores(&self) -> Vec<usize> {
        self.affinity.network.clone()
    }

    // ============ Ex-1 本地消息表（Outbox）============

    /// Ex-8.9：前台写压力代理（与 Ex-7.4 同口径——主 MemTable 水位 0..1）。只读、廉价，
    /// 供后台维护 worker 负载感知（Busy/Normal/Idle 判定）。
    pub fn write_pressure(&self) -> f64 {
        let used = self.primary.memtable_bytes() as f64;
        let max = self.memtable_max_bytes.max(1) as f64;
        (used / max).clamp(0.0, 1.0)
    }

    /// 引擎状态指标（design 20 / development 5.25，供 `admin status`）。
    pub fn stats(&self) -> EngineStats {
        // P52：磁盘剩余空间占比（syscall 带缓存，1s 间隔）
        let (disk_ratio, disk_status) = match disk_space::space_info(&self.data_dir) {
            Ok((avail, total)) if total > 0 => {
                let r = avail as f64 / total as f64;
                let s = match self.watchdog.disk().classify(avail, total) {
                    crate::watchdog::DiskStatus::Normal => "normal",
                    crate::watchdog::DiskStatus::Throttled => "throttled",
                    crate::watchdog::DiskStatus::Stalled => "stalled",
                };
                (r, s.to_string())
            }
            _ => (1.0, "unknown".into()),
        };
        EngineStats {
            sst_file_count: self.primary.sst_count()
                + self.delta.sst_count()
                + self.cidx.as_ref().map_or(0, |c| c.sst_count()),
            inverted_mem_docids: self.inverted.mem_docids(),
            inverted_segments: self.inverted.segment_count(),
            seq: 0, // 阶段 2 接入执行器统计
            mem_ratio: self.mem_ratio,
            max_memory_mb: self.max_memory_mb,
            disk_ratio,
            disk_status,
            cpu_active_queries: self.watchdog.cpu_active(),
            cpu_query_limit: self.watchdog.cpu().limit(),
        }
    }

    /// Ex-8.11：累计写入 SST 字节（主数据 + delta + cidx 三列族 flush/compact 新文件字节和）——
    /// 写放大实验数据源（写放大 ≈ 该值 / 写入数据字节）。
    pub fn sst_written_bytes(&self) -> u64 {
        let mut w = self.primary.sst_written_bytes();
        w += self.delta.sst_written_bytes();
        if let Some(c) = &self.cidx {
            w += c.sst_written_bytes();
        }
        w
    }

    /// Ex-8.11：主列族 L0/L1/L2 段数分布（写放大 A/B 观察合并节奏）。
    pub fn lsm_layer_counts(&self) -> (usize, usize, usize) {
        self.primary.layer_counts()
    }

    /// Ex-8.13：倒排累计写盘字节（GC/刷段 seg 新写文件字节和；IO 审计数据源）。
    pub fn inverted_written_bytes(&self) -> u64 {
        self.inverted.inverted_written_bytes()
    }

}
