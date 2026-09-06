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
    /// Task-026 Per-CPU WAL（阶段1：配置/路由/队列状态骨架；`per_cpu_enabled=false` 时为
    /// 单队列回退形态，路由零开销）。阶段2 起承载队列后台写线程与独立 WAL 文件。
    pub(crate) per_cpu_wal: crate::engine::percpu_wal::PerCpuWal,
    /// Task-026 Per-CPU WAL 运行时（`per_cpu_enabled=true` 时 Some）：队列消费线程、
    /// 独立 `wal-{q}-{gseq_start}.log`、checkpoint（min 各 CF 刷盘水位）/段裁剪。None =
    /// 走既有全局组提交（零回归）。
    pub(crate) percpu: Option<std::sync::Arc<crate::engine::percpu_wal::WalRuntime>>,
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
    /// P94（热列旁路双轨）：colstore 开关（= cfg.storage.colstore_enabled）。
    pub(crate) colstore_enabled: bool,
    /// P94：colstore 热列名单（= cfg.storage.hot_fields；空 = 无法派生）。
    pub(crate) colstore_hot: Vec<String>,
    /// P94：colstore 状态（惰性派生：cs + 水位 + 派生后写入的脏 docid）。
    pub(crate) colstore_state: std::sync::Mutex<crate::engine::colstore::ColstoreState>,
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
    /// R4（review 2026-09-04）：活跃快照注册表（RR/Serializable 事务注册，commit/rollback
    /// 注销）——compact 前取其最小值作 MVCC 保活水位（见 ColumnFamily::mvcc_keep_floor）：
    /// 最新 seq > floor 的 key 保留多版本，使删除/覆盖前旧快照在 compaction 后仍可回读。
    /// value = 注册时刻（unix ms）——支撑快照生命周期计量/状态观测与受控逐出。
    pub(crate) active_snapshots: RwLock<std::collections::BTreeMap<u64, u64>>,
    /// P1-C：活跃 docid 集（`COUNT(*)` O(1) 快路径基线）。None = 未初始化（首次
    /// `count_all_docs` 全键扫一次建基线后置 Some）；写路径增量维护（新 docid put 增 /
    /// delete 减 / 覆盖不变 / 复活增），purge 复位空集。语义 = 引擎最新视图（删除位图
    /// 与 Tombstone 双路径一致）；跨线程由 db 层读写锁串行化，此处 Mutex 仅保护懒建/读。
    pub(crate) live_docids: std::sync::Mutex<Option<RoaringTreemap>>,
    /// P136（2026-09-06）：快照活跃**预过滤**删除事件表（docid → 删除提交 seq，上界口径，
    /// 见 write/read 记账注释）——供 RR 快照读在批量取行前**集合级剔除"快照前已删且未复活"
    /// 的候选**（免 batch_get_at 空跑）；只记录本进程内、删除位图 + per-CPU 模式下发生的
    /// 删除（复活 put 清除条目 → 绝不误剔，漏剔由快照读 None 兜底 = 正确性不受影响）。
    /// 活跃快照不会跨进程（begin 于 open 后）→ 无需全库基线；purge_all 复位空。
    pub(crate) snapshot_dels: std::sync::Mutex<std::collections::HashMap<u64, u64>>,
    /// P137（2026-09-06）：快照读监控计数器——batch_get_at 处理 docid 累计 / 预过滤剔除累计。
    pub(crate) snapshot_batch_rows: std::sync::atomic::AtomicU64,
    pub(crate) snapshot_prefilter_saved: std::sync::atomic::AtomicU64,
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
    /// pub(crate)：跨 engine 子模块（open.rs 构造 Self 需访问；Windows cfg 移除故此前漏提升，
    /// Linux 编译 E0451——2026-09-06 换机交接修复）。
    #[cfg(target_os = "linux")]
    pub(crate) iou: Option<std::sync::Arc<crate::io_queue::backend::IoUringPool>>,
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

/// P144-②：per-CPU WAL checkpoint 现场（`Engine::wal_replay_report` 返回）。
#[derive(Debug, Clone, Default)]
pub struct WalReplayReport {
    /// per-CPU WAL 是否启用（false = 非 per-CPU 模式，其余字段恒 0）。
    pub per_cpu: bool,
    /// checkpoint 文件持久化值（重启回放起点 = 恢复时从该 gseq 之后回放）。
    pub persisted_cp: u64,
    /// 运行期 checkpoint（min(各 CF 已刷水位)；≥ persisted_cp）。
    pub cp: u64,
    /// cidx（组合索引 CF）已刷水位（P144-②：应随主数据收敛、不再恒 0 钉死 cp）。
    pub cidx_watermark: u64,
    /// cidx 已落 SST 数（>0 = 补刷发生过）。
    pub cidx_ssts: usize,
    /// 队列文件中 > persisted_cp 的条目数 = 若此刻崩溃，下次重启将回放的行数近似。
    pub replay_pending: u64,
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
    /// 2026-09-05：活跃 MVCC 快照数（RR/Serializable 事务）。
    pub active_snapshots: usize,
    /// 2026-09-05：最老活跃快照存活 ms（长事务告警；0 = 无活跃快照）。
    pub snapshot_oldest_ms: u64,
}

impl Engine {
    /// 数据目录（Ex-2.5 网关 SAGA 状态持久化目录据此派生 `{data_dir}/saga`）。
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// P144-② 只读诊断：per-CPU WAL checkpoint 现场（运维/回放验证 bin 用）。
    /// `replay_pending` = 队列文件中 > 持久化 cp 的条目数——即此刻若崩溃/非干净退出，
    /// 下次重启将回放的行数近似（cp 推进即持久化后应 ≈ 0）。
    pub fn wal_replay_report(&self) -> WalReplayReport {
        use crate::engine::percpu_wal::CF_CIDX;
        use std::sync::atomic::Ordering as O;
        let mut rep = WalReplayReport::default();
        if let Some(rt) = &self.percpu {
            let persisted = rt.load_checkpoint();
            let pending = rt
                .records_after(persisted)
                .map(|v| v.len() as u64)
                .unwrap_or(0);
            rep = WalReplayReport {
                per_cpu: true,
                persisted_cp: persisted,
                cp: rt.cp.load(O::Relaxed),
                cidx_watermark: rt.cf_watermarks[CF_CIDX as usize].load(O::Relaxed),
                cidx_ssts: self.cidx.as_ref().map_or(0, |c| c.sst_count()),
                replay_pending: pending,
            };
        }
        rep
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

    /// P131b（2026-09-06）：倒排 term **声明字段集**（server 写路径 doc_terms 白名单）——
    /// inverted_fields(白名单) ∪ fulltext_fields ∪ stats_fields ∪ bitmap_fields。
    /// 返回 None = 无声明（保持全字段现行为，default 零回归）；Some(非空) 时只生成这些字段
    /// 的 term——未声明短字段/大文本不再构造 term（省 CPU/字典膨胀，声明式本意）。
    pub fn term_index_fields(&self) -> Option<std::collections::HashSet<String>> {
        let mut s: std::collections::HashSet<String> = match &self.inverted_include {
            Some(v) => v.clone(),
            None => std::collections::HashSet::new(),
        };
        s.extend(self.fulltext_fields.iter().cloned());
        s.extend(self.stats_fields.iter().cloned());
        s.extend(self.inverted.bitmap_fields());
        if s.is_empty() {
            None
        } else {
            Some(s)
        }
    }

    /// 2026-09-05（内存口径观测）：组件内存计量（字节/计数）——`/metrics` 实时 gauge。
    /// 覆盖：主 memtable、hotcache、blockcache、倒排内存 docid；用于 RSS 构成拆解
    /// （cache payload ≠ 进程 RSS；元数据/临时对象/mmap 页另计，容量规划乘 1.4~1.8×）。
    pub fn memory_report(&self) -> Vec<(&'static str, &'static str, u64)> {
        vec![
            ("shanshui_mem_memtable_bytes", "主列族 memtable 内存（字节）", self.primary.memtable_bytes() as u64),
            ("shanshui_mem_hotcache_bytes", "文档热缓存占用（字节）", self.hotcache.used_bytes() as u64),
            ("shanshui_mem_blockcache_bytes", "块缓存占用（字节，SST 块 payload）", self.primary.blockcache_bytes() as u64),
            ("shanshui_mem_inverted_mem_docids", "倒排内存累积 docid posting 数", self.inverted_mem_docids()),
        ]
    }

    /// 2026-09-05（P2 Bloom 分层计量）：读路径过滤计数 gauge（minmax/legacy/分区 probe·skip·pass/fp）。
    /// 聚合 primary + delta + cidx 三列族；`/metrics` 与 SHOW MEMORY 采集。
    pub fn bloom_report(&self) -> Vec<(&'static str, &'static str, u64)> {
        let (p0, p1, p2, p3, p4, p5) = self.primary.bloom_counts();
        let (d0, d1, d2, d3, d4, d5) = self.delta.bloom_counts();
        let (c0, c1, c2, c3, c4, c5) = match &self.cidx {
            Some(cf) => cf.bloom_counts(),
            None => (0, 0, 0, 0, 0, 0),
        };
        let (s0, s1, s2, s3, s4, s5) = (
            p0 + d0 + c0,
            p1 + d1 + c1,
            p2 + d2 + c2,
            p3 + d3 + c3,
            p4 + d4 + c4,
            p5 + d5 + c5,
        );
        vec![
            ("shanshui_bloom_minmax_skip_total", "段级 min/max 粗筛跳过（精确）", s0),
            ("shanshui_bloom_legacy_skip_total", "v3/v4 整文件布隆 miss", s1),
            ("shanshui_bloom_partition_probe_total", "v5 分区布隆校验进入（目标块）", s2),
            ("shanshui_bloom_partition_skip_total", "v5 分区布隆拒绝", s3),
            ("shanshui_bloom_partition_pass_total", "v5 分区布隆放行（真正读块）", s4),
            ("shanshui_bloom_fp_est_total", "误报估计（放行后段未命中）", s5),
        ]
    }

    /// 2026-09-05（P0 观测 ②）：布隆读路径过滤计数**按层**（L0/L1/L2）gauge——
    /// 聚合 primary + delta + cidx 三列族。定位"瓶颈层"：
    /// - L0 桶 `minmax_skip`/`part_probe` 随 L0 段数线性增长 → 段数管理（per-table 压实）；
    /// - L1/L2 桶 `probe/pass` 高而 `skip` 低 → 分层 fpr/布隆覆盖调优候选。
    /// 名称 = `shanshui_bloom_{minmax_skip|legacy_skip|partition_probe|partition_skip|
    /// partition_pass|fp_est}_total_{l0|l1|l2}`。
    pub fn bloom_layer_report(&self) -> Vec<(String, String, u64)> {
        let p = self.primary.bloom_layer_counts();
        let d = self.delta.bloom_layer_counts();
        let c = match &self.cidx {
            Some(cf) => cf.bloom_layer_counts(),
            None => [[0u64; 6]; 3],
        };
        let mut agg = [[0u64; 6]; 3];
        for (lv, col) in agg.iter_mut().enumerate() {
            for (i, cell) in col.iter_mut().enumerate() {
                *cell = p[lv][i] + d[lv][i] + c[lv][i];
            }
        }
        const METRIC: [&str; 6] = [
            "minmax_skip",
            "legacy_skip",
            "partition_probe",
            "partition_skip",
            "partition_pass",
            "fp_est",
        ];
        const HELP: [&str; 6] = [
            "段级 min/max 粗筛跳过（精确）",
            "v3/v4 整文件布隆 miss",
            "v5 分区布隆校验进入（目标块）",
            "v5 分区布隆拒绝",
            "v5 分区布隆放行（真正读块）",
            "误报估计（放行后段未命中）",
        ];
        const LV: [&str; 3] = ["l0", "l1", "l2"];
        let mut out = Vec::with_capacity(18);
        for lv in 0..3usize {
            for i in 0..6usize {
                out.push((
                    format!("shanshui_bloom_{}_total_{}", METRIC[i], LV[lv]),
                    format!("[{}] {}", LV[lv].to_uppercase(), HELP[i]),
                    agg[lv][i],
                ));
            }
        }
        out
    }

    /// 2026-09-06（P137）：倒排专项监控 + 快照读监控 gauge——`/metrics` 与 SHOW MEMORY 采集。
    /// 覆盖：段数 / mem 深度 / 落盘 counter / posting 位图缓存命中·未命中 / GC·delta FST 超限
    /// 待收敛标志（1/0，运维判断"要不要给倒排 GC 腾资源/写入是否过猛导致段堆积"）+ 快照读
    /// 批量 docid 累计与预过滤剔除累计（省空回表收益观测）。
    pub fn inverted_report(&self) -> Vec<(&'static str, &'static str, u64)> {
        use std::sync::atomic::Ordering;
        vec![
            ("shanshui_inv_segment_count", "倒排磁盘段数（读合并代价∝段数）", self.inverted.segment_count() as u64),
            ("shanshui_inv_mem_docids", "倒排内存累积 docid posting 数（待落盘）", self.inverted_mem_docids()),
            ("shanshui_inv_seg_flush_total", "倒排段落盘次数（counter）", self.inverted.seg_flush_total.load(Ordering::Relaxed)),
            ("shanshui_inv_posting_cache_hits_total", "posting 位图缓存命中（重复查询免反序列化）", self.inverted.posting_cache_hits.load(Ordering::Relaxed)),
            ("shanshui_inv_posting_cache_misses_total", "posting 位图缓存未命中（反序列化重建）", self.inverted.posting_cache_misses.load(Ordering::Relaxed)),
            ("shanshui_inv_gc_pending", "段总量超 GC 阈值待收敛（1=是）", u64::from(self.inverted.should_gc())),
            ("shanshui_inv_delta_fst_over_limit", "最新段 delta FST 超限待合并（1=是）", u64::from(self.inverted.should_delta_gc())),
            ("shanshui_snapshot_batch_rows_total", "batch_get_at 处理 docid 累计（快照批量回表量）", self.snapshot_batch_rows.load(Ordering::Relaxed)),
            ("shanshui_snapshot_prefilter_saved_total", "快照预过滤剔除候选累计（免空回表行数）", self.snapshot_prefilter_saved.load(Ordering::Relaxed)),
        ]
    }

    /// 2026-09-05（P0 观测 ①）：块缓存 (命中/未命中/容量淘汰) 计数 gauge——
    /// 聚合 primary + delta + cidx 三列族。命中率与淘汰增速是读放大主判据：
    /// 命中低 + 淘汰高 → 数据块 LRU/预算压力（区别于 L0 段线性放大）。
    pub fn blockcache_report(&self) -> Vec<(&'static str, &'static str, u64)> {
        let (ph, pm, pe) = self.primary.blockcache_stats();
        let (dh, dm, de) = self.delta.blockcache_stats();
        let (ch, cm, ce) = match &self.cidx {
            Some(cf) => cf.blockcache_stats(),
            None => (0, 0, 0),
        };
        vec![
            (
                "shanshui_blockcache_hits_total",
                "块缓存命中（读块免磁盘 IO，三列族聚合）",
                ph + dh + ch,
            ),
            (
                "shanshui_blockcache_misses_total",
                "块缓存未命中（读盘回填，三列族聚合）",
                pm + dm + cm,
            ),
            (
                "shanshui_blockcache_evicts_total",
                "块缓存容量淘汰条目（LRU 压力，三列族聚合）",
                pe + de + ce,
            ),
        ]
    }

    /// 2026-09-05（P0 观测 ③ + P129 补充）：**L0 层按表段数** gauge——主列族（split_by_table，
    /// 每段单表）每表 L0 段数。多表热点场景观测"全局 L0 未满但单表 L0 堆积"
    /// （该表点查读放大 O(段数)），为 per-table L0 优先压实调度提供输入。
    /// 名称 = `shanshui_l0_sst_count_table_{tid}`；行数随活跃表数增长。
    /// **P129 补充汇总行**（监控/告警单点可读，不必解析 N 行动态名）：
    ///   `shanshui_l0_tables_active`（L0 有段表数）/ `shanshui_l0_sst_count_over_trigger`
    ///   （段数 ≥ per_table_l0_trigger 的表数 = 待压实压力）/ `shanshui_l0_sst_count_max`
    ///   （最大段数）/ `shanshui_l0_sst_hottest_table`（段数最多表 id）/
    ///   `shanshui_per_table_compact_runs`（P129 压实执行次数，counter）。
    pub fn l0_table_report(&self) -> Vec<(String, String, u64)> {
        let counts = self.primary.l0_table_counts();
        let active = counts.len();
        let max_n = counts.iter().map(|(_, n)| *n).max().unwrap_or(0);
        let trigger = self.primary.per_table_l0_trigger;
        let (hot_tid, over) = if active == 0 {
            (0u16, 0usize)
        } else {
            (
                counts.iter().max_by_key(|(_, n)| *n).unwrap().0,
                if trigger > 0 {
                    counts.iter().filter(|(_, n)| *n >= trigger).count()
                } else {
                    0
                },
            )
        };
        let mut out: Vec<(String, String, u64)> = counts
            .into_iter()
            .map(|(tid, n)| {
                (
                    format!("shanshui_l0_sst_count_table_{tid}"),
                    format!("主列族表 {tid} L0 段数（全局 L0 计数下该表堆积观测）"),
                    n as u64,
                )
            })
            .collect();
        out.push((
            "shanshui_l0_tables_active".into(),
            "主列族 L0 层有段的表数".into(),
            active as u64,
        ));
        out.push((
            "shanshui_l0_sst_count_over_trigger".into(),
            format!("L0 段数 ≥ per_table_l0_trigger({trigger}) 的表数（待压实压力）"),
            over as u64,
        ));
        out.push((
            "shanshui_l0_sst_count_max".into(),
            "各表 L0 段数最大值（稳态应 ≤1）".into(),
            max_n as u64,
        ));
        out.push((
            "shanshui_l0_sst_hottest_table".into(),
            "L0 段数最多的表 id".into(),
            hot_tid as u64,
        ));
        out.push((
            "shanshui_per_table_compact_runs".into(),
            "per-table L0 压实执行次数（多表写放大间接量，counter）".into(),
            self.primary.per_table_compact_runs(),
        ));
        out
    }

    /// 2026-09-05（P129 补充监控）：/metrics **label 化** per-table 文本（Prometheus 面板/
    /// 告警聚合友好，替代动态名行的不可聚合问题）：
    ///   `shanshui_l0_sst_count{table="<tid>"}`（gauge，每表 L0 段数）
    ///   `shanshui_l0_sst_over_trigger{table="<tid>",trigger="<T>"}`（1 = 该表待压实）
    ///   `shanshui_per_table_compact_runs_total`（counter）
    pub fn l0_table_metrics_prom(&self) -> String {
        let mut out = String::new();
        let counts = self.primary.l0_table_counts();
        out.push_str(
            "# HELP shanshui_l0_sst_count 主列族 L0 层每表段数（split_by_table 每段单表）\n\
             # TYPE shanshui_l0_sst_count gauge\n",
        );
        for (tid, n) in &counts {
            out.push_str(&format!("shanshui_l0_sst_count{{table=\"{tid}\"}} {n}\n"));
        }
        let trigger = self.primary.per_table_l0_trigger;
        if trigger > 0 {
            out.push_str(
                "# HELP shanshui_l0_sst_over_trigger 表 L0 段数 ≥ per_table_l0_trigger（1=该表待压实）\n\
                 # TYPE shanshui_l0_sst_over_trigger gauge\n",
            );
            for (tid, n) in &counts {
                let v = if *n >= trigger { 1u64 } else { 0u64 };
                out.push_str(&format!(
                    "shanshui_l0_sst_over_trigger{{table=\"{tid}\",trigger=\"{trigger}\"}} {v}\n"
                ));
            }
        }
        out.push_str(
            "# HELP shanshui_per_table_compact_runs_total per-table L0 压实执行次数（多表写放大间接量）\n\
             # TYPE shanshui_per_table_compact_runs_total counter\n",
        );
        out.push_str(&format!(
            "shanshui_per_table_compact_runs_total {}\n",
            self.primary.per_table_compact_runs()
        ));
        out
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
            active_snapshots: self.active_snapshot_count(),
            snapshot_oldest_ms: self.oldest_snapshot_age_ms(),
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
