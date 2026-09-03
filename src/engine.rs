//! 文档引擎 + 查询执行器（design 7.1 / development 步骤 12）。
//!
//! 整合主数据列族、组合索引列族、倒排索引与 HotCache，对外提供文档级 CRUD 与查询；
//! 查询执行器基于 optimizer 静态路由（MVP 最小集枚举，无代价估算），
//! 后续动态路由（代价估算依赖统计载荷）在阶段 1.5 落地。
//!
//! 删除一致性：文档删除后，倒排中残留 docid 在回表时经主数据 Tombstone 天然过滤。

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roaring::RoaringBitmap;
use tracing::{info, warn};

use crate::bitmap::DeletionBitmap;
use crate::column_family::ColumnFamily;
use crate::config::model::Config;
use crate::error::Result;
use crate::hotcache::HotCache;
use crate::inverted::InvertedIndex;
use crate::keys::{decode_docid, encode_docid, encode_varlen};
use crate::optimizer::{route, AccessPath, QuerySpec};
use crate::outbox::Outbox;
use crate::watchdog::{Watchdog, DEFAULT_QUERY_TIMEOUT};

/// Ex-9.3 第①步：解析文档 JSON 中声明 stats 字段的数值（与 `stats_fields` 对齐；
/// 缺字段 / JSON null / 非数值 → None 跳过；文档不可解析 → 全 None）。
fn engine_doc_stats(fields: &[String], value: &[u8]) -> Vec<Option<f64>> {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(value) else {
        return vec![None; fields.len()];
    };
    fields.iter().map(|f| v.get(f).and_then(|x| x.as_f64())).collect()
}

/// 引擎：组合主数据 + 组合索引 + Delta 增量 + 倒排 + HotCache。
pub struct Engine {
    /// 主数据列族（value = 序列化文档字节）。
    /// P72（无锁合并）：`Arc<ColumnFamily>`——mysql 后台 worker clone 三 CF Arc 后无锁合并，
    /// flush/compact 的 ssts 变更经 CF 内部 `sst_mutate` 互斥（不再依赖 Engine RwLock 串行）。
    primary: Arc<ColumnFamily>,
    /// 组合索引列族（key = encode_composite_key）。
    cidx: Option<Arc<ColumnFamily>>,
    /// Delta 增量列族（阶段 1.5，key = encode_docid ++ VarLen(field)，Merge-on-Read 覆盖 Base）。
    delta: Arc<ColumnFamily>,
    /// 倒排索引。J 项（7.73）：`Arc`（后台 GC worker 无锁 clone 后执行 gc）。
    pub inverted: Arc<InvertedIndex>,
    /// 文档热缓存（7.72：内部 RwLock+DashMap 粒度化——读路径 `&self` 读读并行、
    /// 写路径（put/invalidate/promote）写锁，不再整包 Mutex 串行热缓存访问）。
    hotcache: HotCache,
    /// 看门狗（OOM 限流 + 查询超时熔断）。
    watchdog: Watchdog,
    /// 内存使用率估算（0~1，由上层注入或监控更新）。
    mem_ratio: f64,
    /// 内存硬上限（MB，`memory.max_memory_mb`，供 admin status）。
    max_memory_mb: usize,
    /// 全局 seq 分配器（MVCC，M7-1）：primary / delta 列族共享，跨列族写入序一致。
    global_seq: Arc<AtomicU64>,
    /// 组提交（M8）：Some((窗口, 字节阈值)) = 开启；None = 关闭（逐条 fsync 强安全）。
    /// 窗口内写入攒批一次 fsync（design 4.3 / M8，`storage.group_commit_us`）。
    group_commit: Option<(Duration, usize)>,
    /// 组提交后台线程停止标志（窗口尾部落盘兜底）。
    gc_stop: Option<Arc<AtomicBool>>,
    /// 组提交后台线程句柄。
    gc_thread: Option<std::thread::JoinHandle<()>>,
    /// 倒排字段白名单（M8-P4）：Some = 只建声明字段倒排；None = 全部（黑名单仍生效）。
    inverted_include: Option<std::collections::HashSet<String>>,
    /// 倒排字段黑名单（M8-P4）：这些字段不建倒排（白名单非空时忽略）。
    inverted_exclude: std::collections::HashSet<String>,
    /// 倒排 term 长度上限（M8-P4）：超过自动跳过（长文本整串不进字典）；0 = 不限。
    max_term_len: usize,
    /// fulltext 分词字段（M8-P7）：声明字段做分词建词 term 索引（`ft:{field}:{token}`）。
    fulltext_fields: std::collections::HashSet<String>,
    /// 中文分词器（M8-P13）：true = jieba 完整词典分词（需 cjk-jieba feature）；
    /// false = bigram（M8-P9）。来自 `[inverted] cjk_segmenter`。
    use_jieba: bool,
    /// Ex-9.3：倒排统计载荷声明字段（`cfg.inverted.stats_fields`，空 = 关闭）。
    stats_fields: Vec<String>,
    /// 写入 Enrich（design 19 / development 5.21）：Some((fail_policy, from_field, to_field)) =
    /// 启用 local 数据源预连接（server /put 走 join::put_with_enrich）；None = 关闭（零开销）。
    enrich: Option<(String, String, String)>,
    /// 批量导入模式（P40）：跳过 HotCache 回填/失效。批量导入只写不读，回填缓存纯浪费内存
    /// （4GB 预算灌满 + stats 泄漏 → 触发页面颠簸 → 行速指数级崩塌，50M 导入 4M 行后卡死）。
    skip_hotcache: bool,
    /// 倒排更新攒批缓冲（Ex-5.3）：put 时 term 先入缓冲，达阈值/查询/flush 时
    /// 一次性 `add_batch` 批量刷入内存字典——低基数 term 跨行聚合，
    /// 减少 DashMap 锁操作次数（N×字段数 → ~唯一 term 数）。
    /// 崩溃安全：WAL 回放重新走 put 重建倒排，缓冲丢失不丢数据。
    /// O 项第②步：内部 `Mutex`——倒排读路径 `&self` 下也能先刷缓冲再查（一致性）。
    pending_inverted: Mutex<Vec<(String, u64)>>,
    /// Compaction 并行度（Ex-5.4）：并行压实 primary/cidx/delta 三列族；
    /// 0 = 自动（min(4, 核数/2)），1 = 串行，>1 = 指定并行数。
    compaction_parallel: usize,
    /// P 项：事件驱动自动 Compaction（`storage.auto_compact`）——写入路径自触发：
    /// 写前 L0 达硬顶（l0_stall_max）先合并（背压），写后 L0 超阈值（段数/大小）合并收敛。
    auto_compact: bool,
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
    deletion_bitmap: Option<Arc<DeletionBitmap>>,
    /// Ex-8.7 删除密度（删除位图置位率驱动 Compaction）调度状态——`Arc` 供无锁合并
    /// （`CompactTargets::run`）在 Engine 读锁外按压实结果回写（drop>0 继续排空 / 0 收敛）。
    /// - `garbage_marked`：位图当前置位 docid **净数**（幂等重删不重计、复活即减，精确）；
    ///   打开时 = 位图既有置位数（历史置位）。
    /// - `garbage_done`：最近一次"排空收敛"时的 `garbage_marked` 快照——此后需新增置位
    ///   ≥ `delete_density_min_docs` 才再次进入删除密度触发（历史置位不重复触发重写）。
    /// - `garbage_draining`：排空进行中（最近一轮主列族压实实际物理丢弃 >0 → 继续 GC，
    ///   直至某轮 0 丢弃 → 收敛并刷新 `garbage_done`）。
    garbage_marked: Arc<AtomicU64>,
    garbage_done: Arc<AtomicU64>,
    garbage_draining: Arc<AtomicBool>,
    /// 曾写入的最大 docid（≈ 曾插入文档数，删除置位率分母；put 时 fetch_max）。
    max_docid: AtomicU64,
    /// §27 P0：重启后 max_docid 归零 → `auto_watermark` 首次调用做一次全库 keys-only
    /// 恢复（AtomicBool swap 保证只扫一次；运行期 put 的 fetch_max 持续维护）。
    max_docid_loaded: AtomicBool,
    /// 删除密度触发阈值（`storage.delete_density_min_ratio` / `_min_docs`）。
    dd_min_ratio: f32,
    dd_min_docs: u64,
    /// 三池核分区（Ex-7.2）：network（server 主线程）/ compute（Compaction 并行）/
    /// io（组提交后台）——绑核消除调度抖动；enabled=false 时为空（no-op）。
    affinity: crate::affinity::CpuPartition,
    /// 后台 IO 限速基准（字节/秒，Ex-7.4）：`storage.io_rate_limit_mb` 换算；0 = 不限速。
    io_rate_base_bytes: u64,
    /// MemTable 容量上限（字节，Ex-7.4 写压力代理基准，`memtable.max_size_mb`）。
    memtable_max_bytes: usize,
    /// 本地消息表（Ex-1）：Some = 启用（业务写同一本地事务入队 outbox，后台投递幂等消费）；
    /// None = 关闭（默认零开销）。
    outbox: Option<Outbox>,
    /// 事务锁表（F 阶段三）：docid 级排他写锁 / 共享读锁 + wait-for 死锁检测。
    /// O 项第②步：内部 `Mutex`——RR 快照读只读并行时，SERIALIZABLE 读锁 / 提交写锁经锁内互斥。
    txn_locks: Mutex<crate::txn::LockTable>,
    /// 数据目录（P52 看门狗磁盘水位检测目标）。
    data_dir: std::path::PathBuf,
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

/// 倒排攒批缓冲阈值（条，Ex-5.3）：达此值强制 `flush_inverted_pending`。
const INVERTED_PENDING_CAP: usize = 8192;

/// 查询结果行：docid + 文档字节。
pub type QueryRow = (u64, Vec<u8>);

/// 合并两列族的压实报告（Ex-5.4：multi-CF 聚合；out_level 保留 base 值）。
fn merge_report(base: &mut crate::column_family::CompactReport, other: &crate::column_family::CompactReport) {
    base.merged_ssts += other.merged_ssts;
    base.kept_keys += other.kept_keys;
    base.freed_bytes += other.freed_bytes;
    base.dropped_keys += other.dropped_keys;
}

/// P72（无锁合并）：`Engine::compaction_targets` 的产物——已 clone 的三 CF Arc + 删除位图 Arc
/// 与最高紧迫度档判定。mysql worker drop Engine 读锁后调 `run()` 无锁合并。
pub struct CompactTargets {
    /// 删除位图（Some 且 deleted_count>0 → 合并过滤已删键；None = 传统 Tombstone 路径）。
    pub deletion_bitmap: Option<Arc<DeletionBitmap>>,
    /// 主数据列族 Arc（clone 共享）。
    pub primary: Arc<ColumnFamily>,
    /// 组合索引列族（可能未启用 / 非最高紧迫度档）。
    pub cidx: Option<Arc<ColumnFamily>>,
    /// Delta 增量列族 Arc。
    pub delta: Arc<ColumnFamily>,
    /// 各列族是否为本轮最高紧迫度档（紧凑调度，与 `Engine::compact` 一致）。
    pub do_primary: bool,
    pub do_cidx: bool,
    pub do_delta: bool,
    /// Ex-8.7：删除密度状态（Engine 字段的 Arc clone，`run()` 锁外按压实结果回写）。
    pub garbage_marked: Arc<AtomicU64>,
    pub garbage_done: Arc<AtomicU64>,
    pub garbage_draining: Arc<AtomicBool>,
    /// Ex-8.7：本轮到 `gc_single` 是否允许**单段重写**（删除密度触发时 true——
    /// 收敛后单底层段无常规合并候选，需重写才能物理回收已删数据）。
    pub gc_single: bool,
}

/// Ex-8.7：主列族压实反馈——按删除位图实际**物理丢弃 >0** → 继续删除密度排空；
/// 0 丢弃 → 排空收敛（快照 `garbage_marked`，此后需新增置位 ≥ min_docs 才再触发）。
fn apply_gc_feedback(
    draining: &AtomicBool,
    done: &AtomicU64,
    marked: &AtomicU64,
    r: &crate::column_family::CompactReport,
) {
    if r.dropped_keys > 0 {
        draining.store(true, Ordering::Relaxed);
    } else {
        draining.store(false, Ordering::Relaxed);
        done.store(marked.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

impl CompactTargets {
    /// 无锁合并执行：仅压最高紧迫度档列族（串行；并列场景由 worker 下一轮收敛）。
    /// 与写并发安全：compact 不碰 memtable；ssts 变更经 CF `sst_mutate` 与 flush 互斥。
    pub fn run(&self) -> Result<crate::column_family::CompactReport> {
        let empty = crate::column_family::CompactReport {
            merged_ssts: 0,
            kept_keys: 0,
            freed_bytes: 0,
            out_level: 0,
            dropped_keys: 0,
        };
        let mut rep = empty;
        let bm = self.deletion_bitmap.as_ref();
        let needs_filter = bm.is_some_and(|b| b.deleted_count() > 0);
        if self.do_primary {
            // Ex-8.7：过滤压实（多段常规合并等价 compact_filtered；删除密度触发时
            // `gc_single=true` 允许收敛后单底层段重写回收）→ 按 dropped 回写排空状态
            let r = if needs_filter {
                let f = |k: &[u8]| bm.is_some_and(|b| b.is_deleted_key(k));
                self.primary.compact_gc(&f, self.gc_single)?
            } else {
                self.primary.compact()?
            };
            merge_report(&mut rep, &r);
            apply_gc_feedback(&self.garbage_draining, &self.garbage_done, &self.garbage_marked, &r);
        }
        if self.do_cidx {
            if let Some(c) = &self.cidx {
                let r = c.compact()?;
                merge_report(&mut rep, &r);
            }
        }
        if self.do_delta {
            let r = self.delta.compact()?;
            merge_report(&mut rep, &r);
        }
        Ok(rep)
    }
}

/// N 项：Delta 批量覆盖收集——单次范围扫描 `[min..max]`（docid 编码 + 字段变长前缀），
/// 按 docid 分组返回字段覆盖列表（`null` 值 = 删除字段），替代逐 docid 扫描。
/// key 布局与 `Engine::patch`/`get` 一致：8 字节 docid ++ 4 字节 VarLen 前缀 ++ 字段名。
/// O 项第②步：`&ColumnFamily`（delta 扫描读路径已 &self）。
fn batch_delta_overrides(
    delta: &ColumnFamily,
    docids: &[u64],
) -> Result<std::collections::HashMap<u64, Vec<(String, serde_json::Value)>>> {
    let mut out: std::collections::HashMap<u64, Vec<(String, serde_json::Value)>> =
        std::collections::HashMap::new();
    let (Some(&min), Some(&max)) = (docids.iter().min(), docids.iter().max()) else {
        return Ok(out);
    };
    let start = encode_docid(min).to_vec();
    let mut end = encode_docid(max).to_vec();
    end.extend_from_slice(&[0xFF; 4]);
    let rows = delta.scan_raw_range(Some(&start), Some(&end))?;
    for (k, v) in rows {
        if k.len() < 12 {
            continue;
        }
        let docid = match decode_docid(&k[..8]) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let field = match String::from_utf8(k[12..].to_vec()) {
            Ok(f) => f,
            Err(_) => continue,
        };
        let val: serde_json::Value = match serde_json::from_slice(&v) {
            Ok(x) => x,
            Err(_) => continue,
        };
        out.entry(docid).or_default().push((field, val));
    }
    Ok(out)
}

/// 分页查询结果（M8-P8）：`total` = 全量命中数（倒排 bitmap.len()，O(1)），
/// `rows` = 当前页（只回表 limit 行，内存 O(limit) 不随 total 膨胀——
/// 大结果集命中数百万行时全量回表 + JSON 构造会内存爆炸，实测 5M 行 → 10GB+ 卡死）。
#[derive(Debug, Clone)]
pub struct PagedRows {
    pub total: u64,
    pub rows: Vec<QueryRow>,
}

/// 增量备份文件（design 20，M6-5）：seq 游标 + WAL 记录集（JSON 持久化）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct IncrementalBackupFile {
    since_seq: u64,
    until_seq: u64,
    records: Vec<crate::wal::WalRecord>,
}

/// 增量备份报告。
#[derive(Debug, Clone, Copy)]
pub struct BackupReport {
    pub since_seq: u64,
    pub until_seq: u64,
    pub records: usize,
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

    /// 打开（或创建）引擎。倒排刷盘阈值取自内存预算的比例（MVP 固定 1M posting）。
    pub fn open(data_dir: &Path, cfg: &Config) -> Result<Self> {
        Self::open_with_timeout(data_dir, cfg, DEFAULT_QUERY_TIMEOUT)
    }

    /// 打开引擎并指定查询超时（压测/大结果集场景需放宽熔断阈值）。
    pub fn open_with_timeout(
        data_dir: &Path,
        cfg: &Config,
        query_timeout: std::time::Duration,
    ) -> Result<Self> {
        // Ex-5.10 多 SSD 条带化目录路由：WAL 独占最快盘（wal_dir），SSTable 数据盘（sst_dir），
        // 倒排独立盘（inverted_dir）；未配置时回退单盘 data_dir 布局（旧行为）。
        let sst_root = cfg.storage.sst_dir.as_deref().map(Path::new).unwrap_or(data_dir);
        let wal_root = cfg.storage.wal_dir.as_deref().map(Path::new).unwrap_or(sst_root);
        let inverted_root = cfg
            .storage
            .inverted_dir
            .as_deref()
            .map(Path::new)
            .unwrap_or(data_dir);
        // V 项：io_uring 后端池初始化（Linux + `runtime.io_uring_enabled`）——SQPOLL 三队列
        // （WAL/SST/倒排）+ affinity 三池外预留核；提前到 CF 打开之前创建，注入各 CF
        // （SST 块读 + WAL fsync 走 io_uring）。Windows 编译为空（cfg 移除变量）。
        #[cfg(target_os = "linux")]
        let iou = {
            let affinity = crate::affinity::plan_partition(&cfg.affinity);
            if crate::io_queue::io_uring_enabled(&cfg.runtime) {
                let sqpoll_cpu =
                    crate::affinity::reserve_sqpoll_core(&affinity).map(|c| c as u32);
                let pool = crate::io_queue::backend::IoUringPool::open(256, 1000, sqpoll_cpu);
                match &pool {
                    Ok(_) => info!(
                        "io_uring 后端池初始化成功（SQPOLL 三队列，预留核={:?}）",
                        sqpoll_cpu
                    ),
                    Err(e) => warn!(
                        "io_uring 后端池初始化失败，回退同步 IO: {e}（io_uring_enabled 未生效）"
                    ),
                }
                pool.ok().map(std::sync::Arc::new)
            } else {
                info!("io_uring 未启用（runtime.io_uring_enabled=false），走同步 IO");
                None
            }
        };
        #[cfg(target_os = "linux")]
        let primary = {
            let mut cf = ColumnFamily::open_with_io_uring(
                "primary",
                &sst_root.join("primary"),
                Some(wal_root),
                cfg,
                iou.clone(),
            )?;
            // M3（§26 多表）：主数据列族 flush/compaction 输出按表切分（docid 高位含表）
            cf.enable_table_split();
            Arc::new(cf)
        };
        #[cfg(not(target_os = "linux"))]
        let primary = {
            let mut cf = ColumnFamily::open_with_wal_dir(
                "primary",
                &sst_root.join("primary"),
                Some(wal_root),
                cfg,
            )?;
            // M3（§26 多表）：主数据列族 flush/compaction 输出按表切分（docid 高位含表）
            cf.enable_table_split();
            Arc::new(cf)
        };
        let mut inverted = InvertedIndex::open_with_gc(
            &inverted_root.join("inverted"),
            // L 项：倒排刷盘阈值可配（config.inverted.flush_threshold；0 = 默认 100 万 term 对）
            if cfg.inverted.flush_threshold > 0 {
                cfg.inverted.flush_threshold
            } else {
                1_000_000
            },
            &cfg.inverted.engine,
            cfg.inverted.segment_max_size_mb * 1024 * 1024,
        )?;
        // 位图索引（design 5.2.4，M7-2）：白名单非空时全量重建内存位图
        inverted.with_bitmap_fields(&cfg.inverted.bitmap_fields)?;
        // Ex-8.13：倒排 GC/后台段写共享后台 IO 预算（与列族压缩同受 Ex-7.4 写压力收窄；
        // 前台紧急刷段仅记账不等待）
        if cfg.storage.io_rate_limit_mb > 0 {
            inverted.attach_io_budget(cfg.storage.io_rate_limit_mb * 1024 * 1024);
        }
        let cidx = {
            // V 项：Linux + 启用时注入 io_uring 池（cidx 可选 CF，失败容忍）
            #[cfg(target_os = "linux")]
            {
                ColumnFamily::open_with_io_uring(
                    "cidx",
                    &sst_root.join("cidx"),
                    Some(wal_root),
                    cfg,
                    iou.clone(),
                )
            }
            #[cfg(not(target_os = "linux"))]
            {
                ColumnFamily::open_with_wal_dir(
                    "cidx",
                    &sst_root.join("cidx"),
                    Some(wal_root),
                    cfg,
                )
            }
        }
        .ok()
        .map(Arc::new);
        #[cfg(target_os = "linux")]
        let delta = Arc::new(ColumnFamily::open_with_io_uring(
            "delta",
            &sst_root.join("delta"),
            Some(wal_root),
            cfg,
            iou.clone(),
        )?);
        #[cfg(not(target_os = "linux"))]
        let delta = Arc::new(ColumnFamily::open_with_wal_dir(
            "delta",
            &sst_root.join("delta"),
            Some(wal_root),
            cfg,
        )?);
        // 删除位图（Ex-5.6）：开启时加载/创建独立位图文件（4KB 页对齐，见 bitmap.rs）
        let deletion_bitmap = if cfg.storage.deletion_bitmap_enabled {
            Some(Arc::new(DeletionBitmap::open(&data_dir.join("deletion.bitmap"))?))
        } else {
            None
        };
        // Ex-8.7：打开时既有置位数 = 删除密度基准（`garbage_done` 同值）——
        // 重启后历史置位不重复触发 GC 重写；需**本会话新增置位** ≥ min_docs 才触发。
        let bm_deleted = deletion_bitmap
            .as_ref()
            .map(|b| b.deleted_count())
            .unwrap_or(0);
        // 本地消息表（Ex-1）：开启时打开 outbox 列族（数据盘，与 primary 同崩溃安全模型）
        let outbox = if cfg.outbox.enabled {
            Some(Outbox::open(&sst_root.join("outbox"), cfg)?)
        } else {
            None
        };
        // MVCC 全局 seq（M7-1）：以各列族 WAL 恢复后的 next_seq 取最大作为全局起点，
        // 此后 primary / delta / outbox 写入共享同一计数器（跨列族快照隔离正确）。
        let global_seq = Arc::new(AtomicU64::new(
            primary
                .wal_next_seq()
                .max(delta.wal_next_seq())
                .max(outbox.as_ref().map_or(0, |o| o.wal_next_seq())),
        ));
        // P72：open 阶段 worker 尚未 clone Arc → get_mut 唯一引用可行（此后 CF 内部 &self 维护）
        let mut primary = primary;
        Arc::get_mut(&mut primary)
            .unwrap()
            .set_external_seq(Arc::clone(&global_seq));
        let mut delta = delta;
        Arc::get_mut(&mut delta)
            .unwrap()
            .set_external_seq(Arc::clone(&global_seq));
        let hotcache = HotCache::new(cfg.hotcache.clone());
        let watchdog = Watchdog::new(cfg, query_timeout);
        let mut engine = Self {
            primary,
            cidx,
            inverted: Arc::new(inverted),
            delta,
            hotcache,
            watchdog,
            mem_ratio: 0.0,
            max_memory_mb: cfg.hotcache.max_memory_mb + cfg.blockcache.max_memory_mb,
            global_seq,
            group_commit: None,
            gc_stop: None,
            gc_thread: None,
            inverted_include: if cfg.inverted.inverted_fields.is_empty() {
                None
            } else {
                Some(cfg.inverted.inverted_fields.iter().cloned().collect())
            },
            inverted_exclude: cfg.inverted.exclude_fields.iter().cloned().collect(),
            max_term_len: cfg.inverted.max_term_len,
            fulltext_fields: cfg.inverted.fulltext_fields.iter().cloned().collect(),
            use_jieba: cfg!(feature = "cjk-jieba") && cfg.inverted.cjk_segmenter == "jieba",
            stats_fields: cfg.inverted.stats_fields.clone(),
            // 写入 Enrich（design 19 / development 5.21）：`[enrich] enabled && source=local` 启用
            enrich: if cfg.enrich.enabled && cfg.enrich.source == "local" {
                Some((
                    cfg.enrich.fail_policy.clone(),
                    cfg.enrich.from_field.clone(),
                    cfg.enrich.to_field.clone(),
                ))
            } else {
                None
            },
            skip_hotcache: false,
            pending_inverted: Mutex::new(Vec::new()),
            compaction_parallel: cfg.storage.compaction_parallel,
            auto_compact: cfg.storage.auto_compact,
            compact_pending: Arc::new(AtomicBool::new(false)),
            compact_worker: Arc::new(AtomicBool::new(false)),
            inverted_gc_pending: Arc::new(AtomicBool::new(false)),
            deletion_bitmap,
            garbage_marked: Arc::new(AtomicU64::new(bm_deleted)),
            garbage_done: Arc::new(AtomicU64::new(bm_deleted)),
            garbage_draining: Arc::new(AtomicBool::new(false)),
            max_docid: AtomicU64::new(0),
            max_docid_loaded: AtomicBool::new(false),
            dd_min_ratio: cfg.storage.delete_density_min_ratio,
            dd_min_docs: cfg.storage.delete_density_min_docs,
            affinity: crate::affinity::plan_partition(&cfg.affinity),
            io_rate_base_bytes: cfg.storage.io_rate_limit_mb * 1024 * 1024,
            memtable_max_bytes: cfg.memtable.max_size_mb * 1024 * 1024,
            outbox,
            txn_locks: Mutex::new(crate::txn::LockTable::new()),
            data_dir: data_dir.to_path_buf(),
            #[cfg(target_os = "linux")]
            iou,
            metrics: crate::metrics::Metrics::default(),
            shard_metrics: std::sync::Mutex::new(None),
        };
        // 组提交（M8）：`storage.group_commit_us > 0` 时开启——窗口内写入攒批一次 fsync，
        // 后台线程兜底窗口尾部落盘；默认 0 = 关闭（保持逐条 fsync 强安全）。
        engine.start_group_commit(cfg);
        Ok(engine)
    }

    /// 启动组提交（M8）：窗口 + 字节阈值触发；spawn 后台线程兜底窗口尾部落盘。
    /// 关闭（`group_commit_us == 0`）时无任何开销（保持逐条 fsync 强安全语义）。
    fn start_group_commit(&mut self, cfg: &Config) {
        let window_us = cfg.storage.group_commit_us;
        if window_us == 0 {
            return;
        }
        let window = Duration::from_micros(window_us);
        let bytes = cfg.storage.group_commit_bytes.max(1);
        self.group_commit = Some((window, bytes));

        // 后台兜底线程：每 ≤ 窗口唤醒一次，有待刷缓冲且窗口到期则 fsync（覆盖窗口尾部）。
        // 仅触碰共享 WAL 锁，不访问 Engine 本体（避免自引用/锁顺序问题）。
        let pwal = self.primary.wal_handle();
        let dwal = self.delta.wal_handle();
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);
        let tick = if window < Duration::from_millis(10) {
            window
        } else {
            Duration::from_millis(10)
        };
        self.gc_stop = Some(stop);
        let io_cores = self.affinity.io.clone(); // Ex-7.2：IO 后台线程绑 io 核
        self.gc_thread = Some(std::thread::spawn(move || {
            crate::affinity::bind_current(&io_cores); // 失败仅忽略（no-op）
            loop {
            std::thread::sleep(tick);
            if stop2.load(Ordering::Relaxed) {
                break;
            }
            let now = std::time::Instant::now();
            for w in [&pwal, &dwal] {
                let _ = w.lock().map(|mut g| {
                    if g.pending_bytes() > 0 && g.sync_due(now, window, 0) {
                        if let Err(e) = g.sync() {
                            tracing::debug!("组提交兜底落盘失败（等待写路径处理）: {e}");
                        }
                    }
                });
            }
            }
        }));
    }

    /// 组提交判定（M8）：关闭 → 逐条 fsync（现状强安全）；
    /// 开启 → 写路径零 fsync，由后台提交线程按窗口统一落盘（ack 后最多延迟 ≤ 窗口，
    /// 字节阈值触发也由后台线程判定）——避免写路径与后台线程双份 fsync + 锁竞争。
    fn maybe_group_commit(&mut self) -> Result<()> {
        if self.group_commit.is_none() {
            self.flush_wal()?;
        }
        Ok(())
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

    /// 写入文档（docid + 序列化字节 + 该文档涉及的倒排词条）。
    /// 写失效链：先失效 HotCache 与组合索引旧条目，最后写 LSM（design 6.6）。
    /// OOM Guardian：写入前按水位限流/熔断（design 14.1.1）。
    /// X 项：写操作计数 + 延迟直方图。
    pub fn put(&mut self, docid: u64, value: Vec<u8>, terms: &[&str]) -> Result<()> {
        let t = std::time::Instant::now();
        // 看门狗统一检查（P52）：内存硬水位熔断 + 磁盘剩余空间熔断；软水位放行记录
        self.watchdog.check_all(self.mem_ratio, &self.data_dir)?;
        self.put_nosync(docid, value, terms)?;
        // 组提交（M8）：开启时窗口内攒批一次 fsync，否则逐条 fsync（强安全）
        self.maybe_group_commit()?;
        // Ex-7.4：按前台写压力动态调整 Compaction 限速（MemTable 水位让路）
        self.adjust_compaction_io_rate();
        self.metrics.write_ops.fetch_add(1, Ordering::Relaxed);
        self.metrics.record_latency(t.elapsed().as_nanos() as u64);
        Ok(())
    }

    /// Ex-7.4：动态限流——按主数据 MemTable 水位（前台写压力代理）下调 Compaction 限速：
    /// 压力 p → 限速 = base × (1 - 0.5p)——压力 0 全速追赶 L0 合并，压力 1 让路 50%
    /// 磁盘带宽给前台写（design_extension 12.6：写压力高时压缩 Compaction 带宽）。
    /// L 项：同源压力同步给各列族 set_write_pressure（动态 L0 阈值反馈），独立于限速配置。
    fn adjust_compaction_io_rate(&mut self) {
        let used = self.primary.memtable_bytes() as f64;
        let max = self.memtable_max_bytes.max(1) as f64;
        let pressure = (used / max).clamp(0.0, 1.0);
        // L 项：写压力 → 各列族动态 L0 阈值（高峰收窄提前收敛）
        self.primary.set_write_pressure(pressure);
        self.delta.set_write_pressure(pressure);
        if let Some(c) = &self.cidx {
            c.set_write_pressure(pressure);
        }
        if self.io_rate_base_bytes == 0 {
            return; // 未配置限速（io_rate_limit_mb = 0）
        }
        let rate = (self.io_rate_base_bytes as f64 * (1.0 - 0.5 * pressure)) as u64;
        self.primary.set_io_rate_bytes(rate);
        self.delta.set_io_rate_bytes(rate);
        if let Some(c) = &self.cidx {
            c.set_io_rate_bytes(rate);
        }
        // Ex-8.13：倒排 GC/后台段写与列族压缩同口径收窄（共享后台 IO 预算）
        self.inverted.set_io_rate_bytes(rate);
    }

    /// 批量写入（不逐条 fsync，供亿级压测；结束时调用 `flush_wal` 统一提交）。
    pub fn put_nosync(&mut self, docid: u64, value: Vec<u8>, terms: &[&str]) -> Result<()> {
        // ① 失效 HotCache 该 docid（批量导入模式跳过：只写不读，避免缓存膨胀挤爆内存，P40）
        if !self.skip_hotcache {
            self.hotcache.invalidate(docid);
        }
        // ② 主数据（权威源，WAL 攒批不逐条 fsync）；全量覆盖 → 清空该 docid 的增量（避免旧 patch 覆盖新数据）
        self.primary
            .put_bytes_nosync(encode_docid(docid).to_vec(), value.clone())?;
        self.delta.delete_prefix(&encode_docid(docid))?;
        // ②.5 删除位图复活（Ex-5.6）：put 覆盖 delete → 清位（O(1) 内存，位未置时零 IO）；
        //     持久性与 WAL 同步（flush_wal 先刷位图后刷 WAL）
        //     Ex-8.7：实际清位（此前已删）→ 减除删除密度净置位数；fetch_max 维护密度分母。
        self.max_docid.fetch_max(docid, Ordering::Relaxed);
        if let Some(bm) = &self.deletion_bitmap {
            if bm.clear(docid) {
                self.garbage_marked.fetch_sub(1, Ordering::Relaxed);
            }
        }
        // ③ 倒排（内存字典累积，Ex-5.3 攒批：term 先入缓冲，达阈值/查询/flush 时批量刷入）；
        //    M8-P4：白名单/黑名单/超长 term 过滤（长文本整串不进字典，防膨胀）
        //    Ex-9.3 第①步：配置 stats_fields 时解析本文档数值并随 allowed term 累积
        let stats = if self.stats_fields.is_empty() {
            Vec::new()
        } else {
            engine_doc_stats(&self.stats_fields, &value)
        };
        for t in terms {
            if self.inverted_allowed(t) {
                self.pending_inverted.lock().unwrap().push((t.to_string(), docid));
                if !stats.is_empty() {
                    self.inverted.add_stats(t, &stats);
                }
            }
        }
        if self.pending_inverted.lock().unwrap().len() >= INVERTED_PENDING_CAP {
            self.flush_inverted_pending();
        }
        // ④ 回填 HotCache（写后回填，供热点查询亚毫秒命中；批量导入模式跳过，P40）
        if !self.skip_hotcache {
            self.hotcache.put(docid, value);
        }
        // P 项：事件驱动自动 Compaction——写后检查（Flush 可能刚新增 L0 段），
        // L0 段数/大小超阈值 → 同步合并收敛（写入自然退避 = 背压）。
        self.auto_compact()?;
        Ok(())
    }

    /// 批量写入（原子批次，用户端批量语义）：一次性提交一组 `(docid, value, terms)`——
    /// put_nosync 攒批 + 一次 `flush_wal` 统一提交（整批落盘或崩溃后按 WAL 批次整体重放，
    /// 无中间态；与组提交正交——显式批次边界，延迟可预期）。
    /// 为 D 项（LSM 事务阶段一 WriteBatch 原子写）的前置基础；单条语义同 `put`。
    pub fn put_batch(&mut self, items: &[(u64, Vec<u8>, Vec<String>)]) -> Result<()> {
        self.watchdog.check_all(self.mem_ratio, &self.data_dir)?;
        for (docid, value, terms) in items {
            let refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            self.put_nosync(*docid, value.clone(), &refs)?;
        }
        self.flush_wal()
    }

    // ===== D/E/F 事务三阶段 =====

    /// D 阶段一：WriteBatch 原子提交。预校验（失败零副作用 = "失败回滚"）→ 逐条应用 →
    /// 单次 `flush_wal`。崩溃原子：WAL 单次 fsync 批次整体重放（整批恢复或整批丢弃，无中间态）。
    /// 等价于 `put_batch` + delete 语义 + 事务上下文（回滚 = 丢弃未应用的批次）。
    pub fn write(&mut self, batch: &crate::txn::WriteBatch) -> Result<()> {
        batch.validate()?;
        self.watchdog.check_all(self.mem_ratio, &self.data_dir)?;
        for op in batch.ops() {
            match op {
                crate::txn::Op::Put {
                    docid,
                    value,
                    terms,
                } => {
                    let refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
                    self.put_nosync(*docid, value.clone(), &refs)?;
                }
                crate::txn::Op::Delete { docid } => self.delete(*docid)?,
            }
        }
        self.flush_wal()
    }

    /// E/F：开启事务。快照 seq = 当前已分配最大全局 seq（RR/SERIALIZABLE 一致读基准）。
    pub fn txn_begin(&mut self, isolation: crate::txn::Isolation) -> crate::txn::Transaction {
        crate::txn::Transaction::new(isolation, self.begin_snapshot())
    }

    /// E/F：事务读。RC = 最新已提交（`get`）；RR/SERIALIZABLE = 快照一致读（`get_at`）；
    /// SERIALIZABLE 读目标额外加共享锁（2PL，读锁持有至提交）。
    /// H-4：先查本事务未提交的写（`read_own`，同事务写后读可见），再走引擎。
    /// O 项第②步：读路径 `&self`（RR/RC 快照只读并行；SERIALIZABLE 读锁经 `txn_locks` 内部 Mutex）。
    pub fn txn_get(
        &self,
        txn: &mut crate::txn::Transaction,
        docid: u64,
    ) -> Result<Option<Vec<u8>>> {
        if txn.is_finished() {
            return Err(crate::error::Error::TxnAborted(format!(
                "txn#{} 已结束",
                txn.id
            )));
        }
        // 同事务写后读可见（未提交的攒批写优先）
        if let Some(own) = txn.read_own(docid) {
            return Ok(own.map(|v| v.to_vec()));
        }
        // T 项：RR/SERIALIZABLE 快照读——事务内点查小缓存（同 key 二次读直达，免 LSM 冷读
        // 放大；快照 seq 事务内恒定 → 缓存结果一致）。命中跳过加锁（首次读已加）。
        if txn.isolation.uses_snapshot() {
            if let Some(v) = txn.snap_get(docid) {
                return Ok(v);
            }
        }
        if txn.isolation.locks_reads() {
            self.txn_locks.lock().unwrap().acquire_shared(txn.id, docid)?;
            txn.add_lock(docid);
        }
        let result = if txn.isolation.uses_snapshot() {
            self.get_at(docid, txn.snapshot())
        } else {
            self.get(docid)
        };
        // 仅快照读写缓存（RC 读最新，缓存会破坏语义）；错误结果不缓存
        if txn.isolation.uses_snapshot() {
            if let Ok(v) = &result {
                txn.snap_put(docid, v.clone());
            }
        }
        result
    }

    /// E/F：事务提交。写锁（write_set 全目标排他，含共享→排他升级）→
    /// 写写冲突检测（RR/SERIALIZABLE：目标在快照后被并发事务修改 → `TxnConflict` abort）→
    /// 应用 ops + 单次 flush_wal → 释放全部锁（失败路径同样释放，防锁泄漏）。
    pub fn txn_commit(&mut self, mut txn: crate::txn::Transaction) -> Result<()> {
        if txn.is_finished() {
            return Err(crate::error::Error::TxnAborted(format!(
                "txn#{} 已结束",
                txn.id
            )));
        }
        let result = (|| -> Result<()> {
            txn.validate()?;
            // P52：提交前统一看门狗检查（内存/磁盘熔断）
            self.watchdog.check_all(self.mem_ratio, &self.data_dir)?;
            // ① 写锁（SERIALIZABLE 已持共享锁的 docid 自动升级为排他）
            let targets: Vec<u64> = txn.write_set().iter().copied().collect();
            for d in targets {
                self.txn_locks
                    .lock()
                    .unwrap()
                    .acquire_exclusive(txn.id, d)?;
                txn.add_lock(d);
            }
            // ② 写写冲突检测（RR/SERIALIZABLE）
            // P1-4：FOR UPDATE **当前读**已显式读到最新版本的行允许写入（MySQL 语义）——
            // 该键被当前读锁定且最新 seq 仍等于读取时记录值（期间无并发写）→ 放行；
            // 被并发事务再次修改（seq 前进）→ 仍冲突（乐观锁正确性，不覆盖并发新值）。
            if txn.isolation.checks_write_conflict() {
                for &d in txn.write_set() {
                    let cur = self.last_write_seq(d)?;
                    if cur > txn.snapshot() {
                        let locked = txn.cur_lock_seq(d);
                        if let Some(seen) = locked {
                            if cur == seen {
                                continue;
                            }
                        }
                        return Err(crate::error::Error::TxnConflict(format!(
                            "txn#{} 写冲突：docid={d} 在快照 {} 后被并发事务修改（当前 seq {cur}）",
                            txn.id,
                            txn.snapshot()
                        )));
                    }
                }
            }
            // ③ 应用 + 单次 flush_wal（崩溃原子）
            for op in txn.ops() {
                match op {
                    crate::txn::Op::Put {
                        docid,
                        value,
                        terms,
                    } => {
                        let refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
                        self.put_nosync(*docid, value.clone(), &refs)?;
                    }
                    crate::txn::Op::Delete { docid } => self.delete(*docid)?,
                }
            }
            self.flush_wal()?;
            Ok(())
        })();
        if result.is_ok() {
            txn.mark_finished();
        }
        self.txn_locks.lock().unwrap().release(txn.id);
        result
    }

    /// E/F：事务回滚（丢弃攒批 + 释放锁，引擎零变更）。
    pub fn txn_rollback(&mut self, mut txn: crate::txn::Transaction) {
        if txn.is_finished() {
            return;
        }
        txn.mark_finished();
        self.txn_locks.lock().unwrap().release(txn.id);
    }

    /// docid 当前最新提交版本 seq（删除位图已删 → 返回 current_seq 视为已删的"最新"）。
    /// pub：FOR UPDATE 当前读需记录锁定版本（P1-4）。
    pub fn last_write_seq(&self, docid: u64) -> Result<u64> {
        if let Some(bm) = &self.deletion_bitmap {
            if bm.is_deleted(docid) {
                return Ok(self.current_seq());
            }
        }
        match self.primary.get_bytes(&encode_docid(docid))? {
            Some((_, seq)) => Ok(seq),
            None => Ok(0),
        }
    }

    /// 倒排 term 过滤（M8-P4）：白名单（只建声明字段）→ 黑名单（排除字段）→ 超长 term 自动跳过。
    /// term 编码 `field=value`，field 为 JSON 字段路径（嵌套用 `.` 连接）。
    /// fulltext 词 term（`ft:{field}:{token}`）与 inverted_fields 白名单正交：是否建索引
    /// 由 fulltext_fields 声明决定（白名单非空时 ft: term 不被滤掉，否则无法分词检索）。
    fn inverted_allowed(&self, term: &str) -> bool {
        // 超长 term（长文本整串）自动跳过：防止误配下字典膨胀
        if self.max_term_len > 0 && term.len() > self.max_term_len {
            return false;
        }
        if let Some(rest) = term.strip_prefix("ft:") {
            let field = rest.split(':').next().unwrap_or("");
            return self.fulltext_fields.contains(field);
        }
        let field = term.split('=').next().unwrap_or("");
        if let Some(include) = &self.inverted_include {
            return include.contains(field);
        }
        !self.inverted_exclude.contains(field)
    }

    /// Ex-9.1：mysql `COUNT WHERE f='v'` 快路径可路由判定——字段须已建索引且计数是亚毫秒级：
    /// ① `bitmap_fields` 内存位图（写路径同步维护，O(1) 精确）或 ② 倒排白名单字段（回退精确
    /// 去重 doc_count——大 term 非亚毫秒，故建议 COUNT 高频字段配 bitmap_fields）。未索引字段
    /// 不得路由（防 `doc_count` 把"未建索引"误报成 0）。
    pub fn inverted_count_eligible(&self, field: &str) -> bool {
        !field.is_empty()
            && (self.inverted_allowed(&format!("{field}=x"))
                || self.inverted.is_bitmap_field(field))
    }

    /// 统一提交 WAL（批量写入结束后调用，保证崩溃可恢复）。
    /// Ex-5.6：删除位图脏页**先于** WAL fsync 落盘——若崩溃发生在 WAL fsync 之后、
    /// 环形 WAL 截断推进之前，位图已持久（删除不丢）；反之位图先持久、WAL 回放重删幂等。
    pub fn flush_wal(&mut self) -> Result<()> {
        if let Some(bm) = &self.deletion_bitmap {
            bm.flush()?;
        }
        self.primary.sync_wal()?;
        self.delta.sync_wal()?;
        // Ex-1：outbox 消息与业务写同 fsync 点（本地原子：崩溃恢复按 seq 回放）
        if let Some(ob) = &mut self.outbox {
            ob.sync_wal()?;
        }
        Ok(())
    }

    /// 强制刷盘主数据 MemTable → SST（测试 / 备份一致性准备用）。
    pub fn flush_primary(&mut self) -> Result<()> {
        self.primary.switch_and_flush()
    }

    /// DROP TABLE / TRUNCATE（文档库唯一表统一映射 documents）purge：清空引擎全部数据并对齐
    /// MySQL 整表删除语义——主数据/组合索引/Delta 三列族（MemTable+SST+WAL）、倒排（内存+段）、
    /// 删除位图、HotCache、倒排攒批缓冲与全局 seq/删除密度状态全部归零；数据目录清空
    /// （此后重启打开为空库，反复 --init / cleanup 基线可比）。
    /// 须在引擎写锁（`&mut self`）内调用：与 flush/写路径互斥；后台 compact / inverted gc 并发
    /// 安全（列族 `sst_mutate` / 倒排 `mutate` 互斥）。outbox 业务消息表不受影响（独立于表数据）。
    pub fn purge_all(&mut self) -> Result<()> {
        info!("引擎整库 purge 开始（DROP TABLE / TRUNCATE TABLE）");
        self.primary.purge_data()?;
        if let Some(c) = &self.cidx {
            c.purge_data()?;
        }
        self.delta.purge_data()?;
        self.inverted.purge_all()?;
        self.hotcache.clear();
        if let Some(bm) = &self.deletion_bitmap {
            bm.purge();
        }
        self.pending_inverted.lock().unwrap().clear();
        self.global_seq.store(0, Ordering::Relaxed);
        self.max_docid.store(0, Ordering::Relaxed);
        self.max_docid_loaded.store(false, Ordering::Relaxed);
        self.garbage_marked.store(0, Ordering::Relaxed);
        self.garbage_done.store(0, Ordering::Relaxed);
        self.garbage_draining.store(false, Ordering::Relaxed);
        self.compact_pending.store(false, Ordering::Relaxed);
        self.inverted_gc_pending.store(false, Ordering::Relaxed);
        info!("引擎整库 purge 完成");
        Ok(())
    }

    /// 当前已分配的最大 seq（全量备份点 / 增量备份游标基础）。
    pub fn current_seq(&self) -> u64 {
        self.global_seq.load(Ordering::Relaxed).saturating_sub(1)
    }

    /// 增量备份（design 20，M6-5）：导出 seq ∈ (since_seq, 当前] 的 WAL 记录为 JSON 文件。
    /// 若 WAL 已被截断（环形覆盖 / 长时间未备份）导致缺口 → 报错提示改做全量备份。
    pub fn backup_incremental(&mut self, since_seq: u64, out_path: &Path) -> Result<BackupReport> {
        // 组提交（M8）前置落盘：保证导出的 WAL 记录已持久化（否则崩溃恢复可能丢失 → 备份与恢复不一致）
        self.flush_wal()?;
        let until_seq = self.current_seq();
        let (oldest, records) = self.primary.wal_records_since(since_seq)?;
        if since_seq != 0 && oldest > since_seq + 1 {
            return Err(crate::error::Error::Unsupported(format!(
                "增量备份缺口：可用 WAL 最旧 seq {oldest} > 上次备份点 {since_seq}+1，请先做全量备份"
            )));
        }
        let file = IncrementalBackupFile {
            since_seq,
            until_seq,
            records,
        };
        let count = file.records.len();
        let text = serde_json::to_string_pretty(&file)
            .map_err(|e| crate::error::Error::Serialize(format!("增量备份序列化失败: {e}")))?;
        let tmp = out_path.with_extension("tmp");
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, out_path)?; // 原子落盘（tmp+rename）
        Ok(BackupReport {
            since_seq,
            until_seq,
            records: count,
        })
    }

    /// 增量恢复：将增量记录按序重放到已还原的引擎（PUT 重新派生倒排词条；DELETE 写墓碑）。
    /// 返回应用记录数。恢复后调用方应再做一次全量备份以合并游标。
    pub fn restore_incremental(&mut self, path: &Path) -> Result<usize> {
        let text = std::fs::read_to_string(path)?;
        let file: IncrementalBackupFile = serde_json::from_str(&text)
            .map_err(|e| crate::error::Error::Corrupted(format!("增量备份文件解析失败: {e}")))?;
        let mut applied = 0usize;
        for r in &file.records {
            let docid = decode_docid(&r.key)
                .map_err(|_| crate::error::Error::Corrupted("增量记录 key 非 docid 编码".into()))?;
            match r.op {
                crate::wal::OP_PUT => {
                    let value = r.value.clone().unwrap_or_default();
                    let terms = match serde_json::from_slice::<serde_json::Value>(&value) {
                        Ok(v) => crate::server::extract_terms(&v),
                        Err(_) => Vec::new(), // 非 JSON 原始字节文档：无倒排词条
                    };
                    let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
                    self.put(docid, value, &t)?;
                }
                crate::wal::OP_DELETE => {
                    self.delete(docid)?;
                }
                other => {
                    return Err(crate::error::Error::Corrupted(format!(
                        "增量记录未知 op {other}"
                    )))
                }
            }
            applied += 1;
        }
        Ok(applied)
    }

    /// 删除文档：失效 HotCache + 清空 Delta（倒排残留 docid 由回表过滤）。
    /// Ex-5.6：删除位图开启时写 1bit（O(1) 最新态隐藏 + compaction 物理回收）+ **memtable
    /// Tombstone（版本化，缺陷 B/C4 修复：复活清位后快照读仍能判定删除区间）** + WAL 删除记录
    /// （供增量备份/崩溃回放）。墓碑进入版本链，快照读按 seq 过滤（快照点在删除与复活之间
    /// → 不可见），与 MySQL RR 一致。关闭位图时回退传统 Tombstone 路径。
    pub fn delete(&mut self, docid: u64) -> Result<()> {
        self.watchdog.check_all(self.mem_ratio, &self.data_dir)?;
        self.hotcache.invalidate(docid);
        match &self.deletion_bitmap {
            Some(bm) => {
                // Ex-8.7：**新置位**（此前未删）才计入删除密度净置位数——
                // WAL 回放/重复删除幂等（位已置 → 不计，与 bm_deleted 初始化口径一致）。
                if bm.mark_deleted(docid) {
                    self.garbage_marked.fetch_add(1, Ordering::Relaxed);
                }
                self.primary
                    .delete_record_mem(encode_docid(docid).to_vec())?;
                self.delta.delete_prefix(&encode_docid(docid))?;
            }
            None => {
                self.primary.delete(docid)?;
                self.delta.delete_prefix(&encode_docid(docid))?;
            }
        }
        Ok(())
    }

    /// M3（§26 多表，实施清单④）：DROP TABLE 磁盘文件级回收——物理删除主列族内
    /// **完全落在指定表 docid 区间**的 SST（表切分后每文件单表，可整文件删）。
    /// 须在 `multitable::drop_table_range`（逐 docid 逻辑删除：墓碑已覆盖全部该表键）之后调用，
    /// 此时删文件不改变可见性（墓碑保证无复活），仅提前释放磁盘。
    pub fn drop_table_sst_files(&self, tid: u16) -> crate::error::Result<usize> {
        self.primary.drop_table_range_files(tid)
    }

    /// 部分更新（阶段 1.5，design 4.7）：仅写入变更字段到 Delta CF（几十字节小记录），
    /// 读取时 Merge-on-Read 覆盖 Base；`null` 值表示删除该字段。替代全量 PUT，写入 IO 放大趋近 1。
    pub fn patch(&mut self, docid: u64, fields: &[(&str, serde_json::Value)]) -> Result<()> {
        self.hotcache.invalidate(docid);
        for (f, v) in fields {
            let mut key = encode_docid(docid).to_vec();
            encode_varlen(&mut key, f.as_bytes());
            let val =
                serde_json::to_vec(v).map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
            self.delta.put_bytes_nosync(key, val)?;
        }
        self.maybe_group_commit()
    }

    /// 点查文档：HotCache 命中直达，否则主数据 LSM + Delta Merge-on-Read。
    /// Ex-5.6：删除位图开启时先 O(1) 判定，已删文档直接返回 None（零 LSM 读）。
    /// O 项第②步：读路径 `&self`（HotCache 内部 Mutex）。
    /// X 项：读操作计数 + 延迟直方图（完整路径）。
    pub fn get(&self, docid: u64) -> Result<Option<Vec<u8>>> {
        self.metrics.read_ops.fetch_add(1, Ordering::Relaxed);
        let t = std::time::Instant::now();
        if let Some(bm) = &self.deletion_bitmap {
            if bm.is_deleted(docid) {
                return Ok(None);
            }
        }
        if let Some(v) = self.hotcache.get(docid) {
            return Ok(Some(v));
        }
        let found = self.primary.get(docid)?;
        let Some((bv, _)) = found else {
            return Ok(None);
        };
        // Delta 覆盖（对象合并；null 删除字段）；非 JSON / 非对象文档直接返回 Base（raw 字节场景）
        let obj: serde_json::Value = match serde_json::from_slice(&bv) {
            Ok(v) => v,
            Err(_) => return Ok(Some(bv)),
        };
        let mut map = match obj {
            serde_json::Value::Object(m) => m,
            _ => return Ok(Some(bv)),
        };
        let start = encode_docid(docid).to_vec();
        let mut end = start.clone();
        end.extend_from_slice(&[0xFF; 4]);
        let rows = self.delta.scan_raw_range(Some(&start), Some(&end))?;
        for (k, v) in rows {
            if !k.starts_with(&start) || k.len() < 12 {
                continue;
            }
            let field = String::from_utf8(k[12..].to_vec())
                .map_err(|_| crate::error::Error::Corrupted("Delta 字段名非法 UTF-8".into()))?;
            let val: serde_json::Value = serde_json::from_slice(&v)
                .map_err(|e| crate::error::Error::Corrupted(format!("Delta 值解析失败: {e}")))?;
            if val.is_null() {
                map.shift_remove(&field);
            } else {
                map.insert(field, val);
            }
        }
        let merged =
            serde_json::to_vec(&map).map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
        self.hotcache.put(docid, merged.clone());
        self.metrics.record_latency(t.elapsed().as_nanos() as u64);
        Ok(Some(merged))
    }

    /// 批量回表（N 项：借鉴 batch_get 建议落地；倒排/全文检索 posting 回表路径）。
    /// 语义与 `get` 一致（删除位图 / HotCache / Delta 字段覆盖），但一次处理多个 docid：
    /// ① 删除位图 O(1) 批量过滤；② HotCache 批量命中；③ primary `get_many` 批量读
    /// （SST 层按块分组，同块多 key 只读/解压一次，块缓存复用）；④ Delta 覆盖用**单次
    /// 范围扫描** [min..max] 按 docid 分组，替代逐 docid 扫描。
    /// 输入要求：docids 升序且无重复（倒排 bitmap 迭代天然满足）。
    /// 返回与输入顺序对齐的 `Vec<Option<value>>`。
    /// O 项第②步：读路径 `&self`（HotCache 内部 RwLock 读读并行 + DashMap 无锁计数）。
    pub fn batch_get(&self, docids: &[u64]) -> Result<Vec<Option<Vec<u8>>>> {
        let n = docids.len();
        let mut out: Vec<Option<Vec<u8>>> = vec![None; n];
        if n == 0 {
            return Ok(out);
        }
        // ① 删除位图 + ② HotCache
        let mut need_primary: Vec<usize> = Vec::new();
        for (i, &d) in docids.iter().enumerate() {
            if let Some(bm) = &self.deletion_bitmap {
                if bm.is_deleted(d) {
                    continue;
                }
            }
            if let Some(v) = self.hotcache.get(d) {
                out[i] = Some(v);
            } else {
                need_primary.push(i);
            }
        }
        if need_primary.is_empty() {
            return Ok(out);
        }
        let sub: Vec<u64> = need_primary.iter().map(|&i| docids[i]).collect();
        let found = self.primary.get_many(&sub)?; // Vec<Option<(value, seq)>>
        // ④ Delta 批量覆盖（单次范围扫描，按 docid 分组）
        let overrides = batch_delta_overrides(&self.delta, &sub)?;
        for (j, &i) in need_primary.iter().enumerate() {
            let d = docids[i];
            let Some((bv, _seq)) = &found[j] else {
                continue;
            };
            let obj: serde_json::Value = match serde_json::from_slice(bv) {
                Ok(v) => v,
                Err(_) => {
                    // 非 JSON 原始字节文档：无 Delta 覆盖
                    out[i] = Some(bv.clone());
                    continue;
                }
            };
            let mut map = match obj {
                serde_json::Value::Object(m) => m,
                _ => {
                    out[i] = Some(bv.clone());
                    continue;
                }
            };
            if let Some(over) = overrides.get(&d) {
                for (field, val) in over {
                    if val.is_null() {
                        map.shift_remove(field);
                    } else {
                        map.insert(field.clone(), val.clone());
                    }
                }
            }
            let merged = serde_json::to_vec(&map)
                .map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
            self.hotcache.put(d, merged.clone());
            out[i] = Some(merged);
        }
        Ok(out)
    }

    /// 获取当前快照点（已分配的最大 seq）：此后以该值为快照的 `get_at` 读到一致视图。
    pub fn begin_snapshot(&self) -> u64 {
        self.primary.wal_next_seq().saturating_sub(1)
    }

    /// 快照读（design 4.7 MVCC）：返回 **seq ≤ `snapshot_seq`** 的文档视图。
    /// 主数据取快照点最新版本（MemTable + SST 按 seq 过滤，Tombstone 语义保留）；
    /// **Delta 增量按全局 seq 过滤**（M7-1：跨列族共享 seq 分配，快照后的字段级热更不可见；
    /// null 删除字段 / Tombstone 均按快照点判定）。
    /// 不走 HotCache（避免快照读污染热缓存）。
    /// Ex-5.6：删除位图开启时已删文档在任何快照点均不可见（位图删除为立即/全局语义，
    /// 快照隔离仅保证更新可见性；位图置位后该 docid 视为物理删除）。
    /// O 项第②步：读路径 `&self`。
    /// X 项：读操作计数（事务内点查）。
    pub fn get_at(&self, docid: u64, snapshot_seq: u64) -> Result<Option<Vec<u8>>> {
        self.metrics.read_ops.fetch_add(1, Ordering::Relaxed);
        if let Some(bm) = &self.deletion_bitmap {
            if bm.is_deleted(docid) {
                return Ok(None);
            }
        }
        let found = self
            .primary
            .get_bytes_at(&encode_docid(docid), snapshot_seq)?;
        let Some((bv, _)) = found else {
            return Ok(None);
        };
        let obj: serde_json::Value = match serde_json::from_slice(&bv) {
            Ok(v) => v,
            Err(_) => return Ok(Some(bv)),
        };
        let mut map = match obj {
            serde_json::Value::Object(m) => m,
            _ => return Ok(Some(bv)),
        };
        let start = encode_docid(docid).to_vec();
        let mut end = start.clone();
        end.extend_from_slice(&[0xFF; 4]);
        let rows = self
            .delta
            .scan_raw_range_with_seq(Some(&start), Some(&end))?;
        for (k, seq, v) in rows {
            if seq > snapshot_seq {
                continue; // 快照点之后的增量不可见
            }
            if !k.starts_with(&start) || k.len() < 12 {
                continue;
            }
            let field = String::from_utf8(k[12..].to_vec())
                .map_err(|_| crate::error::Error::Corrupted("Delta 字段名非法 UTF-8".into()))?;
            match v {
                Some(bytes) => {
                    let val: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
                        crate::error::Error::Corrupted(format!("Delta 值解析失败: {e}"))
                    })?;
                    if val.is_null() {
                        map.shift_remove(&field);
                    } else {
                        map.insert(field, val);
                    }
                }
                None => {
                    map.shift_remove(&field); // 增量删除字段（Tombstone）
                }
            }
        }
        let merged =
            serde_json::to_vec(&map).map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
        Ok(Some(merged))
    }

    /// 倒排词条查询：合并 posting（RoaringBitmap）→ 回表取文档。
    pub fn search_term(&mut self, term: &str) -> Result<Vec<QueryRow>> {
        Ok(self.search_term_paged(term, None, 0)?.rows)
    }

    /// 倒排词条分页查询（M8-P8）：bitmap 迭代 docid 天然升序，skip(offset) 后只回表 limit 行。
    /// `total` = 全量命中数（bitmap.len() O(1)）；limit=None 取全部（兼容非分页调用）。
    pub fn search_term_paged(
        &mut self,
        term: &str,
        limit: Option<u64>,
        offset: u64,
    ) -> Result<PagedRows> {
        // Ex-5.3：查询前刷入攒批缓冲，保证 put 后未达阈值的数据立即可查（一致性）
        self.flush_inverted_pending();
        // K 项（7.74）：v3 分块快速路径——只解码 [offset, offset+limit) 覆盖的容器
        // （大 posting 近页从全量反序列化降至窗口解码，x211）；total 来自容器头基数。
        let (total, ids) = self
            .inverted
            .search_paged(term, offset, limit.unwrap_or(u64::MAX))?;
        let mut rows = Vec::new();
        // N 项：收集可见窗口 docid（bitmap 升序）→ 一次 `batch_get` 批量回表。
        let vals = self.batch_get(&ids.iter().map(|&d| d as u64).collect::<Vec<_>>())?;
        for (d, v) in ids.into_iter().zip(vals) {
            if let Some(v) = v {
                rows.push((d as u64, v));
            }
        }
        Ok(PagedRows { total, rows })
    }

    /// fulltext 分词检索（M8-P7）：按字段 + 关键词构造词 term `ft:{field}:{word}` 查询
    /// （词 term 由 fulltext_fields 声明字段分词生成）。命中 posting 合并 → 回表取文档。
    pub fn fulltext_search(&mut self, field: &str, word: &str) -> Result<Vec<QueryRow>> {
        self.fulltext_search_paged(field, word, None, 0).map(|p| p.rows)
    }

    /// fulltext 分词检索分页（M8-P8）：同 `search_term_paged` 语义（构造 `ft:{field}:{word}`）。
    pub fn fulltext_search_paged(
        &mut self,
        field: &str,
        word: &str,
        limit: Option<u64>,
        offset: u64,
    ) -> Result<PagedRows> {
        self.search_term_paged(&format!("ft:{field}:{word}"), limit, offset)
    }

    /// fulltext 分词字段集合（M8-P7）：供 term 提取层判断字段是否走分词索引。
    pub fn fulltext_fields(&self) -> &std::collections::HashSet<String> {
        &self.fulltext_fields
    }

    /// 中文分词器开关（M8-P13）：true = jieba 完整词典分词，false = bigram。
    pub fn use_jieba(&self) -> bool {
        self.use_jieba
    }

    /// Ex-9.3：读取某 term 的数值统计（内存累积 + v5 段载荷；与 `stats_fields` 对齐）。
    /// 未配置 stats_fields / 无该 term → None。
    pub fn inverted_term_stats(&self, term: &str) -> Option<Vec<crate::inverted::FieldAgg>> {
        self.inverted.term_stats(term).ok().flatten()
    }

    /// Ex-9.3：字段是否声明为倒排统计载荷字段（返回其在 stats_fields 中的位序，
    /// 用于定位 term 统计的对应聚合列）。
    pub fn stats_field_pos(&self, field: &str) -> Option<usize> {
        self.stats_fields.iter().position(|x| x == field)
    }

    /// Ex-9.3 第④步：倒排词典枚举 `GROUP BY <field>` 聚合行（值, 组行数, 数值统计）。
    /// 仅含已索引的组值；缺字段文档（NULL 组）由调用方按语义约束处理。
    pub fn inverted_group_stats(
        &self,
        field: &str,
    ) -> crate::error::Result<Vec<(String, u64, Vec<crate::inverted::FieldAgg>)>> {
        self.inverted.group_stats(field)
    }

    /// 主键范围扫描分页（M8-P8 + M8-P10 流式化）：k-way merge 流式扫描——内存 O(page)
    /// 不随扫描总量膨胀（旧实现先全量收集 O(total) 再截断）；`total` = 范围行数
    /// （全扫计数，limit 取满页后仅计数不回表，语义与全量一致）。
    pub fn scan_range_paged(
        &mut self,
        start: Option<u64>,
        end: Option<u64>,
        limit: Option<u64>,
        offset: u64,
    ) -> Result<PagedRows> {
        let cap = limit.unwrap_or(u64::MAX);
        let sk = start.map(|s| crate::keys::encode_docid(s).to_vec());
        let ek = end.map(|e| crate::keys::encode_docid(e).to_vec());
        let mut rows = Vec::new();
        let mut skipped = 0u64;
        let mut total = 0u64;
        self.primary
            .scan_stream(sk.as_deref(), ek.as_deref(), |key, val| {
                total += 1;
                if skipped < offset {
                    skipped += 1;
                    return Ok(true);
                }
                if rows.len() as u64 >= cap {
                    return Ok(true); // 页已取满：仅继续计数 total，不再收集
                }
                let docid = crate::keys::decode_docid(key).map_err(|_| {
                    crate::error::Error::Corrupted("scan 流式 key 非 docid 编码".into())
                })?;
                rows.push((docid, val.to_vec()));
                Ok(true)
            })?;
        Ok(PagedRows { total, rows })
    }

    /// scan 游标续扫（M8-P11）：从 `after`（上次最后 docid，None=从头）之后取 `limit` 条，
    /// **取满即提前终止**（不做 total 全扫）——全库遍历每页 O(limit) + 游标定位，
    /// 避免 offset 翻页的累积跳过与 total 全扫开销（7.18 已知限制）。
    /// 语义：docid 升序、不含 after 本身；配合 end 上界可限定范围。
    pub fn scan_after(
        &mut self,
        after: Option<u64>,
        end: Option<u64>,
        limit: u64,
    ) -> Result<Vec<QueryRow>> {
        let start = after.map(|a| encode_docid(a.saturating_add(1)).to_vec());
        let ek = end.map(|e| encode_docid(e).to_vec());
        let mut rows = Vec::new();
        self.primary
            .scan_stream(start.as_deref(), ek.as_deref(), |key, val| {
                if rows.len() as u64 >= limit {
                    return Ok(false); // 取满页：提前终止（不再扫后续）
                }
                let docid = crate::keys::decode_docid(key).map_err(|_| {
                    crate::error::Error::Corrupted("scan 流式 key 非 docid 编码".into())
                })?;
                rows.push((docid, val.to_vec()));
                Ok(true)
            })?;
        Ok(rows)
    }

    /// 主键范围扫描。
    pub fn scan_range(&self, start: Option<u64>, end: Option<u64>) -> Result<Vec<QueryRow>> {
        let mut rows = self.primary.scan_range(start, end)?;
        // Ex-8.1：删除位图语义对齐（get 不可见 → scan 也不返回已删 docid）
        if let Some(bm) = &self.deletion_bitmap {
            rows.retain(|(d, _)| !bm.is_deleted(*d));
        }
        Ok(rows)
    }

    /// 流式主键范围扫描（design 20.5 导出管道）：回调按 docid 升序收到 `(docid, value)`；
    /// 返回 `false` 提前终止（取满批/游标续扫）。内存 O(批)，不随扫描总量膨胀。
    pub fn scan_stream<F: FnMut(u64, &[u8]) -> Result<bool>>(
        &self,
        start: Option<u64>,
        end: Option<u64>,
        mut f: F,
    ) -> Result<()> {
        let sk = start.map(|s| encode_docid(s).to_vec());
        let ek = end.map(|e| encode_docid(e).to_vec());
        self.primary.scan_stream(sk.as_deref(), ek.as_deref(), |key, val| {
            let docid = decode_docid(key).map_err(|_| {
                crate::error::Error::Corrupted("scan 流式 key 非 docid 编码".into())
            })?;
            // Ex-8.1：删除位图语义对齐（与 get/scan_range 一致，已删 docid 跳过）
            if let Some(bm) = &self.deletion_bitmap {
                if bm.is_deleted(docid) {
                    return Ok(true);
                }
            }
            f(docid, val)
        })
    }

    /// Ex-8.3 Part B：keys-only 流式 id 扫描（最新视图，免整文档值解码）——纯 `SELECT id` /
    /// COUNT 类只关心 docid 存在性的路径；merge 版本折叠 + Tombstone 跳过 + 删除位图过滤，
    /// 回调返回 false 提前终止。语义与 `scan_stream` 输出 docid 集一致。
    pub fn scan_stream_ids<F: FnMut(u64) -> Result<bool>>(
        &self,
        start: Option<u64>,
        end: Option<u64>,
        mut f: F,
    ) -> Result<()> {
        let sk = start.map(|s| encode_docid(s).to_vec());
        let ek = end.map(|e| encode_docid(e).to_vec());
        self.primary
            .scan_stream_keys(sk.as_deref(), ek.as_deref(), |key| {
                let docid = decode_docid(key).map_err(|_| {
                    crate::error::Error::Corrupted("scan keys key 非 docid 编码".into())
                })?;
                if let Some(bm) = &self.deletion_bitmap {
                    if bm.is_deleted(docid) {
                        return Ok(true);
                    }
                }
                f(docid)
            })
    }

    /// §27 P0：auto_increment / 自动 docid **水位** = 已写入最大 docid + 1（自动分配起点，
    /// 重启续接不撞已提交行、显式大 id 后自动 id 抬位）。运行期由 put 的 fetch_max 维护；
    /// 重启后（max_docid 归零）首次调用做一次全库 keys-only 扫描恢复现存最大 docid
    /// （AtomicBool swap 只扫一次；顺带修复删除密度分母 Ex-8.7 的重启失真）。
    pub fn auto_watermark(&self) -> u64 {
        if !self.max_docid_loaded.swap(true, Ordering::AcqRel) {
            let mut mx = 0u64;
            // 扫描失败（读损坏等）保守保持 0 → 水位 1；loaded 已置位避免每次重扫
            let _ = self.scan_stream_ids(None, None, |d| {
                if d > mx {
                    mx = d;
                }
                Ok(true)
            });
            self.max_docid.store(mx, Ordering::Relaxed);
        }
        self.max_docid.load(Ordering::Relaxed) + 1
    }

    /// 7.100 全库可见行计数（COUNT(*) 无 WHERE 快路径）：主数据 key-only 流式计数——
    /// SST keys-only 解码免文档值反序列化/clone；merge 版本语义（同 key 最新、Tombstone
    /// 跳过）与 `scan_stream` 全表扫描一致。
    pub fn count_all_docs(&self) -> Result<u64> {
        // Ex-8.1：删除位图启用且存在已删 docid 时，COUNT 与 scan/get 对齐（排除已删），
        // 否则走 key-only 快速路径（零额外开销）。
        if let Some(bm) = &self.deletion_bitmap {
            if bm.deleted_count() > 0 {
                return self
                    .primary
                    .count_keys_range_filtered(None, None, &mut |k| bm.is_deleted_key(k));
            }
        }
        self.primary.count_keys_range(None, None)
    }

    /// 导出共享后台 IO 限速（design 20.5）：启用/关闭顺序扫描路径限速（MB/s；0 = 关闭）。
    /// 与 Compaction 的 `io_limiter` 同 Token Bucket 策略（默认低于前台读写）——导出读 SST
    /// 与后台合并共享同一后台 IO 预算语义，对在线业务影响 <5% 目标。
    pub fn set_scan_rate_limit(&self, mb: u64) {
        let bytes = mb.saturating_mul(1024 * 1024);
        self.primary.set_scan_rate_limit(bytes);
        self.delta.set_scan_rate_limit(bytes);
        if let Some(c) = &self.cidx {
            c.set_scan_rate_limit(bytes);
        }
    }

    /// 事务范围扫描（M 项，事务类查询优化 P0）：RR/SERIALIZABLE 走 `scan_range_at`
    /// 快照版本过滤（一次 k-way merge 扫描，替代逐 id `txn_get`）；同事务未提交写
    /// （`read_own`）覆盖扫描结果；事务内删除的 docid 从结果中排除。
    /// O 项第②步：事务范围读 `&self`（RR 快照只读并行）。
    pub fn scan_range_txn(
        &self,
        txn: &mut crate::txn::Transaction,
        start: Option<u64>,
        end: Option<u64>,
    ) -> Result<Vec<QueryRow>> {
        if txn.is_finished() {
            return Err(crate::error::Error::TxnAborted(format!(
                "txn#{} 已结束",
                txn.id
            )));
        }
        // 扫描快照视图（RC 语义 = 最新视图；RR/SERIALIZABLE = 快照过滤）
        let snapshot = if txn.isolation.uses_snapshot() {
            txn.snapshot()
        } else {
            u64::MAX
        };
        let mut out: Vec<QueryRow> = self.primary.scan_range_at(snapshot, start, end)?;
        // Ex-8.10：删除位图语义对齐（与 txn_get/get_at 一致）——快照视图先排除位图已删 docid。
        // 置于 read_own 覆盖**之前**：事务内对已删 docid 的未提交写（自写复活）仍可覆盖显现。
        // 注：位图删除为非版本化全局语义（get_at 同近似），快照不晚于删除时点亦隐藏（既有取舍）。
        if let Some(bm) = &self.deletion_bitmap {
            out.retain(|(d, _)| !bm.is_deleted(*d));
        }
        // 同事务写覆盖：write_set 中的 docid 用 read_own 值替换/排除
        for row in out.iter_mut() {
            if let Some(own) = txn.read_own(row.0) {
                match own {
                    Some(v) => row.1 = v.to_vec(),
                    None => row.1.clear(), // 事务内已删除：标记为空（下方过滤）
                }
            }
        }
        out.retain(|(_, v)| !v.is_empty());
        // Ex-8.10：事务内未提交 Put 的**新 docid / 已删复活**（基表扫描不含该行）并入窗口——
        // read_own 仅覆盖"已出现"的行（上循环）；此处对 write_set 中未见 docid 补入最新自写值。
        if !txn.ops().is_empty() {
            let mut own_ids: Vec<u64> = txn
                .ops()
                .iter()
                .filter_map(|op| match op {
                    crate::txn::Op::Put { docid, .. } => Some(*docid),
                    crate::txn::Op::Delete { .. } => None,
                })
                .collect();
            own_ids.sort_unstable();
            own_ids.dedup();
            let present: std::collections::HashSet<u64> = out.iter().map(|(d, _)| *d).collect();
            let mut added = false;
            for d in own_ids {
                if present.contains(&d) {
                    continue;
                }
                let in_win = start.map_or(true, |s| d >= s) && end.map_or(true, |e| d <= e);
                if in_win {
                    if let Some(Some(v)) = txn.read_own(d) {
                        out.push((d, v.to_vec()));
                        added = true;
                    }
                }
            }
            if added {
                out.sort_by_key(|r| r.0); // 保持升序（自写并入后重排；事务窗口通常小）
            }
        }
        Ok(out)
    }

    /// 组合索引前缀查询：编码前缀键范围扫描 → 回表主数据。
    pub fn query_by_composite_prefix(&mut self, fields: &[&[u8]]) -> Result<Vec<QueryRow>> {
        let Some(cidx) = self.cidx.clone() else {
            return Ok(Vec::new());
        };
        let start = crate::keys::encode_composite_key(fields, 0);
        let end = crate::keys::encode_composite_key(fields, u64::MAX);
        let hits = cidx.scan_raw_range(Some(&start), Some(&end))?;
        let mut out = Vec::new();
        for (key, _) in hits {
            let (_fields, docid) = crate::keys::decode_composite_key(&key)?;
            if let Some(v) = self.get(docid)? {
                out.push((docid, v));
            }
        }
        Ok(out)
    }

    /// 查询执行器：按 QuerySpec 静态路由到访问路径并执行（design 7.1 最小集枚举）。
    /// 看门狗：查询超时熔断（逐行检查 QueryGuard，超时返回 QueryTooExpensive）。
    pub fn execute(&mut self, spec: &QuerySpec) -> Result<Vec<QueryRow>> {
        // P52：CPU 并发限制（超限返回 Stalled）+ 查询超时熔断
        let guard = self.watchdog.try_begin_query()?;
        // Ex-5.3：倒排查询前刷入攒批缓冲（Inverted 分支可能命中 pending 中的 term）
        self.flush_inverted_pending();
        let rows = match route(spec) {
            AccessPath::PrimaryPoint => {
                let docid = spec
                    .primary_eq
                    .as_ref()
                    .map(|k| crate::keys::decode_docid(k))
                    .transpose()?
                    .unwrap_or(0);
                self.get(docid)?.into_iter().map(|v| (docid, v)).collect()
            }
            AccessPath::PrimaryRange => self.scan_range(None, None)?,
            AccessPath::CompositeIndex { fields } => {
                let fs: Vec<&[u8]> = fields.iter().map(|s| s.as_bytes()).collect();
                self.query_by_composite_prefix(&fs)?
            }
            AccessPath::Inverted { term } => {
                // 倒排回表：逐行熔断检查
                let bitmap = self.inverted.search(&term)?;
                let mut out = Vec::new();
                for docid in bitmap {
                    if guard.is_expired() {
                        return Err(crate::error::Error::QueryTooExpensive(format!(
                            "查询超时（guard #{} > {}ms），熔断中止",
                            guard.query_id(),
                            guard.timeout().as_millis()
                        )));
                    }
                    if let Some(v) = self.get(docid as u64)? {
                        out.push((docid as u64, v));
                    }
                }
                out
            }
            AccessPath::FullScan => self.scan_range(None, None)?,
        };
        Ok(rows)
    }

    /// 倒排内存累积条数（供后台刷盘决策；含攒批缓冲，Ex-5.3）。
    pub fn inverted_mem_docids(&self) -> u64 {
        self.inverted.mem_docids() + self.pending_inverted.lock().unwrap().len() as u64
    }

    // ============ 10 亿库阶段 D：分片级可观测 ============

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

    /// 将倒排攒批缓冲一次性刷入内存字典（Ex-5.3 批处理）。
    /// 低基数 term 跨行聚合：一组 (term, docid) 按 term 分组合并，每 term 一次锁操作。
    /// 崩溃安全：WAL 回放重新走 put 重建倒排，缓冲丢失不丢数据。
    /// O 项第②步：`&self`（pending_inverted 内部 Mutex，读路径查询前也可刷缓冲）。
    fn flush_inverted_pending(&self) {
        let items: Vec<(String, u64)> = std::mem::take(&mut *self.pending_inverted.lock().unwrap());
        if items.is_empty() {
            return;
        }
        let refs: Vec<(&str, u64)> = items.iter().map(|(t, d)| (t.as_str(), *d)).collect();
        self.inverted.add_batch(&refs);
    }

    /// 强制倒排刷盘（先刷入攒批缓冲，再整段落盘）。
    /// J 项（7.73）：刷盘后检测段总量超 GC 阈值 → 置后台 GC 信号（mysql worker 消费；
    /// 无 worker 时由显式 `inverted_gc` / 兜底周期消费）。
    pub fn flush_inverted(&self) -> Result<()> {
        self.flush_inverted_pending();
        self.inverted.flush_segment()?;
        // J 项：段数/大小超阈值 → 后台 GC 信号（避免段数爆炸放大查询延迟）
        if self.inverted.should_gc() {
            self.inverted_gc_pending.store(true, Ordering::Release);
        }
        Ok(())
    }

    /// 倒排段 GC 合并（design 5.2.2/5.2.4⑤）：段文件总量超阈值时合并为少量大段。
    /// 大数据量导入后段数可能爆炸（demo 每 100 万 term 对刷一段 → 5000 万库数百段），
    /// 查询每次遍历全部段（高频 term 每段反序列化 posting）→ 段数直接放大查询延迟。
    /// J 项（7.73）：改 `&self`（inverted 内部 mutate 锁与 flush 互斥）——批量导入后
    /// 由后台 GC worker 自动周期触发（写路径刷盘置信号），显式调用仍可用。
    pub fn inverted_gc(&self) -> Result<crate::inverted::GcReport> {
        self.flush_inverted_pending();
        self.inverted.gc()
    }

    /// 查询看门狗守卫（类 SQL 扫描过滤/回表熔断用）：`is_expired()` 超时后返回
    /// QueryTooExpensive（复用 engine.execute 的查询超时机制）。
    pub fn query_guard(&self) -> crate::watchdog::QueryGuard {
        self.watchdog.begin_query()
    }

    /// 写入 Enrich 配置（design 19 / development 5.21）：Some((fail_policy, from_field,
    /// to_field)) = 启用 local 数据源预连接（server /put 走 join::put_with_enrich）；None = 关闭。
    pub fn enrich_config(&self) -> Option<(&str, &str, &str)> {
        self.enrich
            .as_ref()
            .map(|(f, a, b)| (f.as_str(), a.as_str(), b.as_str()))
    }

    /// 倒排某词条命中的 docid 集合（不回表，供测试/监控/sqlish 等值筛选）。
    /// O 项第②步：读路径 `&self`（查询前刷入攒批缓冲保证一致性）。
    pub fn inverted_posting(&self, term: &str) -> Result<RoaringBitmap> {
        self.flush_inverted_pending();
        self.inverted.search(term)
    }

    /// 倒排某词条命中的文档数（COUNT 聚合，<0.1ms）。
    /// 位图索引快速路径（design 5.2.4，M7-2）：term 命中 `bitmap_fields` 白名单 → 内存位图计数；
    /// 否则回退倒排段扫描。
    pub fn inverted_doc_count(&mut self, term: &str) -> Result<u64> {
        self.flush_inverted_pending();
        // Ex-9.1b：段级 TermMeta 计数载荷求和（全 v4 段亚毫秒，恒含存量）；老段回退精确遍历
        if let Some(n) = self.inverted.doc_count_fast(term)? {
            return Ok(n);
        }
        self.inverted.doc_count(term)
    }

    /// 按字段前缀分组（GROUP BY 聚合）：返回 `field=value` 各分组的文档数。
    /// 位图索引快速路径（M7-2）：字段命中白名单 → 内存位图分组；否则回退倒排段扫描。
    pub fn inverted_group_by(&mut self, field: &str) -> Result<Vec<(String, u64)>> {
        self.flush_inverted_pending();
        if let Some(rows) = self.inverted.bitmap_group_by(field) {
            return Ok(rows);
        }
        self.inverted.group_by(field)
    }

    /// 内存位图组合 AND 计数（design 5.2.4，M7-2）：全部 term 命中白名单 → 交集计数（亚毫秒）；
    /// 否则返回 None（调用方回退逐词条倒排查询）。
    pub fn inverted_bitmap_and_count(&mut self, terms: &[&str]) -> Option<u64> {
        self.flush_inverted_pending();
        self.inverted.bitmap_and(terms).map(|b| b.len())
    }

    /// Ex-8.7：删除置位率 = 净置位数 / max(置位数, 曾写入最大 docid)——"位图置位率"密度。
    fn delete_gc_density(&self) -> f32 {
        let marked = self.garbage_marked.load(Ordering::Relaxed);
        let denom = marked.max(self.max_docid.load(Ordering::Relaxed)).max(1);
        marked as f32 / denom as f32
    }

    /// Ex-8.7：删除密度 Compaction 是否就绪（needs_compact / 紧迫度 / GC 单段重写门槛）：
    /// 位图开启 + `delete_density_min_ratio` > 0 + 置位率 ≥ 阈值 + 无积压……
    /// （`garbage_draining` 排空中无视"新增置位"门槛——直到某轮 0 丢弃收敛）。
    pub fn delete_garbage_pending(&self) -> bool {
        if self.deletion_bitmap.is_none() || self.dd_min_ratio <= 0.0 {
            return false;
        }
        let marked = self.garbage_marked.load(Ordering::Relaxed);
        if marked == 0 {
            return false;
        }
        let draining = self.garbage_draining.load(Ordering::Relaxed);
        if !draining && marked.saturating_sub(self.garbage_done.load(Ordering::Relaxed)) < self.dd_min_docs {
            return false;
        }
        self.delete_gc_density() >= self.dd_min_ratio
    }

    /// Ex-8.7：删除密度维度的跨列族紧迫度权重（W 项公式外挂项）——就绪时 +`DD_URGENCY`：
    /// 低于 L0 段数主因子（×10/段），高于纯 L0 大小软阈值 +8 之下的次级——
    /// 收敛后（L0=0）删除密集主列族仍能压过空闲 delta/cidx 率先被合并回收空间。
    fn delete_garbage_urgency(&self) -> u32 {
        if self.delete_garbage_pending() {
            crate::column_family::DD_URGENCY
        } else {
            0
        }
    }

    /// 基础 Compaction（design 4.5，阶段 3；Ex-5.4 并行化）：primary/cidx/delta 列族压实。
    /// 并行度 `compaction_parallel`：0 = 自动（min(4, 核数/2)）；1 = 串行；>1 = 指定。
    /// W 项：跨列族**紧迫度调度**——每轮仅压实紧迫度最高档（L0 压力/大小超限最大）的列族，
    /// 并列档并行（保留 SSD 并发收益）；其余列族由后台 worker 后续轮次（`while needs_compact`）
    /// 压实——压力最大的列族（通常 primary 主数据）优先收敛，读路径最快受益。
    /// Ex-5.6：删除位图开启时 primary 压实按位图**物理丢弃**已删 docid 的旧数据（墓碑不污染层级）。
    /// Ex-8.7：删除密集时紧迫度叠加删除密度权重；触发后允许收敛单段重写（`compact_gc`），
    /// 并按压实**实际丢弃数**回写排空状态（drop>0 继续 / 0 收敛，见 `apply_gc_feedback`）。
    /// O 项第③步：`&self`——后台合并 worker 在引擎**读锁**下执行（合并不阻塞读；
    /// 与写互斥由 Engine RwLock 保证，快照 store 无并发丢失）。
    pub fn compact(&self) -> Result<crate::column_family::CompactReport> {
        // W 项：紧迫度 = 列族 compaction_urgency（L0 段数×10 + 大小超限 +8）+ 删除密度权重
        let pu = self.primary.compaction_urgency() + self.delete_garbage_urgency();
        let du = self.delta.compaction_urgency();
        let cu = self.cidx.as_ref().map_or(0, |c| c.compaction_urgency());
        let max = pu.max(du).max(cu);
        let empty = crate::column_family::CompactReport {
            merged_ssts: 0,
            kept_keys: 0,
            freed_bytes: 0,
            out_level: 0,
            dropped_keys: 0,
        };
        if max == 0 {
            // 无 L0 压力（urgency 只计 L0 段数/大小）——但底层仍可能需要合并（L1→L2 /
            // L2 收敛 / Ex-8.11 L1 攒批下沉）。防空闲饿死：直接压实 needs_compact 的列族
            // （优先级 primary > delta > cidx），否则空返回。
            let pn = self.primary.needs_compact();
            let dn = self.delta.needs_compact();
            let cn = self.cidx.as_ref().map_or(false, |c| c.needs_compact());
            if pn || dn || cn {
                let mut rep = empty;
                if pn {
                    let r = self.primary.compact()?;
                    merge_report(&mut rep, &r);
                } else if dn {
                    let r = self.delta.compact()?;
                    merge_report(&mut rep, &r);
                } else {
                    let r = self.cidx.as_ref().unwrap().compact()?;
                    merge_report(&mut rep, &r);
                }
                self.metrics.compact_count.fetch_add(1, Ordering::Relaxed);
                return Ok(rep);
            }
            return Ok(empty); // 无压力（调用方应在 needs_compact 下进入）
        }
        let do_p = pu == max;
        let do_c = self.cidx.is_some() && cu == max;
        let do_d = du == max;
        // Ex-8.7：删除密度触发时允许主列族"单底层段重写"（GC 回收已删数据）
        let gc_single = self.delete_garbage_pending();

        let parallel = if self.compaction_parallel == 0 {
            std::thread::available_parallelism()
                .map(|n| (n.get() / 2).clamp(1, 4))
                .unwrap_or(1)
        } else {
            self.compaction_parallel.max(1)
        };
        let cf_count = usize::from(do_p) + usize::from(do_c) + usize::from(do_d);
        let threads = parallel.min(cf_count.max(1));
        // Ex-5.6/5.8：位图不可变借用（与列族共享借用互不冲突）。
        let bm = self.deletion_bitmap.as_ref();
        let needs_filter = bm.is_some_and(|b| b.deleted_count() > 0);
        let filter = |k: &[u8]| bm.is_some_and(|b| b.is_deleted_key(k));
        if threads <= 1 {
            // 串行：仅最高紧迫度档列族
            let mut rep = empty;
            if do_p {
                let r = if needs_filter {
                    self.primary.compact_gc(&filter, gc_single)?
                } else {
                    self.primary.compact()?
                };
                merge_report(&mut rep, &r);
                apply_gc_feedback(&self.garbage_draining, &self.garbage_done, &self.garbage_marked, &r);
            }
            if do_c {
                let r = self.cidx.as_ref().unwrap().compact()?;
                merge_report(&mut rep, &r);
            }
            if do_d {
                let r = self.delta.compact()?;
                merge_report(&mut rep, &r);
            }
            self.metrics.compact_count.fetch_add(1, Ordering::Relaxed);
            return Ok(rep);
        }
        // 并行：仅最高紧迫度档（并列）列族
        let compute_cores = self.affinity.compute.clone(); // Ex-7.2：Compaction 并行线程绑 compute 核
        let (p, c, d) = (&self.primary, self.cidx.as_ref(), &self.delta);
        let merged = std::thread::scope(|s| -> Result<crate::column_family::CompactReport> {
            let h1 = if do_p {
                let cc = compute_cores.clone();
                let f = filter;
                Some(s.spawn(move || {
                    crate::affinity::bind_current(&cc);
                    if needs_filter {
                        p.compact_gc(&f, gc_single)
                    } else {
                        p.compact()
                    }
                }))
            } else {
                None
            };
            let h2 = if do_c {
                let cc = compute_cores.clone();
                let cf = c.unwrap();
                Some(s.spawn(move || {
                    crate::affinity::bind_current(&cc);
                    cf.compact()
                }))
            } else {
                None
            };
            let h3 = if do_d {
                let cc = compute_cores.clone();
                Some(s.spawn(move || {
                    crate::affinity::bind_current(&cc);
                    d.compact()
                }))
            } else {
                None
            };
            let mut merged = empty;
            // h1（下标 0）= primary：join 后按实际丢弃回写删除密度排空状态
            for (k, h) in [h1, h2, h3].into_iter().enumerate() {
                if let Some(handle) = h {
                    let r = handle.join().unwrap()?;
                    if k == 0 && do_p {
                        apply_gc_feedback(
                            &self.garbage_draining,
                            &self.garbage_done,
                            &self.garbage_marked,
                            &r,
                        );
                    }
                    merge_report(&mut merged, &r);
                }
            }
            self.metrics.compact_count.fetch_add(1, Ordering::Relaxed);
            Ok(merged)
        })?;
        Ok(merged)
    }

    /// 是否需要 Compaction（主数据 / delta / cidx 任一列族 L0 超阈值或 L1/L2 需收敛，
    /// 或 Ex-8.7 删除密度就绪——位图删除数据待回收）。
    pub fn needs_compact(&self) -> bool {
        self.primary.needs_compact()
            || self.delta.needs_compact()
            || self.cidx.as_ref().map_or(false, |c| c.needs_compact())
            || self.delete_garbage_pending()
    }

    /// P72（无锁合并）：读取三 CF Arc + 删除位图 Arc + 紧迫度判定（紧凑调度复刻 `compact`）——
    /// mysql worker 在 Engine 读锁内**快速**调用本方法（clone 廉价），drop 锁后对返回的
    /// `CompactTargets::run()` 执行**无锁合并**（与写并发；ssts 变更经 CF `sst_mutate` 互斥，
    /// flush 同锁 → 无丢失更新）。返回 None = 无紧迫度（不需要合并）。
    pub fn compaction_targets(&self) -> Option<CompactTargets> {
        let pu = self.primary.compaction_urgency() + self.delete_garbage_urgency();
        let du = self.delta.compaction_urgency();
        let cu = self.cidx.as_ref().map_or(0, |c| c.compaction_urgency());
        let max = pu.max(du).max(cu);
        if max == 0 {
            return None;
        }
        Some(CompactTargets {
            deletion_bitmap: self.deletion_bitmap.clone(),
            primary: self.primary.clone(),
            cidx: self.cidx.clone(),
            delta: self.delta.clone(),
            do_primary: pu == max,
            do_cidx: self.cidx.is_some() && cu == max,
            do_delta: du == max,
            garbage_marked: Arc::clone(&self.garbage_marked),
            garbage_done: Arc::clone(&self.garbage_done),
            garbage_draining: Arc::clone(&self.garbage_draining),
            gc_single: self.delete_garbage_pending(),
        })
    }

    /// 主数据列族当前 L0 段数（P 项自动 Compaction 收敛性观测 / 测试）。
    pub fn primary_l0_count(&self) -> usize {
        self.primary.l0_count()
    }

    /// X 项：全列族累计刷盘次数（/metrics flush 指标）。
    pub fn total_flush_count(&self) -> u64 {
        self.primary.flush_count()
            + self.delta.flush_count()
            + self.cidx.as_ref().map_or(0, |c| c.flush_count())
    }

    /// P 项：事件驱动自动 Compaction——写入路径自触发（Flush 后 L0 段数/大小超阈值 → 合并收敛）。
    /// O 项第③步双分支：
    /// - **有后台 worker**（mysql 服务挂载，`compact_worker=true`）：只置 `compact_pending` 信号，
    ///   实际合并由 worker 在引擎**读锁**下执行——读写均不被合并阻塞（合并不阻塞读）；
    /// - **无 worker**（demo/rpc/测试）：保持同步执行（单写者模型：合并期间阻塞读写，
    ///   写入自然退避 = 背压，L0 有界）。
    /// guard 上限 8：一次写入最多收敛 8 轮（正常 1~2 轮即收敛，防异常空转死循环）。
    fn auto_compact(&mut self) -> Result<()> {
        if !self.auto_compact {
            return Ok(());
        }
        if self.compact_worker.load(Ordering::Acquire) {
            if self.needs_compact() {
                self.compact_pending.store(true, Ordering::Release);
            }
            return Ok(());
        }
        let mut guard = 0;
        while self.needs_compact() && guard < 8 {
            self.compact()?;
            guard += 1;
        }
        Ok(())
    }

    /// Ex-7.2：网络核列表（server 主线程绑核用）。
    pub fn network_cores(&self) -> Vec<usize> {
        self.affinity.network.clone()
    }

    // ============ Ex-1 本地消息表（Outbox）============

    /// 入队 outbox 消息（Ex-1.1）：docid + 全局 seq 幂等键，与业务写共享 fsync 点
    /// （`maybe_group_commit`）——崩溃恢复按 seq 回放，消息与业务写本地原子。
    /// 返回幂等键（docid, seq）；outbox 关闭时返回 Err(Unsupported)。
    pub fn enqueue_outbox(&mut self, docid: u64, payload: &[u8]) -> Result<(u64, u64)> {
        let Some(ob) = &mut self.outbox else {
            return Err(crate::error::Error::Unsupported(
                "outbox 未启用（config.outbox.enabled = true）".into(),
            ));
        };
        let seq = self.global_seq.fetch_add(1, Ordering::Relaxed);
        ob.enqueue(docid, seq, payload)?;
        self.maybe_group_commit()?; // 与业务写同 fsync 点（本地原子）
        Ok((docid, seq))
    }

    /// 投递器（Ex-1.2）：扫描 pending → 回调投递（true=成功）→ 标记 done。
    /// 返回投递成功数；失败留 pending（调用方退避重试）。投递成功后统一落盘
    /// （done 状态持久，防重投）。
    pub fn dispatch_outbox(&mut self, deliver: impl FnMut(&[u8], &[u8]) -> bool) -> Result<usize> {
        let n = match &mut self.outbox {
            Some(ob) => ob.dispatch(deliver)?,
            None => 0,
        };
        if n > 0 {
            self.flush_wal()?;
        }
        Ok(n)
    }

    /// 当前 pending 消息数（排空校验/监控）。
    pub fn outbox_pending(&mut self) -> Result<usize> {
        match &mut self.outbox {
            Some(ob) => ob.pending_count(),
            None => Ok(0),
        }
    }

    /// 是否已排空（Ex-1.4：扩容/切换前置条件）。
    pub fn outbox_drained(&mut self) -> Result<bool> {
        match &mut self.outbox {
            Some(ob) => ob.drained(),
            None => Ok(true),
        }
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

    /// 备份前一致性准备（development 5.11 冷备份第 1-2 步）：
    /// 刷 WAL → 全部 MemTable 落盘为 SST → 倒排内存字典刷盘为 `.seg` 段，
    /// 保证数据目录磁盘态自包含（含倒排段清单 Manifest、字段注册表等随目录整体打包）。
    pub fn prepare_backup(&mut self) -> Result<()> {
        self.flush_wal()?;
        if self.primary.memtable_bytes() > 0 {
            self.primary.switch_and_flush()?;
        }
        if let Some(cidx) = &self.cidx {
            if cidx.memtable_bytes() > 0 {
                cidx.switch_and_flush()?;
            }
        }
        self.flush_inverted()?;
        Ok(())
    }
}

/// 组提交（M8）清理：停后台兜底线程并 join，最终落盘待刷 WAL（保证正常退出不丢窗口尾部）。
impl Drop for Engine {
    fn drop(&mut self) {
        if let Some(stop) = &self.gc_stop {
            stop.store(true, Ordering::Relaxed);
        }
        if let Some(h) = self.gc_thread.take() {
            let _ = h.join();
        }
        if self.group_commit.is_some() {
            let _ = self.flush_wal();
        }
        // Ex-5.6：正常退出兜底落盘删除位图脏页（组提交关闭时 flush_wal 不执行，位图独立 flush）
        if let Some(bm) = &self.deletion_bitmap {
            let _ = bm.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;

    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let name = format!("eng-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    #[test]
    fn shard_metrics_attach_and_render() {
        // 10 亿库阶段 D：挂载分片指标 → 水位上报 → /metrics 渲染 + 预警
        let cfg = Config::default();
        let e = Engine::open(&tmp(), &cfg).unwrap();
        assert!(e.shard_metrics_render().is_empty(), "未挂载渲染为空");
        e.attach_shard_metrics(10);
        e.update_shard_watermark(0, 100_000_000); // 每分片 1 亿（10 亿库）
        e.record_shard_write(0);
        e.record_shard_read(0);
        let out = e.shard_metrics_render();
        assert!(out.contains("shanshui_shard_docid_watermark{shard=\"0\"} 100000000"));
        assert!(out.contains("shanshui_shard_writes_total{shard=\"0\"} 1"));
        assert!(e.shard_watermark_alerts().is_empty(), "10 亿库水位无预警");
        // 高水位（≈82%）→ Warn 预警
        e.update_shard_watermark(1, 900_000_000_000);
        let alerts = e.shard_watermark_alerts();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].0, 1);
        assert_eq!(alerts[0].1, crate::shard_metrics::WatermarkLevel::Warn);
    }

    #[test]
    fn compact_targets_run_matches_engine_compact() {
        // P72（无锁合并）：`Engine::compaction_targets` + `CompactTargets::run`（worker 无锁路径）
        // 与 `Engine::compact`（读锁内串行）收敛结果一致——多列族压力 + 删除位图过滤双路径。
        let mut cfg = Config::default();
        cfg.memtable.max_size_mb = 1; // 小 MemTable → 写入快速 flush 多段
        cfg.storage.l0_stall_threshold = 2; // 低 L0 阈值 → 2 段即触发合并
        let mut e1 = Engine::open(&tmp(), &cfg).unwrap();
        let mut e2 = Engine::open(&tmp(), &cfg).unwrap();
        let val = vec![b'x'; 1024];
        for seg in 0..4u64 {
            for i in seg * 500..seg * 500 + 500 {
                let t: &[&str] = &["tag_a"];
                e1.put(i, val.clone(), t).unwrap();
                e2.put(i, val.clone(), t).unwrap();
            }
        }
        // 删除一部分（删除位图开启）→ 合并需物理丢弃
        for i in (0..2_000u64).step_by(3) {
            e1.delete(i).unwrap();
            e2.delete(i).unwrap();
        }
        e1.flush_wal().unwrap();
        e2.flush_wal().unwrap();
        // e1：Engine::compact 读锁路径收敛；e2：无锁路径（compaction_targets 循环 run）收敛
        while e1.needs_compact() {
            let _ = e1.compact().unwrap();
        }
        while e2.needs_compact() {
            let Some(t) = e2.compaction_targets() else { break };
            t.run().unwrap();
        }
        assert!(!e1.needs_compact());
        assert!(!e2.needs_compact());
        // 收敛后数据一致（存活 docid 全部命中；已删不可见）
        for i in 0..2_000u64 {
            let v1 = e1.get(i).unwrap();
            let v2 = e2.get(i).unwrap();
            assert_eq!(v1, v2, "docid={i}");
        }
        // 段数收敛一致（同输入 → 同压实结果）
        assert_eq!(e1.primary_l0_count(), e2.primary_l0_count());
        assert_eq!(e1.primary.sst_count(), e2.primary.sst_count());
    }

    #[test]
    fn persist_manifest_reflects_memory_snapshot_only() {
        // P73：persist_manifest 基于内存快照（ssts ArcSwap）重建清单——磁盘上存在但不在
        // 内存快照中的文件（如无锁合并并发时"正在写入的半写段"）不得写入 manifest，
        // 否则重启加载失败。确定性验证：放置幽灵段后 flush，manifest 不含它。
        let mut cfg = Config::default();
        cfg.memtable.max_size_mb = 1; // 小 MemTable → flush 触发 manifest 重写
        let data_dir = tmp();
        let mut engine = Engine::open(&data_dir, &cfg).unwrap();
        for i in 0..2000u64 {
            engine.put(i, format!("v{i}").into_bytes(), &[]).unwrap();
        }
        engine.flush_primary().unwrap();
        // 磁盘放"幽灵"段（模拟并发写入中的半写段 / 残留文件）
        let ghost = data_dir.join("primary").join("sst-99999999.sst");
        std::fs::write(&ghost, b"partial-written-not-in-snapshot").unwrap();
        // 再写并 flush → persist 重写 manifest
        for i in 2000..3000u64 {
            engine.put(i, format!("v{i}").into_bytes(), &[]).unwrap();
        }
        engine.flush_primary().unwrap();
        let m: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(data_dir.join("primary").join("manifest.json")).unwrap(),
        )
        .unwrap();
        let files = m["sst_files"].as_array().expect("manifest sst_files");
        assert!(
            !files
                .iter()
                .any(|f| f.as_str().unwrap().contains("99999999")),
            "manifest 不得引用非内存快照段（幽灵段）"
        );
        assert!(files.len() >= 2, "flush 段应在 manifest 中");
        // 重开：数据完整（manifest 与磁盘一致可加载）
        drop(engine);
        let engine = Engine::open(&data_dir, &cfg).unwrap();
        for i in (0..3000u64).step_by(997) {
            let v = engine.get(i).unwrap();
            assert_eq!(v.as_deref(), Some(format!("v{i}").as_bytes()), "docid={i}");
        }
    }

    #[test]
    fn scan_rate_limit_slows_stream() {
        // design 20.5：导出共享后台 IO 限速——启用限速后顺序扫描显著变慢（Token Bucket 生效），
        // 关闭后恢复；前台点查不受限速影响（scan_limiter 只作用于 scan_stream）。
        let mut cfg = Config::default();
        cfg.memtable.max_size_mb = 1;
        let mut engine = Engine::open(&tmp(), &cfg).unwrap();
        for i in 0..10u64 {
            engine.put(i, vec![b'x'; 200_000], &[]).unwrap(); // 200KB × 10 = 2MB
        }
        engine.flush_primary().unwrap();
        // 无限制：扫描快
        let t0 = std::time::Instant::now();
        let mut n = 0u64;
        engine
            .scan_stream(None, None, |_, v| {
                n += 1;
                assert_eq!(v.len(), 200_000);
                Ok(true)
            })
            .unwrap();
        let fast = t0.elapsed();
        assert_eq!(n, 10);
        // 限速 1MB/s：2MB 数据（1s 突发桶 + 1MB 需补桶）→ 显著慢于无限速
        engine.set_scan_rate_limit(1);
        let t1 = std::time::Instant::now();
        engine.scan_stream(None, None, |_, _| Ok(true)).unwrap();
        let slow = t1.elapsed();
        assert!(slow > fast, "限速后扫描应更慢（fast={fast:?} slow={slow:?}）");
        assert!(
            slow.as_millis() >= 300,
            "限速 1MB/s 扫描 2MB 应 ≥300ms（实际 {slow:?}）"
        );
        // 关闭限速恢复
        engine.set_scan_rate_limit(0);
        let t2 = std::time::Instant::now();
        engine.scan_stream(None, None, |_, _| Ok(true)).unwrap();
        assert!(t2.elapsed() < slow, "关闭限速后应恢复快速扫描");
    }

    fn cfg() -> Config {
        let mut c = Config::default();
        c.sstable.compression = "none".into();
        c
    }

    fn gc_cfg(window_us: u64) -> Config {
        let mut c = cfg();
        c.storage.group_commit_us = window_us;
        c
    }

    // ---------- D/E/F LSM 事务三阶段 ----------

    #[test]
    fn write_batch_atomic_commit_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        let mut wb = crate::txn::WriteBatch::new();
        wb.put(1, b"doc-1".to_vec(), vec!["t1".into()]);
        wb.put(2, b"doc-2".to_vec(), vec!["t2".into()]);
        wb.delete(3); // 删除不存在的 docid 应合法（幂等）
        e.write(&wb).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"doc-1");
        assert_eq!(e.get(2).unwrap().unwrap(), b"doc-2");
        assert!(e.get(3).unwrap().is_none());
    }

    #[test]
    fn write_batch_validate_rejects_zero_docid_before_apply() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        let mut wb = crate::txn::WriteBatch::new();
        wb.put(0, b"x".to_vec(), vec![]);
        wb.put(10, b"ok".to_vec(), vec![]);
        assert!(e.write(&wb).is_err(), "预校验失败 → 拒绝提交");
        assert!(e.get(10).unwrap().is_none(), "预校验失败不得应用任何 op（失败回滚语义）");
    }

    #[test]
    fn write_batch_rollback_discards_ops() {
        let mut wb = crate::txn::WriteBatch::new();
        wb.put(1, b"x".to_vec(), vec![]);
        wb.delete(2);
        wb.rollback();
        assert!(wb.is_empty());
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.write(&wb).unwrap(); // 空批合法提交
        assert!(e.get(1).unwrap().is_none());
    }

    #[test]
    fn txn_rr_snapshot_read_ignores_concurrent_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v0".to_vec(), &["t"]).unwrap();
        // MemTable 不保留多版本（design 4.7 已知局限）：快照读需旧版本已落 SST
        e.flush_primary().unwrap();
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        // 快照后并发写入（模拟其他事务已提交）
        e.put(1, b"v1".to_vec(), &["t"]).unwrap();
        let got = e.txn_get(&mut txn, 1).unwrap();
        assert_eq!(got.unwrap(), b"v0", "RR 应读事务开始前的快照值");
        e.txn_rollback(txn);
    }

    #[test]
    fn txn_rr_snapshot_sees_old_version_in_memtable_without_flush() {
        // S 项（严格 MVCC）：旧实现快照读需旧版本已落 SST（MemTable 仅保最新）——
        // 事务活跃期 + 并发写同 key 且未 flush 时，快照读会读到新版本（正确性缺陷）。
        // 修复后 MemTable 保留版本链，未刷盘也能读到快照点版本。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v0".to_vec(), &["t"]).unwrap();
        // 不 flush：v0 留在 MemTable
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        // 快照后并发写同 key（仍不 flush）→ MemTable 出现新版本
        e.put(1, b"v1".to_vec(), &["t"]).unwrap();
        let got = e.txn_get(&mut txn, 1).unwrap();
        assert_eq!(
            got.unwrap(),
            b"v0",
            "RR 快照应读到旧版本（无需 flush 落 SST）"
        );
        // 同事务写覆盖（read_own 优先）
        txn.put(1, b"own".to_vec(), vec![]);
        assert_eq!(e.txn_get(&mut txn, 1).unwrap().unwrap(), b"own");
        e.txn_rollback(txn);
        // 提交后最新可见
        assert_eq!(e.get(1).unwrap().unwrap(), b"v1");
    }

    #[test]
    fn txn_snapshot_cache_repeated_get_hits_without_stale() {
        // T 项：RR 快照读事务内点查小缓存——同 key 二次读直达（snap_get 命中）且结果一致
        // （快照 seq 恒定）；RC 不缓存（读最新语义）；事务 drop 即弃（重新 begin 缓存为空）。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v0".to_vec(), &["t"]).unwrap();
        e.flush_primary().unwrap();

        // RR：第一次读写入缓存，第二次读命中缓存（外部已改但快照一致）
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        assert_eq!(txn.snap_get(1), None, "缓存初始为空");
        let first = e.txn_get(&mut txn, 1).unwrap().unwrap();
        assert_eq!(first, b"v0");
        assert_eq!(txn.snap_get(1), Some(Some(b"v0".to_vec())), "首读后已缓存");
        // 外部并发写（seq > 快照）→ 二次读仍走缓存返回快照值
        e.put(1, b"v1".to_vec(), &["t"]).unwrap();
        assert_eq!(e.txn_get(&mut txn, 1).unwrap().unwrap(), b"v0", "缓存命中=快照一致");
        assert_eq!(txn.snap_get(2), None, "未读过的 key 不在缓存");
        // 同事务写后读 → read_own 优先于缓存
        txn.put(1, b"own".to_vec(), vec![]);
        assert_eq!(e.txn_get(&mut txn, 1).unwrap().unwrap(), b"own");
        e.txn_rollback(txn);

        // 新事务缓存为空（随 Transaction drop 即弃）
        let mut txn2 = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        assert_eq!(txn2.snap_get(1), None, "新事务缓存应为空");
        assert_eq!(e.txn_get(&mut txn2, 1).unwrap().unwrap(), b"v1", "读最新已提交");
        e.txn_rollback(txn2);

        // RC：不缓存（每次读最新，缓存会破坏语义）
        let mut txn3 = e.txn_begin(crate::txn::Isolation::ReadCommitted);
        assert_eq!(e.txn_get(&mut txn3, 1).unwrap().unwrap(), b"v1");
        assert_eq!(txn3.snap_get(1), None, "RC 不写缓存");
        e.txn_rollback(txn3);
    }

    #[test]
    fn scan_range_txn_snapshot_filter_and_own_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=5u64 {
            e.put(i, format!("v0-{i}").into_bytes(), &["t"]).unwrap();
        }
        e.flush_primary().unwrap(); // 旧版本落 SST（MemTable 不保留多版本）
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        // 快照后并发写 docid 3 → 扫描应显示快照值 v0-3（隔离并发写）
        e.put(3, b"v1-3".to_vec(), &["t"]).unwrap();
        // 事务内写 docid 2 / 事务内删除 docid 4 → 扫描应覆盖/排除
        txn.put(2, b"own-2".to_vec(), vec!["t".into()]);
        txn.delete(4);
        let rows = e.scan_range_txn(&mut txn, Some(1), Some(5)).unwrap();
        let map: std::collections::HashMap<u64, Vec<u8>> = rows.into_iter().collect();
        assert_eq!(map.get(&1).unwrap(), b"v0-1");
        assert_eq!(map.get(&2).unwrap(), b"own-2", "同事务写应覆盖扫描结果");
        assert_eq!(map.get(&3).unwrap(), b"v0-3", "快照隔离：并发写不可见");
        assert_eq!(map.len(), 4, "事务内删除 docid 4 应从扫描排除");
        assert!(!map.contains_key(&4));
        e.txn_rollback(txn);
    }

    #[test]
    fn txn_scan_respects_deletion_bitmap_revival_and_insert() {
        // Ex-8.10：事务扫描删除位图过滤（与 txn_get/get_at 对齐）+ 事务内自写复活已删 docid / 新插入
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=10u64 {
            e.put(i, format!("v-{i}").into_bytes(), &["t"]).unwrap();
        }
        e.flush_primary().unwrap();
        e.delete(5).unwrap(); // 位图删除
        // (1) 快照扫描排除位图已删（与 txn_get 一致）
        {
            let mut t = e.txn_begin(crate::txn::Isolation::RepeatableRead);
            let rows = e.scan_range_txn(&mut t, None, None).unwrap();
            assert_eq!(rows.len(), 9, "位图已删 docid 5 应从扫描排除");
            assert!(!rows.iter().any(|r| r.0 == 5));
            assert!(e.txn_get(&mut t, 5).unwrap().is_none(), "与 txn_get 语义一致");
            e.txn_rollback(t);
        }
        // (2) 事务内复活已删 docid 5（未提交）→ 扫描应含自写值（位图过滤后 read_own 复活）
        {
            let mut t = e.txn_begin(crate::txn::Isolation::RepeatableRead);
            t.put(5, b"revived".to_vec(), vec!["t".into()]);
            let rows = e.scan_range_txn(&mut t, None, None).unwrap();
            let map: std::collections::HashMap<u64, Vec<u8>> = rows.into_iter().collect();
            assert_eq!(map.len(), 10);
            assert_eq!(map.get(&5).unwrap(), b"revived", "自写复活已删 docid 应可见");
            e.txn_rollback(t);
        }
        // (3) 事务内新插入 docid 11 → 窗口含自写
        {
            let mut t = e.txn_begin(crate::txn::Isolation::RepeatableRead);
            t.put(11, b"new".to_vec(), vec!["t".into()]);
            let rows = e.scan_range_txn(&mut t, Some(9), Some(11)).unwrap();
            let map: std::collections::HashMap<u64, Vec<u8>> = rows.into_iter().collect();
            assert_eq!(map.len(), 3, "docid 9,10 已有 + 11 自写");
            assert_eq!(map.get(&11).unwrap(), b"new");
            e.txn_rollback(t);
        }
    }

    #[test]
    fn txn_range_scan_hides_phantom_after_flush_c4() {
        // 缺陷 B（C4 变体）：快照后他事务插入且已 flush 落 SST——范围快照扫仍须隐藏幻影行
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        // pre：900401 已提交并落盘（快照点 0）
        e.put(
            900401,
            serde_json::json!({"k": 0}).to_string().into_bytes(),
            &[],
        )
        .unwrap();
        e.flush_primary().unwrap();
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        // 他事务在区间内插入 900400（快照后）→ flush 落 SST（幻影落盘）
        e.put(
            900400,
            serde_json::json!({"k": 1}).to_string().into_bytes(),
            &[],
        )
        .unwrap();
        e.flush_primary().unwrap();
        // 范围快照扫：仅 900401（900400 seq 在快照后，即使已落盘也不可见）
        let rows = e.scan_range_txn(&mut txn, Some(900400), Some(900402)).unwrap();
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![900401], "flush 后幻影行仍不可见");
        assert!(e.get(900400).unwrap().is_some(), "最新视图应含他事务插入（写入在）");
        e.txn_rollback(txn);
    }

    #[test]
    fn txn_scan_hides_phantom_multi_segment_heap_c4() {
        // 缺陷 B（C4 多段变体）：>4 源走 heap 归并分支——快照后他事务插入（多段场景）仍须隐藏
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        // 6 段 × 100 行 pre 数据（全量窗口内 → 每段都建迭代器 → mem+6sst > 4 → heap 分支）
        for g in 0..6u64 {
            for i in 0..100u64 {
                let d = g * 100 + i;
                e.put(d, format!("v{d}").into_bytes(), &["t"]).unwrap();
            }
            e.flush_primary().unwrap();
        }
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        // 他事务在快照后插入 900400 → 最新视图可见、快照不可见
        e.put(900400, b"phantom".to_vec(), &["t"]).unwrap();
        let rows = e.scan_range_txn(&mut txn, None, None).unwrap();
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(rows.len(), 600, "快照全量扫描应仅含 6×100 行 pre 数据");
        assert!(!ids.contains(&900400), "heap 分支幻影行 900400 不可见");
        assert!(e.get(900400).unwrap().is_some(), "最新视图应含幻影行（写入在）");
        e.txn_rollback(txn);
    }

    #[test]
    fn txn_range_scan_hides_revived_row_c4() {
        // 缺陷 B（C4 根因）：delete（位图）→ 他事务 put 复活（清位图）后，快照点位于
        // [删除, 复活) 的主事务范围快照扫不得见复活行（回读到复活前旧版本 = 幻影）
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        // 前轮已提交行 900400（历史版本 v1）
        e.put(900400, b"v1".to_vec(), &[]).unwrap();
        // 本轮 case 清理：delete 900400（位图置位 + 版本化 tombstone）
        e.delete(900400).unwrap();
        // pre 900401（BEGIN 前 autocommit 提交，快照可见）
        e.put(900401, b"v0".to_vec(), &[]).unwrap();
        // main BEGIN：快照点在删除之后、复活之前
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        // aux INSERT 复活 900400（put 清位 + 新版本 seq > snapshot）
        e.put(900400, b"v2".to_vec(), &[]).unwrap();
        // 范围快照扫：仅 900401（900400 快照点在删除期 → 不可见）
        let rows = e.scan_range_txn(&mut txn, Some(900400), Some(900402)).unwrap();
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![900401], "复活行在快照(删除期)不可见");
        assert!(e.get(900400).unwrap().is_some(), "最新视图应见复活行 v2");
        e.txn_rollback(txn);
    }

    #[test]
    fn count_all_docs_matches_scan_stream() {
        // 7.100：key-only 免值计数与 scan_stream 全表可见行一致（覆盖 + 删除 + flush 混合）
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 0..5000u64 {
            let doc = serde_json::json!({"k": format!("d{i}"), "n": i});
            e.put(i, serde_json::to_vec(&doc).unwrap(), &["k"]).unwrap();
        }
        // 覆盖（同 docid 二次 put 不增行）+ 删除
        e.put(0, serde_json::to_vec(&serde_json::json!({"k": "d0-v2"})).unwrap(), &["k"]).unwrap();
        e.delete(1).unwrap();
        e.delete(2).unwrap();
        e.delete(3).unwrap();
        let fast = e.count_all_docs().unwrap();
        let mut slow = 0u64;
        e.scan_stream(None, None, |_d, _v| {
            slow += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(fast, slow, "count_keys_range 应与 scan_stream 一致（memtable 期）");
        // flush 后（SST keys-only 解码路径）再验
        e.flush_primary().unwrap();
        let fast2 = e.count_all_docs().unwrap();
        let mut slow2 = 0u64;
        e.scan_stream(None, None, |_d, _v| {
            slow2 += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(fast2, slow2, "count_keys_range 应与 scan_stream 一致（flush 后）");
        assert_eq!(fast, fast2, "flush 前后计数一致");
    }

    #[test]
    fn scan_collapses_within_source_multi_versions() {
        // Ex-8.1（demo range-window 发现的折叠缺口）：同 docid 覆盖写后未 compaction 收敛刷盘，
        // 同源（memtable/SST）连续同 key 多版本行——scan/流式/count 均应折叠为最新版本
        // （修复前：收集 100 行 vs 流式/计数 110 行）。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=100u64 {
            e.put(i, format!("v0-{i}").into_bytes(), &["t"]).unwrap();
        }
        for i in 10..=20u64 {
            e.put(i, format!("v1-{i}").into_bytes(), &["t"]).unwrap();
        }
        // 不 compaction，直接刷盘 → 同 key 新旧两行同落文件
        e.flush_primary().unwrap();
        let c = e.scan_range(None, None).unwrap();
        assert_eq!(c.len(), 100, "scan_range（收集路径）应折叠同源多版本");
        let mut s = 0u64;
        e.scan_stream(None, None, |_d, _v| {
            s += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(s, 100, "scan_stream（流式 merge）应折叠同源同 key 旧版本");
        assert_eq!(e.count_all_docs().unwrap(), 100, "count 应折叠同源同 key 旧版本");
        // 值取最新版本
        let rows: std::collections::HashMap<u64, Vec<u8>> = e.scan_range(None, None).unwrap().into_iter().collect();
        assert_eq!(rows.get(&15).unwrap(), b"v1-15", "覆盖写应返回最新版本");
    }

    #[test]
    fn scan_excludes_deleted_and_revive() {
        // Ex-8.1：删除位图语义对齐——delete 后 scan/流式/count 与 get 一致不可见；
        // put 清位复活后重新可见。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=100u64 {
            e.put(i, format!("d{i}").into_bytes(), &["t"]).unwrap();
        }
        e.flush_primary().unwrap();
        for i in 30..=40u64 {
            e.delete(i).unwrap(); // 11 个（30..=40 闭区间）
        }
        let rows = e.scan_range(None, None).unwrap();
        assert_eq!(rows.len(), 89, "scan_range 应排除位图已删 docid");
        assert!(rows.iter().all(|(d, _)| !(30..=40).contains(d)));
        let mut s = 0u64;
        let mut has_del = false;
        e.scan_stream(None, None, |d, _v| {
            s += 1;
            if (30..=40).contains(&d) {
                has_del = true;
            }
            Ok(true)
        })
        .unwrap();
        assert_eq!(s, 89, "scan_stream 应排除位图已删 docid");
        assert!(!has_del);
        assert_eq!(e.count_all_docs().unwrap(), 89, "count 应排除位图已删 docid");
        assert!(e.get(35).unwrap().is_none(), "get 应不可见已删");
        // put 复活：清位后重新可见
        e.put(35, b"revived".to_vec(), &["t"]).unwrap();
        assert_eq!(e.get(35).unwrap().unwrap(), b"revived");
        let rows2 = e.scan_range(None, None).unwrap();
        assert_eq!(rows2.len(), 90, "put 复活后 scan 应重新可见");
        assert!(rows2.iter().any(|(d, _)| *d == 35));
    }

    #[test]
    fn scan_prunes_disjoint_ssts_windows() {
        // Ex-8.2：scan 路径段级 key 范围剪枝——3 个不相交文件下，窗口只命中相交文件，
        // 结果与全建迭代器一致（收集 == 流式），全扫与边界窗口正确。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for (lo, hi) in [(1u64, 4000u64), (4001, 8000), (8001, 12000)] {
            for i in lo..=hi {
                e.put(i, format!("d{i}").into_bytes(), &["t"]).unwrap();
            }
            e.flush_primary().unwrap(); // 三个不相交 docid 范围的 SST
        }
        let wins: Vec<(Option<u64>, Option<u64>)> = vec![
            (None, None),
            (Some(3990), Some(4010)),   // 跨文件边界
            (Some(7000), Some(7200)),   // 只命中中段
            (Some(11000), Some(12000)), // 尾段含端点
            (Some(1), Some(1)),
            (Some(12001), Some(13000)), // 越界空
            (None, Some(4000)),
            (Some(8001), None),
        ];
        for &(a, b) in &wins {
            let c = e.scan_range(a, b).unwrap();
            let mut s: Vec<(u64, Vec<u8>)> = Vec::new();
            e.scan_stream(a, b, |d, v| {
                s.push((d, v.to_vec()));
                Ok(true)
            })
            .unwrap();
            assert_eq!(c.len(), s.len(), "窗口 {a:?}..{b:?} 行数不一致 {} vs {}", c.len(), s.len());
            for (i, (cr, sr)) in c.iter().zip(s.iter()).enumerate() {
                assert_eq!(cr, sr, "窗口 {a:?}..{b:?} 第 {i} 行不一致");
            }
        }
        assert_eq!(e.scan_range(None, None).unwrap().len(), 12000, "全扫应 12000 行");
        assert_eq!(e.count_all_docs().unwrap(), 12000, "全库计数应 12000");
        assert_eq!(
            e.scan_range(Some(7000), Some(7200)).unwrap().len(),
            201,
            "中段窗口应 201 行（7000..=7200）"
        );
        // 剪枝不丢跨文件边界行
        let cross = e.scan_range(Some(3998), Some(4003)).unwrap();
        assert_eq!(cross.len(), 6, "跨文件窗口应 6 行");
    }

    #[test]
    fn scan_block_cache_warm_repeat_consistent() {
        // Ex-8.3：扫描路径块缓存（写穿 + 全组命中免 IO/解压）——首扫预热后重复窗口结果一致
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=20_000u64 {
            e.put(i, format!("d{i}").into_bytes(), &["t"]).unwrap();
        }
        e.flush_primary().unwrap();
        let w = (Some(9000u64), Some(9300u64));
        let first = e.scan_range(w.0, w.1).unwrap();
        assert_eq!(first.len(), 301);
        // 二次（应全块缓存命中）与流式/计数一致
        for _ in 0..3 {
            assert_eq!(e.scan_range(w.0, w.1).unwrap(), first, "缓存后扫描应一致");
        }
        let mut s = 0u64;
        e.scan_stream(w.0, w.1, |_d, _v| {
            s += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(s, 301, "流式窗口应一致");
        // 计数（keys-only 缓存路径）重复一致
        let c1 = e.count_all_docs().unwrap();
        let c2 = e.count_all_docs().unwrap();
        assert_eq!(c1, c2);
        assert_eq!(c1, 20_000);
    }

    #[test]
    fn auto_watermark_resumes_after_reopen() {
        // §27 P0：auto docid 水位 = 已写入最大 docid + 1；重启后惰性全库扫描恢复
        // （auto 分配续接不撞已提交行）；loaded 后幂等不重扫
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        // 运行期：put 的 fetch_max 维护 → 水位 = 曾写最大 + 1
        e.put(5, b"v5".to_vec(), &[]).unwrap();
        e.put(2, b"v2".to_vec(), &[]).unwrap();
        assert_eq!(e.auto_watermark(), 6, "运行期水位 = max+1");
        assert_eq!(e.auto_watermark(), 6, "重复调用幂等");
        e.put(100, b"v100".to_vec(), &[]).unwrap();
        assert_eq!(e.auto_watermark(), 101, "显式大 id 抬水位");
        // 重启：max_docid 归零 → 首次 auto_watermark 扫现存最大恢复（不含新引擎的 put 前）
        drop(e);
        let e2 = Engine::open(dir.path(), &cfg()).unwrap();
        assert_eq!(e2.auto_watermark(), 101, "重启后惰性恢复现存最大+1");
        assert_eq!(e2.auto_watermark(), 101);
        // 恢复后的水位保证 auto 分配不撞已提交行（分配 ≥ 101）
        assert!(e2.auto_watermark() > 100);
    }

    #[test]
    fn scan_stream_ids_matches_scan_stream() {
        // Ex-8.3 Part B：keys-only id 流式与全值 scan_stream 的 docid 集一致
        // （覆盖折叠 / 删除位图 / 多文件 / 越界空窗口）
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=5000u64 {
            e.put(i, format!("v0-{i}").into_bytes(), &["t"]).unwrap();
        }
        for i in 10..=20u64 {
            e.put(i, format!("v1-{i}").into_bytes(), &["t"]).unwrap(); // 覆盖（折叠验证）
        }
        e.flush_primary().unwrap();
        for i in 5001..=10_000u64 {
            e.put(i, format!("d{i}").into_bytes(), &["t"]).unwrap();
        }
        e.flush_primary().unwrap();
        for i in 3000..=3005u64 {
            e.delete(i).unwrap(); // 位图删除（6 个）
        }
        let wins: Vec<(Option<u64>, Option<u64>)> = vec![
            (None, None),
            (Some(1), Some(100)),
            (Some(2990), Some(3010)), // 覆盖段 + 删除段混合
            (Some(6000), Some(7000)),
            (Some(9990), Some(10010)), // 端点 + 越界
            (Some(20000), Some(30000)),
        ];
        for &(a, b) in &wins {
            let mut full: Vec<u64> = Vec::new();
            e.scan_stream(a, b, |d, _v| {
                full.push(d);
                Ok(true)
            })
            .unwrap();
            let mut ids: Vec<u64> = Vec::new();
            e.scan_stream_ids(a, b, |d| {
                ids.push(d);
                Ok(true)
            })
            .unwrap();
            assert_eq!(ids, full, "窗口 {a:?}..{b:?} keys-only 与全值 docid 集不一致");
        }
        let all: Vec<u64> = {
            let mut v = Vec::new();
            e.scan_stream_ids(None, None, |d| {
                v.push(d);
                Ok(true)
            })
            .unwrap();
            v
        };
        assert_eq!(all.len(), 9994, "删除 6 个后应有 9994 行");
        assert!(!all.contains(&3000));
        assert!(all.contains(&10));
    }

    #[test]
    fn delayed_l1_promotion_converges_and_preserves() {
        // Ex-8.11：L1 延迟大合并配置下，burst+flush+drain 工作负载**收敛**且数据完整
        // （选择级延迟语义由 column_family::tests::select_compaction_inputs_picks_levels 覆盖）
        fn run(l1_trigger: usize, l2_trigger: usize) -> (u64, u64) {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = cfg();
            cfg.storage.auto_compact = false;
            cfg.storage.group_commit_us = 2000;
            cfg.storage.l0_stall_threshold = 2;
            cfg.storage.l0_stall_min = 2;
            cfg.storage.l0_stall_max = 2;
            cfg.storage.l1_trigger_files = l1_trigger;
            cfg.storage.l2_trigger_files = l2_trigger;
            let mut e = Engine::open(dir.path(), &cfg).unwrap();
            let mut rounds = 0u64;
            let mut id = 0u64;
            for _b in 0..4 {
                for _f in 0..2 {
                    for _ in 0..30u64 {
                        id += 1;
                        e.put_nosync(id, format!("d{id}").into_bytes(), &["t"]).unwrap();
                    }
                    e.flush_primary().unwrap();
                }
                while e.needs_compact() && rounds < 500 {
                    e.compact().unwrap();
                    rounds += 1;
                }
            }
            // 收敛护栏：不应出现 needs_compact 恒真空转
            assert!(
                !e.needs_compact() || rounds >= 500,
                "l1_trigger={l1_trigger} 应在护栏内收敛，rounds={rounds}"
            );
            let total = id;
            assert_eq!(e.count_all_docs().unwrap(), total, "数据应完整");
            (rounds, total)
        }
        let (r0, t0) = run(0, 0); // 现行为
        let (rd, t1) = run(3, 2); // 延迟（攒 3 才下沉）
        assert_eq!(t0, t1);
        assert!(rd <= r0 + 2, "延迟模式收敛轮数不应显著劣化：default={r0} delayed={rd}");
        eprintln!("[Ex-8.11] rounds default={r0} delayed(l1=3)={rd} total={t0}");
    }

    #[test]
    fn seq_prune_snapshot_reads_stable_across_reopen() {
        // Ex-8.6：段级 min seq 快照剪枝——旧快照读对新文件整段跳过；语义与版本过滤一致；
        // 重开后惰性推导重建（无 manifest 扩展）结果不变。
        let dir = tempfile::tempdir().unwrap();
        let run = |e: &Engine, old_snap: u64| {
            // 旧快照：只应见文件1（1..=50），文件2（101..=150）整段 seq > 快照 → 剪枝
            assert!(e.get_at(5, old_snap).unwrap().is_some(), "文件1 行在快照内");
            assert!(e.get_at(101, old_snap).unwrap().is_none(), "文件2 行在快照后应不可见");
            assert!(e.get_at(1, old_snap).unwrap().is_some());
            // 最新视图（MAX）：全部可见
            assert!(e.get_at(150, u64::MAX).unwrap().is_some());
            assert_eq!(e.count_all_docs().unwrap(), 100);
            // 最新流式全扫：文件2 贡献（MAX 不剪枝）
            let mut n = 0u64;
            e.scan_stream(None, None, |_d, _v| {
                n += 1;
                Ok(true)
            })
            .unwrap();
            assert_eq!(n, 100);
        };
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=50u64 {
            e.put(i, format!("v-{i}").into_bytes(), &["t"]).unwrap();
        }
        e.flush_primary().unwrap(); // 文件1
        // 旧快照（在文件2 写入前）：
        let mut t = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        let old_snap = t.snapshot();
        e.txn_rollback(t);
        for i in 101..=150u64 {
            e.put(i, format!("v-{i}").into_bytes(), &["t"]).unwrap();
        }
        e.flush_primary().unwrap(); // 文件2（全部行 seq > old_snap）
        run(&e, old_snap);
        drop(e);
        // 重开：seq_min 记忆清空 → 首次快照读惰性 keys-only 推导重建
        let e2 = Engine::open(dir.path(), &cfg()).unwrap();
        run(&e2, old_snap);
    }

    #[test]
    fn batch_get_matches_get_with_delta_and_deletion_bitmap() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 0..40u64 {
            let doc = serde_json::json!({"k": format!("d{i}"), "n": i});
            e.put(i, serde_json::to_vec(&doc).unwrap(), &["t"]).unwrap();
        }
        // Delta 字段级覆盖（patch）+ 删除位图删除
        e.patch(1, &[("k", serde_json::Value::String("patched".into()))]).unwrap();
        e.patch(2, &[("extra", serde_json::Value::from(42))]).unwrap();
        e.delete(4).unwrap();
        let ids: Vec<u64> = (0..45).filter(|i| i % 2 == 0).collect();
        let batch = e.batch_get(&ids).unwrap();
        assert_eq!(batch.len(), ids.len());
        for (i, &d) in ids.iter().enumerate() {
            let single = e.get(d).unwrap();
            assert_eq!(batch[i], single, "docid {d} batch_get 与 get 结果不一致");
        }
        // 删除语义：docid 4 在两条路径均为 None
        assert!(e.get(4).unwrap().is_none());
        let idx4 = ids.iter().position(|&x| x == 4).unwrap();
        assert!(batch[idx4].is_none());
    }

    #[test]
    fn search_term_paged_batch_backfill_matches_individual_get() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 0..30u64 {
            let doc = serde_json::json!({"city": "beijing", "i": i});
            e.put(i, serde_json::to_vec(&doc).unwrap(), &["city=beijing"]).unwrap();
        }
        // 倒排回表：批量路径应与逐条 get 一致（含分页 offset/limit 语义）
        let p1 = e.search_term_paged("city=beijing", Some(10), 5).unwrap();
        assert_eq!(p1.total, 30);
        assert_eq!(p1.rows.len(), 10);
        for (d, v) in &p1.rows {
            assert_eq!(e.get(*d).unwrap().unwrap(), *v, "docid {d} 回表值不一致");
        }
        let p2 = e.search_term_paged("city=beijing", Some(5), 25).unwrap();
        assert_eq!(p2.rows.len(), 5, "末页应返回剩余 5 行");
        // 删除后回表应过滤（Tombstone / 删除位图）
        e.delete(3).unwrap();
        let p3 = e.search_term_paged("city=beijing", None, 0).unwrap();
        assert_eq!(p3.total, 30, "posting 含已删 docid（回表过滤）");
        assert!(!p3.rows.iter().any(|(d, _)| *d == 3), "已删 docid 不应回表");
    }

    // ---------- P 项：事件驱动自动 Compaction ----------

    fn p_compact_cfg() -> Config {
        let mut c = cfg();
        c.storage.auto_compact = true;
        c.storage.l0_stall_min = 2;
        c.storage.l0_stall_max = 3;
        c.storage.l0_stall_threshold = 2; // L0 > 2 即触发
        c.memtable.max_size_mb = 1; // 1MB MemTable → 快速多次 flush
        c.storage.group_commit_us = 2000; // 组提交避免逐条 fsync 拖慢测试
        c
    }

    fn fill_small_docs(e: &mut Engine, rounds: u32, per_round: u32) {
        let mut id = 0u64;
        for _ in 0..rounds {
            for _ in 0..per_round {
                let doc = serde_json::json!({"k": id, "c": "x".repeat(8000)});
                e.put(id, serde_json::to_vec(&doc).unwrap(), &[]).unwrap();
                id += 1;
            }
        }
    }

    #[test]
    fn auto_compact_keeps_l0_bounded_on_flush() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &p_compact_cfg()).unwrap();
        // ~6MB 数据 → 多次 MemTable flush → 写路径自触发合并收敛 L0
        fill_small_docs(&mut e, 12, 64);
        assert!(
            e.primary_l0_count() <= 3,
            "auto_compact 应把 L0 收敛到阈值内（实际 {}）",
            e.primary_l0_count()
        );
        assert!(!e.needs_compact(), "收敛后不应再需要合并");
        assert!(e.get(0).unwrap().is_some(), "数据应完整可读");
        assert!(e.get(12 * 64 - 1).unwrap().is_some());
    }

    #[test]
    fn background_trigger_sets_pending_and_readlock_compact_converges() {
        // O 项第③步：挂载后台 worker（compact_worker=true）后，写路径只置信号不阻塞；
        // Engine::compact 可在 &self（读锁语义）下执行并收敛 L0。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &p_compact_cfg()).unwrap();
        e.compact_worker.store(true, Ordering::Release);
        fill_small_docs(&mut e, 12, 64);
        assert!(
            e.compact_pending.load(Ordering::Acquire),
            "写路径应置 pending 信号（后台合并触发）"
        );
        assert!(e.needs_compact(), "L0 超阈值应判需要合并");
        // 后台语义：合并经 &Engine（读锁）执行——验证 &self 路径收敛
        let eng = &e;
        let _ = eng.compact().unwrap();
        assert!(!eng.needs_compact(), "读锁合并后应收敛");
        assert!(eng.get(0).unwrap().is_some(), "数据应完整可读");
        assert!(eng.get(12 * 64 - 1).unwrap().is_some());
    }

    #[test]
    fn ex813_inverted_write_accounting_and_io_budget() {
        // Ex-8.13 切片 1：倒排 seg 写盘统一记账（inverted_written_bytes）+ 后台 IO 预算
        // 接线（attach/set_io_rate_bytes 不改变写入语义；默认 rate0=不启用）
        // 1) 默认无预算：flush 后写盘字节 >0（记账累计）
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        assert_eq!(e.inverted_written_bytes(), 0);
        for i in 0..500u64 {
            let d = format!("{{\"a\":{i}}}").into_bytes();
            e.put_nosync(i, d, &["a=1"]).unwrap();
        }
        e.flush_inverted().unwrap();
        let w0 = e.inverted_written_bytes();
        assert!(w0 > 0, "倒排刷段应累计写盘字节");
        // 2) 启用预算（rate>0）attach 不报错、写入语义不变（写盘仍累计、可读回）
        let dir2 = tempfile::tempdir().unwrap();
        let mut cfg2 = crate::config::Config::default();
        cfg2.storage.io_rate_limit_mb = 1024; // 大预算：acquire 不阻塞
        let mut e2 = Engine::open(dir2.path(), &cfg2).unwrap();
        for i in 0..500u64 {
            let d = format!("{{\"a\":{i}}}").into_bytes();
            e2.put_nosync(i, d, &["a=1"]).unwrap();
        }
        e2.flush_inverted().unwrap();
        assert!(e2.inverted_written_bytes() > 0, "预算开启下刷段仍记账");
        assert!(e2.get(100).unwrap().is_some(), "写路径不受预算影响");
        // 3) 再次刷段 → 记账单调累计（多次 seg 写盘字节和）
        e2.put_nosync(999, b"{}".to_vec(), &["a=1"]).unwrap();
        e2.flush_inverted().unwrap();
        assert!(e2.inverted_written_bytes() > w0);
    }

    #[test]
    fn metrics_count_reads_writes_flush_compact() {
        // X 项：读写操作计数 + 延迟直方图 + flush/compact 计数埋点
        let dir = tempfile::tempdir().unwrap();
        let mut c = p_compact_cfg();
        c.memtable.max_size_mb = 1; // 小 MemTable → 写入过程触发 flush
        let mut e = Engine::open(dir.path(), &c).unwrap();
        assert_eq!(e.metrics.read_ops.load(Ordering::Relaxed), 0);
        assert_eq!(e.metrics.write_ops.load(Ordering::Relaxed), 0);
        for i in 0..64u64 {
            e.put(i, format!("v{i}").into_bytes(), &["t"]).unwrap();
        }
        assert!(e.metrics.write_ops.load(Ordering::Relaxed) >= 64, "put 应计数");
        let _ = e.get(0).unwrap();
        assert_eq!(e.metrics.read_ops.load(Ordering::Relaxed), 1, "get 应计数");
        // flush：64 条 × ~12B 远小于 1MB → 显式刷盘触发计数
        let f0 = e.total_flush_count();
        e.flush_primary().unwrap();
        assert!(e.total_flush_count() >= f0 + 1, "flush 应计数");
        // 延迟直方图：64 次 put 记录延迟（get 命中 hotcache 提前返回不计延迟）
        let sum: u64 = e
            .metrics
            .latency_buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .sum();
        assert!(sum >= 64, "put 应记录延迟（实际 {sum}）");
        // compact 计数
        let c0 = e.metrics.compact_count.load(Ordering::Relaxed);
        if e.needs_compact() {
            let _ = e.compact().unwrap();
            assert!(
                e.metrics.compact_count.load(Ordering::Relaxed) >= c0 + 1,
                "compact 应计数"
            );
        }
    }

    #[test]
    fn auto_compact_off_leaves_l0_accumulated() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = p_compact_cfg();
        c.storage.auto_compact = false;
        let mut e = Engine::open(dir.path(), &c).unwrap();
        fill_small_docs(&mut e, 12, 64);
        assert!(
            e.primary_l0_count() >= 4,
            "关闭 auto_compact 后 L0 应累积（实际 {}）",
            e.primary_l0_count()
        );
        assert!(e.needs_compact(), "L0 超阈值应判需要合并");
    }

    #[test]
    fn rwlock_concurrent_reads_and_writes() {
        // O 项第②步：Engine 跨线程共享（Arc<RwLock<Engine>>）——多读线程读锁并行 +
        // 写线程写锁互斥；验证 SstReader Sync 化后读路径无数据竞争。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 0..100u64 {
            e.put(i, format!("v{i}").into_bytes(), &[]).unwrap();
        }
        let engine = std::sync::Arc::new(std::sync::RwLock::new(e));
        let mut handles = Vec::new();
        // 4 个读线程：各自并发点查固定 docid（读读并行）
        for t in 0..4u64 {
            let eng = engine.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..200 {
                    let g = eng.read().unwrap();
                    let v = g.get(t).unwrap().expect("读线程应命中");
                    assert_eq!(v, format!("v{t}").into_bytes());
                }
            }));
        }
        // 1 个写线程：并发写入新 docid（写锁互斥）
        let w = engine.clone();
        handles.push(std::thread::spawn(move || {
            for i in 100..110u64 {
                let mut g = w.write().unwrap();
                g.put(i, format!("w{i}").into_bytes(), &[]).unwrap();
            }
        }));
        for h in handles {
            h.join().unwrap();
        }
        // 写线程提交后可见
        let g = engine.read().unwrap();
        assert!(g.get(105).unwrap().is_some());
        assert!(g.get(4).unwrap().is_some());
    }

    #[test]
    fn txn_rr_write_conflict_detected_on_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v0".to_vec(), &["t"]).unwrap();
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        e.put(1, b"v1".to_vec(), &["t"]).unwrap(); // 并发事务在快照后修改 docid=1
        txn.put(1, b"txn-write".to_vec(), vec![]);
        assert!(
            e.txn_commit(txn).is_err(),
            "RR 提交应因写写冲突 abort（last_write_seq > snapshot）"
        );
        // 冲突 abort 后引擎保持并发写结果，且锁已释放
        assert_eq!(e.get(1).unwrap().unwrap(), b"v1");
        assert_eq!(e.txn_locks.lock().unwrap().lock_count(), 0, "abort 后锁应全部释放");
    }

    #[test]
    fn txn_rc_reads_latest_committed() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v0".to_vec(), &["t"]).unwrap();
        let mut txn = e.txn_begin(crate::txn::Isolation::ReadCommitted);
        e.put(1, b"v1".to_vec(), &["t"]).unwrap();
        let got = e.txn_get(&mut txn, 1).unwrap();
        assert_eq!(got.unwrap(), b"v1", "RC 应读最新已提交版本");
        e.txn_rollback(txn);
    }

    #[test]
    fn txn_serializable_read_lock_blocks_concurrent_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v0".to_vec(), &["t"]).unwrap();
        // txn1 读 docid=1（共享锁，SERIALIZABLE 持有至提交）
        let mut t1 = e.txn_begin(crate::txn::Isolation::Serializable);
        let v = e.txn_get(&mut t1, 1).unwrap();
        assert_eq!(v.unwrap(), b"v0");
        // txn2 写 docid=1 → 共享读锁未释放 → 排他请求冲突
        let mut t2 = e.txn_begin(crate::txn::Isolation::Serializable);
        t2.put(1, b"v2".to_vec(), vec![]);
        assert!(e.txn_commit(t2).is_err(), "SERIALIZABLE 读锁持有期间排他写应冲突");
        // txn1 提交释放读锁后，新事务可写
        e.txn_commit(t1).unwrap();
        let mut t3 = e.txn_begin(crate::txn::Isolation::Serializable);
        t3.put(1, b"v3".to_vec(), vec![]);
        e.txn_commit(t3).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"v3");
    }

    #[test]
    fn txn_serializable_upgrades_read_lock_on_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(1, b"v0".to_vec(), &["t"]).unwrap();
        // 同一事务先读后写同一 docid：共享 → 排他升级（2PL 合法）
        let mut t1 = e.txn_begin(crate::txn::Isolation::Serializable);
        let v = e.txn_get(&mut t1, 1).unwrap();
        assert_eq!(v.unwrap(), b"v0");
        t1.put(1, b"v1".to_vec(), vec![]);
        e.txn_commit(t1).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"v1");
    }

    #[test]
    fn txn_deadlock_detected_at_engine_level() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        // 通过锁表构造 wait-for 环：txn9001 持 10 等 20；txn9002 持 20 等 10
        e.txn_locks.lock().unwrap().acquire_exclusive(9001, 10).unwrap();
        e.txn_locks.lock().unwrap().acquire_exclusive(9002, 20).unwrap();
        let r1 = e.txn_locks.lock().unwrap().acquire_exclusive(9001, 20);
        assert!(matches!(r1, Err(crate::error::Error::TxnConflict(_))), "无环 → 冲突");
        let r2 = e.txn_locks.lock().unwrap().acquire_exclusive(9002, 10);
        assert!(
            matches!(r2, Err(crate::error::Error::TxnDeadlock(_))),
            "环 → 检测死锁（victim abort）"
        );
        e.txn_locks.lock().unwrap().release(9001);
        e.txn_locks.lock().unwrap().release(9002);
        assert_eq!(e.txn_locks.lock().unwrap().lock_count(), 0);
    }

    #[test]
    fn txn_delete_and_mixed_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        e.put(5, b"keep".to_vec(), &["t"]).unwrap();
        e.put(6, b"del".to_vec(), &["t"]).unwrap();
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        txn.delete(6);
        txn.put(7, b"new".to_vec(), vec!["t".into()]);
        e.txn_commit(txn).unwrap();
        assert!(e.get(6).unwrap().is_none(), "事务删除生效");
        assert_eq!(e.get(7).unwrap().unwrap(), b"new");
        assert_eq!(e.get(5).unwrap().unwrap(), b"keep");
    }

    #[test]
    fn txn_rollback_leaves_no_trace() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        let mut txn = e.txn_begin(crate::txn::Isolation::RepeatableRead);
        txn.put(1, b"x".to_vec(), vec![]);
        txn.delete(2);
        e.txn_rollback(txn);
        assert!(e.get(1).unwrap().is_none(), "回滚后无写入");
        assert_eq!(e.txn_locks.lock().unwrap().lock_count(), 0, "回滚释放全部锁");
    }

    #[test]
    fn txn_snapshot_advances_with_committed_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        let s0 = e.txn_begin(crate::txn::Isolation::RepeatableRead).snapshot();
        e.put(1, b"v1".to_vec(), &["t"]).unwrap();
        let s1 = e.txn_begin(crate::txn::Isolation::RepeatableRead).snapshot();
        assert!(s1 > s0, "提交后新事务快照 seq 应推进");
        // 旧快照仍读到写前状态（历史版本由 WAL/MemTable seq 过滤保证）
        let got = e.get_at(1, s0).unwrap();
        assert!(got.is_none(), "旧快照点 docid=1 尚不存在");
        assert_eq!(e.get_at(1, s1).unwrap().unwrap(), b"v1");
    }

    // ---------- 组提交（M8） ----------

    #[test]
    fn group_commit_default_disabled_persists_each_put() {
        // 默认 group_commit_us=0：put 逐条 fsync，drop 后重开数据完整
        let dir = tempfile::tempdir().unwrap();
        {
            let mut e = Engine::open(dir.path(), &cfg()).unwrap();
            assert!(e.group_commit.is_none(), "默认应关闭组提交");
            for i in 0..50u64 {
                e.put(i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
            }
        }
        let mut e2 = Engine::open(dir.path(), &cfg()).unwrap();
        assert_eq!(e2.get(49).unwrap().unwrap(), b"doc-49");
    }

    // ---------- 批量导入模式（P40） ----------

    #[test]
    fn bulk_import_skips_hotcache() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        // 默认：写后回填 HotCache
        e.put_nosync(1, b"doc-1".to_vec(), &["t"]).unwrap();
        assert_eq!(e.hotcache.len(), 1, "默认写后应回填热缓存");
        // 批量导入模式：只写不读，跳过回填（P40 防缓存膨胀挤爆内存）
        e.set_bulk_import(true);
        e.put_nosync(2, b"doc-2".to_vec(), &["t"]).unwrap();
        assert_eq!(e.hotcache.len(), 1, "批量导入模式不应回填热缓存");
        // 关闭后恢复常规语义
        e.set_bulk_import(false);
        e.put_nosync(3, b"doc-3".to_vec(), &["t"]).unwrap();
        assert_eq!(e.hotcache.len(), 2, "关闭后应恢复回填");
        // 主数据不受影响：批量写入的文档仍可正常读取
        e.set_bulk_import(true);
        e.put_nosync(4, b"doc-4".to_vec(), &["t"]).unwrap();
        assert_eq!(e.hotcache.len(), 2);
        assert_eq!(e.get(4).unwrap().unwrap(), b"doc-4");
    }

    // ---------- fulltext 分词索引（M8-P7） ----------

    #[test]
    fn fulltext_search_finds_long_text_and_persists() {
        // 核心动机：>96B 长文本整串被 max_term_len 跳过，分词词 term 短 → 长文本可检索
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.inverted.fulltext_fields = vec!["big_text".into()];
        let val = serde_json::json!({"docid": 42, "status": "active", "big_text": format!("{:<300}", "rec-00000042-msg-777")});
        let ft = c.inverted.fulltext_fields.iter().cloned().collect();
        let terms =
            crate::server::extract_terms_with_fulltext(&val, None, Some(&ft));
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        {
            let mut e = Engine::open(dir.path(), &c).unwrap();
            e.put(42, serde_json::to_vec(&val).unwrap(), &t).unwrap();
            e.flush_inverted().unwrap();
            // 词 term 可检索（整串 300B > max_term_len=96 会被跳过，词 term 不受影响）
            assert_eq!(e.fulltext_search("big_text", "rec").unwrap().len(), 1);
            assert_eq!(e.fulltext_search("big_text", "777").unwrap().len(), 1);
            // 非 fulltext 字段整串 term 照常
            assert_eq!(e.inverted_doc_count("status=active").unwrap(), 1);
        }
        // 刷盘 + 重开：词 term 持久化可查（倒排段已落盘）
        let mut e2 = Engine::open(dir.path(), &c).unwrap();
        assert_eq!(e2.fulltext_search("big_text", "777").unwrap().len(), 1);
        let hits = e2.fulltext_search("big_text", "00000042").unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, 42);
    }

    #[test]
    fn cjk_fulltext_searchable_via_bigram() {
        // M8-P9：中文整串不再当单 token（无法检索）——bigram 分词后 2-4 字关键词可检索
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.inverted.fulltext_fields = vec!["content".into()];
        let mut e = Engine::open(dir.path(), &c).unwrap();
        let docs = [
            (1u64, "山水存迹数据库存储引擎"),
            (2u64, "基于Rust的LSM树文档数据库"),
            (3u64, "分布式缓存系统设计"),
        ];
        for (id, content) in docs {
            let val = serde_json::json!({"docid": id, "content": content});
            let ft = e.fulltext_fields().clone();
            let terms = crate::server::extract_terms_with_fulltext(&val, None, Some(&ft));
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put_nosync(id, serde_json::to_vec(&val).unwrap(), &t).unwrap();
        }
        e.flush_inverted().unwrap();

        // 3 字关键词"数据库" → bigram 数据/据库（AND 交集精确命中含该词的文档 1、2）
        let d1 = e.inverted_posting("ft:content:数据").unwrap();
        let d2 = e.inverted_posting("ft:content:据库").unwrap();
        let and = d1 & d2;
        assert_eq!(and.len(), 2);
        assert!(and.contains(1) && and.contains(2));
        // 4 字关键词"山水存迹" → 3 个 bigram AND → 只命中 doc 1
        let inter = e.inverted_posting("ft:content:山水").unwrap()
            & e.inverted_posting("ft:content:水存").unwrap()
            & e.inverted_posting("ft:content:存迹").unwrap();
        assert_eq!(inter.len(), 1);
        assert!(inter.contains(1));
        // fulltext_search 回表
        let hits = e.fulltext_search("content", "数据").unwrap();
        assert!(hits.iter().any(|(id, _)| *id == 1 || *id == 2));
    }

    #[test]
    fn fulltext_survives_inverted_whitelist() {
        // M8-P4/P7 正交：inverted_fields 白名单非空时，ft: 词 term 不受白名单过滤
        // （否则长文本分词索引被白名单误滤，fulltext 检索恒空）
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.inverted.inverted_fields = vec!["status".into()]; // 白名单只建 status
        c.inverted.fulltext_fields = vec!["content".into()];
        let mut e = Engine::open(dir.path(), &c).unwrap();
        let val = serde_json::json!({"docid": 1, "status": "active", "content": "山水存迹"});
        let ft = e.fulltext_fields().clone();
        let terms = crate::server::extract_terms_with_fulltext(&val, None, Some(&ft));
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        e.put_nosync(1, serde_json::to_vec(&val).unwrap(), &t).unwrap();
        e.flush_inverted().unwrap();
        // 白名单字段 term 建了；ft: 词 term 也建了（不被滤掉）
        assert!(e.inverted_posting("status=active").unwrap().contains(1));
        assert!(e.inverted_posting("ft:content:山水").unwrap().contains(1));
        assert_eq!(e.fulltext_search("content", "山水").unwrap().len(), 1);
    }

    #[test]
    fn jieba_fulltext_meaningful_word_hit() {
        // M8-P13：jieba 词典分词——"数据库"单 term 精确命中（bigram 需 数据+据库 AND）
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.inverted.fulltext_fields = vec!["content".into()];
        c.inverted.cjk_segmenter = "jieba".into();
        let mut e = Engine::open(dir.path(), &c).unwrap();
        assert!(e.use_jieba(), "cjk_segmenter=jieba 应启用 jieba 分词");
        let docs = [
            (1u64, "山水存迹数据库存储引擎"),
            (2u64, "基于Rust的LSM树文档数据库"),
            (3u64, "分布式缓存系统设计"),
        ];
        for (id, content) in docs {
            let val = serde_json::json!({"docid": id, "content": content});
            let ft = e.fulltext_fields().clone();
            let terms =
                crate::server::extract_terms_with_fulltext_seg(&val, None, Some(&ft), e.use_jieba());
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put_nosync(id, serde_json::to_vec(&val).unwrap(), &t).unwrap();
        }
        e.flush_inverted().unwrap();
        // jieba 词典词"数据库"单 term 精确命中 2 个文档
        assert_eq!(e.inverted_posting("ft:content:数据库").unwrap().len(), 2);
        let hits = e.fulltext_search("content", "数据库").unwrap();
        assert_eq!(hits.len(), 2);
        // 无该词 → 0
        assert_eq!(e.fulltext_search("content", "缓存系统").unwrap().len(), 0);
    }

    // ---------- 分页查询（M8-P8） ----------

    #[test]
    fn paged_queries_semantics_and_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.inverted.fulltext_fields = vec!["big_text".into()];
        let mut e = Engine::open(dir.path(), &c).unwrap();
        // 100 docs：status 三态（active 34，docid 等差 3），big_text 每行唯一 token
        for i in 0..100u64 {
            let status = ["active", "inactive", "pending"][(i % 3) as usize];
            let val = serde_json::json!({
                "docid": i,
                "status": status,
                "big_text": format!("rec-{i:08}-msg-{i}"),
            });
            let ft = e.fulltext_fields().clone();
            let terms = crate::server::extract_terms_with_fulltext(&val, None, Some(&ft));
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put_nosync(i, serde_json::to_vec(&val).unwrap(), &t).unwrap();
        }
        e.flush_inverted().unwrap();

        // 全量 total + 行数
        let all = e.search_term_paged("status=active", None, 0).unwrap();
        assert_eq!(all.total, 34);
        assert_eq!(all.rows.len(), 34);
        // 分页：total 恒为全量命中数，rows 只含当前页，docid 升序接续
        let p1 = e.search_term_paged("status=active", Some(10), 0).unwrap();
        let p2 = e.search_term_paged("status=active", Some(10), 10).unwrap();
        assert_eq!(p1.total, 34);
        assert_eq!(p1.rows.len(), 10);
        assert_eq!(p2.rows.len(), 10);
        assert_eq!(p2.rows[0].0, p1.rows[9].0 + 3, "active docid 等差 3 接续");
        // 拼接 == 全量（有序稳定）
        let mut merged = p1.rows.clone();
        merged.extend(p2.rows);
        for off in [20u64, 30] {
            let p = e.search_term_paged("status=active", Some(10), off).unwrap();
            merged.extend(p.rows);
        }
        assert_eq!(merged.len(), 34);
        for (a, b) in merged.iter().zip(all.rows.iter()) {
            assert_eq!(a.0, b.0, "分页拼接必须与全量一致");
        }
        // 边界：limit=0 → 空页 total 不变；offset > total → 空页；limit > total → 全部
        let z = e.search_term_paged("status=active", Some(0), 0).unwrap();
        assert_eq!(z.total, 34);
        assert!(z.rows.is_empty());
        let o = e.search_term_paged("status=active", Some(5), 100).unwrap();
        assert!(o.rows.is_empty());
        let big = e.search_term_paged("status=active", Some(10_000), 0).unwrap();
        assert_eq!(big.rows.len(), 34);

        // fulltext 分页同语义
        let ft = e.fulltext_search_paged("big_text", "rec", Some(10), 90).unwrap();
        assert_eq!(ft.total, 100);
        assert_eq!(ft.rows.len(), 10);
        assert_eq!(ft.rows[0].0, 90);
        // scan 分页
        let sc = e.scan_range_paged(Some(10), Some(50), Some(5), 0).unwrap();
        assert_eq!(sc.total, 41);
        assert_eq!(sc.rows.len(), 5);
        assert_eq!(sc.rows[0].0, 10);
    }

    #[test]
    fn scan_after_cursor_traversal() {
        // M8-P11：游标续扫——遍历一致性 / 边界 / 提前终止（无 total 全扫）
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=100u64 {
            let val = serde_json::json!({"docid": i, "v": i});
            e.put_nosync(i, serde_json::to_vec(&val).unwrap(), &[]).unwrap();
            if i % 25 == 0 {
                e.flush_primary().unwrap();
            }
        }
        e.flush_primary().unwrap();
        // 游标遍历：limit=10 逐页续扫，拼接 == 全量升序
        let mut merged: Vec<u64> = Vec::new();
        let mut after: Option<u64> = None;
        loop {
            let page: Vec<u64> = e
                .scan_after(after, None, 10)
                .unwrap()
                .into_iter()
                .map(|(d, _)| d)
                .collect();
            if page.is_empty() {
                break;
            }
            merged.extend(page.iter().copied());
            after = Some(*page.last().unwrap());
        }
        assert_eq!(merged.len(), 100);
        assert_eq!(merged, (1..=100).collect::<Vec<u64>>(), "游标遍历覆盖全部且升序");
        // 边界：after=0 → 从 1 起；after=100 → 空；尾部不足 limit
        let from0: Vec<u64> = e
            .scan_after(Some(0), None, 3)
            .unwrap()
            .into_iter()
            .map(|(d, _)| d)
            .collect();
        assert_eq!(from0, vec![1, 2, 3]);
        assert!(e.scan_after(Some(100), None, 10).unwrap().is_empty());
        assert_eq!(
            e.scan_after(Some(97), None, 10).unwrap().len(),
            3,
            "尾部不足一页取剩余"
        );
        // 上界限定：after=0 & end=50 → 1..=50
        let bounded: Vec<u64> = e
            .scan_after(Some(0), Some(50), 100)
            .unwrap()
            .into_iter()
            .map(|(d, _)| d)
            .collect();
        assert_eq!(bounded, (1..=50).collect::<Vec<u64>>());
    }

    #[test]
    fn group_commit_drop_persists_all() {
        // 开启 2ms 窗口：快速 put 全部攒批，drop 最终落盘 → 重开数据完整
        let dir = tempfile::tempdir().unwrap();
        {
            let mut e = Engine::open(dir.path(), &gc_cfg(2_000)).unwrap();
            assert!(e.group_commit.is_some());
            for i in 0..100u64 {
                e.put(i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
            }
        }
        let mut e2 = Engine::open(dir.path(), &cfg()).unwrap();
        assert_eq!(e2.get(99).unwrap().unwrap(), b"doc-99");
        assert_eq!(e2.get(0).unwrap().unwrap(), b"doc-0");
    }

    #[test]
    fn gc_thread_flushes_tail_without_new_writes() {
        // 后台兜底线程：单条写后无新写，窗口到期也应落盘（WAL 待刷缓冲清零）
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &gc_cfg(2_000)).unwrap();
        e.put(1, b"doc-1".to_vec(), &["t"]).unwrap();
        // 窗口内第 1 条通常不触发 fsync（有待刷缓冲）；有界轮询等后台线程兜底落盘
        // （并行测试负载下后台线程调度可能延迟，固定 sleep 易 flaky）
        let mut pending = 1usize;
        for _ in 0..200 {
            std::thread::sleep(std::time::Duration::from_millis(10));
            pending = e.primary.wal_handle().lock().unwrap().pending_bytes()
                + e.delta.wal_handle().lock().unwrap().pending_bytes();
            if pending == 0 {
                break;
            }
        }
        assert_eq!(pending, 0, "后台线程应在窗口内兜底落盘");
    }

    #[test]
    fn backup_incremental_flushes_before_export() {
        // 组提交开启下 backup_incremental 前置 flush：未手动 flush 也应导出全部记录
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &gc_cfg(2_000)).unwrap();
        for i in 0..3u64 {
            e.put(i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
        }
        let bak = dir.path().join("incr.json");
        let rep = e.backup_incremental(0, &bak).unwrap();
        assert_eq!(rep.records, 3, "组提交开启下备份应前置落盘并导出全部");
    }

    #[test]
    fn group_commit_window_batches_fsync() {
        // 行为验证：窗口内连续 put 不逐条 fsync（WAL 待刷字节在窗口内累积，
        // 直到窗口到期或字节阈值触发一次性落盘）。写 100 条后窗口未到期时缓冲非空。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &gc_cfg(60_000)).unwrap(); // 60ms 大窗口
        let mut synced = 0usize;
        let mut missed = 0usize;
        for i in 0..50u64 {
            e.put(i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
            let pending = e.primary.wal_handle().lock().unwrap().pending_bytes();
            if pending == 0 {
                synced += 1; // 已落盘（窗口到期/阈值触发）
            } else {
                missed += 1; // 攒批中（未 fsync）
            }
        }
        // 60ms 窗口 + 4KB 阈值：快速 50 条 put（远快于窗口）绝大部分应攒批
        assert!(missed >= 40, "窗口内应攒批（攒批 {missed}，同步 {synced}）");
    }

    // ---------- 倒排字段白名单/黑名单/长文本（M8-P4） ----------

    #[test]
    fn inverted_whitelist_only_indexes_declared_fields() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.inverted.inverted_fields = vec!["status".into(), "city".into()];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let doc = json!({"docid": 1, "status": "active", "city": "beijing", "name": "alice"});
        let terms = crate::server::extract_terms(&doc);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        e.put(1, serde_json::to_vec(&doc).unwrap(), &t).unwrap();
        e.flush_inverted().unwrap();
        // 白名单字段可查
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 1);
        assert_eq!(e.inverted_doc_count("city=beijing").unwrap(), 1);
        // 非白名单字段不建倒排
        assert_eq!(e.inverted_doc_count("name=alice").unwrap(), 0);
    }

    #[test]
    fn inverted_exclude_skips_fields() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.inverted.exclude_fields = vec!["big_text".into()];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let doc = json!({"docid": 1, "status": "active", "big_text": "hello world"});
        let terms = crate::server::extract_terms(&doc);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        e.put(1, serde_json::to_vec(&doc).unwrap(), &t).unwrap();
        e.flush_inverted().unwrap();
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 1);
        assert_eq!(
            e.inverted_doc_count("big_text=hello world").unwrap(),
            0,
            "黑名单字段不建倒排"
        );
    }

    #[test]
    fn inverted_max_term_len_skips_long_text() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.inverted.max_term_len = 16; // 16 字节以上 term 自动跳过（长文本整串保护）
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let long = "x".repeat(100);
        let doc = json!({"docid": 1, "status": "active", "payload": long});
        let terms = crate::server::extract_terms(&doc);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        e.put(1, serde_json::to_vec(&doc).unwrap(), &t).unwrap();
        e.flush_inverted().unwrap();
        assert_eq!(
            e.inverted_doc_count("status=active").unwrap(),
            1,
            "短字段仍建"
        );
        assert_eq!(
            e.inverted_doc_count("payload=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx").unwrap(),
            0,
            "超长 term 自动跳过（防长文本膨胀）"
        );
    }

    #[test]
    fn inverted_default_all_fields_built() {
        // 默认配置（白名单空）：短字符串字段全建；仅超长 term（>96）自动跳过
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        let doc = json!({"docid": 1, "status": "active", "city": "beijing"});
        let terms = crate::server::extract_terms(&doc);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        e.put(1, serde_json::to_vec(&doc).unwrap(), &t).unwrap();
        e.flush_inverted().unwrap();
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 1);
        assert_eq!(e.inverted_doc_count("city=beijing").unwrap(), 1);
    }

    #[test]
    fn put_get_roundtrip() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"doc-1".to_vec(), &["rust"]).unwrap();
        e.put(2, b"doc-2".to_vec(), &["go"]).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"doc-1");
        assert_eq!(e.get(2).unwrap().unwrap(), b"doc-2");
        assert!(e.get(99).unwrap().is_none());
    }

    #[test]
    fn put_batch_atomic_batch_visible_and_indexed() {
        // put_batch：批量原子提交——全部可见、倒排可查、覆盖语义正确（D 项 WriteBatch 前置）
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        let items: Vec<(u64, Vec<u8>, Vec<String>)> = (1..=100u64)
            .map(|i| {
                (
                    i,
                    format!("doc-{i}").into_bytes(),
                    vec![format!("status={}", if i % 2 == 0 { "active" } else { "inactive" })],
                )
            })
            .collect();
        e.put_batch(&items).unwrap();
        // 全部可见
        assert_eq!(e.get(1).unwrap().unwrap(), b"doc-1");
        assert_eq!(e.get(100).unwrap().unwrap(), b"doc-100");
        assert!(e.get(101).unwrap().is_none());
        // 倒排可查（查询自动刷 pending）
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 50);
        assert_eq!(e.inverted_doc_count("status=inactive").unwrap(), 50);
        // 覆盖：同批内后写覆盖前写
        let overwrite: Vec<(u64, Vec<u8>, Vec<String>)> =
            vec![(1, b"doc-1-v2".to_vec(), vec![])];
        e.put_batch(&overwrite).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"doc-1-v2");
    }

    #[test]
    fn inverted_batch_pending_flush() {
        // Ex-5.3：put 攒批缓冲——未达阈值时 term 留在 pending，查询自动刷入（一致性）；
        // inverted_mem_docids 统计含 pending；flush_inverted 后落盘、统计归零。
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        for i in 0..100u64 {
            e.put(i, b"v".to_vec(), &["status=active"]).unwrap();
        }
        // 未达阈值（8192）→ pending 未刷入，但统计应含 pending
        assert_eq!(e.inverted_mem_docids(), 100, "统计应含攒批缓冲");
        // 查询自动刷入 → 立即可见
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 100);
        // 查询后 pending 已清空
        assert_eq!(e.pending_inverted.lock().unwrap().len(), 0, "查询后缓冲应清空");
        // flush_inverted 落盘：内存归零、计数仍正确
        e.flush_inverted().unwrap();
        assert_eq!(e.inverted_mem_docids(), 0, "落盘后内存统计归零");
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 100);
    }

    #[test]
    fn delete_hides_doc() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(7, b"x".to_vec(), &["k"]).unwrap();
        e.delete(7).unwrap();
        assert!(e.get(7).unwrap().is_none());
    }

    #[test]
    fn search_term_returns_docs() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"about rust".to_vec(), &["rust"]).unwrap();
        e.put(2, b"rust rocks".to_vec(), &["rust"]).unwrap();
        e.put(3, b"go is cool".to_vec(), &["go"]).unwrap();
        let rows = e.search_term("rust").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[1].0, 2);
        assert!(e.search_term("go").unwrap().len() == 1);
        assert!(e.search_term("absent").unwrap().is_empty());
    }

    #[test]
    fn deleted_doc_excluded_from_search() {
        // 倒排残留 docid 回表时被主数据 Tombstone 过滤
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"rust".to_vec(), &["rust"]).unwrap();
        e.put(2, b"rust2".to_vec(), &["rust"]).unwrap();
        e.delete(1).unwrap();
        let rows = e.search_term("rust").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 2);
    }

    // ---------- MVCC 快照读（design 4.7 二期，M6-3） ----------

    #[test]
    fn get_at_reads_historical_version_after_flush() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        let put_doc = |e: &mut Engine, v: i64| {
            e.put(
                1,
                serde_json::to_vec(&json!({"docid": 1, "v": v})).unwrap(),
                &["v"],
            )
            .unwrap();
        };
        put_doc(&mut e, 1);
        let s1 = e.begin_snapshot();
        e.flush_primary().unwrap(); // v1 落 SST（历史版本保留）
        put_doc(&mut e, 2);
        // 快照读 → v1；最新读 → v2
        let snap: serde_json::Value =
            serde_json::from_slice(&e.get_at(1, s1).unwrap().unwrap()).unwrap();
        assert_eq!(snap["v"], 1, "快照应读回 v1");
        let cur: serde_json::Value = serde_json::from_slice(&e.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(cur["v"], 2);
    }

    #[test]
    fn get_at_ignores_writes_after_snapshot() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, br#"{"k":"base"}"#.to_vec(), &["k"]).unwrap();
        e.flush_primary().unwrap(); // base 落 SST，快照后可回读
        let s = e.begin_snapshot();
        // 快照之后主数据覆盖 + Delta 热更
        e.put(1, br#"{"k":"later"}"#.to_vec(), &["k"]).unwrap();
        e.patch(1, &[("extra", json!("x"))]).unwrap();
        // 快照读：主数据仍为 base，Delta 增量也隔离（M7-1 全局 seq）
        let snap: serde_json::Value =
            serde_json::from_slice(&e.get_at(1, s).unwrap().unwrap()).unwrap();
        assert_eq!(snap["k"], "base", "快照应隔离主数据后续覆盖");
        assert!(snap.get("extra").is_none(), "快照应隔离快照后的 Delta 热更");
        // 最新读：later + delta 叠加
        let cur: serde_json::Value = serde_json::from_slice(&e.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(cur["k"], "later");
        assert_eq!(cur["extra"], "x");
    }

    #[test]
    fn get_at_delta_isolated_by_global_seq() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, br#"{"a":1,"b":1}"#.to_vec(), &["k"]).unwrap();
        let s = e.begin_snapshot();
        // 快照后 Delta 修改字段 a（null 删除 b）
        e.patch(1, &[("a", json!(2)), ("b", json!(null))]).unwrap();
        // 快照读：a=1、b 仍存在（快照后增量不可见）
        let snap: serde_json::Value =
            serde_json::from_slice(&e.get_at(1, s).unwrap().unwrap()).unwrap();
        assert_eq!(snap["a"], 1, "快照应读回 a=1");
        assert_eq!(snap["b"], 1, "快照应保留被删除的 b");
        // 最新读：a=2、b 被 null 删除
        let cur: serde_json::Value = serde_json::from_slice(&e.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(cur["a"], 2);
        assert!(cur.get("b").is_none(), "最新读 b 应被 null 删除");
    }

    #[test]
    fn global_seq_resumes_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        e.put(1, br#"{"a":1}"#.to_vec(), &["k"]).unwrap();
        let s = e.begin_snapshot();
        e.patch(1, &[("a", json!(2))]).unwrap();
        // 重启：全局 seq 从 WAL 恢复
        drop(e);
        let mut e2 = Engine::open(dir.path(), &cfg).unwrap();
        // 快照点之前的读不受影响（重启后快照序号语义保持）
        let snap: serde_json::Value =
            serde_json::from_slice(&e2.get_at(1, s).unwrap().unwrap()).unwrap();
        assert_eq!(snap["a"], 1);
        let cur: serde_json::Value = serde_json::from_slice(&e2.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(cur["a"], 2);
        assert!(e2.begin_snapshot() >= s, "重启后全局 seq 应接续");
    }

    #[test]
    fn get_at_returns_none_after_delete_before_snapshot() {
        // Ex-5.6 语义拆分：
        // ① 删除位图开启（默认）：删除为立即/全局语义——已删 docid 在任何快照点均不可见
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"v1".to_vec(), &["k"]).unwrap();
        let s_before_delete = e.begin_snapshot();
        e.flush_primary().unwrap();
        e.delete(1).unwrap(); // 位图置位（快照点前后均不可见）
        assert!(e.get_at(1, s_before_delete).unwrap().is_none(), "位图删除对旧快照同样隐藏");
        assert!(e.get(1).unwrap().is_none());

        // ② 删除位图关闭：回退传统 Tombstone + MVCC 语义——删除前快照仍可见 v1
        let mut c = cfg();
        c.storage.deletion_bitmap_enabled = false;
        let mut e2 = Engine::open(&tmp(), &c).unwrap();
        e2.put(1, b"v1".to_vec(), &["k"]).unwrap();
        let s2 = e2.begin_snapshot();
        e2.flush_primary().unwrap();
        e2.delete(1).unwrap(); // Tombstone seq > s2
        assert_eq!(e2.get_at(1, s2).unwrap().unwrap(), b"v1", "关闭位图保留 Tombstone 快照语义");
        assert!(e2.get(1).unwrap().is_none(), "删除后最新读 → 不存在");
    }

    // ---------- 增量备份（design 20，M6-5） ----------

    #[test]
    fn incremental_backup_restore_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &crate::config::Config::default()).unwrap();
        let put_doc = |e: &mut Engine, docid: u64, v: &str| {
            let val = json!({"docid": docid, "v": v});
            let bytes = serde_json::to_vec(&val).unwrap();
            let terms = crate::server::extract_terms(&val);
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put(docid, bytes, &t).unwrap();
        };
        put_doc(&mut e, 1, "v1");
        put_doc(&mut e, 2, "v1");
        let full_point = e.current_seq(); // 全量备份点
        put_doc(&mut e, 3, "v3"); // 增量 1
        e.delete(1).unwrap(); // 增量 2（Tombstone）
        let bak = dir.path().join("incr.json");
        let rep = e.backup_incremental(full_point, &bak).unwrap();
        assert_eq!(rep.since_seq, full_point);
        assert_eq!(rep.records, 2, "应导出 2 条增量记录");

        // 模拟"全量还原"后的新引擎：先恢复全量点数据，再应用增量
        let dir2 = tempfile::tempdir().unwrap();
        let mut e2 = Engine::open(dir2.path(), &crate::config::Config::default()).unwrap();
        put_doc(&mut e2, 1, "v1");
        put_doc(&mut e2, 2, "v1");
        let n = e2.restore_incremental(&bak).unwrap();
        assert_eq!(n, 2);
        assert!(e2.get(1).unwrap().is_none(), "增量 Tombstone 应删除 doc1");
        assert!(e2.get(2).unwrap().is_some(), "doc2 保留（全量部分）");
        let v3: serde_json::Value = serde_json::from_slice(&e2.get(3).unwrap().unwrap()).unwrap();
        assert_eq!(v3["v"], "v3", "增量 PUT 应恢复 doc3");
    }

    #[test]
    fn incremental_backup_since_zero_exports_all() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &crate::config::Config::default()).unwrap();
        e.put(1, b"a".to_vec(), &["a"]).unwrap();
        e.flush_primary().unwrap(); // flush → WAL 截断（M8-P5）：已刷盘记录 1 从 WAL 删除
        e.put(2, b"b".to_vec(), &["b"]).unwrap();
        let bak = dir.path().join("incr-all.json");
        let rep = e.backup_incremental(0, &bak).unwrap();
        assert_eq!(
            rep.records, 1,
            "since=0 导出当前 WAL 全部记录（截断后 = 未刷盘记录 2；记录 1 已入 SST 由全量备份覆盖）"
        );
    }

    // ---------- Ex-9.3 第①步：写路径随 term 累积 stats（内存段） ----------

    #[test]
    fn inverted_stats_fields_accumulate_per_term() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.inverted.stats_fields = vec!["amount".to_string()];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let put = |e: &mut Engine, docid: u64, status: &str, amount: Option<f64>| {
            let mut doc = serde_json::json!({"status": status});
            if let Some(a) = amount {
                doc["amount"] = serde_json::json!(a);
            }
            let bytes = serde_json::to_vec(&doc).unwrap();
            let term: &[&str] = &[if status == "active" { "status=active" } else { "status=inactive" }];
            e.put_nosync(docid, bytes, term).unwrap();
        };
        put(&mut e, 1, "active", Some(10.0));
        put(&mut e, 2, "active", Some(20.0));
        put(&mut e, 3, "active", None); // 缺 amount → 跳过不计入
        put(&mut e, 4, "inactive", Some(5.0));
        // active：数值文档 10/20 → n2 sum30 min10 max20
        let a = e.inverted_term_stats("status=active").expect("active 应有统计");
        assert_eq!(a.len(), 1, "stats_fields 单字段");
        assert_eq!(a[0].n, 2, "缺字段文档不计入");
        assert_eq!(a[0].sum, 30.0);
        assert_eq!(a[0].min, 10.0);
        assert_eq!(a[0].max, 20.0);
        let b = e.inverted_term_stats("status=inactive").unwrap();
        assert_eq!(b[0].n, 1);
        assert_eq!(b[0].sum, 5.0);
        // 未声明 stats_fields（空）→ 不产生统计
        let dir2 = tempfile::tempdir().unwrap();
        let mut e2 = Engine::open(dir2.path(), &crate::config::Config::default()).unwrap();
        e2.put_nosync(1, br#"{"status":"active","amount":10}"#.to_vec(), &["status=active"])
            .unwrap();
        assert!(e2.inverted_term_stats("status=active").is_none(), "未配置则无统计");
    }

    #[test]
    fn inverted_stats_persist_across_flush_and_reopen_v5() {
        // Ex-9.3 第②步：stats 随段 v5 载荷落盘 → flush 后 / 重开库后仍可读（不再依赖内存）。
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.inverted.stats_fields = vec!["amount".to_string()];
        let put = |e: &mut Engine, docid: u64, status: &str, amount: Option<f64>| {
            let mut doc = serde_json::json!({"status": status});
            if let Some(a) = amount {
                doc["amount"] = serde_json::json!(a);
            }
            let bytes = serde_json::to_vec(&doc).unwrap();
            let term: &[&str] = &[if status == "active" { "status=active" } else { "status=inactive" }];
            e.put_nosync(docid, bytes, term).unwrap();
        };
        let check = |e: &mut Engine, label: &str| {
            let a = e.inverted_term_stats("status=active").expect(label);
            assert_eq!(a[0].n, 2, "{label}: active n");
            assert_eq!(a[0].sum, 30.0, "{label}: active sum");
            assert_eq!(a[0].min, 10.0, "{label}: min");
            assert_eq!(a[0].max, 20.0, "{label}: max");
            let b = e.inverted_term_stats("status=inactive").unwrap();
            assert_eq!(b[0].sum, 5.0, "{label}: inactive sum");
            assert_eq!(e.inverted_doc_count("status=active").unwrap(), 3, "{label}: count");
        };
        {
            let mut e = Engine::open(dir.path(), &cfg).unwrap();
            put(&mut e, 1, "active", Some(10.0));
            put(&mut e, 2, "active", Some(20.0));
            put(&mut e, 3, "active", None);
            put(&mut e, 4, "inactive", Some(5.0));
            check(&mut e, "flush 前（mem）");
            e.flush_inverted().unwrap(); // 段落盘 v5：含统计载荷
            check(&mut e, "flush 后（段）");
        }
        // 重开（内存清空，只读段）：载荷须从 v5 段读出
        let mut e2 = Engine::open(dir.path(), &cfg).unwrap();
        check(&mut e2, "重开后（v5 段）");
    }

    // ---------- 位图索引（design 5.2.4，M7-2） ----------

    #[test]
    fn bitmap_index_fast_path_for_count_group_and() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.inverted.bitmap_fields = vec!["status".into(), "city".into()];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let put_doc = |e: &mut Engine, docid: u64, status: &str, city: &str| {
            let val = json!({"docid": docid, "status": status, "city": city});
            let bytes = serde_json::to_vec(&val).unwrap();
            let terms = crate::server::extract_terms(&val);
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put(docid, bytes, &t).unwrap();
        };
        put_doc(&mut e, 1, "active", "beijing");
        put_doc(&mut e, 2, "inactive", "beijing");
        put_doc(&mut e, 3, "active", "shanghai");
        // COUNT 快速路径（内存位图）
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 2);
        assert_eq!(e.inverted_doc_count("city=beijing").unwrap(), 2);
        // AND 交集快速路径
        assert_eq!(
            e.inverted_bitmap_and_count(&["status=active", "city=beijing"])
                .unwrap(),
            1
        );
        // GROUP BY 快速路径
        let g = e.inverted_group_by("status").unwrap();
        assert!(g.contains(&("active".to_string(), 2)));
        assert!(g.contains(&("inactive".to_string(), 1)));
        // 重启后位图从段重建，COUNT 仍正确（drop 前刷盘倒排，保证段自包含）
        e.flush_inverted().unwrap();
        drop(e);
        let mut e2 = Engine::open(dir.path(), &cfg).unwrap();
        assert_eq!(e2.inverted_doc_count("status=active").unwrap(), 2);
    }

    #[test]
    fn execute_routes_by_spec() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(42, b"hello world".to_vec(), &["hello"]).unwrap();

        // 主键点查
        let spec = QuerySpec {
            primary_eq: Some(crate::keys::encode_docid(42).to_vec()),
            primary_range: false,
            index_prefix: vec![],
            term: None,
        };
        let rows = e.execute(&spec).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 42);

        // 倒排词条查询
        let spec2 = QuerySpec {
            primary_eq: None,
            primary_range: false,
            index_prefix: vec![],
            term: Some("hello".into()),
        };
        let rows2 = e.execute(&spec2).unwrap();
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0].1, b"hello world");
    }

    #[test]
    fn composite_prefix_query() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        // 组合索引：写入索引条目（key = composite(fields, docid)）
        if let Some(cidx) = &mut e.cidx {
            for docid in [10u64, 20, 30] {
                let key = crate::keys::encode_composite_key(&[b"active"], docid);
                cidx.put_bytes(key, docid.to_le_bytes().to_vec()).unwrap();
            }
            let key = crate::keys::encode_composite_key(&[b"inactive"], 99);
            cidx.put_bytes(key, 99u64.to_le_bytes().to_vec()).unwrap();
        }
        // 主数据写文档（回表需要）
        e.put(10, b"d10".to_vec(), &[]).unwrap();
        e.put(20, b"d20".to_vec(), &[]).unwrap();
        e.put(30, b"d30".to_vec(), &[]).unwrap();
        e.put(99, b"d99".to_vec(), &[]).unwrap();

        let rows = e.query_by_composite_prefix(&[b"active"]).unwrap();
        let mut ids: Vec<u64> = rows.iter().map(|(d, _)| *d).collect();
        ids.sort();
        assert_eq!(ids, vec![10, 20, 30]);
    }

    #[test]
    fn oom_guardian_blocks_writes_at_stall() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.set_mem_ratio(0.5);
        e.put(1, b"ok".to_vec(), &[]).unwrap(); // 正常写入

        e.set_mem_ratio(1.0); // 模拟 RSS 打满
        let err = e.put(2, b"blocked".to_vec(), &[]).unwrap_err();
        assert!(matches!(err, crate::error::Error::MemoryOverload(_)));
        // 被拒写入不生效
        assert!(e.get(2).unwrap().is_none());
        assert!(e.get(1).unwrap().is_some());
    }

    #[test]
    fn patch_merge_on_read() {
        // 阶段 1.5 Delta CF：patch 部分更新 → get 合并覆盖；重启后 WAL 恢复仍生效
        let dir = tmp();
        let mut e = Engine::open(&dir, &cfg()).unwrap();
        e.put(
            1,
            br#"{"status":"active","amount":100,"device":"android"}"#.to_vec(),
            &[],
        )
        .unwrap();
        e.patch(
            1,
            &[
                ("status", serde_json::json!("inactive")),
                ("note", serde_json::json!("updated")),
                ("amount", serde_json::Value::Null), // null = 删除字段
            ],
        )
        .unwrap();
        let v = e.get(1).unwrap().unwrap();
        let obj: serde_json::Value = serde_json::from_slice(&v).unwrap();
        assert_eq!(obj["status"], serde_json::json!("inactive"), "patch 应覆盖");
        assert_eq!(obj["note"], serde_json::json!("updated"), "patch 应新增");
        assert!(obj.get("amount").is_none(), "null patch 应删除字段");
        assert_eq!(
            obj["device"],
            serde_json::json!("android"),
            "未 patch 字段保留"
        );
        // 重启后 Delta 仍生效（WAL 恢复）
        drop(e);
        let mut e2 = Engine::open(&dir, &cfg()).unwrap();
        let obj2: serde_json::Value = serde_json::from_slice(&e2.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(obj2["status"], serde_json::json!("inactive"));
        assert_eq!(obj2["note"], serde_json::json!("updated"));
    }

    #[test]
    fn full_put_clears_delta() {
        // 全量 put 覆盖 → 清空该 docid 增量，避免旧 patch 覆盖新数据
        let dir = tmp();
        let mut e = Engine::open(&dir, &cfg()).unwrap();
        e.put(1, br#"{"status":"active","amount":100}"#.to_vec(), &[])
            .unwrap();
        e.patch(1, &[("status", serde_json::json!("patched"))])
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&e.get(1).unwrap().unwrap()),
            r#"{"status":"patched","amount":100}"#
        );
        e.put(1, br#"{"status":"fresh","amount":200}"#.to_vec(), &[])
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&e.get(1).unwrap().unwrap()),
            r#"{"status":"fresh","amount":200}"#
        );
    }

    #[test]
    fn delete_clears_delta() {
        // 删除文档 → Delta 清空，避免复活
        let dir = tmp();
        let mut e = Engine::open(&dir, &cfg()).unwrap();
        e.put(1, br#"{"status":"active"}"#.to_vec(), &[]).unwrap();
        e.patch(1, &[("note", serde_json::json!("x"))]).unwrap();
        e.delete(1).unwrap();
        assert!(e.get(1).unwrap().is_none(), "删除后 Delta 不应复活文档");
    }

    // ---------- Ex-5.6 删除位图 ----------

    #[test]
    fn multi_ssd_striping_places_files() {
        // Ex-5.10 多 SSD 条带化：wal_dir/sst_dir/inverted_dir 分盘——WAL/SST/倒排文件
        // 落位正确 + 数据跨重启恢复（默认单盘布局由既有测试覆盖）
        let data_dir = tempfile::tempdir().unwrap();
        let wal_dir = tempfile::tempdir().unwrap();
        let sst_dir = tempfile::tempdir().unwrap();
        let inv_dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.storage.wal_dir = Some(wal_dir.path().to_string_lossy().to_string());
        cfg.storage.sst_dir = Some(sst_dir.path().to_string_lossy().to_string());
        cfg.storage.inverted_dir = Some(inv_dir.path().to_string_lossy().to_string());
        {
            let mut e = Engine::open(data_dir.path(), &cfg).unwrap();
            e.put(1, b"doc1".to_vec(), &["rust"]).unwrap();
            e.put(2, b"doc2".to_vec(), &["go"]).unwrap();
            e.flush_primary().unwrap();
            e.flush_inverted().unwrap();
            assert_eq!(e.get(1).unwrap().unwrap(), b"doc1");
        }
        // 落位验证：WAL / SST / 倒排各在其盘
        assert!(
            wal_dir.path().join("primary").join("wal.log").exists(),
            "primary WAL 应落在 wal_dir"
        );
        let sst_files: Vec<_> = std::fs::read_dir(sst_dir.path().join("primary"))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(!sst_files.is_empty(), "SST 应落在 sst_dir");
        assert!(
            inv_dir.path().join("inverted").join("inverted-manifest.json").exists(),
            "倒排应在 inverted_dir"
        );
        assert!(!data_dir.path().join("primary").exists(), "列族目录已外移 sst_dir");
        // 跨重启恢复（多盘布局持久）
        let mut e2 = Engine::open(data_dir.path(), &cfg).unwrap();
        assert_eq!(e2.get(1).unwrap().unwrap(), b"doc1");
        assert_eq!(e2.get(2).unwrap().unwrap(), b"doc2");
        assert!(e2.search_term("rust").unwrap().len() == 1);
    }

    #[test]
    fn dynamic_io_rate_backs_off_with_write_pressure() {
        // Ex-7.4：MemTable 水位（写压力代理）升高 → Compaction 限速下调让路
        // （压力 p → base×(1-0.5p)，最低 50% 基准）
        let mut c = cfg();
        c.storage.io_rate_limit_mb = 100; // 100MB/s 基准
        c.memtable.max_size_mb = 1; // 1MB 小 MemTable → 快速产生写压力
        let mut e = Engine::open(&tmp(), &c).unwrap();
        let base = 100u64 * 1024 * 1024;
        assert_eq!(e.primary.io_rate(), base, "空 MemTable 压力 0 全速");
        // 写入 8 个 64KB 文档 ≈ 512KB（memtable 50% 水位）
        let big = vec![b'x'; 64 * 1024];
        for i in 0..8u64 {
            e.put(i, big.clone(), &[]).unwrap();
        }
        let rate = e.primary.io_rate();
        assert!(rate < base, "写压力下限速应下调: {rate} < {base}");
        assert!(rate >= base / 2, "不低于 50% 基准: {rate}");
        // flush 后水位回落 → 限速回升
        e.flush_primary().unwrap();
        let rate2 = e.primary.io_rate();
        assert!(rate2 >= rate, "flush 后水位降、限速回升: {rate2} >= {rate}");
    }

    #[test]
    fn outbox_e2e_enqueue_dispatch_drain() {
        // Ex-1 端到端：enqueue（与业务写同 seq 空间）→ 重启保留 → 幂等投递 → 排空
        let mut c = cfg();
        c.outbox.enabled = true;
        let dir = tmp();
        let mut consumer = crate::outbox::IdempotentConsumer::new();
        {
            let mut e = Engine::open(&dir, &c).unwrap();
            e.put(1, b"doc1".to_vec(), &["k"]).unwrap();
            e.enqueue_outbox(1, b"msg-1").unwrap();
            e.enqueue_outbox(2, b"msg-2").unwrap();
            e.flush_wal().unwrap();
            assert_eq!(e.outbox_pending().unwrap(), 2);
            // 幂等投递
            let n = e
                .dispatch_outbox(|k, p| {
                    assert!(p.starts_with(b"msg-"));
                    consumer.apply(k)
                })
                .unwrap();
            assert_eq!(n, 2);
            assert!(e.outbox_drained().unwrap(), "投递后排空");
        }
        // 重启：pending 保持 0（done 状态持久）
        let mut e2 = Engine::open(&dir, &c).unwrap();
        assert!(e2.outbox_drained().unwrap());
        assert_eq!(consumer.received(), 2);
    }

    #[test]
    fn outbox_disabled_by_default() {
        // outbox 默认关闭（零开销）：enqueue 返回 Unsupported、pending=0
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        assert!(e.enqueue_outbox(1, b"x").is_err(), "未启用应拒绝入队");
        assert_eq!(e.outbox_pending().unwrap(), 0);
        assert!(e.outbox_drained().unwrap());
    }

    #[test]
    fn outbox_pending_survives_restart() {
        // 崩溃恢复：enqueue 未投递 → 重开 pending 保留（WAL 回放重建）
        let mut c = cfg();
        c.outbox.enabled = true;
        let dir = tmp();
        {
            let mut e = Engine::open(&dir, &c).unwrap();
            e.enqueue_outbox(7, b"keep").unwrap();
            e.flush_wal().unwrap();
        }
        let mut e2 = Engine::open(&dir, &c).unwrap();
        assert_eq!(e2.outbox_pending().unwrap(), 1, "重开 pending 保留");
    }

    #[test]
    fn scale_out_coordinator_e2e_with_outbox() {
        // Ex-1.5 端到端：扩容协调器（状态机 + 路由切换）衔接 engine outbox——
        // 写主（业务+outbox 本地原子）→ 追平投递到新节点 → 排空校验 → 切换 → 新节点接管
        use crate::scale_out::{Phase, ScaleOutCoordinator};
        use std::sync::Mutex;
        let da = tempfile::tempdir().unwrap();
        let db = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.outbox.enabled = true;
        let a = Mutex::new(Engine::open(da.path(), &c).unwrap());
        let b = Mutex::new(Engine::open(db.path(), &c).unwrap());
        // 写主节点 + outbox 入队（本地原子：业务写与消息同 fsync 点）
        for i in 0..20u64 {
            let val = format!("doc-{i}").into_bytes();
            let mut a = a.lock().unwrap();
            a.put(i, val.clone(), &["status=active"]).unwrap();
            a.enqueue_outbox(i, &val).unwrap();
        }
        // 扩容编排开始（新节点注册为 slave）
        let mut meta = crate::meta::MetaCenter::new(4);
        meta.register("node-a", "127.0.0.1:9001", "master").unwrap();
        let mut coord = ScaleOutCoordinator::begin(
            &da.path().join("scale-out.json"),
            meta,
            "node-a",
            "node-b",
            "127.0.0.1:9002",
        )
        .unwrap();
        assert_eq!(coord.phase(), Phase::Adding);
        coord.begin_catch_up().unwrap();
        // 追平：dispatch_outbox 投递到新节点（put 覆盖 = 幂等 apply）
        {
            let mut a = a.lock().unwrap();
            let mut b = b.lock().unwrap();
            let n = a
                .dispatch_outbox(|key, payload| {
                    let docid = u64::from_be_bytes(key[..8].try_into().unwrap());
                    b.put(docid, payload.to_vec(), &[]).unwrap();
                    true
                })
                .unwrap();
            assert_eq!(n, 20, "20 条 outbox 全部投递");
            assert!(a.outbox_drained().unwrap(), "排空校验通过");
        }
        coord.mark_drained().unwrap();
        coord.switch().unwrap();
        assert_eq!(coord.phase(), Phase::Done);
        assert_eq!(coord.master_node().as_deref(), Some("node-b"), "路由切到新节点");
        // 新节点数据完整（与主节点一致）
        let b = b.lock().unwrap();
        for i in 0..20u64 {
            assert_eq!(
                b.get(i).unwrap().as_deref(),
                Some(format!("doc-{i}").as_bytes()),
                "docid {i} 追平一致"
            );
        }
    }

    // ============ Ex-8.7 删除密度 Compaction ============

    #[test]
    fn delete_gc_pending_gates_on_ratio_and_min_docs() {
        // 边界：min_docs（新增置位门槛）与 min_ratio（置位率门槛）独立生效——
        // 小批量删除不触发；达到 min_docs 但密度不足不触发；两者满足才就绪。
        let dir = tmp();
        let mut cfg = cfg();
        cfg.storage.auto_compact = false;
        cfg.storage.delete_density_min_docs = 10;
        cfg.storage.delete_density_min_ratio = 0.10;
        let mut e = Engine::open(&dir, &cfg).unwrap();
        for d in 1..=100u64 {
            e.put(d, format!("v{d}").into_bytes(), &[]).unwrap();
        }
        assert!(!e.delete_garbage_pending(), "无删除不就绪");
        for d in 1..=9u64 {
            e.delete(d).unwrap();
        }
        assert!(
            !e.delete_garbage_pending(),
            "密度 9%<10% 不就绪（虽已满足 min_docs 阈值语义由另一例覆盖）"
        );
        e.delete(10).unwrap(); // 置位率 10%
        assert!(
            e.delete_garbage_pending(),
            "置位率 10% ≥ 阈值 + 新增 ≥ min_docs → 就绪"
        );
        assert!(e.needs_compact(), "就绪 → needs_compact 置位");
        // min_docs 门槛独立验证：置位率足够但新增不足 → 不就绪
        cfg.storage.delete_density_min_docs = 50;
        let mut e2 = Engine::open(&tmp(), &cfg).unwrap();
        for d in 1..=100u64 {
            e2.put(d, format!("v{d}").into_bytes(), &[]).unwrap();
        }
        for d in 1..=20u64 {
            e2.delete(d).unwrap(); // 密度 20% ≥10%，但新增 20 < min_docs 50
        }
        assert!(!e2.delete_garbage_pending(), "新增置位不足 min_docs 不就绪");
    }

    #[test]
    fn delete_density_gc_drains_converged_primary_and_reclaims_space() {
        // Ex-8.7 核心：删除密集负载（位图开启）收敛为单底层段后——常规 select 无多段候选，
        // 删除密度 urgency 触发 GC **单段重写**物理回收已删数据；排空（0 丢弃轮）后收敛。
        let dir = tmp();
        let mut cfg = cfg();
        cfg.storage.auto_compact = false;
        let n = 4_000u64;
        let delete_every = 3u64; // 33% 删除密集
        let mut e = Engine::open(&dir, &cfg).unwrap();
        // 批量灌入（put_nosync 免逐条 fsync）+ 分批 flush → 多 L0
        let chunk = 1_000u64;
        for (c, d) in (1..=n).enumerate() {
            e.put_nosync(d, format!("v{d}").into_bytes(), &[]).unwrap();
            if (c as u64 + 1) % chunk == 0 {
                e.flush_primary().unwrap();
            }
        }
        e.flush_wal().unwrap();
        // 常规压实收敛（无删除 → 位图无关路径）：L0 多段 → 底层单段
        let mut guard = 0;
        while e.primary.sst_count() > 1 && guard < 20 {
            let _ = e.compact().unwrap();
            guard += 1;
        }
        assert_eq!(e.primary.sst_count(), 1, "已收敛为单段");
        let bytes_before = e.primary.sst_bytes();
        assert!(bytes_before > 0);
        // 删除密集（均匀 1/3：3,6,9,…）
        let mut deleted = 0u64;
        for d in (delete_every..=n).step_by(delete_every as usize) {
            e.delete(d).unwrap();
            deleted += 1;
        }
        e.flush_wal().unwrap();
        assert!(
            e.delete_garbage_pending(),
            "置位率≈1/3 ≥ 阈值、新增≥min_docs → 删除密度就绪"
        );
        // GC 排空：逐轮压实直至某轮 0 丢弃收敛
        let mut total_dropped = 0u64;
        let mut rounds = 0;
        while e.delete_garbage_pending() && rounds < 20 {
            let rep = e.compact().unwrap();
            total_dropped += rep.dropped_keys as u64;
            rounds += 1;
        }
        assert!(!e.needs_compact(), "排空收敛后不再需要合并");
        assert_eq!(total_dropped, deleted, "全部已删数据物理丢弃");
        let bytes_after = e.primary.sst_bytes();
        assert!(
            bytes_after < bytes_before,
            "删除密集段重写应回收空间: {} → {}",
            bytes_before,
            bytes_after
        );
        // 语义：存活可见、已删不可见、全表计数一致
        for d in 1..=n {
            let expect = d % delete_every != 0;
            assert_eq!(e.get(d).unwrap().is_none(), !expect, "docid {d}");
        }
        let live = e.count_all_docs().unwrap();
        assert_eq!(live, n - deleted, "可见行数 = 总行 - 已删");
        // 重启：done 基准 = 打开时置位数 → 历史置位不重复触发 GC 重写
        drop(e);
        let mut e2 = Engine::open(&dir, &cfg).unwrap();
        assert!(!e2.delete_garbage_pending(), "历史置位不重复触发");
        assert!(e2.get(3).unwrap().is_none(), "已删 docid3 重启后仍不可见");
        assert_eq!(e2.get(2).unwrap().unwrap(), b"v2");
    }

    #[test]
    fn delete_density_demo_dense_vs_uniform_reclaim_contrast() {
        // Ex-8.7 demo：删除密集 vs 均匀（少量删除）负载——同样载入量下，删除密集
        // 经删除密度 GC 回收 ≈删除比例的空间；均匀负载不触发（置位率/新增均低于门槛），
        // 段不重写、空间保持（对照"删除密集段优先被合并以释放空间"的收益）。
        let dense_dir = tmp();
        let uniform_dir = tmp();
        let mut cfg = cfg();
        cfg.storage.auto_compact = false;
        cfg.storage.delete_density_min_docs = 100; // 演示用小阈值
        cfg.storage.delete_density_min_ratio = 0.05;
        let n = 2_000u64;
        let load = |dir: &std::path::Path, e: &mut Engine| {
            for (c, d) in (1..=n).enumerate() {
                e.put_nosync(d, format!("doc-{d:05}").into_bytes(), &[]).unwrap();
                if (c as u64 + 1) % 500 == 0 {
                    e.flush_primary().unwrap();
                }
            }
            e.flush_wal().unwrap();
            let mut g = 0;
            while e.primary.sst_count() > 1 && g < 20 {
                let _ = e.compact().unwrap();
                g += 1;
            }
        };
        let mut dense = Engine::open(&dense_dir, &cfg).unwrap();
        load(&dense_dir, &mut dense);
        let mut uniform = Engine::open(&uniform_dir, &cfg).unwrap();
        load(&uniform_dir, &mut uniform);
        // 删除密集：删除 50%（step 2）→ GC 回收；均匀：仅删 2% 且低于增量门槛 → 不触发
        let mut del = 0u64;
        for d in (1..=n).step_by(2) {
            dense.delete(d).unwrap();
            del += 1;
        }
        for d in (1..=n).step_by(50) {
            uniform.delete(d).unwrap();
        }
        dense.flush_wal().unwrap();
        uniform.flush_wal().unwrap();
        let mut rounds = 0;
        while dense.delete_garbage_pending() && rounds < 20 {
            let _ = dense.compact().unwrap();
            rounds += 1;
        }
        assert!(!uniform.delete_garbage_pending(), "均匀少量删除不触发 GC");
        let dense_bytes = dense.primary.sst_bytes();
        let uniform_bytes = uniform.primary.sst_bytes();
        println!(
            "[Ex-8.7 demo] 载入 {n} 行：删除密集(-50%) GC 后 {} bytes vs 均匀(-2%) {} bytes；排空 {} 轮",
            dense_bytes, uniform_bytes, rounds
        );
        assert_eq!(dense.primary.sst_count(), 1, "删除密集收敛单段");
        assert_eq!(uniform.primary.sst_count(), 1, "均匀收敛单段");
        assert!(
            dense_bytes < uniform_bytes,
            "删除密集应显著回收空间: dense={} uniform={}",
            dense_bytes,
            uniform_bytes
        );
        assert_eq!(dense.count_all_docs().unwrap(), n - del, "可见行一致");
    }

    #[test]
    fn deletion_bitmap_persists_across_restart() {
        // 删除 → flush_wal（位图落盘）→ 重启 → 位图文件加载，已删 docid 仍不可见
        let dir = tmp();
        let cfg = cfg();
        let bitmap_path = dir.join("deletion.bitmap");
        {
            let mut e = Engine::open(&dir, &cfg).unwrap();
            e.put(1, b"v1".to_vec(), &["k"]).unwrap();
            e.put(2, b"v2".to_vec(), &["k"]).unwrap();
            e.flush_wal().unwrap();
            e.delete(1).unwrap();
            e.flush_wal().unwrap(); // 位图脏页落盘
        }
        let meta = std::fs::metadata(&bitmap_path).unwrap();
        assert_eq!(meta.len() % 4096, 0, "位图文件 4KB 页对齐");
        let mut e2 = Engine::open(&dir, &cfg).unwrap();
        assert!(e2.get(1).unwrap().is_none(), "重启后位图加载，删除持久");
        assert!(e2.get(2).unwrap().is_some(), "未删文档不受影响");
    }

    #[test]
    fn deletion_bitmap_put_resurrects() {
        // delete → put 复活：put 清位，文档重新可见
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"v1".to_vec(), &["k"]).unwrap();
        e.delete(1).unwrap();
        assert!(e.get(1).unwrap().is_none());
        e.put(1, b"v2".to_vec(), &["k"]).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"v2", "put 复活后可见新值");
    }

    #[test]
    fn deletion_bitmap_compaction_drops_deleted_data() {
        // 位图开启：delete 不写 LSM 墓碑 → compaction 按位图物理丢弃已删 docid 旧数据；
        // 重启后位图 + 压实结果均一致（已删不可见、存活可见）
        let dir = tmp();
        let cfg = cfg();
        {
            let mut e = Engine::open(&dir, &cfg).unwrap();
            for d in 1..=4u64 {
                e.put(d, format!("doc{d}").into_bytes(), &["k"]).unwrap();
            }
            e.flush_primary().unwrap(); // L0 第 1 段
            e.put(5, b"doc5".to_vec(), &["k"]).unwrap();
            e.flush_primary().unwrap(); // L0 第 2 段 → 触发 compaction 条件
            e.delete(2).unwrap();
            e.delete(4).unwrap();
            let rep = e.compact().unwrap();
            assert!(rep.merged_ssts >= 2, "L0 压实应合并多段");
            assert!(e.get(2).unwrap().is_none());
            assert!(e.get(4).unwrap().is_none());
            assert!(e.get(1).unwrap().is_some());
            assert!(e.get(5).unwrap().is_some());
        }
        let mut e2 = Engine::open(&dir, &cfg).unwrap();
        for d in 1..=5u64 {
            let deleted = matches!(d, 2 | 4);
            assert_eq!(e2.get(d).unwrap().is_none(), deleted, "重启后 docid {d} 状态一致");
        }
    }

    #[test]
    fn deletion_bitmap_disabled_uses_tombstone_path() {
        // 位图关闭：delete 走传统 Tombstone（primary.delete + 逐条 fsync），get 仍返回 None
        let mut c = cfg();
        c.storage.deletion_bitmap_enabled = false;
        let dir = tmp();
        {
            let mut e = Engine::open(&dir, &c).unwrap();
            e.put(1, b"v1".to_vec(), &["k"]).unwrap();
            e.delete(1).unwrap();
            assert!(e.get(1).unwrap().is_none());
        }
        assert!(
            !dir.join("deletion.bitmap").exists(),
            "位图关闭时不应生成位图文件"
        );
        let mut e2 = Engine::open(&dir, &c).unwrap();
        assert!(e2.get(1).unwrap().is_none(), "Tombstone 路径跨重启删除一致");
    }

    #[test]
    fn deletion_bitmap_incremental_backup_captures_delete() {
        // 位图开启：delete 写 primary WAL 删除记录 → 增量备份导出含删除 → 恢复后删除保持
        let dir = tmp();
        let cfg = cfg();
        let mut e = Engine::open(&dir, &cfg).unwrap();
        let put_doc = |e: &mut Engine, docid: u64, v: &str| {
            e.put(docid, v.as_bytes().to_vec(), &["k"]).unwrap();
        };
        put_doc(&mut e, 1, "v1");
        put_doc(&mut e, 2, "v2");
        let since = e.current_seq();
        e.delete(1).unwrap();
        let bak_dir = tempfile::tempdir().unwrap();
        let bak = bak_dir.path().join("incr.json");
        let rep = e.backup_incremental(since, &bak).unwrap();
        assert!(rep.records >= 1, "增量备份应包含删除记录");
        drop(e);

        // 恢复到全新引擎（位图开启）：删除记录回放 → 重新置位 → doc 1 不可见
        let dir2 = tmp();
        let mut e2 = Engine::open(&dir2, &cfg).unwrap();
        put_doc(&mut e2, 1, "v1");
        put_doc(&mut e2, 2, "v2");
        let n = e2.restore_incremental(&bak).unwrap();
        assert!(n >= 1);
        assert!(e2.get(1).unwrap().is_none(), "增量恢复后删除保持");
        assert!(e2.get(2).unwrap().is_some());
    }

    #[test]
    fn oom_guardian_throttles_in_soft_range() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.set_mem_ratio(0.9); // 软水位区间
        e.put(1, b"throttled-but-allowed".to_vec(), &[]).unwrap();
        assert!(e.get(1).unwrap().is_some());
        // 限流计数已记录
        assert!(e.watchdog.memory().throttled_count() >= 1);
    }

    #[test]
    fn restart_preserves_data_and_inverted() {
        // 崩溃恢复全链路：写入（未强制刷盘）→ 进程退出 → 重开 → 主数据与倒排均完好
        let dir = tmp();
        let cfg = cfg();
        {
            let mut e = Engine::open(&dir, &cfg).unwrap();
            e.put(1, b"doc-1".to_vec(), &["rust"]).unwrap();
            e.put(2, b"doc-2".to_vec(), &["go"]).unwrap();
            e.put(3, b"doc-3".to_vec(), &["rust", "async"]).unwrap();
            e.flush_inverted().unwrap();
        } // 模拟进程退出
        {
            let mut e2 = Engine::open(&dir, &cfg).unwrap();
            // 主数据（WAL 回放恢复）
            assert_eq!(e2.get(1).unwrap().unwrap(), b"doc-1");
            assert_eq!(e2.get(2).unwrap().unwrap(), b"doc-2");
            assert_eq!(e2.get(3).unwrap().unwrap(), b"doc-3");
            // 倒排跨重启可查（段文件 + Manifest）
            let rows = e2.search_term("rust").unwrap();
            let mut ids: Vec<u64> = rows.iter().map(|(d, _)| *d).collect();
            ids.sort();
            assert_eq!(ids, vec![1, 3]);
            assert_eq!(e2.search_term("go").unwrap().len(), 1);
        }
    }
}
