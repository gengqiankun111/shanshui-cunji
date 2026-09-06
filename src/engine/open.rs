//! 文档引擎生命周期/备份恢复（reconstruct.md engine/open.rs）：`open` / `open_with_timeout`
//! 与组提交后台线程启动（start_group_commit）、增量备份/恢复（backup_incremental /
//! restore_incremental / prepare_backup）、`Drop` 清理与备份相关类型（IncrementalBackupFile /
//! BackupReport）。整库 purge 归 write.rs。
//! 内容拆分自原 engine.rs（生命周期/备份主题）；私有 Engine 字段以 `pub(crate)` 提升访问。

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

#[cfg(target_os = "linux")]
use tracing::{info, warn};

use crate::bitmap::DeletionBitmap;
use crate::column_family::ColumnFamily;
use crate::config::model::Config;
use crate::engine::percpu_wal::{WalRuntime, CF_PRIMARY};
use crate::engine::Engine;
use crate::error::Result;
use crate::hotcache::HotCache;
use crate::inverted::InvertedIndex;
use crate::keys::decode_docid;
use crate::outbox::Outbox;
use crate::watchdog::{Watchdog, DEFAULT_QUERY_TIMEOUT};


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

/// 组提交（M8）清理：停后台兜底线程并 join，最终落盘待刷 WAL（保证正常退出不丢窗口尾部）。
impl Drop for Engine {
    fn drop(&mut self) {
        // Task-026：per-CPU 运行时停机——stop 消费线程 → join（尾部已排空）→ 终态 checkpoint
        if let Some(rt) = &self.percpu {
            rt.shutdown();
        }
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

impl Engine {
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
        let mut primary = {
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
        let mut primary = {
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
        // P4-B：delta FST 上限配置（MB → 字节）
        if cfg.inverted.delta_fst_max_mb > 0 {
            inverted.set_delta_fst_max_bytes(cfg.inverted.delta_fst_max_mb * 1024 * 1024);
        }
        // 位图索引（design 5.2.4，M7-2）：白名单非空时全量重建内存位图
        inverted.with_bitmap_fields(&cfg.inverted.bitmap_fields)?;
        // Ex-8.13：倒排 GC/后台段写共享后台 IO 预算（与列族压缩同受 Ex-7.4 写压力收窄；
        // 前台紧急刷段仅记账不等待）
        if cfg.storage.io_rate_limit_mb > 0 {
            inverted.attach_io_budget(cfg.storage.io_rate_limit_mb * 1024 * 1024);
        }
        let mut cidx = {
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
        let mut delta = Arc::new(ColumnFamily::open_with_io_uring(
            "delta",
            &sst_root.join("delta"),
            Some(wal_root),
            cfg,
            iou.clone(),
        )?);
        #[cfg(not(target_os = "linux"))]
        let mut delta = Arc::new(ColumnFamily::open_with_wal_dir(
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
        let mut outbox = if cfg.outbox.enabled {
            Some(Outbox::open(&sst_root.join("outbox"), cfg)?)
        } else {
            None
        };
        // Task-026（`per_cpu_enabled`）：engine 级队列 WAL 接管各 CF 持久化。
        // - 构建队列运行时（每队列独立文件/消费线程/checkpoint）；
        // - CF 切 external（写收集走 TLS scope；flush 完成回调推进刷盘水位）；
        // - 旧自身 WAL 残留（迁移期）→ 强制 flush 落 SST（此后不依赖旧文件）；
        // - 队列文件 gseq 全局归并回放（> checkpoint）到各 CF memtable；
        // - global_seq 以 checkpoint/队列 max/旧 WAL next_seq 取大续接。
        let per_cpu_enabled = cfg.storage.per_cpu_enabled;
        let mut percpu: Option<Arc<WalRuntime>> = None;
        let global_seq: Arc<AtomicU64> = if per_cpu_enabled {
            let (gs, rt) = Self::prepare_per_cpu_open(
                cfg,
                &wal_root,
                &mut primary,
                &mut delta,
                &mut cidx,
                &mut outbox,
            )?;
            percpu = Some(rt);
            gs
        } else {
            Arc::new(AtomicU64::new(
                primary
                    .wal_next_seq()
                    .max(delta.wal_next_seq())
                    .max(outbox.as_ref().map_or(0, |o| o.wal_next_seq())),
            ))
        };
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
            per_cpu_wal: crate::engine::percpu_wal::PerCpuWal::resolve(cfg),
            percpu,
            gc_stop: None,
            gc_thread: None,
            flush_log_at_trx_commit: cfg.storage.flush_log_at_trx_commit,
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
            composite_indexes: cfg.storage.composite_indexes.clone(),
            pending_inverted: Mutex::new(Vec::new()),
            compaction_parallel: cfg.storage.compaction_parallel,
            cost_based_enabled: cfg.optimizer.cost_based_enabled,
            cost_params: crate::optimizer::CostParams {
                point_lookup_cost: cfg.optimizer.point_lookup_cost,
                full_scan_row_cost: cfg.optimizer.full_scan_row_cost,
                inverted_fetch_cost: cfg.optimizer.inverted_fetch_cost,
                inverted_merge_fixed: cfg.optimizer.inverted_merge_fixed,
                composite_fetch_cost: cfg.optimizer.composite_fetch_cost,
                zone_map_effectiveness: cfg.optimizer.zone_map_effectiveness,
                scan_row_factor: 1.5,
                inverted_fallback_threshold: cfg.optimizer.inverted_fallback_threshold,
            },
            // P94：colstore 状态（惰性派生；默认关闭零回归）
            colstore_enabled: cfg.storage.colstore_enabled,
            colstore_hot: cfg.storage.hot_fields.clone(),
            colstore_state: std::sync::Mutex::new(
                crate::engine::colstore::ColstoreState::default(),
            ),
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
            active_snapshots: RwLock::new(std::collections::BTreeMap::new()),
            live_docids: std::sync::Mutex::new(None),
            snapshot_dels: std::sync::Mutex::new(std::collections::HashMap::new()),
            snapshot_batch_rows: std::sync::atomic::AtomicU64::new(0),
            snapshot_prefilter_saved: std::sync::atomic::AtomicU64::new(0),
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
        // 缺口②（P105-②）：**空库打开即播种活跃 docid 空集**（`Some(空)`）——fresh-load
        // 场景（cjserver 空数据目录启动 → wide-load 装数）下 put/delete/delete_batch 全程
        // 增量记账，首个 `COUNT(*)`/区间基数即 O(1)（活跃集 rank），不再触发一次性全键扫
        // 基线（P105 #12 43ms 均值根因：打开时基线 None → load 期不记账 → 首个 COUNT
        // 触发全扫 ~200ms 拖高 5 次均值）。非空库（SST/memtable 有数据）保持 None →
        // 首次 COUNT 懒建基线（P1-C 语义，避免 open 期全扫拖慢启动）；purge_all 复位
        // `Some(空)` 不变。
        if engine.primary.data_empty() {
            *engine.live_docids.lock().unwrap() = Some(roaring::treemap::RoaringTreemap::new());
        }
        // Task-028：cidx 存量补齐——声明 composite_indexes 但 cidx 空/签名不符（配置后加/
        // 旧库无 cidx/崩溃丢键）时 open 期从 primary 回扫重建（正常会话零开销跳过）。
        // 置于 worker 启动前：重建期无并发写，flush 落 SST + 写 cidx.sig 标记幂等。
        engine.ensure_composite_index_backfill()?;
        // P130（2026-09-05）：open 回放后 memtable **超阈** → 主动刷盘落 SST——open 期
        // WAL/队列回放不逐批 flush（回放直接进 memtable，阈值检查只在写路径），只读服务
        // 启动后大 memtable 无限期驻留：区间/全表扫描需跨 memtable+段 k 路归并，实测慢 ~10×
        // （#75/组合 SELECT 160-760ms 临时退化，flush 后回 19-21ms）。空/未超阈不刷（防空 L0）。
        // 置于 worker/组提交启动前：无并发写，flush 安全（external 模式回调已注册推进水位）。
        if engine.primary.memtable_over_threshold() {
            engine.primary.switch_and_flush()?;
        }
        if engine.delta.memtable_over_threshold() {
            engine.delta.switch_and_flush()?;
        }
        if let Some(c) = engine.cidx.as_ref() {
            if c.memtable_over_threshold() {
                c.switch_and_flush()?;
            }
        }
        // 组提交（M8）：`storage.group_commit_us > 0` 时开启——窗口内写入攒批一次 fsync，
        // 后台线程兜底窗口尾部落盘；默认 0 = 关闭（保持逐条 fsync 强安全）。
        // Task-026：per-CPU 启用 → 每队列消费线程启动（替代组提交后台线程）。
        if engine.percpu.is_some() {
            engine.percpu.as_ref().unwrap().start()?;
        } else {
            engine.start_group_commit(cfg);
        }
        Ok(engine)
    }

    /// Task-026：per-CPU WAL 打开准备（`per_cpu_enabled=true` 分支；设计
    /// research/percpu-wal-stage2-design.md §3/§4）。步骤：
    /// 1. 构建队列运行时，加载持久化 checkpoint 作为水位下限；
    /// 2. CF 切 external（写经 TLS scope 收集；flush 完成回调推进刷盘水位）;
    ///    不存在的 CF（cidx/outbox 关闭）水位标记 +∞（不约束 cp）；
    /// 3. 旧自身 WAL 回放进 memtable 的残留（迁移期）→ 强制 flush 落 SST；
    /// 4. 队列文件 gseq 全局归并回放（> checkpoint）到各 CF memtable；
    /// 5. global_seq = max(checkpoint+1, 队列 max+1, 旧 WAL next_seq) 续接。
    fn prepare_per_cpu_open(
        cfg: &Config,
        wal_root: &Path,
        primary: &mut Arc<ColumnFamily>,
        delta: &mut Arc<ColumnFamily>,
        cidx: &mut Option<Arc<ColumnFamily>>,
        outbox: &mut Option<Outbox>,
    ) -> Result<(Arc<AtomicU64>, Arc<WalRuntime>)> {
        use crate::engine::percpu_wal::{CF_CIDX, CF_DELTA, CF_OUTBOX, CF_PRIMARY};
        let pc = crate::engine::percpu_wal::PerCpuWal::resolve(cfg);
        let rt = Arc::new(WalRuntime::build(
            wal_root.join("percpu-wal"),
            pc.queues,
            pc.depth,
            pc.window_us,
        ));
        // 持久化 checkpoint = 已收敛下限（此前已刷盘数据不回退重放）
        let cp = rt.load_checkpoint();
        rt.cp.store(cp, Ordering::Relaxed);
        for w in rt.cf_watermarks.iter() {
            w.store(cp, Ordering::Relaxed);
        }
        if cidx.is_none() {
            rt.mark_cf_absent(CF_CIDX);
        }
        if outbox.is_none() {
            rt.mark_cf_absent(CF_OUTBOX);
        }
        // CF 切 external + 刷盘水位回调
        {
            let cb: Arc<dyn Fn(u64) + Send + Sync> = {
                let rt = Arc::clone(&rt);
                Arc::new(move |m| rt.note_flush(CF_PRIMARY, m))
            };
            Arc::get_mut(primary)
                .ok_or_else(|| crate::error::Error::Unsupported("primary Arc 非唯一".into()))?
                .set_external_wal(CF_PRIMARY, cb);
        }
        {
            let cb: Arc<dyn Fn(u64) + Send + Sync> = {
                let rt = Arc::clone(&rt);
                Arc::new(move |m| rt.note_flush(CF_DELTA, m))
            };
            Arc::get_mut(delta)
                .ok_or_else(|| crate::error::Error::Unsupported("delta Arc 非唯一".into()))?
                .set_external_wal(CF_DELTA, cb);
        }
        if let Some(c) = cidx.as_mut() {
            let cb: Arc<dyn Fn(u64) + Send + Sync> = {
                let rt = Arc::clone(&rt);
                Arc::new(move |m| rt.note_flush(CF_CIDX, m))
            };
            Arc::get_mut(c)
                .ok_or_else(|| crate::error::Error::Unsupported("cidx Arc 非唯一".into()))?
                .set_external_wal(CF_CIDX, cb);
        }
        if let Some(ob) = outbox.as_mut() {
            let cb: Arc<dyn Fn(u64) + Send + Sync> = {
                let rt = Arc::clone(&rt);
                Arc::new(move |m| rt.note_flush(CF_OUTBOX, m))
            };
            ob.set_external_wal(CF_OUTBOX, cb);
        }
        // 迁移收尾：旧自身 WAL 回放进 memtable 的残留 → 强制刷盘落 SST（此后不依赖旧文件；
        // 空 memtable 不刷——空 flush 会产出空 L0 SST，污染紧凑度/GC 调度）
        if primary.memtable_bytes() > 0 {
            primary.switch_and_flush()?;
        }
        if delta.memtable_bytes() > 0 {
            delta.switch_and_flush()?;
        }
        if let Some(c) = cidx.as_ref() {
            if c.memtable_bytes() > 0 {
                c.switch_and_flush()?;
            }
        }
        if let Some(ob) = outbox.as_mut() {
            if ob.memtable_bytes() > 0 {
                ob.flush()?;
            }
        }
        // 队列文件 gseq 全局归并回放（> checkpoint）
        let since = rt.cp.load(Ordering::Relaxed);
        let entries = rt.records_after(since)?;
        let mut max_g = since;
        if !entries.is_empty() {
            let mut by_cf: [Vec<crate::engine::percpu_wal::WalEntry>; 4] =
                std::array::from_fn(|_| Vec::new());
            for e in &entries {
                if (e.cf as usize) < 4 {
                    by_cf[e.cf as usize].push(e.clone());
                }
                max_g = max_g.max(e.gseq);
            }
            primary.replay_external(&by_cf[CF_PRIMARY as usize])?;
            delta.replay_external(&by_cf[CF_DELTA as usize])?;
            if let Some(c) = cidx.as_ref() {
                c.replay_external(&by_cf[CF_CIDX as usize])?;
            }
            if let Some(ob) = outbox.as_mut() {
                ob.replay_external(&by_cf[CF_OUTBOX as usize])?;
            }
            // 播种入队水位：已回放未刷条目约束 cp（防裁剪越过 memtable 中未刷数据）
            for cf in [CF_PRIMARY, CF_DELTA, CF_CIDX, CF_OUTBOX] {
                if let Some(last) = by_cf[cf as usize].last() {
                    rt.note_enqueued(cf, last.gseq);
                }
            }
        }
        // global_seq 起点：checkpoint+1 / 队列 max+1 / 旧 WAL next_seq 取大
        let next = cp
            .saturating_add(1)
            .max(max_g.saturating_add(1))
            .max(primary.wal_next_seq())
            .max(delta.wal_next_seq())
            .max(cidx.as_ref().map_or(0, |c| c.wal_next_seq()))
            .max(outbox.as_ref().map_or(0, |o| o.wal_next_seq()));
        rt.persist_checkpoint()?;
        rt.trim_segments();
        Ok((Arc::new(AtomicU64::new(next)), rt))
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
        // Task-026：per-CPU 模式从队列文件取（primary 记录；恢复回放按 engine put/delete 语义）
        let (oldest, records) = if let Some(rt) = &self.percpu {
            let mut es: Vec<crate::wal::WalRecord> = rt
                .records_after(since_seq)?
                .into_iter()
                .filter(|e| e.cf == CF_PRIMARY)
                .map(|e| crate::wal::WalRecord {
                    seq: e.gseq,
                    op: e.op,
                    key: e.key,
                    value: e.value,
                })
                .collect();
            let oldest = es.iter().map(|r| r.seq).min().unwrap_or(u64::MAX);
            // records_after 按 gseq 升序（CF 过滤后仍升序）
            if oldest == u64::MAX {
                es.clear();
            }
            (oldest, es)
        } else {
            self.primary.wal_records_since(since_seq)?
        };
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
