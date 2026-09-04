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
            active_snapshots: RwLock::new(std::collections::BTreeSet::new()),
            live_docids: std::sync::Mutex::new(None),
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
