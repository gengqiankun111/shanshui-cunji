//! 启动与恢复：open / open_inner / WAL 回放 / TTL 过期判断与时间分桶辅助。

use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use tracing::{info, warn};

use crate::config::model::Config;
use crate::error::{Error, Result};
use crate::sstable::SstReader;
use crate::storage::manifest::{self, MANIFEST_FILE};
use crate::wal::{OP_DELETE, OP_PUT, RingWal, WalBackend, WalMode, WalReader, WalWriter};

use super::*;

impl ColumnFamily {
    /// 打开（或创建）一个列族：加载 Manifest/SST、回放 WAL。WAL 与数据同目录（旧布局）。
    pub fn open(name: &str, dir: &Path, cfg: &Config) -> Result<Self> {
        Self::open_with_wal_dir(name, dir, None, cfg)
    }

    /// 打开列族并指定独立 WAL 目录（Ex-5.10 多 SSD 条带化：WAL 独占最快盘）。
    pub fn open_with_wal_dir(
        name: &str,
        dir: &Path,
        wal_dir: Option<&Path>,
        cfg: &Config,
    ) -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            Self::open_inner(name, dir, wal_dir, cfg, None)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self::open_inner(name, dir, wal_dir, cfg)
        }
    }

    /// V 项：打开列族并注入 io_uring 后端池（Linux + `runtime.io_uring_enabled`）——
    /// 加载的 SST 块读与 WAL fsync 经 SQPOLL 队列；Windows / 未启用传 None（同步路径）。
    #[cfg(target_os = "linux")]
    pub fn open_with_io_uring(
        name: &str,
        dir: &Path,
        wal_dir: Option<&Path>,
        cfg: &Config,
        iou: Option<std::sync::Arc<crate::io_queue::backend::IoUringPool>>,
    ) -> Result<Self> {
        Self::open_inner(name, dir, wal_dir, cfg, iou)
    }

    fn open_inner(
        name: &str,
        dir: &Path,
        wal_dir: Option<&Path>,
        cfg: &Config,
        #[cfg(target_os = "linux")]
        iou: Option<std::sync::Arc<crate::io_queue::backend::IoUringPool>>,
    ) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        if let Some(w) = wal_dir {
            std::fs::create_dir_all(w)?;
            std::fs::create_dir_all(w.join(name))?; // 独立 WAL 盘列族子目录
        }
        if cfg.storage.time_bucket != "day" {
            return Err(Error::Config(format!(
                "storage.time_bucket 仅支持 day（当前: {}）",
                cfg.storage.time_bucket
            )));
        }
        let manifest_path = dir.join(MANIFEST_FILE);
        let (mut sst_names, mut sst_levels, next_sst_id) = manifest::load(&manifest_path)?;

        // TTL 过期桶清理：按天整文件删除（删除成本 O(1)，无墓碑；design 5.4）
        if let Some(ttl_days) = cfg.storage.ttl_days {
            let cutoff = today_epoch_days() - ttl_days as i64;
            let mut removed = 0usize;
            let mut kept = Vec::new();
            for (i, f) in sst_names.iter().enumerate() {
                let keep = match parse_sst_date(f) {
                    Some(days) => days >= cutoff,
                    None => true, // 默认桶（无日期前缀）永不过期
                };
                if keep {
                    kept.push((f.clone(), sst_levels.get(i).copied().unwrap_or(0)));
                } else {
                    let p = dir.join(f);
                    if p.exists() {
                        std::fs::remove_file(&p).map_err(Error::Io)?;
                        removed += 1;
                    }
                }
            }
            if removed > 0 {
                sst_names = kept.iter().map(|(f, _)| f.clone()).collect();
                sst_levels = kept.iter().map(|(_, l)| *l).collect();
                info!("列族 [{name}] TTL 过期桶清理: 删除 {removed} 个 SST");
            }
        }

        // 打开全部 SST（新→旧）
        let mut ssts = Vec::new();
        let mut sst_levels_loaded = Vec::new();
        for (i, f) in sst_names.iter().enumerate() {
            let p = dir.join(f);
            if !p.exists() {
                warn!("Manifest 中的 SST 缺失，跳过: {}", p.display());
                continue;
            }
            // V 项：Linux + io_uring 启用时经 open_with_io_uring 注入 SQPOLL 池，否则同步打开
            #[cfg(target_os = "linux")]
            let open_res = SstReader::open_with_io_uring(
                &p,
                cfg.sstable.index_granularity as usize,
                iou.clone(),
            );
            #[cfg(not(target_os = "linux"))]
            let open_res =
                SstReader::open_with_granularity(&p, cfg.sstable.index_granularity as usize);
            match open_res {
                Ok(r) => {
                    ssts.push(r);
                    sst_levels_loaded.push(sst_levels.get(i).copied().unwrap_or(0));
                }
                Err(e) => {
                    return Err(Error::Corrupted(format!(
                        "SST 加载失败 {}: {e}",
                        p.display()
                    )))
                }
            }
        }
        info!(
            "列族 [{name}] 加载 {} 个 SST，下一个 id={next_sst_id}",
            ssts.len()
        );

        let block_cache = Arc::new(BlockCache::new(
            cfg.blockcache.max_memory_mb * 1024 * 1024,
            cfg.blockcache.block_size_kb * 1024,
        ));
        let compression = Compression::from_str(&cfg.sstable.compression)?;
        let compression_level = cfg.sstable.compression_level as i32;
        // Ex-8.12：L2+ 冷档 zstd 级别（0 = 不分层）
        let compression_level_l2 = cfg.sstable.compression_level_l2 as i32;

        let wal_path = match wal_dir {
            // 独立 WAL 盘按列族分子目录（多列族同名 wal.log 隔离；Ex-5.10 条带化）
            Some(w) => w.join(name).join(WAL_FILE),
            None => dir.join(WAL_FILE),
        };
        // WAL 模式（design 4.3 阶段 3）：
        // - append：传统追加文件（不截断旧 WAL），回放恢复 MemTable 后继续写入；
        // - ring：预分配环形文件，Flush 后上报刷盘游标腾空空间，满则强制 Flush。
        let (wal, wal_records) = match WalMode::parse(&cfg.storage.wal_mode) {
            WalMode::Ring => {
                let size = (cfg.storage.wal_ring_size_mb as usize) * 1024 * 1024;
                let (mut ring, recs) = RingWal::open_or_create(&wal_path, size)?;
                // V 项：注入 io_uring 池（Linux + 启用时）——WAL fsync 走 SQPOLL 队列
                #[cfg(target_os = "linux")]
                ring.set_io_uring(iou.clone());
                (WalBackend::Ring(ring), recs)
            }
            WalMode::Append => {
                // 先创建（open_append 兼容新库空文件），再回放
                let mut w = WalWriter::open_append(&wal_path, 1, false)?;
                // V 项：注入 io_uring 池（Linux + 启用时）——WAL fsync 走 SQPOLL 队列
                #[cfg(target_os = "linux")]
                w.set_io_uring(iou.clone());
                let recs = WalReader::recover(&wal_path)?;
                (WalBackend::Append(w), recs)
            }
        };
        let mut cf = Self {
            name: name.to_string(),
            dir: dir.to_path_buf(),
            cfg: cfg.memtable.clone(),
            compression,
            compression_level,
            compression_level_l2,
            block_size: cfg.blockcache.block_size_kb * 1024,
            bloom_fpr: cfg.sstable.bloom_fpr,
            pax_hot_fields: cfg.storage.hot_fields.clone(),
            ttl_days: cfg.storage.ttl_days,
            ttl_field: cfg.storage.ttl_field.clone(),
            index_granularity: cfg.sstable.index_granularity as usize,
            split_by_table: false,
            io_limiter: if cfg.storage.io_rate_limit_mb > 0 {
                Some(Mutex::new(crate::io_scheduler::IoRateLimiter::new(
                    cfg.storage.io_rate_limit_mb * 1024 * 1024,
                )))
            } else {
                None
            },
            scan_limiter: Mutex::new(None),
            l0_stall_threshold: cfg.storage.l0_stall_threshold,
            l0_stall_min: cfg.storage.l0_stall_min.max(2),
            l0_stall_max: cfg.storage.l0_stall_max.max(cfg.storage.l0_stall_min.max(2)),
            write_pressure: AtomicU64::new(0.0f64.to_bits()),
            l0_max_size_bytes: cfg.storage.l0_max_size_mb * 1024 * 1024,
            compact_input_max_bytes: cfg.storage.compact_input_max_mb * 1024 * 1024,
            // Ex-8.11：L1/L2 段数触发阈值（0 = 现行为）
            l1_trigger_files: AtomicUsize::new(cfg.storage.l1_trigger_files),
            l2_trigger_files: cfg.storage.l2_trigger_files,
            // P129：多表 per-table L0 压实触发阈值（0 = 关闭；单表库由多表门控自动不启用）
            per_table_l0_trigger: cfg.storage.per_table_l0_trigger,
            per_table_compact_runs: AtomicU64::new(0),
            seq_min: RwLock::new(std::collections::HashMap::new()),
            compaction_cooldown: cfg.storage.compaction_cooldown,
            merge_round: AtomicU64::new(0),
            cooldown: Mutex::new(std::collections::HashMap::new()),
            // P4-A：写入速率滑动窗口（默认 8 次 flush 窗口，阈值 4 → 窗口内 >4 次 flush 算爆发）
            write_rate_window: Mutex::new(Vec::with_capacity(8)),
            write_rate_window_size: cfg.storage.compaction_write_rate_window.max(2),
            write_rate_burst_threshold: cfg.storage.compaction_write_rate_burst.max(1),
            base_l1_trigger: cfg.storage.l1_trigger_files,
            flush_counter: AtomicU64::new(0),
            flush_sst_count: AtomicUsize::new(0),
            bloom: BloomCounters::default(),
            sst_written: AtomicU64::new(0),
            mvcc_keep_floor: AtomicU64::new(0),
            memtable: MemTableBuffer::new(),
            ssts: {
                let loaded: Vec<Arc<SstReader>> = ssts.into_iter().map(Arc::new).collect();
                let (layer_ranges, layer_indices, l0_table_ranges) =
                    Self::build_layer_meta(&loaded, &sst_levels_loaded);
                let sizes: Vec<u64> = loaded.iter().map(|r| r.file_len()).collect();
                ArcSwap::new(Arc::new(SstSnapshot {
                    ssts: loaded,
                    levels: sst_levels_loaded,
                    layer_ranges,
                    layer_indices,
                    l0_table_ranges,
                    sizes,
                }))
            },
            sst_mutate: Mutex::new(()),
            block_cache,
            seq: AtomicU64::new(1),
            next_sst_id: AtomicU64::new(next_sst_id),
            wal: Arc::new(Mutex::new(wal)),
            external_seq: None,
            external_wal: false,
            external_cf_id: 0,
            flushed_cb: None,
        };

        // WAL 回放（幂等：以 seq 排序重放，同 key 后写覆盖先写）
        let max_seq = cf.replay_records(&wal_records)?;
        // 新写入 seq 必须接续已回放的最大 seq，避免同 key 版本冲突（重启恢复的正确性）。
        // 截断后重建的 WAL 含头（持久化 next_seq，M8-P5）且无回放记录 → 保持头值不覆盖。
        if max_seq > 0 {
            cf.wal.lock().unwrap().resume_seq(max_seq + 1);
        }
        Ok(cf)
    }

    /// 共享 WAL 句柄（组提交后台线程落盘兜底，M8）。
    pub fn wal_handle(&self) -> Arc<Mutex<WalBackend>> {
        Arc::clone(&self.wal)
    }

    /// 回放 WAL 记录：重建 MemTable 并推进 seq。返回已回放的最大 seq（无记录为 0）。
    /// TTL 启用时，已过期的记录（按文档 ttl_field 判断）不回放入 MemTable。
    fn replay_records(&mut self, recs: &[crate::wal::WalRecord]) -> Result<u64> {
        let mut max_seq = 0u64;
        for r in recs {
            match r.op {
                OP_PUT => {
                    if let Some(v) = &r.value {
                        if !self.is_ttl_expired(v) {
                            self.memtable.put(r.key.clone(), r.seq, v.clone());
                        }
                    }
                }
                OP_DELETE => self.memtable.delete(r.key.clone(), r.seq),
                other => return Err(Error::Corrupted(format!("WAL 未知 op {other}"))),
            }
            max_seq = max_seq.max(r.seq);
        }
        if !recs.is_empty() {
            self.seq.store(max_seq + 1, Ordering::Relaxed);
            info!(
                "列族 [{}] WAL 回放 {} 条，seq 推进至 {}",
                self.name,
                recs.len(),
                max_seq + 1
            );
        }
        Ok(max_seq)
    }

    /// Task-026：engine 级队列 WAL 回放（external 模式；恢复按 gseq 归并后分发到各 CF）。
    /// 与 `replay_records` 同语义（TTL 过期过滤、幂等覆盖），但记录来自 per-CPU 队列文件
    /// （调用方已按目标 CF 过滤）。返回已回放最大 gseq。
    pub fn replay_external(&self, recs: &[crate::engine::percpu_wal::WalEntry]) -> Result<u64> {
        let mut max_seq = 0u64;
        for r in recs {
            match r.op {
                OP_PUT => {
                    if let Some(v) = &r.value {
                        if !self.is_ttl_expired(v) {
                            self.memtable.put(r.key.clone(), r.gseq, v.clone());
                        }
                    }
                }
                OP_DELETE => self.memtable.delete(r.key.clone(), r.gseq),
                other => return Err(Error::Corrupted(format!("队列 WAL 未知 op {other}"))),
            }
            max_seq = max_seq.max(r.gseq);
        }
        if !recs.is_empty() {
            self.seq.store(max_seq + 1, Ordering::Relaxed);
            info!(
                "列族 [{}] 队列 WAL 回放 {} 条，seq 推进至 {}",
                self.name,
                recs.len(),
                max_seq + 1
            );
        }
        Ok(max_seq)
    }

    /// TTL 过期判断：文档 ttl_field 对应桶天数早于截止天数（默认桶/不可解析永不过期）。
    fn is_ttl_expired(&self, value: &[u8]) -> bool {
        let Some(days) = self.document_bucket_days(value) else {
            return false;
        };
        let cutoff = today_epoch_days() - self.ttl_days.unwrap_or(0) as i64;
        days < cutoff
    }
}

// ---------------------------------------------------------------------------
// TTL 时间分桶辅助（design 5.4，无 chrono 依赖的 civil calendar 天数计算）
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// TTL 时间分桶辅助（design 5.4，无 chrono 依赖的 civil calendar 天数计算）
// ---------------------------------------------------------------------------

/// 秒级时间戳 → UTC 纪元天数（整数除法，UTC 基准）。
pub(crate) fn epoch_days(secs: i64) -> i64 {
    secs.div_euclid(86_400)
}

/// 当前 UTC 纪元天数。
pub(crate) fn today_epoch_days() -> i64 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    epoch_days(secs)
}

/// 解析 TTL 桶 SST 文件名 `sst-{days:08}-{id:08}.sst` → 纪元天数；
/// 默认桶（无日期前缀 `sst-{id:08}.sst`）返回 None（永不过期）。
pub(crate) fn parse_sst_date(fname: &str) -> Option<i64> {
    let rest = fname.strip_prefix(SST_PREFIX)?;
    let (days_str, _rest) = rest.split_once('-')?;
    days_str.parse::<i64>().ok()
}
