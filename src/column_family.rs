//! 列族框架 + 主数据 CRUD（design 4.1 / development 步骤 7）。
//!
//! 物理目录布局：
//! ```text
//! {data_dir}/{cf_name}/
//!   ├── wal.log         # 本列族 WAL（组提交）
//!   ├── manifest.json   # SST 文件清单（新→旧），重启时按序加载
//!   └── sst-{id:08}.sst # 刷盘生成的不可变有序文件
//! ```
//!
//! 读路径：MemTable(Mutable → Immutable) → SST 新→旧，首个命中即最新；
//! 写路径：WAL append → MemTable，超阈值冻结切换并后台（MVP 同步）刷盘；
//! 重启恢复：manifest 加载全部 SST + WAL 回放重建 MemTable。
//!
//! 已知 MVP 局限（步骤 9 修复）：flush 时跳过 Tombstone 条目，
//! 删除标记只存在于 WAL/MemTable 生命周期内；跨 flush 的删除一致性由步骤 9 补齐。

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::blockcache::{BlockCache, BlockCacheKey};
use crate::bloom::BloomFilter;
use crate::config::model::{Config, MemtableConfig};
use crate::error::{Error, Result};
use crate::keys::{decode_docid, encode_docid};
use crate::memtable::{MemTable, MemTableBuffer};
use crate::sstable::{Compression, SstFooter, SstReader, SstWriter, FLAG_DELETE, FLAG_PUT};
use crate::wal::{RingWal, WalBackend, WalMode, WalReader, WalWriter, OP_DELETE, OP_PUT};

/// SST 文件前缀。
const SST_PREFIX: &str = "sst-";
const WAL_FILE: &str = "wal.log";
const MANIFEST_FILE: &str = "manifest.json";

/// Manifest：列族内 SST 文件清单（新→旧顺序）。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Manifest {
    /// 最近刷盘的文件在最前（读路径优先命中）。
    sst_files: Vec<String>,
    /// 每个 SST 的层号（design 4.5 二期 Leveled，M6-2）：0 = 刷盘产物（允许重叠），
    /// 1 / 2 = Compaction 输出（层内 key 范围不重叠）。旧 Manifest 缺省按全 0（L0）兼容。
    #[serde(default)]
    levels: Vec<u32>,
    /// 下一个可用 SST id。
    next_sst_id: u64,
}

/// 主数据列族（每 CF 一把粗粒度锁，MVP 保证正确性；并发细化在阶段 1.5 拆分）。
pub struct ColumnFamily {
    name: String,
    dir: PathBuf,
    cfg: MemtableConfig,
    compression: Compression,
    compression_level: i32,
    block_size: usize,
    /// 布隆假阳性率（`sstable.bloom_fpr`，分区布隆每块构建用）。
    bloom_fpr: f64,
    /// PAX 热字段白名单（阶段 1.5，来自 [storage] hot_fields；空 = 行式）。
    pax_hot_fields: Vec<String>,
    /// TTL 天数（None = 关闭；开启后 SST 按文档 ttl_field 分天桶，过期整文件删除）。
    ttl_days: Option<u32>,
    /// TTL 时间字段名（文档 JSON 内数值秒级时间戳）。
    ttl_field: String,
    /// 两级索引粒度（`sstable.index_granularity`，每 N 块一条 Level 1 摘要）。
    index_granularity: usize,
    /// 后台 IO 限速器（design 4.5 阶段 3；`storage.io_rate_limit_mb`，None = 不限速）。
    io_limiter: Option<crate::io_scheduler::IoRateLimiter>,
    /// L0 段数阈值（`storage.l0_stall_threshold`，超过判需要 Compaction）。
    l0_stall_threshold: usize,
    /// 双缓冲 MemTable。
    memtable: MemTableBuffer,
    /// SST 文件（新→旧；读路径按序取首个命中，依赖"新文件在前"版本语义）。
    ssts: Vec<SstReader>,
    /// 与 `ssts` 平行的层号（design 4.5 二期 Leveled Compaction，M6-2）。
    sst_levels: Vec<u32>,
    /// 共享块缓存（跨 CF 共享）。
    block_cache: Arc<BlockCache>,
    /// 单调 seq 分配器（跨重启由 WAL 恢复推进）。
    seq: AtomicU64,
    /// 下一个 SST 文件 id（跨重启由 Manifest 恢复，防止覆盖旧文件）。
    next_sst_id: u64,
    /// 当前 WAL 写入器（append 追加 / ring 环形，design 4.3 阶段 3）。
    /// `Arc<Mutex>` 共享：组提交后台线程（M8）可独立触发落盘兜底。
    wal: Arc<Mutex<WalBackend>>,
    /// 外部全局 seq（MVCC，engine 层统一分配，M7-1）：Some 时写入走外部计数（跨列族一致）；
    /// None = 独立列族（测试 / 单 CF 场景）用内部 WAL seq。
    external_seq: Option<Arc<AtomicU64>>,
}

/// Compaction 结果报告（design 4.5 阶段 3 / 二期 Leveled，M6-2）。
#[derive(Debug, Clone, Copy)]
pub struct CompactReport {
    /// 被合并的旧段数。
    pub merged_ssts: usize,
    /// 被消除的重复旧版本键数（含被 Tombstone 覆盖的键）。
    pub kept_keys: usize,
    /// 释放的磁盘字节数。
    pub freed_bytes: u64,
    /// 压实输出所在层（0 = 未压实；1 / 2 = L1 / L2）。
    pub out_level: u32,
}

impl ColumnFamily {
    /// 打开（或创建）一个列族：加载 Manifest/SST、回放 WAL。
    pub fn open(name: &str, dir: &Path, cfg: &Config) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        if cfg.storage.time_bucket != "day" {
            return Err(Error::Config(format!(
                "storage.time_bucket 仅支持 day（当前: {}）",
                cfg.storage.time_bucket
            )));
        }
        let manifest_path = dir.join(MANIFEST_FILE);
        let (mut sst_names, mut sst_levels, next_sst_id) = load_manifest(&manifest_path)?;

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
            match SstReader::open_with_granularity(&p, cfg.sstable.index_granularity as usize) {
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

        let wal_path = dir.join(WAL_FILE);
        // WAL 模式（design 4.3 阶段 3）：
        // - append：传统追加文件（不截断旧 WAL），回放恢复 MemTable 后继续写入；
        // - ring：预分配环形文件，Flush 后上报刷盘游标腾空空间，满则强制 Flush。
        let (wal, wal_records) = match WalMode::parse(&cfg.storage.wal_mode) {
            WalMode::Ring => {
                let size = (cfg.storage.wal_ring_size_mb as usize) * 1024 * 1024;
                let (ring, recs) = RingWal::open_or_create(&wal_path, size)?;
                (WalBackend::Ring(ring), recs)
            }
            WalMode::Append => {
                // 先创建（open_append 兼容新库空文件），再回放
                let w = WalWriter::open_append(&wal_path, 1, false)?;
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
            block_size: cfg.blockcache.block_size_kb * 1024,
            bloom_fpr: cfg.sstable.bloom_fpr,
            pax_hot_fields: cfg.storage.hot_fields.clone(),
            ttl_days: cfg.storage.ttl_days,
            ttl_field: cfg.storage.ttl_field.clone(),
            index_granularity: cfg.sstable.index_granularity as usize,
            io_limiter: if cfg.storage.io_rate_limit_mb > 0 {
                Some(crate::io_scheduler::IoRateLimiter::new(
                    cfg.storage.io_rate_limit_mb * 1024 * 1024,
                ))
            } else {
                None
            },
            l0_stall_threshold: cfg.storage.l0_stall_threshold,
            memtable: MemTableBuffer::new(),
            ssts,
            sst_levels: sst_levels_loaded,
            block_cache,
            seq: AtomicU64::new(1),
            next_sst_id,
            wal: Arc::new(Mutex::new(wal)),
            external_seq: None,
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

    /// TTL 过期判断：文档 ttl_field 对应桶天数早于截止天数（默认桶/不可解析永不过期）。
    fn is_ttl_expired(&self, value: &[u8]) -> bool {
        let Some(days) = self.document_bucket_days(value) else {
            return false;
        };
        let cutoff = today_epoch_days() - self.ttl_days.unwrap_or(0) as i64;
        days < cutoff
    }

    /// 写入（主键点写，便捷封装）。
    pub fn put(&mut self, docid: u64, value: Vec<u8>) -> Result<u64> {
        self.put_bytes(encode_docid(docid).to_vec(), value)
    }

    /// 写入原始字节键（组合索引等任意 key 使用）。
    pub fn put_bytes(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        let seq = self.put_bytes_nosync(key, value)?;
        self.sync_wal()?;
        Ok(seq)
    }

    /// 批量写入（不逐条 fsync，由调用方最终 `sync_wal` 统一提交）。
    /// 供亿级数据压测/导入使用；强安全模式逐条写请用 `put_bytes`。
    pub fn put_bytes_nosync(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        let seq = match self.wal_append(OP_PUT, &key, Some(&value)) {
            Ok(s) => s,
            // 环形 WAL 缓冲满：先落盘腾空（必要时强制 Flush）再重试
            Err(Error::WalFull(_)) => {
                self.ensure_wal_room()?;
                self.wal
                    .lock()
                    .unwrap()
                    .append(OP_PUT, &key, Some(&value))?
            }
            Err(e) => return Err(e),
        };
        self.memtable.put(key, seq, value);
        self.maybe_flush()?;
        Ok(seq)
    }

    /// 统一提交 WAL 缓冲（批量写入结束时调用）。
    /// 环形 WAL 落盘若需回绕覆盖未刷盘记录 → 强制 Flush 后重试。
    pub fn sync_wal(&mut self) -> Result<()> {
        let r = self.wal.lock().unwrap().sync();
        match r {
            Ok(()) => Ok(()),
            Err(Error::WalFull(_)) => {
                self.switch_and_flush()?;
                self.wal.lock().unwrap().sync()
            }
            Err(e) => Err(e),
        }
    }

    /// 删除（Tombstone，跨 flush/重启一致，见步骤 9）。
    pub fn delete(&mut self, docid: u64) -> Result<u64> {
        self.delete_bytes(encode_docid(docid).to_vec())
    }

    /// 删除原始字节键。
    pub fn delete_bytes(&mut self, key: Vec<u8>) -> Result<u64> {
        let seq = match self.wal_append(OP_DELETE, &key, None) {
            Ok(s) => s,
            Err(Error::WalFull(_)) => {
                self.ensure_wal_room()?;
                self.wal_append(OP_DELETE, &key, None)?
            }
            Err(e) => return Err(e),
        };
        self.sync_wal()?;
        self.memtable.delete(key, seq);
        Ok(seq)
    }

    /// 环形 WAL 满处理：先落盘缓冲；仍满（回绕需覆盖未刷盘记录）则强制 Flush 后重试。
    fn ensure_wal_room(&mut self) -> Result<()> {
        let r = self.wal.lock().unwrap().sync();
        match r {
            Ok(()) => Ok(()),
            Err(Error::WalFull(_)) => {
                self.switch_and_flush()?;
                self.wal.lock().unwrap().sync()
            }
            Err(e) => Err(e),
        }
    }

    /// 删除指定前缀的全部记录（阶段 1.5 Delta CF：全量 put 覆盖后清空该 docid 的增量）。
    /// 前缀上界 = prefix ++ [0xFF;4]（字段名长度前缀上限），闭区间扫描后逐条墓碑。
    pub fn delete_prefix(&mut self, prefix: &[u8]) -> Result<u64> {
        let mut end = prefix.to_vec();
        end.extend_from_slice(&[0xFF; 4]);
        let rows = self.scan_raw_range(Some(prefix), Some(&end))?;
        let mut deleted = 0u64;
        for (k, _) in rows {
            if k.starts_with(prefix) {
                self.delete_bytes(k)?;
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// 查询（主键点查，便捷封装）。返回 (value, seq)，已过滤 Tombstone。
    pub fn get(&mut self, docid: u64) -> Result<Option<(Vec<u8>, u64)>> {
        self.get_bytes(&encode_docid(docid))
    }

    /// 查询原始字节键。返回 (value, seq)，已过滤 Tombstone。
    pub fn get_bytes(&mut self, key: &[u8]) -> Result<Option<(Vec<u8>, u64)>> {
        if let Some(e) = self.memtable.get(key) {
            return Ok(e.value.map(|v| (v, e.seq)));
        }
        let cache = Arc::clone(&self.block_cache);
        for sst in &mut self.ssts {
            match get_from_sst(sst, &cache, key)? {
                // 命中：最新版本。value=None 为 Tombstone → 视为不存在
                Some((value, seq)) => return Ok(value.map(|v| (v, seq))),
                None => continue, // 未命中该 SST，继续查更旧的
            }
        }
        Ok(None)
    }

    /// 快照读（design 4.7 二期 MVCC，M6-3）：返回 **seq ≤ `snapshot_seq`** 的最新版本。
    /// 遍历 MemTable + 全部 SST，取满足条件的最大 seq；该 seq 为 Tombstone 则视为不存在
    /// （快照点已删除；快照点之前的历史版本仍可见）。
    /// 局限：MemTable 仅保留每 key 最新版本，未刷盘覆盖的历史版本无法回读（多版本保留留后续）。
    pub fn get_bytes_at(
        &mut self,
        key: &[u8],
        snapshot_seq: u64,
    ) -> Result<Option<(Vec<u8>, u64)>> {
        let mut best: Option<(u64, Option<Vec<u8>>)> = None; // (seq, value)
        if let Some(e) = self.memtable.get(key) {
            if e.seq <= snapshot_seq && best.as_ref().map_or(true, |(s, _)| e.seq > *s) {
                best = Some((e.seq, e.value));
            }
        }
        let cache = Arc::clone(&self.block_cache);
        for sst in &mut self.ssts {
            if let Some((value, seq)) = get_from_sst(sst, &cache, key)? {
                if seq <= snapshot_seq && best.as_ref().map_or(true, |(s, _)| seq > *s) {
                    best = Some((seq, value));
                }
            }
        }
        match best {
            Some((seq, Some(v))) => Ok(Some((v, seq))),
            Some((_, None)) => Ok(None), // 快照点已删除
            None => Ok(None),
        }
    }

    /// 范围扫描 [start, end]（闭区间，None 端无边界）：先收集 MemTable 与各 SST 候选，
    /// 以最大 seq 去重（最新覆盖，Tombstone 覆盖旧值），返回按 docid 升序的 (docid, value) 列表。
    pub fn scan_range(
        &mut self,
        start: Option<u64>,
        end: Option<u64>,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let start_key = start.map(|s| encode_docid(s).to_vec());
        let end_key = end.map(|e| encode_docid(e).to_vec());
        let rows = self.scan_raw_range(start_key.as_deref(), end_key.as_deref())?;
        let mut out = Vec::with_capacity(rows.len());
        for (k, v) in rows {
            out.push((
                decode_docid(&k).map_err(|_| Error::Corrupted("docid 解码失败".into()))?,
                v,
            ));
        }
        Ok(out)
    }

    /// 原始字节键范围扫描（组合索引前缀查询使用）。返回升序 (key, value) 列表，Tombstone 已过滤。
    pub fn scan_raw_range(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // 候选收集：key → (seq, value)，value=None 表示 Tombstone
        let mut merged: std::collections::HashMap<Vec<u8>, (u64, Option<Vec<u8>>)> =
            std::collections::HashMap::new();

        // MemTable 扫描（含 Tombstone 覆盖）
        self.memtable.scan_range(start, end, |key, e| {
            merge_candidate_bytes(&mut merged, key.to_vec(), e.seq, e.value.clone());
        });

        // SST 范围扫描（Zone Map 剪枝已内置在 scan_range）
        for sst in &mut self.ssts {
            sst.scan_range(start, end, |k, v, seq| {
                merge_candidate_bytes(&mut merged, k.to_vec(), seq, v.map(|x| x.to_vec()));
            })?;
        }

        let mut out: Vec<(Vec<u8>, Vec<u8>)> = merged
            .into_iter()
            .filter_map(|(key, (_seq, value))| value.map(|v| (key, v)))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// 流式范围扫描（M8-P10）：k-way merge（memtable 双缓冲 + 各 SST 有序源）按 key 升序
    /// 回调 `f(key, value)`——同 key 取最大 seq（最新版本），Tombstone 跳过。
    /// 内存 O(块) 不随扫描总量膨胀（对比 `scan_raw_range` 全量收集 O(total)），
    /// 配合分页 skip/take 可提前终止。语义与 `scan_raw_range` 完全一致。
    pub fn scan_stream<F: FnMut(&[u8], &[u8]) -> Result<()>>(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        mut f: F,
    ) -> Result<()> {
        // 源：memtable（immutable + mutable）借用 &self.memtable；SST 借用 &mut self.ssts
        let mut mem_iters = self.memtable.iter_range(start, end);
        let mut sst_iters: Vec<crate::sstable::SstRangeIter> = Vec::new();
        for sst in &mut self.ssts {
            sst_iters.push(crate::sstable::SstRangeIter::new(sst, start, end)?);
        }
        let mem_count = mem_iters.len();
        let total = mem_count + sst_iters.len();
        if total == 0 {
            return Ok(());
        }
        // 各源当前条目 (key, value, seq)
        let mut cur: Vec<Option<(Vec<u8>, Option<Vec<u8>>, u64)>> = Vec::with_capacity(total);
        for it in mem_iters.iter_mut() {
            cur.push(it.next().transpose()?);
        }
        for it in sst_iters.iter_mut() {
            cur.push(it.next().transpose()?);
        }
        // k-way merge 用最小堆（O(N log K)，避免每轮线性扫全部源 O(N·K)——K 大时不可接受）
        let mut heap: std::collections::BinaryHeap<std::cmp::Reverse<(Vec<u8>, usize)>> =
            std::collections::BinaryHeap::new();
        for (i, c) in cur.iter().enumerate() {
            if let Some((k, _, _)) = c {
                heap.push(std::cmp::Reverse((k.clone(), i)));
            }
        }
        loop {
            let Some(std::cmp::Reverse((min_key, i0))) = heap.pop() else {
                break; // 全部耗尽
            };
            // 收集所有 key == min_key 的源（同 key 候选），取最大 seq（最新版本覆盖旧版本）
            let mut to_advance: Vec<usize> = vec![i0];
            while let Some(std::cmp::Reverse((k, i))) = heap.peek() {
                if *k == min_key {
                    to_advance.push(*i);
                    heap.pop();
                } else {
                    break;
                }
            }
            let mut best_seq = 0u64;
            let mut best_val: Option<Vec<u8>> = None;
            for i in to_advance {
                let (k, v, seq) = cur[i].take().unwrap();
                debug_assert!(k == min_key, "同 key 归并");
                if seq >= best_seq {
                    best_seq = seq;
                    best_val = v;
                }
                // 推进该源到下一跳，重新入堆
                cur[i] = if i < mem_count {
                    mem_iters[i].next().transpose()?
                } else {
                    sst_iters[i - mem_count].next().transpose()?
                };
                if let Some((nk, _, _)) = &cur[i] {
                    heap.push(std::cmp::Reverse((nk.clone(), i)));
                }
            }
            // Tombstone（最新版本 value=None）→ 该 key 视为删除，不输出
            if let Some(v) = best_val {
                f(min_key.as_slice(), &v)?;
            }
        }
        Ok(())
    }

    /// 范围扫描并保留 seq 与 Tombstone（MVCC 快照 Delta 隔离用，M7-1）：
    /// 返回升序 `(key, seq, value)`，value=None 表示删除标记；每 key 仅保留最大 seq 版本。
    pub fn scan_raw_range_with_seq(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, u64, Option<Vec<u8>>)>> {
        let mut merged: std::collections::HashMap<Vec<u8>, (u64, Option<Vec<u8>>)> =
            std::collections::HashMap::new();
        self.memtable.scan_range(start, end, |key, e| {
            merge_candidate_bytes(&mut merged, key.to_vec(), e.seq, e.value.clone());
        });
        for sst in &mut self.ssts {
            sst.scan_range(start, end, |k, v, seq| {
                merge_candidate_bytes(&mut merged, k.to_vec(), seq, v.map(|x| x.to_vec()));
            })?;
        }
        let mut out: Vec<(Vec<u8>, u64, Option<Vec<u8>>)> = merged
            .into_iter()
            .map(|(key, (seq, value))| (key, seq, value))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// MemTable 超阈值 → 冻结并刷盘（MVP 同步刷盘）。
    fn maybe_flush(&mut self) -> Result<()> {
        if self.memtable.mutable_bytes() < self.cfg.max_size_mb * 1024 * 1024 {
            return Ok(());
        }
        self.switch_and_flush()
    }

    /// 冻结 Mutable → 刷盘为 SST → 更新 Manifest → 释放 Immutable。
    pub fn switch_and_flush(&mut self) -> Result<()> {
        self.memtable.switch();
        let Some(imm) = self.memtable.take_immutable() else {
            return Ok(());
        };
        // 环形 WAL 覆盖安全：Flush 完成后上报 imm 内最大 seq（<= 该 seq 的记录已刷盘可覆盖）
        let flushed_max = imm_scan_max_seq(&imm);
        if self.ttl_days.is_none() {
            self.flush_single(&imm)?;
        } else {
            self.flush_buckets(&imm)?;
        }
        self.wal.lock().unwrap().set_flushed_seq(flushed_max);
        // WAL 截断（M8-P5）：append 模式 flush 后全部记录已刷盘，清空 WAL 保持小文件
        // （避免无限增长 + 大文件 fsync 拖慢写入）；ring 模式自带覆盖回收（no-op）
        self.wal.lock().unwrap().truncate_and_reset()?;
        Ok(())
    }

    /// 原逻辑：整个 Immutable 落盘为单个 SST。
    fn flush_single(&mut self, imm: &MemTable) -> Result<()> {
        let sst_id = self.next_sst_id;
        self.next_sst_id += 1;
        let path = self.dir.join(format!("{SST_PREFIX}{sst_id:08}.sst"));
        self.write_sst(&path, imm)?;

        // 新文件插到最前（读路径优先命中）
        let fname = path.file_name().unwrap().to_string_lossy().to_string();
        self.io_acquire(&path)?;
        self.ssts.insert(
            0,
            SstReader::open_with_granularity(&path, self.index_granularity)?,
        );
        self.sst_levels.insert(0, 0); // 刷盘产物 → L0
        self.persist_manifest()?;
        info!(
            "列族 [{}] 刷盘完成: {} ({} 条)",
            self.name,
            fname,
            imm.len()
        );
        Ok(())
    }

    /// TTL 分桶 flush：按文档 ttl_field 提取的 UTC 天分桶，每个桶一个 SST 文件；
    /// 无时间字段 / 非 JSON 值落入默认桶（无日期前缀，永不过期）。桶内 key 保持升序。
    fn flush_buckets(&mut self, imm: &MemTable) -> Result<()> {
        let mut buckets: std::collections::BTreeMap<Option<i64>, Vec<BucketRow>> =
            std::collections::BTreeMap::new();
        imm.scan(|k, e| {
            let days = match &e.value {
                Some(v) => self.document_bucket_days(v),
                None => None, // Tombstone 无时间 → 默认桶（随对应 key 的桶删除语义由数据决定）
            };
            let flag = if e.value.is_some() {
                FLAG_PUT
            } else {
                FLAG_DELETE
            };
            buckets
                .entry(days)
                .or_default()
                .push((k.to_vec(), e.value.clone(), flag, e.seq));
        });
        let bucket_count = buckets.len();
        for (days, rows) in buckets {
            let sst_id = self.next_sst_id;
            self.next_sst_id += 1;
            let fname = match days {
                Some(d) => format!("{SST_PREFIX}{d:08}-{sst_id:08}.sst"),
                None => format!("{SST_PREFIX}{sst_id:08}.sst"),
            };
            let path = self.dir.join(&fname);
            self.write_rows(&path, &rows)?;
            self.io_acquire(&path)?;
            self.ssts.insert(
                0,
                SstReader::open_with_granularity(&path, self.index_granularity)?,
            );
            self.sst_levels.insert(0, 0); // 刷盘产物 → L0
        }
        self.persist_manifest()?;
        info!(
            "列族 [{}] TTL 分桶刷盘完成: {} 个桶",
            self.name, bucket_count
        );
        Ok(())
    }

    /// 从文档 JSON 提取 ttl_field（数值秒级时间戳）→ UTC 纪元天数；无法解析返回 None。
    fn document_bucket_days(&self, value: &[u8]) -> Option<i64> {
        let v: serde_json::Value = serde_json::from_slice(value).ok()?;
        let secs = v.get(&self.ttl_field)?.as_i64()?;
        Some(epoch_days(secs))
    }

    /// 按行序列写一个 SST（TTL 分桶用；行内 key 已升序）。
    /// 后台 IO 限速：刷盘完成后按实际文件字节数 acquire（design 4.5 阶段 3；不限速时为空操作）。
    fn io_acquire(&mut self, path: &Path) -> Result<()> {
        if let Some(limiter) = &mut self.io_limiter {
            let sz = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            limiter.acquire(sz)?;
        }
        Ok(())
    }

    /// Leveled-Compaction（design 4.5 二期 / M6-2）：分层压实，限制单次压实量。
    ///
    /// - **L0 → L1**：有 L0 段（刷盘产物，允许重叠）时合并 L0 → 单个 L1 段（不合并既有 L1，
    ///   单次压实量 = 单个刷盘批次，有界）；L1 文件数达到层上限时改合并 L0 + 全部 L1（收敛）；
    /// - **L1 → L2**：L0 为空且 L1 段数 > 1 时，合并全部 L1 → 单个 L2 段（压实下沉）；
    /// - 合并语义：按 (key 升序, seq 降序) 排序去重，后写覆盖先写，Tombstone 保留；
    /// - 崩溃安全：新段写入 → fsync → 原子更新 Manifest → 删除旧段；
    /// - 后台 IO 限速：写完后按实际文件字节 acquire。
    pub fn compact(&mut self) -> Result<CompactReport> {
        let (sel, out_level) = select_compaction_inputs(&self.sst_levels, self.l0_stall_threshold);
        if sel.len() <= 1 {
            return Ok(CompactReport {
                merged_ssts: 0,
                kept_keys: 0,
                freed_bytes: 0,
                out_level: 0,
            });
        }
        let old_count = sel.len();
        // ① 读取选中条目
        let mut rows: Vec<(Vec<u8>, u64, Option<Vec<u8>>)> = Vec::new();
        for idx in &sel {
            self.ssts[*idx].iterate(|k, v, seq| {
                rows.push((k.to_vec(), seq, v.map(|x| x.to_vec())));
            })?;
        }
        // ② 排序 + 去重：key 升序、seq 降序，同 key 保留首个（最高 seq）
        rows.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        let kept_keys = rows.len();
        rows.dedup_by(|a, b| a.0 == b.0);

        // ③ 写新段（读路径新文件插最前）
        let sst_id = self.next_sst_id;
        self.next_sst_id += 1;
        let path = self.dir.join(format!("{SST_PREFIX}{sst_id:08}.sst"));
        {
            let mut w = SstWriter::new_with_pax(
                &path,
                self.compression,
                self.compression_level,
                self.block_size,
                rows.len(),
                &self.pax_hot_fields,
                self.bloom_fpr,
            )?;
            for (key, _seq, value) in &rows {
                match value {
                    Some(v) => w.add(key, v, *_seq)?,
                    None => w.add_tombstone(key, *_seq)?,
                }
            }
            w.finish()?;
        }
        self.io_acquire(&path)?;

        // ④ 原子更新 Manifest（先 Manifest 后删旧文件）
        let old_bytes: u64 = sel
            .iter()
            .map(|&i| {
                std::fs::metadata(self.ssts[i].path())
                    .map(|m| m.len())
                    .unwrap_or(0)
            })
            .sum();
        let mut old_ssts = Vec::new();
        let mut kept_ssts = Vec::new();
        let mut kept_levels = Vec::new();
        let mut removed = vec![false; self.ssts.len()];
        for &i in &sel {
            removed[i] = true;
        }
        for (i, sst) in std::mem::take(&mut self.ssts).into_iter().enumerate() {
            if removed[i] {
                old_ssts.push(sst);
            } else {
                kept_ssts.push(sst);
                kept_levels.push(self.sst_levels[i]);
            }
        }
        self.ssts = kept_ssts;
        self.sst_levels = kept_levels;
        self.ssts.insert(
            0,
            SstReader::open_with_granularity(&path, self.index_granularity)?,
        );
        self.sst_levels.insert(0, out_level);
        self.persist_manifest()?;

        // ⑤ 删除旧段（孤儿无害，启动只加载 Manifest）
        for r in &old_ssts {
            let _ = std::fs::remove_file(r.path());
        }
        let freed_bytes = old_bytes.saturating_sub(self.sst_bytes());
        info!(
            "列族 [{}] Compaction 完成: →L{out_level} 合并 {} 段 → 1（保留 {} 键，释放 {} 字节）",
            self.name,
            old_count,
            rows.len(),
            freed_bytes
        );
        Ok(CompactReport {
            merged_ssts: old_count,
            kept_keys: kept_keys.saturating_sub(rows.len()),
            freed_bytes,
            out_level,
        })
    }

    /// 是否需要 Compaction（design 4.5 二期）：
    /// L0 段数超过 `storage.l0_stall_threshold`（L0→L1），或 L0 为空但 L1 段数 > 1（L1→L2），
    /// 或 L0/L1 均空但 L2 段数 > 1（L2 收敛）。
    pub fn needs_compact(&self) -> bool {
        let l0 = self.sst_levels.iter().filter(|l| **l == 0).count();
        let l1 = self.sst_levels.iter().filter(|l| **l == 1).count();
        let l2 = self.sst_levels.iter().filter(|l| **l >= 2).count();
        l0 > self.l0_stall_threshold || (l0 == 0 && (l1 > 1 || l2 > 1))
    }
    /// 全部 SST 文件字节总和。
    pub fn sst_bytes(&self) -> u64 {
        self.ssts
            .iter()
            .filter_map(|r| std::fs::metadata(r.path()).ok().map(|m| m.len()))
            .sum()
    }

    fn write_rows(&self, path: &Path, rows: &[BucketRow]) -> Result<SstFooter> {
        let mut w = SstWriter::new_with_pax(
            path,
            self.compression,
            self.compression_level,
            self.block_size,
            rows.len(),
            &self.pax_hot_fields,
            self.bloom_fpr,
        )?;
        for (k, v, flag, seq) in rows {
            if *flag == FLAG_DELETE {
                w.add_tombstone(k, *seq).expect("SST Tombstone 写入失败");
            } else {
                w.add(k, v.as_deref().unwrap_or(&[]), *seq)
                    .expect("SST 写入失败");
            }
        }
        w.finish()
    }

    /// 将 Immutable 落盘为 SST（Put 与 Tombstone 均落盘，保证跨 flush 删除一致）。
    fn write_sst(&self, path: &Path, imm: &MemTable) -> Result<SstFooter> {
        let mut w = SstWriter::new_with_pax(
            path,
            self.compression,
            self.compression_level,
            self.block_size,
            imm.len(),
            &self.pax_hot_fields,
            self.bloom_fpr,
        )?;
        imm.scan(|k, e| match &e.value {
            Some(v) => w.add(k, v, e.seq).expect("SST 写入失败"),
            None => w.add_tombstone(k, e.seq).expect("SST Tombstone 写入失败"),
        });
        w.finish()
    }

    fn persist_manifest(&self) -> Result<()> {
        // 以磁盘扫描维护清单（新→旧：id 降序），避免依赖 SstReader 内部状态；
        // 层号按文件名从内存 ssts/sst_levels 映射（缺省 0 = L0，兼容旧格式）
        let mut level_by_file: std::collections::HashMap<String, u32> =
            std::collections::HashMap::new();
        for (sst, lv) in self.ssts.iter().zip(&self.sst_levels) {
            if let Some(name) = sst.path().file_name() {
                level_by_file.insert(name.to_string_lossy().to_string(), *lv);
            }
        }
        let mut files: Vec<String> = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let fname = entry.file_name().to_string_lossy().to_string();
            if fname.starts_with(SST_PREFIX) {
                files.push(fname);
            }
        }
        files.sort_by(|a, b| b.cmp(a));
        let levels: Vec<u32> = files
            .iter()
            .map(|f| level_by_file.get(f).copied().unwrap_or(0))
            .collect();
        let m = Manifest {
            sst_files: files,
            levels,
            next_sst_id: self.next_sst_id,
        };
        let text = serde_json::to_string_pretty(&m)
            .map_err(|e| Error::Serialize(format!("Manifest 序列化失败: {e}")))?;
        let tmp = self.dir.join("manifest.json.tmp");
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, self.dir.join(MANIFEST_FILE))?;
        Ok(())
    }

    /// 当前内存占用（字节），供 OOM Guardian / 监控使用。
    pub fn memtable_bytes(&self) -> usize {
        self.memtable.mutable_bytes() + self.memtable.immutable_bytes()
    }

    pub fn sst_count(&self) -> usize {
        self.ssts.len()
    }

    /// 当前 WAL 下一可分配 seq（Engine 快照点来源，design 4.7 MVCC）。
    pub fn wal_next_seq(&self) -> u64 {
        self.wal.lock().unwrap().next_seq()
    }

    /// 接入外部全局 seq 计数器（MVCC，engine 层统一分配，M7-1）。
    /// 在 WAL 回放完成（内部 seq 已推进）后调用，此后写入走外部计数，跨列族一致。
    pub fn set_external_seq(&mut self, seq: Arc<AtomicU64>) {
        self.external_seq = Some(seq);
    }

    /// 分配 seq 并追加 WAL 记录：外部全局 seq 优先（engine MVCC），否则内部自增。
    fn wal_append(&mut self, op: u8, key: &[u8], value: Option<&[u8]>) -> Result<u64> {
        match &self.external_seq {
            Some(ext) => {
                let seq = ext.fetch_add(1, Ordering::Relaxed);
                self.wal.lock().unwrap().append_at(op, key, value, seq)?;
                Ok(seq)
            }
            None => self.wal.lock().unwrap().append(op, key, value),
        }
    }

    /// 取 WAL 中 seq > `since_seq` 的记录（增量备份，M6-5）。
    /// 返回 `(最旧可用 seq, 过滤记录)`：`since_seq != 0` 且最旧可用 seq > since_seq+1 表示
    /// WAL 已被截断（环形覆盖 / 压缩），存在缺口 → 上层应改做全量备份。
    pub fn wal_records_since(
        &mut self,
        since_seq: u64,
    ) -> Result<(u64, Vec<crate::wal::WalRecord>)> {
        let recs = self.wal.lock().unwrap().recover_records()?;
        let oldest = recs.iter().map(|r| r.seq).min().unwrap_or(u64::MAX);
        let filtered: Vec<_> = recs.into_iter().filter(|r| r.seq > since_seq).collect();
        Ok((oldest, filtered))
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// 内部辅助
// ---------------------------------------------------------------------------

/// TTL 分桶行（key + 值 + flag + seq），flush_buckets / write_rows 使用。
type BucketRow = (Vec<u8>, Option<Vec<u8>>, u8, u64);

// ---------------------------------------------------------------------------
// TTL 时间分桶辅助（design 5.4，无 chrono 依赖的 civil calendar 天数计算）
// ---------------------------------------------------------------------------

/// 秒级时间戳 → UTC 纪元天数（整数除法，UTC 基准）。
fn epoch_days(secs: i64) -> i64 {
    secs.div_euclid(86_400)
}

/// 当前 UTC 纪元天数。
fn today_epoch_days() -> i64 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    epoch_days(secs)
}

/// 解析 TTL 桶 SST 文件名 `sst-{days:08}-{id:08}.sst` → 纪元天数；
/// 默认桶（无日期前缀 `sst-{id:08}.sst`）返回 None（永不过期）。
fn parse_sst_date(fname: &str) -> Option<i64> {
    let rest = fname.strip_prefix(SST_PREFIX)?;
    let (days_str, _rest) = rest.split_once('-')?;
    days_str.parse::<i64>().ok()
}

fn load_manifest(path: &Path) -> Result<(Vec<String>, Vec<u32>, u64)> {
    if !path.exists() {
        return Ok((Vec::new(), Vec::new(), 1));
    }
    let text = std::fs::read_to_string(path)?;
    let m: Manifest = serde_json::from_str(&text)
        .map_err(|e| Error::Corrupted(format!("Manifest 解析失败: {e}")))?;
    // 旧 Manifest 无 levels → 全部按 L0 处理（对齐长度）
    let levels = if m.levels.len() == m.sst_files.len() {
        m.levels
    } else {
        vec![0; m.sst_files.len()]
    };
    Ok((m.sst_files, levels, m.next_sst_id))
}

/// 同 key 候选合并：仅保留 seq 更大（更新）的版本；value=None 的 Tombstone 可覆盖旧值。
fn merge_candidate_bytes(
    merged: &mut std::collections::HashMap<Vec<u8>, (u64, Option<Vec<u8>>)>,
    key: Vec<u8>,
    seq: u64,
    value: Option<Vec<u8>>,
) {
    match merged.get(&key) {
        Some((old_seq, _)) if *old_seq >= seq => {}
        _ => {
            merged.insert(key, (seq, value));
        }
    }
}

/// 单 SST 等值查询（布隆剪枝 → 二分定位块 → 块缓存/读盘 → 块内扫描）。
/// 返回 `(value, seq)`：value=None 表示 Tombstone；整体 None 表示该 SST 无此 key。
fn get_from_sst(
    sst: &mut SstReader,
    cache: &BlockCache,
    key: &[u8],
) -> Result<Option<(Option<Vec<u8>>, u64)>> {
    // v3/v4：整文件布隆粗筛
    if let Some(b) = sst.legacy_bloom() {
        if !b.maybe_contains(&key.to_vec()) {
            return Ok(None);
        }
    }
    // 等值定位块：借用精确索引二分，只克隆单条块条目（design 4.4.2 按需，避免克隆整个 Level 2）
    let Some((block_idx, entry)) = sst.locate_indexed_block(key)? else {
        return Ok(None);
    };
    // v5 分区布隆：只校验目标块（design 4.4.2，查询只加载目标块布隆）
    if let Some(pb) = sst.partition_blooms() {
        if let Some(bytes) = pb.get(block_idx) {
            if let Some(b) = BloomFilter::from_bytes(bytes) {
                if !b.maybe_contains(&key.to_vec()) {
                    return Ok(None);
                }
            }
        }
    }
    let ck = BlockCacheKey {
        file: sst.path().to_path_buf(),
        offset: entry.offset,
    };
    let block = if let Some(b) = cache.get(&ck) {
        b
    } else {
        let b = sst.read_block(&entry)?;
        cache.put(ck, b.clone());
        b
    };
    sst.scan_block_for_key(&block, key)
}

/// 扫描 Immutable 记录的最大 seq（环形 WAL 覆盖安全游标：<= 该 seq 均已刷盘）。
fn imm_scan_max_seq(imm: &MemTable) -> u64 {
    let mut max_seq = 0u64;
    imm.scan(|_, e| {
        if e.seq > max_seq {
            max_seq = e.seq;
        }
    });
    max_seq
}

/// 选择本轮压实输入（design 4.5 二期 Leveled，M6-2）：
/// - L0 ≥ 2 段且 L1 文件数 < `limit` → 合并**仅 L0**，输出 L1（单次压实量 = 刷盘批次，有界）；
/// - L0 ≥ 2 段且 L1 已满 → 合并 L0 + 全部 L1 → L1（收敛 L1 文件数）；
/// - L0 空且 L1 > 1 → 合并全部 L1 → L2（压实下沉）；
/// - L0/L1 均空且 L2 > 1 → 合并 L2（异常残留收敛）。
/// 单段 L0（未达 2 段）不压实——等待更多刷盘批次，避免无收益重写。
/// 返回 `(选中段下标, 输出层)`；无需要压实的输入时返回 `(空, 0)`。
fn select_compaction_inputs(levels: &[u32], level_limit: usize) -> (Vec<usize>, u32) {
    let l0: Vec<usize> = levels
        .iter()
        .enumerate()
        .filter(|(_, l)| **l == 0)
        .map(|(i, _)| i)
        .collect();
    let l1: Vec<usize> = levels
        .iter()
        .enumerate()
        .filter(|(_, l)| **l == 1)
        .map(|(i, _)| i)
        .collect();
    let l2: Vec<usize> = levels
        .iter()
        .enumerate()
        .filter(|(_, l)| **l >= 2)
        .map(|(i, _)| i)
        .collect();
    if !l0.is_empty() {
        if l0.len() < 2 {
            return (Vec::new(), 0); // 单个 L0 段：暂不压实（无收益重写）
        }
        if l1.len() < level_limit.max(1) {
            (l0, 1)
        } else {
            let mut sel = l0.clone();
            sel.extend(l1);
            (sel, 1)
        }
    } else if l1.len() > 1 {
        (l1, 2)
    } else if l2.len() > 1 {
        (l2, 2)
    } else {
        (Vec::new(), 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let name = format!("cf-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    fn small_cfg(max_mb: usize) -> Config {
        let mut cfg = Config::default();
        cfg.memtable.max_size_mb = max_mb;
        cfg.blockcache.block_size_kb = 1;
        cfg.sstable.compression = "none".into();
        cfg
    }

    // ---------- 流式 scan（M8-P10） ----------

    #[test]
    fn scan_stream_matches_scan_raw_range() {
        let dir = tmp();
        let cfg = small_cfg(16); // 小阈值 → 多次 flush → 多 SST 源
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 批量写入并周期性 flush（产生多个 SST：k-way merge 多源）
        for i in 0..2_000u64 {
            cf.put_bytes_nosync(
                format!("k{i:08}").into_bytes(),
                format!("v{i}").into_bytes(),
            )
            .unwrap();
            if i % 500 == 499 {
                cf.switch_and_flush().unwrap();
            }
        }
        // memtable 新版本：覆盖 + 删除
        cf.put_bytes_nosync(b"k00000042".to_vec(), b"updated".to_vec())
            .unwrap();
        cf.delete_bytes(b"k00000100".to_vec()).unwrap();
        cf.sync_wal().unwrap();

        // 全量（旧路径）vs 流式（新路径）：结果完全一致
        let all = cf.scan_raw_range(None, None).unwrap();
        let mut streamed: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        cf.scan_stream(None, None, |k, v| {
            streamed.push((k.to_vec(), v.to_vec()));
            Ok(())
        })
        .unwrap();
        assert_eq!(streamed.len(), all.len(), "流式行数应与全量一致");
        for (a, b) in streamed.iter().zip(all.iter()) {
            assert_eq!(a.0, b.0, "key 顺序一致");
            assert_eq!(a.1, b.1, "value 一致（含覆盖后的新值）");
        }
        // 语义校验：覆盖生效、删除隐藏
        let hit = all.iter().find(|(k, _)| k == b"k00000042").unwrap();
        assert_eq!(hit.1, b"updated");
        assert!(
            !all.iter().any(|(k, _)| k == b"k00000100"),
            "被删除的 key 不应出现"
        );
        // 范围过滤：流式与全量一致
        let ra = cf
            .scan_raw_range(Some(&b"k00001000"[..]), Some(&b"k00001010"[..]))
            .unwrap();
        let mut rs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        cf.scan_stream(
            Some(&b"k00001000"[..]),
            Some(&b"k00001010"[..]),
            |k, v| {
                rs.push((k.to_vec(), v.to_vec()));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(rs, ra, "范围过滤流式应与全量一致");
    }

    // ---------- WAL 截断（M8-P5） ----------

    #[test]
    fn wal_truncated_after_flush_keeps_data() {
        let dir = tmp();
        let cfg = small_cfg(64);
        let wal_path = dir.join(WAL_FILE);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..5_000u64 {
                cf.put_bytes_nosync(i.to_be_bytes().to_vec(), format!("doc-{i}").into_bytes())
                    .unwrap();
            }
            cf.sync_wal().unwrap();
            let before = std::fs::metadata(&wal_path).unwrap().len();
            cf.switch_and_flush().unwrap(); // flush → WAL 截断
            let after = std::fs::metadata(&wal_path).unwrap().len();
            assert!(
                after < before && after < 64,
                "flush 后 WAL 应截断为小文件（before={before} after={after}）"
            );
        }
        // 重开：数据完整（从 SST + WAL 恢复），seq 接续
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.get(0).unwrap().unwrap().0, b"doc-0");
        assert_eq!(cf2.get(4_999).unwrap().unwrap().0, b"doc-4999");
        // 新写入 seq 接续（不回到 1）：头持久化 next_seq
        let next = cf2.wal_next_seq();
        assert!(
            next >= 5_001,
            "重开后 next_seq 应接续（>=5001），实际 {next}"
        );
    }

    #[test]
    fn wal_header_persists_next_seq_across_restart() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let wal_path = dir.join(WAL_FILE);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in 0..300u64 {
            cf.put_bytes_nosync(i.to_be_bytes().to_vec(), format!("v{i}").into_bytes())
                .unwrap();
        }
        cf.sync_wal().unwrap();
        cf.switch_and_flush().unwrap(); // 截断，头写入 next_seq=301
        let first_after_flush = cf.wal_next_seq();
        drop(cf);
        // 重开：next_seq 从头恢复（>300），而非 1
        let cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(
            cf2.wal_next_seq(),
            first_after_flush,
            "重开 next_seq 应接续（头持久化）"
        );
        assert!(
            std::fs::metadata(&wal_path).unwrap().len() < 64,
            "WAL 保持小文件"
        );
    }

    #[test]
    fn old_wal_without_header_still_recovers() {
        // 兼容：旧格式 WAL（无头，M8-P5 之前）照常回放恢复
        let dir = tmp();
        let cfg = small_cfg(64);
        let wal_path = dir.join(WAL_FILE);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..100u64 {
                cf.put_bytes_nosync(i.to_be_bytes().to_vec(), format!("v{i}").into_bytes())
                    .unwrap();
            }
            cf.sync_wal().unwrap();
            // 不 flush：WAL 保留记录（旧格式无头）
        }
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(
            cf2.get(99).unwrap().unwrap().0,
            b"v99",
            "旧格式 WAL 应回放恢复"
        );
        assert!(cf2.wal_next_seq() >= 101, "旧 WAL 回放后 seq 接续");
    }

    #[test]
    fn put_get_roundtrip() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(1, b"doc-1".to_vec()).unwrap();
        cf.put(2, b"doc-2".to_vec()).unwrap();
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"doc-1");
        assert_eq!(cf.get(2).unwrap().unwrap().0, b"doc-2");
        assert!(cf.get(99).unwrap().is_none());
    }

    #[test]
    fn overwrite_returns_latest() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(7, b"v1".to_vec()).unwrap();
        cf.put(7, b"v2".to_vec()).unwrap();
        assert_eq!(cf.get(7).unwrap().unwrap().0, b"v2");
    }

    #[test]
    fn delete_hides_key() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(3, b"x".to_vec()).unwrap();
        cf.delete(3).unwrap();
        assert!(cf.get(3).unwrap().is_none());
    }

    #[test]
    fn delete_survives_flush_and_restart() {
        // 步骤 9：Tombstone 必须落盘——删除后刷盘 + 重启，key 依然不存在
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            cf.put(1, b"v1".to_vec()).unwrap();
            cf.put(2, b"v2".to_vec()).unwrap();
            cf.put(3, b"v3".to_vec()).unwrap();
            cf.switch_and_flush().unwrap(); // 全部落盘
            cf.delete(2).unwrap();
            cf.delete(3).unwrap();
            cf.switch_and_flush().unwrap(); // Tombstone 落盘
        }
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.get(1).unwrap().unwrap().0, b"v1");
        assert!(cf2.get(2).unwrap().is_none(), "删除后重启应不存在");
        assert!(cf2.get(3).unwrap().is_none(), "删除后重启应不存在");
        // 范围扫描同样过滤
        let rows = cf2.scan_range(None, None).unwrap();
        assert_eq!(rows, vec![(1, b"v1".to_vec())]);
    }

    #[test]
    fn delete_overrides_older_sst_value() {
        // 旧 SST 有值、新 SST 有 Tombstone：读路径按新→旧应命中 Tombstone → 不存在
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(9, b"old-value".to_vec()).unwrap();
        cf.switch_and_flush().unwrap();
        // 模拟"删除发生在更晚的时刻"：直接写 tombstone 到新 memtable 并刷盘
        cf.delete(9).unwrap();
        cf.switch_and_flush().unwrap();
        assert!(cf.get(9).unwrap().is_none());
        assert!(cf.scan_range(None, None).unwrap().is_empty());
    }

    #[test]
    fn compact_merges_ssts_preserving_overwrite_and_delete() {
        // design 4.5 阶段 3：多次刷盘 → 全量合并，覆盖/删除语义保留
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 3 个 SST：含跨段覆盖 + 删除
        cf.put(1, b"v1".to_vec()).unwrap();
        cf.put(2, b"v2".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST1
        cf.put(2, b"v2b".to_vec()).unwrap(); // 跨段覆盖
        cf.put(3, b"v3".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST2
        cf.delete(3).unwrap(); // 删除
        cf.put(4, b"v4".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST3

        let before = cf.sst_count();
        assert!(before >= 3, "应产生多个 SST: {before}");
        let rep = cf.compact().unwrap();
        assert_eq!(rep.merged_ssts, before);
        assert!(rep.freed_bytes > 0, "合并应释放空间");
        assert_eq!(cf.sst_count(), 1, "合并后只剩 1 个 SST");

        // 语义保持
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"v1");
        assert_eq!(cf.get(2).unwrap().unwrap().0, b"v2b", "后写覆盖先写");
        assert!(cf.get(3).unwrap().is_none(), "删除后不可见");
        assert_eq!(cf.get(4).unwrap().unwrap().0, b"v4");
        let rows = cf.scan_range(None, None).unwrap();
        assert_eq!(
            rows,
            vec![
                (1, b"v1".to_vec()),
                (2, b"v2b".to_vec()),
                (4, b"v4".to_vec())
            ]
        );

        // 重启后 Manifest 只含新段，数据完整
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.sst_count(), 1);
        assert_eq!(cf2.get(2).unwrap().unwrap().0, b"v2b");
        assert!(cf2.get(3).unwrap().is_none());
    }

    #[test]
    fn compact_noop_when_single_sst() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(1, b"x".to_vec()).unwrap();
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.sst_count(), 1);
        let rep = cf.compact().unwrap();
        assert_eq!(rep.merged_ssts, 0, "单段不需要合并");
        assert_eq!(cf.sst_count(), 1);
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"x");
    }

    // ---------- Leveled-Compaction（design 4.5 二期，M6-2） ----------

    #[test]
    fn leveled_compact_promotes_l0_to_l1_and_persists() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 两批写入并强制刷盘 → 2 个 L0 段
        for i in 0..100u64 {
            cf.put(i, format!("a{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        for i in 100..200u64 {
            cf.put(i, format!("a{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.sst_levels, vec![0, 0], "刷盘产物均为 L0");
        assert!(!cf.needs_compact(), "2 个 L0 未超阈值");
        // 手动压实 → L1
        let rep = cf.compact().unwrap();
        assert_eq!(rep.out_level, 1);
        assert_eq!(cf.sst_count(), 1);
        assert_eq!(cf.sst_levels, vec![1]);
        // 数据完整
        for i in (0..200u64).step_by(7) {
            assert!(cf.get(i).unwrap().is_some());
        }
        // 重启：Manifest 持久化层号
        drop(cf);
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.sst_levels, vec![1], "Manifest 应持久化层号");
        assert!(cf2.get(150).unwrap().is_some());
    }

    #[test]
    fn leveled_compact_sinks_l1_to_l2() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 每轮刷 2 个 L0 段并压实 → L0 合并下沉为 1 个 L1 段；4 轮后 L1 累计 4 个文件
        for round in 0..4u64 {
            for _ in 0..2u64 {
                for i in 0..50u64 {
                    cf.put(round * 100 + i, format!("v{round}-{i}").into_bytes())
                        .unwrap();
                }
                cf.switch_and_flush().unwrap();
            }
            let r = cf.compact().unwrap();
            assert_eq!(r.out_level, 1, "第 {round} 轮 L0→L1");
        }
        assert_eq!(cf.sst_levels.iter().filter(|l| **l == 1).count(), 4);
        // L0 空、L1 > 1 → L1 → L2（压实下沉）
        assert!(cf.needs_compact(), "L1 多段应触发 L1→L2");
        let rep2 = cf.compact().unwrap();
        assert_eq!(rep2.out_level, 2);
        assert_eq!(cf.sst_count(), 1);
        assert_eq!(cf.sst_levels, vec![2]);
        // 全量数据仍完整
        for round in 0..4u64 {
            for i in (0..50u64).step_by(9) {
                assert!(
                    cf.get(round * 100 + i).unwrap().is_some(),
                    "round {round} key {i} 丢失"
                );
            }
        }
        // 重启：L2 持久化 + 数据完整
        drop(cf);
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.sst_levels, vec![2]);
        assert!(cf2.get(300 + 10).unwrap().is_some());
    }

    #[test]
    fn select_compaction_inputs_picks_levels() {
        // 2 个 L0 + L1 未满 → 仅 L0
        assert_eq!(select_compaction_inputs(&[0, 0, 1], 8), (vec![0, 1], 1));
        // 单个 L0 → 暂不压实
        assert_eq!(select_compaction_inputs(&[0, 1], 8), (Vec::new(), 0));
        // L0 ≥ 2 且 L1 已满 → L0 + 全部 L1 收敛
        assert_eq!(
            select_compaction_inputs(&[0, 0, 1, 1, 1, 1], 2),
            (vec![0, 1, 2, 3, 4, 5], 1)
        );
        // L0 空、L1 > 1 → L1 → L2
        assert_eq!(select_compaction_inputs(&[1, 1], 8), (vec![0, 1], 2));
        // L0 空、L1 单段 → 无压实
        assert_eq!(select_compaction_inputs(&[1], 8), (Vec::new(), 0));
        // L0/L1 空、L2 > 1 → 收敛 L2
        assert_eq!(select_compaction_inputs(&[2, 2], 8), (vec![0, 1], 2));
    }

    #[test]
    fn flush_then_read_back() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 写入一批触发显式刷盘，验证落盘后可读
        for i in 0..100u64 {
            cf.put(i, format!("value-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert!(cf.sst_count() >= 1);
        // 落盘后仍可读（读路径先内存后磁盘）
        assert_eq!(cf.get(0).unwrap().unwrap().0, b"value-0");
        assert_eq!(cf.get(99).unwrap().unwrap().0, b"value-99");
    }

    #[test]
    fn restart_recovers_data_from_wal_and_sst() {
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..150u64 {
                cf.put(i, format!("v-{i}").into_bytes()).unwrap();
            }
            // 前 100 条刷盘，后 50 条留在 WAL/MemTable
            for _ in 0..2 {
                cf.switch_and_flush().unwrap();
            }
        } // 模拟进程退出（drop 不执行额外清理）

        // 重启：manifest 加载 SST + WAL 回放
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in 0..150u64 {
            assert_eq!(
                cf2.get(i).unwrap().unwrap().0,
                format!("v-{i}").into_bytes(),
                "key {i} 丢失"
            );
        }
    }

    #[test]
    fn reopen_preserves_wal_only_data() {
        // 步骤 15 暴露的预存 bug：reopen 不得截断 WAL，未刷盘数据必须经回放恢复
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 1..=5u64 {
                cf.put(i, format!("v-{i}").into_bytes()).unwrap();
            }
            // 不刷盘：数据仅存在于 WAL + MemTable
        }
        {
            let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 1..=5u64 {
                assert_eq!(
                    cf2.get(i).unwrap().unwrap().0,
                    format!("v-{i}").into_bytes(),
                    "key {i} 丢失（WAL 被截断？）"
                );
            }
            // 新写入 seq 接续，同 key 覆盖正确
            cf2.put(3, b"v-3-updated".to_vec()).unwrap();
            assert_eq!(cf2.get(3).unwrap().unwrap().0, b"v-3-updated");
        }
        // 再次重开：追加写入与覆盖均正确
        {
            let mut cf3 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            assert_eq!(cf3.get(3).unwrap().unwrap().0, b"v-3-updated");
            assert_eq!(cf3.get(5).unwrap().unwrap().0, b"v-5");
            assert_eq!(cf3.get(1).unwrap().unwrap().0, b"v-1");
        }
    }

    // ---------- 环形 WAL 集成（design 4.3，M6-1） ----------

    fn ring_cfg(mem_mb: usize) -> Config {
        let mut cfg = small_cfg(mem_mb);
        cfg.storage.wal_mode = "ring".into();
        cfg.storage.wal_ring_size_mb = 1;
        cfg
    }

    #[test]
    fn ring_wal_mode_persists_and_recovers() {
        let dir = tmp();
        let cfg = ring_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            cf.put(1, b"v1".to_vec()).unwrap();
            cf.put(2, b"v2".to_vec()).unwrap();
            assert_eq!(cf.get(1).unwrap().unwrap().0, b"v1");
        }
        // 重启：环形 WAL 回放恢复未刷盘数据
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"v1");
        assert_eq!(cf.get(2).unwrap().unwrap().0, b"v2");
        // 覆盖 + 追加后再次重启
        cf.put(2, b"v2-updated".to_vec()).unwrap();
        drop(cf);
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.get(2).unwrap().unwrap().0, b"v2-updated");
    }

    #[test]
    fn ring_wal_full_forces_flush_keeps_data() {
        // 写入量超过 1MB 环形容量 → 强制 Flush 腾空，数据不丢
        let dir = tmp();
        let cfg = ring_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        const N: u64 = 60_000;
        for chunk in 0..6u64 {
            for i in chunk * 10_000..(chunk + 1) * 10_000 {
                cf.put_bytes_nosync(i.to_le_bytes().to_vec(), format!("value-{i}").into_bytes())
                    .unwrap();
            }
            cf.sync_wal().unwrap();
        }
        // 抽查全量数据（跨 MemTable / SST / 环形覆盖后均完整）
        for i in (0..N).step_by(997) {
            assert_eq!(
                cf.get_bytes(&i.to_le_bytes()).unwrap().unwrap().0,
                format!("value-{i}").into_bytes(),
                "key {i} 丢失（环形覆盖未刷盘记录？）"
            );
        }
        // 重启后仍完整（环形恢复 + SST 合并读）
        drop(cf);
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in (0..N).step_by(5003) {
            assert_eq!(
                cf2.get_bytes(&i.to_le_bytes()).unwrap().unwrap().0,
                format!("value-{i}").into_bytes(),
                "重启后 key {i} 丢失"
            );
        }
    }

    #[test]
    fn ttl_helpers() {
        assert_eq!(epoch_days(0), 0);
        assert_eq!(epoch_days(86_400), 1);
        assert_eq!(epoch_days(-1), -1); // div_euclid 向负无穷取整
        assert_eq!(parse_sst_date("sst-00020697-00000001.sst"), Some(20_697));
        assert_eq!(parse_sst_date("sst-00000001.sst"), None);
        assert_eq!(parse_sst_date("manifest.json"), None);
    }

    #[test]
    fn ttl_buckets_and_expiry() {
        // 阶段 1.5 TTL：按天分桶写 SST；重启时过期桶整文件删除，默认桶永不过期
        let dir = tmp();
        let mut cfg = small_cfg(256);
        cfg.storage.ttl_days = Some(2);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            let today = format!(r#"{{"v":"t","timestamp":{now}}}"#).into_bytes();
            let yesterday = format!(r#"{{"v":"y","timestamp":{}}}"#, now - 86_400).into_bytes();
            let old = format!(r#"{{"v":"o","timestamp":{}}}"#, now - 86_400 * 10).into_bytes();
            cf.put(1, today).unwrap();
            cf.put(2, yesterday).unwrap();
            cf.put(3, old).unwrap();
            cf.put(4, b"no-timestamp-raw".to_vec()).unwrap();
            cf.switch_and_flush().unwrap();
            assert!(cf.sst_count() >= 4, "应分 4 个桶，实际 {}", cf.sst_count());
            // 未过期前全部可读
            assert!(cf.get(1).unwrap().is_some());
            assert!(cf.get(3).unwrap().is_some());
        }
        // 重启：10 天前的桶过期删除
        {
            let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            assert!(cf2.get(1).unwrap().is_some(), "今天桶应保留");
            assert!(cf2.get(2).unwrap().is_some(), "昨天桶应保留");
            assert!(cf2.get(3).unwrap().is_none(), "10 天前的桶应过期删除");
            assert!(
                cf2.get(4).unwrap().is_some(),
                "默认桶（无时间字段）永不过期"
            );
            // 过期文件已物理删除
            let names: Vec<String> = std::fs::read_dir(&dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().to_string())
                .filter(|f| f.starts_with(SST_PREFIX))
                .collect();
            assert!(
                names
                    .iter()
                    .any(|f| parse_sst_date(f).is_none_or(|d| d >= today_epoch_days() - 2)),
                "过期 SST 应已删除，剩余: {names:?}"
            );
        }
    }

    #[test]
    fn pax_cf_flush_read_roundtrip() {
        // 阶段 1.5 PAX：配置 hot_fields 后 flush 落盘列式块，读回语义等值；非 JSON 值回退行式
        let dir = tmp();
        let mut cfg = small_cfg(256);
        cfg.storage.hot_fields = vec!["status".to_string(), "city".to_string()];
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(
            1,
            br#"{"status":"active","city":"beijing","amount":10}"#.to_vec(),
        )
        .unwrap();
        cf.put(2, br#"{"status":"inactive","city":"shanghai"}"#.to_vec())
            .unwrap();
        cf.switch_and_flush().unwrap();
        assert!(cf.sst_count() >= 1);
        assert_eq!(
            String::from_utf8_lossy(&cf.get(1).unwrap().unwrap().0),
            r#"{"status":"active","city":"beijing","amount":10}"#
        );
        assert_eq!(
            String::from_utf8_lossy(&cf.get(2).unwrap().unwrap().0),
            r#"{"status":"inactive","city":"shanghai"}"#
        );
        // 非 JSON 值 → 行式块（同一 v4 文件体系，Reader 兼容）
        cf.put(3, b"raw-bytes".to_vec()).unwrap();
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.get(3).unwrap().unwrap().0, b"raw-bytes");
    }

    #[test]
    fn manifest_persists_sst_list() {
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..50u64 {
                cf.put(i, b"v".to_vec()).unwrap();
            }
            cf.switch_and_flush().unwrap();
        }
        let text = std::fs::read_to_string(dir.join(MANIFEST_FILE)).unwrap();
        assert!(text.contains(SST_PREFIX));
        let m: Manifest = serde_json::from_str(&text).unwrap();
        assert!(!m.sst_files.is_empty());
    }

    #[test]
    fn corrupted_sst_rejected_on_open() {
        // 损坏注入：SST 头部魔数被破坏 → 启动必须报 Corrupted 而非 panic（development 9.3）
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..100u64 {
                cf.put(i, b"v".to_vec()).unwrap();
            }
            cf.switch_and_flush().unwrap();
        }
        let sst_path = dir.join(format!("{SST_PREFIX}00000001.sst"));
        assert!(sst_path.exists(), "SST 文件应已生成");
        let mut data = std::fs::read(&sst_path).unwrap();
        data[0..8].copy_from_slice(&[0xFF; 8]); // 破坏魔数
        std::fs::write(&sst_path, &data).unwrap();

        let err = match ColumnFamily::open("primary", &dir, &cfg) {
            Ok(_) => panic!("损坏 SST 应打开失败"),
            Err(e) => e,
        };
        assert!(
            matches!(err, Error::Corrupted(_)),
            "损坏 SST 应报 Corrupted，实际 {err:?}"
        );
    }

    #[test]
    fn wal_partial_tail_recovers_cleanly() {
        // 崩溃恢复：WAL 尾部半条记录（模拟断电时只写入一半）→ 重启只恢复完整记录、不 panic
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 1..=10u64 {
                cf.put(i, format!("v-{i}").into_bytes()).unwrap();
            }
        }
        // 截断 WAL 至一半字节（人为制造半条尾部记录）
        let wal_path = dir.join(WAL_FILE);
        let mut data = std::fs::read(&wal_path).unwrap();
        assert!(data.len() > 40);
        data.truncate(data.len() / 2);
        std::fs::write(&wal_path, &data).unwrap();

        // 回放：完整记录应恢复，半条记录被丢弃，不 panic
        let recs = WalReader::recover(&wal_path).unwrap();
        assert!(!recs.is_empty(), "至少应恢复一条完整记录");
        assert!(recs.len() <= 10);

        // reopen 全链路可用
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for r in &recs {
            assert!(cf2.get_bytes(&r.key).unwrap().is_some(), "恢复的记录应可读");
        }
    }
    #[test]
    fn scan_range_covers_memtable_and_sst() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 部分写入并刷盘，部分留在 MemTable
        for i in 0..30u64 {
            cf.put(i, format!("v-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        for i in 30..40u64 {
            cf.put(i, format!("v-{i}").into_bytes()).unwrap();
        }
        // 更新一个已落盘 key，验证去重取最新
        cf.put(5, b"v-updated".to_vec()).unwrap();

        let rows = cf.scan_range(Some(0), Some(39)).unwrap();
        assert_eq!(rows.len(), 40);
        assert_eq!(rows[0], (0, b"v-0".to_vec()));
        assert_eq!(rows[39], (39, b"v-39".to_vec()));
        assert_eq!(rows[5], (5, b"v-updated".to_vec()));

        // 无边界扫描
        let all = cf.scan_range(None, None).unwrap();
        assert_eq!(all.len(), 40);
    }

    #[test]
    fn scan_range_empty_window() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in 0..10u64 {
            cf.put(i, b"v".to_vec()).unwrap();
        }
        // 无交集区间 → 空
        assert!(cf.scan_range(Some(100), Some(200)).unwrap().is_empty());
    }

    #[test]
    fn composite_index_prefix_query() {
        // 步骤 10：组合索引 = ColumnFamily + encode_composite_key(fields, docid)
        use crate::keys::{decode_composite_key, encode_composite_key};
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("cidx", &dir, &cfg).unwrap();
        // 写入 (status=active, type=click, docid)
        for docid in [1u64, 5, 9, 20] {
            let key = encode_composite_key(&[b"active", b"click"], docid);
            cf.put_bytes(key, docid.to_le_bytes().to_vec()).unwrap();
        }
        // 干扰项：status=active 但 type=view
        let key = encode_composite_key(&[b"active", b"view"], 100);
        cf.put_bytes(key, 100u64.to_le_bytes().to_vec()).unwrap();

        // 前缀查询 active/click：组合键有序，范围 [active/click/0, active/click/FFFF]
        let start = encode_composite_key(&[b"active", b"click"], 0);
        let end = encode_composite_key(&[b"active", b"click"], u64::MAX);
        let mut hits = Vec::new();
        let rows = cf.scan_raw_range(Some(&start), Some(&end)).unwrap();
        for (k, _v) in rows {
            let (fields, docid) = decode_composite_key(&k).unwrap();
            assert_eq!(fields, vec![b"active".to_vec(), b"click".to_vec()]);
            hits.push(docid);
        }
        hits.sort();
        assert_eq!(hits, vec![1, 5, 9, 20]);
    }
}
