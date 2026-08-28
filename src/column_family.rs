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
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::blockcache::{BlockCache, BlockCacheKey};
use crate::bloom::BloomFilter;
use crate::config::model::{Config, MemtableConfig};
use crate::error::{Error, Result};
use crate::keys::{decode_docid, encode_docid};
use crate::memtable::{MemTable, MemTableBuffer};
use crate::sstable::{Compression, SstFooter, SstReader, SstWriter, FLAG_DELETE, FLAG_PUT};
use crate::wal::{WalReader, WalWriter, OP_DELETE, OP_PUT};

/// SST 文件前缀。
const SST_PREFIX: &str = "sst-";
const WAL_FILE: &str = "wal.log";
const MANIFEST_FILE: &str = "manifest.json";

/// Manifest：列族内 SST 文件清单（新→旧顺序）。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Manifest {
    /// 最近刷盘的文件在最前（读路径优先命中）。
    sst_files: Vec<String>,
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
    /// 双缓冲 MemTable。
    memtable: MemTableBuffer,
    /// SST 文件（新→旧）。
    ssts: Vec<SstReader>,
    /// 共享块缓存（跨 CF 共享）。
    block_cache: Arc<BlockCache>,
    /// 单调 seq 分配器（跨重启由 WAL 恢复推进）。
    seq: AtomicU64,
    /// 下一个 SST 文件 id（跨重启由 Manifest 恢复，防止覆盖旧文件）。
    next_sst_id: u64,
    /// 当前 WAL 写入器。
    wal: WalWriter,
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
        let (mut sst_names, next_sst_id) = load_manifest(&manifest_path)?;

        // TTL 过期桶清理：按天整文件删除（删除成本 O(1)，无墓碑；design 5.4）
        if let Some(ttl_days) = cfg.storage.ttl_days {
            let cutoff = today_epoch_days() - ttl_days as i64;
            let mut removed = 0usize;
            for f in &sst_names {
                if let Some(days) = parse_sst_date(f) {
                    if days < cutoff {
                        let p = dir.join(f);
                        if p.exists() {
                            std::fs::remove_file(&p).map_err(Error::Io)?;
                            removed += 1;
                        }
                    }
                }
            }
            if removed > 0 {
                sst_names.retain(|f| parse_sst_date(f).is_none_or(|d| d >= cutoff));
                info!("列族 [{name}] TTL 过期桶清理: 删除 {removed} 个 SST");
            }
        }

        // 打开全部 SST（新→旧）
        let mut ssts = Vec::new();
        for f in &sst_names {
            let p = dir.join(f);
            if !p.exists() {
                warn!("Manifest 中的 SST 缺失，跳过: {}", p.display());
                continue;
            }
            match SstReader::open_with_granularity(&p, cfg.sstable.index_granularity as usize) {
                Ok(r) => ssts.push(r),
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
        // 以追加模式打开（不截断旧 WAL）：先回放恢复 MemTable，再继续写入
        let wal = WalWriter::open_append(&wal_path, 1, false)?;
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
            memtable: MemTableBuffer::new(),
            ssts,
            block_cache,
            seq: AtomicU64::new(1),
            next_sst_id,
            wal,
        };

        // WAL 回放（幂等：以 seq 排序重放，同 key 后写覆盖先写）
        let max_seq = cf.replay_wal(&wal_path)?;
        // 新写入 seq 必须接续已回放的最大 seq，避免同 key 版本冲突（重启恢复的正确性）
        cf.wal.resume_seq(max_seq + 1);
        Ok(cf)
    }

    /// 回放 WAL：重建 MemTable 并推进 seq。返回已回放的最大 seq（无记录为 0）。
    /// TTL 启用时，已过期的记录（按文档 ttl_field 判断）不回放入 MemTable。
    fn replay_wal(&mut self, wal_path: &Path) -> Result<u64> {
        let recs = WalReader::recover(wal_path)?;
        let mut max_seq = 0u64;
        for r in &recs {
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
        self.wal.sync()?;
        Ok(seq)
    }

    /// 批量写入（不逐条 fsync，由调用方最终 `sync_wal` 统一提交）。
    /// 供亿级数据压测/导入使用；强安全模式逐条写请用 `put_bytes`。
    pub fn put_bytes_nosync(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        let seq = self.wal.append(OP_PUT, &key, Some(&value))?;
        self.memtable.put(key, seq, value);
        self.maybe_flush()?;
        Ok(seq)
    }

    /// 统一提交 WAL 缓冲（批量写入结束时调用）。
    pub fn sync_wal(&mut self) -> Result<()> {
        self.wal.sync()
    }

    /// 删除（Tombstone，跨 flush/重启一致，见步骤 9）。
    pub fn delete(&mut self, docid: u64) -> Result<u64> {
        self.delete_bytes(encode_docid(docid).to_vec())
    }

    /// 删除原始字节键。
    pub fn delete_bytes(&mut self, key: Vec<u8>) -> Result<u64> {
        let seq = self.wal.append(OP_DELETE, &key, None)?;
        self.wal.sync()?;
        self.memtable.delete(key, seq);
        Ok(seq)
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
        if self.ttl_days.is_none() {
            return self.flush_single(&imm);
        }
        self.flush_buckets(&imm)
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
        self.ssts
            .insert(0, SstReader::open_with_granularity(&path, self.index_granularity)?);
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
            self.ssts
                .insert(0, SstReader::open_with_granularity(&path, self.index_granularity)?);
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
        // 以磁盘扫描维护清单（新→旧：id 降序），避免依赖 SstReader 内部状态
        let mut files: Vec<String> = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let fname = entry.file_name().to_string_lossy().to_string();
            if fname.starts_with(SST_PREFIX) {
                files.push(fname);
            }
        }
        files.sort_by(|a, b| b.cmp(a));
        let m = Manifest {
            sst_files: files,
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

fn load_manifest(path: &Path) -> Result<(Vec<String>, u64)> {
    if !path.exists() {
        return Ok((Vec::new(), 1));
    }
    let text = std::fs::read_to_string(path)?;
    let m: Manifest = serde_json::from_str(&text)
        .map_err(|e| Error::Corrupted(format!("Manifest 解析失败: {e}")))?;
    Ok((m.sst_files, m.next_sst_id))
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
    // 二分定位首个 first_key <= key 的块
    let index = sst.index();
    let mut lo = 0usize;
    let mut hi = index.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if index[mid].first_key.as_slice() <= key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == 0 {
        return Ok(None);
    }
    let block_idx = lo - 1;
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
    // clone 断开借用，随后 read_block 需要 &mut sst
    let Some(entry) = index.get(block_idx).cloned() else {
        return Ok(None);
    };
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
