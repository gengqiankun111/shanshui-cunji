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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::blockcache::{BlockCache, BlockCacheKey};
use crate::config::model::{Config, MemtableConfig};
use crate::error::{Error, Result};
use crate::keys::{decode_docid, encode_docid};
use crate::memtable::{MemTable, MemTableBuffer};
use crate::sstable::{Compression, SstFooter, SstReader, SstWriter};
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
        let manifest_path = dir.join(MANIFEST_FILE);
        let (sst_names, next_sst_id) = load_manifest(&manifest_path)?;

        // 打开全部 SST（新→旧）
        let mut ssts = Vec::new();
        for f in &sst_names {
            let p = dir.join(f);
            if !p.exists() {
                warn!("Manifest 中的 SST 缺失，跳过: {}", p.display());
                continue;
            }
            match SstReader::open(&p) {
                Ok(r) => ssts.push(r),
                Err(e) => return Err(Error::Corrupted(format!("SST 加载失败 {}: {e}", p.display()))),
            }
        }
        info!("列族 [{name}] 加载 {} 个 SST，下一个 id={next_sst_id}", ssts.len());

        let block_cache = Arc::new(BlockCache::new(
            cfg.blockcache.max_memory_mb * 1024 * 1024,
            cfg.blockcache.block_size_kb * 1024,
        ));
        let compression = Compression::from_str(&cfg.sstable.compression)?;
        let compression_level = cfg.sstable.compression_level as i32;

        let wal_path = dir.join(WAL_FILE);
        let wal = WalWriter::create(&wal_path, false)?;
        let mut cf = Self {
            name: name.to_string(),
            dir: dir.to_path_buf(),
            cfg: cfg.memtable.clone(),
            compression,
            compression_level,
            block_size: cfg.blockcache.block_size_kb * 1024,
            memtable: MemTableBuffer::new(),
            ssts,
            block_cache,
            seq: AtomicU64::new(1),
            next_sst_id,
            wal,
        };

        // WAL 回放（幂等：以 seq 排序重放，同 key 后写覆盖先写）
        cf.replay_wal(&wal_path)?;
        Ok(cf)
    }

    /// 回放 WAL：重建 MemTable 并推进 seq。
    fn replay_wal(&mut self, wal_path: &Path) -> Result<()> {
        let recs = WalReader::recover(wal_path)?;
        for r in &recs {
            match r.op {
                OP_PUT => {
                    if let Some(v) = &r.value {
                        self.memtable.put(r.key.clone(), r.seq, v.clone());
                    }
                }
                OP_DELETE => self.memtable.delete(r.key.clone(), r.seq),
                other => return Err(Error::Corrupted(format!("WAL 未知 op {other}"))),
            }
        }
        if !recs.is_empty() {
            let max_seq = recs.iter().map(|r| r.seq).max().unwrap();
            self.seq.store(max_seq + 1, Ordering::Relaxed);
            info!("列族 [{}] WAL 回放 {} 条，seq 推进至 {}", self.name, recs.len(), max_seq + 1);
        }
        Ok(())
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
    pub fn scan_range(&mut self, start: Option<u64>, end: Option<u64>) -> Result<Vec<(u64, Vec<u8>)>> {
        let start_key = start.map(|s| encode_docid(s).to_vec());
        let end_key = end.map(|e| encode_docid(e).to_vec());
        let rows = self.scan_raw_range(start_key.as_deref(), end_key.as_deref())?;
        let mut out = Vec::with_capacity(rows.len());
        for (k, v) in rows {
            out.push((decode_docid(&k).map_err(|_| Error::Corrupted("docid 解码失败".into()))?, v));
        }
        Ok(out)
    }

    /// 原始字节键范围扫描（组合索引前缀查询使用）。返回升序 (key, value) 列表，Tombstone 已过滤。
    pub fn scan_raw_range(&mut self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // 候选收集：key → (seq, value)，value=None 表示 Tombstone
        let mut merged: std::collections::HashMap<Vec<u8>, (u64, Option<Vec<u8>>)> = std::collections::HashMap::new();

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
        let sst_id = self.next_sst_id;
        self.next_sst_id += 1;
        let path = self.dir.join(format!("{SST_PREFIX}{sst_id:08}.sst"));
        self.write_sst(&path, &imm)?;

        // 新文件插到最前（读路径优先命中）
        let fname = path.file_name().unwrap().to_string_lossy().to_string();
        self.ssts.insert(0, SstReader::open(&path)?);
        self.persist_manifest()?;
        info!("列族 [{}] 刷盘完成: {} ({} 条)", self.name, fname, imm.len());
        Ok(())
    }

    /// 将 Immutable 落盘为 SST（Put 与 Tombstone 均落盘，保证跨 flush 删除一致）。
    fn write_sst(&self, path: &Path, imm: &MemTable) -> Result<SstFooter> {
        let mut w = SstWriter::new(path, self.compression, self.compression_level, self.block_size, imm.len())?;
        imm.scan(|k, e| {
            match &e.value {
                Some(v) => w.add(k, v, e.seq).expect("SST 写入失败"),
                None => w.add_tombstone(k, e.seq).expect("SST Tombstone 写入失败"),
            }
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
        let m = Manifest { sst_files: files, next_sst_id: self.next_sst_id };
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

fn load_manifest(path: &Path) -> Result<(Vec<String>, u64)> {
    if !path.exists() {
        return Ok((Vec::new(), 1));
    }
    let text = std::fs::read_to_string(path)?;
    let m: Manifest = serde_json::from_str(&text).map_err(|e| Error::Corrupted(format!("Manifest 解析失败: {e}")))?;
    Ok((m.sst_files, m.next_sst_id))
}

/// 同 key 候选合并：仅保留 seq 更大（更新）的版本；value=None 的 Tombstone 可覆盖旧值。
fn merge_candidate_bytes(merged: &mut std::collections::HashMap<Vec<u8>, (u64, Option<Vec<u8>>)>, key: Vec<u8>, seq: u64, value: Option<Vec<u8>>) {
    match merged.get(&key) {
        Some((old_seq, _)) if *old_seq >= seq => {}
        _ => {
            merged.insert(key, (seq, value));
        }
    }
}

/// 单 SST 等值查询（布隆剪枝 → 二分定位块 → 块缓存/读盘 → 块内扫描）。
/// 返回 `(value, seq)`：value=None 表示 Tombstone；整体 None 表示该 SST 无此 key。
fn get_from_sst(sst: &mut SstReader, cache: &BlockCache, key: &[u8]) -> Result<Option<(Option<Vec<u8>>, u64)>> {
    if !sst.bloom().maybe_contains(&key.to_vec()) {
        return Ok(None);
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
    // clone 断开借用，随后 read_block 需要 &mut sst
    let Some(entry) = index.get(lo.wrapping_sub(1)).cloned() else {
        return Ok(None);
    };
    let ck = BlockCacheKey { file: sst.path().to_path_buf(), offset: entry.offset };
    let block = if let Some(b) = cache.get(&ck) {
        b
    } else {
        let b = sst.read_block(&entry)?;
        cache.put(ck, b.clone());
        b
    };
    Ok(scan_block(&block, key))
}

/// 块内等值扫描（与 sstable::scan_block_for_key 相同逻辑，独立维护避免跨模块私有）。
/// 返回 `(value, seq)`：value=None 表示 Tombstone；整体 None 表示块内无此 key。
fn scan_block(block: &[u8], key: &[u8]) -> Option<(Option<Vec<u8>>, u64)> {
    let mut cur = 0usize;
    while cur < block.len() {
        let Ok(k) = crate::keys::decode_varlen(block, &mut cur) else { return None };
        let Ok(v) = crate::keys::decode_varlen(block, &mut cur) else { return None };
        if cur + 9 > block.len() {
            return None;
        }
        let flag = block[cur];
        let seq = u64::from_le_bytes(block[cur + 1..cur + 9].try_into().ok()?);
        if k == key {
            let value = (flag == crate::sstable::FLAG_PUT).then(|| v.to_vec());
            return Some((value, seq));
        }
        cur += 9;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let name = format!("cf-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap()).path().join(name)
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
            assert_eq!(cf2.get(i).unwrap().unwrap().0, format!("v-{i}").into_bytes(), "key {i} 丢失");
        }
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
