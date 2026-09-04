//! SstReader 打开与点读（原 mod.rs 拆分，按主题聚类）：Footer/Header 解析、两级索引
//! （Level 1 摘要常驻 + Level 2 精确索引懒加载）、分区布隆、点查（get / scan_block_* /
//! locate_indexed_block）、块读取（read_block / block_raw / read_block_group）与块索引
//! 字节流解码（decode_index）。位置读 read_at / 范围扫描流式迭代器 SstRangeIter /
//! Zone Map 谓词 ZonePredicate 已拆至 [`super::iter`]。

use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::bloom::BloomFilter;
use crate::error::{Error, Result};
use crate::keys::{decode_varint, decode_varlen};
use crate::per_cpu::PerCpuCounter;

use super::block::{decode_data_block, decode_pax_block_fields, extract_fields_from_json_row};
use super::iter::read_at;
use super::writer::{Compression, FieldZone, IndexEntry};
use super::{crc32, BLOCK_KIND_PAX, SST_MAGIC, SST_VERSION, SST_VERSION_ROW, TRAILER_LEN};

/// SST Footer 摘要（Reader 使用）。
#[derive(Debug, Clone)]
pub struct SstFooter {
    pub index_offset: u64,
    pub index_len: usize,
    pub bloom_offset: u64,
    pub bloom_len: usize,
    pub key_count: u64,
    pub footer_offset: u64,
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// SSTable Reader：mmap 式顺序读取（MVP 用文件 read + seek；阶段 3 可换 io_uring）。
/// 采用**两级索引**（design 4.4.2）：
/// - **Level 1（内存常驻）**：每 `index_granularity`（默认 16）个 Block 一条摘要
///   （`summary`：块首键 + 块下标），极轻量；
/// - **Level 2（按需加载）**：精确 Block 索引（全部 `IndexEntry`），首次访问从磁盘解码并缓存。
/// 对比：单层稀疏索引 ~200MB → 两级 ~20MB，内存减少 90%（更多留给 HotCache）。
pub struct SstReader {
    path: PathBuf,
    file: std::fs::File,
    footer: SstFooter,
    /// Level 1：内存常驻摘要（每 index_granularity 个块一条）。
    summary: Vec<SummaryEntry>,
    /// 精确块数（open 时解码索引获得；Level 2 懒加载前即可用于容量判断）。
    index_count: usize,
    /// 两级索引粒度（每 N 个块一条摘要）。
    index_granularity: usize,
    /// Level 2：精确块索引（懒加载缓存；首次访问触发磁盘解码）。
    /// O 项第②步：RefCell → Mutex（`SstReader: Sync`，支持 RwLock 读读并行跨线程共享）。
    full_index: Mutex<Option<Vec<IndexEntry>>>,
    /// v5 分区布隆：每块一个（原始字节，查询时按需反序列化目标块）。
    partition_blooms: Option<Vec<Vec<u8>>>,
    /// v3/v4 整文件布隆（旧格式兼容）。
    bloom: Option<BloomFilter>,
    /// R 项：段 key 范围 [min, max]（open 时从解码索引首尾取，O(1) 内存）——
    /// 点查段级 Zone Map 粗筛（key 越界段 O(1) 跳过，不做二分 + 布隆反序列化）。
    /// 空段（无块）为全空 Vec，`key_range()` 返回 None = 无约束。
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    /// P3-C：该 SST 所属表 ID（从 min_key 提取，因 M3 保证每个 SST 仅含单表）。
    /// 非 docid 编码的混合索引 SST → None，使用全局默认分区 0。
    table_id: Option<u16>,
    /// 文件字节数（open 时一次 metadata；快照 sizes 缓存用——写路径 needs_compact
    /// 不再逐次 fs::metadata，避免每 put 3 次 stat 拖垮写吞吐）。
    file_len: u64,
    compression: Compression,
    /// 文件格式版本（v3=纯行式，v4=PAX，v5=分区布隆）。
    format: u16,
    /// Ex-5.9/Ex-7.1：读热度计数（点查/范围扫描命中递增，冷热 Compaction 选段依据；
    /// PerCpuCounter 按核拆分——并发读多核 touch 无伪共享）。
    heat: PerCpuCounter,
    /// V 项：io_uring 后端池引用（Linux + `runtime.io_uring_enabled` 时 Some）——块读走
    /// SQPOLL 队列异步提交（免 syscall），WAL fsync 同池（IoClass::Sst 路由）。
    /// 非 Linux 编译为空字段（io-uring-file crate 为空）。
    #[cfg(target_os = "linux")]
    iou: Option<std::sync::Arc<crate::io_queue::backend::IoUringPool>>,
}

/// Level 1 摘要条目：每 `index_granularity` 个块一条（design 4.4.2）。
#[derive(Debug, Clone)]
pub struct SummaryEntry {
    /// 块首键（Zone Map min）。
    pub first_key: Vec<u8>,
    /// 对应精确索引中的块下标。
    pub block_index: usize,
}

impl SstReader {
    /// 打开 SST（两级索引粒度默认 16，design 4.4.2）。
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_granularity(path, 16)
    }

    /// 打开 SST 并指定两级索引粒度（`sstable.index_granularity`）。
    pub fn open_with_granularity(path: &Path, index_granularity: usize) -> Result<Self> {
        let granularity = index_granularity.max(1);
        #[cfg(target_os = "linux")]
        {
            Self::open_inner(path, granularity, None)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self::open_inner(path, granularity)
        }
    }

    /// V 项：打开 SST 并注入 io_uring 后端池（Linux + `runtime.io_uring_enabled`）——
    /// 块读经 SQPOLL 队列提交；Windows / 未启用传 None（走同步 read_at）。
    #[cfg(target_os = "linux")]
    pub fn open_with_io_uring(
        path: &Path,
        index_granularity: usize,
        iou: Option<std::sync::Arc<crate::io_queue::backend::IoUringPool>>,
    ) -> Result<Self> {
        let granularity = index_granularity.max(1);
        Self::open_inner(path, granularity, iou)
    }

    fn open_inner(
        path: &Path,
        index_granularity: usize,
        #[cfg(target_os = "linux")]
        iou: Option<std::sync::Arc<crate::io_queue::backend::IoUringPool>>,
    ) -> Result<Self> {
        let mut file = std::fs::File::open(path).map_err(Error::Io)?;
        let fsize = file.metadata().map_err(Error::Io)?.len();
        if fsize < 8 + 54 {
            return Err(Error::Corrupted("SST 文件过小".into()));
        }
        // 文件尾 8 字节：Footer 起始偏移指针
        let mut ptr = [0u8; 8];
        file.seek(std::io::SeekFrom::End(-8)).map_err(Error::Io)?;
        file.read_exact(&mut ptr).map_err(Error::Io)?;
        let footer_offset = u64::from_le_bytes(ptr);

        // 读 Footer 主体（固定 54 字节）
        let mut fb = vec![0u8; 54];
        file.seek(std::io::SeekFrom::Start(footer_offset))
            .map_err(Error::Io)?;
        file.read_exact(&mut fb).map_err(Error::Io)?;

        if &fb[0..8] != SST_MAGIC {
            return Err(Error::Corrupted("SST Footer 魔数错误".into()));
        }
        let version = u16::from_le_bytes([fb[8], fb[9]]);
        // 向后兼容：v3（行式）与 v4（PAX）均可读
        if !(SST_VERSION_ROW..=SST_VERSION).contains(&version) {
            return Err(Error::Corrupted(format!("SST 版本不支持: {version}")));
        }
        let index_offset = u64::from_le_bytes(fb[10..18].try_into().unwrap());
        let index_len = u32::from_le_bytes(fb[18..22].try_into().unwrap()) as usize;
        let bloom_offset = u64::from_le_bytes(fb[22..30].try_into().unwrap());
        let bloom_len = u32::from_le_bytes(fb[30..34].try_into().unwrap()) as usize;
        let key_count = u64::from_le_bytes(fb[34..42].try_into().unwrap());
        let fb_footer_offset = u64::from_le_bytes(fb[42..50].try_into().unwrap());
        if fb_footer_offset != footer_offset {
            return Err(Error::Corrupted("SST Footer 偏移不一致".into()));
        }

        // 校验 Footer CRC（覆盖前 50 字节）
        let expected = u32::from_le_bytes(fb[50..54].try_into().unwrap());
        let actual = crc32(&fb[..50]);
        if expected != actual {
            return Err(Error::Corrupted("SST Footer CRC 校验失败".into()));
        }

        // 读 Block Index（v3 无字段级 Zone Map，v4 有）
        let mut ib = vec![0u8; index_len];
        file.seek(std::io::SeekFrom::Start(index_offset))
            .map_err(Error::Io)?;
        file.read_exact(&mut ib).map_err(Error::Io)?;
        let index = decode_index(&ib, version)?;
        let index_count = index.len();
        // 两级索引（design 4.4.2）：只保留每 granularity 块一条摘要常驻内存，
        // 精确索引（Level 2）懒加载——open 后不再持有完整 IndexEntry。
        let summary = index
            .iter()
            .enumerate()
            .filter(|(i, _)| i % index_granularity == 0)
            .map(|(i, e)| SummaryEntry {
                first_key: e.first_key.clone(),
                block_index: i,
            })
            .collect::<Vec<_>>();

        // 读 Bloom 区：v5 为分区布隆列表；v3/v4 为旧单布隆
        let mut bb = vec![0u8; bloom_len];
        file.seek(std::io::SeekFrom::Start(bloom_offset))
            .map_err(Error::Io)?;
        file.read_exact(&mut bb).map_err(Error::Io)?;
        let (partition_blooms, bloom) = if version >= SST_VERSION {
            // v5：Count(u32) + [len(u32) + bytes]*，与 Index 对齐（每块一个）
            let mut pb = Vec::new();
            let mut cur = 0usize;
            let count = u32::from_le_bytes(
                bb.get(cur..cur + 4)
                    .ok_or_else(|| Error::Corrupted("分区布隆计数越界".into()))?
                    .try_into()
                    .unwrap(),
            ) as usize;
            cur += 4;
            for _ in 0..count {
                let len = u32::from_le_bytes(
                    bb.get(cur..cur + 4)
                        .ok_or_else(|| Error::Corrupted("分区布隆长度越界".into()))?
                        .try_into()
                        .unwrap(),
                ) as usize;
                cur += 4;
                let bytes = bb
                    .get(cur..cur + len)
                    .ok_or_else(|| Error::Corrupted("分区布隆数据越界".into()))?
                    .to_vec();
                cur += len;
                pb.push(bytes);
            }
            (Some(pb), None)
        } else {
            let bloom_len_u32 = u32::from_le_bytes(bb[0..4].try_into().unwrap()) as usize;
            if 4 + bloom_len_u32 != bb.len() {
                return Err(Error::Corrupted("Bloom 长度不一致".into()));
            }
            let b = BloomFilter::from_bytes(&bb[4..])
                .ok_or_else(|| Error::Corrupted("Bloom 解析失败".into()))?;
            (None, Some(b))
        };

        // 读取 Header 获取压缩与块大小
        let mut hb = vec![0u8; 15];
        file.seek(std::io::SeekFrom::Start(0)).map_err(Error::Io)?;
        file.read_exact(&mut hb).map_err(Error::Io)?;
        if &hb[0..8] != SST_MAGIC {
            return Err(Error::Corrupted("SST Header 魔数错误".into()));
        }
        let compression = Compression::from_code(hb[10])?;
        let _block_size = u32::from_le_bytes(hb[11..15].try_into().unwrap()) as usize;

        let min_key = index.first().map(|e| e.first_key.clone()).unwrap_or_default();
        let max_key = index.last().map(|e| e.max_key.clone()).unwrap_or_default();
        // P3-C：从 min_key 提取 table_id（docid 高 2 字节；非 docid 键 → None）
        let table_id = if min_key.len() >= 2 {
            Some(u16::from_be_bytes([min_key[0], min_key[1]]))
        } else {
            None
        };

        Ok(Self {
            path: path.to_path_buf(),
            file,
            footer: SstFooter {
                index_offset,
                index_len,
                bloom_offset,
                bloom_len,
                key_count,
                footer_offset,
            },
            summary,
            index_count,
            index_granularity,
            full_index: Mutex::new(None),
            partition_blooms,
            bloom,
            min_key,
            max_key,
            table_id,
            file_len: fsize,
            compression,
            format: version,
            heat: PerCpuCounter::new(),
            #[cfg(target_os = "linux")]
            iou,
        })
    }

    pub fn footer(&self) -> &SstFooter {
        &self.footer
    }

    /// 文件字节数（open 时缓存；写路径零 syscall 读段大小）。
    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    /// R 项：段 key 范围 [min, max]（闭区间）。空段（无块/无 key）或单侧缺失返回 None
    /// = 无约束（调用方不得跳过）。用于点查段级 Zone Map 粗筛。
    pub fn key_range(&self) -> Option<(&[u8], &[u8])> {
        if self.min_key.is_empty() || self.max_key.is_empty() {
            return None;
        }
        Some((&self.min_key, &self.max_key))
    }

    /// Ex-5.9：读命中递增热度（冷热感知 Compaction 数据源；Ex-7.1 按核无竞争）。
    pub fn touch(&self) {
        self.heat.inc();
    }

    /// Ex-5.9：当前读热度计数。
    pub fn heat(&self) -> u64 {
        self.heat.get()
    }

    /// v5 分区布隆原始字节（每块一个，与 Index 对齐）。
    pub fn partition_blooms(&self) -> Option<&[Vec<u8>]> {
        self.partition_blooms.as_deref()
    }

    /// v3/v4 旧格式整文件布隆（格式兼容用）。
    pub fn legacy_bloom(&self) -> Option<&BloomFilter> {
        self.bloom.as_ref()
    }

    /// 精确块索引（Level 2，design 4.4.2）：懒加载触发后返回完整索引副本。
    /// 供测试 / 全量迭代使用；生产读路径走内部 `block_entry` 按需取单条。
    pub fn index(&self) -> Vec<IndexEntry> {
        self.ensure_index().expect("精确索引加载失败");
        self.full_index.lock().unwrap().as_ref().unwrap().clone()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// P3-C：获取该 SST 所属表 ID（None = 未知/混合索引）。
    pub fn table_id(&self) -> Option<u16> {
        self.table_id
    }

    /// 精确块数（无需触发 Level 2 加载，open 时即得）。
    pub fn index_len(&self) -> usize {
        self.index_count
    }

    /// 两级索引粒度（每 N 个块一条摘要）。
    pub fn index_granularity(&self) -> usize {
        self.index_granularity
    }

    /// Level 1 常驻摘要（测试 / 监控：验证内存减负）。
    pub fn summary(&self) -> &[SummaryEntry] {
        &self.summary
    }

    /// Level 1 摘要条数。
    pub fn summary_len(&self) -> usize {
        self.summary.len()
    }

    /// Level 2 精确索引是否已懒加载（测试 / 监控）。
    pub fn level2_loaded(&self) -> bool {
        self.full_index.lock().unwrap().is_some()
    }

    /// 文件格式版本（v3=纯行式，v4=PAX，v5=分区布隆）——iter.rs 块解码按版本分发。
    pub(crate) fn format(&self) -> u16 {
        self.format
    }

    /// Level 2 懒加载：首次访问从磁盘解码精确块索引并缓存（design 4.4.2 按需加载）。
    fn ensure_index(&self) -> Result<()> {
        if self.full_index.lock().unwrap().is_some() {
            return Ok(());
        }
        let mut ib = vec![0u8; self.footer.index_len];
        let mut f = std::fs::File::open(&self.path)?;
        f.seek(std::io::SeekFrom::Start(self.footer.index_offset))?;
        f.read_exact(&mut ib)?;
        let index = decode_index(&ib, self.format)?;
        *self.full_index.lock().unwrap() = Some(index);
        Ok(())
    }

    /// 取精确索引中第 idx 块的条目（克隆，不持有借用）。
    pub(crate) fn block_entry(&self, idx: usize) -> Result<IndexEntry> {
        self.ensure_index()?;
        self.full_index
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .get(idx)
            .cloned()
            .ok_or_else(|| Error::Corrupted(format!("块下标越界: {idx}")))
    }

    /// 等值查询：定位块 → 分区布隆剪枝（v5）/ 整文件布隆剪枝（v3/v4）→ 读块 → 块内扫描。
    /// 返回 `(value, seq)`：`value=None` 表示 Tombstone（已删除），`None` 整体表示不存在。
    /// O 项第①步：读路径 `&self` 化（内部可变由 RefCell/原子承担）。
    pub fn get(&self, key: &[u8]) -> Result<Option<(Option<Vec<u8>>, u64)>> {
        // v5 分区布隆：先定位块，再只校验目标块布隆（design 4.4.2 按需加载）
        if let Some(pb) = &self.partition_blooms {
            let Some(idx) = self.locate_block_index(key)? else {
                return Ok(None);
            };
            if let Some(bytes) = pb.get(idx) {
                if let Some(b) = BloomFilter::from_bytes(bytes) {
                    if !b.maybe_contains(&key.to_vec()) {
                        return Ok(None);
                    }
                }
            }
            let e = self.block_entry(idx)?;
            let data = self.read_block(&e)?;
            return self.scan_block_for_key(&data, key);
        }
        // v3/v4：整文件布隆粗筛
        if let Some(bloom) = &self.bloom {
            if !bloom.maybe_contains(&key.to_vec()) {
                return Ok(None);
            }
        }
        let Some(e) = self.locate_block(key)? else {
            return Ok(None);
        };
        let data = self.read_block(&e)?;
        self.scan_block_for_key(&data, key)
    }

    /// 定位包含 key 的块在 Index 中的下标（二分首个 first_key <= key 的块）。
    /// 触发 Level 2 懒加载。（iter.rs 的 SstRangeIter 起始块定位亦复用。）
    pub(crate) fn locate_block_index(&self, key: &[u8]) -> Result<Option<usize>> {
        self.ensure_index()?;
        let index = self.full_index.lock().unwrap();
        let index = index.as_ref().unwrap();
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
            Ok(None)
        } else {
            Ok(Some(lo - 1))
        }
    }

    /// 块内等值扫描（按文件格式版本正确处理行式 / PAX 块）。供块缓存命中路径复用。
    pub fn scan_block_for_key(
        &self,
        block: &[u8],
        key: &[u8],
    ) -> Result<Option<(Option<Vec<u8>>, u64)>> {
        // S 项：同 key 多版本取最大 seq（最新版本）；Tombstone value=None 保留
        let mut best: Option<(Option<Vec<u8>>, u64)> = None;
        for (k, v, seq) in decode_data_block(block, self.format)? {
            if k == key && best.as_ref().map_or(true, |(_, bs)| seq > *bs) {
                best = Some((v, seq));
            }
        }
        Ok(best)
    }

    /// 块内快照等值查询（S 项）：返回 **seq ≤ snapshot_seq** 的最大版本。
    /// 该版本为 Tombstone → value=None（快照点已删除）；无 ≤ 快照版本 → None。
    pub fn scan_block_for_key_at(
        &self,
        block: &[u8],
        key: &[u8],
        snapshot_seq: u64,
    ) -> Result<Option<(Option<Vec<u8>>, u64)>> {
        let mut best: Option<(Option<Vec<u8>>, u64)> = None;
        for (k, v, seq) in decode_data_block(block, self.format)? {
            if k == key && seq <= snapshot_seq && best.as_ref().map_or(true, |(_, bs)| seq > *bs) {
                best = Some((v, seq));
            }
        }
        Ok(best)
    }

    /// 块内批量等值扫描（N 项 batch_get）：一次解码数据块，返回 `targets` 集合中全部命中。
    /// 同块多 key 共享一次解压/解码，避免逐 key 重复读块。
    pub fn scan_block_for_keys(
        &self,
        block: &[u8],
        targets: &std::collections::HashSet<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>, u64)>> {
        let mut hits = Vec::new();
        for (k, v, seq) in decode_data_block(block, self.format)? {
            if targets.contains(&k) {
                hits.push((k, v, seq));
            }
        }
        Ok(hits)
    }

    /// P87②：块内批量等值扫描（投影版）——只解码 `fields` 指定列：
    /// - PAX 块 → `decode_pax_block_fields` 单次多列解码（免整行 25 列重构）；
    /// - 行式块 → 整块解码后逐行按需字段提取（P86② 语义）。
    /// 返回 `(key, fields_values, seq)`：`fields_values` = 该行各请求字段的 JSON 值字节
    /// （缺列 → None）；`fields_values` 整体为 None 表示 Tombstone（语义同
    /// `scan_block_for_keys` 的 `value=None`）。
    pub fn scan_block_for_keys_fields(
        &self,
        block: &[u8],
        targets: &std::collections::HashSet<Vec<u8>>,
        fields: &[String],
    ) -> Result<Vec<(Vec<u8>, Option<Vec<Option<Vec<u8>>>>, u64)>> {
        // PAX 块（v4+ 块首 kind）：列解码直取，免整行重构
        if self.format >= SST_VERSION && block.first() == Some(&BLOCK_KIND_PAX) {
            let mut hits = Vec::new();
            for (k, vals, seq) in decode_pax_block_fields(block, fields)? {
                if targets.contains(&k) {
                    hits.push((k, Some(vals), seq));
                }
            }
            return Ok(hits);
        }
        // 行式块（含 v3）：整块解码后逐行按需提取字段
        let mut hits = Vec::new();
        for (k, v, seq) in decode_data_block(block, self.format)? {
            if !targets.contains(&k) {
                continue;
            }
            let vals = match v {
                Some(row) => Some(extract_fields_from_json_row(&row, fields)),
                None => None, // Tombstone
            };
            hits.push((k, vals, seq));
        }
        Ok(hits)
    }

    /// 定位包含 key 的数据块（二分首个 first_key <= key 的块）。触发 Level 2 懒加载。
    fn locate_block(&self, key: &[u8]) -> Result<Option<IndexEntry>> {
        self.ensure_index()?;
        let index = self.full_index.lock().unwrap();
        let index = index.as_ref().unwrap();
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
            Ok(None)
        } else {
            Ok(index.get(lo - 1).cloned())
        }
    }

    /// 等值定位：返回 `(块下标, 块条目)`——**只克隆单条**精确索引（design 4.4.2 按需）。
    /// 供块缓存读路径（`column_family::get_from_sst`）复用：避免克隆整个 Level 2 索引。
    pub(crate) fn locate_indexed_block(&self, key: &[u8]) -> Result<Option<(usize, IndexEntry)>> {
        let Some(idx) = self.locate_block_index(key)? else {
            return Ok(None);
        };
        Ok(Some((idx, self.block_entry(idx)?)))
    }

    /// 读取并解压数据块，校验 CRC（位置读：`&self` 可并发，读写分离读路径基础）。
    pub fn read_block(&self, e: &IndexEntry) -> Result<Vec<u8>> {
        let mut comp = vec![0u8; e.comp_len as usize];
        self.read_at_io(&mut comp, e.offset)?;

        // 读 Trailer 校验
        let mut trailer = vec![0u8; TRAILER_LEN];
        self.read_at_io(&mut trailer, e.offset + e.comp_len as u64)?;
        let raw_len = u32::from_le_bytes(trailer[0..4].try_into().unwrap()) as usize;
        let comp_len = u32::from_le_bytes(trailer[4..8].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(trailer[8..12].try_into().unwrap());
        if comp_len != comp.len() {
            return Err(Error::Corrupted("块长度不一致".into()));
        }
        if crc32(&comp) != crc {
            return Err(Error::Corrupted("数据块 CRC 校验失败".into()));
        }
        self.decompress(&comp, raw_len)
    }

    fn decompress(&self, data: &[u8], raw_len: usize) -> Result<Vec<u8>> {
        match self.compression {
            Compression::None => Ok(data.to_vec()),
            Compression::Zstd | Compression::Lz4 | Compression::Snappy => {
                zstd::bulk::decompress(data, raw_len.max(64))
                    .map_err(|e| Error::Io(std::io::Error::other(format!("解压失败: {e}"))))
            }
        }
    }

    /// V 项：位置读转发——Linux + io_uring 启用（`self.iou` 有值）时经 SQPOLL 队列提交
    /// （`IoClass::Sst` 队列，免 syscall 异步完成），否则回退同步 `read_at`。
    /// 非 Linux 编译恒走同步路径（`iou` 字段不存在）。
    #[cfg(target_os = "linux")]
    fn read_at_io(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        if let Some(iou) = &self.iou {
            iou.read_at(crate::io_queue::IoClass::Sst, &self.file, buf, offset)
                .map_err(Error::Io)?;
            return Ok(());
        }
        read_at(&self.file, buf, offset)
    }

    /// 非 Linux：直接同步读（无 io_uring 路径）。
    #[cfg(not(target_os = "linux"))]
    fn read_at_io(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        read_at(&self.file, buf, offset)
    }

    /// U 项：合并读连续数据块（冷扫预读）——一次 `read_at` 覆盖整组
    /// （4×4KB → 1×16KB，减少顺序扫描 syscall/IO 次数），逐块切片 + CRC 校验 + 解压。
    /// 布局假设：块紧凑连续（`compressed + TRAILER_LEN` 紧邻）；校验失败回退逐块读（安全）。
    pub(crate) fn read_block_group(&self, entries: &[IndexEntry]) -> Result<Vec<Vec<u8>>> {
        let n = entries.len();
        if n == 0 {
            return Ok(Vec::new());
        }
        if n == 1 {
            return Ok(vec![self.read_block(&entries[0])?]);
        }
        let start = entries[0].offset;
        let last = &entries[n - 1];
        let end = last.offset + last.comp_len as u64 + TRAILER_LEN as u64;
        let mut buf = vec![0u8; (end - start) as usize];
        self.read_at_io(&mut buf, start)?;
        // CRC 校验：任一失败 → 布局假设失效，回退逐块读（安全）
        for e in entries {
            let rel = (e.offset - start) as usize;
            let comp = &buf[rel..rel + e.comp_len as usize];
            let tr = &buf[rel + e.comp_len as usize..rel + e.comp_len as usize + TRAILER_LEN];
            let comp_len = u32::from_le_bytes(tr[4..8].try_into().unwrap());
            let crc = u32::from_le_bytes(tr[8..12].try_into().unwrap());
            if comp_len != e.comp_len || crc32(comp) != crc {
                return entries.iter().map(|en| self.read_block(en)).collect();
            }
        }
        // 7.98：组读 8 块（IO 合并收益保留）；组内并行解压实测 spawn 开销 > 收益已回退
        let mut out = Vec::with_capacity(n);
        for e in entries {
            let rel = (e.offset - start) as usize;
            let comp = &buf[rel..rel + e.comp_len as usize];
            out.push(self.decompress(comp, e.raw_len as usize)?);
        }
        Ok(out)
    }

    /// Ex-5.8 元数据-数据解耦：读取块的**原始压缩字节** + 解码内容，供块级复用 Compaction
    /// 原样拷贝数据块（不解压校验 trailer，由复用写入方 `add_raw_block` 重建）。
    /// O 项第③步：`&self`——compact 经 `Arc<SstReader>` 并发读输入段（内部 `read_at` 无状态 seek）。
    pub fn block_raw(&self, e: &IndexEntry) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut comp = vec![0u8; e.comp_len as usize];
        self.read_at_io(&mut comp, e.offset)?;
        let raw = self.decompress(&comp, e.raw_len as usize)?;
        Ok((comp, raw))
    }

    /// 迭代：按块顺序扫描全部条目。回调 `f(key, value, seq)`，`value=None` 表示 Tombstone。
    /// O 项第③步：`&self`（compact 经 `Arc<SstReader>` 并发读）。
    pub fn iterate<F: FnMut(&[u8], Option<&[u8]>, u64)>(&self, mut f: F) -> Result<()> {
        let entries = self.index();
        for e in entries {
            let data = self.read_block(&e)?;
            for (k, v, seq) in decode_data_block(&data, self.format)? {
                f(&k, v.as_deref(), seq);
            }
        }
        Ok(())
    }

    /// 范围扫描 [start, end]（闭区间；None 端无边界），利用块级 Zone Map 剪枝：
    /// 块范围 [first_key, max_key] 与查询区间无交集则跳过，不读块、不解压（design 4.4.1）。
    /// 回调 `f(key, value, seq)`，`value=None` 表示 Tombstone。
    /// O 项第①步：范围扫描读路径 `&self` 化。
    pub fn scan_range<F: FnMut(&[u8], Option<&[u8]>, u64)>(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        mut f: F,
    ) -> Result<()> {
        for e in self.index() {
            // Zone Map 剪枝
            if let Some(s) = start {
                if e.max_key.as_slice() < s {
                    continue; // 块最大值仍小于区间下界
                }
            }
            if let Some(en) = end {
                if e.first_key.as_slice() > en {
                    break; // 索引按 key 有序，后续块更大
                }
            }
            let data = self.read_block(&e)?;
            for (k, v, seq) in decode_data_block(&data, self.format)? {
                if let Some(s) = start {
                    if k.as_slice() < s {
                        continue;
                    }
                }
                if let Some(en) = end {
                    if k.as_slice() > en {
                        continue;
                    }
                }
                f(&k, v.as_deref(), seq);
            }
        }
        Ok(())
    }
}

/// 解码块索引字节流。`version` 决定是否解析字段级 Zone Map（v3 无，v4 有）。
fn decode_index(ib: &[u8], version: u16) -> Result<Vec<IndexEntry>> {
    let mut cur = 0usize;
    let count = decode_varint(ib, &mut cur)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let first_key = decode_varlen(ib, &mut cur)?.to_vec();
        let max_key = decode_varlen(ib, &mut cur)?.to_vec();
        if cur + 16 > ib.len() {
            return Err(Error::Corrupted("索引条目越界".into()));
        }
        let offset = u64::from_le_bytes(ib[cur..cur + 8].try_into().unwrap());
        let raw_len = u32::from_le_bytes(ib[cur + 8..cur + 12].try_into().unwrap());
        let comp_len = u32::from_le_bytes(ib[cur + 12..cur + 16].try_into().unwrap());
        cur += 16;
        let mut zones = Vec::new();
        if version >= SST_VERSION {
            if cur + 2 > ib.len() {
                return Err(Error::Corrupted("索引 Zone 计数越界".into()));
            }
            let zone_count = u16::from_le_bytes(ib[cur..cur + 2].try_into().unwrap()) as usize;
            cur += 2;
            for _ in 0..zone_count {
                let field = String::from_utf8(decode_varlen(ib, &mut cur)?.to_vec())
                    .map_err(|_| Error::Corrupted("Zone 字段名非法 UTF-8".into()))?;
                let min = decode_varlen(ib, &mut cur)?.to_vec();
                let max = decode_varlen(ib, &mut cur)?.to_vec();
                if cur + 8 > ib.len() {
                    return Err(Error::Corrupted("索引 Zone 条目越界".into()));
                }
                let present_count = u32::from_le_bytes(ib[cur..cur + 4].try_into().unwrap());
                let null_count = u32::from_le_bytes(ib[cur + 4..cur + 8].try_into().unwrap());
                cur += 8;
                // P3-B：v6 读取 sum 字段（f64 8 字节）；v5 旧段无此字段 → sum=0.0
                let sum = if version >= 6 {
                    if cur + 8 > ib.len() {
                        return Err(Error::Corrupted("索引 Zone sum 越界".into()));
                    }
                    let s = f64::from_le_bytes(ib[cur..cur + 8].try_into().unwrap());
                    cur += 8;
                    s
                } else {
                    0.0
                };
                zones.push(FieldZone {
                    field,
                    min,
                    max,
                    present_count,
                    null_count,
                    sum,
                });
            }
        }
        out.push(IndexEntry {
            first_key,
            max_key,
            offset,
            raw_len,
            comp_len,
            zones,
        });
    }
    Ok(out)
}
