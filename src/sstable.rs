//! SSTable：磁盘有序文件（design 4.4 / development 步骤 6 / 阶段 1.5 PAX）。
//!
//! 文件布局：
//!
//! ```text
//! ┌────────────────────────────┐
//! │ File Header (magic+版本+配置) │
//! ├────────────────────────────┤
//! │ Data Block 0..N（独立压缩+CRC）│
//! ├────────────────────────────┤
//! │ Block Index（稀疏索引+Zone Map）│
//! │ Bloom Filter               │
//! ├────────────────────────────┤
//! │ Footer（各段偏移/长度/统计）    │
//! └────────────────────────────┘
//! ```
//!
//! 数据块两种布局（阶段 1.5，块首 1 字节 kind 标记，同文件可混合）：
//! - **行式块（kind=0）**：`Key(VarLen) ++ Value(VarLen) ++ Flags(u8) ++ Seq(u64)`，兼容 MVP；
//! - **PAX 列式块（kind=1）**：`列偏移量表（字段名+热/冷标记+偏移/长度）→ 热列组（块头）→ 冷列组（块尾）`，
//!   文档按 JSON 字段拆列（preserve_order 保序，重组与写入字节一致），宽表点查可只读热列切片；
//! - 文件级向后兼容：v3（全行式）与 v4（可含 PAX）共存，Reader 按 Footer 版本 + 块 kind 双重分发。
//!
//! 块尾 Trailer：`RawLen(u32) ++ CompLen(u32) ++ CRC32(u32)`，损坏只影响单块。

use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use crate::bloom::BloomFilter;
use crate::error::{Error, Result};
use crate::keys::{decode_varint, decode_varlen, encode_varint, encode_varlen};

/// 文件魔数 + 版本。
pub const SST_MAGIC: &[u8; 8] = b"NVSSTL01";
/// v4：数据块引入 PAX 列式布局（块首 kind 字节）；仍可读取 v3（行式）。
pub const SST_VERSION: u16 = 4;
/// v3：行式数据块（无 kind 字节）——Reader 向后兼容的最低版本。
pub const SST_VERSION_ROW: u16 = 3;

/// 数据块 kind：行式（MVP 兼容）。
pub const BLOCK_KIND_ROW: u8 = 0;
/// 数据块 kind：PAX 列式（阶段 1.5）。
pub const BLOCK_KIND_PAX: u8 = 1;

/// 条目 Flags：Put。
pub const FLAG_PUT: u8 = 0;
/// 条目 Flags：Tombstone（删除标记）。
pub const FLAG_DELETE: u8 = 1;

/// 压缩算法标识（与 config.sstable.compression 字符串对应）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Zstd,
    Lz4,
    Snappy,
}

impl Compression {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Zstd => "zstd",
            Self::Lz4 => "lz4",
            Self::Snappy => "snappy",
        }
    }

    fn code(&self) -> u8 {
        match self {
            Self::None => 0,
            Self::Zstd => 1,
            Self::Lz4 => 2,
            Self::Snappy => 3,
        }
    }

    fn from_code(c: u8) -> Result<Self> {
        match c {
            0 => Ok(Self::None),
            1 => Ok(Self::Zstd),
            2 => Ok(Self::Lz4),
            3 => Ok(Self::Snappy),
            _ => Err(Error::Corrupted(format!("未知压缩算法码 {c}"))),
        }
    }
}

/// 压缩算法从配置字符串解析（实现标准 trait，供 `Compression::from_str` 调用）。
impl std::str::FromStr for Compression {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "none" => Ok(Self::None),
            "zstd" => Ok(Self::Zstd),
            "lz4" => Ok(Self::Lz4),
            "snappy" => Ok(Self::Snappy),
            other => Err(Error::Config(format!("sstable.compression 非法: {other}"))),
        }
    }
}

/// 数据块 Trailer（写在每个数据块末尾，固定 12 字节）。
const TRAILER_LEN: usize = 12;

/// 写入缓冲中的待编码行（flush 时统一决定行式 / PAX 布局）。
struct PendingRow {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    flag: u8,
    seq: u64,
}

/// 字段级 Zone Map（阶段 1.5，design 4.4.1 强化）：块内单字段采样统计，供范围条件剪枝。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FieldZone {
    pub field: String,
    /// 该列所有值的最小字节（字符串按字节序；数值为 JSON 序列化后的字节序近似）。
    pub min: Vec<u8>,
    pub max: Vec<u8>,
    /// 出现次数（present，含 null）。
    pub present_count: u32,
    /// null 计数（缺失字段不计）。
    pub null_count: u32,
}

/// SSTable Writer：按 key 升序写入，自动切块、压缩、维护稀疏索引与布隆。
pub struct SstWriter {
    out: std::fs::File,
    compression: Compression,
    block_size: usize,
    /// 当前数据块缓冲（行式攒批，flush 时编码）。
    buf: Vec<PendingRow>,
    /// 块索引：[first_key, offset, raw_len, comp_len, zones]。
    index: Vec<IndexEntry>,
    /// 布隆过滤器。
    bloom: BloomFilter,
    /// 已写入字节数（含 header）。
    written: u64,
    /// 写入 key 总数。
    key_count: u64,
    /// 上一个写入的 key（校验升序）。
    last_key: Option<Vec<u8>>,
    /// 当前块内最后一个 key（Zone Map max）。
    buf_last_key: Option<Vec<u8>>,
    /// zstd level 压缩参数。
    zstd_level: i32,
    /// PAX 热字段白名单（空数组 = 纯行式，MVP 行为不变）。
    pax_hot_fields: Vec<String>,
}

/// 稀疏索引条目（含块级 Zone Map 的 key 维度：min=first_key，max=max_key）。
/// 字段级 Zone Map（各列 min/max/null 计数）随阶段 1.5 PAX 列组落地，v4 索引编码。
#[derive(Debug, Clone)]
pub struct IndexEntry {
    /// 块首键 = Zone Map min。
    pub first_key: Vec<u8>,
    /// 块末键 = Zone Map max。
    pub max_key: Vec<u8>,
    pub offset: u64,
    pub raw_len: u32,
    pub comp_len: u32,
    /// 字段级 Zone Map（仅 v4 PAX 块采集）。
    pub zones: Vec<FieldZone>,
}

impl SstWriter {
    pub fn new(
        path: &Path,
        compression: Compression,
        compression_level: i32,
        block_size: usize,
        expected_keys: usize,
    ) -> Result<Self> {
        Self::new_with_pax(
            path,
            compression,
            compression_level,
            block_size,
            expected_keys,
            &[],
        )
    }

    /// 带 PAX 热字段白名单的构造器：`hot_fields` 为空时行为与 MVP 行式完全一致。
    pub fn new_with_pax(
        path: &Path,
        compression: Compression,
        compression_level: i32,
        block_size: usize,
        expected_keys: usize,
        hot_fields: &[String],
    ) -> Result<Self> {
        let out = std::fs::File::create(path).map_err(Error::Io)?;
        let mut w = Self {
            out,
            compression,
            block_size,
            buf: Vec::new(),
            index: Vec::new(),
            bloom: BloomFilter::with_estimated_keys(expected_keys),
            written: 0,
            key_count: 0,
            last_key: None,
            buf_last_key: None,
            zstd_level: compression_level.clamp(1, 22),
            pax_hot_fields: hot_fields.to_vec(),
        };
        w.write_header()?;
        Ok(w)
    }

    fn write_header(&mut self) -> Result<()> {
        let mut h = Vec::with_capacity(32);
        h.extend_from_slice(SST_MAGIC);
        h.extend_from_slice(&SST_VERSION.to_le_bytes());
        h.push(self.compression.code());
        h.extend_from_slice(&(self.block_size as u32).to_le_bytes());
        self.write_all(&h)
    }

    /// 追加一条 (key, value, seq)。要求 key 升序。
    pub fn add(&mut self, key: &[u8], value: &[u8], seq: u64) -> Result<()> {
        self.add_inner(key, Some(value), FLAG_PUT, seq)
    }

    /// 追加删除标记（Tombstone）。要求 key 升序。
    pub fn add_tombstone(&mut self, key: &[u8], seq: u64) -> Result<()> {
        self.add_inner(key, None, FLAG_DELETE, seq)
    }

    fn add_inner(&mut self, key: &[u8], value: Option<&[u8]>, flag: u8, seq: u64) -> Result<()> {
        if let Some(last) = &self.last_key {
            if key <= last.as_slice() {
                return Err(Error::Corrupted(format!(
                    "SST 写入 key 必须严格升序: {:?} <= {:?}",
                    key, last
                )));
            }
        }
        self.buf.push(PendingRow {
            key: key.to_vec(),
            value: value.map(|v| v.to_vec()),
            flag,
            seq,
        });
        self.bloom.insert(&key.to_vec());
        self.key_count += 1;
        self.last_key = Some(key.to_vec());
        self.buf_last_key = Some(key.to_vec());

        if self.estimate_block_bytes() >= self.block_size {
            self.flush_block()?;
        }
        Ok(())
    }

    /// 估算当前块编码后的字节数（行式条目：key+value+1+8；PAX 近似为行式）。
    fn estimate_block_bytes(&self) -> usize {
        self.buf
            .iter()
            .map(|r| r.key.len() + 1 + r.value.as_ref().map_or(0, |v| v.len()) + 1 + 8 + 8)
            .sum()
    }

    /// 冲刷当前块：统一编码（行式 / PAX）→ 压缩 → 写盘 → 索引 + 字段级 Zone Map。
    fn flush_block(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let offset = self.written;
        let first_key = self.buf[0].key.clone();
        let max_key = self
            .buf_last_key
            .take()
            .unwrap_or_else(|| first_key.clone());

        // 尝试 PAX 列式编码；非 JSON 值 / Tombstone / 无字段时回退行式
        let (raw, zones) = if self.pax_hot_fields.is_empty() {
            (encode_row_block(&self.buf, BLOCK_KIND_ROW)?, Vec::new())
        } else {
            match encode_pax_block(&self.buf, &self.pax_hot_fields) {
                Ok((raw, zones)) => (raw, zones),
                Err(_) => (encode_row_block(&self.buf, BLOCK_KIND_ROW)?, Vec::new()),
            }
        };
        let raw_len = raw.len();
        let compressed = self.compress(&raw)?;
        let comp_len = compressed.len();

        self.write_all(&compressed)?;
        // Trailer
        let mut trailer = Vec::with_capacity(TRAILER_LEN);
        trailer.extend_from_slice(&(raw_len as u32).to_le_bytes());
        trailer.extend_from_slice(&(comp_len as u32).to_le_bytes());
        trailer.extend_from_slice(&crc32(&compressed).to_le_bytes());
        self.write_all(&trailer)?;

        self.index.push(IndexEntry {
            first_key,
            max_key,
            offset,
            raw_len: raw_len as u32,
            comp_len: comp_len as u32,
            zones,
        });
        self.buf.clear();
        Ok(())
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.compression {
            Compression::None => Ok(data.to_vec()),
            Compression::Zstd => zstd::bulk::compress(data, self.zstd_level)
                .map_err(|e| Error::Io(std::io::Error::other(format!("zstd 压缩失败: {e}")))),
            // MVP 先统一走 zstd；lz4/snappy 留待阶段 1.5 引入，此处映射到 zstd 以便配置兼容
            Compression::Lz4 | Compression::Snappy => zstd::bulk::compress(data, self.zstd_level)
                .map_err(|e| Error::Io(std::io::Error::other(format!("压缩失败: {e}")))),
        }
    }

    fn write_all(&mut self, data: &[u8]) -> Result<()> {
        self.out.write_all(data).map_err(Error::Io)?;
        self.written += data.len() as u64;
        Ok(())
    }

    /// 完成写入：冲刷末块、写索引、写布隆、写 Footer、fsync。
    pub fn finish(mut self) -> Result<SstFooter> {
        self.flush_block()?;

        // Block Index：块数(varint) + 每条目(VarLen(first_key) + VarLen(max_key) + offset u64 + raw_len u32 + comp_len u32)
        // + v4 字段级 Zone Map（ZoneCount u16 + (VarLen(field) + VarLen(min) + VarLen(max) + present u32 + null u32)*）
        let index_offset = self.written;
        let mut ib = Vec::new();
        encode_varint(&mut ib, self.index.len() as u64);
        for e in &self.index {
            encode_varlen(&mut ib, &e.first_key);
            encode_varlen(&mut ib, &e.max_key);
            ib.extend_from_slice(&e.offset.to_le_bytes());
            ib.extend_from_slice(&e.raw_len.to_le_bytes());
            ib.extend_from_slice(&e.comp_len.to_le_bytes());
            ib.extend_from_slice(&(e.zones.len() as u16).to_le_bytes());
            for z in &e.zones {
                encode_varlen(&mut ib, z.field.as_bytes());
                encode_varlen(&mut ib, &z.min);
                encode_varlen(&mut ib, &z.max);
                ib.extend_from_slice(&z.present_count.to_le_bytes());
                ib.extend_from_slice(&z.null_count.to_le_bytes());
            }
        }
        self.write_all(&ib)?;
        let index_len = ib.len() as u32;

        // Bloom
        let bloom_offset = self.written;
        let bloom_bytes = self.bloom.to_bytes();
        self.write_all(&(bloom_bytes.len() as u32).to_le_bytes())?;
        self.write_all(&bloom_bytes)?;
        let bloom_len = (4 + bloom_bytes.len()) as u32;

        // Footer：magic + 各段偏移/长度 + key_count + 版本
        let footer_offset = self.written;
        let mut fb = Vec::with_capacity(64);
        fb.extend_from_slice(SST_MAGIC);
        fb.extend_from_slice(&SST_VERSION.to_le_bytes());
        fb.extend_from_slice(&index_offset.to_le_bytes());
        fb.extend_from_slice(&index_len.to_le_bytes());
        fb.extend_from_slice(&bloom_offset.to_le_bytes());
        fb.extend_from_slice(&bloom_len.to_le_bytes());
        fb.extend_from_slice(&self.key_count.to_le_bytes());
        fb.extend_from_slice(&footer_offset.to_le_bytes());
        fb.extend_from_slice(&crc32(&fb).to_le_bytes());
        self.write_all(&fb)?;

        // 文件尾 8 字节指针：定位 Footer 起始偏移（Reader 先读此指针）
        self.write_all(&footer_offset.to_le_bytes())?;

        self.out.sync_all().map_err(Error::Io)?;
        Ok(SstFooter {
            index_offset,
            index_len: index_len as usize,
            bloom_offset,
            bloom_len: bloom_len as usize,
            key_count: self.key_count,
            footer_offset,
        })
    }
}

// ---------------------------------------------------------------------------
// 数据块编码（行式 / PAX 列式）
// ---------------------------------------------------------------------------

/// 行式块编码：`kind ++ (VarLen(key) ++ VarLen(value) ++ flag(u8) ++ seq(u64))*`。
fn encode_row_block(rows: &[PendingRow], kind: u8) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(rows.len() * 32);
    out.push(kind);
    for r in rows {
        encode_varlen(&mut out, &r.key);
        match &r.value {
            Some(v) => encode_varlen(&mut out, v),
            None => encode_varlen(&mut out, &[]),
        }
        out.push(r.flag);
        out.extend_from_slice(&r.seq.to_le_bytes());
    }
    Ok(out)
}

/// 尝试将一行 value 解析为保序 JSON 对象；非对象返回 None。
fn json_object(value: &[u8]) -> Option<serde_json::Map<String, serde_json::Value>> {
    let v: serde_json::Value = serde_json::from_slice(value).ok()?;
    v.as_object().cloned()
}

/// PAX 列式块编码：按 JSON 字段拆列（热列组靠前），返回 (块字节, 字段级 Zone Map)。
///
/// 布局：`kind(1) ++ RowCount(u32) ++ Keys(VarLen)* ++ ColCount(u16) ++
/// ColTable((VarLen(field)+IsHot(u8)+Offset(u32)+Len(u32))*) ++
/// 热列数据 ++ 冷列数据 ++ Seqs(u64)*`；列内条目 `Present(u8)+ValLen(VarLen)+Val`。
/// 仅当全部行可解析为 JSON 对象且字段数 > 0 时返回 Ok，否则由调用方回退行式。
fn encode_pax_block(
    rows: &[PendingRow],
    hot_fields: &[String],
) -> Result<(Vec<u8>, Vec<FieldZone>)> {
    use serde_json::Value;

    // 1. 解析全部行
    let mut parsed: Vec<(Vec<u8>, serde_json::Map<String, Value>, u64)> =
        Vec::with_capacity(rows.len());
    for r in rows {
        if r.flag != FLAG_PUT {
            return Err(Error::Corrupted("PAX 块不支持 Tombstone".into()));
        }
        let v = r
            .value
            .as_ref()
            .ok_or_else(|| Error::Corrupted("PAX 块缺少值".into()))?;
        let obj = json_object(v).ok_or_else(|| Error::Corrupted("PAX 值非 JSON 对象".into()))?;
        parsed.push((r.key.clone(), obj, r.seq));
    }
    if parsed.is_empty() {
        return Err(Error::Corrupted("PAX 块为空".into()));
    }

    // 2. 列集：热字段（按白名单顺序，仅保留在行中出现的）→ 冷字段（所有行的字段并集，
    //    按首行出现顺序、后续行新字段追加末尾——弱 schema 下保证字段不丢失）
    let mut cols: Vec<(String, bool)> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for hf in hot_fields {
        if parsed.iter().any(|(_, o, _)| o.contains_key(hf)) && !seen.contains(hf) {
            cols.push((hf.clone(), true));
            seen.insert(hf.clone());
        }
    }
    for (_, obj, _) in &parsed {
        for k in obj.keys() {
            if !seen.contains(k) {
                cols.push((k.clone(), false));
                seen.insert(k.clone());
            }
        }
    }
    if cols.is_empty() {
        return Err(Error::Corrupted("PAX 值无字段".into()));
    }

    // 3. 编码
    let mut out = Vec::new();
    out.push(BLOCK_KIND_PAX);
    out.extend_from_slice(&(parsed.len() as u32).to_le_bytes());
    for (k, _, _) in &parsed {
        encode_varlen(&mut out, k);
    }
    out.extend_from_slice(&(cols.len() as u16).to_le_bytes());
    // 先写列名与 is_hot，offset/len 用占位 8 字节（列数据定位后回填）
    let mut col_entries: Vec<(usize, usize)> = Vec::new(); // (offset_pos, len_pos)
    for (f, hot) in &cols {
        encode_varlen(&mut out, f.as_bytes());
        out.push(if *hot { 1 } else { 0 });
        let off_pos = out.len();
        out.extend_from_slice(&[0u8; 8]); // offset + len 占位
        col_entries.push((off_pos, off_pos + 4));
    }
    // 列数据（按列顺序，热列在前）
    let mut zones: Vec<FieldZone> = Vec::with_capacity(cols.len());
    for (ci, (f, _)) in cols.iter().enumerate() {
        let col_start = out.len();
        let mut min: Option<Vec<u8>> = None;
        let mut max: Option<Vec<u8>> = None;
        let mut present_count: u32 = 0;
        let mut null_count: u32 = 0;
        for (_, obj, _) in &parsed {
            match obj.get(f) {
                None => out.push(0), // present=0（缺失）
                Some(Value::Null) => {
                    out.push(1);
                    out.extend_from_slice(&1u8.to_le_bytes()); // ValLen=1
                    out.push(b'n'); // "null" 的紧凑表示（重组时还原）
                    present_count += 1;
                    null_count += 1;
                }
                Some(v) => {
                    let s = serde_json::to_vec(v)
                        .map_err(|e| Error::Corrupted(format!("字段序列化失败: {e}")))?;
                    out.push(1);
                    encode_varint(&mut out, s.len() as u64);
                    out.extend_from_slice(&s);
                    present_count += 1;
                    if min.as_ref().is_none_or(|m| s.as_slice() < m.as_slice()) {
                        min = Some(s.clone());
                    }
                    if max.as_ref().is_none_or(|m| s.as_slice() > m.as_slice()) {
                        max = Some(s.clone());
                    }
                }
            }
        }
        let col_len = out.len() - col_start;
        // 回填 offset/len
        let (off_pos, len_pos) = col_entries[ci];
        out[off_pos..off_pos + 4].copy_from_slice(&(col_start as u32).to_le_bytes());
        out[len_pos..len_pos + 4].copy_from_slice(&(col_len as u32).to_le_bytes());
        zones.push(FieldZone {
            field: f.clone(),
            min: min.unwrap_or_default(),
            max: max.unwrap_or_default(),
            present_count,
            null_count,
        });
    }
    // Seqs
    for (_, _, seq) in &parsed {
        out.extend_from_slice(&seq.to_le_bytes());
    }
    Ok((out, zones))
}

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

/// CRC32（IEEE 多项式，与 WAL 一致）。
pub fn crc32(data: &[u8]) -> u32 {
    crate::wal::crc32(data)
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// SSTable Reader：mmap 式顺序读取（MVP 用文件 read + seek；阶段 3 可换 io_uring）。
pub struct SstReader {
    path: PathBuf,
    file: std::fs::File,
    footer: SstFooter,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    compression: Compression,
    /// 文件格式版本（v3=纯行式无 kind，v4=可含 PAX）。
    format: u16,
}

impl SstReader {
    pub fn open(path: &Path) -> Result<Self> {
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

        // 读 Bloom
        let mut bb = vec![0u8; bloom_len];
        file.seek(std::io::SeekFrom::Start(bloom_offset))
            .map_err(Error::Io)?;
        file.read_exact(&mut bb).map_err(Error::Io)?;
        let bloom_len_u32 = u32::from_le_bytes(bb[0..4].try_into().unwrap()) as usize;
        if 4 + bloom_len_u32 != bb.len() {
            return Err(Error::Corrupted("Bloom 长度不一致".into()));
        }
        let bloom = BloomFilter::from_bytes(&bb[4..])
            .ok_or_else(|| Error::Corrupted("Bloom 解析失败".into()))?;

        // 读取 Header 获取压缩与块大小
        let mut hb = vec![0u8; 15];
        file.seek(std::io::SeekFrom::Start(0)).map_err(Error::Io)?;
        file.read_exact(&mut hb).map_err(Error::Io)?;
        if &hb[0..8] != SST_MAGIC {
            return Err(Error::Corrupted("SST Header 魔数错误".into()));
        }
        let compression = Compression::from_code(hb[10])?;
        let _block_size = u32::from_le_bytes(hb[11..15].try_into().unwrap()) as usize;

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
            index,
            bloom,
            compression,
            format: version,
        })
    }

    pub fn footer(&self) -> &SstFooter {
        &self.footer
    }

    pub fn bloom(&self) -> &BloomFilter {
        &self.bloom
    }

    pub fn index(&self) -> &[IndexEntry] {
        &self.index
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn index_len(&self) -> usize {
        self.index.len()
    }

    /// 等值查询：布隆剪枝 → 二分定位块 → 读块 → 块内扫描。
    /// 返回 `(value, seq)`：`value=None` 表示 Tombstone（已删除），`None` 整体表示不存在。
    pub fn get(&mut self, key: &[u8]) -> Result<Option<(Option<Vec<u8>>, u64)>> {
        if !self.bloom.maybe_contains(&key.to_vec()) {
            return Ok(None);
        }
        let Some(e) = self.locate_block(key).cloned() else {
            return Ok(None);
        };
        let data = self.read_block(&e)?;
        self.scan_block_for_key(&data, key)
    }

    /// 块内等值扫描（按文件格式版本正确处理行式 / PAX 块）。供块缓存命中路径复用。
    pub fn scan_block_for_key(
        &self,
        block: &[u8],
        key: &[u8],
    ) -> Result<Option<(Option<Vec<u8>>, u64)>> {
        for (k, v, seq) in decode_data_block(block, self.format)? {
            if k == key {
                return Ok(Some((v, seq)));
            }
        }
        Ok(None)
    }

    /// 定位包含 key 的数据块（二分首个 first_key <= key 的块）。
    fn locate_block(&self, key: &[u8]) -> Option<&IndexEntry> {
        let mut lo = 0usize;
        let mut hi = self.index.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.index[mid].first_key.as_slice() <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            None
        } else {
            self.index.get(lo - 1)
        }
    }

    /// 读取并解压数据块，校验 CRC。
    pub fn read_block(&mut self, e: &IndexEntry) -> Result<Vec<u8>> {
        let mut comp = vec![0u8; e.comp_len as usize];
        self.file
            .seek(std::io::SeekFrom::Start(e.offset))
            .map_err(Error::Io)?;
        self.file.read_exact(&mut comp).map_err(Error::Io)?;

        // 读 Trailer 校验
        let mut trailer = vec![0u8; TRAILER_LEN];
        self.file.read_exact(&mut trailer).map_err(Error::Io)?;
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

    /// 迭代：按块顺序扫描全部条目。回调 `f(key, value, seq)`，`value=None` 表示 Tombstone。
    pub fn iterate<F: FnMut(&[u8], Option<&[u8]>, u64)>(&mut self, mut f: F) -> Result<()> {
        let entries: Vec<IndexEntry> = self.index.clone();
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
    pub fn scan_range<F: FnMut(&[u8], Option<&[u8]>, u64)>(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        mut f: F,
    ) -> Result<()> {
        for e in self.index.clone() {
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
                zones.push(FieldZone {
                    field,
                    min,
                    max,
                    present_count,
                    null_count,
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

/// 统一数据块解码：按文件格式版本 + 块 kind 分发（v3=行式，v4=行式/PAX）。
/// 返回 `(key, value, seq)` 行序列，`value=None` 表示 Tombstone。
pub type DecodedRow = (Vec<u8>, Option<Vec<u8>>, u64);

pub fn decode_data_block(data: &[u8], format: u16) -> Result<Vec<DecodedRow>> {
    if format >= SST_VERSION {
        match data.first() {
            Some(&BLOCK_KIND_PAX) => return decode_pax_block(data),
            Some(&BLOCK_KIND_ROW) | Some(_) => {}
            None => return Err(Error::Corrupted("空数据块".into())),
        }
    }
    // 行式解析（v4 跳过块首 kind 字节）
    let start = if format >= SST_VERSION { 1 } else { 0 };
    let mut rows = Vec::new();
    let mut cur = start;
    while cur < data.len() {
        let key = decode_varlen(data, &mut cur)?.to_vec();
        let value = decode_varlen(data, &mut cur)?.to_vec();
        if cur + 9 > data.len() {
            return Err(Error::Corrupted("数据块 flags/seq 越界".into()));
        }
        let flag = data[cur];
        let seq = u64::from_le_bytes(data[cur + 1..cur + 9].try_into().unwrap());
        cur += 9;
        rows.push((key, (flag == FLAG_PUT).then_some(value), seq));
    }
    Ok(rows)
}

/// PAX 列式块解码：按列偏移量表重组每行 JSON 对象（保序，与写入字节一致）。
fn decode_pax_block(data: &[u8]) -> Result<Vec<DecodedRow>> {
    let mut cur = 1usize; // 跳过 kind
    if cur + 4 > data.len() {
        return Err(Error::Corrupted("PAX 块行数越界".into()));
    }
    let row_count = u32::from_le_bytes(data[cur..cur + 4].try_into().unwrap()) as usize;
    cur += 4;

    // Keys
    let mut keys = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        keys.push(decode_varlen(data, &mut cur)?.to_vec());
    }
    // 列偏移量表
    if cur + 2 > data.len() {
        return Err(Error::Corrupted("PAX 列计数越界".into()));
    }
    let col_count = u16::from_le_bytes(data[cur..cur + 2].try_into().unwrap()) as usize;
    cur += 2;
    let mut col_meta: Vec<(String, usize, usize)> = Vec::with_capacity(col_count); // (field, offset, len)
    for _ in 0..col_count {
        let field = String::from_utf8(decode_varlen(data, &mut cur)?.to_vec())
            .map_err(|_| Error::Corrupted("PAX 列名非法 UTF-8".into()))?;
        let _is_hot = data[cur];
        cur += 1;
        if cur + 8 > data.len() {
            return Err(Error::Corrupted("PAX 列表越界".into()));
        }
        let offset = u32::from_le_bytes(data[cur..cur + 4].try_into().unwrap()) as usize;
        let len = u32::from_le_bytes(data[cur + 4..cur + 8].try_into().unwrap()) as usize;
        cur += 8;
        col_meta.push((field, offset, len));
    }

    // 解析列数据为列主序：每列一行条目 `Present(u8)+ValLen(VarLen)+Val`，行序与 Keys 一致
    let mut col_values: Vec<Vec<(bool, Option<serde_json::Value>)>> = Vec::with_capacity(col_count);
    for (_, offset, len) in &col_meta {
        let col_data = &data[*offset..*offset + *len];
        let mut vals = Vec::with_capacity(row_count);
        let mut ccur = 0usize;
        for _ in 0..row_count {
            if ccur >= col_data.len() {
                return Err(Error::Corrupted("PAX 列数据越界".into()));
            }
            let present = col_data[ccur];
            ccur += 1;
            if present == 1 {
                let vlen = decode_varint(col_data, &mut ccur)? as usize;
                if ccur + vlen > col_data.len() {
                    return Err(Error::Corrupted("PAX 列值越界".into()));
                }
                let vbytes = &col_data[ccur..ccur + vlen];
                ccur += vlen;
                if vbytes == b"n" {
                    vals.push((true, None)); // null
                } else {
                    let v = serde_json::from_slice(vbytes)
                        .map_err(|e| Error::Corrupted(format!("PAX 列值解析失败: {e}")))?;
                    vals.push((true, Some(v)));
                }
            } else {
                vals.push((false, None)); // 缺失
            }
        }
        col_values.push(vals);
    }

    // Seqs（块尾：row_count × u64，紧邻列数据区之后）
    if data.len() < row_count * 8 {
        return Err(Error::Corrupted("PAX seq 区越界".into()));
    }
    let seqs_start = data.len() - row_count * 8;
    let mut rows = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let mut map = serde_json::Map::new();
        for (ci, (field, _, _)) in col_meta.iter().enumerate() {
            let (present, v) = &col_values[ci][i];
            if *present {
                match v {
                    Some(v) => {
                        map.insert(field.clone(), v.clone());
                    }
                    None => {
                        map.insert(field.clone(), serde_json::Value::Null);
                    }
                }
            }
        }
        let value = serde_json::to_vec(&map)
            .map_err(|e| Error::Corrupted(format!("PAX 值重组失败: {e}")))?;
        let seq = u64::from_le_bytes(
            data[seqs_start + i * 8..seqs_start + (i + 1) * 8]
                .try_into()
                .unwrap(),
        );
        rows.push((keys[i].clone(), Some(value), seq));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> std::path::PathBuf {
        static DIR: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let name = format!(
            "sst-{}.sst",
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    fn write_sample(path: &Path, n: u64) -> SstFooter {
        let mut w = SstWriter::new(path, Compression::Zstd, 3, 4096, n as usize).unwrap();
        for i in 0..n {
            let k = format!("user-{i:08}").into_bytes();
            let v = format!("value-of-{i}").into_bytes();
            w.add(&k, &v, i).unwrap();
        }
        w.finish().unwrap()
    }

    #[test]
    fn write_read_roundtrip() {
        let path = tmp();
        let footer = write_sample(&path, 100);
        assert_eq!(footer.key_count, 100);
        let mut r = SstReader::open(&path).unwrap();
        assert_eq!(r.footer().key_count, 100);
        assert!(r.index_len() >= 1);
        for i in (0..100u64).step_by(7) {
            let k = format!("user-{i:08}").into_bytes();
            let (v, seq) = r.get(&k).unwrap().unwrap();
            assert_eq!(v.unwrap(), format!("value-of-{i}").into_bytes());
            assert_eq!(seq, i);
        }
    }

    #[test]
    fn bloom_prunes_absent_keys() {
        let path = tmp();
        write_sample(&path, 50);
        let mut r = SstReader::open(&path).unwrap();
        assert!(r.get(b"nope-not-here").unwrap().is_none());
    }

    #[test]
    fn non_strict_order_rejected() {
        let path = tmp();
        let mut w = SstWriter::new(&path, Compression::None, 0, 4096, 10).unwrap();
        w.add(b"b", b"1", 1).unwrap();
        assert!(w.add(b"a", b"2", 2).is_err());
    }

    #[test]
    fn iterate_visits_all_sorted() {
        let path = tmp();
        write_sample(&path, 1000);
        let mut r = SstReader::open(&path).unwrap();
        let mut last: Option<Vec<u8>> = None;
        let mut count = 0u64;
        r.iterate(|k, _v, _seq| {
            if let Some(l) = &last {
                assert!(k > l.as_slice());
            }
            last = Some(k.to_vec());
            count += 1;
        })
        .unwrap();
        assert_eq!(count, 1000);
    }

    #[test]
    fn multi_block_file_queries_across_blocks() {
        // 小块大小强制多块，验证跨块二分定位
        let path = tmp();
        let mut w = SstWriter::new(&path, Compression::Zstd, 1, 64, 200).unwrap();
        for i in 0..200u64 {
            let k = format!("k{i:04}").into_bytes();
            let v = format!("v{i:04}").into_bytes();
            w.add(&k, &v, i).unwrap();
        }
        w.finish().unwrap();
        let mut r = SstReader::open(&path).unwrap();
        assert!(r.index_len() > 1, "应产生多块，实际 {}", r.index_len());
        // 首块、中间块、末块的 key 都要能查到
        for i in [0u64, 99, 199] {
            let k = format!("k{i:04}").into_bytes();
            assert!(r.get(&k).unwrap().is_some(), "key k{i:04} 未命中");
        }
    }

    #[test]
    fn corrupted_block_detected() {
        let path = tmp();
        write_sample(&path, 30);
        // 翻转第一个数据块首字节（Header 15 字节之后），破坏 CRC
        let mut data = std::fs::read(&path).unwrap();
        data[15] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();
        let mut r = SstReader::open(&path).unwrap();
        // 块 CRC 校验失败 → Corrupted 错误（而非 panic）
        assert!(r.get(b"user-00000001").is_err());
    }

    #[test]
    fn pax_block_roundtrip_preserves_json_and_zones() {
        // PAX 列式块：JSON 按字段拆列，重组后语义等值（保序），字段级 Zone Map 采集
        let path = tmp();
        let hot = vec!["status".to_string(), "city".to_string()];
        let mut w = SstWriter::new_with_pax(&path, Compression::None, 0, 1024, 10, &hot).unwrap();
        w.add(
            b"k1",
            br#"{"status":"active","city":"beijing","amount":100}"#,
            1,
        )
        .unwrap();
        w.add(b"k2", br#"{"status":"inactive","city":"shanghai"}"#, 2)
            .unwrap();
        w.add(b"k3", br#"{"status":"active","city":null,"extra":1}"#, 3)
            .unwrap();
        w.finish().unwrap();

        let mut r = SstReader::open(&path).unwrap();
        // 重组保真（字段序 = 原序，紧凑 JSON 与写入字节一致）
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"k1").unwrap().unwrap().0.unwrap()),
            r#"{"status":"active","city":"beijing","amount":100}"#
        );
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"k2").unwrap().unwrap().0.unwrap()),
            r#"{"status":"inactive","city":"shanghai"}"#
        );
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"k3").unwrap().unwrap().0.unwrap()),
            r#"{"status":"active","city":null,"extra":1}"#
        );
        // 字段级 Zone Map（present / null / min / max）
        let zones = &r.index()[0].zones;
        assert!(!zones.is_empty(), "PAX 块应有字段级 Zone Map");
        let st = zones.iter().find(|z| z.field == "status").unwrap();
        assert_eq!(st.present_count, 3);
        assert_eq!(st.null_count, 0);
        let city = zones.iter().find(|z| z.field == "city").unwrap();
        assert_eq!(city.present_count, 3);
        assert_eq!(city.null_count, 1);
        assert_eq!(String::from_utf8_lossy(&city.min), "\"beijing\"");
        assert_eq!(String::from_utf8_lossy(&city.max), "\"shanghai\"");
    }

    #[test]
    fn pax_falls_back_to_row_for_non_json_or_tombstone() {
        // 块内出现非 JSON 值或 Tombstone → 整块回退行式，读取不受影响
        let path = tmp();
        let hot = vec!["a".to_string()];
        let mut w = SstWriter::new_with_pax(&path, Compression::None, 0, 1024, 10, &hot).unwrap();
        w.add(b"k1", br#"{"a":1,"b":2}"#, 1).unwrap();
        w.add(b"k2", b"not-json-bytes", 2).unwrap();
        w.add_tombstone(b"k3", 3).unwrap();
        w.finish().unwrap();

        let mut r = SstReader::open(&path).unwrap();
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"k1").unwrap().unwrap().0.unwrap()),
            r#"{"a":1,"b":2}"#
        );
        assert_eq!(
            r.get(b"k2").unwrap().unwrap().0.as_deref(),
            Some(&b"not-json-bytes"[..])
        );
        assert_eq!(
            r.get(b"k3").unwrap().unwrap().0,
            None,
            "Tombstone 读回应为 None"
        );
        // 非 PAX → 无字段级 Zone Map
        assert!(r.index()[0].zones.is_empty());
    }

    #[test]
    fn pax_mixed_block_kinds_in_one_file() {
        // 同一 v4 文件内 PAX 块与行式块共存：块 kind 分发正确
        let path = tmp();
        let hot = vec!["a".to_string()];
        let mut w = SstWriter::new_with_pax(&path, Compression::None, 0, 30, 10, &hot).unwrap();
        w.add(b"k1", br#"{"a":1,"b":2}"#, 1).unwrap(); // 估算 ~35B ≥ 30 → 单独 flush → PAX 块
        w.add(b"k2", b"not-json", 2).unwrap(); // 行式块
        w.add(b"k3", br#"{"a":3}"#, 3).unwrap();
        w.finish().unwrap();

        let mut r = SstReader::open(&path).unwrap();
        assert!(r.index_len() >= 2, "应产生多块，实际 {}", r.index_len());
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"k1").unwrap().unwrap().0.unwrap()),
            r#"{"a":1,"b":2}"#
        );
        assert_eq!(
            r.get(b"k2").unwrap().unwrap().0.as_deref(),
            Some(&b"not-json"[..])
        );
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"k3").unwrap().unwrap().0.unwrap()),
            r#"{"a":3}"#
        );
        // 至少一个 PAX 块（带字段级 Zone Map）
        assert!(r.index().iter().any(|e| !e.zones.is_empty()));
    }

    #[test]
    fn decode_data_block_v3_format_without_kind() {
        // 向后兼容：v3 行式块无 kind 字节，decode_data_block(_, 3) 直接解析
        let mut row_data = Vec::new();
        crate::keys::encode_varlen(&mut row_data, b"x");
        crate::keys::encode_varlen(&mut row_data, b"y");
        row_data.push(FLAG_PUT);
        row_data.extend_from_slice(&1u64.to_le_bytes());
        crate::keys::encode_varlen(&mut row_data, b"z");
        crate::keys::encode_varlen(&mut row_data, b"");
        row_data.push(FLAG_DELETE);
        row_data.extend_from_slice(&2u64.to_le_bytes());
        let rows = decode_data_block(&row_data, SST_VERSION_ROW).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, b"x");
        assert_eq!(rows[0].1.as_deref(), Some(&b"y"[..]));
        assert_eq!(rows[0].2, 1);
        assert_eq!(rows[1].0, b"z");
        assert_eq!(rows[1].1, None, "Tombstone");
        assert_eq!(rows[1].2, 2);
    }

    #[test]
    fn scan_range_returns_bounded_keys() {
        let path = tmp();
        write_sample(&path, 200);
        let mut r = SstReader::open(&path).unwrap();
        let start = b"user-00000050";
        let end = b"user-00000060";
        let mut keys = Vec::new();
        r.scan_range(Some(start), Some(end), |k, _v, _seq| keys.push(k.to_vec()))
            .unwrap();
        // 闭区间：50..=60 共 11 个
        assert_eq!(keys.len(), 11);
        assert_eq!(keys.first().unwrap().as_slice(), start);
        assert_eq!(keys.last().unwrap().as_slice(), end);
        // 升序
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn scan_range_without_bounds_visits_all() {
        let path = tmp();
        write_sample(&path, 100);
        let mut r = SstReader::open(&path).unwrap();
        let mut count = 0u64;
        r.scan_range(None, None, |_, _, _| count += 1).unwrap();
        assert_eq!(count, 100);
    }

    #[test]
    fn zone_map_prunes_out_of_range_blocks() {
        // 多块文件：查询一个小范围应只读命中块，不读全部块
        let path = tmp();
        let mut w = SstWriter::new(&path, Compression::Zstd, 1, 64, 500).unwrap();
        for i in 0..500u64 {
            let k = format!("k{i:04}").into_bytes();
            w.add(&k, b"v", i).unwrap();
        }
        w.finish().unwrap();
        let mut r = SstReader::open(&path).unwrap();
        assert!(r.index_len() > 2);
        // Zone Map 元数据正确：每块 min <= max
        for e in r.index() {
            assert!(e.first_key <= e.max_key);
        }
        // 精确小区间（key 格式 k{i:04}，查询须同格式）
        let mut hits = Vec::new();
        r.scan_range(Some(b"k0050"), Some(b"k0060"), |k, _, _| {
            hits.push(k.to_vec())
        })
        .unwrap();
        assert_eq!(hits.len(), 11);
    }

    #[test]
    fn tombstone_roundtrip() {
        let path = tmp();
        let mut w = SstWriter::new(&path, Compression::Zstd, 1, 4096, 10).unwrap();
        w.add(b"a", b"va", 1).unwrap();
        w.add_tombstone(b"b", 2).unwrap();
        w.add(b"c", b"vc", 3).unwrap();
        w.finish().unwrap();

        let mut r = SstReader::open(&path).unwrap();
        let (va, _) = r.get(b"a").unwrap().unwrap();
        assert_eq!(va.unwrap(), b"va");
        let (vb, seq_b) = r.get(b"b").unwrap().unwrap();
        assert!(vb.is_none(), "b 应为 Tombstone");
        assert_eq!(seq_b, 2);
        let (vc, _) = r.get(b"c").unwrap().unwrap();
        assert_eq!(vc.unwrap(), b"vc");
        assert!(r.get(b"zzz").unwrap().is_none());

        // 范围扫描同样携带删除语义
        let mut found = Vec::new();
        r.scan_range(None, None, |k, v, _| found.push((k.to_vec(), v.is_some())))
            .unwrap();
        assert_eq!(
            found,
            vec![
                (b"a".to_vec(), true),
                (b"b".to_vec(), false),
                (b"c".to_vec(), true),
            ]
        );
    }
}
