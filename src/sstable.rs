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
use std::sync::Mutex;

/// 位置读（`&File` 可并发，读写分离读路径基础）：Windows `seek_read` / Unix `read_at`，
/// 不移动文件游标——多线程可同时对同一 SST 的不同块做读取。
#[cfg(windows)]
fn read_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> crate::error::Result<()> {
    use std::os::windows::fs::FileExt;
    file.seek_read(buf, offset).map_err(crate::error::Error::Io)?;
    Ok(())
}

#[cfg(not(windows))]
fn read_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> crate::error::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, offset).map_err(crate::error::Error::Io)?;
    Ok(())
}

use crate::bloom::BloomFilter;
use crate::error::{Error, Result};
use crate::keys::{decode_varint, decode_varlen, encode_varint, encode_varlen};
use crate::per_cpu::PerCpuCounter;

/// 文件魔数 + 版本。
pub const SST_MAGIC: &[u8; 8] = b"NVSSTL01";
/// v4：数据块引入 PAX 列式布局（块首 kind 字节）；仍可读取 v3（行式）。
/// 当前格式版本：v5 = 分区布隆（Partitioned Bloom，design 4.4.2）。
pub const SST_VERSION: u16 = 5;
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

/// SSTable Writer：按 key 升序写入，自动切块、压缩、维护稀疏索引与分区布隆（v5）。
pub struct SstWriter {
    out: std::fs::File,
    compression: Compression,
    block_size: usize,
    /// 当前数据块缓冲（行式攒批，flush 时编码）。
    buf: Vec<PendingRow>,
    /// 块索引：[first_key, offset, raw_len, comp_len, zones]。
    index: Vec<IndexEntry>,
    /// 分区布隆（Partitioned Bloom，design 4.4.2）：每数据块一个，flush 时构建。
    partition_blooms: Vec<Vec<u8>>,
    /// 布隆假阳性率（`sstable.bloom_fpr`）。
    bloom_fpr: f64,
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
            0.01,
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
        bloom_fpr: f64,
    ) -> Result<Self> {
        let _ = expected_keys; // 分区布隆按块内实际 key 数构建，无需全文件预估
        let out = std::fs::File::create(path).map_err(Error::Io)?;
        let mut w = Self {
            out,
            compression,
            block_size,
            buf: Vec::new(),
            index: Vec::new(),
            partition_blooms: Vec::new(),
            bloom_fpr,
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

    /// Ex-5.8 元数据-数据解耦：追加一个**已编码的完整数据块**（行式 kind=0）——原样写入
    /// 压缩字节，重建 trailer/索引/分区布隆。用于块级复用 Compaction：无重叠 L0 段合并时
    /// 数据块**零解压零重压缩**直接复用，只重建 Block Index/Bloom/Footer 元数据区
    /// （Compaction 读放大归零、压缩 CPU 免除；demo 实测全量重写 4041ms vs 块级复用毫秒级）。
    /// PAX 块（kind=1）不支持（字段级 Zone Map 无法重建）→ 返回 Unsupported，调用方回退全量合并。
    pub fn add_raw_block(&mut self, raw: &[u8], compressed: &[u8]) -> Result<()> {
        if raw.first() != Some(&BLOCK_KIND_ROW) {
            return Err(Error::Unsupported(
                "块级复用仅支持行式数据块（PAX 需全量合并重建 zones）".into(),
            ));
        }
        let rows = decode_data_block(raw, SST_VERSION)?;
        let first_key = rows
            .first()
            .map(|r| r.0.clone())
            .ok_or_else(|| Error::Corrupted("复用数据块为空".into()))?;
        let max_key = rows
            .last()
            .map(|r| r.0.clone())
            .ok_or_else(|| Error::Corrupted("复用数据块为空".into()))?;
        if let Some(last) = &self.last_key {
            if first_key.as_slice() <= last.as_slice() {
                return Err(Error::Corrupted(format!(
                    "块级复用 key 必须严格升序: {:?} <= {:?}",
                    first_key, last
                )));
            }
        }
        // 分区布隆（design 4.4.2）：按块内 key 重建（与 flush_block 一致）
        let mut b = BloomFilter::with_estimated_keys_fpr(rows.len().max(1), self.bloom_fpr);
        for r in &rows {
            b.insert(&r.0);
        }
        self.partition_blooms.push(b.to_bytes());

        let offset = self.written;
        let raw_len = raw.len();
        let comp_len = compressed.len();
        self.write_all(compressed)?;
        let mut trailer = Vec::with_capacity(TRAILER_LEN);
        trailer.extend_from_slice(&(raw_len as u32).to_le_bytes());
        trailer.extend_from_slice(&(comp_len as u32).to_le_bytes());
        trailer.extend_from_slice(&crc32(compressed).to_le_bytes());
        self.write_all(&trailer)?;
        self.index.push(IndexEntry {
            first_key,
            max_key: max_key.clone(),
            offset,
            raw_len: raw_len as u32,
            comp_len: comp_len as u32,
            zones: Vec::new(),
        });
        self.key_count += rows.len() as u64;
        self.last_key = Some(max_key);
        Ok(())
    }

    fn add_inner(&mut self, key: &[u8], value: Option<&[u8]>, flag: u8, seq: u64) -> Result<()> {
        if let Some(last) = &self.last_key {
            // S 项：允许相等 key（同 key 多版本，seq 升序）；仅拒绝严格逆序
            if key < last.as_slice() {
                return Err(Error::Corrupted(format!(
                    "SST 写入 key 逆序: {:?} < {:?}",
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
        self.key_count += 1;
        self.last_key = Some(key.to_vec());
        // S 项：同 key 多版本**不跨块**——仅当换 key 且块达阈值时刷块。
        // 否则版本被拆到相邻两块，locate_indexed_block 二分（取首个 first_key<=key 的
        // 最后一块）会漏读前一块中的旧版本。
        // 注意：必须先更新 buf_last_key 再刷块（flush_block 以它作块 max_key）。
        let new_key = self
            .buf_last_key
            .as_ref()
            .map_or(true, |l| l != key);
        self.buf_last_key = Some(key.to_vec());
        if new_key && self.estimate_block_bytes() >= self.block_size {
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

        // 分区布隆（design 4.4.2）：按当前块实际 key 数构建，查询只加载目标块布隆
        let mut b = BloomFilter::with_estimated_keys_fpr(self.buf.len().max(1), self.bloom_fpr);
        for r in &self.buf {
            b.insert(&r.key);
        }
        self.partition_blooms.push(b.to_bytes());

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

        // 分区布隆区（v5）：Count(u32) + [len(u32) + bytes]*，按块顺序与 Index 对齐
        let bloom_offset = self.written;
        let mut bb = Vec::new();
        bb.extend_from_slice(&(self.partition_blooms.len() as u32).to_le_bytes());
        for b in &self.partition_blooms {
            bb.extend_from_slice(&(b.len() as u32).to_le_bytes());
            bb.extend_from_slice(b);
        }
        self.write_all(&bb)?;
        let bloom_len = bb.len() as u32;

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
    compression: Compression,
    /// 文件格式版本（v3=纯行式，v4=PAX，v5=分区布隆）。
    format: u16,
    /// Ex-5.9/Ex-7.1：读热度计数（点查/范围扫描命中递增，冷热 Compaction 选段依据；
    /// PerCpuCounter 按核拆分——并发读多核 touch 无伪共享）。
    heat: PerCpuCounter,
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
        Self::open_inner(path, granularity)
    }

    fn open_inner(path: &Path, index_granularity: usize) -> Result<Self> {
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
            // R 项：段 [min, max] 从解码索引首尾取（索引按 key 升序；空段无约束）
            min_key: index.first().map(|e| e.first_key.clone()).unwrap_or_default(),
            max_key: index.last().map(|e| e.max_key.clone()).unwrap_or_default(),
            compression,
            format: version,
            heat: PerCpuCounter::new(),
        })
    }

    pub fn footer(&self) -> &SstFooter {
        &self.footer
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
    fn block_entry(&self, idx: usize) -> Result<IndexEntry> {
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
    /// 触发 Level 2 懒加载。
    fn locate_block_index(&self, key: &[u8]) -> Result<Option<usize>> {
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
        read_at(&self.file, &mut comp, e.offset)?;

        // 读 Trailer 校验
        let mut trailer = vec![0u8; TRAILER_LEN];
        read_at(&self.file, &mut trailer, e.offset + e.comp_len as u64)?;
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
        read_at(&self.file, &mut buf, start)?;
        let mut out = Vec::with_capacity(n);
        for e in entries {
            let rel = (e.offset - start) as usize;
            let comp = &buf[rel..rel + e.comp_len as usize];
            let tr = &buf[rel + e.comp_len as usize..rel + e.comp_len as usize + TRAILER_LEN];
            let raw_len = u32::from_le_bytes(tr[0..4].try_into().unwrap()) as usize;
            let comp_len = u32::from_le_bytes(tr[4..8].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(tr[8..12].try_into().unwrap());
            if comp_len != e.comp_len as usize || crc32(comp) != crc {
                // 布局假设失效（防御）：回退逐块读
                return entries.iter().map(|en| self.read_block(en)).collect();
            }
            out.push(self.decompress(comp, raw_len)?);
        }
        Ok(out)
    }

    /// Ex-5.8 元数据-数据解耦：读取块的**原始压缩字节** + 解码内容，供块级复用 Compaction
    /// 原样拷贝数据块（不解压校验 trailer，由复用写入方 `add_raw_block` 重建）。
    /// O 项第③步：`&self`——compact 经 `Arc<SstReader>` 并发读输入段（内部 `read_at` 无状态 seek）。
    pub fn block_raw(&self, e: &IndexEntry) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut comp = vec![0u8; e.comp_len as usize];
        read_at(&self.file, &mut comp, e.offset)?;
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

/// SST 范围扫描**流式迭代器**（M8-P10 scan 流式化）：块级惰性读取 + Zone Map 剪枝，
/// 逐条 yield `(key, value, seq)`（value=None = Tombstone）。与 `scan_range` 语义一致，
/// 但可暂停/推进（k-way merge 多源归并用），内存 O(块) 而非 O(全量)。
pub struct SstRangeIter<'a> {
    reader: &'a SstReader,
    /// 当前候选块下标（自起始块起，不再持有全索引副本——M 项 P0 修复：
    /// 原 `reader.index()` 每次克隆整个块索引（78 个 SST × 全量深拷贝）致小范围扫描初始化成本秒级）。
    block_idx: usize,
    rows: Vec<DecodedRow>,
    row_idx: usize,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    /// U 项：块预读缓存（块下标 → 解码行）——`advance_block` 一次组读 ≤4 块
    /// （合并 read_at + 预解码），后续块直接消费（冷顺序扫描 IO 放大 4×4KB → 1×16KB）。
    prefetch: std::collections::VecDeque<(usize, Vec<DecodedRow>)>,
}

impl<'a> SstRangeIter<'a> {
    /// O 项第①步：`&SstReader`（读路径共享，配合 RwLock 读读并行）。
    pub fn new(
        reader: &'a SstReader,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Self> {
        // 二分定位起始块（start 无值从 0 开始；Level 2 索引懒加载后缓存，后续零 IO）
        let start_idx = match start {
            Some(s) => reader.locate_block_index(s)?.unwrap_or(0),
            None => 0,
        };
        Ok(Self {
            reader,
            block_idx: start_idx,
            rows: Vec::new(),
            row_idx: 0,
            start: start.map(|s| s.to_vec()),
            end: end.map(|e| e.to_vec()),
            prefetch: std::collections::VecDeque::new(),
        })
    }

    /// 推进到下一个候选块（Zone Map 剪枝），加载并解码；无更多块返回 false。
    /// U 项：一次组读当前块起 ≤4 块（合并 read_at + 预解码进 prefetch 缓存）。
    fn advance_block(&mut self) -> Result<bool> {
        // 优先消费预读缓存
        if let Some((_, rows)) = self.prefetch.pop_front() {
            self.rows = rows;
            self.row_idx = 0;
            return Ok(true);
        }
        // 组读：当前块起 ≤4 块（Zone Map 剪枝）
        let mut picks: Vec<(usize, IndexEntry)> = Vec::new();
        let mut bidx = self.block_idx;
        while bidx < self.reader.index_len() && picks.len() < 4 {
            let e = self.reader.block_entry(bidx)?;
            if let Some(s) = &self.start {
                if e.max_key.as_slice() < s.as_slice() {
                    bidx += 1;
                    continue;
                }
            }
            if let Some(en) = &self.end {
                if e.first_key.as_slice() > en.as_slice() {
                    break; // 索引按 key 有序，后续块更大
                }
            }
            picks.push((bidx, e));
            bidx += 1;
        }
        if picks.is_empty() {
            return Ok(false);
        }
        self.block_idx = picks.last().map(|(i, _)| *i + 1).unwrap_or(self.block_idx);
        let entries: Vec<IndexEntry> = picks.iter().map(|(_, e)| e.clone()).collect();
        let blocks = self.reader.read_block_group(&entries)?;
        for ((i, _), block) in picks.into_iter().zip(blocks) {
            self.prefetch
                .push_back((i, decode_data_block(&block, self.reader.format)?));
        }
        if let Some((_, rows)) = self.prefetch.pop_front() {
            self.rows = rows;
            self.row_idx = 0;
            return Ok(true);
        }
        Ok(false)
    }
}

impl<'a> Iterator for SstRangeIter<'a> {
    type Item = Result<(Vec<u8>, Option<Vec<u8>>, u64)>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.row_idx >= self.rows.len() {
                match self.advance_block() {
                    Ok(true) => continue,
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }
            let (k, v, seq) = &self.rows[self.row_idx];
            self.row_idx += 1;
            if let Some(s) = &self.start {
                if k.as_slice() < s.as_slice() {
                    continue;
                }
            }
            if let Some(en) = &self.end {
                if k.as_slice() > en.as_slice() {
                    return None; // 升序，后续更大
                }
            }
            return Some(Ok((k.clone(), v.clone(), *seq)));
        }
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
    fn sst_range_iter_matches_scan_range() {
        // M8-P10 流式迭代器：与 scan_range 输出完全一致（全量 + 范围过滤 + 升序）
        let path = tmp();
        write_sample(&path, 100);
        let mut r = SstReader::open(&path).unwrap();
        // 全量
        let mut it_all: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        for row in SstRangeIter::new(&mut r, None, None).unwrap() {
            it_all.push(row.unwrap());
        }
        let mut sc_all: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        r.scan_range(None, None, |k, v, seq| {
            sc_all.push((k.to_vec(), v.map(|x| x.to_vec()), seq))
        })
        .unwrap();
        assert_eq!(it_all, sc_all, "迭代器应与 scan_range 全量一致");
        // 范围过滤（含 Zone Map 剪枝路径）
        let lo = b"user-00000030".as_slice();
        let hi = b"user-00000040".as_slice();
        let mut it_rng: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        for row in SstRangeIter::new(&mut r, Some(lo), Some(hi)).unwrap() {
            it_rng.push(row.unwrap());
        }
        let mut sc_rng: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        r.scan_range(Some(lo), Some(hi), |k, v, seq| {
            sc_rng.push((k.to_vec(), v.map(|x| x.to_vec()), seq))
        })
        .unwrap();
        assert_eq!(it_rng, sc_rng, "范围过滤迭代器应与 scan_range 一致");
        // 升序
        for w in it_all.windows(2) {
            assert!(w[0].0 < w[1].0, "迭代必须按 key 升序");
        }
    }

    #[test]
    fn range_iter_prefetch_multi_block_consistency() {
        // U 项：组读预读（≤4 块合并 read_at）——多块段扫描与单块逐读结果一致
        // （覆盖 read_block_group 合并读取 + 预解码缓存路径）
        let path = tmp();
        write_sample(&path, 2000); // ~10 块（4096 块/20B key）→ 覆盖组读
        let r = SstReader::open(&path).unwrap();
        assert!(r.index_len() > 4, "样本应 >4 块（实际 {}）", r.index_len());
        // 全量：组读路径
        let mut it_all: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        for row in SstRangeIter::new(&r, None, None).unwrap() {
            it_all.push(row.unwrap());
        }
        assert_eq!(it_all.len(), 2000, "全量行数");
        for (i, (k, v, seq)) in it_all.iter().enumerate() {
            assert_eq!(k, &format!("user-{i:08}").into_bytes(), "key {i}");
            assert_eq!(v.as_deref(), Some(format!("value-of-{i}").as_bytes()), "val {i}");
            assert_eq!(*seq, i as u64, "seq {i}");
        }
        // 跨块边界范围扫描（Zone Map 剪枝 + 预读）
        let lo = b"user-00000900".to_vec();
        let hi = b"user-00001100".to_vec();
        let mut it_rng: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        for row in SstRangeIter::new(&r, Some(&lo), Some(&hi)).unwrap() {
            it_rng.push(row.unwrap());
        }
        assert_eq!(it_rng.len(), 201, "闭区间 [900,1100] 行数");
        assert_eq!(it_rng[0].0, lo);
        assert_eq!(it_rng[200].0, hi);
        // 与 scan_range 对照（逐块路径）
        let mut sc: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        r.scan_range(Some(&lo), Some(&hi), |k, v, seq| {
            sc.push((k.to_vec(), v.map(|x| x.to_vec()), seq))
        })
        .unwrap();
        assert_eq!(it_rng, sc, "预读迭代器应与逐块 scan_range 一致");
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
        let mut w =
            SstWriter::new_with_pax(&path, Compression::None, 0, 1024, 10, &hot, 0.01).unwrap();
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
        let mut w =
            SstWriter::new_with_pax(&path, Compression::None, 0, 1024, 10, &hot, 0.01).unwrap();
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
        let mut w =
            SstWriter::new_with_pax(&path, Compression::None, 0, 30, 10, &hot, 0.01).unwrap();
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
    fn raw_block_reuse_roundtrip() {
        // Ex-5.8 元数据-数据解耦：add_raw_block 块级复用——源块原样拷贝 + 重建
        // trailer/索引/分区布隆，读回键完整、布隆剪枝生效。
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.sst");
        {
            let mut w = SstWriter::new(&src, Compression::None, 0, 4096, 1000).unwrap();
            for i in 0..1000u64 {
                w.add(&i.to_be_bytes(), &[i as u8; 32], i).unwrap();
            }
            w.finish().unwrap();
        }
        // 块级复用重建（数据块原样，零解压重压缩）
        let dst = dir.path().join("reuse.sst");
        {
            let mut r = SstReader::open(&src).unwrap();
            let mut w = SstWriter::new(&dst, Compression::None, 0, 4096, 0).unwrap();
            let entries = r.index();
            assert!(entries.len() > 4, "应产生多个数据块");
            for e in entries {
                let (comp, raw) = r.block_raw(&e).unwrap();
                w.add_raw_block(&raw, &comp).unwrap();
            }
            w.finish().unwrap();
        }
        // 读回全部键一致
        let mut r2 = SstReader::open(&dst).unwrap();
        let mut n = 0u64;
        r2.iterate(|k, v, _seq| {
            let key = u64::from_be_bytes(k.try_into().unwrap());
            assert_eq!(key, n, "键序一致");
            assert_eq!(v.unwrap(), &[key as u8; 32]);
            n += 1;
        })
        .unwrap();
        assert_eq!(n, 1000);
        // 布隆剪枝生效（缺失键不被命中）
        assert!(r2.get(&9999u64.to_be_bytes()).unwrap().is_none());
        // 分区布隆已重建（数量与索引一致）
        assert!(r2.partition_blooms().is_some());
        assert_eq!(
            r2.partition_blooms().unwrap().len(),
            r2.index().len(),
            "分区布隆按块重建"
        );
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

    #[test]
    fn partitioned_bloom_built_per_block() {
        // v5：每个数据块一个分区布隆，与 Index 对齐
        let path = tmp();
        let mut w = SstWriter::new(&path, Compression::Zstd, 1, 64, 500).unwrap();
        for i in 0..500u64 {
            let k = format!("k{i:04}").into_bytes();
            w.add(&k, b"v", i).unwrap();
        }
        w.finish().unwrap();
        let mut r = SstReader::open(&path).unwrap();
        let pb = r.partition_blooms().expect("v5 应有分区布隆");
        assert!(r.index_len() > 2, "多块文件");
        assert_eq!(pb.len(), r.index_len(), "分区布隆数 = 块数");
        assert!(r.legacy_bloom().is_none(), "v5 无整文件布隆");
        // 查询正确（分区布隆剪枝 + 读块）
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"k0250").unwrap().unwrap().0.unwrap()),
            "v"
        );
        // 缺失 key：定位块后由分区布隆剪枝返回 None
        assert!(r.get(b"k9999").unwrap().is_none());
    }

    #[test]
    fn partitioned_bloom_fpr_configurable() {
        // 不同 fpr 的位数组大小不同（fpr 越小位数越多）
        let strict = BloomFilter::with_estimated_keys_fpr(100, 0.001);
        let loose = BloomFilter::with_estimated_keys_fpr(100, 0.05);
        assert!(
            strict.num_bits() > loose.num_bits(),
            "fpr=0.001 应比 fpr=0.05 更多位"
        );
    }

    #[test]
    fn v4_legacy_bloom_still_readable() {
        // 向后兼容：手写 v4 格式（整文件布隆）读取路径
        let dir = tempfile::tempdir().unwrap();
        // 构造 v4 文件：header(15) + 一个行式块 + index + 旧单布隆 + footer
        let path = dir.path().join("v4.sst");
        let mut out = Vec::new();
        out.extend_from_slice(SST_MAGIC);
        out.extend_from_slice(&4u16.to_le_bytes());
        out.push(Compression::Zstd.code());
        out.extend_from_slice(&4096u32.to_le_bytes());
        // 数据块（行式）：key=ab, value=xy
        let mut block = Vec::new();
        crate::keys::encode_varlen(&mut block, b"ab");
        crate::keys::encode_varlen(&mut block, b"xy");
        block.push(FLAG_PUT);
        block.extend_from_slice(&1u64.to_le_bytes());
        let raw = block;
        let comp = zstd::bulk::compress(&raw, 3).unwrap();
        let block_offset = out.len() as u64;
        out.extend_from_slice(&comp);
        out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        out.extend_from_slice(&(comp.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32(&comp).to_le_bytes());
        // 索引
        let index_offset = out.len() as u64;
        let mut ib = Vec::new();
        crate::keys::encode_varint(&mut ib, 1);
        crate::keys::encode_varlen(&mut ib, b"ab");
        crate::keys::encode_varlen(&mut ib, b"ab");
        ib.extend_from_slice(&block_offset.to_le_bytes());
        ib.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        ib.extend_from_slice(&(comp.len() as u32).to_le_bytes());
        ib.extend_from_slice(&0u16.to_le_bytes()); // zones 空
        out.extend_from_slice(&ib);
        let index_len = ib.len() as u32;
        // 旧单布隆
        let bloom_offset = out.len() as u64;
        let mut bf = BloomFilter::with_estimated_keys(1);
        bf.insert(&b"ab".to_vec());
        let bbytes = bf.to_bytes();
        out.extend_from_slice(&(bbytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&bbytes);
        let bloom_len = (4 + bbytes.len()) as u32;
        // Footer
        let footer_offset = out.len() as u64;
        let mut fb = Vec::new();
        fb.extend_from_slice(SST_MAGIC);
        fb.extend_from_slice(&4u16.to_le_bytes());
        fb.extend_from_slice(&index_offset.to_le_bytes());
        fb.extend_from_slice(&index_len.to_le_bytes());
        fb.extend_from_slice(&bloom_offset.to_le_bytes());
        fb.extend_from_slice(&bloom_len.to_le_bytes());
        fb.extend_from_slice(&1u64.to_le_bytes());
        fb.extend_from_slice(&footer_offset.to_le_bytes());
        fb.extend_from_slice(&crc32(&fb).to_le_bytes());
        out.extend_from_slice(&fb);
        out.extend_from_slice(&footer_offset.to_le_bytes());
        std::fs::write(&path, &out).unwrap();

        let mut r = SstReader::open(&path).unwrap();
        assert!(r.partition_blooms().is_none(), "v4 无分区布隆");
        assert!(r.legacy_bloom().is_some(), "v4 应加载整文件布隆");
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"ab").unwrap().unwrap().0.unwrap()),
            "xy"
        );
        assert!(r.get(b"absent").unwrap().is_none(), "整文件布隆剪枝");
    }

    // ---- 两级索引（design 4.4.2，阶段 2）----

    #[test]
    fn two_level_index_summary_resident_exact_lazy() {
        // 小块文件：制造多个数据块
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tli.sst");
        {
            let mut w = SstWriter::new(&path, Compression::Zstd, 3, 64, 100).unwrap();
            for i in 0..200u64 {
                w.add(format!("key-{i:06}").as_bytes(), b"value", i)
                    .unwrap();
            }
            w.finish().unwrap();
        }
        // 粒度 4：每 4 块一条摘要
        let mut r = SstReader::open_with_granularity(&path, 4).unwrap();
        let blocks = r.index_len();
        assert!(blocks > 4, "应产生多个数据块: {blocks}");
        // Level 1 常驻：摘要条数 = ceil(blocks / 4)，远小于 blocks（内存减少 90%）
        let expected = blocks.div_ceil(4);
        assert_eq!(r.summary_len(), expected);
        assert_eq!(r.summary()[0].block_index, 0);
        // 摘要块下标等差为粒度
        for w in r.summary().windows(2) {
            assert_eq!(w[1].block_index - w[0].block_index, 4);
        }
        // Level 2 尚未懒加载（open 时只留摘要，内存减负）
        assert!(!r.level2_loaded(), "open 后不应加载精确索引");
        // 查询触发 Level 2 懒加载，且结果正确
        let v = r.get(b"key-000042").unwrap().unwrap().0.unwrap();
        assert_eq!(v, b"value");
        assert!(r.level2_loaded(), "首次访问应懒加载精确索引");
        // 懒加载后精确索引内容正确（块数一致）
        assert_eq!(r.index().len(), blocks);
    }

    #[test]
    fn two_level_index_query_across_all_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tli2.sst");
        {
            let mut w = SstWriter::new(&path, Compression::Zstd, 3, 32, 300).unwrap();
            for i in 0..500u64 {
                w.add(format!("k{i:06}").as_bytes(), format!("v{i}").as_bytes(), i)
                    .unwrap();
            }
            w.finish().unwrap();
        }
        let mut r = SstReader::open_with_granularity(&path, 8).unwrap();
        // 抽查若干 key（跨多块），全部命中
        for i in [0u64, 1, 127, 128, 250, 333, 499] {
            let key = format!("k{i:06}");
            let v = r.get(key.as_bytes()).unwrap().unwrap().0.unwrap();
            assert_eq!(String::from_utf8_lossy(&v), format!("v{i}"));
        }
        // 未命中
        assert!(r.get(b"k999999").unwrap().is_none());
        // 范围扫描仍完整
        let mut seen = 0;
        r.scan_range(None, None, |_k, _v, _seq| seen += 1).unwrap();
        assert_eq!(seen, 500);
    }

    #[test]
    fn two_level_index_single_block_has_one_summary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tli3.sst");
        {
            let mut w = SstWriter::new(&path, Compression::Zstd, 3, 4096, 3).unwrap();
            for i in 0..3u64 {
                w.add(format!("a{i}").as_bytes(), b"x", i).unwrap();
            }
            w.finish().unwrap();
        }
        let r = SstReader::open_with_granularity(&path, 16).unwrap();
        assert_eq!(r.index_len(), 1);
        assert_eq!(r.summary_len(), 1, "单块也应有一条摘要（含首块）");
        assert!(!r.level2_loaded());
    }
}
