//! SstWriter 写路径（原 mod.rs 拆分）：压缩算法、PAX/行式数据块编码、稀疏索引与
//! 分区布隆构建、Footer 写出。
//!
//! 本文件与同目录 `reader.rs`（SstFooter 定义）互相引用：`finish()` 产出 `SstFooter`。

use std::io::Write;
use std::path::Path;

use crate::bloom::BloomFilter;
use crate::error::{Error, Result};
use crate::keys::{encode_varint, encode_varlen};

use super::block::decode_data_block;
use super::reader::SstFooter;
use super::{
    crc32, BLOCK_KIND_PAX, BLOCK_KIND_ROW, FLAG_DELETE, FLAG_PUT, SST_MAGIC, SST_VERSION,
    TRAILER_LEN,
};

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

    /// 压缩算法码（写入 Header）；writer/reader 与模块内测试共用 → pub(crate)。
    pub(crate) fn code(&self) -> u8 {
        match self {
            Self::None => 0,
            Self::Zstd => 1,
            Self::Lz4 => 2,
            Self::Snappy => 3,
        }
    }

    /// 压缩算法码解析（Reader 打开时读 Header）；writer/reader 共用 → pub(crate)。
    pub(crate) fn from_code(c: u8) -> Result<Self> {
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

/// 写入缓冲中的待编码行（flush 时统一决定行式 / PAX 布局）。
struct PendingRow {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    flag: u8,
    seq: u64,
}

/// 字段级 Zone Map（阶段 1.5，design 4.4.1 强化）：块内单字段采样统计，供范围条件剪枝。
/// P3-B：v6 新增 `sum` 字段——数值列块内累加和（字节为 JSON 数值序列化），
/// 非数值列固定为 `0.0`；`sum` 聚合并可用时（present_count == null_count == 0 表示未计算）。
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
    /// P3-B：v6 数值列块内累加和（JSON 数值直接求和），非数值列或 v5 旧段为 0.0。
    /// 用于 SUM(amount)/AVG(amount) 等聚合下推：跨块累加 sum 即可，无需读数据块。
    pub sum: f64,
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
                // P3-B：v6 新增 sum 字段（f64 8 字节）
                ib.extend_from_slice(&z.sum.to_le_bytes());
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
        // P3-B：数值列累加和
        let mut is_numeric = true;
        let mut sum: f64 = 0.0;
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
                    // P3-B：数值列累加和
                    if is_numeric {
                        if let Some(n) = v.as_f64() {
                            sum += n;
                        } else {
                            is_numeric = false;
                        }
                    }
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
            // P3-B：非数值列 sum=0.0（由查询方通过 present_count==0 识别未计算的列）
            sum: if is_numeric { sum } else { 0.0 },
        });
    }
    // Seqs
    for (_, _, seq) in &parsed {
        out.extend_from_slice(&seq.to_le_bytes());
    }
    Ok((out, zones))
}
