//! segment：段文件格式（v2-v6 posting 编解码 / v3 分块 / 统计载荷 / 段清单 Manifest）+ FST 术语字典。
//! 重构自 src/inverted.rs 对应主题，行为零变化。

use std::io::Write;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::keys::decode_varlen;
use mmap_file::MmapFile;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use super::decode_varint;
use super::stats::FieldAgg;
use super::{InvertedIndex, Posting};


/// 旧段（v2–v5，32 位 RoaringBitmap）posting 升 64 位（默认表 docid<2^32 数值不变）。
fn bm32(bm: RoaringBitmap) -> Posting {
    let mut p = Posting::new();
    for v in bm.iter() {
        p.insert(v as u64);
    }
    p
}

/// 段文件魔数。
pub(super) const SEG_MAGIC: &[u8; 8] = b"NVINV001";
/// 段文件版本：v2 = term 带字段前缀 + Roaring 紧凑 posting；v3 = posting 分块布局
/// （容器头索引 + 独立容器字节，K 项：分页/COUNT 按需延迟加载，全量解码与 v2 持平）；
/// v4 = 条目插入 `varint(段内 doc_count)`（Ex-9.1b：段级 TermMeta 计数载荷 → COUNT
/// 亚毫秒求和，免逐 docid 遍历去重；老段读取按段版本兼容回退）；
/// v5 = 条目在 doc_count 后追加统计载荷 `varint(fcount) + fcount × (n u64 + sum/min/max f64)`
/// （Ex-9.3：随 term 的数值聚合，支撑 SUM/AVG/MIN/MAX 类聚合免全扫；读取按版本跳过）。
/// v6 = posting 改 **64 位**（RoaringTreemap 序列化，docid = table_id<<48|row_id 的多表
/// 支持；v2–v5 旧段读取时解码为 32 位再升 64 位——默认表（tid=0）docid 不变，零迁移）。
pub(super) const SEG_VERSION: u16 = 6;
/// 段文件前缀。
pub(super) const SEG_PREFIX: &str = "inverted-";
pub(super) const MANIFEST_FILE: &str = "inverted-manifest.json";

/// 段清单（新→旧顺序）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SegmentManifest {
    /// 最近刷盘的段在最前。
    pub(super) segments: Vec<String>,
    pub(super) next_seg_id: u64,
}

/// v5：跳过 term 条目中 doc_count 之后的统计载荷（`varint(fcount)` + `fcount × 32B` 定长
/// n u64 + sum/min/max f64）。v4 及更早无载荷（游标不动）。
fn skip_stats_v5(data: &[u8], cur: &mut usize, ver: u16) -> Result<()> {
    if ver >= 5 {
        let fc = decode_varint(data, cur)? as usize;
        let bytes = fc * (8 + 8 + 8 + 8);
        if *cur + bytes > data.len() {
            return Err(Error::Corrupted("倒排段统计载荷越界".into()));
        }
        *cur += bytes;
    }
    Ok(())
}

/// 解析 v5 条目统计载荷（`entry` 指向 term 起点）。v4 及更早 / fcount==0 → None。
pub(super) fn parse_term_stats_at(data: &[u8], entry: usize, ver: u16) -> Result<Option<Vec<FieldAgg>>> {
    if ver < 5 {
        return Ok(None);
    }
    let mut cur = entry;
    let _t = decode_varlen(data, &mut cur)?;
    let _c = decode_varint(data, &mut cur)?; // doc_count
    let fc = decode_varint(data, &mut cur)? as usize;
    if fc == 0 {
        return Ok(None);
    }
    let bytes = fc * (8 + 8 + 8 + 8);
    if cur + bytes > data.len() {
        return Err(Error::Corrupted("倒排段统计载荷越界".into()));
    }
    let mut out = Vec::with_capacity(fc);
    for _ in 0..fc {
        let n = u64::from_le_bytes(data[cur..cur + 8].try_into().unwrap());
        cur += 8;
        let sum = f64::from_le_bytes(data[cur..cur + 8].try_into().unwrap());
        cur += 8;
        let min = f64::from_le_bytes(data[cur..cur + 8].try_into().unwrap());
        cur += 8;
        let max = f64::from_le_bytes(data[cur..cur + 8].try_into().unwrap());
        cur += 8;
        out.push(FieldAgg { n, sum, min, max });
    }
    Ok(Some(out))
}

/// 从段数据指定偏移解析 (term, posting) 条目（FST 字典指向的条目）。
/// v6 = 64 位 treemap 序列化；v3–v5 = 32 位分块；v2 = Roaring 紧凑字节（旧段兼容）。
pub(super) fn parse_posting_at(data: &[u8], offset: usize, ver: u16) -> Result<Posting> {
    let mut cur = offset;
    let _t = decode_varlen(data, &mut cur)?; // 跳过 term
    if ver >= 4 {
        let _c = decode_varint(data, &mut cur)?; // 跳过 v4+ doc_count 载荷
    }
    skip_stats_v5(data, &mut cur, ver)?; // Ex-9.3：跳过 v5+ 统计载荷
    let p = decode_varlen(data, &mut cur)?.to_vec();
    decode_posting_bytes(&p, ver)
}

/// 按段版本解码 posting：v6 = 64 位 treemap；v3–v5 = 32 位分块；v2 = Roaring 紧凑字节。
fn decode_posting_bytes(p: &[u8], ver: u16) -> Result<Posting> {
    if ver >= 6 {
        Ok(Posting::deserialize_from(p)
            .map_err(|e| Error::Corrupted(format!("v6 posting 反序列化失败: {e}")))?)
    } else if ver >= 3 {
        Ok(bm32(decode_posting_v3(p)?))
    } else {
        Ok(bm32(
            RoaringBitmap::deserialize_from(p)
                .map_err(|e| Error::Corrupted(format!("posting 反序列化失败: {e}")))?,
        ))
    }
}

/// v6 编码：64 位 posting（RoaringTreemap 序列化）→ payload 字节。
pub(super) fn encode_posting_v6(bm: &Posting) -> Vec<u8> {
    let mut bytes = Vec::new();
    bm.serialize_into(&mut bytes).unwrap();
    bytes
}

// ---------- K 项（7.74）：v3 posting 分块布局（按容器延迟加载） ----------
//
// v3 条目 payload：`[u32 容器数][每容器 14B 头：high u16 + card u32 + off u32 + len u32][容器数据区]`。
// 每个容器 = Roaring 单容器 bitmap（值 = **完整 docid**，容器级对齐 → OR 合并零额外成本）。
// 分页 / COUNT 只反序列化窗口覆盖的容器（近页 x211、COUNT x4491；全量解码与 v2 紧凑持平，demo posting-chunk）。

/// v3 容器头。
#[derive(Clone, Copy)]
struct ChunkHeader {
    high: u16,
    card: u64,
    off: usize,
    len: usize,
}

/// 解析 v3 容器头（不复制数据区）。返回（头数组, 头区总字节数）。
fn v3_headers(payload: &[u8]) -> Result<(Vec<ChunkHeader>, usize)> {
    if payload.len() < 4 {
        return Err(Error::Corrupted("v3 posting 头过短".into()));
    }
    let nc = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let hdr_bytes = 4 + 14 * nc;
    if payload.len() < hdr_bytes {
        return Err(Error::Corrupted("v3 posting 容器头截断".into()));
    }
    let mut hs = Vec::with_capacity(nc);
    for i in 0..nc {
        let b = 4 + i * 14;
        hs.push(ChunkHeader {
            high: u16::from_le_bytes(payload[b..b + 2].try_into().unwrap()),
            card: u32::from_le_bytes(payload[b + 2..b + 6].try_into().unwrap()) as u64,
            off: u32::from_le_bytes(payload[b + 6..b + 10].try_into().unwrap()) as usize,
            len: u32::from_le_bytes(payload[b + 10..b + 14].try_into().unwrap()) as usize,
        });
    }
    Ok((hs, hdr_bytes))
}

/// v3 编码：RoaringBitmap → 分块 payload（flush / gc 写路径用）。
fn encode_posting_v3(bm: &RoaringBitmap) -> Vec<u8> {
    // 按高 16 位分容器（bitmap 升序迭代 → 容器天然按 high 升序）
    let mut by_high: Vec<(u16, Vec<u32>)> = Vec::new();
    for d in bm.iter() {
        let high = (d >> 16) as u16;
        match by_high.last_mut() {
            Some((h, v)) if *h == high => v.push(d),
            _ => by_high.push((high, vec![d])),
        }
    }
    // 各容器序列化为 Roaring 单容器字节（值 = 完整 docid → 容器级对齐）
    let mut bodies: Vec<(u16, u64, Vec<u8>)> = Vec::new();
    for (high, vals) in by_high {
        let c: RoaringBitmap = vals.into_iter().collect();
        let mut bytes = Vec::new();
        c.serialize_into(&mut bytes).unwrap();
        bodies.push((high, c.len(), bytes));
    }
    let mut out = Vec::new();
    out.extend((bodies.len() as u32).to_le_bytes());
    let mut data_off = 4 + 14 * bodies.len();
    for (high, card, bytes) in &bodies {
        out.extend(high.to_le_bytes());
        out.extend((*card as u32).to_le_bytes());
        out.extend((data_off as u32).to_le_bytes());
        out.extend((bytes.len() as u32).to_le_bytes());
        data_off += bytes.len();
    }
    for (_, _, bytes) in &bodies {
        out.extend_from_slice(bytes);
    }
    out
}

/// v3 全量解码（search / 合并场景；容器级 OR 与 v2 紧凑反序列化持平）。
fn decode_posting_v3(payload: &[u8]) -> Result<RoaringBitmap> {
    let (hs, _) = v3_headers(payload)?;
    let mut result = RoaringBitmap::new();
    for h in hs {
        let end = h.off + h.len;
        if payload.len() < end {
            return Err(Error::Corrupted("v3 posting 容器数据截断".into()));
        }
        let c = RoaringBitmap::deserialize_from(&payload[h.off..end])
            .map_err(|e| Error::Corrupted(format!("v3 容器反序列化失败: {e}")))?;
        result |= c;
    }
    Ok(result)
}

/// v3 惰性游标（分页 k-way merge 用）：容器级按需解码，逐 docid 产出。
/// 持有段数据 `Arc<MmapFile>`（零拷贝）——容器数据在**数据区**按需反序列化。
pub(super) struct PostingCursor {
    /// 段数据映射；`None` = v2 旧段全量解码后的内存游标（from_bitmap）。
    data: Option<Arc<MmapFile>>,
    /// 条目 payload 起点（相对段文件头）。
    payload_off: usize,
    headers: Vec<ChunkHeader>,
    idx: usize,
    cur: Option<(Vec<u64>, usize)>, // (当前容器值列表[64 位化], 位置)
}

impl PostingCursor {
    /// 从条目 offset（FST 指向）构造：跳过 varlen term（+ v4 doc_count）→ payload → 容器头。
    pub(super) fn new(data: &Arc<MmapFile>, entry: usize, ver: u16) -> Result<Self> {
        let mut cur = entry;
        let _t = decode_varlen(data, &mut cur)?; // 跳过 term（pos → term 末尾）
        if ver >= 4 {
            let _c = decode_varint(data, &mut cur)?; // Ex-9.1b：跳过 v4 doc_count 载荷
        }
        skip_stats_v5(data, &mut cur, ver)?; // Ex-9.3：跳过 v5 统计载荷
        let payload = decode_varlen(data, &mut cur)?; // payload 切片（pos → payload 末尾）
        let (headers, _) = v3_headers(payload)?;
        Ok(Self {
            data: Some(data.clone()),
            payload_off: cur - payload.len(),
            headers,
            idx: 0,
            cur: None,
        })
    }

    /// 从已解码位图构造（v2 旧段 / v6 64 位全量解码兼容）：全部 docid 作为单个"容器"。
    pub(super) fn from_bitmap(bm: Posting) -> Self {
        let vals: Vec<u64> = bm.iter().collect();
        Self {
            data: None,
            payload_off: 0,
            headers: Vec::new(),
            idx: 0,
            cur: Some((vals, 0)),
        }
    }

    /// 该段内该 term 的 posting 总数（头部基数求和）。
    pub(super) fn total(&self) -> u64 {
        self.headers.iter().map(|h| h.card).sum()
    }

    /// 下一个 docid（跨容器推进时解码；64 位 docid）。None = 耗尽。
    pub(super) fn next_docid(&mut self) -> Result<Option<u64>> {
        loop {
            if let Some((vals, pos)) = &mut self.cur {
                if *pos < vals.len() {
                    let v = vals[*pos];
                    *pos += 1;
                    return Ok(Some(v));
                }
            }
            if self.idx >= self.headers.len() {
                return Ok(None);
            }
            let h = self.headers[self.idx];
            self.idx += 1;
            let data = self.data.as_ref().expect("段游标必有数据");
            let start = self.payload_off + h.off;
            let end = start + h.len;
            if data.len() < end {
                return Err(Error::Corrupted("v3 posting 容器数据截断".into()));
            }
            let c = RoaringBitmap::deserialize_from(&data[start..end])
                .map_err(|e| Error::Corrupted(format!("v3 容器反序列化失败: {e}")))?;
            self.cur = Some((c.iter().map(|v| v as u64).collect(), 0));
        }
    }
}

impl InvertedIndex {
    /// 编译并写 FST 字典文件 `inverted-{id}.fst`（term → 段内条目字节偏移，字典序），
    /// 返回 mmap 只读字典（Ex-5.7，供本实例即时使用，无需重启；零堆分配按需加载）。
    pub(super) fn write_fst_dict(
        &self,
        seg_id: u64,
        term_offsets: &[(Vec<u8>, u64)],
    ) -> Result<fst::Map<MmapFile>> {
        let fst_path = self.dir.join(format!("{SEG_PREFIX}{seg_id:08}.fst"));
        let tmp = self.dir.join(format!("{SEG_PREFIX}{seg_id:08}.fst.tmp"));
        {
            let file = std::fs::File::create(&tmp)?;
            {
                let mut w = std::io::BufWriter::new(&file);
                let mut builder = fst::MapBuilder::new(&mut w)
                    .map_err(|e| Error::Serialize(format!("FST 构建失败: {e}")))?;
                for (term, offset) in term_offsets {
                    builder
                        .insert(term, *offset)
                        .map_err(|e| Error::Serialize(format!("FST 写入失败: {e}")))?;
                }
                builder
                    .finish()
                    .map_err(|e| Error::Serialize(format!("FST 完成失败: {e}")))?;
                w.flush()?;
            }
            // 写句柄上 fsync（Windows FlushFileBuffers 需写权限；只读句柄会 PermissionDenied）
            file.sync_all()?;
        }
        // 原子改名（先改名再 mmap：Windows 下已映射文件无法 rename）
        std::fs::rename(&tmp, &fst_path)?;
        let mm = MmapFile::open(&fst_path)?;
        fst::Map::new(mm).map_err(|e| Error::Serialize(format!("FST 字典解析失败: {e}")))
    }

    pub(super) fn persist_manifest(&self) -> Result<()> {
        let segs = self.segments.load(); // Ex-6.2：快照
        let m = SegmentManifest {
            segments: segs.as_ref().clone(),
            next_seg_id: self.next_seg_id.load(Ordering::Relaxed),
        };
        let text = serde_json::to_string_pretty(&m)
            .map_err(|e| Error::Serialize(format!("倒排 Manifest 序列化失败: {e}")))?;
        let tmp = self.dir.join("manifest.json.tmp");
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, self.dir.join(MANIFEST_FILE))?;
        Ok(())
    }

    /// 段文件版本（魔数后 u16；头部不足返回 0）。
    pub(super) fn seg_ver(data: &[u8]) -> u16 {
        if data.len() < 10 {
            0
        } else {
            u16::from_le_bytes(data[8..10].try_into().unwrap())
        }
    }

    /// 读取某段内 term 的 posting（未命中返回空 bitmap）。
    /// FST 字典存在时 O(len(term)) 精确定位（design 5.2.4.1）；旧段回退线性扫描。
    /// G 补充：段数据 mmap 化——首次访问懒加载注册，后续按 FST offset 直接切片，
    /// 免 `fs::read` 全文件读取 + 堆复制（大段文件未命中查询的主要 IO 成本）。
    pub(super) fn read_segment_posting(&self, seg: &str, term: &str) -> Result<Posting> {
        let data = {
            let files = self.data_files.load();
            match files.get(seg) {
                Some(m) => m.clone(),
                None => {
                    drop(files);
                    // J 项（7.73）：后台 GC 与查询并发——查询持旧段快照时 gc 可能已删该段
                    // 文件（数据已合并进新段）。文件不存在 = 快照过期 → 跳过该段返回空
                    // （与 ArcSwap 快照语义一致：读到 gc 前/后快照结果一致），其他 IO 错误
                    // （真损坏）照常传播。
                    let mm = match MmapFile::open(&self.dir.join(seg)) {
                        Ok(m) => Arc::new(m),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            return Ok(Posting::new())
                        }
                        Err(e) => return Err(Error::from(e)),
                    };
                    // rcu 发布（&self 可原子更新）：并发查询下重复注册无害（幂等覆盖）
                    self.data_files.rcu(|m| {
                        let mut n = (**m).clone();
                        n.insert(seg.to_string(), mm.clone());
                        n
                    });
                    mm
                }
            }
        };
        if data.len() < 10 || &data[0..8] != SEG_MAGIC {
            return Err(Error::Corrupted(format!("倒排段魔数错误: {seg}")));
        }
        // 段版本：v6 = 64 位 posting；v2–v5 = 32 位旧格式（解码后升 64 位）
        let ver = Self::seg_ver(&data);
        // FST 精确查找：term → 段内条目字节偏移
        let dicts = self.dicts.load(); // Ex-6.3：Arc 快照零拷贝
        if let Some(map) = dicts.get(seg) {
            return match map.get(term.as_bytes()) {
                Some(offset) => parse_posting_at(&data, offset as usize, ver),
                None => Ok(Posting::new()),
            };
        }
        // 回退线性扫描（无 FST 的旧段 / hash 引擎）
        let mut cur = 10usize;
        let count = decode_varint(&data, &mut cur)?;
        for _ in 0..count {
            let t = decode_varlen(&data, &mut cur)?.to_vec();
            if ver >= 4 {
                let _c = decode_varint(&data, &mut cur)?; // 跳过 v4+ doc_count 载荷（v6 同布局）
            }
            skip_stats_v5(&data, &mut cur, ver)?; // Ex-9.3：跳过 v5+ 统计载荷（v6 含同布局）
            let p = decode_varlen(&data, &mut cur)?.to_vec();
            if t.as_slice() == term.as_bytes() {
                return decode_posting_bytes(&p, ver);
            }
        }
        Ok(Posting::new())
    }

    /// 定位某 term 在某段内的条目起点（FST offset / 线性扫描），并返回段数据映射。
    /// 段文件不存在（后台 GC 已删，J 项）→ None。供 doc_count / search_paged 快速路径用。
    pub(super) fn segment_posting_entry(
        &self,
        seg: &str,
        term: &str,
    ) -> Result<Option<(Arc<MmapFile>, usize)>> {
        let files = self.data_files.load();
        let data = if let Some(m) = files.get(seg) {
            m.clone()
        } else {
            drop(files);
            let mm = match MmapFile::open(&self.dir.join(seg)) {
                Ok(m) => Arc::new(m),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(Error::from(e)),
            };
            self.data_files.rcu(|m| {
                let mut n = (**m).clone();
                n.insert(seg.to_string(), mm.clone());
                n
            });
            mm
        };
        if data.len() < 10 || &data[0..8] != SEG_MAGIC {
            return Err(Error::Corrupted(format!("倒排段魔数错误: {seg}")));
        }
        // FST 精确查找：term → 段内条目字节偏移
        let dicts = self.dicts.load();
        if let Some(map) = dicts.get(seg) {
            return Ok(map.get(term.as_bytes()).map(|o| (data, o as usize)));
        }
        // 回退线性扫描（无 FST 的旧段 / hash 引擎）
        let mut cur = 10usize;
        let count = decode_varint(&data, &mut cur)?;
        for _ in 0..count {
            let entry = cur; // 条目起点（term varlen 前）
            let t = decode_varlen(&data, &mut cur)?.to_vec();
            if Self::seg_ver(&data) >= 4 {
                let _c = decode_varint(&data, &mut cur)?; // Ex-9.1b：跳过 v4 doc_count 载荷
            }
            skip_stats_v5(&data, &mut cur, Self::seg_ver(&data))?; // Ex-9.3：跳过 v5 统计载荷
            let _p = decode_varlen(&data, &mut cur)?;
            if t.as_slice() == term.as_bytes() {
                return Ok(Some((data, entry)));
            }
        }
        Ok(None)
    }

    /// 读取段内全部 term 及其 posting（供遍历）。
    pub(super) fn read_segment_terms(&self, seg: &str) -> Result<Vec<(String, Posting)>> {
        let path = self.dir.join(seg);
        let data = std::fs::read(&path)?;
        if data.len() < 10 || &data[0..8] != SEG_MAGIC {
            return Err(Error::Corrupted(format!("倒排段魔数错误: {seg}")));
        }
        // 段版本：v6 = 64 位 posting；v2–v5 = 32 位旧格式（解码后升 64 位）
        let ver = Self::seg_ver(&data);
        let mut cur = 10usize;
        let count = decode_varint(&data, &mut cur)?;
        let mut out = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let t = decode_varlen(&data, &mut cur)?.to_vec();
            if ver >= 4 {
                let _c = decode_varint(&data, &mut cur)?; // v4+ doc_count 载荷（v6 同布局）
            }
            skip_stats_v5(&data, &mut cur, ver)?; // Ex-9.3：跳过 v5+ 统计载荷
            let p = decode_varlen(&data, &mut cur)?.to_vec();
            let bitmap = decode_posting_bytes(&p, ver)?;
            out.push((String::from_utf8_lossy(&t).into_owned(), bitmap));
        }
        Ok(out)
    }
}
