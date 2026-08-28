//! 倒排索引（design 5.2 / development 步骤 10）。
//!
//! 阶段 1.5 架构（FST + 字典）：
//! - **内存哈希字典**：`DashMap<term, Vec<docid>>` 收集增量写入（保留，热点最新数据）；
//! - **Append-Only 倒排段文件**：达阈值后整段刷盘 `inverted-{id}.seg`，
//!   段内每 term 的 posting 用 **RoaringBitmap** 序列化存储；
//! - **FST 术语字典（design 5.2.4.1）**：每段刷盘时编译 `inverted-{id}.fst`（term → 段内条目字节偏移），
//!   查询用 FST O(len(term)) 精确定位，替代逐段线性扫描；启动时加载为内存不可变字典，
//!   无 FST 的旧段回退线性扫描（兼容）；
//! - **段清单 Manifest**（`inverted-manifest.json`）：记录段文件列表（新→旧），
//!   原子写（tmp + rename），杜绝 GC 崩溃风险（design 4.5）；
//! - **查询**：内存字典 ∪ 各段 posting 合并为 RoaringBitmap；
//! - 段 GC / 压缩（阶段 2 分层 GC）；删除标记（Tombstone）随文档引擎层处理。
//!
//! > mmap 按需加载（冷启动亚秒）为设计目标；本项目 `#![forbid(unsafe_code)]`，
//! > memmap2 的 mmap 为 unsafe API，故 FST 字典采用 fs::read 加载（FST 为压缩结构、体积小），
//! > mmap 化留待独立 crate 封装 unsafe 白名单后落地。

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::error::{Error, Result};
use crate::keys::{decode_varlen, encode_varlen};

/// 段文件魔数。
const SEG_MAGIC: &[u8; 8] = b"NVINV001";
/// 段文件版本：v2 = term 带字段前缀（`field=value`，供 COUNT/GROUP BY 聚合）。
const SEG_VERSION: u16 = 2;
/// 段文件前缀。
const SEG_PREFIX: &str = "inverted-";
const MANIFEST_FILE: &str = "inverted-manifest.json";

/// 段清单（新→旧顺序）。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentManifest {
    /// 最近刷盘的段在最前。
    segments: Vec<String>,
    next_seg_id: u64,
}

/// 倒排索引：内存字典 + 磁盘段集合 + FST 术语字典。
pub struct InvertedIndex {
    dir: PathBuf,
    /// 字典引擎："hash"（纯线性）/ "fst"（FST 精确查找）。
    engine: String,
    /// term → 内存收集的 docid 列表。
    mem: DashMap<String, Vec<u64>>,
    /// 内存累计 docid 数（触发刷盘阈值判断）。
    mem_docids: AtomicU64,
    /// 段文件列表（新→旧，仅含文件名）。
    segments: Vec<String>,
    /// 段 → FST 术语字典（term → 段内条目字节偏移）；engine=fst 且存在 .fst 时填充。
    dicts: HashMap<String, fst::Map<Vec<u8>>>,
    next_seg_id: u64,
    /// 刷盘阈值：内存累计 posting 达此值整段落盘。
    flush_threshold: u64,
}

impl InvertedIndex {
    /// 打开（或创建）倒排索引：加载 Manifest 与 FST 字典。默认 FST 引擎（阶段 1.5）。
    pub fn open(dir: &Path, flush_threshold: u64) -> Result<Self> {
        Self::open_with_engine(dir, flush_threshold, "fst")
    }

    /// 打开（或创建）倒排索引，指定字典引擎。
    pub fn open_with_engine(dir: &Path, flush_threshold: u64, engine: &str) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let manifest_path = dir.join(MANIFEST_FILE);
        let (segments, next_seg_id) = if manifest_path.exists() {
            let text = std::fs::read_to_string(&manifest_path)?;
            let m: SegmentManifest = serde_json::from_str(&text)
                .map_err(|e| Error::Corrupted(format!("倒排 Manifest 解析失败: {e}")))?;
            (m.segments, m.next_seg_id)
        } else {
            (Vec::new(), 1)
        };
        info!(
            "倒排索引打开: {} 个段，下一个 id={next_seg_id}",
            segments.len()
        );
        // 加载 FST 术语字典（design 5.2.4.1）：每个段对应 inverted-{id}.fst；
        // 缺失的段（旧数据 / hash 引擎写入）在查询时回退线性扫描。
        let mut dicts = HashMap::new();
        if engine == "fst" {
            for seg in &segments {
                let fst_name = seg.replace(".seg", ".fst");
                let fst_path = dir.join(&fst_name);
                if fst_path.exists() {
                    if let Ok(bytes) = std::fs::read(&fst_path) {
                        match fst::Map::new(bytes) {
                            Ok(map) => {
                                dicts.insert(seg.clone(), map);
                            }
                            Err(e) => {
                                info!("FST 字典解析失败，该段回退线性扫描: {fst_name}: {e}")
                            }
                        }
                    }
                }
            }
            info!("FST 字典加载: {}/{} 段", dicts.len(), segments.len());
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            engine: engine.to_string(),
            mem: DashMap::new(),
            mem_docids: AtomicU64::new(0),
            segments,
            dicts,
            next_seg_id,
            flush_threshold,
        })
    }

    /// 追加一个 (term, docid) 到内存字典。docid 必须 < 2^32（RoaringBitmap 上限）。
    pub fn add(&self, term: &str, docid: u64) {
        assert!(docid < u32::MAX as u64, "docid 超出 RoaringBitmap 支持范围");
        self.mem.entry(term.to_string()).or_default().push(docid);
        self.mem_docids.fetch_add(1, Ordering::Relaxed);
    }

    /// 当前内存累计 posting 数（供外部决定是否刷盘）。
    pub fn mem_docids(&self) -> u64 {
        self.mem_docids.load(Ordering::Relaxed)
    }

    /// 内存是否达阈值，需要刷盘。
    pub fn needs_flush(&self) -> bool {
        self.mem_docids() >= self.flush_threshold
    }

    /// 将内存字典整段刷盘为 `inverted-{id}.seg`，并原子更新 Manifest。
    /// engine=fst 时同时编译术语字典 `inverted-{id}.fst`（term → 段内条目偏移）。
    pub fn flush_segment(&mut self) -> Result<()> {
        if self.mem.is_empty() {
            return Ok(());
        }
        let seg_id = self.next_seg_id;
        self.next_seg_id += 1;
        let path = self.dir.join(format!("{SEG_PREFIX}{seg_id:08}.seg"));

        // 序列化段内容（内存快照），并记录每个 term 条目的文件偏移（FST 字典用）
        let mut body = Vec::new();
        let mut term_offsets: Vec<(Vec<u8>, u64)> = Vec::new();
        encode_varint(&mut body, self.mem.len() as u64);
        // 按 term 排序，保证段内确定性（FST 也要求 key 按字典序插入）
        let mut terms: Vec<(String, Vec<u64>)> = self
            .mem
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        terms.sort_by(|a, b| a.0.cmp(&b.0));
        for (term, docids) in terms {
            let file_offset = (SEG_MAGIC.len() + std::mem::size_of::<u16>() + body.len()) as u64;
            term_offsets.push((term.clone().into_bytes(), file_offset));
            let bitmap: RoaringBitmap = docids.iter().map(|d| *d as u32).collect();
            let mut bytes = Vec::new();
            bitmap
                .serialize_into(&mut bytes)
                .map_err(|e| Error::Serialize(format!("posting 序列化失败: {e}")))?;
            encode_varlen(&mut body, term.as_bytes());
            encode_varlen(&mut body, &bytes);
        }

        // 写文件：先 tmp 再 rename（原子）
        let tmp = self.dir.join(format!("{SEG_PREFIX}{seg_id:08}.seg.tmp"));
        let mut out = std::fs::File::create(&tmp)?;
        std::io::Write::write_all(&mut out, SEG_MAGIC)?;
        std::io::Write::write_all(&mut out, &SEG_VERSION.to_le_bytes())?;
        std::io::Write::write_all(&mut out, &body)?;
        out.sync_all()?;
        std::fs::rename(&tmp, &path)?;

        let fname = path.file_name().unwrap().to_string_lossy().to_string();

        // FST 术语字典（design 5.2.4.1）：term → 段内条目字节偏移，原子写
        if self.engine == "fst" {
            let map = self.write_fst_dict(seg_id, &term_offsets)?;
            self.dicts.insert(fname.clone(), map);
        }

        // 更新 Manifest（原子）
        self.segments.insert(0, fname.clone());
        self.persist_manifest()?;

        // 清空内存
        self.mem.clear();
        self.mem_docids.store(0, Ordering::Relaxed);
        info!("倒排刷盘完成: {fname}");
        Ok(())
    }

    /// 编译并写 FST 字典文件 `inverted-{id}.fst`（term → 段内条目字节偏移，字典序），
    /// 返回内存字典（供本实例即时使用，无需重启）。
    fn write_fst_dict(
        &self,
        seg_id: u64,
        term_offsets: &[(Vec<u8>, u64)],
    ) -> Result<fst::Map<Vec<u8>>> {
        let fst_path = self.dir.join(format!("{SEG_PREFIX}{seg_id:08}.fst"));
        let tmp = self.dir.join(format!("{SEG_PREFIX}{seg_id:08}.fst.tmp"));
        {
            let file = std::fs::File::create(&tmp)?;
            let mut w = std::io::BufWriter::new(file);
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
        let bytes = std::fs::read(&tmp)?;
        std::fs::rename(&tmp, &fst_path)?;
        fst::Map::new(bytes).map_err(|e| Error::Serialize(format!("FST 内存映射失败: {e}")))
    }

    fn persist_manifest(&self) -> Result<()> {
        let m = SegmentManifest {
            segments: self.segments.clone(),
            next_seg_id: self.next_seg_id,
        };
        let text = serde_json::to_string_pretty(&m)
            .map_err(|e| Error::Serialize(format!("倒排 Manifest 序列化失败: {e}")))?;
        let tmp = self.dir.join("manifest.json.tmp");
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, self.dir.join(MANIFEST_FILE))?;
        Ok(())
    }

    /// 查询 term：合并内存 posting 与各段 posting，返回 RoaringBitmap（docid 按 u32 语义）。
    pub fn search(&self, term: &str) -> Result<RoaringBitmap> {
        let mut result = RoaringBitmap::new();
        // 内存（最新）
        if let Some(docids) = self.mem.get(term) {
            result.extend(docids.iter().map(|d| *d as u32));
        }
        // 各段（新→旧，bitmap 合并天然去重）
        for seg in &self.segments {
            let posting = self.read_segment_posting(seg, term)?;
            result |= posting;
        }
        Ok(result)
    }

    /// 读取某段内 term 的 posting（未命中返回空 bitmap）。
    /// FST 字典存在时 O(len(term)) 精确定位（design 5.2.4.1）；旧段回退线性扫描。
    fn read_segment_posting(&self, seg: &str, term: &str) -> Result<RoaringBitmap> {
        let path = self.dir.join(seg);
        let data = std::fs::read(&path)?;
        if data.len() < 10 || &data[0..8] != SEG_MAGIC {
            return Err(Error::Corrupted(format!("倒排段魔数错误: {seg}")));
        }
        // FST 精确查找：term → 段内条目字节偏移
        if let Some(map) = self.dicts.get(seg) {
            return match map.get(term.as_bytes()) {
                Some(offset) => parse_posting_at(&data, offset as usize),
                None => Ok(RoaringBitmap::new()),
            };
        }
        // 回退线性扫描（无 FST 的旧段 / hash 引擎）
        let mut cur = 10usize;
        let count = decode_varint(&data, &mut cur)?;
        for _ in 0..count {
            let t = decode_varlen(&data, &mut cur)?.to_vec();
            let p = decode_varlen(&data, &mut cur)?.to_vec();
            if t.as_slice() == term.as_bytes() {
                return RoaringBitmap::deserialize_from(&p[..])
                    .map_err(|e| Error::Corrupted(format!("posting 反序列化失败: {e}")));
            }
        }
        Ok(RoaringBitmap::new())
    }

    /// 当前磁盘段数。
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// 当前加载的 FST 术语字典数（测试 / 监控）。
    pub fn fst_dict_count(&self) -> usize {
        self.dicts.len()
    }

    /// 某 term 命中的文档数（COUNT 原子操作，<0.1ms，design 5.17）。
    pub fn doc_count(&self, term: &str) -> Result<u64> {
        Ok(self.search(term)?.len())
    }

    /// 遍历全部 term（内存 + 各段），合并出每个 term 的完整 posting 位图。
    /// 供聚合执行器（GROUP BY）与字典浏览使用；内存 term 合并天然去重。
    pub fn iter_terms(&self) -> Result<Vec<(String, RoaringBitmap)>> {
        let mut map: std::collections::BTreeMap<String, RoaringBitmap> =
            std::collections::BTreeMap::new();
        // 内存（最新）
        for entry in self.mem.iter() {
            let bitmap: RoaringBitmap = entry.value().iter().map(|d| *d as u32).collect();
            let e = map.entry(entry.key().clone()).or_default();
            *e |= bitmap;
        }
        // 各段（新→旧）
        for seg in &self.segments {
            for (term, posting) in self.read_segment_terms(seg)? {
                let e = map.entry(term).or_default();
                *e |= posting;
            }
        }
        Ok(map.into_iter().collect())
    }

    /// 读取段内全部 term 及其 posting（供遍历）。
    fn read_segment_terms(&self, seg: &str) -> Result<Vec<(String, RoaringBitmap)>> {
        let path = self.dir.join(seg);
        let data = std::fs::read(&path)?;
        if data.len() < 10 || &data[0..8] != SEG_MAGIC {
            return Err(Error::Corrupted(format!("倒排段魔数错误: {seg}")));
        }
        let mut cur = 10usize;
        let count = decode_varint(&data, &mut cur)?;
        let mut out = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let t = decode_varlen(&data, &mut cur)?.to_vec();
            let p = decode_varlen(&data, &mut cur)?.to_vec();
            let bitmap = RoaringBitmap::deserialize_from(&p[..])
                .map_err(|e| Error::Corrupted(format!("posting 反序列化失败: {e}")))?;
            out.push((String::from_utf8_lossy(&t).into_owned(), bitmap));
        }
        Ok(out)
    }

    /// 按字段前缀分组（GROUP BY）：遍历全部 term，取 `{field}=` 开头的各 value 及其 doc_count。
    pub fn group_by(&self, field: &str) -> Result<Vec<(String, u64)>> {
        let prefix = format!("{field}=");
        let mut out = Vec::new();
        for (term, posting) in self.iter_terms()? {
            if term.starts_with(&prefix) {
                out.push((term, posting.len()));
            }
        }
        Ok(out)
    }
}

/// 从段数据指定偏移解析 (term, posting) 条目（FST 字典指向的条目）。
fn parse_posting_at(data: &[u8], offset: usize) -> Result<RoaringBitmap> {
    let mut cur = offset;
    let _t = decode_varlen(data, &mut cur)?; // 跳过 term
    let p = decode_varlen(data, &mut cur)?.to_vec();
    RoaringBitmap::deserialize_from(&p[..])
        .map_err(|e| Error::Corrupted(format!("posting 反序列化失败: {e}")))
}

/// 解码段文件计数 / term 条目数（LEB128）。
fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64> {
    crate::keys::decode_varint(data, pos)
}

fn encode_varint(buf: &mut Vec<u8>, n: u64) {
    crate::keys::encode_varint(buf, n);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let name = format!("inv-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    #[test]
    fn add_and_search_in_memory() {
        let idx = InvertedIndex::open(&tmp(), 10_000).unwrap();
        idx.add("rust", 1);
        idx.add("rust", 3);
        idx.add("rust", 5);
        idx.add("go", 2);
        let r = idx.search("rust").unwrap();
        assert!(r.contains(1) && r.contains(3) && r.contains(5));
        assert!(!r.contains(2));
        assert!(idx.search("go").unwrap().contains(2));
        assert!(idx.search("absent").unwrap().is_empty());
    }

    #[test]
    fn flush_and_search_across_segments() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap(); // 阈值 1，立即刷盘
        idx.add("a", 1);
        idx.flush_segment().unwrap();
        idx.add("a", 2);
        idx.add("b", 7);
        idx.flush_segment().unwrap();
        assert_eq!(idx.segment_count(), 2);

        // 跨段合并
        let r = idx.search("a").unwrap();
        assert!(r.contains(1) && r.contains(2));
        assert_eq!(r.len(), 2);
        assert!(idx.search("b").unwrap().contains(7));
    }

    #[test]
    fn restart_loads_manifest_and_segments() {
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open(&dir, 1).unwrap();
            idx.add("term-x", 10);
            idx.flush_segment().unwrap();
            idx.add("term-x", 20);
            idx.add("term-y", 30);
            idx.flush_segment().unwrap();
        }
        // 重启：Manifest 恢复段列表
        let idx2 = InvertedIndex::open(&dir, 1).unwrap();
        assert_eq!(idx2.segment_count(), 2);
        let r = idx2.search("term-x").unwrap();
        assert!(r.contains(10) && r.contains(20));
        assert!(idx2.search("term-y").unwrap().contains(30));
    }

    #[test]
    fn manifest_is_atomic_and_persists_next_id() {
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open(&dir, 1).unwrap();
            idx.add("k", 1);
            idx.flush_segment().unwrap();
            assert_eq!(idx.next_seg_id, 2);
        }
        let text = std::fs::read_to_string(dir.join(MANIFEST_FILE)).unwrap();
        assert!(text.contains("inverted-00000001.seg"));
        let m: SegmentManifest = serde_json::from_str(&text).unwrap();
        assert_eq!(m.next_seg_id, 2);
    }

    #[test]
    fn needs_flush_obeys_threshold() {
        let idx = InvertedIndex::open(&tmp(), 3).unwrap();
        assert!(!idx.needs_flush());
        idx.add("t", 1);
        idx.add("t", 2);
        assert!(!idx.needs_flush());
        idx.add("t", 3);
        assert!(idx.needs_flush());
    }

    #[test]
    fn orphan_segment_not_loaded_after_restart() {
        // 崩溃恢复：GC 中途崩溃可能残留"孤儿段"（不在 Manifest 中）——
        // 启动只按 Manifest 加载，孤儿段不得污染查询结果（development 4.5）
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open(&dir, 1).unwrap();
            idx.add("term-a", 1);
            idx.flush_segment().unwrap();
        }
        // 制造孤儿段：直接写一个 .seg 文件，但不更新 Manifest
        std::fs::write(dir.join("inverted-99999999.seg"), b"orphan-garbage").unwrap();

        let idx2 = InvertedIndex::open(&dir, 1).unwrap();
        assert_eq!(idx2.segment_count(), 1, "只应加载 Manifest 记录的段");
        // 正常段不受影响；孤儿段内容不参与查询
        assert!(idx2.search("term-a").unwrap().contains(1));
        assert!(idx2.search("orphan-garbage").unwrap().is_empty());
    }

    #[test]
    fn doc_count_counts_unique_docs() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap();
        idx.add("status=active", 1);
        idx.add("status=active", 2);
        idx.flush_segment().unwrap(); // 段 1
        idx.add("status=active", 2); // 重复 docid（更新）→ 跨段合并去重
        idx.add("status=pending", 3);
        idx.flush_segment().unwrap(); // 段 2
        assert_eq!(idx.doc_count("status=active").unwrap(), 2, "跨段合并去重");
        assert_eq!(idx.doc_count("status=pending").unwrap(), 1);
        assert_eq!(idx.doc_count("status=absent").unwrap(), 0);
    }

    #[test]
    fn group_by_aggregates_by_field_prefix() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap();
        idx.add("status=active", 1);
        idx.add("status=active", 2);
        idx.add("status=pending", 3);
        idx.flush_segment().unwrap();
        idx.add("status=active", 4); // 内存态
        idx.add("type=order", 1);

        let groups = idx.group_by("status").unwrap();
        let map: std::collections::HashMap<String, u64> = groups.into_iter().collect();
        assert_eq!(map.get("status=active").copied(), Some(3), "内存+段合并");
        assert_eq!(map.get("status=pending").copied(), Some(1));
        assert!(!map.contains_key("type=order"), "只返回指定字段分组");
        assert_eq!(idx.group_by("type").unwrap().len(), 1);
    }

    #[test]
    fn iter_terms_merges_memory_and_segments() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap();
        idx.add("a=1", 1);
        idx.flush_segment().unwrap();
        idx.add("b=2", 2);
        idx.add("a=1", 3);
        let terms = idx.iter_terms().unwrap();
        let map: std::collections::BTreeMap<String, u64> =
            terms.into_iter().map(|(t, b)| (t, b.len())).collect();
        assert_eq!(map.get("a=1").copied(), Some(2), "跨段+内存合并");
        assert_eq!(map.get("b=2").copied(), Some(1));
    }

    #[test]
    fn fst_dict_built_on_flush_and_lookup() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap(); // 默认 fst 引擎
        idx.add("status=active", 1);
        idx.add("status=active", 2);
        idx.add("type=order", 3);
        idx.flush_segment().unwrap();
        // .fst 文件已生成，重启后字典加载
        assert!(
            dir.join("inverted-00000001.fst").exists(),
            "应生成 FST 字典文件"
        );
        let idx2 = InvertedIndex::open(&dir, 1).unwrap();
        assert_eq!(idx2.fst_dict_count(), 1, "FST 字典应加载");
        // 走 FST 精确定位路径
        assert!(idx2.search("status=active").unwrap().contains(1));
        assert!(idx2.search("status=active").unwrap().contains(2));
        assert!(!idx2.search("status=active").unwrap().contains(3));
        assert!(idx2.search("absent").unwrap().is_empty());
        // 聚合走 FST 段数据
        assert_eq!(idx2.doc_count("type=order").unwrap(), 1);
    }

    #[test]
    fn fst_and_hash_engines_return_same_results() {
        let dir_a = tmp();
        let dir_b = tmp();
        let mut fst = InvertedIndex::open_with_engine(&dir_a, 1, "fst").unwrap();
        let mut hash = InvertedIndex::open_with_engine(&dir_b, 1, "hash").unwrap();
        for i in 1..=20u64 {
            fst.add(&format!("f={}", i % 5), i);
            hash.add(&format!("f={}", i % 5), i);
            if i % 7 == 0 {
                fst.flush_segment().unwrap();
                hash.flush_segment().unwrap();
            }
        }
        fst.flush_segment().unwrap();
        hash.flush_segment().unwrap();
        for i in 0..5u64 {
            let term = format!("f={i}");
            assert_eq!(
                fst.doc_count(&term).unwrap(),
                hash.doc_count(&term).unwrap(),
                "引擎结果应一致: {term}"
            );
        }
        assert_eq!(
            fst.fst_dict_count(),
            hash.segment_count(),
            "fst 每段一个字典"
        );
    }

    #[test]
    fn fst_missing_dict_falls_back_to_linear_scan() {
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open(&dir, 1).unwrap();
            idx.add("a=1", 1);
            idx.add("b=2", 2);
            idx.flush_segment().unwrap();
        }
        // 删除 FST 字典（模拟旧段 / 字典损坏），应回退线性扫描
        std::fs::remove_file(dir.join("inverted-00000001.fst")).unwrap();
        let idx = InvertedIndex::open(&dir, 1).unwrap();
        assert_eq!(idx.fst_dict_count(), 0, "字典缺失应回退");
        assert!(idx.search("a=1").unwrap().contains(1));
        assert!(idx.search("b=2").unwrap().contains(2));
        assert_eq!(idx.doc_count("a=1").unwrap(), 1);
    }

    #[test]
    fn hash_engine_writes_no_fst_dict() {
        let dir = tmp();
        let mut idx = InvertedIndex::open_with_engine(&dir, 1, "hash").unwrap();
        idx.add("k=1", 1);
        idx.flush_segment().unwrap();
        assert!(
            !dir.join("inverted-00000001.fst").exists(),
            "hash 引擎不生成 FST"
        );
        let idx2 = InvertedIndex::open(&dir, 1).unwrap();
        assert_eq!(idx2.fst_dict_count(), 0);
        assert!(idx2.search("k=1").unwrap().contains(1));
    }
}
