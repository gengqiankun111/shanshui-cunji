//! 倒排索引（design 5.2 / development 步骤 10）。
//!
//! MVP 架构（与 design 5.2 对齐）：
//! - **内存哈希字典**：`DashMap<term, Vec<docid>>` 收集增量写入；
//! - **Append-Only 倒排段文件**：达阈值后整段刷盘 `inverted-{id}.seg`，
//!   段内每 term 的 posting 用 **RoaringBitmap** 序列化存储；
//! - **段清单 Manifest**（`inverted-manifest.json`）：记录段文件列表（新→旧），
//!   原子写（tmp + rename），杜绝 GC 崩溃风险（design 4.5）；
//! - **查询**：内存字典 ∪ 各段 posting 合并为 RoaringBitmap；
//! - 段 GC / 压缩（阶段 2 分层 GC）；删除标记（Tombstone）随文档引擎层处理。

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
/// 段文件版本。
const SEG_VERSION: u16 = 1;
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

/// 倒排索引：内存字典 + 磁盘段集合。
pub struct InvertedIndex {
    dir: PathBuf,
    /// term → 内存收集的 docid 列表。
    mem: DashMap<String, Vec<u64>>,
    /// 内存累计 docid 数（触发刷盘阈值判断）。
    mem_docids: AtomicU64,
    /// 段文件列表（新→旧，仅含文件名）。
    segments: Vec<String>,
    next_seg_id: u64,
    /// 刷盘阈值：内存累计 posting 达此值整段落盘。
    flush_threshold: u64,
}

impl InvertedIndex {
    /// 打开（或创建）倒排索引：加载 Manifest。
    pub fn open(dir: &Path, flush_threshold: u64) -> Result<Self> {
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
        info!("倒排索引打开: {} 个段，下一个 id={next_seg_id}", segments.len());
        Ok(Self {
            dir: dir.to_path_buf(),
            mem: DashMap::new(),
            mem_docids: AtomicU64::new(0),
            segments,
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
    pub fn flush_segment(&mut self) -> Result<()> {
        if self.mem.is_empty() {
            return Ok(());
        }
        let seg_id = self.next_seg_id;
        self.next_seg_id += 1;
        let path = self.dir.join(format!("{SEG_PREFIX}{seg_id:08}.seg"));

        // 序列化段内容（内存快照）
        let mut body = Vec::new();
        encode_varint(&mut body, self.mem.len() as u64);
        // 按 term 排序，保证段内确定性
        let mut terms: Vec<(String, Vec<u64>)> = self
            .mem
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        terms.sort_by(|a, b| a.0.cmp(&b.0));
        for (term, docids) in terms {
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

        // 更新 Manifest（原子）
        let fname = path.file_name().unwrap().to_string_lossy().to_string();
        self.segments.insert(0, fname.clone());
        self.persist_manifest()?;

        // 清空内存
        self.mem.clear();
        self.mem_docids.store(0, Ordering::Relaxed);
        info!("倒排刷盘完成: {fname}");
        Ok(())
    }

    fn persist_manifest(&self) -> Result<()> {
        let m = SegmentManifest { segments: self.segments.clone(), next_seg_id: self.next_seg_id };
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
    fn read_segment_posting(&self, seg: &str, term: &str) -> Result<RoaringBitmap> {
        let path = self.dir.join(seg);
        let data = std::fs::read(&path)?;
        if data.len() < 10 || &data[0..8] != SEG_MAGIC {
            return Err(Error::Corrupted(format!("倒排段魔数错误: {seg}")));
        }
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
        DIR.get_or_init(|| tempfile::tempdir().unwrap()).path().join(name)
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
}
