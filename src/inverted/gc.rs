//! gc：倒排段 GC / Compaction（全量合并为单段 + FST 重建 + Manifest 原子更新）与触发阈值判断。
//! 重构自 src/inverted.rs 对应主题，行为零变化。

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::error::Result;
use crate::keys::encode_varlen;
use mmap_file::MmapFile;
use tracing::info;

use super::segment::{encode_posting_v6, parse_term_stats_at, SEG_MAGIC, SEG_PREFIX, SEG_VERSION};
use super::stats::merge_field_agg;
use super::{encode_varint, FieldAgg, InvertedIndex, Posting};


impl InvertedIndex {
    /// 全部磁盘段文件总字节数。
    pub fn segment_bytes(&self) -> u64 {
        let segs = self.segments.load(); // Ex-6.2：快照
        segs.iter()
            .filter_map(|s| std::fs::metadata(self.dir.join(s)).ok().map(|m| m.len()))
            .sum()
    }

    /// 是否需要 GC：开启（阈值 > 0）且段数 > 1 且总量 ≥ 阈值。
    pub fn should_gc(&self) -> bool {
        self.gc_threshold_bytes > 0
            && self.segments.load().len() > 1
            && self.segment_bytes() >= self.gc_threshold_bytes
    }

    /// P4-B：是否需要 delta FST GC——最后一段（最新 delta）FST 超过上限。
    /// 检查最新段 FST 文件大小，超过 `delta_fst_max_bytes` 即触发合并。
    pub fn should_delta_gc(&self) -> bool {
        if self.delta_fst_max_bytes == 0 {
            return false;
        }
        let segs = self.segments.load();
        if segs.len() <= 1 {
            return false;
        }
        // 新段列表首位（新→旧顺序，首位 = 最新 delta）
        let delta = &segs[0];
        // 最新段可能有单独的 .fst 文件或 .seg 文件
        let fst_name = delta.replace(".seg", ".fst");
        let fst_path = self.dir.join(&fst_name);
        if fst_path.exists() {
            if let Ok(meta) = std::fs::metadata(&fst_path) {
                return meta.len() >= self.delta_fst_max_bytes;
            }
        }
        // 无 FST（hash 引擎）→ 检查 seg 文件大小
        if let Ok(meta) = std::fs::metadata(self.dir.join(delta)) {
            return meta.len() >= self.delta_fst_max_bytes;
        }
        false
    }

    /// P4-B：设置 delta FST 大小上限（字节），0 = 禁用。
    pub fn set_delta_fst_max_bytes(&mut self, bytes: u64) {
        self.delta_fst_max_bytes = bytes;
    }

    /// 倒排文件 GC（design 5.2.2）：将全部段读取所有 Term 的最新 Bitmap，
    /// **重写为单个紧凑段**（临时文件 → fsync → 原子更新 Manifest → 删除旧段 + 旧 FST），
    /// 中途崩溃不丢数据（启动只加载 Manifest 中的段，孤儿段被忽略）。
    /// J 项（7.73）：改 `&self` + mutate 锁——后台 GC 线程与写路径 flush 并发安全
    /// （flush/gc 写 Manifest 与删文件互斥，防丢失更新）。
    /// P4-B：增强触发条件——also triggers when `should_delta_gc()` returns true.
    pub fn gc(&self) -> Result<GcReport> {
        // J 项：与 flush_segment 互斥（Manifest 写 / 删段文件序列化）
        let _mut = self.mutate.lock().unwrap();
        if !self.should_gc() && !self.should_delta_gc() {
            return Ok(GcReport {
                merged: 0,
                freed_bytes: 0,
                segment_count: self.segments.load().len(),
            });
        }
        // G 项：段合并 → posting 缓存失效（旧段 bitmap 过期）
        self.clear_posting_cache();
        // ① 读取全部段的所有 term 最新 posting（bitmap 合并天然去重）+ v5 统计载荷合并
        let mut map: std::collections::BTreeMap<String, (Posting, Vec<FieldAgg>)> =
            std::collections::BTreeMap::new();
        let segs = self.segments.load(); // Ex-6.2：快照
        for seg in segs.iter() {
            for (term, posting) in self.read_segment_terms(seg)? {
                let e = map
                    .entry(term.clone())
                    .or_insert_with(|| (Posting::new(), Vec::new()));
                e.0 |= posting;
                if let Some((data, entry)) = self.segment_posting_entry(seg, &term)? {
                    if let Some(ss) = parse_term_stats_at(&data, entry, Self::seg_ver(&data))? {
                        if e.1.len() < ss.len() {
                            e.1.resize(ss.len(), FieldAgg::new());
                        }
                        for (d, s) in e.1.iter_mut().zip(ss.iter()) {
                            merge_field_agg(d, s);
                        }
                    }
                }
            }
        }

        // ② 写新段（临时文件 → fsync → 原子 rename）
        let seg_id = self.next_seg_id.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{SEG_PREFIX}{seg_id:08}.seg"));
        let mut body = Vec::new();
        let mut term_offsets: Vec<(Vec<u8>, u64)> = Vec::new();
        encode_varint(&mut body, map.len() as u64);
        for (term, (bitmap, stats)) in &map {
            let file_offset = (SEG_MAGIC.len() + std::mem::size_of::<u16>() + body.len()) as u64;
            term_offsets.push((term.clone().into_bytes(), file_offset));
            // v6：64 位 posting；v4+：term + varint(段内 doc_count) + posting；v5+：doc_count 后统计载荷
            let bytes = encode_posting_v6(bitmap);
            encode_varlen(&mut body, term.as_bytes());
            encode_varint(&mut body, bitmap.len() as u64);
            encode_varint(&mut body, stats.len() as u64);
            for a in stats {
                body.extend_from_slice(&a.n.to_le_bytes());
                body.extend_from_slice(&a.sum.to_le_bytes());
                body.extend_from_slice(&a.min.to_le_bytes());
                body.extend_from_slice(&a.max.to_le_bytes());
            }
            encode_varlen(&mut body, &bytes);
        }
        let tmp = self.dir.join(format!("{SEG_PREFIX}{seg_id:08}.seg.tmp"));
        let mut out = std::fs::File::create(&tmp)?;
        std::io::Write::write_all(&mut out, SEG_MAGIC)?;
        std::io::Write::write_all(&mut out, &SEG_VERSION.to_le_bytes())?;
        std::io::Write::write_all(&mut out, &body)?;
        out.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        // Ex-8.13：GC 后台段写——记账 + 共享预算 acquire 节流（预算不足等待，防 GC 抢前台写带宽）
        self.account_written_budgeted(&path)?;
        let fname = path.file_name().unwrap().to_string_lossy().to_string();

        // ③ 编译新段 FST 字典（写临时文件 → rename）
        let new_dict = if self.engine == "fst" {
            Some(self.write_fst_dict(seg_id, &term_offsets)?)
        } else {
            None
        };

        // ④ 原子更新 Manifest（先 Manifest 后删旧文件，崩溃安全）
        let old_segments = self.segments.load_full(); // Ex-6.2：旧快照（删旧文件依据）
        let old_bytes: u64 = old_segments
            .iter()
            .filter_map(|s| std::fs::metadata(self.dir.join(s)).ok().map(|m| m.len()))
            .sum();
        // Ex-6.2/6.3：store 发布新段清单 + 新字典（释放旧映射 → Windows 可删旧文件）
        self.segments.store(Arc::new(vec![fname.clone()]));
        let mut new_dicts: HashMap<String, Arc<fst::Map<MmapFile>>> = HashMap::new();
        if let Some(m) = new_dict {
            new_dicts.insert(fname.clone(), Arc::new(m));
        }
        self.dicts.store(Arc::new(new_dicts));
        // G 补充：段数据映射同步替换（先发布新映射再删旧文件——Windows 已映射不可删）
        let mut new_files: HashMap<String, Arc<MmapFile>> = HashMap::new();
        if let Ok(mm) = MmapFile::open(&self.dir.join(&fname)) {
            new_files.insert(fname.clone(), Arc::new(mm));
        }
        self.data_files.store(Arc::new(new_files));
        self.persist_manifest()?;

        // ⑤ 删旧段与旧 FST（Ex-5.7：已发布新快照后旧映射被释放）
        for seg in old_segments.iter() {
            let _ = std::fs::remove_file(self.dir.join(seg));
            let _ = std::fs::remove_file(self.dir.join(seg.replace(".seg", ".fst")));
        }

        let freed_bytes = old_bytes.saturating_sub(self.segment_bytes());
        info!(
            "倒排 GC 完成: {} 段合并为 1（释放 {} 字节）",
            old_segments.len(),
            freed_bytes
        );
        Ok(GcReport {
            merged: old_segments.len(),
            freed_bytes,
            segment_count: 1,
        })
    }
}

/// 倒排 GC 结果报告。
#[derive(Debug, Clone, Copy)]
pub struct GcReport {
    /// 被合并的旧段数。
    pub merged: usize,
    /// 释放的磁盘字节数。
    pub freed_bytes: u64,
    /// GC 后的段数。
    pub segment_count: usize,
}
