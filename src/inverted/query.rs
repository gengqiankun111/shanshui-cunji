//! query：term 查询（内存 + 多段合并）、doc_count / 分页快速路径、内存位图索引、遍历与预分片 Chunk。
//! 重构自 src/inverted.rs 对应主题，行为零变化。

use std::sync::Arc;

use crate::error::Result;
use crate::keys::decode_varlen;

use super::decode_varint;
use super::segment::{parse_posting_at, PostingCursor};
use super::{bitmap_shard, InvertedIndex, Posting};


impl InvertedIndex {
    /// 配置位图索引字段白名单并**全量重建**内存位图（design 5.2.4，M7-2）。
    /// 仅当白名单非空时才维护位图（默认关闭零开销）。
    pub fn with_bitmap_fields(&mut self, fields: &[String]) -> Result<()> {
        self.bitmap_fields = fields.iter().cloned().collect();
        if self.bitmap_fields.is_empty() {
            return Ok(());
        }
        // G 项：位图重建 → posting 缓存失效（位图查询路径变化）
        self.clear_posting_cache();
        // 全量重建：清空全部分片，遍历内存 + 各段 posting，命中白名单字段的 term 建内存位图
        for m in &self.bitmaps {
            m.lock().unwrap().clear();
        }
        for (term, posting) in self.iter_terms()? {
            let Some((field, value)) = term.split_once('=') else {
                continue;
            };
            if self.bitmap_fields.contains(field) {
                self.bitmaps[bitmap_shard(field)]
                    .lock()
                    .unwrap()
                    .entry(field.to_string())
                    .or_default()
                    .insert(value.to_string(), posting);
            }
        }
        Ok(())
    }

    /// 内存位图 COUNT（design 5.2.4，M7-2）：field=value 命中白名单 → 亚毫秒计数；否则 None。
    pub fn bitmap_count(&self, field: &str, value: &str) -> Option<u64> {
        let bm = self.bitmaps[bitmap_shard(field)].lock().unwrap();
        bm.get(field)?.get(value).map(|b| b.len())
    }

    /// Ex-9.1：字段是否配置内存位图（`bitmap_fields`，写路径同步维护 → O(1) 亚毫秒计数）。
    pub fn is_bitmap_field(&self, field: &str) -> bool {
        self.bitmap_fields.contains(field)
    }

    /// P131b：已声明位图索引字段列表（写路径 term 白名单合并用）。
    pub fn bitmap_fields(&self) -> Vec<String> {
        self.bitmap_fields.iter().cloned().collect()
    }

    /// 内存位图 AND（M7-2）：全部 term 命中白名单字段 → 交集位图（组合筛选快速路径）；否则 None。
    pub fn bitmap_and(&self, terms: &[&str]) -> Option<Posting> {
        let mut acc: Option<Posting> = None;
        for t in terms {
            let (field, value) = t.split_once('=')?;
            let bm = self.bitmaps[bitmap_shard(field)].lock().unwrap();
            let bitmap = bm.get(field)?.get(value)?;
            acc = Some(match acc {
                Some(a) => a & bitmap.clone(),
                None => bitmap.clone(),
            });
        }
        acc
    }

    /// 内存位图 GROUP BY（M7-2）：字段命中白名单 → 各值计数（按值字典序）；否则 None。
    pub fn bitmap_group_by(&self, field: &str) -> Option<Vec<(String, u64)>> {
        let bm = self.bitmaps[bitmap_shard(field)].lock().unwrap();
        let values = bm.get(field)?;
        let mut out: Vec<(String, u64)> =
            values.iter().map(|(v, b)| (v.clone(), b.len())).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Some(out)
    }

    /// 快照白名单字段的（值 → docid 位图）全集合——COUNT(DISTINCT) 词典快路径用
    /// （Engine 侧再做"窗口 ∩ 活跃集"判活）。组数超 `cap`（高基数，克隆放大失控）或
    /// 字段非白名单 → None（调用方回退权威扫描）。位图克隆后立即释放分片锁，迭代在
    /// 锁外进行（避免读路径长时间持倒排锁与写路径互等）。
    pub fn bitmap_field_snapshot(&self, field: &str, cap: usize) -> Option<Vec<(String, Posting)>> {
        let bm = self.bitmaps[bitmap_shard(field)].lock().unwrap();
        let values = bm.get(field)?;
        if values.len() > cap {
            return None;
        }
        let mut out: Vec<(String, Posting)> = values
            .iter()
            .map(|(v, b)| (v.clone(), b.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Some(out)
    }

    /// 查询 term：合并内存 posting 与各段 posting，返回 64 位 docid 位图（Posting）。
    /// G 项优化（design_extension 9.6）：① 白名单字段 term 直接返回全量内存位图（O(1)）；
    /// ② 非白名单 term 查 LRU 缓存（重复查询免段遍历 + posting 反序列化）。
    pub fn search(&self, term: &str) -> Result<Posting> {
        // ① 白名单字段 term → 内存位图（写路径同步维护，含已落盘段全量）
        if let Some((field, value)) = term.split_once('=') {
            if self.bitmap_fields.contains(field) {
                if let Some(b) = self.bitmaps[bitmap_shard(field)]
                    .lock()
                    .unwrap()
                    .get(field)
                    .and_then(|m| m.get(value))
                {
                    return Ok(b.clone());
                }
            }
        }
        // ② LRU 缓存命中（Ex-8.8 双区；位图浅拷贝返回）
        if let Some(cached) = self.posting_cache.lock().unwrap().get(term) {
            return Ok(cached.as_ref().clone());
        }
        let mut result = Posting::new();
        // 内存（最新）
        if let Some(docids) = self.mem.get(term) {
            result.extend(docids.iter().copied());
        }
        // 各段（新→旧，bitmap 合并天然去重）
        let segs = self.segments.load(); // Ex-6.2：读快照无锁
        for seg in segs.iter() {
            let posting = self.read_segment_posting(seg, term)?;
            result |= posting;
        }
        // ③ 未命中 → 段遍历反序列化后入缓存（下次同 term O(1)）
        self.posting_cache
            .lock()
            .unwrap()
            .put(term.to_string(), Arc::new(result.clone()));
        Ok(result)
    }

    /// 某 term 命中的文档数（COUNT 原子操作，design 5.17）。
    /// K 项（7.74）：惰性游标归并**精确去重**计数（跨段/内存重复 docid 合并）——
    /// 容器级按需解码，不收集 docid 列表。
    pub fn doc_count(&self, term: &str) -> Result<u64> {
        let mem_vals: Vec<u64> = self
            .mem
            .get(term)
            .map(|e| e.value().clone())
            .unwrap_or_default();
        let mut cursors: Vec<PostingCursor> = Vec::new();
        let segs = self.segments.load();
        for seg in segs.iter() {
            if let Some((data, entry)) = self.segment_posting_entry(seg, term)? {
                let ver = Self::seg_ver(&data);
                if ver >= 3 && ver <= 5 {
                    cursors.push(PostingCursor::new(&data, entry, ver)?);
                } else {
                    let bm = parse_posting_at(&data, entry, ver)?;
                    cursors.push(PostingCursor::from_bitmap(bm));
                }
            }
        }
        let mut count = 0u64;
        merge_distinct(&mem_vals, &mut cursors, |_| {
            count += 1;
            false
        })?;
        Ok(count)
    }

    /// Ex-9.1b：段级 TermMeta 计数载荷快速 COUNT——flush 时在每 term 条目写入
    /// `varint(段内 doc_count)`（段内 posting 为去重 docid 集合 → bitmap.len() 精确）；
    /// 此处 mem 去重计数 + 各段载荷求和 = O(段数)，免逐 docid 遍历（亚毫秒）。
    /// 前提：**全部命中段均为 v4**（含载荷）；任一段为老格式（v2/v3）→ Ok(None)，
    /// 调用方回退 `doc_count` 精确遍历。语义注：跨段重复 docid（同 docid 同 term 覆盖写入
    /// 分散多段）求和略高估——写入单调/后台 GC 收敛后段间无重叠即精确；覆盖写入高频场景
    /// 走 `doc_count`（精确去重）。
    pub fn doc_count_fast(&self, term: &str) -> Result<Option<u64>> {
        // mem（未刷盘）部分：term value 去重计数（内存小，HashSet 一次）
        let mut total = 0u64;
        if let Some(e) = self.mem.get(term) {
            let set: std::collections::HashSet<u64> = e.value().iter().copied().collect();
            total += set.len() as u64;
        }
        let segs = self.segments.load();
        for seg in segs.iter() {
            let Some((data, entry)) = self.segment_posting_entry(seg, term)? else {
                continue; // gc 并发删段：跳过（与 doc_count 一致）
            };
            let ver = Self::seg_ver(&data);
            if ver < 4 {
                return Ok(None); // 老段无计数载荷 → 整体回退精确遍历
            }
            // FST/linear entry 指向 term 起点：skip term → varint(doc_count)
            let mut cur = entry;
            let _t = decode_varlen(&data, &mut cur)?;
            total += decode_varint(&data, &mut cur)?;
        }
        Ok(Some(total))
    }

    /// 分页快速路径（K 项）：跨内存 + 各段 k-way merge，只解码 [offset, offset+limit)
    /// 覆盖的容器——大 posting（1600 万 docid）近页从全量反序列化（~3ms）降至窗口解码
    /// （~10µs，demo posting-chunk x211）。返回 (total, 窗口 docid 升序列表，已去重)。
    /// total 为各源头部基数之和（跨段重复 docid 未去重时为上界；后台 GC 收敛后精确）。
    pub fn search_paged(&self, term: &str, offset: u64, limit: u64) -> Result<(u64, Vec<u64>)> {
        // 内存 posting（小，升序）
        let mem_vals: Vec<u64> = self
            .mem
            .get(term)
            .map(|e| e.value().clone())
            .unwrap_or_default();
        let mut total = mem_vals.len() as u64;
        // 各段：v3–v5 → 惰性游标；v2/v6 → 全量解码包游标（兼容）
        let segs = self.segments.load();
        let mut cursors: Vec<PostingCursor> = Vec::new();
        for seg in segs.iter() {
            if let Some((data, entry)) = self.segment_posting_entry(seg, term)? {
                let ver = Self::seg_ver(&data);
                if ver >= 3 && ver <= 5 {
                    let c = PostingCursor::new(&data, entry, ver)?;
                    total += c.total();
                    cursors.push(c);
                } else {
                    let bm = parse_posting_at(&data, entry, ver)?;
                    total += bm.len();
                    cursors.push(PostingCursor::from_bitmap(bm));
                }
            }
        }
        if limit == 0 {
            return Ok((total, Vec::new()));
        }
        // 归并取窗口（去重后流，与 search 的 bitmap 语义一致）
        let mut out: Vec<u64> = Vec::new();
        let mut skipped = 0u64;
        merge_distinct(&mem_vals, &mut cursors, |docid| {
            if skipped < offset {
                skipped += 1;
                false
            } else {
                out.push(docid);
                out.len() as u64 >= limit
            }
        })?;
        Ok((total, out))
    }

    /// 遍历全部 term（内存 + 各段），合并出每个 term 的完整 posting 位图。
    /// 供聚合执行器（GROUP BY）与字典浏览使用；内存 term 合并天然去重。
    pub fn iter_terms(&self) -> Result<Vec<(String, Posting)>> {
        let mut map: std::collections::BTreeMap<String, Posting> =
            std::collections::BTreeMap::new();
        // 内存（最新）
        for entry in self.mem.iter() {
            let bitmap: Posting = entry.value().iter().copied().collect();
            let e = map.entry(entry.key().clone()).or_default();
            *e |= bitmap;
        }
        // 各段（新→旧）
        let segs = self.segments.load(); // Ex-6.2：读快照无锁
        for seg in segs.iter() {
            for (term, posting) in self.read_segment_terms(seg)? {
                let e = map.entry(term).or_default();
                *e |= posting;
            }
        }
        Ok(map.into_iter().collect())
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

    // ============ 预分片 Chunk（design 5.2.1，阶段 2）============

    /// 取 term 完整 posting 中**属于指定分片**的那段 Chunk Bitmap。
    /// `shard_count` 为（虚拟）分片数，`shard_id ∈ [0, shard_count)`。
    /// 分片一致性：与 `sharding::route` 共用 `hash64`（`virtual_shard = hash64(docid) % shard_count`）。
    ///
    /// 广播查询流程（design 9.2）：网关给每个分片只要求本片 Chunk → 各片本地算本片 DocId →
    /// 网关 `concatenate_chunks` 按序直拼（O(1)），无需跨片交集/并集。
    pub fn chunk_for_shard(
        &self,
        term: &str,
        shard_id: u32,
        shard_count: u32,
    ) -> Result<Posting> {
        assert!(shard_count > 0, "shard_count 必须 > 0");
        assert!(shard_id < shard_count, "shard_id 越界");
        let full = self.search(term)?;
        let mut chunk = Posting::new();
        for d in full.iter() {
            let vs = (crate::sharding::hash64(d) % shard_count as u64) as u32;
            if vs == shard_id {
                chunk.insert(d);
            }
        }
        Ok(chunk)
    }

    /// 网关侧按序直拼（design 5.2.1）：各分片 Chunk 互不相交，顺序 OR 即拼接（O(1) 合并开销）。
    pub fn concatenate_chunks(chunks: &[Posting]) -> Posting {
        let mut out = Posting::new();
        for c in chunks {
            out |= c.clone();
        }
        out
    }

    // ============ 倒排段 GC / Compaction（design 5.2.2 + 5.2.4⑤，阶段 2）============
}

/// 多源升序归并（K 项，分页/COUNT 共用）：内存 vals + 各段惰性游标，对每个**去重后**的
/// docid 调 `f(docid)`；`f` 返回 true 时停止（提前退出）。各源 docid 均升序。
fn merge_distinct(
    mem_vals: &[u64],
    cursors: &mut [PostingCursor],
    mut f: impl FnMut(u64) -> bool,
) -> Result<()> {
    let mut mem_next = mem_vals.first().copied();
    let mut mem_pos = 0usize;
    let mut nxt: Vec<Option<u64>> = Vec::with_capacity(cursors.len());
    for c in cursors.iter_mut() {
        nxt.push(c.next_docid()?);
    }
    let mut last = u64::MAX;
    loop {
        let mut best: Option<(u64, usize)> = None; // (值, 源；usize::MAX = 内存)
        if let Some(v) = mem_next {
            if best.map_or(true, |(b, _)| v < b) {
                best = Some((v, usize::MAX));
            }
        }
        for (i, v) in nxt.iter().enumerate() {
            if let Some(x) = v {
                if best.map_or(true, |(b, _)| *x < b) {
                    best = Some((*x, i));
                }
            }
        }
        let Some((val, src)) = best else { break };
        if val != last {
            last = val;
            if f(val) {
                return Ok(());
            }
        }
        // 推进源（重复 docid 也推进）
        if src == usize::MAX {
            mem_pos += 1;
            mem_next = mem_vals.get(mem_pos).copied();
        } else {
            nxt[src] = cursors[src].next_docid()?;
        }
    }
    Ok(())
}
