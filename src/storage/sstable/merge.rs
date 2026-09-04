//! Compaction 合并执行内部（原 compaction.rs 按主题拆出）：`impl ColumnFamily` 上
//! 的执行/收尾方法（compact_merge / try_meta_only_compact / finalize_compact /
//! pick_gc_single / sel_has_merge_work / compression_level_for / write_rows / write_sst）
//! 与列族级选段自由函数（select_compaction_inputs / select_compaction_inputs_ex /
//! cap_by_size / select_inner）。
//! 对外入口与策略方法（compact / compact_filtered / compact_gc / needs_compact /
//! bottom_needs_compact / split_bottom_merge_work / sst_table_of / cooling_indices /
//! l0_count / compaction_urgency / l0_bytes / sst_bytes）留在同目录 `compaction.rs`。
//! 可见性：跨文件互调方法以最小 `pub(crate)` 提升；`column_family::tests` 直接引用的
//! 选段自由函数经 compaction.rs 的 `pub(crate) use super::merge::{...}` 保持原模块路径。

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tracing::info;

use crate::error::{Error, Result};
use crate::memtable::MemTable;
use crate::sstable::{SstFooter, SstReader, SstWriter, FLAG_DELETE};
use crate::storage::column_family::{
    key_table_id, BucketRow, ColumnFamily, CompactReport, SstSnapshot, SST_PREFIX,
};

impl ColumnFamily {
    /// 全量合并压实主体（sel 已由调用方选出）：读全部输入行 → 排序去重 → 位图过滤 → 重写。
    /// O 项第③步：`&self`（输入走快照 Arc，写输出独立文件，提交走 snapshot store）。
    pub(crate) fn compact_merge(
        &self,
        sel: &[usize],
        out_level: u32,
        drop_key: &dyn Fn(&[u8]) -> bool,
    ) -> Result<CompactReport> {
        let old_count = sel.len();
        // ① P80 修复：流式 k 路归并（代替全量 Vec 物化）
        // 为每个输入段读出所有 entries 排序 → 推入迭代器，用最小堆输出
        // 内存占用 O(k + 输出缓冲区)，不随输入规模线性增长，解决 GB 级合并卡死
        let snap = self.ssts.load();

        // 堆元素：Reverse((key, seq, value, 迭代器索引)) — BinaryHeap 是最大堆，Reverse 转最小堆
        use std::cmp::Reverse;
        // 堆排序：key 升序，同 key seq 降序（最新版本优先弹出）
        // 内层 Reverse<u64> 使同 key 时最大 seq 优先出堆
        let mut heap: std::collections::BinaryHeap<Reverse<(Vec<u8>, Reverse<u64>, Option<Vec<u8>>, usize)>> =
            std::collections::BinaryHeap::new();

        // 为每个输入段创建排序好的迭代器
        let mut iterators: Vec<std::boxed::Box<dyn Iterator<Item = (Vec<u8>, u64, Option<Vec<u8>>)>>> =
            Vec::with_capacity(sel.len());
        let mut total_input_entries = 0usize;
        for &sst_idx in sel {
            let mut entries = Vec::new();
            snap.ssts[sst_idx].iterate(|k, v, seq| {
                entries.push((k.to_vec(), seq, v.map(|x| x.to_vec())));
            })?;
            // 同一段内先排序（key 升序，同 key seq 降序 → 弹出时保证最大 seq 优先）
            entries.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
            total_input_entries += entries.len();
            let mut iter = entries.into_iter();
            if let Some(first) = iter.next() {
                heap.push(Reverse((first.0, Reverse(first.1), first.2, iterators.len())));
            }
            iterators.push(Box::new(iter));
        }

        // ② 流式处理：弹出最小 key → 分组同 key → 按 MVCC 保活规则过滤 → 直接输出
        // 不缓存全量到内存，内存占用恒定（仅当前分组 + 堆）
        let floor = self.mvcc_keep_floor.load(Ordering::Acquire);
        let mut total_kept_keys = 0u64;
        let mut dropped_keys = 0u64;
        let mut current_tbl: Option<u16> = None;
        let mut current_writer: Option<SstWriter> = None;
        let mut out_paths: Vec<std::path::PathBuf> = Vec::new();

        // 开始新表输出段（split_by_table 时表边界切分文件）
        let start_new_table = |this: &Self, out_level: u32| -> Result<(SstWriter, PathBuf)> {
            let sst_id = this.next_sst_id.fetch_add(1, Ordering::Relaxed);
            let path = this.dir.join(format!("{SST_PREFIX}{sst_id:08}.sst"));
            let w = SstWriter::new_with_pax(
                &path,
                this.compression,
                this.compression_level_for(out_level),
                this.block_size,
                0, // 流式不知道总行数，0 走动态分配
                &this.pax_hot_fields,
                this.bloom_fpr,
            )?;
            Ok((w, path))
        };

        // 流式弹出 + 分组处理：同 key 全部 entries 缓冲后一次处理输出
        let mut buffer: Vec<(Vec<u8>, u64, Option<Vec<u8>>)> = Vec::new();
        // P80 修复：初始必须创建一个 writer（即使 split_by_table 关闭，也需要输出）
        if self.split_by_table {
            // 按表切分：初始 writer 为 None，第一个 key 输出时创建；
            // 如果 heap 为空（没有输出）则保持 None 不创建。
        } else {
            // 不按表切分：整个合并输出到单个文件，提前创建 writer
            let (w, path) = start_new_table(self, out_level)?;
            current_tbl = None;
            current_writer = Some(w);
            out_paths.push(path);
        }
        while !heap.is_empty() {
            // 弹出当前全局最小 key
            let Reverse((current_key, Reverse(current_seq), current_val, iter_idx)) = heap.pop().unwrap();

            // 缓冲当前 entry 到分组
            if buffer.is_empty() || buffer.last().unwrap().0 == current_key {
                buffer.push((current_key, current_seq, current_val));
            } else {
                // 完成上一个 key 分组：按 MVCC 规则处理并输出
                let group_key = buffer[0].0.clone();
                let deleted = drop_key(&group_key);
                let latest_seq = buffer[0].1; // 已按 seq 降序，第一个 = 最大

                if !(deleted && !(floor > 0 && latest_seq > floor)) {
                    // 需要输出至少一个版本
                    let mut pushed_floor = false;
                    for (k, seq, v) in &buffer {
                        if floor > 0 && latest_seq > floor {
                            if *seq > floor {
                                // 活跃快照保活：seq > floor → 输出
                                if let Some(tbl_id) = key_table_id(k) {
                                    if self.split_by_table && current_tbl != Some(tbl_id) {
                                        // 表边界：结束当前段，开新段
                                        if let Some(mut w) = current_writer.take() {
                                            w.finish()?;
                                            if !out_paths.is_empty() {
                                                self.io_acquire(out_paths.last().unwrap())?;
                                            }
                                        }
                                        let (w, path) = start_new_table(self, out_level)?;
                                        current_tbl = Some(tbl_id);
                                        current_writer = Some(w);
                                        out_paths.push(path);
                                    }
                                }
                                let w = current_writer.as_mut().ok_or_else(|| {
                                    Error::Config("没有当前 writer 输出复合键".into())
                                })?;
                                match v {
                                    Some(val) => w.add(k, val, *seq)?,
                                    None => w.add_tombstone(k, *seq)?,
                                }
                                total_kept_keys += 1;
                            } else if !pushed_floor {
                                // seq ≤ floor → 只输出一个最新
                                // M3：表边界切分（split_by_table 时检测表切换）
                                if let Some(tbl_id) = key_table_id(k) {
                                    if self.split_by_table && current_tbl != Some(tbl_id) {
                                        if let Some(mut w) = current_writer.take() {
                                            w.finish()?;
                                            if !out_paths.is_empty() {
                                                self.io_acquire(out_paths.last().unwrap())?;
                                            }
                                        }
                                        let (w, path) = start_new_table(self, out_level)?;
                                        current_tbl = Some(tbl_id);
                                        current_writer = Some(w);
                                        out_paths.push(path);
                                    }
                                }
                                // P80 修复：首次输出时 writer 为 None → 创建
                                if current_writer.is_none() {
                                    let (w, path) = start_new_table(self, out_level)?;
                                    current_tbl = key_table_id(k);
                                    current_writer = Some(w);
                                    out_paths.push(path);
                                }
                                let w = current_writer.as_mut().ok_or_else(|| {
                                    Error::Config("没有当前 writer 输出复合键".into())
                                })?;
                                match v {
                                    Some(val) => w.add(k, val, *seq)?,
                                    None => w.add_tombstone(k, *seq)?,
                                }
                                pushed_floor = true;
                                total_kept_keys += 1;
                            }
                        } else {
                            // 无保活需求 → 只输出最新版本
                            // M3：表边界切分（split_by_table 时检测表切换）
                            if let Some(tbl_id) = key_table_id(k) {
                                if self.split_by_table && current_tbl != Some(tbl_id) {
                                    if let Some(mut w) = current_writer.take() {
                                        w.finish()?;
                                        if !out_paths.is_empty() {
                                            self.io_acquire(out_paths.last().unwrap())?;
                                        }
                                    }
                                    let (w, path) = start_new_table(self, out_level)?;
                                    current_tbl = Some(tbl_id);
                                    current_writer = Some(w);
                                    out_paths.push(path);
                                }
                            }
                            // P80 修复：首次输出时 writer 为 None → 创建
                            if current_writer.is_none() {
                                let (w, path) = start_new_table(self, out_level)?;
                                current_tbl = key_table_id(k);
                                current_writer = Some(w);
                                out_paths.push(path);
                            }
                            let w = current_writer.as_mut().ok_or_else(|| {
                                Error::Config("没有当前 writer 输出复合键".into())
                            })?;
                            match v {
                                Some(val) => w.add(k, val, *seq)?,
                                None => w.add_tombstone(k, *seq)?,
                            }
                            total_kept_keys += 1;
                            break; // 只输出最新
                        }
                    }
                } else {
                    dropped_keys += 1;
                }

                // 开始新分组
                buffer.clear();
                buffer.push((current_key, current_seq, current_val));
            }

            // 从原迭代器取下一个元素，推回堆
            if let Some(next) = iterators[iter_idx].next() {
                heap.push(Reverse((next.0, Reverse(next.1), next.2, iter_idx)));
            }
        }

        // 处理最后一个 key 分组
        if !buffer.is_empty() {
            let group_key = buffer[0].0.clone();
            let deleted = drop_key(&group_key);
            let latest_seq = buffer[0].1;

            if !(deleted && !(floor > 0 && latest_seq > floor)) {
                    let mut pushed_floor = false;
                    for (k, seq, v) in &buffer {
                        if floor > 0 && latest_seq > floor {
                            if *seq > floor {
                                if let Some(tbl_id) = key_table_id(k) {
                                    // P80 修复：split_by_table 场景，第一个 key 输出时 writer 还没创建
                                    if current_writer.is_none() {
                                        let (w, path) = start_new_table(self, out_level)?;
                                        current_tbl = Some(tbl_id);
                                        current_writer = Some(w);
                                        out_paths.push(path);
                                    }
                                    if self.split_by_table && current_tbl != Some(tbl_id) {
                                        if let Some(mut w) = current_writer.take() {
                                            w.finish()?;
                                            if !out_paths.is_empty() {
                                                self.io_acquire(out_paths.last().unwrap())?;
                                            }
                                        }
                                        let (w, path) = start_new_table(self, out_level)?;
                                        current_tbl = Some(tbl_id);
                                        current_writer = Some(w);
                                        out_paths.push(path);
                                    }
                                }
                                // P80 修复：非 docid 键首次输出时 writer 仍为 None → 创建
                                if current_writer.is_none() {
                                    let (w, path) = start_new_table(self, out_level)?;
                                    current_writer = Some(w);
                                    out_paths.push(path);
                                }
                                let w = current_writer.as_mut().ok_or_else(|| {
                                    Error::Config("没有当前 writer 输出复合键".into())
                                })?;
                            match v {
                                Some(val) => w.add(k, val, *seq)?,
                                None => w.add_tombstone(k, *seq)?,
                            }
                            total_kept_keys += 1;
                        } else if !pushed_floor {
                            // M3：表边界切分（split_by_table 时检测表切换）
                            if let Some(tbl_id) = key_table_id(k) {
                                if self.split_by_table && current_tbl != Some(tbl_id) {
                                    if let Some(mut w) = current_writer.take() {
                                        w.finish()?;
                                        if !out_paths.is_empty() {
                                            self.io_acquire(out_paths.last().unwrap())?;
                                        }
                                    }
                                    let (w, path) = start_new_table(self, out_level)?;
                                    current_tbl = Some(tbl_id);
                                    current_writer = Some(w);
                                    out_paths.push(path);
                                }
                            }
                            // P80 修复：首次输出时 writer 为 None → 创建
                            if current_writer.is_none() {
                                let (w, path) = start_new_table(self, out_level)?;
                                current_tbl = key_table_id(k);
                                current_writer = Some(w);
                                out_paths.push(path);
                            }
                            let w = current_writer.as_mut().ok_or_else(|| {
                                Error::Config("没有当前 writer 输出复合键".into())
                            })?;
                            match v {
                                Some(val) => w.add(k, val, *seq)?,
                                None => w.add_tombstone(k, *seq)?,
                            }
                            pushed_floor = true;
                            total_kept_keys += 1;
                        }
                    } else {
                            // P80 修复：首次输出时 writer 为 None → 创建
                            if current_writer.is_none() {
                                let (w, path) = start_new_table(self, out_level)?;
                                current_writer = Some(w);
                                out_paths.push(path);
                            }
                            // 表切分边界检查（同 non-floor 分支）
                            if let Some(tbl_id) = key_table_id(k) {
                                if self.split_by_table && current_tbl != Some(tbl_id) {
                                    if let Some(mut w) = current_writer.take() {
                                        w.finish()?;
                                        if !out_paths.is_empty() {
                                            self.io_acquire(out_paths.last().unwrap())?;
                                        }
                                    }
                                    let (w, path) = start_new_table(self, out_level)?;
                                    current_tbl = Some(tbl_id);
                                    current_writer = Some(w);
                                    out_paths.push(path);
                                }
                            }
                            let w = current_writer.as_mut().ok_or_else(|| {
                                Error::Config("没有当前 writer 输出复合键".into())
                            })?;
                        match v {
                            Some(val) => w.add(k, val, *seq)?,
                            None => w.add_tombstone(k, *seq)?,
                        }
                        total_kept_keys += 1;
                    }
                }
            } else {
                dropped_keys += 1;
            }
        }

        // 结束最后一个 writer
        if let Some(mut w) = current_writer.take() {
            w.finish()?;
            if !out_paths.is_empty() {
                self.io_acquire(out_paths.last().unwrap())?;
            }
        } else if out_paths.is_empty() {
            // 所有键都被丢弃 → 仍写一个空段保持层结构不变
            let (w, path) = start_new_table(self, out_level)?;
            w.finish()?;
            self.io_acquire(&path)?;
            out_paths.push(path);
        }

        let kept_before_grouping = total_input_entries as u64;

        self.finalize_compact(
            sel,
            &out_paths,
            out_level,
            old_count,
            total_kept_keys as usize,
            kept_before_grouping.saturating_sub(total_kept_keys) as usize,
            dropped_keys as usize,
        )
    }

    /// Ex-5.8 元数据-数据解耦：无重叠输入段合并时**数据块级复用**——各段数据块区原样顺序
    /// 拼接（零解压零重压缩），只重建 Block Index/分区布隆/Footer 元数据区。
    /// 前提：行式列族（pax_hot_fields 为空）+ 相邻段 key 无重叠（前段 max < 后段 min）。
    /// 收益：Compaction 读放大归零、压缩 CPU 免除（demo 实测全量重写 4041ms vs 块级复用毫秒级）。
    /// 返回 Some(report) = 已复用完成；None = 条件不满足（调用方回退全量合并）。
    /// O 项第③步：`&self`（快照读输入段）。
    pub(crate) fn try_meta_only_compact(
        &self,
        sel: &[usize],
        out_level: u32,
    ) -> Result<Option<CompactReport>> {
        if !self.pax_hot_fields.is_empty() {
            return Ok(None); // PAX 列族：块内字段 Zone Map 无法重建，回退全量
        }
        let snap = self.ssts.load();
        // Ex-8.12：分层启用时**跨档合并禁块级复用**——块级复用原样保留输入块压缩档；
        // L0/L1 热档段下沉 L2（或 L2 冷档段回升热档层）必须全量重压缩，否则输出层档位
        // 语义不成立。仅当所有输入段与输出层同档（同为 L2 冷档 / 同为 L0/L1 热档）可复用。
        if self.compression_level_l2 > 0 {
            let out_cold = out_level >= 2;
            for &i in sel {
                let in_lvl = snap.levels.get(i).copied().unwrap_or(0);
                if (in_lvl >= 2) != out_cold {
                    return Ok(None);
                }
            }
        }
        // 读取每段 key 范围 [min, max]（仅解码元数据索引，不碰数据块）
        let mut ranges: Vec<(usize, Vec<u8>, Vec<u8>)> = Vec::with_capacity(sel.len());
        for &i in sel {
            let idx = snap.ssts[i].index();
            let min = idx
                .first()
                .map(|e| e.first_key.clone())
                .unwrap_or_default();
            let max = idx
                .last()
                .map(|e| e.max_key.clone())
                .unwrap_or_default();
            ranges.push((i, min, max));
        }
        // M3（§26 多表）：表切分列族**仅同表段可块级复用**——块级复用把输入块原样拼进
        // **单个**输出文件；若输入跨表（或老格式混表段，段内 min/max 跨表），单文件输出
        // 会把多表混进一个文件，破坏"每文件单表"的文件路由前提 → 回退全量合并按表切分。
        if self.split_by_table {
            let mut common: Option<u16> = None;
            for &(_, ref mn, ref mx) in &ranges {
                match (key_table_id(mn), key_table_id(mx)) {
                    (Some(a), Some(b)) if a == b => match common {
                        Some(t) if t != a => return Ok(None),
                        _ => common = Some(a),
                    },
                    _ => return Ok(None), // 非 8 字节键 / 段内跨表 → 禁复用
                }
            }
        }
        // 按 min 排序，检查相邻段无重叠（重叠需全量合并保证覆盖/去重语义）
        ranges.sort_by(|a, b| a.1.cmp(&b.1));
        for w in ranges.windows(2) {
            if w[0].2 >= w[1].1 {
                return Ok(None);
            }
        }
        // 块级复用：按 key 序逐段原样拷贝数据块，重建元数据
        let old_count = sel.len();
        let sst_id = self.next_sst_id.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{SST_PREFIX}{sst_id:08}.sst"));
        let mut kept = 0u64;
        {
            let mut w = SstWriter::new_with_pax(
                &path,
                self.compression,
                self.compression_level,
                self.block_size,
                0,
                &self.pax_hot_fields,
                self.bloom_fpr,
            )?;
            for &(i, _, _) in &ranges {
                let entries = snap.ssts[i].index();
                for e in entries {
                    let (comp, raw) = snap.ssts[i].block_raw(&e)?;
                    w.add_raw_block(&raw, &comp)?;
                }
                kept += snap.ssts[i].footer().key_count;
            }
            w.finish()?;
        }
        self.io_acquire(&path)?;
        let rep =
            self.finalize_compact(sel, &[path], out_level, old_count, kept as usize, 0, 0)?;
        info!(
            "列族 [{}] 块级复用 Compaction 完成（Ex-5.8，零解压只重建元数据）: →L{out_level} 合并 {} 段，释放 {} 字节",
            self.name,
            old_count,
            rep.freed_bytes
        );
        Ok(Some(rep))
    }

    /// 压实收尾（④⑤）：原子发布新快照（旧段移除 + 新段插最前）→ 更新 Manifest →
    /// 删除旧段（换快照后再删，并发读持 Arc 句柄有效）→ 报告。
    /// M3：`paths` 支持多输出（表切分每表一个文件），全部按 `out_level` 插到快照最前。
    /// O 项第③步：`&self`（snapshot store 原子切换 + merge_round/cooldown 内部可变）。
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn finalize_compact(
        &self,
        sel: &[usize],
        paths: &[PathBuf],
        out_level: u32,
        old_count: usize,
        kept: usize,
        eliminated: usize,
        dropped: usize,
    ) -> Result<CompactReport> {
        // ④ 原子发布新快照（先 store 后删旧文件；读线程持旧快照 Arc 期间句柄有效）
        // P72：sst_mutate 互斥——与 flush（snapshot_insert）无 Engine 锁并发时防快照丢失更新
        let _g = self.sst_mutate.lock().unwrap();
        let cur = self.ssts.load();
        let old_bytes: u64 = sel
            .iter()
            .map(|&i| {
                std::fs::metadata(cur.ssts[i].path())
                    .map(|m| m.len())
                    .unwrap_or(0)
            })
            .sum();
        let mut old_ssts = Vec::new();
        let mut kept_ssts = Vec::new();
        let mut kept_levels = Vec::new();
        let mut removed = vec![false; cur.ssts.len()];
        for &i in sel {
            removed[i] = true;
        }
        for (i, sst) in cur.ssts.iter().enumerate() {
            if removed[i] {
                old_ssts.push(sst.clone());
            } else {
                kept_ssts.push(sst.clone());
                kept_levels.push(cur.levels[i]);
            }
        }
        // M3：多输出全部置前（同层、段间不重叠 → 相对顺序无版本语义影响）
        // Ex-8.11：compaction 每写一个新输出文件 → 累计写入字节（写放大实验数据源）
        for p in paths {
            let reader = SstReader::open_with_granularity(p, self.index_granularity)?;
            self.sst_written.fetch_add(reader.file_len(), Ordering::Relaxed);
            kept_ssts.insert(0, Arc::new(reader));
            kept_levels.insert(0, out_level);
        }
        let (layer_ranges, layer_indices, l0_table_ranges) = Self::build_layer_meta(&kept_ssts, &kept_levels);
        let sizes: Vec<u64> = kept_ssts.iter().map(|r| r.file_len()).collect();
        self.ssts.store(Arc::new(SstSnapshot {
            ssts: kept_ssts,
            levels: kept_levels,
            layer_ranges,
            layer_indices,
            l0_table_ranges,
            sizes,
        }));
        self.persist_manifest()?;

        // ⑤ 删除旧段（P73 修复：在 sst_mutate 锁内完成——persist_manifest 以磁盘扫描重建清单，
        // 若删旧段在锁外，flush 持锁 persist 时可能扫到未删旧段写入 manifest，随后旧段被删 →
        // manifest 悬空引用已删文件 → 重启加载失败。store→persist→remove 原子一致）。
        // 并发读仍持旧快照 Arc → 句柄有效，引用归零后实际释放。
        for r in &old_ssts {
            let _ = std::fs::remove_file(r.path());
        }
        drop(_g);
        let freed_bytes = old_bytes.saturating_sub(self.sst_bytes());
        let n_out = paths.len();
        if dropped > 0 {
            info!(
                "列族 [{}] Compaction 完成: →L{out_level} 合并 {} 段 → {n_out} 文件（保留 {} 键，位图物理丢弃 {dropped} 键，释放 {} 字节）",
                self.name,
                old_count,
                kept,
                freed_bytes
            );
        } else {
            info!(
                "列族 [{}] Compaction 完成: →L{out_level} 合并 {} 段 → {n_out} 文件（保留 {} 键，释放 {} 字节）",
                self.name,
                old_count,
                kept,
                freed_bytes
            );
        }
        // L 项：合并冷却——输出段 N 轮内不参与下一轮合并（防刚合并又合并的无谓重写）；
        // 推进 merge_round + 清理到期项（纯内存调度态，重启后冷却重置，无一致性影响）
        if self.compaction_cooldown > 0 {
            let round = self.merge_round.fetch_add(1, Ordering::Relaxed) + 1;
            let mut cd = self.cooldown.lock().unwrap();
            for p in paths {
                cd.insert(p.to_path_buf(), round + self.compaction_cooldown as u64);
            }
            cd.retain(|_, exp| *exp > round);
        }
        Ok(CompactReport {
            merged_ssts: old_count,
            kept_keys: eliminated,
            freed_bytes,
            out_level,
            dropped_keys: dropped,
        })
    }

    /// Ex-8.7：删除密度单段 GC 候选——最深层（max level）非冷却最大段优先（删除空间回收
    /// 潜力大）；该层全冷却时忽略冷却回退选择（保证排空最终收敛，宁可多读一次不漏回收）。
    pub(crate) fn pick_gc_single(&self, snap: &SstSnapshot) -> Option<(usize, u32)> {
        let cooling = self.cooling_indices();
        let best = |cooling: &std::collections::HashSet<usize>| -> Option<(usize, u32)> {
            let mut best: Option<(usize, u32, u64)> = None;
            for (i, lvl) in snap.levels.iter().enumerate() {
                if cooling.contains(&i) {
                    continue;
                }
                let sz = snap.sizes.get(i).copied().unwrap_or_else(|| snap.ssts[i].file_len());
                let cand = (i, *lvl, sz);
                best = Some(match best {
                    None => cand,
                    Some(b) => {
                        if cand.1 > b.1 || (cand.1 == b.1 && cand.2 > b.2) {
                            cand
                        } else {
                            b
                        }
                    }
                });
            }
            best.map(|(i, lvl, _)| (i, lvl))
        };
        match best(&cooling) {
            Some(pick) => Some(pick),
            None if !cooling.is_empty() => best(&std::collections::HashSet::new()),
            None => None,
        }
    }

    /// M3：选定段集是否含**合并价值**——存在同表 ≥2 段（跨段有重叠/需下沉去重）
    /// 或混表段（老格式，需全量合并按表切分）。跨表每表各 1 段 = 已按表收敛，无需重写。
    pub(crate) fn sel_has_merge_work(&self, sel: &[usize]) -> bool {
        if sel.len() < 2 {
            return false;
        }
        let mut counts: std::collections::HashMap<u16, usize> = std::collections::HashMap::new();
        for &i in sel {
            match self.sst_table_of(i) {
                Some(t) => *counts.entry(t).or_insert(0) += 1,
                None => return true, // 混表段 / 无范围：保守认为需全量合并
            }
        }
        counts.values().any(|&c| c >= 2)
    }

    /// Ex-8.12：按输出层选压缩级别——分层启用（`compression_level_l2`>0）时 L2+ 输出用
    /// 冷档高压缩率（L1→L2 下沉 / L2 内合并 / L2 单段重写），L0/L1 输出用热档（避免
    /// 中间层放大）；未启用恒热档（现行为）。flush 固定 L0（热档，不走本函数）。
    pub(crate) fn compression_level_for(&self, out_level: u32) -> i32 {
        if self.compression_level_l2 > 0 && out_level >= 2 {
            self.compression_level_l2
        } else {
            self.compression_level
        }
    }

    pub(crate) fn write_rows(&self, path: &Path, rows: &[BucketRow]) -> Result<SstFooter> {
        let mut w = SstWriter::new_with_pax(
            path,
            self.compression,
            self.compression_level,
            self.block_size,
            rows.len(),
            &self.pax_hot_fields,
            self.bloom_fpr,
        )?;
        for (k, v, flag, seq) in rows {
            if *flag == FLAG_DELETE {
                w.add_tombstone(k, *seq).expect("SST Tombstone 写入失败");
            } else {
                w.add(k, v.as_deref().unwrap_or(&[]), *seq)
                    .expect("SST 写入失败");
            }
        }
        w.finish()
    }

    /// 将 Immutable 落盘为 SST（Put 与 Tombstone 均落盘，保证跨 flush 删除一致）。
    pub(crate) fn write_sst(&self, path: &Path, imm: &MemTable) -> Result<SstFooter> {
        let mut w = SstWriter::new_with_pax(
            path,
            self.compression,
            self.compression_level,
            self.block_size,
            imm.len(),
            &self.pax_hot_fields,
            self.bloom_fpr,
        )?;
        imm.scan(|k, e| match &e.value {
            Some(v) => w.add(k, v, e.seq).expect("SST 写入失败"),
            None => w.add_tombstone(k, e.seq).expect("SST Tombstone 写入失败"),
        });
        w.finish()
    }
}

/// 选择本轮压实输入（design 4.5 二期 Leveled，M6-2）：
/// - L0 ≥ 2 段且 L1 文件数 < `limit` → 合并**仅 L0**，输出 L1（单次压实量 = 刷盘批次，有界）；
/// - L0 ≥ 2 段且 L1 已满 → 合并 L0 + 全部 L1 → L1（收敛 L1 文件数）；
/// - L0 空且 L1 > 1 → 合并全部 L1 → L2（压实下沉）；
/// - L0/L1 均空且 L2 > 1 → 合并 L2（异常残留收敛）。
/// 单段 L0（未达 2 段）不压实——等待更多刷盘批次，避免无收益重写。
///
/// Ex-5.9 冷热感知：L0 段数超过 `limit`（逼近写 Stall）且存在热度数据时，**优先合并最热的
/// `limit` 段**（热段先下沉 L1 聚合，热点读路径段数更快减少）；无热度数据维持全量合并。
///
/// 返回 `(选中段下标, 输出层)`；无需要压实的输入时返回 `(空, 0)`。
/// L 项：`cooling` = 冷却期段下标集合（合并冷却，新段 N 轮内**优先**不参与合并）。
/// 冷却为「软约束」：若冷却导致无可合并候选（候选 < 2），回退含冷却段——
/// 防止冷却挡住收敛（needs_compact 用原始计数触发时，硬排除会造成合并空转死循环）。
/// 既有测试/调用入口：6 参（Ex-8.11 触发器 0 = 现行为）。
pub(crate) fn select_compaction_inputs(
    levels: &[u32],
    level_limit: usize,
    heat: &[u64],
    cooling: &std::collections::HashSet<usize>,
    sizes: &[u64],
    max_input_bytes: u64,
) -> (Vec<usize>, u32) {
    select_compaction_inputs_ex(levels, level_limit, 0, 0, heat, cooling, sizes, max_input_bytes)
}

/// Ex-8.11：带 L1/L2 触发阈值的选段（l1/l2_trigger=0 = 现行为）。
pub(crate) fn select_compaction_inputs_ex(
    levels: &[u32],
    level_limit: usize,
    l1_trigger: usize,
    l2_trigger: usize,
    heat: &[u64],
    cooling: &std::collections::HashSet<usize>,
    sizes: &[u64],
    max_input_bytes: u64,
) -> (Vec<usize>, u32) {
    let (mut sel, out) = select_inner(levels, level_limit, l1_trigger, l2_trigger, heat, cooling);
    // 单次合并输入大小上限（仅 L0→L1：L0 允许重叠，分批安全；L1→L2 全选保证层内不重叠）
    if sel.len() >= 2 && out == 1 && max_input_bytes > 0 {
        sel = cap_by_size(sel, sizes, max_input_bytes);
    }
    if sel.len() >= 2 {
        return (sel, out);
    }
    // L 项回退：候选不足（冷却挡住收敛）→ 忽略冷却再选（保证收敛 / 防写 Stall）
    select_inner(
        levels,
        level_limit,
        l1_trigger,
        l2_trigger,
        heat,
        &std::collections::HashSet::new(),
    )
}

/// 分批合并：输入总大小超 `max` 时，从尾部（排序后 = 最冷/最大）移除段直到 ≤ max；
/// 至少保留 2 段（保证本轮有进展，剩余段由 worker 后续轮次收敛）。
pub(crate) fn cap_by_size(mut sel: Vec<usize>, sizes: &[u64], max: u64) -> Vec<usize> {
    if sel.len() <= 2 {
        return sel;
    }
    let total: u64 = sel.iter().map(|&i| sizes.get(i).copied().unwrap_or(0)).sum();
    if total <= max {
        return sel;
    }
    let mut acc = total;
    let mut keep = sel.len();
    for (k, &i) in sel.iter().enumerate().rev() {
        if keep <= 2 {
            break;
        }
        acc -= sizes.get(i).copied().unwrap_or(0);
        keep = k;
        if acc <= max {
            break;
        }
    }
    sel.truncate(keep.max(2));
    sel
}

fn select_inner(
    levels: &[u32],
    level_limit: usize,
    l1_trigger: usize,
    l2_trigger: usize,
    heat: &[u64],
    cooling: &std::collections::HashSet<usize>,
) -> (Vec<usize>, u32) {
    let l0: Vec<usize> = levels
        .iter()
        .enumerate()
        .filter(|(_, l)| **l == 0)
        .filter(|(i, _)| !cooling.contains(i))
        .map(|(i, _)| i)
        .collect();
    let l1: Vec<usize> = levels
        .iter()
        .enumerate()
        .filter(|(_, l)| **l == 1)
        .filter(|(i, _)| !cooling.contains(i))
        .map(|(i, _)| i)
        .collect();
    let l2: Vec<usize> = levels
        .iter()
        .enumerate()
        .filter(|(_, l)| **l >= 2)
        .filter(|(i, _)| !cooling.contains(i))
        .map(|(i, _)| i)
        .collect();
    if !l0.is_empty() {
        if l0.len() < 2 {
            return (Vec::new(), 0); // 单个 L0 段（或冷却后不足 2）：暂不压实（无收益重写）
        }
        if l1.len() < level_limit.max(1) {
            // Ex-5.9：L0 超阈值且存在热度 → 优先合并最热的 level_limit 段（热段先下沉 L1）
            if l0.len() > level_limit && heat.iter().any(|h| *h > 0) {
                let mut ranked = l0.clone();
                ranked.sort_by(|a, b| heat[*b].cmp(&heat[*a]).then(a.cmp(b)));
                ranked.truncate(level_limit.max(1));
                (ranked, 1)
            } else {
                (l0, 1)
            }
        } else {
            let mut sel = l0.clone();
            sel.extend(l1);
            (sel, 1)
        }
    } else if l1_trigger == 0 && l1.len() > 1 {
        (l1, 2)
    } else if l1_trigger > 0 && l1.len() >= l1_trigger {
        // Ex-8.11：延迟大合并——L0 空且 L1 攒够阈值 → 一次下沉 L2
        (l1, 2)
    } else if l1_trigger > 0 && !l1.is_empty() {
        (Vec::new(), 0) // 延迟模式：L1 未达阈值，等批次到齐（不提前收敛 L2）
    } else if l2_trigger == 0 && l2.len() > 1 {
        (l2, 2)
    } else if l2_trigger > 0 && l2.len() >= l2_trigger {
        (l2, 2) // Ex-8.11：L2 攒够阈值才收敛为单段
    } else {
        (Vec::new(), 0)
    }
}
