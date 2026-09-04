//! 范围扫描路径：scan_range* / scan_stream* / count_keys_* / scan_raw_range_with_seq
//! （全量与流式范围扫描、快照范围扫描、keys-only 计数），自 `read.rs` 按主题拆分而来。
//! 点查族（get / get_many* / get_bytes_at / sst_min_seq / 自由读函数）见 `read.rs`。

use crate::error::{Error, Result};
use crate::keys::{decode_docid, encode_docid};
use crate::sstable::SstReader;

use super::read::merge_candidate_bytes;
use super::*;

impl ColumnFamily {
    /// 范围扫描 [start, end]（闭区间，None 端无边界）：先收集 MemTable 与各 SST 候选，
    /// 以最大 seq 去重（最新覆盖，Tombstone 覆盖旧值），返回按 docid 升序的 (docid, value) 列表。
    /// O 项第①步：读路径 `&self` 化（范围扫描共用，配合 RwLock 读读并行）。
    pub fn scan_range(
        &self,
        start: Option<u64>,
        end: Option<u64>,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let start_key = start.map(|s| encode_docid(s).to_vec());
        let end_key = end.map(|e| encode_docid(e).to_vec());
        let rows = self.scan_raw_range(start_key.as_deref(), end_key.as_deref())?;
        let mut out = Vec::with_capacity(rows.len());
        for (k, v) in rows {
            out.push((
                decode_docid(&k).map_err(|_| Error::Corrupted("docid 解码失败".into()))?,
                v,
            ));
        }
        Ok(out)
    }

    /// 快照范围扫描 [start, end]（M 项，事务类查询优化 P0）：按快照 seq 过滤版本，
    /// 返回按 docid 升序的 (docid, value) 列表（快照点已删除的 key 跳过）。
    pub fn scan_range_at(
        &self,
        snapshot_seq: u64,
        start: Option<u64>,
        end: Option<u64>,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let start_key = start.map(|s| encode_docid(s).to_vec());
        let end_key = end.map(|e| encode_docid(e).to_vec());
        let mut out = Vec::new();
        self.scan_stream_at(snapshot_seq, start_key.as_deref(), end_key.as_deref(), None, None, |key, val| {
            let docid = decode_docid(key)
                .map_err(|_| Error::Corrupted("scan_at key 非 docid 编码".into()))?;
            out.push((docid, val.to_vec()));
            Ok(true)
        })?;
        Ok(out)
    }

    /// 原始字节键范围扫描（组合索引前缀查询使用）。返回升序 (key, value) 列表，Tombstone 已过滤。
    /// O 项第①步：原始键范围扫描读路径 `&self` 化（delta Merge-on-Read / 批量覆盖共用）。
    pub fn scan_raw_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // 候选收集：key → (seq, value)，value=None 表示 Tombstone
        let mut merged: std::collections::HashMap<Vec<u8>, (u64, Option<Vec<u8>>)> =
            std::collections::HashMap::new();

        // MemTable 扫描（含 Tombstone 覆盖）
        self.memtable.scan_range(start, end, |key, e| {
            merge_candidate_bytes(&mut merged, key.to_vec(), e.seq, e.value.clone());
        });

        // SST 范围扫描（Zone Map 剪枝已内置在 scan_range；Ex-8.2 先按段 key 范围跳过不相交段，
        // 免调 sst.scan_range 的线性索引走查开销）
        for sst in self.ssts.load().ssts.iter() {
            if !sst_intersects_window(sst, start, end) {
                continue;
            }
            sst.scan_range(start, end, |k, v, seq| {
                merge_candidate_bytes(&mut merged, k.to_vec(), seq, v.map(|x| x.to_vec()));
            })?;
        }

        let mut out: Vec<(Vec<u8>, Vec<u8>)> = merged
            .into_iter()
            .filter_map(|(key, (_seq, value))| value.map(|v| (key, v)))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// 流式范围扫描（M8-P10）：k-way merge（memtable 双缓冲 + 各 SST 有序源）按 key 升序
    /// 回调 `f(key, value)`——同 key 取最大 seq（最新版本），Tombstone 跳过。
    /// 回调返回 `bool`：true=继续，**false=提前终止**（M8-P11 游标续扫：取满页即停，
    /// 不再全扫计数 total）。内存 O(块) 不随扫描总量膨胀，语义与 `scan_raw_range` 一致。
    pub fn scan_stream<F: FnMut(&[u8], &[u8]) -> Result<bool>>(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        f: F,
    ) -> Result<()> {
        // 最新视图 = 快照 seq 无上限（取最大版本）
        self.scan_stream_at(u64::MAX, start, end, None, None, f)
    }

    /// P91：投影列流式扫描（最新视图）——语义同 `scan_stream`，但请求列非空时
    /// SST 端 PAX 块只解码请求列并输出**子集 JSON**（行式块/内存直通原 JSON）。
    /// 消费端须只读 `fields` 覆盖的列（请求列须含 WHERE 引用 + 分组 + 聚合全部字段）。
    pub fn scan_stream_fields<F: FnMut(&[u8], &[u8]) -> Result<bool>>(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        fields: Vec<String>,
        f: F,
    ) -> Result<()> {
        self.scan_stream_at(u64::MAX, start, end, None, Some(fields), f)
    }

    /// P1-E：带 Zone Map 字段级范围剪枝的流式扫描——与 `scan_stream` 语义一致，
    /// 额外在 SST 块级检查 `zone_pred` 范围，不相交块跳过（免 IO/解压）。
    pub fn scan_stream_with_zonepred<F: FnMut(&[u8], &[u8]) -> Result<bool>>(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        zone_pred: Option<crate::sstable::ZonePredicate>,
        f: F,
    ) -> Result<()> {
        // 最新视图 = 快照 seq 无上限（取最大版本）
        self.scan_stream_at(u64::MAX, start, end, zone_pred, None, f)
    }

    /// 快照范围流式扫描（M 项，事务类查询优化 P0）：同 key 多版本取
    /// **seq ≤ snapshot_seq 的最大版本**（对齐 `get_bytes_at` 快照语义）；
    /// 快照点前为删除（Tombstone）→ 跳过该 key。其余与 `scan_stream` 一致。
    pub fn scan_stream_at<F: FnMut(&[u8], &[u8]) -> Result<bool>>(
        &self,
        snapshot_seq: u64,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        zone_pred: Option<crate::sstable::ZonePredicate>,
        project: Option<Vec<String>>,
        mut f: F,
    ) -> Result<()> {
        // 源：memtable（immutable + mutable）+ SST。P72：memtable 迭代器借用内部 RwLock 读锁
        // 数据 → HRTB 闭包式（merge 主体在锁作用域内执行）；SST 借用 &self.ssts。
        self.memtable.with_iter_range(start, end, |mut mem_iters| {
            let mut sst_iters: Vec<crate::sstable::SstRangeIter> = Vec::new();
            let snap = self.ssts.load();
            for sst in snap.ssts.iter() {
                // Ex-8.2：scan 路径段级 key 范围剪枝——窗口不相交的段免建迭代器
                // （此前无条件对快照全部 SST 建迭代器，无层/文件粗筛）
                if !sst_intersects_window(sst, start, end) {
                    continue;
                }
                // Ex-8.6：文件最小行 seq > 快照 → 整段剪枝（段内无 ≤ 快照的行，快照贡献为空）
                if snapshot_seq != u64::MAX && self.sst_min_seq(sst)? > snapshot_seq {
                    continue;
                }
                let mut it = crate::sstable::SstRangeIter::new_cached(
                    sst,
                    start,
                    end,
                    std::sync::Arc::clone(&self.block_cache),
                )?;
                if let Some(ref zp) = zone_pred {
                    it.set_zone_pred(zp.clone());
                }
                // P91：投影列（PAX 块只解请求列；行式直通）——scan 只物化/输出所需列
                if let Some(fields) = project.clone() {
                    it.set_project_fields(fields);
                }
                sst_iters.push(it);
            }
            let mem_count = mem_iters.len();
            let total = mem_count + sst_iters.len();
            if total == 0 {
                return Ok(());
            }
            // 各源当前条目 (key, value, seq)
            let mut cur: Vec<Option<(Vec<u8>, Option<Vec<u8>>, u64)>> =
                Vec::with_capacity(total);
            for it in mem_iters.iter_mut() {
                cur.push(it.next().transpose()?);
            }
            for it in sst_iters.iter_mut() {
                cur.push(it.next().transpose()?);
            }
            // 7.98：小源数（≤4，典型 1 SST + memtable 全扫）线性取最小——heap push/pop
            // 每行 log K 结构开销（1100 万行全扫 ~1s）；线性逐源比较 K 次常数极小。
            // 源多（多 L0/层）仍用最小堆避免 O(N·K) 退化。
            if total <= 4 {
                loop {
                    // 找最小 key 的源
                    let mut min_src: Option<usize> = None;
                    for (i, c) in cur.iter().enumerate() {
                        if let Some((k, _, _)) = c {
                            match min_src {
                                None => min_src = Some(i),
                                Some(mi) => {
                                    if k < &cur[mi].as_ref().unwrap().0 {
                                        min_src = Some(i);
                                    }
                                }
                            }
                        }
                    }
                    let Some(i0) = min_src else { break };
                    let min_key = cur[i0].as_ref().unwrap().0.clone();
                    // Ex-8.1（demo range-window）：收集同 key 候选须**吞并同源连续多版本行**
                    // ——S 项 MemTable 多版本下，覆盖写未收敛刷盘会让同一源连续出现同 key 新旧两行；
                    // 仅取各源首行会把旧版本当下一个"新 key"再输出（收集 100 vs 流式 110）。
                    // frontier 循环：取走所有 key==min_key 的行取快照点前最大 seq，推进后若该源
                    // 仍指向 min_key（更旧版本）则继续吞并，直至全部源越过该 key。
                    let mut frontier: Vec<usize> = (0..total)
                        .filter(|i| matches!(&cur[*i], Some((k, _, _)) if *k == min_key))
                        .collect();
                    let mut best_seq = 0u64;
                    let mut best_val: Option<Vec<u8>> = None;
                    loop {
                        let mut nxt: Vec<usize> = Vec::new();
                        for i in frontier {
                            let (k, v, seq) = cur[i].take().unwrap();
                            debug_assert!(k == min_key, "同 key 归并");
                            if seq <= snapshot_seq && seq >= best_seq {
                                best_seq = seq;
                                best_val = v;
                            }
                            cur[i] = if i < mem_count {
                                mem_iters[i].next().transpose()?
                            } else {
                                sst_iters[i - mem_count].next().transpose()?
                            };
                            if matches!(&cur[i], Some((nk, _, _)) if *nk == min_key) {
                                nxt.push(i); // 同源更旧版本，下一轮吞并
                            }
                        }
                        if nxt.is_empty() {
                            break;
                        }
                        frontier = nxt;
                    }
                    if let Some(v) = best_val {
                        if let Some(limiter) = self.scan_limiter.lock().unwrap().as_mut() {
                            limiter.acquire(v.len() as u64)?;
                        }
                        if !f(min_key.as_slice(), &v)? {
                            break; // 提前终止
                        }
                    }
                }
                return Ok(());
            }
            // k-way merge 用最小堆（O(N log K)，避免每轮线性扫全部源 O(N·K)——K 大时不可接受）
            let mut heap: std::collections::BinaryHeap<std::cmp::Reverse<(Vec<u8>, usize)>> =
                std::collections::BinaryHeap::new();
            for (i, c) in cur.iter().enumerate() {
                if let Some((k, _, _)) = c {
                    heap.push(std::cmp::Reverse((k.clone(), i)));
                }
            }
            loop {
                let Some(std::cmp::Reverse((min_key, i0))) = heap.pop() else {
                    break; // 全部耗尽
                };
                // 收集所有 key == min_key 的源（同 key 候选），取快照点前最大 seq（最新可见版本）
                let mut to_advance: Vec<usize> = vec![i0];
                while let Some(std::cmp::Reverse((k, i))) = heap.peek() {
                    if *k == min_key {
                        to_advance.push(*i);
                        heap.pop();
                    } else {
                        break;
                    }
                }
                let mut best_seq = 0u64;
                let mut best_val: Option<Vec<u8>> = None;
                let mut advanced: Vec<usize> = Vec::new();
                // Ex-8.1（demo range-window）：吞并同源连续同 key 多版本行（同线性分支）
                let mut frontier = to_advance;
                loop {
                    let mut nxt: Vec<usize> = Vec::new();
                    for i in frontier {
                        let (k, v, seq) = cur[i].take().unwrap();
                        debug_assert!(k == min_key, "同 key 归并");
                        if seq <= snapshot_seq && seq >= best_seq {
                            best_seq = seq;
                            best_val = v;
                        }
                        cur[i] = if i < mem_count {
                            mem_iters[i].next().transpose()?
                        } else {
                            sst_iters[i - mem_count].next().transpose()?
                        };
                        advanced.push(i);
                        if matches!(&cur[i], Some((nk, _, _)) if *nk == min_key) {
                            nxt.push(i);
                        }
                    }
                    if nxt.is_empty() {
                        break;
                    }
                    frontier = nxt;
                }
                // 推进后的源重新入堆（其残余 key 已越过 min_key）；同一源多轮吞并只保留最终游标
                advanced.sort_unstable();
                advanced.dedup();
                for i in advanced {
                    if let Some((nk, _, _)) = &cur[i] {
                        heap.push(std::cmp::Reverse((nk.clone(), i)));
                    }
                }
                // 快照点前最新版本为 Tombstone → 该 key 在快照点已删除，不输出
                if let Some(v) = best_val {
                    // 导出共享后台 IO 限速（design 20.5）：扫描产出按字节 acquire——
                    // 扫描节奏受限 → SST 顺序读 IO 随节奏受限，与 Compaction 共享后台预算
                    if let Some(limiter) = self.scan_limiter.lock().unwrap().as_mut() {
                        limiter.acquire(v.len() as u64)?;
                    }
                    if !f(min_key.as_slice(), &v)? {
                        break; // 提前终止（游标续扫取满页）
                    }
                }
            }
            Ok(())
        })
    }

    /// 7.100 快速行计数（COUNT(*) 无 WHERE 快路径）——无删除位图过滤版本，等价于
    /// `count_keys_range_filtered(.., 永不跳过)`。
    pub fn count_keys_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<u64> {
        self.count_keys_range_filtered(start, end, &mut |_| false)
    }

    /// Ex-8.1：带 skip 谓词的 keys-only 计数（删除位图过滤用）。merge 版本语义同
    /// `count_keys_range`（同 key 取最新、Tombstone 跳过、同源多版本折叠）。
    pub fn count_keys_range_filtered(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        skip: &mut dyn FnMut(&[u8]) -> bool,
    ) -> Result<u64> {
        self.memtable.with_iter_range(start, end, |mut mem_iters| {
            let mut sst_iters: Vec<crate::sstable::SstRangeIter> = Vec::new();
            let snap = self.ssts.load();
            for sst in snap.ssts.iter() {
                // Ex-8.2：同 scan_stream_at，窗口不相交的段免建 keys-only 迭代器
                if !sst_intersects_window(sst, start, end) {
                    continue;
                }
                sst_iters.push(crate::sstable::SstRangeIter::new_keys_cached(
                    sst,
                    start,
                    end,
                    std::sync::Arc::clone(&self.block_cache),
                )?);
            }
            let mem_count = mem_iters.len();
            let total = mem_count + sst_iters.len();
            if total == 0 {
                return Ok(0u64);
            }
            let mut cur: Vec<Option<(Vec<u8>, Option<Vec<u8>>, u64)>> =
                Vec::with_capacity(total);
            for it in mem_iters.iter_mut() {
                cur.push(it.next().transpose()?);
            }
            for it in sst_iters.iter_mut() {
                cur.push(it.next().transpose()?);
            }
            let mut count = 0u64;
            loop {
                // 线性取最小（同 7.99 merge：源少时省 heap 结构开销）
                let mut min_src: Option<usize> = None;
                for (i, c) in cur.iter().enumerate() {
                    if let Some((k, _, _)) = c {
                        match min_src {
                            None => min_src = Some(i),
                            Some(mi) => {
                                if k < &cur[mi].as_ref().unwrap().0 {
                                    min_src = Some(i);
                                }
                            }
                        }
                    }
                }
                let Some(i0) = min_src else { break };
                let min_key = cur[i0].as_ref().unwrap().0.clone();
                let mut best_seq = 0u64;
                let mut best_put = false;
                // Ex-8.1（demo range-window）：同 scan merge，吞并同源连续同 key 多版本行，
                // 避免覆盖写未收敛刷盘时旧版本被重复计数（COUNT 语义与 scan/get 对齐）。
                let mut frontier: Vec<usize> = (0..total)
                    .filter(|i| matches!(&cur[*i], Some((k, _, _)) if *k == min_key))
                    .collect();
                loop {
                    let mut nxt: Vec<usize> = Vec::new();
                    for i in frontier {
                        let (k, v, seq) = cur[i].take().unwrap();
                        debug_assert!(k == min_key, "同 key 归并");
                        // P1-2（Tombstone 折叠修复）：取**最大 seq 版本**判定可见性——
                        // 旧实现"遇到任一 Some 即 best_put"，高 seq 的 None（删除墓碑）
                        // 被忽略 → 已删 key 仍被 keys-only 输出/计数（DELETE 后纯 id
                        // 窗口 / COUNT 可见性错误，非事务 Tombstone 路径实测暴露）。
                        if seq > best_seq {
                            best_seq = seq;
                            best_put = v.is_some();
                        }
                        cur[i] = if i < mem_count {
                            mem_iters[i].next().transpose()?
                        } else {
                            sst_iters[i - mem_count].next().transpose()?
                        };
                        if matches!(&cur[i], Some((nk, _, _)) if *nk == min_key) {
                            nxt.push(i);
                        }
                    }
                    if nxt.is_empty() {
                        break;
                    }
                    frontier = nxt;
                }
                if best_put && !skip(min_key.as_slice()) {
                    count += 1;
                }
            }
            Ok(count)
        })
    }

    /// Ex-8.3 Part B：keys-only 流式扫描（最新视图）——merge 版本语义同 `count_keys_range`
    /// （同源同 key 折叠、Tombstone 跳过），但**输出可见 key 序列**而非计数：SST 端走
    /// `SstRangeIter::new_keys_cached` 免值解码（+块缓存）；回调返回 false 提前终止
    /// （mysql 纯 id 窗口 / LIMIT 截断，内存 O(输出)）。
    pub fn scan_stream_keys<F: FnMut(&[u8]) -> Result<bool>>(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        mut f: F,
    ) -> Result<()> {
        self.memtable.with_iter_range(start, end, |mut mem_iters| {
            let mut sst_iters: Vec<crate::sstable::SstRangeIter> = Vec::new();
            let snap = self.ssts.load();
            for sst in snap.ssts.iter() {
                if !sst_intersects_window(sst, start, end) {
                    continue;
                }
                sst_iters.push(crate::sstable::SstRangeIter::new_keys_cached(
                    sst,
                    start,
                    end,
                    std::sync::Arc::clone(&self.block_cache),
                )?);
            }
            let mem_count = mem_iters.len();
            let total = mem_count + sst_iters.len();
            if total == 0 {
                return Ok(());
            }
            let mut cur: Vec<Option<(Vec<u8>, Option<Vec<u8>>, u64)>> = Vec::with_capacity(total);
            for it in mem_iters.iter_mut() {
                cur.push(it.next().transpose()?);
            }
            for it in sst_iters.iter_mut() {
                cur.push(it.next().transpose()?);
            }
            loop {
                let mut min_src: Option<usize> = None;
                for (i, c) in cur.iter().enumerate() {
                    if let Some((k, _, _)) = c {
                        match min_src {
                            None => min_src = Some(i),
                            Some(mi) => {
                                if k < &cur[mi].as_ref().unwrap().0 {
                                    min_src = Some(i);
                                }
                            }
                        }
                    }
                }
                let Some(i0) = min_src else { break };
                let min_key = cur[i0].as_ref().unwrap().0.clone();
                let mut best_seq = 0u64;
                let mut best_put = false;
                // 同 count：frontier 吞并同源连续同 key 多版本行（Ex-8.1 折叠）
                let mut frontier: Vec<usize> = (0..total)
                    .filter(|i| matches!(&cur[*i], Some((k, _, _)) if *k == min_key))
                    .collect();
                loop {
                    let mut nxt: Vec<usize> = Vec::new();
                    for i in frontier {
                        let (k, v, seq) = cur[i].take().unwrap();
                        debug_assert!(k == min_key, "同 key 归并");
                        // P1-2：同 count_keys_range——最大 seq 版本为 Tombstone 则跳过
                        if seq > best_seq {
                            best_seq = seq;
                            best_put = v.is_some();
                        }
                        cur[i] = if i < mem_count {
                            mem_iters[i].next().transpose()?
                        } else {
                            sst_iters[i - mem_count].next().transpose()?
                        };
                        if matches!(&cur[i], Some((nk, _, _)) if *nk == min_key) {
                            nxt.push(i);
                        }
                    }
                    if nxt.is_empty() {
                        break;
                    }
                    frontier = nxt;
                }
                if best_put {
                    if !f(min_key.as_slice())? {
                        return Ok(()); // 提前终止（LIMIT 截断）
                    }
                }
            }
            Ok(())
        })
    }

    /// 范围扫描并保留 seq 与 Tombstone（MVCC 快照 Delta 隔离用，M7-1）：
    /// 返回升序 `(key, seq, value)`，value=None 表示删除标记；每 key 仅保留最大 seq 版本。
    pub fn scan_raw_range_with_seq(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, u64, Option<Vec<u8>>)>> {
        let mut merged: std::collections::HashMap<Vec<u8>, (u64, Option<Vec<u8>>)> =
            std::collections::HashMap::new();
        self.memtable.scan_range(start, end, |key, e| {
            merge_candidate_bytes(&mut merged, key.to_vec(), e.seq, e.value.clone());
        });
        for sst in self.ssts.load().ssts.iter() {
            sst.scan_range(start, end, |k, v, seq| {
                merge_candidate_bytes(&mut merged, k.to_vec(), seq, v.map(|x| x.to_vec()));
            })?;
        }
        let mut out: Vec<(Vec<u8>, u64, Option<Vec<u8>>)> = merged
            .into_iter()
            .map(|(key, (seq, value))| (key, seq, value))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }
}

/// Ex-8.2：扫描窗口与段 key 范围相交判定（闭区间，None 端无边界）。
/// 段范围未知（key_range=None：空段/缺侧）→ 保守返回 true 不跳过；精确判定，零假阴性。
/// 复用于 scan_stream_at / count_keys_range_filtered / scan_raw_range 的逐 SST 迭代器
/// 构建前——窗口只与相交段交互，非相交段免建 SstRangeIter（免 L2 定位/索引读）。
fn sst_intersects_window(
    sst: &SstReader,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
) -> bool {
    let Some((min, max)) = sst.key_range() else {
        return true;
    };
    if let Some(s) = start {
        if max < s {
            return false;
        }
    }
    if let Some(e) = end {
        if min > e {
            return false;
        }
    }
    true
}
