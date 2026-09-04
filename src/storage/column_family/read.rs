//! 点查读路径：get / get_bytes / get_many / get_many_fields / get_bytes_at（快照点查）/
//! sst_min_seq（快照剪枝记忆），及自由读函数（get_from_sst* / get_many*_from_sst /
//! merge_candidate_bytes / key_table_id）。
//! 范围扫描族（scan_range* / scan_stream* / count_keys_* / scan_raw_range_with_seq）已按主题
//! 拆至 [`scan`]（`super::scan`）。

use crate::blockcache::{BlockCache, BlockCacheKey};
use crate::bloom::BloomFilter;
use crate::error::Result;
use crate::keys::encode_docid;
use crate::sstable::{IndexEntry, SstReader};

use super::*;

impl ColumnFamily {
    /// 查询（主键点查，便捷封装）。返回 (value, seq)，已过滤 Tombstone。
    /// `&self`：读写分离读路径（Ex-7.1 PerCpuCounter / BlockCache 已内部同步）。
    /// P3-A：从 docid 提取 table_id（高 16 位），利用 L0 按表分组范围跳过非目标表 SST。
    pub fn get(&self, docid: u64) -> Result<Option<(Vec<u8>, u64)>> {
        let key = encode_docid(docid);
        if let Some(e) = self.memtable.get(&key) {
            return Ok(e.value.map(|v| (v, e.seq)));
        }
        let cache = Arc::clone(&self.block_cache);
        let snap = self.ssts.load();
        // P3-A：从 docid 提取 table_id（精确，非 docid 编码的 key 不适用）
        let tid = (docid >> 48) as u16;
        let key_bytes = key.as_slice();
        for (lv, idxs) in snap.layer_indices.iter().enumerate() {
            // 层级 Zone Map 粗筛（精确：层范围 = 层内各段范围并集）
            if let Some((lmin, lmax)) = &snap.layer_ranges[lv] {
                if key_bytes < lmin.as_slice() || key_bytes > lmax.as_slice() {
                    continue; // 整层跳过
                }
            }
            // P3-A：L0 按表分组范围——目标表在 L0 无数据则整层跳过
            if lv == 0 {
                if let Some(ref table_ranges) = snap.l0_table_ranges {
                    match table_ranges.get(&tid) {
                        Some((tmin, tmax)) => {
                            if key_bytes < tmin.as_slice() || key_bytes > tmax.as_slice() {
                                continue; // 该表在 L0 无此范围数据 → 整层跳过
                            }
                        }
                        None => continue, // 该 table_id 完全无 L0 SST → 整层跳过
                    }
                }
            }
            for &i in idxs {
                let sst = &snap.ssts[i];
                match get_from_sst(sst, &cache, key_bytes)? {
                    // 命中：最新版本。value=None 为 Tombstone → 视为不存在
                    Some((value, seq)) => return Ok(value.map(|v| (v, seq))),
                    None => continue, // 未命中该 SST，继续查更旧的
                }
            }
        }
        Ok(None)
    }

    /// 查询原始字节键。返回 (value, seq)，已过滤 Tombstone。
    /// R 项：按层遍历（L0→L1→L2，层序与"新→旧"一致）——每层先层级 Zone Map 粗筛
    /// （key 越出层范围 → 整层 O(1) 跳过，省逐段二分 + 布隆反序列化），层内逐段。
    /// 注意：非 docid 编码的 key 不适用 P3-A 的 L0 按表分组（因无法提取 table_id）。
    pub fn get_bytes(&self, key: &[u8]) -> Result<Option<(Vec<u8>, u64)>> {
        if let Some(e) = self.memtable.get(key) {
            return Ok(e.value.map(|v| (v, e.seq)));
        }
        let cache = Arc::clone(&self.block_cache);
        let snap = self.ssts.load();
        for (lv, idxs) in snap.layer_indices.iter().enumerate() {
            // 层级 Zone Map 粗筛（精确：层范围 = 层内各段范围并集）
            if let Some((lmin, lmax)) = &snap.layer_ranges[lv] {
                if key < lmin.as_slice() || key > lmax.as_slice() {
                    continue; // 整层跳过
                }
            }
            for &i in idxs {
                let sst = &snap.ssts[i];
                match get_from_sst(sst, &cache, key)? {
                    // 命中：最新版本。value=None 为 Tombstone → 视为不存在
                    Some((value, seq)) => return Ok(value.map(|v| (v, seq))),
                    None => continue, // 未命中该 SST，继续查更旧的
                }
            }
        }
        Ok(None)
    }

    /// 批量点查（N 项：借鉴 batch_get 建议落地，倒排/全文检索回表基础）。
    /// 语义与 `get_bytes` 一致：每 key 取最新版本（MemTable 优先，SST 新→旧首个命中即终），
    /// Tombstone 视为不存在；但一次处理多个 key——
    /// ① MemTable 批量命中（含 Tombstone 直接终结，跳过 SST 层）；
    /// ② 逐 SST：整文件布隆粗筛 → 逐 key 二分定位数据块 → 按块分组 →
    ///    每数据块仅读/解压/解码一次（块缓存复用），同块多 key 一次取出。
    /// 返回与输入 docids 顺序对齐的 `Vec<Option<(value, seq)>>`。
    pub fn get_many(&self, docids: &[u64]) -> Result<Vec<Option<(Vec<u8>, u64)>>> {
        let keys: Vec<Vec<u8>> = docids.iter().map(|d| encode_docid(*d).to_vec()).collect();
        let mut out: Vec<Option<(Vec<u8>, u64)>> = vec![None; keys.len()];
        // ① MemTable 批量（最新版本；Tombstone → 保持 None 并终结，不再查 SST）
        let mut remain: Vec<usize> = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            if let Some(e) = self.memtable.get(k) {
                if let Some(v) = e.value {
                    out[i] = Some((v, e.seq));
                }
            } else {
                remain.push(i);
            }
        }
        if remain.is_empty() {
            return Ok(out);
        }
        // ② 逐层（L0→L1→L2，层序与"新→旧"一致），层内逐 SST（新→旧），未命中 key 继续下沉
        let cache = Arc::clone(&self.block_cache);
        let snap = self.ssts.load();
        for (lv, idxs) in snap.layer_indices.iter().enumerate() {
            if remain.is_empty() {
                break;
            }
            // R 项：层级 Zone Map 粗筛——所有剩余 key 均越出层范围 → 整层跳过
            if let Some((lmin, lmax)) = &snap.layer_ranges[lv] {
                if remain
                    .iter()
                    .all(|&i| keys[i].as_slice() < lmin.as_slice() || keys[i].as_slice() > lmax.as_slice())
                {
                    continue;
                }
            }
            // P3-A：L0 按表分组范围——检查所有剩余 key 是否都越出各表范围 → 整层跳过
            if lv == 0 {
                if let Some(ref table_ranges) = snap.l0_table_ranges {
                    let all_out = remain.iter().all(|&i| {
                        let key = keys[i].as_slice();
                        match Self::table_id_from_key(key) {
                            Some(tid) => match table_ranges.get(&tid) {
                                Some((tmin, tmax)) => key < tmin.as_slice() || key > tmax.as_slice(),
                                None => true, // 该表无 L0 数据 → 这个 key out
                            },
                            None => false, // 无法提取 table_id → 保守不跳
                        }
                    });
                    if all_out {
                        continue; // 所有剩余 key 都 out → 整层跳过
                    }
                }
            }
            for &i in idxs {
                if remain.is_empty() {
                    break;
                }
                let hits = get_many_from_sst(&snap.ssts[i], &cache, &keys, &remain)?;
                let mut next = Vec::with_capacity(remain.len());
                for (j, &idx) in remain.iter().enumerate() {
                    match &hits[j] {
                        Some((Some(v), seq)) => out[idx] = Some((v.clone(), *seq)),
                        // Tombstone：该 SST 为最新版本且为删除 → 视为不存在
                        Some((None, _)) => {}
                        None => next.push(idx),
                    }
                }
                remain = next;
            }
        }
        Ok(out)
    }

    /// P87②：投影字段批量点查——语义与 `get_many` 一致（MemTable 优先 + SST 层
    /// 新→旧首个命中终结 / Tombstone 终止），但只返回每个 docid 的**请求字段值**：
    /// - MemTable / 行式块行：整行 JSON 按需字段提取（P86② 语义）；
    /// - PAX 块：`scan_block_for_keys_fields` 列解码直取（免整行 25 列重构）。
    /// 返回与输入 docids 顺序对齐的 `Vec<Option<(字段值列表, seq)>>`；外层 None =
    /// 该列族无此 key / 命中 Tombstone（调用方按 delete 语义处理，同 `get_many`）。
    pub fn get_many_fields(
        &self,
        docids: &[u64],
        fields: &[String],
    ) -> Result<Vec<Option<(Vec<Option<Vec<u8>>>, u64)>>> {
        let keys: Vec<Vec<u8>> = docids.iter().map(|d| encode_docid(*d).to_vec()).collect();
        let mut out: Vec<Option<(Vec<Option<Vec<u8>>>, u64)>> = vec![None; keys.len()];
        // ① MemTable 批量（字段从整行 JSON 提取；Tombstone → 保持 None 并终结）
        let mut remain: Vec<usize> = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            if let Some(e) = self.memtable.get(k) {
                if let Some(v) = e.value {
                    let vals = crate::sstable::extract_fields_from_json_row(&v, fields);
                    out[i] = Some((vals, e.seq));
                }
            } else {
                remain.push(i);
            }
        }
        if remain.is_empty() {
            return Ok(out);
        }
        // ② 逐层（L0→L1→L2），层内逐 SST（新→旧），未命中 key 继续下沉（同 get_many）
        let cache = Arc::clone(&self.block_cache);
        let snap = self.ssts.load();
        for (lv, idxs) in snap.layer_indices.iter().enumerate() {
            if remain.is_empty() {
                break;
            }
            if let Some((lmin, lmax)) = &snap.layer_ranges[lv] {
                if remain
                    .iter()
                    .all(|&i| keys[i].as_slice() < lmin.as_slice() || keys[i].as_slice() > lmax.as_slice())
                {
                    continue;
                }
            }
            if lv == 0 {
                if let Some(ref table_ranges) = snap.l0_table_ranges {
                    let all_out = remain.iter().all(|&i| {
                        let key = keys[i].as_slice();
                        match Self::table_id_from_key(key) {
                            Some(tid) => match table_ranges.get(&tid) {
                                Some((tmin, tmax)) => key < tmin.as_slice() || key > tmax.as_slice(),
                                None => true,
                            },
                            None => false,
                        }
                    });
                    if all_out {
                        continue;
                    }
                }
            }
            for &i in idxs {
                if remain.is_empty() {
                    break;
                }
                let hits = get_many_fields_from_sst(&snap.ssts[i], &cache, &keys, &remain, fields)?;
                let mut next = Vec::with_capacity(remain.len());
                for (j, &idx) in remain.iter().enumerate() {
                    match &hits[j] {
                        Some((Some(vals), seq)) => out[idx] = Some((vals.clone(), *seq)),
                        // Tombstone：该 SST 为最新版本且为删除 → 视为不存在
                        Some((None, _)) => {}
                        None => next.push(idx),
                    }
                }
                remain = next;
            }
        }
        Ok(out)
    }

    /// 快照读（design 4.7 二期 MVCC，M6-3）：返回 **seq ≤ `snapshot_seq`** 的最新版本。
    /// 遍历 MemTable + 全部 SST，取满足条件的最大 seq；该 seq 为 Tombstone 则视为不存在
    /// （快照点已删除；快照点之前的历史版本仍可见）。
    /// 局限：MemTable 仅保留每 key 最新版本，未刷盘覆盖的历史版本无法回读（多版本保留留后续）。
    pub fn get_bytes_at(
        &self,
        key: &[u8],
        snapshot_seq: u64,
    ) -> Result<Option<(Vec<u8>, u64)>> {
        let mut best: Option<(u64, Option<Vec<u8>>)> = None; // (seq, value)
        // S 项：MemTable 多版本——取 seq ≤ snapshot 的最新版本（未刷盘也可正确快照读）
        if let Some(e) = self.memtable.get_at(key, snapshot_seq) {
            if best.as_ref().map_or(true, |(s, _)| e.seq > *s) {
                best = Some((e.seq, e.value));
            }
        }
        let cache = Arc::clone(&self.block_cache);
        // R 项：按层遍历（快照语义取全部层中 seq ≤ snapshot 的最大版本）；层级粗筛跳过
        // key 越界的层（层范围 = 精确并集，不产生假阴性）。
        let snap = self.ssts.load();
        for (lv, idxs) in snap.layer_indices.iter().enumerate() {
            if let Some((lmin, lmax)) = &snap.layer_ranges[lv] {
                if key < lmin.as_slice() || key > lmax.as_slice() {
                    continue;
                }
            }
            for &i in idxs {
                // Ex-8.6：文件最小行 seq > 快照 → 整段剪枝（段内无 ≤ 快照的 put/Tombstone）
                if snapshot_seq != u64::MAX && self.sst_min_seq(&snap.ssts[i])? > snapshot_seq {
                    continue;
                }
                if let Some((value, seq)) = get_from_sst_at(&snap.ssts[i], &cache, key, snapshot_seq)?
                {
                    if best.as_ref().map_or(true, |(s, _)| seq > *s) {
                        best = Some((seq, value));
                    }
                }
            }
        }
        match best {
            Some((seq, Some(v))) => Ok(Some((v, seq))),
            Some((_, None)) => Ok(None), // 快照点已删除
            None => Ok(None),
        }
    }

    /// Ex-8.6：该文件**所有行**（put + Tombstone）的最小 seq——惰性 keys-only 推导 + 记忆。
    /// 快照读剪枝：`快照 < 文件最小行 seq` → 文件内既无 ≤ 快照的 put 也无 ≤ 快照的 Tombstone，
    /// 对快照视图贡献为空（含墓碑掩蔽），可整段跳过。未知文件首次调用做一次全文件 keys-only
    /// 扫描（一次性成本 ≈ COUNT 免值计数；重启后首次快照读自动重建，无需 manifest 扩展）。
    /// pub(crate)：scan.rs（scan_stream_at）快照读剪枝复用。
    pub(crate) fn sst_min_seq(&self, sst: &SstReader) -> Result<u64> {
        let p = sst.path();
        {
            let m = self.seq_min.read().unwrap();
            if let Some(v) = m.get(p) {
                return Ok(*v);
            }
        }
        let mut mn = u64::MAX;
        let mut it = crate::sstable::SstRangeIter::new_keys(sst, None, None)?;
        while let Some((_k, _v, seq)) = it.next().transpose()? {
            if seq < mn {
                mn = seq;
            }
        }
        let v = if mn == u64::MAX { u64::MAX } else { mn };
        self.seq_min.write().unwrap().insert(p.to_path_buf(), v);
        Ok(v)
    }
}

/// 同 key 候选合并：仅保留 seq 更大（更新）的版本；value=None 的 Tombstone 可覆盖旧值。
/// pub(crate)：scan.rs（scan_raw_range / scan_raw_range_with_seq）同 key 去重复用。
pub(crate) fn merge_candidate_bytes(
    merged: &mut std::collections::HashMap<Vec<u8>, (u64, Option<Vec<u8>>)>,
    key: Vec<u8>,
    seq: u64,
    value: Option<Vec<u8>>,
) {
    match merged.get(&key) {
        Some((old_seq, _)) if *old_seq >= seq => {}
        _ => {
            merged.insert(key, (seq, value));
        }
    }
}

/// 单 SST 等值查询（布隆剪枝 → 二分定位块 → 块缓存/读盘 → 块内扫描）。
/// 返回 `(value, seq)`：value=None 表示 Tombstone；整体 None 表示该 SST 无此 key。
fn get_from_sst(
    sst: &SstReader,
    cache: &BlockCache,
    key: &[u8],
) -> Result<Option<(Option<Vec<u8>>, u64)>> {
    // R 项：段级 Zone Map 粗筛——key 越出段范围 → O(1) 跳过（不做二分 + 布隆反序列化；
    // 精确判断，无假阴性）。
    if let Some((min, max)) = sst.key_range() {
        if key < min || key > max {
            return Ok(None);
        }
    }
    // v3/v4：整文件布隆粗筛
    if let Some(b) = sst.legacy_bloom() {
        if !b.maybe_contains(&key.to_vec()) {
            return Ok(None);
        }
    }
    // 等值定位块：借用精确索引二分，只克隆单条块条目（design 4.4.2 按需，避免克隆整个 Level 2）
    let Some((block_idx, entry)) = sst.locate_indexed_block(key)? else {
        return Ok(None);
    };
    // v5 分区布隆：只校验目标块（design 4.4.2，查询只加载目标块布隆）
    if let Some(pb) = sst.partition_blooms() {
        if let Some(bytes) = pb.get(block_idx) {
            if let Some(b) = BloomFilter::from_bytes(bytes) {
                if !b.maybe_contains(&key.to_vec()) {
                    return Ok(None);
                }
            }
        }
    }
    // Ex-5.9：布隆放行（真正读块）→ 读热度 +1（冷热感知 Compaction 数据源）
    sst.touch();
    let tid = sst.table_id().unwrap_or(0);
    let ck = BlockCacheKey::new(sst.path().to_path_buf(), entry.offset, tid);
    let block = if let Some(b) = cache.get(&ck) {
        b
    } else {
        let b = sst.read_block(&entry)?;
        cache.put(ck, b.clone());
        b
    };
    sst.scan_block_for_key(&block, key)
}

/// 单 SST 快照等值查询（S 项）：同 `get_from_sst`，但返回 **seq ≤ snapshot_seq** 的
/// 最大版本（块内多版本过滤；Tombstone value=None 保留）。整体 None = 该 SST 无 ≤ 快照版本。
fn get_from_sst_at(
    sst: &SstReader,
    cache: &BlockCache,
    key: &[u8],
    snapshot_seq: u64,
) -> Result<Option<(Option<Vec<u8>>, u64)>> {
    // R 项：段级 Zone Map 粗筛（同 get_from_sst）
    if let Some((min, max)) = sst.key_range() {
        if key < min || key > max {
            return Ok(None);
        }
    }
    if let Some(b) = sst.legacy_bloom() {
        if !b.maybe_contains(&key.to_vec()) {
            return Ok(None);
        }
    }
    let Some((block_idx, entry)) = sst.locate_indexed_block(key)? else {
        return Ok(None);
    };
    if let Some(pb) = sst.partition_blooms() {
        if let Some(bytes) = pb.get(block_idx) {
            if let Some(b) = BloomFilter::from_bytes(bytes) {
                if !b.maybe_contains(&key.to_vec()) {
                    return Ok(None);
                }
            }
        }
    }
    sst.touch();
    let tid = sst.table_id().unwrap_or(0);
    let ck = BlockCacheKey::new(sst.path().to_path_buf(), entry.offset, tid);
    let block = if let Some(b) = cache.get(&ck) {
        b
    } else {
        let b = sst.read_block(&entry)?;
        cache.put(ck, b.clone());
        b
    };
    sst.scan_block_for_key_at(&block, key, snapshot_seq)
}

/// 单 SST 批量等值查询（N 项）：整文件布隆粗筛 → 逐 key 二分定位数据块 → 按块分组 →
/// 分区布隆校验 → 每块只读一次（块缓存/磁盘）→ 块内一次扫描命中全部 key。
/// 返回与 `idxs`（输入原始下标）对齐：`Some((value, seq))`（value=None = Tombstone）/
/// `None`（该 SST 无此 key，调用方继续下沉旧层）。
fn get_many_from_sst(
    sst: &SstReader,
    cache: &BlockCache,
    keys: &[Vec<u8>],
    idxs: &[usize],
) -> Result<Vec<Option<(Option<Vec<u8>>, u64)>>> {
    let mut out: Vec<Option<(Option<Vec<u8>>, u64)>> = vec![None; idxs.len()];
    if idxs.is_empty() {
        return Ok(out);
    }
    let legacy = sst.legacy_bloom();
    // R 项：段级 Zone Map 粗筛（取一次，逐 key 判断；越界 key O(1) 跳过）
    let seg_range = sst.key_range();
    // 定位：原始下标 + 块号 + 块条目（借用精确索引二分，只克隆单条条目）
    let mut located: Vec<(usize, usize, IndexEntry)> = Vec::new();
    for &i in idxs {
        let k = &keys[i];
        if let Some((min, max)) = seg_range {
            if k.as_slice() < min || k.as_slice() > max {
                continue;
            }
        }
        if let Some(b) = legacy {
            if !b.maybe_contains(k) {
                continue;
            }
        }
        if let Some((block_idx, entry)) = sst.locate_indexed_block(k)? {
            located.push((i, block_idx, entry));
        }
    }
    if located.is_empty() {
        return Ok(out);
    }
    // 按块分组（块号升序 → 块读取顺序化）
    located.sort_by_key(|&(_, bi, _)| bi);
    let slot_of: std::collections::HashMap<usize, usize> = idxs
        .iter()
        .enumerate()
        .map(|(slot, &i)| (i, slot))
        .collect();
    let mut pos = 0usize;
    while pos < located.len() {
        let block_idx = located[pos].1;
        let mut end = pos;
        while end < located.len() && located[end].1 == block_idx {
            end += 1;
        }
        // 分区布隆（v5）校验：放行的 key 才真正读块
        let mut targets: Vec<usize> = Vec::new();
        let mut pruned = false;
        if let Some(pb) = sst.partition_blooms() {
            if let Some(bytes) = pb.get(block_idx) {
                if let Some(b) = BloomFilter::from_bytes(bytes) {
                    for &(i, _, _) in &located[pos..end] {
                        if b.maybe_contains(&keys[i]) {
                            targets.push(i);
                        }
                    }
                    pruned = true;
                }
            }
        }
        if !pruned {
            targets.extend(located[pos..end].iter().map(|&(i, _, _)| i));
        }
        if !targets.is_empty() {
            // 布隆放行 → 读热度 +1（冷热感知 Compaction 数据源，与 get_from_sst 一致）
            sst.touch();
            let entry = located[pos].2.clone();
            let tid = sst.table_id().unwrap_or(0);
            let ck = BlockCacheKey::new(sst.path().to_path_buf(), entry.offset, tid);
            let block = if let Some(b) = cache.get(&ck) {
                b
            } else {
                let b = sst.read_block(&entry)?;
                cache.put(ck, b.clone());
                b
            };
            let target_set: std::collections::HashSet<Vec<u8>> =
                targets.iter().map(|&i| keys[i].clone()).collect();
            for (k, v, seq) in sst.scan_block_for_keys(&block, &target_set)? {
                if let Some(i) = targets.iter().copied().find(|&i| keys[i] == k) {
                    if let Some(&slot) = slot_of.get(&i) {
                        out[slot] = Some((v, seq));
                    }
                }
            }
        }
        pos = end;
    }
    Ok(out)
}

/// 单 SST 投影字段批量等值查询（P87②）：定位/分组/读块流程与 `get_many_from_sst`
/// 一致，但块内解码走 `scan_block_for_keys_fields`（PAX 列解码 / 行式按需提取）。
/// 返回与 `idxs` 对齐：`Some((Some(fields), seq))` = 命中（fields 为请求字段值列表）、
/// `Some((None, _))` = Tombstone、`None` = 该 SST 无此 key（调用方继续下沉）。
fn get_many_fields_from_sst(
    sst: &SstReader,
    cache: &BlockCache,
    keys: &[Vec<u8>],
    idxs: &[usize],
    fields: &[String],
) -> Result<Vec<Option<(Option<Vec<Option<Vec<u8>>>>, u64)>>> {
    let mut out: Vec<Option<(Option<Vec<Option<Vec<u8>>>>, u64)>> = vec![None; idxs.len()];
    if idxs.is_empty() {
        return Ok(out);
    }
    let legacy = sst.legacy_bloom();
    let seg_range = sst.key_range();
    let mut located: Vec<(usize, usize, IndexEntry)> = Vec::new();
    for &i in idxs {
        let k = &keys[i];
        if let Some((min, max)) = seg_range {
            if k.as_slice() < min || k.as_slice() > max {
                continue;
            }
        }
        if let Some(b) = legacy {
            if !b.maybe_contains(k) {
                continue;
            }
        }
        if let Some((block_idx, entry)) = sst.locate_indexed_block(k)? {
            located.push((i, block_idx, entry));
        }
    }
    if located.is_empty() {
        return Ok(out);
    }
    located.sort_by_key(|&(_, bi, _)| bi);
    let slot_of: std::collections::HashMap<usize, usize> = idxs
        .iter()
        .enumerate()
        .map(|(slot, &i)| (i, slot))
        .collect();
    let mut pos = 0usize;
    while pos < located.len() {
        let block_idx = located[pos].1;
        let mut end = pos;
        while end < located.len() && located[end].1 == block_idx {
            end += 1;
        }
        let mut targets: Vec<usize> = Vec::new();
        let mut pruned = false;
        if let Some(pb) = sst.partition_blooms() {
            if let Some(bytes) = pb.get(block_idx) {
                if let Some(b) = BloomFilter::from_bytes(bytes) {
                    for &(i, _, _) in &located[pos..end] {
                        if b.maybe_contains(&keys[i]) {
                            targets.push(i);
                        }
                    }
                    pruned = true;
                }
            }
        }
        if !pruned {
            targets.extend(located[pos..end].iter().map(|&(i, _, _)| i));
        }
        if !targets.is_empty() {
            sst.touch();
            let entry = located[pos].2.clone();
            let tid = sst.table_id().unwrap_or(0);
            let ck = BlockCacheKey::new(sst.path().to_path_buf(), entry.offset, tid);
            let block = if let Some(b) = cache.get(&ck) {
                b
            } else {
                let b = sst.read_block(&entry)?;
                cache.put(ck, b.clone());
                b
            };
            let target_set: std::collections::HashSet<Vec<u8>> =
                targets.iter().map(|&i| keys[i].clone()).collect();
            for (k, vals, seq) in sst.scan_block_for_keys_fields(&block, &target_set, fields)? {
                if let Some(i) = targets.iter().copied().find(|&i| keys[i] == k) {
                    if let Some(&slot) = slot_of.get(&i) {
                        out[slot] = Some((vals, seq));
                    }
                }
            }
        }
        pos = end;
    }
    Ok(out)
}

/// M3（§26 多表）：docid 定长键 → table_id（`docid >> 48`）。
/// 仅 8 字节定长 docid 键可解析（主数据列族）；组合键等不定长键返回 None（不切分）。
/// 字节序 == 数值序（keys 大端规范）→ 同表键在扫描序中天然连续，表边界即切分点。
pub(crate) fn key_table_id(key: &[u8]) -> Option<u16> {
    let docid = crate::keys::decode_docid(key).ok()?;
    Some((docid >> 48) as u16)
}
