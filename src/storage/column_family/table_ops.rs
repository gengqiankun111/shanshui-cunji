//! 表级操作：manifest 持久化 / 内存占用 / purge / DROP TABLE 文件回收 / WAL 状态 / 名称。

use std::sync::atomic::Ordering;

use tracing::info;

use crate::error::Result;
use crate::storage::manifest;

use super::*;

impl ColumnFamily {
    pub(crate) fn persist_manifest(&self) -> Result<()> {
        // P73 修复：用**内存快照**（ssts ArcSwap + levels）重建清单，**不扫描磁盘**——
        // 无锁合并（P72）后 flush（Engine 写锁内写段文件）与合并（worker 无锁 persist）
        // 并发，磁盘扫描会引用"正在写入、尚未写完"的半写段文件 → manifest 悬空引用
        // 半写段 → 重启加载失败（SST seek 越界）。内存快照与 ssts store 原子一致
        // （本函数在 sst_mutate 锁内调用：store→persist 原子）。
        let snap = self.ssts.load();
        let mut files: Vec<String> = Vec::with_capacity(snap.ssts.len());
        let mut level_by_file: std::collections::HashMap<String, u32> =
            std::collections::HashMap::new();
        for (sst, lv) in snap.ssts.iter().zip(&snap.levels) {
            if let Some(name) = sst.path().file_name() {
                let name = name.to_string_lossy().to_string();
                files.push(name.clone());
                level_by_file.insert(name, *lv);
            }
        }
        files.sort_by(|a, b| b.cmp(a));
        let levels: Vec<u32> = files
            .iter()
            .map(|f| level_by_file.get(f).copied().unwrap_or(0))
            .collect();
        manifest::save(
            &self.dir,
            &files,
            &levels,
            self.next_sst_id.load(Ordering::Relaxed),
        )
    }

    /// 当前内存占用（字节），供 OOM Guardian / 监控使用。
    pub fn memtable_bytes(&self) -> usize {
        self.memtable.mutable_bytes() + self.memtable.immutable_bytes()
    }

    /// 2026-09-05：块缓存当前占用（字节，含元数据粗算，见 blockcache used_bytes）。
    pub fn blockcache_bytes(&self) -> usize {
        self.block_cache.used_bytes()
    }

    /// 2026-09-05（P0 观测）：块缓存 (命中, 未命中, 容量淘汰) 计数。
    pub fn blockcache_stats(&self) -> (u64, u64, u64) {
        self.block_cache.cache_stats()
    }

    /// P90：列族是否完全无数据（内存双缓冲 + 磁盘段均空）——delta 列族"无 patch"判定。
    pub fn data_empty(&self) -> bool {
        if self.memtable_bytes() > 0 {
            return false;
        }
        let snap = self.ssts.load();
        snap.ssts.is_empty()
    }

    /// P90：块级 FieldZone 聚合（无 WHERE `COUNT(f)`/`SUM(f)` 下推候选）。
    /// 快照 eligible：无未刷盘数据（memtable 空）+ **单一非空层**（层内文件互不重叠——
    /// L1/L2 由 leveled 语义保证；L0 仅允许单文件），且扫描窗口内所有数据块均为 PAX
    /// 列式（v6 `zones` 非空；行式块 = 非 JSON / Tombstone 混入 → 回退行级）且**整块**
    /// 落在窗口内（部分块行级回退）。任一不满足 → None（调用方回退行级精确扫描）。
    /// 返回 `(sum, present, null_count)`：
    /// - `present - null_count` = COUNT(f)（字段存在且非 JSON null 行数，精确）；
    /// - `sum` = 块内数值列累加和（列含任一非数值行时编码器清零 → 零和歧义由调用方处置）。
    pub fn zone_field_aggregate(
        &self,
        lo_key: Option<&[u8]>,
        hi_key: Option<&[u8]>,
        field: &str,
    ) -> Result<Option<(f64, u64, u64)>> {
        if self.memtable_bytes() > 0 {
            return Ok(None); // 未刷盘/版本折叠不可用 → 行级回退
        }
        let snap = self.ssts.load();
        let non_empty: Vec<usize> = (0..snap.layer_indices.len())
            .filter(|&lv| !snap.layer_indices[lv].is_empty())
            .collect();
        if non_empty.len() != 1 {
            return Ok(None); // 多非空层 → 跨层版本可能遮蔽 → 行级回退
        }
        let lv = non_empty[0];
        let idxs = &snap.layer_indices[lv];
        if lv == 0 && idxs.len() > 1 {
            return Ok(None); // L0 多段重叠 → 同 key 可能多版本 → 行级回退
        }
        let in_win = |k: &[u8]| lo_key.is_none_or(|lo| k >= lo) && hi_key.is_none_or(|hi| k <= hi);
        let mut sum = 0f64;
        let mut present = 0u64;
        let mut nulls = 0u64;
        for &i in idxs {
            let sst = &snap.ssts[i];
            for e in sst.index() {
                // 仅块**完全**落在窗口内可信（含窗口边界的部分块 → 行级回退）
                if !in_win(&e.first_key) || !in_win(&e.max_key) {
                    return Ok(None);
                }
                if e.zones.is_empty() {
                    return Ok(None); // 行式块（非 JSON / Tombstone 回退块）→ 行级
                }
                let Some(z) = e.zones.iter().find(|z| z.field == field) else {
                    return Ok(None); // 块无该字段列（PAX 列集 = 块内行字段并集）→ 行级
                };
                present += z.present_count as u64;
                nulls += z.null_count as u64;
                sum += z.sum;
            }
        }
        Ok(Some((sum, present, nulls)))
    }

    /// DROP TABLE purge：清空本列族全部数据（MemTable + SST + WAL），重写空 Manifest。
    /// 调用方须持有引擎级写锁（与 flush/写路径互斥）；与无锁后台 compact 经 `sst_mutate`
    /// 互斥（store→persist→remove 原子一致，同 finalize_compact）。删除失败的旧段文件
    /// （Windows 读句柄未释放）成为孤儿——Manifest 已空，重启不会加载。
    pub fn purge_data(&self) -> Result<()> {
        // ① 清空内存双缓冲（不刷盘：WAL 随后截断，整表删除语义）
        self.memtable.reset();
        // ② 段快照清空 + Manifest 空写 + 删旧段文件（P73 顺序：先 store 后删，持 sst_mutate）
        let _g = self.sst_mutate.lock().unwrap();
        let cur = self.ssts.load();
        let old: Vec<std::path::PathBuf> =
            cur.ssts.iter().map(|s| s.path().to_path_buf()).collect();
        self.ssts.store(Arc::new(SstSnapshot {
            ssts: Vec::new(),
            levels: Vec::new(),
            layer_ranges: Vec::new(),
            layer_indices: Vec::new(),
            l0_table_ranges: None,
            sizes: Vec::new(),
        }));
        self.seq_min.write().unwrap().clear();
        self.persist_manifest()?;
        for p in &old {
            let _ = std::fs::remove_file(p);
        }
        drop(_g);
        // ③ WAL 截断重建（内存 next_seq 保留递增；引擎级 global_seq 归零由 Engine::purge_all 负责）
        self.wal.lock().unwrap().truncate_and_reset()?;
        // ④ 段块缓存清空（本 CF 独立 BlockCache 实例，清空避免旧段块残留）
        self.block_cache.clear();
        Ok(())
    }

    pub fn sst_count(&self) -> usize {
        self.ssts.load().ssts.len()
    }

    /// M3（§26 多表，实施清单④）：物理删除**完全落在指定表 docid 区间**内的 SST 文件
    /// （表切分后每文件单表，min/max 同属该表即可整文件删；混表老文件 / 空段保守保留，
    /// 其表数据随逻辑删墓碑在后续压缩中回收）。返回删除文件数。
    /// 调用前提：表区间已先完成**逻辑删除**（multitable::drop_table_range 逐 docid 墓碑，
    /// 位图/MemTable/WAL 均含删除标记）——本函数只做磁盘文件级回收，不改变可见性语义。
    pub fn drop_table_range_files(&self, tid: u16) -> Result<usize> {
        let _g = self.sst_mutate.lock().unwrap();
        let cur = self.ssts.load();
        let in_table = |k: &[u8]| -> bool {
            crate::keys::decode_docid(k)
                .map(|d| ((d >> 48) as u16) == tid)
                .unwrap_or(false)
        };
        let mut removed: Vec<std::path::PathBuf> = Vec::new();
        let mut kept_ssts = Vec::new();
        let mut kept_levels = Vec::new();
        for (i, sst) in cur.ssts.iter().enumerate() {
            let whole = match sst.key_range() {
                Some((mn, mx)) => in_table(mn) && in_table(mx),
                None => false, // 空段 / 无范围 → 保守保留
            };
            if whole {
                removed.push(sst.path().to_path_buf());
            } else {
                kept_ssts.push(sst.clone());
                kept_levels.push(cur.levels[i]);
            }
        }
        if removed.is_empty() {
            return Ok(0);
        }
        // 原子发布（store → persist → remove，同 finalize_compact / purge_data）
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
        for p in &removed {
            let _ = std::fs::remove_file(p);
        }
        drop(_g);
        info!(
            "列族 [{}] DROP TABLE 表文件回收: 删除 {} 个表内 SST（table_id={tid}，全键属该表区间）",
            self.name,
            removed.len()
        );
        Ok(removed.len())
    }

    /// Ex-5.9：指定 SST 的读热度（冷热感知 Compaction 监控/策略数据源）。
    pub fn sst_heat(&self, idx: usize) -> u64 {
        self.ssts.load().ssts.get(idx).map_or(0, |s| s.heat())
    }

    /// 当前 WAL 下一可分配 seq（Engine 快照点来源，design 4.7 MVCC）。
    pub fn wal_next_seq(&self) -> u64 {
        self.wal.lock().unwrap().next_seq()
    }

    /// 接入外部全局 seq 计数器（MVCC，engine 层统一分配，M7-1）。
    /// 在 WAL 回放完成（内部 seq 已推进）后调用，此后写入走外部计数，跨列族一致。
    pub fn set_external_seq(&mut self, seq: Arc<AtomicU64>) {
        self.external_seq = Some(seq);
    }

    /// Task-026：切换到 external WAL（engine 级 per-CPU 队列接管本 CF 持久化）。
    /// - `cf_id`：本 CF 编号（WalEntry.cf）；
    /// - `flushed_cb`：flush 完成后回调（已刷盘最大 gseq → engine checkpoint 推进）。
    /// 此后写路径不再 append 自身 WalBackend，改经 engine 写批次 scope 收集；
    /// `sync_wal` 变 no-op（持久性由队列窗口/`flush_wal` 承担）。internal（默认）不受影响。
    pub fn set_external_wal(&mut self, cf_id: u8, flushed_cb: Arc<dyn Fn(u64) + Send + Sync>) {
        self.external_wal = true;
        self.external_cf_id = cf_id;
        self.flushed_cb = Some(flushed_cb);
    }

    /// external 模式下上报刷盘水位（switch_and_flush 完成回调）。
    pub(crate) fn report_flushed(&self, max_gseq: u64) {
        if let Some(cb) = &self.flushed_cb {
            cb(max_gseq);
        }
    }

    /// M3（§26 多表）：开启按表切分输出（仅主数据列族调用——全键为 docid 定长 8 字节，
    /// 高位 table_id 边界即文件边界；cidx 组合键 / outbox 等不定长键列族必须保持关闭）。
    pub fn enable_table_split(&mut self) {
        self.split_by_table = true;
    }
    /// 取 WAL 中 seq > `since_seq` 的记录（增量备份，M6-5）。
    /// 返回 `(最旧可用 seq, 过滤记录)`：`since_seq != 0` 且最旧可用 seq > since_seq+1 表示
    /// WAL 已被截断（环形覆盖 / 压缩），存在缺口 → 上层应改做全量备份。
    pub fn wal_records_since(
        &self,
        since_seq: u64,
    ) -> Result<(u64, Vec<crate::wal::WalRecord>)> {
        let recs = self.wal.lock().unwrap().recover_records()?;
        let oldest = recs.iter().map(|r| r.seq).min().unwrap_or(u64::MAX);
        let filtered: Vec<_> = recs.into_iter().filter(|r| r.seq > since_seq).collect();
        Ok((oldest, filtered))
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
