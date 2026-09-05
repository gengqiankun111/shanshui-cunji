//! 刷盘：MemTable 冻结切换 → 单文件 / 按表切分 / TTL 分桶落盘 → 快照构建与计数。

use std::sync::atomic::Ordering;

use tracing::info;

use crate::error::Result;
use crate::memtable::MemTable;
use crate::sstable::{SstReader, SstWriter, FLAG_DELETE, FLAG_PUT};

use super::*;
use super::open::epoch_days;

impl ColumnFamily {
    /// MemTable 超阈值 → 冻结并刷盘（MVP 同步刷盘）。
    /// P72：`&self`（Engine 字段 Arc<ColumnFamily> 化后 flush 无法取 &mut）。
    pub(crate) fn maybe_flush(&self) -> Result<()> {
        if self.memtable.mutable_bytes() < self.cfg.max_size_mb * 1024 * 1024 {
            return Ok(());
        }
        self.switch_and_flush()
    }

    /// 冻结 Mutable → 刷盘为 SST → 更新 Manifest → 释放 Immutable。
    /// P72：`&self`——memtable 冻结经内部 RwLock 写锁；ssts 发布经 `sst_mutate` 互斥。
    pub fn switch_and_flush(&self) -> Result<()> {
        self.memtable.switch();
        let Some(imm) = self.memtable.take_immutable() else {
            return Ok(());
        };
        // X 项：flush 计数（写路径刷盘指标）
        self.flush_counter.fetch_add(1, Ordering::Relaxed);
        // 环形 WAL 覆盖安全：Flush 完成后上报 imm 内最大 seq（<= 该 seq 的记录已刷盘可覆盖）
        let flushed_max = imm_scan_max_seq(&imm);
        let new_segments = if self.ttl_days.is_none() {
            // M3：主数据列族（多表）按表切分输出；其余单文件（行为不变）
            if self.split_by_table {
                self.flush_by_table(&imm)?;
                // 记录 flush_by_table 创建的 SST 数（snapshot_insert 中计数）
                self.flush_sst_count.load(Ordering::Relaxed)
            } else {
                self.flush_single(&imm)?;
                1
            }
        } else {
            self.flush_buckets(&imm)?;
            // 记录 flush_buckets 创建的 SST 数
            self.flush_sst_count.load(Ordering::Relaxed)
        };
        // P4-A：记录本次 flush 新增 L0 段数 → 写入速率自适应
        self.record_flush_new_l0(new_segments);
        // Task-026 external WAL：无自持文件——flush 完成即推进 engine 侧 CF 水位
        // （checkpoint = min(各 CF 水位)，队列段裁剪依据）
        if self.external_wal {
            self.report_flushed(flushed_max);
        } else {
            self.wal.lock().unwrap().set_flushed_seq(flushed_max);
            // WAL 截断（M8-P5）：append 模式 flush 后全部记录已刷盘，清空 WAL 保持小文件
            // （避免无限增长 + 大文件 fsync 拖慢写入）；ring 模式自带覆盖回收（no-op）
            self.wal.lock().unwrap().truncate_and_reset()?;
        }
        Ok(())
    }

    /// X 项：本列族累计刷盘次数（写路径 flush 指标）。
    pub fn flush_count(&self) -> u64 {
        self.flush_counter.load(Ordering::Relaxed)
    }

    /// Ex-8.11：累计写入 SST 字节（flush/compact 新建文件字节和；写放大实验数据源）。
    pub fn sst_written_bytes(&self) -> u64 {
        self.sst_written.load(Ordering::Relaxed)
    }

    /// Ex-8.11：L0/L1/L2 段数分布（写放大 A/B 观察合并节奏）。
    pub fn layer_counts(&self) -> (usize, usize, usize) {
        let snap = self.ssts.load();
        let mut c = (0usize, 0usize, 0usize);
        for lv in snap.levels.iter() {
            match *lv {
                0 => c.0 += 1,
                1 => c.1 += 1,
                _ => c.2 += 1,
            }
        }
        c
    }

    /// 2026-09-05（P0 观测 ③）：**L0 层按表（table_id）段数分布**——
    /// 多表（split_by_table，每段单表）场景观测"全局 L0 未满但单表 L0 堆积"：
    /// 热点表 flush 频次高 → 其 L0 段数随每批 flush 单调增，读放大 O(段数)。
    /// 返回 (table_id, L0 段数) 升序；单表（table_id=0）场景仅一条/空。
    pub fn l0_table_counts(&self) -> Vec<(u16, usize)> {
        let snap = self.ssts.load();
        let mut m: std::collections::BTreeMap<u16, usize> = std::collections::BTreeMap::new();
        if let Some(l0) = snap.layer_indices.first() {
            for &i in l0 {
                let tid = snap.ssts[i].table_id().unwrap_or(0);
                *m.entry(tid).or_insert(0) += 1;
            }
        }
        m.into_iter().collect()
    }

    /// P129（补充监控）：per-table 压实已执行次数（多表分支每次成功合并 +1）——
    /// 观测压实频率 = 多表写放大间接量。
    pub fn per_table_compact_runs(&self) -> u64 {
        self.per_table_compact_runs.load(Ordering::Relaxed)
    }

    /// 从 docid 编码 key 中提取 table_id（高 16 位）。
    /// key 为 encode_docid 的 8 字节大端编码，前 2 字节 = table_id。
    pub(crate) fn table_id_from_key(key: &[u8]) -> Option<u16> {
        if key.len() >= 2 {
            Some(u16::from_be_bytes([key[0], key[1]]))
        } else {
            None
        }
    }

    /// P3-A：构建 L0 层按表分组的范围——遍历 L0 SST，按 table_id 聚合 key 范围。
    /// 每个 SST 只含单表数据（M3 flush/compact 按表切分），所以从 key_range 的 min key
    /// 提取 table_id 即可确定所属表。
    fn build_l0_table_ranges(
        ssts: &[Arc<SstReader>],
        l0_indices: &[usize],
    ) -> Option<std::collections::HashMap<u16, (Vec<u8>, Vec<u8>)>> {
        if l0_indices.is_empty() {
            return None;
        }
        let mut table_ranges: std::collections::HashMap<u16, (Vec<u8>, Vec<u8>)> =
            std::collections::HashMap::new();
        for &i in l0_indices {
            let s = &ssts[i];
            let (min, max) = match s.key_range() {
                Some((mn, mx)) => (mn, mx),
                None => return None, // 无范围段 → 无法构建，回退 None
            };
            // 从 min key 提取 table_id（M3 保证每文件单表）
            let tid = match Self::table_id_from_key(min) {
                Some(t) => t,
                None => return None,
            };
            match table_ranges.entry(tid) {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    let (lo, hi) = e.get_mut();
                    if min < lo.as_slice() {
                        *lo = min.to_vec();
                    }
                    if max > hi.as_slice() {
                        *hi = max.to_vec();
                    }
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert((min.to_vec(), max.to_vec()));
                }
            }
        }
        Some(table_ranges)
    }

    /// R 项：快照构建时计算层聚合元数据（O(段数)）——每层 key 范围 + 每层段下标。
    /// 层范围 = 层内各段范围并集；层内存在"无范围段"（无约束）→ 该层 None（不可跳过，
    /// 防假阴性——布隆/范围粗筛只允许假阳性，不允许假阴性）。
    pub(crate) fn build_layer_meta(
        ssts: &[Arc<SstReader>],
        levels: &[u32],
    ) -> (
        Vec<Option<(Vec<u8>, Vec<u8>)>>,
        Vec<Vec<usize>>,
        Option<std::collections::HashMap<u16, (Vec<u8>, Vec<u8>)>>,
    ) {
        let mut ranges: Vec<Option<(Vec<u8>, Vec<u8>)>> = vec![None, None, None];
        let mut indices: Vec<Vec<usize>> = vec![Vec::new(), Vec::new(), Vec::new()];
        for (i, (s, lv)) in ssts.iter().zip(levels).enumerate() {
            let lv = (*lv as usize).min(2);
            indices[lv].push(i);
            match s.key_range() {
                Some((min, max)) => match &mut ranges[lv] {
                    Some((lo, hi)) => {
                        if min < lo.as_slice() {
                            *lo = min.to_vec();
                        }
                        if max > hi.as_slice() {
                            *hi = max.to_vec();
                        }
                    }
                    None => ranges[lv] = Some((min.to_vec(), max.to_vec())),
                },
                // 段无范围（空段/无索引）→ 层不可粗筛跳过（保守，防假阴性）
                None => ranges[lv] = None,
            }
        }
        // P3-A：L0 按表分组范围
        let l0_table_ranges = Self::build_l0_table_ranges(ssts, &indices[0]);
        (ranges, indices, l0_table_ranges)
    }

    /// O 项第③步：原子插入新 SST（快照 `store()`）——新文件插最前（读路径优先命中），层号 L0。
    /// Ex-8.11：每次 flush 落一个新文件 → 累计写入字节（写放大实验数据源）。
    fn snapshot_insert(&self, reader: SstReader) {
        self.sst_written
            .fetch_add(reader.file_len(), Ordering::Relaxed);
        let cur = self.ssts.load();
        let mut ssts = cur.ssts.clone();
        ssts.insert(0, Arc::new(reader));
        let mut levels = cur.levels.clone();
        levels.insert(0, 0);
        let (layer_ranges, layer_indices, l0_table_ranges) = Self::build_layer_meta(&ssts, &levels);
        let sizes: Vec<u64> = ssts.iter().map(|r| r.file_len()).collect();
        self.ssts.store(Arc::new(SstSnapshot {
            ssts,
            levels,
            layer_ranges,
            layer_indices,
            l0_table_ranges,
            sizes,
        }));
    }

    /// 原逻辑：整个 Immutable 落盘为单个 SST。
    /// P72/P73：`&self` + `sst_mutate` 锁全程持锁——id 分配、文件写入、store、manifest 原子
    /// （无锁合并并发时：id 无重复、persist 不引用半写段）。
    fn flush_single(&self, imm: &MemTable) -> Result<()> {
        let _g = self.sst_mutate.lock().unwrap();
        let sst_id = self.next_sst_id.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{SST_PREFIX}{sst_id:08}.sst"));
        self.write_sst(&path, imm)?;

        // 新文件插到最前（读路径优先命中）
        let fname = path.file_name().unwrap().to_string_lossy().to_string();
        self.io_acquire(&path)?;
        let reader = SstReader::open_with_granularity(&path, self.index_granularity)?;
        self.snapshot_insert(reader);
        self.persist_manifest()?;
        drop(_g);
        info!(
            "列族 [{}] 刷盘完成: {} ({} 条)",
            self.name,
            fname,
            imm.len()
        );
        Ok(())
    }

    /// M3（§26 多表，实施清单①）：Immutable **按表切分**落盘——遍历升序键流，
    /// 检测 `docid >> 48`（table_id）变化即 Finish 当前 writer、开新 writer（每表一个 SST）。
    /// 前提：docid 高位编码 → 同表键在扫描序中连续，表边界检测即文件边界；
    /// 键非 8 字节定长（理论上不会出现在开启切分的列族）保守并入 table 0。
    /// 单表（table_id=0，含旧库全量）→ 仅开一个 writer = 单文件，与 flush_single 一致。
    /// 空 Immutable：无键可切分，直接返回（不产出空文件，无副作用）。
    fn flush_by_table(&self, imm: &MemTable) -> Result<()> {
        if imm.len() == 0 {
            // 空 Immutable：无键可切分，直接返回（不产出空文件）
            return Ok(());
        }
        let _g = self.sst_mutate.lock().unwrap();
        let mut writer: Option<SstWriter> = None;
        let mut cur_tbl: u16 = 0;
        let mut outputs: Vec<std::path::PathBuf> = Vec::new();
        imm.scan(|k, e| {
            let tbl = key_table_id(k).unwrap_or(0);
            if writer.is_none() || tbl != cur_tbl {
                if let Some(mut w) = writer.take() {
                    w.finish().expect("SST finish 失败");
                }
                let sst_id = self.next_sst_id.fetch_add(1, Ordering::Relaxed);
                let path = self.dir.join(format!("{SST_PREFIX}{sst_id:08}.sst"));
                let w = SstWriter::new_with_pax(
                    &path,
                    self.compression,
                    self.compression_level,
                    self.block_size,
                    imm.len(),
                    &self.pax_hot_fields,
                    self.bloom_fpr,
                )
                .expect("SST 创建失败");
                writer = Some(w);
                cur_tbl = tbl;
                outputs.push(path);
            }
            let w = writer.as_mut().unwrap();
            match &e.value {
                Some(v) => w.add(k, v, e.seq).expect("SST 写入失败"),
                None => w.add_tombstone(k, e.seq).expect("SST Tombstone 写入失败"),
            }
        });
        if let Some(mut w) = writer.take() {
            w.finish().expect("SST finish 失败");
        }
        self.flush_sst_count.store(outputs.len(), Ordering::Relaxed);
        for p in &outputs {
            self.io_acquire(p)?;
            let reader = SstReader::open_with_granularity(p, self.index_granularity)?;
            self.snapshot_insert(reader);
        }
        self.persist_manifest()?;
        drop(_g);
        info!(
            "列族 [{}] 按表切分刷盘完成: {} 个文件（{} 条）",
            self.name,
            outputs.len(),
            imm.len()
        );
        Ok(())
    }

    /// TTL 分桶 flush：按文档 ttl_field 提取的 UTC 天分桶，每个桶一个 SST 文件；
    /// 无时间字段 / 非 JSON 值落入默认桶（无日期前缀，永不过期）。桶内 key 保持升序。
    fn flush_buckets(&self, imm: &MemTable) -> Result<()> {
        let mut buckets: std::collections::BTreeMap<Option<i64>, Vec<BucketRow>> =
            std::collections::BTreeMap::new();
        imm.scan(|k, e| {
            let days = match &e.value {
                Some(v) => self.document_bucket_days(v),
                None => None, // Tombstone 无时间 → 默认桶（随对应 key 的桶删除语义由数据决定）
            };
            let flag = if e.value.is_some() {
                FLAG_PUT
            } else {
                FLAG_DELETE
            };
            buckets
                .entry(days)
                .or_default()
                .push((k.to_vec(), e.value.clone(), flag, e.seq));
        });
        let bucket_count = buckets.len();
        let _g = self.sst_mutate.lock().unwrap();
        let mut created = 0;
        for (days, rows) in buckets {
            let sst_id = self.next_sst_id.fetch_add(1, Ordering::Relaxed);
            let fname = match days {
                Some(d) => format!("{SST_PREFIX}{d:08}-{sst_id:08}.sst"),
                None => format!("{SST_PREFIX}{sst_id:08}.sst"),
            };
            let path = self.dir.join(&fname);
            self.write_rows(&path, &rows)?;
            self.io_acquire(&path)?;
            let reader = SstReader::open_with_granularity(&path, self.index_granularity)?;
            self.snapshot_insert(reader);
            created += 1;
        }
        self.flush_sst_count.store(created, Ordering::Relaxed);
        self.persist_manifest()?;
        drop(_g);
        info!(
            "列族 [{}] TTL 分桶刷盘完成: {} 个桶",
            self.name, bucket_count
        );
        Ok(())
    }

    /// 从文档 JSON 提取 ttl_field（数值秒级时间戳）→ UTC 纪元天数；无法解析返回 None。
    pub(crate) fn document_bucket_days(&self, value: &[u8]) -> Option<i64> {
        let v: serde_json::Value = serde_json::from_slice(value).ok()?;
        let secs = v.get(&self.ttl_field)?.as_i64()?;
        Some(epoch_days(secs))
    }
}

/// 扫描 Immutable 记录的最大 seq（环形 WAL 覆盖安全游标：<= 该 seq 均已刷盘）。
fn imm_scan_max_seq(imm: &MemTable) -> u64 {
    let mut max_seq = 0u64;
    imm.scan(|_, e| {
        if e.seq > max_seq {
            max_seq = e.seq;
        }
    });
    max_seq
}
