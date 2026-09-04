//! 文档引擎压缩协调（reconstruct.md engine/compact.rs）：CompactTargets 无锁合并产物、
//! 跨列族紧迫度调度（compact / compact_inner / compaction_targets）、删除密度触发
//! （delete_gc_density / delete_garbage_pending / delete_garbage_urgency）与写路径
//! 事件驱动自动压缩（auto_compact）。
//! 内容拆分自原 engine.rs（压缩协调主题）；私有 Engine 字段与 auto_compact 以
//! `pub(crate)` 提升访问。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::bitmap::DeletionBitmap;
use crate::column_family::ColumnFamily;
use crate::engine::Engine;
use crate::error::Result;


/// 合并两列族的压实报告（Ex-5.4：multi-CF 聚合；out_level 保留 base 值）。
fn merge_report(base: &mut crate::column_family::CompactReport, other: &crate::column_family::CompactReport) {
    base.merged_ssts += other.merged_ssts;
    base.kept_keys += other.kept_keys;
    base.freed_bytes += other.freed_bytes;
    base.dropped_keys += other.dropped_keys;
}

/// P72（无锁合并）：`Engine::compaction_targets` 的产物——已 clone 的三 CF Arc + 删除位图 Arc
/// 与最高紧迫度档判定。mysql worker drop Engine 读锁后调 `run()` 无锁合并。
pub struct CompactTargets {
    /// 删除位图（Some 且 deleted_count>0 → 合并过滤已删键；None = 传统 Tombstone 路径）。
    pub deletion_bitmap: Option<Arc<DeletionBitmap>>,
    /// 主数据列族 Arc（clone 共享）。
    pub primary: Arc<ColumnFamily>,
    /// 组合索引列族（可能未启用 / 非最高紧迫度档）。
    pub cidx: Option<Arc<ColumnFamily>>,
    /// Delta 增量列族 Arc。
    pub delta: Arc<ColumnFamily>,
    /// 各列族是否为本轮最高紧迫度档（紧凑调度，与 `Engine::compact` 一致）。
    pub do_primary: bool,
    pub do_cidx: bool,
    pub do_delta: bool,
    /// Ex-8.7：删除密度状态（Engine 字段的 Arc clone，`run()` 锁外按压实结果回写）。
    pub garbage_marked: Arc<AtomicU64>,
    pub garbage_done: Arc<AtomicU64>,
    pub garbage_draining: Arc<AtomicBool>,
    /// Ex-8.7：本轮到 `gc_single` 是否允许**单段重写**（删除密度触发时 true——
    /// 收敛后单底层段无常规合并候选，需重写才能物理回收已删数据）。
    pub gc_single: bool,
    /// R4：MVCC 保活水位（seq floor）——compact 时该 CF 保留最新 seq > floor 的多版本
    /// （活跃旧快照可回读删除/覆盖前值）；0 = 无活跃快照（现状收敛/GC 物理回收）。
    pub mvcc_floor: u64,
}

/// Ex-8.7：主列族压实反馈——按删除位图实际**物理丢弃 >0** → 继续删除密度排空；
/// 0 丢弃 → 排空收敛（快照 `garbage_marked`，此后需新增置位 ≥ min_docs 才再触发）。
fn apply_gc_feedback(
    draining: &AtomicBool,
    done: &AtomicU64,
    marked: &AtomicU64,
    r: &crate::column_family::CompactReport,
) {
    if r.dropped_keys > 0 {
        draining.store(true, Ordering::Relaxed);
    } else {
        draining.store(false, Ordering::Relaxed);
        done.store(marked.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

impl CompactTargets {
    /// 无锁合并执行：仅压最高紧迫度档列族（串行；并列场景由 worker 下一轮收敛）。
    /// 与写并发安全：compact 不碰 memtable；ssts 变更经 CF `sst_mutate` 与 flush 互斥。
    pub fn run(&self) -> Result<crate::column_family::CompactReport> {
        let empty = crate::column_family::CompactReport {
            merged_ssts: 0,
            kept_keys: 0,
            freed_bytes: 0,
            out_level: 0,
            dropped_keys: 0,
        };
        let mut rep = empty;
        let bm = self.deletion_bitmap.as_ref();
        let needs_filter = bm.is_some_and(|b| b.deleted_count() > 0);
        // R4：按活跃快照低水位设置 MVCC 保活（compact 期间各 CF 保留旧版本）；
        // 结束后统一复位 0（无活跃快照恢复现状版本收敛/GC 物理回收）。
        self.primary.set_mvcc_keep_floor(self.mvcc_floor);
        if let Some(c) = &self.cidx {
            c.set_mvcc_keep_floor(self.mvcc_floor);
        }
        self.delta.set_mvcc_keep_floor(self.mvcc_floor);
        if self.do_primary {
            // Ex-8.7：过滤压实（多段常规合并等价 compact_filtered；删除密度触发时
            // `gc_single=true` 允许收敛后单底层段重写回收）→ 按 dropped 回写排空状态
            let r = if needs_filter {
                let f = |k: &[u8]| bm.is_some_and(|b| b.is_deleted_key(k));
                self.primary.compact_gc(&f, self.gc_single)?
            } else {
                self.primary.compact()?
            };
            merge_report(&mut rep, &r);
            apply_gc_feedback(&self.garbage_draining, &self.garbage_done, &self.garbage_marked, &r);
        }
        if self.do_cidx {
            if let Some(c) = &self.cidx {
                let r = c.compact()?;
                merge_report(&mut rep, &r);
            }
        }
        if self.do_delta {
            let r = self.delta.compact()?;
            merge_report(&mut rep, &r);
        }
        // R4：复位保活水位
        self.primary.set_mvcc_keep_floor(0);
        if let Some(c) = &self.cidx {
            c.set_mvcc_keep_floor(0);
        }
        self.delta.set_mvcc_keep_floor(0);
        Ok(rep)
    }
}

impl Engine {
    /// Ex-8.7：删除置位率 = 净置位数 / max(置位数, 曾写入最大 docid)——"位图置位率"密度。
    fn delete_gc_density(&self) -> f32 {
        let marked = self.garbage_marked.load(Ordering::Relaxed);
        let denom = marked.max(self.max_docid.load(Ordering::Relaxed)).max(1);
        marked as f32 / denom as f32
    }

    /// Ex-8.7：删除密度 Compaction 是否就绪（needs_compact / 紧迫度 / GC 单段重写门槛）：
    /// 位图开启 + `delete_density_min_ratio` > 0 + 置位率 ≥ 阈值 + 无积压……
    /// （`garbage_draining` 排空中无视"新增置位"门槛——直到某轮 0 丢弃收敛）。
    pub fn delete_garbage_pending(&self) -> bool {
        if self.deletion_bitmap.is_none() || self.dd_min_ratio <= 0.0 {
            return false;
        }
        let marked = self.garbage_marked.load(Ordering::Relaxed);
        if marked == 0 {
            return false;
        }
        let draining = self.garbage_draining.load(Ordering::Relaxed);
        if !draining && marked.saturating_sub(self.garbage_done.load(Ordering::Relaxed)) < self.dd_min_docs {
            return false;
        }
        self.delete_gc_density() >= self.dd_min_ratio
    }

    /// Ex-8.7：删除密度维度的跨列族紧迫度权重（W 项公式外挂项）——就绪时 +`DD_URGENCY`：
    /// 低于 L0 段数主因子（×10/段），高于纯 L0 大小软阈值 +8 之下的次级——
    /// 收敛后（L0=0）删除密集主列族仍能压过空闲 delta/cidx 率先被合并回收空间。
    fn delete_garbage_urgency(&self) -> u32 {
        if self.delete_garbage_pending() {
            crate::column_family::DD_URGENCY
        } else {
            0
        }
    }

    /// 基础 Compaction（design 4.5，阶段 3；Ex-5.4 并行化）：primary/cidx/delta 列族压实。
    /// 并行度 `compaction_parallel`：0 = 自动（min(4, 核数/2)）；1 = 串行；>1 = 指定。
    /// W 项：跨列族**紧迫度调度**——每轮仅压实紧迫度最高档（L0 压力/大小超限最大）的列族，
    /// 并列档并行（保留 SSD 并发收益）；其余列族由后台 worker 后续轮次（`while needs_compact`）
    /// 压实——压力最大的列族（通常 primary 主数据）优先收敛，读路径最快受益。
    /// Ex-5.6：删除位图开启时 primary 压实按位图**物理丢弃**已删 docid 的旧数据（墓碑不污染层级）。
    /// Ex-8.7：删除密集时紧迫度叠加删除密度权重；触发后允许收敛单段重写（`compact_gc`），
    /// 并按压实**实际丢弃数**回写排空状态（drop>0 继续 / 0 收敛，见 `apply_gc_feedback`）。
    /// O 项第③步：`&self`——后台合并 worker 在引擎**读锁**下执行（合并不阻塞读；
    /// 与写互斥由 Engine RwLock 保证，快照 store 无并发丢失）。
    /// R4：wrapper——compact 前按活跃快照低水位设 MVCC 保活，结束后复位。
    pub fn compact(&self) -> Result<crate::column_family::CompactReport> {
        self.apply_mvcc_floor();
        let r = self.compact_inner();
        self.clear_mvcc_floor();
        r
    }


    fn compact_inner(&self) -> Result<crate::column_family::CompactReport> {
        // W 项：紧迫度 = 列族 compaction_urgency（L0 段数×10 + 大小超限 +8）+ 删除密度权重
        let pu = self.primary.compaction_urgency() + self.delete_garbage_urgency();
        let du = self.delta.compaction_urgency();
        let cu = self.cidx.as_ref().map_or(0, |c| c.compaction_urgency());
        let max = pu.max(du).max(cu);
        let empty = crate::column_family::CompactReport {
            merged_ssts: 0,
            kept_keys: 0,
            freed_bytes: 0,
            out_level: 0,
            dropped_keys: 0,
        };
        if max == 0 {
            // 无 L0 压力（urgency 只计 L0 段数/大小）——但底层仍可能需要合并（L1→L2 /
            // L2 收敛 / Ex-8.11 L1 攒批下沉）。防空闲饿死：直接压实 needs_compact 的列族
            // （优先级 primary > delta > cidx），否则空返回。
            let pn = self.primary.needs_compact();
            let dn = self.delta.needs_compact();
            let cn = self.cidx.as_ref().map_or(false, |c| c.needs_compact());
            if pn || dn || cn {
                let mut rep = empty;
                if pn {
                    let r = self.primary.compact()?;
                    merge_report(&mut rep, &r);
                } else if dn {
                    let r = self.delta.compact()?;
                    merge_report(&mut rep, &r);
                } else {
                    let r = self.cidx.as_ref().unwrap().compact()?;
                    merge_report(&mut rep, &r);
                }
                self.metrics.compact_count.fetch_add(1, Ordering::Relaxed);
                return Ok(rep);
            }
            return Ok(empty); // 无压力（调用方应在 needs_compact 下进入）
        }
        let do_p = pu == max;
        let do_c = self.cidx.is_some() && cu == max;
        let do_d = du == max;
        // Ex-8.7：删除密度触发时允许主列族"单底层段重写"（GC 回收已删数据）
        let gc_single = self.delete_garbage_pending();

        let parallel = if self.compaction_parallel == 0 {
            std::thread::available_parallelism()
                .map(|n| (n.get() / 2).clamp(1, 4))
                .unwrap_or(1)
        } else {
            self.compaction_parallel.max(1)
        };
        let cf_count = usize::from(do_p) + usize::from(do_c) + usize::from(do_d);
        let threads = parallel.min(cf_count.max(1));
        // Ex-5.6/5.8：位图不可变借用（与列族共享借用互不冲突）。
        let bm = self.deletion_bitmap.as_ref();
        let needs_filter = bm.is_some_and(|b| b.deleted_count() > 0);
        let filter = |k: &[u8]| bm.is_some_and(|b| b.is_deleted_key(k));
        if threads <= 1 {
            // 串行：仅最高紧迫度档列族
            let mut rep = empty;
            if do_p {
                let r = if needs_filter {
                    self.primary.compact_gc(&filter, gc_single)?
                } else {
                    self.primary.compact()?
                };
                merge_report(&mut rep, &r);
                apply_gc_feedback(&self.garbage_draining, &self.garbage_done, &self.garbage_marked, &r);
            }
            if do_c {
                let r = self.cidx.as_ref().unwrap().compact()?;
                merge_report(&mut rep, &r);
            }
            if do_d {
                let r = self.delta.compact()?;
                merge_report(&mut rep, &r);
            }
            self.metrics.compact_count.fetch_add(1, Ordering::Relaxed);
            return Ok(rep);
        }
        // 并行：仅最高紧迫度档（并列）列族
        let compute_cores = self.affinity.compute.clone(); // Ex-7.2：Compaction 并行线程绑 compute 核
        let (p, c, d) = (&self.primary, self.cidx.as_ref(), &self.delta);
        let merged = std::thread::scope(|s| -> Result<crate::column_family::CompactReport> {
            let h1 = if do_p {
                let cc = compute_cores.clone();
                let f = filter;
                Some(s.spawn(move || {
                    crate::affinity::bind_current(&cc);
                    if needs_filter {
                        p.compact_gc(&f, gc_single)
                    } else {
                        p.compact()
                    }
                }))
            } else {
                None
            };
            let h2 = if do_c {
                let cc = compute_cores.clone();
                let cf = c.unwrap();
                Some(s.spawn(move || {
                    crate::affinity::bind_current(&cc);
                    cf.compact()
                }))
            } else {
                None
            };
            let h3 = if do_d {
                let cc = compute_cores.clone();
                Some(s.spawn(move || {
                    crate::affinity::bind_current(&cc);
                    d.compact()
                }))
            } else {
                None
            };
            let mut merged = empty;
            // h1（下标 0）= primary：join 后按实际丢弃回写删除密度排空状态
            for (k, h) in [h1, h2, h3].into_iter().enumerate() {
                if let Some(handle) = h {
                    let r = handle.join().unwrap()?;
                    if k == 0 && do_p {
                        apply_gc_feedback(
                            &self.garbage_draining,
                            &self.garbage_done,
                            &self.garbage_marked,
                            &r,
                        );
                    }
                    merge_report(&mut merged, &r);
                }
            }
            self.metrics.compact_count.fetch_add(1, Ordering::Relaxed);
            Ok(merged)
        })?;
        Ok(merged)
    }

    /// 是否需要 Compaction（主数据 / delta / cidx 任一列族 L0 超阈值或 L1/L2 需收敛，
    /// 或 Ex-8.7 删除密度就绪——位图删除数据待回收）。
    pub fn needs_compact(&self) -> bool {
        self.primary.needs_compact()
            || self.delta.needs_compact()
            || self.cidx.as_ref().map_or(false, |c| c.needs_compact())
            || self.delete_garbage_pending()
    }

    /// P72（无锁合并）：读取三 CF Arc + 删除位图 Arc + 紧迫度判定（紧凑调度复刻 `compact`）——
    /// mysql worker 在 Engine 读锁内**快速**调用本方法（clone 廉价），drop 锁后对返回的
    /// `CompactTargets::run()` 执行**无锁合并**（与写并发；ssts 变更经 CF `sst_mutate` 互斥，
    /// flush 同锁 → 无丢失更新）。返回 None = 无紧迫度（不需要合并）。
    pub fn compaction_targets(&self) -> Option<CompactTargets> {
        let pu = self.primary.compaction_urgency() + self.delete_garbage_urgency();
        let du = self.delta.compaction_urgency();
        let cu = self.cidx.as_ref().map_or(0, |c| c.compaction_urgency());
        let max = pu.max(du).max(cu);
        if max == 0 {
            return None;
        }
        Some(CompactTargets {
            deletion_bitmap: self.deletion_bitmap.clone(),
            primary: self.primary.clone(),
            cidx: self.cidx.clone(),
            delta: self.delta.clone(),
            do_primary: pu == max,
            do_cidx: self.cidx.is_some() && cu == max,
            do_delta: du == max,
            garbage_marked: Arc::clone(&self.garbage_marked),
            garbage_done: Arc::clone(&self.garbage_done),
            garbage_draining: Arc::clone(&self.garbage_draining),
            gc_single: self.delete_garbage_pending(),
            // R4：活跃快照低水位（compact 保活依据）
            mvcc_floor: self.snapshot_floor(),
        })
    }

    /// 主数据列族当前 L0 段数（P 项自动 Compaction 收敛性观测 / 测试）。
    pub fn primary_l0_count(&self) -> usize {
        self.primary.l0_count()
    }

    /// X 项：全列族累计刷盘次数（/metrics flush 指标）。
    pub fn total_flush_count(&self) -> u64 {
        self.primary.flush_count()
            + self.delta.flush_count()
            + self.cidx.as_ref().map_or(0, |c| c.flush_count())
    }

    /// P 项：事件驱动自动 Compaction——写入路径自触发（Flush 后 L0 段数/大小超阈值 → 合并收敛）。
    /// O 项第③步双分支：
    /// - **有后台 worker**（mysql 服务挂载，`compact_worker=true`）：只置 `compact_pending` 信号，
    ///   实际合并由 worker 在引擎**读锁**下执行——读写均不被合并阻塞（合并不阻塞读）；
    /// - **无 worker**（demo/rpc/测试）：保持同步执行（单写者模型：合并期间阻塞读写，
    ///   写入自然退避 = 背压，L0 有界）。
    /// guard 上限 8：一次写入最多收敛 8 轮（正常 1~2 轮即收敛，防异常空转死循环）。
    /// P80 修复：合并无进展（merged_ssts=0 或错误）时退避 100ms 再重试，防 watchdog 超时截断
    /// 导致永不推进的 0 CPU 死循环。
    pub(crate) fn auto_compact(&mut self) -> Result<()> {
        if !self.auto_compact {
            return Ok(());
        }
        if self.compact_worker.load(Ordering::Acquire) {
            if self.needs_compact() {
                self.compact_pending.store(true, Ordering::Release);
            }
            return Ok(());
        }
        let mut guard = 0;
        while self.needs_compact() && guard < 8 {
            match self.compact() {
                Ok(rep) => {
                    if rep.merged_ssts == 0 {
                        // 无进展：退避防空转，重试直到下一轮 L0 有新段/超 guard 上限
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                }
                Err(e) => {
                    // P80：合并超时/失败 → 记录警告，退避 200ms 再试（异常路径不 panic）
                    tracing::warn!("auto_compact 失败: {e}，退避 200ms 后重试");
                    std::thread::sleep(std::time::Duration::from_millis(200));
                }
            }
            guard += 1;
        }
        Ok(())
    }

}
