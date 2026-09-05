//! Compaction 对外入口与策略方法（原 compaction.rs 按主题拆出，design 4.5 二期 Leveled /
//! M6-2）：`impl ColumnFamily` 上的触发/选择策略（compact / compact_filtered / compact_gc /
//! needs_compact / bottom_needs_compact / split_bottom_merge_work / sst_table_of /
//! cooling_indices / l0_count / compaction_urgency / l0_bytes / sst_bytes）。
//! 合并执行内部（compact_merge / try_meta_only_compact / finalize_compact / pick_gc_single /
//! sel_has_merge_work / compression_level_for / write_rows / write_sst）与选段自由函数
//! （select_compaction_inputs / select_compaction_inputs_ex / cap_by_size / select_inner）
//! 已拆至同目录 [`super::merge`]。
//! 对外 API 路径保持 `crate::storage::column_family::ColumnFamily` 方法不变（pub 方法仍 pub，
//! 被 merge.rs / column_family.rs 其余代码引用的私有项以最小 `pub(crate)` 提升）。

use std::sync::atomic::Ordering;

use crate::error::Result;
use crate::storage::column_family::{key_table_id, ColumnFamily, CompactReport, SstSnapshot};

// column_family::tests 直接引用 `crate::storage::sstable::compaction::{...}` 模块路径 →
// 拆分后经下方 re-export 保持该路径可解析（函数实体在 merge.rs）；本文件入口方法亦经此调用。
// 非 test 构建下 cap_by_size / select_compaction_inputs 无调用方（仅供 column_family::tests）→ allow。
#[allow(unused_imports)]
pub(crate) use super::merge::{
    cap_by_size, select_compaction_inputs, select_compaction_inputs_ex,
};

impl ColumnFamily {
    // ============ P129：per-table L0 优先压实（多表单批 flush 每表 1 文件的全局计数失配治理） ============
    //
    // split_by_table 下每批 flush 产出 N 表 × 每表 1 个 L0 文件——全局 L0 阈值（动态 [8,12]）
    // 与表数强耦合：N 大时每写必触发全量 L0 合并（全部表参与、写放大爆）或调高阈值致热点表
    // 段数失控。本路径把 L0 压实决策改为按表：某表 L0 段数 ≥ per_table_l0_trigger（默认 2）
    // 时只压实该表段子集（L1 同表段并入防层内重叠），其余表不参与；全局计数仅在单表/未开启
    // 时保留为原语义（单表库零回归）。

    /// P129：per-table 压实模式是否生效——split_by_table + 阈值 >0 且 L0 层现存 ≥2 个不同表
    /// （单表库 / L0 空 → 回退原全局逻辑，行为零回归）。L0 含未归属段（旧库混表段，table_id
    /// 无法提取）→ 回退全局（防无人治理）。
    fn per_table_active(&self, snap: &SstSnapshot) -> bool {
        if !self.split_by_table || self.per_table_l0_trigger == 0 {
            return false;
        }
        let mut tids = std::collections::BTreeSet::new();
        for (i, lv) in snap.levels.iter().enumerate() {
            if *lv == 0 {
                match snap.ssts[i].table_id() {
                    Some(t) => {
                        tids.insert(t);
                    }
                    None => return false, // 混表/未知段 → 全局语义（旧行为）
                }
            }
        }
        tids.len() > 1
    }

    /// P129：目标表压实输入子集——L0 段数最多且 ≥ 阈值的表：该表全部 L0 段。
    /// 输入合并去重后**输出 L1 新段**（不并入既有 L1 同表段——对齐 leveled"单次压实量 =
    /// 单批有界"，避免每批 flush 重写该表全部历史致写放大无界；L1 同表多段版本重读正确，
    /// 由 L0 空时的 bottom split 收敛为单段）。无触发 → None（回退原全局选段/空转）。
    fn table_l0_subset(&self, snap: &SstSnapshot) -> Option<Vec<usize>> {
        // 每表 L0 段下标（reader.table_id 取 min_key 高 2 字节；split_by_table 每段单表前提）
        let mut per: std::collections::BTreeMap<u16, Vec<usize>> = Default::default();
        for (i, lv) in snap.levels.iter().enumerate() {
            if *lv != 0 {
                continue;
            }
            if let Some(t) = snap.ssts[i].table_id() {
                per.entry(t).or_default().push(i);
            }
        }
        let trig = self.per_table_l0_trigger;
        // 目标表 = L0 段数最多且 ≥ 触发阈值的表（同表 ≥2 段 = 可能重叠，需合并去重）
        let target = per
            .iter()
            .filter(|(_, v)| v.len() >= trig)
            .max_by_key(|(_, v)| v.len())
            .map(|(t, _)| *t)?;
        let mut sel = per[&target].clone();
        sel.sort_unstable(); // 快照顺序（新→旧），key 归并正确
        if sel.len() <= 1 {
            return None; // 单段无收益
        }
        Some(sel)
    }

    /// 基础 Compaction（无删除位图过滤）。Ex-5.8：无重叠 L0 段合并走**数据块级复用**
    /// （只重建元数据区），否则回退全量合并（等价 `compact_filtered(&|_| false)`）。
    /// P129：多表 per-table 模式下优先压实"段数最多的表"子集（每轮只动一张表）。
    /// O 项第③步：`&self`（ssts 快照 load/store 原子发布）——后台合并线程读锁下执行。
    pub fn compact(&self) -> Result<CompactReport> {
        let snap = self.ssts.load();
        // P129：多表 per-table 压实（目标表 L0→L1，输出合并去重；同表段天然无跨表重叠）
        if self.per_table_active(&snap) {
            if let Some(sel) = self.table_l0_subset(&snap) {
                let rep = if let Some(r) = self.try_meta_only_compact(&sel, 1)? {
                    r
                } else {
                    self.compact_merge(&sel, 1, &|_| false)?
                };
                // P129 监控：per-table 压实执行计数（写放大间接量）
                self.per_table_compact_runs.fetch_add(1, Ordering::Relaxed);
                return Ok(rep);
            }
            // 多表模式无目标表（各表 ≤1 段 = 按表收敛态）：每表 L0 ≤1 段读放大 O(1)，
            // 不触发全量合并（全局计数在多表模式不作为触发——每表段数受控 ⇒ 全局有界）
            return Ok(CompactReport {
                merged_ssts: 0,
                kept_keys: 0,
                freed_bytes: 0,
                out_level: 0,
                dropped_keys: 0,
            });
        }
        let heat: Vec<u64> = snap.ssts.iter().map(|s| s.heat()).collect();
        // Ex-8.11：L1 段数上限（L0 活跃时纳入 L0+L1 合并的"已满"界限）——>0 时用 l1_trigger_files
        let l1_tf = self.l1_trigger_files.load(Ordering::Relaxed);
        let cap = if l1_tf > 0 {
            l1_tf
        } else {
            self.effective_l0_threshold()
        };
        let (sel, out_level) = select_compaction_inputs_ex(
            &snap.levels,
            cap,
            l1_tf,
            self.l2_trigger_files,
            &heat,
            &self.cooling_indices(),
            &snap.sizes,
            self.compact_input_max_bytes,
        );
        if sel.len() <= 1 {
            return Ok(CompactReport {
                merged_ssts: 0,
                kept_keys: 0,
                freed_bytes: 0,
                out_level: 0,
                dropped_keys: 0,
            });
        }
        // M3（表切分）：底层（L1→L2 / L2）合并仅当存在**同表多段或混表段**才有价值——
        // 跨表每表各 1 段的收敛态直接跳过（防多表库底层反复全量重写空转）。
        if self.split_by_table && out_level >= 2 && !self.sel_has_merge_work(&sel) {
            return Ok(CompactReport {
                merged_ssts: 0,
                kept_keys: 0,
                freed_bytes: 0,
                out_level: 0,
                dropped_keys: 0,
            });
        }
        if let Some(rep) = self.try_meta_only_compact(&sel, out_level)? {
            return Ok(rep);
        }
        self.compact_merge(&sel, out_level, &|_| false)
    }

    /// Leveled-Compaction（design 4.5 二期 / M6-2）：分层压实，限制单次压实量。
    ///
    /// - **L0 → L1**：有 L0 段（刷盘产物，允许重叠）时合并 L0 → 单个 L1 段（不合并既有 L1，
    ///   单次压实量 = 单个刷盘批次，有界）；L1 文件数达到层上限时改合并 L0 + 全部 L1（收敛）；
    /// - **L1 → L2**：L0 为空且 L1 段数 > 1 时，合并全部 L1 → 单个 L2 段（压实下沉）；
    /// - 合并语义：按 (key 升序, seq 降序) 排序去重，后写覆盖先写，Tombstone 保留；
    /// - **Ex-5.6 删除位图过滤**：`drop_key` 返回 true 的 key **物理丢弃**（不保留数据、
    ///   不写 Tombstone）——位图已删文档的旧数据在合并时直接回收（墓碑不污染层级；
    ///   位图标记保留，put 复活时清位）；
    /// - 崩溃安全：新段写入 → fsync → 原子更新 Manifest → 删除旧段；
    /// - 后台 IO 限速：写完后按实际文件字节 acquire。
    /// O 项第③步：`&self`（ssts 快照 load + store 原子发布）——后台合并线程可在读锁下
    /// 执行（合并不阻塞读；与写互斥由 Engine 锁保证，快照 store 无并发丢失）。
    pub fn compact_filtered(&self, drop_key: &dyn Fn(&[u8]) -> bool) -> Result<CompactReport> {
        let snap = self.ssts.load();
        let heat: Vec<u64> = snap.ssts.iter().map(|s| s.heat()).collect();
        // Ex-8.11：同 compact()，L1 段数上限 = l1_trigger_files（>0 时）
        let l1_tf = self.l1_trigger_files.load(Ordering::Relaxed);
        let cap = if l1_tf > 0 {
            l1_tf
        } else {
            self.effective_l0_threshold()
        };
        let (sel, out_level) = select_compaction_inputs_ex(
            &snap.levels,
            cap,
            l1_tf,
            self.l2_trigger_files,
            &heat,
            &self.cooling_indices(),
            &snap.sizes,
            self.compact_input_max_bytes,
        );
        if sel.len() <= 1 {
            return Ok(CompactReport {
                merged_ssts: 0,
                kept_keys: 0,
                freed_bytes: 0,
                out_level: 0,
                dropped_keys: 0,
            });
        }
        self.compact_merge(&sel, out_level, drop_key)
    }

    /// Ex-8.7：删除密度 GC 压实——先走常规 Leveled 选段（多段候选时等价 `compact_filtered`，
    /// 合并中按位图物理丢弃已删键）；**无多段候选**（如已收敛为单底层段）且 `allow_single=true`
    /// 时重写单个最底层段回收删除空间（删除位图语义下 delete 不写 Tombstone，已删旧数据
    /// 只能靠压实物理丢弃；收敛后单段不再参与常规合并，故需显式 GC 重写）。
    /// 单段重写绕开 Ex-5.8 块级复用（元数据拼接无法丢键），走全量合并保证 `drop_key` 生效；
    /// `allow_single=false` = 传统 `compact_filtered`（单段不重写，兼容既有调用）。
    pub fn compact_gc(
        &self,
        drop_key: &dyn Fn(&[u8]) -> bool,
        allow_single: bool,
    ) -> Result<CompactReport> {
        let empty = CompactReport {
            merged_ssts: 0,
            kept_keys: 0,
            freed_bytes: 0,
            out_level: 0,
            dropped_keys: 0,
        };
        let snap = self.ssts.load();
        let heat: Vec<u64> = snap.ssts.iter().map(|s| s.heat()).collect();
        let l1_tf = self.l1_trigger_files.load(Ordering::Relaxed);
        let cap = if l1_tf > 0 {
            l1_tf
        } else {
            self.effective_l0_threshold()
        };
        let (sel, out_level) = select_compaction_inputs_ex(
            &snap.levels,
            cap,
            l1_tf,
            self.l2_trigger_files,
            &heat,
            &self.cooling_indices(),
            &snap.sizes,
            self.compact_input_max_bytes,
        );
        if sel.len() >= 2 {
            // M3（表切分）：底层跨表不相交段（每表各 1 段）已按表收敛，多段全量合并无
            // 额外回收 → 落到单段 GC 重写（删除空间仍可按段回收，防多表库 GC 反复合并空转）
            if !(self.split_by_table && out_level >= 2 && !self.sel_has_merge_work(&sel)) {
                return self.compact_merge(&sel, out_level, drop_key);
            }
        }
        if !allow_single {
            return Ok(empty);
        }
        let Some((idx, lvl)) = self.pick_gc_single(&snap) else {
            return Ok(empty);
        };
        self.compact_merge(&[idx], lvl, drop_key)
    }

    /// M3（§26 多表）：段归属表 id（快照内 sst 下标 → Some(table_id)）。
    /// 仅 min/max 同属一表视为单表段；混表（老格式）段 / 空段 / 非 8 字节键 → None。
    pub(crate) fn sst_table_of(&self, i: usize) -> Option<u16> {
        let snap = self.ssts.load();
        match snap.ssts[i].key_range() {
            Some((mn, mx)) => match (key_table_id(mn), key_table_id(mx)) {
                (Some(a), Some(b)) if a == b => Some(a),
                _ => None,
            },
            None => None,
        }
    }

    /// M3：表切分列族底部（L0 空时的 L1/L2）是否需合并——按表、按层口径，且**尊重
    /// Ex-8.11 攒批配置**（l1/l2_trigger_files>0 时按层文件总数攒批一次下沉/收敛，0 = 按
    /// 同表 ≥2 段即触发）：
    /// - L1 或 L2 **某一层内**存在**同表 ≥2 段**（有去重/下沉收益）或混表老段即触发；
    /// - 跨表每层每表各 1 段视为已收敛 → 不触发（防多表库底层反复重写空转）。
    fn split_bottom_merge_work(&self) -> bool {
        let snap = self.ssts.load();
        let mut l1: std::collections::HashMap<u16, usize> = std::collections::HashMap::new();
        let mut l2: std::collections::HashMap<u16, usize> = std::collections::HashMap::new();
        let (mut l1n, mut l2n) = (0usize, 0usize);
        for (i, lv) in snap.levels.iter().enumerate() {
            if *lv == 0 {
                continue;
            }
            match self.sst_table_of(i) {
                Some(t) => {
                    let counts = if *lv == 1 { &mut l1 } else { &mut l2 };
                    *counts.entry(t).or_insert(0) += 1;
                    if *lv == 1 {
                        l1n += 1;
                    } else {
                        l2n += 1;
                    }
                }
                None => return true, // 底部混表老段：需合并切分
            }
        }
        let l1_tf = self.l1_trigger_files.load(Ordering::Relaxed);
        let has_l1_table_with_multi = l1.values().any(|&c| c >= 2);
        let l1_gate = if l1_tf > 0 {
            l1n >= l1_tf || has_l1_table_with_multi
        } else {
            has_l1_table_with_multi
        };
        if l1_gate {
            return true;
        }
        if l1_tf > 0 && l1n > 0 {
            return false; // 延迟模式：L1 未达阈值，所有表单段，等批次到齐（不提前收敛 L2）
        }
        let l2_gate = if self.l2_trigger_files > 0 {
            l2n >= self.l2_trigger_files
        } else {
            l2.values().any(|&c| c >= 2)
        };
        l2_gate
    }

    /// 是否需要 Compaction（design 4.5 二期）：
    /// L0 段数超过 `storage.l0_stall_threshold`（L0→L1），或 L0 为空但 L1 段数 > 1（L1→L2），
    /// 或 L0/L1 均空但 L2 段数 > 1（L2 收敛）。
    /// P 项：启用 `l0_max_size_mb` 时叠加**大小阈值**——L0 文件总字节超限即触发（防大段少量堆积）。
    pub fn needs_compact(&self) -> bool {
        let snap = self.ssts.load();
        // P129：多表 per-table 模式——主触发 = 存在表 L0 段数 ≥ 阈值（每表受控 ⇒ 全局有界，
        // 全局段数/大小阈值在此模式不作触发——L0 总量 = N 表 × ≤1 段为收敛态）；L0 空 → 原 bottom。
        if self.per_table_active(&snap) {
            return self.table_l0_subset(&snap).is_some();
        }
        let l0 = snap.levels.iter().filter(|l| **l == 0).count();
        let l1 = snap.levels.iter().filter(|l| **l == 1).count();
        let l2 = snap.levels.iter().filter(|l| **l >= 2).count();
        // L 项：用动态有效阈值（写压力高 → 阈值收窄 → 更早触发收敛）
        l0 > self.effective_l0_threshold()
            // P 项：大小阈值需 ≥2 段（单段为已排序文件，合并是纯无收益重写）
            || (l0 >= 2 && self.l0_max_size_bytes > 0 && self.l0_bytes() > self.l0_max_size_bytes)
            || (l0 == 0 && self.bottom_needs_compact(l1, l2))
    }

    /// Ex-8.11：L0 空时的底层合并触发（L1→L2 下沉 / L2 收敛）。
    /// `l1_trigger_files>0`：L1 **攒够阈值**才下沉 L2（延迟大合并），且 L1 未达阈值期间
    /// 不提前收敛 L2（等批次到齐一起下沉，减底层重写）；0 = 现行为（L1>1 或 L2>1 即触发）。
    /// `l2_trigger_files>0`：L2 攒够阈值才收敛为单段；0 = 现行为（L2>1 即收敛）。
    /// M3（表切分列族）：改按表口径——同表 ≥2 段 / 混表老段才触发（见 `split_bottom_merge_work`）；
    /// L1→L2 下沉仅在 L1 存在同表 ≥2 段（或混表）时进行，跨表每表 1 段停在当前层即收敛。
    fn bottom_needs_compact(&self, l1: usize, l2: usize) -> bool {
        if self.split_by_table {
            return self.split_bottom_merge_work();
        }
        let l1_tf = self.l1_trigger_files.load(Ordering::Relaxed);
        let l1_gate = if l1_tf > 0 {
            l1 >= l1_tf
        } else {
            l1 > 1
        };
        if l1_gate {
            return true;
        }
        if l1_tf > 0 && l1 > 0 {
            return false; // 延迟模式：L1 未达阈值，等批次到齐
        }
        let l2_gate = if self.l2_trigger_files > 0 {
            l2 >= self.l2_trigger_files
        } else {
            l2 > 1
        };
        l2_gate
    }

    /// L 项：当前处于冷却期的段下标集合（按 path 匹配到期轮次 > 当前合并轮次）。
    pub(crate) fn cooling_indices(&self) -> std::collections::HashSet<usize> {
        let mut out = std::collections::HashSet::new();
        if self.compaction_cooldown == 0 {
            return out;
        }
        let cooldown = self.cooldown.lock().unwrap();
        let round = self.merge_round.load(Ordering::Relaxed);
        for (i, s) in self.ssts.load().ssts.iter().enumerate() {
            if cooldown
                .get(&s.path().to_path_buf())
                .map(|exp| *exp > round)
                .unwrap_or(false)
            {
                out.insert(i);
            }
        }
        out
    }

    /// 当前 L0 段数（层号 == 0）。
    pub fn l0_count(&self) -> usize {
        self.ssts.load().levels.iter().filter(|l| **l == 0).count()
    }

    /// W 项：合并紧迫度（跨列族调度优先级，越高越优先）——
    /// L0 段数压力 ×10（主因子）+ 大小软阈值超限 ×8。热段选段已由
    /// `select_compaction_inputs`（Ex-5.9）在列族内承担，此处为跨列族调度主因子。
    pub fn compaction_urgency(&self) -> u32 {
        let l0 = self.l0_count() as u32;
        let mut u = l0.saturating_mul(10);
        if self.l0_max_size_bytes > 0 && self.l0_bytes() > self.l0_max_size_bytes {
            u += 8;
        }
        u
    }

    /// 当前 L0 段文件总字节（快照 sizes 缓存求和，零 syscall——open/flush/compact
    /// 构建快照时缓存每段大小，写路径 needs_compact 不再逐次 fs::metadata）。
    pub fn l0_bytes(&self) -> u64 {
        let snap = self.ssts.load();
        let mut total = 0u64;
        for (i, s) in snap.ssts.iter().enumerate() {
            if snap.levels.get(i).copied().unwrap_or(0) == 0 {
                total += snap.sizes.get(i).copied().unwrap_or_else(|| s.file_len());
            }
        }
        total
    }
    /// 全部 SST 文件字节总和（快照 sizes 缓存，零 syscall）。
    pub fn sst_bytes(&self) -> u64 {
        let snap = self.ssts.load();
        snap.ssts
            .iter()
            .enumerate()
            .map(|(i, s)| snap.sizes.get(i).copied().unwrap_or_else(|| s.file_len()))
            .sum()
    }
}
