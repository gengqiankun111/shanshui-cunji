//! 文档引擎 MVCC 快照层（reconstruct.md engine/mvcc.rs）：快照点分配（begin_snapshot）、
//! 活跃快照注册/注销（active_snapshots，compact 保活水位依据）、快照读（get_at），
//! 以及 compact 前后 MVCC 保活水位 wrapper（apply/clear_mvcc_floor）。
//! 内容拆分自原 src/engine.rs（MVCC 主题 impl 块）；Engine 相关私有字段以 `pub(crate)`
//! 提升后跨文件访问（语义零变化）。

use std::sync::atomic::Ordering;

use crate::engine::Engine;
use crate::error::Result;
use crate::keys::encode_docid;

impl Engine {
    /// 获取当前快照点（已分配的最大 seq）：此后以该值为快照的 `get_at` 读到一致视图。
    /// Task-026：per-CPU external WAL 下 CF 自身 wal 不推进 → 以 engine 全局 seq 为准
    /// （= 已分配最大 gseq；与 `current_seq` 同源）。
    pub fn begin_snapshot(&self) -> u64 {
        self.global_seq.load(Ordering::Relaxed).saturating_sub(1)
    }

    /// R4：注册活跃快照 seq（RR/Serializable 事务 begin 时调用；commit/rollback 注销），
    /// 记录注册时刻（unix ms，生命周期观测/受控逐出）。集合最小值 = compact 保活水位。
    pub fn register_active_snapshot(&self, seq: u64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.active_snapshots.write().unwrap().insert(seq, now);
    }

    /// R4：注销活跃快照 seq（事务 commit/rollback）。
    pub fn unregister_active_snapshot(&self, seq: u64) {
        self.active_snapshots.write().unwrap().remove(&seq);
    }

    /// R4：当前活跃快照低水位（0 = 无活跃快照 → compact 不保活，现状版本收敛/物理回收）。
    pub(crate) fn snapshot_floor(&self) -> u64 {
        self.active_snapshots
            .read()
            .unwrap()
            .first_key_value()
            .map(|(k, _)| *k)
            .unwrap_or(0)
    }

    /// 2026-09-05（快照生命周期管控）：活跃快照数量。
    pub fn active_snapshot_count(&self) -> usize {
        self.active_snapshots.read().unwrap().len()
    }

    /// 2026-09-05（快照生命周期管控）：最老活跃快照存活时长（ms；0 = 无活跃快照）。
    /// 以注册时刻最早者计（BTreeMap 按键序，逐项取最小时间戳）。
    pub fn oldest_snapshot_age_ms(&self) -> u64 {
        let g = self.active_snapshots.read().unwrap();
        if g.is_empty() {
            return 0;
        }
        let oldest_reg = g.values().copied().min().unwrap_or(0);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        now.saturating_sub(oldest_reg)
    }

    /// 2026-09-05（快照生命周期管控）：**受控逐出**——移除存活超过 `max_age_ms` 的快照
    /// 并返回被逐出的 seq 列表（降低 compact 保活水位，放行旧版本 GC）。
    /// ⚠️ 语义风险：被逐出的快照对应事务若仍存活并继续快照读，可能读到 compaction 后
    /// 已回收的旧版本（破坏 RR）。调用方**仅应在确认对应事务已中止/超时**（如空闲会话
    /// 自动回滚链路）时调用；本引擎不主动自动逐出，交由上层策略。
    pub fn snapshot_evict_older_than(&self, max_age_ms: u64) -> Vec<u64> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let expired: Vec<u64> = {
            let g = self.active_snapshots.read().unwrap();
            g.iter()
                .filter(|(_, t)| now.saturating_sub(**t) > max_age_ms)
                .map(|(k, _)| *k)
                .collect()
        };
        if expired.is_empty() {
            return expired;
        }
        let mut w = self.active_snapshots.write().unwrap();
        for k in &expired {
            w.remove(k);
        }
        expired
    }

    /// 2026-09-05（快照生命周期管控）：观测 gauge（活跃快照数 / 最老存活 ms）。
    /// 与 memory_report 同格式，供 `/metrics` 与 SHOW MEMORY 采集。
    pub fn snapshot_report(&self) -> Vec<(&'static str, &'static str, u64)> {
        vec![
            ("shanshui_snapshots_active", "活跃 MVCC 快照数（RR/Serializable 事务）", self.active_snapshot_count() as u64),
            ("shanshui_snapshot_oldest_ms", "最老活跃快照存活毫秒（长事务告警）", self.oldest_snapshot_age_ms()),
        ]
    }

    /// 快照读（design 4.7 MVCC）：返回 **seq ≤ `snapshot_seq`** 的文档视图。
    /// 主数据取快照点最新版本（MemTable + SST 按 seq 过滤，Tombstone 语义保留）；
    /// **Delta 增量按全局 seq 过滤**（M7-1：跨列族共享 seq 分配，快照后的字段级热更不可见；
    /// null 删除字段 / Tombstone 均按快照点判定）。
    /// 不走 HotCache（避免快照读污染热缓存）。
    /// P0-C 方案 B（2026-09-04）：快照读路径**跳过全局删除位图**——位图只记录"最新已提交
    /// 删除状态"，不携带 seq → 快照读不能短路。让 LSM 多版本 + tombstone seq 裁决可见性：
    /// - tombstone seq ≤ snapshot_seq → 快照前已删 → None（get_bytes_at 返回 None）
    /// - tombstone seq > snapshot_seq → 快照后删 → get_bytes_at 返回旧版本值（RR 正确）
    /// RC / 非事务读（`get`）保留位图短路（最新状态语义正确）。
    /// O 项第②步：读路径 `&self`。
    /// X 项：读操作计数（事务内点查）。
    pub fn get_at(&self, docid: u64, snapshot_seq: u64) -> Result<Option<Vec<u8>>> {
        self.metrics.read_ops.fetch_add(1, Ordering::Relaxed);
        // P0-C：快照读跳过删除位图——位图不携带 seq，短路会违反 RR。
        // tombstone 已进版本链（delete_record_mem），get_bytes_at 按 seq 裁决。
        let found = self
            .primary
            .get_bytes_at(&encode_docid(docid), snapshot_seq)?;
        let Some((bv, _)) = found else {
            return Ok(None);
        };
        // P131（2026-09-06）：Delta 增量按快照 seq 门控合并（§11.2 不坍缩）——
        // 统一走 delta_overrides_range_at（CF scan_stream_at 各源归并后取 ≤ snapshot 最大 seq），
        // 替代旧 scan_raw_range_with_seq「先按最新坍缩再过滤」——同一字段键被两次 patch 时
        // （补丁1 < 快照 < 补丁2），旧路径丢补丁1（读成 base 旧值），新路径正确读补丁1。
        // 无覆盖 → 直通（非 JSON base 亦原样返回，语义与 get 一致）。
        let overrides = self.delta_overrides_range_at(Some(docid), Some(docid), snapshot_seq)?;
        if let Some(ov) = overrides.get(&docid) {
            crate::engine::read::fold_with_overrides(&bv, Some(ov)).map(Some)
        } else {
            Ok(Some(bv))
        }
    }

    /// R4：compact 前置 MVCC 保活水位（活跃快照低水位 → 各 CF）。
    pub(crate) fn apply_mvcc_floor(&self) {
        let f = self.snapshot_floor();
        self.primary.set_mvcc_keep_floor(f);
        if let Some(c) = &self.cidx {
            c.set_mvcc_keep_floor(f);
        }
        self.delta.set_mvcc_keep_floor(f);
    }

    /// R4：compact 后复位保活水位（0 = 无活跃快照，恢复现状收敛/GC 物理回收）。
    pub(crate) fn clear_mvcc_floor(&self) {
        self.primary.set_mvcc_keep_floor(0);
        if let Some(c) = &self.cidx {
            c.set_mvcc_keep_floor(0);
        }
        self.delta.set_mvcc_keep_floor(0);
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::engine::Engine;

    fn open_e() -> (tempfile::TempDir, Engine) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        (dir, Engine::open(&path, &Config::default()).unwrap())
    }

    /// 注册→计数/最老存活/低水位；注销后清零。
    #[test]
    fn snapshot_lifecycle_register_unregister() {
        let (_dir, e) = open_e();
        assert_eq!(e.active_snapshot_count(), 0);
        assert_eq!(e.oldest_snapshot_age_ms(), 0);
        assert_eq!(e.snapshot_floor(), 0);

        e.register_active_snapshot(50);
        e.register_active_snapshot(30);
        assert_eq!(e.active_snapshot_count(), 2);
        assert_eq!(e.snapshot_floor(), 30); // 最小 seq 保活水位
        assert!(e.oldest_snapshot_age_ms() < 60_000, "刚注册存活应很小");

        e.unregister_active_snapshot(30);
        assert_eq!(e.snapshot_floor(), 50);
        e.unregister_active_snapshot(50);
        assert_eq!(e.active_snapshot_count(), 0);
        assert_eq!(e.snapshot_floor(), 0);
    }

    /// 受控逐出：仅移除存活超阈值的快照，floor 随之上升（放行旧版本 GC）。
    #[test]
    fn snapshot_evict_older_than_only_expired() {
        let (_dir, e) = open_e();
        e.register_active_snapshot(10);
        e.register_active_snapshot(20);
        // 手工把 10 号快照注册时刻改老（>10s）
        e.active_snapshots.write().unwrap().insert(10, 1);
        let evicted = e.snapshot_evict_older_than(10_000);
        assert_eq!(evicted, vec![10]);
        assert_eq!(e.active_snapshot_count(), 1);
        assert_eq!(e.snapshot_floor(), 20);
        // 无超龄时不动
        let again = e.snapshot_evict_older_than(10_000);
        assert!(again.is_empty());
    }

    /// 快照观测 gauge（snapshot_report）随注册变化。
    #[test]
    fn snapshot_report_tracks_count_and_age() {
        let (_dir, e) = open_e();
        let base = e.snapshot_report();
        assert_eq!(base[0].2, 0); // active
        e.register_active_snapshot(7);
        let rep = e.snapshot_report();
        assert_eq!(rep[0].2, 1);
        assert!(rep[1].2 < 60_000);
    }
}
