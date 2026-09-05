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

    /// R4：注册活跃快照 seq（RR/Serializable 事务 begin 时调用；commit/rollback 注销）。
    /// 集合最小值 = compact 时 MVCC 保活水位。空集合 = 无活跃快照（floor 0 = 现状回收）。
    pub fn register_active_snapshot(&self, seq: u64) {
        self.active_snapshots.write().unwrap().insert(seq);
    }

    /// R4：注销活跃快照 seq（事务 commit/rollback）。
    pub fn unregister_active_snapshot(&self, seq: u64) {
        self.active_snapshots.write().unwrap().remove(&seq);
    }

    /// R4：当前活跃快照低水位（0 = 无活跃快照 → compact 不保活，现状版本收敛/物理回收）。
    pub(crate) fn snapshot_floor(&self) -> u64 {
        self.active_snapshots.read().unwrap().first().copied().unwrap_or(0)
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
        let obj: serde_json::Value = match serde_json::from_slice(&bv) {
            Ok(v) => v,
            Err(_) => return Ok(Some(bv)),
        };
        let mut map = match obj {
            serde_json::Value::Object(m) => m,
            _ => return Ok(Some(bv)),
        };
        let start = encode_docid(docid).to_vec();
        let mut end = start.clone();
        end.extend_from_slice(&[0xFF; 4]);
        let rows = self
            .delta
            .scan_raw_range_with_seq(Some(&start), Some(&end))?;
        for (k, seq, v) in rows {
            if seq > snapshot_seq {
                continue; // 快照点之后的增量不可见
            }
            if !k.starts_with(&start) || k.len() < 12 {
                continue;
            }
            let field = String::from_utf8(k[12..].to_vec())
                .map_err(|_| crate::error::Error::Corrupted("Delta 字段名非法 UTF-8".into()))?;
            match v {
                Some(bytes) => {
                    let val: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
                        crate::error::Error::Corrupted(format!("Delta 值解析失败: {e}"))
                    })?;
                    if val.is_null() {
                        map.shift_remove(&field);
                    } else {
                        map.insert(field, val);
                    }
                }
                None => {
                    map.shift_remove(&field); // 增量删除字段（Tombstone）
                }
            }
        }
        let merged =
            serde_json::to_vec(&map).map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
        Ok(Some(merged))
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
