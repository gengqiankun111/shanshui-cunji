//! 文档引擎事务层（reconstruct.md engine/txn.rs）：D 阶段 WriteBatch 原子提交、
//! E/F 事务状态机（txn_begin / txn_get / txn_commit / txn_rollback）、锁表/写集协调、
//! 事务内读（txn_get / scan_range_txn）与事务写目标版本查询（last_write_seq）。
//! 内容拆分自原 src/engine.rs（事务主题 impl 块）；Engine 相关私有字段以 `pub(crate)`
//! 提升后跨文件访问（语义零变化）。

use crate::engine::{Engine, QueryRow};
use crate::error::Result;
use crate::keys::encode_docid;

impl Engine {
    /// 批量写入（原子批次，用户端批量语义）：一次性提交一组 `(docid, value, terms)`——
    /// put_nosync 攒批 + 批尾统一提交（整批落盘或崩溃后按 WAL 批次整体重放，无中间态；
    /// 提交语义 = `commit_batch`：档位 1 显式 flush_wal 强安全；档位 0/2 组提交窗口，
    /// 见 P131 2026-09-06——批量 UPDATE 不再每语句双 WAL 同步 fsync）。
    /// 为 D 项（LSM 事务阶段一 WriteBatch 原子写）的前置基础；单条语义同 `put`。
    pub fn put_batch(&mut self, items: &[(u64, Vec<u8>, Vec<String>)]) -> Result<()> {
        self.watchdog.check_all(self.mem_ratio, &self.data_dir)?;
        for (docid, value, terms) in items {
            let refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            self.put_nosync(*docid, value.clone(), &refs)?;
        }
        self.commit_batch()
    }

    // ===== D/E/F 事务三阶段 =====

    /// D 阶段一：WriteBatch 原子提交。预校验（失败零副作用 = "失败回滚"）→ 逐条应用 →
    /// 单次 `flush_wal`。崩溃原子：WAL 单次 fsync 批次整体重放（整批恢复或整批丢弃，无中间态）。
    /// 等价于 `put_batch` + delete 语义 + 事务上下文（回滚 = 丢弃未应用的批次）。
    pub fn write(&mut self, batch: &crate::txn::WriteBatch) -> Result<()> {
        batch.validate()?;
        self.watchdog.check_all(self.mem_ratio, &self.data_dir)?;
        for op in batch.ops() {
            match op {
                crate::txn::Op::Put {
                    docid,
                    value,
                    terms,
                } => {
                    let refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
                    self.put_nosync(*docid, value.clone(), &refs)?;
                }
                crate::txn::Op::Delete { docid } => self.delete(*docid)?,
            }
        }
        self.flush_wal()
    }

    /// E/F：开启事务。快照 seq = 当前已分配最大全局 seq（RR/SERIALIZABLE 一致读基准）。
    /// R4：RR/SERIALIZABLE（uses_snapshot）注册为活跃快照（compact 保活水位依据）。
    pub fn txn_begin(&mut self, isolation: crate::txn::Isolation) -> crate::txn::Transaction {
        let seq = self.begin_snapshot();
        let txn = crate::txn::Transaction::new(isolation, seq);
        if isolation.uses_snapshot() {
            self.register_active_snapshot(seq);
        }
        txn
    }

    /// E/F：事务读。RC = 最新已提交（`get`）；RR/SERIALIZABLE = 快照一致读（`get_at`）；
    /// SERIALIZABLE 读目标额外加共享锁（2PL，读锁持有至提交）。
    /// H-4：先查本事务未提交的写（`read_own`，同事务写后读可见），再走引擎。
    /// O 项第②步：读路径 `&self`（RR/RC 快照只读并行；SERIALIZABLE 读锁经 `txn_locks` 内部 Mutex）。
    pub fn txn_get(
        &self,
        txn: &mut crate::txn::Transaction,
        docid: u64,
    ) -> Result<Option<Vec<u8>>> {
        if txn.is_finished() {
            return Err(crate::error::Error::TxnAborted(format!(
                "txn#{} 已结束",
                txn.id
            )));
        }
        // 同事务写后读可见（未提交的攒批写优先）
        if let Some(own) = txn.read_own(docid) {
            return Ok(own.map(|v| v.to_vec()));
        }
        // T 项：RR/SERIALIZABLE 快照读——事务内点查小缓存（同 key 二次读直达，免 LSM 冷读
        // 放大；快照 seq 事务内恒定 → 缓存结果一致）。命中跳过加锁（首次读已加）。
        if txn.isolation.uses_snapshot() {
            if let Some(v) = txn.snap_get(docid) {
                return Ok(v);
            }
        }
        if txn.isolation.locks_reads() {
            self.txn_locks.lock().unwrap().acquire_shared(txn.id, docid)?;
            txn.add_lock(docid);
        }
        let result = if txn.isolation.uses_snapshot() {
            self.get_at(docid, txn.snapshot())
        } else {
            self.get(docid)
        };
        // 仅快照读写缓存（RC 读最新，缓存会破坏语义）；错误结果不缓存
        if txn.isolation.uses_snapshot() {
            if let Ok(v) = &result {
                txn.snap_put(docid, v.clone());
            }
        }
        result
    }

    /// E/F：事务提交。写锁（write_set 全目标排他，含共享→排他升级）→
    /// 写写冲突检测（RR/SERIALIZABLE：目标在快照后被并发事务修改 → `TxnConflict` abort）→
    /// 应用 ops + 提交落盘（P2-A：档位 1 = 单次 `flush_wal` 崩溃原子；
    /// 档位 0/2 = 组提交窗口攒批，见 `commit_persist`）→ 释放全部锁（失败路径同样释放，防锁泄漏）。
    pub fn txn_commit(&mut self, mut txn: crate::txn::Transaction) -> Result<()> {
        if txn.is_finished() {
            return Err(crate::error::Error::TxnAborted(format!(
                "txn#{} 已结束",
                txn.id
            )));
        }
        let result = (|| -> Result<()> {
            txn.validate()?;
            // P52：提交前统一看门狗检查（内存/磁盘熔断）
            self.watchdog.check_all(self.mem_ratio, &self.data_dir)?;
            // ① 写锁（SERIALIZABLE 已持共享锁的 docid 自动升级为排他）
            let targets: Vec<u64> = txn.write_set().iter().copied().collect();
            for d in targets {
                self.txn_locks
                    .lock()
                    .unwrap()
                    .acquire_exclusive(txn.id, d)?;
                txn.add_lock(d);
            }
            // ② 写写冲突检测（RR/SERIALIZABLE）
            // P1-4：FOR UPDATE **当前读**已显式读到最新版本的行允许写入（MySQL 语义）——
            // 该键被当前读锁定且最新 seq 仍等于读取时记录值（期间无并发写）→ 放行；
            // 被并发事务再次修改（seq 前进）→ 仍冲突（乐观锁正确性，不覆盖并发新值）。
            if txn.isolation.checks_write_conflict() {
                for &d in txn.write_set() {
                    let cur = self.last_write_seq(d)?;
                    if cur > txn.snapshot() {
                        let locked = txn.cur_lock_seq(d);
                        if let Some(seen) = locked {
                            if cur == seen {
                                continue;
                            }
                        }
                        return Err(crate::error::Error::TxnConflict(format!(
                            "txn#{} 写冲突：docid={d} 在快照 {} 后被并发事务修改（当前 seq {cur}）",
                            txn.id,
                            txn.snapshot()
                        )));
                    }
                }
            }
            // ③ 应用 + 提交落盘（P2-A：档位感知——1 = 每次 COMMIT fsync；0/2 = 组提交窗口）
            for op in txn.ops() {
                match op {
                    crate::txn::Op::Put {
                        docid,
                        value,
                        terms,
                    } => {
                        let refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
                        self.put_nosync(*docid, value.clone(), &refs)?;
                    }
                    crate::txn::Op::Delete { docid } => self.delete(*docid)?,
                }
            }
            self.commit_persist()?;
            Ok(())
        })();
        if result.is_ok() {
            txn.mark_finished();
        }
        self.txn_locks.lock().unwrap().release(txn.id);
        // R4：快照事务终结注销活跃快照（保活水位让出）
        if txn.isolation.uses_snapshot() {
            self.unregister_active_snapshot(txn.snapshot());
        }
        result
    }

    /// E/F：事务回滚（丢弃攒批 + 释放锁，引擎零变更）。
    pub fn txn_rollback(&mut self, mut txn: crate::txn::Transaction) {
        if txn.is_finished() {
            return;
        }
        txn.mark_finished();
        self.txn_locks.lock().unwrap().release(txn.id);
        // R4：快照事务终结注销活跃快照
        if txn.isolation.uses_snapshot() {
            self.unregister_active_snapshot(txn.snapshot());
        }
    }

    /// docid 当前最新提交版本 seq（删除位图已删 → 返回 current_seq 视为已删的"最新"）。
    /// pub：FOR UPDATE 当前读需记录锁定版本（P1-4）。
    pub fn last_write_seq(&self, docid: u64) -> Result<u64> {
        if let Some(bm) = &self.deletion_bitmap {
            if bm.is_deleted(docid) {
                return Ok(self.current_seq());
            }
        }
        match self.primary.get_bytes(&encode_docid(docid))? {
            Some((_, seq)) => Ok(seq),
            None => Ok(0),
        }
    }
    /// 事务范围扫描（M 项，事务类查询优化 P0）：RR/SERIALIZABLE 走 `scan_range_at`
    /// 快照版本过滤（一次 k-way merge 扫描，替代逐 id `txn_get`）；同事务未提交写
    /// （`read_own`）覆盖扫描结果；事务内删除的 docid 从结果中排除。
    /// O 项第②步：事务范围读 `&self`（RR 快照只读并行）。
    pub fn scan_range_txn(
        &self,
        txn: &mut crate::txn::Transaction,
        start: Option<u64>,
        end: Option<u64>,
    ) -> Result<Vec<QueryRow>> {
        if txn.is_finished() {
            return Err(crate::error::Error::TxnAborted(format!(
                "txn#{} 已结束",
                txn.id
            )));
        }
        // 扫描快照视图（RC 语义 = 最新视图；RR/SERIALIZABLE = 快照过滤）
        let snapshot = if txn.isolation.uses_snapshot() {
            txn.snapshot()
        } else {
            u64::MAX
        };
        let mut out: Vec<QueryRow> = self.primary.scan_range_at(snapshot, start, end)?;
        // Ex-8.10：删除位图语义对齐（与 txn_get/get_at 一致）——快照视图先排除位图已删 docid。
        // 置于 read_own 覆盖**之前**：事务内对已删 docid 的未提交写（自写复活）仍可覆盖显现。
        // 注：位图删除为非版本化全局语义（get_at 同近似），快照不晚于删除时点亦隐藏（既有取舍）。
        if let Some(bm) = &self.deletion_bitmap {
            out.retain(|(d, _)| !bm.is_deleted(*d));
        }
        // P131（2026-09-06）：Delta Merge-on-Read 接入快照扫描——RR/SERIALIZABLE 只合成
        // `patch_seq ≤ snapshot` 的增量（§11 统一版本规则：倒排/任意 RR 查询读到的行值 =
        // 快照点 base + 可见增量）；RC 合成当前视图。置于 read_own 覆盖之前（后者整值替换）。
        let overrides = if snapshot == u64::MAX {
            self.delta_overrides_range(start, end)?
        } else {
            self.delta_overrides_range_at(start, end, snapshot)?
        };
        if !overrides.is_empty() {
            for row in out.iter_mut() {
                if let Some(ov) = overrides.get(&row.0) {
                    row.1 = crate::engine::read::fold_with_overrides(&row.1, Some(ov))?;
                }
            }
        }
        // 同事务写覆盖：write_set 中的 docid 用 read_own 值替换/排除
        for row in out.iter_mut() {
            if let Some(own) = txn.read_own(row.0) {
                match own {
                    Some(v) => row.1 = v.to_vec(),
                    None => row.1.clear(), // 事务内已删除：标记为空（下方过滤）
                }
            }
        }
        out.retain(|(_, v)| !v.is_empty());
        // Ex-8.10：事务内未提交 Put 的**新 docid / 已删复活**（基表扫描不含该行）并入窗口——
        // read_own 仅覆盖"已出现"的行（上循环）；此处对 write_set 中未见 docid 补入最新自写值。
        if !txn.ops().is_empty() {
            let mut own_ids: Vec<u64> = txn
                .ops()
                .iter()
                .filter_map(|op| match op {
                    crate::txn::Op::Put { docid, .. } => Some(*docid),
                    crate::txn::Op::Delete { .. } => None,
                })
                .collect();
            own_ids.sort_unstable();
            own_ids.dedup();
            let present: std::collections::HashSet<u64> = out.iter().map(|(d, _)| *d).collect();
            let mut added = false;
            for d in own_ids {
                if present.contains(&d) {
                    continue;
                }
                let in_win = start.map_or(true, |s| d >= s) && end.map_or(true, |e| d <= e);
                if in_win {
                    if let Some(Some(v)) = txn.read_own(d) {
                        out.push((d, v.to_vec()));
                        added = true;
                    }
                }
            }
            if added {
                out.sort_by_key(|r| r.0); // 保持升序（自写并入后重排；事务窗口通常小）
            }
        }
        Ok(out)
    }
}
