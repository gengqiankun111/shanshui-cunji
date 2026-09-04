//! 事务子系统（development D/E/F：LSM 事务三阶段）。
//!
//! - 阶段一（D）`WriteBatch`：用户端攒批 → `Engine::write` 单次 WAL fsync 原子提交；
//!   失败回滚语义 = 提交前预校验（`validate`）→ 未应用前无副作用，回滚即丢弃。
//! - 阶段二（E）快照隔离：`Transaction` 持有事务开始时的全局快照 seq，
//!   `Engine::txn_get` 走 `get_at(snapshot_seq)` 一致性快照读（复用 design 4.7 MVCC）；
//!   提交时写写冲突检测（目标在快照后被并发事务修改 → `TxnConflict`）。
//! - 阶段三（F）完整 ACID：隔离级别（RC/RR/SERIALIZABLE）+ docid 级锁表 +
//!   wait-for 图死锁检测（冲突事务为受害者 abort，调用方可重试）。
//!
//! 模型边界：单引擎本地事务（写路径本地原子，WAL 批次重放无中间态）；
//! 分布式事务由 Ex-1 本地消息表 / L1 SAGA 覆盖（design_extension.md 第 6 章决策）。
//!
//! 模块结构：
//! - [`Transaction`]：事务句柄（快照/写集/锁登记，本文件）
//! - [`Isolation`]：隔离级别档位（[`isolation`]）
//! - [`Op`]/[`WriteBatch`]：写操作与原子写批次（[`write_batch`]）
//! - [`LockTable`]：docid 级锁表与死锁检测（[`lock`]）

mod isolation;
mod lock;
mod write_batch;

pub use isolation::Isolation;
pub use lock::LockTable;
pub use write_batch::{Op, WriteBatch};

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};

static TXN_SEQ: AtomicU64 = AtomicU64::new(1);

/// 事务句柄（E 阶段二快照隔离 + F 阶段三锁）。
/// 不持有引擎引用：读写/提交均以 `&mut Engine` 参数传入（单引擎 Mutex 串行模型）。
pub struct Transaction {
    pub id: u64,
    pub isolation: Isolation,
    snapshot_seq: u64,
    ops: Vec<Op>,
    /// 写目标集合（提交时写写冲突检测）。
    write_set: HashSet<u64>,
    /// 已加锁的 docid（提交/回滚时释放）。
    locks: Vec<u64>,
    /// 缺陷 A+P1-4（FOR UPDATE 当前读锁定集）：docid → 当前读时引擎最新提交 seq。
    /// RR 提交写写冲突检测对"快照后被并发提交"的键一律冲突——但 **FOR UPDATE 当前读
    /// 已显式读到最新版本**（MySQL 语义：当前读加锁后写该行允许）。此集合记录当前读
    /// 时的最新 seq：提交时若该键最新 seq **仍等于**记录值（期间无并发写）→ 放行；
    /// 若已被并发事务再次修改（seq 前进）→ 仍按冲突处理（乐观锁正确性）。
    locked_cur: HashMap<u64, u64>,
    /// T 项：事务内点查快照缓存（docid → 快照读结果）——RR 快照读刻意不走 HotCache
    /// （防污染全局热缓存）→ 事务内重复点查冷读放大；小容量（≤256 项）同 key 二次读直达，
    /// 提交/回滚随 Transaction drop 即弃。仅 RR/SERIALIZABLE 快照读启用（RC 读最新不缓存）。
    /// 命中前置条件：快照 seq 事务内恒定 → 重复读结果一致（正确性无副作用）。
    snap_cache: std::collections::HashMap<u64, Option<Vec<u8>>>,
    finished: bool,
}

impl Transaction {
    pub fn new(isolation: Isolation, snapshot_seq: u64) -> Self {
        Transaction {
            id: TXN_SEQ.fetch_add(1, Ordering::Relaxed),
            isolation,
            snapshot_seq,
            ops: Vec::new(),
            write_set: HashSet::new(),
            locks: Vec::new(),
            locked_cur: HashMap::new(),
            snap_cache: std::collections::HashMap::new(),
            finished: false,
        }
    }

    pub fn snapshot(&self) -> u64 {
        self.snapshot_seq
    }

    pub fn ops(&self) -> &[Op] {
        &self.ops
    }

    pub fn write_set(&self) -> &HashSet<u64> {
        &self.write_set
    }

    /// P1-4：记录 FOR UPDATE 当前读锁定（docid → 读取时引擎最新提交 seq）。
    /// 重复当前读同键以最新一次为准（覆盖旧 seq）。
    pub fn mark_current_lock(&mut self, docid: u64, seq: u64) {
        self.locked_cur.insert(docid, seq);
    }

    /// P1-4：该键是否被本事务 FOR UPDATE 当前读锁定过（返回读取时的最新 seq）。
    pub fn cur_lock_seq(&self, docid: u64) -> Option<u64> {
        self.locked_cur.get(&docid).copied()
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// 事务内写：攒批（不立即应用，commit 时原子提交）。
    pub fn put(&mut self, docid: u64, value: Vec<u8>, terms: Vec<String>) {
        self.write_set.insert(docid);
        self.ops.push(Op::Put {
            docid,
            value,
            terms,
        });
    }

    pub fn delete(&mut self, docid: u64) {
        self.write_set.insert(docid);
        self.ops.push(Op::Delete { docid });
    }

    /// 同事务读可见（H-4）：读取本事务未提交的写（最近写优先）。
    /// 返回 `None` = 事务未写该 docid（应走引擎）；`Some(Some(v))` = 本事务 put 的最新值；
    /// `Some(None)` = 本事务已 delete（读为空）。
    pub fn read_own(&self, docid: u64) -> Option<Option<&[u8]>> {
        for op in self.ops.iter().rev() {
            match op {
                Op::Put {
                    docid: d,
                    value,
                    ..
                } if *d == docid => return Some(Some(value.as_slice())),
                Op::Delete { docid: d } if *d == docid => return Some(None),
                _ => {}
            }
        }
        None
    }

    /// T 项：事务内点查快照缓存查询。命中返回 `Some(结果)`；未命中返回 `None`（应走引擎）。
    /// 缓存仅存快照读结果（docid → value），`Some(None)` = 快照点该 key 不存在/已删除。
    pub fn snap_get(&self, docid: u64) -> Option<Option<Vec<u8>>> {
        self.snap_cache.get(&docid).cloned()
    }

    /// T 项：写入事务内点查快照缓存。容量超限（>256 项）清空重置（事务内唯一 key 通常
    /// 远小于 256；清空比 LRU 更简单且命中损失可忽略）。
    pub fn snap_put(&mut self, docid: u64, value: Option<Vec<u8>>) {
        if self.snap_cache.len() >= 256 {
            self.snap_cache.clear();
        }
        self.snap_cache.insert(docid, value);
    }

    /// 标记已获取 docid 锁（Engine 的 txn_get/txn_put 调用锁表后登记）。
    pub(crate) fn add_lock(&mut self, docid: u64) {
        if !self.locks.contains(&docid) {
            self.locks.push(docid);
        }
    }

    pub(crate) fn mark_finished(&mut self) {
        self.finished = true;
    }

    /// 校验写目标不变量（提交前）。
    pub fn validate(&self) -> Result<()> {
        if self.write_set.contains(&0) {
            return Err(Error::TxnConflict(format!(
                "docid=0 非法写目标（txn#{}）",
                self.id
            )));
        }
        Ok(())
    }
}
