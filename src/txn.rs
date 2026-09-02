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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};

/// 隔离级别（F 阶段三）。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Isolation {
    /// Read Committed：读最新已提交版本；写目标加排他锁（写写互斥，
    /// 但不承诺快照——读可看到并发已提交的写）。
    ReadCommitted,
    /// Repeatable Read：读事务开始时一致性快照（快照隔离）；
    /// 提交时对写目标做写写冲突检测（快照后被并发事务修改 → abort）。
    RepeatableRead,
    /// Serializable：RR 快照 + 写冲突检测，叠加读目标共享锁 / 写目标排他锁
    /// （读锁持有至提交，等价于严格 2PL → 串行化）；等锁图死锁检测。
    Serializable,
}

impl Isolation {
    pub fn name(&self) -> &'static str {
        match self {
            Isolation::ReadCommitted => "READ_COMMITTED",
            Isolation::RepeatableRead => "REPEATABLE_READ",
            Isolation::Serializable => "SERIALIZABLE",
        }
    }

    /// 是否使用快照读（RR/SERIALIZABLE 读一致性视图）。
    pub fn uses_snapshot(&self) -> bool {
        !matches!(self, Isolation::ReadCommitted)
    }

    /// 是否做提交时写写冲突检测（RR/SERIALIZABLE）。
    pub fn checks_write_conflict(&self) -> bool {
        !matches!(self, Isolation::ReadCommitted)
    }

    /// 读目标是否加共享锁（SERIALIZABLE）。
    pub fn locks_reads(&self) -> bool {
        matches!(self, Isolation::Serializable)
    }
}

/// 写操作（WriteBatch 与事务 write_set 共用）。
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Put {
        docid: u64,
        value: Vec<u8>,
        terms: Vec<String>,
    },
    Delete {
        docid: u64,
    },
}

impl Op {
    pub fn docid(&self) -> u64 {
        match self {
            Op::Put { docid, .. } | Op::Delete { docid } => *docid,
        }
    }
}

static BATCH_SEQ: AtomicU64 = AtomicU64::new(1);
static TXN_SEQ: AtomicU64 = AtomicU64::new(1);

/// 原子写批次（D 阶段一）：攒批 → `Engine::write` 原子提交；回滚 = 丢弃（未应用无副作用）。
#[derive(Debug, Default)]
pub struct WriteBatch {
    ops: Vec<Op>,
    id: u64,
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch {
            ops: Vec::new(),
            id: BATCH_SEQ.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub fn put(&mut self, docid: u64, value: Vec<u8>, terms: Vec<String>) {
        self.ops.push(Op::Put {
            docid,
            value,
            terms,
        });
    }

    pub fn delete(&mut self, docid: u64) {
        self.ops.push(Op::Delete { docid });
    }

    pub fn ops(&self) -> &[Op] {
        &self.ops
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// 回滚：清空已攒批操作（尚未应用到引擎，无副作用）。
    pub fn rollback(&mut self) {
        self.ops.clear();
    }

    /// 提交前预校验：全部操作不变量合法才允许提交；失败返回 Err 且引擎零变更
    /// （这是 WriteBatch 的"失败回滚"语义——错误在应用前被发现）。
    pub fn validate(&self) -> Result<()> {
        for op in &self.ops {
            if op.docid() == 0 {
                return Err(Error::TxnConflict(format!(
                    "docid=0 非法写目标（WriteBatch#{}）",
                    self.id
                )));
            }
        }
        Ok(())
    }
}

/// 锁模式：共享读锁（SERIALIZABLE 读目标）/ 排他写锁（写目标）。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum LockMode {
    Shared,
    Exclusive,
}

/// 单个 docid 的锁状态：持有者 + 等待队列。
struct LockState {
    /// 持有者（共享可多个；排他至多一个且独占）。
    holders: Vec<u64>,
    /// 是否为排他持有。
    exclusive: bool,
    /// 等待队列（单线程模型下等待即检测死锁；队列保留语义供图构建）。
    waiters: VecDeque<(u64, LockMode)>,
}

impl LockState {
    fn new(txn: u64, mode: LockMode) -> Self {
        let exclusive = mode == LockMode::Exclusive;
        LockState {
            holders: vec![txn],
            exclusive,
            waiters: VecDeque::new(),
        }
    }

    /// 当前持有者能否满足新的 mode 请求（共享可叠加；排他需无持有）。
    fn can_grant(&self, mode: LockMode) -> bool {
        if mode == LockMode::Shared {
            !self.exclusive // 共享锁在排他持有时不放行；共享叠加放行
        } else {
            self.holders.is_empty()
        }
    }
}

/// docid 级事务锁表（F 阶段三）：排他写锁 + 共享读锁 + wait-for 死锁检测。
#[derive(Default)]
pub struct LockTable {
    locks: HashMap<u64, LockState>,
    /// txn id → 已持有的 docid 列表（提交/回滚时整体释放）。
    held: HashMap<u64, Vec<u64>>,
    /// txn id → 正在等待的 docid（死锁检测构建 wait-for 边）。
    waiting: HashMap<u64, u64>,
}

impl LockTable {
    pub fn new() -> Self {
        LockTable::default()
    }

    /// 尝试获取排他写锁。冲突时做死锁检测：发现 wait-for 环 → 请求者 abort
    /// （返回 `TxnDeadlock`）；无环 → 返回 `TxnConflict`（调用方重试）。
    pub fn acquire_exclusive(&mut self, txn: u64, docid: u64) -> Result<()> {
        self.acquire(txn, docid, LockMode::Exclusive)
    }

    /// 尝试获取共享读锁（SERIALIZABLE 读路径）。
    pub fn acquire_shared(&mut self, txn: u64, docid: u64) -> Result<()> {
        self.acquire(txn, docid, LockMode::Shared)
    }

    fn acquire(&mut self, txn: u64, docid: u64, mode: LockMode) -> Result<()> {
        let conflict_holders = if let Some(state) = self.locks.get_mut(&docid) {
            if state.can_grant(mode) {
                // 共享叠加：追加持有者（避免重复记账同 txn）
                if !state.holders.contains(&txn) {
                    state.holders.push(txn);
                }
                self.held.entry(txn).or_default().push(docid);
                return Ok(());
            }
            // 升级：排他请求且唯一持有者是自己（共享读锁 → 排他写锁，2PL 合法升级）
            if mode == LockMode::Exclusive
                && !state.exclusive
                && state.holders.len() == 1
                && state.holders[0] == txn
            {
                state.exclusive = true;
                return Ok(());
            }
            // 冲突：登记等待（单线程模型下等待即检测死锁；队列保留供图构建）
            if !state.waiters.iter().any(|(t, _)| *t == txn) {
                state.waiters.push_back((txn, mode));
            }
            Some(state.holders.clone())
        } else {
            // 无锁 → 直接授予
            self.locks.insert(docid, LockState::new(txn, mode));
            self.held.entry(txn).or_default().push(docid);
            return Ok(());
        };
        // wait-for：txn 等待 docid 的持有者 → 死锁检测（借用已结束）。
        // 无环冲突保留等待关系（后续其他事务的请求才能形成环被检测）；
        // 事务 abort / release 时统一清理。
        self.waiting.insert(txn, docid);
        let deadlocked = self.detect_deadlock(txn);
        if deadlocked {
            self.cancel_wait(txn, docid);
            return Err(Error::TxnDeadlock(format!(
                "txn#{txn} 等待 docid={docid} 检测到死锁环（victim）"
            )));
        }
        Err(Error::TxnConflict(format!(
            "txn#{txn} 获取 docid={docid} 锁冲突（持有者 {:?}）",
            conflict_holders.unwrap_or_default()
        )))
    }

    fn cancel_wait(&mut self, txn: u64, docid: u64) {
        if let Some(state) = self.locks.get_mut(&docid) {
            state.waiters.retain(|(t, _)| *t != txn);
        }
        self.waiting.remove(&txn);
    }

    /// wait-for 环检测：从 txn 出发，沿「txn 等待 docid → docid 持有者 txn' → txn' 等待的 docid → …」
    /// DFS 找环。返回 true = 有环（当前 txn 为受害者）。
    fn detect_deadlock(&self, txn: u64) -> bool {
        let mut stack = vec![(txn, 0u32)];
        let mut visited: HashSet<u64> = HashSet::new();
        while let Some((cur, depth)) = stack.pop() {
            if depth > 0 && cur == txn {
                return true; // 回到起点 = 环
            }
            if !visited.insert(cur) {
                continue;
            }
            // cur 等待的 docid
            let Some(&waited_docid) = self.waiting.get(&cur) else {
                continue;
            };
            // 该 docid 的持有者
            let Some(state) = self.locks.get(&waited_docid) else {
                continue;
            };
            let holders = state.holders.clone();
            for h in holders {
                if h == cur {
                    continue;
                }
                if self.waiting.contains_key(&h) || h == txn {
                    stack.push((h, depth + 1));
                }
            }
        }
        false
    }

    /// 释放事务持有的全部锁（提交/回滚）。
    /// 单线程"等待即失败"模型：持有者清空即删除该 docid 的锁状态（等待者不会自动获锁，
    /// 已被告知冲突，重试会重新 acquire；残留 waiters 无意义，一并清理）。
    pub fn release(&mut self, txn: u64) {
        if let Some(docids) = self.held.remove(&txn) {
            for docid in docids {
                if let Some(state) = self.locks.get_mut(&docid) {
                    state.holders.retain(|h| *h != txn);
                    if state.holders.is_empty() {
                        self.locks.remove(&docid);
                    } else {
                        // 仍有其他持有者（共享叠加）：清除排他标记
                        state.exclusive = false;
                        state.waiters.clear();
                    }
                }
            }
        }
        self.waiting.remove(&txn);
    }

    /// 当前锁数量（测试/诊断）。
    pub fn lock_count(&self) -> usize {
        self.locks.len()
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_batch_rollback_clears_ops() {
        let mut b = WriteBatch::new();
        b.put(1, vec![1], vec![]);
        b.delete(2);
        assert_eq!(b.len(), 2);
        b.rollback();
        assert!(b.is_empty());
    }

    #[test]
    fn write_batch_validate_rejects_zero_docid() {
        let mut b = WriteBatch::new();
        b.put(0, vec![1], vec![]);
        assert!(b.validate().is_err());
        let mut ok = WriteBatch::new();
        ok.put(42, vec![1], vec![]);
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn isolation_flags() {
        assert!(!Isolation::ReadCommitted.uses_snapshot());
        assert!(Isolation::RepeatableRead.uses_snapshot());
        assert!(Isolation::RepeatableRead.checks_write_conflict());
        assert!(!Isolation::ReadCommitted.checks_write_conflict());
        assert!(Isolation::Serializable.locks_reads());
        assert!(!Isolation::RepeatableRead.locks_reads());
    }

    #[test]
    fn lock_exclusive_conflict_and_release() {
        let mut lt = LockTable::new();
        assert!(lt.acquire_exclusive(1, 100).is_ok());
        // 第二个事务拿同一 docid 排他锁 → 冲突
        assert!(lt.acquire_exclusive(2, 100).is_err());
        lt.release(1);
        // 释放后可再获取
        assert!(lt.acquire_exclusive(2, 100).is_ok());
    }

    #[test]
    fn lock_shared_allows_concurrent_readers() {
        let mut lt = LockTable::new();
        assert!(lt.acquire_exclusive(1, 100).is_ok());
        // 排他持有下共享锁请求 → 冲突
        assert!(lt.acquire_shared(2, 100).is_err());
        lt.release(1);
        assert!(lt.acquire_shared(2, 100).is_ok());
        // 共享可叠加
        assert!(lt.acquire_shared(3, 100).is_ok());
        // 共享持有下排他请求 → 冲突
        assert!(lt.acquire_exclusive(4, 100).is_err());
        lt.release(2);
        lt.release(3);
        assert!(lt.acquire_exclusive(4, 100).is_ok());
    }

    #[test]
    fn deadlock_detected_when_wait_for_cycle() {
        let mut lt = LockTable::new();
        // txn1 持 docid 10，等待 docid 20；txn2 持 docid 20，等待 docid 10 → 环
        assert!(lt.acquire_exclusive(1, 10).is_ok());
        assert!(lt.acquire_exclusive(2, 20).is_ok());
        // txn1 请求 20（被 txn2 持）→ 无环（txn2 未等 10）→ 冲突错误
        let r1 = lt.acquire_exclusive(1, 20);
        assert!(r1.is_err() && matches!(r1, Err(Error::TxnConflict(_))));
        // txn2 请求 10（被 txn1 持，且 txn1 等 20 被 txn2 持）→ 环 → 死锁
        let r2 = lt.acquire_exclusive(2, 10);
        assert!(r2.is_err() && matches!(r2, Err(Error::TxnDeadlock(_))));
        // 释放后可恢复
        lt.release(1);
        lt.release(2);
        assert_eq!(lt.lock_count(), 0);
    }
}
