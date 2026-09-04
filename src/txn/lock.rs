//! docid 级事务锁表（F 阶段三）：排他写锁 + 共享读锁 + wait-for 死锁检测。

use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::{Error, Result};

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

#[cfg(test)]
mod tests {
    use super::*;

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
