//! 隔离级别（F 阶段三档位）。

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn isolation_flags() {
        assert!(!Isolation::ReadCommitted.uses_snapshot());
        assert!(Isolation::RepeatableRead.uses_snapshot());
        assert!(Isolation::RepeatableRead.checks_write_conflict());
        assert!(!Isolation::ReadCommitted.checks_write_conflict());
        assert!(Isolation::Serializable.locks_reads());
        assert!(!Isolation::RepeatableRead.locks_reads());
    }
}
