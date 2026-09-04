//! 原子写批次（D 阶段一）与写操作：`WriteBatch` 攒批 → `Engine::write` 原子提交；
//! `Op` 为 WriteBatch 与事务 write_set 共用的写操作。

use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};

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
}
