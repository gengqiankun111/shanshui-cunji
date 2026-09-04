//! 写路径：put / delete / WAL 追加与统一提交（sync_wal / ensure_wal_room）。

use std::sync::atomic::Ordering;

use crate::error::{Error, Result};
use crate::keys::encode_docid;
use crate::wal::{OP_DELETE, OP_PUT};

use super::*;

impl ColumnFamily {
    /// 写入（主键点写，便捷封装）。
    pub fn put(&mut self, docid: u64, value: Vec<u8>) -> Result<u64> {
        self.put_bytes(encode_docid(docid).to_vec(), value)
    }

    /// 写入原始字节键（组合索引等任意 key 使用）。
    pub fn put_bytes(&self, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        let seq = self.put_bytes_nosync(key, value)?;
        self.sync_wal()?;
        Ok(seq)
    }

    /// 批量写入（不逐条 fsync，由调用方最终 `sync_wal` 统一提交）。
    /// 供亿级数据压测/导入使用；强安全模式逐条写请用 `put_bytes`。
    pub fn put_bytes_nosync(&self, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        let seq = match self.wal_append(OP_PUT, &key, Some(&value)) {
            Ok(s) => s,
            // 环形 WAL 缓冲满：先落盘腾空（必要时强制 Flush）再重试
            Err(Error::WalFull(_)) => {
                self.ensure_wal_room()?;
                self.wal
                    .lock()
                    .unwrap()
                    .append(OP_PUT, &key, Some(&value))?
            }
            Err(e) => return Err(e),
        };
        self.memtable.put(key, seq, value);
        self.maybe_flush()?;
        Ok(seq)
    }

    /// 统一提交 WAL 缓冲（批量写入结束时调用）。
    /// 环形 WAL 落盘若需回绕覆盖未刷盘记录 → 强制 Flush 后重试。
    pub fn sync_wal(&self) -> Result<()> {
        let r = self.wal.lock().unwrap().sync();
        match r {
            Ok(()) => Ok(()),
            Err(Error::WalFull(_)) => {
                self.switch_and_flush()?;
                self.wal.lock().unwrap().sync()
            }
            Err(e) => Err(e),
        }
    }

    /// 删除（Tombstone，跨 flush/重启一致，见步骤 9）。
    pub fn delete(&self, docid: u64) -> Result<u64> {
        self.delete_bytes(encode_docid(docid).to_vec())
    }

    /// 仅写 WAL 删除记录（Ex-5.6 删除位图路径）：不写 memtable Tombstone、不逐条 fsync
    /// （由 `sync_wal`/组提交统一提交）——墓碑不进入 LSM 层级；
    /// 记录保留用于增量备份导出与崩溃回放（Engine 层回放转 `Engine::delete` 重新置位，幂等）。
    pub fn delete_record_wal(&self, key: Vec<u8>) -> Result<u64> {
        match self.wal_append(OP_DELETE, &key, None) {
            Ok(s) => Ok(s),
            Err(Error::WalFull(_)) => {
                self.ensure_wal_room()?;
                self.wal_append(OP_DELETE, &key, None)
            }
            Err(e) => Err(e),
        }
    }

    /// Ex-5.6 位图删除路径的**版本化删除**：WAL 删除记录 + **memtable Tombstone**（不逐条
    /// fsync，组提交统一提交）——缺陷 B/C4 修复：删除后 put 复活（清位图）时，快照读须按
    /// 版本判定"快照点位于[删除, 复活) → 不可见"。仅位图即时隐藏会在复活清位后丢失删除
    /// 信息，快照读回读到复活前旧版本 = 幻影。Tombstone 进版本链后该区间读恒为已删。
    pub fn delete_record_mem(&self, key: Vec<u8>) -> Result<u64> {
        let seq = match self.wal_append(OP_DELETE, &key, None) {
            Ok(s) => s,
            Err(Error::WalFull(_)) => {
                self.ensure_wal_room()?;
                self.wal_append(OP_DELETE, &key, None)?
            }
            Err(e) => return Err(e),
        };
        self.memtable.delete(key, seq);
        Ok(seq)
    }

    /// 删除原始字节键。
    pub fn delete_bytes(&self, key: Vec<u8>) -> Result<u64> {
        let seq = match self.wal_append(OP_DELETE, &key, None) {
            Ok(s) => s,
            Err(Error::WalFull(_)) => {
                self.ensure_wal_room()?;
                self.wal_append(OP_DELETE, &key, None)?
            }
            Err(e) => return Err(e),
        };
        self.sync_wal()?;
        self.memtable.delete(key, seq);
        Ok(seq)
    }

    /// 环形 WAL 满处理：先落盘缓冲；仍满（回绕需覆盖未刷盘记录）则强制 Flush 后重试。
    fn ensure_wal_room(&self) -> Result<()> {
        let r = self.wal.lock().unwrap().sync();
        match r {
            Ok(()) => Ok(()),
            Err(Error::WalFull(_)) => {
                self.switch_and_flush()?;
                self.wal.lock().unwrap().sync()
            }
            Err(e) => Err(e),
        }
    }

    /// 删除指定前缀的全部记录（阶段 1.5 Delta CF：全量 put 覆盖后清空该 docid 的增量）。
    /// 前缀上界 = prefix ++ [0xFF;4]（字段名长度前缀上限），闭区间扫描后逐条墓碑。
    pub fn delete_prefix(&self, prefix: &[u8]) -> Result<u64> {
        let mut end = prefix.to_vec();
        end.extend_from_slice(&[0xFF; 4]);
        let rows = self.scan_raw_range(Some(prefix), Some(&end))?;
        let mut deleted = 0u64;
        for (k, _) in rows {
            if k.starts_with(prefix) {
                self.delete_bytes(k)?;
                deleted += 1;
            }
        }
        Ok(deleted)
    }
    /// 分配 seq 并追加 WAL 记录：外部全局 seq 优先（engine MVCC），否则内部自增。
    fn wal_append(&self, op: u8, key: &[u8], value: Option<&[u8]>) -> Result<u64> {
        match &self.external_seq {
            Some(ext) => {
                let seq = ext.fetch_add(1, Ordering::Relaxed);
                self.wal.lock().unwrap().append_at(op, key, value, seq)?;
                Ok(seq)
            }
            None => self.wal.lock().unwrap().append(op, key, value),
        }
    }
}
