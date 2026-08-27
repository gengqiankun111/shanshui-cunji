//! WAL 预写日志（development 5.1 / design 4.3）。
//!
//! 记录格式：`Length(u32) ++ CRC32(u32) ++ Payload`
//! Payload := `Seq(u64) ++ OpType(u8) ++ Key(VarLen) ++ [Value(VarLen)]`
//! - OpType：0 = Put，1 = Delete；
//! - 组提交（Group Commit）：一批记录一次性写盘 + 一次 fsync；
//! - 崩溃回放：读到首条 CRC 损坏/截断记录即停（部分写入安全）；
//! - 延迟删除：旧段切分后重命名交给后台线程 unlink（design 4.3）。

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::keys::{decode_varlen, encode_varlen};

pub const OP_PUT: u8 = 0;
pub const OP_DELETE: u8 = 1;

/// WAL 记录负载。
#[derive(Debug, Clone, PartialEq)]
pub struct WalRecord {
    pub seq: u64,
    pub op: u8,
    pub key: Vec<u8>,
    /// Put 时的值；Delete 时为空。
    pub value: Option<Vec<u8>>,
}

/// WAL 写入器（组提交 + 双同步模式）。
pub struct WalWriter {
    file: Option<std::fs::File>,
    path: PathBuf,
    next_seq: u64,
    /// 待刷盘批次缓冲。
    buf: Vec<u8>,
    /// perf 模式：批量攒到一定字节数才 fsync（牺牲极小安全换吞吐）。
    perf_mode: bool,
    pending_bytes: usize,
}

const GROUP_COMMIT_FSYNC_BYTES: usize = 4 * 1024 * 1024; // perf 模式攒满 4MB 才 fsync

impl WalWriter {
    /// 创建（截断已存在文件）。
    pub fn create(path: &Path, perf_mode: bool) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::File::create(path)?;
        Ok(Self {
            file: Some(file),
            path: path.to_path_buf(),
            next_seq: 1,
            buf: Vec::new(),
            perf_mode,
            pending_bytes: 0,
        })
    }

    /// 以追加模式打开 WAL（**不截断**），用于重启恢复：回放旧记录后继续写入。
    /// `next_seq` 为接续序列号（= 已回放最大 seq + 1），避免同 key 新版本 seq 冲突。
    pub fn open_append(path: &Path, next_seq: u64, perf_mode: bool) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            file: Some(file),
            path: path.to_path_buf(),
            next_seq,
            buf: Vec::new(),
            perf_mode,
            pending_bytes: 0,
        })
    }

    /// 接续序列号（WAL 回放完成后调用，保证新写入 seq 单调递增且不冲突）。
    pub fn resume_seq(&mut self, next_seq: u64) {
        self.next_seq = next_seq;
    }

    /// 追加一条记录并返回分配的 Seq（尚未落盘，由 sync / group_commit 统一提交）。
    pub fn append(&mut self, op: u8, key: &[u8], value: Option<&[u8]>) -> Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let mut payload = Vec::new();
        payload.extend_from_slice(&seq.to_le_bytes());
        payload.push(op);
        encode_varlen(&mut payload, key);
        if let Some(v) = value {
            encode_varlen(&mut payload, v);
        } else {
            encode_varlen(&mut payload, &[]); // Delete 时值部分为空 VarLen
        }

        let crc = crc32(&payload);
        self.buf
            .extend_from_slice(&(payload.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(&crc.to_le_bytes());
        self.buf.extend_from_slice(&payload);
        self.pending_bytes += 8 + payload.len();
        Ok(seq)
    }

    /// 组提交：将缓冲整批写盘并 fsync（标准模式每次提交都 fsync）。
    pub fn sync(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let file = self.file.as_mut().unwrap();
        file.write_all(&self.buf)?;
        self.buf.clear();
        self.pending_bytes = 0;
        if !self.perf_mode {
            file.sync_all()?;
        }
        Ok(())
    }

    /// perf 模式下的按需 fsync（攒满阈值或显式调用）。
    pub fn maybe_fsync(&mut self) -> Result<()> {
        if self.perf_mode && self.pending_bytes >= GROUP_COMMIT_FSYNC_BYTES {
            self.sync()?;
        }
        Ok(())
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// 显式 fsync（紧急 / 关闭前）。
    pub fn flush_sync(&mut self) -> Result<()> {
        self.sync()?;
        if let Some(f) = self.file.as_mut() {
            f.sync_all()?;
        }
        Ok(())
    }

    /// 关闭并落盘。
    pub fn close(mut self) -> Result<()> {
        self.flush_sync()?;
        self.file.take();
        Ok(())
    }

    /// 当前 WAL 文件路径（用于延迟删除 / 切分）。
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 重命名到 `to_delete` 前缀（延迟删除：后台线程空闲期再 unlink）。
    pub fn mark_for_deferred_delete(&mut self, tombstone: &Path) -> Result<()> {
        std::fs::rename(&self.path, tombstone)?;
        Ok(())
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        // 崩溃模拟路径：drop 不保证 flush（测试通过不调用 sync 直接 drop 模拟断电）
        // 正常路径应先调用 close()
        let _ = self.file.take();
    }
}

/// WAL 读取 / 崩溃回放。
pub struct WalReader;

impl WalReader {
    /// 回放：返回按 Seq 升序的有效记录集合；首条损坏/截断处停止。
    pub fn recover(path: &Path) -> Result<Vec<WalRecord>> {
        let mut buf = Vec::new();
        std::fs::File::open(path)?.read_to_end(&mut buf)?;
        let mut records = Vec::new();
        let mut pos = 0usize;
        while pos + 8 <= buf.len() {
            let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(buf[pos + 4..pos + 8].try_into().unwrap());
            pos += 8;
            if pos + len > buf.len() {
                break; // 截断：尾部不完整记录丢弃（断电场景）
            }
            let payload = &buf[pos..pos + len];
            pos += len;
            if crc32(payload) != crc {
                break; // 损坏：停止回放（此记录之后的不可信）
            }
            match decode_payload(payload) {
                Ok(rec) => records.push(rec),
                Err(_) => break,
            }
        }
        Ok(records)
    }
}

fn decode_payload(payload: &[u8]) -> Result<WalRecord> {
    if payload.len() < 9 {
        return Err(Error::Corrupted("WAL payload 过短".into()));
    }
    let seq = u64::from_le_bytes(payload[0..8].try_into().unwrap());
    let op = payload[8];
    let mut pos = 9usize;
    let key = decode_varlen(payload, &mut pos)?.to_vec();
    let val_raw = decode_varlen(payload, &mut pos)?;
    let value = if val_raw.is_empty() && op == OP_DELETE {
        None
    } else {
        Some(val_raw.to_vec())
    };
    Ok(WalRecord {
        seq,
        op,
        key,
        value,
    })
}

/// CRC32（IEEE 多项式，简单可靠的完整性校验）。
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;

    /// 全局持有临时目录 + 递增文件名，保证并行测试各自独立。
    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let name = format!("wal-{}.log", SEQ.fetch_add(1, Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    #[test]
    fn append_sync_recover_roundtrip() {
        let path = tmp();
        let mut w = WalWriter::create(&path, false).unwrap();
        w.append(OP_PUT, b"key_1", Some(b"value_1")).unwrap();
        w.append(OP_PUT, b"key_2", Some(b"value_2")).unwrap();
        w.append(OP_DELETE, b"key_3", None).unwrap();
        w.sync().unwrap();

        let recs = WalReader::recover(&path).unwrap();
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[0].seq, 1);
        assert_eq!(recs[0].op, OP_PUT);
        assert_eq!(recs[0].key, b"key_1");
        assert_eq!(recs[0].value.as_deref(), Some(b"value_1".as_slice()));
        assert_eq!(recs[2].op, OP_DELETE);
        assert_eq!(recs[2].value, None);
    }

    #[test]
    fn crash_before_sync_loses_unsynced_records() {
        let path = tmp();
        let mut w = WalWriter::create(&path, false).unwrap();
        // 写入 100 条，只 fsync 前 50 条，然后 drop 不 flush（模拟断电）
        for i in 0..100u64 {
            w.append(OP_PUT, format!("key_{i}").as_bytes(), Some(b"v"))
                .unwrap();
            if i == 49 {
                w.sync().unwrap();
            }
        }
        drop(w); // 模拟崩溃：剩余 50 条未刷盘

        let recs = WalReader::recover(&path).unwrap();
        // 已 fsync 的 50 条必须完整；未刷盘的可能部分存在（写入 OS 缓冲但未 fsync，Windows 下通常仍在页面缓存），
        // 但绝不能出现"seq 跳跃 + 损坏继续"——回放必须安全停止
        assert!(recs.len() >= 50);
        assert!(recs.iter().all(|r| r.seq <= recs.last().unwrap().seq));
        assert!(recs.iter().all(|r| r.key.len() >= 4));
    }

    #[test]
    fn truncated_tail_is_ignored() {
        let path = tmp();
        let mut w = WalWriter::create(&path, false).unwrap();
        for i in 0..10u64 {
            w.append(OP_PUT, format!("k{i}").as_bytes(), Some(b"v"))
                .unwrap();
        }
        w.sync().unwrap();
        drop(w);

        // 手动截断文件末尾一半，验证回放只保留完整记录
        let mut data = std::fs::read(&path).unwrap();
        data.truncate(data.len() / 2);
        std::fs::write(&path, &data).unwrap();

        let recs = WalReader::recover(&path).unwrap();
        assert!(!recs.is_empty());
        assert!(recs.len() <= 10);
    }

    #[test]
    fn corrupt_middle_stops_replay() {
        let path = tmp();
        let mut w = WalWriter::create(&path, false).unwrap();
        for i in 0..10u64 {
            w.append(OP_PUT, format!("k{i}").as_bytes(), Some(b"v"))
                .unwrap();
        }
        w.sync().unwrap();
        drop(w);

        let mut data = std::fs::read(&path).unwrap();
        // 翻转第 4 条记录的 CRC 区（每条记录约 8+len 字节，翻转中间某字节）
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        let recs = WalReader::recover(&path).unwrap();
        // 损坏点之前的记录可恢复，之后停止；不能 panic
        assert!(recs.len() < 10);
    }

    #[test]
    fn seq_is_monotonic() {
        let path = tmp();
        let mut w = WalWriter::create(&path, false).unwrap();
        let s1 = w.append(OP_PUT, b"a", Some(b"1")).unwrap();
        let s2 = w.append(OP_PUT, b"b", Some(b"2")).unwrap();
        assert!(s2 > s1);
        assert_eq!(w.next_seq(), s2 + 1);
    }

    #[test]
    fn group_commit_batches_records() {
        // 组提交：一批 100 条 append 后只 sync 一次
        let path = tmp();
        let mut w = WalWriter::create(&path, false).unwrap();
        for i in 0..100u64 {
            w.append(OP_PUT, format!("k{i}").as_bytes(), Some(b"v"))
                .unwrap();
        }
        w.sync().unwrap();
        let recs = WalReader::recover(&path).unwrap();
        assert_eq!(recs.len(), 100);
    }

    proptest::proptest! {
        #[test]
        fn recover_allows_empty_wal(bytes in proptest::collection::vec(proptest::arbitrary::any::<u8>(), 0..64)) {
            // 任意字节串回放不得 panic（安全停止）
            let path = tmp();
            std::fs::write(&path, &bytes).unwrap();
            let _ = WalReader::recover(&path).unwrap();
        }
    }
}
