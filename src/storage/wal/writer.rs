use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::keys::encode_varlen;

use super::{WAL_HEADER, WAL_HEADER_LEN, crc32};

/// WAL 记录负载。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// 上次 fsync 时刻（组提交窗口判定，M8）。
    last_sync: std::time::Instant,
    /// V 项：io_uring 后端池引用（Linux + `runtime.io_uring_enabled` 时 Some）——
    /// fsync 提交到 WAL 队列（SQPOLL 免 syscall）。非 Linux 编译为空字段。
    #[cfg(target_os = "linux")]
    iou: Option<std::sync::Arc<crate::io_queue::backend::IoUringPool>>,
}

const GROUP_COMMIT_FSYNC_BYTES: usize = 4 * 1024 * 1024; // perf 模式攒满 4MB 才 fsync

impl WalWriter {
    /// 创建（截断已存在文件）。
    pub fn create(path: &Path, perf_mode: bool) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // read+write（非 append）：truncate_and_reset 需要 set_len/seek 权限（M8-P5）
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)?;
        Ok(Self {
            file: Some(file),
            path: path.to_path_buf(),
            next_seq: 1,
            buf: Vec::new(),
            perf_mode,
            pending_bytes: 0,
            last_sync: std::time::Instant::now(),
            #[cfg(target_os = "linux")]
            iou: None,
        })
    }

    /// 以追加模式打开 WAL（**不截断**），用于重启恢复：回放旧记录后继续写入。
    /// `next_seq` 为接续序列号（= 已回放最大 seq + 1），避免同 key 新版本 seq 冲突。
    /// 若文件含 WAL 头（截断后重建，M8-P5）则 next_seq 从头读取（优先级更高）。
    pub fn open_append(path: &Path, next_seq: u64, perf_mode: bool) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        // 读文件头：截断后的 WAL 持久化了 next_seq（重开接续，避免 seq 冲突）
        let mut head = [0u8; WAL_HEADER_LEN];
        let mut resolved = next_seq;
        let mut f = &file;
        if f.read_exact(&mut head).is_ok() && &head[0..8] == WAL_HEADER {
            resolved = u64::from_le_bytes(head[8..16].try_into().unwrap());
        }
        Ok(Self {
            file: Some(file),
            path: path.to_path_buf(),
            next_seq: resolved,
            buf: Vec::new(),
            perf_mode,
            pending_bytes: 0,
            last_sync: std::time::Instant::now(),
            #[cfg(target_os = "linux")]
            iou: None,
        })
    }

    /// V 项：注入 io_uring 后端池（Linux + `runtime.io_uring_enabled`）——此后 fsync
    /// 提交到 WAL 队列（SQPOLL 免 syscall）。CF 打开 WAL 后调用；未启用保持 None。
    #[cfg(target_os = "linux")]
    pub fn set_io_uring(&mut self, iou: Option<std::sync::Arc<crate::io_queue::backend::IoUringPool>>) {
        self.iou = iou;
    }

    /// V 项：fsync 转发——io_uring 启用（`self.iou` 有值）时经 WAL 队列提交，
    /// 否则回退 `sync_all`。非 Linux 编译恒走同步路径。
    #[cfg(target_os = "linux")]
    fn fsync_file(&mut self) -> Result<()> {
        let iou = self.iou.clone();
        if let Some(f) = self.file.as_ref() {
            if let Some(iou) = &iou {
                iou.fsync(crate::io_queue::IoClass::Wal, f).map_err(crate::error::Error::Io)?;
            } else {
                f.sync_all().map_err(crate::error::Error::Io)?;
            }
        }
        Ok(())
    }

    /// 非 Linux：直接 `sync_all`（无 io_uring 路径）。
    #[cfg(not(target_os = "linux"))]
    fn fsync_file(&mut self) -> Result<()> {
        if let Some(f) = self.file.as_ref() {
            f.sync_all().map_err(crate::error::Error::Io)?;
        }
        Ok(())
    }

    /// 截断重建 WAL（M8-P5）：flush 后所有记录已刷盘，清空文件并写头（magic + next_seq），
    /// 保持 WAL 小文件（避免无限增长 + 大文件 fsync 拖慢写入）。next_seq 在内存保留递增。
    pub fn truncate_and_reset(&mut self) -> Result<()> {
        let next = self.next_seq;
        let file = self.file.as_mut().unwrap();
        file.set_len(0)?;
        file.seek(std::io::SeekFrom::Start(0))?;
        file.write_all(WAL_HEADER)?;
        file.write_all(&next.to_le_bytes())?;
        self.fsync_file()?; // 头 + next_seq 落盘（崩溃恢复 seq 接续依据）
        self.buf.clear();
        self.pending_bytes = 0;
        self.last_sync = std::time::Instant::now();
        Ok(())
    }

    /// 接续序列号（WAL 回放完成后调用，保证新写入 seq 单调递增且不冲突）。
    pub fn resume_seq(&mut self, next_seq: u64) {
        self.next_seq = next_seq;
    }

    /// 追加一条记录并返回分配的 Seq（尚未落盘，由 sync / group_commit 统一提交）。
    pub fn append(&mut self, op: u8, key: &[u8], value: Option<&[u8]>) -> Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.append_at(op, key, value, seq)?;
        Ok(seq)
    }

    /// 以**指定 seq** 追加记录（MVCC 全局 seq，engine 层统一分配，M7-1）。
    /// 内部 next_seq 同步推进到 seq+1（保证崩溃恢复接续单调）。
    pub fn append_at(&mut self, op: u8, key: &[u8], value: Option<&[u8]>, seq: u64) -> Result<()> {
        self.next_seq = self.next_seq.max(seq + 1);

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
        Ok(())
    }

    /// 组提交：将缓冲整批写盘并 fsync（标准模式每次提交都 fsync）。
    pub fn sync(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let file = self.file.as_mut().unwrap();
        // 非 append 模式（read+write，M8-P5）：写入前 seek 到文件尾（头/上次记录之后）
        file.seek(std::io::SeekFrom::End(0))?;
        file.write_all(&self.buf)?;
        self.buf.clear();
        self.pending_bytes = 0;
        if !self.perf_mode {
            self.fsync_file()?;
        }
        self.last_sync = std::time::Instant::now();
        Ok(())
    }

    /// 待刷盘缓冲字节数（组提交窗口判定，M8）。
    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// 组提交是否到期（M8）：距上次 fsync ≥ 窗口，或待刷缓冲 ≥ 字节阈值。
    pub fn sync_due(
        &self,
        now: std::time::Instant,
        window: std::time::Duration,
        bytes: usize,
    ) -> bool {
        now.duration_since(self.last_sync) >= window || self.pending_bytes >= bytes
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
        self.fsync_file()?;
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
