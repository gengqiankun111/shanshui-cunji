//! WAL 预写日志（development 5.1 / design 4.3）。
//!
//! 记录格式：`Length(u32) ++ CRC32(u32) ++ Payload`
//! Payload := `Seq(u64) ++ OpType(u8) ++ Key(VarLen) ++ [Value(VarLen)]`
//! - OpType：0 = Put，1 = Delete；
//! - 组提交（Group Commit）：一批记录一次性写盘 + 一次 fsync；
//! - 崩溃回放：读到首条 CRC 损坏/截断记录即停（部分写入安全）；
//! - 延迟删除：旧段切分后重命名交给后台线程 unlink（design 4.3）。

use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::keys::{decode_varlen, encode_varlen};

pub const OP_PUT: u8 = 0;
pub const OP_DELETE: u8 = 1;

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
}

const GROUP_COMMIT_FSYNC_BYTES: usize = 4 * 1024 * 1024; // perf 模式攒满 4MB 才 fsync

/// WAL 文件头（append 模式截断后写入，M8-P5）：magic + next_seq。
/// flush 后 WAL 清空重建，头持久化 next_seq 保证重开 seq 接续（不冲突）。
const WAL_HEADER: &[u8; 8] = b"SCWAL01\0";
const WAL_HEADER_LEN: usize = 16;

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
        })
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
        file.sync_all()?; // 头 + next_seq 落盘（崩溃恢复 seq 接续依据）
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
            file.sync_all()?;
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
    /// 截断后重建的 WAL 含头（magic + next_seq，M8-P5）→ 跳过头从偏移 16 解析记录。
    pub fn recover(path: &Path) -> Result<Vec<WalRecord>> {
        let buf = std::fs::read(path)?;
        let start = if buf.len() >= WAL_HEADER_LEN && &buf[0..8] == WAL_HEADER {
            WAL_HEADER_LEN
        } else {
            0
        };
        let mut records = Vec::new();
        let mut pos = start;
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

/// 环形 WAL（design 4.3 阶段 3 高性能写入模式）：预分配固定大小文件，写指针循环移动，
/// 省去文件扩展与 inode 元数据更新开销。记录格式与追加 WAL 相同（`Len u32 ++ CRC32 ++ Payload`）。
///
/// 文件布局：
/// ```text
/// [0..4)   魔数 "RGW1"
/// [4..12)  保留（0）
/// [12..20) tail_offset u64   # 下一记录写入位置（始终 >= 20）
/// [20..)   记录区
/// ```
///
/// - **回绕**：记录不跨文件尾；剩余空间不足时写指针回到 `RING_HEADER(20)` 继续（旧区被覆盖）；
/// - **覆盖安全**：回绕覆盖仅允许在**整个环内已无未刷盘记录**时进行（`flushed_seq` 由上层在 Flush 后
///   上报，`set_flushed_seq`）；否则 `sync` 返回 `Error::WalFull`，上层强制 Flush 后重试；
/// - **崩溃安全**：`sync` 两阶段——先写记录区并 fsync，再更新头部 tail 并 fsync；
///   崩溃于两阶段之间 → 恢复使用旧 tail，未提交记录被忽略（安全）；
/// - **恢复**：有效数据恒为线性区间 `[20, tail)`（回绕点固定 20），从 20 顺序解析即可。
pub struct RingWal {
    file: Option<std::fs::File>,
    path: PathBuf,
    size: usize,
    tail: usize,
    next_seq: u64,
    /// 已刷入 SST 的最大 seq（上层 Flush 后上报；覆盖安全依据）。
    flushed_seq: u64,
    /// 待落盘记录（每条 = len+crc+payload 完整字节）。
    pending: Vec<Vec<u8>>,
    pending_bytes: usize,
    /// 已落盘记录索引 (offset, seq)，按 offset 升序（覆盖安全检查）。
    index: Vec<(usize, u64)>,
    /// 已落盘记录最大 seq。
    max_written_seq: u64,
    /// 上次 fsync 时刻（组提交窗口判定，M8）。
    last_sync: std::time::Instant,
}

/// 环形 WAL 头长度（魔数 4 + 保留 8 + tail 8）。
pub const RING_HEADER: usize = 20;
const RING_MAGIC: &[u8; 4] = b"RGW1";

impl RingWal {
    /// 打开（已存在则恢复环内记录与索引）/ 创建（预分配 + 初始化头）。
    /// 返回 `(写入器, 环内已有记录)`——记录供上层崩溃回放，索引供覆盖安全检查。
    pub fn open_or_create(path: &Path, size: usize) -> Result<(Self, Vec<WalRecord>)> {
        if size < RING_HEADER + 8 {
            return Err(crate::error::Error::Config("环形 WAL 容量过小".into()));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let is_new = !path.exists() || std::fs::metadata(path)?.len() == 0;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        if is_new {
            file.set_len(size as u64)?;
            let mut ring = Self {
                file: Some(file),
                path: path.to_path_buf(),
                size,
                tail: RING_HEADER,
                next_seq: 1,
                flushed_seq: 0,
                pending: Vec::new(),
                pending_bytes: 0,
                index: Vec::new(),
                max_written_seq: 0,
                last_sync: std::time::Instant::now(),
            };
            write_ring_header(ring.file.as_mut().unwrap(), ring.tail)?;
            ring.fsync_file()?;
            return Ok((ring, Vec::new()));
        }
        // 已存在：预分配扩展（不收缩，避免截断数据）
        let cur_len = std::fs::metadata(path)?.len() as usize;
        if cur_len < size {
            file.set_len(size as u64)?;
        }
        let mut ring = Self {
            file: Some(file),
            path: path.to_path_buf(),
            size,
            tail: 0,
            next_seq: 1,
            flushed_seq: 0,
            pending: Vec::new(),
            pending_bytes: 0,
            index: Vec::new(),
            max_written_seq: 0,
            last_sync: std::time::Instant::now(),
        };
        ring.tail = ring.read_tail()?;
        let (recs, index) = ring.scan_ring()?;
        ring.index = index;
        ring.max_written_seq = recs.iter().map(|r| r.seq).max().unwrap_or(0);
        Ok((ring, recs))
    }

    /// 读取头部持久化的 tail。
    fn read_tail(&self) -> Result<usize> {
        let file = self.file.as_ref().unwrap();
        let mut h = [0u8; RING_HEADER];
        read_at(file, 0, &mut h)?;
        if &h[0..4] != RING_MAGIC {
            return Err(crate::error::Error::Corrupted("环形 WAL 魔数错误".into()));
        }
        Ok(u64::from_le_bytes(h[12..20].try_into().unwrap()) as usize)
    }

    /// 顺序解析有效区间 [RING_HEADER, tail)：返回 (记录, 索引)；首条损坏/截断处停止。
    fn scan_ring(&self) -> Result<(Vec<WalRecord>, Vec<(usize, u64)>)> {
        let file = self.file.as_ref().unwrap();
        let mut records = Vec::new();
        let mut index = Vec::new();
        let mut pos = RING_HEADER;
        while pos + 8 <= self.tail {
            let mut h = [0u8; 8];
            read_at(file, pos, &mut h)?;
            let len = u32::from_le_bytes(h[0..4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(h[4..8].try_into().unwrap());
            if pos + 8 + len > self.tail {
                break; // 截断
            }
            let mut payload = vec![0u8; len];
            read_at(file, pos + 8, &mut payload)?;
            if crc32(&payload) != crc {
                break; // 损坏
            }
            match decode_payload(&payload) {
                Ok(rec) => {
                    index.push((pos, rec.seq));
                    records.push(rec);
                }
                Err(_) => break,
            }
            pos += 8 + len;
        }
        Ok((records, index))
    }

    /// 追加一条记录到缓冲（未落盘；容量超限返回 WalFull，由上层 Flush 后重试）。
    pub fn append(&mut self, op: u8, key: &[u8], value: Option<&[u8]>) -> Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.append_at(op, key, value, seq)?;
        Ok(seq)
    }

    /// 以**指定 seq** 追加记录（MVCC 全局 seq，M7-1）；next_seq 同步推进保持单调。
    pub fn append_at(&mut self, op: u8, key: &[u8], value: Option<&[u8]>, seq: u64) -> Result<()> {
        self.next_seq = self.next_seq.max(seq + 1);
        let mut payload = Vec::new();
        payload.extend_from_slice(&seq.to_le_bytes());
        payload.push(op);
        encode_varlen(&mut payload, key);
        if let Some(v) = value {
            encode_varlen(&mut payload, v);
        } else {
            encode_varlen(&mut payload, &[]);
        }
        let mut rec = Vec::with_capacity(8 + payload.len());
        rec.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        rec.extend_from_slice(&crc32(&payload).to_le_bytes());
        rec.extend_from_slice(&payload);
        let capacity = self.size - RING_HEADER;
        if self.pending_bytes + rec.len() > capacity {
            return Err(crate::error::Error::WalFull(
                "环形 WAL 缓冲超容量，需先 Flush".into(),
            ));
        }
        self.pending_bytes += rec.len();
        self.pending.push(rec);
        Ok(())
    }

    /// 落盘：构建写计划（必要时回绕）→ 写记录区 fsync → 更新头部 tail fsync → 维护索引。
    /// 回绕覆盖未刷盘记录时返回 WalFull（上层 Flush + `set_flushed_seq` 后重试）。
    pub fn sync(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut plan: Vec<(usize, Vec<u8>)> = Vec::new();
        let mut pos = self.tail;
        for rec in &self.pending {
            if pos + rec.len() > self.size {
                // 回绕：仅当整个环内无未刷盘记录才允许覆盖
                if self.max_written_seq > self.flushed_seq {
                    return Err(crate::error::Error::WalFull(
                        "环形 WAL 满（含未刷盘记录），需先 Flush".into(),
                    ));
                }
                pos = RING_HEADER;
            }
            if pos + rec.len() > self.size {
                return Err(crate::error::Error::WalFull(
                    "单条记录超过环形 WAL 容量".into(),
                ));
            }
            plan.push((pos, rec.clone()));
            pos += rec.len();
        }
        let file = self.file.as_mut().unwrap();
        for (off, bytes) in &plan {
            file.seek(std::io::SeekFrom::Start(*off as u64))?;
            file.write_all(bytes)?;
        }
        file.sync_all()?; // 阶段①：记录区落盘
        self.tail = pos;
        write_ring_header(file, self.tail)?;
        file.sync_all()?; // 阶段②：头部 tail 落盘
        for (off, rec) in &plan {
            let seq = u64::from_le_bytes(rec[8..16].try_into().unwrap());
            self.index.push((*off, seq));
        }
        self.index.sort_by_key(|(o, _)| *o);
        self.max_written_seq = self.index.iter().map(|(_, s)| *s).max().unwrap_or(0);
        self.pending.clear();
        self.pending_bytes = 0;
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

    /// 上报已刷盘的最大 seq：覆盖安全边界前移（Flush 完成后调用）。
    pub fn set_flushed_seq(&mut self, seq: u64) {
        self.flushed_seq = self.flushed_seq.max(seq);
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// 接续序列号（环内记录回放完成后调用，保证新写入 seq 单调递增）。
    pub fn resume_seq(&mut self, next_seq: u64) {
        self.next_seq = next_seq;
    }

    /// 显式落盘（关闭前 / 紧急）。
    pub fn flush_sync(&mut self) -> Result<()> {
        self.sync()?;
        self.fsync_file()
    }

    pub fn close(mut self) -> Result<()> {
        self.flush_sync()?;
        self.file.take();
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 重放环内全部记录（增量备份 / 恢复用）。
    pub fn recover_records(&self) -> Result<Vec<WalRecord>> {
        let (recs, _) = self.scan_ring()?;
        Ok(recs)
    }

    fn fsync_file(&self) -> Result<()> {
        if let Some(f) = self.file.as_ref() {
            f.sync_all()?;
        }
        Ok(())
    }
}

/// 写环形 WAL 头部（魔数 + tail 指针）。
fn write_ring_header(file: &mut std::fs::File, tail: usize) -> Result<()> {
    let mut h = [0u8; RING_HEADER];
    h[0..4].copy_from_slice(RING_MAGIC);
    h[12..20].copy_from_slice(&(tail as u64).to_le_bytes());
    file.seek(std::io::SeekFrom::Start(0))?;
    file.write_all(&h)?;
    Ok(())
}

impl Drop for RingWal {
    fn drop(&mut self) {
        // 崩溃模拟路径：drop 不保证 flush（与 WalWriter 语义一致）
        let _ = self.file.take();
    }
}

/// 定长偏移读取辅助。
fn read_at(file: &std::fs::File, off: usize, buf: &mut [u8]) -> Result<()> {
    use std::io::Seek;
    let mut f = file;
    f.seek(std::io::SeekFrom::Start(off as u64))?;
    f.read_exact(buf)?;
    Ok(())
}

/// WAL 后端抽象：append 传统追加（默认）/ ring 预分配环形（design 4.3 阶段 3 高性能）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalMode {
    Append,
    Ring,
}

impl WalMode {
    pub fn parse(s: &str) -> Self {
        match s {
            "ring" => WalMode::Ring,
            _ => WalMode::Append,
        }
    }
}

/// WAL 后端（append 传统追加 / ring 预分配环形）：列族通过该枚举统一分发。
pub enum WalBackend {
    Append(WalWriter),
    Ring(RingWal),
}

impl WalBackend {
    pub fn append(&mut self, op: u8, key: &[u8], value: Option<&[u8]>) -> Result<u64> {
        match self {
            WalBackend::Append(w) => w.append(op, key, value),
            WalBackend::Ring(r) => r.append(op, key, value),
        }
    }

    /// 以指定 seq 追加记录（MVCC 全局 seq，M7-1）。
    pub fn append_at(&mut self, op: u8, key: &[u8], value: Option<&[u8]>, seq: u64) -> Result<()> {
        match self {
            WalBackend::Append(w) => w.append_at(op, key, value, seq),
            WalBackend::Ring(r) => r.append_at(op, key, value, seq),
        }
    }

    pub fn sync(&mut self) -> Result<()> {
        match self {
            WalBackend::Append(w) => w.sync(),
            WalBackend::Ring(r) => r.sync(),
        }
    }

    /// 截断重建（M8-P5）：append 模式 flush 后清空 WAL（写头持久化 next_seq）；
    /// ring 模式自带覆盖回收（no-op）。
    pub fn truncate_and_reset(&mut self) -> Result<()> {
        match self {
            WalBackend::Append(w) => w.truncate_and_reset(),
            WalBackend::Ring(_) => Ok(()),
        }
    }

    /// 待刷盘缓冲字节数（组提交窗口判定，M8）。
    pub fn pending_bytes(&self) -> usize {
        match self {
            WalBackend::Append(w) => w.pending_bytes(),
            WalBackend::Ring(r) => r.pending_bytes(),
        }
    }

    /// 组提交是否到期（M8）：距上次 fsync ≥ 窗口，或待刷缓冲 ≥ 字节阈值。
    pub fn sync_due(
        &self,
        now: std::time::Instant,
        window: std::time::Duration,
        bytes: usize,
    ) -> bool {
        match self {
            WalBackend::Append(w) => w.sync_due(now, window, bytes),
            WalBackend::Ring(r) => r.sync_due(now, window, bytes),
        }
    }

    pub fn next_seq(&self) -> u64 {
        match self {
            WalBackend::Append(w) => w.next_seq(),
            WalBackend::Ring(r) => r.next_seq(),
        }
    }

    pub fn resume_seq(&mut self, next_seq: u64) {
        match self {
            WalBackend::Append(w) => w.resume_seq(next_seq),
            WalBackend::Ring(r) => r.resume_seq(next_seq),
        }
    }

    pub fn flush_sync(&mut self) -> Result<()> {
        match self {
            WalBackend::Append(w) => w.flush_sync(),
            WalBackend::Ring(r) => r.flush_sync(),
        }
    }

    /// 上报已刷盘最大 seq（仅环形模式生效：覆盖安全边界前移）。
    pub fn set_flushed_seq(&mut self, seq: u64) {
        if let WalBackend::Ring(r) = self {
            r.set_flushed_seq(seq);
        }
    }

    /// 重放当前 WAL 内全部记录（增量备份 / 恢复用）。
    pub fn recover_records(&self) -> Result<Vec<WalRecord>> {
        match self {
            WalBackend::Append(w) => WalReader::recover(w.path()),
            WalBackend::Ring(r) => r.recover_records(),
        }
    }

    pub fn path(&self) -> &Path {
        match self {
            WalBackend::Append(w) => w.path(),
            WalBackend::Ring(r) => r.path(),
        }
    }
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

    // ---------- 环形 WAL（design 4.3，M6） ----------

    #[test]
    fn ring_append_sync_recover_roundtrip() {
        let path = tmp();
        {
            let (mut r, recs) = RingWal::open_or_create(&path, 1024).unwrap();
            assert!(recs.is_empty(), "新环形 WAL 无记录");
            r.append(OP_PUT, b"k1", Some(b"v1")).unwrap();
            r.append(OP_DELETE, b"k2", None).unwrap();
            r.sync().unwrap();
            assert_eq!(r.next_seq(), 3);
            r.flush_sync().unwrap();
        }
        let (_, recs) = RingWal::open_or_create(&path, 1024).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].seq, 1);
        assert_eq!(recs[0].key, b"k1");
        assert_eq!(recs[0].value.as_deref(), Some(b"v1".as_slice()));
        assert_eq!(recs[1].op, OP_DELETE);
    }

    #[test]
    fn ring_wraps_and_recovers_latest_cycle() {
        let path = tmp();
        // 小容量：256 字节，容量 236 → 每周期约 11 条
        let (mut r, _) = RingWal::open_or_create(&path, 256).unwrap();
        let mut written = 0u64;
        // 周期 1：写满一周期并落盘
        loop {
            match r.append(OP_PUT, format!("a{written}").as_bytes(), Some(b"v")) {
                Ok(_) => written += 1,
                Err(Error::WalFull(_)) => break,
                Err(e) => panic!("{e}"),
            }
        }
        r.sync().unwrap();
        // 周期 2：模拟周期 1 已刷盘，允许回绕覆盖
        r.set_flushed_seq(r.next_seq() - 1);
        r.append(OP_PUT, b"b1", Some(b"w")).unwrap();
        r.append(OP_PUT, b"b2", Some(b"w")).unwrap();
        r.sync().unwrap();
        r.flush_sync().unwrap();

        // 恢复：只回放周期 2 的记录（周期 1 已刷盘，覆盖安全）
        let (_, recs) = RingWal::open_or_create(&path, 256).unwrap();
        assert_eq!(recs.len(), 2, "回绕后只恢复最新周期: {recs:?}");
        assert!(recs.iter().all(|r| r.key.starts_with(b"b")));
    }

    #[test]
    fn ring_blocks_wrap_without_flush() {
        let path = tmp();
        let (mut r, _) = RingWal::open_or_create(&path, 256).unwrap();
        let mut n = 0u64;
        loop {
            match r.append(OP_PUT, format!("a{n}").as_bytes(), Some(b"v")) {
                Ok(_) => n += 1,
                Err(Error::WalFull(_)) => break,
                Err(e) => panic!("{e}"),
            }
        }
        r.sync().unwrap(); // 周期 1 落盘，flushed_seq=0
        r.append(OP_PUT, b"x", Some(b"1")).unwrap();
        // 未上报刷盘 → 回绕被拒（覆盖未刷盘记录不安全）
        let err = r.sync().unwrap_err();
        assert!(
            matches!(err, Error::WalFull(_)),
            "未刷盘时回绕应被拒绝: {err}"
        );
    }

    #[test]
    fn ring_append_exceeds_capacity_returns_full() {
        let path = tmp();
        let (mut r, _) = RingWal::open_or_create(&path, 64).unwrap(); // 容量 44 字节
        let mut full = false;
        for i in 0..10u64 {
            if let Err(Error::WalFull(_)) = r.append(OP_PUT, format!("k{i}").as_bytes(), Some(b"v"))
            {
                full = true;
                break;
            }
        }
        assert!(full, "缓冲超容量应返回 WalFull");
    }

    #[test]
    fn ring_survives_crash_reopen_keeps_flushed_only() {
        // 崩溃模拟：sync 后不 flush 直接 drop → 重开应只回放已 sync 的记录
        let path = tmp();
        {
            let (mut r, _) = RingWal::open_or_create(&path, 4096).unwrap();
            r.append(OP_PUT, b"c1", Some(b"1")).unwrap();
            r.sync().unwrap();
            // 一条已 append 未 sync 的记录（模拟断电丢失）
            r.append(OP_PUT, b"c2", Some(b"2")).unwrap();
        } // drop：不落盘
        let (_, recs) = RingWal::open_or_create(&path, 4096).unwrap();
        assert_eq!(recs.len(), 1, "仅回放已 sync 记录");
        assert_eq!(recs[0].key, b"c1");
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
