use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::keys::encode_varlen;

use super::crc32;
use super::reader::decode_payload;
use super::writer::WalRecord;

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
    /// V 项：io_uring 后端池引用（Linux + `runtime.io_uring_enabled` 时 Some）——
    /// fsync 提交到 WAL 队列（SQPOLL 免 syscall）。非 Linux 编译为空字段。
    #[cfg(target_os = "linux")]
    iou: Option<std::sync::Arc<crate::io_queue::backend::IoUringPool>>,
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
            .truncate(false)
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
                #[cfg(target_os = "linux")]
                iou: None,
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
            #[cfg(target_os = "linux")]
            iou: None,
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

    /// 落盘：构建写计划（必要时回绕）→ 头部 tail 与记录区**同一次 fsync 原子提交**（M8-P12）→ 维护索引。
    /// 回绕覆盖未刷盘记录时返回 WalFull（上层 Flush + `set_flushed_seq` 后重试）。
    ///
    /// 崩溃安全（M8-P12 合并 fsync 后）：头部 tail 与记录区写入 page cache 后单次 `sync_all`
    /// 原子提交——tail 与记录**同时可见** → tail 永不指向未落盘记录（与两阶段版保证相同）；
    /// 崩溃于 fsync 前：头尾均未落盘 → 恢复上次提交状态（本次未提交记录忽略，安全）。
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
        self.tail = pos;
        write_ring_header(file, self.tail)?; // 写头部 tail（page cache）
        for (off, bytes) in &plan {
            file.seek(std::io::SeekFrom::Start(*off as u64))?;
            file.write_all(bytes)?;
        }
        // 单次 fsync：头部 tail + 记录区原子提交（消除冗余第二次 fsync）——
        // io_uring 启用时经 WAL 队列（SQPOLL），否则 sync_all。
        self.fsync_file()?;
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

    /// V 项：注入 io_uring 后端池（Linux + `runtime.io_uring_enabled`）——此后 fsync
    /// 提交到 WAL 队列（SQPOLL 免 syscall）。CF 打开 WAL 后调用；未启用保持 None。
    #[cfg(target_os = "linux")]
    pub fn set_io_uring(&mut self, iou: Option<std::sync::Arc<crate::io_queue::backend::IoUringPool>>) {
        self.iou = iou;
    }

    /// V 项：fsync 转发——io_uring 启用（`self.iou` 有值）时经 WAL 队列提交，
    /// 否则回退 `sync_all`。非 Linux 编译恒走同步路径。
    #[cfg(target_os = "linux")]
    fn fsync_file(&self) -> Result<()> {
        if let Some(f) = self.file.as_ref() {
            if let Some(iou) = &self.iou {
                iou.fsync(crate::io_queue::IoClass::Wal, f)
                    .map_err(crate::error::Error::Io)?;
            } else {
                f.sync_all().map_err(crate::error::Error::Io)?;
            }
        }
        Ok(())
    }

    /// 非 Linux：直接 `sync_all`（无 io_uring 路径）。
    #[cfg(not(target_os = "linux"))]
    fn fsync_file(&self) -> Result<()> {
        if let Some(f) = self.file.as_ref() {
            f.sync_all().map_err(crate::error::Error::Io)?;
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
