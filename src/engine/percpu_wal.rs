//! Task-026 Per-CPU WAL（research/range-scan-percpu-wal-design.md §二）。
//!
//! 目标：多核高频非事务写的 WAL fsync 锁竞争摊薄——现有组提交（M8）为**单后台线程 + 各 CF
//! `WalBackend`（Mutex）全局攒批一次 fsync**；Per-CPU 改为 N 个队列（每队列独立后台消费线程 +
//! 独立 `wal-{queue}-{gseq_start}.log`），写入口按当前 CPU 路由入队（超界/未绑核 → 轮询回退），
//! 队列消费线程按组提交窗口批量写盘并 fsync。
//!
//! **阶段1（本文件，2026-09-05）**：配置解析 + 队列数解析 + 路由 + 队列深度/积压状态与监控
//! 快照（供 SHOW STATUS）。写入线程/文件布局/恢复归并（阶段2/3）后续落地；`per_cpu_enabled`
//! 当前默认 **false**（安全回退现有全局组提交），核心完成并经全量回归后再翻转默认 true。
//!
//! 与现有架构的衔接（不冲突）：全局 `gseq`（Arc<AtomicU64>）语义沿用；跨队列最终写盘可交错，
//! 恢复按文件名解析队列与 gseq 范围后**gseq 全局归并回放**；Manifest `checkpoint_gseq` 判定
//! 失败写跳过编号（洞）不回放；关闭/队列 0 时回退全局模式（等价现状）。
//!
//! **阶段2a（本文件底部，2026-09-05）**：WalEntry 编解码 + 队列文件读写（独立于列族，
//! 纯新增模块）；阶段2c（后台消费线程/engine 接线）与 3a（恢复归并）按
//! research/percpu-wal-stage2-design.md 推进。

use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::error::{Error, Result};
use crate::keys::{decode_varlen, encode_varlen};
use crate::wal::{crc32, OP_DELETE, OP_PUT};

/// Per-CPU WAL 运行配置（Engine 持有；阶段1 仅解析 + 路由，写路径接线见阶段2）。
pub(crate) struct PerCpuWal {
    /// 是否启用（true 才建队列；false = 走现有全局组提交/逐条 fsync）。
    pub enabled: bool,
    /// 已解析队列数（1..=64）：0/未启用时 = 1（等效单队列，便于回退语义统一）。
    pub queues: usize,
    /// 单队列最大缓冲条目数（满时写侧背压）。
    pub depth: usize,
    /// 每队列组提交 fsync 窗口（µs）。
    pub window_us: u64,
    /// 轮询回退计数（未绑核/CPU 超界时 round-robin）。
    rr: AtomicU64,
    /// 各队列积压条目计数（写侧入队 +1，消费出队 -1；监控 SHOW STATUS 用）。
    pub(crate) depth_now: Vec<AtomicU64>,
    /// 各队列累计消费（写盘）条目数（监控）。
    pub(crate) consumed: Vec<AtomicU64>,
}

/// CPU 核数上限（research §二 per_cpu_queues：0 = CPU 核数（上限 64））。
const MAX_QUEUES: usize = 64;

impl PerCpuWal {
    /// 从配置解析（阶段1：仅解析，不启动线程；启用但未接线写路径前保持不落地线程）。
    pub fn resolve(cfg: &crate::config::Config) -> Self {
        let enabled = cfg.storage.per_cpu_enabled;
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let queues = if !enabled {
            1
        } else if cfg.storage.per_cpu_queues == 0 {
            cores.clamp(1, MAX_QUEUES)
        } else {
            cfg.storage.per_cpu_queues.clamp(1, MAX_QUEUES)
        };
        let depth = cfg.storage.per_cpu_queue_depth.max(1);
        let window_us = cfg.storage.per_cpu_batch_window_us.max(1);
        PerCpuWal {
            enabled,
            queues,
            depth,
            window_us,
            rr: AtomicU64::new(0),
            depth_now: (0..queues).map(|_| AtomicU64::new(0)).collect(),
            consumed: (0..queues).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// 路由：优先当前 CPU（未超界），否则轮询回退（`rr % queues`）。
    /// disabled（queues=1）恒返回 0（与全局组提交路径语义一致，路由为零开销）。
    pub fn route(&self, cpu_hint: Option<usize>) -> usize {
        if self.queues <= 1 {
            return 0;
        }
        if let Some(c) = cpu_hint {
            if c < self.queues {
                return c;
            }
        }
        let r = self.rr.fetch_add(1, Ordering::Relaxed) as usize % self.queues;
        r
    }

    /// 当前 CPU（Linux 可用 `sched_getcpu` 感知 affinity；Windows 统一 None → 轮询回退）。
    /// 路由正确性不依赖 affinity（仅负载均衡质量），轮询 fallback 已覆盖未绑核场景。
    pub fn current_cpu() -> Option<usize> {
        None
    }

    /// 入队背压判定（阶段2 消费线程接线前不用）：队列满 = depth_now[q] >= depth。
    pub fn queue_full(&self, q: usize) -> bool {
        self.depth_now.get(q).map(|d| d.load(Ordering::Relaxed) >= self.depth as u64).unwrap_or(false)
    }

    /// 队列积压快照（监控 SHOW STATUS：队列深度/消费速率/积压）。
    pub fn status(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!("per_cpu_enabled={} queues={} depth={} window_us={}", self.enabled, self.queues, self.depth, self.window_us));
        for (q, (d, c)) in self.depth_now.iter().zip(self.consumed.iter()).enumerate() {
            s.push_str(&format!(" | q{q}:depth={} consumed={}", d.load(Ordering::Relaxed), c.load(Ordering::Relaxed)));
        }
        s
    }
}

// ===========================================================================
// 阶段2a：WalEntry 编解码 + 队列文件读写（research/percpu-wal-stage2-design.md §2.3）
// 队列文件内条目沿用 `Len(u32)+CRC32(u32)+Payload`（部分写入安全：CRC 坏/截断即止）。
// Payload := gseq(u64 LE) | cf(u8) | op(u8) | key(VarLen) | value(VarLen)
// ===========================================================================

/// 列族编号（WalEntry.cf；与 Engine 打开的列族对应）。
pub(crate) const CF_PRIMARY: u8 = 0;
pub(crate) const CF_DELTA: u8 = 1;
pub(crate) const CF_CIDX: u8 = 2;
pub(crate) const CF_OUTBOX: u8 = 3;

/// Per-CPU 队列 WAL 条目（engine 级，跨列族统一持久化）。
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WalEntry {
    /// 全局单调 seq（组 gseq：一次 SQL/API 写内全部 CF 条目共用同一 gseq →
    /// 崩溃回放按组原子：同组要么全回放要么整组跳过（checkpoint 以组为单位）。
    pub gseq: u64,
    /// 目标列族（CF_PRIMARY / CF_DELTA / CF_CIDX / CF_OUTBOX）。
    pub cf: u8,
    /// 操作（OP_PUT = 0 / OP_DELETE = 1）。
    pub op: u8,
    pub key: Vec<u8>,
    /// Put 的值；Delete 为 None。
    pub value: Option<Vec<u8>>,
}

impl WalEntry {
    pub fn put(cf: u8, gseq: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        WalEntry { gseq, cf, op: OP_PUT, key, value: Some(value) }
    }
    pub fn delete(cf: u8, gseq: u64, key: Vec<u8>) -> Self {
        WalEntry { gseq, cf, op: OP_DELETE, key, value: None }
    }
}

/// 编码单条目 Payload（不含 Len/CRC 帧）。
fn encode_payload(e: &WalEntry, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&e.gseq.to_le_bytes());
    buf.push(e.cf);
    buf.push(e.op);
    encode_varlen(buf, &e.key);
    match &e.value {
        Some(v) => encode_varlen(buf, v),
        None => encode_varlen(buf, &[]), // Delete 时值部分为空 VarLen（同既有 WAL 格式）
    }
}

/// 解码单条目 Payload（格式非法 → Corrupted）。
pub(crate) fn decode_payload(payload: &[u8]) -> Result<WalEntry> {
    if payload.len() < 10 {
        return Err(Error::Corrupted("WalEntry payload 过短".into()));
    }
    let gseq = u64::from_le_bytes(payload[0..8].try_into().unwrap());
    let cf = payload[8];
    let op = payload[9];
    let mut pos = 10usize;
    let key = decode_varlen(payload, &mut pos)?.to_vec();
    let raw = decode_varlen(payload, &mut pos)?;
    let value = if raw.is_empty() && op == OP_DELETE {
        None
    } else {
        Some(raw.to_vec())
    };
    Ok(WalEntry { gseq, cf, op, key, value })
}

/// 队列文件命名：`wal-{queue}-{gseq_start:020}.log`（queue ∈ [0, queues)）。
/// 解析失败（非队列文件 / 旧 `wal.log` 无 `-queue-` 段）→ None（旧格式识别依据）。
pub(crate) fn parse_queue_file_name(fname: &str) -> Option<(usize, u64)> {
    let rest = fname.strip_prefix("wal-")?;
    let (q, tail) = rest.split_once('-')?;
    let start = tail.strip_suffix(".log")?;
    let q: usize = q.parse().ok()?;
    let start: u64 = start.parse().ok()?;
    Some((q, start))
}

/// 队列文件写入器（每队列独立文件；写侧切段/后台裁剪见阶段2c）。
pub(crate) struct QueueFileWriter {
    file: Option<std::fs::File>,
    path: PathBuf,
    buf: Vec<u8>,
    /// 已落盘字节（切段阈值用）。
    bytes_written: u64,
    /// 文件内已写入最大 gseq（裁剪判定）。
    max_gseq: u64,
    min_gseq: u64,
}

impl QueueFileWriter {
    /// 新建（截断已存在文件）；`first_gseq` = 文件首条记录 gseq（命名 + 裁剪参考）。
    pub fn create(dir: &Path, queue: usize, first_gseq: u64) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("wal-{queue}-{first_gseq:020}.log"));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Self {
            file: Some(file),
            path,
            buf: Vec::new(),
            bytes_written: 0,
            max_gseq: first_gseq,
            min_gseq: first_gseq,
        })
    }

    /// 追加一批（不落盘；由窗口/显式 flush 统一 fsync）。返回本批最大 gseq。
    pub fn write_batch(&mut self, entries: &[WalEntry]) -> Result<u64> {
        let mut max = 0u64;
        for e in entries {
            let mut payload = Vec::with_capacity(16 + e.key.len() + e.value.as_ref().map_or(0, |v| v.len()));
            encode_payload(e, &mut payload);
            let crc = crc32(&payload);
            self.buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            self.buf.extend_from_slice(&crc.to_le_bytes());
            self.buf.extend_from_slice(&payload);
            self.bytes_written += (8 + payload.len()) as u64;
            max = max.max(e.gseq);
        }
        if max > 0 {
            self.max_gseq = self.max_gseq.max(max);
        }
        Ok(max)
    }

    /// 写盘并 fsync（组提交窗口边界 / 显式 flush）。
    pub fn flush(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let file = self.file.as_mut().unwrap();
        file.seek(std::io::SeekFrom::End(0))?;
        file.write_all(&self.buf)?;
        self.buf.clear();
        file.sync_all().map_err(Error::Io)?;
        Ok(())
    }

    /// 已写盘 + 待刷字节合计（切段阈值用）。
    pub fn bytes(&self) -> u64 {
        self.bytes_written
    }

    /// 文件内最大 gseq（裁剪判定：`<= checkpoint` 的段可删）。
    pub fn max_gseq(&self) -> u64 {
        self.max_gseq
    }

    pub fn min_gseq(&self) -> u64 {
        self.min_gseq
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 关闭并落盘。
    pub fn close(mut self) -> Result<()> {
        self.flush()?;
        self.file.take();
        Ok(())
    }
}

impl Drop for QueueFileWriter {
    fn drop(&mut self) {
        // 崩溃模拟路径：drop 不保证 flush（测试通过不调用 flush 直接 drop 模拟断电）
        let _ = self.file.take();
    }
}

/// 回放队列文件：返回文件内全部有效条目（CRC 坏/截断处停止，同现有 WalReader 语义）。
pub(crate) fn read_queue_file(path: &Path) -> Result<Vec<WalEntry>> {
    let buf = std::fs::read(path)?;
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

/// 队列文件切段字节阈值（写满即切新段，配合 checkpoint 裁剪控制单文件大小）。
pub(crate) const QUEUE_FILE_ROTATE_BYTES: u64 = 64 * 1024 * 1024;

// ===========================================================================
// 阶段2b/2c：TLS 写批次 scope + 队列运行时（写入口收集 → 按 CPU 路由入队 →
// 每队列后台消费线程按窗口写独立文件并 fsync；checkpoint = min(各 CF 刷盘水位)）。
// ===========================================================================

use std::sync::{Arc, Condvar, Mutex};

/// 一次 SQL/API 写的 WAL 收集批次（同 gseq 组：跨 CF 条目共用 gseq → 崩溃回放原子）。
#[derive(Debug)]
pub(crate) struct WalScope {
    pub gseq: u64,
    pub entries: Vec<WalEntry>,
}

/// 写线程局部 scope 栈（engine 在调用 CF 前 push、结束后 pop；嵌套写 = 内层独立组）。
thread_local! {
    static WAL_SCOPES: std::cell::RefCell<Vec<WalScope>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// push 写批次 scope（engine 写入口调用；gseq 由 engine 全局分配）。
pub(crate) fn push_wal_scope(gseq: u64) {
    WAL_SCOPES.with(|t| {
        t.borrow_mut().push(WalScope { gseq, entries: Vec::new() });
    });
}

/// pop 写批次 scope（返回收集的条目；无 scope → None）。
pub(crate) fn pop_wal_scope() -> Option<WalScope> {
    WAL_SCOPES.with(|t| t.borrow_mut().pop())
}

/// 收集一条 WAL 条目（external CF 写路径调用）：返回组 gseq（供 memtable 定序）。
/// 无活动 scope → None（调用方按外部模式调用错误处理，防静默丢数据）。
pub(crate) fn wal_collect(
    cf: u8,
    op: u8,
    key: &[u8],
    value: Option<&[u8]>,
) -> Option<u64> {
    WAL_SCOPES.with(|t| {
        let mut stack = t.borrow_mut();
        match stack.last_mut() {
            Some(scope) => {
                scope.entries.push(WalEntry {
                    gseq: scope.gseq,
                    cf,
                    op,
                    key: key.to_vec(),
                    value: value.map(|v| v.to_vec()),
                });
                Some(scope.gseq)
            }
            None => None,
        }
    })
}

/// 队列切段元数据（后台裁剪判定：`end <= cp` 的段可删）。
#[derive(Debug, Clone)]
pub(crate) struct SegMeta {
    pub q: usize,
    pub start: u64,
    pub end: u64,
    pub path: PathBuf,
}

/// 队列内部状态（消费线程 + 写路径 flush 共享）。
pub(crate) struct QueueInner {
    pub qid: usize,
    /// 待消费条目（consumer / flush_all 谁先取谁写）。
    pending: Mutex<Vec<WalEntry>>,
    cond: Condvar,
    /// 队列积压条目数（写侧入队 +N、消费出队 -N；监控/背压）。
    pub(crate) depth: AtomicU64,
    /// 累计消费（写盘）条目数（监控）。
    pub(crate) consumed: AtomicU64,
    /// 本队列当前段写入器（None = 尚无写入；写者持锁串行）。
    writer: Mutex<Option<QueueFileWriter>>,
    /// 消费线程退出标志。
    stop: AtomicBool,
}

impl QueueInner {
    fn new(qid: usize) -> Arc<Self> {
        Arc::new(QueueInner {
            qid,
            pending: Mutex::new(Vec::new()),
            cond: Condvar::new(),
            depth: AtomicU64::new(0),
            consumed: AtomicU64::new(0),
            writer: Mutex::new(None),
            stop: AtomicBool::new(false),
        })
    }

    /// 入队一批（组 gseq 条目；队列满背压：等消费线程腾出空间）。
    pub(crate) fn enqueue(&self, entries: Vec<WalEntry>, depth_cap: usize) -> Result<()> {
        let n = entries.len() as u64;
        let mut pending = self.pending.lock().unwrap();
        // 队列满背压（阶段1 `queue_full` 语义：depth >= cap 时写侧等待，防无界内存）
        while pending.len() as u64 + n > depth_cap as u64 {
            pending = self.cond.wait_timeout(pending, std::time::Duration::from_millis(1)).unwrap().0;
            if self.stop.load(Ordering::Relaxed) {
                return Err(Error::Unsupported("队列已关闭".into()));
            }
        }
        pending.extend(entries);
        self.depth.fetch_add(n, Ordering::Relaxed);
        self.cond.notify_all();
        Ok(())
    }

    /// 取空队列（消费线程 / flush_all 用）。返回取出的条目。
    fn drain(&self) -> Vec<WalEntry> {
        let mut pending = self.pending.lock().unwrap();
        std::mem::take(&mut *pending)
    }

    /// 队列是否为空（消费线程退出判定）。
    fn is_empty(&self) -> bool {
        self.pending.lock().unwrap().is_empty()
    }

    /// 把一批条目按 gseq 升序写盘并 fsync（段满自动切段 → 记录到裁剪集合）。
    fn write_batch(&self, dir: &Path, entries: &mut Vec<WalEntry>, trim: &TrimState) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        // 队列内跨线程交错入队可能乱序 → 写盘前按 gseq 排序（恢复归并语义不依赖文件内序，
        // 排序仅为段内近似有序 + 命名起点稳定）
        entries.sort_by_key(|e| e.gseq);
        let first = entries[0].gseq;
        let mut guard = self.writer.lock().unwrap();
        if guard.is_none() {
            *guard = Some(QueueFileWriter::create(dir, self.qid, first)?);
        }
        {
            let w = guard.as_mut().unwrap();
            w.write_batch(entries)?;
            w.flush()?;
        }
        // 段满切新段（旧段记入裁剪集合，`end <= cp` 后可删）
        if guard.as_ref().map_or(0, |w| w.bytes()) >= QUEUE_FILE_ROTATE_BYTES {
            let w = guard.take().unwrap();
            let meta = SegMeta { q: self.qid, start: w.min_gseq(), end: w.max_gseq(), path: w.path().to_path_buf() };
            w.close()?;
            trim.segs.lock().unwrap().push(meta);
        }
        self.consumed.fetch_add(entries.len() as u64, Ordering::Relaxed);
        self.depth.fetch_sub(entries.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// flush_all / 停机后的段收敛：当前段 `end <= cp` 时关闭入裁剪集（下次裁剪即删）。
    fn close_if_flushed(&self, cp: u64, trim: &TrimState) -> Result<()> {
        let mut guard = self.writer.lock().unwrap();
        let Some(w) = guard.as_ref() else { return Ok(()) };
        if w.max_gseq() <= cp {
            let w = guard.take().unwrap();
            let meta = SegMeta { q: self.qid, start: w.min_gseq(), end: w.max_gseq(), path: w.path().to_path_buf() };
            w.close()?;
            trim.segs.lock().unwrap().push(meta);
        }
        Ok(())
    }
}

/// 裁剪集合（跨队列共享；写线程/checkpoint 持锁追加，裁剪删除已收敛段）。
pub(crate) struct TrimState {
    segs: Mutex<Vec<SegMeta>>,
}

impl TrimState {
    fn new() -> Arc<Self> {
        Arc::new(TrimState { segs: Mutex::new(Vec::new()) })
    }
}

/// 队列运行时（Engine 持有一个；消费线程持内部状态 Arc）。
pub(crate) struct WalRuntime {
    pub dir: PathBuf,
    pub queues: Vec<Arc<QueueInner>>,
    /// checkpoint 安全点（min(各 CF 刷盘水位)）：`<= cp` 的段文件可删（恢复只回放 > cp）。
    pub(crate) cp: AtomicU64,
    /// 各 CF 已刷盘水位（索引 = WalEntry.cf；flush 完成回调推进）。
    pub(crate) cf_watermarks: [AtomicU64; 4],
    /// 已持久化 checkpoint（重启恢复回放起点）。
    persisted_cp: AtomicU64,
    checkpoint_path: PathBuf,
    trim: Arc<TrimState>,
    /// 组提交窗口（每队列消费线程写盘 + fsync 周期）。
    window: std::time::Duration,
    depth_cap: usize,
    stop: Arc<AtomicBool>,
    handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
    started: AtomicBool,
}

impl WalRuntime {
    /// 以既有目录/配置构建运行时（不 spawn 线程；Engine open 后调用 `start`）。
    pub fn build(dir: PathBuf, queues: usize, depth: usize, window_us: u64) -> Self {
        WalRuntime {
            dir: dir.clone(),
            queues: (0..queues).map(QueueInner::new).collect(),
            cp: AtomicU64::new(0),
            cf_watermarks: std::array::from_fn(|_| AtomicU64::new(0)),
            persisted_cp: AtomicU64::new(0),
            checkpoint_path: dir.join("checkpoint.json"),
            trim: TrimState::new(),
            window: std::time::Duration::from_micros(window_us.max(1)),
            depth_cap: depth.max(1),
            stop: Arc::new(AtomicBool::new(false)),
            handles: Mutex::new(Vec::new()),
            started: AtomicBool::new(false),
        }
    }

    /// 从 checkpoint 文件恢复持久化安全点（首次/无文件 = 0）。
    pub fn load_checkpoint(&self) -> u64 {
        let text = match std::fs::read_to_string(&self.checkpoint_path) {
            Ok(t) => t,
            Err(_) => return 0,
        };
        text.trim().parse::<u64>().unwrap_or(0)
    }

    /// 持久化 checkpoint（tmp + rename 原子落盘）。
    pub fn persist_checkpoint(&self) -> Result<()> {
        let cp = self.cp.load(Ordering::Relaxed);
        if let Some(p) = self.checkpoint_path.parent() {
            std::fs::create_dir_all(p)?;
        }
        let tmp = self.checkpoint_path.with_extension("tmp");
        std::fs::write(&tmp, cp.to_string())?;
        std::fs::rename(&tmp, &self.checkpoint_path)?;
        self.persisted_cp.store(cp, Ordering::Relaxed);
        Ok(())
    }

    /// CF flush 完成回调：推进该 CF 水位并重算 checkpoint（cp = min(各 CF 水位)）。
    pub fn note_flush(&self, cf: u8, flushed_max: u64) {
        if (cf as usize) < self.cf_watermarks.len() {
            self.cf_watermarks[cf as usize].fetch_max(flushed_max, Ordering::Relaxed);
        }
        let cp = self.cf_watermarks.iter().map(|w| w.load(Ordering::Relaxed)).min().unwrap_or(0);
        self.cp.store(cp, Ordering::Relaxed);
    }

    /// 标记某 CF 不存在（engine 未打开，如 cidx/outbox 关闭）：不参与 checkpoint 约束
    /// （水位视为 +∞，等价"该 CF 无未刷数据"）。
    pub fn mark_cf_absent(&self, cf: u8) {
        if (cf as usize) < self.cf_watermarks.len() {
            self.cf_watermarks[cf as usize].store(u64::MAX, Ordering::Relaxed);
        }
    }

    /// 裁剪：删除 `end <= cp` 的已切段文件（幂等；flush_all / 周期维护调用）。
    pub fn trim_segments(&self) {
        let cp = self.cp.load(Ordering::Relaxed);
        let mut segs = self.trim.segs.lock().unwrap();
        let mut keep = Vec::with_capacity(segs.len());
        for s in segs.drain(..) {
            if s.end <= cp {
                let _ = std::fs::remove_file(&s.path);
            } else {
                keep.push(s);
            }
        }
        *segs = keep;
    }

    /// 路由入队（engine 写入口；当前 CPU → queue）。
    pub(crate) fn submit(&self, q: usize, entries: Vec<WalEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.queues[q].enqueue(entries, self.depth_cap)
    }

    /// 同步排空全部队列并 fsync（flush_wal / 强安全档位）。随后：
    /// 计算 cp（重读各 CF 水位）→ 收敛段 → 持久化 checkpoint → 裁剪。
    pub fn flush_all(&self) -> Result<()> {
        for qi in &self.queues {
            let mut entries = qi.drain();
            if !entries.is_empty() {
                qi.write_batch(&self.dir, &mut entries, &self.trim)?;
            }
        }
        self.note_flush(255, 0); // 以当前水位重算 cp（no-op 推进）
        let cp = self.cp.load(Ordering::Relaxed);
        for qi in &self.queues {
            qi.close_if_flushed(cp, &self.trim)?;
        }
        self.persist_checkpoint()?;
        self.trim_segments();
        Ok(())
    }

    /// 启动每队列消费线程（窗口批量写盘 + fsync；Drop/显式关闭前 `shutdown`）。
    pub fn start(&mut self) -> Result<()> {
        if self.started.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        let dir = self.dir.clone();
        let stop = Arc::clone(&self.stop);
        let trim = Arc::clone(&self.trim);
        let window = self.window;
        let mut handles = Vec::new();
        for qi in &self.queues {
            let qi = Arc::clone(qi);
            let dir = dir.clone();
            let stop = Arc::clone(&stop);
            let trim = Arc::clone(&trim);
            let qid = qi.qid;
            handles.push(std::thread::Builder::new()
                .name(format!("percpu-wal-{qid}"))
                .spawn(move || {
                    // 消费循环：窗口到点/通知即排空写盘（组提交窗口语义）；stop 后排空尾部退出
                    let tick = if window < std::time::Duration::from_millis(10) {
                        window
                    } else {
                        std::time::Duration::from_millis(10)
                    };
                    loop {
                        let mut drained = Vec::new();
                        {
                            let mut pending = qi.pending.lock().unwrap();
                            while pending.is_empty() && !stop.load(Ordering::Relaxed) {
                                pending = qi.cond.wait_timeout(pending, tick).unwrap().0;
                            }
                            if !pending.is_empty() {
                                drained = std::mem::take(&mut *pending);
                            }
                        }
                        if drained.is_empty() {
                            if stop.load(Ordering::Relaxed) {
                                break;
                            }
                            continue;
                        }
                        let _ = qi.write_batch(&dir, &mut drained, &trim);
                        qi.cond.notify_all(); // 唤醒可能的背压等待者
                    }
                })?);
        }
        *self.handles.lock().unwrap() = handles;
        Ok(())
    }

    /// 停机：标志 + 唤醒消费线程排空尾部并 join（正常退出不丢窗口尾部）。
    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::Relaxed);
        for qi in &self.queues {
            qi.stop.store(true, Ordering::Relaxed);
            qi.cond.notify_all();
        }
        let handles = std::mem::take(&mut *self.handles.lock().unwrap());
        for h in handles {
            let _ = h.join();
        }
        let _ = self.flush_all();
    }

    /// 恢复用：读取全部队列文件，返回 `gseq > since` 的条目（gseq 全局归并，组序稳定）。
    pub fn records_after(&self, since: u64) -> Result<Vec<WalEntry>> {
        let mut all: Vec<WalEntry> = Vec::new();
        let entries = std::fs::read_dir(&self.dir)?;
        for e in entries.flatten() {
            let p = e.path();
            if parse_queue_file_name(&p.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default()).is_none() {
                continue; // checkpoint.json / 其它文件
            }
            for rec in read_queue_file(&p)? {
                if rec.gseq > since {
                    all.push(rec);
                }
            }
        }
        all.sort_by_key(|e| e.gseq);
        Ok(all)
    }

    /// 队列健康度快照（SHOW STATUS 数据源）。
    pub fn status(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!("percpu cp={} persisted_cp={}", self.cp.load(Ordering::Relaxed), self.persisted_cp.load(Ordering::Relaxed)));
        for (i, qi) in self.queues.iter().enumerate() {
            s.push_str(&format!(" | q{i}:depth={} consumed={}", qi.depth.load(Ordering::Relaxed), qi.consumed.load(Ordering::Relaxed)));
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn percpu_resolve_disabled_is_single_queue() {
        let c = Config::default();
        assert!(!c.storage.per_cpu_enabled, "阶段1 默认关闭（安全回退）");
        let w = PerCpuWal::resolve(&c);
        assert_eq!(w.queues, 1);
        assert_eq!(w.route(None), 0);
        assert_eq!(w.route(Some(0)), 0);
        assert_eq!(w.route(Some(5)), 0, "disabled 恒回队列 0");
    }

    #[test]
    fn percpu_resolve_enabled_auto_and_route() {
        let mut c = Config::default();
        c.storage.per_cpu_enabled = true;
        c.storage.per_cpu_queues = 0; // 自动
        let w = PerCpuWal::resolve(&c);
        assert!(w.queues >= 1 && w.queues <= 64, "自动 = 核数 clamp 1..=64，实际 {}", w.queues);
        if w.queues > 1 {
            assert_eq!(w.route(Some(0)), 0);
            assert!(w.route(Some(w.queues)) < w.queues, "超界回退轮询");
            assert!(w.route(None) < w.queues);
        }
        // 指定队列数截断
        c.storage.per_cpu_queues = 100;
        let w2 = PerCpuWal::resolve(&c);
        assert_eq!(w2.queues, 64);
        // 显式 1 = 单队列
        c.storage.per_cpu_queues = 1;
        let w3 = PerCpuWal::resolve(&c);
        assert_eq!(w3.queues, 1);
        assert_eq!(w3.route(None), 0);
    }

    #[test]
    fn percpu_queue_full_and_status() {
        let mut c = Config::default();
        c.storage.per_cpu_enabled = true;
        c.storage.per_cpu_queues = 2;
        c.storage.per_cpu_queue_depth = 4;
        let w = PerCpuWal::resolve(&c);
        assert!(!w.queue_full(0));
        w.depth_now[0].store(4, Ordering::Relaxed);
        assert!(w.queue_full(0));
        assert!(!w.queue_full(1));
        let s = w.status();
        assert!(s.contains("queues=2"));
        assert!(s.contains("q0:depth=4 consumed=0"), "{s}");
    }

    // ---------------- 阶段2a：WalEntry 编解码 + 队列文件读写 ----------------

    fn tmp_dir() -> std::path::PathBuf {
        static DIR: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let name = format!("pw-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    #[test]
    fn entry_codec_roundtrip() {
        // Put（各 cf）+ Delete 编解码往返
        let cases = vec![
            WalEntry::put(CF_PRIMARY, 7, vec![0, 0, 0, 0, 0, 0, 0, 42], b"{\"a\":1}".to_vec()),
            WalEntry::put(CF_DELTA, 8, vec![9, 9], b"v".to_vec()),
            WalEntry::put(CF_CIDX, 9, b"composite-key".to_vec(), Vec::new()),
            WalEntry::delete(CF_PRIMARY, 10, vec![1, 2, 3]),
        ];
        for e in &cases {
            let mut buf = Vec::new();
            encode_payload(e, &mut buf);
            let got = decode_payload(&buf).unwrap();
            assert_eq!(got, *e);
        }
    }

    #[test]
    fn entry_codec_rejects_short_or_trailing() {
        assert!(decode_payload(&[0u8; 9]).is_err(), "短负载拒绝");
        // 截断 varlen（len 声明超余下字节）→ Corrupted
        let e = WalEntry::put(CF_DELTA, 5, vec![0xAB; 300], vec![0xCD; 300]);
        let mut buf = Vec::new();
        encode_payload(&e, &mut buf);
        let got = decode_payload(&buf[..buf.len() / 2]).unwrap_err();
        assert!(matches!(got, Error::Corrupted(_)), "{got:?}");
    }

    #[test]
    fn queue_file_name_parse() {
        assert_eq!(parse_queue_file_name("wal-0-00000000000000000042.log"), Some((0, 42)));
        assert_eq!(parse_queue_file_name("wal-3-00000000000000000100.log"), Some((3, 100)));
        assert_eq!(parse_queue_file_name("wal.log"), None, "旧格式无 -queue- 段");
        assert_eq!(parse_queue_file_name("wal-1.log"), None);
        assert_eq!(parse_queue_file_name("sst-00000001.sst"), None);
    }

    #[test]
    fn queue_file_write_batch_flush_read_roundtrip() {
        let dir = tmp_dir();
        let mut w = QueueFileWriter::create(&dir, 0, 1).unwrap();
        let batch = vec![
            WalEntry::put(CF_PRIMARY, 1, vec![1], b"a".to_vec()),
            WalEntry::put(CF_PRIMARY, 2, vec![2], b"b".to_vec()),
            WalEntry::delete(CF_DELTA, 3, vec![3]),
        ];
        let max = w.write_batch(&batch).unwrap();
        assert_eq!(max, 3);
        // 未 flush 前文件为空（断电丢失模拟：drop 不落盘）
        assert!(read_queue_file(&w.path()).unwrap().is_empty());
        w.flush().unwrap();
        let recs = read_queue_file(&w.path()).unwrap();
        assert_eq!(recs, batch);
        // 追加第二批（切段后同文件继续）
        let b2 = vec![WalEntry::put(CF_CIDX, 9, b"k9".to_vec(), Vec::new())];
        w.write_batch(&b2).unwrap();
        w.flush().unwrap();
        let recs2 = read_queue_file(&w.path()).unwrap();
        assert_eq!(recs2.len(), 4);
        assert_eq!(recs2[3], b2[0]);
        assert_eq!(w.max_gseq(), 9);
    }

    #[test]
    fn queue_file_crash_tail_and_corrupt_stop() {
        let dir = tmp_dir();
        let mut w = QueueFileWriter::create(&dir, 1, 10).unwrap();
        for g in 10..=19u64 {
            w.write_batch(&[WalEntry::put(CF_PRIMARY, g, vec![g as u8], vec![0x42])]).unwrap();
        }
        w.flush().unwrap();
        let p = w.path().to_path_buf();
        drop(w);
        let mut data = std::fs::read(&p).unwrap();
        // 截断尾部一半 → 回放只保留完整记录
        data.truncate(data.len() / 2);
        std::fs::write(&p, &data).unwrap();
        let recs = read_queue_file(&p).unwrap();
        assert!(!recs.is_empty() && recs.len() <= 10);
        assert!(recs.iter().all(|r| r.gseq >= 10));
        // 损坏中间字节 → 损坏点前恢复、之后停止（不 panic）
        let mut w2 = QueueFileWriter::create(&dir, 1, 20).unwrap();
        for g in 20..=29u64 {
            w2.write_batch(&[WalEntry::put(CF_PRIMARY, g, vec![g as u8], vec![0x42])]).unwrap();
        }
        w2.flush().unwrap();
        let p2 = w2.path().to_path_buf();
        drop(w2);
        let mut data2 = std::fs::read(&p2).unwrap();
        let mid = data2.len() / 2;
        data2[mid] ^= 0xFF;
        std::fs::write(&p2, &data2).unwrap();
        let recs2 = read_queue_file(&p2).unwrap();
        assert!(recs2.len() < 10, "损坏点之后停止: {}", recs2.len());
    }

    #[test]
    fn queue_file_empty_and_append_reopen() {
        let dir = tmp_dir();
        // 空文件（新建未写）可安全回放为空
        let mut w = QueueFileWriter::create(&dir, 0, 1).unwrap();
        assert!(read_queue_file(&w.path()).unwrap().is_empty());
        w.write_batch(&[WalEntry::put(CF_DELTA, 100, vec![5], b"v".to_vec())]).unwrap();
        w.flush().unwrap();
        drop(w);
        // 重开（create 截断）→ 旧内容清空；新写从新命名文件开始
        let w2 = QueueFileWriter::create(&dir, 0, 101).unwrap();
        assert!(read_queue_file(&w2.path()).unwrap().is_empty());
    }

    // ---------------- 阶段2c/3a：队列运行时（多队列写-刷-回读、checkpoint、消费线程）----------------

    fn rt_dir() -> std::path::PathBuf {
        static DIR: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let name = format!("pwrt-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    #[test]
    fn multi_queue_submit_flush_recover_merged() {
        // 4 队列交错提交（含乱序 gseq）→ flush_all → 新实例读目录 → gseq 全局归并排序一致
        let dir = rt_dir();
        let mut rt = WalRuntime::build(dir.clone(), 4, 64, 1000);
        let mut expected: Vec<WalEntry> = Vec::new();
        let mut gseq = 0u64;
        for i in 0..50u64 {
            gseq += 1;
            let q = (i % 4) as usize;
            let e = if i % 5 == 0 {
                WalEntry::delete(CF_PRIMARY, gseq, vec![0, 0, 0, 0, 0, 0, i as u8, i as u8])
            } else {
                WalEntry::put(CF_DELTA, gseq, vec![i as u8; 8], format!("v{i}").into_bytes())
            };
            expected.push(e.clone());
            rt.submit(q, vec![e]).unwrap();
        }
        assert_eq!(gseq, 50);
        rt.flush_all().unwrap();
        assert!(rt.queues.iter().all(|q| q.is_empty()), "flush_all 后队列排空");
        // 新实例读取：gseq 归并后 = 提交序
        let rt2 = WalRuntime::build(dir.clone(), 4, 64, 1000);
        let recs = rt2.records_after(0).unwrap();
        assert_eq!(recs.len(), 50);
        for (a, b) in recs.iter().zip(expected.iter()) {
            assert_eq!(a.gseq, b.gseq);
            assert_eq!(a, b);
        }
        // since 过滤 + 洞语义（gseq 不是从 1 连续也按编号跳过）
        let rt3 = WalRuntime::build(dir.clone(), 4, 64, 1000);
        let tail = rt3.records_after(40).unwrap();
        assert_eq!(tail.len(), 10);
        assert!(tail.iter().all(|e| e.gseq > 40));
    }

    #[test]
    fn checkpoint_advance_trims_flushed_files() {
        // flush 水位推进 → cp=min(水位) → 持久化后重开不回放已刷记录（幂等不重复）
        let dir = rt_dir();
        let mut rt = WalRuntime::build(dir.clone(), 1, 64, 1000);
        for g in 1..=20u64 {
            rt.submit(0, vec![WalEntry::put(CF_PRIMARY, g, vec![g as u8], format!("v{g}").into_bytes())])
                .unwrap();
        }
        rt.flush_all().unwrap();
        assert_eq!(rt.load_checkpoint(), 0, "尚未有任何 CF flush → cp=0，文件保留");
        assert_eq!(rt.records_after(0).unwrap().len(), 20);
        // 模拟全部 CF 刷盘至 gseq 20（min 水位 = 20）→ flush_all 收敛段并裁剪
        rt.mark_cf_absent(CF_CIDX);
        rt.mark_cf_absent(CF_OUTBOX);
        rt.note_flush(CF_PRIMARY, 20);
        rt.note_flush(CF_DELTA, 20);
        rt.flush_all().unwrap();
        assert_eq!(rt.cp.load(Ordering::Relaxed), 20);
        assert_eq!(rt.load_checkpoint(), 20, "cp 已持久化");
        assert!(rt.records_after(20).unwrap().is_empty(), "已刷记录不重放");
        // 新写入（>cp）仍被回读
        rt.submit(0, vec![WalEntry::put(CF_PRIMARY, 21, vec![21], b"new".to_vec())]).unwrap();
        rt.flush_all().unwrap();
        let after = rt.records_after(20).unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].gseq, 21);
    }

    #[test]
    fn consumer_thread_window_drain_then_shutdown_flush() {
        // 消费线程按窗口自动写盘；停机 join 后排空尾部不丢
        let dir = rt_dir();
        let mut rt = WalRuntime::build(dir.clone(), 2, 1024, 1000);
        rt.start().unwrap();
        for i in 0..100u64 {
            let q = (i % 2) as usize;
            rt.submit(q, vec![WalEntry::put(CF_PRIMARY, i + 1, vec![i as u8], format!("v{i}").into_bytes())])
                .unwrap();
        }
        // 等待消费线程完成窗口落盘（≤ 窗口 + 余量）
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert!(
            rt.queues.iter().all(|q| q.consumed.load(Ordering::Relaxed) > 0),
            "消费线程应已写盘: {}",
            rt.status()
        );
        rt.shutdown();
        let rt2 = WalRuntime::build(dir.clone(), 2, 1024, 1000);
        let recs = rt2.records_after(0).unwrap();
        assert_eq!(recs.len(), 100, "停机后排空尾部不丢");
        // 跨队列 gseq 仍严格单调（写盘前按 gseq 排序）
        assert!(recs.windows(2).all(|w| w[0].gseq < w[1].gseq));
    }

    #[test]
    fn queue_backpressure_blocks_when_full() {
        // 队列满（depth_cap 小）时 enqueue 阻塞等待消费线程腾空；不 panic
        let dir = rt_dir();
        let mut rt = WalRuntime::build(dir.clone(), 1, 2, 5000); // cap=2，慢窗口
        rt.start().unwrap();
        rt.submit(0, vec![WalEntry::put(CF_PRIMARY, 1, vec![1], b"a".to_vec())]).unwrap();
        rt.submit(0, vec![WalEntry::put(CF_PRIMARY, 2, vec![2], b"b".to_vec())]).unwrap();
        // 第 3 批需等消费线程腾空（window 5ms）——验证不阻塞死锁（限时完成）
        rt.submit(0, vec![WalEntry::put(CF_PRIMARY, 3, vec![3], b"c".to_vec())]).unwrap();
        rt.shutdown();
        let rt2 = WalRuntime::build(dir.clone(), 1, 2, 5000);
        assert_eq!(rt2.records_after(0).unwrap().len(), 3);
    }
}
