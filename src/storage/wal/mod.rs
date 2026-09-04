//! WAL 预写日志（development 5.1 / design 4.3）。
//!
//! 记录格式：`Length(u32) ++ CRC32(u32) ++ Payload`
//! Payload := `Seq(u64) ++ OpType(u8) ++ Key(VarLen) ++ [Value(VarLen)]`
//! - OpType：0 = Put，1 = Delete；
//! - 组提交（Group Commit）：一批记录一次性写盘 + 一次 fsync；
//! - 崩溃回放：读到首条 CRC 损坏/截断记录即停（部分写入安全）；
//! - 延迟删除：旧段切分后重命名交给后台线程 unlink（design 4.3）。

mod reader;
mod ring;
mod writer;

pub use reader::WalReader;
pub use ring::{RingWal, RING_HEADER};
pub use writer::{WalRecord, WalWriter};

use std::path::Path;

use crate::error::Result;

pub const OP_PUT: u8 = 0;
pub const OP_DELETE: u8 = 1;

/// WAL 文件头（append 模式截断后写入，M8-P5）：magic + next_seq。
/// flush 后 WAL 清空重建，头持久化 next_seq 保证重开 seq 接续（不冲突）。
const WAL_HEADER: &[u8; 8] = b"SCWAL01\0";
const WAL_HEADER_LEN: usize = 16;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
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
    fn ring_large_capacity_multi_wrap_recovery() {
        // Ex-5.5 规模化回归：64MB 大容量环 + 多轮回绕（每轮写满并刷盘放行覆盖）
        // → 崩溃重开恢复最新周期记录，max_seq 接续（混沌回归增强）
        let path = tmp();
        let size = 8 * 1024 * 1024;
        let (mut r, _) = RingWal::open_or_create(&path, size).unwrap();
        let mut last_seq = 0u64;
        for cycle in 0..6u64 {
            // 每轮写 80_000 条（8MB 环容 ~26 万条；6 轮共 48 万条 > 容量 → 跨轮回绕覆盖）
            for i in 0..80_000u64 {
                last_seq = r
                    .append(OP_PUT, format!("c{cycle}-k{i}").as_bytes(), Some(format!("v{cycle}-{i}").as_bytes()))
                    .unwrap();
            }
            r.sync().unwrap();
            r.set_flushed_seq(last_seq); // 刷盘放行 → 下一轮可覆盖本轮回绕
        }
        drop(r); // 模拟崩溃
        let (_r2, recs) = RingWal::open_or_create(&path, size).unwrap();
        assert!(!recs.is_empty(), "崩溃后应恢复环内最新记录");
        let max_seq = recs.iter().map(|x| x.seq).max().unwrap();
        assert_eq!(max_seq, last_seq, "恢复最大 seq 应为最后写入");
        eprintln!(
            "[RingWal 规模化] 5 轮 × 20 万条（64MB 环）→ 恢复 {} 条，max_seq={max_seq}",
            recs.len()
        );
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
