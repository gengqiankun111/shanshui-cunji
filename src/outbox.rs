//! 本地消息表（Outbox，Ex-1，design_extension v0.1 L1）：业务写 + 待办消息同一本地事务。
//!
//! 思想：业务写入与 outbox 消息写入共享同一全局 seq（Engine 分配）与 fsync 点
//! （`flush_wal`/组提交）——崩溃恢复时各列族 WAL 按 seq 回放，outbox 消息与业务写
//! **同生共死**（本地原子）；后台投递器扫描 pending → 目标投递 → 标记 done；
//! 消费端按幂等键（docid+seq）去重，重复投递不叠加。
//!
//! 存储：独立列族 `outbox`，key = `encode_docid(u64 大端) ++ seq.to_be_bytes()`（前缀扫描
//! 按 docid），value = `[status u8] ++ payload`（0=pending / 1=done）。
//! 复用 ColumnFamily 的 WAL + MemTable + SST 全链路（与 primary 同崩溃安全模型）。

use std::path::Path;

use crate::column_family::ColumnFamily;
use crate::config::model::Config;
use crate::error::Result;
use crate::keys::encode_docid;

/// 消息状态字节。
pub const STATUS_PENDING: u8 = 0;
pub const STATUS_DONE: u8 = 1;

/// 本地消息表：独立列族 + 投递/排空能力。
pub struct Outbox {
    cf: ColumnFamily,
}

impl Outbox {
    /// 打开（或创建）outbox 列族。
    pub fn open(dir: &Path, cfg: &Config) -> Result<Self> {
        let cf = ColumnFamily::open("outbox", dir, cfg)?;
        Ok(Self { cf })
    }

    /// 当前 WAL 下一可分配 seq（Engine 全局 seq 接续用）。
    pub fn wal_next_seq(&self) -> u64 {
        self.cf.wal_next_seq()
    }

    /// 同步 outbox WAL（Engine::flush_wal 统一提交，本地原子性 fsync 点）。
    pub fn sync_wal(&mut self) -> Result<()> {
        self.cf.sync_wal()
    }

    /// 入队（与业务写共享全局 seq 与 fsync 点 → 本地原子）。seq 由 Engine 全局分配。
    pub fn enqueue(&mut self, docid: u64, seq: u64, payload: &[u8]) -> Result<u64> {
        let mut key = encode_docid(docid).to_vec();
        key.extend_from_slice(&seq.to_be_bytes());
        let mut val = Vec::with_capacity(1 + payload.len());
        val.push(STATUS_PENDING);
        val.extend_from_slice(payload);
        self.cf.put_bytes_nosync(key, val)
    }

    /// 扫描某 docid 前缀的全部消息（按 seq 升序）。
    fn scan_docid(&mut self, docid: u64) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let start = encode_docid(docid).to_vec();
        let mut end = start.clone();
        end.extend_from_slice(&[0xFF; 8]);
        self.cf.scan_raw_range(Some(&start), Some(&end))
    }

    /// 扫描全部消息（全表遍历，投递器用）。
    pub fn scan_all(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.cf.scan_raw_range(None, None)
    }

    /// 标记 done（投递成功）。
    pub fn mark_done(&mut self, key: &[u8]) -> Result<()> {
        let (docid, seq) = parse_key(key);
        let rows = self.scan_docid(docid)?;
        let target = rows.into_iter().find(|(k, _)| k.as_slice() == key);
        if let Some((k, mut v)) = target {
            v[0] = STATUS_DONE;
            self.cf.put_bytes_nosync(k, v)?;
        }
        let _ = seq;
        Ok(())
    }

    /// 投递器：扫描全部 pending → 逐条投递（回调返回 true=成功）→ 标记 done。
    /// 失败的留 pending（调用方负责退避重试）。
    pub fn dispatch(&mut self, mut deliver: impl FnMut(&[u8], &[u8]) -> bool) -> Result<usize> {
        let rows = self.scan_all()?;
        let mut delivered = 0usize;
        for (key, val) in rows {
            if val.first() != Some(&STATUS_PENDING) {
                continue;
            }
            let payload = &val[1..];
            if deliver(&key, payload) {
                self.mark_done(&key)?;
                delivered += 1;
            }
        }
        Ok(delivered)
    }

    /// 当前 pending 消息数（排空校验用）。
    pub fn pending_count(&mut self) -> Result<usize> {
        Ok(self
            .scan_all()?
            .iter()
            .filter(|(_, v)| v.first() == Some(&STATUS_PENDING))
            .count())
    }

    /// 是否已排空（扩容/切换前置条件：pending = 0）。
    pub fn drained(&mut self) -> Result<bool> {
        Ok(self.pending_count()? == 0)
    }
}

/// 解析消息 key（docid + seq）。
fn parse_key(key: &[u8]) -> (u64, u64) {
    let docid = u64::from_be_bytes(key[..8].try_into().unwrap_or([0; 8]));
    let seq = u64::from_be_bytes(key[8..].try_into().unwrap_or([0; 8]));
    (docid, seq)
}

/// 消费端幂等去重（Ex-1.3）：按幂等键（docid+seq）记录 applied，重复投递不叠加。
#[derive(Default)]
pub struct IdempotentConsumer {
    applied: std::collections::HashSet<(u64, u64)>,
    received: u64,
}

impl IdempotentConsumer {
    pub fn new() -> Self {
        Self::default()
    }

    /// 幂等 apply：首次返回 true 并记录；重复键返回 false（不叠加）。
    pub fn apply(&mut self, key: &[u8]) -> bool {
        let (docid, seq) = parse_key(key);
        if self.applied.contains(&(docid, seq)) {
            return false;
        }
        self.applied.insert((docid, seq));
        self.received += 1;
        true
    }

    pub fn received(&self) -> u64 {
        self.received
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::sync::OnceLock;

    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<TempDir> = OnceLock::new();
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let name = format!("ob-{}", SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    fn cfg() -> Config {
        let mut c = Config::default();
        c.sstable.compression = "none".into();
        c
    }

    #[test]
    fn enqueue_scan_and_dispatch() {
        let dir = tmp();
        let cfg = cfg();
        let mut ob = Outbox::open(&dir, &cfg).unwrap();
        ob.enqueue(1, 1, b"msg-1").unwrap();
        ob.enqueue(2, 2, b"msg-2").unwrap();
        assert_eq!(ob.pending_count().unwrap(), 2);
        let mut consumer = IdempotentConsumer::new();
        let delivered = ob
            .dispatch(|k, p| {
                assert!(p.starts_with(b"msg-"));
                consumer.apply(k)
            })
            .unwrap();
        assert_eq!(delivered, 2);
        assert!(ob.drained().unwrap(), "投递后排空");
        assert_eq!(consumer.received(), 2);
    }

    #[test]
    fn delivery_retry_until_drained() {
        let dir = tmp();
        let mut ob = Outbox::open(&dir, &cfg()).unwrap();
        ob.enqueue(1, 1, b"m").unwrap();
        let mut attempts = 0u32;
        let mut delivered = 0usize;
        for _ in 0..5 {
            delivered += ob
                .dispatch(|_, _| {
                    attempts += 1;
                    attempts >= 3
                })
                .unwrap();
            if ob.drained().unwrap() {
                break;
            }
        }
        assert_eq!(delivered, 1);
        assert_eq!(attempts, 3, "失败重试直到成功");
        assert!(ob.drained().unwrap());
    }

    #[test]
    fn idempotent_consume_prevents_duplicate() {
        let dir = tmp();
        let mut ob = Outbox::open(&dir, &cfg()).unwrap();
        ob.enqueue(1, 1, b"dup").unwrap();
        let mut consumer = IdempotentConsumer::new();
        ob.dispatch(|k, _| consumer.apply(k)).unwrap();
        assert_eq!(consumer.received(), 1);
        // 重复投递（同一幂等键）
        let mut key = encode_docid(1).to_vec();
        key.extend_from_slice(&1u64.to_be_bytes());
        assert!(!consumer.apply(&key), "重复投递去重");
        assert_eq!(consumer.received(), 1);
    }

    #[test]
    fn pending_survives_reopen() {
        // 崩溃恢复：enqueue 未投递 → 重开后 pending 保留（WAL 回放重建）
        let dir = tmp();
        let cfg = cfg();
        {
            let mut ob = Outbox::open(&dir, &cfg).unwrap();
            ob.enqueue(7, 1, b"keep").unwrap();
            ob.cf.sync_wal().unwrap(); // 落盘（WAL fsync）
        }
        let mut ob2 = Outbox::open(&dir, &cfg).unwrap();
        assert_eq!(ob2.pending_count().unwrap(), 1, "重开 pending 保留");
        let mut consumer = IdempotentConsumer::new();
        let n = ob2.dispatch(|k, _| consumer.apply(k)).unwrap();
        assert_eq!(n, 1);
    }
}
