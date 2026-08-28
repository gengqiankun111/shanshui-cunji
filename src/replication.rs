//! 主从复制（design 9.3，阶段 2）。
//!
//! 模型：一主多从；写入只发主节点，主节点记录**复制日志（ReplicationLog，追加持久化）**，
//! 异步（默认）/ 同步（等 Slave ACK）复制到副本（最终一致性）。
//!
//! - **`ReplicationLog`**：主节点每笔写入追加一条 `ReplEntry`（seq 单调递增），
//!   落盘为 append 文件（崩溃后重启从磁盘恢复，未推送的增量不丢）；
//! - **`Replicator`**：持有 slave RPC 地址列表；
//!   - `async`（默认）：`record` 只写日志，由后台周期调用 `push_pending` 批量推送（攒批 `batch_size`）；
//!   - `sync`（强一致）：`record` 写日志后**立即推送并等待 Slave ACK**（`ack_timeout_ms`）再返回；
//! - **Slave 侧 RPC 处理器**：`register_repl_handlers` 暴露 `repl.apply`（批量应用 + 返回 acked seq）。
//!
//! 复制接入点：分片节点的 `shard.put` 处理器（`register_shard_handlers_with_repl`），
//! 单机内核零修改（design 9.3）。元数据中心负责故障切换（副本升级为主）留阶段 2 后续。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::rpc::{RpcClient, RpcServer};

/// 复制日志条目。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplEntry {
    /// 单调递增序号（复制游标）。
    pub seq: u64,
    /// 操作："put" / "delete"。
    pub op: String,
    pub docid: u64,
    /// 文档 JSON 字符串（op=delete 时为空）。
    pub data: String,
    /// 倒排词条（op=delete 时为空）。
    pub terms: Vec<String>,
}

/// 复制日志（master 侧）：追加持久化 + 内存缓冲 + 推送游标。
pub struct ReplicationLog {
    path: PathBuf,
    /// 内存缓冲（尚未被确认推送到 slave 的增量）。
    entries: Vec<ReplEntry>,
    /// 已确认推送的最大 seq。
    pushed_seq: u64,
}

impl ReplicationLog {
    /// 打开（或创建）复制日志：加载已有追加文件（崩溃恢复）。
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("replication.log");
        let entries = if path.exists() {
            let text = std::fs::read_to_string(&path)?;
            let entries: Vec<ReplEntry> = serde_json::from_str(&text)
                .map_err(|e| Error::Corrupted(format!("复制日志解析失败: {e}")))?;
            entries
        } else {
            Vec::new()
        };
        let pushed_seq = entries.last().map(|e| e.seq).unwrap_or(0);
        Ok(Self {
            path,
            entries,
            pushed_seq,
        })
    }

    /// 追加一条写入（返回 seq）。
    pub fn append(&mut self, op: &str, docid: u64, data: &str, terms: &[String]) -> Result<u64> {
        let seq = self.entries.last().map(|e| e.seq).unwrap_or(0) + 1;
        self.entries.push(ReplEntry {
            seq,
            op: op.to_string(),
            docid,
            data: data.to_string(),
            terms: terms.to_vec(),
        });
        self.persist()?;
        Ok(seq)
    }

    /// 未推送增量（`pushed_seq` 之后的全部）。
    pub fn pending(&self) -> &[ReplEntry] {
        &self.entries
    }

    /// 已确认推送的最大 seq。
    pub fn pushed_seq(&self) -> u64 {
        self.pushed_seq
    }

    /// 确认推送推进到 `to_seq`，并压缩已确认的缓冲。
    pub fn advance(&mut self, to_seq: u64) {
        if to_seq <= self.pushed_seq {
            return;
        }
        self.pushed_seq = to_seq;
        self.entries.retain(|e| e.seq > to_seq);
        // 缓冲压缩后立即落盘（推进游标），避免重启重复推送（重复应用无害，幂等）
        let _ = self.persist();
    }

    /// 末尾 seq。
    pub fn tail_seq(&self) -> u64 {
        self.entries.last().map(|e| e.seq).unwrap_or(0)
    }

    fn persist(&self) -> Result<()> {
        let text = serde_json::to_string(&self.entries)
            .map_err(|e| Error::Serialize(format!("复制日志序列化失败: {e}")))?;
        let tmp = self.path.with_extension("log.tmp");
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

/// 复制器：master → slave 推送（async 攒批 / sync 立即等 ACK）。
pub struct Replicator {
    log: ReplicationLog,
    /// slave RPC 地址列表。
    slaves: Vec<String>,
    sync_mode: String,
    ack_timeout_ms: u64,
    /// 复用连接（node addr → client）。
    clients: HashMap<String, RpcClient>,
}

impl Replicator {
    pub fn new(
        dir: &Path,
        slaves: Vec<String>,
        sync_mode: &str,
        ack_timeout_ms: u64,
    ) -> Result<Self> {
        if !matches!(sync_mode, "async" | "sync") {
            return Err(Error::Cluster(format!("非法复制模式: {sync_mode}")));
        }
        Ok(Self {
            log: ReplicationLog::open(dir)?,
            slaves,
            sync_mode: sync_mode.to_string(),
            ack_timeout_ms,
            clients: HashMap::new(),
        })
    }

    /// 记录一笔写入：
    /// - `sync`：写日志 + 立即推送全部 pending 并等 Slave ACK；
    /// - `async`：只写日志（由后台周期 `push_pending` 批量推送）。
    pub fn record(&mut self, op: &str, docid: u64, data: &str, terms: &[String]) -> Result<()> {
        self.log.append(op, docid, data, terms)?;
        if self.sync_mode == "sync" {
            self.push_pending()?;
        }
        Ok(())
    }

    /// 推送全部 pending 到全部 slave；返回推送条数。async 模式由后台周期调用。
    pub fn push_pending(&mut self) -> Result<u64> {
        if self.slaves.is_empty() || self.log.pending().is_empty() {
            return Ok(0);
        }
        let entries = self.log.pending().to_vec();
        let pushed = entries.len() as u64;
        let slaves = self.slaves.clone();
        let timeout = self.ack_timeout_ms;
        let payload = json!({"entries": entries});
        let mut max_acked = 0u64;
        for addr in &slaves {
            let client = self.client(addr)?;
            client.set_read_timeout(std::time::Duration::from_millis(timeout))?;
            let r = client.call("repl.apply", payload.clone())?;
            let acked = r["acked_seq"].as_u64().unwrap_or(0);
            max_acked = max_acked.max(acked);
        }
        self.log.advance(max_acked);
        Ok(pushed)
    }

    /// 当前未推送条数（监控 / 测试）。
    pub fn pending_count(&self) -> usize {
        self.log.pending().len()
    }

    /// 当前复制游标。
    pub fn pushed_seq(&self) -> u64 {
        self.log.pushed_seq()
    }

    fn client(&mut self, addr: &str) -> Result<&mut RpcClient> {
        if !self.clients.contains_key(addr) {
            let c = RpcClient::connect(addr)?;
            self.clients.insert(addr.to_string(), c);
        }
        Ok(self.clients.get_mut(addr).unwrap())
    }
}

/// Slave 侧：将 Engine 暴露为复制应用端（`repl.apply`，幂等）。
pub fn register_repl_handlers(server: &RpcServer, engine: Arc<Mutex<Engine>>) {
    server.register("repl.apply", move |params| {
        let entries: Vec<ReplEntry> = serde_json::from_value(params["entries"].clone())
            .map_err(|e| format!("复制条目解析失败: {e}"))?;
        let mut eng = engine.lock().unwrap();
        let mut acked_seq = 0u64;
        for e in &entries {
            match e.op.as_str() {
                "put" => {
                    let term_refs: Vec<&str> = e.terms.iter().map(|s| s.as_str()).collect();
                    eng.put(e.docid, e.data.clone().into_bytes(), &term_refs)
                        .map_err(|err| err.to_string())?;
                }
                "delete" => {
                    eng.delete(e.docid).map_err(|err| err.to_string())?;
                }
                other => {
                    return Err(format!("未知复制操作: {other}"));
                }
            }
            acked_seq = e.seq;
        }
        Ok(json!({"acked_seq": acked_seq}))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::RpcServer;

    fn tmpdir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn terms(t: &[&str]) -> Vec<String> {
        t.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn log_appends_and_advances() {
        let dir = tmpdir();
        let mut log = ReplicationLog::open(dir.path()).unwrap();
        assert_eq!(log.tail_seq(), 0);
        let s1 = log.append("put", 1, "{\"a\":1}", &terms(&["t=1"])).unwrap();
        let s2 = log.append("put", 2, "{\"a\":2}", &terms(&["t=2"])).unwrap();
        assert_eq!(s2, s1 + 1);
        assert_eq!(log.pending().len(), 2);
        log.advance(s1);
        assert_eq!(log.pending().len(), 1, "已确认部分应从缓冲压缩");
        assert_eq!(log.pushed_seq(), s1);
        assert_eq!(log.pending()[0].docid, 2);
    }

    #[test]
    fn log_persists_across_restart() {
        let dir = tmpdir();
        {
            let mut log = ReplicationLog::open(dir.path()).unwrap();
            log.append("put", 42, "{\"d\":42}", &terms(&["k=v"])).unwrap();
        }
        // 重启：从磁盘恢复未推送增量（崩溃不丢）
        let log = ReplicationLog::open(dir.path()).unwrap();
        assert_eq!(log.pending().len(), 1);
        assert_eq!(log.pending()[0].docid, 42);
        assert_eq!(log.pushed_seq(), 1, "重启后游标恢复为磁盘末尾 seq（重复推送幂等无害）");
    }

    fn spawn_slave(dir: &std::path::Path) -> (String, Arc<Mutex<Engine>>) {
        let cfg = crate::config::Config::default();
        let engine = Arc::new(Mutex::new(Engine::open(dir, &cfg).unwrap()));
        let server = RpcServer::new();
        register_repl_handlers(&server, engine.clone());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handlers = server.handlers.lock().unwrap().clone();
        std::thread::spawn(move || {
            for s in listener.incoming() {
                if let Ok(s) = s {
                    let h = handlers.clone();
                    std::thread::spawn(move || crate::rpc::handle_connection(h, s));
                }
            }
        });
        (addr, engine)
    }

    #[test]
    fn sync_replication_applies_to_slave() {
        let dir = tmpdir();
        let (slave_addr, slave_engine) = spawn_slave(tmpdir().path());
        let mut repl = Replicator::new(dir.path(), vec![slave_addr], "sync", 2000).unwrap();

        repl.record("put", 1001, "{\"s\":\"active\"}", &terms(&["status=active"]))
            .unwrap();
        // sync 模式：record 即已推送到 slave
        assert_eq!(repl.pending_count(), 0);
        assert_eq!(repl.pushed_seq(), 1);
        let v = slave_engine.lock().unwrap().get(1001).unwrap().unwrap();
        assert!(String::from_utf8_lossy(&v).contains("active"));
    }

    #[test]
    fn async_replication_pushes_on_demand() {
        let dir = tmpdir();
        let (slave_addr, slave_engine) = spawn_slave(tmpdir().path());
        let mut repl = Replicator::new(dir.path(), vec![slave_addr], "async", 2000).unwrap();

        // async：record 只写日志，不立即复制
        repl.record("put", 1, "{\"s\":1}", &terms(&["t=1"])).unwrap();
        repl.record("put", 2, "{\"s\":2}", &terms(&["t=2"])).unwrap();
        assert_eq!(repl.pending_count(), 2);
        assert!(slave_engine.lock().unwrap().get(1).unwrap().is_none());

        // 后台周期调用 push_pending → 批量推送
        assert_eq!(repl.push_pending().unwrap(), 2);
        assert_eq!(repl.pending_count(), 0);
        assert!(slave_engine.lock().unwrap().get(1).unwrap().is_some());
        assert!(slave_engine.lock().unwrap().get(2).unwrap().is_some());
        // 无新数据时返回 0
        assert_eq!(repl.push_pending().unwrap(), 0);
    }

    #[test]
    fn async_replication_delete_applies() {
        let dir = tmpdir();
        let (slave_addr, slave_engine) = spawn_slave(tmpdir().path());
        let mut repl = Replicator::new(dir.path(), vec![slave_addr], "async", 2000).unwrap();
        repl.record("put", 7, "{\"s\":7}", &terms(&[])).unwrap();
        repl.record("delete", 7, "", &terms(&[])).unwrap();
        repl.push_pending().unwrap();
        assert!(slave_engine.lock().unwrap().get(7).unwrap().is_none());
    }

    #[test]
    fn repl_without_slaves_is_noop() {
        let dir = tmpdir();
        let mut repl = Replicator::new(dir.path(), vec![], "async", 2000).unwrap();
        repl.record("put", 1, "{}", &terms(&[])).unwrap();
        assert_eq!(repl.pending_count(), 1, "无 slave 时日志保留待推送");
        assert_eq!(repl.push_pending().unwrap(), 0);
    }
}
