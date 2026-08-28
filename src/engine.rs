//! 文档引擎 + 查询执行器（design 7.1 / development 步骤 12）。
//!
//! 整合主数据列族、组合索引列族、倒排索引与 HotCache，对外提供文档级 CRUD 与查询；
//! 查询执行器基于 optimizer 静态路由（MVP 最小集枚举，无代价估算），
//! 后续动态路由（代价估算依赖统计载荷）在阶段 1.5 落地。
//!
//! 删除一致性：文档删除后，倒排中残留 docid 在回表时经主数据 Tombstone 天然过滤。

use std::path::Path;

use roaring::RoaringBitmap;

use crate::column_family::ColumnFamily;
use crate::config::model::Config;
use crate::error::Result;
use crate::hotcache::HotCache;
use crate::inverted::InvertedIndex;
use crate::keys::{encode_docid, encode_varlen};
use crate::optimizer::{route, AccessPath, QuerySpec};
use crate::watchdog::{Watchdog, DEFAULT_QUERY_TIMEOUT};

/// 引擎：组合主数据 + 组合索引 + Delta 增量 + 倒排 + HotCache。
pub struct Engine {
    /// 主数据列族（value = 序列化文档字节）。
    primary: ColumnFamily,
    /// 组合索引列族（key = encode_composite_key）。
    cidx: Option<ColumnFamily>,
    /// Delta 增量列族（阶段 1.5，key = encode_docid ++ VarLen(field)，Merge-on-Read 覆盖 Base）。
    delta: ColumnFamily,
    /// 倒排索引。
    inverted: InvertedIndex,
    /// 文档热缓存。
    hotcache: HotCache,
    /// 看门狗（OOM 限流 + 查询超时熔断）。
    watchdog: Watchdog,
    /// 内存使用率估算（0~1，由上层注入或监控更新）。
    mem_ratio: f64,
    /// 内存硬上限（MB，`memory.max_memory_mb`，供 admin status）。
    max_memory_mb: usize,
}

/// 查询结果行：docid + 文档字节。
pub type QueryRow = (u64, Vec<u8>);

/// 引擎状态指标（`admin status` 数据源）。
#[derive(Debug, Clone)]
pub struct EngineStats {
    /// LSM：SST 文件总数（primary + cidx + delta）。
    pub sst_file_count: usize,
    /// 倒排内存累积 posting 数。
    pub inverted_mem_docids: u64,
    /// 倒排磁盘段数。
    pub inverted_segments: usize,
    /// 当前序列号（阶段 2 接入）。
    pub seq: u64,
    /// 内存使用率估算（0~1）。
    pub mem_ratio: f64,
    /// 内存硬上限（MB）。
    pub max_memory_mb: usize,
}

impl Engine {
    /// 打开（或创建）引擎。倒排刷盘阈值取自内存预算的比例（MVP 固定 1M posting）。
    pub fn open(data_dir: &Path, cfg: &Config) -> Result<Self> {
        Self::open_with_timeout(data_dir, cfg, DEFAULT_QUERY_TIMEOUT)
    }

    /// 打开引擎并指定查询超时（压测/大结果集场景需放宽熔断阈值）。
    pub fn open_with_timeout(
        data_dir: &Path,
        cfg: &Config,
        query_timeout: std::time::Duration,
    ) -> Result<Self> {
        let primary = ColumnFamily::open("primary", &data_dir.join("primary"), cfg)?;
        let inverted = InvertedIndex::open_with_gc(
            &data_dir.join("inverted"),
            1_000_000,
            &cfg.inverted.engine,
            cfg.inverted.segment_max_size_mb * 1024 * 1024,
        )?;
        let cidx = ColumnFamily::open("cidx", &data_dir.join("cidx"), cfg).ok();
        let delta = ColumnFamily::open("delta", &data_dir.join("delta"), cfg)?;
        let hotcache = HotCache::new(cfg.hotcache.clone());
        let watchdog = Watchdog::new(cfg, query_timeout);
        Ok(Self {
            primary,
            cidx,
            inverted,
            delta,
            hotcache,
            watchdog,
            mem_ratio: 0.0,
            max_memory_mb: cfg.hotcache.max_memory_mb + cfg.blockcache.max_memory_mb,
        })
    }

    /// 更新内存使用率估算（OOM Guardian 输入，由监控/统计层刷新）。
    pub fn set_mem_ratio(&mut self, ratio: f64) {
        self.mem_ratio = ratio.clamp(0.0, 1.0);
    }

    /// 写入文档（docid + 序列化字节 + 该文档涉及的倒排词条）。
    /// 写失效链：先失效 HotCache 与组合索引旧条目，最后写 LSM（design 6.6）。
    /// OOM Guardian：写入前按水位限流/熔断（design 14.1.1）。
    pub fn put(&mut self, docid: u64, value: Vec<u8>, terms: &[&str]) -> Result<()> {
        // 内存限流：软水位返回限流信号（MVP 仍放行，记录计数）；硬水位直接拒绝
        let _status = self.watchdog.memory_check(self.mem_ratio)?;
        self.put_nosync(docid, value, terms)?;
        self.flush_wal()
    }

    /// 批量写入（不逐条 fsync，供亿级压测；结束时调用 `flush_wal` 统一提交）。
    pub fn put_nosync(&mut self, docid: u64, value: Vec<u8>, terms: &[&str]) -> Result<()> {
        // ① 失效 HotCache 该 docid
        self.hotcache.invalidate(docid);
        // ② 主数据（权威源，WAL 攒批不逐条 fsync）；全量覆盖 → 清空该 docid 的增量（避免旧 patch 覆盖新数据）
        self.primary
            .put_bytes_nosync(encode_docid(docid).to_vec(), value.clone())?;
        self.delta.delete_prefix(&encode_docid(docid))?;
        // ③ 倒排（内存字典累积，达阈值由调用方/后台刷盘）
        for t in terms {
            self.inverted.add(t, docid);
        }
        // ④ 回填 HotCache（写后回填，供热点查询亚毫秒命中）
        self.hotcache.put(docid, value);
        Ok(())
    }

    /// 统一提交 WAL（批量写入结束后调用，保证崩溃可恢复）。
    pub fn flush_wal(&mut self) -> Result<()> {
        self.primary.sync_wal()?;
        self.delta.sync_wal()?;
        Ok(())
    }

    /// 删除文档：主数据 Tombstone + 失效 HotCache + 清空 Delta（倒排残留 docid 由回表过滤）。
    pub fn delete(&mut self, docid: u64) -> Result<()> {
        self.hotcache.invalidate(docid);
        self.primary.delete(docid)?;
        self.delta.delete_prefix(&encode_docid(docid))?;
        Ok(())
    }

    /// 部分更新（阶段 1.5，design 4.7）：仅写入变更字段到 Delta CF（几十字节小记录），
    /// 读取时 Merge-on-Read 覆盖 Base；`null` 值表示删除该字段。替代全量 PUT，写入 IO 放大趋近 1。
    pub fn patch(&mut self, docid: u64, fields: &[(&str, serde_json::Value)]) -> Result<()> {
        self.hotcache.invalidate(docid);
        for (f, v) in fields {
            let mut key = encode_docid(docid).to_vec();
            encode_varlen(&mut key, f.as_bytes());
            let val =
                serde_json::to_vec(v).map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
            self.delta.put_bytes_nosync(key, val)?;
        }
        self.flush_wal()
    }

    /// 点查文档：HotCache 命中直达，否则主数据 LSM + Delta Merge-on-Read。
    pub fn get(&mut self, docid: u64) -> Result<Option<Vec<u8>>> {
        if let Some(v) = self.hotcache.get(docid) {
            return Ok(Some(v));
        }
        let found = self.primary.get(docid)?;
        let Some((bv, _)) = found else {
            return Ok(None);
        };
        // Delta 覆盖（对象合并；null 删除字段）；非 JSON / 非对象文档直接返回 Base（raw 字节场景）
        let obj: serde_json::Value = match serde_json::from_slice(&bv) {
            Ok(v) => v,
            Err(_) => return Ok(Some(bv)),
        };
        let mut map = match obj {
            serde_json::Value::Object(m) => m,
            _ => return Ok(Some(bv)),
        };
        let start = encode_docid(docid).to_vec();
        let mut end = start.clone();
        end.extend_from_slice(&[0xFF; 4]);
        let rows = self.delta.scan_raw_range(Some(&start), Some(&end))?;
        for (k, v) in rows {
            if !k.starts_with(&start) || k.len() < 12 {
                continue;
            }
            let field = String::from_utf8(k[12..].to_vec())
                .map_err(|_| crate::error::Error::Corrupted("Delta 字段名非法 UTF-8".into()))?;
            let val: serde_json::Value = serde_json::from_slice(&v)
                .map_err(|e| crate::error::Error::Corrupted(format!("Delta 值解析失败: {e}")))?;
            if val.is_null() {
                map.shift_remove(&field);
            } else {
                map.insert(field, val);
            }
        }
        let merged =
            serde_json::to_vec(&map).map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
        self.hotcache.put(docid, merged.clone());
        Ok(Some(merged))
    }

    /// 倒排词条查询：合并 posting（RoaringBitmap）→ 回表取文档。
    pub fn search_term(&mut self, term: &str) -> Result<Vec<QueryRow>> {
        let bitmap = self.inverted.search(term)?;
        let mut out = Vec::new();
        for docid in bitmap {
            if let Some(v) = self.get(docid as u64)? {
                out.push((docid as u64, v));
            }
        }
        Ok(out)
    }

    /// 主键范围扫描。
    pub fn scan_range(&mut self, start: Option<u64>, end: Option<u64>) -> Result<Vec<QueryRow>> {
        self.primary.scan_range(start, end)
    }

    /// 组合索引前缀查询：编码前缀键范围扫描 → 回表主数据。
    pub fn query_by_composite_prefix(&mut self, fields: &[&[u8]]) -> Result<Vec<QueryRow>> {
        let Some(cidx) = &mut self.cidx else {
            return Ok(Vec::new());
        };
        let start = crate::keys::encode_composite_key(fields, 0);
        let end = crate::keys::encode_composite_key(fields, u64::MAX);
        let hits = cidx.scan_raw_range(Some(&start), Some(&end))?;
        let mut out = Vec::new();
        for (key, _) in hits {
            let (_fields, docid) = crate::keys::decode_composite_key(&key)?;
            if let Some(v) = self.get(docid)? {
                out.push((docid, v));
            }
        }
        Ok(out)
    }

    /// 查询执行器：按 QuerySpec 静态路由到访问路径并执行（design 7.1 最小集枚举）。
    /// 看门狗：查询超时熔断（逐行检查 QueryGuard，超时返回 QueryTooExpensive）。
    pub fn execute(&mut self, spec: &QuerySpec) -> Result<Vec<QueryRow>> {
        let guard = self.watchdog.begin_query();
        let rows = match route(spec) {
            AccessPath::PrimaryPoint => {
                let docid = spec
                    .primary_eq
                    .as_ref()
                    .map(|k| crate::keys::decode_docid(k))
                    .transpose()?
                    .unwrap_or(0);
                self.get(docid)?.into_iter().map(|v| (docid, v)).collect()
            }
            AccessPath::PrimaryRange => self.scan_range(None, None)?,
            AccessPath::CompositeIndex { fields } => {
                let fs: Vec<&[u8]> = fields.iter().map(|s| s.as_bytes()).collect();
                self.query_by_composite_prefix(&fs)?
            }
            AccessPath::Inverted { term } => {
                // 倒排回表：逐行熔断检查
                let bitmap = self.inverted.search(&term)?;
                let mut out = Vec::new();
                for docid in bitmap {
                    if guard.is_expired() {
                        return Err(crate::error::Error::QueryTooExpensive(format!(
                            "查询超时（guard #{} > {}ms），熔断中止",
                            guard.query_id(),
                            guard.timeout().as_millis()
                        )));
                    }
                    if let Some(v) = self.get(docid as u64)? {
                        out.push((docid as u64, v));
                    }
                }
                out
            }
            AccessPath::FullScan => self.scan_range(None, None)?,
        };
        Ok(rows)
    }

    /// 倒排内存累积条数（供后台刷盘决策）。
    pub fn inverted_mem_docids(&self) -> u64 {
        self.inverted.mem_docids()
    }

    /// 强制倒排刷盘。
    pub fn flush_inverted(&mut self) -> Result<()> {
        self.inverted.flush_segment()
    }

    /// 倒排某词条命中的 docid 集合（不回表，供测试/监控）。
    pub fn inverted_posting(&self, term: &str) -> Result<RoaringBitmap> {
        self.inverted.search(term)
    }

    /// 倒排某词条命中的文档数（COUNT 聚合，<0.1ms）。
    pub fn inverted_doc_count(&self, term: &str) -> Result<u64> {
        self.inverted.doc_count(term)
    }

    /// 按字段前缀分组（GROUP BY 聚合）：返回 `field=value` 各分组的文档数。
    pub fn inverted_group_by(&self, field: &str) -> Result<Vec<(String, u64)>> {
        self.inverted.group_by(field)
    }

    /// 引擎状态指标（design 20 / development 5.25，供 `admin status`）。
    pub fn stats(&self) -> EngineStats {
        EngineStats {
            sst_file_count: self.primary.sst_count()
                + self.delta.sst_count()
                + self.cidx.as_ref().map_or(0, |c| c.sst_count()),
            inverted_mem_docids: self.inverted.mem_docids(),
            inverted_segments: self.inverted.segment_count(),
            seq: 0, // 阶段 2 接入执行器统计
            mem_ratio: self.mem_ratio,
            max_memory_mb: self.max_memory_mb,
        }
    }

    /// 备份前一致性准备（development 5.11 冷备份第 1-2 步）：
    /// 刷 WAL → 全部 MemTable 落盘为 SST → 倒排内存字典刷盘为 `.seg` 段，
    /// 保证数据目录磁盘态自包含（含倒排段清单 Manifest、字段注册表等随目录整体打包）。
    pub fn prepare_backup(&mut self) -> Result<()> {
        self.flush_wal()?;
        if self.primary.memtable_bytes() > 0 {
            self.primary.switch_and_flush()?;
        }
        if let Some(cidx) = &mut self.cidx {
            if cidx.memtable_bytes() > 0 {
                cidx.switch_and_flush()?;
            }
        }
        self.flush_inverted()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;

    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let name = format!("eng-{}", SEQ.fetch_add(1, Ordering::Relaxed));
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
    fn put_get_roundtrip() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"doc-1".to_vec(), &["rust"]).unwrap();
        e.put(2, b"doc-2".to_vec(), &["go"]).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"doc-1");
        assert_eq!(e.get(2).unwrap().unwrap(), b"doc-2");
        assert!(e.get(99).unwrap().is_none());
    }

    #[test]
    fn delete_hides_doc() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(7, b"x".to_vec(), &["k"]).unwrap();
        e.delete(7).unwrap();
        assert!(e.get(7).unwrap().is_none());
    }

    #[test]
    fn search_term_returns_docs() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"about rust".to_vec(), &["rust"]).unwrap();
        e.put(2, b"rust rocks".to_vec(), &["rust"]).unwrap();
        e.put(3, b"go is cool".to_vec(), &["go"]).unwrap();
        let rows = e.search_term("rust").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[1].0, 2);
        assert!(e.search_term("go").unwrap().len() == 1);
        assert!(e.search_term("absent").unwrap().is_empty());
    }

    #[test]
    fn deleted_doc_excluded_from_search() {
        // 倒排残留 docid 回表时被主数据 Tombstone 过滤
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"rust".to_vec(), &["rust"]).unwrap();
        e.put(2, b"rust2".to_vec(), &["rust"]).unwrap();
        e.delete(1).unwrap();
        let rows = e.search_term("rust").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 2);
    }

    #[test]
    fn execute_routes_by_spec() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(42, b"hello world".to_vec(), &["hello"]).unwrap();

        // 主键点查
        let spec = QuerySpec {
            primary_eq: Some(crate::keys::encode_docid(42).to_vec()),
            primary_range: false,
            index_prefix: vec![],
            term: None,
        };
        let rows = e.execute(&spec).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 42);

        // 倒排词条查询
        let spec2 = QuerySpec {
            primary_eq: None,
            primary_range: false,
            index_prefix: vec![],
            term: Some("hello".into()),
        };
        let rows2 = e.execute(&spec2).unwrap();
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0].1, b"hello world");
    }

    #[test]
    fn composite_prefix_query() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        // 组合索引：写入索引条目（key = composite(fields, docid)）
        if let Some(cidx) = &mut e.cidx {
            for docid in [10u64, 20, 30] {
                let key = crate::keys::encode_composite_key(&[b"active"], docid);
                cidx.put_bytes(key, docid.to_le_bytes().to_vec()).unwrap();
            }
            let key = crate::keys::encode_composite_key(&[b"inactive"], 99);
            cidx.put_bytes(key, 99u64.to_le_bytes().to_vec()).unwrap();
        }
        // 主数据写文档（回表需要）
        e.put(10, b"d10".to_vec(), &[]).unwrap();
        e.put(20, b"d20".to_vec(), &[]).unwrap();
        e.put(30, b"d30".to_vec(), &[]).unwrap();
        e.put(99, b"d99".to_vec(), &[]).unwrap();

        let rows = e.query_by_composite_prefix(&[b"active"]).unwrap();
        let mut ids: Vec<u64> = rows.iter().map(|(d, _)| *d).collect();
        ids.sort();
        assert_eq!(ids, vec![10, 20, 30]);
    }

    #[test]
    fn oom_guardian_blocks_writes_at_stall() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.set_mem_ratio(0.5);
        e.put(1, b"ok".to_vec(), &[]).unwrap(); // 正常写入

        e.set_mem_ratio(1.0); // 模拟 RSS 打满
        let err = e.put(2, b"blocked".to_vec(), &[]).unwrap_err();
        assert!(matches!(err, crate::error::Error::MemoryOverload(_)));
        // 被拒写入不生效
        assert!(e.get(2).unwrap().is_none());
        assert!(e.get(1).unwrap().is_some());
    }

    #[test]
    fn patch_merge_on_read() {
        // 阶段 1.5 Delta CF：patch 部分更新 → get 合并覆盖；重启后 WAL 恢复仍生效
        let dir = tmp();
        let mut e = Engine::open(&dir, &cfg()).unwrap();
        e.put(
            1,
            br#"{"status":"active","amount":100,"device":"android"}"#.to_vec(),
            &[],
        )
        .unwrap();
        e.patch(
            1,
            &[
                ("status", serde_json::json!("inactive")),
                ("note", serde_json::json!("updated")),
                ("amount", serde_json::Value::Null), // null = 删除字段
            ],
        )
        .unwrap();
        let v = e.get(1).unwrap().unwrap();
        let obj: serde_json::Value = serde_json::from_slice(&v).unwrap();
        assert_eq!(obj["status"], serde_json::json!("inactive"), "patch 应覆盖");
        assert_eq!(obj["note"], serde_json::json!("updated"), "patch 应新增");
        assert!(obj.get("amount").is_none(), "null patch 应删除字段");
        assert_eq!(
            obj["device"],
            serde_json::json!("android"),
            "未 patch 字段保留"
        );
        // 重启后 Delta 仍生效（WAL 恢复）
        drop(e);
        let mut e2 = Engine::open(&dir, &cfg()).unwrap();
        let obj2: serde_json::Value = serde_json::from_slice(&e2.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(obj2["status"], serde_json::json!("inactive"));
        assert_eq!(obj2["note"], serde_json::json!("updated"));
    }

    #[test]
    fn full_put_clears_delta() {
        // 全量 put 覆盖 → 清空该 docid 增量，避免旧 patch 覆盖新数据
        let dir = tmp();
        let mut e = Engine::open(&dir, &cfg()).unwrap();
        e.put(1, br#"{"status":"active","amount":100}"#.to_vec(), &[])
            .unwrap();
        e.patch(1, &[("status", serde_json::json!("patched"))])
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&e.get(1).unwrap().unwrap()),
            r#"{"status":"patched","amount":100}"#
        );
        e.put(1, br#"{"status":"fresh","amount":200}"#.to_vec(), &[])
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&e.get(1).unwrap().unwrap()),
            r#"{"status":"fresh","amount":200}"#
        );
    }

    #[test]
    fn delete_clears_delta() {
        // 删除文档 → Delta 清空，避免复活
        let dir = tmp();
        let mut e = Engine::open(&dir, &cfg()).unwrap();
        e.put(1, br#"{"status":"active"}"#.to_vec(), &[]).unwrap();
        e.patch(1, &[("note", serde_json::json!("x"))]).unwrap();
        e.delete(1).unwrap();
        assert!(e.get(1).unwrap().is_none(), "删除后 Delta 不应复活文档");
    }

    #[test]
    fn oom_guardian_throttles_in_soft_range() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.set_mem_ratio(0.9); // 软水位区间
        e.put(1, b"throttled-but-allowed".to_vec(), &[]).unwrap();
        assert!(e.get(1).unwrap().is_some());
        // 限流计数已记录
        assert!(e.watchdog.memory().throttled_count() >= 1);
    }

    #[test]
    fn restart_preserves_data_and_inverted() {
        // 崩溃恢复全链路：写入（未强制刷盘）→ 进程退出 → 重开 → 主数据与倒排均完好
        let dir = tmp();
        let cfg = cfg();
        {
            let mut e = Engine::open(&dir, &cfg).unwrap();
            e.put(1, b"doc-1".to_vec(), &["rust"]).unwrap();
            e.put(2, b"doc-2".to_vec(), &["go"]).unwrap();
            e.put(3, b"doc-3".to_vec(), &["rust", "async"]).unwrap();
            e.flush_inverted().unwrap();
        } // 模拟进程退出
        {
            let mut e2 = Engine::open(&dir, &cfg).unwrap();
            // 主数据（WAL 回放恢复）
            assert_eq!(e2.get(1).unwrap().unwrap(), b"doc-1");
            assert_eq!(e2.get(2).unwrap().unwrap(), b"doc-2");
            assert_eq!(e2.get(3).unwrap().unwrap(), b"doc-3");
            // 倒排跨重启可查（段文件 + Manifest）
            let rows = e2.search_term("rust").unwrap();
            let mut ids: Vec<u64> = rows.iter().map(|(d, _)| *d).collect();
            ids.sort();
            assert_eq!(ids, vec![1, 3]);
            assert_eq!(e2.search_term("go").unwrap().len(), 1);
        }
    }
}
