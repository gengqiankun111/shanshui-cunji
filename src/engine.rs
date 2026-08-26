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
use crate::optimizer::{route, AccessPath, QuerySpec};

/// 引擎：组合主数据 + 组合索引 + 倒排 + HotCache。
pub struct Engine {
    /// 主数据列族（value = 序列化文档字节）。
    primary: ColumnFamily,
    /// 组合索引列族（key = encode_composite_key）。
    cidx: Option<ColumnFamily>,
    /// 倒排索引。
    inverted: InvertedIndex,
    /// 文档热缓存。
    hotcache: HotCache,
}

/// 查询结果行：docid + 文档字节。
pub type QueryRow = (u64, Vec<u8>);

impl Engine {
    /// 打开（或创建）引擎。倒排刷盘阈值取自内存预算的比例（MVP 固定 1M posting）。
    pub fn open(data_dir: &Path, cfg: &Config) -> Result<Self> {
        let primary = ColumnFamily::open("primary", &data_dir.join("primary"), cfg)?;
        let inverted = InvertedIndex::open(&data_dir.join("inverted"), 1_000_000)?;
        let cidx = ColumnFamily::open("cidx", &data_dir.join("cidx"), cfg).ok();
        let hotcache = HotCache::new(cfg.hotcache.clone());
        Ok(Self { primary, cidx, inverted, hotcache })
    }

    /// 写入文档（docid + 序列化字节 + 该文档涉及的倒排词条）。
    /// 写失效链：先失效 HotCache 与组合索引旧条目，最后写 LSM（design 6.6）。
    pub fn put(&mut self, docid: u64, value: Vec<u8>, terms: &[&str]) -> Result<()> {
        // ① 失效 HotCache 该 docid
        self.hotcache.invalidate(docid);
        // ② 主数据（权威源）
        self.primary.put(docid, value.clone())?;
        // ③ 倒排（内存字典累积，达阈值由调用方/后台刷盘）
        for t in terms {
            self.inverted.add(t, docid);
        }
        // ④ 回填 HotCache（写后回填，供热点查询亚毫秒命中）
        self.hotcache.put(docid, value);
        Ok(())
    }

    /// 删除文档：主数据 Tombstone + 失效 HotCache（倒排残留 docid 由回表过滤）。
    pub fn delete(&mut self, docid: u64) -> Result<()> {
        self.hotcache.invalidate(docid);
        self.primary.delete(docid)?;
        Ok(())
    }

    /// 点查文档：HotCache 命中直达，否则主数据 LSM。
    pub fn get(&mut self, docid: u64) -> Result<Option<Vec<u8>>> {
        if let Some(v) = self.hotcache.get(docid) {
            return Ok(Some(v));
        }
        let found = self.primary.get(docid)?;
        if let Some((v, _)) = found {
            self.hotcache.put(docid, v.clone());
            return Ok(Some(v));
        }
        Ok(None)
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
    pub fn execute(&mut self, spec: &QuerySpec) -> Result<Vec<QueryRow>> {
        match route(spec) {
            AccessPath::PrimaryPoint => {
                let docid = spec.primary_eq.as_ref().map(|k| crate::keys::decode_docid(k)).transpose()?.unwrap_or(0);
                Ok(self.get(docid)?.into_iter().map(|v| (docid, v)).collect())
            }
            AccessPath::PrimaryRange => self.scan_range(None, None),
            AccessPath::CompositeIndex { fields } => {
                let fs: Vec<&[u8]> = fields.iter().map(|s| s.as_bytes()).collect();
                self.query_by_composite_prefix(&fs)
            }
            AccessPath::Inverted { term } => self.search_term(&term),
            AccessPath::FullScan => self.scan_range(None, None),
        }
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
        DIR.get_or_init(|| tempfile::tempdir().unwrap()).path().join(name)
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
}
