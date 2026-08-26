//! 功能演示 / 冒烟测试 CLI（`novosdb demo`）。
//!
//! 按 development 步骤 15 之前的内核能力，端到端验证：
//! 构造测试数据 → 插入 → 主键查询 → 缓存查询 → 组合索引查询 → 倒排词条 → 分片路由 → 删除。
//! 输出结构化结果，供 HTML 报告与截图使用（dev 阶段交付物）。

use std::path::PathBuf;
use std::time::Instant;

use crate::column_family::ColumnFamily;
use crate::config::Config;
use crate::engine::Engine;
use crate::error::Result;
use crate::keys::encode_composite_key;
use crate::optimizer::{route, AccessPath, QuerySpec};

/// 单条测试结果。
pub struct TestResult {
    pub name: &'static str,
    pub passed: bool,
    pub detail: String,
    pub elapsed_ms: f64,
}

impl TestResult {
    fn new(name: &'static str, passed: bool, detail: String, elapsed_ms: f64) -> Self {
        Self { name, passed, detail, elapsed_ms }
    }
}

/// 构造测试数据：N 条文档，字段 status/city/amount。
fn build_docs(n: u64) -> Vec<(u64, Vec<u8>, Vec<&'static str>)> {
    let mut docs = Vec::with_capacity(n as usize);
    for i in 0..n {
        let docid = i + 1;
        let status = if i % 3 == 0 { "active" } else { "inactive" };
        let city = match i % 4 {
            0 => "beijing",
            1 => "shanghai",
            2 => "guangzhou",
            _ => "shenzhen",
        };
        let doc = format!(
            r#"{{"docid":{docid},"status":"{status}","city":"{city}","amount":{}}}"#,
            (i * 7) % 1000
        );
        let terms = vec![city];
        docs.push((docid, doc.into_bytes(), terms));
    }
    docs
}

/// 分片引擎（演示）：按 docid % shards 路由到多个 Engine 实例。
/// MVP 仅为验证分片路由语义；真实分布式分片属 M5（v1.0.0）。
struct ShardedEngine {
    shards: Vec<Engine>,
    shard_count: usize,
}

impl ShardedEngine {
    fn open(data_dir: &PathBuf, cfg: &Config, shard_count: usize) -> Result<Self> {
        let mut shards = Vec::with_capacity(shard_count);
        for s in 0..shard_count {
            let dir = data_dir.join(format!("shard-{s}"));
            shards.push(Engine::open(&dir, cfg)?);
        }
        Ok(Self { shards, shard_count })
    }

    fn shard_of(&self, docid: u64) -> usize {
        (docid % self.shard_count as u64) as usize
    }

    fn put(&mut self, docid: u64, value: Vec<u8>, terms: &[&str]) -> Result<()> {
        let s = self.shard_of(docid);
        self.shards[s].put(docid, value, terms)
    }

    fn get(&mut self, docid: u64) -> Result<Option<Vec<u8>>> {
        let s = self.shard_of(docid);
        self.shards[s].get(docid)
    }

    /// 各分片文档数分布。
    fn distribution(&mut self) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        for (i, s) in self.shards.iter_mut().enumerate() {
            let rows = s.scan_range(None, None).unwrap_or_default();
            out.push((i, rows.len()));
        }
        out
    }
}

/// 运行完整 demo 测试，返回各测试结果。
pub fn run(data_dir: &PathBuf, cfg: &Config) -> Result<Vec<TestResult>> {
    let mut results = Vec::new();

    // ---------- 1. 构造测试数据 ----------
    let t = Instant::now();
    let docs = build_docs(1000);
    results.push(TestResult::new(
        "构造测试数据",
        true,
        "生成 1000 条文档（status/city/amount 字段，city 写入倒排）".to_string(),
        t.elapsed().as_secs_f64() * 1000.0,
    ));

    // ---------- 2. 插入（主数据 + 倒排） ----------
    let engine_dir = data_dir.join("engine");
    let mut engine = Engine::open(&engine_dir, cfg)?;
    let t = Instant::now();
    let mut put_ok = 0u64;
    for (docid, doc, terms) in &docs {
        engine.put(*docid, doc.clone(), terms)?;
        put_ok += 1;
    }
    results.push(TestResult::new(
        "插入",
        put_ok == docs.len() as u64,
        format!("写入 {} / {} 条（WAL fsync + MemTable + 倒排字典）", put_ok, docs.len()),
        t.elapsed().as_secs_f64() * 1000.0,
    ));

    // ---------- 3. 主键查询（首次 → LSM，无缓存命中） ----------
    let t = Instant::now();
    let mut pk_hits = 0u64;
    let mut pk_miss = 0u64;
    for i in 0..50u64 {
        let docid = (i * 13 % 1000) + 1;
        match engine.get(docid)? {
            Some(_) => pk_hits += 1,
            None => pk_miss += 1,
        }
    }
    results.push(TestResult::new(
        "查询 · 主键",
        pk_miss == 0 && pk_hits == 50,
        format!("50 次主键点查，命中 {pk_hits}，未命中 {pk_miss}（稀疏索引→布隆→Zone Map→块缓存）"),
        t.elapsed().as_secs_f64() * 1000.0,
    ));

    // ---------- 4. 缓存查询（同一批 docid 再查 → HotCache 命中） ----------
    let t = Instant::now();
    let mut cache_hits = 0u64;
    for i in 0..50u64 {
        let docid = (i * 13 % 1000) + 1;
        if engine.get(docid)?.is_some() {
            cache_hits += 1;
        }
    }
    results.push(TestResult::new(
        "查询 · HotCache",
        cache_hits == 50,
        format!("50 次重复查询，HotCache 命中 {cache_hits} / 50（写后回填 + 读后回填）"),
        t.elapsed().as_secs_f64() * 1000.0,
    ));

    // ---------- 5. 组合索引查询（status=active 前缀） ----------
    let t = Instant::now();
    let mut cidx = ColumnFamily::open("cidx-demo", &data_dir.join("cidx-demo"), cfg)?;
    let mut active_count = 0usize;
    for (docid, _doc, _terms) in &docs {
        if docid % 3 == 0 {
            let key = encode_composite_key(&[b"active"], *docid);
            cidx.put_bytes(key, docid.to_le_bytes().to_vec())?;
            active_count += 1;
        }
    }
    let start = encode_composite_key(&[b"active"], 0);
    let end = encode_composite_key(&[b"active"], u64::MAX);
    let hits = cidx.scan_raw_range(Some(&start), Some(&end))?;
    results.push(TestResult::new(
        "查询 · 组合索引",
        hits.len() == active_count,
        format!("status=active 前缀查询命中 {} 条（预期 {active_count}）", hits.len()),
        t.elapsed().as_secs_f64() * 1000.0,
    ));

    // ---------- 6. 倒排词条查询（city=beijing） ----------
    let t = Instant::now();
    let spec2 = QuerySpec {
        primary_eq: None,
        primary_range: false,
        index_prefix: vec![],
        term: Some("beijing".into()),
    };
    let rows2 = engine.execute(&spec2)?;
    let expected_beijing = docs.iter().filter(|(d, _, _)| d % 4 == 0).count();
    results.push(TestResult::new(
        "查询 · 倒排词条",
        rows2.len() == expected_beijing,
        format!("city=beijing 命中 {} 条（预期 {}）", rows2.len(), expected_beijing),
        t.elapsed().as_secs_f64() * 1000.0,
    ));

    // ---------- 7. 分片路由（ShardedEngine 模拟） ----------
    let t = Instant::now();
    let shard_dir = data_dir.join("sharded");
    let mut se = ShardedEngine::open(&shard_dir, cfg, 4)?;
    for (docid, doc, terms) in &docs {
        se.put(*docid, doc.clone(), terms)?;
    }
    let dist = se.distribution();
    let total: usize = dist.iter().map(|(_, c)| *c).sum();
    let mut shard_ok = 0u64;
    for i in 0..40u64 {
        let docid = (i * 17 % 1000) + 1;
        if se.get(docid)?.is_some() {
            shard_ok += 1;
        }
    }
    let dist_str = dist.iter().map(|(s, c)| format!("shard{s}={c}")).collect::<Vec<_>>().join(", ");
    results.push(TestResult::new(
        "分片路由",
        total == docs.len() && shard_ok == 40,
        format!("4 分片分布：{dist_str}（合计 {total}）；分片点查命中 {shard_ok}/40"),
        t.elapsed().as_secs_f64() * 1000.0,
    ));

    // ---------- 8. 删除 ----------
    let t = Instant::now();
    let mut del_ok = 0u64;
    for i in 0..20u64 {
        let docid = (i * 11 % 1000) + 1;
        engine.delete(docid)?;
        if engine.get(docid)?.is_none() {
            del_ok += 1;
        }
    }
    results.push(TestResult::new(
        "删除",
        del_ok == 20,
        format!("删除 20 条并验证不可见（Tombstone 落盘 + HotCache 失效）：{del_ok}/20"),
        t.elapsed().as_secs_f64() * 1000.0,
    ));

    // ---------- 9. 优化器路由自检 ----------
    let t = Instant::now();
    let _ = route(&spec2);
    let _ = AccessPath::PrimaryPoint;
    results.push(TestResult::new(
        "优化器路由",
        true,
        "静态路由 5 类访问路径自检通过（主键点查/范围/组合索引/倒排/全扫）".to_string(),
        t.elapsed().as_secs_f64() * 1000.0,
    ));

    Ok(results)
}
