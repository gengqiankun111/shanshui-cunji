//! 分片化倒排检索（10 亿库扩展阶段 B，design-10b-extension.md §6 阶段 B）。
//!
//! 倒排分片化设计：**分片内倒排存 local_id（u32 语义，Roaring 32-bit），跨分片广播时
//! 前缀组合成全局 docid**（`encode(shard_id, local_id)`）。理由：
//! - 每分片 1 亿 local << Roaring 上限（42.9 亿），分片内位图紧凑（K 项 v3 分块/G 项
//!   LRU+mmap/J 项后台 GC 全部保留，存储格式零改动）；
//! - 分片间 local 不重叠 → 全局 docid 天然唯一（广播合并无需去重）；
//! - 前缀保序 → 全局 docid 有序，分页窗口可直接跨分片定位（惰性，不整段合并全量）。
//!
//! 真实部署：每分片 Engine 倒排实现 `LocalInvertedSource`（local_id 存储），网关层
//! `ShardedInvertedSearch` 广播分页；本模块为纯逻辑（不碰 IO），进程内可单测。

use roaring::RoaringBitmap;

use crate::docid_alloc::encode;
use crate::error::{Error, Result};

/// 分片本地倒排源抽象：term → 本分片 local_id 位图（分片内 u32 语义）。
pub trait LocalInvertedSource: Send + Sync {
    /// 本分片命中 term 的 local_id 位图。
    fn search_local(&self, term: &str) -> Result<RoaringBitmap>;
    /// 本分片命中数。
    fn doc_count_local(&self, term: &str) -> Result<u64>;
}

/// 分片化倒排检索：N 分片本地检索 + 前缀组合全局 docid + 跨分片分页窗口。
pub struct ShardedInvertedSearch {
    shards: Vec<Box<dyn LocalInvertedSource>>,
}

impl ShardedInvertedSearch {
    /// 创建（至少 1 分片；真实部署为每分片 Engine 倒排源）。
    pub fn new(shards: Vec<Box<dyn LocalInvertedSource>>) -> Result<Self> {
        if shards.is_empty() {
            return Err(Error::Config("分片倒排源为空（需 ≥1 分片）".into()));
        }
        Ok(Self { shards })
    }

    pub fn shard_count(&self) -> u16 {
        self.shards.len() as u16
    }

    /// 分片内 local 位图 → 全局 docid 列表（前缀组合，保序）。
    pub fn chunk_to_global(shard_id: u16, locals: impl IntoIterator<Item = u32>) -> Vec<u64> {
        locals
            .into_iter()
            .map(|l| encode(shard_id, l as u64))
            .collect()
    }

    /// 全局命中数（跨分片求和）。
    pub fn doc_count_global(&self, term: &str) -> Result<u64> {
        let mut total = 0u64;
        for shard in &self.shards {
            total += shard.doc_count_local(term)?;
        }
        Ok(total)
    }

    /// 广播检索（全量合并）：各分片 local 位图 → 前缀组合 → 按序拼接。
    /// 分片间 local 不重叠 → 全局天然唯一（无需去重）。
    pub fn search_global(&self, term: &str) -> Result<Vec<u64>> {
        let mut out = Vec::new();
        for (sid, shard) in self.shards.iter().enumerate() {
            let locals = shard.search_local(term)?;
            out.extend(Self::chunk_to_global(sid as u16, locals.iter()));
        }
        Ok(out)
    }

    /// 跨分片分页检索（惰性窗口）：不整段合并全量，直接定位窗口所在分片。
    /// 返回 (全局总数, 窗口 docid)。local 升序 + 前缀保序 → 全局 docid 有序。
    pub fn search_paged(&self, term: &str, offset: u64, limit: u64) -> Result<(u64, Vec<u64>)> {
        // ① 各分片命中数（定位窗口用）
        let mut counts = Vec::with_capacity(self.shards.len());
        let mut total = 0u64;
        for shard in &self.shards {
            let c = shard.doc_count_local(term)?;
            counts.push(c);
            total += c;
        }
        if limit == 0 || offset >= total {
            return Ok((total, Vec::new()));
        }
        // ② 定位起始分片（累计跳过 offset）
        let mut start_shard = 0usize;
        let mut skip = offset;
        for (i, &c) in counts.iter().enumerate() {
            if skip < c {
                start_shard = i;
                break;
            }
            skip -= c;
        }
        // ③ 跨分片惰性窗口迭代
        let mut out = Vec::new();
        let mut remaining = limit;
        for sid in start_shard..self.shards.len() {
            if remaining == 0 {
                break;
            }
            let locals = self.shards[sid].search_local(term)?;
            let skip_in_shard = if sid == start_shard { skip as usize } else { 0 };
            let skip_in_shard = skip_in_shard.min(locals.len() as usize);
            let avail = locals.len() - skip_in_shard as u64;
            if avail == 0 {
                continue;
            }
            let take = remaining.min(avail) as usize;
            let win = locals.iter().skip(skip_in_shard).take(take);
            out.extend(Self::chunk_to_global(sid as u16, win));
            remaining -= take as u64;
        }
        Ok((total, out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// 内存倒排源（模拟单分片 Engine 倒排，local_id RoaringBitmap）。
    struct MemInvertedSource {
        index: HashMap<String, RoaringBitmap>,
    }
    impl MemInvertedSource {
        fn from_postings(term: &str, locals: &[u32]) -> Self {
            let mut index = HashMap::new();
            index.insert(term.to_string(), locals.iter().cloned().collect());
            Self { index }
        }
    }
    impl LocalInvertedSource for MemInvertedSource {
        fn search_local(&self, term: &str) -> Result<RoaringBitmap> {
            Ok(self.index.get(term).cloned().unwrap_or_default())
        }
        fn doc_count_local(&self, term: &str) -> Result<u64> {
            Ok(self.index.get(term).map(|b| b.len() as u64).unwrap_or(0))
        }
    }

    /// 4 分片 fixture：每分片 10000 local，term `city1` 命中 local%10==1（1000 个）。
    fn fixture() -> ShardedInvertedSearch {
        let mut shards: Vec<Box<dyn LocalInvertedSource>> = Vec::new();
        for _ in 0..4 {
            let locals: Vec<u32> = (0..10_000u32).filter(|l| l % 10 == 1).collect();
            shards.push(Box::new(MemInvertedSource::from_postings("city1", &locals)));
        }
        ShardedInvertedSearch::new(shards).unwrap()
    }

    #[test]
    fn broadcast_merge_unique_and_sorted() {
        let s = fixture();
        let g = s.search_global("city1").unwrap();
        assert_eq!(g.len(), 4000);
        let uniq: std::collections::HashSet<u64> = g.iter().cloned().collect();
        assert_eq!(uniq.len(), 4000, "广播合并无重复");
        assert!(g.windows(2).all(|w| w[0] < w[1]), "全局 docid 有序");
        assert_eq!(g[0], encode(0, 1));
        assert_eq!(g[1000], encode(1, 1));
    }

    #[test]
    fn paged_window_crosses_shard_boundary() {
        let s = fixture();
        let (total, page) = s.search_paged("city1", 800, 500).unwrap();
        assert_eq!(total, 4000);
        assert_eq!(page.len(), 500);
        assert_eq!(page[0], encode(0, 8001));
        assert_eq!(page[199], encode(0, 9991));
        assert_eq!(page[200], encode(1, 1), "窗口跨入分片 1");
        assert_eq!(page[499], encode(1, 2991));
    }

    #[test]
    fn paged_window_spanning_many_shards() {
        let s = fixture();
        let (total, page) = s.search_paged("city1", 0, 2500).unwrap();
        assert_eq!(total, 4000);
        assert_eq!(page.len(), 2500);
        assert_eq!(page[999], encode(0, 9991));
        assert_eq!(page[1000], encode(1, 1));
        assert_eq!(page[2499], encode(2, 4991));
    }

    #[test]
    fn paged_boundary_cases() {
        let s = fixture();
        let (total, empty) = s.search_paged("city1", 4000, 10).unwrap();
        assert_eq!(total, 4000);
        assert!(empty.is_empty());
        let (_, empty0) = s.search_paged("city1", 0, 0).unwrap();
        assert!(empty0.is_empty());
        let (_, tail) = s.search_paged("city1", 3990, 100).unwrap();
        assert_eq!(tail.len(), 10);
        assert_eq!(tail[0], encode(3, 9901));
        let (t0, p0) = s.search_paged("not-exist", 0, 10).unwrap();
        assert_eq!(t0, 0);
        assert!(p0.is_empty());
    }

    #[test]
    fn doc_count_global_sums_shards() {
        let s = fixture();
        assert_eq!(s.doc_count_global("city1").unwrap(), 4000);
        assert_eq!(s.doc_count_global("nope").unwrap(), 0);
    }

    #[test]
    fn empty_shards_rejected() {
        assert!(ShardedInvertedSearch::new(vec![]).is_err());
    }

    /// 真实 Engine 倒排源适配（分片内 local_id 语义；Engine 倒排存 local docid < u32::MAX）。
    struct EngineLocalInvertedSource {
        engine: std::sync::Mutex<crate::engine::Engine>,
    }
    impl LocalInvertedSource for EngineLocalInvertedSource {
        fn search_local(&self, term: &str) -> Result<RoaringBitmap> {
            // Engine 倒排现为 64 位（多表 docid）；本地分片模拟仅用 <2^32 局部 id → 截断兼容
            let p = self.engine.lock().unwrap().inverted_posting(term)?;
            let mut bm = RoaringBitmap::new();
            for d in p.iter() {
                bm.insert(d as u32);
            }
            Ok(bm)
        }
        fn doc_count_local(&self, term: &str) -> Result<u64> {
            Ok(self.engine.lock().unwrap().inverted_posting(term)?.len() as u64)
        }
    }

    #[test]
    fn real_engine_shards_combine_with_prefix() {
        // 阶段 B 全链路：真实 Engine 倒排存分片内 local_id + 前缀组合全局 docid。
        // 4 分片 × 10000 local（city1 命中 1000）→ 广播 4000 条唯一 + 分页跨分片窗口。
        let mut shards: Vec<Box<dyn LocalInvertedSource>> = Vec::new();
        for _ in 0..4 {
            let dir = tempfile::tempdir().unwrap();
            let cfg = crate::config::Config::default();
            let mut e = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
            for local in 0..10_000u64 {
                if local % 10 == 1 {
                    let doc = format!("{{\"city\":\"a\",\"local\":{local}}}").into_bytes();
                    let t: &[&str] = &["city1"];
                    e.put(local, doc, t).unwrap();
                }
            }
            shards.push(Box::new(EngineLocalInvertedSource {
                engine: std::sync::Mutex::new(e),
            }));
        }
        let s = ShardedInvertedSearch::new(shards).unwrap();
        let g = s.search_global("city1").unwrap();
        assert_eq!(g.len(), 4000, "真实 Engine 广播合并条数");
        let uniq: std::collections::HashSet<u64> = g.iter().cloned().collect();
        assert_eq!(uniq.len(), 4000);
        assert_eq!(g[0], encode(0, 1));
        assert_eq!(g[1000], encode(1, 1));
        let (total, page) = s.search_paged("city1", 800, 500).unwrap();
        assert_eq!(total, 4000);
        assert_eq!(page.len(), 500);
        assert_eq!(page[0], encode(0, 8001), "真实 Engine 分页窗口起点");
        assert_eq!(page[200], encode(1, 1), "真实 Engine 窗口跨分片");
    }
}
