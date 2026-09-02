//! Indexer Node 查询加速代理层（P1，design_remain Indexer 行）：
//!
//! 用户排期：存算分离/Indexer Node——**先做查询代理层，不拆分存储，挂在现有节点下**
//! （复杂查询提速；彻底存算分离等 10 亿级规模）。本模块提供 **IndexerProxy**：
//! - **Indexer 侧**：独立倒排索引（`InvertedIndex`，只存 term→docid，不含文档）——倒排
//!   筛选/COUNT/聚合在索引侧快速完成，**不触达数据节点存储**；
//! - **回表**：命中 docid → 数据节点批量取文档（`fetch` 回调注入：生产 = RPC 批量读 /
//!   `batch_get`；测试 = 进程内 Engine）。
//!
//! 链路（与本地倒排等价性 demo indexer-node 验证）：`query(term)` = indexer.search →
//! 数据节点批量回表。独立节点化的收益在**多副本横向扩展**（索引副本独立扩展，不复制
//! 数据）；RPC 接线复用 gateway/meta（多副本规模落地）。

use crate::error::Result;
use crate::inverted::InvertedIndex;

/// Indexer 查询代理：独立倒排索引 + 回表接口抽象。
pub struct IndexerProxy {
    index: InvertedIndex,
}

impl IndexerProxy {
    /// 打开 Indexer（独立倒排目录）。
    pub fn open(dir: &std::path::Path) -> Result<Self> {
        Ok(Self {
            index: InvertedIndex::open(&dir.join("inverted"), 100_000)?,
        })
    }

    /// 索引文档（写路径同步调用：term → docid；数据节点写文档时同步建索引）。
    pub fn index(&mut self, term: &str, docid: u64) {
        self.index.add(term, docid);
    }

    /// 倒排查询（Indexer 侧，不触数据）：返回命中 docid（升序）。
    pub fn search(&self, term: &str) -> Result<Vec<u64>> {
        Ok(self
            .index
            .search(term)?
            .iter()
            .map(|d| d as u64)
            .collect())
    }

    /// COUNT（索引侧快速聚合，不落数据节点）。
    pub fn count(&self, term: &str) -> Result<u64> {
        self.index.doc_count(term)
    }

    /// 完整查询链路：Indexer 命中 docid → 回表回调批量取文档。
    /// `fetch` 由调用方注入（生产 = RPC 批量读数据节点；测试 = 进程内 batch_get）。
    pub fn query(
        &self,
        term: &str,
        fetch: impl Fn(&[u64]) -> Result<Vec<Option<Vec<u8>>>>,
    ) -> Result<(Vec<u64>, Vec<Option<Vec<u8>>>)> {
        let ids = self.search(term)?;
        let docs = fetch(&ids)?;
        Ok((ids, docs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::engine::Engine;

    #[test]
    fn indexer_proxy_matches_local_inverted_and_fetches() {
        // Indexer 代理链路：命中 docid 与数据节点本地倒排一致 + 回表批量取文档一致
        let dd = tempfile::tempdir().unwrap();
        let idir = tempfile::tempdir().unwrap();
        let mut data = Engine::open(dd.path(), &Config::default()).unwrap();
        let mut proxy = IndexerProxy::open(idir.path()).unwrap();

        let n = 300u64;
        for i in 0..n {
            let term = if i % 3 == 0 { "status=active" } else { "status=pending" };
            data.put(i, format!("{{\"id\":{i}}}").into_bytes(), &[term]).unwrap();
            if i % 3 == 0 {
                proxy.index("status=active", i);
            }
        }
        data.flush_inverted().unwrap();

        // 本地倒排对照
        let local: Vec<u64> = data
            .search_term_paged("status=active", None, 0)
            .unwrap()
            .rows
            .into_iter()
            .map(|(d, _)| d)
            .collect();
        // Indexer 代理查询（回表用 data.batch_get）
        let (ids, docs) = proxy
            .query("status=active", |ds| {
                data.batch_get(&ds.iter().map(|&d| d).collect::<Vec<_>>())
            })
            .unwrap();
        assert_eq!(local, ids, "Indexer 命中与本地倒排一致");
        assert_eq!(docs.iter().filter(|d| d.is_some()).count() as u64, n / 3);
        // COUNT 索引侧
        assert_eq!(proxy.count("status=active").unwrap(), n / 3);
        // 回表文档内容一致
        let first = docs.iter().flatten().next().unwrap();
        assert!(String::from_utf8(first.clone()).unwrap().contains("\"id\":0"));
    }

    #[test]
    fn indexer_count_does_not_touch_data() {
        // 复杂查询（COUNT）只碰 Indexer（独立索引），数据节点零查询——横向扩展查询负载基座
        let idir = tempfile::tempdir().unwrap();
        let mut proxy = IndexerProxy::open(idir.path()).unwrap();
        for i in 0..50u64 {
            proxy.index("city=beijing", i);
        }
        proxy.index.flush_segment().unwrap();
        for _ in 0..100 {
            assert_eq!(proxy.count("city=beijing").unwrap(), 50);
        }
        assert_eq!(proxy.search("city=beijing").unwrap().len(), 50);
    }
}
