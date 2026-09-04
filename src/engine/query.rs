//! 文档引擎查询协调（reconstruct.md engine/query.rs）：倒排/组合索引查询与分页、
//! execute 路由执行、倒排攒批刷入/刷盘/GC、倒排统计/计数/代价估算入口。
//! 内容拆分自原 engine.rs（倒排/组合查询协调主题 impl 块）；私有 Engine 字段与
//! flush_inverted_pending 以 `pub(crate)` 提升访问。

use std::sync::atomic::Ordering;

use roaring::treemap::RoaringTreemap;

use crate::engine::{Engine, PagedRows, QueryRow};
use crate::error::Result;
use crate::optimizer::{route, AccessPath, QuerySpec};


impl Engine {
    /// 倒排词条查询：合并 posting（RoaringBitmap）→ 回表取文档。
    pub fn search_term(&mut self, term: &str) -> Result<Vec<QueryRow>> {
        Ok(self.search_term_paged(term, None, 0)?.rows)
    }

    /// 倒排词条分页查询（M8-P8）：bitmap 迭代 docid 天然升序，skip(offset) 后只回表 limit 行。
    /// `total` = 全量命中数（bitmap.len() O(1)）；limit=None 取全部（兼容非分页调用）。
    pub fn search_term_paged(
        &mut self,
        term: &str,
        limit: Option<u64>,
        offset: u64,
    ) -> Result<PagedRows> {
        // Ex-5.3：查询前刷入攒批缓冲，保证 put 后未达阈值的数据立即可查（一致性）
        self.flush_inverted_pending();
        // K 项（7.74）：v3 分块快速路径——只解码 [offset, offset+limit) 覆盖的容器
        // （大 posting 近页从全量反序列化降至窗口解码，x211）；total 来自容器头基数。
        let (total, ids) = self
            .inverted
            .search_paged(term, offset, limit.unwrap_or(u64::MAX))?;
        let mut rows = Vec::new();
        // N 项：收集可见窗口 docid（bitmap 升序）→ 一次 `batch_get` 批量回表。
        let vals = self.batch_get(&ids.iter().map(|&d| d as u64).collect::<Vec<_>>())?;
        for (d, v) in ids.into_iter().zip(vals) {
            if let Some(v) = v {
                rows.push((d as u64, v));
            }
        }
        Ok(PagedRows { total, rows })
    }

    /// fulltext 分词检索（M8-P7）：按字段 + 关键词构造词 term `ft:{field}:{word}` 查询
    /// （词 term 由 fulltext_fields 声明字段分词生成）。命中 posting 合并 → 回表取文档。
    pub fn fulltext_search(&mut self, field: &str, word: &str) -> Result<Vec<QueryRow>> {
        self.fulltext_search_paged(field, word, None, 0).map(|p| p.rows)
    }

    /// fulltext 分词检索分页（M8-P8）：同 `search_term_paged` 语义（构造 `ft:{field}:{word}`）。
    pub fn fulltext_search_paged(
        &mut self,
        field: &str,
        word: &str,
        limit: Option<u64>,
        offset: u64,
    ) -> Result<PagedRows> {
        self.search_term_paged(&format!("ft:{field}:{word}"), limit, offset)
    }

    /// fulltext 分词字段集合（M8-P7）：供 term 提取层判断字段是否走分词索引。
    pub fn fulltext_fields(&self) -> &std::collections::HashSet<String> {
        &self.fulltext_fields
    }

    /// 中文分词器开关（M8-P13）：true = jieba 完整词典分词，false = bigram。
    pub fn use_jieba(&self) -> bool {
        self.use_jieba
    }

    /// Ex-9.3：读取某 term 的数值统计（内存累积 + v5 段载荷；与 `stats_fields` 对齐）。
    /// 未配置 stats_fields / 无该 term → None。
    pub fn inverted_term_stats(&self, term: &str) -> Option<Vec<crate::inverted::FieldAgg>> {
        self.inverted.term_stats(term).ok().flatten()
    }

    /// Ex-9.3：字段是否声明为倒排统计载荷字段（返回其在 stats_fields 中的位序，
    /// 用于定位 term 统计的对应聚合列）。
    pub fn stats_field_pos(&self, field: &str) -> Option<usize> {
        self.stats_fields.iter().position(|x| x == field)
    }

    /// Ex-9.3 第④步：倒排词典枚举 `GROUP BY <field>` 聚合行（值, 组行数, 数值统计）。
    /// 仅含已索引的组值；缺字段文档（NULL 组）由调用方按语义约束处理。
    pub fn inverted_group_stats(
        &self,
        field: &str,
    ) -> crate::error::Result<Vec<(String, u64, Vec<crate::inverted::FieldAgg>)>> {
        self.inverted.group_stats(field)
    }

    /// 组合索引前缀查询：编码前缀键范围扫描 → 回表主数据。
    pub fn query_by_composite_prefix(&self, fields: &[&[u8]]) -> Result<Vec<QueryRow>> {
        let Some(cidx) = self.cidx.clone() else {
            return Ok(Vec::new());
        };
        let start = crate::keys::encode_composite_key(fields, 0);
        let end = crate::keys::encode_composite_key(fields, u64::MAX);
        let hits = cidx.scan_raw_range(Some(&start), Some(&end))?;
        let mut out = Vec::new();
        // P0-A：多个 composite_indexes 可能共享前缀（如 [status] 和 [status,region]），
        // 前缀扫描会命中多个索引的条目（同一 docid 多次出现），须去重。
        let mut seen = std::collections::HashSet::new();
        for (key, _) in hits {
            let (_fields, docid) = crate::keys::decode_composite_key(&key)?;
            if !seen.insert(docid) {
                continue;
            }
            if let Some(v) = self.get(docid)? {
                out.push((docid, v));
            }
        }
        Ok(out)
    }

    /// P92/#31：单列组合索引**范围**路由——首字段值 ∈ [low, high]（编码字节序区间）→
    /// cidx 范围扫描 + 回表。仅当 `composite_indexes` 声明了单列 `[field]` 时使用
    /// （前缀等值走 `query_by_composite_prefix`）。字段值以十进制/字符串字节落 cidx，
    /// 等宽数值序 = 字节序；边界误命中由调用方对回表行复筛 BETWEEN 兜底（见
    /// `try_composite_index`），语义与全扫 `BETWEEN` 精确一致。
    pub fn query_by_composite_range(
        &self,
        field: &str,
        low: &str,
        high: &str,
    ) -> Result<Vec<QueryRow>> {
        let Some(cidx) = self.cidx.clone() else {
            return Ok(Vec::new());
        };
        let start = crate::keys::encode_composite_key(&[low.as_bytes()], 0);
        let end = crate::keys::encode_composite_key(&[high.as_bytes()], u64::MAX);
        let hits = cidx.scan_raw_range(Some(&start), Some(&end))?;
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for (key, _) in hits {
            let (_fields, docid) = crate::keys::decode_composite_key(&key)?;
            if !seen.insert(docid) {
                continue;
            }
            if let Some(v) = self.get(docid)? {
                out.push((docid, v));
            }
        }
        Ok(out)
    }

    /// 查询执行器：按 QuerySpec 静态路由到访问路径并执行（design 7.1 最小集枚举）。
    /// 看门狗：查询超时熔断（逐行检查 QueryGuard，超时返回 QueryTooExpensive）。
    pub fn execute(&mut self, spec: &QuerySpec) -> Result<Vec<QueryRow>> {
        // P52：CPU 并发限制（超限返回 Stalled）+ 查询超时熔断
        let guard = self.watchdog.try_begin_query()?;
        // Ex-5.3：倒排查询前刷入攒批缓冲（Inverted 分支可能命中 pending 中的 term）
        self.flush_inverted_pending();
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

    /// 倒排内存累积条数（供后台刷盘决策；含攒批缓冲，Ex-5.3）。
    pub fn inverted_mem_docids(&self) -> u64 {
        self.inverted.mem_docids() + self.pending_inverted.lock().unwrap().len() as u64
    }

    // ============ 10 亿库阶段 D：分片级可观测 ============

    /// 将倒排攒批缓冲一次性刷入内存字典（Ex-5.3 批处理）。
    /// 低基数 term 跨行聚合：一组 (term, docid) 按 term 分组合并，每 term 一次锁操作。
    /// 崩溃安全：WAL 回放重新走 put 重建倒排，缓冲丢失不丢数据。
    /// O 项第②步：`&self`（pending_inverted 内部 Mutex，读路径查询前也可刷缓冲）。
    pub(crate) fn flush_inverted_pending(&self) {
        let items: Vec<(String, u64)> = std::mem::take(&mut *self.pending_inverted.lock().unwrap());
        if items.is_empty() {
            return;
        }
        let refs: Vec<(&str, u64)> = items.iter().map(|(t, d)| (t.as_str(), *d)).collect();
        self.inverted.add_batch(&refs);
    }

    /// 强制倒排刷盘（先刷入攒批缓冲，再整段落盘）。
    /// J 项（7.73）：刷盘后检测段总量超 GC 阈值 → 置后台 GC 信号（mysql worker 消费；
    /// 无 worker 时由显式 `inverted_gc` / 兜底周期消费）。
    pub fn flush_inverted(&self) -> Result<()> {
        self.flush_inverted_pending();
        self.inverted.flush_segment()?;
        // J 项：段数/大小超阈值 → 后台 GC 信号（避免段数爆炸放大查询延迟）
        if self.inverted.should_gc() {
            self.inverted_gc_pending.store(true, Ordering::Release);
        }
        Ok(())
    }

    /// 倒排段 GC 合并（design 5.2.2/5.2.4⑤）：段文件总量超阈值时合并为少量大段。
    /// 大数据量导入后段数可能爆炸（demo 每 100 万 term 对刷一段 → 5000 万库数百段），
    /// 查询每次遍历全部段（高频 term 每段反序列化 posting）→ 段数直接放大查询延迟。
    /// J 项（7.73）：改 `&self`（inverted 内部 mutate 锁与 flush 互斥）——批量导入后
    /// 由后台 GC worker 自动周期触发（写路径刷盘置信号），显式调用仍可用。
    pub fn inverted_gc(&self) -> Result<crate::inverted::GcReport> {
        self.flush_inverted_pending();
        self.inverted.gc()
    }

    /// 查询看门狗守卫（类 SQL 扫描过滤/回表熔断用）：`is_expired()` 超时后返回
    /// QueryTooExpensive（复用 engine.execute 的查询超时机制）。
    pub fn query_guard(&self) -> crate::watchdog::QueryGuard {
        self.watchdog.begin_query()
    }

    /// 写入 Enrich 配置（design 19 / development 5.21）：Some((fail_policy, from_field,
    /// to_field)) = 启用 local 数据源预连接（server /put 走 join::put_with_enrich）；None = 关闭。
    pub fn enrich_config(&self) -> Option<(&str, &str, &str)> {
        self.enrich
            .as_ref()
            .map(|(f, a, b)| (f.as_str(), a.as_str(), b.as_str()))
    }

    /// 倒排某词条命中的 docid 集合（不回表，供测试/监控/sqlish 等值筛选；64 位 docid）。
    /// O 项第②步：读路径 `&self`（查询前刷入攒批缓冲保证一致性）。
    pub fn inverted_posting(&self, term: &str) -> Result<RoaringTreemap> {
        self.flush_inverted_pending();
        self.inverted.search(term)
    }

    /// 倒排某词条命中的文档数（COUNT 聚合，<0.1ms）。
    /// 位图索引快速路径（design 5.2.4，M7-2）：term 命中 `bitmap_fields` 白名单 → 内存位图计数；
    /// 否则回退倒排段扫描。
    pub fn inverted_doc_count(&mut self, term: &str) -> Result<u64> {
        self.flush_inverted_pending();
        // Ex-9.1b：段级 TermMeta 计数载荷求和（全 v4 段亚毫秒，恒含存量）；老段回退精确遍历
        if let Some(n) = self.inverted.doc_count_fast(term)? {
            return Ok(n);
        }
        self.inverted.doc_count(term)
    }

    /// P4-C：基于代价的动态路由。返回最优访问路径与代价估算。
    /// 使用配置中的代价模型参数，结合倒排 doc_count 统计。
    pub fn cost_route(&self, spec: &crate::optimizer::QuerySpec) -> crate::optimizer::CostEstimate {
        let total_rows = self.estimated_total_rows();
        let zone_fields: Vec<String> = Vec::new(); // 后续可扩展从 SST 元数据获取
        crate::optimizer::cost_route(
            spec,
            &self.cost_params,
            &|term| self.inverted.doc_count_fast(term).ok().flatten(),
            total_rows,
            &zone_fields,
        )
    }

    /// P4-C：估算表总行数（基于 max_docid 或 inverted 段总数）。
    /// 精确值由 `count_all_docs` 提供（当前是 O(N) 扫描，暂不用于热路径）。
    pub fn estimated_total_rows(&self) -> u64 {
        // 使用 max_docid 作为上界（最接近实际行数）
        let max_docid = self.max_docid.load(Ordering::Relaxed);
        if max_docid > 0 {
            max_docid
        } else {
            // 回退：从 inverted 段总量估算
            self.inverted.mem_docids() + 1000
        }
    }

    /// 按字段前缀分组（GROUP BY 聚合）：返回 `field=value` 各分组的文档数。
    /// 位图索引快速路径（M7-2）：字段命中白名单 → 内存位图分组；否则回退倒排段扫描。
    pub fn inverted_group_by(&mut self, field: &str) -> Result<Vec<(String, u64)>> {
        self.flush_inverted_pending();
        if let Some(rows) = self.inverted.bitmap_group_by(field) {
            return Ok(rows);
        }
        self.inverted.group_by(field)
    }

    /// 内存位图组合 AND 计数（design 5.2.4，M7-2）：全部 term 命中白名单 → 交集计数（亚毫秒）；
    /// 否则返回 None（调用方回退逐词条倒排查询）。
    pub fn inverted_bitmap_and_count(&mut self, terms: &[&str]) -> Option<u64> {
        self.flush_inverted_pending();
        self.inverted.bitmap_and(terms).map(|b| b.len())
    }

}
