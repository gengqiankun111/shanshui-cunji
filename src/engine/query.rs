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
    /// 
    /// B2（Ex-9.3⑤ 默认化，2026-09-07）：`stats_fields` 为空时自动检查自动检测集合。
    pub fn stats_field_pos(&self, field: &str) -> Option<usize> {
        // 先查显式配置
        if let Some(pos) = self.stats_fields.iter().position(|x| x == field) {
            return Some(pos);
        }
        // 显式配置非空 → 不自动追加（用户声明覆盖默认化）
        if !self.stats_fields.is_empty() {
            return None;
        }
        // 默认化：查自动检测集合
        let auto = self.auto_stats_fields.lock().ok()?;
        auto.iter().position(|x| x == field).map(|idx| self.stats_fields.len() + idx)
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

    /// Task-028：cidx 存量补齐（open 期调用）。声明了 `composite_indexes` 但 cidx 与 primary
    /// 不一致（配置后加 / 旧库无 cidx / 崩溃丢键 / 索引字段变更）时，cidx 前缀查询会静默返回
    /// 空/残缺结果——本方法从 primary 全量回扫重建，flush 落 SST 后写 `cidx.sig` 签名标记。
    ///
    /// 判定（幂等、重启安全）：
    /// - 未声明索引 / cidx 不可用（None）→ no-op；
    /// - primary 为空 → 空库即同步（补写标记）；
    /// - `cidx.sig` 标记签名 ≠ 当前声明签名 或 cidx 无任何条目（SST+memtable 皆空）→ 重建；
    /// - 其余（标记一致且 cidx 非空，正常会话/队列 WAL 恢复）→ 零开销跳过。
    pub(crate) fn ensure_composite_index_backfill(&self) -> Result<()> {
        let indexes = &self.composite_indexes;
        if indexes.is_empty() {
            return Ok(());
        }
        let Some(cidx) = &self.cidx else { return Ok(()) };
        let sig = composite_index_sig(indexes);
        let marker = self.data_dir.join("cidx.sig");
        let cur = std::fs::read_to_string(&marker).unwrap_or_default();
        if self.primary.data_empty() {
            // 空库：cidx 空 = 同步；补写标记供后续签名比对
            if cur != sig {
                write_sig_marker(&marker, &sig)?;
            }
            return Ok(());
        }
        let cidx_empty = cidx.sst_count() == 0 && cidx.memtable_bytes() == 0;
        if cur == sig && !cidx_empty {
            return Ok(()); // 正常：标记一致且已有条目（含队列 WAL 恢复路径）
        }
        // —— 重建：primary 存量 → 提取各声明索引组字段值 → 复合键入 cidx（无 WAL，批量）——
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(8192);
        self.primary.scan_stream(None, None, |key, val| {
            let docid = crate::keys::decode_docid(key).map_err(|_| {
                crate::error::Error::Corrupted("cidx 重建扫描 key 非 docid 编码".into())
            })?;
            if self
                .deletion_bitmap
                .as_ref()
                .map(|b| b.is_deleted(docid))
                .unwrap_or(false)
            {
                return Ok(true); // 删除位图已删：不入索引（重建后查询 get 亦跳过）
            }
            let Ok(vobj) = serde_json::from_slice::<serde_json::Value>(val) else {
                return Ok(true); // 非 JSON 原始字节文档：无字段可取，与写路径语义一致
            };
            for fields in indexes {
                let mut field_vals: Vec<Vec<u8>> = Vec::with_capacity(fields.len());
                let mut all_present = true;
                for f in fields {
                    match vobj.get(f) {
                        Some(serde_json::Value::String(s)) => {
                            field_vals.push(s.as_bytes().to_vec())
                        }
                        Some(serde_json::Value::Number(n)) => {
                            field_vals.push(n.to_string().into_bytes())
                        }
                        Some(serde_json::Value::Bool(b)) => field_vals.push(
                            if *b { b"true".to_vec() } else { b"false".to_vec() },
                        ),
                        _ => {
                            all_present = false;
                            break;
                        }
                    }
                }
                if all_present {
                    let refs: Vec<&[u8]> =
                        field_vals.iter().map(|v| v.as_slice()).collect();
                    keys.push(crate::keys::encode_composite_key(&refs, docid));
                }
            }
            if keys.len() >= 65_536 {
                for k in keys.drain(..) {
                    cidx.memtable_put_nolog(k, Vec::new());
                }
            }
            Ok(true)
        })?;
        for k in keys {
            cidx.memtable_put_nolog(k, Vec::new());
        }
        // 持久化：cidx memtable 统一落 SST → 写签名标记（标记在 flush 之后写，崩溃丢标记即重做）
        cidx.switch_and_flush()?;
        write_sig_marker(&marker, &sig)?;
        Ok(())
    }

    /// Task-030 残余项（10w 轮 #61：`COUNT(DISTINCT status)` 65.49ms = 344.7× MySQL 0.19ms）：
    /// 低基数字段 COUNT(DISTINCT) 词典快路径。字段命中**内存位图白名单**（`bitmap_fields`）时
    /// 值 → docid 位图常驻内存：逐值判定「窗口 [start,end] ∩ 活跃 docid 集（`live_docids`，
    /// 删除位图/墓碑口径与权威窗口扫描一致）非空」即计 1 个 distinct 值 → O(组数)，枚举级
    /// 低基数字段亚毫秒；组数超上限（高基数，如 user_id）或字段非白名单 → None（调用方回退
    /// 权威窗口扫描）。已知边界（与引擎倒排既有语义一致）：白名单位图仅追加不摘除（同 docid
    /// 覆盖换值/复活换值留陈旧 docid）→ 存在值变更时可能多计陈旧值，与 `COUNT(*) WHERE f='v'`
    /// 倒排计数同类口径偏差；宽表负载（同值追加 + 更新非分组字段）不触发，扫描路径恒为精确兜底。
    pub fn count_distinct_fast(
        &self,
        field: &str,
        start: Option<u64>,
        end: Option<u64>,
    ) -> Result<Option<u64>> {
        const MAX_DISTINCT_GROUPS: usize = 512;
        if !self.inverted.is_bitmap_field(field) {
            return Ok(None);
        }
        self.flush_inverted_pending(); // 与 inverted_posting 同口径：刷入攒批保证可见最新
        let groups = match self.inverted.bitmap_field_snapshot(field, MAX_DISTINCT_GROUPS) {
            Some(g) => g,
            None => return Ok(None),
        };
        if groups.is_empty() {
            return Ok(Some(0));
        }
        self.live_ensure()?;
        // 活跃集快照后释放锁（位图已克隆，判活迭代在锁外，避免与写路径嵌套锁序）
        let live = self.live_docids.lock().unwrap().clone().unwrap_or_default();
        let hits = |d: u64| {
            live.contains(d)
                && start.map_or(true, |s| d >= s)
                && end.map_or(true, |e| d <= e)
        };
        let mut n = 0u64;
        for (_value, posting) in &groups {
            if posting.iter().any(|d| hits(d)) {
                n += 1;
            }
        }
        Ok(Some(n))
    }

    /// P-GB（2026-09-05）：窗口位图**分组计数**——`GROUP BY <白名单字段[,…]>` 的词典快路径。
    /// 组计数 = 各分组字段值位图 AND（多字段组合）∩「窗口 [start,end] ∩ 活跃 docid 集 ∩（可选）
    /// 候选词条 posting」的长度；活跃集（`live_docids`）口径 = 权威扫描（删除位图/墓碑隐藏已删、
    /// 复活重计），跨表 docid 由窗口排外；`cand_term`（如 `status=active`，WHERE 单等值）非 None 时
    /// 额外以该 posting 收敛（命中集内分组，行级过滤语义=仅含命中行）。
    /// 字段数 1..=2（防笛卡尔爆炸）、全部 ∈ `bitmap_fields`、任一组值数超上限 → None（调用方回退扫描）。
    /// 位图快照克隆后锁外计数（读路径不持倒排锁）。
    /// 返回 `Some((组, 窗口活跃匹配数))`——组 = (字段值序列, 计数)（计数 >0，未排序）；匹配数 =
    /// 「窗口 ∩ 活跃 ∩（候选）」的行数（无候选 = 窗口活跃行数；Σ组 ≤ 匹配数，调用方以此判 NULL 组/
    /// 陈旧放大回退）。已知边界（同 `count_distinct_fast`/P118）：白名单位图仅追加不摘除（同 docid
    /// 换值留陈旧）→ 值变更场景组计数可能偏高——调用方以 Σcounts 与匹配数核对，不等即回退扫描保精确。
    pub fn group_by_bitmap_window(
        &self,
        fields: &[String],
        cand_term: Option<&str>,
        start: Option<u64>,
        end: Option<u64>,
    ) -> Result<Option<(Vec<(Vec<String>, u64)>, u64)>> {
        const MAX_VALUES: usize = 512; // 单字段值数上限（枚举级；user_id 类高基数回退扫描）
        const MAX_COMBOS: usize = 4096; // 两字段组合数上限（防笛卡尔爆炸）
        if fields.is_empty() || fields.len() > 2 {
            return Ok(None);
        }
        if fields.iter().any(|f| !self.inverted.is_bitmap_field(f)) {
            return Ok(None);
        }
        self.flush_inverted_pending(); // 与 inverted_posting 同口径：刷入攒批保证可见最新
        let cand: Option<roaring::treemap::RoaringTreemap> = match cand_term {
            Some(t) => Some(self.inverted.search(t)?),
            None => None,
        };
        let mut maps = Vec::with_capacity(fields.len());
        for f in fields {
            match self.inverted.bitmap_field_snapshot(f, MAX_VALUES) {
                Some(m) => maps.push(m),
                None => return Ok(None), // 非白名单 / 组数超上限
            }
        }
        self.live_ensure()?;
        // 活跃集快照后释放锁（位图已克隆，计数迭代在锁外，避免与写路径嵌套锁序）
        let live = self.live_docids.lock().unwrap().clone().unwrap_or_default();
        let hits = |d: u64| {
            live.contains(d)
                && start.map_or(true, |s| d >= s)
                && end.map_or(true, |e| d <= e)
                && cand.as_ref().map_or(true, |c| c.contains(d))
        };
        let count_live =
            |posting: &roaring::treemap::RoaringTreemap| -> u64 {
                let mut n = 0u64;
                for d in posting.iter() {
                    if hits(d) {
                        n += 1;
                    }
                }
                n
            };
        // 窗口活跃匹配数（无候选 = 全活跃窗口行数）
        let live_match: u64 = match &cand {
            Some(c) => count_live(c),
            None => {
                let s = start.unwrap_or(0);
                let e = end.unwrap_or(u64::MAX);
                let before = if s == 0 { 0 } else { live.rank(s - 1) };
                live.rank(e) - before
            }
        };
        if fields.len() == 1 {
            let mut out = Vec::with_capacity(maps[0].len());
            for (v, p) in &maps[0] {
                let c = count_live(p);
                if c > 0 {
                    out.push((vec![v.clone()], c));
                }
            }
            return Ok(Some((out, live_match)));
        }
        // 两字段组合：迭代较小 posting 判对方/活跃/窗口/候选成员（免笛卡尔 AND 物化）
        let mut out = Vec::new();
        for (v1, p1) in &maps[0] {
            for (v2, p2) in &maps[1] {
                if out.len() >= MAX_COMBOS {
                    return Ok(None);
                }
                let (iter_p, other_p) = if p1.len() <= p2.len() { (p1, p2) } else { (p2, p1) };
                let mut c = 0u64;
                for d in iter_p.iter() {
                    if other_p.contains(d) && hits(d) {
                        c += 1;
                    }
                }
                if c > 0 {
                    out.push((vec![v1.clone(), v2.clone()], c));
                }
            }
        }
        Ok(Some((out, live_match)))
    }

}

/// 组合索引声明签名：字段组按 `.` 连接、组间按 `|` 连接（配置变更 → 签名变 → 触发重建）。
fn composite_index_sig(indexes: &[Vec<String>]) -> String {
    indexes
        .iter()
        .map(|v| v.join("."))
        .collect::<Vec<_>>()
        .join("|")
}

/// 原子写签名标记（tmp + rename）。
fn write_sig_marker(path: &std::path::Path, sig: &str) -> Result<()> {
    let tmp = path.with_extension("sig.tmp");
    std::fs::write(&tmp, sig)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}
