//! stats：随 term 的数值统计（FieldAgg / stats_mem）、v5 段载荷解析合并与 GROUP BY 词典枚举统计。
//! 重构自 src/inverted.rs 对应主题，行为零变化。

use crate::error::Result;

use super::InvertedIndex;
use super::segment::parse_term_stats_at;


/// Ex-9.3：单个 stats 字段在某个 term 文档子集上的数值聚合。
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct FieldAgg {
    /// 计入条数（数值有效文档数）。
    pub n: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl FieldAgg {
    pub(super) fn new() -> Self {
        Self { n: 0, sum: 0.0, min: f64::INFINITY, max: f64::NEG_INFINITY }
    }
    pub(super) fn acc(&mut self, v: f64) {
        self.n += 1;
        self.sum += v;
        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }
    }
}

/// 将 `src` 合并进 `dst`（n/sum 累加；min/max 取跨集极值——跨段重复 docid 的 n/sum
/// 略高估属已文档化上界语义，见 development_remain §19）。
pub(super) fn merge_field_agg(dst: &mut FieldAgg, src: &FieldAgg) {
    if src.n == 0 {
        return;
    }
    dst.n += src.n;
    dst.sum += src.sum;
    if dst.n == src.n {
        // 首次（dst 此前空）
        dst.min = src.min;
        dst.max = src.max;
    } else {
        if src.min < dst.min {
            dst.min = src.min;
        }
        if src.max > dst.max {
            dst.max = src.max;
        }
    }
}

impl InvertedIndex {
    /// 第④步基础：按字段前缀枚举 distinct term 值（mem keys + 各段条目），返回
    /// 值升序（确定性）。仅用于**倒排 GROUP BY 词典枚举**类查询。
    fn field_term_values(&self, field: &str) -> Result<Vec<String>> {
        let prefix = format!("{field}=");
        let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for e in self.mem.iter() {
            let k = e.key();
            if let Some(rest) = k.strip_prefix(&prefix) {
                set.insert(rest.to_string());
            }
        }
        let segs = self.segments.load();
        for seg in segs.iter() {
            for (t, _) in self.read_segment_terms(seg)? {
                if let Some(rest) = t.strip_prefix(&prefix) {
                    set.insert(rest.to_string());
                }
            }
        }
        Ok(set.into_iter().collect())
    }

    /// 第④步：`GROUP BY <field>` 词典枚举聚合——对每个不同值给出该组 posting 行数
    /// 与（若声明 stats_fields）数值统计。不做任何文档值回表。缺该字段的文档（NULL 组）
    /// 不在此集合内（调用方按语义约束决定是否补偿/路由）。
    pub fn group_stats(
        &self,
        field: &str,
    ) -> Result<Vec<(String, u64, Vec<FieldAgg>)>> {
        let mut out = Vec::new();
        for value in self.field_term_values(field)? {
            let term = format!("{field}={value}");
            // 组行数：v4+ 段 doc_count 载荷求和优先；含老段(None) → 精确合并 posting 计数
            let count = match self.doc_count_fast(&term)? {
                Some(n) => n,
                None => self.search(&term)?.len() as u64,
            };
            let stats = self.term_stats(&term)?.unwrap_or_default();
            out.push((value, count, stats));
        }
        Ok(out)
    }

    /// 读取某 term 的数值统计（与 `stats_fields` 对齐）：内存累积 + 各 v5 段载荷合并。
    /// 无该 term/未配置 → Ok(None)。段格式 v4 及更早无载荷（跳过）。
    pub fn term_stats(&self, term: &str) -> Result<Option<Vec<FieldAgg>>> {
        let mut agg: Vec<FieldAgg> = self
            .stats_mem
            .get(term)
            .map(|e| e.value().clone())
            .unwrap_or_default();
        let segs = self.segments.load();
        for seg in segs.iter() {
            let Some((data, entry)) = self.segment_posting_entry(seg, term)? else {
                continue; // gc 并发删段：跳过（与 doc_count 一致）
            };
            let Some(seg_stats) = parse_term_stats_at(&data, entry, Self::seg_ver(&data))? else {
                continue; // v4 及更早 / 该 term 无载荷
            };
            if agg.len() < seg_stats.len() {
                agg.resize(seg_stats.len(), FieldAgg::new());
            }
            for (dst, src) in agg.iter_mut().zip(seg_stats.iter()) {
                merge_field_agg(dst, src);
            }
        }
        if agg.is_empty() {
            Ok(None)
        } else {
            Ok(Some(agg))
        }
    }
}
