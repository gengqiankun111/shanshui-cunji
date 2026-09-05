//! 分组聚合执行（原 sqlish.rs AF#2~#5/Ex-9.3 ④b 段）：`execute_group_by_window`/
//! `execute_group_by` → `GroupResult`/`GroupRow`（组键升序 + ORDER BY + HAVING 过滤 +
//! LIMIT/OFFSET 切片）；无 WHERE 单字段倒排词典枚举快路径 `group_by_fast_inverted`。
//! `aggregate_needed_fields`/`fmt_num` 复用 aggregate.rs；行级判定基建在 eval.rs。

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::sql::parser::{parse_select, CmpOp, HavingExpr, Select, WhereExpr};
use serde_json::Value;

use super::aggregate::{aggregate_needed_fields, fmt_num};
use super::eval::{
    field_of, light_top_field, light_where_matches, subset_doc_bytes, LightVal,
};

/// P91：GROUP BY 全扫所需列 = WHERE 引用 ∪ 分组列 ∪ 聚合列（顶层键、去重）。
/// 供 `scan_stream_fields` 投影解码（PAX 块只解这些列；消费端也只读这些列）。
fn group_scan_needed_fields(
    where_expr: Option<&WhereExpr>,
    group_fields: &[String],
    specs: &[(String, Option<String>)],
) -> Vec<String> {
    let mut out = aggregate_needed_fields(where_expr, None);
    let mut push = |s: &str| {
        let top = s.split('.').next().unwrap_or(s);
        if !top.is_empty() && !out.iter().any(|x| x == top) {
            out.push(top.to_string());
        }
    };
    for f in group_fields {
        push(f);
    }
    for (_n, f) in specs {
        if let Some(f) = f {
            push(f);
        }
    }
    out
}


// ---------------------------------------------------------------------------
// GROUP BY（开发顺序 AF#2 单字段 COUNT/SUM → AF#4 多字段 + 常用聚合）
// ---------------------------------------------------------------------------

/// GROUP BY 结果集：选中分组列 + 聚合列（AF#2 单字段 → AF#4 多字段/AVG/MIN/MAX）。
#[derive(Debug, Clone)]
pub struct GroupResult {
    /// 全部分组字段（层级顺序；`ORDER BY`/键位映射按此下标）。
    pub group_fields: Vec<String>,
    /// 结果集分组列头（选中普通列，select 顺序；值位序 = 在 `group_fields` 中的下标）。
    pub group_cols: Vec<String>,
    /// 聚合列头（如 `COUNT(*)` / `AVG(amount)`）。
    pub headers: Vec<String>,
    /// 分组行（组键升序：Null < Num < Str；ORDER BY 决定键序）。
    pub rows: Vec<GroupRow>,
}

/// 单个分组结果行。
#[derive(Debug, Clone)]
pub struct GroupRow {
    /// 各分组 level（与 `group_fields` 对齐）键文本（None = NULL：字段缺省/null/嵌套）。
    pub keys: Vec<Option<String>>,
    /// `keys` 各 level 是否数值（结果集列类型 LONGLONG/DOUBLE vs VAR_STRING）。
    pub key_is_num: Vec<bool>,
    /// 各聚合值（None = SQL NULL，如空数值集 SUM/AVG/MIN/MAX）；与 `headers` 对齐。
    pub cells: Vec<Option<String>>,
}

/// 组键（自定义 Eq/Hash：`-0.0` 与 `0.0` 归并为同组，数值按位等价）。
#[derive(Debug, Clone)]
enum GroupKey {
    Null,
    Num(f64),
    Str(String),
}

impl GroupKey {
    fn norm(x: f64) -> f64 {
        if x == 0.0 {
            0.0
        } else {
            x
        }
    }
    fn text(&self) -> Option<String> {
        match self {
            GroupKey::Null => None,
            GroupKey::Num(x) => Some(fmt_num(*x)),
            GroupKey::Str(s) => Some(s.clone()),
        }
    }
    fn is_num(&self) -> bool {
        matches!(self, GroupKey::Num(_))
    }
}

impl PartialEq for GroupKey {
    fn eq(&self, o: &Self) -> bool {
        use GroupKey::*;
        match (self, o) {
            (Null, Null) => true,
            (Num(a), Num(b)) => GroupKey::norm(*a).to_bits() == GroupKey::norm(*b).to_bits(),
            (Str(a), Str(b)) => a == b,
            _ => false,
        }
    }
}
impl Eq for GroupKey {}
impl std::hash::Hash for GroupKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use GroupKey::*;
        match self {
            Null => 0u8.hash(state),
            Num(x) => {
                1u8.hash(state);
                GroupKey::norm(*x).to_bits().hash(state);
            }
            Str(s) => {
                2u8.hash(state);
                s.hash(state);
            }
        }
    }
}

fn cmp_group_key(a: &GroupKey, b: &GroupKey) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (GroupKey::Null, GroupKey::Null) => Ordering::Equal,
        (GroupKey::Null, _) => Ordering::Less,
        (_, GroupKey::Null) => Ordering::Greater,
        (GroupKey::Num(x), GroupKey::Num(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (GroupKey::Str(x), GroupKey::Str(y)) => x.cmp(y),
        (GroupKey::Num(_), GroupKey::Str(_)) => Ordering::Less,
        (GroupKey::Str(_), GroupKey::Num(_)) => Ordering::Greater,
    }
}

/// Task-030：聚合值文本比较（ORDER BY 聚合列头）——两可解析为数值 → f64 比较；
/// 否则字典序；`None`（SQL NULL）升序最小（对齐 MySQL NULL 排序）。
fn cmp_agg_text(a: Option<&str>, b: Option<&str>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(x), Some(y)) => match (x.parse::<f64>().ok(), y.parse::<f64>().ok()) {
            (Some(p), Some(q)) => p.partial_cmp(&q).unwrap_or(Ordering::Equal),
            _ => x.cmp(y),
        },
    }
}

/// 单聚合列累积器（每组每列一份，下标与 specs 对齐——杜绝多聚合串扰）。
#[derive(Debug, Clone)]
struct AggState {
    /// COUNT(*) / COUNT(f) 计入行数（非 null 任意类型都计入 COUNT(f)）。
    count: u64,
    /// 数值行数（SUM/AVG/MIN/MAX 的存在性与均值分母；0 → 数值聚合 NULL）。
    n_num: u64,
    sum: f64,
    min: f64,
    max: f64,
}

impl AggState {
    fn new() -> Self {
        Self {
            count: 0,
            n_num: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }
}

/// 从文档取组键：顶层字段优先字节级取值（免整文档反序列化）；点路径/其余值
/// serde 回退——Number → Num、String → Str，其余（缺省/null/布尔/嵌套）→ NULL 组
/// （与 SQL「NULL 分一组」语义一致）。
fn group_key_of(doc: &[u8], field: &str) -> GroupKey {
    if !field.contains('.') {
        match light_top_field(doc, field) {
            Some(LightVal::Num(b)) => {
                if let Some(x) = std::str::from_utf8(b).ok().and_then(|s| s.parse::<f64>().ok()) {
                    return GroupKey::Num(GroupKey::norm(x));
                }
            }
            Some(LightVal::Str(b)) => {
                if let Ok(s) = std::str::from_utf8(b) {
                    return GroupKey::Str(s.to_string());
                }
            }
            _ => {}
        }
    }
    let Ok(v) = serde_json::from_slice::<Value>(doc) else {
        return GroupKey::Null;
    };
    match field_of(&v, field) {
        Some(Value::Number(n)) => n.as_f64().map(GroupKey::Num).unwrap_or(GroupKey::Null),
        Some(Value::String(s)) => GroupKey::Str(s.clone()),
        _ => GroupKey::Null,
    }
}

/// 字段是否「存在且非 JSON null」（COUNT(f) 语义，任意类型非 null 计入）。
fn field_non_null(doc: &[u8], f: &str) -> bool {
    if !f.contains('.') {
        if let Some(r) = light_top_field(doc, f) {
            return !matches!(r, LightVal::Absent | LightVal::Null);
        }
    }
    serde_json::from_slice::<Value>(doc)
        .ok()
        .map(|v| {
            field_of(&v, f)
                .map(|fv| !matches!(fv, Value::Null))
                .unwrap_or(false)
        })
        .unwrap_or(false)
}

/// 取数值字段（SUM/AVG/MIN/MAX 只统计 JSON number；非数值/缺省 → None）。
fn numeric_field(doc: &[u8], f: &str) -> Option<f64> {
    if !f.contains('.') {
        if let Some(LightVal::Num(b)) = light_top_field(doc, f) {
            if let Some(x) = std::str::from_utf8(b).ok().and_then(|s| s.parse::<f64>().ok()) {
                return Some(x);
            }
        }
    }
    serde_json::from_slice::<Value>(doc)
        .ok()
        .and_then(|v| field_of(&v, f).and_then(|fv| fv.as_f64()))
}

/// 单聚合列输出：COUNT → 计数值；SUM/AVG/MIN/MAX 无数值行 → NULL（SQL 语义）。
fn agg_cell(name: &str, st: &AggState) -> Option<String> {
    match name {
        "count" => Some(st.count.to_string()),
        "sum" if st.n_num > 0 => Some(fmt_num(st.sum)),
        "avg" if st.n_num > 0 => Some(fmt_num(st.sum / st.n_num as f64)),
        "min" if st.n_num > 0 => Some(fmt_num(st.min)),
        "max" if st.n_num > 0 => Some(fmt_num(st.max)),
        _ => None,
    }
}

/// 聚合列头（`COUNT(*)` / `SUM(amount)` 形态，与 HAVING 左项/结果集列头一致）。
fn spec_header(name: &str, field: &Option<String>) -> String {
    let arg = field.as_deref().unwrap_or("*");
    format!("{}({arg})", name.to_uppercase())
}

/// HAVING 条件比较：两侧皆可解析为数值 → f64 比较（对齐 MySQL 数值列/COUNT 等），
/// 否则字节/字典序字符串比较。
fn cmp_having_cond(op: &CmpOp, lhs: &str, rhs: &str) -> bool {
    use std::cmp::Ordering::{Equal, Greater, Less};
    let ord = match (lhs.parse::<f64>().ok(), rhs.parse::<f64>().ok()) {
        (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Equal),
        _ => lhs.cmp(rhs),
    };
    match op {
        CmpOp::Eq => ord == Equal,
        CmpOp::Ne => ord != Equal,
        CmpOp::Gt => ord == Greater,
        CmpOp::Lt => ord == Less,
        CmpOp::Ge => ord == Greater || ord == Equal,
        CmpOp::Le => ord == Less || ord == Equal,
    }
}

/// HAVING 过滤单个分组：左项先按聚合列头匹配（specs），再按分组字段匹配（keys）；
/// 值 NULL（组键缺省/空数值聚合）→ 任何比较不成立（对齐 MySQL NULL → 行被过滤）；
/// 未知左项 → 不命中（保守）。
fn having_matches(
    h: &HavingExpr,
    fields: &[String],
    keys: &[GroupKey],
    specs: &[(String, Option<String>)],
    states: &[AggState],
) -> bool {
    match h {
        HavingExpr::Cond(c) => {
            let val: Option<String> = match specs
                .iter()
                .position(|(n, f)| spec_header(n, f) == c.lhs)
            {
                Some(idx) => agg_cell(&specs[idx].0, &states[idx]),
                None => match fields.iter().position(|f| f == &c.lhs) {
                    Some(lv) => keys[lv].text(),
                    None => return false,
                },
            };
            let Some(lhs) = val else {
                return false;
            };
            cmp_having_cond(&c.op, &lhs, &c.value)
        }
        HavingExpr::Not(e) => !having_matches(e, fields, keys, specs, states),
        HavingExpr::And(a, b) => {
            having_matches(a, fields, keys, specs, states)
                && having_matches(b, fields, keys, specs, states)
        }
        HavingExpr::Or(a, b) => {
            having_matches(a, fields, keys, specs, states)
                || having_matches(b, fields, keys, specs, states)
        }
    }
}

/// Ex-9.3 ④b：无 WHERE 单字段 `GROUP BY` 的倒排词典枚举快路径（免逐行扫描）。
/// 路由条件：聚合列 ⊆ {`COUNT(*)`, `SUM/AVG/MIN/MAX(stats_field)`（后者须 ∈ stats_fields）}。
/// NULL 组语义精确：总行数 = `engine.count_all_docs`（key-only）；缺字段行 = N − Σ组行数。
/// 若存在缺字段行且含数值聚合 → 缺字段行的数值贡献无法经倒排获得 → 放弃快路径回退全扫
/// （宁慢勿错）。gs 为空（数值字段无倒排 term / 字段全缺）→ 回退全扫保证与扫描路径一致。
fn group_by_fast_inverted(
    engine: &Engine,
    sel: &Select,
    g: &str,
    specs: &[(String, Option<String>)],
    cap: u64,
) -> Result<Option<GroupResult>> {
    for (n, f) in specs {
        let ok = match n.as_str() {
            "count" => f.is_none(),
            "sum" | "avg" | "min" | "max" => f
                .as_deref()
                .is_some_and(|ff| engine.stats_field_pos(ff).is_some()),
            _ => false,
        };
        if !ok {
            return Ok(None);
        }
    }
    let gs = engine.inverted_group_stats(g)?;
    if gs.is_empty() {
        return Ok(None); // 数值字段（无 term）/ 无该字段文档 → 回退扫描路径
    }
    let has_numeric = specs
        .iter()
        .any(|(n, _)| matches!(n.as_str(), "sum" | "avg" | "min" | "max"));
    let sum_rows: u64 = gs.iter().map(|x| x.1).sum();
    let total = engine.count_all_docs()?;
    let null_rows = total.saturating_sub(sum_rows);
    if has_numeric && null_rows > 0 {
        return Ok(None);
    }
    let mut list: Vec<(Vec<GroupKey>, Vec<AggState>)> = Vec::with_capacity(gs.len() + 1);
    for (value, count, stats) in gs {
        let states = specs
            .iter()
            .map(|(n, f)| {
                let mut st = AggState::new();
                match n.as_str() {
                    "count" => st.count = count,
                    _ => {
                        let pos = engine.stats_field_pos(f.as_deref().unwrap()).unwrap();
                        if let Some(a) = stats.get(pos) {
                            st.n_num = a.n;
                            st.sum = a.sum;
                            st.min = a.min;
                            st.max = a.max;
                        }
                    }
                }
                st
            })
            .collect();
        list.push((vec![GroupKey::Str(value)], states));
    }
    if null_rows > 0 {
        // 缺该字段文档并入 NULL 组（仅 COUNT(*) 场景可达此处）
        let states = specs
            .iter()
            .map(|(n, f)| {
                let mut st = AggState::new();
                if n == "count" && f.is_none() {
                    st.count = null_rows;
                }
                st
            })
            .collect();
        list.push((vec![GroupKey::Null], states));
    }
    // HAVING 过滤（对齐主路径语义）
    if let Some(h) = &sel.having {
        let fields = [g.to_string()];
        list.retain(|(k, sts)| having_matches(h, &fields, k, specs, sts));
    }
    // 排序：ORDER BY 仅限分组字段（= g）；含聚合列头（Task-030，如 COUNT(*)/SUM(f)）→
    // 交主路径（主路径支持聚合值排序，本快路径不实现）；其余字段 → 明确报错。
    for (f, _) in &sel.order_by {
        if f != g {
            if specs.iter().any(|(n, fl)| spec_header(n, fl) == *f) {
                return Ok(None);
            }
            return Err(Error::Config(format!(
                "GROUP BY 结果排序字段 {f} 须属于分组字段（{g}）"
            )));
        }
    }
    let desc = sel.order_by.iter().any(|(_, d)| *d);
    list.sort_by(|a, b| {
        let mut o = cmp_group_key(&a.0[0], &b.0[0]);
        if desc {
            o = o.reverse();
        }
        o
    });
    let offset = sel.offset as usize;
    let limit = sel.limit.unwrap_or(cap).min(cap) as usize;
    let rows: Vec<GroupRow> = list
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|(k, sts)| {
            let keys = k.iter().map(|gk| gk.text()).collect();
            let key_is_num = k.iter().map(|gk| gk.is_num()).collect();
            let cells = specs
                .iter()
                .zip(sts.iter())
                .map(|((name, _), st)| agg_cell(name, st))
                .collect();
            GroupRow { keys, key_is_num, cells }
        })
        .collect();
    let funcs: Vec<&str> = specs.iter().map(|(n, _)| n.as_str()).collect();
    let group_cols: Vec<String> = sel
        .columns
        .iter()
        .filter(|c| !funcs.contains(&c.to_lowercase().as_str()))
        .cloned()
        .collect();
    let headers: Vec<String> = specs
        .iter()
        .map(|(n, f)| spec_header(n, f))
        .collect();
    Ok(Some(GroupResult {
        group_fields: vec![g.to_string()],
        group_cols,
        headers,
        rows,
    }))
}

/// P-GB（2026-09-05）：窗口白名单位图分组快路径（server 整表窗口下 #14/#27/#59/#81 收敛）。
/// 语义 = 权威扫描：组计数经「窗口∩活跃集（∩WHERE 单等值候选 posting）」精确（删除/复活；
/// 候选=WHERE 命中集内分组，等价行级过滤）；单字段可补 NULL 组（Σ<live）；两字段要求
/// Σ==live（无缺字段/NULL 组）否则回退扫描；Σ>live（陈旧值变更交叉放大）亦回退扫描保精确。
/// 支持单列 `COUNT(*)` 聚合（含 HAVING/组字段或该聚合列头排序/LIMIT）；WHERE 为非单等值、
/// SUM/AVG 等 stats 聚合、多聚合 → None（回退主路径）。
fn group_by_fast_bitmap_window(
    engine: &Engine,
    sel: &Select,
    fields: &[String],
    cap: u64,
    start: Option<u64>,
    end: Option<u64>,
) -> Result<Option<GroupResult>> {
    let specs = &sel.group_aggs;
    if specs.len() != 1 || !(specs[0].0 == "count" && specs[0].1.is_none()) {
        return Ok(None);
    }
    // WHERE：无 或 单等值 `f=value`（含数值文本；AND/OR/比较/BETWEEN/LIKE → 行级 → 扫描）
    let cand_term: Option<String> = match sel.where_expr.as_ref() {
        None => None,
        Some(WhereExpr::Cond(c)) if c.op == CmpOp::Eq && !c.field.eq_ignore_ascii_case("docid") => {
            Some(format!("{}={}", c.field, c.value))
        }
        Some(_) => return Ok(None),
    };
    let (combos, live_total) = match engine.group_by_bitmap_window(fields, cand_term.as_deref(), start, end)? {
        Some(v) => v,
        None => return Ok(None),
    };
    let mut list: Vec<(Vec<GroupKey>, Vec<AggState>)> = Vec::with_capacity(combos.len() + 1);
    let mut sum = 0u64;
    for (vals, cnt) in &combos {
        sum += *cnt;
        let mut st = AggState::new();
        st.count = *cnt;
        list.push((vals.iter().map(|v| GroupKey::Str(v.clone())).collect(), vec![st]));
    }
    if sum > live_total {
        return Ok(None); // 陈旧值变更交叉放大 → 回退权威扫描
    }
    if fields.len() == 1 {
        if sum < live_total {
            let mut st = AggState::new();
            st.count = live_total - sum;
            list.push((vec![GroupKey::Null], vec![st]));
        }
    } else if sum < live_total {
        return Ok(None); // 两字段含缺字段行 → NULL 组分布词典无法精确 → 回退扫描
    }
    if list.len() as u64 > cap {
        return Err(Error::QueryTooExpensive(format!(
            "GROUP BY 分组数超过上限（{} 组，上限 {cap}），请加 WHERE 收敛",
            list.len()
        )));
    }
    // HAVING（组字段 + COUNT(*) 聚合左项）
    if let Some(h) = &sel.having {
        list.retain(|(k, sts)| having_matches(h, fields, k, specs, sts));
    }
    // 排序：ORDER BY 序列（组字段级 'f' / 唯一聚合列头 'a'=COUNT(*)，DESC 反转）+ 其余组 level 升序补尾
    let mut order_seq: Vec<(char, usize, bool)> = Vec::new();
    for (f, desc) in &sel.order_by {
        let rf = if let Some(idx) = fields.iter().position(|x| x == f) {
            ('f', idx)
        } else if specs.iter().any(|(n, fl)| spec_header(n, fl) == *f) {
            ('a', 0) // specs 已限单列 COUNT(*)
        } else {
            return Ok(None); // 越界 → 交主路径（正确报错/行为）
        };
        if !order_seq.iter().any(|(k, i, _)| k == &rf.0 && i == &rf.1) {
            order_seq.push((rf.0, rf.1, *desc));
        }
    }
    for (i, _) in fields.iter().enumerate() {
        if !order_seq.iter().any(|(k, j, _)| *k == 'f' && *j == i) {
            order_seq.push(('f', i, false));
        }
    }
    list.sort_by(|a, b| {
        for (k, idx, desc) in &order_seq {
            let mut ord = if *k == 'f' {
                cmp_group_key(&a.0[*idx], &b.0[*idx])
            } else {
                let av = agg_cell(&specs[*idx].0, &a.1[*idx]);
                let bv = agg_cell(&specs[*idx].0, &b.1[*idx]);
                cmp_agg_text(av.as_deref(), bv.as_deref())
            };
            if *desc {
                ord = ord.reverse();
            }
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
    let offset = sel.offset as usize;
    let limit = sel.limit.unwrap_or(cap).min(cap) as usize;
    let rows: Vec<GroupRow> = list
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|(k, sts)| {
            let keys = k.iter().map(|gk| gk.text()).collect();
            let key_is_num = k.iter().map(|gk| gk.is_num()).collect();
            let cells = specs
                .iter()
                .zip(sts.iter())
                .map(|((name, _), st)| agg_cell(name, st))
                .collect();
            GroupRow { keys, key_is_num, cells }
        })
        .collect();
    let funcs: Vec<&str> = specs.iter().map(|(n, _)| n.as_str()).collect();
    let group_cols: Vec<String> = sel
        .columns
        .iter()
        .filter(|c| !funcs.contains(&c.to_lowercase().as_str()))
        .cloned()
        .collect();
    let headers: Vec<String> = specs
        .iter()
        .map(|(n, f)| spec_header(n, f))
        .collect();
    Ok(Some(GroupResult {
        group_fields: fields.to_vec(),
        group_cols,
        headers,
        rows,
    }))
}

/// GROUP BY 执行（AF#2~#4）：`SELECT <cols>, COUNT/SUM/AVG/MIN/MAX ... GROUP BY f1, f2...` →
/// 全量单遍扫描分组（与无索引聚合同语义，不依赖倒排完整性；WHERE 行级过滤），组键升序
/// 输出（Null < Num < Str；`ORDER BY` 决定键序——仅限分组字段），LIMIT/OFFSET 对**组行**
/// 切片；`cap` = 分组数上限（超限 QueryTooExpensive）兼 LIMIT 缺省值。
///
/// 非 GROUP BY SQL 返回 `Ok(None)`（调用方继续走标量聚合/普通查询）。
/// P1-3：带 docid 区间窗口的 GROUP BY 执行（非默认表按表区间分组；None/None = 全库）。
/// 窗口非空禁用倒排词典枚举快路径（引擎全库口径，跨表会串表）→ 强制窗口扫描。
pub fn execute_group_by_window(
    engine: &Engine,
    sql: &str,
    cap: u64,
    start: Option<u64>,
    end: Option<u64>,
) -> Result<Option<GroupResult>> {
    let scoped = start.is_some() || end.is_some();
    let sel = parse_select(sql)?;
    let fields = sel.group_by.clone();
    if fields.is_empty() {
        return Ok(None);
    }
    let specs = sel.group_aggs.clone();
    if specs.is_empty() {
        return Err(Error::Config("GROUP BY 需至少一个聚合列".into()));
    }
    // Ex-9.3 ④b：无 WHERE 单字段 GROUP BY → 倒排词典枚举快路径（不可路由自动回退扫描）。
    // P1-3：表区间窗口禁用（倒排为引擎全库口径，跨表会串表）。
    if !scoped && sel.where_expr.is_none() && fields.len() == 1 {
        if let Some(res) = group_by_fast_inverted(engine, &sel, &fields[0], &specs, cap)? {
            return Ok(Some(res));
        }
    }
    // P-GB（2026-09-05）：**窗口**白名单位图分组快路径——server 恒传本表整窗（select.rs L107）
    // → scoped=true，上面词典路径不可用 → #14/#27/#59/#81 恒全扫。窗口位图路径在引擎侧按
    // 「窗口∩活跃集（∩WHERE 单等值候选）」精确计数（跨表高位 docid 排外，无串表），语义 =
    // 权威扫描；WHERE 非单等值 / SUM/AVG / 两字段含缺字段行时 helper 自动回退下方全量扫描。
    if scoped && (1..=2).contains(&fields.len()) {
        if let Some(res) = group_by_fast_bitmap_window(engine, &sel, &fields, cap, start, end)? {
            return Ok(Some(res));
        }
    }
    let guard = engine.query_guard();
    // P91：GROUP BY 全扫投影列 = WHERE 引用 ∪ 分组列 ∪ 聚合列（PAX 块只解这些列）
    let needed = group_scan_needed_fields(sel.where_expr.as_ref(), &fields, &specs);
    // Task-024：行式布局下 scan_stream_fields 返回整行原文 → 每行先单遍只收 needed 的子集
    // 字节（只构造所需列 Value），后续 where/分组键/聚合提取都在短子集上进行，免逐字段
    // 整行 serde parse ×N（#14/#27 全扫分组热点）。仅顶层简单字段名可走子集（含 ./[ 回退）。
    let simple_needed = needed.iter().all(|f| !f.contains('.') && !f.contains('['));
    // 复合组键 = 各分组 level 键向量；每组持每聚合列一个累积器（与 specs 对齐）。
    let mut groups: std::collections::HashMap<Vec<GroupKey>, Vec<AggState>> =
        std::collections::HashMap::new();
    // Task-025b 阶段②：GROUP BY 分片合并——窗口两端有限时按核等分子窗并发构建局部分组，
    // 再按组键/累加器逐项合并（count/n_num/sum 相加、min/max 取极值；聚合交换律保证一致）。
    let mut did_parallel = false;
    if let Some((lo, hi)) = start.zip(end) {
        if lo < hi {
            if let Ok(ncpu) = std::thread::available_parallelism() {
                let workers = ncpu.get().clamp(2, 8);
                let span = hi - lo + 1;
                let guard_ref = &guard;
                let sel_where = sel.where_expr.as_ref();
                let mut partials: Vec<Result<std::collections::HashMap<Vec<GroupKey>, Vec<AggState>>>> =
                    Vec::with_capacity(workers);
                std::thread::scope(|sc| {
                    let mut handles = Vec::with_capacity(workers);
                    for w in 0..workers {
                        let (cs, ce) = {
                            let step = span / workers as u64;
                            let s = lo + step * w as u64;
                            let e = if w + 1 == workers { hi } else { lo + step * (w as u64 + 1) - 1 };
                            (s, e)
                        };
                        // 以引用捕获（move 闭包只复制引用，不搬走 owned 值）
                        let needed_r = &needed;
                        let fields_r = &fields;
                        let specs_r = &specs;
                        handles.push(sc.spawn(move || -> Result<std::collections::HashMap<Vec<GroupKey>, Vec<AggState>>> {
                            let mut g: std::collections::HashMap<Vec<GroupKey>, Vec<AggState>> =
                                std::collections::HashMap::new();
                            let mut scanned_local = 0u64;
                            engine.scan_stream_fields(Some(cs), Some(ce), needed_r.clone(), |_docid, doc| {
                                scanned_local += 1;
                                if scanned_local % 4096 == 0 && guard_ref.is_expired() {
                                    return Err(Error::QueryTooExpensive(
                                        "GROUP BY 并行全扫超时（熔断中止）".into(),
                                    ));
                                }
                                // 与串行分支一致的子集化 + WHERE + 分组/聚合提取
                                let mut owned_sub;
                                let work: &[u8] = if simple_needed {
                                    match subset_doc_bytes(doc, needed_r) {
                                        Some(b) => {
                                            owned_sub = b;
                                            &owned_sub
                                        }
                                        None => doc,
                                    }
                                } else {
                                    doc
                                };
                                if let Some(wh) = sel_where {
                                    let hit = match light_where_matches(work, wh) {
                                        Some(r) => r,
                                        None => serde_json::from_slice::<Value>(work)
                                            .map(|v| wh.matches_doc(&v))
                                            .unwrap_or(false),
                                    };
                                    if !hit {
                                        return Ok(true);
                                    }
                                }
                                let key: Vec<GroupKey> = fields_r.iter().map(|f| group_key_of(work, f)).collect();
                                let states = g.entry(key).or_insert_with(|| {
                                    (0..specs_r.len()).map(|_| AggState::new()).collect()
                                });
                                for (idx, (name, fld)) in specs_r.iter().enumerate() {
                                    let st = &mut states[idx];
                                    match name.as_str() {
                                        "count" => {
                                            if fld.is_none() || field_non_null(work, fld.as_ref().unwrap()) {
                                                st.count += 1;
                                            }
                                        }
                                        _ => {
                                            if let Some(x) = numeric_field(work, fld.as_ref().unwrap()) {
                                                st.n_num += 1;
                                                st.sum += x;
                                                if x < st.min {
                                                    st.min = x;
                                                }
                                                if x > st.max {
                                                    st.max = x;
                                                }
                                            }
                                        }
                                    }
                                }
                                if g.len() as u64 > cap {
                                    return Err(Error::QueryTooExpensive(format!(
                                        "GROUP BY 分组数超过上限（{} 组，上限 {cap}），请加 WHERE 收敛",
                                        g.len()
                                    )));
                                }
                                Ok(true)
                            })?;
                            Ok(g)
                        }));
                    }
                    for h in handles {
                        partials.push(h.join().unwrap());
                    }
                });
                for p in partials {
                    for (k, sv) in p? {
                        let dst = groups.entry(k).or_insert_with(|| {
                            (0..specs.len()).map(|_| AggState::new()).collect()
                        });
                        for (i, s) in sv.into_iter().enumerate() {
                            let d = &mut dst[i];
                            d.count += s.count;
                            d.n_num += s.n_num;
                            d.sum += s.sum;
                            if s.min < d.min {
                                d.min = s.min;
                            }
                            if s.max > d.max {
                                d.max = s.max;
                            }
                        }
                    }
                }
                did_parallel = true;
            }
        }
    }
    if !did_parallel {
        let mut scanned = 0u64;
        engine.scan_stream_fields(start, end, needed.clone(), |_docid, doc| {
            scanned += 1;
            if scanned % 4096 == 0 && guard.is_expired() {
                return Err(Error::QueryTooExpensive(
                    "GROUP BY 全量扫描超时（熔断中止）".into(),
                ));
            }
            // 整行原文 → 子集字节（缺省：非对象/解析失败保持原文，行为与整行路径一致）
            let mut owned_sub;
            let work: &[u8] = if simple_needed {
                match subset_doc_bytes(doc, &needed) {
                    Some(b) => {
                        owned_sub = b;
                        &owned_sub
                    }
                    None => doc,
                }
            } else {
                doc
            };
            if let Some(wh) = &sel.where_expr {
                let hit = match light_where_matches(work, wh) {
                    Some(r) => r,
                    None => serde_json::from_slice::<Value>(work)
                        .map(|v| wh.matches_doc(&v))
                        .unwrap_or(false),
                };
                if !hit {
                    return Ok(true);
                }
            }
            let key: Vec<GroupKey> = fields.iter().map(|f| group_key_of(work, f)).collect();
            let states = groups.entry(key).or_insert_with(|| {
                (0..specs.len()).map(|_| AggState::new()).collect()
            });
            for (idx, (name, fld)) in specs.iter().enumerate() {
                let st = &mut states[idx];
                match name.as_str() {
                    "count" => {
                        if fld.is_none() || field_non_null(work, fld.as_ref().unwrap()) {
                            st.count += 1;
                        }
                    }
                    _ => {
                        if let Some(x) = numeric_field(work, fld.as_ref().unwrap()) {
                            st.n_num += 1;
                            st.sum += x;
                            if x < st.min {
                                st.min = x;
                            }
                            if x > st.max {
                                st.max = x;
                            }
                        }
                    }
                }
            }
            if groups.len() as u64 > cap {
                return Err(Error::QueryTooExpensive(format!(
                    "GROUP BY 分组数超过上限（{} 组，上限 {cap}），请加 WHERE 收敛",
                    groups.len()
                )));
            }
            Ok(true)
        })?;
    }
    let mut list: Vec<(Vec<GroupKey>, Vec<AggState>)> = groups.into_iter().collect();
    // HAVING（AF#5）：分组完成后、排序/切片前过滤组行。
    if let Some(h) = &sel.having {
        list.retain(|(k, sts)| having_matches(h, &fields, k, &specs, sts));
    }
    // 组行排序：优先级 = ORDER BY 序列——每项可为分组字段（`Field`）或聚合列头
    // （Task-030：`Agg`，按该聚合值数值比较，NULL 组升序最前）；剩余分组 level 升序补尾。
    // DESC 反转该级（Null 随之移末，对齐 MySQL DESC）。
    // 表示：('f', 分组 level 下标) / ('a', specs 下标)。
    let mut order_seq: Vec<(char, usize, bool)> = Vec::new();
    for (f, desc) in &sel.order_by {
        let rf = if let Some(idx) = fields.iter().position(|x| x == f) {
            ('f', idx)
        } else if let Some(idx) = specs.iter().position(|(n, fl)| spec_header(n, fl) == *f) {
            ('a', idx)
        } else {
            let heads: Vec<String> = specs.iter().map(|(n, fl)| spec_header(n, fl)).collect();
            return Err(Error::Config(format!(
                "GROUP BY 结果排序目标 {f} 须为分组字段（{}）或聚合列（{}）",
                fields.join(", "),
                heads.join(", ")
            )));
        };
        if !order_seq.iter().any(|(k, i, _)| k == &rf.0 && i == &rf.1) {
            order_seq.push((rf.0, rf.1, *desc));
        }
    }
    for (i, _) in fields.iter().enumerate() {
        if !order_seq.iter().any(|(k, j, _)| *k == 'f' && *j == i) {
            order_seq.push(('f', i, false));
        }
    }
    list.sort_by(|a, b| {
        for (k, idx, desc) in &order_seq {
            let mut ord = if *k == 'f' {
                cmp_group_key(&a.0[*idx], &b.0[*idx])
            } else {
                // 聚合值排序：数值比较；NULL（无数值聚合）升序最小
                let av = agg_cell(&specs[*idx].0, &a.1[*idx]);
                let bv = agg_cell(&specs[*idx].0, &b.1[*idx]);
                cmp_agg_text(av.as_deref(), bv.as_deref())
            };
            if *desc {
                ord = ord.reverse();
            }
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
    let offset = sel.offset as usize;
    let limit = sel.limit.unwrap_or(cap).min(cap) as usize;
    let rows: Vec<GroupRow> = list
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|(k, sts)| {
            let keys = k.iter().map(|gk| gk.text()).collect();
            let key_is_num = k.iter().map(|gk| gk.is_num()).collect();
            let cells = specs
                .iter()
                .zip(sts.iter())
                .map(|((name, _), st)| agg_cell(name, st))
                .collect();
            GroupRow {
                keys,
                key_is_num,
                cells,
            }
        })
        .collect();
    // 结果集分组列 = 选中普通列（select 顺序；聚合函数名剔除——保留字限定的已知取舍）。
    let funcs: Vec<&str> = specs.iter().map(|(n, _)| n.as_str()).collect();
    let group_cols: Vec<String> = sel
        .columns
        .iter()
        .filter(|c| !funcs.contains(&c.to_lowercase().as_str()))
        .cloned()
        .collect();
    let headers: Vec<String> = specs
        .iter()
        .map(|(n, f)| spec_header(n, f))
        .collect();
    Ok(Some(GroupResult {
        group_fields: fields,
        group_cols,
        headers,
        rows,
    }))
}

/// 全库 GROUP BY（兼容入口 = 无窗口）。
pub fn execute_group_by(engine: &Engine, sql: &str, cap: u64) -> Result<Option<GroupResult>> {
    execute_group_by_window(engine, sql, cap, None, None)
}
