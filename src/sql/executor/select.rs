//! 通用 SELECT 执行（原 sqlish.rs 执行段）：`execute` 入口（组合索引路由 →
//! 代价优化 → DocIdSet 收敛 → 排序/Top-K/分块回表）+ `doc_matches_where` +
//! `get_docid_set`/`docset_to_sorted` + `try_composite_index`/`extract_eq_conds`。
//! 行级字节扫描/位图求值基建在 `super::eval`；JOIN 在 `super::join`。

use crate::engine::{Engine, QueryRow};
use crate::error::{Error, Result};
use crate::sql::parser::{parse_select, CmpOp, Select, WhereExpr};
use roaring::treemap::RoaringTreemap as RoaringBitmap;
use serde_json::Value;
use std::collections::HashMap;

use super::eval::{eval, full_docids, read_target_value, scan_leaf, scan_pushdown, skip_value, ws, Leaf, LightVal};
use super::join::execute_join;

/// P0-A：提取 WHERE 中所有等值条件（field=value），返回 (field, value) 列表。
pub(crate) fn extract_eq_conds(e: &WhereExpr) -> Vec<(String, String)> {
    let mut out = Vec::new();
    match e {
        WhereExpr::Cond(c) if matches!(c.op, CmpOp::Eq) && c.field != "docid" => {
            out.push((c.field.clone(), c.value.clone()));
        }
        WhereExpr::And(a, b) => {
            out.extend(extract_eq_conds(a));
            out.extend(extract_eq_conds(b));
        }
        _ => {}
    }
    out
}

/// P127：WHERE 是否含主键区间叶（`id BETWEEN`/`docid BETWEEN` 数值）——组合路由守卫用。
/// 主键区间是强约束（= docid 区间），composite 等值前缀路由会先物化大候选再复筛；
/// eval AND 快路径（post_filter + LIMIT 早停）在区间∩等值上毫秒级 → 命中即回退 eval。
fn where_has_pk_between(e: &WhereExpr) -> bool {
    match e {
        WhereExpr::Between { field, low, high } => {
            let f = field.to_lowercase();
            (f == "id" || f == "docid")
                && !field.contains('.')
                && !field.contains('[')
                && low.parse::<u64>().is_ok()
                && high.parse::<u64>().is_ok()
        }
        WhereExpr::And(a, b) | WhereExpr::Or(a, b) => {
            where_has_pk_between(a) || where_has_pk_between(b)
        }
        WhereExpr::Not(x) => where_has_pk_between(x),
        _ => false,
    }
}

/// P0-A：声明式组合索引路由。
/// 检查 WHERE 等值条件是否匹配 engine 的 composite_indexes 最左前缀。
/// 匹配时走 `query_by_composite_prefix`（cidx 前缀扫描 → 回表），避免全扫/逐行过滤。
/// 返回 Ok(Some(rows)) = 命中并执行；Ok(None) = 不匹配，回退原路径。
fn try_composite_index(
    engine: &Engine,
    sel: &Select,
    cap: u64,
) -> Result<Option<Vec<QueryRow>>> {
    // P127 守卫：WHERE = 等值(可能命中 composite 前缀) + 主键区间(id/docid BETWEEN)组合 →
    // 回退 eval（主键区间是强 docid 区间约束；composite 等值前缀路由在此会全候选物化+复筛
    // ——组合 SELECT 0.5s 恒定的根因（100k 62ms→1100k 582ms ≈ 9.4×）；eval AND 快路径
    // 区间∩等值毫秒级）。纯主键区间（无等值）不拦——composite 单列 id 范围路由仍可用。
    if let Some(we) = &sel.where_expr {
        if !extract_eq_conds(we).is_empty() && where_has_pk_between(we) {
            return Ok(None);
        }
    }
    if engine.composite_indexes.is_empty() || sel.where_expr.is_none() {
        return Ok(None);
    }
    // P92/#31：单字段 `BETWEEN` + 声明**单列**组合索引 `[field]` → cidx 范围路由
    // （#31 形态：ts BETWEEN 无等值前缀，等值分支不命中 → 此前全扫 ~700-900ms）。
    // 范围扫描命中即回表；边界字节序误命中由下方 WHERE 复筛兜底（语义=全扫 BETWEEN）。
    if let Some(WhereExpr::Between { field, low, high }) = sel.where_expr.as_ref() {
        let has_single_col = engine
            .composite_indexes
            .iter()
            .any(|f| f.len() == 1 && &f[0] == field);
        if has_single_col {
            let mut rows = engine.query_by_composite_range(field, low, high)?;
            if let Some(we) = &sel.where_expr {
                rows.retain(|(_, v)| match serde_json::from_slice::<serde_json::Value>(v) {
                    Ok(doc) => we.matches_doc(&doc),
                    Err(_) => false,
                });
            }
            let limit = sel.limit.unwrap_or(cap).min(cap);
            if sel.offset > 0 {
                rows = rows.into_iter().skip(sel.offset as usize).collect();
            }
            if rows.len() as u64 > limit {
                rows.truncate(limit as usize);
            }
            return Ok(Some(rows));
        }
    }
    let eqs = extract_eq_conds(sel.where_expr.as_ref().unwrap());
    if eqs.is_empty() {
        return Ok(None);
    }
    // 对每个声明的组合索引，检查等值条件是否覆盖最左前缀（至少 1 字段）。
    // 取匹配前缀最长的索引（选择性最高）。
    let mut best: Option<(usize, Vec<String>)> = None; // (index_idx, matched_values)
    for (i, fields) in engine.composite_indexes.iter().enumerate() {
        let mut matched_vals: Vec<String> = Vec::new();
        let mut all_match = true;
        for f in fields {
            if let Some((_, v)) = eqs.iter().find(|(ef, _)| ef == f) {
                matched_vals.push(v.clone());
            } else {
                all_match = false;
                break;
            }
        }
        if all_match && !matched_vals.is_empty() {
            match &best {
                None => best = Some((i, matched_vals)),
                Some((_, prev)) if matched_vals.len() > prev.len() => best = Some((i, matched_vals)),
                _ => {}
            }
        }
    }
    let Some((_, vals)) = best else { return Ok(None); };
    // 走组合索引前缀扫描
    let fields: Vec<&[u8]> = vals.iter().map(|v| v.as_bytes()).collect();
    let mut rows = engine.query_by_composite_prefix(&fields)?;
    // review 修复（2026-09-04）stale 键防护：cidx 前缀命中后回表的是**最新**文档值——
    // put 更新字段变更/delete 不删旧复合键，旧键仍会命中并带回不满足条件的文档。
    // 用完整 WHERE 表达式对回表值复筛，杜绝错行（匹配开销 O(命中集)，远小于全扫）。
    if let Some(we) = &sel.where_expr {
        rows.retain(|(_, v)| match serde_json::from_slice::<serde_json::Value>(v) {
            Ok(doc) => we.matches_doc(&doc),
            Err(_) => false,
        });
    }
    // LIMIT/OFFSET
    let limit = sel.limit.unwrap_or(cap).min(cap);
    if sel.offset > 0 {
        rows = rows.into_iter().skip(sel.offset as usize).collect();
    }
    if rows.len() as u64 > limit {
        rows.truncate(limit as usize);
    }
    Ok(Some(rows))
}

/// A4（DocIdSet 读路径重构）：WHERE 条件 → 统一 docid 集合（optimizer_proces 阶段 1 阶梯）。
///
/// 阶梯选择（对齐 optimizer_integration_design.md §2.4）：
///   - 无 WHERE → `All`（全集，不物化）
///   - 有 WHERE → `eval()` 位图求值（继承 AND 快路径 / LIKE / OR / NOT 既有语义）→ `Bitmap`
///     （空位图收敛为 `Empty`）
///
/// `limit`：传给 eval 的候选早停上界（`post_filter`/`scan_all` 找到 limit 个即停）。
/// 读路径传实际 LIMIT/cap 语义（防大候选全量遍历）；写路径定位传 `None`（须全量收敛，
/// D1 不截断）；JOIN 收敛传 `None`。
///
/// 组合索引前缀路由（P0-A）由 execute/execute_join 层先行尝试（cidx 需回表取 doc 复筛，
/// 属"行产出"而非纯 docid 集；此处专注 WHERE→集合统一抽象，供读/写/JOIN 共用消费端）。
/// 与 `eval` 等价性保证：本函数是既有 eval 路径的形态包装，不重写求值逻辑——665 测试
/// 全量护航无回归；写路径（delete/update 定位）与 JOIN 复用同一 DocIdSet 消费接口。
pub fn get_docid_set(
    engine: &Engine,
    where_expr: Option<&WhereExpr>,
    limit: Option<u64>,
    guard: &crate::watchdog::QueryGuard,
) -> Result<crate::docset::DocIdSet> {
    use crate::docset::DocIdSet;
    let Some(e) = where_expr else {
        return Ok(DocIdSet::All);
    };
    let cap = limit.unwrap_or(u64::MAX);
    let bm = eval(engine, e, cap, guard)?;
    if bm.is_empty() {
        Ok(DocIdSet::Empty)
    } else {
        Ok(DocIdSet::Bitmap(bm))
    }
}

/// A4 辅助：DocIdSet → 有序 docid Vec（Bitmap 升序；All/Empty 已由调用方处理语义）。
/// 供 JOIN/批量定位等需物化消费的场景使用；超大集安全阀由调用方（SORT_MAX_ROWS 等）承担。
pub fn docset_to_sorted(set: &crate::docset::DocIdSet) -> Vec<u64> {
    set.to_vec()
}



/// ORDER BY 候选集上限（防全库排序撑爆内存；超限报错提示 WHERE 收敛）。
const SORT_MAX_ROWS: usize = 200_000;

/// 排序键：Null(缺省/非数值非字符串) < Num < Str。
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SortKey {
    Null,
    Num(f64),
    Str(String),
}

/// 从文档 JSON 顶层字段取排序键（数值→Num，字符串→Str，其余/缺省→Null）。
/// P86② 单字段回退路径（`row_sort_keys` 无法轻量遍历时的 serde 正确性护栏）。
pub(crate) fn sort_key(doc: &[u8], field: &str) -> SortKey {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(doc) else {
        return SortKey::Null;
    };
    match v.get(field) {
        // 缺省 / JSON null / 非标量 → Null（排最后，MySQL 语义）。
        None | Some(serde_json::Value::Null) => SortKey::Null,
        Some(serde_json::Value::Number(n)) => n.as_f64().map(SortKey::Num).unwrap_or(SortKey::Null),
        Some(serde_json::Value::String(s)) => SortKey::Str(s.clone()),
        Some(_) => SortKey::Null,
    }
}

/// P86②：单遍字节级提取多个顶层排序键（跳其余 23 列 Value 构造与丢弃；对比逐字段
/// `sort_key` 每字段一次 serde 整行 parse）。结构无法轻量遍历（转义/嵌套 key/畸形）→
/// None，调用方回退 serde 逐字段（正确性护栏）。缺失字段 = Null（MySQL 语义）。
fn light_sort_keys(doc: &[u8], fields: &[&str]) -> Option<Vec<SortKey>> {
    let b = doc;
    let n = b.len();
    let mut i = ws(b, 0);
    if i >= n || b[i] != b'{' {
        return None;
    }
    i += 1;
    let mut out: Vec<Option<SortKey>> = vec![None; fields.len()];
    loop {
        i = ws(b, i);
        if i >= n {
            return None;
        }
        if b[i] == b'}' {
            break;
        }
        if b[i] != b'"' {
            return None;
        }
        i += 1;
        let ks = i;
        let mut esc = false;
        loop {
            if i >= n {
                return None;
            }
            let c = b[i];
            if c == b'\\' {
                esc = true;
                i += 2;
                continue;
            }
            i += 1;
            if c == b'"' {
                break;
            }
        }
        if esc {
            return None; // 转义 key → 回退 serde
        }
        let key = &b[ks..i - 1];
        i = ws(b, i);
        if i >= n || b[i] != b':' {
            return None;
        }
        i = ws(b, i + 1);
        if i >= n {
            return None;
        }
        if let Some(slot) = fields.iter().position(|f| f.as_bytes() == key) {
            let sk = match read_target_value(b, i)? {
                LightVal::Absent | LightVal::Complex | LightVal::Null | LightVal::Bool(_) => {
                    SortKey::Null
                }
                LightVal::Num(bytes) => match std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    Some(v) => SortKey::Num(v),
                    None => return None,
                },
                LightVal::Str(bytes) => match std::str::from_utf8(bytes) {
                    Ok(s) => SortKey::Str(s.to_string()),
                    Err(_) => return None,
                },
            };
            out[slot] = Some(sk);
        }
        // 消费掉整个值（目标字段的 read_target_value 未推进游标，skip_value 从值头扫过）
        if !skip_value(b, &mut i) {
            return None;
        }
        i = ws(b, i);
        if i >= n {
            return None;
        }
        if b[i] == b',' {
            i += 1;
        } else if b[i] == b'}' {
            break;
        } else {
            return None;
        }
    }
    Some(out.into_iter().map(|k| k.unwrap_or(SortKey::Null)).collect())
}

/// P86②：一行多排序键——单遍轻量提取；无法轻量 → 逐字段 serde 回退（语义等值）。
pub(crate) fn row_sort_keys(doc: &[u8], fields: &[String]) -> Vec<SortKey> {
    let refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
    if let Some(keys) = light_sort_keys(doc, &refs) {
        return keys;
    }
    fields.iter().map(|f| sort_key(doc, f)).collect()
}

/// P87②：字段 JSON 值字节 → 排序键（serde 解析标量；缺失/null/非标量 → Null）。
/// 与 `sort_key` 从整行取字段的语义等值（缺失与 JSON null 均 Null）。
/// P94②：字段值字节 → 排序键。输入为 JSON 标量原字节（colstore 列区域存的 `serde_json::to_vec`
/// 规范字节 / 行式 light 提取的原文 token）。快路径：数字直解 f64、无转义字符串去引号即比
/// （UTF-8 字节序 = 码点序），免去原实现的每字段一次 `serde_json::from_slice` 全量 JSON 解析
/// （P94 实测 100k 行 ×2 列 ~95ms → 该解析占 ~0.5µs/列）；畸形/转义/非标量回退 serde 保语义。
fn field_bytes_to_sort_key(b: &[u8]) -> SortKey {
    if b.is_empty() {
        return SortKey::Null;
    }
    let c = b[0];
    // 数字：'-' 或数字开头（JSON 无前导零/NaN/Inf，serde 同规则）→ 原字节直接 f64
    if c == b'-' || c.is_ascii_digit() {
        if let Ok(s) = std::str::from_utf8(b) {
            if let Ok(x) = s.parse::<f64>() {
                return SortKey::Num(x);
            }
        }
        // 畸形数字 → 落到 serde 兜底（保持 Null/语法语义一致）
    } else if c == b'"' && b.len() >= 2 {
        // 无转义（反斜杠）的简单字符串 → 去引号快路径（UTF-8 字节序比较等价码点序）
        let inner = &b[1..b.len() - 1];
        if !inner.contains(&b'\\') {
            if let Ok(s) = std::str::from_utf8(inner) {
                return SortKey::Str(s.to_string());
            }
        }
        // 含转义 → 落 serde 精确反解（防语义偏差）
    }
    // 其余（null/true/false/对象/数组/畸形）：serde 语义 = 非字符串非数值 → Null；含字符串兜底
    let v: Value = match serde_json::from_slice(b) {
        Ok(v) => v,
        Err(_) => return SortKey::Null,
    };
    match v {
        Value::Number(x) => x.as_f64().map(SortKey::Num).unwrap_or(SortKey::Null),
        Value::String(s) => SortKey::Str(s),
        _ => SortKey::Null,
    }
}

/// P85：DocIdSet 消费端 LIMIT 早停——候选 docid 分块（512）批量回表。复刻全量物化消费的
/// 精确语义：offset 跳过**候选位置**（墓碑/未命中也占 offset 占位），limit 只计**可见行**
/// （墓碑/未命中不占 limit）；产出 offset+limit 行即终止，剩余块零拉取。内存 O(chunk)。
/// `fetch` 抽象批量取行以便单测注入迭代计数（Bitmap/SortedList/All 三分支共用）。
pub(crate) fn collect_limited_rows<F>(
    mut fetch: F,
    docids: impl Iterator<Item = u64>,
    offset: u64,
    limit: u64,
) -> Result<Vec<QueryRow>>
where
    F: FnMut(&[u64]) -> Result<Vec<Option<Vec<u8>>>>,
{
    let mut rows: Vec<QueryRow> = Vec::new();
    if limit == 0 {
        return Ok(rows);
    }
    const CHUNK: usize = 512;
    let mut buf: Vec<u64> = Vec::with_capacity(CHUNK);
    let mut skipped = 0u64;
    let mut it = docids;
    loop {
        buf.clear();
        let mut got = 0usize;
        for _ in 0..CHUNK {
            match it.next() {
                Some(d) => {
                    buf.push(d);
                    got += 1;
                }
                None => break,
            }
        }
        if got == 0 {
            break;
        }
        let batch = fetch(&buf)?;
        for (d, v_opt) in buf.iter().copied().zip(batch.into_iter()) {
            if skipped < offset {
                skipped += 1;
                continue;
            }
            if rows.len() as u64 >= limit {
                break;
            }
            let Some(v) = v_opt else { continue };
            rows.push((d, v));
        }
        if rows.len() as u64 >= limit {
            break;
        }
    }
    Ok(rows)
}

fn cmp_sort_key(a: &SortKey, b: &SortKey) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (SortKey::Null, SortKey::Null) => Ordering::Equal,
        (SortKey::Null, _) => Ordering::Less,
        (_, SortKey::Null) => Ordering::Greater,
        (SortKey::Num(x), SortKey::Num(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (SortKey::Str(x), SortKey::Str(y)) => x.cmp(y),
        // 数值与字符串混合：数值视为更小（MySQL 同语义）。
        (SortKey::Num(_), SortKey::Str(_)) => Ordering::Less,
        (SortKey::Str(_), SortKey::Num(_)) => Ordering::Greater,
    }
}

struct SortRow {
    docid: u64,
    doc: Vec<u8>,
    keys: Vec<SortKey>,
}

/// Top-K 流式堆条目（P87①）：只存 docid + 排序键——候选扫描不再持有整行 JSON，
/// 峰值内存 O(k+chunk)；输出期对 top-K docid 整行回表（P87③）。
struct SortLite {
    docid: u64,
    keys: Vec<SortKey>,
}

/// P0-B：Top-K 有界堆排序——BinaryHeap 保持 top-K，内存 O(K) 替代 O(N) 全排序。
/// P87①流式化：候选 docid 分块（512）→ `batch_get_fields` 只解排序键列入堆；
/// P87②排序键解码下推：batch_get_fields 内部 PAX 块列解码 / 行式块按需字段提取；
/// P87③输出瘦身：top-K 确定后仅对胜出 docid 整行回表（SELECT * 语义），候选扫期间
/// 不物化整行（原实现全量 batch_get 110 万行 ≈1GB+ 峰值物化消除）。
/// P93：SortLite 排序键比较（order_by 字段序含 DESC 翻转；keys 与 order_by 逐位对齐）。
fn sortlite_cmp(a: &SortLite, b: &SortLite, order_by: &[(String, bool)]) -> std::cmp::Ordering {
    for (((_, desc), k1), k2) in order_by.iter().zip(&a.keys).zip(&b.keys) {
        let mut ord = cmp_sort_key(k1, k2);
        if *desc {
            ord = ord.reverse();
        }
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// P93：手动 top-K 堆（数组上浮/下沉，语义与 P92 原内联一致）——模块级纯函数，
/// 供并行分片 worker 与串行/稀疏路径共用（免闭包捕获线程约束）。
fn topk_heap_push(
    heap: &mut Vec<SortLite>,
    row: SortLite,
    k: usize,
    order_by: &[(String, bool)],
) {
    if heap.len() < k {
        heap.push(row);
        // 上浮
        let mut i = heap.len() - 1;
        while i > 0 {
            let parent = (i - 1) / 2;
            if sortlite_cmp(&heap[i], &heap[parent], order_by) == std::cmp::Ordering::Greater {
                heap.swap(i, parent);
                i = parent;
            } else {
                break;
            }
        }
    } else {
        // 堆满：比堆顶（最差）更好 → 替换
        if sortlite_cmp(&row, &heap[0], order_by) == std::cmp::Ordering::Less {
            heap[0] = row;
            // 下沉
            let mut i = 0;
            let n = heap.len();
            loop {
                let mut smallest = i;
                let l = 2 * i + 1;
                let r = 2 * i + 2;
                if l < n
                    && sortlite_cmp(&heap[l], &heap[smallest], order_by)
                        == std::cmp::Ordering::Greater
                {
                    smallest = l;
                }
                if r < n
                    && sortlite_cmp(&heap[r], &heap[smallest], order_by)
                        == std::cmp::Ordering::Greater
                {
                    smallest = r;
                }
                if smallest == i {
                    break;
                }
                heap.swap(i, smallest);
                i = smallest;
            }
        }
    }
}

pub(crate) fn topk_sort(
    engine: &Engine,
    bitmap: &RoaringBitmap,
    order_by: &[(String, bool)],
    k: usize,
    offset: u64,
    limit: u64,
    guard: &crate::watchdog::QueryGuard,
) -> Result<Vec<QueryRow>> {
    // 简化：直接用 Vec + 手动管理 top-K（比较/堆逻辑见模块级 sortlite_cmp / topk_heap_push）
    let fields: Vec<String> = order_by.iter().map(|(f, _)| f.clone()).collect();
    let mut heap: Vec<SortLite> = Vec::with_capacity(k + 1);
    let mut scanned = 0u64;
    // P92：候选**稠密**（窗口跨度 ≤ 4× 候选数）→ 投影列流式窗口扫描替代逐 docid
    // 点查定位（P87①/② 的点查 batch_get_fields ~11µs/docid 是 #29 13.5s 残余瓶颈：
    // 升序稠密 docid 逐点回表仍按 key 二分定位每行）。流式扫描经 scan_stream_fields
    // 顺序读块、PAX 列解码/行式按需只解排序键列；非候选/墓碑行跳过，语义与点查等价
    // （点查对不可见候选返回 None 跳过 ⇔ 扫描对墓碑/删除行不产出）。
    // 注：扫描语义与 execute 聚合/分组全扫一致——不经 HotCache/delta 覆盖（字段级
    // 热补丁在排序键上的可见性取舍同既有 scan 路径）。
    let len = bitmap.len() as u64;
    let dense = len > 0
        && match (bitmap.min(), bitmap.max()) {
            (Some(lo), Some(hi)) => {
                let span = (hi as u64) - (lo as u64) + 1;
                span <= len * 4
            }
            _ => false,
        };
    if dense {
        let lo = bitmap.min().unwrap() as u64;
        let hi = bitmap.max().unwrap() as u64;
        // P94：colstore 热列旁路优先——排序键 ∈ 热列时只解目标列（列 IO -90%+，免整块解压）；
        // 命中返回 Ok(true)；未命中（未启用/非热列/超水位/脏区间）回退下方行式扫描，语义不变。
        let mut served = false;
        if engine.colstore_enabled() {
            if let Some(idxs) = engine.colstore_field_indices(&fields) {
                served = engine.colstore_try_scan_cols(lo, hi, |docid, row| {
                    // 与下方行式分支一致：稠密区间内的非候选洞须剔除（dense = span≤4×len，非满区间）
                    if !bitmap.contains(docid) {
                        return Ok(true);
                    }
                    scanned += 1;
                    if guard.is_expired() {
                        return Err(Error::QueryTooExpensive(format!(
                            "Top-K 排序超时（colstore 熔断中止）"
                        )));
                    }
                    let mut keys: Vec<SortKey> = Vec::with_capacity(idxs.len());
                    for &ci in &idxs {
                        keys.push(match row.field(ci) {
                            Some(b) => field_bytes_to_sort_key(b),
                            None => SortKey::Null,
                        });
                    }
                    topk_heap_push(&mut heap, SortLite { docid, keys }, k, order_by);
                    Ok(true)
                })?;
            }
        }
        if !served {
            // P93：大候选稠密 top-K → [lo..hi] 按 docid 等分子窗**并行**投影扫描，每片独立
            // 维护局部 top-K 堆，全局 top-K = 各片局部堆并集的 top-K（经典正确性：片外淘汰的
            // 行必不可能进全局 top-K）。针对 #29 全表 ORDER BY LIMIT：110 万行整表整块
            // 解压 + 逐行 JSON 解码是 CPU/IO 双瓶颈，串行单核受限；并行后解压/解码摊到多核
            // （块缓存热时近似 CPU-bound → ~核心数倍收益）。阈值 20 万行避免小库线程开销。
            // 候选/单核：默认**串行**（P93-110万实测：并行分片扫描在块缓存 put 速率超淘汰时内存
            // 超预算膨胀 + 多核利用率不足，未达验收前默认关闭；P93_PARALLEL=1 显式开启实验）。
            let span = hi - lo + 1;
            let nw_avail = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
            let big = std::env::var_os("P93_PARALLEL").is_some()
                && len >= 200_000
                && nw_avail >= 2
                && span / (k.max(1) as u64) >= nw_avail as u64;
            if big {
                let nw = (nw_avail as u64).min(16);
                let step = span / nw;
                std::thread::scope(|s| -> Result<()> {
                    let mut handles: Vec<std::thread::ScopedJoinHandle<Result<Vec<SortLite>>>> =
                        Vec::with_capacity(nw as usize);
                    for i in 0..nw {
                        let s0 = lo + i * step;
                        let e0 = if i + 1 == nw {
                            hi
                        } else {
                            lo + (i + 1) * step - 1
                        };
                        if e0 < s0 {
                            continue;
                        }
                        let fields = fields.clone();
                        handles.push(s.spawn(move || -> Result<Vec<SortLite>> {
                            let mut local: Vec<SortLite> = Vec::with_capacity(k + 1);
                            engine.scan_stream_fields(Some(s0), Some(e0), fields.clone(), |docid, doc| {
                                if !bitmap.contains(docid) {
                                    return Ok(true);
                                }
                                if guard.is_expired() {
                                    return Err(Error::QueryTooExpensive(format!(
                                        "Top-K 排序超时（并行分片熔断中止）"
                                    )));
                                }
                                let keys = row_sort_keys(doc, &fields);
                                topk_heap_push(&mut local, SortLite { docid, keys }, k, order_by);
                                Ok(true)
                            })?;
                            Ok(local)
                        }));
                    }
                    for h in handles {
                        let local = h
                            .join()
                            .map_err(|_| Error::QueryTooExpensive("Top-K worker panic".into()))??;
                        for row in local {
                            topk_heap_push(&mut heap, row, k, order_by);
                        }
                    }
                    Ok(())
                })?;
                scanned = len;
            } else {
                // 小候选/单核：原串行稠密投影流式扫描
                engine.scan_stream_fields(Some(lo), Some(hi), fields.clone(), |docid, doc| {
                    if !bitmap.contains(docid) {
                        return Ok(true);
                    }
                    scanned += 1;
                    // 看门狗逐行检查（原子，开销可忽略）：保 P87 熔断语义（首个候选即中止）
                    if guard.is_expired() {
                        return Err(Error::QueryTooExpensive(format!(
                            "Top-K 排序超时（已扫 {scanned} 条，熔断中止）"
                        )));
                    }
                    let keys = row_sort_keys(doc, &fields);
                    topk_heap_push(&mut heap, SortLite { docid, keys }, k, order_by);
                    Ok(true)
                })?;
            }
        }
    } else {
        // P87①：分块流式扫描（Roaring iter 惰性；产出 top-K 即停，块内 watchguard 熔断）
        const CHUNK: usize = 512;
        let mut chunk: Vec<u64> = Vec::with_capacity(CHUNK);
        let mut it = bitmap.iter();
        loop {
            chunk.clear();
            let mut got = 0usize;
            for _ in 0..CHUNK {
                match it.next() {
                    Some(d) => {
                        chunk.push(d);
                        got += 1;
                    }
                    None => break,
                }
            }
            if got == 0 {
                break;
            }
            scanned += got as u64;
            if guard.is_expired() {
                return Err(Error::QueryTooExpensive(format!(
                    "Top-K 排序超时（已扫 {scanned} 条，熔断中止）"
                )));
            }
            // P87②：只取排序键列（PAX 列解码 / 行式按需字段提取），免整行物化
            let fvals = engine.batch_get_fields(&chunk, &fields)?;
            for (docid, f_opt) in chunk.iter().copied().zip(fvals.into_iter()) {
                let Some(vals) = f_opt else { continue };
                let keys: Vec<SortKey> = vals
                    .iter()
                    .map(|b| match b {
                        Some(bytes) => field_bytes_to_sort_key(bytes),
                        None => SortKey::Null,
                    })
                    .collect();
                topk_heap_push(&mut heap, SortLite { docid, keys }, k, order_by);
            }
        }
    }
    // 排序 top-K → 输出切片 docid（skip offset / take limit）
    heap.sort_by(|a, b| sortlite_cmp(a, b, order_by));
    let win: Vec<u64> = heap
        .into_iter()
        .skip(offset as usize)
        .take(limit as usize)
        .map(|r| r.docid)
        .collect();
    if win.is_empty() {
        return Ok(Vec::new());
    }
    // P87③：仅对胜出行整行回表（SELECT * 消费端需完整文档）
    let batch = engine.batch_get(&win)?;
    let mut out = Vec::new();
    for (d, v_opt) in win.into_iter().zip(batch.into_iter()) {
        let Some(v) = v_opt else { continue };
        out.push((d, v));
    }
    Ok(out)
}

/// b：事务覆盖视图谓词复检——对单个文档判断其（含同事务写后的）JSON 是否命中 SQL 的
/// WHERE 条件。sql 形如 `SELECT … FROM t WHERE <cond>`（仅使用 where_expr；无 WHERE → true）。
/// 文档 JSON 解析失败 / 谓词解析失败 → false（对齐求值端语义：字段缺失不命中）。
pub fn doc_matches_where(sql: &str, doc: &[u8]) -> bool {
    let sel = match parse_select(sql) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let we = match sel.where_expr {
        Some(e) => e,
        None => return true,
    };
    match serde_json::from_slice::<serde_json::Value>(doc) {
        Ok(v) => we.matches_doc(&v),
        Err(_) => false,
    }
}

/// DISTINCT 键单列规范化：None（缺列/JSON null）同组；数字开头 token 按 f64 **值**归组
/// （1 与 1.0 同组、-0.0 与 0.0 同组）；其余（字符串带引号/布尔/容器原文）按原文字节归组。
/// 前缀防跨类碰撞（数字 "1" 与字符串 "\"1\"" 不同组）。
fn distinct_part(b: Option<&[u8]>) -> String {
    match b {
        None => "\u{0}null".to_string(),
        Some(raw) if !raw.is_empty() && (raw[0] == b'-' || raw[0].is_ascii_digit()) => {
            match std::str::from_utf8(raw).ok().and_then(|s| s.parse::<f64>().ok()) {
                Some(v) => format!("\u{1}{}", if v == 0.0 { 0u64 } else { v.to_bits() }),
                None => format!("\u{2}{}", String::from_utf8_lossy(raw)),
            }
        }
        Some(raw) => format!("\u{3}{}", String::from_utf8_lossy(raw)),
    }
}

/// SELECT DISTINCT 行去重执行（2026-09-05 立项；首版形态）。
/// 逻辑顺序对齐 MySQL：WHERE 收敛候选 → 按显式列组合值**去重** → ORDER BY（列 ⊆ 列清单）→
/// OFFSET/LIMIT。语义护栏：缺列与 JSON null 同组；数值按值归组（1 与 1.0 同组）；
/// 去重后代表行保留首见 docid（候选位图升序 → 输出确定）。
/// 内存护栏：去重组数 > SORT_MAX_ROWS → QueryTooExpensive（建议 WHERE 收敛/按键分页）；
/// 看门狗逐块熔断。parser 已 1064 组合限制（* / 聚合 / GROUP BY / HAVING / JOIN）。
pub(crate) fn execute_distinct(
    engine: &Engine,
    sel: &Select,
    guard: &crate::watchdog::QueryGuard,
) -> Result<Vec<QueryRow>> {
    // 候选集：全量（去重前不可提前截断——后续候选可能仍是未见组合或重复值）。
    let set = get_docid_set(engine, sel.where_expr.as_ref(), None, guard)?;
    let bitmap = match &set {
        crate::docset::DocIdSet::Bitmap(b) => b.clone(),
        crate::docset::DocIdSet::Empty => return Ok(Vec::new()),
        crate::docset::DocIdSet::SortedList(v) => {
            let mut b = RoaringBitmap::new();
            for &d in v {
                b.insert(d);
            }
            b
        }
        crate::docset::DocIdSet::All => match engine.colstore_all_bitmap() {
            Some(b) => b,
            None => full_docids(engine, guard)?,
        },
    };
    let fields: Vec<String> = sel.columns.clone();
    let order_fields: Vec<String> = sel.order_by.iter().map(|(f, _)| f.clone()).collect();
    let mut rep: HashMap<Vec<String>, u64> = HashMap::new(); // 去重键 → 代表 docid
    let mut first_seen: Vec<u64> = Vec::new(); // 候选升序保序输出
    let mut scanned = 0u64;
    const CHUNK: usize = 512;
    let mut buf: Vec<u64> = Vec::with_capacity(CHUNK);
    let mut it = bitmap.iter();
    loop {
        buf.clear();
        let mut got = 0usize;
        for _ in 0..CHUNK {
            match it.next() {
                Some(d) => {
                    buf.push(d);
                    got += 1;
                }
                None => break,
            }
        }
        if got == 0 {
            break;
        }
        scanned += got as u64;
        if guard.is_expired() {
            return Err(Error::QueryTooExpensive(format!(
                "SELECT DISTINCT 扫描超时（已扫 {scanned} 条，熔断中止）"
            )));
        }
        let batch = engine.batch_get(&buf)?;
        for (d, v_opt) in buf.iter().copied().zip(batch.into_iter()) {
            let Some(doc) = v_opt else { continue };
            let vals = crate::engine::colstore::light_top_fields(&doc, &fields)
                .unwrap_or_else(|| vec![None; fields.len()]);
            let key: Vec<String> = vals.iter().map(|b| distinct_part(*b)).collect();
            if !rep.contains_key(&key) {
                if rep.len() >= SORT_MAX_ROWS {
                    return Err(Error::QueryTooExpensive(format!(
                        "SELECT DISTINCT 去重组数超上限 {}（建议 WHERE 收敛或按键分页）",
                        SORT_MAX_ROWS
                    )));
                }
                rep.insert(key, d);
                first_seen.push(d);
            }
        }
    }
    if first_seen.is_empty() {
        return Ok(Vec::new());
    }
    let limit = sel.limit.unwrap_or(u64::MAX);
    // DISTINCT → ORDER BY：按代表行排序（parser 已保证排序列 ⊆ 列清单）
    let mut win: Vec<u64> = first_seen;
    if !order_fields.is_empty() {
        let docs = engine.batch_get(&win)?;
        let mut items: Vec<(u64, Vec<SortKey>)> = Vec::with_capacity(win.len());
        for (d, v_opt) in win.iter().copied().zip(docs.into_iter()) {
            let Some(doc) = v_opt else { continue };
            items.push((d, row_sort_keys(&doc, &order_fields)));
        }
        items.sort_by(|a, b| {
            for (((_, desc), k1), k2) in sel.order_by.iter().zip(&a.1).zip(&b.1) {
                let mut ord = cmp_sort_key(k1, k2);
                if *desc {
                    ord = ord.reverse();
                }
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
        win = items.into_iter().map(|(d, _)| d).collect();
    }
    let out_ids: Vec<u64> = win
        .into_iter()
        .skip(sel.offset as usize)
        .take(limit as usize)
        .collect();
    if out_ids.is_empty() {
        return Ok(Vec::new());
    }
    let batch = engine.batch_get(&out_ids)?;
    let mut out = Vec::new();
    for (d, v_opt) in out_ids.into_iter().zip(batch.into_iter()) {
        let Some(doc) = v_opt else { continue };
        out.push((d, doc));
    }
    Ok(out)
}

/// 执行类 SQL：解析 + 求值 + 回表 + LIMIT/OFFSET（`cap` 为无 LIMIT 时的上限保护）。
/// 看门狗：扫描过滤/回表逐批熔断（超时返回 QueryTooExpensive，不挂起 server）。
pub fn execute(engine: &Engine, sql: &str, cap: u64) -> Result<Vec<QueryRow>> {
    let sel = parse_select(sql)?;
    if !sel.group_by.is_empty() {
        return Err(Error::Config(
            "GROUP BY 查询须经分组执行入口 execute_group_by".into(),
        ));
    }
    // SELECT DISTINCT（2026-09-05 立项）：行去重独立路径（parser 已限组合形态）——
    // 在组合索引/JOIN 等路由之前（去重须作用于全候选，不做集合裁剪）。
    if sel.distinct {
        let guard = engine.query_guard();
        return execute_distinct(engine, &sel, &guard);
    }
    // P0-D：JOIN 路由——有 JOIN 子句时走 execute_join（参考 research/optimizer_proces.md 阶段 2）。
    // review 修复（2026-09-04）：须在组合索引之前——若主表 WHERE 命中 composite_indexes，
    // 组合索引会返回纯主表行并把 JOIN 静默丢弃（错结果）。
    if sel.join.is_some() {
        return execute_join(engine, &sel, cap);
    }
    // P0-A：声明式组合索引路由——WHERE 等值前缀匹配 composite_indexes 时走 cidx 前缀扫描。
    // 匹配规则：提取 WHERE 中所有等值条件 → 按 composite_indexes 最左前缀匹配 → 取最长匹配。
    if let Some(rows) = try_composite_index(engine, &sel, cap)? {
        return Ok(rows);
    }
    let guard = engine.query_guard();
    let limit = sel.limit.unwrap_or(cap).min(cap);
    let sort = !sel.order_by.is_empty();

    // P4-C：基于代价的优化器——多条件 AND 时评估倒排 vs 全扫 + Zone Map 剪枝，
    // 选择最优路径。当倒排选择性低（doc_count 占比大）时优先走全扫。
    if !sort && engine.cost_based_enabled {
        if let Some(we) = sel.where_expr.as_ref() {
            if let Some(leaf) = scan_leaf(we) {
                // 范围/BETWEEN 条件：评估全扫代价是否低于倒排
                let eq_conds = extract_eq_conds(we);
                let mut eq_terms: Vec<(String, Option<u64>)> = Vec::new();
                for (f, v) in &eq_conds {
                    let term = format!("{f}={v}");
                    // engine 是 &Engine（不可变），用 cost_route 提供的 doc_count_fast 查询
                    let count = engine.inverted.doc_count_fast(&term)
                        .ok()
                        .flatten();
                    eq_terms.push((term, count));
                }
                let range_fields: Vec<String> = match &leaf {
                    Leaf::Cmp(c) => vec![c.field.clone()],
                    Leaf::Between { field, .. } => vec![field.to_string()],
                    _ => vec![],
                };
                let zone_fields: Vec<String> = Vec::new();
                let total_rows = engine.estimated_total_rows();
                let best = crate::optimizer::choose_best_plan(
                    &eq_terms, &range_fields, &engine.cost_params,
                    total_rows, &zone_fields,
                );
                if best.path == crate::optimizer::AccessPath::FullScan {
                    // 全扫更优 → 走 scan_pushdown（流式扫描 + Zone Map 剪枝）
                    return scan_pushdown(engine, &leaf, limit, sel.offset, &guard);
                }
                // 倒排更优 → 继续走原路径
            }
        }
    }

    // 7.94 等值回退：裸 `field=value` 倒排 term 未命中（数字等值/未索引字段）→
    // 单遍流式扫描 + LIMIT/OFFSET 早停（组合 AND/OR/NOT 内回退走 eval_cond 全量集）。
    // 含 ORDER BY 时不走早停快速路径（需完整候选集排序）。
    if !sort {
        if let Some(WhereExpr::Cond(c)) = sel.where_expr.as_ref() {
            if matches!(c.op, CmpOp::Eq) && c.field != "docid" {
                let hit = engine.inverted_posting(&format!("{}={}", c.field, c.value))?;
                if hit.is_empty() {
                    return scan_pushdown(engine, &Leaf::Cmp(c), limit, sel.offset, &guard);
                }
            }
        }
        // 谓词下推（7.93）：WHERE 为裸比较/BETWEEN（无倒排等值可收敛）→ 单遍流式扫描 +
        // LIMIT/OFFSET 早停直接产出命中行（不再全量收集 docid 再逐行回表 get）。
        if let Some(leaf) = sel.where_expr.as_ref().and_then(scan_leaf) {
            return scan_pushdown(engine, &leaf, limit, sel.offset, &guard);
        }
    }
    // A5：WHERE → 统一 DocIdSet（get_docid_set = eval 形态包装，AND 快路径/LIKE 等语义继承）。
    // limit 语义对齐原路径：sort 分支 eval cap = SORT_MAX_ROWS+offset+limit（防全量物化巨大
    // 候选）；非 sort 分支 eval cap = limit（post_filter/scan_all 找到 limit 即停）。
    // - 有 WHERE → Bitmap/Empty；
    // - 无 WHERE → All（全库：消费端物化为全量 docid，语义同 full_docids）。
    let cap = if sort {
        SORT_MAX_ROWS as u64 + sel.offset + limit
    } else {
        limit
    };
    let set = get_docid_set(engine, sel.where_expr.as_ref(), Some(cap), &guard)?;
    if sort {
        // sort 分支：DocIdSet → 位图物化（All 需全库 docid；Bitmap/Empty 直接）
        let bitmap = match &set {
            crate::docset::DocIdSet::Bitmap(bm) => bm.clone(),
            crate::docset::DocIdSet::Empty => RoaringBitmap::new(),
            crate::docset::DocIdSet::All => {
                // P94③：colstore 已派生且覆盖全表（无脏/无超水位新行）→ cs docid 位图直供，
                // 免 full_docids primary 全扫物化整行（All→大窗 ORDER BY 的主开销）；否则原路径。
                match engine.colstore_all_bitmap() {
                    Some(b) => b,
                    None => full_docids(engine, &guard)?,
                }
            }
            crate::docset::DocIdSet::SortedList(v) => {
                let mut b = RoaringBitmap::new();
                for &d in v {
                    b.insert(d);
                }
                b
            }
        };
        // P0-B：Top-K 有界堆——有 LIMIT 时用 BinaryHeap 保持 top-K（LIMIT+OFFSET），
        // 内存 O(K) 替代 O(N) 全排序；无 LIMIT 时回退原全排序（有 SORT_MAX_ROWS 守卫）。
        let k = sel.offset + limit;
        if sel.limit.is_some() && k > 0 {
            // A7：Top-K 堆内存守卫——k = offset+limit 超 SORT_MAX_ROWS 时堆膨胀无界
            // （对齐设计 6.2：深分页 keyset pagination 建议，拒绝防 OOM）。
            if k as usize > SORT_MAX_ROWS {
                return Err(Error::QueryTooExpensive(format!(
                    "ORDER BY OFFSET {} + LIMIT {} = {} 超过 Top-K 上限 {}，\
                     建议用 keyset 分页（WHERE id > last_id ORDER BY id LIMIT N）",
                    sel.offset,
                    limit,
                    k,
                    SORT_MAX_ROWS
                )));
            }
            return topk_sort(engine, &bitmap, &sel.order_by, k as usize, sel.offset, limit, &guard);
        }
        // 无 LIMIT：回退原全排序路径（守卫不变）
        if bitmap.len() as usize > SORT_MAX_ROWS {
            return Err(Error::QueryTooExpensive(format!(
                "ORDER BY 候选集过大（{} 行，上限 {}），请加 WHERE 收敛或用 LIMIT",
                bitmap.len(),
                SORT_MAX_ROWS
            )));
        }
        // P2-D：batch_get 批量取行替代逐行 get
        let order_fields: Vec<String> = sel.order_by.iter().map(|(f, _)| f.clone()).collect();
        let docids: Vec<u64> = bitmap.iter().map(|d| d as u64).collect();
        let batch = engine.batch_get(&docids)?;
        let mut srows: Vec<SortRow> = Vec::with_capacity(docids.len());
        for (docid, v_opt) in docids.into_iter().zip(batch.into_iter()) {
            let Some(v) = v_opt else { continue };
            // P86②：单遍按需提取全部排序键（跳其余列 Value 构造；逐字段 serde parse 改为一次轻量扫）
            let keys = row_sort_keys(&v, &order_fields);
            srows.push(SortRow { docid, doc: v, keys });
        }
        srows.sort_by(|a, b| {
            for (((f, desc), k1), k2) in sel.order_by.iter().zip(&a.keys).zip(&b.keys) {
                let mut ord = cmp_sort_key(k1, k2);
                if *desc {
                    ord = ord.reverse();
                }
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
        let mut out = Vec::new();
        for r in srows.into_iter().skip(sel.offset as usize) {
            if out.len() as u64 >= limit {
                break;
            }
            out.push((r.docid, r.doc));
        }
        return Ok(out);
    }
    // 非 sort 分支：DocIdSet 分块迭代消费（P85：LIMIT 早停——Bitmap/SortedList/All
    // 三分支只回表 offset+limit 行即终止，剩余块零拉取；内存 O(chunk)，消除
    // "22 万 posting 全量 batch_get 解码后才切片" 的 LIMIT 未下推瓶颈）。
    let mut rows = Vec::new();
    if !set.is_empty() {
        let it: Box<dyn Iterator<Item = u64>> = match &set {
            crate::docset::DocIdSet::All => {
                let bm = full_docids(engine, &guard)?;
                let v: Vec<u64> = bm.iter().collect();
                Box::new(v.into_iter())
            }
            other => other.iter(),
        };
        rows = collect_limited_rows(|chunk| engine.batch_get(chunk), it, sel.offset, limit)?;
    }
    Ok(rows)
}
