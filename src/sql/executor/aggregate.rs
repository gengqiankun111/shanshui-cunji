//! 标量聚合执行（原 sqlish.rs 7.95/P1-C/P1-D/P90/P91 段）：
//! `execute_aggregate_window`/`execute_aggregate` → `AggScalar` 单行单列。
//! 快路径：倒排统计载荷 / PAX 块级 zones / posting 候选收敛按需解列；回退全量扫描。
//! `aggregate_needed_fields`（引用列提取）与 `fmt_num` 供 group_by 复用。

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::sql::parser::{parse_select, CmpOp, WhereExpr};
use roaring::treemap::RoaringTreemap as RoaringBitmap;
use serde_json::Value;
use std::collections::HashSet;

use super::eval::{field_of, light_top_field, light_where_matches, LightVal};
use super::select::extract_eq_conds;

/// 聚合标量结果（7.95）：列头 + 值（is_null 时 text 忽略）。
pub struct AggScalar {
    /// 列头（函数原样串，如 `COUNT(*)` / `AVG(amount)`）。
    pub header: String,
    /// SQL NULL（空集 SUM/AVG/MIN/MAX；COUNT 恒 0 不 NULL）。
    pub is_null: bool,
    /// 数值文本（COUNT 整数；SUM/AVG/MIN/MAX 数字，整值无小数点）。
    pub text: String,
}

/// 聚合执行（7.95）：`SELECT COUNT(*)/COUNT(f)/SUM(f)/AVG(f)/MIN(f)/MAX(f) ... WHERE <expr>`
/// → 单行单列标量；非聚合 SQL 返回 Ok(None)（调用方走普通查询）。
///
/// **全量单遍扫描**（WhereExpr::matches_doc 行级过滤）——聚合必须精确，不依赖倒排完整性
/// （等值 posting 可能只覆盖增量写入）；与 MySQL 无索引聚合同语义，看门狗预算保护。
/// COUNT(f) = 字段存在且非 JSON null（任意类型）；SUM/AVG/MIN/MAX 只统计数值字段行
/// （非数值行跳过；数值按 f64 累加——大整数超 2^53 精度受限，标注限制）。
/// P1-3：带 docid 区间窗口的标量聚合（非默认表按表区间执行；`start/end` 均为 None = 全库）。
/// 窗口非空时禁用倒排统计快路径与 `count_all_docs` 快路径（两者为引擎全库口径，跨表会
/// 串表）——强制窗口内全扫，语义与按表区间聚合一致。

/// Task-021：整表 docid 窗口判定——窗口恰好覆盖某表全区间 `[t<<48, (t<<48)+2^48-1]`
/// 时返回 `(start, end)`；部分窗口/半开窗口 → None（保持 keys-only 线性扫）。
fn full_table_window(start: Option<u64>, end: Option<u64>) -> Option<(u64, u64)> {
    let s = start?;
    let e = end?;
    if e < s {
        return None;
    }
    let span: u64 = 1u64 << 48;
    if s % span != 0 {
        return None;
    }
    let table_max = s.checked_add(span - 1)?;
    if e == table_max {
        Some((s, e))
    } else {
        None
    }
}

pub fn execute_aggregate_window(
    engine: &Engine,
    sql: &str,
    start: Option<u64>,
    end: Option<u64>,
) -> Result<Option<AggScalar>> {
    let scoped = start.is_some() || end.is_some();
    let sel = parse_select(sql)?;
    if !sel.group_by.is_empty() {
        // GROUP BY 走 execute_group_by（多行分组），非本标量入口。
        return Err(Error::Config(
            "GROUP BY 查询须经分组执行入口 execute_group_by".into(),
        ));
    }
    let Some((name, field)) = sel.agg.clone() else {
        return Ok(None);
    };
    if field.is_none() && name != "count" {
        return Err(Error::Config(format!("{name}(*) 不支持（仅 COUNT(*)）")));
    }
    // Task-030：`COUNT(DISTINCT f)` 去重计数——独立分支先行（不经过 COUNT 各类快路径，
    // 语义 = 窗口内 WHERE 过滤后字段非 null 去重值数；数值按规范化 f64、字符串原样）。
    if sel.agg_distinct {
        let f = field.clone().ok_or_else(|| {
            Error::Config("COUNT(DISTINCT) 需字段参数".into())
        })?;
        let header = format!("COUNT(DISTINCT {f})");
        let guard = engine.query_guard();
        let needed = aggregate_needed_fields(sel.where_expr.as_ref(), Some(&f));
        let mut seen: HashSet<(u8, String)> = HashSet::with_capacity(1024);
        let mut scanned = 0u64;
        engine.scan_stream_fields(start, end, needed, |_docid, doc| {
            scanned += 1;
            if scanned % 4096 == 0 && guard.is_expired() {
                return Err(Error::QueryTooExpensive(
                    "COUNT(DISTINCT) 全量扫描超时（熔断中止）".into(),
                ));
            }
            if let Some(wh) = &sel.where_expr {
                let hit = match light_where_matches(doc, wh) {
                    Some(r) => r,
                    None => serde_json::from_slice::<Value>(doc)
                        .map(|v| wh.matches_doc(&v))
                        .unwrap_or(false),
                };
                if !hit {
                    return Ok(true);
                }
            }
            if let Some(k) = distinct_key_of(doc, &f) {
                seen.insert(k);
            }
            Ok(true)
        })?;
        return Ok(Some(AggScalar {
            header,
            is_null: false,
            text: seen.len().to_string(),
        }));
    }
    if field.is_none() && sel.where_expr.is_none() {
        // Task-021：整表 docid 窗口（覆盖某表全区间）COUNT(*) 无 WHERE → 引擎活跃 docid
        // 区间基数 O(1)（引擎活跃集 docid 高 16 位即表号，区间计数天然单表隔离），
        // 替代窗口 keys-only 线性扫（10 万 42.8ms / 110 万 ~407ms → µs 级）。
        if let Some((ws, we)) = full_table_window(start, end) {
            let n = engine.count_docs_range(ws, we)?;
            return Ok(Some(AggScalar {
                header: "COUNT(*)".into(),
                is_null: false,
                text: n.to_string(),
            }));
        }
        if scoped {
            // COUNT(*) 无 WHERE（表区间版，非整表窗口）：docid 窗口 keys-only 计数（免文档值反序列化）
            let mut n = 0u64;
            engine.scan_stream_ids(start, end, |_| {
                n += 1;
                Ok(true)
            })?;
            return Ok(Some(AggScalar {
                header: "COUNT(*)".into(),
                is_null: false,
                text: n.to_string(),
            }));
        }
        // 7.100：COUNT(*) 无 WHERE → 引擎 key-only 流式计数（免文档值反序列化）。
        // 语义与全表扫描 COUNT 一致（同 key 最新版本、Tombstone 跳过）。
        let n = engine.count_all_docs()?;
        return Ok(Some(AggScalar {
            header: "COUNT(*)".into(),
            is_null: false,
            text: n.to_string(),
        }));
    }
    // P1-C：COUNT(*) WHERE field='value'（裸倒排等值）→ posting 长度直接计数，免全扫。
    // 仅无窗口、裸等值条件（无 AND/OR/NOT）时启用；多条件回落全扫。
    if !scoped && name == "count" && field.is_none() {
        if let Some(WhereExpr::Cond(c)) = sel.where_expr.as_ref() {
            if c.op == CmpOp::Eq && c.field != "docid" {
                let term = format!("{}={}", c.field, c.value);
                let posting = engine.inverted_posting(&term)?;
                return Ok(Some(AggScalar {
                    header: "COUNT(*)".into(),
                    is_null: false,
                    text: posting.len().to_string(),
                }));
            }
        }
    }
    // Ex-9.3 第③步：`SUM/AVG/MIN/MAX(stats_field) ... WHERE f='v'`（裸等值、无排序/分组）
    // → 倒排 term 统计载荷免全扫（内存累积 + v5 段载荷；仅 stats_fields 声明字段可路由；
    // 未命中（未声明/term 无统计/多条件）→ 回落既有全量扫描，结果语义不变）。
    // P1-3：表区间窗口禁用（倒排为引擎全库口径，跨表会串表）。
    if !scoped && matches!(name.as_str(), "sum" | "avg" | "min" | "max") {
        if let (Some(f), Some(WhereExpr::Cond(c))) = (field.as_ref(), sel.where_expr.as_ref()) {
            if c.op == CmpOp::Eq
                && c.field != "docid"
                && sel.order_by.is_empty()
                && sel.limit.is_none()
                && engine.stats_field_pos(f).is_some()
            {
                let term = format!("{}={}", c.field, c.value);
                if let Some(st) = engine.inverted_term_stats(&term) {
                    if let Some(pos) = engine.stats_field_pos(f) {
                        if let Some(a) = st.get(pos) {
                            if a.n > 0 {
                                let text = match name.as_str() {
                                    "sum" => fmt_num(a.sum),
                                    "avg" => fmt_num(a.sum / a.n as f64),
                                    "min" => fmt_num(a.min),
                                    "max" => fmt_num(a.max),
                                    _ => unreachable!(),
                                };
                                let arg = field.as_deref().unwrap_or("*");
                                let header = format!("{}({arg})", name.to_uppercase());
                                return Ok(Some(AggScalar { header, is_null: false, text }));
                            }
                            // 子集内无数值行 → SQL NULL（与全扫一致），仍走快路径
                            let arg = field.as_deref().unwrap_or("*");
                            let header = format!("{}({arg})", name.to_uppercase());
                            return Ok(Some(AggScalar { header, is_null: true, text: String::new() }));
                        }
                    }
                }
            }
        }
    }
    // P90：PAX 块级聚合下推——无 WHERE / 无排序 / 无 LIMIT 的 `SUM(f)`/`COUNT(f)`：
    // 引擎快照 eligible（memtable/delta/删除位图/活跃快照全空 + 单一非空层 + 全 PAX 块）时
    // 用块级 zones（sum/present/null）直接出数（跳过数据块读取与 25 列解码）。
    // AVG/MIN/MAX 及零和 SUM（zones 无法区分"纯数值零和"与"含非数值行"，见 encode_pax_block）
    // 回退行级精确扫描（语义不变）。
    if sel.where_expr.is_none() && sel.order_by.is_empty() && sel.limit.is_none() {
        if let Some(f) = field.as_ref() {
            if matches!(name.as_str(), "count" | "sum") {
                if let Some((zsum, present, nulls)) = engine.zone_field_aggregate(start, end, f)? {
                    let arg = f.as_str();
                    let header = format!("{}({arg})", name.to_uppercase());
                    match name.as_str() {
                        "count" => {
                            return Ok(Some(AggScalar {
                                header,
                                is_null: false,
                                text: (present - nulls).to_string(),
                            }));
                        }
                        _ => {
                            if zsum != 0.0 {
                                return Ok(Some(AggScalar {
                                    header,
                                    is_null: false,
                                    text: fmt_num(zsum),
                                }));
                            }
                            // zsum==0：零和 / 列非数值不可判定 → 落行级保证 NULL/0 精确
                        }
                    }
                }
            }
        }
    }
    let guard = engine.query_guard();
    let mut count = 0u64;
    let mut n_num = 0u64;
    let mut sum = 0f64;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    // 单文档：WHERE 整表达式判定（字节级 light，含点路径回退 serde）+ 聚合累积。
    // 全表扫与 P1-D 候选集共用同一累积逻辑（结果精确一致）。
    let mut acc = |doc: &[u8]| -> Result<bool> {
        if let Some(wh) = &sel.where_expr {
            // 7.96/7.97：表达式（含 AND/OR/NOT 复合）字节级 light 判定；
            // 含点路径/转义等无法轻量 → serde 回退
            let hit = match light_where_matches(doc, wh) {
                Some(r) => r,
                None => serde_json::from_slice::<Value>(doc)
                    .map(|v| wh.matches_doc(&v))
                    .unwrap_or(false),
            };
            if !hit {
                return Ok(true);
            }
        }
        if field.is_none() {
            count += 1; // COUNT(*)：无需解析 doc
            return Ok(true);
        }
        let f = field.as_ref().unwrap();
        // 7.96：字段聚合（COUNT(f)/SUM/AVG/MIN/MAX）优先字节级取字段（顶层单字段）；
        // 点路径/转义值 → serde 回退
        let mut need_serde = true;
        if !f.contains('.') {
            match light_top_field(doc, f) {
                Some(LightVal::Absent | LightVal::Null) => return Ok(true),
                Some(LightVal::Num(bytes)) => {
                    count += 1; // COUNT(f)：非 NULL
                    if let Some(x) = std::str::from_utf8(bytes).ok().and_then(|s| s.parse::<f64>().ok()) {
                        n_num += 1;
                        sum += x;
                        if x < min {
                            min = x;
                        }
                        if x > max {
                            max = x;
                        }
                    }
                    return Ok(true);
                }
                // 字符串/布尔/嵌套值：COUNT(f) 计入，数值聚合跳过（对齐 serde）
                Some(LightVal::Str(_)) | Some(LightVal::Bool(_)) | Some(LightVal::Complex) => {
                    count += 1;
                    return Ok(true);
                }
                Some(_) => {}
                None => need_serde = true,
            }
        }
        if need_serde {
            let val = match serde_json::from_slice::<Value>(doc) {
                Ok(v) => v,
                Err(_) => return Ok(true),
            };
            let Some(fv) = field_of(&val, f) else { return Ok(true) };
            if matches!(fv, Value::Null) {
                return Ok(true);
            }
            count += 1; // COUNT(f)：非 NULL 行
            if let Value::Number(n) = fv {
                n_num += 1;
                let x = n.as_f64().unwrap_or(0.0);
                sum += x;
                if x < min {
                    min = x;
                }
                if x > max {
                    max = x;
                }
            }
        }
        Ok(true)
    };
    // P1-D（2026-09-04）：WHERE = AND 组合且含可倒排等值条件 → posting 候选收敛聚合，
    // 免全表扫（残余条件在候选 doc 上整表达式判定，与全扫等价、结果精确）。
    // 适用如 `SUM(amount) WHERE status='active' AND ts BETWEEN ...`（status 收敛候选后范围过滤）。
    let mut scanned = 0u64;
    // P91：全扫所需列（WHERE 引用 + 聚合列）——`scan_stream_fields` 投影解码用
    let needed = aggregate_needed_fields(sel.where_expr.as_ref(), field.as_deref());
    let candidate = candidate_posting(engine, sel.where_expr.as_ref(), start, end)?;
    match candidate {
        Some(post) => {
            // P1-D（2026-09-04 扩展）：倒排候选收敛后**按需解列**——只回表 WHERE 引用
            // 字段 + 聚合字段（PAX 列解码 / 行式按需字段提取，P86②/P87② 基建），替代整行
            // `engine.get` 全 25 列解码；残余范围/BETWEEN 条件在**子集文档**上判定
            // （缺失 = 原文档缺失语义，结果与整行路径精确一致）。512/块批量回表。
            let mut chunk: Vec<u64> = Vec::with_capacity(512);
            for docid in post {
                chunk.push(docid);
                if chunk.len() < 512 {
                    continue;
                }
                scanned += chunk.len() as u64;
                if scanned % 4096 == 0 && guard.is_expired() {
                    return Err(Error::QueryTooExpensive(
                        "类 SQL 聚合候选扫描超时（熔断中止）".into(),
                    ));
                }
                let fv = engine.batch_get_fields(&chunk, &needed)?;
                for (d, flds_opt) in chunk.drain(..).zip(fv.into_iter()) {
                    // 窗口外候选跳过（posting 为全库口径，多表窗口交集在此过滤）
                    if let Some(s) = start {
                        if d < s {
                            continue;
                        }
                    }
                    if let Some(e) = end {
                        if d > e {
                            continue;
                        }
                    }
                    let Some(vals) = flds_opt else { continue };
                    let doc = subset_doc(&needed, &vals);
                    let bytes = serde_json::to_vec(&doc).unwrap_or_default();
                    acc(&bytes)?;
                }
            }
            if !chunk.is_empty() {
                scanned += chunk.len() as u64;
                let fv = engine.batch_get_fields(&chunk, &needed)?;
                for (d, flds_opt) in chunk.drain(..).zip(fv.into_iter()) {
                    if let Some(s) = start {
                        if d < s {
                            continue;
                        }
                    }
                    if let Some(e) = end {
                        if d > e {
                            continue;
                        }
                    }
                    let Some(vals) = flds_opt else { continue };
                    let doc = subset_doc(&needed, &vals);
                    let bytes = serde_json::to_vec(&doc).unwrap_or_default();
                    acc(&bytes)?;
                }
            }
        }
        None => {
            // Task-025b（阶段①）：无 WHERE + 有限窗口时把 [lo..hi] 等分 W 个子窗**并发**
            // `scan_stream_fields`（每 worker 独立 count/n_num/sum/min/max，逐值与串行 acc 的
            // no-WHERE 顶层字段分支一致；COUNT/SUM/MIN/MAX/AVG 交换律 → 合并即全窗结果）。
            // 其余路径（无界窗口/带 WHERE/排序/LIMIT/GROUP 依赖行序或需合并分组）保持串行。
            let mut did_parallel = false;
            if sel.order_by.is_empty() && sel.limit.is_none() {
                if let Some((lo, hi)) = start.zip(end) {
                    if lo < hi {
                        if let Ok(ncpu) = std::thread::available_parallelism() {
                            let workers = ncpu.get().clamp(2, 8);
                            let span = hi - lo + 1;
                            let guard_ref = &guard;
                            // Task-025b 阶段②：通用 WHERE 并行——WHERE 为逐行纯函数，与分片/聚合
                            // 交换律兼容（判定逻辑与串行 acc 一致：light 优先、serde 回退）
                            let sel_where = sel.where_expr.as_ref();
                            let mut joined: Vec<Result<(u64, u64, f64, f64, f64)>> =
                                Vec::with_capacity(workers);
                            let (mut pc, mut pn, mut ps, mut pmin, mut pmax) =
                                (0u64, 0u64, 0f64, f64::INFINITY, f64::NEG_INFINITY);
                            std::thread::scope(|sc| {
                                let mut handles = Vec::with_capacity(workers);
                                for w in 0..workers {
                                    let engine_ref = engine;
                                    let needed = needed.clone();
                                    let fld = field.clone();
                                    let (cs, ce) = {
                                        let step = span / workers as u64;
                                        let s = lo + step * w as u64;
                                        let e = if w + 1 == workers {
                                            hi
                                        } else {
                                            lo + step * (w as u64 + 1) - 1
                                        };
                                        (s, e)
                                    };
                                    handles.push(sc.spawn(move || -> Result<(u64, u64, f64, f64, f64)> {
                                        let mut c = 0u64;
                                        let mut nn = 0u64;
                                        let mut su = 0f64;
                                        let mut mn = f64::INFINITY;
                                        let mut mx = f64::NEG_INFINITY;
                                        let mut scanned_local = 0u64;
                                        engine_ref.scan_stream_fields(Some(cs), Some(ce), needed, |_d, doc| {
                                            scanned_local += 1;
                                            if scanned_local % 4096 == 0 && guard_ref.is_expired() {
                                                return Err(Error::QueryTooExpensive(
                                                    "类 SQL 聚合并行全扫超时（熔断中止）".into(),
                                                ));
                                            }
                                            // 阶段②：WHERE 逐行判定（与串行 acc 分支一致：light 优先、
                                            // 点路径/转义 serde 回退；投影子集含 WHERE 引用列，语义等价）
                                            if let Some(wh) = sel_where {
                                                let hit = match light_where_matches(doc, wh) {
                                                    Some(r) => r,
                                                    None => serde_json::from_slice::<Value>(doc)
                                                        .map(|v| wh.matches_doc(&v))
                                                        .unwrap_or(false),
                                                };
                                                if !hit {
                                                    return Ok(true);
                                                }
                                            }
                                            // 与串行 acc 的 no-WHERE 分支逐值一致
                                            if let Some(f) = fld.as_deref() {
                                                let mut need_serde = true;
                                                if !f.contains('.') {
                                                    match light_top_field(doc, f) {
                                                        Some(LightVal::Absent | LightVal::Null) => {
                                                            return Ok(true)
                                                        }
                                                        Some(LightVal::Num(bytes)) => {
                                                            c += 1;
                                                            if let Some(x) = std::str::from_utf8(bytes)
                                                                .ok()
                                                                .and_then(|s| s.parse::<f64>().ok())
                                                            {
                                                                nn += 1;
                                                                su += x;
                                                                if x < mn {
                                                                    mn = x;
                                                                }
                                                                if x > mx {
                                                                    mx = x;
                                                                }
                                                            }
                                                            return Ok(true);
                                                        }
                                                        Some(LightVal::Str(_)) | Some(LightVal::Bool(_)) | Some(LightVal::Complex) => {
                                                            c += 1;
                                                            return Ok(true);
                                                        }
                                                        Some(_) => {}
                                                        None => {}
                                                    }
                                                }
                                                if need_serde {
                                                    let val = match serde_json::from_slice::<Value>(doc) {
                                                        Ok(v) => v,
                                                        Err(_) => return Ok(true),
                                                    };
                                                    let Some(fv) = field_of(&val, f) else {
                                                        return Ok(true);
                                                    };
                                                    if matches!(fv, Value::Null) {
                                                        return Ok(true);
                                                    }
                                                    c += 1;
                                                    if let Value::Number(n) = fv {
                                                        nn += 1;
                                                        let x = n.as_f64().unwrap_or(0.0);
                                                        su += x;
                                                        if x < mn {
                                                            mn = x;
                                                        }
                                                        if x > mx {
                                                            mx = x;
                                                        }
                                                    }
                                                }
                                            } else {
                                                c += 1; // COUNT(*)
                                            }
                                            Ok(true)
                                        })?;
                                        Ok((c, nn, su, mn, mx))
                                    }));
                                }
                                for h in handles {
                                    joined.push(h.join().unwrap());
                                }
                            });
                            for acc in joined {
                                let (c, nn, su, mn, mx) = acc?;
                                pc += c;
                                pn += nn;
                                ps += su;
                                if mn < pmin {
                                    pmin = mn;
                                }
                                if mx > pmax {
                                    pmax = mx;
                                }
                            }
                            // 与收尾格式完全一致（等价早退，避免触碰外层 acc 持有变量）
                            let arg = field.as_deref().unwrap_or("*");
                            let header = format!("{}({arg})", name.to_uppercase());
                            let (is_null, text) = match name.as_str() {
                                "count" => (false, pc.to_string()),
                                "sum" if pn > 0 => (false, fmt_num(ps)),
                                "avg" if pn > 0 => (false, fmt_num(ps / pn as f64)),
                                "min" if pn > 0 => (false, fmt_num(pmin)),
                                "max" if pn > 0 => (false, fmt_num(pmax)),
                                _ => (true, String::new()),
                            };
                            return Ok(Some(AggScalar { header, is_null, text }));
                        }
                    }
                }
            }
            if !did_parallel {
                // P91：全扫聚合投影解码——只解 WHERE/聚合所需列（PAX 块列解码），
                // 行式/内存直通原 JSON（消费端 acc 本就只读所需列，语义不变）
                engine.scan_stream_fields(start, end, needed, |_docid, doc| {
                    scanned += 1;
                    if scanned % 4096 == 0 && guard.is_expired() {
                        return Err(Error::QueryTooExpensive(
                            "类 SQL 聚合全量扫描超时（熔断中止）".into(),
                        ));
                    }
                    acc(doc)
                })?;
            }
        }
    }
    let arg = field.as_deref().unwrap_or("*");
    let header = format!("{}({arg})", name.to_uppercase());
    let (is_null, text) = match name.as_str() {
        "count" => (false, count.to_string()),
        "sum" if n_num > 0 => (false, fmt_num(sum)),
        "avg" if n_num > 0 => (false, fmt_num(sum / n_num as f64)),
        "min" if n_num > 0 => (false, fmt_num(min)),
        "max" if n_num > 0 => (false, fmt_num(max)),
        _ => (true, String::new()), // 空集 SUM/AVG/MIN/MAX → NULL
    };
    Ok(Some(AggScalar { header, is_null, text }))
}

/// 全库标量聚合（兼容入口 = 无窗口）。
pub fn execute_aggregate(engine: &Engine, sql: &str) -> Result<Option<AggScalar>> {
    execute_aggregate_window(engine, sql, None, None)
}

/// 数字文本化：整值（Rust f64 to_string）无小数点。
pub(crate) fn fmt_num(x: f64) -> String {
    if x.fract() == 0.0 && x.abs() < 1e15 {
        format!("{}", x as i64)
    } else {
        x.to_string()
    }
}

/// Task-030：`COUNT(DISTINCT f)` 去重键——(类型标签, 规范化文本)：
/// 数值按 f64 规范化（1 与 1.0/1e0 同值，对齐 MySQL 数值去重）、字符串原样、
/// 布尔 "true"/"false"；缺字段 / JSON null / 嵌套对象数组 → None（不计 DISTINCT，
/// 对齐 SQL「NULL 不计入 COUNT(DISTINCT)」）。仅字段值语义判定，不依赖倒排。
fn distinct_key_of(doc: &[u8], f: &str) -> Option<(u8, String)> {
    if !f.contains('.') {
        if let Some(lv) = light_top_field(doc, f) {
            match lv {
                LightVal::Num(b) => {
                    return std::str::from_utf8(b)
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok())
                        .map(|x| (0u8, fmt_num(if x == 0.0 { 0.0 } else { x })))
                }
                LightVal::Str(b) => {
                    return std::str::from_utf8(b).ok().map(|s| (1u8, s.to_string()))
                }
                LightVal::Bool(t) => return Some((2u8, t.to_string())),
                LightVal::Absent | LightVal::Null => return None,
                // 嵌套对象/数组：无可比标量 → 落下方 serde 按原值语义（极少见）
                LightVal::Complex => {}
            }
        }
    }
    serde_json::from_slice::<Value>(doc)
        .ok()
        .and_then(|v| match field_of(&v, f) {
            Some(Value::Number(n)) => {
                n.as_f64().map(|x| (0u8, fmt_num(if x == 0.0 { 0.0 } else { x })))
            }
            Some(Value::String(s)) => Some((1u8, s.clone())),
            Some(Value::Bool(t)) => Some((2u8, t.to_string())),
            _ => None,
        })
}

/// P1-D（2026-09-04）：聚合候选收敛——WHERE 为 AND 组合且含**可倒排等值**子条件时，
/// 取其首个倒排命中的 posting 作候选 docid 集（免全表扫）。残余条件（范围/BETWEEN 等）
/// 在候选 doc 上整表达式判定（与全扫等价、结果精确）。
/// 返回 None = 无可用等值候选（回退全表扫）。窗口参数仅用于接收（窗口过滤在消费循环做）。
fn candidate_posting(
    engine: &Engine,
    where_expr: Option<&WhereExpr>,
    _start: Option<u64>,
    _end: Option<u64>,
) -> Result<Option<RoaringBitmap>> {
    let Some(we) = where_expr else { return Ok(None) };
    if !matches!(we, WhereExpr::And(_, _)) {
        return Ok(None); // 裸等值已走倒排统计快路径；非 AND 组合无收敛价值
    }
    for (f, v) in extract_eq_conds(we) {
        let term = format!("{f}={v}");
        let post = engine.inverted_posting(&term)?;
        if !post.is_empty() {
            return Ok(Some(post));
        }
    }
    Ok(None)
}

/// P1-D 扩展：聚合查询（WHERE + 聚合列）引用的顶层字段集——供 posting 候选收敛后的
/// **按需解列**（只回表这些字段，免整行 25 列解码）。点路径（`a.b`）取顶层键
/// （子树整体解出，谓词/聚合走 Value 路径时可下钻）；去重。
pub(crate) fn aggregate_needed_fields(where_expr: Option<&WhereExpr>, agg: Option<&str>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    fn push(mut s: &str, out: &mut Vec<String>) {
        if let Some(p) = s.find('.') {
            s = &s[..p];
        }
        if !s.is_empty() && !out.iter().any(|x| x == s) {
            out.push(s.to_string());
        }
    }
    fn walk(e: &WhereExpr, out: &mut Vec<String>) {
        match e {
            WhereExpr::Cond(c) => push(&c.field, out),
            WhereExpr::Between { field, .. } => push(field, out),
            WhereExpr::Like { field, .. } => push(field, out),
            WhereExpr::Not(x) => walk(x, out),
            WhereExpr::And(a, b) => {
                walk(a, out);
                walk(b, out);
            }
            WhereExpr::Or(a, b) => {
                walk(a, out);
                walk(b, out);
            }
        }
    }
    if let Some(we) = where_expr {
        walk(we, &mut out);
    }
    if let Some(f) = agg {
        push(f, &mut out);
    }
    out
}

/// P1-D 扩展：按需字段字节 → 子集 JSON 对象（缺失字段 → 键缺省 = 原文档缺省语义；
/// `b"null"` → JSON null；非法字节忽略——与整行解析的缺省语义一致）。
fn subset_doc(fields: &[String], vals: &[Option<Vec<u8>>]) -> Value {
    let mut m = serde_json::Map::new();
    for (f, v) in fields.iter().zip(vals.iter()) {
        if let Some(bytes) = v {
            if let Ok(vv) = serde_json::from_slice::<Value>(bytes) {
                m.insert(f.clone(), vv);
            }
        }
    }
    Value::Object(m)
}
