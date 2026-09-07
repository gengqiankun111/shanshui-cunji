//! P141（2026-09-06）：事务内聚合/分组 MVCC 权威版执行族（server/command/txn_agg.rs）。
//!
//! 覆盖 SQL：`SELECT COUNT(*) / COUNT(f) / COUNT(DISTINCT f) / SUM(f) / AVG(f) /
//! MIN(f) / MAX(f) [WHERE ...] [GROUP BY f1, f2 [HAVING ...] [ORDER BY ...] [LIMIT]]`
//! 在事务内（点查 / id 窗口 / 字段谓词 / 无 WHERE 全表）执行。
//!
//! **MVCC 权威 = 行源一律取事务视图**：
//! - 快照（RR/SERIALIZABLE）与 RC 走 `scan_range_txn` / `txn_get`（引擎内已按隔离级别
//!   折叠：快照 ≤S / RC 最新已提交 + Delta 覆盖 + 同事务写 read_own 合并 + 自写新 docid 并入）；
//! - FOR UPDATE 当前读走 `txn_read_current` / `txn_scan_current`（最新已提交 + 自写覆盖 + 锁定）。
//!
//! 因此**跨过引擎最新态的倒排 posting/统计载荷/位图快路径**（P134 后同类快路径为权威
//! 扫描口径）；行级字段判定/累积与 sqlish 权威执行器同函数：
//! - 标量聚合：`distinct_key_of`/`fmt_num`（aggregate.rs）＋ `field_non_null`/`numeric_field`
//!   （group_by.rs，与权威 acc 逐值一致）；
//! - 分组：`group_key_of`/`AggState` 逐行累积 → `finalize_groups`（group_by.rs 权威收尾：
//!   HAVING/组排序/切片/GroupResult）——同输入同输出，事务结果与非事务分组逐字节一致。
//!
//! 谓词复检与权威聚合 acc 同构：`light_where_matches` 优先、serde `matches_doc` 回退。
//!
//! 边界（延续 P134/旧 b 路径，防扩面）：
//! - 字段谓词 / 无 WHERE 全表仅默认表（tid=0）；非默认表仅主键窗口（id BETWEEN/点/IN）可用；
//! - 无 WHERE 全表窗口要求 auto_watermark ≤ 2^48（单表行库），否则 1064 引导 id 窗口/WHERE；
//! - FOR UPDATE + 无 WHERE 全表 → 1064（当前读全表 = 引擎最新视图 + 写合并，首版限窗口/谓词）；
//! - wm > 2^48（多表混合库）字段谓词 → 回落 sqlish 候选路径（同 P134 既有边界）。

use crate::engine::{Engine, QueryRow};
use crate::error::Result;
use crate::server::*;
use crate::sql::executor::aggregate::{distinct_key_of, fmt_num};
use crate::sql::executor::eval::light_where_matches;
use crate::sql::executor::group_by::{
    field_non_null, finalize_groups, group_key_of, numeric_field, AggState, GroupKey,
};
use crate::sql::parser::{parse_select, Select, WhereExpr};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// 分组数上限（对齐 select.rs 非事务 `execute_group_by_window(engine, sql, 10_000, ...)`）。
const GROUP_CAP: u64 = 10_000;
/// sqlish 候选行数上限（对齐 txn_select_by_predicate 旧 CAP）。
const SQ_CAP: u64 = 200_000;
/// 快照窗口分页步长（对齐 P134 SPAN=16384）。
const SPAN: u64 = 16_384;

/// 聚合行源。
enum Src {
    /// `id BETWEEN A AND B` → docid 闭区间 [da, db]。
    Between { da: u64, db: u64 },
    /// 点查 / `id IN (...)` → 逐 id。
    Ids(Vec<u64>),
    /// 字段谓词（WhereExpr 有值）或无 WHERE 全表（None）——窗口或 sqlish 候选取行。
    Pred(Option<WhereExpr>),
}

/// 行级谓词判定（与权威聚合 acc 同构：light 优先、serde 回退）。
fn where_hit(wh: &WhereExpr, doc: &[u8]) -> bool {
    match light_where_matches(doc, wh) {
        Some(r) => r,
        None => serde_json::from_slice::<Value>(doc)
            .map(|v| wh.matches_doc(&v))
            .unwrap_or(false),
    }
}

/// 事务聚合/分组执行入口（txn_select 顶部 detect 命中后调用；sql = FOR UPDATE 已剥离核心）。
pub(crate) fn txn_aggregate(
    engine: &Engine,
    session: &mut Session,
    sql: &str,
    for_update: bool,
) -> QueryResponse {
    let sel = match parse_select(sql) {
        Ok(s) => s,
        Err(e) => {
            return QueryResponse::Err(1064, format!("事务内聚合查询语法错误: {e}"))
        }
    };
    // 分组形态：GROUP BY 需聚合列（权威同文案）
    let grouped = !sel.group_by.is_empty();
    if grouped && sel.group_aggs.is_empty() {
        return QueryResponse::Err(1064, "GROUP BY 需至少一个聚合列".into());
    }
    if !grouped && sel.agg.is_none() {
        // 理论上不可达（txn_select 只在该 SQL 命中聚合/分组特征后调用）
        return QueryResponse::Err(1064, "非聚合查询进入聚合执行".into());
    }
    let txn = session.txn.as_mut().unwrap();
    // 主键访问形态优先（id BETWEEN 闭窗 / 点查 / id IN —— 与事务非聚合同款提取与 docid 换算）
    let tid = table_id_for(&sel.table);
    let src = if let Some((a, b)) = extract_between_range(sql) {
        Src::Between { da: docid_for(tid, a), db: docid_for(tid, b) }
    } else if let Some(ids) = extract_target_ids(sql) {
        Src::Ids(ids.into_iter().map(|r| docid_for(tid, r)).collect())
    } else {
        // 字段谓词 / 无 WHERE 全表：仅默认表可用（§26 M1 边界延续）
        if tid != 0 {
            return QueryResponse::Err(
                1064,
                "事务内聚合字段谓词/无 WHERE 仅支持默认表（非默认表请用 id 窗口/点查）"
                    .to_string(),
            );
        }
        Src::Pred(sel.where_expr.clone())
    };
    let wm = engine.auto_watermark();
    // P141 边界（头注 §边界）：无 WHERE 全表（Pred(None)）当前读 / 多表混合库 → sqlish 无谓词
    // 可构造且全表当前读超范围 → 1064 引导 id 窗口/点查或 WHERE 收敛（与 P134/旧 b 边界一致）。
    if matches!(src, Src::Pred(None)) && (for_update || wm > (1u64 << 48)) {
        let msg = if for_update {
            "FOR UPDATE 当前读不支持无 WHERE 全表聚合（请用 id 窗口/点查或 WHERE 收敛）"
        } else {
            "无 WHERE 全表聚合仅限单表行库（请用 id 窗口/点查或 WHERE 收敛）"
        };
        return QueryResponse::Err(1064, msg.to_string());
    }
    if grouped {
        txn_group(engine, txn, &sel, sql, for_update, src, wm)
    } else {
        txn_scalar(engine, txn, &sel, sql, for_update, src, wm)
    }
}

// ---------------------------------------------------------------------------
// 行源取数
// ---------------------------------------------------------------------------

/// 窗口页取数：整行快照视图扫描（事务语义已由引擎合入：≤S/RC 最新、Delta 折叠、同事务写覆盖）。
/// 注：纯 COUNT(*) 本可 keys-only（免 PAX 整行解码），但 `scan_range_txn_fields` 空字段集投影
/// 产出空值会被消费端过滤清空 → 暂统一整行（COUNT(*) 累积闭包不解 doc；轻量键计数留引擎侧）。
fn page_rows(
    engine: &Engine,
    txn: &mut crate::txn::Transaction,
    w: u64,
    e: u64,
) -> Result<Vec<QueryRow>> {
    engine.scan_range_txn(txn, Some(w), Some(e))
}

/// 逐页遍历快照窗口 [start, end]（含同事务写合并，由引擎保证）。
fn for_each_window<F>(
    engine: &Engine,
    txn: &mut crate::txn::Transaction,
    start: u64,
    end: u64,
    mut f: F,
) -> Result<()>
where
    F: FnMut(u64, &[u8]) -> Result<()>,
{
    let mut w = start;
    loop {
        let e = w.saturating_add(SPAN - 1).min(end);
        let page = page_rows(engine, txn, w, e)?;
        for (d, v) in page {
            f(d, &v)?;
        }
        if e == end {
            break;
        }
        w = e.saturating_add(1);
    }
    Ok(())
}

/// 统一行流驱动：按 src 取事务视图行，经谓词（仅 Pred 源）过滤后喂累积闭包。
fn drive<F>(
    engine: &Engine,
    txn: &mut crate::txn::Transaction,
    sql: &str,
    for_update: bool,
    src: &Src,
    mut f: F,
) -> Result<()>
where
    F: FnMut(u64, &[u8]) -> Result<()>,
{
    match src {
        Src::Between { da, db } => {
            if for_update {
                // 当前读：一次范围扫描（锁定命中行）+ 自写覆盖（txn_scan_current）
                let rows = txn_scan_current(engine, txn, Some(*da), Some(*db))?;
                for (d, v) in rows {
                    f(d, &v)?;
                }
            } else {
                for_each_window(engine, txn, *da, *db, &mut f)?;
            }
        }
        Src::Ids(ids) => {
            for id in ids {
                let r = if for_update {
                    txn_read_current(engine, txn, *id)?
                } else {
                    engine.txn_get(txn, *id)?
                };
                if let Some(v) = r {
                    f(*id, &v)?;
                }
            }
        }
        Src::Pred(wh) => {
            // 谓词源：非 FOR UPDATE 且单表单行库（wm≤2^48）→ 快照窗口分页（权威：RR 快照 /
            // RC 最新视图，scan_range_txn 内部按隔离分流）；FOR UPDATE（当前读）或多表混合库
            // → sqlish 当前视图候选 + 逐候选复检（延续旧 b / P134 边界路径）。
            if !for_update && engine.auto_watermark() <= (1u64 << 48) {
                let mut hi = engine.auto_watermark().saturating_sub(1);
                // 同事务未提交 Put（新 docid 恒在引擎水位之上，如 txn INSERT 后引擎水位未动）
                // → 抬高窗口上界让其并入：scan_range_txn 尾段自写合并仅并入窗内 docid，聚合须
                //   见自插新行（等效 P134 谓词快照路径的 complete 后自写补入）。域限低位表。
                for op in txn.ops() {
                    if let crate::txn::Op::Put { docid, .. } = op {
                        if *docid < (1u64 << 48) && *docid > hi {
                            hi = *docid;
                        }
                    }
                }
                match wh {
                    // 无 WHERE 全表：每行均喂累积（无谓词过滤）
                    None => for_each_window(engine, txn, 0, hi, &mut f)?,
                    // 有 WHERE：谓词复检后喂行（light 优先 / serde 回退，与权威 acc 同构）
                    Some(we) => for_each_window(engine, txn, 0, hi, |_d, v| {
                        if where_hit(we, v) {
                            f(_d, v)?;
                        }
                        Ok(())
                    })?,
                }
                return Ok(());
            }
            // sqlish 候选路径：当前视图命中 ∪ 同事务写集 → 逐候选读行 + 谓词复检。
            // A1-3（2026-09-07）：尾截断须含 GROUP BY/HAVING——cond_sql 只取 WHERE 条件
            // （分组累积在 txn_group 行流完成）；旧实现只截 ORDER BY/LIMIT → 字段谓词 +
            // GROUP BY 走兜底（混合库 wm>2^48 / FOR UPDATE）时 cond 含 "GROUP BY s" →
            // parse "非分组列 docid" 解析失败。
            let lower = sql.to_lowercase();
            let pos = lower
                .find("where")
                .ok_or_else(|| crate::error::Error::Config("谓词源缺 WHERE".into()))?;
            let rest = &sql[pos + 5..];
            let ol = rest.to_lowercase();
            let end = [
                ol.find("order by"),
                ol.find("group by"),
                ol.find("having"),
                ol.find(" limit "),
                ol.find(" limit)"),
            ]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(rest.len());
            let tail = rest[..end].trim();
            if tail.is_empty() {
                return Err(crate::error::Error::Config("谓词源缺 WHERE".into()));
            }
            let cond_sql = format!("SELECT docid FROM t WHERE {tail}");
            let base = crate::sqlish::execute(engine, &cond_sql, SQ_CAP)?;
            let mut set: HashSet<u64> = base.into_iter().map(|r| r.0).collect();
            set.extend(txn.write_set().iter().copied());
            let mut ids: Vec<u64> = set.into_iter().collect();
            ids.sort_unstable();
            for id in ids {
                let r = if for_update {
                    txn_read_current(engine, txn, id)?
                } else {
                    engine.txn_get(txn, id)?
                };
                if let Some(v) = r {
                    if crate::sqlish::doc_matches_where(&cond_sql, &v) {
                        f(id, &v)?;
                    }
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 标量聚合（COUNT/SUM/AVG/MIN/MAX/COUNT(DISTINCT)，权威字段语义）
// ---------------------------------------------------------------------------

/// 标量累积器：与 aggregate.rs 权威 `acc` 闭包逐值一致的计数/数值累积。
struct ScalarAgg {
    name: String,
    field: Option<String>,
    distinct: bool,
    count: u64,
    n_num: u64,
    sum: f64,
    min: f64,
    max: f64,
    seen: Option<HashSet<(u8, String)>>,
}

impl ScalarAgg {
    fn new(name: String, field: Option<String>, distinct: bool) -> Self {
        let seen = if distinct { Some(HashSet::new()) } else { None };
        Self {
            name,
            field,
            distinct,
            count: 0,
            n_num: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            seen,
        }
    }

    fn hit(&mut self, doc: &[u8]) {
        let Some(f) = self.field.as_deref() else {
            // COUNT(*)：无需解析 doc
            self.count += 1;
            return;
        };
        if self.distinct {
            if let Some(k) = distinct_key_of(doc, f) {
                self.seen.as_mut().unwrap().insert(k);
            }
            return;
        }
        match self.name.as_str() {
            "count" => {
                if field_non_null(doc, f) {
                    self.count += 1;
                }
            }
            _ => {
                // SUM/AVG/MIN/MAX：只统计数值行（非数值/缺省跳过；空数值集 → SQL NULL）
                if let Some(x) = numeric_field(doc, f) {
                    self.n_num += 1;
                    self.sum += x;
                    if x < self.min {
                        self.min = x;
                    }
                    if x > self.max {
                        self.max = x;
                    }
                }
            }
        }
    }

    fn finish(&self) -> (String, bool, String) {
        if self.distinct {
            let f = self.field.as_deref().unwrap_or("");
            return (
                format!("COUNT(DISTINCT {f})"),
                false,
                self.seen.as_ref().unwrap().len().to_string(),
            );
        }
        let arg = self.field.as_deref().unwrap_or("*");
        let header = format!("{}({arg})", self.name.to_uppercase());
        let (is_null, text) = match self.name.as_str() {
            "count" => (false, self.count.to_string()),
            "sum" if self.n_num > 0 => (false, fmt_num(self.sum)),
            "avg" if self.n_num > 0 => (false, fmt_num(self.sum / self.n_num as f64)),
            "min" if self.n_num > 0 => (false, fmt_num(self.min)),
            "max" if self.n_num > 0 => (false, fmt_num(self.max)),
            _ => (true, String::new()), // 空集 SUM/AVG/MIN/MAX → SQL NULL
        };
        (header, is_null, text)
    }
}

/// 标量聚合响应（对齐 select.rs 权威聚合响应：AVG/MIN/MAX → DOUBLE，COUNT/SUM → LONGLONG）。
fn scalar_response(agg: &ScalarAgg) -> QueryResponse {
    let (header, is_null, text) = agg.finish();
    let (col_type, charset) = if header.starts_with("AVG(")
        || header.starts_with("MIN(")
        || header.starts_with("MAX(")
    {
        (MYSQL_TYPE_DOUBLE, 63)
    } else {
        (MYSQL_TYPE_LONGLONG, 63)
    };
    let row = if is_null {
        vec![vec![vec![MYSQL_NULL_CELL]]]
    } else {
        vec![vec![text.into_bytes()]]
    };
    let columns = vec![column_payload(&header, col_type, charset)];
    QueryResponse::Set { columns, rows: row }
}

fn txn_scalar(
    engine: &Engine,
    txn: &mut crate::txn::Transaction,
    sel: &Select,
    sql: &str,
    for_update: bool,
    src: Src,
    _wm: u64,
) -> QueryResponse {
    let (name, field) = sel.agg.clone().unwrap();
    let distinct = sel.agg_distinct;
    let mut acc = ScalarAgg::new(name, field, distinct);
    let r = drive(engine, txn, sql, for_update, &src, |_d, v| {
        acc.hit(v);
        Ok(())
    });
    match r {
        Ok(()) => scalar_response(&acc),
        Err(e) => QueryResponse::Err(3500, format!("事务内聚合失败: {e}")),
    }
}

// ---------------------------------------------------------------------------
// 分组聚合（GROUP BY：COUNT/SUM/AVG/MIN/MAX + HAVING/ORDER/LIMIT，权威收尾）
// ---------------------------------------------------------------------------

struct GroupAgg {
    fields: Vec<String>,
    specs: Vec<(String, Option<String>)>,
    groups: HashMap<Vec<GroupKey>, Vec<AggState>>,
}

impl GroupAgg {
    fn new(fields: Vec<String>, specs: Vec<(String, Option<String>)>) -> Self {
        Self { fields, specs, groups: HashMap::new() }
    }

    /// 逐行累积（与 group_by.rs 权威扫描闭包逐值一致）。
    fn hit(&mut self, doc: &[u8]) -> Result<()> {
        let key: Vec<GroupKey> = self.fields.iter().map(|f| group_key_of(doc, f)).collect();
        let states = self.groups.entry(key).or_insert_with(|| {
            (0..self.specs.len()).map(|_| AggState::new()).collect()
        });
        for (idx, (name, fld)) in self.specs.iter().enumerate() {
            let st = &mut states[idx];
            match name.as_str() {
                "count" => {
                    if fld.is_none() || field_non_null(doc, fld.as_ref().unwrap()) {
                        st.count += 1;
                    }
                }
                _ => {
                    if let Some(x) = numeric_field(doc, fld.as_ref().unwrap()) {
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
        if self.groups.len() as u64 > GROUP_CAP {
            return Err(crate::error::Error::QueryTooExpensive(format!(
                "GROUP BY 分组数超过上限（{} 组，上限 {GROUP_CAP}），请加 WHERE 收敛",
                self.groups.len()
            )));
        }
        Ok(())
    }
}

/// 分组结果响应（列类型推断/行列字节组装——对齐 select.rs 非事务 GROUP BY 响应构造）。
pub(crate) fn group_response(gr: &crate::sql::executor::group_by::GroupResult) -> QueryResponse {
    let mut columns: Vec<Vec<u8>> = Vec::new();
    for name in &gr.group_cols {
        let Some(level) = gr.group_fields.iter().position(|f| f == name) else {
            return QueryResponse::Err(1064, format!("group col {name} 非分组字段"));
        };
        let mut col_type = MYSQL_TYPE_VAR_STRING;
        for r in &gr.rows {
            if let Some(t) = r.keys.get(level).and_then(|k| k.as_ref()) {
                col_type = if !r.key_is_num[level] {
                    MYSQL_TYPE_VAR_STRING
                } else if t.contains('.') || t.contains('e') || t.contains('E') {
                    MYSQL_TYPE_DOUBLE
                } else {
                    MYSQL_TYPE_LONGLONG
                };
                break;
            }
        }
        let charset = if col_type == MYSQL_TYPE_VAR_STRING { 45 } else { 63 };
        columns.push(column_payload(name, col_type, charset));
    }
    for (i, h) in gr.headers.iter().enumerate() {
        let frac = gr.rows.iter().any(|r| {
            r.cells
                .get(i)
                .map(|c| {
                    c.as_deref()
                        .map(|t| t.contains('.') || t.contains('e') || t.contains('E'))
                        .unwrap_or(false)
                })
                .unwrap_or(false)
        });
        columns.push(column_payload(
            h,
            if frac { MYSQL_TYPE_DOUBLE } else { MYSQL_TYPE_LONGLONG },
            63,
        ));
    }
    let mut rows: Vec<Vec<Vec<u8>>> = Vec::with_capacity(gr.rows.len());
    for r in &gr.rows {
        let mut row = Vec::with_capacity(columns.len());
        for name in &gr.group_cols {
            let level = gr.group_fields.iter().position(|f| f == name).unwrap_or(0);
            match r.keys.get(level).and_then(|k| k.as_ref()) {
                Some(t) => row.push(t.clone().into_bytes()),
                None => row.push(vec![MYSQL_NULL_CELL]),
            }
        }
        for c in &r.cells {
            match c {
                Some(t) => row.push(t.clone().into_bytes()),
                None => row.push(vec![MYSQL_NULL_CELL]),
            }
        }
        rows.push(row);
    }
    QueryResponse::Set { columns, rows }
}

fn txn_group(
    engine: &Engine,
    txn: &mut crate::txn::Transaction,
    sel: &Select,
    sql: &str,
    for_update: bool,
    src: Src,
    _wm: u64,
) -> QueryResponse {
    let mut acc = GroupAgg::new(sel.group_by.clone(), sel.group_aggs.clone());
    // GROUP BY 需要 doc 字段（组键/聚合）→ 恒整行（谓词过滤已在 drive 的 Pred 源内完成）
    let r = drive(engine, txn, sql, for_update, &src, |_d, v| acc.hit(v));
    let gr = match r {
        Ok(()) => match finalize_groups(
            sel,
            sel.group_by.clone(),
            sel.group_aggs.clone(),
            acc.groups,
            GROUP_CAP,
        ) {
            Ok(g) => g,
            Err(e) => return QueryResponse::Err(1064, format!("query error: {e}")),
        },
        Err(e) => return QueryResponse::Err(3500, format!("事务内分组失败: {e}")),
    };
    group_response(&gr)
}
