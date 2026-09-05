//! 求值/轻量字节扫描基建（原 sqlish.rs 求值段）：倒排 posting 位图求值 +
//! 字节级顶层字段扫描（LightVal，免整行 serde）+ leaf 扫描后过滤/下推 +
//! 行级判定（WhereExpr::matches_doc）。select/aggregate/group_by/join 共享。

use crate::engine::{Engine, QueryRow};
use crate::error::{Error, Result};
use crate::sstable::ZonePredicate;
use crate::sql::parser::{CmpOp, Cond, WhereExpr};
use roaring::treemap::RoaringTreemap as RoaringBitmap;
use serde_json::Value;

/// Task-024：单遍只收顶层 `keep` 字段的 JSON 子集（非目标值 `IgnoredAny` 跳过，免整行
/// 25 列 Value 构造/丢弃与大文本分配）→ 序列化为子集 JSON 字节。缺失字段省略（消费端
/// “缺失 = NULL”语义与整行路径一致）。仅顶层简单字段名适用；含 `.`/`[` 由调用方回退整行。
pub(crate) fn subset_doc_bytes(doc: &[u8], keep: &[String]) -> Option<Vec<u8>> {
    use serde::Deserializer as _;
    struct Pick<'a> {
        keep: &'a [String],
    }
    impl<'de, 'a> serde::de::Visitor<'de> for Pick<'a> {
        type Value = serde_json::Value;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a JSON object")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut a: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut out = serde_json::map::Map::new();
            while let Some(k) = a.next_key::<String>()? {
                if self.keep.iter().any(|x| x == &k) {
                    out.insert(k, a.next_value::<serde_json::Value>()?);
                } else {
                    let _skip: serde::de::IgnoredAny = a.next_value()?;
                }
            }
            Ok(serde_json::Value::Object(out))
        }
    }
    let mut de = serde_json::Deserializer::from_slice(doc);
    de.deserialize_map(Pick { keep })
        .ok()
        .and_then(|v| serde_json::to_vec(&v).ok())
}


// ---------------------------------------------------------------------------
// 求值（引擎版：倒排 posting 位图 + 比较运算扫描过滤）
// ---------------------------------------------------------------------------

/// 全量 docid 位图（NOT/!=/比较运算的论域；逐批熔断防挂起）。
pub(crate) fn full_docids(engine: &Engine, guard: &crate::watchdog::QueryGuard) -> Result<RoaringBitmap> {
    let mut bm = RoaringBitmap::new();
    let mut scanned = 0u64;
    for (docid, _) in engine.scan_range(None, None)? {
        scanned += 1;
        if scanned % 4096 == 0 && guard.is_expired() {
            return Err(Error::QueryTooExpensive("类 SQL 全量扫描超时（熔断中止），建议用倒排等值条件收敛范围".into()));
        }
        if docid < u32::MAX as u64 {
            bm.insert(docid);
        }
    }
    Ok(bm)
}

// 7.96 轻量顶层字段判定（免整文档反序列化）——
// 无索引全扫（下推/等值回退/后过滤/聚合）每行 `serde_json::from_slice::<Value>` 构建整树，
// 1000 万行 ~12s vs MySQL 列式 2s。这里手写字节级扫描：只定位**目标顶层字段**的原始值
// 字节（数字/无转义字符串/布尔/null），跳过其余顶层项；结果与 serde 语义对齐，任何
// 无法轻量确定的结构（转义值/嵌套值内含引号规则）→ 调用方回退完整 serde（正确性护栏）。

/// 轻量目标字段值。
pub(crate) enum LightVal<'a> {
    /// 顶层无该键（语义 = 字段缺失 → 任何条件 false）。
    Absent,
    /// 数字原始字节（不含空白）。
    Num(&'a [u8]),
    /// 字符串值内部字节（无转义）。
    Str(&'a [u8]),
    Bool(bool),
    Null,
    /// 值本身是嵌套对象/数组（serde 语义下任何比较均为 false）。
    Complex,
}

pub(crate) fn ws(b: &[u8], i: usize) -> usize {
    let mut j = i;
    while j < b.len() && matches!(b[j], b' ' | b'\t' | b'\n' | b'\r') {
        j += 1;
    }
    j
}

/// 跳过一个字符串字面量（含 `\\`/`\"` 转义），返回是否成功。
fn skip_string(b: &[u8], i: &mut usize) -> bool {
    if *i >= b.len() || b[*i] != b'"' {
        return false;
    }
    *i += 1;
    while *i < b.len() {
        let c = b[*i];
        if c == b'\\' {
            *i += 2;
            continue;
        }
        *i += 1;
        if c == b'"' {
            return true;
        }
    }
    false
}

/// 跳过一个 JSON 值（对象/数组做括号平衡，内部字符串跳过引号规则）。
pub(crate) fn skip_value(b: &[u8], i: &mut usize) -> bool {
    if *i >= b.len() {
        return false;
    }
    match b[*i] {
        b'"' => skip_string(b, i),
        b'{' | b'[' => {
            let open = b[*i];
            let close = if open == b'{' { b'}' } else { b']' };
            let mut depth = 1i32;
            *i += 1;
            while *i < b.len() {
                let c = b[*i];
                if c == b'"' {
                    if !skip_string(b, i) {
                        return false;
                    }
                    continue;
                }
                *i += 1;
                if c == open {
                    depth += 1;
                } else if c == close {
                    depth -= 1;
                    if depth == 0 {
                        return true;
                    }
                }
            }
            false
        }
        _ => {
            // 标量到逗号/右括号
            while *i < b.len() && b[*i] != b',' && b[*i] != b'}' {
                *i += 1;
            }
            true
        }
    }
}

/// 读取目标字段的值（假定已定位到值起点）。转义/畸形 → None（回退 serde）。
pub(crate) fn read_target_value<'a>(b: &'a [u8], i: usize) -> Option<LightVal<'a>> {
    let n = b.len();
    if i >= n {
        return None;
    }
    match b[i] {
        b'"' => {
            let mut j = i + 1;
            let vs = j;
            let mut esc = false;
            while j < n {
                let c = b[j];
                if c == b'\\' {
                    esc = true;
                    j += 2;
                    continue;
                }
                j += 1;
                if c == b'"' {
                    return if esc { None } else { Some(LightVal::Str(&b[vs..j - 1])) };
                }
            }
            None
        }
        b't' => {
            if b[i..].starts_with(b"true") {
                Some(LightVal::Bool(true))
            } else {
                None
            }
        }
        b'f' => {
            if b[i..].starts_with(b"false") {
                Some(LightVal::Bool(false))
            } else {
                None
            }
        }
        b'n' => {
            if b[i..].starts_with(b"null") {
                Some(LightVal::Null)
            } else {
                None
            }
        }
        b'-' | b'0'..=b'9' => {
            let vs = i;
            let mut j = i;
            while j < n
                && (b[j].is_ascii_digit()
                    || matches!(b[j], b'-' | b'+' | b'.' | b'e' | b'E'))
            {
                j += 1;
            }
            Some(LightVal::Num(&b[vs..j]))
        }
        b'{' | b'[' => Some(LightVal::Complex),
        _ => None,
    }
}

/// 字节级定位 doc 顶层目标字段值。`None` = 结构无法轻量遍历（畸形/转义 key）。
pub(crate) fn light_top_field<'a>(doc: &'a [u8], field: &str) -> Option<LightVal<'a>> {
    let b = doc;
    let n = b.len();
    let mut i = ws(b, 0);
    if i >= n || b[i] != b'{' {
        return None;
    }
    i += 1;
    loop {
        i = ws(b, i);
        if i >= n {
            return None;
        }
        if b[i] == b'}' {
            return Some(LightVal::Absent);
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
        if key == field.as_bytes() {
            return read_target_value(b, i);
        }
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
            return Some(LightVal::Absent);
        } else {
            return None;
        }
    }
}

/// 轻量 leaf 判定（顶层单字段）。返回 None = 无法轻量（点路径/转义/畸形），调用方回退 serde。
/// 语义与 serde 路径一致：缺失/嵌套值/null → false；数字等值仅纯整数直比（浮点回退）；
/// 比较类数值按 f64；字符串字节序与 UTF-8 字典序一致。
fn light_leaf_result<'a>(doc: &'a [u8], leaf: &Leaf<'a>) -> Option<bool> {
    let field = match leaf {
        Leaf::Cmp(c) => c.field.as_str(),
        Leaf::Between { field, .. } => field,
        Leaf::Like { field, .. } => field,
    };
    if field.contains('.') {
        return None;
    }
    let lv = light_top_field(doc, field)?;
    match (leaf, lv) {
        (Leaf::Cmp(c), lv) => Some(light_cmp_value(lv, &c.op, &c.value)?),
        (Leaf::Between { low, high, .. }, lv) => Some(light_between_value(lv, low, high)?),
        (Leaf::Like { pattern, .. }, lv) => Some(light_like_value(lv, pattern)?),
    }
}

fn light_between_value<'a>(lv: LightVal<'a>, low: &str, high: &str) -> Option<bool> {
    match lv {
        LightVal::Absent | LightVal::Complex | LightVal::Null => Some(false),
        LightVal::Num(bytes) => {
            let v = std::str::from_utf8(bytes).ok()?.parse::<f64>().ok()?;
            let l = low.parse::<f64>().ok()?;
            let h = high.parse::<f64>().ok()?;
            Some(v >= l && v <= h)
        }
        LightVal::Str(bytes) => Some(bytes >= low.as_bytes() && bytes <= high.as_bytes()),
        LightVal::Bool(_) => Some(false),
    }
}

/// LIKE 轻量判定（字节级）：仅 Str 参与（`%` 通配）；Str 字节为无转义 UTF-8 → 转 str 复用
/// `like_match`；非字符串值恒 false（对齐 serde 路径）。
fn light_like_value<'a>(lv: LightVal<'a>, pattern: &str) -> Option<bool> {
    match lv {
        LightVal::Absent | LightVal::Complex | LightVal::Null | LightVal::Bool(_) => Some(false),
        LightVal::Str(bytes) => Some(like_match(std::str::from_utf8(bytes).ok()?, pattern)),
        LightVal::Num(_) => Some(false),
    }
}

/// 标量与比较运算符判定（对齐 serde 路径语义；无法对齐 → None 回退）。
fn light_cmp_value<'a>(lv: LightVal<'a>, op: &CmpOp, rhs: &str) -> Option<bool> {
    use CmpOp::*;
    match lv {
        LightVal::Absent | LightVal::Complex | LightVal::Null => Some(false),
        LightVal::Bool(t) => {
            let txt: &[u8] = if t { b"true" } else { b"false" };
            match op {
                Eq => Some(txt == rhs.as_bytes()),
                Ne => Some(txt != rhs.as_bytes()),
                Gt | Ge | Lt | Le => Some(false), // serde 无布尔大小比较 arm
            }
        }
        LightVal::Num(bytes) => {
            let pure_int = |s: &[u8]| !s.is_empty() && s.iter().all(|c| c.is_ascii_digit());
            match op {
                // serde 语义：Number Eq/Ne 为 `n.to_string()==rhs` 字符串比——
                // 仅纯整数两边字面可比；浮点/负号等值回退 serde（尾零/科学计数差异）
                Eq => {
                    if pure_int(bytes) && pure_int(rhs.as_bytes()) {
                        Some(bytes == rhs.as_bytes())
                    } else {
                        None
                    }
                }
                Ne => {
                    if pure_int(bytes) && pure_int(rhs.as_bytes()) {
                        Some(bytes != rhs.as_bytes())
                    } else {
                        None
                    }
                }
                Gt | Ge | Lt | Le => {
                    let v = std::str::from_utf8(bytes).ok()?.parse::<f64>().ok()?;
                    let r = rhs.parse::<f64>().ok()?;
                    Some(match op {
                        Gt => v > r,
                        Ge => v >= r,
                        Lt => v < r,
                        Le => v <= r,
                        _ => unreachable!(),
                    })
                }
            }
        }
        LightVal::Str(bytes) => {
            let rb = rhs.as_bytes();
            match op {
                Eq => Some(bytes == rb),
                Ne => Some(bytes != rb),
                Gt => Some(bytes > rb),
                Ge => Some(bytes >= rb),
                Lt => Some(bytes < rb),
                Le => Some(bytes <= rb),
            }
        }
    }
}

pub(crate) fn field_of<'a>(doc: &'a Value, field: &str) -> Option<&'a Value> {
    let mut cur = doc;
    for part in field.split('.') {
        cur = cur.get(part)?;
    }
    Some(cur)
}

fn cmp_value(doc_val: &Value, op: &CmpOp, rhs: &str) -> bool {
    use serde_json::Value::*;
    match (doc_val, op) {
        (String(s), CmpOp::Eq) => s == rhs,
        (String(s), CmpOp::Ne) => s != rhs,
        (Number(n), CmpOp::Eq) => n.to_string() == rhs,
        (Number(n), CmpOp::Ne) => n.to_string() != rhs,
        (Bool(b), CmpOp::Eq) => b.to_string() == rhs,
        (Bool(b), CmpOp::Ne) => b.to_string() != rhs,
        (Number(n), CmpOp::Gt | CmpOp::Ge | CmpOp::Lt | CmpOp::Le) => {
            let Ok(r) = rhs.parse::<f64>() else { return false };
            let l = n.as_f64().unwrap_or(0.0);
            match op {
                CmpOp::Gt => l > r,
                CmpOp::Ge => l >= r,
                CmpOp::Lt => l < r,
                CmpOp::Le => l <= r,
                _ => unreachable!(),
            }
        }
        (String(s), CmpOp::Gt | CmpOp::Ge | CmpOp::Lt | CmpOp::Le) => match op {
            CmpOp::Gt => s.as_str() > rhs,
            CmpOp::Ge => s.as_str() >= rhs,
            CmpOp::Lt => s.as_str() < rhs,
            CmpOp::Le => s.as_str() <= rhs,
            _ => unreachable!(),
        },
        _ => false,
    }
}

/// 数值/字典序闭区间判断（BETWEEN low AND high）。
fn between_value(doc_val: &Value, low: &str, high: &str) -> bool {
    use serde_json::Value::*;
    match doc_val {
        Number(n) => {
            let Ok(l) = low.parse::<f64>() else { return false };
            let Ok(h) = high.parse::<f64>() else { return false };
            let v = n.as_f64().unwrap_or(0.0);
            v >= l && v <= h
        }
        String(s) => s.as_str() >= low && s.as_str() <= high,
        _ => false,
    }
}

/// LIKE 行级判定（serde 路径）：仅字符串字段参与；`%` 通配任意长度串（含空串）。
fn like_value(doc_val: &Value, pattern: &str) -> bool {
    match doc_val {
        Value::String(s) => like_match(s, pattern),
        _ => false,
    }
}

/// SQL `%` 通配匹配（不支持 `_` 单字符通配，也不做 `ESCAPE` 转义——`%` 恒为通配符）。
/// 经典双指针贪心：`*`（%）= 任意长度串（含空）。`star` 记录最近 `%` 位置，
/// 失配时回退让 `%` 多吞一个字符重试。
pub(crate) fn like_match(text: &str, pattern: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let (n, m) = (t.len(), p.len());
    let (mut i, mut j) = (0usize, 0usize);
    let mut star: Option<usize> = None; // 最近 % 在 pattern 的下标
    let mut mark = 0usize; // star 对应时目标串位置
    while i < n {
        if j < m && p[j] == t[i] {
            i += 1;
            j += 1;
        } else if j < m && p[j] == '%' {
            star = Some(j);
            mark = i;
            j += 1;
        } else if let Some(s) = star {
            // 回退：让最近 % 多吞 t[i]，从其下一位置继续
            j = s + 1;
            mark += 1;
            i = mark;
        } else {
            return false;
        }
    }
    // 文本耗尽后，pattern 剩余须全为 %
    while j < m && p[j] == '%' {
        j += 1;
    }
    j == m
}

impl WhereExpr {
    /// 行级判定（7.95 聚合全量过滤用）：文档 JSON 值是否满足表达式（递归 AND/OR/NOT），
    /// 与 eval 位图语义一致（字段点路径；数值/字典序比较）。
    pub fn matches_doc(&self, doc: &Value) -> bool {
        match self {
            WhereExpr::Cond(c) => field_of(doc, &c.field)
                .map(|v| cmp_value(v, &c.op, &c.value))
                .unwrap_or(false),
            WhereExpr::Between { field, low, high } => field_of(doc, field)
                .map(|v| between_value(v, low, high))
                .unwrap_or(false),
            WhereExpr::Like { field, pattern } => field_of(doc, field)
                .map(|v| like_value(v, pattern))
                .unwrap_or(false),
            WhereExpr::Not(x) => !x.matches_doc(doc),
            WhereExpr::And(a, b) => a.matches_doc(doc) && b.matches_doc(doc),
            WhereExpr::Or(a, b) => a.matches_doc(doc) || b.matches_doc(doc),
        }
    }
}

/// 扫描叶子（比较/BETWEEN/LIKE——倒排无法表达，作后过滤/全量扫描）。
pub(crate) enum Leaf<'a> {
    Cmp(&'a Cond),
    Between { field: &'a str, low: &'a str, high: &'a str },
    Like { field: &'a str, pattern: &'a str },
}

/// 若表达式是裸扫描叶子则返回（用于 AND 后过滤快路径）。
pub(crate) fn scan_leaf<'a>(e: &'a WhereExpr) -> Option<Leaf<'a>> {
    match e {
        WhereExpr::Cond(c) if !matches!(c.op, CmpOp::Eq | CmpOp::Ne) => Some(Leaf::Cmp(c)),
        WhereExpr::Between { field, low, high } => Some(Leaf::Between { field, low, high }),
        WhereExpr::Like { field, pattern } => Some(Leaf::Like { field, pattern }),
        _ => None,
    }
}

/// 等值条件（非 docid，7.94）：倒排 term 未命中（数字/未索引字段）时视作扫描叶，
/// AND 快路径在其另一分支位图上后过滤，避免回退全表扫描再取交集。
fn as_eq_cond<'a>(e: &'a WhereExpr) -> Option<&'a Cond> {
    match e {
        WhereExpr::Cond(c) if matches!(c.op, CmpOp::Eq) && c.field != "docid" => Some(c),
        _ => None,
    }
}

/// 单文档判定：字段值是否满足扫描叶子。
fn leaf_passes(engine: &Engine, docid: u64, leaf: &Leaf) -> Result<bool> {
    let Some(raw) = engine.get(docid)? else { return Ok(false) };
    // 7.96：顶层单字段条件 → 字节级轻量判定（免 serde 整行）
    if let Some(r) = light_leaf_result(&raw, leaf) {
        return Ok(r);
    }
    let Ok(val) = serde_json::from_slice::<Value>(&raw) else { return Ok(false) };
    match leaf {
        Leaf::Cmp(c) => Ok(field_of(&val, &c.field)
            .map(|v| cmp_value(v, &c.op, &c.value))
            .unwrap_or(false)),
        Leaf::Between { field, low, high } => Ok(field_of(&val, field)
            .map(|v| between_value(v, low, high))
            .unwrap_or(false)),
        Leaf::Like { field, pattern } => Ok(field_of(&val, field)
            .map(|v| like_value(v, pattern))
            .unwrap_or(false)),
    }
}

/// 扫描行值直接判定（7.94/7.96）：下推/等值回退共用——优先字节级轻量判定
/// （顶层单字段，免整 doc 反序列化）；无法轻量 → 回退 serde 整行解析。
fn scan_row_matches(doc: &[u8], leaf: &Leaf) -> bool {
    if let Some(r) = light_leaf_result(doc, leaf) {
        return r;
    }
    let val = match serde_json::from_slice::<Value>(doc) {
        Ok(v) => v,
        Err(_) => return false,
    };
    match leaf {
        Leaf::Cmp(c) => field_of(&val, &c.field)
            .map(|v| cmp_value(v, &c.op, &c.value))
            .unwrap_or(false),
        Leaf::Between { field, low, high } => field_of(&val, field)
            .map(|v| between_value(v, low, high))
            .unwrap_or(false),
        Leaf::Like { field, pattern } => field_of(&val, field)
            .map(|v| like_value(v, pattern))
            .unwrap_or(false),
    }
}

/// 表达式轻量判定（7.96/7.97）：Cond/Between/And/Or/Not 递归字节级——
/// 每行按需扫顶层单字段叶，AND/OR 短路减少扫描；任何叶无法轻量（点路径/转义/畸形）
/// → None（调用方回退 serde）。
pub(crate) fn light_where_matches(doc: &[u8], e: &WhereExpr) -> Option<bool> {
    match e {
        WhereExpr::Cond(c) => light_leaf_result(doc, &Leaf::Cmp(c)),
        WhereExpr::Between { field, low, high } => {
            light_leaf_result(doc, &Leaf::Between { field, low, high })
        }
        WhereExpr::Like { field, pattern } => {
            light_leaf_result(doc, &Leaf::Like { field, pattern })
        }
        WhereExpr::Not(x) => light_where_matches(doc, x).map(|b| !b),
        WhereExpr::And(a, b) => {
            let la = light_where_matches(doc, a)?;
            if !la {
                return Some(false); // 短路
            }
            light_where_matches(doc, b)
        }
        WhereExpr::Or(a, b) => {
            let la = light_where_matches(doc, a)?;
            if la {
                return Some(true); // 短路
            }
            light_where_matches(doc, b)
        }
    }
}

/// 等值回退全量位图（7.94）：倒排 term 未命中 ≠ 0 行——数字字段等值（term 不建数字）
/// / 字段未索引时倒排为空，须单遍扫描收集**全部**命中（AND/OR/NOT 组合需完整集；
/// 看门狗熔断保护超长扫描）。
fn scan_backfill_bitmap(
    engine: &Engine,
    leaf: &Leaf,
    guard: &crate::watchdog::QueryGuard,
) -> Result<RoaringBitmap> {
    let mut bm = RoaringBitmap::new();
    let mut scanned = 0u64;
    engine.scan_stream(None, None, |docid, doc| {
        scanned += 1;
        if scanned % 4096 == 0 && guard.is_expired() {
            return Err(Error::QueryTooExpensive(format!(
                "类 SQL 等值回退扫描超时（已扫 {scanned} 条，熔断中止），建议改用倒排字段/枚举值"
            )));
        }
        if scan_row_matches(doc, leaf) {
            bm.insert(docid);
        }
        Ok(true)
    })?;
    Ok(bm)
}

/// 后过滤：只检查 `bitmap` 内已命中的文档（AND 快路径——扫描域 = 另一分支位图；逐批熔断）。
/// `limit` 下推：找到 limit 个命中即停——比较/BETWEEN 作后过滤时避免遍历全量命中集
/// （千万级库 status=active 上 LIMIT 50 的 BETWEEN 若全量遍历 = 数百秒，提前停 = 毫秒级）。
fn post_filter(
    engine: &Engine,
    bitmap: RoaringBitmap,
    leaf: &Leaf,
    limit: u64,
    guard: &crate::watchdog::QueryGuard,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut n = 0u64;
    for docid in bitmap {
        n += 1;
        if n % 4096 == 0 && guard.is_expired() {
            return Err(Error::QueryTooExpensive(format!(
                "类 SQL 后过滤超时（已查 {n} 条，熔断中止），建议缩小倒排等值条件范围"
            )));
        }
        if leaf_passes(engine, docid as u64, leaf)? {
            out.insert(docid);
            if out.len() as u64 >= limit {
                break;
            }
        }
    }
    Ok(out)
}

/// 全量扫描过滤（比较/BETWEEN 独立求值，如 OR 分支或单独条件）。
fn scan_all(
    engine: &Engine,
    leaf: &Leaf,
    limit: u64,
    guard: &crate::watchdog::QueryGuard,
) -> Result<RoaringBitmap> {
    let full = full_docids(engine, guard)?;
    post_filter(engine, full, leaf, limit, guard)
}

/// P1-E：将扫描叶子条件转换为 Zone Map 谓词（用于 SST 块级字段范围剪枝）。
/// 返回 `None` 表示该叶子不适合 Zone Map 剪枝（如等值/不等值，由倒排处理）。
fn leaf_to_zone_pred(leaf: &Leaf) -> Option<ZonePredicate> {
    match leaf {
        Leaf::Cmp(c) if !matches!(c.op, CmpOp::Eq | CmpOp::Ne) => {
            let (min, max) = match c.op {
                CmpOp::Gt => (Some(json_val_bytes(&c.value)), None),
                CmpOp::Ge => (Some(json_val_bytes(&c.value)), None),
                CmpOp::Lt => (None, Some(json_val_bytes(&c.value))),
                CmpOp::Le => (None, Some(json_val_bytes(&c.value))),
                _ => return None,
            };
            Some(ZonePredicate { field: c.field.clone(), min, max })
        }
        Leaf::Between { field, low, high } => {
            Some(ZonePredicate {
                field: field.to_string(),
                min: Some(json_val_bytes(low)),
                max: Some(json_val_bytes(high)),
            })
        }
        _ => None,
    }
}

/// P1-E：将 SQL 值字符串转为 JSON 序列化字节（匹配 FieldZone 存储格式）。
/// 数值原样保留（如 `"100"` → `b"100"`）；字符串加双引号（如 `"active"` → `b"\"active\""`）。
fn json_val_bytes(s: &str) -> Vec<u8> {
    // 尝试解析为数值
    if s.parse::<f64>().is_ok() {
        // JSON 数值序列化 = 原样
        s.as_bytes().to_vec()
    } else {
        // JSON 字符串需要双引号
        let mut b = Vec::with_capacity(s.len() + 2);
        b.push(b'"');
        b.extend_from_slice(s.as_bytes());
        b.push(b'"');
        b
    }
}

/// 谓词下推（7.93）：WHERE 为**裸比较/BETWEEN**（无倒排等值可收敛）时，单遍流式扫描 +
/// LIMIT/OFFSET 早停直接产出命中行（含 doc 值）——替代旧路径「scan 全量收集 docid →
/// 逐 docid 二次回表 get」，消除两遍 IO 与全量枚举（千万级库 `amount>90000 LIMIT 10`
/// 由全表熔断降至命中即停的毫秒级，对齐 MySQL 顺序扫表早停语义）。
/// 行级判定用扫描读出的主文档值（与引擎 scan / 倒排词条一致基于主文档）；
/// delta 字段 patch 场景与 scan 值语义一致（SQL 过滤基于主文档）。
pub(crate) fn scan_pushdown(
    engine: &Engine,
    leaf: &Leaf,
    limit: u64,
    offset: u64,
    guard: &crate::watchdog::QueryGuard,
) -> Result<Vec<QueryRow>> {
    let mut out: Vec<QueryRow> = Vec::new();
    let mut skipped = 0u64;
    let mut scanned = 0u64;
    let zp = leaf_to_zone_pred(leaf);
    // Task-025b 阶段④：范围/BETWEEN 谓词下推扫描走**跨文件扇出并行**（窗口命中 ≥2 SST
    // 时逐文件线程并行解码，主线程 k-way 归并；否则 CF 自动回退串行，零回归）。
    // P109 压测（50万×4 交错 L0）：w2~w4 近最优（行式 ~1.6×、PAX ~3×），w≥4 主线程
    // 归并/背压成瓶颈 → 上限取 4。
    let workers = std::thread::available_parallelism()
        .map(|n| n.get().clamp(2, 4))
        .unwrap_or(2);
    engine.scan_stream_parallel(None, None, workers, None, zp, |docid, doc| {
        scanned += 1;
        if scanned % 4096 == 0 && guard.is_expired() {
            return Err(Error::QueryTooExpensive(format!(
                "类 SQL 流式过滤超时（已扫 {scanned} 条，熔断中止），建议用倒排等值条件收敛范围"
            )));
        }
        if !scan_row_matches(doc, leaf) {
            return Ok(true);
        }
        if skipped < offset {
            skipped += 1;
            return Ok(true);
        }
        out.push((docid, doc.to_vec()));
        if out.len() as u64 >= limit {
            return Ok(false); // LIMIT 命中即停
        }
        Ok(true)
    })?;
    Ok(out)
}

/// 单条件求值：`=` 走倒排 posting（docid 特例点查）；`!=` 全量 − posting；比较/BETWEEN 扫描。
fn eval_cond(
    engine: &Engine,
    c: &Cond,
    limit: u64,
    guard: &crate::watchdog::QueryGuard,
) -> Result<RoaringBitmap> {
    match c.op {
        CmpOp::Eq => {
            if c.field == "docid" {
                let mut bm = RoaringBitmap::new();
                if let Ok(d) = c.value.parse::<u64>() {
                    bm.insert(d);
                }
                Ok(bm)
            } else {
                let hit = engine.inverted_posting(&format!("{}={}", c.field, c.value))?;
                if hit.is_empty() {
                    // 7.94 等值回退：倒排 term 未命中 ≠ 0 行——数字字段（term 不建数字）/
                    // 未索引字段等值须单遍扫描确认（对齐 MySQL 无索引等值全扫语义）
                    scan_backfill_bitmap(engine, &Leaf::Cmp(c), guard)
                } else {
                    Ok(hit)
                }
            }
        }
        CmpOp::Ne => {
            if c.field == "docid" {
                let full = full_docids(engine, guard)?;
                let mut bm = RoaringBitmap::new();
                if let Ok(d) = c.value.parse::<u64>() {
                    bm.insert(d);
                }
                return Ok(full - bm);
            }
            let full = full_docids(engine, guard)?;
            let hit = engine.inverted_posting(&format!("{}={}", c.field, c.value))?;
            if hit.is_empty() {
                // 7.94：数字/未索引字段 `!=` 倒排取反是全表（错误），回退扫描收集真实 != 命中
                scan_backfill_bitmap(engine, &Leaf::Cmp(c), guard)
            } else {
                Ok(full - hit)
            }
        }
        _ => scan_all(engine, &Leaf::Cmp(c), limit, guard),
    }
}


/// WHERE 求值 → 命中位图。
/// AND 快路径：比较/BETWEEN 分支作后过滤（只检查另一分支已命中文档，避免全量扫描）。
pub(crate) fn eval(
    engine: &Engine,
    e: &WhereExpr,
    limit: u64,
    guard: &crate::watchdog::QueryGuard,
) -> Result<RoaringBitmap> {
    match e {
        WhereExpr::Cond(c) => eval_cond(engine, c, limit, guard),
        WhereExpr::Between { field, low, high } => {
            scan_all(engine, &Leaf::Between { field, low, high }, limit, guard)
        }
        WhereExpr::Like { field, pattern } => {
            // LIKE 含 % → 倒排无法表达，全扫收集（AND 快路径在 eval_cond 上层已把 Like 当
            // 扫描叶后过滤，此处是裸 LIKE / OR / NOT 内组合的兜底）
            scan_all(engine, &Leaf::Like { field, pattern }, limit, guard)
        }
        WhereExpr::Not(x) => {
            let full = full_docids(engine, guard)?;
            let hit = eval(engine, x, limit, guard)?;
            Ok(full - hit)
        }
        WhereExpr::And(a, b) => {
            // 7.94：倒排未命中的等值（数字/未索引字段）视作扫描叶——在另一分支位图上
            // 后过滤（避免 eval_cond 回退全表扫描再交：active ∩ amount=xxx 从全扫降为
            // 候选集逐查）
            if let Some(c) = as_eq_cond(a) {
                if engine.inverted_posting(&format!("{}={}", c.field, c.value))?.is_empty() {
                    let base = eval(engine, b, limit, guard)?;
                    return post_filter(engine, base, &Leaf::Cmp(c), limit, guard);
                }
            }
            if let Some(c) = as_eq_cond(b) {
                if engine.inverted_posting(&format!("{}={}", c.field, c.value))?.is_empty() {
                    let base = eval(engine, a, limit, guard)?;
                    return post_filter(engine, base, &Leaf::Cmp(c), limit, guard);
                }
            }
            if let Some(leaf) = scan_leaf(a) {
                let base = eval(engine, b, limit, guard)?;
                return post_filter(engine, base, &leaf, limit, guard);
            }
            if let Some(leaf) = scan_leaf(b) {
                let base = eval(engine, a, limit, guard)?;
                return post_filter(engine, base, &leaf, limit, guard);
            }
            let la = eval(engine, a, limit, guard)?;
            let lb = eval(engine, b, limit, guard)?;
            Ok(la & lb)
        }
        WhereExpr::Or(a, b) => {
            let la = eval(engine, a, limit, guard)?;
            let lb = eval(engine, b, limit, guard)?;
            Ok(la | lb)
        }
    }
}
