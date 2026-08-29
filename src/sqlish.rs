//! 类 SQL 解析器（design 157/1358 行：SELECT ... WHERE AND/OR 子集，走倒排/组合索引）。
//!
//! 语法子集（递归下降，零外部依赖——不引入 sqlparser-rs 大依赖）：
//!   SELECT [*|col1,col2] FROM <表名> [WHERE <expr>] [LIMIT n [OFFSET m]]
//!   expr  := and (OR and)*   and := unary (AND unary)*   unary := NOT unary | '(' expr ')' | cond
//!   cond  := 字段 op 值     op := = != > < >= <= | BETWEEN low AND high（闭区间，数值）
//!        值 := '字面量' | "字面量" | 裸词 | 数字
//!
//! 求值语义（内部走倒排，与 search_term_paged 同源）：
//!   - `field=value` → 倒排 posting（Roaring 位图）；AND=交集 / OR=并集 / NOT=补集（相对全量）；
//!   - `field!=value` → 全量 − posting；
//!   - 比较（>/</>=/<=）与 BETWEEN → 倒排无法表达，扫描过滤；**AND 快路径**：作为后过滤
//!     只检查另一分支（倒排等值）已命中的文档，避免全量扫描（推荐写法：
//!     `WHERE 枚举等值 AND 数值 BETWEEN ...`）；
//!   - `docid=123` 特例 → 主键点查单例位图；
//!   - LIMIT/OFFSET 作用于最终位图（与分页语义一致）。
//!
//! 不承诺 MySQL 方言：不支持 JOIN / GROUP BY / 子查询 / 事务（design 157 行明确）。

use crate::engine::{Engine, QueryRow};
use crate::error::{Error, Result};
use roaring::RoaringBitmap;
use serde_json::Value;

/// 解析器内部结果（错误为人类可读 String，入口统一转 crate::Error::Config）。
type PRes<T> = std::result::Result<T, String>;

// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cond {
    pub field: String,
    pub op: CmpOp,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WhereExpr {
    Cond(Cond),
    /// `field BETWEEN low AND high`（闭区间，数值/字典序）。
    Between { field: String, low: String, high: String },
    Not(Box<WhereExpr>),
    And(Box<WhereExpr>, Box<WhereExpr>),
    Or(Box<WhereExpr>, Box<WhereExpr>),
}

/// 解析结果（SELECT 语句）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Select {
    /// 列清单（"*" = 全部）。
    pub columns: Vec<String>,
    pub table: String,
    pub where_expr: Option<WhereExpr>,
    pub limit: Option<u64>,
    pub offset: u64,
}

// ---------------------------------------------------------------------------
// 词法
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Str(String),
    Num(u64),
    Star,
    Comma,
    LParen,
    RParen,
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
    Kw(String),
    Eof,
}

struct Lexer {
    chars: Vec<char>,
    pos: usize,
}

impl Lexer {
    fn new(sql: &str) -> Self {
        Self { chars: sql.chars().collect(), pos: 0 }
    }
    fn skip_ws(&mut self) {
        while self.pos < self.chars.len() && self.chars[self.pos].is_whitespace() {
            self.pos += 1;
        }
    }
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }
    fn next_tok(&mut self) -> PRes<Tok> {
        self.skip_ws();
        let c = match self.peek() {
            Some(c) => c,
            None => return Ok(Tok::Eof),
        };
        if c == '\'' || c == '"' {
            self.pos += 1;
            let mut s = String::new();
            while let Some(ch) = self.peek() {
                if ch == c {
                    self.pos += 1;
                    return Ok(Tok::Str(s));
                }
                s.push(ch);
                self.pos += 1;
            }
            return Err("未闭合字符串字面量".into());
        }
        let two: String = self.chars[self.pos..].iter().take(2).collect();
        match two.as_str() {
            "!=" => {
                self.pos += 2;
                return Ok(Tok::Ne);
            }
            ">=" => {
                self.pos += 2;
                return Ok(Tok::Ge);
            }
            "<=" => {
                self.pos += 2;
                return Ok(Tok::Le);
            }
            _ => {}
        }
        match c {
            '*' => {
                self.pos += 1;
                return Ok(Tok::Star);
            }
            ',' => {
                self.pos += 1;
                return Ok(Tok::Comma);
            }
            '(' => {
                self.pos += 1;
                return Ok(Tok::LParen);
            }
            ')' => {
                self.pos += 1;
                return Ok(Tok::RParen);
            }
            '=' => {
                self.pos += 1;
                return Ok(Tok::Eq);
            }
            '>' => {
                self.pos += 1;
                return Ok(Tok::Gt);
            }
            '<' => {
                self.pos += 1;
                return Ok(Tok::Lt);
            }
            _ => {}
        }
        if c.is_alphanumeric() || c == '_' || c == '.' {
            let start = self.pos;
            while let Some(ch) = self.peek() {
                if ch.is_alphanumeric() || ch == '_' || ch == '.' {
                    self.pos += 1;
                } else {
                    break;
                }
            }
            let word: String = self.chars[start..self.pos].iter().collect();
            if word.chars().all(|ch| ch.is_ascii_digit()) {
                return Ok(Tok::Num(word.parse().unwrap_or(0)));
            }
            let upper = word.to_uppercase();
            if matches!(upper.as_str(), "SELECT" | "FROM" | "WHERE" | "AND" | "OR" | "NOT" | "LIMIT" | "OFFSET" | "BETWEEN") {
                return Ok(Tok::Kw(upper));
            }
            return Ok(Tok::Ident(word));
        }
        Err(format!("无法识别的字符: {c}"))
    }
}

// ---------------------------------------------------------------------------
// 递归下降解析
// ---------------------------------------------------------------------------

struct Parser {
    lex: Lexer,
    peeked: Option<Tok>,
}

impl Parser {
    fn new(sql: &str) -> Self {
        Self { lex: Lexer::new(sql), peeked: None }
    }
    fn next(&mut self) -> PRes<Tok> {
        if let Some(t) = self.peeked.take() {
            return Ok(t);
        }
        self.lex.next_tok()
    }
    fn peek(&mut self) -> PRes<&Tok> {
        if self.peeked.is_none() {
            self.peeked = Some(self.lex.next_tok()?);
        }
        Ok(self.peeked.as_ref().unwrap())
    }
    fn push_back(&mut self, t: Tok) {
        self.peeked = Some(t);
    }
    fn expect_kw(&mut self, kw: &str) -> PRes<()> {
        let t = self.next()?;
        if let Tok::Kw(k) = &t {
            if k == kw {
                return Ok(());
            }
        }
        Err(format!("期望关键字 {kw}，实际 {t:?}"))
    }
    fn ident(&mut self) -> PRes<String> {
        match self.next()? {
            Tok::Ident(i) => Ok(i),
            Tok::Num(n) => Ok(n.to_string()),
            t => Err(format!("期望字段名，实际 {t:?}")),
        }
    }
    fn parse_select(&mut self) -> PRes<Select> {
        self.expect_kw("SELECT")?;
        let mut columns = Vec::new();
        loop {
            match self.next()? {
                Tok::Star => columns.push("*".into()),
                Tok::Ident(i) => columns.push(i),
                t => return Err(format!("期望列名或 *，实际 {t:?}")),
            }
            match self.next()? {
                Tok::Comma => continue,
                t => {
                    self.push_back(t);
                    break;
                }
            }
        }
        self.expect_kw("FROM")?;
        let table = self.ident()?;
        let mut where_expr = None;
        let mut limit = None;
        let mut offset = 0;
        loop {
            match self.peek()? {
                Tok::Kw(k) if k == "WHERE" => {
                    self.next()?;
                    where_expr = Some(self.parse_expr()?);
                }
                Tok::Kw(k) if k == "LIMIT" => {
                    self.next()?;
                    match self.next()? {
                        Tok::Num(n) => limit = Some(n),
                        t => return Err(format!("LIMIT 后期望数字，实际 {t:?}")),
                    }
                }
                Tok::Kw(k) if k == "OFFSET" => {
                    self.next()?;
                    match self.next()? {
                        Tok::Num(n) => offset = n,
                        t => return Err(format!("OFFSET 后期望数字，实际 {t:?}")),
                    }
                }
                Tok::Eof => break,
                t => return Err(format!("意外 token {t:?}")),
            }
        }
        Ok(Select { columns, table, where_expr, limit, offset })
    }
    fn parse_expr(&mut self) -> PRes<WhereExpr> {
        self.parse_or()
    }
    fn parse_or(&mut self) -> PRes<WhereExpr> {
        let mut left = self.parse_and()?;
        while matches!(self.peek()?, Tok::Kw(k) if k == "OR") {
            self.next()?;
            let right = self.parse_and()?;
            left = WhereExpr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }
    fn parse_and(&mut self) -> PRes<WhereExpr> {
        let mut left = self.parse_unary()?;
        while matches!(self.peek()?, Tok::Kw(k) if k == "AND") {
            self.next()?;
            let right = self.parse_unary()?;
            left = WhereExpr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }
    fn parse_unary(&mut self) -> PRes<WhereExpr> {
        if matches!(self.peek()?, Tok::Kw(k) if k == "NOT") {
            self.next()?;
            let inner = self.parse_unary()?;
            return Ok(WhereExpr::Not(Box::new(inner)));
        }
        if matches!(self.peek()?, Tok::LParen) {
            self.next()?;
            let e = self.parse_expr()?;
            if !matches!(self.next()?, Tok::RParen) {
                return Err("期望右括号 )".into());
            }
            return Ok(e);
        }
        self.parse_cond()
    }
    fn parse_cond(&mut self) -> PRes<WhereExpr> {
        let field = self.ident()?;
        // BETWEEN：`field BETWEEN low AND high`（闭区间）
        if matches!(self.peek()?, Tok::Kw(k) if k == "BETWEEN") {
            self.next()?;
            let low = self.value()?;
            if !matches!(self.next()?, Tok::Kw(k) if k == "AND") {
                return Err("BETWEEN 缺 AND 分隔".into());
            }
            let high = self.value()?;
            return Ok(WhereExpr::Between { field, low, high });
        }
        let op = match self.next()? {
            Tok::Eq => CmpOp::Eq,
            Tok::Ne => CmpOp::Ne,
            Tok::Gt => CmpOp::Gt,
            Tok::Lt => CmpOp::Lt,
            Tok::Ge => CmpOp::Ge,
            Tok::Le => CmpOp::Le,
            t => return Err(format!("期望比较运算符，实际 {t:?}")),
        };
        let value = self.value()?;
        Ok(WhereExpr::Cond(Cond { field, op, value }))
    }

    fn value(&mut self) -> PRes<String> {
        match self.next()? {
            Tok::Str(s) => Ok(s),
            Tok::Ident(s) => Ok(s),
            Tok::Num(n) => Ok(n.to_string()),
            t => Err(format!("期望值，实际 {t:?}")),
        }
    }
}

/// 解析入口：`parse_select("SELECT * FROM t WHERE status='active' AND amount>100 LIMIT 10")`。
pub fn parse_select(sql: &str) -> Result<Select> {
    let mut p = Parser::new(sql);
    let sel = p
        .parse_select()
        .map_err(|e| Error::Config(format!("类 SQL 解析失败: {e}")))?;
    if !matches!(p.next(), Ok(Tok::Eof)) {
        return Err(Error::Config("SQL 末尾存在多余 token".into()));
    }
    Ok(sel)
}

// ---------------------------------------------------------------------------
// 求值（引擎版：倒排 posting 位图 + 比较运算扫描过滤）
// ---------------------------------------------------------------------------

/// 全量 docid 位图（NOT/!=/比较运算的论域；逐批熔断防挂起）。
fn full_docids(engine: &mut Engine, guard: &crate::watchdog::QueryGuard) -> Result<RoaringBitmap> {
    let mut bm = RoaringBitmap::new();
    let mut scanned = 0u64;
    for (docid, _) in engine.scan_range(None, None)? {
        scanned += 1;
        if scanned % 4096 == 0 && guard.is_expired() {
            return Err(Error::QueryTooExpensive("类 SQL 全量扫描超时（熔断中止），建议用倒排等值条件收敛范围".into()));
        }
        if docid < u32::MAX as u64 {
            bm.insert(docid as u32);
        }
    }
    Ok(bm)
}

fn field_of<'a>(doc: &'a Value, field: &str) -> Option<&'a Value> {
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

/// 扫描叶子（比较/BETWEEN——倒排无法表达，作后过滤/全量扫描）。
enum Leaf<'a> {
    Cmp(&'a Cond),
    Between { field: &'a str, low: &'a str, high: &'a str },
}

/// 若表达式是裸扫描叶子则返回（用于 AND 后过滤快路径）。
fn scan_leaf<'a>(e: &'a WhereExpr) -> Option<Leaf<'a>> {
    match e {
        WhereExpr::Cond(c) if !matches!(c.op, CmpOp::Eq | CmpOp::Ne) => Some(Leaf::Cmp(c)),
        WhereExpr::Between { field, low, high } => Some(Leaf::Between { field, low, high }),
        _ => None,
    }
}

/// 单文档判定：字段值是否满足扫描叶子。
fn leaf_passes(engine: &mut Engine, docid: u64, leaf: &Leaf) -> Result<bool> {
    let Some(raw) = engine.get(docid)? else { return Ok(false) };
    let Ok(val) = serde_json::from_slice::<Value>(&raw) else { return Ok(false) };
    match leaf {
        Leaf::Cmp(c) => Ok(field_of(&val, &c.field)
            .map(|v| cmp_value(v, &c.op, &c.value))
            .unwrap_or(false)),
        Leaf::Between { field, low, high } => Ok(field_of(&val, field)
            .map(|v| between_value(v, low, high))
            .unwrap_or(false)),
    }
}

/// 后过滤：只检查 `bitmap` 内已命中的文档（AND 快路径——扫描域 = 另一分支位图；逐批熔断）。
fn post_filter(
    engine: &mut Engine,
    bitmap: RoaringBitmap,
    leaf: &Leaf,
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
        }
    }
    Ok(out)
}

/// 全量扫描过滤（比较/BETWEEN 独立求值，如 OR 分支或单独条件）。
fn scan_all(
    engine: &mut Engine,
    leaf: &Leaf,
    guard: &crate::watchdog::QueryGuard,
) -> Result<RoaringBitmap> {
    let full = full_docids(engine, guard)?;
    post_filter(engine, full, leaf, guard)
}

/// 单条件求值：`=` 走倒排 posting（docid 特例点查）；`!=` 全量 − posting；比较/BETWEEN 扫描。
fn eval_cond(engine: &mut Engine, c: &Cond, guard: &crate::watchdog::QueryGuard) -> Result<RoaringBitmap> {
    match c.op {
        CmpOp::Eq => {
            if c.field == "docid" {
                let mut bm = RoaringBitmap::new();
                if let Ok(d) = c.value.parse::<u64>() {
                    if d < u32::MAX as u64 {
                        bm.insert(d as u32);
                    }
                }
                Ok(bm)
            } else {
                engine.inverted_posting(&format!("{}={}", c.field, c.value))
            }
        }
        CmpOp::Ne => {
            let full = full_docids(engine, guard)?;
            let hit = if c.field == "docid" {
                let mut bm = RoaringBitmap::new();
                if let Ok(d) = c.value.parse::<u64>() {
                    if d < u32::MAX as u64 {
                        bm.insert(d as u32);
                    }
                }
                bm
            } else {
                engine.inverted_posting(&format!("{}={}", c.field, c.value))?
            };
            Ok(full - hit)
        }
        _ => scan_all(engine, &Leaf::Cmp(c), guard),
    }
}

/// WHERE 求值 → 命中位图。
/// AND 快路径：比较/BETWEEN 分支作后过滤（只检查另一分支已命中文档，避免全量扫描）。
fn eval(engine: &mut Engine, e: &WhereExpr, guard: &crate::watchdog::QueryGuard) -> Result<RoaringBitmap> {
    match e {
        WhereExpr::Cond(c) => eval_cond(engine, c, guard),
        WhereExpr::Between { field, low, high } => {
            scan_all(engine, &Leaf::Between { field, low, high }, guard)
        }
        WhereExpr::Not(x) => {
            let full = full_docids(engine, guard)?;
            let hit = eval(engine, x, guard)?;
            Ok(full - hit)
        }
        WhereExpr::And(a, b) => {
            if let Some(leaf) = scan_leaf(a) {
                let base = eval(engine, b, guard)?;
                return post_filter(engine, base, &leaf, guard);
            }
            if let Some(leaf) = scan_leaf(b) {
                let base = eval(engine, a, guard)?;
                return post_filter(engine, base, &leaf, guard);
            }
            let la = eval(engine, a, guard)?;
            let lb = eval(engine, b, guard)?;
            Ok(la & lb)
        }
        WhereExpr::Or(a, b) => {
            let la = eval(engine, a, guard)?;
            let lb = eval(engine, b, guard)?;
            Ok(la | lb)
        }
    }
}

/// 执行类 SQL：解析 + 求值 + 回表 + LIMIT/OFFSET（`cap` 为无 LIMIT 时的上限保护）。
/// 看门狗：扫描过滤/回表逐批熔断（超时返回 QueryTooExpensive，不挂起 server）。
pub fn execute(engine: &mut Engine, sql: &str, cap: u64) -> Result<Vec<QueryRow>> {
    let sel = parse_select(sql)?;
    let guard = engine.query_guard();
    let limit = sel.limit.unwrap_or(cap).min(cap);
    let bitmap = match &sel.where_expr {
        Some(e) => eval(engine, e, &guard)?,
        None => full_docids(engine, &guard)?,
    };
    let mut rows = Vec::new();
    let mut skipped = 0u64;
    for docid in bitmap {
        if rows.len() as u64 >= limit {
            break;
        }
        if skipped < sel.offset {
            skipped += 1;
            continue;
        }
        if let Some(v) = engine.get(docid as u64)? {
            rows.push((docid as u64, v));
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn engine_with_docs() -> Engine {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let cities = ["beijing", "shanghai", "shenzhen"];
        for i in 0..100u64 {
            let city = cities[(i % 3) as usize];
            let doc = serde_json::json!({
                "docid": i,
                "status": if i % 3 == 0 { "active" } else { "inactive" },
                "city": city,
                "amount": i * 10,
                "note": format!("note-{i}"),
            });
            let terms: Vec<String> = crate::server::extract_terms(&doc);
            let refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put(i, serde_json::to_vec(&doc).unwrap(), &refs).unwrap();
        }
        e
    }

    #[test]
    fn sql_and_uses_inverted_intersection() {
        let mut e = engine_with_docs();
        let rows = execute(&mut e, "SELECT * FROM t WHERE status='active' AND city='beijing' LIMIT 100", 1000).unwrap();
        assert_eq!(rows.len(), 34, "active 且 beijing 交集（i%3==0）");
    }

    #[test]
    fn sql_or_and_not_complement() {
        let mut e = engine_with_docs();
        let rows = execute(&mut e, "SELECT * FROM t WHERE status='active' OR amount>900", 1000).unwrap();
        // 34 active ∪ 9 amount>900（其中 93/96/99 已含）→ 40
        assert_eq!(rows.len(), 40, "OR 并集去重");
        let rows2 = execute(&mut e, "SELECT * FROM t WHERE NOT (status='active')", 1000).unwrap();
        assert_eq!(rows2.len(), 66, "NOT 补集");
    }

    #[test]
    fn sql_comparison_scan_and_docid() {
        let mut e = engine_with_docs();
        let rows = execute(&mut e, "SELECT * FROM t WHERE amount>=500 AND amount<600", 1000).unwrap();
        assert_eq!(rows.len(), 10, "amount∈[500,600)");
        let rows2 = execute(&mut e, "SELECT * FROM t WHERE docid=42", 1000).unwrap();
        assert_eq!(rows2.len(), 1, "docid 点查单例");
        assert_eq!(rows2[0].0, 42);
    }

    #[test]
    fn sql_between_numeric_range_and_fast_path() {
        let mut e = engine_with_docs();
        // BETWEEN 闭区间（数值）
        let rows = execute(&mut e, "SELECT * FROM t WHERE amount BETWEEN 500 AND 600", 1000).unwrap();
        assert_eq!(rows.len(), 11, "amount∈[500,600] 闭区间 = docid 50..60");
        // AND 快路径：倒排等值收敛 + BETWEEN 后过滤（不做全量扫描）
        let rows2 = execute(&mut e, "SELECT * FROM t WHERE status='active' AND amount BETWEEN 0 AND 30", 1000).unwrap();
        let mut ids: Vec<u64> = rows2.iter().map(|r| r.0).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 3], "active 且 amount<=30");
    }

    #[test]
    fn sql_paging_and_parse_errors() {
        let mut e = engine_with_docs();
        let p1 = execute(&mut e, "SELECT * FROM t WHERE status='active' LIMIT 5", 1000).unwrap();
        let p2 = execute(&mut e, "SELECT * FROM t WHERE status='active' LIMIT 5 OFFSET 5", 1000).unwrap();
        assert_eq!(p1.len(), 5);
        assert_eq!(p2.len(), 5);
        assert_ne!(p1[0].0, p2[0].0, "分页不重叠");
        assert!(parse_select("SELECT * FROM t JOIN x").is_err(), "JOIN 拒绝");
        assert!(parse_select("SELECT * FROM t GROUP BY city").is_err(), "GROUP BY 拒绝");
    }
}

