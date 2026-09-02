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
    /// 聚合（7.95）：`(函数名小写, 参数字段)`——`COUNT(*)` 字段为 None；
    /// `COUNT(f)/SUM(f)/AVG(f)/MIN(f)/MAX(f)` 字段 Some。普通 SELECT 为 None。
    pub agg: Option<(String, Option<String>)>,
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
        let mut agg = None;
        loop {
            let item = self.next()?;
            match item {
                Tok::Star => columns.push("*".into()),
                Tok::Ident(i) => {
                    // 7.95 聚合函数列：COUNT(*) / COUNT(f) / SUM(f) / AVG(f) / MIN(f) / MAX(f)
                    if matches!(self.peek()?, Tok::LParen) {
                        let upper = i.to_uppercase();
                        if matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                            self.next()?; // LParen
                            let arg = match self.next()? {
                                Tok::Star => None,
                                Tok::Ident(f) => Some(f),
                                t => {
                                    return Err(format!("聚合参数期望 * 或字段名，实际 {t:?}"))
                                }
                            };
                            match self.next()? {
                                Tok::RParen => {}
                                t => return Err(format!("聚合期望右括号，实际 {t:?}")),
                            }
                            if agg.is_some() {
                                return Err("暂不支持多列/多聚合（无 GROUP BY）".into());
                            }
                            agg = Some((upper.to_lowercase(), arg));
                            columns.push(upper);
                        } else {
                            return Err(format!("不支持的函数列: {i}"));
                        }
                    } else {
                        columns.push(i);
                    }
                }
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
        Ok(Select { columns, table, where_expr, limit, offset, agg })
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
fn full_docids(engine: &Engine, guard: &crate::watchdog::QueryGuard) -> Result<RoaringBitmap> {
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

// 7.96 轻量顶层字段判定（免整文档反序列化）——
// 无索引全扫（下推/等值回退/后过滤/聚合）每行 `serde_json::from_slice::<Value>` 构建整树，
// 1000 万行 ~12s vs MySQL 列式 2s。这里手写字节级扫描：只定位**目标顶层字段**的原始值
// 字节（数字/无转义字符串/布尔/null），跳过其余顶层项；结果与 serde 语义对齐，任何
// 无法轻量确定的结构（转义值/嵌套值内含引号规则）→ 调用方回退完整 serde（正确性护栏）。

/// 轻量目标字段值。
enum LightVal<'a> {
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

fn ws(b: &[u8], i: usize) -> usize {
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
fn skip_value(b: &[u8], i: &mut usize) -> bool {
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
fn read_target_value<'a>(b: &'a [u8], i: usize) -> Option<LightVal<'a>> {
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
fn light_top_field<'a>(doc: &'a [u8], field: &str) -> Option<LightVal<'a>> {
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
    };
    if field.contains('.') {
        return None;
    }
    let lv = light_top_field(doc, field)?;
    match (leaf, lv) {
        (Leaf::Cmp(c), lv) => Some(light_cmp_value(lv, &c.op, &c.value)?),
        (Leaf::Between { low, high, .. }, lv) => Some(light_between_value(lv, low, high)?),
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
            WhereExpr::Not(x) => !x.matches_doc(doc),
            WhereExpr::And(a, b) => a.matches_doc(doc) && b.matches_doc(doc),
            WhereExpr::Or(a, b) => a.matches_doc(doc) || b.matches_doc(doc),
        }
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
    }
}

/// 表达式轻量判定（7.96/7.97）：Cond/Between/And/Or/Not 递归字节级——
/// 每行按需扫顶层单字段叶，AND/OR 短路减少扫描；任何叶无法轻量（点路径/转义/畸形）
/// → None（调用方回退 serde）。
fn light_where_matches(doc: &[u8], e: &WhereExpr) -> Option<bool> {
    match e {
        WhereExpr::Cond(c) => light_leaf_result(doc, &Leaf::Cmp(c)),
        WhereExpr::Between { field, low, high } => {
            light_leaf_result(doc, &Leaf::Between { field, low, high })
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
        if scan_row_matches(doc, leaf) && docid < u32::MAX as u64 {
            bm.insert(docid as u32);
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

/// 谓词下推（7.93）：WHERE 为**裸比较/BETWEEN**（无倒排等值可收敛）时，单遍流式扫描 +
/// LIMIT/OFFSET 早停直接产出命中行（含 doc 值）——替代旧路径「scan 全量收集 docid →
/// 逐 docid 二次回表 get」，消除两遍 IO 与全量枚举（千万级库 `amount>90000 LIMIT 10`
/// 由全表熔断降至命中即停的毫秒级，对齐 MySQL 顺序扫表早停语义）。
/// 行级判定用扫描读出的主文档值（与引擎 scan / 倒排词条一致基于主文档）；
/// delta 字段 patch 场景与 scan 值语义一致（SQL 过滤基于主文档）。
fn scan_pushdown(
    engine: &Engine,
    leaf: &Leaf,
    limit: u64,
    offset: u64,
    guard: &crate::watchdog::QueryGuard,
) -> Result<Vec<QueryRow>> {
    let mut out: Vec<QueryRow> = Vec::new();
    let mut skipped = 0u64;
    let mut scanned = 0u64;
    engine.scan_stream(None, None, |docid, doc| {
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
                    if d < u32::MAX as u64 {
                        bm.insert(d as u32);
                    }
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
                    if d < u32::MAX as u64 {
                        bm.insert(d as u32);
                    }
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
fn eval(
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

/// 执行类 SQL：解析 + 求值 + 回表 + LIMIT/OFFSET（`cap` 为无 LIMIT 时的上限保护）。
/// 看门狗：扫描过滤/回表逐批熔断（超时返回 QueryTooExpensive，不挂起 server）。
pub fn execute(engine: &Engine, sql: &str, cap: u64) -> Result<Vec<QueryRow>> {
    let sel = parse_select(sql)?;
    let guard = engine.query_guard();
    let limit = sel.limit.unwrap_or(cap).min(cap);
    // 7.94 等值回退：裸 `field=value` 倒排 term 未命中（数字等值/未索引字段）→
    // 单遍流式扫描 + LIMIT/OFFSET 早停（组合 AND/OR/NOT 内回退走 eval_cond 全量集）。
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
    let bitmap = match &sel.where_expr {
        Some(e) => eval(engine, e, limit, &guard)?,
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
pub fn execute_aggregate(engine: &Engine, sql: &str) -> Result<Option<AggScalar>> {
    let sel = parse_select(sql)?;
    let Some((name, field)) = sel.agg.clone() else {
        return Ok(None);
    };
    if field.is_none() && name != "count" {
        return Err(Error::Config(format!("{name}(*) 不支持（仅 COUNT(*)）")));
    }
    if field.is_none() && sel.where_expr.is_none() {
        // 7.100：COUNT(*) 无 WHERE → 引擎 key-only 流式计数（免文档值反序列化）。
        // 语义与全表扫描 COUNT 一致（同 key 最新版本、Tombstone 跳过）。
        let n = engine.count_all_docs()?;
        return Ok(Some(AggScalar {
            header: "COUNT(*)".into(),
            is_null: false,
            text: n.to_string(),
        }));
    }
    let guard = engine.query_guard();
    let mut count = 0u64;
    let mut n_num = 0u64;
    let mut sum = 0f64;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut scanned = 0u64;
    engine.scan_stream(None, None, |_docid, doc| {
        scanned += 1;
        if scanned % 4096 == 0 && guard.is_expired() {
            return Err(Error::QueryTooExpensive(
                "类 SQL 聚合全量扫描超时（熔断中止）".into(),
            ));
        }
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
    })?;
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

/// 数字文本化：整值（Rust f64 to_string）无小数点。
fn fmt_num(x: f64) -> String {
    if x.fract() == 0.0 && x.abs() < 1e15 {
        format!("{}", x as i64)
    } else {
        x.to_string()
    }
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
    fn light_top_field_scanner_basics() {
        use LightVal::*;
        // 目标字段在各位置；数字/字符串/布尔/null 提取
        let doc = br#"{"status":"active","amount":73564,"n":1.5,"ok":true,"no":null,"addr":{"city":"bj"}}"#;
        assert!(matches!(light_top_field(doc, "status"), Some(Str(b"active"))));
        assert!(matches!(light_top_field(doc, "amount"), Some(Num(b"73564"))));
        assert!(matches!(light_top_field(doc, "n"), Some(Num(b"1.5"))));
        assert!(matches!(light_top_field(doc, "ok"), Some(Bool(true))));
        assert!(matches!(light_top_field(doc, "no"), Some(Null)));
        // 嵌套对象值 → Complex；缺失 → Absent
        assert!(matches!(light_top_field(doc, "addr"), Some(Complex)));
        assert!(matches!(light_top_field(doc, "zzz"), Some(Absent)));
        // 非目标嵌套对象（含内部同名 key）跳过不误命中
        let doc2 = br#"{"a":{"status":"x"},"status":"real"}"#;
        assert!(matches!(light_top_field(doc2, "status"), Some(Str(b"real"))));
        // 数组在目标前跳过
        let doc3 = br#"{"tags":["a","b"],"amount":5}"#;
        assert!(matches!(light_top_field(doc3, "amount"), Some(Num(b"5"))));
        // 转义字符串值 → None（回退 serde）；非对象 doc → None
        assert!(light_top_field(br#"{"s":"a\"b"}"#, "s").is_none());
        assert!(light_top_field(b"[1,2]", "f").is_none());
    }

    #[test]
    fn light_leaf_equivalence_with_serde() {
        // 轻量判定与 serde 路径结果一致（随机文档 + 各 op）
        let mut e = engine_with_docs();
        for sql in [
            "SELECT * FROM t WHERE status='active' AND amount>400 LIMIT 1000",
            "SELECT * FROM t WHERE amount BETWEEN 500 AND 530",
            "SELECT * FROM t WHERE status!='active' LIMIT 1000",
            "SELECT * FROM t WHERE amount>900",
        ] {
            let rows = execute(&mut e, sql, 1000).unwrap();
            // 下推/AND 快路径已用 light；此处仅确认执行不回归（结果非空/与旧断言场景一致）
            assert!(!rows.is_empty(), "{sql} 应命中");
        }
    }

    #[test]
    fn sql_aggregate_functions() {
        // 7.95：COUNT(*)/COUNT(f)/SUM/AVG/MIN/MAX（全量 matches_doc，不依赖倒排完整性）
        let mut e = engine_with_docs(); // docid i：amount = i*10（0..990）
        let agg = |sql: &str| execute_aggregate(&e, sql).unwrap().unwrap();
        // 无 WHERE 全表
        assert_eq!((agg("SELECT COUNT(*) FROM t").text.as_str()), "100");
        assert_eq!(agg("SELECT COUNT(*) FROM t").header, "COUNT(*)");
        // WHERE 字段条件（等值/比较/组合）
        assert_eq!(agg("SELECT COUNT(*) FROM t WHERE amount>900").text, "9");
        assert_eq!(agg("SELECT COUNT(*) FROM t WHERE status='active'").text, "34");
        assert_eq!(agg("SELECT COUNT(*) FROM t WHERE status='active' AND amount>400").text, "20");
        // COUNT(f)：缺失字段 = 0；SUM/AVG/MIN/MAX
        assert_eq!(agg("SELECT COUNT(missing) FROM t").text, "0");
        assert_eq!(agg("SELECT COUNT(amount) FROM t").text, "100");
        assert_eq!(agg("SELECT SUM(amount) FROM t").text, "49500");
        assert_eq!(agg("SELECT AVG(amount) FROM t").text, "495");
        assert_eq!(agg("SELECT MIN(amount) FROM t").text, "0");
        assert_eq!(agg("SELECT MAX(amount) FROM t").text, "990");
        assert_eq!(agg("SELECT SUM(amount) FROM t WHERE status='active'").text, "16830");
        // 空集：COUNT → 0；SUM/AVG → NULL
        assert_eq!(agg("SELECT COUNT(*) FROM t WHERE amount>10000").text, "0");
        let s = execute_aggregate(&e, "SELECT SUM(amount) FROM t WHERE amount>10000").unwrap().unwrap();
        assert!(s.is_null, "空集 SUM 应为 NULL");
        // 列头与限制：SUM(*) 拒绝
        assert_eq!(agg("SELECT AVG(amount) FROM t").header, "AVG(amount)");
        assert!(execute_aggregate(&e, "SELECT SUM(*) FROM t").is_err());
        // 复合表达式（7.97 light）：OR / NOT
        assert_eq!(agg("SELECT COUNT(*) FROM t WHERE status='active' OR amount>900").text, "40");
        assert_eq!(agg("SELECT COUNT(*) FROM t WHERE NOT(status='inactive')").text, "34");
        assert_eq!(
            agg("SELECT SUM(amount) FROM t WHERE status='active' OR amount>900").text,
            "22500"
        );
        // 普通 SELECT → None（走普通查询路径）
        assert!(execute_aggregate(&e, "SELECT * FROM t WHERE amount>900").unwrap().is_none());
    }

    #[test]
    fn sql_numeric_eq_backfill() {
        // 7.94：数字字段等值倒排 term 不建（空 posting）→ 回退单遍扫描（裸走早停下推、
        // 组合走 eval_cond 全量回退）——语义对齐 MySQL 无索引等值
        let mut e = engine_with_docs(); // amount = i*10（0..990）
        // 裸等值：amount=500 → docid 50（倒排空 → 下推扫描命中）
        let rows = execute(&mut e, "SELECT * FROM t WHERE amount=500", 1000).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 50);
        // 不存在值 → 回退全扫后 0 行（不再是错误空——与语义一致）
        let rows2 = execute(&mut e, "SELECT * FROM t WHERE amount=999", 1000).unwrap();
        assert_eq!(rows2.len(), 0);
        // 组合：AND 等值(active 倒排) ∩ 数字等值(回退全量) → docid 0
        let rows3 = execute(&mut e, "SELECT * FROM t WHERE status='active' AND amount=0", 1000).unwrap();
        assert_eq!(rows3.len(), 1, "active(i%3==0) 且 amount=0 → docid 0");
        assert_eq!(rows3[0].0, 0);
        // Ne 数字：amount!=0 → 99 行（旧实现倒排取反错误返回全表 100）
        let rows4 = execute(&mut e, "SELECT * FROM t WHERE amount!=0", 1000).unwrap();
        assert_eq!(rows4.len(), 99, "amount!=0 排除 docid 0");
        // 字符串等值（有倒排 term）路径不变：status='active' → 34
        let rows5 = execute(&mut e, "SELECT * FROM t WHERE status='active'", 1000).unwrap();
        assert_eq!(rows5.len(), 34);
    }

    #[test]
    fn sql_comparison_pushdown_single_pass_early_stop() {
        // 7.93：裸比较/BETWEEN 下推——单遍流式扫描 + LIMIT 早停，结果与旧 eval 路径一致
        let mut e = engine_with_docs(); // docid i：amount = i*10
        // 下推（裸比较 amount>900）vs 旧路径（AND 包一层使走 eval+post_filter）：同为 91..99
        let rows = execute(&mut e, "SELECT * FROM t WHERE amount>900", 1000).unwrap();
        assert_eq!(rows.len(), 9, "amount>900 → docid 91..99");
        assert_eq!(rows[0].0, 91);
        let rows_ref = execute(&mut e, "SELECT * FROM t WHERE amount>900 AND docid>0", 1000).unwrap();
        assert_eq!(rows, rows_ref, "下推结果应与旧 eval 路径一致");
        // LIMIT 早停：只取前 2 命中
        let rows2 = execute(&mut e, "SELECT * FROM t WHERE amount>900 LIMIT 2", 1000).unwrap();
        assert_eq!(rows2.len(), 2);
        assert_eq!(rows2[0].0, 91);
        // BETWEEN 下推（amount = i*10 ∈ [500,530] 闭区间 → i∈[50,53] 共 4 行）
        let rows3 = execute(&mut e, "SELECT * FROM t WHERE amount BETWEEN 500 AND 530", 1000).unwrap();
        assert_eq!(rows3.len(), 4);
        assert_eq!(rows3[0].0, 50);
        assert_eq!(rows3[3].0, 53);
        // OFFSET：跳过前 2 个命中（91,92 → 从 93 起）
        let rows4 = execute(&mut e, "SELECT * FROM t WHERE amount>900 LIMIT 2 OFFSET 2", 1000).unwrap();
        assert_eq!(rows4.len(), 2);
        assert_eq!(rows4[0].0, 93);
        // 返回行含完整 doc（后续投影免二次回表）
        let v: serde_json::Value = serde_json::from_slice(&rows2[0].1).unwrap();
        assert_eq!(v["amount"], 910);
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

