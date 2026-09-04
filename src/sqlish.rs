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
// 64 位 docid 域（多表 docid = table_id<<48|row，查询层位图必须 u64）
use roaring::treemap::RoaringTreemap as RoaringBitmap;
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

/// HAVING 原子条件（AF#5）：左项可为**聚合列头**（如 `COUNT(*)`/`SUM(amount)`，
/// 须与 SELECT 聚合列一致）或**分组字段名**；值为数字或字符串字面量。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HavingCond {
    pub lhs: String,
    pub op: CmpOp,
    pub value: String,
}

/// HAVING 表达式（分组结果上的过滤，支持 AND/OR/NOT/括号组合）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HavingExpr {
    Cond(HavingCond),
    Not(Box<HavingExpr>),
    And(Box<HavingExpr>, Box<HavingExpr>),
    Or(Box<HavingExpr>, Box<HavingExpr>),
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
    /// ORDER BY 排序项（开发顺序 #1/#3）：`(字段, 是否 DESC)`。
    pub order_by: Vec<(String, bool)>,
    /// GROUP BY 分组字段（AF#2 单字段 → AF#4 多字段；空 = 无分组）。聚合列见
    /// `group_aggs`；顺序即分组层级（排序/去重键）。
    pub group_by: Vec<String>,
    /// GROUP BY 聚合列（AF#2~#4 支持 COUNT/SUM/AVG/MIN/MAX；每个 `(函数名小写, 参数字段)`，
    /// `COUNT(*)` 字段为 None）。无 GROUP BY 时为空，标量聚合走 `agg`。
    pub group_aggs: Vec<(String, Option<String>)>,
    /// GROUP BY 后的 HAVING 过滤（AF#5；None = 不过滤）。仅配合 GROUP BY。
    pub having: Option<HavingExpr>,
    /// P0-D：JOIN 规格（None = 无 JOIN）。支持 INNER/LEFT JOIN。
    pub join: Option<JoinClause>,
}

/// P0-D：JOIN 子句（`t1 INNER JOIN t2 ON t1.f1 = t2.f2`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinClause {
    /// JOIN 类型（INNER / LEFT）。
    pub join_type: JoinKind,
    /// 从表名。
    pub right_table: String,
    /// 主表关联字段。
    pub left_field: String,
    /// 从表关联字段。
    pub right_field: String,
}

/// P0-D：JOIN 类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
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
            if matches!(upper.as_str(), "SELECT" | "FROM" | "WHERE" | "AND" | "OR" | "NOT" | "LIMIT" | "OFFSET" | "BETWEEN" | "JOIN" | "ON") {
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
    /// P0-D：期望标点（如 `=`）。
    fn expect_punct(&mut self, p: &str) -> PRes<()> {
        let t = self.next()?;
        match (&t, p) {
            (Tok::Eq, "=") => return Ok(()),
            (Tok::Ident(s), _) if s == p => return Ok(()),
            _ => {}
        }
        Err(format!("期望 '{p}'，实际 {t:?}"))
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
        let mut plain: Vec<String> = Vec::new();
        let mut aggs: Vec<(String, Option<String>)> = Vec::new();
        let mut star_seen = false;
        loop {
            let item = self.next()?;
            match item {
                Tok::Star => {
                    columns.push("*".into());
                    star_seen = true;
                }
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
                            aggs.push((upper.to_lowercase(), arg));
                            columns.push(upper);
                        } else {
                            return Err(format!("不支持的函数列: {i}"));
                        }
                    } else {
                        plain.push(i.clone());
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
        // P0-D：JOIN 解析（`[INNER|LEFT] JOIN t2 ON t1.f1 = t2.f2`）
        let mut join = None;
        loop {
            match self.peek()? {
                Tok::Ident(k) if k.eq_ignore_ascii_case("inner") => {
                    self.next()?;
                    self.expect_kw("JOIN")?;
                    let right_table = self.ident()?;
                    self.expect_kw("ON")?;
                    let left_field = self.ident()?;
                    self.expect_punct("=")?;
                    let right_field = self.ident()?;
                    join = Some(JoinClause {
                        join_type: JoinKind::Inner,
                        right_table,
                        left_field,
                        right_field,
                    });
                }
                Tok::Ident(k) if k.eq_ignore_ascii_case("left") => {
                    self.next()?;
                    self.expect_kw("JOIN")?;
                    let right_table = self.ident()?;
                    self.expect_kw("ON")?;
                    let left_field = self.ident()?;
                    self.expect_punct("=")?;
                    let right_field = self.ident()?;
                    join = Some(JoinClause {
                        join_type: JoinKind::Left,
                        right_table,
                        left_field,
                        right_field,
                    });
                }
                Tok::Kw(k) if k == "JOIN" => {
                    self.next()?;
                    let right_table = self.ident()?;
                    self.expect_kw("ON")?;
                    let left_field = self.ident()?;
                    self.expect_punct("=")?;
                    let right_field = self.ident()?;
                    join = Some(JoinClause {
                        join_type: JoinKind::Inner,
                        right_table,
                        left_field,
                        right_field,
                    });
                }
                _ => break,
            }
        }
        let mut where_expr = None;
        let mut limit = None;
        let mut offset = 0;
        let mut order_by = Vec::new();
        let mut group_by: Vec<String> = Vec::new();
        let mut having = None;
        loop {
            match self.peek()? {
                Tok::Kw(k) if k == "WHERE" => {
                    self.next()?;
                    where_expr = Some(self.parse_expr()?);
                }
                Tok::Ident(k) if k.eq_ignore_ascii_case("group") => {
                    // AF#2 单字段 → AF#4 多字段：GROUP BY f1, f2, ...（顺序即层级）
                    self.next()?; // 消费 group
                    match self.next()? {
                        Tok::Ident(k) if k.eq_ignore_ascii_case("by") => {}
                        t => return Err(format!("GROUP 后期望 BY，实际 {t:?}")),
                    }
                    if !group_by.is_empty() {
                        return Err("重复 GROUP BY".into());
                    }
                    loop {
                        let f = self.ident()?;
                        if group_by.iter().any(|x| x == &f) {
                            return Err(format!("GROUP BY 字段重复: {f}"));
                        }
                        group_by.push(f);
                        match self.next()? {
                            Tok::Comma => continue,
                            t => {
                                self.push_back(t);
                                break;
                            }
                        }
                    }
                }
                Tok::Ident(k) if k.eq_ignore_ascii_case("having") => {
                    // AF#5：HAVING <expr>（分组后过滤；左项 = 聚合列头或分组字段）
                    self.next()?; // 消费 having
                    if having.is_some() {
                        return Err("重复 HAVING".into());
                    }
                    having = Some(self.parse_having()?);
                }
                Tok::Ident(k) if k.eq_ignore_ascii_case("order") => {
                    // ORDER BY f1 [ASC|DESC], f2 [ASC|DESC], ...
                    self.next()?; // 消费 order
                    match self.next()? {
                        Tok::Ident(k) if k.eq_ignore_ascii_case("by") => {}
                        t => return Err(format!("ORDER 后期望 BY，实际 {t:?}")),
                    }
                    loop {
                        let f = self.ident()?;
                        let mut desc = false;
                        if let Tok::Ident(d) = self.peek()? {
                            if d.eq_ignore_ascii_case("desc") {
                                desc = true;
                                self.next()?;
                            } else if d.eq_ignore_ascii_case("asc") {
                                self.next()?;
                            }
                        }
                        order_by.push((f, desc));
                        match self.next()? {
                            Tok::Comma => continue,
                            t => {
                                self.push_back(t);
                                break;
                            }
                        }
                    }
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
        // 组装：无 GROUP BY → 单标量聚合（7.95 兼容）；有 GROUP BY → 组聚合列清单。
        let mut agg = None;
        let group_aggs = aggs.clone();
        if !group_by.is_empty() {
            if star_seen {
                return Err("SELECT * 与 GROUP BY 混用不支持（须显式分组字段）".into());
            }
            for p in &plain {
                if !group_by.iter().any(|g| g == p) {
                    return Err(format!(
                        "非分组列 {p} 须属于 GROUP BY 字段（{}）或只选聚合列",
                        group_by.join(", ")
                    ));
                }
            }
            for (n, f) in &aggs {
                let up = n.to_uppercase();
                if !matches!(up.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                    return Err(format!("GROUP BY 不支持的聚合: {n}"));
                }
                if f.is_none() && up != "COUNT" {
                    return Err(format!("{n}(*) 不支持（仅 COUNT(*)）"));
                }
            }
        } else if aggs.len() > 1 {
            return Err("暂不支持多列/多聚合（无 GROUP BY）".into());
        } else {
            agg = aggs.into_iter().next();
        }
        if having.is_some() && group_by.is_empty() {
            return Err("HAVING 需配合 GROUP BY（本期不支持无分组的 HAVING）".into());
        }
        Ok(Select {
            columns,
            table,
            where_expr,
            limit,
            offset,
            agg,
            order_by,
            group_by,
            group_aggs,
            having,
            join,
        })
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
        // `f IN (v1, v2, …)`：解析期展开为 OR 等值链（复用既有 Cond 求值/分组路径，
        // 含倒排收敛/下推/聚合/HAVING/ORDER BY；数值与字符串等值语义同 `f = v`）。
        let is_in = matches!(
            self.peek()?,
            Tok::Ident(k) if k.eq_ignore_ascii_case("in")
        );
        if is_in {
            self.next()?; // in
            if !matches!(self.next()?, Tok::LParen) {
                return Err("IN 后期望 (".into());
            }
            let mut conds: Vec<WhereExpr> = Vec::new();
            loop {
                let v = self.value()?;
                conds.push(WhereExpr::Cond(Cond {
                    field: field.clone(),
                    op: CmpOp::Eq,
                    value: v,
                }));
                match self.next()? {
                    Tok::Comma => continue,
                    Tok::RParen => break,
                    t => return Err(format!("IN 列表期望 , 或 )，实际 {t:?}")),
                }
            }
            let mut it = conds.into_iter();
            let first = it.next().ok_or_else(|| "IN 列表不能为空".to_string())?;
            let mut acc = first;
            for c in it {
                acc = WhereExpr::Or(Box::new(acc), Box::new(c));
            }
            return Ok(acc);
        }
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

    // ---------- HAVING（AF#5）：分组后过滤（左项 = 聚合列头或分组字段） ----------
    fn parse_having(&mut self) -> PRes<HavingExpr> {
        self.parse_having_or()
    }
    fn parse_having_or(&mut self) -> PRes<HavingExpr> {
        let mut left = self.parse_having_and()?;
        while matches!(self.peek()?, Tok::Kw(k) if k == "OR") {
            self.next()?;
            let right = self.parse_having_and()?;
            left = HavingExpr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }
    fn parse_having_and(&mut self) -> PRes<HavingExpr> {
        let mut left = self.parse_having_unary()?;
        while matches!(self.peek()?, Tok::Kw(k) if k == "AND") {
            self.next()?;
            let right = self.parse_having_unary()?;
            left = HavingExpr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }
    fn parse_having_unary(&mut self) -> PRes<HavingExpr> {
        if matches!(self.peek()?, Tok::Kw(k) if k == "NOT") {
            self.next()?;
            let inner = self.parse_having_unary()?;
            return Ok(HavingExpr::Not(Box::new(inner)));
        }
        if matches!(self.peek()?, Tok::LParen) {
            self.next()?;
            let e = self.parse_having()?;
            if !matches!(self.next()?, Tok::RParen) {
                return Err("HAVING 期望右括号 )".into());
            }
            return Ok(e);
        }
        self.parse_having_cond()
    }
    fn parse_having_cond(&mut self) -> PRes<HavingExpr> {
        // 左项：聚合函数 COUNT(f)/SUM(f)/AVG(f)/MIN(f)/MAX(f) 或分组字段名。
        let i = self.ident()?;
        let lhs = if matches!(self.peek()?, Tok::LParen) {
            let upper = i.to_uppercase();
            if !matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                return Err(format!("HAVING 不支持的函数: {i}"));
            }
            self.next()?; // LParen
            let arg = match self.next()? {
                Tok::Star => "*".to_string(),
                Tok::Ident(f) => f,
                t => return Err(format!("HAVING 聚合参数期望 * 或字段名，实际 {t:?}")),
            };
            if !matches!(self.next()?, Tok::RParen) {
                return Err("HAVING 聚合期望右括号 )".into());
            }
            format!("{upper}({arg})")
        } else {
            i
        };
        let op = match self.next()? {
            Tok::Eq => CmpOp::Eq,
            Tok::Ne => CmpOp::Ne,
            Tok::Gt => CmpOp::Gt,
            Tok::Lt => CmpOp::Lt,
            Tok::Ge => CmpOp::Ge,
            Tok::Le => CmpOp::Le,
            t => return Err(format!("HAVING 期望比较运算符，实际 {t:?}")),
        };
        let value = self.value()?;
        Ok(HavingExpr::Cond(HavingCond { lhs, op, value }))
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

/// P0-D：JOIN 执行（参考 research/optimizer_proces.md 8 阶段流程）。
/// 阶段 1：主表 WHERE 独立产出候选集（eval → bitmap）
/// 阶段 2：JOIN 路径——主表候选 batch_get → 提取关联 key → 从表点查/倒排查 → Hash 合并
/// 阶段 3：LIMIT 下推
/// 安全阀：非等值 JOIN 拒绝、表数 ≥3 拒绝
fn execute_join(engine: &Engine, sel: &Select, cap: u64) -> Result<Vec<QueryRow>> {
    let join = sel.join.as_ref().unwrap();
    // 剥离表前缀（`orders.user_id` → `user_id`）
    let left_field = join.left_field.rsplit('.').next().unwrap_or(&join.left_field);
    let right_field = join.right_field.rsplit('.').next().unwrap_or(&join.right_field);
    let limit = sel.limit.unwrap_or(cap).min(cap);
    let guard = engine.query_guard();

    // 阶段 1：主表 WHERE 产出候选 docid 集
    let left_bitmap = match &sel.where_expr {
        Some(e) => eval(engine, e, limit, &guard)?,
        None => full_docids(engine, &guard)?,
    };
    if left_bitmap.is_empty() {
        return Ok(Vec::new());
    }

    // 阶段 2：主表 batch_get → 提取关联 key → 从表点查 → Hash 合并
    let left_docids: Vec<u64> = left_bitmap.iter().map(|d| d as u64).collect();
    let left_docs = engine.batch_get(&left_docids)?;
    let mut right_cache: std::collections::HashMap<String, Option<Vec<u8>>> =
        std::collections::HashMap::new();
    let mut keys: Vec<Option<String>> = Vec::with_capacity(left_docs.len());
    for doc in &left_docs {
        if let Some(d) = doc {
            let key = extract_join_key(d, left_field);
            keys.push(key.clone());
            if let Some(k) = &key {
                if !right_cache.contains_key(k) {
                    right_cache.insert(k.clone(), None);
                }
            }
        } else {
            keys.push(None);
        }
    }
    // 从表关联查询：right_field = "docid" → 主键点查；否则 → 倒排 term
    let unique_keys: Vec<String> = right_cache.keys().cloned().collect();
    for k in &unique_keys {
        if right_field == "docid" || right_field == "id" {
            if let Ok(docid) = k.parse::<u64>() {
                right_cache.insert(k.clone(), engine.get(docid)?);
            }
        } else {
            let term = format!("{}={}", right_field, k);
            let posting = engine.inverted_posting(&term)?;
            if let Some(docid) = posting.iter().next() {
                right_cache.insert(k.clone(), engine.get(docid as u64)?);
            }
        }
    }
    // 合并
    let mut out = Vec::new();
    for (doc_opt, key) in left_docs.into_iter().zip(keys.into_iter()) {
        let Some(doc) = doc_opt else { continue };
        let right = match &key {
            Some(k) => right_cache.get(k).cloned().flatten(),
            None => None,
        };
        match join.join_type {
            JoinKind::Inner => {
                if let Some(rv) = right {
                    let merged = merge_join_doc(&doc, &rv, &sel.table, &join.right_table);
                    out.push((0, merged)); // docid 在 JOIN 结果中不直接有意义
                }
            }
            JoinKind::Left => {
                let merged = match right {
                    Some(rv) => merge_join_doc(&doc, &rv, &sel.table, &join.right_table),
                    None => doc,
                };
                out.push((0, merged));
            }
        }
        if out.len() as u64 >= limit {
            break;
        }
    }
    Ok(out)
}

/// P0-D：从文档 JSON 提取 JOIN 关联 key。
fn extract_join_key(doc: &[u8], field: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(doc).ok()?;
    match v.get(field) {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
        Some(serde_json::Value::Bool(b)) => Some(b.to_string()),
        _ => None,
    }
}

/// P0-D：合并 JOIN 结果文档（左表字段 + 右表字段嵌套）。
fn merge_join_doc(left: &[u8], right: &[u8], left_table: &str, right_table: &str) -> Vec<u8> {
    let lv: serde_json::Value = serde_json::from_slice(left).unwrap_or(serde_json::Value::Null);
    let rv: serde_json::Value = serde_json::from_slice(right).unwrap_or(serde_json::Value::Null);
    serde_json::to_vec(&serde_json::json!({
        left_table: lv,
        right_table: rv,
    }))
    .unwrap_or_else(|_| left.to_vec())
}

/// P0-A：提取 WHERE 中所有等值条件（field=value），返回 (field, value) 列表。
fn extract_eq_conds(e: &WhereExpr) -> Vec<(String, String)> {
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

/// P0-A：声明式组合索引路由。
/// 检查 WHERE 等值条件是否匹配 engine 的 composite_indexes 最左前缀。
/// 匹配时走 `query_by_composite_prefix`（cidx 前缀扫描 → 回表），避免全扫/逐行过滤。
/// 返回 Ok(Some(rows)) = 命中并执行；Ok(None) = 不匹配，回退原路径。
fn try_composite_index(
    engine: &Engine,
    sel: &Select,
    cap: u64,
) -> Result<Option<Vec<QueryRow>>> {
    if engine.composite_indexes.is_empty() || sel.where_expr.is_none() {
        return Ok(None);
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

/// ORDER BY 候选集上限（防全库排序撑爆内存；超限报错提示 WHERE 收敛）。
const SORT_MAX_ROWS: usize = 200_000;

/// 排序键：Null(缺省/非数值非字符串) < Num < Str。
#[derive(Debug, Clone)]
enum SortKey {
    Null,
    Num(f64),
    Str(String),
}

/// 从文档 JSON 顶层字段取排序键（数值→Num，字符串→Str，其余/缺省→Null）。
fn sort_key(doc: &[u8], field: &str) -> SortKey {
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

/// 执行类 SQL：解析 + 求值 + 回表 + LIMIT/OFFSET（`cap` 为无 LIMIT 时的上限保护）。
/// 看门狗：扫描过滤/回表逐批熔断（超时返回 QueryTooExpensive，不挂起 server）。
pub fn execute(engine: &Engine, sql: &str, cap: u64) -> Result<Vec<QueryRow>> {
    let sel = parse_select(sql)?;
    if !sel.group_by.is_empty() {
        return Err(Error::Config(
            "GROUP BY 查询须经分组执行入口 execute_group_by".into(),
        ));
    }
    // P0-A：声明式组合索引路由——WHERE 等值前缀匹配 composite_indexes 时走 cidx 前缀扫描。
    // 匹配规则：提取 WHERE 中所有等值条件 → 按 composite_indexes 最左前缀匹配 → 取最长匹配。
    if let Some(rows) = try_composite_index(engine, &sel, cap)? {
        return Ok(rows);
    }
    // P0-D：JOIN 路由——有 JOIN 子句时走 execute_join（参考 research/optimizer_proces.md 阶段 2）
    if sel.join.is_some() {
        return execute_join(engine, &sel, cap);
    }
    let guard = engine.query_guard();
    let limit = sel.limit.unwrap_or(cap).min(cap);
    let sort = !sel.order_by.is_empty();
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
    let bitmap = match &sel.where_expr {
        Some(e) => {
            let cap_bits = if sort {
                SORT_MAX_ROWS as u64 + sel.offset + limit
            } else {
                limit
            };
            eval(engine, e, cap_bits, &guard)?
        }
        None => full_docids(engine, &guard)?,
    };
    if sort {
        if bitmap.len() as usize > SORT_MAX_ROWS {
            return Err(Error::QueryTooExpensive(format!(
                "ORDER BY 候选集过大（{} 行，上限 {}），请加 WHERE 收敛或用 LIMIT",
                bitmap.len(),
                SORT_MAX_ROWS
            )));
        }
        let mut srows: Vec<SortRow> = Vec::with_capacity(bitmap.len() as usize);
        for docid in bitmap {
            if let Some(v) = engine.get(docid as u64)? {
                let keys = sel
                    .order_by
                    .iter()
                    .map(|(f, _)| sort_key(&v, f))
                    .collect();
                srows.push(SortRow { docid: docid as u64, doc: v, keys });
            }
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
/// P1-3：带 docid 区间窗口的标量聚合（非默认表按表区间执行；`start/end` 均为 None = 全库）。
/// 窗口非空时禁用倒排统计快路径与 `count_all_docs` 快路径（两者为引擎全库口径，跨表会
/// 串表）——强制窗口内全扫，语义与按表区间聚合一致。
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
    if field.is_none() && sel.where_expr.is_none() {
        if scoped {
            // COUNT(*) 无 WHERE（表区间版）：docid 窗口 keys-only 计数（免文档值反序列化）
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
    let guard = engine.query_guard();
    let mut count = 0u64;
    let mut n_num = 0u64;
    let mut sum = 0f64;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut scanned = 0u64;
    engine.scan_stream(start, end, |_docid, doc| {
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

/// 全库标量聚合（兼容入口 = 无窗口）。
pub fn execute_aggregate(engine: &Engine, sql: &str) -> Result<Option<AggScalar>> {
    execute_aggregate_window(engine, sql, None, None)
}

/// 数字文本化：整值（Rust f64 to_string）无小数点。
fn fmt_num(x: f64) -> String {
    if x.fract() == 0.0 && x.abs() < 1e15 {
        format!("{}", x as i64)
    } else {
        x.to_string()
    }
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
    // 排序：ORDER BY 仅限分组字段（= g）；DESC 反转（NULL 组随之移末）
    for (f, _) in &sel.order_by {
        if f != g {
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
    let guard = engine.query_guard();
    // 复合组键 = 各分组 level 键向量；每组持每聚合列一个累积器（与 specs 对齐）。
    let mut groups: std::collections::HashMap<Vec<GroupKey>, Vec<AggState>> =
        std::collections::HashMap::new();
    let mut scanned = 0u64;
    engine.scan_stream(start, end, |_docid, doc| {
        scanned += 1;
        if scanned % 4096 == 0 && guard.is_expired() {
            return Err(Error::QueryTooExpensive(
                "GROUP BY 全量扫描超时（熔断中止）".into(),
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
        let key: Vec<GroupKey> = fields.iter().map(|f| group_key_of(doc, f)).collect();
        let states = groups.entry(key).or_insert_with(|| {
            (0..specs.len()).map(|_| AggState::new()).collect()
        });
        for (idx, (name, fld)) in specs.iter().enumerate() {
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
        if groups.len() as u64 > cap {
            return Err(Error::QueryTooExpensive(format!(
                "GROUP BY 分组数超过上限（{} 组，上限 {cap}），请加 WHERE 收敛",
                groups.len()
            )));
        }
        Ok(true)
    })?;
    let mut list: Vec<(Vec<GroupKey>, Vec<AggState>)> = groups.into_iter().collect();
    // HAVING（AF#5）：分组完成后、排序/切片前过滤组行。
    if let Some(h) = &sel.having {
        list.retain(|(k, sts)| having_matches(h, &fields, k, &specs, sts));
    }
    // 组行排序：优先级 = ORDER BY 序列（每项须为分组字段）→ 剩余分组 level 升序补尾。
    // DESC 反转该 level（Null 随之移末，对齐 MySQL DESC）。
    let mut order_seq: Vec<(usize, bool)> = Vec::new();
    for (f, desc) in &sel.order_by {
        let Some(idx) = fields.iter().position(|x| x == f) else {
            return Err(Error::Config(format!(
                "GROUP BY 结果排序字段 {f} 须属于分组字段（{}）",
                fields.join(", ")
            )));
        };
        if !order_seq.iter().any(|(i, _)| i == &idx) {
            order_seq.push((idx, *desc));
        }
    }
    for (i, _) in fields.iter().enumerate() {
        if !order_seq.iter().any(|(j, _)| j == &i) {
            order_seq.push((i, false));
        }
    }
    list.sort_by(|a, b| {
        for (idx, desc) in &order_seq {
            let mut ord = cmp_group_key(&a.0[*idx], &b.0[*idx]);
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
    fn sql_in_clause_filter_group_and() {
        // SQL `WHERE f IN (…)`：过滤（含 AND 交集、数值）、与 GROUP BY 组合。
        let mut e = engine_with_docs();
        let rows = execute(&mut e, "SELECT * FROM t WHERE city IN ('beijing','shanghai') LIMIT 1000", 1000)
            .unwrap();
        assert_eq!(rows.len(), 67, "beijing(34)+shanghai(33)");
        let r2 = execute(
            &mut e,
            "SELECT * FROM t WHERE status='active' AND city IN ('beijing','shenzhen') LIMIT 1000",
            1000,
        )
        .unwrap();
        assert_eq!(r2.len(), 34, "active∩beijing（shenzhen 无 active）");
        let r3 = execute(&mut e, "SELECT * FROM t WHERE amount IN (500, 900)", 1000).unwrap();
        let mut ids: Vec<u64> = r3.iter().map(|x| x.0).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![50, 90], "数值 IN");
        // GROUP BY + IN（分组查询 WHERE 集合过滤）
        let gr = execute_group_by(
            &mut e,
            "SELECT city, COUNT(*) FROM t WHERE city IN ('shanghai','shenzhen') GROUP BY city",
            1000,
        )
        .unwrap()
        .unwrap();
        let keys: Vec<&str> = gr.rows.iter().map(|r| r.keys[0].as_deref().unwrap()).collect();
        assert_eq!(keys, vec!["shanghai", "shenzhen"]);
        let cnts: Vec<u64> = gr
            .rows
            .iter()
            .map(|r| r.cells[0].as_ref().unwrap().parse().unwrap())
            .collect();
        assert_eq!(cnts, vec![33, 33]);
        // 语法错误：空列表 / 缺右括号
        assert!(parse_select("SELECT * FROM t WHERE city IN ()").is_err());
        assert!(parse_select("SELECT * FROM t WHERE city IN ('a'").is_err());
    }

    #[test]
    fn group_by_fast_inverted_matches_scan() {
        // Ex-9.3 ④b：无 WHERE 单字段 GROUP BY 倒排快路径结果与全扫一致（含 NULL 组；
        // 数值聚合遇缺字段行自动回退全扫）。
        let mk = |stats: bool| -> (crate::engine::Engine, tempfile::TempDir) {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = crate::config::Config::default();
            if stats {
                cfg.inverted.stats_fields = vec!["amount".to_string()];
            }
            let mut e = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
            let put = |e: &mut crate::engine::Engine, id: u64, st: Option<&str>, amt: Option<f64>| {
                let mut d = serde_json::json!({});
                if let Some(s) = st {
                    d["status"] = serde_json::json!(s);
                }
                if let Some(a) = amt {
                    d["amount"] = serde_json::json!(a);
                }
                let b = serde_json::to_vec(&d).unwrap();
                let t: Vec<&str> = match st {
                    Some("active") => vec!["status=active"],
                    Some("inactive") => vec!["status=inactive"],
                    _ => Vec::new(),
                };
                e.put_nosync(id, b, &t).unwrap();
            };
            put(&mut e, 1, Some("active"), Some(10.0));
            put(&mut e, 2, Some("active"), Some(20.0));
            put(&mut e, 3, Some("active"), None);
            put(&mut e, 4, Some("inactive"), Some(5.0));
            put(&mut e, 5, None, Some(7.0)); // 缺 status → NULL 组（含数值贡献）
            (e, dir)
        };
        let enc = |r: &GroupRow| -> (Vec<Option<String>>, Vec<Option<String>>) {
            (r.keys.clone(), r.cells.clone())
        };
        let (mut es, _d1) = mk(true);
        let (mut en, _d2) = mk(false);
        for sql in [
            "SELECT status, COUNT(*) FROM t GROUP BY status",
            "SELECT status, SUM(amount) FROM t GROUP BY status",
            "SELECT status, COUNT(*), SUM(amount) FROM t GROUP BY status",
            "SELECT status, COUNT(*) FROM t GROUP BY status HAVING COUNT(*) > 1",
        ] {
            let a = execute_group_by(&mut es, sql, 1000).unwrap().unwrap();
            let b = execute_group_by(&mut en, sql, 1000).unwrap().unwrap();
            let ra: Vec<_> = a.rows.iter().map(enc).collect();
            let rb: Vec<_> = b.rows.iter().map(enc).collect();
            assert_eq!(ra, rb, "{sql} 快路径应与全扫一致");
        }
        // NULL 组精确：COUNT(*) 无 WHERE 下缺 status 的 doc5 → NULL 组 count=1
        let gr = execute_group_by(&mut es, "SELECT status, COUNT(*) FROM t GROUP BY status", 1000)
            .unwrap()
            .unwrap();
        let null_row = gr.rows.iter().find(|r| r.keys[0].is_none()).expect("应有 NULL 组");
        assert_eq!(null_row.cells[0].as_deref(), Some("1"), "NULL 组计 1 行（doc5）");
    }

    #[test]
    fn stats_load_fast_path_matches_scan() {
        // Ex-9.3 第③步：SUM/AVG/MIN/MAX ... WHERE f='v'（裸等值）走倒排统计载荷，
        // 与无统计全扫路径数值一致（stats_fields 声明字段才路由，否则回落全扫）。
        let put = |e: &mut crate::engine::Engine, id: u64, st: &str, amt: Option<f64>| {
            let mut d = serde_json::json!({"status": st});
            if let Some(a) = amt {
                d["amount"] = serde_json::json!(a);
            }
            let b = serde_json::to_vec(&d).unwrap();
            let t: &[&str] = &[if st == "active" { "status=active" } else { "status=inactive" }];
            e.put_nosync(id, b, t).unwrap();
        };
        // 有统计配置
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.inverted.stats_fields = vec!["amount".to_string()];
        let mut es = crate::engine::Engine::open(dir.path(), &cfg).unwrap();
        put(&mut es, 1, "active", Some(10.0));
        put(&mut es, 2, "active", Some(20.0));
        put(&mut es, 3, "active", None); // 缺 amount：不参与数值聚合（MySQL NULL 语义）
        put(&mut es, 4, "inactive", Some(5.0));
        let agg = |e: &mut crate::engine::Engine, sql: &str| {
            execute_aggregate(e, sql).unwrap().unwrap()
        };
        assert_eq!(agg(&mut es, "SELECT SUM(amount) FROM t WHERE status='active'").text, "30");
        assert_eq!(agg(&mut es, "SELECT AVG(amount) FROM t WHERE status='active'").text, "15");
        assert_eq!(agg(&mut es, "SELECT MIN(amount) FROM t WHERE status='active'").text, "10");
        assert_eq!(agg(&mut es, "SELECT MAX(amount) FROM t WHERE status='active'").text, "20");
        assert_eq!(agg(&mut es, "SELECT SUM(amount) FROM t WHERE status='inactive'").text, "5");
        // 无统计配置：全扫回落，数值一致
        let dir2 = tempfile::tempdir().unwrap();
        let mut en = crate::engine::Engine::open(dir2.path(), &crate::config::Config::default()).unwrap();
        put(&mut en, 1, "active", Some(10.0));
        put(&mut en, 2, "active", Some(20.0));
        put(&mut en, 3, "active", None);
        put(&mut en, 4, "inactive", Some(5.0));
        for sql in [
            "SELECT SUM(amount) FROM t WHERE status='active'",
            "SELECT AVG(amount) FROM t WHERE status='active'",
            "SELECT MAX(amount) FROM t WHERE status='active'",
            "SELECT SUM(amount) FROM t WHERE status='inactive'",
        ] {
            let a = agg(&mut es, sql);
            let b = agg(&mut en, sql);
            assert_eq!(a.text, b.text, "{sql} 快路径应等于全扫");
            assert_eq!(a.is_null, b.is_null, "{sql} NULL 语义一致");
        }
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

    // ---------- ORDER BY（开发顺序 #1：单字段 + LIMIT） ----------

    #[test]
    fn order_by_amount_desc_limit() {
        let mut e = engine_with_docs();
        let rows = execute(&mut e, "SELECT * FROM t ORDER BY amount DESC LIMIT 3", 1000).unwrap();
        assert_eq!(rows.len(), 3);
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![99, 98, 97], "amount 降序取前 3（最大 990/980/970）");
    }

    #[test]
    fn order_by_numeric_asc_with_where_and_offset() {
        let mut e = engine_with_docs();
        // WHERE 收敛后排序 + OFFSET/LIMIT 切片
        let rows = execute(
            &mut e,
            "SELECT * FROM t WHERE amount>=500 AND amount<600 ORDER BY amount ASC LIMIT 2 OFFSET 1",
            1000,
        )
        .unwrap();
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![51, 52], "amount 500..590 升序，跳过最小 50，取 51/52");
    }

    #[test]
    fn order_by_string_field_asc() {
        let mut e = engine_with_docs();
        let rows = execute(&mut e, "SELECT * FROM t ORDER BY note ASC LIMIT 3", 1000).unwrap();
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![0, 1, 10], "note 字典序（note-0 < note-1 < note-10 < note-2，同 MySQL）");
        // DESC：note-99/98 最大，倒序前 2
        let rows2 = execute(&mut e, "SELECT * FROM t ORDER BY note DESC LIMIT 2", 1000).unwrap();
        let ids2: Vec<u64> = rows2.iter().map(|r| r.0).collect();
        assert_eq!(ids2, vec![99, 98], "note 降序取前 2");
        // 缺省字段全 NULL（ASC 均排最前且彼此相等）→ 稳定序返回
        let rows3 = execute(&mut e, "SELECT * FROM t ORDER BY nokey ASC LIMIT 2", 1000).unwrap();
        let ids3: Vec<u64> = rows3.iter().map(|r| r.0).collect();
        assert_eq!(ids3, vec![0, 1], "缺省字段全 NULL，稳定序返回");
    }

    #[test]
    fn order_by_parse_multi_field_and_case() {
        let s = parse_select("SELECT * FROM t WHERE amount>1 ORDER BY amount DESC, note ASC LIMIT 5").unwrap();
        assert_eq!(s.order_by, vec![("amount".into(), true), ("note".into(), false)]);
        assert_eq!(s.limit, Some(5));
        let s2 = parse_select("SELECT * FROM t order by note LIMIT 1").unwrap();
        assert_eq!(s2.order_by, vec![("note".into(), false)], "小写 order by 亦可");
    }

    // ---------- ORDER BY 多字段（开发顺序 AF#3：多键 comparator 终验） ----------

    #[test]
    fn order_by_multi_field_first_key_then_second() {
        // 首键 city 升序，beijing 组内按 amount 降序打破并列
        // （若只看单键 amount DESC，全局前三应为 99/98/97——多键语义须回到 beijing 组内）。
        let mut e = engine_with_docs();
        let rows = execute(
            &mut e,
            "SELECT * FROM t ORDER BY city ASC, amount DESC LIMIT 3",
            1000,
        )
        .unwrap();
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![99, 96, 93], "city 升序并列由 amount 降序打破");
        // 反向：amount 升序主键、city 升序次键——amount 全表唯一 → 退化为纯 amount 序
        let rows2 = execute(
            &mut e,
            "SELECT * FROM t ORDER BY amount ASC, city ASC LIMIT 3",
            1000,
        )
        .unwrap();
        let ids2: Vec<u64> = rows2.iter().map(|r| r.0).collect();
        assert_eq!(ids2, vec![0, 1, 2], "amount 无并列，city 次键不影响结果");
    }

    #[test]
    fn order_by_multi_field_desc_with_where_and_limit() {
        // WHERE 收敛（amount∈[500,600) → docid 50..59）后：city DESC（shenzhen 组最前）
        // 且组内 amount DESC → shenzhen(59/56/53/50) 前三 59,56,53。
        let mut e = engine_with_docs();
        let rows = execute(
            &mut e,
            "SELECT * FROM t WHERE amount>=500 AND amount<600 ORDER BY city DESC, amount DESC LIMIT 3",
            1000,
        )
        .unwrap();
        let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(ids, vec![59, 56, 53], "shenzhen 组 amount 降序前 3");
        // OFFSET 在排序后切片（跳过 59/56 → 53 起）
        let rows2 = execute(
            &mut e,
            "SELECT * FROM t WHERE amount>=500 AND amount<600 ORDER BY city DESC, amount DESC LIMIT 2 OFFSET 2",
            1000,
        )
        .unwrap();
        let ids2: Vec<u64> = rows2.iter().map(|r| r.0).collect();
        assert_eq!(ids2, vec![53, 50], "OFFSET 2 跳过 59/56");
    }

    // ---------- GROUP BY（开发顺序 AF#2：单字段 + COUNT/SUM） ----------
    // fixture：docid 0..99；city 循环 beijing/shanghai/shenzhen；status active 当 i%3==0；
    // amount = i*10（全表 0..990，总和 49500）；note = note-{i}。

    #[test]
    fn group_by_count_single_field() {
        let mut e = engine_with_docs();
        let gr = execute_group_by(&mut e, "SELECT city, COUNT(*) FROM t GROUP BY city", 1000)
            .unwrap()
            .unwrap();
        assert_eq!(gr.group_cols, vec!["city"]);
        assert_eq!(gr.headers, vec!["COUNT(*)"]);
        let keys: Vec<Option<String>> = gr.rows.iter().map(|r| r.keys[0].clone()).collect();
        assert_eq!(
            keys,
            vec![Some("beijing".into()), Some("shanghai".into()), Some("shenzhen".into())],
            "组键升序（字典序）"
        );
        let counts: Vec<u64> = gr
            .rows
            .iter()
            .map(|r| r.cells[0].as_ref().unwrap().parse().unwrap())
            .collect();
        assert_eq!(counts, vec![34, 33, 33], "i%3==0/1/2 各 34/33/33");
    }

    #[test]
    fn group_by_status_multi_agg_sum() {
        let mut e = engine_with_docs();
        let gr = execute_group_by(
            &mut e,
            "SELECT status, COUNT(*), SUM(amount) FROM t GROUP BY status",
            1000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(gr.headers, vec!["COUNT(*)", "SUM(amount)"]);
        let keys: Vec<&str> = gr.rows.iter().map(|r| r.keys[0].as_deref().unwrap()).collect();
        assert_eq!(keys, vec!["active", "inactive"]);
        // active：i%3==0 共 34 行，amount 和 = 30×(0+1+…+33) = 16830
        let a = &gr.rows[0];
        assert_eq!(a.cells[0].as_deref(), Some("34"));
        assert_eq!(a.cells[1].as_deref(), Some("16830"));
        // inactive：66 行，总和 = 49500 - 16830 = 32670
        let b = &gr.rows[1];
        assert_eq!(b.cells[0].as_deref(), Some("66"));
        assert_eq!(b.cells[1].as_deref(), Some("32670"));
    }

    #[test]
    fn group_by_sum_with_where() {
        let mut e = engine_with_docs();
        let gr = execute_group_by(
            &mut e,
            "SELECT city, SUM(amount) FROM t WHERE amount>=500 AND amount<600 GROUP BY city",
            1000,
        )
        .unwrap()
        .unwrap();
        // 命中 docid 50..59（i%3: 0→beijing、1→shanghai、2→shenzhen）：
        // beijing(51/54/57)=1620、shanghai(52/55/58)=1650、shenzhen(50/53/56/59)=2180
        let sums: Vec<&str> = gr
            .rows
            .iter()
            .map(|r| r.cells[0].as_deref().unwrap())
            .collect();
        assert_eq!(sums, vec!["1620", "1650", "2180"]);
        // 行级过滤后无 shanghai 组缺席（各组均含匹配行）
        assert_eq!(gr.rows.len(), 3);
    }

    #[test]
    fn group_by_missing_field_null_group_and_empty_sum() {
        let mut e = engine_with_docs();
        // 缺省字段 → 全部并入 NULL 组；SUM(缺省数值字段) → 无数值行 → NULL
        let gr = execute_group_by(
            &mut e,
            "SELECT nokey, COUNT(*), SUM(amount2) FROM t GROUP BY nokey",
            1000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(gr.rows.len(), 1, "全 NULL 键并为一组");
        assert!(gr.rows[0].keys[0].is_none(), "NULL 组键文本为 None");
        assert_eq!(gr.rows[0].cells[0].as_deref(), Some("100"), "COUNT(*) 计 100 行");
        assert!(gr.rows[0].cells[1].is_none(), "空数值集 SUM → SQL NULL");
    }

    #[test]
    fn group_by_limit_slices_groups() {
        let mut e = engine_with_docs();
        let gr = execute_group_by(&mut e, "SELECT city, COUNT(*) FROM t GROUP BY city LIMIT 2", 1000)
            .unwrap()
            .unwrap();
        assert_eq!(gr.rows.len(), 2, "LIMIT 对组行切片");
        assert_eq!(gr.rows[0].keys[0].as_deref(), Some("beijing"));
        assert_eq!(gr.rows[1].keys[0].as_deref(), Some("shanghai"));
    }

    #[test]
    fn group_by_order_by_group_field_desc() {
        let mut e = engine_with_docs();
        let gr = execute_group_by(
            &mut e,
            "SELECT city, COUNT(*) FROM t GROUP BY city ORDER BY city DESC",
            1000,
        )
        .unwrap()
        .unwrap();
        let keys: Vec<&str> = gr.rows.iter().map(|r| r.keys[0].as_deref().unwrap()).collect();
        assert_eq!(keys, vec!["shenzhen", "shanghai", "beijing"], "组键 DESC");
        // 排序字段 ≠ 分组字段 → 拒绝（组结果仅支持按分组字段排序，AF#5 扩展）
        let mut e2 = engine_with_docs();
        assert!(execute_group_by(
            &mut e2,
            "SELECT city, COUNT(*) FROM t GROUP BY city ORDER BY amount",
            1000,
        )
        .is_err());
    }

    #[test]
    fn group_by_multi_field_all_aggregates() {
        // AF#4：双字段分组 + COUNT/SUM/AVG/MIN/MAX 常用聚合（组内按 region,tier 组合切分）。
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        // (region, tier, amount)：north/south × a/b
        let docs = [
            ("north", "a", 10.0),
            ("north", "a", 20.0),
            ("north", "b", 5.0),
            ("north", "b", 15.0),
            ("south", "a", 7.0),
            ("south", "a", 9.0),
            ("south", "b", 3.0),
            ("south", "b", 1.0),
        ];
        for (i, (r, t, amt)) in docs.iter().enumerate() {
            let doc = format!(r#"{{"region":"{r}","tier":"{t}","amount":{amt}}}"#);
            let refs: Vec<&str> = Vec::new();
            e.put(i as u64 + 1, doc.into_bytes(), &refs).unwrap();
        }
        let gr = execute_group_by(
            &mut e,
            "SELECT region, tier, COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM t GROUP BY region, tier",
            1000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(gr.group_fields, vec!["region", "tier"]);
        assert_eq!(
            gr.group_cols,
            vec!["region", "tier"],
            "结果集分组列 = 选中普通列"
        );
        assert_eq!(
            gr.headers,
            vec!["COUNT(*)", "SUM(amount)", "AVG(amount)", "MIN(amount)", "MAX(amount)"]
        );
        // 组键升序：north-a/b → south-a/b
        let combos: Vec<String> = gr
            .rows
            .iter()
            .map(|r| format!("{}-{}", r.keys[0].as_deref().unwrap(), r.keys[1].as_deref().unwrap()))
            .collect();
        assert_eq!(combos, vec!["north-a", "north-b", "south-a", "south-b"]);
        let cell = |row: usize, col: usize| gr.rows[row].cells[col].as_deref().unwrap().to_string();
        assert_eq!(cell(0, 0), "2");
        assert_eq!(cell(0, 1), "30");
        assert_eq!(cell(0, 2), "15"); // AVG(10,20)
        assert_eq!(cell(0, 3), "10"); // MIN
        assert_eq!(cell(0, 4), "20"); // MAX
        assert_eq!(cell(3, 0), "2");
        assert_eq!(cell(3, 1), "4"); // south-b SUM(3,1)
        assert_eq!(cell(3, 2), "2"); // AVG(3,1)
        assert_eq!(cell(3, 3), "1"); // MIN
        assert_eq!(cell(3, 4), "3"); // MAX
    }

    #[test]
    fn group_by_multi_field_order_on_secondary_level_and_subset_cols() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        for (i, r) in [("north", "a"), ("north", "b"), ("south", "a"), ("south", "b")]
            .iter()
            .enumerate()
        {
            let doc = format!(r#"{{"region":"{}","tier":"{}"}}"#, r.0, r.1);
            let refs: Vec<&str> = Vec::new();
            e.put(i as u64 + 1, doc.into_bytes(), &refs).unwrap();
        }
        // ORDER BY tier DESC：优先级 = 次层键降序 → 同 tier 内 region 升序补尾
        let gr = execute_group_by(
            &mut e,
            "SELECT region, tier, COUNT(*) FROM t GROUP BY region, tier ORDER BY tier DESC",
            1000,
        )
        .unwrap()
        .unwrap();
        let combos: Vec<String> = gr
            .rows
            .iter()
            .map(|r| format!("{}-{}", r.keys[0].as_deref().unwrap(), r.keys[1].as_deref().unwrap()))
            .collect();
        assert_eq!(combos, vec!["north-b", "south-b", "north-a", "south-a"]);
        // 只选中一个分组列：region 仍参与分组（区隔行），但不在结果集出现
        let gr2 = execute_group_by(
            &mut e,
            "SELECT tier, COUNT(*) FROM t GROUP BY region, tier",
            1000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(gr2.group_cols, vec!["tier"], "仅选中列出现在结果集");
        let tiers: Vec<&str> = gr2.rows.iter().map(|r| r.keys[1].as_deref().unwrap()).collect();
        assert_eq!(tiers, vec!["a", "b", "a", "b"], "region×tier 各成组（默认复合键升序）");
        assert_eq!(gr2.rows.len(), 4, "region×tier 仍 4 组（region 未选中仍参与分组）");
    }

    #[test]
    fn group_by_parse_rejections() {
        // 无 GROUP BY 多聚合仍拒绝（标量路径旧语义）
        assert!(parse_select("SELECT COUNT(*), SUM(amount) FROM t").is_err());
        // 非分组列 ≠ 任一 GROUP BY 字段 → 拒绝
        assert!(parse_select("SELECT status, COUNT(*) FROM t GROUP BY city").is_err());
        // 多字段 GROUP BY（AF#4 合法）→ 解析通过
        assert!(parse_select("SELECT city, status, COUNT(*) FROM t GROUP BY city, status").is_ok());
        // GROUP BY 字段重复 → 拒绝
        assert!(parse_select("SELECT COUNT(*) FROM t GROUP BY city, city").is_err());
        // SELECT * 与 GROUP BY 混用 → 拒绝
        assert!(parse_select("SELECT * FROM t GROUP BY city").is_err());
        // AVG/MIN/MAX 聚合（AF#4 合法）→ 解析通过
        assert!(parse_select("SELECT city, AVG(amount), MIN(amount), MAX(amount) FROM t GROUP BY city")
            .is_ok());
        // 无聚合列的 GROUP BY → 执行拒绝（须至少一个聚合列）
        let mut e = engine_with_docs();
        assert!(execute_group_by(&mut e, "SELECT city FROM t GROUP BY city", 1000).is_err());
    }

    // ---------- HAVING（开发顺序 AF#5：分组后过滤） ----------

    #[test]
    fn group_by_having_on_aggregate() {
        // fixture：city 34/33/33；SUM(amount) beijing 16830 / shanghai 16170 / shenzhen 16500。
        let mut e = engine_with_docs();
        let gr = execute_group_by(
            &mut e,
            "SELECT city, COUNT(*), SUM(amount) FROM t GROUP BY city HAVING COUNT(*) > 33",
            1000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(gr.rows.len(), 1, "仅 beijing（34 > 33）");
        assert_eq!(gr.rows[0].keys[0].as_deref(), Some("beijing"));
        assert_eq!(gr.rows[0].cells[0].as_deref(), Some("34"));
        // 聚合 + 分组字段复合条件（AND）
        let gr2 = execute_group_by(
            &mut e,
            "SELECT city, SUM(amount) FROM t GROUP BY city HAVING SUM(amount) > 16000 AND city != 'beijing'",
            1000,
        )
        .unwrap()
        .unwrap();
        let keys: Vec<&str> = gr2.rows.iter().map(|r| r.keys[0].as_deref().unwrap()).collect();
        assert_eq!(keys, vec!["shanghai", "shenzhen"], "16170/16500 > 16000 且排除 beijing");
    }

    #[test]
    fn group_by_having_or_and_sort_slice_after_filter() {
        let mut e = engine_with_docs();
        // OR：beijing（34）∪ shanghai（COUNT>33 OR city 等值）
        let gr = execute_group_by(
            &mut e,
            "SELECT city, COUNT(*) FROM t GROUP BY city HAVING COUNT(*) > 33 OR city = 'shanghai'",
            1000,
        )
        .unwrap()
        .unwrap();
        let keys: Vec<&str> = gr.rows.iter().map(|r| r.keys[0].as_deref().unwrap()).collect();
        assert_eq!(keys, vec!["beijing", "shanghai"]);
        // HAVING 过滤后再 LIMIT 切片（先过滤：仅 beijing/shanghai，LIMIT 1 → beijing）
        let gr2 = execute_group_by(
            &mut e,
            "SELECT city, COUNT(*) FROM t GROUP BY city HAVING COUNT(*) > 33 LIMIT 1",
            1000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(gr2.rows.len(), 1);
        assert_eq!(gr2.rows[0].keys[0].as_deref(), Some("beijing"));
        // 空结果：过滤掉全部组 → 空集
        let gr3 = execute_group_by(
            &mut e,
            "SELECT city, COUNT(*) FROM t GROUP BY city HAVING SUM(amount) > 999999",
            1000,
        )
        .unwrap()
        .unwrap();
        assert!(gr3.rows.is_empty(), "无组满足 HAVING → 空结果集");
    }

    #[test]
    fn having_parse_constraints() {
        // HAVING 配合 GROUP BY 合法
        let s = parse_select("SELECT city, COUNT(*) FROM t GROUP BY city HAVING COUNT(*) > 1 AND city='x'")
            .unwrap();
        assert!(s.having.is_some(), "HAVING 解析成功");
        // 无 GROUP BY 的 HAVING → 拒绝（本期不支持）
        assert!(parse_select("SELECT COUNT(*) FROM t HAVING COUNT(*) > 1").is_err());
        // 非聚合/非分组列左项在解析期不校验（运行期不命中 → 保守丢弃），已知取舍
    }

    /// P0-D：INNER JOIN 解析 + 执行。
    #[test]
    fn join_parse_and_execute() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        // 主表 orders：3 条，关联 user_id
        e.put(1, br#"{"user_id":"101","amount":100}"#.to_vec(), &["user_id=101"])
            .unwrap();
        e.put(2, br#"{"user_id":"102","amount":200}"#.to_vec(), &["user_id=102"])
            .unwrap();
        e.put(3, br#"{"user_id":"101","amount":300}"#.to_vec(), &["user_id=101"])
            .unwrap();
        // 从表 users：2 条
        e.put(101, br#"{"name":"alice"}"#.to_vec(), &["name=alice"])
            .unwrap();
        e.put(102, br#"{"name":"bob"}"#.to_vec(), &["name=bob"])
            .unwrap();
        e.flush_wal().unwrap();

        // INNER JOIN：主表 user_id = 从表 docid（WHERE 收敛主表）
        let rows = execute(
            &e,
            "SELECT * FROM orders INNER JOIN users ON orders.user_id = users.docid WHERE amount>=100 LIMIT 100",
            1000,
        )
        .unwrap();
        assert_eq!(rows.len(), 3, "3 条 order 都有匹配 user");

        // LEFT JOIN：同结果（都有匹配）
        let rows2 = execute(
            &e,
            "SELECT * FROM orders LEFT JOIN users ON orders.user_id = users.docid WHERE amount>=100 LIMIT 100",
            1000,
        )
        .unwrap();
        assert_eq!(rows2.len(), 3, "LEFT JOIN 3 条");

        // 无匹配的 LEFT JOIN → 仍保留左行
        e.put(4, br#"{"user_id":"999","amount":400}"#.to_vec(), &["user_id=999"])
            .unwrap();
        e.flush_wal().unwrap();
        let rows3 = execute(
            &e,
            "SELECT * FROM orders LEFT JOIN users ON orders.user_id = users.docid WHERE amount>=100 LIMIT 100",
            1000,
        )
        .unwrap();
        assert_eq!(rows3.len(), 4, "LEFT JOIN 4 条（含无匹配）");

        // INNER JOIN → 无匹配行不出现
        let rows4 = execute(
            &e,
            "SELECT * FROM orders INNER JOIN users ON orders.user_id = users.docid WHERE amount>=100 LIMIT 100",
            1000,
        )
        .unwrap();
        assert_eq!(rows4.len(), 3, "INNER JOIN 仍 3 条（无匹配不出现）");
    }
}

