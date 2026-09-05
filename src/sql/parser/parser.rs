//! 递归下降解析（原 sqlish.rs 语法分析段）：`Parser` + 入口 `parse_select` /
//! `parse_where_expr`（写路径 WHERE 段复用同一文法）。

use crate::error::{Error, Result};
use super::ast::{BinOp, CmpOp, Cond, Expr, HavingCond, HavingExpr, JoinClause, JoinKind, Select, WhereExpr};
use super::lexer::{Lexer, Tok};
use super::PRes;

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
        // 2026-09-05（行去重立项）：识别 `SELECT DISTINCT <列清单>`（DISTINCT 为保留词，不进列名）。
        let mut distinct = false;
        if let Tok::Ident(k) = self.peek()? {
            if k.eq_ignore_ascii_case("distinct") {
                self.next()?;
                distinct = true;
            }
        }
        let mut columns = Vec::new();
        let mut col_exprs: Vec<Option<Expr>> = Vec::new();
        let mut plain: Vec<String> = Vec::new();
        let mut aggs: Vec<(String, Option<String>)> = Vec::new();
        let mut distincts: Vec<bool> = Vec::new();
        let mut star_seen = false;
        loop {
            let item = self.next()?;
            match item {
                Tok::Star => {
                    columns.push("*".into());
                    col_exprs.push(None);
                    star_seen = true;
                }
                // 2026-09-05（列表达式/函数值 阶段 A）：以操作数/字面量开头的列项 → 表达式列
                Tok::Num(_) | Tok::Str(_) | Tok::Plus | Tok::Minus | Tok::LParen => {
                    let e = self.parse_scalar_from(item)?;
                    columns.push(e.name());
                    col_exprs.push(Some(e));
                }
                Tok::Ident(i) => {
                    // 7.95 聚合函数列：COUNT(*) / COUNT(f) / SUM(f) / AVG(f) / MIN(f) / MAX(f)
                    // Task-030：COUNT(DISTINCT f)（去重计数；DISTINCT 仅 COUNT 支持）
                    if matches!(self.peek()?, Tok::LParen) {
                        let upper = i.to_uppercase();
                        if matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                            self.next()?; // LParen
                            let mut distinct = false;
                            if let Tok::Ident(k) = self.peek()? {
                                if k.eq_ignore_ascii_case("distinct") {
                                    self.next()?; // distinct
                                    distinct = true;
                                }
                            }
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
                            if distinct && upper != "COUNT" {
                                return Err(format!(
                                    "仅 COUNT(DISTINCT f) 受支持（{upper}(DISTINCT) 未支持）"
                                ));
                            }
                            if distinct && arg.is_none() {
                                return Err("DISTINCT 需字段参数（COUNT(DISTINCT f)）".into());
                            }
                            columns.push(if distinct {
                                format!("COUNT(DISTINCT {})", arg.as_deref().unwrap())
                            } else {
                                upper.clone()
                            });
                            col_exprs.push(None);
                            aggs.push((upper.to_lowercase(), arg));
                            distincts.push(distinct);
                        } else if matches!(upper.as_str(), "CONCAT" | "LOWER" | "UPPER" | "LENGTH" | "ROUND" | "ABS") {
                            // 受支持函数列 → 表达式列（函数调用项，可续算术链）
                            let e = self.parse_scalar_from(Tok::Ident(i))?;
                            columns.push(e.name());
                            col_exprs.push(Some(e));
                        } else {
                            return Err(format!("不支持的函数列: {i}"));
                        }
                    } else if matches!(
                        self.peek()?,
                        Tok::Plus | Tok::Minus | Tok::Slash | Tok::Percent | Tok::Star
                    ) || i.parse::<f64>().is_ok()
                    {
                        // 普通字段后跟算术运算符（amount*2）或数值字面量（5.0）→ 表达式列
                        let e = self.parse_scalar_from(Tok::Ident(i))?;
                        columns.push(e.name());
                        col_exprs.push(Some(e));
                    } else {
                        plain.push(i.clone());
                        columns.push(i);
                        col_exprs.push(None);
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
        let has_expr_col = col_exprs.iter().any(|c| c.is_some());
        self.expect_kw("FROM")?;
        let table = self.ident()?;
        // P0-D：JOIN 解析（`[INNER|LEFT] JOIN t2 ON t1.f1 = t2.f2`）
        // review 修复（2026-09-04）：多 JOIN 解析期拒绝——当前仅支持单表 JOIN；
        // 若第二个 JOIN 出现（静默覆盖只留最后一个）→ Err，防错结果。
        let mut join = None;
        loop {
            let kind = {
                match self.peek()? {
                    Tok::Ident(k) if k.eq_ignore_ascii_case("left") => {
                        let kind = JoinKind::Left;
                        self.next()?; // 消费 left
                        match self.peek()? {
                            Tok::Kw(kw) if kw == "JOIN" => kind,
                            _ => return Err("LEFT 后须跟 JOIN".into()),
                        }
                    }
                    Tok::Ident(k) if k.eq_ignore_ascii_case("inner") => {
                        let kind = JoinKind::Inner;
                        self.next()?; // 消费 inner
                        match self.peek()? {
                            Tok::Kw(kw) if kw == "JOIN" => kind,
                            _ => return Err("INNER 后须跟 JOIN".into()),
                        }
                    }
                    Tok::Kw(kw) if kw == "JOIN" => JoinKind::Inner,
                    _ => break,
                }
            };
            if join.is_some() {
                return Err("暂不支持多表 JOIN（>1 个 JOIN 子句）".into());
            }
            self.next()?; // 消费 JOIN
            let right_table = self.ident()?;
            self.expect_kw("ON")?;
            let left_field = self.ident()?;
            self.expect_punct("=")?;
            let right_field = self.ident()?;
            join = Some(JoinClause {
                join_type: kind,
                right_table,
                left_field,
                right_field,
            });
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
                    // Task-030：排序项可为聚合列头（GROUP BY 后按聚合值排序，
                    // 如 `ORDER BY COUNT(*) DESC`/`SUM(amount)`）——解析为规范头串
                    // （`COUNT(*)`/`SUM(amount)`），分组执行器按聚合值排序。
                    self.next()?; // 消费 order
                    match self.next()? {
                        Tok::Ident(k) if k.eq_ignore_ascii_case("by") => {}
                        t => return Err(format!("ORDER 后期望 BY，实际 {t:?}")),
                    }
                    loop {
                        let id = self.ident()?;
                        let f = if matches!(self.peek()?, Tok::LParen) {
                            let up = id.to_uppercase();
                            if !matches!(up.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                                return Err(format!("ORDER BY 不支持函数: {id}"));
                            }
                            self.next()?; // (
                            let a = match self.next()? {
                                Tok::Star => "*".to_string(),
                                Tok::Ident(x) => x,
                                t => {
                                    return Err(format!(
                                        "ORDER BY 聚合参数期望 * 或字段名，实际 {t:?}"
                                    ))
                                }
                            };
                            match self.next()? {
                                Tok::RParen => {}
                                t => return Err(format!("ORDER BY 聚合期望右括号，实际 {t:?}")),
                            }
                            format!("{up}({a})")
                        } else {
                            id
                        };
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
        let mut agg_distinct = false;
        let group_aggs = aggs.clone();
        // 2026-09-05（行去重立项）：SELECT DISTINCT 形态守卫——首版限显式列清单，
        // 不与聚合/GROUP BY/HAVING/JOIN 组合；ORDER BY 列须 ∈ 列清单（防去重-排序语义错位）。
        if distinct {
            if star_seen {
                return Err("SELECT DISTINCT * 暂不支持（须显式列清单）".into());
            }
            if !group_by.is_empty() || !aggs.is_empty() || having.is_some() || join.is_some() {
                return Err(
                    "SELECT DISTINCT 与聚合/GROUP BY/HAVING/JOIN 组合暂不支持".into(),
                );
            }
            for (f, _) in &order_by {
                if !plain.iter().any(|c| c.eq_ignore_ascii_case(f)) {
                    return Err(format!(
                        "SELECT DISTINCT 下 ORDER BY 列 {f} 须在 SELECT 列清单内"
                    ));
                }
            }
        }
        // 2026-09-05（列表达式/函数值 阶段 A）形态守卫：首版限一般 SELECT（无 * / DISTINCT /
        // 聚合 / GROUP BY / HAVING / JOIN 组合）；ORDER BY 表达式（排序列=表达式规范名）阶段 B。
        if has_expr_col {
            if star_seen {
                return Err("SELECT 表达式列与 * 混用暂不支持".into());
            }
            if distinct
                || !group_by.is_empty()
                || !aggs.is_empty()
                || having.is_some()
                || join.is_some()
            {
                return Err(
                    "SELECT 表达式列与 DISTINCT/聚合/GROUP BY/HAVING/JOIN 组合暂不支持（阶段 B）"
                        .into(),
                );
            }
            let expr_names: Vec<String> = col_exprs.iter().filter_map(|c| c.as_ref()).map(|e| e.name()).collect();
            for (f, _) in &order_by {
                if expr_names.iter().any(|n| n.eq_ignore_ascii_case(f)) {
                    return Err(format!("ORDER BY 表达式列 {f} 暂不支持（阶段 B）"));
                }
            }
        }
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
            for (i, (n, f)) in aggs.iter().enumerate() {
                let up = n.to_uppercase();
                if !matches!(up.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                    return Err(format!("GROUP BY 不支持的聚合: {n}"));
                }
                if f.is_none() && up != "COUNT" {
                    return Err(format!("{n}(*) 不支持（仅 COUNT(*)）"));
                }
                if distincts[i] {
                    // Task-030：分组内 DISTINCT 聚合暂不支持（防静默忽略）
                    return Err("GROUP BY 内 DISTINCT 聚合暂不支持（仅标量 COUNT(DISTINCT f)）".into());
                }
            }
        } else if aggs.len() > 1 {
            return Err("暂不支持多列/多聚合（无 GROUP BY）".into());
        } else {
            agg = aggs.into_iter().next();
            agg_distinct = distincts.into_iter().next().unwrap_or(false);
        }
        if having.is_some() && group_by.is_empty() {
            return Err("HAVING 需配合 GROUP BY（本期不支持无分组的 HAVING）".into());
        }
        Ok(Select {
            columns,
            col_exprs,
            distinct,
            table,
            where_expr,
            limit,
            offset,
            agg,
            agg_distinct,
            order_by,
            group_by,
            group_aggs,
            having,
            join,
        })
    }
    // ---- 2026-09-05（列表达式/函数值 阶段 A）：标量表达式（列清单项）解析 ----
    // 优先级：加减 < 乘除模 < 一元负号/括号/函数/字面量/字段。停在顶层分隔符
    // （逗号 / FROM / 子句关键字 / EOF）前，调用方继续既有列清单收尾。
    // 说明：列项首个 token 已被调用方消费；一律经 `parse_scalar_from` 起步（不得
    // push_back——会覆盖 peeked 中已缓存的后缀运算符，见 P126 修）。
    fn parse_scalar_from(&mut self, first: Tok) -> PRes<Expr> {
        let p0 = self.scalar_prim_of(first)?;
        let m = self.parse_scalar_mul_cont(p0)?;
        self.parse_scalar_add_cont(m)
    }
    fn parse_scalar_expr(&mut self) -> PRes<Expr> {
        let p0 = self.parse_scalar_prim()?;
        let m = self.parse_scalar_mul_cont(p0)?;
        self.parse_scalar_add_cont(m)
    }
    fn parse_scalar_add_cont(&mut self, mut l: Expr) -> PRes<Expr> {
        loop {
            match self.peek()? {
                Tok::Plus => {
                    self.next()?;
                    let r = self.parse_scalar_mul()?;
                    l = Expr::Bin { op: BinOp::Add, l: Box::new(l), r: Box::new(r) };
                }
                Tok::Minus => {
                    self.next()?;
                    let r = self.parse_scalar_mul()?;
                    l = Expr::Bin { op: BinOp::Sub, l: Box::new(l), r: Box::new(r) };
                }
                _ => return Ok(l),
            }
        }
    }
    fn parse_scalar_mul(&mut self) -> PRes<Expr> {
        let p0 = self.parse_scalar_prim()?;
        self.parse_scalar_mul_cont(p0)
    }
    fn parse_scalar_mul_cont(&mut self, mut l: Expr) -> PRes<Expr> {
        loop {
            let (op, next) = match self.peek()? {
                Tok::Star => (BinOp::Mul, true),
                Tok::Slash => (BinOp::Div, true),
                Tok::Percent => (BinOp::Mod, true),
                _ => (BinOp::Add, false),
            };
            if !next {
                return Ok(l);
            }
            self.next()?;
            let r = self.parse_scalar_prim()?;
            l = Expr::Bin { op, l: Box::new(l), r: Box::new(r) };
        }
    }
    fn parse_scalar_prim(&mut self) -> PRes<Expr> {
        let t = self.next()?;
        self.scalar_prim_of(t)
    }
    fn scalar_prim_of(&mut self, tok: Tok) -> PRes<Expr> {
        match tok {
            Tok::Num(n) => Ok(if n > i64::MAX as u64 {
                Expr::NumF(n as f64)
            } else {
                Expr::NumI(n as i64)
            }),
            Tok::Str(s) => Ok(Expr::Str(s)),
            Tok::Plus => self.parse_scalar_prim(), // 一元正号（无操作）
            Tok::Minus => {
                let inner = self.parse_scalar_prim()?;
                Ok(Expr::Bin { op: BinOp::Sub, l: Box::new(Expr::NumI(0)), r: Box::new(inner) })
            }
            Tok::LParen => {
                let e = self.parse_scalar_expr()?;
                if !matches!(self.next()?, Tok::RParen) {
                    return Err("表达式期望右括号 )".into());
                }
                Ok(e)
            }
            Tok::Ident(i) => {
                if matches!(self.peek()?, Tok::LParen) {
                    self.next()?; // LParen
                    let mut args: Vec<Expr> = Vec::new();
                    if !matches!(self.peek()?, Tok::RParen) {
                        loop {
                            args.push(self.parse_scalar_expr()?);
                            match self.next()? {
                                Tok::Comma => continue,
                                Tok::RParen => break,
                                t => return Err(format!("函数参数期望 , 或 )，实际 {t:?}")),
                            }
                        }
                    } else {
                        self.next()?; // 空参 ()
                    }
                    Ok(Expr::Call { name: i.to_uppercase(), args })
                } else if let Ok(x) = i.parse::<f64>() {
                    Ok(Expr::NumF(x)) // 浮点字面量（词法按 Ident 输出，如 5.0）
                } else {
                    Ok(Expr::Col(i))
                }
            }
            t => Err(format!("表达式期望操作数，实际 {t:?}")),
        }
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
        // LIKE：`field LIKE 'pattern'`（SQL 通配 `%`）。无 `%` → 折叠为等值（走倒排/索引）；
        // 含 `%` → WhereExpr::Like（倒排不可表达 → 扫描后过滤/全扫，见 eval/scan_leaf）。
        if matches!(self.peek()?, Tok::Ident(k) if k.eq_ignore_ascii_case("like")) {
            self.next()?; // like
            let pat = self.value()?;
            if !pat.contains('%') {
                return Ok(WhereExpr::Cond(Cond {
                    field,
                    op: CmpOp::Eq,
                    value: pat,
                }));
            }
            return Ok(WhereExpr::Like { field, pattern: pat });
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


/// P88：WHERE 段（不带 WHERE 关键字）→ WhereExpr AST——写路径（UPDATE/DELETE 定位）
/// 与读路径共用同一解析器（AND/OR/NOT/比较/BETWEEN/LIKE/IN 等文法一致）。
/// 实现：复用 SELECT 解析（包装 `SELECT * FROM t WHERE <fragment>`），取 where_expr；
/// 无 WHERE / 解析失败 → Error::Config（db_adapter 包装 1064）。
pub fn parse_where_expr(fragment: &str) -> Result<WhereExpr> {
    let f = fragment.trim().trim_end_matches(';').trim();
    if f.is_empty() {
        return Err(Error::Config("WHERE 条件为空".into()));
    }
    let sql = format!("SELECT * FROM t WHERE {f}");
    let sel = parse_select(&sql)?;
    sel.where_expr.ok_or_else(|| Error::Config("WHERE 条件解析为空".into()))
}
