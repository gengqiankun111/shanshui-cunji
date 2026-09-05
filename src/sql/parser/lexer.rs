//! 词法分析（原 sqlish.rs 词法段）：`Tok` 记号 + `Lexer` 单字符流扫描。
//! 仅 parser/parser.rs 使用（`pub(super)` 可见域 = parser 子树）。

use super::PRes;

// ---------------------------------------------------------------------------
// 词法
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub(super) enum Tok {
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
    // 2026-09-05（列表达式/函数值）：算术运算符 token（仅表达式列解析消费；旧路径不触达）
    Plus,
    Minus,
    Slash,
    Percent,
    Kw(String),
    Eof,
}

pub(super) struct Lexer {
    chars: Vec<char>,
    pos: usize,
}

impl Lexer {
    pub(super) fn new(sql: &str) -> Self {
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
    pub(super) fn next_tok(&mut self) -> PRes<Tok> {
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
            '+' => {
                self.pos += 1;
                return Ok(Tok::Plus);
            }
            '-' => {
                self.pos += 1;
                return Ok(Tok::Minus);
            }
            '/' => {
                self.pos += 1;
                return Ok(Tok::Slash);
            }
            '%' => {
                self.pos += 1;
                return Ok(Tok::Percent);
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
