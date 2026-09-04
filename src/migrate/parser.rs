//! mysqldump SQL 导出解析：把 `INSERT INTO ... VALUES (...)` 行解析成结构化值。

/// mysqldump SQL 值。
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Str(String),
    Num(String),
    Null,
    Other(String),
}

/// 解析 mysqldump `INSERT INTO` 行：
/// `INSERT INTO \`t\` (\`a\`,\`b\`) VALUES ('x',1),(NULL,2);`
/// 返回 (列名列表, 值元组列表)。列名缺失（无括号）时为 None 语义的 Vec 空。
pub fn parse_mysql_insert_line(line: &str) -> Option<(Vec<String>, Vec<Vec<SqlValue>>)> {
    let s = line.trim_end_matches([';', '\n', '\r']);
    // 定位 VALUES / VALUE
    let upper = s.to_uppercase();
    let vpos = upper.find("VALUES").or_else(|| upper.find("VALUE"))?;
    let head = &s[..vpos];
    let tail = &s[vpos + "VALUES".len()..];
    // 解析列名列表（可选）：\`a\`,\`b\`
    let cols: Vec<String> = if let Some(open) = head.rfind('(') {
        let inner = &head[open + 1..];
        if let Some(close) = inner.find(')') {
            parse_backtick_list(&inner[..close])
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    // 解析值元组列表
    let tuples = parse_value_tuples(tail);
    Some((cols, tuples))
}

/// 解析反引号逗号列表：`\`a\`,\`b\`` → ["a","b"]。
fn parse_backtick_list(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_tick = false;
    for c in s.chars() {
        match c {
            '`' => {
                if in_tick {
                    out.push(cur.clone());
                    cur.clear();
                    in_tick = false;
                } else {
                    in_tick = true;
                }
            }
            ',' if !in_tick => {}
            _ if in_tick => cur.push(c),
            _ => {}
        }
    }
    out
}

/// 解析 VALUES 后的元组列表：`('a',1),(NULL,'b')` → 两层值。
fn parse_value_tuples(s: &str) -> Vec<Vec<SqlValue>> {
    let mut tuples = Vec::new();
    let mut cur: Vec<SqlValue> = Vec::new();
    let mut val = String::new();
    let mut in_str = false;
    let mut str_val = false;
    let mut in_esc = false;
    let mut depth = 0usize;
    for c in s.chars() {
        match c {
            '(' if !in_str => {
                depth += 1;
                val.clear();
                str_val = false;
            }
            ')' if !in_str => {
                depth = depth.saturating_sub(1);
                if !val.trim().is_empty() || cur.is_empty() {
                    cur.push(mk_value(&val, str_val));
                }
                val.clear();
                str_val = false;
                if depth == 0 && !cur.is_empty() {
                    tuples.push(std::mem::take(&mut cur));
                }
            }
            ',' if !in_str && depth == 1 => {
                if !val.trim().is_empty() {
                    cur.push(mk_value(&val, str_val));
                }
                val.clear();
                str_val = false;
            }
            '\'' if !in_esc => {
                in_str = !in_str;
                if in_str {
                    str_val = true;
                }
            }
            '\\' if in_str => {
                in_esc = true;
                val.push('\\');
            }
            _ => {
                if in_esc {
                    in_esc = false;
                }
                val.push(c);
            }
        }
    }
    tuples
}

/// 将（已剥离引号 / 保留转义序列的）MySQL 字面量转为 SqlValue。
fn mk_value(raw: &str, is_str: bool) -> SqlValue {
    let v = raw.trim();
    if is_str {
        // 处理转义：\' → '，\\ → \
        let mut out = String::new();
        let mut esc = false;
        for ch in v.chars() {
            if esc {
                out.push(ch);
                esc = false;
            } else if ch == '\\' {
                esc = true;
            } else {
                out.push(ch);
            }
        }
        return SqlValue::Str(out);
    }
    if v.eq_ignore_ascii_case("null") {
        return SqlValue::Null;
    }
    if v.chars().all(|c| c.is_ascii_digit() || c == '-') && !v.is_empty() {
        return SqlValue::Num(v.to_string());
    }
    SqlValue::Other(v.to_string())
}
