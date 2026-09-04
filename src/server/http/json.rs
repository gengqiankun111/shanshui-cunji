//! 请求/文档解析辅助（HTTP 网关与 CLI 共用）：query string 与百分号解码（url_decode）、
//! filter 解析、JSON 字符串字段倒排词条提取簇（extract_terms*）。
//!
//! term 编码：`{字段路径}={值}`（如 `status=active`、`meta.device=ios`），路径用 `.` 连接
//! ——带字段维度，供 COUNT / GROUP BY 按字段聚合（development 5.17）。fulltext 分词索引
//! （M8-P7/P9/P13）：`ft:{field}:{token}` 前缀独立命名空间（分词在 tokenize.rs）。

use serde_json::Value;

use super::tokenize::fulltext_terms_seg;

/// 百分号解码（UTF-8）。
pub fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                    if let Ok(b) = u8::from_str_radix(hex, 16) {
                        out.push(b);
                        i += 3;
                        continue;
                    }
                }
                out.push(bytes[i]);
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// 解析 query string（`k=v&k2=v2`，值做百分号解码，`+` → 空格）。
pub(crate) fn parse_query(query: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (url_decode(k), url_decode(v)),
            None => (url_decode(pair), String::new()),
        };
        out.push((k, v));
    }
    out
}

// ---------------------------------------------------------------------------
// filter 解析（CLI 与 HTTP 共用）
// ---------------------------------------------------------------------------

/// 解析 filter：`field=value`，多条件 ` AND ` 连接（大小写不敏感分隔）。
/// 返回 (字段, 值) 列表。支持 `docid=...` 主键条件。
pub fn parse_filter(filter: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for cond in filter.split(" AND ") {
        let cond = cond.trim();
        if cond.is_empty() {
            continue;
        }
        if let Some((k, v)) = cond.split_once('=') {
            let k = k.trim();
            let v = v.trim();
            if !k.is_empty() {
                out.push((k.to_string(), v.to_string()));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// 词条提取（CLI 与 HTTP 共用；fulltext 分词见 tokenize.rs）
// ---------------------------------------------------------------------------

/// 提取 JSON 中全部字符串字段值作为倒排词条（递归，含顶层与嵌套对象/数组）。
/// term 编码：`{字段路径}={值}`（如 `status=active`、`meta.device=ios`），
/// 路径用 `.` 连接——带字段维度，供 COUNT / GROUP BY 按字段聚合（development 5.17）。
pub fn extract_terms(val: &Value) -> Vec<String> {
    extract_terms_filtered(val, None)
}

/// 提取倒排词条并按字段白名单过滤（M8-P4）：`include` 非空时只生成声明字段的 term
/// （不匹配字段的 term **不分配**——长文本/高基数字段整串 term 的分配浪费在生成前消除）。
pub fn extract_terms_filtered(
    val: &Value,
    include: Option<&std::collections::HashSet<String>>,
) -> Vec<String> {
    extract_terms_with_fulltext(val, include, None)
}

/// fulltext 分词索引（M8-P7）：`extract_terms_filtered` 的超集——
/// `fulltext` 集合中声明的字段做**分词建词 term**（`ft:{field}:{token}`）**取代整串 term**：
/// 长文本整串（>max_term_len）会被跳过无法检索；分词后 token 短可建索引、支持关键词检索。
/// 其余字段维持整串 term（受 include 白名单过滤）；两者命名空间不冲突（`ft:` 前缀独立）。
/// 中文分词用 bigram（M8-P9）；需 jieba 词典分词请用 `extract_terms_with_fulltext_seg`。
pub fn extract_terms_with_fulltext(
    val: &Value,
    include: Option<&std::collections::HashSet<String>>,
    fulltext: Option<&std::collections::HashSet<String>>,
) -> Vec<String> {
    extract_terms_with_fulltext_seg(val, include, fulltext, false)
}

/// 同 `extract_terms_with_fulltext`，但可指定中文分词器（M8-P13）：
/// `use_jieba=true` 时 fulltext 字段中文用 jieba 完整词典分词（语义词，非 bigram 碎片）。
pub fn extract_terms_with_fulltext_seg(
    val: &Value,
    include: Option<&std::collections::HashSet<String>>,
    fulltext: Option<&std::collections::HashSet<String>>,
    use_jieba: bool,
) -> Vec<String> {
    let mut terms = Vec::new();
    collect_strings(val, &mut terms, &[], include, fulltext, use_jieba);
    terms
}

/// 单字段 term 生成：fulltext 字段 → 分词词 term（不建整串）；否则整串 term（受白名单过滤）。
fn push_field_term(
    out: &mut Vec<String>,
    field: &str,
    s: &str,
    include: Option<&std::collections::HashSet<String>>,
    fulltext: Option<&std::collections::HashSet<String>>,
    use_jieba: bool,
) {
    if let Some(ft) = fulltext {
        if ft.contains(field) {
            out.extend(fulltext_terms_seg(field, s, use_jieba));
            return;
        }
    }
    if include.map_or(true, |inc| inc.contains(field)) {
        out.push(format!("{field}={s}"));
    }
}

fn collect_strings(
    val: &Value,
    out: &mut Vec<String>,
    path: &[&str],
    include: Option<&std::collections::HashSet<String>>,
    fulltext: Option<&std::collections::HashSet<String>>,
    use_jieba: bool,
) {
    match val {
        Value::String(s) => {
            // 数组元素等叶子字符串：用完整路径生成 term（白名单非空时仅保留声明字段）
            push_field_term(out, &path.join("."), s, include, fulltext, use_jieba);
        }
        Value::Object(map) => {
            for (k, v) in map {
                if path.is_empty() && k == "docid" {
                    continue; // 主键不作为词条
                }
                let mut p = path.to_vec();
                p.push(k.as_str());
                if let Value::String(s) = v {
                    push_field_term(out, &p.join("."), s, include, fulltext, use_jieba);
                } else {
                    collect_strings(v, out, &p, include, fulltext, use_jieba);
                }
            }
        }
        Value::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                let mut p = path.to_vec();
                let idx = i.to_string();
                p.push(&idx);
                collect_strings(v, out, &p, include, fulltext, use_jieba);
            }
        }
        _ => {}
    }
}
