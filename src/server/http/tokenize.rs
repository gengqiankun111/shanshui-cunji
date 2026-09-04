//! 分词工具簇（M8-P7/P9/P13）：ASCII 单词（小写归一）+ 中文 bigram（零依赖）；可选
//! jieba 完整词典分词（`cjk-jieba` feature，关闭时回退 bigram）。fulltext 词 term
//! 生成（`ft:{field}:{token}`，同字段值内去重）供 json.rs 词条提取复用。

/// 分词（bigram，M8-P7 + M8-P9）：ASCII 字母数字 → 单词（小写归一）；
/// 连续中文/非 ASCII → bigram（相邻 2 字，单字回退 unigram），零依赖。
/// jieba 词典分词请用 `tokenize_seg(text, true)`。
pub fn tokenize(text: &str) -> Vec<String> {
    tokenize_seg(text, false)
}

/// 分词并按中文分词器选择（M8-P13）：`use_jieba` → jieba 完整词典分词（需 cjk-jieba feature，
/// 关闭时回退 bigram）；否则 bigram。
pub fn tokenize_seg(text: &str, use_jieba: bool) -> Vec<String> {
    #[cfg(feature = "cjk-jieba")]
    {
        if use_jieba {
            return tokenize_jieba(text);
        }
    }
    tokenize_bigram(text)
}

#[cfg(feature = "cjk-jieba")]
static JIEBA: std::sync::OnceLock<jieba_rs::Jieba> = std::sync::OnceLock::new();

#[cfg(feature = "cjk-jieba")]
fn jieba() -> &'static jieba_rs::Jieba {
    JIEBA.get_or_init(jieba_rs::Jieba::new)
}

/// jieba 完整中文词典分词（M8-P13）：ASCII 字母数字 → 单词（小写归一，同 bigram 规则）；
/// 中文/非 ASCII 块 → jieba 词典切分（语义词，非 bigram 碎片）；过滤标点/空白 token。
#[cfg(feature = "cjk-jieba")]
fn tokenize_jieba(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut ascii_word = String::new();
    let mut cjk = String::new();
    let j = jieba();
    for c in text.chars() {
        if c.is_ascii_alphanumeric() {
            if !cjk.is_empty() {
                out.extend(jieba_cut(j, &cjk));
                cjk.clear();
            }
            ascii_word.push(c.to_ascii_lowercase());
        } else if c.is_alphanumeric() {
            // 非 ASCII 字母数字（CJK 等）：结束 ASCII 单词，进入中文块
            if !ascii_word.is_empty() {
                out.push(std::mem::take(&mut ascii_word));
            }
            cjk.push(c);
        } else {
            // 分隔符
            if !ascii_word.is_empty() {
                out.push(std::mem::take(&mut ascii_word));
            }
            if !cjk.is_empty() {
                out.extend(jieba_cut(j, &cjk));
                cjk.clear();
            }
        }
    }
    if !ascii_word.is_empty() {
        out.push(ascii_word);
    }
    if !cjk.is_empty() {
        out.extend(jieba_cut(j, &cjk));
    }
    out
}

#[cfg(feature = "cjk-jieba")]
fn jieba_cut(j: &jieba_rs::Jieba, text: &str) -> Vec<String> {
    j.cut(text, true)
        .into_iter()
        .map(|t| t.word.to_string())
        .filter(|w| w.chars().any(|c| c.is_alphanumeric()))
        .collect()
}

/// bigram 分词（M8-P7 + 中文 bigram M8-P9）：按字符类分段——
/// **ASCII 字母数字** → 单词 token（按非字母数字边界切分 + 小写归一，原有行为）；
/// **连续中文/非 ASCII 字母数字** → bigram（相邻 2 字一个 token，单字回退 unigram）——
/// 中文整串当单 token 无法检索（7.14 已知限制），bigram 与 Elasticsearch ngram /
/// Lucene CJKAnalyzer 同款（中文检索事实标准），零依赖、无词典、索引膨胀 ≈ 字数。
pub fn tokenize_bigram(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut ascii_word = String::new();
    let mut cjk = String::new();
    for c in text.chars() {
        if c.is_ascii_alphanumeric() {
            if !cjk.is_empty() {
                out.extend(cjk_bigram(&cjk));
                cjk.clear();
            }
            ascii_word.push(c.to_ascii_lowercase());
        } else if c.is_alphanumeric() {
            // 非 ASCII 字母数字（CJK 等）：结束 ASCII 单词，进入中文块
            if !ascii_word.is_empty() {
                out.push(std::mem::take(&mut ascii_word));
            }
            cjk.push(c);
        } else {
            // 分隔符
            if !ascii_word.is_empty() {
                out.push(std::mem::take(&mut ascii_word));
            }
            if !cjk.is_empty() {
                out.extend(cjk_bigram(&cjk));
                cjk.clear();
            }
        }
    }
    if !ascii_word.is_empty() {
        out.push(ascii_word);
    }
    if !cjk.is_empty() {
        out.extend(cjk_bigram(&cjk));
    }
    out
}

/// 中文块 bigram：长度 1 → 单字 unigram（保证单字可查）；≥2 → 相邻 2 字。
fn cjk_bigram(s: &str) -> Vec<String> {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() == 1 {
        return vec![chars[0].to_string()];
    }
    chars.windows(2).map(|w| w.iter().collect()).collect()
}

/// 由分词结果生成 fulltext 词 term：`ft:{field}:{token}`；同一字段值内重复 token 去重
/// （避免 posting 重复 docid 浪费内存）。中文分词 bigram（M8-P9）。
pub fn fulltext_terms(field: &str, text: &str) -> Vec<String> {
    fulltext_terms_seg(field, text, false)
}

/// 同 `fulltext_terms`，可指定中文分词器（M8-P13）：`use_jieba` → jieba 词典分词。
pub fn fulltext_terms_seg(field: &str, text: &str, use_jieba: bool) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    tokenize_seg(text, use_jieba)
        .into_iter()
        .filter(|t| seen.insert(t.clone()))
        .map(|t| format!("ft:{field}:{t}"))
        .collect()
}
