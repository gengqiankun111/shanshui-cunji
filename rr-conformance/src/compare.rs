//! 结果序列化/比较 + 错误码分类（采集对比用）。输入为已解包的行值（Vec<Value>）。

use mysql::Value;

/// 一行值 → 定长字符串（NULL/整数/字符串统一文本）。
pub fn value_row(vals: &[Value]) -> Vec<String> {
    vals.iter().map(val_str).collect()
}

pub fn val_str(v: &Value) -> String {
    match v {
        Value::NULL => "NULL".to_string(),
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        other => format!("{other:?}"),
    }
}

/// 行集 → 每行 '|' 拼接（比较/日志用；顺序确定性由 SQL ORDER BY 保证）。
pub fn rows_to_strings(rows: &[Vec<Value>]) -> Vec<String> {
    rows.iter().map(|r| value_row(r).join("|")).collect()
}

/// P141 A1-1：**数值语义规范化比较键**——屏蔽 MySQL DECIMAL 尾零（"2.0000"）vs SCC
/// 浮点/Double 文本（"2"）与整/浮同值的表示差异：单元格文本可解析为数值 → 统一去尾零
/// 归一（整数按 i64 文本、小数按 f64 最短文本）；NULL/字符串原样。两侧同 SQL 同数据
/// 下行集键一致 ⇔ 数值语义等值（用于 txn 聚合探针的双端/期望断言）。
pub fn value_key(v: &Value) -> String {
    match v {
        Value::NULL => "NULL".to_string(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        Value::Bytes(b) => {
            let s = String::from_utf8_lossy(b).into_owned();
            match s.trim().parse::<f64>() {
                Ok(x) => norm_num(x),
                Err(_) => s,
            }
        }
        Value::Float(x) => norm_num(*x as f64),
        other => format!("{other:?}"),
    }
}

/// 数值归一：整数（|x|<9e15 内）→ i64 文本；小数 → f64 最短文本；非有限原样。
fn norm_num(x: f64) -> String {
    if !x.is_finite() {
        return format!("{x}");
    }
    if x.fract() == 0.0 && x.abs() < 9e15 {
        format!("{}", x as i64)
    } else {
        format!("{x}")
    }
}

/// 行 → 规范化键；行集 → 行键列表。
pub fn row_key(vals: &[Value]) -> String {
    vals.iter().map(value_key).collect::<Vec<_>>().join("|")
}

pub fn rows_to_keys(rows: &[Vec<Value>]) -> Vec<String> {
    rows.iter().map(|r| row_key(r)).collect()
}

/// 错误分类标签：唯一键冲突 / 锁超时 / 死锁 / 其它（错误码由文本/编码共同识别）。
pub fn classify_err(e: &mysql::Error) -> (u16, String) {
    let msg = e.to_string();
    let m = msg.to_uppercase();
    let tag = if m.contains("1062") || m.contains("DUPLICATE") || m.contains("唯一") {
        "DUPLICATE"
    } else if m.contains("1205") || m.contains("TIMEOUT") || m.contains("超时") {
        "LOCK_TIMEOUT"
    } else if m.contains("1213") || m.contains("DEADLOCK") || m.contains("死锁") {
        "DEADLOCK"
    } else {
        "OTHER"
    };
    (0, format!("{tag} {msg}"))
}
