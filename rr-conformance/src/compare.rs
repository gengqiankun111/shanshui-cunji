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
