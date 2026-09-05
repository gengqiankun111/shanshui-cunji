//! SELECT 投影列标量表达式求值（2026-09-05 阶段 A）。
//!
//! 与 parser（src/sql/parser/parser.rs `parse_scalar_*`）配套：列清单中的表达式列
//! 经 `Expr` 树在此按行求值（输入 = 行文档 JSON 顶层）。
//!
//! MySQL 语义对齐（文档型子集）：
//! - 字段缺失 / JSON null / 函数遇 NULL 参数 → NULL 传播（CONCAT 任一 NULL → NULL）；
//! - 数值运算：整型 ± */ % 保持整型；除法 → 浮点；除零/取模零 → NULL；
//! - 非数值操作数按 MySQL 宽松隐式转换：可转数值字符串参与算术，否则 NULL；
//! - 字符串函数输入按 MySQL 隐式字符串化（数字/布尔 → 文本）；
//! - 受支持函数（parser 白名单）：CONCAT / LOWER / UPPER / LENGTH / ROUND / ABS。

use serde_json::Value;
use crate::sql::parser::Expr;

/// 数值化（MySQL 宽松转换：Number / 数值字符串 / 布尔 1/0；其余 → None）。
fn to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

/// 整型化（仅无小数语义可精确取整时）。
fn to_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.trim().parse::<i64>().ok(),
        Value::Bool(b) => Some(if *b { 1 } else { 0 }),
        _ => None,
    }
}

/// MySQL 宽松字符串化（CONCAT/LOWER/LENGTH 等函数参数）。
fn text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => if *b { "1".to_string() } else { "0".to_string() },
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn num_i(v: i64) -> Value {
    Value::Number(serde_json::Number::from(v))
}
fn num_f(v: f64) -> Value {
    Value::Number(serde_json::Number::from_f64(v).unwrap_or(serde_json::Number::from(0)))
}

/// 单行求值：`e` 相对文档顶层 `doc` 求值（doc 非对象时字段引用 → NULL）。
pub fn eval(e: &Expr, doc: &Value) -> Value {
    match e {
        Expr::NumI(n) => num_i(*n),
        Expr::NumF(x) => num_f(*x),
        Expr::Str(s) => Value::String(s.clone()),
        Expr::Col(name) => match doc.get(name) {
            Some(v) if !v.is_null() => v.clone(),
            _ => Value::Null,
        },
        Expr::Bin { op, l, r } => {
            use crate::sql::parser::BinOp;
            let lv = eval(l, doc);
            let rv = eval(r, doc);
            if lv.is_null() || rv.is_null() {
                return Value::Null;
            }
            // 整数语义（± * %；除法一律浮点）
            if !matches!(op, BinOp::Div) {
                if let (Some(a), Some(b)) = (to_i64(&lv), to_i64(&rv)) {
                    let out = match op {
                        BinOp::Add => a.checked_add(b),
                        BinOp::Sub => a.checked_sub(b),
                        BinOp::Mul => a.checked_mul(b),
                        BinOp::Mod => {
                            if b == 0 {
                                None
                            } else {
                                Some(a % b)
                            }
                        }
                        _ => None,
                    };
                    if let Some(x) = out {
                        return num_i(x);
                    }
                }
            }
            let (a, b) = match (to_f64(&lv), to_f64(&rv)) {
                (Some(a), Some(b)) => (a, b),
                _ => return Value::Null, // 非数值 + 算术（MySQL 数值上下文 0 化；文档型子集保守 NULL）
            };
            match op {
                BinOp::Div => {
                    if b == 0.0 {
                        Value::Null
                    } else {
                        num_f(a / b)
                    }
                }
                BinOp::Mod => {
                    if b == 0.0 {
                        Value::Null
                    } else {
                        num_f(a % b)
                    }
                }
                BinOp::Add => num_f(a + b),
                BinOp::Sub => num_f(a - b),
                BinOp::Mul => num_f(a * b),
            }
        }
        Expr::Call { name, args } => call(name, args, doc),
    }
}

fn call(name: &str, args: &[Expr], doc: &Value) -> Value {
    let evaled: Vec<Value> = args.iter().map(|a| eval(a, doc)).collect();
    match name {
        "CONCAT" => {
            // MySQL：任一参数 NULL → 结果 NULL
            if evaled.iter().any(|v| v.is_null()) {
                return Value::Null;
            }
            let mut s = String::new();
            for v in &evaled {
                s.push_str(&text(v));
            }
            Value::String(s)
        }
        "LOWER" | "UPPER" => {
            let v = evaled.first().cloned().unwrap_or(Value::Null);
            if v.is_null() {
                return Value::Null;
            }
            let t = text(&v);
            Value::String(if name == "LOWER" { t.to_lowercase() } else { t.to_uppercase() })
        }
        "LENGTH" => {
            let v = evaled.first().cloned().unwrap_or(Value::Null);
            if v.is_null() {
                return Value::Null;
            }
            num_i(text(&v).len() as i64) // MySQL LENGTH = 字节数（UTF-8 原文字节）
        }
        "ROUND" => {
            let x = evaled.first().cloned().unwrap_or(Value::Null);
            if x.is_null() {
                return Value::Null;
            }
            let val = match to_f64(&x) {
                Some(v) => v,
                None => return Value::Null,
            };
            let d = evaled
                .get(1)
                .and_then(|v| to_i64(v))
                .unwrap_or(0);
            let factor = 10f64.powi(d.clamp(-308, 308) as i32);
            let rounded = (val * factor).round() / factor;
            // 小数位为 0 时返回整型（对齐 MySQL ROUND(x) 整型结果）
            if d <= 0 && rounded.fract() == 0.0 && rounded.abs() <= i64::MAX as f64 {
                num_i(rounded as i64)
            } else {
                num_f(rounded)
            }
        }
        "ABS" => {
            let v = evaled.first().cloned().unwrap_or(Value::Null);
            if v.is_null() {
                return Value::Null;
            }
            match to_i64(&v) {
                Some(i) => num_i(i.checked_abs().unwrap_or(i64::MAX)),
                None => match to_f64(&v) {
                    Some(x) => num_f(x.abs()),
                    None => Value::Null,
                },
            }
        }
        _ => Value::Null, // parser 白名单外不可达
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc() -> Value {
        json!({ "k": 5, "amount": 2, "city": "beijing", "n": 2.5 })
    }

    #[test]
    fn arithmetic_and_precedence() {
        let d = doc();
        // 整型保持：k*amount+1 = 11
        assert_eq!(eval(&Expr::Bin {
            op: crate::sql::parser::BinOp::Add,
            l: Box::new(Expr::Bin {
                op: crate::sql::parser::BinOp::Mul,
                l: Box::new(Expr::Col("k".into())),
                r: Box::new(Expr::Col("amount".into())),
            }),
            r: Box::new(Expr::NumI(1)),
        }, &d), json!(11));
        // 除法 → 浮点：k/amount = 2.5
        assert_eq!(eval(&Expr::Bin {
            op: crate::sql::parser::BinOp::Div,
            l: Box::new(Expr::Col("k".into())),
            r: Box::new(Expr::Col("amount".into())),
        }, &d), json!(2.5));
        // 除零 → NULL；取模零 → NULL
        assert_eq!(eval(&Expr::Bin {
            op: crate::sql::parser::BinOp::Div,
            l: Box::new(Expr::NumI(1)),
            r: Box::new(Expr::NumI(0)),
        }, &d), Value::Null);
        // 缺字段 → NULL 传播
        assert_eq!(eval(&Expr::Bin {
            op: crate::sql::parser::BinOp::Add,
            l: Box::new(Expr::Col("zzz".into())),
            r: Box::new(Expr::NumI(1)),
        }, &d), Value::Null);
    }

    #[test]
    fn string_functions_and_concat() {
        let d = doc();
        let call = |name: &str, args: Vec<Expr>| eval(&Expr::Call { name: name.into(), args }, &d);
        assert_eq!(
            call("CONCAT", vec![Expr::Col("city".into()), Expr::Str("-".into()), Expr::Col("k".into())]),
            json!("beijing-5")
        );
        assert_eq!(call("UPPER", vec![Expr::Col("city".into())]), json!("BEIJING"));
        assert_eq!(call("LENGTH", vec![Expr::Col("city".into())]), json!(7));
        assert_eq!(call("ROUND", vec![Expr::NumF(2.5)]), json!(3));
        assert_eq!(call("ABS", vec![Expr::Bin {
            op: crate::sql::parser::BinOp::Sub,
            l: Box::new(Expr::NumI(0)),
            r: Box::new(Expr::NumI(7)),
        }]), json!(7));
        // CONCAT NULL 传播
        assert_eq!(call("CONCAT", vec![Expr::Col("city".into()), Expr::Col("missing".into())]), Value::Null);
        // 浮点 + 数值字符串参数
        assert_eq!(
            call("CONCAT", vec![Expr::NumF(1.5), Expr::Str("-x".into())]),
            json!("1.5-x")
        );
    }
}
