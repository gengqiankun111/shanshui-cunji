//! 导出管道（design 20.5）：SST 流式扫描 → **Filter（条件筛选）** → **Projection（字段映射/脱敏）**
//! → **Sink Adapter 分叉**（CSV / Parquet / JDBC 直连）。
//!
//! 内存恒定：每批 `batch_size` 刷一次（batch × 单行大小）；无 Filter/Projection 时
//! 不做 JSON 解析（原样透传），零额外开销。
//!
//! Filter 语法：`AND` 分隔条件，每条件 `field op value`：
//!   op ∈ `=` `!=` `>` `>=` `<` `<=` `CONTAINS`；value ∈ 数字 / '字符串' / "字符串" / true / false / null。
//! Projection 语法：`--project 'a,b,c'`（逗号分隔字段子集）+ `--mask 'field=pattern'`（字段值脱敏替换）。

use serde_json::Value;

use crate::error::{Error, Result};

// ============ Filter：JSON 字段条件筛选 ============

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CmpOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Contains,
}

#[derive(Debug)]
struct Cond {
    field: String,
    op: CmpOp,
    value: Value,
}

/// 条件筛选：字段级比较（AND 语义）。
#[derive(Debug, Default)]
pub struct Filter {
    conds: Vec<Cond>,
}

impl Filter {
    /// 解析过滤表达式（空串 / None → 空过滤器 = 全量通过）。
    pub fn parse(expr: Option<&str>) -> Result<Self> {
        let mut conds = Vec::new();
        if let Some(e) = expr {
            let e = e.trim();
            if e.is_empty() {
                return Ok(Self { conds });
            }
            for part in e.split("AND") {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }
                conds.push(Self::parse_cond(part)?);
            }
        }
        Ok(Self { conds })
    }

    fn parse_cond(part: &str) -> Result<Cond> {
        // 定位操作符（从后往前找，避免字段名含操作符字符；CONTAINS 最长先匹配）
        let ops: &[(&str, CmpOp)] = &[
            ("CONTAINS", CmpOp::Contains),
            ("!=", CmpOp::Ne),
            (">=", CmpOp::Ge),
            ("<=", CmpOp::Le),
            ("=", CmpOp::Eq),
            (">", CmpOp::Gt),
            ("<", CmpOp::Lt),
        ];
        for (sym, op) in ops {
            if let Some(idx) = part.rfind(sym) {
                let field = part[..idx].trim();
                let raw = part[idx + sym.len()..].trim();
                if field.is_empty() || raw.is_empty() {
                    return Err(Error::Unsupported(format!(
                        "过滤条件格式错误: `{part}`（期望 `field op value`）"
                    )));
                }
                return Ok(Cond {
                    field: field.to_string(),
                    op: *op,
                    value: parse_value(raw)?,
                });
            }
        }
        Err(Error::Unsupported(format!(
            "过滤条件缺少操作符: `{part}`（支持 = != > >= < <= CONTAINS）"
        )))
    }

    pub fn is_empty(&self) -> bool {
        self.conds.is_empty()
    }

    /// 文档是否通过全部条件（AND）。
    pub fn matches(&self, doc: &Value) -> bool {
        for c in &self.conds {
            let v = doc.get(&c.field);
            if !match_op(v, c.op, &c.value) {
                return false;
            }
        }
        true
    }
}

fn match_op(v: Option<&Value>, op: CmpOp, target: &Value) -> bool {
    match op {
        CmpOp::Eq => v.map_or(false, |x| x == target),
        CmpOp::Ne => v.map_or(true, |x| x != target),
        CmpOp::Contains => v.and_then(Value::as_str).map_or(false, |s| {
            target.as_str().map_or(false, |t| s.contains(t))
        }),
        CmpOp::Gt => cmp_num(v, target) == Some(std::cmp::Ordering::Greater),
        CmpOp::Ge => cmp_num(v, target) != Some(std::cmp::Ordering::Less),
        CmpOp::Lt => cmp_num(v, target) == Some(std::cmp::Ordering::Less),
        CmpOp::Le => cmp_num(v, target) != Some(std::cmp::Ordering::Greater),
    }
}

/// 数值比较（doc 字段与目标都解析为 f64；无法解析 → None）。
fn cmp_num(v: Option<&Value>, target: &Value) -> Option<std::cmp::Ordering> {
    let a = num_of(v?)?;
    let b = num_of(target)?;
    Some(a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal))
}

fn num_of(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

/// 解析过滤值：'x' / "x" 字符串、数字、true/false、null。
fn parse_value(raw: &str) -> Result<Value> {
    let raw = raw.trim();
    if (raw.starts_with('\'') && raw.ends_with('\'') && raw.len() >= 2)
        || (raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2)
    {
        let inner = &raw[1..raw.len() - 1];
        // 反转义：\' → '，\\ → \
        let mut out = String::with_capacity(inner.len());
        let mut chars = inner.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.next() {
                    Some('\'') => out.push('\''),
                    Some('\\') => out.push('\\'),
                    Some(other) => {
                        out.push('\\');
                        out.push(other);
                    }
                    None => out.push('\\'),
                }
            } else {
                out.push(c);
            }
        }
        return Ok(Value::String(out));
    }
    match raw {
        "true" => return Ok(Value::Bool(true)),
        "false" => return Ok(Value::Bool(false)),
        "null" => return Ok(Value::Null),
        _ => {}
    }
    if let Ok(n) = raw.parse::<i64>() {
        return Ok(Value::Number(n.into()));
    }
    if let Ok(f) = raw.parse::<f64>() {
        if let Some(v) = serde_json::Number::from_f64(f) {
            return Ok(Value::Number(v));
        }
    }
    Err(Error::Unsupported(format!("无法解析过滤值: `{raw}`")))
}

// ============ Projection：字段映射 / 脱敏 ============

/// 字段映射与脱敏：`--project 'a,b,c'` 选字段子集 + `--mask 'field=pattern'` 值替换。
#[derive(Debug, Default)]
pub struct Projection {
    /// None = 全字段（仅脱敏）；Some = 只保留这些字段。
    fields: Option<Vec<String>>,
    /// (字段名, 替换文本)。
    masks: Vec<(String, String)>,
}

impl Projection {
    pub fn parse(project: Option<&str>, masks: &[String]) -> Result<Self> {
        let fields = match project {
            Some(p) => {
                let p = p.trim();
                if p.is_empty() {
                    None
                } else {
                    Some(p.split(',').map(|s| s.trim().to_string()).collect())
                }
            }
            None => None,
        };
        let mut mask_list = Vec::new();
        for m in masks {
            let (f, pat) = m.split_once('=').ok_or_else(|| {
                Error::Unsupported(format!("脱敏格式错误: `{m}`（期望 `field=pattern`）"))
            })?;
            mask_list.push((f.trim().to_string(), pat.to_string()));
        }
        Ok(Self {
            fields,
            masks: mask_list,
        })
    }

    pub fn is_identity(&self) -> bool {
        self.fields.is_none() && self.masks.is_empty()
    }

    /// 应用投影：选字段 + 脱敏，输出 JSON 文本（保留字段顺序：字段子集顺序或原文档顺序）。
    pub fn apply(&self, doc: &Value) -> Result<String> {
        let mut out = serde_json::Map::new();
        match &self.fields {
            Some(fields) => {
                for f in fields {
                    if let Some(v) = doc.get(f) {
                        out.insert(f.clone(), self.mask_field(f, v));
                    }
                }
            }
            None => {
                if let Some(obj) = doc.as_object() {
                    for (k, v) in obj {
                        out.insert(k.clone(), self.mask_field(k, v));
                    }
                }
            }
        }
        serde_json::to_string(&Value::Object(out))
            .map_err(|e| Error::Serialize(format!("Projection JSON 序列化失败: {e}")))
    }

    fn mask_field(&self, field: &str, v: &Value) -> Value {
        for (f, pat) in &self.masks {
            if f == field {
                return Value::String(pat.clone());
            }
        }
        v.clone()
    }
}

// ============ Sink Adapter：分叉目标 ============

/// 导出目标分叉：每批 `(docid, JSON 文本)` 刷一次。
pub trait Sink {
    fn write_batch(&mut self, rows: &[(u64, &str)]) -> Result<()>;
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

// ============ 测试 ============

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn filter_parses_and_matches() {
        let f = Filter::parse(Some("amount>100 AND status='active' AND city CONTAINS 'an'")).unwrap();
        assert_eq!(f.conds.len(), 3);
        assert!(f.matches(&json!({"amount": 200, "status": "active", "city": "hangzhou"})));
        assert!(!f.matches(&json!({"amount": 50, "status": "active", "city": "hangzhou"})));
        assert!(!f.matches(&json!({"amount": 200, "status": "pending", "city": "hangzhou"})));
        assert!(!f.matches(&json!({"amount": 200, "status": "active", "city": "beijing"})));
        // 缺失字段 → 不通过（Eq 语义）
        assert!(!f.matches(&json!({"amount": 200})));
        // 空过滤器全通过
        assert!(Filter::parse(None).unwrap().is_empty());
        assert!(Filter::parse(Some("  ")).unwrap().matches(&json!({"a": 1})));
    }

    #[test]
    fn filter_comparison_and_ne() {
        let f = Filter::parse(Some("score>=90 AND score!=100")).unwrap();
        assert!(f.matches(&json!({"score": 95})));
        assert!(!f.matches(&json!({"score": 100})));
        assert!(!f.matches(&json!({"score": 89})));
        let f2 = Filter::parse(Some("name='a\\'b'")).unwrap(); // 含引号字符串
        assert!(f2.matches(&json!({"name": "a'b"})));
    }

    #[test]
    fn filter_bad_expression_errors() {
        assert!(Filter::parse(Some("amount")).is_err());
        assert!(Filter::parse(Some("amount=abc")).is_err());
    }

    #[test]
    fn projection_selects_fields() {
        let p = Projection::parse(Some("id,name"), &[]).unwrap();
        let doc = json!({"id": 1, "name": "alice", "age": 30, "secret": "s3"});
        let out: Value = serde_json::from_str(&p.apply(&doc).unwrap()).unwrap();
        assert_eq!(out, json!({"id": 1, "name": "alice"}));
    }

    #[test]
    fn projection_masks_fields() {
        let p = Projection::parse(None, &["email=***".to_string()]).unwrap();
        let doc = json!({"id": 1, "email": "alice@example.com", "name": "alice"});
        let out: Value = serde_json::from_str(&p.apply(&doc).unwrap()).unwrap();
        assert_eq!(out["email"], "***");
        assert_eq!(out["name"], "alice");
        // 全字段 + 脱敏
        let p2 = Projection::parse(Some("email,name"), &["email=hidden".to_string()]).unwrap();
        let out2: Value = serde_json::from_str(&p2.apply(&doc).unwrap()).unwrap();
        assert_eq!(out2, json!({"email": "hidden", "name": "alice"}));
    }

    #[test]
    fn projection_identity_when_noop() {
        let p = Projection::parse(None, &[]).unwrap();
        assert!(p.is_identity());
        let p2 = Projection::parse(Some("a"), &[]).unwrap();
        assert!(!p2.is_identity());
    }
}
