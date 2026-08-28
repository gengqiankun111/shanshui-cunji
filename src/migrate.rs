//! 迁移工具核心（development 5.16 基础版）：CSV / mysqldump 全量导入。
//!
//! - 输入：CSV 文件（首行表头）或 mysqldump SQL 导出（`INSERT INTO ... VALUES (...)` 行）；
//! - 字段映射：CSV 列名 / SQL 列名直接作为 JSON 字段名；含 `docid` 列则作为主键，否则从 1 递增；
//! - 全量导入、单线程，产出迁移报告（成功/失败/耗时）。

use std::time::Instant;

use crate::engine::Engine;
use crate::error::{Error, Result};

/// 迁移报告。
#[derive(Debug, Clone, Copy)]
pub struct ImportReport {
    pub rows: u64,
    pub failed: u64,
    /// 增量导入跳过的已导入行数（游标续传）。
    pub skipped: u64,
    pub elapsed_ms: u64,
}

impl ImportReport {
    fn new(rows: u64, failed: u64, skipped: u64, elapsed_ms: u64) -> Self {
        Self {
            rows,
            failed,
            skipped,
            elapsed_ms,
        }
    }
}

/// 读取增量导入 checkpoint（文件记录最大已导入 docid；缺失 = 0）。
pub fn load_checkpoint(path: &std::path::Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let text = std::fs::read_to_string(path)?;
    text.trim()
        .parse::<u64>()
        .map_err(|e| Error::Corrupted(format!("checkpoint 解析失败: {e}")))
}

/// 写入增量导入 checkpoint（tmp + rename 原子写）。
pub fn save_checkpoint(path: &std::path::Path, last_docid: u64) -> Result<()> {
    let tmp = path.with_extension("cp.tmp");
    std::fs::write(&tmp, last_docid.to_string())?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// 倒排字段白名单过滤（import-schema，design 20）：term 形如 `field=value`，
/// 白名单存在时只保留声明字段的词条；None 不过滤。
fn filter_terms(terms: Vec<String>, whitelist: Option<&[String]>) -> Vec<String> {
    match whitelist {
        None => terms,
        Some(wl) => terms
            .into_iter()
            .filter(|t| {
                wl.iter()
                    .any(|f| t.starts_with(&format!("{f}=")))
            })
            .collect(),
    }
}

/// CSV 全量导入：首行表头即 JSON 字段名；`docid` 列存在则用之，否则从 1 递增。
pub fn import_csv(engine: &mut Engine, path: &std::path::Path) -> Result<ImportReport> {
    import_csv_filtered(engine, path, None)
}

/// CSV 导入并应用倒排字段白名单（import-schema，design 20）：白名单存在时只对声明字段建倒排。
pub fn import_csv_filtered(
    engine: &mut Engine,
    path: &std::path::Path,
    whitelist: Option<&[String]>,
) -> Result<ImportReport> {
    import_csv_worker(engine, path, whitelist, None, None)
}

/// CSV **增量导入**（design 5.16 阶段 3 高级版）：基于 docid 游标断点续传。
/// `checkpoint_path` 记录已导入的最大 docid（原子写）；重启续跑只处理新增行（docid 更大），
/// 已导入行自动跳过。适合追加式日志 / 埋点数据增量同步。
pub fn import_csv_incremental(
    engine: &mut Engine,
    path: &std::path::Path,
    whitelist: Option<&[String]>,
    checkpoint_path: &std::path::Path,
) -> Result<ImportReport> {
    let base = load_checkpoint(checkpoint_path)?;
    import_csv_worker(engine, path, whitelist, Some(base), Some(checkpoint_path))
}

/// CSV 导入工作器（全量 / 增量共用）。
fn import_csv_worker(
    engine: &mut Engine,
    path: &std::path::Path,
    whitelist: Option<&[String]>,
    cp_base: Option<u64>,
    cp_path: Option<&std::path::Path>,
) -> Result<ImportReport> {
    let t = Instant::now();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .map_err(|e| Error::Migrate(format!("CSV 打开失败: {e}")))?;
    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| Error::Migrate(format!("CSV 表头读取失败: {e}")))?
        .iter()
        .map(|h| h.to_string())
        .collect();
    if headers.is_empty() {
        return Err(Error::Unsupported("CSV 表头为空".into()));
    }
    let docid_col = headers.iter().position(|h| h == "docid");
    // 增量导入要求显式 docid 列（自动递增无法续传）
    if cp_base.is_some() && docid_col.is_none() {
        return Err(Error::Migrate(
            "增量导入必须含 docid 列（自动递增无法断点续传）".into(),
        ));
    }

    let mut rows = 0u64;
    let mut failed = 0u64;
    let mut skipped = 0u64;
    let mut last_docid = cp_base.unwrap_or(0);
    let mut next_id = 1u64;
    for rec in reader.records() {
        let rec = match rec {
            Ok(r) => r,
            Err(_) => {
                failed += 1;
                continue;
            }
        };
        let mut obj = serde_json::Map::new();
        for (i, field) in rec.iter().enumerate() {
            if let Some(name) = headers.get(i) {
                obj.insert(name.clone(), serde_json::Value::String(field.to_string()));
            }
        }
        // 主键：docid 列优先，否则递增
        let docid = match docid_col {
            Some(_) => match obj.get("docid").and_then(|v| v.as_str()) {
                Some(s) => match s.trim().parse::<u64>() {
                    Ok(d) => d,
                    Err(_) => {
                        failed += 1;
                        continue;
                    }
                },
                None => {
                    failed += 1;
                    continue;
                }
            },
            None => {
                // 自动分配：避让已占用 docid
                let mut d = next_id;
                while engine.get(d)?.is_some() {
                    d += 1;
                }
                next_id = d.wrapping_add(1);
                obj.insert("docid".into(), serde_json::Value::from(d));
                d
            }
        };
        // 增量：跳过已导入的 docid（游标续传）
        if let Some(base) = cp_base {
            if docid <= base {
                skipped += 1;
                continue;
            }
        }
        let bytes = serde_json::to_vec(&serde_json::Value::Object(obj))
            .map_err(|e| Error::Serialize(format!("JSON 序列化失败: {e}")))?;
        let val = serde_json::from_slice::<serde_json::Value>(&bytes)
            .map_err(|e| Error::Serialize(format!("JSON 解析失败: {e}")))?;
        let terms = filter_terms(crate::server::extract_terms(&val), whitelist);
        let term_refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        match engine.put(docid, bytes, &term_refs) {
            Ok(()) => {
                rows += 1;
                // 增量：推进并持久化 checkpoint（原子写）
                if docid > last_docid {
                    last_docid = docid;
                    if let Some(p) = cp_path {
                        save_checkpoint(p, last_docid)?;
                    }
                }
            }
            Err(_) => failed += 1,
        }
    }
    // 独立进程导入：倒排词条一次性刷盘
    engine.flush_inverted()?;
    Ok(ImportReport::new(
        rows,
        failed,
        skipped,
        t.elapsed().as_millis() as u64,
    ))
}

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

/// mysqldump 全量导入：解析 `INSERT INTO` 行构造 JSON 文档写入引擎。
pub fn import_mysqldump(engine: &mut Engine, path: &std::path::Path) -> Result<ImportReport> {
    let t = Instant::now();
    let text = std::fs::read_to_string(path)?;
    let mut rows = 0u64;
    let mut failed = 0u64;
    let mut next_id = 1u64;
    for line in text.lines() {
        let line = line.trim();
        if !line.starts_with("INSERT INTO") && !line.starts_with("insert into") {
            continue;
        }
        let Some((cols, tuples)) = parse_mysql_insert_line(line) else {
            continue;
        };
        for tuple in tuples {
            let mut obj = serde_json::Map::new();
            for (i, v) in tuple.iter().enumerate() {
                let name = cols.get(i).cloned().unwrap_or_else(|| format!("c{i}"));
                let jv = match v {
                    SqlValue::Str(s) => serde_json::Value::String(s.clone()),
                    SqlValue::Num(n) => {
                        if let Ok(i) = n.parse::<i64>() {
                            serde_json::Value::from(i)
                        } else if let Ok(f) = n.parse::<f64>() {
                            serde_json::Value::from(f)
                        } else {
                            serde_json::Value::String(n.clone())
                        }
                    }
                    SqlValue::Null => serde_json::Value::Null,
                    SqlValue::Other(o) => serde_json::Value::String(o.clone()),
                };
                obj.insert(name, jv);
            }
            // 主键：docid / id 列（MySQL 惯例）优先，否则递增
            let pk = if obj.contains_key("docid") {
                Some("docid")
            } else if obj.contains_key("id") {
                Some("id")
            } else {
                None
            };
            let docid = match pk {
                Some(k) => match obj.get(k) {
                    Some(serde_json::Value::Number(n)) => n.as_u64(),
                    Some(serde_json::Value::String(s)) => s.trim().parse::<u64>().ok(),
                    _ => None,
                },
                None => None,
            };
            let docid = match docid {
                Some(d) => d,
                None => {
                    // 自动分配：避让已占用 docid（SQL 显式 id 与递增可能冲突）
                    let mut d = next_id;
                    while engine.get(d)?.is_some() {
                        d += 1;
                    }
                    next_id = d.wrapping_add(1);
                    obj.insert("docid".into(), serde_json::Value::from(d));
                    d
                }
            };
            let bytes = match serde_json::to_vec(&serde_json::Value::Object(obj)) {
                Ok(b) => b,
                Err(_) => {
                    failed += 1;
                    continue;
                }
            };
            let terms = match serde_json::from_slice::<serde_json::Value>(&bytes) {
                Ok(v) => crate::server::extract_terms(&v),
                Err(_) => {
                    failed += 1;
                    continue;
                }
            };
            let term_refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            match engine.put(docid, bytes, &term_refs) {
                Ok(()) => rows += 1,
                Err(_) => failed += 1,
            }
        }
    }
    engine.flush_inverted()?;
    Ok(ImportReport::new(
        rows,
        failed,
        0,
        t.elapsed().as_millis() as u64,
    ))
}

/// JSONL 全量导入（数据管道 `import --json`，development 5.27）：每行一个 JSON 对象，
/// 含 docid/id 列作主键（否则从 1 递增），导入完成输出迁移报告。
pub fn import_json(engine: &mut Engine, path: &std::path::Path) -> Result<ImportReport> {
    import_json_filtered(engine, path, None)
}

/// JSONL 导入并应用倒排字段白名单（import-schema，design 20）。
pub fn import_json_filtered(
    engine: &mut Engine,
    path: &std::path::Path,
    whitelist: Option<&[String]>,
) -> Result<ImportReport> {
    import_json_worker(engine, path, whitelist, None, None)
}

/// JSONL **增量导入**（design 5.16 阶段 3）：docid 游标断点续传（需 docid/id 列）。
pub fn import_json_incremental(
    engine: &mut Engine,
    path: &std::path::Path,
    whitelist: Option<&[String]>,
    checkpoint_path: &std::path::Path,
) -> Result<ImportReport> {
    let base = load_checkpoint(checkpoint_path)?;
    import_json_worker(engine, path, whitelist, Some(base), Some(checkpoint_path))
}

/// JSONL 导入工作器（全量 / 增量共用）。
fn import_json_worker(
    engine: &mut Engine,
    path: &std::path::Path,
    whitelist: Option<&[String]>,
    cp_base: Option<u64>,
    cp_path: Option<&std::path::Path>,
) -> Result<ImportReport> {
    let t = Instant::now();
    let text = std::fs::read_to_string(path)?;
    let mut rows = 0u64;
    let mut failed = 0u64;
    let mut skipped = 0u64;
    let mut last_docid = cp_base.unwrap_or(0);
    let mut next_id = 1u64;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut obj: serde_json::Map<String, serde_json::Value> = match serde_json::from_str(line) {
            Ok(serde_json::Value::Object(m)) => m,
            _ => {
                failed += 1;
                continue;
            }
        };
        // 主键：docid / id 列优先，否则递增
        let pk = if obj.contains_key("docid") {
            Some("docid")
        } else if obj.contains_key("id") {
            Some("id")
        } else {
            None
        };
        let docid = match pk.and_then(|k| obj.get(k)) {
            Some(serde_json::Value::Number(n)) => n.as_u64(),
            Some(serde_json::Value::String(s)) => s.trim().parse::<u64>().ok(),
            _ => None,
        };
        let docid = match docid {
            Some(d) => d,
            None => {
                // 增量要求显式 docid/id 列（自动递增无法续传）
                if cp_base.is_some() {
                    failed += 1;
                    continue;
                }
                // 自动分配：避让已占用 docid（显式 docid 与递增可能冲突）
                let mut d = next_id;
                while engine.get(d)?.is_some() {
                    d += 1;
                }
                next_id = d.wrapping_add(1);
                obj.insert("docid".into(), serde_json::Value::from(d));
                d
            }
        };
        // 增量：跳过已导入的 docid（游标续传）
        if let Some(base) = cp_base {
            if docid <= base {
                skipped += 1;
                continue;
            }
        }
        let bytes = match serde_json::to_vec(&serde_json::Value::Object(obj)) {
            Ok(b) => b,
            Err(_) => {
                failed += 1;
                continue;
            }
        };
        let terms = match serde_json::from_slice::<serde_json::Value>(&bytes) {
            Ok(v) => filter_terms(crate::server::extract_terms(&v), whitelist),
            Err(_) => {
                failed += 1;
                continue;
            }
        };
        let term_refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        match engine.put(docid, bytes, &term_refs) {
            Ok(()) => {
                rows += 1;
                // 增量：推进并持久化 checkpoint（原子写）
                if docid > last_docid {
                    last_docid = docid;
                    if let Some(p) = cp_path {
                        save_checkpoint(p, last_docid)?;
                    }
                }
            }
            Err(_) => failed += 1,
        }
    }
    engine.flush_inverted()?;
    Ok(ImportReport::new(
        rows,
        failed,
        skipped,
        t.elapsed().as_millis() as u64,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sql_line_with_columns_and_values() {
        let line =
            "INSERT INTO `t` (`name`,`age`,`note`) VALUES ('alice',30,NULL),('bob',25,'a\\'b');";
        let (cols, tuples) = parse_mysql_insert_line(line).unwrap();
        assert_eq!(cols, vec!["name", "age", "note"]);
        assert_eq!(tuples.len(), 2);
        assert_eq!(tuples[0][0], SqlValue::Str("alice".into()));
        assert_eq!(tuples[0][1], SqlValue::Num("30".into()));
        assert_eq!(tuples[0][2], SqlValue::Null);
        assert_eq!(tuples[1][2], SqlValue::Str("a'b".into()), "转义引号");
    }

    #[test]
    fn parse_sql_line_without_columns() {
        let line = "INSERT INTO `t` VALUES (1,'x'),(2,'y');";
        let (cols, tuples) = parse_mysql_insert_line(line).unwrap();
        assert!(cols.is_empty());
        assert_eq!(tuples.len(), 2);
        assert_eq!(tuples[0][0], SqlValue::Num("1".into()));
        assert_eq!(tuples[1][1], SqlValue::Str("y".into()));
    }

    #[test]
    fn parse_non_insert_line_returns_none() {
        assert!(parse_mysql_insert_line("CREATE TABLE t (id int);").is_none());
        assert!(parse_mysql_insert_line("-- comment").is_none());
    }

    #[test]
    fn csv_import_creates_documents() {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("in.csv");
        std::fs::write(
            &csv_path,
            "docid,status,type\n1,active,order\n2,active,view\n3,pending,order\n",
        )
        .unwrap();
        let cfg = crate::config::Config::default();
        let data_dir = dir.path().join("data");
        let mut engine = Engine::open(&data_dir, &cfg).unwrap();
        let rep = import_csv(&mut engine, &csv_path).unwrap();
        assert_eq!(rep.rows, 3);
        assert_eq!(rep.failed, 0);
        // 查询验证
        assert_eq!(
            engine.search_term("status=active").unwrap().len(),
            2,
            "status=active 应命中 2 条"
        );
        assert_eq!(
            engine.search_term("type=order").unwrap().len(),
            2,
            "type=order 应命中 2 条"
        );
    }

    #[test]
    fn sql_import_creates_documents() {
        let dir = tempfile::tempdir().unwrap();
        let sql_path = dir.path().join("dump.sql");
        std::fs::write(
            &sql_path,
            "INSERT INTO `users` (`id`,`status`,`city`) VALUES (10,'active','bj'),(20,'pending','sh');\n",
        )
        .unwrap();
        let cfg = crate::config::Config::default();
        let data_dir = dir.path().join("data");
        let mut engine = Engine::open(&data_dir, &cfg).unwrap();
        let rep = import_mysqldump(&mut engine, &sql_path).unwrap();
        assert_eq!(rep.rows, 2);
        assert_eq!(rep.failed, 0);
        assert_eq!(
            engine.search_term("status=active").unwrap().len(),
            1,
            "docid 用 id 列"
        );
        let val = engine.get(10).unwrap().expect("docid=10 存在");
        assert!(String::from_utf8_lossy(&val).contains("bj"));
    }

    #[test]
    fn sql_import_without_docid_column_auto_assigns() {
        let dir = tempfile::tempdir().unwrap();
        let sql_path = dir.path().join("dump.sql");
        std::fs::write(
            &sql_path,
            "INSERT INTO `t` (`name`) VALUES ('a'),('b'),('c');\n",
        )
        .unwrap();
        let cfg = crate::config::Config::default();
        let data_dir = dir.path().join("data");
        let mut engine = Engine::open(&data_dir, &cfg).unwrap();
        let rep = import_mysqldump(&mut engine, &sql_path).unwrap();
        assert_eq!(rep.rows, 3);
        assert_eq!(engine.search_term("name=a").unwrap().len(), 1);
        assert_eq!(engine.search_term("name=c").unwrap().len(), 1);
    }

    #[test]
    fn json_import_creates_documents() {
        let dir = tempfile::tempdir().unwrap();
        let json_path = dir.path().join("in.jsonl");
        std::fs::write(
            &json_path,
            "{\"docid\":1,\"status\":\"active\"}\n{\"status\":\"pending\"}\n{\"docid\":3,\"city\":\"bj\"}\n",
        )
        .unwrap();
        let cfg = crate::config::Config::default();
        let data_dir = dir.path().join("data");
        let mut engine = Engine::open(&data_dir, &cfg).unwrap();
        let rep = import_json(&mut engine, &json_path).unwrap();
        assert_eq!(rep.rows, 3);
        assert_eq!(rep.failed, 0);
        // 第 2 行无 docid → 自动分配（递增）
        assert_eq!(engine.search_term("status=active").unwrap().len(), 1);
        assert_eq!(engine.search_term("status=pending").unwrap().len(), 1);
        assert_eq!(engine.search_term("city=bj").unwrap().len(), 1);
        // 自动分配的 docid 落在 1/3 之外（=2）
        let val = engine.get(2).unwrap().expect("自动分配 docid=2");
        assert!(String::from_utf8_lossy(&val).contains("pending"));
    }

    // ---- 增量导入（design 5.16 阶段 3：docid 游标断点续传）----

    #[test]
    fn incremental_json_import_resumes_from_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let json_path = dir.path().join("in.jsonl");
        let cp_path = dir.path().join("checkpoint.cp");
        // 首轮：3 条
        std::fs::write(
            &json_path,
            "{\"docid\":1,\"s\":\"a\"}\n{\"docid\":2,\"s\":\"a\"}\n{\"docid\":3,\"s\":\"b\"}\n",
        )
        .unwrap();
        let cfg = crate::config::Config::default();
        let data_dir = dir.path().join("data");
        let mut engine = Engine::open(&data_dir, &cfg).unwrap();
        let rep1 = import_json_incremental(&mut engine, &json_path, None, &cp_path).unwrap();
        assert_eq!(rep1.rows, 3);
        assert_eq!(rep1.skipped, 0);
        assert_eq!(load_checkpoint(&cp_path).unwrap(), 3, "checkpoint 推进到 3");

        // 追加 2 条新数据（docid 4、5）
        std::fs::write(
            &json_path,
            "{\"docid\":1,\"s\":\"a\"}\n{\"docid\":2,\"s\":\"a\"}\n{\"docid\":3,\"s\":\"b\"}\n{\"docid\":4,\"s\":\"c\"}\n{\"docid\":5,\"s\":\"c\"}\n",
        )
        .unwrap();
        // 续跑：只处理新行，旧行跳过
        let rep2 = import_json_incremental(&mut engine, &json_path, None, &cp_path).unwrap();
        assert_eq!(rep2.rows, 2, "只导入新增 2 条");
        assert_eq!(rep2.skipped, 3, "旧 3 条跳过");
        assert_eq!(load_checkpoint(&cp_path).unwrap(), 5);
        // 数据正确
        assert_eq!(engine.get(4).unwrap().unwrap(), b"{\"docid\":4,\"s\":\"c\"}");
        assert_eq!(engine.search_term("s=c").unwrap().len(), 2);
        // 再次续跑：全部跳过
        let rep3 = import_json_incremental(&mut engine, &json_path, None, &cp_path).unwrap();
        assert_eq!(rep3.rows, 0);
        assert_eq!(rep3.skipped, 5);
    }

    #[test]
    fn incremental_csv_requires_docid_column() {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("in.csv");
        std::fs::write(&csv_path, "status,type\nactive,order\n").unwrap();
        let cfg = crate::config::Config::default();
        let data_dir = dir.path().join("data");
        let mut engine = Engine::open(&data_dir, &cfg).unwrap();
        let cp = dir.path().join("cp");
        let err = import_csv_incremental(&mut engine, &csv_path, None, &cp).unwrap_err();
        assert!(
            err.to_string().contains("docid"),
            "无 docid 列增量导入应报错: {err}"
        );
    }

    #[test]
    fn checkpoint_atomic_persist() {
        let dir = tempfile::tempdir().unwrap();
        let cp = dir.path().join("cp");
        save_checkpoint(&cp, 42).unwrap();
        assert_eq!(load_checkpoint(&cp).unwrap(), 42);
        // 覆盖更新
        save_checkpoint(&cp, 100).unwrap();
        assert_eq!(load_checkpoint(&cp).unwrap(), 100);
        // 缺失 → 0
        assert_eq!(load_checkpoint(&dir.path().join("nope")).unwrap(), 0);
    }
}
