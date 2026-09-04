
//! SQL 路由 / 解析辅助与写定位（server/sqlparse.rs）：内容拆分自原 src/db_adapter.rs——
//! 多表命名空间与 docid 映射（ROW_ID_MASK / table_id_for / docid_for / row_id_of /
//! route_where_ids / table_name_of）、INSERT/UPDATE/DELETE 简易解析与通用字符串工具
//! （split_values / unquote / find_matching_paren / parse_insert* / odku_apply_set 等）、
//! WHERE 写定位（resolve_where_ids / write_locate_table_ids / parse_pk_between 等）。

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::server::*;


// ============ §26 M1：多表命名空间（docid = table_id<<48 | row_id）============

/// row_id 位宽 48bit：单表 row 容量 2^48，SQL id 显示为 row_id。
pub(crate) const ROW_ID_MASK: u64 = (1 << 48) - 1;

/// 表名 → table_id：`documents`（默认表 = 既有单表库）= 0，零迁移；
/// 其余表 = 确定性 FNV-1a 哈希 & 0xFFFF（免注册表/免持久化，跨连接与重启稳定；
/// 哈希 0 兜底为 1，避免与默认表区间冲突）。碰撞（>~几十张表后概率上升）暂不检测，
/// 后续可升级持久化映射（§26 注）。
pub(crate) fn table_id_for(name: &str) -> u16 {
    let n = name.trim().to_lowercase();
    if n == DEFAULT_TABLE || n.is_empty() {
        return 0;
    }
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in n.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let t = (h & 0xFFFF) as u16;
    if t == 0 {
        1
    } else {
        t
    }
}

/// table_id → docid 区间基址（表数据落在 [base, base + 2^48)）。
pub(crate) fn table_base(tid: u16) -> u64 {
    (tid as u64) << 48
}

/// SQL row_id → 引擎 docid（docid = base | row_id，低 48 位）。
pub(crate) fn docid_for(tid: u16, row: u64) -> u64 {
    table_base(tid) | (row & ROW_ID_MASK)
}

/// 引擎 docid → SQL row_id（输出/显示裁剪）。
pub(crate) fn row_id_of(docid: u64) -> u64 {
    docid & ROW_ID_MASK
}

/// §26 M1：UPDATE/DELETE 的 WHERE 候选 → 本表作用 docid 集。
/// - 主键形态（id=/id IN/docid=/docid IN）：`resolve` 返回 SQL row_id → 升位 docid_for。
/// - 字段形态：`resolve` 经 sqlish 全库返回引擎 docid → 按表区间过滤（防跨表写）。
pub(crate) fn route_where_ids(tid: u16, where_part: &str, ids: Vec<u64>) -> Vec<u64> {
    let w = where_part.trim().to_lowercase();
    let primary = w.starts_with("id=")
        || w.starts_with("docid=")
        || w.starts_with("id in")
        || w.starts_with("docid in");
    if primary {
        ids.into_iter().map(|r| docid_for(tid, r)).collect()
    } else {
        ids.into_iter()
            .filter(|d| ((*d >> 48) as u16) == tid)
            .collect()
    }
}

/// 语句表名提取：INSERT INTO <t> / UPDATE <t> / DELETE FROM <t> / SELECT … FROM <t>；
/// 容忍 `db.` 前缀与反引号；解析失败回退默认表（现行为）。表名不参与 SQL 语义，
/// 仅用于 docid 高位路由。
pub(crate) fn table_name_of(sql: &str) -> String {
    let lower = sql.trim().to_lowercase();
    let rest = if lower.starts_with("insert") {
        // 兼容 `INSERT [IGNORE] INTO …`（P1-1：IGNORE 关键字在 INTO 前）
        let mut body = &lower["insert".len()..];
        body = body.trim_start();
        if let Some(b) = body.strip_prefix("ignore") {
            body = b.trim_start();
        }
        body.strip_prefix("into")
    } else if lower.starts_with("update") {
        lower
            .find("update")
            .map(|p| &lower[p + "update".len()..])
    } else if lower.starts_with("delete") {
        lower
            .find("delete from")
            .map(|p| &lower[p + "delete from".len()..])
    } else if lower.starts_with("select") {
        lower
            .find(" from ")
            .map(|p| &lower[p + " from ".len()..])
    } else if lower.starts_with("drop table") {
        lower
            .find("drop table")
            .map(|p| &lower[p + "drop table".len()..])
    } else if lower.starts_with("truncate table") {
        lower
            .find("truncate table")
            .map(|p| &lower[p + "truncate table".len()..])
    } else {
        None
    };
    let Some(r) = rest else {
        return DEFAULT_TABLE.to_string();
    };
    let tok = r.trim_start().trim_start_matches('`');
    let name: String = tok
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '.' || *c == '`')
        .collect();
    let name = name.trim_matches('`').to_string();
    // 去 `db.` 前缀取表名末段
    name.rsplit('.').next().unwrap_or(DEFAULT_TABLE).to_string()
}

// ============ 简易 SQL 解析（INSERT/UPDATE/DELETE 子集）============

/// 解析 `INSERT INTO [db.]table [(cols)] VALUES (...)` → (id, doc)。
/// 兼容单行；多行 VALUES 用 [`parse_insert_multi`]。
pub(crate) fn parse_insert(sql: &str) -> Result<Option<(u64, String)>> {
    Ok(parse_insert_multi(sql)?.and_then(|v| v.into_iter().next()))
}

/// 解析多行 `INSERT ... VALUES (..),(..),...` → 全部 (id, doc) 组。
/// 支持 sysbench `--insert-multiple-rows`（H-6 扩展：一次语句批量入库）。
pub(crate) fn parse_insert_multi(sql: &str) -> Result<Option<Vec<(u64, String)>>> {
    let lower = sql.to_lowercase();
    let values_pos = lower.find("values").ok_or_else(|| {
        Error::Cluster("INSERT 缺 VALUES".into())
    })?;
    let values = &sql[values_pos + 6..];
    // 列清单（可选项，整条语句共享）
    let cols_part = &sql[..values_pos];
    let has_cols = cols_part.contains('(');
    let cols: Vec<String> = if has_cols {
        let cols_open = cols_part.find('(').unwrap();
        let cols_close = cols_part.rfind(')').unwrap();
        split_values(&cols_part[cols_open + 1..cols_close])
    } else {
        Vec::new()
    };
    let mut rows = Vec::new();
    let mut rest = values.trim_start();
    // 逐组解析 `(...)`（组间逗号分隔，容忍结尾分号）
    while let Some(open) = rest.find('(') {
        let close = find_matching_paren(rest, open)?;
        let inner = &rest[open + 1..close];
        let parts = split_values(inner);
        if parts.is_empty() {
            break;
        }
        if has_cols {
            let mut idv: Option<String> = None;
            let mut docv: Option<String> = None;
            // H-6（sysbench 兼容）：非 id/doc 列组装为 JSON 文档 {"列名":值}
            let mut extra: Vec<(String, String)> = Vec::new();
            for (i, c) in cols.iter().enumerate() {
                let name = c.trim().to_lowercase();
                match name.as_str() {
                    "id" | "docid" => idv = parts.get(i).cloned(),
                    "doc" | "value" => docv = parts.get(i).cloned(),
                    _ => {
                        if let Some(v) = parts.get(i) {
                            extra.push((c.trim().to_string(), unquote(v)));
                        }
                    }
                }
            }
            // sysbench 兼容：无 id 列（auto_increment 语义）→ id=0 由调用方自动分配；
            // 显式 NULL（`INSERT INTO t VALUES (NULL, ...)`）= auto 同义（P0-1）
            let id = match idv {
                Some(v) => {
                    let t = v.trim();
                    let low = t.to_lowercase();
                    if low == "null" || t.is_empty() {
                        0u64
                    } else {
                        t.parse::<u64>()
                            .map_err(|_| Error::Cluster("id 非法".into()))?
                    }
                }
                None => 0u64,
            };
            let doc = match docv {
                Some(d) => unquote(&d),
                None => {
                    // 组装 JSON（数字/布尔按 JSON 类型，其余字符串）
                    let mut obj = serde_json::Map::new();
                    for (k, v) in extra {
                        let val: serde_json::Value = serde_json::from_str(&v)
                            .unwrap_or_else(|_| serde_json::Value::String(v));
                        obj.insert(k, val);
                    }
                    serde_json::Value::Object(obj).to_string()
                }
            };
            rows.push((id, doc));
        } else {
            // 无列名形态：仅 (id, doc) 双值（多业务列须显式列名，文档库无列 schema）
            let id = {
                let t = parts[0].trim();
                let low = t.to_lowercase();
                if low == "null" || t.is_empty() {
                    0u64 // `VALUES (NULL, '...')` → auto（P0-1）
                } else {
                    t.parse::<u64>()
                        .map_err(|_| Error::Cluster("id 非法".into()))?
                }
            };
            let doc = unquote(parts.get(1).cloned().unwrap_or_default().as_str());
            rows.push((id, doc));
        }
        rest = rest[close + 1..].trim_start();
        // 跳过组间逗号（容忍 `),(` 与 `) , (` 空格变体）
        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
        }
        // 下一个组必须以 `(` 开头；结尾 `;` / 空白 / 注释则终止
        if !rest.starts_with('(') {
            break;
        }
    }
    if rows.is_empty() {
        return Ok(None);
    }
    Ok(Some(rows))
}

// ---------- P1-1：INSERT IGNORE / INSERT … ON DUPLICATE KEY UPDATE ----------

/// `INSERT [IGNORE]` 判定（MySQL：IGNORE = 重复/错误行降级跳过，不报 1062）。
pub(crate) fn is_insert_ignore(sql: &str) -> bool {
    let s = sql.trim();
    let after = s.get("insert".len()..).unwrap_or("");
    after.trim_start()
        .get(.."ignore".len())
        .map(|t| t.eq_ignore_ascii_case("ignore"))
        .unwrap_or(false)
}

/// 解析 `ON DUPLICATE KEY UPDATE col=expr[, col=expr…]` → 赋值列表；无该子句 → None。
/// 赋值项顶层逗号切分（引号感知，复用 split_values）。
pub(crate) fn parse_insert_odku(sql: &str) -> Option<Vec<(String, String)>> {
    let lower = sql.trim().to_lowercase();
    let kw = "on duplicate key update";
    let p = lower.find(kw)?;
    let tail = sql[p + kw.len()..].trim().trim_end_matches(';').trim();
    let mut out = Vec::new();
    for item in split_values(tail) {
        let eq = item.find('=')?;
        let col = item[..eq].trim().to_string();
        let expr = item[eq + 1..].trim().to_string();
        if !col.is_empty() && !expr.is_empty() {
            out.push((col, expr));
        }
    }
    Some(out)
}

/// ODKU 单赋值应用：作用于当前文档 `prev`（None = 空对象；文档本体即整 JSON，业务列即 JSON 字段）。
/// 支持子集（对齐本引擎 UPDATE 能力）：
/// - `doc=<整 doc>|VALUES(doc)` → **整文档覆盖**（文档列 doc/value 直指文档本体）；
/// - `id/docid=…` → 主键不可更新，忽略（MySQL：冲突即主键重复，不改主键）；
/// - `col=VALUES(col)` → 从插入 doc 取同名字段值覆盖旧 doc 该字段；
/// - `col=col+N` → 数值自增；
/// - 其余 `col=<字面量>` → JSON 类型化赋值（数字/布尔/对象按 JSON，字符串去引号）。
/// 返回应用后的整文档字符串。
pub(crate) fn odku_apply_set(
    prev: Option<&[u8]>,
    insert_doc: &str,
    col: &str,
    expr: &str,
) -> Result<String> {
    let col = col.trim();
    // 文档本体整体覆盖
    if col.eq_ignore_ascii_case("doc") || col.eq_ignore_ascii_case("value") {
        let e = expr.trim();
        let eu = e.to_uppercase();
        let whole = if eu.starts_with("VALUES(") {
            insert_doc.to_string()
        } else {
            unquote(e)
        };
        return Ok(whole);
    }
    // 主键列不可更新
    if col.eq_ignore_ascii_case("id") || col.eq_ignore_ascii_case("docid") {
        return Ok(String::from_utf8_lossy(prev.unwrap_or(b"{}")).into_owned());
    }
    let mut doc: serde_json::Value = prev
        .and_then(|v| serde_json::from_slice(v).ok())
        .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
    if !doc.is_object() {
        doc = serde_json::Value::Object(serde_json::Map::new());
    }
    let obj = doc.as_object_mut().unwrap();
    let e = expr.trim();
    let eu = e.to_uppercase();
    if eu.starts_with("VALUES(") {
        // 从插入 doc 同名取值（无列名插入 doc = 整 JSON，字段取 key 需先解析）
        let open = e.find('(').unwrap_or(0);
        let inner_col = e[open + 1..]
            .trim_end_matches(')')
            .trim()
            .trim_matches(|c| c == '\'' || c == '"')
            .to_string();
        let ins: serde_json::Value = serde_json::from_str(insert_doc)
            .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new()));
        let v = ins.get(&inner_col).cloned().unwrap_or(serde_json::Value::Null);
        obj.insert(col.to_string(), v);
    } else if let Some(n) = parse_increment_expr(col, e) {
        let cur = obj.get(col).and_then(|x| x.as_i64()).unwrap_or(0);
        obj.insert(col.to_string(), serde_json::Value::from(cur + n));
    } else {
        let raw = unquote(e);
        let v: serde_json::Value = serde_json::from_str(&raw)
            .unwrap_or_else(|_| serde_json::Value::String(raw));
        obj.insert(col.to_string(), v);
    }
    serde_json::to_string(&doc).map_err(|e| crate::error::Error::Serialize(e.to_string()))
}

/// 解析 `UPDATE documents SET field=expr WHERE id=1` → (id, field, expr)。
/// 注：事务内 UPDATE 仍为单点语义（txn_update 调用）；非事务路径用 parse_update_where。
pub(crate) fn parse_update(sql: &str) -> Result<(u64, String, String)> {
    let lower = sql.to_lowercase();
    let set_pos = lower.find("set").ok_or_else(|| Error::Cluster("UPDATE 缺 SET".into()))?;
    let where_pos = lower.find("where").ok_or_else(|| {
        Error::Cluster("UPDATE 缺 WHERE id=...".into())
    })?;
    let set_part = &sql[set_pos + 3..where_pos];
    let where_part = &sql[where_pos + 5..];
    let eq = set_part.find('=').ok_or_else(|| Error::Cluster("SET 缺 =".into()))?;
    let field = set_part[..eq].trim().to_string();
    let expr = set_part[eq + 1..].trim().to_string();
    if field.is_empty() || expr.is_empty() {
        return Err(Error::Cluster("SET 字段/值为空".into()));
    }
    // WHERE id=N
    let id = parse_where_id(where_part)?;
    Ok((id, field, expr))
}

/// 解析自增表达式 `field=field+N` → Some(N)；否则（字符串赋值等）→ None。
pub(crate) fn parse_increment_expr(field: &str, expr: &str) -> Option<i64> {
    let e = expr.trim();
    let (f, num) = e.split_once('+')?;
    if !f.trim().eq_ignore_ascii_case(field) {
        return None;
    }
    num.trim().parse::<i64>().ok()
}

/// 解析 `DELETE FROM documents WHERE id=1` → id（事务内 DELETE 单点路径调用；
/// 非事务路径用 parse_delete_where）。
pub(crate) fn parse_delete(sql: &str) -> Result<u64> {
    let lower = sql.to_lowercase();
    let where_pos = lower.find("where").ok_or_else(|| {
        Error::Cluster("DELETE 缺 WHERE id=...".into())
    })?;
    parse_where_id(&sql[where_pos + 5..])
}

/// 提取 `UPDATE … SET field=expr WHERE <cond>` 的 (WHERE 段, field, expr)。
pub(crate) fn parse_update_where(sql: &str) -> Result<(String, String, String)> {
    let lower = sql.to_lowercase();
    let set_pos = lower.find("set").ok_or_else(|| Error::Cluster("UPDATE 缺 SET".into()))?;
    let where_pos = lower
        .find("where")
        .ok_or_else(|| Error::Cluster("UPDATE 缺 WHERE".into()))?;
    let set_part = &sql[set_pos + 3..where_pos];
    let eq = set_part.find('=').ok_or_else(|| Error::Cluster("SET 缺 =".into()))?;
    let field = set_part[..eq].trim().to_string();
    let expr = set_part[eq + 1..].trim().to_string();
    if field.is_empty() || expr.is_empty() {
        return Err(Error::Cluster("SET 字段/值为空".into()));
    }
    Ok((sql[where_pos + 5..].trim().to_string(), field, expr))
}

/// 提取 `DELETE FROM … WHERE <cond>` 的 WHERE 段。
pub(crate) fn parse_delete_where(sql: &str) -> Result<String> {
    let lower = sql.to_lowercase();
    let where_pos = lower
        .find("where")
        .ok_or_else(|| Error::Cluster("DELETE 缺 WHERE".into()))?;
    Ok(sql[where_pos + 5..].trim().to_string())
}

/// WHERE 段 → 命中 docid 列表：
/// - `id = N` → [N]（保持既有单 id 语义，不检查存在性）
/// - `id IN (a, b)` / `docid IN (...)` → 数值集合
/// - 其余字段条件（等值 / f IN / BETWEEN …）→ 经 sqlish 查询**已存在**文档的 docid
///   （无匹配 → 空，UPDATE/DELETE 影响 0 行，对齐 MySQL）。
pub(crate) fn resolve_where_ids(engine: &mut Engine, where_part: &str) -> Result<Vec<u64>> {
    let w = where_part.trim().trim_end_matches(';').trim();
    let lower = w.to_lowercase();
    if let Some(rest) = lower.strip_prefix("id") {
        let after = rest.trim_start();
        if let Some(eq) = after.strip_prefix('=') {
            let num: String = eq.trim().chars().take_while(|c| c.is_ascii_digit()).collect();
            if !num.is_empty() {
                return Ok(vec![num.parse().map_err(|_| Error::Cluster("id 非法".into()))?]);
            }
        }
    }
    if lower.starts_with("id in") || lower.starts_with("docid in") {
        let open = w
            .find('(')
            .ok_or_else(|| Error::Cluster("id IN 缺 (".into()))?;
        let close = w.rfind(')').unwrap_or(w.len());
        let mut ids = Vec::new();
        for p in split_values(&w[open + 1..close]) {
            let t = p.trim().trim_matches(|c| c == '\'' || c == '"');
            ids.push(t.parse::<u64>().map_err(|_| Error::Cluster("id IN 数值非法".into()))?);
        }
        return Ok(ids);
    }
    // 其余条件 → 字段/复合 → P88：WHERE 段 → WhereExpr → get_docid_set 全阶梯收敛
    // （倒排位图 / AND 快路径 / LIKE / 组合条件同 SELECT；**limit=None 写定位不截断**，
    // 修复旧路径 sqlish execute cap=200_000 时 >20 万匹配被静默截断漏行）。
    let we = crate::sqlish::parse_where_expr(w)?;
    let guard = engine.query_guard();
    let set = crate::sqlish::get_docid_set(&*engine, Some(&we), None, &guard)?;
    Ok(set.iter().collect())
}

/// WHERE 段是否主键形态（`id=`/`docid=`/`id IN`/`docid IN`）——与 `route_where_ids`
/// 的判定一致（主键形态返回 SQL row_id，字段形态返回引擎 docid）。
pub(crate) fn where_is_primary_key(where_part: &str) -> bool {
    let w = where_part.trim().to_lowercase();
    w.starts_with("id=")
        || w.starts_with("docid=")
        || w.starts_with("id in")
        || w.starts_with("docid in")
}

/// P88：字段/复合条件写定位——WHERE → WhereExpr → `get_docid_set`（写定位 limit=None
/// 全收敛不截断）→ 本表 docid（升序）。倒排/组合索引/范围条件收敛路径与同条件 SELECT
/// 一致（MySQL server 层聚合/分组已按表区间执行；此处按 tid 过滤防跨表写，D4）。
pub(crate) fn write_locate_table_ids(engine: &mut Engine, tid: u16, where_part: &str) -> Result<Vec<u64>> {
    let w = where_part.trim().trim_end_matches(';').trim();
    let we = crate::sqlish::parse_where_expr(w)?;
    let guard = engine.query_guard();
    let set = crate::sqlish::get_docid_set(&*engine, Some(&we), None, &guard)?;
    Ok(set
        .iter()
        .filter(|d| ((*d >> 48) as u16) == tid)
        .collect())
}

/// 解析 `id = 123`（WHERE 子句内；事务内单点路径 parse_update/parse_delete 调用）。
pub(crate) fn parse_where_id(where_part: &str) -> Result<u64> {
    let eq = where_part
        .find('=')
        .ok_or_else(|| Error::Cluster("WHERE 缺 =".into()))?;
    let v: u64 = where_part[eq + 1..]
        .trim()
        .trim_end_matches(';')
        .parse()
        .map_err(|_| Error::Cluster("WHERE id 非法".into()))?;
    Ok(v)
}

/// 按逗号切分（忽略引号内逗号）。
pub(crate) fn split_values(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut in_str = false;
    let mut quote = ' ';
    for c in s.chars() {
        if in_str {
            cur.push(c);
            if c == quote {
                in_str = false;
            }
        } else if c == '\'' || c == '"' {
            in_str = true;
            quote = c;
            cur.push(c);
        } else if c == ',' {
            parts.push(cur.clone());
            cur.clear();
        } else {
            cur.push(c);
        }
    }
    if !cur.trim().is_empty() || !parts.is_empty() {
        parts.push(cur);
    }
    parts
}

/// 去掉字符串包裹引号。
pub(crate) fn unquote(s: &str) -> String {
    let t = s.trim();
    let body = if t.len() >= 2
        && ((t.starts_with('\'') && t.ends_with('\'')) || (t.starts_with('"') && t.ends_with('"')))
    {
        &t[1..t.len() - 1]
    } else {
        t
    };
    // SQL 反转义（H 项遗留缺陷修复）：客户端参数化/转义把 `"` `\` 等写成 `\"` `\\`，
    // 不还原则 JSON 文档解析失败（pymysql 参数化实测 `{\"v\":1}` → serde 报 key 非法）。
    let mut out = String::with_capacity(body.len());
    let mut chars = body.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\'') => out.push('\''),
                Some('"') => out.push('"'),
                Some('\\') => out.push('\\'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('t') => out.push('\t'),
                Some('0') => out.push('\0'),
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
    out
}

/// 找配对右括号（含引号感知）。
pub(crate) fn find_matching_paren(s: &str, open: usize) -> Result<usize> {
    let mut depth = 0usize;
    let mut in_str = false;
    let mut quote = ' ';
    for (i, c) in s.char_indices().skip(open) {
        if in_str {
            if c == quote {
                in_str = false;
            }
            continue;
        }
        match c {
            '\'' | '"' => {
                in_str = true;
                quote = c;
            }
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i);
                }
            }
            _ => {}
        }
    }
    Err(Error::Cluster("括号不配对".into()))
}
