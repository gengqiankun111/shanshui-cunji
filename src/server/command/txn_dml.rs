
//! 事务内 DML（server/command/txn_dml.rs）：内容拆分自原 src/db_adapter.rs——txn_insert /
//! txn_update / txn_delete / txn_replace 与辅助（txn_apply_update_one / txn_resolve_where_ids）。

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::server::*;


/// 事务内 INSERT：攒批到事务（commit 时原子应用）。支持多行 VALUES。
/// sysbench 兼容：无 id 列（id=0）→ auto_increment 自动分配。
pub(crate) fn txn_insert(engine: &mut Engine, session: &mut Session, sql: &str) -> QueryResponse {
    let tid = table_id_for(&table_name_of(sql));
    match parse_insert_multi(sql) {
        Ok(Some(rows)) => {
            let txn = session.txn.as_mut().unwrap();
            // P1-1：事务内 IGNORE / ODKU（IGNORE 优先）——逐行冲突处理（忽略 / 转 UPDATE）
            let ignore = is_insert_ignore(sql);
            let odku = if ignore {
                None
            } else {
                parse_insert_odku(sql)
            };
            let auto_rows = rows.iter().filter(|(id, _)| *id == 0).count();
            // a：事务内主键重复校验（1062）——同语句重复 + 快照/同事务可见均拒绝（预校验不落批）。
            // Plain 模式专属；IGNORE/ODKU 按行实时冲突处理。
            if !ignore && odku.is_none() {
                let mut seen = std::collections::HashSet::new();
                for (id, _) in &rows {
                    if *id == 0 {
                        continue;
                    }
                    if !seen.insert(*id) {
                        return QueryResponse::Err(
                            1062,
                            format!("Duplicate entry '{id}' for key 'PRIMARY'"),
                        );
                    }
                    if let Ok(Some(_)) = engine.txn_get(txn, docid_for(tid, *id)) {
                        return QueryResponse::Err(
                            1062,
                            format!("Duplicate entry '{id}' for key 'PRIMARY'"),
                        );
                    }
                }
            }
            let mut last_row = 0u64;
            // P1-1：事务内 IGNORE / ODKU 逐行冲突处理（affected：插入 1、ODKU 更新 2、IGNORE 跳过不计）
            if ignore || odku.is_some() {
                let mut affected = 0u64;
                for (id, doc) in &rows {
                    let docid = if *id == 0 {
                        match next_auto_docid_txn(engine, txn, tid, &session.auto_id) {
                            Ok(d) => d,
                            Err(e) => {
                                if ignore {
                                    continue;
                                }
                                return QueryResponse::Err(1064, format!("auto 分配失败: {e}"));
                            }
                        }
                    } else if *id > ROW_ID_MASK {
                        if ignore {
                            continue;
                        }
                        return QueryResponse::Err(1064, format!("id {id} 超出 48bit row_id 上限"));
                    } else {
                        docid_for(tid, *id)
                    };
                    last_row = row_id_of(docid);
                    let exists = engine.txn_get(txn, docid).ok().flatten();
                    if ignore {
                        if exists.is_some() {
                            continue;
                        }
                        match put_doc_txn(txn, docid, doc) {
                            Ok(()) => affected += 1,
                            Err(_) => {} // 行级错误忽略跳过
                        }
                        continue;
                    }
                    match exists {
                        Some(oldv) => {
                            let mut cur = String::from_utf8_lossy(&oldv).into_owned();
                            let mut err = None;
                            for (col, ex) in odku.as_deref().unwrap_or(&[]) {
                                match odku_apply_set(Some(cur.as_bytes()), doc, col, ex) {
                                    Ok(s) => cur = s,
                                    Err(e) => {
                                        err = Some(e);
                                        break;
                                    }
                                }
                            }
                            if let Some(e) = err {
                                return QueryResponse::Err(1064, format!("odku error: {e}"));
                            }
                            match put_doc_txn(txn, docid, &cur) {
                                Ok(()) => affected += 2,
                                Err(e) => {
                                    return QueryResponse::Err(1064, format!("odku error: {e}"))
                                }
                            }
                        }
                        None => match put_doc_txn(txn, docid, doc) {
                            Ok(()) => affected += 1,
                            Err(e) => return QueryResponse::Err(1064, format!("insert error: {e}")),
                        },
                    }
                }
                return QueryResponse::Ok(affected, last_row);
            }
            // §27 P1：默认表事务内段预分配；§26 M2：非默认表 auto 逐行探测（事务视图）
            let mut auto_cur = if auto_rows > 0 && tid == 0 {
                Some(auto_alloc_block(engine, &session.auto_id, auto_rows as u64))
            } else {
                None
            };
            for (id, doc) in &rows {
                // 引擎 docid = 表区间 + SQL row_id（auto：默认表段取 / 非默认表探测）
                let docid = if *id == 0 {
                    if tid == 0 {
                        let cur = auto_cur.take().unwrap();
                        auto_cur = Some(cur + 1);
                        cur
                    } else {
                        match next_auto_docid_txn(engine, txn, tid, &session.auto_id) {
                            Ok(d) => d,
                            Err(e) => {
                                return QueryResponse::Err(1064, format!("auto 分配失败: {e}"))
                            }
                        }
                    }
                } else {
                    docid_for(tid, *id)
                };
                last_row = row_id_of(docid);
                if let Err(e) = put_doc_txn(txn, docid, doc) {
                    return QueryResponse::Err(1064, format!("insert error: {e}"));
                }
            }
            QueryResponse::Ok(rows.len() as u64, last_row)
        }
        Ok(None) => QueryResponse::Ok(0, 0),
        Err(e) => QueryResponse::Err(1064, format!("insert syntax: {e}")),
    }
}

/// 事务内单行字段更新（id 已解析）：整体替换（field=doc）/ 自增 / 类型化赋值
/// （数字/布尔/对象按 JSON 类型，其余字符串）。
/// 读事务视图（`txn_get`：快照 + 同事务写可见）；不存在 → 空文档（与单 id 旧语义一致）。
pub(crate) fn txn_apply_update_one(
    engine: &Engine,
    txn: &mut crate::txn::Transaction,
    id: u64,
    field: &str,
    expr: &str,
) -> Result<()> {
    // 整体替换（field=doc）→ 直接 put
    if field.eq_ignore_ascii_case("doc") {
        let raw = unquote(expr);
        return put_doc_txn(txn, id, &raw);
    }
    // 读事务视图当前文档（快照 + 同事务写可见；不存在则空对象）
    let mut doc: serde_json::Value = match engine.txn_get(txn, id)? {
        Some(v) => serde_json::from_slice(&v)
            .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new())),
        None => serde_json::Value::Object(serde_json::Map::new()),
    };
    if !doc.is_object() {
        doc = serde_json::Value::Object(serde_json::Map::new());
    }
    let obj = doc.as_object_mut().unwrap();
    if let Some(inc) = parse_increment_expr(field, expr) {
        // 自增：k=k+N → 读当前值 + N（sysbench UPDATE k=k+1）
        let cur = obj.get(field).and_then(|v| v.as_i64()).unwrap_or(0);
        obj.insert(field.to_string(), serde_json::Value::from(cur + inc));
    } else {
        // 类型化赋值（对齐 ODKU / 非事务 P131）：数字/布尔/对象按 JSON 类型，其余字符串——
        // 修复：旧实现无条件写字符串（`SET k=2` → doc.k "2"）→ SUM/AVG 数值聚合（numeric_field
        // 仅认 JSON number）读到 NULL，事务内改后数值聚合与 MySQL 不一致（A1-1 对拍暴露）。
        let raw = unquote(expr);
        let v: serde_json::Value = serde_json::from_str(&raw)
            .unwrap_or_else(|_| serde_json::Value::String(raw));
        obj.insert(field.to_string(), v);
    }
    let new_doc = serde_json::to_string(&doc)
        .map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
    put_doc_txn(txn, id, &new_doc)
}

/// 事务内 WHERE 段 → 作用 docid 集合（d 的 txn 路径扩展，对齐非事务 `resolve_where_ids`）：
/// - `id BETWEEN A AND B` / `docid BETWEEN ...`（可后接 `AND <字段条件>`）：主键闭窗口直解
///   ——窗口内**事务视图存在**的 docid（含同事务自插 Put；空洞/已删行排除，affected 对齐
///   MySQL），再对剩余字段条件逐行 `doc_matches_where` 复检。修复：BETWEEN 误走字段候选
///   路径 + doc 复检（doc JSON 无 `id` 字段 → 主键谓词恒假 → UPDATE/DELETE 窗口恒 0 行），
///   对齐事务 SELECT BETWEEN 窗口读路径（A1-1 探针对拍暴露）。
/// - `id IN (..)` / `docid IN (..)`：目标即 docid（直解，不检查存在性——与单 id 语义一致）
/// - 其余（字段条件 / 复合 and/or）：候选 = 引擎当前视图命中（sqlish）∪ 同事务 write_set，
///   逐候选 `txn_get` 取**事务视图**值 + `doc_matches_where` 谓词复检（快照不可见/已删行
///   排除）——与事务 SELECT 谓词路径（b）同口径。
pub(crate) fn txn_resolve_where_ids(
    engine: &Engine,
    txn: &mut crate::txn::Transaction,
    where_part: &str,
    tid: u16,
) -> Result<Vec<u64>> {
    let w = where_part.trim().trim_end_matches(';').trim();
    let lower = w.to_lowercase();
    // 主键闭窗口直解（前缀判定：`id between` / `docid between` 均为 ASCII 前缀，
    // lower 前段与原文等长安全；边界在数字/关键字上，无引号字符串错位风险）
    let bpre = if lower.starts_with("id between") {
        Some("id between".len())
    } else if lower.starts_with("docid between") {
        Some("docid between".len())
    } else {
        None
    };
    if let Some(plen) = bpre {
        let mut tail = w[plen..].trim_start();
        let a_digits: usize = tail.chars().take_while(|c| c.is_ascii_digit()).count();
        if a_digits == 0 {
            return Err(Error::Cluster("BETWEEN 缺下界数值".into()));
        }
        let a: u64 = tail[..a_digits].parse().map_err(|_| Error::Cluster("BETWEEN 下界非法".into()))?;
        tail = tail[a_digits..].trim_start();
        if !tail.to_lowercase().starts_with("and") {
            return Err(Error::Cluster("BETWEEN 缺 AND".into()));
        }
        tail = tail[3..].trim_start();
        let b_digits: usize = tail.chars().take_while(|c| c.is_ascii_digit()).count();
        if b_digits == 0 {
            return Err(Error::Cluster("BETWEEN 缺上界数值".into()));
        }
        let b: u64 = tail[..b_digits].parse().map_err(|_| Error::Cluster("BETWEEN 上界非法".into()))?;
        // 剩余字段条件（`AND <cond>` 链；引用值原样保真——w 为原文）
        let rest = tail[b_digits..].trim_start();
        let rem = if rest.is_empty() {
            None
        } else if rest.to_lowercase().starts_with("and") {
            Some(rest[3..].trim_start())
        } else {
            // BETWEEN + OR/括号等复杂组合暂不拆解 → 回退字段候选路径（sqlish 引擎当前视图）
            return txn_resolve_where_field(engine, txn, w);
        };
        // 窗口过宽防御（事务内逐 id 事务视图取行）
        if b.saturating_sub(a) > 1_000_000 {
            return Err(Error::Cluster("BETWEEN 窗口过大（上限 100 万行）".into()));
        }
        let cond_sql = rem.map(|r| format!("SELECT docid FROM t WHERE {r}"));
        let mut out = Vec::new();
        for r in a..=b {
            let d = docid_for(tid, r);
            // 事务视图存在性：自删/快照不可见 → None（空洞/已删行不计 affected）
            let Some(v) = engine.txn_get(txn, d)? else { continue };
            if let Some(cs) = &cond_sql {
                if !crate::sqlish::doc_matches_where(cs, &v) {
                    continue;
                }
            }
            out.push(d);
        }
        return Ok(out);
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
    txn_resolve_where_field(engine, txn, w)
}

/// 字段条件 / 复合候选解析（引擎当前视图命中 ∪ 同事务写集 → 事务视图 + 谓词复检）。
fn txn_resolve_where_field(
    engine: &Engine,
    txn: &mut crate::txn::Transaction,
    w: &str,
) -> Result<Vec<u64>> {
    // 字段条件 / 复合：引擎视图命中 ∪ 同事务写集 → 事务视图 + 谓词复检
    let cond_sql = format!("SELECT docid FROM t WHERE {w}");
    let base = crate::sqlish::execute(engine, &cond_sql, 200_000)?;
    let mut set: std::collections::HashSet<u64> = base.into_iter().map(|r| r.0).collect();
    set.extend(txn.write_set().iter().copied());
    let mut ids: Vec<u64> = set.into_iter().collect();
    ids.sort_unstable();
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        match engine.txn_get(txn, id)? {
            Some(v) => {
                if crate::sqlish::doc_matches_where(&cond_sql, &v) {
                    out.push(id);
                }
            }
            None => {}
        }
    }
    Ok(out)
}

/// 事务内 UPDATE：单 id= 或 WHERE id IN / 字段条件（d txn 路径扩展）——攒批覆盖写。
/// 字段级（`SET k=k+1` 自增 / `SET c='str'` 赋值）或整体替换（`SET doc='{json}'`）。
/// 逐目标读事务视图（快照 + 同事务写）→ 修改 → 攒批写回；commit 原子应用。
pub(crate) fn txn_update(engine: &mut Engine, session: &mut Session, sql: &str) -> QueryResponse {
    let tid = table_id_for(&table_name_of(sql));
    // 单 id= 快路径（parse_update 语义不变）；其余（WHERE id IN / 字段条件）走扩展解析
    let (ids, field, expr) = match parse_update(sql) {
        Ok((id, f, e)) => (vec![id], f, e),
        Err(_) => {
            let (wp, f, e) = match parse_update_where(sql) {
                Ok(v) => v,
                Err(err) => return QueryResponse::Err(1064, format!("update syntax: {err}")),
            };
            // §26 M1b：非默认表字段条件 UPDATE 放开——候选限定本表区间（防跨表写）
            let txn = session.txn.as_mut().unwrap();
            let mut ids = match txn_resolve_where_ids(engine, txn, &wp, tid) {
                Ok(v) => v,
                Err(err) => return QueryResponse::Err(1064, format!("update where: {err}")),
            };
            if !wp.trim().to_lowercase().starts_with("id in") {
                ids.retain(|d| ((*d >> 48) as u16) == tid);
            }
            (ids, f, e)
        }
    };
    if ids.is_empty() {
        return QueryResponse::Ok(0, 0); // MySQL：无匹配 → 0 影响
    }
    // §26 M1：汇合点统一幂等译码（单 id/in 已是 docid 或 row，docid_for 幂等）
    let ids: Vec<u64> = ids.into_iter().map(|r| docid_for(tid, r)).collect();
    let txn = session.txn.as_mut().unwrap();
    let mut n = 0u64;
    for id in ids {
        match txn_apply_update_one(engine, txn, id, &field, &expr) {
            Ok(_) => n += 1,
            Err(e) => return QueryResponse::Err(1064, format!("update error: {e}")),
        }
    }
    QueryResponse::Ok(n, 0)
}

/// 事务内 DELETE：攒批删除（d txn 路径扩展：WHERE id IN / 字段条件；单 id= 语义不变）。
pub(crate) fn txn_delete(engine: &mut Engine, session: &mut Session, sql: &str) -> QueryResponse {
    let tid = table_id_for(&table_name_of(sql));
    // P1-2：事务内无 WHERE 的 `DELETE FROM <表>` → 整表删除（快照视图枚举本表可见 docid →
    // 逐条写删，commit 原子；其它表不受影响）
    if !sql.to_lowercase().contains("where") {
        let txn = session.txn.as_mut().unwrap();
        let base = table_base(tid);
        let top = base + ROW_ID_MASK;
        let mut n = 0u64;
        match engine.scan_stream_ids(Some(base), Some(top), |d| {
            txn.delete(d);
            n += 1;
            Ok(true)
        }) {
            Ok(()) => return QueryResponse::Ok(n, 0),
            Err(e) => return QueryResponse::Err(1064, format!("delete all error: {e}")),
        }
    }
    let ids: Vec<u64> = match parse_delete(sql) {
        Ok(id) => vec![id],
        Err(_) => {
            let wp = match parse_delete_where(sql) {
                Ok(v) => v,
                Err(e) => return QueryResponse::Err(1064, format!("delete syntax: {e}")),
            };
            // §26 M1b：非默认表字段条件 DELETE 放开——候选限定本表区间（防跨表删）
            let txn = session.txn.as_mut().unwrap();
            let mut ids = match txn_resolve_where_ids(engine, txn, &wp, tid) {
                Ok(v) => v,
                Err(e) => return QueryResponse::Err(1064, format!("delete where: {e}")),
            };
            if !wp.trim().to_lowercase().starts_with("id in") {
                ids.retain(|d| ((*d >> 48) as u16) == tid);
            }
            ids
        }
    };
    // §26 M1：汇合点统一幂等译码（单 id / id in → docid）
    let ids: Vec<u64> = ids.into_iter().map(|r| docid_for(tid, r)).collect();
    if ids.is_empty() {
        return QueryResponse::Ok(0, 0);
    }
    let txn = session.txn.as_mut().unwrap();
    for id in &ids {
        txn.delete(*id);
    }
    QueryResponse::Ok(ids.len() as u64, 0)
}
/// P0-2：事务内 REPLACE（写集 delete+put 覆盖，commit 原子）。
pub(crate) fn txn_replace(engine: &mut Engine, session: &mut Session, sql: &str) -> QueryResponse {
    let tid = table_id_for(&table_name_of(sql));
    let isql = replace_as_insert(sql);
    match parse_insert_multi(&isql) {
        Ok(Some(rows)) => {
            let txn = session.txn.as_mut().unwrap();
            let auto_rows = rows.iter().filter(|(id, _)| *id == 0).count();
            // §26 M2：REPLACE auto 放开——默认表段预分配 / 非默认表逐行探测（事务视图）
            let mut auto_cur = if auto_rows > 0 && tid == 0 {
                Some(auto_alloc_block(engine, &session.auto_id, auto_rows as u64))
            } else {
                None
            };
            let mut last_row = 0u64;
            for (id, doc) in &rows {
                let docid = if *id == 0 {
                    if tid == 0 {
                        let c = auto_cur.take().unwrap();
                        auto_cur = Some(c + 1);
                        c
                    } else {
                        match next_auto_docid_txn(engine, txn, tid, &session.auto_id) {
                            Ok(d) => d,
                            Err(e) => {
                                return QueryResponse::Err(1064, format!("auto 分配失败: {e}"))
                            }
                        }
                    }
                } else {
                    docid_for(tid, *id)
                };
                last_row = row_id_of(docid);
                // REPLACE：覆盖 put 等效删旧插新（写集覆盖，commit 原子）
                if let Err(e) = put_doc_txn(txn, docid, doc) {
                    return QueryResponse::Err(1064, format!("replace error: {e}"));
                }
            }
            QueryResponse::Ok(rows.len() as u64, last_row)
        }
        Ok(None) => QueryResponse::Ok(0, 0),
        Err(e) => QueryResponse::Err(1064, format!("replace syntax: {e}")),
    }
}
