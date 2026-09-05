
//! 非事务 DML 与文档写辅助（server/command/dml.rs）：内容拆分自原 src/db_adapter.rs——
//! INSERT / REPLACE / UPDATE / DELETE 响应（insert_response / replace_response /
//! update_response / delete_response）、主键区间删除（parse_pk_between / delete_pk_range）、
//! auto docid 分配（auto_alloc_block / next_auto_docid*）与 put 辅助（put_doc / doc_terms）。

use std::sync::atomic::{AtomicU64, Ordering};

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::multitable::drop_table_range;
use crate::server::*;


/// INSERT INTO <t> (id, doc) VALUES …——§26 M1：按语句表名路由 docid（docid = table_id<<48 | row）。
/// sysbench 兼容：无 id 列（id=0）→ auto_increment（当前仅默认表 documents 支持，其余表 M2）。
pub(crate) fn insert_response(
    engine: &mut Engine,
    sql: &str,
    auto_id: &AtomicU64,
) -> QueryResponse {
    let tid = table_id_for(&table_name_of(sql));
    match parse_insert_multi(sql) {
        Ok(Some(rows)) => {
            // P1-1：INSERT IGNORE / ON DUPLICATE KEY UPDATE（IGNORE 优先——冲突行跳过）
            let ignore = is_insert_ignore(sql);
            let odku = if ignore {
                None
            } else {
                parse_insert_odku(sql)
            };
            let auto_rows = rows.iter().filter(|(id, _)| *id == 0).count();
            // a：主键重复校验（MySQL 1062）——同语句重复 + 库中已存在均拒绝（按表 docid），
            // 预校验保证多行 VALUES 语句级失败不产生部分写入。Plain 模式专属：
            // IGNORE/ODKU 按行实时冲突处理（忽略 / 转 UPDATE）。
            if !ignore && odku.is_none() {
                let mut seen = std::collections::HashSet::new();
                for (id, _) in &rows {
                    if *id == 0 {
                        continue; // auto 分配不会重复
                    }
                    if *id > ROW_ID_MASK {
                        return QueryResponse::Err(
                            1064,
                            format!("id {id} 超出 48bit row_id 上限"),
                        );
                    }
                    if !seen.insert(*id) {
                        return QueryResponse::Err(
                            1062,
                            format!("Duplicate entry '{id}' for key 'PRIMARY'"),
                        );
                    }
                    if let Ok(Some(_)) = engine.get(docid_for(tid, *id)) {
                        return QueryResponse::Err(
                            1062,
                            format!("Duplicate entry '{id}' for key 'PRIMARY'"),
                        );
                    }
                }
            }
            // H-6 扩展：多行 VALUES 批量入库（逐行 put，事务外；行数作为 affected）
            // §27 P1：默认表语句级 auto 段预分配；§26 M2：非默认表 auto 逐行探测
            // IGNORE/ODKU 模式逐行探测（冲突跳行/更新，段预取会浪费被跳过行的 id）
            let mut auto_cur = if auto_rows > 0 && tid == 0 && !ignore && odku.is_none() {
                Some(auto_alloc_block(engine, auto_id, auto_rows as u64))
            } else {
                None
            };
            let mut last_row = 0u64;
            // P1-1：IGNORE / ODKU 逐行冲突处理（affected：插入 1、ODKU 更新 2、IGNORE 跳过不计）
            if ignore || odku.is_some() {
                let mut affected = 0u64;
                for (id, doc) in &rows {
                    let docid = if *id == 0 {
                        match next_auto_docid(engine, tid, auto_id) {
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
                    let exists = engine.get(docid).ok().flatten();
                    if ignore {
                        // 冲突 / 写入错误均降级跳过（MySQL IGNORE 语义）
                        if exists.is_some() {
                            continue;
                        }
                        match put_doc(engine, docid, doc) {
                            Ok(()) => affected += 1,
                            Err(_) => {} // 行级错误（如坏 JSON）忽略跳过
                        }
                        continue;
                    }
                    // ODKU
                    match exists {
                        Some(oldv) => {
                            // 冲突行：按赋值列表逐项更新（多赋值顺序应用）
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
                            match put_doc(engine, docid, &cur) {
                                Ok(()) => affected += 2,
                                Err(e) => {
                                    return QueryResponse::Err(1064, format!("odku error: {e}"))
                                }
                            }
                        }
                        None => match put_doc(engine, docid, doc) {
                            Ok(()) => affected += 1,
                            Err(e) => return QueryResponse::Err(1064, format!("insert error: {e}")),
                        },
                    }
                }
                return QueryResponse::Ok(affected, last_row);
            }
            for (id, doc) in &rows {
                // 引擎 docid = 表区间 + SQL row_id（auto：默认表段取 / 非默认表探测）
                let docid = if *id == 0 {
                    if tid == 0 {
                        let cur = auto_cur.take().unwrap();
                        auto_cur = Some(cur + 1);
                        cur
                    } else {
                        match next_auto_docid(engine, tid, auto_id) {
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
                if let Err(e) = put_doc(engine, docid, doc) {
                    return QueryResponse::Err(1064, format!("insert error: {e}"));
                }
            }
            let n = rows.len() as u64;
            QueryResponse::Ok(n, last_row)
        }
        Ok(None) => QueryResponse::Ok(0, 0),
        Err(e) => QueryResponse::Err(1064, format!("insert syntax: {e}")),
    }
}

/// REPLACE INTO <t> → INSERT INTO 前缀（parse_insert_multi 只认 INSERT 关键字）。
pub(crate) fn replace_as_insert(sql: &str) -> String {
    let s = sql.trim();
    let sp = s.find(' ').unwrap_or(s.len());
    format!("INSERT{}", &s[sp..])
}

/// P0-2：REPLACE INTO = 覆盖写（显式 id 已存在 → 先删后插；不存在 → 直接插），
/// 对齐 MySQL REPLACE 语义；非事务（写锁内逐行 delete+put 近似原子）。
pub(crate) fn replace_response(engine: &mut Engine, sql: &str, auto_id: &AtomicU64) -> QueryResponse {
    let tid = table_id_for(&table_name_of(sql));
    let isql = replace_as_insert(sql);
    match parse_insert_multi(&isql) {
        Ok(Some(rows)) => {
            let auto_rows = rows.iter().filter(|(id, _)| *id == 0).count();
            // §26 M2：REPLACE auto 放开——默认表段预分配 / 非默认表逐行探测
            let mut auto_cur = if auto_rows > 0 && tid == 0 {
                Some(auto_alloc_block(engine, auto_id, auto_rows as u64))
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
                        match next_auto_docid(engine, tid, auto_id) {
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
                // REPLACE：覆盖 put 已等效"删旧插新"（同 docid 覆盖整 doc，倒排由 put 重建；
                // 历史版本保留于版本链，读路径取最新——缺陷 B 语义自洽）
                if let Err(e) = put_doc(engine, docid, doc) {
                    return QueryResponse::Err(1064, format!("replace error: {e}"));
                }
            }
            QueryResponse::Ok(rows.len() as u64, last_row)
        }
        Ok(None) => QueryResponse::Ok(0, 0),
        Err(e) => QueryResponse::Err(1064, format!("replace syntax: {e}")),
    }
}
/// §27 P0/P1：auto docid 段预分配——一次申请 `n` 个连续 id 的块（返回块起点，
/// 使用 [start, start+n)），把逐行 2×原子降为语句级 1 次（大 INSERT/批量导入受益）。
/// 起点 ≥ 引擎已写入最大 docid + 1（`Engine::auto_watermark`，重启续接不撞已提交行）；
/// 并发安全：`fetch_max(水位)` 抬底后 `fetch_add(n)` 原子取段，多连接窗口不重叠。
pub(crate) fn auto_alloc_block(engine: &Engine, auto_id: &AtomicU64, n: u64) -> u64 {
    let wm = engine.auto_watermark(); // 已写入最大 docid + 1（空库 = 1）
    auto_id.fetch_max(wm, Ordering::Relaxed);
    auto_id.fetch_add(n, Ordering::Relaxed)
}

/// §26 M2：非默认表 auto row_id 探测分配——全局自增计数低位作 row（docid = base | 低位，
/// 该表区间唯一）；与显式 id 冲突（docid 已存在）即跳号。默认表不经过（段预分配不截断高位）。
pub(crate) fn next_auto_docid(engine: &Engine, tid: u16, auto_id: &AtomicU64) -> Result<u64> {
    for _ in 0..1_000_000 {
        let d = auto_id.fetch_add(1, Ordering::Relaxed);
        let docid = docid_for(tid, d);
        if let Ok(None) = engine.get(docid) {
            return Ok(docid);
        }
    }
    Err(Error::Cluster("auto 空间耗尽（冲突过多）".into()))
}

/// 事务版：按事务视图（快照 + 写集）探测空位（落写集由调用方 put，commit 原子）。
pub(crate) fn next_auto_docid_txn(
    engine: &Engine,
    txn: &mut crate::txn::Transaction,
    tid: u16,
    auto_id: &AtomicU64,
) -> Result<u64> {
    for _ in 0..1_000_000 {
        let d = auto_id.fetch_add(1, Ordering::Relaxed);
        let docid = docid_for(tid, d);
        if let Ok(None) = engine.txn_get(txn, docid) {
            return Ok(docid);
        }
    }
    Err(Error::Cluster("auto 空间耗尽（冲突过多）".into()))
}
/// UPDATE documents SET field=expr WHERE id=1（非事务：字段级 / 整体替换）。
pub(crate) fn update_response(engine: &mut Engine, sql: &str) -> QueryResponse {
    match parse_update_where(sql) {
        Ok((where_part, field, expr)) => {
            // §26 M1：SQL row_id / 字段候选 → 本表 docid
            let tid = table_id_for(&table_name_of(sql));
            // P127：剥离 `UPDATE ... LIMIT n`（只更新前 n 命中行，对齐 MySQL；定位早停）
            let (cond, upd_limit) = strip_where_limit(&where_part);
            // P88：写定位——主键形态（id=/docid=/id IN）保持 resolve+route（row → docid）；
            // 字段/复合条件 → P127 组合主键区间收敛（id BETWEEN∩等值 → 区间 keys-only∩位图，
            // LIMIT 早停）或 WhereExpr → get_docid_set（limit 截断；无 LIMIT 不截断防漏行）
            let ids = if where_is_primary_key(&cond) {
                match resolve_where_ids(engine, &cond).map(|v| route_where_ids(tid, &cond, v)) {
                    Ok(v) => {
                        if let Some(l) = upd_limit {
                            let mut v = v;
                            v.truncate(l as usize);
                            v
                        } else {
                            v
                        }
                    }
                    Err(e) => return QueryResponse::Err(1064, format!("update where: {e}")),
                }
            } else {
                match crate::sqlish::parse_where_expr(&cond)
                    .and_then(|we| match extract_pk_between_comb(&we) {
                        Some((lo, hi, rest)) => locate_pk_range_converged(
                            engine,
                            tid,
                            lo,
                            hi,
                            rest.as_ref(),
                            upd_limit,
                        ),
                        None => {
                            // 无主键区间：通用求值收敛；UPDATE ... LIMIT → 求值限早停/截断
                            let guard = engine.query_guard();
                            let set = crate::sqlish::get_docid_set(
                                &*engine,
                                Some(&we),
                                upd_limit,
                                &guard,
                            )?;
                            Ok(set
                                .iter()
                                .filter(|d| ((*d >> 48) as u16) == tid)
                                .collect())
                        }
                    })
                {
                    Ok(v) => v,
                    Err(e) => return QueryResponse::Err(1064, format!("update where: {e}")),
                }
            };
            if ids.is_empty() {
                return QueryResponse::Ok(0, 0); // MySQL：无匹配行 → 0 影响
            }
            // P89：UPDATE 批量管道——分批 batch_get(1000) 取现值 → 逐行变换 →
            // put_batch 攒批提交（倒排/组合索引/位图随 put_nosync 同步，批尾单次 flush_wal），
            // 替代旧逐行 engine.get + engine.put（每行独立 WAL 提交/看门狗/热缓存开销）。
            let mut n = 0u64;
            for chunk in ids.chunks(1000) {
                let cur = match engine.batch_get(chunk) {
                    Ok(v) => v,
                    Err(e) => return QueryResponse::Err(1064, format!("update error: {e}")),
                };
                let mut items: Vec<(u64, Vec<u8>, Vec<String>)> = Vec::with_capacity(chunk.len());
                for (&id, cur_opt) in chunk.iter().zip(cur.into_iter()) {
                    // 整体替换（field=doc）
                    if field.eq_ignore_ascii_case("doc") {
                        let raw = unquote(&expr);
                        let terms = match doc_terms(&raw) {
                            Ok(t) => t,
                            Err(e) => return QueryResponse::Err(1064, format!("update error: {e}")),
                        };
                        items.push((id, raw.into_bytes(), terms));
                        continue;
                    }
                    // 读当前文档 → 字段级修改 → 覆盖写回（缺失/删除 id 视为空文档：与旧单 id 语义一致）
                    let mut doc: serde_json::Value = match cur_opt {
                        Some(v) => serde_json::from_slice(&v)
                            .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new())),
                        None => serde_json::Value::Object(serde_json::Map::new()),
                    };
                    if !doc.is_object() {
                        doc = serde_json::Value::Object(serde_json::Map::new());
                    }
                    let obj = doc.as_object_mut().unwrap();
                    if let Some(inc) = parse_increment_expr(&field, &expr) {
                        let cur_v = obj.get(&field).and_then(|v| v.as_i64()).unwrap_or(0);
                        obj.insert(field.clone(), serde_json::Value::from(cur_v + inc));
                    } else {
                        obj.insert(field.clone(), serde_json::Value::String(unquote(&expr)));
                    }
                    let new_doc = serde_json::to_string(&doc).unwrap_or_default();
                    let terms = match doc_terms(&new_doc) {
                        Ok(t) => t,
                        Err(e) => return QueryResponse::Err(1064, format!("update error: {e}")),
                    };
                    items.push((id, new_doc.into_bytes(), terms));
                }
                match engine.put_batch(&items) {
                    Ok(()) => n += items.len() as u64,
                    Err(e) => return QueryResponse::Err(1064, format!("update error: {e}")),
                }
            }
            QueryResponse::Ok(n, 0)
        }
        Err(e) => QueryResponse::Err(1064, format!("update syntax: {e}")),
    }
}

/// DELETE FROM documents WHERE id=1 / id IN (...) / <字段条件> / （P1-2）无 WHERE = 整表删除。
pub(crate) fn delete_response(engine: &mut Engine, sql: &str) -> QueryResponse {
    // P1-2：无 WHERE 的 `DELETE FROM <表>` → 整表删除（本表 docid 区间逐行删 + 表文件回收，
    // 默认表 documents 同区间语义——不动其它表；对齐 MySQL DELETE 全表 = 逐行删除）
    if !sql.to_lowercase().contains("where") {
        let tid = table_id_for(&table_name_of(sql));
        return match drop_table_range(engine, tid) {
            Ok(n) => QueryResponse::Ok(n as u64, 0),
            Err(e) => QueryResponse::Err(1064, format!("delete all error: {e}")),
        };
    }
    match parse_delete_where(sql) {
        Ok(where_part) => {
            // §26 M1：SQL row_id / 字段候选 → 本表 docid
            let tid = table_id_for(&table_name_of(sql));
            // B2（delete_range50 修复）：主键闭区间 `id BETWEEN a AND b` / `docid BETWEEN a AND b`
            // → **区间 keys-only 扫描**产出 docid → `delete_batch` 批量删（单次提交）。
            // 替代旧路径：resolve_where_ids 落 sqlish `SELECT docid` 全扫物化 Vec（cap 200_000）
            // + 逐行 engine.delete（每行独立 fsync/watchdog/delta 清理 → 50 行区间慢 6729×）。
            if let Some((lo, hi)) = parse_pk_between(&where_part) {
                return delete_pk_range(engine, tid, lo, hi);
            }
            // P88：写定位——主键形态（id=/docid=/id IN）保持 resolve+route（row → docid）；
            // 字段/复合条件 → WhereExpr → get_docid_set（limit=None 不截断）→ **流式**
            // delete_batch（免全量 Vec 物化；超大命中集 chunk 消费，防物化爆内存）。
            if where_is_primary_key(&where_part) {
                let ids = match resolve_where_ids(engine, &where_part)
                    .map(|v| route_where_ids(tid, &where_part, v))
                {
                    Ok(v) => v,
                    Err(e) => return QueryResponse::Err(1064, format!("delete where: {e}")),
                };
                if ids.is_empty() {
                    return QueryResponse::Ok(0, 0);
                }
                match engine.delete_batch(ids.into_iter()) {
                    Ok(n) => QueryResponse::Ok(n, 0),
                    Err(e) => QueryResponse::Err(1064, format!("delete error: {e}")),
                }
            } else {
                // P127：剥离 DELETE ... LIMIT n（只删前 n 命中行）
                let (cond, del_limit) = strip_where_limit(&where_part);
                let we = match crate::sqlish::parse_where_expr(&cond) {
                    Ok(we) => we,
                    Err(e) => return QueryResponse::Err(1064, format!("delete where: {e}")),
                };
                let guard = engine.query_guard();
                let ids: Vec<u64> = match extract_pk_between_comb(&we) {
                    // P127 组合主键区间：keys-only 区间 ∩ 其余条件位图，LIMIT 早停
                    // （替代 get_docid_set 全候选遍历——组合 WHERE 定位收敛核心）
                    Some((lo, hi, rest)) => {
                        match locate_pk_range_converged(engine, tid, lo, hi, rest.as_ref(), del_limit)
                        {
                            Ok(v) => v,
                            Err(e) => return QueryResponse::Err(1064, format!("delete where: {e}")),
                        }
                    }
                    None => {
                        // 通用求值（LIMIT → 求值早停/截断）
                        let set =
                            match crate::sqlish::get_docid_set(&*engine, Some(&we), del_limit, &guard)
                            {
                                Ok(s) => s,
                                Err(e) => {
                                    return QueryResponse::Err(1064, format!("delete where: {e}"))
                                }
                            };
                        let mut v: Vec<u64> = set
                            .iter()
                            .filter(|d| ((*d >> 48) as u16) == tid)
                            .collect();
                        if let Some(l) = del_limit {
                            v.truncate(l as usize);
                        }
                        v
                    }
                };
                if ids.is_empty() {
                    return QueryResponse::Ok(0, 0);
                }
                match engine.delete_batch(ids.into_iter()) {
                    Ok(n) => QueryResponse::Ok(n, 0),
                    Err(e) => QueryResponse::Err(1064, format!("delete error: {e}")),
                }
            }
        }
        Err(e) => QueryResponse::Err(1064, format!("delete syntax: {e}")),
    }
}

/// B2：解析主键闭区间 WHERE——`id BETWEEN a AND b` / `docid BETWEEN a AND b`
/// （SQL row_id 区间，闭区间含 a/b；对齐 MySQL DELETE 主键区间语义）。
/// 其余形态返回 None（交回 resolve_where_ids）。
pub(crate) fn parse_pk_between(where_part: &str) -> Option<(u64, u64)> {
    let w = where_part.trim().trim_end_matches(';').trim();
    let lower = w.to_lowercase();
    let rest = lower
        .strip_prefix("id between ")
        .or_else(|| lower.strip_prefix("docid between "))?;
    let (lo_s, hi_s) = rest.split_once(" and ")?;
    let lo = lo_s.trim().parse::<u64>().ok()?;
    let hi = hi_s.trim().trim_end_matches(';').trim().parse::<u64>().ok()?;
    if lo > hi {
        return None; // 空区间：交回通用路径（也返回 0 行）
    }
    Some((lo, hi))
}

/// B2：主键闭区间批量删——docid ∈ [docid_for(tid,lo), docid_for(tid,hi)] 区间内
/// **keys-only 扫描现存 docid** → `engine.delete_batch`（P122：引擎内已按 per-CPU 队列深度
/// 预算拆子批防单 scope 超深背压死锁）。只删**现存**行（区间内缺失/已删不计数，对齐 MySQL）。
pub(crate) fn delete_pk_range(engine: &mut Engine, tid: u16, lo: u64, hi: u64) -> QueryResponse {
    let start = docid_for(tid, lo);
    let end = docid_for(tid, hi);
    let mut ids: Vec<u64> = Vec::new();
    match engine.scan_stream_ids(Some(start), Some(end), |d| {
        // keys-only：不解文档值；回调 false 可提前终止（此处全量收集）
        if d < start || d > end {
            return Ok(true); // 防御：区间外不收集
        }
        ids.push(d);
        Ok(true)
    }) {
        Ok(()) => {}
        Err(e) => return QueryResponse::Err(1064, format!("delete range: {e}")),
    }
    if ids.is_empty() {
        return QueryResponse::Ok(0, 0);
    }
    match engine.delete_batch(ids.into_iter()) {
        Ok(n) => QueryResponse::Ok(n, 0),
        Err(e) => QueryResponse::Err(1064, format!("delete range: {e}")),
    }
}

/// put 文档（doc JSON → 提取倒排 term 复用 HTTP 路径语义）。
pub(crate) fn put_doc(engine: &mut Engine, id: u64, doc: &str) -> Result<()> {
    let terms = doc_terms(doc)?;
    let refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
    engine.put(id, doc.as_bytes().to_vec(), &refs)
}

/// 事务内 put：攒批到事务（H-4，commit 时原子应用）。
pub(crate) fn put_doc_txn(txn: &mut crate::txn::Transaction, id: u64, doc: &str) -> Result<()> {
    let terms = doc_terms(doc)?;
    txn.put(id, doc.as_bytes().to_vec(), terms);
    Ok(())
}

/// 从 JSON 文档提取倒排词条。
pub(crate) fn doc_terms(doc: &str) -> Result<Vec<String>> {
    let parsed: serde_json::Value = serde_json::from_str(doc)
        .map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
    Ok(crate::server::extract_terms(&parsed))
}
