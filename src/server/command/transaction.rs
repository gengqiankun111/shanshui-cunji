
//! 会话级事务读与隔离级别（server/command/transaction.rs）：内容拆分自原 db_adapter.rs——
//! parse_isolation_level、事务内 SELECT（txn_select / txn_select_by_predicate /
//! txn_read_current / txn_scan_current）与窗口 / 目标 / 限额提取（extract_between_range /
//! extract_target_ids / extract_limit）。

use crate::engine::Engine;
use crate::server::*;
use crate::sql::parser::parse_select;


/// 解析 `SET [SESSION] TRANSACTION ISOLATION LEVEL <level>`（会话级，大写输入）。
/// 返回 None = 非隔离级别 SET（调用方忽略，返回 OK 保持客户端兼容）。
/// READ UNCOMMITTED 未单独实现：映射到 READ COMMITTED（我们的事务写提交前
/// 不可见，天然无脏读，语义比真 RU 更严格、无副作用）。
pub(crate) fn parse_isolation_level(upper: &str) -> Option<crate::txn::Isolation> {
    let up = upper.trim();
    let marker = "TRANSACTION ISOLATION LEVEL";
    let idx = up.find(marker)?;
    let tail = up[idx + marker.len()..].trim();
    if tail.starts_with("READ UNCOMMITTED") || tail.starts_with("READ COMMITTED") {
        Some(crate::txn::Isolation::ReadCommitted)
    } else if tail.starts_with("REPEATABLE READ") {
        Some(crate::txn::Isolation::RepeatableRead)
    } else if tail.starts_with("SERIALIZABLE") {
        Some(crate::txn::Isolation::Serializable)
    } else {
        None
    }
}

/// 缺陷 A：事务内**单行当前读**取值（SELECT … FOR UPDATE）——同事务未提交写优先
/// （read_own：`Some(Some(v))`=put 最新、`Some(None)`=本事务删除→行隐藏），否则读引擎
/// **最新已提交**（`Engine::get`：含删除位图过滤 + Delta 覆盖 + HotCache）。
/// 不写事务 snap 快照缓存（当前读结果不得污染 RR 一致读——C1 断言快照仍见旧值）。
pub(crate) fn txn_read_current(
    engine: &Engine,
    txn: &mut crate::txn::Transaction,
    docid: u64,
) -> crate::error::Result<Option<Vec<u8>>> {
    if txn.is_finished() {
        return Err(crate::error::Error::TxnAborted(format!(
            "txn#{} 已结束",
            txn.id
        )));
    }
    if let Some(own) = txn.read_own(docid) {
        return Ok(own.map(|v| v.to_vec()));
    }
    let v = engine.get(docid)?;
    // P1-4：FOR UPDATE 当前读命中 → 记录锁定版本（读取时引擎最新 seq）；提交时写该键
    // 若期间无并发再改则放行（对齐 MySQL 当前读后写语义）。行不存在（None）不锁。
    if v.is_some() {
        let seq = engine.last_write_seq(docid)?;
        txn.mark_current_lock(docid, seq);
    }
    Ok(v)
}

/// 缺陷 A：事务内**范围当前读**（SELECT … FOR UPDATE + id BETWEEN/范围）——基表 = 引擎
/// **最新已提交**扫描（`Engine::scan_range`，含删除位图过滤），再叠加同事务写覆盖
/// （read_own 值替换 / 本事务删除排除、write_set Put 新 docid 窗口并入）——语义与
/// `Engine::scan_range_txn` 尾部一致，仅基表由快照视图换当前视图（引擎侧不动）。
pub(crate) fn txn_scan_current(
    engine: &Engine,
    txn: &mut crate::txn::Transaction,
    start: Option<u64>,
    end: Option<u64>,
) -> crate::error::Result<Vec<crate::engine::QueryRow>> {
    if txn.is_finished() {
        return Err(crate::error::Error::TxnAborted(format!(
            "txn#{} 已结束",
            txn.id
        )));
    }
    let mut out: Vec<crate::engine::QueryRow> = engine.scan_range(start, end)?;
    // P1-4：范围当前读命中行 → 记录锁定版本（引擎最新 seq；自写行 read_own 属写集，无需锁）
    for row in &out {
        if txn.read_own(row.0).is_none() {
            let seq = engine.last_write_seq(row.0)?;
            txn.mark_current_lock(row.0, seq);
        }
    }
    // 同事务写覆盖：已出现的行用 read_own 最新值替换 / 本事务删除（None）置空 → 下方过滤
    for row in out.iter_mut() {
        if let Some(own) = txn.read_own(row.0) {
            match own {
                Some(v) => row.1 = v.to_vec(),
                None => row.1.clear(),
            }
        }
    }
    out.retain(|(_, v)| !v.is_empty());
    // 同事务未提交 Put 的新 docid（窗口内且基表未见——本事务写未落引擎）并入
    let own_ids: Vec<u64> = txn
        .ops()
        .iter()
        .filter_map(|op| match op {
            crate::txn::Op::Put { docid, .. } => Some(*docid),
            crate::txn::Op::Delete { .. } => None,
        })
        .collect();
    let present: std::collections::HashSet<u64> = out.iter().map(|(d, _)| *d).collect();
    let mut added = false;
    for d in own_ids {
        if present.contains(&d) {
            continue;
        }
        let in_win = start.map_or(true, |s| d >= s) && end.map_or(true, |e| d <= e);
        if in_win {
            if let Some(Some(v)) = txn.read_own(d) {
                out.push((d, v.to_vec()));
                added = true;
            }
        }
    }
    if added {
        out.sort_by_key(|r| r.0); // 保持升序（自写并入后重排；事务窗口通常小）
    }
    Ok(out)
}

/// P140（2026-09-06）：事务 SELECT 投影可否下推为目标列——列清单仅 `id/docid + 简单顶层
/// 字段名`（无 `doc`/`*`/嵌套路径/表达式，且至少一个字段列）→ 返回字段名集合（不含 id）。
/// 不可下推返回 None（调用方维持整行 `scan_range_txn`，语义不变）。
pub(crate) fn plain_field_projection(proj: Option<&[ProjCol]>) -> Option<Vec<String>> {
    let cols = proj?;
    if cols.is_empty() {
        return None;
    }
    let mut fields = Vec::new();
    for c in cols {
        match c {
            ProjCol::Id => {}
            ProjCol::Doc => return None, // 需整行
            ProjCol::Field(f) => {
                // 嵌套路径（addr.city / arr[0]）须整行深查 → 回退
                if f.contains('.') || f.contains('[') {
                    return None;
                }
                fields.push(f.clone());
            }
        }
    }
    if fields.is_empty() {
        return None;
    }
    Some(fields)
}

/// 事务内 SELECT：快照查询（含同事务未提交写可见）。
/// sysbench 兼容（H-6 扩展）：`WHERE id=N` 点查 / `id BETWEEN A AND B` 范围 /
/// `id IN (...)` 多点 / `ORDER BY ... LIMIT N`（简化为排序截断）。
/// P141（2026-09-06）：聚合/分组（COUNT/SUM/AVG/MIN/MAX [DISTINCT]、GROUP BY/HAVING）已在
/// 顶部经 parse_select 检测 → 转 txn_agg（txn_aggregate）权威事务行流执行（见函数头注）。
/// P140（2026-09-06）：纯字段投影范围查询走 `scan_range_txn_fields`（见 plain_field_projection）。
/// M 项优化（P0）：BETWEEN 范围走一次快照扫描（`scan_range_txn`），替代逐 id `txn_get`；
/// 点查 / IN 保持逐 id（目标少，逐 id 更快）。
/// 缺陷 A：`FOR UPDATE` 尾部修饰 → **当前读**（`txn_read_current` / `txn_scan_current`，
/// 最新已提交 + 自写覆盖；不污染快照缓存）——RR 对照 C1/C3 修复。
/// O 项第②步：`&Engine`（事务读在 RwLock 读锁下执行）。
pub(crate) fn txn_select(
    engine: &Engine,
    session: &mut Session,
    sql: &str,
) -> QueryResponse {
    // FOR UPDATE = 当前读标记：解析前剥离该尾部修饰（parser 只认查询核心；FOR UPDATE 语法
    // 恒在 ORDER BY / LIMIT 之后）；读路径按标记分流。
    let for_update = sql.to_uppercase().contains("FOR UPDATE");
    let core: &str = if for_update {
        match sql.to_lowercase().find("for update") {
            Some(p) => &sql[..p],
            None => sql, // 防御：出现在非尾部（字符串字面量误匹配）→ 保持原样
        }
    } else {
        sql
    };
    // P141（2026-09-06）：事务内聚合/分组（COUNT/SUM/AVG/MIN/MAX [DISTINCT] /
    // GROUP BY [HAVING/ORDER BY/LIMIT]）→ txn_agg 权威事务行流执行（快照/RC 视图 + FOR UPDATE
    // 当前读，同事务未提交写可见；语义对齐非事务权威聚合）。仅当 parse_select 成功**且**命中
    // 聚合/分组形态才转入（防扩面：parser 不支持或普通查询维持既有事务读路径）。
    if let Ok(sel) = parse_select(core) {
        if sel.agg.is_some() || !sel.group_by.is_empty() {
            return txn_aggregate(engine, session, core, for_update);
        }
    }
    let proj = parse_projection(core);
    let limit = extract_limit(core);
    let upper = core.to_uppercase();
    let txn = session.txn.as_mut().unwrap();
    // 范围查询（BETWEEN）：快照扫描（M 项 P0，逐 id txn_get → scan_range_txn）；
    // FOR UPDATE → 当前读（最新已提交扫描 + 同事务写覆盖）
    let tid = table_id_for(&table_name_of(core));
    if let Some((a, b)) = extract_between_range(core) {
        // §26 M1：SQL row_id 窗口 → 本表 docid 窗口
        let (da, db) = (docid_for(tid, a), docid_for(tid, b));
        // P140：纯字段投影（非 FOR UPDATE/SUM，且无 doc/嵌套列）→ 快照范围扫 + 目标列下推
        let proj_fields = plain_field_projection(proj.as_deref());
        let rows = if for_update {
            match txn_scan_current(engine, txn, Some(da), Some(db)) {
                Ok(r) => r,
                Err(e) => return QueryResponse::Err(3500, format!("事务范围当前读失败: {e}")),
            }
        } else if let Some(fs) = proj_fields {
            match engine.scan_range_txn_fields(txn, Some(da), Some(db), fs) {
                Ok(r) => r,
                Err(e) => return QueryResponse::Err(3500, format!("事务范围读失败: {e}")),
            }
        } else {
            match engine.scan_range_txn(txn, Some(da), Some(db)) {
                Ok(r) => r,
                Err(e) => return QueryResponse::Err(3500, format!("事务范围读失败: {e}")),
            }
        };
        // 普通范围查询：按投影裁剪（字段列类型推断）+ ORDER BY / LIMIT
        return build_result_set(proj.as_deref(), rows, upper.contains("ORDER BY"), limit);
    }
    // 点查 / IN：逐 id 快照 get（同事务写可见）/ FOR UPDATE 当前读
    let ids: Vec<u64> = match extract_target_ids(core) {
        Some(v) => v,
        None => {
            // b：非主键列谓词 → 主库候选 ∪ 同事务写集覆盖复检（缺陷 A：支持 FOR UPDATE 当前读）
            if tid != 0 {
                return QueryResponse::Err(
                    1064,
                    "事务内字段谓词仅支持默认表（§26 M1 边界，主键/窗口访问可用）".to_string(),
                );
            }
            return txn_select_by_predicate(
                engine, session, core, proj.as_deref(), limit, &upper, for_update,
            );
        }
    };
    // 普通点查 / IN：逐 id 快照 get（同事务写可见）/ 当前读，按投影裁剪
    let mut raw: Vec<(u64, Vec<u8>)> = Vec::with_capacity(ids.len());
    for id in &ids {
        let r = if for_update {
            txn_read_current(engine, txn, *id)
        } else {
            engine.txn_get(txn, *id)
        };
        match r {
            Ok(Some(v)) => raw.push((*id, v)),
            Ok(None) => {}
            Err(e) => return QueryResponse::Err(3500, format!("事务读失败: {e}")),
        }
    }
    build_result_set(proj.as_deref(), raw, upper.contains("ORDER BY"), limit)
}

/// b：事务内**非主键列谓词** SELECT（普通投影查询；聚合已由 txn_select 顶部 P141 拦截）：
/// 候选 = 主库当前视图命中（sqlish，事务持引擎写锁 → 视图稳定）∪ 同事务写集；
/// 逐候选 `txn_get` 覆盖取值 + `sqlish::doc_matches_where` 谓词复检 → 结果与
/// 快照+同事务写一致（自增后自见、删除即不可见、新增被收录）。
pub(crate) fn txn_select_by_predicate(
    engine: &Engine,
    session: &mut Session,
    sql: &str,
    proj: Option<&[ProjCol]>,
    limit: Option<usize>,
    upper: &str,
    for_update: bool,
) -> QueryResponse {
    // 取 WHERE 谓词原文（ASCII 偏移与 lower 一致；按 rest 原样切片保字符串大小写语义）
    let lower = sql.to_lowercase();
    let pos = match lower.find("where") {
        Some(p) => p,
        None => return QueryResponse::Err(1064, "事务内无 WHERE 全表查询暂不支持".to_string()),
    };
    let rest = &sql[pos + 5..];
    let ol = rest.to_lowercase();
    let end = [
        ol.find("order by"),
        ol.find(" limit "),
        ol.find(" limit)"),
    ]
    .into_iter()
    .flatten()
    .min()
    .unwrap_or(rest.len());
    let tail = rest[..end].trim();
    if tail.is_empty() {
        return QueryResponse::Err(1064, "事务内无 WHERE 全表查询暂不支持".to_string());
    }
    let cond_sql = format!("SELECT docid FROM t WHERE {tail}");
    let txn = session.txn.as_mut().unwrap();
    // P134（2026-09-06）：RR/SERIALIZABLE 快照 + 非 FOR UPDATE（当前读）→ 字段谓词候选改走
    // **快照窗口扫描**（候选 = 快照可见行全集，无"最新态删除/换值预过滤"→ 快照后被并发删/
    // 换值的行仍可见，与点查 get_at 语义自洽；旧路径用最新态 sqlish 候选 → 漏行）。RC 无快照、
    // FOR UPDATE 当前读均见最新已提交，旧候选路径正确，保持不变。
    if !for_update && txn.isolation.uses_snapshot() && engine.auto_watermark() <= (1u64 << 48) {
        return txn_select_by_predicate_snapshot(engine, txn, &cond_sql, tail, proj, limit, upper);
    }
    const CAP: u64 = 200_000;
    let base = match crate::sqlish::execute(engine, &cond_sql, CAP) {
        Ok(r) => r,
        Err(e) => return QueryResponse::Err(3500, format!("事务谓词读失败: {e}")),
    };
    let mut set: std::collections::HashSet<u64> = base.into_iter().map(|r| r.0).collect();
    set.extend(txn.write_set().iter().copied());
    let mut ids: Vec<u64> = set.into_iter().collect();
    ids.sort_unstable();
    let mut rows: Vec<(u64, Vec<u8>)> = Vec::with_capacity(ids.len());
    for id in ids {
        // 缺陷 A：FOR UPDATE → 当前读取值（最新已提交 + 自写覆盖），其余走快照 txn_get
        let r = if for_update {
            txn_read_current(engine, txn, id)
        } else {
            engine.txn_get(txn, id)
        };
        match r {
            Ok(Some(v)) => {
                if crate::sqlish::doc_matches_where(&cond_sql, &v) {
                    rows.push((id, v));
                }
            }
            Ok(None) => {}
            Err(e) => return QueryResponse::Err(3500, format!("事务读失败: {e}")),
        }
    }
    build_result_set(proj, rows, upper.contains("ORDER BY"), limit)
}

/// P134：RR/SERIALIZABLE 快照下的事务内字段谓词 SELECT（非 FOR UPDATE）——
/// 候选 = **快照可见行全集**分块窗口扫描 + 谓词复检（`scan_range_txn` 逐窗：
/// ≤快照版本 + 同事务写覆盖 + Delta ≤S 折叠，口径 = 点查 `txn_get`）。
/// 修复：旧路径候选 = 最新态 sqlish 命中（回表 `batch_get` 删除位图剔除已删行）→ 快照后被
/// 并发删/换值的行不在候选 → 事务内重复谓词读消失，与点查 get_at（见旧值）不自洽。
/// 约束：仅默认表域（调用方已校验 tid=0 且 watermark ≤ 2^48）；LIMIT 且无 ORDER BY / 聚合时
/// 早停（结果按 docid 升序，与既有 id 集合排序一致）；RC/FOR UPDATE 走旧路径（见调用点）。
pub(crate) fn txn_select_by_predicate_snapshot(
    engine: &Engine,
    txn: &mut crate::txn::Transaction,
    cond_sql: &str,
    tail: &str,
    proj: Option<&[ProjCol]>,
    limit: Option<usize>,
    upper: &str,
) -> QueryResponse {
    // 默认表单行 docid = row id，高水位 = auto_watermark()-1（调用点已保证 ≤ 2^48-1）
    const SPAN: u64 = 16_384;
    let hi = engine.auto_watermark().saturating_sub(1);
    let order = upper.contains("ORDER BY");
    // P135（2026-09-06）：**候选 superset 快路径**——WHERE 为倒排等值 AND 链时 posting
    // 只加不删 = ever-match 超集 → posting∩窗口 → `batch_get_at(S)` 批量快照复核 + 谓词复检，
    // 替代下方全窗分块扫描（恢复索引加速且 RR 正确：快照后删/换值行仍在 posting → ≤S 版本/
    // 值裁决）。复杂谓词/空 posting/本事务含未提交写（自写合并复杂）→ 全窗扫描兜底。
    if txn.ops().is_empty() {
        if let Some(sup) = inverted_eq_superset(engine, tail) {
            return finish_predicate_superset(engine, txn, cond_sql, sup, hi, proj, limit, order);
        }
    }
    let early_stop = !order && limit.is_some();
    let need = limit.unwrap_or(usize::MAX);
    let mut rows: Vec<(u64, Vec<u8>)> = Vec::with_capacity(need.min(4096));
    // complete = 扫描覆盖到高水位（无早停中断）→ 才需补"超出页高水位的自写新 docid"
    //（早停中断时自写 docid 均按升序落在已扫前缀内/需限之后，补入会破坏 LIMIT 位置）。
    let mut complete = !early_stop;
    let mut w = 0u64;
    'page: loop {
        let e = w.saturating_add(SPAN - 1).min(hi);
        let page = match engine.scan_range_txn(txn, Some(w), Some(e)) {
            Ok(p) => p,
            Err(err) => return QueryResponse::Err(3500, format!("事务谓词读失败: {err}")),
        };
        for (d, v) in page {
            // scan_range_txn 已滤空（自删/墓碑 ≤S）；再按谓词复检快照行值（换值陈旧排除）
            if !crate::sqlish::doc_matches_where(cond_sql, &v) {
                continue;
            }
            rows.push((d, v));
            if early_stop && rows.len() >= need {
                complete = false;
                break 'page;
            }
        }
        if e == hi {
            complete = true;
            break;
        }
        w = e.saturating_add(1);
    }
    // 同事务未提交 Put 的新 docid（完整覆盖时若未被页扫描含入）→ 补入并保序
    if complete && !txn.ops().is_empty() {
        let present: std::collections::HashSet<u64> = rows.iter().map(|(d, _)| *d).collect();
        let mut added = false;
        for op in txn.ops() {
            if let crate::txn::Op::Put { docid, .. } = op {
                if present.contains(docid) || *docid >= (1u64 << 48) {
                    continue;
                }
                if let Some(Some(v)) = txn.read_own(*docid) {
                    if crate::sqlish::doc_matches_where(cond_sql, &v) {
                        rows.push((*docid, v.to_vec()));
                        added = true;
                    }
                }
            }
        }
        if added {
            rows.sort_by_key(|r| r.0);
            if early_stop {
                rows.truncate(need);
            }
        }
    }
    finish_predicate_rows(rows, proj, order, limit)
}

/// P135：谓词结果收尾（build_result_set）——窗口扫描与 superset 快路径共用。
fn finish_predicate_rows(
    rows: Vec<(u64, Vec<u8>)>,
    proj: Option<&[ProjCol]>,
    order: bool,
    limit: Option<usize>,
) -> QueryResponse {
    build_result_set(proj, rows, order, limit)
}

/// P135：WHERE 为"倒排等值 AND 链"→ posting 交集（只加不删 = ever-match **superset**）。
/// 任一叶非 Eq（非 docid）或空 posting（该 term 从未写入 → 无 ever-match 或非倒排可表达）→
/// None（调用方回退快照全窗扫描保正确）。term 形态与 eval_cond 一致（`field=value`）。
fn inverted_eq_superset(engine: &Engine, tail: &str) -> Option<roaring::treemap::RoaringTreemap> {
    use crate::sql::parser::{Cond, CmpOp, WhereExpr};
    let we = crate::sqlish::parse_where_expr(tail).ok()?;
    fn leaf(engine: &Engine, c: &Cond) -> Option<roaring::treemap::RoaringTreemap> {
        if c.op != CmpOp::Eq || c.field == "docid" {
            return None;
        }
        let p = engine.inverted_posting(&format!("{}={}", c.field, c.value)).ok()?;
        if p.is_empty() {
            None // term 从未写入 → 非倒排可表达（数字/未索引/零命中），回退扫描
        } else {
            Some(p)
        }
    }
    fn sup(engine: &Engine, e: &WhereExpr) -> Option<roaring::treemap::RoaringTreemap> {
        match e {
            WhereExpr::Cond(c) => leaf(engine, c),
            WhereExpr::And(a, b) => {
                let m = sup(engine, a)?;
                Some(m & sup(engine, b)?)
            }
            _ => None,
        }
    }
    sup(engine, &we)
}

/// P135：superset 候选 → `batch_get_at(S)` 批量快照复核 + 谓词复检（免 P134 全窗扫）。
/// posting∩默认表窗口（docid ≤ hi，已由调用方保证 < 2^48）升序 → 快照批量取行（语义=逐
/// get_at：跳过位图、tombstone seq 裁决、Delta ≤S 折叠）→ doc_matches_where（换值陈旧排除）。
/// 仅当本事务无未提交写时使用（自写合并走全窗扫描兜底）。
fn finish_predicate_superset(
    engine: &Engine,
    txn: &mut crate::txn::Transaction,
    cond_sql: &str,
    sup: roaring::treemap::RoaringTreemap,
    hi: u64,
    proj: Option<&[ProjCol]>,
    limit: Option<usize>,
    order: bool,
) -> QueryResponse {
    let early_stop = !order && limit.is_some();
    let need = limit.unwrap_or(usize::MAX);
    // 候选收口两条件：默认表单行域（docid 低位域 <2^48）∧ 调用方窗口上界 hi——
    // 单表库（watermark≤2^48）两条件等价于 d≤hi；混合库（watermark 被高 tid 表顶高）时
    // 低位域过滤防跨表高位 docid 混入候选（潜在串行），P134 残余防御（服务端默认表恒低位，
    // 仅引擎直连多命名空间才可达该形态）。
    let mut ids: Vec<u64> = sup.iter().filter(|d| *d < (1u64 << 48) && *d <= hi).collect();
    // P136：批量取行前集合级剔除"S 前已删且未复活"候选（免空回表；漏剔由 batch None 兜底）
    engine.prune_deleted_before_snapshot(&mut ids, txn.snapshot());
    let vals = match engine.batch_get_at(&ids, txn.snapshot()) {
        Ok(v) => v,
        Err(err) => return QueryResponse::Err(3500, format!("事务谓词读失败: {err}")),
    };
    let mut rows: Vec<(u64, Vec<u8>)> = Vec::with_capacity(ids.len().min(4096));
    for (d, bv) in ids.into_iter().zip(vals) {
        let Some(bv) = bv else { continue };
        if !crate::sqlish::doc_matches_where(cond_sql, &bv) {
            continue;
        }
        rows.push((d, bv));
        if early_stop && rows.len() >= need {
            break;
        }
    }
    finish_predicate_rows(rows, proj, order, limit)
}

/// 提取 `WHERE id BETWEEN A AND B` 闭区间 → (A, B)；非 id BETWEEN → None。
pub(crate) fn extract_between_range(sql: &str) -> Option<(u64, u64)> {
    let lower = sql.to_lowercase();
    let w = lower.find("where")?;
    let rest = &lower[w + 5..];
    // A1-3 复测（2026-09-07）：GROUP BY/HAVING 尾部须截断——旧实现只截 ORDER BY/LIMIT →
    // `... id BETWEEN 1 AND 3 GROUP BY s` 的 b 端解析被 "group by s" 污染失败 → 落字段谓词
    // 复检（JSON 无 id → 恒假 → 0 行）；截断后归主键闭窗口直解（txn_agg Between 源权威行流）。
    let rest = rest.split("order by").next()?;
    let rest = rest.split("group by").next()?;
    let rest = rest.split("having").next()?;
    let rest = rest.split("limit").next()?;
    let rest = rest.trim();
    // 仅限 `id between`（排除 k/其他列 BETWEEN）
    let bp = rest.find("id between")?;
    let after = &rest[bp + "id between".len()..];
    let and = after.find("and")?;
    let a: u64 = after[..and].trim().parse().ok()?;
    let b: u64 = after[and + 3..].trim().parse().ok()?;
    Some((a, b))
}

/// 提取 WHERE 目标 id 集合：`id=N` / `id BETWEEN A AND B`（闭区间，上限防爆）/ `id IN (a,b,...)`。
pub(crate) fn extract_target_ids(sql: &str) -> Option<Vec<u64>> {
    let lower = sql.to_lowercase();
    let w = lower.find("where")?;
    let rest = &lower[w + 5..];
    // GROUP BY/HAVING 尾部截断（对齐 extract_between_range；防 b 端/IN 后被聚合子句污染）
    let rest = rest.split("order by").next()?;
    let rest = rest.split("group by").next()?;
    let rest = rest.split("having").next()?;
    let rest = rest.split("limit").next()?;
    let rest = rest.trim();
    // id = N
    if let Some(eq) = rest.find("id=") {
        let after = rest[eq + 3..].trim();
        let num: String = after
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if !num.is_empty() {
            return Some(vec![num.parse().ok()?]);
        }
    }
    // id BETWEEN A AND B（仅限 id 字段——否则 amount/其他列 BETWEEN 会被误当 docid 窗口，7.93 定位）
    if let Some(bp) = rest.find("id between") {
        let after = &rest[bp + "id between".len()..];
        let and = after.find("and")?;
        let a: u64 = after[..and].trim().parse().ok()?;
        let b: u64 = after[and + 3..].trim().parse().ok()?;
        // 闭区间，上限保护（sysbench 范围 100 行内）
        let hi = b.min(a.saturating_add(10_000));
        return Some((a..=hi).collect());
    }
    // id IN (a,b,...)
    if let Some(ip) = rest.find("id in") {
        let after = &rest[ip + 5..];
        let open = after.find('(')?;
        let close = after.find(')')?;
        let inner = &after[open + 1..close];
        let ids: Vec<u64> = inner
            .split(',')
            .filter_map(|s| s.trim().parse::<u64>().ok())
            .collect();
        if !ids.is_empty() {
            return Some(ids);
        }
    }
    None
}

/// 提取 `LIMIT N`。
pub(crate) fn extract_limit(sql: &str) -> Option<usize> {
    let lower = sql.to_lowercase();
    let pos = lower.find("limit")?;
    let rest = lower[pos + 5..].trim();
    let num: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    num.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    /// P135：`inverted_eq_superset` 判定——倒排等值 AND 链 → posting 交集（ever-match，含换值
    /// 陈旧 docid）；非倒排/空 posting/OR/Ne → None（调用方回退快照全窗扫描保正确）。
    #[test]
    fn p135_inverted_eq_superset_gating() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &Config::default()).unwrap();
        e.put(1, br#"{"s":"a","x":5}"#.to_vec(), &["s=a"]).unwrap();
        e.put(2, br#"{"s":"b","x":5}"#.to_vec(), &["s=b"]).unwrap();
        // 单倒排等值 → superset（含 1 不含 2）
        let sup2 = inverted_eq_superset(&e, "s='a'").unwrap();
        assert!(sup2.contains(1) && !sup2.contains(2));
        // AND 链交集
        let sup = inverted_eq_superset(&e, "s='a' AND s='a'").unwrap();
        assert_eq!(sup.iter().collect::<Vec<u64>>(), vec![1]);
        // 非倒排叶（x=5 无 posting）→ None
        assert!(inverted_eq_superset(&e, "x=5").is_none());
        // OR / Ne → None（保守回退）
        assert!(inverted_eq_superset(&e, "s='a' OR s='b'").is_none());
        assert!(inverted_eq_superset(&e, "s!='a'").is_none());
        // 空 posting term → None
        assert!(inverted_eq_superset(&e, "s='never'").is_none());
    }
}
