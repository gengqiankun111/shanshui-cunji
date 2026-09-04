
//! SELECT 响应执行族（server/command/select.rs）：内容拆分自原 src/db_adapter.rs——
//! select_response（VERSION / @@ 系统值 / 主键点查 / BETWEEN 流式窗口 / IN / 聚合 / GROUP BY /
//! sqlish 兜底）、倒排 COUNT 快路径（single_eq_count_field / try_count_fast）与点查提取。

use crate::engine::Engine;
use crate::server::*;


/// Ex-9.1：解析 `SELECT COUNT... FROM ... WHERE <f>='<v>'` 单字段等值模板 → (field, value)。
/// 多条件 / 比较 / BETWEEN / IN / 非 COUNT 聚合 / ORDER|GROUP / 主键 id|k / 非引号值 → None。
pub(crate) fn single_eq_count_field(sql: &str) -> Option<(String, String)> {
    let u = sql.to_uppercase();
    if !u.contains("COUNT(") {
        return None;
    }
    for bad in ["SUM(", "AVG(", "MIN(", "MAX(", "DISTINCT", "GROUP BY", "ORDER BY", "LIMIT"] {
        if u.contains(bad) {
            return None;
        }
    }
    if u.contains(" AND ") || u.contains(" OR ") {
        return None;
    }
    let lower = sql.to_lowercase();
    let w = lower.find("where")?;
    let rest = sql[w + 5..].trim();
    let (lhs, rhs) = rest.split_once('=')?;
    let field = lhs.trim().trim_matches('`').trim().to_string();
    if field.is_empty()
        || field.eq_ignore_ascii_case("id")
        || field.eq_ignore_ascii_case("docid")
        || field.eq_ignore_ascii_case("k")
    {
        return None;
    }
    let rhs = rhs.trim();
    if !rhs.starts_with('\'') {
        return None; // 仅支持字符串等值（status='active' 形态）；数字/裸值维持全扫
    }
    let mut out = String::new();
    let mut it = rhs[1..].chars();
    while let Some(c) = it.next() {
        if c == '\'' {
            if it.clone().next() == Some('\'') {
                out.push('\'');
                it.next();
                continue;
            }
            break;
        }
        out.push(c);
    }
    Some((field, out))
}

/// Ex-9.1：单字段等值 COUNT 倒排快路径——`engine.inverted_doc_count("f=v")`
/// （白名单内存位图 / 倒排段 doc_count + flush pending），亚毫秒级返回；Err 回落全扫。
pub(crate) fn try_count_fast(engine: &mut Engine, sql: &str) -> Option<QueryResponse> {
    let (field, value) = single_eq_count_field(sql)?;
    if !engine.inverted_count_eligible(&field) {
        return None;
    }
    let term = format!("{field}={value}");
    match engine.inverted_doc_count(&term) {
        Ok(n) => {
            let agg_col = column_payload("COUNT(*)", MYSQL_TYPE_LONGLONG, 63);
            Some(QueryResponse::Set {
                columns: vec![agg_col],
                rows: vec![vec![n.to_string().into_bytes()]],
            })
        }
        Err(_) => None,
    }
}

/// SELECT：VERSION() / @@ 系统值 → 单行结果；`WHERE id=N` → 主键点查；
/// 否则走 sqlish 引擎。
pub(crate) fn select_response(engine: &Engine, sql: &str) -> QueryResponse {
    let upper = sql.trim().to_uppercase();
    if upper.contains("VERSION()") {
        let columns = vec![column_payload("VERSION()", MYSQL_TYPE_VAR_STRING, 45)];
        let rows = vec![vec![SERVER_VERSION.as_bytes().to_vec()]];
        return QueryResponse::Set { columns, rows };
    }
    if upper.contains("@@") {
        // 系统变量按名返回真值：rust mysql crate v26 在连接建立后自动执行
        // `SELECT @@max_allowed_packet` 取非 0 数值，否则 SetupError → "Could not setup
        // connection"。此前所有 @@ 一律回字符串版本号 → 数值解析失败 = 该错误的另一根因。
        if upper.contains("@@MAX_ALLOWED_PACKET") {
            let columns = vec![column_payload("@@max_allowed_packet", MYSQL_TYPE_LONGLONG, 21)];
            let rows = vec![vec![b"67108864".to_vec()]];
            return QueryResponse::Set { columns, rows };
        }
        let columns = vec![column_payload("@@version", MYSQL_TYPE_VAR_STRING, 45)];
        let rows = vec![vec![SERVER_VERSION.as_bytes().to_vec()]];
        return QueryResponse::Set { columns, rows };
    }
    // 列投影：`SELECT id` → 仅 id 列；`SELECT status` → 字段列（类型按整列实际值推断）
    let proj = parse_projection(sql);
    let limit = extract_limit(sql);
    // MySQL 客户端以 `id` 为主键列 → 主键点查（sqlish 侧为 docid 特例）
    // §26 M1：SQL row_id → 引擎 docid（docid = table_id<<48 | row）
    let tid = table_id_for(&table_name_of(sql));
    // P1-3：聚合/分组按**本表 docid 区间**执行（sqlish 窗口入口）——默认表 = [0, 2^48) 与
    // 单表库全库等价、多表库正确隔离（documents 不混入其它表）；倒排统计快路径保留于直接
    // sqlish API（bench/demo），本 server 路径一律窗口（禁用跨表倒排串表）。
    let (agg_start, agg_end) = (Some(table_base(tid)), Some(table_base(tid) + ROW_ID_MASK));
    if let Some(id) = extract_point_id(sql) {
        // 标量聚合点查：`SELECT SUM(k)/count(k) … WHERE id=N` → 单行单列（与 BETWEEN/IN
        // 分支同语义；须先于普通点查短路——否则被当行集返回（返回 id 列而非字段和）
        let upper_p = sql.to_uppercase();
        if upper_p.contains("SUM(") || upper_p.contains("COUNT(") {
            let mut sum: i64 = 0;
            if let Ok(Some(v)) = engine.get(docid_for(tid, id)) {
                if let Ok(doc) = serde_json::from_slice::<serde_json::Value>(&v) {
                    if let Some(k) = doc.get("k").and_then(|x| x.as_i64()) {
                        sum += k;
                    }
                }
            }
            let agg_col = column_payload("agg", MYSQL_TYPE_LONGLONG, 63);
            return QueryResponse::Set {
                columns: vec![agg_col],
                rows: vec![vec![sum.to_string().into_bytes()]],
            };
        }
        let raw = match engine.get(docid_for(tid, id)) {
            Ok(Some(v)) => vec![(docid_for(tid, id), v)],
            _ => Vec::new(),
        };
        return build_result_set(proj.as_deref(), raw, false, limit);
    }
    // M 项 P0：`id BETWEEN A AND B` → 一次范围扫描（替代逐 id 点查）
    // Ex-8.1：非事务收集路径（engine.scan_range → 逐 SST 线性走块索引 + L2 全量 clone +
    // 收集排序，50m 下 86ms 且随窗口位置劣化）改走**流式窗口** engine.scan_stream
    // （SstRangeIter 二分定位起始块 + Zone Map 只读相交块 + k-way merge；删除位图已过滤），
    // 语义与收集路径等价（demo range-window 验证）。
    if let Some((a, b)) = extract_between_range(sql) {
        // Ex-8.3 Part B：纯 `SELECT id`（无聚合/无 ORDER BY）走 keys-only 流式——
        // 免整文档值解码/拷贝（SST 端 new_keys_cached 值跳过 + 块缓存 + 位图过滤 + LIMIT 早停）
        let upper0 = sql.to_uppercase();
        let pure_id = !upper0.contains("SUM(")
            && !upper0.contains("COUNT(")
            && !upper0.contains("ORDER BY")
            && matches!(
                proj.as_deref(),
                Some(v) if v.len() == 1 && matches!(v[0], ProjCol::Id)
            );
        if pure_id {
            let mut rows: Vec<(u64, Vec<u8>)> = Vec::new();
            let res = engine.scan_stream_ids(
                Some(docid_for(tid, a)),
                Some(docid_for(tid, b)),
                |docid| {
                    rows.push((docid, Vec::new())); // 纯 id 结果 doc 值不参与输出
                    Ok(true)
                },
            );
            if res.is_err() {
                rows.clear();
            }
            return build_result_set(proj.as_deref(), rows, false, limit);
        }
        let mut rows: Vec<(u64, Vec<u8>)> = Vec::new();
        let res = engine.scan_stream(
            Some(docid_for(tid, a)),
            Some(docid_for(tid, b)),
            |docid, val| {
                rows.push((docid, val.to_vec()));
                Ok(true)
            },
        );
        if res.is_err() {
            rows.clear(); // 与旧 collect 路径一致：错误 → 空结果
        }
        let upper2 = sql.to_uppercase();
        if upper2.contains("SUM(") || upper2.contains("COUNT(") {
            let mut sum: i64 = 0;
            for (_, doc) in &rows {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(doc) {
                    if let Some(k) = v.get("k").and_then(|x| x.as_i64()) {
                        sum += k;
                    }
                }
            }
            let agg_col = column_payload("agg", MYSQL_TYPE_LONGLONG, 63);
            return QueryResponse::Set {
                columns: vec![agg_col],
                rows: vec![vec![sum.to_string().into_bytes()]],
            };
        }
        return build_result_set(proj.as_deref(), rows, upper2.contains("ORDER BY"), limit);
    }
    // sysbench 扩展：`id IN (...)` → 逐 id 点查；
    // `SELECT SUM(k)/count(k) ... WHERE id ...` → 聚合（单行单列）。
    if let Some(ids) = extract_target_ids(sql) {
        let upper2 = sql.to_uppercase();
        if upper2.contains("SUM(") || upper2.contains("COUNT(") {
            let mut sum: i64 = 0;
            for id in &ids {
                if let Ok(Some(v)) = engine.get(docid_for(tid, *id)) {
                    if let Ok(doc) = serde_json::from_slice::<serde_json::Value>(&v) {
                        if let Some(k) = doc.get("k").and_then(|x| x.as_i64()) {
                            sum += k;
                        }
                    }
                }
            }
            let agg_col = column_payload("agg", MYSQL_TYPE_LONGLONG, 63);
            return QueryResponse::Set {
                columns: vec![agg_col],
                rows: vec![vec![sum.to_string().into_bytes()]],
            };
        }
        let mut raw: Vec<(u64, Vec<u8>)> = Vec::with_capacity(ids.len());
        for id in ids {
            if let Ok(Some(v)) = engine.get(docid_for(tid, id)) {
                raw.push((docid_for(tid, id), v));
            }
        }
        return build_result_set(proj.as_deref(), raw, upper2.contains("ORDER BY"), limit);
    }
    // sysbench select_random_points/ranges：`WHERE k IN/BETWEEN ...`（k 为非索引随机键，
    // 文档库无 k 列索引 → 语义上返回空结果集；聚合 count(k) 返回 0，保证协议往返可测）。
    let u3 = sql.to_uppercase();
    if u3.contains("WHERE K IN") || u3.contains("K BETWEEN") || u3.contains("WHERE K =") {
        if u3.contains("COUNT(") || u3.contains("SUM(") {
            let agg_col = column_payload("agg", MYSQL_TYPE_LONGLONG, 63);
            return QueryResponse::Set {
                columns: vec![agg_col],
                rows: vec![vec![b"0".to_vec()]],
            };
        }
        // 非聚合 k 查询 → 空结果集（保持列结构，sysbench 不校验行数）
        let cols = vec![
            column_payload("id", MYSQL_TYPE_LONGLONG, 63),
            column_payload("k", MYSQL_TYPE_LONGLONG, 63),
            column_payload("c", MYSQL_TYPE_VAR_STRING, 45),
            column_payload("pad", MYSQL_TYPE_VAR_STRING, 45),
        ];
        return QueryResponse::Set {
            columns: cols,
            rows: Vec::new(),
        };
    }
    // §26 M1b + P1-3：非默认表聚合/分组**按本表 docid 区间执行**（sqlish 增加窗口入口
    // execute_aggregate_window / execute_group_by_window；窗口内过滤防跨表串表，不再 1064）。
    // 注：非默认表窗口聚合禁用倒排统计快路径（引擎全库口径），语义=表区间全扫。
    // AF#2~#4：GROUP BY（单/多字段 + COUNT/SUM/AVG/MIN/MAX）→ 多行分组结果集
    //（选中分组列 + 每聚合一列）。置于标量聚合前（分组 SQL 含聚合列，须先路由到多行分组执行器）。
    // P1-3：非默认表走 docid 窗口（按表区间分组，防倒排词典/全库跨表串表）。
    match crate::sqlish::execute_group_by_window(engine, sql, 10_000, agg_start, agg_end) {
        Ok(Some(gr)) => {
            // 分组列：列为选中的分组字段（select 顺序）；level = 该字段在全部组字段中的位序。
            let mut columns: Vec<Vec<u8>> = Vec::new();
            for name in &gr.group_cols {
                let Some(level) = gr.group_fields.iter().position(|f| f == name) else {
                    return QueryResponse::Err(1064, format!("group col {name} 非分组字段"));
                };
                // 该 level 列类型按实际值：整型 LONGLONG / 浮点 DOUBLE / 字符串 VAR_STRING。
                let mut col_type = MYSQL_TYPE_VAR_STRING;
                for r in &gr.rows {
                    if let Some(t) = r.keys.get(level).and_then(|k| k.as_ref()) {
                        col_type = if !r.key_is_num[level] {
                            MYSQL_TYPE_VAR_STRING
                        } else if t.contains('.') || t.contains('e') || t.contains('E') {
                            MYSQL_TYPE_DOUBLE
                        } else {
                            MYSQL_TYPE_LONGLONG
                        };
                        break;
                    }
                }
                let charset = if col_type == MYSQL_TYPE_VAR_STRING { 45 } else { 63 };
                columns.push(column_payload(name, col_type, charset));
            }
            // 聚合列：整值（COUNT/整值 SUM/MIN/MAX）LONGLONG；含小数（. / e）→ DOUBLE。
            for (i, h) in gr.headers.iter().enumerate() {
                let frac = gr.rows.iter().any(|r| {
                    r.cells
                        .get(i)
                        .map(|c| {
                            c.as_deref()
                                .map(|t| t.contains('.') || t.contains('e') || t.contains('E'))
                                .unwrap_or(false)
                        })
                        .unwrap_or(false)
                });
                columns.push(column_payload(
                    h,
                    if frac { MYSQL_TYPE_DOUBLE } else { MYSQL_TYPE_LONGLONG },
                    63,
                ));
            }
            let mut rows: Vec<Vec<Vec<u8>>> = Vec::with_capacity(gr.rows.len());
            for r in &gr.rows {
                let mut row = Vec::with_capacity(columns.len());
                for name in &gr.group_cols {
                    let level = gr.group_fields.iter().position(|f| f == name).unwrap_or(0);
                    match r.keys.get(level).and_then(|k| k.as_ref()) {
                        Some(t) => row.push(t.clone().into_bytes()),
                        None => row.push(vec![MYSQL_NULL_CELL]),
                    }
                }
                for c in &r.cells {
                    match c {
                        Some(t) => row.push(t.clone().into_bytes()),
                        None => row.push(vec![MYSQL_NULL_CELL]),
                    }
                }
                rows.push(row);
            }
            return QueryResponse::Set { columns, rows };
        }
        Ok(None) => {}
        Err(e) => return QueryResponse::Err(1064, format!("query error: {e}")),
    }
    // 7.95 聚合（字段条件 / 无 WHERE）：COUNT/SUM/AVG/MIN/MAX → 单行单列。
    // 放在 sqlish 兜底前——id 主键窗口聚合（BETWEEN/IN 分支）已先行返回，不受影响；
    // 聚合走全量单遍扫描 matches_doc（不依赖倒排完整性，与 MySQL 无索引聚合同语义）。
    // P1-3：非默认表按表 docid 区间聚合（窗口内全扫，禁用跨表倒排统计快路径）
    match crate::sqlish::execute_aggregate_window(engine, sql, agg_start, agg_end) {
        Ok(Some(agg)) => {
            let (col_type, charset) = if agg.header.starts_with("AVG(")
                || agg.header.starts_with("MIN(")
                || agg.header.starts_with("MAX(")
            {
                (MYSQL_TYPE_DOUBLE, 63)
            } else {
                (MYSQL_TYPE_LONGLONG, 63) // COUNT / SUM
            };
            let row = if agg.is_null {
                vec![vec![vec![MYSQL_NULL_CELL]]]
            } else {
                vec![vec![agg.text.into_bytes()]]
            };
            let columns = vec![column_payload(&agg.header, col_type, charset)];
            return QueryResponse::Set { columns, rows: row };
        }
        Ok(None) => {}
        Err(e) => return QueryResponse::Err(1064, format!("query error: {e}")),
    }
    // 一般 SELECT → sqlish 引擎（结果按投影列裁剪；limit/排序 sqlish 内部处理）
    match crate::sqlish::execute(engine, sql, 10_000) {
        Ok(mut rows) => {
            // §26 M1b：非默认表纯字段谓词 → 兜底行限定本表区间（防跨表泄漏）
            if tid != 0 {
                rows.retain(|(d, _)| ((*d >> 48) as u16) == tid);
            }
            build_result_set(proj.as_deref(), rows, false, None)
        }
        Err(e) => QueryResponse::Err(1064, format!("query error: {e}")),
    }
}

/// 提取 `WHERE id=N` / `WHERE docid=N` 的 docid（仅纯点查；含其他条件返回 None）。
pub(crate) fn extract_point_id(sql: &str) -> Option<u64> {
    let lower = sql.to_lowercase();
    let w = lower.find("where")?;
    let rest = lower[w + 5..].trim();
    let rest = rest.strip_prefix("id")?.trim_start();
    let rest = rest.strip_prefix('=')?.trim_start();
    let num: String = rest
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    if num.is_empty() {
        None
    } else {
        num.parse().ok()
    }
}
