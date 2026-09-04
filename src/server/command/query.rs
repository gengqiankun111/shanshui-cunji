
//! COM_QUERY / COM_STMT 命令入口与 SQL 分发（server/command/query.rs）：内容拆分自原
//! src/db_adapter.rs——handle_command（命令分发）、读写锁读 / 写分发（dispatch_query /
//! dispatch_query_read / dispatch_query_read_opt / is_read_statement）与 SHOW 系统查询
//! （show_response）。

use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

use crate::engine::Engine;
use crate::multitable::drop_table_range;
use crate::server::*;


/// 处理单个命令包（无 IO）——同步 / 异步连接共用（异步路径经 `spawn_blocking` 执行，
/// 连接 idle 不占 OS 线程，design 9.5 10k 连接目标）。
/// 返回 `(起始 seq, 响应包列表)`；`None` = 连接应终止（COM_QUIT / EOF 由调用方处理）。
pub(crate) fn handle_command(
    engine: &Arc<RwLock<Engine>>,
    session: &mut Session,
    cmd: &[u8],
) -> (u8, Option<Vec<Vec<u8>>>) {
    // 客户端命令包 seq=0，响应包从 seq=1 起递增
    let seq0 = 1u8;
    match cmd[0] {
        COM_QUIT => (seq0, None),
        COM_PING => (seq0, Some(vec![ok_payload(0, 0)])),
        COM_INIT_DB => {
            // 单库模式：任意库名均接受（MySQL 客户端默认带 sbtest 等库名）
            let _db = String::from_utf8_lossy(&cmd[1..]).to_string();
            (seq0, Some(vec![ok_payload(0, 0)]))
        }
        COM_QUERY => {
            // X 项：语句计数（/metrics 指标）
            if let Ok(g) = engine.read() {
                g.metrics.statements.fetch_add(1, Ordering::Relaxed);
            }
            let sql = String::from_utf8_lossy(&cmd[1..]).to_string();
            // O 项第②步：读语句走 RwLock 读锁（多连接 SELECT 并行）；写语句走写锁互斥
            // Ex-9.1：读分发统一入口（含倒排计数快路径，见 dispatch_query_read_opt）
            let resp = if is_read_statement(&sql) {
                dispatch_query_read_opt(engine, &sql, session)
            } else {
                let mut guard = engine.write().unwrap();
                dispatch_query(&mut guard, &sql, session)
            };
            (seq0, Some(query_response_packets(seq0, &resp)))
        }
        COM_STMT_PREPARE => {
            // H-5：预处理语句（JDBC 依赖）。请求 = 0x16 + SQL
            let sql = String::from_utf8_lossy(&cmd[1..]).to_string();
            (seq0, Some(stmt_prepare(session, &sql)))
        }
        COM_STMT_EXECUTE => {
            // I 项高并发：预处理读语句（SELECT/SHOW 等）走 RwLock 读锁——sysbench
            // point_select 等 PREPARE/EXECUTE 负载多连接并行；写语句保持写锁互斥
            match stmt_execute_sql(session, cmd) {
                Ok(sql) => {
                    // Ex-9.1：读分发统一入口（含倒排计数快路径，见 dispatch_query_read_opt）
                    let resp = if is_read_statement(&sql) {
                        dispatch_query_read_opt(engine, &sql, session)
                    } else {
                        let mut guard = engine.write().unwrap();
                        dispatch_query(&mut guard, &sql, session)
                    };
                    (seq0, Some(query_response_packets(seq0, &resp)))
                }
                Err(resp) => (seq0, Some(query_response_packets(seq0, &resp))),
            }
        }
        COM_STMT_CLOSE => {
            // H-5：释放 statement（无响应包）
            if cmd.len() >= 5 {
                let stmt_id = u32::from_le_bytes(cmd[1..5].try_into().unwrap());
                session.statements.remove(&stmt_id);
            }
            (seq0, Some(Vec::new()))
        }
        other => {
            let msg = format!("command {other:#x} not supported");
            (seq0, Some(vec![err_payload(1047, &msg)]))
        }
    }
}
/// 分发 COM_QUERY（H-4：会话级事务 BEGIN/COMMIT/ROLLBACK + 事务内 SQL）。
pub(crate) fn dispatch_query(engine: &mut Engine, sql: &str, session: &mut Session) -> QueryResponse {
    let upper = sql.trim().to_uppercase();
    // 空 / 注释
    if sql.trim().is_empty() || sql.trim().starts_with("--") {
        return QueryResponse::Ok(0, 0);
    }
    // ---- 事务控制语句（H-4）----
    if upper.starts_with("BEGIN") || upper.starts_with("START TRANSACTION") {
        if session.txn.is_some() {
            return QueryResponse::Err(3502, "已有活动事务，不支持嵌套".to_string());
        }
        // 会话级隔离级别（SET TRANSACTION ISOLATION LEVEL 设置，默认 REPEATABLE READ）
        session.txn = Some(engine.txn_begin(session.isolation));
        return QueryResponse::Ok(0, 0);
    }
    if upper.starts_with("COMMIT") {
        // MySQL 语义：无活动事务时 COMMIT 返回 OK（空提交）
        return match session.txn.take() {
            Some(t) => match engine.txn_commit(t) {
                Ok(_) => QueryResponse::Ok(0, 0),
                Err(e) => {
                    // 写冲突 / 死锁 → MySQL 1213（ER_LOCK_DEADLOCK）：客户端（sysbench）
                    // 默认忽略并跳过该事务重试，而非 FATAL 退出。
                    let code = if matches!(
                        e,
                        crate::error::Error::TxnConflict(_) | crate::error::Error::TxnDeadlock(_)
                    ) {
                        1213
                    } else {
                        3500
                    };
                    QueryResponse::Err(code, format!("commit 失败（已回滚）: {e}"))
                }
            },
            None => QueryResponse::Ok(0, 0),
        };
    }
    if upper.starts_with("ROLLBACK") {
        if let Some(t) = session.txn.take() {
            engine.txn_rollback(t);
        }
        return QueryResponse::Ok(0, 0);
    }
    // ---- 事务内语句：快照点查 + 攒批写（同事务可见，commit 原子落库）----
    if session.txn.is_some() {
        if upper.starts_with("SELECT") {
            return txn_select(engine, session, sql);
        }
        if upper.starts_with("INSERT") {
            return txn_insert(engine, session, sql);
        }
        if upper.starts_with("REPLACE") {
            return txn_replace(engine, session, sql);
        }
        if upper.starts_with("UPDATE") {
            return txn_update(engine, session, sql);
        }
        if upper.starts_with("DELETE") {
            return txn_delete(engine, session, sql);
        }
        if upper.starts_with("SET") {
            return QueryResponse::Ok(0, 0);
        }
        return QueryResponse::Err(1064, format!("事务内暂不支持该语句: {sql}"));
    }
    // ---- 非事务语句 ----
    if upper.starts_with("SET") {
        // 会话级 SET：支持 SET [SESSION] TRANSACTION ISOLATION LEVEL <level>；
        // 其余 SET 变量忽略（返回 OK 保持客户端兼容）
        if let Some(lv) = parse_isolation_level(&upper) {
            session.isolation = lv;
        }
        return QueryResponse::Ok(0, 0);
    }
    if upper.starts_with("SHOW") {
        return show_response(&upper);
    }
    if upper.starts_with("SELECT") {
        return select_response(engine, sql);
    }
    if upper.starts_with("INSERT") {
        return insert_response(engine, sql, &session.auto_id);
    }
    if upper.starts_with("REPLACE") {
        return replace_response(engine, sql, &session.auto_id);
    }
    if upper.starts_with("UPDATE") {
        return update_response(engine, sql);
    }
    if upper.starts_with("DELETE") {
        return delete_response(engine, sql);
    }
    if upper.starts_with("SET") || upper.starts_with("USE") {
        return QueryResponse::Ok(0, 0);
    }
    // H-6：DDL 放行（文档库无 schema——CREATE/ALTER/INDEX 映射为 OK 空操作，
    // 使 sysbench prepare/cleanup 可跑通；表统一映射 documents）
    if upper.starts_with("CREATE TABLE")
        || upper.starts_with("ALTER TABLE")
        || upper.starts_with("CREATE INDEX")
        || upper.starts_with("DROP INDEX")
    {
        return QueryResponse::Ok(0, 0);
    }
    // 缺口 c：DROP/TRUNCATE TABLE 真正清库（内存+磁盘段+倒排），对齐 MySQL 整表删除语义——
    // 修 sysbench cleanup / 反复 --init 后残留上轮行（基线不可比）。事务内 DROP 仍走
    // 上方"事务内暂不支持"1064（MySQL 隐式提交语义暂不实现）。
    // 缺口 c + §26 M1：DROP/TRUNCATE TABLE——默认表 documents = purge 全库（既有语义，
    // 兼容 c/sysbench cleanup）；其余表 = 清本表 docid 区间（多表单删，不影响他表）
    if upper.starts_with("DROP TABLE") || upper.starts_with("TRUNCATE TABLE") {
        let tid = table_id_for(&table_name_of(sql));
        if tid == 0 {
            return match engine.purge_all() {
                Ok(()) => QueryResponse::Ok(0, 0),
                Err(e) => QueryResponse::Err(3500, format!("清库失败（表数据未变）: {e}")),
            };
        }
        return match drop_table_range(engine, tid) {
            Ok(_) => QueryResponse::Ok(0, 0),
            Err(e) => QueryResponse::Err(3500, format!("清表失败: {e}")),
        };
    }
    QueryResponse::Err(1064, format!("syntax error: unsupported statement: {sql}"))
}
/// O 项第②步：语句读写分类——读语句（SELECT/SHOW/SET/USE/空/注释）走 RwLock **读锁**并行；
/// 其余（事务控制/INSERT/UPDATE/DELETE/DDL）走写锁互斥。
pub(crate) fn is_read_statement(sql: &str) -> bool {
    let s = sql.trim();
    if s.is_empty() || s.starts_with("--") {
        return true;
    }
    let upper = s.to_uppercase();
    upper.starts_with("SELECT")
        || upper.starts_with("SHOW")
        || upper.starts_with("SET")
        || upper.starts_with("USE")
}

/// Ex-9.1：读语句统一分发——非事务 `SELECT COUNT(*) WHERE f='v'`（f 已建倒排）走**写锁**执行
/// `inverted_doc_count`（需 flush pending 缓冲保证已提交写入可见，亚毫秒返回）；其余维持
/// 读锁读读并行（`dispatch_query_read`）。事务内读走原事务路径（快照语义不变）。
pub(crate) fn dispatch_query_read_opt(
    engine: &Arc<RwLock<Engine>>,
    sql: &str,
    session: &mut Session,
) -> QueryResponse {
    if session.txn.is_none() {
        if let Some((field, _)) = single_eq_count_field(sql) {
            let mut guard = engine.write().unwrap();
            if guard.inverted_count_eligible(&field) {
                if let Some(resp) = try_count_fast(&mut guard, sql) {
                    return resp;
                }
            }
        }
    }
    let guard = engine.read().unwrap();
    dispatch_query_read(&guard, sql, session)
}

/// O 项第②步：读锁分发（`&Engine`）——仅处理纯读语句（SELECT/SHOW/SET/USE/空）。
/// 事务内 SELECT 经 `txn_get`/`scan_range_txn`（已 `&self`），多连接快照读并行。
pub(crate) fn dispatch_query_read(engine: &Engine, sql: &str, session: &mut Session) -> QueryResponse {
    let upper = sql.trim().to_uppercase();
    // 空 / 注释
    if sql.trim().is_empty() || sql.trim().starts_with("--") {
        return QueryResponse::Ok(0, 0);
    }
    // ---- 事务内读语句 ----
    if session.txn.is_some() {
        if upper.starts_with("SELECT") {
            return txn_select(engine, session, sql);
        }
        if upper.starts_with("SET") {
            return QueryResponse::Ok(0, 0);
        }
        // 写语句不应走读锁（is_read_statement 已拦截），防御性拒绝
        return QueryResponse::Err(1064, format!("事务内暂不支持该语句: {sql}"));
    }
    // ---- 非事务读语句 ----
    if upper.starts_with("SET") {
        // 会话级 SET：支持 SET [SESSION] TRANSACTION ISOLATION LEVEL <level>；其余忽略
        if let Some(lv) = parse_isolation_level(&upper) {
            session.isolation = lv;
        }
        return QueryResponse::Ok(0, 0);
    }
    if upper.starts_with("SHOW") {
        return show_response(&upper);
    }
    if upper.starts_with("SELECT") {
        return select_response(engine, sql);
    }
    if upper.starts_with("USE") {
        return QueryResponse::Ok(0, 0);
    }
    QueryResponse::Err(1064, format!("syntax error: unsupported statement: {sql}"))
}
// ============ SQL 分发实现 ============

/// SHOW DATABASES / TABLES / VARIABLES 等系统查询。
pub(crate) fn show_response(upper: &str) -> QueryResponse {
    let upper = upper.trim();
    if upper.contains("DATABASES") {
        let columns = vec![column_payload("Database", MYSQL_TYPE_VAR_STRING, 45)];
        let rows = vec![vec![DEFAULT_DB.as_bytes().to_vec()]];
        return QueryResponse::Set { columns, rows };
    }
    if upper.contains("TABLES") {
        let columns = vec![column_payload(
            format!("Tables_in_{DEFAULT_DB}").as_str(),
            MYSQL_TYPE_VAR_STRING,
            45,
        )];
        let rows = vec![vec![DEFAULT_TABLE.as_bytes().to_vec()]];
        return QueryResponse::Set { columns, rows };
    }
    if upper.contains("VARIABLES") || upper.contains("STATUS") {
        let columns = vec![
            column_payload("Variable_name", MYSQL_TYPE_VAR_STRING, 45),
            column_payload("Value", MYSQL_TYPE_VAR_STRING, 45),
        ];
        let rows = vec![
            vec![b"version".to_vec(), SERVER_VERSION.as_bytes().to_vec()],
            vec![b"version_comment".to_vec(), b"shanshui-cunji".to_vec()],
            vec![b"character_set_server".to_vec(), b"utf8mb4".to_vec()],
        ];
        return QueryResponse::Set { columns, rows };
    }
    // 其他 SHOW → 空结果集
    let columns = vec![column_payload("", MYSQL_TYPE_VAR_STRING, 45)];
    QueryResponse::Set {
        columns,
        rows: Vec::new(),
    }
}
