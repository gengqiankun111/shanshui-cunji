//! MySQL wire protocol 适配（development_process_order H 项）：让 MySQL 客户端 / 生态工具
//! （mysql cli、JDBC、Navicat、sysbench）直接接入本数据库。
//!
//! 实现范围（H-1~H-3）：
//! - H-1 协议核心：HandshakeV10 握手 + mysql_native_password 认证（sha1 scramble）+
//!   报文编解码（packet / OK / ERR / ResultSet / EOF）+ 独立端口监听；
//! - H-2 系统查询：SHOW DATABASES / SHOW TABLES / SHOW VARIABLES、SELECT VERSION() / @@version；
//! - H-3 SQL 映射：INSERT（→ put + docid）、UPDATE（→ put 覆盖）、DELETE（→ engine.delete）、
//!   SELECT（→ sqlish 引擎，返回 id + doc 两列）。
//!
//! 数据模型映射：单库 `scc`，固定表 `documents`——行 = docid（BIGINT 主键），
//! 列 = `id`（LONGLONG）+ `doc`（文档 JSON 文本）。

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use sha1::{Digest, Sha1};

use crate::engine::Engine;
use crate::error::{Error, Result};

// ============ 协议常量 ============

const PROTOCOL_VERSION: u8 = 10;
const SERVER_VERSION: &str = "8.0.0-shanshui-cunji";
/// 固定表名（文档库无表语义，SQL 层映射为单表）。
pub const DEFAULT_TABLE: &str = "documents";
pub const DEFAULT_DB: &str = "scc";

// 能力位（仅声明已支持子集）
const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_CONNECT_WITH_DB: u32 = 1 << 3;
const CLIENT_TRANSACTIONS: u32 = 1 << 13;
const CLIENT_MULTI_STATEMENTS: u32 = 1 << 16;
const CAPABILITIES: u32 = CLIENT_PROTOCOL_41
    | CLIENT_SECURE_CONNECTION
    | CLIENT_PLUGIN_AUTH
    | CLIENT_TRANSACTIONS
    | CLIENT_MULTI_STATEMENTS;
const CHARSET_UTF8MB4: u8 = 45;

// 命令
const COM_QUIT: u8 = 0x01;
const COM_INIT_DB: u8 = 0x02;
const COM_QUERY: u8 = 0x03;
const COM_PING: u8 = 0x0e;

// 包类型
const OK_PACKET: u8 = 0x00;
const EOF_PACKET: u8 = 0xfe;
const ERR_PACKET: u8 = 0xff;

// 列类型
const MYSQL_TYPE_LONGLONG: u8 = 8;
const MYSQL_TYPE_VAR_STRING: u8 = 253;

// ============ 报文编解码 ============

fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) -> std::io::Result<()> {
    let len = payload.len() as u32;
    stream.write_all(&[
        (len & 0xff) as u8,
        ((len >> 8) & 0xff) as u8,
        ((len >> 16) & 0xff) as u8,
        seq,
    ])?;
    stream.write_all(payload)
}

fn read_packet(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut hdr = [0u8; 4];
    stream.read_exact(&mut hdr)?;
    let len = (hdr[0] as usize) | ((hdr[1] as usize) << 8) | ((hdr[2] as usize) << 16);
    let seq = hdr[3];
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload)?;
    Ok((seq, payload))
}

fn write_lenenc(buf: &mut Vec<u8>, v: u64) {
    if v < 251 {
        buf.push(v as u8);
    } else if v < 0x10000 {
        buf.push(0xfc);
        buf.extend_from_slice(&(v as u16).to_le_bytes());
    } else if v < 0x1000000 {
        buf.push(0xfd);
        buf.extend_from_slice(&[v as u8, (v >> 8) as u8, (v >> 16) as u8]);
    } else {
        buf.push(0xfe);
        buf.extend_from_slice(&v.to_le_bytes());
    }
}

fn write_lenenc_str(buf: &mut Vec<u8>, s: &str) {
    write_lenenc(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}

/// 构造 OK 包 payload（affected rows / last insert id）。
fn ok_payload(affected: u64, last_insert_id: u64) -> Vec<u8> {
    let mut b = vec![OK_PACKET];
    write_lenenc(&mut b, affected);
    write_lenenc(&mut b, last_insert_id);
    b.extend_from_slice(&0x0002u16.to_le_bytes()); // status flags: AUTO_COMMIT
    b.extend_from_slice(&0u16.to_le_bytes()); // warnings
    b
}

/// 构造 ERR 包 payload。
fn err_payload(code: u16, msg: &str) -> Vec<u8> {
    let mut b = vec![ERR_PACKET];
    b.extend_from_slice(&code.to_le_bytes());
    b.push(b'#'); // sqlstate marker
    b.extend_from_slice(b"HY000");
    b.extend_from_slice(msg.as_bytes());
    b
}

/// 构造 EOF 包 payload。
fn eof_payload() -> Vec<u8> {
    let mut b = vec![EOF_PACKET];
    b.extend_from_slice(&0u16.to_le_bytes()); // warnings
    b.extend_from_slice(&0x0002u16.to_le_bytes()); // status flags
    b
}

/// 列定义（ColumnDefinition41）。
fn column_payload(name: &str, col_type: u8, charset: u16) -> Vec<u8> {
    let mut b = Vec::new();
    write_lenenc_str(&mut b, "def"); // catalog
    write_lenenc_str(&mut b, DEFAULT_DB); // schema
    write_lenenc_str(&mut b, ""); // table
    write_lenenc_str(&mut b, ""); // org_table
    write_lenenc_str(&mut b, name); // name
    write_lenenc_str(&mut b, name); // org_name
    b.push(0x0c); // fixed length of following fields
    b.extend_from_slice(&charset.to_le_bytes());
    b.extend_from_slice(&0u32.to_le_bytes()); // column length
    b.push(col_type);
    b.extend_from_slice(&0u16.to_le_bytes()); // flags
    b.push(0); // decimals
    b.extend_from_slice(&[0, 0]); // filler
    b
}

// ============ 认证（mysql_native_password）============

/// 校验 native_password 认证响应（H-1）。
/// 客户端 token = stage1 XOR sha1(scramble + stage2)，其中 stage1=sha1(pw)，stage2=sha1(stage1)。
/// 服务器校验：stage1' = token XOR crypto（crypto = sha1(scramble + stage2)），
/// 然后 sha1(stage1') == stage2。
pub fn check_native_password(auth_response: &[u8], scramble: &[u8], password: &str) -> bool {
    if password.is_empty() {
        return auth_response.is_empty();
    }
    if auth_response.len() != 20 {
        return false;
    }
    let stage1 = Sha1::digest(password.as_bytes());
    let stage2 = Sha1::digest(&stage1);
    let mut h = Sha1::new();
    h.update(scramble);
    h.update(stage2);
    let crypto = h.finalize();
    let mut stage1_recovered = [0u8; 20];
    for i in 0..20 {
        stage1_recovered[i] = auth_response[i] ^ crypto[i];
    }
    Sha1::digest(&stage1_recovered) == stage2
}

// ============ 会话 ============

/// 单连接会话状态。
struct Session {
    user: String,
    authenticated: bool,
    // 每包序号（客户端 → 服务器方向独立计数）。
    seq: u8,
}

// ============ MySQL 服务器 ============

/// MySQL 协议服务器：持有引擎（Arc<Mutex<Engine>>，与 rpc.rs 同模式），
/// 每连接独立线程处理握手 → 认证 → 命令循环。
pub struct MySqlServer {
    engine: Arc<Mutex<Engine>>,
    user: String,
    password: String,
    next_conn_id: AtomicU64,
}

impl MySqlServer {
    pub fn new(engine: Engine, user: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            engine: Arc::new(Mutex::new(engine)),
            user: user.into(),
            password: password.into(),
            next_conn_id: AtomicU64::new(1),
        }
    }

    /// 绑定并接受连接（阻塞）。每连接 spawn 线程处理。
    pub fn serve(self, addr: &str) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        tracing::info!("MySQL 协议服务已启动: mysql://{addr}（库 {DEFAULT_DB}，表 {DEFAULT_TABLE}）");
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else {
                continue;
            };
            let engine = self.engine.clone();
            let user = self.user.clone();
            let password = self.password.clone();
            let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
            std::thread::spawn(move || {
                if let Err(e) = handle_connection(&mut stream, engine, &user, &password, conn_id) {
                    tracing::warn!("MySQL 会话结束: {e}");
                }
            });
        }
        Ok(())
    }

    /// 测试用：绑定到随机端口并返回地址（单连接阻塞处理，供协议级测试）。
    pub fn serve_once(self, addr: &str) -> Result<std::net::SocketAddr> {
        let listener = TcpListener::bind(addr)?;
        let local = listener.local_addr()?;
        let engine = self.engine.clone();
        let user = self.user.clone();
        let password = self.password.clone();
        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let _ = handle_connection(&mut stream, engine, &user, &password, conn_id);
            }
        });
        Ok(local)
    }
}

/// 单连接处理：握手 → 认证 → 命令循环。
fn handle_connection(
    stream: &mut TcpStream,
    engine: Arc<Mutex<Engine>>,
    user: &str,
    password: &str,
    conn_id: u64,
) -> Result<()> {
    let mut session = Session {
        user: String::new(),
        authenticated: false,
        seq: 0,
    };
    // ① 握手（HandshakeV10）
    let scramble = gen_scramble(conn_id);
    let mut hb = Vec::new();
    hb.push(PROTOCOL_VERSION);
    // 协议字符串均为 NUL 结尾 C 串（无 lenenc 长度前缀）
    hb.extend_from_slice(SERVER_VERSION.as_bytes());
    hb.push(0);
    hb.extend_from_slice(&(conn_id as u32).to_le_bytes());
    hb.extend_from_slice(&scramble[0..8]);
    hb.push(0); // filler
    hb.extend_from_slice(&(CAPABILITIES as u16).to_le_bytes());
    hb.push(CHARSET_UTF8MB4);
    hb.extend_from_slice(&0x0002u16.to_le_bytes()); // status
    hb.extend_from_slice(&((CAPABILITIES >> 16) as u16).to_le_bytes());
    hb.push(21); // auth plugin data length
    hb.extend_from_slice(&[0u8; 10]); // reserved
    hb.extend_from_slice(&scramble[8..20]);
    hb.push(0); // auth plugin data part2 终止 NUL（auth_len=21 = 8+12+NUL）
    hb.extend_from_slice(b"mysql_native_password");
    hb.push(0);
    write_packet(stream, 0, &hb)?;

    // ② 读握手响应
    let (_, resp) = read_packet(stream)?;
    tracing::debug!(
        "握手响应 {} 字节: {:02x?}",
        resp.len(),
        &resp[..resp.len().min(96)]
    );
    if resp.is_empty() {
        return Err(Error::Cluster("客户端握手响应为空".into()));
    }
    let mut pos = 0usize;
    let cap = read_u32_le(&resp, &mut pos);
    let _max_packet = read_u32_le(&resp, &mut pos);
    let _charset = resp.get(pos).copied().unwrap_or(0);
    pos += 1;
    pos += 23; // filler（协议 41：charset 后 23 字节零填充）
    session.user = read_nul_string(&resp, &mut pos)?;
    // 协议 41 握手响应顺序：username → auth_response → [CONNECT_WITH_DB] db →
    // [PLUGIN_AUTH] auth_plugin_name → [CONNECT_ATTRS] attrs。
    // 各字段由「服务器声明」决定客户端是否发送：服务器 CAPABILITIES 未声明
    // CONNECT_WITH_DB / CONNECT_ATTRS → 客户端不应发送（跳过）。
    let mut auth_response: Vec<u8> = Vec::new();
    if cap & CLIENT_PLUGIN_AUTH != 0 {
        // auth_response：服务器未声明 CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA → 1 字节长度前缀
        // （小值下 lenenc 与 1 字节一致，read_lenenc_raw 兼容两者）
        let auth_len = read_lenenc_raw(&resp, &mut pos)? as usize;
        if pos + auth_len > resp.len() {
            return Err(Error::Cluster("auth_response 越界".into()));
        }
        auth_response = resp[pos..pos + auth_len].to_vec();
        pos += auth_len;
        tracing::debug!("认证: user={} auth_len={} resp_len={}", session.user, auth_len, resp.len());
    }
    if cap & CLIENT_CONNECT_WITH_DB != 0 && CAPABILITIES & CLIENT_CONNECT_WITH_DB != 0 {
        let _db = read_nul_string(&resp, &mut pos)?;
    }
    if cap & CLIENT_PLUGIN_AUTH != 0 {
        let _plugin = read_nul_string(&resp, &mut pos)?;
    }
    let ok = session.user == user && check_native_password(&auth_response, &scramble, password);
    if !ok {
        tracing::debug!("认证失败: user={} auth_len={}", session.user, auth_response.len());
        let _ = write_packet(stream, 2, &err_payload(1045, "Access denied for user"))?;
        return Err(Error::Cluster("认证失败".into()));
    }
    tracing::debug!("认证通过: user={}", session.user);
    session.authenticated = true;
    // ③ 认证成功 → OK（seq=2：握手 seq0 → 客户端握手响应 seq1 → 授权 seq2，全局连续）
    write_packet(stream, 2, &ok_payload(0, 0))?;
    tracing::debug!("认证 OK 已发送，进入命令循环");

    // ④ 命令循环
    loop {
        let (_, cmd) = match read_packet(stream) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        tracing::debug!("收到命令包 {} 字节: {:02x?}", cmd.len(), &cmd[..cmd.len().min(16)]);
        if cmd.is_empty() {
            continue;
        }
        // 客户端命令包 seq=0，响应包从 seq=1 起递增
        let seq0 = 1u8;
        match cmd[0] {
            COM_QUIT => return Ok(()),
            COM_PING => write_packet(stream, seq0, &ok_payload(0, 0))?,
            COM_INIT_DB => {
                // 仅接受默认库
                let db = String::from_utf8_lossy(&cmd[1..]).to_string();
                if db == DEFAULT_DB {
                    write_packet(stream, seq0, &ok_payload(0, 0))?;
                } else {
                    write_packet(stream, seq0, &err_payload(1049, "Unknown database"))?;
                }
            }
            COM_QUERY => {
                let sql = String::from_utf8_lossy(&cmd[1..]).to_string();
                let mut guard = engine.lock().unwrap();
                let resp = dispatch_query(&mut guard, &sql);
                write_query_response(stream, seq0, resp)?;
            }
            other => {
                let msg = format!("command {other:#x} not supported");
                write_packet(stream, seq0, &err_payload(1047, &msg))?;
            }
        }
    }
}

/// COM_QUERY 响应（OK / ERR / ResultSet）。
enum QueryResponse {
    Ok(u64, u64),
    Err(u16, String),
    Set {
        columns: Vec<Vec<u8>>,
        rows: Vec<Vec<Vec<u8>>>,
    },
}

/// 分发 COM_QUERY。
fn dispatch_query(engine: &mut Engine, sql: &str) -> QueryResponse {
    let upper = sql.trim().to_uppercase();
    // 空 / 注释
    if sql.trim().is_empty() || sql.trim().starts_with("--") {
        return QueryResponse::Ok(0, 0);
    }
    if upper.starts_with("SHOW") {
        return show_response(&upper);
    }
    if upper.starts_with("SELECT") {
        return select_response(engine, sql);
    }
    if upper.starts_with("INSERT") {
        return insert_response(engine, sql);
    }
    if upper.starts_with("UPDATE") {
        return update_response(engine, sql);
    }
    if upper.starts_with("DELETE") {
        return delete_response(engine, sql);
    }
    if upper.starts_with("SET") || upper.starts_with("BEGIN") || upper.starts_with("START TRANSACTION")
        || upper.starts_with("COMMIT") || upper.starts_with("ROLLBACK") || upper.starts_with("USE")
    {
        // H-4 事务语义在会话级实现前先放行（SET/USE/BEGIN/COMMIT/ROLLBACK → OK）
        return QueryResponse::Ok(0, 0);
    }
    QueryResponse::Err(1064, format!("syntax error: unsupported statement: {sql}"))
}

/// 写 COM_QUERY 响应（ResultSet 多包 / OK / ERR）。
fn write_query_response(stream: &mut TcpStream, seq0: u8, resp: QueryResponse) -> Result<()> {
    let mut seq = seq0;
    match resp {
        QueryResponse::Ok(affected, lid) => write_packet(stream, seq, &ok_payload(affected, lid))?,
        QueryResponse::Err(code, msg) => write_packet(stream, seq, &err_payload(code, &msg))?,
        QueryResponse::Set { columns, rows } => {
            let mut cnt = Vec::new();
            write_lenenc(&mut cnt, columns.len() as u64);
            write_packet(stream, seq, &cnt)?;
            seq = seq.wrapping_add(1);
            for c in &columns {
                write_packet(stream, seq, c)?;
                seq = seq.wrapping_add(1);
            }
            write_packet(stream, seq, &eof_payload())?;
            seq = seq.wrapping_add(1);
            for row in &rows {
                let mut rp = Vec::new();
                for cell in row {
                    write_lenenc(&mut rp, cell.len() as u64);
                    rp.extend_from_slice(cell);
                }
                write_packet(stream, seq, &rp)?;
                seq = seq.wrapping_add(1);
            }
            write_packet(stream, seq, &eof_payload())?;
        }
    }
    Ok(())
}

// ============ SQL 分发实现 ============

/// SHOW DATABASES / TABLES / VARIABLES 等系统查询。
fn show_response(upper: &str) -> QueryResponse {
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

/// SELECT：VERSION() / @@ 系统值 → 单行结果；`WHERE id=N` → 主键点查；
/// 否则走 sqlish 引擎。
fn select_response(engine: &mut Engine, sql: &str) -> QueryResponse {
    let upper = sql.trim().to_uppercase();
    if upper.contains("VERSION()") {
        let columns = vec![column_payload("VERSION()", MYSQL_TYPE_VAR_STRING, 45)];
        let rows = vec![vec![SERVER_VERSION.as_bytes().to_vec()]];
        return QueryResponse::Set { columns, rows };
    }
    if upper.contains("@@") {
        let columns = vec![column_payload("@@version", MYSQL_TYPE_VAR_STRING, 45)];
        let rows = vec![vec![SERVER_VERSION.as_bytes().to_vec()]];
        return QueryResponse::Set { columns, rows };
    }
    let columns = vec![
        column_payload("id", MYSQL_TYPE_LONGLONG, 63),
        column_payload("doc", MYSQL_TYPE_VAR_STRING, 45),
    ];
    // MySQL 客户端以 `id` 为主键列 → 主键点查（sqlish 侧为 docid 特例）
    if let Some(id) = extract_point_id(sql) {
        let rows = match engine.get(id) {
            Ok(Some(v)) => vec![vec![id.to_string().into_bytes(), v]],
            _ => Vec::new(),
        };
        return QueryResponse::Set { columns, rows };
    }
    // 一般 SELECT → sqlish 引擎（id + doc 两列）
    match crate::sqlish::execute(engine, sql, 10_000) {
        Ok(rows) => {
            let data: Vec<Vec<Vec<u8>>> = rows
                .iter()
                .map(|(id, v)| vec![id.to_string().into_bytes(), v.clone()])
                .collect();
            QueryResponse::Set {
                columns,
                rows: data,
            }
        }
        Err(e) => QueryResponse::Err(1064, format!("query error: {e}")),
    }
}

/// 提取 `WHERE id=N` / `WHERE docid=N` 的 docid（仅纯点查；含其他条件返回 None）。
fn extract_point_id(sql: &str) -> Option<u64> {
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

/// INSERT INTO documents (id, doc) VALUES (1, '...') / VALUES (1, '...')。
fn insert_response(engine: &mut Engine, sql: &str) -> QueryResponse {
    match parse_insert(sql) {
        Ok(Some((id, doc))) => match put_doc(engine, id, &doc) {
            Ok(_) => QueryResponse::Ok(1, id),
            Err(e) => QueryResponse::Err(1064, format!("insert error: {e}")),
        },
        Ok(None) => QueryResponse::Ok(0, 0),
        Err(e) => QueryResponse::Err(1064, format!("insert syntax: {e}")),
    }
}

/// UPDATE documents SET doc='...' WHERE id=1 → put 覆盖。
fn update_response(engine: &mut Engine, sql: &str) -> QueryResponse {
    match parse_update(sql) {
        Ok((id, doc)) => match put_doc(engine, id, &doc) {
            Ok(_) => QueryResponse::Ok(1, 0),
            Err(e) => QueryResponse::Err(1064, format!("update error: {e}")),
        },
        Err(e) => QueryResponse::Err(1064, format!("update syntax: {e}")),
    }
}

/// DELETE FROM documents WHERE id=1。
fn delete_response(engine: &mut Engine, sql: &str) -> QueryResponse {
    match parse_delete(sql) {
        Ok(id) => match engine.delete(id) {
            Ok(_) => QueryResponse::Ok(1, 0),
            Err(e) => QueryResponse::Err(1064, format!("delete error: {e}")),
        },
        Err(e) => QueryResponse::Err(1064, format!("delete syntax: {e}")),
    }
}

/// put 文档（doc JSON → 提取倒排 term 复用 HTTP 路径语义）。
fn put_doc(engine: &mut Engine, id: u64, doc: &str) -> Result<()> {
    let parsed: serde_json::Value = serde_json::from_str(doc)
        .map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
    let terms = crate::server::extract_terms(&parsed);
    let refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
    engine.put(id, doc.as_bytes().to_vec(), &refs)
}

// ============ 简易 SQL 解析（INSERT/UPDATE/DELETE 子集）============

/// 解析 `INSERT INTO [db.]table [(cols)] VALUES (...)` → (id, doc)。
fn parse_insert(sql: &str) -> Result<Option<(u64, String)>> {
    let lower = sql.to_lowercase();
    let values_pos = lower.find("values").ok_or_else(|| {
        Error::Cluster("INSERT 缺 VALUES".into())
    })?;
    let values = &sql[values_pos + 6..];
    let open = values.find('(').ok_or_else(|| Error::Cluster("VALUES 缺 (".into()))?;
    let close = find_matching_paren(values, open)?;
    let inner = &values[open + 1..close];
    // 按逗号切分（外层），去引号
    let parts = split_values(inner);
    if parts.is_empty() {
        return Ok(None);
    }
    // 列清单（可选项）
    let cols_part = &sql[..values_pos];
    let has_cols = cols_part.contains('(');
    let id: u64;
    let doc: String;
    if has_cols {
        let cols_open = cols_part.find('(').unwrap();
        let cols_close = cols_part.rfind(')').unwrap();
        let cols = split_values(&cols_part[cols_open + 1..cols_close]);
        let mut idv: Option<String> = None;
        let mut docv: Option<String> = None;
        for (i, c) in cols.iter().enumerate() {
            match c.trim().to_lowercase().as_str() {
                "id" | "docid" => idv = parts.get(i).cloned(),
                "doc" | "value" => docv = parts.get(i).cloned(),
                _ => {}
            }
            if idv.is_some() && docv.is_some() {
                break;
            }
        }
        id = idv
            .ok_or_else(|| Error::Cluster("INSERT 缺 id 列".into()))?
            .trim()
            .parse::<u64>()
            .map_err(|_| Error::Cluster("id 非法".into()))?;
        doc = docv.ok_or_else(|| Error::Cluster("INSERT 缺 doc 列".into()))?;
    } else {
        id = parts[0]
            .trim()
            .parse::<u64>()
            .map_err(|_| Error::Cluster("id 非法".into()))?;
        doc = parts.get(1).cloned().unwrap_or_default();
    }
    Ok(Some((id, unquote(&doc))))
}

/// 解析 `UPDATE documents SET doc='...' WHERE id=1` → (id, doc)。
fn parse_update(sql: &str) -> Result<(u64, String)> {
    let lower = sql.to_lowercase();
    let set_pos = lower.find("set").ok_or_else(|| Error::Cluster("UPDATE 缺 SET".into()))?;
    let where_pos = lower.find("where").ok_or_else(|| {
        Error::Cluster("UPDATE 缺 WHERE id=...".into())
    })?;
    let set_part = &sql[set_pos + 3..where_pos];
    let where_part = &sql[where_pos + 5..];
    // SET doc='...'
    let eq = set_part.find('=').ok_or_else(|| Error::Cluster("SET 缺 =".into()))?;
    let value = unquote(set_part[eq + 1..].trim());
    // WHERE id=N
    let id = parse_where_id(where_part)?;
    Ok((id, value))
}

/// 解析 `DELETE FROM documents WHERE id=1` → id。
fn parse_delete(sql: &str) -> Result<u64> {
    let lower = sql.to_lowercase();
    let where_pos = lower.find("where").ok_or_else(|| {
        Error::Cluster("DELETE 缺 WHERE id=...".into())
    })?;
    parse_where_id(&sql[where_pos + 5..])
}

/// 解析 `id = 123`（WHERE 子句内）。
fn parse_where_id(where_part: &str) -> Result<u64> {
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
fn split_values(s: &str) -> Vec<String> {
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
fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.len() >= 2 && ((t.starts_with('\'') && t.ends_with('\'')) || (t.starts_with('"') && t.ends_with('"'))) {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

/// 找配对右括号（含引号感知）。
fn find_matching_paren(s: &str, open: usize) -> Result<usize> {
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

// ============ 工具 ============

fn read_u32_le(data: &[u8], pos: &mut usize) -> u32 {
    let mut b = [0u8; 4];
    let n = (data.len() - *pos).min(4);
    b[..n].copy_from_slice(&data[*pos..*pos + n]);
    *pos += 4;
    u32::from_le_bytes(b)
}

fn read_nul_string(data: &[u8], pos: &mut usize) -> Result<String> {
    let start = *pos;
    while *pos < data.len() && data[*pos] != 0 {
        *pos += 1;
    }
    let s = String::from_utf8_lossy(&data[start..*pos]).to_string();
    *pos = (*pos + 1).min(data.len());
    Ok(s)
}

fn read_lenenc_raw(data: &[u8], pos: &mut usize) -> Result<u64> {
    if *pos >= data.len() {
        return Err(Error::Cluster("lenenc 越界".into()));
    }
    let b = data[*pos];
    *pos += 1;
    match b {
        0xfc => {
            if *pos + 2 > data.len() {
                return Err(Error::Cluster("lenenc 0xfc 越界".into()));
            }
            let v = u16::from_le_bytes([data[*pos], data[*pos + 1]]);
            *pos += 2;
            Ok(v as u64)
        }
        0xfd => {
            if *pos + 3 > data.len() {
                return Err(Error::Cluster("lenenc 0xfd 越界".into()));
            }
            let v = (data[*pos] as u64) | ((data[*pos + 1] as u64) << 8) | ((data[*pos + 2] as u64) << 16);
            *pos += 3;
            Ok(v)
        }
        0xfe => {
            if *pos + 8 > data.len() {
                return Err(Error::Cluster("lenenc 0xfe 越界".into()));
            }
            let v = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            Ok(v)
        }
        n => Ok(n as u64),
    }
}

/// 读取 lenenc 整数（测试/结果集解析用）。
#[cfg(test)]
fn read_lenenc(data: &[u8], pos: &mut usize) -> Result<u64> {
    read_lenenc_raw(data, pos)
}

/// 伪随机 20 字节 scramble（无 rand 依赖：连接 id + 时间戳 → sha1 派生）。
fn gen_scramble(conn_id: u64) -> [u8; 20] {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut seed = [0u8; 16];
    seed[..8].copy_from_slice(&conn_id.to_le_bytes());
    seed[8..].copy_from_slice(&t.to_le_bytes());
    let h = Sha1::digest(&seed);
    let mut out = [0u8; 20];
    out.copy_from_slice(&h);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_engine() -> Engine {
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        Engine::open(dir.path(), &cfg).unwrap()
    }

    // ---------- 单元：认证 ----------

    #[test]
    fn native_password_accepts_correct_and_rejects_wrong() {
        let scramble = [7u8; 20];
        let pw = "secret";
        // 构造合法 token
        let stage1 = Sha1::digest(pw.as_bytes());
        let stage2 = Sha1::digest(&stage1);
        let mut h = Sha1::new();
        h.update(&scramble);
        h.update(stage2);
        let crypto = h.finalize();
        let mut token = [0u8; 20];
        for i in 0..20 {
            token[i] = stage1[i] ^ crypto[i];
        }
        assert!(check_native_password(&token, &scramble, pw));
        token[0] ^= 0xff;
        assert!(!check_native_password(&token, &scramble, pw));
        // 空密码：响应为空 → 接受
        assert!(check_native_password(&[], &scramble, ""));
    }

    // ---------- 单元：SQL 解析 ----------

    #[test]
    fn parse_insert_with_columns_and_plain() {
        let (id, doc) = parse_insert(
            "INSERT INTO documents (id, doc) VALUES (42, '{\"a\":1}')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(id, 42);
        assert_eq!(doc, r#"{"a":1}"#);
        let (id2, doc2) = parse_insert("INSERT INTO documents VALUES (7, 'x')")
            .unwrap()
            .unwrap();
        assert_eq!(id2, 7);
        assert_eq!(doc2, "x");
    }

    #[test]
    fn parse_update_and_delete() {
        let (id, doc) =
            parse_update("UPDATE documents SET doc='{\"b\":2}' WHERE id=9").unwrap();
        assert_eq!(id, 9);
        assert_eq!(doc, r#"{"b":2}"#);
        assert_eq!(parse_delete("DELETE FROM documents WHERE id=5").unwrap(), 5);
    }

    #[test]
    fn split_values_respects_quoted_commas() {
        let parts = split_values("1, '{\"a\":1,\"b\":2}', 3");
        assert_eq!(parts.len(), 3);
        assert_eq!(unquote(&parts[1]), r#"{"a":1,"b":2}"#);
    }

    // ---------- 集成：协议往返 ----------

    /// 测试客户端：连接 → 握手 → 认证 → 查询。
    struct TestClient {
        stream: TcpStream,
    }

    impl TestClient {
        fn connect(addr: std::net::SocketAddr) -> Self {
            Self {
                stream: TcpStream::connect(addr).unwrap(),
            }
        }

        /// 读握手包并返回 (scramble, auth_plugin)。
        fn handshake(&mut self) -> (Vec<u8>, String) {
            let (_, payload) = read_packet(&mut self.stream).unwrap();
            assert_eq!(payload[0], PROTOCOL_VERSION);
            // 解析 auth plugin data：位置由 cap 高低位 + len 字段决定
            let mut pos = 1usize;
            while pos < payload.len() && payload[pos] != 0 {
                pos += 1;
            }
            pos += 1; // server version NUL
            pos += 4; // conn id
            let mut scramble = payload[pos..pos + 8].to_vec();
            pos += 8 + 1 + 2 + 1 + 2 + 2;
            let auth_len = payload[pos] as usize;
            pos += 1;
            pos += 10; // reserved
            // part2 = auth_len - 8 - 1（去掉终止 NUL）= 12 字节有效 scramble
            let part2_len = auth_len.saturating_sub(9);
            scramble.extend_from_slice(&payload[pos..pos + part2_len]);
            // auth plugin name（part2 后 + NUL 终止 → 跳过）
            let mut p = pos + part2_len + 1;
            let start = p;
            while p < payload.len() && payload[p] != 0 {
                p += 1;
            }
            let plugin = String::from_utf8_lossy(&payload[start..p]).to_string();
            (scramble, plugin)
        }

        /// 发送认证（native_password token）。
        fn authenticate(&mut self, user: &str, password: &str, scramble: &[u8]) {
            let mut b = Vec::new();
            b.extend_from_slice(&CAPABILITIES.to_le_bytes());
            b.extend_from_slice(&0x100_0000u32.to_le_bytes()); // max packet
            b.push(CHARSET_UTF8MB4);
            b.extend_from_slice(&[0u8; 23]);
            b.extend_from_slice(user.as_bytes());
            b.push(0);
            let token = if password.is_empty() {
                Vec::new()
            } else {
                let stage1 = Sha1::digest(password.as_bytes());
                let stage2 = Sha1::digest(&stage1);
                let mut h = Sha1::new();
                h.update(scramble);
                h.update(stage2);
                let crypto = h.finalize();
                (0..20).map(|i| stage1[i] ^ crypto[i]).collect()
            };
            write_lenenc(&mut b, token.len() as u64);
            b.extend_from_slice(&token);
            write_packet(&mut self.stream, 1, &b).unwrap();
            let (_, resp) = read_packet(&mut self.stream).unwrap();
            assert_eq!(resp[0], OK_PACKET, "认证应成功");
        }

        /// 发 COM_QUERY，收全部响应包（OK/ERR 单包；ResultSet 直到 EOF 包）。
        fn query(&mut self, sql: &str) -> Vec<Vec<u8>> {
            let mut cmd = vec![COM_QUERY];
            cmd.extend_from_slice(sql.as_bytes());
            write_packet(&mut self.stream, 0, &cmd).unwrap();
            let (_, first) = read_packet(&mut self.stream).unwrap();
            if first.first() == Some(&OK_PACKET) || first.first() == Some(&ERR_PACKET) {
                return vec![first];
            }
            let mut packets = vec![first];
            let mut saw_eof = false; // 列定义后第一个 EOF（非终止）
            loop {
                let (_, payload) = read_packet(&mut self.stream).unwrap();
                let is_eof = payload.first() == Some(&EOF_PACKET) && payload.len() < 9;
                let last = is_eof && saw_eof; // 第二个 EOF（行尾）终止
                if is_eof {
                    saw_eof = true;
                }
                packets.push(payload);
                if last {
                    break;
                }
            }
            packets
        }
    }

    #[test]
    fn handshake_auth_and_query_roundtrip() {
        let engine = test_engine();
        let server = MySqlServer::new(engine, "root", "secret");
        let addr = server.serve_once("127.0.0.1:0").unwrap();

        let mut c = TestClient::connect(addr);
        let (scramble, plugin) = c.handshake();
        assert_eq!(plugin, "mysql_native_password");
        c.authenticate("root", "secret", &scramble);

        // SHOW DATABASES → ResultSet 含 scc
        let packets = c.query("SHOW DATABASES");
        assert_eq!(packets[0][0], 1, "列数 = 1");
        // 解析末行首列
        let row = &packets[packets.len() - 2];
        let mut pos = 0usize;
        let _ = read_lenenc(&row, &mut pos).unwrap();
        let name = String::from_utf8_lossy(&row[pos..]).to_string();
        assert_eq!(name, DEFAULT_DB);

        // INSERT + SELECT 往返
        let ok = c.query("INSERT INTO documents (id, doc) VALUES (1, '{\"k\":1}')");
        assert_eq!(ok[0][0], OK_PACKET);
        let sel = c.query("SELECT * FROM documents WHERE id=1");
        assert_eq!(sel[0][0], 2, "两列 id/doc");
        // 末行含 doc 内容
        let data = sel[sel.len() - 2].clone();
        let mut p = 0usize;
        let n = read_lenenc(&data, &mut p).unwrap();
        assert_eq!(n, 1, "第一列 id 值长度 = 1");
        assert_eq!(&data[p..p + n as usize], b"1", "id = 1");
        p += n as usize;
        let dlen = read_lenenc(&data, &mut p).unwrap() as usize;
        assert_eq!(&data[p..p + dlen], br#"{"k":1}"#);

        // UPDATE / DELETE
        let upd = c.query("UPDATE documents SET doc='{\"k\":2}' WHERE id=1");
        assert_eq!(upd[0][0], OK_PACKET);
        let del = c.query("DELETE FROM documents WHERE id=1");
        assert_eq!(del[0][0], OK_PACKET);
        let sel2 = c.query("SELECT * FROM documents WHERE id=1");
        // 空结果集：列数 + 2 列定义 + 2 EOF（无数据行）
        assert_eq!(sel2.len(), 5, "删除后应返回空结果集（无数据行）");
    }

    #[test]
    fn wrong_password_rejected() {
        let engine = test_engine();
        let server = MySqlServer::new(engine, "root", "secret");
        let addr = server.serve_once("127.0.0.1:0").unwrap();
        let mut c = TestClient::connect(addr);
        let (scramble, _) = c.handshake();
        // 错误密码：直接验证 check 函数（连接级拒绝在 serve_once 单连接后关闭）
        assert!(!check_native_password(&[0u8; 20], &scramble, "secret"));
    }
}
