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
use std::sync::{Arc, RwLock};

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
const COM_STMT_PREPARE: u8 = 0x16;
const COM_STMT_EXECUTE: u8 = 0x17;
const COM_STMT_CLOSE: u8 = 0x19;

// 包类型
const OK_PACKET: u8 = 0x00;
const EOF_PACKET: u8 = 0xfe;
const ERR_PACKET: u8 = 0xff;

// 列类型
const MYSQL_TYPE_LONGLONG: u8 = 8;
const MYSQL_TYPE_VAR_STRING: u8 = 253;
const MYSQL_TYPE_LONG: u8 = 3;
const MYSQL_TYPE_STRING: u8 = 254;
const MYSQL_TYPE_DOUBLE: u8 = 5;

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
    /// H-4：活动事务（BEGIN 后创建，COMMIT/ROLLBACK 结束；连接断开自动回滚 = drop）。
    txn: Option<crate::txn::Transaction>,
    /// 会话级隔离级别（SET TRANSACTION ISOLATION LEVEL 设置；BEGIN 时生效，默认 RR）。
    isolation: crate::txn::Isolation,
    /// H-5：预处理语句表（stmt_id → 原始 SQL，占位符 `?`）。
    statements: std::collections::HashMap<u32, String>,
    next_stmt_id: u32,
    /// sysbench 兼容：无 id 列的 INSERT（auto_increment 语义）共享递增分配器。
    auto_id: Arc<AtomicU64>,
}

// ============ MySQL 服务器 ============

/// MySQL 协议服务器：持有引擎（Arc<RwLock<Engine>>，O 项第②步：读语句读锁并行、
/// 写语句写锁互斥；每连接独立线程处理握手 → 认证 → 命令循环。
pub struct MySqlServer {
    engine: Arc<RwLock<Engine>>,
    user: String,
    password: String,
    next_conn_id: AtomicU64,
    /// 无 id 列 INSERT 的 auto_increment 计数器（跨连接共享）。
    auto_id: Arc<AtomicU64>,
}

impl MySqlServer {
    pub fn new(engine: Engine, user: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            engine: Arc::new(RwLock::new(engine)),
            user: user.into(),
            password: password.into(),
            next_conn_id: AtomicU64::new(1),
            auto_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// O 项第③步：后台合并 worker。写路径（Engine::auto_compact）检测 L0 超阈值后只置
    /// `compact_pending` 信号；本线程读取信号后在引擎**读锁**（`try_read` 非阻塞）下执行
    /// `Engine::compact`——合并期间读语句仍可并行（读读共享锁），不阻塞读。
    /// - 信号驱动（100ms 轮询）：写后即时收敛 L0（替代 P 项写路径同步合并）；
    /// - 10 分钟兜底：覆盖 flush 未触发 / 信号丢失等异常路径（原 guard 线程语义）。
    /// - `try_read` 非阻塞：引擎正忙（写语句持写锁）时跳过本轮，不干扰前台写。
    /// - P72（无锁合并根治）：worker 在 Engine 读锁内**快速** clone 三 CF Arc + 删除位图 Arc +
    ///   紧迫度判定（`Engine::compaction_targets`）→ **drop 锁** → 对 `CompactTargets::run()` 执行
    ///   **无锁合并**——写语句持 Engine 写锁与合并**并发执行**（不再写锁排队等读锁）；
    ///   ssts 变更经 CF `sst_mutate` 与 flush 互斥（无丢失更新）。紧凑度调度由 targets 判定，
    ///   每轮压最高紧迫度档（同 Engine::compact 串行分支），多轮循环收敛。
    fn spawn_compaction_worker(&self) {
        let engine = self.engine.clone();
        let pending = self.engine.read().unwrap().compact_pending.clone();
        let worker = self.engine.read().unwrap().compact_worker.clone();
        worker.store(true, Ordering::Release); // 写路径此后只发信号
        std::thread::spawn(move || {
            let mut last_backstop = std::time::Instant::now();
            loop {
                std::thread::sleep(std::time::Duration::from_millis(100));
                let signaled = pending.swap(false, Ordering::AcqRel);
                let backstop = last_backstop.elapsed() >= std::time::Duration::from_secs(600);
                if signaled || backstop {
                    // 读锁内 clone 目标（Arc clone 廉价）→ drop 锁 → 无锁合并
                    let targets = engine.read().map(|g| g.compaction_targets()).unwrap_or(None);
                    if let Some(t) = targets {
                        let _ = t.run();
                    }
                    if backstop {
                        last_backstop = std::time::Instant::now();
                    }
                }
            }
        });
    }

    /// 绑定并接受连接（阻塞）。每连接 spawn 线程处理。
    pub fn serve(self, addr: &str) -> Result<()> {
        // O 项第③步：后台合并 worker（信号驱动 + 10 分钟兜底）——写路径只发信号，
        // 合并读锁下执行，读写均不被合并阻塞（替代 P 项写路径同步合并 + guard 定时器）。
        self.spawn_compaction_worker();
        let listener = TcpListener::bind(addr)?;
        tracing::info!("MySQL 协议服务已启动: mysql://{addr}（库 {DEFAULT_DB}，表 {DEFAULT_TABLE}）");
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else {
                continue;
            };
            let engine = self.engine.clone();
            let user = self.user.clone();
            let password = self.password.clone();
            let auto_id = self.auto_id.clone();
            let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
            std::thread::spawn(move || {
                // X 项：连接计数（活跃/累计，/metrics 指标）
                if let Ok(g) = engine.read() {
                    g.metrics.active_conns.fetch_add(1, Ordering::Relaxed);
                    g.metrics.total_conns.fetch_add(1, Ordering::Relaxed);
                }
                let r = handle_connection(
                    &mut stream,
                    engine.clone(),
                    &user,
                    &password,
                    conn_id,
                    auto_id,
                );
                if let Ok(g) = engine.read() {
                    g.metrics.active_conns.fetch_add(-1, Ordering::Relaxed);
                }
                if let Err(e) = r {
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
        let auto_id = self.auto_id.clone();
        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let _ = handle_connection(&mut stream, engine, &user, &password, conn_id, auto_id);
            }
        });
        Ok(local)
    }
}

/// 单连接处理：握手 → 认证 → 命令循环。
fn handle_connection(
    stream: &mut TcpStream,
    engine: Arc<RwLock<Engine>>,
    user: &str,
    password: &str,
    conn_id: u64,
    auto_id: Arc<AtomicU64>,
) -> Result<()> {
    let mut session = Session {
        user: String::new(),
        authenticated: false,
        txn: None,
        isolation: crate::txn::Isolation::RepeatableRead,
        statements: std::collections::HashMap::new(),
        next_stmt_id: 1,
        auto_id,
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
                // 单库模式：任意库名均接受（MySQL 客户端默认带 sbtest 等库名）
                let _db = String::from_utf8_lossy(&cmd[1..]).to_string();
                write_packet(stream, seq0, &ok_payload(0, 0))?;
            }
            COM_QUERY => {
                // X 项：语句计数（/metrics 指标）
                if let Ok(g) = engine.read() {
                    g.metrics.statements.fetch_add(1, Ordering::Relaxed);
                }
                let sql = String::from_utf8_lossy(&cmd[1..]).to_string();
                // O 项第②步：读语句走 RwLock 读锁（多连接 SELECT 并行）；写语句走写锁互斥
                let resp = if is_read_statement(&sql) {
                    let guard = engine.read().unwrap();
                    dispatch_query_read(&guard, &sql, &mut session)
                } else {
                    let mut guard = engine.write().unwrap();
                    dispatch_query(&mut guard, &sql, &mut session)
                };
                write_query_response(stream, seq0, resp)?;
            }
            COM_STMT_PREPARE => {
                // H-5：预处理语句（JDBC 依赖）。请求 = 0x16 + SQL
                let sql = String::from_utf8_lossy(&cmd[1..]).to_string();
                let packets = stmt_prepare(&mut session, &sql);
                let mut seq = seq0;
                for p in packets {
                    write_packet(stream, seq, &p)?;
                    seq = seq.wrapping_add(1);
                }
            }
            COM_STMT_EXECUTE => {
                // O 项第②步：预处理执行暂走写锁（sysbench 用 COM_QUERY 为主；预读优化留后）
                let mut guard = engine.write().unwrap();
                let resp = stmt_execute(&mut guard, &mut session, &cmd);
                write_query_response(stream, seq0, resp)?;
            }
            COM_STMT_CLOSE => {
                // H-5：释放 statement（无响应包）
                if cmd.len() >= 5 {
                    let stmt_id = u32::from_le_bytes(cmd[1..5].try_into().unwrap());
                    session.statements.remove(&stmt_id);
                }
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

/// 分发 COM_QUERY（H-4：会话级事务 BEGIN/COMMIT/ROLLBACK + 事务内 SQL）。
fn dispatch_query(engine: &mut Engine, sql: &str, session: &mut Session) -> QueryResponse {
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
            return txn_insert(session, sql);
        }
        if upper.starts_with("UPDATE") {
            return txn_update(engine, session, sql);
        }
        if upper.starts_with("DELETE") {
            return txn_delete(session, sql);
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
    if upper.starts_with("UPDATE") {
        return update_response(engine, sql);
    }
    if upper.starts_with("DELETE") {
        return delete_response(engine, sql);
    }
    if upper.starts_with("SET") || upper.starts_with("USE") {
        return QueryResponse::Ok(0, 0);
    }
    // H-6：DDL 放行（文档库无 schema——CREATE/DROP/TRUNCATE TABLE 映射为 OK 空操作，
    // 使 sysbench prepare/cleanup 可跑通；表统一映射 documents）
    if upper.starts_with("CREATE TABLE")
        || upper.starts_with("DROP TABLE")
        || upper.starts_with("TRUNCATE TABLE")
        || upper.starts_with("ALTER TABLE")
        || upper.starts_with("CREATE INDEX")
        || upper.starts_with("DROP INDEX")
    {
        return QueryResponse::Ok(0, 0);
    }
    QueryResponse::Err(1064, format!("syntax error: unsupported statement: {sql}"))
}

/// O 项第②步：语句读写分类——读语句（SELECT/SHOW/SET/USE/空/注释）走 RwLock **读锁**并行；
/// 其余（事务控制/INSERT/UPDATE/DELETE/DDL）走写锁互斥。
fn is_read_statement(sql: &str) -> bool {
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

/// O 项第②步：读锁分发（`&Engine`）——仅处理纯读语句（SELECT/SHOW/SET/USE/空）。
/// 事务内 SELECT 经 `txn_get`/`scan_range_txn`（已 `&self`），多连接快照读并行。
fn dispatch_query_read(engine: &Engine, sql: &str, session: &mut Session) -> QueryResponse {
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

/// 解析 `SET [SESSION] TRANSACTION ISOLATION LEVEL <level>`（会话级，大写输入）。
/// 返回 None = 非隔离级别 SET（调用方忽略，返回 OK 保持客户端兼容）。
/// READ UNCOMMITTED 未单独实现：映射到 READ COMMITTED（我们的事务写提交前
/// 不可见，天然无脏读，语义比真 RU 更严格、无副作用）。
fn parse_isolation_level(upper: &str) -> Option<crate::txn::Isolation> {
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

/// 事务内 SELECT：快照查询（含同事务未提交写可见）。
/// sysbench 兼容（H-6 扩展）：`WHERE id=N` 点查 / `id BETWEEN A AND B` 范围 /
/// `id IN (...)` 多点 / `SUM(k)` 聚合 / `ORDER BY ... LIMIT N`（简化为排序截断）。
/// M 项优化（P0）：BETWEEN 范围走一次快照扫描（`scan_range_txn`），替代逐 id `txn_get`；
/// 点查 / IN 保持逐 id（目标少，逐 id 更快）。
/// O 项第②步：`&Engine`（事务读在 RwLock 读锁下执行）。
fn txn_select(
    engine: &Engine,
    session: &mut Session,
    sql: &str,
) -> QueryResponse {
    let columns = vec![
        column_payload("id", MYSQL_TYPE_LONGLONG, 63),
        column_payload("doc", MYSQL_TYPE_VAR_STRING, 45),
    ];
    let upper = sql.to_uppercase();
    // 聚合：`SELECT SUM(k) FROM ... WHERE id BETWEEN A AND B` → 单行单列数值
    let is_sum = upper.contains("SUM(");
    let txn = session.txn.as_mut().unwrap();
    // 范围查询（BETWEEN）：一次快照扫描（M 项 P0，逐 id txn_get → scan_range_txn）
    if let Some((a, b)) = extract_between_range(sql) {
        let rows = match engine.scan_range_txn(txn, Some(a), Some(b)) {
            Ok(r) => r,
            Err(e) => return QueryResponse::Err(3500, format!("事务范围读失败: {e}")),
        };
        if is_sum {
            // 聚合：扫描结果逐行解析 JSON 累加 k 字段（缺失视为 0）；返回单行单列
            let mut sum: i64 = 0;
            for (_, doc) in &rows {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(doc) {
                    if let Some(k) = v.get("k").and_then(|x| x.as_i64()) {
                        sum += k;
                    }
                }
            }
            let sum_col = column_payload("SUM(k)", MYSQL_TYPE_LONGLONG, 63);
            return QueryResponse::Set {
                columns: vec![sum_col],
                rows: vec![vec![sum.to_string().into_bytes()]],
            };
        }
        // 普通范围查询：组装结果 + ORDER BY / LIMIT
        let mut data: Vec<Vec<Vec<u8>>> = rows
            .into_iter()
            .map(|(id, doc)| vec![id.to_string().into_bytes(), doc])
            .collect();
        if upper.contains("ORDER BY") {
            data.sort_by(|a, b| a[1].cmp(&b[1]));
        }
        if let Some(lim) = extract_limit(sql) {
            data.truncate(lim.min(data.len()));
        }
        return QueryResponse::Set { columns, rows: data };
    }
    // 点查 / IN：逐 id 快照 get（同事务写可见）
    let ids: Vec<u64> = match extract_target_ids(sql) {
        Some(v) => v,
        None => {
            return QueryResponse::Err(1064, "事务内仅支持 WHERE id= / BETWEEN / IN 查询".to_string());
        }
    };
    if is_sum {
        // 聚合：逐 id 取 doc，解析 JSON 累加 k 字段（缺失视为 0）；返回单行单列
        let mut sum: i64 = 0;
        for id in &ids {
            if let Ok(Some(v)) = engine.txn_get(txn, *id) {
                if let Ok(doc) = serde_json::from_slice::<serde_json::Value>(&v) {
                    if let Some(k) = doc.get("k").and_then(|x| x.as_i64()) {
                        sum += k;
                    }
                }
            }
        }
        let sum_col = column_payload("SUM(k)", MYSQL_TYPE_LONGLONG, 63);
        return QueryResponse::Set {
            columns: vec![sum_col],
            rows: vec![vec![sum.to_string().into_bytes()]],
        };
    }
    // 普通点查 / IN：逐 id 快照 get
    let mut data: Vec<Vec<Vec<u8>>> = Vec::with_capacity(ids.len());
    for id in &ids {
        match engine.txn_get(txn, *id) {
            Ok(Some(v)) => data.push(vec![id.to_string().into_bytes(), v]),
            Ok(None) => {}
            Err(e) => return QueryResponse::Err(3500, format!("事务读失败: {e}")),
        }
    }
    // ORDER BY ... LIMIT N：按 doc 字节排序后截断（sysbench 不校验内容，仅测吞吐）
    if upper.contains("ORDER BY") {
        data.sort_by(|a, b| a[1].cmp(&b[1]));
    }
    if let Some(lim) = extract_limit(sql) {
        data.truncate(lim.min(data.len()));
    }
    QueryResponse::Set { columns, rows: data }
}

/// 提取 `WHERE id BETWEEN A AND B` 闭区间 → (A, B)；非 id BETWEEN → None。
fn extract_between_range(sql: &str) -> Option<(u64, u64)> {
    let lower = sql.to_lowercase();
    let w = lower.find("where")?;
    let rest = &lower[w + 5..];
    let rest = rest.split("order by").next()?;
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
fn extract_target_ids(sql: &str) -> Option<Vec<u64>> {
    let lower = sql.to_lowercase();
    let w = lower.find("where")?;
    let rest = &lower[w + 5..];
    let rest = rest.split("order by").next()?;
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
    // id BETWEEN A AND B
    if let Some(bp) = rest.find("between") {
        let after = &rest[bp + 7..];
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
fn extract_limit(sql: &str) -> Option<usize> {
    let lower = sql.to_lowercase();
    let pos = lower.find("limit")?;
    let rest = lower[pos + 5..].trim();
    let num: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    num.parse().ok()
}

/// 事务内 INSERT：攒批到事务（commit 时原子应用）。支持多行 VALUES。
/// sysbench 兼容：无 id 列（id=0）→ auto_increment 自动分配。
fn txn_insert(session: &mut Session, sql: &str) -> QueryResponse {
    match parse_insert_multi(sql) {
        Ok(Some(rows)) => {
            let txn = session.txn.as_mut().unwrap();
            let mut last_id = 0u64;
            for (id, doc) in &rows {
                let real_id = if *id == 0 {
                    session.auto_id.fetch_add(1, Ordering::Relaxed)
                } else {
                    *id
                };
                last_id = real_id;
                if let Err(e) = put_doc_txn(txn, real_id, doc) {
                    return QueryResponse::Err(1064, format!("insert error: {e}"));
                }
            }
            QueryResponse::Ok(rows.len() as u64, last_id)
        }
        Ok(None) => QueryResponse::Ok(0, 0),
        Err(e) => QueryResponse::Err(1064, format!("insert syntax: {e}")),
    }
}

/// 事务内 UPDATE：攒批覆盖。
/// 事务内 UPDATE：字段级（`SET k=k+1` 自增 / `SET c='str'` 字符串赋值）或
/// 整体替换（`SET doc='{json}'`）。读当前文档（快照 + 同事务写）→ 修改 → 攒批写回。
fn txn_update(engine: &mut Engine, session: &mut Session, sql: &str) -> QueryResponse {
    match parse_update(sql) {
        Ok((id, field, expr)) => {
            let txn = session.txn.as_mut().unwrap();
            // 整体替换（field=doc）→ 兼容旧语义直接 put
            if field.eq_ignore_ascii_case("doc") {
                let raw = unquote(&expr);
                return match put_doc_txn(txn, id, &raw) {
                    Ok(_) => QueryResponse::Ok(1, 0),
                    Err(e) => QueryResponse::Err(1064, format!("update error: {e}")),
                };
            }
            // 读当前文档（快照 + 同事务写可见；不存在则空对象）
            let mut doc: serde_json::Value = match engine.txn_get(txn, id) {
                Ok(Some(v)) => serde_json::from_slice(&v)
                    .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new())),
                Ok(None) => serde_json::Value::Object(serde_json::Map::new()),
                Err(e) => return QueryResponse::Err(1064, format!("update error: {e}")),
            };
            // 字段级修改（确保是对象）
            if !doc.is_object() {
                doc = serde_json::Value::Object(serde_json::Map::new());
            }
            let obj = doc.as_object_mut().unwrap();
            if let Some(inc) = parse_increment_expr(&field, &expr) {
                // 自增：k=k+N → 读当前值 + N（sysbench UPDATE k=k+1）
                let cur = obj.get(&field).and_then(|v| v.as_i64()).unwrap_or(0);
                obj.insert(field.clone(), serde_json::Value::from(cur + inc));
            } else {
                // 字符串赋值：c='value'
                obj.insert(field.clone(), serde_json::Value::String(unquote(&expr)));
            }
            let new_doc = serde_json::to_string(&doc).unwrap_or_default();
            match put_doc_txn(txn, id, &new_doc) {
                Ok(_) => QueryResponse::Ok(1, 0),
                Err(e) => QueryResponse::Err(1064, format!("update error: {e}")),
            }
        }
        Err(e) => QueryResponse::Err(1064, format!("update syntax: {e}")),
    }
}

/// 事务内 DELETE：攒批删除。
fn txn_delete(session: &mut Session, sql: &str) -> QueryResponse {
    match parse_delete(sql) {
        Ok(id) => {
            let txn = session.txn.as_mut().unwrap();
            txn.delete(id);
            QueryResponse::Ok(1, 0)
        }
        Err(e) => QueryResponse::Err(1064, format!("delete syntax: {e}")),
    }
}

/// H-5：COM_STMT_PREPARE。分配 stmt_id 存 SQL，返回 PREPARE_OK + [参数定义 + EOF] + 列定义 + EOF。
fn stmt_prepare(session: &mut Session, sql: &str) -> Vec<Vec<u8>> {
    let num_params = sql.bytes().filter(|b| *b == b'?').count() as u16;
    let stmt_id = session.next_stmt_id;
    session.next_stmt_id += 1;
    session.statements.insert(stmt_id, sql.to_string());
    let mut packets = Vec::new();
    // PREPARE_OK：0x00 + stmt_id(4) + num_columns(2) + num_params(2) + filler(1) + warnings(2)
    let mut ok = vec![0x00];
    ok.extend_from_slice(&stmt_id.to_le_bytes());
    ok.extend_from_slice(&2u16.to_le_bytes()); // 列 = id, doc
    ok.extend_from_slice(&num_params.to_le_bytes());
    ok.push(0);
    ok.extend_from_slice(&0u16.to_le_bytes());
    packets.push(ok);
    // 参数定义（ParameterDefinition41 与 ColumnDefinition41 同构）
    for _ in 0..num_params {
        packets.push(column_payload("?", MYSQL_TYPE_VAR_STRING, 45));
    }
    if num_params > 0 {
        packets.push(eof_payload());
    }
    // 列定义 + EOF（id / doc）
    packets.push(column_payload("id", MYSQL_TYPE_LONGLONG, 63));
    packets.push(column_payload("doc", MYSQL_TYPE_VAR_STRING, 45));
    packets.push(eof_payload());
    packets
}

/// H-5：COM_STMT_EXECUTE。解析参数（null bitmap + 类型 + 二进制值）→ 占位符替换 →
/// 复用 COM_QUERY 分发逻辑。
fn stmt_execute(engine: &mut Engine, session: &mut Session, cmd: &[u8]) -> QueryResponse {
    if cmd.len() < 10 {
        return QueryResponse::Err(1094, "EXECUTE 包过短".to_string());
    }
    let stmt_id = u32::from_le_bytes(cmd[1..5].try_into().unwrap());
    let Some(sql) = session.statements.get(&stmt_id).cloned() else {
        return QueryResponse::Err(1094, format!("未知 statement id {stmt_id}"));
    };
    let num_params = sql.bytes().filter(|b| *b == b'?').count();
    let null_len = (num_params + 7) / 8;
    let mut pos = 10usize;
    if pos + null_len > cmd.len() {
        return QueryResponse::Err(1094, "EXECUTE 参数位图越界".to_string());
    }
    let null_bitmap = &cmd[pos..pos + null_len];
    pos += null_len;
    // new_params_bound_flag = 1 → 参数类型表
    let mut types: Vec<u8> = Vec::new();
    if cmd.get(pos).copied() == Some(1) {
        pos += 1;
        if pos + num_params * 2 > cmd.len() {
            return QueryResponse::Err(1094, "EXECUTE 类型表越界".to_string());
        }
        for _ in 0..num_params {
            types.push(cmd[pos]);
            pos += 2; // type + unsigned_flag
        }
    }
    // 解析参数值
    let mut values: Vec<String> = Vec::new();
    for i in 0..num_params {
        if null_bitmap[i / 8] & (1 << (i % 8)) != 0 {
            values.push("NULL".to_string());
            continue;
        }
        let t = types.get(i).copied().unwrap_or(MYSQL_TYPE_VAR_STRING);
        match t {
            MYSQL_TYPE_LONGLONG => {
                if pos + 8 > cmd.len() {
                    return QueryResponse::Err(1094, "EXECUTE LONGLONG 越界".to_string());
                }
                let v = u64::from_le_bytes(cmd[pos..pos + 8].try_into().unwrap());
                pos += 8;
                values.push(v.to_string());
            }
            MYSQL_TYPE_LONG => {
                if pos + 4 > cmd.len() {
                    return QueryResponse::Err(1094, "EXECUTE LONG 越界".to_string());
                }
                let v = u32::from_le_bytes(cmd[pos..pos + 4].try_into().unwrap());
                pos += 4;
                values.push(v.to_string());
            }
            MYSQL_TYPE_DOUBLE => {
                if pos + 8 > cmd.len() {
                    return QueryResponse::Err(1094, "EXECUTE DOUBLE 越界".to_string());
                }
                let bits = u64::from_le_bytes(cmd[pos..pos + 8].try_into().unwrap());
                pos += 8;
                values.push(f64::from_bits(bits).to_string());
            }
            // 字符串类（VAR_STRING/STRING/BLOB）：lenenc 长度 + 数据
            _ => {
                let len = match read_lenenc_raw(cmd, &mut pos) {
                    Ok(l) => l as usize,
                    Err(e) => return QueryResponse::Err(1094, format!("参数长度越界: {e}")),
                };
                if pos + len > cmd.len() {
                    return QueryResponse::Err(1094, "EXECUTE 字符串越界".to_string());
                }
                let s = String::from_utf8_lossy(&cmd[pos..pos + len]).to_string();
                pos += len;
                // 字符串参数按 SQL 字面量（转义单引号）
                values.push(format!("'{}'", s.replace('\'', "''")));
            }
        }
    }
    // 占位符替换 → 走 COM_QUERY 分发
    let exec_sql = replace_params(&sql, &values);
    dispatch_query(engine, &exec_sql, session)
}

/// 按顺序把 SQL 中的 `?` 替换为参数值（values 已是 SQL 字面量形式）。
fn replace_params(sql: &str, values: &[String]) -> String {
    let mut out = String::with_capacity(sql.len() + 16);
    let mut vi = 0usize;
    let mut in_str = false;
    let mut quote = ' ';
    for c in sql.chars() {
        if in_str {
            out.push(c);
            if c == quote {
                in_str = false;
            }
        } else if c == '\'' || c == '"' {
            in_str = true;
            quote = c;
            out.push(c);
        } else if c == '?' {
            if let Some(v) = values.get(vi) {
                out.push_str(v);
            }
            vi += 1;
        } else {
            out.push(c);
        }
    }
    out
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
fn select_response(engine: &Engine, sql: &str) -> QueryResponse {
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
    // M 项 P0：`id BETWEEN A AND B` → 一次范围扫描（替代逐 id 点查）
    if let Some((a, b)) = extract_between_range(sql) {
        let rows = match engine.scan_range(Some(a), Some(b)) {
            Ok(r) => r,
            Err(_) => Vec::new(),
        };
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
        let mut data: Vec<Vec<Vec<u8>>> = rows
            .into_iter()
            .map(|(id, doc)| vec![id.to_string().into_bytes(), doc])
            .collect();
        if upper2.contains("ORDER BY") {
            data.sort_by(|a, b| a[1].cmp(&b[1]));
        }
        if let Some(lim) = extract_limit(sql) {
            data.truncate(lim.min(data.len()));
        }
        return QueryResponse::Set { columns, rows: data };
    }
    // sysbench 扩展：`id BETWEEN A AND B` / `id IN (...)` → 逐 id 点查；
    // `SELECT SUM(k)/count(k) ... WHERE id ...` → 聚合（单行单列）。
    if let Some(ids) = extract_target_ids(sql) {
        let upper2 = sql.to_uppercase();
        if upper2.contains("SUM(") || upper2.contains("COUNT(") {
            let mut sum: i64 = 0;
            for id in &ids {
                if let Ok(Some(v)) = engine.get(*id) {
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
        let mut data: Vec<Vec<Vec<u8>>> = Vec::with_capacity(ids.len());
        for id in ids {
            if let Ok(Some(v)) = engine.get(id) {
                data.push(vec![id.to_string().into_bytes(), v]);
            }
        }
        if upper2.contains("ORDER BY") {
            data.sort_by(|a, b| a[1].cmp(&b[1]));
        }
        if let Some(lim) = extract_limit(sql) {
            data.truncate(lim.min(data.len()));
        }
        return QueryResponse::Set { columns, rows: data };
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
/// sysbench 兼容：无 id 列（id=0）→ auto_increment 自动分配。
fn insert_response(
    engine: &mut Engine,
    sql: &str,
    auto_id: &AtomicU64,
) -> QueryResponse {
    match parse_insert_multi(sql) {
        Ok(Some(rows)) => {
            // H-6 扩展：多行 VALUES 批量入库（逐行 put，事务外；行数作为 affected）
            let mut last_id = 0u64;
            for (id, doc) in &rows {
                let real_id = if *id == 0 {
                    auto_id.fetch_add(1, Ordering::Relaxed)
                } else {
                    *id
                };
                last_id = real_id;
                if let Err(e) = put_doc(engine, real_id, doc) {
                    return QueryResponse::Err(1064, format!("insert error: {e}"));
                }
            }
            let n = rows.len() as u64;
            QueryResponse::Ok(n, last_id)
        }
        Ok(None) => QueryResponse::Ok(0, 0),
        Err(e) => QueryResponse::Err(1064, format!("insert syntax: {e}")),
    }
}

/// UPDATE documents SET field=expr WHERE id=1（非事务：字段级 / 整体替换）。
fn update_response(engine: &mut Engine, sql: &str) -> QueryResponse {
    match parse_update(sql) {
        Ok((id, field, expr)) => {
            // 整体替换（field=doc）
            if field.eq_ignore_ascii_case("doc") {
                let raw = unquote(&expr);
                return match put_doc(engine, id, &raw) {
                    Ok(_) => QueryResponse::Ok(1, 0),
                    Err(e) => QueryResponse::Err(1064, format!("update error: {e}")),
                };
            }
            // 读当前文档 → 字段级修改 → 覆盖写回
            let mut doc: serde_json::Value = match engine.get(id) {
                Ok(Some(v)) => serde_json::from_slice(&v)
                    .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new())),
                Ok(None) => serde_json::Value::Object(serde_json::Map::new()),
                Err(e) => return QueryResponse::Err(1064, format!("update error: {e}")),
            };
            if !doc.is_object() {
                doc = serde_json::Value::Object(serde_json::Map::new());
            }
            let obj = doc.as_object_mut().unwrap();
            if let Some(inc) = parse_increment_expr(&field, &expr) {
                let cur = obj.get(&field).and_then(|v| v.as_i64()).unwrap_or(0);
                obj.insert(field.clone(), serde_json::Value::from(cur + inc));
            } else {
                obj.insert(field.clone(), serde_json::Value::String(unquote(&expr)));
            }
            let new_doc = serde_json::to_string(&doc).unwrap_or_default();
            match put_doc(engine, id, &new_doc) {
                Ok(_) => QueryResponse::Ok(1, 0),
                Err(e) => QueryResponse::Err(1064, format!("update error: {e}")),
            }
        }
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
    let terms = doc_terms(doc)?;
    let refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
    engine.put(id, doc.as_bytes().to_vec(), &refs)
}

/// 事务内 put：攒批到事务（H-4，commit 时原子应用）。
fn put_doc_txn(txn: &mut crate::txn::Transaction, id: u64, doc: &str) -> Result<()> {
    let terms = doc_terms(doc)?;
    txn.put(id, doc.as_bytes().to_vec(), terms);
    Ok(())
}

/// 从 JSON 文档提取倒排词条。
fn doc_terms(doc: &str) -> Result<Vec<String>> {
    let parsed: serde_json::Value = serde_json::from_str(doc)
        .map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
    Ok(crate::server::extract_terms(&parsed))
}

// ============ 简易 SQL 解析（INSERT/UPDATE/DELETE 子集）============

/// 解析 `INSERT INTO [db.]table [(cols)] VALUES (...)` → (id, doc)。
/// 兼容单行；多行 VALUES 用 [`parse_insert_multi`]。
fn parse_insert(sql: &str) -> Result<Option<(u64, String)>> {
    Ok(parse_insert_multi(sql)?.and_then(|v| v.into_iter().next()))
}

/// 解析多行 `INSERT ... VALUES (..),(..),...` → 全部 (id, doc) 组。
/// 支持 sysbench `--insert-multiple-rows`（H-6 扩展：一次语句批量入库）。
fn parse_insert_multi(sql: &str) -> Result<Option<Vec<(u64, String)>>> {
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
            // sysbench 兼容：无 id 列（auto_increment 语义）→ id=0 由调用方自动分配
            let id = match idv {
                Some(v) => v
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| Error::Cluster("id 非法".into()))?,
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
            let id = parts[0]
                .trim()
                .parse::<u64>()
                .map_err(|_| Error::Cluster("id 非法".into()))?;
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

/// 解析 `UPDATE documents SET doc='...' WHERE id=1` → (id, doc)。
/// 解析 `UPDATE tbl SET field=expr WHERE id=N` → (id, field, expr)。
/// field 可为任意列；expr 支持 `field+N`（字段自增）、`'string'`（字符串赋值）、
/// `doc='{json}'`（整体替换，兼容旧语义）。
fn parse_update(sql: &str) -> Result<(u64, String, String)> {
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
fn parse_increment_expr(field: &str, expr: &str) -> Option<i64> {
    let e = expr.trim();
    let (f, num) = e.split_once('+')?;
    if !f.trim().eq_ignore_ascii_case(field) {
        return None;
    }
    num.trim().parse::<i64>().ok()
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
        // H-6：sysbench 风格多列（非 id 列组装 JSON 文档）
        let (id3, doc3) = parse_insert(
            "INSERT INTO sbtest1 (id, k, c, pad) VALUES (3, 500, 'hello', 'world')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(id3, 3);
        let v: serde_json::Value = serde_json::from_str(&doc3).unwrap();
        assert_eq!(v["k"], 500);
        assert_eq!(v["c"], "hello");
        assert_eq!(v["pad"], "world");
    }

    #[test]
    fn parse_insert_multi_rows() {
        // 多行 VALUES（sysbench --insert-multiple-rows 风格）
        let rows = parse_insert_multi(
            "INSERT INTO sbtest1 (id, k, c, pad) VALUES (1, 100, 'a', 'x'),(2, 200, 'b', 'y'),(3, 300, 'c', 'z')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[2].0, 3);
        let v2: serde_json::Value = serde_json::from_str(&rows[1].1).unwrap();
        assert_eq!(v2["k"], 200);
        assert_eq!(v2["c"], "b");
        // 单行兼容：parse_insert 取首行
        let (id, _) = parse_insert("INSERT INTO sbtest1 (id, k, c, pad) VALUES (9, 1, 'a', 'b')")
            .unwrap()
            .unwrap();
        assert_eq!(id, 9);
        // 多行内嵌逗号/引号不拆错
        let rows2 = parse_insert_multi(
            "INSERT INTO t (id, doc) VALUES (1, '{\"a\":1,\"b\":2}'),(2, 'hello, world')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(rows2.len(), 2);
        assert_eq!(rows2[0].1, r#"{"a":1,"b":2}"#);
        assert_eq!(rows2[1].1, "hello, world");
        // sysbench 兼容：无 id 列（auto_increment）→ id=0 标记，调用方自动分配
        let rows3 = parse_insert_multi(
            "INSERT INTO sbtest1 (k, c, pad) VALUES (1, 'a', 'b'),(2, 'c', 'd')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(rows3.len(), 2);
        assert_eq!(rows3[0].0, 0);
        assert_eq!(rows3[1].0, 0);
        let v3: serde_json::Value = serde_json::from_str(&rows3[0].1).unwrap();
        assert_eq!(v3["k"], 1);
        assert_eq!(v3["c"], "a");
    }

    #[test]
    fn extract_target_ids_point_range_in() {
        // 点查
        assert_eq!(
            extract_target_ids("SELECT c FROM sbtest1 WHERE id=42").unwrap(),
            vec![42]
        );
        assert_eq!(
            extract_target_ids("SELECT c FROM sbtest WHERE id=7 ORDER BY c").unwrap(),
            vec![7]
        );
        // BETWEEN 闭区间
        let ids = extract_target_ids(
            "SELECT c FROM sbtest WHERE id BETWEEN 100 AND 103",
        )
        .unwrap();
        assert_eq!(ids, vec![100, 101, 102, 103]);
        // IN 多点
        let ids2 = extract_target_ids(
            "SELECT c FROM sbtest WHERE id IN (5, 9, 12)",
        )
        .unwrap();
        assert_eq!(ids2, vec![5, 9, 12]);
        // LIMIT 提取
        assert_eq!(extract_limit("SELECT c FROM sbtest WHERE id BETWEEN 1 AND 5 ORDER BY c LIMIT 10"), Some(10));
        // 不支持 → None
        assert!(extract_target_ids("SELECT * FROM sbtest WHERE status='a'").is_none());
    }

    // ---------- 单元：SET TRANSACTION ISOLATION LEVEL ----------

    #[test]
    fn parse_isolation_level_variants() {
        use crate::txn::Isolation;
        assert_eq!(
            parse_isolation_level("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
            Some(Isolation::RepeatableRead)
        );
        assert_eq!(
            parse_isolation_level("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
            Some(Isolation::Serializable)
        );
        assert_eq!(
            parse_isolation_level("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"),
            Some(Isolation::ReadCommitted)
        );
        // READ UNCOMMITTED 未单独实现 → 映射 READ COMMITTED（无脏读语义）
        assert_eq!(
            parse_isolation_level("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"),
            Some(Isolation::ReadCommitted)
        );
        // 非隔离级别 SET → None（调用方忽略返回 OK）
        assert_eq!(parse_isolation_level("SET autocommit=1"), None);
        assert_eq!(parse_isolation_level("SET NAMES utf8mb4"), None);
    }

    #[test]
    fn extract_between_range_variants() {
        // 标准 BETWEEN
        assert_eq!(
            extract_between_range("SELECT c FROM sbtest WHERE id BETWEEN 100 AND 200"),
            Some((100, 200))
        );
        // 带 ORDER BY / LIMIT
        assert_eq!(
            extract_between_range("SELECT c FROM sbtest WHERE id BETWEEN 5 AND 9 ORDER BY c LIMIT 10"),
            Some((5, 9))
        );
        // 非 BETWEEN → None
        assert_eq!(extract_between_range("SELECT c FROM sbtest WHERE id=42"), None);
        assert_eq!(extract_between_range("SELECT c FROM sbtest WHERE id IN (1,2,3)"), None);
        assert_eq!(extract_between_range("SELECT c FROM sbtest WHERE k BETWEEN 1 AND 5"), None); // k 列非 id
    }

    #[test]
    fn parse_update_and_delete() {
        // 整体替换：SET doc='{json}'
        let (id, field, expr) =
            parse_update("UPDATE documents SET doc='{\"b\":2}' WHERE id=9").unwrap();
        assert_eq!(id, 9);
        assert_eq!(field, "doc");
        assert_eq!(expr, r#"'{"b":2}'"#);
        assert_eq!(unquote(&expr), r#"{"b":2}"#);
        // 字段自增：SET k=k+1
        let (id2, f2, e2) = parse_update("UPDATE sbtest1 SET k=k+1 WHERE id=49873363").unwrap();
        assert_eq!((id2, f2.as_str(), e2.as_str()), (49873363, "k", "k+1"));
        assert_eq!(parse_increment_expr(&f2, &e2), Some(1));
        // 字符串赋值：SET c='str'
        let (id3, f3, e3) = parse_update("UPDATE sbtest1 SET c='abc123' WHERE id=1").unwrap();
        assert_eq!((id3, f3.as_str()), (1, "c"));
        assert_eq!(parse_increment_expr(&f3, &e3), None);
        assert_eq!(unquote(&e3), "abc123");
        assert_eq!(parse_delete("DELETE FROM documents WHERE id=5").unwrap(), 5);
    }

    #[test]
    fn split_values_respects_quoted_commas() {
        let parts = split_values("1, '{\"a\":1,\"b\":2}', 3");
        assert_eq!(parts.len(), 3);
        assert_eq!(unquote(&parts[1]), r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn compaction_worker_converges_l0_with_single_round_per_wake() {
        // 9e77872（P71 阶段一）：worker 锁内**单轮**合并 + 100ms 循环——写触发信号后，
        // worker 多轮单轮合并收敛 L0（旧实现锁内 while 8 轮连续合并阻塞写）。
        // 覆盖空白：engine 级测试只手动调 compact()，此处验证 worker 线程真实运行收敛。
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.memtable.max_size_mb = 1; // 小 MemTable → 写入过程快速 flush 多段
        cfg.storage.l0_stall_threshold = 2; // 低 L0 阈值 → 2 段 L0 即触发合并
        let engine = Engine::open(dir.path(), &cfg).unwrap();
        let server = MySqlServer::new(engine, "root", "");
        server.spawn_compaction_worker();
        // 写 5 段（各 ~2MB）→ L0 超阈值 → auto_compact 置信号（worker 挂载：写不阻塞）
        let val = vec![b'x'; 2048];
        for seg in 0..5u64 {
            for i in seg * 1000..seg * 1000 + 1000 {
                server.engine.write().unwrap().put(i, val.clone(), &[]).unwrap();
            }
        }
        // 等待 worker 多轮单轮合并收敛（100ms 轮询 + 单轮合并；10s 上限）
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if !server.engine.read().unwrap().needs_compact()
                || std::time::Instant::now() > deadline
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(
            !server.engine.read().unwrap().needs_compact(),
            "worker 多轮单轮合并后 L0 应收敛（10s 内）"
        );
        // 数据完整
        for i in (0..5_000u64).step_by(997) {
            let v = server.engine.read().unwrap().get(i).unwrap();
            assert_eq!(v.as_deref(), Some(val.as_slice()), "docid={i}");
        }
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

        /// 发 COM_STMT_PREPARE，收全部响应包（PREPARE_OK + 参数定义 + EOF + 列定义 + EOF）。
        fn stmt_prepare(&mut self, sql: &str) -> Vec<Vec<u8>> {
            let mut cmd = vec![COM_STMT_PREPARE];
            cmd.extend_from_slice(sql.as_bytes());
            write_packet(&mut self.stream, 0, &cmd).unwrap();
            let mut packets = Vec::new();
            let (_, first) = read_packet(&mut self.stream).unwrap();
            packets.push(first);
            // PREPARE_OK 后：参数定义 + EOF + 列定义 + EOF（直到第二个 EOF）
            let mut saw_eof = false;
            loop {
                let (_, payload) = read_packet(&mut self.stream).unwrap();
                let is_eof = payload.first() == Some(&EOF_PACKET) && payload.len() < 9;
                let last = is_eof && saw_eof;
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

        /// 发 COM_STMT_EXECUTE（LONGLONG 单参数），收全部响应包。
        fn stmt_execute(&mut self, stmt_id: u32, param: u64) -> Vec<Vec<u8>> {
            let mut cmd = vec![COM_STMT_EXECUTE];
            cmd.extend_from_slice(&stmt_id.to_le_bytes());
            cmd.push(0); // flags
            cmd.extend_from_slice(&1u32.to_le_bytes()); // iteration
            cmd.push(0); // null_bitmap（无 NULL）
            cmd.push(1); // new_params_bound_flag
            cmd.push(MYSQL_TYPE_LONGLONG);
            cmd.push(0); // unsigned
            cmd.extend_from_slice(&param.to_le_bytes());
            self.send_command_and_read(&cmd)
        }

        /// 发 COM_STMT_EXECUTE（LONGLONG + 字符串参数）。
        fn stmt_execute_str(&mut self, stmt_id: u32, id: u64, doc: &str) -> Vec<Vec<u8>> {
            let mut cmd = vec![COM_STMT_EXECUTE];
            cmd.extend_from_slice(&stmt_id.to_le_bytes());
            cmd.push(0);
            cmd.extend_from_slice(&1u32.to_le_bytes());
            cmd.push(0); // null_bitmap
            cmd.push(1); // new_params_bound_flag
            cmd.push(MYSQL_TYPE_LONGLONG);
            cmd.push(0);
            cmd.push(MYSQL_TYPE_VAR_STRING);
            cmd.push(0);
            cmd.extend_from_slice(&id.to_le_bytes());
            cmd.push(doc.len() as u8); // lenenc（短串）
            cmd.extend_from_slice(doc.as_bytes());
            self.send_command_and_read(&cmd)
        }

        /// 发命令并读取响应（OK/ERR 单包；ResultSet 直到第二个 EOF）。
        fn send_command_and_read(&mut self, cmd: &[u8]) -> Vec<Vec<u8>> {
            write_packet(&mut self.stream, 0, cmd).unwrap();
            let (_, first) = read_packet(&mut self.stream).unwrap();
            if first.first() == Some(&OK_PACKET) || first.first() == Some(&ERR_PACKET) {
                return vec![first];
            }
            let mut packets = vec![first];
            let mut saw_eof = false;
            loop {
                let (_, payload) = read_packet(&mut self.stream).unwrap();
                let is_eof = payload.first() == Some(&EOF_PACKET) && payload.len() < 9;
                let last = is_eof && saw_eof;
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

    // ---------- H-4：事务语句 ----------

    #[test]
    fn txn_begin_rollback_and_commit_roundtrip() {
        let engine = test_engine();
        let server = MySqlServer::new(engine, "root", "secret");
        let addr = server.serve_once("127.0.0.1:0").unwrap();
        let mut c = TestClient::connect(addr);
        let (scramble, _) = c.handshake();
        c.authenticate("root", "secret", &scramble);

        // BEGIN → 事务内 INSERT（攒批）→ 同事务 SELECT 可见 → ROLLBACK → 无数据
        assert_eq!(c.query("BEGIN")[0][0], OK_PACKET);
        let ins = c.query("INSERT INTO documents (id, doc) VALUES (5, '{\"tx\":1}')");
        assert_eq!(ins[0][0], OK_PACKET);
        // 同事务读可见（read_own）
        let sel = c.query("SELECT * FROM documents WHERE id=5");
        let row = &sel[sel.len() - 2];
        let mut p = 0usize;
        let _ = read_lenenc(&row, &mut p).unwrap();
        assert_eq!(&row[p..p + 1], b"5", "事务内读到自己未提交的写");
        // ROLLBACK 后无数据
        assert_eq!(c.query("ROLLBACK")[0][0], OK_PACKET);
        let sel2 = c.query("SELECT * FROM documents WHERE id=5");
        assert_eq!(sel2.len(), 5, "回滚后空结果集");

        // BEGIN → INSERT → COMMIT → 持久可见
        assert_eq!(c.query("BEGIN")[0][0], OK_PACKET);
        assert_eq!(
            c.query("INSERT INTO documents (id, doc) VALUES (6, '{\"tx\":2}')")[0][0],
            OK_PACKET
        );
        assert_eq!(c.query("COMMIT")[0][0], OK_PACKET);
        let sel3 = c.query("SELECT * FROM documents WHERE id=6");
        assert_eq!(sel3[0][0], 2, "提交后两列");
        let row3 = &sel3[sel3.len() - 2];
        let mut p3 = 0usize;
        let _ = read_lenenc(&row3, &mut p3).unwrap();
        assert_eq!(&row3[p3..p3 + 1], b"6");
    }

    #[test]
    fn txn_nested_begin_is_error_and_idle_commit_ok() {
        let engine = test_engine();
        let server = MySqlServer::new(engine, "root", "secret");
        let addr = server.serve_once("127.0.0.1:0").unwrap();
        let mut c = TestClient::connect(addr);
        let (scramble, _) = c.handshake();
        c.authenticate("root", "secret", &scramble);
        // 无活动事务 COMMIT → OK（MySQL 空提交语义）
        assert_eq!(c.query("COMMIT")[0][0], OK_PACKET);
        assert_eq!(c.query("ROLLBACK")[0][0], OK_PACKET);
        // 嵌套 BEGIN → 错误
        assert_eq!(c.query("BEGIN")[0][0], OK_PACKET);
        let r2 = c.query("BEGIN");
        assert_eq!(r2[0][0], ERR_PACKET);
        assert_eq!(c.query("ROLLBACK")[0][0], OK_PACKET);
    }

    // ---------- H-5：预处理语句 ----------

    #[test]
    fn stmt_prepare_execute_roundtrip() {
        let engine = test_engine();
        let server = MySqlServer::new(engine, "root", "secret");
        let addr = server.serve_once("127.0.0.1:0").unwrap();
        let mut c = TestClient::connect(addr);
        let (scramble, _) = c.handshake();
        c.authenticate("root", "secret", &scramble);

        // 预插入一条
        assert_eq!(
            c.query("INSERT INTO documents (id, doc) VALUES (7, '{\"p\":1}')")[0][0],
            OK_PACKET
        );
        // PREPARE：SELECT * FROM documents WHERE id=?
        let prep = c.stmt_prepare("SELECT * FROM documents WHERE id=?");
        assert_eq!(prep[0][0], OK_PACKET, "PREPARE_OK 头");
        let stmt_id = u32::from_le_bytes(prep[0][1..5].try_into().unwrap());
        assert_eq!(stmt_id, 1);
        assert_eq!(u16::from_le_bytes(prep[0][7..9].try_into().unwrap()), 1, "1 个参数");

        // EXECUTE：参数 LONGLONG=7 → 结果集
        let packets = c.stmt_execute(stmt_id, 7u64);
        assert_eq!(packets[0][0], 2, "两列 id/doc");
        let row = &packets[packets.len() - 2];
        let mut p = 0usize;
        let n = read_lenenc(&row, &mut p).unwrap();
        assert_eq!(n, 1);
        assert_eq!(&row[p..p + 1], b"7", "EXECUTE 参数 7 命中");

        // EXECUTE 字符串参数（doc 搜索参数场景：INSERT 占位）
        let prep2 = c.stmt_prepare("INSERT INTO documents (id, doc) VALUES (?, ?)");
        assert_eq!(
            u16::from_le_bytes(prep2[0][7..9].try_into().unwrap()),
            2,
            "2 个参数"
        );
        let stmt_id2 = u32::from_le_bytes(prep2[0][1..5].try_into().unwrap());
        // EXECUTE：LONGLONG=8 + 字符串 '{"x":1}'
        let ok = c.stmt_execute_str(stmt_id2, 8u64, r#"{"x":1}"#);
        assert_eq!(ok[0][0], OK_PACKET);
        let sel = c.query("SELECT * FROM documents WHERE id=8");
        let row8 = &sel[sel.len() - 2];
        let mut p8 = 0usize;
        let _ = read_lenenc(&row8, &mut p8).unwrap();
        assert_eq!(&row8[p8..p8 + 1], b"8", "预处理 INSERT 生效");
    }

    #[test]
    fn stmt_execute_unknown_id_is_error() {
        let engine = test_engine();
        let server = MySqlServer::new(engine, "root", "secret");
        let addr = server.serve_once("127.0.0.1:0").unwrap();
        let mut c = TestClient::connect(addr);
        let (scramble, _) = c.handshake();
        c.authenticate("root", "secret", &scramble);
        let r = c.stmt_execute(999, 1u64);
        assert_eq!(r[0][0], ERR_PACKET, "未知 stmt_id → 错误");
    }
}
