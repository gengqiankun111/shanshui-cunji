
//! JDBC 直连客户端（server/client.rs）：内容拆分自原 src/db_adapter.rs——MysqlWireClient
//! （握手 + mysql_native_password 认证 + COM_QUERY 建表/批量 INSERT，export --jdbc 直写）与
//! SQL 字面量转义 escape_sql。

use std::net::TcpStream;

use sha1::{Digest, Sha1};

use crate::error::{Error, Result};
use crate::server::*;


// ============ JDBC 直连客户端（design 20.5：export --jdbc 无文件落盘写目标库）============

/// 生成 native_password 客户端认证 token（stage1 XOR sha1(scramble + stage2)）。
pub(crate) fn native_auth_token(password: &str, scramble: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let stage1 = Sha1::digest(password.as_bytes());
    let stage2 = Sha1::digest(&stage1);
    let mut h = Sha1::new();
    h.update(scramble);
    h.update(stage2);
    let crypto = h.finalize();
    (0..20).map(|i| stage1[i] ^ crypto[i]).collect()
}

/// JDBC 直连客户端（MySQL wire 协议）：握手 + mysql_native_password 认证 + COM_QUERY
/// （建表 / 批量 INSERT）。export `--jdbc` 直写目标库，无中间文件落盘（design 20.5 阶段 3）。
pub struct MysqlWireClient {
    stream: Option<TcpStream>,
    host: String,
    user: String,
    password: String,
    db: String,
}

impl MysqlWireClient {
    /// 解析连接串 `mysql://[user[:pass]@]host[:port]/[db]`（缺省 127.0.0.1:3306 / root / 空）。
    pub fn from_url(url: &str) -> Result<Self> {
        let rest = url
            .strip_prefix("mysql://")
            .ok_or_else(|| Error::Unsupported(format!("JDBC 连接串需 mysql:// 前缀: {url}")))?;
        let (auth_host, db) = match rest.split_once('/') {
            Some((ah, db)) => (ah, db),
            None => (rest, ""),
        };
        let (user, host_port) = match auth_host.split_once('@') {
            Some((u, hp)) => (u, hp),
            None => ("root", auth_host),
        };
        let (user, password) = match user.split_once(':') {
            Some((u, p)) => (u, p),
            None => (user, ""),
        };
        Ok(Self {
            stream: None,
            host: if host_port.is_empty() {
                "127.0.0.1:3306".into()
            } else {
                host_port.to_string()
            },
            user: user.to_string(),
            password: password.to_string(),
            db: db.to_string(),
        })
    }

    /// 连接：TCP → 握手 → 认证 → （可选）USE db。
    pub fn connect(&mut self) -> Result<()> {
        let mut stream =
            TcpStream::connect(&self.host).map_err(Error::Io)?;
        // ① 握手（HandshakeV10）：解析 scramble + auth plugin
        let (_, payload) = read_packet(&mut stream).map_err(Error::Io)?;
        if payload.first() != Some(&PROTOCOL_VERSION) {
            return Err(Error::Unsupported("JDBC 目标非 MySQL 协议握手".into()));
        }
        let mut pos = 1usize;
        while pos < payload.len() && payload[pos] != 0 {
            pos += 1;
        }
        pos += 1; // server version NUL
        pos += 4; // conn id
        let mut scramble = payload[pos..pos + 8].to_vec();
        pos += 8 + 1 + 2 + 1 + 2 + 2; // auth part1 + filler + cap low + charset + status + cap high
        let auth_len = payload[pos] as usize;
        pos += 1 + 10; // auth_len + reserved
        let part2_len = auth_len.saturating_sub(9); // 去终止 NUL
        scramble.extend_from_slice(&payload[pos..pos + part2_len]);
        // ② 认证（native_password token）
        let token = native_auth_token(&self.password, &scramble);
        let mut b = Vec::new();
        b.extend_from_slice(&CAPABILITIES.to_le_bytes());
        b.extend_from_slice(&0x100_0000u32.to_le_bytes()); // max packet
        b.push(CHARSET_UTF8MB4);
        b.extend_from_slice(&[0u8; 23]);
        b.extend_from_slice(self.user.as_bytes());
        b.push(0);
        write_lenenc(&mut b, token.len() as u64);
        b.extend_from_slice(&token);
        write_packet(&mut stream, 1, &b).map_err(Error::Io)?;
        let (_, resp) = read_packet(&mut stream).map_err(Error::Io)?;
        if resp.first() != Some(&OK_PACKET) {
            let msg = String::from_utf8_lossy(&resp[9..]).to_string();
            return Err(Error::Unsupported(format!("JDBC 目标库认证失败: {msg}")));
        }
        // ③ USE db
        if !self.db.is_empty() {
            Self::query_conn(&mut stream, &format!("USE `{}`", self.db))?;
        }
        self.stream = Some(stream);
        Ok(())
    }

    /// COM_QUERY 执行（INSERT/DDL：OK 即成功，ERR 报错，ResultSet 读到 EOF 丢弃）。
    fn query_conn(stream: &mut TcpStream, sql: &str) -> Result<()> {
        let mut cmd = vec![COM_QUERY];
        cmd.extend_from_slice(sql.as_bytes());
        write_packet(stream, 0, &cmd).map_err(Error::Io)?;
        let (_, first) = read_packet(stream).map_err(Error::Io)?;
        match first.first() {
            Some(&OK_PACKET) => Ok(()),
            Some(&ERR_PACKET) => {
                let msg = String::from_utf8_lossy(&first[9..]).to_string();
                Err(Error::Unsupported(format!("JDBC 目标库错误: {msg}")))
            }
            _ => {
                let mut saw_eof = false; // 列定义后第一个 EOF 非终止；行尾第二个 EOF 终止
                loop {
                    let (_, p) = read_packet(stream).map_err(Error::Io)?;
                    let is_eof = p.first() == Some(&EOF_PACKET) && p.len() < 9;
                    if is_eof && saw_eof {
                        break;
                    }
                    if is_eof {
                        saw_eof = true;
                    }
                }
                Ok(())
            }
        }
    }

    pub fn query(&mut self, sql: &str) -> Result<()> {
        let s = self
            .stream
            .as_mut()
            .ok_or_else(|| Error::Unsupported("JDBC 客户端未连接".into()))?;
        Self::query_conn(s, sql)
    }

    /// 批量 INSERT：`INSERT INTO t (docid, doc) VALUES (...)`——doc JSON 文本转义
    /// （单引号 / 反斜杠 / 换行），无文件落盘（design 20.5 `--jdbc`）。
    pub fn insert_batch(&mut self, table: &str, rows: &[(u64, &str)]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut sql = String::with_capacity(64 + rows.len() * 96);
        sql.push_str(&format!("INSERT INTO `{table}` (docid, doc) VALUES "));
        for (i, (docid, doc)) in rows.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({docid},'{}')", escape_sql(doc)));
        }
        self.query(&sql)
    }
}

/// SQL 字符串字面量转义（单引号 / 反斜杠 / 换行——MySQL 默认 NO_BACKSLASH_ESCAPES 关闭）。
pub fn escape_sql(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '\'' => out.push_str("\\'"),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out
}
