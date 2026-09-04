
//! MySQL wire 协议包编解码与协议常量（server/protocol/packet.rs）：内容拆分自原
//! src/db_adapter.rs——协议常量、报文编解码（read_packet / write_packet / read_command +
//! tokio 异步版）、lenenc / OK / ERR / EOF / ColumnDefinition payload 构造、以及字节流
//! 解析工具（read_u32_le / read_nul_string / read_lenenc_raw）。

use std::io::{Read, Write};
use std::net::TcpStream;

use crate::error::{Error, Result};



pub(crate) const PROTOCOL_VERSION: u8 = 10;
pub(crate) const SERVER_VERSION: &str = "8.0.0-shanshui-cunji";
/// 固定表名（文档库无表语义，SQL 层映射为单表）。
pub const DEFAULT_TABLE: &str = "documents";
pub const DEFAULT_DB: &str = "cjserver";

// 能力位（仅声明已支持子集）
pub(crate) const CLIENT_PROTOCOL_41: u32 = 1 << 9;
pub(crate) const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
pub(crate) const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
pub(crate) const CLIENT_CONNECT_WITH_DB: u32 = 1 << 3;
/// 客户端连接属性（RustMySQL v26 等客户端**无条件**随握手响应发送该能力位与键值对载荷：
/// OS/客户端名等；若不消费 attrs 字节，认证后首条命令会被读错 → 连接建立失败
/// CouldNotSetupConnection，即 mysql crate ↔ SCC 握手不兼容根因）。
pub(crate) const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
pub(crate) const CLIENT_TRANSACTIONS: u32 = 1 << 13;
pub(crate) const CLIENT_MULTI_STATEMENTS: u32 = 1 << 16;
pub(crate) const CAPABILITIES: u32 = CLIENT_PROTOCOL_41
    | CLIENT_SECURE_CONNECTION
    | CLIENT_PLUGIN_AUTH
    | CLIENT_TRANSACTIONS
    | CLIENT_MULTI_STATEMENTS;
pub(crate) const CHARSET_UTF8MB4: u8 = 45;

// 命令
pub(crate) const COM_QUIT: u8 = 0x01;
pub(crate) const COM_INIT_DB: u8 = 0x02;
pub(crate) const COM_QUERY: u8 = 0x03;
pub(crate) const COM_PING: u8 = 0x0e;
pub(crate) const COM_STMT_PREPARE: u8 = 0x16;
pub(crate) const COM_STMT_EXECUTE: u8 = 0x17;
pub(crate) const COM_STMT_CLOSE: u8 = 0x19;

// 包类型
pub(crate) const OK_PACKET: u8 = 0x00;
pub(crate) const EOF_PACKET: u8 = 0xfe;
pub(crate) const ERR_PACKET: u8 = 0xff;

// 列类型
pub(crate) const MYSQL_TYPE_LONGLONG: u8 = 8;
pub(crate) const MYSQL_TYPE_VAR_STRING: u8 = 253;
pub(crate) const MYSQL_TYPE_LONG: u8 = 3;
pub(crate) const MYSQL_TYPE_STRING: u8 = 254;
pub(crate) const MYSQL_TYPE_DOUBLE: u8 = 5;

/// 倒排内存 term 落盘阈值（条，7.93）：mysql-server 写路径 term 攒内存不落盘，
/// 达此阈值强制 `flush_inverted` 落段（重启后字段等值仍可查；段数由 GC worker 收敛）。
pub(crate) const INVERTED_MEM_FLUSH_THRESHOLD: u64 = 1_000_000;

// ============ 报文编解码 ============

pub(crate) fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) -> std::io::Result<()> {
    let len = payload.len() as u32;
    stream.write_all(&[
        (len & 0xff) as u8,
        ((len >> 8) & 0xff) as u8,
        ((len >> 16) & 0xff) as u8,
        seq,
    ])?;
    stream.write_all(payload)
}

pub(crate) fn read_packet(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut hdr = [0u8; 4];
    stream.read_exact(&mut hdr)?;
    let len = (hdr[0] as usize) | ((hdr[1] as usize) << 8) | ((hdr[2] as usize) << 16);
    let seq = hdr[3];
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload)?;
    Ok((seq, payload))
}

/// 读完整命令负载（COM_QUERY 等）：MySQL 协议单包载荷上限 0xFFFFFF——客户端对超大
/// 语句自动分包（seq 递增，直到短包收尾）；此处拼接续包还原完整命令，防包序错乱。
/// 返回 `(下一个响应 seq, 拼接后的完整负载)`——响应 seq 须接在请求最后一包之后
/// （单包命令 seq0 → 响应从 1 起；两包命令 seq0,1 → 响应从 2 起，客户端会校验）。
pub(crate) fn read_command(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut next_seq = 0u8;
    loop {
        let (seq, p) = read_packet(stream)?;
        next_seq = seq.wrapping_add(1);
        let full = p.len() == 0xFFFFFF;
        buf.extend_from_slice(&p);
        if !full {
            return Ok((next_seq, buf));
        }
    }
}

pub(crate) fn write_lenenc(buf: &mut Vec<u8>, v: u64) {
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

pub(crate) fn write_lenenc_str(buf: &mut Vec<u8>, s: &str) {
    write_lenenc(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}

/// 构造 OK 包 payload（affected rows / last insert id）。
pub(crate) fn ok_payload(affected: u64, last_insert_id: u64) -> Vec<u8> {
    let mut b = vec![OK_PACKET];
    write_lenenc(&mut b, affected);
    write_lenenc(&mut b, last_insert_id);
    b.extend_from_slice(&0x0002u16.to_le_bytes()); // status flags: AUTO_COMMIT
    b.extend_from_slice(&0u16.to_le_bytes()); // warnings
    b
}

/// 构造 ERR 包 payload。
pub(crate) fn err_payload(code: u16, msg: &str) -> Vec<u8> {
    let mut b = vec![ERR_PACKET];
    b.extend_from_slice(&code.to_le_bytes());
    b.push(b'#'); // sqlstate marker
    b.extend_from_slice(b"HY000");
    b.extend_from_slice(msg.as_bytes());
    b
}

/// 构造 EOF 包 payload。
pub(crate) fn eof_payload() -> Vec<u8> {
    let mut b = vec![EOF_PACKET];
    b.extend_from_slice(&0u16.to_le_bytes()); // warnings
    b.extend_from_slice(&0x0002u16.to_le_bytes()); // status flags
    b
}

/// 列定义（ColumnDefinition41）。
pub(crate) fn column_payload(name: &str, col_type: u8, charset: u16) -> Vec<u8> {
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
// ============ 异步协程运行时（design 9.5：10k 连接目标）============

/// 异步读 MySQL 包（4 字节头 + payload）。
pub(crate) async fn read_packet_async(
    stream: &mut tokio::net::TcpStream,
) -> std::io::Result<(u8, Vec<u8>)> {
    use tokio::io::AsyncReadExt;
    let mut hdr = [0u8; 4];
    stream.read_exact(&mut hdr).await?;
    let len = (hdr[0] as usize) | ((hdr[1] as usize) << 8) | ((hdr[2] as usize) << 16);
    let seq = hdr[3];
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok((seq, payload))
}

/// 异步版 [`read_command`]：拼接 >16MB 分包命令（COM_QUERY 大语句）。
pub(crate) async fn read_command_async(
    stream: &mut tokio::net::TcpStream,
) -> std::io::Result<(u8, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut next_seq = 0u8;
    loop {
        let (seq, p) = read_packet_async(stream).await?;
        next_seq = seq.wrapping_add(1);
        let full = p.len() == 0xFFFFFF;
        buf.extend_from_slice(&p);
        if !full {
            return Ok((next_seq, buf));
        }
    }
}

/// 异步写 MySQL 包。
pub(crate) async fn write_packet_async(
    stream: &mut tokio::net::TcpStream,
    seq: u8,
    payload: &[u8],
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let len = payload.len() as u32;
    stream
        .write_all(&[
            (len & 0xff) as u8,
            ((len >> 8) & 0xff) as u8,
            ((len >> 16) & 0xff) as u8,
            seq,
        ])
        .await?;
    stream.write_all(payload).await
}

// ============ 工具 ============

pub(crate) fn read_u32_le(data: &[u8], pos: &mut usize) -> u32 {
    let mut b = [0u8; 4];
    let n = (data.len() - *pos).min(4);
    b[..n].copy_from_slice(&data[*pos..*pos + n]);
    *pos += 4;
    u32::from_le_bytes(b)
}

pub(crate) fn read_nul_string(data: &[u8], pos: &mut usize) -> Result<String> {
    let start = *pos;
    while *pos < data.len() && data[*pos] != 0 {
        *pos += 1;
    }
    let s = String::from_utf8_lossy(&data[start..*pos]).to_string();
    *pos = (*pos + 1).min(data.len());
    Ok(s)
}

pub(crate) fn read_lenenc_raw(data: &[u8], pos: &mut usize) -> Result<u64> {
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
pub(crate) fn read_lenenc(data: &[u8], pos: &mut usize) -> Result<u64> {
    read_lenenc_raw(data, pos)
}
