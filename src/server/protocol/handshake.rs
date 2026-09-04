
//! MySQL 握手与认证（server/protocol/handshake.rs）：内容拆分自原 src/db_adapter.rs——
//! HandshakeV10 握手包构造（build_handshake_packet）、握手响应解析与 native_password 认证
//! （parse_handshake_response / check_native_password）、scramble 生成（gen_scramble）。

use sha1::{Digest, Sha1};

use crate::error::{Error, Result};
use crate::server::*;


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
/// 构造 HandshakeV10 握手包（同步/异步共用）。
pub(crate) fn build_handshake_packet(conn_id: u64, scramble: &[u8; 20]) -> Vec<u8> {
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
    hb
}

/// 解析握手响应并校验 native_password 认证（同步/异步共用）。返回是否认证通过。
pub(crate) fn parse_handshake_response(
    resp: &[u8],
    session: &mut Session,
    user: &str,
    password: &str,
    scramble: &[u8; 20],
) -> Result<bool> {
    if resp.is_empty() {
        return Err(Error::Cluster("客户端握手响应为空".into()));
    }
    let mut pos = 0usize;
    let cap = read_u32_le(resp, &mut pos);
    let _max_packet = read_u32_le(resp, &mut pos);
    let _charset = resp.get(pos).copied().unwrap_or(0);
    pos += 1;
    pos += 23; // filler（协议 41：charset 后 23 字节零填充）
    session.user = read_nul_string(resp, &mut pos)?;
    // 协议 41 握手响应顺序：username → auth_response → [CONNECT_WITH_DB] db →
    // [PLUGIN_AUTH] auth_plugin_name → [CONNECT_ATTRS] attrs。
    // 各字段由「服务器声明」决定客户端是否发送：服务器 CAPABILITIES 未声明
    // CONNECT_WITH_DB / CONNECT_ATTRS → 客户端不应发送（跳过）。
    let mut auth_response: Vec<u8> = Vec::new();
    if cap & CLIENT_PLUGIN_AUTH != 0 {
        // auth_response：服务器未声明 CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA → 1 字节长度前缀
        // （小值下 lenenc 与 1 字节一致，read_lenenc_raw 兼容两者）
        let auth_len = read_lenenc_raw(resp, &mut pos)? as usize;
        if pos + auth_len > resp.len() {
            return Err(Error::Cluster("auth_response 越界".into()));
        }
        auth_response = resp[pos..pos + auth_len].to_vec();
        pos += auth_len;
    }
    if cap & CLIENT_CONNECT_WITH_DB != 0 && CAPABILITIES & CLIENT_CONNECT_WITH_DB != 0 {
        let _db = read_nul_string(resp, &mut pos)?;
    }
    if cap & CLIENT_PLUGIN_AUTH != 0 {
        let _plugin = read_nul_string(resp, &mut pos)?;
    }
    // 协议 41 握手响应最后字段：CLIENT_CONNECT_ATTRS = lenenc 总长 + 键值对字节。
    // 实测 RustMySQL v26 无视服务器未声明而发送 attrs（client_cap 含 bit20）——必须消费，
    // 否则残留字节被误读为认证后首条命令 → CouldNotSetupConnection。
    if cap & CLIENT_CONNECT_ATTRS != 0 {
        let attr_len = read_lenenc_raw(resp, &mut pos)? as usize;
        if pos + attr_len > resp.len() {
            return Err(Error::Cluster("connect attrs 越界".into()));
        }
        pos += attr_len;
    }
    Ok(session.user == user && check_native_password(&auth_response, scramble, password))
}
/// 伪随机 20 字节 scramble（无 rand 依赖：连接 id + 时间戳 → sha1 派生）。
pub(crate) fn gen_scramble(conn_id: u64) -> [u8; 20] {
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
