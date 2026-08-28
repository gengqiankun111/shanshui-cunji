//! 极简 Redis RESP 客户端（design 21，阶段 2）。
//!
//! 零外部依赖：用 `std::net::TcpStream` 实现 RESP 协议子集，与内核同步风格一致
//! （无异步运行时、无 unsafe）：
//! - `PING`：探活；
//! - `GET key`：读（nil → None）；
//! - `SETEX key seconds value`：带 TTL 写入（Cache-Aside 回填 / 空值缓存）；
//! - `DEL key...`：批量删除（Write-Invalidate）。
//!
//! 超时由 `timeout_ms` 控制（design 21.4：超时自动降级，不阻塞）。

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

use crate::error::{Error, Result};

/// RESP 回复类型。
#[derive(Debug, Clone, PartialEq)]
enum Reply {
    Simple(String),
    Bulk(Option<Vec<u8>>),
    Integer(i64),
    Error(String),
    Array(Vec<Reply>),
}

/// 极简 Redis 客户端（单连接；连接断开由调用方重建）。
pub struct RedisClient {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl RedisClient {
    /// 连接 Redis（`addr` 形如 `127.0.0.1:6379`）。
    pub fn connect(addr: &str, timeout: Duration) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        let reader = BufReader::new(stream.try_clone()?);
        Ok(Self { stream, reader })
    }

    /// 探活：PING → PONG。
    pub fn ping(&mut self) -> Result<bool> {
        let r = self.command(&[b"PING"])?;
        Ok(matches!(r, Reply::Simple(s) if s == "PONG"))
    }

    /// GET：命中返回字节，缺失返回 None。
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.command(&[b"GET", key])? {
            Reply::Bulk(b) => Ok(b),
            other => Err(Error::Rpc(format!("GET 意外回复: {other:?}"))),
        }
    }

    /// SETEX：带 TTL 写入（秒）。
    pub fn set_ex(&mut self, key: &[u8], value: &[u8], ttl_secs: u64) -> Result<()> {
        let ttl = ttl_secs.to_string();
        match self.command(&[b"SETEX", key, ttl.as_bytes(), value])? {
            Reply::Simple(s) if s == "OK" => Ok(()),
            other => Err(Error::Rpc(format!("SETEX 意外回复: {other:?}"))),
        }
    }

    /// DEL：返回被删除的 key 数。
    pub fn del(&mut self, keys: &[&[u8]]) -> Result<u64> {
        let mut args: Vec<&[u8]> = vec![b"DEL"];
        args.extend_from_slice(keys);
        match self.command(&args)? {
            Reply::Integer(n) => Ok(n.max(0) as u64),
            other => Err(Error::Rpc(format!("DEL 意外回复: {other:?}"))),
        }
    }

    /// 发送命令并读取回复。
    fn command(&mut self, args: &[&[u8]]) -> Result<Reply> {
        let mut buf = Vec::with_capacity(64);
        buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf)?;
        self.stream.flush()?;
        read_reply(&mut self.reader)
    }
}

/// 读取一个 RESP 回复。
fn read_reply<R: BufRead>(r: &mut R) -> Result<Reply> {
    let mut line = String::new();
    r.read_line(&mut line)?;
    if !line.ends_with("\r\n") {
        return Err(Error::Rpc("RESP 行缺少 CRLF".into()));
    }
    let line = line.trim_end_matches("\r\n");
    let bytes = line.as_bytes();
    match bytes.first() {
        Some(b'+') => Ok(Reply::Simple(line[1..].to_string())),
        Some(b'-') => Ok(Reply::Error(line[1..].to_string())),
        Some(b':') => {
            let n: i64 = line[1..]
                .parse()
                .map_err(|e| Error::Rpc(format!("整数回复解析失败: {e}")))?;
            Ok(Reply::Integer(n))
        }
        Some(b'$') => {
            let len: i64 = line[1..]
                .parse()
                .map_err(|e| Error::Rpc(format!("Bulk 长度解析失败: {e}")))?;
            if len < 0 {
                return Ok(Reply::Bulk(None)); // $-1 = nil
            }
            let mut data = vec![0u8; len as usize];
            r.read_exact(&mut data)?;
            // 尾部 CRLF
            let mut crlf = [0u8; 2];
            r.read_exact(&mut crlf)?;
            Ok(Reply::Bulk(Some(data)))
        }
        Some(b'*') => {
            let n: i64 = line[1..]
                .parse()
                .map_err(|e| Error::Rpc(format!("数组长度解析失败: {e}")))?;
            let mut arr = Vec::with_capacity(n.max(0) as usize);
            for _ in 0..n.max(0) {
                arr.push(read_reply(r)?);
            }
            Ok(Reply::Array(arr))
        }
        _ => Err(Error::Rpc("未知 RESP 前缀".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 简易 Mock Redis 服务端：解析 RESP 命令数组并应答。
    fn spawn_mock_redis(
        store: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<Vec<u8>, (Vec<u8>, u64)>>>,
    ) -> String {
        use std::io::{BufRead, BufReader, Write};
        use std::net::TcpListener;

        fn read_args(r: &mut impl BufRead) -> Option<Vec<Vec<u8>>> {
            let mut line = String::new();
            r.read_line(&mut line).ok()?;
            let line = line.trim_end();
            if !line.starts_with('*') {
                return None;
            }
            let n: usize = line[1..].parse().ok()?;
            let mut args = Vec::with_capacity(n);
            for _ in 0..n {
                let mut l2 = String::new();
                r.read_line(&mut l2).ok()?;
                if !l2.starts_with('$') {
                    return None;
                }
                let len: usize = l2[1..].trim().parse().ok()?;
                let mut data = vec![0u8; len];
                r.read_exact(&mut data).ok()?;
                let mut crlf = [0u8; 2];
                r.read_exact(&mut crlf).ok()?;
                args.push(data);
            }
            Some(args)
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let store = store.clone();
        std::thread::spawn(move || {
            for s in listener.incoming() {
                if let Ok(mut s) = s {
                    let store = store.clone();
                    std::thread::spawn(move || {
                        let mut r = BufReader::new(s.try_clone().unwrap());
                        loop {
                            let Some(args) = read_args(&mut r) else { break };
                            let cmd = args.first().map(|a| a.as_slice()).unwrap_or(b"");
                            let reply = match cmd {
                                b"PING" => b"+PONG\r\n".to_vec(),
                                b"GET" => {
                                    let key = &args[1];
                                    let st = store.lock().unwrap();
                                    match st.get(key) {
                                        Some((v, _)) => {
                                            let mut out = format!("${}\r\n", v.len()).into_bytes();
                                            out.extend_from_slice(v);
                                            out.extend_from_slice(b"\r\n");
                                            out
                                        }
                                        None => b"$-1\r\n".to_vec(),
                                    }
                                }
                                b"SETEX" => {
                                    let key = args[1].clone();
                                    let ttl: u64 =
                                        String::from_utf8_lossy(&args[2]).parse().unwrap_or(0);
                                    let val = args[3].clone();
                                    store.lock().unwrap().insert(key, (val, ttl));
                                    b"+OK\r\n".to_vec()
                                }
                                b"DEL" => {
                                    let st = store.lock().unwrap();
                                    let mut n = 0u64;
                                    for k in &args[1..] {
                                        if st.contains_key(k) {
                                            n += 1;
                                        }
                                    }
                                    drop(st);
                                    let mut st = store.lock().unwrap();
                                    for k in &args[1..] {
                                        st.remove(k);
                                    }
                                    format!(":{n}\r\n").into_bytes()
                                }
                                _ => b"-ERR unknown command\r\n".to_vec(),
                            };
                            if s.write_all(&reply).is_err() {
                                break;
                            }
                            let _ = s.flush();
                        }
                    });
                }
            }
        });
        addr
    }

    #[test]
    fn redis_client_get_set_del_roundtrip() {
        let store = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let addr = spawn_mock_redis(store);
        let mut c = RedisClient::connect(&addr, Duration::from_millis(500)).unwrap();

        assert!(c.ping().unwrap());
        assert!(c.get(b"k1").unwrap().is_none(), "未写入应返回 nil");
        c.set_ex(b"k1", b"value-1", 300).unwrap();
        assert_eq!(c.get(b"k1").unwrap().unwrap(), b"value-1");
        c.set_ex(b"k2", b"v2", 60).unwrap();
        assert_eq!(c.del(&[b"k1", b"k2"]).unwrap(), 2);
        assert!(c.get(b"k1").unwrap().is_none());
        assert_eq!(c.del(&[b"k1"]).unwrap(), 0, "已删除再删返回 0");
    }

    #[test]
    fn redis_client_connect_refused() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        assert!(RedisClient::connect(&addr, Duration::from_millis(200)).is_err());
    }
}
