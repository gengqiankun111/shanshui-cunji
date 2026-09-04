//! `RaftTransport` 的具体传输实现：
//! - `LocalRaftTransport`：进程内队列（测试 / 单机多节点联调）；
//! - `TcpRaftTransport`：JSON-over-TCP（raft 阶段二真实节点间接线，复用 rpc.rs 帧格式）。

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::error::{Error, Result};

use super::{RaftMsg, RaftTransport};

/// 进程内传输：每节点一个收件箱队列（共享中枢 + Mutex，单机多节点联调/测试）。
#[derive(Clone)]
pub struct LocalRaftTransport {
    id: u8,
    hub: Arc<Mutex<HashMap<u8, VecDeque<(u8, RaftMsg)>>>>,
}

impl LocalRaftTransport {
    pub fn new(id: u8, hub: Arc<Mutex<HashMap<u8, VecDeque<(u8, RaftMsg)>>>>) -> Self {
        hub.lock().unwrap().entry(id).or_default();
        Self { id, hub }
    }
}

impl RaftTransport for LocalRaftTransport {
    fn send(&mut self, to: u8, msg: RaftMsg) -> Result<()> {
        self.hub.lock().unwrap().entry(to).or_default().push_back((self.id, msg));
        Ok(())
    }
    fn recv(&mut self) -> Result<Option<(u8, RaftMsg)>> {
        Ok(self.hub.lock().unwrap().get_mut(&self.id).and_then(|q| q.pop_front()))
    }
}

// ============================ TCP 传输（raft 阶段二真实接线） ============================

/// 单帧长度上限（64MB，防恶意长度放大内存；对齐 rpc.rs）。
const MAX_RAFT_FRAME: usize = 64 * 1024 * 1024;

/// 写一帧 `[u32 LE 长度][JSON]`（复用 rpc.rs 帧格式；握手帧同格式）。
fn write_raft_frame(stream: &mut TcpStream, payload: &[u8]) -> Result<()> {
    let len = (payload.len() as u32).to_le_bytes();
    stream.write_all(&len)?;
    stream.write_all(payload)?;
    stream.flush()?;
    Ok(())
}

/// 读一帧（EOF/连接关闭 → Io 错误 → 调用方退出）。
fn read_raft_frame(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_RAFT_FRAME {
        return Err(Error::Rpc(format!("帧长度超限: {len}")));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf)?;
    Ok(buf)
}

/// TCP 传输：真实节点间 JSON-over-TCP（每节点一个监听端口）。
///
/// - **连接握手**：出站连接建立后首帧声明本节点 id（`{"raft_peer_id":N}`），
///   接收端据此确定消息来源（inbox 项 `(from, msg)`）；
/// - **接收**：accept 线程 + 每连接一个 reader 线程 → 反序列化 → 推入收件箱；
/// - **发送**：懒连接缓存（peer id → TcpStream），断线清理下次重连；
/// - `recv` 非阻塞（pop 收件箱），驱动循环轮询（RaftNodeRuntime::pump 语义不变）。
pub struct TcpRaftTransport {
    id: u8,
    peers: HashMap<u8, String>,
    outbound: Mutex<HashMap<u8, TcpStream>>,
    inbox: Arc<Mutex<VecDeque<(u8, RaftMsg)>>>,
    listener: Option<TcpListener>,
    stop: Arc<AtomicBool>,
    accept_thread: Option<JoinHandle<()>>,
}

impl TcpRaftTransport {
    /// 绑定监听端口（`listen_addr` 可 `127.0.0.1:0` 自动分配，经 `peer_addr()` 查询）。
    pub fn bind(id: u8, listen_addr: &str) -> Result<Self> {
        let listener = TcpListener::bind(listen_addr)?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let inbox: Arc<Mutex<VecDeque<(u8, RaftMsg)>>> = Arc::new(Mutex::new(VecDeque::new()));
        let inbox_clone = Arc::clone(&inbox);
        let stop_clone = Arc::clone(&stop);
        let listener_clone = listener
            .try_clone()
            .map_err(|e| Error::Io(e))?;
        // accept 线程：非阻塞轮询 + 每连接一个 reader 线程
        let accept_thread = std::thread::spawn(move || {
            let inbox = Arc::clone(&inbox_clone);
            loop {
                if stop_clone.load(AtomicOrdering::Acquire) {
                    break;
                }
                match listener_clone.accept() {
                    Ok((stream, _)) => {
                        let inbox = Arc::clone(&inbox);
                        std::thread::spawn(move || {
                            let mut stream = stream;
                            // 握手：首帧 = 声明对端节点 id
                            let handshake = match read_raft_frame(&mut stream) {
                                Ok(b) => b,
                                Err(_) => return,
                            };
                            let from: u8 = serde_json::from_slice::<serde_json::Value>(&handshake)
                                .ok()
                                .and_then(|v| v.get("raft_peer_id")?.as_u64())
                                .and_then(|v| u8::try_from(v).ok())
                                .unwrap_or(255);
                            loop {
                                match read_raft_frame(&mut stream) {
                                    Ok(b) => {
                                        if let Ok(msg) = serde_json::from_slice::<RaftMsg>(&b) {
                                            inbox.lock().unwrap().push_back((from, msg));
                                        }
                                    }
                                    Err(_) => break, // 连接关闭/对端断开
                                }
                            }
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => {
                        if stop_clone.load(AtomicOrdering::Acquire) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                }
            }
        });
        Ok(Self {
            id,
            peers: HashMap::new(),
            outbound: Mutex::new(HashMap::new()),
            inbox,
            listener: Some(listener),
            stop,
            accept_thread: Some(accept_thread),
        })
    }

    /// 实际监听地址（`127.0.0.1:0` 绑定后查询真实端口）。
    pub fn peer_addr(&self) -> Result<String> {
        Ok(self
            .listener
            .as_ref()
            .ok_or_else(|| Error::Rpc("listener 已关闭".into()))?
            .local_addr()?
            .to_string())
    }

    /// 登记对端节点地址（peer id → 节点监听地址）。
    pub fn add_peer(&mut self, peer_id: u8, addr: String) {
        self.peers.insert(peer_id, addr);
    }

    /// 停止传输（Drop 兜底）：置 stop 标志 + 释放监听端口。
    /// **不 join accept 线程**——Windows 上 std 非阻塞 accept 实为阻塞等待（内部 poll 无
    /// 超时），stop 无法中断其 accept，join 会挂死（实测挂起根因之一）；Linux 非阻塞
    /// accept 返回 EAGAIN 轮询可正常退出。accept 线程在进程退出时回收（测试/节点停机
    /// 场景均可接受）。
    pub fn shutdown(&mut self) {
        self.stop.store(true, AtomicOrdering::Release);
        self.listener = None; // 释放监听端口
        self.accept_thread.take();
    }

    /// 获取（或建立）到 peer 的连接：首次连接发握手帧声明本节点 id。
    /// 网络调用全部带超时（连接 2s / 读写 5s），防对端不可达挂死驱动循环。
    fn conn(&self, to: u8) -> Result<TcpStream> {
        let addr = self
            .peers
            .get(&to)
            .ok_or_else(|| Error::Rpc(format!("未知 peer {to}")))?;
        let sock: std::net::SocketAddr = addr
            .parse()
            .map_err(|_| Error::Rpc(format!("peer 地址非法: {addr}")))?;
        let mut stream = TcpStream::connect_timeout(&sock, Duration::from_secs(2))?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        let handshake = serde_json::json!({"raft_peer_id": self.id}).to_string();
        write_raft_frame(&mut stream, handshake.as_bytes())?;
        Ok(stream)
    }

    /// 序列化 RaftMsg（serde 错误 → Error::Serialize）。
    fn encode(msg: &RaftMsg) -> Result<Vec<u8>> {
        serde_json::to_vec(msg).map_err(|e| Error::Serialize(e.to_string()))
    }
}

impl Drop for TcpRaftTransport {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl RaftTransport for TcpRaftTransport {
    fn send(&mut self, to: u8, msg: RaftMsg) -> Result<()> {
        let payload = Self::encode(&msg)?;
        // 锁作用域关键：remove 的 MutexGuard 必须显式结束——`if let Some(s) =
        // self.outbound.lock().unwrap().remove(&to)` 的 if-let scrutinee 临时 guard 存活到
        // 整个 if 块，内层 insert 同线程二次 lock 同一 Mutex 死锁（实测挂起根因）。
        let cached = self.outbound.lock().unwrap().remove(&to);
        if let Some(mut s) = cached {
            match write_raft_frame(&mut s, &payload) {
                Ok(()) => {
                    self.outbound.lock().unwrap().insert(to, s);
                    return Ok(());
                }
                Err(_) => {} // 断开 → 清理重连
            }
        }
        let mut s = self.conn(to)?;
        write_raft_frame(&mut s, &payload)?;
        self.outbound.lock().unwrap().insert(to, s);
        Ok(())
    }
    fn recv(&mut self) -> Result<Option<(u8, RaftMsg)>> {
        Ok(self.inbox.lock().unwrap().pop_front())
    }
}
