
//! MySQL 协议服务器主体（server/server.rs）：内容拆分自原 src/db_adapter.rs——DbServer
//! 主结构体 + 生命周期（后台 worker / serve / serve_async / serve_once）与同步 / 异步单连接
//! 处理（handle_connection / handle_connection_async）。

use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::server::*;


// ============ MySQL 服务器 ============

/// MySQL 协议服务器：持有引擎（Arc<RwLock<Engine>>，O 项第②步：读语句读锁并行、
/// 写语句写锁互斥；每连接独立线程处理握手 → 认证 → 命令循环。
pub struct DbServer {
    // 字段 pub(crate)：原 db_adapter.rs mod tests 同模块可直读 engine / spawn_* worker
    // （拆分后 tests.rs 在 server 根，跨文件访问需提升可见性，语义零变化）。
    pub(crate) engine: Arc<RwLock<Engine>>,
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) next_conn_id: AtomicU64,
    /// 无 id 列 INSERT 的 auto_increment 计数器（跨连接共享）。
    pub(crate) auto_id: Arc<AtomicU64>,
    /// Ex-8.9：后台维护 worker 是否**负载感知**（Busy 退避 / Idle 密集集中）。默认开；
    /// A/B 验收时关 = 旧固定节奏行为。
    pub(crate) idle_aware: bool,
}

impl DbServer {
    pub fn new(engine: Engine, user: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            engine: Arc::new(RwLock::new(engine)),
            user: user.into(),
            password: password.into(),
            next_conn_id: AtomicU64::new(1),
            auto_id: Arc::new(AtomicU64::new(1)),
            idle_aware: true,
        }
    }

    /// Ex-8.9：开关后台 worker 负载感知（serve/spawn 前调用）。
    pub fn set_idle_aware(&mut self, on: bool) {
        self.idle_aware = on;
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
    pub(crate) fn spawn_compaction_worker(&self) {
        let engine = self.engine.clone();
        let aware = self.idle_aware; // Ex-8.9：负载感知开关（关 = 旧固定节奏 100ms）
        let pending = self.engine.read().unwrap().compact_pending.clone();
        let worker = self.engine.read().unwrap().compact_worker.clone();
        worker.store(true, Ordering::Release); // 写路径此后只发信号
        std::thread::spawn(move || {
            let mut last_backstop = std::time::Instant::now();
            let mut last_idle_run = std::time::Instant::now();
            let mut last_ops = (0u64, 0u64);
            let mut idle_streak = 0u32;
            loop {
                 // Ex-8.9 空闲感知：Busy → 退避 1s；Normal → 200ms；Idle → 50ms 密集检查
                 //（aware=false → 旧固定节奏 100ms）
                 let (busy, idle) = if aware {
                     if let Ok(g) = engine.read() {
                         let w = g.metrics.write_ops.load(std::sync::atomic::Ordering::Relaxed);
                         let r = g.metrics.read_ops.load(std::sync::atomic::Ordering::Relaxed);
                         let dw = w.wrapping_sub(last_ops.0);
                         let dr = r.wrapping_sub(last_ops.1);
                         last_ops = (w, r);
                         (
                             g.write_pressure() >= 0.5 || dw >= 4 || dr >= 10,
                             g.write_pressure() == 0.0 && dw == 0 && dr == 0,
                         )
                     } else {
                         (true, false)
                     }
                 } else {
                     (false, false)
                 };
                 if idle {
                     idle_streak += 1;
                 } else {
                     idle_streak = 0;
                 }
                 let tick_ms = if !aware {
                     100
                 } else if busy {
                     1000
                 } else if idle {
                     50
                 } else {
                     200
                 };
                 std::thread::sleep(std::time::Duration::from_millis(tick_ms));
                let signaled = pending.swap(false, Ordering::AcqRel);
                let backstop = last_backstop.elapsed() >= std::time::Duration::from_secs(600);
                // 空闲集中：连续空闲 ≥4 tick（约 ≥200ms 无读写）且距上次集中 ≥5s → 强制执行一轮
                let idle_run = idle
                    && idle_streak >= 4
                    && last_idle_run.elapsed() >= std::time::Duration::from_secs(5);
                if signaled || backstop || idle_run {
                    // 读锁内 clone 目标（Arc clone 廉价）→ drop 锁 → 无锁合并
                    let targets = engine.read().map(|g| g.compaction_targets()).unwrap_or(None);
                    if let Some(t) = targets {
                        // P80：合并失败记日志，不 panic（下一轮信号/兜底再试）
                        if let Err(e) = t.run() {
                            tracing::warn!("后台合并 worker 失败: {e}，下一轮再试");
                        }
                    }
                    if backstop {
                        last_backstop = std::time::Instant::now();
                    }
                    if idle_run {
                        last_idle_run = std::time::Instant::now();
                    }
                }
            }
        });
    }

    /// J 项（7.73）：倒排段 GC 后台 worker。写路径（Engine::flush_inverted）检测段超
    /// GC 阈值后置 `inverted_gc_pending` 信号；本线程读取信号后检查 `should_gc()` 并执行
    /// `InvertedIndex::gc()`——Engine 读锁内快速 clone inverted Arc → **drop 锁** → 无锁
    /// 执行 gc（gc 内部 mutate 锁仅与写路径 flush_segment 互斥，不阻塞查询读）。
    /// 10 分钟兜底：覆盖刷盘未触发 / 信号丢失等异常路径（段数爆炸不再依赖显式调用）。
    pub(crate) fn spawn_inverted_gc_worker(&self) {
        let engine = self.engine.clone();
        let aware = self.idle_aware; // Ex-8.9：负载感知开关（关 = 旧固定节奏 100ms）
        let pending = self.engine.read().unwrap().inverted_gc_pending.clone();
        std::thread::spawn(move || {
            let mut last_backstop = std::time::Instant::now();
            let mut last_idle_run = std::time::Instant::now();
            let mut last_ops = (0u64, 0u64);
            let mut idle_streak = 0u32;
            loop {
                // Ex-8.9 空闲感知：Busy → 退避 1s；Normal → 200ms；Idle → 50ms 密集检查
                //（aware=false → 旧固定节奏 100ms）
                let (busy, idle) = if aware {
                    if let Ok(g) = engine.read() {
                        let w = g.metrics.write_ops.load(std::sync::atomic::Ordering::Relaxed);
                        let r = g.metrics.read_ops.load(std::sync::atomic::Ordering::Relaxed);
                        let dw = w.wrapping_sub(last_ops.0);
                        let dr = r.wrapping_sub(last_ops.1);
                        last_ops = (w, r);
                        (
                            g.write_pressure() >= 0.5 || dw >= 4 || dr >= 10,
                            g.write_pressure() == 0.0 && dw == 0 && dr == 0,
                        )
                    } else {
                        (true, false)
                    }
                } else {
                    (false, false)
                };
                if idle {
                    idle_streak += 1;
                } else {
                    idle_streak = 0;
                }
                let tick_ms = if !aware {
                    100
                } else if busy {
                    1000
                } else if idle {
                    50
                } else {
                    200
                };
                std::thread::sleep(std::time::Duration::from_millis(tick_ms));
                let signaled = pending.swap(false, Ordering::AcqRel);
                let backstop = last_backstop.elapsed() >= std::time::Duration::from_secs(600);
                // Ex-8.9：空闲集中——连续空闲且距上次集中 ≥5s → 强制检查 GC
                let idle_run = idle
                    && idle_streak >= 4
                    && last_idle_run.elapsed() >= std::time::Duration::from_secs(5);
                if signaled || backstop || idle_run {
                    // 读锁内 clone inverted Arc（廉价）→ drop 锁 → 无锁 gc
                    let inverted = engine.read().ok().map(|g| g.inverted.clone());
                    if let Some(inv) = inverted {
                        if inv.should_gc() {
                            let _ = inv.gc();
                        }
                    }
                    if backstop {
                        last_backstop = std::time::Instant::now();
                    }
                    if idle_run {
                        last_idle_run = std::time::Instant::now();
                    }
                }
            }
        });
    }

    /// 7.93：倒排**落盘**后台 worker——把内存 term 落段持久化（重启后字段等值查询不丢）。
    /// 背景：引擎写路径只把 term 攒入内存（pending_inverted/mem），落段依赖显式
    /// `flush_inverted`；cjserver 服务端场景无显式落盘 → 进程重启内存 term 即失，
    /// 字段等值过滤查空（7.93 实测 `status='active'` 0 行）。本线程周期落盘：
    /// 内存 term 超阈值实时刷 + 30s 非空兜底（段文件持久，重启后经 FST 可查）。
    /// 段数增长由 GC worker（7.73 信号 + 10 分钟兜底）收敛。
    pub(crate) fn spawn_inverted_flush_worker(&self) {
        let engine = self.engine.clone();
        let aware = self.idle_aware; // Ex-8.9：负载感知开关（关 = 旧固定节奏 200ms 轮询）
        std::thread::spawn(move || {
            let mut last = std::time::Instant::now();
            let mut last_ops = (0u64, 0u64);
            loop {
                // Ex-8.9：Busy → 1s（仅硬阈值落盘）；Idle → 50ms tick + 1s 密集落盘
                //（aware=false → 旧固定节奏 200ms 轮询 + 仅硬阈值/30s 兜底落盘）
                let (busy, idle) = if aware {
                    if let Ok(g) = engine.read() {
                        let w = g.metrics.write_ops.load(std::sync::atomic::Ordering::Relaxed);
                        let r = g.metrics.read_ops.load(std::sync::atomic::Ordering::Relaxed);
                        let dw = w.wrapping_sub(last_ops.0);
                        let dr = r.wrapping_sub(last_ops.1);
                        last_ops = (w, r);
                        (
                            g.write_pressure() >= 0.5 || dw >= 4 || dr >= 10,
                            g.write_pressure() == 0.0 && dw == 0 && dr == 0,
                        )
                    } else {
                        (true, false)
                    }
                } else {
                    (false, false)
                };
                let tick_ms = if !aware {
                    200
                } else if busy {
                    1000
                } else if idle {
                    50
                } else {
                    200
                };
                std::thread::sleep(std::time::Duration::from_millis(tick_ms));
                let mem = engine
                    .read()
                    .ok()
                    .map(|g| g.inverted_mem_docids())
                    .unwrap_or(0);
                if mem == 0 {
                    continue;
                }
                // 攒批涨得快 → 达阈值立即落盘；空闲 → 1s 密集落盘；兜底 30s
                let force = mem >= INVERTED_MEM_FLUSH_THRESHOLD;
                let fast = idle && last.elapsed() >= std::time::Duration::from_secs(1);
                let slow = last.elapsed() >= std::time::Duration::from_secs(30);
                if force || fast || slow {
                    if let Ok(g) = engine.read() {
                        let _ = g.flush_inverted();
                    }
                    last = std::time::Instant::now();
                }
            }
        });
    }

    /// 绑定并接受连接（阻塞）。每连接 spawn 线程处理。返回**实际绑定地址**
    /// （addr 为 `127.0.0.1:0` 时返回 OS 分配的随机端口——并发测试/动态端口场景）。
    pub fn serve(self, addr: &str) -> Result<std::net::SocketAddr> {
        // O 项第③步：后台合并 worker（信号驱动 + 10 分钟兜底）——写路径只发信号，
        // 合并读锁下执行，读写均不被合并阻塞（替代 P 项写路径同步合并 + guard 定时器）。
        self.spawn_compaction_worker();
        // J 项（7.73）：倒排段 GC 后台 worker（信号 + 10 分钟兜底）
        self.spawn_inverted_gc_worker();
        // 7.93：倒排落盘 worker（内存 term → 段文件持久，重启后等值可查）
        self.spawn_inverted_flush_worker();
        let listener = TcpListener::bind(addr)?;
        let local = listener.local_addr()?;
        tracing::info!("MySQL 协议服务已启动: mysql://{addr}（库 {DEFAULT_DB}，表 {DEFAULT_TABLE}）");
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else {
                continue;
            };
            // 协议低延迟：禁 Nagle（Linux loopback 延迟 ACK 会给小请求加 ~40ms 往返；
            // MySQL 服务端同此设置）。benchmark/sysbench 每语句一个往返，此项必需。
            let _ = stream.set_nodelay(true);
            let engine = self.engine.clone();
            let user = self.user.clone();
            let password = self.password.clone();
            let auto_id = self.auto_id.clone();
            let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
            // I 项高并发：连接线程小栈（512KB，默认 2MB/8MB）——高连接数下大幅降虚拟内存
            // 占用（10k 连接 × 栈 = 5GB vs 20GB+），支撑更多并发查询连接
            std::thread::Builder::new()
                .name(format!("mysql-conn-{conn_id}"))
                .stack_size(512 * 1024)
                .spawn(move || {
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
                })
                .expect("连接线程 spawn 失败");
        }
        Ok(local)
    }

    /// 异步服务（design 9.5 10k 连接目标）：tokio accept 循环，每连接一个 **task**——
    /// 连接 idle 不占 OS 线程；查询经 `spawn_blocking` 复用同步引擎（活跃查询才占阻塞线程）。
    /// 需在 tokio runtime 内调用（`#[tokio::main]` / `tokio::runtime`）。返回实际绑定地址。
    pub async fn serve_async(self, addr: &str) -> Result<std::net::SocketAddr> {
        self.spawn_compaction_worker();
        // J 项（7.73）：倒排段 GC 后台 worker
        self.spawn_inverted_gc_worker();
        // 7.93：倒排落盘 worker（内存 term → 段文件持久，重启后等值可查）
        self.spawn_inverted_flush_worker();
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        tracing::info!(
            "MySQL 协议服务已启动（异步协程）: mysql://{addr}（库 {DEFAULT_DB}，表 {DEFAULT_TABLE}）"
        );
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("异步 accept 错误: {e}");
                    continue;
                }
            };
            // 协议低延迟：禁 Nagle（见同步 serve 注释）——每语句一个往返，must。
            let _ = stream.set_nodelay(true);
            let engine = self.engine.clone();
            let user = self.user.clone();
            let password = self.password.clone();
            let auto_id = self.auto_id.clone();
            let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                // X 项：连接计数（活跃/累计，/metrics 指标）
                if let Ok(g) = engine.read() {
                    g.metrics.active_conns.fetch_add(1, Ordering::Relaxed);
                    g.metrics.total_conns.fetch_add(1, Ordering::Relaxed);
                }
                let r = handle_connection_async(
                    &mut stream,
                    engine.clone(),
                    &user,
                    &password,
                    conn_id,
                    auto_id,
                )
                .await;
                if let Ok(g) = engine.read() {
                    g.metrics.active_conns.fetch_add(-1, Ordering::Relaxed);
                }
                if let Err(e) = r {
                    tracing::warn!("MySQL 会话结束（异步）: {e}");
                }
            });
        }
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
pub(crate) fn handle_connection(
    stream: &mut TcpStream,
    engine: Arc<RwLock<Engine>>,
    user: &str,
    password: &str,
    conn_id: u64,
    auto_id: Arc<AtomicU64>,
) -> Result<()> {
    let mut session = new_session(auto_id);
    // ① 握手（HandshakeV10）
    let scramble = gen_scramble(conn_id);
    write_packet(stream, 0, &build_handshake_packet(conn_id, &scramble))?;

    // ② 读握手响应
    let (_, resp) = read_packet(stream)?;
    tracing::debug!(
        "握手响应 {} 字节: {:02x?}",
        resp.len(),
        &resp[..resp.len().min(96)]
    );
    let ok = parse_handshake_response(&resp, &mut session, &user, &password, &scramble)?;
    if !ok {
        tracing::debug!("认证失败: user={} auth_len={}", session.user, resp.len());
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
        let (cmd_seq, cmd) = match read_command(stream) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        tracing::debug!("收到命令包 {} 字节: {:02x?}", cmd.len(), &cmd[..cmd.len().min(16)]);
        if cmd.is_empty() {
            continue;
        }
        // 命令分发（无 IO 逻辑——同步/异步连接共用，异步路径经 spawn_blocking）
        let (_, packets) = handle_command(&engine, &mut session, &cmd);
        let Some(packets) = packets else {
            return Ok(()); // COM_QUIT / EOF
        };
        let mut seq = cmd_seq;
        for p in packets {
            write_packet(stream, seq, &p)?;
            seq = seq.wrapping_add(1);
        }
    }
}
/// 异步单连接处理：握手 → 认证 → 命令循环。
/// **连接 idle 不占 OS 线程**（tokio task）；查询经 `spawn_blocking` 复用同步引擎
/// （引擎 RwLock + session 独占在阻塞线程执行）——10k 长连接仅活跃查询占线程。
pub(crate) async fn handle_connection_async(
    stream: &mut tokio::net::TcpStream,
    engine: Arc<RwLock<Engine>>,
    user: &str,
    password: &str,
    conn_id: u64,
    auto_id: Arc<AtomicU64>,
) -> Result<()> {
    // session 用 Option 包装：spawn_blocking 独占期间 take，处理完归还
    let mut session: Option<Session> = Some(new_session(auto_id));
    // ① 握手（HandshakeV10）
    let scramble = gen_scramble(conn_id);
    write_packet_async(stream, 0, &build_handshake_packet(conn_id, &scramble)).await?;
    // ② 读握手响应 + 认证（native_password）
    let (_, resp) = read_packet_async(stream).await?;
    let ok = parse_handshake_response(
        &resp,
        session.as_mut().unwrap(),
        &user,
        &password,
        &scramble,
    )?;
    if !ok {
        let _ =
            write_packet_async(stream, 2, &err_payload(1045, "Access denied for user")).await?;
        return Err(Error::Cluster("认证失败".into()));
    }
    session.as_mut().unwrap().authenticated = true;
    write_packet_async(stream, 2, &ok_payload(0, 0)).await?;
    // ③ 命令循环：异步读包（idle 不占线程）→ spawn_blocking 执行查询 → 异步写响应
    loop {
        let (cmd_seq, cmd) = match read_command_async(stream).await {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(Error::Io(e)),
        };
        if cmd.is_empty() {
            continue;
        }
        let engine2 = engine.clone();
        let mut sess = session.take().expect("session 应存在");
        let r = tokio::task::spawn_blocking(move || {
            let (_, pkts) = handle_command(&engine2, &mut sess, &cmd);
            (sess, pkts)
        })
        .await
        .map_err(|e| {
            Error::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("blocking task: {e}")))
        })?;
        session = Some(r.0);
        let Some(pkts) = r.1 else {
            return Ok(()); // COM_QUIT
        };
        let mut seq = cmd_seq;
        for p in pkts {
            write_packet_async(stream, seq, &p).await?;
            seq = seq.wrapping_add(1);
        }
    }
}
