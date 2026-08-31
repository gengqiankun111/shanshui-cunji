//! io_uring 文件 IO 安全封装（unsafe 白名单隔离，V 项）。
//!
//! 背景：主库 `#![forbid(unsafe_code)]`，而 io_uring 需内核异步 IO 接口（提交队列 SQ /
//! 完成队列 CQ，Linux 专属）——故封装隔离到**本独立 crate**（unsafe 白名单位置），
//! 对外只暴露安全 API；非 Linux 平台本 crate 编译为空（`#![cfg(target_os = "linux")]`）。
//! 用途：WAL fsync / SSTable 随机读 走 SQPOLL 提交队列（免 syscall 内核轮询，NVMe 多
//! 硬件队列下按 IoClass 分队列，design_extension v0.5 第 12.3）。
//!
//! # Safety 论证（本 crate unsafe 白名单的完整依据）
//!
//! io_uring 内核接口本身为 unsafe（裸指针缓冲 + 内核异步访问），封装必须满足：
//! 1. **缓冲生命周期**：提交 read/write 时，内核在请求完成前访问缓冲区——本封装采用
//!    **同步提交-等待**模型：submit 后立即 `wait_for_cqe` 阻塞至完成，缓冲区在 submit
//!    期间保持存活（调用方栈/堆）且不可变，完成后才返回 → 满足"完成前缓冲有效"；
//! 2. **fd 生命周期**：每次操作从 `AsRawFd` 取 fd，io_uring 请求生命周期短于文件句柄
//!    （提交-等待同步完成），无悬挂 fd 引用；
//! 3. **SQPOLL 内核线程**：启用时内核轮询线程操作 SQ，与用户线程无共享可变状态竞争
//!    （io-uring crate 内部同步）；提交侧 Mutex 串行化多线程 submit；
//! 4. **Send/Sync**：`IoUringFile` 内部 `io_uring::IoUring` 为 `!Send`（crate 保守标记），
//!    本封装以 `Mutex` 包裹 + 补 `unsafe impl Sync`——依据：所有操作经互斥锁串行提交，
//!    无并发裸指针访问；`&IoUringFile` 跨线程共享 = 串行化进入内核队列，语义安全。

#![cfg(target_os = "linux")]
// （本 crate 为 unsafe 白名单位置，允许 unsafe；主库 forbid(unsafe_code) 承诺不变）

/// io_uring 队列封装（read/write/fsync 同步提交-等待，可选 SQPOLL）。
pub mod queue;
