//! io_uring 文件 IO 封装（Linux）：同步提交-等待模型，可选 SQPOLL 内核轮询。
//!
//! # 模型与安全性
//!
//! 每个操作 = 1 个 SQE 提交 + 1 个 CQE 等待（同步完成）：
//! - 缓冲生命周期：`submit_entry` 前缓冲区存活（调用方借用），`wait_for_cqe` 返回后
//!   内核已完成访问 → 满足"完成前缓冲有效"（无悬垂指针）；
//! - fd 生命周期：请求同步完成，不持有跨调用的 fd 引用；
//! - 多线程：`Mutex<IoUring>` 串行化提交（io_uring 实例非 Sync，mutex 包裹 + 补 Sync），
//!   无并发裸指针进入内核队列；
//! - SQPOLL：内核线程轮询 SQ（`setup_sqpoll` + 可选 `setup_sq_thread_cpu` 绑核），
//!   用户线程提交免 `io_uring_enter` syscall（高 IOPS 场景省系统调用开销）。
//!
//! unsafe 仅出现在调用 io-uring crate 的 `submit_entry`（其内部操作 SQ 裸内存）；
//! 依据上述生命周期论证，封装语义安全（本 crate 为 unsafe 白名单位置，主库零 unsafe 不变）。

use io_uring::{opcode, types, IoUring, squeue::Entry};
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::sync::Mutex;

/// io_uring 队列（单实例，Mutex 串行化提交）。
pub struct IoUringFile {
    ring: Mutex<IoUring>,
}

/// 队列参数。
#[derive(Debug, Clone, Copy)]
pub struct QueueParams {
    /// 提交队列深度（SQE 槽位数，2 的幂）。
    pub entries: u32,
    /// SQPOLL 内核线程空闲退出时间（µs）；0 = 关闭 SQPOLL（每次提交走 enter syscall）。
    pub sqpoll_idle_us: u32,
    /// SQPOLL 内核线程绑定的 CPU（`setup_sq_thread_cpu`）；None = 不绑。
    pub sqpoll_cpu: Option<u32>,
}

impl Default for QueueParams {
    fn default() -> Self {
        Self {
            entries: 256,
            sqpoll_idle_us: 0,
            sqpoll_cpu: None,
        }
    }
}

impl IoUringFile {
    /// 创建 io_uring 队列（可选 SQPOLL + 绑核）。
    pub fn open(params: QueueParams) -> io::Result<Self> {
        let entries = params.entries.max(8).next_power_of_two();
        let mut builder = IoUring::builder();
        if params.sqpoll_idle_us > 0 {
            builder.setup_sqpoll(params.sqpoll_idle_us);
            if let Some(cpu) = params.sqpoll_cpu {
                builder.setup_sqpoll_cpu(cpu);
            }
        }
        let ring = builder.build(entries)?;
        Ok(Self {
            ring: Mutex::new(ring),
        })
    }

    /// 提交单个 op 并同步等待完成：push SQE → submit_and_wait(1) → 消费 CQE 返回结果。
    /// `res` = 内核返回（≥0 字节数；<0 为 -errno）。
    fn submit_and_wait(ring: &mut IoUring, entry: Entry) -> io::Result<i32> {
        // SAFETY: 缓冲与 fd 生命周期见模块头论证——同步提交-等待，submit_and_wait 返回后
        // 内核已完成访问；push 将 entry 复制进 SQ（内核随后读取），调用期内 entry 有效。
        unsafe { ring.submission().push(&entry) }
            .map_err(|e| io::Error::other(format!("io_uring SQE 入队失败: {e:?}")))?;
        ring.submitter().submit_and_wait(1)?;
        // 只提交了一个 op → 消费首个 CQE（result = 字节数或 -errno）
        for cqe in ring.completion() {
            return Ok(cqe.result());
        }
        Err(io::Error::other("io_uring 无完成事件"))
    }

    /// 同步读：偏移 `offset` 读 `buf.len()` 字节到 `buf`。返回实际读字节数。
    pub fn read_at(&self, file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let ring = &mut *self.ring.lock().unwrap();
        let entry = opcode::Read::new(
            types::Fd(file.as_raw_fd()),
            buf.as_mut_ptr(),
            buf.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(0x01);
        let res = Self::submit_and_wait(ring, entry)?;
        if res < 0 {
            Err(io::Error::from_raw_os_error(-res))
        } else {
            Ok(res as usize)
        }
    }

    /// 同步写：偏移 `offset` 写 `buf` 全部字节。返回实际写字节数。
    pub fn write_at(&self, file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
        let ring = &mut *self.ring.lock().unwrap();
        let entry = opcode::Write::new(
            types::Fd(file.as_raw_fd()),
            buf.as_ptr(),
            buf.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(0x02);
        let res = Self::submit_and_wait(ring, entry)?;
        if res < 0 {
            Err(io::Error::from_raw_os_error(-res))
        } else {
            Ok(res as usize)
        }
    }

    /// 同步 fsync（文件全部数据落盘，WAL 提交点）。
    pub fn fsync(&self, file: &File) -> io::Result<()> {
        let ring = &mut *self.ring.lock().unwrap();
        let entry = opcode::Fsync::new(types::Fd(file.as_raw_fd()))
            .build()
            .user_data(0x03);
        let res = Self::submit_and_wait(ring, entry)?;
        if res < 0 {
            Err(io::Error::from_raw_os_error(-res))
        } else {
            Ok(())
        }
    }
}

// SAFETY: 所有操作经 Mutex 串行化，无并发裸指针进入内核；`&IoUringFile` 跨线程共享 =
// 串行进入队列，语义安全（论证见模块头）。
unsafe impl Sync for IoUringFile {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    fn params() -> QueueParams {
        QueueParams {
            entries: 64,
            sqpoll_idle_us: 0, // 测试用普通模式（无内核线程依赖）
            sqpoll_cpu: None,
        }
    }

    #[test]
    fn read_write_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.bin");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"hello io_uring world").unwrap();
        f.sync_all().unwrap();
        drop(f);

        let file = std::fs::File::open(&path).unwrap();
        let q = IoUringFile::open(params()).unwrap();
        let mut buf = vec![0u8; 5];
        let n = q.read_at(&file, &mut buf, 0).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
        let mut buf2 = vec![0u8; 11];
        let n = q.read_at(&file, &mut buf2, 6).unwrap();
        assert_eq!(n, 11);
        assert_eq!(&buf2, b"io_uring w");
    }

    #[test]
    fn write_and_fsync_then_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w.bin");
        let file = std::fs::File::options()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let q = IoUringFile::open(params()).unwrap();
        let n = q.write_at(&file, b"xyz", 0).unwrap();
        assert_eq!(n, 3);
        q.fsync(&file).unwrap();
        drop(q);

        let mut f = std::fs::File::open(&path).unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();
        assert_eq!(s, "xyz");
    }

    #[test]
    fn read_past_eof_returns_short() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("e.bin");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"abc").unwrap();
        drop(f);
        let file = std::fs::File::open(&path).unwrap();
        let q = IoUringFile::open(params()).unwrap();
        let mut buf = vec![0u8; 64];
        let n = q.read_at(&file, &mut buf, 0).unwrap();
        assert_eq!(n, 3);
        // 越界读：返回 0（非错误）
        let n = q.read_at(&file, &mut buf, 100).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn concurrent_reads_serialized_via_mutex() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.bin");
        let data: Vec<u8> = (0..256u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &data).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let q = std::sync::Arc::new(IoUringFile::open(params()).unwrap());
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let q = std::sync::Arc::clone(&q);
            let f = file.try_clone().unwrap();
            handles.push(std::thread::spawn(move || {
                let mut buf = vec![0u8; 64];
                let off = t * 64;
                let n = q.read_at(&f, &mut buf, off).unwrap();
                assert_eq!(n, 64);
                for (i, b) in buf.iter().enumerate() {
                    assert_eq!(*b, data[(off as usize + i) % data.len()]);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
}
