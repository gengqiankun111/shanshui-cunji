//! IO 队列抽象（Ex-7.3，design_extension v0.5 第 12.3）：WAL / SSTable / 倒排分队列。
//!
//! 目标（io_uring SQPOLL + 多队列）：NVMe 多硬件队列下，WAL 与 SSTable 落不同队列、
//! 各归各的提交组（WAL fsync 与刷盘并行），避免单队列拥塞。
//!
//! 当前实现：`io_uring` 为 Linux 专属且需 unsafe 依赖（memmap 同策略留待独立 crate 封装），
//! Windows 开发环境不可用——故本模块落地**队列分类抽象 + 配置**：
//! - `IoClass`：按 IO 类型分类（Wal / Sst / Inverted），映射到 Ex-5.10 的多盘目录
//!   （wal_dir / sst_dir / inverted_dir 即物理队列）；
//! - `io_queue_count`：NVMe 多队列数配置（预留；1 = 单队列旧行为）；
//! - io_uring 后端接入点：Linux 部署启用 `runtime.io_uring_enabled` 时，各 IoClass
//!   映射到 `class.queue_id()` 对应的 SQPOLL 提交队列（liburing 独立 crate 封装后接入）。

use crate::config::model::RuntimeConfig;

/// IO 类型分类：决定物理队列/盘（与 Ex-5.10 多盘条带化对齐）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IoClass {
    /// WAL（写路径 fsync 热点，独占最快盘/队列）。
    Wal,
    /// SSTable（主数据/组合索引/增量列族数据）。
    Sst,
    /// 倒排段与字典。
    Inverted,
}

impl IoClass {
    /// 队列号：多队列配置下按类分队列（0 基）；单队列（默认）全为 0。
    pub fn queue_id(&self) -> usize {
        match self {
            IoClass::Wal => 0,
            IoClass::Sst => 1,
            IoClass::Inverted => 2,
        }
    }

    /// 展示名（日志/监控）。
    pub fn as_str(&self) -> &'static str {
        match self {
            IoClass::Wal => "wal",
            IoClass::Sst => "sst",
            IoClass::Inverted => "inverted",
        }
    }
}

/// 当前是否启用 io_uring（Linux 部署配置 `runtime.io_uring_enabled = true` 时，
/// 各 IoClass 由 `queue_id()` 路由到对应 SQPOLL 队列；Windows 开发恒 false）。
pub fn io_uring_enabled(cfg: &RuntimeConfig) -> bool {
    cfg.io_uring_enabled && cfg!(target_os = "linux")
}

/// V 项：io_uring 后端池（Linux 专属）——按 IoClass 三队列（WAL/SST/倒排）SQPOLL 实例，
/// read_at/write_at/fsync 转发。主库零 unsafe（unsafe 在 io-uring-file crate 白名单内）。
/// 非 Linux 目标编译为空类型（io-uring-file crate 在非 Linux 为空）。
#[cfg(target_os = "linux")]
pub mod backend {
    use super::IoClass;
    use io_uring_file::queue::{IoUringFile, QueueParams};
    use std::fs::File;
    use std::io;

    /// 三队列池（每 IoClass 一个 SQPOLL 实例；`queue_id()` 路由）。
    pub struct IoUringPool {
        queues: [IoUringFile; 3],
    }

    impl IoUringPool {
        /// 初始化三队列。`sqpoll_idle_us`：SQPOLL 空闲退出（µs）；`sqpoll_cpu`：内核
        /// 轮询线程绑核（V 项：affinity 三池外预留核，防与用户线程抢核）。
        pub fn open(
            entries: u32,
            sqpoll_idle_us: u32,
            sqpoll_cpu: Option<u32>,
        ) -> io::Result<Self> {
            let params = QueueParams {
                entries,
                sqpoll_idle_us,
                sqpoll_cpu,
            };
            let queues = [
                IoUringFile::open(params)?,
                IoUringFile::open(params)?,
                IoUringFile::open(params)?,
            ];
            Ok(Self { queues })
        }

        fn q(&self, class: IoClass) -> &IoUringFile {
            &self.queues[class.queue_id()]
        }

        pub fn read_at(&self, class: IoClass, file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.q(class).read_at(file, buf, offset)
        }

        pub fn write_at(&self, class: IoClass, file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
            self.q(class).write_at(file, buf, offset)
        }

        pub fn fsync(&self, class: IoClass, file: &File) -> io::Result<()> {
            self.q(class).fsync(file)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_ids_are_disjoint_by_class() {
        // 三类队列号互异：WAL=0 / SST=1 / 倒排=2（多队列路由正确性）
        let ids: Vec<usize> = [IoClass::Wal, IoClass::Sst, IoClass::Inverted]
            .iter()
            .map(|c| c.queue_id())
            .collect();
        let mut uniq = ids.clone();
        uniq.sort_unstable();
        uniq.dedup();
        assert_eq!(uniq.len(), 3, "三类队列号应互异: {ids:?}");
        assert_eq!(IoClass::Wal.as_str(), "wal");
        assert_eq!(IoClass::Sst.as_str(), "sst");
        assert_eq!(IoClass::Inverted.as_str(), "inverted");
    }

    #[test]
    fn io_uring_only_on_linux_when_enabled() {
        // Windows 开发环境恒 false（即便配置开启）；Linux 且启用才为 true
        let cfg_on = RuntimeConfig {
            io_uring_enabled: true,
            ..Default::default()
        };
        assert_eq!(io_uring_enabled(&cfg_on), cfg!(target_os = "linux"));
        let cfg_off = RuntimeConfig {
            io_uring_enabled: false,
            ..Default::default()
        };
        assert!(!io_uring_enabled(&cfg_off));
    }
}
