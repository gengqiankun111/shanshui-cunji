//! 只读磁盘可用空间查询（安全封装）。
//!
//! 背景（对齐 P23 unsafe 白名单决策）：主库 `#![forbid(unsafe_code)]`，而跨平台查询磁盘
//! 剩余空间必须调用 OS API（Windows `GetDiskFreeSpaceExW` / Unix `statvfs`），均为 unsafe。
//! 故隔离到**本独立 crate**（与 `crates/mmap-file` 同模式），对外只暴露安全函数
//! `available_space(&Path) -> io::Result<u64>`，主库源码保持零 unsafe 承诺不变。
//!
//! 用途：看门狗硬盘超限保护（磁盘剩余空间水位分级：预警 / 限流 / 熔断）。
//!
//! # Safety 论证（本 crate unsafe 白名单的完整依据）
//!
//! 1. **只读系统调用**：`GetDiskFreeSpaceExW` / `statvfs` 均为查询语义，不修改任何状态；
//! 2. **内存安全**：参数为栈上局部缓冲区（Windows 三个 `u64` / Unix `statvfs` 结构体），
//!    内核按固定大小写入（`GetDiskFreeSpaceExW` 以 `&mut u64` 由内核填充 8 字节；
//!    `statvfs` 以 `&mut statvfs` 由内核填充固定布局），无越界写、无指针逃逸；
//! 3. **无生命周期耦合**：调用返回即复制出 u64 结果，不保留任何指向系统内存的引用；
//! 4. **空指针安全**：Windows 路径以 `\0` 结尾的 `Vec<u16>`（`as_ptr()` 非空）；Unix
//!    `statvfs` 路径为 `CString`（NUL 结尾，`as_ptr()` 非空）；
//! 5. **错误处理**：返回 0 或负值（statvfs 返回 -1）时映射为 `io::Error`，无未定义行为。

use std::io;
use std::path::Path;

/// 查询路径所在文件系统的空间信息 `(可用字节, 总量字节)`。跨平台：
/// Windows 用 `GetDiskFreeSpaceExW`（返回调用者可用的字节数 + 总字节数）；
/// Unix 用 `statvfs`（`f_bavail × f_frsize` / `f_blocks × f_frsize`）。
pub fn space_info(path: &Path) -> io::Result<(u64, u64)> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        let wide: Vec<u16> = path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        let mut avail = 0u64;
        let mut total = 0u64;
        let mut free = 0u64;
        // SAFETY: 见模块头论证——只读查询；三个 &mut u64 由内核填充固定 8 字节；
        // wide 以 NUL 结尾（as_ptr 非空）；返回值非零表示成功。
        let ok = unsafe {
            winapi_get_free_space(wide.as_ptr(), &mut avail, &mut total, &mut free)
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((avail, total))
    }
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let c = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "路径含 NUL 字节"))?;
        // SAFETY: 见模块头论证——statvfs 只读查询，结构体由 libc crate 正确定义
        // （跨平台布局），c 以 NUL 结尾（as_ptr 非空）；返回 0 表示成功。
        let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::statvfs(c.as_ptr(), &mut st) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        let bsize = st.f_frsize.max(1);
        Ok((
            st.f_bavail.saturating_mul(bsize),
            st.f_blocks.saturating_mul(bsize),
        ))
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = path;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "当前平台不支持磁盘空间查询",
        ))
    }
}

/// 查询路径所在文件系统的可用空间（字节）。跨平台：
/// Windows 用 `GetDiskFreeSpaceExW`（返回调用者可用的字节数）；
/// Unix 用 `statvfs`（`f_bavail × f_frsize`，普通用户可用字节，非 root 视角）。
pub fn available_space(path: &Path) -> io::Result<u64> {
    Ok(space_info(path)?.0)
}

// ============ 平台绑定（仅本 crate 使用，隔离到白名单）============

#[cfg(windows)]
extern "system" {
    fn GetDiskFreeSpaceExW(
        lpDirectoryName: *const u16,
        lpFreeBytesAvailableToCaller: *mut u64,
        lpTotalNumberOfBytes: *mut u64,
        lpTotalNumberOfFreeBytes: *mut u64,
    ) -> i32;
}

#[cfg(windows)]
unsafe fn winapi_get_free_space(
    dir: *const u16,
    avail: *mut u64,
    total: *mut u64,
    free: *mut u64,
) -> i32 {
    // SAFETY: 转发到系统 API（论证见模块头）
    unsafe { GetDiskFreeSpaceExW(dir, avail, total, free) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn available_space_is_positive() {
        let dir = std::env::temp_dir();
        let bytes = available_space(&dir).expect("应能查询临时目录可用空间");
        assert!(bytes > 0, "临时目录可用空间应 > 0，实际 {bytes}");
    }

    #[test]
    fn available_space_cwd() {
        let bytes = available_space(Path::new(".")).expect("应能查询当前目录");
        assert!(bytes > 0);
    }
}
