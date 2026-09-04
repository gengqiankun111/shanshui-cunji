//! 备份 / 还原（design 8.1 / development 步骤 14）。
//!
//! 冷备份：调用方先做一致性准备（`Engine::prepare_backup`：刷 WAL + 全部 MemTable 落盘 +
//! 倒排内存字典刷盘），随后 `backup` 递归打包整个数据目录（含 SST、倒排 `.seg` 段、
//! 段清单 Manifest、WAL、字段注册表等全部文件）为单个备份文件；
//! 还原：停止服务后清空数据目录 → 解压 → 校验魔数 / 版本 / CRC → 重启加载。
//!
//! 备份文件格式（版本 1；格式兼容约束见 development 4.6）：
//! ```text
//! Magic "SSCJBK01" (8B) ++ Version(u16 LE) ++ EntryCount(u64 LE)
//! ++ [Entry, ...]                          // 逐条目交错（头 + 负载）
//! Entry := PathLen(u32 LE) ++ Path ++ Size(u64 LE) ++ Crc32(u32 LE) ++ Payload
//! ```
//! 路径为相对路径（统一 `/` 分隔）；还原时拒绝绝对路径、`..`、空路径（防穿越）。
//! 单文件流式读写：内存占用 O(单文件)，可备份 GB 级数据目录。

use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::Instant;

use crate::error::{Error, Result};

/// 备份文件魔数。
pub const BACKUP_MAGIC: &[u8; 8] = b"SSCJBK01";
/// 备份文件格式版本（未来格式演进只允许新增段/字段，见 development 4.6）。
pub const BACKUP_VERSION: u16 = 1;

/// 备份结果报告。
#[derive(Debug)]
pub struct BackupReport {
    pub entry_count: usize,
    pub total_bytes: u64,
    pub elapsed_ms: f64,
}

/// 还原结果报告。
#[derive(Debug)]
pub struct RestoreReport {
    pub entry_count: usize,
    pub total_bytes: u64,
    pub elapsed_ms: f64,
}

/// 计算 CRC32（备份条目完整性校验）。
fn crc32(data: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(data);
    h.finalize()
}

/// 将 `data_dir` 递归打包为单个备份文件（确定性顺序：按相对路径排序）。
pub fn backup(data_dir: &Path, backup_path: &Path) -> Result<BackupReport> {
    if !data_dir.is_dir() {
        return Err(Error::NotFound(format!(
            "数据目录不存在: {}",
            data_dir.display()
        )));
    }
    let mut files: Vec<(String, PathBuf)> = Vec::new();
    collect_files(data_dir, &mut files)?;
    files.sort_by(|a, b| a.0.cmp(&b.0));

    let t = Instant::now();
    let mut out = std::fs::File::create(backup_path)?;
    out.write_all(BACKUP_MAGIC)?;
    out.write_all(&BACKUP_VERSION.to_le_bytes())?;
    out.write_all(&(files.len() as u64).to_le_bytes())?;

    let mut total_bytes: u64 = 0;
    for (rel, path) in &files {
        let data = std::fs::read(path)?;
        let pb = rel.as_bytes();
        out.write_all(&(pb.len() as u32).to_le_bytes())?;
        out.write_all(pb)?;
        out.write_all(&(data.len() as u64).to_le_bytes())?;
        out.write_all(&crc32(&data).to_le_bytes())?;
        out.write_all(&data)?;
        total_bytes += data.len() as u64;
    }
    out.sync_all()?;

    Ok(BackupReport {
        entry_count: files.len(),
        total_bytes,
        elapsed_ms: t.elapsed().as_secs_f64() * 1000.0,
    })
}

/// 从备份文件还原到 `data_dir`（先清空目标目录，冷备份语义：服务停止后执行）。
pub fn restore(backup_path: &Path, data_dir: &Path) -> Result<RestoreReport> {
    let mut f = std::fs::File::open(backup_path)?;

    // 魔数
    let mut magic = [0u8; 8];
    f.read_exact(&mut magic)?;
    if &magic != BACKUP_MAGIC {
        return Err(Error::Corrupted(format!(
            "备份文件魔数错误（{}，期望 SSCJBK01）",
            String::from_utf8_lossy(&magic)
        )));
    }
    // 版本（只拒绝更高版本，低版本旧格式读取路径按 4.6 保留）
    let mut vbuf = [0u8; 2];
    f.read_exact(&mut vbuf)?;
    let version = u16::from_le_bytes(vbuf);
    if version > BACKUP_VERSION {
        return Err(Error::Unsupported(format!(
            "备份文件版本 {version} 高于当前支持版本 {BACKUP_VERSION}"
        )));
    }
    // 条目数
    let mut cnt_buf = [0u8; 8];
    f.read_exact(&mut cnt_buf)?;
    let count = u64::from_le_bytes(cnt_buf);

    // 清空目标目录（防止旧数据与新数据混叠）
    if data_dir.exists() {
        std::fs::remove_dir_all(data_dir)?;
    }
    std::fs::create_dir_all(data_dir)?;

    let t = Instant::now();
    let mut total_bytes: u64 = 0;
    for _ in 0..count {
        // 路径
        let mut plen_buf = [0u8; 4];
        f.read_exact(&mut plen_buf)?;
        let plen = u32::from_le_bytes(plen_buf) as usize;
        if plen > 4096 {
            return Err(Error::Corrupted(format!("备份条目路径过长: {plen}")));
        }
        let mut path_bytes = vec![0u8; plen];
        f.read_exact(&mut path_bytes)?;
        let rel = String::from_utf8(path_bytes)
            .map_err(|_| Error::Corrupted("备份条目路径非 UTF-8".into()))?;
        let safe = sanitize_rel(&rel)?;

        // 大小 + CRC
        let mut size_buf = [0u8; 8];
        f.read_exact(&mut size_buf)?;
        let size = u64::from_le_bytes(size_buf);
        let mut crc_buf = [0u8; 4];
        f.read_exact(&mut crc_buf)?;
        let expect_crc = u32::from_le_bytes(crc_buf);

        // 负载 + CRC 校验
        let mut payload = vec![0u8; size as usize];
        f.read_exact(&mut payload)?;
        if crc32(&payload) != expect_crc {
            return Err(Error::Corrupted(format!("备份条目 CRC 校验失败: {rel}")));
        }

        let out_path = data_dir.join(&safe);
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&out_path, &payload)?;
        total_bytes += size;
    }

    Ok(RestoreReport {
        entry_count: count as usize,
        total_bytes,
        elapsed_ms: t.elapsed().as_secs_f64() * 1000.0,
    })
}

/// 递归收集目录下全部文件（相对路径，统一 `/` 分隔；跳过 `.tmp` 临时文件）。
/// 相对路径以顶层 `base` 为基准，递归子目录时保持前缀。
fn collect_files(base: &Path, out: &mut Vec<(String, PathBuf)>) -> Result<()> {
    collect_files_under(base, base, out)
}

fn collect_files_under(base: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let p = entry.path();
        if p.is_dir() {
            collect_files_under(base, &p, out)?;
        } else if p.extension().and_then(|e| e.to_str()) == Some("tmp") {
            continue; // 上次异常退出残留的临时文件，不入备份
        } else {
            let rel = p
                .strip_prefix(base)
                .unwrap_or(&p)
                .to_string_lossy()
                .replace('\\', "/");
            out.push((rel, p));
        }
    }
    Ok(())
}

/// 校验并规范化备份内相对路径：拒绝绝对路径、`..`、空路径（防目录穿越）。
fn sanitize_rel(rel: &str) -> Result<PathBuf> {
    let p = Path::new(rel);
    if p.is_absolute() {
        return Err(Error::Corrupted(format!("备份条目含绝对路径: {rel}")));
    }
    let mut out = PathBuf::new();
    for comp in p.components() {
        match comp {
            Component::Normal(c) => out.push(c),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(Error::Corrupted(format!("备份条目含 .. 路径: {rel}")))
            }
            _ => return Err(Error::Corrupted(format!("备份条目含非法路径: {rel}"))),
        }
    }
    if out.as_os_str().is_empty() {
        return Err(Error::Corrupted("备份条目路径为空".into()));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;

    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let name = format!("bak-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        let p = DIR
            .get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn make_tree(dir: &Path) {
        std::fs::create_dir_all(dir.join("primary")).unwrap();
        std::fs::create_dir_all(dir.join("inverted")).unwrap();
        std::fs::write(
            dir.join("primary/manifest.json"),
            r#"{"sst_files":[],"next_sst_id":1}"#,
        )
        .unwrap();
        std::fs::write(dir.join("primary/wal.log"), b"WAL-DATA").unwrap();
        std::fs::write(dir.join("primary/sst-00000001.sst"), b"SST-1").unwrap();
        std::fs::write(
            dir.join("inverted/inverted-manifest.json"),
            "{\"segments\":[]}".as_bytes(),
        )
        .unwrap();
        std::fs::write(dir.join("inverted/inverted-00000001.seg"), b"SEG-1").unwrap();
        // 临时文件应被跳过
        std::fs::write(dir.join("primary/manifest.json.tmp"), b"stale").unwrap();
    }

    #[test]
    fn backup_restore_roundtrip_preserves_tree() {
        let src = tmp();
        let dst = tmp();
        let bak = src.join("backup.bak");
        make_tree(&src);

        let rep = backup(&src, &bak).unwrap();
        // 实际文件：primary 3 + inverted 2 = 5；.tmp 被跳过
        assert_eq!(rep.entry_count, 5);

        restore(&bak, &dst).unwrap();
        assert_eq!(
            std::fs::read(dst.join("primary/wal.log")).unwrap(),
            b"WAL-DATA"
        );
        assert_eq!(
            std::fs::read(dst.join("primary/sst-00000001.sst")).unwrap(),
            b"SST-1"
        );
        assert_eq!(
            std::fs::read(dst.join("inverted/inverted-00000001.seg")).unwrap(),
            b"SEG-1"
        );
        assert!(std::fs::read(dst.join("primary/manifest.json")).is_ok());
        assert!(
            !dst.join("primary/manifest.json.tmp").exists(),
            ".tmp 不应入备份"
        );
    }

    #[test]
    fn restore_clears_existing_dir() {
        let src = tmp();
        let dst = tmp();
        let bak = src.join("backup.bak");
        make_tree(&src);
        backup(&src, &bak).unwrap();

        // 目标目录有旧脏文件
        std::fs::create_dir_all(&dst).unwrap();
        std::fs::write(dst.join("stale.txt"), b"old").unwrap();
        restore(&bak, &dst).unwrap();
        assert!(!dst.join("stale.txt").exists(), "还原前应清空目标目录");
        assert!(dst.join("primary/wal.log").exists());
    }

    #[test]
    fn restore_rejects_bad_magic() {
        let bak = tmp().join("bad.bak");
        std::fs::write(&bak, b"NOTBACKUP").unwrap();
        let err = restore(&bak, &tmp()).unwrap_err();
        assert!(matches!(err, Error::Corrupted(_)));
    }

    #[test]
    fn restore_rejects_newer_version() {
        let bak = tmp().join("future.bak");
        let mut data = Vec::new();
        data.extend_from_slice(BACKUP_MAGIC);
        data.extend_from_slice(&(BACKUP_VERSION + 1).to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        std::fs::write(&bak, data).unwrap();
        let err = restore(&bak, &tmp()).unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
    }

    #[test]
    fn restore_rejects_corrupted_payload_crc() {
        let src = tmp();
        let bak = src.join("backup.bak");
        make_tree(&src);
        backup(&src, &bak).unwrap();

        // 篡改最后一个字节 → CRC 失败
        let mut data = std::fs::read(&bak).unwrap();
        let n = data.len();
        data[n - 1] ^= 0xFF;
        std::fs::write(&bak, data).unwrap();
        let err = restore(&bak, &tmp()).unwrap_err();
        assert!(matches!(err, Error::Corrupted(_)));
    }

    #[test]
    fn sanitize_rejects_traversal() {
        assert!(sanitize_rel("../etc/passwd").is_err());
        assert!(sanitize_rel("a/../../b").is_err());
        assert!(sanitize_rel("/abs/path").is_err());
        assert!(sanitize_rel("").is_err());
        assert!(sanitize_rel("primary/wal.log").is_ok());
        assert!(sanitize_rel("a/./b").is_ok());
    }
}
