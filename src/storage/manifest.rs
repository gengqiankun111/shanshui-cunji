//! Manifest：列族内 SST 文件清单（新→旧顺序）的序列化/反序列化与原子落盘。
//!
//! - 加载：重启时读取 `manifest.json`，按序恢复全部 SST + 层号 + 下一个可用 SST id；
//!   旧 Manifest 无 `levels` 时全部按 L0（0）兼容。
//! - 落盘：先写 `manifest.json.tmp` 再原子 rename —— 崩溃只留下旧清单或完整新清单。
//!   P73 修复要求调用方用**内存快照**重建清单、不扫描磁盘（见 ColumnFamily::persist_manifest 注释）。

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Manifest 文件名。
pub(crate) const MANIFEST_FILE: &str = "manifest.json";

/// Manifest：列族内 SST 文件清单（新→旧顺序）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Manifest {
    /// 最近刷盘的文件在最前（读路径优先命中）。
    pub(crate) sst_files: Vec<String>,
    /// 每个 SST 的层号（design 4.5 二期 Leveled，M6-2）：0 = 刷盘产物（允许重叠），
    /// 1 / 2 = Compaction 输出（层内 key 范围不重叠）。旧 Manifest 缺省按全 0（L0）兼容。
    #[serde(default)]
    pub(crate) levels: Vec<u32>,
    /// 下一个可用 SST id。
    pub(crate) next_sst_id: u64,
}

/// 加载 Manifest：不存在返回空清单（next id = 1）；解析失败视为损坏。
pub(crate) fn load(path: &Path) -> Result<(Vec<String>, Vec<u32>, u64)> {
    if !path.exists() {
        return Ok((Vec::new(), Vec::new(), 1));
    }
    let text = std::fs::read_to_string(path)?;
    let m: Manifest = serde_json::from_str(&text)
        .map_err(|e| Error::Corrupted(format!("Manifest 解析失败: {e}")))?;
    // 旧 Manifest 无 levels → 全部按 L0 处理（对齐长度）
    let levels = if m.levels.len() == m.sst_files.len() {
        m.levels
    } else {
        vec![0; m.sst_files.len()]
    };
    Ok((m.sst_files, levels, m.next_sst_id))
}

/// 原子落盘：`dir/manifest.json.tmp` 写入 → rename 到 `dir/manifest.json`。
pub(crate) fn save(
    dir: &Path,
    files: &[String],
    levels: &[u32],
    next_sst_id: u64,
) -> Result<()> {
    let m = Manifest {
        sst_files: files.to_vec(),
        levels: levels.to_vec(),
        next_sst_id,
    };
    let text = serde_json::to_string_pretty(&m)
        .map_err(|e| Error::Serialize(format!("Manifest 序列化失败: {e}")))?;
    let tmp = dir.join("manifest.json.tmp");
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, dir.join(MANIFEST_FILE))?;
    Ok(())
}
