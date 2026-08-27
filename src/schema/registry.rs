//! FieldRegistry 实现：字段名 ↔ u16 ID 的持久化与自动扩展（D1）。

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use crate::error::{Error, Result};
use crate::keys::{decode_varlen, encode_varlen};

/// 文件魔数：`NFREG1`，用于格式识别与版本演进。
const MAGIC: &[u8] = b"NFREG1";
const VERSION: u16 = 1;

/// 预留字段 ID 0（编码中表示"空/未注册"，与 value 编码互斥）。
pub const RESERVED_ID: u16 = 0;

/// 字段注册表：双向映射 + 单调递增 ID 分配。
#[derive(Debug, Clone)]
pub struct FieldRegistry {
    by_name: HashMap<String, u16>,
    by_id: HashMap<u16, String>,
    next_id: u16,
}

impl FieldRegistry {
    pub fn new() -> Self {
        Self {
            by_name: HashMap::new(),
            by_id: HashMap::new(),
            next_id: 1, // 0 为预留
        }
    }

    /// 注册字段（已存在则返回原 ID）；自动扩展映射，无强约束（弱 Schema）。
    pub fn register(&mut self, name: &str) -> u16 {
        if let Some(&id) = self.by_name.get(name) {
            return id;
        }
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("字段数超过 u16 上限 65535");
        self.by_name.insert(name.to_string(), id);
        self.by_id.insert(id, name.to_string());
        id
    }

    /// 按名称查询 ID。
    pub fn id(&self, name: &str) -> Option<u16> {
        self.by_name.get(name).copied()
    }

    /// 按 ID 查询名称。
    pub fn name(&self, id: u16) -> Option<&str> {
        self.by_id.get(&id).map(|s| s.as_str())
    }

    /// 已注册字段数。
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    /// 持久化到 `metadata/fields.idx`。
    ///
    /// 格式：`MAGIC(6) ++ Version(u16) ++ Count(u32) ++ (ID(u16) ++ VarLen(name))*`
    pub fn persist(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&(self.by_name.len() as u32).to_le_bytes());
        // 按 ID 升序写出，保证确定性
        let mut entries: Vec<(&u16, &String)> = self.by_id.iter().collect();
        entries.sort_by_key(|(id, _)| **id);
        for (id, name) in entries {
            buf.extend_from_slice(&id.to_le_bytes());
            encode_varlen(&mut buf, name.as_bytes());
        }
        let mut file = std::fs::File::create(path)?;
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }

    /// 从文件加载；文件不存在返回空注册表。
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let buf = std::fs::read(path)?;
        Self::from_bytes(&buf)
    }

    fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < 12 || &buf[0..6] != MAGIC {
            return Err(Error::Corrupted("fields.idx 魔数或长度不符".into()));
        }
        let version = u16::from_le_bytes(buf[6..8].try_into().unwrap());
        if version != VERSION {
            return Err(Error::Unsupported(format!(
                "fields.idx 版本 {version} 不受支持"
            )));
        }
        let count = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
        let mut pos = 12usize;
        let mut reg = Self::new();
        for _ in 0..count {
            if pos + 2 > buf.len() {
                return Err(Error::Corrupted("字段条目头越界".into()));
            }
            let id = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap());
            pos += 2;
            let raw = decode_varlen(buf, &mut pos)?;
            let name = String::from_utf8(raw.to_vec())
                .map_err(|_| Error::Corrupted("字段名非法 UTF-8".into()))?;
            reg.register(&name);
            // 若加载的 ID 大于当前 next_id，推进游标以保持单调
            if id >= reg.next_id {
                reg.next_id = id + 1;
            }
            // 校验一致性：name → id 与 id → name 双向一致
            debug_assert_eq!(reg.id(&name), Some(id));
            debug_assert_eq!(reg.name(id), Some(name.as_str()));
        }
        Ok(reg)
    }
}

impl Default for FieldRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_is_idempotent() {
        let mut reg = FieldRegistry::new();
        let a = reg.register("status");
        let b = reg.register("status");
        assert_eq!(a, b);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn ids_are_monotonic_from_1() {
        let mut reg = FieldRegistry::new();
        let s = reg.register("status");
        let t = reg.register("type");
        assert_eq!(s, 1);
        assert_eq!(t, 2);
        assert!(s != RESERVED_ID && t != RESERVED_ID);
    }

    #[test]
    fn lookup_both_directions() {
        let mut reg = FieldRegistry::new();
        let id = reg.register("created_at");
        assert_eq!(reg.id("created_at"), Some(id));
        assert_eq!(reg.name(id), Some("created_at"));
        assert_eq!(reg.id("missing"), None);
        assert_eq!(reg.name(9999), None);
    }

    #[test]
    fn persist_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fields.idx");

        let mut reg = FieldRegistry::new();
        reg.register("docid");
        reg.register("status");
        reg.register("type");
        reg.register("created_at");
        reg.persist(&path).unwrap();

        let loaded = FieldRegistry::load(&path).unwrap();
        assert_eq!(loaded.len(), reg.len());
        assert_eq!(loaded.id("status"), reg.id("status"));
        assert_eq!(loaded.id("created_at"), reg.id("created_at"));
        assert_eq!(loaded.name(loaded.id("type").unwrap()), Some("type"));
    }

    #[test]
    fn load_missing_file_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let loaded = FieldRegistry::load(&dir.path().join("nope.idx")).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn load_rejects_wrong_magic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.idx");
        let mut bad = b"XXXXXX".to_vec();
        bad.extend_from_slice(&[0u8; 6]);
        std::fs::write(&path, &bad).unwrap();
        assert!(FieldRegistry::load(&path).is_err());
    }

    #[test]
    fn auto_expand_handles_new_fields_after_restart() {
        // 模拟：进程 A 注册 status/type 并落盘；进程 B 重启加载后遇到磁盘上已有的新字段 payload
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fields.idx");

        let mut reg_a = FieldRegistry::new();
        reg_a.register("status");
        reg_a.register("type");
        reg_a.persist(&path).unwrap();

        let mut reg_b = FieldRegistry::load(&path).unwrap();
        let new_id = reg_b.register("payload"); // 自动扩展
        assert!(new_id > reg_b.id("type").unwrap());
        assert_eq!(reg_b.len(), 3);

        // 扩展后再次落盘，下次启动依旧完整
        reg_b.persist(&path).unwrap();
        let reg_c = FieldRegistry::load(&path).unwrap();
        assert_eq!(reg_c.len(), 3);
        assert_eq!(reg_c.id("payload"), reg_b.id("payload"));
    }
}
