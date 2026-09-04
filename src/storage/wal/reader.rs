use std::path::Path;

use crate::error::{Error, Result};
use crate::keys::decode_varlen;

use super::writer::WalRecord;
use super::{OP_DELETE, WAL_HEADER, WAL_HEADER_LEN, crc32};

/// WAL 读取 / 崩溃回放。
pub struct WalReader;

impl WalReader {
    /// 回放：返回按 Seq 升序的有效记录集合；首条损坏/截断处停止。
    /// 截断后重建的 WAL 含头（magic + next_seq，M8-P5）→ 跳过头从偏移 16 解析记录。
    pub fn recover(path: &Path) -> Result<Vec<WalRecord>> {
        let buf = std::fs::read(path)?;
        let start = if buf.len() >= WAL_HEADER_LEN && &buf[0..8] == WAL_HEADER {
            WAL_HEADER_LEN
        } else {
            0
        };
        let mut records = Vec::new();
        let mut pos = start;
        while pos + 8 <= buf.len() {
            let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(buf[pos + 4..pos + 8].try_into().unwrap());
            pos += 8;
            if pos + len > buf.len() {
                break; // 截断：尾部不完整记录丢弃（断电场景）
            }
            let payload = &buf[pos..pos + len];
            pos += len;
            if crc32(payload) != crc {
                break; // 损坏：停止回放（此记录之后的不可信）
            }
            match decode_payload(payload) {
                Ok(rec) => records.push(rec),
                Err(_) => break,
            }
        }
        Ok(records)
    }
}

pub(crate) fn decode_payload(payload: &[u8]) -> Result<WalRecord> {
    if payload.len() < 9 {
        return Err(Error::Corrupted("WAL payload 过短".into()));
    }
    let seq = u64::from_le_bytes(payload[0..8].try_into().unwrap());
    let op = payload[8];
    let mut pos = 9usize;
    let key = decode_varlen(payload, &mut pos)?.to_vec();
    let val_raw = decode_varlen(payload, &mut pos)?;
    let value = if val_raw.is_empty() && op == OP_DELETE {
        None
    } else {
        Some(val_raw.to_vec())
    };
    Ok(WalRecord {
        seq,
        op,
        key,
        value,
    })
}
