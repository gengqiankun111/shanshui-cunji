//! 文档二进制序列化（development 4.2，对齐 design 3.4 / 4.4.2）。
//!
//! 文档格式：`DocId(u64) ++ FieldCount(u32) ++ Field{ FieldID(u16) ++ TypeTag(u8) ++ Payload }*`
//! - 字段 ID 来自 FieldRegistry（u16）；
//! - 标量负载：Bool=1B、Int/Timestamp=ZigZag Varint、Float=8B 定长、Str/Bytes=VarLen、Null=空。

use crate::error::{Error, Result};
use crate::keys::{
    decode_varint, decode_varlen, encode_varint, encode_varlen, zigzag_decode, zigzag_encode,
};

/// 类型标签（TypeTag）。
pub mod tag {
    pub const NULL: u8 = 0;
    pub const BOOL: u8 = 1;
    pub const I64: u8 = 2;
    pub const F64: u8 = 3;
    pub const STR: u8 = 4;
    pub const BYTES: u8 = 5;
    pub const TIMESTAMP: u8 = 6;
}

/// 文档字段值（弱 Schema，无强类型约束）。
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    /// 毫秒级时间戳。
    Timestamp(i64),
}

impl Value {
    pub fn type_tag(&self) -> u8 {
        match self {
            Value::Null => tag::NULL,
            Value::Bool(_) => tag::BOOL,
            Value::Int(_) => tag::I64,
            Value::Float(_) => tag::F64,
            Value::Str(_) => tag::STR,
            Value::Bytes(_) => tag::BYTES,
            Value::Timestamp(_) => tag::TIMESTAMP,
        }
    }

    fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Value::Null => {}
            Value::Bool(b) => buf.push(*b as u8),
            Value::Int(v) => encode_varint(buf, zigzag_encode(*v)),
            Value::Timestamp(v) => encode_varint(buf, zigzag_encode(*v)),
            Value::Float(f) => buf.extend_from_slice(&f.to_le_bytes()),
            Value::Str(s) => encode_varlen(buf, s.as_bytes()),
            Value::Bytes(b) => encode_varlen(buf, b),
        }
    }

    fn decode(tag: u8, buf: &[u8], pos: &mut usize) -> Result<Value> {
        Ok(match tag {
            tag::NULL => Value::Null,
            tag::BOOL => {
                let b = buf
                    .get(*pos)
                    .ok_or_else(|| Error::Corrupted("Bool 负载缺失".into()))?;
                *pos += 1;
                Value::Bool(*b != 0)
            }
            tag::I64 | tag::TIMESTAMP => {
                let v = zigzag_decode(decode_varint(buf, pos)?);
                if tag == tag::TIMESTAMP {
                    Value::Timestamp(v)
                } else {
                    Value::Int(v)
                }
            }
            tag::F64 => {
                if *pos + 8 > buf.len() {
                    return Err(Error::Corrupted("Float 负载越界".into()));
                }
                let v = f64::from_le_bytes(buf[*pos..*pos + 8].try_into().unwrap());
                *pos += 8;
                Value::Float(v)
            }
            tag::STR => {
                let raw = decode_varlen(buf, pos)?;
                let s = String::from_utf8(raw.to_vec())
                    .map_err(|_| Error::Corrupted("字符串非法 UTF-8".into()))?;
                Value::Str(s)
            }
            tag::BYTES => {
                let raw = decode_varlen(buf, pos)?;
                Value::Bytes(raw.to_vec())
            }
            other => return Err(Error::Corrupted(format!("未知 TypeTag: {other}"))),
        })
    }
}

/// 序列化文档：`DocId(u64) ++ FieldCount(u32) ++ Field{ FieldID(u16) ++ TypeTag(u8) ++ Payload }*`。
pub fn encode_document(buf: &mut Vec<u8>, docid: u64, fields: &[(u16, Value)]) {
    buf.extend_from_slice(&docid.to_le_bytes());
    buf.extend_from_slice(&(fields.len() as u32).to_le_bytes());
    for (fid, val) in fields {
        buf.extend_from_slice(&fid.to_le_bytes());
        buf.push(val.type_tag());
        val.encode(buf);
    }
}

/// 反序列化文档，返回 `(docid, Vec<(FieldID, Value)>)`。
pub fn decode_document(buf: &[u8]) -> Result<(u64, Vec<(u16, Value)>)> {
    if buf.len() < 12 {
        return Err(Error::Corrupted("文档头过短".into()));
    }
    let docid = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let count = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
    let mut pos = 12usize;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 3 > buf.len() {
            return Err(Error::Corrupted("字段头越界".into()));
        }
        let fid = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap());
        let tag = buf[pos + 2];
        pos += 3;
        let val = Value::decode(tag, buf, &mut pos)?;
        fields.push((fid, val));
    }
    Ok((docid, fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(fields: &[(u16, Value)]) -> (u64, Vec<(u16, Value)>) {
        let mut buf = Vec::new();
        encode_document(&mut buf, 1001, fields);
        decode_document(&buf).unwrap()
    }

    #[test]
    fn empty_document_roundtrip() {
        let (docid, fields) = roundtrip(&[]);
        assert_eq!(docid, 1001);
        assert!(fields.is_empty());
    }

    #[test]
    fn all_types_roundtrip() {
        let fields = vec![
            (1u16, Value::Null),
            (2, Value::Bool(true)),
            (3, Value::Bool(false)),
            (4, Value::Int(-42)),
            (5, Value::Int(i64::MAX)),
            (6, Value::Int(i64::MIN)),
            (7, Value::Float(std::f64::consts::PI)),
            (8, Value::Str("status=active".into())),
            (9, Value::Str("中文 & emoji 🎉".into())),
            (10, Value::Str(String::new())),
            (11, Value::Bytes(vec![0, 1, 2, 0xff])),
            (12, Value::Timestamp(1_777_000_000_000)),
        ];
        let (docid, decoded) = roundtrip(&fields);
        assert_eq!(docid, 1001);
        assert_eq!(decoded, fields);
    }

    #[test]
    fn decode_rejects_unknown_type_tag() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1001u64.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.push(99); // 非法 TypeTag
        assert!(decode_document(&buf).is_err());
    }

    #[test]
    fn decode_rejects_truncated() {
        assert!(decode_document(&[0u8; 8]).is_err()); // 缺 FieldCount
        assert!(decode_document(&[0u8; 20]).is_ok()); // 全零字段也合法（Null 字段）
    }

    proptest::proptest! {
        #[test]
        fn str_roundtrip_prop(s in ".*") {
            let fields = vec![(1u16, Value::Str(s))];
            let (_, decoded) = roundtrip(&fields);
            assert_eq!(decoded, fields);
        }

        #[test]
        fn int_roundtrip_prop(v in i64::MIN..i64::MAX) {
            let fields = vec![(1u16, Value::Int(v))];
            let (_, decoded) = roundtrip(&fields);
            assert_eq!(decoded, fields);
        }
    }
}
