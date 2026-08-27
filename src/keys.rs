//! 键编码规范（development 4.1，对齐 design 3.4 / 4.4.2）。
//!
//! - 主键：`DocId`（u64 大端，8 字节定长）——大端保证**字节序 == 数值序**，
//!   LSM 范围扫描 / Zone Map 剪枝依赖键的字节序比较（小端在 docid>255 时序错乱）；
//! - 组合索引：`VarLen(field1) ++ VarLen(field2) ++ ... ++ DocId(u64)`；
//! - Varint（LEB128）：DocId / 长度字段的变长编码（design 4.4.2）。

use crate::error::{Error, Result};

/// 主键编码：DocId 为 u64 大端定长 8 字节（字节序 == 数值序）。
pub fn encode_docid(docid: u64) -> [u8; 8] {
    docid.to_be_bytes()
}

/// 主键解码：输入必须恰好 8 字节。
pub fn decode_docid(bytes: &[u8]) -> Result<u64> {
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Error::Corrupted("DocId 必须为 8 字节定长".into()))?;
    Ok(u64::from_be_bytes(arr))
}

/// VarLen 编码：4 字节长度前缀（LE）+ 原始字节。
pub fn encode_varlen(buf: &mut Vec<u8>, value: &[u8]) {
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value);
}

/// VarLen 解码：从 `buf[pos..]` 读取一段，返回切片并前进 pos。
pub fn decode_varlen<'a>(buf: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
    if *pos + 4 > buf.len() {
        return Err(Error::Corrupted("VarLen 长度前缀越界".into()));
    }
    let len = u32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap()) as usize;
    *pos += 4;
    if *pos + len > buf.len() {
        return Err(Error::Corrupted("VarLen 内容越界".into()));
    }
    let start = *pos;
    *pos += len;
    Ok(&buf[start..*pos])
}

/// 组合索引键：`VarLen(field1) ++ VarLen(field2) ++ ... ++ DocId(u64)`。
/// 尾部 DocId 用大端，保证同前缀下按 docid 数值序排列（前缀范围扫描的正确性）。
pub fn encode_composite_key(fields: &[&[u8]], docid: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    for f in fields {
        encode_varlen(&mut buf, f);
    }
    buf.extend_from_slice(&docid.to_be_bytes());
    buf
}

/// 组合索引键解码：返回字段值切片列表与尾部 DocId。
pub fn decode_composite_key(buf: &[u8]) -> Result<(Vec<Vec<u8>>, u64)> {
    if buf.len() < 8 {
        return Err(Error::Corrupted("组合索引键过短".into()));
    }
    let mut pos = 0usize;
    let mut fields = Vec::new();
    // 除尾部 8 字节 DocId 外的部分按 VarLen 切分
    while pos + 8 < buf.len() {
        let v = decode_varlen(buf, &mut pos)?.to_vec();
        fields.push(v);
    }
    let docid = u64::from_be_bytes(buf[pos..pos + 8].try_into().unwrap());
    Ok((fields, docid))
}

/// LEB128 Varint 编码（design 4.4.2）：小整数 1 字节，大整数渐进增长。
pub fn encode_varint(buf: &mut Vec<u8>, mut n: u64) {
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n != 0 {
            buf.push(byte | 0x80);
        } else {
            buf.push(byte);
            break;
        }
    }
}

/// LEB128 Varint 解码：从 `buf[pos..]` 读取，返回数值并前进 pos。
pub fn decode_varint(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        if *pos >= buf.len() {
            return Err(Error::Corrupted("Varint 未终结".into()));
        }
        let byte = buf[*pos];
        *pos += 1;
        if shift >= 64 {
            return Err(Error::Corrupted("Varint 溢出 u64".into()));
        }
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

/// ZigZag 编码：有符号整数 → 无符号 Varint。
pub fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// ZigZag 解码：无符号 → 有符号整数。
pub fn zigzag_decode(u: u64) -> i64 {
    ((u >> 1) as i64) ^ -((u & 1) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docid_roundtrip() {
        for id in [0u64, 1, 255, 65_536, u32::MAX as u64, u64::MAX] {
            let enc = encode_docid(id);
            assert_eq!(decode_docid(&enc).unwrap(), id);
        }
    }

    #[test]
    fn docid_byte_order_matches_numeric_order() {
        // 大端：字节序 == 数值序，范围扫描 / Zone Map 剪枝的前提
        let mut ids = vec![0u64, 1, 255, 256, 1001, 2000, 65_536, u64::MAX];
        ids.sort();
        for w in ids.windows(2) {
            assert!(encode_docid(w[0]) < encode_docid(w[1]), "docid {} 的编码序错乱", w[0]);
        }
        // 组合索引同前缀下按 docid 数值序排列
        let a = encode_composite_key(&[b"active"], 1001);
        let b = encode_composite_key(&[b"active"], 2000);
        assert!(a < b);
    }

    #[test]
    fn docid_decode_requires_8_bytes() {
        assert!(decode_docid(&[1, 2, 3]).is_err());
        assert!(decode_docid(&[]).is_err());
    }

    #[test]
    fn varlen_roundtrip() {
        let cases: &[&[u8]] = &[b"", b"a", b"status", &[0u8, 0x7f, 0xff, 0x80]];
        for c in cases {
            let mut buf = Vec::new();
            encode_varlen(&mut buf, c);
            let mut pos = 0;
            assert_eq!(decode_varlen(&buf, &mut pos).unwrap(), *c);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn composite_key_roundtrip() {
        let key = encode_composite_key(&[b"active", b"click"], 1001);
        let (fields, docid) = decode_composite_key(&key).unwrap();
        assert_eq!(fields, vec![b"active".to_vec(), b"click".to_vec()]);
        assert_eq!(docid, 1001);
    }

    #[test]
    fn composite_key_order_is_bytewise() {
        // 组合索引键必须按字段值字节序有序（LSM 前缀扫描的前提）
        let a = encode_composite_key(&[b"a"], 1);
        let b = encode_composite_key(&[b"b"], 1);
        assert!(a < b);
    }

    #[test]
    fn varint_roundtrip() {
        for n in [0u64, 1, 127, 128, 300, 16_384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            encode_varint(&mut buf, n);
            let mut pos = 0;
            assert_eq!(decode_varint(&buf, &mut pos).unwrap(), n);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn varint_small_uses_few_bytes() {
        let mut buf = Vec::new();
        encode_varint(&mut buf, 127); // 1 字节
        assert_eq!(buf.len(), 1);
        buf.clear();
        encode_varint(&mut buf, 300); // 2 字节
        assert_eq!(buf.len(), 2);
        buf.clear();
        encode_varint(&mut buf, u64::MAX); // 10 字节
        assert_eq!(buf.len(), 10);
    }

    #[test]
    fn zigzag_roundtrip() {
        for v in [0i64, 1, -1, 63, -64, i32::MAX as i64, i32::MIN as i64, i64::MIN] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v);
        }
    }

    proptest::proptest! {
        #[test]
        fn varint_prop_roundtrip(n in 0u64..u64::MAX) {
            let mut buf = Vec::new();
            encode_varint(&mut buf, n);
            let mut pos = 0;
            assert_eq!(decode_varint(&buf, &mut pos).unwrap(), n);
            assert_eq!(pos, buf.len());
        }
    }
}
