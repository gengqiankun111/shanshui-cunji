//! 数据块解码与字段提取（原 mod.rs 拆分）：行式 / PAX 列式块的统一解码、免值解码、
//! 单列/多列 PAX 解码、投影解码与 JSON 字段提取。

use crate::error::{Error, Result};
use crate::keys::{decode_varint, decode_varlen};

use super::{BLOCK_KIND_PAX, BLOCK_KIND_ROW, FLAG_PUT, SST_VERSION};

/// 统一数据块解码：按文件格式版本 + 块 kind 分发（v3=行式，v4=行式/PAX）。
/// 返回 `(key, value, seq)` 行序列，`value=None` 表示 Tombstone。
pub type DecodedRow = (Vec<u8>, Option<Vec<u8>>, u64);

pub fn decode_data_block(data: &[u8], format: u16) -> Result<Vec<DecodedRow>> {
    if format >= SST_VERSION {
        match data.first() {
            Some(&BLOCK_KIND_PAX) => return decode_pax_block(data),
            Some(&BLOCK_KIND_ROW) | Some(_) => {}
            None => return Err(Error::Corrupted("空数据块".into())),
        }
    }
    // 行式解析（v4 跳过块首 kind 字节）
    let start = if format >= SST_VERSION { 1 } else { 0 };
    let mut rows = Vec::new();
    let mut cur = start;
    while cur < data.len() {
        let key = decode_varlen(data, &mut cur)?.to_vec();
        let value = decode_varlen(data, &mut cur)?.to_vec();
        if cur + 9 > data.len() {
            return Err(Error::Corrupted("数据块 flags/seq 越界".into()));
        }
        let flag = data[cur];
        let seq = u64::from_le_bytes(data[cur + 1..cur + 9].try_into().unwrap());
        cur += 9;
        rows.push((key, (flag == FLAG_PUT).then_some(value), seq));
    }
    Ok(rows)
}

/// 7.100 免值解码（行计数快路径）：行式块只解析 key/flag/seq，**跳过值字节不拷贝**——
/// `COUNT(*)` 类全表计数不必构建/反序列化文档值（省 1100 万行 ~200B 的 alloc+memcpy）。
/// PAX 列式块（列交错）回退完整解码后映射（主库默认行式，PAX 命中率低）。
/// 返回 `(key, is_put, seq)`——值有无仅以 flag 表达，与 `decode_data_block` 的
/// `value=None = Tombstone` 语义对应。
pub fn decode_data_block_keys(data: &[u8], format: u16) -> Result<Vec<(Vec<u8>, bool, u64)>> {
    if format >= SST_VERSION {
        match data.first() {
            Some(&BLOCK_KIND_PAX) => {
                return Ok(decode_pax_block(data)?
                    .into_iter()
                    .map(|(k, v, seq)| (k, v.is_some(), seq))
                    .collect());
            }
            Some(&BLOCK_KIND_ROW) | Some(_) => {}
            None => return Err(Error::Corrupted("空数据块".into())),
        }
    }
    let start = if format >= SST_VERSION { 1 } else { 0 };
    let mut rows = Vec::new();
    let mut cur = start;
    while cur < data.len() {
        let key = decode_varlen(data, &mut cur)?.to_vec();
        // 值：4 字节 u32 长度前缀 + 值字节（直接跳过不拷贝）
        if cur + 4 > data.len() {
            return Err(Error::Corrupted("数据块值长度越界".into()));
        }
        let vlen = u32::from_le_bytes(data[cur..cur + 4].try_into().unwrap()) as usize;
        cur += 4;
        if cur + vlen > data.len() {
            return Err(Error::Corrupted("数据块值内容越界".into()));
        }
        cur += vlen;
        if cur + 9 > data.len() {
            return Err(Error::Corrupted("数据块 flags/seq 越界".into()));
        }
        let flag = data[cur];
        let seq = u64::from_le_bytes(data[cur + 1..cur + 9].try_into().unwrap());
        cur += 9;
        rows.push((key, flag == FLAG_PUT, seq));
    }
    Ok(rows)
}

/// PAX 列式块解码：按列偏移量表重组每行 JSON 对象（保序，与写入字节一致）。
/// mod.rs 底部测试模块直接调用 → pub(crate)。
pub(crate) fn decode_pax_block(data: &[u8]) -> Result<Vec<DecodedRow>> {
    let mut cur = 1usize; // 跳过 kind
    if cur + 4 > data.len() {
        return Err(Error::Corrupted("PAX 块行数越界".into()));
    }
    let row_count = u32::from_le_bytes(data[cur..cur + 4].try_into().unwrap()) as usize;
    cur += 4;

    // Keys
    let mut keys = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        keys.push(decode_varlen(data, &mut cur)?.to_vec());
    }
    // 列偏移量表
    if cur + 2 > data.len() {
        return Err(Error::Corrupted("PAX 列计数越界".into()));
    }
    let col_count = u16::from_le_bytes(data[cur..cur + 2].try_into().unwrap()) as usize;
    cur += 2;
    let mut col_meta: Vec<(String, usize, usize)> = Vec::with_capacity(col_count); // (field, offset, len)
    for _ in 0..col_count {
        let field = String::from_utf8(decode_varlen(data, &mut cur)?.to_vec())
            .map_err(|_| Error::Corrupted("PAX 列名非法 UTF-8".into()))?;
        let _is_hot = data[cur];
        cur += 1;
        if cur + 8 > data.len() {
            return Err(Error::Corrupted("PAX 列表越界".into()));
        }
        let offset = u32::from_le_bytes(data[cur..cur + 4].try_into().unwrap()) as usize;
        let len = u32::from_le_bytes(data[cur + 4..cur + 8].try_into().unwrap()) as usize;
        cur += 8;
        col_meta.push((field, offset, len));
    }

    // 解析列数据为列主序：每列一行条目 `Present(u8)+ValLen(VarLen)+Val`，行序与 Keys 一致
    let mut col_values: Vec<Vec<(bool, Option<serde_json::Value>)>> = Vec::with_capacity(col_count);
    for (_, offset, len) in &col_meta {
        let col_data = &data[*offset..*offset + *len];
        let mut vals = Vec::with_capacity(row_count);
        let mut ccur = 0usize;
        for _ in 0..row_count {
            if ccur >= col_data.len() {
                return Err(Error::Corrupted("PAX 列数据越界".into()));
            }
            let present = col_data[ccur];
            ccur += 1;
            if present == 1 {
                let vlen = decode_varint(col_data, &mut ccur)? as usize;
                if ccur + vlen > col_data.len() {
                    return Err(Error::Corrupted("PAX 列值越界".into()));
                }
                let vbytes = &col_data[ccur..ccur + vlen];
                ccur += vlen;
                if vbytes == b"n" {
                    vals.push((true, None)); // null
                } else {
                    let v = serde_json::from_slice(vbytes)
                        .map_err(|e| Error::Corrupted(format!("PAX 列值解析失败: {e}")))?;
                    vals.push((true, Some(v)));
                }
            } else {
                vals.push((false, None)); // 缺失
            }
        }
        col_values.push(vals);
    }

    // Seqs（块尾：row_count × u64，紧邻列数据区之后）
    if data.len() < row_count * 8 {
        return Err(Error::Corrupted("PAX seq 区越界".into()));
    }
    let seqs_start = data.len() - row_count * 8;
    let mut rows = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let mut map = serde_json::Map::new();
        for (ci, (field, _, _)) in col_meta.iter().enumerate() {
            let (present, v) = &col_values[ci][i];
            if *present {
                match v {
                    Some(v) => {
                        map.insert(field.clone(), v.clone());
                    }
                    None => {
                        map.insert(field.clone(), serde_json::Value::Null);
                    }
                }
            }
        }
        let value = serde_json::to_vec(&map)
            .map_err(|e| Error::Corrupted(format!("PAX 值重组失败: {e}")))?;
        let seq = u64::from_le_bytes(
            data[seqs_start + i * 8..seqs_start + (i + 1) * 8]
                .try_into()
                .unwrap(),
        );
        rows.push((keys[i].clone(), Some(value), seq));
    }
    Ok(rows)
}

/// P3-B：从 PAX 块中读取指定列的值，不重构完整行——只反序列化目标列的数据，
/// 跳过其他列（减少 JSON 解析和内存分配开销，适用于聚合查询）。
///
/// 返回 `(key, column_value, seq)` 列表，其中 `column_value` 是目标列的 JSON 值字节。
/// 如果目标列不存在或全为 null，则 `column_value` 为 None。
pub fn decode_pax_block_column(data: &[u8], column: &str) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>, u64)>> {
    let mut cur = 1usize; // 跳过 kind
    if cur + 4 > data.len() {
        return Err(Error::Corrupted("PAX 块行数越界".into()));
    }
    let row_count = u32::from_le_bytes(data[cur..cur + 4].try_into().unwrap()) as usize;
    cur += 4;

    // Keys
    let mut keys = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        keys.push(decode_varlen(data, &mut cur)?.to_vec());
    }
    // 列偏移量表
    if cur + 2 > data.len() {
        return Err(Error::Corrupted("PAX 列计数越界".into()));
    }
    let col_count = u16::from_le_bytes(data[cur..cur + 2].try_into().unwrap()) as usize;
    cur += 2;
    for _ci in 0..col_count {
        let field = String::from_utf8(decode_varlen(data, &mut cur)?.to_vec())
            .map_err(|_| Error::Corrupted("PAX 列名非法 UTF-8".into()))?;
        let _is_hot = data[cur];
        cur += 1;
        if cur + 8 > data.len() {
            return Err(Error::Corrupted("PAX 列表越界".into()));
        }
        let offset = u32::from_le_bytes(data[cur..cur + 4].try_into().unwrap()) as usize;
        let len = u32::from_le_bytes(data[cur + 4..cur + 8].try_into().unwrap()) as usize;
        cur += 8;
        if field == column {
            // 读取目标列数据
            let col_data = &data[offset..offset + len];
            let mut vals: Vec<Option<Vec<u8>>> = Vec::with_capacity(row_count);
            let mut ccur = 0usize;
            for _ in 0..row_count {
                if ccur >= col_data.len() {
                    return Err(Error::Corrupted("PAX 列数据越界".into()));
                }
                let present = col_data[ccur];
                ccur += 1;
                if present == 1 {
                    let vlen = decode_varint(col_data, &mut ccur)? as usize;
                    if ccur + vlen > col_data.len() {
                        return Err(Error::Corrupted("PAX 列值越界".into()));
                    }
                    let vbytes = &col_data[ccur..ccur + vlen];
                    ccur += vlen;
                    if vbytes == b"n" {
                        vals.push(Some(b"null".to_vec())); // null
                    } else {
                        vals.push(Some(vbytes.to_vec()));
                    }
                } else {
                    vals.push(None); // 缺失
                }
            }
            // Seqs（块尾：row_count × u64）
            if data.len() < row_count * 8 {
                return Err(Error::Corrupted("PAX seq 区越界".into()));
            }
            let seqs_start = data.len() - row_count * 8;
            let mut rows = Vec::with_capacity(row_count);
            for i in 0..row_count {
                let seq = u64::from_le_bytes(
                    data[seqs_start + i * 8..seqs_start + (i + 1) * 8]
                        .try_into()
                        .unwrap(),
                );
                rows.push((keys[i].clone(), vals[i].clone(), seq));
            }
            return Ok(rows);
        }
    }
    // 目标列不存在：返回空值列表
    let seqs_start = data.len() - row_count * 8;
    let mut rows = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let seq = u64::from_le_bytes(
            data[seqs_start + i * 8..seqs_start + (i + 1) * 8]
                .try_into()
                .unwrap(),
        );
        rows.push((keys[i].clone(), None, seq));
    }
    Ok(rows)
}

/// P87②：从 PAX 块中读取**多个**指定列——单次解析列偏移量表后逐请求列解码，
/// 对比 `decode_pax_block_column`（每列一调）免重复解析表头，对比 `decode_pax_block`
/// 免整行 25 列 JSON 重构。与逐列解码等值。
///
/// 返回 `(key, fields_values, seq)` 行序列：`fields_values[j]` = `fields[j]` 的 JSON 值
/// 字节（`b"null"` = JSON null；None = 该行缺列）。块中不存在的列 → 全行 None。
pub fn decode_pax_block_fields(
    data: &[u8],
    fields: &[String],
) -> Result<Vec<(Vec<u8>, Vec<Option<Vec<u8>>>, u64)>> {
    let mut cur = 1usize; // 跳过 kind
    if cur + 4 > data.len() {
        return Err(Error::Corrupted("PAX 块行数越界".into()));
    }
    let row_count = u32::from_le_bytes(data[cur..cur + 4].try_into().unwrap()) as usize;
    cur += 4;

    // Keys
    let mut keys = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        keys.push(decode_varlen(data, &mut cur)?.to_vec());
    }
    // 列偏移量表
    if cur + 2 > data.len() {
        return Err(Error::Corrupted("PAX 列计数越界".into()));
    }
    let col_count = u16::from_le_bytes(data[cur..cur + 2].try_into().unwrap()) as usize;
    cur += 2;
    let mut col_meta: Vec<(String, usize, usize)> = Vec::with_capacity(col_count);
    for _ci in 0..col_count {
        let field = String::from_utf8(decode_varlen(data, &mut cur)?.to_vec())
            .map_err(|_| Error::Corrupted("PAX 列名非法 UTF-8".into()))?;
        let _is_hot = data[cur];
        cur += 1;
        if cur + 8 > data.len() {
            return Err(Error::Corrupted("PAX 列表越界".into()));
        }
        let offset = u32::from_le_bytes(data[cur..cur + 4].try_into().unwrap()) as usize;
        let len = u32::from_le_bytes(data[cur + 4..cur + 8].try_into().unwrap()) as usize;
        cur += 8;
        col_meta.push((field, offset, len));
    }
    // 逐请求列解码（每列与行序对齐；列不存在 → 全行 None = 缺列）
    let mut col_vals: Vec<Vec<Option<Vec<u8>>>> = Vec::with_capacity(fields.len());
    for f in fields {
        let mut vals: Vec<Option<Vec<u8>>> = vec![None; row_count];
        if let Some((_, offset, len)) = col_meta.iter().find(|(name, _, _)| name == f) {
            let col_data = &data[*offset..*offset + *len];
            let mut ccur = 0usize;
            for v in vals.iter_mut() {
                if ccur >= col_data.len() {
                    return Err(Error::Corrupted("PAX 列数据越界".into()));
                }
                let present = col_data[ccur];
                ccur += 1;
                if present == 1 {
                    let vlen = decode_varint(col_data, &mut ccur)? as usize;
                    if ccur + vlen > col_data.len() {
                        return Err(Error::Corrupted("PAX 列值越界".into()));
                    }
                    let vbytes = &col_data[ccur..ccur + vlen];
                    ccur += vlen;
                    *v = if vbytes == b"n" {
                        Some(b"null".to_vec()) // null
                    } else {
                        Some(vbytes.to_vec())
                    };
                }
                // present=0 → 保持 None（缺失）
            }
        }
        col_vals.push(vals);
    }
    // Seqs（块尾：row_count × u64，紧邻列数据区之后）
    if data.len() < row_count * 8 {
        return Err(Error::Corrupted("PAX seq 区越界".into()));
    }
    let seqs_start = data.len() - row_count * 8;
    let mut rows = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let seq = u64::from_le_bytes(
            data[seqs_start + i * 8..seqs_start + (i + 1) * 8]
                .try_into()
                .unwrap(),
        );
        let vals: Vec<Option<Vec<u8>>> = col_vals.iter().map(|cv| cv[i].clone()).collect();
        rows.push((keys[i].clone(), vals, seq));
    }
    Ok(rows)
}

/// P91：scan 投影列块解码——PAX 块只解请求列并组装**子集 JSON**（免整行 25 列重构
/// 与重序列化）；行式块直通整行原 JSON 字节（零额外开销，消费端 light 按需取列）。
/// 返回行序列 `(key, value, seq)`，语义与 `decode_data_block` 对齐（Tombstone → None）。
pub fn decode_projected_block(
    data: &[u8],
    format: u16,
    fields: &[String],
) -> Result<Vec<DecodedRow>> {
    // PAX 列式块（v4+）：列解码只取请求列 → 每行组装子集 JSON
    if format >= SST_VERSION && data.first() == Some(&BLOCK_KIND_PAX) {
        let mut rows = Vec::new();
        for (k, vals, seq) in decode_pax_block_fields(data, fields)? {
            let value = assemble_subset_json(fields, &vals);
            rows.push((k, Some(value), seq));
        }
        return Ok(rows);
    }
    // 行式块（含 v3）：值即整行原 JSON 字节，直通（消费端只读其所需列）
    decode_data_block(data, format)
}

/// P91：请求列字节 → 子集 JSON 对象字节（`{"f1":<v1>,"f2":null}`）。
/// - `None` = 原文档缺失该键 → 子集省略（与整行文档缺键语义一致）；
/// - `Some(b"null")` = JSON null（PAX null 哨兵解码结果）；
/// - 其余 = 值已为 JSON 片段（字符串带引号/数字原样/布尔）→ 直接嵌入。
/// 字段名 JSON 转义由 `serde_json::to_string` 处理（含引号/反斜杠/控制符）。
fn assemble_subset_json(fields: &[String], vals: &[Option<Vec<u8>>]) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::with_capacity(64);
    out.push(b'{');
    let mut first = true;
    for (f, v) in fields.iter().zip(vals.iter()) {
        let Some(b) = v else { continue };
        if !first {
            out.push(b',');
        }
        first = false;
        // 字段名：JSON 双引号 + 转义
        out.extend_from_slice(&serde_json::to_string(f).unwrap_or_default().into_bytes());
        out.push(b':');
        out.extend_from_slice(b);
    }
    out.push(b'}');
    out
}

/// P87②/P86②：整行 JSON → 指定顶层字段的 JSON 值字节（serde 语义：缺键 → None；
/// 键存在且值为 JSON null → Some(b"null")）。非 JSON / 非对象行 → 全部 None（对齐
/// `sort_key` 解析失败返回 Null 的语义——行式块回退按需字段提取，正确性护栏保留）。
pub fn extract_fields_from_json_row(row: &[u8], fields: &[String]) -> Vec<Option<Vec<u8>>> {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(row) else {
        return vec![None; fields.len()];
    };
    let Some(map) = v.as_object() else {
        return vec![None; fields.len()];
    };
    fields
        .iter()
        .map(|f| map.get(f).map(|x| serde_json::to_vec(x).unwrap_or_default()))
        .collect()
}
