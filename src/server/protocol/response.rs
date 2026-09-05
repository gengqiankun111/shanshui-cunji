
//! COM_QUERY 响应与结果集构造（server/protocol/response.rs）：内容拆分自原
//! src/db_adapter.rs——QueryResponse 枚举、响应包序列编码（query_response_packets /
//! write_query_response）、SELECT 投影列（ProjCol / parse_projection）与结果集构建
//! （build_result_set / 字段类型推断 / ORDER BY-LIMIT 收尾）。

use std::net::TcpStream;

use crate::error::Result;
use crate::server::*;

/// Task-022：单遍 MapAccess **只收目标顶层字段**的 JSON 子集提取——未请求字段用
/// `IgnoredAny` 跳过（免为整行 25 列构造/丢弃 Value 与超长文本分配），替代
/// “整行 parse 全量 Object 后逐字段取”。语义与整行路径逐字节一致：
/// - 缺失字段不进子集 → 消费端按“缺失 = NULL”处理（与原整行 lookup 一致）；
/// - null / 嵌套对象数组 / 数字文本化均由消费端按 Value 同路径生成（结果不变）。
fn stream_projected_map(
    doc: &[u8],
    wanted: &std::collections::HashSet<&str>,
) -> Option<serde_json::Value> {
    use serde::Deserializer as _;
    struct Pick<'a> {
        wanted: &'a std::collections::HashSet<&'a str>,
    }
    impl<'de, 'a> serde::de::Visitor<'de> for Pick<'a> {
        type Value = serde_json::Value;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a JSON object")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut a: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut out = serde_json::map::Map::new();
            while let Some(k) = a.next_key::<String>()? {
                if self.wanted.contains(k.as_str()) {
                    // 目标字段：正常反序列化（与整行 parse 的 Value 完全一致）
                    out.insert(k, a.next_value::<serde_json::Value>()?);
                } else {
                    // 非目标字段：整值跳过（不构造 Value）
                    let _skip: serde::de::IgnoredAny = a.next_value()?;
                }
            }
            Ok(serde_json::Value::Object(out))
        }
    }
    let mut de = serde_json::Deserializer::from_slice(doc);
    de.deserialize_map(Pick { wanted }).ok()
}


/// COM_QUERY 响应（OK / ERR / ResultSet）。
pub(crate) enum QueryResponse {
    Ok(u64, u64),
    Err(u16, String),
    Set {
        columns: Vec<Vec<u8>>,
        rows: Vec<Vec<Vec<u8>>>,
    },
}
/// 编码 COM_QUERY 响应为包序列（ResultSet 多包 / OK / ERR）——与 IO 解耦，
/// 同步/异步连接共用（异步路径逐包 `write_packet_async`）。
pub(crate) fn query_response_packets(seq0: u8, resp: &QueryResponse) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut seq = seq0;
    match resp {
        QueryResponse::Ok(affected, lid) => out.push(ok_payload(*affected, *lid)),
        QueryResponse::Err(code, msg) => out.push(err_payload(*code, msg)),
        QueryResponse::Set { columns, rows } => {
            let mut cnt = Vec::new();
            write_lenenc(&mut cnt, columns.len() as u64);
            out.push(cnt);
            for c in columns {
                out.push(c.clone());
            }
            out.push(eof_payload());
            for row in rows {
                let mut rp = Vec::new();
                for cell in row {
                    if cell.len() == 1 && cell[0] == MYSQL_NULL_CELL {
                        // 文本协议 NULL：0xfb 单字节长度前缀（无内容）——合法 utf8 文本值
                        // 不可能出现单字节 0xfb，哨兵无歧义
                        rp.push(MYSQL_NULL_CELL);
                    } else {
                        write_lenenc(&mut rp, cell.len() as u64);
                        rp.extend_from_slice(cell);
                    }
                }
                out.push(rp);
            }
            out.push(eof_payload());
        }
    }
    // seq 仅用于包序号（协议要求递增；此处响应包连续）
    let _ = seq;
    out
}

/// 写 COM_QUERY 响应（ResultSet 多包 / OK / ERR）。
pub(crate) fn write_query_response(stream: &mut TcpStream, seq0: u8, resp: QueryResponse) -> Result<()> {
    let mut seq = seq0;
    for p in query_response_packets(seq0, &resp) {
        write_packet(stream, seq, &p)?;
        seq = seq.wrapping_add(1);
    }
    Ok(())
}
/// SELECT 投影列类型：id 主键 / doc 整文档 / doc 顶层 JSON 字段（字段级裁剪）。
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ProjCol {
    Id,
    Doc,
    Field(String),
}

/// 无投影（* / 解析失败）时的默认双列。
pub(crate) const DEFAULT_PROJ: &[ProjCol] = &[ProjCol::Id, ProjCol::Doc];

/// SELECT 列投影解析——结果集列裁剪（避免无脑返回整 doc JSON，降低回包字节与序列化开销）。
///
/// 支持简单列清单：`id` / `doc` / `doc` 顶层 JSON 字段名 / `*`（保书写顺序）。
/// 含函数 / 别名 / DISTINCT 等 → `None`，调用方维持 id+doc 双列现状。
pub(crate) fn parse_projection(sql: &str) -> Option<Vec<ProjCol>> {
    let lower = sql.to_lowercase();
    let f = lower.find(" from ")?;
    let list = lower[7..f].trim();
    // DISTINCT / 复杂子句 → 不裁剪（现状双列）
    if list.is_empty() || list.starts_with("distinct") || list.contains('(') {
        return None;
    }
    let mut cols = Vec::new();
    for item in list.split(',') {
        let raw = item.trim();
        let c = raw.trim_matches('`'); // 支持反引号包裹（MySQL 兼容）
        if c.is_empty() || c.contains(' ') || c.contains('(') || c.contains(')') {
            return None; // 别名/表达式 → 回退双列
        }
        match c {
            "*" => {
                cols.push(ProjCol::Id);
                cols.push(ProjCol::Doc);
            }
            "id" | "docid" => cols.push(ProjCol::Id),
            "doc" | "document" => cols.push(ProjCol::Doc),
            // 其余视为 doc 顶层 JSON 字段（无 schema 文档库按字段名提取）
            _ => cols.push(ProjCol::Field(c.to_string())),
        }
    }
    if cols.is_empty() {
        None
    } else {
        Some(cols)
    }
}

/// 文本协议 NULL 标记（0xfb 单字节长度前缀，无内容）。
pub(crate) const MYSQL_NULL_CELL: u8 = 0xfb;

/// doc 顶层字段值类型（用于结果集列类型精确化推断）。
#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum ValKind {
    Null,
    Bool,
    Int,
    Float,
    Str, // string / array / object（统一文本化）
}

/// JSON 值类型归类：布尔/整数归整型（可声明 LONGLONG）；浮点 → DOUBLE；其余文本化。
pub(crate) fn value_kind(v: &serde_json::Value) -> ValKind {
    match v {
        serde_json::Value::Null => ValKind::Null,
        serde_json::Value::Bool(_) => ValKind::Bool,
        serde_json::Value::Number(n) if n.is_f64() => ValKind::Float,
        serde_json::Value::Number(_) => ValKind::Int,
        _ => ValKind::Str,
    }
}

/// JSON 值 → 结果集文本 cell（NULL → 0xfb 哨兵；字符串原样；数字 to_string；
/// 布尔 1/0；嵌套对象/数组 JSON 串化——文本协议下客户端按列类型转数值/字符串）。
pub(crate) fn value_cell(v: &serde_json::Value) -> Vec<u8> {
    match v {
        serde_json::Value::Null => vec![MYSQL_NULL_CELL],
        serde_json::Value::String(s) => s.clone().into_bytes(),
        serde_json::Value::Bool(b) => {
            if *b {
                b"1".to_vec()
            } else {
                b"0".to_vec()
            }
        }
        serde_json::Value::Number(n) => n.to_string().into_bytes(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => v.to_string().into_bytes(),
    }
}

/// 字段列单行值类型聚合（列类型按整列实际值推断，避免逐行声明不一致）。
#[derive(Clone, Copy, Default)]
pub(crate) struct FieldAgg {
    seen: bool,
    has_str: bool,
    has_float: bool,
    has_int: bool,
    has_bool: bool,
}

impl FieldAgg {
    fn add(&mut self, k: ValKind) {
        self.seen = true;
        match k {
            ValKind::Null => {}
            ValKind::Bool => self.has_bool = true,
            ValKind::Int => self.has_int = true,
            ValKind::Float => self.has_float = true,
            ValKind::Str => self.has_str = true,
        }
    }
    /// 列类型决议：含文本/数组/对象 → VAR_STRING；只数字/布尔 → 有浮点 DOUBLE 否则
    /// LONGLONG；全 NULL / 未见值 → VAR_STRING（无可推断值取最保守类型）。
    fn col_type(&self) -> u8 {
        if !self.seen
            || self.has_str
            || (!self.has_float && !self.has_int && !self.has_bool)
        {
            MYSQL_TYPE_VAR_STRING
        } else if self.has_float {
            MYSQL_TYPE_DOUBLE
        } else {
            MYSQL_TYPE_LONGLONG
        }
    }
}

/// doc 内单层 key 查找（大小写容错：精确 → 小写 → 遍历不敏感命中）。
pub(crate) fn lookup_in_map<'a>(
    m: &'a serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<&'a serde_json::Value> {
    if let Some(v) = m.get(key) {
        return Some(v);
    }
    let lk = key.to_lowercase();
    if let Some(v) = m.get(&lk) {
        return Some(v);
    }
    m.iter()
        .find(|(k, _)| k.to_lowercase() == lk)
        .map(|(_, v)| v)
}

/// doc 顶层字段 / 嵌套字段取值（点路径 `a.b.c` 逐层下钻，每层大小写容错）。
/// 返回 (类型, cell)；缺失 / 中间非对象 / JSON null → (Null, NULL 哨兵)。
pub(crate) fn doc_field_kind_cell(
    obj: Option<&serde_json::Map<String, serde_json::Value>>,
    path: &str,
) -> (ValKind, Vec<u8>) {
    let Some(m) = obj else {
        return (ValKind::Null, vec![MYSQL_NULL_CELL]);
    };
    let mut segs = path.split('.');
    let first = segs.next().unwrap_or("");
    let mut node = lookup_in_map(m, first);
    for seg in segs {
        node = match node {
            Some(serde_json::Value::Object(nm)) => lookup_in_map(nm, seg),
            _ => None, // 中间值非对象 → 无法下钻，视为缺失
        };
    }
    match node {
        Some(v) => (value_kind(v), value_cell(v)),
        None => (ValKind::Null, vec![MYSQL_NULL_CELL]),
    }
}

/// 字段列结果集列名：MySQL `SELECT a.b` 列名为路径最后一段（b）。
pub(crate) fn field_col_name(field: &str) -> &str {
    field.rsplit('.').next().unwrap_or(field)
}

/// 按投影构建 ResultSet 列定义（None = id + doc 双列；id → LONGLONG；
/// doc/字段 → VAR_STRING——prepare 阶段无值可推断，字段列取静态文本类型）。
pub(crate) fn proj_columns(proj: Option<&[ProjCol]>) -> Vec<Vec<u8>> {
    let cols = proj.unwrap_or(DEFAULT_PROJ);
    cols.iter()
        .map(|c| match c {
            ProjCol::Id => column_payload("id", MYSQL_TYPE_LONGLONG, 63),
            ProjCol::Doc => column_payload("doc", MYSQL_TYPE_VAR_STRING, 45),
            ProjCol::Field(f) => column_payload(field_col_name(f), MYSQL_TYPE_VAR_STRING, 45),
        })
        .collect()
}

/// 投影结果集构建：原始 (id, doc) 行 → 行 cell + 列定义。
/// 字段列类型按**整列实际值**推断（LONGLONG / DOUBLE / VAR_STRING）——客户端按列类型
/// 正确解析数值（如 pymysql amount 列拿 int 而非 '73564' 字符串）。
pub(crate) fn build_result_set(
    proj: Option<&[ProjCol]>,
    raw: Vec<(u64, Vec<u8>)>,
    order_by: bool,
    limit: Option<usize>,
) -> QueryResponse {
    let cols: Vec<ProjCol> = proj.unwrap_or(DEFAULT_PROJ).to_vec();
    // Task-022：含字段列时预收集“目标字段集合” → 行内单遍只收目标成员（跳过其余列），
    // 免整行 25 列 Value 构造/丢弃 + 大文本分配（SELECT 10 列 ≈ SELECT* 量级）。
    // 仅顶层简单字段名可走子集流式；点号/下标嵌套路径（addr.city / arr[0]）须整行 parse
    // 深查 → 回退旧全量路径（语义不变）。
    let field_names: Option<Vec<&str>> = {
        let names: Vec<&str> = cols
            .iter()
            .filter_map(|c| match c {
                ProjCol::Field(f) => Some(f.as_str()),
                _ => None,
            })
            .collect();
        if names.is_empty() {
            None
        } else {
            Some(names)
        }
    };
    let simple_projection = field_names
        .as_ref()
        .map(|v| v.iter().all(|f| !f.contains('.') && !f.contains('[')))
        .unwrap_or(true);
    let wanted: Option<std::collections::HashSet<&str>> =
        field_names.as_ref().map(|v| v.iter().copied().collect());
    let mut aggs: Vec<Option<FieldAgg>> = vec![None; cols.len()];
    let mut data: Vec<(u64, Vec<Vec<u8>>)> = Vec::with_capacity(raw.len());
    for (id, doc) in raw {
        // 仅当结果集含字段列才做提取（SELECT id/doc 纯列保持零解析热路径）
        let obj = match &wanted {
            Some(w) if simple_projection => stream_projected_map(&doc, w),
            Some(_) => match serde_json::from_slice::<serde_json::Value>(&doc) {
                Ok(serde_json::Value::Object(m)) => Some(serde_json::Value::Object(m)),
                _ => None,
            },
            None => None,
        };
        let mut row = Vec::with_capacity(cols.len());
        for (i, c) in cols.iter().enumerate() {
            match c {
                ProjCol::Id => row.push(row_id_of(id).to_string().into_bytes()),
                ProjCol::Doc => row.push(doc.clone()),
                ProjCol::Field(f) => {
                    let (k, cell) = doc_field_kind_cell(obj.as_ref().and_then(|v| v.as_object()), f);
                    aggs[i].get_or_insert_with(FieldAgg::default).add(k);
                    row.push(cell);
                }
            }
        }
        data.push((id, row));
    }
    let rows = sort_limit_by_docid(data, order_by, limit);
    let columns: Vec<Vec<u8>> = cols
        .iter()
        .enumerate()
        .map(|(i, c)| match c {
            ProjCol::Id => column_payload("id", MYSQL_TYPE_LONGLONG, 63),
            ProjCol::Doc => column_payload("doc", MYSQL_TYPE_VAR_STRING, 45),
            ProjCol::Field(f) => {
                let t = aggs[i].map(|a| a.col_type()).unwrap_or(MYSQL_TYPE_VAR_STRING);
                column_payload(field_col_name(f), t, 63)
            }
        })
        .collect();
    QueryResponse::Set { columns, rows }
}

/// ORDER BY / LIMIT 收尾：统一按 docid 数值升序（对齐 MySQL 主键排序列语义；
/// 旧实现按 doc 字节序，`SELECT id ... ORDER BY id` 场景排序键错误）。
pub(crate) fn sort_limit_by_docid(
    mut rows: Vec<(u64, Vec<Vec<u8>>)>,
    order_by: bool,
    limit: Option<usize>,
) -> Vec<Vec<Vec<u8>>> {
    if order_by {
        rows.sort_by_key(|(id, _)| *id);
    }
    if let Some(l) = limit {
        rows.truncate(l.min(rows.len()));
    }
    rows.into_iter().map(|(_, r)| r).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::protocol::response::{build_result_set, parse_projection, ProjCol};

    /// 旧实现参考：整行 parse 全量 Object → 逐字段取 cell（用于 Task-022 子集提取等值对照）。
    fn reference_cells(doc: &[u8], id: u64, cols: &[ProjCol]) -> Vec<Vec<u8>> {
        let obj = if cols.iter().any(|c| matches!(c, ProjCol::Field(_))) {
            match serde_json::from_slice::<serde_json::Value>(doc) {
                Ok(serde_json::Value::Object(m)) => Some(m),
                _ => None,
            }
        } else {
            None
        };
        cols.iter()
            .map(|c| match c {
                ProjCol::Id => row_id_of(id).to_string().into_bytes(),
                ProjCol::Doc => doc.to_vec(),
                ProjCol::Field(f) => doc_field_kind_cell(obj.as_ref(), f).1,
            })
            .collect()
    }

    fn set_rows(resp: QueryResponse) -> Vec<Vec<Vec<u8>>> {
        match resp {
            QueryResponse::Set { rows, .. } => rows,
            _ => panic!("expected result set"),
        }
    }

    #[test]
    fn task022_stream_projection_matches_full_parse_semantics() {
        // 行文档覆盖：缺失字段 / JSON null / 嵌套对象 / 数组 / 超长非目标文本 /
        // 数字（整/浮）/ 带引号与反斜杠转义字符串 / 目标字段重复出现
        let docs: Vec<(u64, Vec<u8>)> = vec![
            (1, br#"{"id":1,"k":7,"amount":73564.5,"status":"active","city":"bei\"jing","note":"x\\y","nested":{"a":1},"arr":[1,2],"big":"AAAA...BBBB","txt_b":"skip_me_0123456789"}"#.to_vec()),
            (2, br#"{"id":2,"k":null,"status":"closed"}"#.to_vec()), // 大量字段缺失
            (3, br#"{"id":3,"k":-42,"amount":0,"status":"pending"}"#.to_vec()),
        ];
        // SELECT k, status, amount, note, big（非目标大文本/嵌套被跳过）
        let sql = "SELECT k,status,amount,note,big FROM t WHERE id IN (1,2,3)";
        let proj = parse_projection(sql).unwrap();
        let want_fields: Vec<&str> = proj
            .iter()
            .filter_map(|c| match c {
                ProjCol::Field(f) => Some(f.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(want_fields, vec!["k", "status", "amount", "note", "big"]);

        let resp = build_result_set(Some(&proj), docs.clone(), false, None);
        let rows = set_rows(resp);
        for (i, (id, doc)) in docs.iter().enumerate() {
            let expect = reference_cells(doc, *id, &proj);
            assert_eq!(rows[i], expect, "投影 cell 与整行 parse 语义逐字节一致 row={id}");
        }
    }

    #[test]
    fn task022_select_star_keeps_raw_bytes_no_parse() {
        // SELECT id,doc（无字段列）：保持零解析热路径——doc cell 为原字节直通
        let doc = br#"{"k":1,"status":"a\u00e9","txt":"\u0001raw"}"#.to_vec();
        let sql = "SELECT id,doc FROM t";
        let proj = parse_projection(sql).unwrap();
        let resp = build_result_set(Some(&proj), vec![(9, doc.clone())], false, None);
        let rows = set_rows(resp);
        assert_eq!(rows[0], vec![b"9".to_vec(), doc], "SELECT id,doc 直通原字节");
    }
}
