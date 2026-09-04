
//! 预处理语句（server/command/stmt.rs）：内容拆分自原 src/db_adapter.rs——COM_STMT_PREPARE
//! （stmt_prepare）/ COM_STMT_EXECUTE 参数解析与占位符替换（stmt_execute_sql / replace_params）。

use crate::server::*;


/// H-5：COM_STMT_PREPARE。分配 stmt_id 存 SQL，返回 PREPARE_OK + [参数定义 + EOF] + 列定义 + EOF。
pub(crate) fn stmt_prepare(session: &mut Session, sql: &str) -> Vec<Vec<u8>> {
    let num_params = sql.bytes().filter(|b| *b == b'?').count() as u16;
    let stmt_id = session.next_stmt_id;
    session.next_stmt_id += 1;
    session.statements.insert(stmt_id, sql.to_string());
    let mut packets = Vec::new();
    // PREPARE_OK：0x00 + stmt_id(4) + num_columns(2) + num_params(2) + filler(1) + warnings(2)
    let proj_cols = parse_projection(sql).unwrap_or_else(|| vec![ProjCol::Id, ProjCol::Doc]);
    let mut ok = vec![0x00];
    ok.extend_from_slice(&stmt_id.to_le_bytes());
    ok.extend_from_slice(&(proj_cols.len() as u16).to_le_bytes()); // 列 = 投影列数（EXECUTE 对齐）
    ok.extend_from_slice(&num_params.to_le_bytes());
    ok.push(0);
    ok.extend_from_slice(&0u16.to_le_bytes());
    packets.push(ok);
    // 参数定义（ParameterDefinition41 与 ColumnDefinition41 同构）
    for _ in 0..num_params {
        packets.push(column_payload("?", MYSQL_TYPE_VAR_STRING, 45));
    }
    if num_params > 0 {
        packets.push(eof_payload());
    }
    // 列定义 + EOF（与投影一致：`SELECT id` 只声明 1 列 id，EXECUTE 结果列数不越界）
    packets.extend(proj_columns(Some(&proj_cols)));
    packets.push(eof_payload());
    packets
}

/// H-5：COM_STMT_EXECUTE。解析参数（null bitmap + 类型 + 二进制值）→ 占位符替换 →
/// 复用 COM_QUERY 分发逻辑。
/// 解析 EXECUTE 包：取 SQL + 参数 → 占位符替换，返回最终可执行 SQL。
/// （I 项高并发拆分：与 Engine 锁解耦，供读锁 / 写锁两条分发路径共用。）
pub(crate) fn stmt_execute_sql(session: &Session, cmd: &[u8]) -> std::result::Result<String, QueryResponse> {
    if cmd.len() < 10 {
        return Err(QueryResponse::Err(1094, "EXECUTE 包过短".to_string()));
    }
    let stmt_id = u32::from_le_bytes(cmd[1..5].try_into().unwrap());
    let Some(sql) = session.statements.get(&stmt_id).cloned() else {
        return Err(QueryResponse::Err(1094, format!("未知 statement id {stmt_id}")));
    };
    let num_params = sql.bytes().filter(|b| *b == b'?').count();
    let null_len = (num_params + 7) / 8;
    let mut pos = 10usize;
    if pos + null_len > cmd.len() {
        return Err(QueryResponse::Err(1094, "EXECUTE 参数位图越界".to_string()));
    }
    let null_bitmap = &cmd[pos..pos + null_len];
    pos += null_len;
    // new_params_bound_flag = 1 → 参数类型表
    let mut types: Vec<u8> = Vec::new();
    if cmd.get(pos).copied() == Some(1) {
        pos += 1;
        if pos + num_params * 2 > cmd.len() {
            return Err(QueryResponse::Err(1094, "EXECUTE 类型表越界".to_string()));
        }
        for _ in 0..num_params {
            types.push(cmd[pos]);
            pos += 2; // type + unsigned_flag
        }
    }
    // 解析参数值
    let mut values: Vec<String> = Vec::new();
    for i in 0..num_params {
        if null_bitmap[i / 8] & (1 << (i % 8)) != 0 {
            values.push("NULL".to_string());
            continue;
        }
        let t = types.get(i).copied().unwrap_or(MYSQL_TYPE_VAR_STRING);
        match t {
            MYSQL_TYPE_LONGLONG => {
                if pos + 8 > cmd.len() {
                    return Err(QueryResponse::Err(1094, "EXECUTE LONGLONG 越界".to_string()));
                }
                let v = u64::from_le_bytes(cmd[pos..pos + 8].try_into().unwrap());
                pos += 8;
                values.push(v.to_string());
            }
            MYSQL_TYPE_LONG => {
                if pos + 4 > cmd.len() {
                    return Err(QueryResponse::Err(1094, "EXECUTE LONG 越界".to_string()));
                }
                let v = u32::from_le_bytes(cmd[pos..pos + 4].try_into().unwrap());
                pos += 4;
                values.push(v.to_string());
            }
            MYSQL_TYPE_DOUBLE => {
                if pos + 8 > cmd.len() {
                    return Err(QueryResponse::Err(1094, "EXECUTE DOUBLE 越界".to_string()));
                }
                let bits = u64::from_le_bytes(cmd[pos..pos + 8].try_into().unwrap());
                pos += 8;
                values.push(f64::from_bits(bits).to_string());
            }
            // 字符串类（VAR_STRING/STRING/BLOB）：lenenc 长度 + 数据
            _ => {
                let len = match read_lenenc_raw(cmd, &mut pos) {
                    Ok(l) => l as usize,
                    Err(e) => return Err(QueryResponse::Err(1094, format!("参数长度越界: {e}"))),
                };
                if pos + len > cmd.len() {
                    return Err(QueryResponse::Err(1094, "EXECUTE 字符串越界".to_string()));
                }
                let s = String::from_utf8_lossy(&cmd[pos..pos + len]).to_string();
                pos += len;
                // 字符串参数按 SQL 字面量（转义单引号）
                values.push(format!("'{}'", s.replace('\'', "''")));
            }
        }
    }
    // 占位符替换 → 返回最终 SQL（由调用方按读写分发）
    Ok(replace_params(&sql, &values))
}

/// 按顺序把 SQL 中的 `?` 替换为参数值（values 已是 SQL 字面量形式）。
pub(crate) fn replace_params(sql: &str, values: &[String]) -> String {
    let mut out = String::with_capacity(sql.len() + 16);
    let mut vi = 0usize;
    let mut in_str = false;
    let mut quote = ' ';
    for c in sql.chars() {
        if in_str {
            out.push(c);
            if c == quote {
                in_str = false;
            }
        } else if c == '\'' || c == '"' {
            in_str = true;
            quote = c;
            out.push(c);
        } else if c == '?' {
            if let Some(v) = values.get(vi) {
                out.push_str(v);
            }
            vi += 1;
        } else {
            out.push(c);
        }
    }
    out
}
