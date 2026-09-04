//! 数据操作子命令（本地引擎直连，与 HTTP 共享同一内核调用路径，
//! 勿与 server 同目录并发）：`put` / `patch` / `get` / `delete`。

use std::path::Path;

use serde_json::{json, Value};

use super::open_engine;

/// `put --id 1001 --data '{"status":"active","type":"order"}'`
pub(crate) fn run_cli_put(config_path: &Path, id: u64, data: &str) {
    if id == 0 {
        eprintln!("❌ put 需要 --id <docid>（>0）");
        std::process::exit(1);
    }
    if id >= u32::MAX as u64 {
        eprintln!("❌ docid 超出倒排索引支持范围（< 2^32）");
        std::process::exit(1);
    }
    let mut val: Value = match serde_json::from_str(data) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("❌ --data 不是合法 JSON: {e}");
            std::process::exit(1);
        }
    };
    if !val.is_object() {
        eprintln!("❌ --data 必须是 JSON 对象（如 '{{\"status\":\"active\"}}'）");
        std::process::exit(1);
    }
    // 与 HTTP /put 同构：文档对象含 docid，字符串字段值自动建倒排词条
    if let Some(obj) = val.as_object_mut() {
        obj.insert("docid".into(), json!(id));
    }
    let terms = shanshui_cunji::server::extract_terms(&val);
    let bytes = val.to_string().into_bytes();
    let term_refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
    let mut engine = open_engine(config_path);
    match engine.put(id, bytes, &term_refs) {
        Ok(()) => {
            // 倒排词条刷盘落盘：CLI 为独立进程，不刷盘则后续进程查不到（长驻 server 无需）
            if engine.inverted_mem_docids() > 0 {
                if let Err(e) = engine.flush_inverted() {
                    eprintln!("❌ 倒排刷盘失败: {e}");
                    std::process::exit(1);
                }
            }
            println!("✅ 已写入 docid={id}（倒排词条 {} 个）", terms.len());
        }
        Err(e) => {
            eprintln!("❌ 写入失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `patch --id 1001 --data '{"status":"inactive","note":null}'`（null = 删除字段，阶段 1.5 Delta CF）
pub(crate) fn run_cli_patch(config_path: &Path, id: u64, data: &str) {
    if id == 0 {
        eprintln!("❌ patch 需要 --id <docid>（>0）");
        std::process::exit(1);
    }
    let val: Value = match serde_json::from_str(data) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("❌ --data 不是合法 JSON: {e}");
            std::process::exit(1);
        }
    };
    let Some(obj) = val.as_object() else {
        eprintln!("❌ --data 必须是 JSON 对象（如 '{{\"status\":\"inactive\"}}'）");
        std::process::exit(1);
    };
    let fields: Vec<(&str, serde_json::Value)> =
        obj.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
    let mut engine = open_engine(config_path);
    match engine.patch(id, &fields) {
        Ok(()) => println!("✅ 已更新 docid={id}（字段 {} 个）", fields.len()),
        Err(e) => {
            eprintln!("❌ 更新失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `get --id 1001`
pub(crate) fn run_cli_get(config_path: &Path, id: u64) {
    if id == 0 {
        eprintln!("❌ get 需要 --id <docid>（>0）");
        std::process::exit(1);
    }
    let mut engine = open_engine(config_path);
    match engine.get(id) {
        Ok(Some(v)) => println!("{}", String::from_utf8_lossy(&v)),
        Ok(None) => println!("（未找到 docid={id}）"),
        Err(e) => {
            eprintln!("❌ 查询失败: {e}");
            std::process::exit(1);
        }
    }
}

/// `delete --id 1001`
pub(crate) fn run_cli_delete(config_path: &Path, id: u64) {
    if id == 0 {
        eprintln!("❌ delete 需要 --id <docid>（>0）");
        std::process::exit(1);
    }
    let mut engine = open_engine(config_path);
    match engine.delete(id) {
        Ok(()) => println!("✅ 已删除 docid={id}"),
        Err(e) => {
            eprintln!("❌ 删除失败: {e}");
            std::process::exit(1);
        }
    }
}
