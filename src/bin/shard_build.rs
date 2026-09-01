//! shanshui-cunji-shard-build：分片构建工具（10 亿库扩展阶段 A，design-10b-extension.md §7.2）。
//!
//! 把一个大源文件（CSV / JSONL）构建为 N 个分片数据目录（每分片独立 Engine）：
//! docid = `shard_id<<40 | local_id`（docid_alloc 分片前缀，路由 O(1)）；
//! 无显式主键时行号取模均匀分布，有 docid/id 列时按高 N 位直取分片。
//!
//! 用法：
//!   shanshui-cunji-shard-build --csv <in.csv> --shards 10 --data-dir-prefix <dir> [--config config.toml]
//!   shanshui-cunji-shard-build --csv <in.csv> --shards 10 --data-dirs <d0,d1,...> [--config config.toml]
//!   # 多进程并行：每进程只构建第 N 分片（--shard-id N，各进程独立读源并行导入）
//!   shanshui-cunji-shard-build --json <in.jsonl> --shards 10 --data-dir-prefix <dir> --shard-id 3

use std::path::{Path, PathBuf};
use std::time::Instant;

use shanshui_cunji::config::Config;
use shanshui_cunji::docid_alloc::shard_of;
use shanshui_cunji::engine::Engine;
use shanshui_cunji::error::Result;
use shanshui_cunji::shard_build::ShardBuildPlanner;

/// 单分片构建结果。
struct ShardReport {
    shard_id: u16,
    rows: u64,
    docid_min: u64,
    docid_max: u64,
    elapsed_ms: u128,
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut csv_path: Option<PathBuf> = None;
    let mut json_path: Option<PathBuf> = None;
    let mut shards = 10u16;
    let mut data_dir_prefix: Option<PathBuf> = None;
    let mut data_dirs: Option<Vec<PathBuf>> = None;
    let mut shard_id: Option<u16> = None;
    let mut config_path = PathBuf::from("config.toml");
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--csv" => {
                i += 1;
                if i < args.len() {
                    csv_path = Some(PathBuf::from(&args[i]));
                }
            }
            "--json" => {
                i += 1;
                if i < args.len() {
                    json_path = Some(PathBuf::from(&args[i]));
                }
            }
            "--shards" => {
                i += 1;
                if i < args.len() {
                    shards = args[i].parse().unwrap_or(10);
                }
            }
            "--data-dir-prefix" => {
                i += 1;
                if i < args.len() {
                    data_dir_prefix = Some(PathBuf::from(&args[i]));
                }
            }
            "--data-dirs" => {
                i += 1;
                if i < args.len() {
                    data_dirs = Some(
                        args[i]
                            .split(',')
                            .map(PathBuf::from)
                            .collect::<Vec<_>>(),
                    );
                }
            }
            "--shard-id" => {
                i += 1;
                if i < args.len() {
                    shard_id = args[i].parse().ok();
                }
            }
            "--config" | "-c" => {
                i += 1;
                if i < args.len() {
                    config_path = PathBuf::from(&args[i]);
                }
            }
            _ => {}
        }
        i += 1;
    }

    let src = match (&csv_path, &json_path) {
        (Some(p), None) => p.clone(),
        (None, Some(p)) => p.clone(),
        _ => {
            eprintln!("❌ 用法: shanshui-cunji-shard-build --csv <in.csv> | --json <in.jsonl> --shards N --data-dir-prefix <dir> | --data-dirs <d0,d1,...> [--shard-id N] [--config config.toml]");
            std::process::exit(1);
        }
    };
    if shards == 0 {
        eprintln!("❌ --shards 必须 ≥1");
        std::process::exit(1);
    }
    if data_dirs.is_none() && data_dir_prefix.is_none() {
        eprintln!("❌ 必须指定 --data-dir-prefix <dir> 或 --data-dirs <d0,d1,...>");
        std::process::exit(1);
    }
    let dirs: Vec<PathBuf> = match (&data_dirs, &data_dir_prefix) {
        (Some(ds), _) => ds.clone(),
        (None, Some(p)) => (0..shards).map(|s| p.join(format!("shard-{s:02}"))).collect(),
        _ => unreachable!(),
    };
    if dirs.len() as u16 != shards {
        eprintln!("❌ --data-dirs 数量 {} ≠ --shards {shards}", dirs.len());
        std::process::exit(1);
    }

    let cfg = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ 配置加载失败: {e}");
            std::process::exit(1);
        }
    };

    let planner = match ShardBuildPlanner::new(shards) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("❌ 分片规划器创建失败: {e}");
            std::process::exit(1);
        }
    };
    let target_shards: Vec<u16> = match shard_id {
        Some(s) => {
            if s >= shards {
                eprintln!("❌ --shard-id {s} 超出分片数 {shards}");
                std::process::exit(1);
            }
            vec![s]
        }
        None => (0..shards).collect(),
    };

    // 打开目标分片引擎（每分片独立 Engine/数据目录）
    let mut engines: Vec<(u16, Engine)> = Vec::new();
    for &sid in &target_shards {
        let dir = &dirs[sid as usize];
        std::fs::create_dir_all(dir).expect("创建分片数据目录失败");
        match Engine::open(dir, &cfg) {
            Ok(e) => engines.push((sid, e)),
            Err(e) => {
                eprintln!("❌ 打开分片 {sid} 引擎失败: {e}");
                std::process::exit(1);
            }
        }
    }

    let t0 = Instant::now();
    let has_docid_col = {
        // 探明表头是否含 docid/id 主键列
        if let Some(p) = &csv_path {
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(true)
                .flexible(true)
                .from_path(p)
                .expect("CSV 打开失败");
            match rdr.headers() {
                Ok(h) => h.iter().any(|h| h == "docid" || h == "id"),
                Err(_) => false,
            }
        } else {
            false
        }
    };

    let mut reports = Vec::new();
    for (sid, engine) in engines.iter_mut() {
        engine.set_bulk_import(true);
        let rep = if csv_path.is_some() {
            build_csv(
                engine,
                &src,
                &planner,
                *sid,
                &dirs[*sid as usize],
                shards,
                has_docid_col,
            )
        } else {
            build_json(engine, &src, &planner, *sid, &dirs[*sid as usize])
        };
        match rep {
            Ok(r) => reports.push(r),
            Err(e) => {
                eprintln!("❌ 分片 {sid} 构建失败: {e}");
                std::process::exit(1);
            }
        }
    }

    println!("✅ 分片构建完成: {shards} 分片 · 共 {} 行 · {:.1}s", reports.iter().map(|r| r.rows).sum::<u64>(), t0.elapsed().as_secs_f64());
    for r in &reports {
        println!(
            "   shard-{:02} | 行数 {} | docid [{}, {}] | {:.0} ms",
            r.shard_id, r.rows, r.docid_min, r.docid_max, r.elapsed_ms as f64
        );
    }
}

/// CSV 分片构建：首行表头即 JSON 字段名；含 docid/id 列则按前缀路由，否则行号取模分配。
fn build_csv(
    engine: &mut Engine,
    path: &Path,
    planner: &ShardBuildPlanner,
    sid: u16,
    dir: &Path,
    n_shards: u16,
    has_docid_col: bool,
) -> Result<ShardReport> {
    let t = Instant::now();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .map_err(|e| shanshui_cunji::error::Error::Migrate(format!("CSV 打开失败: {e}")))?;
    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| shanshui_cunji::error::Error::Migrate(format!("CSV 表头读取失败: {e}")))?
        .iter()
        .map(|h| h.to_string())
        .collect();
    let docid_col = headers
        .iter()
        .position(|h| h == "docid" || h == "id")
        .filter(|_| has_docid_col);
    let ft = engine.fulltext_fields().clone();
    let use_jieba = engine.use_jieba();

    let mut rows = 0u64;
    let mut docid_min = u64::MAX;
    let mut docid_max = 0u64;
    let mut row_idx = 0u64;
    for rec in reader.records() {
        let rec = match rec {
            Ok(r) => r,
            Err(_) => continue,
        };
        row_idx += 1;
        // 本行归属分片：显式主键 → 前缀路由；否则行号取模
        let belongs = if let Some(ci) = docid_col {
            match rec.get(ci).and_then(|s| s.trim().parse::<u64>().ok()) {
                Some(d) => shard_of(d) == sid,
                None => false,
            }
        } else {
            planner.row_to_shard(row_idx) == sid
        };
        if !belongs {
            continue;
        }
        let mut obj = serde_json::Map::new();
        for (i, field) in rec.iter().enumerate() {
            if let Some(name) = headers.get(i) {
                obj.insert(name.clone(), serde_json::Value::String(field.to_string()));
            }
        }
        let docid = match docid_col {
            Some(ci) => rec
                .get(ci)
                .and_then(|s| s.trim().parse::<u64>().ok())
                .unwrap(),
            None => {
                let d = planner.alloc(row_idx)?;
                obj.insert("docid".into(), serde_json::Value::from(d));
                d
            }
        };
        put_row(engine, docid, obj, &ft, use_jieba)?;
        rows += 1;
        docid_min = docid_min.min(docid);
        docid_max = docid_max.max(docid);
    }
    let _ = dir;
    Ok(ShardReport {
        shard_id: sid,
        rows,
        docid_min: if rows > 0 { docid_min } else { 0 },
        docid_max,
        elapsed_ms: t.elapsed().as_millis(),
    })
}

/// JSONL 分片构建：每行一个 JSON 对象；含 docid/id 字段按前缀路由，否则行号取模分配。
fn build_json(
    engine: &mut Engine,
    path: &Path,
    planner: &ShardBuildPlanner,
    sid: u16,
    dir: &Path,
) -> Result<ShardReport> {
    let t = Instant::now();
    let mut content = std::fs::read_to_string(path)
        .map_err(|e| shanshui_cunji::error::Error::Migrate(format!("JSONL 打开失败: {e}")))?;
    // 源文件可能带 UTF-8 BOM（Excel/PowerShell 导出常见），strip 避免首行解析失败
    if content.starts_with('\u{feff}') {
        content = content[3..].to_string();
    }
    let ft = engine.fulltext_fields().clone();
    let use_jieba = engine.use_jieba();

    let mut rows = 0u64;
    let mut docid_min = u64::MAX;
    let mut docid_max = 0u64;
    let mut row_idx = 0u64;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        row_idx += 1;
        let obj: serde_json::Map<String, serde_json::Value> = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let explicit = obj
            .get("docid")
            .or_else(|| obj.get("id"))
            .and_then(|v| v.as_u64());
        let belongs = if let Some(d) = explicit {
            shard_of(d) == sid
        } else {
            planner.row_to_shard(row_idx) == sid
        };
        if !belongs {
            continue;
        }
        let mut obj = obj;
        let docid = match explicit {
            Some(d) => d,
            None => {
                let d = planner.alloc(row_idx)?;
                obj.insert("docid".into(), serde_json::Value::from(d));
                d
            }
        };
        put_row(engine, docid, obj, &ft, use_jieba)?;
        rows += 1;
        docid_min = docid_min.min(docid);
        docid_max = docid_max.max(docid);
    }
    let _ = dir;
    Ok(ShardReport {
        shard_id: sid,
        rows,
        docid_min: if rows > 0 { docid_min } else { 0 },
        docid_max,
        elapsed_ms: t.elapsed().as_millis(),
    })
}

/// 序列化 + term 提取 + 写入引擎。
fn put_row(
    engine: &mut Engine,
    docid: u64,
    obj: serde_json::Map<String, serde_json::Value>,
    ft: &std::collections::HashSet<String>,
    use_jieba: bool,
) -> Result<()> {
    let bytes = serde_json::to_vec(&serde_json::Value::Object(obj))
        .map_err(|e| shanshui_cunji::error::Error::Serialize(format!("JSON 序列化失败: {e}")))?;
    let val = serde_json::from_slice::<serde_json::Value>(&bytes)
        .map_err(|e| shanshui_cunji::error::Error::Serialize(format!("JSON 解析失败: {e}")))?;
    let terms = shanshui_cunji::server::extract_terms_with_fulltext_seg(&val, None, Some(ft), use_jieba);
    let term_refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
    engine.put(docid, bytes, &term_refs)
}
