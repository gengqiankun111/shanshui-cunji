//! shanshui-cunji-export：数据导出（design 20 / development 5.23 基础版 + E 模块导出增强）。
//!
//! 用法：`shanshui-cunji-export --csv out.csv [--config config.toml]`
//!       `shanshui-cunji-export --parquet out.parquet [--config config.toml]`
//!       `shanshui-cunji-export --csv out.csv --incremental --checkpoint cp.txt [--config config.toml]`
//! CSV：两列 docid, json（RFC 4180 转义，csv crate）。
//! Parquet：两列 docid(Int64), json(Utf8)，SNAPPY 压缩，分块写入。
//! 增量（design 20.5）：`--incremental`（DocId 游标断点续传）——首次全量导出并记录最大 docid
//! 到 checkpoint；后续只导 `docid > checkpoint` 的新数据并推进游标（对称 P3-4 增量导入；
//! JDBC / updated_at 时间戳游标留阶段 2+）。

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use shanshui_cunji::config::Config;
use shanshui_cunji::migrate::{load_checkpoint, save_checkpoint};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut csv_path: Option<PathBuf> = None;
    let mut parquet_path: Option<PathBuf> = None;
    let mut checkpoint_path: Option<PathBuf> = None;
    let mut incremental = false;
    let mut config_path = PathBuf::from("config.toml");
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--csv" | "-o" => {
                i += 1;
                if i < args.len() {
                    csv_path = Some(PathBuf::from(&args[i]));
                }
            }
            "--parquet" => {
                i += 1;
                if i < args.len() {
                    parquet_path = Some(PathBuf::from(&args[i]));
                }
            }
            "--checkpoint" => {
                i += 1;
                if i < args.len() {
                    checkpoint_path = Some(PathBuf::from(&args[i]));
                }
            }
            "--incremental" => incremental = true,
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
    let Some((out, mode)) = csv_path.map(|p| (p, "csv")).or_else(|| parquet_path.map(|p| (p, "parquet"))) else {
        eprintln!("❌ 用法: shanshui-cunji-export --csv <out.csv> | --parquet <out.parquet> [--incremental --checkpoint <cp>] [--config config.toml]");
        std::process::exit(1);
    };

    let cfg = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ 配置加载失败: {e}");
            std::process::exit(1);
        }
    };
    let data_dir = PathBuf::from(&cfg.storage.data_dir);
    let mut engine = match shanshui_cunji::engine::Engine::open(&data_dir, &cfg) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ 打开引擎失败: {e}");
            std::process::exit(1);
        }
    };

    // 增量模式：checkpoint 默认与输出同路径（.checkpoint 后缀）；读游标
    let cp_path = checkpoint_path
        .clone()
        .unwrap_or_else(|| out.with_extension("checkpoint"));
    let base = if incremental {
        match load_checkpoint(&cp_path) {
            Ok(b) => b,
            Err(e) => {
                // 对齐 import.rs：checkpoint 不存在（首次运行/断档）按全量导出处理
                println!("⚠️ checkpoint 不存在或读取失败: {e} —— 按全量导出处理（首次运行）");
                0
            }
        }
    } else {
        0
    };

    let t = std::time::Instant::now();
    let result = match mode {
        "csv" => export_csv(&mut engine, &out, base),
        _ => export_parquet(&mut engine, &out, base),
    };
    match result {
        Ok((rows, max_docid)) => {
            if incremental && max_docid > base {
                if let Err(e) = save_checkpoint(&cp_path, max_docid) {
                    eprintln!("❌ checkpoint 写入失败: {e}");
                    std::process::exit(1);
                }
                println!(
                    "✅ 增量导出完成: {} 行（docid {}..={}）→ {}（{:.0} ms），游标推进至 {}",
                    rows,
                    base + 1,
                    max_docid,
                    out.display(),
                    t.elapsed().as_secs_f64() * 1000.0,
                    max_docid
                );
            } else {
                println!(
                    "✅ 导出完成: {} 行 → {}（{:.0} ms）",
                    rows,
                    out.display(),
                    t.elapsed().as_secs_f64() * 1000.0
                );
            }
        }
        Err(e) => {
            eprintln!("❌ 导出失败: {e}");
            std::process::exit(1);
        }
    }
}

/// CSV 导出（两列 docid, json）。`base` = 增量游标（0 = 全量）。
fn export_csv(
    engine: &mut shanshui_cunji::engine::Engine,
    out: &PathBuf,
    base: u64,
) -> Result<(u64, u64), String> {
    let mut wtr = csv::WriterBuilder::new()
        .from_path(out)
        .map_err(|e| e.to_string())?;
    let _ = wtr.write_record(["docid", "json"]);
    let start = if base > 0 { Some(base + 1) } else { None };
    let all = engine.scan_range(start, None).map_err(|e| e.to_string())?;
    let mut rows = 0u64;
    let mut max_docid = base;
    for (docid, bytes) in all {
        let json_str = String::from_utf8_lossy(&bytes);
        if wtr
            .write_record([docid.to_string(), json_str.into_owned()])
            .is_err()
        {
            break;
        }
        rows += 1;
        max_docid = max_docid.max(docid);
    }
    let _ = wtr.flush();
    Ok((rows, max_docid))
}

/// Parquet 导出（docid Int64 + json Utf8，SNAPPY，分块 10 万行批量写）。`base` = 增量游标。
fn export_parquet(
    engine: &mut shanshui_cunji::engine::Engine,
    out: &PathBuf,
    base: u64,
) -> Result<(u64, u64), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("docid", DataType::Int64, false),
        Field::new("json", DataType::Utf8, false),
    ]));
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let file = std::fs::File::create(out).map_err(|e| e.to_string())?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(|e| e.to_string())?;

    let start = if base > 0 { Some(base + 1) } else { None };
    let all = engine.scan_range(start, None).map_err(|e| e.to_string())?;
    let mut rows = 0u64;
    let mut max_docid = base;
    let mut ids = Vec::with_capacity(100_000);
    let mut jsons = Vec::with_capacity(100_000);
    for (docid, bytes) in all {
        ids.push(docid as i64);
        jsons.push(String::from_utf8_lossy(&bytes).into_owned());
        rows += 1;
        max_docid = max_docid.max(docid);
        if ids.len() >= 100_000 {
            write_batch(&mut writer, &schema, &ids, &jsons)?;
            ids.clear();
            jsons.clear();
        }
    }
    if !ids.is_empty() {
        write_batch(&mut writer, &schema, &ids, &jsons)?;
    }
    writer.close().map_err(|e| e.to_string())?;
    Ok((rows, max_docid))
}

fn write_batch(
    writer: &mut ArrowWriter<std::fs::File>,
    schema: &Arc<Schema>,
    ids: &[i64],
    jsons: &[String],
) -> Result<(), String> {
    let rb = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(jsons.to_vec())),
        ],
    )
    .map_err(|e| e.to_string())?;
    writer.write(&rb).map_err(|e| e.to_string())
}
