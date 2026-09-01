//! shanshui-cunji-export：数据导出（design 20 / development 5.23 基础版 + E 模块导出增强）。
//!
//! 用法：`shanshui-cunji-export --csv out.csv [--config config.toml]`
//!       `shanshui-cunji-export --parquet out.parquet [--config config.toml]`
//! CSV：两列 docid, json（RFC 4180 转义，csv crate）。
//! Parquet：两列 docid(Int64), json(Utf8)，SNAPPY 压缩，分块写入（增量 / JDBC 留阶段 2+）。

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use shanshui_cunji::config::Config;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut csv_path: Option<PathBuf> = None;
    let mut parquet_path: Option<PathBuf> = None;
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
        eprintln!("❌ 用法: shanshui-cunji-export --csv <out.csv> | --parquet <out.parquet> [--config config.toml]");
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

    let t = std::time::Instant::now();
    let rows = match mode {
        "csv" => export_csv(&mut engine, &out),
        _ => export_parquet(&mut engine, &out),
    };
    match rows {
        Ok(rows) => println!(
            "✅ 导出完成: {} 行 → {}（{:.0} ms）",
            rows,
            out.display(),
            t.elapsed().as_secs_f64() * 1000.0
        ),
        Err(e) => {
            eprintln!("❌ 导出失败: {e}");
            std::process::exit(1);
        }
    }
}

/// CSV 导出（两列 docid, json）。
fn export_csv(engine: &mut shanshui_cunji::engine::Engine, out: &PathBuf) -> Result<u64, String> {
    let mut wtr = csv::WriterBuilder::new()
        .from_path(out)
        .map_err(|e| e.to_string())?;
    let _ = wtr.write_record(["docid", "json"]);
    let all = engine.scan_range(None, None).map_err(|e| e.to_string())?;
    let mut rows = 0u64;
    for (docid, bytes) in all {
        let json_str = String::from_utf8_lossy(&bytes);
        if wtr
            .write_record([docid.to_string(), json_str.into_owned()])
            .is_err()
        {
            break;
        }
        rows += 1;
    }
    let _ = wtr.flush();
    Ok(rows)
}

/// Parquet 导出（docid Int64 + json Utf8，SNAPPY，分块 10 万行批量写）。
fn export_parquet(engine: &mut shanshui_cunji::engine::Engine, out: &PathBuf) -> Result<u64, String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("docid", DataType::Int64, false),
        Field::new("json", DataType::Utf8, false),
    ]));
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let file = std::fs::File::create(out).map_err(|e| e.to_string())?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(|e| e.to_string())?;

    let all = engine.scan_range(None, None).map_err(|e| e.to_string())?;
    let mut rows = 0u64;
    let mut ids = Vec::with_capacity(100_000);
    let mut jsons = Vec::with_capacity(100_000);
    for (docid, bytes) in all {
        ids.push(docid as i64);
        jsons.push(String::from_utf8_lossy(&bytes).into_owned());
        rows += 1;
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
    Ok(rows)
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
