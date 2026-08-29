//! shanshui-cunji-gen-dataset：异步（分块流式 + 进度输出，可后台运行）构建 Parquet 数据集。
//!
//! 规格：N 条记录 × 20 字段——9 个数值型（Int64×7 / Int32×2 / Float64×1）+ 2 个 256 字符文本
//! （`big_text_a/b`）+ 8 个短字符串/枚举 + 1 个布尔，满足「几个整型 + 1-2 个 256 字符字段」。
//!
//! 用法：
//!   shanshui-cunji-gen-dataset --rows 50000000 --out D:\shanshui-data\ds-50m.parquet
//!   --rows 行数（默认 5000 万）· --batch 批大小（默认 10 万）· --seed 确定性种子 · --out 输出路径
//!
//! 特点：
//! - **分块流式**：每批 10 万条构建 RecordBatch 写入，内存占用恒定（不随 N 增长）；
//! - **可后台运行**：循环 + 每 100 万条打印进度（普通同步 IO，无异步运行时依赖，保持内核零异步原则）；
//! - **确定性**：`--seed` 固定生成内容（复现/对账用）。

use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let get = |name: &str, def: u64| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(def)
    };
    let rows = get("--rows", 50_000_000);
    let batch = get("--batch", 100_000).max(1_000);
    let seed = get("--seed", 42);
    let out = args
        .iter()
        .position(|a| a == "--out")
        .and_then(|i| args.get(i + 1))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("ds-{rows}.parquet"))
        });
    if let Some(p) = out.parent() {
        std::fs::create_dir_all(p).expect("创建输出目录");
    }
    println!(
        "[gen] rows={rows} batch={batch} seed={seed} out={}",
        out.display()
    );

    // 20 字段 schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("docid", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("age", DataType::Int32, false),
        Field::new("score", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("big_text_a", DataType::Utf8, false),
        Field::new("big_text_b", DataType::Utf8, false),
        Field::new("tag_a", DataType::Utf8, false),
        Field::new("tag_b", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("device", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("flag", DataType::Boolean, false),
        Field::new("active_days", DataType::Int32, false),
        Field::new("visit_count", DataType::Int64, false),
        Field::new("balance", DataType::Int64, false),
    ]));

    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let file = std::fs::File::create(&out).expect("创建 parquet 文件");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .expect("创建 ArrowWriter");

    let t0 = std::time::Instant::now();
    let mut written: u64 = 0;
    while written < rows {
        let n = batch.min(rows - written);
        let start = written;
        let rb = build_batch(&schema, start, n, seed);
        writer.write(&rb).expect("写入批次");
        written += n;
        if written % 1_000_000 == 0 || written == rows {
            let speed = written as f64 / t0.elapsed().as_secs_f64();
            println!(
                "[gen] {written}/{rows} ({:.1}%) · {:.0} rows/s · {:.1}s",
                written as f64 * 100.0 / rows as f64,
                speed,
                t0.elapsed().as_secs_f64()
            );
        }
    }
    writer.close().expect("关闭 writer");
    let size_mb = std::fs::metadata(&out).map(|m| m.len() / 1024 / 1024).unwrap_or(0);
    println!(
        "[gen] ✅ 完成: {rows} 条 → {}（{size_mb} MB）· {:.1}s",
        out.display(),
        t0.elapsed().as_secs_f64()
    );
}

/// 构建一个批次（start..start+n 的 20 列数组）。
fn build_batch(schema: &Arc<Schema>, start: u64, n: u64, seed: u64) -> RecordBatch {
    let docid: Vec<i64> = (start..start + n).map(|i| i as i64).collect();
    let user_id: Vec<i64> = docid.iter().map(|&i| 1_000_000 + (i % 9_999_999)).collect();
    let amount: Vec<i64> = docid.iter().map(|&i| (i as u64).wrapping_mul(2654435761) % 1_000_000).map(|v| v as i64).collect();
    let age: Vec<i32> = docid.iter().map(|&i| 18 + (i % 60) as i32).collect();
    let score: Vec<f64> = docid.iter().map(|&i| (i % 1000) as f64 / 10.0).collect();
    let ts: Vec<i64> = docid.iter().map(|&i| 1_700_000_000 + (i % 31_536_000)).collect();
    let status: Vec<String> = docid.iter().map(|&i| ["active", "inactive", "pending"][(i % 3) as usize].into()).collect();
    let city: Vec<String> = docid.iter().map(|&i| ["beijing", "shanghai", "shenzhen", "hangzhou", "chengdu"][(i % 5) as usize].into()).collect();
    let big_text_a: Vec<String> = docid.iter().map(|&i| text256(i as u64, seed)).collect();
    let big_text_b: Vec<String> = docid
        .iter()
        .map(|&i| text256((i as u64).wrapping_mul(7).wrapping_add(seed), seed ^ 0x9E3779B9))
        .collect();
    let tag_a: Vec<String> = docid.iter().map(|&i| ["A", "B", "C"][(i % 3) as usize].into()).collect();
    let tag_b: Vec<String> = docid.iter().map(|&i| ["x", "y"][(i % 2) as usize].into()).collect();
    let note: Vec<String> = docid.iter().map(|&i| format!("note-{i}")).collect();
    let region: Vec<String> = docid.iter().map(|&i| ["east", "west", "south", "north"][(i % 4) as usize].into()).collect();
    let device: Vec<String> = docid.iter().map(|&i| ["pc", "mobile", "tablet"][(i % 3) as usize].into()).collect();
    let channel: Vec<String> = docid.iter().map(|&i| ["web", "app", "api"][(i % 3) as usize].into()).collect();
    let flag: Vec<bool> = docid.iter().map(|&i| i % 2 == 0).collect();
    let active_days: Vec<i32> = docid.iter().map(|&i| (i % 365) as i32).collect();
    let visit_count: Vec<i64> = docid.iter().map(|&i| i % 10_000).collect();
    let balance: Vec<i64> = docid.iter().map(|&i| i % 1_000_000).collect();

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(docid)) as Arc<dyn arrow::array::Array>,
            Arc::new(Int64Array::from(user_id)),
            Arc::new(Int64Array::from(amount)),
            Arc::new(Int32Array::from(age)),
            Arc::new(Float64Array::from(score)),
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from(status)),
            Arc::new(StringArray::from(city)),
            Arc::new(StringArray::from(big_text_a)),
            Arc::new(StringArray::from(big_text_b)),
            Arc::new(StringArray::from(tag_a)),
            Arc::new(StringArray::from(tag_b)),
            Arc::new(StringArray::from(note)),
            Arc::new(StringArray::from(region)),
            Arc::new(StringArray::from(device)),
            Arc::new(StringArray::from(channel)),
            Arc::new(BooleanArray::from(flag)),
            Arc::new(Int32Array::from(active_days)),
            Arc::new(Int64Array::from(visit_count)),
            Arc::new(Int64Array::from(balance)),
        ],
    )
    .expect("构建 RecordBatch")
}

/// 固定 256 宽文本（尾部空格填充；确定性 + 压缩友好）。
fn text256(i: u64, salt: u64) -> String {
    let s = format!(
        "rec-{i:08}-msg-{}-tag{}",
        i.wrapping_mul(31).wrapping_add(salt) % 100_000,
        salt % 100
    );
    format!("{s:<256}")
}
