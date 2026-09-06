//! shanshui-cunji-gen-dataset：分块流式构建 Parquet 宽表数据集（与宽表 SQL 基准 25 列同构）。
//!
//! 规格：N 条记录 × 25 列——docid 主键 + `k/amount/score/ts/balance`（数值）+ 枚举
//! `status/region/channel/tag` + `user_id/age/active_days/visit_count/flag` + 定宽文本
//! `note/title/url/email/phone/ip/desc_a/desc_b/txt_a/txt_b`（列名与值分布对齐
//! rr-conformance wide_load COLS_FULL / 宽表 SQL 基准 `wide.t`，单行 ≈1KB）。
//! 确定性 PRNG（SplitMix64）：同 seed 两端生成逐位一致（本机 / Ubuntu 复现）。
//!
//! 用法：
//!   shanshui-cunji-gen-dataset --rows 500000 --out /path/ds-500k.parquet
//!   --rows 行数（默认 5000 万）· --batch 批大小（默认 10 万）· --seed 确定性种子 · --out 输出路径
//!
//! 特点：
//! - 分块流式：每批构建 RecordBatch 写入，内存占用恒定；
//! - 主键列名 `docid`（int64、自 1 递增）——`shanshui-cunji-import --parquet` 直接识别。

use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// 宽表 25 列（docid + 24），列序对齐 rr-conformance wide_load COLS_FULL（id→docid）。
const ENUM_STATUS: [&str; 5] = ["active", "closed", "pending", "failed", "archived"];
const ENUM_REGION: [&str; 8] = [
    "beijing",
    "shanghai",
    "shenzhen",
    "hangzhou",
    "guangzhou",
    "chengdu",
    "wuhan",
    "nanjing",
];
const ENUM_CHANNEL: [&str; 5] = ["web", "app", "api", "mobile", "wechat"];
const ENUM_TAG: [&str; 5] = ["free", "basic", "gold", "vip", "new"];
const ALPHA: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

/// SplitMix64：确定性 PRNG（无外部 rand 依赖；同 seed 同序列）。
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    /// [lo, hi] 闭区间均匀整数。
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }
    /// [0,1) f64。
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn pick<'a>(&mut self, arr: &'a [&str]) -> &'a str {
        arr[self.range(0, (arr.len() - 1) as u64) as usize]
    }
    fn rs(&mut self, n: u64) -> String {
        (0..n)
            .map(|_| ALPHA[self.range(0, (ALPHA.len() - 1) as u64) as usize] as char)
            .collect()
    }
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

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
        .unwrap_or_else(|| std::env::temp_dir().join(format!("ds-{rows}.parquet")));
    if let Some(p) = out.parent() {
        std::fs::create_dir_all(p).expect("创建输出目录");
    }
    println!(
        "[gen] rows={rows} batch={batch} seed={seed} out={}",
        out.display()
    );

    // 25 列 schema（docid 主键列名固定，import --parquet 识别；列序同宽表基准）
    let schema = Arc::new(Schema::new(vec![
        Field::new("docid", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("score", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("user_id", DataType::Int32, false),
        Field::new("age", DataType::Int32, false),
        Field::new("active_days", DataType::Int32, false),
        Field::new("visit_count", DataType::Int32, false),
        Field::new("balance", DataType::Float64, false),
        Field::new("flag", DataType::Int32, false),
        Field::new("tag", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("phone", DataType::Utf8, false),
        Field::new("ip", DataType::Utf8, false),
        Field::new("desc_a", DataType::Utf8, false),
        Field::new("desc_b", DataType::Utf8, false),
        Field::new("txt_a", DataType::Utf8, false),
        Field::new("txt_b", DataType::Utf8, false),
    ]));

    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let file = std::fs::File::create(&out).expect("创建 parquet 文件");
    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("创建 ArrowWriter");

    let mut rng = Rng(seed);
    let t0 = std::time::Instant::now();
    let mut written: u64 = 0;
    while written < rows {
        let n = batch.min(rows - written);
        let start = written; // docid 自 1 起
        let rb = build_batch(&schema, &mut rng, start, n);
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
    let size_mb = std::fs::metadata(&out)
        .map(|m| m.len() / 1024 / 1024)
        .unwrap_or(0);
    println!(
        "[gen] ✅ 完成: {rows} 条 → {}（{size_mb} MB）· {:.1}s",
        out.display(),
        t0.elapsed().as_secs_f64()
    );
}

/// 构建一个批次：docid = start+1 ..= start+n（与 wide_load gen() 相同的列分布，
/// 每行固定消耗的随机数调用序，同 seed 两端逐位一致）。
fn build_batch(schema: &Arc<Schema>, rng: &mut Rng, start: u64, n: u64) -> RecordBatch {
    let mut docid = Vec::with_capacity(n as usize);
    let mut k = Vec::with_capacity(n as usize);
    let mut amount = Vec::with_capacity(n as usize);
    let mut score = Vec::with_capacity(n as usize);
    let mut ts = Vec::with_capacity(n as usize);
    let mut status = Vec::with_capacity(n as usize);
    let mut region = Vec::with_capacity(n as usize);
    let mut channel = Vec::with_capacity(n as usize);
    let mut user_id = Vec::with_capacity(n as usize);
    let mut age = Vec::with_capacity(n as usize);
    let mut active_days = Vec::with_capacity(n as usize);
    let mut visit_count = Vec::with_capacity(n as usize);
    let mut balance = Vec::with_capacity(n as usize);
    let mut flag = Vec::with_capacity(n as usize);
    let mut tag = Vec::with_capacity(n as usize);
    let mut note = Vec::with_capacity(n as usize);
    let mut title = Vec::with_capacity(n as usize);
    let mut url = Vec::with_capacity(n as usize);
    let mut email = Vec::with_capacity(n as usize);
    let mut phone = Vec::with_capacity(n as usize);
    let mut ip = Vec::with_capacity(n as usize);
    let mut desc_a = Vec::with_capacity(n as usize);
    let mut desc_b = Vec::with_capacity(n as usize);
    let mut txt_a = Vec::with_capacity(n as usize);
    let mut txt_b = Vec::with_capacity(n as usize);
    for i in (start + 1)..=(start + n) {
        docid.push(i as i64);
        k.push(rng.range(1, 20_000_000) as i64);
        amount.push(round2(rng.unit() * 1_000_000.0));
        score.push(round2(rng.unit() * 100.0));
        ts.push((1_700_000_000u64 + rng.range(0, 30_000_000)) as i64);
        status.push(rng.pick(&ENUM_STATUS).to_string());
        region.push(rng.pick(&ENUM_REGION).to_string());
        channel.push(rng.pick(&ENUM_CHANNEL).to_string());
        user_id.push(rng.range(1, 5_000_000) as i32);
        age.push(rng.range(18, 70) as i32);
        active_days.push(rng.range(0, 365) as i32);
        visit_count.push(rng.range(0, 100_000) as i32);
        balance.push(round2(rng.unit() * 1_000_000.0));
        flag.push(rng.range(0, 1) as i32);
        tag.push(rng.pick(&ENUM_TAG).to_string());
        note.push(rng.rs(35));
        title.push(rng.rs(50));
        url.push(rng.rs(80));
        email.push(rng.rs(28));
        phone.push(rng.rs(11));
        ip.push(rng.rs(20));
        desc_a.push(rng.rs(200));
        desc_b.push(rng.rs(160));
        txt_a.push(rng.rs(140));
        txt_b.push(rng.rs(120));
    }
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(docid)) as Arc<dyn arrow::array::Array>,
            Arc::new(Int64Array::from(k)),
            Arc::new(Float64Array::from(amount)),
            Arc::new(Float64Array::from(score)),
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from(status)),
            Arc::new(StringArray::from(region)),
            Arc::new(StringArray::from(channel)),
            Arc::new(Int32Array::from(user_id)),
            Arc::new(Int32Array::from(age)),
            Arc::new(Int32Array::from(active_days)),
            Arc::new(Int32Array::from(visit_count)),
            Arc::new(Float64Array::from(balance)),
            Arc::new(Int32Array::from(flag)),
            Arc::new(StringArray::from(tag)),
            Arc::new(StringArray::from(note)),
            Arc::new(StringArray::from(title)),
            Arc::new(StringArray::from(url)),
            Arc::new(StringArray::from(email)),
            Arc::new(StringArray::from(phone)),
            Arc::new(StringArray::from(ip)),
            Arc::new(StringArray::from(desc_a)),
            Arc::new(StringArray::from(desc_b)),
            Arc::new(StringArray::from(txt_a)),
            Arc::new(StringArray::from(txt_b)),
        ],
    )
    .expect("构建 RecordBatch")
}
