//! shanshui-cunji-export：数据导出（design 20 / development 5.23 基础版 + E 模块导出增强）。
//!
//! 用法：`shanshui-cunji-export --csv out.csv [--config config.toml]`
//!       `shanshui-cunji-export --parquet out.parquet [--config config.toml]`
//!       `shanshui-cunji-export --jdbc 'mysql://root@127.0.0.1:3306/db' [--table t] [--config config.toml]`
//! 导出管道（design 20.5）：流式扫描 → Filter（--filter 'field op value AND ...'）→
//! Projection（--project 'a,b,c' 字段子集 + --mask 'f=pat' 脱敏）→ Sink 分叉
//! （CSV / Parquet / JDBC 直连）。--rate-limit <rows/s> 限流；--batch-size 每批行数。
//! MySQL 兼容（--mysql-compatible）：CSV 导出后生成配套 SQL（CREATE TABLE + LOAD DATA INFILE，
//! 比逐条 INSERT 快 ~20 倍）；--mysql-max-varchar <n> 控制 doc 列 VARCHAR(n)/TEXT。
//! 建表 DDL（--dry-run-schema <out.sql> [--target clickhouse|mysql]）：只生成目标库建表 DDL
//! （ClickHouse MergeTree 供 Parquet 直读 / MySQL），不导出数据。
//! 增量（design 20.5）：`--incremental`（DocId 游标断点续传）——首次全量导出并记录最大 docid
//! 到 checkpoint；后续只导 `docid > checkpoint` 的新数据并推进游标（对称 P3-4 增量导入）。

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use shanshui_cunji::config::Config;
use shanshui_cunji::error::Result;
use shanshui_cunji::export_pipeline::{Filter, Projection, Sink};
use shanshui_cunji::migrate::{load_checkpoint, save_checkpoint};
use shanshui_cunji::mysql::MysqlWireClient;

/// 默认批大小（design 20.5：内存恒定 = 批 × 单行，如 10k × 1KB ≈ 10MB）。
const DEFAULT_BATCH: usize = 10_000;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut csv_path: Option<PathBuf> = None;
    let mut parquet_path: Option<PathBuf> = None;
    let mut jdbc_url: Option<String> = None;
    let mut table = "export_rows".to_string();
    let mut checkpoint_path: Option<PathBuf> = None;
    let mut incremental = false;
    let mut config_path = PathBuf::from("config.toml");
    let mut filter_expr: Option<String> = None;
    let mut project_expr: Option<String> = None;
    let mut masks: Vec<String> = Vec::new();
    let mut rate_limit: f64 = 0.0;
    let mut batch_size = DEFAULT_BATCH;
    let mut mysql_compatible = false;
    let mut mysql_max_varchar: usize = 0;
    let mut dry_run_schema: Option<PathBuf> = None;
    let mut target = "mysql".to_string();
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
            "--jdbc" => {
                i += 1;
                if i < args.len() {
                    jdbc_url = Some(args[i].clone());
                }
            }
            "--table" => {
                i += 1;
                if i < args.len() {
                    table = args[i].clone();
                }
            }
            "--checkpoint" => {
                i += 1;
                if i < args.len() {
                    checkpoint_path = Some(PathBuf::from(&args[i]));
                }
            }
            "--incremental" => incremental = true,
            "--filter" => {
                i += 1;
                if i < args.len() {
                    filter_expr = Some(args[i].clone());
                }
            }
            "--project" => {
                i += 1;
                if i < args.len() {
                    project_expr = Some(args[i].clone());
                }
            }
            "--mask" => {
                i += 1;
                if i < args.len() {
                    masks.push(args[i].clone());
                }
            }
            "--rate-limit" => {
                i += 1;
                if i < args.len() {
                    rate_limit = args[i].parse().unwrap_or(0.0);
                }
            }
            "--batch-size" => {
                i += 1;
                if i < args.len() {
                    batch_size = args[i].parse().unwrap_or(DEFAULT_BATCH);
                }
            }
            "--mysql-compatible" => mysql_compatible = true,
            "--mysql-max-varchar" => {
                i += 1;
                if i < args.len() {
                    mysql_max_varchar = args[i].parse().unwrap_or(0);
                }
            }
            "--dry-run-schema" => {
                i += 1;
                if i < args.len() {
                    dry_run_schema = Some(PathBuf::from(&args[i]));
                }
            }
            "--target" => {
                i += 1;
                if i < args.len() {
                    target = args[i].clone();
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
    // --dry-run-schema：只生成目标库建表 DDL（不导出数据），供 ClickHouse/MySQL 手动建表
    if let Some(ddl_path) = &dry_run_schema {
        let ddl = match target.as_str() {
            "clickhouse" => clickhouse_ddl(&table),
            _ => mysql_ddl(&table, mysql_max_varchar),
        };
        if let Err(e) = std::fs::write(ddl_path, &ddl) {
            eprintln!("❌ DDL 写入失败: {e}");
            std::process::exit(1);
        }
        println!("✅ 建表 DDL 已生成（--target {target}）→ {}:\n{ddl}", ddl_path.display());
        return;
    }

    // 输出目标：--csv / --parquet / --jdbc 三选一
    let mode = if csv_path.is_some() {
        "csv"
    } else if parquet_path.is_some() {
        "parquet"
    } else if jdbc_url.is_some() {
        "jdbc"
    } else {
        eprintln!("❌ 用法: shanshui-cunji-export --csv <out.csv> | --parquet <out.parquet> | --jdbc 'mysql://...'");
        eprintln!("    [--incremental --checkpoint <cp>] [--filter 'field op value AND ...'] [--project 'a,b']");
        eprintln!("    [--mask 'field=pat'] [--rate-limit <rows/s>] [--batch-size <n>] [--config config.toml]");
        eprintln!("    [--mysql-compatible [--mysql-max-varchar <n>]] [--dry-run-schema <out.sql> --target clickhouse|mysql]");
        std::process::exit(1);
    };

    let filter = match Filter::parse(filter_expr.as_deref()) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("❌ {e}");
            std::process::exit(1);
        }
    };
    let projection = match Projection::parse(project_expr.as_deref(), &masks) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("❌ {e}");
            std::process::exit(1);
        }
    };

    let cfg = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ 配置加载失败: {e}");
            std::process::exit(1);
        }
    };
    let data_dir = PathBuf::from(&cfg.storage.data_dir);
    let engine = match shanshui_cunji::engine::Engine::open(&data_dir, &cfg) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ 打开引擎失败: {e}");
            std::process::exit(1);
        }
    };

    // 增量模式：checkpoint 默认与输出同路径（.checkpoint 后缀）；读游标
    let out: Option<PathBuf> = csv_path.clone().or_else(|| parquet_path.clone());
    let cp_path = match &checkpoint_path {
        Some(cp) => cp.clone(),
        None => out
            .as_ref()
            .map(|p| p.with_extension("checkpoint"))
            .unwrap_or_else(|| PathBuf::from("export.checkpoint")),
    };
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

    // 构建 Sink（分叉目标）
    let mut sink: Box<dyn Sink> = match mode {
        "csv" => Box::new(CsvSink::new(csv_path.as_ref().unwrap()).unwrap_or_else(|e| {
            eprintln!("❌ CSV 创建失败: {e}");
            std::process::exit(1);
        })),
        "parquet" => Box::new(ParquetSink::new(parquet_path.as_ref().unwrap()).unwrap_or_else(|e| {
            eprintln!("❌ Parquet 创建失败: {e}");
            std::process::exit(1);
        })),
        _ => Box::new(JdbcSink::new(jdbc_url.as_ref().unwrap(), &table).unwrap_or_else(|e| {
            eprintln!("❌ JDBC 连接失败: {e}");
            std::process::exit(1);
        })),
    };

    let t = std::time::Instant::now();
    // 流式扫描 → Filter → Projection → 攒批 → Sink（内存恒定 = batch × 单行）
    let start = if base > 0 { Some(base + 1) } else { None };
    let mut rows: Vec<(u64, String)> = Vec::with_capacity(batch_size);
    let mut count = 0u64;
    let mut max_docid = base;
    let scan = engine.scan_stream(start, None, |docid, val| {
        let doc = std::str::from_utf8(val).map_err(|_| {
            shanshui_cunji::error::Error::Corrupted("导出值非 UTF-8".into())
        })?;
        // Filter：无条件时跳过 JSON 解析（原样透传）
        let text: String = if filter.is_empty() {
            doc.to_string()
        } else {
            let v: serde_json::Value = serde_json::from_str(doc).map_err(|_| {
                shanshui_cunji::error::Error::Corrupted(format!("docid={docid} JSON 解析失败"))
            })?;
            if !filter.matches(&v) {
                return Ok(true); // 条件不通过：跳过
            }
            if projection.is_identity() {
                doc.to_string()
            } else {
                projection.apply(&v)?
            }
        };
        count += 1;
        max_docid = max_docid.max(docid);
        rows.push((docid, text));
        if rows.len() >= batch_size {
            flush_batch(&mut *sink, &mut rows, rate_limit)?;
        }
        Ok(true)
    });
    match scan {
        Ok(()) => {}
        Err(e) => {
            eprintln!("❌ 导出失败: {e}");
            std::process::exit(1);
        }
    }
    flush_batch(&mut *sink, &mut rows, rate_limit).unwrap_or_else(|e| {
        eprintln!("❌ 导出失败: {e}");
        std::process::exit(1);
    });
    sink.finish().unwrap_or_else(|e| {
        eprintln!("❌ 导出完成失败: {e}");
        std::process::exit(1);
    });

    // 增量：推进 checkpoint（原子 tmp+rename）
    if incremental && max_docid > base {
        if let Err(e) = save_checkpoint(&cp_path, max_docid) {
            eprintln!("❌ checkpoint 写入失败: {e}");
            std::process::exit(1);
        }
        println!(
            "✅ 增量导出完成: {} 行（docid {}..={}）→ {}（{:.0} ms），游标推进至 {}",
            count,
            base + 1,
            max_docid,
            out.as_ref().map_or_else(|| jdbc_url.as_ref().unwrap().as_str(), |p| p.to_str().unwrap()),
            t.elapsed().as_secs_f64() * 1000.0,
            max_docid
        );
    } else {
        println!(
            "✅ 导出完成: {} 行 → {}（{:.0} ms）",
            count,
            out.as_ref().map_or_else(|| jdbc_url.as_ref().unwrap().as_str(), |p| p.to_str().unwrap()),
            t.elapsed().as_secs_f64() * 1000.0
        );
    }

    // --mysql-compatible：CSV 导出后生成 MySQL 配套 SQL（CREATE TABLE + LOAD DATA INFILE，
    // 比逐条 INSERT 快 ~20 倍；--mysql-max-varchar 控制 doc 列 VARCHAR/TEXT）
    if mysql_compatible && mode == "csv" {
        if let Some(csv) = &csv_path {
            let sql_path = csv.with_extension("sql");
            let sql = format!(
                "{}\n\n{}",
                mysql_ddl(&table, mysql_max_varchar),
                load_data_sql(&csv.to_string_lossy(), &table)
            );
            if let Err(e) = std::fs::write(&sql_path, &sql) {
                eprintln!("❌ MySQL 配套 SQL 写入失败: {e}");
                std::process::exit(1);
            }
            println!("✅ MySQL 配套 SQL 已生成（LOAD DATA INFILE）→ {}", sql_path.display());
        }
    }
}

/// 攒批刷新到 Sink（可选限流：每批按目标速率 sleep）。
fn flush_batch(
    sink: &mut dyn Sink,
    rows: &mut Vec<(u64, String)>,
    rate_limit: f64,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let batch: Vec<(u64, &str)> = rows.iter().map(|(d, s)| (*d, s.as_str())).collect();
    sink.write_batch(&batch)?;
    if rate_limit > 0.0 {
        // 目标批间隔 = 批行数 / 速率（秒）；保守 sleep 到满批时长
        let target = rows.len() as f64 / rate_limit;
        std::thread::sleep(std::time::Duration::from_secs_f64(target));
    }
    rows.clear();
    Ok(())
}

// ============ Sink 实现 ============

/// CSV Sink：两列 docid, json（RFC 4180 转义，csv crate）。
struct CsvSink {
    wtr: csv::Writer<std::fs::File>,
}

impl CsvSink {
    fn new(out: &PathBuf) -> Result<Self> {
        let mut wtr = csv::WriterBuilder::new()
            .from_path(out)
            .map_err(|e| shanshui_cunji::error::Error::Unsupported(e.to_string()))?;
        let _ = wtr.write_record(["docid", "json"]);
        Ok(Self { wtr })
    }
}

impl Sink for CsvSink {
    fn write_batch(&mut self, rows: &[(u64, &str)]) -> Result<()> {
        for (docid, doc) in rows {
            if self
                .wtr
                .write_record([docid.to_string(), doc.to_string()])
                .is_err()
            {
                break;
            }
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        self.wtr.flush().map_err(shanshui_cunji::error::Error::Io)
    }
}

/// Parquet Sink：两列 docid(Int64), json(Utf8)，SNAPPY 压缩，分块批量写。
struct ParquetSink {
    writer: Option<ArrowWriter<std::fs::File>>,
    schema: Arc<Schema>,
    ids: Vec<i64>,
    jsons: Vec<String>,
}

impl ParquetSink {
    fn new(out: &PathBuf) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("docid", DataType::Int64, false),
            Field::new("json", DataType::Utf8, false),
        ]));
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();
        let file = std::fs::File::create(out).map_err(shanshui_cunji::error::Error::Io)?;
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| shanshui_cunji::error::Error::Unsupported(e.to_string()))?;
        Ok(Self {
            writer: Some(writer),
            schema,
            ids: Vec::with_capacity(100_000),
            jsons: Vec::with_capacity(100_000),
        })
    }
}

impl Sink for ParquetSink {
    fn write_batch(&mut self, rows: &[(u64, &str)]) -> Result<()> {
        for (docid, doc) in rows {
            self.ids.push(*docid as i64);
            self.jsons.push(doc.to_string());
            if self.ids.len() >= 100_000 {
                self.flush_arrow()?;
            }
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        self.flush_arrow()?;
        if let Some(w) = self.writer.take() {
            w.close()
                .map_err(|e| shanshui_cunji::error::Error::Unsupported(e.to_string()))?;
        }
        Ok(())
    }
}

impl ParquetSink {
    fn flush_arrow(&mut self) -> Result<()> {
        if self.ids.is_empty() {
            return Ok(());
        }
        let rb = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(Int64Array::from(self.ids.clone())),
                Arc::new(StringArray::from(self.jsons.clone())),
            ],
        )
        .map_err(|e| shanshui_cunji::error::Error::Unsupported(e.to_string()))?;
        self.writer
            .as_mut()
            .unwrap()
            .write(&rb)
            .map_err(|e| shanshui_cunji::error::Error::Unsupported(e.to_string()))?;
        self.ids.clear();
        self.jsons.clear();
        Ok(())
    }
}

/// JDBC Sink（design 20.5）：直连目标 MySQL，建表 + 批量 INSERT（无文件落盘）。
struct JdbcSink {
    client: MysqlWireClient,
    table: String,
}

impl JdbcSink {
    fn new(url: &str, table: &str) -> Result<Self> {
        let mut client = MysqlWireClient::from_url(url)?;
        client.connect()?;
        // 建表（幂等）：docid BIGINT UNSIGNED 主键 + doc JSON 文本列
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS `{table}` (docid BIGINT UNSIGNED PRIMARY KEY, doc TEXT)"
        );
        client.query(&ddl)?;
        Ok(Self {
            client,
            table: table.to_string(),
        })
    }
}

impl Sink for JdbcSink {
    fn write_batch(&mut self, rows: &[(u64, &str)]) -> Result<()> {
        self.client.insert_batch(&self.table, rows)
    }
}

// ============ DDL 生成（design 20.5：--dry-run-schema / --mysql-compatible）============

/// MySQL 建表 DDL：docid BIGINT UNSIGNED 主键 + doc（`--mysql-max-varchar > 0` → VARCHAR(n)，
/// 否则 TEXT——处理 MySQL 65KB 行大小限制，超长字段降级）。
pub fn mysql_ddl(table: &str, max_varchar: usize) -> String {
    let doc_type = if max_varchar > 0 {
        format!("VARCHAR({max_varchar})")
    } else {
        "TEXT".to_string()
    };
    format!(
        "CREATE TABLE IF NOT EXISTS `{table}` (\n  `docid` BIGINT UNSIGNED NOT NULL PRIMARY KEY,\n  `doc` {doc_type}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    )
}

/// ClickHouse MergeTree 建表 DDL（Parquet 导出后 `INSERT ... SELECT FROM file('*.parquet')` 直读）。
pub fn clickhouse_ddl(table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {table} (\n  docid UInt64,\n  doc String\n) ENGINE = MergeTree\nORDER BY docid;"
    )
}

/// LOAD DATA INFILE 配套 SQL（比逐条 INSERT 快 ~20 倍；FIELDS 对齐 RFC 4180 CSV 输出）。
pub fn load_data_sql(out_csv: &str, table: &str) -> String {
    let csv = out_csv.replace('\\', "/"); // MySQL INFILE 路径用正斜杠
    format!(
        "LOAD DATA INFILE '{csv}'\nINTO TABLE `{table}`\nFIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\\\\'\nLINES TERMINATED BY '\\n'\nIGNORE 1 LINES\n(`docid`, `doc`);"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_ddl_default_text_and_varchar() {
        let ddl = mysql_ddl("t", 0);
        assert!(ddl.contains("BIGINT UNSIGNED NOT NULL PRIMARY KEY"));
        assert!(ddl.contains("`doc` TEXT"));
        assert!(!ddl.contains("VARCHAR"));
        let ddl2 = mysql_ddl("t", 255);
        assert!(ddl2.contains("`doc` VARCHAR(255)"));
        assert!(!ddl2.contains("TEXT"));
    }

    #[test]
    fn clickhouse_ddl_merge_tree() {
        let ddl = clickhouse_ddl("sbt_export");
        assert!(ddl.contains("docid UInt64"));
        assert!(ddl.contains("doc String"));
        assert!(ddl.contains("ENGINE = MergeTree"));
        assert!(ddl.contains("ORDER BY docid"));
    }

    #[test]
    fn load_data_sql_aligns_with_csv() {
        let sql = load_data_sql("D:\\tmp\\out.csv", "sbt_export");
        assert!(sql.contains("LOAD DATA INFILE 'D:/tmp/out.csv'"), "反斜杠转正斜杠");
        assert!(sql.contains("FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\\\\'"));
        assert!(sql.contains("IGNORE 1 LINES"));
        assert!(sql.contains("(`docid`, `doc`)"));
    }

    #[test]
    fn mysql_compatible_sql_composes_ddl_and_load() {
        // 模拟 --mysql-compatible 的配套 SQL 组装
        let sql = format!("{}\n\n{}", mysql_ddl("t", 255), load_data_sql("out.csv", "t"));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS"));
        assert!(sql.contains("LOAD DATA INFILE 'out.csv'"));
        // DDL 与 LOAD 的表名一致（各 1 次反引号表名）
        assert_eq!(sql.matches("`t`").count(), 2);
    }
}
