//! 各数据源导入 loader：CSV / mysqldump / JSONL / Parquet 写入引擎。
//!
//! - 统一产出迁移报告（成功/失败/耗时），支持增量导入 docid 游标 checkpoint；
//! - 全量导入、单线程；批量导入只写不读（跳过 HotCache 回填/失效）。

use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::engine::Engine;
use crate::error::{Error, Result};

use super::parser::{parse_mysql_insert_line, SqlValue};

/// 迁移报告。
#[derive(Debug, Clone, Copy)]
pub struct ImportReport {
    pub rows: u64,
    pub failed: u64,
    /// 增量导入跳过的已导入行数（游标续传）。
    pub skipped: u64,
    pub elapsed_ms: u64,
}

impl ImportReport {
    fn new(rows: u64, failed: u64, skipped: u64, elapsed_ms: u64) -> Self {
        Self {
            rows,
            failed,
            skipped,
            elapsed_ms,
        }
    }
}

/// 读取增量导入 checkpoint（文件记录最大已导入 docid；缺失 = 0）。
pub fn load_checkpoint(path: &std::path::Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let text = std::fs::read_to_string(path)?;
    text.trim()
        .parse::<u64>()
        .map_err(|e| Error::Corrupted(format!("checkpoint 解析失败: {e}")))
}

/// 写入增量导入 checkpoint（tmp + rename 原子写）。
pub fn save_checkpoint(path: &std::path::Path, last_docid: u64) -> Result<()> {
    let tmp = path.with_extension("cp.tmp");
    std::fs::write(&tmp, last_docid.to_string())?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// CSV 全量导入：首行表头即 JSON 字段名；`docid` 列存在则用之，否则从 1 递增。
pub fn import_csv(engine: &mut Engine, path: &std::path::Path) -> Result<ImportReport> {
    import_csv_filtered(engine, path, None)
}

/// CSV 导入并应用倒排字段白名单（import-schema，design 20）：白名单存在时只对声明字段建倒排。
pub fn import_csv_filtered(
    engine: &mut Engine,
    path: &std::path::Path,
    whitelist: Option<&[String]>,
) -> Result<ImportReport> {
    import_csv_worker(engine, path, whitelist, None, None)
}

/// CSV **增量导入**（design 5.16 阶段 3 高级版）：基于 docid 游标断点续传。
/// `checkpoint_path` 记录已导入的最大 docid（原子写）；重启续跑只处理新增行（docid 更大），
/// 已导入行自动跳过。适合追加式日志 / 埋点数据增量同步。
pub fn import_csv_incremental(
    engine: &mut Engine,
    path: &std::path::Path,
    whitelist: Option<&[String]>,
    checkpoint_path: &std::path::Path,
) -> Result<ImportReport> {
    let base = load_checkpoint(checkpoint_path)?;
    import_csv_worker(engine, path, whitelist, Some(base), Some(checkpoint_path))
}

/// CSV 导入工作器（全量 / 增量共用）。
fn import_csv_worker(
    engine: &mut Engine,
    path: &std::path::Path,
    whitelist: Option<&[String]>,
    cp_base: Option<u64>,
    cp_path: Option<&std::path::Path>,
) -> Result<ImportReport> {
    let t = Instant::now();
    // P40：批量导入只写不读，跳过 HotCache 回填/失效
    engine.set_bulk_import(true);
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .map_err(|e| Error::Migrate(format!("CSV 打开失败: {e}")))?;
    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| Error::Migrate(format!("CSV 表头读取失败: {e}")))?
        .iter()
        .map(|h| h.to_string())
        .collect();
    if headers.is_empty() {
        return Err(Error::Unsupported("CSV 表头为空".into()));
    }
    let docid_col = headers.iter().position(|h| h == "docid");
    // 增量导入要求显式 docid 列（自动递增无法续传）
    if cp_base.is_some() && docid_col.is_none() {
        return Err(Error::Migrate(
            "增量导入必须含 docid 列（自动递增无法断点续传）".into(),
        ));
    }

    let mut rows = 0u64;
    let mut failed = 0u64;
    let mut skipped = 0u64;
    let mut last_docid = cp_base.unwrap_or(0);
    let mut next_id = 1u64;
    for rec in reader.records() {
        let rec = match rec {
            Ok(r) => r,
            Err(_) => {
                failed += 1;
                continue;
            }
        };
        let mut obj = serde_json::Map::new();
        for (i, field) in rec.iter().enumerate() {
            if let Some(name) = headers.get(i) {
                obj.insert(name.clone(), serde_json::Value::String(field.to_string()));
            }
        }
        // 主键：docid 列优先，否则递增
        let docid = match docid_col {
            Some(_) => match obj.get("docid").and_then(|v| v.as_str()) {
                Some(s) => match s.trim().parse::<u64>() {
                    Ok(d) => d,
                    Err(_) => {
                        failed += 1;
                        continue;
                    }
                },
                None => {
                    failed += 1;
                    continue;
                }
            },
            None => {
                // 自动分配：避让已占用 docid
                let mut d = next_id;
                while engine.get(d)?.is_some() {
                    d += 1;
                }
                next_id = d.wrapping_add(1);
                obj.insert("docid".into(), serde_json::Value::from(d));
                d
            }
        };
        // 增量：跳过已导入的 docid（游标续传）
        if let Some(base) = cp_base {
            if docid <= base {
                skipped += 1;
                continue;
            }
        }
        let bytes = serde_json::to_vec(&serde_json::Value::Object(obj))
            .map_err(|e| Error::Serialize(format!("JSON 序列化失败: {e}")))?;
        let val = serde_json::from_slice::<serde_json::Value>(&bytes)
            .map_err(|e| Error::Serialize(format!("JSON 解析失败: {e}")))?;
        // M8-P7：fulltext 字段分词建词 term（与白名单正交）；其余字段整串 term 受白名单过滤
        let whitelist_set = whitelist
            .map(|wl| wl.iter().cloned().collect::<std::collections::HashSet<_>>());
        let ft = engine.fulltext_fields().clone();
        let terms = crate::server::extract_terms_with_fulltext_seg(
            &val,
            whitelist_set.as_ref(),
            Some(&ft),
            engine.use_jieba(),
        );
        let term_refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        match engine.put(docid, bytes, &term_refs) {
            Ok(()) => {
                rows += 1;
                // 增量：推进并持久化 checkpoint（原子写）
                if docid > last_docid {
                    last_docid = docid;
                    if let Some(p) = cp_path {
                        save_checkpoint(p, last_docid)?;
                    }
                }
            }
            Err(_) => failed += 1,
        }
    }
    // 独立进程导入：倒排词条一次性刷盘
    engine.flush_inverted()?;
    Ok(ImportReport::new(
        rows,
        failed,
        skipped,
        t.elapsed().as_millis() as u64,
    ))
}

/// mysqldump 全量导入：解析 `INSERT INTO` 行构造 JSON 文档写入引擎。
pub fn import_mysqldump(engine: &mut Engine, path: &std::path::Path) -> Result<ImportReport> {
    let t = Instant::now();
    let text = std::fs::read_to_string(path)?;
    let mut rows = 0u64;
    let mut failed = 0u64;
    let mut next_id = 1u64;
    for line in text.lines() {
        let line = line.trim();
        if !line.starts_with("INSERT INTO") && !line.starts_with("insert into") {
            continue;
        }
        let Some((cols, tuples)) = parse_mysql_insert_line(line) else {
            continue;
        };
        for tuple in tuples {
            let mut obj = serde_json::Map::new();
            for (i, v) in tuple.iter().enumerate() {
                let name = cols.get(i).cloned().unwrap_or_else(|| format!("c{i}"));
                let jv = match v {
                    SqlValue::Str(s) => serde_json::Value::String(s.clone()),
                    SqlValue::Num(n) => {
                        if let Ok(i) = n.parse::<i64>() {
                            serde_json::Value::from(i)
                        } else if let Ok(f) = n.parse::<f64>() {
                            serde_json::Value::from(f)
                        } else {
                            serde_json::Value::String(n.clone())
                        }
                    }
                    SqlValue::Null => serde_json::Value::Null,
                    SqlValue::Other(o) => serde_json::Value::String(o.clone()),
                };
                obj.insert(name, jv);
            }
            // 主键：docid / id 列（MySQL 惯例）优先，否则递增
            let pk = if obj.contains_key("docid") {
                Some("docid")
            } else if obj.contains_key("id") {
                Some("id")
            } else {
                None
            };
            let docid = match pk {
                Some(k) => match obj.get(k) {
                    Some(serde_json::Value::Number(n)) => n.as_u64(),
                    Some(serde_json::Value::String(s)) => s.trim().parse::<u64>().ok(),
                    _ => None,
                },
                None => None,
            };
            let docid = match docid {
                Some(d) => d,
                None => {
                    // 自动分配：避让已占用 docid（SQL 显式 id 与递增可能冲突）
                    let mut d = next_id;
                    while engine.get(d)?.is_some() {
                        d += 1;
                    }
                    next_id = d.wrapping_add(1);
                    obj.insert("docid".into(), serde_json::Value::from(d));
                    d
                }
            };
            let bytes = match serde_json::to_vec(&serde_json::Value::Object(obj)) {
                Ok(b) => b,
                Err(_) => {
                    failed += 1;
                    continue;
                }
            };
            let terms = match serde_json::from_slice::<serde_json::Value>(&bytes) {
                Ok(v) => crate::server::extract_terms_with_fulltext_seg(
                    &v,
                    None,
                    Some(engine.fulltext_fields()),
                    engine.use_jieba(),
                ),
                Err(_) => {
                    failed += 1;
                    continue;
                }
            };
            let term_refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            match engine.put(docid, bytes, &term_refs) {
                Ok(()) => rows += 1,
                Err(_) => failed += 1,
            }
        }
    }
    engine.flush_inverted()?;
    Ok(ImportReport::new(
        rows,
        failed,
        0,
        t.elapsed().as_millis() as u64,
    ))
}

/// JSONL 全量导入（数据管道 `import --json`，development 5.27）：每行一个 JSON 对象，
/// 含 docid/id 列作主键（否则从 1 递增），导入完成输出迁移报告。
pub fn import_json(engine: &mut Engine, path: &std::path::Path) -> Result<ImportReport> {
    import_json_filtered(engine, path, None)
}

/// JSONL 导入并应用倒排字段白名单（import-schema，design 20）。
pub fn import_json_filtered(
    engine: &mut Engine,
    path: &std::path::Path,
    whitelist: Option<&[String]>,
) -> Result<ImportReport> {
    import_json_worker(engine, path, whitelist, None, None)
}

/// JSONL **增量导入**（design 5.16 阶段 3）：docid 游标断点续传（需 docid/id 列）。
pub fn import_json_incremental(
    engine: &mut Engine,
    path: &std::path::Path,
    whitelist: Option<&[String]>,
    checkpoint_path: &std::path::Path,
) -> Result<ImportReport> {
    let base = load_checkpoint(checkpoint_path)?;
    import_json_worker(engine, path, whitelist, Some(base), Some(checkpoint_path))
}

/// JSONL 导入工作器（全量 / 增量共用）。
fn import_json_worker(
    engine: &mut Engine,
    path: &std::path::Path,
    whitelist: Option<&[String]>,
    cp_base: Option<u64>,
    cp_path: Option<&std::path::Path>,
) -> Result<ImportReport> {
    let t = Instant::now();
    // P40：批量导入只写不读，跳过 HotCache 回填/失效
    engine.set_bulk_import(true);
    let text = std::fs::read_to_string(path)?;
    let mut rows = 0u64;
    let mut failed = 0u64;
    let mut skipped = 0u64;
    let mut last_docid = cp_base.unwrap_or(0);
    let mut next_id = 1u64;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut obj: serde_json::Map<String, serde_json::Value> = match serde_json::from_str(line) {
            Ok(serde_json::Value::Object(m)) => m,
            _ => {
                failed += 1;
                continue;
            }
        };
        // 主键：docid / id 列优先，否则递增
        let pk = if obj.contains_key("docid") {
            Some("docid")
        } else if obj.contains_key("id") {
            Some("id")
        } else {
            None
        };
        let docid = match pk.and_then(|k| obj.get(k)) {
            Some(serde_json::Value::Number(n)) => n.as_u64(),
            Some(serde_json::Value::String(s)) => s.trim().parse::<u64>().ok(),
            _ => None,
        };
        let docid = match docid {
            Some(d) => d,
            None => {
                // 增量要求显式 docid/id 列（自动递增无法续传）
                if cp_base.is_some() {
                    failed += 1;
                    continue;
                }
                // 自动分配：避让已占用 docid（显式 docid 与递增可能冲突）
                let mut d = next_id;
                while engine.get(d)?.is_some() {
                    d += 1;
                }
                next_id = d.wrapping_add(1);
                obj.insert("docid".into(), serde_json::Value::from(d));
                d
            }
        };
        // 增量：跳过已导入的 docid（游标续传）
        if let Some(base) = cp_base {
            if docid <= base {
                skipped += 1;
                continue;
            }
        }
        let bytes = match serde_json::to_vec(&serde_json::Value::Object(obj)) {
            Ok(b) => b,
            Err(_) => {
                failed += 1;
                continue;
            }
        };
        let terms = match serde_json::from_slice::<serde_json::Value>(&bytes) {
            Ok(v) => {
                // M8-P7：fulltext 字段分词建词 term（与白名单正交）；其余字段整串 term 受白名单过滤
                let whitelist_set = whitelist
                    .map(|wl| wl.iter().cloned().collect::<std::collections::HashSet<_>>());
                let ft = engine.fulltext_fields().clone();
                crate::server::extract_terms_with_fulltext_seg(
                    &v,
                    whitelist_set.as_ref(),
                    Some(&ft),
                    engine.use_jieba(),
                )
            }
            Err(_) => {
                failed += 1;
                continue;
            }
        };
        let term_refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        match engine.put(docid, bytes, &term_refs) {
            Ok(()) => {
                rows += 1;
                // 增量：推进并持久化 checkpoint（原子写）
                if docid > last_docid {
                    last_docid = docid;
                    if let Some(p) = cp_path {
                        save_checkpoint(p, last_docid)?;
                    }
                }
            }
            Err(_) => failed += 1,
        }
    }
    engine.flush_inverted()?;
    Ok(ImportReport::new(
        rows,
        failed,
        skipped,
        t.elapsed().as_millis() as u64,
    ))
}

/// Parquet 全量导入（大数据集，如 5000 万条 × 20 字段）：读 parquet → `put_nosync` 批量
/// 写入（每 `FLUSH_EVERY` 条统一 fsync 一次 + 结尾统一提交），倒排词条自动派生。
/// 主键：`docid` 列存在则用之，否则从 1 递增。
pub fn import_parquet(
    engine: &mut Engine,
    path: &std::path::Path,
    whitelist: Option<&[String]>,
) -> Result<ImportReport> {
    let t = Instant::now();
    // P40：批量导入只写不读，跳过 HotCache 回填/失效（避免 4GB 缓存灌满挤爆内存触发页面颠簸）
    engine.set_bulk_import(true);
    let file =
        std::fs::File::open(path).map_err(|e| Error::Migrate(format!("Parquet 打开失败: {e}")))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Migrate(format!("Parquet 元数据解析失败: {e}")))?;
    let schema_fields = builder.schema().fields().clone();
    let docid_idx = schema_fields.iter().position(|f| f.name() == "docid");
    // 生成前字段过滤（M8-P4）：import-schema 白名单直接传给 extract（不匹配 term 不分配）
    let include: Option<std::collections::HashSet<String>> =
        whitelist.map(|wl| wl.iter().cloned().collect());
    let reader = builder
        .build()
        .map_err(|e| Error::Migrate(format!("Parquet reader 构建失败: {e}")))?;

    let mut rows = 0u64;
    let mut failed = 0u64;
    let mut next_id = 1u64;
    // 批量提交：每 FLUSH_EVERY 条统一 fsync + 倒排刷盘一次（5000 万条逐条 fsync 需数小时，批量分钟级；
    // 倒排 posting 定期刷段控制内存——100 万行 × ~10 词条 = 1 亿 posting ≈ 2-3GB，刷 20 次较优；
    // 过频（50 万）刷段开销大拖慢导入）
    const FLUSH_EVERY: u64 = 1_000_000;
    let mut since_flush = 0u64;
    for batch in reader {
        let batch = match batch {
            Ok(b) => b,
            Err(_) => {
                failed += 1;
                continue;
            }
        };
        let n = batch.num_rows();
        let cols: Vec<ArrayRef> = batch.columns().to_vec();
        for r in 0..n {
            let mut obj = serde_json::Map::new();
            for (idx, f) in schema_fields.iter().enumerate() {
                if let Some(v) = arr_value(&cols[idx], r) {
                    obj.insert(f.name().clone(), v);
                }
            }
            // 主键：docid 列优先，否则递增
            let docid = match docid_idx {
                Some(_) => match obj.get("docid").and_then(|v| v.as_i64()) {
                    Some(d) if d >= 0 => d as u64,
                    _ => {
                        failed += 1;
                        continue;
                    }
                },
                None => {
                    let d = next_id;
                    next_id = d.wrapping_add(1);
                    obj.insert("docid".into(), serde_json::Value::from(d));
                    d
                }
            };
            let val = serde_json::Value::Object(obj);
            let bytes = serde_json::to_vec(&val)
                .map_err(|e| Error::Serialize(format!("JSON 序列化失败: {e}")))?;
            // 生成前字段过滤 + 引擎运行时过滤（白名单/黑名单/超长 term）双重兜底；
            // M8-P7：fulltext 字段分词建词 term（与白名单正交），其余字段整串 term 受白名单过滤
            let terms = crate::server::extract_terms_with_fulltext_seg(
                &val,
                include.as_ref(),
                Some(engine.fulltext_fields()),
                engine.use_jieba(),
            );
            let term_refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            match engine.put_nosync(docid, bytes, &term_refs) {
                Ok(()) => {
                    rows += 1;
                    since_flush += 1;
                    // 细粒度进度（每 10 万行，定位卡点行区间用）
                    if rows % 100_000 == 0 {
                        use std::io::Write;
                        println!(
                            "  [progress] {rows} 行 · 累计 {:.0}s · {:.0} 行/s",
                            t.elapsed().as_secs_f64(),
                            rows as f64 / t.elapsed().as_secs_f64()
                        );
                        std::io::stdout().flush().ok();
                    }
                    if since_flush >= FLUSH_EVERY {
                        let t0 = std::time::Instant::now();
                        engine.flush_wal()?;
                        let t1 = std::time::Instant::now();
                        engine.flush_inverted()?; // 倒排 posting 定期刷段，控制内存
                        let t2 = std::time::Instant::now();
                        engine.flush_primary()?; // 主数据强制刷盘（防 memtable 异常累积卡顿）
                        let t3 = std::time::Instant::now();
                        since_flush = 0;
                        println!(
                            "[import-parquet] 已导入 {rows} 行（WAL {:.1}s / 倒排 {:.1}s / 主 {:.1}s）",
                            t1.duration_since(t0).as_secs_f64(),
                            t2.duration_since(t1).as_secs_f64(),
                            t3.duration_since(t2).as_secs_f64()
                        );
                        use std::io::Write;
                        std::io::stdout().flush().ok(); // 实时进度（管道缓冲）
                    }
                }
                Err(_) => failed += 1,
            }
        }
    }
    engine.flush_wal()?;
    engine.flush_inverted()?;
    println!(
        "[import-parquet] 完成: {rows} 行成功 / {failed} 失败 · {:.0} ms",
        t.elapsed().as_millis() as f64
    );
    Ok(ImportReport::new(
        rows,
        failed,
        0,
        t.elapsed().as_millis() as u64,
    ))
}

/// 从 arrow 数组取第 row 行的值（Int64/Int32/Float64/Boolean/Utf8 → serde_json Value）。
fn arr_value(col: &ArrayRef, row: usize) -> Option<serde_json::Value> {
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return a
            .is_valid(row)
            .then(|| serde_json::Value::from(a.value(row)));
    }
    if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        return a
            .is_valid(row)
            .then(|| serde_json::Value::from(a.value(row)));
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return a
            .is_valid(row)
            .then(|| serde_json::Value::from(a.value(row)));
    }
    if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
        return a
            .is_valid(row)
            .then(|| serde_json::Value::Bool(a.value(row)));
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return a
            .is_valid(row)
            .then(|| serde_json::Value::String(a.value(row).to_string()));
    }
    None
}
