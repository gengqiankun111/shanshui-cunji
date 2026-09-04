//! 迁移工具核心：CSV / mysqldump / JSONL / Parquet 数据导入。
//!
//! - 输入：CSV 文件（首行表头）、mysqldump SQL 导出（`INSERT INTO ... VALUES (...)` 行）、
//!   JSONL（每行一个 JSON 对象）、Parquet；
//! - 字段映射：源列名直接作为 JSON 字段名；含 `docid`（或 `id`）列则作为主键，否则从 1 递增；
//! - 支持全量导入与 docid 游标断点续传的增量导入，单线程，产出迁移报告（成功/失败/耗时）。

mod loader;
mod parser;

pub use loader::{
    import_csv, import_csv_filtered, import_csv_incremental, import_json, import_json_filtered,
    import_json_incremental, import_mysqldump, import_parquet, load_checkpoint, save_checkpoint,
    ImportReport,
};
pub use parser::{parse_mysql_insert_line, SqlValue};

#[cfg(test)]
mod tests;
