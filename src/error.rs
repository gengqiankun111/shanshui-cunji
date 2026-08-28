//! 统一错误类型（development 第 2 章 / 步骤 1）。

use std::io;

use thiserror::Error;

/// shanshui-cunji 全局错误类型。
#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("config error: {0}")]
    Config(String),

    #[error("corrupted data: {0}")]
    Corrupted(String),

    #[error("serialization error: {0}")]
    Serialize(String),

    #[error("key not found: {0}")]
    NotFound(String),

    #[error("unsupported: {0}")]
    Unsupported(String),

    #[error("memory overload: {0}")]
    MemoryOverload(String),

    #[error("query too expensive: {0}")]
    QueryTooExpensive(String),

    #[error("stalled: {0}")]
    Stalled(String),

    #[error("migrate error: {0}")]
    Migrate(String),
}

/// shanshui-cunji 全局结果类型。
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_is_human_readable() {
        let e = Error::NotFound("docid=1001".into());
        assert_eq!(e.to_string(), "key not found: docid=1001");
    }

    #[test]
    fn io_error_converts() {
        let io_err = io::Error::other("disk full");
        let e: Error = io_err.into();
        assert!(e.to_string().starts_with("io error:"));
    }
}
