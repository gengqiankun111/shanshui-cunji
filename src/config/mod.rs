//! 配置模型、加载、校验与环境变量覆盖（design 13 / development 步骤 1）。
//!
//! - TOML 加载（`serde(default)` 保证缺省字段有默认值）；
//! - 环境变量覆盖：`SHANSHUI_CUNJI__SECTION__KEY`（如 `SHANSHUI_CUNJI__HOTCACHE__MAX_MEMORY_MB=2048`）；
//! - 启动校验：HotCache + BlockCache 预算 < 可用内存 × 0.7（越界警告并降级）。

pub mod model;

pub use model::*;
