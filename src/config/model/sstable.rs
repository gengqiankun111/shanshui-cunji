//! SSTable 配置：压缩 / 冷档分层压缩 / 布隆过滤器 / 两级索引（design 4.8 / Ex-5.1 / Ex-8.12）。

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SstableConfig {
    /// none / snappy / lz4 / zstd。
    pub compression: String,
    /// zstd 专用 1~22。
    pub compression_level: u32,
    /// Ex-8.12：L2+ 冷档 zstd 压缩级别（0 = 不分层，全层用 `compression_level`）。
    /// 启用（>0）后：flush→L0 与 L0/L1 合并输出用热档 `compression_level`（避免中间层
    /// 放大）；L2 输出（L1→L2 下沉 / L2 内合并 / L2 单段重写）用本冷档高压缩率。
    /// 非 zstd 压缩器无 level 语义（忽略，无分层效果）。
    pub compression_level_l2: u32,
    /// 布隆假阳性率。
    pub bloom_fpr: f64,
    /// 两级索引：每 N 个 Block 一条摘要。
    pub index_granularity: u32,
}

impl Default for SstableConfig {
    fn default() -> Self {
        Self {
            compression: "zstd".into(),
            compression_level: 3,
            compression_level_l2: 0,
            bloom_fpr: 0.01,
            // Ex-5.1：与 4KB 块联动（块数 ×4），64 粒度保持 L1 摘要内存与 16KB 块×16 相当
            // （demo 实测 4KB+g64 vs 16KB+g16 摘要数比例 0.99）。
            index_granularity: 64,
        }
    }
}