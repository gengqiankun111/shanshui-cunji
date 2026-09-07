//! 倒排索引配置：引擎 / 字典 / 段合并 / 位图 / FST / 分词（design 5.2 / M8 / Ex-9.3 / P4-B）。

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct InvertedConfig {
    /// hash（MVP）/ fst（阶段 1.5，mmap 亚秒冷启动）。
    pub engine: String,
    /// 倒排字典内存硬上限（MB）。
    pub max_memory_bytes: usize,
    /// 倒排刷盘阈值（term-docid 对数，L 项）：内存累计达此值刷一段。100 万 → 500 万：
    /// 段数 -83%（5000 万库 1240→210 段）、查询段遍历 -83%、刷盘频率 -80%；代价 = 内存
    /// +阈值大小、单次刷盘停顿 ×5。0 = 用默认 100 万。
    pub flush_threshold: u64,
    /// 魔鬼倒排列表门控：超过则不展开，降级全表扫描 + Zone Map。
    pub max_posting_scan: u64,
    /// 倒排段 GC 阈值（MB，design 5.2.2 / 5.2.4⑤）：段文件总量超此值触发后台合并。
    pub segment_max_size_mb: u64,
    /// 位图索引字段白名单（design 5.2.4，M7-2）：枚举/离散字段名（如 status / city），
    /// 命中字段的值位图**常驻内存**（term → RoaringBitmap），COUNT / GROUP BY / AND 组合筛选
    /// 走内存位图快速路径（亚毫秒）；空 = 关闭（默认，零额外开销）。
    pub bitmap_fields: Vec<String>,
    /// 倒排字段白名单（M8-P4 / P131b）：**显式声明才建立倒排词条**（其余字段不索引）——
    /// 高基数 ID / 长文本字段建倒排是纯浪费（每 term 单 posting，字典膨胀 45 万倍，实测）；
    /// 经验准则：100 字段表倒排字段 ≤ 20（枚举/标签类）；**空 = 无倒排（零声明零索引，
    /// 不再有"空 = 全部字符串字段建"的隐式默认）**。声明来源：库内 cj.schema.json（权威）
    /// 或启动 --config；未声明字段的等值/范围查询只能走主键/组合索引/全表扫描。
    /// Ex-4 配置模板（design_extension 9.4）见仓库 `config.import-example.toml`——
    /// 枚举/低基数白名单 + 高基数 exclude_fields 补充排除 + 长文本 fulltext 分词，
    /// db-50m 实测 inverted 523.5MB → ~200MB（排除 note 后）。
    pub inverted_fields: Vec<String>,
    /// 倒排字段黑名单（M8-P4）：这些字段**不建倒排**——对白名单的补充排除
    /// （白名单命中且不在黑名单才建词条）；白名单为空（零声明）时无任何普通词条。
    pub exclude_fields: Vec<String>,
    /// **声明制开关**（P131b，2026-09-07）：true = 显式声明制——`inverted_fields` 空即
    /// **零倒排**（不再回退"空 = 全部字符串字段建倒排"的 legacy 全字段行为）；false = legacy
    /// 兼容（空 = 全字段，供引擎内部/单元测试与旧装载路径）。**服务入口（cjserver/库内
    /// schema）恒为 true**：索引一律用户显式声明（cj.schema.json 或 --config）。
    #[serde(default)]
    pub declared_only: bool,
    /// 倒排 term 长度上限（字节，M8-P4）：超过的 term 自动跳过（长文本/长字符串整串进字典
    /// 是纯浪费——每 term 单 posting + 字典膨胀；默认 96 = 长文本自动不建倒排，0 = 不限）。
    pub max_term_len: usize,
    /// fulltext 分词字段（M8-P7）：声明字段做**分词建词 term 索引**（`ft:{field}:{token}`），
    /// **取代整串 term**——长文本（>max_term_len）整串被跳过无法检索，分词后 token 短可建索引，
    /// 支持关键词检索；与 inverted_fields 白名单正交（fulltext 字段优先分词，不生成整串）。
    /// 空 = 关闭（默认，零开销）。
    pub fulltext_fields: Vec<String>,
    /// 中文分词器（M8-P13）：`bigram`（默认，M8-P9 字符碎片，零依赖）/ `jieba`（完整中文
    /// 词典分词——语义词精确命中、索引词数更少；需 `cjk-jieba` feature，关闭时回退 bigram）。
    pub cjk_segmenter: String,
    /// 倒排统计载荷字段（Ex-9.3）：声明数字字段后，写路径随 term 累积该字段
    /// sum/min/max/avg（term → 文档子集聚合）——支撑 `SUM(amount) WHERE status='x'` /
    /// `GROUP BY status` 聚合免全扫（配合 v5 段载荷；第①步仅内存段累积）。
    /// 空 = 关闭（默认，零额外写开销）；多数字字段全开失控——对齐 Ex-4 成本控制准则。
    pub stats_fields: Vec<String>,
    /// P4-B：delta FST 大小上限（MB）——最后一段 FST 超过此大小自动触发合并进 base
    /// （0 = 默认 16MB，每次合并后新 delta 从零开始）。
    pub delta_fst_max_mb: u64,
}

impl Default for InvertedConfig {
    fn default() -> Self {
        Self {
            // 阶段 1.5 起默认 FST + mmap 字典（design 5.2.4.1：亚秒冷启动、按需加载）
            engine: "fst".into(),
            max_memory_bytes: 12 * 1024 * 1024 * 1024,
            flush_threshold: 1_000_000,
            max_posting_scan: 1_000_000,
            segment_max_size_mb: 1024,
            bitmap_fields: Vec::new(),
            inverted_fields: Vec::new(),
            exclude_fields: Vec::new(),
            declared_only: false,
            max_term_len: 96,
            fulltext_fields: Vec::new(),
            cjk_segmenter: "bigram".into(),
            stats_fields: Vec::new(),
            // P4-B：delta FST 上限默认 16MB，超过后自动 roll into base
            delta_fst_max_mb: 16,
        }
    }
}