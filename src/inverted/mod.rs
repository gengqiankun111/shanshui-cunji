//! 倒排索引（design 5.2 / development 步骤 10）。
//!
//! 阶段 1.5 架构（FST + 字典）：
//! - **内存哈希字典**：`DashMap<term, Vec<docid>>` 收集增量写入（保留，热点最新数据）；
//! - **Append-Only 倒排段文件**：达阈值后整段刷盘 `inverted-{id}.seg`，
//!   段内每 term 的 posting 用 **RoaringBitmap** 序列化存储；
//! - **FST 术语字典（design 5.2.4.1）**：每段刷盘时编译 `inverted-{id}.fst`（term → 段内条目字节偏移），
//!   查询用 FST O(len(term)) 精确定位，替代逐段线性扫描；启动时加载为内存不可变字典，
//!   无 FST 的旧段回退线性扫描（兼容）；
//! - **段清单 Manifest**（`inverted-manifest.json`）：记录段文件列表（新→旧），
//!   原子写（tmp + rename），杜绝 GC 崩溃风险（design 4.5）；
//! - **查询**：内存字典 ∪ 各段 posting 合并为 RoaringBitmap；
//! - 阶段 2 架构升级：
//!   - **预分片 Chunk（design 5.2.1）**：`chunk_for_shard` 按 `hash64(docid) % shard_count`
//!     抽出属于指定分片的 posting 子集，网关 `concatenate_chunks` 按序直拼（O(1)），广播查询免交集/并集；
//!   - **倒排段 GC（design 5.2.2 + 5.2.4⑤）**：段总量超 `segment_max_size_mb` 阈值时
//!     `gc()` 将全部段合并为单个紧凑段（临时文件 → fsync → 原子更新 Manifest → 删旧段），
//!     中途崩溃不丢数据；分层 Tiered Segments 合并（每次只合并最小 2 段）留后续优化。
//!
//! > mmap 按需加载（冷启动亚秒）为设计目标；本项目 `#![forbid(unsafe_code)]`，
//! > Ex-5.7 已落地：独立 crate `mmap-file`（crates/mmap-file/，P23 unsafe 白名单）封装
//! > 只读 mmap 安全 API，FST 字典改用 `fst::Map<MmapFile>`——主库源码保持零 unsafe。

// 重构自 src/inverted.rs：按主题拆分为子模块（segment/query/write/gc/stats/tests），
// 本文件保留模块文档 + InvertedIndex 主类型 + 打开构造 + 汇总 pub use，行为零变化。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use lru::LruCache;
use roaring::treemap::RoaringTreemap;
use tracing::info;

use crate::error::{Error, Result};
use crate::per_cpu::PerCpuCounter;
use mmap_file::MmapFile;

use segment::{MANIFEST_FILE, SegmentManifest};

mod gc;
mod query;
mod segment;
mod stats;
mod write;

pub use gc::GcReport;
pub use stats::FieldAgg;

#[cfg(test)]
mod tests;

/// 64 位 posting / 位图类型（多表 docid = `table_id<<48 | row_id`，非默认表 ≥2^32；
/// RoaringTreemap 按高 32 位分键，低 docid 场景与 RoaringBitmap 容器同构、开销相当）。
type Posting = RoaringTreemap;

/// 倒排索引：内存字典 + 磁盘段集合 + FST 术语字典。
pub struct InvertedIndex {
    dir: PathBuf,
    /// 字典引擎："hash"（纯线性）/ "fst"（FST 精确查找）。
    engine: String,
    /// term → 内存收集的 docid 列表。
    mem: DashMap<String, Vec<u64>>,
    /// Ex-9.3 第①步：term → 各声明 stats 字段聚合（写路径随 term 累积；独立于 mem，
    /// 不影响既有 flush/检索路径）。仅当配置 stats_fields 非空时由 Engine 调用。
    stats_mem: DashMap<String, Vec<FieldAgg>>,
    /// 内存累计 docid 数（触发刷盘阈值判断）。Ex-7.1：PerCpuCounter 按核拆分——
    /// 多核并发写 posting 时消除原子计数器伪共享（demo 实测 2.1×）。
    mem_docids: PerCpuCounter,
    /// 段文件列表（新→旧，仅含文件名）。
    /// Ex-6.2：ArcSwap 原子发布——flush/gc 更新（rcu/store），search/iter_terms 读快照无锁
    /// （读线程持 &InvertedIndex 时无需外部锁；快照一致性：旧 Arc 在发布后仍有效）。
    segments: ArcSwap<Vec<String>>,
    /// 段 → FST 术语字典（term → 段内条目字节偏移）；engine=fst 且存在 .fst 时填充。
    /// Ex-5.7：mmap 只读映射（MmapFile 安全封装，P23 unsafe 白名单）——冷启动零堆分配、
    /// 物理页按需缺页加载（design 5.2.4.1），替代旧 fs::read 全量读入。
    /// Ex-6.3：值 `Arc<fst::Map>`（MmapFile 不可 Clone，Arc 使 HashMap 可整体 Clone 供
    /// rcu 发布）——查询拿 Arc 快照零拷贝。
    dicts: ArcSwap<HashMap<String, Arc<fst::Map<MmapFile>>>>,
    next_seg_id: AtomicU64,
    /// J 项（7.73）：flush_segment 与 gc 写 Manifest / 删段文件互斥——GC 后台线程化后，
    /// 写路径 flush 与后台 gc 并发（对齐 CF `sst_mutate` 模式）；无此锁会丢失更新
    /// （demo inverted-gc-bg 确定性复现：flush 持旧快照期间 gc 删文件 → Manifest 引用已删段）。
    mutate: std::sync::Mutex<()>,
    /// 刷盘阈值：内存累计 posting 达此值整段落盘。
    flush_threshold: u64,
    /// 段文件 GC 阈值（字节）：磁盘段总量超此值触发 `gc()` 合并（design 5.2.2；0 = 禁用）。
    gc_threshold_bytes: u64,
    /// P4-B：delta FST 大小上限（字节）——最后一段 FST 超过此大小自动触发合并进 base
    /// （0 = 默认 16MB，每次合并后新 delta 从零开始）。
    delta_fst_max_bytes: u64,
    /// Ex-8.13：后台 IO 预算（Token Bucket，与列族压缩共享同一"写压力收窄后"预算语义）。
    /// GC/后台段写（`account_written_budgeted`）acquire 节流；前台紧急刷段
    /// （`flush_segment`）仅记账不等待——保留写入语义（预算不足时不停前台）。
    io_limiter: std::sync::Mutex<Option<crate::io_scheduler::IoRateLimiter>>,
    /// Ex-8.13：倒排累计写盘字节（seg 每次新写文件累计一次；GC 与刷段均计——写放大/IO 审计数据源）。
    inverted_written: AtomicU64,
    /// 位图索引字段白名单（design 5.2.4，M7-2）：空 = 关闭（默认零开销）。
    bitmap_fields: std::collections::HashSet<String>,
    /// 内存位图索引（Ex-5.2 分片）：field → (value → docid RoaringBitmap)，按 field hash 分
    /// `BITMAP_SHARDS` 片锁——不同 field 并行、同 field 串行（group_by 需同 field 全量一致）。
    /// 写入时同步维护、重启重建。
    bitmaps: Vec<std::sync::Mutex<
        std::collections::HashMap<String, std::collections::HashMap<String, Posting>>,
    >>,
    /// G 项 + Ex-8.8（design_extension 9.6）：term → posting 位图缓存（**双区 LRU**，
    /// protected 60% + probation 40%；POSTING_CACHE_CAP 总量 256）。
    /// 非白名单 term（fulltext 词等）查询首次反序列化后缓存，重复查询直接返回——
    /// posting 随规模线性（5000 万库单次反序列化 ~10-200ms），缓存后 O(1)。
    /// 写路径（add/add_batch/flush_segment/gc/with_bitmap_fields）清空保证一致性。
    posting_cache: std::sync::Mutex<PostingLru>,
    /// G 补充（design_extension 9.6 候选② / K 项落地）：段数据文件 mmap 化——查询按 FST
    /// offset 直接切片反序列化，免 `fs::read` 全文件读取 + 堆复制（大段文件未命中查询的
    /// 主要 IO 成本），物理页按需缺页加载（P23 只读映射白名单，与 dicts 同模式）。
    /// 运行期懒加载（首次查询注册）；gc 先换新映射再删旧文件（Windows 已映射不可删）。
    data_files: ArcSwap<HashMap<String, Arc<MmapFile>>>,
}

/// 位图索引分片锁数（Ex-5.2，design 4.8.3 P0-4）。
const BITMAP_SHARDS: usize = 256;

/// G 项：posting 位图缓存容量（LRU 项数；按 term 池规模取 256——覆盖倒排枚举 + fulltext 词条）。
const POSTING_CACHE_CAP: usize = 256;

/// Ex-8.8：posting 位图**双区 LRU**（Segmented/2Q，仿 HotCache 双区）——
/// 高频 term 命中后提升进 `protected` 保护区，免受低频 term 突发（流式/扫词负载）逐出；
/// 新 term 只入 `probation`（普通区）。命中：protected 直返 / probation 命中 → 提升保护。
/// 写路径清空两区（一致性同前）。容量 = protected 60% + probation 40%（参数化）。
struct PostingLru {
    protected: LruCache<String, Arc<Posting>>,
    probation: LruCache<String, Arc<Posting>>,
}

impl PostingLru {
    fn new(total: usize) -> Self {
        let pcap = (total * 3) / 5;
        Self {
            protected: LruCache::new(std::num::NonZeroUsize::new(pcap).unwrap()),
            probation: LruCache::new(std::num::NonZeroUsize::new((total - pcap).max(1)).unwrap()),
        }
    }

    /// 命中返回缓存位图；probation 命中 → 提升进 protected（下次同 term 直返）。
    fn get(&mut self, term: &str) -> Option<Arc<Posting>> {
        if let Some(v) = self.protected.get(term) {
            return Some(v.clone());
        }
        if let Some(v) = self.probation.pop(term) {
            self.protected.put(term.to_string(), v.clone());
            return Some(v);
        }
        None
    }

    /// 入缓存：protected 已在 → no-op（刷新）；否则入 probation（满则逐出其 LRU=低频冷 term）。
    fn put(&mut self, term: String, v: Arc<Posting>) {
        if self.protected.contains(term.as_str()) {
            return;
        }
        self.probation.put(term, v);
    }

    fn clear(&mut self) {
        self.protected.clear();
        self.probation.clear();
    }

    fn contains(&self, term: &str) -> bool {
        self.protected.contains(term) || self.probation.contains(term)
    }
}

/// FNV-1a 字段分片：field → 分片下标（确定性，同 field 恒同片）。
fn bitmap_shard(field: &str) -> usize {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for b in field.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h % BITMAP_SHARDS as u64) as usize
}

/// 解码段文件计数 / term 条目数（LEB128）。
fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64> {
    crate::keys::decode_varint(data, pos)
}

fn encode_varint(buf: &mut Vec<u8>, n: u64) {
    crate::keys::encode_varint(buf, n);
}

impl InvertedIndex {
    /// 打开（或创建）倒排索引：加载 Manifest 与 FST 字典。默认 FST 引擎（阶段 1.5）。
    /// GC 默认禁用（`gc_threshold_bytes = 0`），由引擎按配置开启。
    pub fn open(dir: &Path, flush_threshold: u64) -> Result<Self> {
        Self::open_with_engine(dir, flush_threshold, "fst")
    }

    /// 打开（或创建）倒排索引，指定字典引擎。GC 默认禁用。
    pub fn open_with_engine(dir: &Path, flush_threshold: u64, engine: &str) -> Result<Self> {
        Self::open_with_gc(dir, flush_threshold, engine, 0)
    }

    /// 打开（或创建）倒排索引，指定字典引擎与段 GC 阈值（字节）。
    pub fn open_with_gc(
        dir: &Path,
        flush_threshold: u64,
        engine: &str,
        gc_threshold_bytes: u64,
    ) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let manifest_path = dir.join(MANIFEST_FILE);
        let (segments, next_seg_id) = if manifest_path.exists() {
            let text = std::fs::read_to_string(&manifest_path)?;
            let m: SegmentManifest = serde_json::from_str(&text)
                .map_err(|e| Error::Corrupted(format!("倒排 Manifest 解析失败: {e}")))?;
            (m.segments, m.next_seg_id)
        } else {
            (Vec::new(), 1)
        };
        info!(
            "倒排索引打开: {} 个段，下一个 id={next_seg_id}",
            segments.len()
        );
        // 加载 FST 术语字典（design 5.2.4.1）：每个段对应 inverted-{id}.fst；
        // 缺失的段（旧数据 / hash 引擎写入）在查询时回退线性扫描。
        // Ex-5.7：mmap 只读映射按需加载（替代 fs::read 全量读入）——冷启动零堆分配。
        let mut dicts = HashMap::new();
        if engine == "fst" {
            for seg in &segments {
                let fst_name = seg.replace(".seg", ".fst");
                let fst_path = dir.join(&fst_name);
                if fst_path.exists() {
                    match MmapFile::open(&fst_path)
                        .map_err(Error::from)
                        .and_then(|mm| {
                            fst::Map::new(mm)
                                .map_err(|e| Error::Serialize(format!("FST 字典解析失败: {e}")))
                        })
                    {
                        Ok(map) => {
                            dicts.insert(seg.clone(), Arc::new(map)); // Ex-6.3：Arc 值
                        }
                        Err(e) => {
                            info!("FST 字典加载失败，该段回退线性扫描: {fst_name}: {e}")
                        }
                    }
                }
            }
            info!("FST 字典加载: {}/{} 段", dicts.len(), segments.len());
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            engine: engine.to_string(),
            // Ex-5.2（design 4.8.3）：Term 字典分 256 shard 锁分区——低基数 Term 高并发时
            // 4 shard 下大量碰撞串行，256 shard 分散（demo 实测 1.39× 加速）；
            // dashmap 要求 shard 数为 2 的幂（256 = 2^8）。
            mem: DashMap::with_capacity_and_shard_amount(0, 256),
            stats_mem: DashMap::with_capacity_and_shard_amount(0, 256),
            io_limiter: std::sync::Mutex::new(None),
            inverted_written: AtomicU64::new(0),
            mem_docids: PerCpuCounter::new(),
            // Ex-6.2/6.3：ArcSwap 原子发布（读路径 load 拿 Arc 快照无锁）
            segments: ArcSwap::new(Arc::new(segments)),
            dicts: ArcSwap::new(Arc::new(dicts)),
            next_seg_id: AtomicU64::new(next_seg_id),
            mutate: std::sync::Mutex::new(()),
            flush_threshold,
            gc_threshold_bytes,
            delta_fst_max_bytes: 0,
            bitmap_fields: std::collections::HashSet::new(),
            bitmaps: (0..BITMAP_SHARDS)
                .map(|_| std::sync::Mutex::new(std::collections::HashMap::new()))
                .collect(),
            posting_cache: std::sync::Mutex::new(PostingLru::new(POSTING_CACHE_CAP)),
            data_files: ArcSwap::from(Arc::new(HashMap::new())),
        })
    }

    /// G 项：清空 posting 位图缓存（写路径 / 段刷盘 / GC / 位图重建时调用，保证缓存一致性）。
    fn clear_posting_cache(&self) {
        self.posting_cache.lock().unwrap().clear();
    }

    /// 当前磁盘段数。
    pub fn segment_count(&self) -> usize {
        self.segments.load().len()
    }

    /// 当前加载的 FST 术语字典数（测试 / 监控）。
    pub fn fst_dict_count(&self) -> usize {
        self.dicts.load().len()
    }
}
