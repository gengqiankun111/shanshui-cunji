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

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use lru::LruCache;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use tracing::info;
use crate::error::{Error, Result};
use crate::keys::{decode_varlen, encode_varlen};
use crate::per_cpu::PerCpuCounter;
use mmap_file::MmapFile;

/// 段文件魔数。
const SEG_MAGIC: &[u8; 8] = b"NVINV001";
/// 段文件版本：v2 = term 带字段前缀 + Roaring 紧凑 posting；v3 = posting 分块布局
/// （容器头索引 + 独立容器字节，K 项：分页/COUNT 按需延迟加载，全量解码与 v2 持平）；
/// v4 = 条目插入 `varint(段内 doc_count)`（Ex-9.1b：段级 TermMeta 计数载荷 → COUNT
/// 亚毫秒求和，免逐 docid 遍历去重；老段读取按段版本兼容回退）。
const SEG_VERSION: u16 = 4;
/// 段文件前缀。
const SEG_PREFIX: &str = "inverted-";
const MANIFEST_FILE: &str = "inverted-manifest.json";

/// 段清单（新→旧顺序）。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentManifest {
    /// 最近刷盘的段在最前。
    segments: Vec<String>,
    next_seg_id: u64,
}

/// 倒排索引：内存字典 + 磁盘段集合 + FST 术语字典。
pub struct InvertedIndex {
    dir: PathBuf,
    /// 字典引擎："hash"（纯线性）/ "fst"（FST 精确查找）。
    engine: String,
    /// term → 内存收集的 docid 列表。
    mem: DashMap<String, Vec<u64>>,
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
    /// 位图索引字段白名单（design 5.2.4，M7-2）：空 = 关闭（默认零开销）。
    bitmap_fields: std::collections::HashSet<String>,
    /// 内存位图索引（Ex-5.2 分片）：field → (value → docid RoaringBitmap)，按 field hash 分
    /// `BITMAP_SHARDS` 片锁——不同 field 并行、同 field 串行（group_by 需同 field 全量一致）。
    /// 写入时同步维护、重启重建。
    bitmaps: Vec<std::sync::Mutex<
        std::collections::HashMap<String, std::collections::HashMap<String, RoaringBitmap>>,
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
    protected: LruCache<String, Arc<RoaringBitmap>>,
    probation: LruCache<String, Arc<RoaringBitmap>>,
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
    fn get(&mut self, term: &str) -> Option<Arc<RoaringBitmap>> {
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
    fn put(&mut self, term: String, v: Arc<RoaringBitmap>) {
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
            mem_docids: PerCpuCounter::new(),
            // Ex-6.2/6.3：ArcSwap 原子发布（读路径 load 拿 Arc 快照无锁）
            segments: ArcSwap::new(Arc::new(segments)),
            dicts: ArcSwap::new(Arc::new(dicts)),
            next_seg_id: AtomicU64::new(next_seg_id),
            mutate: std::sync::Mutex::new(()),
            flush_threshold,
            gc_threshold_bytes,
            bitmap_fields: std::collections::HashSet::new(),
            bitmaps: (0..BITMAP_SHARDS)
                .map(|_| std::sync::Mutex::new(std::collections::HashMap::new()))
                .collect(),
            posting_cache: std::sync::Mutex::new(PostingLru::new(POSTING_CACHE_CAP)),
            data_files: ArcSwap::from(Arc::new(HashMap::new())),
        })
    }

    /// 配置位图索引字段白名单并**全量重建**内存位图（design 5.2.4，M7-2）。
    /// 仅当白名单非空时才维护位图（默认关闭零开销）。
    pub fn with_bitmap_fields(&mut self, fields: &[String]) -> Result<()> {
        self.bitmap_fields = fields.iter().cloned().collect();
        if self.bitmap_fields.is_empty() {
            return Ok(());
        }
        // G 项：位图重建 → posting 缓存失效（位图查询路径变化）
        self.clear_posting_cache();
        // 全量重建：清空全部分片，遍历内存 + 各段 posting，命中白名单字段的 term 建内存位图
        for m in &self.bitmaps {
            m.lock().unwrap().clear();
        }
        for (term, posting) in self.iter_terms()? {
            let Some((field, value)) = term.split_once('=') else {
                continue;
            };
            if self.bitmap_fields.contains(field) {
                self.bitmaps[bitmap_shard(field)]
                    .lock()
                    .unwrap()
                    .entry(field.to_string())
                    .or_default()
                    .insert(value.to_string(), posting);
            }
        }
        Ok(())
    }

    /// 内存位图 COUNT（design 5.2.4，M7-2）：field=value 命中白名单 → 亚毫秒计数；否则 None。
    pub fn bitmap_count(&self, field: &str, value: &str) -> Option<u64> {
        let bm = self.bitmaps[bitmap_shard(field)].lock().unwrap();
        bm.get(field)?.get(value).map(|b| b.len())
    }

    /// Ex-9.1：字段是否配置内存位图（`bitmap_fields`，写路径同步维护 → O(1) 亚毫秒计数）。
    pub fn is_bitmap_field(&self, field: &str) -> bool {
        self.bitmap_fields.contains(field)
    }

    /// 内存位图 AND（M7-2）：全部 term 命中白名单字段 → 交集位图（组合筛选快速路径）；否则 None。
    pub fn bitmap_and(&self, terms: &[&str]) -> Option<RoaringBitmap> {
        let mut acc: Option<RoaringBitmap> = None;
        for t in terms {
            let (field, value) = t.split_once('=')?;
            let bm = self.bitmaps[bitmap_shard(field)].lock().unwrap();
            let bitmap = bm.get(field)?.get(value)?;
            acc = Some(match acc {
                Some(a) => a & bitmap.clone(),
                None => bitmap.clone(),
            });
        }
        acc
    }

    /// 内存位图 GROUP BY（M7-2）：字段命中白名单 → 各值计数（按值字典序）；否则 None。
    pub fn bitmap_group_by(&self, field: &str) -> Option<Vec<(String, u64)>> {
        let bm = self.bitmaps[bitmap_shard(field)].lock().unwrap();
        let values = bm.get(field)?;
        let mut out: Vec<(String, u64)> =
            values.iter().map(|(v, b)| (v.clone(), b.len())).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Some(out)
    }

    /// G 项：清空 posting 位图缓存（写路径 / 段刷盘 / GC / 位图重建时调用，保证缓存一致性）。
    fn clear_posting_cache(&self) {
        self.posting_cache.lock().unwrap().clear();
    }

    /// 追加一个 (term, docid) 到内存字典。docid 必须 < 2^32（RoaringBitmap 上限）。
    pub fn add(&self, term: &str, docid: u64) {
        assert!(docid < u32::MAX as u64, "docid 超出 RoaringBitmap 支持范围");
        // G 项：posting 变更 → 缓存失效（写路径清空 LRU）
        self.clear_posting_cache();
        // 位图索引同步维护（design 5.2.4，M7-2）：仅当白名单非空且 term 命中字段时更新
        if !self.bitmap_fields.is_empty() {
            if let Some((field, value)) = term.split_once('=') {
                if self.bitmap_fields.contains(field) {
                    self.bitmaps[bitmap_shard(field)]
                        .lock()
                        .unwrap()
                        .entry(field.to_string())
                        .or_default()
                        .entry(value.to_string())
                        .or_default()
                        .insert(docid as u32);
                }
            }
        }
        self.mem.entry(term.to_string()).or_default().push(docid);
        self.mem_docids.add(1);
    }

    /// 批量追加 (term, docid) 集合（Ex-5.3 倒排更新批处理）：
    /// 按 term 分组合并后每 term 一次 DashMap entry + 批量 extend——同 term 多 docid
    /// 一次锁操作，省去逐条 add 的重复 hash 查找 / shard 锁 / Vec 反复 realloc；
    /// `mem_docids` 一次累加；白名单位图按 (field,value) 分组合并批量 extend。
    /// 调用方（Engine 攒批缓冲 / 批量导入）负责按写入批次聚合。
    pub fn add_batch(&self, items: &[(&str, u64)]) {
        if items.is_empty() {
            return;
        }
        // G 项：posting 变更 → 缓存失效
        self.clear_posting_cache();
        // 局部分组：同 term 合并 docid（借用 items，不拷贝 term 字符串）
        let mut groups: std::collections::HashMap<&str, Vec<u64>> =
            std::collections::HashMap::with_capacity(items.len());
        for (term, docid) in items {
            assert!(*docid < u32::MAX as u64, "docid 超出 RoaringBitmap 支持范围");
            groups.entry(term).or_default().push(*docid);
        }
        // 位图索引同步维护（design 5.2.4，M7-2）：按 (field, value) 分组合并批量 extend
        if !self.bitmap_fields.is_empty() {
            let mut bm_groups: std::collections::HashMap<(&str, &str), Vec<u64>> =
                std::collections::HashMap::new();
            for (term, docid) in items {
                if let Some((field, value)) = term.split_once('=') {
                    if self.bitmap_fields.contains(field) {
                        bm_groups.entry((field, value)).or_default().push(*docid);
                    }
                }
            }
            for ((field, value), docids) in bm_groups {
                self.bitmaps[bitmap_shard(field)]
                    .lock()
                    .unwrap()
                    .entry(field.to_string())
                    .or_default()
                    .entry(value.to_string())
                    .or_default()
                    .extend(docids.iter().map(|d| *d as u32));
            }
        }
        // 每 term 一次 entry + 批量 extend（Vec 预分配扩容一次）
        for (term, docids) in groups {
            self.mem
                .entry(term.to_string())
                .or_default()
                .extend(docids);
        }
        self.mem_docids.add(items.len() as u64);
    }

    /// 当前内存累计 posting 数（供外部决定是否刷盘）。
    pub fn mem_docids(&self) -> u64 {
        self.mem_docids.get()
    }

    /// 内存是否达阈值，需要刷盘。
    pub fn needs_flush(&self) -> bool {
        self.mem_docids() >= self.flush_threshold
    }

    /// 将内存字典整段刷盘为 `inverted-{id}.seg`，并原子更新 Manifest。
    /// engine=fst 时同时编译术语字典 `inverted-{id}.fst`（term → 段内条目偏移）。
    /// J 项（7.73）：改 `&self`（next_seg_id → AtomicU64 + mutate 锁；后台 GC 与写路径
    /// flush 并发安全）。
    pub fn flush_segment(&self) -> Result<()> {
        // J 项：与 gc 互斥（Manifest 写 / 删段文件序列化，防丢失更新）
        let _mut = self.mutate.lock().unwrap();
        if self.mem.is_empty() {
            return Ok(());
        }
        // G 项：段落盘 → posting 缓存失效
        self.clear_posting_cache();
        let seg_id = self.next_seg_id.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{SEG_PREFIX}{seg_id:08}.seg"));

        // 序列化段内容（内存快照），并记录每个 term 条目的文件偏移（FST 字典用）
        let mut body = Vec::new();
        let mut term_offsets: Vec<(Vec<u8>, u64)> = Vec::new();
        encode_varint(&mut body, self.mem.len() as u64);
        // 按 term 排序，保证段内确定性（FST 也要求 key 按字典序插入）
        let mut terms: Vec<(String, Vec<u64>)> = self
            .mem
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        terms.sort_by(|a, b| a.0.cmp(&b.0));
        for (term, docids) in terms {
            let file_offset = (SEG_MAGIC.len() + std::mem::size_of::<u16>() + body.len()) as u64;
            term_offsets.push((term.clone().into_bytes(), file_offset));
            // K 项（7.74）：v3 分块布局（分页/COUNT 按容器延迟加载）
            let bitmap: RoaringBitmap = docids.iter().map(|d| *d as u32).collect();
            // Ex-9.1b（v4）：条目 = term + varint(段内 doc_count) + posting —— 计数载荷
            // 供 COUNT 亚毫秒求和（段内 posting 为去重 docid 集合 → bitmap.len() 精确）。
            let bytes = encode_posting_v3(&bitmap);
            encode_varlen(&mut body, term.as_bytes());
            encode_varint(&mut body, bitmap.len() as u64);
            encode_varlen(&mut body, &bytes);
        }

        // 写文件：先 tmp 再 rename（原子）
        let tmp = self.dir.join(format!("{SEG_PREFIX}{seg_id:08}.seg.tmp"));
        let mut out = std::fs::File::create(&tmp)?;
        std::io::Write::write_all(&mut out, SEG_MAGIC)?;
        std::io::Write::write_all(&mut out, &SEG_VERSION.to_le_bytes())?;
        std::io::Write::write_all(&mut out, &body)?;
        out.sync_all()?;
        std::fs::rename(&tmp, &path)?;

        let fname = path.file_name().unwrap().to_string_lossy().to_string();

        // FST 术语字典（design 5.2.4.1）：term → 段内条目字节偏移，原子写
        if self.engine == "fst" {
            let map = self.write_fst_dict(seg_id, &term_offsets)?;
            // Ex-6.3：rcu 原子发布（Arc 值 → HashMap 可 Clone；闭包 FnMut 用克隆捕获）
            let map_arc = Arc::new(map);
            self.dicts.rcu(|m| {
                let mut n = (**m).clone();
                n.insert(fname.clone(), map_arc.clone());
                n
            });
        }
        // G 补充：段数据映射预注册（新段已落盘可映射；后续查询零懒加载开销）
        if let Ok(mm) = MmapFile::open(&path) {
            let mm_arc = Arc::new(mm);
            self.data_files.rcu(|m| {
                let mut n = (**m).clone();
                n.insert(fname.clone(), mm_arc.clone());
                n
            });
        }

        // 更新 Manifest（原子）
        // Ex-6.2：rcu 原子发布段清单快照
        self.segments.rcu(|v| {
            let mut n = (**v).clone();
            n.insert(0, fname.clone());
            n
        });
        self.persist_manifest()?;

        // 清空内存
        self.mem.clear();
        self.mem_docids.reset();
        info!("倒排刷盘完成: {fname}");
        Ok(())
    }

    /// 编译并写 FST 字典文件 `inverted-{id}.fst`（term → 段内条目字节偏移，字典序），
    /// 返回 mmap 只读字典（Ex-5.7，供本实例即时使用，无需重启；零堆分配按需加载）。
    fn write_fst_dict(
        &self,
        seg_id: u64,
        term_offsets: &[(Vec<u8>, u64)],
    ) -> Result<fst::Map<MmapFile>> {
        let fst_path = self.dir.join(format!("{SEG_PREFIX}{seg_id:08}.fst"));
        let tmp = self.dir.join(format!("{SEG_PREFIX}{seg_id:08}.fst.tmp"));
        {
            let file = std::fs::File::create(&tmp)?;
            {
                let mut w = std::io::BufWriter::new(&file);
                let mut builder = fst::MapBuilder::new(&mut w)
                    .map_err(|e| Error::Serialize(format!("FST 构建失败: {e}")))?;
                for (term, offset) in term_offsets {
                    builder
                        .insert(term, *offset)
                        .map_err(|e| Error::Serialize(format!("FST 写入失败: {e}")))?;
                }
                builder
                    .finish()
                    .map_err(|e| Error::Serialize(format!("FST 完成失败: {e}")))?;
                w.flush()?;
            }
            // 写句柄上 fsync（Windows FlushFileBuffers 需写权限；只读句柄会 PermissionDenied）
            file.sync_all()?;
        }
        // 原子改名（先改名再 mmap：Windows 下已映射文件无法 rename）
        std::fs::rename(&tmp, &fst_path)?;
        let mm = MmapFile::open(&fst_path)?;
        fst::Map::new(mm).map_err(|e| Error::Serialize(format!("FST 字典解析失败: {e}")))
    }

    fn persist_manifest(&self) -> Result<()> {
        let segs = self.segments.load(); // Ex-6.2：快照
        let m = SegmentManifest {
            segments: segs.as_ref().clone(),
            next_seg_id: self.next_seg_id.load(Ordering::Relaxed),
        };
        let text = serde_json::to_string_pretty(&m)
            .map_err(|e| Error::Serialize(format!("倒排 Manifest 序列化失败: {e}")))?;
        let tmp = self.dir.join("manifest.json.tmp");
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, self.dir.join(MANIFEST_FILE))?;
        Ok(())
    }

    /// 查询 term：合并内存 posting 与各段 posting，返回 RoaringBitmap（docid 按 u32 语义）。
    /// G 项优化（design_extension 9.6）：① 白名单字段 term 直接返回全量内存位图（O(1)）；
    /// ② 非白名单 term 查 LRU 缓存（重复查询免段遍历 + posting 反序列化）。
    pub fn search(&self, term: &str) -> Result<RoaringBitmap> {
        // ① 白名单字段 term → 内存位图（写路径同步维护，含已落盘段全量）
        if let Some((field, value)) = term.split_once('=') {
            if self.bitmap_fields.contains(field) {
                if let Some(b) = self.bitmaps[bitmap_shard(field)]
                    .lock()
                    .unwrap()
                    .get(field)
                    .and_then(|m| m.get(value))
                {
                    return Ok(b.clone());
                }
            }
        }
        // ② LRU 缓存命中（Ex-8.8 双区；RoaringBitmap 浅拷贝返回）
        if let Some(cached) = self.posting_cache.lock().unwrap().get(term) {
            return Ok(cached.as_ref().clone());
        }
        let mut result = RoaringBitmap::new();
        // 内存（最新）
        if let Some(docids) = self.mem.get(term) {
            result.extend(docids.iter().map(|d| *d as u32));
        }
        // 各段（新→旧，bitmap 合并天然去重）
        let segs = self.segments.load(); // Ex-6.2：读快照无锁
        for seg in segs.iter() {
            let posting = self.read_segment_posting(seg, term)?;
            result |= posting;
        }
        // ③ 未命中 → 段遍历反序列化后入缓存（下次同 term O(1)）
        self.posting_cache
            .lock()
            .unwrap()
            .put(term.to_string(), Arc::new(result.clone()));
        Ok(result)
    }

    /// 段文件版本（魔数后 u16；头部不足返回 0）。
    fn seg_ver(data: &[u8]) -> u16 {
        if data.len() < 10 {
            0
        } else {
            u16::from_le_bytes(data[8..10].try_into().unwrap())
        }
    }

    /// 读取某段内 term 的 posting（未命中返回空 bitmap）。
    /// FST 字典存在时 O(len(term)) 精确定位（design 5.2.4.1）；旧段回退线性扫描。
    /// G 补充：段数据 mmap 化——首次访问懒加载注册，后续按 FST offset 直接切片，
    /// 免 `fs::read` 全文件读取 + 堆复制（大段文件未命中查询的主要 IO 成本）。
    fn read_segment_posting(&self, seg: &str, term: &str) -> Result<RoaringBitmap> {
        let data = {
            let files = self.data_files.load();
            match files.get(seg) {
                Some(m) => m.clone(),
                None => {
                    drop(files);
                    // J 项（7.73）：后台 GC 与查询并发——查询持旧段快照时 gc 可能已删该段
                    // 文件（数据已合并进新段）。文件不存在 = 快照过期 → 跳过该段返回空
                    // （与 ArcSwap 快照语义一致：读到 gc 前/后快照结果一致），其他 IO 错误
                    // （真损坏）照常传播。
                    let mm = match MmapFile::open(&self.dir.join(seg)) {
                        Ok(m) => Arc::new(m),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            return Ok(RoaringBitmap::new())
                        }
                        Err(e) => return Err(Error::from(e)),
                    };
                    // rcu 发布（&self 可原子更新）：并发查询下重复注册无害（幂等覆盖）
                    self.data_files.rcu(|m| {
                        let mut n = (**m).clone();
                        n.insert(seg.to_string(), mm.clone());
                        n
                    });
                    mm
                }
            }
        };
        if data.len() < 10 || &data[0..8] != SEG_MAGIC {
            return Err(Error::Corrupted(format!("倒排段魔数错误: {seg}")));
        }
        // K 项（7.74）：段版本 → v3 分块 / v2 紧凑（旧段兼容）；Ex-9.1b v4 多 skip 计数载荷
        let ver = Self::seg_ver(&data);
        // FST 精确查找：term → 段内条目字节偏移
        let dicts = self.dicts.load(); // Ex-6.3：Arc 快照零拷贝
        if let Some(map) = dicts.get(seg) {
            return match map.get(term.as_bytes()) {
                Some(offset) => parse_posting_at(&data, offset as usize, ver),
                None => Ok(RoaringBitmap::new()),
            };
        }
        // 回退线性扫描（无 FST 的旧段 / hash 引擎）
        let mut cur = 10usize;
        let count = decode_varint(&data, &mut cur)?;
        for _ in 0..count {
            let t = decode_varlen(&data, &mut cur)?.to_vec();
            if ver >= 4 {
                let _c = decode_varint(&data, &mut cur)?; // 跳过 v4 doc_count 载荷
            }
            let p = decode_varlen(&data, &mut cur)?.to_vec();
            if t.as_slice() == term.as_bytes() {
                return if ver >= 3 {
                    decode_posting_v3(&p)
                } else {
                    RoaringBitmap::deserialize_from(&p[..])
                        .map_err(|e| Error::Corrupted(format!("posting 反序列化失败: {e}")))
                };
            }
        }
        Ok(RoaringBitmap::new())
    }

    /// 定位某 term 在某段内的条目起点（FST offset / 线性扫描），并返回段数据映射。
    /// 段文件不存在（后台 GC 已删，J 项）→ None。供 doc_count / search_paged 快速路径用。
    fn segment_posting_entry(
        &self,
        seg: &str,
        term: &str,
    ) -> Result<Option<(Arc<MmapFile>, usize)>> {
        let files = self.data_files.load();
        let data = if let Some(m) = files.get(seg) {
            m.clone()
        } else {
            drop(files);
            let mm = match MmapFile::open(&self.dir.join(seg)) {
                Ok(m) => Arc::new(m),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(Error::from(e)),
            };
            self.data_files.rcu(|m| {
                let mut n = (**m).clone();
                n.insert(seg.to_string(), mm.clone());
                n
            });
            mm
        };
        if data.len() < 10 || &data[0..8] != SEG_MAGIC {
            return Err(Error::Corrupted(format!("倒排段魔数错误: {seg}")));
        }
        // FST 精确查找：term → 段内条目字节偏移
        let dicts = self.dicts.load();
        if let Some(map) = dicts.get(seg) {
            return Ok(map.get(term.as_bytes()).map(|o| (data, o as usize)));
        }
        // 回退线性扫描（无 FST 的旧段 / hash 引擎）
        let mut cur = 10usize;
        let count = decode_varint(&data, &mut cur)?;
        for _ in 0..count {
            let entry = cur; // 条目起点（term varlen 前）
            let t = decode_varlen(&data, &mut cur)?.to_vec();
            if Self::seg_ver(&data) >= 4 {
                let _c = decode_varint(&data, &mut cur)?; // Ex-9.1b：跳过 v4 doc_count 载荷
            }
            let _p = decode_varlen(&data, &mut cur)?;
            if t.as_slice() == term.as_bytes() {
                return Ok(Some((data, entry)));
            }
        }
        Ok(None)
    }

    /// 当前磁盘段数。
    pub fn segment_count(&self) -> usize {
        self.segments.load().len()
    }

    /// 当前加载的 FST 术语字典数（测试 / 监控）。
    pub fn fst_dict_count(&self) -> usize {
        self.dicts.load().len()
    }

    /// 某 term 命中的文档数（COUNT 原子操作，design 5.17）。
    /// K 项（7.74）：惰性游标归并**精确去重**计数（跨段/内存重复 docid 合并）——
    /// 容器级按需解码，不收集 docid 列表。
    pub fn doc_count(&self, term: &str) -> Result<u64> {
        let mem_vals: Vec<u32> = self
            .mem
            .get(term)
            .map(|e| e.value().iter().map(|&d| d as u32).collect())
            .unwrap_or_default();
        let mut cursors: Vec<PostingCursor> = Vec::new();
        let segs = self.segments.load();
        for seg in segs.iter() {
            if let Some((data, entry)) = self.segment_posting_entry(seg, term)? {
                let ver = Self::seg_ver(&data);
                if ver >= 3 {
                    cursors.push(PostingCursor::new(&data, entry, ver)?);
                } else {
                    let bm = parse_posting_at(&data, entry, ver)?;
                    cursors.push(PostingCursor::from_bitmap(bm));
                }
            }
        }
        let mut count = 0u64;
        merge_distinct(&mem_vals, &mut cursors, |_| {
            count += 1;
            false
        })?;
        Ok(count)
    }

    /// Ex-9.1b：段级 TermMeta 计数载荷快速 COUNT——flush 时在每 term 条目写入
    /// `varint(段内 doc_count)`（段内 posting 为去重 docid 集合 → bitmap.len() 精确）；
    /// 此处 mem 去重计数 + 各段载荷求和 = O(段数)，免逐 docid 遍历（亚毫秒）。
    /// 前提：**全部命中段均为 v4**（含载荷）；任一段为老格式（v2/v3）→ Ok(None)，
    /// 调用方回退 `doc_count` 精确遍历。语义注：跨段重复 docid（同 docid 同 term 覆盖写入
    /// 分散多段）求和略高估——写入单调/后台 GC 收敛后段间无重叠即精确；覆盖写入高频场景
    /// 走 `doc_count`（精确去重）。
    pub fn doc_count_fast(&self, term: &str) -> Result<Option<u64>> {
        // mem（未刷盘）部分：term value 去重计数（内存小，HashSet 一次）
        let mut total = 0u64;
        if let Some(e) = self.mem.get(term) {
            let set: std::collections::HashSet<u64> = e.value().iter().copied().collect();
            total += set.len() as u64;
        }
        let segs = self.segments.load();
        for seg in segs.iter() {
            let Some((data, entry)) = self.segment_posting_entry(seg, term)? else {
                continue; // gc 并发删段：跳过（与 doc_count 一致）
            };
            let ver = Self::seg_ver(&data);
            if ver < 4 {
                return Ok(None); // 老段无计数载荷 → 整体回退精确遍历
            }
            // FST/linear entry 指向 term 起点：skip term → varint(doc_count)
            let mut cur = entry;
            let _t = decode_varlen(&data, &mut cur)?;
            total += decode_varint(&data, &mut cur)?;
        }
        Ok(Some(total))
    }

    /// 分页快速路径（K 项）：跨内存 + 各段 k-way merge，只解码 [offset, offset+limit)
    /// 覆盖的容器——大 posting（1600 万 docid）近页从全量反序列化（~3ms）降至窗口解码
    /// （~10µs，demo posting-chunk x211）。返回 (total, 窗口 docid 升序列表，已去重)。
    /// total 为各源头部基数之和（跨段重复 docid 未去重时为上界；后台 GC 收敛后精确）。
    pub fn search_paged(&self, term: &str, offset: u64, limit: u64) -> Result<(u64, Vec<u32>)> {
        // 内存 posting（小，升序）
        let mem_vals: Vec<u32> = self
            .mem
            .get(term)
            .map(|e| e.value().iter().map(|&d| d as u32).collect())
            .unwrap_or_default();
        let mut total = mem_vals.len() as u64;
        // 各段：v3 → 惰性游标；v2 旧段 → 全量解码包游标（兼容）
        let segs = self.segments.load();
        let mut cursors: Vec<PostingCursor> = Vec::new();
        for seg in segs.iter() {
            if let Some((data, entry)) = self.segment_posting_entry(seg, term)? {
                let ver = Self::seg_ver(&data);
                if ver >= 3 {
                    let c = PostingCursor::new(&data, entry, ver)?;
                    total += c.total();
                    cursors.push(c);
                } else {
                    let bm = parse_posting_at(&data, entry, ver)?;
                    total += bm.len();
                    cursors.push(PostingCursor::from_bitmap(bm));
                }
            }
        }
        if limit == 0 {
            return Ok((total, Vec::new()));
        }
        // 归并取窗口（去重后流，与 search 的 bitmap 语义一致）
        let mut out: Vec<u32> = Vec::new();
        let mut skipped = 0u64;
        merge_distinct(&mem_vals, &mut cursors, |docid| {
            if skipped < offset {
                skipped += 1;
                false
            } else {
                out.push(docid);
                out.len() as u64 >= limit
            }
        })?;
        Ok((total, out))
    }

    /// 遍历全部 term（内存 + 各段），合并出每个 term 的完整 posting 位图。
    /// 供聚合执行器（GROUP BY）与字典浏览使用；内存 term 合并天然去重。
    pub fn iter_terms(&self) -> Result<Vec<(String, RoaringBitmap)>> {
        let mut map: std::collections::BTreeMap<String, RoaringBitmap> =
            std::collections::BTreeMap::new();
        // 内存（最新）
        for entry in self.mem.iter() {
            let bitmap: RoaringBitmap = entry.value().iter().map(|d| *d as u32).collect();
            let e = map.entry(entry.key().clone()).or_default();
            *e |= bitmap;
        }
        // 各段（新→旧）
        let segs = self.segments.load(); // Ex-6.2：读快照无锁
        for seg in segs.iter() {
            for (term, posting) in self.read_segment_terms(seg)? {
                let e = map.entry(term).or_default();
                *e |= posting;
            }
        }
        Ok(map.into_iter().collect())
    }

    /// 读取段内全部 term 及其 posting（供遍历）。
    fn read_segment_terms(&self, seg: &str) -> Result<Vec<(String, RoaringBitmap)>> {
        let path = self.dir.join(seg);
        let data = std::fs::read(&path)?;
        if data.len() < 10 || &data[0..8] != SEG_MAGIC {
            return Err(Error::Corrupted(format!("倒排段魔数错误: {seg}")));
        }
        // K 项（7.74）：段版本 → v3 分块 / v2 紧凑（旧段兼容）；Ex-9.1b v4 多 skip 计数载荷
        let ver = Self::seg_ver(&data);
        let mut cur = 10usize;
        let count = decode_varint(&data, &mut cur)?;
        let mut out = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let t = decode_varlen(&data, &mut cur)?.to_vec();
            if ver >= 4 {
                let _c = decode_varint(&data, &mut cur)?; // v4 doc_count 载荷
            }
            let p = decode_varlen(&data, &mut cur)?.to_vec();
            let bitmap = if ver >= 3 {
                decode_posting_v3(&p)?
            } else {
                RoaringBitmap::deserialize_from(&p[..])
                    .map_err(|e| Error::Corrupted(format!("posting 反序列化失败: {e}")))?
            };
            out.push((String::from_utf8_lossy(&t).into_owned(), bitmap));
        }
        Ok(out)
    }

    /// 按字段前缀分组（GROUP BY）：遍历全部 term，取 `{field}=` 开头的各 value 及其 doc_count。
    pub fn group_by(&self, field: &str) -> Result<Vec<(String, u64)>> {
        let prefix = format!("{field}=");
        let mut out = Vec::new();
        for (term, posting) in self.iter_terms()? {
            if term.starts_with(&prefix) {
                out.push((term, posting.len()));
            }
        }
        Ok(out)
    }

    // ============ 预分片 Chunk（design 5.2.1，阶段 2）============

    /// 取 term 完整 posting 中**属于指定分片**的那段 Chunk Bitmap。
    /// `shard_count` 为（虚拟）分片数，`shard_id ∈ [0, shard_count)`。
    /// 分片一致性：与 `sharding::route` 共用 `hash64`（`virtual_shard = hash64(docid) % shard_count`）。
    ///
    /// 广播查询流程（design 9.2）：网关给每个分片只要求本片 Chunk → 各片本地算本片 DocId →
    /// 网关 `concatenate_chunks` 按序直拼（O(1)），无需跨片交集/并集。
    pub fn chunk_for_shard(
        &self,
        term: &str,
        shard_id: u32,
        shard_count: u32,
    ) -> Result<RoaringBitmap> {
        assert!(shard_count > 0, "shard_count 必须 > 0");
        assert!(shard_id < shard_count, "shard_id 越界");
        let full = self.search(term)?;
        let mut chunk = RoaringBitmap::new();
        for d in full.iter() {
            let vs = (crate::sharding::hash64(d as u64) % shard_count as u64) as u32;
            if vs == shard_id {
                chunk.insert(d);
            }
        }
        Ok(chunk)
    }

    /// 网关侧按序直拼（design 5.2.1）：各分片 Chunk 互不相交，顺序 OR 即拼接（O(1) 合并开销）。
    pub fn concatenate_chunks(chunks: &[RoaringBitmap]) -> RoaringBitmap {
        let mut out = RoaringBitmap::new();
        for c in chunks {
            out |= c.clone();
        }
        out
    }

    // ============ 倒排段 GC / Compaction（design 5.2.2 + 5.2.4⑤，阶段 2）============

    /// 全部磁盘段文件总字节数。
    pub fn segment_bytes(&self) -> u64 {
        let segs = self.segments.load(); // Ex-6.2：快照
        segs.iter()
            .filter_map(|s| std::fs::metadata(self.dir.join(s)).ok().map(|m| m.len()))
            .sum()
    }

    /// 是否需要 GC：开启（阈值 > 0）且段数 > 1 且总量 ≥ 阈值。
    pub fn should_gc(&self) -> bool {
        self.gc_threshold_bytes > 0
            && self.segments.load().len() > 1
            && self.segment_bytes() >= self.gc_threshold_bytes
    }

    /// 倒排文件 GC（design 5.2.2）：将全部段读取所有 Term 的最新 Bitmap，
    /// **重写为单个紧凑段**（临时文件 → fsync → 原子更新 Manifest → 删除旧段 + 旧 FST），
    /// 中途崩溃不丢数据（启动只加载 Manifest 中的段，孤儿段被忽略）。
    /// J 项（7.73）：改 `&self` + mutate 锁——后台 GC 线程与写路径 flush 并发安全
    /// （flush/gc 写 Manifest 与删文件互斥，防丢失更新）。
    pub fn gc(&self) -> Result<GcReport> {
        // J 项：与 flush_segment 互斥（Manifest 写 / 删段文件序列化）
        let _mut = self.mutate.lock().unwrap();
        if !self.should_gc() {
            return Ok(GcReport {
                merged: 0,
                freed_bytes: 0,
                segment_count: self.segments.load().len(),
            });
        }
        // G 项：段合并 → posting 缓存失效（旧段 bitmap 过期）
        self.clear_posting_cache();
        // ① 读取全部段的所有 term 最新 posting（bitmap 合并天然去重）
        let mut map: std::collections::BTreeMap<String, RoaringBitmap> =
            std::collections::BTreeMap::new();
        let segs = self.segments.load(); // Ex-6.2：快照
        for seg in segs.iter() {
            for (term, posting) in self.read_segment_terms(seg)? {
                let e = map.entry(term).or_default();
                *e |= posting;
            }
        }

        // ② 写新段（临时文件 → fsync → 原子 rename）
        let seg_id = self.next_seg_id.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{SEG_PREFIX}{seg_id:08}.seg"));
        let mut body = Vec::new();
        let mut term_offsets: Vec<(Vec<u8>, u64)> = Vec::new();
        encode_varint(&mut body, map.len() as u64);
        for (term, bitmap) in &map {
            let file_offset = (SEG_MAGIC.len() + std::mem::size_of::<u16>() + body.len()) as u64;
            term_offsets.push((term.clone().into_bytes(), file_offset));
            // K 项（7.74）：v3 分块布局；Ex-9.1b（v4）：term + varint(段内 doc_count) + posting
            let bytes = encode_posting_v3(bitmap);
            encode_varlen(&mut body, term.as_bytes());
            encode_varint(&mut body, bitmap.len() as u64);
            encode_varlen(&mut body, &bytes);
        }
        let tmp = self.dir.join(format!("{SEG_PREFIX}{seg_id:08}.seg.tmp"));
        let mut out = std::fs::File::create(&tmp)?;
        std::io::Write::write_all(&mut out, SEG_MAGIC)?;
        std::io::Write::write_all(&mut out, &SEG_VERSION.to_le_bytes())?;
        std::io::Write::write_all(&mut out, &body)?;
        out.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        let fname = path.file_name().unwrap().to_string_lossy().to_string();

        // ③ 编译新段 FST 字典（写临时文件 → rename）
        let new_dict = if self.engine == "fst" {
            Some(self.write_fst_dict(seg_id, &term_offsets)?)
        } else {
            None
        };

        // ④ 原子更新 Manifest（先 Manifest 后删旧文件，崩溃安全）
        let old_segments = self.segments.load_full(); // Ex-6.2：旧快照（删旧文件依据）
        let old_bytes: u64 = old_segments
            .iter()
            .filter_map(|s| std::fs::metadata(self.dir.join(s)).ok().map(|m| m.len()))
            .sum();
        // Ex-6.2/6.3：store 发布新段清单 + 新字典（释放旧映射 → Windows 可删旧文件）
        self.segments.store(Arc::new(vec![fname.clone()]));
        let mut new_dicts: HashMap<String, Arc<fst::Map<MmapFile>>> = HashMap::new();
        if let Some(m) = new_dict {
            new_dicts.insert(fname.clone(), Arc::new(m));
        }
        self.dicts.store(Arc::new(new_dicts));
        // G 补充：段数据映射同步替换（先发布新映射再删旧文件——Windows 已映射不可删）
        let mut new_files: HashMap<String, Arc<MmapFile>> = HashMap::new();
        if let Ok(mm) = MmapFile::open(&self.dir.join(&fname)) {
            new_files.insert(fname.clone(), Arc::new(mm));
        }
        self.data_files.store(Arc::new(new_files));
        self.persist_manifest()?;

        // ⑤ 删旧段与旧 FST（Ex-5.7：已发布新快照后旧映射被释放）
        for seg in old_segments.iter() {
            let _ = std::fs::remove_file(self.dir.join(seg));
            let _ = std::fs::remove_file(self.dir.join(seg.replace(".seg", ".fst")));
        }

        let freed_bytes = old_bytes.saturating_sub(self.segment_bytes());
        info!(
            "倒排 GC 完成: {} 段合并为 1（释放 {} 字节）",
            old_segments.len(),
            freed_bytes
        );
        Ok(GcReport {
            merged: old_segments.len(),
            freed_bytes,
            segment_count: 1,
        })
    }
}

/// 倒排 GC 结果报告。
#[derive(Debug, Clone, Copy)]
pub struct GcReport {
    /// 被合并的旧段数。
    pub merged: usize,
    /// 释放的磁盘字节数。
    pub freed_bytes: u64,
    /// GC 后的段数。
    pub segment_count: usize,
}

/// 从段数据指定偏移解析 (term, posting) 条目（FST 字典指向的条目）。
/// v3 = posting 分块布局（K 项）；v2 = Roaring 紧凑字节（旧段兼容）。
fn parse_posting_at(data: &[u8], offset: usize, ver: u16) -> Result<RoaringBitmap> {
    let mut cur = offset;
    let _t = decode_varlen(data, &mut cur)?; // 跳过 term
    if ver >= 4 {
        let _c = decode_varint(data, &mut cur)?; // Ex-9.1b：跳过 v4 doc_count 载荷
    }
    let p = decode_varlen(data, &mut cur)?.to_vec();
    if ver >= 3 {
        decode_posting_v3(&p)
    } else {
        RoaringBitmap::deserialize_from(&p[..])
            .map_err(|e| Error::Corrupted(format!("posting 反序列化失败: {e}")))
    }
}

// ---------- K 项（7.74）：v3 posting 分块布局（按容器延迟加载） ----------
//
// v3 条目 payload：`[u32 容器数][每容器 14B 头：high u16 + card u32 + off u32 + len u32][容器数据区]`。
// 每个容器 = Roaring 单容器 bitmap（值 = **完整 docid**，容器级对齐 → OR 合并零额外成本）。
// 分页 / COUNT 只反序列化窗口覆盖的容器（近页 x211、COUNT x4491；全量解码与 v2 紧凑持平，demo posting-chunk）。

/// v3 容器头。
#[derive(Clone, Copy)]
struct ChunkHeader {
    high: u16,
    card: u64,
    off: usize,
    len: usize,
}

/// 解析 v3 容器头（不复制数据区）。返回（头数组, 头区总字节数）。
fn v3_headers(payload: &[u8]) -> Result<(Vec<ChunkHeader>, usize)> {
    if payload.len() < 4 {
        return Err(Error::Corrupted("v3 posting 头过短".into()));
    }
    let nc = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let hdr_bytes = 4 + 14 * nc;
    if payload.len() < hdr_bytes {
        return Err(Error::Corrupted("v3 posting 容器头截断".into()));
    }
    let mut hs = Vec::with_capacity(nc);
    for i in 0..nc {
        let b = 4 + i * 14;
        hs.push(ChunkHeader {
            high: u16::from_le_bytes(payload[b..b + 2].try_into().unwrap()),
            card: u32::from_le_bytes(payload[b + 2..b + 6].try_into().unwrap()) as u64,
            off: u32::from_le_bytes(payload[b + 6..b + 10].try_into().unwrap()) as usize,
            len: u32::from_le_bytes(payload[b + 10..b + 14].try_into().unwrap()) as usize,
        });
    }
    Ok((hs, hdr_bytes))
}

/// v3 编码：RoaringBitmap → 分块 payload（flush / gc 写路径用）。
fn encode_posting_v3(bm: &RoaringBitmap) -> Vec<u8> {
    // 按高 16 位分容器（bitmap 升序迭代 → 容器天然按 high 升序）
    let mut by_high: Vec<(u16, Vec<u32>)> = Vec::new();
    for d in bm.iter() {
        let high = (d >> 16) as u16;
        match by_high.last_mut() {
            Some((h, v)) if *h == high => v.push(d),
            _ => by_high.push((high, vec![d])),
        }
    }
    // 各容器序列化为 Roaring 单容器字节（值 = 完整 docid → 容器级对齐）
    let mut bodies: Vec<(u16, u64, Vec<u8>)> = Vec::new();
    for (high, vals) in by_high {
        let c: RoaringBitmap = vals.into_iter().collect();
        let mut bytes = Vec::new();
        c.serialize_into(&mut bytes).unwrap();
        bodies.push((high, c.len(), bytes));
    }
    let mut out = Vec::new();
    out.extend((bodies.len() as u32).to_le_bytes());
    let mut data_off = 4 + 14 * bodies.len();
    for (high, card, bytes) in &bodies {
        out.extend(high.to_le_bytes());
        out.extend((*card as u32).to_le_bytes());
        out.extend((data_off as u32).to_le_bytes());
        out.extend((bytes.len() as u32).to_le_bytes());
        data_off += bytes.len();
    }
    for (_, _, bytes) in &bodies {
        out.extend_from_slice(bytes);
    }
    out
}

/// v3 全量解码（search / 合并场景；容器级 OR 与 v2 紧凑反序列化持平）。
fn decode_posting_v3(payload: &[u8]) -> Result<RoaringBitmap> {
    let (hs, _) = v3_headers(payload)?;
    let mut result = RoaringBitmap::new();
    for h in hs {
        let end = h.off + h.len;
        if payload.len() < end {
            return Err(Error::Corrupted("v3 posting 容器数据截断".into()));
        }
        let c = RoaringBitmap::deserialize_from(&payload[h.off..end])
            .map_err(|e| Error::Corrupted(format!("v3 容器反序列化失败: {e}")))?;
        result |= c;
    }
    Ok(result)
}

/// v3 惰性游标（分页 k-way merge 用）：容器级按需解码，逐 docid 产出。
/// 持有段数据 `Arc<MmapFile>`（零拷贝）——容器数据在**数据区**按需反序列化。
struct PostingCursor {
    /// 段数据映射；`None` = v2 旧段全量解码后的内存游标（from_bitmap）。
    data: Option<Arc<MmapFile>>,
    /// 条目 payload 起点（相对段文件头）。
    payload_off: usize,
    headers: Vec<ChunkHeader>,
    idx: usize,
    cur: Option<(Vec<u32>, usize)>, // (当前容器值列表, 位置)
}

impl PostingCursor {
    /// 从条目 offset（FST 指向）构造：跳过 varlen term（+ v4 doc_count）→ payload → 容器头。
    fn new(data: &Arc<MmapFile>, entry: usize, ver: u16) -> Result<Self> {
        let mut cur = entry;
        let _t = decode_varlen(data, &mut cur)?; // 跳过 term（pos → term 末尾）
        if ver >= 4 {
            let _c = decode_varint(data, &mut cur)?; // Ex-9.1b：跳过 v4 doc_count 载荷
        }
        let payload = decode_varlen(data, &mut cur)?; // payload 切片（pos → payload 末尾）
        let (headers, _) = v3_headers(payload)?;
        Ok(Self {
            data: Some(data.clone()),
            payload_off: cur - payload.len(),
            headers,
            idx: 0,
            cur: None,
        })
    }

    /// 从已解码 bitmap 构造（v2 旧段兼容）：全部 docid 作为单个"容器"，无段数据。
    fn from_bitmap(bm: RoaringBitmap) -> Self {
        let vals: Vec<u32> = bm.iter().collect();
        Self {
            data: None,
            payload_off: 0,
            headers: Vec::new(),
            idx: 0,
            cur: Some((vals, 0)),
        }
    }

    /// 该段内该 term 的 posting 总数（头部基数求和）。
    fn total(&self) -> u64 {
        self.headers.iter().map(|h| h.card).sum()
    }

    /// 下一个 docid（跨容器推进时解码）。None = 耗尽。
    fn next_docid(&mut self) -> Result<Option<u32>> {
        loop {
            if let Some((vals, pos)) = &mut self.cur {
                if *pos < vals.len() {
                    let v = vals[*pos];
                    *pos += 1;
                    return Ok(Some(v));
                }
            }
            if self.idx >= self.headers.len() {
                return Ok(None);
            }
            let h = self.headers[self.idx];
            self.idx += 1;
            let data = self.data.as_ref().expect("段游标必有数据");
            let start = self.payload_off + h.off;
            let end = start + h.len;
            if data.len() < end {
                return Err(Error::Corrupted("v3 posting 容器数据截断".into()));
            }
            let c = RoaringBitmap::deserialize_from(&data[start..end])
                .map_err(|e| Error::Corrupted(format!("v3 容器反序列化失败: {e}")))?;
            self.cur = Some((c.iter().collect(), 0));
        }
    }
}

/// 多源升序归并（K 项，分页/COUNT 共用）：内存 vals + 各段惰性游标，对每个**去重后**的
/// docid 调 `f(docid)`；`f` 返回 true 时停止（提前退出）。各源 docid 均升序。
fn merge_distinct(
    mem_vals: &[u32],
    cursors: &mut [PostingCursor],
    mut f: impl FnMut(u32) -> bool,
) -> Result<()> {
    let mut mem_next = mem_vals.first().copied();
    let mut mem_pos = 0usize;
    let mut nxt: Vec<Option<u32>> = Vec::with_capacity(cursors.len());
    for c in cursors.iter_mut() {
        nxt.push(c.next_docid()?);
    }
    let mut last = u32::MAX;
    loop {
        let mut best: Option<(u32, usize)> = None; // (值, 源；usize::MAX = 内存)
        if let Some(v) = mem_next {
            if best.map_or(true, |(b, _)| v < b) {
                best = Some((v, usize::MAX));
            }
        }
        for (i, v) in nxt.iter().enumerate() {
            if let Some(x) = v {
                if best.map_or(true, |(b, _)| *x < b) {
                    best = Some((*x, i));
                }
            }
        }
        let Some((val, src)) = best else { break };
        if val != last {
            last = val;
            if f(val) {
                return Ok(());
            }
        }
        // 推进源（重复 docid 也推进）
        if src == usize::MAX {
            mem_pos += 1;
            mem_next = mem_vals.get(mem_pos).copied();
        } else {
            nxt[src] = cursors[src].next_docid()?;
        }
    }
    Ok(())
}

/// 解码段文件计数 / term 条目数（LEB128）。
fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64> {
    crate::keys::decode_varint(data, pos)
}

fn encode_varint(buf: &mut Vec<u8>, n: u64) {
    crate::keys::encode_varint(buf, n);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;

    #[test]
    fn posting_lru_dual_zone_protects_hot_terms() {
        // Ex-8.8：双区 LRU——命中提升进 protected 后，低频 term 突发不再逐出热点；
        // 冷 term 的缓存仍被有界约束（总量不超预算）
        let mut lru = PostingLru::new(20); // protected 12 + probation 8
        let bm = |n: u32| {
            let mut b = RoaringBitmap::new();
            b.insert(n);
            b
        };
        // 热点 term：入缓存并命中一次 → 提升 protected
        lru.put("hot".to_string(), Arc::new(bm(1)));
        assert!(lru.get("hot").is_some(), "首次 probation 命中应提升");
        assert!(lru.get("hot").is_some(), "protected 直返");
        // 低频突发 40 个冷 term（总量远超预算，均为 miss 入缓存不命中）→ 只驱逐 probation 冷项
        for i in 0..40u32 {
            let t = format!("cold-{i}");
            lru.put(t, Arc::new(bm(i + 10)));
        }
        assert!(
            lru.get("hot").is_some(),
            "protected 热点不应被冷 term 突发逐出"
        );
        // 有界性：protected ≤ 12 且 probation ≤ 8（总项数受 20 预算约束）
        assert!(lru.protected.len() <= 12);
        assert!(lru.probation.len() <= 8);
        // 最老冷项已被逐出（缓存有界生效）
        assert!(lru.get("cold-0").is_none() || lru.get("cold-39").is_none());
        lru.clear();
        assert!(lru.get("hot").is_none(), "写路径清空应双区全清");
    }

    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let name = format!("inv-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    #[test]
    fn add_and_search_in_memory() {
        let idx = InvertedIndex::open(&tmp(), 10_000).unwrap();
        idx.add("rust", 1);
        idx.add("rust", 3);
        idx.add("rust", 5);
        idx.add("go", 2);
        let r = idx.search("rust").unwrap();
        assert!(r.contains(1) && r.contains(3) && r.contains(5));
        assert!(!r.contains(2));
        assert!(idx.search("go").unwrap().contains(2));
        assert!(idx.search("absent").unwrap().is_empty());
    }

    // ---------- G 补充：段数据 mmap 化（只读新段免全文件读取） ----------

    #[test]
    fn segment_data_mmap_registered_on_flush_and_queryable() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 10_000).unwrap();
        for d in 1..=50u64 {
            idx.add("term-a", d);
        }
        idx.flush_segment().unwrap();
        // flush 预注册 mmap：data_files 含新段
        let seg = idx.segments.load()[0].clone();
        assert!(
            idx.data_files.load().contains_key(&seg),
            "flush 后应预注册段数据 mmap"
        );
        // 查询命中（mmap 切片反序列化路径）
        let r = idx.search("term-a").unwrap();
        assert_eq!(r.len(), 50);
        // 未命中 term 走 FST None 分支
        assert!(idx.search("absent-term").unwrap().is_empty());
    }

    #[test]
    fn segment_data_mmap_lazy_load_after_reopen() {
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open(&dir, 10_000).unwrap();
            for d in 1..=30u64 {
                idx.add("lazy", d);
            }
            idx.flush_segment().unwrap();
        }
        // 重开：data_files 空（运行期缓存不持久化），首次查询懒加载注册
        let idx = InvertedIndex::open(&dir, 10_000).unwrap();
        assert!(idx.data_files.load().is_empty(), "重开应懒加载");
        let r = idx.search("lazy").unwrap();
        assert_eq!(r.len(), 30);
        assert!(!idx.data_files.load().is_empty(), "首次查询后应注册 mmap");
    }

    #[test]
    fn segment_data_mmap_survives_gc() {
        let dir = tmp();
        let mut idx = InvertedIndex::open_with_gc(&dir, 10_000, "fst", 1).unwrap();
        for d in 1..=100u64 {
            idx.add("gc-a", d);
        }
        idx.flush_segment().unwrap();
        for d in 1..=100u64 {
            idx.add("gc-b", d);
        }
        idx.flush_segment().unwrap();
        assert_eq!(idx.segment_count(), 2);
        let g = idx.gc().unwrap();
        assert_eq!(g.merged, 2);
        assert_eq!(idx.segment_count(), 1);
        // GC 后：新段已映射，旧段映射已释放，查询仍正确
        let seg = idx.segments.load()[0].clone();
        assert!(
            idx.data_files.load().contains_key(&seg),
            "GC 后应映射合并新段"
        );
        let ra = idx.search("gc-a").unwrap();
        let rb = idx.search("gc-b").unwrap();
        assert_eq!(ra.len(), 100);
        assert_eq!(rb.len(), 100);
    }

    // ---------- 位图索引（design 5.2.4，M7-2） ----------

    // ---------- G 项：posting 检索优化（term→bitmap 缓存） ----------

    #[test]
    fn search_hits_bitmap_whitelist_fast_path() {
        // 白名单字段 term：search 直接返回全量内存位图（与段遍历结果一致）
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 10_000).unwrap();
        idx.with_bitmap_fields(&["status".to_string(), "city".to_string()])
            .unwrap();
        for (term, d) in [
            ("status=active", 1u64),
            ("status=active", 3),
            ("status=inactive", 2),
            ("status=active", 5),
            ("city=beijing", 1),
            ("city=beijing", 2),
        ] {
            idx.add(term, d);
        }
        idx.flush_segment().unwrap(); // 落盘后白名单路径仍应全量
        let r = idx.search("status=active").unwrap();
        assert!(r.contains(1) && r.contains(3) && r.contains(5));
        assert!(!r.contains(2), "白名单路径不应含 inactive docid");
        let bj = idx.search("city=beijing").unwrap();
        assert_eq!(bj.len(), 2);
    }

    #[test]
    fn posting_cache_serves_repeat_terms_and_invalidates_on_write() {
        // 非白名单 term：首次反序列化入缓存，重复查询命中；写路径失效后查询更新
        let mut idx = InvertedIndex::open(&tmp(), 10_000).unwrap();
        idx.add("ft:content:山水", 1);
        idx.add("ft:content:山水", 3);
        idx.flush_segment().unwrap();
        let r1 = idx.search("ft:content:山水").unwrap();
        assert_eq!(r1.len(), 2);
        // 缓存已填充
        assert!(idx.posting_cache.lock().unwrap().contains("ft:content:山水"));
        // 重复查询命中缓存，结果一致
        let r2 = idx.search("ft:content:山水").unwrap();
        assert_eq!(r2, r1);
        // 写路径 → 缓存失效 → 新 docid 可查
        idx.add("ft:content:山水", 7);
        assert!(
            !idx.posting_cache.lock().unwrap().contains("ft:content:山水"),
            "写入后缓存应失效"
        );
        let r3 = idx.search("ft:content:山水").unwrap();
        assert_eq!(r3.len(), 3);
        assert!(r3.contains(7));
    }

    // ---------- 位图索引（design 5.2.4，M7-2） ----------

    #[test]
    fn bitmap_count_and_and_after_adds() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 10_000).unwrap();
        idx.with_bitmap_fields(&["status".to_string(), "city".to_string()])
            .unwrap();
        // status=active: 1,3,5；status=inactive: 2,4；city=beijing: 1,2
        idx.add("status=active", 1);
        idx.add("status=inactive", 2);
        idx.add("status=active", 3);
        idx.add("status=inactive", 4);
        idx.add("status=active", 5);
        idx.add("city=beijing", 1);
        idx.add("city=beijing", 2);
        // COUNT 快速路径
        assert_eq!(idx.bitmap_count("status", "active").unwrap(), 3);
        assert_eq!(idx.bitmap_count("city", "beijing").unwrap(), 2);
        // AND 交集：active AND beijing = {1}
        let and = idx.bitmap_and(&["status=active", "city=beijing"]).unwrap();
        assert_eq!(and.len(), 1);
        assert!(and.contains(1));
        // GROUP BY
        let g = idx.bitmap_group_by("status").unwrap();
        assert_eq!(
            g,
            vec![("active".to_string(), 3), ("inactive".to_string(), 2)]
        );
    }

    #[test]
    fn bitmap_off_by_default_returns_none() {
        let idx = InvertedIndex::open(&tmp(), 10_000).unwrap();
        idx.add("status=active", 1);
        assert!(idx.bitmap_count("status", "active").is_none(), "默认关闭");
        assert!(idx.bitmap_and(&["status=active"]).is_none());
        assert!(idx.bitmap_group_by("status").is_none());
    }

    #[test]
    fn bitmap_rebuilds_from_segments_on_reopen() {
        let dir = tmp();
        let fields = vec!["status".to_string()];
        {
            let mut idx = InvertedIndex::open(&dir, 1).unwrap();
            idx.with_bitmap_fields(&fields).unwrap();
            idx.add("status=active", 1);
            idx.flush_segment().unwrap(); // 落盘段
            idx.add("status=inactive", 2);
            idx.flush_segment().unwrap(); // 两条均落盘（重开才能重建）
        }
        // 重开：位图从段全量重建
        let mut idx = InvertedIndex::open(&dir, 1).unwrap();
        idx.with_bitmap_fields(&fields).unwrap();
        assert_eq!(idx.bitmap_count("status", "active").unwrap(), 1);
        assert_eq!(idx.bitmap_count("status", "inactive").unwrap(), 1);
    }

    #[test]
    fn flush_and_search_across_segments() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap(); // 阈值 1，立即刷盘
        idx.add("a", 1);
        idx.flush_segment().unwrap();
        idx.add("a", 2);
        idx.add("b", 7);
        idx.flush_segment().unwrap();
        assert_eq!(idx.segment_count(), 2);

        // 跨段合并
        let r = idx.search("a").unwrap();
        assert!(r.contains(1) && r.contains(2));
        assert_eq!(r.len(), 2);
        assert!(idx.search("b").unwrap().contains(7));
    }

    #[test]
    fn restart_loads_manifest_and_segments() {
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open(&dir, 1).unwrap();
            idx.add("term-x", 10);
            idx.flush_segment().unwrap();
            idx.add("term-x", 20);
            idx.add("term-y", 30);
            idx.flush_segment().unwrap();
        }
        // 重启：Manifest 恢复段列表
        let idx2 = InvertedIndex::open(&dir, 1).unwrap();
        assert_eq!(idx2.segment_count(), 2);
        let r = idx2.search("term-x").unwrap();
        assert!(r.contains(10) && r.contains(20));
        assert!(idx2.search("term-y").unwrap().contains(30));
    }

    #[test]
    fn manifest_is_atomic_and_persists_next_id() {
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open(&dir, 1).unwrap();
            idx.add("k", 1);
            idx.flush_segment().unwrap();
            assert_eq!(idx.next_seg_id.load(Ordering::Relaxed), 2);
        }
        let text = std::fs::read_to_string(dir.join(MANIFEST_FILE)).unwrap();
        assert!(text.contains("inverted-00000001.seg"));
        let m: SegmentManifest = serde_json::from_str(&text).unwrap();
        assert_eq!(m.next_seg_id, 2);
    }

    #[test]
    fn needs_flush_obeys_threshold() {
        let idx = InvertedIndex::open(&tmp(), 3).unwrap();
        assert!(!idx.needs_flush());
        idx.add("t", 1);
        idx.add("t", 2);
        assert!(!idx.needs_flush());
        idx.add("t", 3);
        assert!(idx.needs_flush());
    }

    #[test]
    fn orphan_segment_not_loaded_after_restart() {
        // 崩溃恢复：GC 中途崩溃可能残留"孤儿段"（不在 Manifest 中）——
        // 启动只按 Manifest 加载，孤儿段不得污染查询结果（development 4.5）
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open(&dir, 1).unwrap();
            idx.add("term-a", 1);
            idx.flush_segment().unwrap();
        }
        // 制造孤儿段：直接写一个 .seg 文件，但不更新 Manifest
        std::fs::write(dir.join("inverted-99999999.seg"), b"orphan-garbage").unwrap();

        let idx2 = InvertedIndex::open(&dir, 1).unwrap();
        assert_eq!(idx2.segment_count(), 1, "只应加载 Manifest 记录的段");
        // 正常段不受影响；孤儿段内容不参与查询
        assert!(idx2.search("term-a").unwrap().contains(1));
        assert!(idx2.search("orphan-garbage").unwrap().is_empty());
    }

    #[test]
    fn doc_count_counts_unique_docs() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap();
        idx.add("status=active", 1);
        idx.add("status=active", 2);
        idx.flush_segment().unwrap(); // 段 1
        idx.add("status=active", 2); // 重复 docid（更新）→ 跨段合并去重
        idx.add("status=pending", 3);
        idx.flush_segment().unwrap(); // 段 2
        assert_eq!(idx.doc_count("status=active").unwrap(), 2, "跨段合并去重");
        assert_eq!(idx.doc_count("status=pending").unwrap(), 1);
        assert_eq!(idx.doc_count("status=absent").unwrap(), 0);
    }

    #[test]
    fn group_by_aggregates_by_field_prefix() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap();
        idx.add("status=active", 1);
        idx.add("status=active", 2);
        idx.add("status=pending", 3);
        idx.flush_segment().unwrap();
        idx.add("status=active", 4); // 内存态
        idx.add("type=order", 1);

        let groups = idx.group_by("status").unwrap();
        let map: std::collections::HashMap<String, u64> = groups.into_iter().collect();
        assert_eq!(map.get("status=active").copied(), Some(3), "内存+段合并");
        assert_eq!(map.get("status=pending").copied(), Some(1));
        assert!(!map.contains_key("type=order"), "只返回指定字段分组");
        assert_eq!(idx.group_by("type").unwrap().len(), 1);
    }

    #[test]
    fn iter_terms_merges_memory_and_segments() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap();
        idx.add("a=1", 1);
        idx.flush_segment().unwrap();
        idx.add("b=2", 2);
        idx.add("a=1", 3);
        let terms = idx.iter_terms().unwrap();
        let map: std::collections::BTreeMap<String, u64> =
            terms.into_iter().map(|(t, b)| (t, b.len())).collect();
        assert_eq!(map.get("a=1").copied(), Some(2), "跨段+内存合并");
        assert_eq!(map.get("b=2").copied(), Some(1));
    }

    #[test]
    fn v4_doc_count_fast_matches_exact_across_flushes() {
        // Ex-9.1b：v4 段计数载荷——多段（docid 不重叠）flush 后 doc_count_fast（求和）
        // == doc_count（精确去重）== 实际总数；重启加载后载荷落盘仍一致。
        let dir = tempfile::tempdir().unwrap();
        let mut idx = InvertedIndex::open(dir.path(), 10_000).unwrap();
        let mut items = Vec::new();
        for d in 1..=100u64 {
            items.push(("status=active", d));
        }
        for d in 1_000..=1_100u64 {
            items.push(("status=active", d));
        }
        idx.add_batch(&items);
        idx.flush_segment().unwrap(); // v4 段 1
        let mut items2 = Vec::new();
        for d in 5_000..=5_200u64 {
            items2.push(("status=active", d));
        }
        idx.add_batch(&items2);
        idx.flush_segment().unwrap(); // v4 段 2
        let expect = 100 + 101 + 201;
        assert_eq!(idx.doc_count("status=active").unwrap(), expect, "精确去重");
        assert_eq!(
            idx.doc_count_fast("status=active").unwrap(),
            Some(expect),
            "fast 载荷求和 == 精确（无跨段重叠）"
        );
        // 重启加载：段 v4 解析 + 载荷求和一致
        drop(idx);
        let idx2 = InvertedIndex::open(dir.path(), 10_000).unwrap();
        assert_eq!(idx2.doc_count("status=active").unwrap(), expect);
        assert_eq!(idx2.doc_count_fast("status=active").unwrap(), Some(expect));
        // search 走 v4 解析也一致（posting 完整可读）
        assert_eq!(idx2.search("status=active").unwrap().len(), expect as u64);
    }

    #[test]
    fn v4_doc_count_fast_overlap_upper_bound_documented() {
        // 语义注：同 docid 同 term 跨段覆盖（update 场景）→ 求和（段间重复）高估，
        // 精确去重 doc_count 仍正确——文档化差异，调用方按场景选择。
        let dir = tempfile::tempdir().unwrap();
        let mut idx = InvertedIndex::open(dir.path(), 10_000).unwrap();
        idx.add_batch(&[("status=active", 7)]);
        idx.flush_segment().unwrap();
        idx.add_batch(&[("status=active", 7)]); // 同 docid 同 term 再写 → 跨段重复
        idx.flush_segment().unwrap();
        assert_eq!(idx.doc_count("status=active").unwrap(), 1, "精确去重 = 1");
        assert_eq!(
            idx.doc_count_fast("status=active").unwrap(),
            Some(2),
            "求和 = 2（跨段重叠高估，文档化近似）"
        );
    }

    fn fst_dict_built_on_flush_and_lookup() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap(); // 默认 fst 引擎
        idx.add("status=active", 1);
        idx.add("status=active", 2);
        idx.add("type=order", 3);
        idx.flush_segment().unwrap();
        // .fst 文件已生成，重启后字典加载
        assert!(
            dir.join("inverted-00000001.fst").exists(),
            "应生成 FST 字典文件"
        );
        let idx2 = InvertedIndex::open(&dir, 1).unwrap();
        assert_eq!(idx2.fst_dict_count(), 1, "FST 字典应加载");
        // 走 FST 精确定位路径
        assert!(idx2.search("status=active").unwrap().contains(1));
        assert!(idx2.search("status=active").unwrap().contains(2));
        assert!(!idx2.search("status=active").unwrap().contains(3));
        assert!(idx2.search("absent").unwrap().is_empty());
        // 聚合走 FST 段数据
        assert_eq!(idx2.doc_count("type=order").unwrap(), 1);
    }

    #[test]
    fn fst_and_hash_engines_return_same_results() {
        let dir_a = tmp();
        let dir_b = tmp();
        let mut fst = InvertedIndex::open_with_engine(&dir_a, 1, "fst").unwrap();
        let mut hash = InvertedIndex::open_with_engine(&dir_b, 1, "hash").unwrap();
        for i in 1..=20u64 {
            fst.add(&format!("f={}", i % 5), i);
            hash.add(&format!("f={}", i % 5), i);
            if i % 7 == 0 {
                fst.flush_segment().unwrap();
                hash.flush_segment().unwrap();
            }
        }
        fst.flush_segment().unwrap();
        hash.flush_segment().unwrap();
        for i in 0..5u64 {
            let term = format!("f={i}");
            assert_eq!(
                fst.doc_count(&term).unwrap(),
                hash.doc_count(&term).unwrap(),
                "引擎结果应一致: {term}"
            );
        }
        assert_eq!(
            fst.fst_dict_count(),
            hash.segment_count(),
            "fst 每段一个字典"
        );
    }

    #[test]
    fn fst_missing_dict_falls_back_to_linear_scan() {
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open(&dir, 1).unwrap();
            idx.add("a=1", 1);
            idx.add("b=2", 2);
            idx.flush_segment().unwrap();
        }
        // 删除 FST 字典（模拟旧段 / 字典损坏），应回退线性扫描
        std::fs::remove_file(dir.join("inverted-00000001.fst")).unwrap();
        let idx = InvertedIndex::open(&dir, 1).unwrap();
        assert_eq!(idx.fst_dict_count(), 0, "字典缺失应回退");
        assert!(idx.search("a=1").unwrap().contains(1));
        assert!(idx.search("b=2").unwrap().contains(2));
        assert_eq!(idx.doc_count("a=1").unwrap(), 1);
    }

    #[test]
    fn hash_engine_writes_no_fst_dict() {
        let dir = tmp();
        let mut idx = InvertedIndex::open_with_engine(&dir, 1, "hash").unwrap();
        idx.add("k=1", 1);
        idx.flush_segment().unwrap();
        assert!(
            !dir.join("inverted-00000001.fst").exists(),
            "hash 引擎不生成 FST"
        );
        let idx2 = InvertedIndex::open(&dir, 1).unwrap();
        assert_eq!(idx2.fst_dict_count(), 0);
        assert!(idx2.search("k=1").unwrap().contains(1));
    }

    // ---- Ex-6.2/6.3 并发读优化（ArcSwap 原子发布）----

    #[test]
    fn arc_swap_snapshot_consistency_after_flush() {
        // Ex-6.2：load_full 旧快照在 flush 发布新快照后仍有效（快照一致性：
        // 旧段文件未删前旧快照仍可查）
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap();
        idx.add("a=1", 1);
        idx.add("b=2", 2);
        idx.flush_segment().unwrap();
        assert_eq!(idx.segment_count(), 1);
        let old_snapshot = idx.segments.load_full(); // 旧快照（段 1）
        // 再 flush 一段 → 发布新快照
        idx.add("c=3", 3);
        idx.flush_segment().unwrap();
        assert_eq!(idx.segment_count(), 2);
        // 旧快照独立不变、新快照已更新
        assert_eq!(old_snapshot.len(), 1, "旧快照发布后不变");
        assert_eq!(idx.segments.load().len(), 2, "新快照已更新");
        // 旧快照中的段仍可查（文件未删）
        assert_eq!(idx.search("a=1").unwrap().len(), 1);
        assert_eq!(idx.search("c=3").unwrap().len(), 1);
        // FST 字典同样快照化（Ex-6.3）：flush 后新段字典可见
        assert!(idx.dicts.load().contains_key("inverted-00000002.seg"));
    }

    #[test]
    fn concurrent_readers_safe_on_shared_index() {
        // Ex-6.2/6.3：&self 读方法（search/segment_count/fst_dict_count）可被多线程
        // 同时调用——ArcSwap 读路径无锁（InvertedIndex: Sync）
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 1).unwrap();
        for i in 0..1000u64 {
            idx.add(&format!("f={}", i % 10), i);
        }
        idx.flush_segment().unwrap();
        let shared = Arc::new(idx);
        let mut hs = Vec::new();
        for t in 0..8u64 {
            let s = Arc::clone(&shared);
            hs.push(std::thread::spawn(move || {
                for i in 0..200u64 {
                    let _ = s.search(&format!("f={}", (i + t) % 10)).unwrap();
                    let _ = s.segment_count();
                    let _ = s.fst_dict_count();
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        // 并发读后状态一致
        assert_eq!(shared.segment_count(), 1);
        assert_eq!(shared.search("f=3").unwrap().len(), 100);
    }

    #[test]
    fn flush_interleaved_with_reads_consistent() {
        // Ex-6.2/6.3：flush 发布新快照与读交替执行，读结果始终一致（旧快照期间
        // 旧段仍可查——发布不破坏进行中的读）
        let dir = tmp();
        let idx = Arc::new(std::sync::Mutex::new(
            InvertedIndex::open(&dir, 1).unwrap(),
        ));
        {
            let mut g = idx.lock().unwrap();
            for i in 0..100u64 {
                g.add(&format!("k={}", i % 10), i);
            }
            g.flush_segment().unwrap();
        }
        let mut hs = Vec::new();
        for t in 0..4u64 {
            let idx = Arc::clone(&idx);
            hs.push(std::thread::spawn(move || {
                for i in 0..60u64 {
                    if i % 4 == 0 {
                        let mut g = idx.lock().unwrap();
                        g.add(&format!("new={}", (i + t) % 10), i + t);
                        let _ = g.flush_segment();
                    } else {
                        let g = idx.lock().unwrap();
                        let _ = g.search(&format!("k={}", (i + t) % 10));
                    }
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        let g = idx.lock().unwrap();
        assert!(g.segment_count() >= 1, "读写交替后段数正常");
        assert_eq!(g.search("k=5").unwrap().len(), 10, "历史段数据保持可查");
    }

    // ---- 预分片 Chunk（design 5.2.1，阶段 2）----

    #[test]
    fn chunks_partition_posting_and_concatenate() {
        let dir = tmp();
        let idx = InvertedIndex::open(&dir, 10_000).unwrap();
        // 散布大量 docid，覆盖全部分片
        for i in 1..=10_000u64 {
            idx.add("status=active", i);
        }
        let shard_count = 4u32;
        let full = idx.search("status=active").unwrap();
        let mut chunks = Vec::new();
        for s in 0..shard_count {
            chunks.push(
                idx.chunk_for_shard("status=active", s, shard_count)
                    .unwrap(),
            );
        }
        // ① 分片 Chunk 互不相交（partition 性质）
        for i in 0..shard_count {
            for j in (i + 1)..shard_count {
                let inter = &chunks[i as usize] & &chunks[j as usize];
                assert!(inter.is_empty(), "分片 Chunk 不得重叠");
            }
        }
        // ② 每个 Chunk 内 docid 确实属于对应分片
        for s in 0..shard_count {
            for d in chunks[s as usize].iter() {
                let vs = (crate::sharding::hash64(d as u64) % shard_count as u64) as u32;
                assert_eq!(vs, s, "Chunk 内 docid 分片归属错误");
            }
        }
        // ③ 按序直拼 = 全集（design 5.2.1：O(1) 合并）
        let merged = InvertedIndex::concatenate_chunks(&chunks);
        assert_eq!(merged, full, "直拼结果必须等于全集");
        assert_eq!(merged.len(), 10_000);
        // ④ 无 docid 落点的分片 → 空 Chunk：单 docid term，其它分片必为空
        idx.add("rare=1", 1);
        let owner_shard = (crate::sharding::hash64(1) % 4) as u32; // docid=1 归属的分片
        let mut empty_count = 0;
        for s in 0..4 {
            let chunk = idx.chunk_for_shard("rare=1", s, 4).unwrap();
            if s == owner_shard {
                assert_eq!(chunk.len(), 1, "归属分片应含该 docid");
            } else {
                assert!(chunk.is_empty(), "非归属分片应为空 Chunk");
                empty_count += 1;
            }
        }
        assert_eq!(empty_count, 3, "其余 3 个分片应为空 Chunk");
    }

    #[test]
    fn chunk_works_across_segments_and_memory() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 3).unwrap();
        idx.add("city=beijing", 1);
        idx.add("city=beijing", 2);
        idx.flush_segment().unwrap();
        idx.add("city=beijing", 3);
        idx.add("city=beijing", 4);
        idx.flush_segment().unwrap();
        idx.add("city=beijing", 5); // 内存态
        let shard_count = 2u32;
        let mut chunks = Vec::new();
        for s in 0..shard_count {
            chunks.push(idx.chunk_for_shard("city=beijing", s, shard_count).unwrap());
        }
        let full = idx.search("city=beijing").unwrap();
        assert_eq!(full.len(), 5);
        assert_eq!(InvertedIndex::concatenate_chunks(&chunks), full);
    }

    // ---- 倒排段 GC（design 5.2.2 + 5.2.4⑤，阶段 2）----

    #[test]
    fn gc_merges_segments_preserving_data() {
        let dir = tmp();
        // 极小 GC 阈值（1 字节）：任意 >1 段即触发
        let mut idx = InvertedIndex::open_with_gc(&dir, 2, "fst", 1).unwrap();
        // 4 段，term 跨段重复
        for seg in 0..4u64 {
            idx.add("status=active", 1 + seg * 2);
            idx.add("status=active", 2 + seg * 2);
            idx.add("status=pending", 100 + seg);
            idx.flush_segment().unwrap();
        }
        assert_eq!(idx.segment_count(), 4);
        assert!(idx.should_gc());
        let before = idx.segment_bytes();

        let report = idx.gc().unwrap();
        assert_eq!(report.merged, 4);
        assert_eq!(idx.segment_count(), 1, "GC 后应合并为 1 段");
        assert!(report.segment_count == 1);
        // 数据保持完整
        let active = idx.search("status=active").unwrap();
        assert_eq!(active.len(), 8, "跨段合并去重后应有 8 个 docid");
        let pending = idx.search("status=pending").unwrap();
        assert_eq!(pending.len(), 4);
        // FST 字典重建（1 段 1 字典）
        assert_eq!(idx.fst_dict_count(), 1);
        // 旧段文件已删除
        for seg in ["inverted-00000001.seg", "inverted-00000002.seg"] {
            assert!(!dir.join(seg).exists(), "旧段 {seg} 应被删除");
        }
        // 新段存在（id=5）
        assert!(dir.join("inverted-00000005.seg").exists());
        assert!(report.freed_bytes >= before.saturating_sub(idx.segment_bytes()));
    }

    #[test]
    fn gc_disabled_when_threshold_zero() {
        let dir = tmp();
        let mut idx = InvertedIndex::open(&dir, 2).unwrap(); // gc 阈值 0 = 禁用
        for _ in 0..3 {
            idx.add("k=1", 1);
            idx.flush_segment().unwrap();
        }
        assert!(!idx.should_gc());
        let report = idx.gc().unwrap();
        assert_eq!(report.merged, 0, "禁用时 GC 应为空操作");
        assert_eq!(idx.segment_count(), 3);
    }

    #[test]
    fn gc_then_restart_loads_manifest_correctly() {
        let dir = tmp();
        {
            let mut idx = InvertedIndex::open_with_gc(&dir, 2, "fst", 1).unwrap();
            idx.add("a=1", 1);
            idx.add("a=1", 2);
            idx.flush_segment().unwrap();
            idx.add("a=1", 3);
            idx.add("b=2", 9);
            idx.flush_segment().unwrap();
            idx.gc().unwrap();
        }
        // 重启：只加载 Manifest 中的新段
        let idx2 = InvertedIndex::open(&dir, 2).unwrap();
        assert_eq!(idx2.segment_count(), 1);
        let a = idx2.search("a=1").unwrap();
        assert!(a.contains(1) && a.contains(2) && a.contains(3));
        assert!(idx2.search("b=2").unwrap().contains(9));
        // Manifest 不含孤儿
        let text = std::fs::read_to_string(dir.join(MANIFEST_FILE)).unwrap();
        assert!(
            !text.contains("inverted-00000001.seg"),
            "旧段不得出现在 Manifest"
        );
        assert!(text.contains("inverted-00000003.seg"));
    }

    #[test]
    fn gc_not_triggered_below_threshold() {
        let dir = tmp();
        // 阈值极大（1TB）：段再多也不 GC
        let mut idx =
            InvertedIndex::open_with_gc(&dir, 2, "fst", 1024 * 1024 * 1024 * 1024).unwrap();
        for _ in 0..3 {
            idx.add("k=1", 1);
            idx.flush_segment().unwrap();
        }
        assert!(!idx.should_gc(), "低于阈值不应触发 GC");
        let report = idx.gc().unwrap();
        assert_eq!(report.merged, 0);
        assert_eq!(idx.segment_count(), 3);
    }

    // ---------- J 项（7.73）：后台 GC 线程化 —— flush/gc 并发安全（mutate 锁） ----------

    #[test]
    fn concurrent_flush_and_gc_no_lost_segment() {
        // 后台 GC 线程化后写路径 flush 与后台 gc **并发**：mutate 锁保证 Manifest 无丢失更新
        // （demo inverted-gc-bg 确定性复现：无锁时 Manifest 引用已删段 → 数据丢失）。
        // 写路径为主库真实形态：单写者 flush（Engine 写锁内）+ 后台 gc 线程并发。
        let dir = tmp();
        // gc 阈值极小（2 段即合并）；flush_threshold=1（add 即达阈值，写线程持续刷盘）
        let idx = Arc::new(InvertedIndex::open_with_gc(&dir, 1, "hash", 1).unwrap());
        // 写线程×1（单写者形态）：add + flush 循环（模拟写路径持续刷盘）
        let writer = {
            let idx = Arc::clone(&idx);
            std::thread::spawn(move || {
                for batch in 0..60u64 {
                    for k in 0..5u64 {
                        idx.add(&format!("w-b{batch}-k{k}"), batch * 5 + k);
                    }
                    let _ = idx.flush_segment(); // 刷盘（写路径）
                }
            })
        };
        // gc 线程×1：周期执行（后台 GC 语义，与写路径并发）
        let gc_thread = {
            let idx = Arc::clone(&idx);
            std::thread::spawn(move || {
                for _ in 0..120 {
                    let _ = idx.gc();
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            })
        };
        writer.join().unwrap();
        gc_thread.join().unwrap();
        // 完整性：Manifest 引用的段文件全部存在（mutate 锁 → 无丢失更新）
        let text = std::fs::read_to_string(dir.join(MANIFEST_FILE)).unwrap();
        let m: SegmentManifest = serde_json::from_str(&text).unwrap();
        assert!(!m.segments.is_empty(), "Manifest 不应为空");
        for seg in &m.segments {
            assert!(dir.join(seg).exists(), "Manifest 引用已删段: {seg}");
        }
        // 数据不丢：所有写入 term 均可检索（含被合并进新段的旧段数据）
        for batch in 0..60u64 {
            for k in 0..5u64 {
                let d = batch * 5 + k;
                assert!(
                    idx.search(&format!("w-b{batch}-k{k}"))
                        .unwrap()
                        .contains(d as u32),
                    "w-b{batch}-k{k} 数据丢失（docid {d}）"
                );
            }
        }
    }

    #[test]
    fn gc_self_ref_reentrant_safe() {
        // J 项：gc 改 &self 后——Arc 共享下可直接调用（后台 worker 无锁执行形态），
        // 且 gc 内部 mutate 锁与 flush_segment 互斥不 panic。
        let dir = tmp();
        let idx = Arc::new(InvertedIndex::open_with_gc(&dir, 1, "hash", 1).unwrap());
        idx.add("a=1", 1);
        idx.flush_segment().unwrap();
        idx.add("b=2", 2);
        idx.flush_segment().unwrap();
        assert!(idx.should_gc());
        let report = idx.gc().unwrap();
        assert_eq!(report.merged, 2);
        // 合并后仍可检索
        assert!(idx.search("a=1").unwrap().contains(1));
        assert!(idx.search("b=2").unwrap().contains(2));
    }

    // ---------- K 项（7.74）：v3 posting 分块布局（分页/COUNT 按容器延迟加载） ----------

    #[test]
    fn v3_paged_matches_full_search_across_segments() {
        // 大 posting 分页快速路径与全量 search 窗口一致（多段、hash 引擎线性扫描定位）
        let dir = tmp();
        let idx = InvertedIndex::open_with_gc(&dir, 10, "hash", 0).unwrap();
        for seg in 0..3u64 {
            for i in seg * 2000..seg * 2000 + 2000 {
                idx.add("hot", i);
            }
            idx.flush_segment().unwrap();
        }
        let full = idx.search("hot").unwrap();
        assert_eq!(full.len(), 6000);
        for (off, lim) in [
            (0u64, 10u64),
            (5, 10),
            (1000, 100),
            (5990, 20),
            (0, 100_000),
        ] {
            let (total, ids) = idx.search_paged("hot", off, lim).unwrap();
            assert_eq!(total, 6000, "total 应一致");
            let expect: Vec<u32> = full.iter().skip(off as usize).take(lim as usize).collect();
            assert_eq!(ids, expect, "窗口 ({off},{lim}) 应与全量 search 一致");
        }
        // COUNT 快速路径精确
        assert_eq!(idx.doc_count("hot").unwrap(), 6000);
    }

    #[test]
    fn v3_paged_dedups_docids_across_segments() {
        // 跨段重复 docid（同主键更新未 GC）：窗口去重、doc_count 精确
        let dir = tmp();
        let idx = InvertedIndex::open_with_gc(&dir, 10, "hash", 0).unwrap();
        idx.add("dup", 1);
        idx.add("dup", 2);
        idx.flush_segment().unwrap();
        idx.add("dup", 2); // 跨段重复
        idx.add("dup", 3);
        idx.flush_segment().unwrap();
        assert_eq!(idx.doc_count("dup").unwrap(), 3, "COUNT 必须跨段去重");
        let (_, ids) = idx.search_paged("dup", 0, 100).unwrap();
        assert_eq!(ids, vec![1, 2, 3], "分页窗口必须去重");
        // 深页 offset 基于去重后流
        let (_, ids2) = idx.search_paged("dup", 1, 100).unwrap();
        assert_eq!(ids2, vec![2, 3], "offset 应基于去重后流");
    }

    #[test]
    fn v3_full_decode_equals_compact_roundtrip() {
        // v3 编码 → 全量解码与原始 bitmap 一致（search 全量路径正确性）
        let dir = tmp();
        let idx = InvertedIndex::open_with_gc(&dir, 10, "hash", 0).unwrap();
        for i in (0..100_000u64).step_by(3) {
            idx.add("sparse", i);
        }
        idx.flush_segment().unwrap();
        let bm = idx.search("sparse").unwrap();
        assert_eq!(bm.len(), 33_334);
        // 命中集合抽查（稀疏跨多容器）
        assert!(bm.contains(0) && bm.contains(99_999) && bm.contains(50_001));
        assert!(!bm.contains(1));
    }
}
