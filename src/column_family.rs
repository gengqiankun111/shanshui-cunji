//! 列族框架 + 主数据 CRUD（design 4.1 / development 步骤 7）。
//!
//! 物理目录布局：
//! ```text
//! {data_dir}/{cf_name}/
//!   ├── wal.log         # 本列族 WAL（组提交）
//!   ├── manifest.json   # SST 文件清单（新→旧），重启时按序加载
//!   └── sst-{id:08}.sst # 刷盘生成的不可变有序文件
//! ```
//!
//! 读路径：MemTable(Mutable → Immutable) → SST 新→旧，首个命中即最新；
//! 写路径：WAL append → MemTable，超阈值冻结切换并后台（MVP 同步）刷盘；
//! 重启恢复：manifest 加载全部 SST + WAL 回放重建 MemTable。
//!
//! 已知 MVP 局限（步骤 9 修复）：flush 时跳过 Tombstone 条目，
//! 删除标记只存在于 WAL/MemTable 生命周期内；跨 flush 的删除一致性由步骤 9 补齐。

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::blockcache::{BlockCache, BlockCacheKey};
use crate::bloom::BloomFilter;
use crate::config::model::{Config, MemtableConfig};
use crate::error::{Error, Result};
use crate::keys::{decode_docid, encode_docid};
use crate::memtable::{MemTable, MemTableBuffer};
use crate::sstable::{
    Compression, IndexEntry, SstFooter, SstReader, SstWriter, FLAG_DELETE, FLAG_PUT,
};
use crate::wal::{RingWal, WalBackend, WalMode, WalReader, WalWriter, OP_DELETE, OP_PUT};

/// SST 文件前缀。
const SST_PREFIX: &str = "sst-";
const WAL_FILE: &str = "wal.log";
const MANIFEST_FILE: &str = "manifest.json";

/// Manifest：列族内 SST 文件清单（新→旧顺序）。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Manifest {
    /// 最近刷盘的文件在最前（读路径优先命中）。
    sst_files: Vec<String>,
    /// 每个 SST 的层号（design 4.5 二期 Leveled，M6-2）：0 = 刷盘产物（允许重叠），
    /// 1 / 2 = Compaction 输出（层内 key 范围不重叠）。旧 Manifest 缺省按全 0（L0）兼容。
    #[serde(default)]
    levels: Vec<u32>,
    /// 下一个可用 SST id。
    next_sst_id: u64,
}

/// SST 快照（O 项第③步）：ssts + 层号**原子打包**——读路径 `load()` 无锁快照（与后台合并并发读），
/// 写路径（flush/compact/加载）构建新快照 `store()` 原子切换；旧 Arc 引用计数归零后文件才可删
/// （P53 模式：先换快照再删旧文件，读线程持 Arc 期间文件句柄保持有效）。
pub struct SstSnapshot {
    /// SST 文件（新→旧；读路径按序取首个命中，依赖"新文件在前"版本语义）。
    pub ssts: Vec<Arc<SstReader>>,
    /// 与 `ssts` 平行的层号（design 4.5 二期 Leveled Compaction，M6-2）。
    pub levels: Vec<u32>,
    /// R 项：每层 key 范围 [min, max]（按层号索引 [L0, L1, L2]；层空或含无范围段 → None
    /// = 该层不可跳过）。点查层级 Zone Map 粗筛：key 越出层范围 → 整层 O(1) 跳过
    /// （省逐段二分 + 分区布隆反序列化；精确判断，无假阴性）。
    pub layer_ranges: Vec<Option<(Vec<u8>, Vec<u8>)>>,
    /// R 项：每层段下标（按层号索引；组内保持快照顺序 = 新→旧，层序 L0→L1→L2 与
    /// "新→旧"一致——flush 只进 L0、compact 下沉，层间版本语义安全）。
    pub layer_indices: Vec<Vec<usize>>,
    /// P3-A：L0 层**按表分组**范围——key 范围 [min, max] 按 table_id 聚合。
    /// 由于 flush 按表切分，每个 L0 SST 只含单表数据，所以可按表分组聚合范围。
    /// 点查时只检查目标表分组内的 L0 SST，跳过其他表全部 L0 SST，减少逐 SST 布隆校验。
    pub l0_table_ranges: Option<std::collections::HashMap<u16, (Vec<u8>, Vec<u8>)>>,
    /// 与 `ssts` 平行的文件字节数（open/flush/compact 构建时缓存；写路径 needs_compact
    /// 的大小条件读此缓存，零 fs::metadata syscall——修复每 put N 次 stat 拖垮写吞吐）。
    pub sizes: Vec<u64>,
}

/// 列族：主数据 / 组合索引 / Delta 共用骨架。
pub struct ColumnFamily {
    name: String,
    dir: PathBuf,
    cfg: MemtableConfig,
    compression: Compression,
    compression_level: i32,
    /// Ex-8.12：L2+ 冷档 zstd 级别（0 = 不分层）；见 `compression_level_for`。
    compression_level_l2: i32,
    block_size: usize,
    /// 布隆假阳性率（`sstable.bloom_fpr`，分区布隆每块构建用）。
    bloom_fpr: f64,
    /// PAX 热字段白名单（阶段 1.5，来自 [storage] hot_fields；空 = 行式）。
    pax_hot_fields: Vec<String>,
    /// TTL 天数（None = 关闭；开启后 SST 按文档 ttl_field 分天桶，过期整文件删除）。
    ttl_days: Option<u32>,
    /// TTL 时间字段名（文档 JSON 内数值秒级时间戳）。
    ttl_field: String,
    /// 两级索引粒度（`sstable.index_granularity`，每 N 块一条 Level 1 摘要）。
    index_granularity: usize,
    /// M3（§26 多表）：按表切分 SST 输出——flush/compaction 按 docid 高 16 位（table_id）
    /// 边界把输出切为**每表一个文件**（docid 区间含表 → 查询窗口剪枝自动跳过其它表文件、
    /// DROP TABLE 可整文件回收）。仅 docid 定长 8 字节键的列族（主数据 primary）开启；
    /// 单表（table_id=0）输出仍单文件，与旧行为一致。
    split_by_table: bool,
    /// 后台 IO 限速器（design 4.5 阶段 3；`storage.io_rate_limit_mb`，None = 不限速）。
    /// O 项第③步：内部 `Mutex`——compact `&self` 读路径并发 acquire。
    io_limiter: Option<Mutex<crate::io_scheduler::IoRateLimiter>>,
    /// 导出共享后台 IO 限速器（design 20.5）：**顺序扫描**（scan_stream）路径专用——
    /// 与 Compaction 的 `io_limiter` 同 Token Bucket 策略（默认低于前台读写）。导出工具
    /// 显式启用（`--io-rate-limit-mb` / Engine::set_scan_rate_limit）；前台点查（get）不受影响。
    scan_limiter: Mutex<Option<crate::io_scheduler::IoRateLimiter>>,
    /// L0 段数阈值（`storage.l0_stall_threshold`，超过判需要 Compaction）。
    l0_stall_threshold: usize,
    /// L 项：动态窗口下限/上限（低峰放宽、高峰收窄；`storage.l0_stall_min/max`）。
    l0_stall_min: usize,
    l0_stall_max: usize,
    /// L 项：前台写压力（0~1，Ex-7.4 同源信号：MemTable 水位代理）——动态窗口反馈依据。
    write_pressure: AtomicU64,
    /// P 项：L0 大小软阈值（字节；`storage.l0_max_size_mb`；0 = 禁用，仅用段数阈值）。
    l0_max_size_bytes: u64,
    /// Ex-8.11：L1 段数触发阈值（0 = 现行为：L0 空时 L1>1 即下沉 L2）。>0 = 延迟大合并，
    /// L1 攒够该段数（或 L0 活跃纳入 L0+L1 合并的"已满"界限）才收敛。
    /// P4-A：AtomicUsize——写入爆发时自适应降为 2（`record_flush_new_l0` 动态调整）。
    l1_trigger_files: AtomicUsize,
    /// Ex-8.11：L2 段数触发阈值（0 = 现行为：L2>1 即收敛为单段）。
    l2_trigger_files: usize,
    /// Ex-8.6：文件级**最小 put seq** 惰性记忆（path → min seq of put rows）。
    /// 快照读（get_bytes_at / scan_stream_at，snapshot<MAX）整段剪枝用：文件所有 put
    /// 行的 seq 均 > 快照点 → 该段对快照贡献为空，O(1) 跳过（免建迭代器/免读块）。
    /// 未知（首见/重启后）→ 对该文件做一次 keys-only 扫描推导（一次性，按需缓存）；
    /// 重启安全：无需 manifest 扩展，首次快照读自动重建。
    seq_min: RwLock<std::collections::HashMap<PathBuf, u64>>,
    /// 单次合并输入大小上限（字节；`storage.compact_input_max_mb`；0 = 不限）——
    /// L0 分批合并防大输入一次合并长时间阻塞写。
    compact_input_max_bytes: u64,
    /// L 项：合并冷却轮次（`storage.compaction_cooldown`；0 = 关闭）。
    compaction_cooldown: u32,
    /// L 项：当前合并轮次（每次 compact 成功 +1；冷却到期基准）。
    /// O 项第③步：原子（compact `&self` 更新）。
    merge_round: AtomicU64,
    /// L 项：冷却中的段（新段 path → 到期轮次）；到期后正常参与合并。纯内存调度态。
    /// O 项第③步：内部 `Mutex`（compact `&self` 读写）。
    cooldown: std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, u64>>,
    /// P4-A：滑动窗口写入速率监控（最近 N 次 flush 的 L0 新增段数）。
    /// 窗口大小 = 配置 `compaction_write_rate_window`，窗口满后计算速率。
    write_rate_window: std::sync::Mutex<Vec<usize>>,
    /// P4-A：窗口大小（滑动窗口长度，flush 次数）。
    write_rate_window_size: usize,
    /// P4-A：L0 段数增速阈值（窗口内新增超过此值 → 爆发模式）。
    write_rate_burst_threshold: usize,
    /// P4-A：基准 `l1_trigger_files`（未爆发时）。
    base_l1_trigger: usize,
    /// X 项：累计刷盘次数（switch_and_flush 成功 +1；/metrics 指标）。
    flush_counter: AtomicU64,
    /// P4-A：最近一次 flush 创建的 SST 段数（由 flush_by_table / flush_buckets 在
    /// snapshot_insert 中累计，供 `switch_and_flush` 记录写入速率窗口）。
    flush_sst_count: AtomicUsize,
    /// R4（review 2026-09-04）：MVCC 保活水位（seq floor）。compact_merge 去重时，
    /// 最新 seq > floor 的 key 保留多版本（活跃旧快照可回读到删除/覆盖前旧值）；
    /// 位图物理回收（drop_key）亦仅当无活跃快照（floor=0）或最新 seq ≤ floor 时执行。
    /// 0 = 关闭（现状：后写覆盖先写、GC 按位图物理回收）。引擎在 compact 前设置活跃
    /// 快照低水位，compact 后复位。
    mvcc_keep_floor: AtomicU64,
    /// Ex-8.11 A/B：累计**写入磁盘的 SST 字节**（flush/compact 每新建一个文件计一次该文件
    /// 字节；覆盖被合并删除的旧文件——总写入 = 数据量 × 写放大；/metrics 与写放大实验数据源）。
    sst_written: AtomicU64,
    /// 双缓冲 MemTable。
    memtable: MemTableBuffer,
    /// SST 快照（O 项第③步：ArcSwap 原子发布——读路径 load 无锁，写路径 store 切换）。
    ssts: ArcSwap<SstSnapshot>,
    /// P72（无锁合并）：SST 快照**变更互斥**——flush（snapshot_insert）与 compact
    /// （finalize_compact）无 Engine 锁并发时，ssts 的 load→build→store + manifest 持久化
    /// 需互斥（防两者基于旧快照 store，后写覆盖前写的并发丢失）。
    sst_mutate: Mutex<()>,
    /// 共享块缓存（跨 CF 共享）。
    block_cache: Arc<BlockCache>,
    /// 单调 seq 分配器（跨重启由 WAL 恢复推进）。
    seq: AtomicU64,
    /// 下一个 SST 文件 id（跨重启由 Manifest 恢复，防止覆盖旧文件）。O 项第③步：原子。
    next_sst_id: AtomicU64,
    /// 当前 WAL 写入器（append 追加 / ring 环形，design 4.3 阶段 3）。
    /// `Arc<Mutex>` 共享：组提交后台线程（M8）可独立触发落盘兜底。
    wal: Arc<Mutex<WalBackend>>,
    /// 外部全局 seq（MVCC，engine 层统一分配，M7-1）：Some 时写入走外部计数（跨列族一致）；
    /// None = 独立列族（测试 / 单 CF 场景）用内部 WAL seq。
    external_seq: Option<Arc<AtomicU64>>,
}

/// Compaction 结果报告（design 4.5 阶段 3 / 二期 Leveled，M6-2）。
#[derive(Debug, Clone, Copy)]
pub struct CompactReport {
    /// 被合并的旧段数。
    pub merged_ssts: usize,
    /// 被消除的重复旧版本键数（含被 Tombstone 覆盖的键）。
    pub kept_keys: usize,
    /// 释放的磁盘字节数。
    pub freed_bytes: u64,
    /// 压实输出所在层（0 = 未压实；1 / 2 = L1 / L2）。
    pub out_level: u32,
    /// Ex-8.7：压实中按删除位图**物理丢弃**的键数（Ex-5.6 位图删除数据回收量；
    /// 引擎据此决定删除密度 GC 是否继续排空 / 收敛）。
    pub dropped_keys: usize,
}

/// Ex-8.7 删除密度跨列族紧迫度权重（外挂项，见 `Engine::delete_garbage_urgency`）：
/// 取值介于 L0 大小软阈值超限（+8）与 L0 段数主因子（×10/段）之间——收敛后（L0=0）的
/// 删除密集主列族能压过空闲 delta/cidx 率先被合并回收空间，又不抢占真正 L0 段数压力档。
pub const DD_URGENCY: u32 = 6;

impl ColumnFamily {
    /// 打开（或创建）一个列族：加载 Manifest/SST、回放 WAL。WAL 与数据同目录（旧布局）。
    pub fn open(name: &str, dir: &Path, cfg: &Config) -> Result<Self> {
        Self::open_with_wal_dir(name, dir, None, cfg)
    }

    /// 打开列族并指定独立 WAL 目录（Ex-5.10 多 SSD 条带化：WAL 独占最快盘）。
    pub fn open_with_wal_dir(
        name: &str,
        dir: &Path,
        wal_dir: Option<&Path>,
        cfg: &Config,
    ) -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            Self::open_inner(name, dir, wal_dir, cfg, None)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self::open_inner(name, dir, wal_dir, cfg)
        }
    }

    /// V 项：打开列族并注入 io_uring 后端池（Linux + `runtime.io_uring_enabled`）——
    /// 加载的 SST 块读与 WAL fsync 经 SQPOLL 队列；Windows / 未启用传 None（同步路径）。
    #[cfg(target_os = "linux")]
    pub fn open_with_io_uring(
        name: &str,
        dir: &Path,
        wal_dir: Option<&Path>,
        cfg: &Config,
        iou: Option<std::sync::Arc<crate::io_queue::backend::IoUringPool>>,
    ) -> Result<Self> {
        Self::open_inner(name, dir, wal_dir, cfg, iou)
    }

    fn open_inner(
        name: &str,
        dir: &Path,
        wal_dir: Option<&Path>,
        cfg: &Config,
        #[cfg(target_os = "linux")]
        iou: Option<std::sync::Arc<crate::io_queue::backend::IoUringPool>>,
    ) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        if let Some(w) = wal_dir {
            std::fs::create_dir_all(w)?;
            std::fs::create_dir_all(w.join(name))?; // 独立 WAL 盘列族子目录
        }
        if cfg.storage.time_bucket != "day" {
            return Err(Error::Config(format!(
                "storage.time_bucket 仅支持 day（当前: {}）",
                cfg.storage.time_bucket
            )));
        }
        let manifest_path = dir.join(MANIFEST_FILE);
        let (mut sst_names, mut sst_levels, next_sst_id) = load_manifest(&manifest_path)?;

        // TTL 过期桶清理：按天整文件删除（删除成本 O(1)，无墓碑；design 5.4）
        if let Some(ttl_days) = cfg.storage.ttl_days {
            let cutoff = today_epoch_days() - ttl_days as i64;
            let mut removed = 0usize;
            let mut kept = Vec::new();
            for (i, f) in sst_names.iter().enumerate() {
                let keep = match parse_sst_date(f) {
                    Some(days) => days >= cutoff,
                    None => true, // 默认桶（无日期前缀）永不过期
                };
                if keep {
                    kept.push((f.clone(), sst_levels.get(i).copied().unwrap_or(0)));
                } else {
                    let p = dir.join(f);
                    if p.exists() {
                        std::fs::remove_file(&p).map_err(Error::Io)?;
                        removed += 1;
                    }
                }
            }
            if removed > 0 {
                sst_names = kept.iter().map(|(f, _)| f.clone()).collect();
                sst_levels = kept.iter().map(|(_, l)| *l).collect();
                info!("列族 [{name}] TTL 过期桶清理: 删除 {removed} 个 SST");
            }
        }

        // 打开全部 SST（新→旧）
        let mut ssts = Vec::new();
        let mut sst_levels_loaded = Vec::new();
        for (i, f) in sst_names.iter().enumerate() {
            let p = dir.join(f);
            if !p.exists() {
                warn!("Manifest 中的 SST 缺失，跳过: {}", p.display());
                continue;
            }
            // V 项：Linux + io_uring 启用时经 open_with_io_uring 注入 SQPOLL 池，否则同步打开
            #[cfg(target_os = "linux")]
            let open_res = SstReader::open_with_io_uring(
                &p,
                cfg.sstable.index_granularity as usize,
                iou.clone(),
            );
            #[cfg(not(target_os = "linux"))]
            let open_res =
                SstReader::open_with_granularity(&p, cfg.sstable.index_granularity as usize);
            match open_res {
                Ok(r) => {
                    ssts.push(r);
                    sst_levels_loaded.push(sst_levels.get(i).copied().unwrap_or(0));
                }
                Err(e) => {
                    return Err(Error::Corrupted(format!(
                        "SST 加载失败 {}: {e}",
                        p.display()
                    )))
                }
            }
        }
        info!(
            "列族 [{name}] 加载 {} 个 SST，下一个 id={next_sst_id}",
            ssts.len()
        );

        let block_cache = Arc::new(BlockCache::new(
            cfg.blockcache.max_memory_mb * 1024 * 1024,
            cfg.blockcache.block_size_kb * 1024,
        ));
        let compression = Compression::from_str(&cfg.sstable.compression)?;
        let compression_level = cfg.sstable.compression_level as i32;
        // Ex-8.12：L2+ 冷档 zstd 级别（0 = 不分层）
        let compression_level_l2 = cfg.sstable.compression_level_l2 as i32;

        let wal_path = match wal_dir {
            // 独立 WAL 盘按列族分子目录（多列族同名 wal.log 隔离；Ex-5.10 条带化）
            Some(w) => w.join(name).join(WAL_FILE),
            None => dir.join(WAL_FILE),
        };
        // WAL 模式（design 4.3 阶段 3）：
        // - append：传统追加文件（不截断旧 WAL），回放恢复 MemTable 后继续写入；
        // - ring：预分配环形文件，Flush 后上报刷盘游标腾空空间，满则强制 Flush。
        let (wal, wal_records) = match WalMode::parse(&cfg.storage.wal_mode) {
            WalMode::Ring => {
                let size = (cfg.storage.wal_ring_size_mb as usize) * 1024 * 1024;
                let (mut ring, recs) = RingWal::open_or_create(&wal_path, size)?;
                // V 项：注入 io_uring 池（Linux + 启用时）——WAL fsync 走 SQPOLL 队列
                #[cfg(target_os = "linux")]
                ring.set_io_uring(iou.clone());
                (WalBackend::Ring(ring), recs)
            }
            WalMode::Append => {
                // 先创建（open_append 兼容新库空文件），再回放
                let mut w = WalWriter::open_append(&wal_path, 1, false)?;
                // V 项：注入 io_uring 池（Linux + 启用时）——WAL fsync 走 SQPOLL 队列
                #[cfg(target_os = "linux")]
                w.set_io_uring(iou.clone());
                let recs = WalReader::recover(&wal_path)?;
                (WalBackend::Append(w), recs)
            }
        };
        let mut cf = Self {
            name: name.to_string(),
            dir: dir.to_path_buf(),
            cfg: cfg.memtable.clone(),
            compression,
            compression_level,
            compression_level_l2,
            block_size: cfg.blockcache.block_size_kb * 1024,
            bloom_fpr: cfg.sstable.bloom_fpr,
            pax_hot_fields: cfg.storage.hot_fields.clone(),
            ttl_days: cfg.storage.ttl_days,
            ttl_field: cfg.storage.ttl_field.clone(),
            index_granularity: cfg.sstable.index_granularity as usize,
            split_by_table: false,
            io_limiter: if cfg.storage.io_rate_limit_mb > 0 {
                Some(Mutex::new(crate::io_scheduler::IoRateLimiter::new(
                    cfg.storage.io_rate_limit_mb * 1024 * 1024,
                )))
            } else {
                None
            },
            scan_limiter: Mutex::new(None),
            l0_stall_threshold: cfg.storage.l0_stall_threshold,
            l0_stall_min: cfg.storage.l0_stall_min.max(2),
            l0_stall_max: cfg.storage.l0_stall_max.max(cfg.storage.l0_stall_min.max(2)),
            write_pressure: AtomicU64::new(0.0f64.to_bits()),
            l0_max_size_bytes: cfg.storage.l0_max_size_mb * 1024 * 1024,
            compact_input_max_bytes: cfg.storage.compact_input_max_mb * 1024 * 1024,
            // Ex-8.11：L1/L2 段数触发阈值（0 = 现行为）
            l1_trigger_files: AtomicUsize::new(cfg.storage.l1_trigger_files),
            l2_trigger_files: cfg.storage.l2_trigger_files,
            seq_min: RwLock::new(std::collections::HashMap::new()),
            compaction_cooldown: cfg.storage.compaction_cooldown,
            merge_round: AtomicU64::new(0),
            cooldown: Mutex::new(std::collections::HashMap::new()),
            // P4-A：写入速率滑动窗口（默认 8 次 flush 窗口，阈值 4 → 窗口内 >4 次 flush 算爆发）
            write_rate_window: Mutex::new(Vec::with_capacity(8)),
            write_rate_window_size: cfg.storage.compaction_write_rate_window.max(2),
            write_rate_burst_threshold: cfg.storage.compaction_write_rate_burst.max(1),
            base_l1_trigger: cfg.storage.l1_trigger_files,
            flush_counter: AtomicU64::new(0),
            flush_sst_count: AtomicUsize::new(0),
            sst_written: AtomicU64::new(0),
            mvcc_keep_floor: AtomicU64::new(0),
            memtable: MemTableBuffer::new(),
            ssts: {
                let loaded: Vec<Arc<SstReader>> = ssts.into_iter().map(Arc::new).collect();
                let (layer_ranges, layer_indices, l0_table_ranges) =
                    Self::build_layer_meta(&loaded, &sst_levels_loaded);
                let sizes: Vec<u64> = loaded.iter().map(|r| r.file_len()).collect();
                ArcSwap::new(Arc::new(SstSnapshot {
                    ssts: loaded,
                    levels: sst_levels_loaded,
                    layer_ranges,
                    layer_indices,
                    l0_table_ranges,
                    sizes,
                }))
            },
            sst_mutate: Mutex::new(()),
            block_cache,
            seq: AtomicU64::new(1),
            next_sst_id: AtomicU64::new(next_sst_id),
            wal: Arc::new(Mutex::new(wal)),
            external_seq: None,
        };

        // WAL 回放（幂等：以 seq 排序重放，同 key 后写覆盖先写）
        let max_seq = cf.replay_records(&wal_records)?;
        // 新写入 seq 必须接续已回放的最大 seq，避免同 key 版本冲突（重启恢复的正确性）。
        // 截断后重建的 WAL 含头（持久化 next_seq，M8-P5）且无回放记录 → 保持头值不覆盖。
        if max_seq > 0 {
            cf.wal.lock().unwrap().resume_seq(max_seq + 1);
        }
        Ok(cf)
    }

    /// 共享 WAL 句柄（组提交后台线程落盘兜底，M8）。
    pub fn wal_handle(&self) -> Arc<Mutex<WalBackend>> {
        Arc::clone(&self.wal)
    }

    /// 回放 WAL 记录：重建 MemTable 并推进 seq。返回已回放的最大 seq（无记录为 0）。
    /// TTL 启用时，已过期的记录（按文档 ttl_field 判断）不回放入 MemTable。
    fn replay_records(&mut self, recs: &[crate::wal::WalRecord]) -> Result<u64> {
        let mut max_seq = 0u64;
        for r in recs {
            match r.op {
                OP_PUT => {
                    if let Some(v) = &r.value {
                        if !self.is_ttl_expired(v) {
                            self.memtable.put(r.key.clone(), r.seq, v.clone());
                        }
                    }
                }
                OP_DELETE => self.memtable.delete(r.key.clone(), r.seq),
                other => return Err(Error::Corrupted(format!("WAL 未知 op {other}"))),
            }
            max_seq = max_seq.max(r.seq);
        }
        if !recs.is_empty() {
            self.seq.store(max_seq + 1, Ordering::Relaxed);
            info!(
                "列族 [{}] WAL 回放 {} 条，seq 推进至 {}",
                self.name,
                recs.len(),
                max_seq + 1
            );
        }
        Ok(max_seq)
    }

    /// TTL 过期判断：文档 ttl_field 对应桶天数早于截止天数（默认桶/不可解析永不过期）。
    fn is_ttl_expired(&self, value: &[u8]) -> bool {
        let Some(days) = self.document_bucket_days(value) else {
            return false;
        };
        let cutoff = today_epoch_days() - self.ttl_days.unwrap_or(0) as i64;
        days < cutoff
    }

    /// 写入（主键点写，便捷封装）。
    pub fn put(&mut self, docid: u64, value: Vec<u8>) -> Result<u64> {
        self.put_bytes(encode_docid(docid).to_vec(), value)
    }

    /// 写入原始字节键（组合索引等任意 key 使用）。
    pub fn put_bytes(&self, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        let seq = self.put_bytes_nosync(key, value)?;
        self.sync_wal()?;
        Ok(seq)
    }

    /// 批量写入（不逐条 fsync，由调用方最终 `sync_wal` 统一提交）。
    /// 供亿级数据压测/导入使用；强安全模式逐条写请用 `put_bytes`。
    pub fn put_bytes_nosync(&self, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        let seq = match self.wal_append(OP_PUT, &key, Some(&value)) {
            Ok(s) => s,
            // 环形 WAL 缓冲满：先落盘腾空（必要时强制 Flush）再重试
            Err(Error::WalFull(_)) => {
                self.ensure_wal_room()?;
                self.wal
                    .lock()
                    .unwrap()
                    .append(OP_PUT, &key, Some(&value))?
            }
            Err(e) => return Err(e),
        };
        self.memtable.put(key, seq, value);
        self.maybe_flush()?;
        Ok(seq)
    }

    /// 统一提交 WAL 缓冲（批量写入结束时调用）。
    /// 环形 WAL 落盘若需回绕覆盖未刷盘记录 → 强制 Flush 后重试。
    pub fn sync_wal(&self) -> Result<()> {
        let r = self.wal.lock().unwrap().sync();
        match r {
            Ok(()) => Ok(()),
            Err(Error::WalFull(_)) => {
                self.switch_and_flush()?;
                self.wal.lock().unwrap().sync()
            }
            Err(e) => Err(e),
        }
    }

    /// 删除（Tombstone，跨 flush/重启一致，见步骤 9）。
    pub fn delete(&self, docid: u64) -> Result<u64> {
        self.delete_bytes(encode_docid(docid).to_vec())
    }

    /// 仅写 WAL 删除记录（Ex-5.6 删除位图路径）：不写 memtable Tombstone、不逐条 fsync
    /// （由 `sync_wal`/组提交统一提交）——墓碑不进入 LSM 层级；
    /// 记录保留用于增量备份导出与崩溃回放（Engine 层回放转 `Engine::delete` 重新置位，幂等）。
    pub fn delete_record_wal(&self, key: Vec<u8>) -> Result<u64> {
        match self.wal_append(OP_DELETE, &key, None) {
            Ok(s) => Ok(s),
            Err(Error::WalFull(_)) => {
                self.ensure_wal_room()?;
                self.wal_append(OP_DELETE, &key, None)
            }
            Err(e) => Err(e),
        }
    }

    /// Ex-5.6 位图删除路径的**版本化删除**：WAL 删除记录 + **memtable Tombstone**（不逐条
    /// fsync，组提交统一提交）——缺陷 B/C4 修复：删除后 put 复活（清位图）时，快照读须按
    /// 版本判定"快照点位于[删除, 复活) → 不可见"。仅位图即时隐藏会在复活清位后丢失删除
    /// 信息，快照读回读到复活前旧版本 = 幻影。Tombstone 进版本链后该区间读恒为已删。
    pub fn delete_record_mem(&self, key: Vec<u8>) -> Result<u64> {
        let seq = match self.wal_append(OP_DELETE, &key, None) {
            Ok(s) => s,
            Err(Error::WalFull(_)) => {
                self.ensure_wal_room()?;
                self.wal_append(OP_DELETE, &key, None)?
            }
            Err(e) => return Err(e),
        };
        self.memtable.delete(key, seq);
        Ok(seq)
    }

    /// 删除原始字节键。
    pub fn delete_bytes(&self, key: Vec<u8>) -> Result<u64> {
        let seq = match self.wal_append(OP_DELETE, &key, None) {
            Ok(s) => s,
            Err(Error::WalFull(_)) => {
                self.ensure_wal_room()?;
                self.wal_append(OP_DELETE, &key, None)?
            }
            Err(e) => return Err(e),
        };
        self.sync_wal()?;
        self.memtable.delete(key, seq);
        Ok(seq)
    }

    /// 环形 WAL 满处理：先落盘缓冲；仍满（回绕需覆盖未刷盘记录）则强制 Flush 后重试。
    fn ensure_wal_room(&self) -> Result<()> {
        let r = self.wal.lock().unwrap().sync();
        match r {
            Ok(()) => Ok(()),
            Err(Error::WalFull(_)) => {
                self.switch_and_flush()?;
                self.wal.lock().unwrap().sync()
            }
            Err(e) => Err(e),
        }
    }

    /// 删除指定前缀的全部记录（阶段 1.5 Delta CF：全量 put 覆盖后清空该 docid 的增量）。
    /// 前缀上界 = prefix ++ [0xFF;4]（字段名长度前缀上限），闭区间扫描后逐条墓碑。
    pub fn delete_prefix(&self, prefix: &[u8]) -> Result<u64> {
        let mut end = prefix.to_vec();
        end.extend_from_slice(&[0xFF; 4]);
        let rows = self.scan_raw_range(Some(prefix), Some(&end))?;
        let mut deleted = 0u64;
        for (k, _) in rows {
            if k.starts_with(prefix) {
                self.delete_bytes(k)?;
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// 查询（主键点查，便捷封装）。返回 (value, seq)，已过滤 Tombstone。
    /// `&self`：读写分离读路径（Ex-7.1 PerCpuCounter / BlockCache 已内部同步）。
    /// P3-A：从 docid 提取 table_id（高 16 位），利用 L0 按表分组范围跳过非目标表 SST。
    pub fn get(&self, docid: u64) -> Result<Option<(Vec<u8>, u64)>> {
        let key = encode_docid(docid);
        if let Some(e) = self.memtable.get(&key) {
            return Ok(e.value.map(|v| (v, e.seq)));
        }
        let cache = Arc::clone(&self.block_cache);
        let snap = self.ssts.load();
        // P3-A：从 docid 提取 table_id（精确，非 docid 编码的 key 不适用）
        let tid = (docid >> 48) as u16;
        let key_bytes = key.as_slice();
        for (lv, idxs) in snap.layer_indices.iter().enumerate() {
            // 层级 Zone Map 粗筛（精确：层范围 = 层内各段范围并集）
            if let Some((lmin, lmax)) = &snap.layer_ranges[lv] {
                if key_bytes < lmin.as_slice() || key_bytes > lmax.as_slice() {
                    continue; // 整层跳过
                }
            }
            // P3-A：L0 按表分组范围——目标表在 L0 无数据则整层跳过
            if lv == 0 {
                if let Some(ref table_ranges) = snap.l0_table_ranges {
                    match table_ranges.get(&tid) {
                        Some((tmin, tmax)) => {
                            if key_bytes < tmin.as_slice() || key_bytes > tmax.as_slice() {
                                continue; // 该表在 L0 无此范围数据 → 整层跳过
                            }
                        }
                        None => continue, // 该 table_id 完全无 L0 SST → 整层跳过
                    }
                }
            }
            for &i in idxs {
                let sst = &snap.ssts[i];
                match get_from_sst(sst, &cache, key_bytes)? {
                    // 命中：最新版本。value=None 为 Tombstone → 视为不存在
                    Some((value, seq)) => return Ok(value.map(|v| (v, seq))),
                    None => continue, // 未命中该 SST，继续查更旧的
                }
            }
        }
        Ok(None)
    }

    /// 查询原始字节键。返回 (value, seq)，已过滤 Tombstone。
    /// R 项：按层遍历（L0→L1→L2，层序与"新→旧"一致）——每层先层级 Zone Map 粗筛
    /// （key 越出层范围 → 整层 O(1) 跳过，省逐段二分 + 布隆反序列化），层内逐段。
    /// 注意：非 docid 编码的 key 不适用 P3-A 的 L0 按表分组（因无法提取 table_id）。
    pub fn get_bytes(&self, key: &[u8]) -> Result<Option<(Vec<u8>, u64)>> {
        if let Some(e) = self.memtable.get(key) {
            return Ok(e.value.map(|v| (v, e.seq)));
        }
        let cache = Arc::clone(&self.block_cache);
        let snap = self.ssts.load();
        for (lv, idxs) in snap.layer_indices.iter().enumerate() {
            // 层级 Zone Map 粗筛（精确：层范围 = 层内各段范围并集）
            if let Some((lmin, lmax)) = &snap.layer_ranges[lv] {
                if key < lmin.as_slice() || key > lmax.as_slice() {
                    continue; // 整层跳过
                }
            }
            for &i in idxs {
                let sst = &snap.ssts[i];
                match get_from_sst(sst, &cache, key)? {
                    // 命中：最新版本。value=None 为 Tombstone → 视为不存在
                    Some((value, seq)) => return Ok(value.map(|v| (v, seq))),
                    None => continue, // 未命中该 SST，继续查更旧的
                }
            }
        }
        Ok(None)
    }

    /// 批量点查（N 项：借鉴 batch_get 建议落地，倒排/全文检索回表基础）。
    /// 语义与 `get_bytes` 一致：每 key 取最新版本（MemTable 优先，SST 新→旧首个命中即终），
    /// Tombstone 视为不存在；但一次处理多个 key——
    /// ① MemTable 批量命中（含 Tombstone 直接终结，跳过 SST 层）；
    /// ② 逐 SST：整文件布隆粗筛 → 逐 key 二分定位数据块 → 按块分组 →
    ///    每数据块仅读/解压/解码一次（块缓存复用），同块多 key 一次取出。
    /// 返回与输入 docids 顺序对齐的 `Vec<Option<(value, seq)>>`。
    pub fn get_many(&self, docids: &[u64]) -> Result<Vec<Option<(Vec<u8>, u64)>>> {
        let keys: Vec<Vec<u8>> = docids.iter().map(|d| encode_docid(*d).to_vec()).collect();
        let mut out: Vec<Option<(Vec<u8>, u64)>> = vec![None; keys.len()];
        // ① MemTable 批量（最新版本；Tombstone → 保持 None 并终结，不再查 SST）
        let mut remain: Vec<usize> = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            if let Some(e) = self.memtable.get(k) {
                if let Some(v) = e.value {
                    out[i] = Some((v, e.seq));
                }
            } else {
                remain.push(i);
            }
        }
        if remain.is_empty() {
            return Ok(out);
        }
        // ② 逐层（L0→L1→L2，层序与"新→旧"一致），层内逐 SST（新→旧），未命中 key 继续下沉
        let cache = Arc::clone(&self.block_cache);
        let snap = self.ssts.load();
        for (lv, idxs) in snap.layer_indices.iter().enumerate() {
            if remain.is_empty() {
                break;
            }
            // R 项：层级 Zone Map 粗筛——所有剩余 key 均越出层范围 → 整层跳过
            if let Some((lmin, lmax)) = &snap.layer_ranges[lv] {
                if remain
                    .iter()
                    .all(|&i| keys[i].as_slice() < lmin.as_slice() || keys[i].as_slice() > lmax.as_slice())
                {
                    continue;
                }
            }
            // P3-A：L0 按表分组范围——检查所有剩余 key 是否都越出各表范围 → 整层跳过
            if lv == 0 {
                if let Some(ref table_ranges) = snap.l0_table_ranges {
                    let all_out = remain.iter().all(|&i| {
                        let key = keys[i].as_slice();
                        match Self::table_id_from_key(key) {
                            Some(tid) => match table_ranges.get(&tid) {
                                Some((tmin, tmax)) => key < tmin.as_slice() || key > tmax.as_slice(),
                                None => true, // 该表无 L0 数据 → 这个 key out
                            },
                            None => false, // 无法提取 table_id → 保守不跳
                        }
                    });
                    if all_out {
                        continue; // 所有剩余 key 都 out → 整层跳过
                    }
                }
            }
            for &i in idxs {
                if remain.is_empty() {
                    break;
                }
                let hits = get_many_from_sst(&snap.ssts[i], &cache, &keys, &remain)?;
                let mut next = Vec::with_capacity(remain.len());
                for (j, &idx) in remain.iter().enumerate() {
                    match &hits[j] {
                        Some((Some(v), seq)) => out[idx] = Some((v.clone(), *seq)),
                        // Tombstone：该 SST 为最新版本且为删除 → 视为不存在
                        Some((None, _)) => {}
                        None => next.push(idx),
                    }
                }
                remain = next;
            }
        }
        Ok(out)
    }

    /// P87②：投影字段批量点查——语义与 `get_many` 一致（MemTable 优先 + SST 层
    /// 新→旧首个命中终结 / Tombstone 终止），但只返回每个 docid 的**请求字段值**：
    /// - MemTable / 行式块行：整行 JSON 按需字段提取（P86② 语义）；
    /// - PAX 块：`scan_block_for_keys_fields` 列解码直取（免整行 25 列重构）。
    /// 返回与输入 docids 顺序对齐的 `Vec<Option<(字段值列表, seq)>>`；外层 None =
    /// 该列族无此 key / 命中 Tombstone（调用方按 delete 语义处理，同 `get_many`）。
    pub fn get_many_fields(
        &self,
        docids: &[u64],
        fields: &[String],
    ) -> Result<Vec<Option<(Vec<Option<Vec<u8>>>, u64)>>> {
        let keys: Vec<Vec<u8>> = docids.iter().map(|d| encode_docid(*d).to_vec()).collect();
        let mut out: Vec<Option<(Vec<Option<Vec<u8>>>, u64)>> = vec![None; keys.len()];
        // ① MemTable 批量（字段从整行 JSON 提取；Tombstone → 保持 None 并终结）
        let mut remain: Vec<usize> = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            if let Some(e) = self.memtable.get(k) {
                if let Some(v) = e.value {
                    let vals = crate::sstable::extract_fields_from_json_row(&v, fields);
                    out[i] = Some((vals, e.seq));
                }
            } else {
                remain.push(i);
            }
        }
        if remain.is_empty() {
            return Ok(out);
        }
        // ② 逐层（L0→L1→L2），层内逐 SST（新→旧），未命中 key 继续下沉（同 get_many）
        let cache = Arc::clone(&self.block_cache);
        let snap = self.ssts.load();
        for (lv, idxs) in snap.layer_indices.iter().enumerate() {
            if remain.is_empty() {
                break;
            }
            if let Some((lmin, lmax)) = &snap.layer_ranges[lv] {
                if remain
                    .iter()
                    .all(|&i| keys[i].as_slice() < lmin.as_slice() || keys[i].as_slice() > lmax.as_slice())
                {
                    continue;
                }
            }
            if lv == 0 {
                if let Some(ref table_ranges) = snap.l0_table_ranges {
                    let all_out = remain.iter().all(|&i| {
                        let key = keys[i].as_slice();
                        match Self::table_id_from_key(key) {
                            Some(tid) => match table_ranges.get(&tid) {
                                Some((tmin, tmax)) => key < tmin.as_slice() || key > tmax.as_slice(),
                                None => true,
                            },
                            None => false,
                        }
                    });
                    if all_out {
                        continue;
                    }
                }
            }
            for &i in idxs {
                if remain.is_empty() {
                    break;
                }
                let hits = get_many_fields_from_sst(&snap.ssts[i], &cache, &keys, &remain, fields)?;
                let mut next = Vec::with_capacity(remain.len());
                for (j, &idx) in remain.iter().enumerate() {
                    match &hits[j] {
                        Some((Some(vals), seq)) => out[idx] = Some((vals.clone(), *seq)),
                        // Tombstone：该 SST 为最新版本且为删除 → 视为不存在
                        Some((None, _)) => {}
                        None => next.push(idx),
                    }
                }
                remain = next;
            }
        }
        Ok(out)
    }

    /// 快照读（design 4.7 二期 MVCC，M6-3）：返回 **seq ≤ `snapshot_seq`** 的最新版本。
    /// 遍历 MemTable + 全部 SST，取满足条件的最大 seq；该 seq 为 Tombstone 则视为不存在
    /// （快照点已删除；快照点之前的历史版本仍可见）。
    /// 局限：MemTable 仅保留每 key 最新版本，未刷盘覆盖的历史版本无法回读（多版本保留留后续）。
    pub fn get_bytes_at(
        &self,
        key: &[u8],
        snapshot_seq: u64,
    ) -> Result<Option<(Vec<u8>, u64)>> {
        let mut best: Option<(u64, Option<Vec<u8>>)> = None; // (seq, value)
        // S 项：MemTable 多版本——取 seq ≤ snapshot 的最新版本（未刷盘也可正确快照读）
        if let Some(e) = self.memtable.get_at(key, snapshot_seq) {
            if best.as_ref().map_or(true, |(s, _)| e.seq > *s) {
                best = Some((e.seq, e.value));
            }
        }
        let cache = Arc::clone(&self.block_cache);
        // R 项：按层遍历（快照语义取全部层中 seq ≤ snapshot 的最大版本）；层级粗筛跳过
        // key 越界的层（层范围 = 精确并集，不产生假阴性）。
        let snap = self.ssts.load();
        for (lv, idxs) in snap.layer_indices.iter().enumerate() {
            if let Some((lmin, lmax)) = &snap.layer_ranges[lv] {
                if key < lmin.as_slice() || key > lmax.as_slice() {
                    continue;
                }
            }
            for &i in idxs {
                // Ex-8.6：文件最小行 seq > 快照 → 整段剪枝（段内无 ≤ 快照的 put/Tombstone）
                if snapshot_seq != u64::MAX && self.sst_min_seq(&snap.ssts[i])? > snapshot_seq {
                    continue;
                }
                if let Some((value, seq)) = get_from_sst_at(&snap.ssts[i], &cache, key, snapshot_seq)?
                {
                    if best.as_ref().map_or(true, |(s, _)| seq > *s) {
                        best = Some((seq, value));
                    }
                }
            }
        }
        match best {
            Some((seq, Some(v))) => Ok(Some((v, seq))),
            Some((_, None)) => Ok(None), // 快照点已删除
            None => Ok(None),
        }
    }

    /// Ex-8.6：该文件**所有行**（put + Tombstone）的最小 seq——惰性 keys-only 推导 + 记忆。
    /// 快照读剪枝：`快照 < 文件最小行 seq` → 文件内既无 ≤ 快照的 put 也无 ≤ 快照的 Tombstone，
    /// 对快照视图贡献为空（含墓碑掩蔽），可整段跳过。未知文件首次调用做一次全文件 keys-only
    /// 扫描（一次性成本 ≈ COUNT 免值计数；重启后首次快照读自动重建，无需 manifest 扩展）。
    fn sst_min_seq(&self, sst: &SstReader) -> Result<u64> {
        let p = sst.path();
        {
            let m = self.seq_min.read().unwrap();
            if let Some(v) = m.get(p) {
                return Ok(*v);
            }
        }
        let mut mn = u64::MAX;
        let mut it = crate::sstable::SstRangeIter::new_keys(sst, None, None)?;
        while let Some((_k, _v, seq)) = it.next().transpose()? {
            if seq < mn {
                mn = seq;
            }
        }
        let v = if mn == u64::MAX { u64::MAX } else { mn };
        self.seq_min.write().unwrap().insert(p.to_path_buf(), v);
        Ok(v)
    }

    /// 范围扫描 [start, end]（闭区间，None 端无边界）：先收集 MemTable 与各 SST 候选，
    /// 以最大 seq 去重（最新覆盖，Tombstone 覆盖旧值），返回按 docid 升序的 (docid, value) 列表。
    /// O 项第①步：读路径 `&self` 化（范围扫描共用，配合 RwLock 读读并行）。
    pub fn scan_range(
        &self,
        start: Option<u64>,
        end: Option<u64>,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let start_key = start.map(|s| encode_docid(s).to_vec());
        let end_key = end.map(|e| encode_docid(e).to_vec());
        let rows = self.scan_raw_range(start_key.as_deref(), end_key.as_deref())?;
        let mut out = Vec::with_capacity(rows.len());
        for (k, v) in rows {
            out.push((
                decode_docid(&k).map_err(|_| Error::Corrupted("docid 解码失败".into()))?,
                v,
            ));
        }
        Ok(out)
    }

    /// 快照范围扫描 [start, end]（M 项，事务类查询优化 P0）：按快照 seq 过滤版本，
    /// 返回按 docid 升序的 (docid, value) 列表（快照点已删除的 key 跳过）。
    pub fn scan_range_at(
        &self,
        snapshot_seq: u64,
        start: Option<u64>,
        end: Option<u64>,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let start_key = start.map(|s| encode_docid(s).to_vec());
        let end_key = end.map(|e| encode_docid(e).to_vec());
        let mut out = Vec::new();
        self.scan_stream_at(snapshot_seq, start_key.as_deref(), end_key.as_deref(), None, |key, val| {
            let docid = decode_docid(key)
                .map_err(|_| Error::Corrupted("scan_at key 非 docid 编码".into()))?;
            out.push((docid, val.to_vec()));
            Ok(true)
        })?;
        Ok(out)
    }

    /// 原始字节键范围扫描（组合索引前缀查询使用）。返回升序 (key, value) 列表，Tombstone 已过滤。
    /// O 项第①步：原始键范围扫描读路径 `&self` 化（delta Merge-on-Read / 批量覆盖共用）。
    pub fn scan_raw_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // 候选收集：key → (seq, value)，value=None 表示 Tombstone
        let mut merged: std::collections::HashMap<Vec<u8>, (u64, Option<Vec<u8>>)> =
            std::collections::HashMap::new();

        // MemTable 扫描（含 Tombstone 覆盖）
        self.memtable.scan_range(start, end, |key, e| {
            merge_candidate_bytes(&mut merged, key.to_vec(), e.seq, e.value.clone());
        });

        // SST 范围扫描（Zone Map 剪枝已内置在 scan_range；Ex-8.2 先按段 key 范围跳过不相交段，
        // 免调 sst.scan_range 的线性索引走查开销）
        for sst in self.ssts.load().ssts.iter() {
            if !sst_intersects_window(sst, start, end) {
                continue;
            }
            sst.scan_range(start, end, |k, v, seq| {
                merge_candidate_bytes(&mut merged, k.to_vec(), seq, v.map(|x| x.to_vec()));
            })?;
        }

        let mut out: Vec<(Vec<u8>, Vec<u8>)> = merged
            .into_iter()
            .filter_map(|(key, (_seq, value))| value.map(|v| (key, v)))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// 流式范围扫描（M8-P10）：k-way merge（memtable 双缓冲 + 各 SST 有序源）按 key 升序
    /// 回调 `f(key, value)`——同 key 取最大 seq（最新版本），Tombstone 跳过。
    /// 回调返回 `bool`：true=继续，**false=提前终止**（M8-P11 游标续扫：取满页即停，
    /// 不再全扫计数 total）。内存 O(块) 不随扫描总量膨胀，语义与 `scan_raw_range` 一致。
    pub fn scan_stream<F: FnMut(&[u8], &[u8]) -> Result<bool>>(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        f: F,
    ) -> Result<()> {
        // 最新视图 = 快照 seq 无上限（取最大版本）
        self.scan_stream_at(u64::MAX, start, end, None, f)
    }

    /// P1-E：带 Zone Map 字段级范围剪枝的流式扫描——与 `scan_stream` 语义一致，
    /// 额外在 SST 块级检查 `zone_pred` 范围，不相交块跳过（免 IO/解压）。
    pub fn scan_stream_with_zonepred<F: FnMut(&[u8], &[u8]) -> Result<bool>>(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        zone_pred: Option<crate::sstable::ZonePredicate>,
        f: F,
    ) -> Result<()> {
        // 最新视图 = 快照 seq 无上限（取最大版本）
        self.scan_stream_at(u64::MAX, start, end, zone_pred, f)
    }

    /// 快照范围流式扫描（M 项，事务类查询优化 P0）：同 key 多版本取
    /// **seq ≤ snapshot_seq 的最大版本**（对齐 `get_bytes_at` 快照语义）；
    /// 快照点前为删除（Tombstone）→ 跳过该 key。其余与 `scan_stream` 一致。
    pub fn scan_stream_at<F: FnMut(&[u8], &[u8]) -> Result<bool>>(
        &self,
        snapshot_seq: u64,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        zone_pred: Option<crate::sstable::ZonePredicate>,
        mut f: F,
    ) -> Result<()> {
        // 源：memtable（immutable + mutable）+ SST。P72：memtable 迭代器借用内部 RwLock 读锁
        // 数据 → HRTB 闭包式（merge 主体在锁作用域内执行）；SST 借用 &self.ssts。
        self.memtable.with_iter_range(start, end, |mut mem_iters| {
            let mut sst_iters: Vec<crate::sstable::SstRangeIter> = Vec::new();
            let snap = self.ssts.load();
            for sst in snap.ssts.iter() {
                // Ex-8.2：scan 路径段级 key 范围剪枝——窗口不相交的段免建迭代器
                // （此前无条件对快照全部 SST 建迭代器，无层/文件粗筛）
                if !sst_intersects_window(sst, start, end) {
                    continue;
                }
                // Ex-8.6：文件最小行 seq > 快照 → 整段剪枝（段内无 ≤ 快照的行，快照贡献为空）
                if snapshot_seq != u64::MAX && self.sst_min_seq(sst)? > snapshot_seq {
                    continue;
                }
                let mut it = crate::sstable::SstRangeIter::new_cached(
                    sst,
                    start,
                    end,
                    std::sync::Arc::clone(&self.block_cache),
                )?;
                if let Some(ref zp) = zone_pred {
                    it.set_zone_pred(zp.clone());
                }
                sst_iters.push(it);
            }
            let mem_count = mem_iters.len();
            let total = mem_count + sst_iters.len();
            if total == 0 {
                return Ok(());
            }
            // 各源当前条目 (key, value, seq)
            let mut cur: Vec<Option<(Vec<u8>, Option<Vec<u8>>, u64)>> =
                Vec::with_capacity(total);
            for it in mem_iters.iter_mut() {
                cur.push(it.next().transpose()?);
            }
            for it in sst_iters.iter_mut() {
                cur.push(it.next().transpose()?);
            }
            // 7.98：小源数（≤4，典型 1 SST + memtable 全扫）线性取最小——heap push/pop
            // 每行 log K 结构开销（1100 万行全扫 ~1s）；线性逐源比较 K 次常数极小。
            // 源多（多 L0/层）仍用最小堆避免 O(N·K) 退化。
            if total <= 4 {
                loop {
                    // 找最小 key 的源
                    let mut min_src: Option<usize> = None;
                    for (i, c) in cur.iter().enumerate() {
                        if let Some((k, _, _)) = c {
                            match min_src {
                                None => min_src = Some(i),
                                Some(mi) => {
                                    if k < &cur[mi].as_ref().unwrap().0 {
                                        min_src = Some(i);
                                    }
                                }
                            }
                        }
                    }
                    let Some(i0) = min_src else { break };
                    let min_key = cur[i0].as_ref().unwrap().0.clone();
                    // Ex-8.1（demo range-window）：收集同 key 候选须**吞并同源连续多版本行**
                    // ——S 项 MemTable 多版本下，覆盖写未收敛刷盘会让同一源连续出现同 key 新旧两行；
                    // 仅取各源首行会把旧版本当下一个"新 key"再输出（收集 100 vs 流式 110）。
                    // frontier 循环：取走所有 key==min_key 的行取快照点前最大 seq，推进后若该源
                    // 仍指向 min_key（更旧版本）则继续吞并，直至全部源越过该 key。
                    let mut frontier: Vec<usize> = (0..total)
                        .filter(|i| matches!(&cur[*i], Some((k, _, _)) if *k == min_key))
                        .collect();
                    let mut best_seq = 0u64;
                    let mut best_val: Option<Vec<u8>> = None;
                    loop {
                        let mut nxt: Vec<usize> = Vec::new();
                        for i in frontier {
                            let (k, v, seq) = cur[i].take().unwrap();
                            debug_assert!(k == min_key, "同 key 归并");
                            if seq <= snapshot_seq && seq >= best_seq {
                                best_seq = seq;
                                best_val = v;
                            }
                            cur[i] = if i < mem_count {
                                mem_iters[i].next().transpose()?
                            } else {
                                sst_iters[i - mem_count].next().transpose()?
                            };
                            if matches!(&cur[i], Some((nk, _, _)) if *nk == min_key) {
                                nxt.push(i); // 同源更旧版本，下一轮吞并
                            }
                        }
                        if nxt.is_empty() {
                            break;
                        }
                        frontier = nxt;
                    }
                    if let Some(v) = best_val {
                        if let Some(limiter) = self.scan_limiter.lock().unwrap().as_mut() {
                            limiter.acquire(v.len() as u64)?;
                        }
                        if !f(min_key.as_slice(), &v)? {
                            break; // 提前终止
                        }
                    }
                }
                return Ok(());
            }
            // k-way merge 用最小堆（O(N log K)，避免每轮线性扫全部源 O(N·K)——K 大时不可接受）
            let mut heap: std::collections::BinaryHeap<std::cmp::Reverse<(Vec<u8>, usize)>> =
                std::collections::BinaryHeap::new();
            for (i, c) in cur.iter().enumerate() {
                if let Some((k, _, _)) = c {
                    heap.push(std::cmp::Reverse((k.clone(), i)));
                }
            }
            loop {
                let Some(std::cmp::Reverse((min_key, i0))) = heap.pop() else {
                    break; // 全部耗尽
                };
                // 收集所有 key == min_key 的源（同 key 候选），取快照点前最大 seq（最新可见版本）
                let mut to_advance: Vec<usize> = vec![i0];
                while let Some(std::cmp::Reverse((k, i))) = heap.peek() {
                    if *k == min_key {
                        to_advance.push(*i);
                        heap.pop();
                    } else {
                        break;
                    }
                }
                let mut best_seq = 0u64;
                let mut best_val: Option<Vec<u8>> = None;
                let mut advanced: Vec<usize> = Vec::new();
                // Ex-8.1（demo range-window）：吞并同源连续同 key 多版本行（同线性分支）
                let mut frontier = to_advance;
                loop {
                    let mut nxt: Vec<usize> = Vec::new();
                    for i in frontier {
                        let (k, v, seq) = cur[i].take().unwrap();
                        debug_assert!(k == min_key, "同 key 归并");
                        if seq <= snapshot_seq && seq >= best_seq {
                            best_seq = seq;
                            best_val = v;
                        }
                        cur[i] = if i < mem_count {
                            mem_iters[i].next().transpose()?
                        } else {
                            sst_iters[i - mem_count].next().transpose()?
                        };
                        advanced.push(i);
                        if matches!(&cur[i], Some((nk, _, _)) if *nk == min_key) {
                            nxt.push(i);
                        }
                    }
                    if nxt.is_empty() {
                        break;
                    }
                    frontier = nxt;
                }
                // 推进后的源重新入堆（其残余 key 已越过 min_key）；同一源多轮吞并只保留最终游标
                advanced.sort_unstable();
                advanced.dedup();
                for i in advanced {
                    if let Some((nk, _, _)) = &cur[i] {
                        heap.push(std::cmp::Reverse((nk.clone(), i)));
                    }
                }
                // 快照点前最新版本为 Tombstone → 该 key 在快照点已删除，不输出
                if let Some(v) = best_val {
                    // 导出共享后台 IO 限速（design 20.5）：扫描产出按字节 acquire——
                    // 扫描节奏受限 → SST 顺序读 IO 随节奏受限，与 Compaction 共享后台预算
                    if let Some(limiter) = self.scan_limiter.lock().unwrap().as_mut() {
                        limiter.acquire(v.len() as u64)?;
                    }
                    if !f(min_key.as_slice(), &v)? {
                        break; // 提前终止（游标续扫取满页）
                    }
                }
            }
            Ok(())
        })
    }

    /// 7.100 快速行计数（COUNT(*) 无 WHERE 快路径）——无删除位图过滤版本，等价于
    /// `count_keys_range_filtered(.., 永不跳过)`。
    pub fn count_keys_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<u64> {
        self.count_keys_range_filtered(start, end, &mut |_| false)
    }

    /// Ex-8.1：带 skip 谓词的 keys-only 计数（删除位图过滤用）。merge 版本语义同
    /// `count_keys_range`（同 key 取最新、Tombstone 跳过、同源多版本折叠）。
    pub fn count_keys_range_filtered(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        skip: &mut dyn FnMut(&[u8]) -> bool,
    ) -> Result<u64> {
        self.memtable.with_iter_range(start, end, |mut mem_iters| {
            let mut sst_iters: Vec<crate::sstable::SstRangeIter> = Vec::new();
            let snap = self.ssts.load();
            for sst in snap.ssts.iter() {
                // Ex-8.2：同 scan_stream_at，窗口不相交的段免建 keys-only 迭代器
                if !sst_intersects_window(sst, start, end) {
                    continue;
                }
                sst_iters.push(crate::sstable::SstRangeIter::new_keys_cached(
                    sst,
                    start,
                    end,
                    std::sync::Arc::clone(&self.block_cache),
                )?);
            }
            let mem_count = mem_iters.len();
            let total = mem_count + sst_iters.len();
            if total == 0 {
                return Ok(0u64);
            }
            let mut cur: Vec<Option<(Vec<u8>, Option<Vec<u8>>, u64)>> =
                Vec::with_capacity(total);
            for it in mem_iters.iter_mut() {
                cur.push(it.next().transpose()?);
            }
            for it in sst_iters.iter_mut() {
                cur.push(it.next().transpose()?);
            }
            let mut count = 0u64;
            loop {
                // 线性取最小（同 7.99 merge：源少时省 heap 结构开销）
                let mut min_src: Option<usize> = None;
                for (i, c) in cur.iter().enumerate() {
                    if let Some((k, _, _)) = c {
                        match min_src {
                            None => min_src = Some(i),
                            Some(mi) => {
                                if k < &cur[mi].as_ref().unwrap().0 {
                                    min_src = Some(i);
                                }
                            }
                        }
                    }
                }
                let Some(i0) = min_src else { break };
                let min_key = cur[i0].as_ref().unwrap().0.clone();
                let mut best_seq = 0u64;
                let mut best_put = false;
                // Ex-8.1（demo range-window）：同 scan merge，吞并同源连续同 key 多版本行，
                // 避免覆盖写未收敛刷盘时旧版本被重复计数（COUNT 语义与 scan/get 对齐）。
                let mut frontier: Vec<usize> = (0..total)
                    .filter(|i| matches!(&cur[*i], Some((k, _, _)) if *k == min_key))
                    .collect();
                loop {
                    let mut nxt: Vec<usize> = Vec::new();
                    for i in frontier {
                        let (k, v, seq) = cur[i].take().unwrap();
                        debug_assert!(k == min_key, "同 key 归并");
                        // P1-2（Tombstone 折叠修复）：取**最大 seq 版本**判定可见性——
                        // 旧实现"遇到任一 Some 即 best_put"，高 seq 的 None（删除墓碑）
                        // 被忽略 → 已删 key 仍被 keys-only 输出/计数（DELETE 后纯 id
                        // 窗口 / COUNT 可见性错误，非事务 Tombstone 路径实测暴露）。
                        if seq > best_seq {
                            best_seq = seq;
                            best_put = v.is_some();
                        }
                        cur[i] = if i < mem_count {
                            mem_iters[i].next().transpose()?
                        } else {
                            sst_iters[i - mem_count].next().transpose()?
                        };
                        if matches!(&cur[i], Some((nk, _, _)) if *nk == min_key) {
                            nxt.push(i);
                        }
                    }
                    if nxt.is_empty() {
                        break;
                    }
                    frontier = nxt;
                }
                if best_put && !skip(min_key.as_slice()) {
                    count += 1;
                }
            }
            Ok(count)
        })
    }

    /// Ex-8.3 Part B：keys-only 流式扫描（最新视图）——merge 版本语义同 `count_keys_range`
    /// （同源同 key 折叠、Tombstone 跳过），但**输出可见 key 序列**而非计数：SST 端走
    /// `SstRangeIter::new_keys_cached` 免值解码（+块缓存）；回调返回 false 提前终止
    /// （mysql 纯 id 窗口 / LIMIT 截断，内存 O(输出)）。
    pub fn scan_stream_keys<F: FnMut(&[u8]) -> Result<bool>>(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        mut f: F,
    ) -> Result<()> {
        self.memtable.with_iter_range(start, end, |mut mem_iters| {
            let mut sst_iters: Vec<crate::sstable::SstRangeIter> = Vec::new();
            let snap = self.ssts.load();
            for sst in snap.ssts.iter() {
                if !sst_intersects_window(sst, start, end) {
                    continue;
                }
                sst_iters.push(crate::sstable::SstRangeIter::new_keys_cached(
                    sst,
                    start,
                    end,
                    std::sync::Arc::clone(&self.block_cache),
                )?);
            }
            let mem_count = mem_iters.len();
            let total = mem_count + sst_iters.len();
            if total == 0 {
                return Ok(());
            }
            let mut cur: Vec<Option<(Vec<u8>, Option<Vec<u8>>, u64)>> = Vec::with_capacity(total);
            for it in mem_iters.iter_mut() {
                cur.push(it.next().transpose()?);
            }
            for it in sst_iters.iter_mut() {
                cur.push(it.next().transpose()?);
            }
            loop {
                let mut min_src: Option<usize> = None;
                for (i, c) in cur.iter().enumerate() {
                    if let Some((k, _, _)) = c {
                        match min_src {
                            None => min_src = Some(i),
                            Some(mi) => {
                                if k < &cur[mi].as_ref().unwrap().0 {
                                    min_src = Some(i);
                                }
                            }
                        }
                    }
                }
                let Some(i0) = min_src else { break };
                let min_key = cur[i0].as_ref().unwrap().0.clone();
                let mut best_seq = 0u64;
                let mut best_put = false;
                // 同 count：frontier 吞并同源连续同 key 多版本行（Ex-8.1 折叠）
                let mut frontier: Vec<usize> = (0..total)
                    .filter(|i| matches!(&cur[*i], Some((k, _, _)) if *k == min_key))
                    .collect();
                loop {
                    let mut nxt: Vec<usize> = Vec::new();
                    for i in frontier {
                        let (k, v, seq) = cur[i].take().unwrap();
                        debug_assert!(k == min_key, "同 key 归并");
                        // P1-2：同 count_keys_range——最大 seq 版本为 Tombstone 则跳过
                        if seq > best_seq {
                            best_seq = seq;
                            best_put = v.is_some();
                        }
                        cur[i] = if i < mem_count {
                            mem_iters[i].next().transpose()?
                        } else {
                            sst_iters[i - mem_count].next().transpose()?
                        };
                        if matches!(&cur[i], Some((nk, _, _)) if *nk == min_key) {
                            nxt.push(i);
                        }
                    }
                    if nxt.is_empty() {
                        break;
                    }
                    frontier = nxt;
                }
                if best_put {
                    if !f(min_key.as_slice())? {
                        return Ok(()); // 提前终止（LIMIT 截断）
                    }
                }
            }
            Ok(())
        })
    }

    /// 范围扫描并保留 seq 与 Tombstone（MVCC 快照 Delta 隔离用，M7-1）：
    /// 返回升序 `(key, seq, value)`，value=None 表示删除标记；每 key 仅保留最大 seq 版本。
    pub fn scan_raw_range_with_seq(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, u64, Option<Vec<u8>>)>> {
        let mut merged: std::collections::HashMap<Vec<u8>, (u64, Option<Vec<u8>>)> =
            std::collections::HashMap::new();
        self.memtable.scan_range(start, end, |key, e| {
            merge_candidate_bytes(&mut merged, key.to_vec(), e.seq, e.value.clone());
        });
        for sst in self.ssts.load().ssts.iter() {
            sst.scan_range(start, end, |k, v, seq| {
                merge_candidate_bytes(&mut merged, k.to_vec(), seq, v.map(|x| x.to_vec()));
            })?;
        }
        let mut out: Vec<(Vec<u8>, u64, Option<Vec<u8>>)> = merged
            .into_iter()
            .map(|(key, (seq, value))| (key, seq, value))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// MemTable 超阈值 → 冻结并刷盘（MVP 同步刷盘）。
    /// P72：`&self`（Engine 字段 Arc<ColumnFamily> 化后 flush 无法取 &mut）。
    fn maybe_flush(&self) -> Result<()> {
        if self.memtable.mutable_bytes() < self.cfg.max_size_mb * 1024 * 1024 {
            return Ok(());
        }
        self.switch_and_flush()
    }

    /// 冻结 Mutable → 刷盘为 SST → 更新 Manifest → 释放 Immutable。
    /// P72：`&self`——memtable 冻结经内部 RwLock 写锁；ssts 发布经 `sst_mutate` 互斥。
    pub fn switch_and_flush(&self) -> Result<()> {
        self.memtable.switch();
        let Some(imm) = self.memtable.take_immutable() else {
            return Ok(());
        };
        // X 项：flush 计数（写路径刷盘指标）
        self.flush_counter.fetch_add(1, Ordering::Relaxed);
        // 环形 WAL 覆盖安全：Flush 完成后上报 imm 内最大 seq（<= 该 seq 的记录已刷盘可覆盖）
        let flushed_max = imm_scan_max_seq(&imm);
        let new_segments = if self.ttl_days.is_none() {
            // M3：主数据列族（多表）按表切分输出；其余单文件（行为不变）
            if self.split_by_table {
                self.flush_by_table(&imm)?;
                // 记录 flush_by_table 创建的 SST 数（snapshot_insert 中计数）
                self.flush_sst_count.load(Ordering::Relaxed)
            } else {
                self.flush_single(&imm)?;
                1
            }
        } else {
            self.flush_buckets(&imm)?;
            // 记录 flush_buckets 创建的 SST 数
            self.flush_sst_count.load(Ordering::Relaxed)
        };
        // P4-A：记录本次 flush 新增 L0 段数 → 写入速率自适应
        self.record_flush_new_l0(new_segments);
        self.wal.lock().unwrap().set_flushed_seq(flushed_max);
        // WAL 截断（M8-P5）：append 模式 flush 后全部记录已刷盘，清空 WAL 保持小文件
        // （避免无限增长 + 大文件 fsync 拖慢写入）；ring 模式自带覆盖回收（no-op）
        self.wal.lock().unwrap().truncate_and_reset()?;
        Ok(())
    }

    /// X 项：本列族累计刷盘次数（写路径 flush 指标）。
    pub fn flush_count(&self) -> u64 {
        self.flush_counter.load(Ordering::Relaxed)
    }

    /// Ex-8.11：累计写入 SST 字节（flush/compact 新建文件字节和；写放大实验数据源）。
    pub fn sst_written_bytes(&self) -> u64 {
        self.sst_written.load(Ordering::Relaxed)
    }

    /// Ex-8.11：L0/L1/L2 段数分布（写放大 A/B 观察合并节奏）。
    pub fn layer_counts(&self) -> (usize, usize, usize) {
        let snap = self.ssts.load();
        let mut c = (0usize, 0usize, 0usize);
        for lv in snap.levels.iter() {
            match *lv {
                0 => c.0 += 1,
                1 => c.1 += 1,
                _ => c.2 += 1,
            }
        }
        c
    }

    /// 从 docid 编码 key 中提取 table_id（高 16 位）。
    /// key 为 encode_docid 的 8 字节大端编码，前 2 字节 = table_id。
    fn table_id_from_key(key: &[u8]) -> Option<u16> {
        if key.len() >= 2 {
            Some(u16::from_be_bytes([key[0], key[1]]))
        } else {
            None
        }
    }

    /// P3-A：构建 L0 层按表分组的范围——遍历 L0 SST，按 table_id 聚合 key 范围。
    /// 每个 SST 只含单表数据（M3 flush/compact 按表切分），所以从 key_range 的 min key
    /// 提取 table_id 即可确定所属表。
    fn build_l0_table_ranges(
        ssts: &[Arc<SstReader>],
        l0_indices: &[usize],
    ) -> Option<std::collections::HashMap<u16, (Vec<u8>, Vec<u8>)>> {
        if l0_indices.is_empty() {
            return None;
        }
        let mut table_ranges: std::collections::HashMap<u16, (Vec<u8>, Vec<u8>)> =
            std::collections::HashMap::new();
        for &i in l0_indices {
            let s = &ssts[i];
            let (min, max) = match s.key_range() {
                Some((mn, mx)) => (mn, mx),
                None => return None, // 无范围段 → 无法构建，回退 None
            };
            // 从 min key 提取 table_id（M3 保证每文件单表）
            let tid = match Self::table_id_from_key(min) {
                Some(t) => t,
                None => return None,
            };
            match table_ranges.entry(tid) {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    let (lo, hi) = e.get_mut();
                    if min < lo.as_slice() {
                        *lo = min.to_vec();
                    }
                    if max > hi.as_slice() {
                        *hi = max.to_vec();
                    }
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert((min.to_vec(), max.to_vec()));
                }
            }
        }
        Some(table_ranges)
    }

    /// R 项：快照构建时计算层聚合元数据（O(段数)）——每层 key 范围 + 每层段下标。
    /// 层范围 = 层内各段范围并集；层内存在"无范围段"（无约束）→ 该层 None（不可跳过，
    /// 防假阴性——布隆/范围粗筛只允许假阳性，不允许假阴性）。
    fn build_layer_meta(
        ssts: &[Arc<SstReader>],
        levels: &[u32],
    ) -> (
        Vec<Option<(Vec<u8>, Vec<u8>)>>,
        Vec<Vec<usize>>,
        Option<std::collections::HashMap<u16, (Vec<u8>, Vec<u8>)>>,
    ) {
        let mut ranges: Vec<Option<(Vec<u8>, Vec<u8>)>> = vec![None, None, None];
        let mut indices: Vec<Vec<usize>> = vec![Vec::new(), Vec::new(), Vec::new()];
        for (i, (s, lv)) in ssts.iter().zip(levels).enumerate() {
            let lv = (*lv as usize).min(2);
            indices[lv].push(i);
            match s.key_range() {
                Some((min, max)) => match &mut ranges[lv] {
                    Some((lo, hi)) => {
                        if min < lo.as_slice() {
                            *lo = min.to_vec();
                        }
                        if max > hi.as_slice() {
                            *hi = max.to_vec();
                        }
                    }
                    None => ranges[lv] = Some((min.to_vec(), max.to_vec())),
                },
                // 段无范围（空段/无索引）→ 层不可粗筛跳过（保守，防假阴性）
                None => ranges[lv] = None,
            }
        }
        // P3-A：L0 按表分组范围
        let l0_table_ranges = Self::build_l0_table_ranges(ssts, &indices[0]);
        (ranges, indices, l0_table_ranges)
    }

    /// O 项第③步：原子插入新 SST（快照 `store()`）——新文件插最前（读路径优先命中），层号 L0。
    /// Ex-8.11：每次 flush 落一个新文件 → 累计写入字节（写放大实验数据源）。
    fn snapshot_insert(&self, reader: SstReader) {
        self.sst_written
            .fetch_add(reader.file_len(), Ordering::Relaxed);
        let cur = self.ssts.load();
        let mut ssts = cur.ssts.clone();
        ssts.insert(0, Arc::new(reader));
        let mut levels = cur.levels.clone();
        levels.insert(0, 0);
        let (layer_ranges, layer_indices, l0_table_ranges) = Self::build_layer_meta(&ssts, &levels);
        let sizes: Vec<u64> = ssts.iter().map(|r| r.file_len()).collect();
        self.ssts.store(Arc::new(SstSnapshot {
            ssts,
            levels,
            layer_ranges,
            layer_indices,
            l0_table_ranges,
            sizes,
        }));
    }

    /// 原逻辑：整个 Immutable 落盘为单个 SST。
    /// P72/P73：`&self` + `sst_mutate` 锁全程持锁——id 分配、文件写入、store、manifest 原子
    /// （无锁合并并发时：id 无重复、persist 不引用半写段）。
    fn flush_single(&self, imm: &MemTable) -> Result<()> {
        let _g = self.sst_mutate.lock().unwrap();
        let sst_id = self.next_sst_id.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{SST_PREFIX}{sst_id:08}.sst"));
        self.write_sst(&path, imm)?;

        // 新文件插到最前（读路径优先命中）
        let fname = path.file_name().unwrap().to_string_lossy().to_string();
        self.io_acquire(&path)?;
        let reader = SstReader::open_with_granularity(&path, self.index_granularity)?;
        self.snapshot_insert(reader);
        self.persist_manifest()?;
        drop(_g);
        info!(
            "列族 [{}] 刷盘完成: {} ({} 条)",
            self.name,
            fname,
            imm.len()
        );
        Ok(())
    }

    /// M3（§26 多表，实施清单①）：Immutable **按表切分**落盘——遍历升序键流，
    /// 检测 `docid >> 48`（table_id）变化即 Finish 当前 writer、开新 writer（每表一个 SST）。
    /// 前提：docid 高位编码 → 同表键在扫描序中连续，表边界检测即文件边界；
    /// 键非 8 字节定长（理论上不会出现在开启切分的列族）保守并入 table 0。
    /// 单表（table_id=0，含旧库全量）→ 仅开一个 writer = 单文件，与 flush_single 一致。
    /// 空 Immutable：无键可切分，直接返回（不产出空文件，无副作用）。
    fn flush_by_table(&self, imm: &MemTable) -> Result<()> {
        if imm.len() == 0 {
            // 空 Immutable：无键可切分，直接返回（不产出空文件）
            return Ok(());
        }
        let _g = self.sst_mutate.lock().unwrap();
        let mut writer: Option<SstWriter> = None;
        let mut cur_tbl: u16 = 0;
        let mut outputs: Vec<std::path::PathBuf> = Vec::new();
        imm.scan(|k, e| {
            let tbl = key_table_id(k).unwrap_or(0);
            if writer.is_none() || tbl != cur_tbl {
                if let Some(mut w) = writer.take() {
                    w.finish().expect("SST finish 失败");
                }
                let sst_id = self.next_sst_id.fetch_add(1, Ordering::Relaxed);
                let path = self.dir.join(format!("{SST_PREFIX}{sst_id:08}.sst"));
                let w = SstWriter::new_with_pax(
                    &path,
                    self.compression,
                    self.compression_level,
                    self.block_size,
                    imm.len(),
                    &self.pax_hot_fields,
                    self.bloom_fpr,
                )
                .expect("SST 创建失败");
                writer = Some(w);
                cur_tbl = tbl;
                outputs.push(path);
            }
            let w = writer.as_mut().unwrap();
            match &e.value {
                Some(v) => w.add(k, v, e.seq).expect("SST 写入失败"),
                None => w.add_tombstone(k, e.seq).expect("SST Tombstone 写入失败"),
            }
        });
        if let Some(mut w) = writer.take() {
            w.finish().expect("SST finish 失败");
        }
        self.flush_sst_count.store(outputs.len(), Ordering::Relaxed);
        for p in &outputs {
            self.io_acquire(p)?;
            let reader = SstReader::open_with_granularity(p, self.index_granularity)?;
            self.snapshot_insert(reader);
        }
        self.persist_manifest()?;
        drop(_g);
        info!(
            "列族 [{}] 按表切分刷盘完成: {} 个文件（{} 条）",
            self.name,
            outputs.len(),
            imm.len()
        );
        Ok(())
    }

    /// TTL 分桶 flush：按文档 ttl_field 提取的 UTC 天分桶，每个桶一个 SST 文件；
    /// 无时间字段 / 非 JSON 值落入默认桶（无日期前缀，永不过期）。桶内 key 保持升序。
    fn flush_buckets(&self, imm: &MemTable) -> Result<()> {
        let mut buckets: std::collections::BTreeMap<Option<i64>, Vec<BucketRow>> =
            std::collections::BTreeMap::new();
        imm.scan(|k, e| {
            let days = match &e.value {
                Some(v) => self.document_bucket_days(v),
                None => None, // Tombstone 无时间 → 默认桶（随对应 key 的桶删除语义由数据决定）
            };
            let flag = if e.value.is_some() {
                FLAG_PUT
            } else {
                FLAG_DELETE
            };
            buckets
                .entry(days)
                .or_default()
                .push((k.to_vec(), e.value.clone(), flag, e.seq));
        });
        let bucket_count = buckets.len();
        let _g = self.sst_mutate.lock().unwrap();
        let mut created = 0;
        for (days, rows) in buckets {
            let sst_id = self.next_sst_id.fetch_add(1, Ordering::Relaxed);
            let fname = match days {
                Some(d) => format!("{SST_PREFIX}{d:08}-{sst_id:08}.sst"),
                None => format!("{SST_PREFIX}{sst_id:08}.sst"),
            };
            let path = self.dir.join(&fname);
            self.write_rows(&path, &rows)?;
            self.io_acquire(&path)?;
            let reader = SstReader::open_with_granularity(&path, self.index_granularity)?;
            self.snapshot_insert(reader);
            created += 1;
        }
        self.flush_sst_count.store(created, Ordering::Relaxed);
        self.persist_manifest()?;
        drop(_g);
        info!(
            "列族 [{}] TTL 分桶刷盘完成: {} 个桶",
            self.name, bucket_count
        );
        Ok(())
    }

    /// 从文档 JSON 提取 ttl_field（数值秒级时间戳）→ UTC 纪元天数；无法解析返回 None。
    fn document_bucket_days(&self, value: &[u8]) -> Option<i64> {
        let v: serde_json::Value = serde_json::from_slice(value).ok()?;
        let secs = v.get(&self.ttl_field)?.as_i64()?;
        Some(epoch_days(secs))
    }

    /// 按行序列写一个 SST（TTL 分桶用；行内 key 已升序）。
    /// 后台 IO 限速：刷盘完成后按实际文件字节数 acquire（design 4.5 阶段 3；不限速时为空操作）。
    /// O 项第③步：`&self`（io_limiter 内部 Mutex，compact 读路径并发限速）。
    fn io_acquire(&self, path: &Path) -> Result<()> {
        if let Some(limiter) = &self.io_limiter {
            let sz = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            limiter.lock().unwrap().acquire(sz)?;
        }
        Ok(())
    }

    /// Ex-7.4：动态调整后台 IO 限速（前台写压力驱动，Engine 调 Compaction 让路）。
    pub fn set_io_rate_bytes(&self, bytes_per_sec: u64) {
        if let Some(limiter) = &self.io_limiter {
            limiter.lock().unwrap().set_rate(bytes_per_sec);
        }
    }

    /// 导出共享后台 IO 限速（design 20.5）：启用/调整顺序扫描路径限速器
    /// （0 = 关闭，恢复前台读不限速）。与 Compaction `io_limiter` 同 Token Bucket 策略——
    /// 导出读 SST 与后台合并共享同一后台 IO 预算语义，默认低于前台读写。
    pub fn set_scan_rate_limit(&self, bytes_per_sec: u64) {
        *self.scan_limiter.lock().unwrap() = if bytes_per_sec > 0 {
            Some(crate::io_scheduler::IoRateLimiter::new(bytes_per_sec))
        } else {
            None
        };
    }

    /// L 项：设置前台写压力（0~1，Ex-7.4 同源信号：MemTable 水位代理）——动态窗口反馈依据。
    /// 写压力高 → 有效 L0 阈值收窄（提前收敛防堆积 + 写 Stall）；低 → 放宽（降合并次数/写放大）。
    pub fn set_write_pressure(&self, p: f64) {
        self.write_pressure
            .store(p.clamp(0.0, 1.0).to_bits(), Ordering::Relaxed);
    }

    /// R4：设置 MVCC 保活水位（seq floor，0 = 关闭）。引擎在 compact 前按活跃快照
    /// 低水位设置；compact 结束后调用方复位 0。
    pub fn set_mvcc_keep_floor(&self, floor: u64) {
        self.mvcc_keep_floor.store(floor, Ordering::Release);
    }

    /// L 项：有效 L0 阈值 = 基础阈值 ± 压力调整（clamp 在 [min, max]）。
    /// 滞回由压力平滑（Ex-7.4 每次写后按水位重算）保证，防窗口振荡。
    fn effective_l0_threshold(&self) -> usize {
        let pressure = f64::from_bits(self.write_pressure.load(Ordering::Relaxed));
        let range = self.l0_stall_max.saturating_sub(self.l0_stall_min);
        let shrink = (range as f64 * pressure) as usize;
        self.l0_stall_threshold
            .saturating_sub(shrink)
            .clamp(self.l0_stall_min, self.l0_stall_max)
    }

    /// P4-A：Compaction 写入速率自适应——记录本次 flush 新增 L0 段数，
    /// 更新滑动窗口，检测写入爆发并动态调整 `l1_trigger_files`。
    /// 爆发（窗口内新增 > 阈值）→ `l1_trigger` 降为 2 → 提前下沉 L1→L2，防 L0 爆胀；
    /// 正常 → 恢复基准 `base_l1_trigger`。
    pub fn record_flush_new_l0(&self, new_segments: usize) {
        let mut window = self.write_rate_window.lock().unwrap();
        // 滑动窗口：窗口满 → 弹出最旧，加入最新
        if window.len() >= self.write_rate_window_size {
            window.remove(0);
        }
        window.push(new_segments);
        // 计算窗口内总新增段数，判断是否爆发
        let total: usize = window.iter().sum();
        drop(window);

        // 原子安全地调整 l1_trigger_files（AtomicUsize 无需 &mut）
        if total > self.write_rate_burst_threshold {
            // 写入爆发 → 降阈值，提前合并
            self.l1_trigger_files.store(2.max(self.base_l1_trigger / 2), Ordering::Relaxed);
        } else {
            // 正常写入 → 恢复基准，攒批降写放大
            self.l1_trigger_files.store(self.base_l1_trigger, Ordering::Relaxed);
        }
    }

    /// P4-A：获取当前有效 `l1_trigger_files`（基准/爆发自适应）。
    pub fn effective_l1_trigger(&self) -> usize {
        self.l1_trigger_files.load(Ordering::Relaxed)
    }

    /// L 项：当前处于冷却期的段下标集合（按 path 匹配到期轮次 > 当前合并轮次）。
    fn cooling_indices(&self) -> std::collections::HashSet<usize> {
        let mut out = std::collections::HashSet::new();
        if self.compaction_cooldown == 0 {
            return out;
        }
        let cooldown = self.cooldown.lock().unwrap();
        let round = self.merge_round.load(Ordering::Relaxed);
        for (i, s) in self.ssts.load().ssts.iter().enumerate() {
            if cooldown
                .get(&s.path().to_path_buf())
                .map(|exp| *exp > round)
                .unwrap_or(false)
            {
                out.insert(i);
            }
        }
        out
    }

    /// Ex-7.4：当前后台 IO 限速（字节/秒；0 = 不限速/未配置）。
    pub fn io_rate(&self) -> u64 {
        self.io_limiter
            .as_ref()
            .map_or(0, |l| l.lock().unwrap().rate())
    }

    /// 基础 Compaction（无删除位图过滤）。Ex-5.8：无重叠 L0 段合并走**数据块级复用**
    /// （只重建元数据区），否则回退全量合并（等价 `compact_filtered(&|_| false)`）。
    /// O 项第③步：`&self`（ssts 快照 load/store 原子发布）——后台合并线程读锁下执行。
    pub fn compact(&self) -> Result<CompactReport> {
        let snap = self.ssts.load();
        let heat: Vec<u64> = snap.ssts.iter().map(|s| s.heat()).collect();
        // Ex-8.11：L1 段数上限（L0 活跃时纳入 L0+L1 合并的"已满"界限）——>0 时用 l1_trigger_files
        let l1_tf = self.l1_trigger_files.load(Ordering::Relaxed);
        let cap = if l1_tf > 0 {
            l1_tf
        } else {
            self.effective_l0_threshold()
        };
        let (sel, out_level) = select_compaction_inputs_ex(
            &snap.levels,
            cap,
            l1_tf,
            self.l2_trigger_files,
            &heat,
            &self.cooling_indices(),
            &snap.sizes,
            self.compact_input_max_bytes,
        );
        if sel.len() <= 1 {
            return Ok(CompactReport {
                merged_ssts: 0,
                kept_keys: 0,
                freed_bytes: 0,
                out_level: 0,
                dropped_keys: 0,
            });
        }
        // M3（表切分）：底层（L1→L2 / L2）合并仅当存在**同表多段或混表段**才有价值——
        // 跨表每表各 1 段的收敛态直接跳过（防多表库底层反复全量重写空转）。
        if self.split_by_table && out_level >= 2 && !self.sel_has_merge_work(&sel) {
            return Ok(CompactReport {
                merged_ssts: 0,
                kept_keys: 0,
                freed_bytes: 0,
                out_level: 0,
                dropped_keys: 0,
            });
        }
        if let Some(rep) = self.try_meta_only_compact(&sel, out_level)? {
            return Ok(rep);
        }
        self.compact_merge(&sel, out_level, &|_| false)
    }

    /// Leveled-Compaction（design 4.5 二期 / M6-2）：分层压实，限制单次压实量。
    ///
    /// - **L0 → L1**：有 L0 段（刷盘产物，允许重叠）时合并 L0 → 单个 L1 段（不合并既有 L1，
    ///   单次压实量 = 单个刷盘批次，有界）；L1 文件数达到层上限时改合并 L0 + 全部 L1（收敛）；
    /// - **L1 → L2**：L0 为空且 L1 段数 > 1 时，合并全部 L1 → 单个 L2 段（压实下沉）；
    /// - 合并语义：按 (key 升序, seq 降序) 排序去重，后写覆盖先写，Tombstone 保留；
    /// - **Ex-5.6 删除位图过滤**：`drop_key` 返回 true 的 key **物理丢弃**（不保留数据、
    ///   不写 Tombstone）——位图已删文档的旧数据在合并时直接回收（墓碑不污染层级；
    ///   位图标记保留，put 复活时清位）；
    /// - 崩溃安全：新段写入 → fsync → 原子更新 Manifest → 删除旧段；
    /// - 后台 IO 限速：写完后按实际文件字节 acquire。
    /// O 项第③步：`&self`（ssts 快照 load + store 原子发布）——后台合并线程可在读锁下
    /// 执行（合并不阻塞读；与写互斥由 Engine 锁保证，快照 store 无并发丢失）。
    pub fn compact_filtered(&self, drop_key: &dyn Fn(&[u8]) -> bool) -> Result<CompactReport> {
        let snap = self.ssts.load();
        let heat: Vec<u64> = snap.ssts.iter().map(|s| s.heat()).collect();
        // Ex-8.11：同 compact()，L1 段数上限 = l1_trigger_files（>0 时）
        let l1_tf = self.l1_trigger_files.load(Ordering::Relaxed);
        let cap = if l1_tf > 0 {
            l1_tf
        } else {
            self.effective_l0_threshold()
        };
        let (sel, out_level) = select_compaction_inputs_ex(
            &snap.levels,
            cap,
            l1_tf,
            self.l2_trigger_files,
            &heat,
            &self.cooling_indices(),
            &snap.sizes,
            self.compact_input_max_bytes,
        );
        if sel.len() <= 1 {
            return Ok(CompactReport {
                merged_ssts: 0,
                kept_keys: 0,
                freed_bytes: 0,
                out_level: 0,
                dropped_keys: 0,
            });
        }
        self.compact_merge(&sel, out_level, drop_key)
    }

    /// Ex-8.7：删除密度 GC 压实——先走常规 Leveled 选段（多段候选时等价 `compact_filtered`，
    /// 合并中按位图物理丢弃已删键）；**无多段候选**（如已收敛为单底层段）且 `allow_single=true`
    /// 时重写单个最底层段回收删除空间（删除位图语义下 delete 不写 Tombstone，已删旧数据
    /// 只能靠压实物理丢弃；收敛后单段不再参与常规合并，故需显式 GC 重写）。
    /// 单段重写绕开 Ex-5.8 块级复用（元数据拼接无法丢键），走全量合并保证 `drop_key` 生效；
    /// `allow_single=false` = 传统 `compact_filtered`（单段不重写，兼容既有调用）。
    pub fn compact_gc(
        &self,
        drop_key: &dyn Fn(&[u8]) -> bool,
        allow_single: bool,
    ) -> Result<CompactReport> {
        let empty = CompactReport {
            merged_ssts: 0,
            kept_keys: 0,
            freed_bytes: 0,
            out_level: 0,
            dropped_keys: 0,
        };
        let snap = self.ssts.load();
        let heat: Vec<u64> = snap.ssts.iter().map(|s| s.heat()).collect();
        let l1_tf = self.l1_trigger_files.load(Ordering::Relaxed);
        let cap = if l1_tf > 0 {
            l1_tf
        } else {
            self.effective_l0_threshold()
        };
        let (sel, out_level) = select_compaction_inputs_ex(
            &snap.levels,
            cap,
            l1_tf,
            self.l2_trigger_files,
            &heat,
            &self.cooling_indices(),
            &snap.sizes,
            self.compact_input_max_bytes,
        );
        if sel.len() >= 2 {
            // M3（表切分）：底层跨表不相交段（每表各 1 段）已按表收敛，多段全量合并无
            // 额外回收 → 落到单段 GC 重写（删除空间仍可按段回收，防多表库 GC 反复合并空转）
            if !(self.split_by_table && out_level >= 2 && !self.sel_has_merge_work(&sel)) {
                return self.compact_merge(&sel, out_level, drop_key);
            }
        }
        if !allow_single {
            return Ok(empty);
        }
        let Some((idx, lvl)) = self.pick_gc_single(&snap) else {
            return Ok(empty);
        };
        self.compact_merge(&[idx], lvl, drop_key)
    }

    /// Ex-8.7：删除密度单段 GC 候选——最深层（max level）非冷却最大段优先（删除空间回收
    /// 潜力大）；该层全冷却时忽略冷却回退选择（保证排空最终收敛，宁可多读一次不漏回收）。
    fn pick_gc_single(&self, snap: &SstSnapshot) -> Option<(usize, u32)> {
        let cooling = self.cooling_indices();
        let best = |cooling: &std::collections::HashSet<usize>| -> Option<(usize, u32)> {
            let mut best: Option<(usize, u32, u64)> = None;
            for (i, lvl) in snap.levels.iter().enumerate() {
                if cooling.contains(&i) {
                    continue;
                }
                let sz = snap.sizes.get(i).copied().unwrap_or_else(|| snap.ssts[i].file_len());
                let cand = (i, *lvl, sz);
                best = Some(match best {
                    None => cand,
                    Some(b) => {
                        if cand.1 > b.1 || (cand.1 == b.1 && cand.2 > b.2) {
                            cand
                        } else {
                            b
                        }
                    }
                });
            }
            best.map(|(i, lvl, _)| (i, lvl))
        };
        match best(&cooling) {
            Some(pick) => Some(pick),
            None if !cooling.is_empty() => best(&std::collections::HashSet::new()),
            None => None,
        }
    }

    /// Ex-8.12：按输出层选压缩级别——分层启用（`compression_level_l2`>0）时 L2+ 输出用
    /// 冷档高压缩率（L1→L2 下沉 / L2 内合并 / L2 单段重写），L0/L1 输出用热档（避免
    /// 中间层放大）；未启用恒热档（现行为）。flush 固定 L0（热档，不走本函数）。
    fn compression_level_for(&self, out_level: u32) -> i32 {
        if self.compression_level_l2 > 0 && out_level >= 2 {
            self.compression_level_l2
        } else {
            self.compression_level
        }
    }

    /// 全量合并压实主体（sel 已由调用方选出）：读全部输入行 → 排序去重 → 位图过滤 → 重写。
    /// O 项第③步：`&self`（输入走快照 Arc，写输出独立文件，提交走 snapshot store）。
    fn compact_merge(
        &self,
        sel: &[usize],
        out_level: u32,
        drop_key: &dyn Fn(&[u8]) -> bool,
    ) -> Result<CompactReport> {
        let old_count = sel.len();
        // ① P80 修复：流式 k 路归并（代替全量 Vec 物化）
        // 为每个输入段读出所有 entries 排序 → 推入迭代器，用最小堆输出
        // 内存占用 O(k + 输出缓冲区)，不随输入规模线性增长，解决 GB 级合并卡死
        let snap = self.ssts.load();

        // 堆元素：Reverse((key, seq, value, 迭代器索引)) — BinaryHeap 是最大堆，Reverse 转最小堆
        use std::cmp::Reverse;
        // 堆排序：key 升序，同 key seq 降序（最新版本优先弹出）
        // 内层 Reverse<u64> 使同 key 时最大 seq 优先出堆
        let mut heap: std::collections::BinaryHeap<Reverse<(Vec<u8>, Reverse<u64>, Option<Vec<u8>>, usize)>> =
            std::collections::BinaryHeap::new();

        // 为每个输入段创建排序好的迭代器
        let mut iterators: Vec<std::boxed::Box<dyn Iterator<Item = (Vec<u8>, u64, Option<Vec<u8>>)>>> =
            Vec::with_capacity(sel.len());
        let mut total_input_entries = 0usize;
        for &sst_idx in sel {
            let mut entries = Vec::new();
            snap.ssts[sst_idx].iterate(|k, v, seq| {
                entries.push((k.to_vec(), seq, v.map(|x| x.to_vec())));
            })?;
            // 同一段内先排序（key 升序，同 key seq 降序 → 弹出时保证最大 seq 优先）
            entries.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
            total_input_entries += entries.len();
            let mut iter = entries.into_iter();
            if let Some(first) = iter.next() {
                heap.push(Reverse((first.0, Reverse(first.1), first.2, iterators.len())));
            }
            iterators.push(Box::new(iter));
        }

        // ② 流式处理：弹出最小 key → 分组同 key → 按 MVCC 保活规则过滤 → 直接输出
        // 不缓存全量到内存，内存占用恒定（仅当前分组 + 堆）
        let floor = self.mvcc_keep_floor.load(Ordering::Acquire);
        let mut total_kept_keys = 0u64;
        let mut dropped_keys = 0u64;
        let mut current_tbl: Option<u16> = None;
        let mut current_writer: Option<SstWriter> = None;
        let mut out_paths: Vec<std::path::PathBuf> = Vec::new();

        // 开始新表输出段（split_by_table 时表边界切分文件）
        let start_new_table = |this: &Self, out_level: u32| -> Result<(SstWriter, PathBuf)> {
            let sst_id = this.next_sst_id.fetch_add(1, Ordering::Relaxed);
            let path = this.dir.join(format!("{SST_PREFIX}{sst_id:08}.sst"));
            let w = SstWriter::new_with_pax(
                &path,
                this.compression,
                this.compression_level_for(out_level),
                this.block_size,
                0, // 流式不知道总行数，0 走动态分配
                &this.pax_hot_fields,
                this.bloom_fpr,
            )?;
            Ok((w, path))
        };

        // 流式弹出 + 分组处理：同 key 全部 entries 缓冲后一次处理输出
        let mut buffer: Vec<(Vec<u8>, u64, Option<Vec<u8>>)> = Vec::new();
        // P80 修复：初始必须创建一个 writer（即使 split_by_table 关闭，也需要输出）
        if self.split_by_table {
            // 按表切分：初始 writer 为 None，第一个 key 输出时创建；
            // 如果 heap 为空（没有输出）则保持 None 不创建。
        } else {
            // 不按表切分：整个合并输出到单个文件，提前创建 writer
            let (w, path) = start_new_table(self, out_level)?;
            current_tbl = None;
            current_writer = Some(w);
            out_paths.push(path);
        }
        while !heap.is_empty() {
            // 弹出当前全局最小 key
            let Reverse((current_key, Reverse(current_seq), current_val, iter_idx)) = heap.pop().unwrap();

            // 缓冲当前 entry 到分组
            if buffer.is_empty() || buffer.last().unwrap().0 == current_key {
                buffer.push((current_key, current_seq, current_val));
            } else {
                // 完成上一个 key 分组：按 MVCC 规则处理并输出
                let group_key = buffer[0].0.clone();
                let deleted = drop_key(&group_key);
                let latest_seq = buffer[0].1; // 已按 seq 降序，第一个 = 最大

                if !(deleted && !(floor > 0 && latest_seq > floor)) {
                    // 需要输出至少一个版本
                    let mut pushed_floor = false;
                    for (k, seq, v) in &buffer {
                        if floor > 0 && latest_seq > floor {
                            if *seq > floor {
                                // 活跃快照保活：seq > floor → 输出
                                if let Some(tbl_id) = key_table_id(k) {
                                    if self.split_by_table && current_tbl != Some(tbl_id) {
                                        // 表边界：结束当前段，开新段
                                        if let Some(mut w) = current_writer.take() {
                                            w.finish()?;
                                            if !out_paths.is_empty() {
                                                self.io_acquire(out_paths.last().unwrap())?;
                                            }
                                        }
                                        let (w, path) = start_new_table(self, out_level)?;
                                        current_tbl = Some(tbl_id);
                                        current_writer = Some(w);
                                        out_paths.push(path);
                                    }
                                }
                                let w = current_writer.as_mut().ok_or_else(|| {
                                    Error::Config("没有当前 writer 输出复合键".into())
                                })?;
                                match v {
                                    Some(val) => w.add(k, val, *seq)?,
                                    None => w.add_tombstone(k, *seq)?,
                                }
                                total_kept_keys += 1;
                            } else if !pushed_floor {
                                // seq ≤ floor → 只输出一个最新
                                // M3：表边界切分（split_by_table 时检测表切换）
                                if let Some(tbl_id) = key_table_id(k) {
                                    if self.split_by_table && current_tbl != Some(tbl_id) {
                                        if let Some(mut w) = current_writer.take() {
                                            w.finish()?;
                                            if !out_paths.is_empty() {
                                                self.io_acquire(out_paths.last().unwrap())?;
                                            }
                                        }
                                        let (w, path) = start_new_table(self, out_level)?;
                                        current_tbl = Some(tbl_id);
                                        current_writer = Some(w);
                                        out_paths.push(path);
                                    }
                                }
                                // P80 修复：首次输出时 writer 为 None → 创建
                                if current_writer.is_none() {
                                    let (w, path) = start_new_table(self, out_level)?;
                                    current_tbl = key_table_id(k);
                                    current_writer = Some(w);
                                    out_paths.push(path);
                                }
                                let w = current_writer.as_mut().ok_or_else(|| {
                                    Error::Config("没有当前 writer 输出复合键".into())
                                })?;
                                match v {
                                    Some(val) => w.add(k, val, *seq)?,
                                    None => w.add_tombstone(k, *seq)?,
                                }
                                pushed_floor = true;
                                total_kept_keys += 1;
                            }
                        } else {
                            // 无保活需求 → 只输出最新版本
                            // M3：表边界切分（split_by_table 时检测表切换）
                            if let Some(tbl_id) = key_table_id(k) {
                                if self.split_by_table && current_tbl != Some(tbl_id) {
                                    if let Some(mut w) = current_writer.take() {
                                        w.finish()?;
                                        if !out_paths.is_empty() {
                                            self.io_acquire(out_paths.last().unwrap())?;
                                        }
                                    }
                                    let (w, path) = start_new_table(self, out_level)?;
                                    current_tbl = Some(tbl_id);
                                    current_writer = Some(w);
                                    out_paths.push(path);
                                }
                            }
                            // P80 修复：首次输出时 writer 为 None → 创建
                            if current_writer.is_none() {
                                let (w, path) = start_new_table(self, out_level)?;
                                current_tbl = key_table_id(k);
                                current_writer = Some(w);
                                out_paths.push(path);
                            }
                            let w = current_writer.as_mut().ok_or_else(|| {
                                Error::Config("没有当前 writer 输出复合键".into())
                            })?;
                            match v {
                                Some(val) => w.add(k, val, *seq)?,
                                None => w.add_tombstone(k, *seq)?,
                            }
                            total_kept_keys += 1;
                            break; // 只输出最新
                        }
                    }
                } else {
                    dropped_keys += 1;
                }

                // 开始新分组
                buffer.clear();
                buffer.push((current_key, current_seq, current_val));
            }

            // 从原迭代器取下一个元素，推回堆
            if let Some(next) = iterators[iter_idx].next() {
                heap.push(Reverse((next.0, Reverse(next.1), next.2, iter_idx)));
            }
        }

        // 处理最后一个 key 分组
        if !buffer.is_empty() {
            let group_key = buffer[0].0.clone();
            let deleted = drop_key(&group_key);
            let latest_seq = buffer[0].1;

            if !(deleted && !(floor > 0 && latest_seq > floor)) {
                    let mut pushed_floor = false;
                    for (k, seq, v) in &buffer {
                        if floor > 0 && latest_seq > floor {
                            if *seq > floor {
                                if let Some(tbl_id) = key_table_id(k) {
                                    // P80 修复：split_by_table 场景，第一个 key 输出时 writer 还没创建
                                    if current_writer.is_none() {
                                        let (w, path) = start_new_table(self, out_level)?;
                                        current_tbl = Some(tbl_id);
                                        current_writer = Some(w);
                                        out_paths.push(path);
                                    }
                                    if self.split_by_table && current_tbl != Some(tbl_id) {
                                        if let Some(mut w) = current_writer.take() {
                                            w.finish()?;
                                            if !out_paths.is_empty() {
                                                self.io_acquire(out_paths.last().unwrap())?;
                                            }
                                        }
                                        let (w, path) = start_new_table(self, out_level)?;
                                        current_tbl = Some(tbl_id);
                                        current_writer = Some(w);
                                        out_paths.push(path);
                                    }
                                }
                                // P80 修复：非 docid 键首次输出时 writer 仍为 None → 创建
                                if current_writer.is_none() {
                                    let (w, path) = start_new_table(self, out_level)?;
                                    current_writer = Some(w);
                                    out_paths.push(path);
                                }
                                let w = current_writer.as_mut().ok_or_else(|| {
                                    Error::Config("没有当前 writer 输出复合键".into())
                                })?;
                            match v {
                                Some(val) => w.add(k, val, *seq)?,
                                None => w.add_tombstone(k, *seq)?,
                            }
                            total_kept_keys += 1;
                        } else if !pushed_floor {
                            // M3：表边界切分（split_by_table 时检测表切换）
                            if let Some(tbl_id) = key_table_id(k) {
                                if self.split_by_table && current_tbl != Some(tbl_id) {
                                    if let Some(mut w) = current_writer.take() {
                                        w.finish()?;
                                        if !out_paths.is_empty() {
                                            self.io_acquire(out_paths.last().unwrap())?;
                                        }
                                    }
                                    let (w, path) = start_new_table(self, out_level)?;
                                    current_tbl = Some(tbl_id);
                                    current_writer = Some(w);
                                    out_paths.push(path);
                                }
                            }
                            // P80 修复：首次输出时 writer 为 None → 创建
                            if current_writer.is_none() {
                                let (w, path) = start_new_table(self, out_level)?;
                                current_tbl = key_table_id(k);
                                current_writer = Some(w);
                                out_paths.push(path);
                            }
                            let w = current_writer.as_mut().ok_or_else(|| {
                                Error::Config("没有当前 writer 输出复合键".into())
                            })?;
                            match v {
                                Some(val) => w.add(k, val, *seq)?,
                                None => w.add_tombstone(k, *seq)?,
                            }
                            pushed_floor = true;
                            total_kept_keys += 1;
                        }
                    } else {
                            // P80 修复：首次输出时 writer 为 None → 创建
                            if current_writer.is_none() {
                                let (w, path) = start_new_table(self, out_level)?;
                                current_writer = Some(w);
                                out_paths.push(path);
                            }
                            // 表切分边界检查（同 non-floor 分支）
                            if let Some(tbl_id) = key_table_id(k) {
                                if self.split_by_table && current_tbl != Some(tbl_id) {
                                    if let Some(mut w) = current_writer.take() {
                                        w.finish()?;
                                        if !out_paths.is_empty() {
                                            self.io_acquire(out_paths.last().unwrap())?;
                                        }
                                    }
                                    let (w, path) = start_new_table(self, out_level)?;
                                    current_tbl = Some(tbl_id);
                                    current_writer = Some(w);
                                    out_paths.push(path);
                                }
                            }
                            let w = current_writer.as_mut().ok_or_else(|| {
                                Error::Config("没有当前 writer 输出复合键".into())
                            })?;
                        match v {
                            Some(val) => w.add(k, val, *seq)?,
                            None => w.add_tombstone(k, *seq)?,
                        }
                        total_kept_keys += 1;
                    }
                }
            } else {
                dropped_keys += 1;
            }
        }

        // 结束最后一个 writer
        if let Some(mut w) = current_writer.take() {
            w.finish()?;
            if !out_paths.is_empty() {
                self.io_acquire(out_paths.last().unwrap())?;
            }
        } else if out_paths.is_empty() {
            // 所有键都被丢弃 → 仍写一个空段保持层结构不变
            let (w, path) = start_new_table(self, out_level)?;
            w.finish()?;
            self.io_acquire(&path)?;
            out_paths.push(path);
        }

        let kept_before_grouping = total_input_entries as u64;

        self.finalize_compact(
            sel,
            &out_paths,
            out_level,
            old_count,
            total_kept_keys as usize,
            kept_before_grouping.saturating_sub(total_kept_keys) as usize,
            dropped_keys as usize,
        )
    }

    /// Ex-5.8 元数据-数据解耦：无重叠输入段合并时**数据块级复用**——各段数据块区原样顺序
    /// 拼接（零解压零重压缩），只重建 Block Index/分区布隆/Footer 元数据区。
    /// 前提：行式列族（pax_hot_fields 为空）+ 相邻段 key 无重叠（前段 max < 后段 min）。
    /// 收益：Compaction 读放大归零、压缩 CPU 免除（demo 实测全量重写 4041ms vs 块级复用毫秒级）。
    /// 返回 Some(report) = 已复用完成；None = 条件不满足（调用方回退全量合并）。
    /// O 项第③步：`&self`（快照读输入段）。
    fn try_meta_only_compact(
        &self,
        sel: &[usize],
        out_level: u32,
    ) -> Result<Option<CompactReport>> {
        if !self.pax_hot_fields.is_empty() {
            return Ok(None); // PAX 列族：块内字段 Zone Map 无法重建，回退全量
        }
        let snap = self.ssts.load();
        // Ex-8.12：分层启用时**跨档合并禁块级复用**——块级复用原样保留输入块压缩档；
        // L0/L1 热档段下沉 L2（或 L2 冷档段回升热档层）必须全量重压缩，否则输出层档位
        // 语义不成立。仅当所有输入段与输出层同档（同为 L2 冷档 / 同为 L0/L1 热档）可复用。
        if self.compression_level_l2 > 0 {
            let out_cold = out_level >= 2;
            for &i in sel {
                let in_lvl = snap.levels.get(i).copied().unwrap_or(0);
                if (in_lvl >= 2) != out_cold {
                    return Ok(None);
                }
            }
        }
        // 读取每段 key 范围 [min, max]（仅解码元数据索引，不碰数据块）
        let mut ranges: Vec<(usize, Vec<u8>, Vec<u8>)> = Vec::with_capacity(sel.len());
        for &i in sel {
            let idx = snap.ssts[i].index();
            let min = idx
                .first()
                .map(|e| e.first_key.clone())
                .unwrap_or_default();
            let max = idx
                .last()
                .map(|e| e.max_key.clone())
                .unwrap_or_default();
            ranges.push((i, min, max));
        }
        // M3（§26 多表）：表切分列族**仅同表段可块级复用**——块级复用把输入块原样拼进
        // **单个**输出文件；若输入跨表（或老格式混表段，段内 min/max 跨表），单文件输出
        // 会把多表混进一个文件，破坏"每文件单表"的文件路由前提 → 回退全量合并按表切分。
        if self.split_by_table {
            let mut common: Option<u16> = None;
            for &(_, ref mn, ref mx) in &ranges {
                match (key_table_id(mn), key_table_id(mx)) {
                    (Some(a), Some(b)) if a == b => match common {
                        Some(t) if t != a => return Ok(None),
                        _ => common = Some(a),
                    },
                    _ => return Ok(None), // 非 8 字节键 / 段内跨表 → 禁复用
                }
            }
        }
        // 按 min 排序，检查相邻段无重叠（重叠需全量合并保证覆盖/去重语义）
        ranges.sort_by(|a, b| a.1.cmp(&b.1));
        for w in ranges.windows(2) {
            if w[0].2 >= w[1].1 {
                return Ok(None);
            }
        }
        // 块级复用：按 key 序逐段原样拷贝数据块，重建元数据
        let old_count = sel.len();
        let sst_id = self.next_sst_id.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{SST_PREFIX}{sst_id:08}.sst"));
        let mut kept = 0u64;
        {
            let mut w = SstWriter::new_with_pax(
                &path,
                self.compression,
                self.compression_level,
                self.block_size,
                0,
                &self.pax_hot_fields,
                self.bloom_fpr,
            )?;
            for &(i, _, _) in &ranges {
                let entries = snap.ssts[i].index();
                for e in entries {
                    let (comp, raw) = snap.ssts[i].block_raw(&e)?;
                    w.add_raw_block(&raw, &comp)?;
                }
                kept += snap.ssts[i].footer().key_count;
            }
            w.finish()?;
        }
        self.io_acquire(&path)?;
        let rep =
            self.finalize_compact(sel, &[path], out_level, old_count, kept as usize, 0, 0)?;
        info!(
            "列族 [{}] 块级复用 Compaction 完成（Ex-5.8，零解压只重建元数据）: →L{out_level} 合并 {} 段，释放 {} 字节",
            self.name,
            old_count,
            rep.freed_bytes
        );
        Ok(Some(rep))
    }

    /// 压实收尾（④⑤）：原子发布新快照（旧段移除 + 新段插最前）→ 更新 Manifest →
    /// 删除旧段（换快照后再删，并发读持 Arc 句柄有效）→ 报告。
    /// M3：`paths` 支持多输出（表切分每表一个文件），全部按 `out_level` 插到快照最前。
    /// O 项第③步：`&self`（snapshot store 原子切换 + merge_round/cooldown 内部可变）。
    #[allow(clippy::too_many_arguments)]
    fn finalize_compact(
        &self,
        sel: &[usize],
        paths: &[PathBuf],
        out_level: u32,
        old_count: usize,
        kept: usize,
        eliminated: usize,
        dropped: usize,
    ) -> Result<CompactReport> {
        // ④ 原子发布新快照（先 store 后删旧文件；读线程持旧快照 Arc 期间句柄有效）
        // P72：sst_mutate 互斥——与 flush（snapshot_insert）无 Engine 锁并发时防快照丢失更新
        let _g = self.sst_mutate.lock().unwrap();
        let cur = self.ssts.load();
        let old_bytes: u64 = sel
            .iter()
            .map(|&i| {
                std::fs::metadata(cur.ssts[i].path())
                    .map(|m| m.len())
                    .unwrap_or(0)
            })
            .sum();
        let mut old_ssts = Vec::new();
        let mut kept_ssts = Vec::new();
        let mut kept_levels = Vec::new();
        let mut removed = vec![false; cur.ssts.len()];
        for &i in sel {
            removed[i] = true;
        }
        for (i, sst) in cur.ssts.iter().enumerate() {
            if removed[i] {
                old_ssts.push(sst.clone());
            } else {
                kept_ssts.push(sst.clone());
                kept_levels.push(cur.levels[i]);
            }
        }
        // M3：多输出全部置前（同层、段间不重叠 → 相对顺序无版本语义影响）
        // Ex-8.11：compaction 每写一个新输出文件 → 累计写入字节（写放大实验数据源）
        for p in paths {
            let reader = SstReader::open_with_granularity(p, self.index_granularity)?;
            self.sst_written.fetch_add(reader.file_len(), Ordering::Relaxed);
            kept_ssts.insert(0, Arc::new(reader));
            kept_levels.insert(0, out_level);
        }
        let (layer_ranges, layer_indices, l0_table_ranges) = Self::build_layer_meta(&kept_ssts, &kept_levels);
        let sizes: Vec<u64> = kept_ssts.iter().map(|r| r.file_len()).collect();
        self.ssts.store(Arc::new(SstSnapshot {
            ssts: kept_ssts,
            levels: kept_levels,
            layer_ranges,
            layer_indices,
            l0_table_ranges,
            sizes,
        }));
        self.persist_manifest()?;

        // ⑤ 删除旧段（P73 修复：在 sst_mutate 锁内完成——persist_manifest 以磁盘扫描重建清单，
        // 若删旧段在锁外，flush 持锁 persist 时可能扫到未删旧段写入 manifest，随后旧段被删 →
        // manifest 悬空引用已删文件 → 重启加载失败。store→persist→remove 原子一致）。
        // 并发读仍持旧快照 Arc → 句柄有效，引用归零后实际释放。
        for r in &old_ssts {
            let _ = std::fs::remove_file(r.path());
        }
        drop(_g);
        let freed_bytes = old_bytes.saturating_sub(self.sst_bytes());
        let n_out = paths.len();
        if dropped > 0 {
            info!(
                "列族 [{}] Compaction 完成: →L{out_level} 合并 {} 段 → {n_out} 文件（保留 {} 键，位图物理丢弃 {dropped} 键，释放 {} 字节）",
                self.name,
                old_count,
                kept,
                freed_bytes
            );
        } else {
            info!(
                "列族 [{}] Compaction 完成: →L{out_level} 合并 {} 段 → {n_out} 文件（保留 {} 键，释放 {} 字节）",
                self.name,
                old_count,
                kept,
                freed_bytes
            );
        }
        // L 项：合并冷却——输出段 N 轮内不参与下一轮合并（防刚合并又合并的无谓重写）；
        // 推进 merge_round + 清理到期项（纯内存调度态，重启后冷却重置，无一致性影响）
        if self.compaction_cooldown > 0 {
            let round = self.merge_round.fetch_add(1, Ordering::Relaxed) + 1;
            let mut cd = self.cooldown.lock().unwrap();
            for p in paths {
                cd.insert(p.to_path_buf(), round + self.compaction_cooldown as u64);
            }
            cd.retain(|_, exp| *exp > round);
        }
        Ok(CompactReport {
            merged_ssts: old_count,
            kept_keys: eliminated,
            freed_bytes,
            out_level,
            dropped_keys: dropped,
        })
    }

    /// M3（§26 多表）：段归属表 id（快照内 sst 下标 → Some(table_id)）。
    /// 仅 min/max 同属一表视为单表段；混表（老格式）段 / 空段 / 非 8 字节键 → None。
    fn sst_table_of(&self, i: usize) -> Option<u16> {
        let snap = self.ssts.load();
        match snap.ssts[i].key_range() {
            Some((mn, mx)) => match (key_table_id(mn), key_table_id(mx)) {
                (Some(a), Some(b)) if a == b => Some(a),
                _ => None,
            },
            None => None,
        }
    }

    /// M3：选定段集是否含**合并价值**——存在同表 ≥2 段（跨段有重叠/需下沉去重）
    /// 或混表段（老格式，需全量合并按表切分）。跨表每表各 1 段 = 已按表收敛，无需重写。
    fn sel_has_merge_work(&self, sel: &[usize]) -> bool {
        if sel.len() < 2 {
            return false;
        }
        let mut counts: std::collections::HashMap<u16, usize> = std::collections::HashMap::new();
        for &i in sel {
            match self.sst_table_of(i) {
                Some(t) => *counts.entry(t).or_insert(0) += 1,
                None => return true, // 混表段 / 无范围：保守认为需全量合并
            }
        }
        counts.values().any(|&c| c >= 2)
    }

    /// M3：表切分列族底部（L0 空时的 L1/L2）是否需合并——按表、按层口径，且**尊重
    /// Ex-8.11 攒批配置**（l1/l2_trigger_files>0 时按层文件总数攒批一次下沉/收敛，0 = 按
    /// 同表 ≥2 段即触发）：
    /// - L1 或 L2 **某一层内**存在**同表 ≥2 段**（有去重/下沉收益）或混表老段即触发；
    /// - 跨表每层每表各 1 段视为已收敛 → 不触发（防多表库底层反复重写空转）。
    fn split_bottom_merge_work(&self) -> bool {
        let snap = self.ssts.load();
        let mut l1: std::collections::HashMap<u16, usize> = std::collections::HashMap::new();
        let mut l2: std::collections::HashMap<u16, usize> = std::collections::HashMap::new();
        let (mut l1n, mut l2n) = (0usize, 0usize);
        for (i, lv) in snap.levels.iter().enumerate() {
            if *lv == 0 {
                continue;
            }
            match self.sst_table_of(i) {
                Some(t) => {
                    let counts = if *lv == 1 { &mut l1 } else { &mut l2 };
                    *counts.entry(t).or_insert(0) += 1;
                    if *lv == 1 {
                        l1n += 1;
                    } else {
                        l2n += 1;
                    }
                }
                None => return true, // 底部混表老段：需合并切分
            }
        }
        let l1_tf = self.l1_trigger_files.load(Ordering::Relaxed);
        let has_l1_table_with_multi = l1.values().any(|&c| c >= 2);
        let l1_gate = if l1_tf > 0 {
            l1n >= l1_tf || has_l1_table_with_multi
        } else {
            has_l1_table_with_multi
        };
        
        if l1_gate {
            return true;
        }
        if l1_tf > 0 && l1n > 0 {
            return false; // 延迟模式：L1 未达阈值，所有表单段，等批次到齐（不提前收敛 L2）
        }
        let l2_gate = if self.l2_trigger_files > 0 {
            l2n >= self.l2_trigger_files
        } else {
            l2.values().any(|&c| c >= 2)
        };
        l2_gate
    }

    /// 是否需要 Compaction（design 4.5 二期）：
    /// L0 段数超过 `storage.l0_stall_threshold`（L0→L1），或 L0 为空但 L1 段数 > 1（L1→L2），
    /// 或 L0/L1 均空但 L2 段数 > 1（L2 收敛）。
    /// P 项：启用 `l0_max_size_mb` 时叠加**大小阈值**——L0 文件总字节超限即触发（防大段少量堆积）。
    pub fn needs_compact(&self) -> bool {
        let snap = self.ssts.load();
        let l0 = snap.levels.iter().filter(|l| **l == 0).count();
        let l1 = snap.levels.iter().filter(|l| **l == 1).count();
        let l2 = snap.levels.iter().filter(|l| **l >= 2).count();
        // L 项：用动态有效阈值（写压力高 → 阈值收窄 → 更早触发收敛）
        l0 > self.effective_l0_threshold()
            // P 项：大小阈值需 ≥2 段（单段为已排序文件，合并是纯无收益重写）
            || (l0 >= 2 && self.l0_max_size_bytes > 0 && self.l0_bytes() > self.l0_max_size_bytes)
            || (l0 == 0 && self.bottom_needs_compact(l1, l2))
    }

    /// Ex-8.11：L0 空时的底层合并触发（L1→L2 下沉 / L2 收敛）。
    /// `l1_trigger_files>0`：L1 **攒够阈值**才下沉 L2（延迟大合并），且 L1 未达阈值期间
    /// 不提前收敛 L2（等批次到齐一起下沉，减底层重写）；0 = 现行为（L1>1 或 L2>1 即触发）。
    /// `l2_trigger_files>0`：L2 攒够阈值才收敛为单段；0 = 现行为（L2>1 即收敛）。
    /// M3（表切分列族）：改按表口径——同表 ≥2 段 / 混表老段才触发（见 `split_bottom_merge_work`）；
    /// L1→L2 下沉仅在 L1 存在同表 ≥2 段（或混表）时进行，跨表每表 1 段停在当前层即收敛。
    fn bottom_needs_compact(&self, l1: usize, l2: usize) -> bool {
        if self.split_by_table {
            return self.split_bottom_merge_work();
        }
        let l1_tf = self.l1_trigger_files.load(Ordering::Relaxed);
        let l1_gate = if l1_tf > 0 {
            l1 >= l1_tf
        } else {
            l1 > 1
        };
        if l1_gate {
            return true;
        }
        if l1_tf > 0 && l1 > 0 {
            return false; // 延迟模式：L1 未达阈值，等批次到齐
        }
        let l2_gate = if self.l2_trigger_files > 0 {
            l2 >= self.l2_trigger_files
        } else {
            l2 > 1
        };
        l2_gate
    }

    /// 当前 L0 段数（层号 == 0）。
    pub fn l0_count(&self) -> usize {
        self.ssts.load().levels.iter().filter(|l| **l == 0).count()
    }

    /// W 项：合并紧迫度（跨列族调度优先级，越高越优先）——
    /// L0 段数压力 ×10（主因子）+ 大小软阈值超限 ×8。热段选段已由
    /// `select_compaction_inputs`（Ex-5.9）在列族内承担，此处为跨列族调度主因子。
    pub fn compaction_urgency(&self) -> u32 {
        let l0 = self.l0_count() as u32;
        let mut u = l0.saturating_mul(10);
        if self.l0_max_size_bytes > 0 && self.l0_bytes() > self.l0_max_size_bytes {
            u += 8;
        }
        u
    }

    /// 当前 L0 段文件总字节（快照 sizes 缓存求和，零 syscall——open/flush/compact
    /// 构建快照时缓存每段大小，写路径 needs_compact 不再逐次 fs::metadata）。
    pub fn l0_bytes(&self) -> u64 {
        let snap = self.ssts.load();
        let mut total = 0u64;
        for (i, s) in snap.ssts.iter().enumerate() {
            if snap.levels.get(i).copied().unwrap_or(0) == 0 {
                total += snap.sizes.get(i).copied().unwrap_or_else(|| s.file_len());
            }
        }
        total
    }
    /// 全部 SST 文件字节总和（快照 sizes 缓存，零 syscall）。
    pub fn sst_bytes(&self) -> u64 {
        let snap = self.ssts.load();
        snap.ssts
            .iter()
            .enumerate()
            .map(|(i, s)| snap.sizes.get(i).copied().unwrap_or_else(|| s.file_len()))
            .sum()
    }

    fn write_rows(&self, path: &Path, rows: &[BucketRow]) -> Result<SstFooter> {
        let mut w = SstWriter::new_with_pax(
            path,
            self.compression,
            self.compression_level,
            self.block_size,
            rows.len(),
            &self.pax_hot_fields,
            self.bloom_fpr,
        )?;
        for (k, v, flag, seq) in rows {
            if *flag == FLAG_DELETE {
                w.add_tombstone(k, *seq).expect("SST Tombstone 写入失败");
            } else {
                w.add(k, v.as_deref().unwrap_or(&[]), *seq)
                    .expect("SST 写入失败");
            }
        }
        w.finish()
    }

    /// 将 Immutable 落盘为 SST（Put 与 Tombstone 均落盘，保证跨 flush 删除一致）。
    fn write_sst(&self, path: &Path, imm: &MemTable) -> Result<SstFooter> {
        let mut w = SstWriter::new_with_pax(
            path,
            self.compression,
            self.compression_level,
            self.block_size,
            imm.len(),
            &self.pax_hot_fields,
            self.bloom_fpr,
        )?;
        imm.scan(|k, e| match &e.value {
            Some(v) => w.add(k, v, e.seq).expect("SST 写入失败"),
            None => w.add_tombstone(k, e.seq).expect("SST Tombstone 写入失败"),
        });
        w.finish()
    }

    fn persist_manifest(&self) -> Result<()> {
        // P73 修复：用**内存快照**（ssts ArcSwap + levels）重建清单，**不扫描磁盘**——
        // 无锁合并（P72）后 flush（Engine 写锁内写段文件）与合并（worker 无锁 persist）
        // 并发，磁盘扫描会引用"正在写入、尚未写完"的半写段文件 → manifest 悬空引用
        // 半写段 → 重启加载失败（SST seek 越界）。内存快照与 ssts store 原子一致
        // （本函数在 sst_mutate 锁内调用：store→persist 原子）。
        let snap = self.ssts.load();
        let mut files: Vec<String> = Vec::with_capacity(snap.ssts.len());
        let mut level_by_file: std::collections::HashMap<String, u32> =
            std::collections::HashMap::new();
        for (sst, lv) in snap.ssts.iter().zip(&snap.levels) {
            if let Some(name) = sst.path().file_name() {
                let name = name.to_string_lossy().to_string();
                files.push(name.clone());
                level_by_file.insert(name, *lv);
            }
        }
        files.sort_by(|a, b| b.cmp(a));
        let levels: Vec<u32> = files
            .iter()
            .map(|f| level_by_file.get(f).copied().unwrap_or(0))
            .collect();
        let m = Manifest {
            sst_files: files,
            levels,
            next_sst_id: self.next_sst_id.load(Ordering::Relaxed),
        };
        let text = serde_json::to_string_pretty(&m)
            .map_err(|e| Error::Serialize(format!("Manifest 序列化失败: {e}")))?;
        let tmp = self.dir.join("manifest.json.tmp");
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, self.dir.join(MANIFEST_FILE))?;
        Ok(())
    }

    /// 当前内存占用（字节），供 OOM Guardian / 监控使用。
    pub fn memtable_bytes(&self) -> usize {
        self.memtable.mutable_bytes() + self.memtable.immutable_bytes()
    }

    /// DROP TABLE purge：清空本列族全部数据（MemTable + SST + WAL），重写空 Manifest。
    /// 调用方须持有引擎级写锁（与 flush/写路径互斥）；与无锁后台 compact 经 `sst_mutate`
    /// 互斥（store→persist→remove 原子一致，同 finalize_compact）。删除失败的旧段文件
    /// （Windows 读句柄未释放）成为孤儿——Manifest 已空，重启不会加载。
    pub fn purge_data(&self) -> Result<()> {
        // ① 清空内存双缓冲（不刷盘：WAL 随后截断，整表删除语义）
        self.memtable.reset();
        // ② 段快照清空 + Manifest 空写 + 删旧段文件（P73 顺序：先 store 后删，持 sst_mutate）
        let _g = self.sst_mutate.lock().unwrap();
        let cur = self.ssts.load();
        let old: Vec<std::path::PathBuf> =
            cur.ssts.iter().map(|s| s.path().to_path_buf()).collect();
        self.ssts.store(Arc::new(SstSnapshot {
            ssts: Vec::new(),
            levels: Vec::new(),
            layer_ranges: Vec::new(),
            layer_indices: Vec::new(),
            l0_table_ranges: None,
            sizes: Vec::new(),
        }));
        self.seq_min.write().unwrap().clear();
        self.persist_manifest()?;
        for p in &old {
            let _ = std::fs::remove_file(p);
        }
        drop(_g);
        // ③ WAL 截断重建（内存 next_seq 保留递增；引擎级 global_seq 归零由 Engine::purge_all 负责）
        self.wal.lock().unwrap().truncate_and_reset()?;
        // ④ 段块缓存清空（本 CF 独立 BlockCache 实例，清空避免旧段块残留）
        self.block_cache.clear();
        Ok(())
    }

    pub fn sst_count(&self) -> usize {
        self.ssts.load().ssts.len()
    }

    /// M3（§26 多表，实施清单④）：物理删除**完全落在指定表 docid 区间**内的 SST 文件
    /// （表切分后每文件单表，min/max 同属该表即可整文件删；混表老文件 / 空段保守保留，
    /// 其表数据随逻辑删墓碑在后续压缩中回收）。返回删除文件数。
    /// 调用前提：表区间已先完成**逻辑删除**（multitable::drop_table_range 逐 docid 墓碑，
    /// 位图/MemTable/WAL 均含删除标记）——本函数只做磁盘文件级回收，不改变可见性语义。
    pub fn drop_table_range_files(&self, tid: u16) -> Result<usize> {
        let _g = self.sst_mutate.lock().unwrap();
        let cur = self.ssts.load();
        let in_table = |k: &[u8]| -> bool {
            crate::keys::decode_docid(k)
                .map(|d| ((d >> 48) as u16) == tid)
                .unwrap_or(false)
        };
        let mut removed: Vec<std::path::PathBuf> = Vec::new();
        let mut kept_ssts = Vec::new();
        let mut kept_levels = Vec::new();
        for (i, sst) in cur.ssts.iter().enumerate() {
            let whole = match sst.key_range() {
                Some((mn, mx)) => in_table(mn) && in_table(mx),
                None => false, // 空段 / 无范围 → 保守保留
            };
            if whole {
                removed.push(sst.path().to_path_buf());
            } else {
                kept_ssts.push(sst.clone());
                kept_levels.push(cur.levels[i]);
            }
        }
        if removed.is_empty() {
            return Ok(0);
        }
        // 原子发布（store → persist → remove，同 finalize_compact / purge_data）
        let (layer_ranges, layer_indices, l0_table_ranges) = Self::build_layer_meta(&kept_ssts, &kept_levels);
        let sizes: Vec<u64> = kept_ssts.iter().map(|r| r.file_len()).collect();
        self.ssts.store(Arc::new(SstSnapshot {
            ssts: kept_ssts,
            levels: kept_levels,
            layer_ranges,
            layer_indices,
            l0_table_ranges,
            sizes,
        }));
        self.persist_manifest()?;
        for p in &removed {
            let _ = std::fs::remove_file(p);
        }
        drop(_g);
        info!(
            "列族 [{}] DROP TABLE 表文件回收: 删除 {} 个表内 SST（table_id={tid}，全键属该表区间）",
            self.name,
            removed.len()
        );
        Ok(removed.len())
    }

    /// Ex-5.9：指定 SST 的读热度（冷热感知 Compaction 监控/策略数据源）。
    pub fn sst_heat(&self, idx: usize) -> u64 {
        self.ssts.load().ssts.get(idx).map_or(0, |s| s.heat())
    }

    /// 当前 WAL 下一可分配 seq（Engine 快照点来源，design 4.7 MVCC）。
    pub fn wal_next_seq(&self) -> u64 {
        self.wal.lock().unwrap().next_seq()
    }

    /// 接入外部全局 seq 计数器（MVCC，engine 层统一分配，M7-1）。
    /// 在 WAL 回放完成（内部 seq 已推进）后调用，此后写入走外部计数，跨列族一致。
    pub fn set_external_seq(&mut self, seq: Arc<AtomicU64>) {
        self.external_seq = Some(seq);
    }

    /// M3（§26 多表）：开启按表切分输出（仅主数据列族调用——全键为 docid 定长 8 字节，
    /// 高位 table_id 边界即文件边界；cidx 组合键 / outbox 等不定长键列族必须保持关闭）。
    pub fn enable_table_split(&mut self) {
        self.split_by_table = true;
    }

    /// 分配 seq 并追加 WAL 记录：外部全局 seq 优先（engine MVCC），否则内部自增。
    fn wal_append(&self, op: u8, key: &[u8], value: Option<&[u8]>) -> Result<u64> {
        match &self.external_seq {
            Some(ext) => {
                let seq = ext.fetch_add(1, Ordering::Relaxed);
                self.wal.lock().unwrap().append_at(op, key, value, seq)?;
                Ok(seq)
            }
            None => self.wal.lock().unwrap().append(op, key, value),
        }
    }

    /// 取 WAL 中 seq > `since_seq` 的记录（增量备份，M6-5）。
    /// 返回 `(最旧可用 seq, 过滤记录)`：`since_seq != 0` 且最旧可用 seq > since_seq+1 表示
    /// WAL 已被截断（环形覆盖 / 压缩），存在缺口 → 上层应改做全量备份。
    pub fn wal_records_since(
        &self,
        since_seq: u64,
    ) -> Result<(u64, Vec<crate::wal::WalRecord>)> {
        let recs = self.wal.lock().unwrap().recover_records()?;
        let oldest = recs.iter().map(|r| r.seq).min().unwrap_or(u64::MAX);
        let filtered: Vec<_> = recs.into_iter().filter(|r| r.seq > since_seq).collect();
        Ok((oldest, filtered))
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// 内部辅助
// ---------------------------------------------------------------------------

/// TTL 分桶行（key + 值 + flag + seq），flush_buckets / write_rows 使用。
type BucketRow = (Vec<u8>, Option<Vec<u8>>, u8, u64);

// ---------------------------------------------------------------------------
// TTL 时间分桶辅助（design 5.4，无 chrono 依赖的 civil calendar 天数计算）
// ---------------------------------------------------------------------------

/// 秒级时间戳 → UTC 纪元天数（整数除法，UTC 基准）。
fn epoch_days(secs: i64) -> i64 {
    secs.div_euclid(86_400)
}

/// 当前 UTC 纪元天数。
fn today_epoch_days() -> i64 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    epoch_days(secs)
}

/// 解析 TTL 桶 SST 文件名 `sst-{days:08}-{id:08}.sst` → 纪元天数；
/// 默认桶（无日期前缀 `sst-{id:08}.sst`）返回 None（永不过期）。
fn parse_sst_date(fname: &str) -> Option<i64> {
    let rest = fname.strip_prefix(SST_PREFIX)?;
    let (days_str, _rest) = rest.split_once('-')?;
    days_str.parse::<i64>().ok()
}

fn load_manifest(path: &Path) -> Result<(Vec<String>, Vec<u32>, u64)> {
    if !path.exists() {
        return Ok((Vec::new(), Vec::new(), 1));
    }
    let text = std::fs::read_to_string(path)?;
    let m: Manifest = serde_json::from_str(&text)
        .map_err(|e| Error::Corrupted(format!("Manifest 解析失败: {e}")))?;
    // 旧 Manifest 无 levels → 全部按 L0 处理（对齐长度）
    let levels = if m.levels.len() == m.sst_files.len() {
        m.levels
    } else {
        vec![0; m.sst_files.len()]
    };
    Ok((m.sst_files, levels, m.next_sst_id))
}

/// 同 key 候选合并：仅保留 seq 更大（更新）的版本；value=None 的 Tombstone 可覆盖旧值。
fn merge_candidate_bytes(
    merged: &mut std::collections::HashMap<Vec<u8>, (u64, Option<Vec<u8>>)>,
    key: Vec<u8>,
    seq: u64,
    value: Option<Vec<u8>>,
) {
    match merged.get(&key) {
        Some((old_seq, _)) if *old_seq >= seq => {}
        _ => {
            merged.insert(key, (seq, value));
        }
    }
}

/// Ex-8.2：扫描窗口与段 key 范围相交判定（闭区间，None 端无边界）。
/// 段范围未知（key_range=None：空段/缺侧）→ 保守返回 true 不跳过；精确判定，零假阴性。
/// 复用于 scan_stream_at / count_keys_range_filtered / scan_raw_range 的逐 SST 迭代器
/// 构建前——窗口只与相交段交互，非相交段免建 SstRangeIter（免 L2 定位/索引读）。
fn sst_intersects_window(
    sst: &SstReader,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
) -> bool {
    let Some((min, max)) = sst.key_range() else {
        return true;
    };
    if let Some(s) = start {
        if max < s {
            return false;
        }
    }
    if let Some(e) = end {
        if min > e {
            return false;
        }
    }
    true
}

/// 单 SST 等值查询（布隆剪枝 → 二分定位块 → 块缓存/读盘 → 块内扫描）。
/// 返回 `(value, seq)`：value=None 表示 Tombstone；整体 None 表示该 SST 无此 key。
fn get_from_sst(
    sst: &SstReader,
    cache: &BlockCache,
    key: &[u8],
) -> Result<Option<(Option<Vec<u8>>, u64)>> {
    // R 项：段级 Zone Map 粗筛——key 越出段范围 → O(1) 跳过（不做二分 + 布隆反序列化；
    // 精确判断，无假阴性）。
    if let Some((min, max)) = sst.key_range() {
        if key < min || key > max {
            return Ok(None);
        }
    }
    // v3/v4：整文件布隆粗筛
    if let Some(b) = sst.legacy_bloom() {
        if !b.maybe_contains(&key.to_vec()) {
            return Ok(None);
        }
    }
    // 等值定位块：借用精确索引二分，只克隆单条块条目（design 4.4.2 按需，避免克隆整个 Level 2）
    let Some((block_idx, entry)) = sst.locate_indexed_block(key)? else {
        return Ok(None);
    };
    // v5 分区布隆：只校验目标块（design 4.4.2，查询只加载目标块布隆）
    if let Some(pb) = sst.partition_blooms() {
        if let Some(bytes) = pb.get(block_idx) {
            if let Some(b) = BloomFilter::from_bytes(bytes) {
                if !b.maybe_contains(&key.to_vec()) {
                    return Ok(None);
                }
            }
        }
    }
    // Ex-5.9：布隆放行（真正读块）→ 读热度 +1（冷热感知 Compaction 数据源）
    sst.touch();
    let tid = sst.table_id().unwrap_or(0);
    let ck = BlockCacheKey::new(sst.path().to_path_buf(), entry.offset, tid);
    let block = if let Some(b) = cache.get(&ck) {
        b
    } else {
        let b = sst.read_block(&entry)?;
        cache.put(ck, b.clone());
        b
    };
    sst.scan_block_for_key(&block, key)
}

/// 单 SST 快照等值查询（S 项）：同 `get_from_sst`，但返回 **seq ≤ snapshot_seq** 的
/// 最大版本（块内多版本过滤；Tombstone value=None 保留）。整体 None = 该 SST 无 ≤ 快照版本。
fn get_from_sst_at(
    sst: &SstReader,
    cache: &BlockCache,
    key: &[u8],
    snapshot_seq: u64,
) -> Result<Option<(Option<Vec<u8>>, u64)>> {
    // R 项：段级 Zone Map 粗筛（同 get_from_sst）
    if let Some((min, max)) = sst.key_range() {
        if key < min || key > max {
            return Ok(None);
        }
    }
    if let Some(b) = sst.legacy_bloom() {
        if !b.maybe_contains(&key.to_vec()) {
            return Ok(None);
        }
    }
    let Some((block_idx, entry)) = sst.locate_indexed_block(key)? else {
        return Ok(None);
    };
    if let Some(pb) = sst.partition_blooms() {
        if let Some(bytes) = pb.get(block_idx) {
            if let Some(b) = BloomFilter::from_bytes(bytes) {
                if !b.maybe_contains(&key.to_vec()) {
                    return Ok(None);
                }
            }
        }
    }
    sst.touch();
    let tid = sst.table_id().unwrap_or(0);
    let ck = BlockCacheKey::new(sst.path().to_path_buf(), entry.offset, tid);
    let block = if let Some(b) = cache.get(&ck) {
        b
    } else {
        let b = sst.read_block(&entry)?;
        cache.put(ck, b.clone());
        b
    };
    sst.scan_block_for_key_at(&block, key, snapshot_seq)
}

/// 单 SST 批量等值查询（N 项）：整文件布隆粗筛 → 逐 key 二分定位数据块 → 按块分组 →
/// 分区布隆校验 → 每块只读一次（块缓存/磁盘）→ 块内一次扫描命中全部 key。
/// 返回与 `idxs`（输入原始下标）对齐：`Some((value, seq))`（value=None = Tombstone）/
/// `None`（该 SST 无此 key，调用方继续下沉旧层）。
fn get_many_from_sst(
    sst: &SstReader,
    cache: &BlockCache,
    keys: &[Vec<u8>],
    idxs: &[usize],
) -> Result<Vec<Option<(Option<Vec<u8>>, u64)>>> {
    let mut out: Vec<Option<(Option<Vec<u8>>, u64)>> = vec![None; idxs.len()];
    if idxs.is_empty() {
        return Ok(out);
    }
    let legacy = sst.legacy_bloom();
    // R 项：段级 Zone Map 粗筛（取一次，逐 key 判断；越界 key O(1) 跳过）
    let seg_range = sst.key_range();
    // 定位：原始下标 + 块号 + 块条目（借用精确索引二分，只克隆单条条目）
    let mut located: Vec<(usize, usize, IndexEntry)> = Vec::new();
    for &i in idxs {
        let k = &keys[i];
        if let Some((min, max)) = seg_range {
            if k.as_slice() < min || k.as_slice() > max {
                continue;
            }
        }
        if let Some(b) = legacy {
            if !b.maybe_contains(k) {
                continue;
            }
        }
        if let Some((block_idx, entry)) = sst.locate_indexed_block(k)? {
            located.push((i, block_idx, entry));
        }
    }
    if located.is_empty() {
        return Ok(out);
    }
    // 按块分组（块号升序 → 块读取顺序化）
    located.sort_by_key(|&(_, bi, _)| bi);
    let slot_of: std::collections::HashMap<usize, usize> = idxs
        .iter()
        .enumerate()
        .map(|(slot, &i)| (i, slot))
        .collect();
    let mut pos = 0usize;
    while pos < located.len() {
        let block_idx = located[pos].1;
        let mut end = pos;
        while end < located.len() && located[end].1 == block_idx {
            end += 1;
        }
        // 分区布隆（v5）校验：放行的 key 才真正读块
        let mut targets: Vec<usize> = Vec::new();
        let mut pruned = false;
        if let Some(pb) = sst.partition_blooms() {
            if let Some(bytes) = pb.get(block_idx) {
                if let Some(b) = BloomFilter::from_bytes(bytes) {
                    for &(i, _, _) in &located[pos..end] {
                        if b.maybe_contains(&keys[i]) {
                            targets.push(i);
                        }
                    }
                    pruned = true;
                }
            }
        }
        if !pruned {
            targets.extend(located[pos..end].iter().map(|&(i, _, _)| i));
        }
        if !targets.is_empty() {
            // 布隆放行 → 读热度 +1（冷热感知 Compaction 数据源，与 get_from_sst 一致）
            sst.touch();
            let entry = located[pos].2.clone();
            let tid = sst.table_id().unwrap_or(0);
            let ck = BlockCacheKey::new(sst.path().to_path_buf(), entry.offset, tid);
            let block = if let Some(b) = cache.get(&ck) {
                b
            } else {
                let b = sst.read_block(&entry)?;
                cache.put(ck, b.clone());
                b
            };
            let target_set: std::collections::HashSet<Vec<u8>> =
                targets.iter().map(|&i| keys[i].clone()).collect();
            for (k, v, seq) in sst.scan_block_for_keys(&block, &target_set)? {
                if let Some(i) = targets.iter().copied().find(|&i| keys[i] == k) {
                    if let Some(&slot) = slot_of.get(&i) {
                        out[slot] = Some((v, seq));
                    }
                }
            }
        }
        pos = end;
    }
    Ok(out)
}

/// 单 SST 投影字段批量等值查询（P87②）：定位/分组/读块流程与 `get_many_from_sst`
/// 一致，但块内解码走 `scan_block_for_keys_fields`（PAX 列解码 / 行式按需提取）。
/// 返回与 `idxs` 对齐：`Some((Some(fields), seq))` = 命中（fields 为请求字段值列表）、
/// `Some((None, _))` = Tombstone、`None` = 该 SST 无此 key（调用方继续下沉）。
fn get_many_fields_from_sst(
    sst: &SstReader,
    cache: &BlockCache,
    keys: &[Vec<u8>],
    idxs: &[usize],
    fields: &[String],
) -> Result<Vec<Option<(Option<Vec<Option<Vec<u8>>>>, u64)>>> {
    let mut out: Vec<Option<(Option<Vec<Option<Vec<u8>>>>, u64)>> = vec![None; idxs.len()];
    if idxs.is_empty() {
        return Ok(out);
    }
    let legacy = sst.legacy_bloom();
    let seg_range = sst.key_range();
    let mut located: Vec<(usize, usize, IndexEntry)> = Vec::new();
    for &i in idxs {
        let k = &keys[i];
        if let Some((min, max)) = seg_range {
            if k.as_slice() < min || k.as_slice() > max {
                continue;
            }
        }
        if let Some(b) = legacy {
            if !b.maybe_contains(k) {
                continue;
            }
        }
        if let Some((block_idx, entry)) = sst.locate_indexed_block(k)? {
            located.push((i, block_idx, entry));
        }
    }
    if located.is_empty() {
        return Ok(out);
    }
    located.sort_by_key(|&(_, bi, _)| bi);
    let slot_of: std::collections::HashMap<usize, usize> = idxs
        .iter()
        .enumerate()
        .map(|(slot, &i)| (i, slot))
        .collect();
    let mut pos = 0usize;
    while pos < located.len() {
        let block_idx = located[pos].1;
        let mut end = pos;
        while end < located.len() && located[end].1 == block_idx {
            end += 1;
        }
        let mut targets: Vec<usize> = Vec::new();
        let mut pruned = false;
        if let Some(pb) = sst.partition_blooms() {
            if let Some(bytes) = pb.get(block_idx) {
                if let Some(b) = BloomFilter::from_bytes(bytes) {
                    for &(i, _, _) in &located[pos..end] {
                        if b.maybe_contains(&keys[i]) {
                            targets.push(i);
                        }
                    }
                    pruned = true;
                }
            }
        }
        if !pruned {
            targets.extend(located[pos..end].iter().map(|&(i, _, _)| i));
        }
        if !targets.is_empty() {
            sst.touch();
            let entry = located[pos].2.clone();
            let tid = sst.table_id().unwrap_or(0);
            let ck = BlockCacheKey::new(sst.path().to_path_buf(), entry.offset, tid);
            let block = if let Some(b) = cache.get(&ck) {
                b
            } else {
                let b = sst.read_block(&entry)?;
                cache.put(ck, b.clone());
                b
            };
            let target_set: std::collections::HashSet<Vec<u8>> =
                targets.iter().map(|&i| keys[i].clone()).collect();
            for (k, vals, seq) in sst.scan_block_for_keys_fields(&block, &target_set, fields)? {
                if let Some(i) = targets.iter().copied().find(|&i| keys[i] == k) {
                    if let Some(&slot) = slot_of.get(&i) {
                        out[slot] = Some((vals, seq));
                    }
                }
            }
        }
        pos = end;
    }
    Ok(out)
}

/// 扫描 Immutable 记录的最大 seq（环形 WAL 覆盖安全游标：<= 该 seq 均已刷盘）。
fn imm_scan_max_seq(imm: &MemTable) -> u64 {
    let mut max_seq = 0u64;
    imm.scan(|_, e| {
        if e.seq > max_seq {
            max_seq = e.seq;
        }
    });
    max_seq
}

/// M3（§26 多表）：docid 定长键 → table_id（`docid >> 48`）。
/// 仅 8 字节定长 docid 键可解析（主数据列族）；组合键等不定长键返回 None（不切分）。
/// 字节序 == 数值序（keys 大端规范）→ 同表键在扫描序中天然连续，表边界即切分点。
fn key_table_id(key: &[u8]) -> Option<u16> {
    let docid = crate::keys::decode_docid(key).ok()?;
    Some((docid >> 48) as u16)
}

/// 选择本轮压实输入（design 4.5 二期 Leveled，M6-2）：
/// - L0 ≥ 2 段且 L1 文件数 < `limit` → 合并**仅 L0**，输出 L1（单次压实量 = 刷盘批次，有界）；
/// - L0 ≥ 2 段且 L1 已满 → 合并 L0 + 全部 L1 → L1（收敛 L1 文件数）；
/// - L0 空且 L1 > 1 → 合并全部 L1 → L2（压实下沉）；
/// - L0/L1 均空且 L2 > 1 → 合并 L2（异常残留收敛）。
/// 单段 L0（未达 2 段）不压实——等待更多刷盘批次，避免无收益重写。
///
/// Ex-5.9 冷热感知：L0 段数超过 `limit`（逼近写 Stall）且存在热度数据时，**优先合并最热的
/// `limit` 段**（热段先下沉 L1 聚合，热点读路径段数更快减少）；无热度数据维持全量合并。
///
/// 返回 `(选中段下标, 输出层)`；无需要压实的输入时返回 `(空, 0)`。
/// L 项：`cooling` = 冷却期段下标集合（合并冷却，新段 N 轮内**优先**不参与合并）。
/// 冷却为「软约束」：若冷却导致无可合并候选（候选 < 2），回退含冷却段——
/// 防止冷却挡住收敛（needs_compact 用原始计数触发时，硬排除会造成合并空转死循环）。
/// 既有测试/调用入口：6 参（Ex-8.11 触发器 0 = 现行为）。
fn select_compaction_inputs(
    levels: &[u32],
    level_limit: usize,
    heat: &[u64],
    cooling: &std::collections::HashSet<usize>,
    sizes: &[u64],
    max_input_bytes: u64,
) -> (Vec<usize>, u32) {
    select_compaction_inputs_ex(levels, level_limit, 0, 0, heat, cooling, sizes, max_input_bytes)
}

/// Ex-8.11：带 L1/L2 触发阈值的选段（l1/l2_trigger=0 = 现行为）。
fn select_compaction_inputs_ex(
    levels: &[u32],
    level_limit: usize,
    l1_trigger: usize,
    l2_trigger: usize,
    heat: &[u64],
    cooling: &std::collections::HashSet<usize>,
    sizes: &[u64],
    max_input_bytes: u64,
) -> (Vec<usize>, u32) {
    let (mut sel, out) = select_inner(levels, level_limit, l1_trigger, l2_trigger, heat, cooling);
    // 单次合并输入大小上限（仅 L0→L1：L0 允许重叠，分批安全；L1→L2 全选保证层内不重叠）
    if sel.len() >= 2 && out == 1 && max_input_bytes > 0 {
        sel = cap_by_size(sel, sizes, max_input_bytes);
    }
    if sel.len() >= 2 {
        return (sel, out);
    }
    // L 项回退：候选不足（冷却挡住收敛）→ 忽略冷却再选（保证收敛 / 防写 Stall）
    select_inner(
        levels,
        level_limit,
        l1_trigger,
        l2_trigger,
        heat,
        &std::collections::HashSet::new(),
    )
}

/// 分批合并：输入总大小超 `max` 时，从尾部（排序后 = 最冷/最大）移除段直到 ≤ max；
/// 至少保留 2 段（保证本轮有进展，剩余段由 worker 后续轮次收敛）。
fn cap_by_size(mut sel: Vec<usize>, sizes: &[u64], max: u64) -> Vec<usize> {
    if sel.len() <= 2 {
        return sel;
    }
    let total: u64 = sel.iter().map(|&i| sizes.get(i).copied().unwrap_or(0)).sum();
    if total <= max {
        return sel;
    }
    let mut acc = total;
    let mut keep = sel.len();
    for (k, &i) in sel.iter().enumerate().rev() {
        if keep <= 2 {
            break;
        }
        acc -= sizes.get(i).copied().unwrap_or(0);
        keep = k;
        if acc <= max {
            break;
        }
    }
    sel.truncate(keep.max(2));
    sel
}

fn select_inner(
    levels: &[u32],
    level_limit: usize,
    l1_trigger: usize,
    l2_trigger: usize,
    heat: &[u64],
    cooling: &std::collections::HashSet<usize>,
) -> (Vec<usize>, u32) {
    let l0: Vec<usize> = levels
        .iter()
        .enumerate()
        .filter(|(_, l)| **l == 0)
        .filter(|(i, _)| !cooling.contains(i))
        .map(|(i, _)| i)
        .collect();
    let l1: Vec<usize> = levels
        .iter()
        .enumerate()
        .filter(|(_, l)| **l == 1)
        .filter(|(i, _)| !cooling.contains(i))
        .map(|(i, _)| i)
        .collect();
    let l2: Vec<usize> = levels
        .iter()
        .enumerate()
        .filter(|(_, l)| **l >= 2)
        .filter(|(i, _)| !cooling.contains(i))
        .map(|(i, _)| i)
        .collect();
    if !l0.is_empty() {
        if l0.len() < 2 {
            return (Vec::new(), 0); // 单个 L0 段（或冷却后不足 2）：暂不压实（无收益重写）
        }
        if l1.len() < level_limit.max(1) {
            // Ex-5.9：L0 超阈值且存在热度 → 优先合并最热的 level_limit 段（热段先下沉 L1）
            if l0.len() > level_limit && heat.iter().any(|h| *h > 0) {
                let mut ranked = l0.clone();
                ranked.sort_by(|a, b| heat[*b].cmp(&heat[*a]).then(a.cmp(b)));
                ranked.truncate(level_limit.max(1));
                (ranked, 1)
            } else {
                (l0, 1)
            }
        } else {
            let mut sel = l0.clone();
            sel.extend(l1);
            (sel, 1)
        }
    } else if l1_trigger == 0 && l1.len() > 1 {
        (l1, 2)
    } else if l1_trigger > 0 && l1.len() >= l1_trigger {
        // Ex-8.11：延迟大合并——L0 空且 L1 攒够阈值 → 一次下沉 L2
        (l1, 2)
    } else if l1_trigger > 0 && !l1.is_empty() {
        (Vec::new(), 0) // 延迟模式：L1 未达阈值，等批次到齐（不提前收敛 L2）
    } else if l2_trigger == 0 && l2.len() > 1 {
        (l2, 2)
    } else if l2_trigger > 0 && l2.len() >= l2_trigger {
        (l2, 2) // Ex-8.11：L2 攒够阈值才收敛为单段
    } else {
        (Vec::new(), 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let name = format!("cf-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    fn small_cfg(max_mb: usize) -> Config {
        let mut cfg = Config::default();
        cfg.memtable.max_size_mb = max_mb;
        cfg.blockcache.block_size_kb = 1;
        cfg.sstable.compression = "none".into();
        cfg
    }

    // ---------- 流式 scan（M8-P10） ----------

    #[test]
    fn scan_stream_matches_scan_raw_range() {
        let dir = tmp();
        let cfg = small_cfg(16); // 小阈值 → 多次 flush → 多 SST 源
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 批量写入并周期性 flush（产生多个 SST：k-way merge 多源）
        for i in 0..2_000u64 {
            cf.put_bytes_nosync(
                format!("k{i:08}").into_bytes(),
                format!("v{i}").into_bytes(),
            )
            .unwrap();
            if i % 500 == 499 {
                cf.switch_and_flush().unwrap();
            }
        }
        // memtable 新版本：覆盖 + 删除
        cf.put_bytes_nosync(b"k00000042".to_vec(), b"updated".to_vec())
            .unwrap();
        cf.delete_bytes(b"k00000100".to_vec()).unwrap();
        cf.sync_wal().unwrap();

        // 全量（旧路径）vs 流式（新路径）：结果完全一致
        let all = cf.scan_raw_range(None, None).unwrap();
        let mut streamed: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        cf.scan_stream(None, None, |k, v| {
            streamed.push((k.to_vec(), v.to_vec()));
            Ok(true)
        })
        .unwrap();
        assert_eq!(streamed.len(), all.len(), "流式行数应与全量一致");
        for (a, b) in streamed.iter().zip(all.iter()) {
            assert_eq!(a.0, b.0, "key 顺序一致");
            assert_eq!(a.1, b.1, "value 一致（含覆盖后的新值）");
        }
        // 语义校验：覆盖生效、删除隐藏
        let hit = all.iter().find(|(k, _)| k == b"k00000042").unwrap();
        assert_eq!(hit.1, b"updated");
        assert!(
            !all.iter().any(|(k, _)| k == b"k00000100"),
            "被删除的 key 不应出现"
        );
        // 范围过滤：流式与全量一致
        let ra = cf
            .scan_raw_range(Some(&b"k00001000"[..]), Some(&b"k00001010"[..]))
            .unwrap();
        let mut rs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        cf.scan_stream(
            Some(&b"k00001000"[..]),
            Some(&b"k00001010"[..]),
            |k, v| {
                rs.push((k.to_vec(), v.to_vec()));
                Ok(true)
            },
        )
        .unwrap();
        assert_eq!(rs, ra, "范围过滤流式应与全量一致");
    }

    // ---------- WAL 截断（M8-P5） ----------

    #[test]
    fn wal_truncated_after_flush_keeps_data() {
        let dir = tmp();
        let cfg = small_cfg(64);
        let wal_path = dir.join(WAL_FILE);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..5_000u64 {
                cf.put_bytes_nosync(i.to_be_bytes().to_vec(), format!("doc-{i}").into_bytes())
                    .unwrap();
            }
            cf.sync_wal().unwrap();
            let before = std::fs::metadata(&wal_path).unwrap().len();
            cf.switch_and_flush().unwrap(); // flush → WAL 截断
            let after = std::fs::metadata(&wal_path).unwrap().len();
            assert!(
                after < before && after < 64,
                "flush 后 WAL 应截断为小文件（before={before} after={after}）"
            );
        }
        // 重开：数据完整（从 SST + WAL 恢复），seq 接续
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.get(0).unwrap().unwrap().0, b"doc-0");
        assert_eq!(cf2.get(4_999).unwrap().unwrap().0, b"doc-4999");
        // 新写入 seq 接续（不回到 1）：头持久化 next_seq
        let next = cf2.wal_next_seq();
        assert!(
            next >= 5_001,
            "重开后 next_seq 应接续（>=5001），实际 {next}"
        );
    }

    #[test]
    fn wal_header_persists_next_seq_across_restart() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let wal_path = dir.join(WAL_FILE);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in 0..300u64 {
            cf.put_bytes_nosync(i.to_be_bytes().to_vec(), format!("v{i}").into_bytes())
                .unwrap();
        }
        cf.sync_wal().unwrap();
        cf.switch_and_flush().unwrap(); // 截断，头写入 next_seq=301
        let first_after_flush = cf.wal_next_seq();
        drop(cf);
        // 重开：next_seq 从头恢复（>300），而非 1
        let cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(
            cf2.wal_next_seq(),
            first_after_flush,
            "重开 next_seq 应接续（头持久化）"
        );
        assert!(
            std::fs::metadata(&wal_path).unwrap().len() < 64,
            "WAL 保持小文件"
        );
    }

    #[test]
    fn old_wal_without_header_still_recovers() {
        // 兼容：旧格式 WAL（无头，M8-P5 之前）照常回放恢复
        let dir = tmp();
        let cfg = small_cfg(64);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..100u64 {
                cf.put_bytes_nosync(i.to_be_bytes().to_vec(), format!("v{i}").into_bytes())
                    .unwrap();
            }
            cf.sync_wal().unwrap();
            // 不 flush：WAL 保留记录（旧格式无头）
        }
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(
            cf2.get(99).unwrap().unwrap().0,
            b"v99",
            "旧格式 WAL 应回放恢复"
        );
        assert!(cf2.wal_next_seq() >= 101, "旧 WAL 回放后 seq 接续");
    }

    #[test]
    fn put_get_roundtrip() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(1, b"doc-1".to_vec()).unwrap();
        cf.put(2, b"doc-2".to_vec()).unwrap();
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"doc-1");
        assert_eq!(cf.get(2).unwrap().unwrap().0, b"doc-2");
        assert!(cf.get(99).unwrap().is_none());
    }

    #[test]
    fn overwrite_returns_latest() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(7, b"v1".to_vec()).unwrap();
        cf.put(7, b"v2".to_vec()).unwrap();
        assert_eq!(cf.get(7).unwrap().unwrap().0, b"v2");
    }

    #[test]
    fn delete_hides_key() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(3, b"x".to_vec()).unwrap();
        cf.delete(3).unwrap();
        assert!(cf.get(3).unwrap().is_none());
    }

    #[test]
    fn delete_survives_flush_and_restart() {
        // 步骤 9：Tombstone 必须落盘——删除后刷盘 + 重启，key 依然不存在
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            cf.put(1, b"v1".to_vec()).unwrap();
            cf.put(2, b"v2".to_vec()).unwrap();
            cf.put(3, b"v3".to_vec()).unwrap();
            cf.switch_and_flush().unwrap(); // 全部落盘
            cf.delete(2).unwrap();
            cf.delete(3).unwrap();
            cf.switch_and_flush().unwrap(); // Tombstone 落盘
        }
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.get(1).unwrap().unwrap().0, b"v1");
        assert!(cf2.get(2).unwrap().is_none(), "删除后重启应不存在");
        assert!(cf2.get(3).unwrap().is_none(), "删除后重启应不存在");
        // 范围扫描同样过滤
        let rows = cf2.scan_range(None, None).unwrap();
        assert_eq!(rows, vec![(1, b"v1".to_vec())]);
    }

    #[test]
    fn delete_overrides_older_sst_value() {
        // 旧 SST 有值、新 SST 有 Tombstone：读路径按新→旧应命中 Tombstone → 不存在
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(9, b"old-value".to_vec()).unwrap();
        cf.switch_and_flush().unwrap();
        // 模拟"删除发生在更晚的时刻"：直接写 tombstone 到新 memtable 并刷盘
        cf.delete(9).unwrap();
        cf.switch_and_flush().unwrap();
        assert!(cf.get(9).unwrap().is_none());
        assert!(cf.scan_range(None, None).unwrap().is_empty());
    }

    #[test]
    fn compact_merges_ssts_preserving_overwrite_and_delete() {
        // design 4.5 阶段 3：多次刷盘 → 全量合并，覆盖/删除语义保留
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 3 个 SST：含跨段覆盖 + 删除
        cf.put(1, b"v1".to_vec()).unwrap();
        cf.put(2, b"v2".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST1
        cf.put(2, b"v2b".to_vec()).unwrap(); // 跨段覆盖
        cf.put(3, b"v3".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST2
        cf.delete(3).unwrap(); // 删除
        cf.put(4, b"v4".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST3

        let before = cf.sst_count();
        assert!(before >= 3, "应产生多个 SST: {before}");
        let rep = cf.compact().unwrap();
        assert_eq!(rep.merged_ssts, before);
        assert!(rep.freed_bytes > 0, "合并应释放空间");
        assert_eq!(cf.sst_count(), 1, "合并后只剩 1 个 SST");

        // 语义保持
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"v1");
        assert_eq!(cf.get(2).unwrap().unwrap().0, b"v2b", "后写覆盖先写");
        assert!(cf.get(3).unwrap().is_none(), "删除后不可见");
        assert_eq!(cf.get(4).unwrap().unwrap().0, b"v4");
        let rows = cf.scan_range(None, None).unwrap();
        assert_eq!(
            rows,
            vec![
                (1, b"v1".to_vec()),
                (2, b"v2b".to_vec()),
                (4, b"v4".to_vec())
            ]
        );

        // 重启后 Manifest 只含新段，数据完整
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.sst_count(), 1);
        assert_eq!(cf2.get(2).unwrap().unwrap().0, b"v2b");
        assert!(cf2.get(3).unwrap().is_none());
    }

    #[test]
    fn compact_filtered_physically_drops_deleted_keys() {
        // Ex-5.6：删除位图过滤——合并时按 drop_key 物理丢弃（不保留数据、不写 Tombstone），
        // 位图已删 docid 的旧数据随压实直接回收（墓碑不污染层级）。
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(1, b"v1".to_vec()).unwrap();
        cf.put(2, b"v2".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST1
        cf.put(3, b"v3".to_vec()).unwrap();
        cf.put(4, b"v4".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST2

        // 模拟删除位图：docid 2、4 已删（主键为 8 字节大端）
        let deleted = |k: &[u8]| {
            k.len() == 8
                && matches!(u64::from_be_bytes(k.try_into().unwrap()), 2 | 4)
        };
        let rep = cf.compact_filtered(&deleted).unwrap();
        assert!(rep.merged_ssts >= 2, "应合并多段: {}", rep.merged_ssts);
        assert_eq!(cf.sst_count(), 1);

        // 已删 key 物理消失：读不到、扫描无记录（数据不在磁盘，非内存过滤）
        assert!(cf.get(2).unwrap().is_none());
        assert!(cf.get(4).unwrap().is_none());
        let rows = cf.scan_range(None, None).unwrap();
        assert_eq!(rows, vec![(1, b"v1".to_vec()), (3, b"v3".to_vec())]);

        // 重启后同样物理消失（新段 Manifest 不含已删键）
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        let rows = cf2.scan_range(None, None).unwrap();
        assert_eq!(rows, vec![(1, b"v1".to_vec()), (3, b"v3".to_vec())]);
        assert!(cf2.get(2).unwrap().is_none());
    }

    #[test]
    fn compact_filtered_keeps_other_tombstones() {
        // Ex-5.6：过滤只丢弃位图已删 key；其他 Tombstone（非位图路径写入）语义保留
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(1, b"v1".to_vec()).unwrap();
        cf.put(2, b"v2".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST1
        cf.delete(2).unwrap(); // 传统 Tombstone（位图未删 docid 2 的场景不在此测——仅验证 Tombstone 保留）
        cf.put(3, b"v3".to_vec()).unwrap();
        cf.switch_and_flush().unwrap(); // SST2

        let deleted = |_k: &[u8]| false; // 空过滤（无位图删除）
        cf.compact_filtered(&deleted).unwrap();
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"v1");
        assert!(cf.get(2).unwrap().is_none(), "Tombstone 语义保留");
        assert_eq!(cf.get(3).unwrap().unwrap().0, b"v3");
    }

    #[test]
    fn compact_gc_rewrites_single_segment_dropping_deleted_keys() {
        // Ex-8.7：收敛为单段（常规 select 无多段候选）——`compact_gc(allow_single=true)`
        // 单段重写按位图已删键物理丢弃回收空间；`allow_single=false`（传统路径）不重写。
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for d in 1..=40u64 {
            cf.put(d, format!("v{d}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        for d in 41..=80u64 {
            cf.put(d, format!("v{d}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        let rep = cf.compact().unwrap();
        assert_eq!(rep.merged_ssts, 2, "常规合并收敛");
        assert_eq!(cf.sst_count(), 1, "已收敛为单段");

        let deleted = |k: &[u8]| {
            k.len() == 8 && {
                let id = u64::from_be_bytes(k.try_into().unwrap());
                id % 3 == 0
            }
        };
        // 传统路径（allow_single=false）：单段不重写（空转）
        let rep0 = cf.compact_gc(&deleted, false).unwrap();
        assert_eq!(rep0.merged_ssts, 0, "allow_single=false 不重写单段");
        // 删除密度 GC：单段重写 → 1/3 键物理丢弃
        let rep = cf.compact_gc(&deleted, true).unwrap();
        assert_eq!(rep.merged_ssts, 1, "单段重写 1 段");
        assert!(rep.dropped_keys > 0, "应物理丢弃已删键: {}", rep.dropped_keys);
        assert!(rep.freed_bytes > 0, "重写应释放空间");
        let rows = cf.scan_range(None, None).unwrap();
        assert!(rows.iter().all(|(d, _)| d % 3 != 0), "已删键全部消失");
        assert_eq!(rows.len(), 54, "80 - ⌊80/3⌋ = 54 存活");
        // 二次 GC：无垃圾可丢 → 0 丢弃（引擎排空收敛判定依据）
        let rep2 = cf.compact_gc(&deleted, true).unwrap();
        assert_eq!(rep2.dropped_keys, 0, "无已删键 → 0 丢弃收敛");
        // 重启后物理态一致
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.scan_range(None, None).unwrap().len(), 54);
    }

    #[test]
    fn compact_reuses_blocks_when_no_overlap() {
        // Ex-5.8 元数据-数据解耦：无重叠 L0 段合并走数据块级复用（只重建元数据区，
        // 数据块零解压）——合并后数据完整、跨重启一致。
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 段 A：键 0..500；段 B：键 500..1000（key 范围无重叠）
        for i in 0..500u64 {
            cf.put(i, format!("v{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        for i in 500..1000u64 {
            cf.put(i, format!("v{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.sst_count(), 2, "两个无重叠 L0 段");

        let rep = cf.compact().unwrap();
        assert_eq!(rep.merged_ssts, 2, "应合并 2 段（块级复用）");
        assert_eq!(cf.sst_count(), 1, "合并后单段");
        // 数据完整（块级复用后所有键可读）
        for i in [0u64, 1, 499, 500, 501, 999] {
            assert_eq!(
                cf.get(i).unwrap().unwrap().0,
                format!("v{i}").into_bytes(),
                "键 {i} 复用后仍可读"
            );
        }
        assert!(cf.get(1000).unwrap().is_none());

        // 跨重启：Manifest 只含新段，数据完整
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.sst_count(), 1);
        assert_eq!(cf2.get(0).unwrap().unwrap().0, b"v0");
        assert_eq!(cf2.get(999).unwrap().unwrap().0, b"v999");
    }

    #[test]
    fn compact_full_merge_when_overlap() {
        // Ex-5.8 回退验证：有重叠 L0 段合并必须走全量路径（覆盖/去重语义保留），
        // 块级复用检测应返回 None 且结果与旧全量合并一致。
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 段 A：键 0..300；段 B：键 200..500（[200,300) 重叠）
        for i in 0..300u64 {
            cf.put(i, format!("va-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        for i in 200..500u64 {
            cf.put(i, format!("vb-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.sst_count(), 2);

        let rep = cf.compact().unwrap();
        assert_eq!(rep.merged_ssts, 2);
        assert_eq!(cf.sst_count(), 1);
        // 重叠区后写覆盖先写（B 段更新）
        assert_eq!(cf.get(250).unwrap().unwrap().0, b"vb-250");
        assert_eq!(cf.get(100).unwrap().unwrap().0, b"va-100");
        assert_eq!(cf.get(499).unwrap().unwrap().0, b"vb-499");
        // 全量合并（kept_keys > 0 表示有去重消除）
        assert_eq!(cf.get(300).unwrap().unwrap().0, b"vb-300");
    }

    #[test]
    fn compact_noop_when_single_sst() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(1, b"x".to_vec()).unwrap();
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.sst_count(), 1);
        let rep = cf.compact().unwrap();
        assert_eq!(rep.merged_ssts, 0, "单段不需要合并");
        assert_eq!(cf.sst_count(), 1);
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"x");
    }

    // ---------- Leveled-Compaction（design 4.5 二期，M6-2） ----------

    #[test]
    fn leveled_compact_promotes_l0_to_l1_and_persists() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 两批写入并强制刷盘 → 2 个 L0 段
        for i in 0..100u64 {
            cf.put(i, format!("a{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        for i in 100..200u64 {
            cf.put(i, format!("a{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.ssts.load().levels, vec![0, 0], "刷盘产物均为 L0");
        assert!(!cf.needs_compact(), "2 个 L0 未超阈值");
        // 手动压实 → L1
        let rep = cf.compact().unwrap();
        assert_eq!(rep.out_level, 1);
        assert_eq!(cf.sst_count(), 1);
        assert_eq!(cf.ssts.load().levels, vec![1]);
        // 数据完整
        for i in (0..200u64).step_by(7) {
            assert!(cf.get(i).unwrap().is_some());
        }
        // 重启：Manifest 持久化层号
        drop(cf);
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.ssts.load().levels, vec![1], "Manifest 应持久化层号");
        assert!(cf2.get(150).unwrap().is_some());
    }

    #[test]
    fn leveled_compact_sinks_l1_to_l2() {
        let dir = tmp();
        let mut cfg = small_cfg(256);
        cfg.storage.l1_trigger_files = 0; // 本测针对 L1→L2 下沉语义（不受 Ex-8.11 默认 8 延迟影响）
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 每轮刷 2 个 L0 段并压实 → L0 合并下沉为 1 个 L1 段；4 轮后 L1 累计 4 个文件
        for round in 0..4u64 {
            for _ in 0..2u64 {
                for i in 0..50u64 {
                    cf.put(round * 100 + i, format!("v{round}-{i}").into_bytes())
                        .unwrap();
                }
                cf.switch_and_flush().unwrap();
            }
            let r = cf.compact().unwrap();
            assert_eq!(r.out_level, 1, "第 {round} 轮 L0→L1");
        }
        assert_eq!(cf.ssts.load().levels.iter().filter(|l| **l == 1).count(), 4);
        // L0 空、L1 > 1 → L1 → L2（压实下沉）。L 项合并冷却：新生成段 N 轮内不参与下一轮
        // 合并 → 收敛需多轮（冷却段到期后正常参与），循环 compact 直到收敛
        assert!(cf.needs_compact(), "L1 多段应触发 L1→L2");
        let mut guard = 0;
        while cf.needs_compact() && guard < 8 {
            cf.compact().unwrap();
            guard += 1;
        }
        assert!(guard < 8, "冷却后多轮应收敛");
        assert_eq!(cf.sst_count(), 1);
        assert_eq!(cf.ssts.load().levels, vec![2]);
        // 全量数据仍完整
        for round in 0..4u64 {
            for i in (0..50u64).step_by(9) {
                assert!(
                    cf.get(round * 100 + i).unwrap().is_some(),
                    "round {round} key {i} 丢失"
                );
            }
        }
        // 重启：L2 持久化 + 数据完整
        drop(cf);
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.ssts.load().levels, vec![2]);
        assert!(cf2.get(300 + 10).unwrap().is_some());
    }

    #[test]
    fn tiered_compression_level_for_and_l2_cold_preserves_data() {
        // Ex-8.12：分层压缩——L2+ 用冷档 zstd level，L0/L1 用热档；跨档合并禁块级复用
        // （L1 热档段下沉 L2 必须全量重压缩）；收敛后 L2 单段 + 数据完整 + 重启可读
        let dir = tmp();
        let mut cfg = Config::default(); // compression=zstd level3
        cfg.storage.l1_trigger_files = 0; // 本测针对 L1→L2 分层收敛语义（默认 8 延迟不影响）
        cfg.sstable.compression_level_l2 = 19;
        cfg.blockcache.block_size_kb = 1;
        cfg.memtable.max_size_mb = 256;
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 档位选择逻辑
        assert_eq!(cf.compression_level_for(0), 3, "L0 输出用热档");
        assert_eq!(cf.compression_level_for(1), 3, "L1 输出用热档");
        assert_eq!(cf.compression_level_for(2), 19, "L2 输出用冷档");
        // 未启用分层 → 恒热档
        let mut cfg_flat = Config::default();
        cfg_flat.sstable.compression_level_l2 = 0;
        let dir2 = tmp();
        let cf2 = ColumnFamily::open("primary", &dir2, &cfg_flat).unwrap();
        assert_eq!(cf2.compression_level_for(2), 3, "不分层：L2 也用热档");
        // 端到端：4 轮 L0→L1 → L1×4 下沉 L2（输入 L1 热档、输出 L2 冷档 → meta_only 门控
        // 回退全量重压缩，同时覆盖 compact_merge 冷档写路径）
        for round in 0..4u64 {
            for _ in 0..2u64 {
                for i in 0..50u64 {
                    cf.put(round * 100 + i, format!("v{round}-{i}").into_bytes())
                        .unwrap();
                }
                cf.switch_and_flush().unwrap();
            }
            let r = cf.compact().unwrap();
            assert_eq!(r.out_level, 1, "第 {round} 轮 L0→L1");
        }
        assert_eq!(cf.ssts.load().levels.iter().filter(|l| **l == 1).count(), 4);
        assert!(cf.needs_compact(), "L1 多段应触发 L1→L2");
        let mut guard = 0;
        while cf.needs_compact() && guard < 8 {
            cf.compact().unwrap();
            guard += 1;
        }
        assert!(guard < 8, "冷却后多轮应收敛");
        assert_eq!(cf.sst_count(), 1);
        assert_eq!(cf.ssts.load().levels, vec![2], "收敛为单个 L2 冷档段");
        for round in 0..4u64 {
            for i in (0..50u64).step_by(9) {
                assert!(cf.get(round * 100 + i).unwrap().is_some(), "key 丢失");
            }
        }
        drop(cf);
        let mut cf3 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf3.ssts.load().levels, vec![2]);
        assert!(cf3.get(300 + 10).unwrap().is_some(), "重启后 L2 冷档段可读");
    }

    #[test]
    fn select_compaction_inputs_picks_levels() {
        let no_heat = &[];
        let no_cooling = &std::collections::HashSet::new();
        let no_sizes = &[0u64; 8];
        // 2 个 L0 + L1 未满 → 仅 L0
        assert_eq!(
            select_compaction_inputs_ex(&[0, 0, 1], 8, 0, 0, no_heat, no_cooling, no_sizes, 0),
            (vec![0, 1], 1)
        );
        // 单个 L0 → 暂不压实
        assert_eq!(
            select_compaction_inputs_ex(&[0, 1], 8, 0, 0, no_heat, no_cooling, no_sizes, 0),
            (Vec::new(), 0)
        );
        // L0 ≥ 2 且 L1 已满 → L0 + 全部 L1 收敛
        assert_eq!(
            select_compaction_inputs_ex(&[0, 0, 1, 1, 1, 1], 2, 0, 0, no_heat, no_cooling, no_sizes, 0),
            (vec![0, 1, 2, 3, 4, 5], 1)
        );
        // L0 空、L1 > 1 → L1 → L2
        assert_eq!(
            select_compaction_inputs_ex(&[1, 1], 8, 0, 0, no_heat, no_cooling, no_sizes, 0),
            (vec![0, 1], 2)
        );
        // L0 空、L1 单段 → 无压实
        assert_eq!(
            select_compaction_inputs_ex(&[1], 8, 0, 0, no_heat, no_cooling, no_sizes, 0),
            (Vec::new(), 0)
        );
        // L0/L1 空、L2 > 1 → 收敛 L2
        assert_eq!(
            select_compaction_inputs_ex(&[2, 2], 8, 0, 0, no_heat, no_cooling, no_sizes, 0),
            (vec![0, 1], 2)
        );
        // Ex-8.11：延迟大合并——L0 空、L1=3 < l1_trigger=4 → 暂不下沉
        assert_eq!(
            select_compaction_inputs_ex(&[1, 1, 1], 4, 4, 0, no_heat, no_cooling, no_sizes, 0),
            (Vec::new(), 0)
        );
        // Ex-8.11：L1 攒够 4 → 一次下沉 L2
        assert_eq!(
            select_compaction_inputs_ex(&[1, 1, 1, 1], 4, 4, 0, no_heat, no_cooling, no_sizes, 0),
            (vec![0, 1, 2, 3], 2)
        );
        // Ex-8.11：L2 攒够 l2_trigger=2 → 收敛
        assert_eq!(
            select_compaction_inputs_ex(&[2, 2], 8, 0, 2, no_heat, no_cooling, no_sizes, 0),
            (vec![0, 1], 2)
        );
    }

    #[test]
    fn select_compaction_excludes_cooling_segments() {
        // L 项：冷却段优先不参与合并；候选不足时回退（冷却为软约束，保证收敛）
        let no_heat = &[];
        let no_sizes = &[0u64; 8];
        let mut cooling = std::collections::HashSet::new();
        cooling.insert(0);
        // L0 段 0 冷却 + 段 1 → 冷却后不足 2 → 回退含冷却段（全量合并，防收敛死循环）
        assert_eq!(
            select_compaction_inputs(&[0, 0], 8, no_heat, &cooling, no_sizes, 0),
            (vec![0, 1], 1)
        );
        // L0 段 1 冷却 → 段 0 + 段 2 足够 → 冷却生效，排除段 1
        let mut cooling2 = std::collections::HashSet::new();
        cooling2.insert(1);
        assert_eq!(
            select_compaction_inputs(&[0, 0, 0], 8, no_heat, &cooling2, no_sizes, 0),
            (vec![0, 2], 1)
        );
    }

    #[test]
    fn select_compaction_hot_first_when_l0_over_limit() {
        // Ex-5.9：L0 段数超过 limit 且存在热度 → 优先合并最热的 limit 段（热段先下沉 L1）
        let levels = [0u32, 0, 0, 0, 0]; // 5 个 L0，limit=3
        let heat = [0u64, 0, 100, 50, 10]; // 段 2 最热
        let no_cooling = &std::collections::HashSet::new();
        let (sel, out) = select_compaction_inputs(&levels, 3, &heat, no_cooling, &[0u64; 8], 0);
        assert_eq!(out, 1);
        assert_eq!(sel, vec![2, 3, 4], "应选最热 3 段（100/50/10）");
        // 无热度数据 → 维持全量合并
        let (sel2, _) = select_compaction_inputs(&levels, 3, &[], no_cooling, &[0u64; 8], 0);
        assert_eq!(sel2, vec![0, 1, 2, 3, 4], "无热度全量合并");
        // L0 未超阈值 → 全量（热度不参与）
        let (sel3, _) = select_compaction_inputs(&[0, 0], 3, &heat, no_cooling, &[0u64; 8], 0);
        assert_eq!(sel3, vec![0, 1]);
    }

    #[test]
    fn select_compaction_caps_l0_input_by_size() {
        // 分批合并（compact_input_max_mb）：L0 总大小超上限 → 只合并 ≤ 上限的部分段
        //（防大 L0 一次全合并长时间阻塞写）；L1→L2 不受限（层内不重叠需全选）。
        let no_heat = &[];
        let no_cooling = &std::collections::HashSet::new();
        // L0 三段各 600MB（总 1.8GB），上限 1GB → 合并 2 段（600+600=1.2GB 仍超 → 只留 2 段保底）
        let sizes = [600u64, 600, 600, 100, 100];
        let (sel, out) = select_compaction_inputs(&[0, 0, 0, 1, 1], 8, no_heat, no_cooling, &sizes, 1024);
        assert_eq!(out, 1);
        assert_eq!(sel.len(), 2, "超限应截断到保底 2 段: {sel:?}");
        // L1→L2（L0 空）：大小上限不生效（全选，保证 L2 无重叠）
        let (sel2, out2) = select_compaction_inputs(&[1, 1], 8, no_heat, no_cooling, &sizes, 1024);
        assert_eq!(out2, 2);
        assert_eq!(sel2, vec![0, 1], "L1→L2 全选不受上限限制");
        // 总大小未超限 → 全量合并
        let small = [100u64, 200, 300];
        let (sel3, _) = select_compaction_inputs(&[0, 0, 0], 8, no_heat, no_cooling, &small, 1024);
        assert_eq!(sel3, vec![0, 1, 2], "未超限全量合并");
        // max=0（不限）→ 全量合并（旧行为）
        let (sel4, _) = select_compaction_inputs(&[0, 0, 0], 8, no_heat, no_cooling, &sizes, 0);
        assert_eq!(sel4, vec![0, 1, 2], "max=0 不限");
    }

    #[test]
    fn cap_by_size_truncates_and_keeps_min_two() {
        // cap_by_size 函数级：未超限全保留；超限从尾部移除到 ≤ max；保底 2 段
        // 未超限 → 全保留
        assert_eq!(cap_by_size(vec![0, 1, 2], &[100, 200, 300], 1024), vec![0, 1, 2]);
        // 超限 → 从尾部移除到 ≤ max（保底 2 段：总 800 > 700 但只剩 2 段即停）
        assert_eq!(cap_by_size(vec![0, 1, 2], &[400, 400, 400], 700), vec![0, 1]);
        // 超限且移除一段即满足 → 保留前缀
        assert_eq!(cap_by_size(vec![0, 1, 2], &[300, 300, 500], 600), vec![0, 1]);
        // 仅 2 段 → 直接返回（保底下限）
        assert_eq!(cap_by_size(vec![0, 1], &[2000, 2000], 100), vec![0, 1]);
        // 单段 → 直接返回
        assert_eq!(cap_by_size(vec![0], &[5000], 100), vec![0]);
        // 剩余段由后续轮次再选（前缀保留 = 本轮输入；未选段仍在 levels 中可再被选）
        let no_heat = &[];
        let no_cooling = &std::collections::HashSet::new();
        let sizes = [400u64, 400, 400];
        let (sel, _) = select_compaction_inputs(&[0, 0, 0], 8, no_heat, no_cooling, &sizes, 700);
        assert_eq!(sel, vec![0, 1], "本轮分批 2 段");
        // 下一轮（同输入，无冷却）：未选段 2 参与（冷却回退/再次全选后 cap）→ 仍能推进收敛
        let (sel2, _) = select_compaction_inputs(&[0, 0, 0], 8, no_heat, no_cooling, &sizes, 700);
        assert_eq!(sel2.len(), 2, "下轮仍可选 2 段（含段 2）");
    }

    #[test]
    fn compact_batches_l0_with_small_input_cap() {
        // 端到端：compact_input_max_mb=1MB，flush 5 段（各 ~4MB）→ 单次 compact 输入被 cap
        //（保底 2 段 < 全部 5 段）→ 分批合并；多轮收敛后数据完整。
        let dir = tmp();
        let mut cfg = small_cfg(64);
        cfg.storage.compact_input_max_mb = 1; // 1MB 上限
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        let big = vec![b'x'; 2048]; // 2KB/行 → 每段 2000 行 ≈ 4MB > 1MB 上限
        for seg in 0..5u64 {
            for i in seg * 2000..seg * 2000 + 2000 {
                cf.put(i, big.clone()).unwrap();
            }
            cf.switch_and_flush().unwrap();
        }
        let before = cf.sst_count();
        assert!(before >= 5, "应产生 5+ SST: {before}");
        let rep = cf.compact().unwrap();
        assert!(
            rep.merged_ssts >= 2 && rep.merged_ssts < before,
            "单轮分批：输入被 cap（保底 2 段 < 全部 {before}），实际合并 {} 段",
            rep.merged_ssts
        );
        assert!(cf.sst_count() < before, "本轮合并后段数减少");
        // 分批合并不丢数据（全部可读）
        for i in 0..10_000u64 {
            let v = cf.get(i).unwrap();
            assert_eq!(v.as_ref().map(|r| r.0.as_slice()), Some(big.as_slice()), "docid={i} 数据完整");
        }
        // 多轮收敛（worker while 语义）：最终 needs_compact=false 且数据仍完整
        let mut rounds = 0;
        while cf.needs_compact() && rounds < 30 {
            cf.compact().unwrap();
            rounds += 1;
        }
        assert!(!cf.needs_compact(), "多轮 {rounds} 轮内收敛");
        for i in (0..10_000u64).step_by(997) {
            let v = cf.get(i).unwrap();
            assert_eq!(v.as_ref().map(|r| r.0.as_slice()), Some(big.as_slice()), "收敛后 docid={i}");
        }
    }

    #[test]
    fn sst_heat_tracks_point_reads() {
        // Ex-5.9：点查命中递增 SST 热度；跨 flush/compact 保持
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in 0..50u64 {
            cf.put(i, format!("v{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.sst_count(), 1);
        assert_eq!(cf.sst_heat(0), 0, "初始零热度");
        // 多次点查（含未命中——未命中不计数）
        for _ in 0..10 {
            cf.get(5).unwrap();
        }
        cf.get(1000).unwrap(); // 未命中（键序在范围外，布隆拦截不计数）
        let h = cf.sst_heat(0);
        assert_eq!(h, 10, "10 次命中计数，未命中不计数");
        // 热度读取不重置（累积）
        cf.get(6).unwrap();
        assert_eq!(cf.sst_heat(0), 11);
    }

    #[test]
    fn flush_then_read_back() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 写入一批触发显式刷盘，验证落盘后可读
        for i in 0..100u64 {
            cf.put(i, format!("value-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert!(cf.sst_count() >= 1);
        // 落盘后仍可读（读路径先内存后磁盘）
        assert_eq!(cf.get(0).unwrap().unwrap().0, b"value-0");
        assert_eq!(cf.get(99).unwrap().unwrap().0, b"value-99");
    }

    #[test]
    fn restart_recovers_data_from_wal_and_sst() {
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..150u64 {
                cf.put(i, format!("v-{i}").into_bytes()).unwrap();
            }
            // 前 100 条刷盘，后 50 条留在 WAL/MemTable
            for _ in 0..2 {
                cf.switch_and_flush().unwrap();
            }
        } // 模拟进程退出（drop 不执行额外清理）

        // 重启：manifest 加载 SST + WAL 回放
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in 0..150u64 {
            assert_eq!(
                cf2.get(i).unwrap().unwrap().0,
                format!("v-{i}").into_bytes(),
                "key {i} 丢失"
            );
        }
    }

    #[test]
    fn get_many_matches_individual_get_across_flush_and_tombstone() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in 0..60u64 {
            cf.put(i, format!("v-{i}").into_bytes()).unwrap();
        }
        cf.delete(7).unwrap();
        // 刷盘：数据落 SST（多 key 共享数据块 → 按块分组批量命中路径）
        cf.switch_and_flush().unwrap();
        // 混入 MemTable 层新写 + 新删除（tombstone 终结路径）
        cf.put(100, b"v-100".to_vec()).unwrap();
        cf.delete(100).unwrap();
        let ids = [0u64, 7, 30, 59, 60, 100, 101];
        let got = cf.get_many(&ids).unwrap();
        assert_eq!(got.len(), ids.len());
        for (i, &d) in ids.iter().enumerate() {
            let expect = cf.get(d).unwrap();
            assert_eq!(
                got[i].as_ref().map(|(v, _)| v.as_slice()),
                expect.as_ref().map(|(v, _)| v.as_slice()),
                "docid {d} get_many 与 get 结果不一致"
            );
        }
        // 删除语义：7（SST tombstone）与 100（MemTable tombstone）均为 None
        assert!(got[1].is_none(), "SST tombstone 应视为不存在");
        assert!(got[5].is_none(), "MemTable tombstone 应视为不存在");
        assert!(got[6].is_none(), "不存在的 key 应为 None");
        assert!(got[2].is_some() && got[3].is_some(), "未删除 key 应命中");
        // 空输入
        assert!(cf.get_many(&[]).unwrap().is_empty());
    }

    #[test]
    fn layered_range_skip_preserves_reads_across_levels() {
        // R 项：层/段两级 Zone Map 粗筛——多段多层（L0/L1 混合）下点查/get_at/get_many
        // 跨层命中正确、越界 key 不假阴性（层范围 = 段范围精确并集）。
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 3 个 L0 段：段 i 覆盖 [i*100, i*100+100)
        for seg in 0..3u64 {
            for i in seg * 100..seg * 100 + 100 {
                cf.put(i, format!("v{seg}-{i}").into_bytes()).unwrap();
            }
            cf.switch_and_flush().unwrap();
        }
        // 3 段 compact → L1（覆盖 [0,299]）；再 flush 第 4 段 → L0（覆盖 [300,399]）混合
        let rep = cf.compact().unwrap();
        assert_eq!(rep.out_level, 1);
        for i in 300..400u64 {
            cf.put(i, format!("v3-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        // 快照层元数据：L0 1 段（[300,399]）、L1 1 段（[0,299]）
        let snap = cf.ssts.load();
        assert_eq!(snap.layer_indices[0].len(), 1, "L0 为第 4 段");
        assert_eq!(snap.layer_indices[1].len(), 1, "L1 为合并输出");
        assert_eq!(
            snap.layer_ranges[1],
            Some((
                crate::keys::encode_docid(0).to_vec(),
                crate::keys::encode_docid(299).to_vec()
            )),
            "L1 范围应覆盖 [0,299]"
        );
        assert_eq!(
            snap.layer_ranges[0],
            Some((
                crate::keys::encode_docid(300).to_vec(),
                crate::keys::encode_docid(399).to_vec()
            )),
            "L0 范围应覆盖 [300,399]"
        );
        drop(snap);
        // 跨层/边界点查（get / get_at / get_many 一致）
        for i in [0u64, 50, 99, 100, 199, 299, 300, 350, 399] {
            let expect = format!("v{}-{i}", i / 100).into_bytes();
            assert_eq!(cf.get(i).unwrap().unwrap().0, expect, "get key {i}");
            let at = cf
                .get_bytes_at(&crate::keys::encode_docid(i), u64::MAX)
                .unwrap()
                .unwrap()
                .0;
            assert_eq!(at, expect, "get_at key {i}");
        }
        let got = cf.get_many(&[0, 100, 299, 300, 399]).unwrap();
        assert_eq!(got[0].as_ref().unwrap().0, b"v0-0".to_vec());
        assert_eq!(got[1].as_ref().unwrap().0, b"v1-100".to_vec());
        assert_eq!(got[2].as_ref().unwrap().0, b"v2-299".to_vec());
        assert_eq!(got[3].as_ref().unwrap().0, b"v3-300".to_vec());
        assert_eq!(got[4].as_ref().unwrap().0, b"v3-399".to_vec());
        // 越界 key（层范围外）→ None，不假阴性
        for miss in [400u64, 999, 10000] {
            assert!(cf.get(miss).unwrap().is_none(), "get miss {miss}");
            assert!(
                cf.get_bytes_at(&crate::keys::encode_docid(miss), u64::MAX)
                    .unwrap()
                    .is_none(),
                "get_at miss {miss}"
            );
        }
    }

    #[test]
    fn compaction_urgency_grows_with_l0_pressure() {
        // W 项：紧迫度 = L0 段数 ×10 + 大小超限 +8——多段 flush 后递增（跨列族调度主因子）
        let dir = tmp();
        let cfg = small_cfg(64);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf.compaction_urgency(), 0, "空库无压力");
        for seg in 0..3u64 {
            for i in seg * 50..seg * 50 + 50 {
                cf.put(i, format!("v-{i}").into_bytes()).unwrap();
            }
            cf.switch_and_flush().unwrap();
            let u = cf.compaction_urgency();
            let expect = (seg + 1) as u32 * 10;
            assert_eq!(u, expect, "L0 段数 {} → urgency {u}", seg + 1);
        }
        // 大小软阈值：l0_max_size_bytes 配小 → 超限追加 +8
        let dir2 = tmp();
        let mut cfg2 = small_cfg(64);
        cfg2.storage.l0_max_size_mb = 1; // 1MB 大小软阈值
        let mut cf2 = ColumnFamily::open("primary", &dir2, &cfg2).unwrap();
        // 单段 ~3.2MB（50×64KB）→ 超 1MB 软阈值
        for i in 0..50u64 {
            cf2.put(i, vec![0x55u8; 64 * 1024]).unwrap();
        }
        cf2.switch_and_flush().unwrap();
        assert!(
            cf2.compaction_urgency() >= 18,
            "大小超限应追加 +8（l0=1 → 10+8，实际 {}）",
            cf2.compaction_urgency()
        );
    }

    #[test]
    fn needs_compact_by_l0_size_threshold() {
        let dir = tmp();
        let mut cfg = small_cfg(256);
        cfg.storage.l0_max_size_mb = 1; // 1MB 大小软阈值（叠加在段数阈值之上）
        cfg.memtable.max_size_mb = 1; // 1MB MemTable → 写入过程自动多次 flush
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        let val = vec![0x42u8; 64 * 1024];
        for i in 0..64u64 {
            cf.put(i, val.clone()).unwrap(); // 64KB×64 ≈ 4MB → ~4 次 flush
        }
        cf.switch_and_flush().unwrap(); // 清空残余 MemTable
        assert!(
            cf.l0_count() >= 2,
            "应产生多个 L0 段（实际 {}）",
            cf.l0_count()
        );
        assert!(
            cf.needs_compact(),
            "L0 总大小超阈值（{} B > 1MB）应触发合并",
            cf.l0_bytes()
        );
        // 多段合并收敛后大小阈值不再触发
        cf.compact().unwrap();
        assert!(!cf.needs_compact());
        // 单段超大小阈值不触发合并（单段为已排序文件，合并是纯无收益重写）
        let dir2 = tmp();
        let mut cfg2 = small_cfg(256);
        cfg2.storage.l0_max_size_mb = 1;
        cfg2.memtable.max_size_mb = 8; // 大 MemTable：2MB 值不触发自动 flush
        let mut cf2 = ColumnFamily::open("primary", &dir2, &cfg2).unwrap();
        cf2.put(0, vec![0x42u8; 2 * 1024 * 1024]).unwrap(); // 单条 2MB > 1MB
        cf2.switch_and_flush().unwrap();
        assert_eq!(cf2.l0_count(), 1);
        assert!(!cf2.needs_compact(), "单段超大小阈值不应触发合并");
    }

    #[test]
    fn l0_bytes_and_sst_bytes_match_disk_sizes() {
        // 修复（96ac6bc）：l0_bytes/sst_bytes 读快照 sizes 缓存（零 fs::metadata），
        // 验证缓存值与磁盘实际文件大小一致——open/flush/compact 三个构建点均正确。
        let dir = tmp();
        let cfg = small_cfg(64);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // flush 3 段（L0）
        for seg in 0..3u64 {
            for i in seg * 50..seg * 50 + 50 {
                cf.put(i, format!("v-{i}").into_bytes()).unwrap();
            }
            cf.switch_and_flush().unwrap();
        }
        let disk_l0: u64 = {
            let snap = cf.ssts.load();
            snap.ssts
                .iter()
                .enumerate()
                .filter(|(i, _)| snap.levels[*i] == 0)
                .map(|(_, s)| std::fs::metadata(s.path()).unwrap().len())
                .sum()
        };
        let disk_all: u64 = {
            let snap = cf.ssts.load();
            snap.ssts
                .iter()
                .map(|s| std::fs::metadata(s.path()).unwrap().len())
                .sum()
        };
        assert_eq!(cf.l0_bytes(), disk_l0, "L0 字节 = 磁盘实际和（flush 构建点 sizes 缓存）");
        assert_eq!(cf.sst_bytes(), disk_all, "全部字节 = 磁盘实际和");
        // sizes 与每段 file_len 一致（修复双缓存同源）
        let snap = cf.ssts.load();
        for (i, s) in snap.ssts.iter().enumerate() {
            assert_eq!(snap.sizes[i], s.file_len(), "sizes[{i}] = file_len");
        }
        // compact 发布点：新快照 sizes 随合并更新，仍与磁盘一致
        cf.compact().unwrap();
        let disk_all2: u64 = {
            let snap = cf.ssts.load();
            snap.ssts
                .iter()
                .map(|s| std::fs::metadata(s.path()).unwrap().len())
                .sum()
        };
        assert_eq!(cf.sst_bytes(), disk_all2, "compact 后 sizes 仍与磁盘一致");
    }

    #[test]
    fn sizes_cache_survives_reopen() {
        // 修复：load（重开）构建点 sizes 正确——重开后 l0_bytes/sst_bytes 仍与磁盘一致
        let dir = tmp();
        let cfg = small_cfg(64);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..50u64 {
                cf.put(i, format!("v-{i}").into_bytes()).unwrap();
            }
            cf.switch_and_flush().unwrap();
        } // drop = 关闭
        let cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        let disk_all: u64 = {
            let snap = cf2.ssts.load();
            snap.ssts
                .iter()
                .map(|s| std::fs::metadata(s.path()).unwrap().len())
                .sum()
        };
        assert_eq!(cf2.sst_bytes(), disk_all, "重开后 sizes 缓存仍正确");
        assert_eq!(cf2.l0_bytes(), disk_all, "单层全 L0 → l0_bytes = 全部字节");
    }

    // ---------- S 项：MemTable 多版本（严格 MVCC 快照读） ----------

    #[test]
    fn snapshot_read_sees_old_version_while_both_in_memtable() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 同一 key 连续写入两次（均未刷盘，旧实现仅保留最新 → 快照读漏掉旧版本）
        let s1 = cf.put(1, b"v1".to_vec()).unwrap();
        let s2 = cf.put(1, b"v2".to_vec()).unwrap();
        assert!(s2 > s1);
        // 快照落在 s1..s2 → 应读到 v1（S 项修复点）
        assert_eq!(cf.get_bytes_at(&encode_docid(1), s1).unwrap().unwrap().0, b"v1");
        assert_eq!(cf.get_bytes_at(&encode_docid(1), s2).unwrap().unwrap().0, b"v2");
        assert!(cf.get_bytes_at(&encode_docid(1), s1 - 1).unwrap().is_none());
        // 非快照读最新
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"v2");
        // 刷盘后快照仍可读旧版本（SST 多版本落盘）
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.get_bytes_at(&encode_docid(1), s1).unwrap().unwrap().0, b"v1");
        assert_eq!(cf.get_bytes_at(&encode_docid(1), s2).unwrap().unwrap().0, b"v2");
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"v2");
    }

    #[test]
    fn snapshot_read_sees_deleted_as_tombstone_after_memtable_delete() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        let s1 = cf.put(1, b"v1".to_vec()).unwrap();
        let sd = cf.delete(1).unwrap();
        // 快照在删除前 → 可见 v1；删除点后 → None
        assert_eq!(cf.get_bytes_at(&encode_docid(1), s1).unwrap().unwrap().0, b"v1");
        assert!(cf.get_bytes_at(&encode_docid(1), sd).unwrap().is_none());
        assert!(cf.get(1).unwrap().is_none(), "非快照读：当前已删除");
        // 刷盘后语义保持
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.get_bytes_at(&encode_docid(1), s1).unwrap().unwrap().0, b"v1");
        assert!(cf.get_bytes_at(&encode_docid(1), sd).unwrap().is_none());
    }

    #[test]
    fn reopen_preserves_wal_only_data() {
        // 步骤 15 暴露的预存 bug：reopen 不得截断 WAL，未刷盘数据必须经回放恢复
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 1..=5u64 {
                cf.put(i, format!("v-{i}").into_bytes()).unwrap();
            }
            // 不刷盘：数据仅存在于 WAL + MemTable
        }
        {
            let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 1..=5u64 {
                assert_eq!(
                    cf2.get(i).unwrap().unwrap().0,
                    format!("v-{i}").into_bytes(),
                    "key {i} 丢失（WAL 被截断？）"
                );
            }
            // 新写入 seq 接续，同 key 覆盖正确
            cf2.put(3, b"v-3-updated".to_vec()).unwrap();
            assert_eq!(cf2.get(3).unwrap().unwrap().0, b"v-3-updated");
        }
        // 再次重开：追加写入与覆盖均正确
        {
            let mut cf3 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            assert_eq!(cf3.get(3).unwrap().unwrap().0, b"v-3-updated");
            assert_eq!(cf3.get(5).unwrap().unwrap().0, b"v-5");
            assert_eq!(cf3.get(1).unwrap().unwrap().0, b"v-1");
        }
    }

    // ---------- 环形 WAL 集成（design 4.3，M6-1） ----------

    fn ring_cfg(mem_mb: usize) -> Config {
        let mut cfg = small_cfg(mem_mb);
        cfg.storage.wal_mode = "ring".into();
        cfg.storage.wal_ring_size_mb = 1;
        cfg
    }

    #[test]
    fn ring_wal_mode_persists_and_recovers() {
        let dir = tmp();
        let cfg = ring_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            cf.put(1, b"v1".to_vec()).unwrap();
            cf.put(2, b"v2".to_vec()).unwrap();
            assert_eq!(cf.get(1).unwrap().unwrap().0, b"v1");
        }
        // 重启：环形 WAL 回放恢复未刷盘数据
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf.get(1).unwrap().unwrap().0, b"v1");
        assert_eq!(cf.get(2).unwrap().unwrap().0, b"v2");
        // 覆盖 + 追加后再次重启
        cf.put(2, b"v2-updated".to_vec()).unwrap();
        drop(cf);
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        assert_eq!(cf2.get(2).unwrap().unwrap().0, b"v2-updated");
    }

    #[test]
    fn ring_wal_full_forces_flush_keeps_data() {
        // 写入量超过 1MB 环形容量 → 强制 Flush 腾空，数据不丢
        let dir = tmp();
        let cfg = ring_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        const N: u64 = 60_000;
        for chunk in 0..6u64 {
            for i in chunk * 10_000..(chunk + 1) * 10_000 {
                cf.put_bytes_nosync(i.to_le_bytes().to_vec(), format!("value-{i}").into_bytes())
                    .unwrap();
            }
            cf.sync_wal().unwrap();
        }
        // 抽查全量数据（跨 MemTable / SST / 环形覆盖后均完整）
        for i in (0..N).step_by(997) {
            assert_eq!(
                cf.get_bytes(&i.to_le_bytes()).unwrap().unwrap().0,
                format!("value-{i}").into_bytes(),
                "key {i} 丢失（环形覆盖未刷盘记录？）"
            );
        }
        // 重启后仍完整（环形恢复 + SST 合并读）
        drop(cf);
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in (0..N).step_by(5003) {
            assert_eq!(
                cf2.get_bytes(&i.to_le_bytes()).unwrap().unwrap().0,
                format!("value-{i}").into_bytes(),
                "重启后 key {i} 丢失"
            );
        }
    }

    #[test]
    fn ttl_helpers() {
        assert_eq!(epoch_days(0), 0);
        assert_eq!(epoch_days(86_400), 1);
        assert_eq!(epoch_days(-1), -1); // div_euclid 向负无穷取整
        assert_eq!(parse_sst_date("sst-00020697-00000001.sst"), Some(20_697));
        assert_eq!(parse_sst_date("sst-00000001.sst"), None);
        assert_eq!(parse_sst_date("manifest.json"), None);
    }

    #[test]
    fn ttl_buckets_and_expiry() {
        // 阶段 1.5 TTL：按天分桶写 SST；重启时过期桶整文件删除，默认桶永不过期
        let dir = tmp();
        let mut cfg = small_cfg(256);
        cfg.storage.ttl_days = Some(2);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            let today = format!(r#"{{"v":"t","timestamp":{now}}}"#).into_bytes();
            let yesterday = format!(r#"{{"v":"y","timestamp":{}}}"#, now - 86_400).into_bytes();
            let old = format!(r#"{{"v":"o","timestamp":{}}}"#, now - 86_400 * 10).into_bytes();
            cf.put(1, today).unwrap();
            cf.put(2, yesterday).unwrap();
            cf.put(3, old).unwrap();
            cf.put(4, b"no-timestamp-raw".to_vec()).unwrap();
            cf.switch_and_flush().unwrap();
            assert!(cf.sst_count() >= 4, "应分 4 个桶，实际 {}", cf.sst_count());
            // 未过期前全部可读
            assert!(cf.get(1).unwrap().is_some());
            assert!(cf.get(3).unwrap().is_some());
        }
        // 重启：10 天前的桶过期删除
        {
            let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            assert!(cf2.get(1).unwrap().is_some(), "今天桶应保留");
            assert!(cf2.get(2).unwrap().is_some(), "昨天桶应保留");
            assert!(cf2.get(3).unwrap().is_none(), "10 天前的桶应过期删除");
            assert!(
                cf2.get(4).unwrap().is_some(),
                "默认桶（无时间字段）永不过期"
            );
            // 过期文件已物理删除
            let names: Vec<String> = std::fs::read_dir(&dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().to_string())
                .filter(|f| f.starts_with(SST_PREFIX))
                .collect();
            assert!(
                names
                    .iter()
                    .any(|f| parse_sst_date(f).is_none_or(|d| d >= today_epoch_days() - 2)),
                "过期 SST 应已删除，剩余: {names:?}"
            );
        }
    }

    #[test]
    fn pax_cf_flush_read_roundtrip() {
        // 阶段 1.5 PAX：配置 hot_fields 后 flush 落盘列式块，读回语义等值；非 JSON 值回退行式
        let dir = tmp();
        let mut cfg = small_cfg(256);
        cfg.storage.hot_fields = vec!["status".to_string(), "city".to_string()];
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.put(
            1,
            br#"{"status":"active","city":"beijing","amount":10}"#.to_vec(),
        )
        .unwrap();
        cf.put(2, br#"{"status":"inactive","city":"shanghai"}"#.to_vec())
            .unwrap();
        cf.switch_and_flush().unwrap();
        assert!(cf.sst_count() >= 1);
        assert_eq!(
            String::from_utf8_lossy(&cf.get(1).unwrap().unwrap().0),
            r#"{"status":"active","city":"beijing","amount":10}"#
        );
        assert_eq!(
            String::from_utf8_lossy(&cf.get(2).unwrap().unwrap().0),
            r#"{"status":"inactive","city":"shanghai"}"#
        );
        // 非 JSON 值 → 行式块（同一 v4 文件体系，Reader 兼容）
        cf.put(3, b"raw-bytes".to_vec()).unwrap();
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.get(3).unwrap().unwrap().0, b"raw-bytes");
    }

    #[test]
    fn manifest_persists_sst_list() {
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..50u64 {
                cf.put(i, b"v".to_vec()).unwrap();
            }
            cf.switch_and_flush().unwrap();
        }
        let text = std::fs::read_to_string(dir.join(MANIFEST_FILE)).unwrap();
        assert!(text.contains(SST_PREFIX));
        let m: Manifest = serde_json::from_str(&text).unwrap();
        assert!(!m.sst_files.is_empty());
    }

    #[test]
    fn corrupted_sst_rejected_on_open() {
        // 损坏注入：SST 头部魔数被破坏 → 启动必须报 Corrupted 而非 panic（development 9.3）
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 0..100u64 {
                cf.put(i, b"v".to_vec()).unwrap();
            }
            cf.switch_and_flush().unwrap();
        }
        let sst_path = dir.join(format!("{SST_PREFIX}00000001.sst"));
        assert!(sst_path.exists(), "SST 文件应已生成");
        let mut data = std::fs::read(&sst_path).unwrap();
        data[0..8].copy_from_slice(&[0xFF; 8]); // 破坏魔数
        std::fs::write(&sst_path, &data).unwrap();

        let err = match ColumnFamily::open("primary", &dir, &cfg) {
            Ok(_) => panic!("损坏 SST 应打开失败"),
            Err(e) => e,
        };
        assert!(
            matches!(err, Error::Corrupted(_)),
            "损坏 SST 应报 Corrupted，实际 {err:?}"
        );
    }

    #[test]
    fn wal_partial_tail_recovers_cleanly() {
        // 崩溃恢复：WAL 尾部半条记录（模拟断电时只写入一半）→ 重启只恢复完整记录、不 panic
        let dir = tmp();
        let cfg = small_cfg(256);
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            for i in 1..=10u64 {
                cf.put(i, format!("v-{i}").into_bytes()).unwrap();
            }
        }
        // 截断 WAL 至一半字节（人为制造半条尾部记录）
        let wal_path = dir.join(WAL_FILE);
        let mut data = std::fs::read(&wal_path).unwrap();
        assert!(data.len() > 40);
        data.truncate(data.len() / 2);
        std::fs::write(&wal_path, &data).unwrap();

        // 回放：完整记录应恢复，半条记录被丢弃，不 panic
        let recs = WalReader::recover(&wal_path).unwrap();
        assert!(!recs.is_empty(), "至少应恢复一条完整记录");
        assert!(recs.len() <= 10);

        // reopen 全链路可用
        let mut cf2 = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for r in &recs {
            assert!(cf2.get_bytes(&r.key).unwrap().is_some(), "恢复的记录应可读");
        }
    }
    #[test]
    fn scan_range_covers_memtable_and_sst() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        // 部分写入并刷盘，部分留在 MemTable
        for i in 0..30u64 {
            cf.put(i, format!("v-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        for i in 30..40u64 {
            cf.put(i, format!("v-{i}").into_bytes()).unwrap();
        }
        // 更新一个已落盘 key，验证去重取最新
        cf.put(5, b"v-updated".to_vec()).unwrap();

        let rows = cf.scan_range(Some(0), Some(39)).unwrap();
        assert_eq!(rows.len(), 40);
        assert_eq!(rows[0], (0, b"v-0".to_vec()));
        assert_eq!(rows[39], (39, b"v-39".to_vec()));
        assert_eq!(rows[5], (5, b"v-updated".to_vec()));

        // 无边界扫描
        let all = cf.scan_range(None, None).unwrap();
        assert_eq!(all.len(), 40);
    }

    #[test]
    fn scan_range_empty_window() {
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        for i in 0..10u64 {
            cf.put(i, b"v".to_vec()).unwrap();
        }
        // 无交集区间 → 空
        assert!(cf.scan_range(Some(100), Some(200)).unwrap().is_empty());
    }

    #[test]
    fn composite_index_prefix_query() {
        // 步骤 10：组合索引 = ColumnFamily + encode_composite_key(fields, docid)
        use crate::keys::{decode_composite_key, encode_composite_key};
        let dir = tmp();
        let cfg = small_cfg(256);
        let mut cf = ColumnFamily::open("cidx", &dir, &cfg).unwrap();
        // 写入 (status=active, type=click, docid)
        for docid in [1u64, 5, 9, 20] {
            let key = encode_composite_key(&[b"active", b"click"], docid);
            cf.put_bytes(key, docid.to_le_bytes().to_vec()).unwrap();
        }
        // 干扰项：status=active 但 type=view
        let key = encode_composite_key(&[b"active", b"view"], 100);
        cf.put_bytes(key, 100u64.to_le_bytes().to_vec()).unwrap();

        // 前缀查询 active/click：组合键有序，范围 [active/click/0, active/click/FFFF]
        let start = encode_composite_key(&[b"active", b"click"], 0);
        let end = encode_composite_key(&[b"active", b"click"], u64::MAX);
        let mut hits = Vec::new();
        let rows = cf.scan_raw_range(Some(&start), Some(&end)).unwrap();
        for (k, _v) in rows {
            let (fields, docid) = decode_composite_key(&k).unwrap();
            assert_eq!(fields, vec![b"active".to_vec(), b"click".to_vec()]);
            hits.push(docid);
        }
        hits.sort();
        assert_eq!(hits, vec![1, 5, 9, 20]);
    }

    // ---------- M3（§26 多表）：Flush / Compaction 按表切分（同表合并） ----------

    /// 表内 docid：docid = table<<48 | row
    fn tdoc(table: u16, row: u64) -> u64 {
        ((table as u64) << 48) | row
    }
    /// 单表 row 容量 2^48：表内最大 row_id（窗口上界用，勿用 u64::MAX——OR 全 1 会越出本表区间）
    const TROW_MAX: u64 = (1u64 << 48) - 1;

    /// 快照内各 SST 归属表 id（单表段 → Some(tid)；混表/空段 → None），保持快照顺序。
    fn sst_tables(cf: &ColumnFamily) -> Vec<Option<u16>> {
        let snap = cf.ssts.load();
        snap.ssts
            .iter()
            .map(|s| match s.key_range() {
                Some((mn, mx)) => match (key_table_id(mn), key_table_id(mx)) {
                    (Some(a), Some(b)) if a == b => Some(a),
                    _ => None,
                },
                None => None,
            })
            .collect()
    }

    #[test]
    fn m3_flush_splits_imm_by_table() {
        // 实施清单①：单次 flush 内多表 docid 同落 Immutable → 按表边界切多个 SST
        let dir = tmp();
        let cfg = small_cfg(64);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.enable_table_split();
        // 默认表 0 / 表 1 / 表 7 各 3 行（row 同号共存）
        for row in [1u64, 2, 3] {
            cf.put(tdoc(0, row), format!("t0-r{row}").into_bytes()).unwrap();
            cf.put(tdoc(1, row), format!("t1-r{row}").into_bytes()).unwrap();
            cf.put(tdoc(7, row), format!("t7-r{row}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.sst_count(), 3, "3 张表应切 3 个 SST，实际 {}", cf.sst_count());
        // 每个 SST 均为单表段，且三表齐全
        let mut ts: Vec<u16> = sst_tables(&cf).into_iter().map(|t| t.unwrap()).collect();
        ts.sort();
        assert_eq!(ts, vec![0, 1, 7], "切分后每文件单表，实际 {ts:?}");
        // 全部行读回
        for row in [1u64, 2, 3] {
            for t in [0u16, 1, 7] {
                let got = cf.get(tdoc(t, row)).unwrap().expect("行应可读");
                assert_eq!(got.0, format!("t{t}-r{row}").into_bytes());
            }
        }
    }

    #[test]
    fn m3_compact_merges_per_table_and_converges() {
        // 实施清单② + 同表合并：flush 按表切分后，压缩只合并同表段；
        // 跨表每表 1 段即"按表收敛"——不反复重写（needs_compact 归零、重复 compact 无空转）
        let dir = tmp();
        let cfg = small_cfg(64);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.enable_table_split();
        // 表 1：两批 flush（L0 同表 2 段 → 需合并去重）
        for row in 1..=3u64 {
            cf.put(tdoc(1, row), format!("t1-a{row}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        for row in 2..=4u64 {
            cf.put(tdoc(1, row), format!("t1-b{row}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        // 表 2：一批 flush（L0 单段）
        for row in 1..=4u64 {
            cf.put(tdoc(2, row), format!("t2-r{row}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        // L0 = 表1×2 + 表2×1
        assert_eq!(cf.sst_count(), 3);
        // 压缩至收敛（直接驱动：默认 L0 阈值较高，逐轮 compact 直至 no-op）
        let mut guard = 0;
        loop {
            let rep = cf.compact().unwrap();
            if rep.merged_ssts == 0 {
                break;
            }
            guard += 1;
            assert!(guard < 8, "压缩应快速收敛（当前 {} 轮）", guard);
        }
        // 收敛：跨表每表 1 段（表1 去重为 1 段、表2 1 段），不再需要压缩
        eprintln!("DEBUG: sst_count={}, needs_compact={}, levels={:?}, l1_tf={}, effective_l0={}, l0_max_size={}",
            cf.sst_count(),
            cf.needs_compact(),
            { let snap = cf.ssts.load(); snap.levels.clone() },
            cf.l1_trigger_files.load(std::sync::atomic::Ordering::Relaxed),
            cf.effective_l0_threshold(),
            cf.l0_max_size_bytes,
        );
        assert!(!cf.needs_compact(), "按表收敛后不应再触发压缩");
        let mut ts: Vec<u16> = sst_tables(&cf).into_iter().map(|t| t.unwrap()).collect();
        ts.sort();
        assert_eq!(ts, vec![1, 2], "收敛后每表 1 段，实际 {ts:?}");
        assert_eq!(cf.sst_count(), 2, "收敛后文件数 = 表数");
        // 重复 compact 不应空转重写（无新工作 → merged=0）
        let rep = cf.compact().unwrap();
        assert_eq!(rep.merged_ssts, 0, "收敛态重复 compact 应为 no-op");
        // 语义：表 1 后写覆盖先写、行 1..4 各 1 条；表 2 独立不受影响
        let t1 = cf.scan_range(Some(tdoc(1, 0)), Some(tdoc(1, TROW_MAX))).unwrap();
        assert_eq!(t1.len(), 4);
        let b4 = cf.get(tdoc(1, 4)).unwrap().unwrap();
        assert_eq!(b4.0, b"t1-b4");
        let a1 = cf.get(tdoc(1, 1)).unwrap().unwrap();
        assert_eq!(a1.0, b"t1-a1"); // 未覆盖的行保持 a 批次
        let b2 = cf.get(tdoc(1, 2)).unwrap().unwrap();
        assert_eq!(b2.0, b"t1-b2"); // 覆盖生效
        let t2 = cf.scan_range(Some(tdoc(2, 0)), Some(tdoc(2, TROW_MAX))).unwrap();
        assert_eq!(t2.len(), 4);
    }

    #[test]
    fn m3_single_table_regression_stays_single_file() {
        // 实施清单⑤：table_id=0（含旧库全量）flush/compact 均单文件输出，行为与旧一致
        let dir = tmp();
        let cfg = small_cfg(64);
        let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
        cf.enable_table_split();
        for i in 1..=500u64 {
            cf.put(i, format!("doc-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        assert_eq!(cf.sst_count(), 1, "单表 flush 应 1 个 SST，实际 {}", cf.sst_count());
        // 多批 flush → L0 多段 → 压缩收敛单段
        for i in 501..=1000u64 {
            cf.put(i, format!("doc-{i}").into_bytes()).unwrap();
        }
        cf.switch_and_flush().unwrap();
        let mut guard = 0;
        loop {
            let rep = cf.compact().unwrap();
            if rep.merged_ssts == 0 {
                break;
            }
            guard += 1;
            assert!(guard < 8, "单表压缩应快速收敛（{} 轮）", guard);
        }
        assert_eq!(cf.sst_count(), 1, "单表压缩后应收敛为 1 段，实际 {}", cf.sst_count());
        assert_eq!(cf.scan_range(None, None).unwrap().len(), 1000);
    }

    #[test]
    fn m3_drop_table_range_files_physically_removes_table_ssts() {
        // 实施清单④：表切分后 DROP TABLE 物理删该表区间专属文件，manifest 同步、他表不受影响
        let dir = tmp();
        let cfg = small_cfg(64);
        let sst_dir = dir.clone();
        {
            let mut cf = ColumnFamily::open("primary", &dir, &cfg).unwrap();
            cf.enable_table_split();
            for row in 1..=3u64 {
                cf.put(tdoc(1, row), format!("t1-r{row}").into_bytes()).unwrap();
            }
            cf.switch_and_flush().unwrap();
            for row in 1..=3u64 {
                cf.put(tdoc(2, row), format!("t2-r{row}").into_bytes()).unwrap();
            }
            cf.switch_and_flush().unwrap();
            assert_eq!(cf.sst_count(), 2);
            // 模拟"先逻辑删再物理回收"（引擎 drop_table_range 先逐 docid 墓碑）
            for row in 1..=3u64 {
                cf.delete(tdoc(1, row)).unwrap();
            }
            cf.sync_wal().unwrap();
            let n = cf.drop_table_range_files(1).unwrap();
            assert_eq!(n, 1, "表 1 专属 SST 应被物理删除 1 个");
            assert_eq!(cf.sst_count(), 1, "表 2 SST 保留");
            assert!(cf.get(tdoc(2, 1)).unwrap().is_some(), "他表数据不受影响");
            assert!(cf.get(tdoc(1, 1)).unwrap().is_none(), "被删表行不可见");
        }
        // 重启：manifest 不悬空（被删文件不加载），表 2 数据仍在
        let mut cf2 = ColumnFamily::open("primary", &sst_dir, &cfg).unwrap();
        assert_eq!(cf2.sst_count(), 1);
        assert!(cf2.get(tdoc(2, 2)).unwrap().is_some());
        // 磁盘上表 1 的 SST 已消失
        let names: Vec<String> = std::fs::read_dir(&sst_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|f| f.starts_with(SST_PREFIX))
            .collect();
        assert_eq!(names.len(), 1, "磁盘应只剩表 2 的 SST，实际 {names:?}");
    }
}
