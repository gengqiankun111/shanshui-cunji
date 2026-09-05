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

//! 目录结构（按主题拆分，对外 API 路径保持 `crate::storage::column_family::*` /
//! `crate::column_family::*` 不变）：
//! - `mod.rs`：结构体/常量定义 + 子模块声明 + 顶层 re-export；
//! - [`open`]：启动恢复（open / WAL 回放 / TTL 过期清理）；
//! - [`write`]：写路径（put / delete / WAL append）；
//! - [`read`]：点查读路径（get / get_many* / get_bytes_at / sst_min_seq / 自由读函数）；
//! - [`scan`]：范围扫描路径（scan_range* / scan_stream* / count_keys_* / scan_raw_range_with_seq）；
//! - [`flush`]：刷盘（maybe_flush / flush_* / 快照构建）；
//! - [`io`]：后台 IO 限速 / 写压力自适应；
//! - [`table_ops`]：表级操作（purge / DROP TABLE / manifest 持久化）；
//! - [`tests`]：单元测试（自原 `mod tests` 外移）。
//!
//! Compaction 相关方法（compact_merge / finalize_compact / write_rows / write_sst / select_*）
//! 在 `crate::storage::sstable::compaction` 中实现（同类型 impl，按主题外置）。

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use arc_swap::ArcSwap;

use crate::blockcache::BlockCache;
use crate::config::model::MemtableConfig;
use crate::memtable::MemTableBuffer;
use crate::sstable::{Compression, SstReader};
use crate::wal::WalBackend;

/// SST 文件前缀。
pub(crate) const SST_PREFIX: &str = "sst-";
const WAL_FILE: &str = "wal.log";

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

/// 2026-09-05（P2 Bloom 分层计量）：布隆读路径三层过滤计数。
/// 语义（按候选 SST、每次点查/批量点查 key 计）：
/// - `minmax_skip`：段级 Zone Map（min/max）粗筛跳过（精确，无假阴性）；
/// - `legacy_skip`：v3/v4 整文件布隆 miss；
/// - `part_probe / part_skip / part_pass`：v5 分区布隆（目标块）校验进入/拒绝/放行；
/// - `fp_est`：误报估计——分区布隆放行并读块后该段未命中（近似，多版本/删除会低估；
///   真实命中在其他更旧段不减少本段计数）。
///
/// **2026-09-05（P0 观测升级）：计数按层（L0/L1/L2）分桶**——读路径层级固定三层
/// （layer_indices 0..=2），每段过滤发生在所属层桶内。观测可区分"L0 段线性放大"
/// （L0 桶 minmax_skip/part_probe 随段数线性）与"L1/L2 布隆 miss"（L1/L2 桶
/// probe/skip/pass），为 per-table L0 压实与分层 fpr 调参提供数据。
#[derive(Debug, Default)]
pub(crate) struct BloomCounters {
    /// 每层一组计数（索引 = 层号 lv；越界层号防御性落 L2 末桶，理论不发生）。
    pub(crate) layers: [BloomLayerCounters; 3],
}

impl BloomCounters {
    /// 取指定层计数桶（lv ≥ 已知层数 → 落 L2 末桶防御；读路径恒 0..=2）。
    #[inline]
    pub(crate) fn layer(&self, lv: usize) -> &BloomLayerCounters {
        &self.layers[lv.min(self.layers.len() - 1)]
    }

    /// 跨层聚合 6 元组（minmax/legacy/probe/skip/pass/fp）——既有观测总口径。
    pub(crate) fn totals(&self) -> (u64, u64, u64, u64, u64, u64) {
        let mut t = [0u64; 6];
        for lay in &self.layers {
            let v = lay.values();
            for (i, x) in v.iter().enumerate() {
                t[i] += x;
            }
        }
        (t[0], t[1], t[2], t[3], t[4], t[5])
    }
}

/// 单层布隆过滤计数桶（P0 分层观测；字段语义同上结构注释）。
#[derive(Debug, Default)]
pub(crate) struct BloomLayerCounters {
    pub(crate) minmax_skip: AtomicU64,
    pub(crate) legacy_skip: AtomicU64,
    pub(crate) part_probe: AtomicU64,
    pub(crate) part_skip: AtomicU64,
    pub(crate) part_pass: AtomicU64,
    pub(crate) fp_est: AtomicU64,
}

impl BloomLayerCounters {
    /// 本桶 6 计数（minmax/legacy/probe/skip/pass/fp，与既有 `bloom_counts` 顺序一致）。
    pub(crate) fn values(&self) -> [u64; 6] {
        [
            self.minmax_skip.load(Ordering::Relaxed),
            self.legacy_skip.load(Ordering::Relaxed),
            self.part_probe.load(Ordering::Relaxed),
            self.part_skip.load(Ordering::Relaxed),
            self.part_pass.load(Ordering::Relaxed),
            self.fp_est.load(Ordering::Relaxed),
        ]
    }
}

/// 列族：主数据 / 组合索引 / Delta 共用骨架。
pub struct ColumnFamily {
    pub(crate) name: String,
    pub(crate) dir: PathBuf,
    cfg: MemtableConfig,
    pub(crate) compression: Compression,
    pub(crate) compression_level: i32,
    /// Ex-8.12：L2+ 冷档 zstd 级别（0 = 不分层）；见 `compression_level_for`。
    pub(crate) compression_level_l2: i32,
    pub(crate) block_size: usize,
    /// 布隆假阳性率（`sstable.bloom_fpr`，分区布隆每块构建用）。
    pub(crate) bloom_fpr: f64,
    /// PAX 热字段白名单（阶段 1.5，来自 [storage] hot_fields；空 = 行式）。
    pub(crate) pax_hot_fields: Vec<String>,
    /// TTL 天数（None = 关闭；开启后 SST 按文档 ttl_field 分天桶，过期整文件删除）。
    ttl_days: Option<u32>,
    /// TTL 时间字段名（文档 JSON 内数值秒级时间戳）。
    ttl_field: String,
    /// 两级索引粒度（`sstable.index_granularity`，每 N 块一条 Level 1 摘要）。
    pub(crate) index_granularity: usize,
    /// M3（§26 多表）：按表切分 SST 输出——flush/compaction 按 docid 高 16 位（table_id）
    /// 边界把输出切为**每表一个文件**（docid 区间含表 → 查询窗口剪枝自动跳过其它表文件、
    /// DROP TABLE 可整文件回收）。仅 docid 定长 8 字节键的列族（主数据 primary）开启；
    /// 单表（table_id=0）输出仍单文件，与旧行为一致。
    pub(crate) split_by_table: bool,
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
    pub(crate) l0_max_size_bytes: u64,
    /// Ex-8.11：L1 段数触发阈值（0 = 现行为：L0 空时 L1>1 即下沉 L2）。>0 = 延迟大合并，
    /// L1 攒够该段数（或 L0 活跃纳入 L0+L1 合并的"已满"界限）才收敛。
    /// P4-A：AtomicUsize——写入爆发时自适应降为 2（`record_flush_new_l0` 动态调整）。
    pub(crate) l1_trigger_files: AtomicUsize,
    /// Ex-8.11：L2 段数触发阈值（0 = 现行为：L2>1 即收敛为单段）。
    pub(crate) l2_trigger_files: usize,
    /// P129：多表 per-table L0 压实触发阈值（`storage.per_table_l0_trigger`；0 = 关闭）——
    /// 某表在 L0 的段数 ≥ 该值 → compact() 只压实该表段子集（L1 同表并入），其余表不参与；
    /// 仅 split_by_table 且 L0 含 ≥2 表时启用（默认 2：同表 ≥2 段=可能重叠即压 → 稳态每表
    /// L0 ≤1 段、点查 O(1) 段；多表单批 flush 每表 1 文件场景防"每写必全量 L0 合并"，
    /// 单表库零回归）。
    pub(crate) per_table_l0_trigger: usize,
    /// Ex-8.6：文件级**最小 put seq** 惰性记忆（path → min seq of put rows）。
    /// 快照读（get_bytes_at / scan_stream_at，snapshot<MAX）整段剪枝用：文件所有 put
    /// 行的 seq 均 > 快照点 → 该段对快照贡献为空，O(1) 跳过（免建迭代器/免读块）。
    /// 未知（首见/重启后）→ 对该文件做一次 keys-only 扫描推导（一次性，按需缓存）；
    /// 重启安全：无需 manifest 扩展，首次快照读自动重建。
    seq_min: RwLock<std::collections::HashMap<PathBuf, u64>>,
    /// 单次合并输入大小上限（字节；`storage.compact_input_max_mb`；0 = 不限）——
    /// L0 分批合并防大输入一次合并长时间阻塞写。
    pub(crate) compact_input_max_bytes: u64,
    /// L 项：合并冷却轮次（`storage.compaction_cooldown`；0 = 关闭）。
    pub(crate) compaction_cooldown: u32,
    /// L 项：当前合并轮次（每次 compact 成功 +1；冷却到期基准）。
    /// O 项第③步：原子（compact `&self` 更新）。
    pub(crate) merge_round: AtomicU64,
    /// L 项：冷却中的段（新段 path → 到期轮次）；到期后正常参与合并。纯内存调度态。
    /// O 项第③步：内部 `Mutex`（compact `&self` 读写）。
    pub(crate) cooldown: std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, u64>>,
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
    /// 2026-09-05（P2）：布隆读路径三层过滤计数（点查/批量点查埋点）。
    pub(crate) bloom: BloomCounters,
    /// R4（review 2026-09-04）：MVCC 保活水位（seq floor）。compact_merge 去重时，
    /// 最新 seq > floor 的 key 保留多版本（活跃旧快照可回读到删除/覆盖前旧值）；
    /// 位图物理回收（drop_key）亦仅当无活跃快照（floor=0）或最新 seq ≤ floor 时执行。
    /// 0 = 关闭（现状：后写覆盖先写、GC 按位图物理回收）。引擎在 compact 前设置活跃
    /// 快照低水位，compact 后复位。
    pub(crate) mvcc_keep_floor: AtomicU64,
    /// Ex-8.11 A/B：累计**写入磁盘的 SST 字节**（flush/compact 每新建一个文件计一次该文件
    /// 字节；覆盖被合并删除的旧文件——总写入 = 数据量 × 写放大；/metrics 与写放大实验数据源）。
    pub(crate) sst_written: AtomicU64,
    /// 双缓冲 MemTable。
    memtable: MemTableBuffer,
    /// SST 快照（O 项第③步：ArcSwap 原子发布——读路径 load 无锁，写路径 store 切换）。
    pub(crate) ssts: ArcSwap<SstSnapshot>,
    /// P72（无锁合并）：SST 快照**变更互斥**——flush（snapshot_insert）与 compact
    /// （finalize_compact）无 Engine 锁并发时，ssts 的 load→build→store + manifest 持久化
    /// 需互斥（防两者基于旧快照 store，后写覆盖前写的并发丢失）。
    pub(crate) sst_mutate: Mutex<()>,
    /// 共享块缓存（跨 CF 共享）。
    block_cache: Arc<BlockCache>,
    /// 单调 seq 分配器（跨重启由 WAL 恢复推进）。
    seq: AtomicU64,
    /// 下一个 SST 文件 id（跨重启由 Manifest 恢复，防止覆盖旧文件）。O 项第③步：原子。
    pub(crate) next_sst_id: AtomicU64,
    /// 当前 WAL 写入器（append 追加 / ring 环形，design 4.3 阶段 3）。
    /// `Arc<Mutex>` 共享：组提交后台线程（M8）可独立触发落盘兜底。
    wal: Arc<Mutex<WalBackend>>,
    /// 外部全局 seq（MVCC，engine 层统一分配，M7-1）：Some 时写入走外部计数（跨列族一致）；
    /// None = 独立列族（测试 / 单 CF 场景）用内部 WAL seq。
    external_seq: Option<Arc<AtomicU64>>,
    /// Task-026 external WAL（research/percpu-wal-stage2-design.md 方案 A）：true 时本 CF
    /// **不再自持磁盘 WAL**——写路径把 (op,key,value) 交给 engine 写批次 TLS scope
    /// （`wal_collect`，组 gseq），持久化由 engine 级 per-CPU 队列接管。false（默认）逐字节
    /// 保持既有行为（自身 WalBackend append/组提交）。Engine 按 `per_cpu_enabled` 打开后设置。
    external_wal: bool,
    /// external 模式下本 CF 编号（WalEntry.cf；primary=0/delta=1/cidx=2/outbox=3）。
    external_cf_id: u8,
    /// 刷盘水位回调：flush 完成后上报本 CF 已刷盘最大 gseq（engine 侧推进 checkpoint =
    /// min(各 CF 水位)，供队列段裁剪）。None（internal 模式）= 不回调。
    flushed_cb: Option<Arc<dyn Fn(u64) + Send + Sync>>,
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

// ---------------------------------------------------------------------------
// 子模块（按主题拆分；子文件为私有 mod，公开项经下方 re-export 汇总）
// ---------------------------------------------------------------------------
mod flush;
mod io;
mod open;
mod read;
mod scan;
mod table_ops;
mod write;
#[cfg(test)]
mod tests;

// 跨文件复用的内部辅助（原 column_family.rs 模块级 `pub(crate)` 项，拆入子模块后
// 经此处 re-export 保持 `crate::storage::column_family::*` 可见性不变）。
// 注：TTL 时间工具（epoch_days / today_epoch_days / parse_sst_date）原为模块级私有项、
// 无 crate 公开路径需求，定义在 [`open`] 内（pub(crate)），需要处显式
// `use super::open::{...}` 引用，不在此汇总。
pub(crate) use read::key_table_id;

// ---------------------------------------------------------------------------
// 内部辅助
// ---------------------------------------------------------------------------

/// TTL 分桶行（key + 值 + flag + seq），flush_buckets / write_rows 使用。
pub(crate) type BucketRow = (Vec<u8>, Option<Vec<u8>>, u8, u64);
