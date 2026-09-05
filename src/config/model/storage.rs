//! 存储配置：内存表 / LSM 存储（数据目录、WAL、组提交、压缩预算）/ outbox 列族（design 4.x / Ex-*）。

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MemtableConfig {
    /// 跳表上限，达阈值冻结切换并后台刷盘。
    pub max_size_mb: usize,
}

impl Default for MemtableConfig {
    fn default() -> Self {
        Self { max_size_mb: 256 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// L0 文件数阈值，超过则写 Stall 限流。
    pub l0_stall_threshold: usize,
    /// TTL 天数（时间分区过期整删）。
    pub ttl_days: Option<u32>,
    /// 数据目录。
    pub data_dir: String,
    /// 多 SSD 条带化（Ex-5.10，design 4.8.3 P2）：WAL 独占最快 SSD 的目录
    /// （None = 用 `data_dir` 内各列族目录，维持旧布局）。
    pub wal_dir: Option<String>,
    /// 多 SSD 条带化：SSTable 数据盘目录（primary/cidx/delta 列族落此盘；
    /// None = 用 `data_dir` 内各列族目录）。
    pub sst_dir: Option<String>,
    /// 多 SSD 条带化：倒排索引独立盘目录（None = 用 `data_dir` 内 inverted）。
    pub inverted_dir: Option<String>,
    /// 热字段白名单（阶段 1.5 PAX 列式块）：高频查询字段进热列组（块头），其余进冷列组（块尾）。
    pub hot_fields: Vec<String>,
    /// TTL 时间分桶粒度：`day`（MVP）/ `hour`（预留，阶段 1.5 仅 day）。
    pub time_bucket: String,
    /// TTL 时间字段名（文档 JSON 内，数值秒级时间戳）。
    pub ttl_field: String,
    /// 后台 IO 限速（MB/s，design 4.5 阶段 3）：刷盘/Compaction 走 Token Bucket；0 = 不限速。
    pub io_rate_limit_mb: u64,
    /// WAL 模式："append"（传统追加文件，默认）/ "ring"（预分配环形文件，design 4.3 阶段 3 高性能）。
    pub wal_mode: String,
    /// 环形 WAL 预分配大小（MB，默认 64）。环形满且未刷盘时强制 Flush 腾空。
    pub wal_ring_size_mb: u64,
    /// 组提交窗口（µs，design 4.3 / M8）：0 = 关闭（保持逐条 fsync 强安全）；
    /// >0 = 窗口内所有写入攒批，一次 fsync 覆盖（延迟耐久：崩溃最多丢 ≤ 窗口数据）。
    /// **默认 1000µs（2026-09-05 Task-005 A/B 采纳：写混合负载 ~2k → ~48k ops/s，p99 ≈ 2.8ms）**。
    pub group_commit_us: u64,
    /// 组提交字节阈值：WAL 待刷缓冲 ≥ 此值立即 fsync（不等窗口）。
    pub group_commit_bytes: usize,
    /// P2-A（2026-09-04）：事务 COMMIT 提交耐久档位——对齐 MySQL `innodb_flush_log_at_trx_commit`：
    /// 1 = 每次 COMMIT 显式 fsync（强安全，默认）；
    /// 0/2 = COMMIT 不单独 fsync，落盘交给组提交窗口统一执行（延迟耐久：崩溃最多丢 ≤ 窗口数据，
    /// 并发事务共享窗口内一次 fsync——事务基准对比配此档位可把逐 COMMIT fsync 的 8× 拉到 ~2-3×；
    /// 本引擎无 InnoDB 独立 redo/OS-cache 层，档位 0/2 当前均为此语义，见 problem_solving P82）。
    pub flush_log_at_trx_commit: u8,
    /// Compaction 并行度（Ex-5.4，design 4.8.3）：并行压实 primary/cidx/delta 三列族
    /// （SSD 并发 IO 强，demo 实测 3 CF 并行 2.14×）；0 = 自动（min(4, 核数/2)）；
    /// 1 = 串行；>1 = 指定并行数。
    pub compaction_parallel: usize,
    /// 删除位图（Ex-5.6，design 4.6 / 4.8.3 阶段二）：独立于 LSM 的按 DocId 1bit 删除位图
    /// （4KB 页对齐）——删除仅写 1bit + fsync 1 页（-99% IO），查询 O(1) 跳过已删文档，
    /// 墓碑不再污染 LSM 层级；compaction 按位图物理删除，put 清位复活。
    /// true = 开启（默认）；false = 回退传统 Tombstone 路径。
    pub deletion_bitmap_enabled: bool,
    /// L 项（Compaction 智能调度）：动态窗口下限——低峰（写压力低）时 L0 更晚收敛，
    /// 合并次数更少、写放大更低（空间换写放大，SSD 时代）。
    pub l0_stall_min: usize,
    /// L 项：动态窗口上限——高峰（写压力高）时 L0 更早收敛，提前防段堆积 + 写 Stall。
    pub l0_stall_max: usize,
    /// L 项：合并冷却轮次（compact 输出段 N 轮内不参与下一轮合并，防"刚合并又合并"的
    /// 无谓重写，写放大 -10~20%）；0 = 关闭。纯调度策略，不改数据格式（崩溃安全）。
    pub compaction_cooldown: u32,
    /// P 项：事件驱动自动 Compaction——写入路径自触发（Flush 后 L0 文件数/大小超阈值 →
    /// 同步合并收敛，替代仅 CLI 显式 compact）。true = 开启（默认）；false = 保持仅显式调用。
    pub auto_compact: bool,
    /// P 项：L0 大小软阈值（MB）——L0 文件总字节超此值触发合并（与段数阈值互补，
    /// 防大段少量堆积；0 = 仅用段数阈值，默认关闭）。
    pub l0_max_size_mb: u64,
    /// 单次合并输入大小上限（MB，默认 1024）：L0 段多且总大小超限时**分批**合并
    /// （每轮只合并 ≤ 上限的部分段）——单次合并快 → 后台 worker 持读锁时间短 →
    /// 写锁等待短（修复大 L0 一次全合并长时间阻塞写）；多轮收敛由 worker 循环兜底。
    /// 0 = 不限（旧行为，一次合并全部 L0）。
    pub compact_input_max_mb: u64,
    /// Ex-8.11：L1 段数触发阈值（L0 空时 L1 **攒够该数量**才下沉 L2；L0 活跃时也以
    /// 该值为"L1 已满"纳入 L0+L1 合并的界限）。0 = 现行为（L0 空时 L1>1 即下沉）。
    /// 调大（如 8~12）= 延迟大合并：L1→L2 次数/底层重写 -50~80%，代价 L1 段数暂升
    /// （非重叠段，Ex-8.2 剪枝后窗口读不受影响）。
    /// **默认 8（2026-09-04 Ex-8.11 A/B 采纳：攒 8 写放大 5.62→3.21（-42.9%），点查/范围无回退）**。
    pub l1_trigger_files: usize,
    /// Ex-8.11：L2 段数触发阈值（L1 下沉后 L2 收敛为单段的触发数）。0 = 现行为（L2>1 即收敛）。
    pub l2_trigger_files: usize,
    /// Ex-8.7：删除密度 Compaction（删除位图置位率驱动的 GC 合并）——置位率 ≥
    /// `delete_density_min_ratio` 且自上次 GC 以来新增置位 ≥ `delete_density_min_docs` 时，
    /// compaction 紧迫度增加删除密度维度并把删除密集段（含收敛后单底层段）重写回收空间。
    /// 0.0 = 关闭（仅当 `deletion_bitmap_enabled` 且 >0 时生效）。
    pub delete_density_min_ratio: f32,
    /// Ex-8.7：单次 GC 触发的最小**新增**置位 docid 数（防小批量删除触发整段重写；
    /// 重启后初始基准 = 打开时位图置位数，历史置位不重复触发）。
    pub delete_density_min_docs: u64,
    /// P0-A（2026-09-04）：声明式组合索引——每项为一组字段名（最左前缀匹配）。
    /// 写路径自动提取字段值写入 cidx 列族；读路径 sqlish 匹配 WHERE 等值前缀后路由到
    /// `query_by_composite_prefix`（引擎已具备，缺口在写路径 + SQL 层接线）。
    /// 例：`[["status","ts"], ["region","k"]]` → `WHERE status='active' AND ts=?` 走组合索引。
    pub composite_indexes: Vec<Vec<String>>,
    /// P4-A：Compaction 写入速率自适应——滑动窗口大小（flush 次数），窗口内统计新增 L0 段数。
    /// 默认 8 → 看最近 8 次 flush 平均增速。
    pub compaction_write_rate_window: usize,
    /// P4-A：写入爆发阈值（滑动窗口内新增 L0 段数），超过此值视为写入爆发，
    /// L1 触发阈值从 `l1_trigger_files` 降为 2 → 提前下沉 L1→L2，防 L0 爆胀。
    pub compaction_write_rate_burst: usize,
    // ---- Task-026 Per-CPU WAL（research/range-scan-percpu-wal-design.md §二，2026-09-05）----
    /// 每队列独立 `wal-{queue}-{gseq_start}.log` + N 个后台消费线程（组提交窗口）批量写，
    /// 目标：多核高频非事务写的 WAL fsync 锁竞争摊薄（16 核预期 +36~59%）。全局 gseq 语义沿用。
    /// **当前默认 false（安全回退全局组提交）**——阶段1 仅落地配置与路由骨架；核心线程写盘/
    /// 恢复归并完成后翻转默认 true 并全量回归（回退 = false → 现有 WalBackend 全局组提交）。
    pub per_cpu_enabled: bool,
    /// 队列数：0 = 自动（CPU 核数，上限 64）；1 = 等效单队列（可作对照）；>64 截断到 64。
    pub per_cpu_queues: usize,
    /// 单队列最大缓冲条目数：队列满时写侧背压（阻塞/503），防无界内存。
    pub per_cpu_queue_depth: usize,
    /// 每队列组提交 fsync 窗口（µs；队列后台线程批量写窗口，语义同 group_commit_us）。
    pub per_cpu_batch_window_us: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            // Ex-5.4（design 4.8.3）：L0 触发阈值放宽（8→12，SSD 空间换写放大——
            // L0 更晚收敛 → 压实频率低，写放大 15~25×→6~10×；空间放大 1.2×→1.8×）。
            l0_stall_threshold: 12,
            ttl_days: None,
            data_dir: "./data".into(),
            // Ex-5.10：多 SSD 条带化默认关闭（None = 单盘 data_dir 布局）；
            // 配置 wal_dir/sst_dir/inverted_dir 指向不同盘实现 WAL 独占最快盘 + 数据/倒排分盘。
            wal_dir: None,
            sst_dir: None,
            inverted_dir: None,
            hot_fields: Vec::new(),
            time_bucket: "day".into(),
            ttl_field: "timestamp".into(),
            io_rate_limit_mb: 0,
            wal_mode: "append".into(),
            // Ex-5.5：环形 WAL 规模化默认（64→256MB）——减少小环频繁回绕强制 Flush；
            // 大容量预分配（GB 级）+ 环形覆盖均匀（SSD 磨损天然均衡）已由 RingWal 支持。
            wal_ring_size_mb: 256,
            // 组提交默认开（Task-005 A/B 采纳，2026-09-05）：1000µs 窗口批量一次 fsync，
            // 写混合吞吐 ~24×；显式 0 = 逐条 fsync 强安全（旧默认）。
            group_commit_us: 1000,
            group_commit_bytes: 256 * 1024,
            // P2-A：事务 COMMIT 默认逐次 fsync（强安全，同 MySQL innodb_flush_log_at_trx_commit=1）。
            flush_log_at_trx_commit: 1,
            compaction_parallel: 0, // 0 = 自动（并行）
            // Ex-5.6（design 4.6）：SSD 原生删除位图默认开启——删除 1bit+1 页 fsync（-99% IO），
            // 查询 O(1) 跳过，墓碑不污染 LSM；关闭回退传统 Tombstone 路径。
            deletion_bitmap_enabled: true,
            // L 项（Compaction 智能调度）：动态窗口 8~16（基础 12 ± 4，随写压力浮动，
            // 高峰收窄提前收敛、低峰放宽降写放大）；合并冷却 2 轮防刚合并又合并。
            l0_stall_min: 8,
            l0_stall_max: 16,
            compaction_cooldown: 2,
            // P 项：事件驱动自动 Compaction 默认开启；大小阈值默认关闭（仅段数阈值，
            // 保持既有行为；需按段大小触发时配置 l0_max_size_mb）。
            auto_compact: true,
            l0_max_size_mb: 0,
            compact_input_max_mb: 1024,
            // Ex-8.11：L1 触发阈值默认 8（攒批延迟大合并，A/B -42.9% 写放大已采纳）；
            // L2 触发默认 0 = 现行为（L2>1 收敛）。
            l1_trigger_files: 8,
            l2_trigger_files: 0,
            // Ex-8.7：删除密度 GC 默认开启——置位率 ≥10% 且自上次 GC 新增置位 ≥1000 时触发
            // （防小批量删除/历史置位误触发整段重写；删除密集负载空间回收依赖此路径）。
            delete_density_min_ratio: 0.10,
            delete_density_min_docs: 1000,
            // P4-A：写入速率自适应窗口 8 次 flush，爆发阈值 4（窗口内 >4 次 flush 新增 L0 = 爆发）
            compaction_write_rate_window: 8,
            compaction_write_rate_burst: 4,
            composite_indexes: Vec::new(),
            // Task-026：默认关闭（阶段1 安全回退），核心/恢复完成后翻 true 见 dev_remain。
            per_cpu_enabled: false,
            per_cpu_queues: 0,
            per_cpu_queue_depth: 4096,
            per_cpu_batch_window_us: 100,
        }
    }
}

/// 本地消息表（Ex-1，design_extension v0.1 L1）：业务写 + outbox 消息同一本地事务，
/// 后台投递幂等消费（双写扩容衔接 / 异步索引补偿 / 跨节点异步写）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct OutboxConfig {
    /// 是否启用 outbox 列族（默认关闭——按需开启，零额外开销）。
    pub enabled: bool,
}