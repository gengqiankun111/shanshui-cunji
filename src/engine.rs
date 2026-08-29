//! 文档引擎 + 查询执行器（design 7.1 / development 步骤 12）。
//!
//! 整合主数据列族、组合索引列族、倒排索引与 HotCache，对外提供文档级 CRUD 与查询；
//! 查询执行器基于 optimizer 静态路由（MVP 最小集枚举，无代价估算），
//! 后续动态路由（代价估算依赖统计载荷）在阶段 1.5 落地。
//!
//! 删除一致性：文档删除后，倒排中残留 docid 在回表时经主数据 Tombstone 天然过滤。

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use roaring::RoaringBitmap;

use crate::bitmap::DeletionBitmap;
use crate::column_family::ColumnFamily;
use crate::config::model::Config;
use crate::error::Result;
use crate::hotcache::HotCache;
use crate::inverted::InvertedIndex;
use crate::keys::{decode_docid, encode_docid, encode_varlen};
use crate::optimizer::{route, AccessPath, QuerySpec};
use crate::outbox::Outbox;
use crate::watchdog::{Watchdog, DEFAULT_QUERY_TIMEOUT};

/// 引擎：组合主数据 + 组合索引 + Delta 增量 + 倒排 + HotCache。
pub struct Engine {
    /// 主数据列族（value = 序列化文档字节）。
    primary: ColumnFamily,
    /// 组合索引列族（key = encode_composite_key）。
    cidx: Option<ColumnFamily>,
    /// Delta 增量列族（阶段 1.5，key = encode_docid ++ VarLen(field)，Merge-on-Read 覆盖 Base）。
    delta: ColumnFamily,
    /// 倒排索引。
    inverted: InvertedIndex,
    /// 文档热缓存。
    hotcache: HotCache,
    /// 看门狗（OOM 限流 + 查询超时熔断）。
    watchdog: Watchdog,
    /// 内存使用率估算（0~1，由上层注入或监控更新）。
    mem_ratio: f64,
    /// 内存硬上限（MB，`memory.max_memory_mb`，供 admin status）。
    max_memory_mb: usize,
    /// 全局 seq 分配器（MVCC，M7-1）：primary / delta 列族共享，跨列族写入序一致。
    global_seq: Arc<AtomicU64>,
    /// 组提交（M8）：Some((窗口, 字节阈值)) = 开启；None = 关闭（逐条 fsync 强安全）。
    /// 窗口内写入攒批一次 fsync（design 4.3 / M8，`storage.group_commit_us`）。
    group_commit: Option<(Duration, usize)>,
    /// 组提交后台线程停止标志（窗口尾部落盘兜底）。
    gc_stop: Option<Arc<AtomicBool>>,
    /// 组提交后台线程句柄。
    gc_thread: Option<std::thread::JoinHandle<()>>,
    /// 倒排字段白名单（M8-P4）：Some = 只建声明字段倒排；None = 全部（黑名单仍生效）。
    inverted_include: Option<std::collections::HashSet<String>>,
    /// 倒排字段黑名单（M8-P4）：这些字段不建倒排（白名单非空时忽略）。
    inverted_exclude: std::collections::HashSet<String>,
    /// 倒排 term 长度上限（M8-P4）：超过自动跳过（长文本整串不进字典）；0 = 不限。
    max_term_len: usize,
    /// fulltext 分词字段（M8-P7）：声明字段做分词建词 term 索引（`ft:{field}:{token}`）。
    fulltext_fields: std::collections::HashSet<String>,
    /// 中文分词器（M8-P13）：true = jieba 完整词典分词（需 cjk-jieba feature）；
    /// false = bigram（M8-P9）。来自 `[inverted] cjk_segmenter`。
    use_jieba: bool,
    /// 批量导入模式（P40）：跳过 HotCache 回填/失效。批量导入只写不读，回填缓存纯浪费内存
    /// （4GB 预算灌满 + stats 泄漏 → 触发页面颠簸 → 行速指数级崩塌，50M 导入 4M 行后卡死）。
    skip_hotcache: bool,
    /// 倒排更新攒批缓冲（Ex-5.3）：put 时 term 先入缓冲，达阈值/查询/flush 时
    /// 一次性 `add_batch` 批量刷入内存字典——低基数 term 跨行聚合，
    /// 减少 DashMap 锁操作次数（N×字段数 → ~唯一 term 数）。
    /// 崩溃安全：WAL 回放重新走 put 重建倒排，缓冲丢失不丢数据。
    pending_inverted: Vec<(String, u64)>,
    /// Compaction 并行度（Ex-5.4）：并行压实 primary/cidx/delta 三列族；
    /// 0 = 自动（min(4, 核数/2)），1 = 串行，>1 = 指定并行数。
    compaction_parallel: usize,
    /// 删除位图（Ex-5.6）：Some = 开启（delete 写 1bit 跳 Tombstone、get O(1) 跳过、
    /// compaction 物理删除）；None = 关闭（传统 Tombstone 路径）。
    deletion_bitmap: Option<DeletionBitmap>,
    /// 三池核分区（Ex-7.2）：network（server 主线程）/ compute（Compaction 并行）/
    /// io（组提交后台）——绑核消除调度抖动；enabled=false 时为空（no-op）。
    affinity: crate::affinity::CpuPartition,
    /// 后台 IO 限速基准（字节/秒，Ex-7.4）：`storage.io_rate_limit_mb` 换算；0 = 不限速。
    io_rate_base_bytes: u64,
    /// MemTable 容量上限（字节，Ex-7.4 写压力代理基准，`memtable.max_size_mb`）。
    memtable_max_bytes: usize,
    /// 本地消息表（Ex-1）：Some = 启用（业务写同一本地事务入队 outbox，后台投递幂等消费）；
    /// None = 关闭（默认零开销）。
    outbox: Option<Outbox>,
}

/// 倒排攒批缓冲阈值（条，Ex-5.3）：达此值强制 `flush_inverted_pending`。
const INVERTED_PENDING_CAP: usize = 8192;

/// 查询结果行：docid + 文档字节。
pub type QueryRow = (u64, Vec<u8>);

/// 合并两列族的压实报告（Ex-5.4：multi-CF 聚合；out_level 保留 base 值）。
fn merge_report(base: &mut crate::column_family::CompactReport, other: &crate::column_family::CompactReport) {
    base.merged_ssts += other.merged_ssts;
    base.kept_keys += other.kept_keys;
    base.freed_bytes += other.freed_bytes;
}

/// 分页查询结果（M8-P8）：`total` = 全量命中数（倒排 bitmap.len()，O(1)），
/// `rows` = 当前页（只回表 limit 行，内存 O(limit) 不随 total 膨胀——
/// 大结果集命中数百万行时全量回表 + JSON 构造会内存爆炸，实测 5M 行 → 10GB+ 卡死）。
#[derive(Debug, Clone)]
pub struct PagedRows {
    pub total: u64,
    pub rows: Vec<QueryRow>,
}

/// 增量备份文件（design 20，M6-5）：seq 游标 + WAL 记录集（JSON 持久化）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct IncrementalBackupFile {
    since_seq: u64,
    until_seq: u64,
    records: Vec<crate::wal::WalRecord>,
}

/// 增量备份报告。
#[derive(Debug, Clone, Copy)]
pub struct BackupReport {
    pub since_seq: u64,
    pub until_seq: u64,
    pub records: usize,
}

/// 引擎状态指标（`admin status` 数据源）。
#[derive(Debug, Clone)]
pub struct EngineStats {
    /// LSM：SST 文件总数（primary + cidx + delta）。
    pub sst_file_count: usize,
    /// 倒排内存累积 posting 数。
    pub inverted_mem_docids: u64,
    /// 倒排磁盘段数。
    pub inverted_segments: usize,
    /// 当前序列号（阶段 2 接入）。
    pub seq: u64,
    /// 内存使用率估算（0~1）。
    pub mem_ratio: f64,
    /// 内存硬上限（MB）。
    pub max_memory_mb: usize,
}

impl Engine {
    /// 打开（或创建）引擎。倒排刷盘阈值取自内存预算的比例（MVP 固定 1M posting）。
    pub fn open(data_dir: &Path, cfg: &Config) -> Result<Self> {
        Self::open_with_timeout(data_dir, cfg, DEFAULT_QUERY_TIMEOUT)
    }

    /// 打开引擎并指定查询超时（压测/大结果集场景需放宽熔断阈值）。
    pub fn open_with_timeout(
        data_dir: &Path,
        cfg: &Config,
        query_timeout: std::time::Duration,
    ) -> Result<Self> {
        // Ex-5.10 多 SSD 条带化目录路由：WAL 独占最快盘（wal_dir），SSTable 数据盘（sst_dir），
        // 倒排独立盘（inverted_dir）；未配置时回退单盘 data_dir 布局（旧行为）。
        let sst_root = cfg.storage.sst_dir.as_deref().map(Path::new).unwrap_or(data_dir);
        let wal_root = cfg.storage.wal_dir.as_deref().map(Path::new).unwrap_or(sst_root);
        let inverted_root = cfg
            .storage
            .inverted_dir
            .as_deref()
            .map(Path::new)
            .unwrap_or(data_dir);
        let primary = ColumnFamily::open_with_wal_dir(
            "primary",
            &sst_root.join("primary"),
            Some(wal_root),
            cfg,
        )?;
        let mut inverted = InvertedIndex::open_with_gc(
            &inverted_root.join("inverted"),
            1_000_000,
            &cfg.inverted.engine,
            cfg.inverted.segment_max_size_mb * 1024 * 1024,
        )?;
        // 位图索引（design 5.2.4，M7-2）：白名单非空时全量重建内存位图
        inverted.with_bitmap_fields(&cfg.inverted.bitmap_fields)?;
        let cidx = ColumnFamily::open_with_wal_dir(
            "cidx",
            &sst_root.join("cidx"),
            Some(wal_root),
            cfg,
        )
        .ok();
        let delta = ColumnFamily::open_with_wal_dir(
            "delta",
            &sst_root.join("delta"),
            Some(wal_root),
            cfg,
        )?;
        // 删除位图（Ex-5.6）：开启时加载/创建独立位图文件（4KB 页对齐，见 bitmap.rs）
        let deletion_bitmap = if cfg.storage.deletion_bitmap_enabled {
            Some(DeletionBitmap::open(&data_dir.join("deletion.bitmap"))?)
        } else {
            None
        };
        // 本地消息表（Ex-1）：开启时打开 outbox 列族（数据盘，与 primary 同崩溃安全模型）
        let outbox = if cfg.outbox.enabled {
            Some(Outbox::open(&sst_root.join("outbox"), cfg)?)
        } else {
            None
        };
        // MVCC 全局 seq（M7-1）：以各列族 WAL 恢复后的 next_seq 取最大作为全局起点，
        // 此后 primary / delta / outbox 写入共享同一计数器（跨列族快照隔离正确）。
        let global_seq = Arc::new(AtomicU64::new(
            primary
                .wal_next_seq()
                .max(delta.wal_next_seq())
                .max(outbox.as_ref().map_or(0, |o| o.wal_next_seq())),
        ));
        let mut primary = primary;
        primary.set_external_seq(Arc::clone(&global_seq));
        let mut delta = delta;
        delta.set_external_seq(Arc::clone(&global_seq));
        let hotcache = HotCache::new(cfg.hotcache.clone());
        let watchdog = Watchdog::new(cfg, query_timeout);
        let mut engine = Self {
            primary,
            cidx,
            inverted,
            delta,
            hotcache,
            watchdog,
            mem_ratio: 0.0,
            max_memory_mb: cfg.hotcache.max_memory_mb + cfg.blockcache.max_memory_mb,
            global_seq,
            group_commit: None,
            gc_stop: None,
            gc_thread: None,
            inverted_include: if cfg.inverted.inverted_fields.is_empty() {
                None
            } else {
                Some(cfg.inverted.inverted_fields.iter().cloned().collect())
            },
            inverted_exclude: cfg.inverted.exclude_fields.iter().cloned().collect(),
            max_term_len: cfg.inverted.max_term_len,
            fulltext_fields: cfg.inverted.fulltext_fields.iter().cloned().collect(),
            use_jieba: cfg!(feature = "cjk-jieba") && cfg.inverted.cjk_segmenter == "jieba",
            skip_hotcache: false,
            pending_inverted: Vec::new(),
            compaction_parallel: cfg.storage.compaction_parallel,
            deletion_bitmap,
            affinity: crate::affinity::plan_partition(&cfg.affinity),
            io_rate_base_bytes: cfg.storage.io_rate_limit_mb * 1024 * 1024,
            memtable_max_bytes: cfg.memtable.max_size_mb * 1024 * 1024,
            outbox,
        };
        // 组提交（M8）：`storage.group_commit_us > 0` 时开启——窗口内写入攒批一次 fsync，
        // 后台线程兜底窗口尾部落盘；默认 0 = 关闭（保持逐条 fsync 强安全）。
        engine.start_group_commit(cfg);
        Ok(engine)
    }

    /// 启动组提交（M8）：窗口 + 字节阈值触发；spawn 后台线程兜底窗口尾部落盘。
    /// 关闭（`group_commit_us == 0`）时无任何开销（保持逐条 fsync 强安全语义）。
    fn start_group_commit(&mut self, cfg: &Config) {
        let window_us = cfg.storage.group_commit_us;
        if window_us == 0 {
            return;
        }
        let window = Duration::from_micros(window_us);
        let bytes = cfg.storage.group_commit_bytes.max(1);
        self.group_commit = Some((window, bytes));

        // 后台兜底线程：每 ≤ 窗口唤醒一次，有待刷缓冲且窗口到期则 fsync（覆盖窗口尾部）。
        // 仅触碰共享 WAL 锁，不访问 Engine 本体（避免自引用/锁顺序问题）。
        let pwal = self.primary.wal_handle();
        let dwal = self.delta.wal_handle();
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);
        let tick = if window < Duration::from_millis(10) {
            window
        } else {
            Duration::from_millis(10)
        };
        self.gc_stop = Some(stop);
        let io_cores = self.affinity.io.clone(); // Ex-7.2：IO 后台线程绑 io 核
        self.gc_thread = Some(std::thread::spawn(move || {
            crate::affinity::bind_current(&io_cores); // 失败仅忽略（no-op）
            loop {
            std::thread::sleep(tick);
            if stop2.load(Ordering::Relaxed) {
                break;
            }
            let now = std::time::Instant::now();
            for w in [&pwal, &dwal] {
                let _ = w.lock().map(|mut g| {
                    if g.pending_bytes() > 0 && g.sync_due(now, window, 0) {
                        if let Err(e) = g.sync() {
                            tracing::debug!("组提交兜底落盘失败（等待写路径处理）: {e}");
                        }
                    }
                });
            }
            }
        }));
    }

    /// 组提交判定（M8）：关闭 → 逐条 fsync（现状强安全）；
    /// 开启 → 写路径零 fsync，由后台提交线程按窗口统一落盘（ack 后最多延迟 ≤ 窗口，
    /// 字节阈值触发也由后台线程判定）——避免写路径与后台线程双份 fsync + 锁竞争。
    fn maybe_group_commit(&mut self) -> Result<()> {
        if self.group_commit.is_none() {
            self.flush_wal()?;
        }
        Ok(())
    }

    /// 更新内存使用率估算（OOM Guardian 输入，由监控/统计层刷新）。
    pub fn set_mem_ratio(&mut self, ratio: f64) {
        self.mem_ratio = ratio.clamp(0.0, 1.0);
    }

    /// 批量导入模式开关（P40）：开启后 `put_nosync` 跳过 HotCache 失效/回填。
    /// 批量导入只写不读，回填缓存纯浪费内存（默认 4GB 预算会把文档全部塞入，
    /// 叠加桌面负载触发页面颠簸 → 行速崩塌）。导入结束后应关闭恢复常规缓存语义。
    pub fn set_bulk_import(&mut self, on: bool) {
        self.skip_hotcache = on;
    }

    /// 写入文档（docid + 序列化字节 + 该文档涉及的倒排词条）。
    /// 写失效链：先失效 HotCache 与组合索引旧条目，最后写 LSM（design 6.6）。
    /// OOM Guardian：写入前按水位限流/熔断（design 14.1.1）。
    pub fn put(&mut self, docid: u64, value: Vec<u8>, terms: &[&str]) -> Result<()> {
        // 内存限流：软水位返回限流信号（MVP 仍放行，记录计数）；硬水位直接拒绝
        let _status = self.watchdog.memory_check(self.mem_ratio)?;
        self.put_nosync(docid, value, terms)?;
        // 组提交（M8）：开启时窗口内攒批一次 fsync，否则逐条 fsync（强安全）
        self.maybe_group_commit()?;
        // Ex-7.4：按前台写压力动态调整 Compaction 限速（MemTable 水位让路）
        self.adjust_compaction_io_rate();
        Ok(())
    }

    /// Ex-7.4：动态限流——按主数据 MemTable 水位（前台写压力代理）下调 Compaction 限速：
    /// 压力 p → 限速 = base × (1 - 0.5p)——压力 0 全速追赶 L0 合并，压力 1 让路 50%
    /// 磁盘带宽给前台写（design_extension 12.6：写压力高时压缩 Compaction 带宽）。
    fn adjust_compaction_io_rate(&mut self) {
        if self.io_rate_base_bytes == 0 {
            return; // 未配置限速（io_rate_limit_mb = 0）
        }
        let used = self.primary.memtable_bytes() as f64;
        let max = self.memtable_max_bytes.max(1) as f64;
        let pressure = (used / max).clamp(0.0, 1.0);
        let rate = (self.io_rate_base_bytes as f64 * (1.0 - 0.5 * pressure)) as u64;
        self.primary.set_io_rate_bytes(rate);
        self.delta.set_io_rate_bytes(rate);
        if let Some(c) = &mut self.cidx {
            c.set_io_rate_bytes(rate);
        }
    }

    /// 批量写入（不逐条 fsync，供亿级压测；结束时调用 `flush_wal` 统一提交）。
    pub fn put_nosync(&mut self, docid: u64, value: Vec<u8>, terms: &[&str]) -> Result<()> {
        // ① 失效 HotCache 该 docid（批量导入模式跳过：只写不读，避免缓存膨胀挤爆内存，P40）
        if !self.skip_hotcache {
            self.hotcache.invalidate(docid);
        }
        // ② 主数据（权威源，WAL 攒批不逐条 fsync）；全量覆盖 → 清空该 docid 的增量（避免旧 patch 覆盖新数据）
        self.primary
            .put_bytes_nosync(encode_docid(docid).to_vec(), value.clone())?;
        self.delta.delete_prefix(&encode_docid(docid))?;
        // ②.5 删除位图复活（Ex-5.6）：put 覆盖 delete → 清位（O(1) 内存，位未置时零 IO）；
        //     持久性与 WAL 同步（flush_wal 先刷位图后刷 WAL）
        if let Some(bm) = &mut self.deletion_bitmap {
            bm.clear(docid);
        }
        // ③ 倒排（内存字典累积，Ex-5.3 攒批：term 先入缓冲，达阈值/查询/flush 时批量刷入）；
        //    M8-P4：白名单/黑名单/超长 term 过滤（长文本整串不进字典，防膨胀）
        for t in terms {
            if self.inverted_allowed(t) {
                self.pending_inverted.push((t.to_string(), docid));
            }
        }
        if self.pending_inverted.len() >= INVERTED_PENDING_CAP {
            self.flush_inverted_pending();
        }
        // ④ 回填 HotCache（写后回填，供热点查询亚毫秒命中；批量导入模式跳过，P40）
        if !self.skip_hotcache {
            self.hotcache.put(docid, value);
        }
        Ok(())
    }

    /// 倒排 term 过滤（M8-P4）：白名单（只建声明字段）→ 黑名单（排除字段）→ 超长 term 自动跳过。
    /// term 编码 `field=value`，field 为 JSON 字段路径（嵌套用 `.` 连接）。
    fn inverted_allowed(&self, term: &str) -> bool {
        // 超长 term（长文本整串）自动跳过：防止误配下字典膨胀
        if self.max_term_len > 0 && term.len() > self.max_term_len {
            return false;
        }
        let field = term.split('=').next().unwrap_or("");
        if let Some(include) = &self.inverted_include {
            return include.contains(field);
        }
        !self.inverted_exclude.contains(field)
    }

    /// 统一提交 WAL（批量写入结束后调用，保证崩溃可恢复）。
    /// Ex-5.6：删除位图脏页**先于** WAL fsync 落盘——若崩溃发生在 WAL fsync 之后、
    /// 环形 WAL 截断推进之前，位图已持久（删除不丢）；反之位图先持久、WAL 回放重删幂等。
    pub fn flush_wal(&mut self) -> Result<()> {
        if let Some(bm) = &mut self.deletion_bitmap {
            bm.flush()?;
        }
        self.primary.sync_wal()?;
        self.delta.sync_wal()?;
        // Ex-1：outbox 消息与业务写同 fsync 点（本地原子：崩溃恢复按 seq 回放）
        if let Some(ob) = &mut self.outbox {
            ob.sync_wal()?;
        }
        Ok(())
    }

    /// 强制刷盘主数据 MemTable → SST（测试 / 备份一致性准备用）。
    pub fn flush_primary(&mut self) -> Result<()> {
        self.primary.switch_and_flush()
    }

    /// 当前已分配的最大 seq（全量备份点 / 增量备份游标基础）。
    pub fn current_seq(&self) -> u64 {
        self.global_seq.load(Ordering::Relaxed).saturating_sub(1)
    }

    /// 增量备份（design 20，M6-5）：导出 seq ∈ (since_seq, 当前] 的 WAL 记录为 JSON 文件。
    /// 若 WAL 已被截断（环形覆盖 / 长时间未备份）导致缺口 → 报错提示改做全量备份。
    pub fn backup_incremental(&mut self, since_seq: u64, out_path: &Path) -> Result<BackupReport> {
        // 组提交（M8）前置落盘：保证导出的 WAL 记录已持久化（否则崩溃恢复可能丢失 → 备份与恢复不一致）
        self.flush_wal()?;
        let until_seq = self.current_seq();
        let (oldest, records) = self.primary.wal_records_since(since_seq)?;
        if since_seq != 0 && oldest > since_seq + 1 {
            return Err(crate::error::Error::Unsupported(format!(
                "增量备份缺口：可用 WAL 最旧 seq {oldest} > 上次备份点 {since_seq}+1，请先做全量备份"
            )));
        }
        let file = IncrementalBackupFile {
            since_seq,
            until_seq,
            records,
        };
        let count = file.records.len();
        let text = serde_json::to_string_pretty(&file)
            .map_err(|e| crate::error::Error::Serialize(format!("增量备份序列化失败: {e}")))?;
        let tmp = out_path.with_extension("tmp");
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, out_path)?; // 原子落盘（tmp+rename）
        Ok(BackupReport {
            since_seq,
            until_seq,
            records: count,
        })
    }

    /// 增量恢复：将增量记录按序重放到已还原的引擎（PUT 重新派生倒排词条；DELETE 写墓碑）。
    /// 返回应用记录数。恢复后调用方应再做一次全量备份以合并游标。
    pub fn restore_incremental(&mut self, path: &Path) -> Result<usize> {
        let text = std::fs::read_to_string(path)?;
        let file: IncrementalBackupFile = serde_json::from_str(&text)
            .map_err(|e| crate::error::Error::Corrupted(format!("增量备份文件解析失败: {e}")))?;
        let mut applied = 0usize;
        for r in &file.records {
            let docid = decode_docid(&r.key)
                .map_err(|_| crate::error::Error::Corrupted("增量记录 key 非 docid 编码".into()))?;
            match r.op {
                crate::wal::OP_PUT => {
                    let value = r.value.clone().unwrap_or_default();
                    let terms = match serde_json::from_slice::<serde_json::Value>(&value) {
                        Ok(v) => crate::server::extract_terms(&v),
                        Err(_) => Vec::new(), // 非 JSON 原始字节文档：无倒排词条
                    };
                    let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
                    self.put(docid, value, &t)?;
                }
                crate::wal::OP_DELETE => {
                    self.delete(docid)?;
                }
                other => {
                    return Err(crate::error::Error::Corrupted(format!(
                        "增量记录未知 op {other}"
                    )))
                }
            }
            applied += 1;
        }
        Ok(applied)
    }

    /// 删除文档：失效 HotCache + 清空 Delta（倒排残留 docid 由回表过滤）。
    /// Ex-5.6：删除位图开启时**仅写 1bit + WAL 删除记录**（不写 memtable Tombstone，
    /// 不逐条 fsync——墓碑不进入 LSM 层级，-99% IO）；WAL 记录供增量备份导出/崩溃回放
    /// （回放转本函数重新置位，幂等）。关闭时回退传统 Tombstone 路径。
    pub fn delete(&mut self, docid: u64) -> Result<()> {
        self.hotcache.invalidate(docid);
        match &mut self.deletion_bitmap {
            Some(bm) => {
                bm.mark_deleted(docid);
                self.primary
                    .delete_record_wal(encode_docid(docid).to_vec())?;
                self.delta.delete_prefix(&encode_docid(docid))?;
            }
            None => {
                self.primary.delete(docid)?;
                self.delta.delete_prefix(&encode_docid(docid))?;
            }
        }
        Ok(())
    }

    /// 部分更新（阶段 1.5，design 4.7）：仅写入变更字段到 Delta CF（几十字节小记录），
    /// 读取时 Merge-on-Read 覆盖 Base；`null` 值表示删除该字段。替代全量 PUT，写入 IO 放大趋近 1。
    pub fn patch(&mut self, docid: u64, fields: &[(&str, serde_json::Value)]) -> Result<()> {
        self.hotcache.invalidate(docid);
        for (f, v) in fields {
            let mut key = encode_docid(docid).to_vec();
            encode_varlen(&mut key, f.as_bytes());
            let val =
                serde_json::to_vec(v).map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
            self.delta.put_bytes_nosync(key, val)?;
        }
        self.maybe_group_commit()
    }

    /// 点查文档：HotCache 命中直达，否则主数据 LSM + Delta Merge-on-Read。
    /// Ex-5.6：删除位图开启时先 O(1) 判定，已删文档直接返回 None（零 LSM 读）。
    pub fn get(&mut self, docid: u64) -> Result<Option<Vec<u8>>> {
        if let Some(bm) = &self.deletion_bitmap {
            if bm.is_deleted(docid) {
                return Ok(None);
            }
        }
        if let Some(v) = self.hotcache.get(docid) {
            return Ok(Some(v));
        }
        let found = self.primary.get(docid)?;
        let Some((bv, _)) = found else {
            return Ok(None);
        };
        // Delta 覆盖（对象合并；null 删除字段）；非 JSON / 非对象文档直接返回 Base（raw 字节场景）
        let obj: serde_json::Value = match serde_json::from_slice(&bv) {
            Ok(v) => v,
            Err(_) => return Ok(Some(bv)),
        };
        let mut map = match obj {
            serde_json::Value::Object(m) => m,
            _ => return Ok(Some(bv)),
        };
        let start = encode_docid(docid).to_vec();
        let mut end = start.clone();
        end.extend_from_slice(&[0xFF; 4]);
        let rows = self.delta.scan_raw_range(Some(&start), Some(&end))?;
        for (k, v) in rows {
            if !k.starts_with(&start) || k.len() < 12 {
                continue;
            }
            let field = String::from_utf8(k[12..].to_vec())
                .map_err(|_| crate::error::Error::Corrupted("Delta 字段名非法 UTF-8".into()))?;
            let val: serde_json::Value = serde_json::from_slice(&v)
                .map_err(|e| crate::error::Error::Corrupted(format!("Delta 值解析失败: {e}")))?;
            if val.is_null() {
                map.shift_remove(&field);
            } else {
                map.insert(field, val);
            }
        }
        let merged =
            serde_json::to_vec(&map).map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
        self.hotcache.put(docid, merged.clone());
        Ok(Some(merged))
    }

    /// 获取当前快照点（已分配的最大 seq）：此后以该值为快照的 `get_at` 读到一致视图。
    pub fn begin_snapshot(&self) -> u64 {
        self.primary.wal_next_seq().saturating_sub(1)
    }

    /// 快照读（design 4.7 MVCC）：返回 **seq ≤ `snapshot_seq`** 的文档视图。
    /// 主数据取快照点最新版本（MemTable + SST 按 seq 过滤，Tombstone 语义保留）；
    /// **Delta 增量按全局 seq 过滤**（M7-1：跨列族共享 seq 分配，快照后的字段级热更不可见；
    /// null 删除字段 / Tombstone 均按快照点判定）。
    /// 不走 HotCache（避免快照读污染热缓存）。
    /// Ex-5.6：删除位图开启时已删文档在任何快照点均不可见（位图删除为立即/全局语义，
    /// 快照隔离仅保证更新可见性；位图置位后该 docid 视为物理删除）。
    pub fn get_at(&mut self, docid: u64, snapshot_seq: u64) -> Result<Option<Vec<u8>>> {
        if let Some(bm) = &self.deletion_bitmap {
            if bm.is_deleted(docid) {
                return Ok(None);
            }
        }
        let found = self
            .primary
            .get_bytes_at(&encode_docid(docid), snapshot_seq)?;
        let Some((bv, _)) = found else {
            return Ok(None);
        };
        let obj: serde_json::Value = match serde_json::from_slice(&bv) {
            Ok(v) => v,
            Err(_) => return Ok(Some(bv)),
        };
        let mut map = match obj {
            serde_json::Value::Object(m) => m,
            _ => return Ok(Some(bv)),
        };
        let start = encode_docid(docid).to_vec();
        let mut end = start.clone();
        end.extend_from_slice(&[0xFF; 4]);
        let rows = self
            .delta
            .scan_raw_range_with_seq(Some(&start), Some(&end))?;
        for (k, seq, v) in rows {
            if seq > snapshot_seq {
                continue; // 快照点之后的增量不可见
            }
            if !k.starts_with(&start) || k.len() < 12 {
                continue;
            }
            let field = String::from_utf8(k[12..].to_vec())
                .map_err(|_| crate::error::Error::Corrupted("Delta 字段名非法 UTF-8".into()))?;
            match v {
                Some(bytes) => {
                    let val: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
                        crate::error::Error::Corrupted(format!("Delta 值解析失败: {e}"))
                    })?;
                    if val.is_null() {
                        map.shift_remove(&field);
                    } else {
                        map.insert(field, val);
                    }
                }
                None => {
                    map.shift_remove(&field); // 增量删除字段（Tombstone）
                }
            }
        }
        let merged =
            serde_json::to_vec(&map).map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
        Ok(Some(merged))
    }

    /// 倒排词条查询：合并 posting（RoaringBitmap）→ 回表取文档。
    pub fn search_term(&mut self, term: &str) -> Result<Vec<QueryRow>> {
        Ok(self.search_term_paged(term, None, 0)?.rows)
    }

    /// 倒排词条分页查询（M8-P8）：bitmap 迭代 docid 天然升序，skip(offset) 后只回表 limit 行。
    /// `total` = 全量命中数（bitmap.len() O(1)）；limit=None 取全部（兼容非分页调用）。
    pub fn search_term_paged(
        &mut self,
        term: &str,
        limit: Option<u64>,
        offset: u64,
    ) -> Result<PagedRows> {
        // Ex-5.3：查询前刷入攒批缓冲，保证 put 后未达阈值的数据立即可查（一致性）
        self.flush_inverted_pending();
        let bitmap = self.inverted.search(term)?;
        let total = bitmap.len() as u64;
        let mut rows = Vec::new();
        let cap = limit.unwrap_or(u64::MAX);
        let mut skipped = 0u64;
        for docid in bitmap {
            if skipped < offset {
                skipped += 1;
                continue;
            }
            if rows.len() as u64 >= cap {
                break;
            }
            if let Some(v) = self.get(docid as u64)? {
                rows.push((docid as u64, v));
            }
        }
        Ok(PagedRows { total, rows })
    }

    /// fulltext 分词检索（M8-P7）：按字段 + 关键词构造词 term `ft:{field}:{word}` 查询
    /// （词 term 由 fulltext_fields 声明字段分词生成）。命中 posting 合并 → 回表取文档。
    pub fn fulltext_search(&mut self, field: &str, word: &str) -> Result<Vec<QueryRow>> {
        self.fulltext_search_paged(field, word, None, 0).map(|p| p.rows)
    }

    /// fulltext 分词检索分页（M8-P8）：同 `search_term_paged` 语义（构造 `ft:{field}:{word}`）。
    pub fn fulltext_search_paged(
        &mut self,
        field: &str,
        word: &str,
        limit: Option<u64>,
        offset: u64,
    ) -> Result<PagedRows> {
        self.search_term_paged(&format!("ft:{field}:{word}"), limit, offset)
    }

    /// fulltext 分词字段集合（M8-P7）：供 term 提取层判断字段是否走分词索引。
    pub fn fulltext_fields(&self) -> &std::collections::HashSet<String> {
        &self.fulltext_fields
    }

    /// 中文分词器开关（M8-P13）：true = jieba 完整词典分词，false = bigram。
    pub fn use_jieba(&self) -> bool {
        self.use_jieba
    }

    /// 主键范围扫描分页（M8-P8 + M8-P10 流式化）：k-way merge 流式扫描——内存 O(page)
    /// 不随扫描总量膨胀（旧实现先全量收集 O(total) 再截断）；`total` = 范围行数
    /// （全扫计数，limit 取满页后仅计数不回表，语义与全量一致）。
    pub fn scan_range_paged(
        &mut self,
        start: Option<u64>,
        end: Option<u64>,
        limit: Option<u64>,
        offset: u64,
    ) -> Result<PagedRows> {
        let cap = limit.unwrap_or(u64::MAX);
        let sk = start.map(|s| crate::keys::encode_docid(s).to_vec());
        let ek = end.map(|e| crate::keys::encode_docid(e).to_vec());
        let mut rows = Vec::new();
        let mut skipped = 0u64;
        let mut total = 0u64;
        self.primary
            .scan_stream(sk.as_deref(), ek.as_deref(), |key, val| {
                total += 1;
                if skipped < offset {
                    skipped += 1;
                    return Ok(true);
                }
                if rows.len() as u64 >= cap {
                    return Ok(true); // 页已取满：仅继续计数 total，不再收集
                }
                let docid = crate::keys::decode_docid(key).map_err(|_| {
                    crate::error::Error::Corrupted("scan 流式 key 非 docid 编码".into())
                })?;
                rows.push((docid, val.to_vec()));
                Ok(true)
            })?;
        Ok(PagedRows { total, rows })
    }

    /// scan 游标续扫（M8-P11）：从 `after`（上次最后 docid，None=从头）之后取 `limit` 条，
    /// **取满即提前终止**（不做 total 全扫）——全库遍历每页 O(limit) + 游标定位，
    /// 避免 offset 翻页的累积跳过与 total 全扫开销（7.18 已知限制）。
    /// 语义：docid 升序、不含 after 本身；配合 end 上界可限定范围。
    pub fn scan_after(
        &mut self,
        after: Option<u64>,
        end: Option<u64>,
        limit: u64,
    ) -> Result<Vec<QueryRow>> {
        let start = after.map(|a| encode_docid(a.saturating_add(1)).to_vec());
        let ek = end.map(|e| encode_docid(e).to_vec());
        let mut rows = Vec::new();
        self.primary
            .scan_stream(start.as_deref(), ek.as_deref(), |key, val| {
                if rows.len() as u64 >= limit {
                    return Ok(false); // 取满页：提前终止（不再扫后续）
                }
                let docid = crate::keys::decode_docid(key).map_err(|_| {
                    crate::error::Error::Corrupted("scan 流式 key 非 docid 编码".into())
                })?;
                rows.push((docid, val.to_vec()));
                Ok(true)
            })?;
        Ok(rows)
    }

    /// 主键范围扫描。
    pub fn scan_range(&mut self, start: Option<u64>, end: Option<u64>) -> Result<Vec<QueryRow>> {
        self.primary.scan_range(start, end)
    }

    /// 组合索引前缀查询：编码前缀键范围扫描 → 回表主数据。
    pub fn query_by_composite_prefix(&mut self, fields: &[&[u8]]) -> Result<Vec<QueryRow>> {
        let Some(cidx) = &mut self.cidx else {
            return Ok(Vec::new());
        };
        let start = crate::keys::encode_composite_key(fields, 0);
        let end = crate::keys::encode_composite_key(fields, u64::MAX);
        let hits = cidx.scan_raw_range(Some(&start), Some(&end))?;
        let mut out = Vec::new();
        for (key, _) in hits {
            let (_fields, docid) = crate::keys::decode_composite_key(&key)?;
            if let Some(v) = self.get(docid)? {
                out.push((docid, v));
            }
        }
        Ok(out)
    }

    /// 查询执行器：按 QuerySpec 静态路由到访问路径并执行（design 7.1 最小集枚举）。
    /// 看门狗：查询超时熔断（逐行检查 QueryGuard，超时返回 QueryTooExpensive）。
    pub fn execute(&mut self, spec: &QuerySpec) -> Result<Vec<QueryRow>> {
        let guard = self.watchdog.begin_query();
        // Ex-5.3：倒排查询前刷入攒批缓冲（Inverted 分支可能命中 pending 中的 term）
        self.flush_inverted_pending();
        let rows = match route(spec) {
            AccessPath::PrimaryPoint => {
                let docid = spec
                    .primary_eq
                    .as_ref()
                    .map(|k| crate::keys::decode_docid(k))
                    .transpose()?
                    .unwrap_or(0);
                self.get(docid)?.into_iter().map(|v| (docid, v)).collect()
            }
            AccessPath::PrimaryRange => self.scan_range(None, None)?,
            AccessPath::CompositeIndex { fields } => {
                let fs: Vec<&[u8]> = fields.iter().map(|s| s.as_bytes()).collect();
                self.query_by_composite_prefix(&fs)?
            }
            AccessPath::Inverted { term } => {
                // 倒排回表：逐行熔断检查
                let bitmap = self.inverted.search(&term)?;
                let mut out = Vec::new();
                for docid in bitmap {
                    if guard.is_expired() {
                        return Err(crate::error::Error::QueryTooExpensive(format!(
                            "查询超时（guard #{} > {}ms），熔断中止",
                            guard.query_id(),
                            guard.timeout().as_millis()
                        )));
                    }
                    if let Some(v) = self.get(docid as u64)? {
                        out.push((docid as u64, v));
                    }
                }
                out
            }
            AccessPath::FullScan => self.scan_range(None, None)?,
        };
        Ok(rows)
    }

    /// 倒排内存累积条数（供后台刷盘决策；含攒批缓冲，Ex-5.3）。
    pub fn inverted_mem_docids(&self) -> u64 {
        self.inverted.mem_docids() + self.pending_inverted.len() as u64
    }

    /// 将倒排攒批缓冲一次性刷入内存字典（Ex-5.3 批处理）。
    /// 低基数 term 跨行聚合：一组 (term, docid) 按 term 分组合并，每 term 一次锁操作。
    /// 崩溃安全：WAL 回放重新走 put 重建倒排，缓冲丢失不丢数据。
    fn flush_inverted_pending(&mut self) {
        if self.pending_inverted.is_empty() {
            return;
        }
        let items: Vec<(&str, u64)> = self
            .pending_inverted
            .iter()
            .map(|(t, d)| (t.as_str(), *d))
            .collect();
        self.inverted.add_batch(&items);
        self.pending_inverted.clear();
    }

    /// 强制倒排刷盘（先刷入攒批缓冲，再整段落盘）。
    pub fn flush_inverted(&mut self) -> Result<()> {
        self.flush_inverted_pending();
        self.inverted.flush_segment()
    }

    /// 倒排某词条命中的 docid 集合（不回表，供测试/监控）。
    pub fn inverted_posting(&mut self, term: &str) -> Result<RoaringBitmap> {
        self.flush_inverted_pending();
        self.inverted.search(term)
    }

    /// 倒排某词条命中的文档数（COUNT 聚合，<0.1ms）。
    /// 位图索引快速路径（design 5.2.4，M7-2）：term 命中 `bitmap_fields` 白名单 → 内存位图计数；
    /// 否则回退倒排段扫描。
    pub fn inverted_doc_count(&mut self, term: &str) -> Result<u64> {
        self.flush_inverted_pending();
        if let Some((field, value)) = term.split_once('=') {
            if let Some(n) = self.inverted.bitmap_count(field, value) {
                return Ok(n);
            }
        }
        self.inverted.doc_count(term)
    }

    /// 按字段前缀分组（GROUP BY 聚合）：返回 `field=value` 各分组的文档数。
    /// 位图索引快速路径（M7-2）：字段命中白名单 → 内存位图分组；否则回退倒排段扫描。
    pub fn inverted_group_by(&mut self, field: &str) -> Result<Vec<(String, u64)>> {
        self.flush_inverted_pending();
        if let Some(rows) = self.inverted.bitmap_group_by(field) {
            return Ok(rows);
        }
        self.inverted.group_by(field)
    }

    /// 内存位图组合 AND 计数（design 5.2.4，M7-2）：全部 term 命中白名单 → 交集计数（亚毫秒）；
    /// 否则返回 None（调用方回退逐词条倒排查询）。
    pub fn inverted_bitmap_and_count(&mut self, terms: &[&str]) -> Option<u64> {
        self.flush_inverted_pending();
        self.inverted.bitmap_and(terms).map(|b| b.len())
    }

    /// 基础 Compaction（design 4.5，阶段 3；Ex-5.4 并行化）：primary/cidx/delta 三列族压实。
    /// 并行度 `compaction_parallel`：0 = 自动（min(4, 核数/2)）；1 = 串行；>1 = 指定。
    /// 并行压实利用 SSD 并发 IO（demo 实测 3 CF 并行 2.14×）；每列族独立压实（&mut 字段拆分借用）。
    /// Ex-5.6：删除位图开启时 primary 压实按位图**物理丢弃**已删 docid 的旧数据（墓碑不污染层级）。
    pub fn compact(&mut self) -> Result<crate::column_family::CompactReport> {
        let parallel = if self.compaction_parallel == 0 {
            std::thread::available_parallelism()
                .map(|n| (n.get() / 2).clamp(1, 4))
                .unwrap_or(1)
        } else {
            self.compaction_parallel.max(1)
        };
        let cf_count = 1 + usize::from(self.cidx.is_some()) + 1; // primary + (cidx) + delta
        let threads = parallel.min(cf_count);
        // Ex-5.6/5.8：位图不可变借用（与 primary/cidx/delta 的 &mut 拆分借用互不冲突）。
        // 位图无已删 docid 时无过滤 → primary 走 Ex-5.8 无重叠块级复用压实（数据块零解压复用、
        // 只重建元数据区）；位图有已删 docid 时按位图物理丢弃（全量合并路径）。
        let bm = self.deletion_bitmap.as_ref();
        let needs_filter = bm.is_some_and(|b| b.deleted_count() > 0);
        let filter = |k: &[u8]| bm.is_some_and(|b| b.is_deleted_key(k));
        if threads <= 1 {
            // 串行（含 cidx/delta 无输入时 no-op，行为与旧版一致）
            let mut rep = if needs_filter {
                self.primary.compact_filtered(&filter)?
            } else {
                self.primary.compact()?
            };
            if let Some(c) = self.cidx.as_mut() {
                let r = c.compact()?;
                merge_report(&mut rep, &r);
            }
            let r = self.delta.compact()?;
            merge_report(&mut rep, &r);
            return Ok(rep);
        }
        // 并行：三列族独立 &mut 借用（字段拆分）→ thread::scope 并发压实，聚合返回
        let compute_cores = self.affinity.compute.clone(); // Ex-7.2：Compaction 并行线程绑 compute 核
        let (p, c, d) = (&mut self.primary, self.cidx.as_mut(), &mut self.delta);
        let merged = std::thread::scope(|s| -> Result<crate::column_family::CompactReport> {
            let h1 = if needs_filter {
                let cc = compute_cores.clone();
                s.spawn(move || {
                    crate::affinity::bind_current(&cc);
                    p.compact_filtered(&filter)
                })
            } else {
                let cc = compute_cores.clone();
                s.spawn(move || {
                    crate::affinity::bind_current(&cc);
                    p.compact()
                })
            };
            let cc = compute_cores.clone();
            let h3 = s.spawn(move || {
                crate::affinity::bind_current(&cc);
                d.compact()
            });
            let h2 = c.map(|cf| {
                let cc = compute_cores.clone();
                s.spawn(move || {
                    crate::affinity::bind_current(&cc);
                    cf.compact()
                })
            });
            let r1 = h1.join().unwrap()?;
            let r2 = match h2 {
                Some(h) => Some(h.join().unwrap()?),
                None => None,
            };
            let r3 = h3.join().unwrap()?;
            let mut merged = crate::column_family::CompactReport {
                merged_ssts: 0,
                kept_keys: 0,
                freed_bytes: 0,
                out_level: r1.out_level, // out_level 取 primary
            };
            for r in std::iter::once(&r1)
                .chain(r2.as_ref())
                .chain(std::iter::once(&r3))
            {
                merged.merged_ssts += r.merged_ssts;
                merged.kept_keys += r.kept_keys;
                merged.freed_bytes += r.freed_bytes;
            }
            Ok(merged)
        })?;
        Ok(merged)
    }

    /// 是否需要 Compaction（主数据列族 L0 段数超过阈值）。
    pub fn needs_compact(&self) -> bool {
        self.primary.needs_compact()
    }

    /// Ex-7.2：网络核列表（server 主线程绑核用）。
    pub fn network_cores(&self) -> Vec<usize> {
        self.affinity.network.clone()
    }

    // ============ Ex-1 本地消息表（Outbox）============

    /// 入队 outbox 消息（Ex-1.1）：docid + 全局 seq 幂等键，与业务写共享 fsync 点
    /// （`maybe_group_commit`）——崩溃恢复按 seq 回放，消息与业务写本地原子。
    /// 返回幂等键（docid, seq）；outbox 关闭时返回 Err(Unsupported)。
    pub fn enqueue_outbox(&mut self, docid: u64, payload: &[u8]) -> Result<(u64, u64)> {
        let Some(ob) = &mut self.outbox else {
            return Err(crate::error::Error::Unsupported(
                "outbox 未启用（config.outbox.enabled = true）".into(),
            ));
        };
        let seq = self.global_seq.fetch_add(1, Ordering::Relaxed);
        ob.enqueue(docid, seq, payload)?;
        self.maybe_group_commit()?; // 与业务写同 fsync 点（本地原子）
        Ok((docid, seq))
    }

    /// 投递器（Ex-1.2）：扫描 pending → 回调投递（true=成功）→ 标记 done。
    /// 返回投递成功数；失败留 pending（调用方退避重试）。投递成功后统一落盘
    /// （done 状态持久，防重投）。
    pub fn dispatch_outbox(&mut self, deliver: impl FnMut(&[u8], &[u8]) -> bool) -> Result<usize> {
        let n = match &mut self.outbox {
            Some(ob) => ob.dispatch(deliver)?,
            None => 0,
        };
        if n > 0 {
            self.flush_wal()?;
        }
        Ok(n)
    }

    /// 当前 pending 消息数（排空校验/监控）。
    pub fn outbox_pending(&mut self) -> Result<usize> {
        match &mut self.outbox {
            Some(ob) => ob.pending_count(),
            None => Ok(0),
        }
    }

    /// 是否已排空（Ex-1.4：扩容/切换前置条件）。
    pub fn outbox_drained(&mut self) -> Result<bool> {
        match &mut self.outbox {
            Some(ob) => ob.drained(),
            None => Ok(true),
        }
    }

    /// 引擎状态指标（design 20 / development 5.25，供 `admin status`）。
    pub fn stats(&self) -> EngineStats {
        EngineStats {
            sst_file_count: self.primary.sst_count()
                + self.delta.sst_count()
                + self.cidx.as_ref().map_or(0, |c| c.sst_count()),
            inverted_mem_docids: self.inverted.mem_docids(),
            inverted_segments: self.inverted.segment_count(),
            seq: 0, // 阶段 2 接入执行器统计
            mem_ratio: self.mem_ratio,
            max_memory_mb: self.max_memory_mb,
        }
    }

    /// 备份前一致性准备（development 5.11 冷备份第 1-2 步）：
    /// 刷 WAL → 全部 MemTable 落盘为 SST → 倒排内存字典刷盘为 `.seg` 段，
    /// 保证数据目录磁盘态自包含（含倒排段清单 Manifest、字段注册表等随目录整体打包）。
    pub fn prepare_backup(&mut self) -> Result<()> {
        self.flush_wal()?;
        if self.primary.memtable_bytes() > 0 {
            self.primary.switch_and_flush()?;
        }
        if let Some(cidx) = &mut self.cidx {
            if cidx.memtable_bytes() > 0 {
                cidx.switch_and_flush()?;
            }
        }
        self.flush_inverted()?;
        Ok(())
    }
}

/// 组提交（M8）清理：停后台兜底线程并 join，最终落盘待刷 WAL（保证正常退出不丢窗口尾部）。
impl Drop for Engine {
    fn drop(&mut self) {
        if let Some(stop) = &self.gc_stop {
            stop.store(true, Ordering::Relaxed);
        }
        if let Some(h) = self.gc_thread.take() {
            let _ = h.join();
        }
        if self.group_commit.is_some() {
            let _ = self.flush_wal();
        }
        // Ex-5.6：正常退出兜底落盘删除位图脏页（组提交关闭时 flush_wal 不执行，位图独立 flush）
        if let Some(bm) = &mut self.deletion_bitmap {
            let _ = bm.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;

    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let name = format!("eng-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    fn cfg() -> Config {
        let mut c = Config::default();
        c.sstable.compression = "none".into();
        c
    }

    fn gc_cfg(window_us: u64) -> Config {
        let mut c = cfg();
        c.storage.group_commit_us = window_us;
        c
    }

    // ---------- 组提交（M8） ----------

    #[test]
    fn group_commit_default_disabled_persists_each_put() {
        // 默认 group_commit_us=0：put 逐条 fsync，drop 后重开数据完整
        let dir = tempfile::tempdir().unwrap();
        {
            let mut e = Engine::open(dir.path(), &cfg()).unwrap();
            assert!(e.group_commit.is_none(), "默认应关闭组提交");
            for i in 0..50u64 {
                e.put(i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
            }
        }
        let mut e2 = Engine::open(dir.path(), &cfg()).unwrap();
        assert_eq!(e2.get(49).unwrap().unwrap(), b"doc-49");
    }

    // ---------- 批量导入模式（P40） ----------

    #[test]
    fn bulk_import_skips_hotcache() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        // 默认：写后回填 HotCache
        e.put_nosync(1, b"doc-1".to_vec(), &["t"]).unwrap();
        assert_eq!(e.hotcache.len(), 1, "默认写后应回填热缓存");
        // 批量导入模式：只写不读，跳过回填（P40 防缓存膨胀挤爆内存）
        e.set_bulk_import(true);
        e.put_nosync(2, b"doc-2".to_vec(), &["t"]).unwrap();
        assert_eq!(e.hotcache.len(), 1, "批量导入模式不应回填热缓存");
        // 关闭后恢复常规语义
        e.set_bulk_import(false);
        e.put_nosync(3, b"doc-3".to_vec(), &["t"]).unwrap();
        assert_eq!(e.hotcache.len(), 2, "关闭后应恢复回填");
        // 主数据不受影响：批量写入的文档仍可正常读取
        e.set_bulk_import(true);
        e.put_nosync(4, b"doc-4".to_vec(), &["t"]).unwrap();
        assert_eq!(e.hotcache.len(), 2);
        assert_eq!(e.get(4).unwrap().unwrap(), b"doc-4");
    }

    // ---------- fulltext 分词索引（M8-P7） ----------

    #[test]
    fn fulltext_search_finds_long_text_and_persists() {
        // 核心动机：>96B 长文本整串被 max_term_len 跳过，分词词 term 短 → 长文本可检索
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.inverted.fulltext_fields = vec!["big_text".into()];
        let val = serde_json::json!({"docid": 42, "status": "active", "big_text": format!("{:<300}", "rec-00000042-msg-777")});
        let ft = c.inverted.fulltext_fields.iter().cloned().collect();
        let terms =
            crate::server::extract_terms_with_fulltext(&val, None, Some(&ft));
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        {
            let mut e = Engine::open(dir.path(), &c).unwrap();
            e.put(42, serde_json::to_vec(&val).unwrap(), &t).unwrap();
            e.flush_inverted().unwrap();
            // 词 term 可检索（整串 300B > max_term_len=96 会被跳过，词 term 不受影响）
            assert_eq!(e.fulltext_search("big_text", "rec").unwrap().len(), 1);
            assert_eq!(e.fulltext_search("big_text", "777").unwrap().len(), 1);
            // 非 fulltext 字段整串 term 照常
            assert_eq!(e.inverted_doc_count("status=active").unwrap(), 1);
        }
        // 刷盘 + 重开：词 term 持久化可查（倒排段已落盘）
        let mut e2 = Engine::open(dir.path(), &c).unwrap();
        assert_eq!(e2.fulltext_search("big_text", "777").unwrap().len(), 1);
        let hits = e2.fulltext_search("big_text", "00000042").unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, 42);
    }

    #[test]
    fn cjk_fulltext_searchable_via_bigram() {
        // M8-P9：中文整串不再当单 token（无法检索）——bigram 分词后 2-4 字关键词可检索
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.inverted.fulltext_fields = vec!["content".into()];
        let mut e = Engine::open(dir.path(), &c).unwrap();
        let docs = [
            (1u64, "山水存迹数据库存储引擎"),
            (2u64, "基于Rust的LSM树文档数据库"),
            (3u64, "分布式缓存系统设计"),
        ];
        for (id, content) in docs {
            let val = serde_json::json!({"docid": id, "content": content});
            let ft = e.fulltext_fields().clone();
            let terms = crate::server::extract_terms_with_fulltext(&val, None, Some(&ft));
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put_nosync(id, serde_json::to_vec(&val).unwrap(), &t).unwrap();
        }
        e.flush_inverted().unwrap();

        // 3 字关键词"数据库" → bigram 数据/据库（AND 交集精确命中含该词的文档 1、2）
        let d1 = e.inverted_posting("ft:content:数据").unwrap();
        let d2 = e.inverted_posting("ft:content:据库").unwrap();
        let and = d1 & d2;
        assert_eq!(and.len(), 2);
        assert!(and.contains(1) && and.contains(2));
        // 4 字关键词"山水存迹" → 3 个 bigram AND → 只命中 doc 1
        let inter = e.inverted_posting("ft:content:山水").unwrap()
            & e.inverted_posting("ft:content:水存").unwrap()
            & e.inverted_posting("ft:content:存迹").unwrap();
        assert_eq!(inter.len(), 1);
        assert!(inter.contains(1));
        // fulltext_search 回表
        let hits = e.fulltext_search("content", "数据").unwrap();
        assert!(hits.iter().any(|(id, _)| *id == 1 || *id == 2));
    }

    #[test]
    fn jieba_fulltext_meaningful_word_hit() {
        // M8-P13：jieba 词典分词——"数据库"单 term 精确命中（bigram 需 数据+据库 AND）
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.inverted.fulltext_fields = vec!["content".into()];
        c.inverted.cjk_segmenter = "jieba".into();
        let mut e = Engine::open(dir.path(), &c).unwrap();
        assert!(e.use_jieba(), "cjk_segmenter=jieba 应启用 jieba 分词");
        let docs = [
            (1u64, "山水存迹数据库存储引擎"),
            (2u64, "基于Rust的LSM树文档数据库"),
            (3u64, "分布式缓存系统设计"),
        ];
        for (id, content) in docs {
            let val = serde_json::json!({"docid": id, "content": content});
            let ft = e.fulltext_fields().clone();
            let terms =
                crate::server::extract_terms_with_fulltext_seg(&val, None, Some(&ft), e.use_jieba());
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put_nosync(id, serde_json::to_vec(&val).unwrap(), &t).unwrap();
        }
        e.flush_inverted().unwrap();
        // jieba 词典词"数据库"单 term 精确命中 2 个文档
        assert_eq!(e.inverted_posting("ft:content:数据库").unwrap().len(), 2);
        let hits = e.fulltext_search("content", "数据库").unwrap();
        assert_eq!(hits.len(), 2);
        // 无该词 → 0
        assert_eq!(e.fulltext_search("content", "缓存系统").unwrap().len(), 0);
    }

    // ---------- 分页查询（M8-P8） ----------

    #[test]
    fn paged_queries_semantics_and_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg();
        c.inverted.fulltext_fields = vec!["big_text".into()];
        let mut e = Engine::open(dir.path(), &c).unwrap();
        // 100 docs：status 三态（active 34，docid 等差 3），big_text 每行唯一 token
        for i in 0..100u64 {
            let status = ["active", "inactive", "pending"][(i % 3) as usize];
            let val = serde_json::json!({
                "docid": i,
                "status": status,
                "big_text": format!("rec-{i:08}-msg-{i}"),
            });
            let ft = e.fulltext_fields().clone();
            let terms = crate::server::extract_terms_with_fulltext(&val, None, Some(&ft));
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put_nosync(i, serde_json::to_vec(&val).unwrap(), &t).unwrap();
        }
        e.flush_inverted().unwrap();

        // 全量 total + 行数
        let all = e.search_term_paged("status=active", None, 0).unwrap();
        assert_eq!(all.total, 34);
        assert_eq!(all.rows.len(), 34);
        // 分页：total 恒为全量命中数，rows 只含当前页，docid 升序接续
        let p1 = e.search_term_paged("status=active", Some(10), 0).unwrap();
        let p2 = e.search_term_paged("status=active", Some(10), 10).unwrap();
        assert_eq!(p1.total, 34);
        assert_eq!(p1.rows.len(), 10);
        assert_eq!(p2.rows.len(), 10);
        assert_eq!(p2.rows[0].0, p1.rows[9].0 + 3, "active docid 等差 3 接续");
        // 拼接 == 全量（有序稳定）
        let mut merged = p1.rows.clone();
        merged.extend(p2.rows);
        for off in [20u64, 30] {
            let p = e.search_term_paged("status=active", Some(10), off).unwrap();
            merged.extend(p.rows);
        }
        assert_eq!(merged.len(), 34);
        for (a, b) in merged.iter().zip(all.rows.iter()) {
            assert_eq!(a.0, b.0, "分页拼接必须与全量一致");
        }
        // 边界：limit=0 → 空页 total 不变；offset > total → 空页；limit > total → 全部
        let z = e.search_term_paged("status=active", Some(0), 0).unwrap();
        assert_eq!(z.total, 34);
        assert!(z.rows.is_empty());
        let o = e.search_term_paged("status=active", Some(5), 100).unwrap();
        assert!(o.rows.is_empty());
        let big = e.search_term_paged("status=active", Some(10_000), 0).unwrap();
        assert_eq!(big.rows.len(), 34);

        // fulltext 分页同语义
        let ft = e.fulltext_search_paged("big_text", "rec", Some(10), 90).unwrap();
        assert_eq!(ft.total, 100);
        assert_eq!(ft.rows.len(), 10);
        assert_eq!(ft.rows[0].0, 90);
        // scan 分页
        let sc = e.scan_range_paged(Some(10), Some(50), Some(5), 0).unwrap();
        assert_eq!(sc.total, 41);
        assert_eq!(sc.rows.len(), 5);
        assert_eq!(sc.rows[0].0, 10);
    }

    #[test]
    fn scan_after_cursor_traversal() {
        // M8-P11：游标续扫——遍历一致性 / 边界 / 提前终止（无 total 全扫）
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        for i in 1..=100u64 {
            let val = serde_json::json!({"docid": i, "v": i});
            e.put_nosync(i, serde_json::to_vec(&val).unwrap(), &[]).unwrap();
            if i % 25 == 0 {
                e.flush_primary().unwrap();
            }
        }
        e.flush_primary().unwrap();
        // 游标遍历：limit=10 逐页续扫，拼接 == 全量升序
        let mut merged: Vec<u64> = Vec::new();
        let mut after: Option<u64> = None;
        loop {
            let page: Vec<u64> = e
                .scan_after(after, None, 10)
                .unwrap()
                .into_iter()
                .map(|(d, _)| d)
                .collect();
            if page.is_empty() {
                break;
            }
            merged.extend(page.iter().copied());
            after = Some(*page.last().unwrap());
        }
        assert_eq!(merged.len(), 100);
        assert_eq!(merged, (1..=100).collect::<Vec<u64>>(), "游标遍历覆盖全部且升序");
        // 边界：after=0 → 从 1 起；after=100 → 空；尾部不足 limit
        let from0: Vec<u64> = e
            .scan_after(Some(0), None, 3)
            .unwrap()
            .into_iter()
            .map(|(d, _)| d)
            .collect();
        assert_eq!(from0, vec![1, 2, 3]);
        assert!(e.scan_after(Some(100), None, 10).unwrap().is_empty());
        assert_eq!(
            e.scan_after(Some(97), None, 10).unwrap().len(),
            3,
            "尾部不足一页取剩余"
        );
        // 上界限定：after=0 & end=50 → 1..=50
        let bounded: Vec<u64> = e
            .scan_after(Some(0), Some(50), 100)
            .unwrap()
            .into_iter()
            .map(|(d, _)| d)
            .collect();
        assert_eq!(bounded, (1..=50).collect::<Vec<u64>>());
    }

    #[test]
    fn group_commit_drop_persists_all() {
        // 开启 2ms 窗口：快速 put 全部攒批，drop 最终落盘 → 重开数据完整
        let dir = tempfile::tempdir().unwrap();
        {
            let mut e = Engine::open(dir.path(), &gc_cfg(2_000)).unwrap();
            assert!(e.group_commit.is_some());
            for i in 0..100u64 {
                e.put(i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
            }
        }
        let mut e2 = Engine::open(dir.path(), &cfg()).unwrap();
        assert_eq!(e2.get(99).unwrap().unwrap(), b"doc-99");
        assert_eq!(e2.get(0).unwrap().unwrap(), b"doc-0");
    }

    #[test]
    fn gc_thread_flushes_tail_without_new_writes() {
        // 后台兜底线程：单条写后无新写，窗口到期也应落盘（WAL 待刷缓冲清零）
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &gc_cfg(2_000)).unwrap();
        e.put(1, b"doc-1".to_vec(), &["t"]).unwrap();
        // 窗口内第 1 条通常不触发 fsync（有待刷缓冲）
        std::thread::sleep(std::time::Duration::from_millis(30));
        let pending = e.primary.wal_handle().lock().unwrap().pending_bytes()
            + e.delta.wal_handle().lock().unwrap().pending_bytes();
        assert_eq!(pending, 0, "后台线程应在窗口内兜底落盘");
    }

    #[test]
    fn backup_incremental_flushes_before_export() {
        // 组提交开启下 backup_incremental 前置 flush：未手动 flush 也应导出全部记录
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &gc_cfg(2_000)).unwrap();
        for i in 0..3u64 {
            e.put(i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
        }
        let bak = dir.path().join("incr.json");
        let rep = e.backup_incremental(0, &bak).unwrap();
        assert_eq!(rep.records, 3, "组提交开启下备份应前置落盘并导出全部");
    }

    #[test]
    fn group_commit_window_batches_fsync() {
        // 行为验证：窗口内连续 put 不逐条 fsync（WAL 待刷字节在窗口内累积，
        // 直到窗口到期或字节阈值触发一次性落盘）。写 100 条后窗口未到期时缓冲非空。
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &gc_cfg(60_000)).unwrap(); // 60ms 大窗口
        let mut synced = 0usize;
        let mut missed = 0usize;
        for i in 0..50u64 {
            e.put(i, format!("doc-{i}").into_bytes(), &["t"]).unwrap();
            let pending = e.primary.wal_handle().lock().unwrap().pending_bytes();
            if pending == 0 {
                synced += 1; // 已落盘（窗口到期/阈值触发）
            } else {
                missed += 1; // 攒批中（未 fsync）
            }
        }
        // 60ms 窗口 + 4KB 阈值：快速 50 条 put（远快于窗口）绝大部分应攒批
        assert!(missed >= 40, "窗口内应攒批（攒批 {missed}，同步 {synced}）");
    }

    // ---------- 倒排字段白名单/黑名单/长文本（M8-P4） ----------

    #[test]
    fn inverted_whitelist_only_indexes_declared_fields() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.inverted.inverted_fields = vec!["status".into(), "city".into()];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let doc = json!({"docid": 1, "status": "active", "city": "beijing", "name": "alice"});
        let terms = crate::server::extract_terms(&doc);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        e.put(1, serde_json::to_vec(&doc).unwrap(), &t).unwrap();
        e.flush_inverted().unwrap();
        // 白名单字段可查
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 1);
        assert_eq!(e.inverted_doc_count("city=beijing").unwrap(), 1);
        // 非白名单字段不建倒排
        assert_eq!(e.inverted_doc_count("name=alice").unwrap(), 0);
    }

    #[test]
    fn inverted_exclude_skips_fields() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.inverted.exclude_fields = vec!["big_text".into()];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let doc = json!({"docid": 1, "status": "active", "big_text": "hello world"});
        let terms = crate::server::extract_terms(&doc);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        e.put(1, serde_json::to_vec(&doc).unwrap(), &t).unwrap();
        e.flush_inverted().unwrap();
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 1);
        assert_eq!(
            e.inverted_doc_count("big_text=hello world").unwrap(),
            0,
            "黑名单字段不建倒排"
        );
    }

    #[test]
    fn inverted_max_term_len_skips_long_text() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.inverted.max_term_len = 16; // 16 字节以上 term 自动跳过（长文本整串保护）
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let long = "x".repeat(100);
        let doc = json!({"docid": 1, "status": "active", "payload": long});
        let terms = crate::server::extract_terms(&doc);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        e.put(1, serde_json::to_vec(&doc).unwrap(), &t).unwrap();
        e.flush_inverted().unwrap();
        assert_eq!(
            e.inverted_doc_count("status=active").unwrap(),
            1,
            "短字段仍建"
        );
        assert_eq!(
            e.inverted_doc_count("payload=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx").unwrap(),
            0,
            "超长 term 自动跳过（防长文本膨胀）"
        );
    }

    #[test]
    fn inverted_default_all_fields_built() {
        // 默认配置（白名单空）：短字符串字段全建；仅超长 term（>96）自动跳过
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &cfg()).unwrap();
        let doc = json!({"docid": 1, "status": "active", "city": "beijing"});
        let terms = crate::server::extract_terms(&doc);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        e.put(1, serde_json::to_vec(&doc).unwrap(), &t).unwrap();
        e.flush_inverted().unwrap();
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 1);
        assert_eq!(e.inverted_doc_count("city=beijing").unwrap(), 1);
    }

    #[test]
    fn put_get_roundtrip() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"doc-1".to_vec(), &["rust"]).unwrap();
        e.put(2, b"doc-2".to_vec(), &["go"]).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"doc-1");
        assert_eq!(e.get(2).unwrap().unwrap(), b"doc-2");
        assert!(e.get(99).unwrap().is_none());
    }

    #[test]
    fn inverted_batch_pending_flush() {
        // Ex-5.3：put 攒批缓冲——未达阈值时 term 留在 pending，查询自动刷入（一致性）；
        // inverted_mem_docids 统计含 pending；flush_inverted 后落盘、统计归零。
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        for i in 0..100u64 {
            e.put(i, b"v".to_vec(), &["status=active"]).unwrap();
        }
        // 未达阈值（8192）→ pending 未刷入，但统计应含 pending
        assert_eq!(e.inverted_mem_docids(), 100, "统计应含攒批缓冲");
        // 查询自动刷入 → 立即可见
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 100);
        // 查询后 pending 已清空
        assert_eq!(e.pending_inverted.len(), 0, "查询后缓冲应清空");
        // flush_inverted 落盘：内存归零、计数仍正确
        e.flush_inverted().unwrap();
        assert_eq!(e.inverted_mem_docids(), 0, "落盘后内存统计归零");
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 100);
    }

    #[test]
    fn delete_hides_doc() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(7, b"x".to_vec(), &["k"]).unwrap();
        e.delete(7).unwrap();
        assert!(e.get(7).unwrap().is_none());
    }

    #[test]
    fn search_term_returns_docs() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"about rust".to_vec(), &["rust"]).unwrap();
        e.put(2, b"rust rocks".to_vec(), &["rust"]).unwrap();
        e.put(3, b"go is cool".to_vec(), &["go"]).unwrap();
        let rows = e.search_term("rust").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[1].0, 2);
        assert!(e.search_term("go").unwrap().len() == 1);
        assert!(e.search_term("absent").unwrap().is_empty());
    }

    #[test]
    fn deleted_doc_excluded_from_search() {
        // 倒排残留 docid 回表时被主数据 Tombstone 过滤
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"rust".to_vec(), &["rust"]).unwrap();
        e.put(2, b"rust2".to_vec(), &["rust"]).unwrap();
        e.delete(1).unwrap();
        let rows = e.search_term("rust").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 2);
    }

    // ---------- MVCC 快照读（design 4.7 二期，M6-3） ----------

    #[test]
    fn get_at_reads_historical_version_after_flush() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        let put_doc = |e: &mut Engine, v: i64| {
            e.put(
                1,
                serde_json::to_vec(&json!({"docid": 1, "v": v})).unwrap(),
                &["v"],
            )
            .unwrap();
        };
        put_doc(&mut e, 1);
        let s1 = e.begin_snapshot();
        e.flush_primary().unwrap(); // v1 落 SST（历史版本保留）
        put_doc(&mut e, 2);
        // 快照读 → v1；最新读 → v2
        let snap: serde_json::Value =
            serde_json::from_slice(&e.get_at(1, s1).unwrap().unwrap()).unwrap();
        assert_eq!(snap["v"], 1, "快照应读回 v1");
        let cur: serde_json::Value = serde_json::from_slice(&e.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(cur["v"], 2);
    }

    #[test]
    fn get_at_ignores_writes_after_snapshot() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, br#"{"k":"base"}"#.to_vec(), &["k"]).unwrap();
        e.flush_primary().unwrap(); // base 落 SST，快照后可回读
        let s = e.begin_snapshot();
        // 快照之后主数据覆盖 + Delta 热更
        e.put(1, br#"{"k":"later"}"#.to_vec(), &["k"]).unwrap();
        e.patch(1, &[("extra", json!("x"))]).unwrap();
        // 快照读：主数据仍为 base，Delta 增量也隔离（M7-1 全局 seq）
        let snap: serde_json::Value =
            serde_json::from_slice(&e.get_at(1, s).unwrap().unwrap()).unwrap();
        assert_eq!(snap["k"], "base", "快照应隔离主数据后续覆盖");
        assert!(snap.get("extra").is_none(), "快照应隔离快照后的 Delta 热更");
        // 最新读：later + delta 叠加
        let cur: serde_json::Value = serde_json::from_slice(&e.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(cur["k"], "later");
        assert_eq!(cur["extra"], "x");
    }

    #[test]
    fn get_at_delta_isolated_by_global_seq() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, br#"{"a":1,"b":1}"#.to_vec(), &["k"]).unwrap();
        let s = e.begin_snapshot();
        // 快照后 Delta 修改字段 a（null 删除 b）
        e.patch(1, &[("a", json!(2)), ("b", json!(null))]).unwrap();
        // 快照读：a=1、b 仍存在（快照后增量不可见）
        let snap: serde_json::Value =
            serde_json::from_slice(&e.get_at(1, s).unwrap().unwrap()).unwrap();
        assert_eq!(snap["a"], 1, "快照应读回 a=1");
        assert_eq!(snap["b"], 1, "快照应保留被删除的 b");
        // 最新读：a=2、b 被 null 删除
        let cur: serde_json::Value = serde_json::from_slice(&e.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(cur["a"], 2);
        assert!(cur.get("b").is_none(), "最新读 b 应被 null 删除");
    }

    #[test]
    fn global_seq_resumes_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = crate::config::Config::default();
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        e.put(1, br#"{"a":1}"#.to_vec(), &["k"]).unwrap();
        let s = e.begin_snapshot();
        e.patch(1, &[("a", json!(2))]).unwrap();
        // 重启：全局 seq 从 WAL 恢复
        drop(e);
        let mut e2 = Engine::open(dir.path(), &cfg).unwrap();
        // 快照点之前的读不受影响（重启后快照序号语义保持）
        let snap: serde_json::Value =
            serde_json::from_slice(&e2.get_at(1, s).unwrap().unwrap()).unwrap();
        assert_eq!(snap["a"], 1);
        let cur: serde_json::Value = serde_json::from_slice(&e2.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(cur["a"], 2);
        assert!(e2.begin_snapshot() >= s, "重启后全局 seq 应接续");
    }

    #[test]
    fn get_at_returns_none_after_delete_before_snapshot() {
        // Ex-5.6 语义拆分：
        // ① 删除位图开启（默认）：删除为立即/全局语义——已删 docid 在任何快照点均不可见
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"v1".to_vec(), &["k"]).unwrap();
        let s_before_delete = e.begin_snapshot();
        e.flush_primary().unwrap();
        e.delete(1).unwrap(); // 位图置位（快照点前后均不可见）
        assert!(e.get_at(1, s_before_delete).unwrap().is_none(), "位图删除对旧快照同样隐藏");
        assert!(e.get(1).unwrap().is_none());

        // ② 删除位图关闭：回退传统 Tombstone + MVCC 语义——删除前快照仍可见 v1
        let mut c = cfg();
        c.storage.deletion_bitmap_enabled = false;
        let mut e2 = Engine::open(&tmp(), &c).unwrap();
        e2.put(1, b"v1".to_vec(), &["k"]).unwrap();
        let s2 = e2.begin_snapshot();
        e2.flush_primary().unwrap();
        e2.delete(1).unwrap(); // Tombstone seq > s2
        assert_eq!(e2.get_at(1, s2).unwrap().unwrap(), b"v1", "关闭位图保留 Tombstone 快照语义");
        assert!(e2.get(1).unwrap().is_none(), "删除后最新读 → 不存在");
    }

    // ---------- 增量备份（design 20，M6-5） ----------

    #[test]
    fn incremental_backup_restore_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &crate::config::Config::default()).unwrap();
        let put_doc = |e: &mut Engine, docid: u64, v: &str| {
            let val = json!({"docid": docid, "v": v});
            let bytes = serde_json::to_vec(&val).unwrap();
            let terms = crate::server::extract_terms(&val);
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put(docid, bytes, &t).unwrap();
        };
        put_doc(&mut e, 1, "v1");
        put_doc(&mut e, 2, "v1");
        let full_point = e.current_seq(); // 全量备份点
        put_doc(&mut e, 3, "v3"); // 增量 1
        e.delete(1).unwrap(); // 增量 2（Tombstone）
        let bak = dir.path().join("incr.json");
        let rep = e.backup_incremental(full_point, &bak).unwrap();
        assert_eq!(rep.since_seq, full_point);
        assert_eq!(rep.records, 2, "应导出 2 条增量记录");

        // 模拟"全量还原"后的新引擎：先恢复全量点数据，再应用增量
        let dir2 = tempfile::tempdir().unwrap();
        let mut e2 = Engine::open(dir2.path(), &crate::config::Config::default()).unwrap();
        put_doc(&mut e2, 1, "v1");
        put_doc(&mut e2, 2, "v1");
        let n = e2.restore_incremental(&bak).unwrap();
        assert_eq!(n, 2);
        assert!(e2.get(1).unwrap().is_none(), "增量 Tombstone 应删除 doc1");
        assert!(e2.get(2).unwrap().is_some(), "doc2 保留（全量部分）");
        let v3: serde_json::Value = serde_json::from_slice(&e2.get(3).unwrap().unwrap()).unwrap();
        assert_eq!(v3["v"], "v3", "增量 PUT 应恢复 doc3");
    }

    #[test]
    fn incremental_backup_since_zero_exports_all() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &crate::config::Config::default()).unwrap();
        e.put(1, b"a".to_vec(), &["a"]).unwrap();
        e.flush_primary().unwrap(); // flush → WAL 截断（M8-P5）：已刷盘记录 1 从 WAL 删除
        e.put(2, b"b".to_vec(), &["b"]).unwrap();
        let bak = dir.path().join("incr-all.json");
        let rep = e.backup_incremental(0, &bak).unwrap();
        assert_eq!(
            rep.records, 1,
            "since=0 导出当前 WAL 全部记录（截断后 = 未刷盘记录 2；记录 1 已入 SST 由全量备份覆盖）"
        );
    }

    // ---------- 位图索引（design 5.2.4，M7-2） ----------

    #[test]
    fn bitmap_index_fast_path_for_count_group_and() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::default();
        cfg.inverted.bitmap_fields = vec!["status".into(), "city".into()];
        let mut e = Engine::open(dir.path(), &cfg).unwrap();
        let put_doc = |e: &mut Engine, docid: u64, status: &str, city: &str| {
            let val = json!({"docid": docid, "status": status, "city": city});
            let bytes = serde_json::to_vec(&val).unwrap();
            let terms = crate::server::extract_terms(&val);
            let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            e.put(docid, bytes, &t).unwrap();
        };
        put_doc(&mut e, 1, "active", "beijing");
        put_doc(&mut e, 2, "inactive", "beijing");
        put_doc(&mut e, 3, "active", "shanghai");
        // COUNT 快速路径（内存位图）
        assert_eq!(e.inverted_doc_count("status=active").unwrap(), 2);
        assert_eq!(e.inverted_doc_count("city=beijing").unwrap(), 2);
        // AND 交集快速路径
        assert_eq!(
            e.inverted_bitmap_and_count(&["status=active", "city=beijing"])
                .unwrap(),
            1
        );
        // GROUP BY 快速路径
        let g = e.inverted_group_by("status").unwrap();
        assert!(g.contains(&("active".to_string(), 2)));
        assert!(g.contains(&("inactive".to_string(), 1)));
        // 重启后位图从段重建，COUNT 仍正确（drop 前刷盘倒排，保证段自包含）
        e.flush_inverted().unwrap();
        drop(e);
        let mut e2 = Engine::open(dir.path(), &cfg).unwrap();
        assert_eq!(e2.inverted_doc_count("status=active").unwrap(), 2);
    }

    #[test]
    fn execute_routes_by_spec() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(42, b"hello world".to_vec(), &["hello"]).unwrap();

        // 主键点查
        let spec = QuerySpec {
            primary_eq: Some(crate::keys::encode_docid(42).to_vec()),
            primary_range: false,
            index_prefix: vec![],
            term: None,
        };
        let rows = e.execute(&spec).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 42);

        // 倒排词条查询
        let spec2 = QuerySpec {
            primary_eq: None,
            primary_range: false,
            index_prefix: vec![],
            term: Some("hello".into()),
        };
        let rows2 = e.execute(&spec2).unwrap();
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0].1, b"hello world");
    }

    #[test]
    fn composite_prefix_query() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        // 组合索引：写入索引条目（key = composite(fields, docid)）
        if let Some(cidx) = &mut e.cidx {
            for docid in [10u64, 20, 30] {
                let key = crate::keys::encode_composite_key(&[b"active"], docid);
                cidx.put_bytes(key, docid.to_le_bytes().to_vec()).unwrap();
            }
            let key = crate::keys::encode_composite_key(&[b"inactive"], 99);
            cidx.put_bytes(key, 99u64.to_le_bytes().to_vec()).unwrap();
        }
        // 主数据写文档（回表需要）
        e.put(10, b"d10".to_vec(), &[]).unwrap();
        e.put(20, b"d20".to_vec(), &[]).unwrap();
        e.put(30, b"d30".to_vec(), &[]).unwrap();
        e.put(99, b"d99".to_vec(), &[]).unwrap();

        let rows = e.query_by_composite_prefix(&[b"active"]).unwrap();
        let mut ids: Vec<u64> = rows.iter().map(|(d, _)| *d).collect();
        ids.sort();
        assert_eq!(ids, vec![10, 20, 30]);
    }

    #[test]
    fn oom_guardian_blocks_writes_at_stall() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.set_mem_ratio(0.5);
        e.put(1, b"ok".to_vec(), &[]).unwrap(); // 正常写入

        e.set_mem_ratio(1.0); // 模拟 RSS 打满
        let err = e.put(2, b"blocked".to_vec(), &[]).unwrap_err();
        assert!(matches!(err, crate::error::Error::MemoryOverload(_)));
        // 被拒写入不生效
        assert!(e.get(2).unwrap().is_none());
        assert!(e.get(1).unwrap().is_some());
    }

    #[test]
    fn patch_merge_on_read() {
        // 阶段 1.5 Delta CF：patch 部分更新 → get 合并覆盖；重启后 WAL 恢复仍生效
        let dir = tmp();
        let mut e = Engine::open(&dir, &cfg()).unwrap();
        e.put(
            1,
            br#"{"status":"active","amount":100,"device":"android"}"#.to_vec(),
            &[],
        )
        .unwrap();
        e.patch(
            1,
            &[
                ("status", serde_json::json!("inactive")),
                ("note", serde_json::json!("updated")),
                ("amount", serde_json::Value::Null), // null = 删除字段
            ],
        )
        .unwrap();
        let v = e.get(1).unwrap().unwrap();
        let obj: serde_json::Value = serde_json::from_slice(&v).unwrap();
        assert_eq!(obj["status"], serde_json::json!("inactive"), "patch 应覆盖");
        assert_eq!(obj["note"], serde_json::json!("updated"), "patch 应新增");
        assert!(obj.get("amount").is_none(), "null patch 应删除字段");
        assert_eq!(
            obj["device"],
            serde_json::json!("android"),
            "未 patch 字段保留"
        );
        // 重启后 Delta 仍生效（WAL 恢复）
        drop(e);
        let mut e2 = Engine::open(&dir, &cfg()).unwrap();
        let obj2: serde_json::Value = serde_json::from_slice(&e2.get(1).unwrap().unwrap()).unwrap();
        assert_eq!(obj2["status"], serde_json::json!("inactive"));
        assert_eq!(obj2["note"], serde_json::json!("updated"));
    }

    #[test]
    fn full_put_clears_delta() {
        // 全量 put 覆盖 → 清空该 docid 增量，避免旧 patch 覆盖新数据
        let dir = tmp();
        let mut e = Engine::open(&dir, &cfg()).unwrap();
        e.put(1, br#"{"status":"active","amount":100}"#.to_vec(), &[])
            .unwrap();
        e.patch(1, &[("status", serde_json::json!("patched"))])
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&e.get(1).unwrap().unwrap()),
            r#"{"status":"patched","amount":100}"#
        );
        e.put(1, br#"{"status":"fresh","amount":200}"#.to_vec(), &[])
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&e.get(1).unwrap().unwrap()),
            r#"{"status":"fresh","amount":200}"#
        );
    }

    #[test]
    fn delete_clears_delta() {
        // 删除文档 → Delta 清空，避免复活
        let dir = tmp();
        let mut e = Engine::open(&dir, &cfg()).unwrap();
        e.put(1, br#"{"status":"active"}"#.to_vec(), &[]).unwrap();
        e.patch(1, &[("note", serde_json::json!("x"))]).unwrap();
        e.delete(1).unwrap();
        assert!(e.get(1).unwrap().is_none(), "删除后 Delta 不应复活文档");
    }

    // ---------- Ex-5.6 删除位图 ----------

    #[test]
    fn multi_ssd_striping_places_files() {
        // Ex-5.10 多 SSD 条带化：wal_dir/sst_dir/inverted_dir 分盘——WAL/SST/倒排文件
        // 落位正确 + 数据跨重启恢复（默认单盘布局由既有测试覆盖）
        let data_dir = tempfile::tempdir().unwrap();
        let wal_dir = tempfile::tempdir().unwrap();
        let sst_dir = tempfile::tempdir().unwrap();
        let inv_dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg();
        cfg.storage.wal_dir = Some(wal_dir.path().to_string_lossy().to_string());
        cfg.storage.sst_dir = Some(sst_dir.path().to_string_lossy().to_string());
        cfg.storage.inverted_dir = Some(inv_dir.path().to_string_lossy().to_string());
        {
            let mut e = Engine::open(data_dir.path(), &cfg).unwrap();
            e.put(1, b"doc1".to_vec(), &["rust"]).unwrap();
            e.put(2, b"doc2".to_vec(), &["go"]).unwrap();
            e.flush_primary().unwrap();
            e.flush_inverted().unwrap();
            assert_eq!(e.get(1).unwrap().unwrap(), b"doc1");
        }
        // 落位验证：WAL / SST / 倒排各在其盘
        assert!(
            wal_dir.path().join("primary").join("wal.log").exists(),
            "primary WAL 应落在 wal_dir"
        );
        let sst_files: Vec<_> = std::fs::read_dir(sst_dir.path().join("primary"))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(!sst_files.is_empty(), "SST 应落在 sst_dir");
        assert!(
            inv_dir.path().join("inverted").join("inverted-manifest.json").exists(),
            "倒排应在 inverted_dir"
        );
        assert!(!data_dir.path().join("primary").exists(), "列族目录已外移 sst_dir");
        // 跨重启恢复（多盘布局持久）
        let mut e2 = Engine::open(data_dir.path(), &cfg).unwrap();
        assert_eq!(e2.get(1).unwrap().unwrap(), b"doc1");
        assert_eq!(e2.get(2).unwrap().unwrap(), b"doc2");
        assert!(e2.search_term("rust").unwrap().len() == 1);
    }

    #[test]
    fn dynamic_io_rate_backs_off_with_write_pressure() {
        // Ex-7.4：MemTable 水位（写压力代理）升高 → Compaction 限速下调让路
        // （压力 p → base×(1-0.5p)，最低 50% 基准）
        let mut c = cfg();
        c.storage.io_rate_limit_mb = 100; // 100MB/s 基准
        c.memtable.max_size_mb = 1; // 1MB 小 MemTable → 快速产生写压力
        let mut e = Engine::open(&tmp(), &c).unwrap();
        let base = 100u64 * 1024 * 1024;
        assert_eq!(e.primary.io_rate(), base, "空 MemTable 压力 0 全速");
        // 写入 8 个 64KB 文档 ≈ 512KB（memtable 50% 水位）
        let big = vec![b'x'; 64 * 1024];
        for i in 0..8u64 {
            e.put(i, big.clone(), &[]).unwrap();
        }
        let rate = e.primary.io_rate();
        assert!(rate < base, "写压力下限速应下调: {rate} < {base}");
        assert!(rate >= base / 2, "不低于 50% 基准: {rate}");
        // flush 后水位回落 → 限速回升
        e.flush_primary().unwrap();
        let rate2 = e.primary.io_rate();
        assert!(rate2 >= rate, "flush 后水位降、限速回升: {rate2} >= {rate}");
    }

    #[test]
    fn outbox_e2e_enqueue_dispatch_drain() {
        // Ex-1 端到端：enqueue（与业务写同 seq 空间）→ 重启保留 → 幂等投递 → 排空
        let mut c = cfg();
        c.outbox.enabled = true;
        let dir = tmp();
        let mut consumer = crate::outbox::IdempotentConsumer::new();
        {
            let mut e = Engine::open(&dir, &c).unwrap();
            e.put(1, b"doc1".to_vec(), &["k"]).unwrap();
            e.enqueue_outbox(1, b"msg-1").unwrap();
            e.enqueue_outbox(2, b"msg-2").unwrap();
            e.flush_wal().unwrap();
            assert_eq!(e.outbox_pending().unwrap(), 2);
            // 幂等投递
            let n = e
                .dispatch_outbox(|k, p| {
                    assert!(p.starts_with(b"msg-"));
                    consumer.apply(k)
                })
                .unwrap();
            assert_eq!(n, 2);
            assert!(e.outbox_drained().unwrap(), "投递后排空");
        }
        // 重启：pending 保持 0（done 状态持久）
        let mut e2 = Engine::open(&dir, &c).unwrap();
        assert!(e2.outbox_drained().unwrap());
        assert_eq!(consumer.received(), 2);
    }

    #[test]
    fn outbox_disabled_by_default() {
        // outbox 默认关闭（零开销）：enqueue 返回 Unsupported、pending=0
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        assert!(e.enqueue_outbox(1, b"x").is_err(), "未启用应拒绝入队");
        assert_eq!(e.outbox_pending().unwrap(), 0);
        assert!(e.outbox_drained().unwrap());
    }

    #[test]
    fn outbox_pending_survives_restart() {
        // 崩溃恢复：enqueue 未投递 → 重开 pending 保留（WAL 回放重建）
        let mut c = cfg();
        c.outbox.enabled = true;
        let dir = tmp();
        {
            let mut e = Engine::open(&dir, &c).unwrap();
            e.enqueue_outbox(7, b"keep").unwrap();
            e.flush_wal().unwrap();
        }
        let mut e2 = Engine::open(&dir, &c).unwrap();
        assert_eq!(e2.outbox_pending().unwrap(), 1, "重开 pending 保留");
    }

    #[test]
    fn deletion_bitmap_persists_across_restart() {
        // 删除 → flush_wal（位图落盘）→ 重启 → 位图文件加载，已删 docid 仍不可见
        let dir = tmp();
        let cfg = cfg();
        let bitmap_path = dir.join("deletion.bitmap");
        {
            let mut e = Engine::open(&dir, &cfg).unwrap();
            e.put(1, b"v1".to_vec(), &["k"]).unwrap();
            e.put(2, b"v2".to_vec(), &["k"]).unwrap();
            e.flush_wal().unwrap();
            e.delete(1).unwrap();
            e.flush_wal().unwrap(); // 位图脏页落盘
        }
        let meta = std::fs::metadata(&bitmap_path).unwrap();
        assert_eq!(meta.len() % 4096, 0, "位图文件 4KB 页对齐");
        let mut e2 = Engine::open(&dir, &cfg).unwrap();
        assert!(e2.get(1).unwrap().is_none(), "重启后位图加载，删除持久");
        assert!(e2.get(2).unwrap().is_some(), "未删文档不受影响");
    }

    #[test]
    fn deletion_bitmap_put_resurrects() {
        // delete → put 复活：put 清位，文档重新可见
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.put(1, b"v1".to_vec(), &["k"]).unwrap();
        e.delete(1).unwrap();
        assert!(e.get(1).unwrap().is_none());
        e.put(1, b"v2".to_vec(), &["k"]).unwrap();
        assert_eq!(e.get(1).unwrap().unwrap(), b"v2", "put 复活后可见新值");
    }

    #[test]
    fn deletion_bitmap_compaction_drops_deleted_data() {
        // 位图开启：delete 不写 LSM 墓碑 → compaction 按位图物理丢弃已删 docid 旧数据；
        // 重启后位图 + 压实结果均一致（已删不可见、存活可见）
        let dir = tmp();
        let cfg = cfg();
        {
            let mut e = Engine::open(&dir, &cfg).unwrap();
            for d in 1..=4u64 {
                e.put(d, format!("doc{d}").into_bytes(), &["k"]).unwrap();
            }
            e.flush_primary().unwrap(); // L0 第 1 段
            e.put(5, b"doc5".to_vec(), &["k"]).unwrap();
            e.flush_primary().unwrap(); // L0 第 2 段 → 触发 compaction 条件
            e.delete(2).unwrap();
            e.delete(4).unwrap();
            let rep = e.compact().unwrap();
            assert!(rep.merged_ssts >= 2, "L0 压实应合并多段");
            assert!(e.get(2).unwrap().is_none());
            assert!(e.get(4).unwrap().is_none());
            assert!(e.get(1).unwrap().is_some());
            assert!(e.get(5).unwrap().is_some());
        }
        let mut e2 = Engine::open(&dir, &cfg).unwrap();
        for d in 1..=5u64 {
            let deleted = matches!(d, 2 | 4);
            assert_eq!(e2.get(d).unwrap().is_none(), deleted, "重启后 docid {d} 状态一致");
        }
    }

    #[test]
    fn deletion_bitmap_disabled_uses_tombstone_path() {
        // 位图关闭：delete 走传统 Tombstone（primary.delete + 逐条 fsync），get 仍返回 None
        let mut c = cfg();
        c.storage.deletion_bitmap_enabled = false;
        let dir = tmp();
        {
            let mut e = Engine::open(&dir, &c).unwrap();
            e.put(1, b"v1".to_vec(), &["k"]).unwrap();
            e.delete(1).unwrap();
            assert!(e.get(1).unwrap().is_none());
        }
        assert!(
            !dir.join("deletion.bitmap").exists(),
            "位图关闭时不应生成位图文件"
        );
        let mut e2 = Engine::open(&dir, &c).unwrap();
        assert!(e2.get(1).unwrap().is_none(), "Tombstone 路径跨重启删除一致");
    }

    #[test]
    fn deletion_bitmap_incremental_backup_captures_delete() {
        // 位图开启：delete 写 primary WAL 删除记录 → 增量备份导出含删除 → 恢复后删除保持
        let dir = tmp();
        let cfg = cfg();
        let mut e = Engine::open(&dir, &cfg).unwrap();
        let put_doc = |e: &mut Engine, docid: u64, v: &str| {
            e.put(docid, v.as_bytes().to_vec(), &["k"]).unwrap();
        };
        put_doc(&mut e, 1, "v1");
        put_doc(&mut e, 2, "v2");
        let since = e.current_seq();
        e.delete(1).unwrap();
        let bak_dir = tempfile::tempdir().unwrap();
        let bak = bak_dir.path().join("incr.json");
        let rep = e.backup_incremental(since, &bak).unwrap();
        assert!(rep.records >= 1, "增量备份应包含删除记录");
        drop(e);

        // 恢复到全新引擎（位图开启）：删除记录回放 → 重新置位 → doc 1 不可见
        let dir2 = tmp();
        let mut e2 = Engine::open(&dir2, &cfg).unwrap();
        put_doc(&mut e2, 1, "v1");
        put_doc(&mut e2, 2, "v2");
        let n = e2.restore_incremental(&bak).unwrap();
        assert!(n >= 1);
        assert!(e2.get(1).unwrap().is_none(), "增量恢复后删除保持");
        assert!(e2.get(2).unwrap().is_some());
    }

    #[test]
    fn oom_guardian_throttles_in_soft_range() {
        let mut e = Engine::open(&tmp(), &cfg()).unwrap();
        e.set_mem_ratio(0.9); // 软水位区间
        e.put(1, b"throttled-but-allowed".to_vec(), &[]).unwrap();
        assert!(e.get(1).unwrap().is_some());
        // 限流计数已记录
        assert!(e.watchdog.memory().throttled_count() >= 1);
    }

    #[test]
    fn restart_preserves_data_and_inverted() {
        // 崩溃恢复全链路：写入（未强制刷盘）→ 进程退出 → 重开 → 主数据与倒排均完好
        let dir = tmp();
        let cfg = cfg();
        {
            let mut e = Engine::open(&dir, &cfg).unwrap();
            e.put(1, b"doc-1".to_vec(), &["rust"]).unwrap();
            e.put(2, b"doc-2".to_vec(), &["go"]).unwrap();
            e.put(3, b"doc-3".to_vec(), &["rust", "async"]).unwrap();
            e.flush_inverted().unwrap();
        } // 模拟进程退出
        {
            let mut e2 = Engine::open(&dir, &cfg).unwrap();
            // 主数据（WAL 回放恢复）
            assert_eq!(e2.get(1).unwrap().unwrap(), b"doc-1");
            assert_eq!(e2.get(2).unwrap().unwrap(), b"doc-2");
            assert_eq!(e2.get(3).unwrap().unwrap(), b"doc-3");
            // 倒排跨重启可查（段文件 + Manifest）
            let rows = e2.search_term("rust").unwrap();
            let mut ids: Vec<u64> = rows.iter().map(|(d, _)| *d).collect();
            ids.sort();
            assert_eq!(ids, vec![1, 3]);
            assert_eq!(e2.search_term("go").unwrap().len(), 1);
        }
    }
}
