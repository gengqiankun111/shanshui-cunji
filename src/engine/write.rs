//! 文档引擎写入族（reconstruct.md engine/write.rs）：put / put_nosync / delete /
//! delete_batch / patch / flush_wal / flush_primary、组提交提交协调（maybe_group_commit /
//! commit_persist）、整库 purge（purge_all）、outbox 消息表写入族与倒排词条过滤
//! （engine_doc_stats / inverted_allowed / inverted_count_eligible）。
//! 内容拆分自原 engine.rs（写入主题 impl 块）；私有 Engine 字段以 `pub(crate)` 提升访问。

use std::sync::atomic::Ordering;

use roaring::treemap::RoaringTreemap;
use tracing::info;

use crate::engine::Engine;
use crate::error::Result;
use crate::keys::{encode_docid, encode_varlen};

/// Task-026：per-CPU 写入口包裹——分配 gseq → push TLS scope（CF external 写收集条目）→
/// 执行主体 → pop → 整组路由入队（≤ 窗口由队列消费线程落盘；失败路径已收集条目不丢——
/// 与既有"WAL 先于 memtable"的崩溃语义一致）。未启用（`percpu=None`）：零开销直通。
macro_rules! percpu_write {
    ($self:expr, $body:expr) => {{
        if $self.percpu.is_some() {
            let gseq = $self.global_seq.fetch_add(1, Ordering::Relaxed);
            crate::engine::percpu_wal::push_wal_scope(gseq);
            let __res = $body;
            let __scope = crate::engine::percpu_wal::pop_wal_scope();
            $self.enqueue_scope(__scope)?;
            return __res;
        }
        $body
    }};
}
use crate::engine::percpu_wal::{PerCpuWal, WalScope};


/// Ex-9.3 第①步：解析文档 JSON 中声明 stats 字段的数值（与 `stats_fields` 对齐；
/// 缺字段 / JSON null / 非数值 → None 跳过；文档不可解析 → 全 None）。
fn engine_doc_stats(fields: &[String], value: &[u8]) -> Vec<Option<f64>> {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(value) else {
        return vec![None; fields.len()];
    };
    fields.iter().map(|f| v.get(f).and_then(|x| x.as_f64())).collect()
}

/// 倒排攒批缓冲阈值（条，Ex-5.3）：达此值强制 `flush_inverted_pending`。
const INVERTED_PENDING_CAP: usize = 8192;

impl Engine {
    /// 提交写批次 scope（`percpu_write!` 宏在主体执行后调用）：按当前 CPU 路由整组入队。
    /// scope 为空（无 CF 条目 / 未启用时 None）→ no-op。
    fn enqueue_scope(&self, scope: Option<WalScope>) -> Result<()> {
        let Some(scope) = scope else { return Ok(()) };
        if scope.entries.is_empty() {
            return Ok(());
        }
        if let Some(rt) = &self.percpu {
            let q = self.per_cpu_wal.route(PerCpuWal::current_cpu());
            rt.submit(q, scope.entries)?;
        }
        Ok(())
    }

    /// 组提交判定（M8）：关闭 → 逐条 fsync（现状强安全）；
    /// 开启 → 写路径零 fsync，由后台提交线程按窗口统一落盘（ack 后最多延迟 ≤ 窗口，
    /// 字节阈值触发也由后台线程判定）——避免写路径与后台线程双份 fsync + 锁竞争。
    /// Task-026：per-CPU 启用时恒 no-op（每队列消费线程按窗口落盘，组提交线程停用）。
    fn maybe_group_commit(&mut self) -> Result<()> {
        if self.percpu.is_some() {
            return Ok(());
        }
        if self.group_commit.is_none() {
            self.flush_wal()?;
        }
        Ok(())
    }

    /// P2-A（development_remain P2-A ②）：事务 COMMIT 的落盘语义按
    /// `storage.flush_log_at_trx_commit` 档位执行（对齐 MySQL `innodb_flush_log_at_trx_commit`）：
    /// - **1**（默认）：每次 COMMIT 显式 `flush_wal`（位图 + WAL + outbox 全 fsync，强安全；
    ///   COMMIT ack = 已落盘）。
    /// - **0 / 2**：COMMIT 不单独 fsync——落盘交给组提交窗口（与单条 put 同一攒批路径，
    ///   并发 COMMIT 共享窗口内一次 fsync；ack 后最多延迟 ≤ 窗口落盘）。组提交关闭（无后台
    ///   落盘线程）时 `maybe_group_commit` 回退 `flush_wal`（强安全兜底）。
    pub(crate) fn commit_persist(&mut self) -> Result<()> {
        if self.flush_log_at_trx_commit == 1 {
            self.flush_wal()
        } else {
            self.maybe_group_commit()
        }
    }

    /// 写入文档（docid + 序列化字节 + 该文档涉及的倒排词条）。
    /// 写失效链：先失效 HotCache 与组合索引旧条目，最后写 LSM（design 6.6）。
    /// OOM Guardian：写入前按水位限流/熔断（design 14.1.1）。
    /// X 项：写操作计数 + 延迟直方图。
    pub fn put(&mut self, docid: u64, value: Vec<u8>, terms: &[&str]) -> Result<()> {
        let t = std::time::Instant::now();
        // 看门狗统一检查（P52）：内存硬水位熔断 + 磁盘剩余空间熔断；软水位放行记录
        self.watchdog.check_all(self.mem_ratio, &self.data_dir)?;
        self.put_nosync(docid, value, terms)?;
        // 组提交（M8）：开启时窗口内攒批一次 fsync，否则逐条 fsync（强安全）
        self.maybe_group_commit()?;
        // Ex-7.4：按前台写压力动态调整 Compaction 限速（MemTable 水位让路）
        self.adjust_compaction_io_rate();
        self.metrics.write_ops.fetch_add(1, Ordering::Relaxed);
        self.metrics.record_latency(t.elapsed().as_nanos() as u64);
        Ok(())
    }

    /// Ex-7.4：动态限流——按主数据 MemTable 水位（前台写压力代理）下调 Compaction 限速：
    /// 压力 p → 限速 = base × (1 - 0.5p)——压力 0 全速追赶 L0 合并，压力 1 让路 50%
    /// 磁盘带宽给前台写（design_extension 12.6：写压力高时压缩 Compaction 带宽）。
    /// L 项：同源压力同步给各列族 set_write_pressure（动态 L0 阈值反馈），独立于限速配置。
    fn adjust_compaction_io_rate(&mut self) {
        let used = self.primary.memtable_bytes() as f64;
        let max = self.memtable_max_bytes.max(1) as f64;
        let pressure = (used / max).clamp(0.0, 1.0);
        // L 项：写压力 → 各列族动态 L0 阈值（高峰收窄提前收敛）
        self.primary.set_write_pressure(pressure);
        self.delta.set_write_pressure(pressure);
        if let Some(c) = &self.cidx {
            c.set_write_pressure(pressure);
        }
        if self.io_rate_base_bytes == 0 {
            return; // 未配置限速（io_rate_limit_mb = 0）
        }
        let rate = (self.io_rate_base_bytes as f64 * (1.0 - 0.5 * pressure)) as u64;
        self.primary.set_io_rate_bytes(rate);
        self.delta.set_io_rate_bytes(rate);
        if let Some(c) = &self.cidx {
            c.set_io_rate_bytes(rate);
        }
        // Ex-8.13：倒排 GC/后台段写与列族压缩同口径收窄（共享后台 IO 预算）
        self.inverted.set_io_rate_bytes(rate);
    }

    /// 批量写入（不逐条 fsync，供亿级压测；结束时调用 `flush_wal` 统一提交）。
    /// Task-026：per-CPU 启用时以单 gseq 组包裹（跨 CF 条目同组 → 崩溃回放原子）。
    pub fn put_nosync(&mut self, docid: u64, value: Vec<u8>, terms: &[&str]) -> Result<()> {
        percpu_write!(self, self.put_nosync_inner(docid, value, terms))
    }

    /// put_nosync 主体（语义不变，被 per-CPU 写批次 scope 包裹）。
    fn put_nosync_inner(&mut self, docid: u64, value: Vec<u8>, terms: &[&str]) -> Result<()> {
        // P94：colstore 派生后对旧行写 → 记脏（命中脏区间整查询回退行式）
        self.colstore_note_write(docid);
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
        //     Ex-8.7：实际清位（此前已删）→ 减除删除密度净置位数；fetch_max 维护密度分母。
        self.max_docid.fetch_max(docid, Ordering::Relaxed);
        // P1-C：活跃 docid 增量记账（新 docid / 已删复活 → 插入 +1；覆盖既有不变）
        self.live_add(docid);
        if let Some(bm) = &self.deletion_bitmap {
            if bm.clear(docid) {
                self.garbage_marked.fetch_sub(1, Ordering::Relaxed);
            }
        }
        // ③ 倒排（内存字典累积，Ex-5.3 攒批：term 先入缓冲，达阈值/查询/flush 时批量刷入）；
        //    M8-P4：白名单/黑名单/超长 term 过滤（长文本整串不进字典，防膨胀）
        //    Ex-9.3 第①步：配置 stats_fields 时解析本文档数值并随 allowed term 累积
        let stats = if self.stats_fields.is_empty() {
            Vec::new()
        } else {
            engine_doc_stats(&self.stats_fields, &value)
        };
        for t in terms {
            if self.inverted_allowed(t) {
                self.pending_inverted.lock().unwrap().push((t.to_string(), docid));
                if !stats.is_empty() {
                    self.inverted.add_stats(t, &stats);
                }
            }
        }
        if self.pending_inverted.lock().unwrap().len() >= INVERTED_PENDING_CAP {
            self.flush_inverted_pending();
        }
        // P0-A：声明式组合索引写路径——提取 JSON 字段值写入 cidx 列族。
        // key = encode_composite_key(field_values, docid)，前缀扫描命中后回表主数据。
        if let Some(cidx) = &self.cidx {
            if !self.composite_indexes.is_empty() {
                if let Ok(vobj) = serde_json::from_slice::<serde_json::Value>(&value) {
                    for fields in &self.composite_indexes {
                        let mut field_vals: Vec<Vec<u8>> = Vec::with_capacity(fields.len());
                        let mut all_present = true;
                        for f in fields {
                            match vobj.get(f) {
                                Some(serde_json::Value::String(s)) => field_vals.push(s.as_bytes().to_vec()),
                                Some(serde_json::Value::Number(n)) => field_vals.push(n.to_string().into_bytes()),
                                Some(serde_json::Value::Bool(b)) => field_vals.push(if *b { b"true".to_vec() } else { b"false".to_vec() }),
                                _ => { all_present = false; break; }
                            }
                        }
                        if all_present {
                            let key = crate::keys::encode_composite_key(
                                &field_vals.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
                                docid,
                            );
                            let _ = cidx.put_bytes_nosync(key, Vec::new());
                        }
                    }
                }
            }
        }
        // ④ 回填 HotCache（写后回填，供热点查询亚毫秒命中；批量导入模式跳过，P40）
        if !self.skip_hotcache {
            self.hotcache.put(docid, value);
        }
        // P 项：事件驱动自动 Compaction——写后检查（Flush 可能刚新增 L0 段），
        // L0 段数/大小超阈值 → 同步合并收敛（写入自然退避 = 背压）。
        self.auto_compact()?;
        Ok(())
    }


    /// 倒排 term 过滤（M8-P4）：白名单（只建声明字段）→ 黑名单（排除字段）→ 超长 term 自动跳过。
    /// term 编码 `field=value`，field 为 JSON 字段路径（嵌套用 `.` 连接）。
    /// fulltext 词 term（`ft:{field}:{token}`）与 inverted_fields 白名单正交：是否建索引
    /// 由 fulltext_fields 声明决定（白名单非空时 ft: term 不被滤掉，否则无法分词检索）。
    fn inverted_allowed(&self, term: &str) -> bool {
        // 超长 term（长文本整串）自动跳过：防止误配下字典膨胀
        if self.max_term_len > 0 && term.len() > self.max_term_len {
            return false;
        }
        if let Some(rest) = term.strip_prefix("ft:") {
            let field = rest.split(':').next().unwrap_or("");
            return self.fulltext_fields.contains(field);
        }
        let field = term.split('=').next().unwrap_or("");
        if let Some(include) = &self.inverted_include {
            return include.contains(field);
        }
        !self.inverted_exclude.contains(field)
    }

    /// Ex-9.1：mysql `COUNT WHERE f='v'` 快路径可路由判定——字段须已建索引且计数是亚毫秒级：
    /// ① `bitmap_fields` 内存位图（写路径同步维护，O(1) 精确）或 ② 倒排白名单字段（回退精确
    /// 去重 doc_count——大 term 非亚毫秒，故建议 COUNT 高频字段配 bitmap_fields）。未索引字段
    /// 不得路由（防 `doc_count` 把"未建索引"误报成 0）。
    pub fn inverted_count_eligible(&self, field: &str) -> bool {
        !field.is_empty()
            && (self.inverted_allowed(&format!("{field}=x"))
                || self.inverted.is_bitmap_field(field))
    }

    /// 统一提交 WAL（批量写入结束后调用，保证崩溃可恢复）。
    /// Ex-5.6：删除位图脏页**先于** WAL fsync 落盘——若崩溃发生在 WAL fsync 之后、
    /// 环形 WAL 截断推进之前，位图已持久（删除不丢）；反之位图先持久、WAL 回放重删幂等。
    pub fn flush_wal(&mut self) -> Result<()> {
        if let Some(bm) = &self.deletion_bitmap {
            bm.flush()?;
        }
        // Task-026：per-CPU 运行时——同步排空全部队列 + fsync + checkpoint 持久化/裁剪
        // （各 CF 自身 sync_wal 在 external 模式为 no-op；强安全语义 = 队列全量落盘）
        if let Some(rt) = &self.percpu {
            return rt.flush_all();
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

    /// Task-026：per-CPU WAL 健康度快照（SHOW STATUS 数据源：队列深度/消费/checkpoint）。
    pub fn percpu_status(&self) -> String {
        if let Some(rt) = &self.percpu {
            format!("{} | {}", self.per_cpu_wal.status(), rt.status())
        } else {
            self.per_cpu_wal.status()
        }
    }

    /// DROP TABLE / TRUNCATE（文档库唯一表统一映射 documents）purge：清空引擎全部数据并对齐
    /// MySQL 整表删除语义——主数据/组合索引/Delta 三列族（MemTable+SST+WAL）、倒排（内存+段）、
    /// 删除位图、HotCache、倒排攒批缓冲与全局 seq/删除密度状态全部归零；数据目录清空
    /// （此后重启打开为空库，反复 --init / cleanup 基线可比）。
    /// 须在引擎写锁（`&mut self`）内调用：与 flush/写路径互斥；后台 compact / inverted gc 并发
    /// 安全（列族 `sst_mutate` / 倒排 `mutate` 互斥）。outbox 业务消息表不受影响（独立于表数据）。
    pub fn purge_all(&mut self) -> Result<()> {
        info!("引擎整库 purge 开始（DROP TABLE / TRUNCATE TABLE）");
        self.primary.purge_data()?;
        if let Some(c) = &self.cidx {
            c.purge_data()?;
        }
        self.delta.purge_data()?;
        self.inverted.purge_all()?;
        self.hotcache.clear();
        if let Some(bm) = &self.deletion_bitmap {
            bm.purge();
        }
        // P1-C：purge 后活跃集复位空（后续 put 从 0 增量）
        *self.live_docids.lock().unwrap() = Some(RoaringTreemap::new());
        self.pending_inverted.lock().unwrap().clear();
        self.global_seq.store(0, Ordering::Relaxed);
        self.max_docid.store(0, Ordering::Relaxed);
        self.max_docid_loaded.store(false, Ordering::Relaxed);
        self.garbage_marked.store(0, Ordering::Relaxed);
        self.garbage_done.store(0, Ordering::Relaxed);
        self.garbage_draining.store(false, Ordering::Relaxed);
        self.compact_pending.store(false, Ordering::Relaxed);
        self.inverted_gc_pending.store(false, Ordering::Relaxed);
        // Task-026：per-CPU 队列运行时全清零（队列/文件/checkpoint 水位归零，重启空库可比）
        if let Some(rt) = &self.percpu {
            rt.reset_all()?;
        }
        info!("引擎整库 purge 完成");
        Ok(())
    }

    /// 删除文档：失效 HotCache + 清空 Delta（倒排残留 docid 由回表过滤）。
    /// Ex-5.6：删除位图开启时写 1bit（O(1) 最新态隐藏 + compaction 物理回收）+ **memtable
    /// Tombstone（版本化，缺陷 B/C4 修复：复活清位后快照读仍能判定删除区间）** + WAL 删除记录
    /// （供增量备份/崩溃回放）。墓碑进入版本链，快照读按 seq 过滤（快照点在删除与复活之间
    /// → 不可见），与 MySQL RR 一致。关闭位图时回退传统 Tombstone 路径。
    /// Task-026：per-CPU 启用时以单 gseq 组包裹（跨 CF 条目同组 → 崩溃回放原子）。
    pub fn delete(&mut self, docid: u64) -> Result<()> {
        percpu_write!(self, self.delete_inner(docid))
    }

    /// delete 主体（语义不变，被 per-CPU 写批次 scope 包裹）。
    fn delete_inner(&mut self, docid: u64) -> Result<()> {
        // P94：colstore 派生后删除旧行 → 记脏（删除位图路径本会跳过，但 tombstone 路径需回退行式）
        self.colstore_note_write(docid);
        self.watchdog.check_all(self.mem_ratio, &self.data_dir)?;
        self.hotcache.invalidate(docid);
        match &self.deletion_bitmap {
            Some(bm) => {
                // Ex-8.7：**新置位**（此前未删）才计入删除密度净置位数——
                // WAL 回放/重复删除幂等（位已置 → 不计，与 bm_deleted 初始化口径一致）。
                if bm.mark_deleted(docid) {
                    self.garbage_marked.fetch_add(1, Ordering::Relaxed);
                }
                self.primary
                    .delete_record_mem(encode_docid(docid).to_vec())?;
                self.delta.delete_prefix(&encode_docid(docid))?;
            }
            None => {
                self.primary.delete(docid)?;
                self.delta.delete_prefix(&encode_docid(docid))?;
            }
        }
        // P1-C：活跃集剔除（删除位图 / Tombstone 双路径；删不存在为幂等 no-op）
        self.live_remove(docid);
        Ok(())
    }

    /// delete_range50 修复（性能项①）：**批量删除原语**——流式消费 docid 迭代器，逐 docid
    /// 执行与 [`delete`](Self::delete) 完全一致的语义（HotCache 失效 + 删除位图置位 +
    /// 版本化 memtable Tombstone + WAL 删除记录 + Delta 前缀清理；幂等），但**不逐行 fsync**：
    /// 墓碑统一走 `delete_record_mem`（WAL 攒批，批尾一次 `maybe_group_commit` 提交——
    /// 组提交关闭时回退单次 `flush_wal`），watchdog 批头检查一次 + 每 4096 行巡检。
    /// 收益：范围删 50 行从「50 次独立 fsync + 50 次 watchdog/热缓存/Delta 开销」降到
    /// 「1 次提交 + 摊销巡检」；语义与逐行 `delete` 完全一致（含删除密度计数、复活清位、快照版本判定）。
    /// Task-026：per-CPU 启用时整批单 gseq 组（批量原子：崩溃回放整组或跳过，无中间态）。
    pub fn delete_batch<I: Iterator<Item = u64>>(&mut self, docids: I) -> Result<u64> {
        // P122：per-CPU WAL（默认开启）下，若单 gseq scope 条目 > queue depth，入队背压等待与
        // 持引擎写锁的调用方互锁 → 服务整体锁死（见 problem_solving P122）。此处按队列深度预算
        // **内部拆子批**（每个子批独立 scope 入队），任何调用方（SQL DELETE / 写定位 / CLI 等）都安全；
        // 语义与一次性整批完全一致（幂等、计数逐批累加）。未启用 per-CPU 时维持原单 scope 路径。
        if self.percpu.is_some() {
            let budget = self.per_cpu_wal.scope_doc_budget();
            if budget > 1 {
                let mut n = 0u64;
                let mut buf: Vec<u64> = Vec::with_capacity(budget);
                for d in docids {
                    buf.push(d);
                    if buf.len() == budget {
                        n += self.delete_batch_scope(buf.drain(..))?;
                    }
                }
                if !buf.is_empty() {
                    n += self.delete_batch_scope(buf.into_iter())?;
                }
                return Ok(n);
            }
        }
        self.delete_batch_scope(docids)
    }

    /// 单个 per-CPU scope 的批量删除（内部辅助；未启用 per-CPU 时等同原 delete_batch 单 scope）。
    fn delete_batch_scope<I: Iterator<Item = u64>>(&mut self, docids: I) -> Result<u64> {
        percpu_write!(self, self.delete_batch_inner(docids))
    }

    /// delete_batch 主体（语义不变，被 per-CPU 写批次 scope 包裹）。
    fn delete_batch_inner<I: Iterator<Item = u64>>(&mut self, docids: I) -> Result<u64> {
        self.watchdog.check_all(self.mem_ratio, &self.data_dir)?;
        let mut n = 0u64;
        for docid in docids {
            self.hotcache.invalidate(docid);
            self.colstore_note_write(docid); // P94：批量删同样记脏
            match &self.deletion_bitmap {
                Some(bm) => {
                    // Ex-8.7：新置位才计入删除密度净置位数（重复删除幂等）
                    if bm.mark_deleted(docid) {
                        self.garbage_marked.fetch_add(1, Ordering::Relaxed);
                    }
                    self.primary
                        .delete_record_mem(encode_docid(docid).to_vec())?;
                    self.delta.delete_prefix(&encode_docid(docid))?;
                }
                None => {
                    // 位图关闭（传统 Tombstone 路径）：墓碑进版本链（快照语义同 delete），
                    // 但批尾统一 sync 替代逐行 sync_wal（delete_range50 提速关键）
                    self.primary
                        .delete_record_mem(encode_docid(docid).to_vec())?;
                    self.delta.delete_prefix(&encode_docid(docid))?;
                }
            }
            // P1-C：活跃集剔除（幂等；delete_batch 与 delete 同语义）
            self.live_remove(docid);
            n += 1;
            // 摊销看门狗：每 4096 行巡检（防超长批量撞硬水位/磁盘熔断）
            if n % 4096 == 0 {
                self.watchdog.check_all(self.mem_ratio, &self.data_dir)?;
            }
        }
        // 持久性对齐逐行 delete：
        // - 位图路径（Some）：Engine::delete 本身不主动 flush（删除位图内存即时隐藏，
        //   落盘由组提交/后续 flush 负责）→ 镜像语义，批尾不提交（避免触发 bm.flush 落盘）。
        // - Tombstone 路径（None）：Engine::delete 走 delete_bytes 逐行 sync_wal（强安全），
        //   此处批尾**单次** sync 主/delta WAL（墓碑 + 删除记录已入 WAL），50 行从 50 次 fsync → 1 次。
        if self.deletion_bitmap.is_none() {
            self.primary.sync_wal()?;
            self.delta.sync_wal()?;
        }
        Ok(n)
    }

    /// M3（§26 多表，实施清单④）：DROP TABLE 磁盘文件级回收——物理删除主列族内
    /// **完全落在指定表 docid 区间**的 SST（表切分后每文件单表，可整文件删）。
    /// 须在 `multitable::drop_table_range`（逐 docid 逻辑删除：墓碑已覆盖全部该表键）之后调用，
    /// 此时删文件不改变可见性（墓碑保证无复活），仅提前释放磁盘。
    pub fn drop_table_sst_files(&self, tid: u16) -> crate::error::Result<usize> {
        self.primary.drop_table_range_files(tid)
    }

    /// 部分更新（阶段 1.5，design 4.7）：仅写入变更字段到 Delta CF（几十字节小记录），
    /// 读取时 Merge-on-Read 覆盖 Base；`null` 值表示删除该字段。替代全量 PUT，写入 IO 放大趋近 1。
    /// Task-026：per-CPU 启用时整次 patch 单 gseq 组。
    /// P131（2026-09-06）：组提交时机对齐 `put`（nosync 入组 → 组提交窗口统一 fsync）。
    pub fn patch(&mut self, docid: u64, fields: &[(&str, serde_json::Value)]) -> Result<()> {
        self.patch_nosync(docid, fields)?;
        self.maybe_group_commit()
    }

    /// patch_nosync：不入组提交的字段增量（批量调用方攒批后自行 flush_wal，见 patch_batch）。
    /// Task-026：per-CPU 启用时以单 gseq 组包裹（跨 CF 条目同组 → 崩溃回放原子）。
    fn patch_nosync(&mut self, docid: u64, fields: &[(&str, serde_json::Value)]) -> Result<()> {
        percpu_write!(self, self.patch_nosync_inner(docid, fields))
    }

    /// patch_nosync 主体（语义不变，被 per-CPU 写批次 scope 包裹；不触发组提交）。
    fn patch_nosync_inner(
        &mut self,
        docid: u64,
        fields: &[(&str, serde_json::Value)],
    ) -> Result<()> {
        self.hotcache.invalidate(docid);
        for (f, v) in fields {
            let mut key = encode_docid(docid).to_vec();
            encode_varlen(&mut key, f.as_bytes());
            let val =
                serde_json::to_vec(v).map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
            self.delta.put_bytes_nosync(key, val)?;
        }
        Ok(())
    }

    /// P131（2026-09-06）：批量写/批量增量的统一提交语义——按事务落盘档位：
    /// 档位 1 = 显式 `flush_wal`（强安全批边界，语义不变）；档位 0/2 = 走组提交窗口
    /// （`maybe_group_commit`：无后台窗口时兜底 flush_wal，有则 ack 后 ≤窗口落盘）。
    /// 此前批量路径恒 `flush_wal` → UPDATE 每语句对 primary/delta WAL 各做一次同步 fsync
    /// （单连接下组提交窗口空转，fsync 常数不可摊薄），成为 UPDATE 相对 INSERT/MySQL
    /// （innodb_flush_log_at_trx_commit=2 不逐提交 fsync）的 p50 差主源之一。
    pub(crate) fn commit_batch(&mut self) -> Result<()> {
        if self.flush_log_at_trx_commit == 1 {
            self.flush_wal()
        } else {
            self.maybe_group_commit()
        }
    }

    /// P131（2026-09-06）：批量字段增量（原子批次，语义对齐 `put_batch`）——多个 docid 的
    /// 字段 patch 攒批写入，批尾按 `commit_batch` 统一提交（档位 1 = 单次 flush_wal；
    /// 档位 0/2 = 组提交窗口；per-CPU 下整批经 nosync 单 gseq 组 + 批尾 drain）。
    /// SQL 单列 UPDATE（非索引列）落此路径。
    pub fn patch_batch(
        &mut self,
        items: &[(u64, Vec<(String, serde_json::Value)>)],
    ) -> Result<()> {
        self.watchdog.check_all(self.mem_ratio, &self.data_dir)?;
        for (docid, fields) in items {
            let refs: Vec<(&str, serde_json::Value)> = fields
                .iter()
                .map(|(f, v)| (f.as_str(), v.clone()))
                .collect();
            self.patch_nosync(*docid, &refs)?;
        }
        self.commit_batch()
    }

    /// 入队 outbox 消息（Ex-1.1）：docid + 全局 seq 幂等键，与业务写共享 fsync 点
    /// （`maybe_group_commit`）——崩溃恢复按 seq 回放，消息与业务写本地原子。
    /// 返回幂等键（docid, seq）；outbox 关闭时返回 Err(Unsupported)。
    /// Task-026：per-CPU 启用时以单 gseq 组包裹（outbox 行随队列窗口落盘）。
    pub fn enqueue_outbox(&mut self, docid: u64, payload: &[u8]) -> Result<(u64, u64)> {
        percpu_write!(self, self.enqueue_outbox_inner(docid, payload))
    }

    /// enqueue_outbox 主体（语义不变，被 per-CPU 写批次 scope 包裹）。
    fn enqueue_outbox_inner(&mut self, docid: u64, payload: &[u8]) -> Result<(u64, u64)> {
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
    /// （done 状态持久，防重投）。Task-026：整批单 gseq 组 + 批尾 flush_wal（drain 队列）。
    pub fn dispatch_outbox(&mut self, deliver: impl FnMut(&[u8], &[u8]) -> bool) -> Result<usize> {
        percpu_write!(self, self.dispatch_outbox_inner(deliver))
    }

    /// dispatch_outbox 主体（语义不变，被 per-CPU 写批次 scope 包裹）。
    fn dispatch_outbox_inner(
        &mut self,
        mut deliver: impl FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<usize> {
        let n = match &mut self.outbox {
            Some(ob) => ob.dispatch(&mut deliver)?,
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

}
