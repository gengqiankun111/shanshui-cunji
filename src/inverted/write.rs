//! write：内存写入（add / add_stats / add_batch）、阈值判断、flush_segment 整段刷盘 + FST、purge_all、IO 记账。
//! 重构自 src/inverted.rs 对应主题，行为零变化。

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use crate::error::Result;
use crate::keys::encode_varlen;
use mmap_file::MmapFile;
use tracing::info;

use super::segment::{encode_posting_v6, SEG_MAGIC, SEG_PREFIX, SEG_VERSION};
use super::{bitmap_shard, encode_varint, FieldAgg, InvertedIndex, Posting};


impl InvertedIndex {
    /// 追加一个 (term, docid) 到内存字典（docid 为引擎 64 位 docid，含多表高位）。
    pub fn add(&self, term: &str, docid: u64) {
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
                        .insert(docid);
                }
            }
        }
        self.mem.entry(term.to_string()).or_default().push(docid);
        self.mem_docids.add(1);
    }

    /// Ex-9.3 第①步：随 term 累积一个文档的数值贡献。`stats` 与配置 `stats_fields` 对齐，
    /// `None` = 该文档缺字段/非数值（跳过，不计入 n）。统计仅进内存 `stats_mem`（段格式
    /// v5 载荷为第②步）；term 集合与 mem posting 一致（Engine 在 allowed 过滤后同批调用）。
    pub fn add_stats(&self, term: &str, stats: &[Option<f64>]) {
        if stats.is_empty() {
            return;
        }
        let mut agg = self.stats_mem.entry(term.to_string()).or_default();
        if agg.len() < stats.len() {
            agg.resize(stats.len(), FieldAgg::new());
        }
        for (a, s) in agg.iter_mut().zip(stats.iter()) {
            if let Some(x) = s {
                a.acc(*x);
            }
        }
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
                    .extend(docids.iter().copied());
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

    /// Ex-8.13：挂载后台 IO 预算（0 = 关闭）。GC/后台段写 acquire 节流；紧急刷段仅记账。
    pub fn attach_io_budget(&self, bytes_per_sec: u64) {
        *self.io_limiter.lock().unwrap() = if bytes_per_sec > 0 {
            Some(crate::io_scheduler::IoRateLimiter::new(bytes_per_sec))
        } else {
            None
        };
    }

    /// Ex-7.4/Ex-8.13：动态调整倒排后台 IO 预算（前台写压力驱动收窄，与列族压缩同口径）。
    pub fn set_io_rate_bytes(&self, bytes_per_sec: u64) {
        if let Some(l) = self.io_limiter.lock().unwrap().as_mut() {
            l.set_rate(bytes_per_sec);
        }
    }

    /// Ex-8.13：倒排累计写盘字节（写放大 / IO 审计数据源）。
    pub fn inverted_written_bytes(&self) -> u64 {
        self.inverted_written.load(Ordering::Relaxed)
    }

    /// 新写 seg 文件记账（GC 与前台刷段均累计写盘字节）。
    fn account_written(&self, path: &Path) {
        let sz = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        self.inverted_written.fetch_add(sz, Ordering::Relaxed);
    }

    /// GC/后台段写：记账 + 共享预算 acquire 节流（预算不足等待；与 CF `io_acquire` 同语义）。
    pub(super) fn account_written_budgeted(&self, path: &Path) -> Result<()> {
        let sz = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        self.inverted_written.fetch_add(sz, Ordering::Relaxed);
        if let Some(l) = self.io_limiter.lock().unwrap().as_mut() {
            l.acquire(sz)?;
        }
        Ok(())
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
            // v6：64 位 posting（RoaringTreemap 序列化；多表 docid 高位直存）
            let bitmap: Posting = docids.iter().copied().collect();
            // Ex-9.1b（v4）：条目 = term + varint(段内 doc_count) + posting —— 计数载荷
            // 供 COUNT 亚毫秒求和（段内 posting 为去重 docid 集合 → bitmap.len() 精确）。
            let bytes = encode_posting_v6(&bitmap);
            encode_varlen(&mut body, term.as_bytes());
            encode_varint(&mut body, bitmap.len() as u64);
            // Ex-9.3（v5）：条目追加统计载荷 = varint(fcount) + fcount × (n u64 + sum/min/max f64 定长)，
            // 取自写路径随 term 累积的 stats_mem（engine stats_fields 对齐；未配置/无贡献 → fcount 0）。
            let stats: Vec<FieldAgg> = self
                .stats_mem
                .get(&term)
                .map(|e| e.value().clone())
                .unwrap_or_default();
            encode_varint(&mut body, stats.len() as u64);
            for a in &stats {
                body.extend_from_slice(&a.n.to_le_bytes());
                body.extend_from_slice(&a.sum.to_le_bytes());
                body.extend_from_slice(&a.min.to_le_bytes());
                body.extend_from_slice(&a.max.to_le_bytes());
            }
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

        // Ex-8.13：新段写盘记账（前台紧急刷段仅记账不等待预算）
        self.account_written(&path);

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
        self.stats_mem.clear(); // 统计已随段落盘（v5 载荷），避免下次 flush 重复累积
        self.mem_docids.reset();
        info!("倒排刷盘完成: {fname}");
        // P137：段落盘次数 counter（专项监控）
        self.seg_flush_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // P4-B：刷盘后检查最新 delta 段 FST 是否超限，超限触发后台 GC 合并进 base
        if self.should_delta_gc() {
            info!("delta FST 达到上限 {} MB，触发自动合并进 base", self.delta_fst_max_bytes / (1024 * 1024));
            // 后台合并已经持有 mutate 锁（当前我们已经持有），直接 gc
            let report = self.gc()?;
            info!("delta FST 自动合并完成: 合并 {} 段，释放 {} 字节", report.merged, report.freed_bytes);
        }

        Ok(())
    }

    /// DROP TABLE purge：清空倒排全部状态（内存字典/位图/posting 缓存 + 磁盘段/FST + Manifest）。
    /// 与 flush_segment/gc 经 `mutate` 互斥（后台 GC 并发安全）；先换空快照再删段文件
    /// （Windows 已 mmap 文件不可删——删除失败忽略 → 孤儿段不被空 Manifest 加载，重启安全）。
    pub fn purge_all(&self) -> Result<()> {
        let _g = self.mutate.lock().unwrap();
        let old: Vec<String> = self.segments.load().as_ref().clone();
        self.mem.clear();
        self.stats_mem.clear();
        self.mem_docids.reset();
        self.segments.store(Arc::new(Vec::new()));
        self.dicts.store(Arc::new(HashMap::new()));
        self.data_files.store(Arc::new(HashMap::new()));
        for b in &self.bitmaps {
            b.lock().unwrap().clear();
        }
        self.posting_cache.lock().unwrap().clear();
        self.next_seg_id.store(1, Ordering::Relaxed);
        self.persist_manifest()?;
        for seg in &old {
            let _ = std::fs::remove_file(self.dir.join(seg));
            let _ = std::fs::remove_file(self.dir.join(seg.replace(".seg", ".fst")));
        }
        Ok(())
    }
}
