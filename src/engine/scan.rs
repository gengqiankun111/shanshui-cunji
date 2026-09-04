//! 文档引擎扫描族（reconstruct.md engine/scan.rs）：主键范围/流式/keys-only/分页/游标
//! 扫描、自动水位（auto_watermark）、活跃 docid 集维护（live_ensure/live_add/live_remove）
//! 与 COUNT(*)（count_all_docs）。
//! 内容拆分自原 engine.rs（扫描主题 impl 块）；私有 Engine 字段以 `pub(crate)` 提升访问。

use std::sync::atomic::Ordering;

use roaring::treemap::RoaringTreemap;

use crate::engine::{Engine, PagedRows, QueryRow};
use crate::error::Result;
use crate::keys::{decode_docid, encode_docid};


impl Engine {
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
    pub fn scan_range(&self, start: Option<u64>, end: Option<u64>) -> Result<Vec<QueryRow>> {
        let mut rows = self.primary.scan_range(start, end)?;
        // Ex-8.1：删除位图语义对齐（get 不可见 → scan 也不返回已删 docid）
        if let Some(bm) = &self.deletion_bitmap {
            rows.retain(|(d, _)| !bm.is_deleted(*d));
        }
        Ok(rows)
    }

    /// 流式主键范围扫描（design 20.5 导出管道）：回调按 docid 升序收到 `(docid, value)`；
    /// 返回 `false` 提前终止（取满批/游标续扫）。内存 O(批)，不随扫描总量膨胀。
    pub fn scan_stream<F: FnMut(u64, &[u8]) -> Result<bool>>(
        &self,
        start: Option<u64>,
        end: Option<u64>,
        mut f: F,
    ) -> Result<()> {
        let sk = start.map(|s| encode_docid(s).to_vec());
        let ek = end.map(|e| encode_docid(e).to_vec());
        self.primary.scan_stream(sk.as_deref(), ek.as_deref(), |key, val| {
            let docid = decode_docid(key).map_err(|_| {
                crate::error::Error::Corrupted("scan 流式 key 非 docid 编码".into())
            })?;
            // Ex-8.1：删除位图语义对齐（与 get/scan_range 一致，已删 docid 跳过）
            if let Some(bm) = &self.deletion_bitmap {
                if bm.is_deleted(docid) {
                    return Ok(true);
                }
            }
            f(docid, val)
        })
    }

    /// P91：投影列流式扫描（最新视图）——语义同 `scan_stream`，但 SST 端按 `fields`
    /// 投影解码（PAX 块只解所需列 → 子集 JSON；行式/内存直通原 JSON 字节）。
    /// 消费端只读 `fields` 覆盖列（须含 WHERE 引用 + 分组 + 聚合字段全集）。
    pub fn scan_stream_fields<F: FnMut(u64, &[u8]) -> Result<bool>>(
        &self,
        start: Option<u64>,
        end: Option<u64>,
        fields: Vec<String>,
        mut f: F,
    ) -> Result<()> {
        let sk = start.map(|s| encode_docid(s).to_vec());
        let ek = end.map(|e| encode_docid(e).to_vec());
        self.primary
            .scan_stream_fields(sk.as_deref(), ek.as_deref(), fields, |key, val| {
                let docid = decode_docid(key).map_err(|_| {
                    crate::error::Error::Corrupted("scan fields key 非 docid 编码".into())
                })?;
                if let Some(bm) = &self.deletion_bitmap {
                    if bm.is_deleted(docid) {
                        return Ok(true);
                    }
                }
                f(docid, val)
            })
    }

    /// P1-E：带 Zone Map 字段级范围剪枝的流式扫描——与 `scan_stream` 语义一致，
    /// 但额外在 SST 块级检查 `zone_pred` 的 min/max，不相交块跳过（免 IO/解压）。
    /// 适用于 SQL 范围查询（`ts BETWEEN`、`amount > N`）的扫描下推路径。
    pub fn scan_stream_with_zonepred<F: FnMut(u64, &[u8]) -> Result<bool>>(
        &self,
        start: Option<u64>,
        end: Option<u64>,
        zone_pred: Option<crate::sstable::ZonePredicate>,
        mut f: F,
    ) -> Result<()> {
        let sk = start.map(|s| encode_docid(s).to_vec());
        let ek = end.map(|e| encode_docid(e).to_vec());
        self.primary.scan_stream_with_zonepred(sk.as_deref(), ek.as_deref(), zone_pred, |key, val| {
            let docid = decode_docid(key).map_err(|_| {
                crate::error::Error::Corrupted("scan 流式 key 非 docid 编码".into())
            })?;
            if let Some(bm) = &self.deletion_bitmap {
                if bm.is_deleted(docid) {
                    return Ok(true);
                }
            }
            f(docid, val)
        })
    }

    /// Ex-8.3 Part B：keys-only 流式 id 扫描（最新视图，免整文档值解码）——纯 `SELECT id` /
    /// COUNT 类只关心 docid 存在性的路径；merge 版本折叠 + Tombstone 跳过 + 删除位图过滤，
    /// 回调返回 false 提前终止。语义与 `scan_stream` 输出 docid 集一致。
    pub fn scan_stream_ids<F: FnMut(u64) -> Result<bool>>(
        &self,
        start: Option<u64>,
        end: Option<u64>,
        mut f: F,
    ) -> Result<()> {
        let sk = start.map(|s| encode_docid(s).to_vec());
        let ek = end.map(|e| encode_docid(e).to_vec());
        self.primary
            .scan_stream_keys(sk.as_deref(), ek.as_deref(), |key| {
                let docid = decode_docid(key).map_err(|_| {
                    crate::error::Error::Corrupted("scan keys key 非 docid 编码".into())
                })?;
                if let Some(bm) = &self.deletion_bitmap {
                    if bm.is_deleted(docid) {
                        return Ok(true);
                    }
                }
                f(docid)
            })
    }

    /// §27 P0：auto_increment / 自动 docid **水位** = 已写入最大 docid + 1（自动分配起点，
    /// 重启续接不撞已提交行、显式大 id 后自动 id 抬位）。运行期由 put 的 fetch_max 维护；
    /// 重启后（max_docid 归零）首次调用做一次全库 keys-only 扫描恢复现存最大 docid
    /// （AtomicBool swap 只扫一次；顺带修复删除密度分母 Ex-8.7 的重启失真）。
    pub fn auto_watermark(&self) -> u64 {
        if !self.max_docid_loaded.swap(true, Ordering::AcqRel) {
            let mut mx = 0u64;
            // 扫描失败（读损坏等）保守保持 0 → 水位 1；loaded 已置位避免每次重扫
            let _ = self.scan_stream_ids(None, None, |d| {
                if d > mx {
                    mx = d;
                }
                Ok(true)
            });
            self.max_docid.store(mx, Ordering::Relaxed);
        }
        self.max_docid.load(Ordering::Relaxed) + 1
    }

    /// 7.100 全库可见行计数（COUNT(*) 无 WHERE 快路径）：主数据 key-only 流式计数——
    /// SST keys-only 解码免文档值反序列化/clone；merge 版本语义（同 key 最新、Tombstone
    /// 跳过）与 `scan_stream` 全表扫描一致。
    /// P1-C：懒建活跃 docid 基线（首次 `count_all_docs` 全键扫一次；此后写路径增量
    /// 维护，读取 O(1)）。keys-only 扫最新视图（Tombstone / 删除位图已隐藏），口径与
    /// 既有 count_all_docs 完全一致。
    fn live_ensure(&self) -> Result<()> {
        let mut g = self.live_docids.lock().unwrap();
        if g.is_some() {
            return Ok(());
        }
        let mut bm = RoaringTreemap::new();
        self.scan_stream_ids(None, None, |d| {
            bm.insert(d);
            Ok(true)
        })?;
        *g = Some(bm);
        Ok(())
    }

    /// P1-C：活跃集增量（put 路径）——新 docid / 已删复活 → +1；覆盖既有 docid 不变。
    pub(crate) fn live_add(&mut self, docid: u64) {
        let mut g = self.live_docids.lock().unwrap();
        if let Some(bm) = g.as_mut() {
            if !bm.contains(docid) {
                bm.insert(docid);
            }
        }
    }

    /// P1-C：活跃集剔除（delete 路径）——幂等（删不存在 / 重复删为 no-op）。
    pub(crate) fn live_remove(&mut self, docid: u64) {
        let mut g = self.live_docids.lock().unwrap();
        if let Some(bm) = g.as_mut() {
            bm.remove(docid);
        }
    }

    /// COUNT(*) 无 WHERE 快路径（P1-C：O(1) 增量计数——首次调用全键扫建基线，
    /// 此后 put/delete/purge 增量记账；语义与 keys-only 扫描口径一致）。
    pub fn count_all_docs(&self) -> Result<u64> {
        self.live_ensure()?;
        let g = self.live_docids.lock().unwrap();
        Ok(g.as_ref().map(|b| b.len()).unwrap_or(0))
    }

    /// 导出共享后台 IO 限速（design 20.5）：启用/关闭顺序扫描路径限速（MB/s；0 = 关闭）。
    /// 与 Compaction 的 `io_limiter` 同 Token Bucket 策略（默认低于前台读写）——导出读 SST
    /// 与后台合并共享同一后台 IO 预算语义，对在线业务影响 <5% 目标。
    pub fn set_scan_rate_limit(&self, mb: u64) {
        let bytes = mb.saturating_mul(1024 * 1024);
        self.primary.set_scan_rate_limit(bytes);
        self.delta.set_scan_rate_limit(bytes);
        if let Some(c) = &self.cidx {
            c.set_scan_rate_limit(bytes);
        }
    }


}
