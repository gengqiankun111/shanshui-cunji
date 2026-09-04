//! 范围扫描流式迭代与 Zone Map 谓词（原 reader.rs 拆分，按主题聚类）：
//! - `read_at`：位置读（`&File` 并发随机读基础，Windows/Unix 双实现）——reader 块读回退路径复用；
//! - `ZonePredicate`：字段级 Zone Map 查询谓词（P1-E，与迭代器 Zone 剪枝耦合紧，随 iter）；
//! - `SstRangeIter`：SST 范围扫描流式迭代器（M8-P10）——块级惰性读取 + Zone Map 剪枝 +
//!   组读预取（SCAN_GROUP）/仅 key 模式/块缓存/字段投影/Zone 谓词剪枝。

use crate::error::Result;

use super::block::{
    decode_data_block, decode_data_block_keys, decode_projected_block, DecodedRow,
};
use super::reader::SstReader;
use super::writer::IndexEntry;
use super::SCAN_GROUP;

/// 位置读（`&File` 可并发，读写分离读路径基础）：Windows `seek_read` / Unix `read_at`，
/// 不移动文件游标——多线程可同时对同一 SST 的不同块做读取。
#[cfg(windows)]
pub(crate) fn read_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> crate::error::Result<()> {
    use std::os::windows::fs::FileExt;
    file.seek_read(buf, offset).map_err(crate::error::Error::Io)?;
    Ok(())
}

#[cfg(not(windows))]
pub(crate) fn read_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> crate::error::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, offset).map_err(crate::error::Error::Io)?;
    Ok(())
}

/// P1-E：字段级 Zone Map 谓词——块级 min/max 与查询范围比较，不相交块跳过。
/// 字段的 min/max 是 `serde_json` 序列化后的字节序，比较时直接按字节序（数值/字符串均有序）。
/// `min`/`max` 为 `None` 表示该侧无界（如 `amount > 100` 的 max 为 None）。
#[derive(Debug, Clone)]
pub struct ZonePredicate {
    pub field: String,
    /// 查询范围下界（JSON 序列化字节，闭区间）；None = 无下界。
    pub min: Option<Vec<u8>>,
    /// 查询范围上界（JSON 序列化字节，闭区间）；None = 无上界。
    pub max: Option<Vec<u8>>,
}

/// SST 范围扫描**流式迭代器**（M8-P10 scan 流式化）：块级惰性读取 + Zone Map 剪枝，
/// 逐条 yield `(key, value, seq)`（value=None = Tombstone）。与 `scan_range` 语义一致，
/// 但可暂停/推进（k-way merge 多源归并用），内存 O(块) 而非 O(全量)。
pub struct SstRangeIter<'a> {
    reader: &'a SstReader,
    /// 当前候选块下标（自起始块起，不再持有全索引副本——M 项 P0 修复：
    /// 原 `reader.index()` 每次克隆整个块索引（78 个 SST × 全量深拷贝）致小范围扫描初始化成本秒级）。
    block_idx: usize,
    rows: Vec<DecodedRow>,
    row_idx: usize,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    /// U 项：块预读缓存（块下标 → 解码行）——`advance_block` 一次组读 ≤4 块
    /// （合并 read_at + 预解码），后续块直接消费（冷顺序扫描 IO 放大 4×4KB → 1×16KB）。
    prefetch: std::collections::VecDeque<(usize, Vec<DecodedRow>)>,
    /// 7.100：仅 key 模式（行计数快路径）——`decode_data_block_keys` 免值解码。
    keys_only: bool,
    /// Ex-8.3：可选块缓存（点查同款 BlockCache LRU，key=文件+块 offset）——扫描/计数路径
    /// 读块**写穿**缓存：全组命中免磁盘 IO + 解压，重复窗口（分页/热范围/重测）直达；
    /// None = 不缓存（低层/测试直用，行为与改造前一致）。
    cache: Option<std::sync::Arc<crate::blockcache::BlockCache>>,
    /// P1-E：字段级 Zone Map 谓词——块级 min/max 检查，不相交块跳过。
    zone_pred: Option<ZonePredicate>,
    /// P91：通用 scan 投影列——非空时块解码只物化/输出请求列的子集 JSON：
    /// PAX 块走列解码（免整行 25 列重构）；行式块直通原 JSON（消费端按需取列，
    /// 与既有语义一致）。供无 WHERE 全扫聚合 / GROUP BY / 排序键扫描等只读所需列。
    project: Option<Vec<String>>,
}

impl<'a> SstRangeIter<'a> {
    /// O 项第①步：`&SstReader`（读路径共享，配合 RwLock 读读并行）。
    pub fn new(
        reader: &'a SstReader,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Self> {
        Self::with_mode(reader, start, end, false, None)
    }

    /// 7.100：key-only 迭代（免值解码，仅 key/seq/tombstone）——行计数专用。
    pub fn new_keys(
        reader: &'a SstReader,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Self> {
        Self::with_mode(reader, start, end, true, None)
    }

    /// Ex-8.3：带块缓存的流式迭代（scan_stream_at 用）。
    pub fn new_cached(
        reader: &'a SstReader,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        cache: std::sync::Arc<crate::blockcache::BlockCache>,
    ) -> Result<Self> {
        Self::with_mode(reader, start, end, false, Some(cache))
    }

    /// Ex-8.3：带块缓存的 key-only 迭代（count_keys_range_filtered 用）。
    pub fn new_keys_cached(
        reader: &'a SstReader,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        cache: std::sync::Arc<crate::blockcache::BlockCache>,
    ) -> Result<Self> {
        Self::with_mode(reader, start, end, true, Some(cache))
    }

    fn with_mode(
        reader: &'a SstReader,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        keys_only: bool,
        cache: Option<std::sync::Arc<crate::blockcache::BlockCache>>,
    ) -> Result<Self> {
        // 二分定位起始块（start 无值从 0 开始；Level 2 索引懒加载后缓存，后续零 IO）
        let start_idx = match start {
            Some(s) => reader.locate_block_index(s)?.unwrap_or(0),
            None => 0,
        };
        Ok(Self {
            reader,
            block_idx: start_idx,
            rows: Vec::new(),
            row_idx: 0,
            start: start.map(|s| s.to_vec()),
            end: end.map(|e| e.to_vec()),
            prefetch: std::collections::VecDeque::new(),
            keys_only,
            cache,
            zone_pred: None,
            project: None,
        })
    }

    /// P1-E：设置字段级 Zone Map 谓词（扫描路径用，默认 None 无剪枝）。
    pub fn set_zone_pred(&mut self, zp: ZonePredicate) {
        self.zone_pred = Some(zp);
    }

    /// P91：设置投影列（扫描只解/输出这些列；默认 None = 整行直通）。
    pub fn set_project_fields(&mut self, fields: Vec<String>) {
        self.project = Some(fields);
    }

    /// 推进到下一个候选块（Zone Map 剪枝），加载并解码；无更多块返回 false。
    /// U 项（7.98）：一次组读当前块起 ≤8 块（合并 read_at + 组内并行解压进 prefetch 缓存）。
    fn advance_block(&mut self) -> Result<bool> {
        // 优先消费预读缓存
        if let Some((_, rows)) = self.prefetch.pop_front() {
            self.rows = rows;
            self.row_idx = 0;
            return Ok(true);
        }
        // 组读：当前块起 ≤SCAN_GROUP 块（Zone Map 剪枝）
        let mut picks: Vec<(usize, IndexEntry)> = Vec::new();
        let mut bidx = self.block_idx;
        while bidx < self.reader.index_len() && picks.len() < SCAN_GROUP {
            let mut e = self.reader.block_entry(bidx)?;
            if let Some(s) = &self.start {
                if e.max_key.as_slice() < s.as_slice() {
                    bidx += 1;
                    continue;
                }
            }
            if let Some(en) = &self.end {
                if e.first_key.as_slice() > en.as_slice() {
                    break; // 索引按 key 有序，后续块更大
                }
            }
            // P1-E：字段级 Zone Map 剪枝——块级 min/max 与谓词比较，不相交则跳过整块
            if let Some(ref zp) = self.zone_pred {
                if let Some(zone) = e.zones.iter().find(|z| z.field == zp.field) {
                    // 若查询下界 > 块上界 → 整块不命中（跳过）
                    if let Some(ref min) = zp.min {
                        if min.as_slice() > zone.max.as_slice() {
                            bidx += 1;
                            continue;
                        }
                    }
                    // 若查询上界 < 块下界 → 整块不命中（跳过）
                    if let Some(ref max) = zp.max {
                        if max.as_slice() < zone.min.as_slice() {
                            bidx += 1;
                            continue;
                        }
                    }
                }
            }
            picks.push((bidx, e));
            bidx += 1;
        }
        if picks.is_empty() {
            return Ok(false);
        }
        self.block_idx = picks.last().map(|(i, _)| *i + 1).unwrap_or(self.block_idx);
        let entries: Vec<IndexEntry> = picks.iter().map(|(_, e)| e.clone()).collect();
        // Ex-8.3：块缓存（与点查同 key=文件+offset）——全组命中免磁盘 IO + 解压（重复窗口直达）；
        // 未全命中则整组读并**写穿**缓存（热窗口二次扫描起命中）。
        let blocks = if let Some(cache) = &self.cache {
            let file = self.reader.path().to_path_buf();
            let tid = self.reader.table_id().unwrap_or(0);
            let cks: Vec<crate::blockcache::BlockCacheKey> = picks
                .iter()
                .map(|(_, e)| crate::blockcache::BlockCacheKey::new(file.clone(), e.offset, tid))
                .collect();
            let hits: Vec<Option<Vec<u8>>> = cks.iter().map(|ck| cache.get(ck)).collect();
            if hits.iter().all(Option::is_some) {
                hits.into_iter().map(|b| b.unwrap()).collect()
            } else {
                let group = self.reader.read_block_group(&entries)?;
                for (ck, b) in cks.into_iter().zip(group.iter()) {
                    cache.put(ck, b.clone());
                }
                group
            }
        } else {
            self.reader.read_block_group(&entries)?
        };
        for ((i, _), block) in picks.into_iter().zip(blocks) {
            let rows = if self.keys_only {
                decode_data_block_keys(&block, self.reader.format())?
                    .into_iter()
                    .map(|(k, is_put, seq)| (k, is_put.then_some(Vec::new()), seq))
                    .collect()
            } else if let Some(fields) = self.project.clone() {
                decode_projected_block(&block, self.reader.format(), &fields)?
            } else {
                decode_data_block(&block, self.reader.format())?
            };
            self.prefetch.push_back((i, rows));
        }
        if let Some((_, rows)) = self.prefetch.pop_front() {
            self.rows = rows;
            self.row_idx = 0;
            return Ok(true);
        }
        Ok(false)
    }
}

impl<'a> Iterator for SstRangeIter<'a> {
    type Item = Result<(Vec<u8>, Option<Vec<u8>>, u64)>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.row_idx >= self.rows.len() {
                match self.advance_block() {
                    Ok(true) => continue,
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }
            let (k, v, seq) = &self.rows[self.row_idx];
            self.row_idx += 1;
            if let Some(s) = &self.start {
                if k.as_slice() < s.as_slice() {
                    continue;
                }
            }
            if let Some(en) = &self.end {
                if k.as_slice() > en.as_slice() {
                    return None; // 升序，后续更大
                }
            }
            return Some(Ok((k.clone(), v.clone(), *seq)));
        }
    }
}
