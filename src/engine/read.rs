//! 文档引擎读族（reconstruct.md engine/read.rs）：get / batch_get / batch_get_fields /
//! zone_field_aggregate 与 Delta 批量覆盖收集辅助（batch_delta_overrides）。
//! 内容拆分自原 engine.rs（读主题 impl 块）；私有 Engine 字段以 `pub(crate)` 提升访问。

use std::sync::atomic::Ordering;

use crate::column_family::ColumnFamily;
use crate::engine::Engine;
use crate::error::Result;
use crate::keys::{decode_docid, encode_docid};


/// N 项：Delta 批量覆盖收集——单次范围扫描 `[min..max]`（docid 编码 + 字段变长前缀），
/// 按 docid 分组返回字段覆盖列表（`null` 值 = 删除字段），替代逐 docid 扫描。
/// key 布局与 `Engine::patch`/`get` 一致：8 字节 docid ++ 4 字节 VarLen 前缀 ++ 字段名。
/// O 项第②步：`&ColumnFamily`（delta 扫描读路径已 &self）。
fn batch_delta_overrides(
    delta: &ColumnFamily,
    docids: &[u64],
) -> Result<std::collections::HashMap<u64, Vec<(String, serde_json::Value)>>> {
    let mut out: std::collections::HashMap<u64, Vec<(String, serde_json::Value)>> =
        std::collections::HashMap::new();
    let (Some(&min), Some(&max)) = (docids.iter().min(), docids.iter().max()) else {
        return Ok(out);
    };
    let start = encode_docid(min).to_vec();
    let mut end = encode_docid(max).to_vec();
    end.extend_from_slice(&[0xFF; 4]);
    let rows = delta.scan_raw_range(Some(&start), Some(&end))?;
    for (k, v) in rows {
        if k.len() < 12 {
            continue;
        }
        let docid = match decode_docid(&k[..8]) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let field = match String::from_utf8(k[12..].to_vec()) {
            Ok(f) => f,
            Err(_) => continue,
        };
        let val: serde_json::Value = match serde_json::from_slice(&v) {
            Ok(x) => x,
            Err(_) => continue,
        };
        out.entry(docid).or_default().push((field, val));
    }
    Ok(out)
}

impl Engine {
    /// 点查文档：HotCache 命中直达，否则主数据 LSM + Delta Merge-on-Read。
    /// Ex-5.6：删除位图开启时先 O(1) 判定，已删文档直接返回 None（零 LSM 读）。
    /// O 项第②步：读路径 `&self`（HotCache 内部 Mutex）。
    /// X 项：读操作计数 + 延迟直方图（完整路径）。
    pub fn get(&self, docid: u64) -> Result<Option<Vec<u8>>> {
        self.metrics.read_ops.fetch_add(1, Ordering::Relaxed);
        let t = std::time::Instant::now();
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
        self.metrics.record_latency(t.elapsed().as_nanos() as u64);
        Ok(Some(merged))
    }

    /// 批量回表（N 项：借鉴 batch_get 建议落地；倒排/全文检索 posting 回表路径）。
    /// 语义与 `get` 一致（删除位图 / HotCache / Delta 字段覆盖），但一次处理多个 docid：
    /// ① 删除位图 O(1) 批量过滤；② HotCache 批量命中；③ primary `get_many` 批量读
    /// （SST 层按块分组，同块多 key 只读/解压一次，块缓存复用）；④ Delta 覆盖用**单次
    /// 范围扫描** [min..max] 按 docid 分组，替代逐 docid 扫描。
    /// 输入要求：docids 升序且无重复（倒排 bitmap 迭代天然满足）。
    /// 返回与输入顺序对齐的 `Vec<Option<value>>`。
    /// O 项第②步：读路径 `&self`（HotCache 内部 RwLock 读读并行 + DashMap 无锁计数）。
    pub fn batch_get(&self, docids: &[u64]) -> Result<Vec<Option<Vec<u8>>>> {
        let n = docids.len();
        let mut out: Vec<Option<Vec<u8>>> = vec![None; n];
        if n == 0 {
            return Ok(out);
        }
        // ① 删除位图 + ② HotCache
        let mut need_primary: Vec<usize> = Vec::new();
        for (i, &d) in docids.iter().enumerate() {
            if let Some(bm) = &self.deletion_bitmap {
                if bm.is_deleted(d) {
                    continue;
                }
            }
            if let Some(v) = self.hotcache.get(d) {
                out[i] = Some(v);
            } else {
                need_primary.push(i);
            }
        }
        if need_primary.is_empty() {
            return Ok(out);
        }
        let sub: Vec<u64> = need_primary.iter().map(|&i| docids[i]).collect();
        let found = self.primary.get_many(&sub)?; // Vec<Option<(value, seq)>>
        // ④ Delta 批量覆盖（单次范围扫描，按 docid 分组）
        let overrides = batch_delta_overrides(&self.delta, &sub)?;
        for (j, &i) in need_primary.iter().enumerate() {
            let d = docids[i];
            let Some((bv, _seq)) = &found[j] else {
                continue;
            };
            // P86①：无 Delta 覆盖 → 直通短路（跳 parse/reserialize 等值空转）。
            // 键序差异不影响 JSON 消费端语义；hotcache 缓存原字节（保持 get 同构缓存）。
            if !overrides.contains_key(&d) {
                let v = bv.clone();
                self.hotcache.put(d, v.clone());
                out[i] = Some(v);
                continue;
            }
            let obj: serde_json::Value = match serde_json::from_slice(bv) {
                Ok(v) => v,
                Err(_) => {
                    // 非 JSON 原始字节文档：无 Delta 覆盖
                    out[i] = Some(bv.clone());
                    continue;
                }
            };
            let mut map = match obj {
                serde_json::Value::Object(m) => m,
                _ => {
                    out[i] = Some(bv.clone());
                    continue;
                }
            };
            if let Some(over) = overrides.get(&d) {
                for (field, val) in over {
                    if val.is_null() {
                        map.shift_remove(field);
                    } else {
                        map.insert(field.clone(), val.clone());
                    }
                }
            }
            let merged = serde_json::to_vec(&map)
                .map_err(|e| crate::error::Error::Serialize(e.to_string()))?;
            self.hotcache.put(d, merged.clone());
            out[i] = Some(merged);
        }
        Ok(out)
    }

    /// P87②：投影字段批量回表——每个 docid 只返回请求的顶层字段值（倒排候选
    /// Top-K 排序键解码下推：PAX 块列解码 / 行式块按需字段提取，免整行 25 列解码）。
    /// 语义与 `batch_get` 一致（删除位图 O(1) 过滤 / HotCache 命中 / Delta 字段级覆盖 /
    /// Tombstone → None），但输出为 `Vec<Option<Vec<Option<Vec<u8>>>>>`：
    /// `out[i] = Some(vals)` 表示 docid 存在，`vals[j]` = `fields[j]` 的 JSON 值字节
    /// （`b"null"` = JSON null；None = 缺字段/行不存在）。非 JSON 基行 → 字段全 None
    /// （对齐 get 的"非 JSON 直接返回 base、不合并 delta"语义）。
    /// 输入要求：docids 升序且无重复；fields 为空 → 全部 None（调用方不应如此调用）。
    pub fn batch_get_fields(
        &self,
        docids: &[u64],
        fields: &[String],
    ) -> Result<Vec<Option<Vec<Option<Vec<u8>>>>>> {
        let n = docids.len();
        let mut out: Vec<Option<Vec<Option<Vec<u8>>>>> = vec![None; n];
        if n == 0 || fields.is_empty() {
            return Ok(out);
        }
        // ① 删除位图 + ② HotCache（缓存整行 → 按需提取）
        let mut need_primary: Vec<usize> = Vec::new();
        for (i, &d) in docids.iter().enumerate() {
            if let Some(bm) = &self.deletion_bitmap {
                if bm.is_deleted(d) {
                    continue;
                }
            }
            if let Some(v) = self.hotcache.get(d) {
                out[i] = Some(crate::sstable::extract_fields_from_json_row(&v, fields));
            } else {
                need_primary.push(i);
            }
        }
        if need_primary.is_empty() {
            return Ok(out);
        }
        let sub: Vec<u64> = need_primary.iter().map(|&i| docids[i]).collect();
        // ③ primary 投影批量点查（PAX 列解码 / 行式按需提取）
        let found = self.primary.get_many_fields(&sub, fields)?;
        // ④ Delta 字段级覆盖（单次范围扫描按 docid 分组；仅覆盖请求字段生效）
        let overrides = batch_delta_overrides(&self.delta, &sub)?;
        for (j, &i) in need_primary.iter().enumerate() {
            let d = docids[i];
            let Some((base_vals, _seq)) = &found[j] else {
                continue;
            };
            let vals: Vec<Option<Vec<u8>>> = match overrides.get(&d) {
                Some(ov) if !ov.is_empty() => {
                    let mut v2 = base_vals.clone();
                    for (fi, f) in fields.iter().enumerate() {
                        for (of, oval) in ov {
                            if of == f {
                                v2[fi] = if oval.is_null() {
                                    None // delta null = 删除该字段
                                } else {
                                    Some(serde_json::to_vec(oval).unwrap_or_default())
                                };
                                break;
                            }
                        }
                    }
                    v2
                }
                _ => base_vals.clone(), // 无覆盖 → 直通（P86① 同构短路）
            };
            out[i] = Some(vals);
        }
        Ok(out)
    }

    /// P90：PAX 块级聚合下推入口（无 WHERE `SUM(f)`/`COUNT(f)` 快路径候选）。
    /// eligible 前置：删除位图无置位 + delta 列族空（无字段 patch）+ 无活跃 RR 快照
    /// （MVCC 保活多版本会污染块级单版本假设）+ primary 快照 eligible（单一非空层 /
    /// 全 PAX 块 / memtable 空，见 `ColumnFamily::zone_field_aggregate`）。
    /// 返回 `(sum, present, null_count)`：`present - null_count` = COUNT(f) 精确；
    /// `sum` = 块内数值列累加和。任一条件不满足 → None（调用方回退行级精确扫描）。
    /// SUM 的零和/列非数值歧义由调用方处置（zsum==0 时回退行级，保证 NULL/0 语义）。
    pub fn zone_field_aggregate(
        &self,
        start: Option<u64>,
        end: Option<u64>,
        field: &str,
    ) -> Result<Option<(f64, u64, u64)>> {
        if let Some(bm) = &self.deletion_bitmap {
            if bm.deleted_count() > 0 {
                return Ok(None); // 删除位图置位 → 文件内已删行不可见 → 行级回退
            }
        }
        if !self.delta.data_empty() {
            return Ok(None); // delta patch 混入 → 字段可能被覆盖 → 行级回退
        }
        if !self.active_snapshots.read().unwrap().is_empty() {
            return Ok(None); // 活跃快照 → MVCC 保活多版本 → 行级回退
        }
        let lo = start.map(|s| crate::keys::encode_docid(s).to_vec());
        let hi = end.map(|e| crate::keys::encode_docid(e).to_vec());
        self.primary
            .zone_field_aggregate(lo.as_deref(), hi.as_deref(), field)
    }


}
