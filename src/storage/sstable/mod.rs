//! SSTable：磁盘有序文件（design 4.4 / development 步骤 6 / 阶段 1.5 PAX）。
//!
//! 文件布局：
//!
//! ```text
//! ┌────────────────────────────┐
//! │ File Header (magic+版本+配置) │
//! ├────────────────────────────┤
//! │ Data Block 0..N（独立压缩+CRC）│
//! ├────────────────────────────┤
//! │ Block Index（稀疏索引+Zone Map）│
//! │ Bloom Filter               │
//! ├────────────────────────────┤
//! │ Footer（各段偏移/长度/统计）    │
//! └────────────────────────────┘
//! ```
//!
//! 数据块两种布局（阶段 1.5，块首 1 字节 kind 标记，同文件可混合）：
//! - **行式块（kind=0）**：`Key(VarLen) ++ Value(VarLen) ++ Flags(u8) ++ Seq(u64)`，兼容 MVP；
//! - **PAX 列式块（kind=1）**：`列偏移量表（字段名+热/冷标记+偏移/长度）→ 热列组（块头）→ 冷列组（块尾）`，
//!   文档按 JSON 字段拆列（preserve_order 保序，重组与写入字节一致），宽表点查可只读热列切片；
//! - 文件级向后兼容：v3（全行式）与 v4（可含 PAX）共存，Reader 按 Footer 版本 + 块 kind 双重分发。
//!
//! 块尾 Trailer：`RawLen(u32) ++ CompLen(u32) ++ CRC32(u32)`，损坏只影响单块。
//!
//! 模块结构（按主题拆分，对外 API 路径保持 `crate::storage::sstable::*` / `crate::sstable::*` 不变）：
//! - [`writer`](writer)：写路径——`Compression` / `PendingRow` / `FieldZone` / `IndexEntry` /
//!   `SstWriter` 及行式/PAX 块编码（`encode_row_block` / `json_object` / `encode_pax_block`）；
//! - [`reader`](reader)：读打开/点读——`SstReader` / `SstFooter` / `SummaryEntry` / `decode_index`、
//!   块读取与块内点查（get / scan_block_* / locate_indexed_block）；
//! - [`iter`](iter)：范围扫描/谓词——`read_at` / `ZonePredicate` / `SstRangeIter`（流式迭代）；
//! - [`block`](block)：块解码/字段提取——`DecodedRow` / `decode_data_block` / `decode_pax_block*` /
//!   `decode_projected_block` / `extract_fields_from_json_row`；
//! - 本文件：模块文档 + 共享常量 + `crc32` + 顶层 re-export + 单元测试。
//! 各子文件为私有 `mod`（仅 crate 内部重组用），全部公开项经下方 `pub use` 原路径汇总。

mod block;
mod reader;
mod iter;
mod writer;
// Compaction（自 column_family.rs 按主题抽出）：`pub(crate)`——column_family.rs 的
// mod tests 直接引用其选段自由函数；无对外 crate::sstable 公开路径需求。
// 再按主题拆分：compaction.rs 保留对外入口与策略方法；merge.rs 承载合并执行内部
// （compact_merge / select_* 等），compaction.rs 经 re-export 保持 `compaction::{...}` 路径。
pub(crate) mod compaction;
pub(crate) mod merge;

// 对外公开项 re-export（等价于拆分前 mod.rs 内直接 pub 定义，路径不变）。
pub use block::{
    decode_data_block, decode_data_block_keys, decode_pax_block_column, decode_pax_block_fields,
    decode_projected_block, extract_fields_from_json_row, DecodedRow,
};
pub use iter::{SstRangeIter, ZonePredicate};
pub use reader::{SstFooter, SstReader, SummaryEntry};
pub use writer::{Compression, FieldZone, IndexEntry, SstWriter};

/// 文件魔数 + 版本。
pub const SST_MAGIC: &[u8; 8] = b"NVSSTL01";
/// v4：数据块引入 PAX 列式布局（块首 kind 字节）；仍可读取 v3（行式）。
/// 当前格式版本：v5 = 分区布隆（Partitioned Bloom，design 4.4.2）。
/// v6 = P3-B：FieldZone 新增 `sum` 字段（数值列块内累加和，供 SUM/AVG 聚合下推）。
pub const SST_VERSION: u16 = 6;
/// v3：行式数据块（无 kind 字节）——Reader 向后兼容的最低版本。
pub const SST_VERSION_ROW: u16 = 3;

/// 数据块 kind：行式（MVP 兼容）。
pub const BLOCK_KIND_ROW: u8 = 0;
/// 数据块 kind：PAX 列式（阶段 1.5）。
pub const BLOCK_KIND_PAX: u8 = 1;

/// 条目 Flags：Put。
pub const FLAG_PUT: u8 = 0;
/// 条目 Flags：Tombstone（删除标记）。
pub const FLAG_DELETE: u8 = 1;

/// 顺序扫描组读块数（7.98）：一次 read_at 合并 ≤8 块（冷全扫 IO 次数减半；
/// U 项 4 块 → 8 块；组内并行解压实测 spawn 开销 > 收益已回退串行）。
pub(crate) const SCAN_GROUP: usize = 8;

/// 数据块 Trailer（写在每个数据块末尾，固定 12 字节）。
/// writer/reader 共用（同一模块子树内可见）。
const TRAILER_LEN: usize = 12;

/// CRC32（IEEE 多项式，与 WAL 一致）。
pub fn crc32(data: &[u8]) -> u32 {
    crate::wal::crc32(data)
}

// 拆分专用：mod.rs 底部测试模块需直接调用块解码内部函数（保留测试访问子模块项）。
pub(crate) use block::decode_pax_block;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bloom::BloomFilter;
    use std::path::Path;

    fn tmp() -> std::path::PathBuf {
        static DIR: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let name = format!(
            "sst-{}.sst",
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    fn write_sample(path: &Path, n: u64) -> SstFooter {
        let mut w = SstWriter::new(path, Compression::Zstd, 3, 4096, n as usize).unwrap();
        for i in 0..n {
            let k = format!("user-{i:08}").into_bytes();
            let v = format!("value-of-{i}").into_bytes();
            w.add(&k, &v, i).unwrap();
        }
        w.finish().unwrap()
    }

    #[test]
    fn sst_reader_file_len_matches_disk() {
        // 修复（96ac6bc）：open 时一次 metadata 缓存 file_len——后续 l0_bytes/sst_bytes
        // 读快照 sizes 缓存零 syscall；此处验证缓存值与磁盘实际字节数一致。
        let path = tmp();
        write_sample(&path, 100);
        let r = SstReader::open(&path).unwrap();
        let disk = std::fs::metadata(&path).unwrap().len();
        assert_eq!(r.file_len(), disk, "file_len 缓存 = 磁盘实际字节数");
        assert!(r.file_len() > 0, "非空段 file_len > 0");
    }

    #[test]
    fn sst_range_iter_matches_scan_range() {
        // M8-P10 流式迭代器：与 scan_range 输出完全一致（全量 + 范围过滤 + 升序）
        let path = tmp();
        write_sample(&path, 100);
        let mut r = SstReader::open(&path).unwrap();
        // 全量
        let mut it_all: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        for row in SstRangeIter::new(&mut r, None, None).unwrap() {
            it_all.push(row.unwrap());
        }
        let mut sc_all: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        r.scan_range(None, None, |k, v, seq| {
            sc_all.push((k.to_vec(), v.map(|x| x.to_vec()), seq))
        })
        .unwrap();
        assert_eq!(it_all, sc_all, "迭代器应与 scan_range 全量一致");
        // 范围过滤（含 Zone Map 剪枝路径）
        let lo = b"user-00000030".as_slice();
        let hi = b"user-00000040".as_slice();
        let mut it_rng: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        for row in SstRangeIter::new(&mut r, Some(lo), Some(hi)).unwrap() {
            it_rng.push(row.unwrap());
        }
        let mut sc_rng: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        r.scan_range(Some(lo), Some(hi), |k, v, seq| {
            sc_rng.push((k.to_vec(), v.map(|x| x.to_vec()), seq))
        })
        .unwrap();
        assert_eq!(it_rng, sc_rng, "范围过滤迭代器应与 scan_range 一致");
        // 升序
        for w in it_all.windows(2) {
            assert!(w[0].0 < w[1].0, "迭代必须按 key 升序");
        }
    }

    #[test]
    fn range_iter_prefetch_multi_block_consistency() {
        // U 项：组读预读（≤4 块合并 read_at）——多块段扫描与单块逐读结果一致
        // （覆盖 read_block_group 合并读取 + 预解码缓存路径）
        let path = tmp();
        write_sample(&path, 2000); // ~10 块（4096 块/20B key）→ 覆盖组读
        let r = SstReader::open(&path).unwrap();
        assert!(r.index_len() > 4, "样本应 >4 块（实际 {}）", r.index_len());
        // 全量：组读路径
        let mut it_all: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        for row in SstRangeIter::new(&r, None, None).unwrap() {
            it_all.push(row.unwrap());
        }
        assert_eq!(it_all.len(), 2000, "全量行数");
        for (i, (k, v, seq)) in it_all.iter().enumerate() {
            assert_eq!(k, &format!("user-{i:08}").into_bytes(), "key {i}");
            assert_eq!(v.as_deref(), Some(format!("value-of-{i}").as_bytes()), "val {i}");
            assert_eq!(*seq, i as u64, "seq {i}");
        }
        // 跨块边界范围扫描（Zone Map 剪枝 + 预读）
        let lo = b"user-00000900".to_vec();
        let hi = b"user-00001100".to_vec();
        let mut it_rng: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        for row in SstRangeIter::new(&r, Some(&lo), Some(&hi)).unwrap() {
            it_rng.push(row.unwrap());
        }
        assert_eq!(it_rng.len(), 201, "闭区间 [900,1100] 行数");
        assert_eq!(it_rng[0].0, lo);
        assert_eq!(it_rng[200].0, hi);
        // 与 scan_range 对照（逐块路径）
        let mut sc: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        r.scan_range(Some(&lo), Some(&hi), |k, v, seq| {
            sc.push((k.to_vec(), v.map(|x| x.to_vec()), seq))
        })
        .unwrap();
        assert_eq!(it_rng, sc, "预读迭代器应与逐块 scan_range 一致");
    }

    #[test]
    fn write_read_roundtrip() {
        let path = tmp();
        let footer = write_sample(&path, 100);
        assert_eq!(footer.key_count, 100);
        let mut r = SstReader::open(&path).unwrap();
        assert_eq!(r.footer().key_count, 100);
        assert!(r.index_len() >= 1);
        for i in (0..100u64).step_by(7) {
            let k = format!("user-{i:08}").into_bytes();
            let (v, seq) = r.get(&k).unwrap().unwrap();
            assert_eq!(v.unwrap(), format!("value-of-{i}").into_bytes());
            assert_eq!(seq, i);
        }
    }

    #[test]
    fn bloom_prunes_absent_keys() {
        let path = tmp();
        write_sample(&path, 50);
        let mut r = SstReader::open(&path).unwrap();
        assert!(r.get(b"nope-not-here").unwrap().is_none());
    }

    #[test]
    fn non_strict_order_rejected() {
        let path = tmp();
        let mut w = SstWriter::new(&path, Compression::None, 0, 4096, 10).unwrap();
        w.add(b"b", b"1", 1).unwrap();
        assert!(w.add(b"a", b"2", 2).is_err());
    }

    #[test]
    fn iterate_visits_all_sorted() {
        let path = tmp();
        write_sample(&path, 1000);
        let mut r = SstReader::open(&path).unwrap();
        let mut last: Option<Vec<u8>> = None;
        let mut count = 0u64;
        r.iterate(|k, _v, _seq| {
            if let Some(l) = &last {
                assert!(k > l.as_slice());
            }
            last = Some(k.to_vec());
            count += 1;
        })
        .unwrap();
        assert_eq!(count, 1000);
    }

    #[test]
    fn multi_block_file_queries_across_blocks() {
        // 小块大小强制多块，验证跨块二分定位
        let path = tmp();
        let mut w = SstWriter::new(&path, Compression::Zstd, 1, 64, 200).unwrap();
        for i in 0..200u64 {
            let k = format!("k{i:04}").into_bytes();
            let v = format!("v{i:04}").into_bytes();
            w.add(&k, &v, i).unwrap();
        }
        w.finish().unwrap();
        let mut r = SstReader::open(&path).unwrap();
        assert!(r.index_len() > 1, "应产生多块，实际 {}", r.index_len());
        // 首块、中间块、末块的 key 都要能查到
        for i in [0u64, 99, 199] {
            let k = format!("k{i:04}").into_bytes();
            assert!(r.get(&k).unwrap().is_some(), "key k{i:04} 未命中");
        }
    }

    #[test]
    fn corrupted_block_detected() {
        let path = tmp();
        write_sample(&path, 30);
        // 翻转第一个数据块首字节（Header 15 字节之后），破坏 CRC
        let mut data = std::fs::read(&path).unwrap();
        data[15] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();
        let mut r = SstReader::open(&path).unwrap();
        // 块 CRC 校验失败 → Corrupted 错误（而非 panic）
        assert!(r.get(b"user-00000001").is_err());
    }

    #[test]
    fn pax_block_roundtrip_preserves_json_and_zones() {
        // PAX 列式块：JSON 按字段拆列，重组后语义等值（保序），字段级 Zone Map 采集
        let path = tmp();
        let hot = vec!["status".to_string(), "city".to_string()];
        let mut w =
            SstWriter::new_with_pax(&path, Compression::None, 0, 1024, 10, &hot, 0.01).unwrap();
        w.add(
            b"k1",
            br#"{"status":"active","city":"beijing","amount":100}"#,
            1,
        )
        .unwrap();
        w.add(b"k2", br#"{"status":"inactive","city":"shanghai"}"#, 2)
            .unwrap();
        w.add(b"k3", br#"{"status":"active","city":null,"extra":1}"#, 3)
            .unwrap();
        w.finish().unwrap();

        let mut r = SstReader::open(&path).unwrap();
        // 重组保真（字段序 = 原序，紧凑 JSON 与写入字节一致）
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"k1").unwrap().unwrap().0.unwrap()),
            r#"{"status":"active","city":"beijing","amount":100}"#
        );
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"k2").unwrap().unwrap().0.unwrap()),
            r#"{"status":"inactive","city":"shanghai"}"#
        );
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"k3").unwrap().unwrap().0.unwrap()),
            r#"{"status":"active","city":null,"extra":1}"#
        );
        // 字段级 Zone Map（present / null / min / max）
        let zones = &r.index()[0].zones;
        assert!(!zones.is_empty(), "PAX 块应有字段级 Zone Map");
        let st = zones.iter().find(|z| z.field == "status").unwrap();
        assert_eq!(st.present_count, 3);
        assert_eq!(st.null_count, 0);
        let city = zones.iter().find(|z| z.field == "city").unwrap();
        assert_eq!(city.present_count, 3);
        assert_eq!(city.null_count, 1);
        assert_eq!(String::from_utf8_lossy(&city.min), "\"beijing\"");
        assert_eq!(String::from_utf8_lossy(&city.max), "\"shanghai\"");
    }

    #[test]
    fn pax_falls_back_to_row_for_non_json_or_tombstone() {
        // 块内出现非 JSON 值或 Tombstone → 整块回退行式，读取不受影响
        let path = tmp();
        let hot = vec!["a".to_string()];
        let mut w =
            SstWriter::new_with_pax(&path, Compression::None, 0, 1024, 10, &hot, 0.01).unwrap();
        w.add(b"k1", br#"{"a":1,"b":2}"#, 1).unwrap();
        w.add(b"k2", b"not-json-bytes", 2).unwrap();
        w.add_tombstone(b"k3", 3).unwrap();
        w.finish().unwrap();

        let mut r = SstReader::open(&path).unwrap();
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"k1").unwrap().unwrap().0.unwrap()),
            r#"{"a":1,"b":2}"#
        );
        assert_eq!(
            r.get(b"k2").unwrap().unwrap().0.as_deref(),
            Some(&b"not-json-bytes"[..])
        );
        assert_eq!(
            r.get(b"k3").unwrap().unwrap().0,
            None,
            "Tombstone 读回应为 None"
        );
        // 非 PAX → 无字段级 Zone Map
        assert!(r.index()[0].zones.is_empty());
    }

    #[test]
    fn pax_mixed_block_kinds_in_one_file() {
        // 同一 v4 文件内 PAX 块与行式块共存：块 kind 分发正确
        let path = tmp();
        let hot = vec!["a".to_string()];
        let mut w =
            SstWriter::new_with_pax(&path, Compression::None, 0, 30, 10, &hot, 0.01).unwrap();
        w.add(b"k1", br#"{"a":1,"b":2}"#, 1).unwrap(); // 估算 ~35B ≥ 30 → 单独 flush → PAX 块
        w.add(b"k2", b"not-json", 2).unwrap(); // 行式块
        w.add(b"k3", br#"{"a":3}"#, 3).unwrap();
        w.finish().unwrap();

        let mut r = SstReader::open(&path).unwrap();
        assert!(r.index_len() >= 2, "应产生多块，实际 {}", r.index_len());
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"k1").unwrap().unwrap().0.unwrap()),
            r#"{"a":1,"b":2}"#
        );
        assert_eq!(
            r.get(b"k2").unwrap().unwrap().0.as_deref(),
            Some(&b"not-json"[..])
        );
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"k3").unwrap().unwrap().0.unwrap()),
            r#"{"a":3}"#
        );
        // 至少一个 PAX 块（带字段级 Zone Map）
        assert!(r.index().iter().any(|e| !e.zones.is_empty()));
    }

    #[test]
    fn decode_pax_block_fields_matches_full_row_and_single_column() {
        // P87②：单块多列解码与整行解码（逐字段）及逐列解码三方等值
        let path = tmp();
        let hot = vec!["status".to_string(), "city".to_string()];
        let mut w =
            SstWriter::new_with_pax(&path, Compression::None, 0, 1024, 10, &hot, 0.01).unwrap();
        w.add(b"k1", br#"{"status":"active","city":"beijing","amount":100}"#, 1)
            .unwrap();
        w.add(b"k2", br#"{"status":"inactive","city":"shanghai"}"#, 2)
            .unwrap();
        w.add(b"k3", br#"{"status":"active","city":null,"extra":1}"#, 3)
            .unwrap();
        w.add(b"k4", br#"{"status":"active","city":"shenzhen","amount":200,"note":"n1"}"#, 4)
            .unwrap();
        w.finish().unwrap();
        let r = SstReader::open(&path).unwrap();
        let block = r.read_block(&r.index()[0]).unwrap();
        let fields: Vec<String> = ["status", "city", "amount", "note", "missing"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let rows = decode_pax_block_fields(&block, &fields).unwrap();
        assert_eq!(rows.len(), 4, "块内 4 行");
        for (i, (key, vals, _seq)) in rows.iter().enumerate() {
            let full = decode_pax_block(&block).unwrap()[i].1.clone().unwrap();
            let expect = extract_fields_from_json_row(&full, &fields);
            assert_eq!(vals, &expect, "row {i} 多列解码 = 整行按字段提取");
            for (j, f) in fields.iter().enumerate() {
                let col = decode_pax_block_column(&block, f).unwrap();
                assert_eq!(vals[j], col[i].1, "row {i} col {f} 与逐列解码等值");
            }
            assert_eq!(key.as_slice(), format!("k{}", i + 1).as_bytes());
        }
        // 语义抽查：缺失列全 None；JSON null（city 行 3）→ Some(b"null")
        assert!(rows[0].1[4].is_none(), "missing 列 → None");
        assert_eq!(rows[2].1[1].as_deref(), Some(&b"null"[..]), "city=null → b\"null\"");
        assert!(rows[2].1[0].is_some() && rows[2].1[3].is_none(), "extra 非请求列不解");
        // 行式块回退路径：SstReader::scan_block_for_keys_fields 处理行式块
        let row_path = tmp();
        let mut rw = SstWriter::new(&row_path, Compression::None, 0, 1024, 4).unwrap();
        rw.add(b"a", br#"{"x":1,"y":"b"}"#, 5).unwrap();
        rw.add(b"b", b"raw-bytes", 6).unwrap();
        rw.finish().unwrap();
        let rr = SstReader::open(&row_path).unwrap();
        let rb = rr.read_block(&rr.index()[0]).unwrap();
        let mut targets = std::collections::HashSet::new();
        targets.insert(b"a".to_vec());
        targets.insert(b"b".to_vec());
        let want = vec!["x".to_string(), "y".to_string()];
        let hits = rr
            .scan_block_for_keys_fields(&rb, &targets, &want)
            .unwrap();
        assert_eq!(hits.len(), 2, "行式块命中 2 key");
        for (k, vals, seq) in hits {
            if k == b"a" {
                assert_eq!(seq, 5);
                let v0 = vals.unwrap();
                assert_eq!(v0[0].as_deref(), Some(&b"1"[..]), "x=1");
                assert_eq!(v0[1].as_deref(), Some(&b"\"b\""[..]), "y=\"b\"（含引号）");
            } else {
                assert_eq!(seq, 6);
                let v0 = vals.unwrap();
                assert!(v0.iter().all(|x| x.is_none()), "非 JSON 行 → 字段全 None");
            }
        }
    }

    #[test]
    fn decode_data_block_v3_format_without_kind() {
        // 向后兼容：v3 行式块无 kind 字节，decode_data_block(_, 3) 直接解析
        let mut row_data = Vec::new();
        crate::keys::encode_varlen(&mut row_data, b"x");
        crate::keys::encode_varlen(&mut row_data, b"y");
        row_data.push(FLAG_PUT);
        row_data.extend_from_slice(&1u64.to_le_bytes());
        crate::keys::encode_varlen(&mut row_data, b"z");
        crate::keys::encode_varlen(&mut row_data, b"");
        row_data.push(FLAG_DELETE);
        row_data.extend_from_slice(&2u64.to_le_bytes());
        let rows = decode_data_block(&row_data, SST_VERSION_ROW).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, b"x");
        assert_eq!(rows[0].1.as_deref(), Some(&b"y"[..]));
        assert_eq!(rows[0].2, 1);
        assert_eq!(rows[1].0, b"z");
        assert_eq!(rows[1].1, None, "Tombstone");
        assert_eq!(rows[1].2, 2);
    }

    #[test]
    fn scan_range_returns_bounded_keys() {
        let path = tmp();
        write_sample(&path, 200);
        let mut r = SstReader::open(&path).unwrap();
        let start = b"user-00000050";
        let end = b"user-00000060";
        let mut keys = Vec::new();
        r.scan_range(Some(start), Some(end), |k, _v, _seq| keys.push(k.to_vec()))
            .unwrap();
        // 闭区间：50..=60 共 11 个
        assert_eq!(keys.len(), 11);
        assert_eq!(keys.first().unwrap().as_slice(), start);
        assert_eq!(keys.last().unwrap().as_slice(), end);
        // 升序
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn raw_block_reuse_roundtrip() {
        // Ex-5.8 元数据-数据解耦：add_raw_block 块级复用——源块原样拷贝 + 重建
        // trailer/索引/分区布隆，读回键完整、布隆剪枝生效。
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.sst");
        {
            let mut w = SstWriter::new(&src, Compression::None, 0, 4096, 1000).unwrap();
            for i in 0..1000u64 {
                w.add(&i.to_be_bytes(), &[i as u8; 32], i).unwrap();
            }
            w.finish().unwrap();
        }
        // 块级复用重建（数据块原样，零解压重压缩）
        let dst = dir.path().join("reuse.sst");
        {
            let mut r = SstReader::open(&src).unwrap();
            let mut w = SstWriter::new(&dst, Compression::None, 0, 4096, 0).unwrap();
            let entries = r.index();
            assert!(entries.len() > 4, "应产生多个数据块");
            for e in entries {
                let (comp, raw) = r.block_raw(&e).unwrap();
                w.add_raw_block(&raw, &comp).unwrap();
            }
            w.finish().unwrap();
        }
        // 读回全部键一致
        let mut r2 = SstReader::open(&dst).unwrap();
        let mut n = 0u64;
        r2.iterate(|k, v, _seq| {
            let key = u64::from_be_bytes(k.try_into().unwrap());
            assert_eq!(key, n, "键序一致");
            assert_eq!(v.unwrap(), &[key as u8; 32]);
            n += 1;
        })
        .unwrap();
        assert_eq!(n, 1000);
        // 布隆剪枝生效（缺失键不被命中）
        assert!(r2.get(&9999u64.to_be_bytes()).unwrap().is_none());
        // 分区布隆已重建（数量与索引一致）
        assert!(r2.partition_blooms().is_some());
        assert_eq!(
            r2.partition_blooms().unwrap().len(),
            r2.index().len(),
            "分区布隆按块重建"
        );
    }

    #[test]
    fn scan_range_without_bounds_visits_all() {
        let path = tmp();
        write_sample(&path, 100);
        let mut r = SstReader::open(&path).unwrap();
        let mut count = 0u64;
        r.scan_range(None, None, |_, _, _| count += 1).unwrap();
        assert_eq!(count, 100);
    }

    #[test]
    fn zone_map_prunes_out_of_range_blocks() {
        // 多块文件：查询一个小范围应只读命中块，不读全部块
        let path = tmp();
        let mut w = SstWriter::new(&path, Compression::Zstd, 1, 64, 500).unwrap();
        for i in 0..500u64 {
            let k = format!("k{i:04}").into_bytes();
            w.add(&k, b"v", i).unwrap();
        }
        w.finish().unwrap();
        let mut r = SstReader::open(&path).unwrap();
        assert!(r.index_len() > 2);
        // Zone Map 元数据正确：每块 min <= max
        for e in r.index() {
            assert!(e.first_key <= e.max_key);
        }
        // 精确小区间（key 格式 k{i:04}，查询须同格式）
        let mut hits = Vec::new();
        r.scan_range(Some(b"k0050"), Some(b"k0060"), |k, _, _| {
            hits.push(k.to_vec())
        })
        .unwrap();
        assert_eq!(hits.len(), 11);
    }

    #[test]
    fn tombstone_roundtrip() {
        let path = tmp();
        let mut w = SstWriter::new(&path, Compression::Zstd, 1, 4096, 10).unwrap();
        w.add(b"a", b"va", 1).unwrap();
        w.add_tombstone(b"b", 2).unwrap();
        w.add(b"c", b"vc", 3).unwrap();
        w.finish().unwrap();

        let mut r = SstReader::open(&path).unwrap();
        let (va, _) = r.get(b"a").unwrap().unwrap();
        assert_eq!(va.unwrap(), b"va");
        let (vb, seq_b) = r.get(b"b").unwrap().unwrap();
        assert!(vb.is_none(), "b 应为 Tombstone");
        assert_eq!(seq_b, 2);
        let (vc, _) = r.get(b"c").unwrap().unwrap();
        assert_eq!(vc.unwrap(), b"vc");
        assert!(r.get(b"zzz").unwrap().is_none());

        // 范围扫描同样携带删除语义
        let mut found = Vec::new();
        r.scan_range(None, None, |k, v, _| found.push((k.to_vec(), v.is_some())))
            .unwrap();
        assert_eq!(
            found,
            vec![
                (b"a".to_vec(), true),
                (b"b".to_vec(), false),
                (b"c".to_vec(), true),
            ]
        );
    }

    #[test]
    fn partitioned_bloom_built_per_block() {
        // v5：每个数据块一个分区布隆，与 Index 对齐
        let path = tmp();
        let mut w = SstWriter::new(&path, Compression::Zstd, 1, 64, 500).unwrap();
        for i in 0..500u64 {
            let k = format!("k{i:04}").into_bytes();
            w.add(&k, b"v", i).unwrap();
        }
        w.finish().unwrap();
        let mut r = SstReader::open(&path).unwrap();
        let pb = r.partition_blooms().expect("v5 应有分区布隆");
        assert!(r.index_len() > 2, "多块文件");
        assert_eq!(pb.len(), r.index_len(), "分区布隆数 = 块数");
        assert!(r.legacy_bloom().is_none(), "v5 无整文件布隆");
        // 查询正确（分区布隆剪枝 + 读块）
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"k0250").unwrap().unwrap().0.unwrap()),
            "v"
        );
        // 缺失 key：定位块后由分区布隆剪枝返回 None
        assert!(r.get(b"k9999").unwrap().is_none());
    }

    #[test]
    fn partitioned_bloom_fpr_configurable() {
        // 不同 fpr 的位数组大小不同（fpr 越小位数越多）
        let strict = BloomFilter::with_estimated_keys_fpr(100, 0.001);
        let loose = BloomFilter::with_estimated_keys_fpr(100, 0.05);
        assert!(
            strict.num_bits() > loose.num_bits(),
            "fpr=0.001 应比 fpr=0.05 更多位"
        );
    }

    #[test]
    fn v4_legacy_bloom_still_readable() {
        // 向后兼容：手写 v4 格式（整文件布隆）读取路径
        let dir = tempfile::tempdir().unwrap();
        // 构造 v4 文件：header(15) + 一个行式块 + index + 旧单布隆 + footer
        let path = dir.path().join("v4.sst");
        let mut out = Vec::new();
        out.extend_from_slice(SST_MAGIC);
        out.extend_from_slice(&4u16.to_le_bytes());
        out.push(Compression::Zstd.code());
        out.extend_from_slice(&4096u32.to_le_bytes());
        // 数据块（行式）：key=ab, value=xy
        let mut block = Vec::new();
        crate::keys::encode_varlen(&mut block, b"ab");
        crate::keys::encode_varlen(&mut block, b"xy");
        block.push(FLAG_PUT);
        block.extend_from_slice(&1u64.to_le_bytes());
        let raw = block;
        let comp = zstd::bulk::compress(&raw, 3).unwrap();
        let block_offset = out.len() as u64;
        out.extend_from_slice(&comp);
        out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        out.extend_from_slice(&(comp.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32(&comp).to_le_bytes());
        // 索引
        let index_offset = out.len() as u64;
        let mut ib = Vec::new();
        crate::keys::encode_varint(&mut ib, 1);
        crate::keys::encode_varlen(&mut ib, b"ab");
        crate::keys::encode_varlen(&mut ib, b"ab");
        ib.extend_from_slice(&block_offset.to_le_bytes());
        ib.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        ib.extend_from_slice(&(comp.len() as u32).to_le_bytes());
        ib.extend_from_slice(&0u16.to_le_bytes()); // zones 空
        out.extend_from_slice(&ib);
        let index_len = ib.len() as u32;
        // 旧单布隆
        let bloom_offset = out.len() as u64;
        let mut bf = BloomFilter::with_estimated_keys(1);
        bf.insert(&b"ab".to_vec());
        let bbytes = bf.to_bytes();
        out.extend_from_slice(&(bbytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&bbytes);
        let bloom_len = (4 + bbytes.len()) as u32;
        // Footer
        let footer_offset = out.len() as u64;
        let mut fb = Vec::new();
        fb.extend_from_slice(SST_MAGIC);
        fb.extend_from_slice(&4u16.to_le_bytes());
        fb.extend_from_slice(&index_offset.to_le_bytes());
        fb.extend_from_slice(&index_len.to_le_bytes());
        fb.extend_from_slice(&bloom_offset.to_le_bytes());
        fb.extend_from_slice(&bloom_len.to_le_bytes());
        fb.extend_from_slice(&1u64.to_le_bytes());
        fb.extend_from_slice(&footer_offset.to_le_bytes());
        fb.extend_from_slice(&crc32(&fb).to_le_bytes());
        out.extend_from_slice(&fb);
        out.extend_from_slice(&footer_offset.to_le_bytes());
        std::fs::write(&path, &out).unwrap();

        let mut r = SstReader::open(&path).unwrap();
        assert!(r.partition_blooms().is_none(), "v4 无分区布隆");
        assert!(r.legacy_bloom().is_some(), "v4 应加载整文件布隆");
        assert_eq!(
            String::from_utf8_lossy(&r.get(b"ab").unwrap().unwrap().0.unwrap()),
            "xy"
        );
        assert!(r.get(b"absent").unwrap().is_none(), "整文件布隆剪枝");
    }

    // ---- 两级索引（design 4.4.2，阶段 2）----

    #[test]
    fn two_level_index_summary_resident_exact_lazy() {
        // 小块文件：制造多个数据块
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tli.sst");
        {
            let mut w = SstWriter::new(&path, Compression::Zstd, 3, 64, 100).unwrap();
            for i in 0..200u64 {
                w.add(format!("key-{i:06}").as_bytes(), b"value", i)
                    .unwrap();
            }
            w.finish().unwrap();
        }
        // 粒度 4：每 4 块一条摘要
        let mut r = SstReader::open_with_granularity(&path, 4).unwrap();
        let blocks = r.index_len();
        assert!(blocks > 4, "应产生多个数据块: {blocks}");
        // Level 1 常驻：摘要条数 = ceil(blocks / 4)，远小于 blocks（内存减少 90%）
        let expected = blocks.div_ceil(4);
        assert_eq!(r.summary_len(), expected);
        assert_eq!(r.summary()[0].block_index, 0);
        // 摘要块下标等差为粒度
        for w in r.summary().windows(2) {
            assert_eq!(w[1].block_index - w[0].block_index, 4);
        }
        // Level 2 尚未懒加载（open 时只留摘要，内存减负）
        assert!(!r.level2_loaded(), "open 后不应加载精确索引");
        // 查询触发 Level 2 懒加载，且结果正确
        let v = r.get(b"key-000042").unwrap().unwrap().0.unwrap();
        assert_eq!(v, b"value");
        assert!(r.level2_loaded(), "首次访问应懒加载精确索引");
        // 懒加载后精确索引内容正确（块数一致）
        assert_eq!(r.index().len(), blocks);
    }

    #[test]
    fn two_level_index_query_across_all_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tli2.sst");
        {
            let mut w = SstWriter::new(&path, Compression::Zstd, 3, 32, 300).unwrap();
            for i in 0..500u64 {
                w.add(format!("k{i:06}").as_bytes(), format!("v{i}").as_bytes(), i)
                    .unwrap();
            }
            w.finish().unwrap();
        }
        let mut r = SstReader::open_with_granularity(&path, 8).unwrap();
        // 抽查若干 key（跨多块），全部命中
        for i in [0u64, 1, 127, 128, 250, 333, 499] {
            let key = format!("k{i:06}");
            let v = r.get(key.as_bytes()).unwrap().unwrap().0.unwrap();
            assert_eq!(String::from_utf8_lossy(&v), format!("v{i}"));
        }
        // 未命中
        assert!(r.get(b"k999999").unwrap().is_none());
        // 范围扫描仍完整
        let mut seen = 0;
        r.scan_range(None, None, |_k, _v, _seq| seen += 1).unwrap();
        assert_eq!(seen, 500);
    }

    #[test]
    fn two_level_index_single_block_has_one_summary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tli3.sst");
        {
            let mut w = SstWriter::new(&path, Compression::Zstd, 3, 4096, 3).unwrap();
            for i in 0..3u64 {
                w.add(format!("a{i}").as_bytes(), b"x", i).unwrap();
            }
            w.finish().unwrap();
        }
        let r = SstReader::open_with_granularity(&path, 16).unwrap();
        assert_eq!(r.index_len(), 1);
        assert_eq!(r.summary_len(), 1, "单块也应有一条摘要（含首块）");
        assert!(!r.level2_loaded());
    }
}
