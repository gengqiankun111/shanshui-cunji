//! P94：热列旁路双轨 —— colstore 内存列存（行式主副本之外的热列旁路，惰性派生）
//!
//! 设计（research/dual_track_colstore.md M3）：`storage.colstore_enabled=true` 且 hot_fields 非空时，
//! 首次需要时从行式主**派生**一份内存列存：docid 数组 + 每热列一个“ends 游标 + present 位图 +
//! blob”独立区域（零拷贝按列读取，排序/投影扫描只解目标列，与 M2 demo 布局一致）。
//!
//! 正确性模型（保守，宁慢勿错）：
//! - colstore 只服务 `docid ≤ watermark`（派生时最大 docid）且未删除、未在派生后重写的行；
//! - 派生后对旧行的任何写（put/delete）经 `colstore_note_write` 记入 dirty —— 查询命中 dirty 区间
//!   即**整查询回退行式主**（正确性优先，扫描路径与现状完全一致）；
//! - 删除位图跳过派生后被删的行；派生前已删的行在派生快照（最新视图）中本就不存在；
//! - watermark 之后新增的行（派生后新写入的更大 docid）不在 colstore —— 整查询回退行式主。
//! - 派生失败/开关关闭 → 一律回退行式（零回归：默认 `colstore_enabled=false`）。
//!
//! 说明：本模块为阶段①最小版（内存常驻、重启重建），落盘/增量派生/EXPLAIN 标注留 M3 后续阶段。

use std::sync::Arc;

use roaring::treemap::RoaringTreemap as RoaringBitmap;

use crate::engine::Engine;
use crate::error::Result;

/// 单列区域：每行值字节连续进 blob，ends[i] = 第 i 行**结束**游标（ends[i-1]=起点，首行起点 0）；
/// present 位图标记该行有值（缺列/JSON null → 位 0，值字节 0）。
#[derive(Default)]
pub(crate) struct ColArena {
    ends: Vec<u32>,
    present: Vec<u64>,
    blob: Vec<u8>,
}

impl ColArena {
    fn push_none(&mut self) {
        self.ends.push(self.blob.len() as u32);
    }

    fn push_value(&mut self, bytes: &[u8], row_idx: u64) {
        let bit = row_idx as usize;
        if self.present.len() * 64 <= bit {
            self.present.push(0);
        }
        self.present[bit / 64] |= 1u64 << (bit % 64);
        self.blob.extend_from_slice(bytes);
        self.ends.push(self.blob.len() as u32);
    }

    fn value_at(&self, row_idx: usize) -> Option<&[u8]> {
        let bit = row_idx as usize;
        if bit >= self.ends.len() || (self.present[bit / 64] >> (bit % 64)) & 1 == 0 {
            return None;
        }
        let start = if bit == 0 { 0 } else { self.ends[bit - 1] as usize };
        let end = self.ends[bit] as usize;
        Some(&self.blob[start..end])
    }
}

/// colstore 派生列存（行序与 docids 对齐）。
pub(crate) struct Colstore {
    pub docids: Vec<u64>,
    pub names: Vec<String>,
    pub cols: Vec<ColArena>,
}

/// 行视图：零拷贝按列取该行值（JSON 值字节：字符串带引号、数值裸——同 `field_bytes_to_sort_key` 输入）。
pub(crate) struct ColRow<'a> {
    cs: &'a Colstore,
    idx: usize,
}

impl<'a> ColRow<'a> {
    pub(crate) fn field(&self, col_idx: usize) -> Option<&[u8]> {
        self.cs.cols.get(col_idx).and_then(|a| a.value_at(self.idx))
    }
}

/// colstore 状态（引擎级；惰性派生 + 覆盖合并守卫）。
#[derive(Default)]
pub(crate) struct ColstoreState {
    pub cs: Option<Arc<Colstore>>,
    pub watermark: u64,
    pub dirty: RoaringBitmap,
}

// ---------- 字节级顶层字段抽取（派生用；免整行 serde Value 构建） ----------

fn ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\r' | b'\n') {
        i += 1;
    }
    i
}

/// `i` 指向 `"`，返回跳过该字符串（含转义）后的下标；畸形 → None。
fn str_end(b: &[u8], mut i: usize) -> Option<usize> {
    if b.get(i) != Some(&b'"') {
        return None;
    }
    i += 1;
    while i < b.len() {
        match b[i] {
            b'\\' => i += 2,
            b'"' => return Some(i + 1),
            _ => i += 1,
        }
    }
    None
}

/// `i` 指向值 token 起点，返回该值结束下标（容器/字符串平衡；裸 token 扫到分隔符）；畸形 → None。
fn val_end(b: &[u8], mut i: usize) -> Option<usize> {
    if i >= b.len() {
        return None;
    }
    match b[i] {
        b'"' => str_end(b, i),
        b'{' | b'[' => {
            let open = b[i];
            let close = if open == b'{' { b'}' } else { b']' };
            let mut depth = 0usize;
            while i < b.len() {
                match b[i] {
                    b'"' => {
                        i = str_end(b, i)?;
                    }
                    c if c == open => {
                        depth += 1;
                        i += 1;
                    }
                    c if c == close => {
                        depth -= 1;
                        i += 1;
                        if depth == 0 {
                            return Some(i);
                        }
                    }
                    _ => i += 1,
                }
            }
            None
        }
        _ => {
            while i < b.len() && !matches!(b[i], b',' | b'}' | b']' | b' ' | b'\t' | b'\r' | b'\n') {
                i += 1;
            }
            Some(i)
        }
    }
}

/// 单遍抽取对象顶层字段原文 token。返回 `Some(vals)`：`vals[ci]` = 该列值原文切片
/// （JSON null/缺列 → None；重复键取**最后**一个，对齐 serde_json 覆盖语义）。
/// 结构畸形（非对象/转义错/截断）→ `None`（调用方回退整行 serde 解析，保既有派生语义）。
fn light_top_fields<'a>(doc: &'a [u8], names: &[String]) -> Option<Vec<Option<&'a [u8]>>> {
    let n = doc.len();
    let mut i = ws(doc, 0);
    if i >= n || doc[i] != b'{' {
        return None;
    }
    i += 1;
    let mut vals: Vec<Option<&'a [u8]>> = vec![None; names.len()];
    loop {
        i = ws(doc, i);
        if i >= n {
            return None;
        }
        if doc[i] == b'}' {
            break;
        }
        let key_end = str_end(doc, i)?; // 闭引号后
        let key = &doc[i + 1..key_end - 1];
        i = ws(doc, key_end);
        if i >= n || doc[i] != b':' {
            return None;
        }
        i = ws(doc, i + 1);
        if i >= n {
            return None;
        }
        let ve = val_end(doc, i)?;
        let raw = &doc[i..ve];
        for (ci, nm) in names.iter().enumerate() {
            if nm.as_bytes() == key {
                vals[ci] = if raw == b"null" { None } else { Some(raw) };
                break;
            }
        }
        i = ws(doc, ve);
        if i >= n {
            return None;
        }
        match doc[i] {
            b',' => i += 1,
            b'}' => break,
            _ => return None,
        }
    }
    Some(vals)
}

impl Engine {
    /// P94：colstore 可用（开关开且声明了热列）。
    pub(crate) fn colstore_enabled(&self) -> bool {
        self.colstore_enabled && !self.colstore_hot.is_empty()
    }

    /// P94：派生后对旧行（≤ 水位）的写记脏 → 命中区间整查询回退行式主。
    pub(crate) fn colstore_note_write(&self, docid: u64) {
        if !self.colstore_enabled() {
            return;
        }
        let mut st = self.colstore_state.lock().unwrap();
        if st.cs.is_some() && docid <= st.watermark {
            st.dirty.insert(docid);
        }
    }

    /// P94：确保已派生（惰性；派生失败静默 → 走行式，保守不报错）。
    pub(crate) fn colstore_ensure(&self) {
        if !self.colstore_enabled() {
            return;
        }
        {
            let st = self.colstore_state.lock().unwrap();
            if st.cs.is_some() {
                return;
            }
        }
        // 派生：最新视图全表扫描，逐行抽 hot 列原文进各列区域。P94④：行级用字节级顶层字段
        // 抽取（免整行 serde Value 构建——1.1M×25 列整行 parse ~27s 一次性冷启成本 → ~秒级）；
        // 结构无法轻量遍历（畸形/非对象）→ 该行回退 serde 全解析（语义护栏，等价既有路径）。
        let hot = self.colstore_hot.clone();
        let mut docids: Vec<u64> = Vec::new();
        let mut wm = 0u64;
        let mut bld: Vec<ColArena> = std::iter::repeat_with(ColArena::default)
            .take(hot.len())
            .collect();
        let res = self.scan_stream_fields(None, None, hot.clone(), |docid, doc| {
            docids.push(docid);
            wm = docid;
            let row_idx = (docids.len() - 1) as u64;
            match light_top_fields(doc, &hot) {
                // 抽到全部热列原文（含 null → None）；缺失列 → None
                Some(vals) => {
                    for (ci, a) in bld.iter_mut().enumerate() {
                        match vals[ci] {
                            Some(bytes) => a.push_value(bytes, row_idx),
                            None => a.push_none(),
                        }
                    }
                }
                // 结构异常 → 回退整行 serde（与既有派生语义一致）
                None => {
                    let v: serde_json::Value = match serde_json::from_slice(doc) {
                        Ok(v) => v,
                        Err(_) => {
                            for a in bld.iter_mut() {
                                a.push_none();
                            }
                            return Ok(true);
                        }
                    };
                    for (ci, a) in bld.iter_mut().enumerate() {
                        let name = hot[ci].as_str();
                        let val = match v.get(name) {
                            Some(x) if !x.is_null() => serde_json::to_vec(x).ok(),
                            _ => None,
                        };
                        match val {
                            None => a.push_none(),
                            Some(bytes) => a.push_value(&bytes, row_idx),
                        }
                    }
                }
            }
            Ok(true)
        });
        if res.is_err() {
            return; // 派生失败：保持 None → 行式回退
        }
        let cs = Arc::new(Colstore {
            docids,
            names: hot,
            cols: bld,
        });
        let mut st = self.colstore_state.lock().unwrap();
        if st.cs.is_none() {
            st.cs = Some(cs);
            st.watermark = wm;
        }
    }

    /// P94：请求排序/投影列 → colstore 列下标（含触发派生；任一列不在热列 → None = 行式）。
    pub(crate) fn colstore_field_indices(&self, fields: &[String]) -> Option<Vec<usize>> {
        self.colstore_ensure();
        let st = self.colstore_state.lock().unwrap();
        let cs = st.cs.as_ref()?;
        let mut out = Vec::with_capacity(fields.len());
        for f in fields {
            out.push(cs.names.iter().position(|n| n == f)?);
        }
        Some(out)
    }

    /// P94：colstore 区间扫描（只解目标列）。返回值：
    /// - Ok(true)  = 已由 colstore 服务（回调逐行触发；删除位图跳过已删行）；
    /// - Ok(false) = 不适用（未派生/超水位/区间含脏行/范围内无数据）→ 调用方须走行式主路径。
    pub(crate) fn colstore_try_scan_cols<F>(&self, lo: u64, hi: u64, mut f: F) -> Result<bool>
    where
        F: FnMut(u64, &ColRow<'_>) -> Result<bool>,
    {
        self.colstore_ensure();
        let st = self.colstore_state.lock().unwrap();
        let cs = match &st.cs {
            Some(c) => c,
            None => return Ok(false),
        };
        if st.watermark < hi {
            return Ok(false); // 派生后新增（更大 docid）行：交回行式
        }
        let n = cs.docids.len();
        if n == 0 {
            return Ok(false);
        }
        let start = cs.docids.partition_point(|&d| d < lo);
        if start >= n {
            return Ok(false);
        }
        let mut served = false;
        for i in start..n {
            let d = cs.docids[i];
            if d > hi {
                break;
            }
            if st.dirty.contains(d) {
                return Ok(false); // 脏行（派生后重写）：整查询回退行式，宁慢勿错
            }
            if let Some(bm) = &self.deletion_bitmap {
                if bm.is_deleted(d) {
                    continue;
                }
            }
            served = true;
            let row = ColRow { cs, idx: i };
            if !f(d, &row)? {
                break;
            }
        }
        Ok(served)
    }

    /// P94③：All 分支位图直供——colstore 已派生、无脏、且无 > 水位的新可见行时，直接用
    /// `cs.docids`（最新视图派生快照）构建 Roaring 位图，**免 `full_docids` primary 全扫物化
    /// 整行**（All→ORDER BY 大窗时该物化是主开销：100k ~85ms，110 万 ~GB 级）。不满足任一
    /// 守卫 → None，调用方走原 full_docids（零行为差异）。
    pub(crate) fn colstore_all_bitmap(&self) -> Option<RoaringBitmap> {
        if !self.colstore_enabled() {
            return None;
        }
        let st = self.colstore_state.lock().unwrap();
        let cs = st.cs.as_ref()?;
        if !st.dirty.is_empty() {
            return None; // 有脏行：全量可见集 ≠ cs.docids
        }
        if cs.docids.is_empty() {
            return Some(RoaringBitmap::new());
        }
        let wm = st.watermark;
        if wm == u64::MAX {
            return Some(docid_bitmap(&cs.docids));
        }
        // 窥视 > wm 是否存在可见行（scan_stream 已滤删除位图）：存在 → colstore 未覆盖全表
        // 锁保持期间窥视（阻止写者并发插入 dirty，保证判定与位图原子一致；读路径不取引擎写锁，无死锁环）
        let mut newer = false;
        if self
            .scan_stream(Some(wm + 1), None, |_d, _v| {
                newer = true;
                Ok(false) // 首个即停
            })
            .is_err()
        {
            return None; // 扫描失败保守回退
        }
        if newer {
            return None;
        }
        Some(docid_bitmap(&cs.docids))
    }
}

/// cs.docids → RoaringTreemap（逐 docid 插入，升序已有序但位图合并同复杂度）。
fn docid_bitmap(docids: &[u64]) -> RoaringBitmap {
    let mut b = RoaringBitmap::new();
    for &d in docids {
        b.insert(d);
    }
    b
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::engine::Engine;

    fn doc_json(id: u64, a: &str, b: u64) -> Vec<u8> {
        serde_json::json!({ "id": id, "a": a, "b": b }).to_string().into_bytes()
    }

    fn open_cs() -> (tempfile::TempDir, Engine) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let mut cfg = Config::default();
        cfg.storage.hot_fields = vec!["a".into(), "b".into()];
        cfg.storage.colstore_enabled = true;
        (dir, Engine::open(&path, &cfg).unwrap())
    }

    /// 行式参考：最新视图 (docid, 全行 JSON) 的字段值字节（None = 缺列/JSON null）。
    fn row_field(e: &Engine, col: &str) -> Vec<Option<Vec<u8>>> {
        let mut out = Vec::new();
        e.scan_stream(None, None, |_d, v| {
            let val: serde_json::Value = serde_json::from_slice(v).unwrap();
            // 与 colstore 存储语义一致：非 null 值存 JSON 序列化字节；缺列/null → None
            out.push(match val.get(col) {
                Some(x) if !x.is_null() => serde_json::to_vec(x).ok(),
                _ => None,
            });
            Ok(true)
        })
        .unwrap();
        out
    }

    #[test]
    fn light_top_fields_matches_serde_semantics() {
        // 与 serde_json 语义对齐：重复键取最后、转义字符串保留原文、嵌套容器整块原文、
        // JSON null/缺列 → None
        let names: Vec<String> = ["a", "b", "c", "s"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let doc = br#"{"a":1,"b":"x\"y","c":{"z":[1,2]},"nested":{"a":99},"s":null,"a":7}"#;
        let vals = light_top_fields(doc, &names).expect("结构合法应抽取成功");
        let want: Vec<Option<&[u8]>> = vec![
            Some(b"7"),          // 重复键取最后一个（serde 覆盖语义）
            Some(b"\"x\\\"y\""), // 转义字符串原文保留（含引号/反斜杠）
            Some(b"{\"z\":[1,2]}"), // 嵌套容器整块原文（顶层字段 c）
            None,                // null → None
        ];
        assert_eq!(vals, want, "light 抽取与 serde 语义对齐");
        // 嵌套同名键（nested.a）不得命中顶层 a 抽取（本行 a 取 1 后再由重复 7 覆盖，跳过嵌套对象）
        let doc2 = br#"{"a":1,"b":2,"nested":{"a":99},"s":"ok"}"#;
        let vals2 = light_top_fields(doc2, &names).unwrap();
        assert_eq!(vals2[0], Some(&b"1"[..]), "嵌套内同名键不干扰顶层");
        assert_eq!(vals2[3], Some(&b"\"ok\""[..]));
        // 畸形 → None（回退 serde）
        assert!(light_top_fields(br#"{"a":1,"b""#, &names).is_none());
    }

    #[test]
    fn derive_and_scan_matches_row_path() {
        let (_d, mut e) = open_cs();
        for i in 1..=200u64 {
            let doc = if i == 7 {
                serde_json::json!({ "id": i, "a": "x", "b": null }).to_string().into_bytes()
            } else {
                doc_json(i, &format!("k{}", i % 7), i * 3)
            };
            e.put(i, doc, &[]).unwrap();
        }
        e.flush_primary().unwrap();
        let idxs = e.colstore_field_indices(&["a".into(), "b".into()]).expect("热列命中");
        assert_eq!(idxs.len(), 2);
        // colstore 行序与最新视图一致，且逐值 == 行式解析（含 JSON null → None）
        let want_a = row_field(&e, "a");
        let want_b = row_field(&e, "b");
        let mut got_a: Vec<Option<Vec<u8>>> = Vec::new();
        let mut got_b: Vec<Option<Vec<u8>>> = Vec::new();
        let mut seen = 0u64;
        let served = e
            .colstore_try_scan_cols(1, 200, |docid, row| {
                seen += 1;
                assert_eq!(docid, seen, "docid 升序对齐");
                got_a.push(row.field(0).map(|b| b.to_vec()));
                got_b.push(row.field(1).map(|b| b.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert!(served);
        assert_eq!(got_a.len(), 200);
        assert_eq!(got_a, want_a, "a 列与行式一致");
        assert_eq!(got_b, want_b, "b 列与行式一致（第 7 行 JSON null → None）");
    }

    #[test]
    fn delete_before_derive_excluded_and_after_derive_falls_back() {
        let (_d, mut e) = open_cs();
        for i in 1..=100u64 {
            e.put(i, doc_json(i, "k", i), &[]).unwrap();
        }
        e.flush_primary().unwrap();
        // 派生前删除 → 快照即最新视图，不含该 docid
        e.delete(50).unwrap();
        let _ = e.colstore_field_indices(&["a".into()]).unwrap();
        let mut ids = Vec::new();
        e.colstore_try_scan_cols(1, 100, |d, _row| {
            ids.push(d);
            Ok(true)
        })
        .unwrap();
        assert!(!ids.contains(&50), "派生前删除的行不在 colstore");
        // 派生后删除 → 记脏 → 整查询回退行式（Ok(false)）
        e.delete(60).unwrap();
        let served = e
            .colstore_try_scan_cols(1, 100, |_d, _row| Ok(true))
            .unwrap();
        assert!(!served, "派生后删除命中脏区间须回退行式");
    }

    #[test]
    fn new_docid_beyond_watermark_falls_back() {
        let (_d, mut e) = open_cs();
        for i in 1..=50u64 {
            e.put(i, doc_json(i, "k", i), &[]).unwrap();
        }
        let _ = e.colstore_field_indices(&["a".into()]).unwrap();
        // 派生后新增更大 docid → 查询上界超水位 → 回退行式
        e.put(1000, doc_json(1000, "new", 1), &[]).unwrap();
        let served = e
            .colstore_try_scan_cols(1, 1000, |_d, _row| Ok(true))
            .unwrap();
        assert!(!served, "上界超过派生水位须回退行式");
        // 但只查已覆盖区间仍服务（新增行不在区间）
        let served2 = e
            .colstore_try_scan_cols(1, 50, |_d, _row| Ok(true))
            .unwrap();
        assert!(served2, "已覆盖区间仍由 colstore 服务");
    }

    /// All 位图直供：全 docid（含删除位图/最新视图）快照一致。
    #[test]
    fn all_bitmap_covers_full_visible_set_when_static() {
        let (_d, mut e) = open_cs();
        for i in 1..=100u64 {
            e.put(i, doc_json(i, "k", i), &[]).unwrap();
        }
        e.flush_primary().unwrap();
        e.delete(33).unwrap(); // 派生前删除 → 最新视图不含 33
        let _ = e.colstore_field_indices(&["a".into()]).expect("热列命中（触发派生）");
        let want: Vec<u64> = (1..=100u64).filter(|&i| i != 33).collect();
        let bm = e.colstore_all_bitmap().expect("静态全表应可直供位图");
        let got: Vec<u64> = bm.iter().collect();
        assert_eq!(got, want, "cs docid 位图 == 最新可见全集（含派生前删除）");
    }

    /// 守卫：派生后有超水位新行 / 有脏行 / 未启用 → None（交回 full_docids）。
    #[test]
    fn all_bitmap_guards_new_rows_dirty_and_disabled() {
        // ① 派生后新增更大 docid → None
        let (_d, mut e) = open_cs();
        for i in 1..=30u64 {
            e.put(i, doc_json(i, "k", i), &[]).unwrap();
        }
        let _ = e.colstore_field_indices(&["a".into()]).unwrap();
        e.put(9999, doc_json(9999, "new", 1), &[]).unwrap();
        assert!(e.colstore_all_bitmap().is_none(), "超水位新行 → 须回退");

        // ② 派生后重写旧行（脏）→ None
        let (_d2, mut e2) = open_cs();
        for i in 1..=30u64 {
            e2.put(i, doc_json(i, "k", i), &[]).unwrap();
        }
        let _ = e2.colstore_field_indices(&["a".into()]).unwrap();
        e2.put(5, doc_json(5, "k2", 5), &[]).unwrap();
        assert!(e2.colstore_all_bitmap().is_none(), "脏行 → 须回退");

        // ③ 派生后删除旧行（脏）→ None
        let (_d3, mut e3) = open_cs();
        for i in 1..=30u64 {
            e3.put(i, doc_json(i, "k", i), &[]).unwrap();
        }
        let _ = e3.colstore_field_indices(&["a".into()]).unwrap();
        e3.delete(7).unwrap();
        assert!(e3.colstore_all_bitmap().is_none(), "派生后删除 → 须回退");

        // ④ 默认（未启用）→ None
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let mut cfg = Config::default();
        cfg.storage.hot_fields = vec!["a".into()]; // 热列声明但开关关
        let e4 = Engine::open(&path, &cfg).unwrap();
        assert!(e4.colstore_all_bitmap().is_none(), "未启用 colstore → None");
    }
}
