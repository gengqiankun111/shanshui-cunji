//! DocIdSet / LimitSpec —— 优化器统一 docid 集合抽象（阶段 A，research/optimizer_integration_design.md）。
//!
//! 职责边界：只负责"WHERE 收敛 → docid 集合"，不关心消费端（读路径回表输出 / 写路径批量
//! 删/更新 / JOIN 交集）。读/写/JOIN 共用同一定位抽象——DocIdSet 是"定位器"不是"执行器"。
//!
//! 形态取舍（重要，对齐引擎架构）：本引擎 scan API 全部是**回调式 push**（`scan_stream` /
//! `scan_stream_ids` 以闭包消费，不是 pull 迭代器）。设计文档中的 `Stream(Box<dyn Iterator>)`
//! 惰性流与"扫描借 `&Engine` + 消费需 `&mut Engine`"存在 Rust 借用硬冲突（design §9.2.3 D2）。
//! 故枚举收敛为**物化集合**（Bitmap / SortedList / Empty / All）；范围/全扫场景由 sqlish 层
//! 保留现有回调式 `scan_pushdown` / `post_filter` 路径（不经 DocIdSet 物化），仅在需要
//! "集合语义"（倒排 AND/OR、JOIN 交集、批量定位）时收敛为 Bitmap/SortedList。
//!
//! 内存上限：SortedList 物化 u64 docid（8B/项）；50 万行 ≈ 4MB，可控（对齐 SORT_MAX_ROWS 守卫
//! 思路——超限由调用方安全阀拦截）。

use roaring::treemap::RoaringTreemap as RoaringBitmap;

// ---------------------------------------------------------------------------
// LimitSpec：LIMIT + OFFSET 统一规格（like_offset_design 改动点 A）
// ---------------------------------------------------------------------------

/// LIMIT + OFFSET 规格。用于替代散落的 `Option<u64>`，统一管理 LIMIT 下推与 OFFSET 跳过。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LimitSpec {
    pub limit: Option<u64>,
    pub offset: u64,
}

impl LimitSpec {
    pub fn new(limit: Option<u64>, offset: u64) -> Self {
        Self { limit, offset }
    }

    /// 需要获取的总行数（下推用）= offset + limit。`LIMIT 10 OFFSET 1000` → 1010。
    pub fn total_to_fetch(&self) -> Option<u64> {
        self.limit.map(|l| self.offset + l)
    }

    /// 可早停判定（offset 较小，扫到 total 即停）。
    pub fn can_early_stop(&self) -> bool {
        self.limit.is_some() && self.offset < 10_000
    }

    /// 无限制（写定位：不可截断，须全量收敛）。
    pub fn unlimited() -> Self {
        Self { limit: None, offset: 0 }
    }
}

// ---------------------------------------------------------------------------
// DocIdSet：WHERE 收敛产出的 docid 集合
// ---------------------------------------------------------------------------

/// 统一 DocId 集合抽象（读路径回表 / 写路径批量删改 / JOIN 交集共用）。
#[derive(Debug, Clone)]
pub enum DocIdSet {
    /// 倒排/位图产出的文档位图（最紧凑，AND 极快）。
    Bitmap(RoaringBitmap),
    /// 有序 docid 列表（组合索引前缀扫描 / 主键区间物化；已升序）。
    SortedList(Vec<u64>),
    /// 空集（过滤条件直接冲突，如 status='active' AND status='inactive'）。
    Empty,
    /// 全集（无过滤条件）。
    All,
}

impl DocIdSet {
    /// 与另一个 DocIdSet 做交集（AND）。
    pub fn intersect(self, other: DocIdSet) -> DocIdSet {
        use DocIdSet::*;
        match (self, other) {
            (Empty, _) | (_, Empty) => Empty,
            (x, All) | (All, x) => x,
            (Bitmap(a), Bitmap(b)) => {
                let r = a & b;
                if r.is_empty() {
                    Empty
                } else {
                    Bitmap(r)
                }
            }
            (Bitmap(bm), SortedList(list)) => {
                // 遍历有序列表，在位图中查（列表小则 O(list)）
                let out: Vec<u64> = list.into_iter().filter(|d| bm.contains(*d)).collect();
                if out.is_empty() {
                    Empty
                } else {
                    SortedList(out)
                }
            }
            (SortedList(list), Bitmap(bm)) => {
                let out: Vec<u64> = list.into_iter().filter(|d| bm.contains(*d)).collect();
                if out.is_empty() {
                    Empty
                } else {
                    SortedList(out)
                }
            }
            (SortedList(a), SortedList(b)) => {
                // 两有序列表归并求交（均升序）
                let out = merge_intersect(&a, &b);
                if out.is_empty() {
                    Empty
                } else {
                    SortedList(out)
                }
            }
        }
    }

    /// 提取全部 docid 为 Vec（物化；Bitmap 升序、SortedList 保序）。
    pub fn to_vec(&self) -> Vec<u64> {
        match self {
            DocIdSet::Bitmap(bm) => bm.iter().collect(),
            DocIdSet::SortedList(v) => v.clone(),
            DocIdSet::Empty => Vec::new(),
            DocIdSet::All => Vec::new(), // All 不可物化（调用方须先收敛）
        }
    }

    /// 是否为空集。
    pub fn is_empty(&self) -> bool {
        match self {
            DocIdSet::Empty => true,
            DocIdSet::Bitmap(bm) => bm.is_empty(),
            DocIdSet::SortedList(v) => v.is_empty(),
            DocIdSet::All => false,
        }
    }

    /// 估算行数（安全阀 / 代价输入）。
    pub fn len_estimate(&self) -> u64 {
        match self {
            DocIdSet::Empty => 0,
            DocIdSet::Bitmap(bm) => bm.len(),
            DocIdSet::SortedList(v) => v.len() as u64,
            DocIdSet::All => u64::MAX, // 全集：未知大
        }
    }

    /// 迭代访问 docid（Bitmap 升序 / SortedList 保序 / Empty 空）。
    pub fn iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        match self {
            DocIdSet::Bitmap(bm) => Box::new(bm.iter()),
            DocIdSet::SortedList(v) => Box::new(v.iter().copied()),
            DocIdSet::Empty => Box::new(std::iter::empty()),
            DocIdSet::All => Box::new(std::iter::empty()), // All 不可迭代（须先收敛）
        }
    }
}

/// 两升序列表归并求交（O(a+b)）。
fn merge_intersect(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bm(items: &[u64]) -> RoaringBitmap {
        let mut b = RoaringBitmap::new();
        for &x in items {
            b.insert(x);
        }
        b
    }

    #[test]
    fn limit_spec_total_and_early_stop() {
        let s = LimitSpec::new(Some(10), 1000);
        assert_eq!(s.total_to_fetch(), Some(1010));
        assert!(s.can_early_stop()); // offset=1000 < 10000 → 可早停
        assert!(LimitSpec::new(Some(10), 9999).can_early_stop());
        assert!(!LimitSpec::new(Some(10), 10_000).can_early_stop());
        assert_eq!(LimitSpec::unlimited().limit, None);
        assert_eq!(LimitSpec::new(None, 0).total_to_fetch(), None);
        assert!(!LimitSpec::new(None, 0).can_early_stop(), "无 limit 不可早停");
    }

    #[test]
    fn intersect_bitmap_bitmap() {
        let a = DocIdSet::Bitmap(bm(&[1, 2, 3, 5]));
        let b = DocIdSet::Bitmap(bm(&[2, 3, 4]));
        match a.intersect(b) {
            DocIdSet::Bitmap(r) => {
                let v: Vec<u64> = r.iter().collect();
                assert_eq!(v, vec![2, 3]);
            }
            other => panic!("期望 Bitmap: {other:?}"),
        }
    }

    #[test]
    fn intersect_bitmap_sorted_list() {
        let a = DocIdSet::Bitmap(bm(&[1, 3, 5, 7, 9]));
        let b = DocIdSet::SortedList(vec![3, 4, 5, 9, 10]);
        match a.intersect(b) {
            DocIdSet::SortedList(v) => assert_eq!(v, vec![3, 5, 9]),
            other => panic!("期望 SortedList: {other:?}"),
        }
    }

    #[test]
    fn intersect_sorted_sorted_merge() {
        let a = DocIdSet::SortedList(vec![1, 3, 5, 7, 9]);
        let b = DocIdSet::SortedList(vec![2, 3, 5, 8]);
        match a.intersect(b) {
            DocIdSet::SortedList(v) => assert_eq!(v, vec![3, 5]),
            other => panic!("期望 SortedList: {other:?}"),
        }
    }

    #[test]
    fn intersect_empty_and_all() {
        let s = DocIdSet::SortedList(vec![1, 2]);
        assert!(matches!(s.clone().intersect(DocIdSet::Empty), DocIdSet::Empty));
        assert!(matches!(DocIdSet::Empty.intersect(s.clone()), DocIdSet::Empty));
        // All ∩ x = x
        match DocIdSet::All.intersect(s.clone()) {
            DocIdSet::SortedList(v) => assert_eq!(v, vec![1, 2]),
            other => panic!("All ∩ SortedList 应返回列表: {other:?}"),
        }
    }

    #[test]
    fn intersect_disjoint_becomes_empty() {
        let a = DocIdSet::Bitmap(bm(&[1, 2]));
        let b = DocIdSet::Bitmap(bm(&[3, 4]));
        assert!(matches!(a.intersect(b), DocIdSet::Empty));
    }

    #[test]
    fn to_vec_and_iter_order() {
        let d = DocIdSet::Bitmap(bm(&[10, 1, 5]));
        assert_eq!(d.to_vec(), vec![1, 5, 10]); // bitmap 升序
        assert_eq!(d.len_estimate(), 3);
        assert!(!d.is_empty());
        let gathered: Vec<u64> = d.iter().collect();
        assert_eq!(gathered, vec![1, 5, 10]);
        assert!(DocIdSet::Empty.is_empty());
        assert_eq!(DocIdSet::Empty.len_estimate(), 0);
        assert_eq!(DocIdSet::All.len_estimate(), u64::MAX);
    }
}
