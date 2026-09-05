//! Task-027：TinyLFU 读回填准入过滤器（Count-Min Sketch + Doorkeeper + 采样衰减）。
//!
//! 语义（research/cache_TinyLFU.md §二/§三，2026-09-05 定参）：
//! - 数组 depth=4 × width=512 × 4-bit 饱和计数器（≈1KB，远小于 HotCache 预算）；
//! - **首次访问不计入 CMS**（Doorkeeper 门卫：未见过 → 只入 doorkeeper，不计数），
//!   防止全表扫/长尾首访污染频率表；≥2 次访问才计入 CMS；
//! - **准入 = Estimate ≥ 阈值**（默认 4）：连续热读第 5 次起可回填，扫描型单次访问永不回填；
//! - **衰减**：累计 Record 样本 ≥ `reset_samples`（默认 width×depth=2048）全量计数器 >>1 并
//!   清空 doorkeeper——按“累计记录数”触发而非查询次数，高 QPS 自动更频繁（2048/读QPS 秒）；
//! - **写操作**（put/invalidate）：该 key 的 4 个计数减半 + 清 doorkeeper（CMS 无法精确删单 key，
//!   减半保留部分热度避免缓存饥饿；新写入本身直写回填不受准入限制）。
//!
//! 说明：Counter 用 u8 存 4-bit 饱和值（内存 ≈2KB，可接受；未来可压 4-bit 包装）。

use std::sync::Mutex;

/// Count-Min Sketch 行数（哈希函数数）。
pub(crate) const LFU_DEPTH: usize = 4;
/// Count-Min Sketch 每行列数。
pub(crate) const LFU_WIDTH: usize = 512;
/// 计数器数组长度 = depth × width。
pub(crate) const LFU_CELLS: usize = LFU_DEPTH * LFU_WIDTH;
/// Doorkeeper 容量（u64 × 32768 = 256KB，开放寻址线性探测）。
pub(crate) const DOORKEEPER_SLOTS: usize = 32_768;

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// 第 row 路哈希 → 列下标（行内 0..width）。
fn col_of(docid: u64, row: usize) -> usize {
    let h = splitmix64(docid.wrapping_add((row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)));
    (h % LFU_WIDTH as u64) as usize
}

/// 可删除的 Doorkeeper（开放寻址 + 墓碑删除；负载过高时重建清理墓碑）。
struct Doorkeeper {
    slots: Box<[u64]>,
    used: usize,
}

const EMPTY: u64 = 0; // docid 0 保留不用（docid 从 1 起，见 encode_docid 约定）
const DELETED: u64 = u64::MAX;

impl Doorkeeper {
    fn new() -> Self {
        Self { slots: vec![EMPTY; DOORKEEPER_SLOTS].into_boxed_slice(), used: 0 }
    }
    fn idx_of(&self, docid: u64) -> usize {
        (splitmix64(docid) as usize) & (self.slots.len() - 1)
    }
    /// 负载（含墓碑）超 70% → 重建去墓碑；仍满（活跃 ≥85%）→ 清空保活。
    fn maybe_rebuild(&mut self) {
        let cap = self.slots.len();
        let active = self.used; // used = 活跃（含墓碑为 deleted 单独计）
        if active as f64 / cap as f64 > 0.70 {
            let mut next: Vec<u64> = Vec::with_capacity(cap);
            let mut cnt = 0usize;
            for &s in self.slots.iter() {
                if s != EMPTY && s != DELETED {
                    // 简单插入保序
                    let mut i = (splitmix64(s) as usize) & (cap - 1);
                    loop {
                        if next.len() <= i {
                            next.resize(i + 1, EMPTY);
                        }
                        if next[i] == EMPTY {
                            next[i] = s;
                            cnt += 1;
                            break;
                        }
                        i = (i + 1) & (cap - 1);
                    }
                }
            }
            let l = cap.max(next.len());
            next.resize(l, EMPTY);
            if (cnt as f64 / cap as f64) > 0.85 {
                self.slots = vec![EMPTY; cap].into_boxed_slice(); // 清空保活
                self.used = 0;
            } else {
                self.slots = next.into_boxed_slice();
                self.used = cnt;
            }
        }
    }
    fn insert(&mut self, docid: u64) {
        self.maybe_rebuild();
        let cap = self.slots.len();
        let mut i = self.idx_of(docid);
        loop {
            match self.slots[i] {
                EMPTY | DELETED => {
                    self.slots[i] = docid;
                    self.used += 1;
                    return;
                }
                d if d == docid => return, // 已存在
                _ => {}
            }
            i = (i + 1) & (cap - 1);
        }
    }
    fn contains(&self, docid: u64) -> bool {
        let cap = self.slots.len();
        let mut i = self.idx_of(docid);
        loop {
            match self.slots[i] {
                EMPTY => return false,
                d if d == docid => return true,
                _ => {}
            }
            i = (i + 1) & (cap - 1);
        }
    }
    fn remove(&mut self, docid: u64) {
        let cap = self.slots.len();
        let mut i = self.idx_of(docid);
        loop {
            match self.slots[i] {
                EMPTY => return,
                d if d == docid => {
                    self.slots[i] = DELETED;
                    self.used = self.used.saturating_sub(1);
                    return;
                }
                _ => {}
            }
            i = (i + 1) & (cap - 1);
        }
    }
    fn clear(&mut self) {
        self.slots.fill(EMPTY);
        self.used = 0;
    }
}

/// TinyLFU 准入门（线程安全，独立于 HotCache 主 RwLock——读路径计数互不阻塞）。
pub(crate) struct TinyLfu {
    inner: Mutex<TinyLfuInner>,
}

struct TinyLfuInner {
    cells: Box<[u8]>,
    samples: u32,
    reset_samples: u32,
    dk: Doorkeeper,
}

impl TinyLfu {
    pub(crate) fn new(reset_samples: u32) -> Self {
        Self {
            inner: Mutex::new(TinyLfuInner {
                cells: vec![0u8; LFU_CELLS].into_boxed_slice(),
                samples: 0,
                reset_samples: reset_samples.max(1),
                dk: Doorkeeper::new(),
            }),
        }
    }

    fn incr(g: &mut TinyLfuInner, docid: u64) {
        for row in 0..LFU_DEPTH {
            let i = row * LFU_WIDTH + col_of(docid, row);
            let c = &mut g.cells[i];
            if *c < 15 {
                *c += 1;
            }
        }
    }

    fn estimate(g: &TinyLfuInner, docid: u64) -> u32 {
        let mut min = u8::MAX;
        for row in 0..LFU_DEPTH {
            let i = row * LFU_WIDTH + col_of(docid, row);
            min = min.min(g.cells[i]);
        }
        min as u32
    }

    /// 读回填准入判定（含副作用）：
    /// 首次访问 → 只入 doorkeeper（返回 false，不计入 CMS）；再次访问起计入 CMS 并返回当前
    /// 估计值 ≥ admit 则准入（true）。
    pub(crate) fn record_admit(&self, docid: u64, admit: u32) -> bool {
        let mut g = self.inner.lock().unwrap();
        if !g.dk.contains(docid) {
            g.dk.insert(docid); // 首次访问：门卫记账，不污染 CMS
            return false;
        }
        Self::incr(&mut g, docid);
        g.samples = g.samples.saturating_add(1);
        let est = Self::estimate(&g, docid);
        if g.samples >= g.reset_samples {
            // 采样衰减：全量 >>1 + 清 doorkeeper（“最近热度窗口”滑动）
            for c in g.cells.iter_mut() {
                *c >>= 1;
            }
            g.samples = 0;
            g.dk.clear();
        }
        est >= admit
    }

    /// 写操作：该 key 计数减半 + 清 doorkeeper（保留部分热度；后续读按新 key 重新门卫）。
    pub(crate) fn on_write(&self, docid: u64) {
        let mut g = self.inner.lock().unwrap();
        g.dk.remove(docid);
        for row in 0..LFU_DEPTH {
            let i = row * LFU_WIDTH + col_of(docid, row);
            g.cells[i] >>= 1;
        }
    }
}
