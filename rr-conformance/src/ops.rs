//! RR 对照操作模型：两张表（t_test 单主键 / t_combo 主键 + 二级 a,b），
//! 12 类操作全集；事务级 ops 生成（70% 单点 / 30% 范围·批量；单点写类按锁 key 升序重排
//! 减少随机死锁干扰；range/batch 原样下发、不重排、允许死锁只采集）。
//! **键空间按 worker 分区（Seg）**：每个 worker 只访问自己独占的键区 → 双端各自的多
//! worker 独立调度不产生跨事务键竞争（否则同引擎两库并发也会合法分叉）。
//! 注意（本方案边界）：进程在 mysql.commit 与 mydb.commit 之间被 kill 会两库不一致（接受）；
//! ODKU 暂不纳入（方案注明后续再加）。

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::Rng;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum Table {
    TTest,  // 单列主键 id：单点行锁
    TCombo, // 主键 id + 唯一索引 idx_ab(a,b)：二级等值/范围/间隙
}
impl Table {
    pub fn name(&self) -> &'static str {
        match self {
            Table::TTest => "t_test",
            Table::TCombo => "t_combo",
        }
    }
}

/// 行键：单点操作落点（t_test 主键 id；t_combo 二级唯一 (a,b)）
#[derive(Clone, Copy, Debug)]
pub enum Key {
    Id(u32),
    Ab(u32, u32),
}

/// 校验/范围列通道：主键 id 或二级 a（t_combo 范围走 a BETWEEN 触发二级索引区间）
#[derive(Clone, Copy, Debug)]
pub enum Col {
    Pk,
    A,
}

#[derive(Clone, Debug)]
pub enum Op {
    PointSel { t: Table, k: Key },
    PointFu { t: Table, k: Key }, // 当前读（锁）
    Insert { t: Table, id: u32, a: u32, b: u32 },
    Update { t: Table, id: u32 },
    Delete { t: Table, id: u32 },
    RangeSel { t: Table, on: Col, lo: u32, hi: u32 },
    RangeFu { t: Table, on: Col, lo: u32, hi: u32 },
    BatchIns { t: Table, items: Vec<(u32, u32, u32)> },
    BatchSel { t: Table, ids: Vec<u32> },
    BatchUpd { t: Table, ids: Vec<u32> },
    BatchDel { t: Table, ids: Vec<u32> },
}

fn table_cols(t: Table) -> &'static str {
    match t {
        Table::TTest => "id,val",
        Table::TCombo => "id,a,b,val",
    }
}

/// 操作主 SQL（两边同一文本原样下发）。
impl Op {
    pub fn sql(&self) -> String {
        match self {
            Op::PointSel { t, k } => format!(
                "SELECT {} FROM {} WHERE {}",
                table_cols(*t),
                t.name(),
                key_cond(*k)
            ),
            Op::PointFu { t, k } => format!(
                "SELECT {} FROM {} WHERE {} FOR UPDATE",
                table_cols(*t),
                t.name(),
                key_cond(*k)
            ),
            Op::Insert { t, id, a, b } => match t {
                Table::TTest => format!("INSERT INTO t_test(id,val) VALUES({id},1)"),
                Table::TCombo => format!("INSERT INTO t_combo(id,a,b,val) VALUES({id},{a},{b},1)"),
            },
            Op::Update { t, id } => format!("UPDATE {} SET val=val+1 WHERE id={id}", t.name()),
            Op::Delete { t, id } => format!("DELETE FROM {} WHERE id={id}", t.name()),
            Op::RangeSel { t, on, lo, hi } => {
                format!(
                    "SELECT {} FROM {} WHERE {} BETWEEN {lo} AND {hi} ORDER BY {}",
                    table_cols(*t),
                    t.name(),
                    col_expr(*on),
                    col_order(*on)
                )
            }
            Op::RangeFu { t, on, lo, hi } => {
                format!(
                    "SELECT {} FROM {} WHERE {} BETWEEN {lo} AND {hi} ORDER BY {} FOR UPDATE",
                    table_cols(*t),
                    t.name(),
                    col_expr(*on),
                    col_order(*on)
                )
            }
            Op::BatchIns { t, items } => {
                let rows: Vec<String> = items
                    .iter()
                    .map(|(id, a, b)| match t {
                        Table::TTest => format!("({id},1)"),
                        Table::TCombo => format!("({id},{a},{b},1)"),
                    })
                    .collect();
                match t {
                    Table::TTest => format!(
                        "INSERT INTO t_test(id,val) VALUES {}",
                        rows.join(",")
                    ),
                    Table::TCombo => format!(
                        "INSERT INTO t_combo(id,a,b,val) VALUES {}",
                        rows.join(",")
                    ),
                }
            }
            Op::BatchSel { t, ids } => format!(
                "SELECT {} FROM {} WHERE id IN ({}) ORDER BY id",
                table_cols(*t),
                t.name(),
                join_ids(ids)
            ),
            Op::BatchUpd { t, ids } => {
                format!("UPDATE {} SET val=val+1 WHERE id IN ({})", t.name(), join_ids(ids))
            }
            Op::BatchDel { t, ids } => {
                format!("DELETE FROM {} WHERE id IN ({})", t.name(), join_ids(ids))
            }
        }
    }

    /// Try 阶段预加锁语句（当前读）。DML 转成其作用行的 SELECT FOR UPDATE；
    /// range/batch 同样取 FOR UPDATE 形态（原样条件，不拆分）。
    /// 注意：MySQL 在**无索引列**（t_combo 的 a / (a,b)）上 FOR UPDATE 走全表 next-key 锁，
    /// 锁会扩散到其它 worker 的键区 → 跨 worker 死锁（一侧被回滚另一侧未回滚 = 双端合法分叉，
    /// 2000 事务标定 DIFF/终态不一致的根因）。因此非主键谓词的普通 SELECT：TRY 不加锁，
    /// 纯快照读（其行集自限本 worker 键区，两侧一致）；锁路径只保留主键 id 作用域。
    pub fn try_fu_sql(&self) -> String {
        match self {
            // 非主键谓词：无索引 → 退回纯快照读，不预锁
            Op::PointSel { k: Key::Ab(..), .. } | Op::RangeSel { on: Col::A, .. } => self.sql(),
            Op::PointFu { t, k } => self.sql(),
            Op::PointSel { t, k } => format!(
                "SELECT {} FROM {} WHERE {} FOR UPDATE",
                table_cols(*t),
                t.name(),
                key_cond(*k)
            ),
            Op::Insert { t, id, .. } => match t {
                // 预锁只取主键（(a,b) 无索引，连带判断会全表锁）
                Table::TTest => format!("SELECT id,val FROM t_test WHERE id={id} FOR UPDATE"),
                Table::TCombo => {
                    format!("SELECT id,a,b,val FROM t_combo WHERE id={id} FOR UPDATE")
                }
            },
            Op::Update { t, id } => {
                format!("SELECT {} FROM {} WHERE id={id} FOR UPDATE", table_cols(*t), t.name())
            }
            Op::Delete { t, id } => {
                format!("SELECT {} FROM {} WHERE id={id} FOR UPDATE", table_cols(*t), t.name())
            }
            Op::RangeSel { t, on, lo, hi } | Op::RangeFu { t, on, lo, hi } => {
                format!(
                    "SELECT {} FROM {} WHERE {} BETWEEN {lo} AND {hi} ORDER BY {} FOR UPDATE",
                    table_cols(*t),
                    t.name(),
                    col_expr(*on),
                    col_order(*on)
                )
            }
            Op::BatchSel { t, ids } => format!(
                "SELECT {} FROM {} WHERE id IN ({}) ORDER BY id FOR UPDATE",
                table_cols(*t),
                t.name(),
                join_ids(ids)
            ),
            Op::BatchIns { t, items } => {
                // 批量插入：仅对目标主键 id 预锁（新行池 per-worker 独占，(a,b) 无索引不锁）
                let ids: Vec<u32> = items.iter().map(|x| x.0).collect();
                format!(
                    "SELECT {} FROM {} WHERE id IN ({}) ORDER BY id FOR UPDATE",
                    table_cols(*t),
                    t.name(),
                    join_ids(&ids)
                )
            }
            Op::BatchUpd { t, ids } | Op::BatchDel { t, ids } => {
                // 与原始 DML 相同的扫描/锁条件 → 先做一次 SELECT FOR UPDATE 等价预锁
                format!(
                    "SELECT {} FROM {} WHERE id IN ({}) ORDER BY id FOR UPDATE",
                    table_cols(*t),
                    t.name(),
                    join_ids(ids)
                )
            }
        }
    }

    /// 事务内/外部校验查询：作用于本 op 影响行集的普通读（稳定排序）。
    pub fn snapshot_check_sql(&self) -> String {
        self.check_sql(false)
    }
    /// 事务内/外部校验查询：当前读（FOR UPDATE）版本——即使原 op 只是普通 select，
    /// 校验也执行一次 FOR UPDATE（锁 + 最新版本视角）。
    pub fn current_check_sql(&self) -> String {
        self.check_sql(true)
    }

    /// 谓词是否主键（id）作用域：主键锁只落在本 worker 键区；非主键列（t_combo 无索引
    /// 的 a/(a,b)）上 FOR UPDATE 会全表 next-key 锁 → 校验的"当前读"退化为快照读。
    fn pk_scoped(&self) -> bool {
        match self {
            Op::PointSel { k: Key::Id(..), .. } | Op::PointFu { k: Key::Id(..), .. } => true,
            Op::PointSel { k: Key::Ab(..), .. } | Op::PointFu { k: Key::Ab(..), .. } => false,
            Op::RangeSel { on: Col::A, .. } | Op::RangeFu { on: Col::A, .. } => false,
            Op::RangeSel { on: Col::Pk, .. } | Op::RangeFu { on: Col::Pk, .. } => true,
            Op::Insert { .. } | Op::Update { .. } | Op::Delete { .. } | Op::BatchIns { .. }
            | Op::BatchSel { .. } | Op::BatchUpd { .. } | Op::BatchDel { .. } => true,
        }
    }

    fn check_sql(&self, for_update: bool) -> String {
        let fu = if for_update && self.pk_scoped() { " FOR UPDATE" } else { "" };
        match self {
            Op::PointSel { t, k } | Op::PointFu { t, k } => format!(
                "SELECT {} FROM {} WHERE {} ORDER BY id{fu}",
                table_cols(*t),
                t.name(),
                key_cond(*k)
            ),
            Op::Insert { t, id, a, b } => match t {
                Table::TTest => format!("SELECT id,val FROM t_test WHERE id={id} ORDER BY id{fu}"),
                Table::TCombo => format!(
                    "SELECT id,a,b,val FROM t_combo WHERE id={id} ORDER BY id{fu}"
                ),
            },
            Op::Update { t, id } | Op::Delete { t, id } => format!(
                "SELECT {} FROM {} WHERE id={id} ORDER BY id{fu}",
                table_cols(*t),
                t.name()
            ),
            Op::RangeSel { t, on, lo, hi } | Op::RangeFu { t, on, lo, hi } => format!(
                "SELECT {} FROM {} WHERE {} BETWEEN {lo} AND {hi} ORDER BY {}{fu}",
                table_cols(*t),
                t.name(),
                col_expr(*on),
                col_order(*on)
            ),
            Op::BatchIns { t, items } => {
                let ids: Vec<u32> = items.iter().map(|x| x.0).collect();
                format!(
                    "SELECT {} FROM {} WHERE id IN ({}) ORDER BY id{fu}",
                    table_cols(*t),
                    t.name(),
                    join_ids(&ids)
                )
            }
            Op::BatchSel { t, ids } => format!(
                "SELECT {} FROM {} WHERE id IN ({}) ORDER BY id{fu}",
                table_cols(*t),
                t.name(),
                join_ids(ids)
            ),
            Op::BatchUpd { t, ids } | Op::BatchDel { t, ids } => format!(
                "SELECT {} FROM {} WHERE id IN ({}) ORDER BY id{fu}",
                table_cols(*t),
                t.name(),
                join_ids(ids)
            ),
        }
    }
}

fn key_cond(k: Key) -> String {
    match k {
        Key::Id(id) => format!("id={id}"),
        Key::Ab(a, b) => format!("a={a} AND b={b}"),
    }
}
fn col_expr(c: Col) -> &'static str {
    match c {
        Col::Pk => "id",
        Col::A => "a",
    }
}
fn col_order(c: Col) -> &'static str {
    match c {
        Col::Pk => "id",
        Col::A => "a,id",
    }
}
fn join_ids(ids: &[u32]) -> String {
    ids.iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// 单点写类排序锁 key（升序；同事务按锁 key 单调执行 → 减少随机死锁）。
fn write_lock_key(op: &Op) -> Option<(Table, u64)> {
    match op {
        Op::Insert { t, id, .. }
        | Op::Update { t, id }
        | Op::Delete { t, id }
        | Op::PointFu { t, k: Key::Id(id), .. } => Some((*t, *id as u64)),
        // t_combo 二级 (a,b) 当前读 → 编码 (a<<20)|b 与 id 空间隔离（按通道各自单调）
        Op::PointFu { t: Table::TCombo, k: Key::Ab(a, b), .. } => {
            Some((Table::TCombo, ((*a as u64) << 20) | (*b as u64)))
        }
        _ => None,
    }
}

/// 单 worker 专属键区：与其它 worker 的区间互不重叠 → 跨 worker 零键冲突。
/// 这样两侧（MySQL / SCC）即使各自多 worker 独立调度、提交顺序不同，也不产生
/// 合法分歧（此前全局共用小键池时，两个同引擎库并发跑也会因调度不同而终态分叉，
/// 逐 op 行集 DIFF——见 2000 事务标定）；同 worker 内事务串行执行 → 双端历史等价。
#[derive(Clone, Copy, Debug)]
pub struct Seg {
    /// 本 worker 独占的既有种子行 id 区间（含端点；t_test / t_combo 同用）。
    pub lo: u32,
    pub hi: u32,
    /// 本 worker 专属新行插入池（种子行 rows 之后预留，跨 worker 不重叠）。
    pub new_lo: u32,
    pub new_hi: u32,
}

impl Seg {
    /// 既有种子行内均匀取键。
    fn pick_existing(&self, rng: &mut StdRng) -> u32 {
        rng.gen_range(self.lo..=self.hi)
    }
    /// 新行池内取键（同一 worker 多次 txn 复用该池 → 可能撞自插行，属确定性重复，
    /// 两边一致地 DUP/取消）。
    fn pick_new(&self, rng: &mut StdRng) -> u32 {
        rng.gen_range(self.new_lo..=self.new_hi)
    }
    /// 二级通道 a 值（与本 worker 的 id 空间共用区间，避免跨 worker 的 (a,*) 竞争）。
    fn pick_a(&self, rng: &mut StdRng) -> u32 {
        self.pick_existing(rng)
    }
}

/// 生成一个事务的操作序列（键全部分配在本 worker 的 Seg 内 → 跨 worker 零冲突）。
pub fn gen_txn(rng: &mut StdRng, seg: Seg) -> Vec<Op> {
    let mut ops: Vec<Op> = Vec::new();
    let n = rng.gen_range(2..=6);
    for _ in 0..n {
        let t = if rng.gen_bool(0.5) { Table::TTest } else { Table::TCombo };
        // 70% 单点 / 30% 范围·批量
        if rng.gen_bool(0.7) {
            let pick: u32 = rng.gen_range(0..5);
            let id = seg.pick_existing(rng);
            match pick {
                0 => ops.push(Op::PointSel { t, k: Key::Id(id) }),
                1 => ops.push(Op::PointFu { t, k: Key::Id(id) }),
                2 => {
                    // 二级等值（t_combo 才走 Ab 通道；t_test 无二级 → 转主键 sel）。
                    // 普通 SELECT：Ab 无索引，加锁=全表锁，只做纯快照读。
                    if t == Table::TCombo {
                        let (a, b) = (seg.pick_a(rng), rng.gen_range(0..50));
                        ops.push(Op::PointSel { t, k: Key::Ab(a, b) });
                    } else {
                        ops.push(Op::PointSel { t, k: Key::Id(id) });
                    }
                }
                3 => {
                    let (a, b) = (seg.pick_a(rng), rng.gen_range(0..50));
                    ops.push(Op::Insert { t, id, a, b });
                }
                4 => ops.push(Op::Delete { t, id }),
                _ => unreachable!(),
            }
        } else {
            let pick: u32 = rng.gen_range(0..6);
            let max_lo = seg.hi.saturating_sub(2).max(seg.lo);
            let lo = rng.gen_range(seg.lo..=max_lo);
            let hi = (lo + rng.gen_range(2..=30)).min(seg.hi);
            match pick {
                0 => ops.push(Op::RangeSel { t, on: if t == Table::TCombo { Col::A } else { Col::Pk }, lo, hi }),
                // RangeFu 只走主键（a 无索引：FOR UPDATE 全表 next-key 锁 → 跨 worker 死锁）
                1 => ops.push(Op::RangeFu { t, on: Col::Pk, lo, hi }),
                2 => {
                    let ids: Vec<u32> = (0..rng.gen_range(2..=5)).map(|_| seg.pick_existing(rng)).collect();
                    ops.push(Op::BatchUpd { t, ids });
                }
                3 => {
                    let items: Vec<(u32, u32, u32)> = (0..rng.gen_range(2..=5))
                        .map(|_| (seg.pick_new(rng), seg.pick_a(rng), rng.gen_range(0..50)))
                        .collect();
                    ops.push(Op::BatchIns { t, items });
                }
                4 => {
                    let ids: Vec<u32> = (0..rng.gen_range(2..=5)).map(|_| seg.pick_existing(rng)).collect();
                    ops.push(Op::BatchDel { t, ids });
                }
                _ => {
                    let ids: Vec<u32> = (0..rng.gen_range(2..=5)).map(|_| seg.pick_existing(rng)).collect();
                    ops.push(Op::BatchSel { t, ids });
                }
            }
        }
    }
    // 单点写类按锁 key 升序（先写后查亦可；只对写类排序——方案：单点写类提取锁 key 排序）
    let mut writes: Vec<Op> = Vec::new();
    let mut rest: Vec<Op> = Vec::new();
    for op in ops {
        if write_lock_key(&op).is_some() {
            writes.push(op);
        } else {
            rest.push(op);
        }
    }
    writes.sort_by_key(|op| write_lock_key(op).unwrap().1);
    writes.extend(rest);
    writes
}
