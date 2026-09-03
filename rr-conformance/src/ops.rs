//! RR 对照操作模型：两张表（t_test 单主键 / t_combo 主键 + 唯一索引 idx_ab(a,b)），
//! 12 类操作全集；事务级 ops 生成（70% 单点 / 30% 范围·批量；单点写类按锁 key 升序重排
//! 减少随机死锁干扰；range/batch 原样下发、不重排、允许死锁只采集）。
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
    pub fn try_fu_sql(&self) -> String {
        match self {
            Op::PointFu { t, k } => self.sql(),
            Op::PointSel { t, k } => format!(
                "SELECT {} FROM {} WHERE {} FOR UPDATE",
                table_cols(*t),
                t.name(),
                key_cond(*k)
            ),
            Op::Insert { t, id, a, b } => match t {
                Table::TTest => format!("SELECT id,val FROM t_test WHERE id={id} FOR UPDATE"),
                Table::TCombo => {
                    format!("SELECT id,a,b,val FROM t_combo WHERE (id={id} OR (a={a} AND b={b})) FOR UPDATE")
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
                // 批量插入：对目标 id 与 (a,b) 预锁（防并发插同键/唯一冲突下重复）
                let ids: Vec<u32> = items.iter().map(|x| x.0).collect();
                match t {
                    Table::TTest => format!(
                        "SELECT id,val FROM t_test WHERE id IN ({}) FOR UPDATE",
                        join_ids(&ids)
                    ),
                    Table::TCombo => {
                        let conds: Vec<String> = items
                            .iter()
                            .map(|(id, a, b)| format!("(id={id} OR (a={a} AND b={b}))"))
                            .collect();
                        format!(
                            "SELECT id,a,b,val FROM t_combo WHERE {} FOR UPDATE",
                            conds.join(" OR ")
                        )
                    }
                }
            }
            Op::BatchUpd { t, ids } | Op::BatchDel { t, ids } => {
                // 与原始 DML 相同的扫描/锁条件 → 先做一次 FOR UPDATE 读等价预锁
                let cond = match self {
                    Op::BatchUpd { .. } => {
                        format!("UPDATE {} SET val=val+1 WHERE id IN ({})", t.name(), join_ids(ids))
                    }
                    _ => {
                        format!("DELETE FROM {} WHERE id IN ({})", t.name(), join_ids(ids))
                    }
                };
                let _ = cond; // 预锁用 SELECT FOR UPDATE 形态（与 DML 同范围）
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

    fn check_sql(&self, for_update: bool) -> String {
        let fu = if for_update { " FOR UPDATE" } else { "" };
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

/// 生成一个事务的操作序列。
/// `rows`：既有种子行数（决定冲突/新行策略域）；`p_dup`：故意撞既有键的概率（采集 1062）。
pub fn gen_txn(rng: &mut StdRng, rows: u32, p_dup: f64) -> Vec<Op> {
    let mut ops: Vec<Op> = Vec::new();
    let n = rng.gen_range(2..=6);
    for _ in 0..n {
        let t = if rng.gen_bool(0.5) { Table::TTest } else { Table::TCombo };
        // 70% 单点 / 30% 范围·批量
        if rng.gen_bool(0.7) {
            let pick: u32 = rng.gen_range(0..5);
            let id = pick_existing_id(rng, rows, p_dup);
            match pick {
                0 => ops.push(Op::PointSel { t, k: Key::Id(id) }),
                1 => ops.push(Op::PointFu { t, k: Key::Id(id) }),
                2 => {
                    // 二级等值（t_combo 才走 Ab 通道；t_test 无二级 → 转主键 fu）
                    if t == Table::TCombo {
                        let (a, b) = (rng.gen_range(1..=rows.max(1)), rng.gen_range(0..50));
                        ops.push(Op::PointFu { t, k: Key::Ab(a, b) });
                    } else {
                        ops.push(Op::PointSel { t, k: Key::Id(id) });
                    }
                }
                3 => {
                    let (a, b) = (rng.gen_range(1..=rows.max(1)), rng.gen_range(0..50));
                    ops.push(Op::Insert { t, id, a, b });
                }
                4 => ops.push(Op::Delete { t, id }),
                _ => unreachable!(),
            }
        } else {
            let pick: u32 = rng.gen_range(0..6);
            let lo = rng.gen_range(1..=rows.max(2));
            let hi = (lo + rng.gen_range(2..=30)).min(rows + 100);
            match pick {
                0 => ops.push(Op::RangeSel { t, on: if t == Table::TCombo { Col::A } else { Col::Pk }, lo, hi }),
                1 => ops.push(Op::RangeFu { t, on: if t == Table::TCombo { Col::A } else { Col::Pk }, lo, hi }),
                2 => {
                    let ids: Vec<u32> = (0..rng.gen_range(2..=5)).map(|_| pick_existing_id(rng, rows, p_dup)).collect();
                    ops.push(Op::BatchUpd { t, ids });
                }
                3 => {
                    let items: Vec<(u32, u32, u32)> = (0..rng.gen_range(2..=5))
                        .map(|_| (pick_new_id(rng, rows), rng.gen_range(1..=rows.max(1)), rng.gen_range(0..50)))
                        .collect();
                    ops.push(Op::BatchIns { t, items });
                }
                4 => {
                    let ids: Vec<u32> = (0..rng.gen_range(2..=5)).map(|_| pick_existing_id(rng, rows, p_dup)).collect();
                    ops.push(Op::BatchDel { t, ids });
                }
                _ => {
                    let ids: Vec<u32> = (0..rng.gen_range(2..=5)).map(|_| pick_existing_id(rng, rows, p_dup)).collect();
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

fn pick_existing_id(rng: &mut StdRng, rows: u32, p_dup: f64) -> u32 {
    if rows == 0 {
        1
    } else {
        let id = rng.gen_range(1..=rows);
        if rng.gen_bool(p_dup) {
            id // 撞既有主键（Insert 场景产生 1062）
        } else {
            id
        }
    }
}
fn pick_new_id(rng: &mut StdRng, rows: u32) -> u32 {
    rows + 1 + rng.gen_range(0..=500)
}
