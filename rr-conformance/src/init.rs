//! 初始化：双库（MySQL 3306 / 自研 SCC 3308）建表 + 种子数据，保证起点一致。

use mysql::prelude::*;
use mysql::Conn;

use crate::ops::Table;

pub const DDL_T_TEST: &str =
    "CREATE TABLE IF NOT EXISTS t_test (id INT PRIMARY KEY, val INT)";
// 注：自研库（SCC）无二级唯一索引实现——保持两端**同一 DDL** 以跑同构对照，
// 二级唯一约束缺失带来的"重复插入不报错"属已知差异域（由工具采集记录，不作断言）。
pub const DDL_T_COMBO: &str =
    "CREATE TABLE IF NOT EXISTS t_combo (id INT PRIMARY KEY, a INT, b INT, val INT)";
pub const DDL_DROP_T_TEST: &str = "DROP TABLE IF EXISTS t_test";
pub const DDL_DROP_T_COMBO: &str = "DROP TABLE IF EXISTS t_combo";

/// 清空 + 建表 + 种子 rows 行（val=0；t_combo a=i、b=i%50 → (a,b) 全唯一）。
/// 种子保证两边逐行同构（终态 dump 基准一致）。
pub fn init(m: &mut Conn, d: &mut Conn, rows: u32) -> anyhow::Result<()> {
    for sql in [
        DDL_DROP_T_TEST,
        DDL_DROP_T_COMBO,
        DDL_T_TEST,
        DDL_T_COMBO,
    ] {
        m.query_drop(sql)?;
        d.query_drop(sql)?;
    }
    seed(m, Table::TTest, rows, rows)?;
    seed(d, Table::TTest, rows, rows)?;
    seed(m, Table::TCombo, rows, rows)?;
    seed(d, Table::TCombo, rows, rows)?;
    Ok(())
}

/// 批量多行 VALUES 分块灌种子（块 500 行），两库逐块一致。
fn seed(c: &mut Conn, t: Table, rows: u32, _cap: u32) -> anyhow::Result<()> {
    let mut id = 1u32;
    while id <= rows {
        let end = (id + 499).min(rows);
        let mut parts: Vec<String> = Vec::with_capacity((end - id + 1) as usize);
        for i in id..=end {
            match t {
                Table::TTest => parts.push(format!("({i},0)")),
                Table::TCombo => parts.push(format!("({i},{i},{},0)", i % 50)),
            }
        }
        let sql = match t {
            Table::TTest => format!("INSERT INTO t_test(id,val) VALUES {}", parts.join(",")),
            Table::TCombo => {
                format!("INSERT INTO t_combo(id,a,b,val) VALUES {}", parts.join(","))
            }
        };
        c.query_drop(sql)?;
        id = end + 1;
    }
    Ok(())
}
