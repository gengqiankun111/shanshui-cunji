//! SQL AST 定义（原 sqlish.rs AST 段）：CmpOp/Cond/WhereExpr/HavingCond/HavingExpr/
//! Select/JoinClause/JoinKind；parser/ 各文件与 executor/ 消费端共用。
// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cond {
    pub field: String,
    pub op: CmpOp,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WhereExpr {
    Cond(Cond),
    /// `field BETWEEN low AND high`（闭区间，数值/字典序）。
    Between { field: String, low: String, high: String },
    /// `field LIKE pattern`（SQL 通配，仅支持 `%` = 任意长度串，含空串）。
    /// 解析期分类：无 `%` → 折叠为 `Cond(Eq)`；含 `%` → 保留本变体（倒排不可表达 → 扫描）。
    Like { field: String, pattern: String },
    Not(Box<WhereExpr>),
    And(Box<WhereExpr>, Box<WhereExpr>),
    Or(Box<WhereExpr>, Box<WhereExpr>),
}

/// HAVING 原子条件（AF#5）：左项可为**聚合列头**（如 `COUNT(*)`/`SUM(amount)`，
/// 须与 SELECT 聚合列一致）或**分组字段名**；值为数字或字符串字面量。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HavingCond {
    pub lhs: String,
    pub op: CmpOp,
    pub value: String,
}

/// HAVING 表达式（分组结果上的过滤，支持 AND/OR/NOT/括号组合）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HavingExpr {
    Cond(HavingCond),
    Not(Box<HavingExpr>),
    And(Box<HavingExpr>, Box<HavingExpr>),
    Or(Box<HavingExpr>, Box<HavingExpr>),
}

/// 解析结果（SELECT 语句）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Select {
    /// 列清单（"*" = 全部）。
    pub columns: Vec<String>,
    pub table: String,
    pub where_expr: Option<WhereExpr>,
    pub limit: Option<u64>,
    pub offset: u64,
    /// 聚合（7.95）：`(函数名小写, 参数字段)`——`COUNT(*)` 字段为 None；
    /// `COUNT(f)/SUM(f)/AVG(f)/MIN(f)/MAX(f)` 字段 Some。普通 SELECT 为 None。
    pub agg: Option<(String, Option<String>)>,
    /// Task-030：标量聚合 `COUNT(DISTINCT f)` 去重标记（仅 `agg` 单聚合场景；
    /// 与 name="count" 组合生效；GROUP BY 内 DISTINCT 暂不支持 → 解析期拒绝）。
    pub agg_distinct: bool,
    /// ORDER BY 排序项（开发顺序 #1/#3）：`(字段, 是否 DESC)`。
    pub order_by: Vec<(String, bool)>,
    /// GROUP BY 分组字段（AF#2 单字段 → AF#4 多字段；空 = 无分组）。聚合列见
    /// `group_aggs`；顺序即分组层级（排序/去重键）。
    pub group_by: Vec<String>,
    /// GROUP BY 聚合列（AF#2~#4 支持 COUNT/SUM/AVG/MIN/MAX；每个 `(函数名小写, 参数字段)`，
    /// `COUNT(*)` 字段为 None）。无 GROUP BY 时为空，标量聚合走 `agg`。
    pub group_aggs: Vec<(String, Option<String>)>,
    /// GROUP BY 后的 HAVING 过滤（AF#5；None = 不过滤）。仅配合 GROUP BY。
    pub having: Option<HavingExpr>,
    /// P0-D：JOIN 规格（None = 无 JOIN）。支持 INNER/LEFT JOIN。
    pub join: Option<JoinClause>,
}

/// P0-D：JOIN 子句（`t1 INNER JOIN t2 ON t1.f1 = t2.f2`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinClause {
    /// JOIN 类型（INNER / LEFT）。
    pub join_type: JoinKind,
    /// 从表名。
    pub right_table: String,
    /// 主表关联字段。
    pub left_field: String,
    /// 从表关联字段。
    pub right_field: String,
}

/// P0-D：JOIN 类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}
