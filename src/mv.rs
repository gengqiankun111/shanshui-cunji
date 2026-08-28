//! 物化视图调度器（design 19 / development 5.22，阶段 2）。
//!
//! Cron / 触发式定时聚合任务：扫描新写入 → 按维度分组聚合 → 写入独立结果文档集；
//! **增量模式**：基于 docid 游标只处理增量数据（重复刷新同一批次自动跳过）；
//! 查询直接走结果集（毫秒级，`query(dimension)` 内存查表，零回表）。
//!
//! - `MaterializedView`：单视图状态（维度 → 聚合值）+ JSON 持久化（tmp + rename 原子写）；
//! - `MvScheduler`：多视图容器，`refresh_all(new_batch)` 触发式增量聚合入口。
//!
//! 聚合函数：`Count`（计数）/ `Sum`（求和）/ `Avg`（均值，维护 sum+count）。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;

use crate::error::{Error, Result};

/// 聚合函数。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFn {
    Count,
    Sum,
    Avg,
}

/// 聚合结果值。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggValue {
    Count(u64),
    Sum(f64),
    /// 均值：sum / count。
    Avg {
        sum: f64,
        count: u64,
    },
}

impl AggValue {
    /// 以纯数值形式输出（查询 / 序列化）。
    pub fn as_number(&self) -> f64 {
        match self {
            AggValue::Count(c) => *c as f64,
            AggValue::Sum(s) => *s,
            AggValue::Avg { sum, count } => {
                if *count == 0 {
                    0.0
                } else {
                    sum / *count as f64
                }
            }
        }
    }
}

/// 物化视图定义。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MvDefinition {
    /// 视图名（唯一）。
    pub name: String,
    /// 分组维度字段（文档 JSON 字段；字符串原样，数值/布尔转字符串）。
    pub dimension: String,
    /// 聚合字段（Sum/Avg 使用；Count 忽略）。
    pub agg_field: String,
    pub agg: AggFn,
}

/// 一次增量刷新的报告。
#[derive(Debug, Clone)]
pub struct RefreshReport {
    pub view: String,
    /// 实际处理的（新）文档数。
    pub processed: usize,
    /// 被游标跳过的（旧）文档数。
    pub skipped: usize,
    /// 当前分组数。
    pub groups: usize,
}

/// 物化视图：维度分组聚合状态 + 增量游标 + JSON 持久化。
pub struct MaterializedView {
    def: MvDefinition,
    path: PathBuf,
    /// 维度值 → 聚合值（有序，查询稳定）。
    groups: BTreeMap<String, AggValue>,
    /// 已处理的最大 docid（增量游标）。
    cursor: u64,
}

impl MaterializedView {
    /// 打开（或创建）视图：目录下存在同名状态文件则恢复（定义以落盘为准）。
    pub fn open(def: MvDefinition, dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("mv-{}.json", def.name));
        if path.exists() {
            let text = std::fs::read_to_string(&path)?;
            let state: MvState = serde_json::from_str(&text)
                .map_err(|e| Error::Corrupted(format!("物化视图状态解析失败: {e}")))?;
            return Ok(Self {
                def: state.def,
                path,
                groups: state.groups,
                cursor: state.cursor,
            });
        }
        Ok(Self {
            def,
            path,
            groups: BTreeMap::new(),
            cursor: 0,
        })
    }

    pub fn definition(&self) -> &MvDefinition {
        &self.def
    }

    /// 增量刷新：只处理 `docid > cursor` 的新写入（design 5.22 增量模式）。
    /// `docs` 为调用方提供的新写入批次 `(docid, 文档 JSON)`。
    pub fn refresh(&mut self, docs: &[(u64, Value)]) -> Result<RefreshReport> {
        let mut processed = 0;
        let mut skipped = 0;
        for (docid, doc) in docs {
            if *docid <= self.cursor {
                skipped += 1;
                continue;
            }
            let dim = dimension_of(doc, &self.def.dimension);
            match self.def.agg {
                AggFn::Count => {
                    self.groups
                        .entry(dim)
                        .and_modify(|v| {
                            if let AggValue::Count(c) = v {
                                *c += 1;
                            }
                        })
                        .or_insert(AggValue::Count(1));
                }
                AggFn::Sum | AggFn::Avg => {
                    let Some(v) = numeric_field(doc, &self.def.agg_field) else {
                        continue; // 无数值聚合字段：跳过该文档
                    };
                    match self.def.agg {
                        AggFn::Sum => {
                            self.groups
                                .entry(dim)
                                .and_modify(|x| {
                                    if let AggValue::Sum(s) = x {
                                        *s += v;
                                    }
                                })
                                .or_insert(AggValue::Sum(v));
                        }
                        _ => {
                            let e = self
                                .groups
                                .entry(dim)
                                .or_insert(AggValue::Avg { sum: 0.0, count: 0 });
                            if let AggValue::Avg { sum, count } = e {
                                *sum += v;
                                *count += 1;
                            }
                        }
                    }
                }
            }
            self.cursor = self.cursor.max(*docid);
            processed += 1;
        }
        if processed > 0 {
            self.persist()?;
        }
        Ok(RefreshReport {
            view: self.def.name.clone(),
            processed,
            skipped,
            groups: self.groups.len(),
        })
    }

    /// 查询某维度的聚合值（内存查表，毫秒级）。
    pub fn query(&self, dimension: &str) -> Option<&AggValue> {
        self.groups.get(dimension)
    }

    /// 全部分组（有序）。
    pub fn all(&self) -> &BTreeMap<String, AggValue> {
        &self.groups
    }

    /// 当前增量游标。
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// 持久化（tmp + rename 原子写）。
    fn persist(&self) -> Result<()> {
        let state = MvState {
            def: self.def.clone(),
            groups: self.groups.clone(),
            cursor: self.cursor,
        };
        let text = serde_json::to_string(&state)
            .map_err(|e| Error::Serialize(format!("物化视图序列化失败: {e}")))?;
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

/// 物化视图持久化状态（含定义，重启完整恢复）。
#[derive(Debug, Serialize, Deserialize)]
struct MvState {
    def: MvDefinition,
    groups: BTreeMap<String, AggValue>,
    cursor: u64,
}

/// 物化视图调度器：多视图容器 + 触发式增量聚合入口。
pub struct MvScheduler {
    dir: PathBuf,
    views: BTreeMap<String, MaterializedView>,
}

impl MvScheduler {
    /// 打开调度器目录（恢复已保存的全部视图）。
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut views = BTreeMap::new();
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                let fname = e.file_name().to_string_lossy().into_owned();
                if let Some(stem) = fname
                    .strip_prefix("mv-")
                    .and_then(|s| s.strip_suffix(".json"))
                {
                    let def = MvDefinition {
                        name: stem.to_string(),
                        dimension: String::new(),
                        agg_field: String::new(),
                        agg: AggFn::Count,
                    };
                    if let Ok(v) = MaterializedView::open(def, dir) {
                        views.insert(stem.to_string(), v);
                    }
                }
            }
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            views,
        })
    }

    /// 创建（或覆盖）视图。已存在同名视图返回错误（避免误覆盖）。
    pub fn create(&mut self, def: MvDefinition) -> Result<()> {
        if self.views.contains_key(&def.name) {
            return Err(Error::Config(format!("物化视图已存在: {}", def.name)));
        }
        let v = MaterializedView::open(def.clone(), &self.dir)?;
        info!(
            "物化视图创建: {}（维度 {}，聚合 {:?}）",
            def.name, def.dimension, def.agg
        );
        self.views.insert(def.name, v);
        Ok(())
    }

    /// 触发式增量聚合：将新写入批次应用到全部视图（Cron / 写入钩子调用）。
    pub fn refresh_all(&mut self, docs: &[(u64, Value)]) -> Result<Vec<RefreshReport>> {
        let mut reports = Vec::new();
        for v in self.views.values_mut() {
            reports.push(v.refresh(docs)?);
        }
        Ok(reports)
    }

    /// 查询视图某维度聚合值。
    pub fn query(&self, view: &str, dimension: &str) -> Option<&AggValue> {
        self.views.get(view).and_then(|v| v.query(dimension))
    }

    /// 视图清单。
    pub fn view_names(&self) -> Vec<String> {
        self.views.keys().cloned().collect()
    }
}

/// 提取维度值：字符串原样；数值/布尔转字符串；缺失 → 空串（归入 "" 分组）。
fn dimension_of(doc: &Value, dimension: &str) -> String {
    match doc.get(dimension) {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::Bool(b)) => b.to_string(),
        _ => String::new(),
    }
}

/// 提取聚合字段数值（Sum/Avg 用）：数值 / 字符串数字。
fn numeric_field(doc: &Value, field: &str) -> Option<f64> {
    match doc.get(field) {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc(dim: &str, amount: f64) -> Value {
        json!({"city": dim, "amount": amount, "status": "active"})
    }

    #[test]
    fn count_aggregates_by_dimension() {
        let dir = tempfile::tempdir().unwrap();
        let def = MvDefinition {
            name: "orders-by-city".into(),
            dimension: "city".into(),
            agg_field: String::new(),
            agg: AggFn::Count,
        };
        let mut mv = MaterializedView::open(def, dir.path()).unwrap();
        let docs = vec![
            (1, doc("beijing", 10.0)),
            (2, doc("shanghai", 20.0)),
            (3, doc("beijing", 30.0)),
        ];
        let r = mv.refresh(&docs).unwrap();
        assert_eq!(r.processed, 3);
        assert_eq!(r.groups, 2);
        assert_eq!(mv.query("beijing"), Some(&AggValue::Count(2)));
        assert_eq!(mv.query("shanghai"), Some(&AggValue::Count(1)));
        assert_eq!(mv.query("guangzhou"), None);
        assert_eq!(mv.cursor(), 3);
    }

    #[test]
    fn sum_and_avg_aggregate_numeric_field() {
        let dir = tempfile::tempdir().unwrap();
        // Sum
        let mut sum_mv = MaterializedView::open(
            MvDefinition {
                name: "sum".into(),
                dimension: "city".into(),
                agg_field: "amount".into(),
                agg: AggFn::Sum,
            },
            dir.path(),
        )
        .unwrap();
        sum_mv
            .refresh(&[
                (1, doc("beijing", 10.0)),
                (2, doc("beijing", 20.0)),
                (3, doc("shanghai", 5.0)),
            ])
            .unwrap();
        assert_eq!(sum_mv.query("beijing"), Some(&AggValue::Sum(30.0)));
        assert_eq!(sum_mv.query("beijing").unwrap().as_number(), 30.0);
        // Avg
        let mut avg_mv = MaterializedView::open(
            MvDefinition {
                name: "avg".into(),
                dimension: "city".into(),
                agg_field: "amount".into(),
                agg: AggFn::Avg,
            },
            dir.path(),
        )
        .unwrap();
        avg_mv
            .refresh(&[(1, doc("beijing", 10.0)), (2, doc("beijing", 20.0))])
            .unwrap();
        assert_eq!(
            avg_mv.query("beijing"),
            Some(&AggValue::Avg {
                sum: 30.0,
                count: 2
            })
        );
        assert_eq!(avg_mv.query("beijing").unwrap().as_number(), 15.0);
        // 无数值字段文档被跳过（Sum 不计数）
        sum_mv.refresh(&[(4, json!({"city": "beijing"}))]).unwrap();
        assert_eq!(
            sum_mv.query("beijing"),
            Some(&AggValue::Sum(30.0)),
            "无数值字段不参与 Sum"
        );
    }

    #[test]
    fn incremental_refresh_uses_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let mut mv = MaterializedView::open(
            MvDefinition {
                name: "v".into(),
                dimension: "city".into(),
                agg_field: String::new(),
                agg: AggFn::Count,
            },
            dir.path(),
        )
        .unwrap();
        mv.refresh(&[(1, doc("a", 0.0)), (2, doc("b", 0.0)), (3, doc("a", 0.0))])
            .unwrap();
        // 重复刷新同一批次 → 全部被游标跳过（不重复计数）
        let r = mv
            .refresh(&[(1, doc("a", 0.0)), (2, doc("b", 0.0))])
            .unwrap();
        assert_eq!(r.processed, 0);
        assert_eq!(r.skipped, 2);
        assert_eq!(mv.query("a"), Some(&AggValue::Count(2)));
        // 增量批次（更高 docid）只处理新增
        let r = mv
            .refresh(&[(4, doc("a", 0.0)), (5, doc("c", 0.0))])
            .unwrap();
        assert_eq!(r.processed, 2);
        assert_eq!(mv.query("a"), Some(&AggValue::Count(3)));
        assert_eq!(mv.query("c"), Some(&AggValue::Count(1)));
    }

    #[test]
    fn view_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut mv = MaterializedView::open(
                MvDefinition {
                    name: "p".into(),
                    dimension: "city".into(),
                    agg_field: "amount".into(),
                    agg: AggFn::Sum,
                },
                dir.path(),
            )
            .unwrap();
            mv.refresh(&[(1, doc("beijing", 7.0)), (2, doc("beijing", 3.0))])
                .unwrap();
        }
        // 重启恢复：分组 + 游标
        let mut mv2 = MaterializedView::open(
            MvDefinition {
                name: "p".into(),
                dimension: "city".into(),
                agg_field: "amount".into(),
                agg: AggFn::Sum,
            },
            dir.path(),
        )
        .unwrap();
        assert_eq!(mv2.query("beijing"), Some(&AggValue::Sum(10.0)));
        assert_eq!(mv2.cursor(), 2);
        // 增量续跑：只处理新 docid
        mv2.refresh(&[(1, doc("beijing", 100.0)), (3, doc("beijing", 5.0))])
            .unwrap();
        assert_eq!(
            mv2.query("beijing"),
            Some(&AggValue::Sum(15.0)),
            "旧 docid 不重复计数"
        );
    }

    #[test]
    fn scheduler_create_refresh_query() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = MvScheduler::open(dir.path()).unwrap();
        s.create(MvDefinition {
            name: "by-status".into(),
            dimension: "status".into(),
            agg_field: String::new(),
            agg: AggFn::Count,
        })
        .unwrap();
        assert!(
            s.create(MvDefinition {
                name: "by-status".into(),
                dimension: "x".into(),
                agg_field: String::new(),
                agg: AggFn::Count
            })
            .is_err(),
            "重复创建拒绝"
        );
        let reports = s
            .refresh_all(&[
                (1, json!({"status": "active"})),
                (2, json!({"status": "pending"})),
                (3, json!({"status": "active"})),
            ])
            .unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].processed, 3);
        assert_eq!(s.query("by-status", "active"), Some(&AggValue::Count(2)));
        assert_eq!(s.query("by-status", "pending"), Some(&AggValue::Count(1)));
        assert_eq!(s.view_names(), vec!["by-status".to_string()]);
        // 调度器重启恢复
        let s2 = MvScheduler::open(dir.path()).unwrap();
        assert_eq!(s2.query("by-status", "active"), Some(&AggValue::Count(2)));
    }
}
