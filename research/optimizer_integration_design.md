# 优化器流程整合设计（DocIdSet + get_docid_set + execute_join 重构）

> 参考：research/optimizer_proces.md（8 阶段流程）
> 参考：research/like_offset_design.md（LIKE 前缀通配 + OFFSET 支持）
> 目标：将 `optimizer_proces.md` 描述的执行流程落到实处，用 `DocIdSet` 统一框架替代 `sqlish.rs` 中散落的 `eval()` / `post_filter()` / `scan_pushdown()` 调用。
> 补充：2026-09-04 用户定稿写路径边界（INSERT/UPDATE/DELETE 本身不走优化器，仅 UPDATE/DELETE 的**定位行（WHERE 收敛）部分**走 `get_docid_set`，定位后走写执行器批量管道）——见文末「九、写路径（UPDATE/DELETE）整合设计」。

---

## 一、DocIdSet 枚举设计

### 1.1 定义位置

`src/sqlish.rs` 中新增（约 100 行），位于 `WhereExpr` 定义之后、`execute` 函数之前。

### 1.2 枚举定义

```rust
/// 统一 DocId 集合抽象，支持 AND/OR 交集运算。
///
/// 三种具体形态 + 两个特殊值：
/// - Bitmap:   倒排/位图产出的文档位图（最紧凑，AND/OR 极快）
/// - SortedList: 组合索引前缀扫描产出的有序 docid 列表（已排序，归并求交）
/// - Stream:   全表扫描产出的流式迭代器（惰性，不提前物化）
/// - Empty:    空集（过滤条件直接冲突，如 status='active' AND status='inactive'）
/// - All:      全集（无过滤条件）
///
/// 设计原则：不尝试做"最优"选择，只提供统一的"集合运算"接口。
/// 调用方（get_docid_set/execute_join）负责选择创建哪种形态。
pub enum DocIdSet {
    Bitmap(RoaringBitmap),
    SortedList(Vec<u64>),
    Stream(Box<dyn Iterator<Item = u64>>),
    Empty,
    All,
}
```

### 1.3 操作方法

```rust
impl DocIdSet {
    /// 与另一个 DocIdSet 做交集（AND）
    pub fn intersect(self, other: DocIdSet) -> DocIdSet { ... }

    /// 提取所有 docid 为 Vec（物化），用于批量回表
    pub fn to_vec(&self) -> Vec<u64> { ... }

    /// 是否为空集
    pub fn is_empty(&self) -> bool { ... }

    /// 估算行数
    pub fn len_estimate(&self) -> u64 { ... }

    /// 迭代器（流式访问 docid，Stream 形态惰性）
    pub fn iter(&self) -> Box<dyn Iterator<Item = u64> + '_> { ... }
}
```

### 1.4 intersect 组合规则

| 组合 | 实现 | 复杂度 |
|------|------|--------|
| Bitmap ∩ Bitmap | `a.and(b)` | O(min(a,b)) |
| Bitmap ∩ SortedList | 遍历 SortedList，在位图中查 | O(列表长度) |
| SortedList ∩ SortedList | 双指针归并（两列表都是有序的） | O(a+b) |
| Bitmap ∩ Stream | 流式过滤：`stream.filter(docid → bitmap.contains(docid))` | O(流长度) |
| SortedList ∩ Stream | 同上，用 HashSet 加速 | O(流长度) |
| Stream ∩ Stream | 不推荐，回退全量物化再交集 | O(流长度) |

**安全问题**：`Stream ∩ Stream` 必须物化至少一个流，有 OOM 风险。在 `get_docid_set` 中不应产生 `Stream ∩ Stream` 的组合——至少有一边是 Bitmap 或 SortedList。

### 1.5 新增 LimitSpec 结构体

（来自 `like_offset_design.md` 改动点 A）

```rust
/// LIMIT + OFFSET 规格。
/// 用于替代散落的 `Option<u64>`，统一管理 LIMIT 下推和 OFFSET 跳过。
#[derive(Clone, Debug, Default)]
pub struct LimitSpec {
    pub limit: Option<u64>,
    pub offset: u64,
}

impl LimitSpec {
    /// 需要获取的总行数（用于下推）= offset + limit。
    /// 例如 `LIMIT 10 OFFSET 1000` → total = 1010。
    pub fn total_to_fetch(&self) -> Option<u64> {
        self.limit.map(|l| self.offset + l)
    }

    /// 是否可以早停（扫到 total 就停）。
    pub fn can_early_stop(&self) -> bool {
        self.limit.is_some() && self.offset < 10000
    }

    /// 从 `Sel` 的解析结果构建。
    pub fn from_select(limit: Option<u64>, offset: u64) -> Self {
        Self { limit, offset }
    }
}
```

---

## 二、get_docid_set() 函数设计

### 2.1 函数签名

```rust
/// 对一张表的 WHERE 条件，返回该表所有匹配行的 DocId 集合。
///
/// 阶梯选择逻辑见 optimizer_proces.md 阶段 1。
/// 对每张表独立调用，不感知 JOIN 另一侧的存在。
///
/// `table_id`: 表 ID（用于组合索引扫描/区间过滤）
/// `where_expr`: 该表的 WHERE 表达式树
/// `limit`: LIMIT + OFFSET 规格（用于下推）
fn get_docid_set(
    engine: &Engine,
    table_id: u64,
    where_expr: Option<&WhereExpr>,
    limit: Option<LimitSpec>,
    guard: &QueryGuard,
) -> Result<DocIdSet>
```

**关键改动**：`limit` 从 `Option<u64>` 改为 `Option<LimitSpec>`，支持 OFFSET 下推。

### 2.2 内部处理逻辑

```
get_docid_set(engine, table_id, expr, limit, guard):
    if expr is None → return DocIdSet::All

    // 提取 OFFSET 信息用于下推
    let total = limit.and_then(|l| l.total_to_fetch());

    match expr:
        Cond(op=Eq, field!=docid) → 走倒排 | scan_backfill
            → DocIdSet::Bitmap
        Cond(op=Eq, field=docid) → 主键点查
            → DocIdSet::Bitmap(单例)
        Cond(op=Ne/Gt/Lt/Ge/Le) → scan_pushdown
            → DocIdSet::Stream
        Between → scan_pushdown + ZoneMap
            → DocIdSet::Stream

        // ===== 新增：LIKE 处理 =====
        Like(field, pattern, is_prefix=false) →
            // '%abc' 或 '%abc%' → 全扫 + 行过滤
            → DocIdSet::Stream(full_scan_with_like_filter)

        Like(field, pattern, is_prefix=true) →
            // 'abc%' → 组合索引前缀匹配（已在 1.1 处理，此处做兜底）
            // 如果没有组合索引，走全扫 + starts_with 过滤
            → DocIdSet::Stream(full_scan_with_like_filter)

        And(a, b) → 见 2.3 阶梯选择
            left.intersect(right)

        Or(a, b) → 降级为 eval() → DocIdSet::Bitmap
        Not(x) → 降级为 eval() → DocIdSet::Bitmap
```

### 2.3 LIKE 处理分支

（来自 `like_offset_design.md` 改动点 A）

`LIKE` 在解析期做分类：

| SQL 写法 | 解析结果 | 处理方式 |
|----------|---------|---------|
| `LIKE 'abc'`（无通配符） | 转为 `Cond { op: Eq }` | 走倒排/组合索引 |
| `LIKE 'abc%'`（后缀通配） | `WhereExpr::Like { is_prefix: true }` | 组合索引前缀匹配 |
| `LIKE '%abc'`（前缀通配） | `WhereExpr::Like { is_prefix: false }` | 全扫 + 行过滤 |
| `LIKE '%abc%'`（前后通配） | `WhereExpr::Like { is_prefix: false }` | 全扫 + 行过滤 |

在 `get_docid_set` 中，`Like(is_prefix=true)` 尝试组合索引 1.1，不匹配则走全扫；`Like(is_prefix=false)` 直接走全扫：

```
// 新增阶梯（在 1.4 混合扫描之后，1.5 全扫+ZoneMap 之前）：
if let Some(like) = extract_like_leaf(where_expr) {
    if !like.is_prefix {
        // '%abc' / '%abc%' → 全扫 + 行过滤
        // Zone Map 无法对字符串做范围剪枝，所以不能走 1.5
        let stream = full_table_scan_with_like_filter(
            engine, like.field, like.pattern, guard
        );
        return Ok(DocIdSet::Stream(Box::new(stream)));
    }
    // 'abc%' → 走 1.1 组合索引（如果匹配），否则全扫
}
```

**LIKE + 倒排等值 AND 的优化**（如 `status='active' AND name LIKE '%abc'`）：

```
And(a, b) 处理时：
  1. 提取倒排等值 a（status='active'）→ Bitmap
  2. 提取 LIKE b（name LIKE '%abc'）→ 视为后过滤
  3. 对 Bitmap 中的 docid 做行级 LIKE 检查
  → 只在倒排命中的行上做 LIKE 过滤，不用全扫整表
```

### 2.4 OR / NOT 的降级处理

**Or** 不能用 `intersect` 表达，需要 `union`。当前 `DocIdSet` 没有 `union` 方法。

**解决方案**：`get_docid_set` 遇到 `Or/Not` 时，降级为调用现有的 `eval()` 函数（返回 RoaringBitmap），然后包装为 `DocIdSet::Bitmap`。

```
get_docid_set 遇到 Or/Not:
    → 调用 eval() 产出 RoaringBitmap
    → 包装为 DocIdSet::Bitmap

    这是"降级"路径，但足够安全——因为 eval() 已经实现了
    OR/AND/NOT 的完整位图运算，不会产生错误结果。
    对 AND 条件，优先走 get_docid_set 的阶梯选择。
```

### 2.5 AND 阶梯选择（优化器阶段 1 映射）

```
get_docid_set(engine, table_id, WhereExpr::And(a, b), limit, guard):
    // 尝试提取倒排等值 + 后过滤（前置过滤缩小范围后再后过滤）
    // 优先级：倒排等值 > 组合索引 > LIKE + 倒排 > 普通 AND

    // 1) 倒排等值 + 范围/BETWEEN 后过滤
    if let Some(eq_cond) = extract_eq_from(a) {
        if field_has_inverted(eq_cond.field) {
            let bitmap = inverted_posting(eq_cond.term);
            if let Some(scan_leaf) = scan_leaf(b) {
                let base = DocIdSet::Bitmap(bitmap);
                // 混合扫描：post_filter 在 DocIdSet 框架内处理
                return hybrid_filter(base, scan_leaf, engine, limit, guard);
            }
            let right = get_docid_set(engine, table_id, b, limit, guard);
            return DocIdSet::Bitmap(bitmap).intersect(right);
        }
    }

    // 2) 倒排等值 + LIKE 后过滤（对称处理另一边）
    // 已在 AND 递归中自然处理——a 是倒排等值 → Bitmap，
    // b 是 LIKE → 看做后过滤

    // 对称处理 b
    ...

    // 兜底：递归 AND
    let left = get_docid_set(engine, table_id, a, limit, guard);
    let right = get_docid_set(engine, table_id, b, limit, guard);
    left.intersect(right)
```

---

## 三、`execute()` 重构设计

### 3.1 当前结构 → 改造后结构

```
execute()                                      execute()
    ├── GROUP BY 路由                             ├── GROUP BY 路由（不变）
    ├── JOIN 路由                                 ├── JOIN 路由（重构）
    ├── 组合索引路由           → 并入 →           ├── get_docid_set
    ├── P4-C 代价优化          → 并入 →           │   → DocIdSet
    ├── 等值回退早停路径        → 并入 →           ├── 根据 DocIdSet 消费
    ├── 裸比较 scan_pushdown   → 并入 →           │   Bitmap → batch_get 回表
    ├── eval() 产位图 → 回表   → 并入 →           │   SortedList → batch_get 回表
    └── ORDER BY 处理                              │   Stream → 流式回表
                                                   └── ORDER BY（Top-K/全量）
```

### 3.2 DocIdSet 消费路径（含 OFFSET 支持）

```
let limit_spec = LimitSpec::from_select(sel.limit, sel.offset);
let set = get_docid_set(engine, table_id, sel.where_expr.as_ref(),
                         Some(limit_spec), &guard)?;

if set.is_empty() {
    return Ok(Vec::new());
}

if !sel.order_by.is_empty() {
    // ORDER BY 路径（见 3.3）
    handle_order_by(engine, set, sel, limit_spec, &guard)
} else {
    // 无 ORDER BY：流式回表输出
    let total = limit_spec.total_to_fetch().unwrap_or(u64::MAX);
    let mut output = Vec::new();
    let mut skipped = 0u64;

    match set {
        DocIdSet::Bitmap(bm) => {
            // 位图迭代：skip(offset) + take(limit)
            for docid in bm.iter().skip(limit_spec.offset as usize) {
                if output.len() as u64 >= limit_spec.limit.unwrap_or(u64::MAX) {
                    break;
                }
                if let Some(doc) = engine.get(docid)? {
                    output.push((docid, doc));
                }
            }
        }
        DocIdSet::SortedList(list) => {
            // 有序列表切片：list[offset..offset+limit]
            let start = (limit_spec.offset as usize).min(list.len());
            let end = limit_spec.limit
                .map(|l| (start + l as usize).min(list.len()))
                .unwrap_or(list.len());
            let docids = &list[start..end];
            let batch = engine.batch_get(docids)?;
            for (docid, v_opt) in docids.iter().zip(batch.into_iter()) {
                if let Some(doc) = v_opt {
                    output.push((*docid, doc));
                }
            }
        }
        DocIdSet::Stream(stream) => {
            // 流式迭代：skip(offset) + take(limit)
            for docid in stream.skip(limit_spec.offset as usize) {
                if output.len() as u64 >= limit_spec.limit.unwrap_or(u64::MAX) {
                    break;
                }
                if let Some(doc) = engine.get(docid)? {
                    output.push((docid, doc));
                }
            }
        }
        DocIdSet::All | DocIdSet::Empty => { /* 现有路径不变 */ }
    }
    Ok(output)
}
```

### 3.3 ORDER BY + OFFSET 处理

（来自 `like_offset_design.md` 改动点 D）

```
fn handle_order_by(engine, set, sel, limit_spec, guard) {
    let k_total = limit_spec.offset + limit_spec.limit.unwrap_or(0);

    if limit_spec.limit.is_some() && k_total > 0 {
        // 安全阀：大 offset 拒绝
        if k_total > 100000 {
            return Err(Error::QueryTooExpensive(
                "OFFSET + LIMIT 过大，建议缩小 OFFSET 或使用 keyset pagination"
            ));
        }
        // Top-K 堆排序：堆大小 = offset + limit
        let bitmap = materialize_to_bitmap(&set)?;
        let result = topk_sort_with_offset(
            engine, &bitmap, &sel.order_by,
            k_total as usize, limit_spec, guard
        )?;
        return Ok(result);
    }

    // 无 LIMIT：全量排序（守卫不变）
    ...
}
```

**`topk_sort_with_offset` 实现**：

```
堆大小 = offset + limit
遍历所有行，堆中保留"最差的 offset+limit 个"
排序后，跳过 offset 个，取后面 limit 个
```

### 3.4 materialize_to_bitmap 辅助函数

```rust
fn materialize_to_bitmap(set: &DocIdSet) -> Result<RoaringBitmap> {
    match set {
        DocIdSet::Bitmap(bm) => Ok(bm.clone()),
        DocIdSet::SortedList(list) => {
            let mut bm = RoaringBitmap::new();
            for &docid in list {
                bm.insert(docid);
            }
            Ok(bm)
        }
        DocIdSet::Stream(_) => {
            Err(Error::Config("Stream 物化为位图暂不支持，请加倒排等值条件".into()))
        }
        DocIdSet::All => {
            Err(Error::Config("All 物化为位图暂不支持".into()))
        }
        DocIdSet::Empty => Ok(RoaringBitmap::new()),
    }
}
```

---

## 四、`execute_join()` 重构设计

### 4.1 当前代码的问题

当前 `execute_join`（L1418-1534）：
1. 用 `eval()` 产主表 bitmap
2. 主表 batch_get → 提取关联 key
3. 逐 key 查从表（docid 点查/倒排 term）
4. 内存 HashMap 合并

**不足**：硬编码为单一路径，没有统一的 DocIdSet 抽象，不支持 JOIN 路径选择。

### 4.2 改造后流程（对齐 optimizer_proces.md 阶段 1~3）

```
execute_join(engine, sel, cap):
    ┌─────────────────────────────────────────────────────────┐
    │ 阶段 0：安全检查（解析期已做，运行时补充）              │
    │ - 表数 ≥ 3 → 拒绝（已在解析期做）                       │
    │ - 非等值 JOIN → 拒绝（已在解析期做）                     │
    │ - LIKE 前缀通配警告（如果有）                           │
    │ - OFFSET > 10000 警告（如果有）                         │
    └─────────────────────────────────────────────────────────┘
        │
        ▼
    ┌─────────────────────────────────────────────────────────┐
    │ 阶段 1：每张表 WHERE 独立产出 DocIdSet                  │
    │                                                         │
    │ left_set = get_docid_set(engine, left_table_id,         │
    │     sel.where_expr, Some(limit_spec), guard)            │
    │                                                         │
    │ right_set = get_docid_set(engine, right_table_id,       │
    │     sel.right_where_expr, Some(limit_spec), guard)      │
    └─────────────────────────────────────────────────────────┘
        │
        ▼
    ┌─────────────────────────────────────────────────────────┐
    │ 阶段 2：JOIN 路径选择                                    │
    │                                                         │
    │ 2.1 主键 JOIN（JOIN 字段是 docid）                       │
    │     left_set → to_vec → 直接作为右表主键列表            │
    │     → batch_get(left_docs) + batch_get(right_docs)      │
    │     → 按 JOIN 类型组装（+ LIMIT 下推 + OFFSET 跳过）   │
    │                                                         │
    │ 2.2 索引-索引 JOIN（两边都有索引）                       │
    │     left_set.intersect(right_set) → matched_set         │
    │     → batch_get(matched_set) → 提取 JOIN 字段值 → 匹配│
    │     → 注意：intersect 只是候选集缩小，不是 JOIN 匹配    │
    │                                                         │
    │ 2.3 有索引-无索引 JOIN                                   │
    │     索引端 HashSet → 无索引端全扫逐行过滤               │
    │     安全阀：无索引表 > 500 万 → 拒绝                    │
    │                                                         │
    │ 2.4 广播哈希 JOIN（两边都无索引，右表 < 10 万）         │
    │     全扫右表建 HashMap → 左表流式查哈希                  │
    │                                                         │
    │ 2.5 拒绝执行（两边都无索引且右表 ≥ 10 万）              │
    └─────────────────────────────────────────────────────────┘
        │
        ▼
    ┌─────────────────────────────────────────────────────────┐
    │ 阶段 3：跨表 WHERE 后过滤（预留，当前 SQL 解析器不支持）│
    └─────────────────────────────────────────────────────────┘
        │
        ▼
    ┌─────────────────────────────────────────────────────────┐
    │ 阶段 7：LIMIT + OFFSET 下推                              │
    │ - JOIN 产出过程中计数，达到 limit + offset 即停         │
    │ - 最终结果跳过 offset 个，取 limit 个                   │
    └─────────────────────────────────────────────────────────┘
```

### 4.3 LEFT JOIN 语义保留

LEFT JOIN 的"左表所有行保留，右表无匹配行填充 NULL"语义不变：

```
LEFT JOIN 流程：
1. left_set → batch_get → Vec<(docid, left_doc)>
2. 对每个 left_doc:
   a. 提取 JOIN key
   b. 在 right_set 中查找匹配行
      - 如果 JOIN 字段是 docid → key 直接作为 docid 点查
      - 如果 JOIN 字段是其他字段 → 倒排查 term → 批量回表
   c. 有匹配 → 每行左+右展开
   d. 无匹配 → 左+NULL
3. LIMIT + OFFSET 作用在最终结果上
```

### 4.4 JOIN 字段匹配细化

**关键**：`DocIdSet.intersect()` 是基于 docid 的交集，不是基于 JOIN 字段值。只有 JOIN 字段是 `docid` 时，intersect 的结果才是正确的 JOIN 匹配集。

```
场景 A：右表 JOIN 字段是 docid
    ON orders.user_id = users.docid
    → left_set 的 docid 直接作为右表主键，最优（2.1 主键 JOIN）

场景 B：两边都是业务字段
    ON orders.user_id = users.user_id（字符串）
    → left_set ∩ right_set 先缩小候选集
    → 再回表提取字段值做行级匹配
```

---

## 五、阶段 0 安全检查增强

### 5.1 LIKE 前缀通配警告

```rust
fn safety_check(where_expr: Option<&WhereExpr>, limit_spec: Option<&LimitSpec>) {
    // LIKE 前缀通配 → 全表扫描警告
    if let Some(like) = extract_like_leaf(where_expr) {
        if !like.is_prefix {
            log::warn!(
                "LIKE '%{}' on field '{}' will trigger full table scan, \
                 consider using inverted full-text index or add AND with inverted field",
                like.pattern, like.field
            );
        }
    }

    // OFFSET > 10000 → 深分页警告
    if let Some(ls) = limit_spec {
        if ls.offset > 10000 {
            log::warn!(
                "Large OFFSET {} detected ({}+{} rows to fetch), \
                 consider using keyset pagination (WHERE id > last_id ORDER BY id LIMIT N)",
                ls.offset, ls.offset, ls.limit.unwrap_or(0)
            );
        }
    }
}
```

### 5.2 LIMIT 下推感知 offset

（来自 `like_offset_design.md` 改动点 E）

```
LIMIT 下推时，下推的是 total = limit + offset（不是仅 limit）。
例如 SELECT ... LIMIT 10 OFFSET 1000：
  - 倒排查位图：取前 1010 个 docid（不是 10 个）
  - 全扫：扫到 1010 行即停（不是 10 行）
  - JOIN 流式：产出 1010 行即停（不是 10 行）
  - 最终结果跳过前 1000 个，取后面 10 个
```

---

## 六、改造边界与风险控制

### 6.1 改造范围

| 文件 | 改动 |
|------|------|
| `src/sqlish.rs` | 新增 `LimitSpec` 结构体 + `DocIdSet` 枚举 + `get_docid_set` 函数 + 改 `execute` + 改 `execute_join` |
| `src/sqlish.rs` | 删除：组合索引路由（并入 `get_docid_set`）、等值回退早停（并入）、裸比较 `scan_pushdown`（并入） |
| `src/sqlish.rs` | 保留：`GROUP BY`/`ORDER BY`/`HAVING`/`IN`/聚合 `eval` 函数 |
| `src/sqlish.rs` | 解析器：新增 `WhereExpr::Like` 枚举变体 + LIKE 解析 |
| `src/sqlish.rs` | `topk_sort` 改造为支持 offset |

### 6.2 风险控制

| 风险 | 等级 | 缓解措施 |
|------|------|----------|
| LEFT JOIN 结果不一致 | 🔴 高 | 保留现有 LEFT JOIN 的"left 逐行 → right_cache 查 → 匹配/NULL"逻辑不变，只把 WHERE 过滤部分替换为 `get_docid_set` |
| JOIN 字段匹配错误 | 🔴 高 | 非 docid 字段的 JOIN 必须回表后行级判定，不能依赖 `DocIdSet.intersect` 的结果 |
| Stream 物化 OOM | 🟡 中 | `materialize_to_bitmap` 对 Stream 直接报错 |
| OFFSET 实现错误 | 🟡 中 | 所有路径统一用 `LimitSpec` 管理，确保 `skip(offset) + take(limit)` 在每处实现一致 |
| LIKE 前缀通配慢 | 🟡 中 | 输出警告提示用户优化；如果与其他倒排条件 AND，只在倒排结果上过滤 |
| 现有测试回归 | 🟢 低 | 改造后运行 `cargo test` 647 测试全过 |
| 性能退化 | 🟢 低 | 核心路径（倒排/scan/回表）不变，新增的 DocIdSet 是零开销抽象（枚举 dispatch） |

### 6.3 测试策略

1. **DocIdSet 单元测试**：intersect 所有组合（12 种），验证正确性
2. **LimitSpec 单元测试**：`total_to_fetch()` / `can_early_stop()` 边界
3. **get_docid_set 单元测试**：单表等值/范围/AND/OR/NOT/LIKE 产出正确 DocIdSet 类型
4. **OFFSET 单元测试**：每种形态（Bitmap/SortedList/Stream）的 skip+take 正确
5. **LIKE 单元测试**：前缀通配/后缀通配/无通配符的执行路径正确
6. **execute 回归测试**：改造前后结果一致
7. **execute_join 回归测试**：
   - INNER JOIN 2 表（等值/非 docid 字段）
   - LEFT JOIN 左表全保留 + NULL 填充
   - 从表 1:N 展开（当前已有测试 L3632+）
   - 广播哈希 JOIN（右表小）
   - 拒绝场景（非等值 JOIN、多表 JOIN）

---

## 七、工作量汇总

| 任务 | 行数 | 来源 |
|------|------|------|
| 新增 `LimitSpec` 结构体 + 方法 | ~20 | `like_offset_design.md` |
| 新增 `DocIdSet` 枚举 + intersect + to_vec + iter | ~100 | 本文 |
| 新增 `get_docid_set` 阶梯选择（含 LIKE） | ~120 | 本文 + `like_offset_design.md` |
| 解析器：新增 `WhereExpr::Like` + LIKE 解析 | ~30 | `like_offset_design.md` |
| 重构 `execute()`（替换 eval + 各路径 offset 支持） | ~80 | 本文 + `like_offset_design.md` |
| 重构 `execute_join()`（按 8 阶段流程） | ~150 | 本文 |
| `topk_sort` 适配 offset | ~30 | `like_offset_design.md` |
| 阶段 0 安全检查（LIKE/OFFSET 警告） | ~15 | `like_offset_design.md` |
| 单元测试 | ~150 | 本文 |
| **合计** | **~695 行** | |

---

## 八、开发步骤（待用户指令后执行）

```
Step 0: 新增 LimitSpec 结构体 + 辅助方法
Step 1: 新增 DocIdSet 枚举 + intersect + to_vec + iter
Step 2: 解析器：新增 WhereExpr::Like + LIKE 解析分类
Step 3: 修改 get_docid_set 签名 → 支持 LimitSpec + LIKE
Step 4: 实现 get_docid_set 阶梯选择（含 LIKE 分支）
Step 5: 重构 execute() → 替换 eval 为 get_docid_set，各路径支持 offset
Step 6: 重构 execute_join() → 按 8 阶段流程，支持 LIMIT+OFFSET 下推
Step 7: Top-K 排序适配 offset
Step 8: 阶段 0 安全检查添加 LIKE/OFFSET 警告
Step 9: 运行全量测试，修复回归
Step 10: 更新 development_remain.md 状态
```

---

## 九、写路径（UPDATE/DELETE）整合设计

### 9.1 写操作边界（用户 2026-09-04 定稿）

```
SELECT / JOIN / GROUP BY / ORDER BY / COUNT → ✅ 走优化器
INSERT / 批量 INSERT                       → ❌ 不走优化器（写 WAL+MemTable，路径固定）
UPDATE / DELETE / 批量 UPDATE / 批量 DELETE → 分两段：
    - 定位行（WHERE 收敛 → DocIdSet）     → ✅ 走 get_docid_set（读）
    - 定位后的写（新版本/墓碑/倒排/位图）  → ❌ 写执行器批量管道
DELETE FROM t（无 WHERE）                  → ❌ 不走优化器（DROP/TRUNCATE 表级特殊路径）
```

**原因**：写瓶颈在存储层（WAL fsync / MemTable / Compaction / 组提交），不在路径选择。
优化器是给读选索引的，写路径无可选分支。UPDATE/DELETE 是"读+写"混合操作：
读部分（WHERE 收敛）需要 `get_docid_set` 选路径，写部分直接进写执行器。

### 9.2 带 WHERE 写语句（UPDATE/DELETE）的定位：走优化器主流程（兼容/扩展设计）

> **用户 2026-09-04 确认：INSERT / UPDATE / DELETE 只要有 WHERE（定位语义），就要走优化器主流程
> （DocIdSet / get_docid_set 8 阶段），不是旁路。** 写路径与读路径的关系是：
> 「定位 = 读（走优化器主流程）」+「定位后写 = 独立写执行器」。本节给出兼容/扩展设计。

#### 9.2.1 现状与目标差异

| 语句 | 当前定位实现 | 缺口 | 目标 |
|------|-------------|------|------|
| `UPDATE … WHERE id=N` | db_adapter `parse_update` 字符串切 `id=N` → 单点 | 只认主键单点/`id IN`；字段条件走 sqlish 全扫 | WHERE 统一走 `get_docid_set` |
| `UPDATE … WHERE <字段条件>` | `resolve_where_ids` → `sqlish::execute("SELECT docid …")` | sqlish 全扫物化 Vec（cap 200_000），不享受倒排/组合索引 | 同左 → 倒排位图/组合索引收敛 |
| `DELETE … WHERE id BETWEEN a AND b` | `resolve_where_ids` → sqlish 全扫 → 逐行 `engine.delete` | 逐行 fsync（6729×）；字段条件全扫 | 主键区间 → keys-only 扫描 → `delete_batch` |
| `DELETE … WHERE <字段条件>` | `resolve_where_ids` → sqlish 全扫 → 逐行删 | 同上 | 倒排/组合索引收敛 → `delete_batch` |
| `DELETE FROM t`（无 WHERE） | `drop_table_range` | 无 | **不走优化器**（表级特殊路径，保持不变） |
| `INSERT` | 无 WHERE，直接写 | 无 | **不走优化器** |
| `INSERT … ON DUPLICATE KEY UPDATE` | 主键/唯一键直接定位 | 无索引可选 | **不走优化器**（主键/唯一键直达） |

**核心兼容点**：优化器主流程（DocIdSet）的产出不是"回表输出行"，而是"**待写 docid 集合**"——
消费端从「batch_get 回表 + 投影输出」换成「批量写/批量删」。上游 8 阶段
（阶段 1 每表 DocIdSet、阶段 2 索引选择、阶段 5 降级链）完全复用。

#### 9.2.2 写定位的调用形态（与读路径共用 get_docid_set）

```
execute_update / execute_delete（db_adapter 或 sqlish 层）：
    ┌─────────────────────────────────────────────────────────┐
    │ ① WHERE 段 → WhereExpr AST（复用 sqlish 解析器）        │
    │    - 现 resolve_where_ids 是字符串级解析，只认 id=/id IN│
    │    - 扩展：parse_where_expr() → WhereExpr（同 SELECT）   │
    └─────────────────────────────────────────────────────────┘
        ▼
    ┌─────────────────────────────────────────────────────────┐
    │ ② get_docid_set(table_id, where_expr, limit=None, guard)│
    │    → DocIdSet（优化器主流程阶段 1 全阶梯）              │
    │    - id=N / id IN      → Bitmap / SortedList            │
    │    - id BETWEEN 主键   → 主键区间扫描（keys-only）→ SortedList/Stream│
    │    - 字段等值（倒排）   → Bitmap（零全扫）               │
    │    - 字段范围 + 倒排候选→ 混合扫描                       │
    │    - 无索引范围/无索引  → 全扫 + Zone Map（同 SELECT 兜底）│
    │    limit = None：写定位**不可截断**（须全量收敛）        │
    └─────────────────────────────────────────────────────────┘
        ▼
    ┌─────────────────────────────────────────────────────────┐
    │ ③ 物化为可消费集合（&Engine → &mut Engine 桥）          │
    │    DocIdSet 消费规则（写路径专用）：                    │
    │    - Bitmap / SortedList → 直接迭代（已是物化）         │
    │    - Stream（惰性，借 &Engine）→ 分批物化 Vec 再写      │
    │      ⚠️ Rust 借用：写需 &mut Engine，Stream 借 &self     │
    │      不能边扫边写 → Stream 先 chunk 物化（≤ N=64K）     │
    │      或：写路径优先选 Bitmap/SortedList（倒排/区间形态） │
    └─────────────────────────────────────────────────────────┘
        ▼
    ┌─────────────────────────────────────────────────────────┐
    │ ④ 批量写（写执行器，不走优化器）                        │
    │    UPDATE → batch_get(1000) → 构建新文档 → put_batch    │
    │    DELETE → engine.delete_batch(iter)                   │
    └─────────────────────────────────────────────────────────┘
```

#### 9.2.3 关键设计决策（兼容性约束）

| # | 决策 | 理由 |
|---|------|------|
| D1 | **写定位 limit=None（不截断）** | SELECT 的 LIMIT 是用户语义；写语句 WHERE 定位必须全量，截断 = 数据错删/漏改 |
| D2 | **DocIdSet 消费桥**：写路径需 `&mut Engine`，`get_docid_set` 内 Stream 借 `&Engine` | Rust 借用：先产出 DocIdSet（含惰性 Stream），消费时若为 Stream → **分批物化 chunk（≤64K）→ delete_batch**；Bitmap/SortedList 直接消费。避免「scan_stream 回调内调 &mut delete」的双重借用 |
| D3 | **写路径优先选物化形态** | 倒排等值/主键区间天然产 Bitmap/SortedList（内存可控 ≤ 命中行）；全扫 Stream 用于无索引大范围，须配合批量删防逐行 fsync |
| D4 | **多表隔离**（§26）：`route_where_ids` 表区间过滤逻辑保留 | DocIdSet docid 已含 table_id 高位；非目标表 docid 天然不在本表区间。字段条件查询须按表区间收敛（真多表下 sqlish 是全库口径 → 须加 tid 过滤） |
| D5 | **事务内写**：`txn_update/txn_delete` 走事务路径，不重复接线 | 事务写已由快照点查 + write_set 攒批 + commit 原子落库；优化器接线仅限非事务路径（本阶段），事务路径定位语义（快照可见性）不变 |
| D6 | **无 WHERE 全表 DELETE / TRUNCATE / DROP**：不走优化器 | 表级特殊路径（`drop_table_range`），无 WHERE 收敛需求 |
| D7 | **INSERT 系列**：无 WHERE 或主键/唯一键直达 → 不走优化器 | INSERT 没有"路径选择"（必须写 WAL+MemTable）；冲突检测是主键/唯一键点查，无倒排/全扫之争 |
| D8 | **写锁语义**：定位是读（可读锁），定位后写是写锁 | db_adapter 现按"语句含写关键字 → 整语句拿写锁"（`is_read_statement` 粗粒度）。优化器接线后**同一语句内先读后写**仍在一把写锁内执行（锁粒度不变，只提升定位效率）；如需读锁并行优化 → 后续细粒度阶段 |

#### 9.2.4 对现有代码的落地改动（供阶段 B/后续排期）

```
db_adapter.rs：
  - resolve_where_ids(engine, where_part) → 新增分支：
      id=N / id IN          → 保持（主键直达，改造为 docid_for 升位）
      id BETWEEN a AND b    → 主键区间：route 到 delete_range 路径（阶段 B2）
      docid BETWEEN 同      → 同上
      字段条件              → 解析 WhereExpr → 倒排/组合索引收敛（依赖阶段 A get_docid_set）
  - delete_response / update_response：逐行 delete/put 循环 → delete_batch / put_batch

sqlish.rs（阶段 A 交付后）：
  - get_docid_set 公开（pub(crate)），供 db_adapter 调用
  - 提供 parse_where_expr 出口（WHERE 段 → WhereExpr）
```

> **阶段归属**：9.2.2 的②③（DocIdSet 定位复用）依赖阶段 A（A2/A4）交付；
> 阶段 B（delete_range50）先行落地 9.2.2 的「主键区间 → keys-only 扫描 → delete_batch」子集
> （不依赖阶段 A，直接用 engine.scan_stream_ids + delete_batch 接线），随后在 A4 完成后
> 字段条件写定位平滑切换到 get_docid_set 全阶梯。

#### 9.2.5 数据结构是否需要扩展？（结论：DocIdSet 不改，只扩消费端接口）

> 用户 2026-09-04 确认。**DocIdSet 本身读写通用——它只负责"产出 docid 集合"，
> 不关心该集合用于"回表输出"还是"批量删除/更新"。**

```
DocIdSet 职责边界：
  ┌─────────────────────────────────────────────────────┐
  │  DocIdSet：Where 条件 → docid 集合（只读定位）    │
  │  形态：Bitmap / SortedList / Stream / Empty / All │
  └─────────────────────────────────────────────────────┘
                         │
                         ▼ 消费端（两种路径）
          ┌──────────────┴──────────────┐
          ▼                              ▼
   读路径（execute）              写路径（DELETE/UPDATE）
   回表 + 投影输出               批量删/批量更新管道
```

| 扩展点 | 位置 | 说明 |
|--------|------|------|
| `Engine::delete_batch` | engine.rs | ✅ 新增（已落地，阶段 B1）：流式消费 docid 迭代器，攒批删（位图/Tombstone/WAL 单次提交） |
| `Engine::update_batch` | engine.rs | ✅ 组合即可，不强制新增：`batch_get`（定位行）+ `put_batch`（批量新版本） |
| `DocIdSet::consume_for_write` | sqlish.rs | ❌ 不需要——写路径直接 `set.iter()` 取 docid 喂 `delete_batch`/`update_batch` |

```
读路径：                         写路径：
let set = get_docid_set();      let set = get_docid_set();   // ← 完全一样
for docid in set.iter() {        delete_batch(set.iter())    // ← 消费方式不同
    let row = engine.get(docid); //  写执行器内部攒批
    output.push(row);
}
```

| 组件 | 是否扩展 | 说明 |
|------|----------|------|
| `DocIdSet` | ❌ 不扩展 | 定位职责不变（只产 docid 集合） |
| `get_docid_set` | ❌ 不扩展 | `limit=None` 即可（写定位不截断，D1） |
| `Engine::delete_batch` | ✅ 新增 | 批量删原语（阶段 B1 已落地） |
| `Engine::update_batch` | ✅ 组合 | `batch_get` + `put_batch`，不强制新增独立原语 |

> **核心原则：DocIdSet 是"定位器"不是"执行器"。定位与消费分离，
> 数据结构无需为写路径做任何改动——写路径只是换了 DocIdSet 的消费端。**

### 9.3 单点 UPDATE/DELETE（复用读路径定位）

```
UPDATE t SET status='done' WHERE id=100：
    1. get_docid_set(WHERE id=100) → SortedList([100])   // 读，走优化器
    2. engine.get(100) → 旧 Document                     // 读
    3. apply_updates → 新 Document                       // 内存
    4. engine.put(100, 新doc)                            // 写执行器（WAL+MemTable+倒排）

DELETE FROM t WHERE id=100：
    1. get_docid_set(WHERE id=100) → SortedList([100])   // 读，走优化器
    2. engine.delete(100)                                // 写执行器（位图/墓碑）
```

### 9.4 范围/批量 UPDATE/DELETE（delete_range50 修复 —— 性能项 ①）

**现状缺陷**（db_adapter.rs `delete_response` L3220-3255）：
`resolve_where_ids` 先 `sqlish::execute("SELECT docid ...")` **全量物化候选 Vec**（cap 200_000），
再 `for id in ids { engine.delete(id) }` **逐行点删**——每个 delete 独立走
HotCache 失效 + 位图置位 + memtable 墓碑 + delta 删前缀 + watchdog → 范围删 50 万行慢 6729×。

**修复设计：流式定位 + 批量删管道**

```
DELETE FROM t WHERE <范围条件>（如 docid BETWEEN a AND b / status='x'）：
    ┌─────────────────────────────────────────────────────────┐
    │ 定位：get_docid_set → DocIdSet                          │
    │   - 主键范围（id BETWEEN）→ 主键区间扫描 → Stream       │
    │   - 字段条件 → 倒排/全扫 → Bitmap/Stream                │
    │   ⚠️ 严禁先 to_vec() 物化大候选再循环                   │
    └─────────────────────────────────────────────────────────┘
        │ 流式消费（不物化整表）
        ▼
    ┌─────────────────────────────────────────────────────────┐
    │ 批量删：engine.delete_batch(iter<docid>)                │
    │   - 内部每 N=1024 攒批：                                │
    │     ① HotCache.invalidate 批量                          │
    │     ② 删除位图批量置位（mark_deleted 批量）            │
    │     ③ primary.delete_record_mem 批量墓碑（一次 memtable write batch）│
    │     ④ delta.delete_prefix 批量                          │
    │     ⑤ 一次 watchdog.check_all（每批）                  │
    │   - 批尾一次 maybe_group_commit / flush_wal             │
    └─────────────────────────────────────────────────────────┘
```

**需要的引擎新原语**（engine.rs）：

```rust
/// 批量删除：流式消费 docid（迭代器），攒批写墓碑/位图，批尾统一提交。
/// 相比逐行 engine.delete()：HotCache 失效/位图置位/memtable 写入均为批量，
/// watchdog 每批检查一次；语义与逐行 delete 完全一致（幂等）。
pub fn delete_batch<I: Iterator<Item = u64>>(&mut self, docids: I) -> Result<u64> { ... }
```

**复用点**：`resolve_where_ids` 改为：
- `id=N` / `id IN (...)` 单点/小集合 → 保持现状（数量少，点删开销可忽略）
- 字段/范围条件 → 改用 `sqlish::execute_stream` / `get_docid_set` 流式定位 → `delete_batch`

**收益预估**：50 万行范围删从 逐行 O(N × 点删开销) → 攒批 O(N/1024 × 批开销)，
写放大从 N 次 WAL record 降到 N/1024 次 batch record；MySQL 同档位可期。

### 9.5 UPDATE 批量管道（结构对称，不单独排期）

```
批量 UPDATE：
    get_docid_set → 分批 batch_get(1000) → 逐批构建新 Document
    → 批量 put（攒批 WAL）→ 批量更新倒排（add_batch）→ 删除位图批量置旧版本
```

> 注：当前 UPDATE 主键单点路径（parse_update）已存在；批量字段条件 UPDATE
> （`UPDATE t SET x=1 WHERE status='active'`）走 sqlish 全扫枚举 → 逐行 engine.put。
> 本节为远期优化目标，不阻塞本节 DocIdSet 读路径改造。

### 9.6 PAX 聚合接线（性能项 ②）—— 独立引擎层工作，与 DocIdSet 正交

**现状**（P3-B 遗留缺口）：
- `decode_pax_block_column`（sstable.rs:1799）已实现 pub，**零调用点**（死代码）
- `FieldZone.sum`（sstable.rs:151）v6 已落盘可解析，**生产零消费方**
- sqlish 聚合全扫 `scan_stream` 整行回调（execute_aggregate_window:2226 / group_by_window:2713），
  PAX 块经 `decode_pax_block`（1694）**整行重组**再逐字段取

**接线点**（与 DocIdSet 框架正交，消费端优化）：
1. `SstRangeIter.advance_block`（sstable.rs:1504-1513）：PAX 块按需走 `decode_pax_block_column`
   单列投影（聚合只需目标列）——需 scan API 传入"投影列集合"
2. 引擎新增"聚合扫描"入口：暴露块级 `IndexEntry.zones[]`（sum/present_count）下推
   SUM/AVG 跨块累加，跳过数据块读取（与倒排统计载荷正交补充）
3. 语义约束：块级 sum 仅当块内无 tombstone 覆盖、无 memtable/delta 未刷盘混入时可信
   （MVCC 版本折叠语义），否则须回退行级累加——接线时不可绕开的正确性点

> **排期**：本节（DocIdSet 整合）与 delete_range50 修复优先；PAX 接线单独立项，不并入本次重构。

### 9.7 写路径改造范围汇总

| 项 | 文件 | 改动 | 优先级 |
|----|------|------|--------|
| `Engine::delete_batch` | src/engine.rs | 新增批量删原语 | 🔴 本次（delete_range50） |
| `delete_response` 范围分支 | src/db_adapter.rs | resolve_where_ids 范围条件改流式 | 🔴 本次 |
| `resolve_where_ids` | src/db_adapter.rs | 字段/范围条件 → 流式 DocIdSet 定位 | 🔴 本次 |
| UPDATE 批量管道 | src/db_adapter.rs | 远期（不阻塞） | 🟢 远期 |
| PAX 聚合接线 | src/sstable.rs / engine / sqlish | 独立立项 | 🟡 后续单独 |

---

## 十、开发步骤（合并版，按优先级排序）

```
阶段 A（DocIdSet 读路径重构 —— 本设计主交付）：
  A1. LimitSpec 结构体 + 辅助方法
  A2. DocIdSet 枚举 + intersect/to_vec/iter/len_estimate
  A3. 解析器 WhereExpr::Like + LIKE 分类
  A4. get_docid_set 阶梯选择（含 LIKE）
  A5. execute() 消费 DocIdSet + offset
  A6. execute_join() 重构 + 8 阶段
  A7. Top-K 排序 offset + 阶段 0 安全警告
  A8. 全量测试 + development_remain 更新

阶段 B（写路径 delete_range50 修复）：
  B1. Engine::delete_batch 批量删原语（含单测）
  B2. delete_response 主键区间（id BETWEEN）→ keys-only 扫描 + delete_batch（不依赖阶段 A）
  B3. resolve_where_ids 字段条件 → 解析 WhereExpr → 倒排收敛（阶段 A 交付后切换 get_docid_set 全阶梯）
  B4. 范围删 50 万行 A/B 对照（MySQL 同档位）
  B5. development_remain 更新 + problem_solving 记录

阶段 C（PAX 聚合接线 —— 独立立项，待 A/B 完成后评估）
```

> 本设计文档完成后，请等待用户指令再开始开发。