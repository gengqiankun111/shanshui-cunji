优化器主流程（文字版）
整体策略：确定性阶梯降级，不做代价估算
text
对于任意一条 SQL，执行以下 8 个阶段，每个阶段如果能匹配就走该路径，否则降级到下一阶段。
阶段 0：安全与可行性检查（入口护城河）
触发时机：任何查询进入优化器的第一步。

检查清单：

如果 WHERE 条件中有非等值 JOIN（>, <, LIKE, != 关联两表）→ 直接拒绝，返回 1064（倒排不支持非等值 JOIN）

如果 JOIN 表数 ≥ 3 → 直接拒绝，返回 1064（建议拆分为多个查询）

如果 HAVING 中包含子查询 → 直接拒绝（子查询暂不支持）

如果 ORDER BY 的字段既不是主键也不是倒排字段，且无 LIMIT → 降级到全量排序守卫（候选集 > 20 万行即拒绝）

如果 JOIN 右表估行 > 100 万，且 JOIN 字段两边都无倒排 → 直接拒绝（避免 OOM）

不通过 → 返回错误，不走后续阶段

阶段 1：JOIN 路径选择（扩展版）

开始
  │
  ▼
┌─────────────────────────────────────────────────────────────┐
│ 1.0 安全检查                                              │
│ - 非等值 JOIN? → 拒绝                                     │
│ - 表数 ≥ 3? → 拒绝                                        │
│ - JOIN 字段无索引且右表 > 10万? → 拒绝                    │
└─────────────────────────────────────────────────────────────┘
  │ 通过
  ▼
┌─────────────────────────────────────────────────────────────┐
│ 1.1 主键 JOIN 检测（最快路径）                            │
│ 条件：a.id = b.id（或 b.id = a.id）                      │
│ 执行：                                                    │
│   1. left_set = get_docid_set(left_table)                 │
│   2. left_docs = left_set.to_vec()  // 提取所有 docid     │
│   3. 用 left_docs 直接回表取右表行                        │
│   4. 同时回表取左表行                                     │
│   5. 按 JOIN 类型组装输出                                 │
└─────────────────────────────────────────────────────────────┘
  │ 不是主键 JOIN
  ▼
┌─────────────────────────────────────────────────────────────┐
│ 1.2 索引-索引 JOIN                                       │
│ 条件：两边都能产出 DocIdSet（倒排 OR 组合索引）          │
│ 执行：                                                    │
│   1. left_set = get_docid_set(left_table)                 │
│   2. right_set = get_docid_set(right_table)               │
│   3. 先对两表做"自身条件"的交集（缩小数据量）            │
│   4. 再按 JOIN 字段做交集：                               │
│      a. 如果 JOIN 字段是 docid → 走 1.1                  │
│      b. 如果 JOIN 字段是倒排 → 对两边取倒排位图再 AND    │
│      c. 如果 JOIN 字段无索引 → 回退到过滤                │
│   5. 最终匹配的 docid → 批量回表                         │
└─────────────────────────────────────────────────────────────┘
  │ 只有一边能产出 DocIdSet
  ▼
┌─────────────────────────────────────────────────────────────┐
│ 1.3 有索引-无索引 JOIN                                    │
│ 条件：一边能产出 DocIdSet，另一边无索引                 │
│ 执行：                                                    │
│   1. indexed_set = get_docid_set(indexed_table)           │
│   2. 无索引表全扫流式遍历                                 │
│   3. 对每行检查：a. 该行 docid 在 indexed_set 中；       │
│                 b. JOIN 条件匹配                         │
│   4. 匹配则输出                                           │
│ 安全阀：如果无索引表 > 500 万行 → 拒绝                   │
└─────────────────────────────────────────────────────────────┘
  │ 两边都无索引
  ▼
┌─────────────────────────────────────────────────────────────┐
│ 1.4 广播哈希 JOIN（兜底）                                 │
│ 条件：右表 < 10 万行                                      │
│ 执行：全扫右表建哈希 → 左表全扫查哈希                    │
└─────────────────────────────────────────────────────────────┘
  │ 不满足
  ▼
┌─────────────────────────────────────────────────────────────┐
│ 1.5 拒绝执行                                              │
│ 返回 1064：建议为 JOIN 字段建倒排或组合索引              │
└─────────────────────────────────────────────────────────────┘

预估耗时：匹配 1 万行 → <1ms；匹配 100 万行 → ~500ms

1.2 倒排-主键 JOIN（次优）

检查条件：左表 JOIN 字段有倒排 且 右表 JOIN 字段是主键（docid）

执行路径：

从倒排中取出左表 Term 的位图（BitmapA）
该位图中的 docid 直接作为右表主键 → 不需要额外位图运算
直接批量回表取右表行（按 BitmapA 中的 docid）
左表行从倒排查到后直接取（或走 HotCache）
剩余 WHERE/LIMIT 下推到回表后
复杂度：O(位图大小 + 匹配行数回表)

预估耗时：匹配 1 万行 → <1ms；匹配 100 万行 → ~300ms（比倒排-倒排少一次位图 AND）

1.3 对称：主键-倒排 JOIN

检查条件：左表 JOIN 字段是主键 且 右表 JOIN 字段有倒排

执行路径：与 1.2 对称，方向相反

复杂度/耗时：同上

1.4 广播哈希 JOIN（兜底）

检查条件：

两边 JOIN 字段都无倒排

右表估行 < 10 万行（硬上限）

执行路径：

全扫右表所有行，建立 HashMap<join_key, Vec<docid>>
左表流式遍历，每行查 HashMap 找匹配的右表 docid
匹配后回表取右表行（或从哈希中直接取行，如果缓存在内存）
如果左表也有 WHERE 条件 → 流式遍历前先过滤
内存预算：右表 10 万行 × 平均 1KB = 100MB（可接受）

预估耗时：右表扫 10 万行 (~0.5s) + 左表扫 N 行 + 回表匹配行

1.5 无倒排且右表大 → 拒绝执行

返回错误：JOIN on fieldX = fieldY requires inverted index on at least one side, or right table must be <100k rows

JOIN 阶段输出：

输出一个“JOIN 后的 docid 结果集”（可能是位图，也可能是流式迭代器）

带上 JOIN 类型标记（INNER/LEFT/RIGHT），供后续阶段处理空值

阶段 2：WHERE 条件下推（JOIN 前后）
注：此阶段在 JOIN 前/后都执行，是贯穿全流程的优化。

2.1 单表 WHERE 条件下推到 JOIN 前（减少 JOIN 数据量）

如果 WHERE 条件只涉及左表 → 在 JOIN 前先过滤左表

如果 WHERE 条件只涉及右表 → 在 JOIN 前先过滤右表（尤其对广播哈希 JOIN 意义大）

执行路径：

如果过滤字段是倒排 → 走倒排查位图

如果过滤字段是组合索引 → 走组合索引前缀

否则 → 全扫 + 行过滤（Zone Map 剪枝）

2.2 涉及两表的 WHERE 条件

如果 WHERE 条件同时涉及左表和右表（如 a.amount > b.amount）→ 无法下推，必须在 JOIN 后行过滤

如果 WHERE 条件只涉及 JOIN 结果（如 a.status = 'active' AND b.region = 'us'）→ 分别下推到两表

2.3 倒排过滤器合并（重要优化）

如果同一张表有多个倒排条件（如 status='active' AND region='us'）→ 位图 AND

如果条件来自多张表且 JOIN 前可独立过滤 → 分别用位图 AND，JOIN 时再做一次位图 AND

阶段 3：扫描路径选择（无 JOIN 或 JOIN 后）
如果查询无 JOIN：直接从下表选择扫描路径。

如果查询有 JOIN：JOIN 已经产生了一个“中间结果集”（位图或流式迭代器），后续阶段基于这个结果集操作。

扫描路径选择阶梯：

3.1 组合索引扫描（最精确）

触发条件：WHERE 条件匹配某个组合索引的最左前缀

执行：走 cidx 列族，前缀扫描 + 回表取行

适用：status='active' AND ts BETWEEN 这类精确条件

3.2 倒排位图扫描（次优）

触发条件：WHERE 中有等值条件匹配倒排字段

执行：取 Term 位图 → 批量回表取行

容错：如果位图候选集 > 50 万且无 LIMIT → 降级到 3.3 或 3.4

3.3 混合扫描（倒排候选 + 范围过滤）

触发条件：有倒排条件做候选，但还有非倒排字段的范围条件（如 amount > 5000）

执行：先取倒排位图候选 → 分批 batch_get 回表 → 内存中过滤范围条件

容错：如果候选集 > 100 万且无 LIMIT → 降级到 3.4

3.4 全表扫描 + Zone Map 剪枝

触发条件：有范围条件（>, <, BETWEEN）但无倒排可用

执行：SST 扫描时检查每个 Block 的 Zone Map（min/max），跳过不相交的 Block

适用：amount BETWEEN 100 AND 200（amount 无倒排）

3.5 全表扫描 keys-only（COUNT 专用）

触发条件：COUNT(*) 且无 WHERE

执行：走 count_keys_range 只遍历键，不解值

注意：这是 COUNT 的最快路径，但仍然要扫所有键

3.6 全表扫描兜底

触发条件：没有任何条件（SELECT * FROM t）

执行：全扫所有 SST，流式返回

安全阀：如果无 LIMIT，返回警告但不拒绝（用户可能真要全导出）

阶段 4：GROUP BY 路径选择
触发条件：GROUP BY 子句存在。

4.1 倒排统计载荷 GROUP BY（最优）

触发条件：GROUP BY 字段是 bitmap_field（倒排字段）且 所有聚合函数支持载荷（COUNT/SUM/AVG/MIN/MAX）

执行路径：

遍历该字段的所有 Term
从倒排 Term 元数据中直接读取每个 Term 的 doc_count、sum、avg 等统计载荷
如果还有 WHERE 条件 → 先用倒排查位图，再对位图做聚合（部分降级，但比全扫快）
零回表、零解行
复杂度：O(Term 数量)，与数据量无关

预估耗时：100 个 Term → <0.1ms；10 万个 Term → ~5ms

这是你系统对 MySQL 的最大优势

4.2 全扫 GROUP BY + 哈希聚合（兜底）

触发条件：GROUP BY 字段无倒排统计载荷

执行路径：

全扫所有行（可配合 Zone Map 剪枝）
每行提取 GROUP BY 字段值 → 累加到哈希表
最终哈希表输出分组结果
内存预算：哈希表大小 = GROUP BY 唯一值数量 × 聚合状态

安全阀：如果唯一值 > 100 万，触发降级到外部排序（或拒绝）

预估耗时：3000 万行 × 解行 → ~10-30s

阶段 5：HAVING 过滤
触发条件：HAVING 子句存在。

执行规则：

HAVING 在 GROUP BY 之后执行

对聚合结果做行过滤（如 HAVING COUNT(*) > 100）

无法下推到 GROUP BY 之前（语义决定）

如果 HAVING 条件简单（如 HAVING COUNT(*) > 100）→ 在聚合过程中做“早期截断”：如果某个组的 COUNT 达到阈值，可提前停止该组的聚合（但需要确认不会再增长）

安全提醒：HAVING 不会显著影响性能（因为 GROUP BY 已经聚合了数据量）

阶段 6：ORDER BY 排序路径选择
触发条件：ORDER BY 子句存在。

6.1 Top-K 堆排序（最优）

触发条件：有 LIMIT k 且 k <= 10000

执行路径：

上游查询（扫描/JOIN/聚合）流式产出行
每行插入一个大小 = k 的 BinaryHeap（最小堆/最大堆）
遍历结束后，堆中就是 Top-K 行
按 ORDER BY 顺序弹出输出
内存：O(k)，不随数据量增长

预估耗时：O(N·log k)，3000 万行 × log(100) ≈ 1-3s

6.2 全量排序（次优，带守卫）

触发条件：有 ORDER BY 但无 LIMIT，或 LIMIT > 10000

执行路径：

上游查询产出所有行 → 收集到 Vec<Row>
全部收集完后调用 Rust 的 sort_by 排序
输出排序后全部行
安全阀：如果行数 > 200,000 → 拒绝执行（返回 1064，提示加 LIMIT 或 WHERE 收敛）

预估耗时：20 万行全量排序 ≈ 100-200ms；3000 万行全量排序 ≈ 10-30s（但会被守卫拦截）

6.3 排序与倒排的联动优化

触发条件：ORDER BY 字段是倒排字段，且查询有 WHERE 条件

执行路径：

先用 WHERE 倒排查位图 → 候选 docid 集合（可能很大）
对候选 docid 集合按 docid 排序取前 k 个（因为 docid 编码里可能包含时间/顺序信息）
再回表取这 k 行
收益：避免回表 100 万行再排序，只回表 k 行

适用场景：WHERE status='active' ORDER BY id LIMIT 100

阶段 7：LIMIT 下推（贯穿全流程）
核心原则：LIMIT 越早执行越好。

下推路径：

倒排查询：位图迭代时只取前 k 个 docid → 回表只回 k 行

全表扫描：扫描时计数，达到 k 行立即终止

JOIN：JOIN 过程流式产出匹配行，达到 k 行立即停止 JOIN（不需要等全部匹配完）

ORDER BY + LIMIT：Top-K 堆排序天然配合

GROUP BY + LIMIT：需要在哈希聚合完成后才能截断（但可以减少输出行数）

注意：如果查询有 OFFSET，LIMIT 下推要带上 offset（需要跳过前 offset 行），可能无法提前终止。

阶段 8：执行计划打包与执行
最终输出：一个 ExecutionPlan 枚举，包含：

text
ExecutionPlan {
    plan_type: 单表扫描 | 聚合 | 排序 | JOIN | 组合,
    inner_plans: 子计划列表,
    filters: 下推后的过滤条件,
    limit: 下推后的 LIMIT,
    order_by: 排序规格,
    estimated_rows: 估算行数（用于安全阀）,
    memory_budget: 预估内存 (MB),
}
执行器按计划执行，如果执行中超过内存预算 → 动态降级：

哈希聚合内存 > 阈值 → 切换为外部排序聚合

JOIN 哈希表 > 阈值 → 切换为嵌套循环（但可能很慢，需告警）

完整流程图（文字版）
SQL 语句
    ↓
┌─────────────────────────────────────────────────────────────┐
│ 阶段 0：安全与可行性检查                                    │
│ - 非等值 JOIN？ → 拒绝                                     │
│ - 多表 JOIN ≥3？ → 拒绝                                    │
│ - 子查询？ → 拒绝                                          │
│ - JOIN 右表 > 100 万 且 两边都无索引？ → 拒绝              │
│ - 无 LIMIT 的全量排序？ → 守卫拦截                         │
└─────────────────────────────────────────────────────────────┘
    ↓（通过）
┌─────────────────────────────────────────────────────────────┐
│ 阶段 1：分别处理每张表的 WHERE（统一产 docid 集合）        │
│                                                             │
│ 对每张表独立执行 get_docid_set(table, filters)：           │
│                                                             │
│   1.1 组合索引前缀匹配 → DocIdSet::SortedList              │
│       条件：WHERE 条件匹配组合索引最左前缀                  │
│       执行：cidx 列族前缀扫描 → 产出有序 docid 列表        │
│                                                             │
│   1.2 倒排查等值 → DocIdSet::Bitmap                        │
│       条件：WHERE 中等值条件匹配倒排字段                   │
│       执行：Term → RoaringTreemap 位图                     │
│                                                             │
│   1.3 多个倒排条件合并 → DocIdSet::Bitmap                   │
│       条件：有多个倒排条件（如 status AND region）          │
│       执行：多个位图 AND → 合并位图                        │
│                                                             │
│   1.4 混合扫描 → DocIdSet::SortedList（动态物化）          │
│       条件：倒排候选 + 非索引列范围条件（amount > 100）    │
│       执行：先取倒排位图 → 分批 batch_get 回表 → 过滤      │
│       安全阀：候选集 > 50 万 且 无 LIMIT → 降级到 1.5     │
│                                                             │
│   1.5 全扫 + Zone Map 剪枝 → DocIdSet::Stream              │
│       条件：有范围条件（BETWEEN / > / <）且无倒排可用      │
│       执行：SST 扫描时检查 Zone Map（min/max）跳过 Block   │
│                                                             │
│   1.6 keys-only COUNT → DocIdSet::Stream                   │
│       条件：COUNT(*) 且无 WHERE                             │
│       执行：count_keys_range 只遍历键，不解值              │
│                                                             │
│   1.7 全扫兜底 → DocIdSet::Stream                          │
│       条件：无任何过滤条件                                  │
│       执行：全扫所有 SST，流式返回                          │
│                                                             │
│ 输出：每张表一个 DocIdSet                                   │
└─────────────────────────────────────────────────────────────┘
    ↓（每张表都产出了 DocIdSet）
┌─────────────────────────────────────────────────────────────┐
│ 阶段 2：JOIN 路径选择（基于 DocIdSet 统一抽象）            │
│                                                             │
│ 2.1 主键 JOIN（最快）                                      │
│     条件：ON a.id = b.id 或 ON a.id = b.user_id（主键）   │
│     执行：left_set 中的 docid 直接作为右表主键列表         │
│           → 批量回表取右表行，同时回表取左表行             │
│           → 按 JOIN 类型组装                                │
│     复杂度：O(left_set 大小 + 回表行数)                    │
│                                                             │
│ 2.2 索引-索引 JOIN                                         │
│     条件：两边都能产出 DocIdSet（倒排位图 OR 组合索引列表）│
│     执行：                                                  │
│       a. left_set 和 right_set 做交集（DocIdSet.intersect）│
│          - 位图 ∩ 位图 → 位图 AND                          │
│          - 位图 ∩ 列表 → 遍历列表查位图                    │
│          - 列表 ∩ 列表 → 归并排序求交                      │
│       b. 交集结果 = 匹配的 docid 集合                      │
│       c. 如果还有 JOIN 字段过滤（ON a.user_id = b.user_id）│
│          且该字段有倒排 → 再取倒排位图做二次 AND           │
│       d. 批量回表取行                                       │
│     复杂度：O(左集合 + 右集合 + 回表行数)                  │
│                                                             │
│ 2.3 有索引-无索引 JOIN                                     │
│     条件：一边有索引（能产出 DocIdSet），另一边无索引      │
│     执行：                                                  │
│       a. indexed_set = get_docid_set(有索引表)            │
│       b. 无索引表全扫流式遍历，对每行检查：                │
│          - 该行 docid 在 indexed_set 中？                  │
│          - JOIN 条件匹配？                                 │
│       c. 匹配则输出                                        │
│     安全阀：无索引表 > 500 万行 → 拒绝                    │
│     复杂度：O(无索引表行数 × 位图查找)                    │
│                                                             │
│ 2.4 广播哈希 JOIN（兜底）                                  │
│     条件：两边都无索引，右表估行 < 10 万                   │
│     执行：全扫右表建 HashMap<join_key, Vec<docid>>         │
│           → 左表流式遍历查哈希 → 匹配则回表取行            │
│     内存预算：右表 10 万行 × 1KB ≈ 100MB                  │
│     复杂度：O(右表扫描 + 左表扫描 + 回表行数)              │
│                                                             │
│ 2.5 拒绝执行                                               │
│     条件：两边都无索引，且右表 ≥ 10 万行                  │
│     返回：1064，提示为 JOIN 字段建倒排或组合索引           │
└─────────────────────────────────────────────────────────────┘
    ↓（JOIN 完成，得到匹配的 docid 集合）
┌─────────────────────────────────────────────────────────────┐
│ 阶段 3：涉及两表的 WHERE 条件过滤（JOIN 后）               │
│                                                             │
│ - 如果 WHERE 中有跨表条件（如 a.amount > b.amount）        │
│   在 JOIN 结果流上做行过滤                                  │
│ - 无法下推到 JOIN 前，因为需要两表值同时存在               │
└─────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────┐
│ 阶段 4：GROUP BY 路径选择（有 GROUP BY 时）                │
│                                                             │
│ 4.1 倒排统计载荷 GROUP BY（最优，零回表）                  │
│     触发条件：                                              │
│       - GROUP BY 字段是 bitmap_field（倒排字段）            │
│       - 所有聚合函数支持载荷（COUNT/SUM/AVG/MIN/MAX）      │
│     执行：                                                  │
│       - 遍历该字段所有 Term                                 │
│       - 从倒排元数据直接读 doc_count/sum/avg               │
│       - 输出分组结果                                        │
│     预估耗时：100 个 Term → <0.1ms；10 万个 Term → ~5ms   │
│                                                             │
│ 4.2 全扫 + 哈希聚合（兜底）                                 │
│     触发条件：GROUP BY 字段无倒排统计载荷                  │
│     执行：                                                  │
│       - 全扫所有匹配行（可配合 Zone Map 剪枝）              │
│       - 每行提取分组字段 → 累加到哈希表                    │
│       - 最终输出哈希表                                      │
│     内存安全阀：唯一值 > 100 万 → 降级到外部排序或拒绝     │
│     预估耗时：3000 万行 → 10-30s                           │
└─────────────────────────────────────────────────────────────┘
    ↓（有 GROUP BY 时输出聚合结果）
┌─────────────────────────────────────────────────────────────┐
│ 阶段 5：HAVING 过滤                                         │
│                                                             │
│ - 在 GROUP BY 聚合结果上执行行过滤                          │
│ - 无法下推（语义决定）                                      │
│ - 简单 COUNT 条件可早期截断：某组达到阈值即可停止计数      │
└─────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────┐
│ 阶段 6：ORDER BY 路径选择（有 ORDER BY 时）                │
│                                                             │
│ 6.1 Top-K 堆排序（最优）                                   │
│     触发条件：有 LIMIT k 且 k ≤ 10000                      │
│     执行：                                                  │
│       - 上游查询流式产出行                                   │
│       - 每行插入大小 = k 的 BinaryHeap（最小堆/最大堆）    │
│       - 遍历结束，堆中即 Top-K                             │
│     内存：O(k)，不随数据量增长                             │
│     预估耗时：3000 万行 × log(100) ≈ 1-3s                 │
│                                                             │
│ 6.2 全量排序（带守卫）                                      │
│     触发条件：有 ORDER BY 但无 LIMIT，或 LIMIT > 10000     │
│     执行：                                                  │
│       - 上游产出所有行 → 收集到 Vec<Row>                   │
│       - Rust sort_by 全量排序                               │
│     安全阀：行数 > 200,000 → 拒绝（1064）                  │
│                                                             │
│ 6.3 ORDER BY 倒排字段 + WHERE（联动优化）                  │
│     触发条件：ORDER BY 字段是倒排字段，且查询有 WHERE      │
│     执行：                                                  │
│       - 先用 WHERE 倒排查位图 → 候选 docid 集合            │
│       - 从候选集合中按 docid 顺序取前 k 个                 │
│       - 只回表这 k 行                                       │
│     收益：避免回表大量行再排序                              │
└─────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────┐
│ 阶段 7：LIMIT 下推（贯穿全流程）                           │
│                                                             │
│ - 倒排查询：位图迭代只取前 k 个 docid，回表只回 k 行      │
│ - JOIN 流式：达到 k 行立即终止 JOIN                        │
│ - 全扫：计数达到 k 行立即终止扫描                          │
│ - ORDER BY + LIMIT：Top-K 堆排序天然支持                   │
│ - GROUP BY：无法下推（需全部聚合完才能知道分组结果）       │
│ - OFFSET：下推受限，需要跳过前 offset 行，可能无法提前终止│
└─────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────┐
│ 阶段 8：生成 ExecutionPlan 并执行                          │
│                                                             │
│ 输出：ExecutionPlan 枚举（包含所有子计划）                  │
│ 执行器执行计划：                                            │
│   - 如果执行中内存超限 → 动态降级（哈希→外部排序等）       │
│   - 如果执行超时 → 熔断返回错误                            │
└─────────────────────────────────────────────────────────────┘
    ↓
查询结果

关键设计原则总结
原则	说明
确定性阶梯	不依赖统计信息采样，用固定优先级选择执行路径
早停优先	LIMIT 越早执行越好，位图/扫描/JOIN 都支持提前终止
倒排优先	任何能用倒排的地方都用倒排（COUNT/GROUP BY/JOIN/WHERE）
内存硬上限	哈希 JOIN 右表 <10 万行；全量排序 <20 万行；哈希聚合 <100 万唯一值
拒绝优于崩溃	不支持的操作直接 1064，不尝试硬做导致 OOM
JOIN 降级链	倒排-倒排 → 倒排-主键 → 广播哈希 → 拒绝
聚合降级链	倒排统计载荷 → 全扫哈希聚合 → 拒绝
排序降级链	Top-K 堆排序 → 全量排序守卫 → 拒绝
与排期项的映射
排期项	在优化器中的位置
P0-C（MVCC 修复）	执行器底层，优化器不感知
P0-A（组合索引接线）	阶段 3.1（扫描路径）
P0-B（Top-K 排序）	阶段 6.1（排序路径）
P1-C（keys-only COUNT）	阶段 3.5（扫描路径）
P1-D（统计载荷范围扩展）	阶段 4.1（聚合路径）
P2-D（批量回表）	阶段 1/3 的所有回表操作内部
P1-E（Zone Map 剪枝）	阶段 3.4（全扫路径）
JOIN 支持（新增）	阶段 1（JOIN 路径）
结论：这个优化器是纯规则驱动，没有代价模型，代码量控制在 800-1000 行，可以覆盖 95% 的查询模式，且在 3000 万行下不会选错执行计划。它本质上是把你的 LSM-Tree 优势（倒排位图、统计载荷、顺序扫描）固化为查询路径的优先级排序，而不是做动态代价估算。这正好匹配你“20+ 倒排字段”的架构特点。


enum DocIdSet {
    /// 倒排查出的位图（最紧凑，支持 AND/OR 极快）
    Bitmap(RoaringTreemap),
    
    /// 组合索引前缀扫描产出的有序 docid 列表（已排序，支持归并）
    SortedList(Vec<DocId>),
    
    /// 全表扫描产出的流式迭代器（惰性，不预先物化）
    Stream(Box<dyn Iterator<Item = DocId>>),
    
    /// 空集（优化：过滤条件直接冲突，如 status='active' AND status='inactive'）
    Empty,
    
    /// 全集（无过滤条件，不推荐用于 JOIN，会触发降级）
    All,
}

impl DocIdSet {
    /// 与另一个 DocIdSet 做交集
    fn intersect(&self, other: &DocIdSet) -> DocIdSet {
        match (self, other) {
            // 最优：位图 ∩ 位图 → 位图 AND
            (Bitmap(a), Bitmap(b)) => Bitmap(a.and(b)),
            
            // 次优：位图 ∩ 列表 → 遍历列表，在位图中查（如果列表小）
            (Bitmap(bitmap), SortedList(list)) => {
                let result: Vec<DocId> = list.iter()
                    .filter(|docid| bitmap.contains(**docid))
                    .collect();
                SortedList(result)
            }
            
            // 次优：列表 ∩ 位图（对称）
            (SortedList(list), Bitmap(bitmap)) => {
                let result: Vec<DocId> = list.iter()
                    .filter(|docid| bitmap.contains(**docid))
                    .collect();
                SortedList(result)
            }
            
            // 通用：两个有序列表 → 归并求交
            (SortedList(a), SortedList(b)) => {
                let result = merge_intersect(a, b);
                SortedList(result)
            }
            
            // 位图 ∩ 流 → 位图过滤流（惰性）
            (Bitmap(bitmap), Stream(stream)) => {
                let filtered = stream.filter(|docid| bitmap.contains(*docid));
                Stream(Box::new(filtered))
            }
            
            // 空集与任何集合交集 → 空集
            (Empty, _) | (_, Empty) => Empty,
            
            // 全集与任何集合交集 → 另一个集合
            (All, x) | (x, All) => x.clone(),
        }
    }
}

--
/// 对一张表的 WHERE 条件，返回该表所有匹配行的 DocId 集合
fn get_docid_set(
    table: TableId,
    filters: &[FilterCondition],
    limit: Option<usize>,
) -> DocIdSet {
    // 如果没有过滤条件 → 返回 All（但不推荐用于 JOIN）
    if filters.is_empty() {
        return DocIdSet::All;
    }
    
    // 阶梯 1：尝试组合索引（如果有多个条件，匹配最左前缀）
    if let Some(composite_match) = try_composite_index(table, filters) {
        let docids = execute_composite_index_scan(composite_match, limit);
        return DocIdSet::SortedList(docids);
    }
    
    // 阶梯 2：尝试倒排查（等值条件）
    if let Some(bitmap_match) = try_inverted_index(table, filters) {
        return DocIdSet::Bitmap(bitmap_match.bitmap);
    }
    
    // 阶梯 3：多个倒排条件 → 位图 AND（合并）
    let bitmaps: Vec<RoaringTreemap> = filters
        .iter()
        .filter(|f| is_inverted_field(table, f.field))
        .map(|f| get_inverted_bitmap(table, f))
        .collect();
    
    if !bitmaps.is_empty() {
        let merged = bitmaps.iter().fold(None, |acc, b| {
            acc.map(|a| a.and(b)).or(Some(b.clone()))
        });
        if let Some(bitmap) = merged {
            return DocIdSet::Bitmap(bitmap);
        }
    }
    
    // 阶梯 4：范围条件（有 Zone Map 可剪枝）→ 返回流（惰性全扫）
    if filters.iter().any(|f| f.is_range()) {
        let stream = create_zonemap_filtered_scan(table, filters);
        return DocIdSet::Stream(Box::new(stream));
    }
    
    // 阶梯 5：全表扫描（兜底）
    DocIdSet::Stream(Box::new(full_table_scan(table)))
}
--

修改前（仅支持倒排-倒排）：

rust
fn execute_join(left_query: &QuerySpec, right_query: &QuerySpec) -> Result<Vec<Row>> {
    let left_bitmap = inverted.get(left_query.join_field)?;
    let right_bitmap = inverted.get(right_query.join_field)?;
    let matched = left_bitmap.and(&right_bitmap);
    batch_get(matched)
}
修改后（统一 DocIdSet）：

rust
fn execute_join(
    left_query: &QuerySpec, 
    right_query: &QuerySpec,
    join_condition: &JoinCondition,
) -> Result<Vec<Row>> {
    // 1. 分别获取两表的 docid 集合
    let left_set = get_docid_set(left_query.table, &left_query.filters, None);
    let right_set = get_docid_set(right_query.table, &right_query.filters, None);
    
    // 2. 如果是主键 JOIN，可以直接用 left_set 作为右表主键列表
    if is_primary_key_join(join_condition) {
        // left_set 中的 docid 就是右表的主键，无需额外交集
        let matched_docids = left_set.to_vec(); // 从 DocIdSet 提取所有 docid
        return batch_join_rows(matched_docids, left_query, right_query);
    }
    
    // 3. 普通等值 JOIN：对 JOIN 字段再做一次集合交集
    // 注意：这里的 JOIN 字段不是 docid，而是 user_id 等业务字段
    // 需要从各自的 DocIdSet 中，进一步按 JOIN 字段值过滤
    
    // 方法 A：如果 JOIN 字段有倒排，用倒排查位图
    if is_inverted_field(left_query.table, &join_condition.left_field) {
        let join_bitmap = get_inverted_bitmap(
            left_query.table, 
            &join_condition.left_field, 
            join_condition.right_value
        );
        // 对 left_set 和 join_bitmap 做交集
        let filtered_left = left_set.intersect(&DocIdSet::Bitmap(join_bitmap));
        // ... 类似处理右表
    }
    
    // 方法 B：如果 JOIN 字段无倒排，用广播哈希（小表）
    // ... 降级逻辑
    
    // 4. 最终匹配：left_matched ∩ right_matched
    let final_matched = left_matched.intersect(&right_matched);
    
    // 5. 批量回表取行
    batch_get(final_matched)
}

--

倒排-组合索引 JOIN 不是一个新的独立路径，而是 统一 DocIdSet 抽象的自然产物。

核心改动是：

新增 DocIdSet 枚举：统一表示倒排位图、组合索引列表、全扫流

新增 get_docid_set() 函数：对任意 WHERE 条件，产出一个 DocIdSet

实现 intersect() 方法：让倒排位图、组合索引列表之间可以做无缝交集

JOIN 执行器统一使用 DocIdSet：不再区分“这是倒排”还是“这是组合索引”

这样改动后，所有索引类型（倒排、组合索引、主键）在 JOIN 场景下都是平等的，优化器只需要问一句：“你能产出 docid 集合吗？” 如果能，就放到 DocIdSet 里统一处理。