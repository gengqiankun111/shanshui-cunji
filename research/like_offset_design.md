LIKE 和 OFFSET 实现方案
一、LIKE 实现方案
1.1 分类处理策略
LIKE 模式	示例	处理方式	走不走主流程
无通配符	LIKE 'abc'	等同于 =，走倒排/组合索引	❌ 不改，解析阶段转为 Eq
后缀通配	LIKE 'abc%'	前缀匹配，走组合索引（最左前缀）	❌ 不改，组合索引已支持
前缀通配	LIKE '%abc'	全扫 + 行过滤（无法用倒排/B-Tree）	✅ 需改主流程
前后通配	LIKE '%abc%'	全扫 + 行过滤（无法用倒排/B-Tree）	✅ 需改主流程
全文检索	CONTAINS(text, 'keyword')	走倒排全文索引（jieba 分词）	❌ 不改，已有 fulltext 路径
1.2 主流程改动位置
改动点 A：get_docid_set() 增加 LIKE 处理分支

rust
// src/sqlish.rs - get_docid_set() 函数

fn get_docid_set(
    table_id: TableId,
    filters: &[FilterCondition],
    limit: Option<LimitSpec>,
) -> Result<DocIdSet> {
    // ... 现有阶梯 1.1 ~ 1.3 ...

    // ===== 新增阶梯 1.4.5：LIKE 前缀通配 / 前后通配 =====
    if let Some(like_filter) = find_like_pattern(filters) {
        if like_filter.has_leading_wildcard() {
            // '%abc' 或 '%abc%' → 无法用倒排/组合索引
            // 走全扫 + 行过滤（Zone Map 无法对字符串做范围剪枝）
            let stream = full_table_scan_with_filter(
                table_id,
                filters,
                |row| row.get_string(&like_filter.field).contains(&like_filter.pattern),
            );
            // 注意：这里不能用 Zone Map 剪枝（字符串范围剪枝需要字典序，目前没做）
            return Ok(DocIdSet::Stream(Box::new(stream)));
        }
        // 如果是 'abc%'（后缀通配），组合索引已处理，不会走到这里
    }

    // ... 后续阶梯 1.5 ~ 1.7 ...
}
改动点 B：阶段 0 安全检查增强

rust
// src/sqlish.rs - 阶段 0 安全检查

fn safety_check(query: &QuerySpec) -> Result<()> {
    // ... 现有检查 ...

    // LIKE 前缀通配安全检查
    for filter in &query.filters {
        if let Filter::Like(field, pattern, _) = filter {
            if pattern.starts_with('%') || (pattern.starts_with('%') && pattern.ends_with('%')) {
                // 警告：前缀通配会触发全表扫描
                log::warn!(
                    "LIKE '{}...' on field '{}' will trigger full table scan, \
                     consider using inverted full-text index for better performance",
                    pattern, field
                );
                // 但不拒绝（用户可以接受慢查询）
            }
        }
    }

    Ok(())
}
改动点 C：优化器代价标记（用于 P4-C）

rust
// 在 CostEstimate 中标记 LIKE 前缀通配的代价
// 这样 P4-C 可以给这种路径很高的代价，优先选择其他路径（如果有）

impl CostEstimate {
    fn for_like_prefix_wildcard(table_rows: u64) -> Self {
        // 前缀通配 LIKE 无法用 Zone Map 剪枝，必须扫所有行
        // 代价 = 全表行数 × 解行成本
        CostEstimate {
            rows_scanned: table_rows,
            cost: table_rows * 10.0,  // 10倍于普通全扫
            uses_index: false,
        }
    }
}
1.3 LIKE 执行流程（主流程中）
text
LIKE '%abc' 或 '%abc%'
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ 阶段 0：安全检查                                            │
│ - 记录警告日志："LIKE '%abc' will trigger full scan"       │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ 阶段 1：get_docid_set()                                    │
│ - 尝试组合索引 → 不匹配（LIKE 不是最左前缀）               │
│ - 尝试倒排 → 不匹配（倒排只支持等值）                      │
│ - 检测到 LIKE 前缀通配 → 走全扫 + 行过滤                   │
│ - 返回 DocIdSet::Stream                                    │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ 执行阶段：scan_rows(stream, filters)                       │
│ - 全扫所有行，对每行检查：                                 │
│   row.get_string(field).contains(pattern)                  │
│ - 如果 pattern 是 'abc%'（后缀通配），可以用 starts_with  │
│ - 如果 pattern 是 '%abc'（前缀通配），用 contains         │
│ - LIMIT 早停：扫到 limit 行即停止                         │
└─────────────────────────────────────────────────────────────┘
二、OFFSET 实现方案
2.1 主流程改动位置
改动点 A：LimitSpec 结构体增加 offset

rust
// src/sqlish.rs

#[derive(Clone, Debug)]
pub struct LimitSpec {
    pub limit: Option<usize>,
    pub offset: usize,  // 新增
}

impl LimitSpec {
    pub fn total_rows_to_fetch(&self) -> Option<usize> {
        match (self.limit, self.offset) {
            (Some(limit), offset) if limit > 0 => Some(limit + offset),
            _ => None,
        }
    }
    
    // 判断是否可以早停
    pub fn can_early_stop(&self) -> bool {
        self.limit.is_some() && self.limit.unwrap() > 0 && self.offset < 10000
    }
}
改动点 B：get_docid_set() 签名增加 LimitSpec

rust
fn get_docid_set(
    table_id: TableId,
    filters: &[FilterCondition],
    limit: Option<LimitSpec>,  // 从 Option<usize> 改为 Option<LimitSpec>
) -> Result<DocIdSet> {
    // ...
    
    // 传入 limit 到各执行路径
    if let Some(limit_spec) = &limit {
        // 如果 offset 很大（> 10000），记录警告
        if limit_spec.offset > 10000 {
            log::warn!(
                "Large OFFSET {} detected, consider using keyset pagination \
                 (WHERE id > last_id ORDER BY id LIMIT N) for better performance",
                limit_spec.offset
            );
        }
    }
    
    // ...
}
改动点 C：各执行路径支持 offset

rust
// 1. 倒排位图 + offset

fn execute_bitmap_with_offset(
    bitmap: &RoaringBitmap,
    limit: Option<LimitSpec>,
) -> Vec<DocId> {
    let total_to_fetch = limit.as_ref()
        .and_then(|l| l.total_rows_to_fetch())
        .unwrap_or(usize::MAX);
    
    // RoaringBitmap 支持 skip + take
    let iter = bitmap.iter();
    if let Some(limit_spec) = limit {
        // 先跳过 offset 个
        let iter = iter.skip(limit_spec.offset);
        // 再取 limit 个
        if let Some(limit) = limit_spec.limit {
            iter.take(limit).collect()
        } else {
            iter.collect()
        }
    } else {
        iter.collect()
    }
}

// 2. SortedList + offset

fn execute_sorted_list_with_offset(
    list: &[DocId],
    limit: Option<LimitSpec>,
) -> Vec<DocId> {
    match limit {
        Some(limit_spec) if limit_spec.limit.is_some() => {
            let start = limit_spec.offset.min(list.len());
            let end = (start + limit_spec.limit.unwrap()).min(list.len());
            list[start..end].to_vec()
        }
        Some(limit_spec) => {
            // 只有 offset，没有 limit（理论上不可能，SQL 中 OFFSET 必须有 LIMIT）
            list[limit_spec.offset.min(list.len())..].to_vec()
        }
        None => list.to_vec(),
    }
}

// 3. Stream + offset

fn execute_stream_with_offset(
    stream: Box<dyn Iterator<Item = DocId>>,
    limit: Option<LimitSpec>,
) -> Box<dyn Iterator<Item = DocId>> {
    match limit {
        Some(limit_spec) => {
            let iter = stream.skip(limit_spec.offset);
            if let Some(limit) = limit_spec.limit {
                Box::new(iter.take(limit))
            } else {
                Box::new(iter)
            }
        }
        None => stream,
    }
}
改动点 D：阶段 6 ORDER BY + offset 优化（Top-K 特殊处理）

rust
// 在 ORDER BY + LIMIT + OFFSET 时，Top-K 堆需要保留 offset + limit 个元素

fn top_k_sort_with_offset(
    source: Box<dyn Iterator<Item = Row>>,
    order_by: &[OrderSpec],
    limit_spec: LimitSpec,
) -> Result<Vec<Row>> {
    let k = limit_spec.limit.unwrap_or(0);
    let total = k + limit_spec.offset;  // 堆需要保留这么多
    
    if total > 100000 {
        // 如果 offset 太大，Top-K 堆会保留太多元素
        // 建议走全量排序或拒绝
        return Err(Error::QueryTooExpensive(
            format!("OFFSET {} + LIMIT {} = {} too large for Top-K sort, consider smaller OFFSET", 
                limit_spec.offset, k, total)
        ));
    }
    
    let mut heap = BinaryHeap::with_capacity(total);
    for row in source {
        heap.push(OrderedRow(row, order_by));
        if heap.len() > total {
            heap.pop();  // 保留最小的 total 个
        }
    }
    
    // 弹出所有，排序后取 offset..offset+limit
    let mut result: Vec<Row> = heap.into_sorted_vec()
        .into_iter()
        .map(|r| r.0)
        .collect();
    
    // 跳过 offset 个，取 limit 个
    let start = limit_spec.offset.min(result.len());
    let end = (start + k).min(result.len());
    Ok(result[start..end].to_vec())
}
改动点 E：阶段 7 LIMIT 下推感知 offset

rust
// LIMIT 下推时，需要下推的是 limit + offset（因为要跳过 offset 个）

fn push_down_limit(
    plan: &mut ExecutionPlan,
    limit_spec: &LimitSpec,
) {
    // 下推时用 total = limit + offset
    let total_to_fetch = limit_spec.total_rows_to_fetch();
    
    match plan {
        ExecutionPlan::InvertedIndexScan { limit, .. } => {
            *limit = total_to_fetch;  // 取 offset+limit 个 docid
        }
        ExecutionPlan::FullScan { limit, .. } => {
            *limit = total_to_fetch;  // 扫到 offset+limit 行才停
        }
        ExecutionPlan::CompositeIndexScan { limit, .. } => {
            *limit = total_to_fetch;
        }
        // JOIN 流式也要下推 total_to_fetch
        ExecutionPlan::PrimaryKeyJoin { limit, .. } => {
            *limit = total_to_fetch;
        }
        // ...
    }
}
2.2 OFFSET 执行流程（主流程中）
text
SELECT ... ORDER BY id LIMIT 10 OFFSET 1000
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ 阶段 0：安全检查                                            │
│ - OFFSET 1000 < 10000 → 正常                               │
│ - 如果 OFFSET > 10000 → 警告日志                          │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ 阶段 1-5：执行扫描/JOIN/聚合                               │
│ - LIMIT 下推时，下推 total = 1010 行                      │
│ - 倒排取前 1010 个 docid                                   │
│ - 全扫扫到 1010 行即停                                     │
│ - JOIN 流式到 1010 行即停                                  │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ 阶段 6：ORDER BY Top-K 排序                                │
│ - 堆大小 = 10 + 1000 = 1010                               │
│ - 排序后，跳过前 1000 个，取后 10 个                      │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
结果：第 1001-1010 行
三、对主流程的影响评估
改动点	是否改主流程	文件	行数	说明
LimitSpec 增加 offset	✅ 是	src/sqlish.rs	~20	结构体改动，影响所有调用方
get_docid_set 签名改	✅ 是	src/sqlish.rs	~10	参数从 Option<usize> 改为 Option<LimitSpec>
get_docid_set LIKE 分支	✅ 是	src/sqlish.rs	~30	新增前缀通配处理
倒排位图 skip+take	✅ 是	src/inverted.rs	~20	支持 offset 跳过
SortedList 切片	✅ 是	src/sqlish.rs	~15	支持 offset 切片
Stream skip+take	✅ 是	src/sqlish.rs	~10	支持 offset 跳过
Top-K 堆大小调整	✅ 是	src/sqlish.rs	~30	堆大小 = limit + offset
LIMIT 下推 total	✅ 是	src/sqlish.rs	~20	下推 limit+offset
阶段 0 警告	✅ 是	src/sqlish.rs	~15	OFFSET > 10000 警告
总计			~170 行	约 1 天工作量
四、边界情况处理
4.1 LIKE 在组合索引中
sql
-- 如果有组合索引 (status, name)
-- 这种情况 status='active' 走组合索引前缀，name LIKE 'abc%' 用行过滤
SELECT * FROM orders WHERE status='active' AND name LIKE 'abc%'

-- get_docid_set 处理：
-- 1. 组合索引前缀匹配 (status) → SortedList
-- 2. 对 SortedList 中的 docid 做行过滤 name LIKE 'abc%'
-- 3. 只对 1 万行做 LIKE 检查，不用全扫
4.2 LIKE + 倒排
sql
-- status 是倒排字段，name 无索引
SELECT * FROM orders WHERE status='active' AND name LIKE '%abc'

-- get_docid_set 处理：
-- 1. 倒排查 status='active' → Bitmap（50 万行）
-- 2. 对 Bitmap 做行过滤 name LIKE '%abc'
-- 3. 只回表 50 万行（而不是 3000 万行），可接受
4.3 OFFSET 在子查询中
sql
-- 内部查询的 OFFSET 不影响外部
SELECT * FROM (SELECT * FROM orders LIMIT 10 OFFSET 1000) t WHERE status='active'
-- 先执行内部 OFFSET（内部查询的 LIMIT 下推），再外部过滤
4.4 OFFSET + ORDER BY 无索引
sql
SELECT * FROM orders ORDER BY amount LIMIT 10 OFFSET 100000
-- Top-K 堆大小 = 100010，内存 ~1.6MB（100010 × 16 字节）
-- 可接受，但不建议更大
4.5 OFFSET + GROUP BY
sql
SELECT status, COUNT(*) FROM orders GROUP BY status LIMIT 10 OFFSET 1000
-- OFFSET 无法下推到 GROUP BY 之前
-- 必须全部聚合完，再跳 1000 个
-- 这是 SQL 语义限制，不是优化器问题
五、总结
项目	是否改主流程	实现方式	工作量
LIKE 前缀通配	✅ 是	get_docid_set 新增分支，走全扫 + 行过滤	0.5 天
LIKE 后缀通配	❌ 否	组合索引已支持	0 天
LIKE 无通配符	❌ 否	转为 =，走倒排	0 天
OFFSET 支持	✅ 是	LimitSpec 结构体 + 各路径 skip/take	0.5 天
深分页告警	✅ 是	阶段 0 检查，OFFSET > 10000 告警	0 天（含在 OFFSET 中）