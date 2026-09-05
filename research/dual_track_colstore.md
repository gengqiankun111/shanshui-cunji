# P94 设计：热列旁路双轨（行式主副本 + hot_fields 热列列存旁路）

- 状态：待评审（2026-09-05）
- 关联：P0-B #29 达线（Task-024 验收 ≤1.5s @110 万）；前置 P91/P92/P93（投影扫描基建与并行实验结论）
- 定位：**列分块双轨，不整表双写**。行式 SST 为主（全列/点查/SELECT * 语义与性能不变），仅
  `storage.hot_fields` 热列以**独立块/独立文件**旁路列存；投影/排序/聚合只解热列块。

## 1. 背景与根因（P93 结论）
- #29 ORDER BY k,amount LIMIT 100 全表扫描：v6 PAX 热列虽在块内列区域化，但各列仍封在**同一块内
  压缩** → 全表排序必须整块解压（体积≈行式），110 万每轮 ~GB 级 IO 主导耗时（串行 13~43s 冷热波动；
  10 万 PAX 224ms ≈ 行式 234ms，PAX 无 IO 收益）。
- 并行分片 top-K（P93）：语义正确（746 绿 + 200k 单测），110 万实测块缓存 put 速率超淘汰 → RSS 超
  预算（5.35G→9.7G）+ 核利用低（2/12，SST 仅 2 文件）→ 默认关闭，留 P93_PARALLEL=1 实验开关。
- 结论：**要根治需让排序键列独立成块/独立压缩**——扫描只读 k/amount 两列 ~30-50MB（当前 ~500MB），
  预估 <0.5s（36~54× 提升），可稳达 ≤1.5s 验收线。

## 2. 目标 / 非目标
目标：
- 阶段①：热列（先 k/amount）独立块写入与读取；topk 稠密/投影扫描路由列存 → 110 万 #29 ≤1.5s。
- 阶段②：双轨路由 + EXPLAIN 标注（TableScan: Columnar / RowStore）+ 一键回退开关。
- 非目标（记录不开发）：全 25 列列存化/双写副本；客户侧列族建表语法；列存上的写（update-in-columnar）。

## 3. 现状资产盘点（复用而非重写）
- SST v6 PAX：块内列区域 + 按列 zone map；`decode_pax_block_fields`（sstable/block.rs）已能只解请求列。
- 投影扫描链路已接线：engine.scan_stream_fields → CF.scan_stream_at(project) → SstRangeIter::set_project_fields
  → decode_projected_block（PAX 列解码 / 行式直通）；P106 点查 projection pushdown；P86② 字节级字段提取。
- 列族架构：primary / cidx / delta / outbox（多 CF 并行 WAL 已成熟）；列族内 SST 独立文件、块级
  key 区间剪枝（sst_intersects_window）与块缓存按表分区（P3-C）。
- hot_fields 写入链：storage/column_family/open.rs → pax_hot_fields → flush.rs 写 PAX 块（阶段 1.5）。

## 4. 总体架构（双轨）
```
写路径（不放大主写）:
  SQL put → primary memtable/WAL（不变，source of truth）
  flush primary 不可变段时，同一批次**衍生**生成 colstore 段（仅 hot_fields 列，重排为按列分块）
  → 无热路径双写、无两副本原子性（colstore 是派生物，可由行式任意重建=在线迁移/降级红利）
读路径（自动路由）:
  点查 / SELECT * / 全列扫描 / 未命中热列  → 行式主（原路径，语义不变，无雪崩）
  排序键 ⊆ hot_fields 且扫描窗口大（topk 稠密 / 聚合投影） → colstore（逐列独立块流式，只解目标列）
  最新视图合并：colstore 只服务已 flush 段水位（< docid watermark）；memtable/热更新/删除行由行式主
  覆盖合并（稀疏回填，同 P92 dense 语义）——阶段① 先限定 fresh-load + flush 后静态数据场景验证
一致性/恢复: 删除位图仍为权威（行式主）；colstore 为只读派生，损坏可丢弃重建（meta 校验和）
回退开关: colstore_enabled=false → 全部走行式（一键秒回）；灰度：按表/按列开放
```

## 5. 布局选型
- 形态 A（本设计采用）：**colstore = 独立列族（CF "colstore"）**，其 SST 块按“列”分组：
  每列一组块（组内块按 docid 区间有序，值=该列字节，与 PAX 列区同编码）；读某列只取该列组块，
  免整块解压。块级 zone map 按列存（复用 P1-E）。
  - 为什么不是“行式值里再做整块解压”：已证（P93）无用；列独立成块才把 IO 从 ~500MB 降到 ~30MB。
- 形态 B（候选，阶段③ 再评）：全列列存 / 客户列族——记录不开发（见 §2）。

## 6. 写路径细化（阶段①）
1. `put/delete` 不改（仍写 primary）。
2. flush primary 段 S 时：构造 colstore 输入 = S 全部行（docid 区间 × 文档），按 hot_fields 列
   重排成列组块 → 写 colstore CF 段 C（独立 flush 任务，后台异步，与 compaction 解耦）。
3. 删除/覆盖：新版本在行式主；colstore 按 docid 水位提供旧值——查询合并以行式主（最新）为准
   （同一 docid 出现则行式覆盖 colstore 值）。阶段① 静态场景无覆盖 → 无歧义。
4. 写放大核算：主写零放大；flush 阶段每行多一次热列字节复制（热列占全行 ~10-30% → 全量写放大
   ≈ +10~30% 的 flush 阶段成本，摊销到 group/后台线程，守护 wide-load ≤1.3×）。

## 7. 读路径细化（阶段①）
- topk_sort（dense 分支）命中条件：无 WHERE（或可倒排小窗不生效）/ order_by 列 ∪ 回表需要 ⊆
  hot_fields → 改走 `engine.scan_colstore_cols(range, cols, cb(docid, vals))`（新原语，逐列块流式）。
- 命中后胜出 docid 整行回表仍走行式 batch_get（复用 P87③）→ SELECT * 兼容。
- 未命中（列非热 / 窗口过小 / 行式更优）→ 原行式路径（回归保护=与现状逐字节一致）。

## 8. 代价测算（对比用户方案 A/B）
| 维度 | 全列双写（方案A） | 列族分组（方案B） | **热列旁路双轨（本设计）** |
|---|---|---|---|
| 存储 | ~2× | ~1.1× | +10~30%（仅热列） |
| 写放大 | ~2×（伤写卖点） | ~1.1× | ≈0 热路径 + flush 派生 +10~30% |
| 全列/点查 | 行式副本（同现状） | 跨 2 族 | 行式主（同现状，无雪崩） |
| 客户配置 | 零 | 高（列族认知负担） | 零（hot_fields 引擎级） |
| 一致性复杂度 | 高（双写原子） | 中 | 低（派生可重建） |
| 在线迁移/降级 | 有 | 需工具 | 有（删 colstore 即回行式） |
- 结论：收益与方案 A 对齐（#29 目标 <0.5s），写侧/一致性代价远小于 A，客户体验等于 A（透明路由）。

## 9. 里程碑与验收
- M1 设计评审通过（本文档）。
- M2 demo（src/demo/p94/）：colstore 列组块写读 + 只解目标列，单测+边界（缺列/删除/非热列回退）。
- M3 kernel 整合：colstore CF + flush 衍生 + scan_colstore_cols 原语 + topk/EXPLAIN 路由 + 回退开关。
- M4 回归 + 基准回填：110 万 #29 ≤1.5s；写侧守护 wide-load ≤1.3× / YCSB 容忍带；全列/点查/全扫
  无回归（746 基线）；EXPLAIN 标注；回退开关一键行式。

## 10. 风险与开放问题
- 读取合并水位（memtable 未 flush 行 vs colstore 段）语义：阶段① 静态场景规避；阶段② 明确覆盖规则。
- colstore 与删除位图：删除行仍以行式主为准（colstore 只读旧段），查询须经删除位图过滤（复用现有）。
- 新原语对现有投影链路不回归：colstore 仅在显式路由启用，默认行式。
- compaction 维度：colstore 独立层策略（含热列重复覆盖清理）阶段② 评估，避免写放大回归 P4-A 成果。

## 附：相关文件
- src/storage/sstable/block.rs（PAX 列区 / decode_pax_block_fields）
- src/storage/column_family/{open,flush,scan}.rs（hot_fields / flush 派生点 / scan_stream_at）
- src/engine/{scan.rs,query.rs}（scan_stream_fields 原语）
- src/sql/executor/select.rs（topk_sort 稠密路由点，P93 堆重构已就绪）
- src/sql/executor/eval.rs（light 字段提取） / src/explain.rs（路由标注）
