# design_remain.md —— 设计未落地 / 留待设计点

> 从 design.md / design_extension.md 提取的**未落地或留待**设计点（已落地设计不在此列）。
> 决策下一步时以本文件为准；触发条件满足才实施的项已标注。

## 1. 分布式事务：Calvin 全局事务序（远期蓝图）

- **来源**：design_extension 13.3 / 14.8 / 14.9（v0.6~v0.8）
- **状态**：🔍 远期，**当前不进入 kernel**（Ex-3 评估完成）
- **触发条件（13.3.1，不满足不落地）**：强一致多 docid 跨分区事务需求 + 读写集可静态预声明
- **落地阶段（14.8）**：
  1. 阶段一：全局 gseq 分配器（持久化游标不重号）+ 单写者确定性执行 demo（真实 WriteBatch/快照衔接）
  2. 阶段二：全局复制日志（跨分区广播/拉取）+ 分区确定性执行 + 幂等键 `(gseq, docid)` + 落后追赶
  3. 阶段三：序节点高可用（raft 复制 gseq 游标与日志）+ 故障转移 + 对账
- **API 形态**：14.9 读写集声明（`POST /txn/submit`，单 docid 不走全局序；交互式先读后写不支持）

## 2. 导出增强：增量导出 / JDBC / 流式管道完整版

- **来源**：design 20.5（shanshui-cunji-export）
- **现状**：CSV + Parquet 两列全量已落地；以下**未落地**：
  - **增量导出**：`--incremental --checkpoint`（updated_at 时间戳游标，首次全量记 max、后续只导增量）；
    无时间字段用 `--range 'docid > X AND docid <= Y'`（DocId 游标）；断点续传
  - **JDBC 直连**：`--jdbc` 批量插入（无文件落盘，阶段 3）；ClickHouse `INSERT ... SELECT FROM file('*.parquet')` +
    `--dry-run-schema` 建表 DDL
  - **流式管道**：Filter（条件筛选）→ Projection（字段映射/脱敏）→ Sink Adapter 分叉；当前 export 为简单全量两列
  - **资源控制**：`--rate-limit` 限流、与 Compaction 共享后台 IO 优先级（对在线业务影响 <5%）
  - **MySQL 兼容**：`--mysql-compatible` CSV 转义 + LOAD DATA INFILE 配套 SQL、`--mysql-max-varchar`

## 3. 合并阻塞根治：无锁合并（✅ 完成 af24dbd + P73 3d58137）

- **来源**：problem_solving P72/P73 / development 7.65
- **已落地**：分批缓解（compact_input_max_mb）→ **根治**（2026-09-01）：
  1. CF 增 `sst_mutate: Mutex<()>`（flush/compact 的 ssts store 互斥）✓
  2. CF `switch_and_flush` 改 `&self`（MemTableBuffer 内部 RwLock 冻结）✓
  3. Engine primary/delta/cidx 字段 `Arc<ColumnFamily>` ✓
  4. mysql worker 无锁合并（clone CF Arc → drop 锁 → CompactTargets::run）✓
  - 附带修复 P73：persist_manifest 内存快照（不扫描磁盘，防引用半写段）+ store→persist→remove
    原子；1 亿库实测合并期写不塌陷（25-43k rows/s），469 全绿

## 4. 读写分离（COW 快照读）

- **来源**：design 9.5 / feature G / M8-P1（be09a07）
- **状态**：⏸ 暂缓——组提交已解决"读被写拖垮"；M8-P1 结论：RwLock 剩余收益 <20%，Engine &self 化改动面大
- **再启条件**：复制型分布式阶段（届时读放大场景需要）

## 5. 高并发查询优化（design 9.5 目标）

- **来源**：design 9.5 / development_process_order I 项（P3 ⏳）
- **状态**：M6 后留待（10k 连接类需 M6 异步运行时；H 模块并发查询优化未启动）

## 6. 远期/评估蓝图（触发或规模条件不满足不落地）

| 设计点 | 来源 | 状态 | 条件 |
|---|---|---|---|
| 多副本 raft 高可用（序节点/元数据游标） | design_extension 14.x / 710 | 🔍 远期阶段三 | Calvin 落地阶段三；或元数据切换需求 |
| 存算分离 / Indexer Node（倒排外置） | design 9.10 / 1130 | 🔍 远期蓝图 | 百亿级规模（50 亿属过度设计，不推荐 MVP） |
| 两级索引（Level 1 内存常驻摘要 + Level 2 精确） | design 4.4.2（阶段 2） | ⏳ 待评估 | 当前 4KB 块 + 段级 Block Index 已覆盖；需评估增量收益 |
| Tiered 分层合并（每次只合最小 2 段） | development 7.3 | ⏳ 后续优化 | 写放大再优化需求 |
| io_uring 热路径实测收益 | development 7.63 | ⏳ 待验证 | Linux ≥4GB 环境 A/B（代码已就绪，指引已写） |

## 设计决策边界（已定，不重复评估）

- 2PC / TCC / Seata 本体：不做（L1 outbox + L2 SAGA 已覆盖）
- SAGA vs Calvin：SAGA 是当前与近期的答案；Calvin 远期触发
- 读写分离：暂缓（组提交已解决）
- 无锁合并：分批缓解已落地，根治方案已记录（P72）
