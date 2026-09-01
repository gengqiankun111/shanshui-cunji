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
- **已落地**：CSV 全量、Parquet 全量（`--parquet`，70c3b30）、增量导出（`--incremental --checkpoint`
  docid 游标断点续传，2174531）、流式管道（`--filter/--project/--mask` Filter+Projection+Sink 分叉，
  c6b5417）、JDBC 直连（`--jdbc mysql://...` MySQL wire 客户端建表+批量 INSERT，c6b5417）、
  `--rate-limit` 限流（c6b5417）、MySQL 兼容 CSV 配套（`--mysql-compatible` 自动生成 CREATE TABLE +
  LOAD DATA INFILE SQL + `--mysql-max-varchar`，313bd81）、建表 DDL（`--dry-run-schema --target
  clickhouse|mysql` MergeTree / InnoDB，313bd81）、与 Compaction 共享后台 IO 优先级
  （`--io-rate-limit-mb` scan_limiter Token Bucket，40e8abb）
- **未落地**：无——**design 20.5 导出功能全部完成**（✅）

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
- **补充（7.72）**：HotCache 内部锁粒度已落地（`9071984`——整包 Mutex → 内部 RwLock + DashMap
  无锁计数，点查热路径读读并行：demo A/B 纯读 x4.16、混合负载 x5.42，482 全绿）；M8-P1 整体
  暂缓结论不变——剩余写路径（txn_commit/compaction）仍串行，组提交已解决主瓶颈
- **再启条件**：复制型分布式阶段（届时读放大场景需要）

## 5. 高并发查询优化（design 9.5 目标）

- **来源**：design 9.5 / development_process_order I 项（P3）
- **状态**：✅ **异步协程运行时完成**（2802885：tokio 网络层 serve_async + spawn_blocking 查询，
  连接 idle 不占 OS 线程——500 idle 连接仅 15 线程，10k 长连接可行；480 全绿）。
  高并发查询优化整体完成（同步模型预处理读锁 + 异步网络层）
- **剩余**：10k 连接 / 85 万 QPS 目标的**吞吐达成**需在目标硬件（16 核/64G/NVMe）基准复测
  （P95 高连接数下验证）——✅ **本机复测完成**（2026-09-02，images/perf-0.8.0/10k连接-85万QPS复测.md）：
  **10k 连接目标达成**（5000 idle 连接仅 19 线程，保持后全可用）；本机（6 物理核/12 逻辑核）
  QPS 上限 ≈ **37.5k**（16 线程峰值，延迟 sub-ms）——85 万 QPS 需 16 核目标硬件（本机物理核
  仅目标的 37.5%）；读路径无回归（QPS 随线程数正常扩展至 12 核上限）

## 6. 远期/评估蓝图（触发或规模条件不满足不落地）

| 设计点 | 来源 | 状态 | 条件 |
|---|---|---|---|
| 多副本 raft 高可用（序节点/元数据游标） | design_extension 14.x / 710 | ✅ 阶段一落地（7.77 `e2a76a0`：元数据 Raft 自动 failover + 脑裂安全）；剩余阶段二（Calvin gseq raft 联动，RPC 接线） | 元数据切换需求已落地；Calvin 阶段三联动依赖 Calvin 落地 |
| Calvin 硬件卸载（gseq 接 DSA/PMem） | design_extension 14.9 | 🔍 评估完成（7.80 `demo gseq-hw`：AtomicU64 原子 seq 1-2 亿/s >> 100 万/s 目标 2+ 数量级）→ **无必要性**（远期跨机房+CPU 瓶颈时复评） | 跨机房强一致需求 + 单机 CPU gseq 瓶颈 |
| 存算分离 / Indexer Node（倒排外置） | design 9.10 / 1130 | 🔍 查询代理层已落地（7.79 `46b2be7`：IndexerProxy 独立倒排 + 回表抽象，500 全绿）；彻底存算分离 | 百亿级规模（50 亿属过度设计，不推荐 MVP；代理层先挂现有节点） |
| 两级索引（Level 1 内存常驻摘要 + Level 2 精确） | design 4.4.2（阶段 2） | ✅ 评估完成（7.80 `demo two-level-index`：全局摘要内存为 Zone Map 7812×、过滤收益为 0、点查已 sub-ms）→ **不引入**（现有 Block Index+布隆+Zone Map 已覆盖） | — |
| Tiered 分层合并（每次只合最小 2 段） | development 7.3 | ✅ 评估完成（7.78 `demo tiered-compaction`：模拟验证写放大不优于 Leveled，当前分批+无锁合并已解决阻塞痛点）→ **暂不引入**（读放大回归风险） | 写放大再优化需求（key 级更新密集场景复评） |
| io_uring 热路径实测收益 | development 7.71 | ✅ 已实测 | 阿里云 Debian 12 / 内核 6.1：池初始化成功、核隔离生效；2 核小机器写 -13%（SQPOLL 占核）、读持平（块缓存主导）→ 默认关；收益在多核/高 IOPS 场景（7.63 指引） |
| **10 亿库扩展（分片横向扩展）** | `design-10b-extension.md`（2026-09-02） | 📋 规划完成——**阶段 A P0 可先行**（全局 docid 分配器：分片前缀 `shard_id<<40\|local_id`，路由 O(1)/分片内自增/扩容归属不变）；阶段 B 倒排分片化验证、C raft RPC 接线、D 分片可观测 | 10 亿规模需求（当前 1 亿库 10×；分片基建 + 扩容编排已就绪 70%+） |

## 设计决策边界（已定，不重复评估）

- 2PC / TCC / Seata 本体：不做（L1 outbox + L2 SAGA 已覆盖）
- SAGA vs Calvin：SAGA 是当前与近期的答案；Calvin 远期触发
- 读写分离：暂缓（组提交已解决）
- 无锁合并：分批缓解已落地，根治方案已记录（P72）
