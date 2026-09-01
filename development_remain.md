# development_remain.md —— 开发未完成任务

> 从 development.md / development_extension.md / problem_solving.md 提取的**未完成任务**
> （development_extension.md 中 Ex-5.x/6.x/7.x 的 `[ ]` 为**过时状态**——实际已落地，
> 见 development.md 7.24~7.45 章节，不列入本清单）。
> 当前基线：466 测试全绿（ae41d60）。

## 1. 导出增强剩余（开发进行中项）

- **来源**：feature E 模块 🔄 / design 20.5 / export.rs
- **已完成**：CSV 全量、Parquet 全量（`--parquet`，70c3b30）、**增量导出**（`--incremental --checkpoint`
  docid 游标断点续传，2174531——首轮全量记 max、后续只导新增、断档自动全量重建）、**流式管道**
  （`--filter/--project/--mask` Filter+Projection+Sink 分叉 + `--rate-limit`，c6b5417）、
  **JDBC 直连**（`--jdbc mysql://...` MySQL wire 客户端建表+批量 INSERT 无文件落盘，c6b5417）、
  **MySQL 兼容 CSV 配套**（`--mysql-compatible` CREATE TABLE + LOAD DATA INFILE SQL +
  `--mysql-max-varchar`，313bd81）、**建表 DDL**（`--dry-run-schema` ClickHouse/MySQL，313bd81）、
  **与 Compaction 共享后台 IO 优先级**（`--io-rate-limit-mb` scan_limiter Token Bucket，40e8abb）
- **待做**：无——**导出增强（design 20.5）全部完成**（✅）

## 2. 合并阻塞根治：无锁合并（✅ 完成 af24dbd + 3d58137）

- **来源**：P72 / P73 / development 7.65
- **已落地**：syscall 风暴修复（96ac6bc）、分批合并（1763554）、worker 单轮（9e77872）、
  **无锁合并根治**（af24dbd：MemTableBuffer RwLock &self 化 + CF sst_mutate + Engine Arc 化 +
  worker clone CF Arc 无锁合并）+ **P73 manifest 竞态修复**（3d58137：persist 内存快照 +
  store→persist→remove 原子）+ 回归测试（5de5ab0）；1 亿库实测合并期写不塌陷；469 全绿

## 3. Ex-1.5 与 M5 双写扩容协议衔接

- **来源**：development_extension Ex-1 剩余 / development 7.43
- **状态**：`[ ]` 未完成——outbox 投递器 + 幂等消费已落地（7348acd），"双写→追平→切换"改造为
  "本地事务写 + outbox 待办 + 排空校验"留待**真实扩容联调**时落地
- **前置**：需两节点扩容场景（真实分布式构建阶段）

## 4. io_uring Linux 部署实测

- **来源**：V 项 / development 7.63 指引
- **状态**：代码已就绪（Linux 门控）；指引已写；**实测待 Linux ≥4GB 环境**（本机 Windows）
- **步骤**：内核 ≥5.1 → `[runtime] io_uring_enabled=true` + SQPOLL 核 → A/B（WAL fsync / 块 read_at）
  吞吐与 P50/P95 → 核隔离验证

## 5. 文档维护（工作流收尾）

- **来源**：development_extension.md
- **待做**：Ex-5.x/6.x/7.x 的 checkbox 回填为 ✅（实际已落地，文档过时）；
  Ex-2.5 状态补 `[x]` + 提交号（13.6/13.7 拓扑并行/对账重试已在 development.md 7.58 记录）

## 6. 远期开发（触发后执行，见 design_remain）

- Calvin 阶段一/二/三（gseq 分配器 → 全局复制日志 → raft 高可用）——触发条件（13.3.1）
- 多副本 raft 兜底——Calvin 阶段三 / 元数据切换需求
- 增量备份已有（M6-5），增量导出待做（见 §1）

## 已完成基线（勿重复）

- 排期大项 A~Y 全完成（development_process_order.md 第 2 章）
- development.md 7.x 至 7.63；problem_solving P1~P72
- SAGA 内核+网关+补偿协议+拓扑并行+对账（Ex-2/2.5/13.5/13.6/13.7）
- 1 亿库读优化（R/M/O）与写路径修复（syscall + 分批 + worker 单轮）
