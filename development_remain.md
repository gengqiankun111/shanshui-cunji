# development_remain.md —— 开发未完成任务

> 从 development.md / development_extension.md / problem_solving.md 提取的**未完成任务**
> （development_extension.md 中 Ex-5.x/6.x/7.x 的 `[ ]` 为**过时状态**——实际已落地，
> 见 development.md 7.24~7.45 章节，不列入本清单）。
> 当前基线：466 测试全绿（ae41d60）。

## 1. 导出增强剩余（开发进行中项）

- **来源**：feature E 模块 🔄 / design 20.5 / export.rs
- **已完成**：CSV 全量、Parquet 全量（`--parquet`，70c3b30）
- **待做**：
  - **增量导出**：`--incremental --checkpoint`——docid/updated_at 游标断点续传（对称 P3-4 增量导入 5085db8）
  - **JDBC 直连导出**（阶段 3）：批量插入目标库
  - 流式管道 Filter/Projection + `--rate-limit` 资源控制

## 2. 合并阻塞根治：无锁合并（O 项规模，阶段一已落地）

- **来源**：P72 / development 7.60
- **已落地**：syscall 风暴修复（96ac6bc）、分批合并（1763554）、worker 单轮合并阶段一（9e77872）
- **待做（根治）**：CF `sst_mutate` + `switch_and_flush` 改 `&self` + Engine 字段 `Arc<ColumnFamily>` +
  mysql worker 无锁合并路径（方案见 design_remain §3 / P72）

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
