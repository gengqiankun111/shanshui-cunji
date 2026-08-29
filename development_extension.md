# 开发扩展：分布式事务落地计划（development_extension.md）

> 版本：v0.1（2026-08-29）· 关联：design_extension.md（设计决策，分层 L0~L3）
> 状态图例：⏳ 待开发 · 🔄 进行中 · ✅ 已完成
> 说明：本计划对应 design_extension.md 第 6 章决策的 **L1（本地消息表）/ L2（SAGA）/ L3（确定性事务评估）**；
> L0（单分片本地事务 + Group Commit）已由主项目完成，不在此列。
> 开发遵循主项目 6 步工作流：读文档 → demo 研究（src/demo/）→ 整合 kernel → 测试提交 → 记录。

---

## Ex-1 本地消息表 + 幂等消费（最终一致，首选落地）

> 对应 design_extension.md L1。解决：双写扩容衔接、异步索引/物化视图失败补偿、跨节点异步写。
> 思想：业务写 + 待办消息写**同一本地事务**（pending），后台扫描批量投递（幂等键=docid+seq），
> 成功标记 done；失败重试 + 对账。**复用主从复制 ReplicationLog 的 seq 游标 + 幂等 apply 机制**。

### 任务分解

- [ ] **Ex-1.1 消息表存储**：列族 `outbox`（docid+seq 复合键 → 消息载荷 + 状态 pending/done）；
     写入挂接现有 `put_nosync` 事务路径（同一 WAL + memtable 提交，天然本地原子）。
- [ ] **Ex-1.2 投递器**：后台线程扫描 pending（复用 IO 池/看门狗心跳模型）→ 目标投递（RPC/本地）→
     幂等键去重 → 标记 done；指数退避重试 + 上限。
- [ ] **Ex-1.3 消费端幂等**：`apply` 按幂等键（docid+全局 seq）去重（已 applied 集合/布隆），
     防重复投递叠加（复用 Replicator apply 语义）。
- [ ] **Ex-1.4 对账/排空**：原子切换/扩容前校验 outbox 排空（pending=0）；对账任务对比新旧节点
     docid 集合收敛（复用 scan_after 游标遍历）。
- [ ] **Ex-1.5 与双写扩容协议衔接**：M5 扩容"双写→追平→切换"改造成"本地事务写 + outbox 待办 +
     幂等 apply"，切换前排空校验。

### 验证

- demo（src/demo/outbox/）：消息表与业务写原子性（崩溃恢复 outbox 不丢/不重复）、投递重试、
  幂等消费（重复投递不叠加）、排空校验；单元 + 边界测试。
- kernel 集成测试：双写扩容故障注入（投递失败 → 重试收敛）、异步索引刷新失败补偿。

### 依赖

主数据 LSM、ReplicationLog seq/幂等 apply、scan_after 游标遍历、IO 池/看门狗。全部已有 ✅。

---

## Ex-2 SAGA 编排 + 补偿状态机（跨分片业务事务，未来）

> 对应 design_extension.md L2。解决：跨分片业务事务（一笔操作涉及多个 docid 落在不同分片）。
> 思想：docid 级本地事务为步骤，网关/协调器编排（正向前进 + 失败反向补偿），
> 状态机持久化（meta 存储）；屏障（Barrier）防空回滚/悬挂；补偿幂等。

### 任务分解

- [ ] **Ex-2.1 步骤状态机**：`src/saga.rs`——Saga 定义（步骤 + 补偿 + 幂等键），
     状态机（init → executing → succeeded/failed → compensating → compensated），
     持久化到 meta 存储（复用现有 JSON 持久化模式，如 MvScheduler）。
- [ ] **Ex-2.2 协调器**：驻留网关（已有元数据中心）——按序执行步骤（docid 本地事务），
     任一步失败反向补偿；**补偿覆盖超时分支**（宁可多发由屏障空转）。
- [ ] **Ex-2.3 屏障（Barrier）**：空回滚/悬挂防护——分支登记先于执行；补偿幂等键；
     回查接口持久化 transactionId→status（复用 Ex-1 outbox 机制）。
- [ ] **Ex-2.4 执行引擎**：Saga 执行器（同步/异步步骤 + 重试），与现有 RPC（`rpc.rs`）衔接。
- [ ] **Ex-2.5 API**：网关 `/saga/start` `/saga/status` `/saga/compensate`（HTTP，参照 DTM 无侵入协议）。

### 验证

- demo（src/demo/saga/）：正向前进全成功 / 中段失败反向补偿 / 超时分支补偿（屏障空转）/
  协调器崩溃恢复状态机续跑 / 幂等；单元 + 边界测试。
- kernel 集成：2 节点真实 TCP（复用 M5 分片测试框架）跨分片 Saga 端到端。

### 依赖

网关 + 元数据中心 + RPC + meta 持久化（M5 已有）、Ex-1 outbox。

---

## Ex-3 确定性事务评估（Calvin 思想，远期方向）

> 对应 design_extension.md L3。不做 2PC；若未来出现强一致跨节点需求，走"全局事务序执行"。
> 本项为**研究/评估**任务（demo 级验证可行性），不承诺落地。

### 任务分解

- [ ] **Ex-3.1 调研**：Calvin（Yale）/ 确定性并发控制论文要点（事务调度与执行分离、按序无锁执行、
     确定性冲突检测）——输出对比报告。
- [ ] **Ex-3.2 demo 可行性**：src/demo/calvin/——单分片内"全局事务序"批量执行 vs 2PC 模拟对比
     （吞吐/延迟/锁等待）；评估与 docid 确定性路由的契合度。
- [ ] **Ex-3.3 决策**：基于 demo 数据决定是否进入 kernel（状态机/日志复制衔接 ReplicationLog）。

### 验证

demo 对比数据 + 决策报告（记录到 development_extension.md 状态）。

---

## Ex-4 倒排索引字段策略落地（v0.2，2026-08-29）

> 对应 design_extension.md 第 9 章。目的：把字段倒排三准则（枚举建 / 高基数排除 / 长文本分词）
> 落到**现有 50M 正式库（db-50m）**——排除 note 后 inverted 2.2GB → ~200MB，构建时间同步下降，
> 查询不变。机制（M8-P4 白名单/黑名单 + P38 max_term_len + M8-P7 fulltext）均已落地，
> 本里程碑是**配置实践 + 重建验证 + 文档固化**。

### 任务分解

- [ ] **Ex-4.1 重建 50M 倒排**：用 9.4 配置模板（`inverted_fields` 7 枚举 + `exclude_fields=["note"]`
      + `max_term_len=96`）重建 db-50m（保留主数据，重导/重刷倒排）——记录 inverted 大小、
      段构建时间 vs 现状（2.2GB）。
- [ ] **Ex-4.2 验证**：`status=active`=16,666,667、`city=beijing`=10,000,000、`tag_b=x`=25,000,000
      计数不变；点查 docid=0/25M/49999999 命中；倒排体积与导入总耗时对比报告。
- [ ] **Ex-4.3 配置模板固化**：`src/config/model.rs` 注释引用 design_extension 9.4；提供一个
      `config.import-example.toml`（gitignore 除外）或文档示例供实际开发复制。
- [ ] **Ex-4.4 更新文档**：feature.md C 模块状态、problem_solving.md（若遇新问题 P#）、
      design_extension.md 9.5 量化收益回填实测值。

### 验证

重建后 db-50m 倒排体积/构建时间对比 + 计数/点查断言全过（复用现有集成测试断言）。

### 依赖

M8-P4/P38/M8-P7 机制（已落地 ✅）、import 工具（`--inverted-engine hash` + config）。

---

## 里程碑状态跟踪

| 里程碑 | 内容 | 状态 | 提交 |
|---|---|---|---|
| L0 | 写路径单分片本地事务 + Group Commit | ✅ 已有 | `648d9bd` 等 |
| Ex-1 | 本地消息表 + 幂等消费 | ⏳ | — |
| Ex-2 | SAGA 编排 + 补偿状态机 | ⏳ | — |
| Ex-3 | Calvin 确定性事务评估 | ⏳ | — |
| Ex-4 | 倒排索引字段策略落地（db-50m 重建 + 配置模板） | ⏳ | — |

## 与本项目其他文档关系

- **design_extension.md**：本计划的设计依据（v0.1 分布式事务技术全景/分层决策；v0.2 倒排字段策略
  三准则/配置模板）。
- **feature.md**：主项目功能清单（L0 对应 M8-P0 等）；Ex-1/2 落地后同步更新 feature.md 的
  G 模块（分布式/网关）状态；Ex-4 对应 C 模块（倒排全文）状态。
- **development.md 7.x**：Ex-* 里程碑落地后按 7.x 续编（Ex-1 → 7.22 等）。
- **problem_solving.md**：落地过程中问题闭环 P# 记录。
