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

- [x] **Ex-1.1 消息表存储**：列族 `outbox`（docid+seq 复合键 → 消息载荷 + 状态 pending/done）；
     写入挂接现有 `put_nosync` 事务路径（同一 WAL + memtable 提交，天然本地原子）。
- [x] **Ex-1.2 投递器**：`Outbox::dispatch` 扫描 pending → 投递回调（FnMut）→ 幂等键去重 →
     标记 done；`pending_count`/`drained` 排空校验；engine 层 `dispatch_outbox` 投递后
     `flush_wal` 落盘防重投。
- [x] **Ex-1.3 消费端幂等**：`IdempotentConsumer` 按幂等键（docid+全局 seq）去重（applied 集合），
     防重复投递叠加（复用 Replicator apply 语义）。
- [x] **Ex-1.4 对账/排空**：`outbox_drained`（pending=0）供原子切换/扩容前排空校验；
     `scan_after` 游标遍历对账复用已有。
- [x] **Ex-1.5 与双写扩容协议衔接**：M5 扩容"双写→追平→切换"改造完成——`src/scale_out.rs`
     （7.76 `8f8c1ea`）本地事务写 + outbox 待办 + 排空校验（`outbox_drained`，未排空禁切）+
     回滚预案 + 崩溃续跑（ADDING→CATCH_UP→DRAIN→SWITCH→DONE 状态机）。

### 验证

- demo（src/demo/outbox/，4 测试）：消息表与业务写原子性（崩溃恢复 outbox 不丢/不重复）、投递重试、
  幂等消费（重复投递不叠加）、排空校验；单元 + 边界测试。
- kernel 集成（engine 3 测试）：e2e enqueue→dispatch→drain、重启存活（pending 落盘）、默认关闭零开销。
- ✅ 完成：`cargo test` **356 全绿**（含 outbox 7）；提交 `7348acd`。

### 依赖

主数据 LSM、ReplicationLog seq/幂等 apply、scan_after 游标遍历、IO 池/看门狗。全部已有 ✅。

---

## Ex-2 SAGA 编排 + 补偿状态机（跨分片业务事务，未来）

> 对应 design_extension.md L2。解决：跨分片业务事务（一笔操作涉及多个 docid 落在不同分片）。
> 思想：docid 级本地事务为步骤，网关/协调器编排（正向前进 + 失败反向补偿），
> 状态机持久化（meta 存储）；屏障（Barrier）防空回滚/悬挂；补偿幂等。

### 任务分解

- [x] **Ex-2.1 步骤状态机**：`src/saga.rs`——`SagaStep` trait + `ClosureStep`（正向/补偿闭包），
     状态机（init → executing → succeeded/failed → compensating → compensated），
     持久化 JSON tmp+rename（saga-{tx_id}.json，复用 MvScheduler 原子写模式）。
- [x] **Ex-2.2 协调器**：`SagaCoordinator`——按序执行步骤（docid 本地事务），
     任一步失败反向补偿；补偿失败保持 Compensating 续跑重试（**补偿覆盖超时分支**由屏障空转）。
- [x] **Ex-2.3 屏障（Barrier）**：空回滚/悬挂防护——分支登记（正向成功后记录）先于执行；
     补偿幂等键（tx_id+step）；回查接口 `status`/`all_states` 持久化 transactionId→status。
- [x] **Ex-2.4 执行引擎**：`run`（启动/续跑，Failed/Compensating 自动转补偿）+
     `compensate`（逆序补偿），与现有 RPC 衔接由业务方实现 `SagaStep`。
- [x] **Ex-2.5 网关 HTTP API**：`src/server.rs` `/saga/start`（POST `{tx_id, steps[]}`，
     执行业务步骤并自动逆序补偿）/ `/saga/status`（GET transactionId→status 回查，屏障依据）/
     `/saga/compensate`（POST 强制补偿重试）；`src/saga.rs` 增 `HttpStep`（HTTP 业务步骤）+
     `http_post`（极简 HTTP/1.1 POST 客户端，非 2xx/超时 → 步骤失败）；
     协调器目录 = `{data_dir}/saga`（崩溃恢复续跑）——提交 `781199e`

### 验证

- demo（src/demo/saga/，6 测试）：正向前进全成功 / 中段失败反向补偿 / 超时分支屏障空转 /
  终态拒迟到正向（悬挂防护）/ 崩溃恢复状态机续跑 / 补偿失败重试 + 幂等。
- kernel 集成（src/saga.rs 6 测试）：正向成功无补偿 / 中段失败逆序补偿 / 重启恢复终态 /
  终态拒绝重复正向 / 补偿重试 3 次成功 / 重复登记被拒。
- Ex-2.5 网关 e2e（src/server.rs 3 测试，模拟业务节点）：正向全成功无补偿 + 状态文件持久化 /
  中段失败逆序补偿 + 终态幂等（重发不重复执行/补偿）/ 网关重启后状态恢复。
- 13.5 补偿协议形式化落地（src/saga.rs +5 测试，14 全绿）：Executing/Failed/Compensating 中间态
  崩溃恢复续跑（不重复已登记/已补偿分支）/ 缺步骤定义保持 Compensating（修复：不得静默终态）/
  超时未登记分支屏障空转不补偿（宁可漏补偿不可错补偿）+ 已登记分支逆序补偿。
- ✅ 完成：`cargo test` **445 全绿**（saga 14 + 网关 3）；提交 `781199e`（Ex-2.5）+ `170bf21`（13.5 补偿协议）。
  跨分片 2 节点真实联调留待分布式构建阶段（设计见 design_extension 13.2）。

### 依赖

网关 + 元数据中心 + RPC + meta 持久化（M5 已有）、Ex-1 outbox。

---

## Ex-3 确定性事务评估（Calvin 思想，远期方向）

> 对应 design_extension.md L3。不做 2PC；若未来出现强一致跨节点需求，走"全局事务序执行"。
> 本项为**研究/评估**任务（demo 级验证可行性），不承诺落地。

### 任务分解

- [x] **Ex-3.1 调研**：Calvin（Yale, Thomson & Abadi SIGMOD'12 / IEEE'13）要点——三阶段
     流水线（logging→scheduling→execution）、确定性锁（读/写集**预声明**、按确定序一次
     申请全部锁、无跨网络锁等待）、请求先落持久化复制日志、执行等价于按日志序串行（副本
     无分歧 → 无提交协议）；代价：读写集须执行前声明（依赖读需侦察）、排除交互式多语句会话。
- [x] **Ex-3.2 demo 可行性**：src/demo/calvin/（4 测试，tick 模拟时钟）——高冲突
     （键域 40/N=400）2PC 锁等待显著、Calvin 确定性序**零等待**；低冲突（键域 10 万/N=2000）
     2PC 每事务下限 = 2×RTT（固定往返）vs Calvin 无协调往返；跨分区占比 10%/50%/90%：
     2PC 105k/125k/145k ticks vs Calvin 恒定 11k（**吞吐与跨分区比例无关**）；确定性序执行
     副本间状态一致（无分歧）。
- [x] **Ex-3.3 决策**：🔍 **评估完成——不进入 kernel（远期方向保留）**。理由：
     ① 本项目写路径 = docid 一致性哈希确定性路由 → **单 docid 事务天然不分片**，跨分片
     场景极少（多 docid 批量事务）；② 现有 L1 outbox + L2 SAGA 已覆盖异步/最终一致需求；
     ③ Calvin 需全局事务序协调器（单点）+ 读写集预声明（倒排词表难静态预声明）——投入产出
     不匹配。**触发条件**：出现强一致多 docid 跨分区事务需求时，再按"全局事务序 + 状态机
     衔接 ReplicationLog"落地（完整协议设计见 design_extension **第 14 章**：全局 gseq 分配 /
     确定性锁 / 幂等键 `(gseq,docid)` / 故障恢复 / SAGA vs Calvin 判据，远期蓝图 v0.7）。

### 验证

demo 对比数据 + 决策报告（记录到 development_extension.md 状态）。✅ 完成。
- demo（src/demo/calvin/，4 测试）全绿；数据见 Ex-3.2 摘要。

---

## Ex-4 倒排索引字段策略落地（v0.2，2026-08-29）

> 对应 design_extension.md 第 9 章。目的：把字段倒排三准则（枚举建 / 高基数排除 / 长文本分词）
> 落到**现有 50M 正式库（db-50m）**——排除 note 后 inverted 2.2GB → ~200MB，构建时间同步下降，
> 查询不变。机制（M8-P4 白名单/黑名单 + P38 max_term_len + M8-P7 fulltext）均已落地，
> 本里程碑是**配置实践 + 重建验证 + 文档固化**。

### 任务分解

- [x] **Ex-4.1 重建 50M 倒排**：用 9.4 配置模板（`inverted_fields` 7 枚举 + `exclude_fields=["note"]`
      + `max_term_len=96`）重导到 **`D:\shanshui-data\db-50m-opt`**（新建，不动既有库）——
      `shanshui-cunji-import --parquet ds-50m.parquet --config config-ex4.toml`：
      50,000,000 行成功 / 0 失败，**838,208 ms**（59.7k rows/s，含倒排 FST 构建）；
      **inverted 2231.8MB → 144.3MB（-93.5%，16.9×）**，优于 ~200MB 预估。
- [x] **Ex-4.2 验证**（CLI count + HTTP /get）：
      `status=active`=**16,666,667** ✓、`city=beijing`=**10,000,000** ✓、`tag_b=x`=**25,000,000** ✓
      计数与优化前一致；点查 docid=0/25,000,000/49,999,999 全部命中（含边界 0 与末条）；
      注：db-50m-clean 实为 4M 条中间产物（status=active=1,333,334），非 50M 基线，基线取原始
      db-50m（primary 3.7GB + inverted 2231.8MB）。
- [x] **Ex-4.3 配置模板固化**：仓库新增 `config.import-example.toml`（9.4 模板 + 实测收益注释）；
      `src/config/model.rs` `InvertedConfig::inverted_fields` 注释引用 design_extension 9.4。
- [x] **Ex-4.4 更新文档**：design_extension.md 9.5 回填实测（2231.8MB → 144.3MB）；feature.md
      C 模块状态；development.md 7.46。

### 验证

重建后 db-50m 倒排体积/构建时间对比 + 计数/点查断言全过（复用现有集成测试断言）。✅ 完成。
- inverted **2231.8 → 144.3MB**（-93.5%）；导入 838s 含倒排段构建（逐字段倒排追加 0.1s/百万级）；计数/点查全命中。

### 依赖

M8-P4/P38/M8-P7 机制（已落地 ✅）、import 工具（`--inverted-engine hash` + config）。

---

## Ex-5 SSD 原生迁移计划（v0.3，2026-08-29）

> 定位（design 1.2/1.3/4.8）：**放弃机械硬盘（HDD）兼容，只支持 NVMe/SATA SSD**。
> 目标场景「写入快 + 20 倒排字段」：写入 TPS 40 万+、写 P95 0.45ms、倒排更新 0.1ms、写放大 5×。
> ⚠️ **开发环境用机械硬盘性能大幅下降（10 倍级），压测无参考价值——不要压测**，验证须在 SSD 上。

### P0 任务（低风险快速收益，阶段一）

- [x] **Ex-5.1 SSTable 4KB 块 + 两级索引**：`block_size_kb` 默认 16→4（配置化）；
      Block Index 两级（内存摘要 + 磁盘精索，防索引内存 +4 倍）；回表读放大 16×→4×。~350 行
      ——提交 `056b21d`（development 7.24）
- [x] **Ex-5.2 倒排分片锁**：倒排 Term 字典按 Hash 分 256 锁分区（同 Term 串行、不同 Term 并行），
      低基数字段高并发锁竞争 P99 -40%。~150 行——提交 `c7ebe72`（development 7.25）
- [x] **Ex-5.3 倒排更新批处理**：写入线程攒批——同 Term 多 DocId 内存聚合批量追加倒排文件，
      与 Group Commit 窗口联动；倒排更新 CPU -60%。~500 行——提交 `d38e8ab`（development 7.26）
- [x] **Ex-5.4 Compaction 参数调优**：层级比例 10×→20×、L0 触发放宽、并行度 1→2~4（SSD 并发 IO）；
      空间放大 1.2×→1.8× 换写入放大 15~25×→6~10×——提交 `624ce9e`（development 7.31）

### P1 任务（核心改造，阶段二）

- [x] **Ex-5.5 环形大文件 WAL 规模化**：在 v0.6 RingWal（预分配单文件 + 环形指针）基础上扩展
      大容量预分配 + 磨损均衡 + 崩溃恢复回归（混沌测试）；WAL P99 -60%——提交 `4974ef3`（development 7.32）
- [x] **Ex-5.6 删除位图（Deletion Bitmap）**：独立 LSM 的按 DocId 1bit 位图（4KB 页对齐 + mmap），
      删除写 1bit + fsync 1 页（-99% IO）、查询 O(1) 跳过；compaction 物理删除后清标记——提交 `e615071`
- [x] **Ex-5.7 倒排 FST + Mmap 字典（design 5.2.4.1）**：内存字典 ~8GB → 分层 FST + mmap；
      冷启动 8s→50ms、写入查表 -30%——提交 `442981c`（development 7.35）
- [x] **Ex-5.8 元数据-数据解耦**：SST 的 Block Index/Bloom/Zone Map 独立元数据区，Compaction
      只重写元数据（倒排 TermMeta 同理）；写放大 -50%——提交 `cd00d85`（数据块级复用）

### P2 任务（进阶优化，阶段三）

- [x] **Ex-5.9 冷热感知 Compaction + Bloom Merge**：按 SST 热度排序 + 合并前布隆判断；写入量 -30%——提交 `ba709e2`
- [x] **Ex-5.10 多 SSD 条带化**：WAL 独占最快 SSD、SSTable 分布多盘、倒排独立放置；多盘 +3~4×——提交 `e6a5610`

### 验证

每项 P0 完成：单元/边界测试 + demo 研究（src/demo/ 下，gitignore 不提交）跑通后整合；
性能收益须在 **SSD 环境**实测记录（HDD 环境不压测、数据不采信）。

### 依赖

v0.6 存储内核（环形 WAL/倒排/compaction 已有基础）、design 4.8 设计依据。

---

## Ex-6 并发读优化（Seqlock/Arc，v0.4，2026-08-29）

> 设计依据：design_extension v0.4 第 11 章（方案全景对比 + 各模块最优组合 + Seqlock 倒排设计）。
> 目标：写 22 万 TPS + 读 85 万 QPS 下读路径持续响应——倒排段清单/FST 字典用 Seqlock/Arc
> 实现无锁读（数据小、写多读少）。
> ⚠️ **依赖**：需先打破 Engine 全局锁（读写分离/双写加速，feature.md I 模块 ⏸）才有真实
> 读写并发；全局锁下实现 Seqlock 无收益。

### 任务分解

- [x] **Ex-6.1 Seqlock 原语** ✅ `1946161`：`src/seqlock.rs`（零 unsafe：AtomicU64 版本号奇偶 +
      RwLock 写短锁 + 读 try_read 立即重试不阻塞写）；`retries()` 重试监控；单元测试 4
      （并发不撕裂/可见性/低频写重试率 0.015%/版本奇偶）——测试 318 全绿（+4）。
- [x] **Ex-6.2 倒排段清单 Seqlock/Arc**：`segments: Vec<String>` → 版本化快照或
      `Arc<Vec<String>>` + ArcSwap（原子指针发布）——`flush_segment`/`gc` 更新发布，
      `search`/`doc_count`/`iter_terms` 读快照无锁——提交 `c8183cf`（Ex-6.2 ArcSwap 段清单）
- [x] **Ex-6.3 倒排 FST 字典 Arc**：`dicts: HashMap<String, fst::Map>` → 值改 `Arc<fst::Map>` +
      ArcSwap 指针发布——查询 `dicts.get(seg)` 拿 Arc 快照，读路径零拷贝——提交 `c8183cf`（Ex-6.3 字典快照化）
- [x] **Ex-6.4 验证**：并发读-写正确性（读不阻塞写、写不阻塞读）、重试率统计（预期 <0.1%）、
      与全局锁基线对比（SSD 环境实测，HDD 不压测）——`c8183cf` 回归 + demo 验证（重试率 0.015%）。

### 验证

Seqlock 单元测试（并发写读交错）+ 倒排端到端（flush/gc 并发 search 结果一致）+ 重试率监控。

### 依赖

设计已定（design_extension v0.4）；代码落地依赖**读写分离**（feature.md I 模块）解除 Engine
全局锁；原语（Ex-6.1）可独立先行（不与全局锁冲突）。

---

## Ex-7 多核优化（v0.5，2026-08-29）

> 设计依据：design_extension v0.5 第 12 章（五处理点：锁竞争已落地 / 缓存伪共享新增 /
> 绑核 / io_uring 多队列 / compaction 动态限流）。核心：**Shard Everything**——
> 按核分计数器、按 Term 分锁、按物理核分调度池，把单核极限吞吐提升为多核稳定并发。

### 任务分解

- [x] **Ex-7.1 缓存伪共享**（P0）：`PerCpuCounter`（按核拆分计数器，读取汇总，align(64) 隔离
      缓存行）——`total_writes` / 倒排 `mem_docids` 改 PerCpuCounter；热统计结构体
      `#[repr(align(64))]`；demo 验证多核写计数器吞吐提升（4/8 核对比）——提交 `c5fa66c`（demo 2.1×）
- [x] **Ex-7.2 绑核默认开启**（P1）：`[affinity]` 配置默认开启（多核机器），三池绑物理核
      （网络 0-3 / 计算 4-7 / IO 尾核，跳过超线程虚拟核）；`core_affinity` crate + taskset
      兜底；验证绑核 vs 不绑核 P99（SSD 环境，HDD 不压测）——提交 `b294532`
- [x] **Ex-7.3 io_uring SQPOLL + 多队列**（P1）：阶段 3 io_uring 落地时，WAL/SSTable 落不同
      NVMe 队列/SSD（同 Ex-5.10 多盘条带化），WAL fsync 与刷盘并行——提交 `fd0b519`
      （io_uring 队列抽象，7.71 热路径接入 + Debian 12 实测）
- [x] **Ex-7.4 Compaction 动态限流**（P2）：并行度限后台 IO 池（2~4），按前台写负载动态下调
      `rate_limit_mb/s`（Ex-5.4 compaction 调优后叠加）——提交 `ddbc20e`

### 验证

Ex-7.1 demo（PerCpuCounter vs 单 AtomicU64 多核写吞吐）+ 热路径统计正确性；
Ex-7.2 P99 对比（SSD 环境实测）。

### 依赖

Ex-5.2（分片锁）/ Ex-6.1（Seqlock）已落地锁竞争；design_extension v0.5 设计依据。

---

## 里程碑状态跟踪

| 里程碑 | 内容 | 状态 | 提交 |
|---|---|---|---|
| L0 | 写路径单分片本地事务 + Group Commit | ✅ 已有 | `648d9bd` 等 |
| Ex-1 | 本地消息表 + 幂等消费 | ✅ | `7348acd` |
| Ex-2 | SAGA 编排 + 补偿状态机 + 网关 HTTP API | ✅ | `990bf6b`（Ex-2.5 网关 `/saga/*` 端点 + HttpStep） |
| Ex-3 | Calvin 确定性事务评估 | ✅ 评估完成（🔍 不进入 kernel，远期方向保留） | demo `src/demo/calvin` |
| Ex-4 | 倒排索引字段策略落地（db-50m 重建 + 配置模板） | ✅ | `db-50m-opt`（inverted 2231.8→144.3MB） |
| Ex-5 | SSD 原生迁移（P0 ✅ + P1 环形 WAL/删除位图/FST/解耦 ✅ + P2 冷热/条带化 ✅） | ✅ | `e6a5610` 等 |
| Ex-6 | 并发读优化（Ex-6.1 Seqlock 原语 ✅ → 6.2 段清单 ArcSwap ✅ / 6.3 FST 字典 Arc ✅，依赖读写分离解除全局锁） | 🔄 | `c8183cf` |
| Ex-7 | 多核优化（Ex-7.1 PerCpuCounter ✅ / 7.2 绑核 ✅ / 7.3 io_uring 队列抽象 ✅ / 7.4 动态限流 ✅） | ✅ | `ddbc20e` 等 |

## 与本项目其他文档关系

- **design_extension.md**：本计划的设计依据（v0.1 分布式事务技术全景/分层决策；v0.2 倒排字段策略
  三准则/配置模板；v0.3 SSD 原生优化定位与优先级；v0.4 并发读优化 Seqlock/Arc；v0.5 多核优化
  PerCpuCounter/绑核/io_uring 多队列/compaction 动态限流）。
- **design.md**：SSD 原生存储优化设计见 4.8（核心设计转变/场景瓶颈/优化优先级/配置模板/路线图）；
  HDD 开发环境警告见 1.2。
- **feature.md**：主项目功能清单（L0 对应 M8-P0 等）；Ex-1/2 落地后同步更新 feature.md 的
  G 模块（分布式/网关）状态；Ex-4 对应 C 模块（倒排全文）状态；Ex-5 对应 J 模块（SSD 原生优化）。
- **development.md 7.x**：Ex-* 里程碑落地后按 7.x 续编（Ex-1 → 7.22 等）。
- **problem_solving.md**：落地过程中问题闭环 P# 记录。
