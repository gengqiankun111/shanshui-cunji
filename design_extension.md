# 设计扩展（design_extension.md）

> 版本：v0.5（2026-08-29）· 多主题扩展文档：
> **v0.1 分布式事务**（第 1~8 章）· **v0.2 倒排索引字段策略**（第 9 章）·
> **v0.3 SSD 原生优化**（第 10 章）· **v0.4 并发读优化**（第 11 章）·
> **v0.5 多核优化**（第 12 章，2026-08-29 增补）
> 关联：design.md（主设计）、development_extension.md（开发扩展计划，Ex-1/2/3 分布式 + Ex-4 倒排策略 + Ex-5 SSD 原生 + Ex-6 并发读优化 + Ex-7 多核优化）

---

## 1. 问题界定：山水存迹需要分布式事务吗

- **写路径现状**：docid → 一致性哈希 → **单分片**确定性路由（`sharding::route`），写事务
  落在单节点 → 主数据 LSM + 倒排 + HotCache 失效在**同一进程内同步提交**（本地事务语义已满足）。
  单分片写**不产生跨节点事务需求**（这是路由设计带来的天然优势）。
- **真正出现跨节点一致性的场景**：
  1. **双写扩容**（M5 无损扩容协议）：迁移分片同时写老/新节点 → 需要两节点写入原子性/最终一致；
  2. **跨分片业务事务**（未来：一笔操作涉及两个 docid 落在不同分片——如聚合/转账类业务）；
  3. **广播索引/物化视图刷新**（主数据已提交、索引异步构建的失败补偿）。
- 结论：**核心写路径不需要 2PC**；需要的是**跨节点的最终一致机制**（双写扩容衔接 + 未来跨分片业务）。

---

## 2. 分布式事务技术全景（六大方案）

| 方案 | 核心思想 | 一致性 | 性能 | 业务侵入 | 典型坑 | 代表实现 |
|---|---|---|---|---|---|---|
| **2PC / XA** | 准备阶段（PREPARE 持锁）+ 提交阶段 | 强一致 | 差（同步阻塞、跨网络往返持锁） | 低（DB 原生） | 协调者单点、prepared 事务永久持锁、二阶段网络割裂不一致 | MySQL XA、PG PREPARE TRANSACTION、Atomikos/Narayana（JTA） |
| **3PC** | CanCommit → PreCommit → DoCommit + 超时 | 强一致（改进） | 差 | 低 | 理论多、实际无主流 | 无主流框架 |
| **TCC** | Try（预留）/ Confirm / Cancel 三方法 | 最终一致（近强） | 高（无长锁，Try 后即提交） | **高**（每分支三方法） | 空回滚、悬挂、幂等（须先登记分支再 Try） | Seata-TCC、DTM-TCC |
| **SAGA** | 长事务拆 N 本地事务 + 反向补偿 | 最终一致 | 高（无全局锁） | 中（每步+补偿） | **无隔离性（脏读）**、补偿须覆盖超时分支 | Seata-SAGA（状态机）、DTM-SAGA、Camunda |
| **本地消息表** | 业务+消息同本地事务，异步扫描投递 | 最终一致 | 高 | 中 | 消息表与业务耦合、投递延迟 | 自研（业界最常用最稳） |
| **事务消息** | 半消息 → 本地事务 → commit/rollback | 最终一致 | 高 | 低 | 依赖 MQ | RocketMQ 事务消息 |
| **最大努力通知** | 重试 + 对账 | 最终一致（最弱） | 最高 | 低 | 一致性保障最弱 | 通知类（短信/邮件） |

**关键洞察（补偿的本质）**：补偿 = 语义相反的**新操作**，而非还原原样——这决定方案需不需要全局锁：
- XA/AT：持全局锁直到事务结束 → 强隔离但并发受限；
- TCC/SAGA：用业务字段表达中间态（冻结/预留）替代锁 → 高并发但靠业务代码保证。

---

## 3. 开源产品调研

| 产品 | 版本 | 模式 | 实现 | 存储 | 侵入性 | 状态 |
|---|---|---|---|---|---|---|
| **Apache Seata** | v2.6.0（2026-01） | AT / TCC / SAGA / XA | Java | file/db/redis/raft | AT 零侵入（undo_log + 全局锁） | Java 生态最主流、生产验证最广 |
| **DTM** | v1.19.0（2025-02） | SAGA / TCC / 二阶段消息 / XA / workflow | Go | mysql/postgres/redis/boltdb | 无侵入（HTTP/gRPC 协议） | 通用协调器，跨语言 |
| **dtmrs** | 0.3.0 | 同 DTM | **Rust** | sqlite/postgres/mysql/redis | 无侵入 | DTM 的 Rust 移植（**对 Rust 项目最具参考价值**） |
| Atomikos / Narayana | — | XA/JTA | Java | — | 低 | 传统 JTA，协调者模式 |
| RocketMQ 事务消息 | — | 二阶段消息 | Java | MQ | 低 | 消息最终一致代表 |

**Seata AT 模式机制**（值得借鉴的"快"）：一阶段业务数据+undo_log 同本地事务提交、**释放本地锁**；
二阶段提交**异步化**（秒级完成），回滚按前后镜像反向补偿；写隔离靠**全局锁**（提交前获取，
只锁记录不锁连接）。代价：全局锁竞争 + 读隔离仅读未提交（脏读）。

---

## 4. 借鉴点分析（对山水存迹）

1. **写路径保持单分片 + 本地事务**（现有路由设计天然满足）——**不引入协调者**，避免 2PC 全部代价；
   这是与 Seata/DTM 场景的本质区别（我们无需"全局事务"编排，因为写不跨节点）。
2. **双写扩容 → 本地消息表 + 幂等消费**：迁移分片双写时，用本地消息表（pending→done）+ 重试 +
   对账收敛，替代当前"双写→追平→切换"中的强同步假设，失败可补偿、可重试（借鉴 SAGA 补偿思想）。
3. **跨分片业务事务（未来）→ SAGA 编排**：docid 级本地事务 + 补偿步骤，网关/协调器记录
   步骤状态机（借鉴 Seata-SAGA 状态机 + DTM workflow）；补偿须覆盖超时分支（宁可多发由屏障空转）。
4. **Rust 参考系 = dtmrs**：若未来需要独立事务协调器，参考 dtmrs 的无侵入 HTTP/gRPC 协议
   （业务服务只暴露补偿接口），而非 Seata 的 Java Agent 侵入式方案。
5. **幂等/空回滚/悬挂三板斧**（所有补偿方案通病）：分支登记先于 Try；补偿幂等键；回查接口
   持久化 transactionId→status（本地事务表），是任何实现的前提。

---

## 5. 思考：分布式事务"如何更快"

| 方向 | 机制 | 收益 | 落地可行性（山水存迹） |
|---|---|---|---|
| **确定性事务（Calvin 思想）** | 无协调者：全局排序即提交（事务调度与执行分离，按序执行天然无锁冲突） | 吞吐最优（跨节点事务无 2PC 往返） | 高——docid 路由已确定性，跨分片事务可扩展为"按全局事务序执行"；**远期最有价值** |
| **2PC 优化** | read-only 优化 / presume abort / 并行 prepare / 一阶段提交 | 减 1 轮往返 | 低——不采用 2PC 主路径 |
| **Seata AT 式异步二阶段** | 一阶段提交释放锁 + 二阶段异步化 | 提交路径近似本地事务 | 中——若做强一致跨节点，借鉴其"本地提交+异步补偿" |
| **本地消息表 + 批量投递** | 扫描批处理 + 指数退避重试 | 吞吐高、实现稳 | **高——首选落地**（双写扩容衔接） |
| **SAGA 编排 + 流水线** | 步骤并行化 + 状态机持久化 | 长事务吞吐 | 中——跨分片业务事务远期 |
| **写路径零 fsync（已有 Group Commit）** | 提交器模式窗口落盘 | 已 45×（91,296 ops/s） | ✅ 已完成，分布式事务的"本地腿"已最快 |

**结论（如何更快）**：
- **近期**：不引入全局协调者；写路径单分片本地事务（已最快）+ 双写扩容用**本地消息表最终一致**
  （吞吐高、可重试、与现有异步复制同构）。
- **远期**：若出现强跨分片事务需求，走 **Calvin 式确定性调度**（全局事务序）而非 2PC——
  与 docid 确定性路由一脉相承，避免协调者瓶颈与锁往返。

---

## 6. 决策（分层）

| 层 | 方案 | 一致性 | 状态 |
|---|---|---|---|
| **L0 写路径**（单分片） | 本地事务（主数据+倒排+缓存失效同进程提交）+ Group Commit 零 fsync | 强一致（本地） | ✅ 已有 |
| **L1 跨节点（双写扩容/异步索引）** | **本地消息表 + 幂等消费 + 重试/对账**（最终一致） | 最终一致 | ⏳ 开发扩展 Ex-1 |
| **L2 跨分片业务事务（未来）** | **SAGA 编排**（docid 本地事务 + 补偿状态机 + 屏障防空回滚/悬挂） | 最终一致 | ⏳ 开发扩展 Ex-2 |
| **L3 强一致跨节点（远期候选）** | **Calvin 式确定性事务**（全局事务序执行，无协调者） | 强一致 | 🔍 评估（不做 2PC） |
| 可选参考系 | dtmrs（Rust 无侵入协调器协议）——仅当需独立协调器时借鉴 | — | 🔍 评估 |

**不采用**：2PC/XA/3PC（同步阻塞 + 协调者单点 + 我们的写不跨节点）；TCC（侵入重，双写/索引补偿用不到
预留语义）；Seata 本体（Java 栈，与 Rust 项目不匹配）。
**采用**：本地消息表（L1 首选，最稳最快）+ SAGA 补偿思想（L2）+ Calvin 确定性（L3 方向）。

---

## 7. 与现有架构衔接

- **双写扩容（M5 无损扩容协议）**：把"双写强同步"升级为"双写 + 本地消息表待办 + 幂等 apply"——
  老节点写完本地事务后记 pending 消息，后台投递新节点；追平阶段失败可重放（幂等键=docid+seq），
  原子切换前校验消息表排空（替代当前"追平全量拷贝"的强假设）。
- **主从复制（ReplicationLog）**：已是异步最终一致——本地消息表可复用 ReplicationLog 的
  seq 游标 + 幂等 apply 机制（Ex-1 直接借用）。
- **物化视图/广播索引**：刷新失败 = 补偿场景——用本地消息表登记刷新任务，重试到成功（Ex-1 覆盖）。
- **网关**：SAGA 编排器可驻留网关（已有元数据中心做协调），状态机持久化到 meta 存储。

---

## 8. 风险与红线

- **无隔离性可接受边界**：SAGA/本地消息表的中间态暴露（双写期间新旧节点短暂不一致）——
  必须用"写新读新/读旧回查"策略或版本号规避；金融级账务类禁止 SAGA（除非业务字段预留）。
- **幂等是硬前提**：所有跨节点写必须幂等键（docid+全局 seq），否则补偿重试会叠加。
- **屏障（Barrier）**：空回滚/悬挂防护（分支登记先于执行；补偿幂等；回查持久化状态）。
- **不引入 Java 栈协调器**：保持 Rust 单栈；dtmrs 协议仅作参考，不依赖其二进制。

---

# 9. 倒排索引字段策略（v0.2，2026-08-29）

> 主题：**字段是否建倒排、建多少字段、term 基数与构建成本/磁盘的关系**——给出可执行准则
> 与配置模板。前置机制均已落地：M8-P4 白名单/黑名单（`inverted_fields`/`exclude_fields`）、
> P38 长文本保护（`max_term_len=96`）、M8-P7 fulltext 分词（`fulltext_fields`）。

## 9.1 问题

50M 数据集（20 字段）实测暴露两个现象：
1. **高基数字段撑爆字典**：`note-{i}` 每行唯一 → 50M 个 term，占 inverted 2.2GB 大头
   （实测 `status=active` 仅 16.6M posting，note 却有 50M 个 term）；
2. 字段多≠都该建倒排——20 字段中实际只有 9 个字符串字段生成 term（数值/布尔 11 个不走倒排、
   2 个 256 字符长文本被 max_term_len 跳过）。

## 9.2 成本分析：term 基数 M 与构建成本/磁盘

- **构建**：写入插入 O(N·F) 线性；**段构建排序 O(M·log M)**（M=唯一 term 数）——M 从 3 到 50M，
  排序成本超线性爆炸；高基数 term 的 posting 追加/内存同样更高。
- **磁盘**：倒排 = term 字典 + posting（Roaring）。**term 数越多，字典占大头**
  （每 term 字符串/哈希/偏移 ~10-20B）——note 的 50M term 即 2.2GB 大头来源。
- **核心指标是 posting 密度 = N/M（行数/唯一 term 数）**，不是 term 个数本身：

| 密度 N/M | 含义 | 判断 |
|---|---|---|
| 大（>1K/term） | 枚举/低基数，位图密集，Roaring 压缩 + 与/或高效 | ✅ 建倒排 |
| 中（10~1K/term） | 理想过滤区间 | ✅ 建倒排 |
| 小（≈1，term 数≈行数） | 唯一值字段（note）——字典膨胀 + 构建慢 + 查询退化点查 | ❌ **排除** |

## 9.3 决策：字段倒排策略三准则

1. **枚举/低基数**（唯一值 <1K）→ 建倒排 ✓（status/city/tag 等）；
2. **高基数/唯一**（note/user_id/时间戳/订单号）→ `exclude_fields` 排除 ✗；
3. **长文本**（>max_term_len）→ `fulltext_fields` 分词建词索引（不整串）。

建倒排字段数 **≤20**（M8-P4 原则：100 字段的表倒排不超过 20）。写入成本 O(N·F) 随字段数线性，
字段多不是瓶颈；**瓶颈是建倒排字段的 term 基数与数量**。

## 9.4 配置模板（实际开发）

```toml
[inverted]
engine = "hash"            # 免 FST 编译（无前缀检索需求时）
inverted_fields = ["status", "city", "tag_a", "tag_b", "region", "device", "channel"]  # 白名单 ≤20
fulltext_fields = ["title", "content", "remark"]   # 长文本分词
exclude_fields = ["note"]                          # 高基数/唯一字段
max_term_len = 96                                  # 超长自动跳过（兜底）
```

## 9.5 量化收益（db-50m 优化重建）

按 9.3/9.4 排除 note 并白名单锁定后：inverted 2.2GB → **~200MB 量级**，段构建时间同步下降，
`status/city` 等查询不变。落地任务见 development_extension.md **Ex-4**。

---

# 第 10 章 SSD 原生优化（v0.3，2026-08-29）

> 与 design.md 4.8 配套：**放弃机械硬盘（HDD）兼容，只支持 NVMe/SATA SSD**。
> ⚠️ **开发环境用机械硬盘性能大幅下降（10 倍级）、压测无参考价值——不要压测**。

## 10.1 核心设计转变（HDD 友好 → SSD 原生）

| 优化维度 | HDD 友好设计（旧） | SSD 原生设计（新） | 收益 |
| --- | --- | --- | --- |
| 随机 I/O 代价 | 尽量合并为顺序 I/O | 接受随机 I/O，消除合并开销 | 写入延迟 -30%，CPU -40% |
| WAL 布局 | 分段环形文件 | 单一大文件 + 环形指针 | 省文件切换开销 |
| SST 索引粒度 | 粗粒度（减少随机读） | 细粒度（4KB 对齐 SSD 页） | 读取放大降低 70% |
| 数据块大小 | 16KB~64KB（对齐 HDD） | 4KB（对齐 SSD 页） | 读放大 -80% |
| LSM Compaction | 避免空间放大 | 允许空间放大换取速度 | 写入放大 -60% |
| fsync 频率 | 尽量少 fsync | 充分利用 SSD 缓存 | 延迟更可预测 |
| 擦写均衡 | N/A | WAL 磨损均衡 + 冷数据迁移 | SSD 寿命 +300% |

## 10.2 场景瓶颈：「写入快 + 20 倒排字段」

- 单条写入 0.9ms 分解（估算）：WAL fsync **0.25ms**（瓶颈 1）+ 20 倒排字段更新 **0.40ms**
  （瓶颈 2，占写入 CPU 60%+，Hash 0.1 + 追加 0.2 + 元数据 0.1）+ MemTable/SST 0.1ms + 网络 0.15ms；
- 低基数字段列表极长 → 高并发倒排列表追加竞争；追加式倒排大量小 append → 元数据更新频繁；
- SSD Only 接受随机写 → 释放 WAL 与倒排更新的设计约束。

## 10.3 优化方向与优先级（P0 → P1 → P2，详细见 design 4.8.3）

- **P0（阶段一，写入 TPS 22 万→32 万）**：① 环形大文件 WAL（WAL P99 -60%）② 倒排更新批处理
  （CPU -60%）③ SSTable 4KB 块 + 两级索引（回表读放大 -75%）④ 倒排分片锁（锁竞争 -40%）；
- **P1（阶段二，→38 万）**：⑤ 倒排 FST + Mmap 字典（冷启动 8s→50ms、查表 -30%）⑥ 元数据-数据
  解耦（写放大 -50%）；
- **P2（阶段三）**：⑦ 冷热感知 Compaction + Bloom Merge（写入量 -30%）⑧ 多 SSD 条带化（多盘 +3~4×）；
- **最终目标**：写入 TPS 40 万+、写 P95 0.45ms、倒排更新 0.1ms、写放大 5×、冷启动 5s。

落地任务见 development_extension.md **Ex-5**（Ex-5.1~5.10），feature.md **J 模块**跟踪状态。

---

# 第 11 章 并发读优化（v0.4，2026-08-29）

> 背景：写入 22 万 TPS + 读 85 万 QPS 的高并发场景，读路径需持续响应。
> 现状：Engine 由 `Arc<Mutex<Engine>>` 全局串行（写路径互斥），倒排读写因此**无真实并发**
> （Rust 借用 + 全局锁保证）。本章为**读写分离/双写加速落地后的并发读设计**（feature.md I 模块 ⏸
> 项的前置设计），核心是把 Seqlock/Arc 设计加入倒排索引。

## 11.1 并发读方案全景

| 方案 | 原理 | 读阻塞 | 写阻塞 | 复杂度 | 适用场景 |
| --- | --- | --- | --- | --- | --- |
| ① 读写锁（RWLock） | 读共享锁，写独占锁 | 写时读阻塞 | 读时写阻塞 | ⭐ 低 | 读写频繁但读多写少 |
| ② 双缓冲 + 原子切换 | 两个副本，读写分离 | ✅ 不阻塞 | ✅ 不阻塞 | ⭐⭐ 中 | 写密集 + 读持续响应 |
| ③ RCU（Read-Copy-Update） | 读不加锁，写复制后发布 | ✅ 不阻塞（零开销） | 发布时短暂阻塞 | ⭐⭐⭐ 高 | Linux 内核高频读（路由表/文件系统）|
| ④ 无锁数据结构（Lock-Free） | CAS 原子操作 | ✅ 不阻塞 | ✅ 不阻塞 | ⭐⭐⭐⭐ 极高 | 极高性能（网络路由表）|
| ⑤ Seqlock（顺序锁） | 写递增版本号，读检测重试 | 可能重试 | ✅ 不阻塞 | ⭐⭐ 中 | 数据小、写多读少 |

**取舍结论（对山水存迹）**：
- ① **不用**：读写频繁交替时写阻塞读、读阻塞写，读延迟飙升；
- ③ **不引入**：双缓冲已足够（读 85 万 QPS），RCU 宽限期管理 + 内存屏障复杂度收益有限；
- ④ **不用**：DashMap 分片锁已接近无锁效果，易用性/性能平衡已佳，无锁实现（ABA/内存回收）风险不值；
- ② **已用/规划**：MemTable 切换（MemTableBuffer 双缓冲已实现）、SSTable 元数据指针、倒排段清单（Manifest）、配置热加载；
- ⑤ **引入倒排**：倒排字典 FST 指针切换（8 字节指针，Seqlock 零开销写、查询几乎不重试）。

## 11.2 各模块最优组合建议（现状核对）

| 模块 | 推荐方案 | 现状 | 行动 |
| --- | --- | --- | --- |
| MemTable 切换 | 双缓冲 | ✅ 已实现（`MemTableBuffer`，active/buffer 切换）| 无 |
| SSTable 元数据指针 | 双缓冲 + Arc | ✅ 稀疏索引内存摘要常驻 + 精确索引懒加载（两级索引）| 无 |
| **倒排字典（FST）指针** | **Seqlock 或 Arc** | 现状：`dicts: HashMap<String, fst::Map>` open 加载、flush 时插入，读走 &self | **Ex-6 引入 Seqlock/Arc** |
| HotCache 热点缓存 | DashMap（分片锁）| ✅ 已实现（LruCache 字节预算 + LFU 采样，P41）| 无 |
| **倒排文件段清单（Manifest）** | **双缓冲 + Arc** | 现状：`segments: Vec<String>` &mut 修改、&self 遍历 | **Ex-6 引入 Arc 快照** |
| 配置热加载 | Arc + 原子指针 | ✅ 已实现（Config::reload + ReloadReport，P3-1）| 无 |

## 11.3 Seqlock 设计（倒排索引应用，核心）

**为什么倒排适合 Seqlock**：段清单（`Vec<String>`，每条 30B）与 FST 字典（`Arc<Map>` 指针 8B）
都是**小数据**（能装进缓存行），写（flush_segment / gc）不频繁且要零阻塞读；Seqlock 读只可能重试
且几乎不重试，写零阻塞——完美匹配"数据小、写多读少"。

```
Seqlock 原语：
写（flush_segment / gc 更新段清单/FST 指针）：
  version.fetch_add(1, SeqCst);        // 进入奇数（写中）
  修改 segments / 替换 dicts 指针
  version.fetch_add(1, SeqCst);        // 回到偶数（一致）

读（search / doc_count 遍历段）：
  loop {
    v1 = version.load(SeqCst);
    if v1 & 1 == 1 { continue; }       // 写进行中，重试
    let snapshot = &self.segments;     // 拷贝/快照段清单（小，<缓存行）
    v2 = version.load(SeqCst);
    if v1 == v2 { break; }             // 一致，用快照
  }
```

**倒排落地位置**（Ex-6）：
1. **段清单 `segments`**：`Vec<String>` 改为 Seqlock 保护的版本化快照——`flush_segment`/`gc`
   更新时写版本号；`search`/`doc_count`/`iter_terms` 读快照，无锁；
2. **FST 字典 `dicts`**：`HashMap<String, fst::Map>` 的值改为 `Arc<fst::Map>`，配合
   `ArcSwap`（原子指针发布）或 Seqlock 版本化——查询 `dicts.get(seg)` 拿 Arc 快照，读路径零拷贝；
3. **边界**：段清单/字典数据量大时不适用 Seqlock（拷贝开销高）——倒排段数通常 <100、
   FST 指针 8B，均满足"一个缓存行内"约束；SSTable 大块数据不适用（维持双缓冲 + Arc）。

**依赖**：需要先打破 Engine 全局锁（读写分离/双写加速，feature.md I 模块 ⏸）才有真实读-写并发；
在全局锁下实现 Seqlock 无收益（写与读仍串行）。落地顺序见 development_extension.md **Ex-6**。

## 11.4 落地路径

1. **Seqlock 原语**（`src/seqlock.rs`，~100 行）：版本号 + 快照模板方法（通用小数据）；
2. **倒排接入**：段清单 + FST 字典指针 Seqlock/Arc（依赖读写分离先行或独立先做原语与单元测试）；
3. **验证**：并发读-写正确性（读不阻塞写、写不阻塞读）、重试率统计（预期 <0.1%）、
   与全局锁基线对比吞吐；SSD 环境实测（HDD 不压测）。
4. 状态跟踪：development_extension.md **Ex-6**；feature.md I 模块。

---

# 附录：与各文档关系

- **design.md**：主设计（存储引擎 4.x、索引 5.x、缓存 6.x）；
- **development.md 7.x**：各里程碑落地记录（7.27 起为并发读优化）；
- **development_extension.md**：Ex-1~Ex-6 落地计划与状态；
- **feature.md**：主项目功能清单（I 模块含读写分离/并发读候选）。

---

# 第 12 章 多核优化（v0.5，2026-08-29）

> 背景：多核 CPU 下数据库的明确可处理点集中在**锁竞争、缓存局部性、IO 并行**三层。
> 核心不是"让所有核干活"，而是**数据分片（Shard Everything）**——按核分计数器、
> 按 Term 分锁、按物理核分调度池，把单核极限吞吐提升为多核稳定并发。
> 现状核对：design.md 已含三池模型/防超售分区/绑核（默认关闭）/io_uring/compaction 限流
> （design 14.1.2 / 1177-1187）；Ex-5.2（倒排 256 shards + 位图分片）与 Ex-6.1（Seqlock）
> 已落地锁竞争优化。本章新增**缓存伪共享**设计与其余强化点。

## 12.1 多核五处理点全景（现状标注）

| # | 层面 | 处理点 | 现状 | 行动 |
| --- | --- | --- | --- | --- |
| 1 | 锁竞争 | 倒排热路径：DashMap 分片调大（256/512）、Sharded Lock 按 Term、无锁读 | ✅ Ex-5.2（256 shards + bitmaps 按 field 分片）、Ex-6.1（Seqlock 无锁读）| 无（已落地）|
| 2 | 缓存伪共享 | 高频写统计按核拆分（PerCpuCounter）+ 热结构体 `#[repr(align(64))]` | ❌ 未覆盖 | **Ex-7.1 新增** |
| 3 | 调度/NUMA | 三池绑物理核（网络 0-3 / 计算 4-7 / IO 尾核），稳定 P99 | design 已有（`[affinity]` 默认关闭）| Ex-7.2 默认开启 + 文档指引 |
| 4 | WAL/IO 并行 | io_uring SQPOLL 多核提交 + WAL/SSTable 落不同 NVMe 队列/SSD | design 阶段 3 已有 io_uring；多队列未细化 | Ex-7.3 强化 |
| 5 | Compaction | 并行度 + 动态限流（rate_limit_mb/s），不给前台抢占 | design 已有 rate_limit/ionice | Ex-7.4 动态限流 |

## 12.2 锁竞争（已落地，Ex-5.2 / Ex-6.1）

- **Term 字典分片**：`mem: DashMap` 256 shards（2 的幂），低基数 Term 高并发 1.39×（Ex-5.2 demo）；
- **位图索引分片**：按 field hash 256 片锁，不同 field 并行；
- **无锁读**：Seqlock 原语（Ex-6.1，零 unsafe）——段清单/FST 字典接入待读写分离（Ex-6.2/6.3）；
- 若极高频 Term（status=active）仍成热点，可进一步 **Term 级 Sharded Lock**（按 Term Hash 独立锁，
  同 Term 串行、不同 Term 并行）——Ex-5.2 已按 DashMap shard 近似实现。

## 12.3 缓存伪共享（False Sharing，新增设计，Ex-7.1）

**问题**：多核同时修改相邻内存（相邻 DocId 计数器/统计字段）→ 同一 64B 缓存行频繁失效 →
性能骤降（每核修改触发其他核缓存行无效 + 内存同步）。

**处理点**：

```rust
/// 按核拆分的计数器（PerCpuCounter）：每核独立累加，读取时汇总——
/// 避免多核写同一计数器导致缓存行抖动。
#[derive(Default)]
pub struct PerCpuCounter {
    /// 每核独立槽（align(64) 隔离缓存行，杜绝伪共享）。
    slots: Vec<std::sync::atomic::AtomicU64>,
    /// 有效核数（0 = 自动检测）。
    ncpu: usize,
}
impl PerCpuCounter {
    pub fn new() -> Self {
        let ncpu = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        Self { slots: (0..ncpu).map(|_| std::sync::atomic::AtomicU64::new(0)).collect(), ncpu }
    }
    pub fn add(&self, n: u64) {
        // 当前线程绑核 id（近似：thread_id % ncpu）；生产可经 affinity 精确映射
        let idx = std::thread::current().id().as_u64() as usize % self.ncpu;
        self.slots[idx].fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn sum(&self) -> u64 {
        self.slots.iter().map(|s| s.load(std::sync::atomic::Ordering::Relaxed)).sum()
    }
}
```

- **热结构体对齐**：高频共写的统计/索引结构体加 `#[repr(align(64))]`（缓存行对齐），
  相邻字段天然隔离；不同核写不同字段时互不失效缓存行；
- **落地点**：`total_writes` / 倒排 `mem_docids`（现为单 AtomicU64 原子 RMW——多核写竞争）→
  PerCpuCounter；HotCache 统计字段 align(64)。

## 12.4 调度与 NUMA / 绑核（design 已有，Ex-7.2 强化）

- design 1177-1187 已定义三池防超售分区 + `[affinity]` 绑核（默认关闭）：
  - 网络池（Tokio）：N×0.3 物理核；计算池（Rayon）：N×0.3；后台 IO 池：固定 2~4；
  - **绑核建议**：网络池绑物理核 0-3、计算池绑 4-7、IO 池尾核——避免 Tokio 工作窃取使
    热点数据跨核"跳跃"（稳定 P99）；跳过超线程虚拟核；跨 NUMA 避免跨插槽访存；
- **Ex-7.2**：`[affinity]` 默认开启（多核机器），`core_affinity` crate 绑核 + taskset 兜底；
  验证：绑核 vs 不绑核的 P99 对比（SSD 环境实测，HDD 不压测）。

## 12.5 WAL 与 IO 并行（design 已有，Ex-7.3 强化）

- design 阶段 3：io_uring SQPOLL 异步提交 + 环形 WAL + O_DIRECT（NVMe 延迟 -30%~50%）；
- **强化（多队列）**：WAL 与 SSTable 落**不同 NVMe 队列**（`/sys/block/nvme*/queue/` 或
  不同 SSD）——WAL fsync 与 SSTable 刷盘 IO 并行，互不争抢；倒排文件可独立放置
  （同 Ex-5.10 多 SSD 条带化）；
- Ex-7.3：io_uring SQPOLL 落地（阶段 3）时按多队列/多盘配置验证写并行收益。

## 12.6 Compaction 并行度 + 动态限流（design 已有，Ex-7.4 强化）

- design 4.5：ionice 最低优先级 + `compaction.rate_limit_mb/s` + 写 Stall 阈值；
- **强化（动态限流）**：Compaction 并行度仅限后台 IO 池特定线程（2~4），
  按前台负载动态下调 `rate_limit_mb/s`（写压力高时压缩 Compaction 带宽，给前台网络/计算池
  预留算力）；Ex-5.4（compaction 20× 层级 + 并行）落地后叠加动态限流。

## 12.7 落地任务（Ex-7，development_extension.md）

| 任务 | 内容 | 优先级 |
| --- | --- | --- |
| Ex-7.1 | 缓存伪共享：PerCpuCounter（统计/计数器按核拆分）+ 热结构体 `#[repr(align(64))]` | P0 |
| Ex-7.2 | `[affinity]` 绑核默认开启 + 三池物理核分区（P99 验证）| P1 |
| Ex-7.3 | io_uring SQPOLL + WAL/SSTable 多 NVMe 队列/多盘 | P1 |
| Ex-7.4 | Compaction 动态限流（按前台负载调 rate_limit）| P2 |
