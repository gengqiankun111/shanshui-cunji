# 山水存迹数据库（shanshui-cunji）功能开发清单

> 分模块列出开发任务与完成状态。状态：✅ 已完成 · 🔄 进行中 · ⏳ 待办 · ⏸ 暂缓/评估
> 里程碑编号：M1~M6（development.md 第 11 章）、M7 深度优化、M8-P0~P9 前沿路线（Group Commit / 倒排过滤 / WAL 截断 / 批量导入 / fulltext / 分页 / 中文分词）。
> 维护：每个功能完成后更新本文件对应状态与提交号（与 development.md 7.x 同步）。

---

## A. 存储内核（WAL / MemTable / SSTable / Compaction）

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| WAL 预写日志（append 模式，崩溃回放） | ✅ | M1 |
| WAL 环形模式（预分配环形文件，回绕覆盖安全） | ✅ | M6-1 `66813c9` |
| WAL 截断回收（append 模式 flush 后截断 + 文件头持久化 seq） | ✅ | M8-P5 `a4d829a` |
| MemTable 跳表 + 双缓冲（Mutable/Immutable） | ✅ | M1 |
| SSTable 读写 + 块级压缩 + 分区布隆过滤（v5） | ✅ | M4 `e1eebce` |
| SSTable 两级索引（Level 1 常驻摘要 + Level 2 精确懒加载） | ✅ | M5 `8bcc077` |
| 基础 Compaction（全量合并，崩溃安全） | ✅ | P3-3 `3c48521` |
| Leveled-Compaction（L0→L1→L2 分层压实） | ✅ | M6-2 `4c2e17a` |
| IO 速率调度器（Token Bucket 限速） | ✅ | P3-2 `4884a58` |
| **scan 范围扫描流式化**（k-way merge，内存 O(page) 不随总量膨胀） | ✅ | M8-P10（`scan_stream` + 分页接入） |

## B. 写入路径 / 提交模型

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| Group Commit 组提交（提交器模式，写路径零 fsync） | ✅ | M8-P0 `648d9bd`（A 写重 45×：91,296 ops/s） |
| 批量导入模式（HotCache 跳过回填，防内存崩溃） | ✅ | M8-P6 `bde422d`（50M 导入 WS 4.9GB→0.6GB） |

## C. 倒排索引 / 全文检索

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| 倒排索引基础（内存字典 + 磁盘段 + FST 术语字典 / hash 引擎） | ✅ | M3 |
| 倒排架构升级：预分片 Chunk + 段 GC | ✅ | M5 `7db4764` |
| 位图索引（枚举字段白名单，COUNT/AND/GROUP BY 快速路径） | ✅ | M7-2 `4a19550` |
| 字段白名单 / 黑名单 / 长文本保护（max_term_len，防字典膨胀） | ✅ | M8-P4 `cde4f18`（字典压缩 45 万倍） |
| fulltext 分词索引（长文本可检索，`ft:field:token`） | ✅ | M8-P7 `545682f` |
| 中文 bigram 分词（中英混合文本检索） | ✅ | M8-P9 `72badfe` |
| **jieba 完整中文词典分词**（`cjk_segmenter`，语义词精确命中） | ✅ | M8-P13（`cjk-jieba` feature + `tokenize_seg`） |
| 倒排字段策略落地（Ex-4：9.4 模板重建 db-50m，inverted 2231.8→144.3MB -93.5%） | ✅ | `db-50m-opt`（配置模板 `config.import-example.toml`） |
| 倒排 posting 流式输出（大结果集已由分页解决） | ✅ 评估完成 | demo `src/demo/posting-stream`：位图惰性迭代 + 分页内存 O(limit) 已流式；深页 O(offset)（16M offset 591ms vs 近页 36µs，16,204×）；游标续扫 O(limit)/页（10 万条 37×）；**不引入全量流端点**，深页高频翻页如需再加 search_after 游标 |

## D. 查询执行 / 缓存 / MVCC

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| 查询执行器 + 优化器静态路由（AccessPath 枚举） | ✅ | M2 |
| MVCC 快照读（get_at 按快照点过滤） | ✅ | M6-3 `07f556e` |
| MVCC 全局 seq 一致性（跨列族共享 seq 分配） | ✅ | M7-1 `4283568` |
| 热点缓存 HotCache（LRU/LFU + 保护区自动晋升） | ✅ | M6-4 `e34ea87` |
| HotCache 内存缺陷修复（stats 泄漏 / used_bytes 虚增 / LFU O(N) 风暴） | ✅ | P41 `5a937ea` |
| 查询分页（limit/offset/total，防大结果集内存爆炸） | ✅ | M8-P8 `45c3a54`（5M 命中 WS 10GB→221MB） |
| **scan 范围扫描流式化**（见模块 A） | ✅ | M8-P10 `516643f` |
| **scan 游标续扫**（after + 提前终止，全库遍历每页 O(limit)） | ✅ | M8-P11（`scan_after` + `/range?after`） |
| 类 SQL 解析（sqlish，SELECT...WHERE AND/OR 子集，零依赖递归下降） | ✅ | `441282d`（等值走倒排/比较+BETWEEN 扫描+AND 后过滤快路径+看门狗熔断；`/sql?q=` 路由） |
| 写入 Enrich（预连接，design 19.2 ②，join::put_with_enrich） | ✅ | `706c33b`（`[enrich] enabled && source=local` 时 /put WAL 前按 from_field→to_field 展开关联文档；fail_policy reject/degrade；`_enrich.related`） |

## E. 数据管道 / 迁移 / 导入导出

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| 数据导出（CSV） | ✅ | M4 `e96c7c9` |
| 数据导入（CSV / JSONL，复用 migrate 内核） | ✅ | M4 `e96c7c9` |
| import-schema（预注册字段 + 倒排白名单 + 组合索引声明） | ✅ | M5 `35f87cd` |
| 增量导入（docid 游标断点续传） | ✅ | P3-4 `5085db8` |
| Parquet 数据集生成器 + 批量导入（5000 万 × 20 字段） | ✅ | M8-P3 `30b1639` |
| mysqldump 导入（MySQL 迁移） | ✅ | M1（migrate 工具） |
| 导出增强（增量 / Parquet / JDBC） | ⏳ | 阶段 2 规划，未启动 |

## F. 备份 / 一致性 / 外部缓存

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| 全量备份一致性准备（刷 WAL + MemTable + 倒排段） | ✅ | M4 |
| 增量备份 / 恢复（seq 游标，缺口检测） | ✅ | M6-5 `266d03d` |
| Redis 外部缓存（Cache-Aside + 写失效 + 熔断） | ✅ | M5 `da20c4c` |
| Redis 冷热分层 SDK 门面（读回填 + 双删协调） | ✅ | P3-6 `4f693bd` |
| 小表广播 JOIN（阈值判定 + 全量索引 / 回退点查） | ✅ | P3-5 `31fc054` |

## G. 分布式 / 网关（阶段 2）

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| 一致性哈希分片路由（128 虚拟点） | ✅ | M5 `dc17043` |
| 元数据中心 + 平滑扩容（只迁 ~1/N 虚拟分片） | ✅ | M5 `53bb924` |
| 分片节点 RPC + 网关（广播检索 Chunk 直拼） | ✅ | M5 `53bb924` |
| 主从复制（ReplicationLog + 游标推送） | ✅ | M5 `eca36c6` |
| 网关全局 Term 缓存（广播查询） | ✅ | M5 `eca36c6` |
| TDS 术语字典热备 + 无损扩容协议（双写→追平→切换） | ✅ | M5 `fda44a6` |
| 物化视图调度器（Count/Sum/Avg + 增量刷新） | ✅ | M5 `8bcc077` |
| 四层看门狗（写停滞检测 + 心跳 Sidecar） | ✅ | M5 `df8e9d4` |
| 两节点真实 TCP 高并发强一致性测试（8 线程并发写 → 广播精确命中/跨节点路由可见） | ✅ | `3e22f80`（gateway `high_concurrency_writes_strong_consistency_two_nodes`） |
| 真机两节点分布式强一致测试（本机 NVMe + 阿里云 2核/1.6GB/高效云盘，SSH 隧道跨机） | ✅ | 7.51（`src/bin/cluster_demo.rs`：--node 分片节点 + --gateway 网关；4 线程 2000 条跨机并发写 → 逐条点查全部可见 + 广播精确命中，52.4s） |
| 本地消息表 + 幂等消费（Outbox，Ex-1） | ✅ | `7348acd`（列族 outbox：业务写+待办同一本地事务、投递器、按 (docid,seq) 幂等 apply、排空校验；异步索引补偿/扩容衔接基础） |
| SAGA 编排 + 补偿状态机（Ex-2） | ✅ | `990bf6b`（src/saga.rs SagaCoordinator：SagaStep trait + 状态机 + 屏障防空回滚/悬挂 + JSON 持久化续跑 + 补偿幂等；Ex-2.5 网关 /saga/* HTTP 留待分布式阶段） |
| Calvin 确定性事务评估（Ex-3，L3） | 🔍 | demo `src/demo/calvin`：确定性序零锁等待/无协调往返/吞吐与跨分区比例无关；**评估结论不进入 kernel**（单 docid 路由天然不分片 + L1/L2 已覆盖，远期触发再落地） |
| 读写分离（Mutex/RwLock/COW 快照读） | ⏸ | M8-P1 `be09a07` demo 结论暂缓（组提交已解决读被写拖垮） |
| 高并发查询优化（design 9.5 目标） | ⏳ | M6 后留待 |

## H. 运维 / 质量 / 性能工具

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| admin status / QueryRegistry / explain 推演 | ✅ | M4 `e96c7c9` |
| 配置热加载（reload 校验 + 变更区块报告） | ✅ | P3-1 `69b39dc` |
| 全局分配器（mimalloc 默认 / jemalloc 可选） | ✅ | M4 `b0eaa58` |
| YCSB 压测工具（负载 a/b/c/f + 分位数） | ✅ | M7-3 `d918c47` |
| 质量文档体系（quality_system P1~P41 / problem_solving） | ✅ | 持续维护 |
| 三规模性能实测（1000万/2000万/5000万 + 截屏存档） | ✅ | v0.1.0~v0.5.0 发布系列 |

## I. 前沿探索（frontier）

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| 前沿调研（BVLSM/RusKey/DobLIX/TieredKV/AuraDB） | ✅ | M7-3 `d918c47` + frontier-research-2026-08.md |
| 环形 WAL 头部 tail 合并 fsync（sync 单次原子提交） | ✅ | M8-P12（ring+gc 68,756 ops/s，2.3×） |
| 读写分离 / 双写加速 | 🔍 评估完成（维持暂缓） | M8-P1 `be09a07` + `src/demo/rw-separation`（读 P95 3µs→2µs 1.5×、写吞吐不增——写瓶颈 fsync 非锁；组提交已解决读被写拖垮）；读路径 &self 基础已落 `c48a7c1`（read_block 位置读 + ColumnFamily get &self）；剩余阻塞：HotCache 内部 Mutex 化 + 倒排 pending 刷盘归属（搜索类读仍走写锁）；复制型 read_from_replica 属分布式阶段 |
| 倒排并发读（Seqlock/Arc：段清单 + FST 字典指针无锁读） | 🔄 | Ex-6（Ex-6.1 原语 ✅ `1946161`；Ex-6.2/6.3 ArcSwap 段清单+FST 字典快照化 ✅ `c8183cf`；真实读写并发待读写分离） |
| 倒排 posting 压缩（Roaring 已用，Gorilla/变长探索） | ✅ | 探索验证：Roaring 已达理论下限（密集 0.13B/docid=1bit，稀疏 2B/docid 为 delta 2×，但 Roaring AND 快 20×）——维持 Roaring 不引入新编码 |

## J. SSD 原生优化（v0.7 起，Ex-5，放弃 HDD 兼容）

> 定位（design 1.2/4.8）：只支持 NVMe/SATA SSD；目标「写入快 + 20 倒排字段」写入 TPS 40 万+。
> ⚠️ **开发环境用机械硬盘性能大幅下降、勿压测**（design 1.2 警告）。

| 任务 | 优先级 | 状态 |
|---|---|---|
| SSTable 4KB 块 + 两级索引（block_size_kb 16→4，回表读放大 -75%） | P0 | ✅ Ex-5.1 `056b21d`（实测读放大 1257B→413B -67%，L1 摘要内存不膨胀） |
| 倒排分片锁（Term Hash 256 分区，锁竞争 P99 -40%） | P0 | ✅ Ex-5.2 `c7ebe72`（Term 字典 4→256 shards，低基数并发 1.39×；位图索引按 field 分 256 片） |
| 倒排更新批处理（同 Term 攒批批量追加，CPU -60%） | P0 | ✅ Ex-5.3 `d38e8ab`（add_batch 按 term 聚合 + Engine 攒批缓冲，20 字段导入 1.7×） |
| Compaction 参数调优（层级 20× + 并行 2~4，写放大 -60%） | P0 | ✅ Ex-5.4 `624ce9e`（l0 阈值 8→12 空间换写放大 + compaction_parallel 三列族并行 2.14×；**P0 全部完成**） |
| 环形大文件 WAL 规模化（预分配 + 磨损均衡，WAL P99 -60%） | P1 | ✅ Ex-5.5 `4974ef3`（wal_ring 64→256MB；验证 1GB 预分配/多轮回绕恢复/磨损均衡天然成立） |
| 删除位图（Deletion Bitmap，删除 IO -99%） | P1 | ✅ Ex-5.6 `e615071`（按 DocId 1bit + 4KB 页对齐脏页 fsync：delete 跳 Tombstone -99% IO、get O(1) 跳过、put 清位复活、compaction 物理丢弃已删 key） |
| 倒排 FST + Mmap 字典（冷启动 8s→50ms） | P1 | ✅ Ex-5.7 `442981c`（crates/mmap-file 只读 mmap 安全封装落地 P23 白名单：FST 字典 fs::read 全量加载→mmap 按需缺页，冷启动零堆分配 17.3MB→0B+0.14ms） |
| 元数据-数据解耦（Compaction 只重写元数据，写放大 -50%） | P1 | ✅ Ex-5.8 `cd00d85`（无重叠 L0 合并数据块级复用：零解压只重建 Block Index/Bloom/Footer；demo 4041ms→毫秒级；有重叠/位图过滤/PAX 回退全量） |
| 冷热感知 Compaction + Bloom Merge（写入量 -30%） | P2 | ✅ Ex-5.9 `ba709e2`（SST 读热度统计 + L0 超阈值热段优先合并下沉 L1 + sst_heat 监控；Bloom Merge 由 Ex-5.8 无重叠检测承担） |
| 多 SSD 条带化（WAL 独占最快盘，多盘 +3~4×） | P2 | ✅ Ex-5.10 `e6a5610`（[storage] wal_dir/sst_dir/inverted_dir 三盘路由：WAL 独占最快盘 + SSTable 数据盘 + 倒排独立盘；未配置回退单盘） |

## K. 多核优化（v0.5，Ex-7，Shard Everything）

> 设计（design_extension v0.5 第 12 章）：锁竞争（已落地）→ 缓存伪共享 → 绑核 → io_uring 多队列 → compaction 动态限流。
> ⚠️ 性能验证须在 SSD 环境（HDD 不压测）。

| 任务 | 优先级 | 状态 |
|---|---|---|
| 缓存伪共享：PerCpuCounter（按核分计数器）+ 热结构体 `#[repr(align(64))]` | P0 | ✅ Ex-7.1 `c5fa66c`（src/per_cpu.rs：align(64) 缓存行隔离 + thread_local 槽位映射；倒排 mem_docids/SST heat 改造；demo 8 线程写 2.1×） |
| `[affinity]` 绑核默认开启 + 三池物理核分区（网络 0-3/计算 4-7/IO 尾核，P99 验证） | P1 | ✅ Ex-7.2 `b294532`（src/affinity.rs 三池分区 + core_affinity 绑核：server 主线程/Compaction 并行/组提交后台；taskset 兜底） |
| io_uring SQPOLL + WAL/SSTable 多 NVMe 队列/多盘 | P1 | ✅ Ex-7.3 `fd0b519`（src/io_queue.rs IoClass→队列号抽象与 Ex-5.10 条带化对齐；io_uring 仅 Linux，unsafe 依赖待独立 crate 封装后接入 SQPOLL 后端） |
| Compaction 动态限流（按前台负载调 rate_limit_mb/s） | P2 | ✅ Ex-7.4 `ddbc20e`（IoRateLimiter::set_rate 动态调速 + Engine 按 MemTable 水位下调限速：压力 p→base×(1-0.5p)，写压力高让路 50% 带宽；**Ex-7 全部完成**） |

---

## 近期里程碑（按完成顺序）

- **M8-P9 中文 bigram 分词**（`72badfe`）：tokenize 字符类分段，中文 fulltext 可检索（2-4 字关键词 bigram AND 精确命中）
- **M8-P10 scan 范围扫描流式化**（`516643f`）：k-way merge（BinaryHeap 最小堆 O(N log K)），
  `scan_range_paged` 内存 O(page)——50M 库全库分页查询 WS 691MB（旧实现全量收集会 OOM）
- **M8-P11 scan 游标续扫**：`scan_after` + `/range?after`——全库遍历每页 O(limit)，
  50M 库翻页 164-682ms（旧 total 模式全库 70s）
- **Ex-1 本地消息表 + 幂等消费**（`7348acd`）：src/outbox.rs Outbox 列族（docid+seq 复合键），
  业务写 + 待办同一本地事务（共享全局 seq 与 fsync 点）、投递器 dispatch、IdempotentConsumer
  按 (docid,seq) 去重、排空校验——分布式事务 L1 首选方案落地，双写扩容/异步索引补偿衔接基础
- **Ex-2 SAGA 编排 + 补偿状态机**（`990bf6b`）：src/saga.rs SagaCoordinator——SagaStep trait +
  状态机（init→executing→succeeded/failed→compensating→compensated）+ JSON 持久化续跑 +
  屏障（分支登记先于补偿/空回滚/悬挂防护/补偿幂等）+ 回查接口——L2 跨分片业务事务基础
- **Ex-3 Calvin 确定性事务评估**（🔍 demo 结论）：确定性序零锁等待 / 无协调往返 / 吞吐与
  跨分区比例无关（10%/50%/90% 恒 11k vs 2PC 105k/125k/145k）；但本项目单 docid 路由天然
  不分片 + L1/L2 已覆盖 → **不进入 kernel**，远期强一致多 docid 需求触发再落地
- **Ex-4 倒排字段策略落地**（`db-50m-opt` 重建）：9.4 模板（7 枚举白名单 + note 排除 +
  max_term_len=96）——inverted **2231.8→144.3MB（-93.5%）**、50M 行导入 838s、计数/点查全不变；
  配置模板固化 `config.import-example.toml`
- **类 SQL 解析器**（`441282d`）：src/sqlish.rs 零依赖递归下降（SELECT...WHERE AND/OR/NOT/
  括号/BETWEEN/比较/LIMIT/OFFSET）+ 引擎求值——等值走倒排、比较/BETWEEN 扫描（AND 后过滤
  快路径）+ 看门狗熔断；`GET /sql?q=...`；demo 6 测试先验证
- **写入 Enrich 接线**（`706c33b`）：`[enrich] enabled && source=local` → /put 走
  join::put_with_enrich（WAL 前展开关联文档 `_enrich.related`，fail_policy reject/degrade）
- **M8-P12 环形 WAL 头部 tail 合并 fsync**：sync 单次原子提交（消除冗余第二次 fsync）——
  ring+gc 2ms 68,756 ops/s（M8-P1 基线 30,270 → 2.3×）
- **M8-P13 jieba 完整中文词典分词**：`[inverted] cjk_segmenter="jieba"`（`cjk-jieba` feature，
  词典嵌入默认开）——中文语义词精确命中（"数据库"单 term），索引词数 ≤ bigram 碎片
- **倒排 posting 压缩探索**（demo 验证，不集成）：Roaring 密集容器 1bit/docid 已达理论下限
  （16.6M 连续 0.13B/docid），delta-varint 1B/Gorilla 2B 均更差；稀疏 1% 场景 Roaring 2B/docid 为
  delta 2×（绝对差 ~0.5MB/500K docid），但 Roaring AND 查询快 20.1×（337us vs 6.7ms）且库成熟
  → **维持 Roaring，不引入新编码**

## 下一候选

- 类 SQL 解析 / 写入 Enrich / 读写分离（⏸）