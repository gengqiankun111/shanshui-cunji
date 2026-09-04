# 山水存迹数据库（shanshui-cunji）设计摘要

> 版本：v0.1 设计稿精简版（整合原文档分散内容，删减冗余与配置明细）
> 定位：面向海量结构化文档的高性能分布式文档-检索数据库

---

## 一、产品定位与设计哲学

**一句话**：主打高吞吐写入、热点主键极速查询、任意字段灵活筛选；可水平扩展至 **50 亿级**；弱化跨分片事务，适合日志、埋点、标签画像、元数据存储。

**核心能力**：
1. 热点主键查询亚毫秒（HotCache 命中直返）；
2. 固定条件查询对标 MySQL（组合稀疏索引，免全表扫描）；
3. 任意字段 AND/OR 筛选（原生倒排索引，免预建组合索引）；
4. 容量大：单节点 1.5 亿文档 / 约 300GB，分片集群线性扩展至 50 亿；
5. 单点读写延迟远优于 TiDB 类强一致分布式库；
6. 短板：不带分片键的全局检索是广播慢查询，不宜放超高并发核心路径。

**设计哲学（放弃换什么）**：

| 放弃 | 换取 |
| --- | --- |
| 跨分片事务 / 全局分布式锁 | 高吞吐写入、低延迟查询 |
| 跨分片 JOIN | 水平扩容简单、单点性能保留 |
| Raft 强同步 | 单点写入延迟≈单机 |
| HDD 兼容 | SSD 原生存储红利 |

**存储介质（v0.7 起确认，原分散于 1.2/1.3/4.8，此处合并）**：纯 SSD 持久化（NVMe/SATA SSD）。数据必须经 **WAL fsync + 落盘 SSTable** 才算成功；HotCache / BlockCache / 倒排字典仅是内存加速层，断电重启后缓存清空、磁盘数据完好。放弃 HDD 换来：接受随机 IO、4KB 块对齐 SSD 页、环形 WAL、空间放大换写入放大降低、并行 Compaction——核心路径代码量 -30%~50%、性能 +30%~60%。⚠️ 开发环境用 HDD 性能下降 10 倍以上，**勿压测**。

**生态定位（MySQL 管钱，Redis 管热，shanshui-cunji 管海量）**：
- 与 MySQL：Canal / Debezium 监听 binlog 实时同步为只读分析副本；
- 与 Redis：Cache-Aside + Write-Invalidate 冷热分层，1 份 Redis 内存换全量历史查询；
- 可替代 MongoDB/ES 的条件：仅做结构化文档存储 + 简单筛选（无嵌套 / 聚合 / 全文），可降 ~60% 硬件成本、写速提升 3~5 倍；
- 禁止替代：银行交易、库存扣减、购物车结算（需跨行事务）。

**竞争定位**：LSM 写入吞吐 > MongoDB；查询灵活度 > Cassandra/HBase；无 JOIN 场景延迟远低于 Spark/Flink。杀手锏：倒排统计载荷毫秒级 COUNT/GROUP BY + TTL 时间分区整删文件。短板：无跨分片事务、无 JOIN / 子查询、大集广播查询慢。

**选型**：强 ACID → TiDB/OceanBase；复杂 SQL/BI → ClickHouse/Doris；高度嵌套 JSON → MongoDB；跨地域强一致 → CockroachDB；**海量写入 + 快速检索 → shanshui-cunji**。

---

## 二、总体架构

**单机 MVP**：网络服务层（TCP / HTTP-JSON / CLI）→ 查询执行器（主键 / 组合索引 / 倒排）+ HotCache → LSM-Tree 存储引擎（WAL → MemTable 跳表 → SSTable 多层；布隆 / 块缓存 / Compaction）。

**分布式（阶段 2）**：无状态网关 Proxy（分片路由 / 请求分发 / DocId 合并 / 限流监控）→ N 个单机内核分片 → 轻量元数据中心 Meta-Server（路由表 / 健康 / 迁移）。原则：**计算下沉分片，网关只转发与简单合并**。

---

## 三、数据模型与 API

- 文档模型：`DocId(u64)` 主键 → 扁平字段 key-value（字符串 / 数值 / 布尔 / 时间等标量）；
- 存储键空间：主数据（DocId→文档）、组合索引（`Encode(字段…, DocId)`）、倒排索引（独立内存字典 + 追加式文件，不走 LSM KV）；
- **列族分离**：`cf_data` / `cf_cidx` / `cf_inv` / `cf_delta`，各自独立 MemTable / SST / 缓存 / Compaction / 物理目录，TTL 分桶互不干扰；
- 序列化：自定义二进制（字段名→u16 字典编码）；**FieldRegistry 字段注册表必须持久化**（`metadata/fields.idx`），支持 Schema 演进；
- CRUD：`put / patch(部分更新) / get / delete(墓碑) / range / search`；
- 接口：HTTP-JSON、TCP 简易协议、CLI；类 SQL WHERE 子集（sqlparser-rs），**不承诺 MySQL 兼容**，无 JOIN / GROUP BY / 子查询 / 事务。

---

## 四、存储引擎（单机内核，核心中的核心）

技术路线：**LSM-Tree**。

**写入路径**：WAL → MemTable（跳表，双缓冲切换，刷盘不阻塞写入）→ SSTable → Compaction。写 Stall 可配置（L0 文件数阈值默认 8），用 Semaphore 限流防内存爆炸。

**WAL**：批量组提交（杜绝逐条 fsync）；循环分段日志，旧文件延迟删除（重命名 + 后台异步 unlink，防 GB 级 unlink 阻塞毛刺）；双写入模式（强安全默认 / 高性能延迟刷盘）；高性能模式（阶段 3）：io_uring + 环形 WAL + O_DIRECT，NVMe 下写延迟 -30%~50%。

**SSTable 文件**：Header → **PAX 混合列式 Data Block**（热列组头部 + 冷列组尾部 + 列偏移量表，点查只读热列切片、跳过约 90% 无用大字段 IO，宽表 QPS 2~3 倍）→ Block Index（稀疏索引 + Zone Map）→ Bloom Filter → Footer。
- 编码：Varint + 差值编码（存储 -30%~50%）；数据块独立压缩，默认 Zstd L3（总量降至原始 20%~30%）；每块 CRC32；
- **Zone Map**：块级 min/max/null 统计，范围条件可跳过 70%~90% 数据块，与布隆互补（布隆管等值、Zone Map 管范围剪枝）；
- 两级索引（阶段 2）：内存摘要 + 按需精索，索引内存 -90%；
- 布隆：分列族独立生成，默认假阳性 1%，7 哈希；分区布隆（1.5）/ Ribbon（3）；
- KV 分离（阶段 3 可选）：SST 只存 Key + 指针，写放大 -50%~80%；
- SSD 原生：数据块 16KB → 4KB（对齐 SSD 页，点查延迟 -60%）。

**Compaction**：MVP 最简合并 → Leveled 分层压实；后台 IO 与读写隔离（ionice 最低优先级 + 限速）；SSD 参数：L0 触发放宽（4→8~12）、层级比例 10×→20×、并行 2~4 线程，写放大 15~25×→6~10×。

**删除与更新**：Tombstone 墓碑（MVCC 二期）；SSD 原生删除位图（1bit + mmap，删除仅写 1bit+fsync 1 页，IO -99%）。

**部分更新（阶段 1.5）**：Base CF + Delta CF 双层，Merge-on-Read；patch 只追加几十字节小记录，写入 IO 放大 10 倍→1 倍，适合 IoT 宽表。

**SSD 原生优化路线（v0.7）**：
- **P0**（1~2 月，写 TPS 22→32 万）：环形大文件 WAL、倒排更新批处理、4KB 块 + 两级索引、倒排分片锁；
- **P1**（3~4 月，→38 万）：倒排 FST + Mmap 字典、元数据-数据解耦；
- **P2**：冷热感知 Compaction、多 SSD 条带化；
- 最终目标：写 TPS 40 万+、写 P95 0.45ms、倒排更新 0.1ms。

---

## 五、索引设计

**三类索引**：

| 索引 | 用途 | 查询方式 |
| --- | --- | --- |
| 主键 | DocId 精确定位 | LSM 有序 + HotCache |
| 组合稀疏索引 | 固定高频条件（`a=? AND b=?`） | 一步定位，免合并 |
| 原生倒排 | 任意字段自由筛选 | DocId 集合交集 / 并集 |

**倒排索引（内存哈希字典 + 追加式倒排文件，读写分离）**：
- 内存 `DashMap<String, TermMeta>`（约 64B/Term），O(1) 定位 `(file_id, offset, length, doc_count)`；1.5 亿数据、500 万 Term 仅 ~320MB；
- 磁盘 Append-Only 文件（`TermData = RoaringBitmap` 压缩 DocId 列表），写入零随机写，点查延迟 5~30ms → 0.5~2ms；
- **统计载荷**：`doc_count` 内嵌 TermMeta，COUNT / GROUP BY 零磁盘（<0.1ms）；
- 追加序 = 时间序，Top-N 最新直接读 Tail；
- **预分片 Chunk**：列表按物理分片切段，广播查询各片只回本片 Bitmap、网关按序直拼 O(1)，广播延迟 15~30ms → 3~8ms、并发 1000 → >10,000 QPS；
- **GC**：超阈值后台重写紧凑文件、原子替换指针；崩溃一致性靠段清单 Manifest（临时文件 → fsync → 原子更新 Manifest → 删旧段）。

**倒排极端防护（原 5.2.4 合并）**：
1. **Super Term 超大列表** → 结果集截断 + 降级全表扫描 + 熔断返回 `Query Too Expensive`；
2. **字典扩容风暴** → 启动预分配 + Hot / Frozen 分段字典（可写 / 只读分离）；
3. **冷启动穿透** → 异步 Checkpoint 快照（恢复 <15s），加载期降级为部分只读；
4. **多重低基数 AND** → 查询优化器选最小集枚举（5s → 0.5ms）；
5. **GC 写放大** → 分层分段 Tiered Segments（L0~L2，最小两段合并）；
6. **OOM 终极防御** → 内存软限流（异步批处理）/ 硬拒绝 507。

**FST + Mmap 字典（阶段 1.5 高优）**：倒排字典改不可变 FST + mmap，冷启动 <50ms、RSS 按需加载（仅热 20% Term 约 1.6GB）；分层 `base.fst` + `delta.fst` 维护；Checkpoint 转为其构建源、两者共存无缝替换。

**倒排 vs 组合选型**：条件固定 → 组合索引；条件任意搭配 → 倒排 + 集合合并。

**TTL 时间分区（阶段 1.5）**：SST 按 `timestamp` 分桶（如按天目录），桶内 Compaction、跨桶不合并；过期**直接删整个目录**，删除成本 O(1)、无墓碑；配置 `ttl_days` / `time_bucket`。

---

## 六、热点缓存 HotCache

- 结构：`DashMap<u64, Document>` + 热度计数 + 内存硬上限；缓存 = 加速副本非权威源，崩溃可穿透磁盘；
- 淘汰：**LFU**（首选）> LRU-LFU 混合 > LRU；禁用固定 TTL；
- 一致性：写 LSM 后主动失效缓存（最终一致）；
- 联动（二期）：文档进 HotCache 时，将其关联倒排 Term 的 Top-N DocId 预载入内存 Term 缓存；
- 关键配置：`max_memory_mb=4096`、`eviction_policy=lfu`、软水位 0.85 / 低水位 0.75、`hot_threshold=5`；
- BlockCache：与 HotCache 隔离；Key = `(SST_File_ID, Block_Offset)` 双重索引，Compaction 后按 FileID 失效，根治缓存污染（命中率骤降 20~30%）；
- 内存预算示例（64GB 机）：HotCache 8GB + BlockCache 4GB + TermCache 2GB，缓存总和 < 可用内存 × 0.7。

---

## 七、查询执行路径

| 场景 | 路径 | 目标延迟 |
| --- | --- | --- |
| 热点主键 | HotCache 命中直返 | 亚毫秒 |
| 冷主键 | 稀疏索引 → 布隆 → Zone Map 粗筛 → 块缓存 / 磁盘 | 0.1~1ms |
| 固定条件 | 组合稀疏索引一步定位 | ≈MySQL |
| 任意字段 | 内存 Hash → pread Bitmap → 交并 → Zone Map 粗筛 → 取文档 | 单机 P95 <1.5ms |
| 范围 | Zone Map 剪枝 → 命中块顺序扫描 | 优 |

**查询优化器 Lite**：主键点查禁倒排 → 组合索引前缀匹配 → 倒排 + Zone Map 混合（先剪枝再取倒排）→ 大列表代价估算、跳过倒排走全表扫描；可配置开关。

**聚合执行器（阶段 1.5）**：COUNT 直接读 `TermMeta.doc_count`（<0.1ms）；GROUP BY 遍历字段倒排 Term 取各 doc_count 构造结果，零磁盘访问。

**倒排回表优化（三层）**：① Read Reorder 按 (FileID, BlockID) 排序分组合并读请求（IOPS -60%）；② 块内异步批量预取（NVMe QD=128，5~15ms → 2~4ms）；③ Early Termination 按 DocId 序截断 LIMIT 再回表（延迟 -99%）。

---

## 八、备份与还原

**MVP：冷备份 / 全量还原**。暂停写 → 刷盘 WAL + MemTable → 触发倒排字典 Checkpoint → 打包全部 SST + 倒排文件 + 字典快照 + 段清单 Manifest + 元数据。**必含**：主数据 / 组合索引 SST、倒排 `.inv`、`manifest.json`、字典 Checkpoint/FST、`fields.idx`（漏倒排则备份后倒排全丢）。还原：停止服务 → 清库 → 解压 → 校验完整性与版本兼容 → 重启。

**二期（砍掉）**：热备份、增量 / 定时备份、加密、CSV 批量导入导出、MySQL 迁移工具。注意区分：备份还原 = 本地数据安全；云边同步 = 边缘 → 云端数据流转，分开设计。

---

## 九、分布式设计（阶段 2）

**边界：做"集群"，不做"分布式数据库"**。分片即单机（复用单机内核）；网关无状态（仅 DocId 路由转发 + 结果合并 + 熔断限流）；**不做**分布式事务(2PC)、全局锁、跨片 JOIN、全局快照读——由业务层（MySQL / Redis）承担。承诺：8 节点总吞吐 200 万 TPS、延迟 ~1ms。

**分片路由**：分片键固定 DocId；一致性哈希（1024 虚拟分片 → 物理机）；写 / 主键查单分片无广播。**无损扩容三步协议**：双写（Shadow Writes）→ Delta 追平 → 1 秒原子切换（含 5 秒回滚预案）。

**两类查询分流**：路由查询（带分片键）≈单机；广播查询（倒排检索）网关下发给全部分片、只合并 DocId 集合，配合预分片 Chunk 直拼，二次路由取文档。分片内独立维护本地索引。

**高可用**：一主多从异步复制（默认 `async` 高性能，`sync` 强一致可选），主宕由元数据中心触发切换。

**性能目标**（16 核 / 64G / NVMe / 1.5 亿文档基准）：单条写入 0.9ms P95 / 22 万 TPS；热点主键 0.15ms / 85 万 QPS；倒排单 Term 1.8ms / 4 万 QPS；聚合 0.08ms / >100 万 QPS；广播 <8ms / >10,000 QPS。

**容量**：单节点 1.5 亿 / 300GB；8 节点 ≈12 亿、32 节点 ≈48 亿（50 亿级）。

**业务约束**：优先带 DocId；广播检索勿放核心路径；运营分析强制带时间范围防熔断（`Query Too Expensive`）。

**分布式进阶**：网关全局 Term 缓存（Key = 分片 ID + Term → 压缩 Bitmap，写计数心跳 + 短 TTL 双保险失效，广播查询缓存直出）；术语字典热备 TDS（重启向轻量字典服务拉取快照，10~12s 恢复，预推送 + 蓝绿切换 0 抖动）。

---

## 十、开发路线图

**阶段 1（1 个月）单机 MVP**：Rust 内核（musl 静态兼容 Nova OS）、LSM（WAL + 跳表 + SST）、CRUD + 范围查询、字段注册表持久化、Varint + Delta 编码、布隆、块级 Zone Map、列族、倒排基础（内存字典 + Append + RoaringBitmap）、块缓存、缓存配置模型、查询优化器 Lite、OOM 看门狗子集、墓碑删除、冷备份还原、TCP / HTTP / CLI。**砍掉**：集群 / 复杂 SQL / 大事务 / 运维面板 / 云边同步。

**阶段 1.5 列式优化**：PAX 列式块、TTL 时间分区（必做）、部分更新 Delta CF（必做）、倒排统计载荷（必做）、FST + Mmap 字典、迁移工具基础版、Zstd + 分区布隆。收益：读放大 -90%、单机 QPS 2~3 倍。

**阶段 2 分布式**：倒排架构升级、分布式全配置、网关 + 元数据中心、广播检索 + 平滑扩容 + 主从 HA、看门狗补全、网关 Term 缓存、TDS、两级索引、Redis 外部缓存。

**阶段 3 深度优化**：Leveled Compaction 完善、io_uring + 环形 WAL、IO 优先级调度、配置热加载、位图索引 / MVCC、热备份 / 增量 / 导入导出、KV 分离 + Ribbon、存算分离（远期）。

**Nova OS 协同**：开发期跑 x86 Linux 调试，MVP 后交叉编译 musl 部署联调。

---

## 十一、风险与原则

1. 核心存储引擎代码不全文交给 AI 生成，并发 / 锁 / 内存 / 文件 IO 逻辑人工逐行审查；
2. 1 个月产出是演示原型，商用还需 2~6 个月稳定性打磨；
3. 先正确后性能：MVP 跑通写入-查询-备份-还原全链路后再逐项优化，每次压测对比。

---

## 十二、配置体系与稳定性（原 13 + 14 合并）

**配置加载**：动态热加载（二期 SIGHUP）、启动校验（缓存总和 < 内存×0.7，否则告警降级）、环境变量覆盖、默认安全值出厂即用。

**线程模型（单进程多线程）**：IO / 查询主池（tokio 协程收包）→ 倒排 / 计算池（rayon / spawn_blocking，Semaphore 限流）→ 后台 IO 池（std::thread 2~4）。**协程收网络包、线程干重活**。限流：协程数超限返 429；计算池队列满降级全表扫描防雪崩。分片节点独立进程复用单机内核，唯一新增分布式内存组件在网关层。

**多核分配（阶段 3，防超售）**：网络协程池 N×0.5、计算池 N×0.3、后台 IO 池 2~4、系统预留 ~20%；CPU 绑核可选（默认关）；场景化：极致写 12/2/2、极致查 6/8/2、混合 8/6/2、4 核边缘盒 2/1/1。

**内存看门狗（OOM Guardian）**：每 100ms 查 RSS——>0.85 软限流 sleep、>1.0 硬限流返 503。多层回收：缓存 85% 软水位实时驱逐 → MemTable 冻结刷盘 → 倒排字典 90% 异步批处理 / 100% 返 507 → 紧急时缓存缩容 30% + jemalloc `mallctl purge` 归还 RSS。分配器默认 **mimalloc**（高并发必须，musl 默认 malloc 全局锁是瓶颈），jemalloc 可选。

**写停滞看门狗（阶段 2）**：L0 数量 60s 不减少判定 Compaction 假死 → 强制中断重启；连续 3 次失败主动关进程由外部拉起。

**慢查询看门狗**：查询带 deadline（CancellationToken 超时退出）+ Semaphore 并发限流，复用 `broadcast_query` 配置。

**Sidecar 探针（阶段 2）**：轻量子进程 5s ping，连续 3 次无响应 SIGKILL 重启主进程。⚠️ 禁止在 tokio 运行时初始化后 fork，用 `std::process::Command` 启动独立探针。

**自愈闭环**：预防（内存看门狗）→ 检测（写停滞 / 慢查询）→ 自愈（中断重启 / Sidecar）→ 降级（超时限流）。

---

## 十三、迁移与兼容（原 15 合并）

**绝不承诺 MySQL 协议 / 语法兼容**（需 1 年投入 + 性能劣化 + ACID 语义灾难），MySQL 仅作数据迁移来源。三层迁移：① `shanshui-cunji-migrate` 数据迁移工具（mysqldump / JDBC，行 → 文档）；② 类 SQL 查询语法（`SELECT ... WHERE AND/OR` 子集）；③ 客户端 SDK 抽象层（Java / Python / Go，类似 JDBC 的 `query()` / `execute()`，用户仅换 DataSource 与连接串）。官方措辞：提供一键迁移工具 + 类 SQL 支持，业务代码仅改底层驱动。收益：10 倍写性能 + 1/10 硬件成本。

---

## 十四、性能诊断（原 17 + 附录 D 合并）

**瓶颈矩阵**：写密集 → 磁盘 fsync IOPS（横向扩容线性提升）；热点主键高并发 → 网络带宽 / 序列化（单节点受网卡限制）；复杂倒排 AND → CPU 位图交集 + 回表随机读（靠缓存与剪枝）；广播查询 → 网关 CPU / 带宽（需网关集群）；数据超 2 亿/节点 → Compaction 写放大（强行分片）。

**常见瓶颈与调优映射**：写慢 → `l0_stall_threshold`↑；fsync 慢 → 组提交 / io_uring；内存紧 → 缓存上限↓（0.7 校验）；倒排慢 → FST 引擎 + 门控↓；聚合慢 → `doc_count` 统计载荷；广播超时 → `max_concurrent` + Term 缓存；磁盘满 → TTL 分区；冷启动慢 → FST / Checkpoint。

**隐蔽陷阱（附录 D 节选）**：回表随机读放大 → 排序分组预取截断；网卡微突发 → 漏桶限流 800MB/s；Compaction 缓存污染 → FileID 失效；WAL unlink 锁 → 延迟删除；Mmap 缺页风暴 → 后台 mlock / readahead 预热；倒排 Term 锁竞争 → 分片细粒度锁。

---

## 十五、关联查询策略（原 19 合并）

**不做 JOIN**（LSM 随机读代价大 + 分布式 Shuffle + 吃 50% CPU），用 4 种替代覆盖 95% 需求：① 应用层二次查询 + 内存合并（SDK `queryAndJoin`）；② 写入时预连接 Enrich 展开；③ 物化视图后台预聚合；④ 导出 OLAP（Parquet / CSV → ClickHouse，存储计算分离）。JOIN 计划节点：左小先查左、右小反向、都大拒绝熔断（`join.max_rows` 100 万）；跨分片 JOIN 禁止；小表广播 JOIN 阶段 3 可选。

---

## 十六、运维管理接口（原 20 合并）

对标 MySQL 运维习惯，选择性支持：**SHOW PROCESSLIST**（QueryRegistry 注册 + `KILL QUERY` 熔断）、**SHOW STATUS**（内存 / 缓存命中 / L0 文件数 / 吞吐）、**EXPLAIN 适配版**（索引选择 / 扫描行数 / Zone Map 剪枝预期，只推演不执行）、**数据导出** `shanshui-cunji-export`（Parquet / CSV / JSON，流式管道内存恒定、增量导出 checkpoint 游标、ClickHouse 直读零代码）、**数据导入** `shanshui-cunji-import`（导入字段注册表 + 索引定义，不强制约束）。

---

## 十七、外部缓存集成 Redis（原 21 合并）

定位：Redis 作 **L2 分布式热点读缓存**（读加速器，非数据源）。红线：所有写入先落盘返 ACK 再操作 Redis；仅单向 shanshui-cunji → Redis；禁只写 Redis、禁过期反向回写、禁从 Redis 恢复。流程：读 Cache-Aside（未命中查库异步回填）；写先写库成功再 DEL 旧缓存。高可用：熔断降级透传、防击穿互斥锁 + stale-while-revalidate、防雪崩 TTL 随机抖动。落地阶段 2 起，MVP 进程内 HotCache 已够。

---

## 十八、可测试性（原 18 合并）

核心模块依赖注入 + trait 抽象（FileSystem / Clock / Allocator），测试用内存 / Mock 实现；并发竞态用 **loom** 确定性模型；时间可注入 MockClock。测试金字塔：单元 70% + 集成 15% + 混沌 5%（kill -9、网络分区）。验收：单测不依赖外部环境、并发模块 100% loom 通过、CI 单测 <60s。

---

## 十九、前沿演进（原 22 合并）

竞争力在"组合"（LSM 写入 + 倒排检索 + 文档模型 + 缓存 / 配置 / 运维体系）。四大趋势：**RDMA / CXL**（长期，节点通信 / 共享内存池）、**PMEM**（中期实验，WAL 零延迟持久化）、**GPU / IAA 异构计算**（中期评估，卸载 Compaction / 扫描）、**AI 学习型索引**（长期，Learned Index / 热点预测）。短期立足务实优化（TTL / Partial Update / FST / Admin），不追新硬件。

---

## 二十、设计决策归档（原 23 合并，已定不再评估）

**不立项**：桶 / 分区 LSM、2PC / TCC / Seata（L1 outbox + L2 SAGA 已覆盖）、两级索引（Zone Map 已够）、Calvin 硬件卸载、写路径后台 ack、B+Tree 外置索引（范围瓶颈在使用面）。
**已落地**：无锁合并根治、SSD 原生（Ex-5）、多核（Ex-7）、10 亿库扩展蓝图（docid_alloc / shard_build / shard_inverted / raft_rpc / shard_metrics）。
**采纳候选**：并行 Flush、段级 seq 剪枝、删除密度、posting LRU、回表共享块缓存（Ex-8.5~8.8）。
**待确认**：存储格式 vs design 3.4 差异。
