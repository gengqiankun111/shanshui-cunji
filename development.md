# 山水存迹数据库（shanshui-cunji）开发实现汇总

> 整合自 development_0.md + development_1.md。两份文档记录的所有开发任务**均已完成 ✅**，本文件按功能域归并同类/相关内容并精简，不再保留逐项开发流水与 commit 明细；原文件存档保留。

---

## 一、文档定位与开发原则

- **分工**：design.md 回答"为什么"（架构/算法/权衡）；development 回答"怎么落地"（模块/接口/格式/任务）。
- **开发原则**：① 先正确后性能——MVP 跑通「写入→查询→备份→还原」全链路后再叠加优化；② 并发/锁/内存/文件 IO 逻辑人工逐行审查，不依赖 AI 生成核心引擎；③ 磁盘格式向后兼容——SST/WAL/备份包/字段注册表/倒排段清单破坏性变更必须有迁移路径；④ 每项优化落地必须附压测对比。
- **红线**：写路径（WAL 刷盘/双 MemTable/写 Stall）改动必过崩溃恢复测试；严禁在 tokio 运行时内 `fork()`（用 `std::process::Command` 起独立 Sidecar 探针）。

---

## 二、技术栈与工程结构

**语言与依赖**：Rust（2021 edition，musl 静态交叉编译）。核心依赖：tokio（异步/写 Stall 限流）、dashmap（并发字典）、parking_lot、bytes、crc32fast、xxhash/murmur3（布隆/分片）、serde+toml、clap、axum/hyper（HTTP）、mimalloc（默认全局分配器）、tikv-jemallocator（可选 stats+purge）、fst+memmap2（倒排字典）、governor（令牌桶限流）；开发依赖 proptest/loom/tempfile/mockall/dhat/criterion；阶段 3 引入 io-uring。类 SQL 最终采用**零依赖递归下降解析**（sqlish），未用 sqlparser-rs。

**工程结构**：
```
src/
├── config / error / keys / value / traits(FileSystem/Clock/Allocator) / testing
├── schema/registry.rs      # 字段注册表持久化
├── engine/  wal·memtable·sstable·block·bloom·zonemap·compaction·column_family·mv_scheduler
├── index/   cidx·inverted
├── cache/   hotcache·blockcache·termcache·external/redis_manager
├── query/   executor·optimizer·context·filter·sql(sqlish)·join·explain·agg
├── watchdog/ oom·stall·query_guard·sidecar + disk_space·cpu_guardian
├── storage / admin(registry·status) / server / cli
└── 新增：seqlock·per_cpu·io_queue·affinity·io_scheduler·outbox·saga·raft_meta·raft_rpc·
          scale_out·term_cache·tds·reshard·mv·sharding·docid_alloc·shard_build·
          shard_inverted·shard_metrics·indexer_proxy·bitmap·external_cache·mysql(wire)·
          export_pipeline·import_schema·metrics
crates/  mmap-file（只读 mmap 安全封装）· io-uring-file · disk-space   # unsafe 白名单独立 crate
tools/   shanshui-cunji-migrate · shanshui-cunji-export · shanshui-cunji-import
```
**物理目录**：`data/{cf_data, cf_cidx, cf_inv, cf_delta}`（列族隔离 + 时间桶子目录）、`metadata/`（fields.idx 等）、`snapshots/`（字典 Checkpoint/FST）。依赖方向：engine/index/cache 为内核层，不反向依赖 server/cli/query。

---

## 三、数据格式约定（兼容性契约）

| 项 | 格式 |
|---|---|
| 主键 | `DocId` u64 大端定长（字节序==数值序，支撑范围扫描/Zone Map 剪枝） |
| 组合索引 | `VarLen(field1..) ++ DocId(u64)`，值空 |
| 倒排 | 独立内存字典 + 追加式倒排文件（不走 LSM 键），`TermData = Term ++ RoaringBitmap` |
| 文档值 | `FieldCount(u32) ++ Field{FieldID(u16) ++ TypeTag(u8) ++ Payload}`；TypeTag=null/bool/i64/f64/utf8/bytes/timestamp |
| 字段注册表 | 字段名↔u16 持久化到 `metadata/fields.idx`（WAL 风格 + checkpoint 压缩），**反序列化基石、最早实现**，自动扩展支持 Schema 演进 |
| WAL | `Magic ++ Len ++ Payload ++ CRC32`；组提交；旧文件延迟删除（重命名+后台异步 unlink，防 GB 级 unlink 阻塞） |
| SSTable | Header → Data Block（PAX 混合列组，热列组头部+冷列组尾部+列偏移量表）→ Block Index（稀疏+Zone Map）→ Bloom → Footer；Varint+差值编码、Zstd L3 块级压缩、每块 CRC32、两级索引（L1 常驻摘要+L2 懒加载） |
| 倒排段清单 | 版本化 Segment Manifest，GC 用「临时文件→fsync→原子 rename Manifest→删旧段」，崩溃无半成品 |

**关键演进口径**：数据块 16KB→4KB（SSD 原生，7.24）；倒排段格式 v2→v3（posting 分块延迟加载，7.74）→v4（doc_count 载荷，Ex-9.1）；段文件头带版本、旧段兼容读取。

---

## 四、单机存储引擎

**写入路径**：WAL（组提交/环形模式）→ MemTable（跳表，双缓冲切换，刷盘不阻塞写）→ SSTable → Compaction。写 Stall（L0 阈值默认 12）Semaphore 限流防内存爆炸。

**关键演进（按主题归并）**：
- **组提交（M8-P0，7.8）**：YCSB 实证"fsync 逐条串行"为写路径头号瓶颈（55×）→ 窗口攒批统一落盘，2ms 窗口写吞吐 2,003→91,296 ops/s（45×）；读写分离"读被写拖垮"随之解决（B 负载 7.9×）。
- **环形 WAL**：预分配单文件+环形指针+覆盖安全（未刷盘拒覆盖）；M8-P12 头部 tail 与记录区合并单次 fsync（ring 吞吐 2.3×）；Ex-5.5 规模化（默认 256MB，SSD 磨损天然均衡）。默认推荐 append+组提交。
- **WAL 截断（M8-P5）**：flush 后回收 append WAL 防无限增长（6.5GB→小文件）；增量备份只导出未刷盘记录，缺口检测提示全量。
- **批量导入模式（M8-P6）**：`set_bulk_import` 跳过 HotCache 回填，50M 导入 WS 4.9GB→~700MB、稳定 61.7k 行/s。
- **Compaction**：MVP 简单合并 → Leveled 分层压实；**无锁合并（7.65）**根治"合并阻塞写"（CF Arc 化 + sst_mutate 互斥 + 内存快照 manifest，修复 P73 悬空引用竞态；1 亿库合并期写 25-43k rows/s 不塌陷）；`compact_input_max_mb` 分批（阻塞 -55%）；并行压实多列族（2.14×）；动态限流（写压力代理，7.41）；冷热感知优先合并热段（7.36）；无重叠段**数据块级复用**只重建元数据（7.35）；多列族并行（7.31）。
- **删除语义**：Tombstone 墓碑 → **删除位图**（Ex-5.6，1bit/4KB 页对齐/mmap，删除 IO -99%、不污染 LSM；关闭时回退墓碑 MVCC）。
- **部分更新**：Base CF + Delta CF，Merge-on-Read，写放大 10×→1×。
- **扫描层（7.99/7.100）**：块组预读 4→8 + merge 线性化；并行解压实测负收益已回退；`COUNT(*)` key-only 免值计数快路径。
- **大结果集安全**：倒排分页（limit/offset/total）、scan 流式化 k-way merge（内存 O(page)）、游标续扫 after（全库遍历每页 O(limit)）。

---

## 五、索引体系

**三类索引**：主键（LSM 有序+HotCache）、组合稀疏索引（固定条件一步定位）、原生倒排（任意字段，DocId 集合交并）。

**倒排索引（内存字典+追加式文件，读写分离）**：
- 内存 `DashMap<String, TermMeta>`（含 file_id/offset/length/doc_count，64B/Term）O(1) 定位；磁盘 Append-Only 文件（RoaringBitmap 压缩）；写入零随机写、点查 0.5~2ms；`doc_count` 统计载荷支撑 COUNT/GROUP BY 零磁盘（<0.1ms）；追加序=时间序 Top-N 直读尾部。
- **FST+mmap 字典（7.34）**：不可变 FST+mmap 按需加载（冷启动 <50ms、RSS 与访问量成正比）；crates/mmap-file 封装 unsafe 白名单；base.fst+delta.fst 分层维护，与 Checkpoint 共存衔接。
- **段 GC（7.73 后台化）**：后台线程周期触发（100ms 轮询+10min 兜底），flush/gc 经 mutate 锁串行防 Manifest 丢更新，查询持旧段快照 open NotFound 跳过返回空（数据已并入新段）。
- **posting 分块 v3（7.74）**：段格式 v3 容器级分块 + 惰性游标 k-way merge——近页解码 x211、COUNT x4491、全量持平、体积 +0.1%。
- **防字典膨胀（7.11）**：`inverted_fields` 白名单 / `exclude_fields` 黑名单 / `max_term_len=96`（超长自动跳过）——100 字段全建倒排 246MB vs 白名单 3.5MB（字典压缩 45 万倍、写放大 27.7×），"倒排字段 ≤20"经验准则。
- **位图白名单（7.6）**：`bitmap_fields` 枚举字段内存位图加速 COUNT/GROUP/AND（亚毫秒）。
- **fulltext 分词（7.14/7.17/7.21）**：长文本分词建词 term（`ft:{field}:{token}`，与 inverted 正交）；中文 bigram → jieba 完整词典分词（`cjk_segmenter` 可配，默认开启、可回退）。
- **并发与写入（7.25/7.26）**：256 分片锁（同 Term 串行/不同 Term 并行，1.39×）；`add_batch` 攒批合并（1.7×）；倒排查询入口统一先 flush pending 保一致性。
- **预分片 Chunk（7.3）**：列表按分片切段，广播查询各片只回本片 Bitmap、网关按序直拼 O(1)。

**TTL 时间分区**：SST 按 timestamp 分桶（按天目录），桶内 Compaction、跨桶不合并，过期整目录删除（O(1)、无墓碑）。

---

## 六、缓存与内存管理

- **HotCache**：DashMap<DocId, Document> + 热度计数 + 内存硬上限；LFU（首选）淘汰；写失效链（先失效缓存再写 LSM，最终一致）；大文档不缓存。
- **内部锁粒度化（7.72）**：整包 Mutex → RwLock(缓存区)+DashMap(访问计数)——纯读 x4.16、混合负载 x5.42（读不再被写拖垮）。
- **热点保护区（M6-4）**：容量=主缓存 1/5，访问达阈值自动晋升、普通淘汰避让。
- **批量导入跳过回填（7.13）**：修 stats 泄漏/used_bytes 虚增/LFU O(N) 风暴（7.15，软水位渐进淘汰+LFU 采样近似 O(64)）。
- **BlockCache**：LRU，Key=(SST_File_ID, Block_Offset) 双重索引，Compaction 后按 FileID 失效防缓存污染。
- **倒排 ArcSwap 并发读（7.42）**：段清单与 FST 字典原子发布，读路径拿 Arc 快照无锁。
- **内存预算**：HotCache 8GB + BlockCache 4GB + TermCache 2GB（64GB 机示例），缓存总和 < 可用内存×0.7。

---

## 七、查询执行与类 SQL

**执行路径**：热点主键（亚毫秒）→ 冷主键（稀疏索引→布隆→Zone Map 粗筛→块缓存/磁盘）→ 固定条件（组合索引一步定位）→ 任意字段（倒排交并）→ 范围（Zone Map 剪枝+顺序扫描）。优化器 Lite：主键点查禁倒排 → 组合前缀 → 倒排+Zone Map（先剪枝再取倒排）→ 大列表代价估算跳过倒排走全表扫描。

**聚合**：COUNT 直读 `doc_count`（<0.1ms）；GROUP BY 遍历倒排 Term 取各 doc_count；SUM/AVG 二期。

**回表优化三层**：Read Reorder（按 FileID/BlockID 排序分组，IOPS -60%）+ 块内异步批量预取 + Early Termination（LIMIT 提前截断，延迟 -99%）。

**类 SQL（sqlish，零依赖递归下降）**：
- WHERE 子集：`= != > < >= <= BETWEEN AND OR NOT 括号`，显式拒绝 JOIN/GROUP BY/子查询/事务。
- **能力补全（AF 系列，7.101~7.105）**：ORDER BY（单/多字段+ASC/DESC+LIMIT，排序键 Null<Num<Str 对齐 MySQL）→ GROUP BY（单字段 COUNT/SUM → 多字段+AVG/MIN/MAX）→ HAVING（聚合列/分组字段左项）。排序/分组上限 20 万行，超限 QueryTooExpensive。
- **聚合函数（7.95）**：COUNT/SUM/AVG/MIN/MAX + WHERE 字段条件，单遍扫描。
- **扫描期轻量字段提取（7.96/7.97）**：手写 JSON 顶层字段字节级扫描（跳过整文档反序列化）——无索引全扫 12.6s→4.1s（3×），复合表达式短路逐字段扫描。
- **谓词下推（7.93/7.94）**：裸比较/BETWEEN 单遍流式扫描+LIMIT 早停（`amount>90000 LIMIT 10` 17.2s 熔断→0.6ms）；数字等值回退扫描（不把空 posting 当 0 行）；倒排落盘保证重启后字段等值可见；修复 BETWEEN 误当 docid 窗口 bug。
- **MySQL 语义收敛（14.3）**：RR 一致性 rr-cases C1~C9 全部 PASS（快照+当前读、自写可见、防幻读等；修复位图删除非版本化导致的幻读）。

---

## 八、备份与数据管道

- **备份/还原**：冷备份（暂停写→刷盘 WAL+MemTable→倒排字典 Checkpoint→打包 SST+倒排文件+Manifest+字段注册表+快照），版本标记+完整性校验；**增量备份**（按 seq 导出 WAL 未刷盘记录，缺口检测提示全量）。
- **迁移工具 `shanshui-cunji-migrate`**：mysqldump/CSV/Parquet 全量导入 + docid 游标增量断点续传（checkpoint 原子写）。
- **导入 `shanshui-cunji-import`**：CSV/JSONL/Parquet，`--id-field`/`--timestamp-format`/`--batch-size`；`import-schema` 预注册字段+索引定义+倒排白名单（减少写放大）；Parquet 批量导入（每 50 万统一 fsync）。
- **导出 `shanshui-cunji-export`**（design 20.5 全落地）：CSV（RFC 4180）/Parquet/JSONL；`--incremental --checkpoint` docid 游标续传；**流式管道**（Filter/Projection/脱敏/Sink 分叉，内存恒定，无过滤零 JSON 解析）；**JDBC 直连** MySQL（wire 客户端建表+批量 INSERT）；`--mysql-compatible` 配套 LOAD DATA SQL（快 ~20 倍）；`--dry-run-schema` 生成 ClickHouse MergeTree / MySQL 建表 DDL；后台 IO 优先级与 Compaction 同策略 + `--rate-limit`。

---

## 九、分布式与集群

**架构边界（做"集群"不做"分布式数据库"）**：分片即单机（复用内核）；网关无状态（仅 DocId 路由/结果合并/熔断限流）；不做跨分片事务/JOIN/全局快照读，由业务层承担。

**组件**：
- **分片路由**：DocId 一致性哈希（1024 虚拟分片）；写/主键查单分片无广播；平滑扩容迁移量 ≈1/N 验证。
- **复制**：一主多从 async（默认）/sync（等 Slave ACK）可配；分片节点 `RpcServer/RpcClient`（JSON-over-TCP）+ `register_shard_handlers` 复用单机内核。
- **元数据中心 + 网关**：节点注册/摘除自动重建哈希映射；三类转发（写/点查路由、广播检索预分片 Chunk 直拼、健康探活）；端到端两节点强一致验证。
- **网关全局 Term 缓存（9.9）**：Key=(节点,Term)→压缩 Bitmap，命中直出 + 短 TTL 兜底 + 写计数失效。
- **术语字典热备 TDS**：字典快照文件写穿持久化 + RPC 拉回（重启免磁盘重建，降级回退本地）。
- **扩容编排（7.76）**：`ScaleOutCoordinator` 状态机（ADDING→CATCH_UP→DRAIN→SWITCH→DONE/ROLLBACK），outbox 增量追平 + 排空校验（未排空禁切换）+ 崩溃恢复续跑。
- **元数据高可用 Raft（7.77/7.85/7.88）**：最小 Raft 管理 MetaCenter master 角色——多数派选举、日志复制、心跳超时自动 failover、脑裂安全（少数派不能选主）；raft 消息 TCP 传输（复用 rpc 帧，修复 send 锁重入死锁/join 卡死/选举无冷却三处挂起）；`RouteChannel` 让扩容路由副作用走 Raft 日志（7.98）。

**分布式事务（自 design_extension，L1/L2/L3）**：
- **L1 outbox 本地消息表+幂等消费**（7.43）：业务写与消息表共享全局 seq 原子落盘，投递/排空/幂等去重。
- **L2 SAGA 编排+补偿状态机**（7.44/7.57/7.58）：正向步骤/反向补偿、超时屏障空转、悬挂防护、补偿幂等、持久化崩溃续跑；网关 HTTP API（`/saga/start|status|compensate`）、拓扑并行（Kahn 分层）+ 后台对账指数退避重试。
- **L3 Calvin 确定性事务**（7.45）：评估结论**不进入 kernel**（单 docid 天然不分片、outbox+SAGA 已覆盖、读写集难预声明），远期触发条件记录。

**其它评估结论**：读写分离暂缓（组提交已解决读被写拖垮，剩余收益 <20%，7.49）；Tiered 分层合并不引入（写放大恒≈log2N 不优于 Leveled，7.78）；两级索引不引入（内存是 Zone Map 的 7812× 而过滤收益 0，7.80）；Calvin 硬件卸载无必要（gseq 2 亿/s 远超目标 2+ 数量级，7.80）；Indexer Node 查询代理层落地（独立倒排索引 + 回表抽象，COUNT 不触数据节点，7.79）。

---

## 十、10 亿级扩展（AD 阶段 A~D）

- **全局 docid 分配器（7.81）**：`docid = shard_id<<40 | local_id`——路由 O(1) 高位直取、跨分片天然唯一、无集中瓶颈（每分片 AtomicU64）、扩容归属不变；`ShardLocalAllocator` 无锁原子分配 + 水位崩溃续跑。
- **分片构建工具（7.82）**：`shard_build` 把大源文件构建为 N 分片目录（无主键均匀分布/显式主键前缀路由/崩溃续跑/`--shard-id` 多进程并行），修复 UTF-8 BOM 首行解析。
- **分片化倒排（7.83）**：倒排存分片内 local_id（适配 Roaring 32 位），跨分片前缀组合成全局 docid；跨分片惰性分页窗口 O(窗口)、精确 COUNT。
- **raft RPC（7.85）**：`RaftTransport` trait + `RaftNodeRuntime`（选举/日志复制/failover/多数派存活），确定性驱动模拟随机超时。
- **分片级可观测（7.86）**：ShardMetricsRegistry（水位/读写计数/Prometheus 渲染 + 水位 80% Warn/90% Critical 预警）。

---

## 十一、MySQL 生态兼容

- **wire 协议（H-1~H-6，7.55/7.56）**：MySQL wire server（握手 HandshakeV10 + mysql_native_password + COM_QUERY 分发），库 `scc`/表 `documents`（id BIGINT + doc JSON）；SHOW/SELECT/INSERT/UPDATE/DELETE/事务（BEGIN/COMMIT/ROLLBACK→RR 快照+写写冲突检测，同事务写后读可见）/预处理语句（COM_STMT_PREPARE/EXECUTE）；mysql cli 8.0 与 pymysql 真实连接全链路；sysbench 接入。
- **结果集裁剪（7.89~7.92）**：列投影（`SELECT id` 不再整 doc 回包，点查差距 1.28×→1.10×）→ 字段级投影（`SELECT status, city`）→ 列类型推断（LONGLONG/DOUBLE/VAR_STRING 按整列实际值）→ 嵌套字段投影（`SELECT a.b` 点路径下钻 + 大小写容错 + NULL 0xfb 表达）。
- **WHERE 下推与过滤（7.93~7.95）**：字段条件过滤下推 + LIMIT 早停；数字等值回退扫描；聚合函数（COUNT/SUM/AVG/MIN/MAX + WHERE）。
- **DML（14.1）**：batch_insert/UPDATE/DELETE … WHERE id IN / f IN / 字段条件（非事务+事务内），主键重复 1062 预校验无部分写入。
- **高并发（7.69/7.70）**：预处理读语句走读锁并行 + 连接线程小栈 512KB（10k 连接内存 5GB vs 20GB+）；**异步协程运行时**（tokio，idle 连接不占线程——500 idle 连接仅 15 线程，10k 长连接可行）。
- **基准（7.87，vs MySQL 8.0 buffer pool 8G）**：插入 82,341 vs 53,836 rows/s（反超 1.53×）；点查 0.91~0.96×（协议往返固定开销差距）；范围 3.5×（JSON 文档反序列化 vs 二进制列，文档型语义差异）。

---

## 十二、SSD 原生与性能优化（Ex-5，v0.6 起 SSD Only）

| 项 | 内容 | 收益 |
|---|---|---|
| 4KB 数据块（7.24） | block_size 16→4KB + 两级索引粒度联动（index_granularity 16→64 防内存膨胀） | 点查读放大 16×→4× |
| 倒排分片锁（7.25） | mem/bitmaps 按 Hash 256 分片锁 | 高并发锁竞争 1.39× |
| 倒排更新批处理（7.26） | `add_batch` 攒批合并 | 写入倒排更新 1.7× |
| Compaction 调优（7.31） | l0 阈值 8→12、并行压实多列族 | 写放大 15~25×→6~10× |
| 环形大文件 WAL（7.32） | 默认 256MB、磨损均衡 | 省文件切换/均匀磨损 |
| 删除位图（7.33） | 1bit/4KB 页对齐/mmap | 删除 IO -99%、无墓碑污染 |
| FST+mmap 字典（7.34） | crates/mmap-file unsafe 白名单 | 冷启动零堆分配、按需加载 |
| 元数据-数据解耦（7.35） | 无重叠段数据块级复用（零解压重压） | 合并写放大降低 |
| 冷热感知 Compaction（7.36） | 热段优先下沉 + Bloom Merge 由无重叠检测+分区布隆承担 | 热点读路径段数更快减少 |
| 多 SSD 条带化（7.37） | wal_dir/sst_dir/inverted_dir 分盘 | 多盘消除 IO 争用 |

**性能定位决策**：io_uring 保持默认关（2 核小机器 SQPOLL 负收益 -13%，7.71）；多核 NVMe 生产环境开启 + 预留 SQPOLL 核（7.63 指引）。

---

## 十三、并发与多核优化（Ex-6/7）

- **三池隔离模型**：IO/查询主池（tokio 协程收包）→ 倒排/计算池（rayon/spawn_blocking，Semaphore 限流）→ 后台 IO 池（std::thread 2~4）；"协程收网络包、线程干重活"；协程超限 429、计算池满降级全表扫描防雪崩。
- **多核分配（防超售）**：网络 N×0.5 / 计算 N×0.3 / IO 2~4 / 系统预留 20%；`cpu_cores_total=0` 自动检测。
- **Seqlock 原语（7.28）**：零 unsafe 方案（AtomicU64 版本号+RwLock），写不阻塞读、读重试率 0.015%。
- **PerCpuCounter（7.38）**：`#[repr(align(64))]` 缓存行隔离，8 线程写 2.1×。
- **CPU 绑核（7.39）**：`[affinity]` 默认开启自动分区（网络/计算/IO 核），跳过超线程。
- **io_uring 队列抽象（7.40）**：IoClass→队列号 0/1/2（与多盘条带对齐），io_uring 本体 unsafe 独立 crate 封装。
- **Compaction 动态限流（7.41）**：MemTable 水位=写压力代理，压力 1 让路 50% 磁盘带宽。
- **读路径无锁化（O 项，7.65 等）**：`&self` 化 → RwLock 读读并行 → ssts ArcSwap 后台合并 → 无锁合并；1 亿库 read_only 42→561 TPS（+13.3×）、read_write 29.5→230（+7.8×）。
- **HotCache 读读并行（7.72）**：见第六章，纯读 x4.16。

---

## 十四、稳定性与运维

- **看门狗四层**：OOM Guardian（RSS 0.85 软限流/1.0 硬限流 503，紧急止损：刷盘降级+缓存缩容 30%+jemalloc purge 归还 RSS）；写停滞（L0 60s 不减少判 Compaction 假死→中断重置，连续 3 次主动退出）；慢查询 SLA（deadline+CancellationToken+Semaphore）；Sidecar 探针（5s 心跳×3 判死锁，独立进程拉起，禁 fork）。
- **磁盘/CPU 看门狗（7.54）**：DiskGuardian 三级响应（预警 0.20/限流 0.10/熔断 0.05 且绝对剩余<1GB 拒写只读）；CpuGuardian 并发查询数代理（默认 64 拒绝新查询）；写路径统一 `check_all` 挂接。
- **M8-P1 写 Stall/假死回归**：组提交后 B 负载 18,987→149,539 ops/s。
- **运维接口**：SHOW PROCESSLIST（QueryRegistry + KILL QUERY 熔断）、SHOW STATUS（分配器/缓存命中/L0/吞吐）、EXPLAIN 适配（只推演不读数据）、`/metrics` Prometheus 分层埋点 + 分片指标。
- **1 亿库写路径 syscall 风暴修复（7.59）**：`needs_compact` 每次 put 调 `fs::metadata` → 快照缓存化，oltp_insert 2,676→23,964（+795%）、bulk_insert +45×。

---

## 十五、性能基准汇总

**本机 NVMe SSD（组提交 2ms，200k×4 线程，7.50）**：
| 负载 | 写入吞吐 | 混合吞吐 | p50 | p95 |
|---|---|---|---|---|
| a 写重 50/50 | 185.5k w/s | 90,935 ops/s | 8.0µs | 112µs |
| b 读重 95/5 | 186.8k w/s | 168,596 ops/s | 3.8µs | 62µs |
| c 纯读 | 199.0k w/s | 269,891 ops/s | 3.5µs | 44µs |

**大数据量（7.52/7.59/14.4）**：2000 万/5000 万 13 项 demo 全绿；1 亿库 sysbench oltp 全套 + 写路径修复后插入 2.7k→24k TPS；规模扩展 2 亿→5 亿：点查 7,180→5,207 QPS（缓降 -27%）、范围位置无关恒定、`COUNT status=active` 5 亿经 doc_count 载荷 p50 0.253ms（对比全扫 ~330s）。

**类 SQL 全扫优化链（1000 万库，7.96~7.100）**：`COUNT(*)` 12.6s → 3.58s（light 字段提取 + 扫描层优化 + key-only），与 MySQL 差距 14×→11×（结构限制已记录）。

**两节点分布式（7.51）**：真机（本机+阿里云）跨节点 4 线程×500 强一致校验通过（确定性路由点查全可见 + 广播精确命中）。

---

## 十六、编码规范 / 测试 / 构建部署

- **编码规范**：注释中文、Rust 惯例命名、内核层 thiserror / 二进制 anyhow、禁裸 unwrap（测试除外）、**unsafe 红线**（核心路径零 unsafe，确需必论证，mmap/io_uring 封装进白名单 crate）、磁盘格式编解码集中模块、原子提交、依赖治理。
- **测试策略**：可测试性基础设施（FileSystem/Clock/Allocator 抽象 + mock/loom）；并发用 loom 确定性模型；内存用 MockAllocator；崩溃模拟/损坏注入（FaultyFileSystem）；MockClock 快进；测试金字塔单元 70% / 集成 15% / 混沌 5%；全套最终 **613 全绿**（lib）+ 41 mysql。
- **构建部署**：常规 build / musl 静态交叉编译；生产推荐 `cross`（基础镜像用 rust:slim 不用 alpine）；Docker 多阶段构建 scratch 空镜像仅几 MB；与 Nova OS 协同（开发期 x86 Linux，MVP 后 musl 部署联调）。

---

## 十七、里程碑与版本发布

| 版本 | 内容 |
|---|---|
| v0.1.x | MVP：单机 LSM + CRUD + 倒排基础 + HTTP/CLI + 备份还原 + 看门狗子集 |
| v0.2.0 | 列式优化：PAX + TTL + Delta CF + 倒排统计载荷 + FST + 迁移工具 |
| v0.3.0 | 阶段 3 全部完成（配置热加载/IO 调度/基础 compaction/迁移高级版/小表广播 JOIN/Redis SDK/性能实测 260 全绿） |
| v0.4.0 | M6 高性能写入：环形 WAL/Leveled-Compaction/MVCC 快照读/热点保护区/增量备份（279 全绿） |
| v0.5.0 | M7 深度优化：MVCC 全局 seq/位图索引/YCSB+前沿调研（285 全绿） |
| v0.6.0 | SSD 原生迁移（Ex-5 全落地，313 全绿） |

分支模型：日常 develop 分支，里程碑验收后合入 master + Git Tag。

---

## 十八、开发排期队列完成状态（A~AF，全部 ✅）

**P0 级**：A 本机 2 千万/5 千万基准 ✅、B 1 亿 sysbench ✅、M 事务类查询优化 ✅、N 倒排回表批量读 ✅、P 事件驱动自动 Compaction ✅、AD 10 亿库阶段 A~D ✅、AE LSM 读路径范围优化（id BETWEEN p50 86ms→2.48ms，vs MySQL 3.6×）✅、AF SQL 能力补全（#1~#5 完成，#6 倒排加速 GROUP BY 待排期）🚧。
**P1 级**：C 分布式吞吐优化 ✅、H MySQL 协议适配 ✅、L Compaction 智能调度 ✅、O 读路径无锁化 ✅、R L0/层 Zone Map 粗筛 ✅。
**P2/P3 级**：D/E/F LSM 事务三阶段（WriteBatch/快照隔离/完整 ACID）✅、G 倒排 posting 优化 ✅、I 高并发查询 ✅、J 倒排段 GC 后台化 ✅、K fulltext posting 分块 ✅、S/T/U/V/W/X ✅。
**延伸**：Y 分布式+写路径收尾 ✅、Z 无锁合并 ✅、AA/AB/AC 导出增强 ✅。
**多表大项（2026-09-03 立项，非 A~AF 字母序列）**：§26 真多表 M1~M3 ✅（详见「二十、真多表（§26）实现归档」）。

**Ex 系列归档（全部 ✅）**：L0 单分片本地事务+组提交（已有）、Ex-1 outbox、Ex-2 SAGA、Ex-3 Calvin（评估不引入）、Ex-4 倒排字段策略（50M 库 inverted 2231.8→144.3MB，-93.5%）、Ex-5 SSD 原生、Ex-6 并发读（Seqlock/ArcSwap）、Ex-7 多核；Ex-8/Ex-9 系列（AE 与聚合加速）完成标记归档。

---

## 十九、关键取舍（评估后不引入 / 暂缓）

| 项 | 结论 |
|---|---|
| 读写分离 | 暂缓——组提交已解决读被写拖垮，剩余收益 <20% |
| Tiered 分层合并 | 不引入——写放大恒 ≈log2N，不优于 Leveled，且读放大风险 |
| 两级索引 | 不引入——内存为 Zone Map 7812×、过滤收益 0（现有 Block Index+分区布隆+Zone Map 已覆盖） |
| Calvin 确定性事务 | 不引入（远期触发）——单 docid 天然不分片，outbox+SAGA 已覆盖 |
| Calvin 硬件卸载 | 无必要——gseq 2 亿/s 远超目标 2+ 数量级 |
| io_uring | 默认关闭——2 核小机器负收益，多核 NVMe 生产按指引开启 |
| 倒排 posting 新编码 | 维持 Roaring——密集/簇状已达 1bit/docid 理论下限，稀疏绝对量小且牺牲 20× 交集性能 |
| 并行解压 | 回退——spawn 开销 ≫ zstd 解压省时 |

---

> **未尽事项**：AF #6 倒排加速 GROUP BY；SSD 环境「22 万写 TPS + 85 万读 QPS」并发基线实测；10 分片 10 亿构建验收（硬件）；raft TCP 真实多节点扩容编排联调；COUNT(*) 亚秒需引擎级 docid 可见计数维护（跨模块大工程，另行立项）。
> **P1 SQL 兼容补齐 ✅（2026-09-04 闭环，见 development_remain 一.3）**：INSERT IGNORE / ON DUPLICATE KEY UPDATE、DELETE FROM 全表、非默认表聚合按表区间（sqlish 窗口入口）、显式 FOR UPDATE 验证（当前读语义 + keys-only Tombstone 折叠修复）。

---

## 二十、真多表（§26）实现归档（M1~M3 ✅，2026-09-03 立项）

> 语义（用户确认）：支持**真多表**，不同表允许相同主键 id（表级主键空间隔离，对齐 MySQL）。

**方案 A（落地）**：表级 docid 命名空间——`docid = table_id(16bit) << 48 | row_id(48bit)`；
table_id 由表名确定性 FNV-1a hash & 0xFFFF 派生（`documents` = 0 = 既有单表库零迁移）；不触碰 docid_alloc 分片路径、不引入 Snowflake。

### M1 表路由（9d3e155 → ce7dcc1）
- mysql 层表名 → table_id（免注册/免持久化，跨连接与重启稳定）；SQL id ↔ docid 编解码在会话层；
- INSERT/UPDATE/DELETE/SELECT/DROP 按表路由；表级重复主键 1062（默认表 pre-check 已修）；
- DROP TABLE：默认表 documents = purge_all（兼容 c/sysbench cleanup）；非默认表 = 本表 docid 区间逻辑删；
- 验收：rr-conformance `--init` 双表各 id=1..2000 共存不撞 + 双表同 id 读写隔离 ✅。

### M2 per-table row_id 分配（dc53b66）
- 显式 SQL id 直落 row_id；auto 分配按表区间水位：默认表段预分配（水位续接 623726e + AUTO_INCREMENT 端到端）、非默认表逐行探测分配；
- 与显式 id 冲突保持 1062；docid 水位重启续接不撞已提交行。

### M3 Flush/Compaction 按表切分（19745a6，实施清单 1-5）
- flush_by_table：Immutable 升序键流按 `docid>>48` 边界切**每表单文件**（table_id=0 单文件，行为不变）；
- compact_merge/finalize_compact：归并输出按表切多文件、多输出逐文件登记（L1/L2 层内互不重叠）；
- **同表合并收敛**：底层仅同表 ≥2 段或混表老段触发合并；跨表每表 1 段 = 按表收敛（needs_compact 归零、重复 compact no-op）——杜绝多表库底层反复重写空转；
- meta_only 块级复用仅同表单段可用（跨表/混表回退全量合并并按表切分，顺带切分净化老混表文件）；
- DROP TABLE 物理回收该表区间专属 SST（`drop_table_range_files`，先逻辑删墓碑保证无复活）；修复 `table_name_of` 不解析 DROP/TRUNCATE 导致任何 DROP 误 purge 全库的问题；
- 回归：全量 lib 621 用例全绿（含 flush 切分 / 同表合并收敛 / 单表单文件回归 / DROP 文件回收重启一致 / 双表 400 行 e2e）。

**收尾约束（写入运维手册）**：删除位图按 docid 稠密寻址，多表高位 docid 下 delete/DROP 会爆内存 → 多表单删须关 `storage.deletion_bitmap_enabled`（传统 Tombstone 路径）。
**代价**：文件数 = Σ表 × 层文件；运维注意 ulimit -n / TableCache。
