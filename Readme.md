# 山水存迹数据库（shanshui-cunji）

> **海量数据，轻松存取** —— 面向"高吞吐写入 + 快速检索"的文档数据库

**山水存迹数据库（shanshui-cunji）** 是一款**文档型数据库 + 轻量检索引擎**，专门解决"数据量大、写入频繁、还要按任意字段快速查询"的业务场景。基于 LSM-Tree 存储引擎构建，**单节点设计基准为 1.5 亿文档 / 300GB 磁盘**——此水位下写入 P99 < 2ms、热点查询亚毫秒；**集群可线性扩展至 50 亿级**，满足 90% 以上日志、埋点、画像场景，支持从单机到分片平滑演进。

> 一句话定位：**不需要复杂事务、但要求海量数据 + 极速查询**的业务，就是 shanshui-cunji 的用武之地。

## 目录

- [一、它解决什么问题？](#一它解决什么问题)
- [二、容量与部署形态：先单机，后分片](#二容量与部署形态先单机后分片)
- [三、什么时候用它？什么时候别用它？](#三什么时候用它什么时候别用它)
- [四、核心能力一览](#四核心能力一览)
- [五、快速开始](#五快速开始)
- [六、从 MySQL 迁移](#六从-mysql-迁移)
- [七、关键配置](#七关键配置)
- [八、技术概览与更多资料](#八技术概览与更多资料)
- [九、架构定位：与各类数据库的关系](#九架构定位与各类数据库的关系)
- [十、数据管道：导出到 ClickHouse / MySQL](#十数据管道导出到-clickhouse--mysql)

---

## 一、它解决什么问题？

| 你的痛点 | shanshui-cunji 的解法 |
| --- | --- |
| 数据量大、写入频繁，关系库顶不住 | LSM 引擎 + WAL 批量组提交，持续高吞吐写入 |
| 想按任意字段筛选，又不想提前建一堆组合索引 | 内存字典 O(1) 定位 + 倒排索引，任意字段 AND/OR 自由组合 |
| 热点数据要"秒回" | HotCache 热点缓存，命中亚毫秒返回 |
| 大宽表点查慢、IO 浪费 | 列式读取，只读需要的列，跳过无用大字段 |
| 聚合统计拖垮数据库 | COUNT / GROUP BY 走倒排统计载荷，零回表、不 OOM |
| 从 MySQL 迁移太痛苦 | 一键迁移工具 + 类 SQL 语法，DAO 层不用重写 |
| 怕宕机、怕丢数据、运维累 | 自愈看门狗、崩溃安全、备份还原、平滑扩容 |

**存储介质**：shanshui-cunji 是**纯硬盘持久化（Disk-Based）**——数据写入须经 WAL fsync + 落盘 SSTable 才算成功；HotCache / 倒排字典等内存仅是加速层，断电或重启后磁盘数据完好、WAL 恢复。**数据量只受磁盘限制（TB 级），不受内存限制**。

---

## 二、容量与部署形态：先单机，后分片

**这是选型时最需要先想清楚的问题。** shanshui-cunji 支持两种部署形态：

| 维度 | 单机 Standalone | 分片集群 Cluster |
| --- | --- | --- |
| **最大数据量** | **1.5 亿条**（设计基准：1.5 亿文档 / 300GB 磁盘，写入 P99 < 2ms） | **50 亿级**（线性扩展） |
| 部署成本 | 单台机器即可 | 多节点 + 网关 + 元数据中心 |
| 水平扩展 | — | DocId 哈希分片，虚拟分片平滑扩容 |
| 高可用 | 依赖备份还原 | 一主多从异步复制、读写分离 |
| 典型适用 | 数据量 ≤ 1.5 亿，单机够用 | 数据量 1.5 亿 ~ 50 亿，或需要高可用 |

### 怎么选？

- **数据量 ≤ 1.5 亿** → 直接上**单机版**：部署最简单、成本最低、见效最快（单节点设计基准 1.5 亿文档 / 300GB，写入 P99 < 2ms、热点查询亚毫秒）；
- **数据量 1.5 亿 ~ 50 亿**，或需要多副本高可用、水平扩展 → 用**分片集群**；
- **推荐规划方式**：起步先用单机版把业务跑起来，数据量逼近 1.5 亿时再平滑扩容到分片——分片对业务透明，业务代码无需改动。

---

## 三、什么时候用它？什么时候别用它？

### ✅ 推荐场景（完美适配）

| 场景 | 例子 |
| --- | --- |
| 日志 / 行为埋点 | 点击日志、APP 行为日志、访问日志、IoT 上报 |
| 风控 / 画像标签库 | 用户标签、设备画像、黑名单 |
| 对象元数据存储 | 文件、图片、视频的元信息 |
| 电商非交易侧检索 | 商品基础信息、后台运营筛选 |
| IoT 设备快照 | 设备状态、告警标记、时序快照 |
| 后台报表数据源 | 大数据结果离线筛选中间库 |

### ⚠️ 可用但有限制（慎用）

- **用户中心 / 账号系统**：主键查询很快，但**不支持跨分片事务**（仅在极少同时改多用户的场景可用）；
- **订单库**：只能做**只读查询副本**（查询加速库，不能当交易主库）。

> ⚠️ **运营分析类查询强制约束**：针对**不带时间范围**的超大结果集拖拽筛选（如全量 `status='active' AND type='click'`），系统将自动触发熔断，返回 `Query Too Expensive`。建议在查询条件中增加 `time_range`（如 `created_at > '2026-08-01'`），利用 TTL 时间分区将扫描范围控制在 1~2 天内，即可获得毫秒级响应。

### ❌ 不适合（别用）

- 强事务核心交易系统（下单、支付、库存扣减）；
- 超高并发、无分片键的全局 C 端自由搜索（请用 ES 等独立搜索引擎）；
- 大量跨分片 JOIN / 多表关联复杂 SQL。

> **分布式边界**：shanshui-cunji 是**分布式集群**，不是**分布式事务数据库**——不支持跨分片事务、跨分片 JOIN、全局快照读。如果你需要多行事务，请放在 **MySQL / Redis（业务层）** 处理；换来的是 8 节点集群写入总吞吐 **200 万+ TPS、延迟 1ms**（TiDB 跨分片事务延迟 8ms+）。

---

## 四、核心能力一览

- ⚡ **写入快**：LSM-Tree + WAL 批量组提交，持续高吞吐
- 🔥 **热点查询亚毫秒**：HotCache 热点哈希缓存，命中直接返回
- 🔍 **任意字段毫秒筛选**：内存字典 O(1) 定位 + 追加式倒排，AND/OR 自由组合，免建组合索引
- 📐 **固定条件对标 MySQL**：组合稀疏索引一步定位
- 📊 **大宽表省 IO**：PAX 混合列式布局，点查只读热列切片
- 🔢 **聚合零回表**：TermMeta 内嵌统计载荷（doc_count 常驻内存），COUNT / GROUP BY 不 OOM
- 🩹 **更新省 10 倍 IO**：Delta 列族 + Merge-on-Read，部分字段更新只写增量
- 🧠 **倒排查询提速 80%**：随机 IO 转顺序批量预取 + 结果集截断
- 🚀 **重启亚秒恢复**：FST + mmap 冷启动，倒排索引秒级可用
- 🗂️ **列族隔离**：主数据 / 组合索引 / 倒排索引独立存储，互不干扰、可独立调优
- ⚙️ **全维度可配置**：缓存、分片、副本、同步/异步复制、读写分离
- 🚀 **协程网络层 + 线程池隔离**：基于 Tokio 异步运行时与 Rayon 计算池，单机可抗 10k 长连接，写入 TPS 突破 20 万，CPU 密集型倒排查询绝不阻塞网络事件循环
- 🛠️ **生产级运维工具**：支持 SHOW PROCESSLIST / KILL、EXPLAIN 执行计划诊断、实时内存状态监控，数据库运维不再"黑盒"
- 📦 **数据管道生态**：shanshui-cunji-export 一键导出 Parquet/CSV 对接 ClickHouse/Spark；shanshui-cunji-import 快速迁移历史数据
- 🛡️ **7x24 自愈**：内存限流、Compaction 假死自愈、慢查询熔断、进程探针
- 🔄 **平滑迁移**：MySQL 全量/增量导入，类 SQL WHERE 语法
- 📦 **数据安全**：WAL 崩溃安全 + Tombstone 删除 + 备份还原 + TTL 自动过期

---

## 五、快速开始

### 构建

```bash
# 依赖：Rust stable / nightly
cargo build --release
```

### 启动服务

```bash
shanshui-cunji server --config config.toml
```

### 数据模型

shanshui-cunji 中每条记录是一个**文档**：主键 `DocId (u64)` + 扁平字段集合（字符串 / 数值 / 布尔 / 时间）。写入后自动维护主键索引、组合索引与各字段倒排索引，之后可按任意字段组合条件检索。

```json
{
  "docid": 1001,
  "fields": {
    "status": "active",
    "type": "order",
    "device": "android",
    "score": 88,
    "created_at": "2026-08-26T10:00:00Z"
  }
}
```

### CLI 基本用法

```bash
shanshui-cunji put --id 1001 --data '{"status":"active","type":"order","device":"android"}'   # 写入
shanshui-cunji get --id 1001                                                                  # 主键查询
shanshui-cunji search --filter 'status=active AND type=order'                                 # 条件筛选
shanshui-cunji range --start 1000 --end 2000                                                  # 范围查询
shanshui-cunji delete --id 1001                                                               # 删除
```

### HTTP-JSON 接口

```bash
# 写入
curl -X POST http://localhost:8080/put \
  -H 'Content-Type: application/json' \
  -d '{"docid":1001,"fields":{"status":"active","type":"order","device":"android"}}'
# 主键查询
curl http://localhost:8080/get?docid=1001
# 条件筛选
curl 'http://localhost:8080/search?filter=status%3Dactive%20AND%20type%3Dorder'
```

### 备份与还原

```bash
shanshui-cunji backup /path/backup_file
shanshui-cunji restore /path/backup_file
```

---

## 六、从 MySQL 迁移

shanshui-cunji 是**文档型 + 无/弱 Schema** 存储，与 MySQL 的关系型 + 强 Schema + ACID 事务概念模型不同，**不承诺 MySQL 协议 / 语法兼容**，MySQL 仅作为**数据迁移来源**。

### 迁移三步走

1. **导出**：MySQL 侧执行 `mysqldump` 导出数据（或通过 JDBC 拉取）；
2. **迁移**：运行 `shanshui-cunji-migrate` 一键导入——MySQL 每行 → shanshui-cunji 一个文档（`DocId` 主键），`CREATE INDEX` 自动映射为组合索引 / 倒排配置；
3. **改连接**：业务代码将 MySQL 驱动（`jdbc:mysql://...`）替换为 shanshui-cunji 客户端（`http://shanshui-cunji:8080`），DAO 层 SQL 改写为 Filter / 类 SQL WHERE 语法。

### 重要提醒

- ⚠️ **业务代码需要修改**：底层驱动与连接配置必须更换，但 DAO 层业务逻辑无需重写（SDK 提供与 JDBC 类似的 `query()` / `execute()` 接口，适配器模式）；
- 类 SQL 语法仅支持 `SELECT ... WHERE ... AND/OR` 子集，**不支持 JOIN / GROUP BY / 子查询 / 事务**；
- 迁移细节见 [development.md](./development.md)，兼容策略见 [design.md](./design.md) 第 15 章。

---

## 七、关键配置

### 单机最小配置

```toml
[server]
mode = "standalone"
[runtime]
async_mode = "multi-thread"  # 协程池：multi-thread（默认）/ current-thread（边缘低功耗）
cpu_cores_total = 0          # 0 = 自动检测物理核数并按公式分配（防超售）
async_worker_threads = 0     # tokio 工作线程数，0 = 自动检测核数（建议 N-4）
async_max_tasks = 10000      # 最大并发协程数（超限返回 429）
compute_pool_size = 8        # 倒排/位图计算池（0 = 核数/2，满则降级全表扫描）
compute_queue_max = 1000
io_background_threads = 4    # 刷盘/Compaction 磁盘线程
io_uring_enabled = false     # 阶段 3：io_uring 协程化磁盘 IO
[affinity]                   # 阶段 3：CPU 绑核（默认关闭）
enabled = false
network_cpus = []            # 示例 [0,1,2,3,4,5,6,7]
compute_cpus = []            # 示例 [8,9,10,11]
io_cpus = []                 # 示例 [12,13]
[memory]
watermark_high = 0.85        # RSS 软限流水位，触发写限流（OOM Guardian）
watermark_stall = 1.0        # RSS 硬限流水位，触发 503 + 紧急止损（缩容/jemalloc purge）
[memtable]
max_size_mb = 256            # 跳表上限，达阈值冻结切换并后台刷盘
[query.optimizer]
enabled = true          # 多级索引路由选择（主键/组合/倒排）
[hotcache]
max_memory_mb = 4096    # HotCache 硬上限
eviction_policy = "lfu" # lru / lfu / tiny-lfu
eviction_high_water = 0.85  # 软水位，主动淘汰冷数据（防突发卡顿）
eviction_low_water = 0.75   # 淘汰至该水位即止
[blockcache]
max_memory_mb = 2048    # 与 HotCache 隔离
block_size_kb = 16      # NVMe 选 16KB，机械盘选 64KB
eviction_high_water = 0.85  # 软水位，主动淘汰旧块
[inverted]
engine = "fst"          # 倒排字典引擎：hash（MVP）/ fst（阶段 1.5，mmap 亚秒冷启动）
[network]
bytes_per_second_limit = 800_000_000  # 800MB/s 软限流，防止突发打爆网卡
[mmap]
prefetch_ratio = 0.1                  # 启动时预加载 10% 倒排字典到物理内存

[sstable]
compression = "zstd"        # none / snappy / lz4 / zstd
compression_level = 3       # zstd 专用 1-22
bloom_fpr = 0.01            # 布隆假阳性率
index_granularity = 16      # 两级索引：每 N 个 Block 一条摘要

[storage]
l0_stall_threshold = 8  # L0 文件数阈值，超过则写 Stall 限流
```

### 分片集群最小配置

```toml
[server]
mode = "cluster"
node_id = "node-1"
internal_rpc_port = 9090
[sharding]
enabled = true
virtual_shards = 1024   # 扩容只迁移部分虚拟分片
consistent_hash = true
[replication]
enabled = true
role = "slave"
master_addr = "node-0:9090"
sync_mode = "async"     # 边缘网关用 async，金融强一致用 sync
[read_write_separation]
enabled = true
read_from_replica = true
replica_lag_threshold_sec = 10
[broadcast_query]
max_concurrent = 10
timeout_ms = 30000
```

缓存、分片、主从、读写分离、广播熔断等全量配置项及默认值见 [design.md](./design.md) 第 6.5 / 9.8 节与第 13 章。

---

## 八、技术概览与更多资料

### 架构概览

```
网络层 (HTTP-JSON / TCP / CLI)
        │
        ▼
查询执行器 (查询优化器 Lite 路由 / 组合索引 / 倒排交集)
        │
        ▼
LSM-Tree 存储引擎
  WAL → MemTable(跳表) → SSTable(多层, PAX 混合列组)
  主键索引 / 组合稀疏索引 / 倒排(内存字典 + Append 文件)
  布隆过滤 / Zone Map / LRU 块缓存 / HotCache / Compaction
```

- **写入路径**：写请求 → WAL → MemTable → 后台刷盘 → SSTable → Compaction
- **主键查询**：HotCache 命中（亚毫秒）→ 未命中走 LSM + 布隆过滤 + 稀疏索引
- **条件筛选**：查询优化器 Lite 动态路由——固定条件走组合索引；任意字段走内存字典定位 + Bitmap 交集/并集
- **范围剪枝**：读块前先用块级 min/max（Zone Map）粗筛，跳过不满足条件的数据块
- **分布式**：网关路由层 + 分片节点（复用单机内核）+ 元数据中心，DocId 一致性哈希（虚拟分片）路由，平滑扩容，异步复制高可用

### 性能调优快速指引

| 症状 | 调整参数 | 说明 |
| --- | --- | --- |
| 写入慢 / 频繁 Stall | `storage.l0_stall_threshold` ↑、`compaction.rate_limit_mb/s` ↑ | 提升 Compaction 吞吐 |
| WAL fsync 慢 | 组提交批大小、`server.wal_mode = "perf"` | 牺牲极小安全换吞吐 |
| 热点点查命中率低 | `hotcache.max_memory_mb` ↑、`hotcache.eviction_policy` | 提高缓存命中 |
| 倒排查询慢 | `inverted.engine = "fst"`、`inverted.max_posting_scan` ↓ | 门控 + 亚秒冷启动 |
| 聚合慢 | 倒排统计载荷（`doc_count`） | COUNT/GROUP BY 零回表 |
| 广播查询超时 | `broadcast_query.max_concurrent` / `broadcast_query.timeout_ms` | 配合网关 Term 缓存 |
| 磁盘空间紧张 | `storage.ttl_days` 开启 | TTL 时间分区整删目录 |
| 冷启动慢 | `inverted.engine = "fst"` | mmap 亚秒级恢复倒排服务 |
| 内存吃紧 | `hotcache.max_memory_mb` / `blockcache.max_memory_mb` ↓ | 启动校验可用内存 0.7 阈值 |

### 更多资料

- [design.md](./design.md) — 完整技术设计（存储引擎、索引、缓存、备份、分布式蓝图、路线图）
- [development.md](./development.md) — 开发实现文档（模块结构、数据格式约定、实现要点、开发顺序、编码规范、测试与验收）
- [join_function.md](./join_function.md) — 关联查询（JOIN）白皮书与详细使用指导
- [redis-integration-guide.md](./redis-integration-guide.md) — Redis 集成部署指南（Cache-Aside + Write-Invalidate、熔断降级、配置模板、性能基准）
- 完整诊断手册（写入 / 查询 / 分布式三路监控项 + 红绿灯瓶颈矩阵）见 [design.md](./design.md) 第 17 章

---

## 九、架构定位：与各类数据库的关系

> **不是替代品，而是"容量与速度的扩展层"。** MySQL 管钱，Redis 管热，shanshui-cunji 管海量——三者搭配，各司其职。

### 与 MySQL / MongoDB / Elasticsearch 的定位矩阵

| 数据库 | 核心模型 | 事务/一致性 | 写入性能 | shanshui-cunji 能否替换 |
| --- | --- | --- | --- | --- |
| MySQL (InnoDB) | 关系型（B+Tree） | 强 ACID 跨行事务 | 中等（随机写受限） | ❌ 不能替换核心交易库（无跨行事务） |
| MongoDB | 文档型 | 文档级 ACID | 中等 | ⚠️ 部分替换：点击流 / 设备上报 / 无事务元数据，写入快 3~5 倍、存储成本更低 |
| Elasticsearch | 倒排全文检索 | 最终一致 | 慢（分词 + 建索引） | ✅ 特定场景：仅存结构化字段（status/type）不做全文分词，写入快 10 倍、资源省 50% |

**可放心替换**：行为埋点、IoT 设备上报、历史订单归档、系统审计日志、用户标签画像。
**绝对禁止替换**：银行交易、库存扣减、购物车结算、账户余额——必须保留 MySQL/PostgreSQL。

### 与 Redis：天然黄金搭档（冷热分层）

| 维度 | Redis | shanshui-cunji | 互补关系 |
| --- | --- | --- | --- |
| 存储介质 | 内存（贵） | 硬盘（廉价） | 热数据进 Redis，冷 / 全量数据进 shanshui-cunji |
| 数据量上限 | 受内存限制 | 受磁盘限制（TB 级） | Redis 扛热点，shanshui-cunji 扛海量全量 |
| 查询延迟 | 微秒级 | 亚毫秒级 | Redis 一级缓存，shanshui-cunji 二级持久化 |

**Redis + shanshui-cunji 双存储架构（Cache-Aside + Write-Invalidate）：**

```
Client → 网关/SDK 协调器
  Write: shanshui-cunji PUT（WAL 落盘成功返回 ACK）→ DEL Redis 旧缓存
  Read:  优先 Redis（L2）→ Miss 查 shanshui-cunji（HotCache/Disk）→ 异步回填 Redis(TTL)
```

- SDK 内置 `shanshui-cunjiWithRedis` 门面：读回填 + 写失效自动协调，业务代码无感切换；**绝不双写、绝不反向同步**（红线，design 21）；
- 用 1 份 Redis 内存（只存 1 小时热数据）换无限容量的全量历史查询；shanshui-cunji 仅作冷存储时 CPU 几乎全留给写入；
- MySQL 只读分析副本：Canal / Debezium 监听 binlog 实时同步到 shanshui-cunji（详见 [design.md](./design.md) 第 1.3 节）。

**三种部署模式：**

| 模式 | 适用场景 | 配置要点 |
| --- | --- | --- |
| 🥇 单机纯 shanshui-cunji | 数据量 < 2 亿、QPS < 5 万 | `[cache.external] enabled = false`（默认），依赖进程内 HotCache |
| 🥈 集群 + Redis 旁路缓存（推荐） | 数据量 > 2 亿、热点集中、QPS > 10 万 | 启用 `[cache.external]`，Redis 存热点完整文档，命中率 > 90%，延迟 0.1~0.3ms |
| 🥉 轻量防穿透层 | 大量无效 DocId 查询（爬虫攻击） | `cache_null_values = true`，Redis 仅存 null 标记，TTL 60s，拦截 90% 无效查询 |

> 完整部署指南（读写流程、双删策略、熔断状态机、配置模板、性能基准）见 [redis-integration-guide.md](./redis-integration-guide.md)。

### 关联查询（JOIN）：不提供语法，但提供 4 种替代方案

> shanshui-cunji 是**文档检索数据库**，不提供 SQL JOIN。但 95% 的"需要 JOIN"实际是"需要数据关联后的结果"——我们有 4 种高效替代方案：

| 方案 | 适用场景 | 落地阶段 |
| --- | --- | --- |
| ① 二次查询 + 内存合并（SDK `queryAndJoin`） | 外键关联、结果集 < 10 万条 | 阶段 1.5 |
| ② 写入时预连接（Enrich 展开关联数据） | 关联关系固定、写入时已知 | 阶段 1.5 |
| ③ 物化视图（后台定时预聚合） | 固定维度聚合统计 | 阶段 2 |
| ④ 导出到 OLAP（`shanshui-cunji-export` → ClickHouse） | 复杂多表 JOIN / BI 报表 | 阶段 2 |

- ❌ **跨分片 JOIN**：网关直接拒绝，返回 `Query Not Supported`；
- 📊 决策树与详细使用指导见 [join_function.md](./join_function.md)。

### 与全品类系统对比

| 对比维度 | shanshui-cunji | NewSQL（TiDB/CockroachDB） | MongoDB | Cassandra/HBase | Spark/Flink |
| --- | --- | --- | --- | --- | --- |
| 核心定位 | 海量文档高速写入与灵活检索 | 兼容 MySQL/PG 的弹性扩展数据库 | 灵活 Schema 通用文档库 | 高可用高可写海量存储 | 批处理与流计算 |
| 数据模型 | 文档型（扁平字段） | 关系型 | 文档型（嵌套 JSON/BSON） | 宽表 / 列族 | 无固定模型 |
| 事务（ACID） | 无跨分片事务 | 强一致，完整 ACID | 多文档 ACID（有性能代价） | 最终一致 / 行级 | 通常不提供 |
| 写入性能 | 极高（~20 万 TPS/节点） | 高 | 较高 | 极高（写优化） | 高（批量/流式） |
| 点查延迟 | 亚毫秒（HotCache 命中） | 毫秒（P99 <15ms） | 毫秒（~1.5ms） | 毫秒（P99 <10ms） | 秒级（批处理） |
| 复杂分析能力 | 弱（无 JOIN/复杂聚合） | 强（HTAP 列存） | 中等（聚合管道） | 弱 | 极强（核心能力） |
| 索引能力 | 主键 + 组合 + 倒排 | 丰富二级索引 | 丰富 + 地理/文本索引 | 主要主键/行键 | 依赖底层存储 |
| 扩展性 | 高（哈希分片） | 极高（自动分片再平衡） | 高（分片集群） | 极高（对等节点） | 极高（分布式计算） |
| 一致性 | 最终一致（默认异步复制） | 强一致（Raft/Paxos） | 可调 | 最终一致 / 可调 | 不适用 |
| 运维复杂度 | 低 | 高（组件多，调优复杂） | 中高 | 高 | 极高 |
| 典型场景 | 日志/埋点/IoT/画像/元数据 | 金融核心/电商订单/实时风控 | 内容管理/用户画像/IoT | 时序/事件日志/推荐 | 数据湖/ETL/机器学习 |

**优势**：在特定领域做到极致——超高性价比写入吞吐、组合+倒排查询灵活度、极低资源成本与运维门槛，独有聚合毫秒级（倒排统计载荷）+ TTL 无碎片过期。

**劣势与边界**：无跨分片事务（金融交易禁用）、分析能力弱（当"数据源"而非"分析引擎"）、无分片键全局检索是广播慢查询、功能丰富度不及通用库。

**选型建议：**

**选择 shanshui-cunji，如果**：场景是日志/埋点/IoT/用户行为事件；负载以高吞吐写入 + 主键或字段快速查询为主；数据是扁平结构化文档；可接受最终一致性；希望降低硬件与运维成本。

**不要选择 shanshui-cunji，如果**：需要强 ACID 事务（金融/订单）→ 选 TiDB、OceanBase；需要复杂 SQL/BI 分析 → 选 ClickHouse、Doris；数据高度嵌套 JSON 且查询多变 → 选 MongoDB；需要跨地域强一致多活 → 选 CockroachDB。

**一句话总结**：*以放弃通用性为代价，换取日志 / 埋点 / IoT 等海量写入与检索场景的极致性能和成本优势。*

---

## 十、数据管道：导出到 ClickHouse / MySQL

> 统一**流式导出管道**：SST 顺序扫描 → Filter → Projection → Sink Adapter 分叉。内存恒定（batch × 单行）、不阻塞写入、断点续传。详见 [design.md](./design.md) 第 20.5 节。

### 导出到 ClickHouse（分析引擎首选，Parquet 直读）

```bash
# 导出为 Parquet，ClickHouse 直接 SELECT FROM file() 读取
shanshui-cunji export \
  --filter 'created_at > "2026-08-01"' \
  --format parquet \
  --output /data/export/202608.parquet \
  --batch-size 50000 --compression zstd
```

```sql
-- ClickHouse 侧：零代码导入
INSERT INTO my_table SELECT * FROM file('/data/export/*.parquet', 'Parquet');
```

- `--dry-run-schema` 一键生成 MergeTree 建表 DDL；按 `toYYYYMM(created_at)` 分区对齐。

### 导出到 MySQL（兼容/迁移，CSV + LOAD DATA）

```bash
# 生成 MySQL 兼容 CSV + 配套 LOAD DATA SQL（比逐条 INSERT 快 ~20 倍）
shanshui-cunji export \
  --format csv --mysql-compatible \
  --output /data/export/mysql_import/

# 或 JDBC 直连写入（无文件落盘，阶段 3）
shanshui-cunji export \
  --jdbc "mysql://localhost:3306/analytics" \
  --table "orders" --batch-size 5000
```

- `--mysql-max-varchar` 处理 65KB 行限制，超长字段自动降级 TEXT。

### 增量导出（每日同步数仓）

```bash
shanshui-cunji export \
  --filter 'updated_at > "$(cat /data/checkpoint.txt)"' \
  --format parquet \
  --output /data/export/incremental_$(date +%Y%m%d).parquet \
  --checkpoint /data/checkpoint.txt
```

- 基于 `updated_at` 时间戳游标自动续传；无时间字段场景用 `--range 'docid > X AND docid <= Y'`。

### 资源与安全

- 顺序扫描（无随机 IO）+ 后台 IO 优先级低于前台读写 + `--rate-limit` 限网卡；
- `--dry-run` 预评估行数 / 体积 / 耗时（不实际导出）；
- **对在线业务影响 < 5%**，shanshui-cunji 是"可靠数据源"而非"数据孤岛"。
