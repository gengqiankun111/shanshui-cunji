山水存迹数据库（shanshui-cunji）Redis 集成部署指南（Redis Integration Guide）
版本：v1.0
适用版本：shanshui-cunji >= v0.2（分布式阶段）
前置条件：已部署 Redis 5.0+（单机 / 哨兵 / 集群）
关联文档：design.md | development.md

1. 设计哲学：Redis 是“读加速器”，不是“数据源”
角色	说明
shanshui-cunji	唯一的真理源（Source of Truth）。所有写入必须落盘（WAL + SST）。
Redis	分布式热读缓存（L2 Cache）。仅在 shanshui-cunji 之上加速查询，绝不作为写入缓冲。
同步方向	单向：shanshui-cunji → Redis（回填 / 失效）。绝不支持 Redis → shanshui-cunji 反向写入。
1.1 黄金法则（必须遵守）
先落盘，后缓存：写入顺序必须为 shanshui-cunji WAL → 返回 ACK → 异步失效/更新 Redis。严禁先写 Redis 再异步写 shanshui-cunji。

缓存只是副本：Redis 数据可随时丢失或清空，业务不应受影响（仅降级为较慢的磁盘读取）。

不做双向同步：绝不实现 Redis → shanshui-cunji 的数据同步，防止一致性灾难。

2. 部署模式（三种典型方案）
2.1 模式一：单机 shanshui-cunji + 本地 HotCache（默认，无外部 Redis）
适用场景：数据量 < 2 亿，QPS < 5 万，无需跨节点共享缓存。

配置：config.toml 中 [cache.external] enabled = false（默认）。

缓存层次：Client → shanshui-cunji HotCache（进程内）→ Disk。

延迟：热点命中 0.1~0.3ms。

2.2 模式二：集群 shanshui-cunji + Redis 旁路缓存（推荐）
适用场景：数据量 > 2 亿，热点集中，QPS > 10 万，多节点共享热数据。

配置：启用 [cache.external]，Redis 作为 L2 分布式缓存。

缓存层次：Client → Gateway → Redis（L2）→ shanshui-cunji HotCache（L1）→ Disk。

延迟：Redis 命中 0.1~0.3ms；穿透 shanshui-cunji 0.5~2ms。

2.3 模式三：轻量级防穿透（仅缓存空值）
适用场景：热点不集中，但存在大量无效 DocId 查询（如爬虫攻击）。

配置：cache_null_values = true，Redis 仅缓存 null 标记，TTL 60 秒。

收益：拦截 90% 以上的无效查询，保护 shanshui-cunji 不被穿透。

3. 读写流程详解（Cache-Aside + Write-Invalidate）
3.1 读路径（Read Path）
text
Client Request (GET /docid/1001)
        │
        ▼
┌────────────────────────────────────────────────────┐
│              网关 / SDK 协调器                     │
│  1. 查 Redis（L2 分布式缓存）                      │
│     ├── 命中 → 反序列化 → 直接返回 (0.1ms)        │
│     └── 未命中 → 降级查 shanshui-cunji                   │
│  2. 查 shanshui-cunji                                    │
│     ├── HotCache 命中 → 返回 (0.1ms)              │
│     └── 磁盘读取 → 返回 (0.5~2ms)                 │
│  3. 【异步】回填 Redis（SETEX, TTL=300s）          │
│     ├── 成功 → 记录命中率                         │
│     └── 失败 → 仅记录日志，不影响业务              │
└────────────────────────────────────────────────────┘
回填策略：

只有查询成功（文档存在）才回填。

可配置 cache_null_values = true 缓存空值（防穿透）。

回填采用 异步非阻塞（tokio::spawn），不增加主路径延迟。

3.2 写路径（Write Path）
text
Client Request (PUT /docid/1001)
        │
        ▼
┌────────────────────────────────────────────────────┐
│              网关 / SDK 协调器                     │
│  1. 先写 shanshui-cunji（权威数据源）                     │
│     ├── WAL fsync → MemTable → SST                │
│     └── 成功后返回 ACK 给客户端                    │
│  2. 【同步或异步】删除 Redis 中的旧缓存            │
│     └── DEL docid:1001                            │
│  3. 【可选】如果开启了预热（preheat_on_write）      │
│     └── 异步加载新数据到 Redis（SETEX）            │
└────────────────────────────────────────────────────┘
为什么是“删除”而不是“更新”？

删除是最安全的：下次读请求触发 Cache-Aside 回填最新数据，彻底杜绝缓存与数据库不一致。

更新需要额外读一次 shanshui-cunji（增加 IO），且可能出现并发写乱序。

3.3 双删策略（Double Delete，可选，强一致性场景）
在极高并发下（写后立即读），可能出现“删缓存 → 写磁盘 → 旧数据被其他线程回填”的极端情况。

解决：采用“先删缓存 → 写数据库 → 延迟 500ms 再删一次”。

text
PUT /docid/1001
1. Redis DEL docid:1001          // 第一次删
2. shanshui-cunji PUT (WAL + SST)        // 写磁盘
3. Thread.sleep(500ms)           // 等待可能发生的脏回填
4. Redis DEL docid:1001          // 第二次删（兜底）
配置开关：write_policy = "double_delete"（默认 invalidate，不开启）。

4. 配置详解（config.toml）
toml
[cache.external]
# ========== 总开关 ==========
enabled = false                     # 是否启用外部 Redis 缓存（默认关闭）

# ========== Redis 连接 ==========
redis_mode = "cluster"              # "single" | "sentinel" | "cluster"
redis_addrs = ["127.0.0.1:6379", "127.0.0.1:6380"]
redis_password = ""
redis_db = 0
redis_username = ""                 # Redis 6.0+ ACL

# ========== 缓存策略 ==========
ttl_seconds = 300                   # 缓存过期时间（5分钟，建议 60~600）
cache_null_values = true            # 是否缓存空值（防缓存穿透）
null_ttl_seconds = 60               # 空值缓存时间（1分钟）

# ========== 读写策略 ==========
write_policy = "invalidate"         # "invalidate"（推荐）/"double_delete" / "none"
read_policy = "cache-aside"         # "cache-aside"（推荐）/"read-through"

# ========== 高级保护 ==========
max_connections = 100               # Redis 连接池大小
timeout_ms = 100                    # Redis 操作超时（毫秒），超时自动降级
batch_invalidate_size = 100         # 批量删除每批最大 key 数
retry_attempts = 3                  # 失败重试次数
retry_delay_ms = 10                 # 重试间隔

# ========== 预热与统计 ==========
preheat_on_write = false            # 写入后是否主动预热（会增加写入延迟）
stats_interval_sec = 60             # 打印缓存统计周期（命中率）
5. 一致性保障机制（回答“会不会读到脏数据”）
场景	保障措施	一致性级别
正常写入	先写 shanshui-cunji（权威源），再删 Redis 缓存	强一致（读请求下次回填最新值）
并发写入同一 key	shanshui-cunji 自身有版本号/时间戳，最后写入者胜出；Redis 缓存被多次删除，最终是最新值	最终一致（毫秒级收敛）
Redis 不可用	熔断器打开，直接降级读 shanshui-cunji，不返回旧数据	降级一致（读权威源）
缓存穿透（回填期间）	多个请求同时 miss 时，使用 互斥锁（Mutex），仅一个请求查 shanshui-cunji	防击穿
缓存雪崩	TTL 加随机抖动（TTL = 300 + rand(60)）	防雪崩
6. 故障处理与降级策略
故障类型	检测方式	处理动作	用户影响
Redis 连接超时	timeout_ms 超时触发	立即返回错误，不阻塞，降级读 shanshui-cunji	延迟从 0.1ms 升至 0.5~2ms
Redis 节点宕机	连接池检测到断连	熔断器（Circuit Breaker）打开，后续请求直接透传 shanshui-cunji；定时探测恢复	同超时降级
Redis 内存满（OOM）	Redis 返回 OOM 错误	捕获异常，记录日志，不影响业务	无感知（缓存未命中，直接读磁盘）
网络分区	持续超时	熔断器保持打开状态，直到恢复	同超时降级
熔断器状态机：

text
CLOSED（正常）─── 失败次数 > 阈值 ───▶ OPEN（熔断，直接降级）
                                            │
                                     定时探测（如 10s）
                                            │
                          ┌─────────────────┘
                          ▼
                      HALF-OPEN（半开，允许少量请求通过）
                          ├── 成功 → CLOSED
                          └── 失败 → OPEN
7. 缓存统计与可观测性
7.1 管理命令
bash
# 查看缓存统计
shanshui-cunji admin cache-stats
> Redis Cache:
>   Hit Rate: 92.3%
>   Total Requests: 1,245,678
>   Hits: 1,149,000
>   Misses: 96,678
>   Errors: 45
>   Avg Latency: 0.12ms

# 手动清空指定 key 的缓存
shanshui-cunji admin cache-evict --key docid:1001

# 手动预热（将热点 key 提前加载到 Redis）
shanshui-cunji admin cache-warm --filter 'status="active"' --limit 10000
7.2 监控指标（接入 Prometheus）
指标名	类型	说明
shanshui-cunji_cache_redis_hits_total	Counter	Redis 命中次数
shanshui-cunji_cache_redis_misses_total	Counter	Redis 未命中次数
shanshui-cunji_cache_redis_errors_total	Counter	Redis 操作失败次数
shanshui-cunji_cache_redis_latency_seconds	Histogram	Redis 操作延迟分布
shanshui-cunji_cache_redis_connections_active	Gauge	当前活跃连接数
8. 性能基准（参考值）
8.1 各缓存层级延迟对比（P95）
层级	介质	P95 延迟	说明
L1：进程内 HotCache	内存（本地）	0.05~0.1ms	shanshui-cunji 自带，无需外部依赖
L2：Redis 分布式缓存	内存（网络）	0.1~0.3ms	本集成方案，跨节点共享
L3：shanshui-cunji 冷查询	磁盘 + BlockCache	0.5~2ms	缓存未命中时降级至此
8.2 吞吐能力（单网关，16核）
场景	QPS	说明
Redis 命中	~150,000 QPS	受限于网络和 Redis 单线程性能
Redis 未命中（降级 shanshui-cunji）	~50,000 QPS	受限于 shanshui-cunji 查询能力
混合场景（命中率 90%）	~130,000 QPS	加权平均
推荐 Redis 规格：

QPS < 5 万：Redis 单机（4GB 内存）

QPS 5~20 万：Redis 哨兵/集群（8~16GB 内存）

QPS > 20 万：Redis 集群（多分片）+ 客户端侧缓存

9. 最佳实践与避坑指南
✅ 推荐做法
设置合理的 TTL：建议 300~600 秒。太短（< 60s）命中率低；太长（> 3600s）可能读到脏数据。

开启空值缓存：cache_null_values = true，防止恶意查询穿透。

批量失效：当更新影响大量 DocId 时（如批量导入），使用 batch_invalidate_size 分批删除，避免 Redis 阻塞。

监控缓存命中率：低于 70% 时，考虑增大 TTL 或增加预热策略。

写入预热策略：preheat_on_write = false（默认），保持写入低延迟。

❌ 绝对禁止
绝对禁止“先写 Redis 再异步写 shanshui-cunji”：如果 shanshui-cunji 写入失败，Redis 中会留下脏数据。

绝对禁止“双向同步”：Redis → shanshui-cunji 的反向同步会导致不可控的环形更新。

绝对禁止把 Redis 当“消息队列”：不要用 Redis 存储待处理的任务列表。

绝对禁止没有 TTL 的缓存：必须设置过期时间，防止内存泄漏。

🚨 常见误区澄清
误区	真相
“Redis 挂了业务就不可用”	❌ 熔断器会自动降级读 shanshui-cunji，业务仅变慢，不会不可用。
“缓存一定要实时更新”	❌ TTL 过期 + Cache-Aside 回填，足以满足 99% 的场景。
“双删策略一定要开”	❌ 双删会增加写入延迟，仅在极端高并发强一致场景开启。
10. 架构演进与规划
阶段	能力	说明
阶段 2（分布式）	基础 Cache-Aside + Write-Invalidate	本指南所述核心功能
阶段 2.5	缓存预热工具 + 统计监控	管理命令 cache-warm、cache-stats
阶段 3	客户端侧缓存（Client-Side Cache）	在 SDK 中增加本地缓存，进一步降低延迟
阶段 3	Redis Streams 变更订阅（CDC）	将 shanshui-cunji 的写入事件（WAL）通过 Redis Streams 广播给订阅者
11. 附录：快速配置模板
A. 最小配置（单机 Redis）
toml
[cache.external]
enabled = true
redis_mode = "single"
redis_addrs = ["127.0.0.1:6379"]
ttl_seconds = 300
write_policy = "invalidate"
B. 生产配置（Redis 集群）
toml
[cache.external]
enabled = true
redis_mode = "cluster"
redis_addrs = ["redis-1:6379", "redis-2:6379", "redis-3:6379"]
redis_password = "${REDIS_PASSWORD}"  # 从环境变量读取
ttl_seconds = 600
cache_null_values = true
null_ttl_seconds = 60
write_policy = "invalidate"
timeout_ms = 50
max_connections = 200
C. 强一致配置（金融级）
toml
[cache.external]
enabled = true
write_policy = "double_delete"        # 启用双删
ttl_seconds = 120                     # 缩短 TTL
timeout_ms = 30                       # 缩短超时，快速失败
preheat_on_write = true               # 写入后主动预热
12. 总结
维度	结论
定位	Redis 是 shanshui-cunji 的 L2 分布式热读缓存，不是数据源。
同步方向	单向：shanshui-cunji → Redis（回填 + 失效）。
写入顺序	先 shanshui-cunji，后 Redis（失效），保证数据一致性。
读取策略	Cache-Aside：先查 Redis，未命中则查 shanshui-cunji 并回填。
可用性	Redis 不可用时，熔断器自动降级读 shanshui-cunji，业务仍可用。
一致性	默认最终一致性；可选“双删”提升至近似强一致。
一句话概括：

“shanshui-cunji 管持久化，Redis 管读加速——各司其职，协同工作，这才是现代云原生架构的正确姿势。”