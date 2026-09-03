# 山水存迹数据库 v0.3.0 发布说明

> 发布日期：2026-08-28 · Git tag：`v0.3.0`
> 对比基线：v0.2.1（f8c3615）· 阶段：阶段 2（M5 分布式集群）+ 阶段 3（深度优化）完成

## 一、本版本亮点

1. **分布式集群（阶段 2，M5）**：一致性哈希分片路由器（128 虚拟点/节点）+ 元数据中心 + 无状态网关（广播检索 Chunk 直拼 O(1)）+ 分片节点 RPC；主从复制（ReplicationLog 追加持久化 + async 攒批 / sync 等 ACK）+ 网关全局 Term 缓存（LRU + TTL 兜底 + 写计数失效）；TDS 术语字典热备 + **无损扩容三步协议**（Shadow Writes → Delta Catch-up → Atomic Switch，含回滚预案）；
2. **倒排架构升级**：预分片 Chunk 广播检索直拼（分区性质）、倒排段 GC（崩溃安全）、写停滞 StallWatchdog 假死自愈 + Sidecar 心跳探活；
3. **物化视图 + 两级索引（design 4.4.2）**：MaterializedView 维度聚合（Count/Sum/Avg + docid 增量游标）+ MvScheduler；SST 两级索引——Level 1 常驻摘要（内存 ~1/16）+ Level 2 精确索引懒加载；
4. **数据管道增强**：import-schema（倒排字段白名单，预创建索引字段基座）；Redis 外部缓存（design 21，RESP 零依赖客户端 + Cache-Aside + Write-Invalidate + 熔断）；
5. **阶段 3 深度优化**：配置热加载（`reload` + ReloadReport）、IO Token Bucket 限速（`io_rate_limit_mb`）、基础 Compaction（全量合并 + 原子切换 + 限速）、迁移工具增量导入（checkpoint 断点续传）、**小表广播 JOIN**（design 19.3，关联 key 数 ≤ 阈值一次全量扫描建内存索引）、**Redis 冷热分层 SDK 门面**（`ShanshuiCunjiWithRedis`：读回填 + 写失效协调）；
6. **性能实测对照 design 9.5**：三规模 10/10；**修复读路径回归**（`get_from_sst` 克隆整个 Level 2 索引致倒排查询挂起 → 按需取单条，恢复 2.4s）。

## 二、功能与变更明细

| 类别 | 内容 | 提交 |
|---|---|---|
| 分片路由 | 一致性哈希两级路由（docid→虚拟分片→物理节点，128 虚拟点），route 单分片 / nodes 广播目标；平滑扩容仅迁 ~1/N 虚拟分片 | dc17043 |
| RPC / 网关 / 元数据 | JSON-over-TCP 帧协议 + Engine 分片处理器；元数据中心（注册/摘除/主从/JSON 持久化）；无状态网关（路由下发 + 广播 Chunk 直拼 + 探活），Local/Rpc 双端点 | 53bb924 |
| 主从复制 | ReplicationLog（追加 + seq 游标恢复）+ Replicator（async 攒批 / sync 等 ACK）+ repl.apply 幂等批量 | eca36c6 |
| Term 缓存 | 网关全局 LRU（节点,Term）→ Bitmap，TTL 兜底 + 1s 写计数主动失效 | eca36c6 |
| TDS / 扩容 | 术语字典热备（内存 + 文件写穿）；无损扩容三步协议 + 回滚预案；shard.scan_all | fda44a6 |
| 物化视图 | MaterializedView（维度 Count/Sum/Avg + 增量游标）+ JSON 持久化 + MvScheduler | 8bcc077 |
| 两级索引 | SstReader Level 1 常驻摘要 + Level 2 懒加载精确索引，`index_granularity` 可配 | 8bcc077 |
| 看门狗 | StallWatchdog 写停滞假死自愈（连续 3 次 FatalExit）+ HeartbeatSidecar 文件锁心跳 | df8e9d4 |
| 倒排升级 | 预分片 Chunk（同哈希抽分片）+ 网关直拼 O(1)；倒排段 GC（tmp→fsync→原子 Manifest） | 7db4764 |
| 数据管道 | import-schema（倒排白名单 / 组合索引声明 / 时间戳游标） | 35f87cd |
| Redis 外部缓存 | RESP 客户端 + CacheBackend 抽象 + Cache-Aside / Write-Invalidate（double_delete）/ 熔断器 + `[cache.external]` | da20c4c |
| 配置热加载 | `Config::reload` + ReloadReport（变更节）+ CLI `reload` | 69b39dc |
| IO 限速 | `IoRateLimiter` Token Bucket（0=不限速）+ 刷盘按实际字节 acquire | 4884a58 |
| Compaction | ColumnFamily 全量合并（key 升序 seq 降序去重 + 原子 Manifest）+ `needs_compact` + CLI `compact` | 3c48521 |
| 增量导入 | `import_csv/json_incremental` docid 游标断点续传 + checkpoint 原子写 + `--incremental --checkpoint` | 5085db8 |
| 小表广播 JOIN | `join.broadcast_enabled/threshold`；≤ 阈值一次全量扫描建内存索引（首个命中优先），否则回退点查 | 31fc054 |
| Redis 门面 | `ShanshuiCunjiWithRedis`：读回填（Cache-Aside）+ 写失效协调（先落盘再删缓存） | 4f693bd |
| 读路径修复 | `get_from_sst` 克隆整个 Level 2 索引（M5 遗留）→ `locate_indexed_block` 按需取单条，倒排查询恢复 2.4s | d472f94 |

## 三、性能实测（2026-08-28，i7-10750H 6C12T / 16G / NVMe）

### 3.1 功能性能测试（1000万 / 2000万 / 5000万条，跳过 1 亿）

| 规模 | 批量插入 | 插入速率 | 倒排词条检索 | 分片路由 | 备份·还原 | 结果 |
|---|---|---|---|---|---|---|
| 1000万 | 29.6s | 33.8 万条/s | 2.4s（250万命中）| 35.4s | 26.8s | 10/10 |
| 2000万 | 67.9s | 29.5 万条/s | 2.6s（500万命中）| 104.4s | 60.6s | 10/10 |
| 5000万 | 163.6s | 30.6 万条/s | 2.9s（1250万命中）| 313.8s | 222.1s | 10/10 |

对照 design 9.5（16核/64G/NVMe/1.5 亿）：**组提交写入与热点查询延迟达成/超出目标**（硬件仅 6C/16G 低于基准）；倒排计数毫秒级、HotCache 0.02ms；10k 连接并发类指标留待 M6 异步运行时。证据：`images/perf-0.3.0/`。

### 3.2 读路径回归修复

M5 两级索引重构后 `get_from_sst` 每次点查克隆整个 Level 2 精确索引（亿级库 200k 次回表需数亿次小分配），实测 1000万 倒排词条查询从 2s 恶化至挂起（>8min）；改为 `locate_indexed_block` 借用二分 + 单条克隆后恢复 2.4s，2000万 / 5000万 同样稳定。

## 四、质量数据

- **260 个单元测试全绿**（`cargo test`），demo 冒烟三规模 10/10；
- 项目自身 **unsafe = 0**（`#![forbid(unsafe_code)]` 编译期强制）；
- 数据一致性：分片 4 等分精确、备份还原倒排计数精确匹配、增量导入断点续传幂等。

## 五、构建与使用

```bash
# 默认（mimalloc，推荐）
cargo build --release

# jemalloc（Linux/musl 推荐）
cargo build --release --features alloc-jemalloc

# 系统分配器（对比基线）
cargo build --release --no-default-features

# 阶段 3 新增运维
shanshui-cunji reload --config config.toml   # 配置热加载
shanshui-cunji compact                        # 手动 Compaction
shanshui-cunji-import --incremental --checkpoint <file>  # 增量导入断点续传
```
