# v0.3.0 发布说明摘要

**山水存迹数据库 v0.3.0** · 2026-08-28 · `git tag v0.3.0`（对比基线 v0.2.1）

## 核心亮点
- **分布式集群（阶段 2 / M5）**：一致性哈希分片 + 元数据中心 + 无状态网关（广播 Chunk 直拼 O(1)）+ RPC；主从复制（async 攒批 / sync 等 ACK）+ 网关全局 Term 缓存；TDS 热备 + **无损扩容三步协议**（双写→追平→原子切换）
- **倒排架构升级**：预分片 Chunk 广播直拼、倒排段 GC、写停滞 StallWatchdog 自愈 + Sidecar 心跳
- **物化视图 + 两级索引**：维度聚合（增量游标）+ SST Level 1 常驻摘要 / Level 2 懒加载（内存 ~1/16）
- **数据管道**：import-schema（倒排白名单）+ Redis 外部缓存（Cache-Aside + Write-Invalidate + 熔断）
- **阶段 3 深度优化**：配置热加载、IO 限速（Token Bucket）、基础 Compaction、增量导入（checkpoint 续传）、**小表广播 JOIN**、**Redis 冷热分层 SDK 门面**（`ShanshuiCunjiWithRedis`）
- **性能实测对照 design 9.5** + 修复读路径回归（`get_from_sst` 克隆整个 Level 2 索引 → 按需取单条）

## 性能实测（2026-08-28，i7-10750H 6C12T / 16G / NVMe）
| 场景 | 结果 |
| --- | --- |
| 批量插入 | 29.5~33.8 万条/s（1000万/2000万/5000万，10/10 通过）|
| 倒排检索 | 近常量：1.25 亿命中 2.9s（FST + 分区布隆 + 20 万抽样回表全对）|
| 热点查询 | 主键 0.02~0.35ms / HotCache 0.02ms（100/100 命中）|
| 对照 design 9.5 | 组提交写入与热点查询达成/超出目标（硬件 6C/16G 低于基准 16C/64G）；10k 并发留待 M6 |

## 质量
260 个单元测试全绿 · demo 冒烟三规模 10/10 · 项目自身 unsafe=0（`#![forbid(unsafe_code)]`）

## 构建
```bash
cargo build --release                                        # mimalloc（默认）
cargo build --release --features alloc-jemalloc --no-default-features  # jemalloc
cargo build --release --no-default-features                  # 系统分配器（对比基线）
```

## 阶段 3 新增运维
```bash
shanshui-cunji reload --config config.toml                 # 配置热加载
shanshui-cunji compact                                      # 手动 Compaction
shanshui-cunji-import --incremental --checkpoint <file>     # 增量导入断点续传
```

## 证据存档
- `images/perf-0.3.0/`：1000万/2000万/5000万 逐节截图 + 报告 + 汇总（对照 design 9.5）
- 完整发布说明见 [RELEASE-v0.3.0.md](./RELEASE-v0.3.0.md)
