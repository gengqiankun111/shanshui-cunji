# v0.2.1 发布说明摘要

**山水存迹数据库 v0.2.1** · 2026-08-28 · `git tag v0.2.1`（对比基线 v0.2.0）

## 核心亮点
- **分配器加固（design 14.0）**：默认启用 mimalloc，消除 musl 默认 malloc 全进程单锁瓶颈
- **SST v5 分区布隆（design 4.4.2）**：每块独立布隆，查询只校验目标块，内存减半
- **数据关联（design 19）**：`query_and_join`（Inner/Left/Right + 熔断）、HTTP `/join`、写入侧 Enrich
- **运维管理 + 数据管道（design 20）**：`admin status/processlist/kill`、`explain` 推演、`export/import`（CSV/JSONL）
- **压测工具**：`shanshui-cunji-bench` 分配器高并发对比基线

## 性能实测（2026-08-28）
| 场景 | 结果 |
| --- | --- |
| 分配器（musl）| mimalloc 4 线程 **×9.9**（298k QPS vs 系统 30k），1/2/4 线程 ×4.7/×4.2/×9.9 |
| 分配器（glibc）| mimalloc ×1.30 |
| 批量插入 | 31.6~36.4 万条/s（1000万/2000万/5000万 三规模稳定，10/10 通过）|
| 倒排检索 | 近常量：1.25 亿命中 ~2.3s |
| vs v0.1.0 | 插入 **+70%**、倒排 **-81%** |

## 质量
163 个单元测试全绿 · demo 冒烟 10/10 · 项目自身 unsafe=0（`#![forbid(unsafe_code)]`）· cargo audit/deny 通过

## 构建
```bash
cargo build --release                                        # mimalloc（默认）
cargo build --release --features alloc-jemalloc --no-default-features  # jemalloc
cargo build --release --no-default-features                  # 系统分配器（对比基线）
```

## 兼容性
- SST v5 新格式，Reader 自动回退兼容 v3/v4，旧库可直接打开
- KILL 为标记式（真正中断留阶段 2）；jemalloc 未验证 Windows

## 证据存档
- `images/allocator-bench/`：分配器对比图表 + 原始数据 + 验证记录
- `images/perf-0.2.1/`：1000万/2000万/5000万 逐节截图 + 报告 + 汇总
- 完整发布说明见 [RELEASE-v0.2.1.md](./RELEASE-v0.2.1.md)
