# v0.5.0 发布说明摘要

**山水存迹数据库 v0.5.0** · 2026-08-29 · `git tag v0.5.0`（对比基线 v0.4.0）

## 核心亮点
- **MVCC 全局 seq 一致性（design 4.7）**：primary/delta 共享全局 seq，`get_at` Delta 增量按全局 seq 过滤——跨列族快照隔离正确化（快照后字段级热更不可见）
- **位图索引增强（design 5.2.4/7.2）**：`[inverted] bitmap_fields` 枚举字段白名单 → 内存 RoaringBitmap 常驻，COUNT/GROUP/AND 亚毫秒快速路径（默认关闭零开销）
- **YCSB 压测定位瓶颈（design 22.2）**：新增 `shanshui-cunji-ycsb` 工具；实证 **fsync 单条串行 = 写路径头号瓶颈**（A 写重 2,077 vs 无 fsync 113,587 ops/s，55×）→ Group Commit + 读写分离列入下一里程碑
- **前沿调研（design 22）**：BVLSM / RusKey / DobLIX / TieredKV / AuraDB 评估 → 近期组提交、中期大 value KV 分离、长期 RL+学习索引

## 性能数据（2026-08-29）
| 场景 | 结果 |
| --- | --- |
| load 灌入 | 19–23 万 w/s（100 万条 5.2s）|
| 纯读（冷缓存）| **18 万 ops/s**，P50≈5µs（100 万 SST 分层后不掉速）|
| 纯读（热缓存）| **87 万 ops/s**，P50 0.9µs |
| 写重 A（fsync）| 2,077 ops/s（瓶颈实证）|
| 写重 A（无 fsync）| 113,587 ops/s |

## 质量
285 个单元测试全绿（+6）· demo 1000万 回归快检 · 项目自身 unsafe=0（`#![forbid(unsafe_code)]`）

## 构建
```bash
cargo build --release                          # mimalloc（默认）
[inverted]
bitmap_fields = ["status", "city"]            # 位图快速路径白名单
shanshui-cunji-ycsb --workload a --records 100000 --ops 20000 --threads 4
```

## 证据存档
- `images/perf-0.5.0/`：YCSB 压测验证记录 + demo 1000万 快检
- `frontier-research-2026-08.md`：前沿调研报告
- 完整发布说明见 [RELEASE-v0.5.0.md](./RELEASE-v0.5.0.md)
