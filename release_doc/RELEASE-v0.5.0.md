# 山水存迹数据库 v0.5.0 发布说明

> 发布日期：2026-08-29 · Git tag：`v0.5.0`
> 对比基线：v0.4.0 · 里程碑：M7 深度优化二阶段（design 4.7/5.2.4/22）

## 一、本版本亮点

1. **MVCC 全局 seq 一致性（design 4.7 完善）**：primary / delta 列族共享全局 seq 分配器
   （`Arc<AtomicU64>`）——所有写入（put / delete / patch / delta 清理）从全局计数分配 seq，
   `get_at` 的 Delta 增量按全局 seq 过滤：快照后字段级热更不可见，null 删除 / Tombstone 均按快照点判定，
   补全跨列族快照隔离；重启后从各列族 WAL 恢复全局起点；
2. **位图索引增强（design 5.2.4/7.2）**：枚举字段内存位图加速 COUNT/GROUP/AND——
   `[inverted] bitmap_fields` 白名单（默认关闭零开销）+ 启动全量重建
   （内存 `field → (value → RoaringBitmap)` 常驻）+ 写路径 `add` 同步维护；
   `bitmap_count`（COUNT 亚毫秒）/ `bitmap_group_by` / `bitmap_and`（组合 AND 交集）快速路径；
3. **YCSB 压测工具 + 瓶颈定位（design 22.2）**：新增 `shanshui-cunji-ycsb`（YCSB 规范负载 a/b/c/f，
   延迟分位数统计）；实测定位**头号瓶颈 = fsync 单条串行**（A 写重带 fsync 2,077 ops/s vs 无 fsync
   113,587 ops/s，55×）→ 组提交 + 读写分离列入下一里程碑（design 4.1.3 规划）；
4. **前沿调研报告（design 22）**：BVLSM WAL-time KV 分离 / RusKey RL Compaction / DobLIX·TieredKV 学习索引 /
   AuraDB Rust 生态——近期组提交、中期大 value KV 分离、长期 RL+学习索引。

## 二、功能与变更明细

| 类别 | 内容 | 提交 |
|---|---|---|
| MVCC 全局 seq | `Engine::global_seq`（primary/delta 共享）+ `ColumnFamily::set_external_seq` + `wal_append`；`WalWriter/RingWal::append_at`（指定 seq 追加，next_seq 单调推进）；`get_at` Delta 增量按全局 seq 过滤（快照隔离正确化）；`begin_snapshot`/`current_seq` 读全局计数 | 4283568 |
| 位图索引 | `[inverted] bitmap_fields` 白名单 + `InvertedIndex::with_bitmap_fields` 全量重建 + `add` 同步维护；`bitmap_count`/`bitmap_and`/`bitmap_group_by` 快速路径；`Engine::inverted_doc_count`/`inverted_group_by` 命中白名单走位图 + 新增 `inverted_bitmap_and_count` 组合 AND | 4a19550 |
| YCSB 压测 | `shanshui-cunji-ycsb`（负载 a/b/c/f、splitmix64 伪随机、P50/P95/P99/P999）；压测记录 `images/perf-0.5.0/验证记录.md`；瓶颈定位 + 优化建议 | d918c47 |
| 前沿调研 | `frontier-research-2026-08.md`：BVLSM/RusKey/DobLIX/TieredKV/SwiftKV/AuraDB 评估 | d918c47 |

## 三、性能快检（2026-08-29）

- **YCSB 负载**（4 线程 × 2 万 ops）：纯读冷缓存 18 万 ops/s（P50≈5µs）、热缓存 87 万 ops/s、
  100 万数据 SST 分层后读吞吐不掉；load 灌入 19–23 万 w/s；
- **写路径瓶颈实证**：A 写重带 fsync 2,077 ops/s vs 无 fsync 113,587 ops/s（55×）——
  fsync 单条串行为头号瓶颈，Group Commit 收益空间明确（design 4.1.3 表 4-1 预测 22 万 TPS）；
- **demo 1000万 回归快检 10/10**：批量插入 29.3 万条/s（34.1s；v0.3.0 基线 33.8 万/s，同机能波动）、
  主键点查 3.45ms/100、倒排 2.19s、备份·还原全对——M7 为功能层增强，读写热路径无回归。

## 四、质量数据

- **285 个单元测试全绿**（`cargo test`），较 v0.4.0（279）新增 6（M7-1 +2、M7-2 +4）；
- 项目自身 **unsafe = 0**（`#![forbid(unsafe_code)]`）；
- 新增能力均有测试：MVCC 快照隔离 Delta / null 删除 / 跨重启 seq 接续、位图内存写入 / 重启从段重建 /
  引擎级快速路径（COUNT/GROUP/AND）。

## 五、构建与使用

```bash
cargo build --release                          # mimalloc（默认）

# 位图索引（design 5.2.4 快速路径，默认关闭）
[inverted]
bitmap_fields = ["status", "city"]            # 枚举字段白名单 → COUNT/GROUP/AND 走内存位图

# YCSB 压测
shanshui-cunji-ycsb --workload a --records 100000 --ops 20000 --threads 4
shanshui-cunji-ycsb --workload c --records 200000 --ops 20000 --threads 4 --warm   # 缓存命中对照
```
