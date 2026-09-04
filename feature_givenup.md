# feature_givenup.md —— 明确不开发 / 评估结论不集成清单

> 2026-09-03 治理：从 feature.md / feature_remain.md 迁出的**明确不开发**项。
> 收录原则：已完成评估、结论为「不进入 kernel / 不采纳 / 不立项 / 维持现状」，短期无重开计划；
> 如未来触发条件出现，可回归 feature_remain.md 远期区重评估。

## 不立项 / 不集成清单

| 功能 | 来源模块 | 评估结论 | 依据 |
|---|---|---|---|
| Calvin 确定性事务（Ex-3，L3） | G 分布式 | **不进入 kernel**：确定性序零锁等待/无协调往返，但单 docid 路由天然不分片 + L1（Outbox）/L2（SAGA）已覆盖 | demo src/demo/calvin；「不进入 kernel」为最终结论，远期强一致多 docid 触发见 feature_remain |
| 倒排 posting 压缩新编码（Gorilla / 变长 / delta） | I 前沿 | **维持 Roaring，不引入新编码**：Roaring 密集 0.13B/docid=1bit 达理论下限；稀疏 2B/docid 为 delta 2×但绝对差小；Roaring AND 快 20.1×（337us vs 6.7ms）且库成熟 | M8 压缩探索（demo 验证） |
| 自适应多级 Block 索引 | 存储/读路径 | **不采纳**：块级 zstd 下「行级细索引跳块直读 200B」不可达（关键前提错误）；热路径已被 BlockCache/HotCache/Ex-8.3 覆盖；mmap 数据面不适用 | design_remain §14 / development_remain §17（评估完成标记） |
| 全局纪元 + 多文件 WAL | 写路径/WAL | **不立项**：单 NVMe 并发 fsync 无增益（设备串行）；seq（per-CF + WAL 头 next_seq）与水位语义（组提交 + flushed_seq/manifest）已覆盖且更简单；单 docid 本地事务无跨文件因果；真多设备由 Ex-5.10 条带化承担 | design_remain §9 / development_remain §13（评估结论：不立项） |
| Flush 频率优化（Ex-8.5 档位 A/B） | 写路径/性能 | **维持默认，不做档位调整**：memtable 256MB→512MB 档位 A/B 实测无收益（flush 非瓶颈）；并行 flush 非优先（推高 L0/compaction 压力） | 2026-09-03 Ex-8.5 档位实验 |

## 说明

- 本文件与 feature_remain.md 边界：**明确不做** → 本文件；**远期/条件触发再做** → feature_remain.md。
- 各条含完整结论与依据（含对应 design_remain / development_remain 引用），重开评估时先读引用。
