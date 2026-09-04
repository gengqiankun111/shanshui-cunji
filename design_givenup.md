# design_givenup.md —— 明确不开发 / 评估否决的设计项

> 2026-09-03 治理：从 design.md / design_remain.md 迁出的**明确不开发（评估否决/不引入/不立项）**
> 设计项，含裁决依据。与 design_remain.md 边界：**明确不做** → 本文件；**远期/条件触发再做** → design_remain.md。
> 汇总裁决表见 design.md §23.2。

## 否决设计清单

| 设计项 | 来源 | 裁决 | 依据 |
|---|---|---|---|
| Calvin 全局事务序（L3） | design_extension 13.3/14.8/14.9（原 design_remain §1/§18） | **不进入 kernel**（远期方向保留） | 单 docid 事务天然不分片；L1 outbox + L2 SAGA 已覆盖；Calvin 需全局事务序协调器（单点）+ 读写集预声明（倒排词表难静态声明） |
| 桶 / 分区 LSM（固定哈希分桶 + 桶内重叠度调度） | 原 design_remain §17 | **不立项** | 主键 = auto-increment docid 无界单调 → 固定桶写入恒落最后一桶（倾斜）；动态分裂桶 = 现有"时间窗口 L0→L1→L2"层级收敛同构；主数据 L0 文件先验重叠 ≈ 0；四阶段大多已落地。真正增量 = 宽文件 range 局部合并（sub-compaction，P3 触发项，见 development_remain） |
| B+Tree 外置索引文件 / L1/L2 改 B+Tree 存储 | 原 design_remain §7 | **当前不引入** | 范围/回表瓶颈在使用面（收集路径线性税、扫描无层/文件剪枝、全值解码、块缓存旁路）非存储格式；现有 L2 精确索引本质即"外部 key→块索引"；Ex-8.1~8.3 修复使用面后 102×→3.6× 达标；Ex-8.4 远期触发见 design_remain |
| 两级索引（Level 1 内存常驻摘要 + Level 2） | 原 design_remain §6/7.80 | **不引入** | demo two-level-index：全局摘要内存为 Zone Map 7812×、过滤收益 0、点查已 sub-ms；现有 Block Index + 分区布隆 + Zone Map 已覆盖 |
| Calvin 硬件卸载（gseq 接 DSA/PMem） | design_extension 14.9（原 §6） | **无必要性** | demo gseq-hw：AtomicU64 原子 seq 1-2 亿/s >> 100 万/s 目标（2+ 数量级）；跨机房+CPU 瓶颈才复评 |
| 热度感知自适应多级 Block 索引 | 原 design_remain §14 | **不采纳** | 行级偏移无法越过块级 zstd 解压（关键前提错误）；热块已被 BlockCache/HotCache 覆盖（Ex-8.3 扩到扫描）；mmap 数据面不适用。记录备选 = 块内稀疏重启点（P3 微项，不主动排期） |
| 全局纪元计数器 + 多文件 WAL | 原 design_remain §9 | **不立项** | 单 NVMe 并发 fsync ≠ 更快（设备串行）；per-CF seq + 组提交 + flushed_seq/manifest 语义已覆盖且更简单；单 docid 本地事务无跨文件因果；真多设备由 Ex-5.10 条带化承担 |
| 写路径阶段化提交（fsync 移锁外 + 后台 ack） | 原 design_remain §15 | **不采纳** | fsync 早已不在写锁内（组提交窗口统一 fsync，M8-P0）；后台 ack 破坏同步耐久语义（put 返回 = 已入组提交窗口）；瓶颈是单写者串行 + 倒排/合并背压，非锁内 fsync |
| 分片跳表（256 分片）/ 跳表→B+Tree 热切换 | 原 design_remain §8.1 | **否决** | crossbeam-skiplist 本就并发安全；put 受组提交 fsync + 倒排 + flush + 合并背压约束；分片破坏 memtable 全局有序（scan/merge/快照依赖）；S 项多版本版本链已覆盖 |
| 文件系统式存储引擎 | 原 design_remain §11 | **不推荐**（质变重构） | 元数据/并发/事务复杂度剧增；与量变策略相悖 |
| 16KB 块替换 4KB | 原 design_remain §8.1 | **不默认切换**（可配置实验） | 与 Ex-5.1 冲突：4KB 正是 NVMe 优化产物（读放大 -67%）；block_size_kb 已参数化 |
| 并行 Flush | 原 design_remain §8/§13 | **不实施**（Ex-8.5 结论） | 档位 A/B（256/512MB）实测无收益 → flush 非瓶颈；并行 flush 仅推高 L0/compaction 压力 |
| 写路径多写者阶段一之外 | 原 design_remain §9.2 | 远期触发（Ex-13 链） | 需引擎写锁拆分 + 多设备/NVMe 多队列；见 design_remain 一.6 |

## 说明

- 各条含完整裁决依据（对应 design_remain 旧版节 / development_remain / design.md §23.2），重开评估先读引用；
- 与 development_givenup.md / feature_givenup.md 重叠条目（Calvin、全局纪元 WAL、自适应 Block 索引、Ex-8.5）以各系视角分别记录。
