# 前沿调研报告（design 22，M7-3）

> 调研日期：2026-08-29
> 主题：LSM-Tree 存储引擎前沿（WAL-time KV 分离 / RL 驱动 Compaction / 学习型索引 / Rust 生态）
> 定位：标注**可选择性吸收**方向，均为远期，不改变当前务实路线（design 22.3）

## 1. 前沿成果盘点

### 1.1 WAL-time KV 分离（BVLSM，2025）

- **痛点**：传统 KV 分离（WiscKey 等）在 flush 阶段才解耦，MemTable 仍存大 value，内存压力大、写放大高。
- **方案**：在 **WAL 阶段**就解耦 key/value——WAL 只记 key + meta，大 value 直接进独立 Value Log，MemTable 只存轻量元数据（Key-ValueOffset），大 value 走多队列并行存储。
- **收益**：异步 WAL 下 64KB 随机写吞吐比 RocksDB **7.6×**、比 BlobDB **1.9×**；显著降低写放大与内存占用、消除 I/O 抖动。

### 1.2 RL 驱动 Compaction（RusKey，SIGMOD'24）

- **痛点**：Leveling（读优化）与 Tiering（写优化）静态二选一，动态负载下无普适最优。
- **方案**：提出 **FLSM-tree**（层内动态 K 值 = 每层 Run 数量），用 **RL（DDPG）在线指导 LSM 变换**（何时 Leveling↔Tiering、Bloom 内存分配），无需先验负载知识。
- **收益**：动态负载下端到端比 RocksDB **4×**。后续 ArceKV/ElasticLSM（RL 压缩决策引擎，动态场景 ~3×）进一步扩展动作空间（含写停顿规避）。

### 1.3 学习型索引（DobLIX PVLDB'25 / TieredKV SYSTOR'25 / SwiftKV 2025）

- **DobLIX**：LSM 场景**双目标**学习索引——同时优化「索引查找」与「存储数据访问」，并带 RL agent 实时调参；吞吐较 SOTA 提升 **1.19–2.21×**（RocksDB 内验证）。
- **TieredKV**：两层设计——LSM 管新写入，独立学习索引管读；GC 时**非阻塞**把 LSM 数据转成学习索引；读最高 **4.32×**、写 1.43×。
- **SwiftKV**：SSTable 索引块用两层级联回归模型（Greedy-PLR + LR）替代二分，索引内存省 30%、读延迟降 1.19–1.60×。

### 1.4 Rust 生态现状（AuraDB，2025 crates.io）

- **AuraDB**（`auradb`，0.1.0）：三合一概念引擎——BVLSM（WAL-time KV 分离）+ RusKey（RL Compaction）+ DobLIX（学习索引），异步优先，自带完整 YCSB 压测套件；目前仅 M0（基础 LSM 骨架，3.5K SLoC）。
- 启示：Rust LSM 生态仍在早期；shanshui-cunji 的**文档模型 + 倒排 + 缓存/运维体系**组合定位仍具差异化优势（design 22.3）。

## 2. 对 shanshui-cunji 的借鉴评估

| 方向 | 与现状差距 | 价值评估 | 建议阶段 |
|---|---|---|---|
| Group Commit 组提交 | **未实现（已实证为头号瓶颈，见 perf-0.5.0）** | 极高 | **下一里程碑（P0）** |
| WAL-time KV 分离 | 当前 WAL 记全量 JSON | 中高（山水存迹存文档，大 value 场景直接受益） | 中期（评估大 value 占比后） |
| RL 驱动 Compaction | Leveled 静态分界 | 中（先补齐组提交/写读分离再谈） | 长期 |
| 学习型索引 | 倒排 FST + 两级索引 + 位图（M7-2）已务实加速 | 低-中（数据分布未知时收益不稳） | 长期 |
| RDMA / CXL / PMEM / GPU | 单机 + 主从 | 依硬件普及 | 长期 |

## 3. 结论

- **短期**：落实 Group Commit + 读写分离（压测实证 55× 收益空间），随后重跑 YCSB 基线（目标：A 写重 ≥ 5 万 ops/s）。
- **中期**：若大 value 文档占比高 → 探索 WAL-time KV 分离（BVLSM 思路，与环形 WAL 叠加）。
- **长期**：跟踪 RL 调参（RusKey/ArceKV）与学习索引（DobLIX/TieredKV），仅在明确收益后再吸收——坚持「组合取胜」的产品定位（design 22.3）。

## 参考来源

- RusKey：arXiv:2308.07013（SIGMOD'24，Learning to Optimize LSM-trees via RL）
- BVLSM：arXiv:2506.04678（WAL-Time Key-Value Separation）
- DobLIX：PVLDB 18(11)（Dual-Objective Learned Index for LSM）
- TieredKV：SYSTOR'25（Tiered LSM-Learned Index）
- SwiftKV：Future Internet 2025, 17, 398
- AuraDB：crates.io/crates/auradb（BVLSM + RusKey + DobLIX 概念引擎）
