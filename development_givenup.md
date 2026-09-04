# development_givenup.md —— 明确不开发 / 评估结论不立项清单

> 2026-09-03 治理：从 development.md / development_remain.md 迁出的**明确不开发**项。
> 收录原则：已完成评估、结论为「不进入 kernel / 不立项 / 不采纳 / 维持现状 / 已有等价能力」。
> 与 development_remain.md 边界：**明确不做** → 本文件；**远期/条件触发再做** → development_remain.md（三、远期）。

## 不立项 / 不引入清单

| 项 | 来源 | 评估结论 | 依据 |
|---|---|---|---|
| Ex-3 Calvin 确定性事务（L3） | development.md 7.45 / feature_givenup | **不进入 kernel（远期方向保留）**：单 docid 事务天然不分片；L1 outbox + L2 SAGA 已覆盖；Calvin 需全局事务序协调器（单点）+ 读写集预声明（倒排词表难静态声明），投入产出不匹配 | demo `src/demo/calvin`；7.45 / Ex-3.3 记录 |
| 倒排 posting 压缩新编码（Gorilla/变长/delta） | development.md 7.22 | **维持 Roaring，不引入新编码**：密集 0.13B/docid=1bit 达理论下限；稀疏 2B/docid 差量小；AND 快 20.1× | 7.22 探索记录 / feature_givenup |
| 全局纪元 + 多文件 WAL | 原 development_remain §13 | **不立项**：单 NVMe 并发 fsync 无增益（设备串行）；seq（per-CF + WAL 头 next_seq）与水位语义（组提交 + flushed_seq/manifest）已覆盖且更简单；单 docid 本地事务无跨文件因果；真多设备由 Ex-5.10 条带化承担 | design_remain §9 / problem_solving Pxx；远期触发（无锁多写者+多队列）见 development_remain 三 |
| 自适应多级 Block 索引 | 原 development_remain §17 | **不采纳**：块级 zstd 使行级细索引无法跳过整块解压（关键前提错误）；热路径已被 BlockCache/HotCache/Ex-8.3 覆盖；mmap 数据面不适用 | 记录备选 = 行式块内稀疏重启点（P3 微项，不主动排期）；design_remain §14 |
| Ex-9.2 持久 visible_count（无条件 COUNT 快路径） | 原 development_remain §19 | **已评估：不立项**（单集合口径受限）——单集合无分表（表=别名），全集合可见总数对业务 COUNT 无意义；价值仅在"单集合=单业务表"部署形态成立；触发 = 表/集合隔离（数据模型级）后的分域计数再评估 | 2026-09-03 评估（3 亿库 COUNT 194s 触发） |
| Ex-8.5 Flush 档位调整 / 并行 flush | 原 development_remain §12 | **维持默认 256MB，不做并行 flush**：256/512MB 档位 A/B 实测无收益（flush 非写吞吐瓶颈）；并行 flush 仅推高 L0/compaction 压力 | 2026-09-03 Ex-8.5 档位实验（65.0k vs 65.9k rows/s，噪声级） |
| 写路径阶段化提交（后台 ack） | 原 development_remain §18 复核 | **不新立项**：组提交已把 fsync 移出锁内；后台 ack 破坏同步耐久语义 | 复核结论 |
| MemTable 切换改造 | 原 development_remain §18 复核 | **不新立项**：双缓冲已具备 | 复核结论 |
| 后台预热（P3 可选） | 原 development_remain §14 | 不新立项（Linux 门控可选）；等价：空闲感知维护 Ex-8.9（待做，见 development_remain） | 标注 |
| scan IO 合并预读 / 熔断 / 零拷贝 | 原 development_remain §14/§15 | 不新立项：SCAN_GROUP=8 已落地 / 看门狗+cap 已具备 / 零拷贝并入 Ex-8.3 keys-only 投影 | 标注 |
| 两级索引（B+Tree 化）与 Calvin 硬件卸载 | development.md 7.80 | 评估收尾：**均不引入** | 7.80 / design_extension P2 评估 |

## 说明

- 各条含完整结论与依据（含 development.md / design_remain / feature_givenup 引用），重开评估时先读引用；
- 与 feature_givenup.md 重叠条目（Calvin、posting 压缩、自适应 Block、全局纪元、Ex-8.5）以 feature 侧为准汇总，本文件保留 development 视角记录。
