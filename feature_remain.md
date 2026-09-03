# feature_remain.md —— 功能未完成清单

> 2026-09-03 治理：已完成（✅ + commit）项已回填 feature.md「近期归档」节；
> 明确不开发/评估结论不集成项已移至 feature_givenup.md；
> 本文件仅保留 **未完成 / 待办 / 待评估 / 远期触发** 的功能。

## ⏳ 待办 / 排期

| 功能 | 模块 | 说明 |
|---|---|---|
| 存储格式现状澄清（design_remain 二，原 §16） | 设计文档 | design 3.4 字段ID紧凑编码未落地——现状=行式字段列+JSON 原文值；落地=P3 远期/修订 design 文档待用户确认 |
| 段级 min/max seq 快照剪枝（Ex-8.6 扩展） | 引擎读路径 | min seq 整段跳过已落地（`29be7b1`，回填 feature.md 近期归档）；max seq 侧候选——段元数据加 seq 范围，get_at/scan_range_at 整段跳过历史（跟踪 → development_remain 二） |
| 后台预热（P3 可选） | 缓存 | 空闲窗口预热缓存（跟踪 → development_remain 二） |

## ⏸ 暂缓 / 评估

| 功能 | 模块 | 结论 |
|---|---|---|
| 读写分离（Mutex/RwLock/COW 快照读；含双写加速） | G / I | M8-P1 `be09a07` 评估完成、维持暂缓——组提交已解决读被写拖垮；**等价能力已落地**：O 项第①②步（读路径 &self 化 `df16058` + RwLock 读读并行 `4585bb9`，1 亿库 read_only 42→561 TPS）+ HotCache 内部锁粒度（`9071984`）；剩余：写路径（txn_commit/compaction）仍串行；复制型 read_from_replica 属分布式阶段（触发再启） |
| posting 压缩/双区等缓存演进 | 倒排/缓存 | 已回填项见 feature.md 近期归档；新候选（如深页高频 search_after 端点）暂无明确触发 |

## 🔍 远期（触发条件满足后落地）

| 功能 | 模块 | 触发条件 |
|---|---|---|
| Calvin 全局事务序（Ex-3 远期） | G（L3） | 强一致多 docid 跨分区事务 + 读写集可静态预声明（13.3.1）；按 14.8 三阶段落地（当前评估结论「不进入 kernel」见 feature_givenup.md） |
| 多副本 raft 高可用 | G | Calvin 阶段三 / 元数据自动切换需求 |
| 存算分离 / Indexer Node | G | 百亿级规模（不推荐 MVP） |

## 说明

- 增量导入（P3-4）、增量备份（M6-5）、Parquet 生成器（M8-P3）等均已存在/评估，不重复立项。
- 本清单变更时同步更新 feature.md 对应行状态与「近期归档」节。
