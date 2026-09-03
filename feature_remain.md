# feature_remain.md —— 功能未完成清单

> 从 feature.md（分模块状态）提取的**未完成功能**，按状态分类。
> 已 ✅ 功能不在此列（参见 feature.md 全表）。当前基线：466 测试全绿。

## 🔄 进行中（有部分落地，剩余待做）

| 功能 | 模块 | 已落地 | 剩余 |
|---|---|---|---|
| 导出增强（design 20.5 全功能） | E | ✅ 全部完成：Parquet（70c3b30）；增量（2174531）；流式管道（c6b5417）；JDBC 直连（c6b5417）；MySQL 兼容 CSV + LOAD DATA SQL（313bd81）；--dry-run-schema DDL（313bd81）；--io-rate-limit-mb 后台 IO 优先级（40e8abb） | — |

## ⏳ 待办

| 功能 | 模块 | 说明 |
|---|---|---|
| 存储格式现状澄清（2026-09-03，design_remain §16） | 设计文档 | design 3.4 字段ID紧凑编码未落地——现状=行式字段列+JSON 原文值；落地=P3 远期/修订 design 文档待用户确认 |
| 事务扫描位图过滤（Ex-8.10，✅ e1ae41b） | 事务/删除 | scan_range_txn 排除位图已删 + 自写新插入/复活并入（558 全绿） |
| 删除位图读无锁化（Ex-8.14，✅ 2c623d9） | 读路径/删除 | is_deleted 改 ArcSwap 快照 + 原子字节读（无 RwLock/无互斥）；写路径短 Mutex + 倍增扩容；格式/持久语义不变（559 全绿） |
| 自适应多级 Block 索引（已评估，2026-09-03） | 存储/读路径 | ❌ 不采纳：块级 zstd 下"行级细索引跳块解压直读 200B"不可达（关键前提错误）；热路径已被 BlockCache/HotCache/Ex-8.3 覆盖；mmap 数据面不适用。备选记录 = 块内稀疏重启点 P3 微项（design_remain §14 / development_remain §17） |
| 分层压缩（Ex-8.12，已排期 P2） | 存储/压缩 | 值得做：L2+ zstd 高等级（6~15）——zstd 解压近等级无关 → 近免费空间/IO 收益，代价仅后台压缩 CPU；落地点 = compaction out_level 选档（现有 compression_level 单配置）。风险核查：Ex-5.8 块级复用在跨层等级变化失效 + L0/L1 轻量档中间层放大；50m 库 A/B 量化后定默认。共享字典压缩 = 远期触发（design_remain §12 / development_remain §16） |
| L1/L2 延迟大合并（Ex-8.11，已排期 P2） | Compaction/写放大 | 值得做：`l0==0 && (l1>1||l2>1)` 即全收敛 → 级联写放大属实（column_family.rs:1573-1583）。新增 L1/L2 独立段数/大小阈值受控实验（A/B 在 50m 库测写放大/点查/范围）；前置 Ex-8.2 剪枝防范围读放大回退；L0 全局键索引与主动异步压 L0 已落地不重复（design_remain §11 / development_remain §15） |
| "量变优化"五方向（已评估，2026-09-03） | 引擎/运维 | 删除语义对齐（scan 位图）✅ Ex-8.1 已修（e63603a）；剩余 Ex-8.10 txn 扫描位图过滤（P1）+ Ex-8.9 空闲感知维护（P3）；IO 合并预读/熔断/keys-only 已落地或并入 Ex-8.2/8.3；后台预热 P3 可选（design_remain §10 / development_remain §14） |
| 全局纪元 + 多文件 WAL（已评估，2026-09-03） | 写路径/WAL | ❌ 不立项：单 NVMe 并发 fsync 无增益（设备串行）；序号（per-CF seq + WAL 头 next_seq）与水位语义（组提交 + flushed_seq/manifest）已覆盖且更简单；单 docid 本地事务无跨文件因果；真多设备由 Ex-5.10 条带化承担。远期触发 = 无锁多写者 + 独立 WAL + NVMe 多队列（design_remain §9 / development_remain §13） |
| 范围查询优化（Ex-8.1~8.3，已排期 2026-09-03） | 引擎读路径/性能 | 根因：非事务 `id BETWEEN` 走收集路径（逐 SST 线性走块索引 + L2 全量 clone + 收集排序）→ 50m 范围 86ms 且位置依赖。**Ex-8.1 ✅（e63603a，553 全绿 + 50m 验收：86ms→3.1ms，MySQL 差距 ~102×→3.7×）**：流式 merge 折叠同源同 key 多版本 + scan/count 删除位图过滤（put 复活）+ 非事务 id BETWEEN 流式化；demo range-window 6 passed。Ex-8.2 scan 层/文件剪枝 → Ex-8.3 块缓存 + keys-only；详见 design_remain §7 / development_remain §11 |
| Flush 频率优化（Ex-8.5，P2，2026-09-03 修正） | 写路径/性能 | 先零成本 memtable 档位 A/B（256→512MB config 一行）测 flush 频次/停顿/L0 增长；并行 flush（多 immutable 分片）仅当档位实验证实 flush 为瓶颈再实施（并行 flush 推高 L0/compaction 压力，非优先；design_remain §13） |
| 倒排后台 IO 预算共享（Ex-8.13，P3 候选） | 倒排/后台 IO | 倒排 flush/GC 写纳入既有后台 io_limiter（与 compaction/导出共享预算语义）或并入 Ex-8.9 空闲窗口，消除与合并的 IO 争用（design_remain §13 矛盾③收尾） |
| 段级 min/max seq 快照剪枝（Ex-8.6，P2 候选） | 引擎读路径 | 段元数据加 seq 范围，get_at/scan_range_at 整段跳过历史；与 Ex-8.2 key 范围剪枝互补 |
| 删除密度 urgency（Ex-8.7，P3 候选） | Compaction | urgency 加位图置位率加权，删除密集段优先合并释放空间 |
| posting LRU 双区热点化（Ex-8.8，P2 候选） | 倒排检索 | 现有 posting LRU 256（c380792）仿 HotCache 双区 promote + 容量参数化，不新增缓存结构 |
| 高并发查询优化（design 9.5 目标） | I（P3） | ✅ 完成：同步预处理读锁 + 小栈（97e3586）+ 异步协程运行时（2802885，连接 idle 不占线程，500 连接仅 15 线程）；10k 连接吞吐达成需目标硬件基准复测 |
| io_uring Linux 部署实测 | io_queue（V） | ✅ 完成（7.71）：热路径接入（SSTable 块读 + WAL fsync 走 SQPOLL）+ 阿里云 Debian 12 实测（池初始化成功、核隔离生效、A/B：2 核小机器写 -13%、读持平）→ 默认关，多核 NVMe 开启 |
| 导出管道资源控制 | E | ✅ 完成（--io-rate-limit-mb 40e8abb） |

## ⏸ 暂缓 / 评估

| 功能 | 模块 | 结论 |
|---|---|---|
| 读写分离（Mutex/RwLock/COW 快照读） | G（I） | M8-P1 暂缓——组提交已解决读被写拖垮；**HotCache 内部锁粒度已落地**（7.72 `9071984`，读读并行 demo x4.16、混合负载 x5.42）；复制型分布式阶段再启 |

## 🔍 远期（触发条件满足后落地）

| 功能 | 模块 | 触发条件 |
|---|---|---|
| Calvin 全局事务序 | G（L3） | 强一致多 docid 跨分区事务 + 读写集可静态预声明（13.3.1）；按 14.8 三阶段落地 |
| 多副本 raft 高可用 | G | Calvin 阶段三 / 元数据自动切换需求 |
| 存算分离 / Indexer Node | G | 百亿级规模（不推荐 MVP） |

## 说明

- 增量导入（P3-4）、增量备份（M6-5）、Parquet 生成器（M8-P3）、读写分离 demo 结论
  （be09a07）均已存在/评估——不作为"未完成"重复立项
- 本清单变更时同步更新 feature.md 对应行状态
