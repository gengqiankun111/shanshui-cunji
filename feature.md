# 山水存迹数据库（shanshui-cunji）功能开发清单

> 分模块列出开发任务与完成状态。状态：✅ 已完成 · 🔄 进行中 · ⏳ 待办 · ⏸ 暂缓/评估
> 里程碑编号：M1~M6（development.md 第 11 章）、M7 深度优化、M8-P0~P9 前沿路线（Group Commit / 倒排过滤 / WAL 截断 / 批量导入 / fulltext / 分页 / 中文分词）。
> 维护：每个功能完成后更新本文件对应状态与提交号（与 development.md 7.x 同步）。

---

## A. 存储内核（WAL / MemTable / SSTable / Compaction）

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| WAL 预写日志（append 模式，崩溃回放） | ✅ | M1 |
| WAL 环形模式（预分配环形文件，回绕覆盖安全） | ✅ | M6-1 `66813c9` |
| WAL 截断回收（append 模式 flush 后截断 + 文件头持久化 seq） | ✅ | M8-P5 `a4d829a` |
| MemTable 跳表 + 双缓冲（Mutable/Immutable） | ✅ | M1 |
| SSTable 读写 + 块级压缩 + 分区布隆过滤（v5） | ✅ | M4 `e1eebce` |
| SSTable 两级索引（Level 1 常驻摘要 + Level 2 精确懒加载） | ✅ | M5 `8bcc077` |
| 基础 Compaction（全量合并，崩溃安全） | ✅ | P3-3 `3c48521` |
| Leveled-Compaction（L0→L1→L2 分层压实） | ✅ | M6-2 `4c2e17a` |
| IO 速率调度器（Token Bucket 限速） | ✅ | P3-2 `4884a58` |
| **scan 范围扫描流式化**（k-way merge，内存 O(page) 不随总量膨胀） | ✅ | M8-P10（`scan_stream` + 分页接入） |

## B. 写入路径 / 提交模型

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| Group Commit 组提交（提交器模式，写路径零 fsync） | ✅ | M8-P0 `648d9bd`（A 写重 45×：91,296 ops/s） |
| 批量导入模式（HotCache 跳过回填，防内存崩溃） | ✅ | M8-P6 `bde422d`（50M 导入 WS 4.9GB→0.6GB） |

## C. 倒排索引 / 全文检索

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| 倒排索引基础（内存字典 + 磁盘段 + FST 术语字典 / hash 引擎） | ✅ | M3 |
| 倒排架构升级：预分片 Chunk + 段 GC | ✅ | M5 `7db4764` |
| 位图索引（枚举字段白名单，COUNT/AND/GROUP BY 快速路径） | ✅ | M7-2 `4a19550` |
| 字段白名单 / 黑名单 / 长文本保护（max_term_len，防字典膨胀） | ✅ | M8-P4 `cde4f18`（字典压缩 45 万倍） |
| fulltext 分词索引（长文本可检索，`ft:field:token`） | ✅ | M8-P7 `545682f` |
| 中文 bigram 分词（中英混合文本检索） | ✅ | M8-P9 `72badfe` |
| **jieba 完整中文词典分词**（`cjk_segmenter`，语义词精确命中） | ✅ | M8-P13（`cjk-jieba` feature + `tokenize_seg`） |
| 倒排 posting 流式输出（大结果集已由分页解决） | ⏳ | 评估中 |

## D. 查询执行 / 缓存 / MVCC

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| 查询执行器 + 优化器静态路由（AccessPath 枚举） | ✅ | M2 |
| MVCC 快照读（get_at 按快照点过滤） | ✅ | M6-3 `07f556e` |
| MVCC 全局 seq 一致性（跨列族共享 seq 分配） | ✅ | M7-1 `4283568` |
| 热点缓存 HotCache（LRU/LFU + 保护区自动晋升） | ✅ | M6-4 `e34ea87` |
| HotCache 内存缺陷修复（stats 泄漏 / used_bytes 虚增 / LFU O(N) 风暴） | ✅ | P41 `5a937ea` |
| 查询分页（limit/offset/total，防大结果集内存爆炸） | ✅ | M8-P8 `45c3a54`（5M 命中 WS 10GB→221MB） |
| **scan 范围扫描流式化**（见模块 A） | ✅ | M8-P10 `516643f` |
| **scan 游标续扫**（after + 提前终止，全库遍历每页 O(limit)） | ✅ | M8-P11（`scan_after` + `/range?after`） |
| 类 SQL 解析（query::sql，降低迁移成本） | ⏳ | 阶段 1.5 规划，未启动 |
| 写入 Enrich（预连接） | ⏳ | 阶段 1.5 规划，未启动 |

## E. 数据管道 / 迁移 / 导入导出

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| 数据导出（CSV） | ✅ | M4 `e96c7c9` |
| 数据导入（CSV / JSONL，复用 migrate 内核） | ✅ | M4 `e96c7c9` |
| import-schema（预注册字段 + 倒排白名单 + 组合索引声明） | ✅ | M5 `35f87cd` |
| 增量导入（docid 游标断点续传） | ✅ | P3-4 `5085db8` |
| Parquet 数据集生成器 + 批量导入（5000 万 × 20 字段） | ✅ | M8-P3 `30b1639` |
| mysqldump 导入（MySQL 迁移） | ✅ | M1（migrate 工具） |
| 导出增强（增量 / Parquet / JDBC） | ⏳ | 阶段 2 规划，未启动 |

## F. 备份 / 一致性 / 外部缓存

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| 全量备份一致性准备（刷 WAL + MemTable + 倒排段） | ✅ | M4 |
| 增量备份 / 恢复（seq 游标，缺口检测） | ✅ | M6-5 `266d03d` |
| Redis 外部缓存（Cache-Aside + 写失效 + 熔断） | ✅ | M5 `da20c4c` |
| Redis 冷热分层 SDK 门面（读回填 + 双删协调） | ✅ | P3-6 `4f693bd` |
| 小表广播 JOIN（阈值判定 + 全量索引 / 回退点查） | ✅ | P3-5 `31fc054` |

## G. 分布式 / 网关（阶段 2）

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| 一致性哈希分片路由（128 虚拟点） | ✅ | M5 `dc17043` |
| 元数据中心 + 平滑扩容（只迁 ~1/N 虚拟分片） | ✅ | M5 `53bb924` |
| 分片节点 RPC + 网关（广播检索 Chunk 直拼） | ✅ | M5 `53bb924` |
| 主从复制（ReplicationLog + 游标推送） | ✅ | M5 `eca36c6` |
| 网关全局 Term 缓存（广播查询） | ✅ | M5 `eca36c6` |
| TDS 术语字典热备 + 无损扩容协议（双写→追平→切换） | ✅ | M5 `fda44a6` |
| 物化视图调度器（Count/Sum/Avg + 增量刷新） | ✅ | M5 `8bcc077` |
| 四层看门狗（写停滞检测 + 心跳 Sidecar） | ✅ | M5 `df8e9d4` |
| 读写分离（Mutex/RwLock/COW 快照读） | ⏸ | M8-P1 `be09a07` demo 结论暂缓（组提交已解决读被写拖垮） |
| 高并发查询优化（design 9.5 目标） | ⏳ | M6 后留待 |

## H. 运维 / 质量 / 性能工具

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| admin status / QueryRegistry / explain 推演 | ✅ | M4 `e96c7c9` |
| 配置热加载（reload 校验 + 变更区块报告） | ✅ | P3-1 `69b39dc` |
| 全局分配器（mimalloc 默认 / jemalloc 可选） | ✅ | M4 `b0eaa58` |
| YCSB 压测工具（负载 a/b/c/f + 分位数） | ✅ | M7-3 `d918c47` |
| 质量文档体系（quality_system P1~P41 / problem_solving） | ✅ | 持续维护 |
| 三规模性能实测（1000万/2000万/5000万 + 截屏存档） | ✅ | v0.1.0~v0.5.0 发布系列 |

## I. 前沿探索（frontier）

| 任务 | 状态 | 里程碑 / 提交 |
|---|---|---|
| 前沿调研（BVLSM/RusKey/DobLIX/TieredKV/AuraDB） | ✅ | M7-3 `d918c47` + frontier-research-2026-08.md |
| 环形 WAL 头部 tail 合并 fsync（sync 单次原子提交） | ✅ | M8-P12（ring+gc 68,756 ops/s，2.3×） |
| 读写分离 / 双写加速 | ⏸ | 评估中 |
| 倒排 posting 压缩（Roaring 已用，Gorilla/变长探索） | ✅ | 探索验证：Roaring 已达理论下限（密集 0.13B/docid=1bit，稀疏 2B/docid 为 delta 2×，但 Roaring AND 快 20×）——维持 Roaring 不引入新编码 |

---

## 近期里程碑（按完成顺序）

- **M8-P9 中文 bigram 分词**（`72badfe`）：tokenize 字符类分段，中文 fulltext 可检索（2-4 字关键词 bigram AND 精确命中）
- **M8-P10 scan 范围扫描流式化**（`516643f`）：k-way merge（BinaryHeap 最小堆 O(N log K)），
  `scan_range_paged` 内存 O(page)——50M 库全库分页查询 WS 691MB（旧实现全量收集会 OOM）
- **M8-P11 scan 游标续扫**：`scan_after` + `/range?after`——全库遍历每页 O(limit)，
  50M 库翻页 164-682ms（旧 total 模式全库 70s）
- **M8-P12 环形 WAL 头部 tail 合并 fsync**：sync 单次原子提交（消除冗余第二次 fsync）——
  ring+gc 2ms 68,756 ops/s（M8-P1 基线 30,270 → 2.3×）
- **M8-P13 jieba 完整中文词典分词**：`[inverted] cjk_segmenter="jieba"`（`cjk-jieba` feature，
  词典嵌入默认开）——中文语义词精确命中（"数据库"单 term），索引词数 ≤ bigram 碎片
- **倒排 posting 压缩探索**（demo 验证，不集成）：Roaring 密集容器 1bit/docid 已达理论下限
  （16.6M 连续 0.13B/docid），delta-varint 1B/Gorilla 2B 均更差；稀疏 1% 场景 Roaring 2B/docid 为
  delta 2×（绝对差 ~0.5MB/500K docid），但 Roaring AND 查询快 20.1×（337us vs 6.7ms）且库成熟
  → **维持 Roaring，不引入新编码**

## 下一候选

- 倒排 posting 流式输出（大结果集已由分页解决，仍可评估）；类 SQL 解析 / 写入 Enrich / 读写分离（⏸）