# development_remain.md —— 开发未完成任务

> 从 development.md / development_extension.md / problem_solving.md 提取的**未完成任务**
> （development_extension.md 中 Ex-5.x/6.x/7.x 的 `[ ]` 为**过时状态**——实际已落地，
> 见 development.md 7.24~7.45 章节，不列入本清单）。
> 当前基线：466 测试全绿（ae41d60）。

## 1. 导出增强剩余（开发进行中项）

- **来源**：feature E 模块 🔄 / design 20.5 / export.rs
- **已完成**：CSV 全量、Parquet 全量（`--parquet`，70c3b30）、**增量导出**（`--incremental --checkpoint`
  docid 游标断点续传，2174531——首轮全量记 max、后续只导新增、断档自动全量重建）、**流式管道**
  （`--filter/--project/--mask` Filter+Projection+Sink 分叉 + `--rate-limit`，c6b5417）、
  **JDBC 直连**（`--jdbc mysql://...` MySQL wire 客户端建表+批量 INSERT 无文件落盘，c6b5417）、
  **MySQL 兼容 CSV 配套**（`--mysql-compatible` CREATE TABLE + LOAD DATA INFILE SQL +
  `--mysql-max-varchar`，313bd81）、**建表 DDL**（`--dry-run-schema` ClickHouse/MySQL，313bd81）、
  **与 Compaction 共享后台 IO 优先级**（`--io-rate-limit-mb` scan_limiter Token Bucket，40e8abb）
- **待做**：无——**导出增强（design 20.5）全部完成**（✅）

## 2. 合并阻塞根治：无锁合并（✅ 完成 af24dbd + 3d58137）

- **来源**：P72 / P73 / development 7.65
- **已落地**：syscall 风暴修复（96ac6bc）、分批合并（1763554）、worker 单轮（9e77872）、
  **无锁合并根治**（af24dbd：MemTableBuffer RwLock &self 化 + CF sst_mutate + Engine Arc 化 +
  worker clone CF Arc 无锁合并）+ **P73 manifest 竞态修复**（3d58137：persist 内存快照 +
  store→persist→remove 原子）+ 回归测试（5de5ab0）；1 亿库实测合并期写不塌陷；469 全绿

## 3. Ex-1.5 与 M5 双写扩容协议衔接（✅ 完成 7.76 `8f8c1ea`）

- **来源**：development_extension Ex-1 剩余 / development 7.43
- **已落地**：扩容编排协调器 `src/scale_out.rs`——ADDING→CATCH_UP→DRAIN→SWITCH→DONE
  状态机（防跳步/终态拒绝）+ **回滚预案**（路由不切换/新节点摘除/幂等）+ 状态持久化崩溃续跑；
  "双写→追平→切换"改造为 **"本地事务写 + outbox 待办 + 排空校验"**（业务只写主节点，
  outbox 消息同 seq/fsync 本地原子；追平 = dispatch_outbox → RPC repl.apply 幂等应用到
  新节点；排空校验 = outbox_drained + 数据一致性抽样，**未排空禁止切换**）
- **衔接**：meta.rs 路由（switch 提升新节点 master / rollback 回退）+ replication.rs
  repl.apply 幂等 + outbox.rs 幂等消费；生产接 RPC，测试进程内双 Engine
- **回归**：494 全绿（+6：scale_out 状态机 5 + engine e2e 1）

## 4. io_uring Linux 部署实测

- **来源**：V 项 / development 7.63 指引
- **状态**：✅ **已完成**（7.71）——热路径接入（SSTable 块读 + WAL fsync 走 SQPOLL）+ 阿里云
  Debian 12 / 内核 6.1 实测：io_uring 池初始化成功、核隔离生效（SQPOLL 3 线程绑核 0、业务核 1）；
  2 核小机器写路径 -13%（SQPOLL 占核）、点查/扫描持平（块缓存主导）→ io_uring 保持默认关，
  多核 NVMe 生产环境按 7.63 指引开启

## 5. HotCache 内部锁粒度（读写分离收尾，✅ 完成 7.72 `9071984`）

- **来源**：feature I 模块"读写分离"行剩余项（HotCache 内部 Mutex 粒度）
- **已落地**：整包 `Mutex<HotCache>` → 内部 `RwLock`（cache/protected/used_bytes/promotions）+
  `DashMap`（stats 无锁计数）；读路径 `peek` 持读锁读读并行、计数无锁；写路径
  put/invalidate/promote 持写锁；`get` 达热点阈值先释放读锁再写锁 promote（幂等）；engine
  调用点去外层 lock()（get/batch_get/scan 回填、put 回填、invalidate 直接 &self 调用）
- **实测**（demo A/B `src/demo/hotcache-rw`，4 读线程热 key 全命中）：纯读 OLD 80.6 万 qps →
  NEW 335.6 万（**x4.16**）；混合负载（写线程节流 ~3 万写/s put+invalidate）读吞吐 436.9 万
  （**x5.42**，写不再拖垮读）
- **回归**：482 全绿（含 hotcache 新增 2 并发测试）

## 6. 文档维护（工作流收尾，✅ 完成 7.75）

- **来源**：development_extension.md
- **已完成**：Ex-5.1~5.10 / Ex-6.2~6.4 / Ex-7.1~7.4 checkbox 全部回填 `[x]` + 提交号
  （056b21d/c7ebe72/d38e8ab/624ce9e/4974ef3/e615071/442981c/cd00d85/ba709e2/e6a5610 /
  c8183cf / c5fa66c/b294532/fd0b519/ddbc20e）；Ex-2.5 补提交号 `781199e`；
  Ex-6.4 验证重试率 0.015% 记录——文档与代码/development.md 7.24~7.45 对齐

## 7. 远期开发（触发后执行，见 design_remain）

- Calvin 阶段一/二/三（gseq 分配器 → 全局复制日志 → raft 高可用）——触发条件（13.3.1）；
  **元数据 raft 阶段一已落地**（7.77 `e2a76a0`：自动 failover + 脑裂安全），阶段二
  （Calvin gseq raft 联动 + RPC 接线）依赖 Calvin 落地
- 增量备份已有（M6-5），增量导出待做（见 §1）

## 8. 倒排段 GC 后台化（排期 J 项，✅ 完成 7.73 `b76dd40`）

- **来源**：development_process_order J 项（P2）——原"gc() 需显式调用；后台线程周期触发"
- **已落地**：InvertedIndex `next_seg_id → AtomicU64`、`flush_segment`/`gc` 改 `&self` +
  `mutate: Mutex<()>`（与写路径 flush 序列化 Manifest 写/删段文件，防丢失更新）；
  `read_segment_posting` 读段 NotFound 跳过（后台 GC 与查询并发）；Engine `inverted` Arc 化 +
  `inverted_gc_pending` 信号（flush_inverted 刷盘后置位）；mysql 后台 GC worker
  （信号 + 10 分钟兜底，读锁内 clone Arc 无锁执行 gc，不阻塞查询）
- **回归**：485 全绿（+3：并发 flush/gc 无丢段、gc &self、worker 收敛段数）

## 9. fulltext 大 posting 反序列化优化（排期 K 项，✅ 完成 7.74 `ed2588d`）

- **来源**：development_process_order K 项（P2）——原"5000 万库 content 词 posting ~1600 万，
  首次反序列化 ~100ms+；候选：段内 posting 分块延迟加载"
- **已落地**：段格式 v2→v3 posting 分块布局（容器头索引 + 独立容器字节，值存完整 docid
  容器级对齐）；`search_paged`/`doc_count` 惰性游标 k-way merge（分页只解码窗口容器、
  COUNT 精确跨段去重）；engine search_term_paged/fulltext 分页走快速路径；旧段 v2 兼容读取
- **实测**（demo posting-chunk，1600 万 posting）：近页 x211、COUNT x4491、全量分块解码
  与紧凑持平（x1.0）、数据仅 +0.1%
- **回归**：488 全绿（+3：分页与全量一致、跨段去重、v3 往返）

## 10. 10 亿库扩展（设计 `design-10b-extension.md`，阶段 A P0）

- **来源**：2026-09-02 规划（10 亿库分片扩展方案）
- **阶段 A（P0）— ✅ 已完成**：全局 docid 分配器 `src/docid_alloc.rs`（分片前缀
  `docid = shard_id<<40 | local_id`，路由 O(1)、分片内 AtomicU64 自增无集中瓶颈、扩容归属不变、
  40 位溢出保护）；demo `demo/docid-alloc` 5 测试 + kernel 7 单测，507 全绿（development.md 7.81）
- **后续**：分片构建工具 `src/shard_build.rs` + `shanshui-cunji-shard-build`（✅ development.md 7.82）：
  行号取模均匀分配/显式主键前缀路由/--shard-id 多进程并行/BOM 修复，513 全绿
- **阶段 B（倒排分片化验证）— ✅ 已完成**（development.md 7.83）：
  分片内倒排存 local_id（Roaring 32-bit，存储格式零改动），跨分片前缀组合全局 docid
  （`src/shard_inverted.rs`：ShardedInvertedSearch 广播合并唯一 + 跨分片惰性分页窗口 +
  真实 Engine 集成验证），520 全绿
- **待验证**：10 分片 10 亿构建（验收：构建 ≤60 分钟、点查 10 万+ QPS）+ 亿级 posting 规模回归
- **阶段 C（raft RPC 接线）— ✅ 已完成**（development.md 7.85）：
  `src/raft_rpc.rs`——RaftMsg serde + RaftTransport trait（LocalRaftTransport 进程内/
  TCP 接线点）+ RaftNodeRuntime（选举/日志复制/自动 failover/多数派，MetaOp 复制到
  MetaCenter 状态机），525 全绿
- **阶段 D（分片级可观测）— ✅ 已完成**（development.md 7.86）：
  `src/shard_metrics.rs`——分片 docid 水位 gauge + 读写计数 + 80%/90% 上限预警 +
  /metrics 集成（`shanshui_shard_docid_alert`），Engine 挂载全链路，530 全绿
- **阶段 A~D 全部完成**；剩余：10 分片 10 亿构建验收（硬件）+ raft TCP 传输/扩容编排联动（部署推进）
- **触发**：阶段 A 立即（10 亿库前提）；B/C/D 随节点规模/硬件推进

## 11. LSM 读路径范围查询优化（Ex-8 系列，✅ 已排期 2026-09-03）

- **来源**：design_remain §7 / 5000 万 MySQL 对照（范围 10m 18.8× → 50m ~102×）
- **前置分析**：根因 = 收集路径线性索引税 + 流式路径无层/文件剪枝 + 全值解码/无块缓存
  （非缺 B+Tree；3308 实验：非事务收集 67~101ms 位置单调劣化 vs 事务流式恒 ~5ms）
- **Ex-8.1（P0）非事务 id BETWEEN 改走流式窗口扫描**：
  - [ ] mysql.rs 非事务 `id BETWEEN` 拦截点（1856-1878）由 `engine.scan_range`（收集路径：
    逐 SST `sst.scan_range` 线性走块索引 + `index()` 全量 clone + HashMap 合并排序）改走
    `engine.scan_stream` 窗口（SstRangeIter 二分定位起始块 + Zone Map 只读相交块 + 逐条回调收集）
  - [ ] **前置正确性修复（demo range-window 发现，切换前提）**：
    ① 流式 merge 折叠**同源同 key 旧版本**（覆盖写未收敛刷盘：收集 100 行 vs 流式 110 行，
    demo `finding_multi_version_within_source_not_collapsed` #[ignore] 待修复后启用）；
    ② 删除位图消费语义：scan 收集/流式两路径均不查位图（get 不可见但扫描返回已删 docid），
    需定语义（对齐位图过滤 或 文档注明），demo `equiv_delete_multi_sst` 已记录观察
  - [ ] 语义对齐：与收集路径同结果（窗口闭区间、无快照实时语义一致）+ SUM/COUNT/ORDER BY 分支保持
  - [ ] 边界：空窗口 / 窗口跨 SST 文件边界 / 单文件内多块组 / 与删除位图/tombstone 交互
  - [ ] demo ✅ 完成（src/demo/range-window，2026-09-03：6 passed + 1 #[ignore] 记录分歧；
    memtable-only/单 SST/多段压实/删除/重开两路径全等价 + perf 对照观测）
  - [ ] 预期：50m 范围 p50 86ms → ~5ms（与事务流式持平）；1 亿/更大库线性税消除
- **Ex-8.2（P0）scan 路径层 + 文件 key 范围剪枝**：
  - [ ] `scan_stream_at` / `scan_range` 复用 `layer_ranges/layer_indices`（现仅点查路径消费）：
    窗口 [start,end] 只构造相交 SST 的 SstRangeIter；memtable 按 docid 窗口 with_iter_range 已具备
  - [ ] 全扫（无窗口）语义不变（全部文件照旧）
  - [ ] demo + 单测（剪枝 vs 全建迭代器同结果；多段/跨层窗口正确）
- **Ex-8.3（P1）扫描路径块缓存 + keys-only 投影**：
  - [ ] SstRangeIter/scan_range 读块挂既有 blockcache（当前只点查 get_from_sst 挂载）
  - [ ] 纯 `SELECT id` 窗口扫描走 keys-only 解码（7.100 `decode_data_block_keys` 模式扩展到
    有界窗口 + 行式格式值跳过；PAX 回退）
  - [ ] demo + 单测（keys-only 窗口 vs 全解码同结果；热点窗口重复读块缓存命中）
- **Ex-8.4（远期，不主动排期）**：L1/L2 B+Tree 存储替换（触发条件见 design_remain §7.2）
- **验收**：Ex-8.1~8.3 落地后 50m 范围查询对照复测（tmp_bench_mysql_vs_scc_50m.py + probe），
  目标 MySQL 差距 100× → 个位数×；全量回归全绿后按工作流提交 + 回填 feature_remain / development.md 7.x

## 12. MemTable/写侧与倒排提案采纳候选（design_remain §8，2026-09-03 排期）

> 分片跳表 / L1+B+Tree 存储 / 16KB 默认块 / TRIM / 大页 NUMA / 倒排回表 B+Tree 化等已否决或平台远期，
> 见 design_remain §8.1。本清单 = 采纳候选（每项 demo-first 验证后实施）。

- **Ex-8.5（P2）并行 Flush**：
  - [ ] demo：单线程 vs 分片并行写 SST fragment（imm memtable 按 key 段切分，多 worker zstd + 写段，
    顺序合并 fragment）对照（复用 Ex-7.2 绑核思路）
  - [ ] 与合并背压（P 项 auto_compact）/ io_uring 队列关系评估；memtable 256MB 上限下频次实测
- **Ex-8.6（P2）段级 min/max seq 元数据 + 快照读整段跳过**：
  - [ ] 段元数据加 min_seq/max_seq（manifest/SST footer 版本兼容）；get_at / scan_range_at 剪枝
    （快照 seq ≤ 段 min_seq → 整段跳过，仅读历史快照时生效）
  - [ ] demo + 单测（快照读与全读一致；多段混合 seq 正确性；对 RR 长事务读放大收益）
  - [ ] 与 Ex-8.2 文件范围剪枝互补（key 范围 + seq 范围双维剪枝）
- **Ex-8.7（P3）删除密度 urgency**：
  - [ ] compaction_urgency 增加删除密度维度（位图置位率/段内删除比例加权），删除密集段优先合并释放空间
  - [ ] demo：删除密集 vs 均匀负载下空间回收/读路径段数收敛对照
- **Ex-8.8（P2）posting LRU 双区热点化 + 参数化**：
  - [ ] 现有 posting LRU 256 项（c380792）仿 HotCache 双区（普通 + promote 保护区）热点化 + 容量配置化
  - [ ] demo：热点/冷 term 交替负载命中率与内存预算对照（无需新缓存结构）
- **Ex-8.1 前置正确性修复**（流式 merge 折叠同源同 key + scan 删除位图语义）→ 见 §11 Ex-8.1 前置项，
  随 Ex-8.1 内核实现一起落地（demo range-window 已记录）

## 13. 全局纪元 + 多文件 WAL（评估结论：不立项，design_remain §9）

- **判定**：❌ 当前不立项（单 NVMe 并发 fsync 无增益；组提交/flushed_seq/manifest 已覆盖序号+水位语义；
  因果风险因单 docid 本地事务天然不存在；真多设备场景由 Ex-5.10 条带化承担）
- **远期触发项（满足后复评）**：无锁多写者 + 每写者独立 WAL + NVMe 多队列/多设备——前置 = 解除引擎写侧
  串行（O 项仅读并行，写仍单锁）+ 实测吞吐先定位写锁/倒排/合并瓶颈（A 项写放大）确证 WAL fsync 为天花板
- **验收口径（若复评）**：manifest 持久化各文件 max_flushed_epoch + 恢复按水位（禁止逐号扫缺口）；
  与删除位图 flush 序（先位图后 WAL）保持；增量备份/环形 WAL 截断交互回归

## 已完成基线（勿重复）

- 排期大项 A~Y 全完成（development_process_order.md 第 2 章）
- development.md 7.x 至 7.63；problem_solving P1~P72
- SAGA 内核+网关+补偿协议+拓扑并行+对账（Ex-2/2.5/13.5/13.6/13.7）
- 1 亿库读优化（R/M/O）与写路径修复（syscall + 分批 + worker 单轮）
