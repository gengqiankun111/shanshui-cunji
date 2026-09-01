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
- **后续**：分片构建工具 → 10 分片 10 亿构建
- **阶段 B/C/D**：倒排分片化验证、scale_out+raft_meta 接 RPC（raft 阶段二）、分片级可观测
- **触发**：阶段 A 立即（10 亿库前提）；B/C/D 随节点规模/硬件推进

## 已完成基线（勿重复）

- 排期大项 A~Y 全完成（development_process_order.md 第 2 章）
- development.md 7.x 至 7.63；problem_solving P1~P72
- SAGA 内核+网关+补偿协议+拓扑并行+对账（Ex-2/2.5/13.5/13.6/13.7）
- 1 亿库读优化（R/M/O）与写路径修复（syscall + 分批 + worker 单轮）
