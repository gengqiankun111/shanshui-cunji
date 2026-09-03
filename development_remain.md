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

## 10. 10 亿库扩展（阶段 A~D 已完成，见 process_order AD；设计并入 design_remain §19 归档）

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
- **Ex-8.1（P0）非事务 id BETWEEN 改走流式窗口扫描** — ✅ 内核完成（e63603a，553 全绿）
  - [x] mysql.rs 非事务 `id BETWEEN` 拦截点（1856-1878）由 `engine.scan_range`（收集路径）改走
    `engine.scan_stream` 窗口（SstRangeIter 二分定位起始块 + Zone Map 只读相交块 + k-way merge）
  - [x] **前置正确性修复**：① 流式 merge 折叠同源同 key 旧版本（scan_stream_at 线性/heap +
    count_keys_range 三处 frontier 吞并，demo finding #[ignore] 待启用）；
    ② 删除位图语义对齐（engine.scan_range/scan_stream/count_all_docs 过滤位图已删，put 复活）
  - [x] 语义对齐（SUM/COUNT/ORDER BY 分支保持）+ 边界（空窗口/越界/跨文件/删除）
  - [x] demo ✅（src/demo/range-window：6 passed + 1 #[ignore] 记录分歧）
  - [x] **50m 验收（2026-09-03，release 3308 复测）**：非事务 id BETWEEN p50 中位
    **86ms → 3.1ms**（位置无关 2.96~3.43ms；MySQL 3306 0.84ms → 差距 **~102× → 3.7×**，目标"个位数×"达成）
- **Ex-8.2（P0）scan 路径层 + 文件 key 范围剪枝** — ✅ 内核完成（49469f6，554 全绿）
  - [x] `scan_stream_at` / `count_keys_range_filtered` / `scan_raw_range` 复用段级 `key_range`
    （sst_intersects_window，闭区间/None 端零假阴性）：窗口 [start,end] 只建相交 SST 的
    SstRangeIter（免非相交段 L2 定位/索引读与线性走查）；memtable 按窗口 with_iter_range 已具备
  - [x] 全扫（无窗口）语义不变（全部文件照旧）
  - [x] demo/单测（scan_prunes_disjoint_ssts_windows：3 不相交文件 × 8 窗口收集==流式 +
    全扫/跨文件边界/越界空窗口）
- **Ex-8.3（P1）扫描路径块缓存 + keys-only 投影** — ✅ 完成（Part A 0d8fdcf + Part B bbb1c7a，556 全绿）
  - [x] Part A：SstRangeIter 可选块缓存（new_cached/new_keys_cached，与点查同 key=文件+块
    offset）——读块写穿 + 全组命中免磁盘 IO/解压；scan_stream_at / count_keys_range_filtered
    挂 CF block_cache（此前仅点查挂载）
  - [x] Part B：CF::scan_stream_keys（keys 流式：折叠 + Tombstone 跳过 + 早停，SST 端
    new_keys_cached 免值解码）+ Engine::scan_stream_ids（位图过滤）；mysql 非事务 id BETWEEN
    纯 `SELECT id`（无聚合/ORDER BY）改走 keys-only
  - [x] 单测：scan_block_cache_warm_repeat_consistent + scan_stream_ids_matches_scan_stream
    （覆盖折叠/删除位图/多文件/越界窗口 keys-only 与全值 docid 集一致）
- **Ex-8.4（远期，不主动排期）**：L1/L2 B+Tree 存储替换（触发条件见 design_remain §7.2）
- **验收**：Ex-8.1~8.3 ✅ 全部落地（e63603a / 49469f6 / 0d8fdcf+bbb1c7a）；
  50m release 复测（Ex-8.1 已验证 86→3.1ms；Ex-8.2/8.3 后范围 p50 与重复窗口命中待复测）

## 12. MemTable/写侧与倒排提案采纳候选（design_remain §8，2026-09-03 排期）

> 分片跳表 / L1+B+Tree 存储 / 16KB 默认块 / TRIM / 大页 NUMA / 倒排回表 B+Tree 化等已否决或平台远期，
> 见 design_remain §8.1。本清单 = 采纳候选（每项 demo-first 验证后实施）。

- **Ex-8.5（P2）Flush 频率优化 — ✅ 档位 A/B 完成（2026-09-03，不做并行 flush）**
  - [x] **零成本档位 A/B（memtable 256 vs 512MB，50m 库 orders 追加灌入 400 万行/档，
    组提交 2ms，逐 2s 打点）**：256 → 61.5s / **65,019 rows/s**；512 → 60.7s / **65,916 rows/s**
    （+1.4%，噪声级，无显著差异）
  - [x] 停顿观察：两档均存在一次 ~5~9s 吞吐 dip（出现在相近累计写入量 ~2.87M 行处）——
    增大 memtable 未消除停顿 → **flush 非写吞吐瓶颈**（dip 更可能来自倒排内存段刷盘/
    其它周期性后台写，与 memtable 档位弱相关）；256 档起跑还背负 ~1GB 历史 WAL 回放滞留劣势，
    持平结果更证 512 无增益
  - [x] 结论：**维持默认 256MB，不做并行 flush**（Ex-8.5 修正路径确认——档位实验证实 flush 非瓶颈，
    并行 flush 仅推高 L0/compaction 压力）；数据打点 tmp_ex85_ticks_{256,512}.txt / 脚本 tmp_ex85_load.py
- **Ex-8.6（P2）段级 min/max seq 元数据 + 快照读整段跳过** — ✅ 完成（29be7b1，560 全绿）
  - [x] CF `seq_min` 惰性记忆（path → 文件最小行 seq；keys-only 一次性推导 + 缓存）——
    **无需 manifest/footer 扩展**：重启后首次快照读自动重建（惰性按需）
  - [x] 剪枝点：get_bytes_at（快照点查/事务点查）+ scan_stream_at（快照范围/事务扫描）——
    `快照 < 文件最小行 seq` 整段跳过（含 put/Tombstone 掩蔽语义，无假阴性；仅历史快照读生效）
  - [x] 与 Ex-8.2 文件范围剪枝互补（key 范围 + seq 范围双维剪枝）
  - [x] 单测 seq_prune_snapshot_reads_stable_across_reopen（旧快照对新文件不可见 + 重开惰性重建一致）
- **Ex-8.7（P3）删除密度 urgency** — ✅ 完成（a6d53c3，565 全绿）
  - [x] compaction urgency 增加删除密度维度：Engine 级**净置位计数**（bitmap mark/clear 返回"是否实际翻转"，
    幂等重删不重计 / 复活即减 / 打开基准 = 位图既有置位数）+ `max_docid` 分母 → 位图置位率；
    就绪门槛 = 置位率 ≥ `delete_density_min_ratio`（默认 0.10）**且**自上次 GC 新增置位 ≥
    `delete_density_min_docs`（默认 1000；重启后历史置位不重复触发整段重写）
  - [x] 三处挂载：`Engine::needs_compact` 追加 `delete_garbage_pending`；`compact` / `compaction_targets`
    主列族紧迫度 **+DD_URGENCY(6)**（介于 L0 大小软阈值 +8 与段数主因子 ×10 之间——收敛后 L0=0 的
    删除密集主列族压过空闲 delta/cidx 率先被合并，又不抢占真正 L0 段数压力档）
  - [x] 收敛单段 GC 重写：`ColumnFamily::compact_gc(drop_key, allow_single)`——常规 select 无多段候选
    （已收敛单底层段）时全量重写物理丢弃已删键（绕开 Ex-5.8 块级复用：元数据拼接无法丢键）；
    `CompactReport` 增 `dropped_keys` 回写排空状态（drop>0 → 继续排空；0 丢弃 → 收敛快照 done=marked）
  - [x] 单测 ×4（含 demo 对照）：compact_gc 单段重写物理丢弃 + 二次 0 丢弃收敛 + 重启一致；
    置位率×min_docs 双门槛边界；4000 行 33% 删除收敛后 GC 排空（总丢弃=删除数、空间回收、语义、
    重启不重复触发）；demo：删除密集(-50%) vs 均匀(-2%) 同载入量下空间回收对照
  - 已知取舍：GC 再触发 = 新增置位 ≥ min_docs（排空 0-丢弃轮中断）；垃圾高度集中于少数小段而大段
    先被选中清空时可能留尾——后续写波 structural 合并仍按位图过滤回收（有兜底，见 problem_solving）
- **Ex-8.8（P2）posting LRU 双区热点化 + 参数化** — ✅ 内核完成（8f70b3e，561 全绿）
  - [x] 单 LRU(256) → PostingLru 双区（Segmented/2Q：protected 60% + probation 40%，
    POSTING_CACHE_CAP 总量参数化）：命中提升保护区免被低频突发逐出；新 term 只入普通区（满逐冷）；
    写路径清空两区语义不变
  - [x] 单测 posting_lru_dual_zone_protects_hot_terms（热点冷突发存活 + 有界性 + 清空）
  - [ ] demo（可选）：热点/冷 term 交替负载命中率与内存预算对照；容量接 config 文件（当前 const 参数化）
- **Ex-8.1 前置正确性修复**（流式 merge 折叠同源同 key + scan 删除位图语义）→ 见 §11 Ex-8.1 前置项，
  随 Ex-8.1 内核实现一起落地（demo range-window 已记录）

## 13. 全局纪元 + 多文件 WAL（评估结论：不立项，design_remain §9）

- **判定**：❌ 当前不立项（单 NVMe 并发 fsync 无增益；组提交/flushed_seq/manifest 已覆盖序号+水位语义；
  因果风险因单 docid 本地事务天然不存在；真多设备场景由 Ex-5.10 条带化承担）
- **远期触发项（满足后复评）**：无锁多写者 + 每写者独立 WAL + NVMe 多队列/多设备——前置 = 解除引擎写侧
  串行（O 项仅读并行，写仍单锁）+ 实测吞吐先定位写锁/倒排/合并瓶颈（A 项写放大）确证 WAL fsync 为天花板
- **验收口径（若复评）**：manifest 持久化各文件 max_flushed_epoch + 恢复按水位（禁止逐号扫缺口）；
  与删除位图 flush 序（先位图后 WAL）保持；增量备份/环形 WAL 截断交互回归

## 14. "量变优化"采纳候选（design_remain §10，2026-09-03 排期）

- **Ex-8.10（P1，正确性收尾）事务扫描删除位图过滤** — ✅ 完成（e1ae41b，558 全绿）
  - [x] `scan_range_txn` 快照视图先排除位图已删 docid（与 txn_get/get_at 对齐，置于 read_own 覆盖前）；
    补 write_set 中未出现的自写 Put（新 docid / 已删复活）按窗口并入 + 保持升序
  - [x] 单测 txn_scan_respects_deletion_bitmap_revival_and_insert（位图排除 / 复活 / 新插入 / 与 txn_get 一致）
  - [x] 注：位图删除为非版本化全局语义（get_at 同近似）——快照不晚于删除时点亦隐藏，属既有取舍（已注释）
- **Ex-8.9（P3）空闲感知维护调度**：
  - [ ] 负载信号：读/写计数（metrics）、CPU/IO 等待、L0 段数；低负载窗口（QPS<峰 20% 且 CPU<40%）：
    收紧合并阈值（urgency 权重提高）、触发倒排 GC、深度合并 L0→1、执行删除位图脏页回收；
    高负载退避（与 Ex-7.4 动态限流联动）
  - [ ] demo：交变负载（峰/闲）下合并与回收的波峰转移对照
- **Ex-8.13（P3）倒排后台 IO 预算共享**（design_remain §13 矛盾③收尾）：
  - [ ] 倒排 flush_segment/GC 写纳入既有后台 io_limiter（与 compaction/导出共享后台预算语义），
    或并入 Ex-8.9 空闲窗口执行——消除与 compaction 的前台/后台 IO 争用
  - [ ] demo：合并 + 倒排 GC 并发窗口的写/读延迟对照
- **已标注不新立项**：后台预热（P3 可选 Linux 门控）、scan IO 合并预读（SCAN_GROUP=8 已落地，
  增量并入 Ex-8.2/8.3）、熔断（看门狗+cap 已具备）、零拷贝（并入 Ex-8.3 keys-only 投影）

## 15. L1/L2 延迟大合并实验（Ex-8.11，design_remain §11，2026-09-03 排期）

- **Ex-8.11（P2，受控实验，不直接改默认）L1/L2 独立触发阈值**：
  - [x] 内核 ✅（8ec3a70，557 全绿）：配置 `storage.l1_trigger_files/l2_trigger_files`（默认 0=现行为）
    + needs_compact.bottom_needs_compact / select_inner_ex 门控（L1 攒批下沉、L2 攒批收敛、
    延迟期不提前收敛 L2）+ L0 活跃纳入合并上限用 l1_trigger_files
  - [x] 单测：select 延迟用例（L1=3<4 不下沉 / 攒 4 下沉 / L2 攒批收敛）+ engine 收敛护栏+数据完整
  - [ ] A/B 写放大实测（50m 库，现收敛 vs l1 攒 8~12）：写放大/点查 p99/范围 p50/空间放大/合并 CPU
  - [ ] 注意：L0 段数不放宽（overlap 层）；"L0 二分候选段索引"仅当未来放宽 L0 才启用
  - [ ] 验收口径：写放大 -30%+ 且范围 p50 无回退即采纳调默认
- **已标注不新立项**：L0 全局键范围索引（R 项 Zone Map 已落地）、主动异步压 L0（P/O 项已落地）、
  文件系统式存储（质变否决）

## 16. 分层压缩实验（Ex-8.12，design_remain §12，2026-09-03 排期）

- **Ex-8.12（P2，受控实验）分层压缩（L0/L1 热档 ↔ L2+ 冷档）**：
  - [x] 新增配置（沿用现有 compression/compression_level 语义）：`sstable.compression_level_l2`
    （0 = 不分层；>0 即 L2+ 冷档），CF `compression_level_for(out_level)` 按输出层选档
    （flush→L0 与 L0/L1 输出热档；L2 输出 = L1→L2 下沉 / L2 内合并 / L2 单段重写 用冷档）——✅ 内核完成（b25b86d）
  - [x] 风险核查：Ex-5.8 数据块级复用跨层失效 → meta_only 跨档门控（分层启用且 L0/L1↔L2
    跨档合并回退全量重压缩，保层档语义）——✅ 已处理（b25b86d，测试覆盖跨档回退路径）
  - [ ] demo/50m 库 A/B：空间（数据目录字节）、写放大（合并次数/重写字节）、范围 p50/点查 p99、
    压缩 CPU；验收：L2 zstd 高等级不劣化范围读（解压近等级无关）且空间 -10%+ 即采纳默认
- **已标注不新立项**：共享字典压缩（远期触发：Ex-8.12 后空间仍瓶颈再评估，ScyllaDB 式基建）

## 17. 自适应多级 Block 索引（评估结论：不采纳，design_remain §14）

- **判定**：❌ 不立项——块级 zstd 使行级细索引无法跳过整块解压（关键前提错误）；热路径已被
  BlockCache/HotCache/Ex-8.3 覆盖；mmap 数据面不适用 LSM 写/compact 场景
- **记录备选（P3 微项，不主动排期）**：行式块内稀疏重启点/二分加速块内 scan（当前块内扫描 ~us 非瓶颈，
  实测为瓶颈再评估）

## 18. 删除位图读路径无锁化（Ex-8.14，design_remain §15，P2 排期）

- **Ex-8.14（P2）deletion_bitmap 读无锁化** — ✅ 完成（2c623d9，559 全绿）
  - [x] bits RwLock<Vec<u64>> → bytes ArcSwap<Box<[AtomicU8]>>：is_deleted 快照 + 单字节原子读
    （无 RwLock/无互斥，删除/扫描/计数并发互不阻塞）；写路径短 Mutex 串行 + 越界倍增扩容换代
    （in-place 位翻转原子）；flush/deleted_count 改快照原子读
  - [x] 文件格式与持久语义不变（4KB 页对齐脏页 fsync + WAL 回放重建）
  - [x] 单测：bitmap 5 项全过 + 新增并发冒烟（4 读者 vs 写者翻转+扩容，最终态全量校验）
- **已复核不新立项**：写路径阶段化提交（组提交已把 fsync 移出锁内；后台 ack 破坏同步耐久语义）、
  MemTable 切换（双缓冲已具备）、多写者引擎写锁拆分（远期 Ex-13 触发链）

## 19. 聚合 / COUNT 加速候选（2026-09-03，`D:\traeprojs\cunji\统计.txt` 评估 → 排期）

> 触发：规模测试（2026-09-03，async 3308）3 亿库 `COUNT(*)` 194s / `COUNT status='active'` 197.8s /
> `SUM amount>90000` 214s（全扫）。外部建议"写入时维护 total_count + 倒排 TermMeta 统计载荷"。
> 代码核实后分期如下：

| 项 | 现状（代码核实） | 判定 |
|---|---|---|
| `COUNT(*)`（无条件） | Engine `count_all_docs` 全集合 key-only 全扫（3 亿 ~194s） | 慢，需快路径 |
| `COUNT(*) WHERE status='x'`（单字段等值） | **引擎已具备** `inverted_doc_count("status=x")`（白名单内存位图/倒排 doc_count，demo 万次级亚毫秒）；**mysql 层未路由**——被当无索引字段整表全扫 | 协议层小改 = 最高性价比项 |
| `SUM(amount) WHERE status='x'` | 引擎无 posting 统计载荷（仅 doc_count） | 需引擎扩展 |
| 持久 total_count | 无跨重启精确计数 | 需设计评审（update 覆盖不重计 / delete 减 / WAL 回放幂等） |

- **Ex-9.1（P1）mysql 聚合路由到倒排计数 — ✅ 完成（4 亿库实测 276.6s → 42.5s / 6.5×；亚毫秒 = Ex-9.1b）**
  - [x] mysql.rs 单字段等值 COUNT 模板（`single_eq_count_field`：无 AND/OR/ORDER/比较/主键/非引号值）+ 读分发
    统一入口 `dispatch_query_read_opt`（COM_QUERY / COM_STMT_EXECUTE 共用）：非事务模板命中且字段已建倒排 →
    写锁 `Engine::inverted_doc_count`（flush pending 保证已提交写入可见）；其余维持读锁读读并行
  - [x] `Engine::inverted_count_eligible`（白名单/bitmap 字段判定，防"未建索引误报 0"）；单测 ×3
    （模板解析边界 / 与全扫一致 / 可路由判定），mysql 29 全绿
  - [x] 实测（4 亿库）：`COUNT status='active'` 全扫 276.6s → 42.5s（6.5×）——精确 doc_count 回退路径；
    **亚毫秒需段级载荷（Ex-9.1b）**
- **Ex-9.1b（P1，2026-09-03 追加）倒排段级 doc_count 载荷 — ✅ 完成（SEG_VERSION 4，569 全绿）**
  - [x] 段格式 v4：条目 = term + `varint(段内 doc_count)` + posting（flush_segment 与 gc 重写均写载荷，
    段内 posting 为去重 docid 集合 → bitmap.len() 精确）；老段 v2/v3 按段版本兼容读
    （parse_posting_at / PostingCursor / 两处 linear 回退 / read_segment_terms 全版本化）
  - [x] `InvertedIndex::doc_count_fast`：mem 去重 + 各段载荷求和 = O(段数) 亚毫秒；**任一命中段为老格式 →
    回退精确 `doc_count` 遍历**（正确优先）；Engine::inverted_doc_count 改 fast 优先，并移除 bitmap_count
    短路（内存位图仅覆盖运行期写入、重启空/冷库漏存量 = 既有缺陷一并修正；位图仍服务 search/组合筛选）
  - [x] 语义注（文档化）：同 docid 同 term 跨段覆盖（update）求和略高估；写入单调 / 后台 GC 收敛后段间
    无重叠即精确；覆盖写入高频场景走 `doc_count`（精确去重）
  - [x] 单测 ×2（fast==exact 跨段 + 重叠上界文档化）；存量升级路径 = 倒排 GC 把全段合并为单 v4 段后
    COUNT 自动转亚毫秒（混合 v3 存量现回退 42.5s，见 Ex-9.1 实测）
- **Ex-9.2（P2→评估）持久 visible_count（无条件 COUNT(*) 快路径）— ✅ 评估：不立项（单集合口径受限，2026-09-03）**
  - [x] 调研方案 C（exist 位图 1bit/docid + visible 原子 + 随 flush_wal 8B checkpoint；put 覆盖不重计/
    复活补计/回放按规则重放幂等）技术上可行
  - [x] **否决理由**：单集合无分表（表=别名，orders/users 共享 documents）——全集合可见总数对业务
    COUNT 无意义（`COUNT(*) FROM orders` 会含 users，50m final"发现 3"口径问题；规模测试 4 亿
    COUNT=359M<400M 即口径混乱体现）——快计数只是把歧义口径变快，不解决歧义
  - [x] 价值仅在"单集合=单业务表"部署形态成立（配置/部署语义，非计数器可解）
  - 触发项：引入表/集合隔离（数据模型级）后的**分域计数**再评估；当前无条件 COUNT 维持 key-only 全扫，
    条件/字段 COUNT 走 Ex-9.1b v4 doc_count（亚毫秒）
- **Ex-9.3（P3）倒排统计载荷（sum/min/max/avg）**：TermMeta/段格式扩展（写路径随 term 维护载荷 + 段格式
  版本 + GC/合并保持）→ 支撑 `SUM(amount) WHERE status='x'` 级聚合秒级；与 Ex-9.1 共用路由；
  仅对配置声明字段启用（`stats_fields`，防多数字字段全开失控——对齐 Ex-4 成本控制准则）
  - **方案细化（2026-09-03 spike，待实现）**：代码核实确认 flush 时仅存 docid 无文档值 →
    统计必须**写路径随 term 累积**。设计：mem 项 `term → Posting{docids, stats: Vec<f64>}`（stats 按
    段头声明的 `stats_fields` 顺序对齐；put 时对每个 term 解析声明数字字段值累加）；段格式 v5 条目 =
    term + varint(doc_count) + 定长 stats 数组（低基数过滤词 term 仅几十/百个/段，定长 24B×字段 × term
    数开销可忽略）+ posting 容器；GC/合并重写保持载荷；`sum_stats(term)` 求和亚毫秒。Query 语义：
    `SELECT status, SUM(amount) FROM t GROUP BY status` 走词典逐 term `sum_stats` + 倒排值回表组装
    （仅需组键值，无大文档 IO）；`SUM(amount) WHERE status='x'` 单 term 直读。限制：跨段同 docid 同
    term 覆盖（update）求和略高估（同 Ex-9.1b 注）；文档缺 stats 字段按 0 计（MySQL NULL 语义差异文档化）；
    单值字段分区假设下 NULL 组 = 全文档数 − Σdoc_count（待做：全文档数快计或声明限制）。
  - **落地进度（2026-09-03）**：① mem 写路径累积 + Engine put 透传 ✅（5a792cc）→ ② 段格式 v5 写/读 +
    GC 合并保持载荷 ✅（e52941a，592 全绿）→ ③ 引擎 API + sqlish 路由：`SUM/AVG/MIN/MAX(stats_field)
    WHERE f='v'` 裸等值走倒排载荷免全扫，未命中回落全扫数值一致 ✅（本期，593 全绿）→ ④ GROUP BY 逐
    term 词典枚举聚合（待做：需倒排词典 term 前缀遍历）→ ⑤ 50m A/B 与全量回归（待做）。
  - 后续小节：实现顺序 = ① mem 写路径累积 + Engine put 透传 stats 值 → ② 段格式 v5 写/读 → ③
    GC/合并载荷保持 → ④ `sum_stats`/引擎 API + sqlish GROUP BY/SUM 路由 → ⑤ 50m A/B 与全量回归
- **远期不排期**：组合索引范围聚合（依赖 B+Tree 化，Ex-8.4 触发链）、物化视图 / 聚合缓存（TTL/失效语义待产品化）
- **验收口径（3 亿库）**：无条件 COUNT(*) ≤1ms；条件 COUNT ≤1ms（Ex-9.1）；`SUM WHERE status` ≤100ms（Ex-9.3）；
  数值与全扫一致（抽样断言）

## 20. Ex-3 Calvin 确定性事务评估归档（归档自 development_extension.md，2026-09-03 合并）

- **判定**：✅ 评估完成——**不进入 kernel（远期方向保留）**（development_extension.md Ex-3.3 /
  development.md 7.45 记录一致）
- **理由摘要**：写路径 docid 一致性哈希 → 单 docid 事务天然不分片；L1 outbox + L2 SAGA 已覆盖
  跨节点/异步最终一致；Calvin 需全局事务序协调器（单点）+ 读写集预声明（倒排词表难静态声明），
  投入产出不匹配
- **远期触发**：出现强一致多 docid 跨分区事务需求时，按"全局事务序 + 状态机衔接 ReplicationLog"
  落地——阶段一/二/三排期见本文件 **§7**，完整蓝图见 design_remain.md §1（design_extension 13.3/14.8/14.9）

## 21. 规模扩展测试收口（2亿→5亿，2026-09-03，async 3308）

> 报告：`tmp_scc_scale_2b_to_5b.md`（不入库）；逐级明细 tmp_scc_probe_{2b..5b}.txt / tmp_load_*.txt。
> 数据资产：db-bench-mysqlcmp-50m orders 递增扩充至 **5 亿**（保留复用）。

| 项 | 2 亿 | 3 亿 | 4 亿 | 5 亿 | MySQL 50m 基线 | 结论 |
|---|---|---|---|---|---|---|
| 灌数吞吐/1 亿增量 | 56.5k | 55.1k | 48.6k | 49.1k rows/s | ~53k | 3 亿后 -12%（深库后台负担），LSM 顺序写优势保持 |
| 点查 QPS（16×15s） | 7,180 | 6,730 | 6,145 | 5,207 | 7,587 | 随深度缓降 -27%；2 亿≈MySQL，5 亿 1.46× |
| 范围 p50（100 行窗） | 2.8ms | 2.6ms | 2.6ms | 2.9ms | 0.70ms | **位置无关恒定**（Ex-8.1 验收：无超线性放大）；5 亿 ≈4.1×（固定开销+形态差） |
| COUNT(*) 无条件 | 107.5s | 194.3s | 262.3s | 347.6s | 2.2s | key-only 全扫随行数线性；口径=单集合可见（Ex-9.2 不立项） |
| COUNT status=active | 133.9s¹ | 197.8s¹ | 276.6s¹ | **0.25ms**² | 17.4s | ²5 亿复验（2026-09-03）：segment_max_size_mb=64 触发全量 GC（1038 段→单 v4 段 654.8MB）后走 doc_count_fast 载荷求和 = **p50 0.253ms**（status active 98.9M / city 83.3M / closed 99.0M），值=单段精确（无跨段重叠）；对比全扫 ~330s、doc_count 回退 73s |
| SUM amount>90000 | 150.4s | 214.1s | 306.9s | 375.6s | 12.2s | 全扫线性；Ex-9.3（统计载荷）P3 |
| 空间 | ~6.7GB | ~9.6GB | ~12.6GB | ~15.5GB | — | ~3GB/亿（zstd） |

¹ 2~4 亿 status 为全扫口径（当时未带 Ex-9.1 二进制）；³ COUNT/SUM 全扫与点查存在冷热缓存差异。

## 已完成基线（勿重复）

- 排期大项 A~Y 全完成（development.md §13 排期队列 第 13.2 表）
- development.md 7.x 至 7.105；problem_solving P1~P78
- SAGA 内核+网关+补偿协议+拓扑并行+对账（Ex-2/2.5/13.5/13.6/13.7）
- 1 亿库读优化（R/M/O）与写路径修复（syscall + 分批 + worker 单轮）

## 22. 排期入口变更（2026-09-03，development_process_order.md 停用删除）

- **原 development_process_order.md（开发排期唯一入口）已删除**，其队列/详情/已完成/环境备忘
  并入 development.md **§13 开发排期队列与已完成大项**（流程约定 13.1 / 队列 13.2 / 详情 13.3 /
  已完成 13.4 / 环境备忘 13.5）。开发路线入口自此 = development.md §13 + 本文件（未完成任务明细）。
- 相关引用已同步改写（design_remain.md / development_remain.md / images 报告中的旧文件名 →
  development.md §13 排期队列）。
- 本文件 §19（聚合/COUNT 加速候选）持续跟踪 **AF #6 = Ex-9.3 倒排统计载荷加速 GROUP BY**：
  现状 `GROUP BY`/聚合已支持（AF #2/#4/#5，development.md 7.102/7.104/7.105），倒排加速 =
  Ex-9.1b 风格（v4 doc_count 亚毫秒）扩展为 posting 统计载荷（sum/min/max/avg）以支撑
  `GROUP BY status` 与 `SUM(amount) WHERE status='x'` 聚合秒级；仅对配置声明字段启用
  （`stats_fields`，对齐 Ex-4 成本控制）。

## 23. SQL DML 增强（用户 2026-09-03 排期请求）——✅ 全部完成

> 语义确认（2026-09-03）：batch_insert/update/delete = **MySQL 同名命令形态**（batch_insert =
> 多行 `INSERT`；batch_update/delete = `UPDATE/DELETE … WHERE id IN / f IN / 字段条件`），
> 不新增自定义 batch_* 语法入口。均在 MySQL 同名命令下落地：

| 项 | 内容 | 状态 |
|---|---|---|
| UPDATE/DELETE … WHERE id IN / f IN | WHERE 集合过滤（IN 解析 + 执行层按 docid 定位逐条 put/delete） | ✅ 非事务（3d4ac30，`update_response`/`delete_response` + `resolve_where_ids`）；事务内（0675cd3，`txn_update`/`txn_delete` + `txn_resolve_where_ids` 复检） |
| batch_insert | 多行 `INSERT … VALUES (..),(..),…` | ✅ 已支持（insert_response / `parse_insert_multi`，显式 id + auto id；1062 预校验无部分写入 a5b4171） |
| batch_update | `UPDATE … WHERE id IN / f IN / 字段条件`（逐条 + 影响行数汇总） | ✅ 非事务 3d4ac30 + 事务内 0675cd3 |
| batch_delete | `DELETE … WHERE id IN / f IN / 字段条件`（返回受影响行数） | ✅ 非事务 3d4ac30 + 事务内 0675cd3 |

> 验证：`update_delete_where_in`（非事务，含无匹配 0 行）、`txn_update_delete_where_in_and_predicate`
> （事务内攒批可见 + 回滚原子）、INSERT 多行/1062 用例；mysql 41、lib 608 全绿。

## 24. MySQL 收敛缺口清单（用户 2026-09-03，RR 对照 Stage3 后按序解决）

| # | 缺口 | 表现 | 状态/计划 |
|---|---|---|---|
| a | INSERT 不校验主键重复 | MySQL 1062 vs SCC 成功（DUP 取消 ~40%、覆盖=潜在覆盖语义） | ✅ 已实现（a5b4171，非事务+事务路径预校验 1062、无部分写入） |
| b | 事务内 SELECT 仅 WHERE id=/BETWEEN/IN | 非主键列谓词直接 1064 | ✅ 已实现（f78290b，主库候选 ∪ 同事务写集 txn_get 覆盖 + 谓词复检） |
| c | DROP TABLE 不 purge 数据 | --init 后残留上轮行 | ✅ 已实现（10c946c，DROP/TRUNCATE 走 Engine::purge_all 清内存+磁盘段+倒排+位图；重启空库；600 lib 全绿） |
| d | UPDATE/DELETE WHERE id IN(…) 不支持 | 仅支持 id=（批量写通道废） | ✅ 已实现（3d4ac30 非 txn；本提交扩展 txn 路径：事务内 UPDATE/DELETE … WHERE id IN / 字段条件，攒批可见 + 回滚原子） |
| e | DIFF=21 / DEADLOCK=2 语义分歧 | RR Stage3 核心产出被 a/c 污染 | 已由权威 rr-cases C1~C6 收敛为缺陷 A/B（FOR UPDATE 当前读、BETWEEN 幻读）→ 见 §25 排期 |

> 注：d 的 txn 路径已扩展完成（事务内 UPDATE/DELETE … WHERE IN / 字段条件）；e 已细化入 §25（缺陷 A/B，均已修）——修后按 §25 环境复验 C1~C6 全绿即收敛。

## 25. RR 一致性权威结果与缺陷排期（rr-cases C1~C6，用户 2026-09-03）

> 权威结果（results-rrcases3 / rr-cases.log）。修正此前误报：**C5 "COMMIT 不持久化"不成立**——
> 上轮失败因用例把 BEGIN 发在准备行插入之前、SCC 快照建得过早（MySQL 快照在首次一致读才建）；
> 准备行移到 BEGIN 前之后 C5 通过（final 两库均为 1）。C1/C3 的"点读快照稳定/已删行仍见"
> 两步本身 PASS——点读快照是对的，仅 FOR UPDATE 当前读语义缺失。

| 用例 | 结论 | 失败步骤 |
|---|---|---|
| C1 快照稳定 | ✅ PASS | —（A 修复后转绿） |
| C2 自写可见 | PASS | — |
| C3 已删行在快照 | ✅ PASS | —（A 修复后转绿） |
| C4 防幻读 | ✅ PASS | —（B 修复后转绿） |
| C5 COMMIT 持久 | PASS | —（时序伪差，非引擎问题） |
| C6 ROLLBACK 丢弃 | PASS | — |
| C7 主键重复 INSERT→1062 | PASS | —（缺口 a） |
| C8 事务内非主键列谓词 | PASS | —（缺口 b） |
| C9 非事务 UPDATE/DELETE id IN | PASS | —（缺口 d） |

### 缺陷 A：事务内 SELECT … FOR UPDATE 不是当前读（C1、C3 失败）——✅ 已修复（4818ecb）

- 代码定位：`src/mysql.rs txn_select()`——拿到 ids 后一律 `engine.txn_get(txn, id)`
  （快照），BETWEEN 分支一律 `engine.scan_range_txn(txn, …)`（快照）；**FOR UPDATE 关键字被完全忽略**。
- 修法（已落地）：解析前剥离尾部 FOR UPDATE → 点/IN 走 `txn_read_current`（`read_own` 优先，
  否则 `engine.get` 最新已提交；**不写 snap 快照缓存**——当前读不污染 RR 一致读）；范围走
  `txn_scan_current`（`engine.scan_range` 最新基表 + read_own 覆盖/删除排除/新 docid 并入）；
  非主键谓词分支同步支持。**引擎侧未动**。快照一致读路径保持原样。
- 验证：`txn_for_update_current_read_c1/c3` 两连接端到端（快照稳定 0/5、FOR UPDATE 见 1 /
  隐藏已删行、随后一致读不被污染）；lib 602 全绿。
- 复现（C1）：pre 插 id=900100 → BEGIN → 点读见 0 → aux 提交 UPDATE val+1 → 再点读仍 0（快照对）→
  `SELECT … FOR UPDATE`：mysql=[900100|1]（当前读）vs mydb=[900100|0] ✗。C3 同根：他事务已 DELETE
  提交后 FOR UPDATE，mysql=∅、mydb 仍返回旧行 900300。

### 缺陷 B：BETWEEN 范围一致读出现幻读（C4 失败）——✅ 已修复（根因：位图删除非版本化 + 复活清位）

- **根因定位**（本地 rr 连续跑复现后收敛）：位图删除路径（Ex-5.6）只写 1bit + WAL 记录、
  **不写 memtable Tombstone**。rr 每 case 开头 DELETE 清键（位图置位），pre/aux 再 INSERT
  同键 = **put 复活（清位）**。复活后快照读失去"快照点在该行删除期"的信息，LSM 版本链只剩
  [旧版本 S1(≤快照), 复活版本 R(>快照)] → 快照读回退 S1 → **幻影可见**。（点读路径因事务
  snap 缓存首读命中而掩盖；BETWEEN 范围扫不走缓存 → 暴露。这就是"干净库单跑不现、
  累计库全档可现"的原因。）
- **修法（已落地）**：位图路径 delete 新增 `ColumnFamily::delete_record_mem`（WAL 记录 +
  **memtable Tombstone 进版本链**，不逐条 fsync），`Engine::delete` 位图分支改用之——复活后
  快照读按 seq≤snapshot 取最大版本 = Tombstone → [删除, 复活) 区间恒不可见，对齐 MySQL RR。
- **验证**：新增根因单测 `txn_range_scan_hides_revived_row_c4`（delete→复活→范围快照扫不可见）；
  rr-conformance 本地闭环（本地 SCC + 连续 C1~C8 **三轮含累积加深**）**9/9 全绿**（修复前
  累计库 C4 FAIL，重建库单跑/干净库 PASS）；lib **607 全绿**。工具新增 `--rr-case <N>` /
  `--rr-reinit`（单用例 + 重建表，便于累积隔离复验）。
- 附：C4 权威 SQL 形态 `INSERT INTO t_test(id,val) VALUES(900400,1)`（列式）；
  `SELECT id,val FROM t_test WHERE id BETWEEN 900400 AND 900402 ORDER BY id`；同 SQL 的
  FOR UPDATE 当前读**已正确**（缺陷 A 修复生效）。

### 排期与验证

- 实现顺序：缺陷 A ✅（4818ecb，C1/C3 转绿）→ 缺陷 B ✅（位图 delete 版本化 Tombstone；
  本地 rr 累计三轮 9/9 全绿，C4 转绿）。**权威复核完成**：MySQL 3306 ↔ SCC 3309（db-s3-3a
  累计库）`results-rrcases7` **9/9 全绿**（C1~C9，conn_err=0）→ C1~C6 收敛目标达成。
- 验证：环境 MySQL 3306（db rr）、SCC 3309（db-s3-3a）；命令
  `rr-conformance --rr-cases --out <dir> --mysql-url … --my-url …`（新增 `--rr-case <N>` 单用例、
  `--rr-reinit` 重建表）；用例源码 `rr-conformance/src/rr_cases.rs`（+main.rs 入口）。

## 26. 真多表支持（表级主键空间隔离，用户 2026-09-03 确认语义并排期）

### 语义确认（用户）

> 支持**真多表**：不同表允许相同主键 id（表级主键空间隔离，对齐 MySQL）。当前 SCC "表"只是
> SQL 层别名，全部落到同一 documents 集合 + 全局 docid —— t_test / t_combo 各自 id=1..2000
> 互相撞键：a 修复（1062 预校验）前静默覆盖、修复后 `--init` 直接 1062。

### 现状与缺口

- 存储模型：单文档集合（内存/磁盘/WAL/倒排/位图/事务/引擎 API 全部围绕**全局 docid**）；
  mysql server 固定"库 scc、表 documents"（README 明示），语句表名解析后被忽略/不校验。
- 触发：rr-conformance `--init` 双表（t_test + t_combo）种子各 id=1..2000 → SCC 同 docid 互撞；
  已用 `--single` 单表化绕开（工具口径，未修引擎）。真实多表业务/迁移同样会互踩。

### 方案选项（排期评估用）

| 方案 | 做法 | 改动面 | 备注 |
|---|---|---|---|
| A. 表级 docid 命名空间（架构级） | 存储/API key 带表维度：docid 分配按表分段（表 id 高位）或 key 编码加表名前缀 | 引擎 key 编码（encode_docid 8B 固定）、扫描/组合索引、倒排 posting docid、删除位图、事务 write_set、hotcache、mysql 层表名解析全链路 | 完整对齐 MySQL 多表；需迁移既有单表资产（db-s3-3a 等）与段格式兼容策略 |
| B. 每表独立引擎/列族集 | 建表 = 新 Engine（子目录）或 CF 对；mysql 层按表路由 | mysql 会话层 + 多实例生命周期；引擎内改动小 | 无跨表查询则成本可控；与"单引擎多 CF"架构（primary/delta/inverted 三 CF）叠加需重设计 |
| C. 口径单表化（已做 --single） | 测试只在单表跑 | rr-conformance 工具 | 不修引擎；产品多表仍不支持 |

### 影响面清单（方案 A 前置调研要点，2026-09-03 修订：与 docid_alloc/自动生成统筹）

- docid 语义：`encode_docid`（8B 大端 u64）贯穿 keys/CF/Engine API；表维度需入 docid。
  **docid 布局现状约束**：已有分片前缀方案（`docid_alloc.rs`，10 亿阶段 A）——
  `docid = shard_id(16bit) << 40 | local_id(40bit)`，占高 16 位。多表高位必须与其统筹：
  - 单机多表子方案：docid = `table_id << 48 | row_id(48)`（table_id 16 位、row_id 48 位，
    8B 不变，倒排/位图/事务零改）；
  - 与分片前缀冲突：分片（shard<<40）与表高位不能并存于同一定义 → 需统一 docid 布局决策
    （如单机用表高位、分片构建入口复用 docid_alloc 的 shard 编码分别路由，或扩位方案留待
    真分布式多表再评估）；**本期只做单机多表（表高位），不触碰分片路径**。
- row_id 分配（见 §27）：显式 SQL id 直落 row_id；自动生成 = 表内自增 + 持久水位，
  与显式 id 并存（冲突走 1062，a 已修）。**不引入 Snowflake**（单机 LSM 自增已最优；
  分布式唯一已有 docid_alloc 分片前缀，无需时钟/worker 分配）。
- 倒排：posting docid 无需改（docid 带表后天然隔离）；位图索引 term 亦同。
- 删除位图/删除密度/垃圾回收：docid 位图按全局 docid，无需表粒度（docid 已编码表）。
- 事务：write_set/锁表/snapshot seq 全局，seq 与表无关，docid 带表后无冲突。
- 兼容：既有单表库数据（docid 0..N）视为默认表（table_id=0），零迁移。
- mysql 层表名解析：INSERT/UPDATE/DELETE/SELECT 表名 → table_id（CREATE TABLE 分配、
  不回收；上限 65535 表足够）；SQL id ↔ docid 编解码在会话层。

### 排期状态

- **已立项（2026-09-03，用户确认三里程碑逐步实施）**：
  - **M1 表路由可用**：表名 → table_id（**确定性 FNV-1a hash & 0xFFFF 派生**，免注册表/免持久化、
    跨连接与重启稳定；表名 "documents" 特例 = table_id 0 = 既有单表库零迁移）+ SQL 层 id↔docid
    编解码（docid = table_id<<48 | row_id）+ DROP TABLE 单表区间清（默认表仍 purge_all 兼容 c）；
    验收 = rr-conformance `--init` 双表种子各自 id=1..2000 共存不撞 + 双表同 id 读写隔离。
  - **M2 per-table row_id 分配**（§27 表内分配器落地：显式 id 直落 row_id、auto 分配按表区间水位）。
  - **M3 Flush/Compaction 按表切分**（本小节实施清单 1-3，纯表文件）。
- 当前不阻塞：`--single` 已绕开对照；RR 收敛目标（C1~C6 全绿）已达成。

### 实施清单（随立项，用户 2026-09-03 细化：Flush / Compaction 按表切分）

> 前提：docid = `table_id << 48 | row_id` 高位编码落地（本小节才成立）。docid 高位有序 →
> MemTable/归并遍历天然"同表连续区间"，两处切分都只需检测区间边界（`docid >> 48`），
> 无需表概念进入引擎。目标 = 每层每表独立 SSTable（物理表隔离）：

| # | 项 | 内容 | 适配点（本引擎与 RocksDB 心智差异） |
|---|---|---|---|
| 1 | **Flush（MemTable→L0）按表切分** | 遍历 immutable 写 SST 时，writer 检测 `(next_key>>48)` 变化即 Finish 开新 writer（同表区间一次扫出；table_id=0 单表 = 单文件，与现行为一致） | `flush_single`/`write_sst` 支持单次多输出；manifest `snapshot_insert` 逐文件登记（level 0） |
| 2 | **Compaction 输出按表切分（L0→L1 及深层）** | 多路归并写段时同 ① 检测表变化切文件——各表区间不重叠 → 输出多段**同层不重叠**（天然符合 L1/L2 层内不重叠语义，范围剪枝/层级元数据直接适用） | 现 compact 每轮**单输出文件**（无 target_file_size 切分语义）：compact_merge 需支持多输出 + 逐个 finalize/登记；select/needs_compact/layer 按段列表已支持同层多段 → 改动集中在输出循环 |
| 3 | meta_only 块级复用适配 | 复用输出也须按表切（跨表区间拼接 = 混合文件）；或跨表合并禁 meta_only 回退全量切分 | 复用"相邻段无重叠"现含跨表相邻段 → 增加"同表才复用/跨表切多输出"门控 |
| 4 | 文件路由/删除 | 查询：docid 区间已含表 → 既有 sst 窗口剪枝自动跳过其它表文件（受益）；DROP TABLE（单表）删该表区间文件（docid 前缀过滤 compaction 物理回收或文件级删） | 无需文件名带 table_id（区间即表）；文件名带表名可作可选运维/备份便利 |
| 5 | 单表回归 | 全库单表（table_id=0）时 ① ② 均单文件输出，行为与现状一致 | 需回归确认无分层无多表时零变化 |

> 收益（用户分析确认）：L0→L1 读/写放大降（不再跳读混表）、查询零跨表 IO、缓存按表隔离
> （冷热分离，静态表不再被反复合并）、DROP 单表低成本。
> 代价：文件数 = Σ表 × 层文件（L1≈表数；L2+ 按大小切分后 ≈ Σ表数据量/单文件上限）；
> 运维 ulimit -n（如 65536）与文件句柄缓存（TableCache 只缓存近期访问）说明写入运维手册。
> 状态：随 §26 多表立项实施（内部 P1，与高位编码同步落地，避免先上线后二次优化）。

## 27. DocID 生成与自增主键增强（用户 2026-09-03 分析 + 排期）

### 现状（已核实代码）

- **docid 来源分两层**：引擎层 = 调用方显式传入的 u64 键（put/get/delete docid，无生成逻辑，
  `encode_docid` 8B 大端）；mysql 兼容层 = 客户端显式 id 直用，**无 id 列 / `id=0` → 共享自增**
  （`Session.auto_id: Arc<AtomicU64>`，跨连接从 1 起 `fetch_add`——sysbench AUTO_INCREMENT 兼容）。
- **现有三缺陷**：① auto_id **不持久化**（重启回 1 → 续插撞已提交行 1062）；② 显式 id 与自动 id
  混用可能碰撞（显式插到 auto_id 将分配值 → 后续自动插入 1062，a 修复后显式化）；③ 无表内
  隔离（单表下无碍，多表后按 §26 需表内分配器）。
- 已有分布式 docid 资产：`docid_alloc.rs`（`shard_id<<40 | local_id` 分片前缀 + `ShardLocalAllocator`
  Atomic 分配），供 10 亿分片构建路径；单机 mysql 路径未走它。

### 技术决策（2026-09-03 分析结论）

- **docid 保持 u64 自增族，不引入 Snowflake**：单机 LSM 写入天然按递增 docid 顺序（友好）；
  分布式唯一已有分片前缀（无时钟/worker 协调）；Snowflake 的"趋势递增"对 LSM 与自增等价。
- **自增 = 服务端兜底已存在，增强方向是正确性/持久化**：
  1. 自动分配起点续接库内水位（启动/首插取 `max_docid+1`，非恒 1）；
  2. 批量导入用**段预分配**（每批取一段，减 fetch_add 竞争，对齐 10 亿灌数）；
  3. 多表后分配器按表（row_id 域，见 §26）。
- AUTO_INCREMENT 语法识别：CREATE TABLE 列属性解析为"该表主键自增"标记（当前列清单解析
  已忽略列属性，无需新协议）；行为即现有"无 id 列/0 占位自动分配"。

### 排期项

| 项 | 内容 | 状态/优先级 |
|---|---|---|
| docid 水位续接 | auto_id 分配器起点 = max_docid+1（首次分配经 `Engine::auto_watermark` 惰性全库 keys-only 恢复，重启续接不撞库）；显式 id 经 put fetch_max 同步抬水位 | ✅ 已完成（623726e：`Engine::auto_watermark` + `auto_alloc_block`；测试 `auto_watermark_resumes_after_reopen` / `auto_insert_resumes_above_explicit_ids`） |
| 段预分配 | 多行 INSERT 语句级**块分配**：`auto_alloc_block`（fetch_max(水位)+fetch_add(行数)）一次取段、逐行零原子，与显式 id 混插按行序分配 | ✅ 已完成（本提交：insert_response/txn_insert 多行 auto 块预分配；测试 `auto_multirow_block_allocation`（混合行序 + 下语句水位续接 301），lib 612 全绿） |
| 表内 row_id 分配器 | §26 多表落地时：每表 `(table_id, row_id 水位)` 分配器（持久化水位存哪：系统键/列族）；与显式 id 冲突 1062 保持 | 随 §26 立项 |
| AUTO_INCREMENT 列属性 | CREATE TABLE `id INT AUTO_INCREMENT` 解析（属性忽略→接受）→ 该表插入无 id 走自动 docid | ✅ 已完成（功能已具备：CREATE TABLE 空操作容忍列属性 + 无 id 列 INSERT 已解析为 auto（parse_insert_multi）；补端到端验证 `auto_increment_ddl_and_idless_insert`（列属性建表 + 单/多行无 id 插入 auto 1..4 + 事务内 SUM 验证），lib 613 全绿） |
| Snowflake（远期备选） | 仅当真分布式多写者无协调分配出现时复评（现有 docid_alloc 已覆盖分片场景） | 远期，不主动排期 |

> 已修复（2026-09-03）：非事务 `SELECT SUM(k) FROM t WHERE id=N` 被 `extract_point_id`
> 短路为普通点查（返回行集 → helper 读到 id 列）——point_id 分支补 SUM/COUNT 标量聚合
> 前置（与 BETWEEN/IN 分支同语义）；事务路径本就正确。回归：auto_increment_ddl 测试
> 改为非事务 SUM 断言验证修复；lib 613 全绿。

### 验收口径

- 重启后无 id 续插不撞已提交行（水位续接）；显式 id 后自动 id 不撞（水位抬升）；
  批量导入 10 亿场景分配开销可忽略（段预分配）；rr `--init`/`--single` 与既有 mysql 41、lib 测试全绿。
