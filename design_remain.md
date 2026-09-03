# design_remain.md —— 设计未落地 / 留待设计点

> 从 design.md / design_extension.md 提取的**未落地或留待**设计点（已落地设计不在此列）。
> 决策下一步时以本文件为准；触发条件满足才实施的项已标注。

## 1. 分布式事务：Calvin 全局事务序（远期蓝图）

- **来源**：design_extension 13.3 / 14.8 / 14.9（v0.6~v0.8）
- **状态**：🔍 远期，**当前不进入 kernel**（Ex-3 评估完成）
- **触发条件（13.3.1，不满足不落地）**：强一致多 docid 跨分区事务需求 + 读写集可静态预声明
- **落地阶段（14.8）**：
  1. 阶段一：全局 gseq 分配器（持久化游标不重号）+ 单写者确定性执行 demo（真实 WriteBatch/快照衔接）
  2. 阶段二：全局复制日志（跨分区广播/拉取）+ 分区确定性执行 + 幂等键 `(gseq, docid)` + 落后追赶
  3. 阶段三：序节点高可用（raft 复制 gseq 游标与日志）+ 故障转移 + 对账
- **API 形态**：14.9 读写集声明（`POST /txn/submit`，单 docid 不走全局序；交互式先读后写不支持）

## 2. 导出增强：增量导出 / JDBC / 流式管道完整版

- **来源**：design 20.5（shanshui-cunji-export）
- **已落地**：CSV 全量、Parquet 全量（`--parquet`，70c3b30）、增量导出（`--incremental --checkpoint`
  docid 游标断点续传，2174531）、流式管道（`--filter/--project/--mask` Filter+Projection+Sink 分叉，
  c6b5417）、JDBC 直连（`--jdbc mysql://...` MySQL wire 客户端建表+批量 INSERT，c6b5417）、
  `--rate-limit` 限流（c6b5417）、MySQL 兼容 CSV 配套（`--mysql-compatible` 自动生成 CREATE TABLE +
  LOAD DATA INFILE SQL + `--mysql-max-varchar`，313bd81）、建表 DDL（`--dry-run-schema --target
  clickhouse|mysql` MergeTree / InnoDB，313bd81）、与 Compaction 共享后台 IO 优先级
  （`--io-rate-limit-mb` scan_limiter Token Bucket，40e8abb）
- **未落地**：无——**design 20.5 导出功能全部完成**（✅）

## 3. 合并阻塞根治：无锁合并（✅ 完成 af24dbd + P73 3d58137）

- **来源**：problem_solving P72/P73 / development 7.65
- **已落地**：分批缓解（compact_input_max_mb）→ **根治**（2026-09-01）：
  1. CF 增 `sst_mutate: Mutex<()>`（flush/compact 的 ssts store 互斥）✓
  2. CF `switch_and_flush` 改 `&self`（MemTableBuffer 内部 RwLock 冻结）✓
  3. Engine primary/delta/cidx 字段 `Arc<ColumnFamily>` ✓
  4. mysql worker 无锁合并（clone CF Arc → drop 锁 → CompactTargets::run）✓
  - 附带修复 P73：persist_manifest 内存快照（不扫描磁盘，防引用半写段）+ store→persist→remove
    原子；1 亿库实测合并期写不塌陷（25-43k rows/s），469 全绿

## 4. 读写分离（COW 快照读）

- **来源**：design 9.5 / feature G / M8-P1（be09a07）
- **状态**：⏸ 暂缓——组提交已解决"读被写拖垮"；M8-P1 结论：RwLock 剩余收益 <20%，Engine &self 化改动面大
- **补充（7.72）**：HotCache 内部锁粒度已落地（`9071984`——整包 Mutex → 内部 RwLock + DashMap
  无锁计数，点查热路径读读并行：demo A/B 纯读 x4.16、混合负载 x5.42，482 全绿）；M8-P1 整体
  暂缓结论不变——剩余写路径（txn_commit/compaction）仍串行，组提交已解决主瓶颈
- **再启条件**：复制型分布式阶段（届时读放大场景需要）

## 5. 高并发查询优化（design 9.5 目标）

- **来源**：design 9.5 / development_process_order I 项（P3）
- **状态**：✅ **异步协程运行时完成**（2802885：tokio 网络层 serve_async + spawn_blocking 查询，
  连接 idle 不占 OS 线程——500 idle 连接仅 15 线程，10k 长连接可行；480 全绿）。
  高并发查询优化整体完成（同步模型预处理读锁 + 异步网络层）
- **剩余**：10k 连接 / 85 万 QPS 目标的**吞吐达成**需在目标硬件（16 核/64G/NVMe）基准复测
  （P95 高连接数下验证）——✅ **本机复测完成**（2026-09-02，images/perf-0.8.0/10k连接-85万QPS复测.md）：
  **10k 连接目标达成**（5000 idle 连接仅 19 线程，保持后全可用）；本机（6 物理核/12 逻辑核）
  QPS 上限 ≈ **37.5k**（16 线程峰值，延迟 sub-ms）——85 万 QPS 需 16 核目标硬件（本机物理核
  仅目标的 37.5%）；读路径无回归（QPS 随线程数正常扩展至 12 核上限）

## 6. 远期/评估蓝图（触发或规模条件不满足不落地）

| 设计点 | 来源 | 状态 | 条件 |
|---|---|---|---|
| 多副本 raft 高可用（序节点/元数据游标） | design_extension 14.x / 710 | ✅ 阶段一（7.77 `e2a76a0`）+ 阶段二机制全落地（7.85 `eab9a38` RaftTransport/LocalRaftTransport、7.88 `1adc5ec` TcpRaftTransport 真实 TCP、7.98 `9821dc0` scale_out RouteChannel raft 联动）；⏳ 剩余：真实 TCP 多进程编排联调（随部署）+ Calvin gseq raft 联动（远期） | 元数据切换需求已落地；Calvin 阶段三联动依赖 Calvin 落地 |
| Calvin 硬件卸载（gseq 接 DSA/PMem） | design_extension 14.9 | 🔍 评估完成（7.80 `demo gseq-hw`：AtomicU64 原子 seq 1-2 亿/s >> 100 万/s 目标 2+ 数量级）→ **无必要性**（远期跨机房+CPU 瓶颈时复评） | 跨机房强一致需求 + 单机 CPU gseq 瓶颈 |
| 存算分离 / Indexer Node（倒排外置） | design 9.10 / 1130 | 🔍 查询代理层已落地（7.79 `46b2be7`：IndexerProxy 独立倒排 + 回表抽象，500 全绿）；彻底存算分离 | 百亿级规模（50 亿属过度设计，不推荐 MVP；代理层先挂现有节点） |
| 两级索引（Level 1 内存常驻摘要 + Level 2 精确） | design 4.4.2（阶段 2） | ✅ 评估完成（7.80 `demo two-level-index`：全局摘要内存为 Zone Map 7812×、过滤收益为 0、点查已 sub-ms）→ **不引入**（现有 Block Index+布隆+Zone Map 已覆盖） | — |
| Tiered 分层合并（每次只合最小 2 段） | development 7.3 | ✅ 评估完成（7.78 `demo tiered-compaction`：模拟验证写放大不优于 Leveled，当前分批+无锁合并已解决阻塞痛点）→ **暂不引入**（读放大回归风险） | 写放大再优化需求（key 级更新密集场景复评） |
| io_uring 热路径实测收益 | development 7.71 | ✅ 已实测 | 阿里云 Debian 12 / 内核 6.1：池初始化成功、核隔离生效；2 核小机器写 -13%（SQPOLL 占核）、读持平（块缓存主导）→ 默认关；收益在多核/高 IOPS 场景（7.63 指引） |
| **10 亿库扩展（分片横向扩展）** | `design-10b-extension.md`（2026-09-02） | ✅ 阶段 A~D 机制全部落地（7.81~7.98，550 全绿）：A docid 分配器+分片构建工具、B 分片化倒排（local 位图+前缀组合+跨分片分页）、C raft RPC 接线（RaftTransport + TcpRaftTransport 真实 TCP `1adc5ec` + scale_out RouteChannel 联动 `9821dc0`）、D 分片可观测（水位预警）；⏳ 剩余 = 硬件验收（10 分片 10 亿构建 `tmp_10b_acceptance.py` 就绪、亿级 posting 规模）+ raft TCP 多进程编排联调；**已实现/未实现总表见 design-10b-extension.md §10** | 10 亿规模需求（当前 1 亿库 10×；分片基建 + 扩容编排已就绪） |

## 7. LSM 读路径范围查询优化（Ex-8 系列，✅ 已排期 2026-09-03）

> **来源**：5000 万量级 MySQL 对照（tmp_scc_vs_mysql_50m_final.md）暴露的范围查询超线性劣化
> （10m 18.8× → 50m ~102×）。原外部建议「SSTable 外置 B+Tree 索引 / L1 层改 B+Tree 存储」经
> 代码审计 + 3308 实时实验（2026-09-03）**证伪其前提**——见下「分析结论」，落地方向为
> **读路径使用面修复**（Ex-8.1~8.3），B+Tree 存储层降为远期触发项（Ex-8.4）。

### 7.1 分析结论（代码审计 + 实验）

- **根因不是缺 B+Tree 索引/存储**，是「范围查询使用面缺陷」：
  1. **主因（收集路径线性索引税）**：mysql 非事务 `id BETWEEN A..B`（[mysql.rs:1856-1878]）→
     `engine.scan_range` → [sstable.rs:1255-1266] `sst.scan_range` **每查询从块 0 线性走完整块索引**
     （且 [sstable.rs:939-941] `index()` 每文件 clone 整个 L2 索引）→ 收集 Vec + HashMap 合并 + 全排序。
     代价 O(文件块数)，随库规模超线性。
  2. **次因（流式路径税）**：[column_family.rs:764-768] `scan_stream_at` 对所有 SST 无条件建
     迭代器（`layer_ranges` 层/文件粗筛只服务点查不服务 scan）；整文档值先解码后投影；
     扫描路径绕过块缓存（块缓存仅点查挂载）。
- **3308 实时实验（50m orders，100 行窗口 p50/20 次）**：非事务收集路径 66.7→101.2ms
  **随窗口 docid 位置单调劣化**（线性索引税实证）；事务流式路径（scan_range_txn →
  scan_stream_at + SstRangeIter 二分定位）恒 **~5.0ms** 位置无关 → **同 SQL 换路径即 17×**。
- **对原 B+Tree 方案的判定**：
  - 点查非瓶颈（1.35×，现为 Zone Map 层/段粗筛 + L2 二分 + 分区布隆 + HotCache）；
  - 现有 L2 精确索引（二分 + 常驻缓存）本质即「外部 key→块索引」——外置 B+Tree 索引文件 = 重复造轮子；
  - L1/L2 改 B+Tree 存储不解决线性索引走查与全值解码（真实瓶颈在使用面，不在存储格式），且引入
    compaction 侧构建/合并成本与 10% 写回退——**不立项为主项**。

### 7.2 Ex-8 落地拆点

| 拆点 | 内容 | 复用 | 预期（50m） |
|---|---|---|---|
| Ex-8.1（P0） | 非事务 `id BETWEEN` 改走流式窗口扫描（SstRangeIter 二分定位 + 只读相交块），替代收集路径 | scan_stream_at / SstRangeIter / mysql.rs 拦截点 | 范围 86ms → ~5ms |
| Ex-8.2（P0） | scan_stream_at / scan_range 层 + 文件 key 范围剪枝（复用 `layer_ranges/layer_indices`，窗口只建相交文件迭代器） | build_layer_meta / layer_ranges | ~5ms → 更低 |
| Ex-8.3（P1） | 扫描路径挂块缓存（复用 blockcache LRU）+ 纯 id 投影走 keys-only 解码（沿用 7.100 decode_data_block_keys 模式） | blockcache / keys-only | 逼近 MySQL ~0.84ms 个位数倍内 |
| Ex-8.4（远期） | L1/L2 B+Tree 存储替换（备选蓝图，不主动排期） | — | 触发：8.1~8.3 落地后仍未达标 + 读多写少极端场景 |

### 7.3 决策记录

- B+Tree 外置索引文件 / L1+B+Tree 存储：**当前不引入**（评估结论见 7.1；工业界先例适用前提 =
  已具备按需定位与页级连续读，与本题根因不对齐）
- 范围查询优化主路径 = Ex-8.1 → 8.2 → 8.3（先修使用面，成本 ~1/5、收益覆盖原方案宣称 100×→个位数× 目标）

## 8. MemTable/写侧优化 + NVMe 原生 + 倒排回表提案评估（2026-09-03）

> **来源**：外部提案两批（① 分片跳表 / L1+B+Tree 存储 / io_uring 全链路 / 大页 NUMA / 16KB 块 / TRIM /
> 并行 Flush / 墓碑 Compaction / 冷热分离；② 倒排回表 B+Tree 化 / 热点 term 缓存 / 倒排+文档联合缓存 /
> 预加载位图缓存）。逐项对照**现有代码与已落地优化**评估如下（已落地项不重复立项）。

### 8.1 决策矩阵

| 提案 | 判定 | 依据（现状/实测） |
|---|---|---|
| 分片跳表（256 分片，写入 4-6×） | ❌ 不立项（远期触发） | 写路径瓶颈不在跳表 CAS：crossbeam-skiplist 本就并发安全；put 受组提交 fsync + 倒排 + flush + 合并背压约束且引擎写锁串行化。分片破坏 memtable 全局有序（scan/merge/快照依赖）→ 跨分片归并复杂化。批量导入已有 put_batch（6197c21）。「4-6×」无写路径瓶颈佐证 |
| 跳表层级预分配/批量构建 | 🔍 并入 bulk-load 实验 | memtable 节点分配走 mimalloc；微观项 |
| 跳表→B+Tree 热切换 | ❌ | 同 §7 B+Tree 判定；S 项 MemTable 多版本版本链 + skiplist 有序读已覆盖 |
| L0 留 SSTable + L1/L2 改 B+Tree 存储 | ❌（已在 §7 否决） | §7.1：范围/回表瓶颈在使用面（收集路径线性税、扫描无层/文件剪枝、全值解码、块缓存旁路），不在存储格式；「L1 比 L0 新」前提在单集合 seq 版本语义下不成立（merge 取 max seq） |
| io_uring 全链路 | ✅ 已覆盖（V 项 7.71 + crates/io-uring-file + IoUringPool 三队列；默认关、多核 NVMe 开启指引 7.63） | 剩余 = Linux 部署验收 + 队列深度/批量提交调参，非新项 |
| 大页 + NUMA | ⏸ 平台远期 | 开发机单 socket Windows 无关；Linux THP=零代码配置；NUMA 分配器仅多 socket Linux 有意义 |
| 16KB 块替换 4KB | ❌ 默认切换（可配置实验） | 与 Ex-5.1 结论冲突：4KB 正是 NVMe 优化产物（读放大 -67%，4KB≈16KB 延迟）；block_size_kb 已参数化；块大小权衡研究已有 demo block4k |
| NVMe TRIM 集成 | ⏸ 平台远期 | 需 io_uring NVMe passthrough；Windows 无；空间回收已由 compaction 物理丢弃 + 删除位图 4KB 页对齐脏页 fsync 承担 |
| 并行 Flush | 🔶 采纳为候选（P2 demo） | flush 单线程序列化 imm→SST（zstd 压缩为主）是真实可并行段；但 flush 频次受 memtable 256MB 上限约束，收益需负载实测（与合并背压/io_uring 队列关系） |
| 墓碑优先 Compaction（脏文件优先） | 🔶 采纳为候选（P3） | 删除密度（位图置位率）加权 urgency，删除密集段优先合并释放空间；现 urgency（W 项）= L0 段数×10+大小超限+8，缺删除密度维度 |
| 读路径元数据过滤（seq/时间跳旧版本 SST） | 🔶 采纳为候选（P2 demo） | 层/段 key 范围 Zone Map 已有（R 项）；新增**段级 min/max seq 元数据** + 快照读（get_at/scan_range_at）整段跳过（快照 seq < 段 min seq）——与 Ex-8.2 文件范围剪枝互补 |
| 冷热分离 | ✅ 已覆盖 | Ex-5.9 冷热感知（ba709e2）+ heat-compaction demo + HotCache 双层 LRU |
| 倒排回表 B+Tree 化 | ❌ 存储方案（依赖 §7 否决项）；🔶 采纳等价优化 | batch_get（N 项 d044b4c）已按块分组（同块多 key 一次读解压），提案「100 docid=100 块解压」为 N 项前旧态；剩余 IO 放大 = posting 稀疏分散；等价优化 = 回表共享块缓存（Ex-8.3）+ 块序批量 |
| 热点 term 内存 B+Tree（term→docid） | 🔶 采纳等价小改（P2） | 与 G 项重叠：bitmap_fields 白名单内存位图 O(1) + posting LRU 256 项（c380792）已缓存常用 term；增强 = LRU 双区热点化（仿 HotCache promote）+ 容量参数化，非新结构 |
| 倒排+文档联合缓存（term→Vec<Document>） | ⏸ 远期不主动 | 查询结果缓存失效管理复杂（写/删/倒排更新联动）；语义一致性优先；先看 HotCache 命中与点查 QPS |
| 预加载高频 term 完整位图 | 🔶 并入「热点 term」项 | 800MB 内存预算需评测；LRU 已部分覆盖，不单独立项 |

### 8.2 采纳候选汇总（→ development_remain §12）

并行 Flush / 段级 seq 剪枝 / 删除密度 urgency / posting LRU 双区热点化 / 倒排回表共享块缓存（并入 Ex-8.3）；
**Ex-8.1 前置正确性修复（demo range-window 发现）**：① 流式 merge 折叠同源同 key 旧版本（收集 100 vs 流式 110）；
② scan 路径是否消费删除位图（现 get 不可见但扫描仍返回已删 docid，两路径一致——需定语义或对齐）。

## 9. 全局纪元计数器 + 多文件 WAL 提案评估（2026-09-03）

> **来源**：外部提案（`D:\traeprojs\cunji\全局纪元计数器 + 多文件 WAL.txt`）——事务提交先取全局
> 单调纪元号，按 docid 哈希分片到多 WAL 文件并行 fsync，水位 = 各文件 max_flushed_epoch 最小值，
> 崩溃按水位回放（宣称单文件 ~80K TPS → 500K+）。

### 9.1 对照现状

| 提案点 | 现状（代码/实测） | 判定 |
|---|---|---|
| 全局单调序号（纪元） | 每 CF 已有 `seq: AtomicU64`（column_family.rs:134）+ WAL 头持久化 next_seq（M8-P5）+ `set_flushed_seq`（flush 推进）；MVCC 快照即用 seq 排序 | ✅ 已具备（概念同构，非新事物） |
| fsync 摊薄 | 组提交（M8-P0，2ms 窗口统一 fsync，实测 91,296 ops/s）+ 环形 WAL tail 合并 fsync（M8-P12，ring+gc 2ms 68,756） | ✅ 已具备 |
| 多文件 | 三 CF（primary/delta/cidx）各有独立 wal.log；**单 CF 数据流不分片** | 部分具备 |
| 恢复水位语义 | flushed_seq/manifest 持久化，「未刷盘 seq = 未提交」；删除位图 flush 先于 WAL | ✅ 语义等价且更简单（单文件序） |

### 9.2 决策矩阵

| 提案子项 | 判定 | 依据 |
|---|---|---|
| 多文件 WAL（docid 哈希分片 + 并行 fsync） | ❌ 当前不立项 | **单 NVMe 下并发 fsync ≠ 更快**（设备写序串行）；组提交已把 fsync 摊到窗口。实测吞吐上限当前不在 WAL fsync：引擎写锁（O 项写侧仍串行）+ memtable 单写 + 倒排攒批 + 合并背压（A 项 5000 万写放大），拆 WAL 不解锁这些。跨文件恢复需 manifest 水位 + 逐号缺口扫描 O(跨度) 不可行（提案第 262 行亦自认需 manifest） |
| 因果一致性风险 | ✅ 天然不成立 | 本引擎事务均为**单 docid 本地事务**（Ex-1/L1 决策，无跨 docid 事务）→ docid 哈希分片不产生跨文件事务依赖 |
| 500K TPS 目标 | 🔶 需先解除写路径串行 | 无锁多写者（每写者独立 WAL）+ 引擎写锁拆分是前置，且需多设备/NVMe 多队列才见收益；真多设备场景已被 Ex-5.10 条带化（e6a5610，更彻底）覆盖 |
| 批量提交窗口期丢失 | ✅ 语义已定义 | 组提交下「整批持久或整批丢」= 现有未刷盘即未提交语义（WAL 回放 seq ≤ flushed） |

### 9.3 结论

- 方向（序号定序 + 保守水位）已被 **per-CF seq + 组提交 + flushed_seq/manifest** 覆盖且更简单（单文件顺序回放，免跨文件合并排序）。
- 多文件 WAL 并行 fsync 在单 NVMe 无理论收益；**先解除写路径串行**（远期），真多设备走条带化。
- **远期触发项**（development_remain §13）：无锁多写者 + 独立 WAL 每写者 + NVMe 多队列，且吞吐目标实测先指向写锁/倒排/合并而非 fsync。

## 10. "量变优化"五方向提案评估（2026-09-03）

> **来源**：外部建议——①后台预热（L2 索引/FST 字典 madvise 预载）+ 空闲维护；②scan IO 合并与预读；
> ③大查询熔断/降级；④零拷贝深化；⑤统一删除语义。逐项对照现状（已落地项不重复立项）。

| 方向 | 判定 | 依据（现状） |
|---|---|---|
| ①后台预热 L2 索引/FST 字典 | ⏸ 可选小项（P3，Linux 门控） | L2 精确索引本为"每文件首次访问整段读入并常驻"（sstable.rs ensure_index），FST 字典 mmap 按需缺页冷启动亚秒（Ex-5.7）——重复 IO 已消除，预热仅把首查页缺失挪到启动期，收益小；madvise 需 mmap+Linux（Windows 无等价），零 unsafe 白名单约束 |
| ①空闲感知维护（合并/位图回收挪低负载期） | 🔶 采纳候选（Ex-8.9，P3） | 已有保底定时器（mysql worker 10 分钟 + 倒排 GC 信号 10 分钟兜底）与 urgency（W 项）但非"负载感知"；增强 = 低负载窗口收紧合并/回收 |
| ②scan IO 合并与预读 | ✅ 已落地（SCAN_GROUP=8 组读 read_block_group + 组预解码，U 项 85b9a62 / 7.99 ea9b113） | 提案核心已实现；剩余增量 = 扫描挂块缓存 + 残余收集路径组读化（并入 Ex-8.2/8.3）。**纠正判断**：50m 范围慢的主因是收集路径线性索引税（Ex-8.1 已修 86→~5ms），非 IO 模式；5ms→0.84ms 剩余差距主体是全值解码+冷 IO+无块缓存，Ex-8.3 ROI 更高 |
| ③大查询熔断/降级 | ✅ 已具备 | 看门狗（--watchdog-secs 全扫超时熔断）+ sqlish cap=10_000（server.rs /sql + mysql 兜底）+ extract_between_range 窗口上限 min(b,a+10000)；增量可选 = mysql 语句级结果集硬上限配置化（P3，默认已 cap） |
| ④零拷贝深化（扫描免临时 Vec 组装） | 🔶 并入 Ex-8.3 | 流式迭代已内存 O(批)（M8-P10）+ keys-only（7.100）+ 投影裁剪（7.89）；剩余值多次拷贝（merge 回调 val.to_vec）由 Ex-8.3 纯 id 投影 keys-only 消除；全量零拷贝（mmap 块内值引用）生命周期复杂，不做 |
| ⑤统一删除语义（scan 返回已删） | ✅ 已修复（Ex-8.1，e63603a） | get/scan_range/scan_stream/count_all_docs 位图过滤已对齐；**残余**：事务 scan_range_txn（scan_range_at）未查位图 → Ex-8.10 收尾 |

### 采纳汇总（→ development_remain §14）

Ex-8.9 空闲感知维护（P3）/ Ex-8.10 txn 扫描位图过滤（P1 正确性收尾）；其余标注已有或并入 Ex-8.2/8.3。

## 11. L0/L1 Compaction 策略提案评估（2026-09-03）

> **来源**：外部提案（`D:\traeprojs\cunji\L0_L1.txt`）——① 为 L0 构建全局键范围内存索引；② 主动异步压 L0
> （≥4 即合并）；③ L1/L2 改用"延迟大合并"（独立宽松阈值 l1_trigger=8~10、l2=12~16，攒批合并），
> 削减级联写放大（宣称 -50~80% L1→L2 次数）。逐项对照现有实现：

| 提案 | 判定 | 依据（现状） |
|---|---|---|
| ①L0 全局键范围索引（BTreeMap<min_key, files>） | ✅ 已落地（R 项 388a916） | 层/段两级 Zone Map 范围粗筛：SstSnapshot 层范围/层索引 + 段级 O(1) 越界跳过（get_bytes 层遍历整层跳过）——点查不扫全 L0；L0 重叠文件含该 key 者本就需逐个查（LSM 固有），非索引可解 |
| ①文件系统式存储引擎 | ❌ 不推荐（质变重构，提案自判） | 与量变策略相悖，元数据/并发/事务复杂度剧增 |
| ②主动异步压 L0（≥4 合并） | ✅ 已落地 | P 项 auto_compact 写路径事件驱动 + O 项后台无锁合并 worker + 动态窗口 l0_stall 8~16（基础 12，Ex-5.4 调优）+ 合并冷却 cooldown=2 + urgency 调度（W 项）——"异步主动压 L0"即现形态；提案推断 l0=4 过时 |
| ③L1/L2 延迟大合并（独立宽松阈值） | 🔶 **值得做，采纳为受控实验（Ex-8.11，P2）** | 核实 [column_family.rs:1573-1583] `needs_compact`：`l0==0 && (l1>1 || l2>1)` 即触发 → **级联属实**：每次 L0→L1 后 L1 段数>1 便全收敛到单段（日志"合并 N 段 → 1"），底层层反复全量重写 = 写放大来源；代价 = L1 多文件下范围扫描源数增多（读放大） |

### 11.1 关键权衡与约束

- **收益**：L1→L2 次数 -50~80%（攒 4→8~12 段合并），底层重写字节与 Compaction CPU 显著下降；
  点查影响小（L1 无重叠 + Zone Map/布隆 → 定位单文件）。
- **风险（读放大）**：范围/全扫在 scan_stream_at 对**每个 L1 文件建迭代器**（现无层/文件剪枝）——
  若先放宽 L1 阈值后做 Ex-8.2 剪枝，范围查询源数随 L1 段数线性涨。
- **实施顺序约束**：**Ex-8.2（scan 层/文件剪枝）先行**，再放宽 L1/L2 阈值（或同批落地），
  否则范围 p50 回退。另注意现收敛策略是"L1/L2 最终 1 大段"（50m 约 400MB/段），
  放宽后为多中段 → 段大小与 compact_input_max_mb/合并冷却/urgency 参数共存。

### 11.2 结论

- ① ② 已落地不重复立项；①的"文件系统式"否决。
- ③ 判定**值得**：级联写放大属实（代码核实），延迟大合并在读放大可控（先 Ex-8.2）前提下收益明确。
  以 compaction-tune/50m 库 A/B 实验量化（Ex-8.11），不直接改默认。

## 12. 压缩策略两方向提案评估（2026-09-03）

> **来源**：外部建议——①分层压缩（L0/L1 热层轻量快压缩，L2+ 冷层高压缩率）；②共享字典压缩
> （ScyllaDB 式 ZstdWithDict，全表训练字典跨块复用）。对照现状：[config/model.rs:552-565]
> compression="zstd" + compression_level=3 **全层统一**；CF 持 (compression, level) 单组，
> compaction 输出已带 `out_level`（column_family.rs:1357）——分层落地点已具备。

| 方向 | 判定 | 依据与权衡 |
|---|---|---|
| ①分层压缩（L0/L1 轻量 ↔ L2+ 高等级 zstd） | 🔶 **采纳为受控实验（Ex-8.12，P2）** | zstd 特性：**压缩等级只影响压缩侧，解压速度基本不随等级变化** → L2+ 提高等级（如 6~15）近乎"免费"拿压缩率/存储/读 IO 收益，代价仅为后台 compaction CPU。落地点 = 按 out_level 选 (compression, level)（flush→L0 用热档如 lz4/zstd3；compact out L1/L2 用冷档）。**风险**：① L0/L1 用 lz4/none 会放大中间层体积与 L0→L1 重写 IO；② Ex-5.8 数据块级复用要求源/目标压缩一致——跨层等级变化使复用失效（L1→L2 需重压缩）；③ 与现有 A 档"空间放大"测试联动。需 50m 库 A/B 量化（空间/写放大/范围读）后调默认 |
| ②共享字典压缩 | ⏸ 远期锦上添花，不立项 | 需后台字典训练/更新/老化 + 字典元数据与文件生命周期管理 + zstd-rs dictionary API 接入（零 unsafe 约束内可行）；文档库 JSON 字段名重复度高（status/city 等），字典理论增益存在。触发条件：先做 Ex-8.12（无字典 zstd 高等级）后空间仍瓶颈且压缩率需求明确再评估 |

### 12.1 结论

- ① 分层压缩值得做：优先验证「L0/L1 维持 zstd3（避免中间层放大）+ L2+ zstd 6~15」档位，
  量化空间/写放大/范围读后决定默认（Ex-8.12）。
- ② 共享字典：远期触发项（ScyllaDB 式），当前不投入训练/生命周期基建。

## 13. 阈值联动 / 一致性表述澄清 / 存量优化复核（2026-09-03）

> **来源**：外部评估一批。逐项复核后：两处"矛盾"基于与实现不符的前提 → 澄清；若干"优化"与已排 Ex-8
> 拆点合并或修正。

| 项 | 判定 | 分析与修正 |
|---|---|---|
| L0/L1 触发阈值联动（l0=4 + l1=12 攒批） | 🔶 并入 Ex-8.11 参数档 | 方向与 Ex-8.11 一致；纠正过时前提：L0 现为**动态 8~16**（Ex-5.4 调 8→12），不回退 4（L0 重叠层过度放宽会推高点查/范围负担）。Ex-8.11 A/B 档含 l1 攒 8~12 与 l0 维持现状两组 |
| 矛盾③：FST+mmap 与 Checkpoint | ✅ 澄清（前提不符） | 本引擎 FST **按段增量构建**（flush_segment 编译新段字典）并经 `ArcSwap<HashMap<seg, Fst>>` rcu 原子发布（inverted.rs:78,912），mmap 按需缺页——**不存在"30 分钟全量重建、重建期间扫描全部倒排文件"**；旧字典 Arc 快照在替换前持续服务，一致性天然保证。真实小问题 = 倒排 flush/GC 与 compaction 的 IO 争用 → 新增 Ex-8.13（倒排写纳入后台 IO 预算 + 空闲窗口执行） |
| 矛盾④：异步数据复制 vs 元数据 Raft | ✅ 澄清（角色分离） | Raft 仅复制**元数据/序**（MetaOp → MetaCenter 状态机，raft-meta/raft-rpc）；**数据**= 单写者主节点 + 异步日志/outbox 幂等 apply（Ex-1 L1）——主从切换丢失仅限 async 数据复制未排空（scale_out 已要求 drained 才切换），与元数据强一致不冲突。用户取舍 = 现有语义已定（元数据强一致 + 数据最终一致 + 单机事务强一致）；"sync 数据复制可选"列为远期（跨地域/金融场景触发），无需性能-一致性矩阵产品化 |
| 优化1：L0 文件范围索引强化（点查先查范围索引） | ✅ 已落地（R 项）+ 强化收益有限不立项 | 点查现为"层 Zone Map → 段 O(1) 越界跳过"（非逐段布隆反序列化，R 项 388a916）；L0 段数被 auto_compact 压在 8~16 → O(L0) 检查已小。"二分候选段"仅当未来放宽 L0 段数（Ex-8.11 不改 L0）才启用，记录在 Ex-8.11 注意项 |
| 优化2：删除密度 urgency | ✅ 已采纳（Ex-8.7 P3） | 维持 |
| 优化3：并行 Flush → 改增大 MemTable | 🔶 **修正 Ex-8.5 路径** | 采纳"先 memtable 档位 A/B（256→512MB，config 一行零代码）而非 400 行并行 flush"——双缓冲切换已不阻塞写；并行 flush 仅当档位实验证实 flush 为瓶颈再实施。并行 flush 额外推高 L0 数/compaction 压力，非优先 |
| 优化4：负载感知空闲维护（Ex-8.9） | 🔶 补充拆点 | 已排 Ex-8.9；监控指标（QPS/CPU/IO 等待/L0 数）与动作（低负载收紧合并阈值 + 倒排 GC + 深度合并 L0→1）并入拆点；倒排 IO 共享预算见 Ex-8.13 |

## 14. 热度感知自适应多级 Block 索引提案评估（2026-09-03）

> **来源**：外部提案（`D:\traeprojs\cunji\block_index.txt`）——热 Block 建"块内行级细索引"
> （row_offset 直读，跳过整块解压，宣称热块点查 ~20×/0.1ms），冷区索引合并降内存；后台分裂/合并。
> **判定：❌ 不采纳**（关键前提与数据布局冲突 + 目标已被既有缓存层覆盖）。

| 维度 | 评估 |
|---|---|
| **硬伤：行级偏移无法越过块级 zstd 解压** | 数据块 = 4KB raw 行集**整块 zstd 压缩**（sstable 每块独立压缩+Trailer）；行字节在压缩流内，不存在磁盘/文件级"row offset 直读 200B"——要取单行仍须先解压整块。细索引只能定位"解压后块内的行"，跳不过解压这一主成本（提案自身示例 A 自相矛盾） |
| **目标已被覆盖** | 热块二次访问 = BlockCache（点查路径缓存解压后块，Ex-8.3 已扩到扫描）+ HotCache（行/文档级，热点文档 0.05μs 级）——"热 block 加速"已由缓存命中承担；细索引仅能优化热块的**首次未缓存读**，而"热"定义即反复访问 → 缓存覆盖，边际收益≈0 |
| **mmap 前提不适用** | 数据块当前非 mmap（read_at + zstd + 块缓存；仅倒排 FST 字典 mmap，Ex-5.7）。提案"基址+offset 零拷贝直读行"在本数据布局不存在；LSM 写/compact 重写场景 mmap 数据面的缺页/SIGBUS/msync 风险（提案自列）也不支持改 mmap |
| **成本/风险** | 自适应树（分裂/合并/热度衰减）+ 序列化扩展段 + 后台维护线程 + 索引并发 ~1000 行/3-4 周 + 格式变更；对比收益极不成比例 |
| **冷块点查放大** | 冷随机点查 = 读 1 压缩块+解压取 1 行——这是块压缩存储的固有形态（InnoDB 页同构），非缺陷；行内扫描本身 ~us 非瓶颈 |

**可选留存（不建议立项，记录备选）**：行式块内无重启点，块内 scan 为顺序扫至命中（~130 doc/块）；若未来块内查找成为实测瓶颈，可加块内稀疏重启点/二分（纯内存/格式微调），P3 微项。现阶段点查路径（Zone Map→L2 二分→分区布隆→块缓存）已覆盖。

## 15. 写路径/锁优化提案审计（2026-09-03）

> **来源**：外部建议——①写路径阶段化提交（持锁只做内存、释放后再 fsync + 后台 ack，宣称 50k→500k TPS）；
> ②MemTable 切换原子指针化；③deletion_bitmap RwLock → ArcSwap 无锁快照。逐项对照代码：

| 项 | 判定 | 依据 |
|---|---|---|
| ①写路径阶段化提交（fsync 移锁外 + 后台 ack） | ❌ 不采纳（前提部分过时） | **fsync 早已不在写锁内**：put_nosync 锁内只做 WAL 内存 append + memtable put + 倒排内存攒批；落盘由组提交窗口统一 fsync（M8-P0，2ms 批次，实测 68~91k ops/s）。50k-65k（协议接入）瓶颈 = 单写者串行 + 倒排/合并背压，非锁内 fsync。改"后台 ack"破坏现同步耐久语义（put 返回 = 已入组提交窗口），需新确认协议，与组提交重叠收益有限——**多写者 + 引擎写锁拆分才是真上限**（已列为远期 Ex-13 触发项） |
| ②MemTable 切换原子指针化 | ✅ 已具备（双缓冲 + P72/Z 无锁合并） | MemTableBuffer 已 RwLock &self 化 + immutable 冻结 + 写路径在引擎写锁内切换（不并发于写）；memtable 插入为 crossbeam 无锁。无需改动 |
| ③deletion_bitmap 读路径无锁化 | 🔶 **采纳小优化候选（Ex-8.14，P2）** | 现状 bits: RwLock<Vec<u64>>，`is_deleted` 每调用取读锁——**scan/count 逐行判定**（50m 行级）读锁竞争真实存在。方向：位图读无锁化（读侧原子 u64 位组直接读 + 写侧按"扩容才需锁"设计，或 seqlock/ArcSwap 只读快照），保留 4KB 脏页 fsync 持久语义 |
| 锁护城河复核（读读并行/ArcSwap/倒排分段） | ✅ 属实 | O 项 RwLock 读读并行（1 亿 read_only +13.3×）、ssts ArcSwap 无锁快照、倒排 256 分区锁——外部评估与本实现一致 |

### 采纳汇总（→ development_remain §18）
Ex-8.14 deletion bitmap 读无锁化（P2）；写路径多写者维持远期（Ex-13/§9 触发链）。

## 16. 存储格式现状 vs 设计 3.4 澄清（2026-09-03）

> **触发**：外部文档模型分析（文档库=数据模型非存储格式）核对代码后发现 design 3.4 与实现存在偏差，需显式记录，避免后续按错误前提排期。

| 项 | design 3.4（文档描述） | **实际实现（代码核实）** | 判定 |
|---|---|---|---|
| 文档序列化 | 自定义紧凑格式：DocId u64 + 字段数 u16 + 字段ID u16 + 类型标签 u8 + Varint（字段名→u16 ID 注册表） | **未按此落地**。SST Value = 文档序列化字节；mysql 接入为"行式字段列集（status/city/amount/ts/title 等独立列）+ doc 字段 JSON 原文值"，主键=docid | 实现 ≠ 设计 |
| 字段名压缩 | 字段名以 u16 ID 替代（省 3-5×） | 无全局字段ID注册表重编码；字段名随 JSON 原文存储 | 未落地 |
| 字段读取 | O(1) 定位（头部含字段ID） | 7.96 字节级跳过 + serde fallback：light_top_field 快路径扫列、嵌套/畸形 bail → serde_json 实时解析 | 读路径实时解析 |
| 结论 | — | 现状 ≈ **"行式字段列 + JSON 原文值"**（近似 MySQL 行式对照形态），非 design 3.4 的字段ID 紧凑文档二进制 | 记录差异 |

### 含义与后续
- **"文档数据库"定位成立**（数据模型 = 文档；弱 Schema / 任意字段检索 / 文档级 API），与存储格式无关。
- **嵌套字段**：存储透传 ✅（JSON 原文保留嵌套）；**检索需路径物化**——当前字段级倒排仅扁平字段
  （status/city），嵌套路径检索依赖 mysql WHERE 的 light 字段判定/组合索引前缀；7.92 嵌套投影是
  **投影层**能力非索引层。嵌套路径入索引 = 显式声明（对齐 Ex-4 倒排白名单 ≤20 字段策略）。
- **是否落地 design 3.4 字段ID紧凑编码**：属存储格式变更（有空间收益），按数据资产保护约定需
  显式排期 + 迁移验证（P3 远期，不主动排期）；若维持现状则建议**修订 design 3.4 描述为行式列集+JSON 值**，
  消除文档与实现偏差（待定，改动设计文档需用户确认）。

## 17. "桶排序 / 分区 LSM" Compaction 提案评估（2026-09-03）

> **来源**：外部提案——把 L0 文件按 Key 范围分桶，只对重叠度高的桶执行 Compaction（"桶感知合并"），
> 宣称降低单次合并代价与写放大；并建议四阶段（加 L2 层 → 逻辑桶 → 热度/墓碑感知 → 冷热分离）。
> 多轮讨论后判定：**方向正确但四阶段大多与现状重叠/已落地；固定哈希分桶与主键模型（docid 无界单调）
> 相悖，不立项**。逐项结论如下。

| 提案点 | 判定 | 依据（代码/数据模型核实） |
|---|---|---|
| L0→L1"全局排序重写全部"的前提 | ❌ 前提不符（已证伪） | L0 flush 逐批 → 单 L1 段（有界）；无重叠段合并走 Ex-5.8 **块级复用**（[column_family.rs](file:///D:/traeprojs/shanshui-cunji/src/column_family.rs) `try_meta_only_compact`，零解压只重建元数据）；单次输入 `cap_by_size` 分批 + 合并冷却——"重写全部"的代价已被局部化+块级复用拆掉，"桶收益"大多已覆盖 |
| 阶段① 增加 L2 层 | ✅ 已落地 | L2 收敛（level≥2 底层单段）+ Ex-8.11 `l1/l2_trigger_files` 延迟大合并（8ec3a70） |
| 阶段② 固定数量桶（方案 A：不增多） | ❌ 不采纳（回避核心矛盾） | 主键 = **auto-increment docid 无界单调**：固定桶数 + 高 bit 前缀映射 → 写入永远落最后一桶（倾斜）；等宽区间映射 → 桶数须随 max_docid 增长（用户直觉正确：**桶会增多**）；固定桶数 = 每桶宽度无限增大、名存实亡。方案 A 仅在**有界 key 空间**（UUID/固定前缀分片）成立 |
| 阶段② 动态分裂桶（方案 B） | ❌ 不立项 | 即"动态等宽/自适应桶"= 桶数随数据增长；需维护分裂/边界重叠/桶间 GC 状态机，且与现有"时间窗口 L0 → L1 → L2"层级收敛**同构**（docid 单调下新写永远在右端 = 增长式时间分区）——现架构已隐式实现其等价物 |
| 阶段② "重叠度高优先合并" | ❌ 前提与写主路径不符 | **主数据 L0 文件先验重叠 ≈ 0**：docid 单调 → 连续 flush 的文件 docid 区间互不相交（L0 覆盖范围向右单调扩张，但文件间无重叠）。重叠仅出现在 UPDATE 旧 docid / 删除后复活回写旧 docid / delta patch（key=docid+field）——即随机/更新密集负载而非写主路径 |
| 阶段③ 热度感知 / 墓碑优先 | ✅ 已落地 | 热度 = Ex-5.9 热段先沉 L1 + 合并冷却 + 写压力动态窗口；墓碑优先 = Ex-8.7 删除密度 GC（a6d53c3） |
| 阶段④ 冷热分离（SST 维护访问频率） | ❌ 不建议 | 读热已被 HotCache 吸收、写热在 docid 单调下等价新旧分层；元数据维护访问频率成本高、收益被缓存层覆盖 |
| **真正增量：宽文件 range 局部合并（sub-compaction）** | 🔶 P3 触发项（本次讨论唯一新增缺口） | "固定桶归属"默认每文件 range 窄，但 flush 文件 range = memtable 生命周期写入区间，**单文件天然横跨多桶** → 归多桶也无法只并子集（文件不可拆分）。要兑现"只合并重叠段"，需要的是宽 L0 文件按重叠段**切分后局部重写**（RocksDB sub-compaction 类），复杂度显著高于桶元数据。仅当 UPDATE 旧 docid 成为主要负载时才评估 |

### 17.1 重叠度量化（若未来启用）

- 原料已具备：每文件 [min, max] 在 `SstSnapshot` 范围元数据 / 块索引首末键（Ex-8.2 `sst_intersects_window` 剪枝同源）；
- 桶内按 min 排序扫区间并集 = **O(F log F)**，`overlap = 1 − 并集长 / Σ文件范围长`；
- 判定"高"取语义阈值（对齐读放大目标），调度信号先单一实测再调权，不采用拍脑袋固定权重（如 0.4/0.6）。

### 17.2 Ex-8.1 50m 复测结果（2026-09-03，随本评估落档）

> 环境：SCC 3308（db-bench-mysqlcmp-50m，release 新二进制，Ex-8.1~8.7 全带）vs MySQL 3306 bench50m
> （8G buffer pool），两端 orders 50m 行内容一致。

| 指标 | 修复前（50m final 报告） | 本次复测 | MySQL | 差距 |
|---|---|---|---|---|
| 范围查询 id BETWEEN 100 行窗口 p50（200 次，6 窗口位置无关） | 86ms | **2.48ms** | 0.70ms | ~102× → **3.6×** |
| 点查 QPS（16 线程×15s 随机） | ≈5.2k | **6,478** | 7,587 | 1.35× → **1.17×** |

- 复测脚本：tmp_range_recheck_50m.py / tmp_ex8_recheck.py；报告 tmp_ex8_mysql_50m_recheck.md；
- Ex-8.1 验收目标达成（范围 100× → 个位数×，且窗口位置依赖消除 = 收集路径根因已移除）；
  详细逐窗口数据见上述脚本 stdout。

### 17.3 结论

- 不立项"桶/分区 LSM 重构"；四阶段与现状重叠/已落地部分不重复排期。
- 唯一留存触发项 = **宽 L0 文件 range 局部合并（sub-compaction）**，P3、需随机主键/更新密集负载实测触发。
- 复评触发条件（若出现任一）：随机主键（UUID/业务键）+ 删除密集 + 多写者并发数据模型。

## 设计决策边界（已定，不重复评估）

- 桶 / 分区 LSM（固定哈希分桶 + 桶内重叠度调度 Compaction）：不立项（§17——主键无界单调、L0 先验零重叠、现有层级收敛已同构等价）

- 2PC / TCC / Seata 本体：不做（L1 outbox + L2 SAGA 已覆盖）
- SAGA vs Calvin：SAGA 是当前与近期的答案；Calvin 远期触发
- 读写分离：暂缓（组提交已解决）
- 无锁合并：分批缓解已落地，根治方案已记录（P72）
