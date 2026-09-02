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

## 设计决策边界（已定，不重复评估）

- 2PC / TCC / Seata 本体：不做（L1 outbox + L2 SAGA 已覆盖）
- SAGA vs Calvin：SAGA 是当前与近期的答案；Calvin 远期触发
- 读写分离：暂缓（组提交已解决）
- 无锁合并：分批缓解已落地，根治方案已记录（P72）
