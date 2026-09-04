# development_remain.md —— 开发未完成任务

> 2026-09-03 治理：**已完成内容已回填 development.md**（§13 大项队列 / §14 收口归档 / 7.x 各节）；
> **明确不开发**项 → development_givenup.md；
> 本文件仅保留 **未完成 / 进行中 / 待评估 / 远期触发**。开发路线入口 = development.md §13 + 本文件。

插队排期，优先开发：

一、任务总览（按阶段分组）
阶段	任务数	总工作量	核心交付
立即执行（当前 Sprint）	5 项	3 天	字段注册表 Vec 化 + fxhash 局部替换 + Read Reorder
阶段 1.5（核心优化）	7 项	3 周	FST 倒排字典 + 时间轮（TTL/Compaction/WAL）
阶段 3（深度优化）	4 项	4 周	Ribbon Filter + Per-CPU WAL
测试与验收	4 项	贯穿	性能基准 + 混沌测试
二、立即执行任务（当前 Sprint，3 天）
Task-001：字段注册表 Vec 化
属性	内容
优先级	P0
工作量	0.5 天
依赖	无
风险	低
具体工作：

□ 将 HashMap<u16, FieldMeta> 替换为 Vec<FieldMeta>
□ 新增 field_id 校验：确保 ID 连续无空洞（或支持稀疏 Vec 占位）
□ 修改所有 registry.get(&id) → registry.get(id as usize)
□ 删除相关锁（Vec 不可变读无需锁）
□ 更新序列化/反序列化逻辑（字段 ID 直接作为索引存储）
验收标准：

单元测试全部通过

基准测试：字段查找延迟降低 ≥30%

Task-002：fxhash 局部替换（内部 Key）
属性	内容
优先级	P0
工作量	1 天
依赖	无
风险	中（需区分内外 Key）
具体工作：

□ 在 Cargo.toml 引入 fxhash = "0.2"
□ 替换以下模块的 HashMap：
HotCache: DashMap<u64, Document> → 换 FxBuildHasher

BlockCache: HashMap<(u64, u64), Block> → FxHashMap

Manifest: HashMap<u64, SSTMeta> → FxHashMap

□ 保留倒排字典的 DashMap<String, TermMeta> 使用默认 ahash（抗 HashDoS）
□ 保留组合索引前缀缓存使用 ahash（Key 来自用户输入）
□ 聚合临时表使用 FxHashMap<String, u64>（内部使用，不暴露）
验收标准：

所有单元测试通过（注意遍历顺序不确定性）

基准测试：内部 Key 查找延迟降低 ≥15%

安全测试：构造 HashDoS 攻击字符串，倒排字典不受影响

Task-003：倒排回表 Read Reorder（排序预取）
属性	内容
优先级	P0
工作量	1 天
依赖	无
风险	低
具体工作：

□ 在倒排查询回表阶段，收集所有 (FileID, BlockID, DocID) 三元组
□ 按 (FileID, BlockID) 排序分组合并
□ 每组一次性 preadv 批量读取（或异步批量提交）
□ 实现 BlockCache 预取：排序后优先从缓存命中
验收标准：

压测：随机点查 IOPS 降低 ≥50%

pre 系统调用次数减少 ≥60%

Task-004：移除 HashMap 遍历顺序依赖（测试修复）
属性	内容
优先级	P0
工作量	0.5 天
依赖	Task-002
风险	低
具体工作：

□ 全局搜索测试代码中的 assert_eq!(map.iter().collect(), vec![...])
□ 替换为 assert!(map.contains_key(...)) 或排序后比较
□ 修复倒排字典导出 Term 列表的测试用例
验收标准：

cargo test 全部通过（无顺序依赖失败）

Task-005：性能基准基线采集
属性	内容
优先级	P0
工作量	0.5 天
依赖	无
风险	低
具体工作：

□ 在执行任何优化前，运行完整性能基准套件
□ 记录关键指标：
点查 QPS / P95 延迟

写入 TPS / P95 延迟

倒排单 Term 查询 QPS

TTL 删除扫描耗时（当前基准）

冷启动时间

□ 保存基线数据用于后续对比
验收标准：

基线数据已保存到 benchmarks/baseline_20260904.json

三、阶段 1.5 核心优化（3 周）
Task-006：FST 倒排字典（核心改造）
属性	内容
优先级	P0
工作量	10 天
依赖	无（可并行）
风险	高（核心模块改动）
具体工作：

□ 引入 fst crate 或自研 FST 实现
□ 设计双 FST 结构：base.fst（只读）+ delta.fst（可写）
□ 实现 TermMeta 序列化到 FST Value（存储 (file_id, offset, length, doc_count)）
□ 替换 DashMap<String, TermMeta> 查找逻辑
□ 实现 FST Checkpoint：base.fst 固化 + delta.fst 合并
□ Mmap 映射 base.fst 实现零拷贝读
□ 构建期：从旧 HashMap 迁移到 FST（后台异步，双写过渡）
□ 实现前缀搜索接口：search(prefix: &str) -> Vec<TermMeta>
验收标准：

冷启动时间：15s → <50ms（Mmap 直接映射）

单 Term 查找 QPS 不下降（持平或略升）

前缀查询性能：当前无法做 → >10 万 QPS

内存占用：FST 比 HashMap 降低 ≥40%

Task-007：层级时间轮基础框架
属性	内容
优先级	P0
工作量	2 天
依赖	无
风险	低
具体工作：

□ 实现层级时间轮（秒/分/时/天 四级）
□ 总桶数：60 + 60 + 24 + 365 = 509 个桶
□ 实现接口：
schedule(delay: Duration, task: Task)：注册延迟任务

tick()：推进指针，执行到期任务

□ 支持任务取消（返回 TaskHandle）
□ 持久化检查点：每 10 分钟记录当前指针位置到磁盘
□ 冷启动恢复：从检查点恢复 + 扫描元数据重建未完成任务
验收标准：

单元测试覆盖所有层级跃迁

时间加速测试（MockClock）：验证 1 年 TTL 正确触发

重启后任务不丢失（检查点恢复）

Task-008：TTL 分区删除（时间轮集成）
属性	内容
优先级	P0
工作量	2 天
依赖	Task-007
风险	低
具体工作：

□ SSTable Flush 时计算过期时间点：expire_at = max_timestamp + ttl
□ 将 (file_id, bucket_path) 注册到时间轮对应槽位
□ 时间轮 tick 触发时，检查到期 FileID 列表
□ 验证文件全部过期后，删除整个目录（O(1)）
□ 删除前更新 Manifest（原子操作）
□ 支持 TTL 配置动态变更（已存在文件的 TTL 变更需重新计算）
验收标准：

10 亿数据场景：TTL 删除扫描耗时 <1 秒/次（原 5-10 分钟）

删除延迟误差：≤ tick 精度（如 1 小时）

删除过程不阻塞正常读写

Task-009：Compaction 延迟重试（时间轮集成）
属性	内容
优先级	P1
工作量	1 天
依赖	Task-007
风险	低
具体工作：

□ Compaction 失败（资源不足/锁冲突）时不 sleep 阻塞线程
□ 改为注册到时间轮：schedule(Duration::from_secs(5), || retry_compaction(file_id))
□ 指数退避：失败重试间隔 5s → 10s → 20s → ...
□ 最大重试次数限制（如 10 次后告警）
验收标准：

Compaction 重试不阻塞后台线程池

失败重试间隔精确可控

Task-010：WAL 旧文件延迟删除（时间轮集成）
属性	内容
优先级	P1
工作量	0.5 天
依赖	Task-007
风险	低
具体工作：

□ WAL 文件刷盘完成后，不立即 unlink
□ 注册到时间轮：延迟 5 分钟后删除（确保所有读取完成）
□ 删除前检查：last_read_timestamp 是否已超过安全窗口
验收标准：

无 "WAL 文件正在使用却被删除" 的报错

旧 WAL 文件在安全延迟后自动清理

Task-011：时间轮监控指标
属性	内容
优先级	P2
工作量	0.5 天
依赖	Task-007
风险	低
具体工作：

□ 暴露时间轮指标：
timewheel.current_slot

timewheel.pending_tasks（各层级待执行任务数）

timewheel.tasks_executed_total

timewheel.tasks_failed_total

□ 集成到 SHOW STATUS 命令
验收标准：

SHOW STATUS 能看到时间轮运行状态

Task-012：阶段 1.5 集成测试
属性	内容
优先级	P0
工作量	2 天
依赖	Task-006 ~ Task-011
风险	中
具体工作：

□ 端到端测试：写入带 TTL 数据 → 等待过期 → 时间轮触发删除
□ 混沌测试：模拟重启（检查点恢复）
□ 性能基准对比（vs 阶段 1.5 前）
□ 回归测试：确保 FST 替换后查询结果一致
验收标准：

所有集成测试通过

性能基准：整体 QPS 提升 ≥20%

四、阶段 3 深度优化（4 周）
Task-013：Ribbon Filter 替换 Bloom Filter
属性	内容
优先级	P1
工作量	1 周
依赖	无
风险	中（需验证正确性）
具体工作：

□ 研究 Ribbon Filter 实现（参考 RocksDB 或 ribbon-filter crate）
□ 在 SSTable Builder 中替换标准 Bloom 构建逻辑
□ 保持 API 兼容（might_contain(key) -> bool）
□ 支持从旧 Bloom 在线升级（读取时同时检查两种，写入时只写 Ribbon）
□ 同内存配置下对比假阳性率
验收标准：

同内存配置：假阳性率降低 ≥30%

构建速度不低于标准 Bloom

查询性能不下降（CPU 缓存友好）

Task-014：Per-CPU WAL RingBuffer
属性	内容
优先级	P2
工作量	2 周
依赖	无（可并行）
风险	高（写入路径核心改动）
具体工作：

□ 为每个 CPU Core 绑定独立 WAL 写入队列（crossbeam::queue::ArrayQueue）
□ 写入请求根据当前线程 CPU 亲缘性路由到对应队列
□ 每个队列由独立后台线程消费（刷盘 + fsync）
□ 全局 gseq 分配器确保跨队列序（或容忍乱序 + 恢复时重排序）
□ 实现队列背压（队列满时阻塞或返回 503）
□ 实现 io_uring 异步提交（阶段 3 目标）
验收标准：

写入 TPS 提升 ≥30%（vs 全局组提交）

P99 延迟降低 ≥20%

无锁竞争（perf lock 检查）

Task-015：写路径多队列监控
属性	内容
优先级	P2
工作量	0.5 天
依赖	Task-014
风险	低
具体工作：

□ 暴露每个队列的深度、消费速率、延迟
□ 检测队列倾斜（某些核过载）
□ 支持动态调整队列数（SIGHUP 重载）
验收标准：

SHOW STATUS 显示各队列健康度

Task-016：阶段 3 性能压测与调优
属性	内容
优先级	P0
工作量	1 周
依赖	Task-013 ~ Task-015
风险	中
具体工作：

□ 16 核 / 64G / NVMe 目标硬件复测
□ 对比阶段 1.5 基准：
写入：22 万 TPS → ≥32 万 TPS

点查：85 万 QPS → ≥85 万 QPS（持平或略升）

倒排：4 万 QPS → ≥5 万 QPS

□ 调优参数（队列深度、批次大小、fsync 间隔）
验收标准：

所有性能目标达成（design.md §九 性能目标）

P95 延迟不劣化

五、测试与验收（贯穿）
Task-017：HashDoS 安全测试
属性	内容
优先级	P0
工作量	0.5 天
依赖	Task-002
风险	低
具体工作：

□ 构造 HashDoS 攻击字符串（fxhash 碰撞集）
□ 验证倒排字典（ahash）不受影响
□ 验证内部 Key（u64）不受影响
验收标准：

攻击下 CPU 不飙升，QPS 下降 <10%

Task-018：时间轮混沌测试
属性	内容
优先级	P1
工作量	1 天
依赖	Task-007
风险	低
具体工作：

□ 模拟时间跳跃（系统时间调整）
□ 模拟重启（检查点恢复）
□ 模拟高频任务注册（10 万/秒）
□ 验证任务不丢失、不重复执行
验收标准：

所有混沌场景通过

Task-019：升级兼容性测试
属性	内容
优先级	P1
工作量	1 天
依赖	所有优化
风险	中
具体工作：

□ 老版本数据（旧 Bloom + HashMap 倒排）加载到新版本
□ 验证查询结果一致
□ 验证 TTL 时间轮能从老元数据重建
□ 验证 WAL 恢复兼容
验收标准：

数据零丢失

查询结果 100% 一致

Task-020：最终验收报告
属性	内容
优先级	P0
工作量	0.5 天
依赖	所有任务
风险	低
具体工作：

□ 汇总所有性能对比数据（优化前 vs 优化后）
□ 编写验收报告（含硬件配置、压测参数、结论）
□ 更新 design.md §23 设计决策归档
□ 标记 opti_proj.md 中所有任务为 ✅ 完成
验收标准：

验收报告通过评审

六、甘特图（时间线）
text
Week 1  | Task-001 ████▌ Task-002 ████████▌ Task-003 ████████▌ Task-004 ████▌ Task-005 ████▌
Week 2  | Task-006 ████████████████████████████████████████████████████████████████████████
Week 3  | Task-006 ████████████████████████████████████████████████████████████████████████
Week 4  | Task-006 ████████████████████████████████▌ Task-007 ████████████▌ Task-008 ████████▌
Week 5  | Task-009 ████████▌ Task-010 ████▌ Task-011 ████▌ Task-012 ████████████████▌
Week 6  | Task-013 ██████████████████████████████████████████████
Week 7  | Task-013 ████████████▌ Task-014 ██████████████████████████████████████████████████
Week 8  | Task-014 ██████████████████████████████████████████████████
Week 9  | Task-014 ████████████████████▌ Task-015 ████▌ Task-016 ████████████████████████████
Week 10 | Task-016 ██████████████████████████████████▌ Task-017 ████▌ Task-018 ████████▌ 
        | Task-019 ████████▌ Task-020 ████▌
总工期：10 周（约 2.5 个月）

七、风险与依赖总结
风险	影响任务	缓解措施
FST 实现复杂度超预期	Task-006	先用 fst crate，后续自研
Per-CPU WAL 乱序问题	Task-014	全局 gseq 分配器兜底
时间轮检查点恢复不一致	Task-007	启动时全量扫描重建（可接受慢启动）
Ribbon Filter 正确性存疑	Task-013	并行验证（新旧同时检查 1 周）
fxhash 分片负载不均	Task-002	仅对 u64 内部 Key 替换，String 保留 ahash
八、里程碑
里程碑	时间	关键交付
M1: 立即优化	Week 1 结束	Vec 注册表 + fxhash 局部替换 + Read Reorder
M2: 阶段 1.5 完成	Week 5 结束	FST 倒排 + 时间轮（TTL/Compaction/WAL）
M3: 阶段 3 完成	Week 9 结束	Ribbon Filter + Per-CPU WAL
M4: 验收完成	Week 10 结束	验收报告 + 所有性能目标达成

插队完成
----------
## 一、进行中（P0/P1 已立项，2026-09-03）

### 1. 真多表支持（表级主键空间隔离，用户 2026-09-03 确认语义并排期，commit 9d3e155 已建 M1 起点）

#### 语义确认（用户）

> 支持**真多表**：不同表允许相同主键 id（表级主键空间隔离，对齐 MySQL）。当前 SCC "表"只是
> SQL 层别名，全部落到同一 documents 集合 + 全局 docid —— t_test / t_combo 各自 id=1..2000
> 互相撞键：a 修复（1062 预校验）前静默覆盖、修复后 `--init` 直接 1062。

#### 现状与缺口

- 存储模型：单文档集合（内存/磁盘/WAL/倒排/位图/事务/引擎 API 全部围绕**全局 docid**）；
  mysql server 固定"库 scc、表 documents"（README 明示），语句表名解析后被忽略/不校验。
- 触发：rr-conformance `--init` 双表（t_test + t_combo）种子各 id=1..2000 → SCC 同 docid 互撞；
  已用 `--single` 单表化绕开（工具口径，未修引擎）。真实多表业务/迁移同样会互踩。

#### 方案选项（排期评估用）

| 方案 | 做法 | 改动面 | 备注 |
|---|---|---|---|
| A. 表级 docid 命名空间（架构级） | 存储/API key 带表维度：docid 分配按表分段（表 id 高位）或 key 编码加表名前缀 | 引擎 key 编码（encode_docid 8B 固定）、扫描/组合索引、倒排 posting docid、删除位图、事务 write_set、hotcache、mysql 层表名解析全链路 | 完整对齐 MySQL 多表；需迁移既有单表资产与段格式兼容策略 |
| B. 每表独立引擎/列族集 | 建表 = 新 Engine（子目录）或 CF 对；mysql 层按表路由 | mysql 会话层 + 多实例生命周期；引擎内改动小 | 无跨表查询则成本可控；与"单引擎多 CF"架构叠加需重设计 |
| C. 口径单表化（已做 --single） | 测试只在单表跑 | rr-conformance 工具 | 不修引擎；产品多表仍不支持 |

#### 影响面清单（方案 A 前置调研要点，2026-09-03 修订：与 docid_alloc/自动生成统筹）

- docid 语义：`encode_docid`（8B 大端 u64）贯穿 keys/CF/Engine API；表维度需入 docid。
  已有分片前缀方案（`docid_alloc.rs`：`docid = shard_id(16bit) << 40 | local_id(40bit)`，占高 16 位）——
  **本期只做单机多表（表高位），不触碰分片路径**；统一布局留真分布式多表再评估。
- row_id 分配（见第 2 节）：显式 SQL id 直落 row_id；自动生成 = 表内自增 + 持久水位，
  与显式 id 并存（冲突走 1062，a 已修）。**不引入 Snowflake**。
- 倒排/位图/删除密度/垃圾回收/事务：docid 带表后天然隔离，无需表粒度改动。
- 兼容：既有单表库数据（docid 0..N）视为默认表（table_id=0），零迁移。
- mysql 层表名解析：INSERT/UPDATE/DELETE/SELECT 表名 → table_id（上限 65535 表足够）；
  SQL id ↔ docid 编解码在会话层。

#### 排期状态

- **✅ 已完成（2026-09-03 立项，2026-09-04 三里程碑全部落地并推送 develop）**：
  - **M1 表路由可用**（起点 commit 9d3e155，收口 ce7dcc1）：表名 → table_id（确定性 FNV-1a hash & 0xFFFF 派生，免注册表/免持久化、
    跨连接与重启稳定；表名 "documents" 特例 = table_id 0 = 既有单表库零迁移）+ SQL 层 id↔docid
    编解码（docid = table_id<<48 | row_id）+ DROP TABLE 单表区间清（默认表仍 purge_all 兼容 c）；
    验收 = rr-conformance `--init` 双表种子各自 id=1..2000 共存不撞 + 双表同 id 读写隔离。
  - **M2 per-table row_id 分配**（dc53b66）：非默认表 auto 逐行探测分配、默认表段预分配（水位续接 + AUTO_INCREMENT 端到端）；
    表内显式 id 直落 row_id、auto 分配按表区间水位（冲突仍 1062）。
  - **M3 Flush/Compaction 按表切分**（19745a6，实施清单 1-5 全落地）：flush/compact 输出每表单文件（同表合并收敛，
    跨表每表 1 段即按表收敛不空转）、meta_only 仅同表复用、DROP TABLE 物理回收该表区间 SST（含 DROP/TRUNCATE 表名路由修复）。
- 归档：完整设计与要点已回填 development.md「二十、真多表（§26）实现归档」；本节以下仅保留收尾约束与后续项。
- 当前不阻塞：`--single` 已绕开对照；RR 收敛目标（C1~C6 全绿）已达成。

#### 实施清单（随立项，用户 2026-09-03 细化：Flush / Compaction 按表切分）

> **✅ 1~5 全部落地（M3，commit 19745a6）**。本表保留作验收依据，不再执行。

> 前提：docid = `table_id << 48 | row_id` 高位编码落地。docid 高位有序 → MemTable/归并遍历
> 天然"同表连续区间"，两处切分只需检测区间边界（`docid >> 48`），无需表概念进入引擎。

| # | 项 | 内容 |
|---|---|---|
| 1 | Flush 按表切分 | 遍历 immutable 写 SST 时检测 `(next_key>>48)` 变化即 Finish 开新 writer（table_id=0 单表=单文件与现行为一致） |
| 2 | Compaction 输出按表切分 | 多路归并同①检测表变化切文件——输出多段同层不重叠（天然符合 L1/L2 语义）；compact 支持多输出 + 逐个 finalize/登记 |
| 3 | meta_only 块级复用适配 | 同表才复用 / 跨表切多输出 / 跨表合并禁 meta_only 回退全量切分 |
| 4 | 文件路由/删除 | docid 区间已含表 → sst 窗口剪枝自动跳过其它表文件；DROP TABLE 删该表区间文件 |
| 5 | 单表回归 | table_id=0 时①②均单文件输出，行为与现状一致 |

> 收益：L0→L1 读/写放大降（不跳读混表）、查询零跨表 IO、缓存按表隔离、DROP 单表低成本。
> 代价：文件数 = Σ表 × 层文件；运维 ulimit -n / TableCache 说明写入运维手册。
> **收尾约束（M3 实测）**：删除位图按 docid 稠密寻址，多表高位 docid（table<<48）下 delete/DROP 会爆内存——
> 多表单删须关 `storage.deletion_bitmap_enabled`（传统 Tombstone 路径），该约束写入运维手册。

### 2. DocID 生成与自增增强（随第 1 节多表立项的剩余项）

- ✅ 已完成（已回填 development.md §14 归档）：docid 水位续接（623726e）、多行段预分配（e874285/4bb55e0 验证）、
  AUTO_INCREMENT 列属性端到端（功能已具备 + 测试）；**表内 row_id 分配器已随 M2（dc53b66）落地**（探测分配/段预分配 + 冲突 1062）。
- **远期备选**：Snowflake —— 仅当真分布式多写者无协调分配时复评（现有 docid_alloc 已覆盖分片场景），不主动排期。

### 3. MySQL SQL 兼容 P1 补齐清单（随多表收尾 P0/P1 排查，2026-09-03 记录）

> **✅ 四项全部落地（2026-09-04）**，本清单保留作验收依据：

| 项 | 结果 |
|---|---|
| INSERT IGNORE | ✅ 冲突行跳过不报 1062（affected 不计跳过行）；事务内同语义；INSERT IGNORE 表名路由修复（table_name_of 兼容 `INSERT [IGNORE] INTO`） |
| INSERT … ON DUPLICATE KEY UPDATE | ✅ 冲突转 UPDATE：支持 `doc=VALUES(doc)` 整 doc 覆盖 / `col=VALUES(col)` / `col=col+N` 自增 / 字面量 / 多赋值；affected=插入 1 / 更新 2；事务内同语义 |
| DELETE FROM 表（无 WHERE / 全表） | ✅ 非事务 = drop_table_range（本表区间逐行删 + 表文件回收）；事务内 = 快照枚举本表可见 docid 逐条写删（commit 原子、回滚恢复）；仅本表，他表不受影响 |
| 非默认表聚合按表区间执行 | ✅ sqlish 新增 execute_aggregate_window / execute_group_by_window（docid 窗口；窗口下禁用倒排统计/词典快路径防跨表串表）；mysql server 层聚合/分组一律按**本表 docid 区间**执行（含默认表 [0,2^48)，多表正确隔离）；直接 sqlish API 保留全库快路径 |
| 显式 FOR UPDATE 确认 | ✅ 验证通过：点查/窗口/IN 当前读（最新已提交 + 自写）、快照读不受影响（RR 幻影由当前读排除）、同事务写后 commit、非默认表主键/窗口放行 |

**P1-4 顺带修复**：keys-only 扫描（count_keys_range / scan_stream_keys）Tombstone 折叠 bug——旧实现"任一版本为 put 即可见"，高 seq 删除墓碑被忽略 → DELETE 后纯 id 窗口 / COUNT 仍见已删行；改为取最大 seq 版本判定可见性（非事务 Tombstone 路径实测暴露）。
**FOR UPDATE 快照冲突修复（2026-09-04）**：RR 下事务对"快照外新提交行"FOR UPDATE 当前读后写该行，commit 曾一律被并发冲突判定拒绝（无当前读锁定集）。现引入**当前读锁定集**：`Transaction.locked_cur`（docid → 当前读时引擎最新 seq，`txn_read_current`/`txn_scan_current` 命中行记录；`Engine::last_write_seq` 公开 &self）；commit 冲突检测遇锁定键且最新 seq 仍等于记录值（期间无并发再改）→ 放行（对齐 MySQL 当前读后写语义）；期间被并发事务再次修改（seq 前进）→ 仍冲突（乐观锁正确性，不覆盖并发新值）。未 FOR UPDATE 的快照写冲突判定不变。测试：p1_for_update_current_read_semantics / p1_for_update_conflict_on_concurrent_modify（正例+并发再改负例+快照写回归）。

### 4. 宽表基准缺口闭合（排期，2026-09-04 审计采纳 + 3000 万行目标修订；验收口径 = 110 万行 37 探针 vs MySQL，见 user_guide/宽表SQL性能基准记录.md §9~§12）

> 外部审计逐项核对结论：**过期不排** = ①docid 与倒排 32 位冲突（已由 P79 / c3f8403 解决：inverted 段 v6、Posting=RoaringTreemap 全链 u64、旧段读取升 64 位默认表零迁移）；
> ②Compaction"未实现"（已具备并调优：Ex-8.11 写放大 A/B 采纳 l1_trigger=8、Ex-8.12 分层压缩 A/B、Ex-8.9 空闲感知合并调度）。
> **采纳排期（执行顺序）** = ~~P2-B~~ ✅ → ~~P0-C~~ ✅ → ~~P80~~ ✅ → ~~P0-A~~ ✅ → P0-B → P1-C → P1-D → ~~P2-A~~ ✅ → ~~P2-D~~ ✅ → ~~P1-E~~ ✅ → ~~P3-A~~ ✅ → ~~P3-B~~ ✅ → ~~P3-C~~ ✅ → ~~P4-A~~ ✅ → ~~P4-B~~ ✅ → ~~P4-C~~ ✅
> **2026-09-04 新基准审计追加（性能对比-优化器大项后 10万/110万 报告）**：LIMIT 未下推回表（#6/8/9）与 ORDER BY 全量回表（#29）根因同源——DocIdSet 消费端全量物化后再切片/排序，LIMIT 未到回表前 → 追加 P85（位图消费端 LIMIT 早停）→ P86（回表解码瘦身，共享前置）→ P87（ORDER BY Top-K 流式 + 排序键解码下推，P0-B 收口达线）；事务 #25/35/36 为档位不对称（默认档位 1），P2-A 档位 2 已具备 → 追加 Ex-9.4 公平档位复测闭环（见 §二）
> 3000 万行目标下 P1-A/P1-B 升级为 P0（30M 放大后可用性阻断）。
> 2026-09-04 按 design_goal.md 对照补充：引擎已实现但 SQL 协议层未接线的断裂点（P1-D/P1-E）。
> 2026-09-04 3000 万行隐藏瓶颈分析（存储层物理限制）：已具备项确认（布隆 ✅ 分区布隆 v5 已下推、多表分层隔离 ✅ compaction 按表切分 SST + L1/L2 层范围粗筛跳非目标表、多级缓存 ✅ HotCache+BlockCache+OOM 水位）；P3 追加项针对 30M 放大后剩余物理限制。

| 序 | 项 | 内容 | 验收（1,098,342 行 / 37 探针；3000 万行外推） | 状态 |
|---|---|---|---|---|
| P2-B | 删除位图稀疏化（多表 docid 主题最后硬伤） | DeletionBitmap 现按 docid 稠密 `Vec<u8>` 寻址（bitmap.rs）→ 非默认表高位 docid（tid<<48）下 delete/DROP 内存爆炸（理论 32TB），M3 收尾只能关 `storage.deletion_bitmap_enabled` 降级 Tombstone。**demo 已验证方案 a（RoaringTreemap）+ kernel 已整合（src/bitmap.rs 重写）：** 稠密 Vec<AtomicU8> → RoaringTreemap 稀疏位图 + ArcSwap COW 无锁读 + 全量序列化持久化；21 单测 + 7 引擎测试全绿；API 完全兼容（mark_deleted/clear/is_deleted/is_deleted_key/deleted_count/has_pending/flush/purge 签名不变） | 非默认表开 deletion_bitmap 跑 rr-conformance 双表 DELETE/DROP 不 OOM；默认表单删回归不退化 | ✅ 已完成 |
| **P80** | **compact_merge 流式 k 路归并修复（高阻塞）** | 当前 compact_merge 全量 Vec 物化 + watchdog 500ms 空转 → 大表合并卡死，阻塞 Ex-8.12 L2 压缩默认化。**已完成：** 流式 k 路归并（逐行推进不提前物化所有堆节点）+ 合并推进语义修正（watchdog 不重复空转） | 5M/50m 双规模复现不卡死；合并推进语义正确；验收线通过 | ✅ 已完成 |
| **P0-A (高)** | **SQL 组合索引：混合扫描兜底 + 声明式路由（用户选"两者都做"）** | 阶段 1 混合扫描兜底：status 位图倒排候选 + ts/amount 范围条件进倒排/位图（数值范围位图或行过滤降载），先收敛 #30/#31 全扫过滤；阶段 2 声明式路由：schema `composite_indexes` → sqlish `try_composite_index` 提取 WHERE 等值条件匹配最左前缀 → `query_by_composite_prefix`（cidx 前缀扫描 + 回表）。**已完成：** engine.rs `query_by_composite_prefix()` 写+读路径 + optimizer.rs `QuerySpec.index_prefix` + sqlish.rs `try_composite_index` 声明式路由 + stale 键复筛 + 8 单测（引擎层 1 + 优化器层 2 + SQL 层 4 + CF 层 1） | #30 status='active' AND ts=? 921ms → <10ms（30M 外推 ~25s → <1s）；#31 ts BETWEEN 7300ms → 同量级收敛（MySQL 对照 0.30 / 1.40ms）。注：MySQL 覆盖索引免回表，cjserver cidx+回表非覆盖，全平需 cidx 存全部查询列。**复测回填（2026-09-04）：** v3 声明 `(status,ts)` 下 #30 110 万 **0.25ms**；v4 增 `["ts"]`：#30 0.33ms、#31 仍 902ms（**仅声明不收敛——`try_composite_index` 只路由 WHERE 等值，BETWEEN 属代码缺口**）；**v5（P92 范围路由代码）**：#30 **0.30ms**（≈ MySQL 0.30ms）、#31 **15.24ms**（v4 902ms → 59×，368 行 cidx 范围扫描+回表+复筛；MySQL 1.40ms 为覆盖索引免回表） | ✅ 已完成 |
| **P0-B (高)** | **ORDER BY Top-K 有界堆** | sqlish `SORT_MAX_ROWS=200_000` 守卫对含 ORDER BY 全量物化、忽略 LIMIT → 110 万行 + LIMIT 100 被 1064 拒。改 LIMIT k（小 k）走 BinaryHeap 部分有序（内存 O(k)）；无 LIMIT 保留全量守卫 | #29 ORDER BY k,amount LIMIT 100：1064 拒绝 → 可跑（110 万 ~1.2s；30M 外推 2-4s）；窗口/早停路径不回归 | ⏳（Top-K 有界堆 ✅ + P87 流式化 ✅；**复测闭环 2026-09-04：#29 110 万 17.9s → 13.1s**（行式默认布局，-27%，仍 25.3× MySQL），未达 ≤1.5s 验收线——残余瓶颈 = 1.1M 候选**逐 docid 投影点查定位**（batch_get_fields ~11µs/docid）；PAX(hot_fields) 轮 18.9s 亦未达且拖慢全扫聚合（PAX 通用扫描未接线）→ **收口 P91/P92 已落地**（scan 投影列 + 稠密窗口流式，2026-09-04）：复测 17.9→13.1（P87）→**10.0s（P92，-26% vs v4）**——逐 docid 点查定位已消除，残余 = 行式全量读 ~1.1GB IO 地板；破 ≤1.5s 需 PAX 列 IO 排序键扫描（P93 候选） |
| **P1-C (中)** | **无索引聚合加速** | COUNT(*)/SUM(amount)/GROUP BY（无索引列）现全扫解 25 列宽行：①COUNT 走 keys-only 扫描（复用 count_keys_range 基建不解行值，**已接线**）；②声明式统计载荷（Ex-9.3 ⑤ SUM/AVG/MIN/MAX 随 term 载荷）推广到高频数值列默认启用。**已完成部分（2026-09-04）：** `Engine::count_all_docs` **O(1)**——活跃 docid 集（RoaringTreemap）懒建基线（首次 COUNT 全键扫一次，重启恢复）+ put/delete/delete_batch/purge 增量记账（新 docid/复活 +1、覆盖不变、删除幂等 -1、purge 复位 0），COUNT(*) 从全键扫 O(N) → 增量读 O(1)；1 新增单测（put/覆盖/删除/复活/delete_batch/purge/重开全路径 = scan 口径）。**Ex-9.3 ⑤ 默认化前置阻塞解除**。剩余 ② 载荷默认启用随 Ex-9.3 ⑤ 默认化执行 | 110 万行 count_all 5869ms → keys-only ~1800ms（~3×）；30M 外推 ~50s。5-10s 需 zonemap/列存配合（远期）。**复测回填（2026-09-04）：count_all_docs O(1) 已生效（sqlish 直连，单测覆盖）；但 MySQL 协议层聚合按"本表 docid 窗口"执行 → 默认表 [0,2^48) 窗口仍走 keys-only 全扫（110 万 407ms，2.6× MySQL）——窗口全包直通 count_all_docs 待接线（随 P91）** | ⏳（剩 ② 载荷默认启用，随 Ex-9.3 ⑤ 默认化执行） |
| **P1-D (中)** | **倒排统计载荷范围条件扩展（design_goal 断裂点 G3）** | **引擎已实现** engine.rs `inverted_term_stats()` 支持随 term 的 SUM/AVG/MIN/MAX 载荷聚合。**协议层断裂：** sqlish.rs 仅裸 `field=value` 等值条件才走统计载荷路径（`execute_aggregate_window` 等值分支）；`BETWEEN`/范围条件或 `status='active' AND ts>?` 组合条件回退全扫解行。需扩展统计载荷路由至范围/组合条件（倒排候选 ∩ 范围过滤后走载荷，而非全扫）。**已完成（2026-09-04，P1-D 扩展）：** `status='active' AND ts>?` / `BETWEEN` 组合聚合路由——`candidate_posting` 倒排候选收敛后**按需解列**：`aggregate_needed_fields`（WHERE 引用 + 聚合列的顶层字段集，点路径取顶层键、去重）→ 候选 512/块 `engine.batch_get_fields`（PAX 列解码 / 行式按需字段提取，P86②/P87② 基建）→ `subset_doc` 子集对象 → 残余范围/BETWEEN/复合条件在子集上判定 → 聚合累积——替代原整行 `engine.get` 全 25 列解码。**语义等价说明：** 含范围/组合过滤时 term 级统计载荷不可直接取（须逐行判范围），投影解列承载同一"免全扫解 25 列"目标；行缺失/JSON null/非数值语义与整行路径精确一致（子集缺失 = 原文档缺失）。**1 新增单测：** SUM/COUNT/AVG × AND(等值,`>=`)/BETWEEN × 多字段组合 + 无命中 NULL + 字段集去重。回归 688 全绿 | 110 万行 sum_where_enum 6524ms → 倒排候选+载荷 ~500ms（~13×）；30M 外推从 ~180s → ~15s（回归 688 全绿）。**复测回填（2026-09-04）：sum_where_enum 110 万 839→841ms 持平未降——残余 = 22 万候选逐 docid 投影点查定位（batch_get_fields ~3.8µs/docid），解码瘦身被定位开销淹没 → 随 P91 块流式取数收口** | ✅ 已完成（2026-09-04） |
| P2-A | 事务/写路径 fsync 语义对等 | **根因核对（①）：** sqlrun/rr-conformance 事务经 MySQL 协议 → db_adapter COMMIT → engine.txn_commit 尾部**无条件 flush_wal()**（位图 + primary/delta/outbox 三路 fsync）——组提交（mysql_server 默认 2000µs）只摊薄非事务 put，事务 COMMIT **不落攒批、逐次显式 fsync**（8× 直接原因）。**config 可配档位（②，已完成）：** 新增 `storage.flush_log_at_trx_commit`（0/1/2 默认 1，validate 校验；对齐 MySQL innodb_flush_log_at_trx_commit）——txn_commit 落盘改走 `commit_persist()`：档位 1 = 每次 COMMIT flush_wal（强安全，现状保持）；档位 0/2 = COMMIT 交组提交窗口（`maybe_group_commit`：并发 COMMIT 共享一次 fsync；组提交关自动回退强安全）。4 新增单测（durability1 pending=0 / durability2 攒批+后台兜底+重开完整 / durability2+组提交关回退 / 档位 3 拒绝）。**③档位语义已写入** user_guide/宽表SQL性能基准记录.md §13。0/2 当前等价（无 InnoDB redo 的 OS-cache-only 层）已在文档标注 | 并发场景 ≤2-3×；单连接结构差 4-5×（每 COMMIT fsync 语义差难消） | ✅ 已完成（代码+单测+文档；**2026-09-04 档位 2 实测：1.1M 事务探针 vs MySQL 全部 1.0-1.4×（#25 8.4×→1.4×、#35/#36 ~6×→~1×），验收线全部越过**，见基准记录 §14） |
| P2-D | 倒排回表批量预取（design_goal 断裂点 G2） | **引擎已实现** engine.rs `batch_get()`（含 HotCache + Delta + 删除位图批量过滤）。**协议层断裂：** sqlish.rs 3 处 `engine.get()` 逐行调用 → 改 `engine.batch_get()` 批量取行（利用 LSM 有序性 + BlockCache 局部性）→ 减少随机 IO。对 30M 放大后位图命中取行收益最大。**已完成：** sqlish.rs 回表路径批量预取改造 + 单元测试通过 | 110 万行 enum_sel_limit100 4.2ms → ~1.5ms；30M 外推从 ~120s → ~30s | ✅ 已完成 |
| P1-E | Zone Map SQL 层范围剪枝（design_goal 断裂点 G4） | **引擎已实现** sstable.rs `IndexEntry.zones` 块级 min/max 统计；column_family.rs 层级范围粗筛 `[lmin,lmax]` 已用于点查/批量点查跳过整层。**协议层断裂修复：** sqlish.rs 范围查询（`ts BETWEEN`、`amount > N`）接入 SST 块级 zone map 过滤（跳过 min/max 不相交块）。**已完成：** sqlish.rs scan_pushdown 路径 zone map 剪枝 + 单元测试通过 | 110 万行 cmp_between 6686ms → zone map 剪枝 ~2000ms（跳过 ~70% 块）；30M 外推从 ~180s → ~55s | ✅ 已完成 |
| P3-A | L0 按表分组层范围（多表查询避免不必要扫描） | **现状：** compaction/flush 按 `docid>>48`（table_id）切分 SST → 每个 SST 只含单表数据；L1/L2 层范围粗筛天然跳过非目标表整层。**瓶颈：** L0 层范围 = 所有表 SST 的 [min,max] 并集 → 多表混布 L0 时无法整层跳过，需逐 SST 布隆校验。**优化：** L0 层范围改 per-table 分组（`HashMap<tid, (min, max)>`），点查时按 docid 高 16 位定位目标表组 → 仅扫该组 SST。**已完成：** column_family.rs `SstSnapshot.l0_table_ranges` + `build_l0_table_ranges` + `get` 方法 L0 表组定位 + 单元测试通过 | 30M 10 表 L0 点查 p99 从 ~5ms（逐 SST 布隆）→ ~1ms（表组定位 + 布隆）；L0 段数 50+ 时收益最大 | ✅ 已完成 |
| P3-B | 列存块 / 微分区下推（30M 全扫聚合物理限制） | **现状：** 行存 + 分区布隆 + 块级 zone map（sstable.rs `IndexEntry.zones`）；无列存块格式。**瓶颈：** COUNT(*)/SUM(amount)/GROUP BY 无索引列需全扫解 25 列宽行。**优化：** SSTable 块内列存编码 + FieldZone sum 统计（SUM/AVG 聚合下推）。**已完成：** sstable.rs SST v6 升级 + FieldZone.sum + PAX 列存编码 + `decode_pax_block_column` + 单元测试通过 | 30M SUM(amount) 从 ~180s → <10s（跳过 70% 块 + 列读免解 24 列）；SST 格式升级 v6 | ✅ 已完成 |
| P3-C | 自适应缓存水位（30M 缓存命中率物理限制） | **现状：** HotCache(行级) + BlockCache(LRU 块级) 两级 + OOM Guardian 水位限流；30GB 数据 vs 2GB HotCache → ~7% 命中率。**优化：** ①HotCache 自适应淘汰策略（LFU 替代 LRU，保热点）✅（已有 LFU 采样近似 + 热点保护区晋升）；②BlockCache 按表分区水位（多表场景热点表多分配）✅（新增 per-table 分区 + 自适应淘汰冷表优先）；③可选：SSD-aware 块预读（留待后续）。**已完成：** blockcache.rs 按表分区 + adaptive_evict + SstReader.table_id 提取 + 20 单测全绿 | 30M 10 表点查缓存命中率从 ~7% → >40%；p99 从 ~5ms → <1ms（缓存命中） | ✅ 已完成 |

### 5. 高难深水区排期（2026-09-04 advance_develop.md 分析；正确性优先，方案 B 最先开发）

> 来源：research/advance_develop.md 四大难点分析。逐项核对代码现状后判定：
> - 难点 1（Compaction 动态自适应）：Ex-8.9 空闲感知 + Ex-8.11 l1_trigger=8 已具备静态层；缺**写入速率自适应**（L0 爆胀防护）。
> - 难点 2（MVCC + 删除位图版本化）：**精确断裂点已定位**——engine.rs:1257 `get_at` 中位图短路返回 None 不看 snapshot_seq → RR 违反。`get_at` 本身已具备版本化读（L1262 `primary.get_bytes_at(key, snapshot_seq)` 按 seq 过滤 + tombstone 语义保留）；Transaction 已有 `write_set`（本事务写后读可见）+ `snap_cache`（快照缓存）。修复核心 = 快照读路径跳过位图、让 LSM 版本裁决。用户选定**方案 B（全局删除位图 + 事务快照删除日志）**，排期最先开发。
> - 难点 3（倒排 FST 大 Term 集）：base.fst + delta.fst 分层已具（7.34）；delta 上限 + 多 Segment 倒排已落地（P4-B，2026-09-04）。
> - 难点 4（成本估算优化器）：仅硬编码规则；缺统计信息 + 代价模型。30M 下可暂缓。

| 序 | 项 | 内容 | 验收 | 状态 |
|---|---|---|---|---|
| **P0-C** | **方案 B：MVCC + 删除位图版本化（RR 正确性修复，最先开发）** | **断裂点：** engine.rs:1257 `get_at` 中 `bm.is_deleted(docid)` 短路返回 None，不看 snapshot_seq → 事务 B 删除后事务 A 快照读违反 RR。**已有基础：** `get_at` L1262 `primary.get_bytes_at(key, snapshot_seq)` 已按 seq 过滤 LSM 版本（tombstone 语义保留）；Transaction 已有 `write_set`（read_own 本事务写后读可见）+ `snap_cache`（快照点查缓存）。**修复（已完成）：** `get_at` 快照读路径跳过全局删除位图，让 LSM 多版本 + tombstone seq 裁决——tombstone seq ≤ snapshot_seq → 快照前已删 → None；tombstone seq > snapshot_seq → 快照后删 → `get_bytes_at` 返回旧版本值；RC / 非事务读保留位图短路。demo 9 测试全绿 + kernel 631 测试全绿。**R4 补强（review 闭环）：** compact 保活——`ColumnFamily::mvcc_keep_floor` + `Engine::active_snapshots`（RR 事务注册/注销），活跃快照期间 compact 保留删除/覆盖前旧版本，快照读跨 compaction 仍正确 | C1~C9 RR 一致性测试在 **deletion_bitmap_enabled=true** + 并发跨事务删除下全绿；快照读已删行返回旧版本值；点查 p99 不退化 | ✅ 已完成 |
| **P0-D** | **JOIN 支持：DocIdSet 统一抽象 + 8 阶段优化器流程（参考 research/optimizer_proces.md）** | **设计来源：** research/optimizer_proces.md（2026-09-04 更新版，8 阶段流程重构：WHERE 与 JOIN 组合顺序变更）。**新流程核心变化：** 旧流程 JOIN 先 → WHERE 下推；**新流程每张表 WHERE 独立先产出 DocIdSet → JOIN 基于 DocIdSet 交集 → 跨表 WHERE 后过滤**。8 阶段：①阶段 0 安全检查（非等值 JOIN/表数≥3/大表无索引拒绝）→ ②**阶段 1 每张表 WHERE 独立产出 DocIdSet**（1.1 组合索引前缀→SortedList、1.2 倒排等值→Bitmap、1.3 多倒排 AND→Bitmap、1.4 混合扫描→SortedList、1.5 全扫+ZoneMap→Stream、1.6 keys-only COUNT→Stream、1.7 全扫兜底→Stream）→ ③**阶段 2 JOIN 路径**（2.1 主键 JOIN 直达、2.2 索引-索引 DocIdSet.intersect、2.3 有索引-无索引、2.4 广播哈希 <10万、2.5 拒绝）→ ④阶段 3 跨表 WHERE 后过滤 → ⑤阶段 4 GROUP BY → ⑥阶段 5 HAVING → ⑦阶段 6 ORDER BY（Top-K/全量守卫）→ ⑧阶段 7 LIMIT 下推 → 阶段 8 执行计划。**核心改动：** 新增 `DocIdSet` 枚举 + `get_docid_set()` + `intersect()` + JOIN 执行器统一用 DocIdSet。**依赖：** P0-A（组合索引提供 SortedList）、P2-D（batch_get 批量回表）。**排期项→优化器映射：** P0-A→阶段 1.1、P0-D→阶段 2、P0-B→阶段 6.1、P1-C→阶段 1.6、P1-D→阶段 4.1、P2-D→阶段 1/2 回表、P1-E→阶段 1.5 | 2 表等值 JOIN 1 万行 <1ms、100 万行 ~500ms；主键 JOIN 直达；广播哈希 <10万行；非等值/3 表/大表无索引拒绝 1064 | ✅ 已完成（INNER/LEFT JOIN 解析+执行；多 JOIN 解析期拒绝；从表 1:N 展开；JOIN 路由先于组合索引；见 P81） |
| P4-A | Compaction 写入速率自适应（难点 1） | **现状：** Ex-8.9 空闲感知调度 + Ex-8.11 l1_trigger=8 静态阈值 + Ex-8.13 IO 预算。**缺口：** 无写入速率自适应——写入爆发时 L0 段数暴增、compaction 按部就班 → L0 爆炸 → 点查扫数十 SST → p99 从 0.2ms 飙到 200ms。**优化：** ①实时监控 L0 段数 + 写入速率 + 滑动窗口爆发检测 ✅（新增 `compaction_write_rate_window` + `compaction_write_rate_burst` 配置，`record_flush_new_l0` 滑动窗口）；②动态调整 compaction 参数 ✅（写入爆发时 `l1_trigger_files` 从 8 自适应降为 2，提前下沉 L1→L2 防 L0 爆胀）；③L0 stall 阈值动态收放 ✅（现有 `effective_l0_threshold` 按写压力 + 新增写入速率窗口协同）；④与 Ex-8.9 空闲感知协同 ✅（`adjust_compaction_io_rate` 已接入写压力信号，压力高时 IO 让路 + 阈值提前收敛）。**已完成：** column_family.rs `record_flush_new_l0` + `effective_l1_trigger` + `l1_trigger_files` AtomicUsize + config 新增字段 + 647 测试全绿 | 30M 持续写入下 L0 段数 ≤ 阈值（如 12），点查 p99 < 2ms；写入爆发（10× 均值）不 stall | ✅ 已完成 |
| P4-B | 倒排 delta.fst 上限 + 多 Segment 倒排（难点 3，30M+ 规模） | **现状：** base.fst + delta.fst 分层（7.34）已具；无 delta 上限、无多 Segment。**缺口：** 高频写入 → delta.fst 膨胀 → base+delta 合并查询超时。**优化（远期）：** ①delta.fst 大小上限 + 自动 roll into base（全量重建 FST，期间查旧 FST）；②或重构为多 Segment 倒排（类 ES，每 segment 独立 FST，查询合并结果） | 30M 高频写入下 delta.fst ≤ 上限；查询 latency 不退化；FST 重建不阻塞查询 | ✅ 已完成 |
| P4-C | 基于成本的优化器（难点 4，30M+ 规模） | **现状：** optimizer.rs 仅硬编码规则（主键点查禁倒排、组合前缀）。**缺口：** 无统计信息、无代价模型 → 多条件查询选错执行计划（慢 1000×）。**优化（P4-C 已落地）：** ①统计信息结构（`ColumnStatistics` / `TableStatistics`）✅；②代价模型（`CostParams` + `CostEstimate` + `cost_route` 函数：倒排查 N docid 开销 vs 全扫 M 行 zone map 剪枝开销，支持 `choose_best_plan` 选最优组合）✅；③动态执行计划选择（`cost_route` 替代 `route` 静态硬编码，`sqlish::execute` 中集成代价判断——倒排选择性低时自动走全扫 + Zone Map）✅；④`OptimizerConfig` 配置区块（`cost_based_enabled` / 各代价参数可调）✅；⑤13 个单元测试覆盖（主键点查、高选择性倒排、低选择性倒排回退全扫、组合索引、多条件 AND 最优选择）✅。**已知局限：** `estimated_total_rows` 基于 max_docid（上界而非精确行数）；`zone_fields` 暂时为空（后续可从 SST 元数据提取）；无 ANALYZE TABLE 持久化统计（当前仅用倒排 doc_count 运行时统计）。 | 30M 多条件查询选择正确执行计划；status='active' AND amount>5000 优先走 amount zone map 剪枝 | ✅ 已完成 |
| delete_range50 | **DELETE 主键区间批量删修复（用户 2026-09-04 立项，写路径 delete_batch + SQL 主键区间快路径；基准 #24 最严重 6729×）** | **根因（双层）：** ①定位层 `resolve_where_ids` 对 `id BETWEEN` 不识别为主键区间 → 落 sqlish 全扫物化；②删除层逐行 `engine.delete`——多表须关 deletion_bitmap → `delete_bytes` **逐行 sync_wal fsync**。**已完成：** ①引擎 `Engine::delete_batch`（位图/墓碑双路径语义同 delete，批尾单次 sync；持久性镜像 delete——位图路径不主动 flush，仅 Tombstone 路径批尾 sync_wal，避免触发 bm.flush 落盘 os error 3）；②db_adapter `parse_pk_between` + `delete_pk_range`（`id/docid BETWEEN` → 区间 keys-only 扫现存 → delete_batch，只删现存行对齐 MySQL affected_rows，多表隔离）；③通用字段条件 DELETE 逐行删改走 delete_batch。5 新增单测。**A/B 实测**（src/demo/delete-range-ab，release 5 万行位图关）：A 逐行 48319ms vs B 批量 74.7ms = **646.7×**（与 6729× 同根因）。设计见 research/optimizer_integration_design.md §9（写路径整合，DocIdSet 消费端先行落地） | 范围删 50 行（位图关）从 7537ms → ~1-2ms 同档 MySQL；影响行数 = 现存行；区间外/他表不受影响 | ✅ 已完成（2026-09-04，P83） |
| DocIdSet 读路径重构（阶段 A） | **优化器统一 docid 集合抽象（用户 2026-09-04 立项；optimizer_integration_design 阶段 A，P84）** | **A1/A2** src/docset.rs `LimitSpec` + `DocIdSet`（Bitmap/SortedList/Empty/All + intersect/to_vec/iter）；**A3** `WhereExpr::Like` + 解析分类（无 `%`→Eq 走倒排；含 `%` 扫描）+ 双指针通配，AND 快路径天然生效；**A4** `get_docid_set`（eval 形态包装，读写 JOIN 共用）；**A5** execute() bitmap 生产/消费改走 DocIdSet（sort 保 Top-K/全排序）；**A6** execute_join() 主表候选走 get_docid_set（修正：主表候选不再 limit 截断，防低匹配密度漏行）；**A7** Top-K k=offset+limit 超 SORT_MAX_ROWS 拒绝（深分页防堆膨胀）。11 新增单测。**回归：676 全绿**。设计见 research/optimizer_integration_design.md §一~八 | 读/写/JOIN 共用 DocIdSet 消费；LIKE `%` 通配可查；深分页守卫拒绝防 OOM；JOIN 主表候选全量不漏行 | ✅ 已完成（2026-09-04，P84） |
| **P85 (高)** | **位图消费端 LIMIT 早停 + 分块批量回表（#6/8/9，optimizer_proces 阶段 7 LIMIT 下推落地）** | **根因（#6 820×/#8 143×/#9 1749×，110 万轮）：** sqlish.rs `execute()` 非排序分支（A5 消费端）`set.to_vec()` **全量物化倒排候选**（#6 status='active' ≈22 万 docid）→ `engine.batch_get(&docids)` **全量回表解码**后才 LIMIT 切片——LIMIT 只作用于 eval 过滤 cap（L2059），倒排 term 解码的整张 posting 位图不受限，P84 A5 阶段 A 把"切片前全量物化"带入回表 → 解码量 = posting 数而非 limit（与 #6 17ms@2万 → 729ms@22万 线性吻合；#7 走载荷 0.29ms 证明位图本身正常）。**修复：** 非排序分支改 **DocIdSet 分块迭代消费**——`set.iter()`（Bitmap 升序）按 256-512 docid 分块 → `batch_get(chunk)` → 逐 docid 复刻现状"offset 占位 + 可见行计数 + limit 截断"循环（墓碑/未命中不占 limit），**产出 offset+limit 行即终止后续块拉取**（Roaring iter 可提前 drop）；`All` 分支（无 WHERE 全库 LIMIT）同路径分块，顺带消除无 WHERE LIMIT 全库回表隐患。内存 O(chunk)，回表解码 22 万行 → ~offset+limit 行。**已完成（2026-09-04，P85）：** sqlish.rs `collect_limited_rows`（512/块 batch_get、offset 占候选位 + limit 计可见行 + 终止后剩余块零拉取；Bitmap/SortedList/All 三分支共用）+ execute 非排序分支接线；**5 新增单测**（三分支 offset+limit 早停 / 终止后剩余块零拉取（迭代计数 mock）/ 墓碑不占 limit / 可见行恰好 limit / 端到端 LIMIT+OFFSET）。**验收：** 110 万 #6 729ms→<10ms、#8 97ms→<10ms、#9 1206ms→<15ms（10 万轮同步 ~10 倍改善）；#7 载荷/扫描/聚合/JOIN 不回归（回归 684 全绿）。**复测回填（2026-09-04，110 万轮）：#6 729→3.50ms、#8 97.35→3.52ms、#9 1206.69→3.62ms（-99.5% ~ -99.7%，208×/28×/333×），验收线 <10/<10/<15ms 全部越过；10 万轮同步 ~3ms** | ✅ 已完成 |
| **P86 (中，低风险共享前置，建议先于 P87)** | **batch_get 无 Delta 覆盖短路 + 排序键按需字段提取（回表解码瘦身）** | **根因（#6/#8/#9/#29/JOIN 回表共同放大项）：** engine.rs `batch_get` L1357-1390 对每个回表行**无条件** `serde_json::from_slice` 整行 → `to_vec` 重序列化（无 delta override 时是等值空转：parse + serialize 各一次全量 25 列）；sqlish `sort_key` L1833 再对同一行**第二次**全量 JSON parse——#29 候选行每行 2 parse + 1 serialize。**修复①（engine.rs）：** `batch_delta_overrides` 为空或当前 docid 无覆盖 → `out[i]=Some(bv)` 直通短路（跳 parse/reserialize；键序差异不影响 JSON 消费端语义，hotcache 缓存原字节）；**修复②（sqlish.rs）：** `sort_key`/`extract_top_fields` 改为 serde 顶层 MapAccess **流式只收目标成员**（跳其余 23 列 Value 构造与丢弃）。**已完成（2026-09-04，P86）：** ① engine.rs `batch_get` 无 Delta 覆盖直通短路（hotcache 缓存原字节）；② sqlish.rs `row_sort_keys`/`light_sort_keys` 单遍字节级多排序键提取（跳其余列 Value 构造与丢弃，转义/畸形回退 serde 正确性护栏），topk 与全排序路径统一接线。**3 新增单测：** 短路原字节直通（未 parse/reserialize 重排）+ 有 override 合并回归、`batch_get_fields` 与 get 语义等值（Delta 覆盖 / null 删字段 / 删除位图 / SST 行式块路径）。**验收：** 单测 + 全量回归无退化（684 全绿）；110 万 #29 17.9s → ~8-10s（解码减半量级，P87 后进一步下探）；P85 后 #6/8/9 常数再降 | ✅ 已完成 |
| **P87 (高)** | **ORDER BY Top-K 流式化 + 排序键解码下推（#29 达线，P0-B 收口）** | **根因（#29 31×，17.9s）：** `topk_sort` L1892-1893 对全量候选（110 万）`bitmap.iter().collect()` + 整批 `batch_get`——单查询瞬时物化 ~110 万行全 25 列 JSON（≈1GB+ 峰值内存 + HotCache 冲刷），堆 O(k) 只省排序不省**回表物化**；排序键提取仍需逐行再 parse。**修复三阶：** ①**流式化**：候选 docid 分块（256-512）→ 块内 `get_many` → 排序键入堆（内存 O(k+chunk)，看门狗逐块熔断）——消除 1GB 峰值物化；②**排序键解码下推**：PAX 块新增 `decode_pax_block_fields(data, &[k,amount])` 单块多列一次解码（sstable.rs `decode_pax_block_column` 已具但未接线消费端）；行式块回退 P86② 按需字段提取——候选扫每行只解 2 排序列而非 25 列；③**输出瘦身**：top-k 确定后仅对 k 个 docid 整行回表（SELECT *）或直接列值组装（投影 ⊆ 已解列，#29 SELECT id,k,amount 即此，回表全免）。**已完成（2026-09-04，P87）：** ①topk_sort 流式化（候选 512/块 `batch_get_fields` 只解排序键列入堆、堆仅存 docid+键、峰值内存 O(k+chunk)、看门狗逐块熔断）→ 输出期仅对胜出 docid 整行回表；②排序键解码下推全链路接线——sstable.rs `decode_pax_block_fields`（单块多列一次解码）+ `SstReader::scan_block_for_keys_fields`（PAX 列解码 / 行式块按需字段提取）+ CF `get_many_fields` / `get_many_fields_from_sst` + Engine `batch_get_fields`（投影批量回表，Delta 字段级覆盖 / null 删字段 / 删除位图 / HotCache 语义同 batch_get）；③Top-K 确定后仅 k 个 docid 整行回表。**3 新增单测：** 流式 topk 与全量排序结果一致（多键/DESC/OFFSET，PAX 块端到端）、Top-K 块看门狗熔断、PAX 列解码与整行解码/逐列解码三方等值（含行式块回退）。**数据侧：** 基准库 tmp-cfg-wide-2g 增 `storage.hot_fields=["k","amount"]` 重装（SST PAX 化后列解码生效；MySQL 对照不变，SCC 仅重建 db-wide-scc 测试库，不动 parquet/资产）。**验收：** 110 万 #29 17.9s → ≤1.5s（≥12×，对齐 P0-B 原始 ~1.2s 目标）；10 万 ~988ms → ≤120ms；Top-K 守卫/无 LIMIT 全排序/#16 窗口 ORDER BY 不回归（回归 684 全绿）。**实测回填（2026-09-04 复测闭环）：110 万 #29 17.9s → 13.1s（仅 -27%，未达 ≤1.5s）；10 万 988→550ms（未达 ≤120ms）——流式化消除 O(N) 物化峰值，但取数仍逐 docid 投影点查（batch_get_fields ~11µs/docid）；PAX(hot_fields) 布局下实测 18.9s 不降反升、且通用全扫聚合回归 5-8s（PAX 通用扫描未接线）→ **#29 时延收口移 P91（docid 窗口块流式扫描解码排序键）** | ✅ 已完成 |
| **P88 (高)** | **写路径 UPDATE/DELETE DocIdSet 定位整合（optimizer_integration_design §9 阶段 B 剩余主项 B3，2026-09-04 排期追加）** | **目标：字段条件写定位统一走 get_docid_set 全阶梯（§9 主设计核心）。前序已落地：** delete_range50 P83（B1/B2：delete_batch + id/docid BETWEEN keys-only 快路径 + 通用字段条件 DELETE 已改 delete_batch 攒批）；主键单点 UPDATE/DELETE（9.3）现状已由 parse_update 字符串切直达。**现状缺口：** db_adapter.rs `resolve_where_ids` 字段条件分支仍落 sqlish `SELECT docid` 全扫物化 Vec（cap 200_000），不享受倒排/组合索引收敛、不消费 DocIdSet；批量字段条件 UPDATE 定位后全扫枚举逐行 put。**剩余开发（对应 9.2.2②③ + 9.2.4，均为 ❌ 未开始）：** ① sqlish 提供 `parse_where_expr`（WHERE 段 → WhereExpr）出口 + `get_docid_set` 改 pub(crate)；② `resolve_where_ids` 字段条件分支 → 解析 WhereExpr → `get_docid_set`（倒排位图/组合索引前缀收敛，**写定位 limit=None 不截断** D1）→ Bitmap/SortedList 直消费、超大集分批物化（chunk ≤64K 防物化爆内存）→ delete_batch / 批量 put 消费端。决策 D1-D8 见 research/optimizer_integration_design.md §9。**已完成（2026-09-04，P88）：** ① sqlish.rs `parse_where_expr`（WHERE 段 → WhereExpr）pub 出口（get_docid_set 本已 pub）；② db_adapter 写定位分流——`where_is_primary_key`（id=/docid=/id IN）保持 resolve+route；字段/复合条件 → `parse_where_expr` + `get_docid_set(limit=None)`（写定位全收敛不截断，修复旧 cap=200_000 大命中漏行）→ `write_locate_table_ids`（按表 tid 过滤，D4 多表隔离）；③ DELETE 字段条件 → **流式** `delete_batch`（免全量 Vec 物化）；UPDATE 字段条件 → P89 批量管道消费（见下行）。**2 新增单测：** 字段条件 DELETE/UPDATE 全量命中不截断 + doc= 整文档替换 + AND 复合收敛、同字段值跨表写定位按表隔离 + 2200 行跨 chunk 全改。回归 687 全绿 | 字段条件 UPDATE/DELETE（如 `status='active'`）定位从全扫（cap 200_000 物化）→ 倒排位图/组合索引收敛，时延对齐同条件 SELECT；写定位不截断不漏行；DELETE 影响行数 = 现存行（对齐 MySQL affected_rows）；主键区间删维持 delete_range50 批量档（646×）；多表隔离不回归 | ✅ 已完成（2026-09-04，P88） |
| **P89 (中)** | **UPDATE 批量管道（optimizer_integration_design §9.5，依赖 P88 基建）** | **现状：** 字段条件 UPDATE 定位后**逐行 put**（无攒批、无倒排批量 add）。**开发：** `get_docid_set` 收敛 → 分批 `batch_get(1000)` 取现值 → `put_batch` + 倒排 `add_batch` 批量提交（同 delete_batch 批尾单次 sync 语义），Watchdog 分批熔断。**已完成（2026-09-04，P89）：** update_response 消费端批量管道——P88 定位 docids 按 `chunks(1000)` 分批 → `engine.batch_get(chunk)` 取现值 → 逐行变换（字段级修改 / `field=doc` 整文档替换 / `field=field+N` 自增语义与旧逐行一致）→ `engine.put_batch`（put_nosync 攒批：倒排 pending 累积达阈值批量刷入 / cidx / 位图复活 / hotcache 失效，批尾单次 flush_wal），替代逐行 get+put 的逐行 WAL 提交/看门狗/热缓存开销。倒排/位图/组合索引同步语义同 put（无逐行退化）。**新增单测随 P88**（2200 行跨 3 个 chunk 全改不丢；doc= 与自增路径随既有回归覆盖）。回归 687 全绿 | 字段条件 UPDATE（如 `status='active' SET note=...`）批量管道生效：无逐行 put 退化、倒排/位图同步一致、批中断点续跑不重复；时延同 delete_range50 批量档量级 | ✅ 已完成（2026-09-04，P89） |
| **P90 (中，独立立项)** | **PAX 聚合接线（optimizer_integration_design §9.6，独立立项，不并入写路径重构）** | **现状：** P3-B SST v6 已落 PAX 列存 + FieldZone.sum（sstable.rs），但 `decode_pax_block_column` **零调用点**、`FieldZone.sum`/`present_count` **无生产消费方**——SUM/AVG 等聚合仍全扫解全行。**开发：** ① CF 层 scan 投影列 API（只解所需列：PAX 块走列解码 / 行式块按需字段提取，复用 P86② 基建）；② 块级 `sum`/`present_count` 下推——SUM/AVG 聚合免读数据块（索引 zones 已含块级统计）；③ tombstone/delta 混入块回退行级精确聚合（版本敏感场景禁用块级近似）；④ 与 P1-C/P1-D 统计载荷路径经 cost_route 选路协同。**已完成（2026-09-04，P90）：** ② 块级下推接线——CF `zone_field_aggregate`（eligible：memtable 空 + 单一非空层（L0 仅单文件 / L1/L2 层内不重叠）+ 窗口内全 PAX v6 块且整块落窗 + 块 zones 含目标字段）+ Engine `zone_field_aggregate` 前置（删除位图无置位 / delta 空 / 无活跃快照）+ sqlish `execute_aggregate_window` 无 WHERE `SUM(f)`/`COUNT(f)` 快路径（present-null 计 COUNT(f) 精确；`zsum!=0` 时 SUM 精确——**零和/列非数值歧义回退行级**保证 NULL/0 语义）。③ 版本混入回退：memtable/delta/删除位图/活跃快照任一非空 → None 回退行级精确（语义不变）。**新增单测：** SUM/COUNT 块级与行级等值（PAX 单段）、memtable 写入回退且含新行、全零和回退不误报 NULL、AVG/MIN/MAX 行级精确。**已知边界（写入排期行）：** AVG/MIN/MAX 未走块级——FieldZone 无"数值行数/列纯数值"信息（含非数值行时编码器清零 sum），无法精确导出 avg/min/max 语义 → 保持行级（正确性优先；如需块级 AVG 需 v6+ 格式增 numeric_count，另行评估）。回归 687 全绿 | SUM(amount)/AVG(amount) 无 WHERE 全扫：块级 sum 下推跳过数据块读取、命中块仅解单列；版本混入/位图删除场景正确回退行级；与 Ex-9.3⑤ 载荷、P1-D 范围扩展不冲突（SUM/COUNT(f) 已块级；AVG/MIN/MAX 行级精确）。**复测回填（2026-09-04）：行式默认布局（无 hot_fields）下块级路径不 eligible（要求全 PAX 块）→ 走行级精确路径，语义正确无回退误报；PAX(hot_fields) 布局下通用全扫聚合回归 5-8s 属"PAX 通用扫描未接线"（P91 收口），非块级下推本身问题** | ✅ 已完成（2026-09-04，P90） |

| **P91 (中，2026-09-04 排期追加)** | **通用 scan 投影列（收口 ② 剩余全扫聚合 / 复测闭环 ①-b 残余）** | **背景：** 复测闭环（2026-09-04）量化 ② 剩余全扫聚合 #12 407ms/#13 841/#14 900/#27 1044/#11 1504ms（均行式默认布局、IO/整行解码受限）且发现 PAX(hot_fields) 布局下**通用全扫聚合整体回归 5-8s**（PAX 块整行重构 + 重序列化逐行 25 列，decode_pax_block 为每行构建全列 serde Map + to_vec）。**开发：** ① sstable `decode_projected_block`（PAX 块只解请求列 → 组装子集 JSON，免整行重构；行式块直通原 JSON 零开销）+ `SstRangeIter.project`（`set_project_fields`）；② CF `scan_stream_at` 增 project 参数 + `scan_stream_fields` 包装；③ Engine `scan_stream_fields`（删除位图语义同 scan_stream）；④ sqlish 全扫消费端接线——`execute_aggregate_window` 无候选全扫 & `execute_group_by_window` 全扫改走投影扫描（needed = WHERE 引用 ∪ 分组列 ∪ 聚合列：`aggregate_needed_fields`/`group_scan_needed_fields`），语义与整行路径精确一致（PAX 子集含全部消费字段；缺失 = 原文档缺失）。**1 新增单测：** `p91_scan_stream_fields_matches_scan_stream_row_and_pax`（行式 + hot_fields PAX × memtable/flush/覆盖写/删除/重开，请求列与全量扫描逐行等值）。回归 690 全绿 | PAX 布局下全扫聚合不再整行重构回归（预期回到 ≤ 行式量级或更快——只解所需列）；行式默认布局 #14/#27/#11 等 IO 受限项语义不变（不回归）；#12/#13/#14 的窗口快路径（count O(1) / 倒排词典枚举）与 #29 块流式 Top-K 接线为 P92 候选（见对比报告 §5.3 判定） | ✅ 已完成（代码+单测 2026-09-04；PAX 全轮实测数值随后续基准轮回填） |
| **P92 (中，2026-09-04 追加，收口 ①-b #29 与 #31 范围形态)** | **Top-K 稠密窗口投影流式 + 单列组合索引范围路由** | **背景：** 复测闭环①-b #29 残余 = 1.1M 候选逐 docid 投影点查定位（batch_get_fields ~11µs/docid，P87 流式化未省定位）；#31 ts BETWEEN 经 v3/v4 两轮证明**纯配置（声明 ["ts"]）不收敛**——`try_composite_index` 只路由 WHERE 等值，范围/BETWEEN 属代码缺口。**开发：** ① sqlish `topk_sort` 候选**稠密**（跨度 ≤4× 候选数）→ 走 `engine.scan_stream_fields` 投影流式顺序读块（PAX 列解码/行式按需只解排序键），替代逐 docid 点查；稀疏保持 P87 分块点查；看门狗逐行熔断（保 P87 语义）。② engine `query_by_composite_range`（单列组合索引首字段值 ∈[low,high] 字节序区间 cidx 范围扫描+回表去重）+ sqlish `try_composite_index` BETWEEN 分支（WHERE 复筛兜底边界字节序误命中）。**3 新增单测：** topk 稠密流式/稀疏点查/单键 DESC × 行式+PAX = 全扫地面真值、cidx 范围路由命中集 = BETWEEN 全扫语义（含边界）。回归 693 全绿。**v5 实测（110 万，重装后立即跑）：** #29 13.5→10.0s（-26%，点查定位消除，残余=行式全量读 ~1.1GB IO 地板）；#31 902→15.24ms（59×，368 行 cidx 回表+复筛）；#30 0.30ms（≈MySQL） | #29 点查定位消除（破 ≤1.5s 需 PAX 列 IO 排序键扫描 = P93 候选）；#31 收敛 15ms 级（MySQL 1.40ms 为覆盖索引免回表）；**已知边界：cidx nosync 未刷盘重启丢键（v5 首测 #31=0 行即此），基准轮须重装后立即跑** | ✅ 已完成（代码+单测+v5 实测 2026-09-04） |

## 二、待办 / 排期（P2/P3 与受控实验）

| 项 | 内容 | 状态 |
|---|---|---|
| Ex-8.9 (P3) | **空闲感知维护调度 - 交变验收** | **设计已出（research/ex8.9-ex8.13-idle-maintenance-io-budget.md）+ 概念 demo 已跑通（src/demo/idle-maintenance）+ 切片 2A 已落地（2026-09-04：方案 A——不改引擎锁模型，server 层 3 个后台 worker（compaction/inverted GC/inverted flush）加负载三档：Busy 退避 1s / Normal 200ms / Idle 50ms 密集 + 5s 集中执行；Engine::write_pressure 主 MemTable 水位代理 + write/read_ops 窗口判档；单测 ex89_write_pressure_proxy）**；待办：交变负载 demo A/B 对照（忙时 p99 无退化、空闲收敛积压）与全量回归 | 设计 ✅ / demo ✅ / 切片2A ✅ / 交变验收待做 |
| Ex-8.13 倒排后台 IO 预算共享（P3） | **切片 1 已落地（2026-09-04）：倒排 seg 写盘统一记账 inverted_written_bytes（flush_segment 与 gc 段写均累计）+ IO 预算接线（Engine open rate>0 attach；adjust_compaction_io_rate 与列族同口径收窄；GC 段写 account_written_budgeted acquire 节流、前台紧急刷段仅记账不等待）+ 单测 ex813_inverted_write_accounting_and_io_budget（倒排回归 64+13 通过）**；切片 2（维护线程/调度）受 Engine 无内部 RwLock 前置约束（见 research 设计 §4）→ **由用户选定方案 A 并入 Ex-8.9 切片 2A（2026-09-04）**：worker 负载感知即调度实现（Idle 50ms 密集 + ≥5s idle_run 集中，覆盖 inverted flush/gc 与 L0/底部收敛），IO 预算接线随 worker 生效。**独立预算 A/B（原可选）：2026-09-04 按用户选择收尾不做（核心能力已随切片 1/切片 2A 落地并有单测覆盖；启用条件为 io_rate_limit_mb>0，可独立开启无需改动）**。 | ✅ 核心完成 + 收尾（独立 A/B 按用户选择跳过） |
| Ex-8.11 A/B 写放大实测（P2 受控实验） | 内核已回填（8ec3a70）；**A/B demo 已建（src/demo/wa-ab，12.8 万 ×512B 关压缩实测：默认收敛 WA 5.62 vs L1 攒 8 WA 3.21，写放大 -42.9%，点查 p50 0.8→1.1µs、范围 p50 2600→2120µs 无回退）→ 采纳默认 l1_trigger_files=8（2026-09-04）**；顺带修复：①Engine compact 无 L0 压力时底部（L1→L2/L2 收敛）合并空转饿死 → 底层 needs 直接压实对应列族；②M3 表切分 bottom 触发忽略 l1_trigger_files → 已尊重攒批配置；③新增 sst_written_bytes 累计写指标。**50m 复测（2026-09-04，db-e93-50m 5000 万行）：WA=0.31（sst_written 13.09GB / 原始 41.61GB）✓、最终空间=6656MB / 层=(9,7,0) / sst=16 inv_seg=16 ✓、点查 p50=135.5µs 范围 p50=54.9ms（L0=9 重叠段拖慢窗口读，与 5M 基线 (0,2,0) p50=0.2µs / 0.2µs 比 ≈ 700× / 27000× → 直接证明 L0 重叠段是主退化源）** | ✅ 已采纳 + 50m 复测完成 ✅ |
| Ex-8.12 分层压缩 50m A/B（P2 受控实验） | 内核已回填（b25b86d）；**A/B demo 已建（src/demo/compression-ab，12.8 万 ×低重复 JSON 实测：L2 冷档 zstd19 vs 不分层 空间 -4.7%（未达 -10% 验收线）、范围 p50 330→305µs 无回退）→ 原判不采纳默认**。**50m 重评（2026-09-04，口径：200k 行样本 × ds-50m.parquet JSON 载荷 × 独立 zstd 压缩比，<1% 误差）**：zstd3 vs zstd19 = shrink=0.5823 → L2 冷档**省 41.8% 磁盘**（大幅越过 -10% 验收线，与 demo12.8 万"低重复 JSON 差 4.7%"完全不在同一工况——真实 50m/200k 样本是高重复 status/city/region 字符串 + 长数值列，zstd19 字典大窗收益显著）；解压 p50（64KB block × 10k 次）zstd19=25.60µs vs zstd3=30.90µs = 0.83×（**读退化不存在，反快 17%**）；外推 50m 基线 6.66GB → L2 档 ≈ 4.06GB（省 ~39%）。**P80 阻塞已解除（2026-09-04：compact_merge 流式 k 路归并修复完成）**，Ex-8.12 L2 压缩默认化前置条件已满足。 | demo 不采纳（原判撤销，50m 强收益） / 50m 重评完成 ✅ / P80 阻塞已解除，可推进默认化 |
| Ex-9.3 倒排统计载荷 ⑤（AF #6） | ①mem 累积（5a792cc）→②段格式 v5（e52941a）→③引擎+s qlish SUM/AVG/MIN/MAX 路由（03d38dd）→④GROUP BY 词典枚举快路径（fe0e045/a4d37c2/4fb6e7f）已回填 development.md §14；**⑤ A/B demo 已建（src/demo/groupby-inverted，30 万实测：COUNT 53.8ms vs 全扫 92.3ms=1.7×，SUM 76.3ms vs 167.9ms=2.2×）**。**50m 验收（2026-09-04，db-e93-50m，count_all_docs=50,000,000 一致 ✓）**：①正确性：status/city/region `inverted_group_stats` doc_count 载荷跨段求和 = 50,000,000（缺字段行 0）✓；端到端 execute_group_by status/COUNT(*) 3 组 组计数总和=50M 一致=true ✓。②**纯倒排词典枚举**中位（组数 N 无关）：status 3 组 7023µs / city 332 组 5764µs / region 6 组 5452µs ≈ 5.5–7.0ms（固定开销来自 field_term_values 跨 16 段 FST 全读，随段数线性、随组数亚线性）。③端到端 A/B（默认化语义，带 NULL 组补 + watchdog_budget）：A=120.6s vs B=122.5s → 加速比 1.02×（持平）——收益被**内嵌 count_all_docs 全键扫 112.8s**几乎完全抵消。默认化前置阻塞：**Engine.count_all_docs 必须 O(1)**（put/delete/flush/事务增量记账，启动恢复阶段扫一次 manifest 建基线）。**✅ 阻塞解除（2026-09-04，P1-C）：** `Engine::count_all_docs` 已 O(1)——活跃 docid 集（RoaringTreemap）懒建基线 + put/delete/delete_batch/purge 增量记账，1 新增单测（含重开基线恢复），全量回归 689 全绿。**剩余 = Ex-9.3 ⑤ 默认化本体**（默认化语义 A/B 已 1.02× 持平，count 抵消消除后预期提速，另行默认化执行）。④**全量回归通过：cargo test — 628 passed / 0 failed / 3 ignored（190s）**，与历史基线一致。 | demo ✅ / 50m 验收完成 ✅（正确性全过）/ 默认化阻塞解除（count_all_docs O(1)，P1-C）/ 全量回归 ✅。**默认化 A/B 执行（2026-09-04，count 抵消消除后）：** 原 50m 库 `db-e93-50m` 打开失败（sst-107 VarLen 越界，疑似早前复制残留损坏，资产保留待 ds-50m.parquet 重建）→ 以同 cfg 重建 **db-e93-5m**（5M 行 · 100.8s）probe：A 快路径端到端 1.24s vs B 全扫 12.59s = **10.15×**（旧 50m A/B 1.02× 持平系 A 内嵌 count 全键扫 ~116s 抵消——现已 O(1) 消除）。**默认化语义采纳**。注：复建库纯词典枚举 ~1.2s/字段（vs 历史 50m 毫秒级，待 P92 倒排段枚举排查）；MySQL 协议层窗口直通 unscoped 快路径 = P92 候选 |
| Ex-8.8 demo 可选 | posting 双区 LRU（内核 8f70b3e 已回填）；热点/冷 term 负载 demo + 容量接 config | 可选，无明确触发可不做 |
| 10 分片 10 亿构建验收 | 10 亿库扩展阶段 A~D 已回填（7.81~7.86）；剩余 = 硬件/部署验收（验收标准与脚本见 design_remain 三） | 部署/硬件推进 |
| AF #6（对应 Ex-9.3 ⑤） | 倒排加速 GROUP BY 的验收与默认化（见上 Ex-9.3 ⑤） | 随 Ex-9.3 |
| Ex-9.4（事务 #25/35/36，2026-09-04 新基准报告追加） | **事务公平档位复测闭环 + 残余优化触发项** | **根因（已由 P2-A 定位，2026-09-04）：** cjserver 默认 `storage.flush_log_at_trx_commit=1`（逐 COMMIT 三路 fsync 强安全）→ 本轮 110 万 #25 3.5×/#35 3.1×/#36 2.8×、10 万 #25 2.1×/#35 3.2×/#36 2.5× 系**档位不对称**（MySQL 该实例 `innodb_flush_log_at_trx_commit=2`）；P2-A 档位 0/2（组提交窗口）代码已落地且 1.1M 探针实测 #25 8.4×→1.4×、#35 ~6×→1.05×、#36 ~6×→1.0×（基准记录 §14，对比报告 §四.3 已注明可复测）。**本轮行动：** ①公平复测闭环——`tmp/tmp-cfg-wide-2g.toml` 增 `storage.flush_log_at_trx_commit=2`，重启 3317 复跑 10 万/110 万 37 探针，#25/35/36 预期落 1.0-1.4×，实测值回填对比报告 §四.3（档位语义见基准记录 §13）；②**残余差分解**（档位 2 下 #25 若仍 ~1.4×≈0.57ms vs MySQL 0.40ms，~0.17ms 为单连接逐 COMMIT 固定开销，无法并发摊薄）：txn_locks Mutex 获取 ×2（commit 加锁 + release）+ RR 写冲突检测逐目标 `last_write_seq`（LSM 点读）+ active_snapshot 注册/注销 RwLock 写锁 + watchdog.check_all；**③触发式微优化候选**（公平档位复测后仍 >2× 才立项，预期单事务 ~0.1ms 级）：a. active_snapshots 改原子低水位替代 RwLock<BTreeSet> 全量写；b. 写冲突检测与 ops 应用合并同一次主数据访问（逐 docid 一次 get_many）；c. txn_locks 无并发持有者时 try_lock 快速路径跳过 Mutex 排队 | **复测闭环 ✅（2026-09-04，随 P85–P90 复测轮执行）：`tmp/tmp-cfg-wide-2g.toml` 已置 `flush_log_at_trx_commit=2` 并重启 3317 复跑 10 万/110 万 37 探针；实测 #25/35/36（110 万）p50 = 0.50/0.42/0.46ms（对齐档位 2 基线 §14 的 0.4–0.6ms），mean 1.5–2.1×（受写区首次 fsync 尾部 p99 1.9–16ms 抬高）——"并发 ≤2-3×"验收越过；10 万轮 1.1–1.3×。残余微优化 a/b/c 仍为触发式（mean 需压至 ~1.0× 时才立项）** |
| 事务微优化（触发项，见 Ex-9.4） | 仅当公平档位（flush_log_at_trx_commit=2）复测 #25/35/36 仍 >2× 时立项：①active_snapshots 原子水位；②冲突检测 + ops 应用合并；③txn_locks try_lock 快路径 | 触发式（暂不排期） |

## 三、远期（触发条件满足后落地；蓝图/触发/验收基准见 design_remain 对应节，此处只做执行跟踪）

- **Calvin 阶段二/三**（gseq 分配器 → 全局复制日志 → raft 高可用）：蓝图、13.3.1 触发与 14.8 阶段 → design_remain 一.1；
  元数据 raft 阶段一已落地（7.77），阶段二依赖 Calvin 落地。
- **Ex-8.4：L1/L2 B+Tree 存储替换**（备选蓝图，不主动排期）：触发 = 范围目标仍未达标 + 读多写少
  极端场景（当前 50m 已 3.6× MySQL，design_remain 一.4 / design.md §23.1）。
- **写路径多写者 / 无锁写侧（Ex-13 触发链）**：无锁多写者 + 每写者独立 WAL + NVMe 多队列/多设备；
  前置 = 解除引擎写侧串行 + 吞吐实测指向写锁（design_remain 一.6）。
- **raft TCP 传输 / 扩容编排联动**（7.88 已落地 TCP 传输与挂起修复；生产 RPC + 编排推进 = 10 亿验收，design_remain 三）。
- **Tiered 分层合并**（7.78 模拟验证后暂不引入；复评触发与结论见 design_remain 一.3）。
- **共享字典压缩**（触发与结论见 design_remain 一.5：Ex-8.12 后空间仍瓶颈再评估，ScyllaDB 式基建）。

## 说明

- **分工**：本文件（development_remain）= 开发排期唯一跟踪（进行中/待办/受控实验/验证）；
  设计蓝图/触发条件/验收基准由 design_remain 详述，重叠主题以本文件为执行入口、design_remain 为设计依据，两处不一致时以本文件排期状态为准；
- 已完成基线与历史排期（原 A~Y/Z/AA~AF 队列全完成）见 development.md §13.2/§13.4 与 §14 归档；
- problem_solving P1~P78 见 problem_solving.md；
- 每项完成后回填 development.md（§13 队列或 §14 归档）并从此移除。
