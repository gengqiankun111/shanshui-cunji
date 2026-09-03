# development_remain.md —— 开发未完成任务

> 2026-09-03 治理：**已完成内容已回填 development.md**（§13 大项队列 / §14 收口归档 / 7.x 各节）；
> **明确不开发**项 → development_givenup.md；
> 本文件仅保留 **未完成 / 进行中 / 待评估 / 远期触发**。开发路线入口 = development.md §13 + 本文件。

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

## 二、待办 / 排期（P2/P3 与受控实验）

| 项 | 内容 | 状态 |
|---|---|---|
| Ex-8.9 空闲感知维护调度（P3） | **设计已出（research/ex8.9-ex8.13-idle-maintenance-io-budget.md）+ 概念 demo 已跑通（src/demo/idle-maintenance：写突发后空闲，A 现行为残留 L0×6/倒排 pending 23.7 万不落段；B 空闲窗口维护队列→L0=0/L1=1 单文件、pending 落段 seg=1，数据一致）**；内核实现切片（io_budget → 维护线程 → 接入四项）待排期 | 设计 ✅ / demo ✅ / 内核待做 |
| Ex-8.13 倒排后台 IO 预算共享（P3） | **切片 1 已落地（2026-09-04）：倒排 seg 写盘统一记账 inverted_written_bytes（flush_segment 与 gc 段写均累计）+ IO 预算接线（Engine open rate>0 attach；adjust_compaction_io_rate 与列族同口径收窄；GC 段写 account_written_budgeted acquire 节流、前台紧急刷段仅记账不等待）+ 单测 ex813_inverted_write_accounting_and_io_budget（倒排回归 64+13 通过）**；切片 2（维护线程/调度）受 Engine 无内部 RwLock 前置约束（见 research 设计 §4），待排期 | 切片 1 ✅ / 切片 2 待做 |
| Ex-8.11 A/B 写放大实测（P2 受控实验） | 内核已回填（8ec3a70）；**A/B demo 已建（src/demo/wa-ab，12.8 万 ×512B 关压缩实测：默认收敛 WA 5.62 vs L1 攒 8 WA 3.21，写放大 -42.9%，点查 p50 0.8→1.1µs、范围 p50 2600→2120µs 无回退）→ 采纳默认 l1_trigger_files=8（2026-09-04）**；顺带修复：①Engine compact 无 L0 压力时底部（L1→L2/L2 收敛）合并空转饿死 → 底层 needs 直接压实对应列族；②M3 表切分 bottom 触发忽略 l1_trigger_files → 已尊重攒批配置；③新增 sst_written_bytes 累计写指标 | ✅ 已采纳（50m 复测可选） |
| Ex-8.12 分层压缩 50m A/B（P2 受控实验） | 内核已回填（b25b86d）；**A/B demo 已建（src/demo/compression-ab，12.8 万 ×低重复 JSON 实测：L2 冷档 zstd19 vs 不分层 空间 -4.7%（未达 -10% 验收线）、范围 p50 330→305µs 无回退）→ 不采纳默认（保持 level_l2=0），50m 真实库复测后重评** | ✅ demo（未采纳，50m 复测可重评） |
| Ex-9.3 倒排统计载荷 ⑤（AF #6） | ①mem 累积（5a792cc）→②段格式 v5（e52941a）→③引擎+s qlish SUM/AVG/MIN/MAX 路由（03d38dd）→④GROUP BY 词典枚举快路径（fe0e045/a4d37c2/4fb6e7f）已回填 development.md §14；**⑤ A/B demo 已建（src/demo/groupby-inverted，30 万实测：COUNT 53.8ms vs 全扫 92.3ms=1.7×，SUM 76.3ms vs 167.9ms=2.2×）**；50m 规模验收与全量回归挂起（需先建 50m 库） | demo 已建 / 50m 验证待做（原 §19） |
| Ex-8.8 demo 可选 | posting 双区 LRU（内核 8f70b3e 已回填）；热点/冷 term 负载 demo + 容量接 config | 可选，无明确触发可不做 |
| 10 分片 10 亿构建验收 | 10 亿库扩展阶段 A~D 已回填（7.81~7.86）；剩余 = 硬件/部署验收（验收标准与脚本见 design_remain 三） | 部署/硬件推进 |
| AF #6（对应 Ex-9.3 ⑤） | 倒排加速 GROUP BY 的验收与默认化（见上 Ex-9.3 ⑤） | 随 Ex-9.3 |

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
