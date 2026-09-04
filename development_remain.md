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

### 4. 宽表基准缺口闭合（排期，2026-09-04 审计采纳 + 3000 万行目标修订；验收口径 = 110 万行 37 探针 vs MySQL，见 user_guide/宽表SQL性能基准记录.md §9~§12）

> 外部审计逐项核对结论：**过期不排** = ①docid 与倒排 32 位冲突（已由 P79 / c3f8403 解决：inverted 段 v6、Posting=RoaringTreemap 全链 u64、旧段读取升 64 位默认表零迁移）；
> ②Compaction"未实现"（已具备并调优：Ex-8.11 写放大 A/B 采纳 l1_trigger=8、Ex-8.12 分层压缩 A/B、Ex-8.9 空闲感知合并调度）。
> **采纳排期（执行顺序）** = ~~P2-B~~ ✅ → ~~P0-C~~ ✅ → P0-A → P0-D → P0-B → P1-C → P1-D → P2-A → P2-D → P1-E → P3-A → P3-B → P3-C → P4-A → P4-B → P4-C。
> 3000 万行目标下 P1-A/P1-B 升级为 P0（30M 放大后可用性阻断）。
> 2026-09-04 按 design_goal.md 对照补充：引擎已实现但 SQL 协议层未接线的断裂点（P1-D/P1-E）。
> 2026-09-04 3000 万行隐藏瓶颈分析（存储层物理限制）：已具备项确认（布隆 ✅ 分区布隆 v5 已下推、多表分层隔离 ✅ compaction 按表切分 SST + L1/L2 层范围粗筛跳非目标表、多级缓存 ✅ HotCache+BlockCache+OOM 水位）；P3 追加项针对 30M 放大后剩余物理限制。

| 序 | 项 | 内容 | 验收（1,098,342 行 / 37 探针；3000 万行外推） | 状态 |
|---|---|---|---|---|
| P2-B | 删除位图稀疏化（多表 docid 主题最后硬伤） | DeletionBitmap 现按 docid 稠密 `Vec<u8>` 寻址（bitmap.rs）→ 非默认表高位 docid（tid<<48）下 delete/DROP 内存爆炸（理论 32TB），M3 收尾只能关 `storage.deletion_bitmap_enabled` 降级 Tombstone。**demo 已验证方案 a（RoaringTreemap）+ kernel 已整合（src/bitmap.rs 重写）：** 稠密 Vec<AtomicU8> → RoaringTreemap 稀疏位图 + ArcSwap COW 无锁读 + 全量序列化持久化；21 单测 + 7 引擎测试全绿；API 完全兼容（mark_deleted/clear/is_deleted/is_deleted_key/deleted_count/has_pending/flush/purge 签名不变） | 非默认表开 deletion_bitmap 跑 rr-conformance 双表 DELETE/DROP 不 OOM；默认表单删回归不退化 | ✅ 已完成 |
| P0-A | SQL 组合索引：混合扫描兜底 + 声明式路由（用户选"两者都做"） | 阶段 1 混合扫描兜底：status 位图倒排候选 + ts/amount 范围条件进倒排/位图（数值范围位图或行过滤降载），先收敛 #30/#31 全扫过滤；阶段 2 声明式路由：schema `composite_indexes` → sqlish 谓词规范化填充 `index_prefix` → 引擎 CompositeIndex 路径（cidx 列族前缀扫描 + 回表，写路径维持索引同步；engine/optimizer 机制已具，缺口在 SQL 层接线）。**断裂点 G1：** engine.rs `query_by_composite_prefix()` + cidx 列族已实现，sqlish.rs 未填充 `QuerySpec.index_prefix` | #30 status='active' AND ts=? 921ms → <10ms（30M 外推 ~25s → <1s）；#31 ts BETWEEN 7300ms → 同量级收敛（MySQL 对照 0.30 / 1.40ms）。注：MySQL 覆盖索引免回表，cjserver cidx+回表非覆盖，全平需 cidx 存全部查询列 | ⏳ |
| P0-B | ORDER BY Top-K 有界堆 | sqlish `SORT_MAX_ROWS=200_000` 守卫对含 ORDER BY 全量物化、忽略 LIMIT → 110 万行 + LIMIT 100 被 1064 拒。改 LIMIT k（小 k）走 BinaryHeap 部分有序（内存 O(k)）；无 LIMIT 保留全量守卫 | #29 ORDER BY k,amount LIMIT 100：1064 拒绝 → 可跑（110 万 ~1.2s；30M 外推 2-4s）；窗口/早停路径不回归 | ⏳ |
| P1-C | 无索引聚合加速 | COUNT(*)/SUM(amount)/GROUP BY（无索引列）现全扫解 25 列宽行：①COUNT 走 keys-only 扫描（复用 count_keys_range 基建不解行值，**已接线**）；②声明式统计载荷（Ex-9.3 ⑤ SUM/AVG/MIN/MAX 随 term 载荷）推广到高频数值列默认启用 | 110 万行 count_all 5869ms → keys-only ~1800ms（~3×）；30M 外推 ~50s。5-10s 需 zonemap/列存配合（远期） | ⏳ |
| P1-D | 倒排统计载荷范围条件扩展（design_goal 断裂点 G3） | **引擎已实现** engine.rs `inverted_term_stats()` 支持随 term 的 SUM/AVG/MIN/MAX 载荷聚合。**协议层断裂：** sqlish.rs 仅裸 `field=value` 等值条件才走统计载荷路径（`execute_aggregate_window` 等值分支）；`BETWEEN`/范围条件或 `status='active' AND ts>?` 组合条件回退全扫解行。需扩展统计载荷路由至范围/组合条件（倒排候选 ∩ 范围过滤后走载荷，而非全扫） | 110 万行 sum_where_enum 6524ms → 倒排候选+载荷 ~500ms（~13×）；30M 外推从 ~180s → ~15s | ⏳ |
| P2-A | 事务/写路径 fsync 语义对等 | 单连接事务提交逐 COMMIT 等 fsync（组提交仅并发摊薄）→ 事务对比 8×：①核对 sqlrun/rr-conformance 事务是否落组提交路径（db_adapter 启动 set group_commit_us 的覆盖范围，**已确认 mysql_server.rs 启动默认 2000µs**）；②config 可配提交耐久档位（对齐 MySQL innodb_flush_log_at_trx_commit 语义）；③档位语义写入基准文档 | 并发场景 ≤2-3×；单连接结构差 4-5×（每 COMMIT fsync 语义差难消） | ⏳ |
| P2-D | 倒排回表批量预取（design_goal 断裂点 G2） | **引擎已实现** engine.rs `batch_get()`（含 HotCache + Delta + 删除位图批量过滤）。**协议层断裂：** sqlish.rs 3 处 `engine.get()` 逐行调用（L1468/L1508 排序+普通查询、L1083 谓词复检），零 `batch_get` 调用。改：位图命中结果按 docid 排序 → `engine.batch_get()` 批量取行（利用 LSM 有序性 + BlockCache 局部性）→ 减少随机 IO。对 30M 放大后位图命中取行收益最大 | 110 万行 enum_sel_limit100 4.2ms → ~1.5ms；30M 外推从 ~120s → ~30s | ⏳ |
| P1-E | Zone Map SQL 层范围剪枝（design_goal 断裂点 G4） | **引擎已实现** sstable.rs `IndexEntry.zones` 块级 min/max 统计；column_family.rs 层级范围粗筛 `[lmin,lmax]` 已用于点查/批量点查跳过整层。**协议层断裂：** sqlish.rs 范围查询（`ts BETWEEN`、`amount > N`）未走 zone map 块级剪枝——全扫仍解所有块。需在 scan_pushdown / eval 路径接入 SST 块级 zone map 过滤（跳过 min/max 不相交块） | 110 万行 cmp_between 6686ms → zone map 剪枝 ~2000ms（跳过 ~70% 块）；30M 外推从 ~180s → ~55s | ⏳ |
| P3-A | L0 按表分组层范围（多表查询避免不必要扫描） | **现状：** compaction/flush 按 `docid>>48`（table_id）切分 SST → 每个 SST 只含单表数据；L1/L2 层范围粗筛天然跳过非目标表整层。**瓶颈：** L0 层范围 = 所有表 SST 的 [min,max] 并集 → 多表混布 L0 时无法整层跳过，需逐 SST 布隆校验（虽布隆已实现 v5 分区下推，但 30M 下 L0 段数增多、逐文件布隆 + 二分定位开销累加）。**优化：** L0 层范围改 per-table 分组（`HashMap<tid, (min, max)>`），点查时按 docid 高 16 位定位目标表组 → 仅扫该组 SST | 30M 10 表 L0 点查 p99 从 ~5ms（逐 SST 布隆）→ ~1ms（表组定位 + 布隆）；L0 段数 50+ 时收益最大 | ⏳ |
| P3-B | 列存块 / 微分区下推（30M 全扫聚合物理限制） | **现状：** 行存 + 分区布隆 + 块级 zone map（sstable.rs `IndexEntry.zones`）；无列存块格式。**瓶颈：** COUNT(*)/SUM(amount)/GROUP BY 无索引列需全扫解 25 列宽行（110 万 5.9s → 30M ~180s）；keys-only COUNT 已缓解 COUNT(*) 但 SUM/AVG 仍需解值。**优化：** SSTable 块内列存编码（按列打包数值列，min/max/zonemap 跳块 + 列读免解无关列）；或独立列存文件（按高频聚合列构建） | 30M SUM(amount) 从 ~180s → <10s（跳过 70% 块 + 列读免解 24 列）；需 SST 格式升级或独立列存 | ⏳ |
| P3-C | 自适应缓存水位（30M 缓存命中率物理限制） | **现状：** HotCache(行级) + BlockCache(LRU 块级) 两级 + OOM Guardian 水位限流；30GB 数据 vs 2GB HotCache → ~7% 命中率。**瓶颈：** 30M 下大量点查穿透 HotCache → BlockCache → 磁盘；无自适应淘汰升级（HotCache 满时自动升级到 BlockCache 加大、或热点探测提升高频 key 到 HotCache）。**优化：** ①HotCache 自适应淘汰策略（LFU 替代 LRU，保热点）；②BlockCache 按表分区水位（多表场景热点表多分配）；③可选：SSD-aware 块预读（点查 miss 时预读相邻块） | 30M 10 表点查缓存命中率从 ~7% → >40%；p99 从 ~5ms → <1ms（缓存命中） | ⏳ |

### 5. 高难深水区排期（2026-09-04 advance_develop.md 分析；正确性优先，方案 B 最先开发）

> 来源：research/advance_develop.md 四大难点分析。逐项核对代码现状后判定：
> - 难点 1（Compaction 动态自适应）：Ex-8.9 空闲感知 + Ex-8.11 l1_trigger=8 已具备静态层；缺**写入速率自适应**（L0 爆胀防护）。
> - 难点 2（MVCC + 删除位图版本化）：**精确断裂点已定位**——engine.rs:1257 `get_at` 中位图短路返回 None 不看 snapshot_seq → RR 违反。`get_at` 本身已具备版本化读（L1262 `primary.get_bytes_at(key, snapshot_seq)` 按 seq 过滤 + tombstone 语义保留）；Transaction 已有 `write_set`（本事务写后读可见）+ `snap_cache`（快照缓存）。修复核心 = 快照读路径跳过位图、让 LSM 版本裁决。用户选定**方案 B（全局删除位图 + 事务快照删除日志）**，排期最先开发。
> - 难点 3（倒排 FST 大 Term 集）：base.fst + delta.fst 分层已具（7.34）；缺 delta 上限 + 多 Segment 倒排。30M 下可暂缓（静态阈值 + NVMe 可撑）。
> - 难点 4（成本估算优化器）：仅硬编码规则；缺统计信息 + 代价模型。30M 下可暂缓。

| 序 | 项 | 内容 | 验收 | 状态 |
|---|---|---|---|---|
| **P0-C** | **方案 B：MVCC + 删除位图版本化（RR 正确性修复，最先开发）** | **断裂点：** engine.rs:1257 `get_at` 中 `bm.is_deleted(docid)` 短路返回 None，不看 snapshot_seq → 事务 B 删除后事务 A 快照读违反 RR。**已有基础：** `get_at` L1262 `primary.get_bytes_at(key, snapshot_seq)` 已按 seq 过滤 LSM 版本（tombstone 语义保留）；Transaction 已有 `write_set`（read_own 本事务写后读可见）+ `snap_cache`（快照点查缓存）。**修复（已完成）：** `get_at` 快照读路径跳过全局删除位图，让 LSM 多版本 + tombstone seq 裁决——tombstone seq ≤ snapshot_seq → 快照前已删 → None；tombstone seq > snapshot_seq → 快照后删 → `get_bytes_at` 返回旧版本值；RC / 非事务读保留位图短路。demo 9 测试全绿 + kernel 631 测试全绿。**R4 补强（review 闭环）：** compact 保活——`ColumnFamily::mvcc_keep_floor` + `Engine::active_snapshots`（RR 事务注册/注销），活跃快照期间 compact 保留删除/覆盖前旧版本，快照读跨 compaction 仍正确 | C1~C9 RR 一致性测试在 **deletion_bitmap_enabled=true** + 并发跨事务删除下全绿；快照读已删行返回旧版本值；点查 p99 不退化 | ✅ 已完成 |
| **P0-D** | **JOIN 支持：DocIdSet 统一抽象 + 8 阶段优化器流程（参考 research/optimizer_proces.md）** | **设计来源：** research/optimizer_proces.md（2026-09-04 更新版，8 阶段流程重构：WHERE 与 JOIN 组合顺序变更）。**新流程核心变化：** 旧流程 JOIN 先 → WHERE 下推；**新流程每张表 WHERE 独立先产出 DocIdSet → JOIN 基于 DocIdSet 交集 → 跨表 WHERE 后过滤**。8 阶段：①阶段 0 安全检查（非等值 JOIN/表数≥3/大表无索引拒绝）→ ②**阶段 1 每张表 WHERE 独立产出 DocIdSet**（1.1 组合索引前缀→SortedList、1.2 倒排等值→Bitmap、1.3 多倒排 AND→Bitmap、1.4 混合扫描→SortedList、1.5 全扫+ZoneMap→Stream、1.6 keys-only COUNT→Stream、1.7 全扫兜底→Stream）→ ③**阶段 2 JOIN 路径**（2.1 主键 JOIN 直达、2.2 索引-索引 DocIdSet.intersect、2.3 有索引-无索引、2.4 广播哈希 <10万、2.5 拒绝）→ ④阶段 3 跨表 WHERE 后过滤 → ⑤阶段 4 GROUP BY → ⑥阶段 5 HAVING → ⑦阶段 6 ORDER BY（Top-K/全量守卫）→ ⑧阶段 7 LIMIT 下推 → 阶段 8 执行计划。**核心改动：** 新增 `DocIdSet` 枚举 + `get_docid_set()` + `intersect()` + JOIN 执行器统一用 DocIdSet。**依赖：** P0-A（组合索引提供 SortedList）、P2-D（batch_get 批量回表）。**排期项→优化器映射：** P0-A→阶段 1.1、P0-D→阶段 2、P0-B→阶段 6.1、P1-C→阶段 1.6、P1-D→阶段 4.1、P2-D→阶段 1/2 回表、P1-E→阶段 1.5 | 2 表等值 JOIN 1 万行 <1ms、100 万行 ~500ms；主键 JOIN 直达；广播哈希 <10万行；非等值/3 表/大表无索引拒绝 1064 | ✅ 已完成（INNER/LEFT JOIN 解析+执行；多 JOIN 解析期拒绝；从表 1:N 展开；JOIN 路由先于组合索引；见 P81） |
| P4-A | Compaction 写入速率自适应（难点 1） | **现状：** Ex-8.9 空闲感知调度 + Ex-8.11 l1_trigger=8 静态阈值 + Ex-8.13 IO 预算。**缺口：** 无写入速率自适应——写入爆发时 L0 段数暴增、compaction 按部就班 → L0 爆炸 → 点查扫数十 SST → p99 从 0.2ms 飙到 200ms。**优化：** ①实时监控 L0 段数 + 写入速率 + 磁盘 IO 利用率；②动态调整 compaction 线程数 / 每次合并数据量 / l1_trigger；③L0 stall 阈值动态收放（写入高峰降低阈值强制 compact、低峰放宽）；④与 Ex-8.9 空闲感知协同（Busy 档提前 compact） | 30M 持续写入下 L0 段数 ≤ 阈值（如 12），点查 p99 < 2ms；写入爆发（10× 均值）不 stall | ⏳ |
| P4-B | 倒排 delta.fst 上限 + 多 Segment 倒排（难点 3，30M+ 规模） | **现状：** base.fst + delta.fst 分层（7.34）已具；无 delta 上限、无多 Segment。**缺口：** 高频写入 → delta.fst 膨胀 → base+delta 合并查询超时。**优化（远期）：** ①delta.fst 大小上限 + 自动 roll into base（全量重建 FST，期间查旧 FST）；②或重构为多 Segment 倒排（类 ES，每 segment 独立 FST，查询合并结果） | 30M 高频写入下 delta.fst ≤ 上限；查询 latency 不退化；FST 重建不阻塞查询 | ⏳ 远期 |
| P4-C | 基于成本的优化器（难点 4，30M+ 规模） | **现状：** optimizer.rs 仅硬编码规则（主键点查禁倒排、组合前缀）。**缺口：** 无统计信息、无代价模型 → 多条件查询选错执行计划（慢 1000×）。**优化（远期）：** ①ANALYZE TABLE 采样统计（字段基数、min/max、值分布直方图）；②代价模型（倒排查 N docid 开销 vs 全扫 M 行 zone map 剪枝开销）；③动态执行计划选择 | 30M 多条件查询选择正确执行计划；status='active' AND amount>5000 优先走 amount zone map 剪枝 | ⏳ 远期 |

## 二、待办 / 排期（P2/P3 与受控实验）

| 项 | 内容 | 状态 |
|---|---|---|
| Ex-8.9 空闲感知维护调度（P3） | **设计已出（research/ex8.9-ex8.13-idle-maintenance-io-budget.md）+ 概念 demo 已跑通（src/demo/idle-maintenance）+ 切片 2A 已落地（2026-09-04：方案 A——不改引擎锁模型，server 层 3 个后台 worker（compaction/inverted GC/inverted flush）加负载三档：Busy 退避 1s / Normal 200ms / Idle 50ms 密集 + 5s 集中执行；Engine::write_pressure 主 MemTable 水位代理 + write/read_ops 窗口判档；单测 ex89_write_pressure_proxy）**；待办：交变负载 demo A/B 对照（忙时 p99 无退化、空闲收敛积压）与全量回归 | 设计 ✅ / demo ✅ / 切片2A ✅ / 交变验收待做 |
| Ex-8.13 倒排后台 IO 预算共享（P3） | **切片 1 已落地（2026-09-04）：倒排 seg 写盘统一记账 inverted_written_bytes（flush_segment 与 gc 段写均累计）+ IO 预算接线（Engine open rate>0 attach；adjust_compaction_io_rate 与列族同口径收窄；GC 段写 account_written_budgeted acquire 节流、前台紧急刷段仅记账不等待）+ 单测 ex813_inverted_write_accounting_and_io_budget（倒排回归 64+13 通过）**；切片 2（维护线程/调度）受 Engine 无内部 RwLock 前置约束（见 research 设计 §4）→ **由用户选定方案 A 并入 Ex-8.9 切片 2A（2026-09-04）**：worker 负载感知即调度实现（Idle 50ms 密集 + ≥5s idle_run 集中，覆盖 inverted flush/gc 与 L0/底部收敛），IO 预算接线随 worker 生效。**独立预算 A/B（原可选）：2026-09-04 按用户选择收尾不做（核心能力已随切片 1/切片 2A 落地并有单测覆盖；启用条件为 io_rate_limit_mb>0，可独立开启无需改动）**。 | ✅ 核心完成 + 收尾（独立 A/B 按用户选择跳过） |
| Ex-8.11 A/B 写放大实测（P2 受控实验） | 内核已回填（8ec3a70）；**A/B demo 已建（src/demo/wa-ab，12.8 万 ×512B 关压缩实测：默认收敛 WA 5.62 vs L1 攒 8 WA 3.21，写放大 -42.9%，点查 p50 0.8→1.1µs、范围 p50 2600→2120µs 无回退）→ 采纳默认 l1_trigger_files=8（2026-09-04）**；顺带修复：①Engine compact 无 L0 压力时底部（L1→L2/L2 收敛）合并空转饿死 → 底层 needs 直接压实对应列族；②M3 表切分 bottom 触发忽略 l1_trigger_files → 已尊重攒批配置；③新增 sst_written_bytes 累计写指标。**50m 复测（2026-09-04，db-e93-50m 5000 万行）：WA=0.31（sst_written 13.09GB / 原始 41.61GB）✓、最终空间=6656MB / 层=(9,7,0) / sst=16 inv_seg=16 ✓、点查 p50=135.5µs 范围 p50=54.9ms（L0=9 重叠段拖慢窗口读，与 5M 基线 (0,2,0) p50=0.2µs / 0.2µs 比 ≈ 700× / 27000× → 直接证明 L0 重叠段是主退化源）** | ✅ 已采纳 + 50m 复测完成 ✅ |
| Ex-8.12 分层压缩 50m A/B（P2 受控实验） | 内核已回填（b25b86d）；**A/B demo 已建（src/demo/compression-ab，12.8 万 ×低重复 JSON 实测：L2 冷档 zstd19 vs 不分层 空间 -4.7%（未达 -10% 验收线）、范围 p50 330→305µs 无回退）→ 原判不采纳默认**。**50m 重评（2026-09-04，口径：200k 行样本 × ds-50m.parquet JSON 载荷 × 独立 zstd 压缩比，<1% 误差）**：zstd3 vs zstd19 = shrink=0.5823 → L2 冷档**省 41.8% 磁盘**（大幅越过 -10% 验收线，与 demo12.8 万"低重复 JSON 差 4.7%"完全不在同一工况——真实 50m/200k 样本是高重复 status/city/region 字符串 + 长数值列，zstd19 字典大窗收益显著）；解压 p50（64KB block × 10k 次）zstd19=25.60µs vs zstd3=30.90µs = 0.83×（**读退化不存在，反快 17%**）；外推 50m 基线 6.66GB → L2 档 ≈ 4.06GB（省 ~39%）。⚠️ 投入默认化前置阻塞：**P80 L1→L2 底部合并卡死**（compact_merge 全量 Vec 物化 + watchdog 500ms 空转双因；5M 50m 双规模复现，见 problem_solving P80，需另立项改流式 k 路归并 + compact 返回推进语义）。 | demo 不采纳（原判撤销，50m 强收益但被 P80 阻塞落地，默认 level_l2=0 保持）/ 50m 重评完成 ✅ / 落地待 P80 |
| Ex-9.3 倒排统计载荷 ⑤（AF #6） | ①mem 累积（5a792cc）→②段格式 v5（e52941a）→③引擎+s qlish SUM/AVG/MIN/MAX 路由（03d38dd）→④GROUP BY 词典枚举快路径（fe0e045/a4d37c2/4fb6e7f）已回填 development.md §14；**⑤ A/B demo 已建（src/demo/groupby-inverted，30 万实测：COUNT 53.8ms vs 全扫 92.3ms=1.7×，SUM 76.3ms vs 167.9ms=2.2×）**。**50m 验收（2026-09-04，db-e93-50m，count_all_docs=50,000,000 一致 ✓）**：①正确性：status/city/region `inverted_group_stats` doc_count 载荷跨段求和 = 50,000,000（缺字段行 0）✓；端到端 execute_group_by status/COUNT(*) 3 组 组计数总和=50M 一致=true ✓。②**纯倒排词典枚举**中位（组数 N 无关）：status 3 组 7023µs / city 332 组 5764µs / region 6 组 5452µs ≈ 5.5–7.0ms（固定开销来自 field_term_values 跨 16 段 FST 全读，随段数线性、随组数亚线性）。③端到端 A/B（默认化语义，带 NULL 组补 + watchdog_budget）：A=120.6s vs B=122.5s → 加速比 1.02×（持平）——收益被**内嵌 count_all_docs 全键扫 112.8s**几乎完全抵消。默认化前置阻塞：**Engine.count_all_docs 必须 O(1)**（put/delete/flush/事务增量记账，启动恢复阶段扫一次 manifest 建基线）。④**全量回归通过：cargo test — 628 passed / 0 failed / 3 ignored（190s）**，与历史基线一致。 | demo ✅ / 50m 验收完成 ✅（正确性全过，词典枚举毫秒级）/ 默认化阻塞：count_all_docs 需 O(1)（另立项）/ 全量回归 ✅ |
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
