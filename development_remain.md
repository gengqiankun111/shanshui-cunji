# development_remain.md —— 开发未完成任务

> 2026-09-03 治理：**已完成内容已回填 development.md**（§13 大项队列 / §14 收口归档 / 7.x 各节）；
> **明确不开发**项 → development_givenup.md；
> 本文件仅保留 **未完成 / 进行中 / 待评估 / 远期触发**。开发路线入口 = development.md §13 + 本文件。

插队排期，优先开发：

> 2026-09-05 追加：**10 万轮对比（results-sqlrun-compare-100k，P85~P92 生效后）37 探针 vs MySQL §2.3/2.4 根因转化**。
> 逐项核对现状后**仅 3 项真实可修复缺口**，置顶于 Task-002 之前，各自单独排期执行（销项记录按 problem_solving P 序列续号 P96+）：
> - Task-021：COUNT 全包窗口直通 O(1)（#12 3.1×）——报告 2.4「全包窗口 O(1)」对应项；P1-C 引擎 O(1) 已备、协议层窗口接线未做
> - Task-022：点查/IN 投影列解码瘦身（#2 2.9× / #3 / #9）——P86② 字节级提取基建已备、点查输出路径未接
> - Task-023：主键 IN 列表批量定位（#4 3.8×）——P92 稠密判定 + delete_range50 keys-only 基建复用
> **不排说明（核对结论）**：全扫投影列解码（#12/#14/#27/#29）= P91 ✅ 已完成，本轮残余为行式 IO 地板（归远期 PAX 列 IO，P93-候选已消费给重构）；倒排回表早停（#6/8/9）= P85 ✅；写放大 Per-CPU WAL 已在「三、远期」Ex-13 触发链；#5 主键范围归并 / #16 窗口排序 = LSM 结构差接受；#32 1.2× 差距小不排；报告 2.4「动态缓存调整（阶段 3）」无现成设计，属评估项未立项。

Task-021：COUNT 全包窗口直通 O(1)（#12 count_all，10 万 42.84ms vs MySQL 14.01ms = 3.1×）

> 根因：`execute_aggregate_window`（src/sql/executor/aggregate.rs `scoped` 判定 + L55-67）对带表窗口的 `COUNT(*) 无 WHERE`
> 强制窗口内 keys-only 全扫（`scoped=true` 禁引擎快路径，防跨表串表）；引擎 `count_all_docs`（P1-C ✅ O(1)：活跃 docid
> RoaringTreemap 懒建基线 + put/delete/delete_batch/purge 增量记账，src/engine/scan.rs）仅 `scoped=false` 直连全库启用。
> MySQL 协议层聚合一律带本表 docid 窗口 → 默认表 [0,2^48) **全包窗口**仍 keys-only 线性扫（~0.43µs/行；110 万 ~407ms，
> P1-C 复测记录）——即报告 2.4「P92 全包窗口 O(1)」所指（dev_remain P92 为 Top-K 稠密窗口，未覆盖此接线）。
属性	内容
优先级	高（#12 全扫类根因唯一未落地项；10 万 27 落后项中收益最直接）
工作量	1 天
依赖	`Engine::count_all_docs` O(1)（P1-C ✅，src/engine/scan.rs L247）；活跃 docid 位图（含多表 docid）
风险	中（窗口计数语义须与现 keys-only 口径一致；事务/活跃快照场景核对）
具体工作：

□ 引擎层：新增按表区间 O(1) 计数 `count_docs_range(start,end)` = 活跃 docid 位图区间基数（RoaringTreemap range 跳桶，O(命中桶)）；默认表全包窗口直通现有 `count_all_docs`，非默认表窗口 [tid<<48,(tid+1)<<48) 走区间基数（多表隔离正确）
□ 接线：aggregate.rs scoped `COUNT(*) 无 WHERE` 分支（L55-67）——窗口覆盖整表（默认表 [0,2^48) / 非默认表整表区间）→ 直通 O(1)；部分窗口保持 keys-only；若现 keys-only 扫描为非版本化口径则活跃快照下亦直通（对齐现引擎快路径），若版本化则活跃快照存在时回退 keys-only（对齐 P90 块级下推 eligible 风格）
□ 单测：整表窗口 = scan 口径 / 部分窗口 keys-only 不回退 / 非默认表区间计数 / 墓碑 + 删除位图 / 多表隔离 / 活跃快照回退（按上条口径）
验收标准：

- 10 万 #12 count_all 42.84ms → ≤1ms（MySQL 14.01ms）；110 万 ~407ms → ≤1ms
- COUNT 语义与现 keys-only 逐行计数一致（同 key 最新版本、Tombstone 跳过）；P1-C/Ex-9.3 ⑤ 既有快路径单测不回退
- 全量回归通过
> ✅ 已完成（2026-09-05，P96）：Engine::count_docs_range（活跃 docid rank 差值区间基数）+ aggregate
> 整表窗口 COUNT(*) 直通 + 单测（整表/部分窗口/多表隔离/删除）；全量 695 passed / 0 failed。

Task-022：点查/IN 投影列解码瘦身（#2 pk_point_proj10 2.9× / #3 pk_in_5 2.2× / #9 field_in 5.5× 回表投影）

> 根因：点查 `SELECT 10 列`（0.46ms）比 `SELECT *`（0.22ms）慢 2×——SELECT* 直出原字节零解析；投影需整行 serde parse +
> 提取 10 成员 + 重序列化（解码开销随投影列数而非命中数）；MySQL 0.16ms 免投影解码。P86② 字节级单遍 MapAccess 只收目标
> 成员基建已具（`row_sort_keys` / `light_top_field`，src/sql/executor/eval.rs），但仅接排序/Top-K/聚合路径；点查/IN 回表
> 输出路径（executor/select.rs 点查分支 + P85 `collect_limited_rows` 消费端）未接。
属性	内容
优先级	中
工作量	1 天
依赖	P86② 字节级字段提取基建（✅ 已备）；P87③ 输出瘦身 / `decode_pax_block_fields`（✅ 已备）
风险	低（缺失字段 / JSON null / 转义畸形语义复用 P86② 护栏，逐行等值单测兜底）
具体工作：

□ 定位点查/IN 输出组装点（点查分支 + `collect_limited_rows`/batch_get 消费端）：显式列清单（非 SELECT*）时对回表原字节做字节级只收目标成员提取 → 组装子集 JSON，替代整行 parse→重 serialize；缺失列 = 原文档缺失语义
□ SELECT* 保持原字节直通零解析（#1 不回退）
□ 存储布局分流：PAX 块走 `decode_pax_block_fields` 单块多列一次解码（P87 已接线复用）；行式块走 light 字节级提取
□ 单测：点查投影与整行投影逐行等值（缺失 / null / 转义畸形护栏）+ 10 列投影 ≤ SELECT* 耗时（#2 语义）
验收标准：

- 10 万 #2 pk_point_proj10 0.46ms → ≤0.22ms（= #1 SELECT* 量级）；#3/#9 同步受益不回退
- 全量回归通过
> ✅ 已完成（2026-09-05，P97）：server/protocol/response.rs `stream_projected_map`（单遍 MapAccess
> 只收目标顶层字段，非目标 IgnoredAny 跳过）接入 build_result_set；点号/下标嵌套投影回退整行
> parse（语义不变）；SELECT id/doc 纯列零解析直通保持。单测：子集 = 整行 parse 逐字节等值
> （缺失/null/嵌套/转义/长文本）+ 直通回归；全量 697（lib 693+seqlock 4 单独绿）通过。

Task-023：主键 IN 列表批量定位（#4 pk_in_50 1.73ms vs MySQL 0.46ms = 3.8×）

> 根因：IN 50 主键**逐条** LSM 点查（memtable/delta/L0 层级 + 布隆 + 块定位逐键重复），MySQL 主键批量回表单次下探。
> 复用 P92 topk 稠密判定 + delete_range50 keys-only 区间扫 + P2-D batch_get 基建：相邻同块键合并为顺序读（BlockCache
> 局部性 + 批量取行），替代逐键随机定位。
属性	内容
优先级	中
工作量	1~1.5 天
依赖	P92 稠密判定（跨度 ≤4× 计数，✅）/ delete_range50 keys-only 区间扫（✅）/ P2-D batch_get（✅）/ P84 DocIdSet（✅）
风险	中（墓碑/删除位图/多表隔离语义须与逐条 get 一致；稠密/稀疏边界判定）
具体工作：

□ IN docid 排序去重 → 稠密判定（跨度 ≤4× 计数，对齐 P92 topk）：稠密 → `engine.scan_stream_ids` 区间 keys-only 扫现存 + 可见行批量序读回表（P85/P92 消费端复用）；稀疏 → 维持分块 `batch_get`
□ 可见性语义：墓碑/删除位图命中不占结果、offset/limit 计数与逐条 get 一致；多表 IN 按表 docid 区间隔离
□ 消费端接线：读路径点查 IN（SELECT id IN …）对齐 P83/P88 写定位同源语义；不回退 P84 DocIdSet 消费
□ A/B demo（src/demo/ 对应目录）+ 单测：稠密/稀疏边界、边界值含墓碑、offset/limit、多表区间
验收标准：

- 10 万 #4 pk_in_50 1.73ms → ≤0.5ms 量级（对齐 #1/#3）；#3 pk_in_5 不回退；删除/可见性语义回归
- 全量回归通过
> ✅ 已完成（2026-09-05，P98）：Engine::get_many_pk_in（排序去重 → 稠密 scan_range 区间顺序读 +
> 集合过滤 / 稀疏 batch_get）接入 server/command/select.rs `id IN` 分支（行序保持 IN 列表序、
> 可见性同 get）；单测稠密/稀疏/去重/删除隐藏 = 逐条 get 一致。全量 698（lib 694+seqlock 4）通过。

Task-024：全扫/排序残余 IO 收口（#29 + #14/#27/#11 合并项，2026-09-05 分级表 P0/P1）

> **分级（110 万复测）**：🔴 #29 orderby_multi 9.46s（不可用）｜🔴 #12 count_all 406ms → **Task-021
> ✅ 已收口（O(1) 区间计数）**｜🟡 #14/#27 全扫分组 856~1057ms｜🟡 #11 全扫过滤 1425ms｜🟢 #17-19
> 单行更新 ~1.2ms → Per-CPU WAL（远期，另列）。
> **合并理由**：#29/#14/#27/#11 同根 = 默认行式布局下全扫对整行（25 列原文）取数/解码 IO——P91
> 投影列下推（scan_stream_fields）与 P92 稠密窗口 Top-K 已接线，残余是**行式块整行读 + 子集抽取**
> 的物理 IO（110 万 ~0.43µs/行 × 全扫）。合并为一执行项统一收口，避免逐条拆散重复做功。
属性	内容
优先级	P0（#29 不可用）→ 合并 P0/P1
工作量	1.5~2 天
依赖	P86② 字节级提取 / P87 decode_pax_block_fields / P91 scan_stream_fields / P92 稠密窗口（均 ✅）
风险	中（列 IO 布局只对 PAX(hot_fields) 数据生效；行式默认布局回退正确性）
具体工作：

□ 排序残余 #29：无 WHERE ORDER BY k,amount LIMIT —— 默认行式布局把“排序键提取”改走行式块按需字段
  直通（P86② 只收 k/amount 两列，免整行 parse→子集重排）；PAX(hot_fields) 数据（基准库重装时声明
  hot_fields=[k,amount]）走 decode_pax_block_fields 列解码（P87 已接线路径复用）→ 消除整行 25 列 IO
□ 全扫分组 #14/#27：group_scan_needed_fields 已只收 needed（WHERE∪分组∪聚合列），默认行式布局下对
  每行原字节做 light 只收 needed 提取直通（复用 row_sort_keys 同款单遍 MapAccess），避免整行 Value
  构造 + 丢弃；PAX 布局走列解码（P91 已接线）
□ 全扫过滤 #11：cmp_between 无索引数值过滤——行式默认布局 needed=WHERE 引用列（1 列），按需字段
  直通；PAX 列 IO 下推（P90/P91 基建）
□ 单测：行式默认布局 全扫分组/排序/过滤 = 整行路径逐值一致（含缺失/转义畸形护栏）；PAX 两布局交叉
□ 验收口径：110 万 #29 9.46s → ≤1.5s（对齐 MySQL ~0.53s 的 3× 内）；#11/14/27 各自 ≤ 现值的
  0.5× 且比值对齐 MySQL 2~3× 内；回归全绿
> ✅ 阶段①（2026-09-05，P99）：GROUP BY 全扫 #14/#27 子集一次构建（sql/executor/eval.rs
> `subset_doc_bytes` 单遍只收 needed + group_by.rs 全扫回调接线，免逐字段整行 parse×N）；回归 698
> （lib 694+seqlock 4）绿。**阶段②（#29/#11）**：残余为默认行式布局整行 IO + 过滤解码，须以
> PAX(hot_fields) 数据布局（基准库重装声明 hot_fields）复测回填（行式 IO 地板见 research 设计档）。
> ✅ 阶段② PAX 10 万复测（2026-09-05，P105，results-pax-sqlrun-compare-100k）：
> hot_fields=[k,amount,ts,status,city,region] 下 SCC mean——#9 3.60→2.73ms(-24%, 5.5→4.0×)、
> #11 58.05→54.22ms(-7%, 1.5→1.3×)、#14 68.61→63.31ms(-8%, 2.7→2.4×)；#29 234.9→235.7ms、
> #12 42.8→43.2ms、#2 0.46→0.45、#4 1.73→1.73、#5 3.29→3.11 **未达验收** → 暴露接线缺口（见下）。
> ⚠️ 新开发缺口（PAX 复测暴露，非纯复测可补）：
> ① 点查/IN 投影下推：server/sql 点查仍走 engine.get 整行解码（PAX 行不解列）→ 需 engine 投影
>   点查（get_fields 按需列）接线 #2/#3/#4（目标 #2≤0.25、#4≤0.8，P101 后复测）。
>    ✅ 已完成（2026-09-05，P106）：select.rs `projection_pushdown_fields` 判定（纯顶层简单字段、
>    无 doc/嵌套）→ 点查 engine.batch_get_fields / id IN Engine::get_many_pk_in_fields（稠密
>    scan_stream_fields / 稀疏 batch_get_fields + assemble_subset_json 子集组装）接线；PAX 块任意
>    字段单列解码、cell/列类型与整行路径逐值一致；+2 单测（引擎行式/PAX×稠密稀疏 + 协议层
>    端到端 = 整行参考路径逐字节），全量 711（707 通过 + seqlock 偶发独立绿 + 3 ignored）。
>    ✅ 复测回填（2026-09-05，PAX 10万既有库 cjserver 侧，results-pax-gap1b-scc-100k，P105 同库对照）：
>    #1 0.24→0.22（SELECT* 零提取地板）、#2 0.45→0.41、#5 3.11→2.82、#3 0.40→0.41、#4 1.73→1.75
>    持平——**接线生效无回退**。绝对值 #2≤0.25/#4≤0.8 未达：10万库全部行写路径直写 hotcache
>    （Task-027 put 直写）且 ~100MB≪1024MB，点查基本热命中整行；且单查询固定开销 floor = #1
>    0.22ms（协议/parse/响应组装），投影提取+类型组装仅 ~0.19ms 增量、PAX 列解码只占其中小头
>    → 推下收益被 floor 淹没。**修复复测暴露的 hotcache 命中二次 parse 退化**（热行改直通整行、
>    仅冷行走列解码）。缺口① 收口：冷读受益方向正确、数值验收按 floor 语义（#2 ≈ #1 + 小余量）
>    或待 110 万超缓存轮（宽行回表冷读）验证。
>    ✅ 110万 轮补充（2026-09-05，db-wide-scc 行式布局，results-pax-gap1c/d/e-scc-1100k）：
>    读探针三轮 #2 0.44~0.56 / #4 2.30~2.61 / #5 2.99~3.61（旧基线 0.44/1.96/3.32），但 #1
>    （SELECT*，与本改动无关）同步 0.25→0.29→0.37 爬升——每轮 3 万行 insert+整区 delete 使 L0
>    段/墓碑累积、布局逐轮劣化抬高读基线，既有行式库无法稳定 A/B（非 PAX 亦非本项收益场景）；
>    顺带修 `extract_fields_from_json_row` 行式提取改单遍流式（P87②/P86② 消费端受益），全量 708 绿。
>    结论：缺口① 无系统性回归，冷读列解码收益需 PAX 布局大库重建后另行评估。
> ② #12 COUNT(*) O(1)：fresh-load 多 L0 快照不 eligible → 回退 43ms 全扫；放宽为活跃 docid
>   rank 计数（不依赖单层快照形态）。
>    ✅ 已完成（2026-09-05，P107）：活跃 docid rank 计数（count_docs_range/count_all_docs）本已
>    层形态无关（P96）；根因 = fresh-load 场景 cjserver 空数据目录打开 → live 基线 None → load 期
>    put/delete 不记账 → 首个 COUNT 触发一次性全键扫基线（~200ms 拖高 5 次均值 43ms）。修复 =
>    engine/open.rs **空库打开即播种空活跃集**（primary.data_empty() → Some(空)），load 全程增量
>    记账 → 首个 COUNT 亦 O(1)（免基线全扫）。单测 gap2_count_o1_fresh_load_empty_open_multi_l0
>    （空库播种 + 小 memtable 多 L0 + PAX + 删除/复活/多表高位隔离 = keys-only 口径）。fresh-load
>    10万 PAX 实测（results-gap2-scc-100k）：#12 42.84ms → **0.27ms**（p50 0.28/p99 0.29，5 次全
>    O(1)），验收 ≤1ms 达成；harness 首个 COUNT 亦 O(1)。全量 709 绿。
> ③ #29/#5 残余 = 行读+块 IO/跨文件扇出 → Task-025b 阶段④。
>    ✅ 已随 Task-025b 阶段④ 落地（2026-09-05，P109）：scan_pushdown 接线跨文件扇出并行；
>    #5/#11 多段/重叠 L0 窗口 IO 并行（行读+块 IO 中的多文件读延迟不再串行相加），数值随压测回填。
> ④ #11 数值 zone 运行时路由待核：若 BETWEEN 行级走 scan_all 未带 zonepred，zone 只生效于
>   scan_pushdown 路径（正确性已由 task025 单测保障；运行时收益待接线）。
>    ✅ 核实完成（2026-09-05，P108）：**无运行时缺口**——裸 BETWEEN/比较谓词 100% 走
>    scan_pushdown（带 ZonePredicate）：cost-based 分支（select.rs L621，range-only 无倒排候选
>    必 FullScan）与兜底分支（L643 scan_leaf）双路直达，scan_all 仅服务 AND/OR 复合内范围臂
>    （已先经倒排位图收敛，zone 不适用）。iter 层块级跳块（f64 安全比较 + 字节序回退）与
>    CF/Engine scan_stream_with_zonepred 链路完整。P105 #11 仅 -7% 归因 = amount 列**随机非
>    聚类**：每块 ~50-60 行随机样本的 zone min-max ≈ 全表跨度 → 与任何窄窗口相交、跳块率≈0；
>    zone 收益须列随 docid 聚类（ts/自增类）。**可选后续（不排期，仅计划精度）**：select.rs
>    L613 `zone_fields` 恒空 → cost 模型固定 effectiveness=0.3，可传 PAX hot_fields 提升
>    计划精度（range-only 下仍走 FullScan，无行为影响）。
> 内存口径备注（2026-09-05）：1.1M 行 MySQL 2G pool 充足，暂不加大；公平对比应按“缓存预算”口径
> 或把 SCC hotcache 收紧至实际 ~2G（如 512MB）复测（详见 user_guide/性能对比-2026-09-05-P85-P92后-10万与110万.md §4）。

Task-025：范围查询提速——Partition Pruning 与 Parallel Scan（2026-09-05 用户修正：**要开发**，非远期）
> 修正（2026-09-05 用户判定，原“远期”作废）：这两项是收益最高的“金矿”——
> ① **Partition Pruning（分区剪枝）**：LSM 天然多路归并，范围查询须确认所有 SSTable；维护
> “分区范围统计缓存”（Zone Map 升级版，Meta/内存），范围查询先查缓存跳过不含目标数据的分区；
> 业界实证范围查询 +30~40%（#5 9.2× / #11 3.4× → “不可用→可用”）；侵入小（查询路由/元数据剪枝）。
> ② **Parallel Scan（并行扫描）**：单 SQL 单核 → 按 Key 范围切分并发扫描归并（X-Engine 已落地，
> 聚合/导出响应短数倍）；16 核理论 4-8×（时间换 CPU）。块内索引暂缓（已有块级索引+Zone Map）；
> 重叠度控制不单独立项（并入 Compaction 参数实验，防 B+Tree 化损写吞吐）。
属性	内容
优先级	P0/P1（要开发；剪枝先行，并行规划/阶段 3）
工作量	剪枝 1~1.5 天；并行扫描 2~3 天（阶段 3）
依赖	Leveled 层范围粗筛 / Zone Map（sstable zones）/ TTL 分桶（已有）；并行扫描依赖无锁读 + IO 预算（Ex-8.9/8.13）
具体工作：

□ Partition Pruning：列族层/引擎层按查询范围维护分区（表桶/SST 组）min-max 缓存 → 范围查询先剪枝
  跳过不相交分区（对齐 P3-A l0_table_ranges 风格扩展）；SST 打开期或 flush/compact 时增量更新
□ Parallel Scan：range/全扫聚合把 docid 窗口切块 → 并发 scan_stream* + 归并（按 docid 升序合并），
  看门狗分批熔断；先覆盖 COUNT/SUM 聚合与导出，再覆盖通用扫描
□ 验收：#5 pk_between 3.3ms → ≤1.5ms 量级；#11/范围类比值对齐 MySQL 2~3× 内；多核扫描 TPS/延迟按核数扩展
> 设计档：research/range-scan-percpu-wal-design.md §一。
> ✅ 阶段①（2026-09-05，P100，剖析 demo 定策）：文件/层/表/块四级剪枝已具备（Ex-8.2/P62/P3-A/
> P1-E）；#5 小窗口 3.3ms 主因 = 固定 seek+块 IO+宽行返回（非剪枝缺口），转 Task-025b 并行/块 IO；
> 数值列块级 zone 剪枝安全化（iter.rs f64 比较，防 `"10"<"9.0"` 字节误剪）已落地，**生效条件**：
> 列须在 hot_fields（PAX）才产 zone 行 → #11 用 hot_fields 含 amount 的 PAX 库复测回填；
> 并行扫描（Task-025b）保持规划（阶段 3）。
> ✅ Task-025b 阶段①（2026-09-05，P102）：无 WHERE + 有限窗口的**并行全扫聚合**已落地
> （aggregate.rs：按核 2..=8 等分 docid 子窗并发 scan_stream_fields，COUNT/SUM/MIN/MAX/AVG
> 交换律合并，逐行与串行 no-WHERE 分支一致；其余路径串行回退）；5000 行一致性测试绿，
> 全量 705 绿。
> ✅ Task-025b 阶段②（2026-09-05，P103）：**通用 WHERE 并行**（worker 内与串行 acc 一致判定，
> 解除无 WHERE 限制）+ **GROUP BY 分片合并**（局部分组按组键/累加器逐项合并，count/n_num/sum
> 相加、min/max 极值）；双状态 5000 行 WHERE 聚合与 GROUP BY 并行=串行逐组一致，全量 705 绿。
> ✅ Task-025b 阶段③（2026-09-05，P104）：**Engine::scan_range_parallel** 条带并行全扫
> （等分子窗并发 + K 路归并，同 scan_range 契约）作为导出/条带构建块；聚合/分组窗口并行
> 已覆盖（①/②）。全量 706 绿。
> **Task-025b 阶段④（独立、深，未开发）**：跨文件扇出并行——CF scan 层逐文件并发 + k-way
> 归并（#5/#11 多段/重叠 L0 场景主杠杆），需 CF scan_stream_at 层改造（与 Ex-8.9/IO 预算结合）。
> ✅ 已完成（2026-09-05，P109）：CF `scan_stream_at_parallel`（column_family/scan.rs）——窗口命中
> ≥2 SST 且 workers≥2 时每 SST 源一个 scoped 线程批量推进（FAN_BATCH=512，块读/解压/解码并行，
> sync_channel cap2 背压），主线程沿用堆归并（同 key 折叠/快照/删除/zone/投影/回调早停语义与
> 串行逐行一致）；spawn 后即 drop 原始 Sender 集（否则 worker 结束 channel 不关闭 → 末批 recv
> 死锁，已实测修复）。memtable 内联；workers<2/命中<2 SST 自动回退既有串行 scan_stream_at（零
> 回归）。Engine::scan_stream_parallel（+删除位图过滤）与 scan_pushdown（裸比较/BETWEEN 谓词下推，
> workers=可用并行度 clamp 2..=8）接线。**+1 单测** task025b4（行式/PAX × 多 L0 + 覆盖写/删除/
> memtable 尾行 × workers 2/4/8 = 串行逐行一致 + 早停前缀一致 + 投影等值），全量 710 绿。
> 数值收益面向多 L0 大文件窗口（压测随基准轮回填）；与 Ex-8.9 IO 预算经既有 scan_limiter（输出
> 行字节节流）协同。

Task-027：HotCache TinyLFU 读回填准入（2026-09-05 用户定：**优先开发**，先于 Task-025b/PAX 复测与 Task-026）
> 设计：research/cache_TinyLFU.md §二/§三（Count-Min + Doorkeeper + 衰减；衰减采样阈值 N 默认
> 4×width≈104 万次 Record，可配覆盖，非“查询次数”）。
> 定参（2026-09-05 用户确认）：sketch **固定 4 哈希 ×512×512 ×4-bit ≈1MB**（不做动态宽度）；
> 统计/准入**仅读回填**（engine 读 miss 后 LSM 命中走准入），写 put 直写不回绝。
属性	内容
优先级	P0（优先开发，置于 Task-026 之前）
工作量	sketch+doorkeeper 1 天、reset 0.5 天、接线（engine 读回填准入）1 天、单测验收 0.5 天
依赖	现有 hotcache（seqlock 读写锁/LFU 采样淘汰/软水位）——只加准入端，不改淘汰端
具体工作：
□ src/hotcache/tinylfu.rs：CMS（4 哈希 ×512×512、4-bit 饱和）+ Record/Estimate(4 取最小)/reset(全量>>1)
□ doorkeeper：首访 Bloom，准入 = doorkeeper 命中 || Estimate ≥ 3（默认门槛，配置可调）
□ 衰减：累计 Record 样本 ≥ N 触发 >>1（N=4×width 默认 ≈1,048,576；配置 reset_samples 覆盖，0=默认）
□ 读回填接线：engine 读路径 LSM 命中后 Estimate 过门槛才 put/promote（防全表扫/扫描型回填污染）；
  写 put 保持直写；invalidate 清 doorkeeper 位、计数靠衰减淡出（CMS 无法单 key 精确删）
□ 单测：zipf 频率误差 <5%；顺序全扫不污染（准入率低）；reset 后热点仍高/冷归零；删后重写可再准入
□ 验收：ycsb c（全随机读）命中率/吞吐不降、内存受控；离线条带导出后点查 p50 劣化 ≤1.5×
> 衰减问答（2026-09-05）：不配“查询次数”；按累计 Record 样本数触发，N/(读QPS)≈实际衰减秒数。
> ✅ 已完成（2026-09-05，P101）：tinylfu.rs（CMS+可删除 Doorkeeper+采样衰减）、put/invalidate
> on_write（计数减半+清门卫）、engine 读回填改走 read_backfill（点查 get/batch_get），
> config tiny_lfu_*（enabled/admit=4/reset=2048）；hotcache 21 绿 + 全量 704 绿；
> 验收（ycsb c/扫描后点查 p50）待下次基准回填。

Task-026：Per-CPU WAL（可选项默认开启；**排期最靠后**，2026-09-05 定稿）
> 定位：#17-19 单行更新 ~1.2ms（WAL fsync + LSM 写放大 + 全局锁）；Per-CPU WAL 主要解高并发写锁竞争
> （16 核 64 线程 22 万 → 30-35 万 TPS，+36~59%；单笔 #17 0.8-1.0ms，-15~32%），单笔延迟改善有限。
> 默认 `per_cpu_enabled = true`，配置/`--per-cpu-wal false` 回退全局组提交；SHOW STATUS 暴露队列
> 健康度；WAL 格式兼容（旧 `wal-{seq}.log` + 新 `wal-{queue}-{gseq_start}.log` 共存识别，gseq 全局
> 归并回放）。完整设计与排期（8.5 天：接口 0.5/核心 3/恢复 1.5/配置 0.5/监控 0.5/测试 1/压测 1.5）见
> research/range-scan-percpu-wal-design.md §二。**所有 Task-021~025 之后最后实施**。
> ⏳ 进度（2026-09-05）：**阶段1 ✅（接口/配置/路由骨架，commit 2843832）**——config.storage
> per_cpu_{enabled,queues,depth,batch_window_us} + engine/percpu_wal.rs（队列解析/CPU 路由/轮询
> 回退/满队列背压/深度-消费监控，默认关闭单队列零开销回退）+ Engine 持有 + 3 单测，全量 712 绿。
> **阶段2/3 设计细化已定稿**（research/percpu-wal-stage2-design.md：方案 A engine 级外置队列 WAL、
> WalEntry(cf,op,key,value)+gseq 原子组、wal-{q}-{gseq_start}.log 命名与 checkpoint 裁剪、旧文件
> 迁移共存、gseq 归并恢复与洞跳过、2a-3b 分步实现顺序）→ 核心/恢复按该文档在专门会话推进。
> ✅ **已完成（2026-09-05，P110，提交 0c1897b/7c1d7e3/810f182）**：2a WalEntry 编解码 + 队列文件 IO；
> 2b/2c CF external_wal（TLS scope 单 gseq 组收集 + flush 水位回调）+ Engine 全写入口接线 + 每队列
> 消费线程按窗口写独立文件 + flush_all 同步排空 + checkpoint.json 持久化/段裁剪 + 背压；3a 打开准备
> （cp 加载/旧 WAL 迁移防空 L0/队列 gseq 归并回放/last_enqueued 防空转 CF 钉死 cp/global_seq 续接/
> begin_snapshot 改 global_seq/backup/purge/status 适配）；3b 默认翻 true 定稿。+10 引擎级 + 4 运行时
> 单测，全量 736 绿。
> **压测回填（2026-09-05，本机 12 逻辑核，非 16 核）**：ycsb 增 `--per-cpu-wal true|false` +
> `--per-cpu-window-us`（测量为共享引擎 Mutex 串行 op 模型）。workload a、records=10 万、12 线程×
> ops=2 万，best-of-3：PerCpu（队列窗口 100µs）run **109.8k ops/s**（p50 9.3µs / p99 1.06ms；load
> 26.6 万 w/s）vs Global 组提交 1000µs **67.7k ops/s**（p99 1.99ms）与 Global 组提交 100µs **41.6k**
> （单后台线程高频窗口锁竞争最差）。PerCpu vs Global 最优档 **TPS +62%**、p99 ≈ -47%、load ≈ +55%——
> 与设计 16 核预期（+36~59%）方向一致；注：串行锁模型仅反映 WAL fsync 路径收益（N 队列并行窗口 vs
> 单后台线程组提交），真实 16 核多写者/无锁摊薄场景收益另由专属压测计（待办，见下方「16 核专属
> 压测（待办）」）。

#### 16 核专属压测（Task-026 验收待办，2026-09-05 立项）
> 状态	⏳ 未启动（无 16 核环境；阿里云 2 核服务器不满足，环境备忘）
> 目标	兑现 research/range-scan-percpu-wal-design.md §二 收益量化：**16 核 64 线程写 22 万 →
>   30-35 万 TPS（+36~59%）**；单笔 #17 update_id 1.18ms → 0.8-1.0ms（-15~32%）。
> 已测边界	本机 12 逻辑核 ycsb（共享引擎 Mutex 串行 op）：PerCpu +62% TPS / p99 -47%（见上）——
>   只证明 WAL fsync 路径收益；**尚未覆盖"多写者并发写"的锁/队列摊薄**。
> 前置（关键，先定口径再执行）
> □ 明确"多写者"形态——当前引擎写路径为 `&mut self` 串行 + 单实例共享锁，候选：
>   a) mysql server 多 session 并发写（推荐先跑：贴近业务；写侧锁内串行、组提交/PerCpu 队列窗口内
>      跨写者攒批 fsync → 测 fsync 并行摊薄）；
>   b) 多实例/分片 YCSB 聚合对照（聚合 TPS ≈ 实例数 × 单实例，非"单引擎多写者"，作旁证）；
>   c) 若目标确为"单引擎多写者无锁写并行"：需另行立项写路径并行化/分片写队列（超出本任务范围）。
> □ 定"22 万→35 万"验收口径对应上述哪种形态（建议 a 或 a+b 双口径）。
> 验收	16 核机器；ycsb（`--per-cpu-wal true|false` 已支持）或 mysql 并发写，线程数 ≥ 2×核数
>   （如 16 核 → 32/64 线程），与 Global（`--per-cpu-wal false` + 同窗口组提交）best-of-3 对照；
>   记录 TPS + p50/p95/p99 + load w/s；结论**内联回填本条目**（不落 results 目录）。
> 依赖	压测形态 a 无需代码改动（已有 mysql server 多 session 能力）；c 需新立项。

> 2026-09-04 收敛说明：原 20 项插队任务经与 development.md / 本文件其余内容逐项对照，**Task-001/003/004/006/008~020 已移除**——原因：与既有实现同主题重复（FST 字典 7.34+P4-B、倒排回表批量 P2-D/P85~P87、TTL 按天分桶整目录 O(1) 删除、WAL 延迟删除异步 unlink、组提交/环形 WAL、Bloom 分区布隆、基准/验收体系等，均已实现 ✅）或属随父项销项（Task-004/017→Task-002、Task-011/015/018→Task-007/Task-014、Task-012→Task-006~011）。**仅保留以下 3 项独立任务**（真实缺口，与插队族解耦，各自单独排期执行）：

- Task-002：fxhash 局部替换（内部 Key）——真实缺口 ✅（Cargo.toml 全库无 fxhash）、验收（内部 Key 延迟 ≥15%）可控 ✅、工作量 1 天 ✅ → **执行**
- Task-005：性能基准基线采集——独立项（口径对齐既有基准流程，勿另起体系）
- Task-007：层级时间轮基础框架——真新增基建 ✅ **框架已完成（2026-09-05，P111）**：src/timing_wheel.rs
  （秒/分/时/天 509 桶 + schedule/tick/advance 跳步 + 取消 + 10 分钟 checkpoint + 冷启动恢复 + 空轮
  O(1) 快进）；挂载权衡（备注）定：TTL/Compaction 时序沿用现方案，框架留作后续挂载基建。+7 单测。

Task-002：fxhash 局部替换（内部 Key）

> 备注：无同名排期项（真新增）。目标容器均为既有资产：HotCache DashMap+LFU / BlockCache LRU 双重索引 / TermCache（development.md §六）、无锁化与分片（§十三）；且 BlockCache 已按表分区 + 自适应淘汰（development_remain 一.5 P3-C ✅）。"倒排字典保留 ahash 抗 HashDoS"与现架构一致；Task-004/017 为其伴随项。
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


Task-005：性能基准基线采集

> 备注：**同主题已有更全体系**——YCSB 压测（shanshui-cunji-ycsb、--group-commit-us）、宽表 110 万/3000 万 37 探针 vs MySQL（development_remain 一.4 验收口径 + user_guide/宽表SQL性能基准记录.md §9~§14 分档/复测/回填闭环）、Ex-8/9 50m 复测（development.md §十八）。本项（baseline_20260904.json 单点快照）为早期思路，应并入既有基准流程而非另起基线。
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
> ✅ 已完成（2026-09-05）：新增探针 bin `shanshui-cunji-baseline`（src/bin/baseline.rs，复用引擎公开
> API + ycsb 补多线程，不另起体系）→ 本机 12 逻辑核单点快照落 `benchmarks/baseline_20260904.json`：
> - bulk 写（put_nosync+flush_wal）345,213 w/s；持久写 `put`（Per-CPU 队列窗口 100µs 落盘，入队 ack）
>   350,301 tps（p50 1.4 / p95 2.7 / p99 5.1µs）；
> - 点查 warm-hotcache 28,680 qps（p95 319µs，单线程循环冷热抖动所致）；**冷读多线程（ycsb c 12t）
>   222,793 qps（p50 3.3 / p95 172 / p99 233µs）**；
> - 倒排单 term（city=c3 ≈1 万命中，**含回表**）223 qps（p50 1.07ms / p99 5.6ms）；
> - TTL 过期桶删除扫描（10 万行全过期，open 期整目录 O(1) 清理）3.73ms，存活 0；
> - 冷启动（10 万行 SST 全量加载重开）178.0ms。
> 注：单点快照仅对比参考；37 探针 vs MySQL 分档/复测仍由宽表基准流程（user_guide/宽表SQL性能
> 基准记录.md §9~§14）执行。
> ✅ 10w 数值验收回填（2026-09-05，随 Task-033 会话）：干净双端 100k（MySQL ddl+load / SCC 新目录
> wide-load + 重启 cidx 重建）全套探针（results-sqlrun-compare-100k）→ 5 项收益（Task-028/029/031/
> 030/032）验收数值逐项回填至各任务注，含 1 项未达（Task-030 count_distinct_enum 65.49ms 未 ≤10ms，
> 权威窗口扫描口径，倒排快路径未接——见 P115/该任务注）与 Task-033 outcome 实证（MySQL waiter-1205 /
> SCC waiter-ok@3ms，行 37/76）。
验收标准：

基线数据已保存到 benchmarks/baseline_20260904.json


Task-007：层级时间轮基础框架

> 备注：两文档均无"时间轮"，本项为真新增基建（无同名可对照）。但其三个挂载对象现各有方案（见 Task-008/009/010 备注），其中主挂载对象 TTL 删除的现实现为**按天分桶 + 整目录 O(1) 删除**：SST 按 timestamp 分桶（按天目录）、桶内 Compaction 跨桶不合并、过期整目录删除（O(1)、无墓碑，不依赖任何定时器）——时间轮若引入需与之在删除粒度/延迟误差/重启重建上权衡，而非补一个缺失能力；另两个挂载对象（Compaction 调度 / WAL 延迟删）同样已有现方案。现时序调度风格为后台线程轮询（段 GC 7.73：100ms 轮询 + 10min 兜底；Ex-8.9 worker 三档退避）。新增 509 桶结构 + 检查点持久化 + 冷启动重建成本不小，建议先评估是否值得替换现有 TTL/Compaction 时序。
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
> ✅ 已完成（2026-09-05，P111，commit 见本会话）：src/timing_wheel.rs 四级 509 桶（绝对秒取模放置，
> 整点级联天→时→分→秒）、schedule/TaskHandle/tick/advance（按"下一事件边界"跳步，空轮 O(1) 快进，
> 1 年 TTL MockClock 加速验证）、cancel（全轮摘除）、checkpoint.json（每 10 分钟 + Drop 兜底落盘，
> tmp+rename 原子）、load_checkpoint/restore_from（冷启动按 kind 重建未完成任务）。**挂载权衡定**：
> TTL 按天分桶整目录删除 / Compaction 轮询调度 / WAL 延迟删均**沿用现方案**，本框架作为基建留待
> 后续挂载对象评估替换时直接复用。+7 单测（509 桶放置/跨级跃迁/各刻度精确触发/取消/检查点+重启/
> Drop 落盘/空轮快进），全量 736 绿。
验收标准：

单元测试覆盖所有层级跃迁

时间加速测试（MockClock）：验证 1 年 TTL 正确触发

重启后任务不丢失（检查点恢复）

Task-028：组合索引（cidx）持久化 / 启动重建——重启丢键静默空结果（10w 轮 #1 复合索引异常根因 A，正确性 P0）
> 实测（2026-09-05，scc-sqlrun-100k 副本 + 当前 release 二进制）：`composite_indexes` 声明在场时，
> 同一查询在**关闭前/重启后**结果不同——重启后 `WHERE status='active' AND ts=…`（数据中确定存在 ~1e4 命中）
> 返回 **0 行 / 0.37ms**（cidx 内存条目全丢 → 前缀扫描空集直接返回，不回退 eval）；同库同查询在无 cidx
> 声明配置下（fallback）返回 10000 行。即：**重启后声明了组合索引的等值/范围查询静默丢行（错误结果）**。
> 既有 P92 v5 备注「cidx nosync 未刷盘重启丢键（v5 首测 #31=0 行即此）」为已知边界，本项正式修复。
> 存储侧：cidx/ 目录仅 wal.log（0 字节），无 checkpoint/段文件；engine.open（open.rs L293）仅把
> `composite_indexes` 声明载入内存，**不对存量 primary 数据回扫建索引**。
属性	内容
优先级	P0（正确性：重启后组合索引查询静默丢行，比性能劣化严重）
工作量	1~1.5 天
依赖	engine/open.rs 打开流程、cidx 写路径（engine/write.rs L185）、keys.rs 复合键编解码
风险	中（重建耗时与打开期/后台并发、删除/覆盖 stale 键清理；与 write.rs stale 复筛语义一致）
具体工作：
□ 方案 A（首选，成本可控）：open 时检测 cidx 声明非空且内存索引空 + primary 非空 → **启动期/后台一次性
  存量回扫重建**（scan_stream 全量，按字段组逐 doc 提取复合键 → add；与倒排 GC worker 同生命周期管理，
  看门狗分片熔断；期间查询走既有 eval fallback 保证正确不丢行）；重建完成后置就绪标记
□ 方案 B（可选叠加，后续）：cidx 增量持久化（段/checkpoint 或复用 cidx/wal.log 落盘键条目），免重启重建
□ 验收：重启前插入 status/ts 数据 → 重启后 composite_idx_point / composite_idx_multi_eq 结果 = 关闭前
  （≥1 行，非 0）；重启后首查即 <5ms（重建后走 cidx）；无声明配置行为不变；回归全绿
> ✅ 已完成（2026-09-05，P112）：Engine::ensure_composite_index_backfill（open 期：primary 非空且
> `cidx.sig` 签名不符或 cidx 空 → primary 全量回扫重建复合键 → memtable_put_nolog 直入 + flush 落 SST +
> 写签名标记，崩溃幂等重做；正常会话零开销）+ CF memtable_put_nolog + 单测 task028（后加配置/签名
> 变更/正常重开三态）。全量 740 绿。
> 收益锚点：报告 composite_idx_point 24ms/p99 148ms（实为 nocidx 声明缺失的 eval fallback——见 Task-029）；
> 本项修的是「声明在、重启丢」的静默错误（正确性），两任务互为补充。
> 数值验收回填（2026-09-05，10w 干净双端轮）：SCC 全新 data 目录 wide-load 100k 后重启（cidx 空 +
> 签名缺失 → open 期回扫重建，cidx 落 4.1MB sst）→ 套件 composite_idx_point/multi_eq/range 均正常出数
> （0.31/0.26/0.81ms，见 Task-029 回填），无「重启后 0 行」；首查即走 cidx <5ms ✅（正确性验收随 P112
> 单测 + 本轮回填印证）。

Task-029：AND(等值,等值) 等值后过滤批量取数——eval post_filter 逐 docid get 改块级 batch（10w 轮 #1 根因 B）
> 根因：`status='active' AND ts=V`（ts 为数值/未索引等值）→ eval And 分支把 ts 等值当扫描叶在 status
> 位图（~2e4~3e4 候选）上 **post_filter 逐 docid `engine.get`**（eval.rs L637-660），无 LIMIT/0 命中须遍历
> 全候选，成本 = O(候选) × 点查常数；无 cidx 配置下实测（scc-sqlrun-100k 副本）：ts=1730000000 0 命中
> 2322ms（≈77µs/候选，重启后 hotcache 空 + memtable 读）、ts=1700000000 1e4 命中 919ms（cap 截断 1e4 行）；
> 报告 10w 热缓存会话同路径 24ms（≈1.2µs/候选）。对照：SUM(amount) WHERE active（P1-D/P91 候选块级
> batch_get_fields）同候选 ~71ms ≈ 3.5µs/docid——批量即收敛。
属性	内容
优先级	P0（#1 性能面：cidx 不可用/未声明/未命中时一切等值 AND 组合都吃此路径；p99 抖动来自候选全遍历）
工作量	0.5~1 天
依赖	eval.rs post_filter、engine.batch_get_fields / get_many_pk_in_fields、P85 collect_limited_rows 消费模式
风险	低（行序/offset/limit/墓碑可见性语义与现逐 docid 一致；仅候选获取方式分批）
具体工作：
□ post_filter/leaf_passes 批量改造：候选 bitmap 按 512 分块 → batch_get_fields（needed = 叶字段集合，P86②
  字节级提取）+ scan_row_matches 块内判定 → 命中入 out；看门狗逐块熔断；offset/limit 占位语义不变
□ 单测：批量后过滤 = 逐 docid 逐行一致（墓碑/缺失/转义护栏、0 命中全遍历、limit 早停）；AND(倒排×倒排) 快路径不回退
□ 验收：nocidx 配置 10w：ts= 无匹配 2322ms → ≤60ms（≈1e4 候选 ×6µs）；报告 24ms → ≤5ms 量级；回归全绿
> ✅ 已完成（2026-09-05，P113）：post_filter 改块级批量——顶层简单字段叶（无点路径）候选按 512/块
> `engine.get_many_pk_in_fields`（HotCache 直通/稠密区间流/稀疏 batch_get_fields 子集）+ 块内字节级判定；
> 嵌套点路径叶保持逐 docid leaf_passes。+单测 task029（跨块候选/删除隐藏/0 命中全遍历/LIKE 组合）。
> 全量 740 绿；数值收益随基准轮回填。
> 收益锚点：报告 #1/#2 高风险项中「组合索引点查 24ms、p99 148ms」在无声明配置下的实际成因。
> 数值验收回填（2026-09-05，10w 干净双端轮，SCC mean，cidx 声明配置套件）：composite_idx_point
> **0.31ms**、composite_idx_multi_eq **0.26ms**、composite_idx_range **0.81ms**（MySQL 40.8/66.6/40.3ms，
> 0.004~0.02×）——cidx 命中即索引点查，不再落到 nocidx 逐候选 post_filter；nocidx 兜底的块级批量
> 后过滤路径（本任务主体）由单测 task029 覆盖，套件在 cidx 声明配置下不可直接复现其数值。

Task-030：MySQL 语法面补齐——COUNT(DISTINCT col) 与 GROUP BY … ORDER BY <聚合表达式>（探针 SQL 报错根因）
> 实测 1064：`SELECT COUNT(DISTINCT status)` → 「聚合期望右括号，实际 Ident("status")」（parser 不支持
> DISTINCT 参数）；`SELECT region,COUNT(*) … GROUP BY region ORDER BY COUNT(*) DESC LIMIT 20` → 「意外
> token LParen」（ORDER BY 不允许聚合表达式）。MySQL 8.0 双侧同 SQL 可跑 → 该两探针仅 SCC 报错。
> 语义锚点：COUNT(DISTINCT) = 组去重计数；GROUP BY 后 ORDER BY 聚合 = 组结果按聚合值排序后 LIMIT。
属性	内容
优先级	P1（测量阻塞：2 个扩展探针无法跑，distinct 能力未知；语法面 MySQL 兼容缺口）
工作量	1 天
依赖	parser ast（agg 参数扩展 + order_by 项支持聚合头）、aggregate/group_by 执行（DistinctSet/HashSet 去重；
      位图/倒排字段可走 term 词典 distinct 快路径，高基数回退扫描）
风险	中（COUNT(DISTINCT) 语义：NULL 不计入（对齐 MySQL）、非数值列类型；ORDER BY 聚合需组结果物化排序，防
      超大组数内存——LIMIT 守卫）
具体工作：
□ 解析：COUNT/SUM 等聚合参数支持 `DISTINCT <field>`（ast 标记 distinct）；GROUP BY 查询的 ORDER BY 项允许
  聚合头引用（与 HAVING 同款聚合解析）；LIMIT 随组排序生效
□ 执行：COUNT(DISTINCT f) 无 GROUP BY = 单值列 HashSet 去重计数（位图/倒排声明字段走词典枚举快路径：
  inverted_group_stats/term 数；status 5 组毫秒级）；GROUP BY + ORDER BY COUNT(*) = 组行按聚合值排序切片
□ 单测：NULL 不计、浮点/字符串去重语义、混合 GROUP BY+ORDER BY 聚合+LIMIT、bitmap 字段快路径 = 扫描口径
□ 验收：两个探针 SQL（原样）在 SCC 端可跑且行集 = MySQL；10w 下 COUNT(DISTINCT status) ≤10ms、
   COUNT(DISTINCT user_id)（高基数回退扫描）≤ 全扫量级；回归全绿
> ✅ 已完成（2026-09-05，P115）：parser 聚合参数支持 `COUNT(DISTINCT f)`（Select.agg_distinct；
> GROUP BY 内 DISTINCT 解析期拒绝防静默）+ ORDER BY 项支持聚合列头规范串（`COUNT(*)`/`SUM(f)`）；
> execute_aggregate 新增去重计数分支（窗口扫描非 null 去重值：数值 f64 规范化、缺字段/NULL 不计、
> WHERE 过滤生效）；execute_group_by 排序支持聚合下标（数值比较、NULL 升序最前）且倒排快路径遇
> 聚合排序自动交主路径。+单测 task030。全量 740 绿。注：COUNT(DISTINCT) 走权威窗口扫描
> （status 等低基数字段 10w ~数十 ms，非倒排快路径；验收数值随基准轮回填）。
> 数值验收回填（2026-09-05，10w 干净双端轮，SCC mean）：count_distinct_enum **65.49ms**（走权威窗口
> 扫描口径，未达 ≤10ms——倒排/bitmap 去重快路径尚未接线，与 P115 注一致，作残余项记录）；高基数
> count_distinct_highcard **87.61ms** ≈ MySQL 72.70ms（1.2×，≤全扫量级 ✅）；COUNT(DISTINCT) 与
> GROUP BY…ORDER BY 聚合两探针正确性两侧行集一致（rows 等值）。
> **残余闭环（2026-09-05，P118）：** 低基数 COUNT(DISTINCT) **位图词典快路径已接线**——inverted
> `bitmap_field_snapshot` + engine `count_distinct_fast`（窗口∩活跃集判活，整值全删/复活精确）+ aggregate
> distinct 分支先行尝试，不可用回退权威扫描；+1 单测 task030b（= 带 WHERE 逼扫描等值：基础/整值全删/
> 复活/非白名单兜底）。全量 lib 回归 742 绿。10w #61 65.49ms → 目标 <1ms 量级（status 5 组枚举），
> 110 万同口径复测数值随基准轮回填（边界与明细见 problem_solving P118）。

Task-031：结果集行输出批量化（~25µs/行输出常数，10w 轮 ②⑤④ 行输出类探针共同瓶颈）
> 实测（scc-sqlrun-100k 副本）：引擎侧同窗 COUNT(20k 行) 94ms ≈ **5µs/行**；SELECT id keys-only 20k 行
> 606ms ≈ **30µs/行**；3 列 662ms、11 列 679ms —— 行输出成本 ~25µs/行且与列数/payload 几乎无关
> （每行固定，逐包协议/分配为主）。报告全部行返回探针（pk_between_10000 293ms / enum_sel_limit10000
> 337ms / IN 5000 174ms / 深分页 / 长只读 10 万行 3.2s）的每行常数均落此量级；同窗 COUNT 侧证引擎非瓶颈。
属性	内容
优先级	P1（系统性：所有行输出查询的固定行常数，10w→110w 线性放大）
工作量	0.5~1 天（demo 定策 0.5 + 接线 0.5）
依赖	server/protocol/response.rs（query_response_packets/write_query_response，逐包 write_packet）、nodelay 已开
风险	低（协议语义不变；仅合并写批/减少每行包系统调用与分配；先 demo 分离 server 端输出与 client 读包开销
      ——用同窗 COUNT/聚合侧证 server 引擎耗时占比后接线）
具体工作：
□ demo（src/demo/rowout-batch 或复用 rr-conformance --one）：server 端输出耗时 vs client 读包分摊分离
  （引擎同窗聚合计时对照 + SHOW 状态）
□ 接线：若 server 端主导 → write_query_response 合并大缓冲（一次 write_all 或多行包连写），减少逐包
  syscall/分配；SELECT 结果集流式分批发包（分批 limit 语义不变）防峰值内存
□ 单测：分批发送行集 = 全量逐包（列结构/行序/EOF/错误码）；回归全绿
□ 验收：20000 行 keys-only 606ms → ≤150ms（对齐引擎 ~5µs/行 + 小余量）；#pk_between_10000 293ms /
  enum_sel_limit10000 337ms / 长只读等行输出探针同比例受益；回归全绿
> ✅ 已完成（2026-09-05，P114）：server.rs 增 `frame_response`——多包响应合并单帧一次 `write_all`
> （同步 handle_connection 与异步 handle_connection_async 同接线；字节流与逐包 write_packet 完全一致，
> 包边界/seq 保留）；协议单元测试 task031（帧 = 逐包序列、seq 回绕、按长度前缀还原）。全量 740 绿；
> 行输出常数数值收益随基准轮（rr-conformance --one 大窗 keys-only 前后对照）回填。
> 收益锚点：报告风险 ②⑤④（IN 大批量、大 limit、长事务 3s+、深分页）的行输出部分。
> 数值验收回填（2026-09-05，10w 干净双端轮，SCC mean）：pk_between_10000（1 万行 3 列）**31.38ms**
> ≈3.1µs/行（旧 2 万行 keys-only 606ms ≈30µs/行 → 行常数 ~10× 下降，等值于验收 606ms→≤150ms 达成 ✅，
> MySQL 13.10ms，2.4×）；txn_long_read 10 万行窗 **332ms**（旧 3.2s → ~10×，MySQL 129ms，2.6×）；
> orderby_multi_limit500 258ms（排序/行式整窗 IO 残余，随 PAX/列 IO 远期）。

Task-032：主键 IN 稀疏大批次批量定位（pk_in_1000/5000 逐键点查残余）
> 根因：pk_in 随机 id 稀疏（跨度 ≫ 4×计数）→ get_many_pk_in_fields 稀疏分支逐 docid 点查（server/command/
> select.rs L237+），成本 = Σ点查常数（报告 ~31µs/键 warm；本副本冷态 ~50µs/键）+ 行输出常数（Task-031 共享）。
> 实测本副本 IN5 边际 ≈0.06ms/键。报告 5000 键 174ms。
属性	内容
优先级	P2（随 Task-031 共享收益后残余；业务已要求接口限 IN 数量，属吞吐优化）
工作量	1 天
依赖	P109 跨文件并行/窗口聚集读、Task-023 get_many_pk_in（稠密已走 scan_range）、Task-031 行输出
风险	中（稀疏大列表乱序 → 排序聚集后需保 IN 列顺序语义；可见性/墓碑/去重同 get）
具体工作：
□ 稀疏路径排序后**窗口聚集**：相邻键间隔 ≤ W（如 4096）归并为窗口 → scan_stream_fields 一次区间读 +
  集合过滤（对齐 P92 稠密判定的局部化扩展），替代逐键随机点查；分散键保持分块 batch_get
□ 行序保持 IN 列表序（现语义）；与 Task-031 输出批量化叠加评估
□ 单测：稀疏窗口聚集 = 逐键 get（墓碑/去重/offset）；混合聚集/分散边界
□ 验收：10w warm：pk_in_1000 31ms → ≤12ms；pk_in_5000 174ms → ≤60ms；pk_in_5/50 不回退；回归全绿
> ✅ 已完成（2026-09-05，P116）：`get_many_pk_in` / `get_many_pk_in_fields` 稠密判定 4×→64×
> （区间顺序读 ~0.5µs/行 vs 逐键点查 ~30µs/键：随机稀疏列表局部密度高，span≤64×n 即优于逐键；
> 超限自动回退 batch_get 不劣化），整行/投影两变体对齐。+单测 task032（4×~64× 窗口 =
> 逐条 get、删除隐藏、投影子集等值）。全量 740 绿；pk_in_1000/5000 数值验收随基准轮回填。
> 收益锚点：报告风险 ②（主键 IN 列表），Task-031 先行后残余再收敛。
> 数值验收回填（2026-09-05，10w 干净双端轮 results-sqlrun-compare-100k，SCC mean）：
> pk_in_1000 31ms → **4.66ms**（≤12ms ✅，MySQL 5.23ms，≈0.9×）；pk_in_5000 174ms → **51.78ms**
> （≤60ms ✅，MySQL 23.27ms，2.2×——行数多时残余行输出/投影常数仍偏高，但验收达标）；
> pk_in_50 **0.34ms**（0.7×，不回退 ✅）。

Task-033：锁等待超时语义对齐（innodb_lock_wait_timeout → 1205）或明示差异
> 实测/根因：txn_lock_wait / txn_lock_mid_contend 探针（run_lock_wait）**主会话 FOR UPDATE 持锁后 sleep 4s**
> 再 COMMIT —— 测量 4s 是探针设计的持锁时长（非引擎卡死）；副会话 UPDATE：MySQL（innodb_lock_wait_timeout=3）
> 第 3s 报 1205；SCC txn/lock.rs 为「等待即失败」模型（acquire 冲突即 TxnConflict 由调用方重试），
> 无超时参数 → waiter 阻塞至主会话提交后成功（无 1205 语义）。风险清单 #3「锁冲突 4s 卡死」系探针语义，
> 应改判为**语义一致性项**：长持锁期间 SCC 无超时上限与 MySQL 行为不一致。⚠️ 原推断已被 P117 实测
> 修正（SCC waiter 未阻塞、3ms 直接成功——FOR UPDATE 不持锁），见下方收口注。
属性	内容
优先级	P2（一致性/运维可预期性；并发 for update 长持锁的确定性）
工作量	0.5~1 天
依赖	server/command/transaction.rs 或 txn_dml FOR UPDATE 重试循环、txn/lock.rs
风险	中（1205 语义需重试次数×退避换算锁等待超时；RR 当前读重试路径不得误伤已持锁成功场景）
具体工作：
□ 事务级锁等待超时（读会话变量 innodb_lock_wait_timeout，默认 50s）：FOR UPDATE/写重试累计等待 > 阈值 →
  返回 1205（ER_LOCK_WAIT_TIMEOUT）；死锁环保持 1213
□ 探针 outcome 收敛：SCC 副会话应报 waiter-1205 与 MySQL 一致（探针已设 3s）
□ 单测：持锁 > 阈值 waiter 1205 / 阈值内成功 / 死锁 1213 不回退
□ 验收：txn_lock_wait 两库 outcome 一致（waiter-1205）；延迟行从风险清单改判；回归全绿
> ✅ 已收口（2026-09-05，P117，走「或明示差异」路径，不改引擎事务语义）：
> - 探针修正（rr-conformance sqlrun.rs `run_lock_wait`）：`SET SESSION innodb_lock_wait_timeout=3`
>   原 SET 在主连接，副（等待方）连接走 MySQL 默认 50s → 永不触发 1205（两侧都只是等主提交后拿到，
>   即原 10w 轮「4s 两侧一致」的成因）；改为在副连接生效，并把 outcome（waiter-ok/waiter-1205/
>   waiter-t=xxms）以 ⚑ 记入 stdout/summary.md（对比脚本视 ⚑ 为记录非失败）；另加 --only 探针过滤。
> - 实测（干净 100k 双端，主会话 FOR UPDATE 持锁 sleep 4s 后 COMMIT，副会话同 id UPDATE）：
>   MySQL 3316：**waiter-1205锁等待超时（waiter-t=3013ms**，3s 超时）；SCC 3317：**waiter-ok（先等锁后
>   拿到 waiter-t=3ms）**——SCC 副会话**未阻塞、3ms 直接成功**。
> - 根因修正：SCC `FOR UPDATE` 是乐观「当前读锁定集」（txn/mod.rs `cur_lock_seq` 记录读取时引擎最新
>   seq + engine/txn.rs 提交期写写冲突判定），事务期间**不真正持排他行锁**，并发 UPDATE 放行；
>   原「waiter 阻塞至主提交后成功」推断不成立（测量 4s 均为主会话固定 sleep，探针未测出阻塞）。
> - 明示差异结论：1205 收敛需以「行锁持有 + 等待/超时」为前提（FOR UPDATE 持久持锁 + 等待队列/累计
>   超时 + 死锁 1213 保持），属锁生命周期重构，风险高于本任务预期，列入远期深水区评估不在此盲改；
>   风险清单 #3「锁冲突 4s 卡死」改判 = 探针持锁 sleep 4s 设计 + 上述锁语义差异（真实差异域：
>   MySQL 副会话 1205 vs SCC 放行，业务如需 MySQL 式行锁等待需随远期重构）。
> - 收敛口径：txn_lock_wait/txn_lock_mid_contend 两侧 outcome 如实入 summary（MySQL waiter-1205 /
>   SCC waiter-ok@3ms），差异进已知边界；SCC 侧无引擎改动，回归全绿。

测量处置说明（2026-09-05，10w 轮报告逐项复核，不改码）：
- count_all 0.2ms ✅（Task-021 P96 O(1) 已生效；首调用基线扫描为 P107 已修 fresh-load 语义，此处为后次调用）；
- txn_lock_wait/txn_lock_mid_contend "4s" = 探针持锁 sleep 4s 设计（见 Task-033）；
- txn_long_read 3.2s ≈ 10 万行窗 × ~30µs 行输出常数（Task-031 收益对象），非事务机制开销；
- sum_where_enum（#13，n=3）与 sum_where_idx（扩展组，n=10）**同一条 SQL**（sql_sumwhere）先后跑，均值差
  系库状态漂移（中间大量写探针），非聚合算子缺陷；两者绝对值同为 O(active 候选)×取数，由 Task-029 批量改造收敛；
- update_in50 8.68ms / hotrow p99 6.77ms：批量 update 走 P88/P89 管道（已具备），残余为逐行 WAL/索引同步常数，
  归入远期 Per-CPU WAL 触发链（Task-026 后评估），不单独立项；
- group by / 多字段 order by / biz 列表 200ms 级：引擎全扫 ~5µs/行 + 候选取数 + 行输出常数（Task-031/029 受益），
  在线接口语义限制（仅后台）维持。

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
| Ex-9.4（事务 #25/35/36，2026-09-04 新基准报告追加） | **事务公平档位复测闭环 + 残余优化触发项** | **根因（已由 P2-A 定位，2026-09-04）：** cjserver 默认 `storage.flush_log_at_trx_commit=1`（逐 COMMIT 三路 fsync 强安全）→ 本轮 110 万 #25 3.5×/#35 3.1×/#36 2.8×、10 万 #25 2.1×/#35 3.2×/#36 2.5× 系**档位不对称**（MySQL 该实例 `innodb_flush_log_at_trx_commit=2`）；P2-A 档位 0/2（组提交窗口）代码已落地且 1.1M 探针实测 #25 8.4×→1.4×、#35 ~6×→1.05×、#36 ~6×→1.0×（基准记录 §14，对比报告 §四.3 已注明可复测）。**本轮行动：** ①公平复测闭环——`tmp/sqlrun/tmp-cfg-wide-2g.toml` 增 `storage.flush_log_at_trx_commit=2`，重启 3317 复跑 10 万/110 万 37 探针，#25/35/36 预期落 1.0-1.4×，实测值回填对比报告 §四.3（档位语义见基准记录 §13）；②**残余差分解**（档位 2 下 #25 若仍 ~1.4×≈0.57ms vs MySQL 0.40ms，~0.17ms 为单连接逐 COMMIT 固定开销，无法并发摊薄）：txn_locks Mutex 获取 ×2（commit 加锁 + release）+ RR 写冲突检测逐目标 `last_write_seq`（LSM 点读）+ active_snapshot 注册/注销 RwLock 写锁 + watchdog.check_all；**③触发式微优化候选**（公平档位复测后仍 >2× 才立项，预期单事务 ~0.1ms 级）：a. active_snapshots 改原子低水位替代 RwLock<BTreeSet> 全量写；b. 写冲突检测与 ops 应用合并同一次主数据访问（逐 docid 一次 get_many）；c. txn_locks 无并发持有者时 try_lock 快速路径跳过 Mutex 排队 | **复测闭环 ✅（2026-09-04，随 P85–P90 复测轮执行）：`tmp/sqlrun/tmp-cfg-wide-2g.toml` 已置 `flush_log_at_trx_commit=2` 并重启 3317 复跑 10 万/110 万 37 探针；实测 #25/35/36（110 万）p50 = 0.50/0.42/0.46ms（对齐档位 2 基线 §14 的 0.4–0.6ms），mean 1.5–2.1×（受写区首次 fsync 尾部 p99 1.9–16ms 抬高）——"并发 ≤2-3×"验收越过；10 万轮 1.1–1.3×。残余微优化 a/b/c 仍为触发式（mean 需压至 ~1.0× 时才立项）** |
| 事务微优化（触发项，见 Ex-9.4） | 仅当公平档位（flush_log_at_trx_commit=2）复测 #25/35/36 仍 >2× 时立项：①active_snapshots 原子水位；②冲突检测 + ops 应用合并；③txn_locks try_lock 快路径 | 触发式（暂不排期） |
| **P-GB（2026-09-05 追加）** | **窗口位图分组快路径（GROUP BY 5.7~7.2× 与 #60/#81 收敛）。根因（10w 轮 #14/#27/#59 = 210/262/252ms vs MySQL 29/46/39ms；#60 290ms；#81 308ms）**：server 聚合/分组一律传本表整窗（select.rs L107）→ `execute_group_by_window` scoped=true **禁用 `group_by_fast_inverted`**（仅 !scoped 启用）→ 全扫。**方案（复用 P118 基建）**：engine `group_by_bitmap_window(fields, cand_term, start, end)`——≤2 白名单字段值位图 AND ∩「窗口∩活跃∩(WHERE 单等值候选 posting)」逐组计数，返回 (组, 匹配数)，Σcounts 守卫（>匹配数 / 两字段 <匹配数 → 回退扫描保精确）；group_by.rs `group_by_fast_bitmap_window`（COUNT(*)/HAVING/组字段或聚合头排序/LIMIT；其余回退扫描）；基准 cfg `bitmap_fields` 补 `"channel"`（#60 region,channel 全白名单）。**10w 干净轮实测回填（P119）**：#14 210→**1.6ms**、#59 253→**1.3~1.7ms**、#27 262→**7.6~9.6ms**、#60 290→**10.8ms**、#81 308→**2.6~3.8ms**（MySQL 对照 29/39/46/112/27ms，全部反超）；单测扩展 ①~⑧ = 权威扫描等值（删除/复活/缺字段回退/HAVING/LIMIT/WHERE 候选+聚合排序）。边界同 P118（值变更陈旧、非白名单字段回退扫描） | ✅ 已完成（2026-09-05，P119，数值回填） |
| **P-GB2（2026-09-05 追加）** | **数值统计载荷窗口化（#15/#28 收敛，GROUP BY + SUM/AVG/MIN/MAX）。根因**：#15 271ms/#28 286ms——数值聚合逐行解码求和，P-GB 位图仅覆盖 COUNT。**方案（扩展 P-GB）**：聚合形态 = COUNT(*) 与单个 SUM/AVG/MIN/MAX(<stats_field>)（可只数值）；数值经 `engine.inverted_group_stats` term 载荷（FieldAgg n/sum/min/max）填组；**精确守卫 = 每组分组的载荷 n == 位图活跃计数**（删除/复活/换值/跨表/缺 amount → 回退权威扫描宁慢勿错）；限单字段分组；前置 `[inverted] stats_fields` 声明 + 写路径 add_stats（段 v5 载荷跨重启）。**10w 干净轮实测（P120）**：#15 271→**4.0ms**、#28 286→**3.8ms**（MySQL 162/181）。时序注：完整套件 #28 位于删除探针后 → 守卫回退扫描（精确兜底）；#15 在删除前全程快路径；只读/仪表盘聚合场景全程 ms 级。单测 pg_windowed_bitmap_group_stats_matches_scan ①~⑤ | ✅ 已完成（2026-09-05，P120，数值回填） |
| **UPDATE 10×+ 决策（2026-09-05 追加）** | **宽表 UPDATE 系（#17-19/73/75 = 8.7~15.3×）规避决策 + 触发式候选。实测与旁证**：INSERT 单行 0.5×、DELETE 0.1~0.5×、**txn 内 UPDATE #25/#35/#36 0.6~1.4×** → 引擎写能力非瓶颈；单语句 autocommit UPDATE ≈2ms/条（#17 1.99ms）——结构 = 读-改-写整文档 put_batch（P89：批尾单次落盘；单连接串行组提交窗口等满 ~2ms）+ 全量解码/倒排重索引。**决策（用户 2026-09-05）：宽表对比/使用场景规避 UPDATE，改用 INSERT 新 docid / DELETE+重建语义**；探针保留如实记录。触发式候选（UPDATE 成主流场景再立项）：①单字段 `SET f=v` 走 delta patch（免整行重写/重索引，与下方接线项 6 同源）；②单语句落盘等待对齐 INSERT 路径（组提交/异步 ack）。验收：update_id ≤2×（触发项）；不触发不排期 | 决策 ✅（2026-09-05，记录于本行） |

### 接线盘点（2026-09-05 全量盘点：能力 53 / 已接线 40（其中 4 仅 bin 工具）/ 未接线 13）
> 口径：对 src/engine、storage、inverted、txn、join、mv、bitmap、scale_out、redis、backup 的 pub 能力逐一在
> src/sql、src/server、src/cli、src/bin 定向 Grep 统计非测试调用（明细见 P118 会话记录）。13 项未接线中：
> **任务级 8 项（下表，逐项立项）**；**重复/内部 4 项不立项**（scan_stream_with_zonepred 与 scan_stream_parallel(zone_pred)
> 功能等价、fulltext_search 非分页与 paged 重复、cost_route 由 engine.execute() 内部自用、auto_watermark 引擎内部水位）；
> WriteBatch 类型上层直用 put_batch 亦可（不立项）；timing_wheel（Task-007）基建就绪**待挂载对象**（状态不变，不重开）。

| 项 | 内容 | 状态 |
|---|---|---|
| **M-1 物化视图接线（src/mv.rs）** | MaterializedView/MvScheduler 已实现但**全仓零生产引用**（仅 lib.rs 模块声明）——确认是否保留（接线到 SQL `CREATE MATERIALIZED VIEW`/HTTP 管理端点）或废弃归档，二者择一，避免死代码 | 待评估（接线 or 废弃） |
| **M-2 outbox 投递接线（Ex-1）** | engine enqueue/dispatch/pending/drained 仅测试覆盖，无上层投递入口（CDC/消息消费/扩容切换前置）——接 HTTP/gateway 事件面或归档 | 待评估 |
| **M-3 增量备份接线（M6-5）** | backup_incremental/restore_incremental 仅引擎测试；全量备份已接 CLI backup/restore——补齐 CLI 增量子命令 + 验收 | 待排 |
| **M-4 扩容 scale_out/reshard 管理入口** | ScaleOutCoordinator 仅自测引用；reshard 仅 gateway migration 中间层消费——补 admin/CLI 触发与状态面（生产 RPC+编排联动见 §三） | 待排（联动 10 亿验收） |
| **M-5 external_cache/sdk_cache/Redis 链路启用接线** | RedisClient(TTL SETEX)/SDK 缓存/外部缓存整链零生产引用——接到 server 配置启用（缓存降级/共享热层）或归档 | 待评估 |
| **M-6 SQL fulltext 检索接线** | fulltext_search_paged 仅 HTTP `/fulltext`；SQL parser 无 MATCH…AGAINST/全文谓词，LIKE 是子串扫、f=v 是整词倒排——补 SQL 全文检索形态 | 待评估（语法面扩展） |
| **M-7 SQL 类型化多字段 UPDATE（接线 engine.patch）** | engine.patch 支持多字段/JSON 类型化/null 删字段/Delta 合并，仅 HTTP/CLI patch 入口；SQL UPDATE 单字段且值一律字符串化——接线类型化 UPDATE = UPDATE 决策行候选① | 待评估（随 UPDATE 候选①） |
| **M-8 聚合/写定位快路径缺失调用点接线** | inverted_bitmap_and_count 无生产调用——挂优化器多条件计数候选（P4-C choose_best_plan 后选）/SQL 多条件 COUNT；Engine::execute(QuerySpec) 已有 HTTP/explain 但 MySQL 协议层无 spec 执行端点（低优先，HTTP 面已覆盖） | 待排（小项） |

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
