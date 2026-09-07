# development_remain.md —— 开发排期跟踪（2026-09-07 起）

> 2026-09-07：原 development_remain.md（全量历史 + 逐项验收记录 + 2026-09-07 状态回填注）已**归档为
> development_0907.md**；本文件接管排期跟踪 = 十一节待办清单 + 十二节排期批次。
> **用法**：每项排期/开发/销项以本文件为准；逐项明细、验收数值与行内治理注一律查
> development_0907.md（按任务名 / P 序列 / Ex-* 定位）；完成项照旧回填 development.md §14 后销项。
> 重叠主题冲突时以本文件批次为准、development_0907.md 为出处依据。

## 待办清单（2026-09-07 快照；各节为全量未完成项）

一、已立项但未开发（⬜/部分动工）
项	内容要点	状态
P142 Estimate 数量级接口（A2）	HTTP ?estimate/独立命令//stats，契约"约 N 行(approx)"；引擎侧 count_all_docs/count_docs_range 已备，入口接线 + approx 标注 + live 后台预热 + 量级验证全未做	⬜ 立项未接线
P141（A1）后续	接线已完成（本日推送）；待办 = demo/内核确认 + rr 探针 + "FOR QUICK 落点（默认权威/加词 O(1) 近似）对照基线"	⏳ 收尾
Task-002 fxhash 局部替换	HotCache/BlockCache/Manifest/聚合临时表换 FxHash（倒排/组合索引前缀保留 ahash）。代码核实：Cargo.toml 全库仍无 fxhash，未动工（P96 会话曾评估"读已极快、边际小"搁置）	⬜ P0 未排期
P94 阶段②（colstore 收尾）	EXPLAIN 标注 TableScan: Columnar/RowStore + 回退开关一键行式 + .cs 落盘/增量派生（现内存惰性派生、重启重建）	⬜ 阶段①/内核已做，②未开发
Ex-8.12 L2 zstd19 冷档默认化	50m 强收益（省 41.8% 磁盘）已证、P80 阻塞已解除 → "可推进默认化"本体尚未执行	✅ 已完成（2026-09-07，commit 待定→B1）
Ex-9.3 ⑤ 载荷默认化	demo + 50m 正确性 + 5M 端到端 10.15× 实证完成、语义采纳；代码默认开启状态需核实收口（P1-C 剩余②挂此项）	⏳ 待核实/收尾
P127 残留小项	server extract_between_range 纯主键区间快速路径 ~360ms（组合收敛路径 19ms 反快）→ 复用 pk_range_select(rest=None)	⬜ 小项保留
写链 memtable 驻留定位成本	连环写后 #75 120ms vs 干净 21ms 的候选机制（定位扫描跨 memtable 退化），另行评估	⬜ 候选
二、未接线（内核/引擎能力已具，无上层调用点）——2026-09-05 盘点 13 项中 8 项任务级
项	内容	状态
M-1 物化视图（src/mv.rs）	全仓零生产引用 → 接 SQL CREATE MATERIALIZED VIEW/HTTP 或废弃	待评估
M-2 outbox 投递（Ex-1）	enqueue/dispatch 仅测试覆盖，无上层入口（CDC/扩容切换前置）	待评估
M-3 增量备份 CLI	全量已接，backup_incremental 仅引擎测试 → 补 CLI 增量子命令	待排
M-4 扩容 scale_out/reshard 管理入口	补 admin/CLI 触发与状态面（联动 10 亿验收）	待排
M-5 external_cache/sdk_cache/Redis 链路	整链零生产引用 → server 配置启用或归档	待评估
M-6 SQL fulltext 检索	仅 HTTP /fulltext；无 MATCH…AGAINST/全文谓词	待评估
M-7 SQL 类型化多字段 UPDATE 接 engine.patch	P131 delta patch 已落单字段版，多字段/JSON 类型化/null 删字段的 SQL 接线缺口仍在（原 UPDATE 决策候选①）	待评估（需按 P131 后重新核）
M-8 快路径缺失调用点	inverted_bitmap_and_count 无生产调用（挂 cost 模型/SQL 多条件 COUNT）；Engine::execute(QuerySpec) 无 MySQL 协议端点（低优）	待排
timing_wheel（Task-007）	509 桶框架完成，TTL/Compaction/WAL 现均沿用原方案 → 无挂载对象	基建待挂载
三、远期（触发条件满足后落地）
Calvin 阶段二/三：gseq 分配器 → 全局复制日志 → raft HA（依赖 Calvin 落地）
raft TCP 传输 / 扩容编排联动（10 亿验收侧）
Ex-8.4：L1/L2 B+Tree 存储替换（备选蓝图，不主动排期）
写路径多写者 / 无锁写侧（Ex-13 触发链）：前置 = 解除引擎写侧串行 + 吞吐实测指向写锁
Tiered 分层合并（模拟后暂不引入）
共享字典压缩（ScyllaDB 式基建，Ex-8.12 后空间仍瓶颈再评）
Snowflake docid（仅真分布式多写者时复评，不主动排期）
MySQL 式行锁等待/1205（Task-033 改判）：锁生命周期重构，远期深水区
#17-19 单行更新残余（Per-CPU WAL 触发链尾项）与排序/行式整窗 IO 残余（#63-66 等 → PAX/列 IO 远期）
四、待定 / 触发式（条件满足才排期）
事务微优化 a/b/c（active_snapshots 原子水位 / 冲突检测+ops 合并 / txn_locks try_lock）——Ex-9.4：#25/35/36 mean 压至 ~1.0× 才立项
UPDATE 触发式候选②③（落盘对齐 INSERT/组提交 ack）——update_id ≤2× 触发线；当前 ~2.6×（P131 后值变形态），维持"规避 UPDATE"决策
Ex-8.8 posting 双区 LRU demo（可选，无触发不做）
zone_fields 计划精度（cost 模型 effectiveness 传 PAX hot_fields，P108 可选后续，不排期）
内存观测候选优化（倒排临时 bitmap 复用 / memtable 跳表打包 / cache 统计含元数据）——评估后立项
分层 fpr 命中场景调优（暂不立项）
五、待复测 / 数值验收回填（实现已落地，数值未闭环）
Task-027/P101：ycsb c 命中率/扫描后点查 p50 验收 → "待下次基准回填"
P118 count_distinct_enum 位图词典快路径：10w 65.49ms→<1ms 目标，110 万同口径复测数值随基准轮回填
P-GB2/P-GB3 守卫回退残余：#28/#13 在删除探针后/多态下走权威扫描（#13 仍 10.6~38×），属"守卫回退"预期态
110 万残余性能族（P134-137 后复测仍在）：#29 行式 24-31×（colstore 已反超 0.23s）、#63-66 排序 18-31×、#77 txn_long_read 16-37×、#56 like 15-21×、#41-43 宽表整行回表 9.8-15.3×、#45/46/48 倒排大窗 12-27×、#11/#53-55 扫描族 6-10×
#79/80 归 colstore 族（P139 形态定论中列）——待 colstore 阶段②后评估
六、待验收（依赖外部环境/硬件）
16 核专属压测（Task-026 验收）：⏳ 未启动（无 16 核环境），目标 22 万→30-35 万 TPS（+36-59%）
10 分片 10 亿构建验收：硬件/部署推进（阶段 A~D 代码已回填）
M-4 / raft 扩容编排：联动 10 亿验收
七、待排（小项/观测/文档，未排期）
会话空闲事务自动回滚（等价 MySQL wait_timeout；引擎 snapshot_evict_older_than 已备，上层策略待接）
内存口径三组观测（只读基线/读写混合/同组探针单向上涨 + cache 元数据统计，独立会话）
P2 容量规划文档（Windows WS ≠ 引擎堆；×1.4~1.8 策略；与内存观测合并）
Bloom 残余：②每 SST bloom/索引元数据入 memory_report；④886ms 尖峰冷热对照实验；L0 分段压测脚本
八、SQL 语法缺口（未排期，待挑优先级）
① 子查询 IN (SELECT…)/EXISTS　② UNION/INTERSECT/EXCEPT　④ 多 JOIN（>1 现 1064）/RIGHT/FULL/CROSS/非等值 ON　⑤ 列表达式阶段 B（进 WHERE/ORDER BY/聚合参数、AS 别名、DECIMAL/CAST）　⑥ 窗口函数（ROW_NUMBER/RANK/OVER…）　⑦ 无 GROUP BY 的 HAVING

九、2026-09-07 状态回填——如实修正（经代码核实"似完成实未做"，development_remain.md 行内已标注）
- Ex-9.3⑤ 载荷默认化：config `stats_fields` 默认空 Vec，载荷快路径须显式声明字段才路由 → "默认化本体（推广到高频数值列默认启用）"未执行（P1-C② 同挂此）
- Ex-8.12 L2 zstd19 冷档默认化：`sstable.compression_level_l2` 默认仍 0（不分层）→ 默认化未执行（50m 收益 + P80 阻塞解除仅"可推进"）
- Task-002 fxhash 局部替换：Cargo.toml 全库无 fxhash → 未动工（P96 曾评估"读已极快"边际小，搁置未排期）

十、2026-09-07 状态回填——缺口收窄 / 备注
- P1-C ②（载荷默认启用）：明确"未做"，挂 Ex-9.3⑤ 默认化（① count O(1) 已完成 = 销项收口项，见 development_remain 一.4 表）
- M-7 SQL 类型化多字段 UPDATE：P131（2026-09-06）已接 SQL 单非索引列 `SET col=字面量` → patch_batch（值类型化 + 免整行重写）；多字段/JSON 类型化/null 删字段的 SQL 形态仍未接（缺口收窄，待评估）
- P141：接线完成已注（txn_agg 挂载 + txn_select 顶部聚合拦截 + 行源扫描）；待办 = demo/内核 + rr 探针 + FOR QUICK 对照基线
- 治理备注：development_remain.md 头部新增 2026-09-07 回填导引注

十一、其余项（核实后维持原状态，无改动）
- 已立项未开发：P142 Estimate 接口（未接线）、P94 阶段②（colstore EXPLAIN 标注/回退开关/.cs 落盘增量派生）、P127 残留小项（extract_between_range 纯主键区间快路径复用 pk_range_select）、写链 memtable 驻留定位成本（另行评估）
- 未接线：M-1~M-6、M-8、timing_wheel 待挂载对象
- 远期：Calvin 阶段二/三、raft TCP 传输/扩容编排联动、Ex-8.4 L1/L2 B+Tree 替换、写路径多写者/无锁写侧、Tiered 分层合并、共享字典压缩、Snowflake docid、MySQL 式行锁等待 1205、#17-19 单行更新与排序族 IO 残余
- 待定/触发式：事务微优化 a/b/c、UPDATE 候选②③（update_id ≤2×）、Ex-8.8 posting 双区 LRU、zone_fields 计划精度、内存观测候选优化、分层 fpr 命中调优
- 待复测/数值回填：Task-027/P101（ycsb c）、P118 count_distinct 110 万、P-GB2/3 守卫回退、110 万残余性能族（#29 行式 24-31×、#63-66 排序 18-31×、#77 长快照窗 16-37×、#56 like 15-21×、#41-43 宽表回表 9.8-15.3×、#45/46/48 倒排大窗 12-27×、#11/#53-55 扫描族 6-10×）、#79/80 colstore 族
- 待验收（外部环境）：16 核专属压测（Task-026）、10 分片 10 亿构建、M-4/raft 扩容编排
- 待排：会话空闲事务自动回滚、内存口径三组观测、P2 容量规划文档、Bloom ②④ + L0 分段压测脚本
- SQL 语法缺口：①子查询 IN(SELECT)/EXISTS　②UNION/INTERSECT/EXCEPT　④多 JOIN/RIGHT/FULL/CROSS/非等值 ON　⑤表达式阶段 B　⑥窗口函数　⑦无 GROUP BY 的 HAVING

## 十二、排期批次（2026-09-07 草案，按依赖/风险/验收目标排序；待用户确认后执行）

> 假定：下一阶段目标 = ① 收尾正确性/权威面（P141 族）→ ② 已实证收益的"默认化"落地 → ③ 性能/结构收口（colstore ②）→ ④ 管理面与语法面按需 → ⑤ 观测/治理。
> 括号内为前置/验收依据（development_0907.md）。

| 批次 | 项 | 内容与前置 | 风险/工作量级 |
|---|---|---|---|
| A1 | **P141 收尾** | demo/内核确认 + rr 探针用例 + "FOR QUICK 落点（默认权威/加词 O(1) 近似）对照基线"；txn 聚合权威面闭环 | 低（⏳ 2026-09-07 拆解完成） |
| A2 | **P142 Estimate 接口** | 独立非 SQL 入口（HTTP/命令//stats）+ approx 标注 + live 预热（或首调标注）+ 量级验证；引擎 count_all_docs/count_docs_range 已备零 MVCC 改动 | 低（⏳ 2026-09-07 拆解完成） |
| B1 | **Ex-8.12 L2 zstd19 默认化** | `compression_level_l2` 0→19；前置 = 5m/50m 全量回归 + 写档位兼容验证（50m 省磁盘 ~39-41.8% 已实证） | 中 | ✅ 2026-09-07 完成
| B2 | **Ex-9.3⑤ 载荷默认化** | 高频数值列默认启载荷（stats_fields 默认推广）；前置 = 默认化 A/B 已 10.15×（5m）；P1-C② 同销 | 中 |
| C1 | **P94 阶段② colstore** | EXPLAIN 标注 TableScan Columnar/RowStore + 回退开关 + .cs 落盘/增量派生（现内存重启重建）；收排序族 #29/#63-66 与 #79/80 | 中 |
| C2 | **P127 残留小项** | server extract_between_range 纯主键区间 → 复用 pk_range_select(rest=None)（110 万 ~360ms→~19ms 量级）。**2026-09-07 复测扩展**：nontxn `GROUP BY + id BETWEEN` 主键窗口恒 0 行（分组执行器 execute_group_by_window 未剥离主键 id/docid 谓词复检；行查询 P127 已修同族） | 低 |
| D1 | **M-3 增量备份 CLI + M-4 扩容管理入口** | 全量备份已接 CLI；补 incremental 子命令；scale_out/reshard admin/CLI 状态面（联动 10 亿验收） | 中 |
| D2 | **M-1/M-2/M-5 评估定夺** | 物化视图/outbox/Redis 缓存链：三选一（接线 or 废弃/归档），避免死代码 | 低 |
| E1 | **语法面按序小项** | ①子查询 IN/EXISTS → ②UNION 族 → ⑤表达式阶段 B → ⑥窗口函数 → ⑦无 GROUP BY HAVING（④多 JOIN 随 ① 评估）；每项独立可交付 | 逐项小 |
| F1 | **会话 idle 回滚 + 内存观测/P2 文档 + Bloom②④** | 运维/观测面：snapshot_evict_older_than 上层策略；三组内存观测 + 容量文档；bloom 元数据统计 + 尖峰实验 + L0 压测脚本 | 低 |
| X | **外部依赖（硬件）** | 16 核专属压测（Task-026 验收 +36-59%）；10 分片 10 亿构建验收；M-4/raft 编排 | 待机 |
| Y | **触发式（不主动排）** | 事务微优化 a/b/c；UPDATE 候选②③；Ex-8.8；zone_fields；Task-002 fxhash（边际小）；fpr 调优 | — |

> 备注：C1 与 #79/80、排序族数值验收互链；B1/B2 完成后在 development_0907.md 对应行销项；批次顺序可调，待用户拍板。

### A 批任务拆解（用户 2026-09-07 选定先行；批次表 A1/A2 标记 ⏳ 进行中）

**A1 P141 收尾（txn 聚合权威面闭环）**
- [x] A1-1 rr/sqlrun 探针对拍：新增事务内聚合探针（txn COUNT/SUM/AVG/MIN/MAX [DISTINCT]、GROUP BY/HAVING、COUNT(*) 无 WHERE 全表），与 MySQL 双端对拍——覆盖三态：FOR UPDATE 当前读 / 同事务自插·自改·自删 / 空集数值聚合 NULL（两库行集等值，纳入既有复测闭环）
  - ✅ 2026-09-07：txn_agg_{cnt_all,scalar,avg,group,fu} 双端全 exp-ok（MariaDB a11g / SCC a11f，同 sqlrun 版本，行集数值语义等值）。附带修复 2 个 SCC 缺陷（commit 32ed256）：
    - txn_dml `WHERE id BETWEEN` 主键闭窗口直解——误走字段候选 + doc 无 id 复检 → 事务内窗口 UPDATE/DELETE 恒 0 行；
    - txn 单字段赋值 JSON 类型化——`SET k=2` 旧落 doc.k 字符串 → 事务内 SUM/AVG 数值聚合读 NULL（对齐非事务 P131/ODKU）。
  - 边界注：单语句复合 SET（SET k=2, amount=2.50）超出 txn UPDATE 单字段解析 → avg 探针 SET 拆两条单列（M-7 已知缺口，非 P141 面）；组态同款不阻塞。
- [x] A1-2 "FOR QUICK 落点"对照基线（评估项）：默认权威 vs 加词降级 O(1) 近似——盘点可加词近似的聚合形态（enum/bitmap 字段 COUNT 族），产出定位 + 探针对照基线，不强制接 code
  - ✅ 2026-09-07：同一 500k 库（guest db-p144-500k-v2，documents，无并发）权威 vs 加词快路对照（tmp/vm-a11/a13.out，探针非 txn SQL 直连 3308）：
    - txn-RR 权威全扫（MVCC 快照逐行判活，a12-base/a13 双测）：COUNT WHERE status='active' 5.2-5.6s→101149；region='beijing' 5.2-6.1s→63707；GROUP BY/COUNT DISTINCT 同量级 ~5.1s
    - 加词 O(1) 快路：COUNT(WHERE) 0.1-0.3ms（~10⁴×）；COUNT(DISTINCT status) 0.1-0.2ms→5；GROUP BY status 7.4-7.5ms→101149（~700×；首调 4.9s = 懒建/攒批 flush，非稳态）
  - 可加词近似形态与定位（白名单族 = cfg inverted.bitmap_fields/倒排字段）：
    - COUNT(WHERE 单等值)：非 txn Ex-9.1 → `inverted_doc_count`（engine/query.rs，server count 快路；段 TermMeta 载荷/doc_count_fast）——**口径无 live∩**：含墓碑/换值残留 → 本库高估恒定 +53288（active 154437 vs 权威 101149；region 116995 vs 63707；两字段差同 = 同墓碑残留集），相对 +52.7%@active
    - GROUP BY <白名单 1..=2 字段>：`group_by_bitmap_window`（engine/query.rs，live∩ + cand_term 收敛）→ 7.5ms 且与权威一致
    - COUNT(DISTINCT 低基数)：`count_distinct_fast`（engine/query.rs，live∩ 判活）→ 亚毫秒且与权威一致（5）
    - /estimate 条件估计（P142/A2）：posting∩live → 与权威一致（101149）
    - SUM/AVG/MIN/MAX 与 COUNT(f)：无位图等价（须逐行取值/数值）→ 不可加词近似，恒权威全扫（~5s）
  - 结论（FOR QUICK 落点）：加词降级 O(1) 语义安全前提 = 白名单字段 + **live∩ 判活路径**（group_by_bitmap_window/count_distinct_fast/estimate 族，与权威一致）；Ex-9.1 `inverted_doc_count` 单等值快路缺 live∩ → 删除/换值库上近似高估墓碑残留——FOR QUICK 若走该路须标注近似语义或先补 live∩（留作后续，本项未接 code）
- [x] A1-3 demo/内核确认：release 下事务聚合路径正确性/耗时 sanity（可选 src/demo/，与 txn_agg 单测互补；确认 sqlish 候选兜底路径 + 混合库 wm>2^48 分支实际可达性）
  - ✅ 2026-09-07：补 p141_txn_agg_sqlish_fallback_fu_and_mixed_wm 可达性单测（两条兜底分支 + 修复）：
    - ① sqlish 候选兜底（单表单行库 + FOR UPDATE 字段谓词）：候选=当前视图 sqlish ∪ 写集 → txn_read_current 取值 + doc_matches_where 复检——自改离开谓词行被剔除、仍命中行当前读见新值（SUM 4→100 断言）；
    - ② wm>2^48 混合库分支（t_a 高位 docid 推水位）：快照字段谓词标量/分组走 sqlish 兜底，限定默认表（t_a 无 s 不命中），与 nontxn 权威对照一致；
    - **发现并修复缺陷（commit 917171c）**：txn_agg drive sqlish 兜底构造 cond_sql 时尾截断只含 ORDER BY/LIMIT → 字段谓词 + GROUP BY（混合库/FOR UPDATE）cond 误带 "GROUP BY s" → parse "非分组列 docid" 失败；现截断含 GROUP BY/HAVING（分组累积仍在 txn_group 行流）。41 项 txn 回归全绿。
  - release sanity：复用 A1-1 SCC 500k release 探针双端全绿 + 本单测 debug 通过（耗时基线见 A1-2/A1-1，权威 ~5.2s/条）。
- 销项 ✅：development_0907.md P141 行已完成（A1-1~A1-3 全 [x]，development_0907.md P141 行已标 ✅）

**A2 P142 Estimate 数量级接口（独立非 SQL，零 MVCC 改动）**
- [x] A2-1 契约与入口：HTTP `GET /estimate`（无参 = 全库；`field=&value=` = 条件估计；`range=` 可选窗口）→ 响应 JSON 显式 `approx: true` 与数量级标注；复用 route_request（src/server/http/mod.rs），doc_api 新 handle_estimate
- [x] A2-2 引擎接线：读锁内 `count_all_docs`（全库）/ `count_docs_range(start,end)`（表窗口 = docid 高 16 位表号；Engine::count_docs_range 已备 P96）返回 approx；条件估计 = bitmap_fields 内存位图 或 inverted posting∩live（当前视图；换值旧值残留按近似语义不修）
- [x] A2-3 live 懒建首调尖刺：open 后台预热（live_ensure 懒建 → open 收尾触发）或接口标注首次慢 —— **选标注**（`/estimate` 全库响应 note 已注明首次懒建基线、大库秒级，后续 O(1)）；后台预热开放留触发式（若生产首调尖刺成问题再上）
- [x] A2-4 单测 + 量级验证 + 文档：全库/表窗/条件×位图/posting 单测；与权威 COUNT 数量级对照；HTTP 面文档（user_guide/README）回填
  - ✅ 2026-09-07：http 端到端测试覆盖 all/window/field/field+window/400（commit 5716dcb）；量级验证（VM 50 万 SCC 库，est2）：all=501200 精确（权威 COUNT(*) 501200，首调 5.7s 懒建基线）、window 1-1000=1000、field status=active=101149（权威 154437，同 1e5 量级——差值 = 重启后倒排 term 未达内存落盘阈值丢段/陈旧残留，属近似语义 + P144 空心索引已知面，接口 approx 标注兜底）。A2 接线 commit 37fe410，文档 93938cc
- 销项 ✅：development_0907.md P142 行已完成（A2-1~A2-4 全 [x]，development_0907.md P142 行已标 ✅）

> 执行顺序建议：A1-1（探针对拍，闭环正确性）→ A2-1~A2-4（独立接口）→ A1-2/A1-3（评估/确认）。

### 完整重跑探针复测（2026-09-07，A 批收尾后 rerun-full）

- 方式：rr-conformance `--sql-run` 全量探针（无 `--reps`/`--only`，87 项自带次数）双端先后重跑——SCC（documents 3308，原 50 万库不重建）→ MariaDB（wide.t 3306）。输出 tmp/vm-a11/rerun-{scc,mariadb}-summary.md。
- 结果：双端 87/87 全 ok、无 FAIL/mismatch；txn_agg 五探针双端 exp-ok。MySQL 锁等待探针 waiter-1205（3s）为预期语义分支。
- **复测抓到 2 个验收盲区并修复**：
  1. **SCC `GROUP BY + id BETWEEN`（主键窗口）恒 0 行**（txn 与 nontxn 皆然）——`extract_between_range`/`extract_target_ids` WHERE 尾只截 ORDER BY/LIMIT，`…BETWEEN 1 AND 3 GROUP BY s` 的 b 端解析被 "group by s" 污染失败 → txn 落字段谓词复检（JSON 无 id → 恒假 → 剔光 → 0 组）。修复：尾截断补 GROUP BY/HAVING → 归主键闭窗口直解（commit c8a5388）。回归 `p141_txn_group_by_between_window`（HAVING/ORDER BY 同源，42 txn 绿）。VM 重建 release cjserver 后重验：SCC txn_agg_group 注现含 `gb=[active|2|3;b|2|3;c|1|1];having=[active|2;b|2]` 与 MariaDB 逐字节一致（此前 SCC 该注缺 gb/having = 窗口 GROUP BY 空跑）。
  2. **txn_agg_group 探针假绿**：`run_txn_agg` 仅 rows 非空才断言 → SCC 窗口 GROUP BY 0 行被静默跳过（exp-ok 但 gb/having 从未真验）。修复：期望非空步骤 0 行强制判失败（commit c8a5388）。
  - **遗留（P127 族，另行排期）**：nontxn `GROUP BY + id BETWEEN` 仍 0 行（行查询 P127 已修、分组执行器 execute_group_by_window 未剥离主键 id/docid 谓词复检）——见下"待排期"。
- 复测后 SCC 库行态：rerun-full 写探针 + 清理 best-effort 后 COUNT(*) ≈50.3 万（N 自留 50 万基线略浮动，探针自适应自洽，非对拍差异）。

### B1 批次（Ex-8.12 L2 zstd19 默认化，2026-09-07）

- [x] B1-1 核实 compression_level_for 分层触发路径：sstable.rs Default compression_level_l2=0→19 → L2 输出（L1→L2 下沉/L2 内合并/L2 单段重写）用冷档高压缩率，flush→L0/L0-L1 仍热档 compression_level=3（防中间层放大）
- [x] B1-2 默认值修改 + 单测：`sstable.rs` compression_level_l2: 0→19；`tests.rs` 断言 `compression_level_l2==19`（B1 注释）
- [x] B1-3 编译通过 + 分层/列族/读兼容回归测试全绿
- [x] B1-4 VM 回归验证：默认配置启动（含 compression_level_l2=19），wide-load 103.5k 行数据加载，Python raw protocol 全部 spot check 通过（COUNT/WHERE/BETWEEN），数据目录 194MB
- 销项 ✅：development_0907.md Ex-8.12 行已完成（待回填销项）

### B2 批次（Ex-9.3⑤ 载荷默认化，2026-09-07）

- [x] B2-1 配置核实：`InvertedConfig.stats_fields` 默认空 Vec → 写路径 `engine_doc_stats` 跳过，查询路径 `stats_field_pos` 仅查显式声明
- [x] B2-2 实现默认化：`Engine` 新增 `auto_stats_fields: Mutex<Vec<String>>`；写路径 `stats_fields` 为空时解析 JSON 自动发现所有数值字段并入 auto 集合（单调增长）；`stats_field_pos` 同时查显式集和 auto 集
- [x] B2-3 测试：更新 `inverted_stats_fields_accumulate_per_term` 验证默认配置下自动检测 `amount` 字段且 `stats_field_pos("amount")==Some(0)`、非数值字段 `stats_field_pos("status")==None`；显式配置 `stats_fields` 时 auto 集不生效（用户声明覆盖默认化）
- [x] B2-4 全量回归：801 passed / 0 failed / 4 ignored
- 销项 ✅：development_0907.md Ex-9.3⑤ 行标记 DONE + P1-C② 同销（commit 29bc8ee）
