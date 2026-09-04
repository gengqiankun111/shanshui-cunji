# 宽表 SQL 性能基准记录（MySQL 2G，2026-09-04）

> 范围按用户指示收口：**MySQL 侧测试完整执行并记录**；cjserver 侧对比因内核多表改造
> 兼容问题**未能执行**，受阻详情见「§6 cjserver 侧记录（未完成，供修复会话）」。

---

## §1 结论摘要

- 目标：在「100 万多字段宽表」上分别跑 MySQL 与 cjserver（各 2G 内存预算）同一套 ≥20 种
  SQL 负载（含 group by / order by / 范围查询 / 批量查询 / 批量插入（不同批）/ 批量
  更新 / 批量删除 / 事务），对比平均耗时、p95、p99、最大耗时。
- **MySQL 侧已完整跑完**：26/26 探针通过，初始行数 **N=1,098,342**（25 列宽表 `wide.t`），
  结果归档于 `results-sqlrun-mysql-2g/summary.md`，全表见 §5。
- **cjserver 侧未完成**：现运行实例为另一任务编译中的新内核（§26 真多表 M1~M3 +
  `src/mysql.rs` 未提交改动）；旧格式宽表数据在新内核下 `COUNT(*) FROM t` 返回 0
  （多表命名空间导致旧数据不可见）；按指示重建数据集时触发内核 panic
  `src/inverted.rs:490 docid 超出 RoaringBitmap 支持范围` 及毒锁
  `src/mysql.rs:825 unwrap PoisonError`。该问题属内核修复范围（另一工作任务），
  未在本会话修复（用户指示只跑测试）。
- 收尾状态：独立 MySQL 实例 3316 已停止（数据与配置保留在 D 盘，可随时复用）；
  3317 已停止且数据目录已清空为初始空库；3306（系统 MySQL）与 3309/3310（RR 测试
  cjserver 实例）保持运行。

## §2 测试环境

| 项 | 值 |
|---|---|
| 机器 | Windows，RAM 15.8 GB（空闲 ~11.6 GB），C 盘余 1.5 GB，D 盘余 34 GB |
| MySQL 被测端 | **独立实例 3316**（MySQL 8.0.45 Community，D 盘 datadir，专用 my.ini，`innodb_buffer_pool_size=2G`、4 实例），配置 `tmp/my-wide-3316.ini` |
| cjserver 被测端 | 3317（`tmp/tmp-cfg-wide-2g.toml`：hotcache 1024 + blockcache 512 + inverted 256 + memtable 256 MB ≈ 2G）——**未测成**，见 §6 |
| 数据 | 宽表 `t`：25 列（id + 9 数值/枚举 k,amount,score,ts,user_id,age,active_days,visit_count,balance,flag + 枚举 status/region/channel/tag + 长字符串 note/title/url/email/phone/ip/desc_a/desc_b/txt_a/txt_b），单行约 1 KB |
| 行数 | 装载 **1,098,342 行**（id 1..1,098,342），MySQL 与（计划中的）cjserver 相同参数、相同确定性种子 |
| 工具 | `rr-conformance`（rust mysql crate 客户端，仅依赖 mysql/rand/anyhow，**不编译 cjserver**）扩展子命令 `--sql-run` / `--wide-load`；exe 位于 `D:\shanshui-cunji-target\release\rr-conformance.exe` |

> 为什么新起 3316 实例：系统 MySQL 3306 的 datadir 在 C 盘（仅余 1.5 GB，装不下 ~1.3 GB
> 宽表数据），且其 `innodb_buffer_pool_size` 仅 128M、上面跑着其它业务库 → 按用户确认
> 新起独立实例，不碰 3306。

## §3 数据装载（MySQL 3316）

- DDL 同 `tmp/tmp_wide_load.py`（`CREATE DATABASE wide; CREATE TABLE wide.t ... 5 索引`）。
- 装载：`python tmp/tmp_wide_load.py --mode load --port 3316 --user root --rows 1098342 --procs 4`
  用时 **185.7 s**（4 进程，每进程约 1480 rows/s，500 行/批）。
- 行生成确定性：每进程 `random.Random(42 + w*7919)`、`id = start(1) + w`、步进 `procs`；
  装载结束 `SELECT COUNT(*) = 1,098,342`。

## §4 探针集（26 项，两侧方言统一）

扩展自旧版 sqlrun（22 项），改动要点：

- **新增**（覆盖用户要求的批量类）：`pk_in_50`（批量查询）、`insert_batch10`（10 行/语句）、
  `insert_batch100`（100 行/语句）、`update_in50`（批量更新）、`delete_range50`（批量删除）；
- **修正**：`group_by_sum_having` 去掉列别名（sqlish 不支持 `HAVING c>0`，改为
  `HAVING COUNT(*) > 0`），保证 MySQL/cjserver 同一 SQL；
- **移除** cjserver 特有 `SELECT id,doc`（MySQL `wide.t` 无 doc 列）；
- 写探针只作用于 `id > N+200000` 预留区（upd 200 行保留 / del 100 / delb 500 / ins 1500），
  结束后整区清理，不污染主数据。

26 项分组：#1–4 点查（含批量查询 IN50）· #5 范围 · #6–9 倒排/枚举 · #10–11 扫描 ·
#12–15 聚合（含 GROUP BY / HAVING）· #16 排序 · #17–24 写（单/批增删改）· #25–26 事务。

## §5 MySQL 3316 实测结果（N=1,098,342，2G buffer pool）

结果文件：`results-sqlrun-mysql-2g/summary.md`（运行约 2–3 分钟；探测 n 次每次计
mean/p50/p99/max，单位 ms）。

| # | 类别 | 探针 | 说明 | OK/n | 行/影响 | mean | p50 | p99 | max |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 点查 | pk_point_star | SELECT * | 300/300 | 1 | 0.20 | 0.18 | 0.36 | 0.41 |
| 2 | 点查 | pk_point_proj10 | 10 列投影 | 300/300 | 1 | 0.18 | 0.16 | 0.30 | 0.31 |
| 3 | 点查 | pk_in_5 | id IN 5 点 | 150/150 | 5 | 0.27 | 0.19 | 0.40 | 8.85 |
| 4 | 点查 | pk_in_50 | id IN 50 点（批量查询） | 30/30 | 50 | 0.50 | 0.51 | 0.70 | 0.70 |
| 5 | 范围 | pk_between_100 | 100 行窗口 | 150/150 | 101 | 0.29 | 0.27 | 0.42 | 0.42 |
| 6 | 倒排 | enum_sel_limit100 | 枚举等值 bitmap | 60/60 | 100 | 0.55 | 0.52 | 0.74 | 0.74 |
| 7 | 倒排 | enum_count | COUNT 倒排载荷 | 60/60 | 1 | 63.01 | 61.70 | 91.02 | 91.02 |
| 8 | 倒排 | combo_and | 枚举×枚举 AND | 60/60 | 100 | 0.57 | 0.54 | 0.92 | 0.92 |
| 9 | 倒排 | field_in | 字段 IN 列表 | 60/60 | 100 | 0.67 | 0.64 | 1.02 | 1.02 |
| 10 | 扫描 | cmp_gt_limit50 | 数值> LIMIT 早停 | 20/20 | 50 | 1.88 | 1.86 | 2.32 | 2.32 |
| 11 | 扫描 | cmp_between | 数值 BETWEEN（全扫） | 20/20 | 68 | 500.41 | 508.45 | 584.89 | 584.89 |
| 12 | 聚合 | count_all | 无条件 COUNT | 5/5 | 1 | 174.96 | 174.94 | 183.78 | 183.78 |
| 13 | 聚合 | sum_where_enum | SUM WHERE（全扫） | 3/3 | 1 | 515.75 | 507.66 | 646.02 | 646.02 |
| 14 | 聚合 | group_by_status | 全扫分组 | 3/3 | 5 | 327.99 | 312.88 | 361.31 | 361.31 |
| 15 | 聚合 | group_by_sum_having | 多聚合+HAVING(函数式) | 3/3 | 5 | 2158.51 | 2095.93 | 2299.31 | 2299.31 |
| 16 | 排序 | orderby_win_1000 | 窗口 1000 ORDER BY | 20/20 | 20 | 0.91 | 0.86 | 1.58 | 1.58 |
| 17 | 写 | update_id | UPDATE id= | 100/100 | 1 | 0.28 | 0.21 | 4.24 | 4.24 |
| 18 | 写 | update_in2 | UPDATE id IN 2 | 50/50 | 0 | 0.26 | 0.24 | 0.49 | 0.49 |
| 19 | 写 | update_in50 | UPDATE id IN 50（批量更新） | 20/20 | 0 | 0.74 | 0.63 | 2.65 | 2.65 |
| 20 | 写 | insert_single | INSERT 单行 | 100/100 | 1 | 0.24 | 0.21 | 0.85 | 0.85 |
| 21 | 写 | insert_batch10 | INSERT 10 行/语句 | 30/30 | 10 | 1.77 | 0.73 | 24.71 | 24.71 |
| 22 | 写 | insert_batch100 | INSERT 100 行/语句 | 10/10 | 100 | 3.77 | 3.55 | 6.24 | 6.24 |
| 23 | 写 | delete_id | DELETE id= | 100/100 | 0 | 0.25 | 0.24 | 0.50 | 0.50 |
| 24 | 写 | delete_range50 | DELETE 50 行区间（批量删除） | 10/10 | 50 | 1.21 | 1.16 | 2.07 | 2.07 |
| 25 | 事务 | txn_begin_upd_commit | BEGIN→UPDATE→COMMIT | 100/100 | 0 | 0.51 | 0.50 | 2.69 | 2.69 |
| 26 | 事务 | txn_for_update_read | BEGIN→FOR UPDATE→COMMIT | 50/50 | 0 | 0.41 | 0.39 | 0.56 | 0.56 |

### 要点解读（MySQL）

- 点查/主键类（#1–5）：0.18–0.50 ms；`IN 50` 批量查询 ~0.5 ms（≈50 次点查的索引查找开销合批）；
- 枚举/倒排等值（#6,8,9，命中 idx_status / idx_status_region 二级索引）：0.55–0.67 ms；
- **无索引字段全扫**（amount，表无 amount 索引，模拟 cjserver 无对应倒排）：`BETWEEN` #11 500 ms、
  `SUM WHERE` #13 516 ms、GROUP BY #14 328 ms、COUNT #12 175 ms——符合二级索引读 1.1M 行量级；
- `enum_count` #7 63 ms：二级索引覆盖计数（idx_status 扫描统计 ~22 万行/值）；
- 写：单行 UPDATE/DELETE 0.25–0.28 ms；**批量写提升明显**——INSERT 100 行/语句 3.8 ms
  ≈ 单行 0.24 ms×100 的 ~1/6；UPDATE IN 50 0.74 ms；DELETE 50 行区间 1.21 ms；
- 事务块（autocommit 关 + BEGIN/COMMIT）：0.4–0.5 ms 级。

## §6 cjserver 侧记录（未完成，供修复会话）

### 6.1 背景

- 3317 宽表实例原本（旧内核，二进制 2026-09-03 20:35）可正常读旧格式 1.09M 行数据，
  旧 22 探针已跑出过一轮结果（`results-sqlrun-wide2g/summary.md`，N=1,098,242）。
- 另一工作任务已提交 §26 真多表（M1 表路由 03f0a80 → M3 按表切分 19745a6，HEAD
  `19745a6`）+ §27 docid 相关提交；`src/mysql.rs` 仍有**未提交改动**（时间 2026-09-04
  00:43，晚于本会话编译时刻）。本会话按其指示在其停止后自行编译：
  `cargo build --release --bin shanshui-cunji-mysql-server --target-dir D:\shanshui-cunji-target`
  （2026-09-04 00:41 产出，4.5 MB）。

### 6.2 症状 1：旧数据在新内核不可见

- 新内核重启 3317（同参数 `--data-dir D:/shanshui-data/db-wide-scc --config tmp\tmp-cfg-wide-2g.toml
  --bind 127.0.0.1:3317 --watchdog-secs 3600`）后，WAL 回放/2 SST 正常加载（seq 1,184,995），
  但 `SELECT COUNT(*) FROM t` 返回 **0**（探针工具直接判定表空）。
- 推断：§26 多表后 t 按“真实表”路由到独立命名空间，旧单命名空间数据不再落入 t；
  需在修复侧确认旧数据归属（documents/默认库）或迁移方案。

### 6.3 症状 2：重建数据集触发内核 panic

- 按用户指示“重新构建 scc 的数据集”：清空 `D:\shanshui-data\db-wide-scc` → 重启 3317 →
  用 rust mysql crate 装载（`rr-conformance --wide-load --rows 1098342 --procs 4`，与
  MySQL 同源同参；pymysql 与 cjserver 握手不兼容故弃用 python loader）。
- 约 2k–6k 行后服务端 panic：
  - `src\inverted.rs:490`：`assert!(*docid < u32::MAX as u64, "docid 超出 RoaringBitmap 支持范围")`；
  - 随后 `src\mysql.rs:825`：`engine.write().unwrap()` 毒锁 `PoisonError` 连锁 panic。
- 客户端表现：`IoError { server disconnected }`，四线程在 i≈1998/3999/4000/5997 断开。
- 疑点（供修复侧定位）：docid 本应为 1..~6k，却超 u32 —— 疑似 §26/§27 对非默认表
  分配高位编码 docid（表级主键空间隔离），而倒排位图（RoaringBitmap 32 位）未同步
  处理高 docid；或 bitmap 字段（status/region 走 `bitmap_fields`）路径在批量装载下
  收到被抬高的 docid。复现命令见 6.2 参数 + `--wide-load`。

### 6.4 收尾状态

- 3317 已停止，`D:\shanshui-data\db-wide-scc` 已清空为初始空目录（其中数据为本次
  失败装载的部分垃圾行；原 1.09M 旧数据已于重建前按指示删除）。
- 3309/3310（db-s3-2b / db-s3-3a，RR 场景实例）已用新内核二进制按原命令行恢复运行。

## §7 可复用资产与后续

- MySQL 独立实例：配置 `tmp/my-wide-3316.ini`；datadir `D:\shanshui-data\mysql-wide-2g`
  （含 wide.t 1,098,342 行 + 测试后残余 200 行 upd 预留）。重启命令：
  `"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqld.exe" --defaults-file=d:\traeprojs\shanshui-cunji\tmp\my-wide-3316.ini --console`
- 复跑 MySQL：`D:\shanshui-cunji-target\release\rr-conformance.exe --sql-run --url "mysql://root@127.0.0.1:3316/wide" --out results-sqlrun-mysql-2g`
  （root 空密码；mysql 8 该实例默认 mysql_native_password）。
- cjserver 侧待内核修复（§26 docid 高位 vs 32 位 RoaringBitmap / 旧数据归属）后，
  再按 §6 流程重建数据集并跑 `--sql-run --url mysql://root@127.0.0.1:3317 --out results-sqlrun-scc-2g`，
  用 `tmp/tmp_compare_sqlrun.py` 合表出 mean/p95/p99/max 对比。

## §8 时间线（2026-09-03/04，本地）

| 时间 | 事件 |
|---|---|
| 23:51 | 旧内核 3317 实例启动（20:35 二进制） |
| 00:00 前后 | 旧 22 探针 cjserver 首轮结果（N=1,098,242） |
| 00:30 | 另一任务提交 M3（HEAD 19745a6） |
| 00:41 | 本会话编译新 cjserver 内核（含未提交 mysql.rs 改动） |
| ~00:46 | MySQL 3316 建实例（2G）→ 装载 1,098,342 行（185.7s）→ 26 探针全过（记录本文） |
| ~01:0x | cjserver 新内核读旧数据 COUNT=0；重建数据触发 inverted.rs panic（未修复，收尾） |
| 02:1x | 第二轮：探针扩至 37 项，MySQL 重测（基线 v2b / 组合索引对照 idx）并做 EXPLAIN |

## §9 第二轮：MySQL 37 探针（2026-09-04 02:1x，N=1,098,342）

在 §5 的 26 项基础上新增 11 项（按用户给定清单适配），新增项与结果（mean ms，
完整明细见 `results-sqlrun-mysql-2g-v2b/summary.md`；索引对照轮 `results-sqlrun-mysql-2g-idx/summary.md`）：

| # | 类别 | 探针 | 说明 | 无组合索引 mean | 加 (status,ts) 后 mean | p50 | p99 | max |
|---|---|---|---|---|---|---|---|---|
| 27 | 聚合 | group_by_multi | GROUP BY status,region | 513.44 | 500.24 | 518 | 528 | 528 |
| 28 | 聚合 | having_avg_gt | GROUP BY+HAVING AVG(amount)>50万 | 2116.48 | 2304.28 | 2408 | 2417 | 2417 |
| 29 | 排序 | orderby_multi | ORDER BY k,amount LIMIT 100（全表 filesort） | 648.94 | 644.33 | 648 | 746 | 746 |
| 30 | 索引 | composite_idx_point | status='active' AND ts=？ | **411.07** | **0.30** | 0.26 | 0.72 | 0.72 |
| 31 | 索引 | composite_idx_range | ts BETWEEN ?（非前置列） | **433.62** | **1.40** | 1.36 | 1.87 | 1.87 |
| 32 | 批量写 | insert_batch_10000 | INSERT 10,000 行/语句 | 260.21 | 332.79 | 319 | 369 | 369 |
| 33 | 批量写 | upsert_duplicate_key | INSERT..ON DUPLICATE KEY 单行（命中更新） | 0.23 | 0.27 | 0.23 | 4.45 | 4.45 |
| 34 | 批量写 | upsert_batch_100 | ..ON DUPLICATE KEY 100行/语句 | 2.07 | 1.95 | 1.95 | 2.06 | 2.06 |
| 35 | 事务 | txn_rr_readwrite | REPEATABLE READ 读写事务 | 0.62 | 0.58 | 0.56 | 0.76 | 0.76 |
| 36 | 事务 | txn_serializable | SERIALIZABLE 读写事务 | 0.55 | 0.63 | 0.60 | 0.91 | 0.91 |
| 37 | 事务 | txn_lock_wait | 双连接 FOR UPDATE 锁等待（副连接等锁 3s→1205） | 4001.99 | 4002.11 | 4002 | 4002 | 4002 |

补充解读：
- #27 多字段 GROUP BY ≈ 500ms（单字段 #14 ≈ 300ms），组合分组哈希/临时表开销 ~1.7×；
- #28 聚合 HAVING AVG 全表聚合 ≈ 2.1–2.3s（同 #15 量级）；
- #29 全表多列排序 ≈ 645ms（filesort）；
- #32 INSERT 10,000 行/语句 ≈ 260ms（≈0.026ms/行，远低于单行 0.24ms×10k 的 6s，批放大 ~20×）；
- #33/#34 upsert（走“重复→更新”分支）≈ 单行 0.2ms / 100 行 2ms——唯一键检查开销可忽略；
- #35/#36 单行事务在 RR/SERIALIZABLE 下 ≈ 0.55–0.65ms，与默认 RC(#25 ≈ 0.4ms) 差 ~0.2ms（SET+更高锁粒度）；
- #37 锁等待：副连接同 id UPDATE 在 3s 超时后 1205（主连接持锁 4s），单次 ≈ 4.0s。

## §10 组合索引实验与结论（是否需要建）

- 实验：`ALTER TABLE wide.t ADD INDEX idx_status_ts (status, ts)`（1,098,342 行，耗时 5.1s），
  对 #30/#31 重测；EXPLAIN 确认：
  - #30 点查 `WHERE status='active' AND ts=?`：`ref` 走 `idx_status_ts` + **Using index（覆盖）**
    → 411ms（idx_status 扫 ~22 万行后过滤）降至 **0.30ms**（~1370×）；
  - #31 `WHERE ts BETWEEN ?`（非前置列）：MySQL 8.0 用 **skip scan**（Using index for skip scan）
    借复合索引完成范围扫描 → 433ms（全表扫）降至 **1.40ms**（~310×）。
- 结论：
  1. 对“等值 status + 等值/范围 ts”这类负载，**建议建组合索引 `(status, ts)`**，收益巨大（千倍级），
     DDL 成本低（5s / 1.1M 行）；
  2. 组合索引按最左前缀设计，前置列应是等值高频列（status 类），后置列放范围/过滤列（ts）；
  3. 若 ts 独立范围查询为更高频路径，更稳妥是 `idx_ts` 或把 ts 放前置 `(ts, status)`
     （skip scan 估算行数 ~9.8 万，数据量更大/索引更宽时会退化）；
  4. 测试后已 **DROP** 该索引恢复原 schema（PRIMARY + idx_k/status/region/status_region），
      便于与 cjserver 侧（无组合索引、status/region 走位图倒排）公平对照；需要时重加仅 5s。

## §11 cjserver（最新产物 2026-09-04 07:40）10 万行冒烟（cjserver/documents）

- 产物：`D:\traeprojs\shanshui-cunji\cjserver.exe`（release 4.3 MB，07:40）。
- **装载路径修正（关键）**：cjserver 单库 `cjserver`、默认表 `documents`（`db_adapter.rs`：
  `docid = table_base(tid)<<48 | row`，`documents` tid=0 → docid=row 落在 32 位内；
  其它表 tid≠0 → docid ≥2⁴⁸）。而 status/region 走 **RoaringBitmap（32 位）位图倒排**
  （`inverted.rs` 断言 `docid < u32::MAX`）——**非默认表 + bitmap_fields 会触发内核
  panic**（0:41/0:57/7:40 三版在 `INSERT INTO t` 装载 ~2.5k–6k 行后均复现：
  `docid 超出 RoaringBitmap 支持范围` + PoisonError）。
  → cjserver 多表支持本身没问题（§26 M1~M3 表路由/按表 docid 区间），缺口在**位图倒排对
  非默认表高位 docid 的兼容**（内核侧待修；现以默认表 `documents` 承载单宽表语义，
  与 MySQL `wide.t` 对齐）。
  → 工具侧新增 `--table`（默认 `t`，cjserver 用 `documents`），sqlrun 与 wide_load 均支持。
- 装载：`rr-conformance --wide-load --table documents --rows 100000 --procs 4`
  → **100,000 行 · 4.9s**，无 panic。
- 37 探针全部通过（OK 100%，N=100,000，见 `results-sqlrun-scc-100k/summary.md`）关键均值(ms)：
  点查 0.29 · IN50 2.81 · BETWEEN100 4.0 · 枚举等值~3.8 · enum_count 0.22 · count_all 55.7 ·
  全扫(amount/cmp) 60.8 · group_by 69–97 · having_avg 80 · orderby(窗口) 5.9 ·
  orderby_multi(全表) 1231 · 写 0.2–3.6 · insert_batch_10000 311（10k行/语句可用）·
  upsert 单行 0.53 / 批量100行 3.33（cjserver 已支持 ON DUPLICATE KEY UPDATE）·
  事务 1.6–2.4 · 锁等待 ~4.0s（等锁超时，同 MySQL 形态）。
- 该轮同时验证：cjserver 连接/握手正常（rust mysql crate），删除区间/事务/多语句均可。

## §12 cjserver↔MySQL 37 探针正式对比（同量 1,098,342 行，均 2G 内存）

- 内核：仓库根 `cjserver.exe`（2026-09-04 08:24，P79 64 位化后），数据目录 `D:\shanshui-data\db-wide-scc`；
  配置 `tmp/tmp-cfg-wide-2g.toml`（2G = hotcache 1024 + blockcache 512 + inverted 256 + memtable 256 MB）。
- MySQL：3316（8.0.45，innodb_buffer_pool_size=2G），库 wide 表 t，**初始 N=1,098,342**（确定性装载 185.7s）。
- cjserver：默认表 documents，**初始 N=1,098,342**（与 MySQL 同量，wide-load 装载 60.4s）。
- 方法论：**装载即热（写入刚落 memtable/倒排内存），装载后直接实测一轮**；
  不叠加"预热轮"——实测发现 sqlrun 预留大区（insert_batch_10000 的 3 万行）区间清理
  （DELETE BETWEEN 预留区）**未生效**（历史遗留：多次 sqlrun 后 N 会漂移 +3 万+），
  采用"每轮前重置 + 重装载"保证口径一致（该清理缺口另记，非本内核 64 位化引入）。
- 对比表：[results-sqlrun-compare-2g/summary.md](../../results-sqlrun-compare-2g/summary.md)
  两侧明细：`results-sqlrun-mysql-2g-v2b/summary.md`、`results-sqlrun-scc-1m-2g/summary.md`；
  cjserver 10 万轮（阶段 A）另见 `results-sqlrun-scc-100k-v3/summary.md`（37/37 通过）。

**结论速览（mean，cjserver/MySQL 比值）**
1. **cjserver 显著领先**：enum_count 倒排 COUNT 载荷（0.66ms vs 64.39ms，快 ~98×）；
   写入同档或略慢（单点/批量 10-100 行 0.9–1.5×，事务 8× 因 fsync 组提交语义）；
2. **同档（~1×）**：点查 * 0.27 vs 0.21（1.3×）、数值>早停 1.2×、delete_id 0.9×、
   批量插 10/100 行 1.1×；
3. **cjserver 明显慢（无覆盖式二级索引/存算直扫）**：pk_between 100 行窗口 14×、
   枚举等值取行 4.5–8.1×、**全表无索引扫（count_all/sum/group/cmp_between）13–32×**（MySQL 走
   5 个二级索引/紧凑 InnoDB 页；cjserver 全扫需解 25 列宽文档）、事务性写路径 3–8×；
4. **cjserver 安全阀拒绝**：orderby_multi（ORDER BY k,amount LIMIT 100）在 110 万行上触发
   "候选集过大（>200,000）"守卫（1064 拒绝，MySQL 649ms 可跑）——cjserver 全表排序需显式
   WHERE 收敛；10 万行下则能跑（~1136ms）；
5. **组合索引语义不对等**：cjserver 无 ts/status 复合索引，#30/#31 实为
   "status 位图倒排候选 + ts 逐行过滤"（110 万行 → 921ms / 7300ms），而 MySQL 加
   `(status,ts)` 后为 0.30ms/1.40ms（A/B 见 §10，测试后已 DROP）。cjserver 侧等值/范围列的
   倒排能力仅覆盖 status/region/k 等字段位图；ts/amount 等未索引字段的过滤为全扫。
6. 数据漂移口径：每轮 sqlrun 尾部 +200 upd 预留行（N 以轮起始计），与 MySQL 侧一致。

## §13 事务/写路径 fsync 语义对等（P2-A，2026-09-04；development_remain §一.4 P2-A）

### 13.1 根因核对（P2-A ①：sqlrun/rr-conformance 事务是否落组提交路径）

事务探针（#25/#26/#35-37，MySQL ≈0.4–0.65ms vs cjserver 8×）的根因代码定位：

- MySQL 侧：`innodb_flush_log_at_trx_commit=1`（默认）——每次 COMMIT 对 redo fsync，但 InnoDB
  组提交把**同刻进入提交的并发事务合并为一次 fsync**，并发事务延迟不随连接数线性放大；
- cjserver 侧（本项核对结论）：COMMIT → `db_adapter::dispatch_query` → `engine.txn_commit`
  尾部**无条件 `flush_wal()`**（删除位图 flush + primary/delta/outbox 三路 WAL fsync）——
  即使 cjserver 默认 `group_commit_us=2000`，组提交也只摊薄**非事务 put**（`maybe_group_commit`），
  事务 COMMIT **不落组提交攒批、逐次显式 fsync** → 事务对比 8× 的直接原因；
  sqlrun/rr-conformance 经 MySQL 协议连 cjserver，其事务探针全部命中上述路径。

### 13.2 提交耐久档位语义（P2-A ②：`storage.flush_log_at_trx_commit`）

config 新增 `storage.flush_log_at_trx_commit`（0/1/2，**默认 1**，对齐 MySQL
`innodb_flush_log_at_trx_commit`；越界值校验拒绝），只管辖**事务 COMMIT** 的落盘语义：

| 档位 | MySQL（InnoDB）语义 | cjserver 实现（txn_commit 尾部） | 崩溃丢失 |
|---|---|---|---|
| 0 | 不随 COMMIT 落盘，后台每秒 flush+fsync | 与 2 同路径（本引擎无 InnoDB 独立 redo / OS-cache-only 写层，0/2 当前等价，见下） | ≤ 窗口（进程崩 / 断电） |
| 1（默认） | 每次 COMMIT fsync | `commit_persist()` = `flush_wal()`（位图 + WAL + outbox 全 fsync，COMMIT ack = 已落盘） | 无 |
| 2 | 每次 COMMIT 写 OS cache（不 fsync），后台每秒 fsync | `commit_persist()` = `maybe_group_commit()`：组提交开启（有后台落盘线程）→ 零同步 fsync，窗口/字节阈值统一落盘；组提交关闭 → **回退档位 1**（强安全兜底） | ≤ 窗口（断电；进程崩同窗口级） |

- 组提交窗口 = `group_commit_us`（cjserver 默认 2000µs）+ `group_commit_bytes`（默认 256KB）。
- **档位 0 与 2 的差异说明**：InnoDB 档位 2 的"COMMIT 写 OS page cache → 进程崩溃不丢"在本引擎
  无对应单层（WAL 攒批缓冲在进程内存，落盘即 write+fsync 一体）；档位 0/2 均表现为"COMMIT 交组
  提交窗口延迟落盘"。窗口毫秒级（远小于 InnoDB 的 1s 周期），需要更强保护用档位 1 或缩小窗口。
- 非事务单条写（autocommit INSERT/UPDATE/DELETE 的隐式提交）**不受本档位影响**，仍由
  `group_commit_us` 控制（保持既有插入吞吐语义）。

### 13.3 基准对比调档建议

- 对齐 MySQL 默认档位 1（强安全）：cjserver 配 1 → 单连接逐 COMMIT fsync 结构差 ~4-5×
  （每 COMMIT 双 WAL + 位图 fsync，与 MySQL 单 redo fsync 的物理差，排期接受为"难消"）；
- 并发事务对比：cjserver config 配 `flush_log_at_trx_commit = 2`（组提交默认开启）→ 并发
  COMMIT 共享窗口内一次 fsync（同 MySQL 组提交效果），目标 **≤2-3×**；
- 相关提交：`feat(P2-A)` 5421571（config 字段 + `commit_persist` + 4 单测），详见 problem_solving P82。

## §14 档位 2 实测：1.1M 事务探针对比（2026-09-04，P2-A 验收）

- 环境：cjserver（shanshui-cunji 内核，编译含 P2-A 5421571）`tmp/tmp-cfg-wide-2g-d2.toml`
  （= 原 2G 配置 + `flush_log_at_trx_commit = 2`，group_commit_us=2000）；3317 端口，默认表 documents。
- 数据：复用 db-wide-scc 现库（N=1,130,481，历史轮次漂移 +31k，事务探针单行 UPDATE 不受影响）。
- 结果目录：`results-sqlrun-scc-1m-2g-d2/summary.md`（37/37 通过，含 #29 ORDER BY Top-K 现可跑 19s×10）。
- 事务探针对比（mean ms）：

| # | 探针 | MySQL（§12 对比表，档位 1） | cjserver 档位 1（历史 scc-1m-2g） | **cjserver 档位 2（本轮）** | 档位 2/MySQL |
|---|---|---|---|---|---|
| 25 | txn_begin_upd_commit | 0.40 | 3.35（8.4×） | **0.57** | **1.4×** |
| 26 | txn_for_update_read | 0.38 | 0.58（1.5×） | **0.45** | **1.2×** |
| 35 | txn_rr_readwrite（RR 读写事务） | 0.58 | 3.52（6.1×） | **0.61** | **1.05×** |
| 36 | txn_serializable | 0.63 | 3.38（5.4×） | **0.61** | **1.0×** |
| 37 | txn_lock_wait（双连接等锁 3s） | 4002 | 4002 | 4001 | 1.0× |

- **验收判定**：P2-A 验收线"并发场景 ≤2-3×；单连接结构差 4-5×" **实测全部越过**——档位 2 下
  事务探针相对 MySQL 全部 ~1.0–1.4×（#25 由档位 1 的 8.4× → 1.4×，-83%；#35/#36 6× → ~1×）。
  档位 2 = COMMIT 交组提交窗口（2000µs 攒批一次 fsync）消除了逐 COMMIT 三路 fsync 的结构差，
  单连接串行事务也受益（窗口内 100 次 COMMIT 共享 fsync）。
- 顺带观测：#29 ORDER BY Top-K（P0-B）在 1.1M 上可跑（19s×10，冷全表排序），不再 1064 拒绝；
  #32 insert_batch_10000 213ms（档位 2 写路径攒批）优于档位 1 历史 396ms。

