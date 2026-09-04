# 交接说明：MySQL ↔ cjserver 对照（RR 一致性 + 宽表性能）

> 用途：带到新工作任务会话继续使用。本说明覆盖两块结论：
> A. RR 事务一致性对照（MySQL ↔ cjserver，确定性场景集）；
> B. 两者在 ~110 万行宽表上的 37 探针性能对照。
> 生成时间：2026-09-04；环境：Windows，本地实例 3316(MySQL) / 3317(cjserver)。

---

## 0. 术语与现状（重要）

- 产品/引擎名：**山水存迹 shanshui-cunji**；服务端二进制 **cjserver.exe**（Cargo `[[bin]] name="cjserver"`，入口 `src/bin/mysql_server.rs`）。
- **改名动作（进行中，未提交）**：内核默认库名 `DEFAULT_DB` 由 `"scc"` → `"cjserver"`（单库模型：库 `cjserver`、表 `documents`；启动日志/`SHOW DATABASES` 同步变）。已改文件：
  - `src/db_adapter.rs`（const + 注释）
  - `src/bin/mysql_server.rs`（doc 注释）
  - `rr-conformance/src/main.rs`（`--my-db` 默认 `scc`→`cjserver`）
  - `rr-conformance/src/sqlrun.rs`（env 标签 “SCC（3317…”→“cjserver（shanshui-cunji，3317…”）
  - `user_guide/宽表SQL性能基准记录.md`、`results-sqlrun-compare-2g/summary.md`（标签 SCC→cjserver 全文替换）
- 状态：`cargo test --release --lib` **628 passed / 0 failed / 3 ignored** ✅；**尚未 git commit**，且改名后 **cjserver.exe / rr-conformance.exe 尚未重建**（当前 3317 跑的是改名前的旧 exe）。
- 历史目录/结果文件名里的小写 `scc`（如 `results-sqlrun-scc-1m-2g/`、`db-wide-scc`）**未改名**，仅为存储路径，与默认库名无关。
- 仓库最近提交（develop）：`cd0d002`（.gitignore 收编 results*/log/pycache/根 exe）为最新；改名改动在工作区未提交。

---

## A. RR 事务一致性对照（MySQL ↔ cjserver）

### A.1 工具与入口

- crate：`rr-conformance/`（独立二进制 `rr-conformance.exe`，产物在 `D:\shanshui-cunji-target\release\rr-conformance.exe`）。
- 入口：`--rr-cases`（**确定性 SQL 场景集**，跑完即退，非随机矩阵）。
- 两侧同时执行同一 SQL 序列并逐条比对结果：`mysql`（MySQL）↔ `mydb`（cjserver/SCC）。
- 复用/对照逻辑：本仓库的事务语义实现与 rr-conformance 的验证方法一致（快照一致读、当前读、提交/回滚可见性等）。

### A.2 场景集 C1–C9（`rr-conformance/src/rr_cases.rs`）

| 用例 | 语义 | 覆盖点 |
|---|---|---|
| C1 | snapshot-stable | 他事务 UPDATE 提交后，快照一致读仍见旧值；当前读见新值 |
| C2 | own writes visible | 事务内自插/自改立即可见；看不到他会话未提交写 |
| C3 | deleted row still visible | 他事务 DELETE 提交后，一致读仍见旧行；当前读不见 |
| C4 | phantom insert invisible | 他事务区间 INSERT 提交后，主事务范围一致读仍不见（**防幻读**） |
| C5 | COMMIT persists | 提交后 aux 会话可见 |
| C6 | ROLLBACK discards | 回滚后 aux 会话见原值 |
| C7 | duplicate-key INSERT | 主键重复 INSERT → 两侧均 1062（不静默覆盖） |
| C8 | txn non-PK predicate SELECT | 事务内**非主键列**谓词 SELECT（主库候选 ∪ 同事务写集） |
| C9 | non-txn IN-list write | autocommit `UPDATE/DELETE WHERE id IN (...)` 行为对齐 |

### A.3 结论

- **最新确定性运行：pass = 9 / fail = 0**（尾部汇总见 `rr-conformance/results-rrcases7-run.log`，逐用例输出同文件）。
- 早期排查记录：缺陷 A（FOR UPDATE 当前读语义）与缺陷 B（RR 范围一致读/防幻读）已修复；C4 属防幻读验证用例。修复涉及事务读路径与 COMMIT 持久化/隔离读路径（详见本项目事务相关 commit 与排期文档 §25 记录）。
- 说明：C1–C6 验证快照隔离核心语义，C7–C9 验证边界行为与 MySQL 对齐（1062、事务内非主键谓词、autocommit IN 写）。

### A.4 复跑命令（模板）

```
D:\shanshui-cunji-target\release\rr-conformance.exe --rr-cases \
  --mysql-url "mysql://root@127.0.0.1:3306/mysql" \
  --my-url    "mysql://root@127.0.0.1:3317"
```

（`--my-db` 默认值已随改名改为 `cjserver`；`rr-cases` 用固定种子表 t_test/t_combo，id=1..2000 区段。）

---

## B. ~110 万宽表性能对照（37 探针，均 2G 内存）

### B.1 环境（两侧同量、同装载口径）

| 项 | MySQL | cjserver |
|---|---|---|
| 实例 | 3316（8.0.45，独立进程） | 3317（cjserver，data dir `D:\shanshui-data\db-wide-scc`） |
| 内存 | innodb_buffer_pool_size=2G | 2G = hotcache 1024 + blockcache 512 + inverted 256 + memtable 256 MB |
| 数据 | 库 `wide`，表 `t`（25 列宽表） | 库 `cjserver`（原 `scc`），表 `documents`（25 列同构） |
| 初始行数 | **N=1,098,342** | **N=1,098,342**（与 MySQL 同量） |
| 装载 | 确定性装载（~185.7s） | `rr-conformance --wide-load --table documents --rows 1098342`（60.4s） |
| 索引 | PRIMARY + idx_k/status/region/status_region（组合索引 A/B 实验后已 DROP） | status/region 等走位图倒排；无二级索引 |
| 口径 | 装载即热，装载后直接实测一轮；SQL 探针工具 `rr-conformance --sql-run --table …` | 同左（每轮前“重置+重装载”，避免历史轮预留行残留） |

### B.2 产物

- 对比表：`results-sqlrun-compare-2g/summary.md`（已改名标签，已入库）
- 明细：`results-sqlrun-mysql-2g-v2b/summary.md`（MySQL）、`results-sqlrun-scc-1m-2g/summary.md`（cjserver 110 万）、`results-sqlrun-scc-100k-v3/summary.md`（cjserver 10 万，37/37）
- 记录主档：`user_guide/宽表SQL性能基准记录.md`（§12 为 110 万正式对比；§9-§10 MySQL 两轮+组合索引实验；§11 cjserver 冒烟/多表缺口）

### B.3 37 探针构成

点查(1-4)/范围(5)/倒排枚举(6-9)/数值扫描(10-11)/聚合(12-15)/排序(16,29)/写(17-24)/事务(25-26,35-37)/多字段聚合与组合索引语义(27-31)/大批量写与 upsert(32-34)/锁等待(37)。覆盖：PK 点查/IN/区间、枚举位图、全表无索引扫、GROUP BY/HAVING、ORDER BY、批量插 10/100/10k 行、upsert、区间删除、RR/SERIALIZABLE、并发锁等待。

### B.4 关键数字（mean ms；比值 = cjserver / MySQL，<1 为 cjserver 快）

| 探针 | MySQL | cjserver | 比值 | 解读 |
|---|---|---|---|---|
| pk_point_star | 0.21 | 0.27 | 1.3× | 同档 |
| pk_in_50 | 0.38 | 1.92 | 5.1× | 批量点查 cjserver 慢（无预取） |
| pk_between_100 | 0.29 | 4.05 | 14.0× | 主键窗口扫：MySQL 聚簇顺序页 vs cjserver 逐行定位+宽文档解码 |
| enum_sel_limit100 | 0.52 | 4.20 | 8.1× | 位图命中→随机 docid 取行 |
| **enum_count** | 64.39 | **0.66** | **0.01×（快 ~98×）** | 倒排计数载荷，MySQL 遍历二级索引逐行数 |
| combo_and | 0.61 | 3.49 | 5.7× | 枚举×枚举 AND 位图取行 |
| cmp_between（amount 全扫） | 504.6 | 6686 | 13.3× | 无索引数值区间=全扫+解码 |
| count_all | 185.7 | 5869 | **31.6×** | MySQL 走紧凑索引页计数；cjserver 逐文档解码 |
| sum_where_enum | 398.8 | 6524 | 16.4× | 同上 |
| group_by_status | 337.9 | 6435 | 19.0× | 同上 |
| group_by_multi | 513.4 | 6894 | 13.4× | 同上 |
| having_avg_gt | 2116 | 6701 | 3.2× | MySQL 该查询本身重（临时表） |
| orderby_win_1000 | 0.82 | 8.13 | 9.9× | 窗口排序 |
| **orderby_multi（全表）** | 648.9 | **拒绝** | — | cjserver 安全阀：候选 110 万 > 20 万上限，1064；MySQL filesort 649ms |
| delete_id | 0.20 | 0.19 | 0.9× | 同档 |
| update_in50 | 0.64 | 2.20 | 3.4× | |
| insert_single / batch100 / batch10000 | 0.22 / 3.09 / 260 | 0.30 / 3.41 / 396 | 1.4×/1.1×/1.5× | 写入同档偏慢 |
| upsert 单行 / 批100 | 0.23 / 2.07 | 0.48 / 8.39 | 2.1×/4.1× | ON DUPLICATE KEY UPDATE 已支持 |
| **delete_range50** | 1.12 | 7537 | **~6700×** | 区间删：MySQL B+Tree 页级删；cjserver 逐行 tombstone+倒排逐词删 |
| txn begin→update→commit | 0.40 | 3.35 | 8.4× | COMMIT fsync/组提交语义 |
| txn_rr_readwrite | 0.62 | 3.52 | 5.7× | |
| txn_serializable | 0.55 | 3.38 | 6.1× | |
| txn_lock_wait | 4002 | 4002 | 1.0× | 双端等锁超时形态一致（~4.0s） |

### B.5 结论与设计级差距（110 万量级，2G）

1. **cjserver 体系性领先**：倒排等值统计（enum_count 0.66ms，快 ~98×）；写入同档偏慢 1-2×（可再开组提交拉齐）。
2. **同档**：点查、单点/小批量写、delete_id（0.9-1.5×）。
3. **cjserver 显著慢（无覆盖式索引/宽文档解码）**：主键范围窗口 14×、位图命中取行 4.5-8×、全表无索引扫 13-32×、事务写 3-8×、区间删除 ~6700×、upsert 批 4×。
4. **安全阀**：全表 ORDER BY（候选 >20 万）被 1064 拒；MySQL filesort 可跑。→ 需 WHERE 收敛或内核支持 top-K/外部排序。
5. 不对等项：#30/#31 “组合索引”探针在 cjserver 实为 status 位图+ts 逐行过滤（921ms / 7.3s）；MySQL 在 §10 A/B 加 `(status,ts)` 后为 0.30ms/1.40ms（测后已 DROP）。
6. 方法论坑（新会话注意）：cjserver 侧 sqlrun 预留大区（insert_batch_10000 的 3 万行）区间清理 **未生效**（历史遗留，非 64 位化引入）→ 每轮前“重置+重装载”保证 N 口径；MySQL 侧清理正常。

---

## C. 下一步（新任务建议起点）

1. **收尾改名**：commit 工作区改名改动（A.0 所列 5+ 文件）→ `cargo build --release --bin cjserver` 与 rr-conformance → 替换仓库根 `cjserver.exe` → 重启 3317 冒烟（启动日志应为“库 cjserver，表 documents”，旧 `D:\shanshui-data\db-wide-scc` 数据应原样可读——数据按目录存储，与默认库名无关）。
2. **1,000 万行瓶颈定位（本次方向）**：先 MySQL 后 cjserver 同量对测，回答“MySQL 瓶颈在哪、是否 ~1 千万起”：
   - MySQL：需先把 25 列 `wide.t` DDL/确定性装载脚本翻出来（python loader ~185.7s/1.1M；或直接复用 `rr-conformance --wide-load --url mysql://root@127.0.0.1:3316/wide10 --table t --rows 10000000`，注意 loader 是否自建表）；建议**新建库/新数据目录**（保留 1,098,342 基准资产，勿删），bp 仍 2G；先查 D 盘剩余空间（预计两侧各需 8-15GB）。
   - cjserver：重置 `db-wide-scc` → `--wide-load --table documents --rows 10000000`（1.1M 时 60.4s，10M 预计 ~10-20min）→ `--sql-run --url mysql://root@127.0.0.1:3317 --table documents --out results-sqlrun-cj-10m`。
   - 对比：`tmp/tmp_compare_sqlrun.py <mysql-summary> <cj-summary> <out>`；全扫类与 delete_range 在 10M 会放大 ~10×（数秒~数分钟/探针），预留足够墙钟。
3. 可用于更大规模验证的资产：`shanshui-cunji-gen-dataset` 可生成 Parquet 数据集（已提到 50M 档）；YCSB 负载在 `shanshui-cunji-ycsb`（a/b/c/f，`--group-commit-us` 测组提交）。

---

## D. 环境现状（交接时刻快照）

- MySQL 3316：运行中，`wide.t` COUNT=1,098,342 ✅（基准保留）。
- cjserver 3317：运行中（**改名前的旧 exe**），`D:\shanshui-data\db-wide-scc` 约 113 万行（1,098,342 + 探针预留行未清，重测先重置）。
- 3306/3309/3310：运行中（旧实例，未动）。
- 未提交改动：改名（见 A.0）；未跟踪：`tmp/` 脚本、`user_guide/决策.md`、`user_guide/代码统计.md`、根 exe 等（gitignore 已收编 results*/log/pycache）。
- 仓库最新 commit：`cd0d002`（chore .gitignore）。
