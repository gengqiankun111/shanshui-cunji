# 宽表 SQL 性能对比（MySQL ↔ cjserver）测试步骤存档

> 目的：为 `user_guide/性能对比-优化器大项后-10万与110万_v0.md` 一类的 MySQL ↔ cjserver 宽表 SQL
> 对比报告提供**可复用的完整执行步骤**。方法源头：`user_guide/宽表SQL性能基准记录.md` §7~§12
> （2026-09-03/04 实测沉淀），本档整理为可重复执行流程。

## 1. 概念与产物

- **被测两端**：MySQL 8.0.45（独立实例）+ cjserver（本库 MySQL 协议服务器 `shanshui-cunji-mysql-server`）。
- **探针集**：37 项宽表 SQL（点查/范围/倒排/扫描/聚合/排序/索引/写/事务；`pk_*`/`enum_*`/`combo_*`/
  `field_in`/`cmp_*`/`count_all`/`sum_where_enum`/`group_by_*`/`having_*`/`orderby_*`/`composite_idx_*`/
  `update_*`/`insert_*`/`delete_*`/`txn_*` 等），定义在 rr-conformance 扩展子命令 `--sql-run` 内。
- **工具**：`rr-conformance`（Rust mysql crate 客户端，`--sql-run` 跑探针 / `--wide-load` 装载；
  exe：`D:\shanshui-cunji-target\release\rr-conformance.exe`，构建见 §2）。
- **结果**：`results-sqlrun-<端>-<规模>/summary.md`（运行日志 + 汇总）；对比表由
  `tmp/tmp_compare_sqlrun.py` 合表生成，再整理成报告 md。

## 2. 构建（一次）

```powershell
# 本机 cargo target-dir 独立（D:\shanshui-cunji-target）
cargo build --release --bin shanshui-cunji-mysql-server --target-dir D:\shanshui-cunji-target
# rr-conformance 独立 crate（只依赖 mysql/rand/anyhow，不编译 cjserver）
cargo build --release --manifest-path rr-conformance\Cargo.toml --target-dir D:\shanshui-cunji-target
```

## 3. 环境准备

### 3.1 MySQL 独立实例（2G buffer pool，宽表基准专用）

- 配置：`tmp/my-wide-3316.ini`；datadir `D:\shanshui-data\mysql-wide-2g`；
  `innodb_buffer_pool_size=2G`；root 空密码 + mysql_native_password。
- 启动：
  ```powershell
  & "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqld.exe" `
    --defaults-file=d:\traeprojs\shanshui-cunji\tmp\my-wide-3316.ini --console
  ```
- 建库/宽表（`wide` 库，表名 MySQL 侧默认 `t`）：按 sqlrun/wide_load 的建表方言（37 探针字段：
  id 主键、status/city 枚举、ts 时间、amount/score 数值、多文本列等；见 rr-conformance 源码）。

### 3.2 cjserver（SCC）被测端

- 配置：`tmp/tmp-cfg-wide-2g.toml`（2G = hotcache 1024 + blockcache 512 + inverted 256 +
  memtable 256 MB）；数据目录 `D:\shanshui-data\db-wide-scc`。
- 启动（端口示例 3317）：
  ```powershell
  .\target\release\cjserver.exe --data-dir D:\shanshui-data\db-wide-scc `
    --config tmp\tmp-cfg-wide-2g.toml --bind 127.0.0.1:3317 --watchdog-secs 3600
  ```
  （若用 `shanshui-cunji-mysql-server`，注意其 CLI 参数对应关系；本仓库根 `cjserver.exe` 为
  便捷别名产物。）

## 4. 数据集装载（两规模：10 万 / 110 万）

同源同参装载两侧，保证行数/内容一致。

- **MySQL 侧（python 装载器）**：
  ```powershell
  python tmp\tmp_wide_load.py --mode load --port 3316 --user root --rows 1098342 --procs 4
  python tmp\tmp_wide_load.py --mode load --port 3316 --user root --rows 100000  --procs 4
  ```
- **cjserver 侧**（pymysql 与 cjserver 握手不兼容 → 用 rr-conformance 同源装载）：
  ```powershell
  # 先清空 db-wide-scc 重启 3317，再装载（多表命名空间：SCC 表名用 documents）
  D:\shanshui-cunji-target\release\rr-conformance.exe --wide-load --table documents --rows 1098342 --procs 4
  D:\shanshui-cunji-target\release\rr-conformance.exe --wide-load --table documents --rows 100000  --procs 4
  ```
- 已知坑：
  - **组合索引（cidx）nosync 未刷盘时重启丢键** → 基准轮必须“重装后立即跑”，不要重启后再测 #30/#31。
  - 装完先跑 count 对齐：`SELECT COUNT(*)` 两侧一致再进 §5。

## 5. 跑 37 探针（两侧各自出结果目录）

```powershell
# MySQL（wide 库，root 空密码）
D:\shanshui-cunji-target\release\rr-conformance.exe --sql-run `
  --url "mysql://root@127.0.0.1:3316/wide" --out results-sqlrun-mysql-100k
# …110 万轮：--out results-sqlrun-mysql-1100k

# cjserver（表 documents；url 到 3317 实例）
D:\shanshui-cunji-target\release\rr-conformance.exe --sql-run `
  --url "mysql://root@127.0.0.1:3317" --table documents --out results-sqlrun-scc-100k
# …110 万轮：--out results-sqlrun-scc-1100k
```

约定结果目录名（与 v0 报告一致）：
`results-sqlrun-mysql-100k / results-sqlrun-scc-100k / results-sqlrun-mysql-1100k / results-sqlrun-scc-1100k`。

注意：
- 每轮约 2–3 分钟；不叠加“预热轮”（sqlrun 预留大区 `insert_batch_10000` 的 3 万行清理会
  干扰后续轮，保持每轮从干净状态起测）。
- 写/事务探针会改变数据 → 对比轮两端的探针顺序保持一致即可，重测需重灌数据集。

## 6. 对比汇总与报告

1. 用 `tmp/tmp_compare_sqlrun.py` 合表（mean/p95/p99/max），输出比值表。
2. 按报告模板整理：每探针列 MySQL mean / cjserver mean / 比值（<1 = cjserver 快），
   标注类别与根因备注（如“LIMIT 未下推回表”“PAX 布局列 IO”等）。
3. 归档模板参照 `user_guide/性能对比-优化器大项后-10万与110万_v0.md` 的表格结构。

## 7. 资产与复跑清单

| 资产 | 路径 |
|---|---|
| MySQL 实例配置 | `tmp/my-wide-3316.ini` |
| SCC 2G 基准配置 | `tmp/tmp-cfg-wide-2g.toml` |
| MySQL python 装载 | `tmp/tmp_wide_load.py` |
| 对比合表脚本 | `tmp/tmp_compare_sqlrun.py` |
| SCC 数据目录 | `D:\shanshui-data\db-wide-scc` |
| MySQL datadir | `D:\shanshui-data\mysql-wide-2g` |
| 方法源记录 | `user_guide/宽表SQL性能基准记录.md` |

## 8. 参考（历史报告）

- `user_guide/性能对比-优化器大项后-10万与110万_v0.md`（37 探针、10 万 + 110 万两轮）
- `user_guide/宽表SQL性能基准记录.md`（同方法早期版本 + 组合索引实验 + 装载/结果时间线）
