# rr-conformance

RR（REPEATABLE READ）隔离级别**行为对照压测**：MySQL ↔ 自研库（山水存迹），
Try-Confirm-Cancel 简易两阶段 + 提交后外部视角校验 + 全表终态逐行比对。

## 用法

```bash
# 阶段 1：双 MySQL（两个库）——先验证工具/校验链路本身（同引擎期望 0 DIFF、终态一致）
rr-conformance --init --txns 200 --threads 4 \
  --mysql-url mysql://root:123456@127.0.0.1:3306 \
  --my-url    mysql://root:123456@127.0.0.1:3306 \
  --mysql-db rr_a --my-db rr_b --out results-mysql2

# 阶段 2：双 SCC（两个独立数据目录实例）
rr-conformance --init --txns 200 --threads 4 \
  --mysql-url mysql://root@127.0.0.1:3309 --my-url mysql://root@127.0.0.1:3310 \
  --mysql-db scc --my-db scc --out results-scc2

# 阶段 3：MySQL + SCC
rr-conformance --init --txns 200 --threads 4 \
  --mysql-url mysql://root:123456@127.0.0.1:3306 --my-url mysql://root@127.0.0.1:3308 \
  --mysql-db rr --my-db scc --out results-mix
```

输出：`summary.txt`（提交/回滚分类/终态一致）、`diff.log`（差异与回滚详情）、`txn/`。

## 每事务流程（两库同 SQL 顺序执行）

1. **Try**：对 op 涉及资源执行 `SELECT ... FOR UPDATE`（当前读预锁），比对行集；
2. **Confirm**：逐条执行原始 op，比对 affected/行集；每 op 额外做**事务内快照读 + 当前读**
   两次校验；affected 不一致时用事务可见快照复核（并发时序伪差 → NOTE，真差异 → DIFF）；
3. **Cancel**：任一步异常两边同时 ROLLBACK，记录 gtrx/ops/每步返回/错误分类
   （DUPLICATE / LOCK_TIMEOUT / DEADLOCK / DIFF）；
4. 提交成功后抽样开外部连接做快照 + FOR UPDATE 外部视角校验；
5. 全部跑完：全表 dump（`t_test` / `t_combo`）逐行比对终态。

## 操作集与生成

- `t_test`（主键 id）单点行锁；`t_combo`（主键 id + 二级 a,b）等值/范围/批量；
- 70% 单点 / 30% 范围·批量；单点写类按锁 key 升序重排（降随机死锁），range/batch 原样下发；
- **worker 按键分区**：种子行 `[1,rows]` 均分给 `--threads` 个 worker，新行池在 rows 后按
  worker 步进预留——跨 worker 键零重叠。否则两个同引擎库各自多 worker 独立调度也会因
  提交顺序不同产生合法分歧（逐 op 行集 DIFF + 终态分叉，见 2000 事务标定）；
  分区后每个 worker 内事务串行 → 双端历史等价。需 `--rows ≥ 8×threads`。
- **FOR UPDATE 仅主键（id）作用域**：t_combo 的 a/(a,b) 无索引，MySQL 在其上 FOR UPDATE 走
  全表 next-key 锁 → 锁扩散到其它 worker 键区造成跨 worker 死锁（一侧回滚一侧未回滚 =
  终态分叉）。故非主键谓词只做纯快照读（行集自限本 worker 键区，两侧仍一致）；
  二级列加锁/间隙行为差异记入已知边界。

## 已知边界（方案约定）

- 只处理应用逻辑异常：进程在 mysql.commit 与 mydb.commit 之间被 kill → 两库可能不一致（不做
  完整 XA 崩溃恢复）；
- 两库独立执行 → 并发交错会产生 affected 等合法时序差异（工具以事务可见快照复核区分伪差）；
- `insert on duplicate key update` 暂不纳入；
- SCC 无二级唯一索引实现 → 两端使用**同一 DDL**（无 UNIQUE），"重复键不报错"属已知差异域，
  由工具采集记录而非断言。
- mysql crate 与 SCC 握手的兼容问题待解决前，阶段 2/3 的"自研端"请用支持其协议的客户端路径。
