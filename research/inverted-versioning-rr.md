# 倒排索引 RR 快照读设计（posting 无版本，snap_seq 为准）

> 状态：设计（2026-09-06 立项；同日机制澄清修订）。
> **用户确定的读法**：走倒排索引时**倒排内不加版本号**；读取顺序 = 先取全局 MVCC seq
> （快照 S），再读 term 对应的 docid 集合，**所有数据以 MVCC snap_seq 为准**（逐 docid 到
> 主表裁决可见性/值）。排期入口 = development_remain「倒排 MVCC/RR 专项」（P134 先行）。

## 0. 决策（2026-09-06，含机制修订）
1. **倒排 posting 不加版本号**——否决"posting 内每 (term,docid) 维护 add/remove seq"的初案
   （本档旧版曾以 posting 版本化为方向；经用户机制澄清修订如下）。posting 维持"最新态候选、
   只加不删"（现状不变，段 v6 不升 v7、无需 term diff/反向索引）。
2. **RR 一致性全部由主表全局 MVCC（seq / tombstone / 行版本）承担**。
3. 倒排读固定三步（事务与非事务统一语义）：
   ① 取快照 seq S——事务 = RR 快照 seq（对齐 MySQL：首条一致性读锚定；现实现注册时机核对，
     见 P134 核对项）；当前读/autocommit = 语句开始时的全局最新 seq；
   ② 读 term → docid 集合（posting 候选，含历史/已删/换值离开的 docid，**不做最新态删除预过滤**）；
   ③ 逐 docid 以 S 到主表裁决：≤S 行版本可见性（跳过删除位图、tombstone seq≤S→None、>S→旧版）
     与字段值复核——**所有数据以 snap_seq 为准**。
4. 正确性缺口（P134）先行修复，使上述模型在事务路径成立。
5. B：排序不做独立项（Roaring 结构性有序）；C：live_docids 升"快照活跃预过滤"可选小项
   （只滤"S 前已删"，换值仍须回表判值）；D：监控随阶段埋点。

## 1. 现状速览（2026-09-06 审计）
- Posting = `RoaringTreemap`（src/inverted/mod.rs `Posting` L58）；**无版本、无删除 API、
  只加不删**（mem DashMap<term, Vec<u64>> L66；delete/UPDATE 不触倒排）。
- 分层已具：mem → 段链（FST 字典 + v6 posting）→ `gc()` 全段合并成 1 段（src/inverted/gc.rs）；
  读 = `search()` union 全部段（src/inverted/query.rs L106-141）；mem Vec<u64> **无序**。
- 陈旧 docid 由行级复核兜底（§11.5 模型）：回表 `batch_get` 位图剔除 / `get_at` / WHERE 重算。
- `live_docids`（src/engine/engine.rs L151）= **最新态无版本活跃集**（put/delete/复活/purge 记账）。
- 删除双路：删除位图（无 seq，当前态短路）+ 版本化 Tombstone（mem 带 seq）。`get_at` 快照读
  **跳过位图**、按 tombstone seq 裁决（src/engine/mvcc.rs L114-135）。
- 快路径守卫：仅 `zone_field_aggregate` 检测活跃快照回退（src/engine/read.rs L385）；其余
  posting/live 快路径以最新态为口径、只服务非事务 SELECT。

## 2. 正确性缺口（P134 修，模型前提）
1. **字段谓词事务读漏行**：`txn_select_by_predicate`（src/server/command/transaction.rs L248-324）
   候选 = 最新态 sqlish execute（回表 `batch_get` **删除位图剔除**已删 docid）∪ 事务写集 →
   快照后被并发删除的行不在候选 → 同事务重复读消失；同事务点查 `id=N` 走 `get_at` 却见旧值
   → RR 不自洽。**违反"以 snap_seq 为准"**：候选不应做最新态删除预过滤。
2. **区间事务读与点查不一致**：`scan_range_txn`（src/engine/txn.rs L227-233）快照扫描仍按
   删除位图剔行（位图无 seq）→ 快照后删行被隐藏，与 `get_at` 跳过位图不一致。
3. 核对项：快照锚定时机——事务 RR 快照 seq 是 begin 注册（现 src/engine/txn.rs L52-59）还是
   首条一致性读取全局最新（MySQL 语义）；若需对齐 MySQL 首读锚定，属小改（含测试口径调整）。
- 修法总则：**候选/扫描不做"最新态删除"预过滤**（posting 只加不删 ⇒ 候选含历史 docid），
  可见性统一交快照裁决（`txn_get`/`scan_range_at` tombstone seq）；与 zone_field_aggregate
  "活跃快照下宁慢勿错" 哲学一致。

## 3. 统一读流程与正确性机制（无 posting 版本号）

### 3.1 流程（等值/区间/组合索引命中同构）
```
S = 快照 seq（事务 RR 快照 / 当前读语句级全局最新）
cand = posting(term) ∩ 窗口         // 只加不删 → 含历史 docid；LIMIT 早停沿用 P85 分块
rows = batch_get_at(cand, S)        // 新 API：批量快照取行（见 3.3）
→ 逐行以 S 取值：tombstone ≤ S → None（S 前已删，排除）；> S → 返回 S 前旧版（快照后删/覆盖仍可见）
→ 字段谓词按 S 行值复核（换值陈旧 docid 排除）→ 输出行（列投影照旧）
```

### 3.2 为什么无需 posting 版本号
- **删除/复活可见性**由主表 tombstone seq 裁决；**换值陈旧**由"取 S 行值复核"裁决——两路都
  要求回表，posting 只承担"缩小候选"职责，内嵌版本号省不下回表。
- posting 内 add/remove seq 方案代价（term diff、docid→terms 反查、段 v7、写放大）大于收益
  （只免"快照前已删/换值"候选的空回表——其中删除可用 3.4 的集合级预过滤替代，成本低一个量级）。
- 前提边界：候选生成必须**不含最新态删除预过滤**（P134 ①）、快照扫描**不按位图剔行**（P134 ②）。

### 3.3 需要的基建（P135，demo 先行）
- `Engine::batch_get_at(docids, S)` / `get_many_pk_in_at`：批量快照取行，语义 = 逐 `get_at`
  （跳过删除位图、tombstone seq 裁决、Delta CF 折叠 ≤S、HotCache 语义按 S 或直通），复用
  P2-D `batch_get` 的分块/缓存骨架。
- 接线点：`txn_select_by_predicate`（候选 → batch_get_at 复核替代逐行 txn_get，事务写集仍并入）、
  非事务倒排消费端（select.rs `eval_cond` posting 命中、collect_limited_rows / 回表）统一走
  `batch_get_at(S)`；区间/组合索引路径同构。
- autocommit 语义：S = 语句开始全局 seq；与"最新态 + 删除位图短路"结果等价（快照后删不跨语句
  可见），demo 验证保留位图快路径或统一快照读的成本，选实现。
- mem `Vec<u64>` flush 前按 docid 排序（闭合 merge_distinct k-way 升序前提，顺手 0.5 天）。

### 3.4 C：快照活跃预过滤（可选，P136）
- `live_docids` v2 = docid 级 `(add_seq, del_seq)` → `snapshot_live(S) = { add ≤ S ∧ (del 无 ∨ del > S) }`；
  候选先 ∩ snapshot_live(S)：**集合级剔除 "S 前已删" docid**，免其 batch_get_at 空跑。
- 局限（明确标注）：换值不产生 del → 换值陈旧不能靠它过滤，字段复核（3.1 末步）仍必要；
  "S 后删"行仍在 snapshot_live 内（del > S）→ 回表见旧值（RR 正确）。
- 与 P1 MVCC 快照生命周期（active_snapshots + mvcc_keep_floor）联动防 del 事件堆积。

## 4. 快路径守卫/计数口径（版本语义说明）
- P-GB/P-GB2/P121/count_* /inverted_group_stats 目前只服务**非事务** SELECT（最新态口径，
  守卫回退保证不误报）；接入快照读前统一改口径为 `posting 候选 ∩ snapshot_live(S)` + 行级
  抽样复核兜底，或维持"仅非事务 + 活跃快照回退"（同 zone_field_aggregate 门禁）。
- 本次方向下**不新增倒排内版本结构**，故 §4 主要为"接线时守卫选择"而非新内核。

## 5. D：倒排专项监控（随阶段埋点）
- `shanshui_inv_segment_count`、`shanshui_inv_gc_pending_bytes`、`shanshui_inv_delta_fst_over_bytes`、
  `shanshui_inv_mem_docids`、`shanshui_inv_posting_cache_{hit,miss}`、`shanshui_inv_flush_segments_total`、
  快照侧（已有 active_snapshots/oldest）：`shanshui_snapshot_read_{rows,recheck}_total`（batch_get_at 回表率）、
  删除预过滤省行数；SHOW MEMORY/STATUS 行集 + 诊断 demo。

## 6. 分阶段排期（P134 起）
| 项 | 内容 | 量级/依赖 | 验收 |
|---|---|---|---|
| **P134 事务读快照语义收口**（先行） | 修 §2 缺口①②（候选去最新态删除预过滤、scan_range_txn 去位图剔行）+ 快照锚定时机核对项③ | ~1.5~2 天；P0-C 基建 | 谓词/区间与点查 RR 语义自洽（快照后删/并发换值跨事务测试）；全量回归绿 |
| **P135 倒排快照读统一接线**（snapshot-first，demo 先行） | §3.3：batch_get_at + 候选端/区间/组合接线 + autocommit S 语义选型 + mem 排序收尾 | 依赖 P134 | 倒排等值/区间事务读 = get_at 口径（无位图剔除漏行）；LIMIT/分块/墓碑语义回归；A/B 回表成本量化 |
| **P136 快照活跃预过滤**（C，可选） | §3.4：live v2 (add/del seq) + snapshot_live + 候选预过滤 | 依赖 P135 | S 前已删候选零空回表；快照后删/复活口径 = 权威扫描 |
| **P137 倒排专项监控**（D） | §5 gauge + SHOW 行集 + 诊断 demo | 随阶段埋点收口 | 回表率/GC 积压/长快照窗可见 |

## 7. 风险与边界
- 回表成本 = 候选大小：posting 命中大（如低选择性）时 batch_get_at 量大 → 沿用 P85 分块 +
  LIMIT 早停 + P85/P2-D 块缓存局部性；3.4 预过滤只省"已删"部分。
- autocommit 从"位图短路"转统一快照读的开销 demo 验证；可保留位图路径（S=当前 seq 等价）。
- 删除位图仍服务当前态读/写侧，不回退；v6 段格式不动（无版本化字段），零迁移。
