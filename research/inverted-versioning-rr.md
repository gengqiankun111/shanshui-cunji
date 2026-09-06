# 倒排索引版本化 + RR 快照读设计（inverted posting MVCC）

> 状态：设计/立项（2026-09-06）。**用户确定方向：倒排索引走版本号（posting 版本化），
> 以该方向为基础做 RR 事务隔离级倒排读**；配合 live_docids（活跃集）版本化与倒排专项监控。
> 排期入口 = development_remain「倒排 MVCC/RR 专项」（P134 先行）。本档为设计依据。

## 0. 决策（用户 2026-09-06）
1. **倒排 posting 走版本号 = 确定方向**（非可选）；目标 = 倒排能独立服务 RR 快照读，
   不依赖"每次全候选回表 + 行级字段复核"兜正确性。
2. 范围排序：**P134 事务读快照语义收口（正确性小项）先行** → 版本化大项（demo 先行立项）。
3. B（倒排排序）**不做独立大项**：Roaring 结构性有序，交集 = 位图 `&`；仅收 mem `Vec<u64>` 排序小修。
4. C（live_docids 配合）：升格为"版本化活跃集"与 posting 版本化统一成共享原语。
5. D（监控）：倒排专项计量随版本化各阶段逐步埋点。

## 1. 现状速览（2026-09-06 审计）
- Posting = `RoaringTreemap`（src/inverted/mod.rs `Posting` L58）；全库**无版本、无删除 API、
  **只加不删**（mem DashMap<term, Vec<u64>> L66；flush/gc 无 remove；delete/UPDATE 不触倒排）。
- 分层已具：mem → 段链（每段 FST 字典 + v6 posting，SEG_VERSION=6）→ `gc()` 全段合并成 1 段
  （src/inverted/gc.rs: union L88-105，term 字典序、docid 位图结构性升序）。
- 读 = `search()` union 全部段一次（src/inverted/query.rs L106-141）；mem Vec<u64> **无序**
  （merge_distinct k-way 前提"各源升序"隐含依赖）。
- 陈旧 docid 由行级复核兜底（§11.5 模型）：回表 `batch_get` 位图剔除 / `get_at` / WHERE 重算。
- `live_docids = Mutex<Option<RoaringTreemap>>`（src/engine/engine.rs L151）= **最新态无版本活跃集**
  （open 空库播种 / 懒建基线 / put·delete·复活·purge 记账）。
- 删除双路：删除位图（无 seq，当前态短路）+ 版本化 Tombstone（mem 带 seq）。`get_at` 快照读
  **跳过位图**、按 tombstone seq 裁决（src/engine/mvcc.rs L114-135）。
- 快路径守卫：仅 `zone_field_aggregate` 检测活跃快照回退（src/engine/read.rs L385）；
  P-GB/P-GB2/P121/count_* 快路径以最新态 live/posting 为口径，只服务**非事务** SELECT。

## 2. 正确性缺口（P134 修）
1. **字段谓词事务读漏行**：`txn_select_by_predicate`（src/server/command/transaction.rs L248-324）
   候选 = 最新态 sqlish execute（回表 batch_get 位图剔除已删 docid）∪ 事务写集 → 快照后被并发
   删除的行不在候选 → 同事务重复读消失；同事务点查 `id=N` 走 `get_at` 却见旧值 → RR 不自洽。
2. **区间事务读与点查不一致**：`scan_range_txn`（src/engine/txn.rs L227-233）快照扫描仍强制按
   删除位图剔行（位图无 seq）→ 快照后删行被隐藏，与 `get_at` 跳过位图不一致。
- 修法总则：**候选/扫描不按"最新态删除"预过滤**（posting 只加不删 ⇒ 候选天然含历史 docid），
  可见性统一交给快照裁决（`txn_get`/`scan_range_at` tombstone seq）；与 zone_field_aggregate
  "活跃快照下宁慢勿错" 哲学一致。

## 3. 版本化倒排总体架构（主线，A+C 统一）

### 3.1 目标语义
每 (term, docid) 维护版本事件：`add_seq`（字段值进入该 term）与 `remove_seq`（离开该 term：
行删除、或字段值变更/覆盖离开旧值）。posting 快照视图：

```
view(term, S) = { docid | add_seq ≤ S ∧ (remove_seq 不存在 ∨ remove_seq > S) }
```

字段等值谓词快照答案 = `view(term,S) ∩ table窗口 ∩ 快照活跃` —— **免逐候选回表字段复核**。
计数/守卫快路径同样以 view(term,S) 为口径。

### 3.2 版本粒度与存储形态（demo 选型点）
RoaringTreemap **无 per-docid payload**，不能直接存 seq。候选形态：
- **甲：位图 + 旁路 seq**——每 term 维护 added 位图/有序数组 + 对应 add_seq 数组（docid 升序，
  seq 并排数组/间隔编码），remove 侧同构；快照过滤 = 双指针/游标按 (docid, seq) 滤窗口。
  好处：读可二分/游标、与 Roaring 迭代共存；代价：seq 编码膨胀（v7 格式设计）。
- **乙：版本化双位图集合**——(added≤S) 位图与 (removed≤S) 位图（值域编码：add 事件记入
  "seq 桶"，快照取桶并集）；代价：桶粒度精度、GC 收敛同甲。
- **丙：段粒度 seq 窗口（推荐先验证）**——版本落在**写入批次/段**：段记 [min_seq, max_seq]，
  快照剪掉 min_seq > S 的段（对标主表 sst_min_seq 整文件剪枝 src/storage/column_family/read.rs
  L292-295）；mem 存 (docid, seq)。**换值/删除的 remove 仍需 docid 粒度**（行级事件），
  但可复用"版本化活跃集"(§3.3) 的 del_seq 原语。
- demo 目标：量化 3 形态的 写放大 / 读快照过滤成本 / 编码膨胀；收敛到 1 个内核形态 + 段 v7。

### 3.3 版本化活跃集（live_docids v2，C）
`live_docids` 从"最新态位图"升级为 docid 级版本事件：
```
live_add(docid) @ seq；live_del(docid) @ seq（delete 即记；复活 = 新 add）
snapshot_live(S) = { docid | add ≤ S ∧ (del 不存在 ∨ del > S) }   // 与 view(term,S) 同构
```
统一成"**版本化 docid 集合**"原语：`versioned_set(added_seq, removed_seq) → view(S)`；
posting(term) 与 live 都是该原语实例 → 过滤、交集、计数、GC 全复用。
- 与 P1 MVCC 快照生命周期（active_snapshots 注册 + mvcc_keep_floor 保活）联动：
  版本化集合的旧事件仅在被活跃快照引用时保留，无快照后由 gc/合并收敛（防"864 万版本"堆积）。

### 3.4 写路径变化（成本大头，demo 先行验证）
- put/UPDATE：需要 **term diff**——旧值 terms ∖ 新值 terms 记 remove@seq、new∖old 记 add@seq
  （现 UPDATE 全量路径经 P89 管道已读旧行，可 diff；非索引列 delta 路径不涉 posting）。
- delete：docid 离开其所有 term 记 remove@seq —— 需 docid→terms 反查或写期记录（现状无，
  新增反向索引或把 remove 记到"版本化活跃集"统一处理，posting remove 由快照过滤推导）。
- flush/gc：mem (docid,seq) 升序编码入段 v7；gc 合并 = 多段版本化 posting 归并（同 key
  版本折叠：保留 add ≤ 最新 remove 的事件窗，无快照引用时直接折叠旧事件，段窗收敛）。

### 3.5 B：排序结论
- 磁盘 posting 保持位图/版本化数组形态即结构性有序；交集 = 位图 `&` 或游标双指针；
  不做独立"排序"项。收尾小修：**mem Vec<u64> flush 前按 docid 排序**（闭合 merge_distinct
  升序前提，随 P134/P135 顺手带掉）。

## 4. 快路径快照化接线清单（版本化落地后）
- 事务等值谓词 / 区间：候选 = view(term,S) ∩ snapshot_live(S) ∩ 窗口（免逐行复核）；
- P-GB/P-GB2/P121/count_distinct_fast/count_all_docs/count_docs_range/live_window_ids：
  口径从"最新态 live/posting"改为 view(S)/snapshot_live(S)，活跃快照下不再需要"回退扫描"
  守卫（或守卫改为快照口径比较）；
- `inverted_group_stats` / stats 载荷：按 view(term,S) 组内 docid 计数重算（防换值陈旧载荷）。
- zone_field_aggregate 的"活跃快照 → None"门禁在版本化后可按快照口径放行（可选，后续评估）。

## 5. D：倒排专项监控（随阶段埋点）
- `shanshui_inv_segment_count`、`shanshui_inv_gc_pending_bytes`（积压）、`shanshui_inv_delta_fst_over_bytes`、
  `shanshui_inv_mem_docids`、`shanshui_inv_posting_cache_{hit,miss}`、`shanshui_inv_flush_segments_total`；
- 版本化后：`shanshui_inv_*_seq_window`（每段 min/max seq 跨度）、无快照引用可收敛事件数、
  长快照窗（已有 active_snapshots/oldest）；SHOW MEMORY/STATUS 行集 + 诊断 demo。

## 6. 分阶段排期（P134 起）
| 项 | 内容 | 量级/依赖 | 验收 |
|---|---|---|---|
| **P134 事务读快照语义收口**（§2，先行） | 谓词候选去最新态删除预过滤（可见性交 txn_get）；scan_range_txn 去位图剔行 | ~1.5 天；P0-C 基建 | RR 谓词/区间与点查语义自洽；并发删/换值跨事务测试绿 |
| **P135 版本化倒排（主线 demo）** | §3.1-3.4 demo：形态甲/乙/丙 A/B + mem 排序收尾 + 段 v7 编码草案 | 依赖 P134；demo 先行 | 量化写放大/读收益/膨胀；选形态并出 v7 内核设计 |
| **P136 版本化活跃集 + 共享原语（C）** | §3.3 live v2 + versioned_set 原语 + 快照过滤接线 | 依赖 P135 形态定 | snapshot_live/view(S) 语义 = 权威扫描；守卫快路径快照口径 |
| **P137 倒排专项监控（D）** | §5 gauge + SHOW 行集 + 诊断 demo | 随阶段埋点收口 | 长快照窗/GC 积压/段窗可见；运维可判 |
| B 结论行 | 不做独立项；mem 排序收尾随 P134/135 | — | 已记录 |

## 7. 风险与迁移
- 段格式 v6→v7（posting 版本化/seq 旁路）需迁移与兼容读；v6 段按"无版本（add=0、remove=∞）"
  等价于任何快照均含 → 快照读自动回退行级复核兜底，可灰度。
- 写放大：term diff + remove 事件 + 反向索引 → demo 量化 vs 免回表收益。
- bitmap_fields 值位图 / stats 载荷同受陈旧值影响 → 一并版本化或声明为"仅当前态"路径并加
  快照门禁（二选一，随 P135 决策）。
- posting_cache 全清策略不变（段变更全清）；版本化后缓存项须带 S（快照无关不可缓存或按段窗缓存）。
