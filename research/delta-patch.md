# P131 增量 UPDATE（单字段免整行重写）设计

> 状态：**路线重定（2026-09-06，用户选定：复用既有 Delta CF + 统一 MVCC/版本规则）**
> 历史：初案「value 内嵌 wrapper（`__sp_base/__sp_p`）」因 base+p 字节 ≥ 整 doc（写字节不减）暂缓，
> 机制 demo 保留作参考（src/demo/delta-patch）；**2026-09-06 内核集成改走引擎既有 Delta CF**
> （`Engine::patch` 字段级增量：docid++field 键、每 patch 自带全局 seq、`get_at` 已按 seq 过滤 = 增量天然 MVCC）。
> 触发：Task-005 110 万干净复测 #75 = 3.5×（SCC 10.5ms vs MySQL 3.01）；拆解 = 组提交常数 ~2ms +
> 定位/读现值 ~2-3ms + **整文档重写 + 全字段重索引 ~5-6ms**（③ 是增量 UPDATE 消灭目标）。
> 定位/常数不属本设计（见 P127/P89）。

## 0. 集成范围（2026-09-06 定稿，对应 kernel 改动清单 §10）

- **写**：dml.rs `update_response` —— `SET <单非索引列> = 字面量`（不含增量表达式、非 id/docid/doc、
  非倒排/组合/bitmap/fulltext/stats 声明列）→ 走 `Engine::patch_batch`（新增，免逐行组提交），
  索引列 / 整 doc 替换 / 表达式依赖旧值 → 既有全量路径（读旧值撤 posting + 整 doc 重写）。
- **读**：全值输出路径统一 Merge-on-Read（现状缺口：扫描族不合并 Delta，HTTP patch 后 SQL 扫不到）：
  scan_range / scan_stream / scan_stream_fields / scan_stream_with_zonepred / scan_stream_parallel /
  scan_range_paged / scan_after + txn `scan_range_txn`（RR 快照按 seq 门控）。
- **版本规则**：见 §11「统一增量 MVCC 版本规则」（用户 2026-09-06 约束：倒排索引与一切 RR 查询
  都必须 MVCC——增量只对 ≥ 其 seq 的快照可见）。

## 1. 目标

`UPDATE SET <非索引列> = v`（纯赋值、不依赖旧值）在文档模型下免整行重写：
写放大从 O(整行 25 列 × 全部索引) → O(1 列补丁 + 0 索引动作)。

收益场景：#75 的 `note='x9'`（非索引大列）、#73/#17-19 单列 update。

> ⚠️ §2~§9 为**已弃用参考**（value 内嵌 wrapper 初案；2026-09-06 用户选定 Delta CF 路线后不再采用，
> 保留供"增量折叠 vs 版本可见性"语义比对参考）。**现行设计见 §10/§11。**

## 2. 存储表示（value 内嵌 patch，不动主链 docid 唯一性）【已弃用参考】

```
存储 value（仍是单条 JSON 文档，单 seq 版本——MVCC 行版本机制零改动）：
  {"__sp_base": {…用户文档（上次折叠/初始态）…},
   "__sp_p":    {"note": {"v":"x9", "s":<写 seq>}, …}}     // 每列最多 1 条（同列覆盖，链长恒 ≤1）
```

- 保留键 `__sp_base` / `__sp_p`（serde_json 顶层），用户文档字段不含此前缀（写入路径转义检查，冲突 → 拒绝/回退全量）。
- **每列 ≤1 delta**：同列再次 SET → 读当前 wrapper → 直接覆写 `__sp_p.col.v`（不追加、不产生新列条目）→ 写回（seq 更新）。跨列各自 1 条。
- 读折叠（engine get/get_many/scan 输出路径）：`base` clone → 应用 `__sp_p` 各列 → 返回纯用户 doc（保留键剥离）。折叠成本 O(base 深度 + #patch 列)。
- 折叠态（无 `__sp_p`/无 wrapper）= 普通 doc，旧段直接兼容（**v6 格式不变**，补丁仅体现在 value 字节结构；compact 输出/读路径识别保留键即可——与 PAX 块 zone/布隆无关）。

## 3. 写入路径（dml update_response / SQL SET）

判定（列是否走 delta）：
- **非索引列**（不在 composite_indexes ∪ bitmap_fields ∪ inverted_fields ∪ stats_fields ∪ hot_fields 声明，且非 id/docid/doc）→ delta 化：
  1. 读现值（wrapper 或折叠 doc）；
  2. 若现值非 wrapper → 包 base + 单列 p；
  3. 若已是 wrapper → 覆写该列 p（或新增列键）；
  4. `doc_terms`：**wrapper 的倒排词条 = 折叠后文档的 term**（base+已应用 p）——索引保持"折叠态一致"（非索引列本无 term，故与 delta 前 term 集不变）；写 path 专用 wrapper_terms 函数（跳过保留键、对折叠结果取 terms）。
- **索引列 / doc 整体替换 / 多赋值含索引列** → 现有全量路径（读旧值撤 posting + 整 doc 重写折叠后落 wrapper 的 base 或纯 doc）。边界：一条 UPDATE 若同时改索引列与非索引列 → 整条走全量（保守，语句级粒度）。
- 自增/函数表达式（`SET k=k+1`，parse_increment_expr）依赖旧值 → 全量路径（暂不 delta）。
- 同值跳过写（P127 残余② 6fb2a07）在 delta 前先做字节比较（折叠后 == 旧折叠后 → 跳过）。

## 4. 读路径（折叠点）

engine 输出侧（get/get_many/scan_stream/scan_stream_keys 保持 keys-only 不解值）凡是**返回 value 字节**的：先 `fold_doc(bytes)`（非 wrapper 原样返回零拷贝）。点：
- engine get/batch_get/read.rs 读主链（primary）返回给 server 前。
- scan 值路径（scan_stream 已解码 value 的回调、get_many_pk_in_fields 子集读取——wrapper 下字段子集读取需先折叠再取列，或按列直读 base+若 p 覆盖该列取 p.v → 更优：子集直读避免整 doc 折叠，列为优化）。
- keys-only/计数/位图路径不受影响（不解值）。
- PAX/hot_fields 列存：wrapper 列若在 hot_fields（即索引列）→ 不走 delta（§3 边界），故列存路径只见折叠 doc（兼容）。
- Zone Map（块级 min/max 基于折叠后字节写块时生成？存储块内为 wrapper 字节 → zone 字段解析会取到 wrapper 结构 → 块写时 zone 提取需用折叠语义（写侧 zone 从折叠后 doc 抽）——落点：SstWriter 的 zone/列存编码在 value 入段前先 fold。折叠发生在 **flush/compact 编码前**更彻底（见 §5 折叠时机 A），则段内永不存 wrapper。

## 5. 折叠（fold-back）时机与 MVCC 窗口

核心：**段内永不存 wrapper**（避免所有 zone/列存/子集读路径感知 wrapper）。折叠点在 **flush 与 compaction 输出**：
- 折叠适用窗口 = 该 doc 版本的 `seq ≤ mvcc_keep_floor`（≤ 最老活跃 RR 快照）——折叠 = 应用 patch 产生**新纯 doc 版本**（折叠是逻辑应用；若按物理"改写字节"则新版本 seq 提升 → 破坏 ≤ floor 快照可见性）。
  正确语义：折叠**不改变版本可见性**——compaction merge 在选同 key 最大 seq 版本后，若该版本为 wrapper 且 `seq ≤ mvcc_keep_floor` → **输出 = 折叠后的 doc（保持原 seq 编码回写字节）**；若 `seq > mvcc_keep_floor`（仍有活跃快照可能读旧态）→ **原样保留 wrapper**（读路径折叠）。因此：
  - memtable / L0（近写，seq 高、可能 > floor）→ wrapper 驻留 → 读路径 §4 折叠。
  - L1+（compaction 输出，折叠发生在 merge 内对 ≤ floor 版本）→ 纯 doc。
  - 倒排/term 一致性：compact 折叠该版本时其词条 = 折叠 doc terms（写段时重建布隆/term，与既有 merge 重建一致）；memtable/L0 wrapper 期间倒排 = wrapper_terms（折叠语义，§3）→ 全链一致，P121/P-GB 载荷守卫无需改动（统计与折叠 doc 对齐）。
- GC/删除：墓碑 seq > patch seq → 链随 doc 版本作废（行版本机制既有，无新增）。

## 6. 与既有设施复用（勿重造）

- seq/快照/MVCC 行版本：已具备（单 seq 单 value）。
- compaction merge：在"输出回调"里对 wrapper 且 ≤ floor 调用 fold（一处）。
- txn 冲突/锁：不变（悲观 txn_locks / FOR UPDATE）。
- 同值跳过、LIMIT 早停、组提交：不变。
- 载荷守卫（term stats 折叠对齐后）：不变。
- 不需要 posting 条目级版本 / HLC / 分布式 seq（单机原子 seq）。

## 7. 工作量落点（kernel 集成清单，按序）

1. `src/sql/expr`? → 新增 `delta.rs`（server 或 engine 共享）：wrapper 构造/解析、`fold_doc`、`wrapper_terms`、`is_delta_column(cfg, col)`、转义检查。单元测试完备后；
2. dml.rs update_response：列判定 + wrapper 写路径（delta）/ 全量回退；
3. 读输出折叠点（engine get/batch_get/scan 值路径 + server 子集读优化可选）；
4. flush/compact 输出前 fold（≤ mvcc_keep_floor；保留 wrapper 若 > floor）；
5. SstWriter zone/列存编码仅见折叠 doc（由 4 保证）；
6. 回归 + #75 定向压测（目标：③组分 5-6ms → ≤1ms，#75 SCC mean 10.5 → ~5-6ms，3.5× → ~1.8-2×）。

## 8. 风险与守卫

- 用户文档含 `__sp_` 前缀键 → 写路径转义检查，冲突回退全量（宁慢勿错）。
- wrapper 字节比原 doc 大（base+p 两段序列化）→ memtable/段空间略增；由折叠（5）控制生命周期。
- 子集读（get_many_pk_in_fields）wrapper 下先 fold 再取列（正确性优先；性能优化留后）。
- 同语句多列 / 索引列混改 → 全量（保守）；增量演进。

## 9. Demo 验证点（src/demo/delta-patch，先行）【已弃用参考】

- ① 连续 SET 非索引列 → wrapper 每列 1 条、同列覆盖链长不增、折叠结果与全量重写一致；
- ② 多列 patch 应用顺序无关（列级覆盖）；
- ③ 折叠窗口：seq ≤ floor 折叠输出纯 doc（字节=全量等价）；seq > floor 保留 wrapper 且读折叠结果一致；
- ④ DELETE 后链随版本作废；复活新基；
- ⑤ wrapper_terms == 折叠 doc terms（索引一致）。

---

# 现行方案（2026-09-06 定稿）：Delta CF 字段级增量

## 10. 存储与写读（复用既有 Delta 子系统，零格式改动）

- **写**：`Engine::patch_batch(&[(docid, [(field, JSON value)])])` —— per-docid 字段键
  （key = 8B docid ++ VarLen 前缀 ++ 字段名）`delta.put_bytes_nosync`，批尾一次 `flush_wal`
  （per-CPU 下整批单 gseq 组）。每个字段键一次写 = 一个独立全局 seq 版本 → 版本链天然 MVCC。
  生效范围（dml 判定）：单字段 `SET col = 字面量`，col ∉ composite_indexes ∪ term_index_fields()
  （inverted/fulltext/stats/bitmap）且非 id/docid/doc；`SET col = NULL` / 自增表达式 / 整 doc 替换 /
  索引列 → 既有全量路径（读现值 → 整 doc 写回 + term/组合索引重建）。
- **读（Merge-on-Read）**：引擎所有**值输出**路径统一先查 Delta 覆盖再折叠（只对命中行 parse+合并，
  无覆盖直通原字节——P86① 短路同构，干净库零开销）；keys-only/计数路径不涉值不受影响。
- **折叠语义**（与 `get`/`get_at` 同构）：base JSON 对象 + 逐字段覆盖；`b"null"` 值 = 删除字段
  （shift_remove）；非 JSON base 原样返回。
- **索引一致性**：delta 列恒非索引 → 倒排 term/posting/载荷/bitmap/composite/cidx 全部零动作；
  声明列（含 bitmap_fields/stats_fields）禁止走 delta → 无"posting 陈旧"风险面。

## 11. 统一增量 MVCC 版本规则（用户 2026-09-06 约束：倒排索引与一切 RR 查询都必须 MVCC）

增量写入（Delta CF 字段键；语义同 wrapper 列增量的单列覆盖）服从**单一版本规则**：

1. **可见性 = 写版本 seq 门控**：快照 T 合并增量只取 `patch_seq ≤ T` 的最新版本；
   最新视图（T = ∞）取全部。倒排检索出的候选 docid 回表/后过滤一律走上述折叠读 →
   任一 RR 查询看到的行值恒为该快照点"base + 可见增量"合成态，与快照后写入/删除无关。
2. **多版本不坍缩**：同一字段键可跨源存在多版本（memtable 未刷 + SST 已刷）；快照读必须在
   **各源归并后按 `≤ snapshot` 取最大 seq**（用 CF `scan_stream_at` 语义），禁止先按最新坍缩再过滤
   （后者丢"最新补丁在快照后、旧补丁在快照前"的旧态——既有 `scan_raw_range_with_seq` 陷阱，修复于 P131）。
3. **折叠 = 读时合成，不产生新版本**：Delta 折叠发生在读侧内存，不改写主链、不提升任何 seq；
   因此折叠对快照可见性零影响（区别于 wrapper 案的物理折叠需保版本 seq）。
4. **GC/压实 = mvcc_keep_floor 门控**：primary/delta 各 CF compact 只回收 `seq < floor` 的旧版本；
   floor = 最老活跃 RR 快照（R4 既有机制）。增量旧版本与 base 旧版本同规则回收，无额外窗口。
5. **索引结构为"最新态候选 + 行级快照复核"模型**：term/posting 反映最新已提交写；候选集可含
   陈旧 docid（删除/复活/换值），精确性由 ① 折叠读 + WHERE 行级重算兜底（P-GB/P121 守卫同源）；
   增量列非索引 ⇒ 不改变 posting 形态，此模型不受增量扰动。声明列变更走全量重写（§10）维持模型。
6. **同值跳过**：SET 字面量 == 现值（折叠读比较）→ 不写增量（MySQL affected 0；免版本堆积）。

## 12. Kernel 改动清单（2026-09-06，P131 集成，按序）

1. engine/read.rs：`delta_overrides_range(_at)` + `fold_with_overrides(_fields)` helpers；
   修 `get_at` 快照合并改走 `scan_stream_at`（修多版本坍缩，见 §11.2）；
2. engine/scan.rs：scan_range / scan_range_paged / scan_after / scan_stream /
   scan_stream_fields / scan_stream_with_zonepred / scan_stream_parallel 接入折叠（无 delta 时零开销短路）；
3. engine/txn.rs `scan_range_txn`：RR 快照扫描增量按 snapshot 门控合并；
4. engine/write.rs：`patch_batch`（原子批量，批尾单次 flush_wal）；
5. server/command/dml.rs `update_response`：delta 判定分流（§10）；
6. 单测：patch 后扫描可见 / RR 快照旧值 / 倒排检索值一致 / 同值 affected 0 / 索引列回退全量；
7. 回归 + #75 值变形态压测回填（写链目标 ≤1ms 增量、mean 10.5 → ~6-7ms）。

