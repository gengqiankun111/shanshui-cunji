# P131 delta patch（单字段 UPDATE 免整行重写）设计

> 状态：立项（2026-09-06，用户选定方案：**每列 ≤1 delta + MVCC seq 管理**）
> 触发：Task-005 110 万干净复测 #75 = 3.5×（SCC 10.5ms vs MySQL 3.01）；拆解 = 组提交常数 ~2ms +
> 定位/读现值 ~2-3ms + **整文档重写 + 全字段重索引 ~5-6ms**（③ 是 delta patch 消灭目标）。
> 定位/常数不属本设计（见 P127/P89）。

## 1. 目标

`UPDATE SET <非索引列> = v`（纯赋值、不依赖旧值）在文档模型下免整行重写：
写放大从 O(整行 25 列 × 全部索引) → O(1 列补丁 + 0 索引动作)。

收益场景：#75 的 `note='x9'`（非索引大列）、#73/#17-19 单列 update。

## 2. 存储表示（value 内嵌 patch，不动主链 docid 唯一性）

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

## 9. Demo 验证点（src/demo/delta-patch，先行）

- ① 连续 SET 非索引列 → wrapper 每列 1 条、同列覆盖链长不增、折叠结果与全量重写一致；
- ② 多列 patch 应用顺序无关（列级覆盖）；
- ③ 折叠窗口：seq ≤ floor 折叠输出纯 doc（字节=全量等价）；seq > floor 保留 wrapper 且读折叠结果一致；
- ④ DELETE 后链随版本作废；复活新基；
- ⑤ wrapper_terms == 折叠 doc terms（索引一致）。
