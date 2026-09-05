# Task-026 Per-CPU WAL 阶段2 设计细化（2026-09-05）

> 上级设计：research/range-scan-percpu-wal-design.md §二（8.5 天排期）。阶段1（配置/路由骨架，
> 默认关闭）已落地并提交（2843832）。本文档把 **阶段2（核心：N 队列独立文件写盘）与阶段3
> （恢复归并）** 细化到可直接开发的精度，供专门会话按步实现。
> 现状接触点（阶段0 研究结论）：WAL 内聚于各 ColumnFamily（`wal_handle()` = Mutex<WalBackend>，
> Append/Ring），记录格式 `Len(4)+CRC(4)+Payload(Seq u64, Op u8, Key, [Value])`；组提交 = 单后台
> 线程（open.rs start_group_commit）遍历 pwal/dwal 判 `sync_due` 后 `sync`；flush 后 append 模式
> `truncate_and_reset`；恢复 = CF open 读自身 wal 回放；engine 写路径 put/put_nosync/delete/
> delete_batch/patch/outbox 均以 `maybe_group_commit`/`flush_wal` 收口。

## 1. 目标与验收（阶段2）

- 写入口（组提交窗口内）把待持久化变更按当前 CPU 路由入队（阶段1 `PerCpuWal::route`），
  N 个后台消费线程各写**独立文件**并组窗口 fsync —— 打破"所有写 append 竞争单一 CF WAL 锁 +
  单线程 sync"。
- 恢复/格式兼容：既有库（旧 `wal.log`/ring）零迁移可开；`per_cpu_enabled=false` 完全回退现状。
- 验收：单测覆盖 单/多队列 写-刷-重启回放一致；跨 CF 原子组不撕裂；洞（失败未落盘编号）跳过；
  混合模式（旧文件 + 新队列文件共存目录）识别正确；全量回归 0 回归。

## 2. 架构决策：外置 engine 级队列 WAL（推荐 A）

现状每 CF 拥有 wal 的根因：LSM 各列族独立刷盘/恢复。Per-CPU 若要解除单一 append 锁，必须把
"哪个 CF、哪些 key"的持久化从 CF 内移出。两个候选：

- **方案 A（推荐）engine 级队列 WAL**：CF 不再自持磁盘 WAL；写路径把变更生成
  `WalEntry { cf, seq(gseq), op, key(bytes, CF 自身编码), value }` 入 engine 队列（memtable 应用
  仍在写路径同步做，与组提交延迟耐久语义一致：ack ≤ 窗口落盘）。队列线程批量写各自文件。
  恢复 = engine 按 gseq 归并回放分发到各 CF replay（幂等）。
- 方案 B（否决）仅把"fsync 调度"队列化、CF 文件不动：append 锁竞争仍在，达不到设计收益。

### 2.1 原子组（一次 SQL/API 写跨多 CF）

一次 `put_nosync` 可能同时改 primary（docid 主键）、cidx（组合索引键）、delta（清前缀）：
这些变更必须同一 gseq **同组**落盘/恢复，否则崩溃回放会撕裂。实现：
- 写路径先 `gseq = global_seq.fetch_add(1)`，随后对涉及的每个 CF 生成一条 `WalEntry`（共用该
  gseq），全部放入同一队列批次（同写线程 → 同队列 → 同文件追加段连续）；文件段内按 gseq
  连续即天然原子（跨队列不需要——单写线程串行提交，队列内顺序即提交序；多写者跨队列交错仅
  影响 fsync 时间点，恢复按 gseq 归并仍原子：同 gseq 的 N 条记录要么全部在（各队列都 fsync）
  要么整组跳过（checkpoint 判定以组为单位）。

### 2.2 文件布局

- 目录：`<data_dir>/percpu-wal/`（或 `[storage] wal_dir` 优先）。
- 命名：`wal-{queue_id}-{gseq_start:020}.log`；queue_id ∈ [0, queues)。`gseq_start` = 该文件首条
  记录 gseq（便于按名排序/裁剪）。记录体内仍带 gseq（校验 + 归并键）。
- 文件生命周期：写侧切段/后台裁剪；**checkpoint**：`min(各 CF 已刷盘水位)` 即全局安全点
  `cp`；所有 `gseq_end ≤ cp` 的段文件可删除（恢复只回放 `> cp`）。
- 与旧格式共存识别：目录内旧 `wal.log`（append）/ring 文件仅当 `per_cpu_enabled=false` 或
  **迁移期**（首次开启且发现旧文件）→ 先回放旧文件完成旧模式恢复，再把 engine 切到队列模式
  （一次迁移，见 §4）。文件名区分规则：旧 = 不含 `-queue-` 段 或 ring 前缀；新 = 含
  `wal-{q}-{gseq}`。

### 2.3 记录格式（沿用既有可靠性约定）

文件内条目沿用 `Len(u32)+CRC(u32)+Payload`（部分写入安全：CRC 坏/截断即止，同现有 WalReader）。
Payload 扩展为：
```
gseq u64（全局单调） | cf u8（0=primary,1=delta,2=cidx,3=outbox）| op u8 | key varlen | [value varlen]
```
（`seq` 即 gseq，弃用各 CF 各自 seq 写盘——恢复期回放时以 gseq 顺序应用，seq 语义由全局序
保证。若需保持 CF 内既有 seq 依赖（MVCC），可将 CF 原 seq 一并写入 varlen 前缀，恢复时还原。）

## 3. 队列/写线程（阶段2 核心改动清单）

1. `ColumnFamily` 增开关 `external_wal: bool`（open 参数，engine 决定）：true 时 `put_bytes_nosync`/
   `delete_record_mem` 等**不再 append 自身 WalBackend**，改为把 (op, key, value) 交给一个
   `&mut dyn FnMut` 回调（engine 在调用 CF 前注入 = 收集 WalEntry 到本写批次）；flush_primary/
   swap 后也不再 truncate 自身 wal（无文件）。
   - 侵入点集中在 CF 的写/恢复入口；memtable/SST 逻辑不动。
2. `PerCpuWal` 增字段（阶段2）：
   - `queues: Vec<QueueState>`，`QueueState { entries: Mutex<VecDeque<EntryBatch>>, file: 写句柄,
     condvar, stop, depth_now, consumed }`（无锁优化后续：ArrayQueue + 每队列专用文件句柄）。
   - 后台线程 ×queues：`spawn_per_cpu(cfg)` 启动；每窗口 `per_cpu_batch_window_us` 取空队列 →
     按 gseq 排序（队列内天然有序，仅校验）→ 写文件 + fsync；深度/积压进 `depth_now/consumed`。
   - 关闭（Drop/`stop`）：线程 flush 残余后退出（对齐组提交兜底线程语义）。
3. 写路径入口（engine/write.rs）：`maybe_group_commit`/`commit_persist` 改判：
   - `per_cpu_wal.enabled` → 本写批次（写路径已攒 `Vec<WalEntry>`）路由到队列（route(current_cpu)）
     并**立即返回 ack**（≤ 窗口由队列线程落盘）；`flush_wal()`（强安全/档位1）→ 显式 drain 全
     队列并 sync（与现状 flush_wal 语义对齐）。
   - 组提交线程（open.rs）在 enabled 时停用（队列线程替代）；disabled 走原逻辑（零回归）。
4. checkpoint 推进：`flush_primary`/`switch_and_flush`（CF flush 完成，memtable → SST，WAL 水位
   前移）→ engine 更新该 CF 已刷水位；`cp = min(水位)` → 后台按 cp 裁剪段文件 + manifest 持久化。

## 4. 恢复（阶段3）

1. open：`per_cpu_enabled=true` 时扫描 `percpu-wal/`：
   - 存在旧 `wal.log`/ring → 先按旧模式完成恢复（回放进各 CF），随后 truncate/rename 旧文件为
     `.migrated`（或删除），此后全部用队列文件（一次性迁移，记录到 manifest）。
   - 解析所有 `wal-{q}-{gseq_start}`：文件名得 (q, 区间)；manifest `checkpoint_gseq=cp` 提供安全点；
     丢弃 `gseq ≤ cp` 段文件（可整删）；对剩余记录按 **gseq 全局归并**（跨队列 k-way，复用
     scan 归并思路）逐组分发回各 CF replay（幂等：以 gseq 覆盖；同 gseq 多 cf 记录一组原子）。
2. 洞跳过：写失败/未 fsync 组 = gseq 空洞；恢复以 manifest cp 为界：≤ cp 不回放；> cp 的段
   内偶发缺号（尾部截断文件）→ 该文件读至坏记录即停（沿用 CRC 约定），后续更大 gseq 文件
   不存在（单点 fsync 序）→ 无跨文件前向洞。**恢复一致性 = 与现状"读到坏记录即停"等价**。
3. `global_seq` 续接：现由 CF wal 头 next_seq/manifest 恢复；队列模式改由 manifest
   `checkpoint_gseq` + 最大段 gseq_end 续接（`max(读取到 gseq)+1`）。

## 5. 配置联动（阶段1 已就绪）

`per_cpu_enabled`（默认 false→核心回归全绿后翻 true）/`queues`(0=auto ≤64)/`depth`/`batch_window_us`
全部已解析于 `PerCpuWal`。阶段2 仅在 enabled 时创建线程/队列文件；false 路径代码零改动。

## 6. 测试计划（每步单测先行）

1. WalEntry 编解码 + CRC（坏尾截断、跨 cf 组、空洞）。
2. 单队列：写→（窗口/显式 flush）→drop→重开回放 = 全量一致；多队列：4 写线程并发 put →
   归并回放结果与串行语义一致（含同 key 覆盖序按 gseq、跨 CF 组原子）。
3. checkpoint：flush 后重开不回放已刷记录（幂等不重复、不丢未刷）。
4. 混合/迁移：先旧模式写库 → enabled=true 重开 → 迁移回放正确且旧文件清理。
5. 回退：enabled=false 全量既有测试不变（现默认即此）。
6. 关闭 Drop：残余 flush。
7. 全量回归 + 压测（16 核 Global vs PerCpu，回填数值——结果内联进 dev_remain，不落 results 目录）。

## 7. 实现顺序（每步可提交）

- 2a：WalEntry 类型 + 编解码 + 队列文件 writer/reader（独立于 CF，纯新增模块 + 单测）。
- 2b：CF `external_wal` 注入点（写收集回调 + flush 水位回调），保持 false 默认行为逐字节不变
  （对照既有 CF 测试）。
- 2c：PerCpuWal 后台线程 + engine 写路径接线 + checkpoint 裁剪（enabled=true 单测同 6.2/6.3）。
- 3a：恢复归并 + 迁移 + gseq 续接（6.4 混合测试）。
- 3b：翻默认 true + 全量回归 + 压测回填（P# 文档收口）。

> 风险护栏：任何一步不回退旧默认行为；持久性改动一律"先单测后接线，默认关闭提交"。
