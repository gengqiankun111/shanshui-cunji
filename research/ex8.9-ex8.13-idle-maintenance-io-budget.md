# Ex-8.9 + Ex-8.13 设计：空闲感知维护调度 与 倒排后台 IO 预算共享（P3）

> 实现前设计。目标：维护从"写路径同步夹带"变为"空闲窗口 + 共享 IO 预算"的后台受控行为。

## 1. 现状与缺口
- L0 合并 = 写路径同步 auto_compact；L1→L2/L2 收敛已修"底部饿死"，但仍由写路径/调用方驱动，冷库维护随下次写入推进。
- 倒排 pending 前台刷段 / inverted_gc 手动；均不进 IO limiter。
- 删除 GC（Ex-8.7）经 delete 密度加权进 auto_compact；无空闲窗口推进。
- Ex-7.4 只按 MemTable 水位收窄列族合并 io_rate；无引擎级维护调度器、无统一 IO 账本（含倒排 seg）。

## 2. Ex-8.13：倒排后台 IO 预算共享（先做，小且正交）
- 记账点：倒排写盘仅 `inverted-{id}.seg`（pending 刷段 / GC 重写）。统一在写文件处记账（同 CF io_acquire：先占额后实账），暴露 `inverted_written_bytes`。
- 预算共享：抽 `io_budget` 小模块（原子配额+等待），主/倒排/组合/倒排 seg 共用 Ex-7.4 收窄后的同一预算池 → 前台写高峰自然压制倒排后台写。
- 前台/后台拆分：紧急刷段（mem_docids≥flush_threshold 或查询需要）走不受限前台配额（保留现语义）；仅 GC/空闲窗口刷段记账节流。
- demo A/B：持续前台写 + 手动大 inverted_gc，预算开关两组：前台 TPS/p99 不被侵蚀（p99 退化≤5%）、GC 收敛不劣化。

## 3. Ex-8.9：空闲感知维护调度器
- 空闲判定（每 ~100ms，全用现成代理，零新计数器）：写窗口增量 / MemTable 水位（Ex-7.4 pressure）；读计数窗口；compaction_urgency()、倒排 mem_docids、删除位图密度。
  三档：Busy（读写超阈或 L0 紧迫）→ 暂停低优维护（紧急项仍由写路径 auto_compact 兜底）；Normal → 小步长低优推进；Idle → 高优收敛追赶。
- 维护队列（每轮 ≤1 项、每项限 compact_input_max_mb 防长持锁）：① L0 收敛（非 Busy）② L1 沉底/L2 收敛 ③ 倒排 pending 后台刷段 ④ inverted_gc ⑤ 删除 GC（复用 compact_gc(gc_single)）。
- 线程模型：单维护线程绑 io 核（同 gc_thread 模式，stop flag join）；引擎读锁下执行（O 项：不阻塞读，与写互斥同 RwLock 串行）；进入前快检避免与写路径 auto_compact 重复争抢。
- 与 Ex-7.4/L 联动：维护每步经 CF io_acquire 记账 → 天然受收窄预算；Busy 暂停 = 高负载退避的另一半；动态 L0 阈值不变。
- demo A/B：交变负载（写/读突发 ↔ 空闲，构造 L1 积压+倒排 pending+删除密集）：A 现行为 vs B 空闲维护；验收 B 忙时读/写 p99 与 A 相当（±5%），空闲期收敛积压段、倒排 GC/删除 GC 完成。

## 4. 风险与边界
- 双入口（写路径 auto_compact + 维护线程）同 RwLock 写锁串行即正确；维护每 tick 先读锁快检防轮询开销。
- 先抽 io_budget 共享模块（SST 与 seg 共用），避免两套限速。
- 默认关闭（或仅 io_rate_limit_mb>0 启动），验收达标才默认开（P3 惯例）。
- **切片 2 前置约束（2026-09-04 实测发现）**：Engine 自身**无内部 RwLock**（锁在 gateway/server 层）；
  后台维护线程直接调 Engine::compact/flush_inverted 与 server 写锁并发不安全 → 切片 2 需先
  在 Engine 内建共享 RwLock（open 时初始化，写路径/查询经读锁、维护写锁），或维护线程由
  server 调度层在无写会话窗口触发（二者择一，切片 2 设计时定）。切片 1（io_budget 记账）已落地无此依赖。

## 5. 实现顺序（demo-first）
0. demo（src/demo/idle-maintenance）：公共 API 模拟"空闲窗口维护队列"（写突发 vs 空闲维护）验证收益 → 边界测试。
1. io_budget 模块 + 倒排 seg 写记账（Ex-8.13 最小切片）→ 单测：记账正确 / 限速节流 / 紧急刷段不受限。
2. 维护线程骨架（stop/join、三档判定、优先级队列）→ 单测：Busy 暂停 / Idle 追赶 / 不重复。
3. 接入 L1 沉底、倒排 pending 刷段、inverted_gc、删除 GC → 每项单测 + 交变负载 demo A/B。
4. 默认策略决策 + development_remain 回填 + 全量回归。
