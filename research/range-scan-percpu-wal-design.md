# 范围查询提速 + Per-CPU WAL 设计（2026-09-05，用户定稿）

> 决策摘要：**Partition Pruning 与 Parallel Scan 是“金矿”，要开发（非远期）**；块内索引暂缓、
> 重叠度并入压缩参数实验；**Per-CPU WAL 作为可选项默认开启，整合进排期但放到最靠后**。
> 对应排期：Task-025（剪枝/并行）、Task-026（Per-CPU WAL 最后，本文件为其设计档）。

## 一、范围查询提速四项判定

| 项 | 判定 | 理由 |
|---|---|---|
| Partition Pruning（分区剪枝） | **要开发（收益最高）** | LSM 多路归并结构下，范围查询须确认所有 SSTable；维护“分区范围统计缓存”（Zone Map 升级版，Meta/内存），执行范围查询先查缓存跳过不含目标数据的分区。业界（X-Engine/Cassandra）实证范围查询 +30~40%；对 #5（9.2×）/ #11（3.4×）意味着“不可用→可用”。侵入小：仅查询路由/元数据剪枝判断。 |
| Parallel Scan（并行扫描） | **要开发（规划/阶段 3，提前排）** | 单 SQL 单核；按 Key 范围切分任务并发扫描归并；X-Engine 已落地（聚合/导出响应短数倍）；16 核理论 4-8× 加速（时间换 CPU）。 |
| 块内索引 | 暂缓 | 收益较小，Data Block 已有块级稀疏索引 + Zone Map；细化收益有限。 |
| 重叠度控制 | 不单独立项 | 过度低重叠退化 B+Tree 全局有序、损写吞吐；属读/写平衡工程参数，随 Compaction 策略一并调参（既有 l1/l2_trigger_files 等）。 |

## 二、Per-CPU WAL（可选项，默认开启；排期最靠后 Task-026）

### 设计目标
- 吞吐：高并发（16 核+）写入 TPS +20~50%（vs 全局组提交；锁竞争消除）；
- 延迟：p50 持平/略降，p99 略升（队列调度微抖动）；
- 兼容：WAL 格式/恢复路径兼容两种模式；可观测（SHOW STATUS：队列深度/消费速率/积压）。

### 架构
- 写入入口 → 路由（当前 CPU 亲和 / 轮询 fallback）→ N 个 `ArrayQueue<WalEntry>` → N 个后台线程
  批量（组提交窗口）写各自独立 WAL 文件（`wal-{queue_id}-{gseq_start}.log`）→ 共享 MemTable →
  SSTable Flush。
- `PerCpuWal { queues: Vec<Mutex<WalQueue>>, writers: Vec<WalWriter>, enabled, num_queues,
  max_queue_depth, gseq: Arc<AtomicU64> }`；`WalQueue { inner: ArrayQueue<WalEntry>, depth:
  AtomicUsize }`。
- 路由：优先当前 CPU，超界/未绑核 → 轮询 `round_robin % num_queues`；关闭时回退全局模式（队列 0）。

### 全局序（gseq）与恢复（关键风险）
- 每条目分配全局递增 gseq；写盘前按 gseq 排序（跨队列最终写盘可交错）。
- 恢复：扫描所有 `wal-{queue}-{gseq}.log`，按文件名解析队列与 gseq 范围，**gseq 全局归并排序后
  回放**（同事务日志序 = 提交序）。
- 失败写跳过编号（洞）：Manifest `MAX(gseq)` 判定，未落盘的洞不回放；不影响日志连续性。
- 内存可见性：各队列独立刷盘，因 gseq 与全局序绑定，上层按 gseq 有序入 MemTable。

### 配置（[storage.wal]）
```toml
per_cpu_enabled = true            # 默认开启；false → 全局组提交（fallback_global_wal）
per_cpu_queues = 0                # 0 = CPU 核数（上限 64）
per_cpu_queue_depth = 4096        # 队列满背压（阻塞/503）
per_cpu_batch_window_us = 100     # 组提交 fsync 窗口
per_cpu_consumers_per_queue = 1
fallback_global_wal = true
```

### 实现排期（8.5 天）
1. 设计/接口（0.5d）：wal.rs 抽象 trait，Global/PerCpu 两实现；
2. PerCpuWal 核心（3d）：队列/路由/后台线程/gseq（Task 3 可并行）；
3. 恢复兼容（1.5d）：两模式重启恢复、旧 `wal-{seq}.log` 与新高命名共存识别；
4. 配置/热加载（0.5d）；5. 监控 SHOW STATUS（0.5d）；
6. 单测/集成（1d）：单/多队列、故障恢复、混合模式；7. 压测调优（1.5d）：16 核 Global vs PerCpu。

### 风险与缓解
- 恢复跨队列乱序 → gseq 全局归并回放 + Manifest `checkpoint_gseq`；
- 队列倾斜 → 轮询 fallback + 监控队列深度 + 可选 CPU 亲和；
- 内存积压 → `max_queue_depth` 背压；fsync 风暴 → 组提交窗口合并；
- 热切换 → 新旧命名共存，SHOW STATUS 显示当前模式。

### 收益量化（预期）
- 16 核 64 线程并发写：22 万 → 30-35 万 TPS（+36~59%）；p50 0.42→0.40ms 持平；p99 1.8→2.0-2.5ms；
- #17 update_id 1.18ms → 0.8-1.0ms（-15~32%：主要解锁竞争，fsync/写放大改善有限）。
- 注：#17 瓶颈 = WAL fsync + LSM 写放大；Per-CPU WAL 最大收益在高并发多线程。

### 最终判定
开发 ✅（排期最靠后，Task-026）；默认开启（`per_cpu_enabled=true`）、配置回退全局组提交；
监控 SHOW STATUS；与现架构无冲突（全局 gseq 语义沿用）。

## 三、一句话总结
Partition Pruning / Parallel Scan = 把范围查询从“扫全量确认”变为“剪枝 + 多核并行”的金矿；
Per-CPU WAL = 多核写入的无锁化管道，默认开启、可回退，高并发 +36~59%，排到所有 Task 之后实施。
