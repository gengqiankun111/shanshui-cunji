# Group Commit 组提交方案调研与设计（M8-P0）

> 调研日期：2026-08-29 · 背景：M7-3 YCSB 压测定位「fsync 单条串行」为写路径头号瓶颈（55×）
> 关联设计：design 4.3「批量组提交」规划（表 4-1 预测组提交单条写 22 万 TPS @ 0.9ms）
>
> **状态：✅ 已实现并验证（2026-08-29）**——采用第 4 节方案 A（提交器模式），实测 A 写重
> 2ms 窗口 91,296 ops/s（基线 2,003 → **45×**，P50 7.8µs），1ms 窗口 75,330 ops/s（37×）。
> 详见 `images/perf-0.5.0/验证记录.md` 附录 A 与 development.md 7.7。

## 1. 现状与瓶颈（代码事实）

- **写路径**：`Engine::put` = `put_nosync`（WAL append + MemTable + 倒排 + HotCache）→ `flush_wal`（`primary.sync_wal()` + `delta.sync_wal()`）——**每次 put 都做一次 fsync，且 primary/delta 两次**。
- **WAL 层已具备组提交雏形**：`WalWriter::append_at` 只攒 `buf`（不写盘），`sync` 一次整批写盘 + fsync；`perf_mode` 攒 4MB 才 fsync。**瓶颈在调用频率**（每条都 `sync`），不在 WAL 层能力。
- **并发模型**：全库写经 `Arc<Mutex<Engine>>` 串行（RPC 每连接一线程，写锁全局互斥）——写天然串行，所有并发写互相阻塞在锁内的 fsync 上。
- **实测**（i7-10750H / NVMe，M7-3）：A 写重带 fsync 2,077 ops/s vs 无 fsync 113,587 ops/s（**55×**，~480µs/fsync）。

## 2. 业界方案对标

| 系统 | 机制 | 关键参数/结构 |
| --- | --- | --- |
| **MySQL InnoDB** | 组提交三阶段（flush→sync→commit），同批同 stage 推进 | `innodb_flush_log_at_trx_commit` = 1（逐条）/ 2（每秒一次）/ 0（关闭）|
| **PostgreSQL** | 提交攒批 + 窗口延迟 | `synchronous_commit`（on/off）+ `commit_delay`（µs 窗口）+ `commit_siblings`（≥N 个并发才延迟）|
| **RocksDB** | **WriteThread leader/follower**：并发 writer 入队（无锁链表头插），Leader 合并整组一次 WAL 写 + fsync，Followers 并行写 MemTable 后一起 ack | `WriteGroup`（leader + last_writer 链表）+ `STATE_*` 状态机 |
| **RocksDB #14627（2026 提案）** | **无 leader 演进**：ping-pong 双缓冲 WAL，fsync 移出互斥区（O(1) 换缓冲），writer 各claim LSN 字节区间并行 memcpy，无锁水位线（InnoDB Link_buf 式）发布可见性 | 32 核纯写 +50% 吞吐、CPU 利用 20→26 核 |
| **Speedb** | 专用写线程 + 双容器换批 + 定址并行 WAL 写 | DB mutex → RW lock |
| **ScyllaDB** | Seastar commitlog（每 shard 独立日志 + 组提交写）| shard-local |
| **SQLite** | WAL checkpoint 攒批提交 | checkpoint |
| **HenryDB（实测）** | 立即 fsync 53 TPS → 5ms 批 3,704 TPS（**70×**）| batch 模式 |

**共识**：fsync 成本 ~0.5-18ms 不等（NVMe 消费级 ~0.1-1ms）；组提交 = 「一个窗口内的事务共享一次 fsync」，吞吐随并发近似线性增长，提交延迟增加 ≤ 窗口时长。**fsync 仍是提交点，只是时机被摊销——不牺牲耐久语义**（窗口尾部未 fsync 数据在崩溃时丢失，属于显式可配置的延迟耐久）。

## 3. 方案选型（结合本项目现状）

| 方案 | 复杂度 | 适配现状 | 收益 | 风险 |
| --- | --- | --- | --- | --- |
| **A. 时间/字节窗口提交器（推荐）** | 低 | 高（Mutex 串行天然攒批，无并发 writer 也不需要 leader/follower）| 高（10-30×）| 窗口内未 fsync 数据崩溃丢失（可配置）|
| B. Leader/Follower（RocksDB 式）| 高 | 低（写已全局串行，无并发 writer 可合并）| 中 | 复杂度高但收益被 Mutex 抵消 |
| C. 双缓冲异步 fsync（#14627 式）| 高 | 低（需重构写路径为并发写）| 高 | 工程量大，留待阶段 3 并发写路径 |
| D. 纯调用方攒批（put_nosync+手动 flush）| 最低 | 中（仅适合导入类，网关写路径不适用）| 中 | 要求调用方配合，语义外露 |

**推荐 A**：与现状（单写者 Mutex 串行）最贴合——Mutex 已经把所有 writer 串成一个队列，`put` 只需「攒进 WAL buf 后按窗口/字节阈值决定是否 fsync」，一次 fsync 天然覆盖窗口内所有 put。代码改动小、收益直接来自 fsync 次数降低 N 倍。B/C 在引入真正的并发写路径（无锁/多写者）时再升级。

## 4. 设计（方案 A 详细）

### 4.1 配置（`StorageConfig` 新增）

```rust
/// 组提交窗口（µs）：0 = 关闭（保持逐条 fsync 强安全，默认）；
/// >0 = 窗口内所有写入攒批，一次 fsync 覆盖（延迟耐久 ≤ 窗口）。
pub group_commit_us: u64,        // 默认 0（关闭）
/// 组提交字节阈值：WAL 待刷缓冲 ≥ 此值立即 fsync（不受窗口等待）。
pub group_commit_bytes: usize,   // 默认 256KB
```

### 4.2 写入路径改造（最终实现：提交器模式）

```
put(docid, value, terms):
  put_nosync(...)                      # WAL append 到 buf + MemTable + 倒排 + HotCache（不变）
  maybe_group_commit()                 # 关闭模式 → flush_wal()（现状，逐条 fsync 强安全）
                                       # 开启模式 → 写路径零 fsync，直接返回
                                       #            （ack 后最多延迟 ≤ 窗口落盘）
                                       # 落盘统一由【后台提交线程】完成：
后台线程 loop:
  sleep(tick = min(窗口, 10ms))
  for primary/delta WAL:
    if pending > 0 且 sync_due(窗口或字节阈值):
      sync()                           # 一次 fsync 覆盖窗口内全部写入
```

- `flush_wal()` 保持强制语义（备份/导入/drop/显式调用用），内部 primary+delta 各一次 `sync`；
- **后台提交线程**：Engine 打开时 spawn（持有 `Arc<Mutex<WalBackend>>` 共享句柄 + 停止标志，
  不触碰 Engine 本体）；`drop` 时置停止标志、join、最终 `flush_wal`（不丢窗口尾部）；
- **不采用「写路径窗口判定 + 后台线程兜底」双触发**：首版实测双份 fsync + 锁竞争反而更慢
  （A 写重 1,176 ops/s < 基线 2,003）——单一提交器后达 91K（见 problem_solving P37）。

### 4.3 与既有机制交互（逐一核对）

| 机制 | 影响 | 处理 |
| --- | --- | --- |
| **环形 WAL 覆盖安全** | 组提交只是延后 fsync 时机 | `WalFull` → `ensure_wal_room`（先 sync 腾空，必要时强制 Flush）路径保留，天然兼容 |
| **MVCC 全局 seq** | seq 在 append 时分配（进 buf），fsync 延后不影响单调性；快照读读 memtable（已内存）| 无影响 |
| **增量备份 `wal_records_since`** | 读 WAL 文件可能含未 fsync（page cache）记录，崩溃后可能丢失 → 备份与恢复不一致 | `backup_incremental` 前置 `flush_wal()`（强制落盘后再导出）|
| **崩溃恢复** | 文件尾部未完整/未 fsync 记录截断丢弃 | 现有截断逻辑不变，语义 = 丢最近 ≤ 窗口的数据（组提交开启时显式接受）|
| **OOM Guardian / Watchdog** | 不涉及 fsync 时机 | 无影响 |
| **demo / import 导入** | 走 put_nosync + 显式 flush | 不变 |

### 4.4 收益预估（本机实测外推）

| 配置 | 预计写吞吐（A 写重） | 依据 |
| --- | --- | --- |
| 现状（逐条 fsync）| 2,077 ops/s | 实测 |
| 组提交 1ms 窗口 | 2-5 万 ops/s | ~每批 2-5 条共享一次 fsync |
| 组提交 2ms 窗口 | 5-10 万 ops/s | ~每批 5-10 条；design 表 4-1 预测 22 万 TPS（更强机器）|
| 无 fsync（上限参照）| 113,587 ops/s | 实测 |

### 4.5 实施步骤

1. `StorageConfig` 增加 `group_commit_us` / `group_commit_bytes`（默认关闭）；
2. `WalWriter` 增加「距上次 sync 时间 + 待刷字节」跟踪与 `sync_due(now)` 判定；
3. `Engine`：`put`/`delete`/`patch` 改走 `maybe_group_commit`；spawn 后台提交线程（窗口兜底）；`drop` join + 最终 fsync；
4. `backup_incremental` 前置 `flush_wal`；
5. 测试：窗口触发 / 字节阈值触发 / 后台线程兜底 / 崩溃丢窗口语义 / 环形 WAL 交互 / 重启恢复 / 跨列族 seq 连续；
6. YCSB 重跑 A 写重验证（目标 ≥ 5 万 ops/s），记录对比数据。

## 5. 风险与权衡

- **耐久语义**：组提交开启时 ack ≠ 已 fsync，崩溃最多丢 ≤ 窗口（默认关闭保持强安全；开启需显式配置）。对齐 design 4.3「强安全模式默认，高性能模式可选」。
- **延迟**：单条提交延迟增加 ≤ 窗口（1-2ms），换吞吐 10-30×。
- **不做**：leader/follower 与无锁双缓冲（B/C）——当前 Mutex 串行模型下收益被锁抵消，留待并发写路径（阶段 3）。

## 参考来源

- MySQL：`innodb_flush_log_at_trx_commit` + binlog 三段组提交
- PostgreSQL：`synchronous_commit` / `commit_delay` / `commit_siblings`（PG 文档 WAL Configuration）
- RocksDB：`db/write_thread.h`（WriteThread leader/follower 状态机）
- RocksDB #14627（2026-04）：无 leader 写路径提案（ping-pong 双缓冲 + Link_buf 水位线）
- ScyllaDB commitlog / SQLite WAL checkpoint / Speedb write-flow 文档
- HenryDB fsync tax 实测（5ms 批 70×）；M7-3 本机压测（images/perf-0.5.0/验证记录.md）
