# 山水存迹数据库 v0.4.0 发布说明

> 发布日期：2026-08-28 · Git tag：`v0.4.0`
> 对比基线：v0.3.0（ad59d13）· 里程碑：M6 高性能写入模式 + 深度优化（阶段 3 末）

## 一、本版本亮点

1. **环形 WAL（design 4.3）**：预分配固定大小文件 + 写指针循环移动（省文件扩展与 inode 元数据更新），
   覆盖安全（仅覆盖已刷盘记录，Flush 后上报游标）+ 两阶段 fsync 崩溃安全；`[storage] wal_mode="ring"` 可选开启；
2. **Leveled-Compaction（design 4.5 二期）**：SST 分层压实——Manifest 层号（旧库兼容），
   L0→L1 / L1→L2 有界压实（单次压实量 = 刷盘批次），限制大合并瞬间 IO 打满；
3. **MVCC 快照读（design 4.7 二期）**：`get_at(docid, snapshot_seq)` 按 seq 快照读历史版本
   （Tombstone 保留、删除前快照可见）；`begin_snapshot` 快照点；
4. **热点 key 自动缓存（design 14.1.2）**：访问计数达 `hotcache.hot_threshold` 自动晋升保护区，
   普通淘汰避让，热点不被冷数据挤掉；
5. **增量备份（design 20）**：seq 游标增量备份（WAL 记录导出，缺口检测提示全量备份）+ 增量恢复重放；
6. **性能回归快检**：1000万 10/10，插入 38.6 万条/s，M6 全部功能改动无性能回归。

## 二、功能与变更明细

| 类别 | 内容 | 提交 |
|---|---|---|
| 环形 WAL | `RingWal` 预分配环形文件 + 写指针循环；覆盖安全（flushed_seq 游标 + WalFull 强制 Flush）；两阶段 fsync；`[storage] wal_mode`/`wal_ring_size_mb`；`WalBackend` 统一分发 | 66813c9 |
| Leveled-Compaction | Manifest `levels` 层号（旧 Manifest 全 0 兼容）；`select_compaction_inputs` 有界压实 L0→L1/L1→L2；`needs_compact` 分层判定；`CompactReport.out_level` | 4c2e17a |
| MVCC 快照读 | `Engine::get_at(docid, snapshot_seq)` + `begin_snapshot`；`ColumnFamily::get_bytes_at`（seq ≤ 快照点最新版本）；`Engine::flush_primary` | 07f556e |
| 热点缓存 | HotCache 保护区（容量 1/5）+ 访问计数自动晋升 + LFU 淘汰避让；`protected_len`/`promotions` | e34ea87 |
| 增量备份 | `backup_incremental(since_seq)`（WAL 记录导出 + 缺口检测 + 原子落盘）+ `restore_incremental` 重放（PUT 重派生词条 / DELETE 墓碑）；`WalRecord` Serialize | 266d03d |
| 收尾 | 1000万 回归快检 10/10 + 报告 `images/perf-0.4.0/` | b16d511 |

## 三、性能回归快检（2026-08-28，1000万）

| 指标 | v0.3.0 | v0.4.0 | 变化 |
|---|---|---|---|
| 批量插入 | 29.6s（33.8 万条/s）| **25.9s（38.6 万条/s）** | +14%（波动）|
| 倒排词条 | 2.4s | 2.1s | 持平 |
| 备份·还原 | 26.8s | 20.7s | +23%（波动）|

结论：M6 五项功能均为功能层增强（环形 WAL 默认关闭、Leveled 仅影响 compact 路径、MVCC/热点/增量备份为新增 API），读写热路径无回归。

## 四、质量数据

- **279 个单元测试全绿**（`cargo test`），demo 冒烟 10/10；
- 项目自身 **unsafe = 0**（`#![forbid(unsafe_code)]`）；
- 新增能力均有测试：环形 WAL 回环/回绕/崩溃恢复、Leveled L0→L1/L1→L2/层号持久化、MVCC 快照读
  历史版本/删除前快照/快照后写入隔离、热点晋升/冷数据挤压存活、增量备份断点+还原/缺口检测。

## 五、构建与使用

```bash
cargo build --release                          # mimalloc（默认）

# 环形 WAL（design 4.3 高性能模式，默认 append）
[storage]
wal_mode = "ring"                              # append / ring
wal_ring_size_mb = 64

# MVCC 快照读（SDK）
let snap = engine.begin_snapshot();            # 快照点
let doc  = engine.get_at(docid, snap)?;        # 快照读

# 增量备份
engine.backup_incremental(since_seq, "incr.json")?;   # 备份
engine.restore_incremental("incr.json")?;             # 恢复
```
