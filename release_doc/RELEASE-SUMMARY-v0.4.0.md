# v0.4.0 发布说明摘要

**山水存迹数据库 v0.4.0** · 2026-08-28 · `git tag v0.4.0`（对比基线 v0.3.0）

## 核心亮点
- **环形 WAL（design 4.3）**：预分配环形文件 + 写指针循环（省文件扩展/inode 更新）；覆盖安全 + 两阶段 fsync 崩溃安全；`[storage] wal_mode="ring"` 可选
- **Leveled-Compaction（design 4.5 二期）**：SST 分层（Manifest 层号，旧库兼容）；L0→L1 / L1→L2 有界压实
- **MVCC 快照读（design 4.7 二期）**：`get_at(docid, snapshot_seq)` 历史版本快照读 + `begin_snapshot`
- **热点 key 自动缓存（design 14.1.2）**：访问计数自动晋升保护区，淘汰避让
- **增量备份（design 20）**：seq 游标增量备份 + 缺口检测 + 恢复重放

## 性能回归快检（2026-08-28，1000万）
| 场景 | 结果 |
| --- | --- |
| 批量插入 | **38.6 万条/s**（25.9s，10/10 通过）|
| 倒排词条 | 2.1s（250 万命中 + 20 万回表全对）|
| 结论 | M6 五项功能改动无性能回归（环形 WAL 默认关闭、Leveled 仅 compact 路径）|

## 质量
279 个单元测试全绿 · demo 冒烟 10/10 · 项目自身 unsafe=0（`#![forbid(unsafe_code)]`）

## 构建
```bash
cargo build --release                          # mimalloc（默认）
# 环形 WAL：config.toml [storage] wal_mode="ring"
```

## 证据存档
- `images/perf-0.4.0/`：1000万 回归快检报告 + console.log + report.html
- 完整发布说明见 [RELEASE-v0.4.0.md](./RELEASE-v0.4.0.md)
