# 山水存迹数据库 v0.8.0 发布说明

> 发布日期：2026-09-02 · Git tag：`v0.8.0`
> 对比基线：v0.7.0 · 里程碑：M8 系列 + 排期大项 A~Z/AA~AC 全部完成 + J/K 倒排收尾

## 一、本版本亮点

1. **读路径无锁化三部曲（O 项，df16058 → 4585bb9 → e9f7d39）**：读 API `&self` 化 →
   引擎级 RwLock 读读并行 → ssts ArcSwap 原子发布 + 后台合并——1 亿库事务类
   **read_only 42 → 561 TPS（+13.3×）**、read_write 29.5 → 230 TPS（+7.8×），
   合并期间读读并行不阻塞、写互斥由 RwLock 保证；
2. **无锁合并根治（P72/P73，af24dbd + 3d58137）**：CF `sst_mutate` + MemTableBuffer RwLock
   + Engine Arc 化 + worker clone CF Arc 无锁合并——1 亿库合并期写 **25-43k rows/s 不塌陷**
   （修复前 8.2k-18.2k 分钟级阻塞）+ P73 manifest 竞态修复；
3. **高并发异步网络层（I 项，2802885）**：tokio 协程服务 + `spawn_blocking` 查询——
   **500 个 idle 连接仅 15 线程**（同步模式 500+ 线程），10k 长连接目标可行；
4. **HotCache 内部锁粒度化（7.72，9071984）**：整包 Mutex → 内部 RwLock + DashMap 无锁计数——
   点查热路径读读并行（demo：4 读线程纯读 **x4.16**、混合负载 **x5.42**）；
5. **倒排段 GC 后台化 + posting 分块延迟加载（J/K 项，b76dd40 + ed2588d）**：后台 GC worker
   信号驱动收敛段数（不阻塞查询、防丢失更新）；段格式 v3 posting 分块布局——分页只解码窗口
   容器：1600 万 posting 近页 **x211**、COUNT **x4491**、全量解码与 v2 持平、数据仅 +0.1%；
6. **分布式 SAGA 完整落地（Ex-2.5/13.5/13.6/13.7）**：网关 HTTP API + 补偿协议（中间态恢复 +
   超时屏障）+ 拓扑并行执行（depends_on DAG）+ 后台对账自动重试（指数退避）。

## 二、功能与变更明细

| 类别 | 内容 | 提交 |
|---|---|---|
| 读路径无锁化① | 读 API `&self` 化：SstReader + ColumnFamily 六扫描方法，delta Merge-on-Read 共享 | df16058 |
| 读路径无锁化② | 引擎读方法 `&self` + 内部 Mutex（HotCache/txn_locks/pending_inverted）+ mysql.rs `Arc<RwLock<Engine>>` 读写锁拆分 | 4585bb9 |
| 读路径无锁化③ | `ssts → ArcSwap<SstSnapshot>` 原子发布 + 后台 worker 读锁合并（合并不阻塞读） | e9f7d39 |
| 事件驱动 Compaction | 写路径自触发（L0 段数/大小阈值）+ 保底定时器 + 背压；1 亿库 78 L0 → 1 L1 | 24861c6 |
| 事务范围扫描 | BETWEEN/SUM 从逐 id 展开改为 `scan_range_txn` 一次快照扫描（read_only +68%） | d044b4c |
| 倒排批量回表 | `batch_get`：SST 按块分组只读/解压一次 + Delta 单次范围扫描 | d044b4c |
| 严格 MVCC | MemTable 版本链 + SST 多版本落盘 + 版本感知读（RR 快照未刷盘也一致） | e7a413a |
| 层/段 Zone Map | 点查每层 1 次范围粗筛替代逐 SST 二分+布隆（16 段点查 ≈ 单段） | 388a916 |
| 事务快照缓存 | RR 事务内同 key 二次读直达（256 项，提交/回滚即弃） | 0eca7a5 |
| 4KB 预读合并 | SstRangeIter 组读 4×4KB → 1×16KB | 85b9a62 |
| Compaction 优先级 | 跨列族紧迫度调度（L0×10+大小超限+8），每轮压最高档 | f09e9fb |
| Metrics | Prometheus 风格 /metrics（计数器 + 延迟对数直方图分层埋点） | 0257835 |
| io_uring 后端 | Linux SQPOLL 三队列（WAL/SST/倒排）+ 核预留；热路径接入（块读/fsync 走 SQPOLL）+ Debian 12 实测（2 核小机器写 -13% → 默认关） | f09e9fb / 956d9c3 |
| 写路径 syscall 修复 | needs_compact 每 put 3×fs::metadata 风暴 → sizes 快照缓存零 syscall（oltp_insert +795%） | 96ac6bc |
| 分批合并 | `compact_input_max_mb` 分批 L0 合并防长阻塞写 | 1763554 |
| 无锁合并根治 | worker clone CF Arc 无锁合并 + manifest 内存快照原子（P72/P73） | af24dbd / 3d58137 |
| 预处理读锁 | COM_STMT_EXECUTE 读语句走读锁 + 连接线程 512KB 小栈 | 97e3586 |
| 异步协程服务 | tokio serve_async + spawn_blocking 查询（连接 idle 不占 OS 线程） | 2802885 |
| HotCache 粒度化 | 内部 RwLock + DashMap 无锁计数（读读并行 / 写失效写锁） | 9071984 |
| 倒排 GC 后台化 | flush/gc `&self` + mutate 互斥（防 Manifest 丢失更新）+ worker 信号/兜底 | b76dd40 |
| posting 分块 v3 | 段格式 v2→v3 分块布局 + 惰性游标 k-way merge（分页窗口解码 / COUNT 精确去重，旧段兼容） | ed2588d |
| SAGA 网关 API | /saga/start /saga/status /saga/compensate + HttpStep | 781199e |
| SAGA 补偿协议 | 中间态恢复 + 超时屏障空转 + 缺步骤定义修复 | 170bf21 |
| SAGA 拓扑并行 | depends_on DAG 并行执行 | 71aa712 |
| SAGA 后台对账 | 后台对账自动重试（指数退避） | 71aa712 |
| 导出 Parquet | export --parquet（docid+json 两列 SNAPPY 分块） | 70c3b30 |
| 导出增量 | --incremental --checkpoint DocId 游标断点续传 | 2174531 |
| 导出流式管道 | Filter/Projection/Sink 分叉 + --rate-limit | c6b5417 |
| 导出 JDBC 直连 | MySQL wire 客户端建表 + 批量 INSERT 无文件落盘 | c6b5417 |
| 导出 MySQL 兼容 | --mysql-compatible LOAD DATA 配套 SQL + --mysql-max-varchar | 313bd81 |
| 导出建表 DDL | --dry-run-schema（ClickHouse MergeTree / MySQL InnoDB） | 313bd81 |
| 导出 IO 限流 | --io-rate-limit-mb 与 Compaction 共享后台 Token Bucket | 40e8abb |

## 三、性能实测（1 亿库 db-100m-v070，3308，v0.8.0 release）

### sysbench 全套（8 线程，对比 v0.7.0 基线）
| 测试项 | v0.8.0 TPS | 基线 | 对比 |
|---|---|---|---|
| oltp_point_select | 26,503 | 12,816 | **+107%** |
| select_random_ranges | 35,770 | 30,410 | +18% |
| oltp_delete | 39,551 | 16,239 | **+144%** |
| oltp_update_index | 30,147 | 13,255 | **+127%** |
| oltp_update_non_index | 19,461 | 8,896 | **+119%** |
| oltp_insert | 26,547 | 23,964 | +11% |
| bulk_insert | 226,950 | 210,895 | +8% |
| oltp_read_only | 526（uniform） | 550.6 | 持平 |
| oltp_write_only | 401（special） | 296.6 | **+35%** |
| oltp_read_write | 226（special） | 245.7 | 持平 |

### 关键指标（v0.7.0 后新增验证）
- **HotCache 读读并行**（demo A/B，4 读线程热 key）：纯读 80.6 万 → 335.6 万 qps（x4.16）、
  混合负载 x5.42；
- **posting 分块**（demo posting-chunk，16,666,667 posting）：近页 2.69ms → 12.8µs（x211）、
  COUNT → 600ns（x4491）、全量解码持平、数据 +0.1%；
- **io_uring**（阿里云 Debian 12 / 内核 6.1）：SQPOLL 三队列初始化 + 核隔离生效（3 线程绑核 0、
  业务核 1）；2 核小机器写 -13%、读持平 → 默认关，多核 NVMe 开启；
- **倒排端到端**（1 亿库，100 万行枚举字段）：写入 64,117 rows/s，分页查询 0.7~3.2ms。

## 四、质量数据

- **488 个单元测试全绿**（`cargo test`），较 v0.7.0（480）新增 8；
- 项目自身 **unsafe = 0**（`#![forbid(unsafe_code)]`；io-uring/mmap 白名单独立 crate）；
- 新增能力均有测试：RwLock 读读并行、ArcSwap 后台合并、无锁合并回归、MVCC 快照、
  分批合并、Worker 单轮收敛、倒排并发 flush/gc 无丢段、posting 分页一致性、跨段去重等。

## 五、兼容性与注意事项

1. **倒排段格式 v2 → v3（SEG_VERSION=3）**：posting 分块布局；**旧 v2 段自动兼容读取**
   （段文件头版本分发），老库无需重建；新段由 flush/gc 写入 v3；
2. **io_uring 默认关闭**（`io_uring_enabled=false`）：2 核小机器 SQPOLL 占核负收益；
   多核 NVMe 生产环境按 development.md 7.63/7.71 指引开启 + 预留 SQPOLL 核；
3. **后台倒排 GC 默认生效**：写路径刷盘后检测段超阈值自动置信号，mysql 服务后台
   worker 收敛段数（10 分钟兜底）；独立部署（demo/rpc）仍可显式 `inverted_gc()`；
4. 事务隔离级别：`SET TRANSACTION ISOLATION LEVEL` 支持
   READ UNCOMMITTED/READ COMMITTED/REPEATABLE READ/SERIALIZABLE（默认 RR）；
5. 倒排字段策略：枚举/低基数（<1K）建倒排、高基数唯一字段 `exclude_fields` 排除、
   长文本 `fulltext_fields` 分词（design_extension v0.2 第 9 章，建倒排字段 ≤20）。

## 六、构建与使用

```bash
cargo build --release                          # mimalloc（默认）

# MySQL 协议服务（1 亿库实测形态）
shanshui-cunji-mysql-server --data-dir D:/shanshui-data/db-100m-v070 \
    --config config.mysql-build-100m.toml --bind 0.0.0.0:3308

# 异步协程模式（10k 长连接目标）
shanshui-cunji-mysql-server --async --data-dir ... --bind 0.0.0.0:3308

# 倒排段 GC 后台化（[storage]）
# 写路径刷盘自动置信号；mysql 服务后台 worker 周期收敛，无需显式调用

# 倒排字段策略（[inverted]）
inverted_fields = ["status", "city", ...]      # 枚举/低基数字段白名单（≤20）
exclude_fields  = ["k", "c", "pad", ...]       # 高基数唯一字段排除
flush_threshold = 5000000                      # 倒排刷盘阈值（L 项二分收敛推荐）

# 导出（v0.8.0 全套：CSV/Parquet/增量/JDBC/流式管道）
shanshui-cunji-export --parquet --incremental --checkpoint ck.json \
    --filter "status=active" --project "id,status" --jdbc mysql://...
```
