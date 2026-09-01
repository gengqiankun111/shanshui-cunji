# 开发流程顺序（development_process_order）

> 本文件是**开发路线图的唯一入口**：每个开发会话/任务的第一步 = 读取本文件，
> 确定「下一步开发内容」与优先级，再进入项目标准工作流（development.md）。
> 大项完成后回填状态并重排优先级。

## 1. 流程约定

1. **第一步（固定）**：读取本文件 → 定位当前 `P0/P1` 大项 → 明确本次开发内容；
2. 大项粒度 = 可独立交付的功能/性能/质量项；子任务拆分见 development.md 里程碑；
3. **优先级评估准则**：`影响（业务/性能收益）` × `成本（工作量）` × `依赖（前置阻塞）`，
   得分为 P0（立即）/ P1（近期）/ P2（中期）/ P3（远期）；
4. 完成大项 → 回填「已完成」区 + 更新 development.md / feature.md / problem_solving.md。

## 2. 大项队列（按优先级）

| 编号 | 大项 | 优先级 | 状态 | 排期说明 |
|---|---|---|---|---|
| A | 本机 2000 万 / 5000 万性能吞吐基准（demo 13 项放大） | P0 | ✅ 完成 | 2026-08-30，13/13 全绿（images/perf-0.6.0/ 汇总报告）；查询次数：主键/HotCache/分片/删除 100 万、倒排检索 1 千+COUNT 1 万、fulltext 1 千、SQL 1 千+amount/ts BETWEEN 各 100 |
| B | 本机 1 亿数据性能与吞吐测试 | P0 | ✅ 完成 | 2026-08-31：sysbench 标准 MySQL 语句构建 1 亿行（25.6min，~6.5 万行/s，10.8GB）+ 11 项 oltp 全套 mean/max + P0 优化复测（见 images/perf-0.7.0/sysbench-100m/构建记录.md） |
| C | 分布式吞吐优化（机制已验证 → 性能） | P1 | ✅ 完成 | ① 网关分片并行（每线程独立 Gateway）② RPC 批量写入（shard.put_batch + Gateway::put_batch 哈希分组）③ 节点组提交（--group-commit-us）；本机两节点 364k w/s + 跨地域 0.5s/21.6k w/s（vs 1074s，~2100×） |
| D | LSM 事务阶段一：WriteBatch 原子写 | P1 | ✅ 完成 | WriteBatch（攒批 put/delete）+ Engine::write 原子提交（预校验失败零副作用 = 回滚语义）+ 单次 flush_wal 崩溃原子 |
| E | 事务阶段二：快照隔离（Snapshot/MVCC） | P2 | ✅ 完成 | Transaction（快照 seq 一致读 get_at + 提交时写写冲突检测，last_write_seq > snapshot → TxnConflict abort）；MVCC 已知局限：MemTable 不保留多版本，快照读需旧版本已落 SST |
| F | 事务阶段三：完整 ACID 与隔离级别 | P2 | ✅ 完成 | Isolation（RC/RR/SERIALIZABLE）+ docid 级锁表（共享读/排他写 + 2PL 升级）+ wait-for 图死锁检测（victim abort）+ 失败路径锁释放 |
| G | 倒排 posting 检索优化 | P2 | ✅ 完成 | c380792 LRU 缓存 + 白名单内存位图；补充：段数据 mmap 化（K 项落地）——免 fs::read 全文件读取，FST offset 直接切片反序列化，物理页按需加载（P23 白名单） |
| H | MySQL 协议适配（MySQL 生态接入） | P1 | ✅ 完成 | H-1~H-3 ✅（握手+认证+SQL 映射）+ H-4 事务语句 ✅（BEGIN/COMMIT/ROLLBACK → txn.rs，同事务读可见）+ H-5 预处理语句 ✅（COM_STMT_PREPARE/EXECUTE）+ H-6 sysbench 接入 ✅（多列 INSERT 映射 + DDL 放行 + 负载模拟：prepare 945 w/s / 并发点查 3040 q/s / 事务 1744 txn/s）；mysql cli 8.0 / pymysql 真实连接全链路通过 |
| I | 高并发查询优化（design 9.5 目标） | P3 | ✅ 完成 | 同步模型（97e3586：COM_STMT_EXECUTE 读语句走读锁 + 连接线程 512KB 小栈）+ **异步协程运行时**（2802885：tokio 网络层 serve_async + spawn_blocking 查询，连接 idle 不占 OS 线程——500 idle 连接仅 15 线程，10k 长连接可行）；480 全绿；详见 development.md 7.69/7.70 |
| J | 倒排段 GC 后台化 | P2 | ⏳ | 当前 gc() 需显式调用（demo 插入后合并）；后台线程周期触发（设计已有，工程化） |
| K | fulltext 大 posting 反序列化优化 | P2 | ⏳ | 5000 万库 content 词 posting ~1600 万，首次反序列化 ~100ms+；候选：段内 posting 分块延迟加载 |
| L | Compaction 智能调度（合并冷却 + 动态窗口 + 倒排阈值） | P1 | ✅ 完成 | 28eae9d：①合并冷却（compaction_cooldown=2，软约束防收敛死循环）②动态窗口（l0_stall_min/max=8/16，写压力驱动）③倒排刷盘阈值参数化（flush_threshold，二分收敛推荐 500 万：段数 -80%、吞吐仅 -6%）；+demo 13 项单条 avg/max 统计 |
| M | 事务类查询优化（范围查询改一次扫描） | P0 | ✅ 完成 | 1 亿库 sysbench 实测：oltp_read_only 42 TPS / read_write 29.5 TPS（点查类 5k-7k TPS）。根因：BETWEEN 范围/SUM 聚合被展开为**逐 id 独立 txn_get**（每事务 ~700 次 LSM 点查）。方案：范围查询走 `scan_range_at`（MemTable+SSTable 合并扫描 + 快照 seq 过滤），SUM/ORDER BY/DISTINCT 在扫描结果上处理；复测（d044b4c）：read_only 42→71 TPS（+68%）、read_write 29.5→53 TPS（+80%）；未达 50× 预估——真实瓶颈为引擎单 Mutex 每语句串行（~1ms×14 语句），后续 P1 并发模型改造 |
| N | 倒排回表批量读（batch_get） | P0 | ✅ 完成 | 借鉴 batch_get 架构建议：倒排/全文 posting 回表从**逐 docid 点查**改为**批量读**——SST 层按块分组（同块多 key 只读/解压一次）+ 整文件/分区布隆批量粗筛 + Delta 单次范围扫描分组覆盖；万级 posting 回表从万次随机读降为块级顺序读 |
| O | 读路径无锁化改造（&self 化 → RwLock 读读并行 → ssts ArcSwap 后台合并，O/Q 合并） | P1 | ✅ 完成 | 评审修正（设计不一致）：O（RwLock）与 Q（ArcSwap）共享前置——读 API `&self` 化，原两条存在循环依赖 → 合并为大项：① 读 API `&self` 化（✅ `df16058`：SstReader::get/scan_range/SstRangeIter + ColumnFamily 六扫描方法 &self）→ ② RwLock 读读并行（✅ `4585bb9`：引擎读方法 &self + HotCache/txn_locks/pending_inverted/SstReader.full_index 内部 Mutex + mysql.rs `Arc<RwLock<Engine>>` 读写锁拆分；1 亿库 read_only 42→561 TPS +13.3×、read_write 29.5→230 TPS +7.8×）→ ③ ssts ArcSwap 原子发布 + 后台合并（✅ `e9f7d39`：`ssts → ArcSwap<SstSnapshot>`（读 load 无锁快照 / 写 store 原子切换）+ CF/Engine compact 全链 &self + mysql 后台 worker（写路径置 `compact_pending` 信号、worker try_read 读锁合并、10 分钟兜底）——合并期间读读并行不阻塞，写互斥由 RwLock 保证；430 测试全绿，1 亿库无读回归） |
| P | 事件驱动自动 Compaction（写路径自触发 + 大小阈值 + 背压） | P0 | ✅ 完成 | 24861c6：写路径 `auto_compact`（Flush 后 L0 段数/大小超阈值 → 同步合并收敛，写入自然退避=背压）+ `l0_max_size_mb` 大小软阈值（≥2 段才触发，单段不无收益重写）+ mysql_server 保底 10 分钟定时器；422 测试全绿 |
| Q | ssts ArcSwap 化（合并不阻塞读） | P1 | ⏳ 并入 O | 已并入 O 项第三阶段（&self 前置由 O 承担），不再独立排期 |
| R | L0/层全局布隆预过滤（点查每层 1 次粗筛替代逐 SST 布隆） | P1 | ✅ 完成 | 388a916：排期原方案「层布隆 = 段布隆 OR 合并」经分析**数学上不可行**（P62：块/段布隆按各自 key 数分配 num_bits 无法 OR；强制统一则段布隆=层容量→磁盘爆炸；L0 层布隆=历史全 key 集必然假阳性爆表；meta-only compact 无 key 无法增量维护）→ 落地等价目标**层/段两级 Zone Map（min/max）范围粗筛**：SstSnapshot 层范围/层索引（快照构建 O(段数) 聚合）+ 点查层遍历整层跳过 + 段级 O(1) 越界跳过；零格式变更、零假阴性；demo 16 段点查 ≈ 单段（0.95×）；431 测试全绿 |
| S | MemTable 多版本（严格 MVCC，RR 快照读正确性） | P1 | ✅ 完成 | e7a413a：MemTable 版本链（put/delete 追加版本）+ SST 多版本落盘（同 key 版本不跨块）+ 版本感知读（`get_at`/`scan_block_for_key_at`/`get_from_sst_at`）——修复 RR 快照在未刷盘时读到新版本的正确性缺陷；compaction 仍按 max seq 收敛旧版本；428 测试全绿 |
| T | 事务点查快照缓存（get_at per-txn 小缓存） | P2 | ✅ 完成 | 0eca7a5：Transaction 内 256 项快照缓存（snap_get/snap_put，超限清空）——RR/SERIALIZABLE 快照读同 key 二次读直达（免 LSM 冷读放大，快照 seq 恒定结果一致）；命中跳过重复加锁；RC 不缓存保读最新语义；提交/回滚随 Transaction drop 即弃；432 测试全绿 |
| U | 4KB 块冷扫预读合并（SstRangeIter 组读 4×4KB → 1×16KB） | P3 | ✅ 完成 | 85b9a62：SstReader::read_block_group（一次 read_at 覆盖整组 + 逐块切片/CRC/解压，布局假设校验失败回退逐块读）+ SstRangeIter advance_block 一次组读 ≤4 块预解码缓存；扫描语义与逐块一致；437 测试全绿 |
| V | io_uring 后端落地 + SQPOLL 预留核 | P2 | ✅ 完成（Linux 部署验证） | f09e9fb：crates/io-uring-file（unsafe 白名单独立 crate，Linux 门控非 Linux 空编译）——io-uring 0.7 封装 read_at/write_at/fsync 同步提交-等待 + 可选 SQPOLL/绑核，4 个运行测试经 --target x86_64-unknown-linux-gnu 交叉 check 通过；主库 IoUringPool 三队列（WAL/SST/倒排按 IoClass 路由）+ Engine 持池（Linux + `runtime.io_uring_enabled` 初始化）+ affinity SQPOLL 预留核；热路径接入（sstable/wal）留 Linux 部署验证（主库 Linux 交叉编译受 zstd-sys 原生依赖阻塞） |
| W | Compaction 优先级队列 | P2 | ✅ 完成 | f09e9fb：跨列族**紧迫度调度**——column_family::compaction_urgency（L0 段数×10 + 大小超限 +8），Engine::compact 每轮仅压最高紧迫度档列族（并列并行保留 SSD 并发），其余由后台 worker 后续轮次压实（while needs_compact 多轮）；压力最大列族（primary 主数据）优先收敛，读路径最快受益；433 测试全绿 |
| X | Metrics（Prometheus 风格 /metrics） | P2 | ✅ 完成 | 0257835：新增 src/metrics.rs（原子计数器 + 延迟对数直方图 + Prometheus 文本渲染）分层埋点——引擎层（读写 ops/延迟/compact 次数）+ 列族层（flush_counter）+ 网络层（mysql 连接/语句计数）；server.rs `GET /metrics`（counter/histogram/gauge）；436 测试全绿 |
| Y | 分布式延伸 + 写路径收尾（2026-09-01） | 延伸 | ✅ 完成 | SAGA 网关 HTTP API（Ex-2.5，781199e）+ 13.5 补偿协议（170bf21）+ 13.6 拓扑并行 + 13.7 后台对账（71aa712）；1 亿库写路径 syscall 风暴修复（96ac6bc，oltp_insert +795%）+ 合并阻塞分批缓解（1763554）；465 测试全绿；详见 development.md 7.57~7.61 / P71 |
| Z | 合并阻塞写根治：无锁合并（2026-09-01） | 延伸 | ✅ 完成 | P72 完整方案（af24dbd：MemTableBuffer RwLock &self 化 + CF sst_mutate + Engine Arc 化 + worker clone CF Arc 无锁合并——写与合并并发）+ P73 manifest 竞态修复（3d58137：persist 内存快照 + store→persist→remove 原子）+ 回归测试（5de5ab0）；1 亿库实测合并期写 25-43k rows/s 不塌陷（修复前 8.2k-18.2k 分钟级阻塞）；469 全绿；详见 development.md 7.65 / P72 / P73 |
| AA | 导出增强：流式管道 + JDBC 直连（2026-09-01） | 延伸 | ✅ 完成 | design 20.5 阶段 3（c6b5417）：流式管道（--filter/--project/--mask Filter+Projection+Sink 分叉，内存 O(批)）+ JDBC 直连（MysqlWireClient MySQL wire 客户端建表+批量 INSERT 无文件落盘）+ --rate-limit；端到端验证 CSV/Parquet/JDBC；476 全绿；详见 development.md 7.66 |
| AB | 导出增强：MySQL 兼容 CSV 配套 + 建表 DDL（2026-09-01） | 延伸 | ✅ 完成 | design 20.5（313bd81）：--mysql-compatible 自动生成 CREATE TABLE + LOAD DATA INFILE 配套 SQL（比逐条 INSERT 快 ~20 倍）+ --mysql-max-varchar 控制 VARCHAR/TEXT + --dry-run-schema --target clickhouse\|mysql 建表 DDL（MergeTree / InnoDB）；+4 测试 476 全绿；详见 development.md 7.67 |
| AC | 导出增强：与 Compaction 共享后台 IO 优先级（2026-09-01） | 延伸 | ✅ 完成 | design 20.5 收尾（40e8abb）：CF scan_limiter Token Bucket（扫描路径专用，前台点查不受影响）+ export --io-rate-limit-mb（默认 storage.io_rate_limit_mb 同 Compaction 后台预算语义）；+1 测试 477 全绿；**导出功能全部完成**；详见 development.md 7.68 |

## 3. 大项详情

### A. 本机 2000 万 / 5000 万性能吞吐基准（P0，✅ 完成 2026-08-30）
- 内容：`shanshui-cunji demo --scale N --config config.bench.toml`，13 项测试（冒烟/2000 万/5000 万全绿）：
  构造数据 → 批量插入（单条流式）→ 批量插入（put_batch 1000/5000 条/批，批大小可配
  `SHANSHUI_BATCH_SIZE`）→ 主键 100 万次 → HotCache 100 万次 → 组合索引 1 万次 → 倒排检索 1 千次 +
  COUNT（内存位图）1 万次 → fulltext 分词 1 千次（中文 bigram + 英文整词）→ 类 SQL 等值 1 千次 +
  amount/ts BETWEEN 各 100 次 → 分片路由（抽样 100 万）→ 删除 100 万次 → 优化器自检 → 备份还原；
- 配套：`config.bench.toml`（inverted_fields=status/city + fulltext_fields=title/content/remark +
  bitmap_fields=status/city）；引擎修复：fulltext 词 term 与倒排白名单正交、sqlish LIMIT 下推、
  倒排段 GC 合并入口（inverted_gc）、put_batch 批量 API（6197c21）、G 项 posting 缓存（c380792）；
- 结果：插入 4.6 万条/s（2000 万）/ 3.1 万条/s（5000 万，写放大 + compaction）；
  主键 15.6-26.5µs/次；倒排检索+COUNT 0.5-1.4s；fulltext 5.4-64.8ms/次；总耗时 9.4 / 28 分钟；
  详见 `images/perf-0.6.0/汇总报告.md`；
- 交付：images/perf-0.6.0/{2000万,5000万,2000万-b5000}/ 报告 + console.log。

### B. 本机 1 亿数据性能与吞吐测试（P0，排期）
- 目标：全链路 1 亿条量级的写入/查询/索引/备份基准（补全三规模 2000 万/5000 万/1 亿）；
- 前置：A 完成并确认 12 项全绿；预计耗时约 30-60 分钟（1 亿插入 × 2 引擎 + 分片 + 查询循环）；
- 风险：fulltext/倒排查询次数需按 posting 成本复核（1 亿库单次 bitmap 反序列化 ~20ms+，1 万次 ≈ 200s+ 可接受）；
- 交付：images/perf-0.6.0/1亿/ + 汇总报告（与 2000 万/5000 万对比，验证线性扩展）。

### C. 分布式吞吐优化（P1）✅ 完成
- 背景：跨地域真机 10000 条写 1074s（9.3 w/s）——瓶颈 = 网关全局锁串行 + 同步 RPC 往返 + 节点无组提交；
  同机预期 1000-5000 条/s（机制正确性已验证，7.51）；
- 改造（三项独立，已全部落地）：
  ① Gateway 按分片并行——cluster_demo 网关写循环每线程独立 Gateway 实例（独立 RPC 连接集合，去全局 Mutex 串行）；
  ② RPC 批量写入——`shard.put_batch` handler（节点 Engine::put_batch 原子提交）+ `ShardEndpoint::put_batch` trait +
     `Gateway::put_batch` 按 docid 一致性哈希分组 → 每节点一次 RPC 批量提交（RTT 分摊到批）；
  ③ 节点组提交——cluster_demo `--group-commit-us`（默认 2000µs，配置 `storage.group_commit_us`），窗口内写攒批一次 fsync；
- 验收：本机两节点 10000 条（4 线程 × 2500，batch=10000，group_commit=2000µs）写 0.03s（364,584 w/s），
  广播检索精确命中 + 逐条点查跨节点路由强一致校验通过（无丢失/重复）；对照跨地域 1074s 目标 <60s 大幅达标；
  375 测试全绿（新增 gateway_put_batch 路由/计数测试）；
- 跨地域真机复测 ✅（2026-08-30）：阿里云 node-b（2 核/1.6GB，HDD，SSH 隧道 19092→9092）+ 本机 node-a，
  10000 条（4 线程 × 2500，batch=10000，group_commit=2000µs）写 **0.5s（21,590 w/s）**，强一致校验通过
  （广播精确命中 + 逐条可见）——对照基线 1074s 提升 ~2100×，目标 <60s 达标；写路径 RTT 分摊到批 + 组提交
  消除逐条 fsync；剩余耗时在逐条点查读路径（跨地域 RTT ~52ms/次，非 C 项范围）。

### H. MySQL 协议适配（MySQL 生态接入，P1）🔄 进行中
- **背景**：sqlish 为类 SQL 子集（仅 SELECT 点查/范围 + LIMIT），MySQL 客户端/生态工具
  （mysql cli、JDBC/MyBatis、Navicat、sysbench）无法接入；网关层实现 MySQL wire protocol →
  生态工具直接可用（协议兼容场景的 sysbench 压测前提，见上一轮压测选型分析）；
- **已完成（H-1~H-3）**：`src/mysql.rs`（握手 HandshakeV10 + mysql_native_password 认证 +
  报文编解码 + COM_QUERY 分发）+ `src/bin/mysql_server.rs`（独立 bin，默认 0.0.0.0:3307）；
  SHOW DATABASES/TABLES/VARIABLES + SELECT（`WHERE id=N` 主键点查 / sqlish 引擎）+
  INSERT/UPDATE/DELETE（映射到文档引擎 put/delete）+ SET/BEGIN/COMMIT/ROLLBACK（放行）；
  数据模型：库 `scc`，表 `documents`，列 `id`（BIGINT 主键）+ `doc`（JSON 文档）；
  **验证**：mysql cli 8.0 真实连接 + SELECT VERSION()/SHOW DATABASES/INSERT/UPDATE/DELETE 全链路 ✓；
  pymysql 全链路 ✓；协议级测试 +6（认证/解析/往返）；
- **待办（H-4~H-6）**：事务语句（BEGIN/COMMIT → txn.rs）、预处理语句（JDBC）、sysbench 接入验证；
- 子任务（按阶段顺序）：
  - H-1 协议核心 ✅：HandshakeV10 握手 + mysql_native_password 认证（sha1 scramble）+ 报文编解码
    （packet/OK/ERR/ResultSet/EOF）+ 独立端口监听（默认 3307 避免冲突）；
  - H-2 系统查询 ✅：SHOW DATABASES / SHOW TABLES / SHOW VARIABLES、SELECT VERSION()/@@version——
    客户端连接与元数据兼容（mysql cli 可交互）；
  - H-3 SQL 映射 ✅：INSERT（→ put + docid 分配）、UPDATE（→ put 全量覆盖）、DELETE（→ engine.delete）、
    SELECT（→ 主键点查 / sqlish 引擎）、LIMIT/OFFSET；
  - H-4 事务语句 ✅：BEGIN/COMMIT/ROLLBACK → txn.rs 事务 API（RR 快照 + 提交写写冲突检测）；
    事务内 SELECT（快照点查 + 同事务未提交写可见 read_own）/ INSERT / UPDATE / DELETE（攒批，
    commit 原子落库）；MySQL 语义：无活动事务 COMMIT/ROLLBACK 返回 OK（空提交）；
  - H-5 预处理语句 ✅：COM_STMT_PREPARE（stmt_id + 参数/列定义）/ COM_STMT_EXECUTE（null bitmap +
    类型表 + LONGLONG/LONG/DOUBLE/字符串二进制参数解析 → 占位符替换 → COM_QUERY 逻辑）/
    COM_STMT_CLOSE（释放）；+2 测试（PREPARE/EXECUTE 往返、未知 id 报错）；
  - H-6 sysbench 接入 ✅：多列 INSERT（`(id,k,c,pad)` → 组装 JSON 文档）+ DDL 放行
    （CREATE/DROP/TRUNCATE/ALTER TABLE → OK，文档库无 schema）+ 负载模拟（pymysql 驱动
    sysbench 风格）：prepare 20000 行 945 w/s、8 线程并发 point_select 3040 q/s、
    BEGIN/SELECT/COMMIT 事务点查 1744 txn/s；sysbench 本体需在 Linux/WSL 安装（Windows 无预编译）；
- **验收** ✅：mysql cli 连接 + INSERT/SELECT/UPDATE/DELETE 往返 + 事务语句 + sysbench 风格负载全通过；
- **优先级说明**：P1（生态接入价值高：MySQL 工具链/客户端立即可用），排 B（P0）之后、P2 项之前；
- 备注：读写分离（原 H）暂缓保留——组提交已解决读被写拖垮，待复制型分布式阶段再启。

### D/E/F. LSM 事务三阶段（P1/P2）✅ 完成
- 阶段一 WriteBatch（src/txn.rs `WriteBatch` + `Engine::write`）：攒批 put/delete → 预校验（失败零副作用 =
  "失败回滚"）→ 单次 flush_wal 原子提交（崩溃按 WAL 批次整体重放，无中间态）；回滚 = 丢弃未应用批次；
- 阶段二 Snapshot/MVCC（`Transaction` + `Engine::txn_begin/txn_get/txn_commit`）：快照 seq = 事务开始时的
  全局 seq，RR/SERIALIZABLE 走 `get_at(snapshot)` 一致快照读（复用 design 4.7 MVCC）；提交时写写冲突检测
  （目标在快照后被并发事务修改 → `TxnConflict` abort，冲突后锁全部释放）；
  已知局限：MemTable 不保留多版本（design 4.7 注明），快照读需旧版本已落 SST（flush 后准确）；
- 阶段三 完整 ACID（`Isolation` + `LockTable`）：RC（读最新已提交）/ RR（快照 + 写冲突检测）/
  SERIALIZABLE（RR + 读共享锁/写排他锁 2PL 至提交，共享→排他合法升级）；wait-for 图死锁检测
  （环中请求者为 victim abort，调用方可重试）；提交/回滚/失败路径均释放锁（防泄漏）；
- 测试：+15 事务测试（WriteBatch 原子/回滚/预校验、RR 快照读/写冲突、RC 最新读、SERIALIZABLE 读写锁、
  升级、死锁环、delete 混合提交、快照 seq 推进）+ 393 全绿；错误类型新增 TxnConflict/TxnDeadlock/TxnAborted；
- 对齐既有评估：单 docid 路由天然不分片（Ex-3 Calvin 结论），分布式事务 L1 Outbox/L2 SAGA 已覆盖写路径本地性。

### G. 倒排 posting 检索优化（P2）✅ 完成
- 主体（c380792）：term→bitmap LRU 缓存（256 项，Arc 浅拷贝 O(1)）+ bitmap_fields 白名单内存位图
  （写路径同步维护，查询 O(1) 直接返回）+ 写路径（add/flush/gc）缓存失效；2000 万库倒排 0.5s；
- 补充（K 项方向落地）：**段数据 mmap 化**——`data_files: ArcSwap<HashMap<seg, Arc<MmapFile>>>`，
  查询按 FST offset 直接 mmap 切片反序列化（免 `fs::read` 全文件读取 + 堆复制），物理页按需缺页加载
  （P23 只读映射白名单，与 dicts 同模式）；flush 预注册 + 重开懒加载 + gc 先换映射再删旧文件
  （Windows 已映射不可删）；大段文件未命中查询（首次反序列化）IO 成本显著下降；
- 测试：+3 mmap 测试（flush 注册/重开懒加载/GC 后正确）；收益预估：重复 term 10-50×（LRU）+ 首次查询段读取优化（mmap）。

### M. 事务类查询优化：范围查询改一次扫描（P0，✅ 完成 d044b4c）
- **背景**：1 亿库 sysbench 全套实测（2026-08-31）：单语句读写 5k-32 万 TPS、延迟 sub-ms；但事务类
  `oltp_read_only` 仅 42 TPS（mean 189ms）、`oltp_read_write` 29.5 TPS（mean 270ms）——与点查类差两个数量级。
- **根因**（mysql.rs `txn_select`/`extract_target_ids`）：`WHERE id BETWEEN A AND B` 被**展开为逐 id 列表**
  （上限 10000），SUM(k) 聚合/ORDER BY/DISTINCT 同样**逐 id 独立 `engine.txn_get`**——oltp_read_only 每事务
  ~700 次完整 LSM 点查（MemTable + 78 SST 逐层布隆/二分/解压 + JSON 解析），范围扫描的活被退化成 700 次随机点查。
  叠加 RR 快照读 `get_at` 不走 HotCache + 并发写冲突（1213）。
- **方案（P0，4 小项全部落地 d044b4c）**：
  1. ✅ `column_family::scan_range_at`（622 行）：MemTable + SSTable 合并扫描，键/值按全局 seq 过滤
     （对齐 `get_bytes_at` 快照语义）；
  2. ✅ `Engine::scan_range_txn`（engine.rs 1169 行）：事务读入口，快照 + 同事务写 `read_own` 覆盖，
     SERIALIZABLE 不加读锁（范围读只读投影）；
  3. ✅ `txn_select`（mysql.rs 685 行）：BETWEEN 走一次 `scan_range_txn`（点查/IN 保持逐 id `txn_get`），
     SUM 扫描时累加，ORDER BY / DISTINCT 在扫描结果上处理（LIMIT 截断）；
  4. ✅ `select_response`（mysql.rs 1157 行）：非事务 BETWEEN 同步换 `scan_range` 一次扫描（实时语义，无快照）；
- **达成**：read_only 42 → **71 TPS（+68%）**、read_write 29.5 → **53（+80%）**（1 亿库复测，构建记录 images/perf-0.7.0/sysbench-100m/）；
- 测试：范围快照一致性（扫描 vs 逐 id 同结果）、SUM 累加正确、ORDER BY/LIMIT、事务内写后扫描可见
  （scan_range* 7 测试全绿）；提交 `d044b4c`（419 全绿）。

### N. 倒排回表批量读（batch_get，P0，✅ 完成 d044b4c）
- **背景**（架构建议借鉴）：倒排/全文检索的 posting 命中 docid 集合后需**回表**取文档。旧实现
  `search_term_paged` 对每个 docid 逐次 `engine.get()`——每 key 独立走完整 LSM 点查（MemTable +
  全部分层 SST 的布隆/二分/读块/解压 + Delta 扫描 + JSON 合并）。posting 返回 1 万主键 = 1 万次
  随机点查，是倒排链路的下一性能瓶颈（G/K 项已解决 posting 查询端，回表端未批量）。
- **方案（借鉴建议的三步批量接口）**：
  1. `sstable.rs` 新增 `scan_block_for_keys`：数据块一次解码，命中 targets 集合全部 key；
  2. `column_family.rs` 新增 `get_many(docids)` + `get_many_from_sst`：① MemTable 批量（Tombstone
     直接终结）② 逐 SST：整文件布隆粗筛 → 逐 key 二分定位块 → **按块分组** → 分区布隆校验 →
     每块只读/解压一次（块缓存复用）→ 块内一次扫描；语义与 `get_bytes` 一致（最新版本优先，
     Tombstone 视为不存在）；
  3. `engine.rs` 新增 `batch_get(docids)`：删除位图 O(1) 批量过滤 → HotCache 批量命中 → primary
     `get_many` → Delta 覆盖用**单次范围扫描 [min..max] 按 docid 分组**（替代逐 docid 扫描）；
  4. `search_term_paged`/`fulltext_search_paged` 回表改走 `batch_get`（bitmap 迭代 docid 升序，
     天然满足批量输入要求）；
- **预期**：万级 posting 回表从万次随机读降为块级顺序读（同块多 key 共享一次 IO/解压）；
- 测试：get_many vs 逐条 get 一致（跨 flush + tombstone）、batch_get vs get 一致（Delta 覆盖 +
  删除位图）、倒排回表分页/删除过滤；交付后 1 亿库 fulltext/倒排复测。

### P. 事件驱动自动 Compaction（P0，✅ 完成 24861c6）
- **背景**（架构评审修正：检查≠触发）：Compaction 原仅 CLI/demo 显式调用（`engine.compact`），写路径
  零触发 → server 长跑 L0 只增不减（1 亿库 78 个 L0 段全留在读路径）。事件驱动才是 LSM 标准形态——
  写入路径自己负责触发合并，不依赖外部定时器；定时器只作保底。
- **方案**：
  1. `engine.put_nosync` 写后调 `auto_compact()`：`needs_compact()`（L0 段数超动态阈值 / L0 大小超
     `l0_max_size_mb` / L1/L2 需收敛）→ 同步 `compact()`，guard ≤8 轮（正常 1~2 轮收敛）；
     同步执行 = 写入自然退避 = **背压**（Q 项 ArcSwap 化后改后台无锁，本函数退化为触发调度）；
  2. `needs_compact` 大小阈值需 **L0 ≥ 2 段**（单段为已排序文件，合并是纯无收益重写）；
  3. `mysql_server` 保底守护线程（10 分钟周期，`try_lock` 非阻塞兜底 flush 未触发/合并失败）；
  4. `auto_compact` / `l0_max_size_mb` 配置化（默认开 / 0=仅段数阈值）。
- **测试**：+3（auto_compact 收敛 L0 至阈值内且数据完整、关闭后 L0 累积、大小阈值多段触发/单段不触发）；
  422 全绿。

### 架构评审 6 点对照（2026-08-31）
| 评审点 | 现状 | 结论 |
|---|---|---|
| ① L0 自动触发 Compaction | 写路径零触发（仅 CLI 显式） | ✅ 已修（P 项 24861c6） |
| ② L0 层全局布隆预过滤 | 点查遍历全部 SST 逐个布隆（78 段=78 次） | ✅ 已修（R 项 388a916：层/段两级 Zone Map 范围粗筛——排期原布隆 OR 方案数学不可行，P62 论证） |
| ③ 倒排并发读锁 | 读路径已 ArcSwap 无锁快照（Ex-6.2/6.3 `c8183cf`） | ✅ 已实现（文档澄清；引擎级全局 Mutex 归 O 项） |
| ④ 环形 WAL 回绕覆盖 vs 崩溃恢复 | 已实现：Flush 后 `set_flushed_seq`，回绕仅覆盖已刷盘记录，未刷盘 → `WalFull` → 强制 Flush（M6-1 + P35） | ✅ 已实现（文档澄清） |
| ⑤ 4KB 块 IO 放大（预读合并） | 块缓存 LRU 已摊薄重复读；zstd 压缩 + 分区布隆 | ✅ 已修（U 项 85b9a62：SstRangeIter 组读预读 4×4KB → 1×16KB） |
| ⑥ io_uring SQPOLL 与绑核冲突 | io_uring 后端未落地（io_queue.rs 仅队列抽象，`io_uring_enabled` 默认关，Linux 专属） | 备注：后端落地时需给 SQPOLL 预留独立核（affinity 分区扩展） |

### 缺陷 → 优化方案路线（与 feature.md「架构评审与补充」对齐）
| 缺陷（按严重度） | 性质 | 方案要点 | 排期项 |
|---|---|---|---|
| 引擎全局单 Mutex 串行 + Compaction 同步阻塞 | 性能（吞吐天花板 ~1000 stmt/s） | **读路径无锁化合并**：① 读 API `&self` 化（O/Q/R 共同前置）→ ② RwLock 读读并行（RR 快照读只读）→ ③ ssts ArcSwap + 后台合并 | O（P1，O/Q 合并） |
| L0/L1 层无全局布隆 | 性能（点查逐 SST 布隆+二分） | 每层 OR 合并布隆，`get_bytes` 整层粗筛一次跳过 | R（P1） |
| RR 事务点查冷读（get_at 不走 HotCache） | 性能（事务内重复点查放大） | 事务对象内 256 项快照小缓存，提交/回滚即弃 | T（P2） |
| io_uring 后端未落地 | 平台（Linux 性能上限未兑现） | liburing unsafe 封装 + `io_uring_enabled` 接入 + SQPOLL 预留核 | V（P2） |
| Compaction 优先级队列缺失 | 调度（多列族/层级无序） | 合并任务入优先级队列（热段 > 冷却 > 文件数压力） | W（P2） |
| Metrics 缺失 | 运维（无观测指标） | admin `/metrics` + 计数器/分位直方图分层埋点 | X（P2） |
| 4KB 块冷扫 IO 放大 | 性能（低优先） | 相邻块合并 read_at + 预填充块缓存 | U（P3） |

> 依赖链：O 项第①步（读 API &self 化）是 O 第②③步与 R 项共同前置；S 项已独立完成（✅ e7a413a，
> 不依赖并发改造）；建议顺序 O → R → T/W/X → V → U。

## 4. 已完成大项（最近）

- S 项 MemTable 多版本（严格 MVCC）✅（e7a413a：RR 快照读未刷盘也一致，+6 测试，428 全绿）
- P 项事件驱动自动 Compaction ✅（24861c6：写路径自触发 + 大小阈值 + 背压 + 保底定时器，1 亿库 78→1 段验证）
- N 项倒排回表批量读 batch_get ✅（d044b4c：SST 按块分组 + Delta 单次范围扫描，万级 posting 块级顺序读）
- M 项事务范围查询一次扫描 ✅（d044b4c：read_only 42→71 TPS +68%、read_write 29.5→53 +80%）

- H 项 MySQL 协议适配 ✅（H-1~H-6：握手/认证/SQL 映射/事务/预处理/sysbench 接入，mysql cli 8.0 + pymysql 真实连接）
- D/E/F LSM 事务三阶段 ✅（WriteBatch 原子写 + 快照隔离 + ACID 锁/死锁检测/隔离级别，+15 测试）
- G 项倒排 posting 检索优化 ✅（c380792 LRU + 白名单位图；补充段数据 mmap 化，+3 测试）
- 本机 2000 万 / 5000 万大数据量基准 ✅（A：13 项全绿，perf-0.6.0/汇总报告）
- C 项分布式吞吐优化 ✅（本机 364k w/s + 跨地域 0.5s/~2100×，强一致通过）
- put_batch 批量插入 API ✅（6197c21：原子批次，D 项 WriteBatch 前置）
- 阿里云两节点分布式强一致测试 ✅（7.51，cluster_demo：2000 条隧道 + 10000 条直连均强一致通过）
- 服务器 YCSB 基准 ✅（7.51：rotational=1 高效云盘 load 约本机一半、读 p50 持平、长尾放大）
- 类 SQL 解析 / 写入 Enrich / 读写分离评估 ✅（441282d / 706c33b / fcc26a6）
- Ex-1~4 分布式事务与倒排策略 ✅（7348acd / 990bf6b / 653fdc8 / 04aae97）
- 本机 NVMe SSD 基准 ✅（7.50：YCSB 写重 90.9k、纯读 269.9k ops/s，20 万条热测）

## 5. 环境备忘

- 本机 Rust 工具链：`D:\cargo-home\bin` + `D:\rustup-home`（RUSTUP_HOME/CARGO_HOME）；
- release 构建：`cargo build --release --target-dir D:\shanshui-cunji-target --bin shanshui-cunji`；
- 大数据量测试临时目录：`SHANSHUI_CUNJI_TMP=D:\shanshui-cunji-tmp`（C 盘空间不足）；
- 服务器（阿里云 106.14.68.116，凭据不入库）：/root/scc-new + vendored offline 构建，CARGO_BUILD_JOBS=1；
- 测试辅助脚本：`function_test/`（gitignore 排除：run_demo.ps1 / run_bigdata_bench.ps1 / screenshot_sections.py）。
