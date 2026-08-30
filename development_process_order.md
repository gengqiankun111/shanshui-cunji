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
| B | 本机 1 亿数据性能与吞吐测试 | P0 | ⏳ 待排期 | 前置 A ✅；预估插入 30-40 分钟 + 查询（fulltext 大 posting 首次反序列化 ~100ms+/次需复核）；交付 images/perf-0.6.0/1亿/ |
| C | 分布式吞吐优化（机制已验证 → 性能） | P1 | ✅ 完成 | ① 网关分片并行（每线程独立 Gateway）② RPC 批量写入（shard.put_batch + Gateway::put_batch 哈希分组）③ 节点组提交（--group-commit-us）；本机两节点 364k w/s + 跨地域 0.5s/21.6k w/s（vs 1074s，~2100×） |
| D | LSM 事务阶段一：WriteBatch 原子写 | P1 | ✅ 完成 | WriteBatch（攒批 put/delete）+ Engine::write 原子提交（预校验失败零副作用 = 回滚语义）+ 单次 flush_wal 崩溃原子 |
| E | 事务阶段二：快照隔离（Snapshot/MVCC） | P2 | ✅ 完成 | Transaction（快照 seq 一致读 get_at + 提交时写写冲突检测，last_write_seq > snapshot → TxnConflict abort）；MVCC 已知局限：MemTable 不保留多版本，快照读需旧版本已落 SST |
| F | 事务阶段三：完整 ACID 与隔离级别 | P2 | ✅ 完成 | Isolation（RC/RR/SERIALIZABLE）+ docid 级锁表（共享读/排他写 + 2PL 升级）+ wait-for 图死锁检测（victim abort）+ 失败路径锁释放 |
| G | 倒排 posting 检索优化 | P2 | ✅ 完成 | c380792 LRU 缓存 + 白名单内存位图；补充：段数据 mmap 化（K 项落地）——免 fs::read 全文件读取，FST offset 直接切片反序列化，物理页按需加载（P23 白名单） |
| H | MySQL 协议适配（MySQL 生态接入） | P1 | 🔄 进行中 | H-1 协议核心 ✅（握手+认证+报文）→ H-2 系统查询 ✅（SHOW/VERSION）→ H-3 SQL 映射 ✅（INSERT/UPDATE/DELETE/SELECT）→ H-4 事务语句 → H-5 预处理语句 → H-6 sysbench 接入验证；mysql cli 8.0 / pymysql 真实连接全链路通过 |
| I | 高并发查询优化（design 9.5 目标） | P3 | ⏳ | M6 后留待 |
| J | 倒排段 GC 后台化 | P2 | ⏳ | 当前 gc() 需显式调用（demo 插入后合并）；后台线程周期触发（设计已有，工程化） |
| K | fulltext 大 posting 反序列化优化 | P2 | ⏳ | 5000 万库 content 词 posting ~1600 万，首次反序列化 ~100ms+；候选：段内 posting 分块延迟加载 |
| L | Compaction 智能调度（合并冷却 + 动态窗口 + 倒排阈值） | P1 | ✅ 完成 | 28eae9d：①合并冷却（compaction_cooldown=2，软约束防收敛死循环）②动态窗口（l0_stall_min/max=8/16，写压力驱动）③倒排刷盘阈值参数化（flush_threshold，二分收敛推荐 500 万：段数 -80%、吞吐仅 -6%）；+demo 13 项单条 avg/max 统计 |

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
  - H-4 事务语句 ⏳：BEGIN/COMMIT/ROLLBACK → txn.rs 事务 API（RR/SERIALIZABLE）；
  - H-5 预处理语句 ⏳：COM_STMT_PREPARE/EXECUTE（JDBC 依赖，参数化查询）；
  - H-6 sysbench 接入验证 ⏳：oltp_point_select/read_write 子集跑通 + 与 HTTP 吞吐对照；
- **验收**：mysql cli 连接 + INSERT/SELECT/UPDATE/DELETE 往返 + 事务语句 + sysbench 子集可跑；
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

## 4. 已完成大项（最近）

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
