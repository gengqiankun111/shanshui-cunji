# 山水存迹数据库（shanshui-cunji）· 用户指南

> 面向**使用与运维**工程师的功能性文档。功能**开发清单**（分模块状态/提交号）见
> [feature.md](../feature.md)，**未完成/边界**见 [feature_remain.md](../feature_remain.md)
> （本文按二者校准能力表述与限制）。版本发布说明见 [release_doc/](../release_doc/)。

---

## 1. 它解决什么问题？

| 你的痛点 | shanshui-cunji 的解法 |
| --- | --- |
| 数据量大、写入频繁，关系库顶不住 | LSM 引擎 + WAL 批量组提交，持续高吞吐写入 |
| 想按任意字段筛选，又不想提前建一堆组合索引 | 内存字典 O(1) 定位 + 倒排索引，任意字段 AND/OR 自由组合 |
| 热点数据要"秒回" | HotCache 热点缓存，命中亚毫秒返回 |
| 聚合统计拖垮数据库 | COUNT / GROUP BY 走倒排计数/统计载荷，免全量回表 |
| 从 MySQL 迁移太痛苦 | MySQL 协议接入（mysql-server）+ 一键迁移/导入工具 + 类 SQL 语法 |
| 怕宕机、怕丢数据、运维累 | 自愈看门狗、崩溃安全、备份还原、平滑扩容、/metrics |

**存储介质**：纯 SSD 持久化（NVMe/SATA SSD Only）——写入须经 WAL fsync + 落盘 SSTable
才算成功；内存仅是加速层。数据量只受磁盘限制（TB 级）。⚠️ 存储格式按 SSD 优化设计
（4KB 块 / 环形 WAL），**机械硬盘（HDD）性能会大幅下降（10 倍级），不要压测**。

---

## 2. 容量与部署形态：先单机，后分片

| 维度 | 单机 Standalone | 分片集群 Cluster |
| --- | --- | --- |
| 最大数据量 | **1.5 亿条**（写入 P99 < 2ms、热点亚毫秒） | **50 亿级**（线性扩展） |
| 水平扩展 / 高可用 | — | DocId 一致性哈希 + 虚拟分片平滑扩容；一主多从异步复制 |
| 典型适用 | 数据量 ≤ 1.5 亿 | 1.5 亿 ~ 50 亿，或需要高可用 |

- 推荐规划：单机起步 → 数据量逼近 1.5 亿时平滑扩容到分片，业务代码无需改动。

---

## 3. 核心能力一览（对照 feature.md 模块）

| 能力模块 | 说明 |
| --- | --- |
| 写入路径 | WAL 组提交（A 写重 91,296 ops/s，关闭组提交仅 2,003）、批量导入模式 50M ~6 万行/s、WriteBatch 原子写 |
| 存储内核 | WAL（环形/append + 截断回收）→ MemTable（跳表双缓冲，多版本严格 MVCC）→ SSTable（块压缩 + 分区布隆 + 两级索引）→ Leveled Compaction（事件驱动自动触发 + 动态窗口/冷却 + 删除密度 GC） |
| 倒排/全文 | 内存字典 + 磁盘段（v4 计数载荷亚毫秒）+ FST/mmap 字典；位图白名单常驻（COUNT/GROUP BY/组合筛选快路径）；fulltext 分词（bigram / jieba）；GC 后台化 + posting LRU + 流式分页 |
| 查询 | 优化器静态路由（主键/组合/倒排/范围）；范围扫描流式化（内存 O(page)）+ 块组预读 + Zone Map 剪枝；HotCache 亚毫秒；scan 游标续扫 |
| 事务 | 本地原子事务 + 隔离级别 RC/RR/SERIALIZABLE（快照读 + 写冲突检测 + docid 锁 + 死锁检测）；**无跨分片事务** |
| 分布式 | 分片集群 + raft 元数据 RPC + 分片化倒排 + 全局 docid 分配 + 网关广播/批量写 + 组提交；SAGA/outbox 异步编排 |
| SQL（sqlish 子集） | SELECT 点查/范围/BETWEEN/IN + COUNT/SUM/AVG/MIN/MAX + **ORDER BY（单/多字段）+ GROUP BY（单/多字段）+ HAVING** + LIMIT/OFFSET（详见 §4） |
| 生态 | MySQL 协议接入（mysql cli / pymysql / JDBC 预处理可用）、类 SQL WHERE、EXPLAIN、/metrics |
| 运维 | 看门狗熔断（QueryTooExpensive）、内存水位限流、SHOW PROCESSLIST / KILL、备份还原（全量/增量）、TTL、导出管道（Parquet/CSV/JDBC/增量/--rate-limit） |

---

## 4. SQL / MySQL 协议能力现状（对照 MySQL 8.0，2026-09-03）

> 详细三档语法清单（已支持 / 待开发 / 待决策，含事务与 RR 语义现状）见 [语法.md](./语法.md)。

### 4.1 已支持（经 cjserver 的 MySQL wire 协议；2026-09-05 语法面收尾核对）

- **DDL 放行**、预处理语句（PREPARE/EXECUTE）、事务（BEGIN/COMMIT/ROLLBACK、RR/SERIALIZABLE、FOR UPDATE 当前读）；
- **写**：INSERT 单行/多值/批量、`INSERT IGNORE`、`INSERT … ON DUPLICATE KEY UPDATE`（doc=整文档/col=VALUES(col)/自增/字面量）、
  UPDATE（主键/字段条件批量、`col=col+N` 自增、`doc=…` 整文档替换）、DELETE（id/主键区间/字段条件/全表）；
- **多表**：不同表独立主键空间（表级 docid 隔离，对齐 MySQL），AUTO_INCREMENT 端到端；
- **SELECT 读**：主键点查 / `id BETWEEN` / `id IN (…)`；字段条件 `= != > < >= <=`、`f BETWEEN`、`f LIKE '%..'`、`f IN (…)`，
  支持 `AND/OR/NOT` 组合与括号（含倒排收敛/范围下推/组合索引路由）；
- **聚合**：标量 `COUNT(*) / COUNT(f) / COUNT(DISTINCT f) / SUM / AVG / MIN / MAX`；
  `GROUP BY f1,f2` + 同组聚合 + `HAVING` + 组结果 `ORDER BY 聚合列/组字段` + `LIMIT/OFFSET`；无条件 `COUNT(*)` O(1)；
- **排序/分页**：`ORDER BY 多字段 ASC/DESC`、`LIMIT/OFFSET`（Top-K 有界堆 + keyset 深分页守卫）；
- **JOIN**：单 `INNER JOIN / LEFT JOIN … ON t1.f = t2.f`（等值、DocIdSet 交集执行）；
- 会话/运维：SHOW PROCESSLIST / KILL（协议层）；执行计划推演走 HTTP `GET /explain`（MySQL 协议 EXPLAIN 未接，见 §8）。

### 4.2 已知限制 / 不支持（2026-09-05 核对 parser/executor 1064 守卫）

| 能力 | 说明 |
| --- | --- |
| 子查询 / `IN (SELECT…)` / EXISTS | ❌ 不提供；替代：二次查询合并、JOIN（单表）、写入预连接、物化视图、导出 OLAP（见 join_function.md） |
| UNION / INTERSECT / EXCEPT | ❌ 不提供 |
| JOIN 扩展 | 仅单 JOIN（>1 个 JOIN 子句解析期 1064）；RIGHT/FULL/CROSS、非等值 ON、三表级联不支持 |
| 行去重 `SELECT DISTINCT` / GROUP BY 内 `DISTINCT` 聚合 | ❌（仅标量 `COUNT(DISTINCT f)` 支持） |
| HAVING 无 GROUP BY | ❌（HAVING 语义 = 分组后过滤，须配 GROUP BY） |
| 无 GROUP BY 的多列/多聚合标量 SELECT | ❌（标量聚合仅单个；`SELECT *` 与 GROUP BY 混用不支持） |
| 列表达式/函数值 | ❌ `amount*2`、CONCAT/LOWER/日期函数/CAST 等不支持；WHERE/ORDER BY 项限字段名与字面量（聚合列头除外） |
| 窗口函数（ROW_NUMBER/RANK/OVER…） | ❌ 不提供 |
| 值类型 | 比较按数值/字典序自动判定，无完整 MySQL 类型系统/隐式转换语义 |
| 跨分片事务 / 跨分片 JOIN / 全局快照读 | ❌ 分布式集群 ≠ 分布式事务库（业务强事务请留在 MySQL/Redis） |

> 详细开发项状态：功能清单 [feature.md](../feature.md)、未完成与评估 [feature_remain.md](../feature_remain.md)、
> SQL 语法面剩余缺口审计与排期建议见 development_remain.md「二、待办」SQL 语法面收尾行。

---

## 5. 快速开始

```bash
# 构建
cargo build --release

# 原生服务（HTTP/TCP/CLI 于同一二进制）
shanshui-cunji server --config config.toml

# MySQL 协议服务（生态工具直连：库 scc / 表 documents）
cjserver --data-dir ./data --bind 0.0.0.0:3307 --user root --password 123456
# 例：mysql -h127.0.0.1 -P3307 -uroot -p123456 -e "INSERT INTO documents(id,doc) VALUES (1,'{\"city\":\"bj\",\"amount\":10}'); SELECT * FROM documents WHERE amount>5 ORDER BY amount DESC;"
```

### 数据模型

每条记录 = 一个文档：主键 `DocId (u64)` + 扁平字段（字符串/数值/布尔/时间）。
写入后自动维护主键索引与按配置的倒排索引，可按任意字段组合检索。

```json
{"docid": 1001, "status": "active", "type": "order", "device": "android", "score": 88}
```

### CLI 基本用法

```bash
shanshui-cunji put --id 1001 --data '{"status":"active","type":"order"}'
shanshui-cunji get --id 1001
shanshui-cunji search --filter 'status=active AND type=order'
shanshui-cunji range --start 1000 --end 2000
shanshui-cunji delete --id 1001
shanshui-cunji backup /path/backup && shanshui-cunji restore /path/backup
```

### HTTP-JSON

```bash
curl -X POST http://localhost:8080/put -H 'Content-Type: application/json' \
  -d '{"docid":1001,"fields":{"status":"active","type":"order"}}'
curl 'http://localhost:8080/get?docid=1001'
curl 'http://localhost:8080/search?filter=status%3Dactive'
```

> 构建环境与启动参数细节见 [../compile.md](../compile.md)。

---

## 6. 从 MySQL 迁移

1. **导出**：MySQL 侧 `mysqldump`（或 JDBC 拉取）；
2. **迁移**：`shanshui-cunji-migrate` / `shanshui-cunji-import` 导入——每行 → 一个文档；
3. **改连接**：驱动替换为 cjserver / 原生 SDK，DAO 层改写为 Filter / 类 SQL WHERE。

- ⚠️ 类 SQL 为子集（见 §4.2），**JOIN / 子查询不支持**；迁移与兼容策略详见
  [design.md](../design.md) 第 15 章。

---

## 7. 关键配置速查

全量配置项与默认值见 [design.md](../design.md) 6.5 / 9.8 / 第 13 章；常见模板见
[config-example/](../config-example/)。最常用项：

| 配置 | 说明 |
| --- | --- |
| `[inverted] inverted_fields / bitmap_fields / fulltext_fields / stats_fields` | 倒排/位图/全文/统计载荷声明（枚举低基数走 bitmap，长文本走 fulltext，数字统计字段走 stats_fields，控制索引成本） |
| `[inverted] max_term_len / flush_threshold` | term 长度保护 / 刷盘阈值 |
| `[memtable] max_size_mb` | 跳表上限 |
| `[hotcache]/[blockcache] max_memory_mb` | 缓存上限（隔离） |
| `[storage] group_commit_us / l0_stall_*` | 组提交窗口 / L0 写退避 |
| `[storage] hot_fields / colstore_enabled` | PAX 热列声明 / 热列列存旁路开关（见 7.1） |
| `[runtime] async_* / compute_pool_size / io_*` | 协程/计算/IO 线程池 |

### 7.1 热列（hot_fields）与热列列存旁路（colstore_enabled）

**声明**（配置在 `[storage]` 段，无需建表语法，引擎级生效、客户端零感知）：

```toml
[storage]
# 热字段白名单：全表排序/高频数值扫描/聚合引用频率高的列（k/amount/ts/status…）
hot_fields = ["k", "amount", "ts", "status", "region", "score"]
# 热列列存旁路（P94 双轨）开关：true = 惰性派生“docid 数组 + 每热列独立区域”的内存列存
#（排序等大窗扫描只解热列，IO -90%+，如 110 万行全表 ORDER BY 9.5s → 0.2~0.3s）；
# false = 关闭（默认，零回归，全部走行式主）。
colstore_enabled = true
```

**作用与路由规则**（排序/投影/聚合触达列 vs 热列的二元 ⊆ 判定）：

| 判定 | 走向 |
| --- | --- |
| 排序键列 ⊆ hot_fields 且扫描窗口大、区间无脏行/未超派生水位 | Columnar：只解热列，胜出行整行回行式主 |
| 任一排序键非热列 / 窗口小 / 区间含脏 / 有超水位新行 / 需要整行投影 | RowStore：行式主（结果与默认布局逐字节一致） |

- **取舍提示**：热列占全行 ~10–30% 时列存收益最大；声明过少 → 排序/扫描仍走行式；
  声明过多 → 派生内存与写侧 flush 成本上升。建议只放**排序键 + 高频数值范围/分组列**，
  大文本列（note/title/…）永远不要进 hot_fields。
- **正确性护栏**：colstore 只服务「派生水位内、未删除、派生后未再写」的行；一旦命中脏行/
  超水位即整查询回退行式主（宁慢勿错）；派生失败/关闭开关 → 自动回退，无数据语义差异。
- **冷启动**：派生在首次命中查询时惰性执行（110 万行一次性 ~秒级~分钟级，与整行解析量相关），
  随后常驻；重启后按需重建（内存常驻版）。

---

## 8. 运维与调优速查

| 症状 | 调整 |
| --- | --- |
| 写入慢 / 频繁 Stall | `l0_stall_threshold`↑、compaction IO 限速↑、组提交开启 |
| 热点点查命中率低 | `hotcache.max_memory_mb`↑、`eviction_policy` |
| 倒排/聚合慢 | 声明 bitmap/fulltext/stats 字段、倒排 GC 收敛段数（v4 计数亚毫秒） |
| 超大无界拖拽筛选被熔断 | 增加时间/键范围条件收敛（`QueryTooExpensive` 为保护，勿关闭） |
| 内存吃紧 | HotCache/BlockCache 上限↓（软硬水位 0.85/1.0） |
| 冷启动慢 | `inverted.engine = "fst"`（mmap 亚秒恢复） |

运维能力：`/metrics`（Prometheus）、EXPLAIN 执行计划、SHOW PROCESSLIST / KILL、看门狗自愈、备份还原、TTL 过期。

---

## 9. 数据管道（导出到 ClickHouse / MySQL / 数仓）

```bash
# Parquet（ClickHouse 直接 SELECT FROM file() 直读；--dry-run-schema 生成建表 DDL）
shanshui-cunji export --filter 'created_at > "2026-08-01"' --format parquet \
  --output /data/export/202608.parquet --batch-size 50000 --compression zstd
# MySQL 兼容 CSV + LOAD DATA SQL（比逐条 INSERT 快 ~20 倍）
shanshui-cunji export --format csv --mysql-compatible --output /data/export/mysql_import/
# JDBC 直连写入（无文件落盘）
shanshui-cunji export --jdbc "mysql://localhost:3306/analytics" --table orders
# 增量导出（时间戳/checkpoint 游标断点续传）
shanshui-cunji export --filter 'updated_at > "…"' --format parquet --checkpoint /data/checkpoint.txt
```

顺序扫描 + 后台 IO 优先级低于前台 + `--rate-limit` 限流，对在线业务影响 < 5%。

---

## 10. 相关文档导航

- 完整技术设计：[design.md](../design.md)（存储/索引/缓存/分布式蓝图/配置全量/路线图）
- 开发实现记录：[development.md](../development.md)、未完成任务 [development_remain.md](../development_remain.md)
- 功能开发清单（内部）：[feature.md](../feature.md)、[feature_remain.md](../feature_remain.md)
- 问题闭环：[problem_solving.md](../problem_solving.md)、质量体系 [quality_system.md](../quality_system.md)
- 发布说明：release_doc/（v0.2.1 ~ v0.8.0 RELEASE / RELEASE-SUMMARY）
- 专项指南：本目录 join_function.md（关联查询 4 替代方案）、redis-integration-guide.md（Redis 冷热分层）、
  language-selection.md（语言/驱动）
- 前沿与选型调研：research/（architecture-selection / frontier-research-2026-08 / group-commit-design）
