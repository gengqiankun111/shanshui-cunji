# 山水存迹数据库（shanshui-cunji）

> **海量数据，轻松存取** —— 面向"高吞吐写入 + 快速检索"的文档数据库（LSM-Tree + 倒排/全文索引）。
> 单机设计基准 1.5 亿文档 / 300GB（写 P99 < 2ms、热点亚毫秒）；分片集群线性扩展至 50 亿级。
> **纯 SSD 持久化（NVMe/SATA），不要用 HDD 压测。**

---

## 📊 与 MySQL 8.0 的同负载对照（2026-09-02，本机）

> 环境：MySQL 8.0.45（InnoDB，buffer pool 8G）vs 山水存迹 **release** 经 MySQL 协议
> （cjserver，async，默认组提交 2ms）；同一份 SQL/驱动/负载脚本、内容一致的两张表
> （orders 10 万行 + users 1 万行）。

| 指标 | 山水存迹 | MySQL 8.0 | 差距 |
|---|---|---|---|
| 批量插入（rows/s） | **82,341** | 53,836 | **1.53×（山水存迹）** |
| 点查 QPS（16 线程并发） | 5,073 | 7,451 | 1.47×（MySQL） |
| 范围查询 p50（BETWEEN 100 行窗口） | 3.02 ms | 0.87 ms | 3.5×（MySQL） |

- **插入反超 1.53×**：真根因曾为 mysql-server 默认未开组提交（每行独立 WAL fsync → 428 rows/s）；
  默认 2ms 组提交后 428 → 82,341 rows/s（192×），反超 MySQL。
- **点查差距 1.47×**：LSM 读路径（删除位图 + Zone Map + HotCache）已接近 InnoDB 主键点查，
  余量在协议层 JSON 文档解析（原生 API 点查 1 亿库 26.5k TPS）。
- **范围差距 3.5×**：MySQL 定长二进制列 vs 本文档库逐行 JSON 反序列化——文档型语义差异，非缺陷。
- **SQL 能力差异**：MySQL 协议/类 SQL 已支持点查/范围/COUNT·SUM·AVG·MIN·MAX/ORDER BY/GROUP BY/
  HAVING/LIMIT；**JOIN、子查询、无条件 COUNT(*) 快路径等未覆盖**（完整边界见
  [用户指南 §4](user_guide/README.md)）；可复现脚本与更多量级对比见 `tmp/`。

---

## 一句话定位

**不需要复杂事务、但要求海量写入 + 极速检索**的业务（日志/埋点/IoT/画像/元数据），
就是 shanshui-cunji 的用武之地；业务强事务与跨分片 JOIN 请留在 MySQL/Redis。

## 核心能力

- ⚡ 高吞吐写入：WAL 组提交（A 写重 91k ops/s）、50M 批量导入 ~6 万行/s
- 🔥 点查亚毫秒：HotCache + 块缓存 + Zone Map/布隆剪枝
- 🔍 任意字段筛选：内存字典 + 倒排/位图，AND/OR 组合免建组合索引；中文 bigram/jieba 全文
- 🔢 聚合快路径：倒排计数（亚毫秒）＋ COUNT/SUM/AVG/MIN/MAX · GROUP BY · HAVING · ORDER BY
- 🩹 更新省 IO：Delta 增量 + Merge-on-Read；删除位图（delete 跳 Tombstone，GC 物理回收）
- 🗂️ 列族隔离 + 全维度可配置；FST+mmap 亚秒冷启动
- 🚀 协程网络层（10k 长连接）+ 线程池隔离；MySQL 协议生态直连（cli/pymysql/JDBC）
- 🛡️ 生产级：看门狗熔断、内存水位、EXPLAIN、/metrics、备份还原、TTL、平滑扩容
- 📦 数据管道：export → Parquet/CSV/JDBC/增量，对接 ClickHouse / MySQL 数仓

## 快速开始

```bash
cargo build --release
shanshui-cunji server --config config.toml            # 原生 HTTP/TCP/CLI
cjserver --data-dir ./data --bind 0.0.0.0:3307   # MySQL 协议（库 scc）
shanshui-cunji put --id 1 --data '{"status":"active","score":88}'
shanshui-cunji search --filter 'status=active AND score>80'
```

## 文档导航

- 👉 **用户指南（功能/快速开始/迁移/配置/运维/管道）**：[user_guide/README.md](user_guide/README.md)
  （含 [JOIN 替代方案](user_guide/join_function.md)、[Redis 集成](user_guide/redis-integration-guide.md)）
- 技术设计 [design.md](design.md) ｜ 开发记录 [development.md](development.md)
- 功能清单（内部）[feature.md](feature.md) / [feature_remain.md](feature_remain.md)
- 发布说明 [release_doc/](release_doc/) ｜ 调研 [research/](research/) ｜ 构建 [compile.md](compile.md)
- 质量问题闭环 [problem_solving.md](problem_solving.md) / [quality_system.md](quality_system.md)
