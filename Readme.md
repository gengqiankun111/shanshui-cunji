# novosdb

> **高性能大容量文档存储 + 轻量检索引擎**
> 主打高吞吐写入、热点低延迟查询、灵活多字段筛选；放弃跨分片事务换取速度。
> 开发路线：**先单机，后分片**。

novosdb 是一款面向海量结构化文档的分布式文档-检索数据库。基于 LSM-Tree 存储引擎，单机可承载 1~2 亿文档，分片集群可水平扩展至几十亿条，适合日志、埋点、标签画像、元数据存储等场景。

---

## 特性

- ⚡ **写入快**：LSM-Tree 引擎 + WAL 批量组提交，高吞吐持续写入
- 🔥 **热点查询亚毫秒**：内置 HotCache 热点哈希缓存，命中直接返回
- 🔍 **任意字段自由筛选**：嵌入式倒排索引，任意字段 AND/OR 组合，无需预先建一堆组合索引
- 📐 **固定条件对标 MySQL**：组合稀疏索引一步定位
- 💾 **崩溃安全**：WAL 预写日志 + Tombstone 墓碑删除
- 📦 **备份还原**：冷备份 / 全量还原（本地文件）
- 🔌 **接入简单**：HTTP-JSON 接口 + TCP 协议 + CLI 客户端
- 🧩 **可扩展**：DocId 哈希分片，虚拟分片平滑扩容，一主多从异步复制高可用

---

## 适用场景

### ✅ 完美适配

| 场景 | 示例 |
| --- | --- |
| 日志 / 行为埋点 | 点击日志、APP 行为日志、访问日志、IoT 上报数据 |
| 风控 / 画像标签库 | 用户标签、设备画像、黑名单 |
| 对象元数据存储 | 文件、图片、视频元信息 |
| 电商非交易侧检索 | 商品基础信息、后台运营筛选 |
| IoT 设备快照 | 设备状态、告警标记、时序快照 |
| 后台报表数据源 | 大数据结果离线筛选中间库 |

### ⚠️ 可用但有限制

- 用户中心 / 账号系统：主键查询快，但**不支持跨分片事务**（极少同时改多用户才适用）；
- 订单库：可做**只读查询副本**（查询加速库，不是交易主库）。

### ❌ 不适合

- 强事务核心交易系统（下单、支付、库存扣减）；
- 超高并发无分片键的全局 C 端搜索（应使用 ES 等独立搜索引擎）；
- 大量跨分片 JOIN / 多表关联复杂 SQL。

---

## 快速开始

### 构建

```bash
# 依赖：Rust stable / nightly
cargo build --release
```

### 启动服务

```bash
novosdb server --config config.toml
```

### CLI 基本用法

```bash
# 写入文档
novosdb put --id 1001 --data '{"status":"active","type":"order","device":"android"}'

# 主键查询
novosdb get --id 1001

# 条件筛选（任意字段）
novosdb search --filter 'status=active AND type=order'

# 范围查询
novosdb range --start 1000 --end 2000

# 删除
novosdb delete --id 1001
```

### HTTP-JSON 接口

```bash
# 写入
curl -X POST http://localhost:8080/put \
  -H 'Content-Type: application/json' \
  -d '{"docid":1001,"fields":{"status":"active","type":"order","device":"android"}}'

# 主键查询
curl http://localhost:8080/get?docid=1001

# 条件筛选
curl 'http://localhost:8080/search?filter=status%3Dactive%20AND%20type%3Dorder'
```

### 备份与还原

```bash
novosdb backup /path/backup_file
novosdb restore /path/backup_file
```

---

## 架构概览

```
网络层 (HTTP-JSON / TCP / CLI)
        │
        ▼
查询执行器 (主键 / 组合索引 / 倒排交集)
        │
        ▼
LSM-Tree 存储引擎
  WAL → MemTable(跳表) → SSTable(多层)
  主键索引 / 组合稀疏索引 / 嵌入式倒排
  布隆过滤器 / LRU 块缓存 / HotCache / Compaction
```

- **写入路径**：写请求 → WAL → MemTable → 后台刷盘 → SSTable → Compaction
- **主键查询**：HotCache 命中（亚毫秒）→ 未命中走 LSM + 布隆过滤 + 稀疏索引
- **条件筛选**：固定条件走组合索引；任意字段走倒排 + DocId 集合交集/并集

分布式阶段（后续）：网关路由层 + 分片节点（复用单机内核）+ 元数据中心，DocId 取模路由，虚拟分片平滑扩容，异步复制高可用。详见 [design.md](./design.md)。

---

## 开发路线图

| 阶段 | 内容 |
| --- | --- |
| 阶段 1（1 个月） | 单机 MVP：LSM 内核 + WAL + CRUD + 倒排/组合索引 + HotCache + 布隆过滤 + 备份还原 + HTTP/TCP/CLI |
| 阶段 2 | 分布式：分片节点 RPC + 网关 + 元数据中心 + 广播检索 + 虚拟分片扩容 + 副本高可用 |
| 阶段 3 | 深度优化：Leveled Compaction、位图索引、MVCC、热 key 自动缓存、压缩、增量备份、迁移工具 |

详细设计、性能目标与风险说明见 [design.md](./design.md)。

---

## 项目结构（规划）

```
novosdb/
├── design.md          # 技术设计文档
├── readme.md          # 本文件
├── src/
│   ├── engine/        # LSM 存储引擎（WAL / MemTable / SSTable / Compaction）
│   ├── index/         # 主键 / 组合稀疏索引 / 倒排索引
│   ├── cache/         # HotCache 热点缓存
│   ├── storage/       # 备份还原、文件格式
│   ├── server/        # HTTP / TCP 服务
│   └── cli/           # 命令行客户端
├── config.toml        # 配置文件示例
└── Cargo.toml
```

---

## 文档

- [design.md](./design.md) — 完整技术设计（存储引擎、索引、缓存、备份、分布式蓝图、路线图）
