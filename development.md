# 山水存迹数据库（shanshui-cunji）开发文档

> 版本：v0.1（MVP 单机实现 + 分布式演进蓝图）
> 定位：面向**实现**的配套文档。设计决策与原理见 [design.md](./design.md)，本文件给出模块结构、数据格式约定、实现要点、开发顺序、编码规范与验收标准。
> **修订说明（2026-08-26）**：基于全局复盘，补充字段注册表持久化、倒排段清单、备份倒排文件、Sidecar fork 安全、迁移工具前置、混沌压测等关键缺失，确保开发路径无盲区。

---

## 1. 文档定位与开发原则

**与 design.md 的分工：**

| 文档 | 回答的问题 |
| --- | --- |
| design.md | 为什么这么做（架构、算法、配置、权衡） |
| development.md | 怎么落地（模块划分、接口、格式、任务拆解、规范） |

**开发原则（继承 design.md 第 11 章）：**

1. **先正确后性能**：MVP 优先功能正确 + 数据格式稳定，跑通「写入 → 查询 → 备份 → 还原」全链路后再叠加算法优化；
2. **人工审查并发代码**：并发、锁、内存、文件 IO 逻辑必须人工逐行审查，不依赖 AI 生成核心引擎；
3. **文件格式向后兼容**：SST / WAL / 备份包 / 字段注册表 / 倒排段清单格式一旦定稿，后续版本只能增量演进，新版本可读旧格式。

---

## 2. 技术栈与环境

### 2.1 语言与工具链

- **Rust**（stable，目标 2021 edition；musl 静态交叉编译需对应 target）；
- 构建：`cargo build --release`；交叉编译：`rustup target add x86_64-unknown-linux-musl`；
- 开发调试平台：x86 Linux（Nova OS 移植见第 10 章）。

### 2.2 关键依赖（选型）

| 依赖 | 用途 | 备注 |
| --- | --- | --- |
| tokio | 异步运行时、Semaphore（写 Stall 限流）、后台任务 | runtime-multi-thread |
| dashmap | HotCache / 并发 HashMap / 倒排 Term 字典 | 原理同 Redis dict |
| parking_lot | 更快的互斥锁 / 读写锁 | 双 MemTable 切换 |
| bytes | 二进制缓冲与零拷贝切片 | WAL / SST 读写 |
| crc32fast | 校验和（WAL 记录、备份包、倒排段） | 替代软件 crc |
| xxhash-rust / murmur3 | 布隆过滤器、哈希分片 | 双哈希派生 |
| serde + toml | 配置解析 | config.toml |
| clap | CLI 参数解析 | shanshui-cunji 命令行 |
| axum / hyper | HTTP-JSON 接口 | 单机版即可用 |
| thiserror / anyhow | 错误定义（库）/ 错误聚合（二进制） | 分层使用 |
| mimalloc | **全局分配器（默认，design 14.0）**：消除 musl 默认 malloc 全局锁瓶颈，轻量边缘友好 | `#[global_allocator]`，已落地 |
| jemalloc（tikv-jemallocator） | 分配器统计 + mallctl purge（OOM 看门狗水位线） | feature `alloc-jemalloc`（Linux/musl 推荐） |
| sqlparser-rs | 类 SQL WHERE 子句解析 | 仅受限子集 |
| fst | 倒排字典 FST（阶段 1.5） | 编译后 mmap 挂载 |
| memmap2 | FST / 字典文件 mmap | 冷启动亚秒级 |
| governor | 令牌桶限流（网卡微突发，design 附录 D-2） | `network.bytes_per_second_limit` |
| proptest | 属性测试（键编码、布隆、FST roundtrip） | dev-dependencies |
| loom | 确定性并发模型检查（穷举线程交错，design 18） | dev-dependencies，`cfg(loom)` 门控 |
| tempfile | 集成测试临时目录（崩溃恢复、真实路径） | dev-dependencies |
| mockall | mock 生成（OOM 水位线等） | dev-dependencies |
| dhat | 内存泄漏检测（heap profiler） | dev-dependencies，CI 门禁 |
| criterion | 基准测试（跳表吞吐、查询 P95） | dev-dependencies |

> 二期依赖：io-uring（异步 IO，阶段 3）。

---

## 3. 工程结构

```
shanshui-cunji/
├── design.md            # 技术设计文档
├── development.md       # 本文件（开发实现文档）
├── readme.md            # 项目说明
├── Cargo.toml
├── config.toml          # 配置示例（单机 / 分布式见 Readme「关键配置示例」）
├── metadata/            # 全局元数据（字段注册表 fields.idx、倒排段清单 manifest.json、版本信息）
├── data/                # 数据目录（列族物理隔离，见下方「物理目录约定」）
└── src/
    ├── main.rs          # 入口：解析子命令 → 启动 server / cli / backup / restore
    ├── config/          # 配置模型、加载、校验、环境变量覆盖
    ├── error.rs         # 统一 Error 类型
    ├── keys.rs          # 键编码规范（主键 / 组合 / 倒排），见 4.1
    ├── value.rs         # 文档二进制序列化（TypeTag + 字段ID），见 4.2
    ├── traits.rs        # 【新增】FileSystem / Clock / Allocator 三大抽象（可测试性基石，design 18）
    ├── testing/         # 【新增】mock_clock / mock_fs / mock_allocator / loom_wrapper（仅测试辅助）
    ├── schema/          # 【新增】字段注册表持久化与演进（design 3.4 补充）
    │   └── registry.rs  # FieldRegistry：字段名 ↔ u16 ID，持久化到 metadata/fields.idx
    ├── engine/          # LSM 存储引擎
    │   ├── wal.rs       # WAL 预写日志（组提交 / 双模式）
    │   ├── memtable.rs  # 跳表 + 双 MemTable 切换
    │   ├── sstable.rs   # SSTable 写入器 / 读取器、块、布隆、Zone Map
    │   ├── block.rs     # 数据块（PAX 列组 / 前缀压缩 / 列偏移量表）
    │   ├── bloom.rs     # 布隆过滤器
    │   ├── zonemap.rs   # 块级 Zone Map 统计
    │   ├── compaction.rs # 合并策略 + 写 Stall + 倒排文件 GC
    │   ├── column_family.rs # 列族管理（cf_data / cf_cidx / cf_inv / cf_delta）
    │   └── mv_scheduler.rs # 【新增】物化视图调度器（Cron 聚合，design 19）
    ├── index/           # 索引
    │   ├── cidx.rs      # 组合稀疏索引
    │   └── inverted.rs  # 倒排索引（内存字典 + Append 文件 + 段清单 Manifest + GC）
    ├── cache/           # 缓存
    │   ├── hotcache.rs  # HotCache（LFU/LRU/tiny-lfu + 失效链 + 预热）
    │   ├── blockcache.rs # LRU 数据块缓存
    │   ├── termcache.rs # 倒排 Term 缓存（Top N 片段）
    │   └── external/    # 【新增】外部缓存管理器（design 21，阶段 2）
    │       └── redis_manager.rs # Redis L2 缓存（Cache-Aside + Write-Invalidate + 熔断）
    ├── query/           # 查询
    │   ├── executor.rs  # 查询执行器
    │   ├── optimizer.rs # 查询优化器 Lite（多级索引路由，骨架提前至第 6 步）
    │   ├── context.rs   # 【新增】QueryContext（deadline / cancellation_token / 内存预算）
    │   ├── filter.rs    # 过滤条件解析（field=value / range / AND/OR）
    │   ├── sql.rs       # 类 SQL WHERE 子集解析（sqlparser-rs）
    │   ├── join.rs      # 【新增】JOIN 计划节点 + 本地执行（design 19）
    │   └── explain.rs   # 【新增】EXPLAIN 执行计划推演（design 20）
    ├── watchdog/        # 四层看门狗（OOM 限流 / 写停滞自愈 / 查询熔断 / Sidecar）
    │   ├── oom.rs       # 内存水位线监控
    │   ├── stall.rs     # Compaction 假死检测
    │   ├── query_guard.rs # 查询超时与并发限流
    │   └── sidecar.rs   # 探针（std::process::Command 独立进程，规避 tokio fork 死锁）
    ├── storage/         # 备份还原、文件格式（包含倒排文件与字典快照）
    ├── admin/           # 【新增】运维管理（design 20）
    │   ├── registry.rs  # QueryRegistry：进程级查询/后台任务注册、KILL 联动 CancellationToken
    │   └── status.rs    # 状态聚合：jemalloc stats + 缓存命中率 + LSM + TPS/QPS
    ├── server/          # HTTP / TCP 服务
    └── cli/             # 命令行客户端
└── tools/
    ├── shanshui-cunji-migrate/ # MySQL → shanshui-cunji 迁移工具（基础版提前至阶段 1.5）
    ├── shanshui-cunji-export/  # 【新增】数据导出工具（Parquet / CSV / SQL，design 19）
    └── shanshui-cunji-import/  # 【新增】数据导入工具（CSV/JSON/Parquet + import-schema，design 20）
```

**物理目录约定（新增，解决 G4）：**

```
data/
├── cf_data/     # 主数据列族（含按天时间桶子目录，如 2026-08-26/）
├── cf_cidx/     # 组合索引列族
├── cf_inv/      # 倒排索引列族（含 .inv 文件、段清单 manifest.json）
├── cf_delta/    # 增量列族（阶段 1.5）
├── metadata/    # 全局元数据（字段注册表 fields.idx、版本信息）
└── snapshots/   # 倒排字典 Checkpoint / FST 文件
```

**依赖方向约束：** `engine`、`index`、`cache` 为内核层，不依赖 `server` / `cli` / `query`；`query` 依赖内核层；`server` / `cli` 只依赖上层能力。禁止反向引用。

---

## 4. 通用编码与数据格式约定（实现必须遵守）

> 这些格式是**兼容性契约**，改动必须走版本号升级 + 旧格式读取路径。

### 4.1 键编码（对齐 design.md 3.4）

| 键空间 | 格式 | 说明 |
| --- | --- | --- |
| 主键 | `DocId`（u64 大端，8 字节定长） | 内存 / 磁盘统一；大端保证字节序 == 数值序（LSM 范围扫描 / Zone Map 剪枝依赖） |
| 组合索引 | `VarLen(field1) ++ VarLen(field2) ++ ... ++ DocId(u64)` | 先字段值有序，再按 DocId 有序 |
| 倒排索引 | `TermData := Term ++ RoaringBitmap(DocId 列表)` | 追加式倒排文件（design 5.2），不走 LSM 键 |

- `VarLen(x)` = 4 字节长度前缀（LE） + 原始字节（仅适用于 LSM 键空间：主键 / 组合索引）；
- 编码保证：等值快速 seek、前缀扫描、范围查询；
- 提供 `keys.rs` 纯函数 + roundtrip 单测，禁止在业务代码中手工拼键。

### 4.2 文档值序列化（对齐 design.md 3.4）

```
Document := FieldCount(u32) ++ Field{ FieldID(u16) ++ TypeTag(u8) ++ Payload }
```

- **字段注册表（Schema Registry，新增）**：`src/schema/registry.rs` 维护全局 `字段名 ↔ u16` 映射，持久化到 `metadata/fields.idx`。启动时先读元数据，写入新字段时自动分配 ID 并追加持久化（版本号递增）。**这是反序列化正确性的基石，必须最早实现。**
- `TypeTag`：`0=null, 1=bool, 2=i64, 3=f64, 4=utf8, 5=bytes, 6=timestamp`；
- `Payload`：定长类型固定宽度；字符串/字节 = 长度前缀 + 字节；
- HTTP-JSON 层负责 JSON ↔ 内部二进制互转，存储层只见二进制。

### 4.3 WAL 记录格式

```
Record := Magic(u16) ++ Len(u32) ++ Payload ++ CRC32(u32)
Payload := KeyLen(u32) ++ Key ++ ValueLen(u32) ++ Value
```

- Magic 标识记录类型（Put / Delete / Tombstone / **字段注册表变更**）；
- 组提交：一批记录合并为一次 `fsync`；
- 双写入模式：`Safe`（每次提交刷盘，默认）/ `Perf`（延迟刷盘）；
- **旧文件延迟删除（Deferred Deletion，design 4.3）**：GB 级旧分段禁止直接 `unlink`（持目录 inode 锁 ~100ms 造成写入 P99 毛刺），先重命名 `to_delete_xxx` → 低优先级后台线程空闲期异步 unlink；
- 阶段 3 高性能模式：环形文件 + io_uring + O_DIRECT（见 design.md 4.3，默认关闭）。

### 4.4 SSTable 文件布局（对齐 design.md 4.4）

```
File Header (magic, 版本)
Data Block 0 (PAX 混合列组)
Data Block 1 ...
Block Index (稀疏索引 + Zone Map)
Bloom Filter (布隆过滤器)
Footer (各段偏移/长度/统计)
```

- **Data Block（MVP 先行版）**：键有序 + 增量前缀压缩；**阶段 1.5 升级 PAX**：块头列偏移量表 + 热列组（块头）+ 冷列组（块尾），热字段白名单来自配置；
- **Block Index 条目**：`块首键 ++ 块偏移 ++ 块长度 ++ ZoneMap`；内存只保留稀疏副本（块首键 + 偏移 + ZoneMap）；
- **Zone Map**：块内各字段 `min / max / null_count`，字符串采样前缀最值；用于读块前剪枝；
- **Bloom Filter**：按列族独立生成；位数组 = key 数 × 10 bits，哈希 7 个（双哈希派生）；
- **编码（design 4.4.2）**：DocId / 长度字段用 **Varint（LEB128）**，有序键用**差值编码 + Varint**；
- **块级压缩（design 4.4.2）**：`sstable.compression`（默认 zstd，Level 3），每 Block 独立压缩 + **CRC32 Trailer**；
- **两级索引（阶段 2，design 4.4.2）**：Level 1 内存常驻（每 16 Block 摘要）+ Level 2 精确索引；
- **分区布隆（阶段 1.5，design 4.4.2）**：按块分区，查询只加载目标块；
- **Footer**：各段偏移 / 长度 / 统计信息（含校验和）。

### 4.5 倒排段清单（Segment Manifest，新增，解决 D2）

倒排索引不再依赖"扫描全部文件"重建，而是维护一个**版本化的段清单**：

```
Manifest := Magic ++ Version(u32) ++ SegmentCount(u32) ++ [SegmentEntry, ...]
SegmentEntry := SegmentID(u64) ++ FileName(长度前缀字符串) ++ Offset(u64) ++ Length(u64) ++ TermCount(u64) ++ Checksum(u32)
```

- 每个倒排段（`.inv` 文件）在清单中有一条记录；
- GC 写入新段时：先写临时文件 → fsync → **原子替换 Manifest 文件**（rename）→ 删除旧段文件；
- 启动时只读 Manifest，按清单加载各段元数据（不加载 Bitmap 数据，仅加载 TermMeta 或 mmap 句柄）；
- **崩溃恢复保证**：若 GC 中途崩溃，Manifest 未更新，旧段完好无损；若 Manifest 更新成功，新段有效，旧段被删除。无半成品风险。

### 4.6 格式稳定约束

- 所有磁盘格式文件（SST / WAL / 备份包 / 字段注册表 / 倒排段清单）写入版本号字段；
- MVP 定稿后，新格式只能**新增段/字段**，不得破坏旧格式解析路径；
- 破坏性变更必须：提升格式版本 + 提供迁移工具 + 更新设计文档。

---

## 5. 模块实现要点

### 5.1 config —— 配置体系（神经系统）

- 配置模型分三层覆盖：缓存（design 6.5）、分布式（design 9.8）、加载策略（design 13）；
- 加载顺序：**默认值 → config.toml → 环境变量 `shanshui-cunji__SECTION__KEY`**；
- 启动校验：`hotcache.max_memory_mb + blockcache.max_memory_mb < 系统可用内存 × 0.7`，否则告警并降级；
- 阶段 3：支持 SIGHUP 热加载部分配置（如 `broadcast_query.max_concurrent`）；
- MVP 允许硬编码，但**配置骨架必须从第 1 周就定义**（`src/config/mod.rs` 的 `AppConfig` 结构）。

### 5.2 schema::registry —— 字段注册表（新增，解决 D1，高优先级）

```rust
struct FieldRegistry {
    id_to_name: HashMap<u16, String>,
    name_to_id: HashMap<String, u16>,
    next_id: u16,
    version: u64,
}
```

- **持久化**：每次分配新字段时，立即追加写入 `metadata/fields.idx`（WAL 风格，定期做 checkpoint 压缩）；
- **启动**：先加载 `fields.idx` 重建映射；若磁盘上的二进制文档中出现未注册的字段 ID，启动时即报错（拒绝加载），强制运维先行升级注册表；
- **变更记录**：字段注册表变更（新增字段）也写入主 WAL，保证原子性（与数据写入同事务）。

### 5.3 engine::wal —— 预写日志

```rust
struct WalWriter { file: File, buf: Vec<u8>, mode: WalMode, seq: u64 }
```

- 组提交：`group_commit(batch: &[WalRecord])` 攒批一次 `fsync`；
- 循环分段：文件达上限切分新段，回收时校验 CRC；
- 崩溃恢复：启动时从最后完整记录开始回放重建 MemTable **以及字段注册表变更**；
- 旧文件延迟删除（见 4.3）。

### 5.4 engine::memtable —— 跳表 + 双缓冲

```rust
struct MemTable { map: SkipList<Vec<u8>, Vec<u8>>, size: AtomicUsize }
enum Slot { Mutable, Immutable }
```

- 双 MemTable：写线程只碰 `Mutable`；刷盘线程锁定 `Immutable` 后后台落盘；
- 切换用 `parking_lot::RwLock` 或原子指针 swap，**刷盘不阻塞写入**；
- 内存占用达阈值触发切换 + 刷盘（大小计入写 Stall 判断）。

### 5.5 engine::sstable —— SSTable 读写

- **Writer**：按块缓存写、块满落盘 → 生成块索引 + Zone Map → 写布隆 → 写 Footer；
- **Reader**：内存稀疏索引二分 → Zone Map 粗筛 → 精确 seek 数据块 → 块缓存命中免 IO；
- **Block**：MVP 行式 + 前缀压缩；阶段 1.5 实现 PAX 列组布局（列偏移量表 + 热/冷列组）；
- **TTL 时间分区（阶段 1.5）**：SST 按文档 `timestamp` 路由写入时间桶目录（如按天），桶内 Compaction，过期整目录删除、无墓碑（design 5.4）；
- **编码压缩（design 4.4.2）**：Varint + 差值编码（MVP）；块级 Zstd 压缩（默认 Level 3）+ 每 Block CRC32（阶段 1.5）；分区布隆（1.5）、两级索引（阶段 2）、KV 分离 + Ribbon（阶段 3，可选）；
- **BlockCache**：LRU，Key = `(SST_File_ID, Block_Offset)`，`blockcache.block_size_kb` 与 `max_memory_mb` 可配（design 6.5 / 6.7）。

### 5.6 engine::compaction —— 合并与调度

- MVP：简单合并（全量归并，保证功能链路）；
- 阶段 2：Leveled-Compaction；阶段 3：ionice 最低优先级 + `compaction.rate_limit_mb/s`；
- **写 Stall**：L0 文件数 > `l0_stall_threshold`（默认 8）→ tokio Semaphore 限流前台写入；
- **倒排文件 GC**：基于段清单 Manifest（见 4.5），超阈值后台重写为紧凑段 → 原子更新 Manifest → 删除旧段；
- **Compaction 缓存失效联动（design 6.7）**：完成文件合并后向 BlockCache 发送 `invalidate_blocks(file_id)`。

### 5.7 engine::column_family —— 列族

```rust
enum CfName { Data, CompositeIdx, Inverted, Delta }
struct ColumnFamily { name, memtable, ssts, opts, base_path: PathBuf }
```

- 四个列族独立 MemTable / SST / 缓存 / 布隆 / Compaction；
- **物理目录**（新增）：`data/cf_data/`、`data/cf_cidx/`、`data/cf_inv/`、`data/cf_delta/`；TTL 时间桶按列族独立配置；
- 写入路径按列族分派：`put(docid, doc)` 写 `cf_data`，同时派生写 `cf_cidx`、`cf_inv`；
- 阶段 1.5 增加 **Delta 列族**：键为 `DocId + FieldName`、值为带时间戳的新值，读时 Merge-on-Read 覆盖 Base，老 Delta 由 Compaction 压实进 Base（design 4.7）。

### 5.8 index —— 组合索引与倒排

- **cidx**：`Encode(field1..fieldN, DocId)` → 值为空；前缀覆盖判断在优化器层；
- **inverted（读时 Hash + 写时 Append，design 5.2）**：
  - 内存 `DashMap<String, TermMeta>` 字典 + Append-Only 倒排文件；
  - **段清单管理（解决 D2）**：所有 `.inv` 文件由 `manifest.json`（见 4.5）追踪；写入追加到当前活跃段；GC 产生新段并原子更新清单；
  - **TermMeta**：`file_id(u32) ++ offset(u64) ++ length(u32) ++ doc_count(u32) ++ last_updated_ts(u64)`；
  - **Body 压缩**：统一 RoaringBitmap；
  - **统计载荷**：`doc_count` 常驻内存，COUNT / GROUP BY 零磁盘；
  - **预分片 Chunk（阶段 2）**：列表按 `DocId` 路由归属的物理分片（一致性哈希，design 9.1）物理切段，广播查询按序直拼；
  - **Top-N**：追加序 = 时间序，读 Term 尾部即最新 N 条；
  - **Checkpoint 与 FST 过渡（解决 G6）**：后台定时（`inverted.checkpoint_interval_secs`）将内存字典快照为 `.snapshot`（供重启恢复）；阶段 1.5 引入 FST 后，**Checkpoint 仍保留作为 FST 编译源**，两者共存，重启时优先加载 FST（亚秒），若 FST 不存在则回退加载 Checkpoint。

### 5.9 cache —— HotCache / BlockCache / TermCache

- **HotCache**（对齐 design 6.1 结构）：`DashMap<u64, Document>` + 热度计数；淘汰 `lfu`（默认）/ `lru` / `tiny-lfu`；
- **写失效链**（design 6.6）：Put → ① HotCache 失效 DocId → ② 旧版本倒排 Term 缓存标记失效 → ③ 写 LSM；
- **预热**：1 秒内访问 ≥ `hot_threshold`（默认 5）→ 关联倒排 Term Top N（默认 100）载入 TermCache；`prewarm_on_startup` 从热点统计文件重建；
- 大文档不缓存（`max_document_size_bytes`，默认 100KB）。

### 5.10 query —— 执行器 + 优化器 Lite + 上下文

```rust
struct QueryContext {
    deadline: Instant,
    cancellation_token: CancellationToken,
    max_memory_budget: usize,
}
enum QueryPlan {
    PrimaryGet(u64),
    CompositeScan(CompositeFilter),
    Inverted(Box<InvertedPlan>),
    FullScan { filter, use_zone_map: bool },
}
```

- **QueryContext（新增）**：每个查询携带 deadline 和取消令牌，确保资源隔离（配合 5.14 查询熔断）；
- 优化器路由（design 7.1）：主键点查 → 组合索引前缀覆盖 → 倒排 + Zone Map 剪枝 →（代价估算：term 频率过大则跳过倒排全表扫 + Zone Map 过滤）；
- 开关 `query.optimizer.enabled`（默认开），关闭时走固定路径便于对照压测；
- **回表优化（design 7.3）**：Read Reorder（`(FileID, BlockID)` 排序分组）+ 异步批量 Prefetch + Early Termination（LIMIT 提前截断）；
- **开发顺序调整（解决 G1）**：`optimizer.rs` 的骨架（静态路由）在第 6 步就实现，动态代价估算随倒排统计载荷（阶段 1.5）完善。

### 5.11 storage —— 备份还原（必须包含倒排，解决 D3）

- 冷备份（design 8.1）：
  1. 暂停写 → 刷 WAL + 全部 MemTable；
  2. **主动触发倒排字典 Checkpoint**（`inverted.checkpoint_interval_secs` 立即执行）；
  3. 打包全部 `data/` 目录（含 SST、倒排 `.inv` 文件、段清单 Manifest、字段注册表、Checkpoint 快照） + 元数据；
- 还原：停止服务 → 清空库 → 解压 → 校验完整性/版本兼容 → 重启；
- 备份包格式带版本标记，向后兼容。

### 5.12 server —— HTTP / TCP

- HTTP-JSON：`/put /get /search /range /delete /patch`（对齐 Readme「快速开始」）；
- TCP：紧凑二进制协议（边缘环境）；
- 单机 MVP 不做鉴权/多租户；CLI 与 HTTP 共享同一内核调用路径；
- **分布式边界（design 9.0）**：网关层只做 DocId 路由 / 结果拼接 / 熔断限流，**不实现**跨分片事务、跨分片 JOIN、全局快照读；多行事务由业务层（MySQL/Redis）承担。

### 5.13 cli —— 命令行客户端

- 子命令：`put / get / search / range / delete / patch / backup / restore / server / status / migrate`；
- 读 `config.toml`，输出对齐日志约定（见第 8 章）。

### 5.14 watchdog —— 四层看门狗

- **OOM Guardian**（design 14.1）：每 100ms 读 jemalloc mallctl 统计；RSS > `memory.watermark_high`（0.85）→ 软限流；> `memory.watermark_stall`（1.0）→ 硬限流（返回 `503`）；
- **紧急内存止损（design 14.1.1）**：RSS > 95% 强制所有 Immutable MemTable 优先刷盘；触发 Stall 时缓存容量临时缩容 30%（Cache Shrink）；显式调用 jemalloc `arena.<i>.purge` 归还 RSS，防 OOM Killer；
- **Write Stall Watchdog**（design 14.2，阶段 2）：监控 L0 数量，`stall_timeout_secs`（默认 60s）内无减少判假死 → 中断 Compaction 并重置调度器 → 连续失败 3 次主动退出；
- **Query SLA Guard**（design 14.3）：`QueryContext` 携带 deadline，超时经 CancellationToken 退出；`Semaphore` 限制最大并发倒排合并；
- **Sidecar 探针（解决 D4）**：
  - **强制实现策略**：在 **tokio 运行时初始化之前**，使用 `std::process::Command` 生成一个独立的探针子进程（运行 `shanshui-cunji-sidecar` 或内置 `--mode=sidecar`），通过 TCP/UDS 与主进程心跳。**严禁在 tokio 运行时内调用 `fork()`**，防止异步任务持有锁导致子进程死锁；
  - MVP 备选：独立探活线程 + 文件锁心跳。

### 5.15 query::sql —— 类 SQL 解析（降低迁移成本）

- 基于 `sqlparser-rs` 仅支持 `SELECT ... WHERE ... AND/OR` 子集，解析结果直接复用 `filter.rs` 的 `Filter` 结构；
- **明确拒绝**：JOIN / GROUP BY / 子查询 / 事务 / `UPDATE` 语义，命中即返回明确错误（含"不支持"提示）；
- 不承诺 MySQL 方言兼容（design 15）。

### 5.16 迁移工具（tools/shanshui-cunji-migrate）—— 前置至阶段 1.5（解决 G2）

- **阶段 1.5（基础版）**：
  - 输入：`mysqldump` SQL 文件或 CSV 导出；
  - 映射规则由配置文件 `mapping.toml` 静态定义（MySQL 字段 → shanshui-cunji 字段名 + 类型）；
  - 全量导入，单线程，产出迁移报告。
- **阶段 3（高级版）**：
  - 增量导入（基于主键游标），支持 JDBC 直连，字段映射自动推导，断点续传。

### 5.17 query::agg —— 聚合执行器（阶段 1.5）

- **COUNT**：读内存 TermMeta 的 `doc_count` 直接返回（<0.1ms）；
- **GROUP BY**：遍历字段倒排 Term 集合（内存字典 / FST），取各 Term `doc_count` 构造分组结果；
- MVP 只做 COUNT / GROUP BY；SUM / AVG 留二期。

### 5.18 网关全局 Term 缓存（阶段 2，server/gateway 层）

- 全局 LRU：Key = `分片 ID + Term`，Value = RoaringBitmap；
- **失效心跳（解决 G5）**：写入节点在 1 秒内若某 Term 写入量超过 `gateway.cache_invalidate_threshold`（默认 100），主动向网关发送 `InvalidateTerm` 消息；网关同时采用短 TTL（如 5 秒）兜底。

### 5.19 客户端 SDK 与冷热分层（阶段 3，design 1.3）

- **存储定位**：shanshui-cunji 为纯硬盘持久化（Disk-Based），Redis 为热缓存——冷热分层（Redis 扛热点、shanshui-cunji 扛全量）；
- **`shanshui-cunjiWithRedis` 门面**（Java/Go/Python SDK）：**Cache-Aside 读回填**（先查 Redis → Miss 查 shanshui-cunji → `setex` 回填 TTL）+ **Write-Invalidate 写失效**（先写 shanshui-cunji 落盘成功返回 ACK → `DEL` 旧缓存），业务代码无感切换；**绝不双写/反向同步**（红线，design 21）；
- **MySQL 只读分析副本**：Canal / Debezium 监听 binlog 实时同步到 shanshui-cunji（规避跨库 JOIN 压主库）；
- 与 5.16 迁移工具衔接：binlog 同步是"持续增量"，`shanshui-cunji-migrate` 是"一次性全量"；
- **零侵入**：外部缓存管理器仅在网关 / SDK 层，存储内核无感知；`[cache.external]` 默认关闭（部署详见 redis-integration-guide.md）。

### 5.20 sdk::join —— 二次查询 JOIN 辅助（阶段 1.5，design 19）

- **`queryAndJoin()`**：主表倒排筛选 → 批量回表 → 从表批量主键点查 → 内存 Hash 合并（Left / Inner / Right），一次调用完成；
- 结果集上限 `join.max_rows`（默认 100 万），超限熔断；
- 执行策略：左小先查左 / 右小先查右 / 两表都大→拒绝并引导导出（OLAP）；
- 与 5.10 回表优化联动（Read Reorder + Prefetch + Early Termination）。

### 5.21 写入 Enrich（预连接，阶段 1.5，design 19）

- 网络层接收后、WAL 写入前执行 Enrich 回调（展开关联数据到单文档）；
- 失败策略 `enrich.fail_policy`："reject"（拒绝写入）/ "degrade"（降级写入）；
- 数据源 `enrich.source`：redis / mysql / http / local；关联数据常驻缓存减少重复查询。

### 5.22 engine::mv_scheduler —— 物化视图（阶段 2，design 19）

- Cron / 触发式定时聚合任务：扫描新写入 → 按维度分组聚合 → 写入独立结果文档集；
- 增量模式：基于时间戳游标只处理增量数据；
- 查询直接走结果集的倒排 / 组合索引（毫秒级）。

### 5.23 tools::shanshui-cunji-export —— 数据导出（阶段 1.5 基础版 / 阶段 2 增量 / 阶段 3 JDBC，design 19/20）

**流式管道核心接口：**

```rust
pub trait Exporter {
    fn export(&self, source: &mut DataStream, sink: &mut dyn Sink) -> Result<ExportReport>;
}

pub trait Sink {
    fn write_batch(&mut self, batch: &[Document]) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}
```

- **内置 Sink**：`ParquetSink`（arrow + parquet crate）、`CsvSink`（RFC 4180 + MySQL 转义）、`SqlSink`（SQL INSERT / LOAD DATA 配套文件）、`JdbcSink`（阶段 3，mysql_async / sqlx 直连 MySQL/ClickHouse）；
- **ClickHouse**：Parquet + `INSERT FROM INFILE` 直读；`--dry-run-schema` 生成 MergeTree DDL；`toYYYYMM(created_at)` 分区对齐；
- **MySQL**：CSV + LOAD DATA（比逐条 INSERT 快 ~20 倍）；`--mysql-compatible` 转义 / `--mysql-max-varchar` 超长降级 TEXT（65KB 行限制）；
- **增量导出**：`--incremental --checkpoint`（updated_at 时间戳游标，断点续传）/ `--range`（DocId 游标，无时间字段场景）；
- **资源控制**：范围扫描（顺序 IO）+ 后台 IO 优先级低于前台 + `--rate-limit` + 恒定内存（batch × 单行大小）；`--dry-run` 预评估行数/体积/耗时。

### 5.24 admin::registry —— QueryRegistry（阶段 1.5，design 20）

- 查询 / 后台任务（含 Compaction）进入执行器注册、结束注销（`DashMap<QueryID, QueryContext>`）；
- 字段：ClientAddr / Command / Filter（脱敏）/ Plan / Duration / State / RowsScanned；
- **KILL QUERY**：向目标查询发送 CancellationToken 强制终止（与 5.10 QueryContext 复用）；
- 单元测试注入 mock 执行器验证注册 / 注销 / KILL 全生命周期。

### 5.25 admin::status —— 状态聚合（阶段 1.5，design 20）

- jemalloc stats（rss / allocated / active / metadata）经 `mallctl` 读取；
- HotCache / BlockCache 命中率、倒排 term_count、LSM l0_file_count / total_sst_bytes、write_tps / query_qps；
- CLI `shanshui-cunji admin status` + HTTP `/admin/status`（JSON）；对齐 design 17 监控项。

### 5.26 query::explain —— 执行计划推演（阶段 1.5，design 20）

- 复用优化器路由逻辑，**只推演不读数据**；
- 输出 Access Method / Index Key / Estimated Rows（TermMeta doc_count）/ Zone Map 剪枝预期 / Cost / Execution Pool / Warning；
- CLI `shanshui-cunji explain --filter '...'` + HTTP `/explain`；与 7.1 优化器同源，避免两套逻辑漂移。

### 5.27 tools::shanshui-cunji-import —— 数据导入（阶段 1.5 基础版 / 阶段 2 增强，design 20）

- **核心澄清**：文档型弱 Schema，"导入结构" ≠ CREATE TABLE，而是导入**字段注册表 + 索引定义**（不强约束，新字段自动注册）；
- `import`：CSV / JSON（阶段 1.5）→ Parquet（阶段 2）；`--id-field` 指定 DocId 列、`--timestamp-format` 解析时间、`--batch-size` 批量写入；
- `import-schema`（阶段 2）：YAML/JSON 预创建组合索引 / 倒排字段，优化启动性能；
- 与 5.16 迁移工具复用内核：import 是通用格式入口，migrate 是 MySQL 专用出口。

### 5.28 cache::external::redis_manager —— Redis 外部缓存（阶段 2，design 21）

- **依赖**：redis-rs / deadpool-redis；
- **核心结构**：`RedisCacheManager`（连接池 get / set / del / batch_del）、`CacheAsideExecutor`（读路径：查 Redis → 查 shanshui-cunji → 异步回填）、`InvalidateExecutor`（写路径：先 shanshui-cunji 后删 Redis）；
- **熔断降级**：Circuit Breaker（CLOSED / OPEN / HALF-OPEN），Redis 不可用时自动透传 shanshui-cunji；`timeout_ms` 超时立即 fallback，不阻塞；
- **一致性**：默认 `write_policy = "invalidate"`；可选 `"double_delete"`（延迟 500ms 二次删除）与版本号 / 时间戳校验；
- **防击穿 / 防雪崩**：热点 key 互斥锁回源 + TTL 随机抖动（`300 + rand(60)`）；
- **零侵入**：仅网关 / SDK 层，内核无感知；`[cache.external]` 默认关闭；
- **可观测**：命中率 / 延迟 / 错误计数指标（`shanshui-cunji_cache_redis_*`），`admin cache-stats` / `cache-warm` / `cache-evict`；
- 部署与配置详见 [redis-integration-guide.md](./redis-integration-guide.md)。

---

## 6. 并发与任务调度模型（三池隔离，design 14.1.2）

- **进程模型**：单进程多线程（不采用多进程），共享内存态的 HotCache 与倒排字典；
- **IO/查询主池（tokio，= 核数）**：网络收发、请求解析、主键点查与组合索引查询；
- **倒排/重度计算池（`spawn_blocking` 或 rayon，6~8 线程，Semaphore 并发度 = 核数-4）**：位图解压、AND/OR 交集、聚合计算——防止重计算占满主池导致网络 Ping 超时；
- **后台 IO 池（专用 std::thread，2~4 线程）**：MemTable 刷盘、Compaction 压实、SST 读写（同步阻塞，绑定 io_uring / fsync）；
- 写入路径在主池处理（写 WAL + 写 MemTable 微秒级）；倒排查询与回表取数经 Semaphore 切换到计算池；
- 并发热点（须人工审查）：双 MemTable 切换、写 Stall（Semaphore）、块缓存并发（LRU 内部锁）、倒排字典写锁（按 Term Hash 分片细粒度锁）；
- 内核层（engine/index/cache/schema）不使用 `async` 深穿透，保持同步内核 + 异步外壳；
- **分片继承**：分布式阶段每节点独立进程启动，单机内核零修改复用（design 14.1.2）。

### 6.1 三池配置化与协程/多线程调度策略

> 核心结论：**协程收网络包，线程干重活**——网络层强制用协程（异步），计算与磁盘层强制用多线程（阻塞），两者的比例与模式必须高度可配置。

**协程 vs 多线程本质差异（选型依据）：**

| 特性 | 异步协程（Tokio Task） | 多线程（std::thread） |
| --- | --- | --- |
| 调度方式 | 用户态 M:N，少量 OS 线程多路复用大量任务 | 内核态 1:1，抢占式 |
| 上下文切换 | 纳秒级（纯内存操作） | 微秒级（内核态切换 + 寄存器保存） |
| 栈大小 | 极小（几十 KB，动态增长） | 较大（默认 2MB+，固定） |
| 阻塞容忍度 | 极差：一个 `std::fs::read` 阻塞会卡死整线程上所有协程 | 优秀：阻塞一个线程不影响其他 |
| 适用场景 | 高并发网络 IO、微服务网关 | CPU 密集（位图交集）、同步磁盘 IO（Compaction） |

**调度策略：**

1. **协程（Async）负责"快"**：网络收包、协议解析、主键/组合索引查询，均在 `tokio` 运行时中执行（`async_mode` 可切换多线程/单线程）；
2. **多线程（Blocking）负责"重"**：倒排 RoaringBitmap 交集、SST 压缩、Compaction 合并，强制通过 `tokio::task::spawn_blocking` 或 `rayon` 隔离到独立线程池（`compute_pool_size` / `io_background_threads`）；
3. **配置化原则**：所有池大小、队列深度、调度模式均可通过 `config.toml` 调整，并支持运行时 `SIGHUP` 热加载（阶段 3）；
4. **防御与降级**：
   - 协程限流：`async_max_tasks` 防止协程爆炸（达上限返回 **429 Too Many Requests**，而非无限生成协程撑爆内存）；
   - 计算池满：倒排查询**自动降级为全表扫描 + Zone Map**，不排队等待（防雪崩）；
   - 单线程模式适配：`async_mode = "current-thread"` 时，Group Commit 与 WAL fsync 自动切同步模式，避免 `spawn_blocking` 频繁打断单线程事件循环导致上下文飙升；
5. **分布式零改动**：分片节点以独立进程运行，复用同一套配置体系；网关层单独配置（见 9.8），不侵入单机内核。

**[runtime] 配置（config.toml）：**

```toml
[runtime]
# ========== 1. 异步协程池（网络层） ==========
async_mode = "multi-thread"  # "multi-thread"（默认）| "current-thread"（边缘低功耗）
async_worker_threads = 0     # tokio 工作线程数，0 = 自动检测 CPU 核数
async_max_tasks = 10000      # 最大并发协程数，防止协程膨胀打爆内存

# ========== 2. CPU 密集计算池（倒排/聚合/位图） ==========
compute_pool_size = 8        # rayon / spawn_blocking，0 = CPU 核数 / 2
compute_queue_max = 1000     # 队列上限，满则降级全表扫描 + Zone Map

# ========== 3. 磁盘 I/O 线程池（刷盘/Compaction） ==========
io_background_threads = 4    # 通常 2~4，NVMe 可适当调高
io_uring_enabled = false     # 阶段 3 开启（Linux 5.8+），协程化磁盘 IO
```

**部署场景推荐：**

| 部署场景 | async_mode | async_worker_threads | compute_pool_size | io_background_threads | 理由 |
| --- | --- | --- | --- | --- | --- |
| 高性能云服务器（64 核） | multi-thread | 32（省一半给计算） | 16 | 6 | 网络与计算分离，互不干扰 |
| 标准开发机（16 核） | multi-thread | 8 | 6 | 3 | 平衡模式 |
| Nova OS 边缘盒子（4 核 ARM） | current-thread | 1（单线程事件循环） | 2 | 2 | 避免线程切换开销，极致省电 |
| 纯主键查询场景（无倒排） | multi-thread | 16（全部给网络） | 1（最小化） | 2 | 计算池闲置，把核全让给网络协程 |

**代码级调度路由（防协程被阻塞任务"污染"）：**

```rust
struct RuntimePools {
    async_handle: tokio::runtime::Handle,  // 网络协程池
    compute_pool: rayon::ThreadPool,       // 计算线程池
    io_pool_sender: mpsc::Sender<FlushTask>, // 后台 IO 池（channel 通信）
}

impl RuntimePools {
    // 查询执行路由：按计划类型显式分配到正确池子
    async fn execute_query(&self, ctx: QueryContext) -> Result<Vec<DocId>> {
        match ctx.plan {
            // 主键/组合索引：微秒级计算，协程内直接执行（不阻塞）
            QueryPlan::PrimaryGet(_) | QueryPlan::CompositeScan(_) =>
                self.do_fast_lookup(ctx).await,
            // 倒排位图交集：昂贵 CPU 计算，必须扔进计算池
            QueryPlan::Inverted(plan) => {
                let pool = self.compute_pool.clone();
                tokio::task::spawn_blocking(move || {
                    pool.install(|| compute_bitmap_intersection(plan))
                }).await.unwrap()
            }
            // 磁盘刷盘：交给后台 IO 线程池
            QueryPlan::FlushMemTable => {
                self.io_pool_sender.send(FlushTask).await?;
                Ok(vec![])
            }
        }
    }
}
```

### 6.2 多核配置与 CPU 绑核策略（含配置化）

1. **分区调度**：三池隔离模型针对多核（>8 核）服务器，必须通过 `config.toml` **显式分配核心数**，避免超售（Oversubscription）——Tokio 与计算池默认都抢满核会导致上下文切换风暴，性能反降；
2. **配置公式**（物理核数 N，排除超线程）：
   - `async_worker_threads`：`N × 0.5` 或 `N − 4`（重点保障网络调度）；
   - `compute_pool_size`：`N × 0.3`（保障位图 / 聚合吞吐）；
   - `io_background_threads`：固定 `2~4`（磁盘并发度有限）；
   - 系统预留 ~20% 给 OS 与中断处理；
3. **场景化推荐（16 核）**：极致写入 12/2/2；极致查询 6/8/2；混合均衡（默认）8/6/2；4 核边缘盒 2/1/1；
4. **CPU 绑核（Affinity，阶段 3，默认关闭）**：`[affinity]` 配置将各池绑定到物理核心（`core_affinity` / taskset），隔离 L3 缓存、避免跨核切换毛刺（P99 稳定）；**跳过超线程虚拟核**；跨 NUMA 时避免跨插槽访存（网络池绑 CPU-0、计算池绑 CPU-1）；
5. **自适应**：`cpu_cores_total = 0` 时读取 `/proc/cpuinfo` 物理核数并按公式自动分配，开箱即用（对齐 design 13）。

---

## 7. 开发顺序与任务拆解（修正版）

### 7.1 阶段 1：单机 MVP（4 周，按此顺序）

| 步骤 | 内容 | 对应模块 | 修正说明 |
| --- | --- | --- | --- |
| 1 | 工程骨架、config 模型、Error 类型、日志 | config / error | - |
| 2 | **字段注册表（Schema Registry）持久化（必须最早）** | schema | 解决 D1，反序列化基石 |
| 3 | 键编码 + 值序列化（roundtrip 单测先行） | keys / value | - |
| 4 | WAL（组提交 / 崩溃回放 / 延迟删除） | wal | - |
| 5 | MemTable（跳表 + 双缓冲切换） | memtable | - |
| 6 | SSTable Writer/Reader + 布隆 + 块缓存 + **查询优化器骨架（静态路由）** | sstable / bloom / blockcache / optimizer | **优化器提前**，验证主键/组合索引路由 |
| 7 | 列族框架（含物理目录）+ 主数据 CRUD 打通（写入-查询-重启恢复） | column_family | - |
| 8 | Zone Map（块索引附加统计 + 读块前剪枝） | zonemap | - |
| 9 | Tombstone 删除 / 更新 | engine | - |
| 10 | 组合索引 + 倒排索引（内存字典 + Append 文件 + **段清单 Manifest** + RoaringBitmap） | cidx / inverted | **Manifest 必做**，杜绝 GC 崩溃风险 |
| 11 | HotCache + 失效链 + 配置项（6.5） | hotcache | - |
| 12 | 查询执行器 + 优化器动态路由（代价估算依赖统计载荷，先做最小集枚举） | query | - |
| 13 | **看门狗（MVP 子集）**：查询超时熔断 + OOM 内存限流 | watchdog | - |
| 14 | 备份 / 还原（**含倒排文件与字典快照**） | storage | 解决 D3 |
| 15 | HTTP-JSON + TCP + CLI | server / cli | - |
| 16 | 单元/集成测试、崩溃恢复、简单压测、musl 交叉编译 | 全局 | - |

### 7.2 阶段 1.5：列式优化 + 迁移基础版

1. PAX 混合列式数据块（列偏移量表 + 热/冷列组）；
2. Zone Maps 强化；
3. **TTL 时间分区**；
4. **Delta CF 部分更新**；
5. ~~**倒排统计载荷 + 聚合执行器**（COUNT / GROUP BY）~~ ✅（2026-08-28）：
   倒排 term 升级为 `field=value` 字段维度编码（段格式 v2），`InvertedIndex::doc_count` /
   `iter_terms` / `group_by`，HTTP `GET /count`、`GET /groupby`，CLI `count` / `groupby`；
   测试：137 全绿（含聚合 4 项 + 端到端断言），CLI 冒烟验证（design 5.17 / quality 报告）；
6. ~~**FST + Mmap 字典**（与 Checkpoint 共存，无缝过渡）~~ ✅（2026-08-28）：
   每段刷盘编译 `inverted-{id}.fst` 术语字典（term → 段内条目偏移，`fst` crate），
   查询 O(len(term)) 精确定位、旧段回退线性扫描；字典启动加载 + 刷盘即时更新，
   `inverted.engine` 默认切 "fst"（design 5.2.4.1）；注意：`#![forbid(unsafe_code)]`
   下 memmap2 的 mmap 为 unsafe API，字典暂用 fs::read 加载（FST 压缩结构体积小），
   mmap 化留待独立 crate 封装 unsafe 白名单（P23）；
7. ~~**迁移工具基础版（shanshui-cunji-migrate）**（解决 G2）：支持 mysqldump / CSV 全量导入~~ ✅（2026-08-28）：
   `src/bin/migrate.rs` + 库模块 `src/migrate.rs`（development 5.16）：CSV（首行表头）与
   mysqldump `INSERT INTO` 行解析（字符串/数字/NULL/转义引号），列名即 JSON 字段，
   `docid`/`id` 列作主键否则递增，单线程全量导入 + 迁移报告；测试 147 全绿（+6），
   冒烟验证 CSV 3 行 + SQL 4 行导入正确；
8. ~~**数据关联基础（design 19）**：SDK `queryAndJoin` 二次查询 + 写入 Enrich 预连接~~ ✅（2026-08-28）：
   `src/join.rs` + 配置 `[join] max_rows` / `[enrich]`（development 5.20/5.21）：
   `query_and_join`（主表倒排筛选 → 批量回表 → 从表主键/倒排点查 → 内存 Hash 合并，
   Inner/Left/Right + max_rows 熔断）、HTTP `GET /join`、写入 Enrich
   `put_with_enrich`（WAL 前回调，fail_policy reject/degrade + local 源 `enrich_check_local`）；
   测试 154 全绿（+7），HTTP 冒烟验证 inner/left；Right 语义=Inner（从表无独立筛选，基础版简化）；
9. ~~**块级压缩（Zstd Level 3）+ 分区布隆过滤器（design 4.4.2）**：存储再降 40%~60%、布隆内存减半~~ ✅（2026-08-28）：
   块级压缩 MVP 已有（每块独立 zstd + 独立 CRC32）；本轮实现 **分区布隆（Partitioned Bloom）**：
   SST 格式 v4→v5，每个数据块构建独立布隆（按块内实际 key 数 + `sstable.bloom_fpr`），
   查询先二分定位块、再只校验目标块布隆（按需反序列化，内存减半）；Reader 兼容 v3/v4 旧格式
   （整文件布隆回退）；测试 157 全绿（+3 分区布隆/v4 兼容/fpr 可配）；
9.5. ~~**全局分配器加固（design 14.0）**：mimalloc 默认，消除 musl malloc 全局锁瓶颈~~ ✅（2026-08-28）：
   数据库为高频小块分配大户（JSON 序列化/MemTable/SST 解压/倒排/HTTP），musl 默认 malloc
   全进程单锁，并发 alloc 排队可致 2~7 倍吞吐差；`#[global_allocator]` 引入 **mimalloc**
   （轻量、边缘友好、无 unsafe 声明，不违反零 unsafe 承诺），feature `alloc-jemalloc`
   可选 tikv-jemallocator（mallctl purge + stats，Linux/musl 推荐）；
   测试 157 全绿（含 mimalloc 编译验证），check 六步链通过（P27）；
   **实测（2026-08-28，阿里云 2 核，shanshui-cunji-bench 4 组合压测）**：glibc+mimalloc +30%（×1.3），
   musl+mimalloc **×4.7/×4.2/×9.9**（1/2/4 线程）；musl-system 4 线程全局锁瓶颈暴跌至 30k QPS，
   musl+mimalloc 298k 基本追平 glibc；数据与图表见 `images/allocator-bench/`；
10. ~~**运维管理（design 20）**：`admin processlist`（QueryRegistry + KILL QUERY）+ `admin status`（分配器 stats + 命中率）+ `explain`（执行计划推演）~~ ✅（2026-08-28）：
    `src/admin.rs`（QueryRegistry 注册/注销/列表/KILL 标记，KILL 真正中断留阶段 2 CancellationToken）
    + `admin::status`（分配器 / SST 文件数 / 倒排 / 内存水位，CLI `admin status` + HTTP `/admin/status`）
    + `src/explain.rs`（复用 optimizer::route 只推演不读数据：访问路径/索引键/估算行数，CLI `explain` + HTTP `/explain`）；
    测试 163 全绿（+6），CLI 冒烟验证；KILL 中断依赖看门狗超时 + 阶段 2；
11. ~~**数据管道（design 20）**：`shanshui-cunji-export`（Parquet/CSV 基础版，与迁移工具同期）+ `shanshui-cunji-import`（CSV/JSON 基础版）~~ ✅（2026-08-28）：
    `src/bin/export.rs`（CSV 两列 docid,json，RFC 4180 转义）与 `src/bin/import.rs`
    （CSV/JSONL，复用 migrate 内核，`--id-field` 语义即 docid/id 列，自动分配避让冲突 P28）；
    Parquet/增量/JDBC 留阶段 2+；测试 163 全绿，冒烟验证 import→status→explain→export 全链路；

### 7.3 阶段 2：分布式集群

1. ~~**倒排架构升级（design 5.2）**：预分片 Chunk + 倒排文件 GC~~ ✅（2026-08-28，基础版）：
   - **预分片 Chunk（5.2.1）**：`chunk_for_shard(term, shard_id, shard_count)` 按 `hash64(docid) % shard_count`
     （与 `sharding::route` 同一哈希，分片一致性）抽出归属分片的 posting 子集；
     `concatenate_chunks` 网关侧按序直拼 O(1)（各片 Chunk 互不相交，partition 性质测试验证）；
   - **倒排段 GC（5.2.2 + 5.2.4⑤）**：`gc()` 全段合并为单紧凑段（临时文件 → fsync → 原子更新 Manifest → 删旧段+FST），
     崩溃安全（启动只加载 Manifest，孤儿段忽略）；配置 `inverted.segment_max_size_mb`（默认 1024MB）经引擎注入；
     Tiered 分层合并（每次只合最小 2 段）留后续优化；
   - 测试 182 全绿（+6 Chunk partition/跨段/GC 合并/禁用/重启/阈值）；
2. ~~**分布式全配置清单（design 9.8）**~~ ✅（2026-08-28，阶段 2 首发）：
   `cluster`（node_id / internal_rpc_port）、`sharding`（enabled / total_shards / virtual_shards / shard_key / consistent_hash）、
   `replication`（role / master_addr / sync_mode async|sync / ack_timeout_ms / batch_size / heartbeat_interval_sec）、
   `read_write_separation`、`broadcast_query`（max_concurrent / timeout_ms / reject_without_shard_key）；
   `server.mode`（standalone / cluster）——**standalone 强制关闭分片/副本/读写分离**（design 9.8"单机模式强制 false"）；
   校验：slave 必须配 master_addr、sync 须 ack_timeout>0、virtual_shards>0、shard_key 仅 docid；
   环境变量覆盖 `SHANSHUI_CUNJI__CLUSTER__*` 等 6 项；`admin status` CLI 输出集群配置块（admin::cluster_status）；
   测试 166 全绿（+3 配置解析/强制关闭/非法拒绝）；
3.0. **一致性哈希分片路由器（design 9.1）** ✅（2026-08-28）：
   `src/sharding.rs` 两级路由：`docid → 虚拟分片`（hash64 % virtual_shards，固定不可变）→ `物理节点`
   （一致性哈希环，每节点 128 虚拟点）；`route(docid)` 单分片定位（写/主键查询无广播）、`nodes()` 广播目标集；
   **平滑扩容属性验证**：3→4 节点仅迁移 ~1/4 虚拟分片、docid 重路由比例 ≈0.25（测试断言 0.13~0.30）；
   测试 171 全绿（+5 确定性/均匀性/扩容迁移量/稳定性/单节点）；网关层路由复用入口（阶段 2 后续）；
3. ~~**分片节点 RPC → 网关 + 元数据中心 → 广播检索 / 虚拟分片扩容 / 主从高可用**~~ ✅（2026-08-28，基础闭环）：
   - **`src/rpc.rs`**：极简 JSON-over-TCP RPC（`[u32 LE 长度][JSON]` 帧，同步 std::net 与内核一致）；
     `RpcServer`（线程池 + 按 method 分发）/ `RpcClient`（读写超时防挂死）；`register_shard_handlers`
     把 Engine 的 put/get/倒排 chunk 检索/ping 暴露为 RPC 方法（单机内核零修改复用，design 9.3）；
   - **`src/meta.rs` 元数据中心**：节点注册/摘除（自动重建一致性哈希分片映射，`resolve(docid)` 单分片路由）、
     广播目标集（顺序即 Chunk 拼接顺序）、主从角色（master/slave）、拓扑 JSON 持久化（tmp+rename 原子写）；
     扩容重路由 ≈25% 测试验证（design 9.1 平滑扩容）；
   - **`src/gateway.rs` 网关**：不持有数据，三类转发——写/主键点查（`resolve` 单分片路由，design 9.1）、
     广播检索（全节点取本片 Chunk → `concatenate_chunks` 按序直拼 O(1)，design 9.2/5.2.1）、
     健康探活（`ping_all` 失活节点检测）；`ShardEndpoint` 抽象：进程内 `LocalShardEndpoint`（测试）+
     跨进程 `RpcShardEndpoint`（真实 TCP）；红线遵守（design 9.4：不跨片 JOIN/事务，只合并 DocId）；
   - **端到端验证**：2 个 Engine 分片节点真实 TCP 启动 + 元数据中心 + 网关 → 写入路由到归属节点、
     主键点查、广播检索跨片直拼、探活全通过；
   - 复制（主→从 async/sync）见下方补充；元数据中心自动故障切换留阶段 2 后续；测试 197 全绿（+15）；
  3.1. ~~**主从复制（design 9.3）**~~ ✅（2026-08-28）：
    `src/replication.rs`：`ReplicationLog`（追加持久化 + seq 单调游标，崩溃重启恢复未推送增量）；
    `Replicator`（async 攒批后台推送 / **sync 立即推送等 Slave ACK** `ack_timeout_ms`，复用 RPC 连接）；
    Slave 侧 `register_repl_handlers` 暴露 `repl.apply`（批量幂等应用 put/delete + 返回 acked_seq）；
    接入点 = 分片节点 `shard.put`（单机内核零修改）；测试验证 sync 即时落 slave / async 按需推送 /
    delete 传播 / 无 slave noop / 重启恢复；
4. ~~**看门狗补全：写停滞假死检测自愈 + Sidecar 探针**~~ ✅（2026-08-28，检测/自愈判定 + 心跳基座）：
   `watchdog::StallWatchdog`（design 14.2）：周期采样 L0 文件数，`compaction.stall_timeout_secs`（默认 60s）
   内无减少判 Compaction 假死 → 自愈信号（中断 Compaction + 重置调度器由存储层接动作），连续
   `max_consecutive_failures`（默认 3）次 → FatalExit（由外部 systemd/Sidecar 重启）；
   `watchdog::HeartbeatSidecar` + `HeartbeatProbe`（design 14.4 MVP：独立探活线程 + 文件锁心跳，
   `sidecar.ping_interval_sec` 默认 5s × `max_missed_pings` 默认 3 判死锁；禁止 fork，独立子进程拉起留阶段 2 后续）；
   配置新增 `[compaction]` / `[sidecar]`（design 14.5）；测试 176 全绿（+5 假死判定/恢复/FatalExit/心跳存活/缺失判死）；
5. ~~**网关全局 Term 缓存** + 失效心跳~~ ✅（2026-08-28，缓存 + 写计数失效 + TTL 兜底）：
   `src/term_cache.rs`（design 9.9）：Key = (节点 ID, Term)，Value = 压缩 RoaringBitmap（LRU `term_cache_max_entries`）；
   **命中直出**（广播查询不透传后端分片，测试用计数端点验证 0 回源）+ **TTL 兜底**（`term_cache_ttl_secs` 默认 5s，过期重查）
   + **写计数失效**（`term_cache_invalid_threshold` 默认 100：1 秒窗口写入超阈值 → 主动失效该 Term 全节点缓存）；
   网关 `put` 记录写计数、`broadcast_search` 命中直出/未命中回填；配置见 `[broadcast_query]`；
   测试 197→211（+14：复制 6 + Term 缓存 5 + 网关集成 3）；失效心跳（节点写计数 → RPC 通知网关）留阶段 2 后续；
6. ~~**术语字典热备 TDS（design 9.10）**~~ ✅（2026-08-28，基础版）：
   `src/tds.rs`：`TdsServer`（RPC `tds.put/get/list`，内存 + **文件写穿持久化** `{dir}/{node}/{seg}.dict`，
   TDS 自身重启不丢快照）+ `TdsClient` + `sync_dicts_to_tds`（刷盘后上报，预加载/蓝绿切换基础）+
   `restore_dicts_from_tds`（重启节点拉回字典写本地 `.fst`，无磁盘重建 IO；TDS 不可用回退本地，降级可用）；
   字节经 hex 编码走 JSON-RPC（零依赖无 unsafe）；测试验证 put/get/list 往返、TDS 重启恢复、上报→冷节点恢复；
7. ~~**无损扩容协议（design 9.1.1）**~~ ✅（2026-08-28）：
   `src/reshard.rs`：`compute_moved_vshards`（新旧节点一致性哈希归属变化集合，只迁移 ~1/N 虚拟分片）；
   `Migration`（双写/追平/切换状态机）；网关集成三步：**双写（Shadow Writes）**——迁移分片 docid
   写老节点后同时写新节点（`begin_migration`，新节点接入端点但不入元数据中心，路由暂不变）；
   **数据追平（Delta Catch-up）**——全量扫描老节点 + `extract_terms` 重新派生词条拷贝到新节点
   （语义等价 SST 拷贝 + WAL 增量，`catch_up`）；**原子切换（Atomic Switch）**——新节点注册元数据中心，
   路由切换、双写关闭（`commit_migration`）；**回滚预案** `abort_migration`（不注册、路由不变、旧数据完好）；
   测试验证：迁移生命周期数据零丢失（广播全量一致）、回滚路由不变、重复迁移拒绝；测试 221 全绿（+10）；
8. ~~**数据关联增强（design 19）**~~ ✅（2026-08-28，物化视图调度器基础版）：
   `src/mv.rs`（development 5.22）：`MaterializedView`（维度分组聚合 Count/Sum/Avg + **docid 增量游标**，
   重复刷新同批次自动跳过）+ JSON 持久化（含定义，重启完整恢复）；`MvScheduler`（多视图容器 +
   `refresh_all` 触发式增量聚合 + `query` 内存查表毫秒级）；JOIN 计划节点本地执行 = `sdk::join`
   已单分片内存 Hash 合并（设计 19）；`shanshui-cunji-export` 基础版 M4 已交付（Parquet 留阶段 3）；
9. ~~**两级索引（design 4.4.2）**：内存索引减少 90%~~ ✅（2026-08-28）：
   `SstReader` 重构为两级索引——**Level 1 常驻**（每 `sstable.index_granularity` 默认 16 块一条摘要：
   块首键 + 块下标，内存 ~1/16）+ **Level 2 按需加载**（精确 Block 索引懒加载，首次访问解码缓存）；
   open 后不再持有完整 IndexEntry（1.5 亿文档单层 ~200MB → 两级 ~20MB，更多留给 HotCache）；
   `index_granularity` 经 ColumnFamily 注入；测试验证摘要粒度/懒加载/跨块查询/范围扫描完整；
   测试 229 全绿（+8：两级索引 3 + 物化视图 5）；
10. ~~**数据管道增强（design 20）**~~ ✅（2026-08-28，import-schema）：
    `src/import_schema.rs`（development 5.27 阶段 2）：`ImportSchema`（JSON：id_field / 倒排字段白名单 /
    组合索引声明 / 时间戳游标）+ 加载校验（非法时间格式/空组合键拒绝）+ `apply`（预注册字段到 FieldRegistry，
    **预创建索引的字段基座**，返回 SchemaReport）；`import_csv_filtered` / `import_json_filtered` 支持
    **倒排字段白名单**（只对声明字段建索引，减少写放大）；`shanshui-cunji-import --schema schema.json`
    CLI 接入；Parquet / JdbcSink 留阶段 3；测试 229→233（+4 schema 加载/校验/apply/白名单）；
11. ~~**Redis 外部缓存（design 21）**~~ ✅（2026-08-28，基础版）：
    `src/redis.rs`：极简 RESP 客户端（std TcpStream 零依赖，PING/GET/SETEX/DEL + 超时 + 重连）；
    `src/external_cache.rs`：`CacheBackend` 抽象（Redis / 测试内存双实现）+
    **Cache-Aside**（`get_or_load`：命中直返 → 回源 SETEX 回填，TTL 抖动防雪崩 + `cache_null_values` 防穿透）+
    **Write-Invalidate**（`invalidate`：invalidate / **double_delete**（500ms 二次删）/ none 三策略）+
    **熔断器**（CLOSED→OPEN→HALF-OPEN 状态机，熔断直接透传引擎，仅延迟上升不雪崩）+ 统计（命中/回源/旁路/熔断）；
    `[cache.external]` 配置（默认关闭，design 21.3 单机纯净）+ 校验；Mock RESP 服务端测试协议往返；
    测试 233→241（+8 RESP 往返/拒绝连接/Cache-Aside/空值防穿透/失效/熔断恢复/双删）；

### 7.4 阶段 3：深度优化

- Leveled-Compaction、io_uring + 环形 WAL、IO 优先级调度器、配置热加载；
- 位图索引、MVCC、热 key 自动缓存、压缩、增量备份；
- **迁移工具高级版**（增量 + JDBC 直连）；
- **Redis 冷热分层 SDK（`shanshui-cunjiWithRedis` 门面）+ MySQL binlog 同步（Canal/Debezium 只读分析副本）**（design 1.3）；
- **小表广播 JOIN（可选，design 19.3）**：`join.broadcast_enabled` 默认关闭，`broadcast_threshold` 默认 100 行；导出到 ClickHouse 直连；
- **KV 分离存储 + Ribbon Filter（design 4.4.2，可选）**：写放大降 50%~80%（WiscKey 式 Value Log）；
- **JDBC Sink 直连（design 20.5，阶段 3）**：MySQL / ClickHouse 实时同步（延迟 < 1min），`--rate-limit` 限流；
- **前沿演进调研（design 22）**：YCSB 基准压测定位瓶颈；GPU 加速 Compaction、PMEM WAL、Learned Index、AuraDB（WAL-time KV 分离 / RL-driven Compaction）可行性评估；CXL 长期跟踪；
- **存算分离（远期蓝图）**。

**阶段 3 落地进度**（每小任务一次提交）：

1. ~~**配置热加载（design 7.4）**~~ ✅（2026-08-28）：`Config::reload`（重读+校验+原地替换）+
   `ReloadReport`（changed_sections / error 字段）+ CLI `shanshui-cunji reload`；测试验证热加载生效/无效路径报错/变更节上报；
2. ~~**IO 速率调度器（design 4.5）**~~ ✅（2026-08-28）：`src/io_scheduler.rs` 新增 `IoRateLimiter`
   **Token Bucket**（acquire 按速率补桶、桶内突发、`0`=不限速），`[storage] io_rate_limit_mb` 配置；
   测试验证稳态限速/突发/不限速；
3. ~~**基础 Compaction（design 4.5）**~~ ✅（2026-08-28）：`ColumnFamily::compact()` 全量合并
   （key 升序 seq 降序去重、tmp→fsync→原子 Manifest→删旧段）+ `CompactReport` + `needs_compact`
   （L0 段数超阈值）+ 合并 IO 走 `io_acquire` 限速 + CLI `shanshui-cunji compact`；测试验证去重/原子切换/限速；
4. ~~**迁移工具高级版·增量导入（design 5.16）**~~ ✅（2026-08-28）：`import_csv_incremental` /
   `import_json_incremental`（**docid 游标断点续传**）+ `load/save_checkpoint`（tmp+rename 原子）+
   `ImportReport.skipped` + CLI `--incremental --checkpoint`；测试验证续传/跳过已完成/首轮全量；
5. ~~**小表广播 JOIN（design 19.3）**~~ ✅（2026-08-28）：`join.broadcast_enabled`（默认关）/
   `broadcast_threshold`（默认 100）配置；`query_and_join` 在去重关联 key 数 ≤ 阈值时
   **一次全量扫描从表建内存索引**（docid 关联用主键、其余取字段值、缺字段跳过、首个命中优先），
   否则回退逐 key 点查；server 链路（serve→handle_join）透传 `JoinBroadcast`；测试验证广播命中/
   首个命中语义/超阈值回退/关闭广播/决策函数；
6. ~~**Redis 冷热分层 SDK 门面（design 1.3/21.5）**~~ ✅（2026-08-28）：
   `src/sdk_cache.rs` 新增 `ShanshuiCunjiWithRedis<'a, B>` 门面——组合 `&mut Engine`（全量持久化）+
   `ExternalCacheManager<B>`（Redis 热点）：**读回填**（`get` 命中 Redis 直返 / 未命中回源引擎
   并 SETEX 回填，熔断透传）+ **写失效协调**（`put` / `delete` 先落盘引擎返回 ACK 再删 Redis 旧缓存）；
   `engine()` 透传引擎访问；测试验证读回填/写失效后回源新值/删除双端清理；测试 260 全绿（+3）；
7. ~~**性能实测对照 design 9.5**~~ ✅（2026-08-28）：三规模 10/10 通过（1000万 29.6s /
   2000万 67.9s / 5000万 163.6s 批量插入，写入 29.5~33.8 万条/s；倒排检索近常量 1.25 亿命中 2.9s）；
   对照 design 9.5：组提交写入与热点查询延迟**达成/超出**目标（硬件 6C/16G 低于基准 16C/64G），
   高并发 10k 连接类指标留待 M6 异步运行时；**发现并修复读路径回归**（`get_from_sst` 每次点查
   克隆整个 Level 2 精确索引致倒排查询挂起 → `locate_indexed_block` 按需取单条，恢复 2.4s）；
   报告 `images/perf-0.3.0/汇总报告.md` + 每规模 10 张截图（Edge headless）；
8. ~~**打 v0.3.0 标签**~~ ✅（2026-08-28）：Cargo.toml 版本 0.2.1→0.3.0 + `RELEASE-v0.3.0.md` /
   `RELEASE-SUMMARY-v0.3.0.md` / README 发布摘要更新 + `git tag v0.3.0` + 推送；阶段 3 全部完成，
   测试 260 全绿，三规模 demo 10/10。

### 7.5 M6 高性能写入模式（design 4.3 阶段 3 末）

1. ~~**环形 WAL（design 4.3）**~~ ✅（2026-08-28）：`RingWal`——预分配固定大小文件 + 写指针循环移动
   （省去文件扩展与 inode 元数据更新），记录格式与追加 WAL 兼容；**覆盖安全**：回绕仅允许在
   整环无未刷盘记录时（Flush 后 `set_flushed_seq` 上报游标），否则 `Error::WalFull` 触发上层强制 Flush；
   **崩溃安全**：sync 两阶段（记录区 fsync → 头部 tail fsync），恢复恒为线性区间 `[20, tail)`；
   `[storage] wal_mode = "ring" / "append"`（默认 append）+ `wal_ring_size_mb`（默认 64）+ 校验；
   ColumnFamily 经 `WalBackend` 统一分发（put/delete/sync 遇 WalFull 自动 Flush 重试）；
   测试验证 回环往返/回绕恢复最新周期/未刷盘拒覆盖/容量预检/崩溃重开/集成强制 Flush 数据完整；
   测试 260→267（+7）；io_uring 内核接入（Linux 5.8+ 异步提交）与 O_DIRECT 留待 Linux 部署验证；
2. ~~**Leveled-Compaction（design 4.5 二期）**~~ ✅（2026-08-28）：SST 分层压实——Manifest 新增
   `levels` 层号（旧 Manifest 全 0 兼容），刷盘产物入 L0、压实产物入 L1/L2；`select_compaction_inputs`
   **有界压实**：L0 ≥ 2 段时合并 L0 → L1（单次压实量 = 刷盘批次；L1 达层上限则 L0+全部 L1 收敛），
   L0 空且 L1 > 1 时 L1 → L2 下沉；`needs_compact` 分层判定；测试验证 L0→L1 / L1→L2 / 层号持久化 /
   选择函数；测试 267→270（+3）；
3. ~~**MVCC 快照读（design 4.7 二期）**~~ ✅（2026-08-28）：`Engine::get_at(docid, snapshot_seq)`
   快照读——主数据 `ColumnFamily::get_bytes_at` 遍历 MemTable + 全部 SST 取 **seq ≤ 快照点** 的最新版本
   （Tombstone 语义保留，快照点前历史版本仍可见）；`Engine::begin_snapshot()` 返回当前最大已分配 seq；
   快照读不走 HotCache（避免污染热缓存）；`Engine::flush_primary` 强制刷盘供测试/备份；
   基础版语义：快照隔离覆盖主数据版本，Delta 字段级热更即时叠加（独立 seq 空间，完整跨列族全局
   seq 一致性留后续；MemTable 单版本，未刷盘覆盖的历史版本不可回读）；测试验证 刷盘后历史版本回读 /
   快照后写入隔离 / 删除前快照可见 / Delta 叠加；测试 270→273（+3）；
4. ~~**热点 key 自动缓存（design 14.1.2）**~~ ✅（2026-08-28）：HotCache 增加**保护区**
   （容量 = 主缓存 1/5）——访问计数达 `hotcache.hot_threshold`（默认 5）自动从主缓存晋升保护区，
   普通淘汰避让（LFU 选择跳过保护区 key），写失效 / 硬预算兜底仍可清除；热点 key put 原地更新
   不重置热度；`protected_len()` / `promotions()` 监控；测试验证 晋升/冷数据挤压存活/失效清除/
   原地更新/未达阈值不晋升；测试 273→277（+4）；
5. ~~**增量备份（design 20）**~~ ✅（2026-08-28）：`Engine::backup_incremental(since_seq, path)`
   ——导出 seq ∈ (since_seq, 当前] 的 WAL 记录（append WAL 全量保留、环形 WAL 重放环内），
   JSON 原子落盘（tmp+rename）；`WalRecord` 补 Serialize；`WalBackend::recover_records` /
   `ColumnFamily::wal_records_since` 统一取数；**缺口检测**：since_seq ≠ 0 且最旧可用 seq >
   since_seq+1 → 报错提示改做全量备份（环形覆盖 / 长时间未备份场景）；
   `Engine::restore_incremental(path)` 按序重放（PUT 重新派生倒排词条 / DELETE 写墓碑）；
   `BackupReport`（since/until/records）；测试验证 全量点+增量还原（PUT/删除/保留）、since=0 全导出；
   测试 277→279（+2）；
6. **收尾**：✅ 性能回归快检（2026-08-28，1000万 10/10，插入 38.6 万条/s 无回归，`images/perf-0.4.0/`）；
   M6 全部 5 项功能完成、测试 279 全绿；打 v0.4.0 标签待确认（版本号 + RELEASE 说明 + 分支同步）。

### 7.6 M7 深度优化二阶段（design 4.7/5.2.4/22）

1. ~~**MVCC 全局 seq 一致性（design 4.7 完善）**~~ ✅（2026-08-28）：`Engine` 增加**全局 seq 分配器**
   （`Arc<AtomicU64>`，primary / delta 列族共享）——`ColumnFamily::set_external_seq` 接入后所有写入
   （put / delete / patch / delta 清理）从全局计数分配 seq（`WalWriter/RingWal::append_at` 指定 seq，
   内部 next_seq 同步推进保持单调）；`get_at` 的 **Delta 增量按全局 seq 过滤**（快照后的字段级热更
   不可见，null 删除 / Tombstone 均按快照点判定）——补全 M6-3 遗留的跨列族快照隔离；
   `begin_snapshot` / `current_seq` 读全局计数；重启后从各列族 WAL 恢复全局起点；
   测试验证 快照隔离 Delta / null 删除 / 跨重启 seq 接续；测试 279→281（+2）；
2. ~~**位图索引增强（design 5.2.4/7.2）**~~ ✅（2026-08-29）：枚举字段**内存位图加速**
   COUNT/GROUP/AND——`[inverted] bitmap_fields` 白名单（默认关闭零开销）+ `InvertedConfig::bitmap_fields`；
   `InvertedIndex::with_bitmap_fields` 启动时全量重建（遍历内存 + 各段 posting，term 拆分 `field=value`
   命中白名单 → 内存 `field → (value → RoaringBitmap)` 常驻）；写路径 `add` 同步维护；快速路径
   `bitmap_count`（COUNT 亚毫秒）/ `bitmap_group_by`（GROUP BY 各值计数）/ `bitmap_and`（组合 AND 交集）；
   `Engine::inverted_doc_count` / `inverted_group_by` 命中白名单走位图、否则回退倒排段扫描，
   新增 `inverted_bitmap_and_count` 组合筛选；测试验证 内存写入 / 重启从段重建（drop 前刷盘）/ 引擎级快速路径；
   测试 281→285（+4）；
3. ~~**YCSB 压测定位瓶颈 + 前沿调研报告（design 22）**~~ ✅（2026-08-29）：新增 `shanshui-cunji-ycsb`
   （src/bin/ycsb.rs，YCSB 规范负载 a/b/c/f，自实现 splitmix64 伪随机 + 延迟分位数统计）；
   压测结论（详见 `images/perf-0.5.0/验证记录.md`）：冷读 18 万 ops/s（P50≈5µs）、热缓存 87 万 ops/s、
   100 万数据 SST 分层后读不掉速；**fsync 单条串行为写路径头号瓶颈**——A 写重带 fsync 仅 2,077 ops/s、
   无 fsync 113,587 ops/s（**55×**）；优化建议 Group Commit（design 4.1.3 已规划未实现）+ 读写分离（P0）；
   前沿调研（`frontier-research-2026-08.md`）：BVLSM WAL-time KV 分离（7.6× RocksDB）/ RusKey RL Compaction
   （4×）/ DobLIX·TieredKV 学习索引 / AuraDB Rust 生态；建议 近期组提交、中期大 value KV 分离、长期 RL+学习索引；
4. ~~**收尾**~~ ✅（2026-08-29）：文档（development 7.6）+ demo 1000万 快检 10/10 + 打 **v0.5.0** 标签
   （RELEASE 说明/摘要/README/quality_system 对齐 285 测试）+ feature/master/release 分支同步。

### 7.7 M8-P0 Group Commit 组提交（design 4.3，YCSB 实证瓶颈修复）

> 前置：M7-3 YCSB 压测实证「fsync 单条串行」为写路径头号瓶颈（A 写重 2,077 vs 无 fsync 113,587 ops/s，55×）；
> 方案调研见 `group-commit-design.md`（业界：InnoDB 三段组提交 / PG commit_delay / RocksDB leader-follower /
> ScyllaDB commitlog / RocksDB #14627 无 leader 演进——共识「窗口内写入共享一次 fsync」）。

1. ~~**Group Commit 组提交实现**~~ ✅（2026-08-29）：`[storage] group_commit_us`（默认 0 = 关闭，逐条 fsync 强安全）
   + `group_commit_bytes`（默认 256KB）；`WalWriter/RingWal` 增加 `pending_bytes` / `sync_due`（距上次 fsync ≥ 窗口
   或待刷 ≥ 字节阈值）；`ColumnFamily::wal` 改 `Arc<Mutex<WalBackend>>`（共享句柄 `wal_handle`）；
   `Engine::maybe_group_commit`——关闭时逐条 fsync（现状），开启时**写路径零 fsync**，spawn 后台提交线程
   按窗口统一落盘（ack 后最多延迟 ≤ 窗口，字节阈值同样由后台线程判定），`Drop` join + 最终落盘；
   `backup_incremental` 前置 `flush_wal`（保证导出记录已持久化）；
   测试 5 个新增（默认关闭持久 / 窗口攒批 drop 完整 / 后台兜底尾部落盘 / 备份前置落盘 / 大窗口攒批行为）；
   测试 285→290（+5）；
2. ~~**YCSB 验证（A 写重）**~~ ✅（2026-08-29）：2ms 窗口 **91,296 ops/s**（基线 2,003 → **45×**，P50 7.8µs，
   达无 fsync 上限的 80%）；1ms 窗口 75,330 ops/s（37×）；`shanshui-cunji-ycsb --group-commit-us` 支持参数对比；
   首版实现（写路径 + 后台线程双份 fsync）实测反而更慢（1176 ops/s）→ 改为提交器模式后达 91K（详见 P37）。

### 7.8 M8-P1 读写分离研究（demo-first 流程首例，结论：暂缓）

> 流程验证：按用户规范，先在 `src/demo/rw-separation/`（独立 mini crate，gitignore 不提交）研究三种并发模型，
> 单元 + 边界测试跑通后，再决策是否整合 kernel。

1. ~~**研究 demo（Mutex / RwLock / COW 快照读）**~~ ✅（2026-08-29）：`DocStore` 统一接口三实现 +
   并发正确性测试 6 个 + 边界测试 4 个（单 key 高频覆盖 / 冷启动并发 / 多快照独立 / 并发写无丢失）+
   release 压测（读重 90/10、写重 50/50，4 线程）；
2. ~~**研究结论**~~ ✅：**读写分离暂缓实施**——
   - 压测：纯内存路径 Mutex vs RwLock 吞吐几乎无差（3.87M vs 4.22M ops/s）——锁开销可忽略，收益不来自网关锁；
   - COW 快照读**不可行**：全量 map 拷贝写放大（写重仅 3.4K ops/s）；
   - 真实引擎验证（B 负载）：组提交 2ms 后 **18,987 → 149,539 ops/s（7.9×）**，P95 1050µs→64µs——
     「读被写拖垮」根因是 put 带 fsync 阻塞全局锁，**组提交零 fsync 后已解决**（B 达纯读 83%）；
   - 结论：RwLock 网关锁剩余收益 <20%，而 Engine 方法 &self 化改动面大——**不作为下一步**；
3. 边界发现（demo 内测试暴露）：COW 并发写丢更新两类竞态（clone 在写锁外 / clone-swap 两次加锁）——
   正确写法须单次写锁全程持有 clone-modify-swap；此类坑已记录供后续若引入 COW 快照参考。

### 7.9 M8-P2 环形 WAL + 组提交组合（demo-first，结论：功能正确，性能定位「WAL 空间有界」）

1. ~~**组合研究 demo（src/demo/ring-wal-gc/，gitignore 不提交）**~~ ✅（2026-08-29）：path 依赖主体 lib，
   4 测试全绿——高写超容量触发强制 Flush+回绕不丢、崩溃恢复（mem::forget 模拟，窗口内未 fsync 丢失 ≤ 窗口）、
   显式 Flush 前移 flushed_seq 覆盖安全、极小环形高刷频无 panic；
2. ~~**YCSB 组合压测**~~ ✅：append+gc 2ms **91,296 ops/s（最优）**；ring+gc 关 564（环形 sync 双 fsync——
   记录区+头部 tail，逐条成本翻倍）；ring+gc 2ms 30,270；ring 4MB+gc 2ms 55,570（小环形更早强制 Flush）；
   `shanshui-cunji-ycsb` 增加 `--wal-mode` / `--ring-size-mb` 参数；
3. ~~**结论**~~ ✅：组合**功能正确无需修复**；环形 + 组提交定位「WAL 空间有界」可选组合，
   默认推荐 append + 组提交（性能最优）；后续可优化点：环形头部 tail 不必每次 fsync
   （scan_ring 容忍 tail 略旧，不会读到垃圾——列入候选优化）。

### 7.10 5000 万条 Parquet 数据集 + 数据库导入（数据资产，保留不删）

1. ~~**数据集生成器（shanshui-cunji-gen-dataset）**~~ ✅（2026-08-29）：分块流式生成 Parquet
   （arrow 59 + parquet 59，Snappy 压缩），**5000 万条 × 20 字段**（9 数值型：Int64×7/Int32×2/Float64×1 +
   2×256 字符文本 big_text_a/b + 8 短字符串枚举 + Boolean），内存恒定、每 100 万条进度输出（可后台运行）；
   `--rows/--batch/--seed/--out`；实测 5000 万条 → 3,437 MB / 314.6s（~15.9 万 rows/s）；
   数据资产：`D:\shanshui-data\ds-50m.parquet`（**建立后不删**，除非改存储/结构）；
2. ~~**Parquet 批量导入（import_parquet）**~~ ✅：`migrate::import_parquet` 读 parquet → `put_nosync`
   批量写（每 50 万条统一 fsync + 结尾统一提交，5000 万条逐条 fsync 需数小时、批量分钟级）；
   主键 docid 列优先否则递增；20 字段类型 roundtrip（Int64/Int32/Float64/Boolean/Utf8，null 列容忍）；
   `shanshui-cunji-import --parquet <in.parquet> [--data-dir]`；导入数据库：`D:\shanshui-data\db-50m`；
3. ~~**dataset-test demo（src/demo/dataset-test/，gitignore 不提交）**~~ ✅：4 测试全绿——含 docid 导入
   roundtrip（行数/点查/倒排计数/数值类型/256 字符长度）、无 docid 自动分配、null 列容忍、导入后重启持久化；
4. 依赖说明：新增 `arrow` / `parquet`（59.2，Apache-2.0 合规，deny.toml 白名单内）——数据集生成与
   parquet 导入必需；编译体积增大但仅作用于导入/生成路径。

### 7.11 M8-P4 倒排字段白名单 / 黑名单 / 长文本保护（防字典膨胀）

> 背景：100 字段表全字段建倒排——高基数 ID / 长文本字段每 term 单 posting 纯浪费。
> demo 研究（src/demo/inverted-whitelist/，gitignore 不提交）实测 10 万文档 × 100 字段：
> 全字段 550 万唯一 term / 246MB vs 白名单 20 字段 **12 个唯一 term / 3.5MB**——**字典压缩 45 万倍、
> 写放大 27.7 倍**（100 字段表倒排 ≤ 20 的经验准则成立）；用户确认：字段级 inverted 默认 true
> + 全局白名单；长文本不建倒排。

1. ~~**配置与实现**~~ ✅（2026-08-29）：`[inverted] inverted_fields`（白名单，非空 = 只建声明字段倒排，
   MySQL 建表式字段声明的运行时等价）/ `exclude_fields`（黑名单）/ `max_term_len`（term 长度上限，
   默认 96B，**超长 term 自动跳过 = 长文本整串不进字典**，0 = 不限）；
   `Engine::inverted_allowed` 写路径统一过滤（put/put_nosync 前检查：白名单 → 黑名单 → 超长）——
   覆盖所有写入路径（HTTP / demo / import），与 import-schema 的 term_filter 正交叠加；
   与 M7-2 `bitmap_fields`（位图白名单）独立；
2. ~~**测试**~~ ✅：engine 4 个新增（白名单只建声明字段 / 黑名单剔除 / 超长 term 自动跳过 / 默认全建兼容）；
   测试 290→294（+4）；
3. ~~**5000 万导入优化**~~ ✅（进行中）：ds-50m 数据集的 2×256 字符 big_text 自动跳过（max_term_len=96），
   倒排字典从 ~1 亿单 posting term 降为 8 个枚举字段；重导到 `D:\shanshui-data\db-50m-clean`
   （旧半成品 db-50m 19GB 保留未删——含 big_text 膨胀索引，弃用）。

### 7.12 M8-P5 WAL 截断（append 模式 flush 后回收，防无限增长）

> 背景：5000 万导入发现 `wal.log` 无限增长（6.5GB）——每 100 万行 fsync 大文件极慢、导入卡顿；
> 磁盘写入翻倍（WAL + SST 双写）。append 模式 WAL 在 SST flush 后不回收。

1. ~~**实现**~~ ✅（2026-08-29）：**WAL 文件头**（magic + next_seq，16B）——`truncate_and_reset` 在
   `switch_and_flush` 成功后执行（flush 后全部记录已刷盘，清空 WAL 写头持久化 next_seq，
   重开 seq 接续不冲突）；`open_append` 读头恢复 next_seq（旧无头 WAL 兼容回放）；
   `WalReader::recover` 识别头跳 16B；`WalWriter` 打开模式 append → read+write
   （`set_len(0)` 需要写权限，Windows append 句柄不允许）+ sync 前 seek 末尾；
   环形 WAL 自带覆盖回收（no-op 不截断）；`resume_seq` 条件化（max_seq>0 才覆盖头值）；
2. ~~**语义影响**~~ ✅：**增量备份**只导出 WAL 未刷盘记录（已刷盘由全量备份覆盖，与环形 WAL 一致）；
   缺口检测仍有效（WAL 截断后旧 seq 缺失 → 提示全量）；
3. ~~**测试**~~ ✅：column_family 3 个新增（flush 后 WAL 变小 + 重开数据完整 seq 接续 /
   头持久化 next_seq 跨重启 / 旧无头 WAL 兼容回放）；engine 1 个更新（增量备份 since=0 导出
   未刷盘记录）；测试 294→297（+3）；5000 万重导验证：WAL 保持小文件、卡顿消除、速度稳定
   100 万/分钟（SST 构建 + 倒排排序主导）。

### 7.13 M8-P6 批量导入模式（HotCache 跳过回填，防内存崩溃）

> 背景：P39 WAL 截断修复后 5000 万导入**仍**确定性卡死——0-4M 行 60K 行/s 正常，之后行速
> 指数级崩塌（4M-5M 2K/s → 8M-9M 0.9K/s → 12M 后假死；CPU 满核、磁盘 ~0.9M/s、inverted/primary
> 停止更新）。排除算法 O(N²)（单行路径全部 O(1)/O(log n)）后锁定为**内存问题**。

1. ~~**根因定位**~~ ✅：**HotCache 默认 4GB 预算被只写不读的导入文档灌满**——`put_nosync` 每行
   `hotcache.put`，4M 行 × ~600B ≈ 2.4GB 全进缓存（导入从不读取）；叠加 LruCache 内部淘汰
   （容量 4M 条目）不同步 `stats` HashMap（泄漏）与 `used_bytes`（虚增）、primary memtable
   256MB、WAL 缓冲 → 进程 WS 4.9GB。本机 16GB 且桌面负载占 ~11GB → 超物理内存 →
   **Windows 页面文件颠簸**（峰值 24.8GB）→ 每个内存访问缺页换入换出 → 行速指数恶化假死。
   加细粒度进度（每 10 万行）+ 刷盘分阶段计时定位；磁盘 0.9M/s = 页面文件换页，非业务写入。
2. ~~**实现**~~ ✅（2026-08-29）：`Engine::set_bulk_import(on)`——批量导入模式跳过
   `put_nosync` 的 HotCache 失效/回填（导入只写不读，回填零收益）；`import_parquet` /
   CSV / JSON 三个导入器入口统一开启。
3. ~~**验证**~~ ✅：5M 复现 **80s 完成、全程稳定 63K 行/s**（越过 4.3M 卡点）；50M 正式导入
   WS 从 4.9GB 降到 **~620-780MB**、61.7K 行/s 稳定通过旧卡点 12M；单元测试
   `bulk_import_skips_hotcache`（默认回填 / 批量跳过 / 关闭恢复 / 主数据不受影响）；
   测试 297→298（+1）。
4. **遗留**：HotCache `stats` 泄漏 / `used_bytes` 虚增是独立内存缺陷（常规读写负载缓慢泄漏），
   后续单独修复。

### 7.14 M8-P7 fulltext 分词索引（长文本可检索，与 inverted:false 正交）

> 背景（P38 后续方向）：长文本字段（如 big_text 256 字符）整串被 `max_term_len=96` 跳过 →
> 长文本无法检索。方案：声明字段**分词建词 term**（`ft:{field}:{token}`），分词 token 短可建索引。

1. ~~**demo 研究（src/demo/fulltext/，gitignore 不提交）**~~ ✅：分词器 `tokenize`（按非字母数字
   边界切分 + 小写归一；中文连续字符暂为单 token，完整中文分词留后续）+ `ft:{field}:{token}`
   编码（独立命名空间不冲突）+ 同文档 token 去重；5 测试全绿（分词边界 / 去重编码 / 端到端
   写入查询回表 / 长文本 token 可检索 / 100K 行 perf sanity）。
2. ~~**kernel 整合**~~ ✅（2026-08-29）：
   - `[inverted] fulltext_fields`（声明分词字段，空 = 关闭）；
   - `server::tokenize` / `fulltext_terms` / `extract_terms_with_fulltext`（fulltext 字段分词
     建词 term **取代整串**；其余字段整串 term 受白名单过滤——**与 inverted_fields 正交**）；
   - `Engine::fulltext_search(field, word)`（词 term 查询 → posting 合并 → 回表）+
     `fulltext_fields()` 访问器；HTTP `GET /fulltext?field=X&word=Y`；
   - 写路径统一接入：HTTP put / import_parquet / CSV / JSON / mysqldump 导入。
3. ~~**验证**~~ ✅：5M 真实数据集配 fulltext 导入（133s / 39K 行/s，分词增加 posting 量属预期），
   HTTP 检索 `word=00000042` 精确命中 docid=42（big_text_a="rec-00000042-msg-1344-tag42"）；
   测试 302 全绿（+4：分词边界 / 字段优先级 / 白名单正交 / 长文本端到端持久化）。
4. **已知限制**：连续中文串 = 单 token（需 jieba/ngram 完整中文分词，留后续）；大结果集
   （数百万行）查询无 limit/分页，server 端全量构造 JSON 会内存爆炸（见 7.15 观察）。

### 7.15 P41 HotCache 内存缺陷修复（stats 泄漏 / used_bytes 虚增 / LFU O(N) 风暴）

> 背景：7.14 验证时 `word=rec`（命中 5M 行）把 server 卡死——暴露 HotCache 既有缺陷。

1. ~~**根因定位**~~ ✅：HotCache 容量按**条目数**（max_memory_mb/1KB）设 LruCache 容量，
   满后 **LruCache 内部淘汰不通知 stats/used_bytes** → stats 无限泄漏、used_bytes 虚增；
   超预算后 `evict_one` 从 stats 选 victim 但该 key 已被内部淘汰（pop=None）→ **淘汰永远失败
   + 超预算死循环**；LFU `pick_lfu_victim` 全量扫描 stats（O(N)）→ 大批量回表（每 get 一次
   hotcache.put）把写/查询路径卡成 O(N²)（P40 50M 导入 4M 行卡死的叠加因素）。
2. ~~**修复**~~ ✅（2026-08-29）：容量 **unbounded**，淘汰**完全由字节预算**统一管理（stats
   与缓存同步、used_bytes 准确）；**软水位渐进淘汰**（每 put 至多 1 个，防单次 put O(N) evict
   风暴）；**LFU 采样近似**（主缓存前 64 条目选最小计数，O(64) 常量）。
3. ~~**验证**~~ ✅：hotcache 14 测试全绿（+2 回归：批量 put 无泄漏/无虚增/真淘汰；
   10K 次 512KB put 渐进淘汰 <5s）；5M 库小查询 959ms 正常（修复前 server 假死）。
4. **观察（非本项）**：大结果集查询（数百万行）server 端全量 JSON 构造仍会内存爆炸
   （`word=tag42` 命中全部 5M 行 → 10GB+）——API 无 limit/分页，后续加 `limit`/游标分页。

### 7.16 M8-P8 大结果集查询分页（limit/offset/total，防全量回表内存爆炸）

> 背景（7.15 观察）：倒排命中数百万行时（`word=rec` 命中 5M）server 端全量回表 +
> 全量 JSON 构造内存爆炸（实测 10GB+ 卡死）。方案：倒排路径分页——RoaringBitmap 迭代
> docid 天然升序，skip(offset) 后只回表 limit 行，内存 O(limit) 不随 total 膨胀。

1. ~~**demo 研究（src/demo/pagination/，gitignore 不提交）**~~ ✅：验证分页语义与边界——
   分页拼接 == 全量（有序稳定）、limit=0 / offset>total / 末尾不足页 / 只回表当前页；
   4 测试全绿。
2. ~~**kernel 整合**~~ ✅（2026-08-29）：
   - `Engine::search_term_paged / fulltext_search_paged / scan_range_paged` → `PagedRows{total, rows}`
     （total = bitmap.len() O(1)；scan 分页仅截断 JSON 构造，扫描本身仍全量，后续流式化）；
   - `server::execute_filter_paged`（单条件 / 多条件 AND / docid 点查统一分页）；
   - HTTP `/search` `/fulltext` `/range` 支持 `limit`/`offset`（limit 缺省/≤0 = 不限制，
     兼容非分页调用）；响应 `total` = **全量命中数**（≠ 当前页行数，供客户端算总页数）。
3. ~~**验证**~~ ✅：5M 库 `word=rec&limit=10`（命中 5M）1.1s 返回 total=5,000,000，
   翻到 offset=4999990 页 316ms 首条 docid=4999990；`search status=active&limit=5`
   total=1,666,667（精确）；**server WS 221MB**（修复前 10GB+ 卡死）；
    测试 307 全绿（+3：engine 分页语义与边界 / execute_filter_paged / parse_paging）。

### 7.17 M8-P9 中文 bigram 分词（fulltext 中文可检索）

> 背景（7.14 已知限制）：`tokenize` 把连续中文字符当单 token（`is_alphanumeric` 全 true）→
> 中文文本整串进倒排，长中文文本无法检索。方案对比（demo 研究）：
> unigram（逐字，膨胀=字数）/ **bigram**（相邻 2 字，与 ES ngram / Lucene CJKAnalyzer 同款，
> 中文检索事实标准，零依赖无词典）/ jieba（精确高但重依赖 ~2MB 词典，vendor 无、离线构建不可行）——选 **bigram**。

1. ~~**demo 研究（src/demo/cjk-tokenizer/，gitignore 不提交）**~~ ✅：混合分词规则验证——
   ASCII 字母数字 → 单词（保留原有）；连续中文/非 ASCII → bigram（单字回退 unigram）；
   索引膨胀 ≈ 字数（≤ unigram）；2-4 字关键词 bigram AND 交集精确命中；4 测试全绿。
2. ~~**kernel 整合**~~ ✅（2026-08-29）：`server::tokenize` 升级为字符类分段分词
   （`cjk_bigram`：长度 1 → unigram，≥2 → 相邻 2 字）；仅影响 fulltext 路径
   （extract_terms 整串 term 不经 tokenize，整串倒排行为不变）；无需新配置
   （fulltext_fields 声明即启用）。
3. ~~**验证**~~ ✅：HTTP 实测中文检索——PUT「山水存迹数据库存储引擎」等 3 条，
   `fulltext 数据` → total=2（含"数据库"文档 5000001/5000002）、`fulltext 山水` → total=1；
   测试 308 全绿（+1：engine 中文 bigram 端到端；tokenize 测试更新为 bigram 断言）。

### 7.18 M8-P10 scan 范围扫描流式化（k-way merge，内存 O(page) 防全量收集 OOM）

> 背景（7.16 已知限制）：`scan_range_paged` 先 `scan_range` 全量收集（HashMap 合并 + 排序，
> 内存 O(total)）再截断——全库 scan（5000 万行）会收集 ~35GB 内存 OOM。方案：k-way merge
> 流式归并（memtable 双缓冲 + 各 SST 有序源，同 key 取最大 seq、Tombstone 跳过），
> 配合分页 skip/take 提前终止，内存 O(page)。

1. ~~**demo 研究（src/demo/scan-stream/，gitignore 不提交）**~~ ✅：归并算法验证——
   单源直通 / 多源交错升序 / 同 key 最新 seq / tombstone 隐藏旧版 / 空边界 / 流式分页==全量；
   6 测试全绿。
2. ~~**kernel 整合**~~ ✅（2026-08-29）：
   - `SstReader::SstRangeIter`（块级惰性迭代 + Zone Map 剪枝，与 scan_range 语义一致）；
   - `MemTable::MemRangeIter` / `MemTableBuffer::iter_range`（skiplist range 惰性迭代，
     owned 查询键免借用）；
   - `ColumnFamily::scan_stream`（k-way merge：BinaryHeap 最小堆 O(N log K)，避免每轮
     线性扫源 O(N·K)——202 SST × 50M 行实测超时 → 堆优化）；
   - `Engine::scan_range_paged` 改流式（skip/take + total 全扫计数，内存 O(page)）。
3. ~~**验证**~~ ✅：50M 库全库 `/range?limit=10` → total=5,000,000 精确、**server WS 691MB**
   （旧实现全量收集 50M 行会 OOM）；小范围翻页 `/range?start..end&limit=20&offset=40`
   117ms（total=101 首条 docid=10000040）；全库 scan ~70s 为 total 计数 + 读全 SST 的固有代价；
   测试 310 全绿（+2：scan_stream vs scan_raw_range 一致性含覆盖/删除/范围过滤、SstRangeIter
   vs scan_range 一致性）。
4. **已知限制**：全库 scan 的 `total` 计数需扫完全部（无 limit 提前终止的 total 语义），
   后续可加「仅需前 N 条」的无 total 模式 / 游标续扫。

### 7.19 M8-P11 scan 游标续扫（after + 提前终止，全库遍历每页 O(limit)）

> 背景（7.18 已知限制）：`/range` 的 `total` 计数需全扫（50M 库 ~70s）；offset 翻页每页
> 累积跳过。方案：**游标续扫**——每页只扫「after 之后」部分（start=after+1 定位 + Zone Map
> 剪枝），取满 `limit` 即**提前终止**（无 total 全扫），全库遍历每页 O(limit) + 游标定位。

1. ~~**demo 研究（src/demo/cursor-pagination/，gitignore 不提交）**~~ ✅：游标语义验证——
   遍历一致性（游标翻页拼接 == 全量升序）/ after 首尾边界 / 删除覆盖下 docid 稳定 /
   上界限定 / 深页与 offset 等价；4 测试全绿。
2. ~~**kernel 整合**~~ ✅（2026-08-29）：
   - `ColumnFamily::scan_stream` 回调改为返回 `bool`（false=**提前终止**，取满页即停）；
   - `Engine::scan_after(after, end, limit)`：from after+1 定位，取满 limit 提前终止，
     无 total 全扫；
   - HTTP `GET /range?after=LAST&limit=N` 游标模式（返回 `{"rows":[...]}`，用末条 docid
     作下页 after；`after` 为**严格之后**语义，首页如需含 docid 0 请用 start/total 模式）。
3. ~~**验证**~~ ✅：50M 库游标翻页——首页 `after=0&limit=10` **682ms**（旧 total 模式全库 70s）、
   深页 `after=49999990&limit=10` **164ms**（docid 49999991-49999999 共 9 条）、server WS
   696MB；测试 311 全绿（+1：scan_after 遍历一致性/边界/提前终止/上界）。

### 7.20 M8-P12 环形 WAL 头部 tail 合并 fsync（sync 单次原子提交）

> 背景（M8-P1 候选优化）：`RingWal::sync` 两阶段——写记录区 `sync_all` + 写头部 tail
> `sync_all`（每次提交 2 次 fsync），ring+gc 2ms 仅 30,270 ops/s vs append+gc 91,296。
> 优化：头部 tail 与记录区**合并为同一次 `sync_all`**（同一文件一次原子提交）。

1. ~~**demo 研究（src/demo/ring-tail-fsync/，gitignore 不提交）**~~ ✅：提交/崩溃语义验证——
   已 sync 记录完整恢复 / 未 sync 记录崩溃丢弃 / 多次提交+回绕恢复最新周期 / WalBackend
   分发语义；4 测试全绿。
2. ~~**kernel 整合**~~ ✅（2026-08-29）：`RingWal::sync`——先写头部 tail（page cache）
   再写记录区，**单次 `sync_all`** 原子提交两者。崩溃安全不变：fsync 前崩溃 → 头尾均未
   落盘，恢复上次提交状态（未提交记录忽略）；fsync 后 → 头尾同时可见，**tail 永不指向
   未落盘记录**（与两阶段版保证相同）。
3. ~~**验证**~~ ✅：ycsb A 写重 ring+gc 2ms **68,756 ops/s**（M8-P1 基线 30,270 → **2.3×**；
   ring 4MB+gc 55,570 → +24%）；wal 模块 20 测试全绿（含环形崩溃恢复/回绕/混沌）；
   测试 311 全绿（无新增单测——合并 fsync 由既有崩溃恢复测试覆盖，demo 4 测试验证语义）。

### 7.21 M8-P13 jieba 完整中文词典分词（`[inverted] cjk_segmenter`）

> 背景（7.17 已知限制）：bigram（M8-P9）对中文是**字符碎片**索引——"数据库" → ["数据","据库"]，
> 查询需两个 bigram AND 交集；jieba 词典分词输出**语义词**（"数据库" → ["数据库"]），单词精确
> 命中、索引词数更少。jieba-rs 0.10（MIT，`default-dict` 内置词典 ~2-3.5MB 压缩嵌入，无外部文件）。

1. ~~**依赖与 feature**~~ ✅（2026-08-29）：`jieba-rs = "0.10"` optional + feature `cjk-jieba`
   （默认开启；`--no-default-features` 可去除，中文分词回退 bigram）；deny.toml 许可白名单
   （MIT/Apache-2.0）覆盖 jieba 全部依赖。
2. ~~**demo 研究（src/demo/cjk-jieba/，gitignore 不提交）**~~ ✅：语义词切分 / HMM 未登录词 /
   索引膨胀对比（jieba 词数 ≤ bigram 碎片）/ 中英混合 / 端到端检索 / 词典加载+切词性能；6 测试全绿。
3. ~~**kernel 整合**~~ ✅：`[inverted] cjk_segmenter = "bigram"(默认) | "jieba"`；
   `server::tokenize_seg / fulltext_terms_seg / extract_terms_with_fulltext_seg`（参数传分词器，
   无全局状态——并发测试安全）；`Engine::use_jieba()`；写路径（HTTP put / parquet / CSV / JSON /
   mysqldump）统一接入；feature 关闭时自动回退 bigram。
4. ~~**验证**~~ ✅：HTTP 实测——PUT 3 条中文（terms=6/8/5 语义词），`fulltext 数据库` 单 term
   精确命中 2 条；测试 313 全绿（+2：tokenize_seg 语义词/混合/标点、engine jieba 端到端单 term
   命中）；feature 关闭编译回退验证通过。

### 7.22 倒排 posting 压缩探索（结论：维持 Roaring，不引入新编码）

> 背景：倒排段内每 term posting 用 RoaringBitmap 序列化（SEG_VERSION=2）。评估
> delta-varint / Gorilla 变长编码能否替代——尤其是稀疏 posting（array container 2B/docid
> 理论 1B）。demo 研究（src/demo/posting-compression/，gitignore 不提交）。

1. ~~**demo 编码实现**~~ ✅：delta-varint（delta + LEB128）、简化 Gorilla（XOR + 前导零控制位）、
   Roaring 三编码；真实分布生成：密集连续（16.6M）/ 簇状（10 簇×1M）/ 稀疏（500K/50M、5K/50M）；
   4 测试全绿（压缩率对比 / AND 查询性能 / 稀疏紧凑性 / 密集下限）。
2. ~~**实测数据**~~ ✅（release）：

   | 场景 | Roaring | delta-varint | Gorilla | 结论 |
   |---|---|---|---|---|
   | 密集 16.6M 连续 | **0.13B/docid**（1bit 理论下限） | 1.00B | 2.00B | Roaring 最优 |
   | 簇状 10 簇×1M | **0.13B/docid** | 1.00B | 2.00B | Roaring 最优 |
   | 稀疏 1%（500K/50M） | 2.01B | **1.00B** | 2.39B | delta 省 ~0.5MB，绝对差小 |
   | 稀疏 0.01%（5K/50M） | 3.22B | **2.00B** | 3.15B | 量级极小，无所谓 |
   | AND 8M∩4M 查询 | **337us** | 需解码后双指针 | — | Roaring 快 20.1×（vs 线性 6.7ms） |

3. ~~**决策**~~ ✅：**维持 Roaring，不引入新编码**。依据：① 密集/簇状（高频词主流场景）
   Roaring 已达 1bit/docid 理论下限，任何变长编码（≥1B）不可能更优；② 稀疏场景 delta 省空间
   但绝对量级小（500K docid 差 ~0.5MB），且需牺牲 Roaring 的向量化 AND/交集 20× 查询性能与
   FST 集成；③ Roaring 库成熟（迭代/交集/并集/差集完备）。后续若出现超高频 posting 段内存
   瓶颈，可再评估 Roaring 64 位 + 密度感知（rle 容器）。

### 7.23 SSD 原生存储优化定位（v0.7 起：放弃 HDD 兼容）

> 背景（2026-08-29 决策）：shanshui-cunji 定位为 **NVMe/SATA SSD Only**，不再兼容机械硬盘。
> 目标场景「写入快 + 20 倒排字段」（1.5 亿文档、20 倒排字段、写入 TPS 最高优先级）——
> 写入路径瓶颈实测/估算：WAL fsync 0.25ms + 倒排字段更新 0.40ms（占 60%+ CPU）。

1. ~~**决策与设计**~~ ✅：design.md 新增 **4.8 SSD 原生存储优化**（核心设计转变表：随机 IO 接受/
   WAL 大文件环形/4KB 块/空间放大换写放大/磨损均衡；写入路径耗时分解；P0~P2 八方向优先级；
   storage 配置模板；三阶段路线图）+ 1.2 设计哲学新增「放弃 HDD 兼容」行 + 1.3 介质确认改
   「纯 SSD 持久化」+ 4.3/4.4/4.5/4.6 对应更新（环形 WAL 现状、块大小 4KB、compaction 20× 并行、
   删除位图）。
2. ~~**扩展文档**~~ ✅：development_extension.md 新增 **Ex-5 SSD 原生迁移计划**（Ex-5.1~5.10：
   P0=4KB 块/倒排分片锁/倒排批处理/compaction 调优；P1=环形大文件 WAL/删除位图/FST+mmap 字典/
   元数据解耦；P2=冷热 compaction/多 SSD 条带化）+ 状态表 + 文档关系；feature.md 新增 **J 模块
   （SSD 原生优化）** 任务清单；design_extension.md 新增 **v0.3 SSD 原生优化** 章节。
3. ~~**开发环境警告**~~ ✅：所有文档明确——**开发环境若使用机械硬盘，写入/查询性能大幅下降
   （10 倍级），压测数据无参考价值，不要压测**；性能验证必须在 SSD 上进行。
4. ~~**发布对齐**~~ ✅：v0.6.0 发布（版本 0.5.0→0.6.0 + RELEASE-v0.6.0.md + README + quality_system
   313 测试对齐 + 四分支同步 + 发布前 bug 清理：WAL `truncate(false)` 表意/删死代码 rows_payload/
   修未用变量）；测试 313 全绿。

### 7.24 Ex-5.1 SSTable 4KB 块（SSD 原生，design 4.8）

> 背景：SSD-only 定位下，块大小从 16KB（对齐 HDD 扇区/寻道）改为 4KB（对齐 SSD 页）——
> 点查读放大 16×→4×；两级索引（design 4.4.2 已有）需防"块变小 → 索引条目 ×4 → 内存膨胀"。

1. ~~**demo 研究（src/demo/block4k/，gitignore 不提交）**~~ ✅：6 测试全绿——50 万行 JSON 实测：
   块数 2726→10823（3.97×）；点查读放大平均命中块 1257B→413B（-67%）；压缩后体积 3.43MB→
   4.48MB（小块 zstd 窗口小 +30%，SSD 空间便宜可接受）；单条 100KB 长 value 超块不 panic；
   5000 随机 + 首/末边界 + 不存在点查全对；**窗口化两级定位**（L1 摘要粗定位 + 窗口内精确二分）
   2000 随机 key 与全量二分结果一致。
2. ~~**kernel 整合**~~ ✅：`BlockCacheConfig.block_size_kb` 默认 16→4；`SstableConfig.index_granularity`
   默认 16→64（与 4KB 块联动，L1 摘要 170 vs 171，内存不膨胀）；读路径从 SST header 取块大小
   （不依赖配置）——旧 16KB 块数据照常可读，无格式变更；测试 313 全绿。
3. ~~**验证**~~ ✅：`cargo test` 313 全绿；demo 6 测试全绿（见 1）；提交 `056b21d`。
   ⚠️ 性能收益需在 **SSD 环境**实测（HDD 开发环境不压测，design 1.2 警告）。

### 7.25 Ex-5.2 倒排分片锁（design 4.8.3 P0-4）

> 背景：低基数字段（status='active'）高并发写入时同一 Term 锁竞争——倒排 Term 字典按 Hash
> 分 256 锁分区（同 Term 串行、不同 Term 并行）；位图索引（M7-2 白名单）全局单 Mutex 同改。

1. ~~**demo 研究（src/demo/sharded-lock/，gitignore 不提交）**~~ ✅：3 测试全绿——DashMap shard
   数对比：8 线程×200K ops 低基数 32 term，4 shards 14.1ms vs 256 shards 10.1ms（**1.39× 加速**）；
   InvertedIndex 并发 add（8 线程×51.2K）mem_docids 精确、32 term search 全一致；白名单并发
   add 后 bitmap_count 精确（active=40000、beijing=26667）。
2. ~~**kernel 整合**~~ ✅：`mem` DashMap 默认 4 → **256 shards**
   （`with_capacity_and_shard_amount`，2 的幂）；`bitmaps` 全局 `Mutex<HashMap>` → 按 field hash
   分 **256 片锁**（FNV-1a 确定性分片，同 field 恒同片；group_by 需同 field 全量一致）；
   `bitmap_and` 逐 term 取片锁（每次仅持一把，无死锁）；`with_bitmap_fields` 重建改逐片清空+
   按片填充；测试 313 全绿。
3. ~~**验证**~~ ✅：`cargo test` 313 全绿；demo 3 测试全绿（见 1）；提交 `c7ebe72`。

### 7.26 Ex-5.3 倒排更新批处理（design 4.8.3 P0-3）

> 背景：写入快 + 20 倒排字段场景，倒排更新占写入 CPU 60%+（每 docid 每字段一次 DashMap
> entry hash + 锁 + Vec push）。批处理 = 同 Term 多 DocId 内存聚合，批量追加——减少锁操作次数。

1. ~~**demo 研究（src/demo/inverted-batch/，gitignore 不提交）**~~ ✅：4 测试全绿——20 万行×
   20 字段（400 万 posting）逐个 add 219ms vs `add_batch` 130ms（**1.7×**）；批量/逐个 search
   结果全一致（8 字段×4 值）；put 未达阈值查询自动刷入（doc_count/posting/execute 全可见）；
   20K 条超阈值自动刷入 + flush_inverted 落盘 + 重启恢复正确。
2. ~~**kernel 整合**~~ ✅：`InvertedIndex::add_batch`——按 term 分组合并后每 term 一次 DashMap
   entry + 批量 extend（`mem_docids` 一次累加；白名单位图按 (field,value) 分组批量 extend）；
   `Engine::pending_inverted` 攒批缓冲——put 过滤后 term 先入缓冲，达阈值（8192）自动批量刷入；
   倒排查询入口（execute/search_term_paged/inverted_posting/inverted_doc_count/inverted_group_by/
   inverted_bitmap_and_count）先 flush pending（一致性）；`flush_inverted` 先刷缓冲再落盘；
   `inverted_mem_docids` 统计含缓冲；**崩溃安全**：WAL 回放重新走 put 重建倒排，缓冲丢失不丢数据；
   4 个 &self 查询方法改 &mut（rpc.rs 一处补 mut）；execute 倒排路由补 flush（测试暴露）。
3. ~~**验证**~~ ✅：`cargo test` **314 全绿**（+1：inverted_batch_pending_flush——统计含 pending/
   查询自动刷入/落盘归零）；demo 4 测试全绿（见 1）；提交 `d38e8ab`。

### 7.27 并发读优化设计（Seqlock/Arc，design_extension v0.4）

> 背景（2026-08-29 决策）：写 22 万 TPS + 读 85 万 QPS 高并发下读路径需持续响应。
> 调研五方案（读写锁/双缓冲/RCU/无锁/Seqlock）后决策：RWLock 不用（读写交替互阻塞）、
> RCU 不引入（复杂度收益有限）、无锁不用（DashMap 已够好）、双缓冲已用（MemTable/SSTable
> 元数据/配置热加载）、**Seqlock/Arc 引入倒排**（段清单 + FST 字典指针，小数据零开销写读）。

1. ~~**设计（design_extension v0.4 第 11 章）**~~ ✅：方案全景对比表 + 取舍结论；各模块最优
   组合（现状核对：MemTableBuffer 双缓冲已实现、两级索引已实现、HotCache DashMap 已实现、
   Config::reload 已实现——均无需改）；**Seqlock 倒排设计**（段清单 `Vec<String>` 版本化快照 +
   FST 字典 `Arc<fst::Map>` 原子发布；写=版本号奇偶递增、读=快照校验重试，重试率预期 <0.1%）；
   边界（大数据不适用 Seqlock，SSTable 大块维持双缓冲 + Arc）；落地路径（Seqlock 原语 → 倒排
   接入 → 并发验证）。
2. ~~**扩展文档**~~ ✅：development_extension.md 新增 **Ex-6 并发读优化**（Ex-6.1 Seqlock 原语 /
   Ex-6.2 段清单 / Ex-6.3 FST 字典 Arc / Ex-6.4 验证）+ 状态表 + 文档关系；feature.md I 模块
   新增「倒排并发读（Seqlock/Arc）⏳ Ex-6」+ 读写分离标注为前置。
3. ~~**依赖说明**~~ ✅：Seqlock 落地依赖**打破 Engine 全局锁**（读写分离/双写加速，feature.md
   I 模块 ⏸）——全局锁下写与读仍串行，Seqlock 无收益；原语（Ex-6.1）可独立先行（不与全局锁
   冲突，~100 行 + 单元测试）。代码落地待读写分离评估后再启。

### 7.28 Ex-6.1 Seqlock 原语（design_extension v0.4 第 11.3）

> 背景：倒排段清单/FST 字典指针需要"读不阻塞写、写不阻塞读"的无锁读。项目
> `#![forbid(unsafe_code)]` 红线排除标准 UnsafeCell 实现 → 采用**零 unsafe 方案**：
> AtomicU64 版本号（奇偶语义）+ RwLock 数据（写持锁窗口极短，读 try_read 立即失败重试）。

1. ~~**demo 研究（src/demo/seqlock/，gitignore 不提交）**~~ ✅：4 测试全绿——2 写 × 4 读 ×
   10 万交错 **0 撕裂**；10K 写 218us（0.02us/次，写不阻塞）；低频写（20µs 间隔）vs 高频读
   （1100 万次）：重试 1647 次，**率 0.015%**（<0.1% 目标）；写后立即可见。已知边界：写频率
   极高时读饥饿（重试率飙升）——倒排 flush/gc 为低频写，不触达。
2. ~~**kernel 整合**~~ ✅：新增 `src/seqlock.rs`——`Seqlock<T>`（`read`/`write`/`version`/
   `retries`，read 用 `Fn` 支持重试循环，write 用 `FnOnce`）；零 unsafe；lib.rs 导出；
   单元测试 4（并发不撕裂/可见性/低频重试率/版本奇偶）。
3. ~~**验证**~~ ✅：`cargo test` **318 全绿**（+4 seqlock）；demo 4 测试全绿（见 1）；
   提交 `1946161`。倒排段清单/FST 字典接入（Ex-6.2/6.3）待读写分离解除 Engine 全局锁。

### 7.29 多核优化设计（Shard Everything，design_extension v0.5）

> 背景（2026-08-29 决策）：多核 CPU 下可处理点集中在锁竞争、缓存局部性、IO 并行三层。
> 核心不是"让所有核干活"，而是**数据分片（Shard Everything）**——按核分计数器、
> 按 Term 分锁、按物理核分调度池。现状核对：design.md 已含三池防超售/绑核（默认关闭）/
> io_uring/compaction 限流；Ex-5.2 + Ex-6.1 已落地锁竞争。

1. ~~**设计（design_extension v0.5 第 12 章）**~~ ✅：五处理点全景（锁竞争 ✅ / 缓存伪共享 ❌
   新增 / 绑核 ✅ 已有 / io_uring 多队列 ⏳ / compaction 动态限流 ⏳）；**缓存伪共享设计**
   （PerCpuCounter 按核拆分计数器 + `#[repr(align(64))]` 缓存行隔离——`total_writes`/
   `mem_docids` 改 PerCpuCounter 消除多核原子 RMW 竞争）；绑核建议（网络 0-3 / 计算 4-7 /
   IO 尾核，跳过超线程，稳定 P99）；WAL/SSTable 多 NVMe 队列；compaction 动态限流。
2. ~~**扩展文档**~~ ✅：development_extension.md 新增 **Ex-7 多核优化**（Ex-7.1 PerCpuCounter
   P0 / Ex-7.2 绑核默认开启 P1 / Ex-7.3 io_uring 多队列 P1 / Ex-7.4 compaction 动态限流 P2）
   + 状态表；feature.md 新增 **K 模块（多核优化）** 任务清单；design.md 已有设计引用。
3. ~~**依赖说明**~~ ✅：Ex-7.1（伪共享）独立可做（不与全局锁冲突）；Ex-7.2 依赖三池模型
   （已有）；Ex-7.3 依赖 io_uring 阶段 3；Ex-7.4 依赖 Ex-5.4 compaction 调优。
   性能验证须在 **SSD 环境**（HDD 不压测，design 1.2 警告）。

### 7.30 musl 目标验证锁定（mimalloc 编译时绑定确认）

> 背景（2026-08-29 确认锁定，非重测）：验证「编译时绑定 mimalloc」在 musl 目标下功能正常——
> 服务器部署目标（x86_64-unknown-linux-musl 静态链接）的构建与测试全链路确认。
> 本机（Windows）无法交叉编译 musl（无 musl 工具链/Docker/WSL 发行版）→ 在阿里云 Debian
> 服务器（106.14.68.116，musl-gcc 已装）执行。

1. ~~**环境准备**~~ ✅：服务器装 rustup（rsproxy 镜像）+ musl target；源码 git archive 打包
   （6.4MB，排除 src/demo/vendor/Windows .cargo 配置）scp 上传；依赖改用**本机 cargo vendor**
   （本机科学上网下载 202 crate → 29.5MB tar.gz → 上传解压 → 服务器 `--offline` 离线构建，
   规避 rsproxy 网络超时反复失败）。
2. ~~**验证执行**~~ ✅：`cargo test --target x86_64-unknown-linux-musl --offline`——
   **libmimalloc-sys v0.1.49 + mimalloc v0.1.52 交叉编译成功**（musl-gcc 编 C 代码）、
   zstd-sys 成功、**318 测试全绿**（与 Windows msvc 完全一致，49.44s）；default features
   = alloc-mimalloc + cjk-jieba 生效确认。
3. ~~**结论**~~ ✅：**mimalloc 编译时绑定在 musl 下锁定确认**——服务器部署构建与测试
   全链路正常（编译 0 错误、测试 0 失败）。读写分离 demo 评估（src/demo/rw-separation，
   gitignore 不提交）：全局锁 vs 读写分离（主写+从读同步复制）——读 P95 3µs→2µs（1.5×）、
   吞吐持平（写瓶颈在 fsync 非锁）、同步复制读己之写强一致、异步最终一致窗口验证。
   读写分离收益定位：**读延迟改善，写吞吐不增**（写瓶颈磁盘 fsync）——整合决策待定
    （feature.md I 模块 ⏸，Ex-6.2/6.3 前置）。

### 7.31 Ex-5.4 Compaction 参数调优（design 4.8.3 P0-4）

> 背景：SSD-only 下 Compaction 从"避免空间放大"转向"允许空间放大换取写放大降低"——
> L0→L1 触发放宽、层级跨度大、并行压实（SSD 并发 IO）。**P0 四项至此全部完成。**

1. ~~**demo 研究（src/demo/compaction-tune/，gitignore 不提交）**~~ ✅：2 测试全绿——
   l0_stall_threshold 权衡（8 批×2000 条：l0=4 压实 1 次 vs l0=8/12 压实 0 次，大阈值降压实
   频率=低写放大；数据正确性抽查全过）；多 CF 并行压实 **2.14×**（3 CF 串行 30ms → 并行 14ms，
   SSD 并发 IO）。
2. ~~**kernel 整合**~~ ✅：`l0_stall_threshold` 默认 8→12（空间放大 1.2×→1.8× 换写放大
   15~25×→6~10×）；新增 `[storage] compaction_parallel`（0=自动 min(4,核数/2) / 1=串行 /
   >1=指定）——`Engine::compact` 并行压实 primary/cidx/delta 三列族（&mut 字段拆分借用 +
   thread::scope；CompactReport 聚合，out_level 取 primary）；串行路径含三列族压实（兼容旧版）。
3. ~~**验证**~~ ✅：`cargo test` **318 全绿**（l0 断言 8→12 更新）；demo 2 测试全绿（见 1）；
   提交 `624ce9e`。⚠️ 性能收益（写放大/压实时长）需在 **SSD 环境**实测（HDD 不压测）。

### 7.32 Ex-5.5 环形大文件 WAL 规模化（design 4.8.3 P1）

> 背景：SSD-only 下 WAL 向"大文件环形 + 环形指针"演进（省文件切换、磨损均衡）。
> RingWal 核心（预分配单文件 + 环形指针 + tail 合并 fsync + 覆盖安全）v0.6 已就绪，
> Ex-5.5 = 规模化验证 + 默认容量提升 + 崩溃恢复混沌回归。

1. ~~**demo 研究（src/demo/ring-wal-scale/，gitignore 不提交）**~~ ✅：4 测试全绿——
   512MB 环 20 万条写入/恢复一致；**1GB 预分配**寻址正确；64KB 环 6 轮×5000 条多轮回绕
   崩溃恢复 = 最新周期（max_seq=30000，旧周期被覆盖）；16 轮循环写入文件恒定
   （环形覆盖均匀 = **SSD 磨损天然均衡**，无文件级 GC）。
2. ~~**kernel 整合**~~ ✅：`[storage] wal_ring_size_mb` 默认 64→**256MB**（规模化：减少小环
   频繁回绕强制 Flush，磁盘便宜）；新增 wal 测试 `ring_large_capacity_multi_wrap_recovery`
   （8MB 环 6 轮×8 万条 = 48 万条 > 容量触发跨轮回绕 → 崩溃重开恢复最新周期，max_seq 一致）。
3. ~~**验证**~~ ✅：`cargo test` **319 全绿**（+1 wal 规模化回归）；demo 4 测试全绿（见 1）；
   提交 `4974ef3`。⚠️ WAL P99 收益需在 **SSD 环境**实测（HDD 不压测）。

### 7.33 Ex-5.6 删除位图（design 4.6 / 4.8.3 P1 阶段二）

> 背景：删除语义 MVP 走 Tombstone 写主数据——每次 delete 触发 WAL append + **逐条 fsync**
> （`delete_bytes` 内 `sync_wal`）+ memtable 墓碑 + 后续各层级传播，删除 IO 重且墓碑污染 LSM。
> Ex-5.6 = 独立于 LSM 的按 DocId 1bit 删除位图（design 4.6 "SSD 原生"段）。

1. ~~**demo 研究（src/demo/deletion-bitmap/，gitignore 不提交）**~~ ✅：6 测试全绿——
   位语义置位/清位/查询 O(1)；**4KB 页对齐**（docid 32768 边界正确分页，文件按页增长）；
   **持久性**：flush=durable / 未 flush=崩溃丢失（由 WAL 回放重建）；页首/页尾批量恢复；
   **IO 经济性**：同页 1000 次删除 = 1 页 4KB + 1 次 fsync（对比 LSM Tombstone 全链路 -99%）；
   compaction 过滤语义：已删 key 物理丢弃 + put 清位复活。
2. ~~**kernel 整合**~~ ✅：
   - `src/bitmap.rs`：稠密 `Vec<u64>` 位图（1.5 亿 docid ≈ 19MB）+ 脏页集合 + 4KB 页对齐
     文件（纯位数组无头，按页增长）；`mark_deleted/clear/is_deleted/is_deleted_key/flush`；
     置位/清位仅在位真正翻转时标脏（重复删除 / 未删 put 清位 = 零 IO）；零 unsafe
     （mmap 与 inverted FST 同策略：文件 read/write 替代，保留页粒度 IO 本质）；
   - `Engine::delete`：位图开启时**仅写 1bit + primary WAL 删除记录**（`delete_record_wal`，
     不写 memtable Tombstone、不逐条 fsync）——墓碑不进入 LSM 层级；WAL 记录供增量备份
     导出与崩溃回放（回放转 `Engine::delete` 重新置位，幂等）；
   - `Engine::get/get_at`：位图 O(1) 判定先于 HotCache/LSM（已删文档零 LSM 读）；
   - `Engine::put_nosync`：put 覆盖 delete → 清位复活（位未置零 IO）；
   - `Engine::flush_wal`：位图脏页**先于** WAL fsync 落盘（崩溃时序安全：位图持久早于
     环形 WAL 截断推进，删除不丢）；
   - `ColumnFamily::compact_filtered(drop_key)`：compaction 按位图**物理丢弃**已删 key
     （不保留数据、不写 Tombstone，旧数据随压实直接回收）；`compact()` 委托空过滤；
   - `[storage] deletion_bitmap_enabled = true` 默认开启（design 4.8.4 模板）；关闭回退
     传统 Tombstone 路径（MVCC 快照隔离仅限关闭模式保留——位图删除为立即/全局语义）。
3. ~~**验证**~~ ✅：`cargo test` **330 全绿**（+10：bitmap 4 + engine 5 + column_family 2；
   `get_at_returns_none_after_delete_before_snapshot` 按 Ex-5.6 语义拆分：位图开启=旧快照
   同样隐藏 / 关闭=保留 Tombstone MVCC）；demo 6 测试全绿（见 1）；clippy 新代码零警告；
   提交 `e615071`。⚠️ 删除 IO 收益需在 **SSD 环境**实测（HDD 不压测）。

### 7.34 Ex-5.7 倒排 FST + Mmap 字典（design 5.2.4.1 / 4.8.3 P1 阶段二）

> 背景：FST 术语字典（term → 段内偏移）M4 已落地但受零 unsafe 约束用 `fs::read` 全量读入
> （P23：mmap 留待独立 crate 封装 unsafe 白名单后落地）。Ex-5.7 = 落地 mmap 按需加载——
> 冷启动零堆分配（design 5.2.4.1：mmap 仅建虚拟地址映射，物理页缺页按需加载）。

1. ~~**demo 研究（src/demo/fst-mmap/，gitignore 不提交）**~~ ✅：4 测试全绿（计数分配器实测）——
   `fst::Map<Mmap>` 可行（泛型 AsRef<[u8]>）；100 万 term 原始 24MB → FST 17.3MB；
   **冷启动对比**：fs::read 堆分配 17.3MB（全量读入）vs mmap 0B + 0.14ms（按需）；
   **按需分页**：100 次查表堆分配 0B（物理页在 OS Page Cache，RSS 与访问量成正比）；
   查表两版（fs::read / mmap）完全一致 + 全量 30 万 term 命中正确。
2. ~~**kernel 整合**~~ ✅：
   - **crates/mmap-file/**（新独立 crate = P23 unsafe 白名单）：`MmapFile` 只读 mmap 安全封装
     （`open` → `Deref<[u8]>` + `AsRef<[u8]>`）；`unsafe impl Send + Sync` 附完整论证
     （只读映射无写逃逸 + 文件不可变约定 + fd 生命周期解耦）；主库 `#![forbid(unsafe_code)]` 不变；
   - `inverted.rs`：`dicts` 改 `HashMap<String, fst::Map<MmapFile>>`——open 加载与
     `write_fst_dict`（先 fsync → rename → mmap）均走 mmap；gc **先清字典释放映射再删旧文件**
     （Windows 已映射文件无法删除）；
   - 修复 Windows 坑：只读句柄 `sync_all()` 被拒（FlushFileBuffers 需写权限）→ 写句柄上 fsync。
3. ~~**验证**~~ ✅：`cargo test` **330 全绿**（既有 FST/GC 测试全部走 mmap 路径，Windows 本机
   验证映射释放顺序）；mmap-file crate 3 测试全绿；demo 4 测试全绿（见 1）；clippy 零警告；
   提交 `442981c`。⚠️ 冷启动收益需在 **SSD 环境**实测（HDD 不压测；真实 1.18 亿 term 规模
   FST 数百 MB 时 mmap 收益放大）。

### 7.35 Ex-5.8 元数据-数据解耦（design 4.8.3 P1）

> 背景：SST 文件内元数据区（Block Index/Bloom/Footer）与数据块区已物理分离，但 Compaction
> 合并时仍读全部输入行 → 排序去重 → 重分块重压缩（写放大 + 压缩 CPU + 读放大）。
> Ex-5.8 = 无重叠输入段合并时**数据块级复用**（原样拷贝数据块，只重建元数据区）。

1. ~~**demo 研究（src/demo/meta-data-separation/，gitignore 不提交）**~~ ✅：3 测试全绿——
   **元数据占比 2.59%**（400K 键×100B：数据块 50.2MB / 索引+布隆 1333KB）——元数据本身小，
   收益不来自"只写元数据字节"，而来自**数据块免重压**；无重叠合并对比：全量重写 51.5MB/
   4041ms（读入 40 万行解压重压）vs 块级复用数据块 50.2MB（零解压）；块边界对齐验证
   （A 末键 < B 首键、拼接后偏移线性平移正确）。
2. ~~**kernel 整合**~~ ✅：
   - `SstWriter::add_raw_block(raw, compressed)`：原样写压缩字节 + 重建 trailer/索引/分区布隆
     （行式块 kind=0；PAX 块返回 Unsupported → 调用方回退全量）；
   - `SstReader::block_raw(e)`：读块原始压缩字节 + 解码内容（供复用拷贝）；
   - `ColumnFamily::compact`：先 `try_meta_only_compact`——行式列族（pax_hot_fields 空）+
     相邻段 key 无重叠（前段 max < 后段 min）→ 数据块区按 key 序原样拼接 + 只重建元数据；
     否则回退全量合并（`compact_merge`，抽公共 `finalize_compact` 收尾）；
   - `Engine::compact`：位图无已删 docid 时 primary 走 `compact()`（触发块级复用）；
     位图有已删 docid 时走 `compact_filtered`（物理丢弃需全量）。
3. ~~**验证**~~ ✅：`cargo test` **333 全绿**（+3：sstable raw_block_reuse_roundtrip /
   column_family 无重叠复用 + 有重叠回退）；demo 3 测试全绿（见 1）；clippy 新代码零警告；
   提交 `cd00d85`。⚠️ 写放大收益需在 **SSD 环境**实测（HDD 不压测；无重叠 L0 段比例越高收益越大）。

### 7.36 Ex-5.9 冷热感知 Compaction + Bloom Merge（design 4.8.3 P2）

> 背景：Compaction 无热度概念——冷热段同等对待；Bloom Merge 需"合并前布隆判断有效性"。

1. ~~**demo 研究（src/demo/heat-compaction/，gitignore 不提交）**~~ ✅：3 测试全绿——热段排序
   选段（热度 [100,5,50,20,30] 取前 2 = [0,2]）；部分 L0 合并读语义不变（6 段→选热 2 段，600 键
   全可读）；热段优先下沉（4 轮内热段退出 L0，L0 余 2）。
2. ~~**kernel 整合**~~ ✅：`SstReader` 读热度计数（`touch()`/`heat()`，点查布隆放行后递增——
   未命中/布隆拦截不计数）；`ColumnFamily::sst_heat(idx)` 监控接口；`select_compaction_inputs`
   增加 heat 参数——L0 段数超过 `l0_stall_threshold` 且存在热度时**优先合并最热的 level_limit 段**
   （热段先下沉 L1 聚合，热点读路径段数更快减少）；无热度/未超阈值维持全量合并。
   Bloom Merge 定位：Ex-5.8 的无重叠检测（索引范围）+ 分区布隆重建（add_raw_block）已承担
   "合并前布隆判断有效性"。
3. ~~**验证**~~ ✅：`cargo test` **335 全绿**（+2：热段优先选段 + 热度统计）；demo 3 测试全绿；
   clippy 新代码零警告；提交 `ba709e2`。⚠️ 热段下沉收益需在 **SSD 环境**实测。

### 7.37 Ex-5.10 多 SSD 条带化（design 4.8.3 P2）

> 背景：单盘布局 WAL/SST/倒排同盘竞争 IO；SSD 便宜，多盘条带化让 WAL 独占最快盘、
> SSTable 与倒排分盘，消除 IO 争用（多盘 +3~4×）。

1. ~~**demo 研究（src/demo/stripe/，gitignore 不提交）**~~ ✅：1 测试全绿——三个 tempdir 模拟
   三块物理盘，500 文档写入 → WAL 落 wal_dir/primary、SST 落 sst_dir、倒排落 inverted_dir，
   跨重启恢复 + 倒排检索全通；data_dir 不再承载列族数据。
2. ~~**kernel 整合**~~ ✅：
   - `[storage] wal_dir / sst_dir / inverted_dir`（Option<String>，默认 None = 单盘 data_dir
     旧布局）——Engine::open 目录路由：sst_root = sst_dir‖data_dir，wal_root = wal_dir‖sst_root，
     inverted_root = inverted_dir‖data_dir；
   - `ColumnFamily::open_with_wal_dir(name, dir, wal_dir, cfg)`：WAL 独立盘按列族子目录
     `wal_dir/{name}/wal.log`（多列族同名 wal.log 隔离）；旧 `open` 委托 None；
3. ~~**验证**~~ ✅：`cargo test` **336 全绿**（+1：multi_ssd_striping_places_files 落位 + 跨重启）；
   demo 1 测试全绿；clippy 零警告；提交 `e6a5610`。⚠️ 多盘吞吐收益需在 **SSD 多盘环境**实测。

### 7.38 Ex-7.1 PerCpuCounter 缓存伪共享（design_extension v0.5 第 12 章 P0）

> 背景：多核高频 `AtomicU64::fetch_add` 同一计数器 → 核间缓存行乒乓（伪共享），吞吐随核数
> 不升反降。

1. ~~**demo 研究（src/demo/per-cpu-counter/，gitignore 不提交）**~~ ✅：3 测试全绿——CpuSlot
   size=align=64（缓存行隔离）；**8 线程 ×200 万写：单 AtomicU64 347ms vs PerCpuCounter 166ms
   （2.1×）**；6 线程并发写汇总正确 + reset 归零。
2. ~~**kernel 整合**~~ ✅：`src/per_cpu.rs`——`PerCpuCounter`（槽位数组 `#[repr(align(64))]` +
   `thread_local` 首访分配槽位，线程数 > 槽数自然分摊；零 unsafe）；倒排 `mem_docids`（add/
   add_batch/flush 判断/重置）与 SST `heat`（Ex-5.9）改 PerCpuCounter。
3. ~~**验证**~~ ✅：`cargo test` **339 全绿**（+3：缓存行隔离/往返/并发写）；demo 3 测试全绿；
   clippy 零警告；提交 `c5fa66c`。

### 7.39 Ex-7.2 绑核默认开启（design_extension v0.5 第 12.2 P1）

> 背景：多核机器线程调度抖动 → P99 毛刺；三池（网络/计算/IO）绑物理核分区稳定延迟。

1. ~~**kernel 整合**~~ ✅：`[affinity]` 配置（enabled 默认 true；network/compute/io 核列表，
   空 = 自动分区：network=核0、compute=中部、io=尾部，1 核退化 no-op）+ `src/affinity.rs`
   （`plan_partition` 纯函数 + `bind_current` 用 core_affinity crate 绑当前线程，跨
   Windows/Linux/macOS；失败忽略，taskset 兜底）——server 主线程绑 network、Compaction
   并行线程绑 compute、组提交后台绑 io。
2. ~~**验证**~~ ✅：`cargo test` **342 全绿**（+3：分区/禁用/显式覆盖）；顺带修复 seqlock
   低频写测试并行负载 flaky（写间隔 20→100µs）；提交 `b294532`。⚠️ P99 收益需 **SSD 多核
   环境**实测。

### 7.40 Ex-7.3 io_uring SQPOLL + 多队列（design_extension v0.5 第 12.3 P1）

> 背景：NVMe 多硬件队列下 WAL 与 SSTable 分队列、WAL fsync 与刷盘并行，避免单队列拥塞。

1. ~~**kernel 整合**~~ ✅：`src/io_queue.rs` IO 队列抽象——`IoClass`（Wal/Sst/Inverted）→
   队列号 0/1/2（与 Ex-5.10 wal_dir/sst_dir/inverted_dir 多盘条带化对齐）；`io_uring_enabled`
   仅 Linux 且配置开启时生效（Windows 恒 false）。io_uring 本体为 unsafe 依赖（memmap 同策略），
   待独立 crate 封装后接入 SQPOLL 后端（接入点：`IoClass::queue_id()` 路由）。
2. ~~**验证**~~ ✅：`cargo test` **344 全绿**（+2：队列号互异 + io_uring 平台门控）；提交 `fd0b519`。

### 7.41 Ex-7.4 Compaction 动态限流（design_extension v0.5 第 12.6 P2）

> 背景：静态 `io_rate_limit_mb` 恒定限速——写压力低时浪费带宽（L0 追赶慢），写压力高时
> Compaction 与前台抢盘。动态限流 = 按前台写压力实时调速率。

1. ~~**demo 研究（src/demo/dynamic-rate/，gitignore 不提交）**~~ ✅：3 测试全绿——Token
   Bucket 动态调速（set_rate 生效，无突发赠予）；压力映射 p=0 全速 / 0.5→75% / 1→50%；
   让路语义：压力 1 取 1KB 等 1000ms vs 压力 0 后 500ms。
2. ~~**kernel 整合**~~ ✅：
   - `IoRateLimiter::set_rate(字节/秒)` + `rate()`：运行中调整补桶速率（容量受新突发上限
     约束）；0 = 不限速；
   - `ColumnFamily::set_io_rate_bytes` / `io_rate`：动态调整/读取后台 IO 限速；
   - `Engine::adjust_compaction_io_rate()`（put 路径调用）：**MemTable 水位 = 写压力代理**
     （used/max clamp 0~1）→ 限速 = base × (1 - 0.5p)——压力 0 全速追赶 L0 合并，压力 1
     让路 50% 磁盘带宽给前台写；flush 后水位回落限速回升。
3. ~~**验证**~~ ✅：`cargo test` **346 全绿**（+2：set_rate 调速 + Engine 写压力让路/回升）；
   demo 3 测试全绿；clippy 零警告；提交 `ddbc20e`。⚠️ 写 P99 收益需 **SSD 环境**实测
   （写重负载下 Compaction 让路对比静态限速）。

### 7.42 Ex-6.2/6.3 倒排并发读优化（design_extension v0.4 第 11 章）

> 背景：Ex-6.1 已提供 Seqlock 原语；倒排段清单（`segments`）与 FST 字典（`dicts`）为
> 普通容器——flush/gc 更新与 search 读无并发保障。Ex-6.2/6.3 = 原子指针发布（ArcSwap），
> 读路径拿 Arc 快照无锁。

1. ~~**kernel 整合**~~ ✅：
   - **Ex-6.2 段清单**：`segments: Vec<String>` → `ArcSwap<Vec<String>>`——flush/gc 用
     `rcu`/`store` 发布新快照；search/doc_count/iter_terms/segment_count/segment_bytes/
     should_gc/persist_manifest 读路径 `load()` 拿 Arc 快照无锁（快照一致性：发布前读到的
     旧 Arc 在发布后仍可查旧段，段文件未删前）；
   - **Ex-6.3 FST 字典**：`dicts: HashMap<String, fst::Map>` → `ArcSwap<HashMap<String,
     Arc<fst::Map<MmapFile>>>>`——值改 `Arc`（MmapFile 不可 Clone，Arc 使 HashMap 可整体
     Clone 供 rcu 发布）；查询 `load().get(seg)` 拿 Arc 快照零拷贝；
   - gc 顺序保持 Windows 兼容：先 `store` 发布新快照（释放旧映射）再删旧文件。
2. ~~**验证**~~ ✅：`cargo test` **349 全绿**（+3：快照一致性 / 8 线程并发读 &self 安全 /
   flush 与读交替结果一致）；倒排 34 测试全绿；clippy 零警告；提交 `c8183cf`。
   ⚠️ 真实读写并发收益需**读写分离**（I 模块）解除 Engine 全局锁后实测。

---

### 7.43 Ex-1 本地消息表 + 幂等消费（design_extension v0.1 L1 首选方案）

> 背景：分布式事务写路径决策 = 单分片本地事务（无需 2PC）。L1 本地消息表 + 幂等消费为
> 首选落地（解决双写扩容衔接 / 异步索引补偿 / 跨节点异步写）。复用 ReplicationLog 的
> seq 游标 + 幂等 apply 思想。

1. ~~**demo**~~ ✅（src/demo/outbox/，4 测试）：消息表与业务写原子性（崩溃恢复 outbox 不丢/
   不重复）、投递重试、幂等消费（重复投递不叠加）、排空校验。
2. ~~**kernel 整合**~~ ✅：
   - **Outbox 存储**（src/outbox.rs）：`Outbox` 包装列族 "outbox"，key = docid ++ seq
     （to_be_bytes），value = [status u8] ++ payload——`enqueue`（put_bytes_nosync，与业务写
     共享全局 seq 与 fsync 点，天然本地原子）、`scan_docid`/`scan_all`、`mark_done`、
     `dispatch`（投递器回调）、`pending_count`/`drained`（排空校验）、`wal_next_seq`/
     `sync_wal`（纳入 Engine::flush_wal 落盘，pending 重启存活）；
   - **幂等消费**：`IdempotentConsumer` 按 (docid, seq) applied 集合去重，防重复投递叠加；
   - **Engine 集成**：`outbox: Option<Outbox>`（OutboxConfig.enabled 默认关，零额外开销）、
     open 时打开 outbox 列族、global_seq 取 max 含 outbox、flush_wal 同步 outbox WAL、
     新 API `enqueue_outbox` / `dispatch_outbox`（投递后 flush_wal 防重投）/ `outbox_pending` /
     `outbox_drained`。
3. ~~**验证**~~ ✅：`cargo test` **356 全绿**（+7：demo 4 + engine 3——e2e enqueue→dispatch→drain、
   pending 重启存活、默认关闭）；提交 `7348acd`。Ex-1.5（与 M5 双写扩容协议衔接）留待真实
   扩容联调时落地。

---

### 7.44 Ex-2 SAGA 编排 + 补偿状态机（design_extension v0.1 L2）

> 背景：分布式事务 L2 = 跨分片业务事务（一笔操作涉及多个 docid 落在不同分片）。
> 决策：SAGA 编排（docid 级本地事务为步骤 + 反向补偿），不做 2PC/TCC；补偿须覆盖
> 超时分支（宁可多发由屏障空转）；补偿幂等是硬前提。

1. ~~**demo**~~ ✅（src/demo/saga/，6 测试）：正向前进全成功 / 中段失败反向补偿 /
   超时分支屏障空转（空回滚防护）/ 终态拒迟到正向（悬挂防护）/ 崩溃恢复续跑 /
   补偿失败重试 + 幂等。
2. ~~**kernel 整合**~~ ✅：
   - **步骤状态机**（src/saga.rs）：`SagaStatus`（Init→Executing→Succeeded/Failed→
     Compensating→Compensated）+ `SagaState`（tx_id、executed_steps、compensated_steps、
     last_error）；`SagaStep` trait + `ClosureStep`（正向/补偿闭包）；
   - **协调器**：`SagaCoordinator::run`（启动/续跑；Failed/Compensating 自动转补偿）、
     `compensate`（对已登记分支逆序补偿，任一失败保持 Compensating 重试）；
   - **屏障（Barrier）**：分支登记（正向成功后记录 executed_steps）先于补偿——空回滚
     防护（未登记分支不补偿）+ 悬挂防护（终态/已补偿分支拒绝迟到正向）+ 补偿幂等键
     （tx_id+step）；`status`/`all_states` 回查接口持久化 transactionId→status；
   - **持久化**：JSON tmp+rename 原子写（saga-{tx_id}.json，复用 MvScheduler 模式），
     重启加载全部续跑。
3. ~~**验证**~~ ✅：`cargo test` **362 全绿**（+6：正向成功无补偿 / 中段失败逆序补偿 /
   重启恢复终态 / 终态拒绝重复正向 / 补偿重试 3 次成功 / 重复登记被拒）；提交 `990bf6b`。
   ⚠️ Ex-2.5 网关 `/saga/start|status|compensate` HTTP API 与 2 节点跨分片端到端联调
   留待分布式构建阶段。
   顺带：seqlock P48 补强——低频写间隔 100→200µs，消除并行负载 flaky（`low_frequency
   write_low_retry_rate` 偶发 >1% 阈值）。

---

### 7.45 Ex-3 Calvin 确定性事务评估（design_extension v0.1 L3，研究/评估不落地）

> 背景：L3 = 强一致跨节点远期候选（Calvin 式确定性事务，无协调者）。本项为研究/评估，
> 不承诺进入 kernel。

1. ~~**调研**~~ ✅（Ex-3.1）：Calvin（Yale, SIGMOD'12/IEEE'13）三阶段流水线（logging→
   scheduling→execution）、确定性锁（读写集预声明、按确定序一次申请全部锁、无跨网络
   锁等待）、请求先落持久化复制日志、执行等价按日志序串行 → 副本无分歧无 2PC；代价：
   读写集须预声明（依赖读需侦察）、排除交互式会话。
2. ~~**demo**~~ ✅（Ex-3.2，src/demo/calvin/，4 测试，tick 模拟）：
   - 高冲突（键域 40/N=400）：2PC 锁等待占总耗时显著 vs Calvin 确定性序**零等待**；
   - 低冲突（键域 10 万/N=2000）：2PC 每事务下限 = 2×RTT=100 ticks（固定往返）vs
     Calvin = EXEC+append=11（无协调往返）；
   - 跨分区占比 10%/50%/90%：2PC 105k/125k/145k ticks vs Calvin 恒定 11k——吞吐与
     跨分区比例无关；
   - 确定性序执行：副本间最终状态一致（无分歧 = 无需提交协议）。
3. ~~**决策**~~ ✅（Ex-3.3）：**不进入 kernel（远期方向保留）**。① 本项目写路径 =
   docid 一致性哈希确定性路由 → 单 docid 事务天然不分片；② L1 outbox + L2 SAGA 已覆盖
   异步/最终一致；③ Calvin 需全局事务序协调器（单点）+ 读写集预声明（倒排词表难静态
   声明）。**触发条件**：强一致多 docid 跨分区事务需求出现时，按"全局事务序 + 状态机
   衔接 ReplicationLog"落地。

---

### 7.46 Ex-4 倒排字段策略落地（design_extension 9.4，db-50m 重建验证 + 配置固化）

> 背景：9.3 三准则（枚举建倒排 / 高基数排除 / 长文本分词）与 9.4 模板已设计，本里程碑
> 落到 50M 正式库验证 + 配置模板固化。

1. ~~**重建**~~ ✅（Ex-4.1）：`config-ex4.toml`（`inverted_fields` 7 枚举 + `exclude_fields=
   ["note"]` + `max_term_len=96`）重导到 `D:\shanshui-data\db-50m-opt`（新建目录，不动既有
   数据资产）——50,000,000 行成功 / 0 失败，**838,208 ms**（59.7k rows/s，含倒排 FST 构建）；
   **inverted 2231.8MB → 144.3MB（-93.5%，16.9×）**，优于 ~200MB 预估。
2. ~~**验证**~~ ✅（Ex-4.2，CLI count + HTTP /get）：`status=active`=16,666,667 ✓、
   `city=beijing`=10,000,000 ✓、`tag_b=x`=25,000,000 ✓；点查 docid=0 / 25,000,000 /
   49,999,999 全部命中（含边界 0 与末条）。⚠️ 注意：db-50m-clean 实为 **4M 条**中间产物
   （status=active=1,333,334），非 50M 基线——基线取原始 db-50m（inverted 2231.8MB）。
3. ~~**固化**~~ ✅（Ex-4.3/4.4）：仓库新增 `config.import-example.toml`（9.4 模板 + 实测收益）；
   `src/config/model.rs` `inverted_fields` 注释引用 9.4；design_extension 9.5 回填实测；
   feature.md C 模块 + 近期里程碑更新。

> **附：倒排 posting 流式输出评估**（demo `src/demo/posting-stream/`，3 测试）：
> Roaring 位图迭代**天然惰性**（O(1) 内存）+ `search_term_paged` 回表限行 → 大结果集内存已流式；
> 深页分页为 **O(offset)**（16M offset 591ms vs 近页 36µs，16,204×）；游标续扫（range 区间迭代，
> 仿 M8-P11 scan_after）每页 **O(limit)**，10 万条流式 37×。**结论：不引入全量流端点**；
> 若未来「深页 + 高频翻页」场景出现，为 term 检索加 `search_after` 游标参数即可（O(limit)/页）。

---

### 7.47 类 SQL 解析器（design 157/1358 行：SELECT ... WHERE AND/OR 子集）

> 背景：降低 MySQL 迁移学习成本——类 SQL WHERE 查询，内部走倒排/组合索引；不承诺方言
> （无 JOIN/GROUP BY/子查询/事务）。原计划基于 sqlparser-rs；落地改为**零依赖递归下降**
> （子集很小，避免引入大型解析依赖）。

1. ~~**demo**~~ ✅（src/demo/sqlish/，6 测试）：语法（AND/OR/NOT/括号/= != > < >= <=/
   BETWEEN low AND high/LIMIT/OFFSET）+ 求值语义（交/并/补/闭区间）+ 语法拒绝（JOIN/GROUP BY）。
2. ~~**kernel 整合**~~ ✅（src/sqlish.rs）：
   - **解析器**：Lexer（关键字/标识符/引号字面量/数字/操作符）+ 递归下降（expr=or→and→unary→
     cond；BETWEEN 消费 `low AND high`）；
   - **求值**：`field=value` → `inverted_posting`（Roaring 位图；AND 交集/OR 并集/NOT 相对全量补集/
     `docid` 点查单例）；比较（>/</>=/<=）与 BETWEEN → 倒排无法表达，扫描过滤——**AND 快路径**：
     扫描叶子作后过滤，只检查另一分支（倒排等值）已命中的文档，避免全量扫描；
   - **看门狗熔断**：Engine 新增 `query_guard()` 暴露查询守卫；全量扫描/后过滤/回表逐批检查
     `is_expired()`，超时返回 QueryTooExpensive（**不挂起 server**——实测 db-50m 上 16.6M 基底
     的 BETWEEN 查询 0.7s 熔断返回 400，server 持续响应）；
   - **HTTP 路由**：`GET /sql?q=SELECT ...`（server.rs）。
3. ~~**验证**~~ ✅：`cargo test` **367 全绿**（+5：AND 交集 / OR+NOT 补集 / 比较+docid / BETWEEN
   数值区间+快路径 / 分页+语法拒绝）；`/sql` 实库验证（db-50m-opt：等值 AND 1.9s 返回正确行）；
   提交 `441282d`。顺带修复 engine `gc_thread_flushes_tail` 定时 flaky（固定 sleep → 有界轮询）。
   ⚠️ 比较/BETWEEN 在大基底下本质扫描受限（50M 库枚举等值基座 ≥10M 时熔断），推荐先倒排等值收敛。

---

### 7.48 写入 Enrich 接线（design 19.2 方案② / 19.3，development 5.21 落地）

> 背景：关联关系固定、写入时已知的场景——网络层接收后、WAL 写入前展开关联字段（预连接），
> 查询 0 增加（不做 JOIN）。库函数 `join::put_with_enrich` + `enrich_check_local` 已存在
> （development 5.21），本里程碑**接入 server /put 写路径**。

1. ~~**demo**~~ ✅（src/demo/enrich/，4 测试）：写入富化注入关联字段 / reject 拒绝 / degrade
   降级 / 默认关零开销。
2. ~~**kernel 接线**~~ ✅：
   - `[enrich] enabled && source=local` → Engine::open 记录 `enrich: Some((fail_policy,
     from_field, to_field))`（`enrich_config()` 访问器；默认 None 零开销）；
   - EnrichConfig 增 `from_field`（默认 user_id）/ `to_field`（默认 docid）；
   - server `/put`：enrich 启用时改走 `join::put_with_enrich`——WAL 前按 from→to 主键点查
     关联文档并展开 `_enrich.related`；fail_policy reject（强一致，失败拒写）/
     degrade（可用性优先，降级写原文档）。
3. ~~**验证**~~ ✅：`cargo test` **368 全绿**（+1 端到端：关联展开 / degrade 降级 / 配置生效）；
   提交 `706c33b`。

---

### 7.49 读写分离评估收口（I 模块，Ex-6 并发读前置）

> 背景：feature.md I 模块「读写分离 / 双写加速」为 Ex-6 倒排并发读（Seqlock/ArcSwap）的前置。
> 评估结论：**维持暂缓（in-process 网关 RwLock）**，但读路径 &self 基础已铺底。

1. ~~**评估依据**~~ ✅：M8-P1 `be09a07`（网关 Mutex→RwLock 剩余收益 <20%——组提交已解决
   「读被写拖垮」，B 负载 18,987→149,539 ops/s）；`src/demo/rw-separation`（主写+从读同步
   复制：读 P95 3µs→2µs **1.5×**、写吞吐持平——写瓶颈在磁盘 fsync 非锁）。
2. ~~**读路径 &self 基础**~~ ✅（`c48a7c1`）：`SstReader::read_block` 改**位置读**（read_at：
   Windows seek_read / Unix read_at，`&File` 可并发不移动游标）+ `ColumnFamily::get/
   get_bytes/get_bytes_at` **&self 化**（内部 PerCpuCounter/BlockCache/SstReader::touch 由
   Ex-5.9/7.1 已内部同步，&mut 为历史遗留）——sstable 22 + column_family 34 测试全绿。
3. ~~**剩余阻塞与路径**~~：① HotCache 内部 LruCache+stats 需 Mutex 化才能 `&self`（热路径锁
    开销需实测）；② 倒排 pending 攒批缓冲刷盘需 `&mut`——搜索类读（search/fulltext/sql/
    count）仍需写锁，仅点查/范围扫描可并发读；③ 复制型读写分离（`read_from_replica` 路由）
    基于已有 ReplicationLog，属**分布式阶段**（与本机+阿里云两节点测试衔接）。

---

### 7.50 本机性能基准（NVMe SSD，组提交 2ms，200k 记录 × 4 线程）

> 环境：本机 SAMSUNG MZVLB512 NVMe SSD（design 1.2 SSD 条件满足）；release 构建；
> `shanshui-cunji-ycsb`（负载 a/b/c × 组提交 2ms，append WAL）。

| 负载 | load 写入吞吐 | 混合吞吐 | p50 | p95 | p99 | p999 |
|---|---|---|---|---|---|---|
| a 写重 50/50 | 185,515 w/s | 90,935 ops/s | 8.0µs | 112µs | 1046µs | 1769µs |
| b 读重 95/5 | 186,770 w/s | 168,596 ops/s | 3.8µs | 62µs | 148µs | 1115µs |
| c 纯读 | 199,028 w/s | 269,891 ops/s | 3.5µs | 44µs | 75µs | 176µs |

> 结论：SSD + 组提交 + Ex-5/7 全量优化下，写重混合 9 万 ops/s、纯读 27 万 ops/s，
> p50 个位数 µs。阿里云两节点对比与分布式强一致性测试见 7.51（需服务器访问）。

---

### 7.51 两节点分布式（本机 + 阿里云）测试指引

> 前置：本机 NVMe SSD 基准（7.50）✅；两节点真实 TCP 高并发强一致性测试（gateway 测试，
> `3e22f80`）✅——分布式机制（一致性哈希路由/广播检索/元数据中心/RPC）已在库内测试框架验证。
> 真机两节点（本机 + 阿里云 106.14.68.116）对比与压测已完成（凭据不入库）。

1. ~~**本机两节点（库内测试框架）**~~ ✅：`gateway::high_concurrency_writes_strong_consistency_two_nodes`
   ——spawn 2 个 RPC 分片节点（真实 TCP）+ Gateway + MetaCenter，8 线程 × 2500 并发写 20000 条：
   广播检索精确命中、逐条点查跨节点确定性路由强一致可见、探活在线。
2. ~~**本机性能基准**~~ ✅（7.50）：YCSB a/b/c × 组提交 2ms（NVMe SSD）。
3. ~~**真机两节点执行**~~ ✅（2026-08-30，`cluster_demo` bin）：
   - **服务器恢复与构建**：阿里云 2 核/1.6GB 首次 release 构建默认多 jobs 触发 OOM 失联 →
     控制台强制重启 + 2G swap + `CARGO_BUILD_JOBS=1` → vendored offline 构建成功（3m45s）；
   - **服务器 YCSB 基准**（高效云盘 rotational=1，HDD 级，组提交 2ms）：

     | 负载 | load 写入吞吐 | 混合吞吐 | p50 | p95 | p99 |
     |---|---|---|---|---|---|
     | a 写重 50/50 | 84,565 w/s | 47,782 ops/s | 7.4µs | 31.6µs | 2677.6µs |
     | b 读重 95/5 | 85,732 w/s | 113,589 ops/s | 3.5µs | 12.9µs | — |
     | c 纯读 | 88,790 w/s | 282,114 ops/s | 3.2µs | 9.5µs | — |

     > 对比 7.50 本机 NVMe：load 约本机一半（磁盘写瓶颈）；读 p50 基本持平（C 甚至 3.2µs 略快）；
     > 长尾 p99 因 2 核 + 慢盘显著放大（a 写重 2677.6µs vs 本机 1046µs）。
   - **两节点强一致测试**：`cluster_demo --node`（分片节点，复用 M5 RpcServer + register_shard_handlers）
     本机 node-a:9091 + 阿里云 node-b:9092；安全组未放行 RPC 端口 → SSH 隧道 `plink -L 19092:127.0.0.1:9092`
     绕过（避免公开暴露无鉴权端口）；`--gateway --nodes a=...,b=...` 4 线程 × 500 = 2000 条跨机并发写
     → **强一致校验通过**：逐条点查全部可见（确定性路由）+ 广播检索精确命中（52.4s）。
4. ~~**结果回填**~~ ✅：本里程碑即结果；跨机吞吐受隧道 + 服务器磁盘/CPU 约束（20000 条超时被杀，缩至 2000 条验证）。
   分布式机制正确性（跨节点路由/广播拼接/强一致可见）已由真机证明，性能量级见 7.50/7.51 基准表。

---

### 7.52 本机 2000 万 / 5000 万大数据量基准（demo 13 项放大）+ 查询引擎优化

> 2026-08-30。`shanshui-cunji demo --scale N --config config.bench.toml`（images/perf-0.6.0/ 汇总报告）。
> 13 项测试全绿（冒烟 / 2000 万 / 5000 万）：构造数据 → 批量插入 → put_batch 批量（1000/5000 条/批）→
> 主键 100 万次 → HotCache 100 万次 → 组合索引 1 万次 → 倒排检索 1 千次 + COUNT 1 万次 → fulltext 1 千次
> → 类 SQL（等值 1 千次 + amount/ts BETWEEN 各 100 次）→ 分片抽样 → 删除 100 万次 → 备份还原。

| 测试项 | 2000 万 | 5000 万 |
|---|---|---|
| 单条流式插入 | 46,492 条/s（430s） | 30,657 条/s（1631s，写放大+compaction） |
| put_batch 批量（1000/批） | 32,746 条/s | 32,602 条/s |
| put_batch 批量（5000/批） | 35,047 条/s（fsync 次数 -80%） | — |
| 主键 ×100 万 | 15.6s（15.6µs/次） | 26.5s（26.5µs/次） |
| HotCache ×100 万 | 0.66s | 0.71s |
| 倒排（1000 检索+1 万 COUNT+抽样 20 万回表） | 0.51s | 1.44s |
| fulltext ×1000（5.4 / 64.8 ms/次） | 5.4s | 64.8s |
| 类 SQL（1000 等值 + BETWEEN 各 100） | 13.1s | 18.8s |
| 删除 ×100 万 | 2.2s | 17.3s |
| 备份 · 还原 | 48.0s | 287.0s |
| 总耗时 | ~9.4 分钟 | ~28 分钟 |

- **G 项倒排 posting 检索优化（c380792）**：`InvertedIndex` 新增 term→bitmap LRU 缓存（256 项）——
  search 对 bitmap_fields 白名单 term 直接返回全量内存位图（O(1)），非白名单 term 首次反序列化后缓存；
  写路径 add/add_batch/flush_segment/gc/with_bitmap_fields 清缓存保一致；+2 单测。
  效果：倒排 1000 检索 + 1 万 COUNT + 抽样 20 万回表 165s（10 万条旧版）→ 2000 万库 0.5s；
  fulltext 1000 次 175s → 5.4s（2000 万）。
- **倒排段 GC 合并入口**：`Engine::inverted_gc()`（496 段 → 1 段，释放 137-344MB）——批量导入后段数
  爆炸（每 100 万 term 对一段）导致查询遍历全部段过慢，合并后查询只遍历 1 段；后台化排期 J 项。
- **put_batch 批量插入 API（6197c21）**：`Engine::put_batch(&[(u64, Vec<u8>, Vec<String>)])`——
  攒批 + 一次 flush_wal 原子提交（WAL 批次整体重放），为 D 项 WriteBatch 前置；demo 批大小可配
  `SHANSHUI_BATCH_SIZE`。批量吞吐受「批次数 × fsync」限制（1000→5000 条/批 +7%）。
- **probe 定位模式**：`SHANSHUI_QUERY_MODE=probe`（查询次数=10）走完全流程定位瓶颈——
  分片全量重写 + 全量 scan（10.6 分钟）为最大瓶颈，改抽样 100 万验证分布。
- 相关修复：fulltext 词 term 与倒排白名单正交（inverted_allowed 放行 ft:）；sqlish post_filter
  LIMIT 下推（BETWEEN 后过滤免遍历全量命中集）；make_doc 增 ts 秒级时间戳（日期 BETWEEN 基准）。
- 结论：G 项 + 白名单位图 + LIMIT 下推后查询路径不再是大数据量瓶颈；插入受写放大（磁盘 200MB/s
  vs 逻辑 9MB/s）与 compaction 限制；1 亿数据基准（B 项）排期见 development_process_order.md。

---

### 7.53 LSM 事务三阶段（D/E/F）+ 倒排段数据 mmap 化（G 补充）

> 2026-08-30。development_process_order.md D/E/F/G 全部完成；393 测试全绿（+18）。

- **阶段一 WriteBatch 原子写（D）**：`src/txn.rs` 新增 `WriteBatch`（攒批 put/delete，`rollback` = 丢弃
  未应用批次）+ `Engine::write(&WriteBatch)` 原子提交——预校验（`validate`，失败零副作用 = "失败回滚"）
  → 逐条应用 → 单次 `flush_wal`（崩溃按 WAL 批次整体重放，无中间态）；错误类型新增
  `TxnConflict / TxnDeadlock / TxnAborted`。
- **阶段二 快照隔离（E）**：`Transaction` 持有事务开始时全局快照 seq（`begin_snapshot`），
  `Engine::txn_begin/txn_get/txn_commit/txn_rollback`；RR/SERIALIZABLE 走 `get_at(snapshot_seq)`
  一致快照读（复用 design 4.7 MVCC：主数据按 seq 过滤 + Delta 增量按全局 seq 隔离 + 删除位图
  立即语义）；提交时写写冲突检测——`last_write_seq(docid) > snapshot` → `TxnConflict` abort
  （事务内写不落引擎，提交时见到的快照后写必来自并发已提交事务，检测干净）。
  已知局限：MemTable 不保留多版本（design 4.7 注明），快照读需旧版本已落 SST（flush 后准确）。
- **阶段三 完整 ACID（F）**：`Isolation`（RC 读最新 / RR 快照 + 写冲突 / SERIALIZABLE 快照 +
  读共享锁 + 写排他锁 2PL 至提交，共享→排他合法升级）+ `LockTable`（docid 级锁 + wait-for 图
  死锁检测，环中请求者 victim abort）+ 提交/回滚/失败路径统一释放锁（防泄漏）。
- **G 补充：倒排段数据 mmap 化**：`data_files: ArcSwap<HashMap<seg, Arc<MmapFile>>>`——查询按 FST
  offset 直接 mmap 切片反序列化，免 `fs::read` 全文件读取 + 堆复制（大段文件未命中查询主要 IO 成本）；
  物理页按需缺页加载（P23 只读映射白名单，与 FST dicts 同模式）；flush 预注册 / 重开懒加载 /
  gc 先换新映射再删旧文件（Windows 已映射不可删）。
- 测试：+15 事务（WriteBatch 原子/回滚/预校验、RR 快照读/写冲突、RC 最新读、SERIALIZABLE 读写锁/
   升级、死锁环、delete 混合提交、快照 seq 推进）+ 3 mmap（flush 注册/重开懒加载/GC 后正确）= 393 全绿；
   问题闭环见 problem_solving P52/P53。

---

### 7.54 看门狗 CPU/磁盘三级响应（P52 设计落地）

> 2026-08-30。401 测试全绿（+8）；`crates/disk-space` 独立 crate（P23 unsafe 白名单）。

- **磁盘看门狗 `DiskGuardian`**：剩余空间三级响应——预警（warn=0.20，记录计数）/ 限流
  （throttle=0.10，软信号放行）/ 熔断（stall=0.05 **且** 绝对剩余 < 1GB → 拒绝写，只读保持）；
  熔断双条件防"小比例但空间仍充裕"误熔断（P54，C 盘 3% 实测触发）；1s 采样缓存免写路径频繁
  syscall；`crates/disk-space`（Windows GetDiskFreeSpaceExW / Unix statvfs，零新增外部依赖）。
- **CPU 看门狗 `CpuGuardian`**：并发查询数代理 CPU 压力——`Watchdog::try_begin_query` 达
  `cpu_query_limit`（默认 64）返回 Stalled 拒绝新查询（防 CPU 风暴），`QueryGuard` drop 自动释放
  槽位（Arc 计数，无泄漏）。
- **写路径统一入口**：`Watchdog::check_all(mem_ratio, data_dir)` = 内存水位 + 磁盘水位；
  挂接 put / put_batch / write / delete / txn_commit（事务提交前检查）。
- **查询执行器**：`Engine::execute` 改用 `try_begin_query`（CPU 并发限制 + 查询超时熔断双保险）。
- **admin status**：`EngineStats` 新增 disk_ratio / disk_status / cpu_active_queries / cpu_query_limit。
- 配置：`[watchdog] disk_warn_ratio / disk_throttle_ratio / disk_stall_ratio / disk_stall_min_mb /
  disk_sample_secs / cpu_query_limit`（validate 校验水位次序 0 < warn 且 warn > throttle >= stall）。

---

### 7.55 MySQL 协议适配（H-1~H-3：握手 + 认证 + SQL 映射）

> 2026-08-30。mysql cli 8.0 / pymysql 真实连接全链路通过；407 测试全绿（+6 协议级）。

- **src/mysql.rs**：MySQL wire protocol 服务器（握手 HandshakeV10 + mysql_native_password 认证
  sha1 scramble + packet/OK/ERR/ResultSet/EOF 编解码 + COM_QUERY 分发）；
  数据模型：库 `scc` / 表 `documents` / 列 `id`（BIGINT 主键）+ `doc`（JSON 文档）；
- **COM_QUERY 分发**：SHOW DATABASES/TABLES/VARIABLES、SELECT VERSION()/@@version、
  SELECT（`WHERE id=N` 主键点查 / sqlish 引擎返回 id+doc 两列）、INSERT/UPDATE/DELETE
  （简易 SQL 解析 → 文档引擎 put/delete + 倒排词条派生）、SET/BEGIN/COMMIT/ROLLBACK（放行）；
- **src/bin/mysql_server.rs**：独立 bin（`--data-dir` + `--bind 0.0.0.0:3307` + `--user` + `--password`），
  Arc<Mutex<Engine>> 每连接线程（与 rpc.rs 同模式）；
- **验证**：mysql cli 8.0 真实连接（SELECT VERSION → `8.0.0-shanshui-cunji`、SHOW DATABASES → scc、
  INSERT/SELECT/UPDATE/DELETE 全链路）+ pymysql 全链路 + 协议级测试 6 个；
- 关键坑（problem_solving P56）：授权包 seq=2（全局连续非每方向独立）、握手响应字段顺序
  （auth_response 在 db 前、plugin name 独立字段、按服务器声明跳过 db/attrs）；

---

### 7.56 MySQL 协议适配 H-4~H-6（事务语句 + 预处理 + sysbench 接入）

> 2026-08-30。H 大项全部完成；411 测试全绿（+4：事务往返/空提交语义/预处理往返/未知 stmt）。

- **H-4 事务语句**：`BEGIN/START TRANSACTION/COMMIT/ROLLBACK` → txn.rs 事务 API（RR 快照 +
  提交写写冲突检测）；会话级 `Session.txn` 跨命令持有（连接断开自动回滚 = drop）；事务内
  SELECT（快照点查）/ INSERT / UPDATE / DELETE 攒批，commit 原子落库；**同事务写后读可见**
  （`Transaction::read_own`：最近写优先遍历 ops，`Engine::txn_get` 先查未提交写）；MySQL 语义：
  无活动事务 COMMIT/ROLLBACK 返回 OK（空提交）、嵌套 BEGIN 报错；
- **H-5 预处理语句**：`COM_STMT_PREPARE`（stmt_id + 参数/列定义）、`COM_STMT_EXECUTE`
  （null bitmap + 类型表 + LONGLONG/LONG/DOUBLE/字符串二进制参数解析 → 占位符替换 `?` →
  复用 COM_QUERY 分发）、`COM_STMT_CLOSE`（释放）；JDBC/参数化查询基础就绪；
- **H-6 sysbench 接入**：多列 INSERT（`(id,k,c,pad)` → 组装 JSON 文档）+ DDL 放行
  （CREATE/DROP/TRUNCATE/ALTER TABLE → OK，文档库无 schema 语义）；pymysql 驱动 sysbench
  风格负载：prepare 20000 行 945 w/s、8 线程并发 point_select 3040 q/s、
  BEGIN/SELECT/COMMIT 事务点查 1744 txn/s（服务器单引擎串行下合理量级）；sysbench 本体需
  Linux/WSL 安装（Windows 无预编译）；

### 7.57 SAGA 网关 HTTP API + 补偿协议（design_extension 13.1/13.5，Ex-2.5 + 13.5 落地）

> 2026-08-31~09-01。SAGA 分布式事务从内核（Ex-2，7.44）接出 HTTP 网关并补全补偿协议；
> 445 → 450 测试全绿。

- **Ex-2.5 网关**（`781199e`）：`HttpStep`（HTTP 业务步骤，非 2xx/超时→失败）+ `http_post`
  （极简 HTTP/1.1 POST 客户端）+ `server.rs` 三端点 `POST /saga/start`（`{tx_id, steps[]}`，
  终态幂等）/ `GET /saga/status` / `POST /saga/compensate`；协调器持久化 `{data_dir}/saga`
  崩溃恢复续跑；`Engine::data_dir()` 访问器；+3 网关 e2e；
- **13.5 补偿协议**（`170bf21` + `136882f`）：修复"缺步骤定义被静默跳过并误标终态"——
  未补偿分支保持 `Compensating` + `last_error`；+10 测试（13.5.3 半途恢复续跑正向/失败
  续补偿/部分补偿不重复 + 缺定义保持待补偿/重试续补/跨重开持久化/终态 no-op + 超时屏障空转）；
- 文档：design_extension 13.5 补偿协议形式化（状态机 + 不变量 I1~I4 + 崩溃恢复时序表）。

### 7.58 SAGA 13.6/13.7 拓扑并行执行 + 后台对账重试（design_extension 13.6/13.7）

> 2026-09-01。SAGA 长事务正向并行化 + 未终态自动收敛；450 → 459 测试全绿（提交 `71aa712`）。

- **13.6 拓扑并行**：`topo_layers`（Kahn 分层 + 环/自依赖/越界检测 → 网关 400）+ `run_parallel`
  （scoped 线程按拓扑层并行正向、层间屏障、失败转补偿；executed_steps 按层序登记 → 逆序补偿
  = 反拓扑序）；`/saga/start` 解析 `depends_on`（未知/环 → 400）；`SagaStep` 加 `Send + Sync`；
- **13.7 后台对账**：`SagaState` 增 `retry_count`/`last_retry_at_ms`/`updated_at_ms`
  （serde default 兼容旧状态文件）+ `retry_pending`（Failed/Compensating 指数退避续补偿、
  Executing 挂起检测、无步骤定义跳过）；`server.rs` 协调器 `Arc<Mutex>` 共享 + 步骤定义缓存
  + 60s 对账线程；+9 测试（分层/环/依赖序/并行提速/反拓扑补偿/退避/挂起/网关环拒绝）。

### 7.59 1 亿库复测 + 写路径 syscall 风暴修复（l0_bytes/sst_bytes 快照缓存化）

> 2026-09-01。1 亿库（db-100m-v070）全套测试 + 定位并修复写路径性能崩塌；459 → 462 全绿。

- **结构检查**：SST（NVSSTL01/V5）与 manifest 格式自构建以来零变化——兼容、无需重建；
- **读类大幅提升**（R 项层/段 Zone Map 粗筛 + M 项范围一次扫描 + O 项 RwLock 无锁化）：
  oltp_point_select 12,816（基线 5,087，+152%）、select_random_points 27,897（+342%）、
  select_random_ranges 30,410（+330%）；
- **写路径修复**（`96ac6bc`）：根因 = `needs_compact` 大小条件每次 put 调 `l0_bytes()` 的
  `fs::metadata`（L0≥2 段时每次写 N 次 stat 的 syscall 风暴）→ 写吞吐崩塌（oltp_insert 2.7k TPS）。
  修复：`SstReader::file_len`（open 一次 metadata）+ `SstSnapshot::sizes` 缓存
  （open/flush/compact 构建时填）→ `l0_bytes()`/`sst_bytes()` 零 syscall；
- **实测**（8 线程 15s）：oltp_insert 2,676→23,964（+795%）、bulk_insert 4,630→210,895（+45×）、
  oltp_update_non_index 3,145→8,896（+183%）；+3 测试（file_len/sizes 与磁盘一致/重开一致）。

### 7.60 写路径收尾：合并阻塞观测 + 分批合并（compact_input_max_mb）

> 2026-09-01。后台 worker 持读锁合并阻塞写（RwLock 语义）的缓解；462 → 465 全绿（`1763554` + `0e4e40c`）。

- **观测**（1 亿库写 ~1.5GB 触发合并）：合并期间写吞吐 39k → 8.2k rows/s（-80%），阻塞 ~60s；
- **修复**：`[storage] compact_input_max_mb`（默认 1024MB）——`select_compaction_inputs` 对
  L0→L1 输入按大小分批（`cap_by_size` 保底 2 段，剩余段 worker 多轮收敛）；L1→L2 不受限
  （层内不重叠需全选）；复测合并期间写吞吐 8.2k → 18.2k（-55%）；
- **说明**：分批缓解未根治——根治需无锁合并（CF Arc 化 + 合并与写并发），留待后续；
- 事务类复测（写修复后与 O③ 持平）：read_only 523/561、read_write 243/215 TPS。

### 7.61 1 亿库构建记录与问题闭环汇总

> 2026-09-01。构建记录（images/perf-0.7.0/sysbench-100m/构建记录.md）追加：插入性能复测
> （pymysql 单条 ~2k、批量 ~4k rows/s）、sysbench 全套对比基线、写路径 A/B 定位（组提交有效
> 18× / auto_compact 检查 syscall 风暴）、事务类复测；problem_solving P69（缺定义补偿）
> / P70（13.6/13.7 落地）闭环。

### 7.62 导出增强：export --parquet（E 模块，Parquet 导出落地）

> 2026-09-01。E 模块"导出增强（增量 / Parquet / JDBC）"的 Parquet 部分落地（`70c3b30`）。

- `shanshui-cunji-export --parquet <out.parquet>`：两列 docid(Int64) + json(Utf8)，SNAPPY 压缩，
  10 万行分块 ArrowWriter（复用 M8-P3 arrow/parquet 依赖）；与 `--csv` 并存（模式互斥）；
- CLI 环回实测（`tmp_export_test.jsonl` 3 行）：JSONL 导入库 A → Parquet 导出 → Parquet 导入
  库 B → CSV 导出，3 行往返一致；
- 增量（docid 游标，对称 P3-4 增量导入）/ JDBC 留阶段 2+。

### 7.63 io_uring Linux 部署验证指引（V 项收尾）

> 2026-09-01。V 项代码已落地（crates/io-uring-file + IoUringPool 三队列 + SQPOLL 预留核 +
> `[runtime] io_uring_enabled` 接入，Linux 门控）；本机（Windows，WSL 异常 / 无 Docker）无法
> 实测，交付以下部署验证指引（阿里云 2 核/1.6GB 编译大依赖有 OOM 风险，建议 ≥4GB 环境）。

1. **前置**：Linux 内核 ≥ 5.1（io_uring）；`crates/io-uring-file` 依赖 `io-uring 0.7` crate
   （纯 Rust，无 liburing C 依赖——无需 gcc 交叉编译）；
2. **编译**：Linux 上 `cargo build --release`（zstd-sys 需系统 gcc + 开发头，如
   `apt install build-essential`）；
3. **配置**：`[runtime] io_uring_enabled = true`（默认关，Windows 编译无此字段）+ 预留 SQPOLL
   核（`[runtime] sqpoll_cpu`，需与绑定核池无重叠，见 affinity `reserve_sqpoll_core`）；
4. **验证**：
   - 启动 mysql-server 连小库 → 日志确认 io_uring 池初始化成功（无回退到同步 IO 的告警）；
   - A/B 对比（io_uring 开 / 关）：YCSB 写重负载（WAL fsync 走 SQPOLL 队列）与读负载
     （块 read_at 走 io_uring）的吞吐 / P50 / P95 延迟；
   - 核隔离：`reserve_sqpoll_core` 预留核期间无业务线程落在该核（Ex-7.2 绑核验证方法）；
5. **预期**：写路径 fsync 批处理与读路径异步提交减少 syscall 上下文切换；收益在核多 / 高
   IOPS 场景显著，2 核小机器可能不显著。

### 7.64 导出增强：export --incremental --checkpoint 增量导出（E 模块，docid 游标断点续传）

> 2026-09-01。E 模块"导出增强（增量 / Parquet / JDBC）"的增量部分落地（`2174531`）。
> 对称 P3-4 增量导入（`5085db8`）；JDBC 直连（阶段 3）与流式管道 Filter/Projection 留后续。

- `shanshui-cunji-export --csv out.csv --incremental [--checkpoint cp]`：DocId 游标断点续传——
  首次全量导出并记录最大 docid 到 checkpoint（默认 `out.checkpoint`）；后续只导
  `docid > checkpoint` 的新数据并推进游标（CSV / Parquet 双路径，`export_csv`/`export_parquet`
  增加 `base: u64` 参数，返回 `(rows, max_docid)`）；
- **checkpoint 缺失语义**：对齐 import.rs `load_checkpoint().unwrap_or(0)`——首次运行 / 断档
  按全量导出处理并重建 checkpoint（修正原实现：checkpoint 缺失直接报错退出）；
- **不变量**（`migrate.rs incremental_export_cursor_progresses` 固化）：只导 `docid > base` 的新行、
  `max_docid` 单调推进、无新数据时 `max_docid == base` 不写 checkpoint（游标不前进）；
- **原子写**：复用 `save_checkpoint` tmp+rename（`checkpoint_atomic_persist` 已覆盖）；
- 端到端验证（D:/shanshui-tmp/exp-a）：首轮 3 行全量 + cp=3 → 追加 2 行二次增量只导 4/5 + cp=5 →
  无新数据 0 行不推进 → 删 cp 断档自动全量 5 行重建；
- 全量测试 466 → **468 全绿**。

### 7.65 合并阻塞写根治：无锁合并（P72 完整方案 + P73 manifest 竞态修复）

> 2026-09-01。P72 根治落地（`af24dbd`）——worker 合并不再持 Engine 读锁，写与合并并发；
> 1 亿库实测暴露 P73 manifest 竞态并修复（`3d58137` + `5de5ab0`）。469 全绿。

- **背景**：P72 阶段一（分批 + worker 单轮）只缓解合并阻塞写（-55%）；backstop 大段合并
  仍分钟级阻塞写（2026-09-01 复测复现，见 P72 复现补充）。根治 = 合并不持 Engine 锁。
- **方案（P72 完整版）**：
  1. `MemTableBuffer` 内部 `RwLock`——`switch`/`take_immutable` `&self` 化（Engine 字段
     `Arc<ColumnFamily>` 后 flush 无法取 `&mut`）；`iter_range` 改 HRTB 闭包式
     `with_iter_range`（scan_stream_at 的 k-way merge 主体入闭包，锁作用域内消费借用）；
  2. CF `switch_and_flush`/`flush_single`/`flush_buckets` `&self` + `sst_mutate: Mutex<()>`
     （flush 与 compact 无 Engine 锁并发的 ssts store/manifest 变更互斥）；写方法
     （put/delete/sync_wal/wal_append 等）`&self` 化；`write_pressure` 原子化（f64 bits）；
  3. Engine `primary`/`cidx`/`delta` 改 `Arc<ColumnFamily>` + `deletion_bitmap`
     `Arc<DeletionBitmap>`（DeletionBitmap 内部 RwLock/Mutex `&self` 化）；
  4. mysql worker：读锁内 `Engine::compaction_targets()`（clone 三 CF Arc + 位图 Arc +
     紧迫度判定，快速）→ **drop 锁** → `CompactTargets::run()` 无锁合并——写语句持 Engine
     写锁与合并**并发执行**（ssts 变更经 CF `sst_mutate` 与 flush 互斥，无丢失更新）；
- **1 亿库实测**（原配置 l0_max_size_mb=1024）：持续写入 36-43k rows/s 全程稳定；
  tmp 配置（l0_max_size_mb=256）强制触发合并：日志 `→L1 合并 2 段 → 1` 正常执行，
  合并期间写速率 25-31k 无塌陷（修复前 8.2k-18.2k + 分钟级阻塞）；flush 正常、数据点查完整。
- **P73（实测暴露）**：无锁合并后 `persist_manifest` 磁盘扫描会引用"正在写入的半写段" →
  manifest 悬空引用 → 重启 `SST seek 越界` 损坏。修复：`persist_manifest` 改**内存快照**
  （ssts ArcSwap + levels）重建；`finalize_compact` 删旧段 + `flush_single` 全程移入
  `sst_mutate` 锁内（store→persist→remove 原子）。回归测试 `persist_manifest_reflects_memory_snapshot_only`
  （幽灵段不入 manifest）；1 亿库原数据（79/88/89/97/98/99 六段）完整恢复，点查全命中。

### 7.66 导出增强：流式管道（Filter/Projection/Sink 分叉）+ JDBC 直连（E 模块，design 20.5）

> 2026-09-01。E 模块"导出增强（增量 / Parquet / JDBC）"的**流式管道**与 **JDBC 直连**落地
> （`c6b5417`）。增量导出（7.64）+ Parquet（7.62）已完成；剩余 MySQL 兼容 CSV 配套留后续。

- **流式管道**（`src/export_pipeline.rs`）：SST 流式扫描 → Filter → Projection → **Sink Adapter
  分叉**（CSV / Parquet / JDBC），每批 `batch_size` 刷一次，内存恒定（批 × 单行）：
  - `--filter 'field op value AND ...'`：op ∈ `=` `!=` `>` `>=` `<` `<=` `CONTAINS`；
    值支持数字 / '字符串'（含 `\'` 转义）/ true / false / null；AND 组合，字段缺失不通过
    （Eq 语义）；
  - `--project 'a,b,c'`：字段子集输出；`--mask 'field=pattern'`：字段值脱敏替换；
  - 无 Filter/Projection 时**零 JSON 解析**（原样透传），全量导出零额外开销；
- **JDBC 直连**（`--jdbc 'mysql://user[:pass]@host[:port]/db'`，无文件落盘）：
  - `mysql.rs` 新增 `MysqlWireClient`——MySQL wire 客户端（握手 + mysql_native_password 认证
    + COM_QUERY 建表/批量 INSERT），复用 H 项协议编解码与 `check_native_password`；
  - 自动 `CREATE TABLE IF NOT EXISTS`（docid BIGINT UNSIGNED 主键 + doc TEXT）；
    批量 `INSERT INTO t (docid, doc) VALUES (...)`（`escape_sql` 转义单引号/反斜杠/换行）；
- **资源控制**：`--rate-limit <rows/s>` 每批按目标速率 sleep 节流；
- **Engine::scan_stream**（&self）：流式主键范围扫描（回调式，`false` 提前终止）——
  导出管道内存 O(批)，不再全量收集；
- 端到端验证（exp-a 5 行库）：CSV 过滤 `amount>=200 AND name CONTAINS 'a'` + 投影 name,amount
  + 脱敏 → carol/dave 两行 `{"name":"***","amount":...}`；Parquet 过滤 amount>=300 → 3 行；
  JDBC 导出 5 行到本机 MySQL（3308）→ 点查数据完整；增量回归 cp 推进正常；
- 全量测试 469 → **476 全绿**（export_pipeline 6 + 相关）。

### 7.67 导出增强：MySQL 兼容 CSV 配套 + ClickHouse/MySQL 建表 DDL（E 模块，design 20.5）

> 2026-09-01。E 模块"导出增强"的 **MySQL 兼容 CSV 配套**与 **建表 DDL** 落地（`313bd81`）。
> 至此 design 20.5 导出功能基本完整：CSV/Parquet/JDBC/增量/流式管道/MySQL 兼容/DDL。

- **`--mysql-compatible`**：CSV 导出后自动生成同名 `.sql` 配套文件（`CREATE TABLE IF NOT EXISTS` +
  `LOAD DATA INFILE`——比逐条 INSERT 快 ~20 倍）；LOAD DATA 的 `FIELDS TERMINATED BY ',' ENCLOSED BY '"'`
  与 RFC 4180 CSV 输出逐字段对齐；
- **`--mysql-max-varchar <n>`**：doc 列 `VARCHAR(n)`（n>0）或 `TEXT`（默认）——处理 MySQL 65KB
  行大小限制，超长字段降级 TEXT；
- **`--dry-run-schema <out.sql> [--target clickhouse|mysql]`**：只生成目标库建表 DDL 不导出数据：
  - ClickHouse：`docid UInt64 + doc String`，`ENGINE = MergeTree ORDER BY docid`（Parquet 导出后
    `INSERT ... SELECT FROM file('*.parquet')` 直读）；
  - MySQL：`docid BIGINT UNSIGNED PRIMARY KEY + doc VARCHAR(n)/TEXT`，`InnoDB utf8mb4`；
- 端到端验证：ClickHouse/MySQL DDL 输出正确；CSV + 配套 SQL（建表 + LOAD DATA）生成正确；
  +4 DDL 单元测试（TEXT/VARCHAR 切换、MergeTree、LOAD DATA FIELDS 对齐、DDL+LOAD 组装）；
- 全量 **476 全绿**。

### 7.68 导出增强：与 Compaction 共享后台 IO 优先级（E 模块，design 20.5 收尾）

> 2026-09-01。E 模块"导出增强"最后一项落地（`40e8abb`）——**design 20.5 导出功能全部完成**：
> CSV/Parquet/JDBC/增量/流式管道/MySQL 兼容/建表 DDL/后台 IO 限速。

- **CF 增 `scan_limiter`**：顺序扫描（scan_stream）路径专用 Token Bucket 限速器——与 Compaction
  的 `io_limiter` **同策略**（共享后台 IO 预算语义，默认低于前台读写）；前台点查（get）不受影响
  （限速只作用于 scan_stream，不作用于点查/范围读）；
- **CF::scan_stream_at 产出按字节 acquire**：扫描节奏受限 → SST 顺序读 IO 随节奏受限
  （后台 IO 让路前台，对在线业务影响 <5% 目标）；
- **export `--io-rate-limit-mb <n>`**：导出限速（默认取 `storage.io_rate_limit_mb`，与 Compaction
  同配置源）；`Engine::set_scan_rate_limit`（0 = 关闭）；
- 测试 `scan_rate_limit_slows_stream`：限速 1MB/s 扫描 2MB 显著变慢（≥300ms）、关闭恢复快速、
  前台读不受影响；全量 **477 全绿**。

### 7.69 高并发查询优化（I 项 P3，同步模型内可落地部分）

> 2026-09-01。design 9.5 目标（10k 连接 / 85 万 QPS 依赖异步协程运行时，`97e3586` 落地**同步模型
> 内可做的高并发优化**）。478 全绿。

- **COM_STMT_EXECUTE 读语句走读锁**：`stmt_execute_sql` 拆分（参数解析 + 占位符替换，与 Engine
  锁解耦）→ 预处理读语句（SELECT/SHOW 等）走 RwLock 读锁——sysbench point_select 等
  PREPARE/EXECUTE 负载多连接**并行**（旧实现全走写锁串行）；写语句保持写锁互斥；
- **连接线程小栈**：`thread::Builder` 512KB + 命名（默认 2MB/8MB）——10k 连接内存 5GB vs 20GB+，
  支撑更多并发连接；
- **MySqlServer::serve 返回实际绑定地址**（`127.0.0.1:0` 随机端口——并发测试/动态端口）；
- 测试 `stmt_execute_concurrent_selects_all_succeed`：8 线程并发 PREPARE/EXECUTE SELECT
  全部成功无死锁（读锁并行正确性）；
- **1 亿库实测**（8 线程 pymysql 并发点查）：1100 QPS 4000/4000 全命中无回归；附带清理
  P73 前旧 release 合并残留的 manifest 缺失引用（99/98/89 数据已并入 sst-100，重建 manifest
  后干净启动）；全量 **478 全绿**。

### 7.70 异步协程运行时（I 项 P3，design 9.5 10k 连接目标）

> 2026-09-01。tokio 异步网络层落地（`2802885`）——连接 idle 不占 OS 线程，10k 长连接可行；
> 查询经 spawn_blocking 复用同步引擎（活跃查询才占阻塞线程）。480 全绿。

- **协议逻辑抽取（同步/异步共用）**：`handle_command`（命令分发无 IO）、`query_response_packets`
  （响应编码）、`build_handshake_packet` / `parse_handshake_response`（握手）、`new_session`；
  同步 `handle_connection` 重构复用（16 mysql 测试回归全过）；
- **异步路径**：`read/write_packet_async`（tokio AsyncRead/Write）+ `handle_connection_async`——
  连接 task 异步读包（idle 不占线程），查询 `spawn_blocking` 执行（引擎 RwLock + session
  take/归还独占）；`MySqlServer::serve_async`（tokio accept 循环 + 每连接 task）；
- **接入**：mysql-server bin `--async` 切换 tokio runtime（默认同步不变）；
- **1 亿库实测**：
  - 并发点查（8 线程 pymysql）：966 QPS 3999/4000 命中（与同步 1100 同量级，pymysql 开销主导）；
  - **idle 连接不占线程（核心验证）**：500 个 idle 连接 → **server 仅 15 线程**（同步模式
    500 连接 = 500+ 线程）；线程数不随连接数线性增长 → 10k 长连接可行；
- 测试 `async_server_protocol_roundtrip`（握手/认证/SELECT/PREPARE/INSERT 往返）+
  `async_server_concurrent_clients_all_succeed`（8 并发客户端全成功）；全量 **480 全绿**。

### 7.71 io_uring Linux 部署实测（V 项收尾：热路径接入 + 阿里云 Debian 12 A/B）

> 2026-09-01。V 项（design_extension v0.5 第 12.3）在 Linux 上完成**部署实测**（7.63 指引执行）。
> 实测发现 7.63 时代 `iou` 池仅初始化、未接入读写热路径 → 本次**补齐热路径接入**后实测。
> 环境：阿里云 Debian 12 / 内核 6.1（io_uring 符号 342）/ 2 核 / 1.6GB。

1. **热路径接入（补 7.63 缺口）**：
   - `SstReader` 增 `iou: Option<Arc<IoUringPool>>` 字段 + `open_with_io_uring` 变体；块读
     （`read_block`/`read_block_group`/`block_raw`）经 `read_at_io` 转发——Linux + 启用时走
     SQPOLL（`IoClass::Sst` 队列），否则回退同步 `read_at`；
   - `WalWriter`/`RingWal` 增 `iou` 字段 + `set_io_uring`；fsync（`sync`/`flush_sync`/
     `truncate_and_reset`/环形合并 fsync）经 `fsync_file` 转发（`IoClass::Wal` 队列）；
   - `ColumnFamily::open_with_io_uring`（Linux）把池注入加载的 SST 与 WAL；engine 将 iou
     池创建**提前到 CF 打开之前**并传入三 CF（primary/cidx/delta）；
   - `io-uring-file` crate 补 `unsafe impl Send`（Arc 跨线程派发需要，白名单内论证）；
   - 启动日志：`io_uring 后端池初始化成功（SQPOLL 三队列，预留核=…）` / 未启用回退提示；
2. **编译验证**：Linux `cargo build --release` 通过（`-C target-cpu=native`；1.6GB 内存需
   `CARGO_BUILD_JOBS=1` 防 OOM）；本地 Windows 480 测试全绿（cfg 门控双分支）；
3. **A/B 实测**（MySQL wire，pymysql 本地回环；memtable 8MB 强制 flush 使读走 SSTable；
   组提交 2ms；WAL append）：

   | 指标（30 万行） | io_uring ON | 同步 OFF | 结论 |
   |---|---|---|---|
   | 写吞吐 rows/s | 17,174 | 19,695 | -13% |
   | 写 P50/P95 ms | 55.1/59.1 | 47.6/55.4 | ON 略慢 |
   | 点查 5k qps | 23 | 23 | 持平 |
   | 点查 P50/P95 ms | 44.0/47.9 | 44.0/44.6 | 持平 |
   | 扫描 30×100k /s | 1.3 | 1.3 | 持平 |
   | 扫描 P50/P95 ms | 747/766 | 751/761 | 持平 |

   → **符合 7.63 预期**：io_uring 池/热路径正确生效（SST 已落盘、点查扫描走 SQPOLL read_at、
   WAL fsync 走 SQPOLL 队列），但 **2 核小机器 SQPOLL 内核轮询线程占用 1/2 核资源，写路径
   反而 -13%**；点查/扫描由块缓存主导、IO 非瓶颈故持平。收益在核多/高 IOPS 场景才显著。
4. **核隔离验证**：`reserve_sqpoll_core` 生效——3 个 `iou-sqp-*` 内核线程（WAL/SST/倒排
   三队列）全部绑核 0，业务主线程在核 1（2 核机器 SQPOLL 独占 1 核，其余业务线程自由调度）；
5. **决策**：io_uring 保持默认关（`io_uring_enabled=false`）——小机器负收益；多核 NVMe
   生产环境按 7.63 指引开启 + 预留 SQPOLL 核。全量 **480 全绿**（本会话无新增测试，接入
   由既有 480 回归覆盖）。

### 7.72 HotCache 内部锁粒度（读写分离收尾，feature I 模块剩余项）

> 2026-09-02。feature.md I 模块"读写分离"行的剩余项：**HotCache 内部 Mutex 粒度**——
> 原实现整包 `Mutex<HotCache>`，点查热路径 `hotcache.lock()` 与写路径 put/invalidate 互斥，
> 多个并发读之间也抢同一把锁（读被写拖垮的残留，O 项第①/②步引擎级 RwLock 之后的最后瓶颈）。

1. **内部粒度化**（`src/hotcache.rs`）：
   - **缓存区**（cache/protected/used_bytes/promotions）改用 `RwLock`：读路径 `peek`（不更新
     LRU 序）持**读锁**——读读完全并行；写路径（put/invalidate/promote/evict）持**写锁**；
   - **访问计数**用 `DashMap`（无锁）——读命中计数不碰 RwLock，热点晋升判定无锁读；
   - `get` 达热点阈值需 promote：先读锁 peek + 无锁计数 → 释放读锁 → 再写锁 promote
     （幂等：pop+put，多线程同时触发无害）；
   - 工程权衡：读命中不刷新 LRU 序（`LruCache::get` 需 `&mut`）→ LRU 淘汰近似化——热度由
     DashMap 计数 + 热点保护区承载，LRU 仅作冷数据兜底序（既有测试全部保持通过）；
2. **engine.rs 去外层锁**：`hotcache` 字段去掉 `Mutex` 包裹，get/batch_get/scan 回填、put 回填、
   invalidate（put_nosync/delete/patch）全部直接 `&self` 调用（读路径不再抢整锁）；
3. **回归**：全量 **482 全绿**（新增 2 并发测试：`concurrent_reads_all_hit_no_data_race` 8 线程
   并发读值一致 + `concurrent_reads_with_write_invalidate_no_stale` 读写并发无脏读）；
4. **A/B 实测**（`src/demo/hotcache-rw`，4 读线程热 key 全命中，512 热 key）：

   | 场景 | OLD（Mutex 整包） | NEW（RwLock+DashMap） | 结论 |
   |---|---|---|---|
   | 纯读 4×200k ops | 806,147 qps | 3,355,579 qps | **x4.16**（读读并行） |
   | 混合负载（写线程节流 ~3 万写/s put+invalidate） | 806,147 qps | 4,369,147 qps | **x5.42**（写不再拖垮读） |

5. **决策**：读读并行收益验证成立（x4.16），混合负载下读吞吐不再被写拖垮（x5.42）——I 模块
   剩余"写路径（txn_commit/compaction）仍串行"属引擎写路径范畴（组提交已解决主瓶颈），维持
   M8-P1 暂缓结论，复制型读写分离留分布式阶段。

## 8. 编码规范

- **注释与文档语言**：中文（与仓库一致），关键算法必须写注释说明「为什么」；
- **命名**：Rust 惯例（snake_case 函数/变量，PascalCase 类型）；
- **错误处理**：内核层自定义 `Error`（thiserror），二进制层聚合（anyhow）；禁止裸 `unwrap`（仅测试允许）；
- **unsafe 红线**：核心存储路径默认零 unsafe；确需使用必须注释安全性论证；
- **格式相关代码**：所有磁盘格式编解码集中在对应模块，改动需版本号；
- **提交规范**：一次提交一个原子变更，message 用中文描述「为什么」；
- **依赖治理**：新增依赖需说明理由；避免引入运行时泛型魔法（保持可读可审查）。

---

## 9. 测试策略（含并发、锁、内存、文件 IO、时间的单元测试设计）

> 单元测试必须像架构一样被设计。LSM-Tree + 异步 IO + 并发缓存这类系统，传统"assert 结果"远远不够，需要"可测试性架构 + 分层测试策略"（对齐 design 第 18 章）。

### 9.0 可测试性基础设施（前置必做）

1. **抽象层定义**（`src/traits.rs`）：
   - `FileSystem` trait（read / write / sync / delete）；
   - `Clock` trait（now / sleep）；
   - `Allocator` trait（allocate / free / current_usage）。
2. **测试辅助模块**（`src/testing/`）：
   - `mock_clock.rs`、`mock_fs.rs`、`mock_allocator.rs`、`loom_wrapper.rs`。
3. **条件编译**：`#[cfg(loom)]` 替换 `std::sync` 为 `loom::sync`（Cargo.toml 配 `[target.'cfg(loom)'.dependencies]`）。

### 9.1 并发与锁测试（必做模块：memtable、hotcache、compaction）

- **确定性模型（loom）**：覆盖 `SkipList::insert` / `remove` 的 ABA 场景，验证无死锁与数据竞争；核心数据结构必须支持 `cfg(loom)` 编译（`loom::sync::Arc` / `loom::cell::UnsafeCell` 替代标准库）；
- **压力模型**：16 线程 × 10k 操作（含同 key 覆盖与读取交叉），最终验证 `size` 计数器与迭代器计数一致、无 panic；
- **DashMap 隔离测试**（外部库无法 loom）：`with_capacity_and_shard_amount` 强制触发 resize，验证无死锁；
- **验收标准**：并发模块在 loom 下 100% 交错覆盖无竞态；criterion 跳表插入吞吐单线程 > 500k QPS（否则设计回退）。

### 9.2 内存测试（必做模块：hotcache、blockcache、oom_guardian）

- **内存上限测试**：注入 `MockAllocator`（仅字节计数、不分配真实内存），验证 `max_bytes` 硬限流触发淘汰且不超限；
- **软水位驱逐测试**：缓存达 85% 软水位触发淘汰、降至 75% 以下；缩容 30%（Cache Shrink）逻辑验证；
- **OOM 水位线测试**：注入模拟 RSS（0.8x / 0.85x / 0.95x / 1.05x），验证软 / 硬限流与 503 返回（不真实吃内存）；
- **紧急止损测试（design 14.1.1）**：模拟 RSS 打满，验证 Immutable 强制刷盘、缓存缩容、jemalloc `purge` 触发后 MockRSS 回落；
- **内存泄漏 CI**：集成 `dhat` heap profiler（或 valgrind），10 万次写入 / 删除循环后 `bytes_allocated - bytes_freed` < 总分配量的 1%。

### 9.3 文件 I/O 与崩溃恢复测试（必做模块：wal、sstable、inverted）

- **抽象文件系统**：单元测试统一用 `InMemoryFileSystem`（零磁盘、极速），集成测试用 `TempFileSystem`（真实路径、自动清理）；
- **崩溃模拟**：在 `drop` 前控制是否调用 `sync_all`，验证 WAL 部分写入恢复的完整性（如写入 100 条仅刷 50 条 → 恢复 50 条）；
- **损坏注入**：`FaultyFileSystem` 在指定写入字节数后返回 `EIO`，验证 SST 生成失败时能回滚并报错、不写半成品；SST 读取必须校验 Footer / Bloom CRC，损坏块返回 `CorruptedFile` 而非 panic；
- **编码与压缩 roundtrip（design 4.4.2）**：Varint / 差值编码 / 块级压缩（zstd/lz4/snappy）解压一致；Block 独立 CRC32 损坏检测；
- **倒排段清单**：模拟 GC rename 前 `kill -9`，重启后验证旧段完整、新段不存在、Manifest 未更新（原子性，design 4.5）。

### 9.4 时间依赖测试（必做模块：ttl、watchdog）

- **MockClock**：所有测试禁止使用 `std::thread::sleep`（除极少数 IO 测试），一律注入 `MockClock.advance()`；
- **TTL 测试**：快进至过期时间后触发 `run_expiry`，验证文件删除与内存清理；
- **看门狗测试**：快进 60s 模拟 Compaction 假死，验证自愈动作触发（中断任务 + 重置调度器）；Sidecar 5s 心跳同理。

### 9.5 单元测试整体设计原则

- **每个模块提供 `debug_assert_invariants()`**（仅测试构建启用），关键操作后检查内部状态（跳表排序、LRU 链表完整性、缓存字节计数）；
- **测试数据生成器**：`proptest` 策略生成随机字段名 / 值 / 大小，覆盖边缘条件（空字符串、超大值、Unicode 边界）；
- **快速反馈**：单元测试套件总耗时 < 60s（CI 门禁）；集成测试单独分组（`#[cfg(not(ci))]`）。

### 9.6 测试金字塔与耗时预算（对齐 design 18.2）

| 层级 | 占比 | 目标 | 耗时预算 |
| --- | --- | --- | --- |
| 单元测试 | 70% | 单函数 / 模块 / 数据结构正确性、边界、错误路径 | < 10ms / 个 |
| 集成测试 | 15% | CRUD 全链路、WAL 回放、备份还原、多线程最终一致性 | < 30s |
| 混沌 / 端到端 | 5% | 崩溃、磁盘满、Compaction 死锁、Leader 切换 | < 5min |

**被测组件速查表：**

| 被测组件 | 测试方法 | 关键工具 / 技术 | 预期耗时 |
| --- | --- | --- | --- |
| 跳表 / SkipList | loom 确定性模型 + 压力并发 | loom + std::thread::Barrier | < 5s |
| HotCache 淘汰 | MockAllocator 计数 | 自定义 Allocator trait | < 100ms |
| OOM Guardian | 注入模拟 RSS（mallctl 模拟） | mockall 或手动 mock | < 10ms |
| WAL 恢复 | tempfile + drop 控制 sync | tempfile + Drop 行为 | < 500ms |
| SST 损坏 | FaultyFileSystem 注入错误 | 自定义 FileSystem trait | < 200ms |
| TTL 过期 | MockClock 快进 | Clock trait + AtomicU64 | < 10ms |
| 倒排段清单原子性 | 模拟 GC 中途 kill（rename 前中断） | 脚本控制 + std::process | < 1s |
| 二次查询 JOIN / Enrich | queryAndJoin 结果正确性（Left/Inner/Right）、`join.max_rows` 熔断、Enrich 失败策略 | SDK 集成测试 + mock 关联源 | < 1s |

**每个性能优化落地后必须压测对比**（design 11.3），禁止凭感觉合并。

---

## 10. 构建与部署

### 10.1 常规构建

```bash
# 开发调试（x86 Linux）
cargo build
cargo build --release

# musl 静态编译（Nova OS 部署）
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl

# 测试
cargo test
cargo test --release
```

### 10.2 生产推荐：cross 交叉编译

> 复杂项目（尤其需交叉编译 aarch64 等架构）强烈推荐 `cross`：通过预配置 Docker 容器提供"零配置"交叉编译，规避 rust:alpine 工具链与 C 依赖的坑。

```bash
cargo install cross --git https://github.com/cross-rs/cross

# 目标平台：x86_64 与 ARM64（Nova OS 边缘设备）
cross build --release --target x86_64-unknown-linux-musl
cross build --release --target aarch64-unknown-linux-musl
```

**关键注意事项：**

- **基础镜像用 `rust:slim`，不用 `rust:alpine`**：alpine 镜像工具链可能触发依赖过程宏（serde_derive）构建失败；slim 手动 `rustup target add x86_64-unknown-linux-musl` 更稳；
- **链接控制**：MUSL 默认静态链接；特殊情况用 `RUSTFLAGS="-Ctarget-feature=+crt-static"`（静态）/ `-crt-static`（动态）；
- **原生 C 依赖**：优先纯 Rust 替代（如 rustls 替代 openssl）；必须用 C 库时使用 `jenskeiner/muslrust` 镜像或 `cross`；
- **release 优化**（见 [Cargo.toml](./Cargo.toml)）：`lto = "thin"` 可升 "fat"、`codegen-units = 1`、`strip = "symbols"` 进一步减体积。

### 10.3 Docker 化（多阶段构建，scratch 终极轻量）

静态二进制可运行在 `scratch`（空镜像）上，生产镜像仅几 MB。Dockerfile 见 [Dockerfile](./Dockerfile)。

```dockerfile
# 阶段 1: Builder（rust:slim 而非 alpine）
FROM rust:1.98-slim AS builder
RUN rustup target add x86_64-unknown-linux-musl
WORKDIR /app
# 依赖层缓存：Cargo.toml/Cargo.lock 不变则不重编依赖
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl && \
    rm -rf src
# 复制真实源码并构建
COPY src ./src
RUN touch src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl

# 阶段 2: Runtime（scratch 空镜像）
FROM scratch
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/shanshui-cunji /shanshui-cunji
COPY config.toml /config.toml
EXPOSE 8080
ENTRYPOINT ["/shanshui-cunji", "--config", "/config.toml"]
```

**多阶段构建要点**：最终镜像只含一个静态二进制（无编译器/源码）；依赖层独立缓存加速 CI/CD；scratch 无 shell 无外部库，安全且极小。

- **与 Nova OS 协同**：开发期跑 x86 Linux；MVP 完成后交叉编译 musl 部署联调；
- Nova OS 只补必备基础组件（SSH / 进程守护 / OTA / NTP），人力重心迁移到 shanshui-cunji（design 10）。

---

## 11. 里程碑验收标准（修正）

| 里程碑 | 验收标准 |
| --- | --- |
| M1（阶段 1 第 2 周末） | 主数据 CRUD 打通：写入 → 重启恢复 → 查询，WAL 崩溃安全验证通过；字段注册表持久化验证通过 |
| M2（阶段 1 第 3 周末） | HTTP/TCP/CLI 跑通全链路；组合索引 + 倒排查询可用；倒排段清单崩溃恢复验证通过 |
| M3（阶段 1 末） | 备份还原（含倒排文件）、Zone Map、缓存、优化器 Lite、看门狗 MVP 全部完成；单测 + 压测报告；musl 产物可运行 |
| M4（阶段 1.5 末） | PAX 列式数据块、TTL 时间分区、Delta CF、倒排统计载荷（COUNT/GROUP BY）、FST + Mmap 字典、迁移工具基础版全部可用 |
| M5（阶段 2 末） | 倒排架构升级 + 分布式配置生效；网关路由 / 广播检索 / 主从切换联调通过；网关 Term 缓存生效；TDS 就绪；写停滞自愈 + Sidecar 探针（无死锁）就绪；混沌压测通过 |
| M6（阶段 3 末） | io_uring 写入模式、IO 调度器、热加载落地；迁移工具高级版可用；对照 design 9.5 性能目标给出实测 |

### 11.1 版本与合入 master 策略（分支模型）

> 工作分支模型：**日常在 `develop` 分支开发，里程碑验收通过后合入 `master` 并打 Git Tag**。master 始终是"最新稳定代码"，用户可切换 Tag 复现问题。

| 里程碑 | 版本号 | 合入 master 时机 | 包含的核心功能 |
| --- | --- | --- | --- |
| MVP 完成（M3） | v0.1.0（α 预览版） | ✅ 首次合入 | 单机 LSM + CRUD + 倒排基础 + HTTP/CLI + 备份还原 + 看门狗子集 |
| MVP 稳定版（M3 后 1~2 周） | v0.1.1（β 测试版） | ✅ 合入 | 修复 MVP 发现的严重 Bug，补充 config.toml 示例和文档 |
| 列式优化完成（M4） | v0.2.0（正式版） | ✅ 合入 | PAX 列组 + TTL 时间分区 + Delta CF + 倒排统计载荷 + FST + 迁移工具基础版 |
| 分布式完成（M5） | v1.0.0（GA 版） | ✅ 合入 | 集群 + 分片 + 主从 + 网关 + 全局 Term 缓存 |

**合入操作约定：**

- 合入 master 的同时必须打 Tag（如 `v0.1.0`），Tag 命名 = 版本号；
- 提交信息建议语义化：`feat:` / `fix:` / `docs:` / `perf:`（Git 规范，见第 8 章）；
- **默认在 develop 分支持续迭代，不直接提交 master**；到达上述里程碑（M3/M4/M5）时由负责人确认后执行 merge + tag。

---

## 12. 风险与开发红线

1. **不把核心存储引擎代码全盘交给 AI**：并发、锁、内存、文件 IO 必须人工逐行审查（design 11.1）；
2. **1 个月产出是演示原型**，商用还需 2~6 个月稳定性打磨（design 11.2）；
3. **先正确后性能**：任何优化必须附压测对比数据；
4. **格式兼容是红线**：SST / WAL / 字段注册表 / 倒排段清单 / 备份包一旦上线，破坏性变更必须有迁移路径；
5. **写路径是心脏**：WAL 刷盘、双 MemTable 切换、写 Stall 的改动必须过崩溃恢复测试；
6. **Sidecar 安全性红线**：严禁在 tokio 运行时内直接 `fork()`，必须使用 `std::process::Command` 或预启动独立进程。
