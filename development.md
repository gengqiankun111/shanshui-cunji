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
| jemalloc（tikv-jemallocator） | 内存分配器统计（OOM 看门狗水位线） | 可选，比 OS RSS 精准 |
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
6. **FST + Mmap 字典**（与 Checkpoint 共存，无缝过渡）；
7. **迁移工具基础版（shanshui-cunji-migrate）**（解决 G2）：支持 mysqldump / CSV 全量导入；
8. **数据关联基础（design 19）**：SDK `queryAndJoin` 二次查询 + 写入 Enrich 预连接；
9. **块级压缩（Zstd Level 3）+ 分区布隆过滤器（design 4.4.2）**：存储再降 40%~60%、布隆内存减半；
10. **运维管理（design 20）**：`admin processlist`（QueryRegistry + KILL QUERY）+ `admin status`（jemalloc stats + 命中率）+ `explain`（执行计划推演）；
11. **数据管道（design 20）**：`shanshui-cunji-export`（Parquet/CSV 基础版，与迁移工具同期）+ `shanshui-cunji-import`（CSV/JSON 基础版）。

### 7.3 阶段 2：分布式集群

1. **倒排架构升级**（预分片 Chunk + 分层 GC + 极端防护）；
2. 分布式全配置清单（design 9.8）；
3. 分片节点 RPC → 网关 + 元数据中心 → 广播检索 / 虚拟分片扩容 / 主从高可用；
4. **看门狗补全**：写停滞假死检测自愈 + Sidecar 探针（`std::process::Command` 独立进程）；
5. **网关全局 Term 缓存** + 失效心跳；
6. **术语字典热备 TDS**；
7. **无损扩容协议（design 9.1.1）**：双写（Shadow Writes）→ 数据追平（Delta Catch-up，SST 拷贝 + WAL 增量回放）→ 原子切换（Atomic Switch，1s 内）+ 5s 回滚预案，业务零感知、数据零丢失；
8. **数据关联增强（design 19）**：物化视图调度器 + `shanshui-cunji-export` 导出工具 + JOIN 计划节点本地执行（单分片内）；
9. **两级索引（design 4.4.2）**：内存索引减少 90%；
10. **数据管道增强（design 20）**：`import` 支持 Parquet、`import-schema`（字段注册表 + 索引定义导入）；
11. **Redis 外部缓存（design 21）**：External Cache Manager（Cache-Aside + Write-Invalidate + 熔断降级），`[cache.external]` 默认关闭，详见 redis-integration-guide.md。

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

---

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
