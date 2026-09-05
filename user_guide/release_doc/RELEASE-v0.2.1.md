# 山水存迹数据库 v0.2.1 发布说明

> 发布日期：2026-08-28 · Git tag：`v0.2.1`（f8c3615）
> 对比基线：v0.2.0（0b43033）· 4 分支同步（develop / master / feature / release）

## 一、本版本亮点

1. **全局分配器加固（design 14.0）**：默认启用 **mimalloc**，消除 musl 默认 malloc 全进程单锁瓶颈；高并发小块分配负载（数据库典型形态）实测 **musl 下 4~10 倍吞吐提升**；
2. **SST v5：块级压缩 + 分区布隆（design 4.4.2）**：每块独立布隆过滤器，查询定位块后只校验目标块（按需反序列化，内存减半）；
3. **数据关联基础（design 19）**：SDK `query_and_join`（Inner/Left/Right + `max_rows` 熔断）+ HTTP `/join` + 写入侧 Enrich（`put_with_enrich`）；
4. **运维管理 + 数据管道（design 20）**：`admin status / processlist / kill`、`explain` 执行计划推演、`shanshui-cunji-export / import`（CSV/JSONL，复用迁移内核）；
5. **分配器压测验证工具**：新增 `shanshui-cunji-bench`，修复 bench 未链接 lib 导致 mimalloc 失效的压测失真问题。

## 二、功能与变更明细

| 类别 | 内容 | 提交 |
|---|---|---|
| 数据关联 | `sdk::join`：`query_and_join` Inner/Left/Right + 熔断；HTTP `POST /join`；`put_with_enrich`（reject/degrade + local 源）；`[join]` / `[enrich]` 配置 | 59dd094 |
| 存储格式 | SST v4→**v5**：PAX 数据块内每块独立布隆（`sstable.bloom_fpr` 可配）；Reader 兼容 v3/v4 整文件布隆回退 | e1eebce |
| 分配器 | `#[global_allocator]` 默认 **mimalloc**；feature `alloc-jemalloc` 可选 tikv-jemallocator（mallctl purge + stats）；`--no-default-features` 用系统分配器（对比基线）；`admin status` 增加 system 分支 | b0eaa58 / f8c3615 |
| 运维 | `admin::status`（分配器 / SST / 倒排 / 内存水位，CLI + HTTP `/admin/status`）；QueryRegistry（`processlist` + KILL 标记）；`explain` 复用 optimizer 只推演不读数据 | e96c7c9 |
| 数据管道 | `shanshui-cunji-export`（CSV）/ `shanshui-cunji-import`（CSV/JSONL），自动分配 docid 避让冲突 | e96c7c9 |
| 压测 | `shanshui-cunji-bench`：混合小块分配负载（JSON 序列化/Vec/String/HashMap/Box），1/2/4 线程可配 | f8c3615 |

## 三、性能实测（2026-08-28）

### 3.1 分配器高并发压测（阿里云 Debian12，2 核 / 1.6GB）

| 组合 | 1 线程 QPS | 2 线程 QPS | 4 线程 QPS | mimalloc 加速比 |
|---|---|---|---|---|
| glibc-system | 250,888 | 271,070 | 270,168 | — |
| glibc-mimalloc | 338,170 | 349,044 | 349,914 | ×1.30 |
| musl-system | 52,651 | 69,455 | **30,136**（4T 全局锁反降 57%）| — |
| **musl-mimalloc** | 247,671 | 291,512 | **298,501** | **×4.70 / ×4.20 / ×9.90** |

结论：musl 默认 malloc 为全进程单锁，4 线程吞吐暴跌；mimalloc 下 musl 基本追平 glibc。证据：`images/allocator-bench/`。

### 3.2 功能性能测试（1000万 / 2000万 / 5000万条，跳过 1 亿）

| 规模 | 批量插入 | 插入速率 | 倒排词条检索 | 分片路由 | 备份·还原 | 结果 |
|---|---|---|---|---|---|---|
| 1000万 | 27.5s | 36.4 万条/s | 2.0s（250万命中）| 42.3s | 26.6s | 10/10 |
| 2000万 | 63.2s | 31.7 万条/s | 2.5s（500万命中）| 107.7s | 51.0s | 10/10 |
| 5000万 | 148.8s | 33.6 万条/s | 2.3s（1250万命中）| 313.7s | 222.0s | 10/10 |

对比 v0.1.0 同规模：**插入 +70%**、倒排检索 **-81%**（分区布隆 + FST 收益）。证据：`images/perf-0.2.1/`。

## 四、质量数据

- **163 个单元测试全绿**（`cargo test`），demo 冒烟 10/10 全通过；
- 项目自身 **unsafe = 0**（cargo-geiger 0/958 函数），`#![forbid(unsafe_code)]` 编译期强制；mimalloc 的 unsafe 实现在第三方 crate 内部；
- `cargo audit` / `cargo deny` 通过；问题与修改记录见 `problem_solving.md`（P25~P29）。

## 五、构建与使用

```bash
# 默认（mimalloc，推荐）
cargo build --release

# jemalloc（mallctl purge + stats，Linux/musl 推荐）
cargo build --release --features alloc-jemalloc --no-default-features

# 系统分配器（压测对比基线）
cargo build --release --no-default-features

# 分配器高并发压测
cargo run --release --bin shanshui-cunji-bench -- --threads 4 --ops 400000
```

新增 CLI 一览：`admin status` / `admin processlist` / `admin kill <id>` / `explain` / `shanshui-cunji-export` / `shanshui-cunji-import` / `shanshui-cunji-bench`。

## 六、兼容性与已知限制

- **SST 格式**：新写入为 v5（分区布隆），Reader 自动回退兼容 v3/v4，旧库可直接打开；
- **倒排段**：v2 字段维度编码（`field=value`）由 v0.2.0 引入，v0.2.1 保持兼容；
- **KILL 中断**：目前为标记式（配合看门狗超时生效），真正中断执行线程留待阶段 2 CancellationToken；
- **分配器**：mimalloc 在 musl/glibc/Windows 均已验证；jemalloc 未在 Windows 验证（Linux/musl 推荐）。
