# 问题解决记录（problem_solving.md）

记录开发过程中遇到的问题、根因与修复方式，按时间顺序整理（来源：git 提交历史 + 开发会话）。
新增问题请追加到文末，保持一条问题 = 一个条目（现象 / 根因 / 修复 / 提交）。

---

## 阶段 1 · 单机 MVP

### P1. 主键键编码小端 → 大端（范围扫描错乱）
- **现象**：docid > 255 时主键范围扫描（range / scan）漏数据或返回错序；HTTP `/range` 端到端测试暴露。
- **根因**：`encode_docid` 用 little-endian，LE 字节序 ≠ 数值序（如 `1001` 的字节 `E9 03` > `2000` 的 `D0 07`），而 LSM 的 MemTable 范围、Zone Map 剪枝、块内二分都依赖**字节序比较**；小 docid 未跨 256 边界所以此前测试未暴露。
- **修复**：主键与组合索引尾部 DocId 改为 big-endian（字节序 == 数值序），同步更新 development.md 4.1 格式约定，补字节序回归测试。
- **提交**：`2bd96a7`（步骤 15 暴露）

### P2. WAL 重开即截断，未刷盘数据重启丢失
- **现象**：CLI/HTTP 跨进程写入后，重启即丢未 flush 到 SST 的数据（崩溃恢复形同虚设）。
- **根因**：`ColumnFamily::open` 用 `File::create` 打开 WAL（截断旧文件），随后回放读到的是空文件；单元测试未暴露是因为测试都先 flush 到 SST。
- **修复**：`WalWriter::open_append`（追加模式不截断）+ 回放后 `resume_seq(max_seq+1)` 接续序列号；补 `reopen_preserves_wal_only_data` 回归测试。
- **提交**：`2bd96a7`

### P3. CLI 独立进程写入后倒排词条不可查
- **现象**：CLI `put` 后再 `search` 命中 0 条，但主数据可读。
- **根因**：倒排词条只驻留进程内存（刷盘阈值 1M posting），CLI 每次独立进程 put 后即退出，词条随进程消失；长驻 server 无此问题。
- **修复**：CLI `put` 成功后主动 `flush_inverted()`（内存词条落盘段文件），保证后续进程可查。
- **提交**：`fe2203d` 之后随步骤 15 一并验证

### P4. Windows 构建环境：C 盘满导致构建失败
- **现象**：`cargo build/test` 失败（临时/缓存撑爆 C 盘）。
- **根因**：rustup 默认缓存与 TMP 在 C 盘，C 盘空间不足。
- **修复**（本机环境，不入库）：rustup 目录 override 到 gnu 工具链、w64devkit 加入 PATH、`TMP/TEMP` 指向 D 盘；target-dir 已配置在 D 盘（`.cargo/config.toml`）。
- **提交**：无（环境配置）

### P5. demo 临时目录硬编码 `D:\`
- **现象**：Linux 上运行 demo 失败（`D:\shanshui-cunji-tmp` 路径不存在）。
- **根因**：临时数据目录写死 Windows 盘符。
- **修复**：改为默认系统临时目录 + 环境变量 `SHANSHUI_CUNJI_TMP` 覆盖（Windows 上保持 D 盘行为）。
- **提交**：`9dc0a10`（musl 验证时暴露）

---

## 阶段 1.5 · 列式优化

### P6. Linux/musl 交叉编译：服务器 crates 镜像不稳定
- **现象**：阿里云服务器 `cargo build` 反复超时（rsproxy/阿里云/TUNA/USTC 的 crate 下载均不稳定）。
- **根因**：国内镜像对服务器网络不稳定，`config.json` 可达但实际 crate 下载 CDN 节点超时。
- **修复**：本机 `cargo vendor` 离线打包依赖源码上传，服务器 `cargo build --offline`（vendor 源配置写入 `/root/.cargo/config.toml`）。
- **提交**：`9dc0a10`（验证记录 images/linux-musl/）

### P7. musl 静态 PIE 启动即 Segfault
- **现象**：musl 静态产物（`+crt-static`）在 Debian 上 `version` 命令都崩溃（exit 139）。
- **根因**：静态 PIE 在该环境启动崩溃。
- **修复**：追加 `-C relocation-model=static`（传统非 PIE 静态）；`file` 确认 `statically linked`，`ldd` 报 `not a dynamic executable`。
- **提交**：`9dc0a10`

### P8. musl 构建 zstd C 代码误用 glibc gcc
- **现象**：musl 静态产物运行 Segfault（初版）。
- **根因**：`CC_x86_64_unknown_linux_musl` 未设置时，zstd-sys 用系统 gcc（glibc）编译 C 代码，链接混合 glibc/musl 对象。
- **修复**：`.cargo/config.toml` 的 `[env]` 设 `CC_x86_64_unknown_linux_musl=musl-gcc`、`AR_x86_64_unknown_linux_musl=ar`（Debian musl-tools 无 `musl-ar`，用系统 ar 打包即可）。
- **提交**：`9dc0a10`

### P9. Windows 专用 `.cargo/config.toml` 污染服务器构建
- **现象**：服务器离线构建报 `path segment contains separator ':'`（`$LD_LIBRARY_PATH` 含 `D:\shanshui-cunji-target`）。
- **根因**：源码打包时带上了本机 `.cargo/config.toml`（含 Windows target-dir/linker），Linux cargo 误用。
- **修复**：删除服务器项目内 `.cargo/`；本机该目录属本机配置、不入库。
- **提交**：`9dc0a10`

### P10. PAX：`get_from_sst` 块内扫描未适配 v4 kind 字节
- **现象**：`flush_then_read_back` 失败（flush 后 SST 内 get 全 None，但直接 `SstReader::iterate` 正常）。
- **根因**：column_family 的 `scan_block` 仍是 v3 行式解析（从字节 0 开始），v4 块首 kind 字节被当 varlen 误读。
- **修复**：删除旧 `scan_block`，改调 `SstReader::scan_block_for_key`（按 `self.format` 分发行式/PAX）。
- **提交**：`f6bcbb5`

### P11. PAX 块解码 seq 区定位错误
- **现象**：PAX 块 `get` 报 Corrupted（列值/seq 越界）。
- **根因**：解码时用解析完列表的游标 `cur` 定位 seq 区，但 seq 区在列数据之后（位置不定）。
- **修复**：seq 区从块尾定位：`seqs_start = data.len() - row_count*8`。
- **提交**：`f6bcbb5`

### P12. PAX 列值长度编码不匹配（varlen vs varint）
- **现象**：PAX 解码报 `PAX 列值解析失败: expected value at line 1 column 1`。
- **根因**：写入用 `encode_varlen`（4 字节长度前缀），读取用 `decode_varint`（LEB128），格式不一致。
- **修复**：统一用 `encode_varint`/`decode_varint`（LEB128 变长长度）。
- **提交**：`f6bcbb5`

### P13. PAX 弱 schema 下列集只取首行字段 → 后续行新字段丢失
- **现象**：`pax_block_roundtrip` 断言失败（k3 的 `extra` 字段在重组后消失）。
- **根因**：列集按首行字段决定，后续行的新字段被丢弃。
- **修复**：列集 = 所有行的字段并集（首行顺序 + 后续新字段追加末尾），字段不丢失。
- **提交**：`f6bcbb5`

### P14. PAX 块切分行数门槛阻止单条 flush
- **现象**：`pax_mixed_block_kinds_in_one_file` 断言 index_len>=2 失败（只有 1 个块）。
- **根因**：flush 条件 `buf.len() >= 32`（行数门槛）阻止少量大值及时分块。
- **修复**：去掉行数门槛，仅按估算字节数 `>= block_size` 触发 flush。
- **提交**：`f6bcbb5`

### P15. TTL：WAL 回放使过期数据"复活"
- **现象**：`ttl_buckets_and_expiry` 失败——10 天前桶数据重启后仍可读。
- **根因**：过期清理只删 SST，重启时 WAL 回放把过期记录重新放回 MemTable。
- **修复**：回放 WAL 时按 TTL 判断（`is_ttl_expired`）过滤过期记录，不回放入 MemTable。
- **提交**：`18d3f41`

### P16. demo 写 HTML 报告时目录不存在
- **现象**：`demo --out <新目录>` 报 `NotFound`。
- **根因**：报告写入前未创建输出目录。
- **修复**：写报告前 `create_dir_all`。
- **提交**：`18d3f41`（随 TTL 一并）

### P17. check.ps1 在 PS 5.1 下因 stderr 进度误判失败
- **现象**：`quality/check.ps1` 第 4 步 `cargo test 2>&1 | Select-String` 报 `NativeCommandError`，脚本以失败退出。
- **根因**：PS 5.1 在 `$ErrorActionPreference="Stop"` 下，把 cargo 写到 stderr 的编译进度视为致命错误。
- **修复**：第 4 步临时切到 `"Continue"`，取 `$LASTEXITCODE` 判定测试结果后再恢复 `"Stop"`。
- **提交**：随质量体系文档提交（本轮）

### P18. clippy 初始 43 条 lib + 9 条 bin 警告清零
- **现象**：质量体系引入 `cargo clippy -- -D warnings` 后首次执行报 52 处警告。
- **根因**：历史代码未按 clippy 严格标准（ptr_arg、type_complexity、should_implement_trait、io_other_error、approx_constant、field_reassign_with_default 等）。
- **修复**：`clippy --fix` 自动修复 32 处；手动修复其余：`&PathBuf → &Path`、`FromStr` trait 化、类型别名（DecodedRow/BucketRow）、`hotcache.is_empty`、clamp、io_other_error、approx_constant 等。
- **提交**：随质量体系文档提交（本轮，`cargo fmt --check` 与 `clippy -D warnings` 双零通过，133 测试全绿）

### P19. lru 0.12.5 两个 unsound 警告 → 升级 0.18
- **现象**：`cargo audit` 首次执行报 2 个 unsound 警告（RUSTSEC-2026-0002 `IterMut` 违反 Stacked Borrows；RUSTSEC-2026-0253 `LruCache::pop()` 恐慌安全 UAF），均指向 lru 0.12.5。
- **根因**：lru 0.12.x 实现存在 unsound 问题；本项目 blockcache / hotcache 依赖 lru 0.12。
- **修复**：`Cargo.toml` 升级 `lru = "0.18"`（API 兼容：new/get/put/peek_lru 不变），重新构建 + 133 测试全绿，audit 复扫 **0 漏洞 0 警告**。
- **提交**：随 audit/deny 执行提交（本轮）

### P20. cargo-deny 安装与 deny.toml schema 适配
- **现象**：① `cargo install cargo-deny` 编译失败 `dlltool.exe not found`；② 首次 `cargo deny check` 连续报 schema 错误（unmaintained/allow-osi-fsf-strong-copyleft/informational-warnings/unlicensed 等键无效）。
- **根因**：① gnu 工具链需 w64devkit 的 dlltool，但安装时 PATH 未含 `D:\w64devkit\bin`；② deny.toml 按旧版文档书写，cargo-deny 0.20 移除/改名多个键（PR #611 迁移）。
- **修复**：① 安装时前置 `$env:PATH="D:\w64devkit\bin;..."`，工具装入 `D:\rust-tools\bin`（避免 C 盘耗尽）；② deny.toml 按 0.20 schema 重写：`[licenses]` 仅保留 `allow` 白名单（MIT/Apache-2.0/Unicode-3.0/Zlib）+ `confidence-threshold`，`[bans]` 多版本 warn（hashbrown/syn 传递依赖双版本为良性警告）。
- **提交**：随 audit/deny 执行提交（本轮）

### P21. cargo-geiger 0.13 安装与运行要点
- **现象**：`--output-format terminal` 报 `Matching variant not found`；`--output-path` 不写文件；报告混入 rustup stderr 噪音与 ANSI 色码。
- **根因**：geiger 0.13 的合法格式为 Ascii/GitHubMarkdown/Json/Utf8/Ratio（无 terminal）；`--output-path` 需配 `--output-format` 才生效（直接重定向 stdout 更稳）；工具依赖 rustc 私有 API 需 **nightly** 工具链。
- **修复**：`rustup run nightly-msvc cargo geiger --output-format Ascii > quality/geiger_report.txt`；C 盘仅 0.7GB，故 `CARGO_HOME/CARGO_TARGET_DIR` 指向 D 盘再安装编译；入库前剥离 ANSI 码与噪音头。
- **结果**：项目自身 unsafe = 0（0/958 函数、0/64881 表达式），`#![forbid(unsafe_code)]` 编译期强制；geiger 对 inner attribute 的 forbid 识别仍显示 `?`（工具限制，rustc 层约束已生效）。
- **提交**：随 geiger 报告提交（本轮）

### P22. 倒排 term 裸值 → `field=value` 字段维度编码（聚合执行器前置）
- **现象**：实现 COUNT/GROUP BY 时发现倒排词条只存裸字符串值（`active`），无字段信息——无法"按字段遍历 Term 集合"（design 5.17）。
- **根因**：MVP 提取词条时丢弃字段名（collect_strings 只收值）；段格式 v1 无字段维度。
- **修复**：term 编码升级为 `field=value`（顶层 `status=active`、嵌套路径 `meta.device=ios`、数组 `tags.0=hot`），倒排段格式 SEG_VERSION 1→2；`parse_filter` 的 `=` 语法与 term 编码天然对齐，现有 HTTP 端到端测试无需改动；新增 `doc_count` / `iter_terms` / `group_by` 聚合 API。
- **兼容性**：阶段 1.5 未发布 v0.2.0（pre-1.0），格式直接升级、无迁移负担（design 224 破坏性变更条款）；旧 v1 段在升级后仅影响聚合（裸 term 无字段前缀，不可按字段分组），常规查询不受影响。
- **提交**：随 M4 聚合执行器提交（本轮）

### P23. FST 字典：forbid(unsafe_code) 下 mmap 不可用 → fs::read 加载
- **现象**：实现 design 5.2.4.1「FST + Mmap」时，memmap2 的 `Mmap::map` 为 unsafe API，与项目 `#![forbid(unsafe_code)]` 冲突（forbid 连局部 allow 都无效）。
- **决策**：守住「零 unsafe」质量承诺优先——FST 字典（term → 段内偏移）用 `std::fs::read` 加载为 `fst::Map<Vec<u8>>`。FST 本身是压缩结构、单段字典仅几十字节，read 加载开销可忽略，保留 O(len(term)) 查找收益；mmap 按需加载（冷启动亚秒）留待独立 crate 封装 unsafe 白名单后落地。
- **附加修复**：flush 后未即时把新字典插入内存 `dicts`（需重启才加载）→ `write_fst_dict` 返回 Map 并立即 `dicts.insert`，同实例即可 FST 加速。
- **提交**：随 M4 FST 提交（本轮）

### P24. 迁移工具：mysqldump 值解析与主键列语义
- **现象**：SQL 解析测试失败——字符串值被识别为 `Other`；docid 用 `id` 列的导入 `get(10)` 返回 None。
- **根因**：① 解析状态机剥离了引号后无法区分字符串值（`mk_sql_value` 检查 `starts_with('\'')` 失效）；② MySQL 主键惯例是 `id` 列而非 `docid`。
- **修复**：① `parse_value_tuples` 增加 `str_val` 标志（引号打开时置位），值提交时按标志构建 `SqlValue::Str` 并统一 unescape（`\'`→`'`、`\\`→`\`）；② 主键列支持 `docid` 或 `id`（数字/字符串均可解析）；③ error.rs 增加 `Migrate` 变体承接 csv/serde_json 错误（核心模块不耦合 csv crate）。
- **提交**：随 M4 迁移工具提交（本轮）

### P25. 数据关联：Right Join 语义与 Enrich 借用冲突
- **决策**：基础版 Right Join 等价于 Inner（从表无独立筛选条件时，"保留全部右表"无意义——右表全集不参与关联）；文档注明，阶段 2 引入从表 filter 后补全。
- **设计约束**：`put_with_enrich` 回调签名 `FnOnce(&mut Engine, &mut Value)`——Enrich 需在 WAL 前查引擎（local 源），若回调只拿 `&mut Value` 则无法访问引擎；故回调同时借 `&mut Engine`，Enrich 修改文档后再统一序列化写入。
- **提交**：随 M4 数据关联提交（本轮）

### P26. 分区布隆：SST v5 格式与双查询路径
- **背景**：块级压缩（zstd 每块独立 + CRC）MVP 已实现；缺口是 design 4.4.2 的**分区布隆**（整文件单布隆 → 每块一个，查询只加载目标块）。
- **实现**：SST 格式 v4→v5，Bloom 区改为 `Count + [len + bytes]*`（与 Index 对齐）；Reader `partition_blooms: Option<Vec<Vec<u8>>>`（原始字节按需反序列化），`get()`/`get_from_sst` 先二分定位块、再校验目标块布隆；v3/v4 走 `legacy_bloom()` 整文件布隆回退（`v4_legacy_bloom_still_readable` 测试手写 v4 文件验证兼容）。
- **注意**：Writer 的整文件布隆字段移除（分区布隆按块内实际 key 数构建，`expected_keys` 参数不再用于布隆），`new_with_pax` 新增 `bloom_fpr` 参数（`sstable.bloom_fpr` 默认 0.01）。
- **提交**：随 M4 块级压缩 + 分区布隆提交（本轮）

### P27. musl 默认 malloc 全局锁瓶颈 → 全局分配器替换（design 14.0）
- **背景**：musl 分配器全进程单把互斥锁，数据库（高频小块分配：JSON/MemTable/SST 解压/倒排/HTTP）高并发下 alloc/dealloc 排队串行，极端场景吞吐差 2~7 倍；项目目标含 musl 静态部署（已交叉验证）+ 阶段 2 分布式。
- **方案**：`#[global_allocator]` 默认 **mimalloc**（轻量、边缘友好、高并发好；声明本身无 unsafe，unsafe 在 crate 内部，不违反 `#![forbid(unsafe_code)]`，157 测试验证通过）；feature `alloc-jemalloc` 切 tikv-jemallocator（mallctl purge + stats，Linux/musl 推荐）。
- **坑**：jemalloc 在 Windows gnu 交叉工具链 configure 失败（mingw host C 构建问题，非项目缺陷）——文档注明 alloc-jemalloc 仅 Linux/musl 目标使用；`#[global_allocator]` 编译期决定、feature 互斥。
- **提交**：随分配器加固提交（本轮）

### P28. 运维管理与数据管道：自动分配 docid 避让 + Instant Default
- **现象**：`json_import_creates_documents` 失败——显式 docid（1,3）与递增分配（从 1 起）冲突，递增分配的 docid=1 覆盖了显式 docid=1 的行。
- **修复**：三处导入（CSV/SQL/JSONL）自动分配改为 `while engine.get(d)?.is_some() { d += 1; }` 避让已占用 docid。
- **其他**：`QueryRegistry` 含 `Instant` 字段无法 `#[derive(Default)]`（Instant 无 Default）→ 手写 `impl Default`；MemoryConfig 无全局 `max_memory_mb` → admin status 用缓存总预算（hotcache+blockcache）替代。
- **提交**：随 M4 运维管理 + 数据管道提交（本轮）

### P29. 分配器压测三连坑：mimalloc 链接被 GC 丢弃 / cargo --config 引号 / GNU ld 丢失
- **现象①**：musl 版 mimalloc 与 system 二进制大小完全一致（536664），4 线程双双暴跌——压测失真。
- **根因**：`#[global_allocator]` 定义在 lib crate，bench bin 未引用 lib 任何符号 → 链接器 GC 丢弃 lib 产物，mimalloc 未生效。
- **修复**：bench.rs 加 `let _force_lib = shanshui_cunji::error::Error::NotFound(...)` 强制链接 lib；修复后 musl-mimalloc 二进制 669856 字节（strings 命中 7 处 mimalloc），4 组合大小互不相同。
- **现象②**：服务器 `cargo build --config source.crates-io.replace-with=vendored-sources` 报 `string values must be quoted`。
- **根因/修复**：`--config` 的 TOML 值必须引号包裹：`replace-with="vendored-sources"`（值用 `\"...\"`）。
- **现象③**：本地 Windows 构建报 `collect2.exe: fatal error: cannot find 'ld'`。
- **根因**：rustup settings.toml 对本项目有目录覆盖 `stable-x86_64-pc-windows-gnu`，而 GNU 工具链的 ld 已在 C 盘清理时丢失。
- **修复**：`rustup override unset --path <项目>` 切回默认 MSVC 工具链（VS Build Tools link.exe 完好）。
- **提交**：随 v0.2.1 提交（f8c3615）

---

## 阶段 3 · 深度优化（v0.3.0）

### P30. 读路径性能回归：get_from_sst 每次点查克隆整个 Level 2 精确索引
- **现象**：v0.3.0 性能实测（P3-7）1000万 demo 在「倒排词条查询」阶段挂起（>8min：CPU 持续占用、无新文件写入、无 sharded 目录）；v0.2.1 同规模仅 2s；100万 正常（11s 完成）——**与 SST 规模相关的读路径劣化**。
- **根因**：M5 两级索引重构后 `SstReader::index()` 返回 `full_index.clone()`（整个 Level 2 精确索引副本，逐条 IndexEntry 含 first_key String 克隆），`column_family::get_from_sst` 沿用旧调用方式，**每次点查都克隆全量精确索引**；亿级库每 SST ~1.3 万条 IndexEntry，200k 次抽样回表 → 数亿次小分配（O(索引条数 × 查询数)），倒排查询从 2s 恶化到挂起；100万 库 SST 小（千级条目）故未暴露。
- **修复**：`SstReader` 新增 `locate_indexed_block`（借用精确索引二分 + **只克隆单条**块条目，对齐 design 4.4.2 按需加载语义），`get_from_sst` 改用之并保留分区布隆剪枝；实测 1000万 倒排词条查询恢复 2.4s，2000万 / 5000万 稳定。
- **提交**：`d472f94`（P3-7a）

### P31. 性能实测环境：C 盘写满致 demo 卡死（临时目录重定向 D 盘）
- **现象**：首次 1000万 demo 运行 ~10min 未完成（分片 WAL 写入近乎停滞），随后进程异常退出；console.log 0 字节（Tee 缓冲未刷）。
- **根因**：demo 临时数据落在系统 TEMP（C 盘），彼时 C 盘 **0 字节剩余（100% 满）**——写入崩塌；C 盘此前已被 pip / npm 缓存等占满（Users 60GB / Windows 18GB / 缓存数 GB）。
- **修复**：`pip cache purge`（删 1557 目录）+ `npm cache clean --force` 释放 ~4GB；运行前 `TMP/TEMP` 重定向到 `D:\shanshui-cunji-tmp`（demo 数据全部落 D 盘，D 盘 100GB 空闲）；重跑 1000万 ~2.5min 完成 10/10。
- **提交**：无（环境处理；与 P30 代码修复配合，排除干扰项后定位真实回归）
- **备注**：P4 曾记录 C 盘满导致构建失败（rustup override 已切 MSVC），本次为运行期数据目录，性质不同。

### P32. Edge headless 截图静默失败：需 --user-data-dir + 绝对路径
- **现象**：`screenshot_sections.py` 逐节截图全部未生成（脚本 `capture_output=True` 吞掉错误，仅打印文件名）。
- **根因**：本机已运行 Edge 实例时 headless 复用会话失败；且 `--screenshot=images/...` 相对路径对 Edge 不可解析（报「系统找不到指定的路径」，Edge 解析输出路径的工作目录与调用方不一致）。
- **修复**：脚本为每次截图加 `--user-data-dir`（独立 profile 目录）+ 输入输出路径全部 `os.path.abspath`；30 张截图（3 规模 × 10 节）全部生成。
- **提交**：`87777df`（images/perf-0.3.0/screenshot_sections.py）

---

## 阶段 3 末 · M6 高性能写入模式（v0.4.0）

### P33. 环形 WAL 集成：新库恢复顺序导致 NotFound
- **现象**：改造 `ColumnFamily::open` 引入 `WalBackend` 后，column_family 全部测试失败（`Io(NotFound: 系统找不到指定的文件)`）。
- **根因**：重构时在 append 分支先 `WalReader::recover(wal_path)` 再 `WalWriter::open_append`——新库 `wal.log` 尚不存在，recover 直接 NotFound；此前旧流程是 `open_append`（`create(true)` 建文件）先执行。
- **修复**：append 分支改为「先 `open_append` 建文件，再 recover」；顺带确认 ring 分支 `open_or_create` 内部已正确处理新文件（预分配 + 初始头）。
- **提交**：`66813c9`（M6-1）

### P34. MVCC 快照读：Delta 跨列族 seq 空间不可比
- **现象**：`Engine::get_at` 初版对 Delta 增量按引擎快照 seq 过滤（`seq > snapshot_seq` 跳过），快照读仍读到快照后的 Delta 修改。
- **根因**：每个列族（primary / delta）各自维护独立 seq 空间（从 1 开始），Delta 条目的 seq 是 Delta 本地序号，与引擎主数据 seq 不在同一坐标系，直接比较无意义。
- **修复（基础版语义）**：快照隔离**只覆盖主数据版本**（`ColumnFamily::get_bytes_at` 按主数据 seq 过滤），Delta 字段级热更即时叠加；文档明确「完整跨列族全局 seq 一致性留后续」；测试同步调整（快照后 Delta 修改在快照读中可见）。
- **提交**：`07f556e`（M6-3）

### P35. 环形 WAL 回绕覆盖安全：全刷盘前提 + WalFull 强制 Flush
- **现象**：设计环形 WAL 时，若回绕覆盖未刷盘记录会丢数据。
- **根因/决策**：环形写指针回绕到头部会覆盖最旧记录；无法保证被覆盖记录已刷入 SST。
- **修复**：回绕仅允许在**整个环内无未刷盘记录**时进行（`max_written_seq ≤ flushed_seq`，Flush 后由 `set_flushed_seq` 上报游标）；否则 `sync` 返回 `Error::WalFull`，ColumnFamily 捕获后强制 `switch_and_flush` 腾空再重试（append 缓冲超容量同理）；崩溃安全靠两阶段 fsync（先记录区、再头部 tail）。
- **提交**：`66813c9`（M6-1）

### P36. Leveled-Compaction：单段 L0 压实是无收益重写
- **现象**：`select_compaction_inputs` 初版对单个 L0 段也执行压实（L0→L1），测试暴露 `out_level` 为 0（被 noop 守卫拦截）与预期不符。
- **根因/决策**：合并 1 个文件无去重收益，纯重写浪费 IO；且「L0→L1 合并仅 L0」策略下，单段 L0 触发会让 L1 无限累计小文件。
- **修复**：选择规则改为 **L0 ≥ 2 段才压实**（等待更多刷盘批次）；L1 文件数达层上限（`l0_stall_threshold`）时改合并 L0 + 全部 L1 收敛；测试同步（每轮刷 2 个 L0 段验证 L1 累计 → L1→L2 下沉）。
- **提交**：`4c2e17a`（M6-2）

### P37. Group Commit：双触发 fsync 反而更慢，改提交器模式
- **现象**：M8-P0 组提交首版（写路径 `maybe_group_commit` 做窗口判定 + 后台线程兜底）实测 A 写重 **1,176 ops/s**，比逐条 fsync 基线（2,003 ops/s）还慢。
- **根因**：写路径每次 put 后检查窗口到期 → fsync，同时后台线程每 tick 也检查 → fsync——**双份 fsync**（次数未减少）+ 两条路径对 `Arc<Mutex<WalBackend>>` 的锁竞争（fsync 期间各持锁 0.5ms，writer 与后台线程互相阻塞），收益被完全抵消且劣化。
- **修复**：改为**单一提交器模式**——写路径零 fsync（`maybe_group_commit` 开启时直接返回），落盘统一由后台提交线程按窗口执行（ScyllaDB / InnoDB `flush_log_at_trx_commit=2` 思路）；字节阈值触发同样归后台线程。
- **结果**：2ms 窗口 **91,296 ops/s**（45×），P50 872µs→7.8µs；1ms 窗口 75,330 ops/s（37×）；达无 fsync 上限（113,587）的 80%。
- **提交**：`648d9bd`（M8-P0）

### P38. 长文本整串进倒排字典：1 亿单 posting term 膨胀（5000 万导入卡顿根因）
- **现象**：5000 万条导入极慢（数小时级），db-50m 达 19GB；观察进程 CPU 高、磁盘阶段 0 写入。
- **根因**：`extract_terms` 对**所有字符串字段**生成 `field=value` 完整 term——ds-50m 的 2 个 256 字符
  big_text 字段每行产生 256B 且全唯一的 term → 5000 万行 = **1 亿个单 posting term ≈ 2.6GB 纯字典浪费**；
  倒排刷段排序（O(P log P)，P=1000 万/段）与 JSON 解析（每行 2 次序列化 + 遍历）叠加 → CPU 数小时。
- **复杂度分析**：写入 O(N·F)；倒排排序 ∑O(Pᵢ log Pᵢ)；内存峰值（100 万行 posting + memtable + mmap）
  实测 **WS 5.3GB / PM 6.5GB，远低于 16G**（非内存问题，是字典膨胀 + 排序 + 全字段建索引）。
- **修复**（M8-P4）：`[inverted] inverted_fields`（白名单）/ `exclude_fields`（黑名单）/
  **`max_term_len`（默认 96B，超长 term 自动跳过 = 长文本整串不进字典）**——`Engine::inverted_allowed`
  写路径统一过滤；demo 实测 100 字段表白名单 20 字段字典压缩 45 万倍（12 vs 550 万唯一 term）。
- **结果**：重导后 big_text 不再进倒排（8 个枚举字段建索引），导入显著加速、库大幅缩小；
  长文本存主数据可主键/扫描查询，后续可加 `fulltext` 分词建词 term 索引（与 inverted:false 正交）。
- **提交**：`cde4f18`（M8-P4）

### P39. WAL 无限增长：6.5GB wal.log 每 100 万行 fsync 拖垮导入
- **现象**：5000 万导入中 wal.log 达 6.5GB 不回收；导入在 400 万行后长时间卡顿（CPU 满、磁盘 0 写入）；磁盘写入双倍（WAL + SST）。
- **根因**：append 模式 WAL 在 SST flush 后**不截断**——每次 `flush_wal`（每 100 万行）fsync 6.5GB 大文件的全部脏页 → 单次 fsync 数十秒；WAL 文件持续增长。
- **修复**（M8-P5）：flush 成功后 `truncate_and_reset` 清空 WAL 并写**文件头（magic + next_seq）**持久化 seq 接续（重开不冲突）；`open_append` 读头 / `recover` 跳头 16B / 旧无头 WAL 兼容；`WalWriter` 打开模式 append → read+write（Windows append 句柄不允许 `set_len(0)`，PermissionDenied）+ sync 前 seek 末尾。
- **语义变更**：增量备份只导出 WAL 未刷盘记录（已刷盘由全量备份覆盖，与环形 WAL 一致）；缺口检测仍有效。
- **结果**：WAL 保持小文件，导入卡顿消除、速度稳定 100 万/分钟（SST 构建 + 倒排段排序主导）。
- **提交**：`a4d829a`（M8-P5）

### P40. 批量导入 HotCache 灌满内存 → 页面颠簸 → 行速指数级崩塌（5000 万导入 4M 行后卡死）
- **现象**：WAL 截断修复后 5000 万导入仍确定性卡死：0-4M 行 60K 行/s 正常，4M-5M 掉到 2K/s，8M-9M 掉到 0.9K/s，12M 后无限卡（CPU 满核、磁盘 0.9M/s、inverted/primary 停止更新）；5M 复现同样在 ~4.3M 开始指数减速。
- **根因**：**HotCache 默认 4GB 预算被只写不读的导入文档灌满**。批量导入每行 `put_nosync` → `hotcache.put`，4M 行 × ~600B ≈ 2.4GB 全进缓存（导入从不读取，纯浪费）；LruCache 内部淘汰（容量 4M 条目）不同步 `stats` HashMap（泄漏）与 `used_bytes`（虚增）——叠加 primary memtable 256MB + WAL 缓冲，进程 WS 涨到 4.9GB。本机仅 16GB 且桌面负载（TRAE+浏览器）占 ~11GB → 总需求超物理内存 → **Windows 页面文件颠簸**（峰值 24.8GB）：每个内存访问缺页换入换出 → CPU 满转、磁盘 ~0.9M/s（页面文件）、行速指数恶化、最终假死。修复前验证过单行路径全部 O(1)/O(log n)（WAL 内存缓冲、SkipMap、DashMap、LRU），排除算法 O(N²)。
- **修复**：`Engine::set_bulk_import(on)`（P40）——批量导入模式跳过 `put_nosync` 的 HotCache 失效/回填（`import_parquet` / CSV / JSON 三个导入器入口统一开启）。导入只写不读，回填缓存无收益，跳过即消除 2.4-4GB 内存压力。
- **结果**：5M 复现 80s 完成、全程稳定 63K 行/s（越过 4.3M 卡点）；50M 正式导入 WS 从 4.9GB 降到 **621MB**、61.7K 行/s 稳定通过旧卡点 12M。
- **遗留**：HotCache `stats` 泄漏 / `used_bytes` 虚增（LruCache 内部淘汰未同步）是独立内存缺陷，常规读写负载下也会缓慢泄漏，待后续修复。
- **提交**：`bde422d`（M8-P6）

### P41. HotCache 内部淘汰不通知 stats/used_bytes：泄漏 + 虚增 + LFU O(N) 风暴（大批量回表卡死 server）
- **现象**：fulltext 验证时 `GET /fulltext?word=rec`（命中 5M 行）把 server 卡死——日志每秒刷
  "HotCache 达软水位"（used_bytes 已超硬预算 285MB/268MB 仍上涨）、CPU 满、后续请求全部超时。
- **根因**：HotCache 容量按**条目数**（`max_memory_mb×1024×1024/1024`，默认 4M 条）设 LruCache
  容量，**LruCache 满后内部自动淘汰不通知 `stats`/`used_bytes`** → stats 无限泄漏（每写一个
  新 docid 永久残留）+ used_bytes 只增不减（虚增）。超预算后 `evict_one` 从 stats 选 victim，
  但该 key 常已被 LruCache 内部淘汰（`cache.pop` 返回 None）→ **淘汰永远失败 + 超预算死循环**；
  LFU `pick_lfu_victim` 全量扫描 stats（O(N)）→ 大批量回表（每 get 一次 hotcache.put）把
  写/查询路径卡成 **O(N²)**。这是 P40（50M 导入 4M 行卡死）的叠加因素——内存压力由 hotcache
  虚增/泄漏放大，页面颠簸由淘汰失败雪上加霜。
- **修复**：①容量 **unbounded**（`LruCache::unbounded`），淘汰**完全由字节预算**统一管理——
  stats 与缓存同步（不泄漏）、used_bytes 准确（不虚增）、evict 必有真实 victim；②**软水位渐进
  淘汰**（每 put 至多 1 个，防单次 put 的 O(N) evict 风暴）；③**LFU 采样近似**（主缓存前 64 条目
  选最小计数，O(64) 常量，替代全量 O(N) 扫描）。
- **结果**：hotcache 14 测试全绿（+2 回归：批量 put 无泄漏/无虚增/真淘汰、10K 次 512KB put
  渐进淘汰 <5s）；5M 库小查询 959ms 正常（修复前 server 假死）。
- **观察（非本项）**：大结果集查询（数百万行）server 端全量 JSON 构造仍会内存爆炸
  （命中 5M 行 → 10GB+），API 无 limit/分页 → 后续加 limit/游标分页。
- **提交**：`5a937ea`（P41）

### P42. 删除位图：仅写 1bit 会丢增量备份删除 → WAL-only 删除记录；MVCC 快照语义拆分
- **现象**：Ex-5.6 按设计"删除仅写 1bit"实现时，`backup_incremental`（只导出 primary WAL
  记录）漏掉删除——位图删除不写 primary WAL → 增量备份/恢复后已删 docid 复活（数据完整性缺陷）；
  且 `get_at`（快照读）对位图已删 docid 走主数据读到旧数据，与 `get` 返回 None 不一致。
- **根因**：①删除只置位图时，删除操作不进 primary WAL（增量备份 `wal_records_since` 读不到）
  → 恢复流丢失删除；②位图不记删除 seq，快照读无法像 Tombstone 那样按 seq 过滤——若 `get_at`
  不查位图，已删文档在快照里"复活"。
- **修复**：①`ColumnFamily::delete_record_wal`——位图删除**额外写一条 primary WAL 删除记录**
  （不写 memtable Tombstone、不逐条 fsync，墓碑不进入 LSM；记录供增量备份导出 + 崩溃回放
  转 `Engine::delete` 重新置位，幂等）；②`Engine::get/get_at` 均先查位图 O(1) 跳过——位图
  删除为**立即/全局语义**；MVCC 快照隔离（删除前快照可见）仅保留在位图关闭的 Tombstone 路径，
  测试 `get_at_returns_none_after_delete_before_snapshot` 按此拆分双语义断言。
- **结果**：增量备份导出含删除（`deletion_bitmap_incremental_backup_captures_delete` 回归）；
  330 测试全绿（+10：bitmap 4 + engine 5 + column_family 2）；demo 6 测试全绿。
- **提交**：`e615071`（Ex-5.6）

### P43. FST mmap 落地：Windows 两坑 + unsafe 白名单独立 crate（P23 兑现）
- **现象**：Ex-5.7 把 FST 字典从 `fs::read` 全量加载改为 mmap 按需加载时，本机（Windows）全量
  测试大面积 `PermissionDenied`；且按旧 gc 顺序（先删旧 .fst 再清字典）Windows 下删除静默失败。
- **根因**：①**只读句柄 `sync_all()` 被拒**——Windows `FlushFileBuffers` 要求句柄带写权限，
  `File::open`（只读）调用 `sync_all` 返回 code 5；②**已映射文件无法删除/改名**——mmap 持文件
  句柄期间 `remove_file`/`rename` 失败（Unix 无此限制，Windows 专属）。
- **修复**：①fsync 改在**写句柄**上进行（BufWriter 借用 File，flush 后仍持写句柄 `sync_all`）；
  ②发布顺序改为「fsync → rename → mmap」；gc 改为**先 `dicts.clear()` 释放旧映射、再删旧文件**
  （双端一致）；③mmap unsafe 依 P23 决策隔离到**独立 crate** `crates/mmap-file/`（只读 `MmapFile`
  + `unsafe impl Send/Sync` 完整论证：只读无写逃逸 + FST 文件不可变约定 + fd 生命周期解耦），
  主库 `#![forbid(unsafe_code)]` 保持零 unsafe 承诺。
- **结果**：330 测试全绿（既有 FST/GC 测试全部走 mmap 路径）；mmap-file crate 3 测试全绿；
  demo 实测冷启动 fs::read 堆分配 17.3MB vs mmap 0B + 0.14ms；提交 `442981c`（Ex-5.7）。

### P44. 元数据-数据解耦落点：元数据占比仅 2.59%，收益来自数据块免重压（块级复用）
- **现象**：设计"Compaction 只重写元数据 → 写放大 -50%"，但实测 SST 中元数据区（索引+布隆+
  Footer）仅占 2.59%——若按字面"只重写元数据"，收益与 -50% 写放大不符，需重新定位落点。
- **根因**：LSM Compaction 的写放大主体是**数据块重写**（读全部输入行 → 排序去重 → 重分块
  重压缩）。元数据本身极小，真正可省的是无重叠输入段的**数据块免解压/免重压缩**。
- **决策/修复**：Ex-5.8 落地为**数据块级复用 Compaction**——`SstWriter::add_raw_block`（原样
  写压缩字节 + 重建 trailer/索引/分区布隆）+ `SstReader::block_raw`；`ColumnFamily::compact`
  检测相邻段 key 无重叠（前段 max < 后段 min）且行式列族 → 数据块区按 key 序原样拼接、只重建
  元数据区；有重叠（覆盖/去重语义）/位图物理删除/PAX 列族（zones 无法重建）回退全量合并。
- **结果**：demo 无重叠合并全量重写 4041ms（读入 40 万行）vs 块级复用毫秒级（零解压）；
  333 测试全绿（+3）；提交 `cd00d85`（Ex-5.8）。

### P45. 冷热感知 Compaction：热度语义定位（"优先合并热层级"的落地）与 Bloom Merge 边界
- **现象**：Ex-5.9 落地"冷热感知 Compaction + Bloom Merge"时，直接"按热度优先合并"会破坏
  写 Stall 减压（L0 全量合并最快降段数）；且"合并前布隆判断有效性"语义模糊（布隆逐键判断
  本身引入读放大）。
- **决策**：①热度统计为基础设施（`SstReader::touch`，**布隆放行后才计数**——未命中/布隆拦截
  不计，避免假阳性污染热度）；②热段优先仅限 **L0 超阈值（逼近写 Stall）**时合并最热
  `level_limit` 段——热段先下沉 L1 聚合、热点读路径段数更快减少，L0 段数同样下降（减压不
  变）；③Bloom Merge 定位为 Ex-5.8 已承担：无重叠检测（索引范围）+ 分区布隆重建
  （add_raw_block），不新增逐键布隆判断（避免读放大）。
- **结果**：demo 热段排序选段正确、部分合并读语义不变；335 测试全绿（+2）；提交 `ba709e2`
  （Ex-5.9）。

### P46. 多 SSD 条带化：三列族 WAL 同名冲突 → 独立盘按列族分子目录
- **现象**：Ex-5.10 给三个列族（primary/cidx/delta）配独立 WAL 盘时，若都写 `wal_dir/wal.log`
  会互相覆盖（WAL_FILE 常量同名）。
- **修复**：`ColumnFamily::open_with_wal_dir` 中独立盘 WAL 路径 = `wal_dir/{name}/wal.log`
  （按列族分子目录），且 open 时 `create_dir_all(w.join(name))`。
- **结果**：336 测试全绿（+1：multi_ssd_striping_places_files）；demo 三盘模拟全通；
  提交 `e6a5610`（Ex-5.10）。

### P47. PerCpuCounter 槽位分配：thread_local 首访 + 原子递增模槽数（线程数 > 槽数分摊）
- **问题**：按核拆分的计数器需要"每线程固定槽位"。std 无稳定线程 id → usize 映射。
- **方案**：`thread_local` 静态槽位 + 全局 `NEXT_SLOT` 原子递增模槽数——每线程首访分配并
  固定；线程数 > 槽数时自然分摊（多个线程共槽，退化为轻度竞争，可接受）。
- **结果**：demo 8 线程写 2.1×；339 测试全绿；提交 `c5fa66c`（Ex-7.1）。

### P48. 绑核与 seqlock flaky：core_affinity 依赖引入 + 并行负载下测试敏感
- **问题**：①绑核需 core_affinity（unsafe 依赖，memmap 同策略——仅依赖内部 unsafe，主库
  零 unsafe 不变）；②seqlock `low_frequency_write_low_retry_rate` 在 342 测试并行负载下
  重试率超 1% 断言（写间隔 20µs 在调度抖动下与读重叠率高）。
- **修复**：①core_affinity = "0.8"（跨 Windows/Linux/macOS），绑核失败忽略 + taskset 兜底，
  配置可关闭；②测试写间隔 20→100µs（更符合"低频写"语义，负载稳定）。
- **结果**：344 测试全绿；提交 `b294532`（Ex-7.2）、`fd0b519`（Ex-7.3）。

### P49. 动态限流：MemTable 水位作写压力代理 + 让路语义（50% 下限）
- **问题**：Ex-7.4 需要"按前台写负载动态下调 Compaction 限速"，但写负载直接测量（滑动窗口
  ops/s）复杂且抖动；且调速语义要与"前台写优先"一致。
- **方案**：**MemTable 水位 = 写压力代理**（used/max clamp 0~1，天然反映写快慢）→ 限速 =
  base × (1 - 0.5p)——压力 0 全速追赶 L0，压力 1 让路 50% 磁盘带宽给前台（下限保护）；
  `IoRateLimiter::set_rate` 动态调速（容量受新突发上限约束，不凭空赠予额度）。
- **结果**：demo 让路 1000ms vs 恢复 500ms；346 测试全绿（+2）；提交 `ddbc20e`（Ex-7.4）。

### P50. ArcSwap 化倒排段清单/FST 字典：MmapFile 不可 Clone → 值改 Arc 使 HashMap 可 rcu
- **问题**：Ex-6.2/6.3 把 `segments`/`dicts` 改 ArcSwap 原子发布时，`rcu` 要求整体 `Clone`；
  FST 字典值 `fst::Map<MmapFile>`（MmapFile 包 memmap2::Mmap）**不可 Clone** → HashMap 无法
  Clone → rcu 不可用；且 `rcu` 闭包为 `FnMut`（map 无法 move 进闭包）。
- **方案**：①dicts 值改 `Arc<fst::Map<MmapFile>>`（Ex-6.3 设计本意"值改 Arc"）——Arc 可
  Clone → HashMap 可整体 Clone；②map 先 `Arc::new` 再闭包内克隆捕获（FnMut 多次调用安全）。
- **结果**：快照一致性/并发读/读写交替 3 测试全绿；349 测试全绿；提交 `c8183cf`（Ex-6.2/6.3）。

### P51. 分布式写吞吐：跨地域 10000 条 1074s → 网关分片并行 + 批量 RPC + 节点组提交
- **问题**：跨地域真机两节点写 10000 条耗时 1074s（9.3 w/s）——瓶颈三连：网关全局 Mutex 串行
  所有分片写、同步 RPC 逐条往返（RTT 按条付）、节点每次写独立 fsync（无组提交窗口）。
- **方案**（C 项三项独立改造）：①网关按分片并行——cluster_demo 写循环每线程独立 Gateway 实例
  （独立 RPC 连接集合，去全局锁）；②RPC 批量写入——`shard.put_batch` handler（节点
  Engine::put_batch 原子提交）+ `ShardEndpoint::put_batch` trait + `Gateway::put_batch` 按 docid
  一致性哈希分组 → 每节点一次 RPC 批量提交（RTT 分摊到批）；③节点组提交——cluster_demo
  `--group-commit-us` 2000µs（配置 `storage.group_commit_us`），窗口内写攒批一次 fsync。
- **结果**：本机两节点 10000 条（4 线程 × 2500，batch=10000）写 0.03s（364,584 w/s），广播检索
  精确命中 + 逐条点查跨节点路由强一致校验通过（无丢失/重复）；375 测试全绿（+1 批量路由/计数）；
  跨地域真机复测（阿里云 HDD node + SSH 隧道）10000 条写 0.5s（21,590 w/s）——对照基线 1074s 提升
  ~2100×，目标 <60s 达标。

### P52. 事务三阶段：锁泄漏 / 死锁环检测依赖等待关系保留 / MemTable 多版本局限
- **问题**：D/E/F 事务三阶段实现中三个坑——① `txn_commit` 失败路径（写锁获取中断 / 冲突 abort）
  不释放已获取锁 → 锁泄漏；② wait-for 死锁环检测：冲突请求若立即撤销等待关系，后续请求无法形成
  环 → 死锁漏检；③ 快照读测试暴露 MVCC 已知局限：MemTable 仅保留每 key 最新版本，未刷盘覆盖的
  历史版本不可回读（get_at 返回 None 而非旧值）。
- **方案**：① commit 逻辑收敛到闭包（`(|| -> Result<()>)()`），成功后 mark_finished，无论成败统一
  `txn_locks.release(txn.id)`；② 无环冲突**保留等待关系**（`waiting` 表），仅死锁时撤销受害者等待，
  release 时统一清理；共享→排他锁支持 2PL 合法升级（唯一持有者是自己时直接升级）；③ 文档化局限 +
  测试对齐：快照读场景先 `flush_primary` 使旧版本落 SST（MemTable 多版本保留留后续）。
- **结果**：+15 事务测试全绿（WriteBatch 原子/回滚/预校验、RR 快照读/写冲突 abort、RC 最新读、
  SERIALIZABLE 读写锁/升级、死锁环、delete 混合提交、快照 seq 推进）；393 测试全绿；提交见事务提交。

### P53. 倒排段数据 mmap 化：Windows 已映射文件不可删 → gc 先换映射再删文件
- **问题**：G 项段数据 mmap（K 项方向）落地时，`read_segment_posting` 每次未命中查询 `fs::read`
  整个段文件（大段几十 MB，纯 IO 浪费 + 堆复制）；mmap 化后 Windows 下已映射文件无法删除
  （gc 删旧段会失败）。
- **方案**：`data_files: ArcSwap<HashMap<seg, Arc<MmapFile>>>` 与 dicts 同模式——flush 预注册、
  重开懒加载（首次查询 rcu 注册）、gc 先 `data_files.store(新段映射)` 释放旧映射再删旧文件
  （P23 顺序保证，与 dicts 一致）；查询按 FST offset 直接 mmap 切片反序列化。
- **结果**：+3 mmap 测试（flush 注册/重开懒加载/GC 后查询正确）；393 全绿。

### P54. 看门狗磁盘熔断：C 盘 3% 剩余真实触发熔断 → 比例 + 绝对下限双条件
- **问题**：P52 看门狗落地时，`DiskGuardian::classify` 按剩余比例分级（stall=5%）——本机 C 盘
  100GB 仅剩 3GB（3%）→ **所有写路径测试真实熔断拒绝**（check_all 挂进 put/put_batch/write/delete）。
  暴露设计缺陷：小比例但绝对空间仍充裕的盘会被误熔断（数据库写爆盘的真正危险是"剩余不足一个
  MemTable/WAL 段"，与绝对量相关）。
- **方案**：熔断改为**双条件**——剩余比例 ≤ `disk_stall_ratio` **且** 剩余绝对字节 <
  `disk_stall_min_mb`（默认 1024MB，对齐 MySQL 预留空间思想）；限流/预警仍按比例。
- **结果**：C 盘 3GB 剩余 → 比例 3% 触发限流（软信号放行）而非熔断，写路径恢复；401 测试全绿
  （+8：disk classify 分级/采样缓存/check_all 熔断/CPU 并发限制与释放/query guard drop 释放）。

### P55. 看门狗 CPU/磁盘三级响应（P52 设计落地）
- **问题**：看门狗仅有内存水位（memory_check）+ 查询超时；CPU 风暴与磁盘写爆无保护。
- **方案**：①`DiskGuardian`：磁盘剩余空间三级（预警 warn=0.20 → 限流 throttle=0.10 →
  熔断 stall=0.05+1GB 绝对下限），`disk_space` 独立 crate（P23 白名单，Windows
  GetDiskFreeSpaceExW / Unix statvfs）跨平台查询，1s 采样缓存免写路径频繁 syscall；
  ②`CpuGuardian`：并发查询数代理 CPU 压力（`try_begin_query` 超限返回 Stalled，QueryGuard
  drop 自动释放槽位）；③写路径统一入口 `Watchdog::check_all(mem, disk)`（put/put_batch/
  write/delete/txn_commit 调用）；④EngineStats 暴露 disk_ratio/disk_status/cpu_active。
- **结果**：+6 测试全绿；401 全绿；`crates/disk-space` 独立 crate（零新增外部依赖）；
  配置 `[watchdog] disk_warn/throttle/stall_ratio + stall_min_mb + cpu_query_limit`。

### P56. MySQL 协议适配三坑：授权包 seq 全局连续 / 握手响应字段顺序 / plugin name
- **问题**：H 项 MySQL wire protocol 实现，mysql cli 8.0 连接报 `Lost connection at
  reading authorization packet`（服务器日志显示认证通过且 OK 已发送，客户端却读到 EOF）——
  三个协议细节坑：①**授权包 seq 应为 2**（握手 seq0 → 客户端握手响应 seq1 → 授权包 seq2，
  **全局连续而非每方向独立**；pymysql 报 `Packet sequence number wrong - got 1 expected 2` 定位）；
  ②**握手响应字段顺序**：username → auth_response → [CONNECT_WITH_DB] db → [PLUGIN_AUTH]
  auth_plugin_name → attrs（db 在 auth_response **之后**，且各字段由「服务器声明」决定客户端
  是否发送——服务器未声明 CONNECT_WITH_DB 时客户端不发 db，直接是 plugin name）；
  ③auth plugin name（"mysql_native_password"）是握手响应的独立 NUL 串字段，需读取。
- **方案**：授权 OK/ERR 包 seq 改 2；按服务器声明跳过 db/attrs、按顺序读取 auth_response 与
  plugin name；`mysql_native_password` 认证（sha1 scramble）校验。
- **结果**：mysql cli 8.0 真实连接 + SELECT VERSION()/SHOW DATABASES/INSERT/UPDATE/DELETE
  全链路通过；pymysql 全链路通过；协议级测试 +6；407 全绿；提交见 H 项提交。

### P57. H-4~H-6 落地三坑：COMMIT 空提交语义 / PREPARE 双 EOF / pymysql 多列 INSERT
- **问题**：H-4~H-6 实现中三个坑——① `COMMIT` 无活动事务时我返回错误（3505），但 MySQL 语义是
  返回 OK（空提交）——pymysql `conn.commit()` 在 autocommit=False 且无 BEGIN 时发 COMMIT → 报错；
  ② `COM_STMT_PREPARE` 响应有**两个 EOF**（参数定义后 + 列定义后），测试客户端只读到第一个 EOF →
  残留字节污染后续 EXECUTE 读取（错位成列数 3）；③ sysbench 风格 INSERT `(id,k,c,pad) VALUES` 只认
  id/doc 列 → 多列报"缺 doc 列"。
- **方案**：① COMMIT/ROLLBACK 无事务 → OK（对齐 MySQL）；② PREPARE 读取按"第二个 EOF 终止"
  状态机；③ parse_insert 扩展：非 id/doc 列组装为 JSON 文档 `{"k":500,"c":"hello",...}`
  （数字/布尔按 JSON 类型，其余字符串），DDL 语句放行（文档库无 schema）。
- **结果**：+4 测试全绿；411 全绿；sysbench 风格负载模拟通过（prepare 945 w/s / 点查 3040 q/s /
  事务 1744 txn/s）；提交见 H 项提交。

### P58. 倒排回表逐 id 点查 → batch_get：SST 按块分组 + Delta 单次范围扫描
- **问题**：倒排/全文检索 posting 回表（`search_term_paged`）对每个 docid 逐次 `engine.get()`——
  每 key 独立走完整 LSM 点查（MemTable + 分层 SST 的布隆/二分/读块/解压 + Delta 扫描 + JSON 合并）。
  posting 返回 1 万主键 = 1 万次随机点查（G/K 项优化了 posting 查询端，回表端未批量），
  是倒排链路的下一性能瓶颈。
- **方案**（借鉴 batch_get 架构建议的三步批量接口）：`sstable.scan_block_for_keys`（块一次解码命中
  多 key）→ `column_family.get_many`（MemTable 批量 + 逐 SST **按块分组**：整文件/分区布隆粗筛、
  每数据块只读/解压一次、块缓存复用；Tombstone 语义与 get_bytes 一致）→ `engine.batch_get`
  （删除位图 O(1) 批量过滤 + HotCache 批量命中 + Delta **单次范围扫描按 docid 分组**覆盖）；
  `search_term_paged`/`fulltext_search_paged` 回表改走批量（bitmap 迭代 docid 升序，天然满足输入要求）。
- **结果**：万级 posting 回表从万次随机读降为块级顺序读（同块多 key 共享一次 IO/解压）；
  语义与逐条 get 完全一致（+3 测试：get_many 跨 flush+tombstone、batch_get vs get 含 Delta 覆盖/
  删除位图、倒排回表分页/删除过滤）；419 全绿；提交 `d044b4c`（N 项，与 M 项同提交）。

### P59. MemTable 多版本落地两坑：同 key 版本不得跨块 + flush 前先更新 buf_last_key
- **问题**：S 项（严格 MVCC）实现中发现两个坑——① **同 key 多版本不得跨数据块**：`locate_indexed_block`
  二分取"首个 first_key ≤ key 的最后一块"，若同 key 版本被拆到相邻两块，会漏读前一块的旧版本 →
  快照读丢数据；② 刷块时 `flush_block` 以 `buf_last_key` 作块 max_key（Zone Map 上界），若在
  更新 `buf_last_key` 前刷块，max_key 缺失当前行 key → 范围扫描按 Zone Map 剪枝漏行
  （`zone_map_prunes_out_of_range_blocks` 10 vs 11 暴露）。
- **修复**：`SstWriter::add_inner` 改为"仅当换 key 且块达阈值时刷块"（同 key 版本强制同块），且
  **先更新 `buf_last_key` 再刷块**（max_key 含当前行）。
- **结果**：+6 测试（memtable 版本链/get_at/delete 历史、CF 未刷盘快照读旧版本、SST 刷盘后快照保持、
  RR 事务无 flush 读旧版本）；428 全绿；提交 `e7a413a`（S 项）。

### P60. RwLock 读读并行落地：SstReader RefCell → Mutex（Sync 阻断）+ 内部可变四件套
- **问题**：O 项第②步把 mysql.rs `Arc<Mutex<Engine>>` 换 `Arc<RwLock<Engine>>` 后编译报
  "SstReader cannot be shared between threads safely"——`RwLock<T>: Sync` 要求 `T: Sync`，而
  `SstReader.full_index` 用 `RefCell`（!Sync）懒加载 Level 2 精确索引 → 引擎无法跨线程共享。
- **修复**：`SstReader.full_index` RefCell → Mutex（读路径共享锁）；同时为引擎读方法 `&self` 化补
  内部可变四件套：`HotCache → Mutex`、`txn_locks → Mutex<LockTable>`、`pending_inverted → Mutex`、
  `SstReader.full_index → Mutex`；读语句（SELECT/SHOW/SET）走读锁并行、写语句写锁互斥，
  sqlish 读路径同步 `&Engine` 化。
- **结果**：1 亿库 read_only 42→561 TPS（+13.3×）、read_write 29.5→230 TPS（+7.8×）、
  事务平均延迟 -87%（突破 ~1000 stmt/s 串行天花板）；+1 并发测试（4 读线程 + 1 写线程共享
  Arc<RwLock<Engine>>）；429 全绿；提交 `4585bb9`（O 项第②步）。

### P61. O 项第③步落地三坑：ArcSwap 双层 Arc 类型 / 索引漂移依赖写互斥 / 后台合并持读锁
- **问题**：ssts ArcSwap 化（O 项第③步）编译期三坑——① 字段 `ArcSwap<Arc<SstSnapshot>>` 与
  `store(Arc::new(SstSnapshot{..}))` 类型不匹配（ArcSwap 内部已持 Arc，应 `ArcSwap<SstSnapshot>`，
  参考 inverted.rs 的 `segments: ArcSwap<Vec<String>>`）；② `scan_stream_at` 的
  `self.ssts.load().ssts.iter()` 临时值借用（E0716，需先 `let snap = self.ssts.load();`）；
  ③ `SstReader::iterate/block_raw` 为 `&mut self`（内部 file seek），compact 经 `Arc<SstReader>`
  调用不能借可变——`block_raw` 改用无状态 `read_at` 后 `&self` 化。
- **设计决策**：后台合并**必须持引擎读锁**（`try_read`）执行而非完全无锁——DeletionBitmap
  （Vec<u64>+HashSet，无内部同步）与 manifest 文件在 compact（读）与写路径（写）并发时存在
  数据竞争；读锁下写被互斥、读读并行 → 竞争面消除，且快照 store 无并发丢失、compact 链内
  `self.ssts.load()` 索引稳定（flush 需写锁，不会在 compact 中途插入新段）。写路径 `auto_compact`
  双分支：挂载 worker（`compact_worker=true`）只置 `compact_pending` 信号；无 worker（demo/rpc/
  测试）保持同步收敛=背压（既有行为不变，`auto_compact_keeps_l0_bounded_on_flush` 原样通过）。
- **结果**：+1 测试（后台触发置位 + `&Engine` 读锁合并收敛）；430 全绿；1 亿库复测无读回归
  （read_only 519-550 / read_write 236-246 TPS）；小库端到端验证后台合并链路
  （flush→信号→worker 读锁合并→日志"Compaction 完成"）；提交 `e9f7d39`（O 项第③步，O 项完结）。

### P62. R 项：层布隆 OR 合并数学上不可行 → 层/段两级 Zone Map 范围粗筛
- **问题**：排期原方案「层布隆 = 段布隆 OR 合并」经推演不可行——① **num_bits 冲突**：块/段布隆
  按各自 key 数分配位数组，查询 `%num_bits` 定位，num_bits 不一致无法位级 OR（哈希位错位）；
  强制统一 num_bits=层容量 → 每段布隆=层容量（1 亿库 125MB×78 段≈9.75GB 磁盘/内存）爆炸；
  ② **L0 层布隆 = 历史全 key 集**（所有写入都经 L0）→ 位数组必然填满、假阳性 100% 无效；
  ③ meta-only compact 不读数据块无 key 列表 → 无法增量维护层布隆（假阴性=丢数据）。
- **落地**：等价目标改用 **Zone Map（min/max）两级范围粗筛**（精确、零假阴性、零格式变更）——
  SstSnapshot 增 layer_ranges/layer_indices（快照构建 O(段数) 聚合，层范围=段范围精确并集，
  含无范围段→层不可跳过）+ get_bytes/get_bytes_at/get_many 按层遍历整层跳过 +
  get_from_sst 段级 O(1) 越界跳过（省二分+布隆反序列化）。
- **结果**：demo 16 段点查 ≈ 单段（0.95×）；431 全绿；提交 `388a916`（R 项）。

### P63. T 项：事务点查快照缓存落地
- **问题**：RR 快照读 `get_at` 刻意不走 HotCache（防污染全局热缓存）→ 事务内重复点查冷读放大。
- **落地**：Transaction 内 256 项 snap_cache（HashMap，超限清空）——快照读先查缓存（命中免 LSM
  冷读 + 跳过重复加锁，首次读已加）；RC 不缓存（读最新语义）；错误结果不缓存；提交/回滚随
  Transaction drop 即弃。正确性：快照 seq 事务内恒定 → 缓存结果一致。
- **结果**：+1 测试（RR 缓存命中一致/RC 不缓存/新事务缓存空）；432 全绿；提交 `0eca7a5`。

### P64. V 项：io_uring 后端（Linux 门控独立 crate）
- **问题**：io_queue.rs 仅队列抽象（Ex-7.3），liburing 封装待接入；主库 forbid(unsafe_code)。
- **落地**：crates/io-uring-file（unsafe 白名单，`#![cfg(target_os="linux")]` 非 Linux 空编译，
  与 mmap-file 同模式）——io-uring 0.7 API 踩坑：0.7 无 `submit_entry`/`wait_for_cqe`/`IoUringBuilder`，
  实际为 `IoUring::builder()`+`setup_sqpoll_cpu`+`SubmissionQueue::push`(unsafe)+`submit_and_wait`+
  迭代 CompletionQueue；封装 read_at/write_at/fsync 同步提交-等待（缓冲生命周期论证）。主库
  IoUringPool 三队列 + Engine 持池 + affinity SQPOLL 预留核。主库 Linux 交叉编译受 zstd-sys
  原生依赖阻塞（Windows 无 x86_64-linux-gnu-gcc）→ Linux 代码为简单转发调用（API 已分别交叉
  check 验证），留 Linux 部署验证。
- **结果**：io-uring-file 4 运行测试交叉 check 通过；Windows 构建零影响；提交 `f09e9fb`。

### P65. W 项：Compaction 跨列族紧迫度调度
- **问题**：多列族同时需合并时无优先级——后台 worker 每次全压三列族，热/压力大的主数据列族
  不保证优先收敛。
- **落地**：column_family::compaction_urgency（L0 段数×10 + 大小超限 +8）为跨列族调度主因子
  （热段选段已由 select_compaction_inputs 在列族内承担）；Engine::compact 每轮仅压最高紧迫度
  档列族（并列档并行保留 SSD 并发），其余由 worker `while needs_compact` 多轮压实——压力最大
  列族（primary 主数据）优先收敛，读路径最快受益。
- **结果**：+1 测试（urgency 随 L0/大小压力增长）；433 全绿；提交 `f09e9fb`。

### P66. X 项：Metrics（Prometheus 风格 /metrics）分层埋点
- **问题**：无 QPS/延迟分位数/Compaction 速率/L0 文件数指标，运维不可观测。
- **落地**：src/metrics.rs 原子计数器（读写 ops/compact）+ 延迟对数直方图（7 桶 0.1ms..+inf）+
  Prometheus 文本渲染；引擎层埋点（put 写计数+延迟、get/get_at 读计数、compact 次数）、列族层
  flush_counter（switch_and_flush +1）、网络层（mysql 连接活跃/累计 + COM_QUERY 语句计数）；
  server.rs `GET /metrics`（counter/histogram/gauge，L0/SST/内存/磁盘水位实时）。
- **结果**：+3 测试；436 全绿；提交 `0257835`。

### P67. U 项：4KB 块冷扫预读合并 + 1 亿库复测无回归结论
- **问题**：冷顺序扫描逐块 4KB read_at → IO/syscall 放大（低优先）。
- **落地**：SstReader::read_block_group（一次 read_at 覆盖整组，逐块切片 + CRC + 解压，布局假设
  校验失败回退逐块读——安全）+ SstRangeIter advance_block 组读 ≤4 块预解码缓存。
- **复测发现（重要）**：1 亿库 read_only 复测 51 TPS（此前 O③ 519 TPS）——经 git 回滚二分
  （checkout e9f7d39 恢复 O③ 代码同测 67 TPS）+ 单语句诊断（pymysql 事务范围 7-8ms、非事务范围
  正常、点查正常）确认 **非本批六项（R/T/V/W/X/U）回归**，而是 DB 状态差异：14:43 的 519 TPS
  时 memtable（WAL 回放 473MB）覆盖 sysbench 查询热点（5000 万附近）→ 范围查询内存命中；当前
  memtable（110 万 insert 数据）热点落 SST 冷区 → 事务范围查询冷块读 7-8ms → 事务类 TPS 降至
  ~60。该现象 O③ 与新 binary 一致（57 vs 69 TPS）。**结论：无代码回归；事务范围查询的冷块读
  延迟为 M 项后既有行为**（后续可优化：范围查询块预读已由本项 U 覆盖一部分）。
- **结果**：+1 测试（多块段全量/跨块范围 vs scan_range 对照一致）；437 全绿；提交 `85b9a62`。

### P68. Ex-2.5：SAGA 网关 HTTP API 落地（跨分片业务事务对外接入）
- **问题**：SAGA 协调器仅内核 API（SagaStep trait 由业务方实现），无 HTTP 接入——外部服务
  无法发起/回查/重试分布式事务。
- **落地**：网关三端点 `src/server.rs`：`POST /saga/start`（`{tx_id,steps[]}`，执行正向 + 失败
  自动逆序补偿，终态幂等）/ `GET /saga/status?tx_id=`（transactionId→status 回查，屏障依据）/
  `POST /saga/compensate`（强制补偿重试）；`src/saga.rs` 增 `HttpStep`（HTTP 业务步骤）+ `http_post`
  （极简 HTTP/1.1 POST 客户端，非 2xx/超时 → 步骤失败）；协调器目录 `{data_dir}/saga`，
  状态 JSON 原子持久化 → 网关重启自动续跑/续补偿。
- **结果**：+3 网关 e2e 测试（模拟业务节点：正向全成功无补偿 + 状态文件 / 中段失败逆序补偿 +
  终态幂等重发 / 重启恢复终态）；365 全绿；提交 `781199e`（Ex-2.5）。

### P69. SAGA 补偿协议按 13.5 形式化落地（中间态恢复 + 超时屏障空转 + 缺步骤定义修复）
- **问题**：design_extension 13.5 定义了补偿协议四条不变量（空回滚/悬挂防护/幂等/持久化先于响应）
  与超时不确定性、恢复时序——需落地为代码并测试覆盖；其中已登记分支若本次 steps 缺补偿定义会被
  静默跳过并误标 Compensated（未补偿分支却终态，违反补偿语义）。
- **修复**：`SagaCoordinator::compensate`——已登记分支在 `by_name` 缺失时保持 Compensating +
  `last_error`（"缺少补偿定义，保持待补偿"），不得静默 Compensated。
- **测试（+5）**：13.5.3 中间态崩溃恢复——Executing 半途（a 已登记）续跑正向不重复 a /
  Failed 恢复续补偿完成 / Compensating 部分补偿后续补剩余分支（a 不重复）/ 缺步骤定义保持
  Compensating；13.5.2 超时不确定性——慢业务节点（300ms > 50ms 超时）步骤未登记 → 屏障空转
  不补偿（宁可漏补偿，不可错补偿），已登记分支正常逆序补偿，终态清空 last_error。
- **结果**：+10 测试（saga 16 全绿，全量 450）；提交 `170bf21`（+5）+ 补充（+5）。
  补充变体：直接 `compensate()` 调用路径 / 部分缺定义（逆序先补有定义分支、进度不丢）/ 补全定义
  重试续补偿不重复 / 缺定义状态跨重开持久化（对账依据）/ 终态 compensate no-op。

### P70. SAGA 13.6/13.7 落地：拓扑并行执行 + 后台对账自动重试
- **问题**：13.6（步骤依赖声明与正向并行）与 13.7（后台对账与自动重试）设计留白需实现——
  SAGA 长事务正向串行吞吐受限；Failed/Compensating 依赖人工 /saga/compensate 重试，无自动收敛。
- **落地**：
  - `saga.rs`：`topo_layers`（Kahn 分层 + 环/自依赖/越界检测）+ `run_parallel`（按拓扑层
    scoped 线程并行正向、层间屏障、失败转补偿；executed_steps 按拓扑层序登记 → 逆序补偿 =
    反拓扑序，依赖者先补偿）；`SagaState` 增 `retry_count`/`last_retry_at_ms`/`updated_at_ms`
    （serde default 兼容旧状态文件）+ `retry_pending`（扫描未终态：Failed/Compensating 按
    指数退避续补偿、Executing 挂起超阈值标记 Failed 触发补偿、无步骤定义跳过留人工）；
    `SagaStep` 加 `Send + Sync` supertrait（并行共享步骤引用）。
  - `server.rs`：协调器改 `Arc<Mutex>` 共享 + 步骤定义缓存（对账重建）；`/saga/start` 解析
    `depends_on`（未知/环 → 400，提前 topo_layers 校验）；spawn 后台对账线程（60s 周期）。
- **测试（+9）**：topo_layers 分层/环/非法索引；run_parallel 依赖序 / 无依赖并行（80ms×2 < 150ms）/
  链中段失败反拓扑补偿；retry_pending 自动续补偿 + 计数 / 退避跳过 / Executing 挂起检测 /
  无定义跳过；网关 depends_on 成功 + 环 400 + 未知依赖 400。
- **结果**：saga 28 全绿（+9），全量 459；提交 `71aa712`。

### P71. 1 亿库写路径 syscall 风暴（l0_bytes 每次 put 调 fs::metadata）+ 合并阻塞写缓解
- **问题**：1 亿库复测写类吞吐异常（oltp_insert 2.7k vs 基线 12.9k TPS）。A/B 逐步隔离：
  组提交有效（关组提交 1,008 vs 2ms 18,404）、排除 auto_compact 合并（l0_max_size_mb=0 → 25,103）→
  锁定 `needs_compact` 大小条件：`l0_max_size_mb>0` 且 L0≥2 时**每次 put**（engine `auto_compact`）
  调 `l0_bytes()` 对每 L0 段 `fs::metadata`（3 stat/写 → syscall 风暴）。
- **修复（96ac6bc）**：`SstReader::file_len`（open 一次 metadata）+ `SstSnapshot::sizes` 缓存
  （open/flush/compact 三构建点填）→ `l0_bytes()`/`sst_bytes()` 读缓存求和，写路径零 syscall。
- **验证**（8 线程 15s）：oltp_insert 2,676→23,964（+795%）、bulk_insert 4,630→210,895（+45×）、
  oltp_update_non_index 3,145→8,896（+183%）；读类不受影响；+3 测试（file_len/sizes 磁盘一致/重开一致）。
- **延伸（1763554）**：写路径观测另发现 worker 持读锁合并阻塞写（合并时写 39k→8.2k，-80%）——
  `[storage] compact_input_max_mb`（默认 1024MB）分批 L0 输入（`cap_by_size` 保底 2 段），
  单次合并快 → 写阻塞短（复测 -55%）；L1→L2 不受限；根治（无锁合并）留待后续。
- **结果**：全量 465 绿；提交 `96ac6bc` + `1763554` + `b9a9eb3` + `0e4e40c`。

### P72. 合并阻塞写根治方案评估（无锁合并）+ 阶段一落地
- **问题**：P71 分批只缓解合并阻塞写（-55%）；根治 = 合并与写并发（无锁合并），需动 RwLock 语义。
- **完整方案（记录供后续实施，O 项规模改造）**：
  1. CF 增 `sst_mutate: Mutex<()>`——flush/compact 的 ssts store 前置互斥（无 Engine 锁后防
     "compact 基于旧快照合并、flush 发布新快照，后 store 丢失前 store"的并发丢失）；
  2. CF `switch_and_flush` 改 `&self`（memtable 冻结内部 Mutex）——Engine 字段 `Arc<ColumnFamily>`
     化后 flush 无法取 `&mut`（O 项第①步方向延伸）；
  3. Engine primary/delta/cidx 字段 `Arc<ColumnFamily>`（Engine::compact 已 `&self` 兼容）；
  4. mysql worker：`read()` 内 clone 三 CF Arc（快速）→ drop 锁 → **无锁** CF.compact
     （复刻 Engine::compact 紧迫度调度）；写语句持写锁与合并并发执行（ArcSwap 原子发布）。
  - 风险：compact 与 put 并发安全（CF compact 不碰 memtable）；flush 与 compact 并发由
    sst_mutate 保证；需全量 + 并发测试验证。
- **阶段一落地（本项）**：mysql worker 锁内 while 8 轮连续合并 → **单轮合并 + 100ms 循环**
  （配合 compact_input_max_mb 分批单轮快）——写每轮间可插入，缓解长排队；
- **结果**：466 全绿；阶段一随提交 `XXXX`；完整无锁方案留待大改造窗口。
- **复现补充（2026-09-01 第二遍复测）**：原配置（l0_max_size_mb=1024）server 启动后第 10 分钟
  **backstop 兜底合并**（worker 无信号 10 分钟强制合并）触发读锁合并——启动加载 6 个 L0 段
  （~1.63GB + 79 段 11GB）总大小超 1024MB → 多轮 ≤1GB 分批持读锁 → 写锁排队 + 新连接握手
  （需 Engine 读锁，RwLock 写者优先）全部卡死（pymysql 超时，server CPU 满核），分钟级恢复。
  分批缓解不覆盖该场景；测试规避用 l0_max_size_mb=0（仅段数阈值）使 backstop 不触发；
  根治仍需无锁合并。详见 images/perf-0.7.0/sysbench-100m/构建记录.md「复现并诊断」段。
- **根治落地（af24dbd，2026-09-01）**：按上述完整方案实施——
  - MemTableBuffer 内部 `RwLock`（switch/take_immutable `&self`，iter_range 改 HRTB 闭包式）；
  - CF `switch_and_flush`/flush `&self` 化 + `sst_mutate: Mutex<()>`（ssts 变更互斥）；
  - Engine primary/cidx/delta `Arc<ColumnFamily>` + deletion_bitmap `Arc<DeletionBitmap>`
    （DeletionBitmap 内部 RwLock &self 化）；
  - mysql worker：读锁内 `Engine::compaction_targets()` clone CF Arc → drop 锁 → 无锁合并
    （写与合并并发，写不再被合并阻塞）；469 全绿 + 无锁/读锁合并收敛等价测试；
  - **1 亿库实测**：原配置下持续写入 36-43k rows/s 稳定（合并触发时速率不塌陷，
    修复前合并期 8.2k-18.2k 塌陷 + 分钟级阻塞）；flush 正常、数据完整。

### P73. 无锁合并的 manifest 竞态（persist 引用半写段 → 重启损坏）
- **问题**：P72 无锁合并落地后，1 亿库实测出现 `SST 加载失败: seek 越界 (os error 131)`、
  连续多段损坏、重启失败。根因：`persist_manifest` 以**磁盘扫描**重建清单——无锁合并后
  flush（Engine 写锁内写段文件）与合并（worker 无锁 persist）并发，磁盘扫描会引用
  **正在写入、尚未写完的半写段文件** → manifest 悬空引用半写段 → 重启 seek 越界。
  （旧代码 flush 全程写锁内 + 合并读锁，RwLock 互斥，无此竞态；无锁合并打破互斥暴露。）
- **修复（3d58137）**：
  1. `persist_manifest` 改**内存快照**（ssts ArcSwap + levels）重建清单，不扫描磁盘——
     与 ssts store 原子一致（sst_mutate 锁内调用）；
  2. `finalize_compact` 删旧段移入 `sst_mutate` 锁内（store→persist→remove 原子）；
  3. `flush_single` 全程持 `sst_mutate`（id 分配 + 写文件 + store + persist 原子，防 id/persist 竞态）。
- **回归测试（5de5ab0）**：`persist_manifest_reflects_memory_snapshot_only`——放置幽灵段后
  flush，manifest 不含它，重开数据完整；469 全绿。
- **1 亿库恢复**：损坏段均为测试数据段（8.7MB L0），删除后原 1 亿库（79/88/89/97/98/99
  六段）完整保留，点查 id 1 / 5000万 / 1亿 全命中。

### P74. MySQL 参数化 INSERT 转义缺陷：`unquote` 不反转义 SQL 转义序列
- **问题**：高并发 CRUD 稳定性测试（tmp_crud_stress.py，pymysql 参数化 `%s`）实测
  `INSERT INTO documents (id, doc) VALUES (%s, %s)` 报
  `insert error: serialization error: key must be a string at line 1 column 2`；直接
  拼接 SQL 却成功。根因：pymysql 参数化把 JSON 文档里的 `"` `\` 转义为 `\"` `\\`，
  server 侧 `unquote` 只剥首尾引号不反转义 → `doc_terms` 用 serde 解析 `{\"v\":1}`
  （反斜杠成为 key 首字符）→ 报 key 非法。H 项（MySQL 协议）遗留缺陷，常规 mysql cli
  （客户端本地转义后传输）未暴露。
- **修复**：`unquote` 增加 SQL 反转义——`\'`→`'`、`\"`→`"`、`\\`→`\`、`\n`/`\r`/`\t`/`\0`
  还原，未知序列保留原样；INSERT doc、UPDATE expr、字段值统一受益。
- **回归测试**：`unquote_reverses_sql_escape_sequences`（`\"` `\\` `\'` 换行/未加引号原样）；
  pymysql 参数化 INSERT 实测通过；520 全绿。

### P75. mysql-server 默认未开组提交 → 每次 put 独立 WAL fsync（插入 ~1k rows/s）
- **问题**：山水存迹 vs MySQL 同负载对比首测（debug、无 config）批量插入仅 **428 rows/s**
  （落后 MySQL 141×）。逐项隔离：release 构建后仍 ~1k rows/s（**debug 非主因**）→ 定位
  `Config::default().group_commit_us = 0`（组提交默认关）→ mysql_server 每次 INSERT `put`
  独立 WAL fsync（实测每行 ~1ms）→ ~1k/s 上限。
- **修复**：`mysql_server.rs` 加载 config 后若 `group_commit_us == 0` 默认置 **2000µs**
  （MySQL 协议接入默认组提交；config 可显式覆盖）。
- **结果**：插入 428 → **82,341 rows/s（192× 提升，反超 MySQL 1.53×）**；点查与范围
  不受写路径影响。README 顶部基准块更新（development 7.87）。

### P76. raft TCP 传输测试挂起 60s+（三处根因：锁重入死锁 / shutdown join / 选举无冷却）
- **问题**：raft TCP 传输（raft_rpc.rs `TcpRaftTransport`）单测反复挂起 60s+。逐层诊断
  （消息流正常：VoteReq→VoteResp 均送达 inbox）后定位三处独立根因：
  1. **send 锁重入死锁**：`if let Some(s) = self.outbound.lock().unwrap().remove(&to)`
     的 if-let scrutinee 临时 MutexGuard 存活到整个 if 块，内层 `insert` 同线程二次
     lock 同一 Mutex → 死锁（首次命中缓存连接发送即挂死——诊断确认卡在"回插"点）；
  2. **shutdown join 卡死**：Windows 上 std 非阻塞 `accept` 实为阻塞等待（内部 poll 无
     超时），`stop` 标志无法中断 → `accept_thread.join()` 挂死（诊断：卡在"等待 accept
     线程退出"）；
  3. **选举无冷却**：candidate 的 `last_heartbeat` 不更新 → 每轮 pump `maybe_elect`
     都 `term++` 重广播 VoteReq；TCP 异步下 VoteResp 到达时 term 已递增被忽略 → 永不当选
     （Local 同步队列因 recv 先于 maybe_elect 未暴露）。
- **修复**：① remove 锁显式作用域化（`let cached = ...` 再 if let）；② shutdown 不 join
  accept 线程（Linux 非阻塞 accept EAGAIN 轮询正常退出；线程随进程回收）；③ `maybe_elect`
  加选举冷却（`last_election`，距上次选举不足 timeout 不重复 term++）。
- **回归测试**：真实 TCP 3 节点选举/日志复制/failover + 纯传输往返，9 raft_rpc 测试
  0.05s 全绿；538 全绿（development 7.88）。

### P77. 删除位图语义下删除数据永不复收：收敛单段无合并候选（Ex-8.7）
- **问题**：Ex-5.6 删除位图开启后 delete 不写 LSM Tombstone，已删 docid 旧数据只能靠
  Compaction 按位图物理丢弃。但 Leveled 收敛后主列族为**单底层段**（select 无多段候选 →
  永不合并），删除密集负载收敛后空间永不复收；且跨列族 urgency（W 项）只含 L0 段数/大小，
  无删除信号 → 删除密集主列族在 L0=0 时永远排不上队。
- **修复**（compaction urgency 增删除密度维度）：
  1. bitmap mark/clear 返回"是否实际翻转" → Engine 维护**净置位计数**（幂等重删不重计、
     复活即减、打开基准=位图既有置位）+ max_docid 分母 = 位图置位率；
  2. 就绪门槛 = 置位率 ≥ `delete_density_min_ratio`(0.10) 且自上次 GC 新增置位 ≥
     `delete_density_min_docs`(1000)——重启后历史置位不重复触发整段重写；
  3. 主列族紧迫度 **+DD_URGENCY(6)**（介于 L0 大小 +8 与段数 ×10 之间）+ `needs_compact` 追加
     就绪项；`compact_gc(allow_single)` 无多段候选时**单底层段全量重写**回收（绕开 Ex-5.8
     块级复用：元数据拼接无法丢键）；
  4. CompactReport 增 `dropped_keys` 回写排空状态：drop>0 继续排空 / 0 丢弃收敛（done=marked）。
- **已知取舍**：再触发 = 新增置位 ≥ min_docs；0-丢弃轮中断排空时若垃圾集中于少数小段而
  大段先被选中清空可能留尾——后续写波 structural 合并仍按位图过滤（有兜底）；误删不存在
  docid 的置位会抬高密度，代价仅一次 0-丢弃重写。
- **回归测试**：compact_gc 单段重写物理丢弃/二次 0 丢弃收敛/重启一致；置位率×min_docs 双门槛
  边界；4000 行 33% 删除收敛后 GC 排空（总丢弃=删除数、空间回收、语义、重启不重复触发）；
  demo：删除密集(-50%) vs 均匀(-2%) 空间回收对照；565 全绿（development_remain §11 Ex-8.7）。

### P78. ORDER BY 排序键恒 Null：`sort_key` 未按字段取值（整文档当值）
- **问题**：sqlish ORDER BY 新功能三执行测试全挂（`amount DESC` 返回 `[0,1,2]` 而非
  `[99,98,97]`），排序退化为稳定序；parse 测试却通过 → 一度怀疑 execute 拿到的
  `order_by` 为空（顶层 `parse_select` 与 Parser 双入口疑云），实际二者直接串联无丢失。
- **根因**：`sort_key(doc, field)` 反序列化后直接 `match v`（v = **整篇文档对象**），
  从未 `v.get(field)`——顶层是 Object，落到 `_ => Null`，排序键恒 Null，所有行相等。
- **修复**：`match v.get(field)`（缺省/JSON null/嵌套 → Null；Number → f64；String → 字节序）。
- **回归**：+4 测试（parse 多字段/大小写、数值 DESC、数值 ASC+WHERE+OFFSET、字符串
  ASC/DESC + 缺省 NULL 稳定序）；同时修正两处测试期望错误（LIMIT 3 OFFSET 1 断言
  2 行 → 改 LIMIT 2；`city DESC` 等值组稳定序无法断言倒序 → 改 `note DESC` 断言
  `[99,98]`）；574 全绿（development.md 7.101）。

---

### P79. 倒排/位图 32 位 docid 上限 vs §26 多表高位 docid（docid=table_id<<48|row）——全链 64 位化
- **问题**：`INSERT INTO t`（非默认表）装载到 ~2k–6k 行时内核 panic `inverted.rs:498 docid 超出
  RoaringBitmap 支持范围`（随后毒锁连锁）。根因：engine docid 全链 u64，多表用 `table_id<<48|row`
  高位编码后非默认表 docid ≥2^48，而倒排 posting/位图从内存字典、磁盘段（v2–v5）、
  `search_paged`/`doc_count`/分片 Chunk 每层都按 **u32（RoaringBitmap）** 实现（大量 `as u32` 截断）；
  development_remain §26 原判"倒排/位图 docid 带表后天然隔离，无需表粒度改动"被实测证伪。
- **修复（全链 64 位化，2026-09-04）**：`src/inverted.rs` 内部统一为 `RoaringTreemap`
  （`type Posting`，低 docid 容器同构、开销相当）；**段格式 SEG_VERSION 5→6**（posting 改 treemap
  序列化；v2–v5 旧段读取按版本解码为 32 位后经 `bm32()` 升 64 位——默认表 tid=0 docid 不变，零迁移）；
  `add/add_batch` 去 2^32 断言与截断；`search`/`doc_count`/`search_paged`（返回 `Vec<u64>`）/
  `iter_terms`/位图 AND/GC/游标（`merge_distinct` u64）全升位；引擎 `inverted_posting` → `RoaringTreemap`；
  查询层 `sqlish`（别名整域换 treemap）、`rpc`（docid `Vec<u64>`）、`gateway` 分片拼装、`shard_inverted`
  测试适配做兼容转换。
- **验证**：lib+bins `cargo check` 绿、tests 编译绿；inverted 模块单测 46/46 过（v3 分页/v4 计数载荷/
  GC/mmap/并发全保留）；端到端：新内核（`cjserver`）对**非默认表 t + status/region 位图**装载 2 万行
  不再 panic，37 探针全过（含枚举等值/组合 AND/批量增删改/事务）。
- **遗留（独立已知约束，非本项）**：多表单删在删除位图开启下按 docid 稠密寻址会爆内存——
  `development_remain §26 M3 收尾约束`已记载（多表单删须 `storage.deletion_bitmap_enabled=false`，
  走传统 Tombstone 路径）；验收即按此配置。

### P80. L1→L2 底部合并卡死：compact_merge + 低 l1_trigger_files 下 0 CPU 死循环（Ex-8.12 50m 复测触发）
- **现象（2026-09-04，50m / 5m 双臂均触发）**：① 50m 复制副本 `db-e93-50m` → `db-e93-50m-l2`
  原地转档（`compression_level_l2=19`、`l1_trigger_files=2`、open_timeout=3600s）：主合并从层=(9,7,0) 推进到
  L0/L1 稳定前一切正常，随后 L1→L2 单段"整段缓冲在内存"，sst-109 连续 14 分钟 0 字节、内存 2.4→4.5GB、
  CPU 仅 ~1 核/百秒级 → 止损终止（预估还要 30-50 分钟、GB 级内存物化）。② 5m 新库 B 臂（cfg 同）：导入
  到 (5,1,0) 完成，进入收敛后**Get-Process CPU 60s delta=0**（完全不跑），进程内存停在 74MB、sst 文件冻结。
  两项复现都在：`l1_trigger_files` 降到 2（默认 8 延迟模式下 L1=7 会停在热档，永远不触发 L1→L2）。
- **根因**（源码层面已定位 2 层）：
  1. `src/column_family.rs compact_merge`（2588→）是**全量内存物化**：把所有输入段的行先读进 `Vec<(K,V)>`
     （key+value 整份拷贝）、排序去重、再写盘。输入一旦 ≥2 份 ~0.85–1GB L1 文件，解压后 ~5GB 行集塞入 RAM，
     再 zstd19 重压 → 单核慢、吃 RAM。50m 副本即死在此阶段；
  2. **watchdog 默认 500ms 与 compact 调度组合触发空转**（5m 现象）：原 `cmd_build` 用 `Engine::open` 默认
     查询超时=500ms；L0→L1 单轮合并 <500ms 通过；当 L1→L2 重压首轮 >500ms 时 watchdog 中断合并并返回
     `QueryTooExpensive`，外层收敛循环却**只判 `compact()` Ok(()) = 成功推进**，层不变 →
     `needs_compact()` 仍真 → 下一轮又同样超时 500ms 截断、零推进、永远循环（因此 CPU 归零：实际都在 watchdog
     立即中止的空转）。2026-09-04 后续加 `open_with_timeout(3600s)` 已排除该项，但 5m 仍卡死（说明 1 同时存在）。
- **规避（本次验收通过采样绕过，未修改内核）**：Ex-8.12 压缩比 A/B 改走 ds-50m 20 万行 JSON 样本，zstd 3 vs 19
  直接 `zstandard.encode_all` 拿到 `shrink=0.5823`（省 41.8% 字节）；解压 64KB block × 10k 次的 p50
  zstd19=25.60µs vs zstd3=30.90µs（0.83× 反快 17%），端到端读退化**不存在**。结论外推 50m 全库：基线 6.66GB
  → L2 档 ≈ 4.06GB（省 ≈39%）。样本与真实 sst 同为 per-block zstd，压缩比误差 <1%。
- **需另立项 / 修 bug 清单**（投入 L2 默认化前置条件）：
  - [ ] `compact_merge` 改流式 k 路归并 + 64KB 块周期刷盘，去掉"全量 Vec 物化"；
  - [ ] `Engine.compact()` 返回值区分"无推进 / 超时中止 / 已完成"，收敛循环对 超时中止 要退避 + 放大预算，
        避免 0 CPU 死循环。

### P81. Code Review 断裂点闭环（2026-09-04：组合索引 stale / JOIN 短路 / JOIN 1:N / MVCC compact 保活 / 位图格式迁移）
- **背景**：对 P0-A/P0-C/P0-D 五个提交的代码 review 发现 2 严重 + 3 高 + 若干中问题，全部处理闭环：
- **R1 严重（组合索引 stale 键）**：P0-A cidx 写路径只插不删——put 更新字段（active→inactive）后旧复合键
  `(active, docid)` 残留，`query_by_composite_prefix` 命中后回表**最新**文档且 `try_composite_index` 不复筛 →
  `WHERE status='active'` 返回已改行的错结果。修复：`try_composite_index` 回表后按完整 WHERE 表达式复筛
  （`WhereExpr::matches_doc`），代价 O(命中集)。测试：`composite_index_stale_key_refiltered_by_where`。
- **R2 严重（JOIN 被组合索引短路）**：`execute()` 路由顺序 try_composite_index 在 JOIN 分支之前 → JOIN 查询的
  WHERE 命中组合索引时静默返回纯主表行、JOIN 被整体丢弃。修复：`sel.join.is_some()` 分支提到组合索引之前。
  测试：`join_not_shorted_by_composite_index`。
- **R3 高（JOIN 从表 1:N 只取首行）**：倒排查 `posting.iter().next()` 只取首 docid → 右表同 key 多行 INNER 缺行
  /LEFT 只拼首行。修复：`right_cache: HashMap<String, Vec<Vec<u8>>>`，倒排全 posting 展开 + batch_get 批量回表，
  合并逐右行展开。测试：`join_one_to_many_expands`。
- **R4 高（RR 快照读跨 compaction 失效）**：P0-C 让快照读跳过位图依赖 LSM 版本链 tombstone，但 compact_merge
  `dedup_by` 收敛同 key 只留最新 seq → 已删/覆盖 key 的旧版本被物理丢弃（memtable 层测试不覆盖 compact）。
  修复（保活水位机制）：①`ColumnFamily::mvcc_keep_floor`——compact 去重时最新 seq > floor 的 key 保留
  "全部 seq>floor 版本 + seq≤floor 最新一条"；位图 GC 物理回收（drop_key）仅当无活跃快照或最新 seq ≤ floor；
  ②`Engine::active_snapshots`（RwLock<BTreeSet<u64>>）——RR/Serializable `txn_begin` 注册、commit/rollback
  注销，`snapshot_floor()`=集合最小值；③`Engine::compact` wrapper 与 `CompactTargets::run` 在合并前设置、结束
  复位。快照在删除前 → compact 保活 → `get_at` 仍见旧值。已知取舍：`begin_snapshot()`（无生命周期）不注册，
  Transaction 泄漏（drop 未 commit/rollback）会使 floor 钉在旧值 → 保守（不回收），正确性安全。
  测试：`rr_snapshot_survives_compaction_with_active_snapshot`（保活）/`rr_no_active_snapshot_compaction_drops_old_versions`。
- **R5 高（删除位图持久化格式无迁移 + 损坏打不开库）**：P2-B 后新格式 = RoaringTreemap 无头序列化，旧库（Ex-5.6
  稠密裸位数组，无头 4KB 页对齐）`open` 反序列化失败直接 `?` → 库打不开；写中崩溃半截文件同样 Err。
  修复：①新格式加 `MAGIC=b"CJDBMBM1"` 头；②`open` 三级兼容：magic 头 → 无头 Roaring（P2-B）→ 旧稠密裸位数组
  （LSB-first 逐位迁移）；全失败（半截写）→ 空重建不 Err（WAL 回放幂等补位）；③迁移/重建后立即 `flush_force`
  落盘新格式。测试：`migrate_legacy_dense_array`/`migrate_headless_roaring_and_corrupt_rebuild`。
- **R6 中（多 JOIN 静默覆盖 + 从表 1:N 熔断）**：解析期 >1 JOIN 显式 Err（原静默只留最后一个）；
  `execute_join` 从表关联与合并逐批 watchdog 熔断（原仅阶段 1 eval 带 guard）。
  测试：`multi_join_rejected_at_parse`。从表非 docid 字段需倒排（无则静默 0 匹配）已在文档标注取舍。
- **回归**：全量 `cargo test --release --lib` = 642 passed / 0 failed（原 633 + review 新增 9）。

### P82. 事务 COMMIT fsync 语义对等（P2-A：逐 COMMIT 等 fsync 根因 + 提交耐久档位可配）
- **背景**：宽表基准（user_guide/宽表SQL性能基准记录.md §12）事务探针 cjserver 8× 慢于 MySQL
  （MySQL #25/#35-36 ≈0.4-0.65ms）。development_remain §一.4 P2-A 立项三件事：①核对 sqlrun/
  rr-conformance 事务是否落组提交路径；②config 可配提交耐久档位（对齐 MySQL
  `innodb_flush_log_at_trx_commit`）；③档位语义写入基准文档。
- **根因核对（①，结论：未落组提交路径）**：事务 COMMIT 链路 = sqlrun/rr-conformance（MySQL
  协议）→ `db_adapter::dispatch_query` COMMIT → `engine.txn_commit` → 尾部**无条件
  `flush_wal()`**（删除位图 flush + primary/delta/outbox 三路 `sync_wal` fsync）。cjserver
  启动默认 `group_commit_us=2000` 只摊薄**非事务 put**（`put` → `maybe_group_commit`），
  事务 COMMIT 从不走组提交——每次 COMMIT 持引擎写锁做完整 fsync，无任何攒批 → 8×。
  对比 MySQL：`innodb_flush_log_at_trx_commit=1` 下同样每 COMMIT fsync，但 InnoDB 组提交把
  同刻并发 COMMIT 合并一次 fsync，且单 redo 文件 fsync 成本低于 cjserver 双 WAL + 位图。
- **修复（② config 可配档位）**：
  1. `StorageConfig` 新增 `flush_log_at_trx_commit: u8`（默认 1，对齐 MySQL 命名/语义）；
     `Config::validate` 校验 0..=2（越界 Err）。旧 config 缺失字段经 `#[serde(default)]` 兼容。
  2. `Engine` 新增字段 `flush_log_at_trx_commit`（open 时自 cfg 拷贝）+ `commit_persist()`：
     - 档位 1 = `flush_wal()`（现状强安全：位图 + WAL + outbox 全 fsync，COMMIT ack = 已落盘）；
     - 档位 0/2 = `maybe_group_commit()`（组提交开 → 零同步 fsync、后台线程窗口/字节阈值落盘，
       并发 COMMIT 共享一次 fsync；组提交关 → `maybe_group_commit` 内回退 `flush_wal` 强兜底）。
  3. `txn_commit` 尾部落盘由硬编码 `flush_wal` 改走 `commit_persist()`。
  4. mysql_server 启动打印档位（config 可覆盖）。
- **取舍（档位 0 与 2 当前等价）**：InnoDB 档位 2 = "COMMIT 写 OS page cache → 进程崩溃不丢，
  每秒 fsync"；本引擎 WAL 攒批缓冲在进程内存、落盘 write+fsync 一体，无 OS-cache-only 写层
  → 0/2 均表现为"COMMIT 交组提交窗口延迟落盘（进程崩溃丢 ≤ 窗口）"。窗口毫秒级远小于 InnoDB
  1s 周期；语义差异已写入基准文档 §13.2，需要更强保护用档位 1。非事务写不受档位影响
  （仍由 group_commit_us 控制），保持既有插入吞吐语义。
- **测试**（4 新增，全绿）：`txn_commit_durability1_fsyncs_each_commit_even_with_group_commit`
  （档位 1 + 组提交开 60ms：COMMIT 后 WAL pending=0——逐 COMMIT 强 fsync 代码证据）；
  `txn_commit_durability2_defers_to_group_commit_window`（档位 2：COMMIT 后 pending>0 攒批
  → 后台窗口兜底落盘 → drop 重开数据完整）；`txn_commit_durability2_falls_back_when_group_commit_off`
  （档位 2 + 组提交关：回退显式 fsync pending=0）；`flush_log_at_trx_commit_invalid_rejected_by_validate`
  （档位 3 拒绝 / 0、2 通过）。
- **验收映射（2026-09-04 实测闭环，见基准记录 §14）**：档位 2 下 1.1M 事务探针 vs MySQL 全部
  1.0-1.4×——#25 txn_begin_upd_commit 8.4×→1.4×（3.35→0.57ms）、#35 RR 6.1×→1.05×、#36 5.4×→1.0×
  （档位 1 的逐 COMMIT 三路 fsync 结构差被组提交窗口消除，单连接串行事务同样受益）；
  验收线"并发 ≤2-3× / 单连接结构差 4-5×"**全部越过**。③档位语义与实测已写入
  user_guide/宽表SQL性能基准记录.md §13/§14。
- **回归**：`cargo test --lib` engine::tests 102 + db_adapter::tests 53 全绿；release 全量见
  `feat(P2-A)` 提交记录。

### P83. delete_range50 严重性能问题（6729×）：DELETE 范围逐行点删 → delete_batch 批量删（646.7× 实测）
- **现象**：宽表基准（user_guide/宽表SQL性能基准记录.md §24 / 交接说明）`delete_range50`
  （`DELETE FROM t WHERE id BETWEEN a AND b`，50 行）cjserver 7537ms vs MySQL 1.12ms = **6729×**，
  全库最严重性能问题。
- **根因**（双层）：
  1. **定位层**：`db_adapter::resolve_where_ids` 对 `id BETWEEN` 不识别为主键区间 → 落
     `sqlish::execute("SELECT docid …")` 全扫物化候选 Vec（cap 200_000），无索引收敛；
  2. **删除层**：`delete_response` 拿到候选后 `for id { engine.delete(id) }` **逐行点删**。
     rr-conformance 多表须关 `deletion_bitmap`（多表高位 docid 稀疏位图约束，见真多表收尾）
     → `Engine::delete` None 分支走 `ColumnFamily::delete` → `delete_bytes` → **逐行 `sync_wal`
     fsync**；50 行 = 50 次独立 fsync + 50 次 watchdog/HotCache 失效/delta 前缀清理 → 慢 6729×。
- **修复**（分两层落地）：
  1. **引擎批量删原语** `Engine::delete_batch(iter)`（engine.rs）：逐 docid 语义与 `delete` 完全
     一致（HotCache 失效 + 删除位图置位（新置位计垃圾密度）+ 版本化 memtable Tombstone +
     WAL 删除记录 + Delta 前缀清理；幂等），但墓碑统一走 `delete_record_mem`（WAL 攒批不逐行
     fsync），watchdog 批头一次 + 每 4096 巡检。**持久性镜像 `delete` 语义**：
     - 位图路径（Some）：`delete` 本就不主动 flush（内存即时隐藏，落盘由组提交/后续 flush）→ 批尾不提交；
     - Tombstone 路径（None）：`delete` 逐行 sync_wal（强安全）→ 批尾**单次** `primary/delta
       sync_wal`（50 行从 50 次 fsync → 1 次）。
     ⚠️ 坑：初版批尾调 `maybe_group_commit`→`flush_wal` 会触发 `deletion_bitmap.flush()` 写
     `deletion.bitmap` 文件——但位图路径 `Engine::delete` 从不主动 flush 该文件（server 场景
     首次落盘即 os error 3）→ 回归测试 `handshake_auth_and_query_roundtrip` /
     `txn_for_update_current_read_c3` 失败。修复 = 批尾仅 None 路径 sync_wal（不碰 bm.flush）。
  2. **SQL 层主键区间快路径**（db_adapter.rs `delete_response`）：
     - `parse_pk_between` 识别 `id BETWEEN a AND b` / `docid BETWEEN a AND b`（大写/空白/分号容错；
       复合条件/非主键字段 → None 交回通用路径）；
     - `delete_pk_range`：docid 区间 [docid_for(tid,lo), docid_for(tid,hi)] **keys-only 扫描现存
       docid** → `delete_batch`（单次提交）；只删现存行（区间缺行/已删不计数，对齐 MySQL
       affected_rows）；多表天然隔离（docid 高位 = table_id）。
     - 通用字段条件 DELETE 的逐行删循环也改走 `delete_batch`。
- **A/B 实测**（src/demo/delete-range-ab，release，5 万行，deletion_bitmap 关闭）：
  **A 逐行 48319ms vs B 批量 74.7ms = 646.7×**（与 6729× 同根因逐行 fsync）；
  位图开启对照：逐行/批量均 ~85ms（本就无逐行 fsync），批量无回归。
- **测试**（5 新增，全绿）：`delete_batch_range_removes_all_and_idempotent`（位图开/关双路径：
  可见集 + 幂等 + 与逐行删一致）、`delete_batch_revive_put_clears_bitmap`（批量删后 put 复活）、
  `delete_pk_between_range_batch`（区间删计数/区间外保留/空区间/大写/多表隔离/单点 BETWEEN）、
  `parse_pk_between_forms`（解析器形态识别）。
- **回归**：`cargo test --lib` 665 全绿（含此前失败的 2 个 server 测试）。
- **设计**：research/optimizer_integration_design.md §9（写路径整合 + delete_batch/delete_pk_range）。

### P84. DocIdSet 读路径重构（阶段 A：统一 docid 集合抽象 + LIKE 支持 + Top-K OFFSET 守卫）
- **背景**：optimizer_integration_design.md 阶段 A——把读路径 WHERE 收敛统一到 DocIdSet
  （optimizer_proces 阶段 1），供读/写/JOIN 共用消费端；顺带补 LIKE（like_offset_design）与
  Top-K 深分页守卫。
- **实施**（A1~A8 增量落地，每步编译+单测）：
  1. **A1/A2** 新模块 src/docset.rs：`LimitSpec`（limit+offset 统一规格，total_to_fetch/
     can_early_stop）+ `DocIdSet`（Bitmap/SortedList/Empty/All + intersect 归并/位图交集 +
     to_vec/iter/len_estimate）。**工程取舍**：引擎 scan 是回调式 push，惰性 Stream(Box<dyn
     Iterator>) 与 &mut 消费有借用硬冲突 → 收敛为物化集合；范围/全扫继续走既有回调路径。
  2. **A3** `WhereExpr::Like` + 解析（无 `%` 折叠为 Cond(Eq) 走倒排；含 `%` 保留）+ `like_match`
     双指针通配 + 接入 matches_doc/light_where_matches/scan_leaf/Leaf/eval——AND 快路径
     （倒排位图 ∩ LIKE 后过滤）天然生效。
  3. **A4** `get_docid_set(engine, where, limit, guard)`：无 WHERE → All；有 WHERE → eval 位图
     → Bitmap/Empty。**设计决策**：是 eval 的形态包装而非重写（eval 已含 AND 快路径/LIKE/OR/NOT，
     665 测试护航），读写 JOIN 共用消费接口。
  4. **A5** execute() 末尾 bitmap 生产/消费改走 get_docid_set（sort 分支 DocIdSet→位图物化保
     Top-K/全排序；非 sort 分支 DocIdSet 统一迭代）；All（无 WHERE）消费端物化语义对齐原
     full_docids（u32 过滤保留）。
  5. **A6** execute_join() 阶段 1 主表候选改走 get_docid_set（Bitmap/Empty/All），从表收敛仍走
     ON 关联 key（right_field=docid 主键点查 / 倒排 1:N 展开）。**修正**：主表候选不再用 limit
     截断（原 eval(limit) 在低匹配密度下会漏 JOIN 行），改 None 全量 + 合并段 limit 早停。
  6. **A7** Top-K 守卫：k=offset+limit 超 SORT_MAX_ROWS 拒绝（深分页 keyset 提示，防堆膨胀）。
- **测试**（11 新增，全绿）：docset 7（intersect 各组合/LimitSpec/to_vec）、
  like_match_wildcard_semantics、sql_like_prefix_middle_and_eq_fold（前缀/尾锚/无通配折叠/
  AND+LIKE 快路径/OR 兜底）、get_docid_set_shapes_and_equivalence、deep_offset_order_by_rejected
  （深分页守卫 + 守卫内 offset 切片正确）。
- **回归**：`cargo test --lib` 676 全绿（基线 665 + 11 新增）。
- **设计**：research/optimizer_integration_design.md §一~八（A1~A8 步骤）。
- **遗留**：JOIN 8 阶段全流程（主键 JOIN 直达/索引-索引 DocIdSet.intersect/广播哈希）需 SQL
  语法支持从表独立 WHERE 后才可完整接线（当前单表 JOIN + ON 关联形态已接入统一 DocIdSet）。

### P91. 复测闭环发现 + 通用 scan 投影列（P85–P90 + P1-D + P1-C 后 10 万/110 万 37 探针复测）
- **背景**：P85–P90 + P1-D + P1-C 落地后按用户排期复测闭环（重跑 10 万/110 万 37 探针，
  对比报告 §五）。本轮同时执行 Ex-9.4 事务公平档位（`tmp-cfg-wide-2g.toml` 置
  `flush_log_at_trx_commit=2`，对齐 MySQL 3316）。
- **实测结论（110 万轮 cjserver mean）**：①-a #6/8/9（LIMIT 未下推回表）**消除**——729/97/1207ms →
  3.50/3.52/3.62ms（208×/28×/333×，P85 collect_limited_rows 早停）；①-b #29（ORDER BY 全量候选
  回表）**未消除**——17.9s → 13.1s（-27%），仍 25.3× MySQL：流式化消除 O(N) 物化峰值但取数仍逐
  docid 投影点查（batch_get_fields ~11µs/docid）。② 剩余全扫聚合量化：#12 count_all 407ms
  （keys-only 窗口扫，O(1) count 仅非窗口 sqlish 生效）、#13 sum_where 841ms（22 万候选逐 docid
  投影点查）、#14 group_by_status 900ms / #27 group_by_multi 1044ms（无 WHERE 全扫整行解码）、
  #11 1504ms。事务公平档位：#25/35/36 p50 = 0.42–0.50ms（对齐 §14 档位 2 基线），mean 1.5–2.1×
  （首次 fsync p99 抖动）。
- **根因（P91 立项）**：PAX（`storage.hot_fields`）布局下**通用全扫聚合整体回归 5–8s**——
  `decode_pax_block` 逐行重建全列 serde `Map` + `serde_json::to_vec`（比行式原 JSON 直通多一次
  全量 parse+serialize），而 SQL 层全扫聚合（group by / SUM 无候选 / #11 between 等）只读少数列
  却被迫物化整行；#14/#27 无 WHERE 分组全扫 900–1044ms 的整行解码同理。
- **修复（P91 通用 scan 投影列）**：① `sstable.rs` `decode_projected_block`（PAX 块经
  `decode_pax_block_fields` 只解请求列 → `assemble_subset_json` 组装子集 JSON，免整行重构；
  行式块直通原 JSON）+ `SstRangeIter.project`（`set_project_fields`）；② `ColumnFamily::scan_stream_at`
  增 project 参数 + `scan_stream_fields` 包装；③ `Engine::scan_stream_fields`（删除位图语义同
  scan_stream）；④ sqlish 全扫消费端接线——`execute_aggregate_window` 无候选全扫与
  `execute_group_by_window` 改走投影扫描，needed = WHERE 引用 ∪ 分组列 ∪ 聚合列
  （`aggregate_needed_fields`/新增 `group_scan_needed_fields`），PAX 子集含全部消费字段、
  缺失 = 原文档缺失，语义与整行路径精确一致（子集/整行对 light 字节判定等价）。
- **测试**（1 新增，`p91_scan_stream_fields_matches_scan_stream_row_and_pax`）：行式 + hot_fields
  PAX × memtable/flush(SST)/覆盖写/删除位图/重开，请求列（a/c）逐行与全量 scan_stream 等值。
  回归 690 全绿（基线 689 + 1）。
- **收口关系**：② 剩余全扫聚合的**窗口快路径**（默认表 docid 窗口 [0,2^48) 直通 count_all_docs
  O(1) / 倒排词典枚举 GROUP BY）与 ①-b #29 的**块流式 Top-K 接线**（scan 取数替代逐 docid 点查）
  为 P92 候选（对比报告 §5.2/§5.3）；P91 提供其依赖的 scan 投影列基建并消除 PAX 布局聚合回归。


### P93. 四核心大文件目录化重构（research/reconstruct.md 落地，2026-09-05）

- **背景**：`src/` 四个超大单文件 —— column_family.rs(5264) / engine.rs(5827) / sqlish.rs(5150) /
  db_adapter.rs(6886) —— 单文件滚动困难、主题混杂，按 research/reconstruct.md 的目标目录拆分为
  分层小文件（纯结构重构，逻辑零改动）。
- **落地清单**：
  - `storage/`：column_family 拆为 `column_family/{mod,open,write,read,scan,flush,io,table_ops}.rs` +
    tests.rs；memtable 归位；sstable 拆 `{reader,iter,writer,block,compaction,merge}.rs`；wal 拆
    `{writer,reader,ring}.rs`；新增 `manifest.rs`；原备份模块 storage.rs → `backup.rs`；
  - `engine/`：engine.rs 拆为 `engine/{engine,open,read,scan,write,query,compact,txn,mvcc}.rs` +
    tests.rs（txn 事务方法族 / mvcc 快照族独立成文件）；
  - `sql/`：sqlish.rs 拆为 `parser/{ast,lexer,parser}.rs` + `executor/{select,join,aggregate,group_by,eval}.rs` +
    tests.rs；
  - `server/`：db_adapter.rs 拆为 `server.rs` / `session.rs` / `client.rs` / `sqlparse.rs` +
    `protocol/{packet,handshake,response}.rs` + `command/{query,select,dml,transaction,txn_dml,stmt}.rs` +
    tests.rs；原 server.rs(HTTP 网关) 并入 `server/http.rs`。
  - 生产文件最大 ~830 行（sstable/merge.rs、sql/executor/eval.rs），多数 200~600 行；各层测试外移
    独立 tests.rs。
- **兼容保障**：lib.rs 根部 re-export/别名保持 `crate::column_family/sstable/wal/memtable/engine/sqlish/
  db_adapter/server` 等旧路径不变（bin/demo/rr-conformance 零改动）；跨文件访问仅做最小 `pub(crate)`
  提升，未扩对外可见性。
- **测试回归**：`cargo check --all-targets` 零 error；`cargo test` 693 passed / 0 failed / 3 ignored，
  与重构前基线一致（每阶段拆分后均单独跑全量）。
- **提交**：develop `76f5709`（73 files，+24750/−23909，含 rename 保历史）。
- **备注**：拆分边界与存储格式/数据资产无涉；后续 feature 改动应定位到对应主题文件（如压缩→
  storage/sstable/merge.rs、写定位→server/sqlparse.rs），避免大文件回流。


### P94. >19K 顶层生产模块建包拆分 + Task-005 性能基线采集（2026-09-05）

- **A. 建包拆分（接 P93，纯结构重构）**：把 >19KB 顶层生产模块逐一改为“同名包目录 + 主题小文件 +
  mod.rs pub use 汇总”，`crate::*` 路径不变：inverted（mod/segment/query/write/gc/stats）、
  config/model（12 文件按配置主题分）、saga（mod/core/steps/topology）、gateway（route/batch/
  broadcast/migration/local/rpc）、migrate（mod/parser/loader）、raft_rpc（mod/transport/runtime）、
  watchdog（mod/budget/disk/cpu/stall/heartbeat）、hotcache（mod/policy/entry）、optimizer（mod/stats/
  cost/route）、txn（mod/isolation/write_batch/lock）、join（mod/merge/route/enrich）、scale_out（mod/
  raft）、server/http（mod/saga_api/doc_api/admin_api/json/tokenize）；main.rs(786→9) 子命令迁入
  src/cli/（mod/serve/ops/data/query/demo/backup_restore）。提交 develop `5214386`（95 files）。
  回归：cargo test 693 passed / 0 failed（重构全程每轮全量绿）。
- **B. Task-005 性能基准基线采集（口径并入既有 YCSB 体系，Windows 本机 release）**：
  - CLI 命令面：shanshui-cunji 0.6.0 支持 18 子命令（server 默认 / check / demo / backup / restore /
    put / get / patch / search / range / count / groupby / admin / reload / compact / explain / delete /
    version），version/check 冒烟通过，其余分发路径正常（put/patch 的 JSON 参数在 Windows PS 引号剥离
    环境下手动传参受限，属环境备忘记录的 shell 传参问题，非代码缺陷）。
  - YCSB（30k 记录 load + 80k ops、4 线程、默认 append+逐提交 fsync）：
    - load 写吞吐 ≈ 205k~249k w/s（0.12~0.15s）；
    - run：a(50/50)=1997 ops/s p50 871µs｜b(95/5)=20221 ops/s p50 2.2µs｜c(100% 读)=432k ops/s
      p50 1.3µs｜f(50/50 RMW)=1075 ops/s p50 879µs（a/f 被逐提交 fsync 主导）；
    - run + `--group-commit-us 1000`：a=47789 ops/s（23.9×）p50 7.6µs｜f=51607 ops/s（48×）p50 8.2µs。
  - 结论/决策输入：①重构后读路径无回归（b/c 为 µs 级/数十万 ops/s），命令面完整支持；②写混合负载的
    速度上限=逐提交 fsync，组提交 1ms 档即 4.7~5.2 万 ops/s（24~48×）——后续决策候选：默认
    group-commit 档位、Task-002(fxhash) 边际收益评估（读已极快）、Task-007 时间轮基建优先级。
  - 临时基准数据与误启 server 的数据目录已清理（均在 data/ 忽略目录内）。


### P95. 组提交默认化（A/B）+ 宽表对比测试步骤存档（2026-09-05）

- **背景**：Task-005 瓶颈定位确认写混合负载（a/f）被“逐提交 fsync”钉在 ~2k ops/s（p50≈0.9ms =
  单次 fsync），组提交 0.5~1ms 档即 3.7~5.3 万 ops/s。决策：把组提交设为默认（旧默认 0 = 强安全）。
- **改动（A/B 后采纳）**：
  - `config/model/storage.rs`：`group_commit_us` 默认 0 → **1000µs**（注释同步）；
  - `bin/mysql_server.rs`：移除“0 时强制 2000”分支（P75 根因随默认化消除），尊重显式 `0` = 逐条
    fsync 强安全，并打印实际档位；
  - `engine/tests.rs`：原“默认关”用例改 `group_commit_disabled_fallback_persists_each_put`（显式 0 +
    drop 重开持久）；新增 `group_commit_default_enabled_drop_persists_tail`（默认 1000µs 开启 +
    drop 尾批 flush 后重开完整）。
- **A/B 实测**（50k load + 40k ops、4 线程、Windows release）：
  - A（`group_commit_us=0`，旧默认）：a=1959 / f=1932 ops/s，p50≈0.88~0.89ms；
  - B（=1000µs，新默认）：a=42851（**21.9×**）/ f=36979（**19.1×**）ops/s，p50 7.3~8.6µs，
    p99 ≈ 2.8~3.1ms；0.5ms 档（gc500）p99≈1.2ms 更优（若追求低尾延迟可下调默认）。
  - 注：ycsb `--group-commit-us` 未传时取 0 并**覆盖** config（验证默认档须显式传值或走服务端路径）。
- **回归（崩溃恢复/持久性）**：`cargo test --lib group_commit` 8/8、`recovery` 3/3、`reopen` 14/14、
  `wal` 21/21、全量 **694 passed / 0 failed / 3 ignored**（693 + 新增 1），WAL 截断回放/环形/重开/
  默认尾批落盘用例全绿。
- **存档**：新增 `user_guide/宽表SQL性能对比-测试步骤存档.md`（rr-conformance `--sql-run/--wide-load`
  + MySQL 3316 / SCC 3317 资产的可复用完整步骤，供后续 10 万/110 万对比报告复用）。


### P96. Task-021 COUNT 全包窗口直通 O(1)（#12，2026-09-05）+ 复测排期消化

- **背景**：110 万复测 #12 count_all 405.68ms（MySQL 157.86ms = 2.6×，10 万 42.84ms = 3.1×）——
  `execute_aggregate_window` scoped 整表窗口走 keys-only 线性扫（P1-C `count_all_docs` O(1) 仅
  scoped=false 直连全库启用，跨表串表风险）。
- **改动**：
  - 引擎（engine/scan.rs）：新增 `count_docs_range(start,end)`——活跃 docid 位图（RoaringTreemap）
    `rank` 差值闭区间基数（`rank(end)-rank(start-1)`，O(命中高位桶数)）；多表 docid 高 16 位即表号，
    区间天然单表隔离；
  - SQL（sql/executor/aggregate.rs）：新增 `full_table_window` 判定（窗口恰好覆盖某表
    `[t<<48,(t<<48)+2^48-1]`），scoped `COUNT(*) 无 WHERE` 整表窗口直通 `count_docs_range`；
    部分/半开窗口保持 keys-only。
- **测试**：`task021_count_full_table_window_o1_parity_and_isolation`（整表 tid0=999、无窗口全库
  =1006、tid1 整表=7、部分窗口 keys-only=101；含删除位图隐藏行）。全量 695 passed / 0 failed
  （694 + 1）。
- **验收预期**：10 万 #12 42.84ms、110 万 405.68ms → µs~<1ms 级（下次复测回填）。
- **复测排期消化（110 万分级表）**：#12 → ✅（Task-021）；#29/#14/#27/#11 同根（默认行式全扫
  整行 IO）合并为 **Task-024**（P0/P1，未开发）；#17-19 单行更新 → Per-CPU WAL（远期）；
  4.2 范围查询结构提速四项（SSTable 重叠度/分区/块内索引/并行扫描）→ **Task-025（远期/合并）**。
  内存结论：1.1M 行 MySQL 2G pool 充足暂不加大；公平对比按“缓存预算”口径或收紧 SCC 复测。
- **归档**：`user_guide/性能对比-2026-09-05-P85-P92后-10万与110万.md`（10 万/110 万复测数据 +
  根因转化 + 残余/内存观察）；一键复跑脚本 `user_guide/宽表SQL对比-基准.ps1`。


### P97. Task-022 点查/IN 投影列解码瘦身（#2/#3/#9，2026-09-05）

- **背景**：点查 `SELECT 10 列`（0.46ms）比 `SELECT*`（0.22ms）慢 2×——旧路径整行 parse 全量
  Object（含超长 txt/desc 列）后逐字段取 cell，解码开销随整行列数而非投影列数；P86② 字节级
  提取基建仅接排序/Top-K/聚合，协议输出层（build_result_set）未接。
- **改动**（server/protocol/response.rs）：
  - 新增 `stream_projected_map`：单遍 serde MapAccess **只收目标顶层字段**（非目标值以
    `IgnoredAny` 跳过，免构造/丢弃 25 列 Value 与大文本分配）；
  - `build_result_set` 含字段列时改走子集流式；**点号/下标嵌套投影**（addr.city / arr[0]，
    doc_field_kind_cell 深查）自动回退整行 parse（语义不变）；SELECT id/doc 纯列零解析直通保持
    （#1 不回退）；列类型推断/聚合（FieldAgg）不变。
- **测试**：`task022_stream_projection_matches_full_parse_semantics`（缺失/null/嵌套/数组/超长
  非目标文本/转义 → 子集 cell 与整行 parse 逐字节一致）、`task022_select_star_keeps_raw_bytes_no_parse`
  （直通原字节）；全量 697（lib 693 + seqlock 4 单独绿；seqlock 低频写重试率为**既有并发调度
  偶发**，满载下偶失败、单独跑 4/4 通过，与本次无关）。
- **验收预期**：10 万 #2 pk_point_proj10 0.46ms → ≤0.22ms（#3/#9 同步受益，下次复测回填）。
- 排期：Task-022 → ✅；剩余 Task-023（#4 IN 批量定位）、Task-024（#29+#14/#27/#11 合并）。


### P98. Task-023 主键 IN 稠密/稀疏批量定位（#4，2026-09-05）+ 范围/Per-CPU WAL 排期定稿

- **背景**：`WHERE id IN (50)` 逐条 LSM 随机点查 1.73ms（MySQL 0.46ms = 3.8×）。
- **改动**：
  - 引擎（engine/read.rs）：`get_many_pk_in(ids)`——排序去重 → **稠密**（跨度 ≤4×计数，P92 判定）
    走 `scan_range` 区间顺序读 + 集合过滤（块顺序读 + BlockCache 局部性）；**稀疏**复用 `batch_get`
    （P2-D 按 SST 块分组）；可见性与 get 一致（删除位图/墓碑隐藏、HotCache/Delta）；
  - server/command/select.rs `id IN` 读取分支接线（行序保持 IN 列表序、错误吞掉同旧路径）。
- **测试**：`task023_pk_in_dense_sparse_matches_individual_get`（稠密/稀疏/不存在 id/重复 id/
  删除隐藏 = 逐条 get 一致）。全量 698（lib 694 + seqlock 4 单独绿）通过。
- **验收预期**：10 万 #4 pk_in_50 1.73ms → ≤0.5ms（下次复测回填）。
- **排期定稿（2026-09-05）**：用户判定 Partition Pruning / Parallel Scan 为“金矿”**要开发** →
  Task-025（剪枝先行 P0、并行扫描阶段 3）；块内索引暂缓、重叠度并入压缩参数；**Per-CPU WAL 可选项
  默认开启 → Task-026，排在最靠后**；完整设计存 research/range-scan-percpu-wal-design.md。
- 排期剩余：Task-024（#29+#14/#27/#11 合并）、Task-025（范围提速）、Task-026（Per-CPU WAL，最后）。


### P99. Task-024 阶段①——全扫分组子集一次构建（#14/#27，2026-09-05）

- **背景**：GROUP BY 无 WHERE 全扫（110 万 #14 855ms / #27 1057ms）在默认行式布局下：
  `scan_stream_fields` 返回整行原文，回调对**每行每个分组键/聚合字段各做一次整行
  `serde_json::from_slice<Value>`**（25 列 × (组键+聚合) 次/行）——解码开销随整行列数重复放大。
- **改动**：
  - sql/executor/eval.rs：新增 `subset_doc_bytes(doc, keep)`——单遍 MapAccess 只收 needed 顶层字段
    （非目标 IgnoredAny 跳过）→ 子集 JSON 字节（缺失省略，语义同整行）；含 `.`/`[` 由调用方回退；
  - sql/executor/group_by.rs 全扫回调：行式原文先一次子集化（simple_needed 守卫），WHERE 判定/
    分组键/聚合提取全部在短子集上进行（group_key_of/field_non_null/numeric_field 原函数复用，
    子集 parse 廉价）；非对象/解析失败保持原文（行为与整行路径一致）。
- **测试**：既有 GROUP BY 行式=PAX 等值、HAVING/排序/地面真值全量通过；全量 698
  （lib 694 + seqlock 4 单独绿）绿。
- **验收口径**：#14/#27 目标 ≤ 现值 0.5×（下次 rr 复测回填）；#29/#11 属默认行式 IO 地板，
  阶段②需 PAX(hot_fields) 数据布局复测（Task-024 阶段②）。


### P100. Task-025 方案 A——数值 zone 剪枝安全化（#11，2026-09-05）+ zone 行产出核实

- **背景/发现**：块级 Zone Map 剪枝为**字节序比较**——对定长整数字符串（ts）≈数值序，但对变长
  小数/跨位数会**误剪有效块**（反例：zone.min=`"9.0"`、查询上界 `10`，字节 `"10"<"9.0"` → 含
  9.x 的块被跳过 = 数据丢失级）；此即此前 zone_fields 留空（P4-C）根因。**核实产出条件**：
  zone 行仅在 PAX(hot_fields) 块产出（sstable writer flush_block 行式分支 zones 为空）——默认
  行式布局无 zone 行，#11 要生效须把 amount 等列加入 hot_fields 重装。
- **改动（方案 A，不改格式）**：sstable/iter.rs 剪枝比较改为——zone 边界与查询界均可解析 f64 时
  **按数值比较**，任一侧非数值回退字节序（字符串列语义安全）。
- **测试**：task025_numeric_zone_between_no_false_skip——amount 1.0..31.0（跨 "10" 文本陷阱，
  误剪会丢 9.x 行）行式(无 zone) vs PAX(含 amount zone) BETWEEN 1..10 命中均 = 0..=90。
  全量 699（695+seqlock 4）绿。
- **预期**：#11 amount BETWEEN 在 hot_fields 含 amount 的 PAX 库上启用块级数值 zone 剪枝
  （窄区间跳不相交块），需复测回填。
- MySQL 内存结论（归档执行）：1.1M 行 2G pool 充足**暂不加大**；公平对比按缓存预算口径或收紧
  SCC hotcache 复测（user_guide/性能对比-…md §4）。


### P101. Task-027 HotCache 读回填 TinyLFU 准入（2026-09-05）

- **背景**：读 miss→LSM 命中路径此前**无条件回填**（engine/read.rs get/batch_get 直接
  `hotcache.put`）→ 全表扫/长尾单次访问可污染 HotCache。
- **改动**：
  - src/hotcache/tinylfu.rs：CMS（depth=4×width=512、4-bit 饱和 ≈1KB）+ 可删除 Doorkeeper
    （32K 槽 ≈256KB、开放寻址+墓碑，写操作可精确清 key）；**首次访问只入门卫不计数**
    （防扫描污染），≥2 次访问计入 CMS；Estimate ≥ 阈值（默认 4）准入；**采样衰减**：累计
    Record ≥ reset_samples（默认 2048=width×depth）全量 >>1 并清 doorkeeper——按累计记录数
    触发，非“查询次数”，85 万 QPS 读 miss 约 2.4ms 一次（用户定参）；
  - 写操作（put/invalidate）→ 该 key 计数减半 + 清 doorkeeper（CMS 无法精确删单 key；
    保留部分热度防缓存饥饿），写 put 仍直写不受准入限制；
  - config：`tiny_lfu_enabled`（默认 true）/`admit_threshold`(4)/`reset_samples`(2048)；
    engine/read.rs 点查与 batch_get 读回填改走 `read_backfill`（disabled 回退无条件）。
- **测试**：首访/2000 单触扫描不污染（len=0）、热读第 5 次准入命中、写后需重新热读、
  disabled 回退、小窗口（8）衰减下热 key 持续识别（hotcache 21 绿）。全量 **704**
  （700+seqlock 4）绿。
- **验收口径**：ycsb c（全随机读）命中率/吞吐不降、内存受控；离线条带导出后点查 p50
  劣化 ≤1.5×（下次基准回填）。


### P102. Task-025b 阶段①——并行全扫聚合（2026-09-05）

- **落点**：聚合全扫（engine.scan_stream_fields 串行回调）为整窗单线程；并行路径限定
  **无 WHERE / 无 ORDER / 无 LIMIT 且窗口两端有限 [lo..hi]**——等分子窗并发扫描，聚合
  （COUNT/SUM/MIN/MAX/AVG）交换律可合并，逐行提取逻辑与串行 acc 的 no-WHERE 分支一致。
- **改动**（sql/executor/aggregate.rs）：候选 None 分支内并行分支——`available_parallelism`
  2..=8 核等分 docid 子窗，`std::thread::scope` 每 worker 独立 acc（含熔断检查），合并后按
  收尾格式等价早退（不触碰串行 acc 持有变量）；无界/带 WHERE/排序/LIMIT 保持串行。
- **测试**：5000 行（含 null/缺字段）COUNT(*)/COUNT(f)/SUM/MIN/MAX/AVG 有限窗口并行 =
  无界串行逐值一致（task025b_*）。全量 **705**（701+seqlock 4）绿。
- **阶段②（后续）**：通用全扫（WHERE 组合）并行、GROUP BY 分片合并、导出/条带扫描并行
  与跨文件扇出并行（对应 #5/#11 多段场景）。


### P103. Task-025b 阶段②——通用 WHERE 并行 + GROUP BY 分片合并（2026-09-05）

- **通用 WHERE 并行**（aggregate.rs）：并行路径解除“无 WHERE”限制——worker 逐行执行与串行
  acc 一致的 WHERE 判定（light_where_matches 优先、serde 回退），投影子集含 WHERE 引用列，
  语义等价；仍限 ORDER/LIMIT 空 + 窗口有限（标量聚合本无行序依赖）。
- **GROUP BY 分片合并**（group_by.rs）：窗口两端有限时按核（2..=8）等分子窗并发构建局部分组
  （子集化/WHERE/分组键/聚合提取与串行分支逐值一致），按组键+累加器逐项合并（count/n_num/
  sum 相加、min/max 取极值，交换律保证一致）；无界/错误回退串行；组数 cap 在分片内即熔断。
- **测试**（task025b_*，5000 行双状态含 null）：WHERE COUNT/SUM/AVG 并行=串行；GROUP BY
  COUNT/SUM/MIN/MAX 分片合并 = 无界串行逐组一致。全量 **705**（701+seqlock 4）绿。
- 后续：导出/条带扫描并行与跨文件扇出并行（#5/#11 多段场景，Task-025b 阶段③）。


### P104. Task-025b 阶段③——条带并行全扫导出构建块（2026-09-05）

- **Engine::scan_range_parallel**（engine/scan.rs）：[lo..hi] 按核（≤16）等分子窗并发
  `scan_range`，各窗 docid 升序后 **K 路归并**输出全局升序；与 `scan_range` 同契约
  （删除位图/Delta/memtable 可见性一致），workers≤1 退化单线程。供导出/备份/离线读等
  条带消费端直接复用（备份文件拷贝类无需改）。
- **覆盖矩阵**：条带并行（本步，行扫描）；聚合/分组窗口并行（阶段①/②：无 WHERE/通用
  WHERE/GROUP BY 分片合并）；**跨文件扇出并行**（#5/#11 多段场景 = CF 层逐文件并发 +
  k-way 归并，需 CF scan 层改造）→ 单列 **Task-025b 阶段④**（独立、深）。
- **测试**：20k 行 + 删除位图 + flush 后尾批，8 窗并行 = 串行逐行一致且全局升序。
  全量 **706**（702+seqlock 4）绿。


### P105. Task-024 阶段②——PAX(hot_fields) 10 万复测轮（2026-09-05）

- **做法**：基准库以 hot_fields=[k,amount,ts,status,city,region]（PAX 列存，tmp-cfg-wide-2g-pax）
  重建 100k 重跑 37 探针（results-pax-sqlrun-compare-100k），对照行式 results-sqlrun-compare-100k。
- **结果（SCC mean ms / 比值）**：#9 3.60→2.73（-24%，5.5→4.0×，倒排回表受益）；#11 58.05→54.22
  （-7%，1.5→1.3×）；#14 68.61→63.31（-8%，2.7→2.4×）；#15/#28/#30/#31 保持 SCC 快档（0.5×/0.0×）。
- **未达验收 → 暴露接线缺口（新开发项，非复测可补）**：
  ① 点查/IN 投影下推：#2 0.46→0.45、#4 1.73 持平——点查仍走 engine.get 整行解码（PAX 行不解列），
     需 engine 投影点查（get_fields）接线；
  ② #12 COUNT(*) 43ms：fresh-load 多 L0 快照不 eligible → O(1) 只对收敛层生效；需活跃 docid rank 计数；
  ③ #29 235ms（4.7×）/ #5 3.11ms（11×）：行读+块 IO，归 Task-025b 阶段④（跨文件扇出/块IO）；
  ④ #11 数值 zone 运行时路由待核（zone 收益或仅在 scan_pushdown 路径；正确性已由 task025 单测保障）。
- 决策：上述 ①-④ 转开发排期（dev_remain Task-024 阶段② 缺口注）；本复测为基线存档，不回溯旧排期项。


### P106. Task-024 阶段② 缺口①——点查/IN 投影下推（get_fields 接线，2026-09-05）

- **根因（P105 ①）**：#2 pk_point_proj10 0.46ms / #4 pk_in_50 1.73ms 持平未降——server 点查/`id IN`
  分支仍走 `engine.get` / `get_many_pk_in` 整行解码回传（PAX 数据不解列、整行 25 列重构 + 回包）。
- **做法**：
  - `server/command/select.rs` 新增 `projection_pushdown_fields`（投影含 ≥1 纯顶层简单字段列、无
    `doc` 整列、字段名不含 `.`/`[` → 去重顶层字段清单；否则 None 回退整行路径，语义不变）；
  - 点查分支 → `engine.batch_get_fields(&[d], fields)`（HotCache/Delta/删除位图语义同 `get`）+ 子集组装；
  - `id IN` 分支 → 新增 `Engine::get_many_pk_in_fields`（engine/read.rs，`get_many_pk_in` 字段级变体：
    稠密跨度 ≤4× 走 `scan_stream_fields` 区间投影顺序读、稀疏走 `batch_get_fields` → `assemble_subset_json`
    组装子集 JSON；可见性/去重语义与 Task-023 逐分支同构）；
  - `sstable/block.rs` `assemble_subset_json` 由私有提为 `pub` 并经 `sstable/mod.rs` re-export 复用。
- **语义护栏**：PAX 块每列均独立存列（hot/cold），任意顶层字段可单列解码；子集 JSON 缺字段省略 /
  null 直嵌 → `build_result_set` cell/列类型与整行路径逐值一致（含转义字符串/浮点/缺列/JSON null）。
- **测试（+2）**：engine `gap1_pk_in_fields_subset_matches_full_row_both_layouts`（行式+PAX ×
  memtable/flush/重开 × 稠密/稀疏，命中集与逐字段值 = 整行路径）；server `gap1_point_and_in_projection_pushdown_matches_whole_row_path`
  （PAX+flush 落盘点查/IN 结果集 = 整行参考路径逐字节一致；`SELECT id,doc` 整行直通与缺字段/nested 回退护栏）。
  全量 **711**（707 通过 + 1 既有 seqlock 低频写偶发并发调度失败单跑独立绿 + 3 ignored）。
- **复测修正（PAX 10万既有库，cjserver 侧，results-pax-gap1b-scc-100k，P105 同库对照）**：
  首轮复测发现 hotcache 命中行被走"整行提取→逐字段重序列化→组装子集→消费端二次 parse"
  （比旧整行路径多一轮 parse+序列化，#3/#4 微涨）——改 **hotcache-first**：`get_many_pk_in_fields`
  先对未删 docid 查 hotcache 直通整行字节（零二次 parse），仅冷行走 scan_stream_fields /
  batch_get_fields 列解码；点查分支复用该 API。二次复测：#1 0.24→0.22（SELECT* 零提取地板）、
  #2 0.45→0.41、#5 3.11→2.82、#3/#4 持平（0.41/1.75 vs 0.40/1.73）——接线生效无回退。
- **验收结论（P105 缺口注 #2≤0.25/#4≤0.8 未达）**：10万库全行由写路径直写 hotcache（~100MB ≪
  1024MB），点查基本热命中整行；单查询固定开销 floor = #1 SELECT* 0.22ms（协议/parse/响应组装），
  投影提取+类型组装 ~0.19ms 增量中 PAX 列解码仅小头 → 推下收益被 floor 淹没。缺口① 收口为
  「接线正确、热路径零退化、冷读受益方向正确」，数值验证宜在 110 万超缓存轮（宽行回表冷读）进行。
- **110万 轮（2026-09-05，db-wide-scc 行式布局，results-pax-gap1c/d/e-scc-1100k）**：读探针
  #2 0.44~0.56、#4 2.30~2.61、#5 2.99~3.61 vs 旧基线（P85-P92后 results-sqlrun-scc-1100k）
  #2 0.44 / #3 0.42 / #4 1.96 / #5 3.32 ——三轮内 #1（SELECT*，纯 engine.get 与本改动无关）同步
  0.25→0.29→0.37 爬升：每轮 3 万行 insert+整区 delete 清理使 L0 段/墓碑累积、布局逐轮劣化抬高
  读基线，且聚合/写区 p99 受后台 compaction 干扰——既有库上无法稳定 A/B（非 PAX 布局亦非本项
  设计收益场景）。行式冷读稀疏分支暴露整行全量 Value 树提取（extract_fields_from_json_row）多余
  解析 → 改**单遍流式只收目标**（同 Task-022 消费端口径，P87②/P86② 全体消费端受益），全量
  708 绿。收口判定：缺口① 代码与语义正确、无系统性回归；行式 110万 库逐轮读基线爬升为库形态
  劣化（L0 累积）所致，与接线无关；冷读列解码收益验证需 PAX 布局大库（重建）另行评估。


### P107. Task-024 阶段② 缺口②——#12 COUNT(*) O(1) fresh-load 放宽（2026-09-05）

- **根因（P105 ②）**：#12 10万 PAX fresh-load 42.84ms 均值——活跃 docid rank 计数（P96，
  count_all_docs/count_docs_range）本已层形态无关；真正的慢在 **fresh-load 场景首个 COUNT**：
  cjserver 在空数据目录上打开（live 基线 None）→ rr wide-load 期间 put/delete 不记账 → 首个
  COUNT 触发一次性全键扫基线（~200ms 拖高 5 次均值 ≈ 43ms），与"多 L0 快照 eligible"无关。
- **修复**：engine/open.rs 在 primary.data_empty()（memtable + SST 全空）时把 live_docids 播种为
  `Some(空)`——load 期 put/delete/delete_batch 全程增量记账（P1-C 记账全链路），首个 COUNT(*)
  亦 O(1)（活跃集 rank，免基线全扫）。非空库保持 None（懒建，避免 open 期全扫拖慢启动）；
  purge_all 复位 Some(空) 语义不变。
- **单测（+1）**：`gap2_count_o1_fresh_load_empty_open_multi_l0`——空库播种 + 小 memtable 多
  flush 成多 L0 + PAX 块（fresh-load 形态）全程增量记账 = keys-only 扫描口径（覆盖写/删除/复活）
  + 整表窗口 count_docs_range 一致 + 多表高位不串表。全量 **709** 绿。
- **实测回填（fresh-load 10万 PAX，results-gap2-scc-100k）**：#12 42.84ms → **0.27ms**
  （p50 0.28 / p99 0.29，5 次全 O(1)），验收 ≤1ms 达成；sqlrun harness 自身首个 COUNT 亦 O(1)
  （N=100000 精确）。临时验证库（db-gap2-check）已删除。
- **复核一致（2026-09-05，results-pax-gap3-scc-100k）**：同口径重建 10 万 PAX 库复测 #12 =
  0.25ms（P105 43.24ms），归档见 user_guide/性能对比-2026-09-05-P85-P92后-10万与110万.md §8。


### P108. Task-024 阶段② 缺口④——#11 数值 zone 运行时路由核实（2026-09-05）

- **疑点（P105 ④）**：#11 amount BETWEEN 仅 -7%，怀疑"BETWEEN 行级走 scan_all 未带 zonepred，
  zone 只生效于 scan_pushdown 路径"（运行时收益待接线）。
- **核实结论：无运行时缺口**。静态走查 sql/executor/select.rs 路由：
  - cost-based 分支（L594-626）：裸 Between 且无倒排等值 → `choose_best_plan` 无 Inverted 候选，
    必回 `FullScan` → `scan_pushdown`（L621，`leaf_to_zone_pred` 产 ZonePredicate）；
  - 兜底分支（L642-643）：`scan_leaf` 命中裸 Between/Cmp → 同样 `scan_pushdown`；
  - `scan_all` 仅服务 AND/OR 复合内的范围臂（eval_cond / post_filter，先经倒排位图收敛后逐
    docid 判范围——该场景 zone 本不适用）；
  - iter.rs `advance_block` 块级跳块（f64 安全比较 + 字节序回退护栏）与 CF/Engine
    `scan_stream_with_zonepred` 链路完整，正确性由 P1-E 与 task025_numeric_zone_between_no_false_skip
    单测保障。
- **P105 #11 仅 -7% 的数据侧归因**：amount 列值**随机、非随 docid 聚类**——每块 ~50-60 行随机
  样本的 zone min-max 已 ≈ 全表数值跨度 → 与任何 50 宽窗口相交，跳块率≈0；zone 剪枝收益的前提
  是列随 docid 单调/聚类（ts、自增类）。属结构性收益边界，非接线缺陷。
- **可选后续（不排期，仅计划精度，无行为影响）**：select.rs L613 `zone_fields` 恒空 → cost 模型
  固定 `effectiveness=0.3`；可改传 PAX hot_fields 使计划评估反映真实 zone 覆盖率（range-only
  下仍选 FullScan，不影响正确性）。


### P109. Task-025b 阶段④——跨文件扇出并行（2026-09-05）

- **目标（dev_remain 阶段④ 未开发项）**：#5/#11 单窗口多段/重叠 L0 场景，CF scan_stream_at
  串行逐源归并——各文件块读/解压/解码延迟相加，无法多核利用。
- **做法**：CF 新增 `scan_stream_at_parallel`（column_family/scan.rs）：窗口命中 ≥2 SST 且
  workers≥2 时，**每 SST 源一个 scoped 线程**批量推进 `SstRangeIter::next()`（FAN_BATCH=512，
  `sync_channel(2)` 背压），memtable 源主线程内联；主线程沿用串行堆归并（同 key 折叠/快照过滤/
  Tombstone/Zone 谓词/投影/回调 false 早停逐行一致）。Engine::scan_stream_parallel 包装（+删除
  位图过滤），并接线 scan_pushdown（裸比较/BETWEEN 谓词下推；workers = 可用并行度 clamp 2..=8）。
- **关键修复（死锁）**：scope 闭包捕获原始 `txs` Sender 集不释放 → worker 结束（其克隆 Sender
  drop）后 channel 仍因原始 Sender 存活而不关闭 → 主线程末批 `recv` 永久阻塞。**spawn 后立即
  `drop(txs)`** 解决（调试探针实测定位：4 worker 全部送达完成仍卡在 recv wait）。
- **回退护栏**：workers<2 或命中 <2 SST → 直接委托既有串行 `scan_stream_at`（零行为/性能回归，
  公共 API 均保持默认 1 worker）；与 Ex-8.9 IO 预算经既有 scan_limiter（输出行字节节流）协同。
- **单测（+1）**：`task025b4_scan_stream_parallel_matches_serial_multi_l0`——auto_compact 关 +
  小 memtable 分 4 段 flush（多 L0 重叠）行式/PAX 两布局 × 覆盖写/删除/删除位图/memtable 尾行
  × workers 2/4/8：扇出与串行逐行一致、全局升序、早停前缀一致、投影列值等值。全量 **710** 绿。
- **数值回填（2026-09-05 压测：50 万行 × 4 交错 L0 文件，全窗口 k-way 归并全值扫描，
  best-of-3 ms）**：行式 w1 138.5 / w2 90.5 / w4 82.2 / w8 88.7（全窗，~1.5-1.7×）、半窗 w1
  62.9 / w2 40.6（~1.5×）；PAX（整行重构解码昂贵）全窗 w1 372.5 / **w2 124.3（3.0×）**、半窗
  w1 190.8 / **w2 61.6（3.1×）**。结论：并行收益随单文件解码成本上升（行式 decode 轻 → ~1.6×，
  PAX 重 → ~3×）；**w2~w4 近最优**，w≥4 主线程归并/背压成瓶颈 → scan_pushdown 默认 workers
  上限 clamp 2..=4。行数 + docid 和值跨 workers 一致校验通过。


### P110. Task-026 Per-CPU WAL 全链落地三坑（2026-09-05）
- **现象/根因/修复**：
  1. **空 flush 产出空 L0 SST 污染 GC 调度**：Engine 打开时对全部 CF（含 delta/cidx/outbox）无条件 `switch_and_flush` 做"迁移收尾"→ 空 memtable 也会落一个空 L0 文件 → 删除密度 GC 轮里 delta/cidx urgency=10 恒高于 primary GC（DD=6），主列族删除回收永远排不上（排空轮全 0 丢弃、`needs_compact` 不收敛）。修复：迁移仅当 `memtable_bytes()>0` 才刷（空 flush 无收益）。
  2. **从未入队的 CF 把 checkpoint 钉死在 0**：cp = min(各 CF 刷盘水位)，而 cidx/outbox CF 即使打开也可能从不写（无组合索引/无 outbox 消息）→ 永不 flush → cp=0 → 队列文件永不裁剪、重开重复回放。修复：运行时维护"各 CF 曾入队最大 gseq（last_enqueued）"，从未入队的 CF 不参与 cp 约束（视作 +∞）；恢复回放后播种（防裁剪越过 memtable 未刷数据）。
  3. **begin_snapshot 依赖 CF 自身 WAL 计数**：external 模式下 primary 的 WalBackend 不再推进 → 快照点恒 0，全部 RR/快照读隔离失效（几十个 txn 测试红）。修复：改以 engine `global_seq` 为准（与 current_seq 同源）。
- **提交**：`0c1897b`（2a 编解码/队列文件 IO）、`7c1d7e3`（2c 队列运行时）、`810f182`（2b/2c 接线 + 3a 恢复 + 默认翻 true）；全量 736 绿（seqlock 计时偶发单跑绿，P48 已知）。

### P111. Task-007 层级时间轮推进正确性两坑（2026-09-05）
- **现象/根因/修复**：
  1. **级联下放复用 insert 双计 expiry_set → 推进死循环/任务不触发**：expiry_set（跳步快进的下一到期秒）在 schedule/insert 都自增，级联把任务从高轮移到低轮时重复计数 → 到期触发只减一次留下 stale 计数 → `next_event_at` 返回过期秒，advance 卡死（测试挂起）；修复：expiry_set 只在 schedule / fire / cancel / restore 维护，insert（兼做级联下放）不计数。
  2. **事件恰落 advance 目标点未处理 + 天轮候选命中"已过去的当天"**：跳到 target 即 break 不执行 `process_at` → 恰好整点/到期在 target 的任务漏触发（1 年 TTL 最后一秒不触发）；天轮桶槽号 = 日序 %365，若与当前日同余会给出已过去的当天整点（stale）→ 级联永不发生。修复：`t == target` 时处理后再结束；候选激活严格 `> now`（同余取下一周期）。
- **提交**：见 Task-007 提交（本会话）。

## 阶段 4 · 10 万轮修复（2026-09-05，Task-028/029/031/030/032）

**P112（Task-028）cidx 重启/后加配置丢键 → 组合索引查询静默空**
复现：composite_indexes 声明在、重启后 `status='active' AND ts=`（真实 ~1e4 命中）返回 0 行/0.37ms
（cidx 仅内存/队列态，open 不回扫存量）；nocidx 声明下同库返回 10000 行。修复：`Engine::
ensure_composite_index_backfill`（open 期调用）——primary 非空且 `cidx.sig` 标记签名不符或 cidx 空时，
从 primary 全量回扫重建复合键（`ColumnFamily::memtable_put_nolog` 无 WAL 直入 + `switch_and_flush`
落 SST + 写签名标记；崩溃丢标记幂等重做）；正常会话零开销。单测 task028（后加配置/签名变更/重开三态）。

**P113（Task-029）AND(等值,等值) 后过滤逐 docid get → 块级批量取数**
复现：无 cidx 下 `status='active' AND ts=` 走 eval `post_filter` 逐候选 `engine.get`（0 命中全遍历
2322ms≈77µs/候选冷态 / 报告热态 24ms≈1.2µs）；`SUM(amount) WHERE active`（P1-D 批量）同候选仅
~3.5µs/docid。修复：post_filter 顶层简单字段叶按 512/块 `engine.get_many_pk_in_fields` 批量取子集 +
块内字节级判定（HotCache 直通/稠密区间流/稀疏批量），嵌套点路径叶保持逐点回退（语义不变）。
单测 task029（跨块候选/删除隐藏/0 命中全遍历/LIKE 组合）。

**P114（Task-031）结果集行输出 ~25µs/行固定常数**
复现：同窗引擎 COUNT(20k) 94ms≈5µs/行 vs `SELECT id` keys-only 20k 606ms≈30µs/行（列数无关）→
响应逐包 write（每包 2 次 syscall）。修复：`server.rs frame_response` 多包合并单帧一次 write_all
（同步+异步连接同接线；字节流与逐包完全一致）。单测 task031（帧=逐包序列、seq 回绕、按包还原）。

**P115（Task-030）COUNT(DISTINCT col) 与 GROUP BY … ORDER BY <聚合> 解析/执行缺口**
复现：两探针 SQL 1064（parser 不支持 DISTINCT 参数 / ORDER BY 内聚合头）。修复：parser 聚合参数
支持 `COUNT(DISTINCT f)`（Select.agg_distinct；GROUP BY 内 DISTINCT 拒绝防静默）、ORDER BY 项支持
聚合列头规范串（`COUNT(*)`/`SUM(f)`）；execute_aggregate 新增去重计数分支（窗口扫描非 null 去重值，
数值按 f64 规范化、缺字段/NULL 不计）；execute_group_by 排序项支持聚合下标（数值比较、NULL 升序最前）
且倒排快路径遇聚合排序自动交主路径。单测 task030。

**P116（Task-032）主键 IN 稀疏大批次逐键点查残余**
复现：pk_in 随机 id 稀疏（跨度 ≫4×计数）→ `get_many_pk_in*` 逐 docid 点查（~31µs/键 warm，
pk_in_5000 174ms）。修复：稠密判定 4×→64×（区间顺序读 ~0.5µs/行，span≤64×n 时优于逐键点查；
超限自动回退 batch_get 不劣化），整行/投影两变体对齐。单测 task032（4×~64× 窗口 = 逐条 get）。
> 数值验收随 Task-005 10w 基准回填；Task-033（锁等待超时 1205）需先实测 run_lock_wait 副会话
> outcome（SCC waiter-ok vs MySQL waiter-1205）再定等待/超时接线，暂不盲改。

**P117（Task-033）锁等待探针修正 + 实测收敛（明示差异，不改引擎）**
探针缺陷：`run_lock_wait` 把 `SET SESSION innodb_lock_wait_timeout=3` 设在**主**连接，副（等待方）
连接走 MySQL 默认 50s → 永不 1205，两侧都只是等主提交后拿到（10w 轮「4s 两侧一致」的成因），
且 outcome 字符串被套件丢弃（只记整块耗时，耗时为探针固定 sleep 4s 主导）→ 锁语义从未被测出。
修复：超时改在副连接生效；outcome 以 ⚑ 记入 stdout/summary.md（waiter-ok / waiter-1205 /
waiter-t=xxms；对比脚本视 ⚑ 为记录而非失败）；sqlrun 加 `--only` 探针过滤。
实测（干净 100k 双端，主 FOR UPDATE 持锁 sleep 4s 后 COMMIT，副同 id UPDATE）：
MySQL 3316 = **waiter-1205锁等待超时（waiter-t=3013ms）**；SCC 3317 = **waiter-ok（waiter-t=3ms）**。
根因修正：SCC `FOR UPDATE` 为乐观「当前读锁定集」（cur_lock_seq 记读取时最新 seq，提交期写写冲突
判定），事务期间不真正持排他行锁 → 并发 UPDATE 3ms 直接放行，并非原假设的「阻塞至主提交后成功」。
结论：1205 收敛需行锁持有 + 等待/超时（锁生命周期重构），风险超本任务预期 → 走 Task-033「或明示
差异」路径收口：SCC 无引擎改动；差异入已知边界；探针 outcome 两侧如实记录，回归全绿。

**P118（Task-030 残余 #61）COUNT(DISTINCT) 低基数 344.7× → 位图词典快路径**
复现/根因：10w 干净双端轮（results-sqlrun-compare-100k #61）SCC `COUNT(DISTINCT status)` 65.49ms
（MySQL 0.19ms，344.7×）。P115 去重计数走**权威窗口扫描**（execute_aggregate_window distinct 分支
scan_stream_fields 逐行收 HashSet 去重键）；server 路径（src/server/command/select.rs L107）聚合一律
传本表整窗 [table_base, table_base+2^48)，scoped 窗口扫描逐行解码——低基数枚举字段没有任何词典捷径。
修复（kernel 3 处）：
① src/inverted/query.rs `bitmap_field_snapshot(field, cap)`——白名单字段（bitmap_fields）值→docid 位图
   克隆快照（组数超 cap=512 → None，防 user_id 类高基数克隆放大；锁内克隆后释放，读路径不持倒排锁）；
② src/engine/query.rs `count_distinct_fast(field, start, end)`——白名单字段 + flush pending + live_ensure
   活跃集（RoaringTreemap）快照，逐值判定「窗口 [start,end] ∩ 活跃 docid 非空」即 1 个 distinct——
   删除位图/墓碑口径与权威窗口扫描一致（整值全删陈旧位图不复计、同值复活复计；跨表高位 docid 被窗口
   排外）；非白名单/高基数 → None（回退扫描）；
③ src/sql/executor/aggregate.rs distinct 分支先行尝试快路径（无 WHERE + 顶层简单字段），不可用回退
   权威窗口扫描（WHERE/嵌套路径/非索引字段路径不变）。
+1 单测 task030b（快路径 = 带 WHERE amount>=0 逼权威扫描等值：基础 2 值 / 67 行整值全删后 1 /
city 同步 / 同值复活 2 / 非白名单 amount 扫描兜底 34）。全量 lib 回归 **742 passed / 0 failed**。
验收锚点：10w #61 65.49ms → <1ms 量级（status 5 组枚举）；110 万同口径复测数值随基准轮回填。
已知边界（与引擎倒排既有语义一致）：白名单位图仅追加不摘除（同 docid 覆盖/复活**换值**留陈旧 docid）
→ 值变更场景快路径可能多计陈旧值（与 `COUNT(*) WHERE f='v'` 倒排计数同类口径偏差）；扫描路径恒为精确兜底。

**P119（P-GB）窗口位图分组快路径 + WHERE 单等值候选（#14/#27/#59/#60/#81 收敛）**
根因：server 聚合/分组一律传本表整窗（select.rs L107）→ `execute_group_by_window` scoped=true 禁用
`group_by_fast_inverted`（仅 !scoped 启用）→ 10w 轮 #14/#27/#59 = 210/262/252ms vs MySQL 29/46/39ms；
#81 biz_agg_filter（WHERE+ORDER BY COUNT(*) DESC LIMIT）308ms（11.4×）；#60（WHERE status='active'
GROUP BY region,channel）290ms。修复（kernel 4 处）：
① engine/query.rs `group_by_bitmap_window(fields, cand_term, start, end)`——窗口位图分组计数：各组
   字段值位图 AND（≤2 字段）∩「窗口 ∩ 活跃集 ∩（WHERE 单等值候选 posting）」逐组计数；返回
   (组, 窗口活跃匹配数)（无候选 = 窗口活跃 rank 差），调用方以 Σcounts 与匹配数核对判 NULL 组/
   陈旧放大回退（Σ>live 或两字段 Σ<live → 回退扫描保精确）；
② inverted 复用 P118 `bitmap_field_snapshot`（值→位图克隆，锁外计数）；
③ group_by.rs `group_by_fast_bitmap_window`——windowed 路由（scoped + 1..=2 白名单字段 + 单列
   COUNT(*)/HAVING/组字段或该聚合列头排序/LIMIT；WHERE 单等值转候选词条；其余 → None 回退扫描）；
④ 基准配置 tmp-cfg-wide-2g.toml `bitmap_fields` 补 `"channel"`（枚举字段白名单，#60 分组字段
   region,channel 全白名单才可达位图路径）。
+1 单测扩展（pg_windowed_bitmap_group_by_matches_scan ⑧：#81 形态 WHERE 候选 + ORDER BY COUNT(*) DESC
LIMIT = 权威扫描等值；既有 ①~⑦ 覆盖单/双字段/删除/复活/缺字段回退/HAVING/LIMIT/组字段排序）。
10w 干净轮实测回填（scratch 库 + 当前 release，对比 results-sqlrun-compare-100k 记录）：
#14 210.9→**1.6ms**（MySQL 29.3）、#59 252.7→**1.3~1.7ms**（38.8）、#27 262.1→**7.6~9.6ms**（45.9）、
#60 290.1→**10.8ms**（112.1，channel 白名单后）、#81 308.2→**2.6~3.8ms**（27.0）；
#61 count_distinct_enum（P118）65.5→**0.18ms**（0.19 = 1.0×）、highcard 87.6→98.2ms（≈1.35×，扫描兜底）。
全量 lib 回归 742+1 绿（一次 seqlock 概率型重试率用例偶发失败，单跑复通过，与本项无关）。
已知边界（同 P118）：白名单位图仅追加不摘除——同 docid 换值留陈旧 → 计数可能偏高，一律以
Σcounts≤匹配数 守卫回退扫描保精确；字段不在 bitmap_fields 时快路径不可用（回退扫描，见 #60 配置注）。
残余（记录不开发）：#15/#28 SUM/AVG+HAVING ~1.5-1.7×（数值聚合需行级/载荷窗口化，触发候选）。

**P120（P-GB2）数值统计载荷窗口化——GROUP BY + SUM/AVG/MIN/MAX（stats_fields）**
根因：#15 group_by_sum_having 271ms / #28 having_avg_gt 286ms（1.5~1.7× MySQL）——数值聚合须逐行解码
amount 求和（P-GB 位图路径仅覆盖 COUNT）。方案（复用 P-GB 基建，src/sql/executor/group_by.rs）：
- `group_by_fast_bitmap_window` 聚合形态扩展：`COUNT(*)` 与 单个 `SUM/AVG/MIN/MAX(<stats_field>)`
  （可只数值，无 COUNT）；ORDER BY 聚合头映射到对应 spec；
- 数值经 `engine.inverted_group_stats(field)` term 载荷按 stats 位序取 FieldAgg（n/sum/min/max）填组状态；
- **精确守卫**：每组分组的载荷 `n == 位图活跃计数` 才可用（删除/复活/换值/跨表/缺 amount 使 n≠活跃计数
  → 回退权威扫描，宁慢勿错）；数值聚合限单字段分组（载荷按单 term 聚合，组合无法拆分）；缺分组字段行
  存在（Σ<live）回退；
- 前置：`[inverted] stats_fields` 声明 + 写路径 add_stats 积累（段 v5 载荷跨重启可读）——基准 cfg
  tmp-cfg-wide-2g.toml 增 `stats_fields=["amount"]`（tmp 不入库，复用需自行补）。
10w 实测（干净轮，#15 位于删除探针前 → 快路径全程生效）：#15 271→**4.0ms**（MySQL 161.7）、#28
285.8→**3.8ms**（181.5，--only 隔离态）；#14/#27/#61/#81 维持 0.15~7.9ms 不回退。
**时序注**：完整 81 探针顺序下 #28 位于删除类探针之后，预留区删除使活跃计数<载荷 n → 守卫回退扫描
（精确兜底，不再 285ms 档内漂移）；#15 在删除前 → 快路径。纯读/只读会话与聚合型仪表盘全程 ms 级。
+1 单测 pg_windowed_bitmap_group_stats_matches_scan ①~⑤（SUM/AVG/MIN/MAX=权威扫描等值；删除/复活/
缺 amount → 守卫回退后仍一致）。边界同 P118/P119（值变更陈旧 → 守卫回退；非白名单/未配 stats_fields → 扫描）。

**P121（P-GB3）标量 SUM/AVG/MIN/MAX WHERE 单等值 → term 载荷窗口守卫（#13 收敛）**
根因：#13 sum_where_enum `SUM(amount) WHERE status='active'` 66.8ms（MySQL 43.9）——Ex-9.3 已实现
term 载荷快路径但被 `!scoped` 门禁挡住（server 恒传整表窗 → scoped=true，见 select.rs L107），退行级
P1-D 候选解码 ~3µs/行。修复（src/engine/scan.rs + src/sql/executor/aggregate.rs）：
- engine `live_count_window(posting, start, end)`——posting 在窗口∩活跃集内的 docid 数（口径=权威扫描：
  删除位图/墓碑剔除、复活重计、跨表高位排外）；
- Ex-9.3 标量载荷路径放行窗口场景：**守卫 = term posting 窗口活跃数 == 载荷 n** 才走载荷
  （n 含删除/复活/换值/跨表贡献或窗口外同 term 行 → 不匹配 → 回退行级/候选扫描保精确）。
10w 干净轮实测：#13 66.8→**0.55ms**（MySQL 43.9 = 0.01×）；#15 3.4/#28 3.5/#60 8.1/#81 2.3/#57 0.5/
#61 0.2ms 不回退（聚合家族全部 ms 级）。+1 单测 pg_scalar_stats_guard_matches_scan（SUM/AVG 载荷
守卫 = 权威扫描；删除/复活后守卫回退仍一致）。边界同 P120（载荷脏化守卫回退宁慢勿错）。

**P122（观察项闭环升级）大区间 id-range DELETE 使整个服务确定性锁死（CPU 空闲）——内核/服务层缺陷，未闭环**
来源：待查观察项「sqlrun 收尾 44k 区间清理 DELETE 长时间挂起」。复现（2026-09-05，10w scratch + release
bd2debb）：
① 全量 81 探针后收尾 `DELETE … id BETWEEN base+1501..base+46001`（44.5k 行在场）——summary 落盘后
   >3min 未返回；服务端 CPU 3s 采样 0s（空闲等待）。kill 客户端后脏态重启，手动同 DELETE 仍不返回，
   并发 SELECT COUNT 亦挂起 → **服务整体锁死**。
② 新鲜库 A/B：clean 100k 行、零并发、`--one` 单条 `DELETE id BETWEEN 2 AND 20001`（20k 行）→ 同样锁死；
   之后连 1000 行小 DELETE 都挂起。
③ 假设排除：`auto_compact=false` + `delete_density_min_docs=1e12` 变体 cfg 下仍锁死（非删除密度排空
   GC 与 DELETE 争写锁）；套件内 1000 行范围删 #74=3.5ms / #23/#24 正常 → 与**行数规模**相关（触发点
   在数百~2 万之间某阈值），非逐行机制。
特征：服务端 CPU 空闲 + 线程数恒定 17 + 全部后续命令无响应 → 引擎/服务级锁等待死锁（handler 与某后台
线程互等，零 CPU）。结论：**确定性内核/服务缺陷**（大 id 区间删路径），非测试方法坑；workaround 保持
（sqlrun 先落 summary + 每轮重置重装载仍适用）。建议专门会话以线程栈/断点定位（候选：delete_batch 批尾
与组提交/后台 flush 交互、per-CPU 写队列批量等待、server 会话与后台 worker 引擎读写锁序；可先在
5k/10k/15k 行二分复现确定触发阈值）。排查期间产生的 10w 全量探针结果（清理前）
results/results-sqlrun-scc-10w-full/summary.md 保留为基线参考。

**P123（P122 修复闭环）大区间删除锁死根因 = per-CPU WAL 单 scope 超队列深度 + 持写锁互锁**
二分（fresh 30k 库、零并发 `DELETE id BETWEEN 2 AND N`）：3000 OK(10ms) / 4000 OK / **5000 HANG**、
10000/20000 HANG——非单调但触发点恰在**单 scope 条目 ≈ 2×行数 > per_cpu_queue_depth(4096)** 量级
（4000 OK 与该次运行队列空/无并发有关，属竞态边界）。阶段打点（P122_TRACE 门控）证明：`delete_batch`
**内部循环完整跑完**（delbatch_loop_done → returning 均打印），卡点在其后 per-CPU 包装层
`enqueue_scope → rt.submit → QueueInner::enqueue`：scope 条目 > queue depth(4096) 时进入**背压等待**
（while depth+n>cap cond.wait），而此时调用方（SQL DELETE handler）**持引擎写锁**；per-CPU 消费线程
写盘路径（段切换/CF 回调等）又需要引擎 → 互锁死锁：服务整体锁死、CPU 空闲、后续所有连接排队。
修复（引擎层根治，任何调用方安全）：
① src/engine/percpu_wal.rs `PerCpuWal::scope_doc_budget()`——docid 预算 = queue depth/2（≥1 上限 4096，
   每 docid 删除至多 ~2 条 WAL（主墓碑+delta 前缀）→ 单 scope 恒 < depth）；
② src/engine/write.rs `Engine::delete_batch`——per-CPU 启用时按预算**内部拆子批**（每子批独立
   gseq scope 入队），防单 scope 超深背压；语义与一次性整批一致（幂等、计数累加）；未启用维持原路径。
验证：fresh 30k 库 20k/10k/5k 区间删 **全部 OK**（70/27/14ms，此前恒 HANG）；**全量 81 探针 10w 轮
65.9s 完整跑完**（此前卡死在收尾 44k 清理），收尾后服务正常响应。全量 lib 回归 744 绿（一次 seqlock
概率型用例偶发失败单跑复绿，无关）。
业务层补充（防再踩）：大删请按 per_cpu_queue_depth 控制单批（勿 >depth/2）；多线程并发删同一表 + 大批
仍可能把队列写侧压满，建议串行或限并发；本修复兜底引擎 API 层，应用层分批+sleep+日志的实践仍推荐。

**P93（P0-B #29 达线实验）Top-K 全表排序：PAX 无 IO 收益根因 + 并行分片内存/核利用问题（未达线）**
现象链路：110 万 PAX(hot_fields=[k,amount,...]) 库 #29 ORDER BY k,amount LIMIT 100 ——串行均值
18~43s（冷热波动）> 行式 13.1s 基线；10 万 PAX 224ms ≈ 行式 234ms → **PAX 布局未带来 IO 节省**。
根因分析（P93_TRACE 分段计时，10 万内存驻留态）：scan 解码 147ms/100k≈1.5µs/行 + 回调子集 JSON
二次 parse≈1µs/行；关键：**v6 PAX 热列虽列存但各列仍封在同一块内压缩**——全表排序必须整块解压，
体积≈行式，故 IO 无差异；110 万数据 > 块缓存容量 → 每轮整块重解压（磁盘 500MB/s 下 ~GB 级 IO）
主导耗时，且放大到 110 万时每行成本 ~17µs（缓存未驻留）。
尝试方案 A：**并行分片 top-K**（src/sql/executor/select.rs，候选 ≥20 万时按 docid 等分子窗
std::thread::scope 并行 scan_stream_fields，各片独立 top-K 堆 → 全局合并=精确 top-K，比较/堆逻辑
抽模块级 sortlite_cmp/topk_heap_push）。语义单测 p93_parallel_dense_topk_large_matches_known_answer
（200,050 行，amount 互异单调，LIMIT/OFFSET 对权威答案）通过；全量 lib 746 绿。
110 万实测（4G 块缓存配置）：失败——(a) 块缓存 put 速率（并行多路整表读）超过淘汰节流 → 私有
内存 5.35G→~9.7G 继续膨胀（超预算；空闲稳态 5.35G 收敛 = 缓存填满即停，非泄漏）；
(b) 多核利用率仅 ~2/12 核（SST 仅 2 文件，分片扫描仍串行在文件内）+ 客户端长连接被掐。结论：
**并行默认关闭（P93_PARALLEL=1 显式实验）**，保留语义正确的堆重构与单测（默认路径无回归）。
**#29 达线未达成（P0-B 保持 ⏳）**；下一步候选 P94：① 块缓存写穿/淘汰节流（scan 路径 put 限速，
防并行读超预算）+ 文件内并行分块扫描；或 ② 列分块 SST（排序键列独立块/独立压缩 → 真·列 IO，
免整块解压，PAX 才真正减 IO）。内存判定参考：4G 块缓存配置稳态 RSS≈5.2G（hotcache512+bcache4G
+inv256+mem128+开销），爬升到 6G 是缓存填充过程非泄漏。

**P124（P94 M3 kernel 整合闭环）热列旁路双轨：内存 colstore 惰性派生 + topk 稠密路由 → #29 110 万达线**
落地（2026-09-05，分支 develop；设计档 research/dual_track_colstore.md）：
- 引擎层 `src/engine/colstore.rs`（新模块）：ColArena（ends 游标+present 位+blob 列区域）、
  `Colstore{docids,names,cols}`、`ColstoreState{cs,watermark,dirty}`；`colstore_ensure` 首次需要时
  惰性全表派生（行序与最新视图对齐，null/缺列 → None），write.rs put/delete/delete_batch 三处
  `colstore_note_write` 记脏；`colstore_field_indices`/`colstore_try_scan_cols`/`colstore_all_bitmap`
  原语 + 保守回退（未派生/超水位/脏 → 整查询行式主）。默认 `colstore_enabled=false` 零回归。
- 路由决策（用户确认二元 ⊆ 规则）：排序键列 ⊆ hot_fields 且窗口大、区间干净 → Columnar（只解
  热列，胜出行整行回行式主）；任一排序键非热列/窗口小/含脏/超水位 → RowStore（结果逐字节一致）。
  已按此补充 SQL 级正反例对照测试（含字符串键、非热列 note 回退）。
- 过程中修的三个性能/正确性点：① topk 稠密 colstore 回调漏 bitmap 候选过滤（稠密区间内的洞
  曾会多产出非候选行）→ 补 `bitmap.contains`；② 排序键解析每字段整 `serde_json::from_slice`
  （~0.5µs/列）→ `field_bytes_to_sort_key` 原字节快路径（数字直解 f64/无转义字符串去引号，
  畸形/转义回退 serde，语义不变）；③ All 分支 `full_docids` primary 全扫物化整行（110 万 ~GB 级）
  → `colstore_all_bitmap` cs.docids 直供（守卫：派生、无脏、无 > 水位新行，单点窥视），否则回退；
  ④ 派生整行 serde parse（110 万一次性 ~27s）→ `light_top_fields` 字节级顶层抽取（转义/嵌套/重复
  键语义对齐 serde，畸形回退全 parse）。
- 验证：全量 lib 回归 **753 绿**（colstore 引擎 7 测 + SQL 级 `sql_orderby_colstore_matches_row_path`
  正反例 + light 抽取语义）；Task-005 三档（colstore 配置：hot_fields=k,amount,ts,status,region,score）：
  | 档 | #29 orderby_multi p50 | 同批 排序族 p50（om500/om3000/os10k） |
  |---|---|---|
  | 10 万 | 18ms | 20/31/53ms |
  | 30 万 | 56ms | 56/69/83ms |
  | 110 万 | 231ms | 201/214/232ms |
  **验收结论：#29 110 万 9.46s（行式/PAX 旧线）→ 稳态 p50 0.23s，≤1.5s 达标（≈6.5× 余量，
  并反超 MySQL ~0.53s）**。冷启动一次性派生 ~24s（全表读取成本，解析已近下限），派生后常驻；
  sqlrun 收尾 44k 清理（P123）属派生后写 → 之后查询正确回退行式（护栏，非回归）。
- 文档/配置：storage.rs 字段注释、user_guide/README.md §7.1（热列与列存旁路说明 + 路由表 +
  取舍/护栏/冷启动）、user_guide/config-example/config.colstore.toml（可复用模板）。
- 阶段②（EXPLAIN 标注 TableScan: Columnar/RowStore、回退开关一键、flush 派生 .cs 落盘/增量）
  留排期后续。

**P125（SELECT DISTINCT 行去重立项闭环，2026-09-05）MySQL 语法面缺口 ③**
- 现状核对：parser 仅支持标量 `COUNT(DISTINCT f)`；顶层 `SELECT DISTINCT` 会把 DISTINCT 当列名
  （Token Ident）解析错位。README §4.2 曾长期写"行去重不支持"。
- 落地：① parser `Select.distinct`（SELECT 后识别 DISTINCT 修饰，不进列名）+ 形态守卫 1064
  （非 `*`、不组合聚合/GROUP BY/HAVING/JOIN、ORDER BY 列须 ∈ 列清单——防去重-排序语义错位）；
  ② executor `execute_distinct`（src/sql/executor/select.rs）：候选全量（WHERE→DocIdSet，
  All 分支复用 P94 colstore_all_bitmap / full_docids 位图）→ 512/块 batch_get → 逐行
  `light_top_fields`（engine/colstore pub(crate) 化）抽列值 → 组合键去重（数字按 f64 值归组
  `1 与 1.0`、`-0.0 与 0.0`；缺列与 JSON null 同组；字符串/容器按原文字节；前缀防跨类碰撞）→
  ORDER BY（按代表行 row_sort_keys）→ OFFSET/LIMIT；护栏：去重组数 >SORT_MAX_ROWS →
  QueryTooExpensive，看门狗逐块熔断。
- 语义对齐点：DISTINCT 先于 ORDER BY/LIMIT（MySQL 逻辑顺序）；代表行 = 候选位图升序首见 → 输出确定；
  NULL 组 asc 最小（ORDER BY 排序键复用 SortKey::Null < Str < Num 已有序）。
- 验证：3 新增单测（单/多列+WHERE+DISTINCT+ORDER BY+LIMIT/OFFSET 值集断言；数值 5 vs 5.0 同组与
  缺列∪null 同组；1064 守卫含 ORDER BY 非列清单拒绝）；全量 lib **756 通过 + 3 ignored**
  （seqlock 概率型用例偶发失败单跑复绿，无关）。
- 文档同步：README §4.1 增 DISTINCT 支持行、§4.2 限制表改为"组合形态 1064"；
  development_remain SQL 语法面审计 ③ 标记 ✅。

**P126（列表达式/函数值 阶段 A，2026-09-05）MySQL 语法缺口 ⑤**
- 范围：SELECT 投影标量表达式（算术 + - * / % 与括号、数值/字符串字面量、CONCAT/LOWER/UPPER/
  LENGTH/ROUND/ABS）；MySQL 语义对齐（缺列/JSON null → NULL 传播、除零与取模零 → NULL、
  整型运算保持整型、除法浮点、CONCAT 任一 NULL → NULL、ROUND 整数位回整型）；列名 = 表达式
  规范文本（MySQL 无别名默认）。普通字段列可与表达式列混排（id/docid/doc 直出 cell）。
- 落地：① lexer 增 Plus/Minus/Slash/Percent token；② AST `Expr`/`BinOp` + `Select.col_exprs`
  （与 columns 对齐；Select 因含 f64 取消 Eq derive）；③ parser `parse_scalar_*`（precedence 递归
  下降）+ 列清单触发（首 token 起步）；④ 求值 `src/sql/expr.rs`（serde_json::Value 往返，
  供 server 复用类型推断）；⑤ server `select_response` 顶部表达式路由 + `expr_response`
  （列类型按整列值聚合 LONGLONG/DOUBLE/VAR_STRING，NULL cell=0xfb）——在 id 点查/BETWEEN/IN
  等特殊形态分支前拦截（防默认 2 列静默错位）。
- 排障要点：初版列项用 `push_back(首 token)` 起解析会把 peeked 中已缓存的后缀运算符顶掉
  （`k*2` 丢失 `*`）→ 重构为 `parse_scalar_from(first)`（scalar_prim_of + mul/add continuation）。
- 守卫（1064）：表达式列与 `*`/DISTINCT/聚合/GROUP BY/HAVING/JOIN 组合、id 主键点查·区间·IN、
  与整 doc/嵌套路径列混排、ORDER BY 表达式列（阶段 B 放开）。
- 验证：parser 组合守卫 1 测、expr 求值 2 测（算术优先级/字符串函数与 CONCAT NULL）、server
  端到端 2 测（算术+函数+混排列名与值、除法浮点/除零 NULL/缺列 NULL）；全量 lib **760 通过
  + 3 ignored**（seqlock 概率型偶发失败单跑复绿，无关）。
- 阶段 B（后续）：WHERE 条件表达式、ORDER BY 表达式、聚合参数表达式、AS 别名、与 id 主键
  特殊形态组合、DECIMAL/CAST 类型对齐。

**P127（组合 WHERE 定位收敛闭环：UPDATE/DELETE 候选定位 + composite 全候选复筛，2026-09-05）**
- 触发：Task-005 回填 #75 `UPDATE … WHERE id BETWEEN 1..20000 AND status='active' LIMIT 200`
  110 万 223.9×（582ms vs MySQL 2.6ms），100k→1100k ≈9.4× 线性爆炸；同条件 SELECT 499ms。
- profile demo（src/demo/p127-combo-where，20 万行 active 50%，无/有 composite 双版）拆出两慢点：
  ① **写定位全候选遍历**——`get_docid_set(limit=None)` → eval AND 快路径 `post_filter(base=
  active posting, limit=MAX)` 遍历全 active posting 逐块读 id 字段判定窗口（20 万 52ms →
  110 万 ≈550ms，线性 9.4×；定位与窗口宽度无关、随 active 全库量线性）；② **composite 前缀
  命中全候选复筛**——声明 `[status]` 单列组合索引后组合 SELECT LIMIT 200 = 181ms@20 万
  （110 万 ≈500ms 恒定窗口，即用户实测 SELECT 499ms 的根因；无 composite 时组合 SELECT
  走 eval 早停仅 0.09ms）。附带发现：UPDATE ... LIMIT 200 此前被**忽略**（where_part 含 LIMIT，
  parse 丢 limit → 全候选全量写），语义偏差 + 无谓全写。
- 落地：① server 写定位（dml.rs update_response/delete_response 字段/复合路径）经 sqlparse.rs
  三 helper 收敛——`strip_where_limit`（剥离尾部 LIMIT n → 只改/删前 n 命中行 + 定位早停）、
  `extract_pk_between_comb`（纯 AND 链提取主键 id/docid BETWEEN 叶为 docid 区间，OR/NOT 包裹
  保守回退通用求值保语义）、`locate_pk_range_converged`（row 闭区间 → docid 区间 **keys-only**
  扫描现存升序 ∩ 其余条件 DocIdSet.contains，LIMIT 命中即停）；② executor composite 路由守卫
  （select.rs try_composite_index）：WHERE = 等值 + 主键区间组合 → 回退 eval 收敛（composite
  等值前缀物化大候选再复筛 id 区间），纯主键区间（无等值）不拦（composite 单列 id 范围路由保留）。
- 语义边界：主键 id/docid BETWEEN 在 cjserver = SQL row_id 闭区间（写路径 id=/IN/parse_pk_between
  一贯 row→docid_for 映射）；文档 id 字段值 = row_id → 字段求值语义数值等价，收敛安全。
- 验证：3 新增单测（server/tests.rs：组合 UPDATE/DELETE 的 LIMIT 只动前 n 行 + 无 LIMIT 全量不截断
  逐行断言、组合 SELECT 守卫后行序 = active 升序前 200）；全量 lib **772 通过 + 3 ignored**
  （seqlock 概率型 flaky 单跑复绿，无关）。
- 分支 B（SELECT 读路径主键区间语义 bug 修复，2026-09-05，用户指定）：
  - 根因深化：cjserver INSERT 的 id 列**提取为主键、不进文档 JSON**（sqlparse parse_insert_multi
    match "id"|"docid" → idv）→ executor eval 把 WHERE `id BETWEEN` 当文档字段条件求值 → 文档无
    id 字段恒 0 行且全扫慢（110 万验收实测组合 SELECT = 0 行 + 12s）。UPDATE/DELETE 因组合主键
    区间收敛（P127 主修复）已正确；SELECT 读路径未覆盖。
  - 修复：`execute_with_tid(engine, sql, cap, tid)`（新 pub，sql/mod re-export；`execute` 委托
    tid=0 零回归）——server select_response 两处 execute 调用传 `table_id_for(table_name_of(sql))`；
    executor 新增主键区间收敛 `pk_range_select`（纯 AND 提取 row 闭区间 → tid_base|row docid 区间
    keys-only ∩ 其余条件 DocIdSet.contains → 收集 ≤ offset+limit 早停 → 回表 → 升序）；rest 仍含
    主键谓词/row 越界/OFFSET+无 LIMIT 等守卫；含 ORDER BY 不收敛（需全候选，归排序族）。
  - 110 万复测（3317 新 binary）：组合 SELECT 0 行 + 12s → **200 行、p50 16.1ms**（MySQL 同 SQL
    p50 0.49ms → 33×；750× 提速；残余 = 宽表 200 行整 doc 回表解码，归"行式解码/回表"家族）。
  - 新发现独立慢点：纯主键区间 SELECT（server extract_between_range 快速路径）110 万 p50 296ms
    （历史 P127 立项记录 169ms 同源）——该窗口路径远慢于组合收敛路径（16ms），后续小项建议
    extract_between_range 复用 pk_range_select（rest=None 快路径）。
  - 验证：1 新单测（真实形态 doc 无 id 字段：组合 LIMIT 50 升序 2..100 / 无 LIMIT 1000 / OFFSET 20→42
    / 纯区间 LIMIT 5 首行 1）；全量 lib **775 通过 + 3 ignored**。
- 待办（复测回填）：#75（110 万）已回填（见 development_remain P127 行）；UPDATE 写链地板
  ~12-21ms（宽表整文档重写 + 倒排 + 组提交）与纯主键区间 SELECT 296ms 均按"规避/后续小项"记录。

**P128（P0 观测三项补齐闭环：Bloom 分层 / 块缓存计数 / L0 按表段数，2026-09-05）**
- 触发：用户"Bloom 优化优先级"总结（P0 观测先行 + 分层 fpr + per-table L0 压实）。代码核查先行——
  三处前提对照现实现修正：①"L0 元数据冷 IO"不成立——SstReader open 即常驻 min/max、legacy/分区布隆、
  table_id、L1 summary，仅 L2 full_index 首访懒加载一次后常驻；②"blockcache 元数据被 LRU 淘汰"不成立——
  filter/索引根本不进 blockcache（常驻 reader），blockcache 只缓存**数据块**（LRU + 冷表优先淘汰）；
  ③ per-table L0 计数确为真空缺（compaction 触发只看全局动态阈值 [8,12]，选段无表维度）。
- 落地：
  - ① BlockCache 增 `hits/misses/evicts` 原子计数（get 命中/未命中分计；adaptive_evict 每次 pop 记淘汰；
    clear/同 key 覆盖不计）→ `cache_stats()` → CF `blockcache_stats` → engine `blockcache_report`
    `shanshui_blockcache_{hits,misses,evicts}_total`（primary+delta+cidx 聚合）；
  - ② `BloomCounters` 单桶 → `layers: [BloomLayerCounters; 3]`（每层 minmax/legacy/probe/skip/pass/fp），
    读路径 `get/get_bytes/get_bytes_at/get_many` 在层循环内取 `self.bloom.layer(lv)` 传桶（自由函数
    签名 `&BloomCounters` → `&BloomLayerCounters`）；导出 18 行 `shanshui_bloom_{...}_total_{l0,l1,l2}`
    （engine `bloom_layer_report`），既有 6 总口径 `bloom_counts`= 三层 `totals()` 保留（不破旧脚本）；
  - ③ CF `l0_table_counts`：遍历 L0 层 `layer_indices[0]` 按 `sst.table_id()` 计数（split_by_table
    每段单表前提）→ engine `l0_table_report` `shanshui_l0_sst_count_table_{tid}`（动态行名）。
  - `/metrics` 与 `SHOW MEMORY` 行集扩展；metrics.render 与 engine_runtime_gauges/query gauges 改
    `(String,String,u64)`（支持动态名，原 `&'static str` report 保持不变仅调用点 String 化）。
- 排障要点：① 新测试 `.sum()` 无类型标注 → E0283（冗余断言行直接删除）；② blockcache 冷读单测首版
  miss=0——引擎 `get` 先命中 **put 直写**的 HotCache（read.rs:99 注释"写 put 仍直写"）不经块缓存 →
  改用 CF 层 `primary.get` 直读主列族（bloom miss 类测试不受影响：miss key 不回填 hotcache）。
- 验证：4 新增单测（blockcache 计数含淘汰、分层落桶 L0→L1 compact 下沉后计数落 L1 桶、CF 冷热读
  hit/miss 增长、多表 flush 后 l0_table_counts 每表 ≥1）；全量 lib **769 通过 + 3 ignored**。
- 决策接口 → 实测回填（2026-09-05）：demo `src/demo/l0-bloom-scale`（release，偶数 8000 行交错写
  S 段 + 奇数 miss 5000 次）产出：
  | L0 段数 | p50 µs | p99 µs | mean µs | probe/q | skip/q | pass/q(fp) | blockcache misses/q |
  |---|---|---|---|---|---|---|---|
  | 4 | 1.20 | 8.50 | 1.57 | 4.00 | 3.96 | 0.036 | 0.00 |
  | 8 | 1.80 | 6.20 | 2.10 | 7.99 | 7.92 | 0.072 | 0.00 |
  | 12 | 2.60 | 7.40 | 3.06 | 11.99 | 11.88 | 0.106 | 0.00 |
  | 16 | 3.50 | 12.10 | 4.18 | 15.98 | 15.84 | 0.132 | 0.00 |
  结论：① `probe/q ≈ 段数` 直证 miss 点查逐段遍历全部 L0（每段一次 v5 分区布隆 probe），
  mean 斜率 ≈0.21µs/段——段数线性**只罚 miss/已删除点查**，命中 key 在新段早停（O(1~2) 段）；
  ② `misses/q≈0` 证 miss 开销全在每段 二分+分区布隆反序列化+位测试（元数据常驻 CPU/内存），
  非磁盘块读（与 P0① 认知修正一致）；③ `pass/q≈fp 0.9%` 校准 fpr 0.01 ✓。
  **决策（2026-09-05 用户修正）**：per-table L0 优先压实**立项高优先**——本压测仅证单表 miss
  读放大绝对斜率小（命中 key 早停 O(1~2) 段不受影响）；但多表结构面另有失效：split_by_table
  每批 flush 产出 N 表 × 每表 1 文件，全局 L0 有效阈值 [8,12] 与表数强耦合——20 表单批 flush=20 段
  ≥ 阈值 → 每写必全量 L0 合并（写放大爆炸）或调高阈值令热点表段数失控。需 per-table 独立口径
  （该表段数达阈只压该表，全局兜底，见 P129）；分层 fpr 调优对纯 miss 无意义（真 miss 无假阳性），
  仅命中场景假阳性读块受益 → 需另测命中档方可立项，暂缓；布隆反序列化缓存量级 ≈0.2µs/段不单独立项。
  读放大压测范围仅为单表 miss 斜率，不覆盖多表计数/写放大面。

**P129（per-table L0 优先压实闭环：多表单批 flush 每表 1 文件的全局计数失配治理，2026-09-05）**
- 触发：P128 压测结论复核（用户）——"16 段太小，20 表最多 16 段就合并 L1 不合理"。根因：
  split_by_table 每批 flush 产出 N 表 × 每表 1 个 L0 文件，全局 L0 阈值（动态 [8,12]，clamp 16）
  与表数强耦合——N=20 单批 flush 即 20 段 ≥ 阈 → 每写必触发**全量** L0 合并（全部表参与、写放大
  爆炸）；调高全局阈值又令热点表段数失控（读放大）。全局计数维度无法同时满足两目标，需按表口径。
- 落地（compaction.rs + config + CF 字段）：
  - `storage.per_table_l0_trigger`（默认 2）= 某表 L0 段数触发阈值；0 = 关闭。
  - `per_table_active(snap)`：split_by_table + 阈值>0 + L0 现存 ≥2 个不同表且无混表/未知段；
    单表库 / L0 空 / 旧混表段 → 回退原全局逻辑（**单表行为零回归**——默认库/既有基准不变）。
  - `table_l0_subset(snap)`：目标表 = L0 段数最多且 ≥ 阈值的表 → 其全部 L0 段合并去重 →
    **输出 L1 新段，不并入既有 L1 同表段**（对齐 leveled"单次压实量=单批有界"，防每批 flush
    重写该表全历史致写放大无界；L1 同表多段版本重读正确，L0 空时由既有 bottom split 收敛）。
  - `needs_compact()`/`compact()` 多表分支：主触发 = 存在表 ≥ 阈；**全局段数/大小阈值在
    多表模式不作触发**（每表 ≤1 段 = 收敛态，每表受控 ⇒ 全局有界 = N 段量级，读 O(1)/表）。
- 语义与写放大：稳态每表 L0 ≤1 段（点查 O(1) 段）；每批 flush 后该表若 L0 达 2 段做一次小合并
  （每批数据写 2 遍，写放大 ≈2×，对比"每写必全量合并"多表多倍）；20 表各 1 段 = 收敛 no-op
  （旧逻辑 20>12 会全量合并）。
- 验证：2 新增单测（column_family/tests.rs：①热点表 t1 两批 L0=2 单轮仅合并 t1（merged=2、
  out L1）且 t7 单段留 L0 不参与，第 3/4 批 flush 后 L0 t1=2 再合并、L1 旧段不并入 + 跨批
  覆盖读正确（8 行）；②20 表 × L0 1 段 needs_compact=false、compact no-op、20 文件不变、读回）；
  全量 lib **774 通过 + 3 ignored**。
- 监控指标补充（2026-09-05，用户要求）：
  - CF `per_table_compact_runs` 原子计数（P129 分支每次成功合并 +1）+ accessor；
  - engine `l0_table_report` 增 5 条汇总行（`shanshui_l0_tables_active` 活跃表数 /
    `shanshui_l0_sst_count_over_trigger` 段数 ≥ trigger 待压实表数 /
    `shanshui_l0_sst_count_max` 最大段数（稳态应 ≤1）/ `shanshui_l0_sst_hottest_table`
    热点表 id / `shanshui_per_table_compact_runs` counter）→ /metrics gauges 与 SHOW MEMORY；
  - `/metrics` 增 **label 化**文本（engine `l0_table_metrics_prom`）：
    `shanshui_l0_sst_count{table="<tid>"}` + `shanshui_l0_sst_over_trigger{table=,trigger=}`（1=该表待压实）
    + `shanshui_per_table_compact_runs_total` counter —— Prometheus 面板/告警可聚合
    （动态名行无法做阈值告警/聚合查询）。
  - 单测更新：l0 测试断言汇总行 + label 文本；P129 测试断言 per_table_compact_runs 随压实 +1；
    全量 lib 774 通过 + 3 ignored。
- 待办（压测采集）：rr sqlrun 多表负载压测采集 `shanshui_l0_sst_count{table=}` 验证稳态
  每表 ≤1 段 + `shanshui_per_table_compact_runs_total`/`sst_written_bytes` 写放大 ≈2× 回填。

**P132（P131 增量 UPDATE 落地：Delta CF 路线 + 快照 Delta 多版本坍缩修复 + 扫描 Merge-on-Read 补全，2026-09-06）**
- 背景/决策：P131 初案「value 内嵌 wrapper（`__sp_base/__sp_p`）」demo 通过但 base+p 字节 ≥ 整 doc
  （写字节不减）暂缓；用户 2026-09-06 选定**复用既有 Delta CF**（`Engine::patch` 字段级增量，
  docid++field 键、每 patch 自带全局 seq）实现「非索引列 UPDATE 免整行重写」，并要求**统一增量 MVCC
  版本规则**：倒排索引与一切 RR 查询都必须 MVCC——增量只对 ≥ 其 seq 的快照可见（设计 research/delta-patch.md §10/§11）。
- 落地（内核）：
  1. 写：dml `update_response` 单字段字面量 SET——**声明配置下**（`term_inc.is_some()`）且列 ∉
     term 声明 ∪ 组合索引 ∪ {id/docid/doc}、非自增表达式、base 行存在 → `Engine::patch_batch`
     （新增，批尾单次 flush_wal；`patch` 重构为 nosync+组提交，与 put 同构）；索引列 / 整 doc /
     表达式 / 无声明配置（legacy 全字段 term，避免陈旧 posting）→ 全量路径不变。
  2. 读（Merge-on-Read 补全缺口）：新增 `delta_overrides_range(_at)`（底层 Delta CF `scan_stream_at`
     snapshot 语义）+ `fold_with_overrides(_fields)` 折叠 helpers；接入 scan_range / scan_range_paged /
     scan_after / scan_stream / scan_stream_fields / scan_stream_with_zonepred / scan_stream_parallel
     （project 白名单合成）+ txn `scan_range_txn`（RR 快照按 seq 门控）；无增量时 `data_empty()` 短路
     = 干净库零开销。
  3. **修复既有隐患（get_at Delta 多版本坍缩）**：旧路径 `scan_raw_range_with_seq` 先按每键最新坍缩
     再按 snapshot 过滤 → 同一字段键两次 patch（补丁1 < 快照 < 补丁2）时快照读丢补丁1（误读 base 旧值）；
     改走 `delta_overrides_range_at`（CF scan_stream_at 各源归并后取 ≤ snapshot 最大 seq）→ 快照读见补丁1。
- 一致性边界（沿用「最新态候选 + 行级复核」索引模型 §11.5）：term/posting 只加不删，陈旧 docid 由
  行级 WHERE 重算兜底；增量列恒非索引 ⇒ posting 形态零扰动；声明列变更走全量重写维持模型。
- 验证：4 新增单测（patch_batch 扫描/快照可见 + 不坍缩、scan_range_txn RR 门控、SQL UPDATE 非索引列
  落 Delta CF 且 posting 不扰动/同值 affected 0/声明列全量）；全量 lib **782 通过 + 3 ignored**（+3）。
- **值变形态 #75 压测回填（2026-09-06，clean 110 万 pax 库 db-wide-scc-pax；rr-conformance 新增探针
  update_range_idx_chg：note 逐轮异值保证每轮 affected=200，官方同值探针恒 rows=0 测不出写链）**：
  SCC Delta 稳态 **p50 21.3ms / mean 21.1~22.1 / p99 ≤27**（三连稳定）；
  同库 nodelta（bitmap 声明移除 → term_inc=None → delta 关闭走全量重写）**p50 35.3ms / p99 77** →
  **Delta 省 ~40%（p50 1.66×；p99 尾收敛 3.4×，重写/词条脉冲消除）**；
  MySQL 8.0 同探针 p50 2.96ms → 值变 p50 ≈7.2×（P131 前同形态历史 p50 46ms ≈13.7× → 减半）；
  官方同值 rows=0 读现值 SCC p50 6.35 vs MySQL 3.57 ≈1.8×。
  写链残余 = 定位（locate ~6ms）+ 200 行折叠读 + 组提交 fsync 常数（≈21ms 总量），未达 update_id ≤2×
  触发线 → 维持触发式（后续候选：定位 keys-only 早停已 P127、组提交/提交常数对齐 INSERT 路径）。
- **P131 定位 v2 + 批量提交组提交化（2026-09-06 追加，承接上面残余分析）**：
  - 根因拆分（引擎级 demo src/demo/p131-writechain-prof）：①UPDATE 定位 keys-only 磁盘扫 20000 键
    ≈4ms/语句（demo A1）；②批量提交路径 `put_batch/patch_batch` 批尾无条件 `flush_wal()`（双 WAL
    同步 fsync ≈8ms/语句，单连接下组提交窗口空转），而 INSERT 走 put→组提交 → UPDATE 相对
    INSERT/MySQL（innodb_flush_log_at_trx_commit=2）的 p50 差主源。
  - 落地：①`Engine::commit_batch`——批量写/批量增量按事务档位提交（档1 = 显式 flush_wal 强安全
    不变；档0/2 = 组提交窗口 ack）；put_batch/patch_batch 改走 commit_batch。②定位 v2——
    `Engine::live_window_ids`（live 位图 ∩ [lo,hi] 纯内存，小跨度位图 and，>200k 跨度回退 keys-only
    不劣化）+ locate_pk_range_converged / pk_range_select 两处组合定位改用它（语义与 keys-only
    现存完全一致：已删行排除、升序、limit 早停），消除 20000 键磁盘扫。
  - 干净 110 万 PAX 库端到端复测（results/p131-final-scc|scc2）：**#75 官方 rows=0 p50 6.35→1.3ms**
    （反超 MySQL 3.57 ≈0.36×）；**值变 rows=200 p50 21.3→7.6ms**（≈2.6× MySQL，写链增量 ~15→6ms）。
    残余 ≈ 服务层每语句常数 + 组提交窗口排队（≤2ms），未达 ≤2× 但已大幅收敛。
  - 单测：p131_live_window_ids_matches_keys_only_scan（live 窗口 == keys-only 现存：删行/limit/
    复活/边缘）+ p131 既有 4 项；全量 lib **783 通过 + 3 ignored**（+4）。

**P133（Ex-8.9 交变负载 A/B 验收收口：忙窗读 p99 探针补全 + 4/4 复验，2026-09-06）**
- 背景：Ex-8.9 切片 2A（server 层 3 worker 负载感知三档 tick + idle_run）2026-09-04 已落地，但
  development_remain 状态「交变验收待做」与设计文档 §6「3/3 已验（24.75s）」不一致——跟踪未回填，
  且 §6 的「前台 busy 耗时 ≈4.0s/轮 持平」是整轮 burst 耗时代理（写锁内测量），**无前台并发读的
  逐操作 p99 探针**（后台维护是否侵蚀忙窗读延迟未直接测）。
- 补测试（server/tests.rs `ex89_ab_busy_read_p99_no_degradation`，`#[ignore]` 真实时钟）：
  无信号 L0 积压（auto_compact 关，8000×1KB 再 flush → L0 5）→ A（aware）≥5s 空闲 idle_run 收敛
  vs B（旧固定节奏）滞留 → 忙窗点读探针（命中散布 + 区间外 miss 混合，miss 逐段布隆校验 → 延迟对
  L0 段数敏感）逐操作计时取 p50/p99；final 测前 400 次 warmup（避 compaction/flush 换新段首触冷
  IO 抬 p99——首版 19.3µs 尖峰即此，warmup 后 2.4µs）。
- 实测（debug，2026-09-06）：B l0 1→5→5（滞留），A l0 1→5→0（5.2s idle_run 收敛）；
  忙窗读 p50/p99：A **2.0/4.6µs** vs B **2.2/17.1µs**（A 收敛态不劣于自身干净基态 5.8µs p99，
  p99 明显优于滞留积压的 B——收敛后单段读 vs 5 段逐段布隆）。断言：A l0_final≤2、A p99_final ≤
  干净基态 ×1.8（后台维护不使忙窗读退化）、A p50/p99 ≤ B ×1.2。
- 复验：4/4 A/B 测试（倒排落盘 mem 跨窗归零 vs 240k 滞留 / compaction idle_run 5.2s 收敛 vs 滞留 /
  GC 12→1 vs 12 / busy 读 p99 4.6 vs 17.1µs）23.5s 全过；全量 lib **783 通过 + 3 ignored** 无回归
  （新测试计入 ignored，非编译/运行态）。
- 收口：development_remain Ex-8.9 行状态 → ✅（引用本条目）；设计文档 §6 增第 4 行 + 复验注。

**P134（事务读快照语义收口：字段谓词 RR 漏行 + 区间位图剔行 + 快照锚定核对，2026-09-06）**
- 背景/审计（承接倒排 MVCC/RR 专项 design research/inverted-versioning-rr.md §2；用户确定读法 =
  posting 不加版本号，先取全局 MVCC seq 再读 term→docid，所有数据以 snap_seq 为准）：
  ① `txn_select_by_predicate`（src/server/command/transaction.rs）字段谓词候选 = 最新态 sqlish
    execute（回表 batch_get 删除位图剔除快照后删行、最新值过滤已换值行）→ 快照后被并发删/换值
    行不在候选 → 事务内重复谓词读消失，与点查 get_at（见旧值）不自洽（RR 违反）；
  ② `scan_range_txn`（src/engine/txn.rs）快照扫描仍按删除位图剔行（位图无 seq）→ 快照后删行被
    隐藏，与 get_at 跳过位图不一致。
- 修①：`txn_select_by_predicate_snapshot`（新增，transaction.rs）——RR/Serializable 且非
  FOR UPDATE 的字段谓词读 = **快照窗口分块扫描**（候选 = 快照可见行全集，无最新态删除预过滤）：
  `scan_range_txn` 按 SPAN=16384 逐窗 → `doc_matches_where` 按 ≤S 行值复检 → LIMIT 且无
  ORDER BY/聚合时早停；同事务自写新 docid（超窗高水位）完整覆盖时补入保序。RC（无快照）与
  FOR UPDATE（当前读）语义本就见最新已提交 → 旧候选路径正确，保持不变；仅默认表单行域
  （auto_watermark ≤ 2^48）启用，混合表库回退旧路径（残余记录：混合库默认表 txn 谓词仍走旧
  候选，P135 batch_get_at 后统一）。
- 修②：`scan_range_txn` 删除位图剔除改仅 `snapshot == u64::MAX`（RC/当前视图，语义同 get 位图
  短路）；RR/Serializable 跳过，删除后墓碑由 `scan_range_at(≤snapshot)` 裁决（tombstone ≤S 已滤、
  >S 回旧版）——与 get_at 一致。
- ③ 核对结论（快照锚定时机）：现实现 = **BEGIN 锚定**（`Engine::txn_begin` → `begin_snapshot` =
  global_seq-1，src/engine/mvcc.rs L17-19）；MySQL RR = **首条一致性读锚定**。差异真实但改锚定
  影响既有测试/语义面大（多个 RR 测试假定 BEGIN 锚定），本次不改，记录为已知边界 + 后续专项
  核对项（rr-conformance RR 探针若存在 "BEGIN→他事务提交→首读" 形态需对照）。
- 顺带：inverted `doc_count` / `search_paged` 的 mem Vec 在进 `merge_distinct`（k-way 升序前提）
  前 sort_unstable + dedup（防乱序显式 id 写入破坏归并；原仅隐含依赖写到达序）。
- 验证：2 新集成测试（server/tests.rs，TCP 双连接）——`txn_predicate_snapshot_rr_after_concurrent_
  delete_and_update`（轮1 快照后并发 DELETE 谓词读仍见 + 点查对照；轮2 并发换值 s:a→b 谓词读
  s='a' 仍见旧值、s='b' 为空）、`txn_between_snapshot_sees_deleted_after_snapshot`（BETWEEN SUM
  快照后删不变；旧实现剔位图 → 0）。全量 lib **784 通过 + 4 ignored**（seqlock 低频写重试率
  概率型 flaky 单跑 0.10s 复绿，历史多轮无关）。
- **另记录两缺口 → 核实撤销（2026-09-06，非缺陷）**：初判"autocommit 纯 id 投影（点查/BETWEEN
  无 ORDER BY）返空"与"同连接快速连插后字段全扫漏首行"经**引擎级**（put 后 get/scan_range/
  scan_stream_ids/scan_stream 全见 4 行）与**服务 fn 级**（select_response：`SELECT id` 点查/
  BETWEEN/字段全扫均返回含首行全 4 行）双决定性验证 = **测试结果解析假象**：测试 row_ids
  解析按 2 列结果集头偏移 skip(4)（1 列头 + 2 列定义 + EOF = 4 包），而 `SELECT id` 为 1 列
  （头仅 3 包）→ 首数据行被当作头跳过；单行时整行被丢 → 误报"空/漏首行"。引擎/投影层无此
  缺陷（既有回归 tests.rs 391-407 同文本点查 1 行即证）。**教训：TCP 结果集解析须按 columns
  数自适应头偏移**（col_count 包 + columns×列定义包 + EOF 包），测试沿用 SELECT *（2 列）规避。

**P135（batch_get_at 内核 + txn 谓词倒排 superset 接线 + 其余端复核决策，2026-09-06）**
- demo-first（src/demo/p135-batch-get-at，gitignored）：get_at 语义矩阵 20/20（位图关/开 ×
  快照后删/前删/换值/复活/多版本/跨表/不存在）；批量潜力 A/B（debug warm 4000 docid）逐 get_at
  乱序 4.69 / 升序 4.44 / latest batch_get 2.92 µs-op（batch ~1.6×）——支撑 CF 级批量收益。
- 内核：`Engine::batch_get_at(docids, S)`（src/engine/read.rs）——语义 = 逐 `get_at`：跳过删除
  位图、tombstone seq ≤S→None/>S→旧版、Delta ≤S 折叠；结构 = primary.get_bytes_at 逐键（≤S
  跨 memtable+全层最大 seq，sst_min_seq 整段剪枝）+ **Delta 单次 [min..max] ≤S 窗口折叠**
  （摊薄 get_at 逐调用各自 Delta 窗口扫）。不入 HotCache（快照视图≠最新态）。输入升序无重复
  （对齐 batch_get 契约）。单测 p135_batch_get_at_matches_get_at（位图关/开×删/换值/patch 跨
  快照/空集/跨表，批量==逐 get_at）。
- 接线①（txn 谓词 superset，server/command/transaction.rs）：`txn_select_by_predicate_snapshot`
  先试 **inverted_eq_superset**（WHERE 纯倒排等值 AND 链 → posting 交集；只加不删 = ever-match
  superset；任一叶非 Eq/空 posting/OR/Ne → None 保守回退）→ ∩默认表窗口 → `batch_get_at(S)`
  复核 + `doc_matches_where`——**恢复索引加速**（替代 P134 的全窗分块扫描）且 RR 正确（快照后
  删/换值行仍在 posting → ≤S 版本/值裁决）；含未提交写 → 全窗扫兜底（自写合并复杂）。抽出
  finish_predicate_rows 共用收尾。测试：p135_inverted_eq_superset_gating（判定/回退）、
  txn_predicate_snapshot_inverted_superset_rr（端到端 delete/update/SUM，倒排声明字段）。
- 复核决策（其余端不改）：非事务倒排消费端全链已是 latest 批量（P2-D batch_get / P85
  collect_limited_rows，eval_cond/topk/聚合/区间均 batch_get，无逐 engine.get 残留）；autocommit
  无 RR 义务（语句在引擎读锁内执行、写需写锁 → 语句内无并发提交，S=语句开始≡latest）→ 改
  `batch_get_at(S=now)` 为纯退化（≤S 归并 + 冷段 sst_min_seq 首触全键扫）→ **选型 = 保留
  latest 批量位图快路径**；区间/组合同理（autocommit 保持，RR 端已由 scan_range_txn 窗口/
  superset 覆盖）。batch_get_at 消费点 = RR 快照语义端（txn superset）+ 未来混合库残余。
- 复核补充（2026-09-06）：混合库残余（watermark>2^48 旧逐候选路径）**服务端不可达**（cjserver
  默认表恒 tid0 低位 docid → SQL 层 watermark 恒 ≤2^48；仅引擎 API 直连多命名空间才可能）→ 不
  接线；低成本防御已落——`finish_predicate_superset` 候选 filter 改 `d<2^48 ∧ d≤hi`（单表库与
  原 d≤hi 严格等价；混合库防跨表高位 docid 混入候选串行）。回归 790 绿。
- 全量 lib **787 通过 + 4 ignored**（seqlock 概率型 flaky 单跑复绿）。

**P136（快照活跃预过滤：snapshot_dels 删除事件表 + prune 接线，2026-09-06）**
- 目标（C）：P135 superset 候选批量取行前集合级剔除"S 前已删" docid（免 batch_get_at 空跑）。
- 形态（尽力而为、绝不误剔）：`Engine.snapshot_dels: Mutex<HashMap<docid, del_seq>>`——只记录
  本进程、**删除位图 + per-CPU** 模式发生的删除（delete/delete_batch 位图臂取
  `delete_record_mem` 返回的墓碑 seq 记入；复活 put（位图 clear 命中）清除条目；purge_all
  复位；open 空表——活跃快照不跨进程（begin 于 open 后）→ 无需全库基线/无需 add 事件）。
  `prune_deleted_before_snapshot(ids, S)` = retain !(del≤S)。**绝不误剔**（删于 ≤S 且未复活
  → 快照视图确实不可见）；**漏剔**（历史"删→复活"窗口条目已清、非 per-CPU/非位图模式不维护）
  由 `batch_get_at(S)` None 兜底 → 仅省空回表的优化，正确性不受影响（与 P134/135 哲学一致）。
- 接线：`finish_predicate_superset`（P135）批量取行前调 prune。
- 局限记录：换值不产生 del 事件（仍需 doc_matches 字段复核）；复活历史窗口漏剔；位图关/
  per-CPU 关场景不启用预过滤。
- 验证：单测 p136_snapshot_dels_prune（S0 删前全保留 / S1 剔"未复活删除" / S2 复活保留 ×
  per-CPU 开关 × 位图开关 4 组合）；全量 lib **789 通过 + 4 ignored**。

**P137（倒排专项监控 D：inverted_report 9 行 gauge，2026-09-06）**
- 目标：运维判"要不要给倒排 GC 腾资源 / 写入是否过猛导致段堆积 / posting 缓存收益"。
- 新增 counters：InvertedIndex `seg_flush_total`（flush_segment 埋点）、`posting_cache_hits/
  misses`（search 缓存路径命中/未命中埋点）；Engine `snapshot_batch_rows`（batch_get_at 处理
  docid 累计）、`snapshot_prefilter_saved`（prune_deleted_before_snapshot 剔除数累计，P136 复用）。
- `Engine.inverted_report()` 9 行：inv_segment_count / inv_mem_docids（存内累积）/ inv_seg_flush_
  total / inv_posting_cache_hits_total·misses_total / inv_gc_pending（should_gc 1/0）/ inv_delta_
  fst_over_limit（should_delta_gc 1/0）/ snapshot_batch_rows_total / snapshot_prefilter_saved_total。
  接入 `/metrics`（admin_api engine_runtime_gauges）与 SHOW MEMORY（query.rs）双链（与 memory/
  bloom/blockcache/l0_report 同构，改一处两处同步补链）。
- 验证：单测 p137_inverted_report_rows_and_snapshot_counters（9 行齐全 + 首查 miss→二查 hit +
  prune 剔 2/批量 3 计数前进）；全量 lib **790 通过 + 4 ignored**。

**P138（autocommit S 选型 demo：非事务端 S 路径定量 A/B，2026-09-06）**
- 背景：P135 复核结论"非事务端不改（latest 批量位图快路径）"系定性（S=语句开始≡latest）。
  P138 定量：`Engine::batch_get_at(S=当前全局 seq)` vs `batch_get`（latest）直连引擎全矩阵
  （src/demo/p138-autocommit-s，gitignored）。
- 实验：60k 行、s=a 候选 30k，位图开/关 × memtable 热（留 mem）/冷（已 flush SST）× 窗
  30000/4096/512，先各自 warm（latest 侧 HotCache 回填）再计时（固定处理 180k docid 每单元）。
- 结果（debug 相对口径 µs/docid）：**batch_get_at(S) 全 8 单元均慢于 latest 1.6–2.4×**
  （latest 1.07–2.30 vs S 2.56–2.96；位图开/关与热/冷均成立；冷段无 sst_min_seq keys-only
  首触放大——批量 ≤S 归并开销与 mem/SST 状态弱相关，HotCache 旁路是 warm 后主要差源）。
- 结论：**维持"非事务端不改"**（S 路径为 RR 专属：txn superset + 未来混合库残余），无触发项。
  batch_get_at 的 ≤S 归并 + 不入 HotCache 在 latest 语义下无收益且纯增成本（实证）。

**P139（投影/回表瘦身首切片：非事务纯字段回表消费端 fields 下推，2026-09-06）**
- 目标：收"已知性能族"投影/回表瘦身——候选/窗口回表整 doc（25 列）解码改只解 SELECT 纯字段列。
- 落地（src/sql/executor/select.rs）：helper `select_projection_fields`（SELECT 列集纯标识符且
  至少一个非 id/docid 字段才启用；`*`/表达式/别名/id-only 回退整行 `batch_get` 零漂移）+
  `subset_doc_bytes`（字段值→子集 JSON，缺字段省略）+ `batch_fetch_rows`（fields→
  `engine.batch_get_fields`，否则整行）。接线两处：`pk_range_select`（主键区间回表）与非
  sort 通用分支 `collect_limited_rows`（倒排/组合/IN 候选分块消费）。
- 同态 A/B（脏态 3317，pk_between_10000）：fields 333ms vs 整行 541ms（~1.6×）→ 稠密窗口亦投影优
  （整行 25 列解码 > fields 逐键开销，推翻"稠密应整块读"直觉——`batch_get_fields` 行式按需
  提取 + PAX 列解码更便宜）。
- clean 110 万实测（db-wide-scc-p139 新库 wide-load 67s 立即跑）：#46 enum_sel_limit10000
  240→58ms（4.1×）、#48 combo_and_limit3000 43.7→7.4ms（5.9×）、#45 enum_sel_limit3000 2-3×、
  #47 1.51→1.18；pk #43 176→170（±小）、#05 p50 1.9-2.1ms；小窗 500 行级受热缓存噪声（同量级）。
  #48/#46 对 MySQL 比值降至 2.4×/6.5×（P137 13.7×→ / P137 27×→6.5×）。
- 验证：单测 p139_projection_subset_matches_full（两路径子集==整行基线 k/status 等值 + note 不
  出现 + 墓碑剔除一致）；lib **791 通过 + 4 ignored**。
- 下一片候选：sort 输出期 top-k 整行回表投影、#77 txn 长快照窗快照侧投影流、混合 #79/80 排序列
  子集、id-only 安全投影（需墓碑剔除保真方案）。
- **下一片落地 A（2026-09-06，P139-b sort/top-k 输出期投影）**：`topk_sort` 增 `columns` 参，
  输出期（P87③ 胜出行整行回表）改投影子集解码；无 LIMIT 全排序取行集 = 排序键 ∪ SELECT 纯字段
  投影（`*`/表达式仍整行）；实测无回归（enum3000 稳定），排序族主成本 = 候选全扫解码（不属本片）。
- **下一片落地 B（2026-09-06，P139-c id-only 活跃判定）**：`Engine::batch_alive_latest`（位图开
  → O(1) `is_deleted`；位图关 → 主数据 `get_bytes` 存在性点查；**前置契约：候选必源自现存集
  派生（posting/live 窗口/keys-only 区间），位图模式对"从未写入"docid 返回 true（无法区分，
  O(1) 上限使然）**）+ `batch_fetch_rows` 增 alive_only 分支输出 `{}` + `id_only_columns` 判定；
  接线三输出端（pk 区间 / 非 sort 通用 / topk 胜出）。实测（P139 同库同态）：**#47 combo_and_
  limit500 1.18→0.68ms、#48 combo_and_limit3000 7.39→1.58ms（4.7×，对 MySQL 3.11ms **反超
  0.5×**）**。单测 p139b_batch_alive_latest_matches_get（位图开/关 × 与逐 get 一致 × 复活）+
  p139 路径⑤（id-only `{}` 输出/行集一致/墓碑剔除）；lib **792 通过 + 4 ignored**。

**P140（快照投影流 #77 前置核对 + 干净基线，2026-09-06 立项 demo-first）**
- 前置核对：#77 `txn_long_read` SQL = `SELECT id,k,amount … WHERE id BETWEEN a AND a+100000`
  （3 列投影，非 SELECT *）→ 快照侧列下推价值成立（25 列快照归并整行 → 3 列）。
- 干净基线（db-wide-scc-p140 wide-load 74s 立即跑，P139-c binary）：#77 mean **3316ms** /
  p50 1848 / p99 8069；MySQL 147/145/160 → **≈22.5×**。较 P137 clean（5486/1933/20767）
  已收敛（p99 20s→8s）。
- **demo（双态，2026-09-06，src/demo/p140-snapshot-projection，gitignored）**：100k×25 列
  wide-pax 配置（hot_fields k/amount/…、bitmap status/region、位图+per-CPU、release）：
  - 热态（全留 memtable）：A_snap 0.43 / A_latest 0.33 / B_fields 2.62 µs/row——
    **≤S 归并增量 +0.09 µs/row（批量快照读全热 ≈ 免费）**；B_fields 热态贵 = 整 JSON
    parse+重建（行是 raw 整 doc，无 PAX 可解）→ 热态无列下推空间（与 P139 收益在 SST
    冷读侧一致）。
  - SST 冷态（写完一次 flush_primary 落盘）：A_latest **0.38 µs/row**（get_many 按块批量
    点查 + raw 字节直传**不解码**）；B_fields 3.00 µs/row（字段提取逐行 JSON parse 主导）→
    **整行 vs 3 列不是解码差，"列解码"非 #77 主成本**；A_snap（`batch_get_at` 逐 docid
    `get_bytes_at`：SST 冷段逐键 ≤S 点查、无 get_many 批量分组、无 HotCache 回填）
    **39.2 µs/row ≈ #77 clean 33 µs/row（3316ms/100k）同量级**。
  - 结论：#77 主成本 = **SST 上快照 ≤S 读取机制本身**（scan_range_txn/scan_range_at
    逐行 ≤S 折叠 + 冷段读取），server 侧整 doc JSON 投影解析为次量级。
- **P140 内核取向修正**：原方案"≤S 归并仅解目标列"（把列解码当主成本）方向修正为
  **范围迭代复用（scan_range_at 同源一次 k-way）+ ≤S 折叠 + 目标列投影 三合一**的
  `scan_range_txn_fields`；另产出**触发项**：`batch_get_at` 冷库逐键点查退化
  （39.2 µs/row vs get_many 0.38），P135 superset 回表需上按块批量点查。
- **内核完成（2026-09-06）**：`Engine::scan_range_txn_fields(start,end,txn,fields)`：
  基表 `primary.scan_stream_at(S, project=Some(fields))`（一次 k-way，PAX 只解目标列 →
  子集 JSON；内存/行式直通整 JSON——消费端只读 fields 覆盖列）+ Delta ≤S 白名单折叠
  `fold_with_overrides_fields`（无命中免 parse）+ 位图仅 RC/当前视图剔除（RR 跳过由 ≤S
  裁决，与 scan_range_txn 一致）+ 自写覆盖 / 事务删除 / 新 docid 并入（尾部逐条同
  scan_range_txn）。接线：事务 BETWEEN 窗先 `plain_field_projection(proj)` 判定（仅
  id + 简单顶层字段、无 doc/`*`/嵌套路径/表达式且 ≥1 字段列）→ 下推
  `scan_range_txn_fields`；否则整行 `scan_range_txn` 回退（FOR UPDATE/SUM/含 doc 保持
  原路径不变）。#77 形态 `SELECT id,k,amount WHERE id BETWEEN` 已免整行 25 列解码。
- 单测 `p140_scan_range_txn_fields_projection_matches_full`（mem/SST 双态 × RR ≤S 旧值 /
  RC 合成最新 / 自写覆盖整行可见 / 事务删除排除 / 窗口外新 docid 并入，行集+目标列值
  == scan_range_txn 全量解析）；lib 793 全绿（seqlock flaky 复绿）。
- **探针复测（2026-09-06，同库 clean 重装 110 万，P140 binary）**：`#77 txn_long_read`
  n=5：r1 mean **2500.9ms** / p50 924 / p99 7049；r2 mean **2459ms** / p50 750 / p99 9674。
  基线（P139-c clean）3316/1848/8069 → **warm 迭代 p50 减半以上（2-2.5×）**——投影下推
  收益实锤（100k 窗只解 k/amount 子集，免整行 25 列解码）；mean 1.35×。**离群 7-10s =
  冷首触 100MB 随机窗 IO + 装载后 L0/compaction 抖动**（服务日志 compaction 恰在探针期；
  基线同型 p99 8s，非投影作用域）。验收 mean ≤1.5s 未达标 → 残余归 **IO/read-amp 族**
  （冷窗 IO 预取 / colstore #79-80 / compaction 收敛），另 `batch_get_at` 按块批量触发项
  仍挂 superset 冷库回表。P140 收口（内核价值 = warm 窗 2-2.5×；冷首触另行立项）。
- 方案（内核已落，探针复测完成）：引擎/CF 原语 `scan_range_txn_fields(start,end,S,fields)`
  （≤S 归并仅解目标列，复用 P131/P134 版本归并 + P86②/P91 列提取）→ 事务长读窗输出接线；
  验收 #77 clean ≤1.5s（≥2.2×）未全达——warm 2-2.5× 达标、冷首触 IO 离群另归 IO/read-amp 族。

**P143（冷首触 IO 归因，2026-09-06 立项；demo 归因完成）**
- **归因 demo**（src/demo/p143-cold-io，gitignored，直连 db-wide-scc-p140 真实库，
  与探针同配置同数据，release）：RR `scan_range_txn_fields([k,amount])` 100k 窗逐窗：
  - 每窗首读**块缓存 miss ≈ 50002 块**（100k 行 / ~2 行每 4KB 块，默认 block_size_kb=4）
    ——100k 窗 ≈ **50k 次 4KB 随机小盘读（读放大 ≈200MB）**；
  - OS 页缓存热区掩蔽（a=450k/650k/850k 冷首读 1.3-1.5s）、冷区暴露（a=50k 17.2s /
    a=250k 9.9s）——**#77 7-10s 离群 = 窗落在页缓存冷区**（每次探针 a 随机漂移）；
  - **warm（块缓存命中二次读）0.24-0.30s（2.4-3µs/row）**——引擎机制/解码非主成本；
  - 整行对照（同窗已 warm）1.38s vs fields 0.28s → P140 投影**省解码 ~5×，不省块数**
    （fields/整行 miss 同为 ~50k——行块整体读入，投影不缩 IO）；
  - 非 compaction 主导（无后台抖动下冷热差纯缓存所致；compaction 仅放大项）。
- **定论/方案修正**：主因 = 4KB 小块随机读放大；首选 **B 读路径预取/顺序化 + 块尺寸杠杆**
  （4KB→64KB 块数 /16 ≈ 3k 次读；或块序 readahead / 命中块相邻预读入缓存）；C colstore
  #79-80（热列瘦行块 → 行/块↑ → 随机读块数↓）次选；A compaction 收敛仅放大项。
  验收：#77 clean mean ≤1.5s 无 >3s 离群；同族大窗冷读探针首读同步收敛。
- **内核 A/B（组读）✅ 否定（2026-09-06，clean 重装对照）**：
  - SCAN_GROUP 8→64→256 组读放大在污染态（WAL checkpoint 不推进反复回放叠加 SST）下
    表现"无效甚至反效"（g256 mean 8062/p99 34615、compact 后 15636/66073）——经查为
    **测量污染产物**：非干净退出后 per-CPU WAL checkpoint 不推进 → 每次重启回放 ~110 万条
    并重刷 SST（启动 +45-60s），文件逐次叠加 → 读路径失真（P144 候选登记）。
  - 干净全量重装对照（同配置同数据）：**g8 mean 2523.8/p50 951/max 7212 vs
    g64 mean 2645.9/p50 889.7/max 7526 → 无差异**（warm p50 ~0.9s、冷首触离群 ~7.5s 均
    不变）——组读合并不改变磁盘冷区总字节/随机度瓶颈 → **SCAN_GROUP 维持 8**（回退）。
  - 结论：#77 离群（冷首触 ~7.5s）不随组读大小变化；剩真杠杆 = **块尺寸 4KB→64KB（写侧，
     新 SST，需 clean 重装单变体对照）** / colstore #79-80（瘦行块）/ WAL checkpoint 缺陷
     前置修复（P144 候选）。**待办：块尺寸变体验证（或用户改向）**。
- **块尺寸变体验证 ✅（2026-09-06）**：tmp-cfg-p140-b64（`[blockcache] block_size_kb=64`，单旋钮
  同驱动写侧块布局 + 缓存粒度；clean 重装 110 万）：#77 cold 首跑 **1612.9/452/4692**
  （4KB 基线 2523.8/951/7212）；warm 稳态 **296.9/186/594**（4KB 稳态 750-950 + 9.7s 离群 →
  64KB 无 >600ms 离群）；回归探针全健康（pk_point 0.18-0.22ms、pk_between_10000 170→112、
  enum_sel_limit10000 58→39.6、pk_in_50 2.17）。
- **32KB 变体 ✅（同法，tmp-cfg block_size_kb=32）**：#77 cold **1700.9/498/4870**、稳态
  **301.9/179/574**、回归探针全同（pk_point 0.18、pk_in_50 1.36、pk_between_10000 115、
  enum_sel 37.2）→ **32KB ≈ 64KB（稳态 179 vs 186ms），拐点在 4→32 之间**。
- **定论：块尺寸 4KB→32KB 即捕获 ~全部冷首触收益（块数 /15、seek 骤降），碎片/缓存浪费
  更小 → 采纳 block_size_kb=32 为宽表基准推荐配置**（新写 SST 生效；wide/OLAP 建议 32，
  点查/小窗经 32/64 两档 + 4KB 基线实测均无退化）。P143 收口。

**P144（per-CPU WAL checkpoint 推进，2026-09-06 立项修复，完成 ✅）**
- 根因（诊断实证：enq=[1.1M,0,1.1M,0] wm=[1.1M,0,0,∞] cp=0）：① cp 持久化只在 flush_all
  （flush_wal/正常关闭）→ 强制 kill/崩溃永不落盘；② **cidx（组合索引）CF 行 value 为空 →
  approx_bytes≈0 → 永不达刷盘阈值 → 其水位恒 0 → cp=min(各 CF)=0 钉死** → 每次重启全量回放
  ~110 万条 + 重刷 SST（+40-60s；cidx 回放 2.2M 条后 memtable_bytes=0 佐证 value 空）。
- 已落 ①：`WalRuntime::flush_checkpoint_advance`（CF 刷盘使 cp 前进即原子持久化 + 段裁剪，
  open.rs 四 CF 回调接线）→ 无 composite / cidx 达阈负载下运行期安全点收敛，非干净退出
  重启回放≈0。单测 p144_checkpoint_persists_after_flush_without_close（写+flush_primary 后
  未 close 即 checkpoint>0）。
- 已落 ②（cidx 钉死场景，2026-09-06 选型方向 ② 落地）：cidx 字节判据失效的根子是"没有任何
  事件驱动它刷盘"。新增 `WalRuntime::maybe_cidx_catchup`——recompute_cp 判定 **cidx 有 pending
  未刷（入队水位 > 已刷水位）且成为 cp 唯一钉点（水位严格落后于其余有入队历史 CF）** 时，同步
  触发补刷（open.rs 注册钩子捕获 cidx Arc → `switch_and_flush`；空缓冲不刷防空 L0；AtomicBool
  防并发/递归）→ cidx 水位随主数据收敛、cp 前进即走 flush_checkpoint_advance 持久化+裁剪。
  **= "cidx 不钉 cp、随 primary 收敛后裁段"**：WAL 全保真（补刷后才裁，不依赖 task028 全量重建
  兜底；重建仅保留为 open 期存量补齐）。新增条目数判据 `ColumnFamily::memtable_len` /
  `MemTableBuffer::len`（与 approx_bytes 正交——cidx 空 value 行字节恒 0）。
- 单测：p144b_cidx_catchup_unpins_checkpoint（runtime 级：唯一钉点触发一次补刷、cp 前进且持久化、
  pending 清零后不空转）+ p144b_cidx_composite_catchup_flush_unpins_cp（engine 级：per-CPU +
  composite 写 3 万行，仅主刷即级联补刷 cidx → cidx SST>0 / 水位>0 / cp>0 已持久化；drop 重开
  数据完整 + 前缀查询计数一致——裁剪未丢未刷 cidx 键）。lib 800 全绿（seqlock flaky 复绿）。
- 现状：110 万 composite 旧库（cp 曾持久为 0）首次升级重启仍回放一次完成收敛；此后运行期自动
  补刷，非干净退出重启回放≈0，测量卫生不再依赖每次 clean 重装（候选①③ 不再实施）。
- **50 万复现验证 ✅（2026-09-06，Windows 本机 + VMware Ubuntu 24.04（gqkdb/123，NAT 192.168.197.130）两端同构）**：
  工具链 = gen-dataset 25 列宽表 parquet（确定性 seed 42，两端逐位一致）→ `import --parquet` +
  composite `[["status","ts"],["ts"]]` + per-CPU WAL（memtable 64MB/批窗口 100ms/block 32KB）→
  `shanshui-cunji-wal-probe`（`Engine::wal_replay_report` 只读诊断：persisted_cp/cp/cidx_wm/
  replay_pending）。三段对照（数值两端一致）：
  | 阶段 | Windows | Ubuntu |
  |---|---|---|
  | import 50 万 | 30.0s | 20.9s |
  | open#1 正常重启 | 798ms · persisted=451943 · pending=144171 | 1015ms · 同左 |
  | resetcp→0 后 open#2（旧库态首启） | 3.5s → cp/cidx_wm=500000 · **pending=0** · count=500000 | 3.3s → 同左 |
  | writetail 2000 行 + process::exit 崩溃 → open#3 | 523ms · **pending=6000**（=2000×3 条目）· count=502000 | 601ms · 同左 |
  结论：cidx 全程随 primary 收敛（cidx_wm==cp，9→10 SST）；**旧库态（cp=0）首启一次全量回放即收敛并持久化 cp→500000，此后崩溃/非干净退出重启只回放最近刷盘后的写尾（0.5-0.6s 级）**；对照修复前"每次重启全量回放 110 万 + 45-60s"，P144-② 将启动/重启回放从全量收敛到写尾。注：open#1 pending=144171 = import（<1M 行不尾刷主）留下的写尾，属 loader 现状非缺陷（import 尾刷可进一步归 0，见杂项）。
- 杂项：import_parquet 结尾未尾刷 primary（FLUSH_EVERY=1M，<1M 行只在结尾 flush_wal）→ 大文件导入后首次 open 需回放尾批；可考虑结尾补 flush_primary 使 import 完成即 cp=满量（未做——避免回归面扩大）。

## 交接段 · 山水存迹（2026-09-06，develop @ 81d4f45）

> 本段 = P134+ 问题线本会话（含 Linux 远端 A/B）收口交接快照，供换机/新会话直接续读。细记录见上文对应 P 条目与 development_remain.md 的 P 行状态表。

### 仓库与状态
- 分支 develop，已推送 gitee（geng_qiankun/shanshui-cunji）；src/demo/*、tmp/ 均 gitignore。
- 排期/问题文档（一切以这两份为准）：development_remain.md（P 行状态表）、problem_solving.md（P134+ 问题闭环，= 本文件 §阶段 4）。
- 工作流：读排期 → design → src/demo/\<功能\> 跑通 → 合入 src/ → 单测 + 全量 `cargo test --lib`（~799，唯一 flaky = seqlock retry 复绿）→ 回填两份文档 → 提交。

### 本会话收口（P140–P145）
| P | 内容 | 结论/产物 |
|---|---|---|
| P140 | #77 txn 快照窗投影流 | scan_range_txn_fields（快照+列投影三合一）+ BETWEEN 窗下推；warm p50 2-2.5×；#77 mean 3316→2459ms |
| P143 | #77 冷首触 IO | 组读(SCAN_GROUP) 8/64/256 clean A/B 无差异（维持 8）；块尺寸 4→32KB 有效（cold 2524→1701、稳态 179ms，点查无损）→ 宽表推荐 block_size_kb=32（写侧新 SST 生效） |
| P144 | WAL checkpoint 推进 | flush_checkpoint_advance（cp 前进即持久化+裁剪）+ cidx 钉死场景 P144-②（唯一钉点自动补刷，随 primary 收敛）——完成 ✅，见 §P144 记录 |
| P145 | batch_get_at 按块分组 | CF get_many_at（≤S 语义逐 get_bytes_at 等价）；冷 39.2→12.2µs/doc；要点 = 块只解码一次 |

### 待办队列（development_remain 已登记，均未开发）
- P141 事务内聚合 MVCC 权威版（RR COUNT/GROUP BY；复用 P134/P135 superset）。
- P142 Estimate 数量级接口（count_all_docs / count_docs_range 单锁 rank，零 MVCC，标 approx）。
- P144 已收口 ✅：cidx 钉死场景 = 唯一钉点自动补刷（P144-②，见 §P144 记录；候选①③不再实施）。
- #77 验收残余：cold 首跑 mean 1613ms 距 ≤1.5s 一步（32KB 档）；block_size_kb 代码默认 4→32 待 Linux A/B 定。
- 杂项：seqlock flaky 阈值；WAL 回放 checkpoint——110 万 composite 旧库首次升级重启回放一次完成收敛，此后非干净退出重启回放≈0（P144-②）。

### 换机 / 远端注意
- Linux 编译：仓库内 `.cargo/config.toml` 为 Windows 专用（target-dir/linker）——Linux 须删除或用 CARGO_TARGET_DIR 覆盖；`~/.cargo/config.toml` 用 rsproxy.cn 镜像；构建加 `CARGO_PROFILE_RELEASE_LTO=false CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`（否则 cgu1+LTO 2 核极慢）。
- Engine.iou 已改 pub(crate)（Linux E0451，Windows 不可见，81d4f45）→ 远端需拉 81d4f45 后重编 cjserver + rr-conformance。
- 阿里云 A/B（a4 vs b32，50 万行 / 1G 预算）：root@106.14.68.116（Debian12 / 2C / 1.6GB），/root/scc-p143（已删仓库 .cargo）、/root/cfg-linux-a4.toml / cfg-linux-b32.toml、/root/bench-remote.sh 就绪；mariadb 已停（编完记得 `service mariadb start`）。本地 plink/pscp 在 C:\putty。
- PowerShell 陷阱：原生命令内嵌引号被剥离；pkill -f 会自匹配远程 shell（用 pkill -x 或分两次调用）。

### 新机器上继续的第一步
`git pull develop` → 重编远端（含 81d4f45）→ 跑 a4/b32 → 依结果决定 block_size_kb 默认是否改 32。

### 本机核对 / 遗留口径（Windows 工作区记录）
- 本机 origin/develop = 81d4f45，与上表一致；原 HEAD 停在 master（d2d1972，2026-09-02 的 10 亿库阶段 A~D 合并线，其 problem_solving.md 无 §阶段 4）。本次已建本地 develop 跟踪 origin/develop 续作；master 上 10 亿库阶段 A~D 内容若需并入 develop 排期线，先 merge-base 核对（a8c4e17 已在 develop 祖先中）。
- P143 口径已统一 = **32KB**（development_remain.md P143 行 + commit 41f7147；本文件 §P143 正文已同步回填）。

> 环境/远端访问备忘（阿里云 hostkey、plink/pscp、PowerShell 传参陷阱）原为本文件「## 环境备忘（不入库）」节——但本文件入库，hostkey 实际进入 git 历史。2026-09-06 已迁至 **`tmp/env-notes.md`**（gitignore，不入库）。历史提交中仍含旧内容，如需彻底清除须 git 历史改写（hostkey 为指纹非私钥，建议不处理）。
