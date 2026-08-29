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
- **提交**：待提交

---

## 环境备忘（不入库）

- **服务器**：阿里云 Debian 12（106.14.68.116），2 核 / 1.6GB 内存；本机 Windows 通过 plink/pscp（`-hostkey SHA256:LiGhXXWmK3WXg+M6c9iNOs8GpGeKQFII5TmeqL8ZvUw`）非交互访问。
- **PowerShell 传参陷阱**：原生命令（cargo/curl/plink）的内嵌双引号会被 PS 5.1 剥离——JSON 参数用反斜杠转义 `{\"k\":1}`；远程 `$()`/`\n` 用 PowerShell 单引号字符串避免被展开。
