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

---

## 环境备忘（不入库）

- **服务器**：阿里云 Debian 12（106.14.68.116），2 核 / 1.6GB 内存；本机 Windows 通过 plink/pscp（`-hostkey SHA256:LiGhXXWmK3WXg+M6c9iNOs8GpGeKQFII5TmeqL8ZvUw`）非交互访问。
- **PowerShell 传参陷阱**：原生命令（cargo/curl/plink）的内嵌双引号会被 PS 5.1 剥离——JSON 参数用反斜杠转义 `{\"k\":1}`；远程 `$()`/`\n` 用 PowerShell 单引号字符串避免被展开。
