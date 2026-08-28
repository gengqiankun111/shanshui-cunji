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

---

## 环境备忘（不入库）

- **服务器**：阿里云 Debian 12（106.14.68.116），2 核 / 1.6GB 内存；本机 Windows 通过 plink/pscp（`-hostkey SHA256:LiGhXXWmK3WXg+M6c9iNOs8GpGeKQFII5TmeqL8ZvUw`）非交互访问。
- **PowerShell 传参陷阱**：原生命令（cargo/curl/plink）的内嵌双引号会被 PS 5.1 剥离——JSON 参数用反斜杠转义 `{\"k\":1}`；远程 `$()`/`\n` 用 PowerShell 单引号字符串避免被展开。
