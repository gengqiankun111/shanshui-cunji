# 山水存迹数据库 · 质量报告（2026-08-27）

> 本报告为质量证明体系首次完整基线（quality_system_process.md 第 10 节对应物）。
> 执行环境：Windows 开发机（静态检查）+ 阿里云 Debian 12（覆盖率，106.14.68.116，2 核 / 1.6GB）。

## 一、静态检查

| 检查项 | 结果 | 说明 |
| --- | --- | --- |
| cargo fmt --check | ✅ 通过 | 零 diff |
| cargo clippy -- -D warnings | ✅ 通过 | 52 处历史警告已清零（P18） |
| cargo audit | ✅ 0 漏洞 0 警告 | 99 个依赖；lru 0.12→0.18 消除 2 个 unsound（P19） |
| cargo deny check | ✅ 全 ok | advisories / bans / licenses / sources；白名单 MIT/Apache-2.0/Unicode-3.0/Zlib |
| unsafe 统计 | ✅ 0 处 | 全代码无 unsafe（`grep -c unsafe src` = 0） |

## 二、测试

- 单元测试：**133 passed / 0 failed**（`cargo test --lib`，0.48s@Windows；52.3s@服务器插桩构建）
- demo 冒烟：10/10（历史基线，报告在 images/）
- 混沌测试（MVP）：WAL 截断恢复 / SST 损坏注入 / 孤儿段忽略 / 跨重启持久化

## 三、代码覆盖率（cargo tarpaulin 0.37.2，阿里云 Debian）

| 口径 | 覆盖行 / 总行 | 覆盖率 | 说明 |
| --- | --- | --- | --- |
| 核心引擎（lib 主体） | 1742 / 1957 | **89.01%** | 达标（目标 ≥80%），排除 CLI/demo 壳 |
| 全部 src（含 CLI 壳） | 1742 / 2469 | 70.55% | main.rs / demo.rs 为 bin 壳，tarpaulin 只测 lib 不计入 |

**各文件明细（按覆盖率升序）**：

| 文件 | 覆盖行/总行 | 覆盖率 |
| --- | --- | --- |
| src/main.rs（CLI 壳） | 0/298 | 0% |
| src/demo.rs（demo 壳） | 0/214 | 0% |
| src/server.rs（HTTP） | 177/237 | 74.7% |
| src/wal.rs | 87/106 | 82.1% |
| src/config/model.rs | 56/68 | 82.4% |
| src/engine.rs | 113/134 | 84.3% |
| src/hotcache.rs | 53/61 | 86.9% |
| src/blockcache.rs | 14/16 | 87.5% |
| src/memtable.rs | 67/76 | 88.2% |
| src/value.rs | 62/69 | 89.9% |
| src/sstable.rs | 452/495 | 91.3% |
| src/keys.rs | 54/59 | 91.5% |
| src/schema/registry.rs | 62/67 | 92.5% |
| src/column_family.rs | 251/265 | 94.7% |
| src/watchdog.rs | 41/43 | 95.3% |
| src/storage.rs | 103/107 | 96.3% |
| src/inverted.rs | 90/93 | 96.8% |
| src/optimizer.rs | 10/10 | 100% |
| src/bloom.rs | 50/50 | 100% |

> 覆盖数据源：`quality/coverage/lcov-20260827.info`（tarpaulin `--out lcov`）。
> CLI 壳（main.rs/demo.rs）的 0% 是工具口径——tarpaulin 默认只统计 lib 目标；
> 其功能正确性由 demo 冒烟 10/10 与 CLI/HTTP 端到端测试保障。

## 四、unsafe 统计（cargo-geiger 0.13.0 正式报告）

| 口径 | 函数 | 表达式 | Impl | Trait | 方法 |
| --- | --- | --- | --- | --- | --- |
| shanshui-cunji 自身 | 0/958 | 0/64881 | 0/771 | 0/66 | 0/3302 |
| 依赖合计（crc32fast/crossbeam/lru/roaring/zstd 等） | 105/1093 | 14073/89644 | 360/1159 | 29/95 | 474/3894 |

- **项目自身 unsafe = 0**；源码已加 `#![forbid(unsafe_code)]`，任何未来 unsafe 在编译期即被拒绝
- 依赖的 unsafe 属生态库内部实现（roaring 位图、crossbeam 无锁结构、zstd 压缩、hashbrown 等），不进入本项目代码
- 完整逐依赖明细：`quality/geiger_report.txt`

## 五、已知问题 / 挂起

- ~~`cargo geiger` 报告~~：已完成，项目自身 unsafe = 0（2026-08-27）
- 覆盖率 HTML 报告（含依赖的全量版）体量过大（88MB），未入库；以 lcov 机器可读版入库
- CLI 壳覆盖率缺口：后续可抽 `build_html_report` 等纯逻辑到 lib 补测

## 六、下一里程碑目标

- 覆盖率 HTML 展示版（tarpaulin `--out Html --include-files` 修复 glob 后产出）
- Gitee CodeCheck 开通并亮牌
- cargo-auditable 内嵌依赖树（musl 静态产物）
