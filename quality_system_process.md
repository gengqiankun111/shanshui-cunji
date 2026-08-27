# 山水存迹数据库 · 质量证明执行流程（quality_system_process.md）

> 配套文档：[quality_system.md](quality_system.md)（对外体系说明）、[problem_solving.md](problem_solving.md)（问题闭环）
> 本文件是**执行手册**：怎么跑、跑什么、结果放哪、失败怎么处理。

---

## 0. 一键命令总览

| 目标 | 命令 | 说明 |
| --- | --- | --- |
| 静态检查 | `make check` / `quality/check.ps1` | fmt + clippy + 构建 + 测试 |
| 全量测试 | `make test-all` | cargo test（含 demo 冒烟） |
| 基准测试 | `make benchmark` | release 构建 + demo 压测报告 |
| 质量报告 | `quality/report.ps1` | 汇总生成 `quality/quality_report_*.md` |
| 依赖审计 | `make audit` | cargo audit + cargo deny |

> Windows 开发机直接执行 `quality/check.ps1`；Linux（CI/服务器）执行 `quality/check.sh`。
> 命令均为幂等、可重复，保证**可复现**。

---

## 1. 工具链安装

```bash
# Rust 工具链（含 fmt / clippy）
rustup component add rustfmt clippy

# 依赖审计与合规（CI / release 节点）
cargo install cargo-audit cargo-deny
cargo install cargo-geiger        # unsafe 统计（里程碑）
cargo install cargo-tarpaulin     # 覆盖率（Linux，Windows 不支持）
```

> 本机 Windows 注意：`CARGO_INSTALL_ROOT` 可重定向到 D 盘（避免 C 盘空间不足）；
> `tarpaulin` 需在 Linux 环境运行（阿里云 Debian 服务器或 CI）。

### 1.1 工具矩阵（免费/低费用 + 对外展示）

| 工具 | 用途 | 触发时机 | 项目落地状态 |
| --- | --- | --- | --- |
| Gitee CodeCheck（华为云） | 圈复杂度 / 重复率 / 有效代码行 | 开源仓库免费开通 | ⏳ 待 Gitee 仓库启用（README 亮截图） |
| cargo fmt / clippy | 格式 + 650+ lint | 每次 commit | ✅ 已纳入 check.ps1（六步链第 1-2 步） |
| cargo audit + auditable | CVE 扫描 + 二进制内嵌依赖树 | 每次 CI | ✅ audit 已执行（0 漏洞）；auditable 待 Linux |
| cargo deny | 许可证合规 + 重复依赖 | 每次 release | ✅ 已执行（advisories/bans/licenses/sources ok） |
| cargo geiger | unsafe 占比（目标 <5%） | 里程碑 | ✅ 当前 unsafe=0（grep 确认）；geiger 报告待 Linux |
| cargo tarpaulin | 覆盖率 ≥80% | 每次 CI | ⏳ Linux 执行 |
| garbage-code-hunter | 0-100 质量评分（gch check） | 里程碑（可选） | ⏳ 可选装 |
| RAPx | UAF/内存泄漏检测 + 形式化验证 | 安全攸关模块（可选） | ⏳ 可选装 |
| cov-rs / grcov | LLVM 覆盖率（PR 评论） | 与 tarpaulin 二选一 | ⏳ 可选 |

**徽章（README 亮牌）**：

```text
[![fmt](https://img.shields.io/badge/rustfmt-ok-brightgreen)]()
[![clippy](https://img.shields.io/badge/clippy-0_warnings-brightgreen)]()
[![coverage](https://img.shields.io/badge/coverage-%E2%89%A580%25-brightgreen)]()
[![unsafe](https://img.shields.io/badge/unsafe-0%25-brightgreen)]()
[![audit](https://img.shields.io/badge/audit-0_vulns-brightgreen)]()
[![deny](https://img.shields.io/badge/licenses-ok-brightgreen)]()
```

---

## 2. 静态分析（每次提交必跑）

```bash
cargo fmt --all -- --check        # 格式零容忍
cargo clippy -- -D warnings       # 警告即错误
cargo build                       # 编译（含类型/借用检查）
cargo test --lib                  # 单元测试
```

**失败处理**：
- `fmt` 失败 → `cargo fmt --all` 自动格式化后复查 diff；
- `clippy` 报错 → 逐条修复（禁止 `#[allow]` 掩盖），修复记录追加到 problem_solving.md；
- 测试失败 → 补修代码 + 回归，不提交半成品。

**里程碑追加**：

```bash
cargo audit                       # 依赖漏洞
cargo deny check                  # 许可证/重复依赖
cargo geiger --threshold 5        # unsafe < 5%
cargo tarpaulin --out Html --output-dir quality/coverage   # 覆盖率 ≥ 80%
```

---

## 3. 架构评审（每里程碑一次）

1. 收集产物：`design.md` + `development.md` + 核心模块（sstable / wal / engine / inverted / optimizer）；
2. 用第 2 层 Prompt 模板发起 AI 评审；
3. 输出 `quality/architecture-review-{Mx}.md`：风险清单（高/中/低）+ 整改项；
4. 整改闭环：高风险项必须修复并在下个里程碑复评；无法短期修复的写入 problem_solving.md 挂起区。

---

## 4. 代码审查（每次 PR）

- **AI PR Review**：提交前对 diff 做一次 AI 审查，输出风险点；
- **自检清单**（quality_system.md 第 3 层）逐项核对；
- **新增 unsafe**：必须附带 safety 注释，否则拒绝合并；`cargo geiger` 追踪占比变化。

---

## 5. 单元 / 集成测试

```bash
cargo test            # 全量（当前 133 个）
cargo test --lib server::tests::http_end_to_end_crud_and_search   # HTTP 端到端
cargo run -- demo --scale 100000 --out images/<fn>                # 冒烟（10 项基准）
```

**覆盖目标**：关键路径（LSM CRUD / WAL 崩溃恢复 / Tombstone / PAX 拆合 / TTL 过期 / Delta 合并 / HTTP）必须有测试；
新功能先写测试再写实现（TDD）；覆盖率报告存档 `quality/coverage/`。

---

## 6. 基准测试（每里程碑）

```bash
cargo build --release
./target/release/shanshui-cunji demo --scale 100000 --out quality/bench/10w
# 规模递进：100w / 1000w / 5000w（脚本 images/run_test.ps1 可复用）
```

**记录**：TPS/QPS、P50/P90/P99、内存峰值 → `quality/bench/bench_report_{scale}.md`；
与上里程碑对比，退化 >20% 需定位（回滚或优化）。

---

## 7. 混沌测试（每里程碑 / 版本发布前）

MVP 已完成（步骤 16，`column_family` / `engine` / `inverted` 测试）：
- WAL 尾部截断 → 回放恢复完整记录、不 panic；
- SST 头部损坏 → open 报 Corrupted（不崩溃）；
- 孤儿倒排段（Manifest 外）→ 重启忽略、不污染查询；
- Engine 跨重启（进程退出重开）→ 主数据 + 倒排完整保留。

阶段 2 追加：kill -9 / 主从切换 / 磁盘写满 / 网络分区（在分布式环境执行）。

---

## 8. 质量报告生成

`quality/report.ps1`（或手写）：

```markdown
# quality_report_{YYYYMMDD}.md
- 静态检查：fmt/clippy/audit/deny 结果
- 测试：通过数 / 失败数 / 覆盖率
- 基准：TPS/QPS/延迟 + 对比基线
- unsafe 统计：块数 / 占比 / 审查状态
- 已知问题：挂起清单
```

存档位置：`quality/`（构建产物与临时文件不入库，仅报告入库）。

---

## 9. CI 集成（Gitee Go / GitHub Actions 参考）

```yaml
steps:
  - name: Static
    run: cargo fmt --all -- --check && cargo clippy -- -D warnings
  - name: Test
    run: cargo test
  - name: Audit
    run: cargo audit && cargo deny check
  - name: Coverage
    run: cargo tarpaulin --out Html --output-dir quality/coverage
```

---

## 10. 当前执行状态（初始基线）

> 首次执行：2026-08-27，本机 Windows（x86_64-pc-windows-gnu 工具链）
> 执行方式：`powershell -ExecutionPolicy Bypass -File quality/check.ps1`（一键跑通）

| 检查项 | 结果 |
| --- | --- |
| cargo fmt --check | ✅ 通过（本轮执行，exit 0） |
| cargo clippy -- -D warnings | ✅ 通过（本轮执行，零警告） |
| cargo build | ✅ 通过（本轮执行） |
| cargo test --lib | ✅ **133 passed; 0 failed**（本轮执行，0.48s） |
| 单元测试总数 | 133 |
| demo 冒烟 | 10/10（历史执行，报告在 images/） |
| unsafe 统计 | ✅ 全代码 0 处 unsafe（`grep -c unsafe src` = 0） |
| cargo audit | ✅ **99 个依赖，0 漏洞、0 警告**（2026-08-27 执行） |
| cargo deny check | ✅ **advisories / bans / licenses / sources 全 ok**（2026-08-27 执行） |
| cargo geiger | ✅ **项目自身 unsafe = 0**（0/958 函数，0/64881 表达式；报告 quality/geiger_report.txt） |
| cargo tarpaulin | ✅ **核心引擎 89.01%**（2026-08-27 阿里云 Debian 执行，见 quality_report_20260827.md） |
| 覆盖率 | ✅ 核心 lib 89.01%（含 CLI 壳 70.55%）；报告 lcov 存档 quality/coverage/ |

**本轮修复闭环**：clippy 52 处警告清零（P18）、check.ps1 PS 5.1 stderr 误判修复（P17）、
lru 0.12→0.18 升级消除 2 个 unsound 警告（P19）、deny.toml schema 适配 0.20（P20）——详见 [problem_solving.md](problem_solving.md)。
