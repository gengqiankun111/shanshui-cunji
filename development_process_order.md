# 开发流程顺序（development_process_order）

> 本文件是**开发路线图的唯一入口**：每个开发会话/任务的第一步 = 读取本文件，
> 确定「下一步开发内容」与优先级，再进入项目标准工作流（development.md）。
> 大项完成后回填状态并重排优先级。

## 1. 流程约定

1. **第一步（固定）**：读取本文件 → 定位当前 `P0/P1` 大项 → 明确本次开发内容；
2. 大项粒度 = 可独立交付的功能/性能/质量项；子任务拆分见 development.md 里程碑；
3. **优先级评估准则**：`影响（业务/性能收益）` × `成本（工作量）` × `依赖（前置阻塞）`，
   得分为 P0（立即）/ P1（近期）/ P2（中期）/ P3（远期）；
4. 完成大项 → 回填「已完成」区 + 更新 development.md / feature.md / problem_solving.md。

## 2. 大项队列（按优先级）

| 编号 | 大项 | 优先级 | 状态 | 排期说明 |
|---|---|---|---|---|
| A | 本机 2000 万 / 5000 万性能吞吐基准（demo 12 项放大） | P0 | 🔄 执行中 | 本次会话；查询次数：主键/HotCache/分片/删除 100 万、倒排/组合/fulltext 1 万、SQL 1 千+BETWEEN 100 |
| B | 本机 1 亿数据性能与吞吐测试 | P0 | ⏳ 待排期 | **前置 = A 完成**；环境本机 NVMe + D 盘临时目录；交付 images/perf-0.6.0/1亿/ 报告+截图 |
| C | 分布式吞吐优化（机制已验证 → 性能） | P1 | ⏳ | Gateway 按分片并行（去全局 Mutex）→ RPC 连接复用 + 批量写入 → 节点开组提交；三项独立可随时做 |
| D | LSM 事务阶段一：WriteBatch 原子写 | P1 | ⏳ | 最低成本事务基座（BTreeMap 攒批 → WAL+MemTable 一次性提交，失败整体回滚）；约 2-4 周量级 |
| E | 事务阶段二：快照隔离（Snapshot/MVCC） | P2 | ⏳ | 全局单调 seq + 一致性快照读 + 历史版本 GC；需 D 先行；约 2-3 个月量级 |
| F | 事务阶段三：完整 ACID 与隔离级别 | P2 | ⏳ | 事务管理器 + 悲观/乐观锁 + 死锁检测 + RC/RR/SERIALIZABLE；需 E 先行；约 3-6 个月量级 |
| G | 倒排 posting 检索优化 | P2 | ⏳ | 本次基准暴露：RoaringBitmap 反序列化是倒排/fulltext/SQL 查询主成本（5000 万库单次 ~10ms）；候选：段内 term→bitmap 内存缓存 / 增量位图 |
| H | 读写分离落地（M8-P1 评估暂缓） | P3 | ⏸ | 组提交已解决读被写拖垮；读路径 &self 基础（c48a7c1）就绪，待复制型分布式阶段 |
| I | 高并发查询优化（design 9.5 目标） | P3 | ⏳ | M6 后留待 |

## 3. 大项详情

### A. 本机 2000 万 / 5000 万性能吞吐基准（P0，执行中）
- 内容：`shanshui-cunji demo --scale N --config config.bench.toml`，12 项测试（冒烟已 12/12 通过）：
  构造数据 → 批量插入 → 主键 100 万次 → HotCache 100 万次 → 组合索引 1 万次 → 倒排 1 万次 →
  fulltext 分词 1 万次（中文 bigram + 英文整词）→ 类 SQL 1 千次 + BETWEEN 100 次 → 分片 100 万次 →
  删除 100 万次 → 优化器自检 → 备份还原（倒排 posting 含 tombstone 语义已修正）；
- 配套：`config.bench.toml`（inverted_fields=status/city + fulltext_fields=title/content/remark）；
  引擎修复：fulltext 词 term 与倒排白名单正交（`inverted_allowed` 放行 `ft:`，含单测）；
  sqlish LIMIT 下推（post_filter 提前终止，BETWEEN 后过滤免全量遍历）；
- 交付：images/perf-0.6.0/{2000万,5000万}/ 报告 + console.log + 截图。

### B. 本机 1 亿数据性能与吞吐测试（P0，排期）
- 目标：全链路 1 亿条量级的写入/查询/索引/备份基准（补全三规模 2000 万/5000 万/1 亿）；
- 前置：A 完成并确认 12 项全绿；预计耗时约 30-60 分钟（1 亿插入 × 2 引擎 + 分片 + 查询循环）；
- 风险：fulltext/倒排查询次数需按 posting 成本复核（1 亿库单次 bitmap 反序列化 ~20ms+，1 万次 ≈ 200s+ 可接受）；
- 交付：images/perf-0.6.0/1亿/ + 汇总报告（与 2000 万/5000 万对比，验证线性扩展）。

### C. 分布式吞吐优化（P1）
- 背景：跨地域真机 10000 条写 1074s（9.3 w/s）——瓶颈 = 网关全局锁串行 + 同步 RPC 往返 + 节点无组提交；
  同机预期 1000-5000 条/s（机制正确性已验证，7.51）；
- 改造（三项独立）：① Gateway 按分片并行（去全局 Mutex，分片独立写线程）；② RPC 连接复用 + 批量写入（pipeline）；
  ③ 节点配置组提交（group_commit_us）；
- 验收：跨地域两节点 10000 条写入 < 60s（当前 1074s）；本机/局域网吞吐对照。

### D/E/F. LSM 事务三阶段（P1/P2）
- 阶段一 WriteBatch：事务写攒批 → WAL + MemTable 原子提交，失败回滚；为阶段二/三打基座；
- 阶段二 Snapshot/MVCC：全局单调 seq（可复用 ReplicationLog seq 体系）→ 一致性快照读 → 历史版本 GC；
- 阶段三 完整 ACID：事务管理器 + 锁（悲观/乐观）+ 死锁检测 + RC/RR/SERIALIZABLE；
- 对齐既有评估：单 docid 路由天然不分片（Ex-3 Calvin 结论），分布式事务 L1 Outbox/L2 SAGA 已覆盖写路径本地性。

### G. 倒排 posting 检索优化（P2）
- 现象：倒排/fulltext/SQL 查询主成本 = RoaringBitmap 反序列化（posting 与规模线性）；
- 候选：term→bitmap 常驻内存缓存（对齐 bitmap_fields 白名单思想）/ 段内增量位图（只读新段合并）；
- 收益预估：重复 term 查询降 10-50×。

## 4. 已完成大项（最近）

- 阿里云两节点分布式强一致测试 ✅（7.51，cluster_demo：2000 条隧道 + 10000 条直连均强一致通过）
- 服务器 YCSB 基准 ✅（7.51：rotational=1 高效云盘 load 约本机一半、读 p50 持平、长尾放大）
- 类 SQL 解析 / 写入 Enrich / 读写分离评估 ✅（441282d / 706c33b / fcc26a6）
- Ex-1~4 分布式事务与倒排策略 ✅（7348acd / 990bf6b / 653fdc8 / 04aae97）
- 本机 NVMe SSD 基准 ✅（7.50：YCSB 写重 90.9k、纯读 269.9k ops/s）

## 5. 环境备忘

- 本机 Rust 工具链：`D:\cargo-home\bin` + `D:\rustup-home`（RUSTUP_HOME/CARGO_HOME）；
- release 构建：`cargo build --release --target-dir D:\shanshui-cunji-target --bin shanshui-cunji`；
- 大数据量测试临时目录：`SHANSHUI_CUNJI_TMP=D:\shanshui-cunji-tmp`（C 盘空间不足）；
- 服务器（阿里云 106.14.68.116，凭据不入库）：/root/scc-new + vendored offline 构建，CARGO_BUILD_JOBS=1；
- 测试辅助脚本：`function_test/`（gitignore 排除：run_demo.ps1 / run_bigdata_bench.ps1 / screenshot_sections.py）。
