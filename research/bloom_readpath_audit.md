# Bloom Filter 读路径核查结论（2026-09-05）

> 目的：核查「Bloom 缺失 / 生成未用 / 合并不重建」三类怀疑，并对照长尾点查（pk_point_star max=886ms）
> 与 L0 段数上升退化现象。核查对象：SST v5 分区布隆 + v3/v4 整文件布隆兼容（design 4.4.2）。

## 一、核查结论（三点全部为「已实现且已接线」）

| 核查点 | 结论 | 证据 |
|---|---|---|
| 1. 实现/数据结构 | ✅ 已实现（位数组 + 哈希，带 FPR） | src/bloom.rs（BloomFilter，with_estimated_keys(_fpr)/from_bytes/maybe_contains） |
| 2. flush 写 SST 构建 | ✅ memtable flush → L0 SST 写分区布隆 | src/storage/column_family/flush.rs:254（bloom_fpr）；SstWriter 每数据块产出一个布隆分区 |
| 3. compaction 输出重建 | ✅ 合并写新 SST 由同一 SstWriter 构建新布隆（按输出 key 集合，不复用旧段布隆） | src/storage/sstable/merge.rs（bloom_fpr 传入 86/450/649/671） |
| 4. 读路径调用 | ✅ 点查/快照点查/批量点查全部先布隆后读块 | column_family/read.rs get_from_sst:351/361、get_from_sst_at:398/406、get_many_from_sst:454/…（legacy + 目标块分区布隆） |
| 5. 加载 | ✅ v5 分区布隆在 SstReader 打开时整体读入（每块一个，与索引对齐）；v4 legacy 兼容 | sstable/reader.rs partition_blooms:309 / legacy_bloom:314；mod.rs v4_legacy_bloom_still_readable:655 |

- 格式版本：v5 = 分区布隆（每数据块一个，查询只校验目标块，只加载目标块布隆字节）——**不是**「缺失」也不是「整文件超大布隆」。
- 配置：`[sstable] bloom_fpr` 默认 0.01（config/model/sstable.rs:18/29）。
- 定位顺序（get_from_sst）：段级 min/max 粗筛 → (legacy) 整文件布隆 → 索引二分定位块 → **目标块分区布隆** → 命中才 touch + 块缓存/磁盘读块。min/max 与布隆分工正确，未混淆。

## 二、对用户三类「情况」的判定

- **情况 1（尚未实现）**：❌ 不成立——已实现且默认开启（fpr 0.01）。
- **情况 2（compaction 不重建）**：❌ 不成立——合并输出走同一 SstWriter 按输出键重建布隆（flush 与 merge 共用写入器，merge.rs 传 bloom_fpr）。
- **情况 3（生成未用）**：❌ 不成立——读路径（当前读 / 快照读 / 批量点查）都在读数据块前调用 legacy 或目标块分区布隆 `maybe_contains`。

## 三、长尾 886ms 与 L0 退化的可能解释（非布隆缺失）

1. 冷读/首触（mmap 页 + 元数据/布隆反序列化首包）——max 单点 886ms 更像一次磁盘 IO + 段元数据懒加载，而非系统性 miss 放大；
2. 快照读（get_at）与批量路径对每个候选 SST 走同一套段粗筛+布隆，L0 段数 N 时开销 O(N×布隆/索引定位)，段多后 p99 升——这是多段线性放大，属结构现象（P3-A 已做 L0 按表分组粗筛减段）；
3. 需要压测验证：构造 L0 4/8/12/16 段 + miss 点查（key-not-exist），看 p99 斜率与布隆跳过计数（后者需先补 metrics，见四）。

## 四、残余缺口（对应原行动清单，排期登记）

| # | 缺口 | 建议 |
|---|---|---|
| 1 | 无布隆过滤命中/跳过统计 | 读路径布隆跳过计数 → metrics（/metrics + SHOW MEMORY 扩展）；用 L0 4/8/12/16 + miss 点查验证收益 |
| 2 | 布隆/段元数据无内存统计 | 每 SST bloom 字节 + 段索引字节纳入 memory_report（现只有 cache/memtable 分项） |
| 3 | MVCC 长快照无主动管控 | 活跃快照数/最长存活时长 metric + 超时告警 + 最大存活强制回收策略（防 compaction 被无限阻塞，关联 txn_long_read 19.1× 长尾） |
| 4 | 快照读对多段线性代价 | 结合 P3-A L0 按表组粗筛与布隆前置已降低单段代价；L0 堆积治理走 Task-025/026 段数收敛 |
| 5 | 886ms 尖峰根因 | 冷热两轮点查 + 段元数据首触对照实验（可复用 rr --one 点查打点） |

> 判定修正依据原文：LSM 写/合并层完成度高属实；读路径布隆**非**短板（已全链实现），长尾更多指向冷 IO、
> L0 多段线性放大与缺少过滤计数。P127（组合 WHERE 定位）、快照管控、metrics 仍为高价值后续项。
