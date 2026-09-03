# 山水存迹数据库 v0.6.0 发布说明

> 发布日期：2026-08-29 · Git tag：`v0.6.0`
> 对比基线：v0.5.0 · 里程碑：M8 前沿路线（Group Commit / 倒排过滤 / fulltext / 中文分词 / 查询流式化）

## 一、本版本亮点

1. **Group Commit 组提交（design 4.1.3 / M8-P0）**：写路径零 fsync，后台提交线程按窗口
   （`[storage] group_commit_us`）统一落盘——A 写重 2ms 窗口 **91,296 ops/s（45×** 于关闭 2,003，
   P50 7.8µs），达无 fsync 上限的 80%；
2. **倒排索引体系（design 5.2.4 / M8-P4~P7）**：字段白名单/黑名单 + 长文本保护
   （max_term_len 防字典膨胀，压缩 45 万倍）+ fulltext 分词索引 + HotCache 内存修复（P41）
   ——50M 库倒排 2.2GB 可优化至 ~200MB（排除高基数字段，见 design_extension v0.2）；
3. **中文全文检索（M8-P9/P13）**：bigram 字符碎片索引 → **jieba 完整词典分词**
   （`[inverted] cjk_segmenter="jieba"`，语义词精确命中，`fulltext 数据库` 单 term 命中）；
4. **查询流式化 + 游标续扫（M8-P10/P11）**：k-way merge（BinaryHeap 最小堆 O(N log K)）
   + 分页 limit/offset/total + `scan_after` 游标——50M 库全库分页 WS 691MB（旧全量收集 OOM），
   翻页 164–682ms（旧 total 模式 70s）；
5. **批量导入 + WAL 体系加固（M8-P5/P6/P12）**：批量导入模式 50M 行 60,507 行/s；
   WAL 截断保持小文件；环形 WAL 头部 tail 合并 fsync（sync 单次原子提交，ring+gc 2.3×）。

## 二、功能与变更明细

| 类别 | 内容 | 提交 |
|---|---|---|
| Group Commit | 写路径零 fsync + 后台提交线程窗口统一落盘；`group_commit_us`/`group_commit_bytes`；提交器模式（Drop join + 最终落盘）；backup 前置 flush_wal | 648d9bd |
| 倒排过滤 | 字段白名单 `inverted_fields` / 黑名单 `exclude_fields` / `max_term_len` 长文本保护 | cde4f18 |
| WAL 截断 | 截断后头部持久化 next_seq（重开接续避免 seq 冲突），WAL 保持小文件 | a4d829a |
| 批量导入 | 批量导入模式（parquet/CSV/JSON/mysqldump），50M 行 60,507 行/s | bde422d |
| fulltext | `ft:{field}:{token}` 命名空间 + fulltext 分词索引 | 545682f |
| HotCache | P41 修复：LruCache 容量满内部淘汰不通知 stats/used_bytes → 字节预算统一管理 + 软水位渐进淘汰 + LFU 采样 O(64) | 5a937ea |
| 查询分页 | limit/offset/total（total=全量命中数，供客户端算总页数） | 45c3a54 |
| 中文 bigram | tokenize 字符类分段，2-4 字关键词 bigram AND 精确命中 | 72badfe |
| scan 流式化 | SstRangeIter/MemRangeIter k-way merge（BinaryHeap O(N log K)），内存 O(page) | 516643f |
| 游标续扫 | `scan_after` + 回调提前终止，全库遍历每页 O(limit) | 848599f |
| 环形 WAL tail | sync 单次原子提交（先写头尾再记录区，消除冗余第二次 fsync），崩溃安全不变 | 49d4f55 |
| jieba 分词 | `[inverted] cjk_segmenter="jieba"`（`cjk-jieba` feature 默认开，词典嵌入）；参数传分词器无全局状态 | c9e05bd |
| 发布清理 | WAL 显式 `truncate(false)` 表恢复语义、删死代码、记录 posting 压缩探索（维持 Roaring） | cd51adb |

## 三、性能快检（2026-08-29 实测，SSD 环境）

- **A 写重**：组提交 2ms **91,296 ops/s**（P50 7.8µs，45× 于关闭 2,003）；1ms 75,330 ops/s；
- **ring+gc**：M8-P12 后 2ms 68,756 ops/s（M8-P1 基线 30,270 → 2.3×）；
- **50M 批量导入**：60,507 行/s（827,950ms / 50,000,000 行，全功能含倒排）；
- **scan 流式化**：50M 全库分页 WS 691MB（旧全量收集 OOM）、小范围翻页 117ms；
- **游标翻页**：50M 库首页 682ms / 深页 164ms（旧 total 模式全库 70s）；
- **倒排 posting 压缩探索**（demo）：Roaring 密集 0.13B/docid 达 1bit 理论下限、AND 查询快
  20.1× → 维持 Roaring 不引入新编码。

## 四、质量数据

- **313 个单元测试全绿**（`cargo test`），较 v0.5.0（285）新增 28；
- 项目自身 **unsafe = 0**（`#![forbid(unsafe_code)]`）；
- 新增能力均有测试：组提交窗口/崩溃恢复、倒排过滤、WAL 截断恢复、批量导入、fulltext、
  分页/游标、scan 流式化、bigram/jieba 分词、HotCache 修复、环形 WAL 混沌测试。

## 五、构建与使用

```bash
cargo build --release                          # mimalloc（默认）

# 组提交（写重场景推荐，[storage]）
group_commit_us = 2000                         # 窗口 µs，0=关闭逐条 fsync 强安全

# 中文分词（[inverted]）
cjk_segmenter = "jieba"                        # bigram（默认）| jieba 语义词精确命中

# 倒排字段策略（design_extension v0.2 第 9 章）
inverted_fields = ["status", "city", ...]      # 枚举/低基数字段白名单（≤20）
exclude_fields  = ["note", "user_id", ...]     # 高基数唯一字段排除（防字典膨胀）
max_term_len    = 96                           # 长文本保护

# 批量导入
shanshui-cunji-cli import --parquet ds.parquet --db mydb
```
