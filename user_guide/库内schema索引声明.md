# 库内 Schema 索引声明（cj.schema.json）

> 适用：cjserver（MySQL 协议服务）。从 P131b（2026-09）起，**索引一律显式声明、随库落盘**。
> 本文是对 [README.md §5/§7](./README.md) 与 [索引优先级.md](./索引优先级.md) 的机制补充。

## 0. 一句话原则（无内置默认）

**没有声明 = 没有索引。**

- 引擎**不再**有"不声明就给全部字符串字段建倒排"的隐式默认；
- 每张表在数据目录内用 `cj.schema.json` 声明自己的倒排字段、位图字段、组合索引；
- cjserver 打开数据目录时**自动读取并装配**该声明，**不需要** `--config` 指配置文件；
- 未声明索引的字段，查询只能走主键 / 已声明组合索引 / 全表扫描。

> 引擎内部的 legacy"全字段倒排"仅存在于单元测试与旧装载路径（`inverted.declared_only=false`）；
> 服务入口（cjserver、库内 schema）恒为声明制（`declared_only=true`）。

## 1. 文件位置与格式

文件：**`<数据目录>/cj.schema.json`**（与 primary / cidx / delta / inverted 列族同目录）。

```json
{
  "tables": [
    {
      "name": "documents",
      "id_field": "id",
      "inverted_fields": ["status", "city", "region", "channel"],
      "bitmap_fields": ["status", "region"],
      "fulltext_fields": ["title"],
      "stats_fields": ["amount"],
      "composite_indexes": [["status", "ts"], ["ts"]]
    }
  ]
}
```

| 字段 | 含义 | 空/缺省 |
| --- | --- | --- |
| `name` | 表名（多表预留；当前引擎装配首张/`documents` 表） | 必填 |
| `id_field` | 主键列名（记录用途） | 可省 |
| `inverted_fields` | **倒排白名单**：只对声明字段建立倒排词条 | 空 = 该表无倒排 |
| `bitmap_fields` | 低基数枚举字段的**常驻内存位图**（COUNT/GROUP BY/AND 亚毫秒） | 空 = 关闭 |
| `fulltext_fields` | 分词字段（`ft:字段:token` 词 term，长文本关键词检索） | 空 = 关闭 |
| `stats_fields` | 随倒排词条累积 sum/min/max/avg 的数值字段（聚合免全扫） | 空 = 关闭 |
| `composite_indexes` | 组合索引声明（每个元素 = 一组字段，按声明顺序编码） | 空 = 无组合索引 |

## 2. 索引语义要点

### 2.1 倒排（inverted_fields）
- **只对字符串值建立词条**：数值 / 布尔不进倒排（数值范围语义交给组合索引或全扫）；
- 词条形如 `字段=值`，仅支持等值 / 枚举收敛（`f='active'`）、`f IN (...)`、枚举×枚举 AND 交集；
- `f BETWEEN / > / <` 字符串字段可走"倒排范围"（字典序），数值字段通常全扫/组合；
- 长 term（默认 >96 字节）自动跳过；`exclude` 可对白名单做补充排除。

### 2.2 位图（bitmap_fields）与倒排的关系
位图 = 把低基数枚举字段的 term→docid 位图**常驻内存**一份，写路径实时维护。
- 服务 `COUNT(f='x')`、`GROUP BY f`、枚举 AND 组合筛选的**亚毫秒快速路径**；
- 建议声明位图的字段**同时进入 `inverted_fields`**（词条是位图数据的持久来源之一）；
- 代价 = 常驻内存，别把高基数/长文本字段放进来。

### 2.3 组合索引（composite_indexes）与路由优先级
- 最左前缀匹配；匹配**前 2 个字段**时组合索引**优先**（铁律）；
- 只匹配第 1 个字段且带额外条件 → 回退由倒排做交集再过滤；
- 单列组合（如 `["ts"]`）用于二级索引范围扫描（`ts BETWEEN` 从全扫降到亚毫秒级）。
路由全貌见 [索引优先级.md](./索引优先级.md)。

## 3. 声明怎么落库

### 场景 A：新建库（导入时声明，推荐）
用 `--schema schema.json` 导入：导入完成会自动把索引声明写入数据目录
（成为自描述库，之后启动无需再给配置）：

```bash
# schema.json 里写 inverted_fields / composite_indexes（见 §1 结构）
shanshui-cunji-import --json ./documents.jsonl --schema ./schema.json \
  --data-dir /path/db-documents --config config.toml
# 完成后：/path/db-documents/cj.schema.json 已生成
```

### 场景 B：存量库补声明
1. 手工写好 `cj.schema.json` 放入数据目录（注意目录属主，写入用库属主/sudo）；
2. 启动 cjserver → open 期**自动补建**：
   - 组合索引：`ensure_composite_index_backfill` 回扫 primary 建 cidx（幂等，`cidx.sig`）；
   - 倒排：`ensure_inverted_backfill` **全量重建**词条（幂等，`inverted.sig`）。

启动日志示例：

```
[cjserver] 库内 schema 装配表 'documents': 倒排 4 字段, 组合 2 组, 位图 2 字段, fulltext 0 字段
[engine] 倒排声明变更（inv: → inv:channel,city,region,status），全量重建倒排词条（扫 primary）…
[engine] 倒排重建完成：396117 词条已落段
[cjserver] 数据目录 … 打开完成，启动 MySQL 协议服务: 127.0.0.1:3308
```

- 声明**没变**的重启：零开销跳过（日志无重建行）；
- 声明**变化**（加字段/去字段）：倒排整库重建（purge 旧段 → 按新白名单重建），cidx 增量补齐；
- 空声明重建会**清除**旧的全部倒排词条（= 声明"不要倒排"）。

## 4. 启动（cjserver）

```bash
# 最小启动：索引声明随库，无需 --config
cjserver --data-dir /path/db-documents --bind 127.0.0.1:3308

# 可选：--config 只承载运行参数（缓存/组提交/看门狗），不覆盖 schema 的索引声明
cjserver --data-dir /path/db-documents --bind 127.0.0.1:3308 --config run.toml
```

- 有 `cj.schema.json` → 打印「库内 schema 装配表 …」；
- 没有 schema 且未声明索引 → 打印「零索引模式」提示（**此时写入不建任何倒排**）；
- 启动参数 `--watchdog-secs 300` 可放宽大库全扫熔断。

## 5. 常见问题

| 现象 | 原因与处理 |
| --- | --- |
| 启动提示"零索引模式" | 数据目录没有 `cj.schema.json`。写一份声明后重启，open 会自动补建 |
| 想加倒排/组合索引却查不到新数据命中 | 检查字段是否在 `inverted_fields`（倒排只收字符串）；数值字段范围走组合/全扫 |
| COUNT/GROUP BY 还是慢（毫秒级） | 枚举字段加 `bitmap_fields`（并保留在 `inverted_fields`）后重启 |
| `cj.schema.json` 写入 Permission denied | 数据目录属主不是当前用户（常见 root 启动过）；用 sudo/属主写入 |
| 声明加了新字段后首次启动较慢 | open 期全量重建倒排（十万行秒级、百万行分钟级），一次性 |
| 既有倒排段被清空 | 声明变成空/零后重启会 purge——与"无声明=无倒排"语义一致，需显式声明 |

## 6. 相关文档
- [启动数据库.md](./启动数据库.md)：最小启动步骤
- [索引优先级.md](./索引优先级.md)：WHERE 路由优先级全图
- [config-example/](./config-example/)：运行参数模板（`[inverted]`/`[storage]` 等）
