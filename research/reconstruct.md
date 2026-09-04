db_adapter.rs
engine.rs
column_family.rs
sqlish.rs
好的，这4个文件构成了一个自下而上的数据库核心引擎。基于你提供的代码，我为你梳理出清晰的设计思路，即每个文件需要新建的“类”（在Rust中为struct）及其职责划分。

> **实施状态（2026-09-05）**：本目录规划已按四层落地到 `src/`——`column_family.rs`→`src/storage/`
> （column_family/{…}、sstable/{reader,iter,writer,block,compaction,merge}、wal/{writer,reader,ring}、
> manifest），`engine.rs`→`src/engine/`（engine/txn/mvcc…），`sqlish.rs`→`src/sql/`
> （parser/{ast,lexer,parser}、executor/{select,join,aggregate,group_by,eval}），
> `db_adapter.rs`→`src/server/`（server/session/client/sqlparse、protocol/…、command/…）。
> 提交 develop `76f5709`；全量 693 测试全绿，`crate::*` 旧路径经 lib.rs re-export/别名兼容。


整体目录结构规划
text
src/
├── storage/                    # 存储引擎层（原 column_family.rs）
│   ├── mod.rs
│   ├── column_family.rs        # 主结构体，~400行
│   ├── memtable.rs             # 内存表实现，~300行
│   ├── sstable/
│   │   ├── mod.rs
│   │   ├── reader.rs           # SSTable读取，~400行
│   │   ├── writer.rs           # SSTable写入，~300行
│   │   ├── block.rs            # 数据块编解码，~350行
│   │   ├── index.rs            # 索引和布隆过滤器，~250行
│   │   └── compaction.rs       # Compaction选段和合并，~500行
│   ├── wal/
│   │   ├── mod.rs
│   │   ├── writer.rs           # WAL写入，~200行
│   │   └── reader.rs           # WAL恢复读取，~200行
│   └── manifest.rs             # Manifest管理，~150行
│
├── engine/                     # 引擎外观层（原 engine.rs）
│   ├── mod.rs
│   ├── engine.rs               # 主Engine结构体，~500行
│   ├── txn.rs                  # 事务管理，~400行
│   ├── mvcc.rs                 # MVCC快照读写，~300行
│   ├── index/
│   │   ├── mod.rs
│   │   ├── inverted.rs         # 倒排索引，~400行
│   │   ├── composite.rs        # 组合索引，~200行
│   │   └── bitmap.rs           # 位图索引，~200行
│   ├── cache/
│   │   ├── mod.rs
│   │   ├── hotcache.rs         # 热数据缓存，~200行
│   │   └── blockcache.rs       # 块缓存，~200行
│   └── outbox.rs               # 本地消息表，~200行
│
├── sql/                        # SQL引擎层（原 sqlish.rs）
│   ├── mod.rs
│   ├── parser/
│   │   ├── mod.rs
│   │   ├── lexer.rs            # 词法分析，~200行
│   │   ├── parser.rs           # 语法分析，~400行
│   │   └── ast.rs              # AST定义，~150行
│   ├── executor/
│   │   ├── mod.rs
│   │   ├── select.rs           # SELECT执行，~400行
│   │   ├── aggregate.rs        # 聚合执行，~350行
│   │   ├── group_by.rs         # GROUP BY执行，~300行
│   │   └── join.rs             # JOIN执行，~250行
│   └── optimizer/
│       ├── mod.rs
│       ├── rule.rs             # 优化规则，~200行
│       └── cost.rs             # 代价估算，~150行
│
├── server/                     # 网络服务层（原 db_adapter.rs）
│   ├── mod.rs
│   ├── server.rs               # DbServer主结构体，~400行
│   ├── session.rs              # Session管理，~200行
│   ├── protocol/
│   │   ├── mod.rs
│   │   ├── handshake.rs        # 握手认证，~250行
│   │   ├── packet.rs           # 包编解码，~300行
│   │   └── response.rs         # 响应构造，~250行
│   ├── command/
│   │   ├── mod.rs
│   │   ├── query.rs            # COM_QUERY处理，~400行
│   │   ├── stmt.rs             # 预处理语句，~300行
│   │   └── transaction.rs      # 事务命令，~200行
│   └── client.rs               # MysqlWireClient，~200行
│
├── common/                     # 公共组件
│   ├── mod.rs
│   ├── error.rs                # 错误定义
│   ├── config.rs               # 配置定义
│   ├── keys.rs                 # 键编解码
│   └── watchdog.rs             # 看门狗
│
└── lib.rs                      # 模块导出
详细拆分说明
1. column_family.rs 的拆分 (原 ~3000行)
拆分为：

新文件	行数	核心职责
storage/column_family.rs	~400	ColumnFamily主结构体，对外API (put, get, delete, scan)
storage/memtable.rs	~300	MemTable和MemTableBuffer，双缓冲实现
storage/sstable/reader.rs	~400	SstReader，SSTable文件读取、块缓存交互
storage/sstable/writer.rs	~300	SstWriter，SSTable文件写入、PAX编码
storage/sstable/block.rs	~350	数据块编解码、扫描、字段提取
storage/sstable/index.rs	~250	块索引、布隆过滤器、Zone Map
storage/sstable/compaction.rs	~500	select_compaction_inputs、compact_merge、块级复用
storage/wal/writer.rs	~200	WAL追加写入、环形缓冲
storage/wal/reader.rs	~200	WAL恢复回放、截断
storage/manifest.rs	~150	Manifest序列化/反序列化
依赖关系：

text
column_family.rs
    ├── memtable.rs
    ├── sstable/reader.rs
    ├── sstable/writer.rs
    ├── sstable/compaction.rs (调用 reader/writer)
    ├── wal/writer.rs
    ├── wal/reader.rs
    └── manifest.rs
2. engine.rs 的拆分 (原 ~3000行)
拆分为：

新文件	行数	核心职责
engine/engine.rs	~500	Engine主结构体，整合各组件，对外API
engine/txn.rs	~400	事务管理 (txn_begin, txn_commit, txn_rollback)、锁表
engine/mvcc.rs	~300	快照读写 (get_at, scan_range_at)、快照注册
engine/index/inverted.rs	~400	InvertedIndex，倒排索引查询、统计、GC
engine/index/composite.rs	~200	组合索引前缀查询
engine/index/bitmap.rs	~200	位图索引快速路径
engine/cache/hotcache.rs	~200	HotCache，热数据缓存
engine/cache/blockcache.rs	~200	BlockCache，共享块缓存
engine/outbox.rs	~200	Outbox，本地消息表
依赖关系：

text
engine/engine.rs
    ├── storage/column_family.rs (主数据、Delta)
    ├── engine/txn.rs (事务协调)
    ├── engine/mvcc.rs (快照)
    ├── engine/index/inverted.rs
    ├── engine/index/composite.rs
    ├── engine/cache/hotcache.rs
    └── engine/outbox.rs
3. sqlish.rs 的拆分 (原 ~2000行)
拆分为：

新文件	行数	核心职责
sql/parser/ast.rs	~150	AST定义 (Select, WhereExpr, JoinClause)
sql/parser/lexer.rs	~200	词法分析 (Lexer, Tok)
sql/parser/parser.rs	~400	递归下降解析 (parse_select, parse_expr)
sql/executor/select.rs	~400	execute，通用SELECT执行
sql/executor/aggregate.rs	~350	execute_aggregate，标量聚合
sql/executor/group_by.rs	~300	execute_group_by，分组聚合
sql/executor/join.rs	~250	execute_join，JOIN执行
sql/optimizer/rule.rs	~200	优化规则（谓词下推、索引选择）
sql/optimizer/cost.rs	~150	代价估算
依赖关系：

text
sql/executor/select.rs
    ├── sql/parser/ast.rs
    ├── sql/parser/parser.rs
    ├── sql/optimizer/rule.rs
    ├── sql/optimizer/cost.rs
    └── engine/engine.rs (执行时调用)
4. db_adapter.rs 的拆分 (原 ~2000行)
拆分为：

新文件	行数	核心职责
server/server.rs	~400	DbServer，服务生命周期、后台worker
server/session.rs	~200	Session，连接状态管理
server/protocol/handshake.rs	~250	握手、认证 (build_handshake_packet, check_native_password)
server/protocol/packet.rs	~300	包编解码 (read_packet, write_packet, read_command)
server/protocol/response.rs	~250	响应构造 (ok_payload, err_payload, query_response_packets)
server/command/query.rs	~400	COM_QUERY处理，SQL分发
server/command/stmt.rs	~300	预处理语句 (COM_STMT_PREPARE, COM_STMT_EXECUTE)
server/command/transaction.rs	~200	事务命令 (BEGIN, COMMIT, ROLLBACK)
server/client.rs	~200	MysqlWireClient，测试客户端
依赖关系：

text
server/server.rs
    ├── server/session.rs
    ├── server/protocol/handshake.rs
    ├── server/protocol/packet.rs
    ├── server/command/query.rs
    ├── server/command/stmt.rs
    ├── server/command/transaction.rs
    └── engine/engine.rs
关键设计原则
1. 接口边界清晰
每个模块只暴露必要的公共API，内部实现细节隐藏。

rust
// storage/column_family.rs
pub struct ColumnFamily { /* 私有字段 */ }
impl ColumnFamily {
    pub fn open(...) -> Result<Self> { ... }
    pub fn put(&self, ...) -> Result<()> { ... }
    pub fn get(&self, ...) -> Result<Option<...>> { ... }
    // 内部方法不对外暴露
    fn compact_inner(&self) -> Result<...> { ... }
}
2. 依赖方向自上而下
text
server/  →  engine/  →  storage/
   ↓           ↓
 sql/    →  engine/
3. 每个文件一个明确的主题
sstable/compaction.rs 只负责Compaction选段和合并逻辑

engine/txn.rs 只负责事务状态机和锁管理

sql/executor/aggregate.rs 只负责聚合执行

4. 使用mod.rs统一导出
rust
// storage/mod.rs
pub mod column_family;
pub mod memtable;
pub mod sstable;
pub mod wal;
pub mod manifest;

// storage/sstable/mod.rs
pub mod reader;
pub mod writer;
pub mod block;
pub mod index;
pub mod compaction;
拆分后的收益
维度	拆分前	拆分后
单文件最大行数	~3000行	~500行
编译并行度	低（大文件串行编译）	高（小文件并行编译）
代码导航	困难（滚动查找）	容易（按主题跳转）
测试隔离	困难（需构造全Engine）	容易（可单独测试sstable/compaction.rs）
团队协作	冲突频繁	冲突减少
新人理解	需要通读整个文件	按模块逐步学习
建议的实施顺序
先拆分column_family.rs：它是最底层的，依赖最少，拆分风险最低

再拆分engine.rs：依赖storage模块，但内部逻辑清晰

然后拆分sqlish.rs：解析和执行逻辑相对独立

最后拆分db_adapter.rs：它是顶层，依赖前面所有模块