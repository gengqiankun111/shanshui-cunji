这是一个非常专业且犀利的问题。在数据库内核开发中，“有难度的点” 往往不是“写不出来”，而是“写出来保证正确且性能不倒退”。

结合你的代码现状（引擎强、SQL弱）和 LSM-Tree 的固有缺陷，我梳理了 4 个极难实现、且一旦实现不好会导致性能雪崩的点。这些点不在你的排期里，但它们才是决定项目生死的关键。

一、 最大的隐藏炸弹：Compaction 的“写入放大”与“读放大”死锁
这是 LSM-Tree 最经典、最无解的难题。你的设计中 Ex-8.11 调了 l1_trigger=8，但这只是冰山一角。

1. 难在哪里？
写放大：3000 万行数据（30GB），LSM 每层 compaction 要重写数据 5~10 遍。如果 compaction 速度跟不上写入速度，L0 文件数会爆炸，点查要扫描几十个文件，读延迟从 0.2ms 飙升到 200ms。

读放大：为了减少写放大，如果调低 compaction 频率，读放大就会增加。反之亦然。

实现不好会怎样：

写 Stall：L0 文件数达到阈值（例如 12），写入被阻塞，TPS 瞬间掉到 0。

空间放大：Compaction 不及时，旧版本数据残留，磁盘空间被撑爆。

你的现状：你只做了 l1_trigger=8 的静态调优，但 3000 万行下负载是动态的（白天写多，晚上读多）。静态阈值必然导致某一方崩塌。

2. 为什么“实现不了”？
动态自适应 Compaction（根据实时写入速率调整 compaction 速度）是 RocksDB 最复杂的模块之一。需要：

实时监控 L0 文件数 / 写入速率 / 磁盘 IO 利用率。

动态调整 compaction 线程数、每次 compaction 的数据量。

而且不能引入死锁（compaction 线程和写入线程抢锁）。

现状：你只有空闲感知调度（Ex-8.9），但 没有“写入速率自适应”。写入爆发时，compaction 依然按部就班，最终 L0 爆炸。

二、 并发控制的“幽灵”：MVCC + 删除位图 + 当前读的一致性
你已经实现了 RR 隔离级别（C1~C9 全绿），但那是 100 万行、并发低 的情况。3000 万行、高并发下，MVCC 的复杂度会指数级上升。

1. 难在哪里？
删除位图（DeletionBitmap）与 MVCC 快照的交互：

你现在的删除位图是 全局的（记录哪些 docid 被删了）。但在 RR 隔离级别下，事务 A 在 t1 时刻开始，事务 B 在 t2 时刻删了一行，事务 A 在 t3 时刻读该行，应该读到旧数据。

如果你的删除位图不携带“版本号/事务序列号”，快照读就会误以为该行已删除，导致 “幻读”或“不可重复读”。

实现不好会怎样：

为了支持 MVCC，删除位图必须变成 Map<docid, (seq, tombstone)>。在 3000 万行下，这意味着删除位图的内存从 2KB（稀疏）变成 3000 万 × 16 字节 = 480MB，且每次读都要查这个 Map。

你的当前实现：src/bitmap.rs 是稠密 Vec<u8>，完全不支持版本化删除。只要多事务并发，RR 必然违反。

2. 为什么“可能实现不了”？
如果改为版本化删除位图，那么 is_deleted(docid, seq) 需要 O(1) 且高并发。你能想到的方案只有：

方案 A：每个 docid 维护一个 BTreeMap<seq, bool> —— 内存爆炸（3000 万行 × 跳跃表开销）。

方案 B：全局删除位图 + 事务快照里记录“删除日志” —— 事务提交时要合并，复杂度 O(事务修改行数)，高并发下冲突严重。

现状：你的 development.md §20 提到“多表单删须关 deletion_bitmap_enabled（传统 Tombstone 路径）”，这实际上是在 “正确性”和“性能”之间选择了性能。但这会让 RR 隔离级别形同虚设。

三、 倒排索引的内存控制：FST 字典 + RoaringBitmap 的膨胀
倒排是你的核心优势（COUNT <0.1ms），但在 3000 万行下，内存控制会崩塌。

1. 难在哪里？
Term 数量爆炸：3000 万行，如果 note、title、desc 等字段开了全文分词（jieba），Term 数量可能达到 数亿。FST 虽然压缩率高，但构建 FST 需要 全部 Term 在内存中。构建期间内存可能超过 10GB，导致 OOM。

RoaringBitmap 的切换阈值：RoaringBitmap 在稀疏集（<4096 个 docid）用数组存储，密集集用位图。但当 3000 万行中某个高频 Term（例如 status='active' 占 50%）时，位图占用 3000 万 bit = 3.75MB，还算可控。但如果有很多高频 Term（例如 100 个枚举值各占 1%），位图总内存 = 100 × 3.75MB = 375MB，加上 FST，轻轻松松超 2GB 内存预算。

2. 为什么“可能实现不了”？
FST 构建需要全量数据：你不能“增量构建 FST”，只能全量重建。3000 万行重建一次 FST 可能需要 数分钟，期间查询只能走旧 FST（读放大）。

你的现状：你实现了 base.fst + delta.fst 分层（7.34），但 delta.fst 没有大小限制。如果高频写入导致 delta.fst 持续膨胀，查询时要合并 base + delta，性能会急剧下降。

行业现实：Elasticsearch 的倒排索引构建是 segment 化 的，每个 segment 有独立的 FST，查询时合并多个 segment 的结果。你的设计是 单 FST + delta，在大数据量下是 不可扩展的。

四、 SQL 层优化器：成本估算与执行计划选择的“不可能三角”
你现在是“直来直去”的执行（倒排候选 → 回表取行），但 3000 万行下，选择错误的执行计划会让查询慢 1000 倍。

1. 难在哪里？
多条件组合：WHERE status='active' AND amount > 5000 AND ts BETWEEN ...

方案 A：先用 status 倒排取 1500 万候选，再逐行过滤 amount/ts → 1500 万次回表，极慢。

方案 B：先扫 amount（无索引）找 500 万行，再过滤 status → 全表扫 3000 万行，也很慢。

最优方案：如果 status 选择性差（50%），而 amount 范围窄（1%），应该先扫 amount（虽然无索引，但 zone map 能跳过大量块）。

你缺少什么：你需要一个 成本估算器（Cost Estimator），能估算“倒排查 1500 万 docid 的开销” vs “全表扫 3000 万行但 zone map 剪枝到 100 万块”的开销。

2. 为什么“可能实现不了”？
统计信息缺失：你没有 ANALYZE TABLE 那样的机制，不知道 status='active' 有多少行，也不知道 amount 的 min/max 分布。没有统计信息，成本估算就是瞎猜。

动态选择：即使实现了统计信息，3000 万行下统计信息本身也很昂贵（要采样或全量扫描）。

现状：你的 src/query/optimizer.rs 只有“主键点查禁倒排”、“组合前缀”这类 硬编码规则，没有基于成本的优化器。这在 3000 万行下是 不可用的。

五、 总结：难点 → 现状 → 风险
难点	当前实现	3000 万行风险	是否可能解决
Compaction 动态自适应	静态阈值 l1_trigger=8 + 空闲感知	写入爆发时 L0 爆炸，点查延迟飙升 100 倍	⚠️ 极难，需要 RocksDB 级别的复杂调优引擎
MVCC + 删除位图版本化	全局稠密 Vec<u8>，关删除位图走 Tombstone	RR 隔离级别违反，幻读/不可重复读	⚠️ 非常难，内存与性能不可兼得
倒排 FST 大 Term 集	base.fst + delta.fst，无 delta 上限	高频写入 → delta 膨胀 → 查询合并超时	⚠️ 需重构为多 Segment 倒排（类 ES）
基于成本的优化器	硬编码规则，无统计信息	多条件查询选错执行计划，慢 1000 倍	⚠️ 几乎不可能，需要完整 ANALYZE + 代价模型
六、 最终建议：务实调整目标
如果你坚持要在 3000 万行全面碾压 MySQL，上述 4 点缺一不可，但每一点都相当于重新写一个 RocksDB/Elasticsearch。

更现实的目标：

接受部分场景的劣势：

复杂多条件查询（非组合索引覆盖的）允许比 MySQL 慢 2~3 倍（因为 LSM 的读放大）。

事务隔离级别降级为 RC（读已提交），放弃 RR（避免版本化删除位图的复杂性）。

定义“优势场景”并做到极致：

主键点查、倒排 COUNT、批量写入、Top-K 排序 —— 这些你已经能碾压 MySQL。

把产品定位为 “大数据量 OLTP + 全文检索”，而不是“通用 MySQL 替代品”。

推迟 Compaction/优化器/多 Segment 倒排：

这 3 个是大数据量（>1 亿）才必须的。在 3000 万行下，静态阈值 + 硬编码规则 配合足够好的硬件（NVMe + 32GB 内存）可以撑住。

最终定论：

你的 LSM 引擎核心代码质量很高，但上述 4 个难点是 数据库领域的世界级难题（RocksDB 花了 10 年才解决 Compaction，Elasticsearch 花了 15 年才稳定多 Segment 倒排）。

不要试图在 3000 万行规模下完美解决它们。把“性价比最高的 30% 场景”做到极致，让用户为“倒排 COUNT 快 1000 倍”买单，而不是为“全表聚合比 MySQL 慢 2 倍”而纠结。这才是 LSM-Tree 数据库的商业化正道。

--------------
在数据库领域，正确性（Correctness）> 性能（Performance） 是铁律。如果为了性能牺牲了 ACID 的 I（隔离性），那么这个数据库在金融、电商等场景下是 不可交付 的。

既然你选择了“正确性优先”，我们就不能关掉 deletion_bitmap_enabled 走 Tombstone（那样 RR 确实形同虚设）。我们需要正视 “MVCC + 删除位图” 这个难题，并找到工程上可实现、且性能可接受的折中方案。

下面我来分析：为什么必须在内存里维护版本化删除信息？以及如何避免 3000 万 × 16 字节 = 480MB 的内存爆炸？

一、 为什么必须在内存里维护删除位图（不能依赖磁盘 Tombstone）？
你可能会想：“MySQL 的 InnoDB 不就是把删除标记（Delete Mark）写在磁盘的聚集索引行里吗？为什么我不能也写在 SST 里？”

InnoDB：更新/删除是原地修改（在 B+Tree 页内标记删除）。读一条数据，只需要一次磁盘 IO 拿回该行，行头里就有 Delete Mark，直接就能判断是否对当前事务可见。

LSM-Tree (你的 cjserver)：更新/删除是追加写入（写一条新的 Tombstone 到 SST）。如果依赖磁盘 Tombstone：

点查 id=100：需要从 L0 读到 L6，找到最新的一条记录，然后看它是不是 Tombstone。

3000 万行下，如果这条数据在 L6（老数据），点查可能要遍历 10 个 SST 文件才能找到它，延迟不可控。

所以，必须在内存里维护一个“最新删除状态”的索引（删除位图），让点查先查内存，如果没被删，再去 LSM 树里读数据。这就避免了扫描磁盘 Tombstone 的开销。

二、 方案探索：如何实现“版本化删除位图”且内存可控？
我们要解决的核心问题是：is_deleted(docid, seq) 需要 O(1) 且内存紧凑。

方案	描述	内存占用 (3000万行)	并发性能	实现复杂度
A. 每个 docid 维护 BTreeMap<seq, bool>	记录该 docid 所有历史删除版本	极高 (~3GB+)，BTreeMap 节点开销巨大	差，需多次跳表查找	中
B. 全局删除位图 + 事务快照“删除日志”	位图记录“最新删除”，事务快照记录“我开始的 seq”	低 (3.75MB)，位图只记录最新状态	极高，O(1) 位运算	复杂 (详见下文)
C. 基于 LSM 的版本化删除 (MVCC 内联)	不维护独立删除位图，利用 LSM 的多版本 + 快照 seq 判断	0 额外内存	点查需扫 LSM 确认最新状态，延迟高	低 (RocksDB 方式)
D. 分层删除位图 (分片 + 布隆)	按 docid 分片，每片维护一个 Vec<(u64, u64)> 最近删除	取决于分片大小和过期策略，可控	中等	极高
结论：

方案 A（BTreeMap）不可行——3000 万行下内存爆炸，直接被否决。

方案 C（扫 LSM）虽然正确，但性能倒退，失去了 LSM 点查快的优势（0.2ms -> 5ms）。

方案 B（全局位图 + 事务日志）是当前业界最成熟的解法（CockroachDB / TiKV 的 MVCC 就是这么做的）。方案 B 是正确的方向。

三、 方案 B 的详细设计：如何用“全局删除位图”支撑 RR？
核心思路：
全局删除位图 (Global Deletion Bitmap)：只记录 “当前已提交的最新删除状态”（不记录历史）。is_deleted(docid) 永远返回“最新已提交”的状态。

事务快照 (Transaction Snapshot)：记录事务开始时的 “全局序列号 (Global Seq)”，以及事务开始时的 “已提交删除列表快照”。

查询时的过滤逻辑：SELECT ... WHERE id=100 时：

如果 bitmap.is_deleted(100) 为 false，走正常 LSM 读。

如果 bitmap.is_deleted(100) 为 true，检查当前事务的 “事务内删除日志 (Local Delete Log)”：

如果该 docid 在当前事务内被删除 → 返回 NULL（不可见）。

如果该 docid 在当前事务内被更新（插入了新版本）→ 返回新版本。

如果该 docid 是在事务开始之后被其他事务删除的 → 根据 RR 规则，应该忽略这个删除，返回事务开始时的旧版本数据（需要从 LSM 的旧版本中读取）。

关键数据结构（内存可控）：
rust
// 1. 全局删除位图 (依然紧凑，只存最新状态)
pub struct GlobalDeletionBitmap {
    // 只记录 "docid 是否被删除（最新已提交状态）"
    // 底层还是 RoaringTreemap (稀疏) 或 Vec<u64> (密集)
    deleted: RoaringTreemap,
}

// 2. 事务快照 (每个事务独有，轻量)
pub struct TransactionSnapshot {
    start_seq: u64,                     // 事务开始的全局序列号
    local_deletes: HashSet<DocId>,      // 本事务内删除的 docid
    local_updates: HashMap<DocId, u64>, // 本事务内更新的 docid -> 新版本 seq
}

// 3. 判断可见性 (O(1))
impl Transaction {
    fn is_visible(&self, docid: DocId, version_seq: u64) -> bool {
        // 如果本事务内删了它 -> 不可见
        if self.local_deletes.contains(&docid) {
            return false;
        }
        // 如果本事务内更新了它 -> 只可见新版本
        if let Some(&new_seq) = self.local_updates.get(&docid) {
            return version_seq == new_seq;
        }
        // 如果是全局删除位图标记为已删
        if GLOBAL_BITMAP.is_deleted(docid) {
            // 但本事务快照开始前它还没被删 -> RR 下应看到旧版本
            // 需要去 LSM 读 version_seq < start_seq 的最新版本
            // 此处返回 false，由上层查 LSM 的旧版本
            return false; 
        }
        true
    }
}
内存评估（3000 万行）：
全局删除位图：假设 10% 的行被删（300 万行），RoaringTreemap 内存约 ~6 MB（Roaring 压缩率极高）。

事务本地缓存：每个事务只缓存自己修改的行。并发 100 个事务，每个事务改 1000 行，内存约 100 * 1000 * (8+8) ≈ 1.6 MB。

总内存 < 10 MB，完全可控。

四、 遗留难题：RR 下，读到“被删行”的旧版本，怎么从 LSM 里捞？
这是方案 B 的唯一难点。LSM 是多版本存储，但你的 engine.get(docid) 默认只返回 最新版本。如果要返回 version_seq < start_seq 的旧版本，必须改造 get 接口：

rust
// 改造后的 get，支持 "读取某个序列号之前的版本"
fn get_version_before(&self, docid: DocId, seq: u64) -> Option<Document> {
    // 遍历 LSM (MemTable + SST)，找到 docid 的 最大版本号 < seq 的记录
    // 如果该记录是 Tombstone，返回 None
}
性能影响：在 3000 万行下，这个操作可能需要扫描多个 SST 文件（因为旧版本可能被 compaction 合并到深层）。

优化手段：

SST 的元数据里缓存“每层的最大/最小 seq”，快速跳过不含目标 seq 范围的 SST。
对频繁读取的“被删行”，在事务内缓存旧版本（一旦读到，缓存在事务的 old_version_cache 里）。
五、 最终结论与建议
方案	正确性	内存占用	点查性能影响	实现复杂度
关删除位图 (当前)	❌ 违反 RR	0	0	0
方案 B (全局位图 + 事务日志)	✅ 完全满足 RR	< 10 MB (3000万行)	点查增加一次位图检查 (几乎无开销) + 偶尔读旧版本 (略慢)	中等 (需改造 get 接口)
建议：

不要关 deletion_bitmap_enabled。正确性优先，哪怕点查偶尔慢一点。

立即实施方案 B。它的核心设计（全局最新状态 + 事务本地补丁）是经过工业界验证的（CockroachDB 的 MVCC 也是这么做的）。

接受“旧版本读取”偶尔慢。RR 下读到旧版本是低频事件（只有当事务 A 开始后，事务 B 删除了 A 要读的行时才会发生）。绝大多数点查依然走 is_deleted == false 的快路径，保持 0.2ms 延迟。

这个难点不是“实现不了”，而是“需要花 2 周时间改造 engine.get 和 Transaction 结构”。但它值得做，因为它是让你的 LSM-Tree 达到 “金融级正确性” 的必经之路。