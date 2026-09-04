# SQL 性能探针（sqlrun）

url=mysql://root@127.0.0.1:3317  表=documents  N=1098342  宽表 25 列  环境=SCC（3317，2G 内存预算 = hotcache 1024 + blockcache 512 + inverted 256 + memtable 256 MB）

| # | 类别 | 探针 | 说明 | OK/n | 行/影响 | mean ms | p50 ms | p99 ms | max ms |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 点查 | pk_point_star | SELECT * | 300/300 | 1 | 0.27 | 0.24 | 0.51 | 0.56 |
| 2 | 点查 | pk_point_proj10 | 10 列投影 | 300/300 | 1 | 0.49 | 0.48 | 0.67 | 0.69 |
| 3 | 点查 | pk_in_5 | id IN 5 点 | 150/150 | 5 | 0.46 | 0.45 | 0.62 | 0.73 |
| 4 | 点查 | pk_in_50 | id IN 50 点（批量查询） | 30/30 | 50 | 1.92 | 1.85 | 2.93 | 2.93 |
| 5 | 范围 | pk_between_100 | 100 行窗口 | 150/150 | 101 | 4.05 | 3.84 | 6.46 | 6.85 |
| 6 | 倒排 | enum_sel_limit100 | 枚举等值 bitmap | 60/60 | 100 | 4.20 | 3.81 | 6.10 | 6.10 |
| 7 | 倒排 | enum_count | COUNT 倒排载荷 | 60/60 | 1 | 0.66 | 0.62 | 0.94 | 0.94 |
| 8 | 倒排 | combo_and | 枚举×枚举 AND | 60/60 | 100 | 3.49 | 3.40 | 5.40 | 5.40 |
| 9 | 倒排 | field_in | 字段 IN 列表 | 60/60 | 100 | 3.19 | 3.09 | 4.69 | 4.69 |
| 10 | 扫描 | cmp_gt_limit50 | 数值> LIMIT 早停 | 20/20 | 50 | 2.47 | 2.27 | 5.88 | 5.88 |
| 11 | 扫描 | cmp_between | 数值 BETWEEN（全扫） | 20/20 | 62 | 6686.22 | 6642.59 | 7240.51 | 7240.51 |
| 12 | 聚合 | count_all | 无条件 COUNT | 5/5 | 1 | 5868.51 | 5701.20 | 6491.70 | 6491.70 |
| 13 | 聚合 | sum_where_enum | SUM WHERE（全扫） | 3/3 | 1 | 6523.94 | 6313.53 | 6995.38 | 6995.38 |
| 14 | 聚合 | group_by_status | 全扫分组 | 3/3 | 5 | 6434.92 | 6413.89 | 6582.41 | 6582.41 |
| 15 | 聚合 | group_by_sum_having | 多聚合+HAVING(函数式) | 3/3 | 5 | 6507.41 | 6501.44 | 6523.13 | 6523.13 |
| 16 | 排序 | orderby_win_1000 | 窗口 1000 ORDER BY | 20/20 | 20 | 8.13 | 10.65 | 12.97 | 12.97 |
| 17 | 写 | update_id | UPDATE id= | 100/100 | 1 | 0.32 | 0.23 | 1.43 | 1.43 |
| 18 | 写 | update_in2 | UPDATE id IN 2 | 50/50 | 2 | 0.37 | 0.26 | 1.32 | 1.32 |
| 19 | 写 | update_in50 | UPDATE id IN 50（批量更新） | 20/20 | 50 | 2.20 | 2.41 | 3.27 | 3.27 |
| 20 | 写 | insert_single | INSERT 单行 | 100/100 | 1 | 0.30 | 0.22 | 1.34 | 1.34 |
| 21 | 写 | insert_batch10 | INSERT 10 行/语句 | 30/30 | 10 | 0.59 | 0.42 | 1.71 | 1.71 |
| 22 | 写 | insert_batch100 | INSERT 100 行/语句 | 10/10 | 100 | 3.41 | 3.50 | 5.11 | 5.11 |
| 23 | 写 | delete_id | DELETE id= | 100/100 | 1 | 0.19 | 0.10 | 4.33 | 4.33 |
| 24 | 写 | delete_range50 | DELETE 50 行区间（批量删除） | 10/10 | 0 | 7536.73 | 7545.58 | 8538.60 | 8538.60 |
| 25 | 事务 | txn_begin_upd_commit | BEGIN→UPDATE→COMMIT | 100/100 | 1 | 3.35 | 3.20 | 12.61 | 12.61 |
| 26 | 事务 | txn_for_update_read | BEGIN→FOR UPDATE→COMMIT | 50/50 | 1 | 0.58 | 0.54 | 0.86 | 0.86 |
| 27 | 聚合 | group_by_multi | GROUP BY status,region | 3/3 | 40 | 6894.26 | 6905.43 | 6961.14 | 6961.14 |
| 28 | 聚合 | having_avg_gt | GROUP BY+HAVING AVG>阈值 | 3/3 | 4 | 6701.29 | 6692.81 | 6825.02 | 6825.02 |
| 29 | 排序 | orderby_multi | ORDER BY k,amount LIMIT 100 | 0/10 | 0 | 0.00 | 0.00 | 0.00 | 0.00 |  ❌ MySqlError { ERROR 1064 (HY000): query error: query too expensive: ORDER BY 候选集过大（1100481 行，上限 200000），请加 WHERE 收敛或用 LIMIT }
| 30 | 索引 | composite_idx_point | status= + ts= 点查 | 100/100 | 0 | 921.46 | 839.29 | 5191.41 | 5191.41 |
| 31 | 索引 | composite_idx_range | ts 范围（非前置列） | 20/20 | 368 | 7300.89 | 7340.78 | 9632.46 | 9632.46 |
| 32 | 批量写 | insert_batch_10000 | INSERT 10000 行/语句 | 3/3 | 10000 | 396.07 | 401.93 | 442.32 | 442.32 |
| 33 | 批量写 | upsert_duplicate_key | INSERT..ON DUPLICATE KEY 单行 | 100/100 | 2 | 0.48 | 0.22 | 12.25 | 12.25 |
| 34 | 批量写 | upsert_batch_100 | INSERT..ON DUP KEY 100行/语句 | 10/10 | 200 | 8.39 | 7.66 | 16.47 | 16.47 |
| 35 | 事务 | txn_rr_readwrite | RR 读写事务 | 50/50 | 1 | 3.52 | 3.46 | 7.52 | 7.52 |
| 36 | 事务 | txn_serializable | SERIALIZABLE 读写事务 | 50/50 | 1 | 3.38 | 3.28 | 5.22 | 5.22 |
| 37 | 事务 | txn_lock_wait | 并发 FOR UPDATE 锁等待 | 5/5 | 0 | 4002.13 | 4001.78 | 4003.74 | 4003.74 |
