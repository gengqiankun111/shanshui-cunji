//! §26 真多表（M1）：表级 docid 命名空间辅助（docid = table_id<<48 | row_id）。
//! 引擎层 docid 仅为 u64 键；隔离在 mysql 兼容层完成。本模块收纳需引擎能力的辅助。

use crate::engine::Engine;

/// row_id 位宽 48bit：单表 row 容量 2^48，SQL id 显示为 row_id。
pub const ROW_ID_MASK: u64 = (1 << 48) - 1;

/// table_id → docid 区间基址（表数据落在 [base, base + 2^48)）。
pub fn table_base(tid: u16) -> u64 {
    (tid as u64) << 48
}

/// DROP TABLE 非默认表：清本表 docid 区间（keys-only 扫 + 逐 docid 位图删）。
/// M3：逐 docid 逻辑删除完成后，追加**磁盘文件级回收**——表切分后主列族内完全落
/// 在该表区间的 SST 整文件删除（墓碑已覆盖全部该表键，删文件不改变可见性）；
/// 混表老文件 / 内存残留由墓碑 + 后续压缩收敛。返回逻辑删除的 docid 数。
pub fn drop_table_range(engine: &mut Engine, tid: u16) -> crate::error::Result<usize> {
    let base = table_base(tid);
    let top = base + ROW_ID_MASK;
    let mut ids = Vec::new();
    engine.scan_stream_ids(Some(base), Some(top), |d| {
        ids.push(d);
        Ok(true)
    })?;
    for d in &ids {
        engine.delete(*d)?;
    }
    // M3 实施清单④：物理回收该表专属 SST 文件（best-effort——逻辑删已保证语义，
    // 文件删失败仅延迟回收，不影响 DROP 结果）
    let _ = engine.drop_table_sst_files(tid);
    Ok(ids.len())
}