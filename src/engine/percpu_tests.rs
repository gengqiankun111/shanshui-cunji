//! Task-026 Per-CPU WAL engine 级测试（默认关闭期先行验证；开启后为全量回归的子集）。
//! 覆盖 research/percpu-wal-stage2-design.md §6：单/多队列写-刷-重启回放一致、跨 CF 组、
//! checkpoint 幂等、混合迁移（旧自身 WAL → external）、回退（enabled=false 不受影响）。

use super::*;
use crate::config::model::Config;
use std::sync::atomic::Ordering;

fn tmp() -> std::path::PathBuf {
    static DIR: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let name = format!("engpw-{}", SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
    DIR.get_or_init(|| tempfile::tempdir().unwrap())
        .path()
        .join(name)
}

/// per-CPU 开启的小配置（2 队列 + 短窗口，测试可快速落盘）。
fn cfg_percpu() -> Config {
    let mut c = Config::default();
    c.storage.per_cpu_enabled = true;
    c.storage.per_cpu_queues = 2;
    c.storage.per_cpu_queue_depth = 4096;
    c.storage.per_cpu_batch_window_us = 5000;
    c
}

fn val(i: u64) -> Vec<u8> {
    format!(r#"{{"id":{i},"name":"doc-{i}","ts":{i}}}"#).into_bytes()
}

#[test]
fn percpu_put_flush_reopen_consistent() {
    // 单/双队列：写 → flush_wal（队列落盘）→ drop → 重开回放 = 全量一致（含删除不可见）
    let dir = tmp();
    {
        let mut e = Engine::open(&dir, &cfg_percpu()).unwrap();
        assert!(e.percpu.is_some(), "per_cpu_enabled=true 应建运行时");
        for i in 0..300u64 {
            e.put(i, val(i), &["tag_a"]).unwrap();
        }
        for i in (0..300u64).step_by(7) {
            e.delete(i).unwrap();
        }
        e.flush_wal().unwrap();
        for i in 0..300u64 {
            let v = e.get(i).unwrap();
            if i % 7 == 0 {
                assert!(v.is_none(), "已删 docid {i} 不可见");
            } else {
                assert_eq!(v, Some(val(i)), "docid {i} 值一致");
            }
        }
    } // drop：运行时停机（尾部排空）
    let mut e2 = Engine::open(&dir, &cfg_percpu()).unwrap();
    for i in 0..300u64 {
        let v = e2.get(i).unwrap();
        if i % 7 == 0 {
            assert!(v.is_none(), "重开后已删 docid {i} 不可见");
        } else {
            assert_eq!(v, Some(val(i)), "重开后 docid {i} 值一致");
        }
    }
}

#[test]
fn percpu_checkpoint_trim_no_replay_after_full_flush() {
    // flush（primary+delta）→ cp = min(水位) 推进 → flush_wal 持久化 + 裁剪
    // → 重开不回放已刷记录（幂等不重复）
    let dir = tmp();
    let cp;
    {
        let mut e = Engine::open(&dir, &cfg_percpu()).unwrap();
        for i in 0..200u64 {
            e.put(i, val(i), &[]).unwrap();
        }
        e.flush_wal().unwrap();
        // 双 CF 都刷盘（delta 无记录 → 水位仍低；模拟 delta 有数据场景先写 patch）
        for i in 0..200u64 {
            e.patch(i, &[("extra", serde_json::json!(i))]).unwrap();
        }
        e.flush_wal().unwrap();
        e.primary.switch_and_flush().unwrap();
        e.delta.switch_and_flush().unwrap();
        e.flush_wal().unwrap();
        let rt = e.percpu.as_ref().unwrap();
        cp = rt.cp.load(Ordering::Relaxed);
        let wms: Vec<u64> = rt.cf_watermarks.iter().map(|w| w.load(Ordering::Relaxed)).collect();
        assert!(
            cp > 0,
            "双 CF 刷盘后 checkpoint 应 > 0：cp={cp} wms={wms:?} status={}",
            rt.status()
        );
        assert_eq!(rt.load_checkpoint(), cp, "checkpoint 已持久化");
    }
    let mut e2 = Engine::open(&dir, &cfg_percpu()).unwrap();
    for i in 0..200u64 {
        let v = e2.get(i).unwrap();
        assert!(v.is_some(), "重开后 docid {i} 可见");
    }
    // patch 已覆盖（delta 合并读取）
    for i in 0..200u64 {
        let v = e2.get(i).unwrap().unwrap();
        let doc: serde_json::Value = serde_json::from_slice(&v).unwrap();
        assert_eq!(doc["extra"], serde_json::json!(i), "patch 字段经 delta 回读一致");
    }
}

#[test]
fn percpu_overwrite_order_after_reopen() {
    // 覆盖写序按 gseq：重开后最后写入可见（并发语义 = 串行提交序）
    let dir = tmp();
    let mut e = Engine::open(&dir, &cfg_percpu()).unwrap();
    for i in 0..100u64 {
        e.put(1, format!("v{i}").into_bytes(), &[]).unwrap();
    }
    e.flush_wal().unwrap();
    drop(e);
    let mut e2 = Engine::open(&dir, &cfg_percpu()).unwrap();
    let v = e2.get(1).unwrap().unwrap();
    assert_eq!(v, b"v99", "最后覆盖 v99 可见");
    e2.put(1, b"final".to_vec(), &[]).unwrap();
    e2.flush_wal().unwrap();
    drop(e2);
    let mut e3 = Engine::open(&dir, &cfg_percpu()).unwrap();
    assert_eq!(e3.get(1).unwrap().unwrap(), b"final");
}

#[test]
fn percpu_migration_legacy_to_external() {
    // 6.4 混合/迁移：先旧模式（enabled=false）写库 → enabled=true 重开 → 迁移回放正确
    // （旧自身 WAL 残留强制 flush 落 SST，数据不丢）→ 继续 external 写 → 再次重开一致
    let dir = tmp();
    let mut legacy = Config::default();
    legacy.storage.per_cpu_enabled = false; // 旧模式（自身 WalBackend）
    {
        let mut e = Engine::open(&dir, &legacy).unwrap(); // 旧模式
        assert!(e.percpu.is_none());
        for i in 0..150u64 {
            e.put(i, val(i), &["mig"]).unwrap();
        }
        e.flush_wal().unwrap();
    }
    {
        let mut e = Engine::open(&dir, &cfg_percpu()).unwrap(); // 迁移到 external
        assert!(e.percpu.is_some());
        for i in 0..150u64 {
            let v = e.get(i).unwrap();
            assert!(v.is_some(), "迁移后 docid {i} 保留");
        }
        // 继续 external 写入（新 gseq 从旧数据后接续）
        for i in 150..250u64 {
            e.put(i, val(i), &["mig"]).unwrap();
        }
        e.flush_wal().unwrap();
    }
    let mut e2 = Engine::open(&dir, &cfg_percpu()).unwrap();
    for i in 0..250u64 {
        let v = e2.get(i).unwrap();
        assert_eq!(v, Some(val(i)), "迁移 + external 混合后 docid {i} 一致");
    }
}

#[test]
fn percpu_batch_delete_patch_outbox_roundtrip() {
    // delete_batch / patch / outbox 三条写路径在 per-CPU 下与既有语义一致
    let mut cfg = cfg_percpu();
    cfg.outbox.enabled = true;
    let dir = tmp();
    {
        let mut e = Engine::open(&dir, &cfg).unwrap();
        for i in 0..100u64 {
            e.put(i, val(i), &[]).unwrap();
        }
        // delete_batch（Tombstone 路径：primary/delta 墓碑）
        let removed = e.delete_batch((0..100u64).step_by(3)).unwrap();
        assert_eq!(removed, 34);
        for i in 0..100u64 {
            if i % 3 == 0 {
                assert!(e.get(i).unwrap().is_none());
            } else {
                assert!(e.get(i).unwrap().is_some());
            }
        }
        // patch（delta 增量）
        e.patch(50, &[("extra", serde_json::json!("x"))]).unwrap();
        let v = e.get(50).unwrap().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&v).unwrap()["extra"],
            "x"
        );
        // outbox
        let (docid, seq) = e.enqueue_outbox(50, b"msg").unwrap();
        assert_eq!(docid, 50);
        assert!(seq > 0);
        assert_eq!(e.outbox_pending().unwrap(), 1);
        e.flush_wal().unwrap();
    }
    // 重开：delete_batch/patch/outbox 全部回放一致
    let mut e2 = Engine::open(&dir, &cfg).unwrap();
    for i in 0..100u64 {
        if i % 3 == 0 {
            assert!(e2.get(i).unwrap().is_none(), "重开后批量删 docid {i} 不可见");
        } else if i != 50 {
            assert!(e2.get(i).unwrap().is_some());
        }
    }
    let v = e2.get(50).unwrap().unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&v).unwrap()["extra"],
        "x",
        "重开后 patch 保留"
    );
    assert_eq!(e2.outbox_pending().unwrap(), 1, "重开后 outbox pending 保留");
}

#[test]
fn percpu_disabled_is_legacy_no_regression() {
    // 回退：enabled=false 走既有全局组提交（零回归，基线由全量 suite 覆盖）
    let dir = tmp();
    let mut legacy = Config::default();
    legacy.storage.per_cpu_enabled = false;
    let mut e = Engine::open(&dir, &legacy).unwrap();
    assert!(e.percpu.is_none());
    for i in 0..50u64 {
        e.put(i, val(i), &[]).unwrap();
    }
    e.flush_wal().unwrap();
    drop(e);
    let mut e2 = Engine::open(&dir, &legacy).unwrap();
    for i in 0..50u64 {
        assert_eq!(e2.get(i).unwrap(), Some(val(i)));
    }
}
