//! shanshui-cunji-bench：分配器高并发压测（musl/glibc × mimalloc/系统 对比基线）。
//!
//! 用法：`shanshui-cunji-bench --threads 4 --ops 200000`
//! 每线程迭代模拟数据库负载形态的高频小块分配：
//! JSON 序列化/反序列化、Vec 缓冲区、String 拼接、HashMap 插入删除、Box 小对象。
//!
//! 对比方法：同一二进制分别用 mimalloc（默认 feature）与系统分配器
//! （`cargo build --release --no-default-features`）构建，观察多线程吞吐差异。

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use serde_json::json;

fn main() {
    // 强制链接 lib crate：global_allocator（mimalloc）定义在 lib，bin 不引用 lib 符号
    // 会被链接器 GC 丢弃，导致分配器不生效（压测失真）。
    let _force_lib = shanshui_cunji::error::Error::NotFound("bench".into());

    let args: Vec<String> = std::env::args().collect();
    let get = |name: &str, def: usize| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(def)
    };
    let threads = get("--threads", 4);
    let ops = get("--ops", 200_000);

    let allocator = if cfg!(feature = "alloc-jemalloc") {
        "jemalloc"
    } else if cfg!(feature = "alloc-mimalloc") {
        "mimalloc"
    } else {
        "system"
    };
    println!("[bench] threads={threads} ops/thread={ops} allocator={allocator}");

    let t0 = Instant::now();
    let total = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..threads {
        let total = total.clone();
        handles.push(thread::spawn(move || {
            let mut acc = 0u64;
            for _ in 0..ops {
                acc = acc.wrapping_add(bench_iteration());
            }
            total.fetch_add(acc, Ordering::Relaxed);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = t0.elapsed();
    let total_ops = (threads as u64) * (ops as u64);
    let qps = total_ops as f64 / elapsed.as_secs_f64();
    let _ = total.load(Ordering::Relaxed);
    println!(
        "[bench] total_ops={total_ops} elapsed={:?} qps={:.0}",
        elapsed, qps
    );
}

/// 一次迭代：混合分配负载（模拟数据库高频小块分配形态）。返回一个校验和防止优化消除。
fn bench_iteration() -> u64 {
    // ① 文档 JSON 序列化 / 反序列化（每次多块分配）
    let doc = json!({
        "docid": 1u64,
        "status": "active",
        "name": "alice",
        "tags": ["a", "b", "c"],
        "nested": {"k": "v", "n": 42},
    });
    let bytes = serde_json::to_vec(&doc).unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let j = parsed["nested"]["n"].as_u64().unwrap_or(0);

    // ② 变长 Vec 缓冲区
    let mut buf = Vec::with_capacity(512);
    buf.extend(std::iter::repeat(0u8).take(256 + (j as usize) % 256));
    let b = buf.len() as u64;

    // ③ String 拼接
    let s = format!("key-{}-{}", 12345u64, "value-suffix");
    let sl = s.len() as u64;

    // ④ HashMap 插入 / 清空
    let mut map: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();
    for i in 0..8u64 {
        map.insert(format!("k{i}"), vec![0u8; 64 + (i as usize) % 32]);
    }
    let m = map.len() as u64;
    drop(map);

    // ⑤ Box 小对象
    let small = Box::new([0u8; 64]);
    let sx = small[0] as u64;

    black_box((j + b + sl + m + sx) as u64)
}
