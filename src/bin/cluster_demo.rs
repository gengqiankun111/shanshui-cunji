//! 两节点分布式集群演示/测试入口（本机 + 阿里云真机两节点，design 9.x M5 组件复用）：
//!
//! 用法：
//!   # 分片节点（每台机器跑一个，--group-commit-us 开节点组提交，C 项③）：
//!   shanshui-cunji-cluster-demo --node --bind 0.0.0.0:9091 --data-dir <dir> [--group-commit-us 2000]
//!   # 网关 + 高并发强一致校验（任一台跑，nodes 指向两节点）：
//!   shanshui-cunji-cluster-demo --gateway --nodes node-a=127.0.0.1:9091,node-b=<服务器IP>:9092 \
//!       --threads 8 --docs 20000 [--batch 500]
//!
//! 网关校验：N 线程 × (docs/threads) 并发写（C 项①：每线程独立 Gateway 无全局 Mutex；
//! C 项②：--batch N 按 docid 哈希分组 → 每节点一次 RPC 批量提交）→ 广播检索精确命中
//! （无丢失/无重复）+ 逐条点查跨节点确定性路由强一致可见 + 探活在线。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use shanshui_cunji::config::Config;
use shanshui_cunji::engine::Engine;
use shanshui_cunji::gateway::{Gateway, RpcShardEndpoint};
use shanshui_cunji::meta::MetaCenter;
use shanshui_cunji::rpc::{register_shard_handlers, RpcServer};

fn usage() -> ! {
    eprintln!(
        "用法:\n  --node --bind <addr> --data-dir <dir> [--group-commit-us US]\n  --gateway --nodes id=addr,id=addr [--threads N] [--docs N] [--batch N]"
    );
    std::process::exit(2);
}

fn arg(args: &[String], key: &str) -> String {
    let mut it = args.iter();
    while let Some(k) = it.next() {
        if k == key {
            if let Some(v) = it.next() {
                return v.clone();
            }
        }
    }
    usage();
}

fn opt_arg(args: &[String], key: &str, default: u64) -> u64 {
    let mut it = args.iter();
    while let Some(k) = it.next() {
        if k == key {
            if let Some(v) = it.next() {
                return v.parse().unwrap_or(default);
            }
        }
    }
    default
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        usage();
    }
    match args[1].as_str() {
        "--node" => run_node(&args),
        "--gateway" => run_gateway(&args),
        _ => usage(),
    }
}

/// 分片节点：RPC 分片处理器（一致性哈希路由的存储端）。
/// C 项③：`--group-commit-us US` 开启节点组提交（默认 2000µs 窗口）——窗口内写入攒批一次
/// fsync，跨地域写吞吐关键（设计 4.3 / M8，`storage.group_commit_us`）。
fn run_node(args: &[String]) {
    let bind = arg(args, "--bind");
    let data_dir = PathBuf::from(arg(args, "--data-dir"));
    let gc_us = opt_arg(args, "--group-commit-us", 2_000);
    let mut cfg = Config::default();
    cfg.storage.group_commit_us = gc_us;
    let engine = Arc::new(std::sync::Mutex::new(
        Engine::open(&data_dir, &cfg).expect("打开引擎失败"),
    ));
    let server = RpcServer::new();
    register_shard_handlers(&server, engine);
    println!(
        "[cluster-node] 分片节点监听 {bind}（data-dir={}，group_commit_us={gc_us}）",
        data_dir.display()
    );
    server.serve(&bind).expect("RPC serve 失败");
}

/// 网关 + 高并发强一致校验（真机两节点）。
fn run_gateway(args: &[String]) {
    let nodes = arg(args, "--nodes");
    let threads: usize = arg(args, "--threads").parse().unwrap_or(8);
    let docs: u64 = arg(args, "--docs").parse().unwrap_or(20_000);

    let mut meta = MetaCenter::new(128);
    for (i, nv) in nodes.split(',').enumerate() {
        let (id, addr) = nv
            .split_once('=')
            .unwrap_or_else(|| usage());
        let role = if i == 0 { "master" } else { "slave" };
        meta.register(id, addr, role).expect("注册节点失败");
    }
    let mut gw = Gateway::new(meta.clone(), RpcShardEndpoint::new(meta.clone()));

    // 探活
    let dead = gw.ping_all();
    assert!(dead.is_empty(), "节点不在线: {dead:?}");
    println!("[gateway] 节点全部在线：{nodes}");

    // 高并发写：threads 线程 × 每线程 docs/threads（独立 docid 区间）。
    // C 项①：每线程独立 Gateway 实例（独立 RPC 连接集合，无全局 Mutex 串行）；
    // C 项②：--batch N 攒批 → put_batch（Gateway 按 docid 哈希分组 → 每节点一次 RPC 批量提交）
    let per = docs / threads as u64;
    let batch = opt_arg(args, "--batch", 500).max(1);
    let mut hs = Vec::new();
    let t0 = std::time::Instant::now();
    for t in 0..threads as u64 {
        let meta = meta.clone();
        hs.push(std::thread::spawn(move || {
            let mut gw = Gateway::new(meta.clone(), RpcShardEndpoint::new(meta));
            let mut buf: Vec<(u64, String, Vec<String>)> = Vec::with_capacity(batch as usize);
            for i in 0..per {
                let d = t * per + i + 1;
                buf.push((
                    d,
                    format!("{{\"d\":{d}}}"),
                    vec!["status=active".to_string()],
                ));
                if buf.len() as u64 == batch {
                    let counts = gw.put_batch(&buf).unwrap();
                    assert_eq!(counts.values().sum::<usize>(), buf.len(), "批量写入计数不符");
                    buf.clear();
                }
            }
            if !buf.is_empty() {
                let counts = gw.put_batch(&buf).unwrap();
                assert_eq!(counts.values().sum::<usize>(), buf.len(), "批量写入计数不符");
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    let write_elapsed = t0.elapsed();
    println!(
        "[gateway] 并发写完成：{threads} 线程 × {per} = {} 条（batch={batch}，分片并行+批量写入），耗时 {:.1}s（{} w/s）",
        threads as u64 * per,
        write_elapsed.as_secs_f64(),
        (threads as u64 * per) as f64 / write_elapsed.as_secs_f64()
    );

    let mut gw = gw;
    // 强一致：广播检索精确命中（无丢失/无重复）
    let all = gw.broadcast_search("status=active").unwrap();
    assert_eq!(
        all.len() as u64,
        threads as u64 * per,
        "广播检索命中数 = 写入总数（无丢失/重复）"
    );
    // 逐条点查：跨节点确定性路由强一致可见
    for d in 1..=threads as u64 * per {
        assert!(gw.get(d).unwrap().is_some(), "docid={d} 强一致可见");
    }
    assert!(gw.ping_all().is_empty(), "两节点在线");
    println!(
        "[gateway] ✅ 强一致校验通过：{} 条全部可见、广播精确命中（{:.1}s）",
        threads as u64 * per,
        write_elapsed.as_secs_f64()
    );
    let _ = HashMap::<String, String>::new();
}
