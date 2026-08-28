//! IO 速率调度器（design 4.5，阶段 3）：后台 IO（刷盘 / Compaction）限速。
//!
//! 目标：后台 IO 打满磁盘带宽会拖垮前台读写（写 P99 飙升）。用 **Token Bucket**
//! 把后台 IO 速率限制在 `storage.io_rate_limit_mb`（0 = 不限速）：
//! - 桶容量 = 1 秒配额（允许小突发），按速率持续补桶；
//! - `acquire(n)`：额度不足则睡眠等待补桶（限速语义，不失败）；
//! - 与前台读写隔离：限速只作用于刷盘 / Compaction 等后台路径。
//!
//! > 阶段 3 完整形态：ionice 最低优先级 + 速率调度；此处提供可测的速率门控内核。

use std::time::{Duration, Instant};

use crate::error::Result;

/// Token Bucket IO 限速器（单线程后台任务持有；0 = 不限速）。
pub struct IoRateLimiter {
    /// 补桶速率（字节/秒）；0 = 不限速。
    rate_bytes_per_sec: u64,
    /// 当前可用令牌（字节）。
    capacity: f64,
    /// 桶容量（允许突发）：1 秒配额。
    burst: f64,
    last_refill: Instant,
}

impl IoRateLimiter {
    /// `bytes_per_sec` 为限速值；0 = 不限速。
    pub fn new(bytes_per_sec: u64) -> Self {
        let burst = bytes_per_sec as f64;
        Self {
            rate_bytes_per_sec: bytes_per_sec,
            capacity: burst,
            burst,
            last_refill: Instant::now(),
        }
    }

    /// 获取 `n` 字节 IO 额度：额度不足则睡眠等待补桶（限速语义）。
    pub fn acquire(&mut self, n_bytes: u64) -> Result<()> {
        if self.rate_bytes_per_sec == 0 || n_bytes == 0 {
            return Ok(());
        }
        loop {
            self.refill();
            if self.capacity >= n_bytes as f64 {
                self.capacity -= n_bytes as f64;
                return Ok(());
            }
            // 额度不足：等待补足差额所需时间
            let deficit = n_bytes as f64 - self.capacity;
            let wait_secs = deficit / self.rate_bytes_per_sec as f64;
            std::thread::sleep(Duration::from_secs_f64(wait_secs.max(0.001)));
        }
    }

    /// 是否启用限速。
    pub fn is_limited(&self) -> bool {
        self.rate_bytes_per_sec > 0
    }

    fn refill(&mut self) {
        let elapsed = self.last_refill.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.capacity = (self.capacity + elapsed * self.rate_bytes_per_sec as f64)
                .min(self.burst);
            self.last_refill = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burst_consumed_immediately() {
        let mut l = IoRateLimiter::new(10_000); // 10KB/s，桶 10KB
        let t = Instant::now();
        l.acquire(5_000).unwrap(); // 桶内直接放行（突发）
        assert!(t.elapsed().as_millis() < 50, "突发额度应立即放行");
    }

    #[test]
    fn rate_limits_steady_io() {
        // 速率 2KB/s：4 次取 1KB → 桶内放行 2 次，另 2 次各等 ~0.5s → 总 ≥0.9s
        let mut l = IoRateLimiter::new(2_000);
        let t = Instant::now();
        for _ in 0..4 {
            l.acquire(1_000).unwrap();
        }
        let elapsed = t.elapsed().as_secs_f64();
        assert!(elapsed >= 0.9, "限速应生效，实际 {elapsed:.2}s");
        assert!(elapsed < 3.0, "不应过度限速，实际 {elapsed:.2}s");
    }

    #[test]
    fn unlimited_does_not_sleep() {
        let mut l = IoRateLimiter::new(0);
        let t = Instant::now();
        for _ in 0..10 {
            l.acquire(1_000_000).unwrap();
        }
        assert!(t.elapsed().as_millis() < 50, "不限速应立即放行");
        assert!(!l.is_limited());
    }

    #[test]
    fn zero_bytes_is_noop() {
        let mut l = IoRateLimiter::new(100);
        l.acquire(0).unwrap();
        assert!(l.is_limited());
    }
}
