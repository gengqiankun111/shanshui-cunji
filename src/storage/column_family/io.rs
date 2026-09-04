//! IO 速率自适应：后台/扫描限速器、写压力、MVCC 保活水位与有效阈值。

use std::path::Path;
use std::sync::atomic::Ordering;

use crate::error::Result;

use super::*;

impl ColumnFamily {
    /// 按行序列写一个 SST（TTL 分桶用；行内 key 已升序）。
    /// 后台 IO 限速：刷盘完成后按实际文件字节数 acquire（design 4.5 阶段 3；不限速时为空操作）。
    /// O 项第③步：`&self`（io_limiter 内部 Mutex，compact 读路径并发限速）。
    pub(crate) fn io_acquire(&self, path: &Path) -> Result<()> {
        if let Some(limiter) = &self.io_limiter {
            let sz = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            limiter.lock().unwrap().acquire(sz)?;
        }
        Ok(())
    }

    /// Ex-7.4：动态调整后台 IO 限速（前台写压力驱动，Engine 调 Compaction 让路）。
    pub fn set_io_rate_bytes(&self, bytes_per_sec: u64) {
        if let Some(limiter) = &self.io_limiter {
            limiter.lock().unwrap().set_rate(bytes_per_sec);
        }
    }

    /// 导出共享后台 IO 限速（design 20.5）：启用/调整顺序扫描路径限速器
    /// （0 = 关闭，恢复前台读不限速）。与 Compaction `io_limiter` 同 Token Bucket 策略——
    /// 导出读 SST 与后台合并共享同一后台 IO 预算语义，默认低于前台读写。
    pub fn set_scan_rate_limit(&self, bytes_per_sec: u64) {
        *self.scan_limiter.lock().unwrap() = if bytes_per_sec > 0 {
            Some(crate::io_scheduler::IoRateLimiter::new(bytes_per_sec))
        } else {
            None
        };
    }

    /// L 项：设置前台写压力（0~1，Ex-7.4 同源信号：MemTable 水位代理）——动态窗口反馈依据。
    /// 写压力高 → 有效 L0 阈值收窄（提前收敛防堆积 + 写 Stall）；低 → 放宽（降合并次数/写放大）。
    pub fn set_write_pressure(&self, p: f64) {
        self.write_pressure
            .store(p.clamp(0.0, 1.0).to_bits(), Ordering::Relaxed);
    }

    /// R4：设置 MVCC 保活水位（seq floor，0 = 关闭）。引擎在 compact 前按活跃快照
    /// 低水位设置；compact 结束后调用方复位 0。
    pub fn set_mvcc_keep_floor(&self, floor: u64) {
        self.mvcc_keep_floor.store(floor, Ordering::Release);
    }

    /// L 项：有效 L0 阈值 = 基础阈值 ± 压力调整（clamp 在 [min, max]）。
    /// 滞回由压力平滑（Ex-7.4 每次写后按水位重算）保证，防窗口振荡。
    pub(crate) fn effective_l0_threshold(&self) -> usize {
        let pressure = f64::from_bits(self.write_pressure.load(Ordering::Relaxed));
        let range = self.l0_stall_max.saturating_sub(self.l0_stall_min);
        let shrink = (range as f64 * pressure) as usize;
        self.l0_stall_threshold
            .saturating_sub(shrink)
            .clamp(self.l0_stall_min, self.l0_stall_max)
    }

    /// P4-A：Compaction 写入速率自适应——记录本次 flush 新增 L0 段数，
    /// 更新滑动窗口，检测写入爆发并动态调整 `l1_trigger_files`。
    /// 爆发（窗口内新增 > 阈值）→ `l1_trigger` 降为 2 → 提前下沉 L1→L2，防 L0 爆胀；
    /// 正常 → 恢复基准 `base_l1_trigger`。
    pub fn record_flush_new_l0(&self, new_segments: usize) {
        let mut window = self.write_rate_window.lock().unwrap();
        // 滑动窗口：窗口满 → 弹出最旧，加入最新
        if window.len() >= self.write_rate_window_size {
            window.remove(0);
        }
        window.push(new_segments);
        // 计算窗口内总新增段数，判断是否爆发
        let total: usize = window.iter().sum();
        drop(window);

        // 原子安全地调整 l1_trigger_files（AtomicUsize 无需 &mut）
        if total > self.write_rate_burst_threshold {
            // 写入爆发 → 降阈值，提前合并
            self.l1_trigger_files.store(2.max(self.base_l1_trigger / 2), Ordering::Relaxed);
        } else {
            // 正常写入 → 恢复基准，攒批降写放大
            self.l1_trigger_files.store(self.base_l1_trigger, Ordering::Relaxed);
        }
    }

    /// P4-A：获取当前有效 `l1_trigger_files`（基准/爆发自适应）。
    pub fn effective_l1_trigger(&self) -> usize {
        self.l1_trigger_files.load(Ordering::Relaxed)
    }

    /// Ex-7.4：当前后台 IO 限速（字节/秒；0 = 不限速/未配置）。
    pub fn io_rate(&self) -> u64 {
        self.io_limiter
            .as_ref()
            .map_or(0, |l| l.lock().unwrap().rate())
    }
}
