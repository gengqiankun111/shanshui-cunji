//! Sidecar 文件锁心跳（design 14.4，阶段 2）：主进程每 `interval` 写心跳文件；
//! 探针读取文件，若距今超过 `interval × max_missed` 则判定主进程死锁，触发重启
//! （独立子进程拉起留阶段 2 后续）。

use std::time::Duration;

/// 心跳探针侧判定：主进程心跳文件是否新鲜。
///
/// 主进程每 `interval` 写一次心跳文件（unix 毫秒时间戳）；探针（独立线程 / 子进程）
/// 读取文件，若距今超过 `interval × max_missed` 则判定主进程死锁，触发重启。
pub struct HeartbeatProbe {
    path: std::path::PathBuf,
    interval: Duration,
    max_missed_pings: u32,
}

impl HeartbeatProbe {
    pub fn new(path: std::path::PathBuf, interval: Duration, max_missed_pings: u32) -> Self {
        Self {
            path,
            interval,
            max_missed_pings,
        }
    }

    /// 心跳是否新鲜（主进程存活）。
    pub fn is_alive(&self) -> bool {
        let now_ms = now_millis();
        let last_ms = std::fs::read_to_string(&self.path)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok());
        match last_ms {
            Some(last) => {
                let tolerance = self.interval.as_millis() as u64 * self.max_missed_pings as u64;
                now_ms.saturating_sub(last) <= tolerance
            }
            None => false,
        }
    }
}

/// Sidecar 心跳线程：主进程内启动，每 `interval` 更新心跳文件。
pub struct HeartbeatSidecar {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl HeartbeatSidecar {
    /// 启动心跳线程。`path` 为心跳文件路径，`interval` 为心跳间隔。
    pub fn start(path: std::path::PathBuf, interval: Duration) -> Self {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop2 = stop.clone();
        let handle = std::thread::spawn(move || {
            while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                if let Some(p) = path.parent() {
                    let _ = std::fs::create_dir_all(p);
                }
                let _ = std::fs::write(&path, now_millis().to_string());
                std::thread::sleep(interval);
            }
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    /// 停止心跳线程（Drop 语义由调用方保证）。
    pub fn stop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for HeartbeatSidecar {
    fn drop(&mut self) {
        self.stop();
    }
}

/// 当前 unix 毫秒时间戳（探针判定新鲜度用）。
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Sidecar 文件锁心跳（design 14.4）----

    #[test]
    fn heartbeat_sidecar_keeps_probe_alive() {
        let dir = tempfile::tempdir().unwrap();
        let hb_path = dir.path().join("heartbeat.hb");
        let mut sidecar = HeartbeatSidecar::start(hb_path.clone(), Duration::from_millis(10));
        let probe = HeartbeatProbe::new(hb_path.clone(), Duration::from_millis(10), 3);
        // 等前几次心跳写入
        std::thread::sleep(Duration::from_millis(50));
        assert!(probe.is_alive(), "心跳线程运行中探针应判定存活");
        // 停止心跳 → 超过 max_missed × interval 后判定死锁
        sidecar.stop();
        std::thread::sleep(Duration::from_millis(60));
        assert!(!probe.is_alive(), "心跳停止后探针应判定死锁");
    }

    #[test]
    fn heartbeat_probe_missing_file_is_dead() {
        let dir = tempfile::tempdir().unwrap();
        let probe = HeartbeatProbe::new(dir.path().join("nope.hb"), Duration::from_millis(10), 3);
        assert!(!probe.is_alive(), "心跳文件不存在应判定死锁");
    }
}
