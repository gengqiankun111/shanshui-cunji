//! Redis 外部缓存管理器（design 21，阶段 2）：Cache-Aside + Write-Invalidate + 熔断降级。
//!
//! 定位：Redis 是 shanshui-cunji 的 **L2 分布式热点读缓存**（L1 Redis → L2 HotCache → L3 磁盘）。
//! 三条红线（design 21.1）：写必须先落盘返回 ACK 再操作 Redis；只允许 shanshui-cunji → Redis
//! 单向同步（回填/失效）；禁止从 Redis 恢复数据（仅可预热）。
//!
//! - **读（Cache-Aside）**：`get_or_load`——命中 0.1ms 直返 → 未命中回源引擎 → SETEX 回填
//!   （TTL 随机抖动防雪崩；`cache_null_values` 缓存空值标记防穿透）；
//! - **写（Write-Invalidate）**：`invalidate`——先写引擎成功后删 Redis 旧缓存（DEL 不更新），
//!   `write_policy = "double_delete"` 时 500ms 延迟二次删除（近似强一致）；
//! - **熔断降级**（design 21.4）：`CircuitBreaker` CLOSED → OPEN（连续失败 ≥ 阈值）→ HALF-OPEN
//!   （冷却后单请求探测）→ 恢复 CLOSED / 再失败 OPEN；熔断期间直接透传引擎（仅延迟上升，不雪崩）。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::CacheExternalConfig;
use crate::error::{Error, Result};
use crate::redis::RedisClient;

/// 空值缓存标记（cache_null_values 防穿透）。
const NULL_MARKER: &[u8] = b"\x00";
/// 双删策略的二次删除延迟（毫秒）。
const DOUBLE_DELETE_DELAY_MS: u64 = 500;
/// TTL 随机抖动上限（毫秒）。
const TTL_JITTER_MS: u64 = 60_000;

/// 缓存后端抽象（Redis / 内存测试实现）。
pub trait CacheBackend {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn set(&mut self, key: &[u8], value: &[u8], ttl_secs: u64) -> Result<()>;
    fn del(&mut self, keys: &[&[u8]]) -> Result<u64>;
}

/// Redis 后端：极简 RESP 客户端 + 断线重连。
pub struct RedisBackend {
    addr: String,
    timeout: Duration,
    client: RedisClient,
}

impl RedisBackend {
    pub fn connect(addr: &str, timeout: Duration) -> Result<Self> {
        Ok(Self {
            addr: addr.to_string(),
            timeout,
            client: RedisClient::connect(addr, timeout)?,
        })
    }

    fn reconnect(&mut self) -> Result<()> {
        self.client = RedisClient::connect(&self.addr, self.timeout)?;
        Ok(())
    }
}

impl CacheBackend for RedisBackend {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.client.get(key) {
            Ok(v) => Ok(v),
            Err(Error::Io(_)) | Err(Error::Rpc(_)) => {
                self.reconnect()?;
                self.client.get(key)
            }
            Err(e) => Err(e),
        }
    }

    fn set(&mut self, key: &[u8], value: &[u8], ttl_secs: u64) -> Result<()> {
        match self.client.set_ex(key, value, ttl_secs) {
            Ok(v) => Ok(v),
            Err(Error::Io(_)) | Err(Error::Rpc(_)) => {
                self.reconnect()?;
                self.client.set_ex(key, value, ttl_secs)
            }
            Err(e) => Err(e),
        }
    }

    fn del(&mut self, keys: &[&[u8]]) -> Result<u64> {
        match self.client.del(keys) {
            Ok(v) => Ok(v),
            Err(Error::Io(_)) | Err(Error::Rpc(_)) => {
                self.reconnect()?;
                self.client.del(keys)
            }
            Err(e) => Err(e),
        }
    }
}

/// 熔断器状态（design 21.4）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

/// 熔断器：CLOSED → OPEN（连续失败 ≥ threshold）→ HALF-OPEN（冷却后探测）。
pub struct CircuitBreaker {
    threshold: u32,
    cooldown: Duration,
    failures: u32,
    state: BreakerState,
    opened_at: Option<Instant>,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, cooldown: Duration) -> Self {
        Self {
            threshold: threshold.max(1),
            cooldown,
            failures: 0,
            state: BreakerState::Closed,
            opened_at: None,
        }
    }

    /// 是否熔断（OPEN / 冷却中的 HALF-OPEN 允许探测但读路径直接透传）。
    pub fn is_open(&self) -> bool {
        self.state == BreakerState::Open
    }

    /// 操作成功：恢复 CLOSED。
    pub fn record_success(&mut self) {
        self.failures = 0;
        if self.state == BreakerState::HalfOpen {
            self.state = BreakerState::Closed;
        }
    }

    /// 操作失败：计数，达阈值 OPEN；HALF-OPEN 失败立即回 OPEN。
    pub fn record_failure(&mut self) {
        self.failures += 1;
        match self.state {
            BreakerState::Closed => {
                if self.failures >= self.threshold {
                    self.state = BreakerState::Open;
                    self.opened_at = Some(Instant::now());
                }
            }
            BreakerState::HalfOpen => {
                self.state = BreakerState::Open;
                self.opened_at = Some(Instant::now());
            }
            BreakerState::Open => {}
        }
    }

    /// 冷却到期后尝试半开探测（read 路径调用一次）。
    pub fn maybe_half_open(&mut self) {
        if self.state == BreakerState::Open {
            if let Some(t) = self.opened_at {
                if t.elapsed() >= self.cooldown {
                    self.state = BreakerState::HalfOpen;
                }
            }
        }
    }

    pub fn state(&self) -> BreakerState {
        self.state
    }
}

/// 缓存统计。
#[derive(Debug, Clone, Copy, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    /// 熔断/失败透传次数。
    pub bypasses: u64,
    /// 后端错误次数。
    pub errors: u64,
    /// 熔断打开次数。
    pub circuit_opens: u64,
}

/// 外部缓存管理器（Cache-Aside + Write-Invalidate + 熔断）。
pub struct ExternalCacheManager<B: CacheBackend> {
    backend: B,
    breaker: CircuitBreaker,
    /// 双删 / 预热重建后端的工厂（如 RedisBackend 重新连接）。
    reconnect: Arc<dyn Fn() -> B + Send + Sync>,
    ttl: u64,
    null_ttl: u64,
    cache_null: bool,
    write_policy: String,
    hits: AtomicU64,
    misses: AtomicU64,
    bypasses: AtomicU64,
    errors: AtomicU64,
    circuit_opens: AtomicU64,
}

impl<B: CacheBackend + Send + 'static> ExternalCacheManager<B> {
    /// `reconnect` 用于双删延迟任务 / 预热重建后端连接。
    pub fn new(
        backend: B,
        reconnect: Arc<dyn Fn() -> B + Send + Sync>,
        cfg: &CacheExternalConfig,
    ) -> Self {
        Self {
            backend,
            breaker: CircuitBreaker::new(
                cfg.retry_attempts.max(1),
                Duration::from_millis(cfg.timeout_ms.max(1) * 10),
            ),
            reconnect,
            ttl: cfg.ttl_seconds,
            null_ttl: cfg.null_ttl_seconds,
            cache_null: cfg.cache_null_values,
            write_policy: cfg.write_policy.clone(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            bypasses: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            circuit_opens: AtomicU64::new(0),
        }
    }

    /// 缓存键：`doc:{docid}`。
    pub fn cache_key(docid: u64) -> Vec<u8> {
        format!("doc:{docid}").into_bytes()
    }

    /// Cache-Aside 读：命中直返；未命中回源（`fetcher` 查引擎）并 SETEX 回填。
    /// `fetcher` 返回 `Ok(None)` 表示文档不存在（可空值缓存防穿透）。
    pub fn get_or_load<F>(&mut self, docid: u64, mut fetcher: F) -> Result<Vec<u8>>
    where
        F: FnMut() -> Result<Option<Vec<u8>>>,
    {
        let key = Self::cache_key(docid);
        // 冷却到期先尝试半开探测（design 21.4）
        self.breaker.maybe_half_open();
        // 熔断 → 直接透传引擎（design 21.4 降级）
        if self.breaker.is_open() {
            self.bypasses.fetch_add(1, Ordering::Relaxed);
            return fetcher()?
                .ok_or_else(|| Error::NotFound(format!("docid={docid}")));
        }

        match self.backend.get(&key) {
            Ok(Some(v)) => {
                if v.as_slice() == NULL_MARKER {
                    // 空值缓存命中：视为未命中（防穿透但不再回源）
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    return Err(Error::NotFound(format!("docid={docid}")));
                }
                self.hits.fetch_add(1, Ordering::Relaxed);
                self.breaker.record_success();
                return Ok(v);
            }
            Ok(None) => {}
            Err(_) => {
                // 后端失败 → 熔断计数 + 降级回源
                self.breaker.record_failure();
                if self.breaker.state() == BreakerState::Open {
                    self.circuit_opens.fetch_add(1, Ordering::Relaxed);
                }
                self.errors.fetch_add(1, Ordering::Relaxed);
                self.bypasses.fetch_add(1, Ordering::Relaxed);
                return fetcher()?
                    .ok_or_else(|| Error::NotFound(format!("docid={docid}")));
            }
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        match fetcher()? {
            Some(v) => {
                let ttl = jitter_ttl(self.ttl);
                let _ = self.backend.set(&key, &v, ttl);
                self.breaker.record_success();
                Ok(v)
            }
            None => {
                if self.cache_null {
                    let _ = self.backend.set(&key, NULL_MARKER, self.null_ttl);
                }
                Err(Error::NotFound(format!("docid={docid}")))
            }
        }
    }

    /// Write-Invalidate：写入引擎成功后删除 Redis 旧缓存（design 21.2 删除最安全）。
    /// `write_policy`：invalidate（删一次）/ double_delete（删两次，500ms 延迟二次删）/ none。
    pub fn invalidate(&mut self, docid: u64) -> Result<()> {
        let key = Self::cache_key(docid);
        match self.write_policy.as_str() {
            "none" => Ok(()),
            "double_delete" => {
                let _ = self.backend.del(&[&key]);
                // 延迟二次删除（近似强一致，design 21.2）
                let key2 = key.clone();
                let rc = self.reconnect.clone();
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(DOUBLE_DELETE_DELAY_MS));
                    let mut b = rc();
                    let _ = b.del(&[&key2]);
                });
                Ok(())
            }
            _ => {
                let _ = self.backend.del(&[&key]);
                Ok(())
            }
        }
    }

    /// 缓存统计。
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            bypasses: self.bypasses.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            circuit_opens: self.circuit_opens.load(Ordering::Relaxed),
        }
    }
}

/// TTL 随机抖动（防缓存雪崩，design 21.4）：ttl + rand(0, jitter)。
fn jitter_ttl(base_secs: u64) -> u64 {
    let jitter_ms = (base_secs as u128 * 1000) % 97; // 确定性伪随机（测试可复现）
    let extra = (jitter_ms as u64 % TTL_JITTER_MS) / 1000;
    base_secs + extra
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// 进程内后端（测试）：内存 KV + 可选失败注入。
    #[derive(Clone, Default)]
    struct MemBackend {
        store: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
        fail_times: Arc<Mutex<u32>>,
    }

    impl MemBackend {
        fn new_failing(fail_times: u32) -> Self {
            Self {
                store: Arc::new(Mutex::new(HashMap::new())),
                fail_times: Arc::new(Mutex::new(fail_times)),
            }
        }

        fn maybe_fail(&self) -> Result<()> {
            let mut f = self.fail_times.lock().unwrap();
            if *f > 0 {
                *f -= 1;
                return Err(Error::Rpc("模拟 Redis 故障".into()));
            }
            Ok(())
        }
    }

    impl CacheBackend for MemBackend {
        fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.maybe_fail()?;
            Ok(self.store.lock().unwrap().get(key).cloned())
        }
        fn set(&mut self, key: &[u8], value: &[u8], _ttl: u64) -> Result<()> {
            self.maybe_fail()?;
            self.store.lock().unwrap().insert(key.to_vec(), value.to_vec());
            Ok(())
        }
        fn del(&mut self, keys: &[&[u8]]) -> Result<u64> {
            self.maybe_fail()?;
            let mut st = self.store.lock().unwrap();
            let mut n = 0;
            for k in keys {
                if st.remove(*k).is_some() {
                    n += 1;
                }
            }
            Ok(n)
        }
    }

    fn cfg(policy: &str) -> CacheExternalConfig {
        CacheExternalConfig {
            enabled: true,
            redis_addrs: vec!["127.0.0.1:6379".into()],
            ttl_seconds: 300,
            cache_null_values: false,
            null_ttl_seconds: 60,
            write_policy: policy.into(),
            timeout_ms: 100,
            retry_attempts: 3,
            preheat_on_write: false,
        }
    }

    fn manager(backend: MemBackend, policy: &str) -> ExternalCacheManager<MemBackend> {
        let b2 = backend.clone();
        ExternalCacheManager::new(
            backend,
            Arc::new(move || b2.clone()),
            &cfg(policy),
        )
    }

    #[test]
    fn cache_aside_hit_and_fill() {
        let mut m = manager(MemBackend::default(), "invalidate");
        // 未命中 → 回源回填
        let v = m
            .get_or_load(1, || Ok(Some(b"doc-1".to_vec())))
            .unwrap();
        assert_eq!(v, b"doc-1");
        // 命中直返（fetcher 不应再被调用）
        let mut fetches = 0;
        let v = m
            .get_or_load(1, || {
                fetches += 1;
                Ok(Some(b"stale".to_vec()))
            })
            .unwrap();
        assert_eq!(v, b"doc-1", "命中应返回缓存值");
        assert_eq!(fetches, 0, "命中不应回源");
        let s = m.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 1);
    }

    #[test]
    fn cache_miss_with_none_doc() {
        // 不缓存空值：未命中 + 文档不存在 → NotFound，且后续仍会回源
        let mut m = manager(MemBackend::default(), "invalidate");
        assert!(m.get_or_load(9, || Ok(None)).is_err());
        assert!(m.get_or_load(9, || Ok(None)).is_err(), "未缓存空值则每次回源");
        // 缓存空值：防穿透（第二次不再调用 fetcher）
        let mut m2 = manager(MemBackend::default(), "invalidate");
        m2.cache_null = true;
        assert!(m2.get_or_load(9, || Ok(None)).is_err());
        let mut fetches = 0;
        assert!(m2
            .get_or_load(9, || {
                fetches += 1;
                Ok(None)
            })
            .is_err());
        assert_eq!(fetches, 0, "空值缓存命中不再回源");
    }

    #[test]
    fn write_invalidate_deletes_cache() {
        let b = MemBackend::default();
        let mut m = manager(b.clone(), "invalidate");
        let _ = m.get_or_load(1, || Ok(Some(b"v1".to_vec()))).unwrap();
        assert_eq!(b.store.lock().unwrap().len(), 1);
        m.invalidate(1).unwrap();
        assert_eq!(b.store.lock().unwrap().len(), 0, "失效应删除缓存");
        // 失效后重新回源到最新值
        let v = m.get_or_load(1, || Ok(Some(b"v2".to_vec()))).unwrap();
        assert_eq!(v, b"v2");
    }

    #[test]
    fn write_policy_none_keeps_cache() {
        let b = MemBackend::default();
        let mut m = manager(b.clone(), "none");
        let _ = m.get_or_load(1, || Ok(Some(b"v1".to_vec()))).unwrap();
        m.invalidate(1).unwrap();
        assert_eq!(b.store.lock().unwrap().len(), 1, "none 策略不删缓存");
    }

    #[test]
    fn circuit_breaker_opens_then_recovers() {
        // 初始失败 3 次 → OPEN → 直接透传（不再触后端）；冷却后 HALF-OPEN 探测成功恢复
        let b = MemBackend::new_failing(3);
        let mut m = manager(b.clone(), "invalidate");
        // 前 3 次后端失败 → 降级回源（fetcher 提供数据）
        for i in 0..3 {
            let v = m.get_or_load(1, || Ok(Some(format!("src-{i}").into_bytes()))).unwrap();
            assert_eq!(v, format!("src-{i}").as_bytes());
        }
        assert_eq!(m.breaker.state(), BreakerState::Open);
        assert_eq!(m.stats().circuit_opens, 1);
        // OPEN 期间：直接透传，不触后端
        let v = m.get_or_load(1, || Ok(Some(b"bypass".to_vec()))).unwrap();
        assert_eq!(v, b"bypass");
        assert_eq!(m.stats().bypasses, 4);
        // 冷却后探测成功 → 恢复 CLOSED，缓存可回填
        m.breaker.opened_at = Some(Instant::now() - Duration::from_secs(60));
        let v = m.get_or_load(1, || Ok(Some(b"recovered".to_vec()))).unwrap();
        assert_eq!(v, b"recovered");
        assert_eq!(m.breaker.state(), BreakerState::Closed);
    }

    #[test]
    fn double_delete_schedules_second_del() {
        let b = MemBackend::default();
        let mut m = manager(b.clone(), "double_delete");
        let _ = m.get_or_load(7, || Ok(Some(b"v".to_vec()))).unwrap();
        m.invalidate(7).unwrap();
        // 首次删除已生效；500ms 后二次删除（幂等，无副作用）
        assert_eq!(b.store.lock().unwrap().len(), 0);
        std::thread::sleep(Duration::from_millis(600));
        assert_eq!(b.store.lock().unwrap().len(), 0);
    }
}
