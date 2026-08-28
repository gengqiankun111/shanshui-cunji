//! Redis 冷热分层 SDK 门面（design 1.3 / 21.5，阶段 3）：`ShanshuiCunjiWithRedis`。
//!
//! 定位（design 1.3 第 2 条）：**Redis 管热、shanshui-cunji 管全量持久化**。门面把引擎与
//! 外部缓存管理器组合成单一入口，业务方一行代码获得冷热分层语义：
//!
//! - **读（读回填，Cache-Aside）**：`get`——命中 Redis 直返（0.1ms 级）；未命中回源引擎
//!   并 SETEX 回填（TTL 抖动防雪崩；熔断期间直接透传引擎，仅延迟上升不雪崩）；
//! - **写（写失效协调，Write-Invalidate）**：`put` / `delete`——**先落盘引擎返回 ACK**，
//!   成功后删除 Redis 旧缓存（删除最安全，design 21.2 红线；`double_delete` 500ms 二次删）。
//!
//! 分层：L1 Redis（分布式共享）→ L2 引擎进程内 HotCache → L3 磁盘；禁止从 Redis 恢复数据。

use std::sync::Arc;

use crate::config::CacheExternalConfig;
use crate::engine::Engine;
use crate::error::Result;
use crate::external_cache::{CacheBackend, CacheStats, ExternalCacheManager};

/// 冷热分层门面：引擎（全量持久化）+ Redis 外部缓存（热点加速）。
pub struct ShanshuiCunjiWithRedis<'a, B: CacheBackend> {
    engine: &'a mut Engine,
    cache: ExternalCacheManager<B>,
}

impl<'a, B: CacheBackend + Send + 'static> ShanshuiCunjiWithRedis<'a, B> {
    /// `reconnect` 用于双删延迟任务 / 预热重建后端连接（与 `ExternalCacheManager::new` 一致）。
    pub fn new(
        engine: &'a mut Engine,
        backend: B,
        reconnect: Arc<dyn Fn() -> B + Send + Sync>,
        cfg: &CacheExternalConfig,
    ) -> Self {
        Self {
            engine,
            cache: ExternalCacheManager::new(backend, reconnect, cfg),
        }
    }

    /// 读回填：命中 Redis 直返；未命中回源引擎并回填（Cache-Aside，design 21）。
    pub fn get(&mut self, docid: u64) -> Result<Vec<u8>> {
        let Self { engine, cache } = self;
        cache.get_or_load(docid, || engine.get(docid))
    }

    /// 写失效协调：先落盘引擎（返回 ACK 后）删除 Redis 旧缓存（Write-Invalidate，design 21.2）。
    pub fn put(&mut self, docid: u64, value: Vec<u8>, terms: &[&str]) -> Result<()> {
        let Self { engine, cache } = self;
        engine.put(docid, value, terms)?;
        cache.invalidate(docid)
    }

    /// 删除 + 失效：先删引擎文档，再删 Redis 缓存。
    pub fn delete(&mut self, docid: u64) -> Result<()> {
        let Self { engine, cache } = self;
        engine.delete(docid)?;
        cache.invalidate(docid)
    }

    /// 缓存统计（命中/回源/熔断旁路等）。
    pub fn cache_stats(&self) -> CacheStats {
        self.cache.stats()
    }

    /// 引擎内部统计（透传 admin status 数据源）。
    pub fn engine(&mut self) -> &mut Engine {
        self.engine
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use serde_json::json;

    /// 进程内测试后端。
    #[derive(Clone, Default)]
    struct MemBackend {
        store: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
    }

    impl CacheBackend for MemBackend {
        fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(self.store.lock().unwrap().get(key).cloned())
        }
        fn set(&mut self, key: &[u8], value: &[u8], _ttl: u64) -> Result<()> {
            self.store
                .lock()
                .unwrap()
                .insert(key.to_vec(), value.to_vec());
            Ok(())
        }
        fn del(&mut self, keys: &[&[u8]]) -> Result<u64> {
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

    fn facade<'a>(
        engine: &'a mut Engine,
        backend: MemBackend,
    ) -> ShanshuiCunjiWithRedis<'a, MemBackend> {
        let b2 = backend.clone();
        ShanshuiCunjiWithRedis::new(
            engine,
            backend,
            Arc::new(move || b2.clone()),
            &CacheExternalConfig::default(),
        )
    }

    fn put_doc(engine: &mut Engine, docid: u64, val: &serde_json::Value) {
        let bytes = serde_json::to_vec(val).unwrap();
        let terms = crate::server::extract_terms(val);
        let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        engine.put(docid, bytes, &t).unwrap();
    }

    #[test]
    fn get_fills_redis_on_miss_and_hits_later() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &crate::config::Config::default()).unwrap();
        put_doc(&mut e, 1, &json!({"docid": 1, "name": "alice"}));
        let b = MemBackend::default();
        let mut f = facade(&mut e, b.clone());

        // 未命中 → 回源并回填 Redis
        let v = f.get(1).unwrap();
        assert!(String::from_utf8_lossy(&v).contains("alice"));
        assert_eq!(b.store.lock().unwrap().len(), 1, "回填后 Redis 应有缓存");

        // 再读命中 Redis（引擎侧删库也不影响）
        f.engine().delete(1).unwrap();
        let v = f.get(1).unwrap();
        assert!(
            String::from_utf8_lossy(&v).contains("alice"),
            "命中 Redis 直返"
        );
        let s = f.cache_stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 1);
    }

    #[test]
    fn put_invalidates_then_refills_new_value() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &crate::config::Config::default()).unwrap();
        let b = MemBackend::default();
        let mut f = facade(&mut e, b.clone());
        let terms = ["name=alice"];

        f.put(1, br#"{"docid":1,"name":"alice"}"#.to_vec(), &terms).unwrap();
        assert_eq!(b.store.lock().unwrap().len(), 0, "写后应删旧缓存");
        let v = f.get(1).unwrap();
        assert_eq!(v, br#"{"docid":1,"name":"alice"}"#);
        assert_eq!(b.store.lock().unwrap().len(), 1, "读后回填 Redis");

        // 更新：先落盘引擎，再失效缓存 → 下次读回源到新值
        f.put(1, br#"{"docid":1,"name":"bob"}"#.to_vec(), &["name=bob"])
            .unwrap();
        assert_eq!(b.store.lock().unwrap().len(), 0, "写后应删除旧缓存");
        let v = f.get(1).unwrap();
        assert!(String::from_utf8_lossy(&v).contains("bob"), "读回源到新值");
    }

    #[test]
    fn delete_removes_engine_and_cache() {
        let dir = tempfile::tempdir().unwrap();
        let mut e = Engine::open(dir.path(), &crate::config::Config::default()).unwrap();
        let b = MemBackend::default();
        let mut f = facade(&mut e, b.clone());
        f.put(
            1,
            br#"{"docid":1,"name":"alice"}"#.to_vec(),
            &["name=alice"],
        )
        .unwrap();
        assert!(f.get(1).is_ok());
        assert_eq!(b.store.lock().unwrap().len(), 1);

        f.delete(1).unwrap();
        assert_eq!(b.store.lock().unwrap().len(), 0, "删除应失效缓存");
        assert!(f.get(1).is_err(), "引擎文档已删除，回源 NotFound");
    }
}
