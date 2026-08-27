//! 布隆过滤器（design 4.4 / development 步骤 6）。
//!
//! - 每 SST 按预估 key 数分配位数组：`key 数 × 10 bits`，假阳性率 ≈ 1%；
//! - 哈希函数 7 个，由双哈希（FNV-1a 主哈希 + 增量派生）生成，避免构建开销；
//! - 用于等值查询"key 是否可能存在"，与 Zone Map 的范围剪枝互补。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// 每 key 分配的位（bit）数，对应约 1% 假阳性率（k=7 时）。
const BITS_PER_KEY: usize = 10;
/// 哈希函数个数。
const NUM_HASHES: usize = 7;

/// 布隆过滤器：位数组 + 元素计数。
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// 位数组（每个 u64 存 64 位）。
    bits: Vec<u64>,
    num_bits: usize,
    /// 已插入 key 数（用于估算与调试）。
    inserted: usize,
}

impl BloomFilter {
    /// 按预估 key 数分配位数组。
    pub fn with_estimated_keys(expected_keys: usize) -> Self {
        let num_bits = expected_keys.saturating_mul(BITS_PER_KEY).max(64);
        Self {
            bits: vec![0u64; num_bits.div_ceil(64)],
            num_bits,
            inserted: 0,
        }
    }

    pub fn insert<K: Hash>(&mut self, key: &K) {
        let (h1, h2) = double_hash(key);
        for i in 0..NUM_HASHES {
            let bit = (h1.wrapping_add(h2.wrapping_mul(i as u64))) % self.num_bits as u64;
            self.bits[bit as usize / 64] |= 1u64 << (bit % 64);
        }
        self.inserted += 1;
    }

    /// 等值查询：返回 false 表示**必定不存在**；true 表示**可能存在**。
    pub fn maybe_contains<K: Hash>(&self, key: &K) -> bool {
        let (h1, h2) = double_hash(key);
        for i in 0..NUM_HASHES {
            let bit = (h1.wrapping_add(h2.wrapping_mul(i as u64))) % self.num_bits as u64;
            if self.bits[bit as usize / 64] & (1u64 << (bit % 64)) == 0 {
                return false;
            }
        }
        true
    }

    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    pub fn inserted(&self) -> usize {
        self.inserted
    }

    /// 序列化为字节（位数组紧凑输出，供 SST Footer/文件段持久化）。
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.bits.len() * 8);
        out.extend_from_slice(&(self.num_bits as u64).to_le_bytes());
        out.extend_from_slice(&(self.inserted as u64).to_le_bytes());
        for w in &self.bits {
            out.extend_from_slice(&w.to_le_bytes());
        }
        out
    }

    /// 从 `to_bytes` 还原；非法输入返回 None。
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 16 {
            return None;
        }
        let num_bits = u64::from_le_bytes(bytes[0..8].try_into().ok()?) as usize;
        let inserted = u64::from_le_bytes(bytes[8..16].try_into().ok()?) as usize;
        let need = num_bits.div_ceil(64);
        if bytes.len() != 16 + need * 8 {
            return None;
        }
        let mut bits = vec![0u64; need];
        for (i, w) in bits.iter_mut().enumerate() {
            let off = 16 + i * 8;
            *w = u64::from_le_bytes(bytes[off..off + 8].try_into().ok()?);
        }
        Some(Self { bits, num_bits, inserted })
    }
}

/// 双哈希：h1 用于主索引，h2 用于增量派生，避免每次重新计算整个 hash。
fn double_hash<K: Hash>(key: &K) -> (u64, u64) {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    let h1 = h.finish();
    let mut h2 = DefaultHasher::new();
    // 加盐，确保 h1/h2 相互独立
    key.hash(&mut h2);
    h2.write_u64(0x9E37_79B9_7F4A_7C15);
    let h2 = h2.finish();
    (h1, h2.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_insert_and_query() {
        let mut b = BloomFilter::with_estimated_keys(100);
        b.insert(&b"hello".to_vec());
        b.insert(&b"world".to_vec());
        assert!(b.maybe_contains(&b"hello".to_vec()));
        assert!(b.maybe_contains(&b"world".to_vec()));
        assert_eq!(b.inserted(), 2);
    }

    #[test]
    fn absent_key_is_confidently_negative() {
        let mut b = BloomFilter::with_estimated_keys(1000);
        for i in 0..500u64 {
            b.insert(&format!("key-{i}"));
        }
        // 未插入的 key 大概率判负（假阳性率 ~1%，500 次断言中偶发冲突可忽略，用少量 key 降低抖动）
        let mut false_pos = 0;
        for i in 0..50u64 {
            if b.maybe_contains(&format!("absent-{i}")) {
                false_pos += 1;
            }
        }
        assert!(false_pos <= 3, "假阳性率异常偏高: {false_pos}/50");
    }

    #[test]
    fn bytes_roundtrip() {
        let mut b = BloomFilter::with_estimated_keys(100);
        b.insert(&b"a".to_vec());
        b.insert(&b"b".to_vec());
        let bytes = b.to_bytes();
        let r = BloomFilter::from_bytes(&bytes).unwrap();
        assert_eq!(r.num_bits(), b.num_bits());
        assert_eq!(r.inserted(), b.inserted());
        assert!(r.maybe_contains(&b"a".to_vec()));
        assert!(r.maybe_contains(&b"b".to_vec()));
        assert!(!r.maybe_contains(&b"zzz".to_vec()));
    }

    #[test]
    fn rejects_truncated_bytes() {
        let mut b = BloomFilter::with_estimated_keys(10);
        b.insert(&b"x".to_vec());
        let bytes = b.to_bytes();
        assert!(BloomFilter::from_bytes(&bytes[..bytes.len() - 4]).is_none());
        assert!(BloomFilter::from_bytes(&[]).is_none());
    }
}
