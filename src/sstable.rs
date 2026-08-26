//! SSTable：磁盘有序文件（design 4.4 / development 步骤 6）。
//!
//! MVP 文件布局（列式 PAX 为阶段 1.5 增强，此处先按行块组织）：
//!
//! ```text
//! ┌────────────────────────────┐
//! │ File Header (magic+版本+配置) │
//! ├────────────────────────────┤
//! │ Data Block 0..N（独立压缩+CRC）│
//! ├────────────────────────────┤
//! │ Block Index（稀疏索引+Zone Map）│
//! │ Bloom Filter               │
//! ├────────────────────────────┤
//! │ Footer（各段偏移/长度/统计）    │
//! └────────────────────────────┘
//! ```
//!
//! 数据块内条目：`Key(VarLen) ++ Value(VarLen) ++ Seq(u64)`；
//! 块尾 Trailer：`RawLen(u32) ++ CompLen(u32) ++ CRC32(u32)`，损坏只影响单块。

use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use crate::bloom::BloomFilter;
use crate::error::{Error, Result};
use crate::keys::{decode_varlen, decode_varint, encode_varlen, encode_varint};

/// 文件魔数 + 版本。
pub const SST_MAGIC: &[u8; 8] = b"NVSSTL01";
pub const SST_VERSION: u16 = 1;

/// 压缩算法标识（与 config.sstable.compression 字符串对应）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Zstd,
    Lz4,
    Snappy,
}

impl Compression {
    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "none" => Ok(Self::None),
            "zstd" => Ok(Self::Zstd),
            "lz4" => Ok(Self::Lz4),
            "snappy" => Ok(Self::Snappy),
            other => Err(Error::Config(format!("sstable.compression 非法: {other}"))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Zstd => "zstd",
            Self::Lz4 => "lz4",
            Self::Snappy => "snappy",
        }
    }

    fn code(&self) -> u8 {
        match self {
            Self::None => 0,
            Self::Zstd => 1,
            Self::Lz4 => 2,
            Self::Snappy => 3,
        }
    }

    fn from_code(c: u8) -> Result<Self> {
        match c {
            0 => Ok(Self::None),
            1 => Ok(Self::Zstd),
            2 => Ok(Self::Lz4),
            3 => Ok(Self::Snappy),
            _ => Err(Error::Corrupted(format!("未知压缩算法码 {c}"))),
        }
    }
}

/// 数据块 Trailer（写在每个数据块末尾，固定 12 字节）。
const TRAILER_LEN: usize = 12;

/// SSTable Writer：按 key 升序写入，自动切块、压缩、维护稀疏索引与布隆。
pub struct SstWriter {
    out: std::fs::File,
    compression: Compression,
    block_size: usize,
    /// 当前数据块缓冲。
    buf: Vec<u8>,
    buf_keys: usize,
    /// 块索引：[first_key, offset, raw_len, comp_len]。
    index: Vec<IndexEntry>,
    /// 布隆过滤器。
    bloom: BloomFilter,
    /// 已写入字节数（含 header）。
    written: u64,
    /// 写入 key 总数。
    key_count: u64,
    /// 上一个写入的 key（校验升序）。
    last_key: Option<Vec<u8>>,
    /// zstd level 压缩参数。
    zstd_level: i32,
}

/// 稀疏索引条目（MVP 不含 Zone Map，阶段 1.5 追加统计字段）。
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub first_key: Vec<u8>,
    pub offset: u64,
    pub raw_len: u32,
    pub comp_len: u32,
}

impl SstWriter {
    pub fn new(path: &Path, compression: Compression, compression_level: i32, block_size: usize, expected_keys: usize) -> Result<Self> {
        let out = std::fs::File::create(path).map_err(|e| Error::Io(e))?;
        let mut w = Self {
            out,
            compression,
            block_size,
            buf: Vec::with_capacity(block_size),
            buf_keys: 0,
            index: Vec::new(),
            bloom: BloomFilter::with_estimated_keys(expected_keys),
            written: 0,
            key_count: 0,
            last_key: None,
            zstd_level: compression_level.max(1).min(22),
        };
        w.write_header()?;
        Ok(w)
    }

    fn write_header(&mut self) -> Result<()> {
        let mut h = Vec::with_capacity(32);
        h.extend_from_slice(SST_MAGIC);
        h.extend_from_slice(&SST_VERSION.to_le_bytes());
        h.push(self.compression.code());
        h.extend_from_slice(&(self.block_size as u32).to_le_bytes());
        self.write_all(&h)
    }

    /// 追加一条 (key, value, seq)。要求 key 升序。
    pub fn add(&mut self, key: &[u8], value: &[u8], seq: u64) -> Result<()> {
        if let Some(last) = &self.last_key {
            if key <= last.as_slice() {
                return Err(Error::Corrupted(format!(
                    "SST 写入 key 必须严格升序: {:?} <= {:?}",
                    key, last
                )));
            }
        }
        // 数据块格式：Key(VarLen) ++ Value(VarLen) ++ Seq(u64)
        encode_varlen(&mut self.buf, key);
        encode_varlen(&mut self.buf, value);
        self.buf.extend_from_slice(&seq.to_le_bytes());
        self.buf_keys += 1;
        self.bloom.insert(&key.to_vec());
        self.key_count += 1;
        self.last_key = Some(key.to_vec());

        if self.buf.len() >= self.block_size {
            self.flush_block()?;
        }
        Ok(())
    }

    /// 冲刷当前块（写盘 + 索引）。
    fn flush_block(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let offset = self.written;
        let first_key = self.parse_first_key()?;
        let raw_len = self.buf.len();
        let compressed = self.compress(&self.buf)?;
        let comp_len = compressed.len();

        self.write_all(&compressed)?;
        // Trailer
        let mut trailer = Vec::with_capacity(TRAILER_LEN);
        trailer.extend_from_slice(&(raw_len as u32).to_le_bytes());
        trailer.extend_from_slice(&(comp_len as u32).to_le_bytes());
        trailer.extend_from_slice(&crc32(&compressed).to_le_bytes());
        self.write_all(&trailer)?;

        self.index.push(IndexEntry {
            first_key,
            offset,
            raw_len: raw_len as u32,
            comp_len: comp_len as u32,
        });
        self.buf.clear();
        self.buf_keys = 0;
        Ok(())
    }

    /// 解析块缓冲首条 key（格式：VarLen(key) ++ key）。
    fn parse_first_key(&self) -> Result<Vec<u8>> {
        let mut cur = 0usize;
        Ok(decode_varlen(&self.buf, &mut cur)?.to_vec())
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.compression {
            Compression::None => Ok(data.to_vec()),
            Compression::Zstd => zstd::bulk::compress(data, self.zstd_level).map_err(|e| Error::Io(std::io::Error::other(format!("zstd 压缩失败: {e}")))),
            // MVP 先统一走 zstd；lz4/snappy 留待阶段 1.5 引入，此处映射到 zstd 以便配置兼容
            Compression::Lz4 | Compression::Snappy => zstd::bulk::compress(data, self.zstd_level).map_err(|e| Error::Io(std::io::Error::other(format!("压缩失败: {e}")))),
        }
    }

    fn write_all(&mut self, data: &[u8]) -> Result<()> {
        self.out.write_all(data).map_err(|e| Error::Io(e))?;
        self.written += data.len() as u64;
        Ok(())
    }

    /// 完成写入：冲刷末块、写索引、写布隆、写 Footer、fsync。
    pub fn finish(mut self) -> Result<SstFooter> {
        self.flush_block()?;

        // Block Index：块数(varint) + 每条目(VarLen(first_key) + offset u64 + raw_len u32 + comp_len u32)
        let index_offset = self.written;
        let mut ib = Vec::new();
        encode_varint(&mut ib, self.index.len() as u64);
        for e in &self.index {
            encode_varlen(&mut ib, &e.first_key);
            ib.extend_from_slice(&e.offset.to_le_bytes());
            ib.extend_from_slice(&e.raw_len.to_le_bytes());
            ib.extend_from_slice(&e.comp_len.to_le_bytes());
        }
        self.write_all(&ib)?;
        let index_len = ib.len() as u32;

        // Bloom
        let bloom_offset = self.written;
        let bloom_bytes = self.bloom.to_bytes();
        self.write_all(&(bloom_bytes.len() as u32).to_le_bytes())?;
        self.write_all(&bloom_bytes)?;
        let bloom_len = (4 + bloom_bytes.len()) as u32;

        // Footer：magic + 各段偏移/长度 + key_count + 版本
        let footer_offset = self.written;
        let mut fb = Vec::with_capacity(64);
        fb.extend_from_slice(SST_MAGIC);
        fb.extend_from_slice(&SST_VERSION.to_le_bytes());
        fb.extend_from_slice(&index_offset.to_le_bytes());
        fb.extend_from_slice(&index_len.to_le_bytes());
        fb.extend_from_slice(&bloom_offset.to_le_bytes());
        fb.extend_from_slice(&bloom_len.to_le_bytes());
        fb.extend_from_slice(&self.key_count.to_le_bytes());
        fb.extend_from_slice(&footer_offset.to_le_bytes());
        fb.extend_from_slice(&crc32(&fb).to_le_bytes());
        self.write_all(&fb)?;

        // 文件尾 8 字节指针：定位 Footer 起始偏移（Reader 先读此指针）
        self.write_all(&footer_offset.to_le_bytes())?;

        self.out.sync_all().map_err(|e| Error::Io(e))?;
        Ok(SstFooter {
            index_offset,
            index_len: index_len as usize,
            bloom_offset,
            bloom_len: bloom_len as usize,
            key_count: self.key_count,
            footer_offset,
        })
    }
}

/// SST Footer 摘要（Reader 使用）。
#[derive(Debug, Clone)]
pub struct SstFooter {
    pub index_offset: u64,
    pub index_len: usize,
    pub bloom_offset: u64,
    pub bloom_len: usize,
    pub key_count: u64,
    pub footer_offset: u64,
}

/// CRC32（IEEE 多项式，与 WAL 一致）。
pub fn crc32(data: &[u8]) -> u32 {
    crate::wal::crc32(data)
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// SSTable Reader：mmap 式顺序读取（MVP 用文件 read + seek；阶段 3 可换 io_uring）。
pub struct SstReader {
    path: PathBuf,
    file: std::fs::File,
    footer: SstFooter,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    compression: Compression,
}

impl SstReader {
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = std::fs::File::open(path).map_err(|e| Error::Io(e))?;
        let fsize = file.metadata().map_err(|e| Error::Io(e))?.len();
        if fsize < 8 + 54 {
            return Err(Error::Corrupted("SST 文件过小".into()));
        }
        // 文件尾 8 字节：Footer 起始偏移指针
        let mut ptr = [0u8; 8];
        file.seek(std::io::SeekFrom::End(-8)).map_err(|e| Error::Io(e))?;
        file.read_exact(&mut ptr).map_err(|e| Error::Io(e))?;
        let footer_offset = u64::from_le_bytes(ptr);

        // 读 Footer 主体（固定 54 字节）
        let mut fb = vec![0u8; 54];
        file.seek(std::io::SeekFrom::Start(footer_offset)).map_err(|e| Error::Io(e))?;
        file.read_exact(&mut fb).map_err(|e| Error::Io(e))?;

        if &fb[0..8] != SST_MAGIC {
            return Err(Error::Corrupted("SST Footer 魔数错误".into()));
        }
        let version = u16::from_le_bytes([fb[8], fb[9]]);
        if version != SST_VERSION {
            return Err(Error::Corrupted(format!("SST 版本不支持: {version}")));
        }
        let index_offset = u64::from_le_bytes(fb[10..18].try_into().unwrap());
        let index_len = u32::from_le_bytes(fb[18..22].try_into().unwrap()) as usize;
        let bloom_offset = u64::from_le_bytes(fb[22..30].try_into().unwrap());
        let bloom_len = u32::from_le_bytes(fb[30..34].try_into().unwrap()) as usize;
        let key_count = u64::from_le_bytes(fb[34..42].try_into().unwrap());
        let fb_footer_offset = u64::from_le_bytes(fb[42..50].try_into().unwrap());
        if fb_footer_offset != footer_offset {
            return Err(Error::Corrupted("SST Footer 偏移不一致".into()));
        }

        // 校验 Footer CRC（覆盖前 50 字节）
        let expected = u32::from_le_bytes(fb[50..54].try_into().unwrap());
        let actual = crc32(&fb[..50]);
        if expected != actual {
            return Err(Error::Corrupted("SST Footer CRC 校验失败".into()));
        }

        // 读 Block Index
        let mut ib = vec![0u8; index_len];
        file.seek(std::io::SeekFrom::Start(index_offset)).map_err(|e| Error::Io(e))?;
        file.read_exact(&mut ib).map_err(|e| Error::Io(e))?;
        let index = decode_index(&ib)?;

        // 读 Bloom
        let mut bb = vec![0u8; bloom_len];
        file.seek(std::io::SeekFrom::Start(bloom_offset)).map_err(|e| Error::Io(e))?;
        file.read_exact(&mut bb).map_err(|e| Error::Io(e))?;
        let bloom_len_u32 = u32::from_le_bytes(bb[0..4].try_into().unwrap()) as usize;
        if 4 + bloom_len_u32 != bb.len() {
            return Err(Error::Corrupted("Bloom 长度不一致".into()));
        }
        let bloom = BloomFilter::from_bytes(&bb[4..])
            .ok_or_else(|| Error::Corrupted("Bloom 解析失败".into()))?;

        // 读取 Header 获取压缩与块大小
        let mut hb = vec![0u8; 15];
        file.seek(std::io::SeekFrom::Start(0)).map_err(|e| Error::Io(e))?;
        file.read_exact(&mut hb).map_err(|e| Error::Io(e))?;
        if &hb[0..8] != SST_MAGIC {
            return Err(Error::Corrupted("SST Header 魔数错误".into()));
        }
        let compression = Compression::from_code(hb[10])?;
        let _block_size = u32::from_le_bytes(hb[11..15].try_into().unwrap()) as usize;

        Ok(Self {
            path: path.to_path_buf(),
            file,
            footer: SstFooter {
                index_offset,
                index_len,
                bloom_offset,
                bloom_len,
                key_count,
                footer_offset,
            },
            index,
            bloom,
            compression,
        })
    }

    pub fn footer(&self) -> &SstFooter {
        &self.footer
    }

    pub fn bloom(&self) -> &BloomFilter {
        &self.bloom
    }

    pub fn index(&self) -> &[IndexEntry] {
        &self.index
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn index_len(&self) -> usize {
        self.index.len()
    }

    /// 等值查询：布隆剪枝 → 二分定位块 → 读块 → 块内扫描。
    pub fn get(&mut self, key: &[u8]) -> Result<Option<(Vec<u8>, u64)>> {
        if !self.bloom.maybe_contains(&key.to_vec()) {
            return Ok(None);
        }
        let Some(e) = self.locate_block(key).cloned() else {
            return Ok(None);
        };
        let data = self.read_block(&e)?;
        Ok(scan_block_for_key(&data, key))
    }

    /// 定位包含 key 的数据块（二分首个 first_key <= key 的块）。
    fn locate_block(&self, key: &[u8]) -> Option<&IndexEntry> {
        let mut lo = 0usize;
        let mut hi = self.index.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.index[mid].first_key.as_slice() <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            None
        } else {
            self.index.get(lo - 1)
        }
    }

    /// 读取并解压数据块，校验 CRC。
    pub fn read_block(&mut self, e: &IndexEntry) -> Result<Vec<u8>> {
        let mut comp = vec![0u8; e.comp_len as usize];
        self.file.seek(std::io::SeekFrom::Start(e.offset)).map_err(|e| Error::Io(e))?;
        self.file.read_exact(&mut comp).map_err(|e| Error::Io(e))?;

        // 读 Trailer 校验
        let mut trailer = vec![0u8; TRAILER_LEN];
        self.file.read_exact(&mut trailer).map_err(|e| Error::Io(e))?;
        let raw_len = u32::from_le_bytes(trailer[0..4].try_into().unwrap()) as usize;
        let comp_len = u32::from_le_bytes(trailer[4..8].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(trailer[8..12].try_into().unwrap());
        if comp_len != comp.len() {
            return Err(Error::Corrupted("块长度不一致".into()));
        }
        if crc32(&comp) != crc {
            return Err(Error::Corrupted("数据块 CRC 校验失败".into()));
        }
        self.decompress(&comp, raw_len)
    }

    fn decompress(&self, data: &[u8], raw_len: usize) -> Result<Vec<u8>> {
        match self.compression {
            Compression::None => Ok(data.to_vec()),
            Compression::Zstd | Compression::Lz4 | Compression::Snappy => {
                zstd::bulk::decompress(data, raw_len.max(64)).map_err(|e| Error::Io(std::io::Error::other(format!("解压失败: {e}"))))
            }
        }
    }

    /// 迭代：按块顺序扫描全部 (key, seq)。MVP 不提供跨块游标，先提供全量。
    pub fn iterate<F: FnMut(&[u8], u64)>(&mut self, mut f: F) -> Result<()> {
        let entries: Vec<IndexEntry> = self.index.clone();
        for e in entries {
            let data = self.read_block(&e)?;
            let mut cur = 0usize;
            while cur < data.len() {
                let key = decode_varlen(&data, &mut cur)?;
                let _value = decode_varlen(&data, &mut cur)?;
                if cur + 8 > data.len() {
                    return Err(Error::Corrupted("迭代 seq 越界".into()));
                }
                let seq = u64::from_le_bytes(data[cur..cur + 8].try_into().unwrap());
                f(key, seq);
                cur += 8;
            }
        }
        Ok(())
    }
}

/// 解码块索引字节流。
fn decode_index(ib: &[u8]) -> Result<Vec<IndexEntry>> {
    let mut cur = 0usize;
    let count = decode_varint(ib, &mut cur)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let first_key = decode_varlen(ib, &mut cur)?.to_vec();
        if cur + 16 > ib.len() {
            return Err(Error::Corrupted("索引条目越界".into()));
        }
        let offset = u64::from_le_bytes(ib[cur..cur + 8].try_into().unwrap());
        let raw_len = u32::from_le_bytes(ib[cur + 8..cur + 12].try_into().unwrap());
        let comp_len = u32::from_le_bytes(ib[cur + 12..cur + 16].try_into().unwrap());
        cur += 16;
        out.push(IndexEntry { first_key, offset, raw_len, comp_len });
    }
    Ok(out)
}

/// 块内顺序扫描等值 key；命中返回 (value, seq)。
fn scan_block_for_key(data: &[u8], key: &[u8]) -> Option<(Vec<u8>, u64)> {
    let mut cur = 0usize;
    while cur < data.len() {
        let k = decode_varlen(data, &mut cur).ok()?;
        let v = decode_varlen(data, &mut cur).ok()?;
        if cur + 8 > data.len() {
            return None;
        }
        let seq = u64::from_le_bytes(data[cur..cur + 8].try_into().ok()?);
        if k == key {
            return Some((v.to_vec(), seq));
        }
        cur += 8;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> std::path::PathBuf {
        static DIR: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let name = format!("sst-{}.sst", SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap()).path().join(name)
    }

    fn write_sample(path: &Path, n: u64) -> SstFooter {
        let mut w = SstWriter::new(path, Compression::Zstd, 3, 4096, n as usize).unwrap();
        for i in 0..n {
            let k = format!("user-{i:08}").into_bytes();
            let v = format!("value-of-{i}").into_bytes();
            w.add(&k, &v, i).unwrap();
        }
        w.finish().unwrap()
    }

    #[test]
    fn write_read_roundtrip() {
        let path = tmp();
        let footer = write_sample(&path, 100);
        assert_eq!(footer.key_count, 100);
        let mut r = SstReader::open(&path).unwrap();
        assert_eq!(r.footer().key_count, 100);
        assert!(r.index_len() >= 1);
        for i in (0..100u64).step_by(7) {
            let k = format!("user-{i:08}").into_bytes();
            let (v, seq) = r.get(&k).unwrap().unwrap();
            assert_eq!(v, format!("value-of-{i}").into_bytes());
            assert_eq!(seq, i);
        }
    }

    #[test]
    fn bloom_prunes_absent_keys() {
        let path = tmp();
        write_sample(&path, 50);
        let mut r = SstReader::open(&path).unwrap();
        assert!(r.get(b"nope-not-here").unwrap().is_none());
    }

    #[test]
    fn non_strict_order_rejected() {
        let path = tmp();
        let mut w = SstWriter::new(&path, Compression::None, 0, 4096, 10).unwrap();
        w.add(b"b", b"1", 1).unwrap();
        assert!(w.add(b"a", b"2", 2).is_err());
    }

    #[test]
    fn iterate_visits_all_sorted() {
        let path = tmp();
        write_sample(&path, 1000);
        let mut r = SstReader::open(&path).unwrap();
        let mut last: Option<Vec<u8>> = None;
        let mut count = 0u64;
        r.iterate(|k, _| {
            if let Some(l) = &last {
                assert!(k > l.as_slice());
            }
            last = Some(k.to_vec());
            count += 1;
        }).unwrap();
        assert_eq!(count, 1000);
    }

    #[test]
    fn multi_block_file_queries_across_blocks() {
        // 小块大小强制多块，验证跨块二分定位
        let path = tmp();
        let mut w = SstWriter::new(&path, Compression::Zstd, 1, 64, 200).unwrap();
        for i in 0..200u64 {
            let k = format!("k{i:04}").into_bytes();
            let v = format!("v{i:04}").into_bytes();
            w.add(&k, &v, i).unwrap();
        }
        w.finish().unwrap();
        let mut r = SstReader::open(&path).unwrap();
        assert!(r.index_len() > 1, "应产生多块，实际 {}", r.index_len());
        // 首块、中间块、末块的 key 都要能查到
        for i in [0u64, 99, 199] {
            let k = format!("k{i:04}").into_bytes();
            assert!(r.get(&k).unwrap().is_some(), "key k{i:04} 未命中");
        }
    }

    #[test]
    fn corrupted_block_detected() {
        let path = tmp();
        write_sample(&path, 30);
        // 翻转第一个数据块首字节（Header 15 字节之后），破坏 CRC
        let mut data = std::fs::read(&path).unwrap();
        data[15] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();
        let mut r = SstReader::open(&path).unwrap();
        // 块 CRC 校验失败 → Corrupted 错误（而非 panic）
        assert!(r.get(b"user-00000001").is_err());
    }
}
