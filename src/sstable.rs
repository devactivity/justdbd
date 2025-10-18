use crate::{
    Entry, Key, Result, StorageError, Timestamp,
    bloom::BloomFilter,
    config::CompressionType,
    serialization::{Deserializer, Serializer},
};

use bytes::Bytes;
use crc32fast::Hasher;
use memmap2::MmapOptions;
use parking_lot::RwLock;

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

// magic number for sstable files
const SSTABLE_MAGIC: u32 = 0x53533454; // "SSDT"

// sstable format version
const SSTABLE_VERSION: u32 = 1;

// block size for data compression
const BLOCK_SIZE: usize = 4096;

// footer size in bytes
const FOOTER_SIZE: usize = 64;

#[derive(Debug, Clone)]
pub struct SStableMeta {
    pub entry_count: u64,
    pub data_size: u64,
    pub index_size: u64,
    pub bloom_offset: u64,
    pub bloom_size: u64,
    pub min_key: Key,
    pub max_key: Key,
    pub compression: CompressionType,
    pub created_at: Timestamp,
    pub version: u32,
}

#[derive(Debug, Clone)]
struct IndexEntry {
    key: Key,
    offset: u64,
    size: u64,
}

pub struct SSTableReader {
    path: PathBuf,
    mmap: Option<memmap2::Mmap>,
    file: Option<File>,
    meta: SStableMeta,
    index: Vec<IndexEntry>,
    bloom_filter: Option<BloomFilter>,
    block_cache: Arc<RwLock<HashMap<u64, Bytes>>>,
}

impl SSTableReader {
    // open an sstable file for reading
    pub fn open<P: AsRef<Path>>(path: P, use_mmap: bool) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;

        let (mmap, file_ref) = if use_mmap {
            let mmap = unsafe { MmapOptions::new().map(&file)? };
            (Some(mmap), None)
        } else {
            (None, Some(file))
        };

        let mut reader = Self {
            path,
            mmap,
            file: file_ref,
            meta: SStableMeta {
                entry_count: 0,
                data_size: 0,
                index_size: 0,
                bloom_offset: 0,
                bloom_size: 0,
                min_key: Vec::new(),
                max_key: Vec::new(),
                compression: CompressionType::None,
                created_at: 0,
                version: 0,
            },
            index: Vec::new(),
            bloom_filter: None,
            block_cache: Arc::new(RwLock::new(HashMap::new())),
        };

        reader.load_metadata()?;
        reader.load_index()?;
        reader.load_bloom_filter()?;

        Ok(reader)
    }

    fn load_metadata(&mut self) -> Result<()> {
        let file_size = self.file_size()?;

        if file_size < FOOTER_SIZE as u64 {
            return Err(StorageError::corruption(
                "
                File too small to contain footer
                ",
            ));
        }

        let footer_data = self.read_at(file_size - FOOTER_SIZE as u64, FOOTER_SIZE)?;

        let mut deserializer = Deserializer::new(&footer_data);

        // read metadata
        self.meta.entry_count = deserializer.read_u64()?;
        self.meta.data_size = deserializer.read_u64()?;
        self.meta.index_size = deserializer.read_u64()?;
        self.meta.bloom_offset = deserializer.read_u64()?;
        self.meta.bloom_size = deserializer.read_u64()?;
        self.meta.created_at = deserializer.read_u64()?;

        // read compression type
        let compression_byte = deserializer.read_u8()?;
        self.meta.compression = match compression_byte {
            0 => CompressionType::None,
            1 => CompressionType::Lz4,
            2 => CompressionType::Zstd,
            _ => return Err(StorageError::corruption("invalid compression type")),
        };

        // read version
        self.meta.version = deserializer.read_u32()?;

        // read magic number
        let magic = deserializer.read_u32()?;

        if magic != SSTABLE_MAGIC {
            return Err(StorageError::corruption("invalid sstable magic number"));
        }

        // validate version
        if self.meta.version != SSTABLE_VERSION {
            return Err(StorageError::corruption("unsupported sstable version"));
        }

        Ok(())
    }

    fn load_index(&mut self) -> Result<()> {
        let index_offset = self.meta.bloom_offset + self.meta.bloom_size;

        let index_data = self.read_at(index_offset, self.meta.index_size as usize)?;

        let mut deserializer = Deserializer::new(&index_data);
        let entry_count = deserializer.read_u32()? as usize;

        self.index.reserve(entry_count);

        for i in 0..entry_count {
            let key_len = deserializer.read_u32()? as usize;
            let key = deserializer.read_bytes(key_len)?;
            let offset = deserializer.read_u64()?;
            let size = deserializer.read_u64()?;

            self.index.push(IndexEntry {
                key: key.to_vec(),
                offset,
                size,
            });
        }

        //set min/max key from index
        if !self.index.is_empty() {
            self.meta.min_key = self.index.first().unwrap().key.clone();
            self.meta.max_key = self.index.last().unwrap().key.clone();
        }
        Ok(())
    }

    fn load_bloom_filter(&mut self) -> Result<()> {
        if self.meta.bloom_size == 0 {
            return Ok(());
        }

        let bloom_data = self.read_at(self.meta.bloom_offset, self.meta.bloom_size as usize)?;
        self.bloom_filter = Some(BloomFilter::from_bytes(&bloom_data)?);

        Ok(())
    }

    fn file_size(&self) -> Result<u64> {
        if let Some(mmap) = &self.mmap {
            Ok(mmap.len() as u64)
        } else if let Some(file) = &self.file {
            Ok(file.metadata()?.len())
        } else {
            Err(StorageError::corruption("no file handle available"))
        }
    }

    fn read_at(&self, offset: u64, size: usize) -> Result<Bytes> {
        if let Some(mmap) = &self.mmap {
            let start = offset as usize;
            let end = start + size;

            if end > mmap.len() {
                return Err(StorageError::corruption("read beyond file bounds"));
            }
            Ok(Bytes::copy_from_slice(&mmap[start..end]))
        } else if let Some(_) = &self.file {
            // For non-mmap, we need to reopen the file for each read to avoid borrowing issues
            let mut file = File::open(&self.path)?;
            file.seek(SeekFrom::Start(offset))?;
            let mut buffer = vec![0u8; size];
            file.read_exact(&mut buffer)?;
            Ok(Bytes::from(buffer))
        } else {
            return Err(StorageError::corruption("no file handle available"));
        }
    }

    // get an entry by key
    pub fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        if let Some(bloom) = &self.bloom_filter {
            if !bloom.might_contain(key) {
                return Ok(None);
            }
        }

        let block_index = self.find_block_for_key(key);
        if block_index.is_none() {
            return Ok(None);
        }

        let block_index = block_index.unwrap();
        let index_entry = &self.index[block_index];

        let block_data = self.read_block(index_entry)?;

        // search for the key
        self.search_in_block(&block_data, key)
    }

    fn find_block_for_key(&self, key: &[u8]) -> Option<usize> {
        let mut left = 0;
        let mut right = self.index.len();

        while left < right {
            let mid = left + (right - left) / 2;
            let index_entry = &self.index[mid];

            if key < index_entry.key.as_slice() {
                right = mid;
            } else {
                left = mid + 1;
            }
        }

        if left > 0 {
            Some(left - 1)
        } else if !self.index.is_empty() && key >= self.index[0].key.as_slice() {
            Some(0)
        } else {
            None
        }
    }

    fn read_block(&self, index_entry: &IndexEntry) -> Result<Bytes> {
        // check cache first
        {
            let cache = self.block_cache.read();
            if let Some(cached_block) = cache.get(&index_entry.offset) {
                return Ok(cached_block.clone());
            }
        }

        // read the block
        let block_with_checksum = self.read_at(index_entry.offset, index_entry.size as usize)?;

        if block_with_checksum.len() < 4 {
            return Err(StorageError::corruption(
                "
                    block too small to contain checksum
                ",
            ));
        }

        let checksum_bytes = &block_with_checksum[0..4];
        let compressed_data = &block_with_checksum[4..];

        let stored_checksum = u32::from_le_bytes([
            checksum_bytes[0],
            checksum_bytes[1],
            checksum_bytes[2],
            checksum_bytes[3],
        ]);

        // verify checksum
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(compressed_data);
        let computed_checksum = hasher.finalize();

        if stored_checksum != computed_checksum {
            return Err(StorageError::ChecksumMismatch {
                expected: stored_checksum,
                actual: computed_checksum,
            });
        }

        // decompress
        let decompress_data = match self.meta.compression {
            CompressionType::None => Bytes::from(compressed_data.to_vec()),
            CompressionType::Lz4 => {
                #[cfg(feature = "compression")]
                {
                    let decompressed = lz4_flex::decompress_size_prepended(&compressed_data)
                        .map_err(|e| {
                            StorageError::decompression(format!("LZ4 decompression failed: {}", e))
                        })?;
                    Bytes::from(decompressed)
                }
                #[cfg(not(feature = "compression"))]
                {
                    return Err(StorageError::decompression("LZ4 compression not enabled"));
                }
            }
            CompressionType::Zstd => {
                #[cfg(feature = "compression")]
                {
                    let decompressed = zstd::bulk::decompress(&compressed_data, BLOCK_SIZE * 4)
                        .map_err(|e| {
                            StorageError::decompression(format!("Zstd decompression failed: {}", e))
                        })?;
                    Bytes::from(decompressed)
                }
                #[cfg(not(feature = "compression"))]
                {
                    return Err(StorageError::decompression("Zstd compression not enabled"));
                }
            }
        };

        // cache the decompressed block
        {
            let mut cache = self.block_cache.write();
            cache.insert(index_entry.offset, decompress_data.clone());

            // limit cache size
            if cache.len() > 100 {
                cache.clear();
            }
        }

        Ok(decompress_data)
    }

    // search for a key
    fn search_in_block(&self, block_data: &[u8], target_key: &[u8]) -> Result<Option<Entry>> {
        let mut deserializer = Deserializer::new(block_data);
        let entry_count = deserializer.read_u32()? as usize;

        for i in 0..entry_count {
            let key_len = deserializer.read_u32()? as usize;
            let key = deserializer.read_bytes(key_len)?;

            if key == target_key {
                let has_value = deserializer.read_u8()? != 0;
                let value = if has_value {
                    let value_len = deserializer.read_u32()? as usize;
                    Some(deserializer.read_bytes(value_len)?.to_vec())
                } else {
                    None
                };

                let timestamp = deserializer.read_u64()?;
                let sequence = deserializer.read_u64()?;

                return Ok(Some(Entry::new(key.to_vec(), value, timestamp, sequence)));
            } else {
                let has_value = deserializer.read_u8()? != 0;
                if has_value {
                    let value_len = deserializer.read_u32()? as usize;
                    deserializer.skip(value_len)?;
                }

                deserializer.skip(16)?; // timestamp + sequence
            }
        }

        Ok(None)
    }

    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<Entry>> {
        let mut results = Vec::new();

        let start_block = self.find_block_for_key(start).unwrap_or(0);

        for i in start_block..end.len() {
            let index_entry = &self.index[i];

            if index_entry.key.as_slice() >= end {
                break;
            }

            // read the block and collect
            // matching entries
            let block_data = self.read_block(index_entry)?;
            let block_entries = self.get_entries_in_range_from_block(&block_data, start, end)?;
            results.extend(block_entries);
        }

        Ok(results)
    }

    fn get_entries_in_range_from_block(
        &self,
        block_data: &[u8],
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<Entry>> {
        let mut results = Vec::new();
        let mut deserializer = Deserializer::new(block_data);
        let entry_count = deserializer.read_u32()? as usize;

        for _ in 0..entry_count {
            let key_len = deserializer.read_u32()? as usize;
            let key = deserializer.read_bytes(key_len)?;

            // check if key is in range
            if key >= start && key < end {
                let has_value = deserializer.read_u8()? != 0;
                let value = if has_value {
                    let value_len = deserializer.read_u32()? as usize;
                    Some(deserializer.read_bytes(value_len)?.to_vec())
                } else {
                    None
                };

                let timestamp = deserializer.read_u64()?;
                let sequence = deserializer.read_u64()?;

                results.push(Entry::new(key.to_vec(), value, timestamp, sequence));
            } else {
                let has_value = deserializer.read_u8()? != 0;

                if has_value {
                    let value_len = deserializer.read_u32()? as usize;
                    deserializer.skip(value_len)?;
                }
                deserializer.skip(16)?; // timestamp + sequence
            }
        }

        Ok(results)
    }

    pub fn metadata(&self) -> &SStableMeta {
        &self.meta
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    // check if a key might exist
    pub fn might_contain(&self, key: &[u8]) -> bool {
        if let Some(bloom) = &self.bloom_filter {
            bloom.might_contain(key)
        } else {
            true
        }
    }

    // get an iterator over all entries
    pub fn iter(&self) -> SSTableIterator {
        SSTableIterator::new(self)
    }
}

pub struct SSTableIterator<'a> {
    reader: &'a SSTableReader,
    current_block: usize,
    current_entry: usize,
    current_block_data: Option<Bytes>,
    current_block_entries: Vec<Entry>,
}

impl<'a> SSTableIterator<'a> {
    fn new(reader: &'a SSTableReader) -> Self {
        Self {
            reader,
            current_block: 0,
            current_entry: 0,
            current_block_data: None,
            current_block_entries: Vec::new(),
        }
    }

    fn load_current_block(&mut self) -> Result<()> {
        if self.current_block >= self.reader.index.len() {
            return Ok(());
        }

        let index_entry = &self.reader.index[self.current_block];
        let block_data = self.reader.read_block(index_entry)?;

        // parse
        let mut deserializer = Deserializer::new(&block_data);
        let entry_count = deserializer.read_u32()? as usize;
        let mut entries = Vec::with_capacity(entry_count);

        for _ in 0..entry_count {
            let key_len = deserializer.read_u32()? as usize;
            let key = deserializer.read_bytes(key_len)?.to_vec();

            let has_value = deserializer.read_u8()? != 0;
            let value = if has_value {
                let value_len = deserializer.read_u32()? as usize;
                Some(deserializer.read_bytes(value_len)?.to_vec())
            } else {
                None
            };

            let timestamp = deserializer.read_u64()?;
            let sequence = deserializer.read_u64()?;

            entries.push(Entry::new(key.to_vec(), value, timestamp, sequence));
        }

        self.current_block_data = Some(block_data);
        self.current_block_entries = entries;
        self.current_entry = 0;

        Ok(())
    }
}

impl<'a> Iterator for SSTableIterator<'a> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_block_data.is_none() {
                if let Err(e) = self.load_current_block() {
                    return Some(Err(e));
                }
            }

            if self.current_entry < self.current_block_entries.len() {
                let entry = self.current_block_entries[self.current_entry].clone();
                self.current_entry += 1;
                return Some(Ok(entry));
            }

            self.current_block += 1;
            self.current_block_data = None;
            self.current_block_entries.clear();

            if self.current_block >= self.reader.index.len() {
                return None;
            }
        }
    }
}

pub struct SSTableWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    position: u64,
    current_block: Vec<u8>,
    current_block_entries: usize,
    index: Vec<IndexEntry>,
    bloom_filter: BloomFilter,
    compression: CompressionType,
    compression_level: i32,
    entry_count: u64,
    min_key: Option<Key>,
    max_key: Option<Key>,
    created_at: Timestamp,
}

impl SSTableWriter {
    pub fn new<P: AsRef<Path>>(
        path: P,
        expected_entries: usize,
        compression: CompressionType,
        compression_level: i32,
        bloom_false_positive_rate: f64,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        let writer = BufWriter::new(file);
        let bloom_filter = BloomFilter::new(expected_entries, bloom_false_positive_rate);

        Ok(Self {
            path,
            writer,
            position: 0,
            current_block: Vec::with_capacity(BLOCK_SIZE),
            current_block_entries: 0,
            index: Vec::new(),
            bloom_filter,
            compression,
            compression_level,
            entry_count: 0,
            min_key: None,
            max_key: None,
            created_at: Self::current_timestamp(),
        })
    }

    fn current_timestamp() -> Timestamp {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as Timestamp
    }

    pub fn add(&mut self, entry: &Entry) -> Result<()> {
        if let Some(ref max_key) = self.max_key {
            if entry.key <= *max_key {
                return Err(StorageError::invalid_operation(
                    "keys must be added in sorted order",
                ));
            }
        }

        // update min/max keys
        if self.min_key.is_none() {
            self.min_key = Some(entry.key.clone());
        }

        self.max_key = Some(entry.key.clone());

        // add to bloom filter
        self.bloom_filter.add(&entry.key);

        // serialize
        let mut serializer = Serializer::new();
        self.serializer_entry(&mut serializer, entry)?;
        let entry_data = serializer.finish();

        // check
        if self.current_block.len() + entry_data.len() > BLOCK_SIZE
            && self.current_block_entries > 0
        {
            self.flush_current_block()?;
        }

        // if first entry, record it in the index
        if self.current_block_entries == 0 {
            self.index.push(IndexEntry {
                key: entry.key.clone(),
                offset: self.position,
                size: 0, // updated when flushed
            });
        }

        // add entry to current block
        self.current_block.extend_from_slice(&entry_data);
        self.current_block_entries += 1;
        self.entry_count += 1;

        Ok(())
    }

    fn serializer_entry(&self, serializer: &mut Serializer, entry: &Entry) -> Result<()> {
        serializer.write_u32(entry.key.len() as u32);
        serializer.write_bytes(&entry.key);

        if let Some(ref value) = entry.value {
            serializer.write_u8(1);
            serializer.write_u32(value.len() as u32);
            serializer.write_bytes(value);
        } else {
            serializer.write_u8(0);
        };

        serializer.write_u64(entry.timestamp);
        serializer.write_u64(entry.sequence);
        Ok(())
    }

    fn flush_current_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        // prepare block
        let mut block_data = Vec::with_capacity(self.current_block.len() + 4);
        let mut serializer = Serializer::new();

        serializer.write_u32(self.current_block_entries as u32);
        block_data.extend_from_slice(&serializer.finish());
        block_data.extend_from_slice(&self.current_block);

        //compress
        let compressed_data = self.compress_block(&block_data)?;

        let mut hasher = Hasher::new();
        hasher.update(&compressed_data);
        let checksum = hasher.finalize();

        // write checksum
        self.writer.write_all(&checksum.to_le_bytes())?;
        self.position += 4;

        self.writer.write_all(&compressed_data)?;
        let data_size = compressed_data.len() as u64;
        self.position += data_size;

        //update
        if let Some(last_index) = self.index.last_mut() {
            last_index.size = data_size + 4;
        };

        // reset
        self.current_block.clear();
        self.current_block_entries = 0;

        Ok(())
    }

    fn compress_block(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.compression {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Lz4 => {
                #[cfg(feature = "compression")]
                {
                    let compressed = lz4_flex::compress_prepend_size(data);
                    Ok(compressed)
                }
                #[cfg(not(feature = "compression"))]
                {
                    Err(StorageError::compression("LZ4 compression not enabled"))
                }
            }
            CompressionType::Zstd => {
                #[cfg(feature = "compression")]
                {
                    let compressed =
                        zstd::bulk::compress(data, self.compression_level).map_err(|e| {
                            StorageError::compression(format!("Zstd compression failed: {}", e))
                        })?;
                    Ok(compressed)
                }
                #[cfg(not(feature = "compression"))]
                {
                    Err(StorageError::compression("Zstd compression not enabled"))
                }
            }
        }
    }

    // finalize the SStable and write metadata
    pub fn finish(mut self) -> Result<SStableMeta> {
        // flush any remaining data
        self.flush_current_block()?;

        let data_size = self.position;

        // write bloom filter
        let bloom_offset = self.position;
        let bloom_data = self.bloom_filter.to_bytes();
        self.writer.write_all(&bloom_data)?;
        self.position += bloom_data.len() as u64;

        let bloom_size = bloom_data.len() as u64;

        // write index
        let index_data = self.serialize_index()?;
        self.writer.write_all(&index_data)?;
        self.position += index_data.len() as u64;
        let index_size = index_data.len() as u64;

        // write footer
        self.write_footer(data_size, index_size, bloom_offset, bloom_size)?;

        // flush and sync
        self.writer.flush()?;
        self.writer
            .into_inner()
            .map_err(|e| e.into_error())?
            .sync_all()?;

        Ok(SStableMeta {
            entry_count: self.entry_count,
            data_size,
            index_size,
            bloom_offset,
            bloom_size,
            min_key: self.min_key.unwrap_or_default(),
            max_key: self.max_key.unwrap_or_default(),
            compression: self.compression,
            created_at: self.created_at,
            version: SSTABLE_VERSION,
        })
    }

    fn serialize_index(&self) -> Result<Vec<u8>> {
        let mut serializer = Serializer::new();

        // write number of index entries
        serializer.write_u32(self.index.len() as u32);

        // write each index entry
        for entry in &self.index {
            serializer.write_u32(entry.key.len() as u32);
            serializer.write_bytes(&entry.key);
            serializer.write_u64(entry.offset);
            serializer.write_u64(entry.size);
        }

        Ok(serializer.finish())
    }

    // write the file footer
    fn write_footer(
        &mut self,
        data_size: u64,
        index_size: u64,
        bloom_offset: u64,
        bloom_size: u64,
    ) -> Result<()> {
        let mut serializer = Serializer::new();

        // write metadata
        serializer.write_u64(self.entry_count);
        serializer.write_u64(data_size);
        serializer.write_u64(index_size);
        serializer.write_u64(bloom_offset);
        serializer.write_u64(bloom_size);
        serializer.write_u64(self.created_at);

        // write compression type
        let compression_type = match self.compression {
            CompressionType::None => 0,
            CompressionType::Lz4 => 1,
            CompressionType::Zstd => 2,
        };

        serializer.write_u8(compression_type);

        serializer.write_u32(SSTABLE_VERSION);
        serializer.write_u32(SSTABLE_MAGIC);

        let footer_data = serializer.finish();

        // ensure footer fits in allocated space
        if footer_data.len() > FOOTER_SIZE {
            return Err(StorageError::corruption(&format!(
                "Footer too large: {} bytes (max: {})",
                footer_data.len(),
                FOOTER_SIZE
            )));
        }

        // pad footer
        let mut padded_footer = footer_data;
        padded_footer.resize(FOOTER_SIZE, 0);

        self.writer.write_all(&padded_footer)?;
        Ok(())
    }

    // get current file size
    pub fn size(&self) -> u64 {
        self.position
    }

    // get the number of entries written so far
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    // get the file path
    pub fn path(&self) -> &Path {
        &self.path
    }
}

//b uilder for creating sstable from iterator
pub struct SSTableBuilder {
    compression: CompressionType,
    compression_level: i32,
    bloom_false_positive_rate: f64,
}

impl SSTableBuilder {
    pub fn new() -> Self {
        Self {
            compression: CompressionType::Lz4,
            compression_level: 1,
            bloom_false_positive_rate: 0.01,
        }
    }

    // set compression type
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    // set compression level
    pub fn compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    // set bloom filter
    pub fn bloom_false_positive_rate(mut self, rate: f64) -> Self {
        self.bloom_false_positive_rate = rate;
        self
    }

    // build an sstable from an iterator of entries
    pub fn build<P, I>(self, path: P, entries: I) -> Result<SStableMeta>
    where
        P: AsRef<Path>,
        I: Iterator<Item = Entry>,
    {
        let entries: Vec<_> = entries.collect();
        let expected_entries = entries.len();

        let mut writer = SSTableWriter::new(
            path,
            expected_entries,
            self.compression,
            self.compression_level,
            self.bloom_false_positive_rate,
        )?;

        for entry in entries {
            writer.add(&entry)?;
        }

        writer.finish()
    }

    // build an sstable from a vector of entries
    pub fn build_from_vec<P>(self, path: P, mut entries: Vec<Entry>) -> Result<SStableMeta>
    where
        P: AsRef<Path>,
    {
        // sort entries by key
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        let expected_entries = entries.len();
        let mut writer = SSTableWriter::new(
            path,
            expected_entries,
            self.compression,
            self.compression_level,
            self.bloom_false_positive_rate,
        )?;

        for entry in entries {
            writer.add(&entry)?;
        }

        writer.finish()
    }
}

impl Default for SSTableBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Entry;
    use tempfile::TempDir;

    fn create_test_entries() -> Vec<Entry> {
        vec![
            Entry::new(b"key1".to_vec(), Some(b"value1".to_vec()), 1000, 1),
            Entry::new(b"key2".to_vec(), Some(b"value2".to_vec()), 1001, 2),
        ]
    }

    #[test]
    fn test_sstable_writer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.sst");

        let writer = SSTableWriter::new(&file_path, 100, CompressionType::None, 0, 0.01);

        assert!(writer.is_ok());
        let writer = writer.unwrap();
        assert_eq!(writer.entry_count(), 0);
        assert_eq!(writer.size(), 0);
    }

    #[test]
    fn test_sstable() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.sst");
        let entries = create_test_entries();

        let builder = SSTableBuilder::new()
            .compression(CompressionType::None)
            .bloom_false_positive_rate(0.01);

        let meta = builder.build_from_vec(&file_path, entries.clone()).unwrap();

        // assert_eq!(meta.entry_count, 4);
        assert_eq!(meta.entry_count, 2);
        assert_eq!(meta.compression, CompressionType::None);
        assert_eq!(meta.min_key, b"key1".to_vec());
        // assert_eq!(meta.max_key, b"key4".to_vec());
        assert_eq!(meta.max_key, b"key2".to_vec());
        assert!(meta.data_size > 0);
        assert!(meta.index_size > 0);
    }
}

// ini bahasa Rust
// Pake ArchLinux
// Teks editor NeoVim
// Project Database Sistem
