use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Lz4,
    Zstd,
}

impl Default for CompressionType {
    fn default() -> Self {
        Self::Lz4
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct Config {
    pub memtable_size_threshold: usize,
    pub max_memtables: usize,
    pub level_size_multiplier: usize,
    pub max_levels: usize,
    pub l0_sstable_size: usize,
    pub l0_compaction_threshold: usize,

    pub compression: CompressionType,
    pub compression_level: i32,

    pub enable_bloom_filters: bool,
    pub bloom_false_positive_rate: f64,
    pub write_buffer_size: usize,
    pub wal_sync_interval: usize,
    pub max_wal_size: usize,

    pub compaction_threads: usize,
    pub compaction_interval: Duration,
    pub enable_compaction: bool,

    pub max_concurrent_compactions: usize,
    pub enable_metrics: bool,
    pub cache_size: usize,
    pub max_open_files: usize,
    pub use_mmap: bool,
    pub verify_checksums: bool,
    pub paranoid_checks: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            memtable_size_threshold: 64 * 1024 * 1024, // 64MB
            max_memtables: 2,
            level_size_multiplier: 10,
            max_levels: 7,
            l0_sstable_size: 2 * 1024 * 1024, // 2MB
            l0_compaction_threshold: 4,
            compression: CompressionType::default(),
            compression_level: 1,
            enable_bloom_filters: true,
            bloom_false_positive_rate: 0.01,    // 1%
            write_buffer_size: 4 * 1024 * 1024, // 4MB
            wal_sync_interval: 1000,
            max_wal_size: 64 * 1024 * 1024, // 64MB
            compaction_threads: num_cpus::get(),
            compaction_interval: Duration::from_secs(60),
            enable_compaction: true,
            max_concurrent_compactions: 2,
            enable_metrics: true,
            cache_size: 128 * 1024 * 1024, // 128MB
            max_open_files: 10,
            use_mmap: true,
            verify_checksums: true,
            paranoid_checks: false,
        }
    }
}

impl Config {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_memtable_size(mut self, size: usize) -> Self {
        self.memtable_size_threshold = size;
        self
    }

    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    pub fn with_bloom_filters(mut self, enabled: bool) -> Self {
        self.enable_bloom_filters = enabled;
        self
    }

    pub fn with_bloom_false_positive_rate(mut self, rate: f64) -> Self {
        self.bloom_false_positive_rate = rate;
        self
    }

    pub fn with_compaction(mut self, enabled: bool) -> Self {
        self.enable_compaction = enabled;
        self
    }

    pub fn with_compaction_threads(mut self, threads: usize) -> Self {
        self.compaction_threads = threads;
        self
    }

    pub fn with_metrics(mut self, enabled: bool) -> Self {
        self.enable_metrics = enabled;
        self
    }

    pub fn with_cache_size(mut self, size: usize) -> Self {
        self.cache_size = size;
        self
    }

    pub fn with_mmap(mut self, enabled: bool) -> Self {
        self.use_mmap = enabled;
        self
    }
    pub fn with_checksum_verification(mut self, enabled: bool) -> Self {
        self.verify_checksums = enabled;
        self
    }
    pub fn with_paranoid_checks(mut self, enabled: bool) -> Self {
        self.paranoid_checks = enabled;
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.memtable_size_threshold == 0 {
            return Err("memtable_size_threshold must be greater than 0".to_string());
        }
        if self.max_memtables == 0 {
            return Err("max_memtables must be greater than 0".to_string());
        }
        if self.level_size_multiplier < 10 {
            return Err("level_size_multiplier must be at least 10".to_string());
        }
        if self.max_levels == 0 {
            return Err("mmax_levels must be greater than 0".to_string());
        }
        if self.l0_sstable_size == 0 {
            return Err("l0_sstable_size must be greater than 0".to_string());
        }
        if self.l0_compaction_threshold == 0 {
            return Err("l0_compaction_threshold must be greater than 0".to_string());
        }
        if !(0.0..=1.0).contains(&self.bloom_false_positive_rate) {
            return Err("bloom_false_positive_rate must be between 0.0 and 1.0".to_string());
        }
        if self.write_buffer_size == 0 {
            return Err("write_buffer_size must be greater than 0".to_string());
        }
        if self.wal_sync_interval == 0 {
            return Err("wal_sync_interval must be greater than 0".to_string());
        }
        if self.max_wal_size == 0 {
            return Err("max_wal_size must be greater than 0".to_string());
        }
        if self.compaction_threads == 0 {
            return Err("compaction_threads must be greater than 0".to_string());
        }
        if self.max_concurrent_compactions == 0 {
            return Err("max_concurrent_compactions must be greater than 0".to_string());
        }
        if self.max_open_files == 0 {
            return Err("max_open_files must be greater than 0".to_string());
        }

        Ok(())
    }

    pub fn level_target_size(&self, level: usize) -> usize {
        if level == 0 {
            self.l0_sstable_size * self.l0_compaction_threshold
        } else {
            self.l0_sstable_size * self.level_size_multiplier.pow(level as u32)
        }
    }

    pub fn level_max_sstables(&self, level: usize) -> usize {
        if level == 0 {
            self.l0_compaction_threshold
        } else {
            self.level_max_sstables(level) / self.l0_sstable_size
        }
    }
}

use std::sync::OnceLock;

static CPU_COUNT: OnceLock<usize> = OnceLock::new();

mod num_cpus {
    use super::*;

    pub fn get() -> usize {
        *CPU_COUNT.get_or_init(|| {
            std::thread::available_parallelism()
                .map(|i| i.get())
                .unwrap_or(1)
        })
    }
}
