pub mod bloom;
pub mod compaction;
pub mod config;
pub mod error;
pub mod memtable;
pub mod serialization;
pub mod sstable;
pub mod storage;

pub use config::Config;
pub use error::{Result, StorageError};
pub use storage::StorageEngine;

/// Key type used throughout the storage engine
pub type Key = Vec<u8>;

/// Value type used throughout the storage engine
pub type Value = Vec<u8>;

/// Timestamp type for versioning
pub type Timestamp = u64;

/// Sequence number for ordering operations
pub type SequenceNumber = u64;

/// Entry in the storage engine
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub key: Key,
    pub value: Option<Value>,
    pub timestamp: Timestamp,
    pub sequence: SequenceNumber,
}

impl Entry {
    pub fn new(
        key: Key,
        value: Option<Value>,
        timestamp: Timestamp,
        sequence: SequenceNumber,
    ) -> Self {
        Self {
            key,
            value,
            timestamp,
            sequence,
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.value.is_none()
    }

    pub fn size(&self) -> usize {
        self.key.len() + self.value.as_ref().map_or(0, |v| v.len()) + 16 // 8 bytes each for timestmap and sequence
    }
}

/// version information for the storage engine
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// max key size in bytes
pub const MAX_KEY_SIZE: usize = 1024;

/// max value size in bytes
pub const MAX_VALUE_SIZE: usize = 1024 * 1024; // 1MB
