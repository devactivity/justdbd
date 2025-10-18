use crate::{Entry, Key, Result, SequenceNumber, StorageError, Timestamp, Value};

use parking_lot::RwLock;
use std::{
    collections::{BTreeMap, btree_map},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

#[derive(Debug, Clone)]
pub struct MemtableStats {
    pub entry_count: usize,
    pub approximate_size: usize,
    pub min_key_size: usize,
    pub max_key_size: usize,
    pub min_value_size: usize,
    pub max_value_size: usize,
    pub creation_time: Timestamp,
    pub is_immutable: bool,
}

impl Clone for MemTable {
    fn clone(&self) -> Self {
        let data = self.data.read();
        let clonded_data = data.clone();

        Self {
            data: Arc::new(RwLock::new(clonded_data)),
            size: AtomicUsize::new(self.size.load(Ordering::SeqCst)),
            count: AtomicUsize::new(self.count.load(Ordering::SeqCst)),
            created_at: self.created_at,
            sequence_counter: AtomicU64::new(self.sequence_counter.load(Ordering::SeqCst)),
            max_size: self.max_size,
            immutable: Arc::new(AtomicBool::new(self.immutable.load(Ordering::SeqCst))),
        }
    }
}
/// iterator over memtable entries
pub struct MemTableIterator {
    inner: btree_map::Iter<'static, Key, Entry>,
    _guard: Arc<RwLock<BTreeMap<Key, Entry>>>,
}

impl MemTableIterator {
    fn new(map: Arc<RwLock<BTreeMap<Key, Entry>>>) -> Self {
        let guard = map.read();
        let iter = unsafe {
            std::mem::transmute::<
                btree_map::Iter<'_, Key, Entry>,
                btree_map::Iter<'static, Key, Entry>,
            >(guard.iter())
        };

        // keep the guard alive
        std::mem::forget(guard);

        Self {
            inner: iter,
            _guard: map,
        }
    }
}

impl Iterator for MemTableIterator {
    type Item = (Key, Entry);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| (k.clone(), v.clone()))
    }
}

#[derive(Debug)]
pub struct MemTable {
    data: Arc<RwLock<BTreeMap<Key, Entry>>>,
    size: AtomicUsize,
    count: AtomicUsize,
    created_at: Timestamp,
    sequence_counter: AtomicU64,
    max_size: usize,
    immutable: Arc<AtomicBool>,
}

impl MemTable {
    pub fn new(max_size: usize) -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            size: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
            created_at: Self::current_timestamp(),
            sequence_counter: AtomicU64::new(0),
            max_size,
            immutable: Arc::new(AtomicBool::new(false)),
        }
    }

    // create starting sequence number
    pub fn with_sequence(max_size: usize, start_sequence: SequenceNumber) -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            size: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
            created_at: Self::current_timestamp(),
            sequence_counter: AtomicU64::new(start_sequence),
            max_size,
            immutable: Arc::new(AtomicBool::new(false)),
        }
    }

    // get current timestamp
    fn current_timestamp() -> Timestamp {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as Timestamp
    }

    // get the next seq number
    fn next_sequence(&self) -> SequenceNumber {
        self.sequence_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// insert a key-value pair
    pub fn put(&self, key: Key, value: Value) -> Result<()> {
        if self.is_immutable() {
            return Err(StorageError::ReadOnly);
        }

        let sequence = self.next_sequence();
        let timestamp = Self::current_timestamp();
        let entry = Entry::new(key.clone(), Some(value), timestamp, sequence);

        let entry_size = entry.size();
        let mut data = self.data.write();

        // calculate
        let size_data = if let Some(old_entry) = data.get(&key) {
            entry_size as i64 - old_entry.size() as i64
        } else {
            entry_size as i64
        };

        // insert the entry
        let is_the_data = data.insert(key, entry).is_none();

        // update
        if is_the_data {
            self.count.fetch_add(1, Ordering::SeqCst);
        }

        if size_data > 0 {
            self.size.fetch_add(size_data as usize, Ordering::SeqCst);
        } else {
            self.size.fetch_sub(size_data as usize, Ordering::SeqCst);
        }

        Ok(())
    }

    // delete a key
    pub fn delete(&self, key: Key) -> Result<()> {
        if self.is_immutable() {
            return Err(StorageError::ReadOnly);
        }

        let sequence = self.next_sequence();
        let timestamp = Self::current_timestamp();
        let entry = Entry::new(key.clone(), None, timestamp, sequence);

        let entry_size = entry.size();
        let mut data = self.data.write();

        // calculate
        let size_data = if let Some(old_entry) = data.get(&key) {
            entry_size as i64 - old_entry.size() as i64
        } else {
            entry_size as i64
        };

        // update stats
        let is_the_data = data.insert(key, entry).is_none();

        if is_the_data {
            self.count.fetch_add(1, Ordering::SeqCst);
        }

        if size_data > 0 {
            self.size.fetch_add(size_data as usize, Ordering::SeqCst);
        } else {
            self.size.fetch_sub(size_data as usize, Ordering::SeqCst);
        }

        Ok(())
    }

    // get value by key
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        let data = self.data.read();
        data.get(key).cloned()
    }

    // check if memtable contain a key
    pub fn contains_key(&self, key: &[u8]) -> bool {
        let data = self.data.read();
        data.contains_key(key)
    }

    // get approximate size in bytes
    pub fn size(&self) -> usize {
        self.size.load(Ordering::SeqCst)
    }

    // get the number of entries
    pub fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    // check if memtable should flushed
    pub fn should_flush(&self) -> bool {
        self.size() >= self.max_size
    }

    // get the creation timestamp
    pub fn created_at(&self) -> Timestamp {
        self.created_at
    }

    // make this immutable
    pub fn make_immutable(&self) {
        self.immutable.store(true, Ordering::SeqCst);
    }

    // check if this is immutable
    pub fn is_immutable(&self) -> bool {
        self.immutable.load(Ordering::SeqCst)
    }

    // get current seq number
    pub fn current_sequence(&self) -> SequenceNumber {
        self.sequence_counter.load(Ordering::SeqCst)
    }

    // create an iterator over all entries
    pub fn iter(&self) -> MemTableIterator {
        MemTableIterator::new(self.data.clone())
    }

    // create an iterator over entries in a key range
    pub fn range_iter(&self, start: &[u8], end: &[u8]) -> Vec<(Key, Entry)> {
        let data = self.data.read();
        let start_bound = std::ops::Bound::Included(start.to_vec());
        let end_bound = std::ops::Bound::Excluded(end.to_vec());

        data.range((start_bound, end_bound))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// get entries starting from a key
    pub fn scan_from(&self, start_key: &[u8]) -> Vec<(Key, Entry)> {
        let data = self.data.read();
        let start_bound = std::ops::Bound::Included(start_key.to_vec());

        data.range((start_bound, std::ops::Bound::Unbounded))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    // get all entries as a vector
    pub fn entries(&self) -> Vec<(Key, Entry)> {
        let data = self.data.read();

        data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    // get memory_usage stats
    pub fn memory_usage(&self) -> MemtableStats {
        let data = self.data.read();
        let entry_count = data.len();
        let approximate_size = self.size();

        // calculate
        let (min_key_size, max_key_size, min_value_size, max_value_size) =
            data.values()
                .fold((usize::MAX, 0, usize::MAX, 0), |acc, entry| {
                    let key_size = entry.key.len();
                    let value_size = entry.value.as_ref().map_or(0, |v| v.len());
                    (
                        acc.0.min(key_size),
                        acc.1.max(key_size),
                        acc.2.min(value_size),
                        acc.3.max(value_size),
                    )
                });

        MemtableStats {
            entry_count,
            approximate_size,
            min_key_size: if min_key_size == usize::MAX {
                0
            } else {
                min_key_size
            },
            max_key_size,
            min_value_size: if min_value_size == usize::MAX {
                0
            } else {
                min_value_size
            },
            max_value_size,
            creation_time: self.created_at,
            is_immutable: self.is_immutable(),
        }
    }
}

// ini bahasa Rust
// Pake ArchLinux
// pake teks editor NeoVim
// Ini bikin Database sistem
// (LSM, MemTable, SSTable)
