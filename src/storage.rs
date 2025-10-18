use crate::{
    Entry, Key, MAX_KEY_SIZE, MAX_VALUE_SIZE, Result, SequenceNumber, StorageError, Value,
    config::Config,
    memtable::MemTable,
    sstable::{SSTableBuilder, SSTableReader},
};

use tracing::warn;

use parking_lot::RwLock;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::Mutex;

// main LSM storage engine
pub struct StorageEngine {
    config: Config,
    data_dir: PathBuf,
    memtable: Arc<RwLock<MemTable>>,
    immutable_memtables: Arc<RwLock<Vec<Arc<MemTable>>>>,
    sstables: Arc<RwLock<HashMap<usize, Vec<Arc<SSTableReader>>>>>,
    sequence_counter: AtomicU64,
    closed: AtomicBool,
    background_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl StorageEngine {
    pub async fn open<P: AsRef<Path>>(data_dir: P, config: Config) -> Result<Arc<Self>> {
        let data_dir = data_dir.as_ref().to_path_buf();

        // validate config
        config.validate().map_err(StorageError::config)?;

        // create data directory if it doesn't exist
        if !data_dir.exists() {
            std::fs::create_dir_all(&data_dir)?;
        }

        // create subdir
        let sstable_dir = data_dir.join("sstables");
        let wal_dir = data_dir.join("wal");
        std::fs::create_dir_all(&sstable_dir)?;
        std::fs::create_dir_all(&wal_dir)?;

        let engine = Arc::new(Self {
            config: config.clone(),
            data_dir,
            memtable: Arc::new(RwLock::new(MemTable::new(config.memtable_size_threshold))),
            immutable_memtables: Arc::new(RwLock::new(Vec::new())),
            sstables: Arc::new(RwLock::new(HashMap::new())),
            sequence_counter: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            background_tasks: Arc::new(Mutex::new(Vec::new())),
        });

        // load existing sstables
        engine.load_sstables().await?;

        // start background tasks
        if config.enable_compaction {
            engine.clone().start_background_compaction().await;
        }

        Ok(engine)
    }

    // put a key-value pair
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.is_closed() {
            return Err(StorageError::Closed);
        }

        // validate input sizes
        if key.len() > MAX_KEY_SIZE {
            return Err(StorageError::InvalidKeySize {
                size: key.len(),
                max_size: MAX_KEY_SIZE,
            });
        }

        if value.len() > MAX_VALUE_SIZE {
            return Err(StorageError::InvalidValueSize {
                size: value.len(),
                max_size: MAX_VALUE_SIZE,
            });
        }

        // check if we need to rotate memtable
        self.check_rotate_memtable().await?;

        // write to current memtable
        {
            let memtable = self.memtable.read();
            memtable.put(key.to_vec(), value.to_vec())?;
        }

        Ok(())
    }

    // get a value by key
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.is_closed() {
            return Err(StorageError::Closed);
        }

        // check current memtable first
        {
            let memtable = self.memtable.read();
            if let Some(entry) = memtable.get(key) {
                return Ok(entry.value);
            }
        }

        // check imjutable memtables
        {
            let immutable = self.immutable_memtables.read();
            for memtable in immutable.iter().rev() {
                // check newest first
                if let Some(entry) = memtable.get(key) {
                    return Ok(entry.value);
                }
            }
        }

        // check ssstable
        {
            let sstables = self.sstables.read();
            for level in 0..self.config.max_levels {
                if let Some(level_tables) = sstables.get(&level) {
                    // check all tables
                    if level == 0 {
                        for table in level_tables.iter().rev() {
                            // check newest first
                            if table.might_contain(key) {
                                if let Some(entry) = table.get(key)? {
                                    return Ok(entry.value);
                                }
                            }
                        }
                    } else {
                        if let Some(table) = self.find_sstable_for_key(level_tables, key) {
                            if table.might_contain(key) {
                                if let Some(entry) = table.get(key)? {
                                    return Ok(entry.value);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    // delete a key
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        if self.is_closed() {
            return Err(StorageError::Closed);
        }

        if key.len() > MAX_KEY_SIZE {
            return Err(StorageError::InvalidKeySize {
                size: key.len(),
                max_size: MAX_KEY_SIZE,
            });
        }

        // check if we need to rotate
        self.check_rotate_memtable().await?;

        // write
        {
            let memtable = self.memtable.read();
            memtable.delete(key.to_vec())?;
        }

        Ok(())
    }

    // scan a range of keys
    pub async fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Key, Value)>> {
        if self.is_closed() {
            return Err(StorageError::Closed);
        }

        let mut results = HashMap::new();

        // collect from current memtable
        {
            let memtable = self.memtable.read();
            let entries = memtable.range_iter(start, end);

            for (key, entry) in entries {
                if let Some(value) = entry.value {
                    results.insert(key, (value, entry.sequence));
                } else {
                    // delete marker
                    results.insert(key, (Vec::new(), entry.sequence));
                }
            }
        }

        // collect from immmutable memtables
        {
            let immutable = self.immutable_memtables.read();
            for memtable in immutable.iter().rev() {
                let entries = memtable.range_iter(start, end);

                for (key, entry) in entries {
                    // only add if we had not seen
                    // this key of this entry is newer
                    if let Some((_, existing_seq)) = results.get(&key) {
                        if entry.sequence <= *existing_seq {
                            continue;
                        }
                    }

                    if let Some(value) = entry.value {
                        results.insert(key, (value, entry.sequence));
                    } else {
                        results.insert(key, (Vec::new(), entry.sequence));
                    }
                }
            }
        }

        // collect from sstable
        {
            let sstables = self.sstables.read();
            for level in 0..self.config.max_levels {
                if let Some(level_tables) = sstables.get(&level) {
                    for table in level_tables {
                        let entries = table.range(start, end)?;
                        for entry in entries {
                            if let Some((_, existing_seq)) = results.get(&entry.key) {
                                if entry.sequence <= *existing_seq {
                                    continue;
                                }
                            }

                            if let Some(value) = entry.value {
                                results.insert(entry.key, (value, entry.sequence));
                            } else {
                                results.insert(entry.key, (Vec::new(), entry.sequence));
                            }
                        }
                    }
                }
            }
        }

        // filter and delete and convert to final result
        let mut final_results: Vec<(Key, Value)> = results
            .into_iter()
            .filter_map(|(key, (value, _))| {
                if value.is_empty() {
                    None
                } else {
                    Some((key, value))
                }
            })
            .collect();

        // sort by key
        final_results.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(final_results)
    }

    async fn check_rotate_memtable(&self) -> Result<()> {
        let should_rotate = {
            let memtable = self.memtable.read();
            memtable.should_flush()
        };

        if should_rotate {
            self.rotate_memtable().await?;
        }

        Ok(())
    }

    async fn rotate_memtable(&self) -> Result<()> {
        let old_memtable = {
            let mut memtable_guard = self.memtable.write();
            let old = std::mem::replace(
                &mut *memtable_guard,
                MemTable::with_sequence(self.config.memtable_size_threshold, self.next_sequence()),
            );

            old.make_immutable();
            Arc::new(old)
        };

        // add to immutable list
        {
            let mut immutable = self.immutable_memtables.write();

            immutable.push(old_memtable.clone());

            // limit number of immutable memtables
            while immutable.len() > self.config.max_memtables {
                let oldest = immutable.remove(0);
                // drop
                drop(immutable);

                // flush
                self.flush_memtable(oldest).await?;
                immutable = self.immutable_memtables.write();
            }
        }
        Ok(())
    }

    // flush a memtable
    async fn flush_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        if memtable.count() == 0 {
            return Ok(());
        }

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let sstable_path = self.data_dir.join("sstables").join(format!(
            "L0_{:010}_{}.sst",
            timestamp,
            memtable.current_sequence()
        ));

        // create entries vector from memtable
        let entries = memtable.entries();

        // build sstable
        let build = SSTableBuilder::new()
            .compression(self.config.compression)
            .compression_level(self.config.compression_level)
            .bloom_false_positive_rate(self.config.bloom_false_positive_rate);

        let meta = build.build_from_vec(
            &sstable_path,
            entries.into_iter().map(|(_, entry)| entry).collect(),
        )?;

        // add to L0
        let reader = SSTableReader::open(&sstable_path, self.config.use_mmap)?;

        {
            let mut sstables = self.sstables.write();
            sstables
                .entry(0)
                .or_insert_with(Vec::new)
                .push(Arc::new(reader));
        }

        println!(
            "flushed memtable to l0: {sstable_path:?} ({} entries)",
            meta.entry_count
        );

        Ok(())
    }

    fn find_sstable_for_key<'a>(
        &self,
        tables: &'a [Arc<SSTableReader>],
        key: &[u8],
    ) -> Option<&'a Arc<SSTableReader>> {
        tables.iter().find(move |table| {
            let meta = table.metadata();
            key >= meta.min_key.as_slice() && key <= meta.max_key.as_slice()
        })
    }

    async fn load_sstables(&self) -> Result<()> {
        let sstable_dir = self.data_dir.join("sstables");
        if !sstable_dir.exists() {
            return Ok(());
        }

        let mut sstable_by_level: HashMap<usize, Vec<Arc<SSTableReader>>> = HashMap::new();

        // read all .sst files
        let entries = std::fs::read_dir(&sstable_dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("sst") {
                if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                    // parse level from filename
                    // format: L{level}_{timestamp}_{sequence}.sst
                    if let Some(level) = self.parse_level_from_filename(file_name) {
                        match SSTableReader::open(&path, self.config.use_mmap) {
                            Ok(reader) => {
                                sstable_by_level
                                    .entry(level)
                                    .or_insert_with(Vec::new)
                                    .push(Arc::new(reader));
                            }
                            Err(e) => {
                                println!("failed to load SSTable {path:?}: {e}");
                            }
                        }
                    }
                }
            }
        }

        // sorting
        for tables in sstable_by_level.values_mut() {
            tables.sort_by_key(|table| table.metadata().created_at);
        }

        // update the storage
        {
            let mut sstables = self.sstables.write();
            *sstables = sstable_by_level;
        }

        Ok(())
    }

    fn parse_level_from_filename(&self, filename: &str) -> Option<usize> {
        if filename.starts_with('L') {
            if let Some(underscore_pos) = filename.find('_') {
                let level_str = &filename[1..underscore_pos];
                level_str.parse().ok()
            } else {
                None
            }
        } else {
            None
        }
    }

    // start bg compaction task
    async fn start_background_compaction(self: Arc<Self>) {
        let engine_clone = Arc::clone(&self);

        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(engine_clone.config.compaction_interval);

            loop {
                interval.tick().await;

                if engine_clone.is_closed() {
                    break;
                }

                if let Err(e) = engine_clone.run_compaction().await {
                    eprintln!("compaction failed: {e}");
                }
            }
        });

        self.background_tasks.lock().await.push(task);
    }

    async fn run_compaction(&self) -> Result<()> {
        // check if L0 needs compaction
        let l0_count = {
            let sstables = self.sstables.read();
            sstables.get(&0).map_or(0, |tables| tables.len())
        };

        if l0_count >= self.config.l0_compaction_threshold {
            self.compact_l0_to_l1().await?;
        }

        // check other level
        for level in 1..self.config.max_levels - 1 {
            if self.level_needs_compaction(level).await {
                self.compact_level(level).await?;
            }
        }

        Ok(())
    }

    async fn level_needs_compaction(&self, level: usize) -> bool {
        let sstables = self.sstables.read();
        if let Some(tables) = sstables.get(&level) {
            let current_size: u64 = tables.iter().map(|t| t.metadata().data_size).sum();
            let target_size = self.config.level_target_size(level) as u64;

            current_size > target_size
        } else {
            false
        }
    }

    async fn compact_l0_to_l1(&self) -> Result<()> {
        let (l0_tables, l1_tables) = {
            let sstables = self.sstables.read();
            let l0 = sstables.get(&0).cloned().unwrap_or_default();
            let l1 = sstables.get(&1).cloned().unwrap_or_default();

            (l0, l1)
        };

        if l0_tables.is_empty() {
            return Ok(());
        }

        // merge
        let merge_entries = self.merge_sstables(&l0_tables, &l1_tables).await?;

        // create l1
        let new_l1_tables = self.create_sstables_for_level(1, merge_entries).await?;

        //update
        {
            let mut sstables = self.sstables.write();
            sstables.insert(0, Vec::new());
            sstables.insert(1, new_l1_tables);
        }

        // delete old files
        for table in l0_tables.iter().chain(l1_tables.iter()) {
            if let Err(e) = std::fs::remove_file(table.path()) {
                eprintln!("failed to delete old SSTable file {:?}: {e}", table.path());
            }
        }

        Ok(())
    }

    async fn compact_level(&self, level: usize) -> Result<()> {
        let (current_tables, next_tables) = {
            let sstables = self.sstables.read();
            let current = sstables.get(&level).cloned().unwrap_or_default();
            let next = sstables.get(&(level + 1)).cloned().unwrap_or_default();

            (current, next)
        };

        if current_tables.is_empty() {
            return Ok(());
        }

        let merge_entries = self.merge_sstables(&current_tables, &next_tables).await?;

        let new_next_tables = self
            .create_sstables_for_level(level + 1, merge_entries)
            .await?;

        //update
        {
            let mut sstables = self.sstables.write();
            sstables.insert(level, Vec::new());
            sstables.insert(level + 1, new_next_tables);
        }

        // delete old files
        for table in current_tables.iter().chain(next_tables.iter()) {
            if let Err(e) = std::fs::remove_file(table.path()) {
                eprintln!("failed to delete old SSTable file {:?}: {e}", table.path());
            }
        }

        Ok(())
    }

    async fn merge_sstables(
        &self,
        tables1: &[Arc<SSTableReader>],
        tables2: &[Arc<SSTableReader>],
    ) -> Result<Vec<Entry>> {
        let mut entries: HashMap<Key, Entry> = HashMap::new();

        // collect entries
        for table in tables1.iter().chain(tables2.iter()) {
            for entry_result in table.iter() {
                let entry = entry_result?;

                // keep the entry with the highest seq number
                if let Some(existing) = entries.get(&entry.key) {
                    if entry.sequence > existing.sequence {
                        entries.insert(entry.key.clone(), entry);
                    }
                } else {
                    entries.insert(entry.key.clone(), entry);
                }
            }
        }

        // convert to sorted vec
        let mut sorted_entries: Vec<Entry> = entries
            .into_values()
            .filter(|entry| entry.value.is_some())
            .collect();

        sorted_entries.sort_by(|a, b| a.key.cmp(&b.key));

        Ok(sorted_entries)
    }

    async fn create_sstables_for_level(
        &self,
        level: usize,
        entries: Vec<Entry>,
    ) -> Result<Vec<Arc<SSTableReader>>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let target_size = if level == 0 {
            self.config.l0_sstable_size
        } else {
            self.config.l0_sstable_size * 2
        };

        let mut sstables = Vec::new();
        let mut current_entries = Vec::new();
        let mut current_size = 0;

        for entry in entries {
            let entry_size = entry.size();

            if current_size + entry_size > target_size && !current_entries.is_empty() {
                // create sstable
                let table = self.create_sstable(level, &current_entries).await?;

                sstables.push(table);

                current_entries.clear();
                current_size = 0;
            }

            current_entries.push(entry);
            current_size += entry_size;
        }

        // create for remaining
        if !current_entries.is_empty() {
            let table = self.create_sstable(level, &current_entries).await?;
            sstables.push(table);
        }

        Ok(sstables)
    }

    async fn create_sstable(&self, level: usize, entries: &[Entry]) -> Result<Arc<SSTableReader>> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let sequence = self.next_sequence();
        let path = self
            .data_dir
            .join("sstables")
            .join(format!("L{}_{:010}_{}.sst", level, timestamp, sequence));

        let builder = SSTableBuilder::new()
            .compression(self.config.compression)
            .compression_level(self.config.compression_level)
            .bloom_false_positive_rate(self.config.bloom_false_positive_rate);

        builder.build_from_vec(&path, entries.to_vec())?;

        let reader = SSTableReader::open(&path, self.config.use_mmap)?;
        Ok(Arc::new(reader))
    }

    // get next seq num
    fn next_sequence(&self) -> SequenceNumber {
        self.sequence_counter.fetch_add(1, Ordering::SeqCst)
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    // close the storage engine
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        // cancel bg task
        {
            let mut tasks = self.background_tasks.lock().await;
            for task in tasks.drain(..) {
                task.abort();
            }
        }

        // flush remaining immutable memtables
        {
            let mut immutable = self.immutable_memtables.write();
            for memtable in immutable.drain(..) {
                if let Err(e) = self.flush_memtable(memtable).await {
                    eprintln!("failed to flush memtable during shutdown : {e}");
                }
            }
        }

        // flush current memetable
        {
            let memtable = self.memtable.read();
            if memtable.count() > 0 {
                let clone_memtable = memtable.clone();
                clone_memtable.make_immutable();

                drop(memtable);

                if let Err(e) = self.flush_memtable(Arc::new(clone_memtable)).await {
                    eprintln!("failed to flush current memtable during shutdown: {e}");
                }
            }

            Ok(())
        }
    }

    pub async fn stats(&self) -> StorageStats {
        let memtable_stats = {
            let memtable = self.memtable.read();
            memtable.memory_usage()
        };

        let immutable_count = {
            let immutable = self.immutable_memtables.read();
            immutable.len()
        };

        let mut sstable_stats = HashMap::new();

        {
            let sstables = self.sstables.read();
            for (level, tables) in sstables.iter() {
                let count = tables.len();
                let total_size: u64 = tables.iter().map(|t| t.metadata().data_size).sum();
                let total_entries: u64 = tables.iter().map(|t| t.metadata().entry_count).sum();

                sstable_stats.insert(
                    *level,
                    LevelStats {
                        table_count: count,
                        total_size,
                        total_entries,
                    },
                );
            }
        }

        StorageStats {
            memtable_stats,
            immutable_memtable_count: immutable_count,
            sstable_stats,
            seqence_number: self.sequence_counter.load(Ordering::SeqCst),
            is_closed: self.is_closed(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LevelStats {
    pub table_count: usize,
    pub total_size: u64,
    pub total_entries: u64,
}

#[derive(Debug, Clone)]
pub struct StorageStats {
    pub memtable_stats: crate::memtable::MemtableStats,
    pub immutable_memtable_count: usize,
    pub sstable_stats: HashMap<usize, LevelStats>,
    pub seqence_number: SequenceNumber,
    pub is_closed: bool,
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        if !self.is_closed() {
            warn!("storage engined dropped without being properly closed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_engine() -> (TempDir, Arc<StorageEngine>) {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new()
            .with_memtable_size(1024)
            .with_compaction(false);

        let engine = StorageEngine::open(temp_dir.path(), config).await.unwrap();

        (temp_dir, engine)
    }

    #[tokio::test]
    async fn test_basic_operations() {
        let (_temp_dir, engine) = create_test_engine().await;

        // test put
        engine.put(b"key1", b"value1").await.unwrap();
        engine.put(b"key2", b"value2").await.unwrap();

        // test get
        assert_eq!(engine.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert_eq!(engine.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
        assert_eq!(engine.get(b"notexist").await.unwrap(), None);

        // test update
        engine.put(b"key1", b"updated_value1").await.unwrap();
        assert_eq!(
            engine.get(b"key1").await.unwrap(),
            Some(b"updated_value1".to_vec())
        );

        // test delete
        engine.delete(b"key1").await.unwrap();
        assert_eq!(engine.get(b"key1").await.unwrap(), None);
        assert_eq!(engine.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
    }
}
