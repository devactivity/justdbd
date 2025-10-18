#[derive(Debug, Clone, Copy)]
pub enum CompactionStrategy {
    SizeTiered,
    Leveled,
}

pub struct CompactionManager {
    strategy: CompactionStrategy,
}

impl CompactionManager {
    pub fn new(strategy: CompactionStrategy) -> Self {
        Self { strategy }
    }
}
