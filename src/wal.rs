// write ahead log
//
// placeholder for later
//

pub struct WriteAheadLog {
    path: std::path::PathBuf,
}

impl WriteAheadLog {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }
}
