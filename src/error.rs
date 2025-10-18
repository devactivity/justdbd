use thiserror::Error;
use tokio::sync::AcquireError;

// type alias
pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Error, Debug)]
pub enum BincodeError {
    #[error("encode error: {0}")]
    Encode(bincode::error::EncodeError),
    #[error("encode error: {0}")]
    Decode(bincode::error::DecodeError),
}

impl From<bincode::error::EncodeError> for BincodeError {
    fn from(value: bincode::error::EncodeError) -> Self {
        BincodeError::Encode(value)
    }
}

impl From<bincode::error::DecodeError> for BincodeError {
    fn from(value: bincode::error::DecodeError) -> Self {
        BincodeError::Decode(value)
    }
}

// error that can occur
#[derive(Error, Debug)]
pub enum StorageError {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    ///  serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] BincodeError), // TODO: fix me bro...
    /// MessagePack serialization error
    #[error("MessagePack error: {0}")]
    MessagePack(#[from] rmp_serde::encode::Error),
    /// MessagePackDecode serialization error
    #[error("MessagePackDecode error: {0}")]
    MessagePackDecode(#[from] rmp_serde::decode::Error),
    /// compression error
    #[error("Compression error: {message}")]
    Compression { message: String },
    /// decompression error
    #[error("Decompression error: {message}")]
    Decompression { message: String },
    /// checksum error
    #[error("Checksum mismatch: expected {expected:x}, got {actual:x}")]
    ChecksumMismatch { expected: u32, actual: u32 },
    /// invalid key size
    #[error("Invalid key size: {size} bytes (max: {max_size})")]
    InvalidKeySize { size: usize, max_size: usize },
    /// invalid value size
    #[error("Invalid value size: {size} bytes (max: {max_size})")]
    InvalidValueSize { size: usize, max_size: usize },
    /// database corruption detected
    #[error("Database corruption detected: {message}")]
    Corruption { message: String },
    /// lock acquisition failed
    #[error("Lock acquisition failed: {message}")]
    LockFailed { message: String },
    /// write-ahead log error
    #[error("WAL error: {message}")]
    Wal { message: String },
    /// Compaction error
    #[error("Compaction error: {message}")]
    Compaction { message: String },
    /// Config error
    #[error("Config error: {message}")]
    Config { message: String },
    /// Memory allocation error
    #[error("Memory allocation error: {message}")]
    Memory { message: String },
    /// Too many open files
    #[error("Too many open files: {current}/{max}")]
    TooManyFiles { current: usize, max: usize },
    /// Invalid file format
    #[error("Invalid file format: {file_type} in {path}")]
    InvalidFileFormat { file_type: String, path: String },

    /// Key not found
    #[error("Key not found")]
    KeyNotFound,
    /// database is read-only
    #[error("Database is read-only")]
    ReadOnly,
    /// timeout error
    #[error("Operation timed out after {duration:?}")]
    Timeout { duration: std::time::Duration },
    /// database is closed
    #[error("Database is closed")]
    Closed,
    /// resource exhausted
    #[error("Resource exhausted: {resource}")]
    ResourceExhausted { resource: String },
    /// invalid operation
    #[error("Invalid operation: {message}")]
    InvalidOperation { message: String },
    /// Thread pool error
    #[error("Thread pool error: {message}")]
    ThreadPool { message: String },
    /// Metrics error
    #[error("Metrics error: {message}")]
    Metrics { message: String },
    /// Recovery error
    #[error("Recovery error: {message}")]
    Recovery { message: String },

    /// Unknown error
    #[error("Unknown error: {message}")]
    Unknown { message: String },
}

impl StorageError {
    /// create a new compression error
    pub fn compression<S: Into<String>>(message: S) -> Self {
        Self::Compression {
            message: message.into(),
        }
    }

    /// create a new decompression error
    pub fn decompression<S: Into<String>>(message: S) -> Self {
        Self::Decompression {
            message: message.into(),
        }
    }

    /// create a new corruption error
    pub fn corruption<S: Into<String>>(message: S) -> Self {
        Self::Corruption {
            message: message.into(),
        }
    }

    /// create a new lock failed error
    pub fn lock_failed<S: Into<String>>(message: S) -> Self {
        Self::LockFailed {
            message: message.into(),
        }
    }

    /// create a new WAL error
    pub fn wal<S: Into<String>>(message: S) -> Self {
        Self::Wal {
            message: message.into(),
        }
    }

    /// create a new compaction error
    pub fn compaction<S: Into<String>>(message: S) -> Self {
        Self::Compaction {
            message: message.into(),
        }
    }

    /// create a new configuration error
    pub fn config<S: Into<String>>(message: S) -> Self {
        Self::Config {
            message: message.into(),
        }
    }

    /// create a new memory error
    pub fn memory<S: Into<String>>(message: S) -> Self {
        Self::Memory {
            message: message.into(),
        }
    }

    /// create a new invalid file format error
    pub fn invalid_file_format<S: Into<String>>(file_type: S, path: S) -> Self {
        Self::InvalidFileFormat {
            file_type: file_type.into(),
            path: path.into(),
        }
    }

    /// create a new timeout error
    pub fn timeout(duration: std::time::Duration) -> Self {
        Self::Timeout { duration }
    }

    /// create a new resource exhauted error
    pub fn resource_exhausted<S: Into<String>>(resource: S) -> Self {
        Self::ResourceExhausted {
            resource: resource.into(),
        }
    }

    /// create a new invalid operation error
    pub fn invalid_operation<S: Into<String>>(message: S) -> Self {
        Self::InvalidOperation {
            message: message.into(),
        }
    }

    /// create a new thread pool error
    pub fn thread_pool<S: Into<String>>(message: S) -> Self {
        Self::ThreadPool {
            message: message.into(),
        }
    }

    /// create a new metrics error
    pub fn metrics<S: Into<String>>(message: S) -> Self {
        Self::Metrics {
            message: message.into(),
        }
    }

    /// create a new recovery error
    pub fn recovery<S: Into<String>>(message: S) -> Self {
        Self::Recovery {
            message: message.into(),
        }
    }

    /// create a new unknown error
    pub fn unknown<S: Into<String>>(message: S) -> Self {
        Self::Unknown {
            message: message.into(),
        }
    }

    /// check if this error is retry-able
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            StorageError::Io(_)
                | StorageError::LockFailed { .. }
                | StorageError::Timeout { .. }
                | StorageError::ResourceExhausted { .. }
                | StorageError::TooManyFiles { .. }
                | StorageError::ThreadPool { .. }
        )
    }

    /// check if this error indicates corruption
    pub fn is_corrupt(&self) -> bool {
        matches!(
            self,
            StorageError::Corruption { .. }
                | StorageError::ChecksumMismatch { .. }
                | StorageError::InvalidFileFormat { .. }
        )
    }

    /// check if this error user's fault (client)
    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            StorageError::InvalidKeySize { .. }
                | StorageError::InvalidValueSize { .. }
                | StorageError::KeyNotFound
                | StorageError::ReadOnly
                | StorageError::Closed
                | StorageError::InvalidOperation { .. }
                | StorageError::Config { .. }
        )
    }

    /// check if this error system (server)
    pub fn is_server_error(&self) -> bool {
        matches!(
            self,
            StorageError::Io(_)
                | StorageError::Serialization(_)
                | StorageError::MessagePack(_)
                | StorageError::MessagePackDecode(_)
                | StorageError::Compression { .. }
                | StorageError::Decompression { .. }
                | StorageError::Corruption { .. }
                | StorageError::LockFailed { .. }
                | StorageError::Wal { .. }
                | StorageError::Compaction { .. }
                | StorageError::Memory { .. }
                | StorageError::TooManyFiles { .. }
                | StorageError::Timeout { .. }
                | StorageError::ResourceExhausted { .. }
                | StorageError::ThreadPool { .. }
                | StorageError::Metrics { .. }
                | StorageError::Recovery { .. }
                | StorageError::Unknown { .. }
        )
    }

    /// get the error category as a string
    pub fn category(&self) -> &'static str {
        match self {
            StorageError::Io(_) => "io",
            StorageError::Serialization(_) => "serialization",
            StorageError::MessagePack(_) => "messagepack",
            StorageError::MessagePackDecode(_) => "messagepack_decode",
            StorageError::Compression { .. } => "compression",
            StorageError::Decompression { .. } => "decompression",
            StorageError::ChecksumMismatch { .. } => "checksum",
            StorageError::InvalidKeySize { .. } => "invalid_key_size",
            StorageError::InvalidValueSize { .. } => "invalid_value_size",
            StorageError::Corruption { .. } => "corruption",
            StorageError::LockFailed { .. } => "lock_failed",
            StorageError::Wal { .. } => "wal",
            StorageError::Compaction { .. } => "compaction",
            StorageError::Config { .. } => "config",
            StorageError::Timeout { .. } => "timeout",
            StorageError::Memory { .. } => "memory",
            StorageError::TooManyFiles { .. } => "too_many_files",
            StorageError::InvalidFileFormat { .. } => "invalid_file_format",
            StorageError::KeyNotFound => "key_not_found",
            StorageError::ReadOnly => "read_only",
            StorageError::Closed => "closed",
            StorageError::ResourceExhausted { .. } => "resource_exhausted",
            StorageError::InvalidOperation { .. } => "invalid_operation",
            StorageError::ThreadPool { .. } => "thread_pool",
            StorageError::Metrics { .. } => "metrics",
            StorageError::Recovery { .. } => "recovery",
            StorageError::Unknown { .. } => "unknown",
        }
    }
}

/// convert from std::io::ErrorKind to StorageError
impl From<std::io::ErrorKind> for StorageError {
    fn from(value: std::io::ErrorKind) -> Self {
        StorageError::Io(std::io::Error::from(value))
    }
}

/// convert lock errors
impl From<AcquireError> for StorageError {
    fn from(_: AcquireError) -> Self {
        StorageError::lock_failed("failed to acquire lock")
    }
}

/// convert from channel send errors
impl<T> From<crossbeam_channel::SendError<T>> for StorageError {
    fn from(_: crossbeam_channel::SendError<T>) -> Self {
        StorageError::thread_pool("failed to send message to thread pool")
    }
}

/// convert from channel receive errors
impl From<crossbeam_channel::RecvError> for StorageError {
    fn from(_: crossbeam_channel::RecvError) -> Self {
        StorageError::thread_pool("failed to receive message from thread pool")
    }
}

/// convert from channel receive timeout errors
impl From<crossbeam_channel::RecvTimeoutError> for StorageError {
    fn from(err: crossbeam_channel::RecvTimeoutError) -> Self {
        match err {
            crossbeam_channel::RecvTimeoutError::Timeout => {
                StorageError::timeout(std::time::Duration::from_secs(1))
            }
            crossbeam_channel::RecvTimeoutError::Disconnected => {
                StorageError::thread_pool("channel disconnected")
            }
        }
    }
}
