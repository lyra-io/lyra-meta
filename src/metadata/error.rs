use oxia::OxiaError;
use prost::DecodeError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, MetadataError>;

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("invalid name: expected 1 to 63 UTF-8 bytes without NUL")]
    InvalidName,

    #[error("invalid stored metadata: {0}")]
    InvalidRecord(&'static str),

    #[error("metadata is not initialized")]
    NotInitialized,

    #[error("metadata is already initialized")]
    AlreadyInitialized,

    #[error("initialization is incomplete; automatic resumption is disabled")]
    IncompleteInitialization,

    #[error("metadata integrity check failed: {0}")]
    Integrity(&'static str),

    #[error("metadata write outcome is uncertain; further mutations are blocked")]
    UncertainWrite,

    #[error("object already exists")]
    AlreadyExists,

    #[error("object does not exist")]
    NotFound,

    #[error("reserved system object cannot be modified")]
    Reserved,

    #[error("user still owns a database")]
    OwnerInUse,

    #[error("metadata backend is closed")]
    Closed,
    #[error("invalid metadata key {key:?}: {reason}")]
    InvalidKey { key: String, reason: &'static str },

    #[error("metadata write condition was not satisfied for key {0:?}")]
    Conflict(String),

    #[error("metadata counter {0:?} is exhausted")]
    CounterExhausted(String),

    #[error("user {0:?} password metadata must contain a valid SCRAM value")]
    InvalidUserPassword(String),

    #[error("failed to decode protobuf metadata: {0}")]
    Decode(#[from] DecodeError),

    #[error("Oxia metadata operation failed: {0}")]
    Oxia(#[source] OxiaError),
}

impl From<OxiaError> for MetadataError {
    fn from(error: OxiaError) -> Self {
        Self::Oxia(error)
    }
}
