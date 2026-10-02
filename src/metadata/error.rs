use prost::DecodeError;
use thiserror::Error;

/// Typed failures for the currently implemented metadata operations.
///
/// Additional operation/backend failures will be added alongside those features.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MetadataError {
    /// The stored bytes are not a valid Protobuf record.
    #[error("metadata record is not valid Protobuf")]
    Decode(#[from] DecodeError),
    /// The message decodes but violates a required metadata invariant.
    #[error("invalid metadata record: {0}")]
    InvalidRecord(&'static str),
    /// A writer panicked while holding the in-memory state lock.
    #[error("in-memory metadata state is poisoned")]
    MemoryStatePoisoned,
    /// This client has been closed and cannot admit another operation.
    #[error("metadata client is closed")]
    Closed,
}
