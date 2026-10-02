//! Typed metadata operations. Memory is for testing, not a durable backend or
//! an automatic fallback when a durable backend is unavailable.

mod allocator;
mod api;
mod error;
mod memory;
mod validation;

pub use api::Metadata;
pub use error::MetadataError;
pub use memory::MemoryMetadata;

/// A typed metadata operation result; absence is represented separately.
pub type Result<T> = std::result::Result<T, MetadataError>;
