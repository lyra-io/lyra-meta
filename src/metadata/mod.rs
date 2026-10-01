mod api;
mod engine;
mod error;
#[cfg(test)]
mod fault_tests;
mod keys;
mod memory;
pub mod oxia;
mod registration;
mod storage;
mod telemetry;
mod validation;

pub use api::{Metadata, MetadataRecord, MetadataVersion, UserInfo};
pub use error::{MetadataError, Result};
pub use memory::MemoryMetadata;
pub use registration::{ComponentIdentity, Registration};
pub use validation::{normalize_sql_identifier, validate_name};

pub const DEFAULT_DATABASE_NAME: &str = "public";
pub const SYSTEM_DATABASE_NAME: &str = "lyrasys";
pub const SYSTEM_USER_NAME: &str = "lyrasys";
pub const DEFAULT_SCHEMA_NAME: &str = "public";
