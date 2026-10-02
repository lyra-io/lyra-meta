use super::{MetadataError, Result};
use crate::proto::pb_meta::Instance;
use async_trait::async_trait;

/// Object-safe metadata operations, shareable as `Arc<dyn Metadata>`.
///
/// This slice defines initialization reads and local client closure. Bootstrap
/// and registration methods are added with their implementations, not as stubs.
/// Reads never initialize, repair, or otherwise mutate stored records.
#[async_trait]
pub trait Metadata: Send + Sync {
    /// Fetch and validate the initialization marker.
    ///
    /// `Ok(None)` means the record is absent. A malformed Protobuf value, an
    /// absent `initialized` field, unavailable storage, or a closed client is an
    /// error, never a missing record. Each call reads current state, not a cache.
    async fn fetch_instance(&self) -> Result<Option<Instance>>;

    /// Read whether the marker explicitly records completed initialization.
    ///
    /// An absent record or explicit false returns false. All errors propagate;
    /// an absent field in an existing record is invalid, not false. True is a
    /// marker observation, not validation of bootstrap objects or a writer lock.
    async fn is_initialized(&self) -> Result<bool> {
        match self.fetch_instance().await? {
            None => Ok(false),
            Some(instance) => instance.initialized.ok_or(MetadataError::InvalidRecord(
                "initialization flag is absent",
            )),
        }
    }

    /// Idempotently close this client without deleting stored records.
    ///
    /// Reads begun after successful closure return [`MetadataError::Closed`].
    /// A read admitted before a concurrent close may finish with its snapshot.
    /// Closing one independently constructed client does not close another.
    async fn close(&self) -> Result<()>;
}
