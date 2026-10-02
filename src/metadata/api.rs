use super::{MetadataError, Result};
use crate::proto::pb_meta::Instance;
use async_trait::async_trait;

/// Object-safe metadata operations, shareable as `Arc<dyn Metadata>`.
///
/// This slice defines initialization reads, ID reservation, and client closure.
/// Bootstrap and registration methods arrive with their implementations.
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

    /// Reserve the next nonzero database ID without creating a database record.
    ///
    /// Database and user counters are independent. A missing counter starts at
    /// one through a create-only write; existing counters advance conditionally.
    /// An ID is returned only after this call's write is confirmed. Failed or
    /// cancelled calls may consume IDs; IDs are not promised to be gapless and
    /// must never be reclaimed or counters reset when records are deleted.
    ///
    /// Malformed counters are errors, never a reason to reset to zero. Exhaustion
    /// returns [`MetadataError::IdExhausted`] without wrapping. A bounded run of
    /// conflicting writes returns [`MetadataError::AllocationContended`]; callers
    /// may retry that error. Other errors propagate without automatic write retry.
    /// Memory guarantees uniqueness only within its shared client state; it is
    /// not durable storage. No counter/key/version API is exposed to callers.
    async fn allocate_database_id(&self) -> Result<u32>;

    /// Reserve the next nonzero user ID without creating a user record.
    ///
    /// Uses a separate counter with the same guarantees and failure semantics as
    /// [`Self::allocate_database_id`]. Allocating IDs does not initialize metadata.
    async fn allocate_user_id(&self) -> Result<u32>;

    /// Idempotently close this client without deleting stored records or counters.
    ///
    /// Operations begun after successful closure return [`MetadataError::Closed`].
    /// An operation admitted before a concurrent close may finish; a confirmed
    /// allocation is never rolled back by closing the client.
    /// Closing one independently constructed client does not close another.
    async fn close(&self) -> Result<()>;
}
