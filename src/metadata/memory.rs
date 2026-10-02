use super::validation::decode_instance;
use super::{Metadata, MetadataError, Result};
use crate::proto::pb_meta::Instance;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

const DATABASE_ID_KEY: &str = "/catalog/allocator/database";
const USER_ID_KEY: &str = "/catalog/allocator/user";

/// In-memory metadata client for contract tests and local development.
///
/// Each constructor creates isolated empty state. Share one client with `Arc`
/// when callers should observe the same state and closure. Construction requires
/// no async runtime and creates no workers, listeners, or global telemetry.
///
/// This slice exposes marker reads and ID reservation, not record creation.
/// The initialization marker cannot be set through the public API; the later
/// bootstrap implementation must validate all required records before publishing
/// completion. Dropping this client discards its memory.
#[derive(Default)]
pub struct MemoryMetadata {
    // Control state
    closed: AtomicBool,

    // Mutable state
    instance: RwLock<Option<Vec<u8>>>,
    counters: RwLock<BTreeMap<&'static str, Vec<u8>>>,
}

impl MemoryMetadata {
    /// Construct an open, empty client without initializing metadata.
    pub fn new() -> Self {
        Self::default()
    }

    fn check_open0(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(MetadataError::Closed)
        } else {
            Ok(())
        }
    }

    fn allocate_id0(&self, key: &'static str) -> Result<u32> {
        self.check_open0()?;
        let mut counters = self
            .counters
            .write()
            .map_err(|_| MetadataError::MemoryStatePoisoned)?;
        self.check_open0()?;
        // Keep the whole read/check/increment/write under one lock. Memory needs
        // neither transport revisions nor a CAS retry loop, and no guard crosses
        // an await. Invalid/exhausted values are rejected before any mutation.
        let previous = match counters.get(key) {
            None => 0,
            Some(bytes) => u32::from_be_bytes(bytes.as_slice().try_into().map_err(|_| {
                MetadataError::InvalidRecord("allocator value must contain exactly four bytes")
            })?),
        };
        let next = previous.checked_add(1).ok_or(MetadataError::IdExhausted)?;
        counters.insert(key, next.to_be_bytes().to_vec());
        Ok(next)
    }
}

#[async_trait]
impl Metadata for MemoryMetadata {
    async fn fetch_instance(&self) -> Result<Option<Instance>> {
        self.check_open0()?;
        let bytes = {
            let state = self
                .instance
                .read()
                .map_err(|_| MetadataError::MemoryStatePoisoned)?;
            self.check_open0()?;
            state.clone()
        };
        // Decode a snapshot without holding a lock. Never recover a poisoned
        // lock as an empty record or retain a blocking guard across an await.
        bytes.as_deref().map(decode_instance).transpose()
    }

    async fn allocate_database_id(&self) -> Result<u32> {
        self.allocate_id0(DATABASE_ID_KEY)
    }

    async fn allocate_user_id(&self) -> Result<u32> {
        self.allocate_id0(USER_ID_KEY)
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use std::panic::catch_unwind;

    #[tokio::test]
    async fn valid_markers_are_read_fresh_without_writes() {
        let metadata = MemoryMetadata::new();
        assert_eq!(metadata.fetch_instance().await.unwrap(), None);
        assert!(!metadata.is_initialized().await.unwrap());
        assert_eq!(*metadata.instance.read().unwrap(), None);

        for initialized in [false, true, false] {
            let expected = Instance {
                initialized: Some(initialized),
            };
            let bytes = expected.encode_to_vec();
            *metadata.instance.write().unwrap() = Some(bytes.clone());
            assert_eq!(metadata.fetch_instance().await.unwrap(), Some(expected));
            assert_eq!(metadata.is_initialized().await.unwrap(), initialized);
            assert_eq!(*metadata.instance.read().unwrap(), Some(bytes));
        }
    }

    #[tokio::test]
    async fn an_unset_flag_is_invalid_not_absent_or_false() {
        let metadata = MemoryMetadata::new();
        for bytes in [vec![], vec![0x10, 0x01]] {
            *metadata.instance.write().unwrap() = Some(bytes.clone());
            assert!(matches!(
                metadata.fetch_instance().await,
                Err(MetadataError::InvalidRecord(_))
            ));
            assert!(matches!(
                metadata.is_initialized().await,
                Err(MetadataError::InvalidRecord(_))
            ));
            assert_eq!(*metadata.instance.read().unwrap(), Some(bytes));
        }
    }

    #[tokio::test]
    async fn malformed_bytes_propagate_errors_without_repair() {
        let metadata = MemoryMetadata::new();
        for bytes in [vec![0x08], vec![0x0a, 0x00], vec![0x00]] {
            *metadata.instance.write().unwrap() = Some(bytes.clone());
            assert!(matches!(
                metadata.fetch_instance().await,
                Err(MetadataError::Decode(_))
            ));
            assert!(matches!(
                metadata.is_initialized().await,
                Err(MetadataError::Decode(_))
            ));
            assert_eq!(*metadata.instance.read().unwrap(), Some(bytes));
        }
    }

    #[tokio::test]
    async fn close_retains_records_and_takes_precedence_over_invalid_data() {
        let metadata = MemoryMetadata::new();
        let bytes = vec![0x08];
        *metadata.instance.write().unwrap() = Some(bytes.clone());
        metadata.close().await.unwrap();
        metadata.close().await.unwrap();
        assert!(matches!(
            metadata.fetch_instance().await,
            Err(MetadataError::Closed)
        ));
        assert!(matches!(
            metadata.is_initialized().await,
            Err(MetadataError::Closed)
        ));
        assert_eq!(*metadata.instance.read().unwrap(), Some(bytes));
    }

    #[tokio::test]
    async fn poisoned_storage_is_an_error_and_can_still_be_closed() {
        let metadata = MemoryMetadata::new();
        let panic = catch_unwind(|| {
            let _guard = metadata.instance.write().unwrap();
            panic!("injected writer failure");
        });
        assert!(panic.is_err());
        assert!(matches!(
            metadata.fetch_instance().await,
            Err(MetadataError::MemoryStatePoisoned)
        ));
        assert!(matches!(
            metadata.is_initialized().await,
            Err(MetadataError::MemoryStatePoisoned)
        ));
        metadata.close().await.unwrap();
        assert!(matches!(
            metadata.fetch_instance().await,
            Err(MetadataError::Closed)
        ));
    }

    #[tokio::test]
    async fn counters_use_separate_exact_keys_and_four_byte_values() {
        let metadata = MemoryMetadata::new();
        assert!(metadata.counters.read().unwrap().is_empty());
        assert_eq!(metadata.allocate_database_id().await.unwrap(), 1);
        assert_eq!(metadata.allocate_database_id().await.unwrap(), 2);
        assert_eq!(metadata.allocate_user_id().await.unwrap(), 1);
        let counters = metadata.counters.read().unwrap();
        assert_eq!(counters.len(), 2);
        assert_eq!(counters["/catalog/allocator/database"], [0, 0, 0, 2]);
        assert_eq!(counters["/catalog/allocator/user"], [0, 0, 0, 1]);
    }

    #[tokio::test]
    async fn allocation_reads_and_writes_big_endian_counters() {
        let metadata = MemoryMetadata::new();
        for (bytes, expected) in [
            ([0, 0, 0, 0], 1u32),
            ([0, 0, 0, 1], 2),
            ([0, 0, 0, 255], 256),
            ([0, 0, 255, 255], 65536),
            ([1, 2, 3, 4], 0x01020305),
            ([255, 255, 255, 254], u32::MAX),
        ] {
            metadata
                .counters
                .write()
                .unwrap()
                .insert(DATABASE_ID_KEY, bytes.to_vec());
            assert_eq!(metadata.allocate_database_id().await.unwrap(), expected);
            assert_eq!(
                metadata.counters.read().unwrap()[DATABASE_ID_KEY],
                expected.to_be_bytes()
            );
        }
    }

    #[tokio::test]
    async fn invalid_and_exhausted_counters_are_not_repaired_or_wrapped() {
        let metadata = MemoryMetadata::new();
        for length in [0, 1, 2, 3, 5, 8] {
            let bytes = vec![0; length];
            metadata
                .counters
                .write()
                .unwrap()
                .insert(DATABASE_ID_KEY, bytes.clone());
            assert!(matches!(
                metadata.allocate_database_id().await,
                Err(MetadataError::InvalidRecord(_))
            ));
            assert_eq!(metadata.counters.read().unwrap()[DATABASE_ID_KEY], bytes);
        }
        metadata
            .counters
            .write()
            .unwrap()
            .insert(DATABASE_ID_KEY, (u32::MAX - 1).to_be_bytes().to_vec());
        assert_eq!(metadata.allocate_database_id().await.unwrap(), u32::MAX);
        assert_eq!(metadata.counters.read().unwrap()[DATABASE_ID_KEY], [255; 4]);
        for _ in 0..2 {
            assert!(matches!(
                metadata.allocate_database_id().await,
                Err(MetadataError::IdExhausted)
            ));
            assert_eq!(metadata.counters.read().unwrap()[DATABASE_ID_KEY], [255; 4]);
        }
        // An exhausted database domain does not exhaust the separate user domain.
        assert_eq!(metadata.allocate_user_id().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn close_retains_counters_and_rejects_allocation() {
        let metadata = MemoryMetadata::new();
        assert_eq!(metadata.allocate_database_id().await.unwrap(), 1);
        assert_eq!(metadata.allocate_user_id().await.unwrap(), 1);
        let before = metadata.counters.read().unwrap().clone();
        metadata.close().await.unwrap();
        metadata.close().await.unwrap();
        assert!(matches!(
            metadata.allocate_database_id().await,
            Err(MetadataError::Closed)
        ));
        assert!(matches!(
            metadata.allocate_user_id().await,
            Err(MetadataError::Closed)
        ));
        assert_eq!(*metadata.counters.read().unwrap(), before);
    }

    #[tokio::test]
    async fn poisoned_counter_storage_errors_and_can_be_closed() {
        let metadata = MemoryMetadata::new();
        assert!(
            catch_unwind(|| {
                let _guard = metadata.counters.write().unwrap();
                panic!("injected counter writer failure");
            })
            .is_err()
        );
        assert!(matches!(
            metadata.allocate_database_id().await,
            Err(MetadataError::MemoryStatePoisoned)
        ));
        assert!(matches!(
            metadata.allocate_user_id().await,
            Err(MetadataError::MemoryStatePoisoned)
        ));
        metadata.close().await.unwrap();
        assert!(matches!(
            metadata.allocate_user_id().await,
            Err(MetadataError::Closed)
        ));
    }
}
