use super::allocator::{CounterRecord, CounterStore, IdKind, allocate_id};
use super::validation::decode_instance;
use super::{Metadata, MetadataError, Result};
use crate::proto::pb_meta::Instance;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

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
    counters: RwLock<BTreeMap<&'static str, CounterRecord>>,
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
        allocate_id(self, IdKind::Database).await
    }

    async fn allocate_user_id(&self) -> Result<u32> {
        allocate_id(self, IdKind::User).await
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }
}

#[async_trait]
impl CounterStore for MemoryMetadata {
    async fn fetch_counter(&self, kind: IdKind) -> Result<Option<CounterRecord>> {
        self.check_open0()?;
        let counters = self
            .counters
            .read()
            .map_err(|_| MetadataError::MemoryStatePoisoned)?;
        self.check_open0()?;
        Ok(counters.get(kind.key()).cloned())
    }

    async fn store_counter(
        &self,
        kind: IdKind,
        expected_version: Option<i64>,
        value: [u8; 4],
    ) -> Result<bool> {
        self.check_open0()?;
        let mut counters = self
            .counters
            .write()
            .map_err(|_| MetadataError::MemoryStatePoisoned)?;
        self.check_open0()?;
        let current_version = counters.get(kind.key()).map(|record| record.version);
        if current_version != expected_version {
            return Ok(false);
        }
        let version = match current_version {
            None => 0,
            Some(version) => version.checked_add(1).ok_or(MetadataError::InvalidRecord(
                "allocator backend revision is exhausted",
            ))?,
        };
        counters.insert(
            kind.key(),
            CounterRecord {
                value: value.to_vec(),
                version,
            },
        );
        Ok(true)
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
    async fn counters_use_create_only_and_version_conditional_writes() {
        let metadata = MemoryMetadata::new();
        assert!(
            metadata
                .fetch_counter(IdKind::Database)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            metadata
                .store_counter(IdKind::Database, None, [0, 0, 0, 41])
                .await
                .unwrap()
        );
        assert!(
            !metadata
                .store_counter(IdKind::Database, None, [0, 0, 0, 1])
                .await
                .unwrap()
        );
        let first = metadata
            .fetch_counter(IdKind::Database)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.value, [0, 0, 0, 41]);
        assert_eq!(first.version, 0);

        assert_eq!(metadata.allocate_database_id().await.unwrap(), 42);
        assert!(
            !metadata
                .store_counter(IdKind::Database, Some(first.version), [0, 0, 0, 99])
                .await
                .unwrap()
        );
        let second = metadata
            .fetch_counter(IdKind::Database)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.value, [0, 0, 0, 42]);
        assert_eq!(second.version, 1);
        assert_eq!(metadata.allocate_user_id().await.unwrap(), 1);
        assert_eq!(metadata.counters.read().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn invalid_and_exhausted_counters_are_not_repaired_or_wrapped() {
        let metadata = MemoryMetadata::new();
        for bytes in [vec![], vec![0], vec![0; 3], vec![0; 5]] {
            let record = CounterRecord {
                value: bytes,
                version: 17,
            };
            metadata
                .counters
                .write()
                .unwrap()
                .insert(IdKind::Database.key(), record.clone());
            assert!(matches!(
                metadata.allocate_database_id().await,
                Err(MetadataError::InvalidRecord(_))
            ));
            assert_eq!(
                metadata.fetch_counter(IdKind::Database).await.unwrap(),
                Some(record)
            );
        }
        metadata.counters.write().unwrap().insert(
            IdKind::Database.key(),
            CounterRecord {
                value: (u32::MAX - 1).to_be_bytes().to_vec(),
                version: 18,
            },
        );
        assert_eq!(metadata.allocate_database_id().await.unwrap(), u32::MAX);
        let final_record = metadata.fetch_counter(IdKind::Database).await.unwrap();
        assert_eq!(final_record.as_ref().unwrap().value, [255; 4]);
        for _ in 0..2 {
            assert!(matches!(
                metadata.allocate_database_id().await,
                Err(MetadataError::IdExhausted)
            ));
            assert_eq!(
                metadata.fetch_counter(IdKind::Database).await.unwrap(),
                final_record
            );
        }
        // An exhausted database domain does not exhaust the separate user domain.
        assert_eq!(metadata.allocate_user_id().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn backend_revision_overflow_does_not_change_counter_bytes() {
        let metadata = MemoryMetadata::new();
        let record = CounterRecord {
            value: 1u32.to_be_bytes().to_vec(),
            version: i64::MAX,
        };
        metadata
            .counters
            .write()
            .unwrap()
            .insert(IdKind::User.key(), record.clone());
        assert!(matches!(
            metadata.allocate_user_id().await,
            Err(MetadataError::InvalidRecord(_))
        ));
        assert_eq!(
            metadata.fetch_counter(IdKind::User).await.unwrap(),
            Some(record)
        );
    }

    #[tokio::test]
    async fn close_retains_counters_and_rejects_reads_and_writes() {
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
        assert!(matches!(
            metadata
                .store_counter(IdKind::User, Some(0), [0, 0, 0, 2])
                .await,
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
