use super::validation::decode_instance;
use super::{Metadata, MetadataError, Result};
use crate::proto::pb_meta::Instance;
use async_trait::async_trait;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

/// In-memory metadata client for contract tests and local development.
///
/// Each constructor creates isolated empty state. Share one client with `Arc`
/// when callers should observe the same state and closure. Construction requires
/// no async runtime and creates no workers, listeners, or global telemetry.
///
/// This slice exposes reads only. The initialization marker cannot be set through
/// the public API; the later bootstrap implementation must validate all required
/// records before publishing completion. Dropping this client discards its memory.
#[derive(Default)]
pub struct MemoryMetadata {
    // Control state
    closed: AtomicBool,

    // Mutable state
    instance: RwLock<Option<Vec<u8>>>,
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
}
