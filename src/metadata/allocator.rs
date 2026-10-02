//! Private counter encoding and conditional allocation; never a component KV API.
use super::{MetadataError, Result};
use async_trait::async_trait;

const MAX_ATTEMPTS: usize = 128;

#[derive(Clone, Copy)]
pub(super) enum IdKind {
    Database,
    User,
}

impl IdKind {
    pub(super) fn key(self) -> &'static str {
        match self {
            Self::Database => "/catalog/allocator/database",
            Self::User => "/catalog/allocator/user",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CounterRecord {
    pub(super) value: Vec<u8>,
    // Backend revision, not the counter value or an allocated object ID.
    pub(super) version: i64,
}

/// Private, counter-only transport. Versions are not part of stored value bytes.
#[async_trait]
pub(super) trait CounterStore: Send + Sync {
    /// Absence permits first allocation in new state. Durable integrations must
    /// reject unexpected counter loss alongside existing records, not silently
    /// turn corruption into permission to reuse IDs.
    async fn fetch_counter(&self, kind: IdKind) -> Result<Option<CounterRecord>>;

    /// `None` means create only if absent; `Some(v)` means replace only version v.
    /// True confirms this write; false proves a conflict with no mutation by this
    /// attempt. Errors, including uncertain outcomes, must never become false.
    async fn store_counter(
        &self,
        kind: IdKind,
        expected_version: Option<i64>,
        value: [u8; 4],
    ) -> Result<bool>;
}

pub(super) async fn allocate_id(store: &impl CounterStore, kind: IdKind) -> Result<u32> {
    for _ in 0..MAX_ATTEMPTS {
        let record = store.fetch_counter(kind).await?;
        let next = next_id(record.as_ref().map(|record| record.value.as_slice()))?;
        if store
            .store_counter(
                kind,
                record.map(|record| record.version),
                next.to_be_bytes(),
            )
            .await?
        {
            return Ok(next);
        }
        // Retry only a confirmed conflict, rereading the current value/version.
        // Never infer ownership of an ID from read-back after an uncertain write.
    }
    Err(MetadataError::AllocationContended)
}

fn next_id(value: Option<&[u8]>) -> Result<u32> {
    let previous = match value {
        None => 0,
        Some(bytes) => u32::from_be_bytes(bytes.try_into().map_err(|_| {
            MetadataError::InvalidRecord("allocator value must contain exactly four bytes")
        })?),
    };
    previous.checked_add(1).ok_or(MetadataError::IdExhausted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::MemoryMetadata;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn binary_counter_is_big_endian_and_reserves_zero() {
        assert_eq!(next_id(None).unwrap(), 1);
        for (bytes, expected) in [
            ([0, 0, 0, 0], 1),
            ([0, 0, 0, 1], 2),
            ([0, 0, 0, 255], 256),
            ([0, 0, 255, 255], 65536),
            ([1, 2, 3, 4], 0x01020305),
            ([255, 255, 255, 254], u32::MAX),
        ] {
            assert_eq!(next_id(Some(&bytes)).unwrap(), expected);
        }
    }

    #[test]
    fn malformed_values_and_exhaustion_are_not_reset() {
        for length in [0, 1, 2, 3, 5, 8] {
            assert!(matches!(
                next_id(Some(&vec![0; length])),
                Err(MetadataError::InvalidRecord(_))
            ));
        }
        assert!(matches!(
            next_id(Some(&u32::MAX.to_be_bytes())),
            Err(MetadataError::IdExhausted)
        ));
    }

    #[test]
    fn counters_have_separate_exact_keys() {
        assert_eq!(IdKind::Database.key(), "/catalog/allocator/database");
        assert_eq!(IdKind::User.key(), "/catalog/allocator/user");
    }

    // Scripted backend outcomes exercise conflict and uncertain/error handling
    // without timing-dependent races or a durable backend dependency.
    struct ScriptedStore {
        // Immutable state
        scenario: Scenario,

        // Mutable state
        calls: Mutex<(usize, usize)>,
    }

    #[derive(Clone, Copy)]
    enum Scenario {
        ConflictThenSuccess,
        AlwaysConflict,
        ReadError,
        WriteError,
    }

    impl ScriptedStore {
        fn new(scenario: Scenario) -> Self {
            Self {
                scenario,
                calls: Mutex::new((0, 0)),
            }
        }
    }

    #[async_trait]
    impl CounterStore for ScriptedStore {
        async fn fetch_counter(&self, _: IdKind) -> Result<Option<CounterRecord>> {
            let mut calls = self.calls.lock().unwrap();
            calls.0 += 1;
            if matches!(self.scenario, Scenario::ReadError) {
                return Err(MetadataError::Closed);
            }
            if calls.0 == 1 {
                Ok(None)
            } else {
                // Simulate another caller winning creation. Backend revision
                // intentionally differs from the stored high-water mark.
                Ok(Some(CounterRecord {
                    value: 41u32.to_be_bytes().to_vec(),
                    version: 7,
                }))
            }
        }

        async fn store_counter(
            &self,
            _: IdKind,
            expected_version: Option<i64>,
            value: [u8; 4],
        ) -> Result<bool> {
            let mut calls = self.calls.lock().unwrap();
            calls.1 += 1;
            if calls.1 == 1 {
                assert_eq!(expected_version, None);
                assert_eq!(value, [0, 0, 0, 1]);
            } else {
                assert_eq!(expected_version, Some(7));
                assert_eq!(value, [0, 0, 0, 42]);
            }
            match self.scenario {
                Scenario::ConflictThenSuccess => Ok(calls.1 == 2),
                Scenario::AlwaysConflict => Ok(false),
                Scenario::WriteError => Err(MetadataError::MemoryStatePoisoned),
                Scenario::ReadError => panic!("write attempted after read failure"),
            }
        }
    }

    #[tokio::test]
    async fn conflict_rereads_value_and_version_before_returning_its_own_id() {
        let store = ScriptedStore::new(Scenario::ConflictThenSuccess);
        assert_eq!(allocate_id(&store, IdKind::Database).await.unwrap(), 42);
        assert_eq!(*store.calls.lock().unwrap(), (2, 2));
    }

    #[tokio::test]
    async fn conflicts_are_bounded() {
        let store = ScriptedStore::new(Scenario::AlwaysConflict);
        assert!(matches!(
            allocate_id(&store, IdKind::User).await,
            Err(MetadataError::AllocationContended)
        ));
        assert_eq!(*store.calls.lock().unwrap(), (MAX_ATTEMPTS, MAX_ATTEMPTS));
    }

    #[tokio::test]
    async fn errors_stop_without_retry_or_read_back() {
        let store = ScriptedStore::new(Scenario::ReadError);
        assert!(matches!(
            allocate_id(&store, IdKind::User).await,
            Err(MetadataError::Closed)
        ));
        assert_eq!(*store.calls.lock().unwrap(), (1, 0));
        let store = ScriptedStore::new(Scenario::WriteError);
        assert!(matches!(
            allocate_id(&store, IdKind::User).await,
            Err(MetadataError::MemoryStatePoisoned)
        ));
        assert_eq!(*store.calls.lock().unwrap(), (1, 1));
    }

    struct LostReplyStore {
        // Control state
        fail_once: AtomicBool,

        // Immutable state
        inner: MemoryMetadata,
    }

    #[async_trait]
    impl CounterStore for LostReplyStore {
        async fn fetch_counter(&self, kind: IdKind) -> Result<Option<CounterRecord>> {
            self.inner.fetch_counter(kind).await
        }

        async fn store_counter(
            &self,
            kind: IdKind,
            expected_version: Option<i64>,
            value: [u8; 4],
        ) -> Result<bool> {
            let committed = self
                .inner
                .store_counter(kind, expected_version, value)
                .await?;
            if committed && self.fail_once.swap(false, Ordering::AcqRel) {
                // Inject a failure after commit. This isn't a real poisoned lock;
                // concrete durable transport errors arrive with that backend.
                return Err(MetadataError::MemoryStatePoisoned);
            }
            Ok(committed)
        }
    }

    #[tokio::test]
    async fn lost_write_reply_consumes_an_id_without_returning_or_reusing_it() {
        let store = LostReplyStore {
            fail_once: AtomicBool::new(true),
            inner: MemoryMetadata::new(),
        };
        assert!(matches!(
            allocate_id(&store, IdKind::Database).await,
            Err(MetadataError::MemoryStatePoisoned)
        ));
        assert_eq!(
            store
                .inner
                .fetch_counter(IdKind::Database)
                .await
                .unwrap()
                .unwrap()
                .value,
            [0, 0, 0, 1]
        );
        assert_eq!(allocate_id(&store, IdKind::Database).await.unwrap(), 2);
    }
}
