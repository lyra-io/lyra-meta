use super::engine::Engine;
use super::keys::Collection;
use super::memory::MemoryStorage;
use super::storage::{Condition, Presence, PresenceEvents, Row, Storage};
use super::{MetadataError, Result};
use crate::proto::pb_meta::{Allocator, Database};
use crate::utils::verifier::make_verifier;
use async_trait::async_trait;
use futures_util::future::join_all;
use prost::Message;
use std::collections::HashSet;
use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::timeout;

#[derive(Default)]
struct FaultStorage {
    base: MemoryStorage,
    mode: AtomicU8,
    counter_fault: AtomicBool,
    allocations: AtomicUsize,
    fail_at: AtomicUsize,
    writes: AtomicUsize,
    presence_fault: AtomicU8,
    presence_writes: AtomicUsize,
}

impl FaultStorage {
    async fn finish<T>(&self, result: Result<T>) -> Result<T> {
        match self.mode.swap(0, Ordering::SeqCst) {
            1 => Err(MetadataError::Closed), // applied, reply lost
            3 => pending().await,            // applied, caller cancelled before reply
            _ => result,
        }
    }
}
#[async_trait]
impl Storage for FaultStorage {
    fn backend(&self) -> &'static str {
        "memory"
    }
    async fn get(&self, key: &str) -> Result<Option<Row>> {
        self.base.get(key).await
    }
    async fn scan(&self, first: &str, last: &str) -> Result<Vec<Row>> {
        self.base.scan(first, last).await
    }
    async fn put(
        &self,
        key: &str,
        value: Vec<u8>,
        condition: Condition,
        index: Option<(&str, &str)>,
    ) -> Result<Row> {
        let write = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_at.load(Ordering::SeqCst) == write {
            self.base.put(key, value, condition, index).await?;
            return Err(MetadataError::Timeout);
        }
        if key.starts_with("/catalog/allocator/") {
            self.allocations.fetch_add(1, Ordering::SeqCst);
            if !self.counter_fault.load(Ordering::SeqCst) {
                return self.base.put(key, value, condition, index).await;
            }
        }
        if self
            .mode
            .compare_exchange(2, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Err(MetadataError::Closed);
        }
        self.finish(self.base.put(key, value, condition, index).await)
            .await
    }
    async fn delete(&self, key: &str, version: i64) -> Result<()> {
        self.finish(self.base.delete(key, version).await).await
    }
    fn identity(&self) -> &str {
        self.base.identity()
    }
    async fn subscribe(&self, key: &str) -> Result<Box<dyn PresenceEvents>> {
        self.base.subscribe(key).await
    }
    async fn fetch_presence(&self, key: &str) -> Result<Option<Presence>> {
        match self.presence_fault.load(Ordering::Acquire) {
            1 => return Err(MetadataError::Timeout),
            2 => return pending().await,
            _ => {}
        }
        self.base.fetch_presence(key).await
    }
    async fn create_presence(&self, key: &str, value: Vec<u8>) -> Result<Presence> {
        self.presence_writes.fetch_add(1, Ordering::AcqRel);
        match self.presence_fault.swap(0, Ordering::AcqRel) {
            4 => {
                self.base.create_presence(key, value).await?;
                return Err(MetadataError::Timeout);
            }
            5 => return Err(MetadataError::Timeout),
            _ => {}
        }
        self.base.create_presence(key, value).await
    }
    async fn delete_presence(&self, key: &str, version: i64) -> Result<()> {
        self.base.delete_presence(key, version).await
    }
    async fn list_presence(&self) -> Result<Vec<Presence>> {
        self.base.list_presence().await
    }
    async fn close(&self) -> Result<()> {
        self.base.close().await
    }
}

#[tokio::test]
async fn startup_registration_failure_is_never_retried_or_claimed_clean_without_evidence() {
    for mode in [4, 5] {
        let store = Arc::new(FaultStorage::default());
        let engine = Engine::new(store.clone(), None);
        store.presence_fault.store(mode, Ordering::Release);
        assert!(engine.monitor.register().await.is_err());
        assert!(matches!(
            engine.monitor.register().await,
            Err(MetadataError::RegistrationAttempted)
        ));
        let close = engine.monitor.close().await;
        assert_eq!(
            close.is_ok(),
            mode == 4,
            "only the observed committed record can be reconciled"
        );
        assert_eq!(store.presence_writes.load(Ordering::Acquire), 1);
    }
}

#[tokio::test]
async fn fresh_registration_reads_bound_backend_and_queue_waits() {
    let store = Arc::new(FaultStorage::default());
    let engine = Engine::new(store.clone(), None);
    engine.monitor.register().await.unwrap();
    for mode in [1, 2] {
        store.presence_fault.store(mode, Ordering::Release);
        let read = timeout(Duration::from_millis(1200), engine.monitor.is_registered()).await;
        assert!(matches!(read.unwrap(), Err(MetadataError::Timeout)));
    }
    store.presence_fault.store(0, Ordering::Release);
    engine.monitor.close().await.unwrap();
}

async fn initialized() -> (Arc<FaultStorage>, Engine, u32) {
    let store = Arc::new(FaultStorage::default());
    let engine = Engine::new(store.clone(), None);
    engine
        .initialize(make_verifier("test-password").unwrap())
        .await
        .unwrap();
    let user = engine.fetch_user("lyrasys").await.unwrap().unwrap().id();
    (store, engine, user)
}

#[tokio::test]
async fn lost_write_replies_are_reconciled_without_repeating_allocations() {
    let (store, engine, owner) = initialized().await;
    store.mode.store(1, Ordering::SeqCst);
    let before = store.allocations.load(Ordering::SeqCst);
    let record = engine
        .create_database(Database::new("lost-create", owner))
        .await
        .unwrap();
    assert_eq!(store.allocations.load(Ordering::SeqCst), before + 1);
    store.mode.store(1, Ordering::SeqCst);
    let mut value = record.value().clone();
    value.allow_connections = Some(false);
    let record = engine
        .update_database(record.id(), value, record.version())
        .await
        .unwrap();
    assert!(
        !engine
            .fetch_database("lost-create")
            .await
            .unwrap()
            .unwrap()
            .value()
            .accepts_connections()
    );
    store.mode.store(1, Ordering::SeqCst);
    engine
        .delete_database(record.id(), record.version())
        .await
        .unwrap();
    assert!(
        engine
            .fetch_database("lost-create")
            .await
            .unwrap()
            .is_none()
    );
    engine
        .create_database(Database::new("after-reconciliation", owner))
        .await
        .unwrap();
}

#[tokio::test]
async fn ambiguous_and_cancelled_mutations_poison_the_writer() {
    for mode in [2, 3] {
        let (store, engine, owner) = initialized().await;
        store.mode.store(mode, Ordering::SeqCst);
        let result = timeout(
            Duration::from_millis(20),
            engine.create_database(Database::new("uncertain", owner)),
        )
        .await;
        if mode == 2 {
            assert!(matches!(
                result.unwrap(),
                Err(MetadataError::UncertainWrite)
            ));
        } else {
            assert!(result.is_err());
        }
        assert!(matches!(
            engine
                .create_database(Database::new("must-not-write", owner))
                .await,
            Err(MetadataError::UncertainWrite)
        ));
        assert!(
            engine
                .fetch_database("must-not-write")
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn independent_writers_atomically_claim_names_and_allocate_ids() {
    let (store, _, owner) = initialized().await;
    let writers: Vec<_> = (0..32).map(|_| Engine::new(store.clone(), None)).collect();
    let results = join_all(
        writers
            .iter()
            .map(|writer| writer.create_database(Database::new("same-name", owner))),
    )
    .await;
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert!(
        results
            .iter()
            .all(|r| r.is_ok() || matches!(r, Err(MetadataError::AlreadyExists)))
    );
    let records =
        join_all(writers.iter().enumerate().map(|(i, writer)| {
            writer.create_database(Database::new(format!("database-{i}"), owner))
        }))
        .await;
    let ids: HashSet<_> = records.into_iter().map(|r| r.unwrap().id()).collect();
    assert_eq!(ids.len(), writers.len());
    Engine::new(store.clone(), None).inventory0().await.unwrap();
}

#[tokio::test]
async fn uncertain_counter_replies_never_return_unconfirmed_ids() {
    for mode in [1, 2, 3] {
        let (store, engine, owner) = initialized().await;
        store.counter_fault.store(true, Ordering::SeqCst);
        store.mode.store(mode, Ordering::SeqCst);
        let result = timeout(
            Duration::from_millis(20),
            engine.create_database(Database::new("uncertain-counter", owner)),
        )
        .await;
        if mode == 3 {
            assert!(result.is_err());
        } else {
            assert!(matches!(
                result.unwrap(),
                Err(MetadataError::UncertainWrite)
            ));
        }
        assert!(
            engine
                .fetch_database("uncertain-counter")
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            engine
                .create_database(Database::new("blocked", owner))
                .await,
            Err(MetadataError::UncertainWrite)
        ));
        let restarted = Engine::new(store, None);
        let next = restarted
            .create_database(Database::new("after-restart", owner))
            .await
            .unwrap();
        assert_eq!(next.id(), if mode == 2 { 3 } else { 4 });
    }
}

#[tokio::test]
async fn durable_counters_survive_tail_deletion_and_reject_exhaustion_or_loss() {
    let (store, engine, owner) = initialized().await;
    let first = engine
        .create_database(Database::new("tail", owner))
        .await
        .unwrap();
    engine
        .delete_database(first.id(), first.version())
        .await
        .unwrap();
    let restarted = Engine::new(store.clone(), None);
    let second = restarted
        .create_database(Database::new("tail", owner))
        .await
        .unwrap();
    assert!(second.id() > first.id());
    let user = restarted
        .create_user("tail", make_verifier("test-password").unwrap())
        .await
        .unwrap();
    restarted
        .delete_user(user.id(), user.version())
        .await
        .unwrap();
    let next_user = restarted
        .create_user("tail", make_verifier("test-password").unwrap())
        .await
        .unwrap();
    assert!(next_user.id() > user.id());
    let key = Collection::Databases.allocator();
    let row = store.get(key).await.unwrap().unwrap();
    let row = store
        .put(
            key,
            Allocator {
                last_allocated: u32::MAX,
            }
            .encode_to_vec(),
            Condition::Version(row.version),
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        restarted
            .create_database(Database::new("overflow", owner))
            .await,
        Err(MetadataError::CounterExhausted(_))
    ));
    store.delete(key, row.version).await.unwrap();
    assert!(matches!(
        restarted
            .create_database(Database::new("missing-counter", owner))
            .await,
        Err(MetadataError::Integrity(_))
    ));
    assert!(restarted.inventory0().await.is_err());
    assert!(store.get(key).await.unwrap().is_none());
}

#[tokio::test]
async fn bootstrap_resumes_after_every_lost_write_reply() {
    for fail_at in 1..=10 {
        let store = Arc::new(FaultStorage::default());
        store.fail_at.store(fail_at, Ordering::SeqCst);
        let engine = Engine::new(store.clone(), None);
        let first = make_verifier("first-bootstrap-password").unwrap();
        assert!(
            engine.initialize(first.clone()).await.is_err(),
            "write {fail_at}"
        );
        let existing = engine.fetch_user_verifier("lyrasys").await.unwrap();
        store.fail_at.store(0, Ordering::SeqCst);
        engine
            .initialize(make_verifier("retry-is-not-a-reset").unwrap())
            .await
            .unwrap();
        assert!(engine.is_initialized().await.unwrap());
        engine.inventory0().await.unwrap();
        if let Some(existing) = existing {
            assert_eq!(
                engine
                    .fetch_user_verifier("lyrasys")
                    .await
                    .unwrap()
                    .unwrap(),
                existing
            );
        }
        assert_eq!(engine.list_users().await.unwrap().len(), 1);
        assert_eq!(engine.list_databases().await.unwrap().len(), 2);
    }
}
