use super::engine::Engine;
use super::keys::Collection;
use super::memory::MemoryStorage;
use super::registration::Lease;
use super::storage::{Condition, Row, Storage};
use super::{ComponentIdentity, MetadataError, Result};
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
    async fn register(&self, component: &str) -> Result<(ComponentIdentity, Arc<dyn Lease>)> {
        self.base.register(component).await
    }
    async fn registrations(&self, component: &str) -> Result<Vec<ComponentIdentity>> {
        self.base.registrations(component).await
    }
    async fn close(&self) -> Result<()> {
        self.base.close().await
    }
}

async fn initialized() -> (Arc<FaultStorage>, Engine, u32) {
    let store = Arc::new(FaultStorage::default());
    let engine = Engine::new(store.clone(), None);
    engine
        .initialize(make_verifier("test-password").unwrap())
        .await
        .unwrap();
    let user = engine.get_user("lyrasys").await.unwrap().unwrap().id();
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
            .get_database("lost-create")
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
    assert!(engine.get_database("lost-create").await.unwrap().is_none());
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
                .get_database("must-not-write")
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
    Engine::new(store.clone(), None)
        .validate_initialized()
        .await
        .unwrap();
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
                .get_database("uncertain-counter")
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
    assert!(restarted.validate_initialized().await.is_err());
    assert!(store.get(key).await.unwrap().is_none());
}
