use super::engine::Engine;
use super::memory::MemoryStorage;
use super::registration::Lease;
use super::storage::{Condition, Row, Storage};
use super::{ComponentIdentity, MetadataError, Result};
use crate::proto::pb_meta::Database;
use crate::utils::verifier::make_verifier;
use async_trait::async_trait;
use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::timeout;

#[derive(Default)]
struct FaultStorage {
    base: MemoryStorage,
    mode: AtomicU8,
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
    async fn find(&self, index: &str, name: &str) -> Result<Vec<Row>> {
        self.base.find(index, name).await
    }
    async fn allocate(&self, prefix: &str, value: Vec<u8>, index: &str, name: &str) -> Result<Row> {
        self.allocations.fetch_add(1, Ordering::SeqCst);
        if self
            .mode
            .compare_exchange(2, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Err(MetadataError::Closed);
        }
        self.finish(self.base.allocate(prefix, value, index, name).await)
            .await
    }
    async fn put(
        &self,
        key: &str,
        value: Vec<u8>,
        condition: Condition,
        index: Option<(&str, &str)>,
    ) -> Result<Row> {
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
    value.name = "lost-rename".into();
    let record = engine
        .update_database(record.id(), value, record.version())
        .await
        .unwrap();
    assert!(engine.get_database("lost-create").await.unwrap().is_none());
    store.mode.store(1, Ordering::SeqCst);
    engine
        .delete_database(record.id(), record.version())
        .await
        .unwrap();
    assert!(engine.get_database("lost-rename").await.unwrap().is_none());
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
