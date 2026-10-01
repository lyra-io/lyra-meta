use super::engine::{Engine, metadata_impl};
use super::storage::{Condition, Presence, PresenceEvents, Row, Storage};
use super::{Metadata, MetadataError, MetadataRecord, MetadataVersion, Result, UserInfo};
use crate::proto::pb_meta::{Component, Database, Instance, ScramSha256Verifier};
use async_trait::async_trait;
use opentelemetry::metrics::Meter;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, watch};
use uuid::Uuid;

#[derive(Default)]
struct State {
    // Mutable state
    rows: BTreeMap<String, Row>,
    indexes: HashMap<String, (String, String)>,
    version: i64,
    owners: HashMap<String, String>,
}

pub(crate) struct MemoryStorage {
    // Immutable state
    identity: String,
    changes: watch::Sender<u64>,
    // Control state
    closed: AtomicBool,
    // Mutable state
    state: Arc<Mutex<State>>,
}

pub struct MemoryMetadata {
    // Immutable state
    engine: Engine,
    store: Arc<MemoryStorage>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self {
            identity: Uuid::new_v4().to_string(),
            changes: watch::channel(0).0,
            closed: AtomicBool::new(false),
            state: Arc::default(),
        }
    }
}

impl MemoryMetadata {
    pub fn new() -> Self {
        Self::new0(Arc::new(MemoryStorage::default()), None)
    }
    fn new0(store: Arc<MemoryStorage>, meter: Option<Meter>) -> Self {
        Self {
            engine: Engine::new(store.clone(), meter),
            store,
        }
    }
    /// Open an independent client to this in-memory namespace.
    pub fn new_client(&self) -> Self {
        Self::new0(
            Arc::new(MemoryStorage {
                closed: AtomicBool::new(false),
                identity: Uuid::new_v4().to_string(),
                changes: self.store.changes.clone(),
                state: Arc::clone(&self.store.state),
            }),
            None,
        )
    }
    pub fn with_meter(meter: Meter) -> Self {
        Self::new0(Arc::new(MemoryStorage::default()), Some(meter))
    }
}

impl Default for MemoryMetadata {
    fn default() -> Self {
        Self::new()
    }
}

fn check0(store: &MemoryStorage) -> Result<()> {
    if store.closed.load(Ordering::Acquire) {
        Err(MetadataError::Closed)
    } else {
        Ok(())
    }
}

fn version0(state: &mut State) -> Result<i64> {
    state.version = state
        .version
        .checked_add(1)
        .ok_or_else(|| MetadataError::CounterExhausted("memory version".into()))?;
    Ok(state.version)
}

#[async_trait]
impl Storage for MemoryStorage {
    fn backend(&self) -> &'static str {
        "memory"
    }
    async fn get(&self, key: &str) -> Result<Option<Row>> {
        let state = self.state.lock().await;
        check0(self)?;
        Ok(state.rows.get(key).cloned())
    }
    async fn scan(&self, first: &str, last: &str) -> Result<Vec<Row>> {
        let state = self.state.lock().await;
        check0(self)?;
        Ok(state
            .rows
            .values()
            .filter(|row| row.key.as_str() >= first && row.key.as_str() < last)
            .cloned()
            .collect())
    }
    async fn put(
        &self,
        key: &str,
        value: Vec<u8>,
        condition: Condition,
        index: Option<(&str, &str)>,
    ) -> Result<Row> {
        let mut state = self.state.lock().await;
        check0(self)?;
        let matches = match condition {
            Condition::Missing => !state.rows.contains_key(key),
            Condition::Version(version) => state
                .rows
                .get(key)
                .is_some_and(|row| row.version == version),
        };
        if !matches {
            return Err(MetadataError::Conflict("conditional write".into()));
        }
        let row = Row {
            key: key.into(),
            value,
            version: version0(&mut state)?,
        };
        state.rows.insert(key.into(), row.clone());
        state.indexes.remove(key);
        if let Some((index, name)) = index {
            state
                .indexes
                .insert(key.into(), (index.into(), name.into()));
        }
        Ok(row)
    }
    async fn delete(&self, key: &str, version: i64) -> Result<()> {
        let mut state = self.state.lock().await;
        check0(self)?;
        if state.rows.get(key).is_none_or(|row| row.version != version) {
            return Err(MetadataError::Conflict("conditional delete".into()));
        }
        state.rows.remove(key);
        state.owners.remove(key);
        state.indexes.remove(key);
        Ok(())
    }
    fn identity(&self) -> &str {
        &self.identity
    }
    async fn subscribe(&self, _key: &str) -> Result<Box<dyn PresenceEvents>> {
        check0(self)?;
        Ok(Box::new(Events(self.changes.subscribe())))
    }
    async fn fetch_presence(&self, key: &str) -> Result<Option<Presence>> {
        let state = self.state.lock().await;
        check0(self)?;
        Ok(state.rows.get(key).cloned().map(|row| Presence {
            session: state.owners.contains_key(key).then_some(1),
            owner: state.owners.get(key).cloned(),
            row,
        }))
    }
    async fn create_presence(&self, key: &str, value: Vec<u8>) -> Result<Presence> {
        let mut state = self.state.lock().await;
        check0(self)?;
        if state.rows.contains_key(key) {
            return Err(MetadataError::Conflict("presence".into()));
        }
        let row = Row {
            key: key.into(),
            value,
            version: version0(&mut state)?,
        };
        state.rows.insert(key.into(), row.clone());
        state.owners.insert(key.into(), self.identity.clone());
        self.changes.send_modify(|n| *n = n.wrapping_add(1));
        Ok(Presence {
            row,
            session: Some(1),
            owner: Some(self.identity.clone()),
        })
    }
    async fn delete_presence(&self, key: &str, version: i64) -> Result<()> {
        self.delete(key, version).await?;
        self.changes.send_modify(|n| *n = n.wrapping_add(1));
        Ok(())
    }
    async fn list_presence(&self) -> Result<Vec<Presence>> {
        let state = self.state.lock().await;
        check0(self)?;
        Ok(state
            .rows
            .values()
            .filter(|row| row.key.starts_with("/discovery/"))
            .map(|row| Presence {
                row: row.clone(),
                session: state.owners.contains_key(&row.key).then_some(1),
                owner: state.owners.get(&row.key).cloned(),
            })
            .collect())
    }
    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        let mut state = self.state.lock().await;
        let keys: Vec<_> = state
            .owners
            .iter()
            .filter(|(_, owner)| *owner == &self.identity)
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            state.rows.remove(&key);
            state.owners.remove(&key);
        }
        self.changes.send_modify(|n| *n = n.wrapping_add(1));
        Ok(())
    }
}

struct Events(watch::Receiver<u64>);
#[async_trait]
impl PresenceEvents for Events {
    async fn next(&mut self) -> bool {
        self.0.changed().await.is_ok()
    }
}

metadata_impl!(MemoryMetadata);
