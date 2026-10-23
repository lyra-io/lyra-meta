use super::engine::{Engine, metadata_impl};
use super::keys::validate_component as super_validate_component;
use super::keys::{registration_key, validate_component};
use super::registration::Lease;
use super::storage::{Condition, Row, Storage};
use super::{
    ComponentIdentity, Metadata, MetadataError, MetadataRecord, MetadataVersion, Registration,
    Result, UserInfo,
};
use crate::proto::pb_meta::{Database, Instance, ScramSha256Verifier};
use async_trait::async_trait;
use opentelemetry::metrics::Meter;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Default)]
struct State {
    // Mutable state
    rows: BTreeMap<String, Row>,
    indexes: HashMap<String, (String, String)>,
    version: i64,
    closed: bool,
}

#[derive(Clone, Default)]
pub(crate) struct MemoryStorage {
    // Mutable state
    state: Arc<Mutex<State>>,
}

pub struct MemoryMetadata {
    // Immutable state
    engine: Engine,
}

impl MemoryMetadata {
    pub fn new() -> Self {
        Self {
            engine: Engine::new(Arc::new(MemoryStorage::default()), None),
        }
    }
    pub fn with_meter(meter: Meter) -> Self {
        Self {
            engine: Engine::new(Arc::new(MemoryStorage::default()), Some(meter)),
        }
    }
}

impl Default for MemoryMetadata {
    fn default() -> Self {
        Self::new()
    }
}

fn check0(state: &State) -> Result<()> {
    if state.closed {
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
        check0(&state)?;
        Ok(state.rows.get(key).cloned())
    }
    async fn scan(&self, first: &str, _last: &str) -> Result<Vec<Row>> {
        let state = self.state.lock().await;
        check0(&state)?;
        Ok(state
            .rows
            .values()
            .filter(|row| row.key.starts_with(first))
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
        check0(&state)?;
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
        check0(&state)?;
        if state.rows.get(key).is_none_or(|row| row.version != version) {
            return Err(MetadataError::Conflict("conditional delete".into()));
        }
        state.rows.remove(key);
        state.indexes.remove(key);
        Ok(())
    }
    async fn register(&self, component: &str) -> Result<(ComponentIdentity, Arc<dyn Lease>)> {
        validate_component(component)?;
        let id = Uuid::new_v4();
        let key = registration_key(component, id)?;
        let row = self.put(&key, Vec::new(), Condition::Missing, None).await?;
        let identity = ComponentIdentity {
            component_type: component.into(),
            registration_id: id,
        };
        let lease = MemoryLease {
            storage: self.clone(),
            key,
            version: row.version,
        };
        Ok((identity, Arc::new(lease)))
    }
    async fn registrations(&self, component: &str) -> Result<Vec<ComponentIdentity>> {
        validate_component(component)?;
        let prefix = format!("/discovery/{component}/instances/");
        let state = self.state.lock().await;
        check0(&state)?;
        state
            .rows
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .map(|key| {
                let suffix = key.strip_prefix(&prefix).unwrap();
                let id = Uuid::parse_str(suffix)
                    .map_err(|_| MetadataError::InvalidRecord("invalid registration UUID"))?;
                Ok(ComponentIdentity {
                    component_type: component.into(),
                    registration_id: id,
                })
            })
            .collect()
    }
    async fn close(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        state.rows.retain(|key, _| !key.starts_with("/discovery/"));
        state.closed = true;
        Ok(())
    }
}

struct MemoryLease {
    // Immutable state
    storage: MemoryStorage,
    key: String,
    version: i64,
}

#[async_trait]
impl Lease for MemoryLease {
    async fn present(&self) -> Result<bool> {
        Ok(self
            .storage
            .get(&self.key)
            .await?
            .is_some_and(|row| row.version == self.version))
    }
    async fn close(&self) -> Result<()> {
        let mut state = self.storage.state.lock().await;
        if state
            .rows
            .get(&self.key)
            .is_some_and(|row| row.version == self.version)
        {
            state.rows.remove(&self.key);
        }
        Ok(())
    }
}

metadata_impl!(MemoryMetadata);
