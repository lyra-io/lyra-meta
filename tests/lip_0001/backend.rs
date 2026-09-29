//! Test-only storage adapter, not a second production Metadata interface.
//! The memory model is a reference fixture; it is not MemoryMetadata.

use oxia::{GetResult, OxiaClient, OxiaError};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;

pub const PARTITION: &str = "catalog";
pub const DATABASES: &str = "/catalog/databases/x";
pub const USERS: &str = "/catalog/users/x";
pub const DATABASE_NAMES: &str = "lyra.database.name";
pub const USER_NAMES: &str = "lyra.user.name";

#[derive(Debug, Error)]
pub enum Error {
    #[error("record not found")]
    Missing,
    #[error("version conflict")]
    Conflict,
    #[error("duplicate name")]
    Duplicate,
    #[error("invalid name")]
    InvalidName,
    #[error("invalid or exhausted object ID")]
    InvalidId,
    #[error("backend request failed")]
    Backend(#[source] OxiaError),
}

impl From<OxiaError> for Error {
    fn from(error: OxiaError) -> Self {
        match error {
            OxiaError::KeyNotFound => Self::Missing,
            OxiaError::UnexpectedVersionId => Self::Conflict,
            error => Self::Backend(error),
        }
    }
}

// Deliberately no Debug: raw records may contain authentication material.
#[derive(Clone)]
pub struct Row {
    pub key: String,
    pub value: Vec<u8>,
    pub version: i64,
    pub index: Option<(String, String)>,
}

impl From<GetResult> for Row {
    fn from(result: GetResult) -> Self {
        Self {
            key: result.key,
            // Successful Get + a version means presence, including a zero-byte value.
            value: result.value.unwrap_or_default().to_vec(),
            version: result.version.version_id,
            index: None,
        }
    }
}

#[derive(Default)]
pub struct Memory {
    // Mutable state
    rows: BTreeMap<String, Row>,
    sequences: HashMap<String, u64>,
    version: i64,
}

pub enum Store {
    Memory(Mutex<Memory>),
    Oxia(OxiaClient),
}

impl Store {
    pub fn new() -> Self {
        Self::Memory(Mutex::new(Memory::default()))
    }

    pub fn backend(&self) -> &'static str {
        match self {
            Self::Memory(_) => "memory",
            Self::Oxia(_) => "oxia",
        }
    }

    pub async fn get(&self, key: &str) -> Result<Row, Error> {
        match self {
            Self::Memory(state) => state
                .lock()
                .await
                .rows
                .get(key)
                .cloned()
                .ok_or(Error::Missing),
            Self::Oxia(client) => Ok(client.get(key).partition_key(PARTITION).await?.into()),
        }
    }

    pub async fn find(&self, index: &str, name: &str) -> Result<Row, Error> {
        match self {
            Self::Memory(state) => state
                .lock()
                .await
                .rows
                .values()
                .find(|row| {
                    row.index
                        .as_ref()
                        .is_some_and(|(i, n)| i == index && n == name)
                })
                .cloned()
                .ok_or(Error::Missing),
            Self::Oxia(client) => Ok(client
                .get(name)
                .partition_key(PARTITION)
                .use_index(index)
                .await?
                .into()),
        }
    }

    pub async fn list(&self, collection: &str) -> Result<Vec<Row>, Error> {
        // Both bounds work with Oxia's hierarchical ordering, not just lexicographic ordering.
        let start = format!("{collection}/");
        let end = format!("{collection}//");
        match self {
            Self::Memory(state) => Ok(state
                .lock()
                .await
                .rows
                .values()
                .filter(|row| row.key.starts_with(&start))
                .cloned()
                .collect()),
            Self::Oxia(client) => Ok(client
                .range_scan(start, end)
                .partition_key(PARTITION)
                .await?
                .into_iter()
                .map(Row::from)
                .collect()),
        }
    }

    /// A direct sequence write. No retry after an ambiguous result in this prototype.
    pub async fn allocate(
        &self,
        prefix: &str,
        value: Vec<u8>,
        index: &str,
        name: &str,
    ) -> Result<Row, Error> {
        match self {
            Self::Memory(state) => {
                let mut state = state.lock().await;
                let sequence = state.sequences.entry(prefix.to_string()).or_default();
                *sequence += 1;
                let key = format!("{prefix}-{:020}", *sequence);
                state.version += 1;
                let row = Row {
                    key,
                    value,
                    version: state.version,
                    index: Some((index.into(), name.into())),
                };
                state.rows.insert(row.key.clone(), row.clone());
                Ok(row)
            }
            Self::Oxia(client) => {
                let result = client
                    .put(prefix, value.clone())
                    .partition_key(PARTITION)
                    .sequence_key_deltas([1])
                    .secondary_index(index, name)
                    .await?;
                Ok(Row {
                    key: result.key,
                    value,
                    version: result.version.version_id,
                    index: None,
                })
            }
        }
    }

    /// None means create-if-absent; Some(version) means compare-and-swap.
    pub async fn put(
        &self,
        key: &str,
        value: Vec<u8>,
        version: Option<i64>,
        index: Option<(&str, &str)>,
    ) -> Result<Row, Error> {
        match self {
            Self::Memory(state) => {
                let mut state = state.lock().await;
                if state.rows.get(key).map(|row| row.version) != version {
                    return Err(Error::Conflict);
                }
                state.version += 1;
                let row = Row {
                    key: key.into(),
                    value,
                    version: state.version,
                    index: index.map(|(i, n)| (i.into(), n.into())),
                };
                state.rows.insert(key.into(), row.clone());
                Ok(row)
            }
            Self::Oxia(client) => {
                let request = client.put(key, value.clone()).partition_key(PARTITION);
                let request = match version {
                    Some(version) => request.expected_version_id(version),
                    None => request.expected_record_not_exists(),
                };
                let request = match index {
                    Some((index, name)) => request.secondary_index(index, name),
                    None => request,
                };
                let result = request.await?;
                Ok(Row {
                    key: result.key,
                    value,
                    version: result.version.version_id,
                    index: None,
                })
            }
        }
    }

    pub async fn delete(&self, key: &str, version: i64) -> Result<(), Error> {
        match self {
            Self::Memory(state) => {
                let mut state = state.lock().await;
                if state.rows.get(key).ok_or(Error::Missing)?.version != version {
                    return Err(Error::Conflict);
                }
                state.rows.remove(key);
                Ok(())
            }
            Self::Oxia(client) => Ok(client
                .delete(key)
                .partition_key(PARTITION)
                .expected_version_id(version)
                .await?),
        }
    }
}

pub async fn connect(address: &str) -> OxiaClient {
    OxiaClient::builder()
        .service_address(address)
        .namespace("default")
        .request_timeout(Duration::from_secs(5))
        .session_timeout(Duration::from_secs(5))
        .session_keep_alive(Duration::from_millis(500))
        .build()
        .await
        .unwrap()
}

pub fn parse_id(prefix: &str, key: &str) -> Result<u32, Error> {
    let suffix = key
        .strip_prefix(prefix)
        .and_then(|suffix| suffix.strip_prefix('-'))
        .filter(|suffix| suffix.len() == 20 && suffix.bytes().all(|b| b.is_ascii_digit()))
        .ok_or(Error::InvalidId)?;
    suffix
        .parse::<u32>()
        .ok()
        .filter(|id| *id != 0)
        .ok_or(Error::InvalidId)
}

pub fn validate_name(name: &str) -> Result<(), Error> {
    // SQL canonicalization belongs to Catalog, before this metadata boundary.
    if name.is_empty() || name.len() > 63 || name.contains('\0') {
        Err(Error::InvalidName)
    } else {
        Ok(())
    }
}
