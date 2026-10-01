use super::OxiaOptions;
use crate::metadata::engine::{Engine, metadata_impl};
use crate::metadata::keys::PARTITION;
use crate::metadata::registration::PREFIX;
use crate::metadata::storage::{Condition, Presence, PresenceEvents, Row, Storage};
use crate::metadata::{Metadata, MetadataError, MetadataRecord, MetadataVersion, Result, UserInfo};
use crate::proto::pb_meta::{Component, Database, Instance, ScramSha256Verifier};
use async_trait::async_trait;
use opentelemetry::metrics::Meter;
use oxia::{GetResult, Notification, Notifications, OxiaClient, OxiaError};
use std::sync::Arc;
use uuid::Uuid;

pub struct OxiaMetadata {
    // Immutable state
    engine: Engine,
}

impl OxiaMetadata {
    pub async fn new(options: &OxiaOptions) -> Result<Self> {
        Self::new0(options, None).await
    }
    pub async fn with_meter(options: &OxiaOptions, meter: Meter) -> Result<Self> {
        Self::new0(options, Some(meter)).await
    }
    async fn new0(options: &OxiaOptions, meter: Option<Meter>) -> Result<Self> {
        let identity = Uuid::new_v4().to_string();
        let client = connect(options, &identity).await?;
        Ok(Self {
            engine: Engine::new(Arc::new(OxiaStorage { client, identity }), meter),
        })
    }
}

struct OxiaStorage {
    // Immutable state
    client: OxiaClient,
    identity: String,
}

fn row0(record: GetResult) -> Row {
    Row {
        key: record.key,
        value: record.value.unwrap_or_default().to_vec(),
        version: record.version.version_id,
    }
}

fn error0(error: OxiaError) -> MetadataError {
    match error {
        OxiaError::UnexpectedVersionId => MetadataError::Conflict("backend version".into()),
        OxiaError::KeyNotFound => MetadataError::NotFound,
        OxiaError::Closed => MetadataError::Closed,
        error => MetadataError::Oxia(error),
    }
}

async fn connect(options: &OxiaOptions, identity: &str) -> Result<OxiaClient> {
    if options.namespace().is_empty() || options.service_address().is_empty() {
        return Err(MetadataError::InvalidRecord(
            "metadata endpoint and namespace are required",
        ));
    }
    Ok(OxiaClient::builder()
        .identity(identity)
        .service_address(options.service_address())
        .namespace(options.namespace())
        .request_timeout(options.request_timeout())
        .session_timeout(options.session_timeout())
        .session_keep_alive(options.session_timeout() / 10)
        .build()
        .await?)
}

#[async_trait]
impl Storage for OxiaStorage {
    fn backend(&self) -> &'static str {
        "oxia"
    }
    async fn get(&self, key: &str) -> Result<Option<Row>> {
        match self.client.get(key).partition_key(PARTITION).await {
            Ok(record) => Ok(Some(row0(record))),
            Err(OxiaError::KeyNotFound) => Ok(None),
            Err(error) => Err(error0(error)),
        }
    }
    async fn scan(&self, first: &str, last: &str) -> Result<Vec<Row>> {
        Ok(self
            .client
            .range_scan(first, last)
            .partition_key(PARTITION)
            .await?
            .into_iter()
            .map(row0)
            .collect())
    }
    async fn put(
        &self,
        key: &str,
        value: Vec<u8>,
        condition: Condition,
        index: Option<(&str, &str)>,
    ) -> Result<Row> {
        let request = self.client.put(key, value.clone()).partition_key(PARTITION);
        let request = match condition {
            Condition::Missing => request.expected_record_not_exists(),
            Condition::Version(version) => request.expected_version_id(version),
        };
        let request = match index {
            Some((index, name)) => request.secondary_index(index, name),
            None => request,
        };
        let result = request.await.map_err(error0)?;
        Ok(Row {
            key: result.key,
            value,
            version: result.version.version_id,
        })
    }
    async fn delete(&self, key: &str, version: i64) -> Result<()> {
        self.client
            .delete(key)
            .partition_key(PARTITION)
            .expected_version_id(version)
            .await
            .map_err(error0)
    }
    fn identity(&self) -> &str {
        &self.identity
    }
    async fn subscribe(&self, key: &str) -> Result<Box<dyn PresenceEvents>> {
        Ok(Box::new(Events {
            stream: self.client.notifications().await?,
            key: key.into(),
        }))
    }
    async fn fetch_presence(&self, key: &str) -> Result<Option<Presence>> {
        match self
            .client
            .get(key)
            .partition_key("discovery/catalog")
            .await
        {
            Ok(row) => Ok(Some(presence(row))),
            Err(OxiaError::KeyNotFound) => Ok(None),
            Err(error) => Err(error0(error)),
        }
    }
    async fn create_presence(&self, key: &str, value: Vec<u8>) -> Result<Presence> {
        let result = self
            .client
            .put(key, value.clone())
            .partition_key("discovery/catalog")
            .expected_record_not_exists()
            .ephemeral()
            .await
            .map_err(error0)?;
        Ok(Presence {
            session: result.version.session_id,
            owner: result.version.client_identity,
            row: Row {
                key: result.key,
                value,
                version: result.version.version_id,
            },
        })
    }
    async fn delete_presence(&self, key: &str, version: i64) -> Result<()> {
        self.client
            .delete(key)
            .partition_key("discovery/catalog")
            .expected_version_id(version)
            .await
            .map_err(error0)
    }
    async fn list_presence(&self) -> Result<Vec<Presence>> {
        // The default Oxia encoder groups keys by slash count. Scan the
        // supported kind's leaf collection at its actual depth, not a recursive
        // /discovery/ range. Add one leaf scan per kind as new kinds are defined.
        Ok(self
            .client
            .range_scan(PREFIX, format!("{PREFIX}~"))
            .partition_key("discovery/catalog")
            .await
            .map_err(error0)?
            .into_iter()
            .map(presence)
            .collect())
    }
    async fn close(&self) -> Result<()> {
        self.client.close().await.map_err(error0)
    }
}

fn presence(record: GetResult) -> Presence {
    Presence {
        session: record.version.session_id,
        owner: record.version.client_identity.clone(),
        row: row0(record),
    }
}

struct Events {
    stream: Notifications,
    key: String,
}
#[async_trait]
impl PresenceEvents for Events {
    async fn next(&mut self) -> bool {
        while let Some(event) = self.stream.recv().await {
            match &event {
                // A range deletion is only a hint. Re-read the exact owned key
                // rather than duplicating Oxia's slash-aware range comparator.
                Notification::KeyRangeDeleted { .. } => return true,
                _ if event.key() == self.key => return true,
                _ => {}
            }
        }
        false
    }
}

metadata_impl!(OxiaMetadata);
