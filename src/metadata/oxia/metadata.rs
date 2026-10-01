use super::OxiaOptions;
use crate::metadata::engine::{Engine, metadata_impl};
use crate::metadata::keys::validate_component as super_validate_component;
use crate::metadata::keys::{PARTITION, registration_key, validate_component};
use crate::metadata::registration::Lease;
use crate::metadata::storage::{Condition, Row, Storage};
use crate::metadata::{
    ComponentIdentity, Metadata, MetadataError, MetadataRecord, MetadataVersion, Registration,
    Result, UserInfo,
};
use crate::proto::pb_meta::{ComponentRegistration, Database, Instance, ScramSha256Verifier};
use async_trait::async_trait;
use opentelemetry::metrics::Meter;
use oxia::{GetResult, OxiaClient, OxiaError};
use prost::Message;
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
        let client = connect(options).await?;
        Ok(Self {
            engine: Engine::new(
                Arc::new(OxiaStorage {
                    client,
                    options: options.clone(),
                }),
                meter,
            ),
        })
    }
}

struct OxiaStorage {
    // Immutable state
    client: OxiaClient,
    options: OxiaOptions,
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

async fn connect(options: &OxiaOptions) -> Result<OxiaClient> {
    if options.namespace().is_empty() || options.service_address().is_empty() {
        return Err(MetadataError::InvalidRecord(
            "metadata endpoint and namespace are required",
        ));
    }
    Ok(OxiaClient::builder()
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
    async fn register(&self, component: &str) -> Result<(ComponentIdentity, Arc<dyn Lease>)> {
        validate_component(component)?;
        let id = Uuid::new_v4();
        let key = registration_key(component, id)?;
        let partition = format!("discovery/{component}");
        // A dedicated SDK instance owns this lease. Closing it cannot close the
        // durable client's sessions or another registration's session.
        let client = connect(&self.options).await?;
        let result = client
            .put(&key, ComponentRegistration {}.encode_to_vec())
            .partition_key(&partition)
            .expected_record_not_exists()
            .ephemeral()
            .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                let _ = client.close().await;
                return Err(error0(error));
            }
        };
        let identity = ComponentIdentity {
            component_type: component.into(),
            registration_id: id,
        };
        let lease = OxiaLease {
            client,
            key,
            partition,
            version: result.version.version_id,
        };
        Ok((identity, Arc::new(lease)))
    }
    async fn registrations(&self, component: &str) -> Result<Vec<ComponentIdentity>> {
        validate_component(component)?;
        let prefix = format!("/discovery/{component}/instances/");
        let end = format!("{prefix}/");
        let records = self
            .client
            .range_scan(&prefix, end)
            .partition_key(format!("discovery/{component}"))
            .await?;
        records
            .into_iter()
            .map(|row| {
                if !row.version.is_ephemeral() {
                    return Err(MetadataError::Integrity("durable registration record"));
                }
                ComponentRegistration::decode(row.value.unwrap_or_default())?;
                let suffix = row
                    .key
                    .strip_prefix(&prefix)
                    .ok_or(MetadataError::InvalidRecord(
                        "registration outside component",
                    ))?;
                let id = Uuid::parse_str(suffix)
                    .map_err(|_| MetadataError::InvalidRecord("invalid registration UUID"))?;
                if id.to_string() != suffix {
                    return Err(MetadataError::InvalidRecord(
                        "non-canonical registration UUID",
                    ));
                }
                Ok(ComponentIdentity {
                    component_type: component.into(),
                    registration_id: id,
                })
            })
            .collect()
    }
    async fn close(&self) -> Result<()> {
        self.client.close().await.map_err(error0)
    }
}

struct OxiaLease {
    // Immutable state
    client: OxiaClient,
    key: String,
    partition: String,
    version: i64,
}

#[async_trait]
impl Lease for OxiaLease {
    async fn present(&self) -> Result<bool> {
        match self
            .client
            .get(&self.key)
            .partition_key(&self.partition)
            .await
        {
            Ok(row) => Ok(row.version.is_ephemeral() && row.version.version_id == self.version),
            Err(OxiaError::KeyNotFound | OxiaError::Closed) => Ok(false),
            Err(error) => Err(error0(error)),
        }
    }
    async fn close(&self) -> Result<()> {
        // Close the owning session, never unconditionally delete a UUID key that
        // might have been replaced. The SDK/server remove only session-owned keys.
        self.client.close().await.map_err(error0)
    }
}

metadata_impl!(OxiaMetadata);
