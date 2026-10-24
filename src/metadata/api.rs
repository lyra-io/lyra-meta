use super::{RegisteredComponent, Registration, Result};
use crate::proto::pb_meta::{
    ComponentRegistration, Database, Format, Instance, ScramSha256Verifier, Stream,
};
use async_trait::async_trait;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataVersion(i64);

impl MetadataVersion {
    pub fn new(value: i64) -> Self {
        Self(value)
    }
    pub fn value(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetadataRecord<T> {
    id: u32,
    value: T,
    version: MetadataVersion,
}

impl<T> MetadataRecord<T> {
    pub fn new(id: u32, value: T, version: MetadataVersion) -> Self {
        Self { id, value, version }
    }
    pub fn id(&self) -> u32 {
        self.id
    }
    pub fn value(&self) -> &T {
        &self.value
    }
    pub fn version(&self) -> MetadataVersion {
        self.version
    }
}

/// Public user inventory never contains a credential.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserInfo {
    pub name: String,
}

/// Name creation and per-type ID allocation are atomic between clients.
/// Database lifecycle/session coordination and user deletion still require one
/// lifecycle writer per namespace; this is not distributed writer fencing.
#[async_trait]
pub trait Metadata: Send + Sync {
    async fn instance(&self) -> Result<Option<Instance>>;
    async fn initialize(&self, verifier: ScramSha256Verifier) -> Result<()>;
    async fn validate_initialized(&self) -> Result<()>;

    async fn get_user(&self, name: &str) -> Result<Option<MetadataRecord<UserInfo>>>;
    async fn get_user_by_id(&self, id: u32) -> Result<Option<MetadataRecord<UserInfo>>>;
    async fn list_users(&self) -> Result<Vec<MetadataRecord<UserInfo>>>;
    /// Explicit authentication-only lookup. Never project this through SQL.
    async fn user_verifier(&self, name: &str) -> Result<Option<ScramSha256Verifier>>;
    async fn create_user(
        &self,
        name: &str,
        verifier: ScramSha256Verifier,
    ) -> Result<MetadataRecord<UserInfo>>;
    async fn delete_user(&self, id: u32, version: MetadataVersion) -> Result<()>;

    async fn get_database(&self, name: &str) -> Result<Option<MetadataRecord<Database>>>;
    async fn get_database_by_id(&self, id: u32) -> Result<Option<MetadataRecord<Database>>>;
    async fn list_databases(&self) -> Result<Vec<MetadataRecord<Database>>>;
    async fn create_database(&self, database: Database) -> Result<MetadataRecord<Database>>;
    async fn update_database(
        &self,
        id: u32,
        database: Database,
        version: MetadataVersion,
    ) -> Result<MetadataRecord<Database>>;
    async fn delete_database(&self, id: u32, version: MetadataVersion) -> Result<()>;

    async fn get_format(&self, name: &str) -> Result<Option<MetadataRecord<Format>>>;
    async fn get_format_by_id(&self, id: u32) -> Result<Option<MetadataRecord<Format>>>;
    async fn list_formats(&self) -> Result<Vec<MetadataRecord<Format>>>;
    async fn create_format(&self, format: Format) -> Result<MetadataRecord<Format>>;
    async fn delete_format(&self, id: u32, version: MetadataVersion) -> Result<()>;

    async fn get_stream(&self, name: &str) -> Result<Option<MetadataRecord<Stream>>>;
    async fn get_stream_by_id(&self, id: u32) -> Result<Option<MetadataRecord<Stream>>>;
    async fn list_streams(&self) -> Result<Vec<MetadataRecord<Stream>>>;
    async fn create_stream(&self, stream: Stream) -> Result<MetadataRecord<Stream>>;
    async fn update_stream(
        &self,
        id: u32,
        stream: Stream,
        version: MetadataVersion,
    ) -> Result<MetadataRecord<Stream>>;
    async fn delete_stream(&self, id: u32, version: MetadataVersion) -> Result<()>;

    async fn register_component(
        &self,
        component: &str,
        registration: ComponentRegistration,
    ) -> Result<Registration>;
    async fn list_components(&self, component: &str) -> Result<Vec<RegisteredComponent>>;
    async fn close(&self) -> Result<()>;
}
