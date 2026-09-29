//! Private transport primitives, shared by the existing Metadata implementations.
//! Components use Metadata, never this key/value interface.
use super::registration::Lease;
use super::{ComponentIdentity, Result};
use async_trait::async_trait;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct Row {
    pub key: String,
    pub value: Vec<u8>,
    pub version: i64,
}

#[derive(Clone, Copy)]
pub(crate) enum Condition {
    Missing,
    Version(i64),
}

#[async_trait]
pub(crate) trait Storage: Send + Sync {
    fn backend(&self) -> &'static str;
    async fn get(&self, key: &str) -> Result<Option<Row>>;
    async fn scan(&self, first: &str, last: &str) -> Result<Vec<Row>>;
    async fn find(&self, index: &str, name: &str) -> Result<Vec<Row>>;
    async fn allocate(&self, prefix: &str, value: Vec<u8>, index: &str, name: &str) -> Result<Row>;
    async fn put(
        &self,
        key: &str,
        value: Vec<u8>,
        condition: Condition,
        index: Option<(&str, &str)>,
    ) -> Result<Row>;
    async fn delete(&self, key: &str, version: i64) -> Result<()>;
    async fn register(&self, component: &str) -> Result<(ComponentIdentity, Arc<dyn Lease>)>;
    async fn registrations(&self, component: &str) -> Result<Vec<ComponentIdentity>>;
    async fn close(&self) -> Result<()>;
}
