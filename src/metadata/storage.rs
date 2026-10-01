//! Private transport primitives, shared by the existing Metadata implementations.
//! Components use Metadata, never this key/value interface.
use super::Result;
use async_trait::async_trait;

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

#[derive(Clone)]
pub(crate) struct Presence {
    pub row: Row,
    pub session: Option<i64>,
    pub owner: Option<String>,
}

#[async_trait]
pub(crate) trait PresenceEvents: Send {
    /// A hint to reread; stream closure is a terminal monitor failure.
    async fn next(&mut self) -> bool;
}

#[async_trait]
pub(crate) trait Storage: Send + Sync {
    fn backend(&self) -> &'static str;
    async fn get(&self, key: &str) -> Result<Option<Row>>;
    async fn scan(&self, first: &str, last: &str) -> Result<Vec<Row>>;
    async fn put(
        &self,
        key: &str,
        value: Vec<u8>,
        condition: Condition,
        index: Option<(&str, &str)>,
    ) -> Result<Row>;
    async fn delete(&self, key: &str, version: i64) -> Result<()>;
    fn identity(&self) -> &str;
    async fn subscribe(&self, key: &str) -> Result<Box<dyn PresenceEvents>>;
    async fn fetch_presence(&self, key: &str) -> Result<Option<Presence>>;
    async fn create_presence(&self, key: &str, value: Vec<u8>) -> Result<Presence>;
    async fn delete_presence(&self, key: &str, version: i64) -> Result<()>;
    async fn list_presence(&self) -> Result<Vec<Presence>>;
    async fn close(&self) -> Result<()>;
}
