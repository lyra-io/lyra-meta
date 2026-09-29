//! One test-only Catalog writer serializes index lookup plus mutation.
//! This mutex is NOT a distributed fence and proves nothing about two Catalogs.

use super::backend::{
    DATABASE_NAMES, DATABASES, Error, Row, Store, USER_NAMES, USERS, parse_id, validate_name,
};
use super::telemetry::Telemetry;
use lyra_meta::proto::pb_meta::{Database, User};
use prost::Message;
use std::time::Instant;
use tokio::sync::Mutex;

pub struct Writer {
    // Immutable state
    pub store: Store,
    pub telemetry: Telemetry,
    // Mutable state
    mutations: Mutex<()>,
}

impl Writer {
    pub fn new(store: Store) -> Self {
        let telemetry = Telemetry::new(store.backend());
        Self {
            store,
            telemetry,
            mutations: Mutex::new(()),
        }
    }

    pub async fn create_database(&self, database: Database) -> Result<Row, Error> {
        self.create0(
            DATABASES,
            DATABASE_NAMES,
            &database.name,
            database.encode_to_vec(),
        )
        .await
    }

    pub async fn create_user(&self, user: User) -> Result<Row, Error> {
        self.create0(USERS, USER_NAMES, &user.name, user.encode_to_vec())
            .await
    }

    async fn create0(
        &self,
        prefix: &str,
        index: &str,
        name: &str,
        value: Vec<u8>,
    ) -> Result<Row, Error> {
        let start = Instant::now();
        let _guard = self.mutations.lock().await;
        let result = async {
            validate_name(name)?;
            match self.store.find(index, name).await {
                Ok(_) => return Err(Error::Duplicate),
                Err(Error::Missing) => {}
                Err(error) => return Err(error),
            }
            let row = self.store.allocate(prefix, value, index, name).await?;
            // Detect invalid IDs; handling a write already committed past u32::MAX
            // remains a production-design gap, not a tested rollback guarantee.
            parse_id(prefix, &row.key)?;
            Ok(row)
        }
        .await;
        self.telemetry.operation("create", start, result.is_ok());
        result
    }

    pub async fn rename_database(&self, row: &Row, name: &str) -> Result<Row, Error> {
        let start = Instant::now();
        let _guard = self.mutations.lock().await;
        let result = async {
            validate_name(name)?;
            match self.store.find(DATABASE_NAMES, name).await {
                Ok(found) if found.key != row.key => return Err(Error::Duplicate),
                Ok(_) | Err(Error::Missing) => {}
                Err(error) => return Err(error),
            }
            let mut database = Database::decode(row.value.as_slice()).unwrap();
            database.name = name.into();
            self.store
                .put(
                    &row.key,
                    database.encode_to_vec(),
                    Some(row.version),
                    Some((DATABASE_NAMES, name)),
                )
                .await
        }
        .await;
        self.telemetry.operation("update", start, result.is_ok());
        result
    }
}
