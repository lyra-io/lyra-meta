use super::keys::{CATALOG, Collection};
use super::storage::{Condition, Row, Storage};
use super::telemetry::Metrics;
use super::{
    DEFAULT_DATABASE_NAME, MetadataError, MetadataRecord, MetadataVersion, Result,
    SYSTEM_DATABASE_NAME, SYSTEM_USER_NAME, UserInfo, validate_name,
};
use crate::proto::pb_meta::{Database, DatabaseState, Instance, ScramSha256Verifier, User};
use crate::utils::verifier::validate_verifier;
use opentelemetry::metrics::Meter;
use prost::Message;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) struct Engine {
    // Immutable state
    pub(crate) store: Arc<dyn Storage>,
    pub(crate) metrics: Metrics,
    // Mutable state
    mutations: Mutex<bool>,
}

impl Engine {
    pub(crate) async fn read<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        let _guard = self.mutations.lock().await;
        future.await
    }
    pub(crate) fn new(store: Arc<dyn Storage>, meter: Option<Meter>) -> Self {
        let metrics = Metrics::new(store.backend(), meter);
        Self {
            store,
            metrics,
            mutations: Mutex::new(false),
        }
    }

    pub(crate) async fn instance(&self) -> Result<Option<Instance>> {
        self.store
            .get(CATALOG)
            .await?
            .map(|row| {
                let instance = Instance::decode(row.value.as_slice())?;
                if instance.initialized.is_none() {
                    return Err(MetadataError::InvalidRecord(
                        "initialization flag is absent",
                    ));
                }
                Ok(instance)
            })
            .transpose()
    }

    async fn ready0(&self) -> Result<()> {
        match self.instance().await? {
            Some(Instance {
                initialized: Some(true),
            }) => Ok(()),
            Some(_) => Err(MetadataError::IncompleteInitialization),
            None => Err(MetadataError::NotInitialized),
        }
    }

    pub(crate) async fn initialize(&self, verifier: ScramSha256Verifier) -> Result<()> {
        validate_verifier(&verifier)?;
        tracing::info!(
            event = "initialization_started",
            operation = "init",
            "metadata initialization started"
        );
        let mut blocked = self.mutations.lock().await;
        if *blocked {
            return Err(MetadataError::UncertainWrite);
        }
        match self.instance().await? {
            Some(Instance {
                initialized: Some(true),
            }) => {
                tracing::info!(
                    event = "initialization_refused",
                    outcome = "already_initialized",
                    "metadata initialization refused without changes"
                );
                return Err(MetadataError::AlreadyInitialized);
            }
            Some(_) => return Err(MetadataError::IncompleteInitialization),
            None => {}
        }
        for kind in [Collection::Users, Collection::Databases] {
            let (first, last) = kind.range();
            if !self.store.scan(&first, &last).await?.is_empty() {
                return Err(MetadataError::Integrity(
                    "objects exist without an initialization record",
                ));
            }
        }
        // Only the successful create-if-absent owner may initialize. A timed-out
        // claim is not permission to resume: no automatic recovery is attempted.
        *blocked = true;
        let marker = match self
            .store
            .put(
                CATALOG,
                Instance {
                    initialized: Some(false),
                }
                .encode_to_vec(),
                Condition::Missing,
                None,
            )
            .await
        {
            Ok(marker) => marker,
            Err(_) => return Err(MetadataError::IncompleteInitialization),
        };
        *blocked = false;
        let user = User {
            name: SYSTEM_USER_NAME.into(),
            password_verifier: Some(verifier),
        };
        let user = self
            .allocate0(
                Collection::Users,
                &user.name,
                user.encode_to_vec(),
                &mut blocked,
            )
            .await?;
        let owner = Collection::Users.id(&user.key)?;
        for name in [SYSTEM_DATABASE_NAME, DEFAULT_DATABASE_NAME] {
            let mut database = Database::new(name, owner);
            if name == SYSTEM_DATABASE_NAME {
                database.allow_connections = Some(false);
            }
            self.allocate0(
                Collection::Databases,
                name,
                database.encode_to_vec(),
                &mut blocked,
            )
            .await?;
        }
        self.inventory0().await?;
        self.put0(
            CATALOG,
            Instance {
                initialized: Some(true),
            }
            .encode_to_vec(),
            marker.version,
            None,
            &mut blocked,
        )
        .await?;
        tracing::info!(
            operation = "init",
            outcome = "success",
            "metadata initialization completed"
        );
        Ok(())
    }

    pub(crate) async fn validate_initialized(&self) -> Result<()> {
        self.ready0().await?;
        self.inventory0().await
    }

    async fn inventory0(&self) -> Result<()> {
        let users = self.list_users().await?;
        let root = users
            .iter()
            .find(|u| u.value().name == SYSTEM_USER_NAME)
            .ok_or(MetadataError::Integrity("system user missing"))?;
        let databases = self.list_databases().await?;
        let system = databases
            .iter()
            .find(|db| db.value().name == SYSTEM_DATABASE_NAME)
            .ok_or(MetadataError::Integrity("system database missing"))?;
        if system.value().owner_user_id != root.id() {
            return Err(MetadataError::Integrity("system database owner mismatch"));
        }
        // public is deliberately not required after initialization.
        Ok(())
    }

    async fn unique0(&self, kind: Collection, name: &str) -> Result<Option<Row>> {
        validate_name(name)?;
        let mut rows = self.store.find(kind.index(), name).await?;
        if rows.len() > 1 {
            return Err(MetadataError::Integrity(
                "duplicate secondary-index matches",
            ));
        }
        if let Some(row) = rows.pop() {
            let actual = match kind {
                Collection::Users => decode_user(&row)?.value().name.clone(),
                Collection::Databases => decode_database(&row)?.value().name.clone(),
            };
            if actual != name {
                return Err(MetadataError::Integrity("name index disagrees with record"));
            }
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    async fn user0(&self, name: &str) -> Result<Option<MetadataRecord<User>>> {
        self.unique0(Collection::Users, name)
            .await?
            .as_ref()
            .map(decode_user)
            .transpose()
    }

    async fn user_id0(&self, id: u32) -> Result<Option<MetadataRecord<User>>> {
        self.store
            .get(&Collection::Users.key(id)?)
            .await?
            .as_ref()
            .map(decode_user)
            .transpose()
    }

    pub(crate) async fn get_user(&self, name: &str) -> Result<Option<MetadataRecord<UserInfo>>> {
        Ok(self.user0(name).await?.map(public_user))
    }
    pub(crate) async fn get_user_by_id(&self, id: u32) -> Result<Option<MetadataRecord<UserInfo>>> {
        Ok(self.user_id0(id).await?.map(public_user))
    }
    pub(crate) async fn user_verifier(&self, name: &str) -> Result<Option<ScramSha256Verifier>> {
        Ok(self
            .user0(name)
            .await?
            .and_then(|user| user.value().password_verifier.clone()))
    }
    pub(crate) async fn list_users(&self) -> Result<Vec<MetadataRecord<UserInfo>>> {
        let (first, last) = Collection::Users.range();
        let mut names = HashSet::new();
        let mut users = Vec::new();
        for row in self.store.scan(&first, &last).await? {
            let user = decode_user(&row)?;
            if !names.insert(user.value().name.clone()) {
                return Err(MetadataError::Integrity("duplicate user names"));
            }
            if self
                .unique0(Collection::Users, &user.value().name)
                .await?
                .is_none_or(|found| found.key != row.key)
            {
                return Err(MetadataError::Integrity("missing user name index"));
            }
            users.push(public_user(user));
        }
        users.sort_by_key(|u| u.id());
        Ok(users)
    }
    pub(crate) async fn create_user(
        &self,
        name: &str,
        verifier: ScramSha256Verifier,
    ) -> Result<MetadataRecord<UserInfo>> {
        validate_name(name)?;
        validate_verifier(&verifier)?;
        if name == SYSTEM_USER_NAME {
            return Err(MetadataError::Reserved);
        }
        let mut blocked = self.mutations.lock().await;
        if *blocked {
            return Err(MetadataError::UncertainWrite);
        }
        self.ready0().await?;
        if self.unique0(Collection::Users, name).await?.is_some() {
            return Err(MetadataError::AlreadyExists);
        }
        let user = User {
            name: name.into(),
            password_verifier: Some(verifier),
        };
        let row = self
            .allocate0(Collection::Users, name, user.encode_to_vec(), &mut blocked)
            .await?;
        Ok(public_user(decode_user(&row)?))
    }
    pub(crate) async fn delete_user(&self, id: u32, version: MetadataVersion) -> Result<()> {
        let mut blocked = self.mutations.lock().await;
        if *blocked {
            return Err(MetadataError::UncertainWrite);
        }
        self.ready0().await?;
        let user = self.user_id0(id).await?.ok_or(MetadataError::NotFound)?;
        if user.value().name == SYSTEM_USER_NAME {
            return Err(MetadataError::Reserved);
        }
        if self
            .list_databases()
            .await?
            .iter()
            .any(|db| db.value().owner_user_id == id)
        {
            return Err(MetadataError::OwnerInUse);
        }
        self.delete0(&Collection::Users.key(id)?, version.value(), &mut blocked)
            .await
    }
    async fn owner0(&self, database: &Database) -> Result<()> {
        if self.user_id0(database.owner_user_id).await?.is_none() {
            return Err(MetadataError::Integrity("database owner does not exist"));
        }
        Ok(())
    }
    pub(crate) async fn get_database(
        &self,
        name: &str,
    ) -> Result<Option<MetadataRecord<Database>>> {
        let record = self
            .unique0(Collection::Databases, name)
            .await?
            .as_ref()
            .map(decode_database)
            .transpose()?;
        if let Some(record) = &record {
            self.owner0(record.value()).await?;
        }
        Ok(record)
    }
    pub(crate) async fn get_database_by_id(
        &self,
        id: u32,
    ) -> Result<Option<MetadataRecord<Database>>> {
        let record = self
            .store
            .get(&Collection::Databases.key(id)?)
            .await?
            .as_ref()
            .map(decode_database)
            .transpose()?;
        if let Some(record) = &record {
            self.owner0(record.value()).await?;
        }
        Ok(record)
    }
    pub(crate) async fn list_databases(&self) -> Result<Vec<MetadataRecord<Database>>> {
        let (first, last) = Collection::Databases.range();
        let mut names = HashSet::new();
        let mut databases = Vec::new();
        for row in self.store.scan(&first, &last).await? {
            let database = decode_database(&row)?;
            self.owner0(database.value()).await?;
            if !names.insert(database.value().name.clone()) {
                return Err(MetadataError::Integrity("duplicate database names"));
            }
            if self
                .unique0(Collection::Databases, &database.value().name)
                .await?
                .is_none_or(|found| found.key != row.key)
            {
                return Err(MetadataError::Integrity("missing database name index"));
            }
            databases.push(database);
        }
        databases.sort_by_key(|db| db.id());
        Ok(databases)
    }
    pub(crate) async fn create_database(
        &self,
        database: Database,
    ) -> Result<MetadataRecord<Database>> {
        validate_database(&database)?;
        if database.state != DatabaseState::Ready as i32 {
            return Err(MetadataError::InvalidRecord("new database must be READY"));
        }
        if database.name == SYSTEM_DATABASE_NAME {
            return Err(MetadataError::Reserved);
        }
        let mut blocked = self.mutations.lock().await;
        if *blocked {
            return Err(MetadataError::UncertainWrite);
        }
        self.ready0().await?;
        self.owner0(&database).await?;
        if self
            .unique0(Collection::Databases, &database.name)
            .await?
            .is_some()
        {
            return Err(MetadataError::AlreadyExists);
        }
        let row = self
            .allocate0(
                Collection::Databases,
                &database.name,
                database.encode_to_vec(),
                &mut blocked,
            )
            .await?;
        decode_database(&row)
    }
    pub(crate) async fn update_database(
        &self,
        id: u32,
        database: Database,
        version: MetadataVersion,
    ) -> Result<MetadataRecord<Database>> {
        validate_database(&database)?;
        let mut blocked = self.mutations.lock().await;
        if *blocked {
            return Err(MetadataError::UncertainWrite);
        }
        self.ready0().await?;
        let old = self
            .get_database_by_id(id)
            .await?
            .ok_or(MetadataError::NotFound)?;
        if old.value().name == SYSTEM_DATABASE_NAME || database.name == SYSTEM_DATABASE_NAME {
            return Err(MetadataError::Reserved);
        }
        if old.version() != version {
            return Err(MetadataError::Conflict("database version".into()));
        }
        let old_state = old.value().state;
        if old_state == DatabaseState::DropFailed as i32 {
            return Err(MetadataError::InvalidRecord(
                "database is in terminal DROP_FAILED state",
            ));
        }
        if old_state == DatabaseState::Dropping as i32
            && database.state != DatabaseState::DropFailed as i32
        {
            return Err(MetadataError::InvalidRecord(
                "DROPPING can only become DROP_FAILED or be deleted",
            ));
        }
        if old_state == DatabaseState::Ready as i32
            && database.state == DatabaseState::DropFailed as i32
        {
            return Err(MetadataError::InvalidRecord(
                "drop must enter DROPPING first",
            ));
        }
        if database.state != DatabaseState::Ready as i32
            && (database.name != old.value().name
                || database.owner_user_id != old.value().owner_user_id
                || database.allow_connections != old.value().allow_connections
                || database.connection_limit != old.value().connection_limit)
        {
            return Err(MetadataError::InvalidRecord(
                "lifecycle transition cannot change database attributes",
            ));
        }
        self.owner0(&database).await?;
        if self
            .unique0(Collection::Databases, &database.name)
            .await?
            .is_some_and(|row| row.key != Collection::Databases.key(id).unwrap())
        {
            return Err(MetadataError::AlreadyExists);
        }
        let row = self
            .put0(
                &Collection::Databases.key(id)?,
                database.encode_to_vec(),
                version.value(),
                Some((Collection::Databases.index(), &database.name)),
                &mut blocked,
            )
            .await?;
        decode_database(&row)
    }
    pub(crate) async fn delete_database(&self, id: u32, version: MetadataVersion) -> Result<()> {
        let mut blocked = self.mutations.lock().await;
        if *blocked {
            return Err(MetadataError::UncertainWrite);
        }
        self.ready0().await?;
        let old = self
            .get_database_by_id(id)
            .await?
            .ok_or(MetadataError::NotFound)?;
        if old.value().name == SYSTEM_DATABASE_NAME {
            return Err(MetadataError::Reserved);
        }
        if old.value().state == DatabaseState::DropFailed as i32 {
            return Err(MetadataError::InvalidRecord(
                "database is in terminal DROP_FAILED state",
            ));
        }
        self.delete0(
            &Collection::Databases.key(id)?,
            version.value(),
            &mut blocked,
        )
        .await
    }

    async fn allocate0(
        &self,
        kind: Collection,
        name: &str,
        value: Vec<u8>,
        blocked: &mut bool,
    ) -> Result<Row> {
        // No independent allocator/tracker. Check the current sequence tail before
        // issuing the direct object Put, under the single-writer mutation gate.
        let (first, last) = kind.range();
        for row in self.store.scan(&first, &last).await? {
            if kind.id(&row.key)? == u32::MAX {
                return Err(MetadataError::CounterExhausted("object ID".into()));
            }
        }
        // Cancellation during an in-flight write also leaves the gate blocked.
        // Only a confirmed result/reconciliation clears it.
        *blocked = true;
        let row = match self
            .store
            .allocate(kind.prefix(), value.clone(), kind.index(), name)
            .await
        {
            Ok(row) => row,
            Err(_) => match self.unique0(kind, name).await {
                Ok(Some(row)) if row.value == value => {
                    self.metrics.reconciled("create");
                    row
                }
                _ => {
                    *blocked = true;
                    return Err(MetadataError::UncertainWrite);
                }
            },
        };
        if kind.id(&row.key).is_err() {
            // Never expose a truncated ID or leave a confirmed out-of-domain row.
            if self.store.delete(&row.key, row.version).await.is_err() {
                *blocked = true;
                return Err(MetadataError::UncertainWrite);
            }
            *blocked = false;
            return Err(MetadataError::CounterExhausted("object ID".into()));
        }
        match self.unique0(kind, name).await {
            Ok(Some(found)) if found.key == row.key && found.value == value => {
                *blocked = false;
                Ok(row)
            }
            _ => {
                *blocked = true;
                Err(MetadataError::Integrity(
                    "allocation could not be reconciled uniquely",
                ))
            }
        }
    }

    async fn put0(
        &self,
        key: &str,
        value: Vec<u8>,
        version: i64,
        index: Option<(&str, &str)>,
        blocked: &mut bool,
    ) -> Result<Row> {
        *blocked = true;
        match self
            .store
            .put(key, value.clone(), Condition::Version(version), index)
            .await
        {
            Ok(row) => {
                *blocked = false;
                Ok(row)
            }
            Err(error @ MetadataError::Conflict(_)) => {
                *blocked = false;
                Err(error)
            }
            Err(_) => match self.store.get(key).await {
                Ok(Some(row)) if row.value == value && row.version != version => {
                    self.metrics.reconciled("update");
                    *blocked = false;
                    Ok(row)
                }
                _ => {
                    *blocked = true;
                    Err(MetadataError::UncertainWrite)
                }
            },
        }
    }
    async fn delete0(&self, key: &str, version: i64, blocked: &mut bool) -> Result<()> {
        *blocked = true;
        match self.store.delete(key, version).await {
            Ok(()) => {
                *blocked = false;
                Ok(())
            }
            Err(error @ MetadataError::Conflict(_)) => {
                *blocked = false;
                Err(error)
            }
            Err(_) => match self.store.get(key).await {
                Ok(None) => {
                    self.metrics.reconciled("delete");
                    *blocked = false;
                    Ok(())
                }
                _ => {
                    *blocked = true;
                    Err(MetadataError::UncertainWrite)
                }
            },
        }
    }
}

fn validate_database(database: &Database) -> Result<()> {
    validate_name(&database.name)?;
    if database.owner_user_id == 0 {
        return Err(MetadataError::InvalidRecord("zero database owner"));
    }
    if database.effective_connection_limit() < -1 {
        return Err(MetadataError::InvalidRecord(
            "connection limit must be -1 or nonnegative",
        ));
    }
    DatabaseState::try_from(database.state)
        .map_err(|_| MetadataError::InvalidRecord("unknown database state"))?;
    Ok(())
}

fn decode_database(row: &Row) -> Result<MetadataRecord<Database>> {
    let id = Collection::Databases.id(&row.key)?;
    let database = Database::decode(row.value.as_slice())?;
    validate_database(&database)?;
    Ok(MetadataRecord::new(
        id,
        database,
        MetadataVersion::new(row.version),
    ))
}
fn decode_user(row: &Row) -> Result<MetadataRecord<User>> {
    let id = Collection::Users.id(&row.key)?;
    let user = User::decode(row.value.as_slice())?;
    validate_name(&user.name)?;
    validate_verifier(
        user.password_verifier
            .as_ref()
            .ok_or(MetadataError::InvalidRecord("missing password verifier"))?,
    )?;
    Ok(MetadataRecord::new(
        id,
        user,
        MetadataVersion::new(row.version),
    ))
}
fn public_user(user: MetadataRecord<User>) -> MetadataRecord<UserInfo> {
    MetadataRecord::new(
        user.id(),
        UserInfo {
            name: user.value().name.clone(),
        },
        user.version(),
    )
}

macro_rules! metadata_impl {
    ($ty:ty) => {
        #[async_trait]
        impl Metadata for $ty {
            async fn instance(&self) -> Result<Option<Instance>> {
                self.engine
                    .metrics
                    .observe("get", self.engine.read(self.engine.instance()))
                    .await
            }
            async fn initialize(&self, verifier: ScramSha256Verifier) -> Result<()> {
                self.engine
                    .metrics
                    .observe("create", self.engine.initialize(verifier))
                    .await
            }
            async fn validate_initialized(&self) -> Result<()> {
                self.engine
                    .metrics
                    .observe("get", self.engine.read(self.engine.validate_initialized()))
                    .await
            }
            async fn get_user(&self, name: &str) -> Result<Option<MetadataRecord<UserInfo>>> {
                self.engine
                    .metrics
                    .observe("get", self.engine.read(self.engine.get_user(name)))
                    .await
            }
            async fn get_user_by_id(&self, id: u32) -> Result<Option<MetadataRecord<UserInfo>>> {
                self.engine
                    .metrics
                    .observe("get", self.engine.read(self.engine.get_user_by_id(id)))
                    .await
            }
            async fn list_users(&self) -> Result<Vec<MetadataRecord<UserInfo>>> {
                self.engine
                    .metrics
                    .observe("list", self.engine.read(self.engine.list_users()))
                    .await
            }
            async fn user_verifier(&self, name: &str) -> Result<Option<ScramSha256Verifier>> {
                self.engine
                    .metrics
                    .observe("get", self.engine.read(self.engine.user_verifier(name)))
                    .await
            }
            async fn create_user(
                &self,
                name: &str,
                verifier: ScramSha256Verifier,
            ) -> Result<MetadataRecord<UserInfo>> {
                self.engine
                    .metrics
                    .observe("create", self.engine.create_user(name, verifier))
                    .await
            }
            async fn delete_user(&self, id: u32, version: MetadataVersion) -> Result<()> {
                self.engine
                    .metrics
                    .observe("delete", self.engine.delete_user(id, version))
                    .await
            }
            async fn get_database(&self, name: &str) -> Result<Option<MetadataRecord<Database>>> {
                self.engine
                    .metrics
                    .observe("get", self.engine.read(self.engine.get_database(name)))
                    .await
            }
            async fn get_database_by_id(
                &self,
                id: u32,
            ) -> Result<Option<MetadataRecord<Database>>> {
                self.engine
                    .metrics
                    .observe("get", self.engine.read(self.engine.get_database_by_id(id)))
                    .await
            }
            async fn list_databases(&self) -> Result<Vec<MetadataRecord<Database>>> {
                self.engine
                    .metrics
                    .observe("list", self.engine.read(self.engine.list_databases()))
                    .await
            }
            async fn create_database(
                &self,
                database: Database,
            ) -> Result<MetadataRecord<Database>> {
                self.engine
                    .metrics
                    .observe("create", self.engine.create_database(database))
                    .await
            }
            async fn update_database(
                &self,
                id: u32,
                database: Database,
                version: MetadataVersion,
            ) -> Result<MetadataRecord<Database>> {
                self.engine
                    .metrics
                    .observe("update", self.engine.update_database(id, database, version))
                    .await
            }
            async fn delete_database(&self, id: u32, version: MetadataVersion) -> Result<()> {
                self.engine
                    .metrics
                    .observe("delete", self.engine.delete_database(id, version))
                    .await
            }
            async fn register_component(&self, component: &str) -> Result<Registration> {
                super_validate_component(component)?;
                let result = self.engine.store.register(component).await;
                self.engine
                    .metrics
                    .registration(component, "register", result.is_ok());
                self.engine.metrics.registered(component, result.is_ok());
                let (identity, lease) = result?;
                Ok(Registration::new(
                    identity,
                    lease,
                    self.engine.metrics.clone(),
                ))
            }
            async fn list_components(&self, component: &str) -> Result<Vec<ComponentIdentity>> {
                super_validate_component(component)?;
                self.engine
                    .metrics
                    .observe("list", self.engine.store.registrations(component))
                    .await
            }
            async fn close(&self) -> Result<()> {
                self.engine.store.close().await
            }
        }
    };
}
pub(crate) use metadata_impl;
