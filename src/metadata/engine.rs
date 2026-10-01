use super::keys::{CATALOG, Collection};
use super::registration::Monitor;
use super::storage::{Condition, Row, Storage};
use super::telemetry::Metrics;
use super::{
    DEFAULT_DATABASE_NAME, MetadataError, MetadataRecord, MetadataVersion, Result,
    SYSTEM_DATABASE_NAME, SYSTEM_USER_NAME, UserInfo, validate_name,
};
use crate::proto::pb_meta::{
    Allocator, Database, DatabaseState, Instance, ScramSha256Verifier, User,
};
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
    pub(crate) monitor: Monitor,
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
            monitor: Monitor::new(Arc::clone(&store), metrics.clone()),
            store,
            metrics,
            mutations: Mutex::new(false),
        }
    }

    pub(crate) async fn fetch_instance(&self) -> Result<Option<Instance>> {
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
        match self.fetch_instance().await? {
            Some(Instance {
                initialized: Some(true),
            }) => Ok(()),
            Some(_) => Err(MetadataError::IncompleteInitialization),
            None => Err(MetadataError::NotInitialized),
        }
    }

    pub(crate) async fn initialize(&self, verifier: ScramSha256Verifier) -> Result<()> {
        self.monitor.check_open()?;
        validate_verifier(&verifier)?;
        // Only local serialization. The false marker is not a distributed lock.
        let _guard = self.mutations.lock().await;
        tracing::info!(
            event = "initialization_started",
            "metadata initialization started"
        );
        let mut marker = self.store.get(CATALOG).await?;
        if marker.is_none() {
            match self
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
                Ok(row) => marker = Some(row),
                Err(MetadataError::Conflict(_)) => marker = self.store.get(CATALOG).await?,
                Err(error) => return Err(error),
            }
        }
        let marker = marker.ok_or(MetadataError::Integrity(
            "initialization marker disappeared",
        ))?;
        match Instance::decode(marker.value.as_slice())?.initialized {
            Some(true) => {
                self.inventory0().await?;
                tracing::info!(
                    event = "initialization_already_complete",
                    "validated completed initialization; no changes"
                );
                return Ok(());
            }
            Some(false) => {
                tracing::info!(
                    event = "initialization_resumed",
                    "ensuring bootstrap records with conditional writes"
                );
            }
            None => {
                return Err(MetadataError::InvalidRecord(
                    "initialization flag is absent",
                ));
            }
        }
        for kind in [Collection::Users, Collection::Databases] {
            if self.store.get(kind.allocator()).await?.is_none() {
                let (first, last) = kind.range();
                if !self.store.scan(&first, &last).await?.is_empty() {
                    return Err(MetadataError::Integrity(
                        "objects exist without their allocator",
                    ));
                }
                match self
                    .store
                    .put(
                        kind.allocator(),
                        Allocator { last_allocated: 0 }.encode_to_vec(),
                        Condition::Missing,
                        None,
                    )
                    .await
                {
                    Ok(_) | Err(MetadataError::Conflict(_)) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        if self.user0(SYSTEM_USER_NAME).await?.is_none() {
            let mut blocked = false;
            let id = self.allocate_id0(Collection::Users, &mut blocked).await?;
            let user = User {
                id,
                name: SYSTEM_USER_NAME.into(),
                password_verifier: Some(verifier),
            };
            match self
                .store
                .put(
                    &Collection::Users.key(SYSTEM_USER_NAME)?,
                    user.encode_to_vec(),
                    Condition::Missing,
                    None,
                )
                .await
            {
                Ok(_) | Err(MetadataError::Conflict(_)) => {}
                Err(error) => return Err(error),
            }
        } else {
            tracing::info!(
                event = "initialization_credentials_retained",
                "existing root credentials retained"
            );
        }
        let owner = self
            .user0(SYSTEM_USER_NAME)
            .await?
            .ok_or(MetadataError::Integrity("system user missing"))?
            .id();
        for name in [SYSTEM_DATABASE_NAME, DEFAULT_DATABASE_NAME] {
            if self.unique0(Collection::Databases, name).await?.is_none() {
                let mut blocked = false;
                let mut database = Database::new(name, owner);
                database.id = self
                    .allocate_id0(Collection::Databases, &mut blocked)
                    .await?;
                database.allow_connections = Some(name != SYSTEM_DATABASE_NAME);
                match self
                    .store
                    .put(
                        &Collection::Databases.key(name)?,
                        database.encode_to_vec(),
                        Condition::Missing,
                        None,
                    )
                    .await
                {
                    Ok(_) | Err(MetadataError::Conflict(_)) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        self.inventory0().await?;
        match self
            .store
            .put(
                CATALOG,
                Instance {
                    initialized: Some(true),
                }
                .encode_to_vec(),
                Condition::Version(marker.version),
                None,
            )
            .await
        {
            Ok(_) => {}
            Err(MetadataError::Conflict(_)) => {
                if !self.is_initialized().await? {
                    return Err(MetadataError::Integrity("initialization marker changed"));
                }
                self.inventory0().await?;
            }
            Err(error) => return Err(error),
        }
        tracing::info!(
            event = "initialization_completed",
            "metadata initialization completed"
        );
        Ok(())
    }

    pub(crate) async fn is_initialized(&self) -> Result<bool> {
        Ok(self
            .fetch_instance()
            .await?
            .is_some_and(|instance| instance.initialized == Some(true)))
    }

    pub(crate) async fn inventory0(&self) -> Result<()> {
        let users = self.list_users().await?;
        let root = users
            .iter()
            .find(|u| u.value().name == SYSTEM_USER_NAME)
            .ok_or(MetadataError::Integrity("system user missing"))?;
        let databases = self.list_databases().await?;
        for (kind, maximum) in [
            (
                Collection::Users,
                users.iter().map(|user| user.id()).max().unwrap_or(0),
            ),
            (
                Collection::Databases,
                databases
                    .iter()
                    .map(|database| database.id())
                    .max()
                    .unwrap_or(0),
            ),
        ] {
            let row = self
                .store
                .get(kind.allocator())
                .await?
                .ok_or(MetadataError::Integrity("allocator missing"))?;
            if Allocator::decode(row.value.as_slice())?.last_allocated < maximum {
                return Err(MetadataError::Integrity("allocator behind object IDs"));
            }
        }
        let system = databases
            .iter()
            .find(|db| db.value().name == SYSTEM_DATABASE_NAME)
            .ok_or(MetadataError::Integrity("system database missing"))?;
        if system.value().owner_user_id != root.id() {
            return Err(MetadataError::Integrity("system database owner mismatch"));
        }
        let public = databases
            .iter()
            .find(|db| db.value().name == DEFAULT_DATABASE_NAME)
            .ok_or(MetadataError::Integrity("default database missing"))?;
        if system.value().accepts_connections()
            || !public.value().accepts_connections()
            || public.value().owner_user_id != root.id()
            || system.value().state != DatabaseState::Ready as i32
            || public.value().state != DatabaseState::Ready as i32
        {
            return Err(MetadataError::Integrity(
                "bootstrap database policy mismatch",
            ));
        }
        Ok(())
    }

    async fn unique0(&self, kind: Collection, name: &str) -> Result<Option<Row>> {
        if let Some(row) = self.store.get(&kind.key(name)?).await? {
            let actual = match kind {
                Collection::Users => decode_user(&row)?.value().name.clone(),
                Collection::Databases => decode_database(&row)?.value().name.clone(),
            };
            if actual != name {
                return Err(MetadataError::Integrity("name key disagrees with record"));
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
        let (first, last) = Collection::Users.range();
        let mut found = None;
        for row in self.store.scan(&first, &last).await? {
            let user = decode_user(&row)?;
            if user.id() == id && found.replace(user).is_some() {
                return Err(MetadataError::Integrity("duplicate user ID"));
            }
        }
        Ok(found)
    }

    pub(crate) async fn fetch_user(&self, name: &str) -> Result<Option<MetadataRecord<UserInfo>>> {
        Ok(self.user0(name).await?.map(public_user))
    }
    pub(crate) async fn fetch_user_by_id(
        &self,
        id: u32,
    ) -> Result<Option<MetadataRecord<UserInfo>>> {
        Ok(self.user_id0(id).await?.map(public_user))
    }
    pub(crate) async fn fetch_user_verifier(
        &self,
        name: &str,
    ) -> Result<Option<ScramSha256Verifier>> {
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
            if !names.insert(user.id()) {
                return Err(MetadataError::Integrity("duplicate user IDs"));
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
            id: 0,
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
        self.delete0(
            &Collection::Users.key(&user.value().name)?,
            version.value(),
            &mut blocked,
        )
        .await
    }
    async fn owner0(&self, database: &Database) -> Result<()> {
        if self.user_id0(database.owner_user_id).await?.is_none() {
            return Err(MetadataError::Integrity("database owner does not exist"));
        }
        Ok(())
    }
    pub(crate) async fn fetch_database(
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
    pub(crate) async fn fetch_database_by_id(
        &self,
        id: u32,
    ) -> Result<Option<MetadataRecord<Database>>> {
        let (first, last) = Collection::Databases.range();
        let mut record = None;
        for row in self.store.scan(&first, &last).await? {
            let database = decode_database(&row)?;
            if database.id() == id && record.replace(database).is_some() {
                return Err(MetadataError::Integrity("duplicate database ID"));
            }
        }
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
            if !names.insert(database.id()) {
                return Err(MetadataError::Integrity("duplicate database IDs"));
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
        if database.id != 0 {
            return Err(MetadataError::InvalidRecord(
                "new database ID must be allocated by Meta",
            ));
        }
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
            .fetch_database_by_id(id)
            .await?
            .ok_or(MetadataError::NotFound)?;
        if old.value().name == SYSTEM_DATABASE_NAME || database.name == SYSTEM_DATABASE_NAME {
            return Err(MetadataError::Reserved);
        }
        if old.version() != version {
            return Err(MetadataError::Conflict("database version".into()));
        }
        if database.id != id || database.name != old.value().name {
            return Err(MetadataError::InvalidRecord(
                "database ID and name are immutable",
            ));
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
        let row = self
            .put0(
                &Collection::Databases.key(&database.name)?,
                database.encode_to_vec(),
                version.value(),
                None,
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
            .fetch_database_by_id(id)
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
            &Collection::Databases.key(&old.value().name)?,
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
        let id = self.allocate_id0(kind, blocked).await?;
        let value = match kind {
            Collection::Users => {
                let mut user = User::decode(value.as_slice())?;
                user.id = id;
                user.encode_to_vec()
            }
            Collection::Databases => {
                let mut database = Database::decode(value.as_slice())?;
                database.id = id;
                database.encode_to_vec()
            }
        };
        let key = kind.key(name)?;
        *blocked = true;
        match self
            .store
            .put(&key, value.clone(), Condition::Missing, None)
            .await
        {
            Ok(row) => {
                *blocked = false;
                Ok(row)
            }
            Err(MetadataError::Conflict(_)) => {
                *blocked = false;
                Err(MetadataError::AlreadyExists)
            }
            Err(_) => match self.store.get(&key).await {
                Ok(Some(row)) if row.value == value => {
                    self.metrics.reconciled("create");
                    *blocked = false;
                    Ok(row)
                }
                _ => Err(MetadataError::UncertainWrite),
            },
        }
    }

    async fn allocate_id0(&self, kind: Collection, blocked: &mut bool) -> Result<u32> {
        loop {
            // Missing counters after bootstrap are corruption, not permission
            // to start again at zero. Deleting objects never deletes this row.
            let row = self
                .store
                .get(kind.allocator())
                .await?
                .ok_or(MetadataError::Integrity("allocator missing"))?;
            let previous = Allocator::decode(row.value.as_slice())?.last_allocated;
            let next = previous
                .checked_add(1)
                .ok_or_else(|| MetadataError::CounterExhausted("object ID".into()))?;
            *blocked = true;
            match self
                .store
                .put(
                    kind.allocator(),
                    Allocator {
                        last_allocated: next,
                    }
                    .encode_to_vec(),
                    Condition::Version(row.version),
                    None,
                )
                .await
            {
                Ok(_) => {
                    *blocked = false;
                    return Ok(next);
                }
                Err(MetadataError::Conflict(_)) => {
                    *blocked = false;
                    tokio::task::yield_now().await;
                }
                // A read-back high-water mark cannot tell which concurrent
                // caller committed it. Never return an unconfirmed ID.
                Err(_) => return Err(MetadataError::UncertainWrite),
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
    let database = Database::decode(row.value.as_slice())?;
    validate_database(&database)?;
    let id = database.id;
    if id == 0 || row.key != Collection::Databases.key(&database.name)? {
        return Err(MetadataError::Integrity("database key or ID mismatch"));
    }
    Ok(MetadataRecord::new(
        id,
        database,
        MetadataVersion::new(row.version),
    ))
}
fn decode_user(row: &Row) -> Result<MetadataRecord<User>> {
    let user = User::decode(row.value.as_slice())?;
    validate_name(&user.name)?;
    let id = user.id;
    if id == 0 || row.key != Collection::Users.key(&user.name)? {
        return Err(MetadataError::Integrity("user key or ID mismatch"));
    }
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
            async fn fetch_instance(&self) -> Result<Option<Instance>> {
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe("get", self.engine.read(self.engine.fetch_instance()))
                    .await
            }
            async fn initialize(&self, verifier: ScramSha256Verifier) -> Result<()> {
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe("create", self.engine.initialize(verifier))
                    .await
            }
            async fn is_initialized(&self) -> Result<bool> {
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe("get", self.engine.read(self.engine.is_initialized()))
                    .await
            }
            async fn fetch_user(&self, name: &str) -> Result<Option<MetadataRecord<UserInfo>>> {
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe("get", self.engine.read(self.engine.fetch_user(name)))
                    .await
            }
            async fn fetch_user_by_id(&self, id: u32) -> Result<Option<MetadataRecord<UserInfo>>> {
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe("get", self.engine.read(self.engine.fetch_user_by_id(id)))
                    .await
            }
            async fn list_users(&self) -> Result<Vec<MetadataRecord<UserInfo>>> {
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe("list", self.engine.read(self.engine.list_users()))
                    .await
            }
            async fn fetch_user_verifier(&self, name: &str) -> Result<Option<ScramSha256Verifier>> {
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe(
                        "get",
                        self.engine.read(self.engine.fetch_user_verifier(name)),
                    )
                    .await
            }
            async fn create_user(
                &self,
                name: &str,
                verifier: ScramSha256Verifier,
            ) -> Result<MetadataRecord<UserInfo>> {
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe("create", self.engine.create_user(name, verifier))
                    .await
            }
            async fn delete_user(&self, id: u32, version: MetadataVersion) -> Result<()> {
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe("delete", self.engine.delete_user(id, version))
                    .await
            }
            async fn fetch_database(&self, name: &str) -> Result<Option<MetadataRecord<Database>>> {
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe("get", self.engine.read(self.engine.fetch_database(name)))
                    .await
            }
            async fn fetch_database_by_id(
                &self,
                id: u32,
            ) -> Result<Option<MetadataRecord<Database>>> {
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe(
                        "get",
                        self.engine.read(self.engine.fetch_database_by_id(id)),
                    )
                    .await
            }
            async fn list_databases(&self) -> Result<Vec<MetadataRecord<Database>>> {
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe("list", self.engine.read(self.engine.list_databases()))
                    .await
            }
            async fn create_database(
                &self,
                database: Database,
            ) -> Result<MetadataRecord<Database>> {
                self.engine.monitor.check_open()?;
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
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe("update", self.engine.update_database(id, database, version))
                    .await
            }
            async fn delete_database(&self, id: u32, version: MetadataVersion) -> Result<()> {
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe("delete", self.engine.delete_database(id, version))
                    .await
            }
            async fn register_catalog_component(&self) -> Result<Component> {
                self.engine.monitor.check_open()?;
                self.engine.monitor.register().await
            }
            async fn is_registered(&self) -> Result<bool> {
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe("get", self.engine.monitor.is_registered())
                    .await
            }
            async fn list_components(&self) -> Result<Vec<Component>> {
                self.engine.monitor.check_open()?;
                self.engine
                    .metrics
                    .observe("list", self.engine.monitor.list())
                    .await
            }
            async fn close(&self) -> Result<()> {
                self.engine.monitor.close().await
            }
        }
    };
}
pub(crate) use metadata_impl;
