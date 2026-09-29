//! LIP-0001 behavior-validation MVP, not the production Catalog implementation.
//!
//! Run the in-memory reference contract with `cargo test --test lip_0001`.
//! Run Oxia on a fresh disposable server via `bash scripts/test-lip-0001.sh`.
//! That script requires OXIA_BIN pointing at an existing Oxia executable.
//!
//! Covered: exact wire values, per-type sequence keys, index lookup/update/delete,
//! single-writer deduplication, conditional writes, empty ephemeral registration,
//! session close/process-death expiry, prototype OTel metrics and structured logs.
//!
//! Not covered: production Metadata integration, SQL/authentication/CLI, protected
//! database policy, FORCE/DROP_FAILED, writer fencing, interrupted/concurrent init,
//! ambiguous-write recovery, u32 exhaustion rollback, OTLP/Prometheus transport,
//! multi-node failover, charts, or chaos CI. The memory fixture is not MemoryMetadata.

#[path = "lip_0001/backend.rs"]
mod backend;
#[path = "lip_0001/telemetry.rs"]
mod telemetry;
#[path = "lip_0001/writer.rs"]
mod writer;

use backend::{
    DATABASE_NAMES, DATABASES, Error, PARTITION, Store, USER_NAMES, USERS, connect, parse_id,
    validate_name,
};
use futures_util::future::join_all;
use lyra_meta::proto::pb_meta::{
    ComponentRegistration, Database, Instance, ScramSha256Verifier, User,
};
use lyra_meta::utils::scram::make_scram;
use oxia::{OxiaClient, OxiaError};
use prost::Message;
use ring::{digest, hmac};
use std::env;
use std::io::{Write, stdout};
use std::process::Stdio;
use std::time::Duration;
use telemetry::Logs;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::time::{sleep, timeout};
use tracing::instrument::WithSubscriber;
use uuid::Uuid;
use writer::Writer;

const TEST_PASSWORD: &str = "mvp-only-password-must-never-be-logged";

fn make_user(name: &str) -> User {
    // Reuse existing SASLprep/PBKDF2 utilities only to construct test fixtures.
    // Persist StoredKey/ServerKey, never plaintext or SaltedPassword.
    let scram = make_scram(TEST_PASSWORD);
    let key = hmac::Key::new(hmac::HMAC_SHA256, &scram.salted_password);
    let client_key = hmac::sign(&key, b"Client Key");
    User {
        name: name.into(),
        password_verifier: Some(ScramSha256Verifier {
            salt: scram.salt,
            iterations: scram.iterations,
            stored_key: digest::digest(&digest::SHA256, client_key.as_ref())
                .as_ref()
                .to_vec()
                .into(),
            server_key: hmac::sign(&key, b"Server Key").as_ref().to_vec().into(),
        }),
    }
}

#[test]
fn wire_schema_and_redaction() {
    assert!(Instance { initialized: None }.encode_to_vec().is_empty());
    assert_eq!(
        Instance {
            initialized: Some(false)
        }
        .encode_to_vec(),
        [8, 0]
    );
    assert_eq!(
        Instance {
            initialized: Some(true)
        }
        .encode_to_vec(),
        [8, 1]
    );
    assert!(ComponentRegistration {}.encode_to_vec().is_empty());
    assert_eq!(
        ComponentRegistration::decode(&[][..]).unwrap(),
        ComponentRegistration {}
    );
    let database = Database {
        name: "db".into(),
        owner_user_id: 7,
    };
    assert_eq!(database.encode_to_vec(), [10, 2, b'd', b'b', 16, 7]);
    assert_eq!(
        Database::decode(database.encode_to_vec().as_slice()).unwrap(),
        database
    );
    let user = make_user("sensitive-user-name");
    let bytes = user.encode_to_vec();
    assert_eq!(User::decode(bytes.as_slice()).unwrap(), user);
    assert!(
        !bytes
            .windows(TEST_PASSWORD.len())
            .any(|b| b == TEST_PASSWORD.as_bytes())
    );
    let verifier = user.password_verifier.as_ref().unwrap();
    assert_eq!(verifier.stored_key.len(), 32);
    assert_eq!(verifier.server_key.len(), 32);
    assert_eq!(format!("{user:?}"), "User { [REDACTED] }");
    assert_eq!(
        format!("{verifier:?}"),
        "ScramSha256Verifier { [REDACTED] }"
    );
    // Lock every verifier field tag, without printing real authentication material.
    let fixture = ScramSha256Verifier {
        salt: vec![1].into(),
        iterations: 4096,
        stored_key: vec![2].into(),
        server_key: vec![3].into(),
    };
    assert_eq!(
        fixture.encode_to_vec(),
        [10, 1, 1, 16, 128, 32, 26, 1, 2, 34, 1, 3]
    );
    assert_eq!(
        User {
            name: "u".into(),
            password_verifier: Some(ScramSha256Verifier::default())
        }
        .encode_to_vec(),
        [10, 1, b'u', 18, 0]
    );
}

#[test]
fn name_and_id_boundaries() {
    for name in ["", "bad\0name", &"a".repeat(64), &"é".repeat(32)] {
        assert!(matches!(validate_name(name), Err(Error::InvalidName)));
    }
    for name in [
        "public",
        "Quoted Name",
        "a/b",
        "中文",
        &"a".repeat(63),
        &"é".repeat(31),
    ] {
        validate_name(name).unwrap();
    }
    assert_eq!(
        parse_id(DATABASES, "/catalog/databases/x-00000000000000000001").unwrap(),
        1
    );
    assert_eq!(
        parse_id(DATABASES, "/catalog/databases/x-00000000004294967295").unwrap(),
        u32::MAX
    );
    for key in [
        "/catalog/databases/x-00000000000000000000",
        "/catalog/databases/x-00000000004294967296",
        "/catalog/databases/x-1",
        "/catalog/databases/x--0000000000000000001",
        "/catalog/users/x-00000000000000000001",
        "/catalog/databases/x-0000000000000000000a",
    ] {
        assert!(matches!(parse_id(DATABASES, key), Err(Error::InvalidId)));
    }
}

#[tokio::test]
async fn memory_contract() {
    let writer = Writer::new(Store::new());
    let logs = Logs::default();
    contract(&writer).with_subscriber(logs.dispatch()).await;
    writer.telemetry.assert_exported(false);
    assert_logs(&logs);
}

async fn contract(writer: &Writer) {
    let store = &writer.store;
    assert!(matches!(store.get("/catalog").await, Err(Error::Missing)));
    assert!(store.list("/catalog/databases").await.unwrap().is_empty());
    assert!(store.list("/catalog/users").await.unwrap().is_empty());
    // These exercise bootstrap storage primitives, not an approved crash-recovery algorithm.
    let initializing = store
        .put(
            "/catalog",
            Instance {
                initialized: Some(false),
            }
            .encode_to_vec(),
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        Instance::decode(store.get("/catalog").await.unwrap().value.as_slice())
            .unwrap()
            .initialized,
        Some(false)
    );
    let root = writer.create_user(make_user("lyrasys")).await.unwrap();
    let root_id = parse_id(USERS, &root.key).unwrap();
    let system = writer
        .create_database(Database {
            name: "lyrasys".into(),
            owner_user_id: root_id,
        })
        .await
        .unwrap();
    let public = writer
        .create_database(Database {
            name: "public".into(),
            owner_user_id: root_id,
        })
        .await
        .unwrap();
    assert_eq!(root_id, 1);
    assert_eq!(
        parse_id(DATABASES, &system.key).unwrap(),
        1,
        "independent sequence domains"
    );
    assert_eq!(parse_id(DATABASES, &public.key).unwrap(), 2);
    let initialized = store
        .put(
            "/catalog",
            Instance {
                initialized: Some(true),
            }
            .encode_to_vec(),
            Some(initializing.version),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        Instance::decode(initialized.value.as_slice())
            .unwrap()
            .initialized,
        Some(true)
    );
    assert!(matches!(
        store
            .put("/catalog", initializing.value.clone(), None, None)
            .await,
        Err(Error::Conflict)
    ));
    assert!(matches!(
        store
            .put(
                "/catalog",
                initializing.value,
                Some(initializing.version),
                None
            )
            .await,
        Err(Error::Conflict)
    ));

    let user = writer.create_user(make_user("public")).await.unwrap();
    assert_eq!(
        store.find(USER_NAMES, "public").await.unwrap().key,
        user.key
    );
    assert_eq!(
        store.find(DATABASE_NAMES, "public").await.unwrap().key,
        public.key
    );
    assert!(matches!(
        writer.create_user(make_user("public")).await,
        Err(Error::Duplicate)
    ));
    for name in ["", "bad\0name", &"é".repeat(32)] {
        assert!(matches!(
            writer
                .create_database(Database {
                    name: name.into(),
                    owner_user_id: root_id
                })
                .await,
            Err(Error::InvalidName)
        ));
        assert!(matches!(
            writer.create_user(make_user(name)).await,
            Err(Error::InvalidName)
        ));
    }
    assert_eq!(store.list("/catalog/databases").await.unwrap().len(), 2);
    assert_eq!(store.list("/catalog/users").await.unwrap().len(), 2);

    let creations = join_all((0..16).map(|_| {
        writer.create_database(Database {
            name: "concurrent-private-name".into(),
            owner_user_id: root_id,
        })
    }))
    .await;
    assert_eq!(creations.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        creations
            .iter()
            .filter(|r| matches!(r, Err(Error::Duplicate)))
            .count(),
        15
    );
    let created = creations.into_iter().find_map(Result::ok).unwrap();
    assert!(matches!(
        writer.rename_database(&created, "public").await,
        Err(Error::Duplicate)
    ));
    let renamed = writer
        .rename_database(&created, "quoted / 数据库")
        .await
        .unwrap();
    assert_eq!(renamed.key, created.key, "rename preserves identity");
    assert_ne!(renamed.version, created.version);
    assert_eq!(
        Database::decode(renamed.value.as_slice())
            .unwrap()
            .owner_user_id,
        root_id
    );
    assert!(matches!(
        store.find(DATABASE_NAMES, "concurrent-private-name").await,
        Err(Error::Missing)
    ));
    assert_eq!(
        store
            .find(DATABASE_NAMES, "quoted / 数据库")
            .await
            .unwrap()
            .key,
        renamed.key
    );
    assert!(matches!(
        writer.rename_database(&created, "stale").await,
        Err(Error::Conflict)
    ));
    assert!(matches!(
        store.find(DATABASE_NAMES, "stale").await,
        Err(Error::Missing)
    ));
    assert!(matches!(
        store.delete(&created.key, created.version).await,
        Err(Error::Conflict)
    ));
    store.delete(&renamed.key, renamed.version).await.unwrap();
    assert!(matches!(store.get(&renamed.key).await, Err(Error::Missing)));
    assert!(matches!(
        store.find(DATABASE_NAMES, "quoted / 数据库").await,
        Err(Error::Missing)
    ));
    store.delete(&user.key, user.version).await.unwrap();
    assert!(matches!(store.get(&user.key).await, Err(Error::Missing)));
    assert!(matches!(
        store.find(USER_NAMES, "public").await,
        Err(Error::Missing)
    ));
    assert_eq!(store.list("/catalog/databases").await.unwrap().len(), 2);
    assert_eq!(store.list("/catalog/users").await.unwrap().len(), 1);
}

fn assert_logs(logs: &Logs) {
    let text = logs.text();
    assert!(text.contains("\"operation\":\"create\""));
    assert!(text.contains("\"outcome\":\"error\""));
    assert!(text.contains("\"outcome\":\"success\""));
    for secret in [
        TEST_PASSWORD,
        "concurrent-private-name",
        "quoted / 数据库",
        "stored_key",
        "server_key",
        "password_verifier",
        "salted_password",
    ] {
        assert!(!text.contains(secret), "sensitive field leaked to logs");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a FRESH disposable Oxia server; use scripts/test-lip-0001.sh"]
async fn oxia_contract() {
    assert_eq!(
        env::var("LYRA_MVP_DISPOSABLE").as_deref(),
        Ok("1"),
        "never run against a shared namespace"
    );
    let address = env::var("OXIA_SERVICE_ADDRESS").expect("explicit disposable address required");
    let client = connect(&address).await;
    let writer = Writer::new(Store::Oxia(client.clone()));
    let logs = Logs::default();
    async {
        contract(&writer).await;
        index_is_not_unique(&writer.store).await;
        observe_sequence_reuse(&writer.store, DATABASES, DATABASE_NAMES).await;
        observe_sequence_reuse(&writer.store, USERS, USER_NAMES).await;
        registration(&address, &client, &writer).await;
    }
    .with_subscriber(logs.dispatch())
    .await;
    writer.telemetry.assert_exported(true);
    assert_logs(&logs);
    client.close().await.unwrap();
    let reconnected = connect(&address).await;
    let store = Store::Oxia(reconnected.clone());
    assert_eq!(
        Instance::decode(store.get("/catalog").await.unwrap().value.as_slice())
            .unwrap()
            .initialized,
        Some(true)
    );
    assert_eq!(store.list("/catalog/databases").await.unwrap().len(), 2);
    assert_eq!(store.list("/catalog/users").await.unwrap().len(), 1);
    reconnected.close().await.unwrap();
}

async fn index_is_not_unique(store: &Store) {
    let value = Database {
        name: "index-is-not-a-constraint".into(),
        owner_user_id: 1,
    }
    .encode_to_vec();
    let first = store
        .allocate(
            DATABASES,
            value.clone(),
            DATABASE_NAMES,
            "index-is-not-a-constraint",
        )
        .await
        .unwrap();
    let second = store
        .allocate(
            DATABASES,
            value,
            DATABASE_NAMES,
            "index-is-not-a-constraint",
        )
        .await
        .unwrap();
    assert_ne!(first.key, second.key);
    assert_eq!(store.list("/catalog/databases").await.unwrap().len(), 4);
    // This bypasses the writer on purpose: lookup + write needs serialization.
    store.delete(&first.key, first.version).await.unwrap();
    store.delete(&second.key, second.version).await.unwrap();
}

async fn observe_sequence_reuse(store: &Store, prefix: &str, index: &str) {
    let value = if prefix == DATABASES {
        Database {
            name: "tail-probe".into(),
            owner_user_id: 1,
        }
        .encode_to_vec()
    } else {
        make_user("tail-probe").encode_to_vec()
    };
    let first = store
        .allocate(prefix, value.clone(), index, "tail-probe")
        .await
        .unwrap();
    store.delete(&first.key, first.version).await.unwrap();
    let next = store
        .allocate(prefix, value, index, "tail-probe")
        .await
        .unwrap();
    if next.key == first.key {
        eprintln!(
            "KNOWN LIMITATION: {prefix} reuses a physically deleted tail ID; non-reuse is NOT validated (awaiting Oxia fix)"
        );
    } else {
        assert!(parse_id(prefix, &next.key).unwrap() > parse_id(prefix, &first.key).unwrap());
        eprintln!("{prefix}: deleted tail ID was not reused by this Oxia build");
    }
    store.delete(&next.key, next.version).await.unwrap();
}

async fn registration(address: &str, durable: &OxiaClient, writer: &Writer) {
    let partition = "discovery/catalog";
    let session = connect(address).await;
    let uuid = Uuid::new_v4();
    let key = format!("/discovery/catalog/instances/{uuid}");
    let result = session
        .put(&key, ComponentRegistration {}.encode_to_vec())
        .partition_key(partition)
        .expected_record_not_exists()
        .ephemeral()
        .await
        .unwrap();
    assert!(result.version.is_ephemeral());
    writer.telemetry.registration("register", true);
    writer.telemetry.assert_registered(true);
    let stored = durable.get(&key).partition_key(partition).await.unwrap();
    assert!(stored.version.is_ephemeral());
    assert_eq!(
        ComponentRegistration::decode(stored.value.unwrap_or_default()).unwrap(),
        ComponentRegistration {}
    );
    let other_component = connect(address).await;
    let other_key = format!("/discovery/func/instances/{uuid}");
    other_component
        .put(&other_key, ComponentRegistration {}.encode_to_vec())
        .partition_key("discovery/func")
        .expected_record_not_exists()
        .ephemeral()
        .await
        .unwrap();
    assert!(matches!(
        other_component
            .put(&key, Vec::new())
            .partition_key(partition)
            .expected_record_not_exists()
            .ephemeral()
            .await,
        Err(OxiaError::UnexpectedVersionId)
    ));
    session.close().await.unwrap();
    wait_absent(durable, &key).await;
    writer.telemetry.registration("unregister", false);
    writer.telemetry.assert_registered(false);
    assert!(
        durable
            .get(&other_key)
            .partition_key("discovery/func")
            .await
            .unwrap()
            .version
            .is_ephemeral()
    );
    other_component.close().await.unwrap();
    timeout(Duration::from_secs(20), async {
        loop {
            match durable
                .get(&other_key)
                .partition_key("discovery/func")
                .await
            {
                Err(OxiaError::KeyNotFound) => break,
                Ok(_) => sleep(Duration::from_millis(100)).await,
                Err(error) => panic!("func registration lookup failed: {error}"),
            }
        }
    })
    .await
    .unwrap();
    assert!(
        durable
            .get("/catalog")
            .partition_key(PARTITION)
            .await
            .is_ok(),
        "registration close must not close durable client"
    );

    // Kill an independent process without closing its SDK session. Server expiry
    // must remove its registration; a healthy session's presence must survive.
    let healthy = connect(address).await;
    let healthy_key = format!("/discovery/catalog/instances/{}", Uuid::new_v4());
    healthy
        .put(&healthy_key, Vec::new())
        .partition_key(partition)
        .ephemeral()
        .await
        .unwrap();
    let mut child = Command::new(env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "registration_process",
            "--nocapture",
        ])
        .env("OXIA_SERVICE_ADDRESS", address)
        .env("LYRA_MVP_REGISTRATION_CHILD", "1")
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap()).lines();
    let crashed_key = timeout(Duration::from_secs(15), async {
        while let Some(line) = output.next_line().await.unwrap() {
            if let Some(key) = line.strip_prefix("REGISTERED ") {
                return key.to_string();
            }
        }
        panic!("registration child exited before creating its record");
    })
    .await
    .unwrap();
    assert_ne!(crashed_key, key);
    assert_ne!(crashed_key, healthy_key);
    assert!(
        durable
            .get(&crashed_key)
            .partition_key(partition)
            .await
            .unwrap()
            .version
            .is_ephemeral()
    );
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    wait_absent(durable, &crashed_key).await;
    assert!(
        durable
            .get(&healthy_key)
            .partition_key(partition)
            .await
            .is_ok()
    );
    healthy.close().await.unwrap();
    wait_absent(durable, &healthy_key).await;
    assert!(
        durable
            .get("/catalog")
            .partition_key(PARTITION)
            .await
            .is_ok()
    );
}

async fn wait_absent(client: &OxiaClient, key: &str) {
    timeout(Duration::from_secs(20), async {
        loop {
            match client.get(key).partition_key("discovery/catalog").await {
                Err(OxiaError::KeyNotFound) => return,
                Ok(_) => sleep(Duration::from_millis(100)).await,
                Err(error) => panic!("presence lookup failed: {error}"),
            }
        }
    })
    .await
    .expect("registration did not disappear within the session-expiry deadline");
}

#[tokio::test]
#[ignore = "internal child process for oxia_contract; not a standalone test"]
async fn registration_process() {
    assert_eq!(env::var("LYRA_MVP_REGISTRATION_CHILD").as_deref(), Ok("1"));
    let client = connect(&env::var("OXIA_SERVICE_ADDRESS").unwrap()).await;
    let key = format!("/discovery/catalog/instances/{}", Uuid::new_v4());
    client
        .put(&key, ComponentRegistration {}.encode_to_vec())
        .partition_key("discovery/catalog")
        .expected_record_not_exists()
        .ephemeral()
        .await
        .unwrap();
    println!("REGISTERED {key}");
    stdout().flush().unwrap();
    // The parent kills this process; no close/cleanup path should run.
    sleep(Duration::from_secs(60)).await;
    panic!("parent did not terminate registration child");
}
