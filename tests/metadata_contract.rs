use futures_util::future::join_all;
use lyra_meta::metadata::oxia::{OxiaMetadata, OxiaOptions};
use lyra_meta::metadata::{MemoryMetadata, Metadata, MetadataError};
use lyra_meta::proto::pb_meta::{Database, DatabaseState, Instance};
use lyra_meta::utils::verifier::make_verifier;
use oxia::OxiaClient;
use prost::Message;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

#[tokio::test]
async fn memory_implements_the_metadata_contract() {
    contract(Arc::new(MemoryMetadata::new())).await;
}

async fn contract(metadata: Arc<dyn Metadata>) {
    assert!(matches!(
        metadata.validate_initialized().await,
        Err(MetadataError::NotInitialized)
    ));
    let verifier = make_verifier("private-test-password").unwrap();
    metadata.initialize(verifier.clone()).await.unwrap();
    assert_eq!(
        metadata.instance().await.unwrap().unwrap().initialized,
        Some(true)
    );
    metadata.validate_initialized().await.unwrap();
    let root = metadata.get_user("lyrasys").await.unwrap().unwrap();
    assert_eq!(metadata.list_users().await.unwrap().len(), 1);
    assert_eq!(metadata.list_databases().await.unwrap().len(), 2);
    assert_eq!(
        metadata.user_verifier("lyrasys").await.unwrap().unwrap(),
        verifier
    );
    assert!(matches!(
        metadata
            .initialize(make_verifier("replacement").unwrap())
            .await,
        Err(MetadataError::AlreadyInitialized)
    ));
    assert_eq!(
        metadata.user_verifier("lyrasys").await.unwrap().unwrap(),
        verifier
    );

    let owner = metadata
        .create_user("owner", make_verifier("owner-password").unwrap())
        .await
        .unwrap();
    assert!(matches!(
        metadata
            .create_user("owner", make_verifier("another").unwrap())
            .await,
        Err(MetadataError::AlreadyExists)
    ));
    assert!(matches!(
        metadata
            .create_user("lyrasys", make_verifier("another").unwrap())
            .await,
        Err(MetadataError::Reserved)
    ));
    let database = Database {
        name: "owner".into(),
        owner_user_id: owner.id(),
        ..Default::default()
    };
    let results = join_all((0..16).map(|_| metadata.create_database(database.clone()))).await;
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(MetadataError::AlreadyExists)))
            .count(),
        15
    );
    let created = results.into_iter().find_map(Result::ok).unwrap();
    assert!(matches!(
        metadata.delete_user(owner.id(), owner.version()).await,
        Err(MetadataError::OwnerInUse)
    ));
    assert!(matches!(
        metadata
            .create_database(Database {
                name: "lyrasys".into(),
                owner_user_id: root.id(),
                ..Default::default()
            })
            .await,
        Err(MetadataError::Reserved)
    ));
    for name in ["", "bad\0name", &"x".repeat(64)] {
        assert!(
            metadata
                .create_database(Database {
                    name: name.into(),
                    owner_user_id: root.id(),
                    ..Default::default()
                })
                .await
                .is_err()
        );
    }
    for id in [0, u32::MAX] {
        assert!(
            metadata
                .create_database(Database {
                    name: "unknown-owner".into(),
                    owner_user_id: id,
                    ..Default::default()
                })
                .await
                .is_err()
        );
    }
    let mut value = created.value().clone();
    value.name = "quoted / 数据库".into();
    let renamed = metadata
        .update_database(created.id(), value.clone(), created.version())
        .await
        .unwrap();
    assert_eq!(renamed.id(), created.id());
    assert_eq!(renamed.value().owner_user_id, owner.id());
    assert!(metadata.get_database("owner").await.unwrap().is_none());
    assert_eq!(
        metadata
            .get_database(&value.name)
            .await
            .unwrap()
            .unwrap()
            .id(),
        created.id()
    );
    assert!(matches!(
        metadata
            .update_database(created.id(), value.clone(), created.version())
            .await,
        Err(MetadataError::Conflict(_))
    ));
    assert!(matches!(
        metadata
            .delete_database(created.id(), created.version())
            .await,
        Err(MetadataError::Conflict(_))
    ));
    metadata
        .delete_database(renamed.id(), renamed.version())
        .await
        .unwrap();
    assert!(metadata.get_database(&value.name).await.unwrap().is_none());
    metadata
        .delete_user(owner.id(), owner.version())
        .await
        .unwrap();
    assert!(metadata.get_user("owner").await.unwrap().is_none());

    let first = metadata.register_component("catalog").await.unwrap();
    let second = metadata.register_component("catalog").await.unwrap();
    let other = metadata.register_component("func").await.unwrap();
    assert_ne!(first.identity(), second.identity());
    assert_eq!(metadata.list_components("catalog").await.unwrap().len(), 2);
    assert_eq!(metadata.list_components("func").await.unwrap().len(), 1);
    assert!(first.is_registered().await.unwrap());
    first.unregister().await.unwrap();
    first.unregister().await.unwrap();
    assert!(!first.is_registered().await.unwrap());
    assert!(second.is_registered().await.unwrap());
    timeout(Duration::from_secs(10), async {
        while metadata.list_components("catalog").await.unwrap().len() != 1 {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    second.unregister().await.unwrap();
    other.unregister().await.unwrap();
    for invalid in ["", "/catalog", "CATALOG", "a/b", "a_1", &"a".repeat(33)] {
        assert!(metadata.register_component(invalid).await.is_err());
    }
    let system = metadata.get_database("lyrasys").await.unwrap().unwrap();
    assert!(matches!(
        metadata
            .delete_database(system.id(), system.version())
            .await,
        Err(MetadataError::Reserved)
    ));
    assert!(matches!(
        metadata.delete_user(root.id(), root.version()).await,
        Err(MetadataError::Reserved)
    ));
    let public = metadata.get_database("public").await.unwrap().unwrap();
    metadata
        .delete_database(public.id(), public.version())
        .await
        .unwrap();
    metadata.validate_initialized().await.unwrap();
    assert!(matches!(
        metadata.initialize(verifier).await,
        Err(MetadataError::AlreadyInitialized)
    ));
    assert!(metadata.get_database("public").await.unwrap().is_none());
    let record = metadata
        .create_database(Database::new("failed-drop", root.id()))
        .await
        .unwrap();
    let mut value = record.value().clone();
    assert!(value.accepts_connections());
    assert_eq!(value.effective_connection_limit(), -1);
    value.connection_limit = Some(-2);
    assert!(
        metadata
            .update_database(record.id(), value.clone(), record.version())
            .await
            .is_err()
    );
    value.connection_limit = Some(0);
    value.allow_connections = Some(false);
    let record = metadata
        .update_database(record.id(), value, record.version())
        .await
        .unwrap();
    assert_eq!(record.value().effective_connection_limit(), 0);
    assert!(!record.value().accepts_connections());
    let mut value = record.value().clone();
    value.state = DatabaseState::Dropping as i32;
    let record = metadata
        .update_database(record.id(), value, record.version())
        .await
        .unwrap();
    let mut value = record.value().clone();
    value.state = DatabaseState::Ready as i32;
    assert!(
        metadata
            .update_database(record.id(), value.clone(), record.version())
            .await
            .is_err()
    );
    value.state = DatabaseState::DropFailed as i32;
    let record = metadata
        .update_database(record.id(), value, record.version())
        .await
        .unwrap();
    assert!(
        metadata
            .delete_database(record.id(), record.version())
            .await
            .is_err()
    );
    assert!(
        metadata
            .update_database(record.id(), record.value().clone(), record.version())
            .await
            .is_err()
    );
    metadata.validate_initialized().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a fresh disposable Oxia server"]
async fn oxia_implements_the_metadata_contract() {
    assert_eq!(env::var("LYRA_MVP_DISPOSABLE").as_deref(), Ok("1"));
    let address = env::var("OXIA_SERVICE_ADDRESS").unwrap();
    let options =
        OxiaOptions::new(&address, "default").with_session_timeout(Duration::from_secs(5));
    let raw = OxiaClient::builder()
        .service_address(&address)
        .namespace("default")
        .build()
        .await
        .unwrap();
    let metadata = Arc::new(OxiaMetadata::new(&options).await.unwrap());
    assert!(
        metadata.instance().await.unwrap().is_none(),
        "refuse an existing namespace"
    );
    let marker = raw
        .put(
            "/catalog",
            Instance {
                initialized: Some(false),
            }
            .encode_to_vec(),
        )
        .partition_key("catalog")
        .expected_record_not_exists()
        .await
        .unwrap();
    assert!(matches!(
        metadata.initialize(make_verifier("secret").unwrap()).await,
        Err(MetadataError::IncompleteInitialization)
    ));
    assert!(metadata.list_users().await.unwrap().is_empty());
    // Remove only the exact incomplete marker created by this isolated test.
    raw.delete("/catalog")
        .partition_key("catalog")
        .expected_version_id(marker.version.version_id)
        .await
        .unwrap();
    contract(metadata.clone()).await;
    metadata.close().await.unwrap();
    let restarted = OxiaMetadata::new(&options).await.unwrap();
    restarted.validate_initialized().await.unwrap();
    assert!(restarted.get_database("public").await.unwrap().is_none());

    // Raw writes are test-only corruption injection. A duplicate index must fail
    // closed instead of returning an arbitrary matching record.
    let root = restarted.get_user("lyrasys").await.unwrap().unwrap();
    let value = Database {
        name: "duplicate".into(),
        owner_user_id: root.id(),
        ..Default::default()
    }
    .encode_to_vec();
    let first = raw
        .put("/catalog/databases/x", value.clone())
        .partition_key("catalog")
        .sequence_key_deltas([1])
        .secondary_index("lyra.database.name", "duplicate")
        .await
        .unwrap();
    let second = raw
        .put("/catalog/databases/x", value)
        .partition_key("catalog")
        .sequence_key_deltas([1])
        .secondary_index("lyra.database.name", "duplicate")
        .await
        .unwrap();
    assert!(matches!(
        restarted.get_database("duplicate").await,
        Err(MetadataError::Integrity(_))
    ));
    assert!(matches!(
        restarted.validate_initialized().await,
        Err(MetadataError::Integrity(_))
    ));
    for row in [first, second] {
        raw.delete(row.key)
            .partition_key("catalog")
            .expected_version_id(row.version.version_id)
            .await
            .unwrap();
    }
    restarted.validate_initialized().await.unwrap();
    observe_tail_reuse(&restarted, root.id()).await;
    registration_expiry(&restarted).await;
    restarted.close().await.unwrap();
    raw.close().await.unwrap();
}

async fn observe_tail_reuse(metadata: &dyn Metadata, owner: u32) {
    let first = metadata
        .create_database(Database::new("tail-probe", owner))
        .await
        .unwrap();
    metadata
        .delete_database(first.id(), first.version())
        .await
        .unwrap();
    let second = metadata
        .create_database(Database::new("tail-probe", owner))
        .await
        .unwrap();
    if first.id() == second.id() {
        eprintln!(
            "KNOWN OXIA LIMITATION: physically deleted database tail ID reused; non-reuse is NOT validated"
        );
    } else {
        assert!(second.id() > first.id());
    }
    metadata
        .delete_database(second.id(), second.version())
        .await
        .unwrap();
    let first = metadata
        .create_user("tail-probe", make_verifier("probe").unwrap())
        .await
        .unwrap();
    metadata
        .delete_user(first.id(), first.version())
        .await
        .unwrap();
    let second = metadata
        .create_user("tail-probe", make_verifier("probe").unwrap())
        .await
        .unwrap();
    if first.id() == second.id() {
        eprintln!(
            "KNOWN OXIA LIMITATION: physically deleted user tail ID reused; non-reuse is NOT validated"
        );
    } else {
        assert!(second.id() > first.id());
    }
    metadata
        .delete_user(second.id(), second.version())
        .await
        .unwrap();
}

async fn registration_expiry(metadata: &dyn Metadata) {
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::process::Command;
    let healthy = metadata.register_component("catalog").await.unwrap();
    let mut child = Command::new(env::current_exe().unwrap())
        .args(["--ignored", "--exact", "registration_child", "--nocapture"])
        .env("LYRA_REGISTRATION_CHILD", "1")
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let id = timeout(Duration::from_secs(15), async {
        while let Some(line) = lines.next_line().await.unwrap() {
            if let Some(id) = line.strip_prefix("REGISTERED ") {
                return id.to_string();
            }
        }
        panic!("registration child exited before registration")
    })
    .await
    .unwrap();
    assert!(
        metadata
            .list_components("catalog")
            .await
            .unwrap()
            .iter()
            .any(|r| r.registration_id.to_string() == id)
    );
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    timeout(Duration::from_secs(20), async {
        loop {
            if !metadata
                .list_components("catalog")
                .await
                .unwrap()
                .iter()
                .any(|r| r.registration_id.to_string() == id)
            {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("dead process registration did not expire");
    assert!(healthy.is_registered().await.unwrap());
    metadata.validate_initialized().await.unwrap();
    healthy.unregister().await.unwrap();
    let fresh = metadata.register_component("catalog").await.unwrap();
    assert_ne!(fresh.identity().registration_id.to_string(), id);
    healthy.unregister().await.unwrap();
    assert!(fresh.is_registered().await.unwrap());
    fresh.unregister().await.unwrap();
}

#[tokio::test]
#[ignore = "internal subprocess for the disposable Oxia test"]
async fn registration_child() {
    use std::io::{Write, stdout};
    assert_eq!(env::var("LYRA_REGISTRATION_CHILD").as_deref(), Ok("1"));
    let options = OxiaOptions::new(env::var("OXIA_SERVICE_ADDRESS").unwrap(), "default")
        .with_session_timeout(Duration::from_secs(5));
    let metadata = OxiaMetadata::new(&options).await.unwrap();
    let registration = metadata.register_component("catalog").await.unwrap();
    println!("REGISTERED {}", registration.identity().registration_id);
    stdout().flush().unwrap();
    std::future::pending::<()>().await;
}
