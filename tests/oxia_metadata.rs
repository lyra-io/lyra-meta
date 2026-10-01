use lyra_meta::metadata::{
    Metadata, MetadataError,
    oxia::{OxiaMetadata, OxiaOptions},
};
use lyra_meta::utils::verifier::make_verifier;
use oxia::OxiaClient;
use std::env;
use std::time::Duration;
use tokio::time::{sleep, timeout};

#[tokio::test]
#[ignore = "requires an explicitly provisioned disposable Oxia namespace"]
async fn foundation_contract_on_real_oxia() {
    let address = env::var("OXIA_SERVICE_ADDRESS").expect("explicit endpoint");
    let namespace = env::var("LYRA_TEST_NAMESPACE").expect("explicit disposable namespace");
    assert!(
        namespace.starts_with("lyra-test-") && namespace.len() > 10,
        "refusing any ordinary deployment namespace"
    );
    let options = OxiaOptions::new(&address, &namespace);
    let a = OxiaMetadata::new(&options).await.unwrap();
    let b = OxiaMetadata::new(&options).await.unwrap();
    assert!(
        !a.is_initialized().await.unwrap(),
        "test namespace must be fresh"
    );
    let (one, two) = tokio::join!(
        a.initialize(make_verifier("first").unwrap()),
        b.initialize(make_verifier("second").unwrap())
    );
    one.unwrap();
    two.unwrap();
    let users = a.list_users().await.unwrap();
    let databases = a.list_databases().await.unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(databases.len(), 2);
    a.initialize(make_verifier("not-a-reset").unwrap())
        .await
        .unwrap();
    assert_eq!(users, a.list_users().await.unwrap());
    assert_eq!(databases, a.list_databases().await.unwrap());
    a.register_catalog_component().await.unwrap();
    b.register_catalog_component().await.unwrap();
    assert!(a.is_registered().await.unwrap());
    assert!(b.is_registered().await.unwrap());
    assert_eq!(a.list_components().await.unwrap().len(), 2);
    assert!(matches!(
        a.register_catalog_component().await,
        Err(MetadataError::RegistrationAttempted)
    ));
    let raw = OxiaClient::builder()
        .identity("lyra-validation-foreign-client")
        .service_address(address)
        .namespace(namespace)
        .build()
        .await
        .unwrap();
    // Discovery enumeration must not treat adjacent namespaces as components.
    raw.put("/discovery-other/item", vec![1]).await.unwrap();
    assert_eq!(a.list_components().await.unwrap().len(), 2);
    raw.delete("/discovery-other/item").await.unwrap();
    let first = "/discovery/catalog/instances/";
    let last = "/discovery/catalog/instances/~";
    let invalid = "/discovery/catalog/instances/not-a-uuid";
    raw.put(invalid, vec![0x12, 0x00])
        .partition_key("discovery/catalog")
        .ephemeral()
        .await
        .unwrap();
    assert!(a.list_components().await.is_err());
    raw.delete(invalid)
        .partition_key("discovery/catalog")
        .await
        .unwrap();
    let records = raw.range_scan(first, last).await.unwrap();
    assert_eq!(records.len(), 2);
    let removed = &records[0];
    raw.delete(&removed.key)
        .partition_key("discovery/catalog")
        .expected_version_id(removed.version.version_id)
        .await
        .unwrap();
    // Recovery must work without HTTP/probe calls and retain exactly the same key.
    timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(record) = raw
                .get(&removed.key)
                .partition_key("discovery/catalog")
                .await
            {
                assert_ne!(record.version.version_id, removed.version.version_id);
                assert_eq!(
                    record.version.client_identity,
                    removed.version.client_identity
                );
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert!(a.is_registered().await.unwrap());
    assert!(b.is_registered().await.unwrap());
    a.close().await.unwrap();
    assert!(b.is_registered().await.unwrap());
    assert_eq!(b.list_components().await.unwrap().len(), 1);
    b.close().await.unwrap();
    assert!(raw.range_scan(first, last).await.unwrap().is_empty());
    assert!(raw.get("/catalog").partition_key("catalog").await.is_ok());
    // Same UUID/payload is not ownership. A different SDK session taking the
    // key is terminal, and conditional cleanup must leave that record alone.
    let c = OxiaMetadata::new(&options).await.unwrap();
    c.register_catalog_component().await.unwrap();
    let row = raw.range_scan(first, last).await.unwrap().remove(0);
    raw.delete(&row.key)
        .partition_key("discovery/catalog")
        .expected_version_id(row.version.version_id)
        .await
        .unwrap();
    let foreign = timeout(Duration::from_secs(10), async {
        loop {
            match raw
                .put(&row.key, vec![0x12, 0x00])
                .partition_key("discovery/catalog")
                .expected_record_not_exists()
                .ephemeral()
                .await
            {
                Ok(value) => break value,
                Err(oxia::OxiaError::UnexpectedVersionId) => {
                    let current = raw
                        .get(&row.key)
                        .partition_key("discovery/catalog")
                        .await
                        .unwrap();
                    let _ = raw
                        .delete(&row.key)
                        .partition_key("discovery/catalog")
                        .expected_version_id(current.version.version_id)
                        .await;
                }
                Err(error) => panic!("foreign-session setup failed: {error}"),
            }
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        c.is_registered().await,
        Err(MetadataError::Integrity(_) | MetadataError::RegistrationMonitorFailed)
    ));
    assert!(
        c.close().await.is_err(),
        "foreign cleanup must not be claimed successful"
    );
    let preserved = raw
        .get(&row.key)
        .partition_key("discovery/catalog")
        .await
        .unwrap();
    assert_eq!(preserved.version.version_id, foreign.version.version_id);
    raw.delete(&row.key)
        .partition_key("discovery/catalog")
        .expected_version_id(foreign.version.version_id)
        .await
        .unwrap();
    raw.close().await.unwrap();
}
