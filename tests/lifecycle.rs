use lyra_meta::metadata::{MemoryMetadata, Metadata, MetadataError};
use lyra_meta::proto::pb_meta::component::Kind;
use lyra_meta::utils::verifier::make_verifier;
use std::future::{Future, poll_fn};
use std::task::Poll;

#[tokio::test]
async fn bootstrap_is_idempotent_preserves_credentials_and_ids() {
    let meta = MemoryMetadata::new();
    assert!(!meta.is_initialized().await.unwrap());
    let first = make_verifier("first-private-password").unwrap();
    meta.initialize(first.clone()).await.unwrap();
    let root = meta.fetch_user("lyrasys").await.unwrap().unwrap();
    let databases = meta.list_databases().await.unwrap();
    meta.initialize(make_verifier("not-a-password-reset").unwrap())
        .await
        .unwrap();
    assert!(meta.is_initialized().await.unwrap());
    assert_eq!(meta.fetch_user("lyrasys").await.unwrap().unwrap(), root);
    assert_eq!(meta.list_databases().await.unwrap(), databases);
    assert_eq!(
        meta.fetch_user_verifier("lyrasys").await.unwrap().unwrap(),
        first
    );
    assert_eq!(databases.len(), 2);
    assert_ne!(databases[0].id(), databases[1].id());
    meta.close().await.unwrap();
    meta.close().await.unwrap();
    assert!(matches!(
        meta.is_initialized().await,
        Err(MetadataError::Closed)
    ));
}

#[tokio::test]
async fn concurrent_bootstrap_and_independent_registration() {
    let a = MemoryMetadata::new();
    let b = a.new_client();
    let (first, second) = tokio::join!(
        a.initialize(make_verifier("first").unwrap()),
        b.initialize(make_verifier("second").unwrap())
    );
    first.unwrap();
    second.unwrap();
    assert_eq!(a.list_users().await.unwrap().len(), 1);
    assert!(!a.is_registered().await.unwrap());
    let component = a.register_catalog_component().await.unwrap();
    assert!(matches!(component.kind, Some(Kind::Catalog(_))));
    let _ = component;
    b.register_catalog_component().await.unwrap();
    assert!(matches!(
        a.register_catalog_component().await,
        Err(MetadataError::RegistrationAttempted)
    ));
    assert!(a.is_registered().await.unwrap());
    assert!(b.is_registered().await.unwrap());
    assert_eq!(a.list_components().await.unwrap().len(), 2);
    // Cancel a caller after it starts shutdown, then resume concurrently. The
    // private worker must still finish SDK cleanup before either call succeeds.
    let mut cancelled = Box::pin(a.close());
    poll_fn(|cx| {
        let _ = cancelled.as_mut().poll(cx);
        Poll::Ready(())
    })
    .await;
    drop(cancelled);
    let (one, two) = tokio::join!(a.close(), a.close());
    one.unwrap();
    two.unwrap();
    assert!(b.is_registered().await.unwrap());
    assert_eq!(b.list_components().await.unwrap().len(), 1);
    assert!(b.is_initialized().await.unwrap());
    b.close().await.unwrap();
}
