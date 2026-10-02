use lyra_meta::metadata::{MemoryMetadata, Metadata, MetadataError};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio::time::timeout;

#[test]
fn construction_needs_no_runtime_and_supports_a_shared_trait_object() {
    let _: Arc<dyn Metadata> = Arc::new(MemoryMetadata::new());
    let _: Arc<dyn Metadata> = Arc::new(MemoryMetadata::default());
}

#[tokio::test]
async fn new_clients_are_uninitialized_and_close_independently() {
    let first: Arc<dyn Metadata> = Arc::new(MemoryMetadata::new());
    let second: Arc<dyn Metadata> = Arc::new(MemoryMetadata::new());
    assert!(first.fetch_instance().await.unwrap().is_none());
    assert!(!first.is_initialized().await.unwrap());
    first.close().await.unwrap();
    first.close().await.unwrap();
    assert!(matches!(
        first.fetch_instance().await,
        Err(MetadataError::Closed)
    ));
    assert!(matches!(
        first.is_initialized().await,
        Err(MetadataError::Closed)
    ));
    assert!(second.fetch_instance().await.unwrap().is_none());
    assert!(!second.is_initialized().await.unwrap());
    second.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_client_reads_and_closes_across_tasks() {
    timeout(Duration::from_secs(5), async {
        let metadata: Arc<dyn Metadata> = Arc::new(MemoryMetadata::new());
        let before_close = Arc::new(Barrier::new(17));
        let after_close = Arc::new(Barrier::new(17));
        let mut tasks = JoinSet::new();
        for _ in 0..16 {
            let metadata = Arc::clone(&metadata);
            let before_close = Arc::clone(&before_close);
            let after_close = Arc::clone(&after_close);
            tasks.spawn(async move {
                assert!(metadata.fetch_instance().await.unwrap().is_none());
                assert!(!metadata.is_initialized().await.unwrap());
                before_close.wait().await;
                after_close.wait().await;
                assert!(matches!(
                    metadata.fetch_instance().await,
                    Err(MetadataError::Closed)
                ));
                assert!(matches!(
                    metadata.is_initialized().await,
                    Err(MetadataError::Closed)
                ));
                metadata.close().await.unwrap();
            });
        }
        before_close.wait().await;
        metadata.close().await.unwrap();
        after_close.wait().await;
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await
    .expect("concurrent metadata operations stalled");
}
