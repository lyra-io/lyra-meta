use lyra_meta::metadata::{MemoryMetadata, Metadata, MetadataError};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio::time::timeout;

#[tokio::test]
async fn independent_domains_start_at_one_without_initializing_metadata() {
    let metadata: Arc<dyn Metadata> = Arc::new(MemoryMetadata::new());
    assert_eq!(metadata.allocate_database_id().await.unwrap(), 1);
    assert_eq!(metadata.allocate_database_id().await.unwrap(), 2);
    assert_eq!(metadata.allocate_user_id().await.unwrap(), 1);
    assert_eq!(metadata.allocate_user_id().await.unwrap(), 2);
    assert_eq!(metadata.allocate_database_id().await.unwrap(), 3);
    assert!(metadata.fetch_instance().await.unwrap().is_none());
    assert!(!metadata.is_initialized().await.unwrap());

    // Memory is explicitly isolated/non-durable, not a deployment-wide allocator.
    let other = MemoryMetadata::new();
    assert_eq!(other.allocate_database_id().await.unwrap(), 1);
    metadata.close().await.unwrap();
    assert!(matches!(
        metadata.allocate_user_id().await,
        Err(MetadataError::Closed)
    ));
    assert_eq!(other.allocate_user_id().await.unwrap(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_callers_receive_unique_ids_in_each_domain() {
    timeout(Duration::from_secs(5), async {
        let metadata: Arc<dyn Metadata> = Arc::new(MemoryMetadata::new());
        let start = Arc::new(Barrier::new(17));
        let mut tasks = JoinSet::new();
        for _ in 0..16 {
            let metadata = Arc::clone(&metadata);
            let start = Arc::clone(&start);
            tasks.spawn(async move {
                start.wait().await;
                let mut databases = Vec::new();
                let mut users = Vec::new();
                for _ in 0..32 {
                    databases.push(metadata.allocate_database_id().await.unwrap());
                    users.push(metadata.allocate_user_id().await.unwrap());
                    tokio::task::yield_now().await;
                }
                (databases, users)
            });
        }
        start.wait().await;
        let mut databases = Vec::new();
        let mut users = Vec::new();
        while let Some(result) = tasks.join_next().await {
            let (database_ids, user_ids) = result.unwrap();
            databases.extend(database_ids);
            users.extend(user_ids);
        }
        databases.sort_unstable();
        users.sort_unstable();
        assert_eq!(databases, (1..=512).collect::<Vec<_>>());
        assert_eq!(users, (1..=512).collect::<Vec<_>>());
        assert_eq!(metadata.allocate_database_id().await.unwrap(), 513);
        assert_eq!(metadata.allocate_user_id().await.unwrap(), 513);
        assert!(!metadata.is_initialized().await.unwrap());
    })
    .await
    .expect("concurrent ID allocation stalled");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_races_never_duplicate_ids_and_block_later_reservations() {
    timeout(Duration::from_secs(5), async {
        let metadata: Arc<dyn Metadata> = Arc::new(MemoryMetadata::new());
        let start = Arc::new(Barrier::new(17));
        let mut tasks = JoinSet::new();
        for _ in 0..16 {
            let metadata = Arc::clone(&metadata);
            let start = Arc::clone(&start);
            tasks.spawn(async move {
                start.wait().await;
                metadata.allocate_database_id().await
            });
        }
        start.wait().await;
        metadata.close().await.unwrap();
        let mut successful = Vec::new();
        while let Some(result) = tasks.join_next().await {
            match result.unwrap() {
                Ok(id) => {
                    assert_ne!(id, 0);
                    successful.push(id);
                }
                Err(MetadataError::Closed) => {}
                Err(error) => panic!("unexpected allocation error: {error}"),
            }
        }
        let count = successful.len();
        successful.sort_unstable();
        successful.dedup();
        assert_eq!(successful.len(), count);
        assert!(matches!(
            metadata.allocate_database_id().await,
            Err(MetadataError::Closed)
        ));
        assert!(matches!(
            metadata.allocate_user_id().await,
            Err(MetadataError::Closed)
        ));
    })
    .await
    .expect("allocation/close race stalled");
}
