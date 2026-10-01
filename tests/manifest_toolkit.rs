use async_trait::async_trait;
use lyra_meta::toolkit::{ManifestWatcher, ReloadError, ReloadTarget, load};
use opentelemetry::global;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use tokio::time::{sleep, timeout};

struct Target {
    applied: AtomicU64,
}
#[async_trait]
impl ReloadTarget for Target {
    type Config = u64;
    type Prepared = u64;
    fn parse(&self, text: &str) -> Result<u64, ReloadError> {
        text.trim()
            .parse()
            .map_err(|_| ReloadError("invalid_test_config"))
    }
    fn changes(&self, _: &u64, new: &u64) -> Result<Vec<&'static str>, ReloadError> {
        if *new > 10 {
            Err(ReloadError("restart_required"))
        } else {
            Ok(vec!["test.live"])
        }
    }
    async fn prepare(&self, _: &u64, new: &u64) -> Result<u64, ReloadError> {
        if *new == 7 {
            Err(ReloadError("bind_failed"))
        } else {
            Ok(*new)
        }
    }
    async fn commit(&self, value: u64, _: u64) {
        self.applied.store(value, Ordering::Release);
    }
}

#[tokio::test]
async fn bounded_loader_and_atomic_replacement_keep_last_good_generation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("manifest");
    std::fs::write(&path, "1").unwrap();
    let target = Arc::new(Target {
        applied: AtomicU64::new(1),
    });
    let mut watcher =
        ManifestWatcher::new(path.clone(), 1, Arc::clone(&target), &global::meter("test"));
    sleep(Duration::from_millis(100)).await;
    for bad in ["invalid", "11", "7"] {
        std::fs::write(&path, bad).unwrap();
        sleep(Duration::from_millis(500)).await;
        assert_eq!(target.applied.load(Ordering::Acquire), 1);
    }
    let replacement = directory.path().join("next");
    std::fs::write(&replacement, "2").unwrap();
    std::fs::rename(&replacement, &path).unwrap();
    timeout(Duration::from_secs(7), async {
        while target.applied.load(Ordering::Acquire) != 2 {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    watcher.close().await.unwrap();
    std::fs::write(&path, vec![b'x'; 65537]).unwrap();
    assert!(load(&path).await.is_err());
    std::fs::write(&path, [0xff]).unwrap();
    assert!(load(&path).await.is_err());
}
