//! Reusable bounded manifest loading and serialized, transactional reloads.
use async_trait::async_trait;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Meter},
};
use std::fs::OpenOptions;
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

pub const MAX_MANIFEST_BYTES: usize = 64 * 1024;

#[derive(Debug, Error)]
#[error("manifest rejected: {0}")]
pub struct ReloadError(pub &'static str);

/// Component code owns its schema and live/restart policy; preparation is the
/// only fallible transition. Dropping a prepared value must release its resources.
#[async_trait]
pub trait ReloadTarget: Send + Sync + 'static {
    type Config: Clone + Eq + Send + Sync;
    type Prepared: Send;
    fn parse(&self, text: &str) -> Result<Self::Config, ReloadError>;
    fn changes(
        &self,
        old: &Self::Config,
        new: &Self::Config,
    ) -> Result<Vec<&'static str>, ReloadError>;
    async fn prepare(
        &self,
        old: &Self::Config,
        new: &Self::Config,
    ) -> Result<Self::Prepared, ReloadError>;
    async fn commit(&self, prepared: Self::Prepared, generation: u64);
}

/// Resolve once without canonicalizing symlinks: ConfigMap projections replace
/// the target, and each read must reopen the originally selected pathname.
pub fn resolve_path(path: &Path) -> Result<PathBuf, ReloadError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()
            .map_err(|_| ReloadError("working_directory"))?
            .join(path))
    }
}

pub async fn load(path: &Path) -> Result<String, ReloadError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_NONBLOCK);
        let file = options
            .open(&path)
            .map_err(|_| ReloadError("file_unavailable"))?;
        let metadata = file
            .metadata()
            .map_err(|_| ReloadError("file_unavailable"))?;
        if !metadata.is_file() {
            return Err(ReloadError("not_regular_file"));
        }
        if metadata.len() > MAX_MANIFEST_BYTES as u64 {
            return Err(ReloadError("file_too_large"));
        }
        let mut bytes = Vec::new();
        file.take(MAX_MANIFEST_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| ReloadError("read_failed"))?;
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(ReloadError("file_too_large"));
        }
        String::from_utf8(bytes).map_err(|_| ReloadError("invalid_utf8"))
    })
    .await
    .map_err(|_| ReloadError("loader_failed"))?
}

pub struct ManifestWatcher {
    // Control state
    context: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl ManifestWatcher {
    pub fn new<T: ReloadTarget>(
        path: PathBuf,
        current: T::Config,
        target: Arc<T>,
        meter: &Meter,
    ) -> Self {
        let context = CancellationToken::new();
        let counter = meter
            .u64_counter("lyra_catalog_manifest_reloads_total")
            .build();
        let generation = meter.u64_gauge("lyra_catalog_manifest_generation").build();
        generation.record(1, &[]);
        let task = tokio::spawn(run(
            path,
            current,
            target,
            context.clone(),
            counter,
            generation,
        ));
        Self {
            context,
            task: Some(task),
        }
    }

    pub async fn close(&mut self) -> Result<(), ReloadError> {
        self.context.cancel();
        if let Some(mut task) = self.task.take()
            && timeout(Duration::from_secs(5), &mut task).await.is_err()
        {
            task.abort();
            let _ = task.await;
            return Err(ReloadError("shutdown_deadline"));
        }
        Ok(())
    }
}
impl Drop for ManifestWatcher {
    fn drop(&mut self) {
        self.context.cancel();
    }
}

async fn run<T: ReloadTarget>(
    path: PathBuf,
    mut current: T::Config,
    target: Arc<T>,
    context: CancellationToken,
    counter: Counter<u64>,
    generation_metric: Gauge<u64>,
) {
    let (tx, mut rx) = mpsc::channel(1);
    let mut watcher: Option<RecommendedWatcher> = None;
    let mut watcher_warned = false;
    let mut last_content: Option<String> = None;
    let mut retry_at = Instant::now();
    let mut failures = 0u32;
    let mut generation = 1u64;
    loop {
        if watcher.is_none() {
            let tx = tx.clone();
            let candidate = notify::recommended_watcher(move |result| {
                let _ = tx.try_send(result);
            });
            if let Ok(mut candidate) = candidate
                && candidate
                    .watch(
                        path.parent().unwrap_or(Path::new(".")),
                        RecursiveMode::NonRecursive,
                    )
                    .is_ok()
            {
                watcher = Some(candidate);
                if watcher_warned {
                    tracing::info!(
                        event = "manifest_watcher_recovered",
                        "manifest watch restored"
                    );
                }
                watcher_warned = false;
            }
            if watcher.is_none() && !watcher_warned {
                tracing::warn!(
                    event = "manifest_watcher_failed",
                    reason = "polling_fallback",
                    "manifest directory watch unavailable"
                );
                watcher_warned = true;
            }
        }
        tokio::select! {
            _ = context.cancelled() => break,
            _ = sleep(Duration::from_secs(5)) => {}
            event = rx.recv() => {
                if matches!(event, Some(Err(_))) { watcher = None; }
                tokio::select! { _ = context.cancelled() => break, _ = sleep(Duration::from_millis(250)) => {} }
                while rx.try_recv().is_ok() {}
            }
        }
        let content = load(&path).await;
        let changed = content.as_ref().ok() != last_content.as_ref();
        if !changed && Instant::now() < retry_at {
            continue;
        }
        if changed {
            failures = 0;
        }
        last_content = content.as_ref().ok().cloned();
        let attempt = async {
            let candidate = target.parse(&content?)?;
            if candidate == current {
                return Ok(None);
            }
            let fields = target.changes(&current, &candidate)?;
            let prepared = target.prepare(&current, &candidate).await?;
            Ok(Some((candidate, prepared, fields)))
        };
        let result = tokio::select! {
            _ = context.cancelled() => break,
            result = timeout(Duration::from_secs(10), attempt) =>
                result.unwrap_or(Err(ReloadError("prepare_deadline"))),
        };
        match result {
            Ok(None) => {
                failures = 0;
            }
            Ok(Some((candidate, prepared, fields))) => {
                generation += 1;
                // Commit is infallible and not cancelled half-way.
                target.commit(prepared, generation).await;
                current = candidate;
                failures = 0;
                counter.add(1, &[KeyValue::new("outcome", "success")]);
                generation_metric.record(generation, &[]);
                tracing::info!(event = "manifest_reload_applied", generation, changed_fields = ?fields,
                    "manifest configuration applied");
            }
            Err(error) => {
                counter.add(1, &[KeyValue::new("outcome", "error")]);
                tracing::warn!(
                    event = "manifest_reload_rejected",
                    reason = error.0,
                    generation,
                    "last successful manifest retained"
                );
                failures = failures.saturating_add(1);
            }
        }
        retry_at = Instant::now() + Duration::from_secs((5u64 << failures.min(3)).min(30));
    }
}
