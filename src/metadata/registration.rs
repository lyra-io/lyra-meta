//! Private registration lifecycle. No lease handle or subscription escapes Meta.
use super::storage::{Presence, Storage};
use super::telemetry::Metrics;
use super::{MetadataError, Result};
use crate::proto::pb_meta::{CatalogComponent, Component, component::Kind};
use prost::Message;
use rand::random_range;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::thread::{Builder as ThreadBuilder, JoinHandle};
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::{Mutex, Notify, oneshot};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub(crate) const PREFIX: &str = "/discovery/catalog/instances/";
const DEADLINE: Duration = Duration::from_millis(800);
const VERIFY_INTERVAL: Duration = Duration::from_secs(5);
const IDLE: u8 = 0;
const STARTING: u8 = 1;
const READY: u8 = 2;
const RECOVERING: u8 = 3;
const FAILED: u8 = 4;

pub(crate) struct Monitor {
    // Control state
    state: Arc<State>,
    thread: StdMutex<Option<JoinHandle<()>>>,
}

struct State {
    // Control state
    context: CancellationToken,
    // Immutable state
    store: Arc<dyn Storage>,
    metrics: Metrics,
    key: OnceLock<String>,
    wake: Notify,
    // Mutable state
    attempted: AtomicBool,
    closing: AtomicBool,
    worker_alive: AtomicBool,
    cleanup_unconfirmed: AtomicBool,
    status: AtomicU8,
    gate: Mutex<()>,
    last_warning: StdMutex<Option<Instant>>,
}

pub(crate) fn catalog() -> Component {
    Component {
        kind: Some(Kind::Catalog(CatalogComponent {})),
    }
}

pub(crate) fn decode(record: &Presence) -> Result<Component> {
    if record.session.is_none() || record.owner.as_ref().is_none_or(String::is_empty) {
        return Err(MetadataError::Integrity(
            "registration is not session-owned",
        ));
    }
    let suffix = record
        .row
        .key
        .strip_prefix(PREFIX)
        .ok_or(MetadataError::InvalidRecord("unknown discovery key"))?;
    let id = Uuid::parse_str(suffix)
        .map_err(|_| MetadataError::InvalidRecord("invalid registration UUID"))?;
    if id.to_string() != suffix {
        return Err(MetadataError::InvalidRecord(
            "noncanonical registration UUID",
        ));
    }
    let component = Component::decode(record.row.value.as_slice())?;
    if !matches!(component.kind, Some(Kind::Catalog(_))) {
        return Err(MetadataError::InvalidRecord(
            "missing or mismatched component kind",
        ));
    }
    Ok(component)
}

fn owned(state: &State, record: &Presence) -> Result<()> {
    decode(record)?;
    if record.owner.as_deref() != Some(state.store.identity()) {
        return Err(MetadataError::Integrity(
            "registration belongs to another session owner",
        ));
    }
    Ok(())
}

impl Monitor {
    pub(crate) fn new(store: Arc<dyn Storage>, metrics: Metrics) -> Self {
        Self {
            state: Arc::new(State {
                context: CancellationToken::new(),
                store,
                metrics,
                key: OnceLock::new(),
                wake: Notify::new(),
                attempted: AtomicBool::new(false),
                closing: AtomicBool::new(false),
                worker_alive: AtomicBool::new(false),
                cleanup_unconfirmed: AtomicBool::new(false),
                status: AtomicU8::new(IDLE),
                gate: Mutex::new(()),
                last_warning: StdMutex::new(None),
            }),
            thread: StdMutex::new(None),
        }
    }

    pub(crate) fn check_open(&self) -> Result<()> {
        if self.state.closing.load(Ordering::Acquire) {
            Err(MetadataError::Closed)
        } else {
            Ok(())
        }
    }

    pub(crate) async fn register(&self) -> Result<Component> {
        self.check_open()?;
        let state = &self.state;
        if state.attempted.swap(true, Ordering::AcqRel) {
            return Err(MetadataError::RegistrationAttempted);
        }
        state
            .key
            .set(format!("{PREFIX}{}", Uuid::new_v4()))
            .unwrap();
        state.status.store(STARTING, Ordering::Release);
        let (tx, rx) = oneshot::channel();
        let state = Arc::clone(state);
        let thread = ThreadBuilder::new()
            .name("lyra-meta-registration".into())
            .spawn(move || {
                let guard = WorkerGuard(Arc::clone(&state));
                let runtime = match Builder::new_current_thread().enable_all().build() {
                    Ok(runtime) => runtime,
                    Err(_) => {
                        let _ = tx.send(Err(MetadataError::RegistrationMonitorFailed));
                        return;
                    }
                };
                state.worker_alive.store(true, Ordering::Release);
                runtime.block_on(run(Arc::clone(&state), tx));
                drop(guard);
            })
            .map_err(|_| MetadataError::RegistrationMonitorFailed)?;
        *self.thread.lock().unwrap_or_else(|e| e.into_inner()) = Some(thread);
        // Cancellation of the caller must not leave a successful hidden registration.
        struct CancelOnDrop<'a>(&'a State, bool);
        impl Drop for CancelOnDrop<'_> {
            fn drop(&mut self) {
                if self.1 {
                    self.0.context.cancel();
                }
            }
        }
        let mut guard = CancelOnDrop(&self.state, true);
        let result = timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| MetadataError::Timeout)?
            .map_err(|_| MetadataError::RegistrationMonitorFailed)?;
        guard.1 = result.is_err(); // failed attempts never enter recovery
        result?;
        Ok(catalog())
    }

    pub(crate) async fn is_registered(&self) -> Result<bool> {
        self.check_open()?;
        let state = &self.state;
        let Some(key) = state.key.get() else {
            return Ok(false);
        };
        // The entire queue wait + read is bounded. A stopped monitor is checked
        // independently: it never has to answer a probe about its own failure.
        let read = timeout(DEADLINE, async {
            let _gate = state.gate.lock().await;
            self.check_open()?;
            let record = state.store.fetch_presence(key).await;
            if !state.worker_alive.load(Ordering::Acquire)
                || state.status.load(Ordering::Acquire) == FAILED
            {
                return Err(MetadataError::RegistrationMonitorFailed);
            }
            match record? {
                Some(record) => {
                    if let Err(error) = owned(state, &record) {
                        state.status.store(FAILED, Ordering::Release);
                        return Err(error);
                    }
                    Ok(state.status.load(Ordering::Acquire) == READY)
                }
                None => {
                    lost(state, "absent");
                    state.wake.notify_one();
                    Ok(false)
                }
            }
        })
        .await;
        match read {
            Ok(result) => result,
            Err(_)
                if !state.worker_alive.load(Ordering::Acquire)
                    || state.status.load(Ordering::Acquire) == FAILED =>
            {
                Err(MetadataError::RegistrationMonitorFailed)
            }
            Err(_) => Err(MetadataError::Timeout),
        }
    }

    pub(crate) async fn list(&self) -> Result<Vec<Component>> {
        self.check_open()?;
        self.state
            .store
            .list_presence()
            .await?
            .iter()
            .map(decode)
            .collect()
    }

    pub(crate) async fn close(&self) -> Result<()> {
        // Mark closing before cancellation or waiting for in-flight operations.
        self.state.closing.store(true, Ordering::Release);
        self.state.context.cancel();
        let thread = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(thread) = thread {
            timeout(
                Duration::from_secs(7),
                tokio::task::spawn_blocking(move || thread.join()),
            )
            .await
            .map_err(|_| MetadataError::Timeout)?
            .map_err(|_| MetadataError::RegistrationMonitorFailed)?
            .map_err(|_| MetadataError::RegistrationMonitorFailed)?;
        }
        // Session cleanup is retryable, including after cancelled close().
        let closed = timeout(Duration::from_secs(2), self.state.store.close())
            .await
            .map_err(|_| MetadataError::Timeout)?;
        closed?;
        if self.state.cleanup_unconfirmed.load(Ordering::Acquire) {
            // Closing the local SDK is not proof that an uncertain remote delete
            // succeeded. Session expiry remains the final fallback.
            Err(MetadataError::Timeout)
        } else {
            Ok(())
        }
    }
}

impl Drop for Monitor {
    fn drop(&mut self) {
        self.state.closing.store(true, Ordering::Release);
        self.state.context.cancel();
    }
}

struct WorkerGuard(Arc<State>);
impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.0.worker_alive.store(false, Ordering::Release);
        self.0.metrics.registered("catalog", false);
        if !self.0.closing.load(Ordering::Acquire) {
            self.0.status.store(FAILED, Ordering::Release);
        }
    }
}

fn lost(state: &State, reason: &'static str) {
    if state.status.load(Ordering::Acquire) == FAILED {
        return;
    }
    state.status.store(RECOVERING, Ordering::Release);
    state.metrics.registered("catalog", false);
    let mut last = state.last_warning.lock().unwrap_or_else(|e| e.into_inner());
    if last.is_none_or(|last| last.elapsed() >= Duration::from_secs(5)) {
        tracing::warn!(
            event = "registration_maybe_lost",
            reason,
            registration_uuid = state.key.get().and_then(|key| key.strip_prefix(PREFIX)),
            "component presence may have been lost"
        );
        *last = Some(Instant::now());
    }
}

async fn run(state: Arc<State>, initial: oneshot::Sender<Result<()>>) {
    let key = state.key.get().unwrap();
    let setup = async {
        // Subscription establishment must be confirmed before the initial put.
        let events = state.store.subscribe(key).await?;
        let row = state
            .store
            .create_presence(key, catalog().encode_to_vec())
            .await?;
        owned(&state, &row)?;
        let row = state
            .store
            .fetch_presence(key)
            .await?
            .ok_or(MetadataError::NotFound)?;
        owned(&state, &row)?;
        Ok(events)
    };
    let setup = tokio::select! {
        biased;
        _ = state.context.cancelled() => Err(MetadataError::Closed),
        result = timeout(Duration::from_secs(8), setup) => result.unwrap_or(Err(MetadataError::Timeout)),
    };
    state
        .metrics
        .registration("catalog", "register", setup.is_ok());
    let mut events = match setup {
        Ok(events) => events,
        Err(error) => {
            let _ = initial.send(Err(error));
            cleanup(&state).await;
            return;
        }
    };
    state.status.store(READY, Ordering::Release);
    state.metrics.registered("catalog", true);
    if initial.send(Ok(())).is_err() {
        state.context.cancel();
    }
    let mut failures = 0u32;
    loop {
        let delay = if failures == 0 {
            VERIFY_INTERVAL
        } else {
            Duration::from_millis((100u64 << failures.min(5)) + random_range(0..100))
        };
        tokio::select! {
            biased;
            _ = state.context.cancelled() => break,
            _ = sleep(delay) => {}
            _ = state.wake.notified(), if failures == 0 => {}
            alive = events.next(), if failures == 0 => {
                if !alive { state.status.store(FAILED, Ordering::Release); break; }
            }
        }
        if state.status.load(Ordering::Acquire) == FAILED {
            break;
        }
        let _gate = state.gate.lock().await;
        if state.context.is_cancelled() {
            break;
        }
        let result = timeout(DEADLINE, async {
            match state.store.fetch_presence(key).await? {
                Some(row) => owned(&state, &row),
                None => {
                    lost(&state, "absent");
                    if state.context.is_cancelled() {
                        return Err(MetadataError::Closed);
                    }
                    let result = state
                        .store
                        .create_presence(key, catalog().encode_to_vec())
                        .await;
                    state
                        .metrics
                        .registration("catalog", "register", result.is_ok());
                    // Never blindly repeat an uncertain write. Next iteration rereads.
                    owned(&state, &result?)?;
                    let row = state
                        .store
                        .fetch_presence(key)
                        .await?
                        .ok_or(MetadataError::NotFound)?;
                    owned(&state, &row)
                }
            }
        })
        .await
        .unwrap_or(Err(MetadataError::Timeout));
        match result {
            Ok(()) => {
                if state.status.swap(READY, Ordering::AcqRel) == RECOVERING {
                    tracing::info!(
                        event = "registration_recovered",
                        registration_uuid = key.strip_prefix(PREFIX),
                        "component presence recovered"
                    );
                }
                state.metrics.registered("catalog", true);
                failures = 0;
            }
            Err(
                MetadataError::Integrity(_)
                | MetadataError::InvalidRecord(_)
                | MetadataError::Decode(_),
            ) => {
                state.status.store(FAILED, Ordering::Release);
                break;
            }
            Err(_) => {
                lost(&state, "backend_uncertain");
                failures = failures.saturating_add(1);
            }
        }
    }
    cleanup(&state).await;
}

async fn cleanup(state: &State) {
    // Serializes cleanup with all recovery puts. Conditional delete cannot erase
    // a replaced record. A cancelled/uncertain put still belongs to our session.
    let _gate = state.gate.lock().await;
    let key = state.key.get().unwrap();
    let result = timeout(Duration::from_secs(2), async {
        if let Some(row) = state.store.fetch_presence(key).await? {
            owned(state, &row)?;
            state.store.delete_presence(key, row.row.version).await?;
        }
        Ok::<_, MetadataError>(())
    })
    .await;
    state
        .metrics
        .registration("catalog", "unregister", matches!(result, Ok(Ok(()))));
    state
        .cleanup_unconfirmed
        .store(!matches!(result, Ok(Ok(()))), Ordering::Release);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::memory::MemoryStorage;

    #[tokio::test]
    async fn deleted_presence_recovers_same_key_without_probes_or_rewrites() {
        let store = Arc::new(MemoryStorage::default());
        let monitor = Monitor::new(store.clone(), Metrics::new("memory", None));
        monitor.register().await.unwrap();
        let first = store.list_presence().await.unwrap().remove(0);
        for _ in 0..5 {
            assert!(monitor.is_registered().await.unwrap());
        }
        assert_eq!(
            store
                .fetch_presence(&first.row.key)
                .await
                .unwrap()
                .unwrap()
                .row
                .version,
            first.row.version
        );
        store
            .delete_presence(&first.row.key, first.row.version)
            .await
            .unwrap();
        timeout(Duration::from_secs(7), async {
            loop {
                if let Some(row) = store.fetch_presence(&first.row.key).await.unwrap() {
                    assert_ne!(row.row.version, first.row.version);
                    break;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        assert!(monitor.is_registered().await.unwrap());
        assert_eq!(store.list_presence().await.unwrap().len(), 1);
        monitor.close().await.unwrap();
    }

    #[tokio::test]
    async fn stopped_worker_is_a_terminal_error_not_a_transient_timeout() {
        let store = Arc::new(MemoryStorage::default());
        let monitor = Monitor::new(store, Metrics::new("memory", None));
        monitor.register().await.unwrap();
        monitor.state.context.cancel();
        timeout(Duration::from_secs(3), async {
            while monitor.state.worker_alive.load(Ordering::Acquire) {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            monitor.is_registered().await,
            Err(MetadataError::RegistrationMonitorFailed)
        ));
        monitor.close().await.unwrap();
    }

    #[test]
    fn protobuf_kind_and_key_are_required_but_payload_has_no_id() {
        let mut record = Presence {
            row: super::super::storage::Row {
                key: format!("{PREFIX}{}", Uuid::new_v4()),
                value: catalog().encode_to_vec(),
                version: 1,
            },
            session: Some(1),
            owner: Some("test-client".into()),
        };
        assert_eq!(record.row.value, vec![0x12, 0x00]);
        assert!(decode(&record).is_ok());
        record.row.value.clear();
        assert!(decode(&record).is_err());
        record.row.value = catalog().encode_to_vec();
        record.row.key = record.row.key.to_uppercase();
        assert!(decode(&record).is_err());
    }
}
