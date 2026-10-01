use axum::{
    Router,
    extract::State,
    http::{Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as HttpBuilder,
    service::TowerToHyperService,
};
use prometheus::{Encoder, Registry, TextEncoder};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct MetricsState {
    registry: Registry,
    path: Arc<RwLock<String>>,
    permits: Arc<Semaphore>,
}

pub(super) struct MetricsServer {
    // Control state
    context: CancellationToken,
    task: Option<JoinHandle<()>>,
    // Mutable state
    path: Arc<RwLock<String>>,
}

impl MetricsServer {
    pub(super) fn new(listener: TcpListener, registry: Registry, path: String) -> Self {
        let context = CancellationToken::new();
        let path = Arc::new(RwLock::new(path));
        let router = Router::new().fallback(scrape).with_state(MetricsState {
            registry,
            path: Arc::clone(&path),
            permits: Arc::new(Semaphore::new(2)),
        });
        let cancel = context.clone();
        let task = tokio::spawn(async move {
            // Own connection tasks: cancelling only axum::serve's accept loop
            // leaves partial-header/slow-reader connections alive after close.
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    Some(_) = connections.join_next(), if !connections.is_empty() => {}
                    accepted = listener.accept() => {
                        let (stream, _) = match accepted {
                            Ok(accepted) => accepted,
                            Err(_) => {
                                tracing::warn!(event = "metrics_accept_failed", "Prometheus accept failed");
                                sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                        };
                        if connections.len() >= 128 { drop(stream); continue; }
                        let service = TowerToHyperService::new(router.clone());
                        connections.spawn(async move {
                            let _ = HttpBuilder::new(TokioExecutor::new())
                                .serve_connection(TokioIo::new(stream), service).await;
                        });
                    }
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Self {
            context,
            task: Some(task),
            path,
        }
    }
    pub(super) fn update_path(&self, path: String) {
        *self.path.write().unwrap_or_else(|e| e.into_inner()) = path;
    }
    pub(super) async fn close(&mut self) {
        self.context.cancel();
        if let Some(mut task) = self.task.take()
            && timeout(Duration::from_secs(2), &mut task).await.is_err()
        {
            task.abort();
            let _ = task.await;
        }
    }
}
impl Drop for MetricsServer {
    fn drop(&mut self) {
        self.context.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn scrape(State(state): State<MetricsState>, method: Method, uri: Uri) -> Response {
    if uri.path() != *state.path.read().unwrap_or_else(|e| e.into_inner()) {
        return StatusCode::NOT_FOUND.into_response();
    }
    if method != Method::GET {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let Ok(permit) = state.permits.try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let result = tokio::task::spawn_blocking(move || {
        // Retain admission until collection finishes, even if the HTTP client
        // disconnects. Slow scrapers cannot create an unbounded blocking queue.
        let _permit = permit;
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&state.registry.gather(), &mut buffer)
            .map(|_| buffer)
    })
    .await;
    match result {
        Ok(Ok(bytes)) => {
            ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], bytes).into_response()
        }
        _ => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
