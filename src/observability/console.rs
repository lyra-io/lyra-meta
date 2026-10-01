use super::logging::{ConsoleReload, DynamicLayer};
use crate::config::ConsoleSettings;
use crate::toolkit::ReloadError;
use console_subscriber::{ConsoleLayer, ServerParts};
use std::net::{SocketAddr, TcpListener as Socket};
use std::sync::Arc;
use std::thread::{Builder as ThreadBuilder, JoinHandle};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic_console::transport::Server;
use tracing_subscriber::Layer;

pub(super) struct PreparedConsole {
    layer: DynamicLayer,
    go: oneshot::Sender<()>,
    session: ConsoleSession,
}

pub(super) struct ConsoleSession {
    // Control state
    context: CancellationToken,
    thread: Option<JoinHandle<()>>,
    // Immutable state
    socket: Arc<Socket>,
}

impl PreparedConsole {
    pub(super) async fn new(
        address: SocketAddr,
        config: &ConsoleSettings,
        previous: Option<&ConsoleSession>,
    ) -> Result<Self, ReloadError> {
        let socket = match previous {
            Some(previous) => Arc::clone(&previous.socket),
            None => {
                let socket = Socket::bind(address).map_err(|_| ReloadError("console_bind"))?;
                socket
                    .set_nonblocking(true)
                    .map_err(|_| ReloadError("console_socket"))?;
                Arc::new(socket)
            }
        };
        let listener = socket
            .try_clone()
            .map_err(|_| ReloadError("console_socket"))?;
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| ReloadError("console_runtime"))?;
        let (layer, server) = ConsoleLayer::builder()
            .publish_interval(Duration::from_millis(config.publish_interval_ms))
            .retention(Duration::from_secs(config.retention_seconds))
            .event_buffer_capacity(config.event_buffer_capacity)
            .client_buffer_capacity(128)
            .build();
        let (go, start) = oneshot::channel();
        let (ready, prepared) = oneshot::channel();
        let context = CancellationToken::new();
        let cancel = context.clone();
        let thread = ThreadBuilder::new().name("lyra-tokio-console".into()).spawn(move || {
            runtime.block_on(async move {
                let listener = match TcpListener::from_std(listener) {
                    Ok(listener) => listener,
                    Err(_) => { let _ = ready.send(Err(ReloadError("console_listener"))); return; }
                };
                let _ = ready.send(Ok(()));
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    result = start => if result.is_err() { return; },
                }
                let ServerParts { instrument_server, aggregator, .. } = server.into_parts();
                let grpc = Server::builder().add_service(instrument_server)
                    .serve_with_incoming(TcpListenerStream::new(listener));
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    result = grpc => {
                        if result.is_err() { tracing::warn!(event = "console_failed", "Tokio Console server failed"); }
                    }
                    _ = aggregator.run() => {
                        tracing::warn!(event = "console_collection_failed", "Tokio Console collector stopped");
                    }
                }
            });
            // All connection/aggregator tasks live on this diagnostic runtime.
            // Dropping it terminates active clients and retained history.
            runtime.shutdown_timeout(Duration::from_secs(2));
        }).map_err(|_| ReloadError("console_thread"))?;
        let session = ConsoleSession {
            context,
            thread: Some(thread),
            socket,
        };
        timeout(Duration::from_secs(2), prepared)
            .await
            .map_err(|_| ReloadError("console_prepare_timeout"))?
            .map_err(|_| ReloadError("console_prepare_failed"))??;
        Ok(Self {
            layer: layer.boxed(),
            go,
            session,
        })
    }

    pub(super) fn commit(self, handle: &ConsoleReload) -> ConsoleSession {
        handle
            .reload(Some(self.layer))
            .expect("global subscriber remains installed");
        let _ = self.go.send(());
        tracing::info!(
            event = "console_started",
            "new Tokio Console session enabled"
        );
        self.session
    }
}
impl ConsoleSession {
    pub(super) async fn close(&mut self) {
        self.context.cancel();
        if let Some(thread) = self.thread.take() {
            let _ = timeout(
                Duration::from_secs(3),
                tokio::task::spawn_blocking(move || thread.join()),
            )
            .await;
        }
        tracing::info!(event = "console_stopped", "Tokio Console session stopped");
    }
}
impl Drop for ConsoleSession {
    fn drop(&mut self) {
        self.context.cancel();
    }
}
