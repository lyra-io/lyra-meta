use super::Result;
use super::telemetry::Metrics;
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::runtime::Handle;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComponentIdentity {
    pub component_type: String,
    pub registration_id: Uuid,
}

#[async_trait]
pub(crate) trait Lease: Send + Sync {
    async fn present(&self) -> Result<bool>;
    async fn close(&self) -> Result<()>;
}

pub struct Registration {
    // Immutable state
    identity: ComponentIdentity,
    lease: Arc<dyn Lease>,
    metrics: Metrics,
    // Mutable state
    closed: AtomicBool,
}

impl Registration {
    pub(crate) fn new(
        identity: ComponentIdentity,
        lease: Arc<dyn Lease>,
        metrics: Metrics,
    ) -> Self {
        Self {
            identity,
            lease,
            metrics,
            closed: AtomicBool::new(false),
        }
    }

    pub fn identity(&self) -> &ComponentIdentity {
        &self.identity
    }

    pub async fn is_registered(&self) -> Result<bool> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(false);
        }
        let result = self.lease.present().await;
        self.metrics
            .registered(&self.identity.component_type, matches!(result, Ok(true)));
        result
    }

    pub async fn unregister(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let result = self.lease.close().await;
        // Cancellation or a failed close must leave Drop able to retry cleanup.
        if result.is_ok() {
            self.closed.store(true, Ordering::Release);
        }
        self.metrics
            .registration(&self.identity.component_type, "unregister", result.is_ok());
        self.metrics
            .registered(&self.identity.component_type, false);
        result
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.metrics
                .registered(&self.identity.component_type, false);
            let lease = Arc::clone(&self.lease);
            if let Ok(runtime) = Handle::try_current() {
                runtime.spawn(async move {
                    let _ = lease.close().await;
                });
            }
            // Without a runtime, Oxia's session timeout remains the fallback.
        }
    }
}
