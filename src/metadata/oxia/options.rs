use std::time::Duration;

const DEFAULT_SERVICE_ADDRESS: &str = "127.0.0.1:6648";
const DEFAULT_NAMESPACE: &str = "default";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OxiaOptions {
    service_address: String,
    namespace: String,
    request_timeout: Duration,
    session_timeout: Duration,
}

impl OxiaOptions {
    pub fn new(service_address: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            service_address: service_address.into(),
            namespace: namespace.into(),
            request_timeout: Duration::from_secs(5),
            session_timeout: Duration::from_secs(30),
        }
    }

    pub fn service_address(&self) -> &str {
        &self.service_address
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }
    pub fn session_timeout(&self) -> Duration {
        self.session_timeout
    }
    pub fn with_session_timeout(mut self, timeout: Duration) -> Self {
        self.session_timeout = timeout;
        self
    }
}

impl Default for OxiaOptions {
    fn default() -> Self {
        Self::new(DEFAULT_SERVICE_ADDRESS, DEFAULT_NAMESPACE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_the_local_oxia_service() {
        let options = OxiaOptions::default();

        assert_eq!(options.service_address(), "127.0.0.1:6648");
        assert_eq!(options.namespace(), "default");
    }
}
