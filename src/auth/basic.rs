use crate::auth::{AuthenticatedUser, AuthenticationError, AuthenticationProvider, Result};
use crate::metadata::Metadata;
use crate::proto::pb_meta::ScramSha256Verifier;
use crate::utils::verifier::verify_verifier;
use async_trait::async_trait;
use std::sync::Arc;

pub struct BasicAuthenticationProvider {
    // Immutable state
    metadata: Arc<dyn Metadata>,
}

impl BasicAuthenticationProvider {
    pub fn new(metadata: Arc<dyn Metadata>) -> Self {
        Self { metadata }
    }
    pub async fn scram(&self, name: &str) -> Result<ScramSha256Verifier> {
        self.metadata
            .fetch_user_verifier(name)
            .await?
            .ok_or(AuthenticationError::InvalidCredentials)
    }
}

#[async_trait]
impl AuthenticationProvider for BasicAuthenticationProvider {
    async fn authenticate(&self, name: &str, password: &str) -> Result<AuthenticatedUser> {
        let user = self
            .metadata
            .fetch_user(name)
            .await?
            .ok_or(AuthenticationError::InvalidCredentials)?;
        let credential = self.scram(name).await?;
        if !verify_verifier(password, &credential) {
            return Err(AuthenticationError::InvalidCredentials);
        }
        Ok(AuthenticatedUser::new(user.id(), user.value().name.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::MemoryMetadata;
    use crate::utils::verifier::make_verifier;

    #[tokio::test]
    async fn authenticates_existing_user_identity() {
        let metadata = Arc::new(MemoryMetadata::new());
        metadata
            .initialize(make_verifier("secret").unwrap())
            .await
            .unwrap();
        let provider = BasicAuthenticationProvider::new(metadata);
        let user = provider.authenticate("lyrasys", "secret").await.unwrap();
        assert_eq!(user.id(), 1);
        assert_eq!(user.name(), "lyrasys");
        assert!(matches!(
            provider.authenticate("lyrasys", "wrong").await,
            Err(AuthenticationError::InvalidCredentials)
        ));
        assert!(matches!(
            provider.authenticate("unknown", "secret").await,
            Err(AuthenticationError::InvalidCredentials)
        ));
    }
}
