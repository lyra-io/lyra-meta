use thiserror::Error;

/// Sanitized credential failures; no variant contains password or verifier data.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum CredentialError {
    /// The password is empty, exceeds the byte limit, or contains NUL.
    #[error("invalid password input")]
    InvalidPassword,
    /// The verifier does not meet this version's structural requirements.
    #[error("invalid SCRAM-SHA-256 verifier")]
    InvalidVerifier,
    /// Operating-system randomness could not produce a fresh salt.
    #[error("secure randomness is unavailable")]
    RandomUnavailable,
}
