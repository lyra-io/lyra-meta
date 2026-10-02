//! Bootstrap SCRAM-SHA-256 verifier generation and structural validation.
//!
//! These synchronous helpers require no runtime, metadata client, or global
//! initialization. Generation does CPU work and obtains operating-system
//! randomness; asynchronous callers should run it off their executor workers.
//! Password-file validation, authentication, and credential storage are separate.
//!
//! Verifiers are secrets despite their redacted `Debug` output. Do not log their
//! fields or encoded bytes. The caller owns and must protect the input password.

mod error;
mod verifier;

pub use error::CredentialError;
pub use verifier::{make_verifier, validate_verifier};

/// A credential operation result, independent of storage/backend errors.
pub type Result<T> = std::result::Result<T, CredentialError>;

/// Iteration count supported by this bootstrap verifier format.
///
/// This retains the reference MVP's bounded work factor, not a claim that this
/// minimum is ideal for every deployment. Configurable work factors are separate.
pub const SCRAM_ITERATIONS: u32 = 4096;

/// Maximum input password length in UTF-8 bytes, before SASLprep normalization.
pub const MAX_PASSWORD_BYTES: usize = 1024;
