use crate::metadata::{MetadataError, Result};
use crate::proto::pb_meta::ScramSha256Verifier;
use ring::{digest, hmac, pbkdf2};
use std::num::NonZeroU32;
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

pub const SCRAM_ITERATIONS: u32 = 4096;
pub const MAX_PASSWORD_BYTES: usize = 1024;

pub fn validate_verifier(verifier: &ScramSha256Verifier) -> Result<()> {
    if verifier.iterations != SCRAM_ITERATIONS
        || !(16..=64).contains(&verifier.salt.len())
        || verifier.stored_key.len() != 32
        || verifier.server_key.len() != 32
    {
        return Err(MetadataError::InvalidRecord(
            "invalid SCRAM-SHA-256 verifier",
        ));
    }
    Ok(())
}

pub fn make_verifier(password: &str) -> Result<ScramSha256Verifier> {
    if password.is_empty() || password.len() > MAX_PASSWORD_BYTES || password.contains('\0') {
        return Err(MetadataError::InvalidRecord("invalid password input"));
    }
    let salt = rand::random::<[u8; 18]>();
    Ok(derive0(password, &salt))
}

pub fn verify_verifier(password: &str, verifier: &ScramSha256Verifier) -> bool {
    if password.is_empty()
        || password.len() > MAX_PASSWORD_BYTES
        || password.contains('\0')
        || validate_verifier(verifier).is_err()
    {
        return false;
    }
    let candidate = derive0(password, &verifier.salt);
    bool::from(
        candidate
            .stored_key
            .as_ref()
            .ct_eq(verifier.stored_key.as_ref())
            & candidate
                .server_key
                .as_ref()
                .ct_eq(verifier.server_key.as_ref()),
    )
}

fn derive0(password: &str, salt: &[u8]) -> ScramSha256Verifier {
    // PostgreSQL applies SASLprep where possible, falling back to the original
    // UTF-8 password when normalization is prohibited.
    let normalized = Zeroizing::new(
        stringprep::saslprep(password)
            .map_or_else(|_| password.to_string(), |value| value.into_owned()),
    );
    let mut salted = Zeroizing::new([0; 32]);
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        NonZeroU32::new(SCRAM_ITERATIONS).unwrap(),
        salt,
        normalized.as_bytes(),
        salted.as_mut(),
    );
    let key = hmac::Key::new(hmac::HMAC_SHA256, salted.as_ref());
    let client = hmac::sign(&key, b"Client Key");
    ScramSha256Verifier {
        salt: salt.to_vec().into(),
        iterations: SCRAM_ITERATIONS,
        stored_key: digest::digest(&digest::SHA256, client.as_ref())
            .as_ref()
            .to_vec()
            .into(),
        server_key: hmac::sign(&key, b"Server Key").as_ref().to_vec().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_valid_private_verifiers_and_rejects_bad_inputs() {
        let verifier = make_verifier("secret").unwrap();
        validate_verifier(&verifier).unwrap();
        assert!(verify_verifier("secret", &verifier));
        assert!(!verify_verifier("wrong", &verifier));
        assert_ne!(verifier.salt, make_verifier("secret").unwrap().salt);
        for password in ["", "a\0b", &"a".repeat(MAX_PASSWORD_BYTES + 1)] {
            assert!(make_verifier(password).is_err());
        }
        let mut invalid = verifier;
        invalid.iterations = u32::MAX;
        assert!(validate_verifier(&invalid).is_err());
        assert!(!verify_verifier("secret", &invalid));
    }
}
