use super::{CredentialError, MAX_PASSWORD_BYTES, Result, SCRAM_ITERATIONS};
use crate::proto::pb_meta::ScramSha256Verifier;
use ring::digest::{SHA256, digest};
use ring::error::Unspecified;
use ring::hmac::{HMAC_SHA256, Key, sign};
use ring::pbkdf2::{PBKDF2_HMAC_SHA256, derive};
use ring::rand::{SecureRandom, SystemRandom};
use std::num::NonZeroU32;
use std::result::Result as StdResult;
use stringprep::saslprep;
use zeroize::Zeroizing;

const SALT_LENGTH: usize = 18;
const KEY_LENGTH: usize = 32;

/// Generate a private SCRAM-SHA-256 verifier with a fresh 18-byte random salt.
///
/// Accepts a nonempty UTF-8 password of at most [`MAX_PASSWORD_BYTES`] bytes,
/// without NUL. No whitespace is trimmed; the caller handles password-file
/// parsing and its separate line/permission rules before calling this helper.
/// Applies SASLprep when it produces a nonempty result; otherwise uses the
/// original UTF-8 password, following PostgreSQL's normalization fallback.
///
/// Uses PBKDF2-HMAC-SHA-256 at [`SCRAM_ITERATIONS`] iterations, then derives the
/// SCRAM stored/server keys. Does not retain plaintext or the salted password in
/// the returned record. Lyra-owned normalized-password and salted-password
/// buffers are zeroized on drop; this does not erase caller-owned input, returned
/// verifier fields, or every temporary inside the crypto/normalization libraries.
///
/// Returns [`CredentialError::InvalidPassword`] before requesting randomness for
/// invalid input. RNG failure returns [`CredentialError::RandomUnavailable`],
/// never a fallback salt or a verifier. This is synchronous CPU work, not login.
pub fn make_verifier(password: &str) -> Result<ScramSha256Verifier> {
    make_verifier0(password, |salt| SystemRandom::new().fill(salt))
}

/// Validate a stored verifier's structure without deriving keys or mutating it.
///
/// Requires exactly [`SCRAM_ITERATIONS`] iterations, 16–64 salt bytes, and two
/// 32-byte keys, retaining the reference MVP's accepted format. Unknown work
/// factors are rejected rather than trusted as an unbounded amount of work.
/// Returns [`CredentialError::InvalidVerifier`] on any mismatch.
///
/// Success does not prove password knowledge, salt randomness, or that the keys
/// were derived together. This is not authentication or password-strength policy.
pub fn validate_verifier(verifier: &ScramSha256Verifier) -> Result<()> {
    if verifier.iterations != SCRAM_ITERATIONS
        || !(16..=64).contains(&verifier.salt.len())
        || verifier.stored_key.len() != KEY_LENGTH
        || verifier.server_key.len() != KEY_LENGTH
    {
        return Err(CredentialError::InvalidVerifier);
    }
    Ok(())
}

fn make_verifier0(
    password: &str,
    fill_salt: impl FnOnce(&mut [u8]) -> StdResult<(), Unspecified>,
) -> Result<ScramSha256Verifier> {
    if password.is_empty() || password.len() > MAX_PASSWORD_BYTES || password.contains('\0') {
        return Err(CredentialError::InvalidPassword);
    }
    let mut salt = [0; SALT_LENGTH];
    fill_salt(&mut salt).map_err(|_| CredentialError::RandomUnavailable)?;
    // PostgreSQL treats an empty SASLprep result as prohibited, too. Never log
    // normalization errors: they may include characters from the password.
    let normalized = Zeroizing::new(match saslprep(password) {
        Ok(value) if !value.is_empty() => value.into_owned(),
        _ => password.to_owned(),
    });
    Ok(derive0(normalized.as_bytes(), &salt))
}

fn derive0(password: &[u8], salt: &[u8]) -> ScramSha256Verifier {
    let mut salted = Zeroizing::new([0; KEY_LENGTH]);
    derive(
        PBKDF2_HMAC_SHA256,
        NonZeroU32::new(SCRAM_ITERATIONS).expect("the fixed iteration count is nonzero"),
        salt,
        password,
        salted.as_mut(),
    );
    let key = Key::new(HMAC_SHA256, salted.as_ref());
    let client = sign(&key, b"Client Key");
    ScramSha256Verifier {
        salt: salt.to_vec(),
        iterations: SCRAM_ITERATIONS,
        stored_key: digest(&SHA256, client.as_ref()).as_ref().to_vec(),
        server_key: sign(&key, b"Server Key").as_ref().to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn derivation_matches_rfc_7677_fixture() {
        // Public RFC 7677 section 3 fixture (password "pencil"), not a real
        // credential: https://www.rfc-editor.org/rfc/rfc7677.html#section-3
        // Stored/server key constants were independently calculated using
        // Python hashlib.pbkdf2_hmac and hmac.digest with SHA-256.
        let salt = [
            91, 109, 153, 104, 157, 18, 53, 142, 236, 160, 75, 20, 18, 54, 250, 129,
        ];
        let verifier = derive0(b"pencil", &salt);
        assert_eq!(verifier.salt, salt);
        assert_eq!(verifier.iterations, 4096);
        assert_eq!(
            verifier.stored_key,
            [
                88, 110, 93, 242, 131, 230, 220, 235, 92, 62, 121, 29, 139, 133, 40, 236, 25, 30,
                102, 64, 69, 206, 151, 23, 146, 226, 230, 181, 187, 19, 226, 166,
            ]
        );
        assert_eq!(
            verifier.server_key,
            [
                193, 243, 203, 193, 193, 58, 157, 53, 161, 76, 9, 144, 238, 217, 118, 41, 234, 34,
                88, 99, 229, 102, 164, 49, 74, 185, 159, 63, 0, 229, 217, 213,
            ]
        );
        validate_verifier(&verifier).unwrap();

        // Also match the published server signature, independent of the locally
        // calculated key constants above. This does not implement a handshake.
        let auth_message = concat!(
            "n=user,r=rOprNGfwEbeRWgbNEkqO,",
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,",
            "s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096,",
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
        );
        let signature = sign(
            &Key::new(HMAC_SHA256, &verifier.server_key),
            auth_message.as_bytes(),
        );
        assert_eq!(
            signature.as_ref(),
            [
                234, 186, 226, 77, 16, 98, 219, 117, 169, 69, 31, 240, 182, 234, 126, 152, 200, 84,
                101, 73, 255, 116, 30, 103, 45, 50, 81, 178, 57, 125, 228, 110,
            ]
        );
    }

    #[test]
    fn normalizes_without_trimming_or_case_folding() {
        for (input, normalized) in [
            ("I\u{00ad}X", "IX"),
            ("\u{00aa}", "a"),
            ("\u{2168}", "IX"),
            ("a\u{00a0}b", "a b"),
            ("e\u{0301}", "\u{00e9}"),
            (" A ", " A "),
        ] {
            let actual = make_verifier0(input, |salt| {
                salt.fill(17);
                Ok(())
            })
            .unwrap();
            assert_eq!(actual, derive0(normalized.as_bytes(), &[17; SALT_LENGTH]));
        }
        assert_ne!(
            derive0(b" A ", &[17; SALT_LENGTH]),
            derive0(b"a", &[17; SALT_LENGTH])
        );
    }

    #[test]
    fn falls_back_to_original_password_for_prohibited_or_empty_normalization() {
        // PostgreSQL's pg_saslprep rejects mapped-to-empty passwords. The
        // stringprep crate returns an empty string, so explicitly handle that.
        // https://github.com/postgres/postgres/blob/REL_18_STABLE/src/common/saslprep.c
        for input in ["a\u{0007}b", "a\u{e000}b", "a\u{0627}", "\u{00ad}"] {
            let actual = make_verifier0(input, |salt| {
                salt.fill(17);
                Ok(())
            })
            .unwrap();
            assert_eq!(actual, derive0(input.as_bytes(), &[17; SALT_LENGTH]));
        }
    }

    #[test]
    fn invalid_input_is_rejected_before_randomness_is_requested() {
        for input in [
            String::new(),
            "a\0b".into(),
            "a".repeat(MAX_PASSWORD_BYTES + 1),
            "é".repeat(513),
        ] {
            let error = make_verifier0(&input, |_| panic!("invalid input requested randomness"))
                .unwrap_err();
            assert_eq!(error, CredentialError::InvalidPassword);
        }
    }

    #[test]
    fn randomness_failure_never_returns_a_partial_verifier_or_retries() {
        let calls = Cell::new(0);
        let error = make_verifier0("synthetic-password", |salt| {
            calls.set(calls.get() + 1);
            salt[0] = 17;
            Err(Unspecified)
        })
        .unwrap_err();
        assert_eq!(calls.get(), 1);
        assert_eq!(error, CredentialError::RandomUnavailable);
    }
}
