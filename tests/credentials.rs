use lyra_meta::credentials::{
    CredentialError, MAX_PASSWORD_BYTES, SCRAM_ITERATIONS, make_verifier, validate_verifier,
};
use lyra_meta::proto::pb_meta::ScramSha256Verifier;
use prost::Message;
use std::error::Error;

#[test]
fn generation_needs_no_runtime_and_produces_distinct_round_trippable_verifiers() {
    let first = make_verifier("synthetic-password").unwrap();
    let second = make_verifier("synthetic-password").unwrap();
    for verifier in [&first, &second] {
        validate_verifier(verifier).unwrap();
        assert_eq!(verifier.iterations, SCRAM_ITERATIONS);
        assert_eq!(verifier.salt.len(), 18);
        assert_eq!(verifier.stored_key.len(), 32);
        assert_eq!(verifier.server_key.len(), 32);
        assert_ne!(verifier.stored_key, verifier.server_key);
        assert_eq!(
            ScramSha256Verifier::decode(verifier.encode_to_vec().as_slice()).unwrap(),
            *verifier,
        );
        assert_eq!(
            format!("{verifier:?}"),
            "ScramSha256Verifier { [REDACTED] }"
        );
        assert_eq!(
            format!("{verifier:#?}"),
            "ScramSha256Verifier { [REDACTED] }"
        );
    }
    assert_ne!(first.salt, second.salt);
    assert_ne!(first.stored_key, second.stored_key);
    assert_ne!(first.server_key, second.server_key);
}

#[test]
fn enforces_input_byte_bounds_and_rejects_nul_without_trimming() {
    for input in [
        " ".into(),
        "a".repeat(MAX_PASSWORD_BYTES),
        "é".repeat(MAX_PASSWORD_BYTES / 2),
    ] {
        validate_verifier(&make_verifier(&input).unwrap()).unwrap();
    }
    for input in [
        String::new(),
        "a\0b".into(),
        "a".repeat(MAX_PASSWORD_BYTES + 1),
        "é".repeat(513),
    ] {
        assert_eq!(
            make_verifier(&input).unwrap_err(),
            CredentialError::InvalidPassword
        );
    }
}

#[test]
fn validation_accepts_only_supported_shapes_without_repair() {
    // Synthetic bytes intentionally demonstrate that structural validity is not
    // proof that a password exists or that the salt was securely generated.
    let valid = ScramSha256Verifier {
        salt: vec![17; 16],
        iterations: SCRAM_ITERATIONS,
        stored_key: vec![34; 32],
        server_key: vec![51; 32],
    };
    for length in [16, 18, 64] {
        let record = ScramSha256Verifier {
            salt: vec![17; length],
            ..valid.clone()
        };
        let before = record.encode_to_vec();
        validate_verifier(&record).unwrap();
        assert_eq!(record.encode_to_vec(), before);
    }
    let mut invalid = vec![ScramSha256Verifier::default()];
    for iterations in [0, 1, 4095, 4097, u32::MAX] {
        invalid.push(ScramSha256Verifier {
            iterations,
            ..valid.clone()
        });
    }
    for length in [0, 15, 65] {
        invalid.push(ScramSha256Verifier {
            salt: vec![17; length],
            ..valid.clone()
        });
    }
    for length in [0, 31, 33] {
        invalid.push(ScramSha256Verifier {
            stored_key: vec![34; length],
            ..valid.clone()
        });
        invalid.push(ScramSha256Verifier {
            server_key: vec![51; length],
            ..valid.clone()
        });
    }
    for record in invalid {
        let before = record.encode_to_vec();
        assert_eq!(
            validate_verifier(&record),
            Err(CredentialError::InvalidVerifier)
        );
        assert_eq!(record.encode_to_vec(), before);
    }
}

#[test]
fn raw_protobuf_decoding_does_not_bypass_credential_validation() {
    let decoded = ScramSha256Verifier::decode([].as_slice()).unwrap();
    assert_eq!(
        validate_verifier(&decoded),
        Err(CredentialError::InvalidVerifier)
    );
}

#[test]
fn errors_have_sanitized_display_debug_and_no_sensitive_source() {
    for (error, display, debug) in [
        (
            CredentialError::InvalidPassword,
            "invalid password input",
            "InvalidPassword",
        ),
        (
            CredentialError::InvalidVerifier,
            "invalid SCRAM-SHA-256 verifier",
            "InvalidVerifier",
        ),
        (
            CredentialError::RandomUnavailable,
            "secure randomness is unavailable",
            "RandomUnavailable",
        ),
    ] {
        assert_eq!(error.to_string(), display);
        assert_eq!(format!("{error:?}"), debug);
        assert!(error.source().is_none());
    }
}
