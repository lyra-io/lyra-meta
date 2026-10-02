use lyra_meta::proto::pb_meta::{Allocator, Database, DatabaseState, ScramSha256Verifier, User};
use prost::Message;

#[test]
fn database_preserves_reference_field_tags_and_values() {
    let record = Database {
        name: "d".into(),
        owner_user_id: 7,
        allow_connections: Some(false),
        connection_limit: Some(-1),
        state: DatabaseState::DropFailed as i32,
        id: 9,
    };
    let bytes = [
        0x0a, 0x01, b'd', // name, field 1.
        0x10, 0x07, // owner user ID, field 2.
        0x18, 0x00, // explicit false, field 3.
        0x20, 0x01, // signed -1 in ZigZag encoding, field 4.
        0x28, 0x02, // DROP_FAILED, field 5.
        0x30, 0x09, // database ID, field 6.
    ];
    assert_eq!(record.encode_to_vec(), bytes);
    assert_eq!(Database::decode(bytes.as_slice()).unwrap(), record);
}

#[test]
fn database_options_distinguish_absence_from_explicit_false_and_zero() {
    for (allow_connections, connection_limit, bytes) in [
        (None, None, vec![]),
        (Some(false), Some(0), vec![0x18, 0x00, 0x20, 0x00]),
        (Some(true), Some(-1), vec![0x18, 0x01, 0x20, 0x01]),
    ] {
        // These are wire fixtures, not valid persisted database records.
        let record = Database {
            allow_connections,
            connection_limit,
            ..Database::default()
        };
        assert_eq!(record.encode_to_vec(), bytes);
        assert_eq!(Database::decode(bytes.as_slice()).unwrap(), record);
    }
}

#[test]
fn database_state_keeps_numeric_values_and_unknown_states_detectable() {
    for (state, value) in [
        (DatabaseState::Ready, 0),
        (DatabaseState::Dropping, 1),
        (DatabaseState::DropFailed, 2),
    ] {
        assert_eq!(state as i32, value);
        assert_eq!(DatabaseState::try_from(value).unwrap(), state);
    }
    let unknown = Database::decode([0x28, 0x63].as_slice()).unwrap();
    assert_eq!(unknown.state, 99);
    assert!(DatabaseState::try_from(unknown.state).is_err());
    assert_eq!(unknown.encode_to_vec(), [0x28, 0x63]);
}

#[test]
fn allocator_preserves_zero_and_full_u32_high_water_mark() {
    for (last_allocated, bytes) in [
        (0, vec![]),
        (1, vec![0x08, 0x01]),
        (u32::MAX, vec![0x08, 0xff, 0xff, 0xff, 0xff, 0x0f]),
    ] {
        let record = Allocator { last_allocated };
        assert_eq!(record.encode_to_vec(), bytes);
        assert_eq!(Allocator::decode(bytes.as_slice()).unwrap(), record);
    }
}

#[test]
fn user_and_verifier_preserve_reference_field_tags() {
    // Deliberately short, synthetic bytes exercise the wire format only. This
    // fixture is not a valid cryptographic verifier and contains no credentials.
    let verifier = ScramSha256Verifier {
        salt: vec![0x11],
        iterations: 4096,
        stored_key: vec![0x22],
        server_key: vec![0x33],
    };
    let verifier_bytes = [
        0x0a, 0x01, 0x11, 0x10, 0x80, 0x20, 0x1a, 0x01, 0x22, 0x22, 0x01, 0x33,
    ];
    assert_eq!(verifier.encode_to_vec(), verifier_bytes);
    assert_eq!(
        ScramSha256Verifier::decode(verifier_bytes.as_slice()).unwrap(),
        verifier
    );

    let user = User {
        name: "u".into(),
        password_verifier: Some(verifier),
        id: 7,
    };
    let mut user_bytes = vec![0x0a, 0x01, b'u', 0x12, 0x0c];
    user_bytes.extend(verifier_bytes);
    user_bytes.extend([0x18, 0x07]);
    assert_eq!(user.encode_to_vec(), user_bytes);
    assert_eq!(User::decode(user_bytes.as_slice()).unwrap(), user);
}

#[test]
fn user_verifier_presence_is_not_credential_validation() {
    let absent = User::decode([].as_slice()).unwrap();
    assert_eq!(absent.password_verifier, None);
    assert_eq!(absent.id, 0);
    let present = User::decode([0x12, 0x00].as_slice()).unwrap();
    assert_eq!(
        present.password_verifier,
        Some(ScramSha256Verifier::default())
    );
    assert_eq!(present.encode_to_vec(), [0x12, 0x00]);
    // Both are invalid user records; semantic validation is a later layer.
}

#[test]
fn unknown_fields_do_not_erase_known_bootstrap_fields() {
    // Field 127 is outside these schemas. Known fields remain readable.
    let suffix = [0xf8, 0x07, 0x01];
    let mut bytes = vec![0x30, 0x09];
    bytes.extend(suffix);
    assert_eq!(Database::decode(bytes.as_slice()).unwrap().id, 9);

    let mut bytes = vec![0x18, 0x07];
    bytes.extend(suffix);
    assert_eq!(User::decode(bytes.as_slice()).unwrap().id, 7);

    let mut bytes = vec![0x08, 0x05];
    bytes.extend(suffix);
    assert_eq!(
        Allocator::decode(bytes.as_slice()).unwrap().last_allocated,
        5
    );

    let mut bytes = vec![0x10, 0x80, 0x20];
    bytes.extend(suffix);
    assert_eq!(
        ScramSha256Verifier::decode(bytes.as_slice())
            .unwrap()
            .iterations,
        4096
    );
}

#[test]
fn malformed_bootstrap_records_fail_wire_decoding() {
    for bytes in [&[0x00][..], &[0x0a, 0x01][..], &[0x0a, 0x01, 0xff][..]] {
        assert!(Database::decode(bytes).is_err());
        assert!(User::decode(bytes).is_err());
    }
    // A malformed nested verifier must fail the enclosing user too.
    assert!(User::decode([0x12, 0x01, 0x00].as_slice()).is_err());
    for bytes in [&[0x00][..], &[0x08][..], &[0x0a, 0x00][..]] {
        assert!(Allocator::decode(bytes).is_err());
    }
    for bytes in [&[0x00][..], &[0x0a, 0x01][..], &[0x10][..]] {
        assert!(ScramSha256Verifier::decode(bytes).is_err());
    }
}

#[test]
fn credential_debug_output_is_redacted_without_changing_serialization() {
    let verifier = ScramSha256Verifier {
        salt: vec![0x11; 16],
        iterations: 4096,
        stored_key: vec![0x22; 32],
        server_key: vec![0x33; 32],
    };
    let bytes = verifier.encode_to_vec();
    assert_eq!(
        format!("{verifier:?}"),
        "ScramSha256Verifier { [REDACTED] }"
    );
    assert_eq!(
        format!("{verifier:#?}"),
        "ScramSha256Verifier { [REDACTED] }"
    );
    assert_eq!(
        ScramSha256Verifier::decode(bytes.as_slice()).unwrap(),
        verifier
    );

    let user = User {
        name: "synthetic-user".into(),
        password_verifier: Some(verifier),
        id: 7,
    };
    let bytes = user.encode_to_vec();
    assert_eq!(format!("{user:?}"), "User { [REDACTED] }");
    assert_eq!(format!("{user:#?}"), "User { [REDACTED] }");
    assert_eq!(User::decode(bytes.as_slice()).unwrap(), user);
}
