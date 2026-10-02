use lyra_meta::proto::pb_meta::{CatalogComponent, Component, Instance, component::Kind};
use prost::Message;

#[test]
fn initialization_flag_preserves_presence_and_wire_tag() {
    for (initialized, bytes) in [
        (None, vec![]),
        (Some(false), vec![0x08, 0x00]),
        (Some(true), vec![0x08, 0x01]),
    ] {
        let value = Instance { initialized };
        assert_eq!(value.encode_to_vec(), bytes);
        assert_eq!(Instance::decode(bytes.as_slice()).unwrap(), value);
    }
}

#[test]
fn empty_catalog_payload_still_selects_a_component_kind() {
    let payload = CatalogComponent {};
    assert!(payload.encode_to_vec().is_empty());

    let value = Component {
        kind: Some(Kind::Catalog(payload)),
    };
    // Field 2, length-delimited, with a zero-length CatalogComponent payload.
    // There is no registration ID in the enclosing message.
    let bytes = [0x12, 0x00];
    assert_eq!(value.encode_to_vec(), bytes);
    assert_eq!(Component::decode(bytes.as_slice()).unwrap(), value);
}

#[test]
fn absent_and_unknown_component_kinds_remain_unselected() {
    let empty = Component::decode([].as_slice()).unwrap();
    assert_eq!(empty.kind, None);
    assert!(empty.encode_to_vec().is_empty());

    // Unknown field 3 is valid Protobuf, but is not a supported component kind.
    // The metadata layer must reject the resulting missing discriminator.
    let unknown = Component::decode([0x1a, 0x00].as_slice()).unwrap();
    assert_eq!(unknown.kind, None);
}

#[test]
fn unknown_fields_do_not_erase_known_values() {
    // Keep forward-compatible wire decoding without treating it as validation.
    let instance = Instance::decode([0x08, 0x01, 0x10, 0x01].as_slice()).unwrap();
    assert_eq!(instance.initialized, Some(true));

    let component = Component::decode([0x12, 0x00, 0x20, 0x01].as_slice()).unwrap();
    assert!(matches!(component.kind, Some(Kind::Catalog(_))));
}

#[test]
fn malformed_initialization_records_fail_wire_decoding() {
    for bytes in [
        &[0x00][..],       // Invalid field zero.
        &[0x08][..],       // Missing bool varint.
        &[0x08, 0x80][..], // Truncated varint.
        &[0x0a, 0x00][..], // Wrong wire type for field 1.
    ] {
        assert!(Instance::decode(bytes).is_err(), "accepted {bytes:?}");
    }
}

#[test]
fn malformed_component_records_fail_wire_decoding() {
    for bytes in [
        &[0x00][..],             // Invalid field zero.
        &[0x12][..],             // Missing nested-message length.
        &[0x12, 0x01][..],       // Truncated CatalogComponent.
        &[0x12, 0x01, 0x00][..], // Malformed nested message.
        &[0x10, 0x00][..],       // Wrong wire type for field 2.
    ] {
        assert!(Component::decode(bytes).is_err(), "accepted {bytes:?}");
    }
}
