//! Generated wire types; no storage operations or metadata validation policy.
//! Initialization record validation lives in [`crate::metadata`].

mod redacted;

/// Lifecycle and bootstrap records generated from `proto/pb_meta.proto`.
///
/// Protobuf decoding checks the wire format, not metadata validity. An unset
/// initialization flag or component kind still requires rejection by the
/// metadata layer. Read the optional fields directly to distinguish absence
/// from an explicit value.
///
/// User/database IDs, names, ownership, state, and verifier validity are not
/// enforced by decoding. Inspect raw enum values with `DatabaseState::try_from`
/// rather than accepting the generated getter's fallback for unknown values.
/// User and verifier debug output is redacted, but their encoded bytes and
/// directly accessed fields still contain credential material.
pub mod pb_meta {
    include!(concat!(env!("OUT_DIR"), "/io.lyra.meta.v1.rs"));
}
