//! Generated wire types; no storage operations or metadata validation policy.

/// Lifecycle records generated from `proto/pb_meta.proto`.
///
/// Protobuf decoding checks the wire format, not metadata validity. An unset
/// initialization flag or component kind still requires rejection by the
/// metadata layer. Read the optional fields directly to distinguish absence
/// from an explicit value.
pub mod pb_meta {
    include!(concat!(env!("OUT_DIR"), "/io.lyra.meta.v1.rs"));
}
