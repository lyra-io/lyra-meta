//! Shared metadata foundation for Lyra.
//!
//! Bootstrap and component registration Protobuf contracts are available in
//! [`proto::pb_meta`], with typed initialization reads in [`metadata`]. Bootstrap
//! writes, durable storage, and registration operations are introduced separately.
//! Bootstrap credential generation and validation live in [`credentials`]; this
//! does not implement authentication or password-file loading.

pub mod credentials;
pub mod metadata;
pub mod proto;
