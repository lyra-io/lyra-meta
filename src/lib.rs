//! Shared metadata foundation for Lyra.
//!
//! Lifecycle Protobuf contracts are available in [`proto::pb_meta`], with typed
//! initialization reads in [`metadata`]. Bootstrap writes, durable storage, and
//! component registration are introduced separately.

pub mod metadata;
pub mod proto;
