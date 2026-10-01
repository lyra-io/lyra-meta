//! Shared Lyra metadata and protocol contracts.
//!
//! This crate provides authentication, protobuf types shared by Cata, Func, and
//! Stream, together with the metadata interface and its Oxia-backed implementation.

pub mod auth;
pub mod config;
pub mod metadata;
#[cfg(feature = "observability")]
pub mod observability;
pub mod proto;
pub mod toolkit;
pub mod utils;
