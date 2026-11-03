//! Shared request/response types for the KV FastData server.
//!
//! The server binary (`main.rs`) owns the HTTP handlers and routing. This library
//! exists so the `generate-openapi` binary can derive the published OpenAPI schema
//! from the exact same request/response types the server uses on the wire.

pub mod types;

#[cfg(feature = "openapi")]
pub mod openapi;
