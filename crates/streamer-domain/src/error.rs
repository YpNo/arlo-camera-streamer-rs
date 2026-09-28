//! Domain-layer error hierarchy.
//!
//! Per project guidelines:
//! - Libraries (this crate) define typed errors with `thiserror`.
//! - Applications (`streamer-bin`) bubble these up via `anyhow::Result`
//!   with `with_context` annotations.

use thiserror::Error;

/// Errors raised by the domain layer or surfaced by infrastructure
/// adapters when fulfilling a port contract.
#[derive(Debug, Error)]
pub enum DomainError {
    /// A stream name failed validation (empty or contains non URL-safe chars).
    #[error("invalid stream name: '{0}'")]
    InvalidStreamName(String),

    /// Configuration could not be parsed or violates an invariant.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Operation referenced a camera id not declared in configuration.
    #[error("unknown camera id: {0}")]
    UnknownCamera(String),

    /// Adapter-level transport failure (connection lost, timeout, etc.).
    /// Adapters wrap their own internal errors into this variant via the
    /// `String` payload to keep the domain decoupled from
    /// infrastructure-specific error types.
    #[error("adapter transport failure: {0}")]
    AdapterTransport(String),

    /// The camera is streaming to another client over a transport that
    /// excludes ours: Arlo refuses our WebRTC session (error 14001) while
    /// the user watches the camera in the mobile app. Not a fault — the
    /// application waits for the view to end instead of backing off.
    #[error("camera busy: {0}")]
    CameraBusy(String),
}
