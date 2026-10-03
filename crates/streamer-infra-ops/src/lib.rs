//! Operational HTTP surface for the streamer daemon.
//!
//! Exposes three endpoints on a single bind address:
//!
//! | Path       | Status                                                       |
//! |------------|--------------------------------------------------------------|
//! | `/metrics` | Prometheus exposition (text/plain; version=0.0.4)            |
//! | `/healthz` | `200 OK` while the process is running (liveness)             |
//! | `/readyz`  | `200 OK` when started **and** Arlo bus is connected;         |
//! |            | `503 Service Unavailable` otherwise (readiness)              |
//!
//! ## Architecture
//!
//! - [`metrics::Metrics`] holds typed Prometheus gauges and a registry.
//!   Constructed once at boot; cloned [`std::sync::Arc`] handles are
//!   shared with parts of the system that need to update gauges
//!   (e.g., the connection-status watch task updates
//!   [`metrics::Metrics::set_arlo_connected`]).
//! - [`health::Readiness`] exposes lock-free atomic flags. The HTTP
//!   handler reads the AND of `started` + `arlo_connected`.
//! - [`server::OpsServer::serve`] binds an axum router and supports
//!   graceful shutdown via [`tokio_util::sync::CancellationToken`].
//!
//! Camera-level metrics (motion counts, state transitions, live
//! seconds) require an application-layer `MetricsRecorder` port that
//! this crate would implement; that work lands in Phase 6 along with
//! the `/admin` write endpoints.

#![forbid(unsafe_code)]

pub mod admin_server;
/// Error types for the ops module.
pub mod error;
pub mod health;
pub mod metrics;
pub mod serve;
pub mod server;

pub use admin_server::{AdminServer, AdminServerError};
pub use error::OpsError;
pub use health::Readiness;
pub use metrics::Metrics;
pub use serve::ServeLimits;
pub use server::OpsServer;
