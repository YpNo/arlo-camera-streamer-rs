//! Admin-control vocabulary.
//!
//! Driving-side port types for the operational write API exposed under
//! `/admin/*`. The application layer implements
//! [`AdminControl`](crate::port::AdminControl); the HTTP layer in
//! `streamer-infra-ops` calls into it.
//!
//! `Snapshot` types are deliberately serializable so the HTTP layer can
//! return them as JSON without a translation step. They live in the
//! domain crate so we keep schema authority here.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::camera::{CameraId, StreamName};

/// Failure modes for an admin operation.
///
/// Designed for clean mapping to HTTP status codes by the inbound
/// adapter:
///
/// | Variant         | HTTP status            |
/// |-----------------|------------------------|
/// | `UnknownCamera` | `404 Not Found`        |
/// | `Unavailable`   | `503 Service Unavailable` |
/// | `Internal`      | `500 Internal Server Error` |
#[derive(Debug, Error)]
pub enum AdminError {
    /// Camera id not configured. The user typed a wrong id.
    #[error("unknown camera: {0}")]
    UnknownCamera(CameraId),

    /// Orchestrator did not respond within the control-plane timeout.
    /// Usually means the daemon is overloaded or stuck.
    #[error("admin operation timed out: {0}")]
    Unavailable(String),

    /// Catch-all for unexpected internal errors. Use sparingly.
    #[error("internal admin error: {0}")]
    Internal(String),
}

/// Snapshot of one camera's runtime state. JSON-serializable for the
/// `/admin/state` and `/admin/cameras/{id}` endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CameraSnapshot {
    /// Arlo device id.
    pub id: CameraId,
    /// Output stream name.
    pub stream_name: StreamName,
    /// Top-level state name (`"idle"`, `"activating"`, `"live"`,
    /// `"cooling"`, `"battery-protect"`, `"failed"`).
    pub state: String,
    /// Total live time observed so far today, in seconds.
    pub live_secs_today: u64,
    /// Daily live budget, in seconds. `0` when disabled.
    pub daily_budget_secs: u64,
    /// Cooldown remaining when in `Cooling`, otherwise `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub cooling_remaining: Option<Duration>,
    /// Last failure reason, if currently `Failed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub last_failure: Option<String>,
    /// Number of retries since the last successful attach.
    pub retries: u32,
}

/// System-wide snapshot returned by `/admin/state`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SystemSnapshot {
    /// Daemon version (`CARGO_PKG_VERSION`).
    pub version: String,
    /// Number of seconds since boot.
    pub uptime_secs: u64,
    /// `true` when the upstream Arlo bus is connected.
    pub arlo_connected: bool,
    /// One snapshot per configured camera.
    pub cameras: Vec<CameraSnapshot>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn camera_snapshot_round_trips_through_json() {
        let snap = CameraSnapshot {
            id: CameraId::new("CAM"),
            stream_name: StreamName::parse("front").unwrap(),
            state: "live".to_string(),
            live_secs_today: 12,
            daily_budget_secs: 600,
            cooling_remaining: Some(Duration::from_secs(15)),
            last_failure: None,
            retries: 0,
        };
        let s = serde_json::to_string(&snap).unwrap();
        let back: CameraSnapshot = serde_json::from_str(&s).unwrap();
        assert_eq!(snap, back);
    }

    #[test]
    fn camera_snapshot_omits_optional_fields_when_none() {
        let snap = CameraSnapshot {
            id: CameraId::new("CAM"),
            stream_name: StreamName::parse("front").unwrap(),
            state: "idle".to_string(),
            live_secs_today: 0,
            daily_budget_secs: 0,
            cooling_remaining: None,
            last_failure: None,
            retries: 0,
        };
        let s = serde_json::to_string(&snap).unwrap();
        assert!(!s.contains("cooling_remaining"));
        assert!(!s.contains("last_failure"));
    }

    #[test]
    fn admin_error_unknown_camera_renders_id() {
        let err = AdminError::UnknownCamera(CameraId::new("XYZ"));
        let msg = err.to_string();
        assert!(msg.contains("XYZ"));
    }

    #[test]
    fn system_snapshot_round_trips() {
        let sys = SystemSnapshot {
            version: "0.1.0".to_string(),
            uptime_secs: 3,
            arlo_connected: true,
            cameras: vec![],
        };
        let s = serde_json::to_string(&sys).unwrap();
        let back: SystemSnapshot = serde_json::from_str(&s).unwrap();
        assert_eq!(sys, back);
    }
}
