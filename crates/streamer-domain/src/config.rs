//! Configuration types deserialized from `streamer.toml`.
//!
//! This module is **purely declarative**. Loading from disk, secret
//! resolution from env vars, and cross-field validation live in
//! `streamer-bin` and `streamer-app` respectively.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::camera::{CameraId, StreamName};
use crate::stream::Codec;

/// Top-level streamer configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StreamerConfig {
    /// Arlo cloud / session settings.
    pub arlo: ArloConfig,
    /// Output endpoints (RTSP / HLS / DASH) and ops sockets.
    pub output: OutputConfig,
    /// Cameras to expose. Empty list is a configuration error caught
    /// at boot in `streamer-bin`.
    #[serde(default)]
    pub cameras: Vec<CameraConfig>,
}

/// Arlo cloud authentication and session-cache configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ArloConfig {
    /// Arlo cloud account email (the address used to log into the
    /// Arlo iOS / Android app).
    pub email: String,
    /// Name of the env var that holds the Arlo cloud account password.
    /// The streamer reads `std::env::var(password_env)` at boot; the
    /// password is **never** written to TOML.
    pub password_env: String,
    /// Filesystem path where the rs-arlo session token snapshot is
    /// persisted across restarts. Must be readable + writable by the
    /// streamer process.
    pub session_cache_path: PathBuf,
    /// MFA strategy for cold-start (when session cache is missing or
    /// expired).
    pub mfa: MfaConfig,
}

/// MFA strategy. `imap` is the production headless choice; `stdin` is
/// for first-time setup or container debugging.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MfaConfig {
    /// Headless: poll an IMAP mailbox for the OTP message.
    Imap(ImapMfaConfig),
    /// Interactive: prompt on stdin. Not suitable for production.
    Stdin,
}

/// IMAP mailbox details for headless MFA. Password is **never** stored
/// in the TOML file — only the name of the env var holding it.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ImapMfaConfig {
    /// IMAP server host (e.g. `imap.gmail.com`).
    pub host: String,
    /// IMAP user / mailbox.
    pub user: String,
    /// Name of the env var that holds the password. The streamer reads
    /// `std::env::var(password_env)` at boot.
    pub password_env: String,
    /// IMAP port (defaults to 993 for IMAPS).
    #[serde(default = "default_imap_port")]
    pub port: u16,
}

const fn default_imap_port() -> u16 {
    993
}

/// All output-side configuration: stream endpoints + ops sockets.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OutputConfig {
    /// RTSP server (always enabled — primary output).
    pub rtsp: RtspOutput,
    /// HLS sink — optional secondary output.
    #[serde(default)]
    pub hls: Option<HlsOutput>,
    /// MPEG-DASH sink — optional secondary output.
    #[serde(default)]
    pub dash: Option<DashOutput>,
    /// Bind address for the Prometheus `/metrics` endpoint.
    #[serde(default = "default_metrics_bind")]
    pub metrics_bind: String,
    /// Bind address for the operational `/admin` HTTP endpoint
    /// (manual wake / force-idle / state snapshot).
    #[serde(default = "default_admin_bind")]
    pub admin_bind: String,
}

/// RTSP output configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RtspOutput {
    /// Bind address for the embedded RTSP server.
    #[serde(default = "default_rtsp_bind")]
    pub bind: String,
}

/// HLS sink configuration. Files are written to `dir/<stream_name>/…`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HlsOutput {
    /// Filesystem directory for HLS segments and playlists.
    pub dir: PathBuf,
    /// Target segment duration in seconds.
    #[serde(default = "default_segment_secs")]
    pub segment_secs: u32,
    /// Number of segments retained in the playlist.
    #[serde(default = "default_playlist_length")]
    pub playlist_length: u32,
}

/// DASH sink configuration. Files are written to `dir/<stream_name>/…`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DashOutput {
    /// Filesystem directory for DASH segments and manifests.
    pub dir: PathBuf,
    /// Target segment duration in seconds.
    #[serde(default = "default_segment_secs")]
    pub segment_secs: u32,
}

fn default_rtsp_bind() -> String {
    "0.0.0.0:8554".to_string()
}
fn default_metrics_bind() -> String {
    "127.0.0.1:9090".to_string()
}
fn default_admin_bind() -> String {
    "127.0.0.1:9091".to_string()
}
const fn default_segment_secs() -> u32 {
    2
}
const fn default_playlist_length() -> u32 {
    10
}

/// Per-camera mapping from an Arlo device to an output stream.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CameraConfig {
    /// Arlo cloud device id (opaque, copied from the Arlo app).
    pub arlo_device_id: CameraId,
    /// Output stream name (URL path component).
    pub stream_name: StreamName,
    /// Codec hint to skip first-stream auto-detection. The media adapter
    /// caches this on disk after the first attach.
    #[serde(default)]
    pub codec_hint: Option<Codec>,
    /// Cooldown / battery-protection knobs.
    #[serde(default)]
    pub cooldown: CooldownConfig,
}

/// Cooldown and battery-protection knobs per camera.
///
/// See architecture doc §"Refined cooldown" for the trade-off
/// rationale behind the three caps.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CooldownConfig {
    /// Hold-live debounce after the last motion event, in seconds.
    /// A new motion within this window resets the timer.
    #[serde(default = "default_debounce")]
    pub debounce_secs: u64,
    /// Hard cap on continuous live duration, in seconds. Forces a
    /// return to idle even under sustained motion (battery protection).
    #[serde(default = "default_max_continuous_live")]
    pub max_continuous_live: u64,
    /// Total live-stream seconds allowed per day. `0` disables the
    /// quota. Once exhausted the camera enters `BatteryProtect` until
    /// `budget_reset`.
    #[serde(default)]
    pub daily_live_budget: u64,
    /// Local-time clock at which `daily_live_budget` resets. Format is
    /// `HH:MM`, parsed and validated in `streamer-app`.
    #[serde(default = "default_budget_reset")]
    pub budget_reset: String,
}

impl Default for CooldownConfig {
    fn default() -> Self {
        Self {
            debounce_secs: default_debounce(),
            max_continuous_live: default_max_continuous_live(),
            daily_live_budget: 0,
            budget_reset: default_budget_reset(),
        }
    }
}

const fn default_debounce() -> u64 {
    60
}
const fn default_max_continuous_live() -> u64 {
    300
}
fn default_budget_reset() -> String {
    "00:00".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cooldown_default_matches_documented_values() {
        let c = CooldownConfig::default();
        assert_eq!(c.debounce_secs, 60);
        assert_eq!(c.max_continuous_live, 300);
        assert_eq!(c.daily_live_budget, 0);
        assert_eq!(c.budget_reset, "00:00");
    }

    #[test]
    fn parses_minimal_streamer_config() {
        // Only required fields; everything else uses defaults.
        let toml_input = r#"
            [arlo]
            email              = "owner@example.com"
            password_env       = "ARLO_PASSWORD"
            session_cache_path = "/tmp/session.json"

            [arlo.mfa]
            kind         = "imap"
            host         = "imap.example.com"
            user         = "u@example.com"
            password_env = "ARLO_IMAP_PW"

            [output.rtsp]

            [[cameras]]
            arlo_device_id = "ABCD"
            stream_name    = "front_door"
        "#;
        let cfg: StreamerConfig = toml::from_str(toml_input).expect("valid minimal config");
        assert_eq!(cfg.cameras.len(), 1);
        assert_eq!(cfg.cameras[0].stream_name.as_str(), "front_door");
        assert_eq!(cfg.output.rtsp.bind, "0.0.0.0:8554");
        assert!(cfg.output.hls.is_none());
        assert!(cfg.output.dash.is_none());
        match &cfg.arlo.mfa {
            MfaConfig::Imap(imap) => {
                assert_eq!(imap.host, "imap.example.com");
                assert_eq!(imap.port, 993);
            }
            MfaConfig::Stdin => panic!("expected imap mfa"),
        }
    }

    #[test]
    fn rejects_invalid_stream_name_in_config() {
        let toml_input = r#"
            [arlo]
            email              = "u@example.com"
            password_env       = "ARLO_PW"
            session_cache_path = "/tmp/session.json"

            [arlo.mfa]
            kind = "stdin"

            [output.rtsp]

            [[cameras]]
            arlo_device_id = "X"
            stream_name    = "has spaces"
        "#;
        let err = toml::from_str::<StreamerConfig>(toml_input).unwrap_err();
        assert!(err.to_string().contains("invalid stream name"));
    }
}
