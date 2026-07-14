//! Configuration types deserialized from `streamer.toml`.
//!
//! This module is **purely declarative**. Loading from disk, secret
//! resolution from env vars, and cross-field validation live in
//! `streamer-bin` and `streamer-app` respectively.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::camera::{CameraId, StreamName};
use crate::stream::{Codec, IceAddressFamily};

/// Top-level streamer configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StreamerConfig {
    /// Arlo cloud / session settings.
    pub arlo: ArloConfig,
    /// Output endpoints (RTSP / HLS / DASH) and ops sockets.
    pub output: OutputConfig,
    /// WebRTC ingestion knobs (ICE address family, …). Absent in the
    /// TOML means `WebrtcConfig::default()` — dual-stack ICE.
    #[serde(default)]
    pub webrtc: WebrtcConfig,
    /// Cameras to expose. Empty list is a configuration error caught
    /// at boot in `streamer-bin`.
    #[serde(default)]
    pub cameras: Vec<CameraConfig>,
}

/// WebRTC-ingestion knobs applied to every camera's `webrtcbin`.
///
/// Kept separate from [`OutputConfig`] because these settings feed the
/// **ingestion** side (offer generation, ICE gathering) rather than an
/// output sink.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct WebrtcConfig {
    /// Address-family policy for local ICE candidate gathering.
    /// Default is dual-stack (IPv4 + IPv6).
    #[serde(default)]
    pub ice_address_family: IceAddressFamily,
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

/// MFA strategy. `email` is the preferred headless choice; `push`
/// approves on the Arlo mobile app (also headless); `sms` requires an
/// interactive stdin prompt.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MfaConfig {
    /// Email MFA. Uses IMAP polling when a mailbox is locatable
    /// (`host` or `provider`) and credentials are set; otherwise
    /// prompts for the OTP on stdin.
    Email(EmailMfaConfig),
    /// SMS MFA. Prompts via stdin. Not suitable for headless environments.
    Sms,
    /// Push MFA. Arlo sends an approval prompt to the account's Arlo
    /// mobile app; the streamer polls until the user taps "Approve".
    /// Fully headless once the app is installed and signed in.
    Push(PushMfaConfig),
}

/// Email MFA configuration. Password is **never** stored in the TOML
/// file — only the name of the env var holding it.
///
/// IMAP auto-retrieval kicks in when the mailbox is locatable (an
/// explicit `host` *or* a known `provider`) **and** `user` **and**
/// `password_env` are all set. Any other shape falls back to a stdin
/// OTP prompt.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EmailMfaConfig {
    /// Explicit IMAP server host (e.g. `imap.gmail.com`). Optional when
    /// `provider` is set; an explicit host always wins over `provider`.
    pub host: Option<String>,
    /// Well-known IMAP provider shortcut — one of `gmail`, `outlook`,
    /// `hotmail`, `yahoo`. Expands to that provider's IMAP host when
    /// `host` is omitted. Optional.
    pub provider: Option<String>,
    /// IMAP user / mailbox. Optional.
    pub user: Option<String>,
    /// Name of the env var that holds the password. Optional.
    pub password_env: Option<String>,
    /// IMAP port (defaults to 993 for IMAPS).
    #[serde(default = "default_imap_port")]
    pub port: u16,
}

const fn default_imap_port() -> u16 {
    993
}

/// Push MFA timing. Both fields are optional in TOML and fall back to
/// the documented defaults — `[arlo.mfa] kind = "push"` alone is valid.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PushMfaConfig {
    /// Seconds between `finishAuth` polls while waiting for the user to
    /// approve the prompt in the Arlo app.
    #[serde(default = "default_push_poll_interval_secs")]
    pub poll_interval_secs: u64,
    /// Hard ceiling, in seconds, on the whole push-approval wait before
    /// boot fails. Long enough for the user to reach their phone.
    #[serde(default = "default_push_timeout_secs")]
    pub timeout_secs: u64,
}

impl Default for PushMfaConfig {
    fn default() -> Self {
        Self {
            poll_interval_secs: default_push_poll_interval_secs(),
            timeout_secs: default_push_timeout_secs(),
        }
    }
}

const fn default_push_poll_interval_secs() -> u64 {
    3
}
const fn default_push_timeout_secs() -> u64 {
    120
}

/// H.264 encoder for the unified splice pipeline.
///
/// The encoder runs continuously per connected camera (see the README
/// "Performance" section), so it dominates CPU. `x264` is portable
/// software encoding; `vaapi` offloads to an Intel/AMD GPU
/// (QuickSync/VAAPI), cutting the per-camera cost to near-zero — but it
/// requires the `gstreamer1.0-vaapi` plugin and a working `/dev/dri`
/// render node on the host.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum VideoEncoder {
    /// Software `x264enc` (default; works everywhere).
    #[default]
    X264,
    /// Hardware `vaapih264enc` (Intel/AMD GPU via VAAPI).
    Vaapi,
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
    /// H.264 encoder for the live/idle pipeline (software or GPU).
    #[serde(default)]
    pub video_encoder: VideoEncoder,
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
    fn video_encoder_parses_lowercase_and_defaults_to_x264() {
        #[derive(Deserialize)]
        struct W {
            #[serde(default)]
            e: VideoEncoder,
        }
        assert_eq!(VideoEncoder::default(), VideoEncoder::X264);
        assert_eq!(toml::from_str::<W>("").unwrap().e, VideoEncoder::X264);
        assert_eq!(
            toml::from_str::<W>("e = \"x264\"").unwrap().e,
            VideoEncoder::X264
        );
        assert_eq!(
            toml::from_str::<W>("e = \"vaapi\"").unwrap().e,
            VideoEncoder::Vaapi
        );
        assert!(toml::from_str::<W>("e = \"nvenc\"").is_err());
    }

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
            kind         = "email"
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
            MfaConfig::Email(email_cfg) => {
                assert_eq!(email_cfg.host.as_deref(), Some("imap.example.com"));
                assert_eq!(email_cfg.port, 993);
            }
            MfaConfig::Sms | MfaConfig::Push(_) => panic!("expected email mfa"),
        }
    }

    #[test]
    fn parses_email_mfa_with_provider_shortcut() {
        let toml_input = r#"
            [arlo]
            email              = "owner@example.com"
            password_env       = "ARLO_PASSWORD"
            session_cache_path = "/tmp/session.json"

            [arlo.mfa]
            kind         = "email"
            provider     = "gmail"
            user         = "u@example.com"
            password_env = "ARLO_IMAP_PW"

            [output.rtsp]
        "#;
        let cfg: StreamerConfig = toml::from_str(toml_input).expect("valid provider config");
        match &cfg.arlo.mfa {
            MfaConfig::Email(email_cfg) => {
                assert_eq!(email_cfg.provider.as_deref(), Some("gmail"));
                assert_eq!(email_cfg.host, None);
                assert_eq!(email_cfg.user.as_deref(), Some("u@example.com"));
                assert_eq!(email_cfg.port, 993);
            }
            MfaConfig::Sms | MfaConfig::Push(_) => panic!("expected email mfa"),
        }
    }

    #[test]
    fn parses_push_mfa_with_default_timing() {
        let toml_input = r#"
            [arlo]
            email              = "owner@example.com"
            password_env       = "ARLO_PASSWORD"
            session_cache_path = "/tmp/session.json"

            [arlo.mfa]
            kind = "push"

            [output.rtsp]
        "#;
        let cfg: StreamerConfig = toml::from_str(toml_input).expect("valid push config");
        match &cfg.arlo.mfa {
            MfaConfig::Push(push) => {
                assert_eq!(push.poll_interval_secs, 3);
                assert_eq!(push.timeout_secs, 120);
            }
            MfaConfig::Email(_) | MfaConfig::Sms => panic!("expected push mfa"),
        }
    }

    #[test]
    fn parses_push_mfa_with_explicit_timing() {
        let toml_input = r#"
            [arlo]
            email              = "owner@example.com"
            password_env       = "ARLO_PASSWORD"
            session_cache_path = "/tmp/session.json"

            [arlo.mfa]
            kind              = "push"
            poll_interval_secs = 5
            timeout_secs       = 90

            [output.rtsp]
        "#;
        let cfg: StreamerConfig = toml::from_str(toml_input).expect("valid push config");
        match &cfg.arlo.mfa {
            MfaConfig::Push(push) => {
                assert_eq!(push.poll_interval_secs, 5);
                assert_eq!(push.timeout_secs, 90);
            }
            MfaConfig::Email(_) | MfaConfig::Sms => panic!("expected push mfa"),
        }
    }

    #[test]
    fn webrtc_defaults_to_dual_stack_when_absent() {
        // Config with no [webrtc] section falls back to the default
        // (dual-stack ICE) — this is the migration path for existing
        // deployments.
        let toml_input = r#"
            [arlo]
            email              = "u@example.com"
            password_env       = "ARLO_PW"
            session_cache_path = "/tmp/session.json"

            [arlo.mfa]
            kind = "sms"

            [output.rtsp]

            [[cameras]]
            arlo_device_id = "X"
            stream_name    = "front_door"
        "#;
        let cfg: StreamerConfig = toml::from_str(toml_input).expect("valid config");
        assert_eq!(cfg.webrtc.ice_address_family, IceAddressFamily::Dual);
    }

    #[test]
    fn parses_ipv4_only_webrtc_section() {
        let toml_input = r#"
            [arlo]
            email              = "u@example.com"
            password_env       = "ARLO_PW"
            session_cache_path = "/tmp/session.json"

            [arlo.mfa]
            kind = "sms"

            [output.rtsp]

            [webrtc]
            ice_address_family = "ipv4"

            [[cameras]]
            arlo_device_id = "X"
            stream_name    = "front_door"
        "#;
        let cfg: StreamerConfig = toml::from_str(toml_input).expect("valid config");
        assert_eq!(cfg.webrtc.ice_address_family, IceAddressFamily::Ipv4);
    }

    #[test]
    fn rejects_unknown_ice_address_family_value() {
        let toml_input = r#"
            [arlo]
            email              = "u@example.com"
            password_env       = "ARLO_PW"
            session_cache_path = "/tmp/session.json"

            [arlo.mfa]
            kind = "sms"

            [output.rtsp]

            [webrtc]
            ice_address_family = "ipv6"

            [[cameras]]
            arlo_device_id = "X"
            stream_name    = "front_door"
        "#;
        let err = toml::from_str::<StreamerConfig>(toml_input).unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("ipv6"),
            "expected the unknown variant to surface in the error, got: {err}"
        );
    }

    #[test]
    fn rejects_invalid_stream_name_in_config() {
        let toml_input = r#"
            [arlo]
            email              = "u@example.com"
            password_env       = "ARLO_PW"
            session_cache_path = "/tmp/session.json"

            [arlo.mfa]
            kind = "sms"

            [output.rtsp]

            [[cameras]]
            arlo_device_id = "X"
            stream_name    = "has spaces"
        "#;
        let err = toml::from_str::<StreamerConfig>(toml_input).unwrap_err();
        assert!(err.to_string().contains("invalid stream name"));
    }
}
