//! Configuration types deserialized from `streamer.toml`.
//!
//! This module is **purely declarative**. Loading from disk, secret
//! resolution from env vars, and cross-field validation live in
//! `streamer-bin` and `streamer-app` respectively.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::camera::{CameraId, StreamName};
use crate::error::DomainError;
use crate::stream::{Codec, IceAddressFamily};

/// Top-level streamer configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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

/// Default for [`WebrtcConfig::live_stall_timeout_secs`]: about three
/// keyframe-request cycles of silence — a source quiet that long is
/// dead from the NVR's point of view.
pub const DEFAULT_LIVE_STALL_TIMEOUT_SECS: u64 = 10;
/// Floor applied to [`WebrtcConfig::live_stall_timeout_secs`] so a single
/// lost keyframe-request cycle (3 s) can never be mistaken for a dead
/// source.
pub const MIN_LIVE_STALL_TIMEOUT_SECS: u64 = 4;

/// WebRTC-ingestion knobs applied to every camera's `webrtcbin`.
///
/// Kept separate from [`OutputConfig`] because these settings feed the
/// **ingestion** side (offer generation, ICE gathering, source-loss
/// detection) rather than an output sink.
///
/// `#[serde(default)]` on the struct makes a `[webrtc]` table with any
/// subset of keys valid; an absent table is the same as an empty one.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebrtcConfig {
    /// Address-family policy for local ICE candidate gathering.
    /// Default is dual-stack (IPv4 + IPv6).
    pub ice_address_family: IceAddressFamily,
    /// Seconds without inbound video RTP (counted after the first
    /// packet) before the live source is declared lost and the camera
    /// returns to idle. Values below [`MIN_LIVE_STALL_TIMEOUT_SECS`]
    /// are raised to it by [`Self::live_stall_timeout`].
    pub live_stall_timeout_secs: u64,
}

impl Default for WebrtcConfig {
    fn default() -> Self {
        Self {
            ice_address_family: IceAddressFamily::default(),
            live_stall_timeout_secs: DEFAULT_LIVE_STALL_TIMEOUT_SECS,
        }
    }
}

impl WebrtcConfig {
    /// The stall timeout as a [`Duration`], never below
    /// [`MIN_LIVE_STALL_TIMEOUT_SECS`].
    #[must_use]
    pub fn live_stall_timeout(&self) -> Duration {
        Duration::from_secs(
            self.live_stall_timeout_secs
                .max(MIN_LIVE_STALL_TIMEOUT_SECS),
        )
    }
}

/// Arlo cloud authentication and session-cache configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArloConfig {
    /// Arlo cloud account email (the address used to log into the
    /// Arlo iOS / Android app).
    pub email: String,
    /// Name of the env var that holds the Arlo cloud account password.
    /// The streamer reads `std::env::var(password_env)` at boot; the
    /// password is **never** written to TOML.
    pub password_env: String,
    /// Filesystem path where the arlo-rs session token snapshot is
    /// persisted across restarts. Must be readable + writable by the
    /// streamer process.
    pub session_cache_path: PathBuf,
    /// Version of the Arlo mobile app the stream query identifies as
    /// when relaying the user's own live view (ADR 0007): Arlo hands the
    /// app identity the view's RTSPS stream, a browser gets DASH.
    #[serde(default = "default_app_version")]
    pub app_version: String,
    /// SHA-256 fingerprint (64 hex digits) of the certificate Arlo's
    /// watch-along host presents, to pin it instead of verifying its
    /// chain against the system roots (ADR 0007). Only needed when the
    /// log says `watch-along certificate chain not trusted`; that line
    /// prints the fingerprint to copy here. Unset: chain verification
    /// with the hostname check waived (the host is a raw IP).
    #[serde(default)]
    pub watch_along_cert_sha256: Option<String>,
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
    Sms(SmsMfaConfig),
    /// Push MFA. Arlo sends an approval prompt to the account's Arlo
    /// mobile app; the streamer polls until the user taps "Approve".
    /// Fully headless once the app is installed and signed in.
    Push(PushMfaConfig),
}

/// SMS MFA takes no settings. A struct rather than a unit variant so a
/// stray key (an IMAP setting left behind after switching to `sms`) is
/// refused like everywhere else instead of silently ignored.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SmsMfaConfig {}

/// Email MFA configuration. Password is **never** stored in the TOML
/// file — only the name of the env var holding it.
///
/// IMAP auto-retrieval kicks in when the mailbox is locatable (an
/// explicit `host` *or* a known `provider`) **and** `user` **and**
/// `password_env` are all set. Any other shape falls back to a stdin
/// OTP prompt.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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

/// H.264 encoder for the unified splice pipeline (ADR 0008).
///
/// The encoder runs continuously per connected camera (see the README
/// "Performance" section), so it dominates CPU. `auto` probes the host
/// at boot and takes the first working backend, hardware first; the
/// explicit names pin one and fail the boot when it does not work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum VideoEncoder {
    /// Probe at boot: NVIDIA, Intel/AMD (`va`, then `vaapi`), V4L2, x264.
    #[default]
    Auto,
    /// Software `x264enc`; works everywhere (the Raspberry Pi 5 has no
    /// H.264 hardware encoder and ends up here).
    X264,
    /// Intel/AMD GPU through the `va` plugin (`vah264enc`).
    Va,
    /// Intel/AMD GPU through the older `vaapi` plugin (`vaapih264enc`).
    Vaapi,
    /// V4L2 memory-to-memory (`v4l2h264enc`): Raspberry Pi 4 / Zero 2 / CM4.
    V4l2,
    /// NVIDIA GPU (`nvh264enc`).
    Nvenc,
}

/// All output-side configuration: stream endpoints + ops sockets.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct RtspOutput {
    /// Bind address for the embedded RTSP server.
    #[serde(default = "default_rtsp_bind")]
    pub bind: String,
}

/// HLS sink configuration. Files are written to `dir/<stream_name>/…`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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

/// Shortest HLS segment accepted; `0` would disable splitting.
pub const MIN_HLS_SEGMENT_SECS: u32 = 1;
/// Shortest HLS playlist accepted: RFC 8216 §6.3.3 wants a client to
/// find at least three segments.
pub const MIN_HLS_PLAYLIST_LENGTH: u32 = 3;
/// Longest HLS segment accepted: past a minute a viewer waits that long
/// for the first picture.
pub const MAX_HLS_SEGMENT_SECS: u32 = 60;
/// Longest HLS playlist accepted. Retention is computed from it, and a
/// value near `u32::MAX` wrapped to "keep every segment" (a full disk).
pub const MAX_HLS_PLAYLIST_LENGTH: u32 = 1000;

impl HlsOutput {
    /// Target segment duration, within
    /// [`MIN_HLS_SEGMENT_SECS`]..=[`MAX_HLS_SEGMENT_SECS`].
    #[must_use]
    pub fn effective_segment_secs(&self) -> u32 {
        self.segment_secs
            .clamp(MIN_HLS_SEGMENT_SECS, MAX_HLS_SEGMENT_SECS)
    }

    /// Playlist window, within
    /// [`MIN_HLS_PLAYLIST_LENGTH`]..=[`MAX_HLS_PLAYLIST_LENGTH`].
    #[must_use]
    pub fn effective_playlist_length(&self) -> u32 {
        self.playlist_length
            .clamp(MIN_HLS_PLAYLIST_LENGTH, MAX_HLS_PLAYLIST_LENGTH)
    }

    /// Refuse a segment duration or playlist length above its ceiling,
    /// so a typo fails the boot instead of being clamped unnoticed.
    ///
    /// # Errors
    ///
    /// [`DomainError::InvalidConfig`] naming the value and its ceiling.
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.segment_secs > MAX_HLS_SEGMENT_SECS {
            return Err(DomainError::InvalidConfig(format!(
                "output.hls.segment_secs = {} is above {MAX_HLS_SEGMENT_SECS}",
                self.segment_secs
            )));
        }
        if self.playlist_length > MAX_HLS_PLAYLIST_LENGTH {
            return Err(DomainError::InvalidConfig(format!(
                "output.hls.playlist_length = {} is above {MAX_HLS_PLAYLIST_LENGTH}",
                self.playlist_length
            )));
        }
        Ok(())
    }
}

/// DASH sink configuration. Files are written to `dir/<stream_name>/…`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DashOutput {
    /// Filesystem directory for DASH segments and manifests.
    pub dir: PathBuf,
    /// Target segment duration in seconds.
    #[serde(default = "default_segment_secs")]
    pub segment_secs: u32,
}

fn default_app_version() -> String {
    "6.46.0".to_string()
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
    /// How often, in seconds, a relay of the user's app view (ADR 0007)
    /// lets go of the stream to learn whether the app still views: our
    /// session keeps the camera streaming, so the camera can only report
    /// `idle` once we release it. No report within a few seconds means
    /// the view goes on and the relay resumes. `0` never probes and caps
    /// the relay at `max_continuous_live` instead.
    #[serde(default = "default_user_view_probe")]
    pub user_view_probe_secs: u64,
}

impl Default for CooldownConfig {
    fn default() -> Self {
        Self {
            debounce_secs: default_debounce(),
            max_continuous_live: default_max_continuous_live(),
            daily_live_budget: 0,
            budget_reset: default_budget_reset(),
            user_view_probe_secs: default_user_view_probe(),
        }
    }
}

/// Longest value accepted for any cooldown duration or quota: one day.
pub const MAX_COOLDOWN_SECS: u64 = 86_400;

impl CooldownConfig {
    /// Range-check the cooldown values. `debounce_secs` and
    /// `max_continuous_live` must be `1..=MAX_COOLDOWN_SECS` (zero would
    /// tear every session down right after attach, the churn the cap
    /// exists to prevent); `daily_live_budget` and `user_view_probe_secs`
    /// may be `0` (disabled) up to the same ceiling.
    ///
    /// # Errors
    ///
    /// [`DomainError::InvalidConfig`] naming the camera and the field.
    pub fn validate(&self, camera: &CameraId) -> Result<(), DomainError> {
        let bounded = |name: &str, value: u64, min: u64| {
            if (min..=MAX_COOLDOWN_SECS).contains(&value) {
                Ok(())
            } else {
                Err(DomainError::InvalidConfig(format!(
                    "camera {camera}: cooldown.{name} = {value} is outside {min}..={MAX_COOLDOWN_SECS} seconds"
                )))
            }
        };
        bounded("debounce_secs", self.debounce_secs, 1)?;
        bounded("max_continuous_live", self.max_continuous_live, 1)?;
        bounded("daily_live_budget", self.daily_live_budget, 0)?;
        bounded("user_view_probe_secs", self.user_view_probe_secs, 0)
    }
}

impl StreamerConfig {
    /// Cross-field checks the parser cannot express: no two cameras may
    /// share an `arlo_device_id` (the second would replace the first's
    /// routes) or a `stream_name` (the second would replace the first's
    /// RTSP mount and share its HLS directory), every cooldown and HLS
    /// value must be in range, and the three listen addresses must parse.
    /// Called by the loader so a bad file fails the boot before anything
    /// is spawned (or any Arlo login).
    ///
    /// # Errors
    ///
    /// [`DomainError::InvalidConfig`] describing the first problem found.
    pub fn validate(&self) -> Result<(), DomainError> {
        let mut ids = std::collections::HashSet::new();
        let mut names = std::collections::HashSet::new();
        for camera in &self.cameras {
            if !ids.insert(&camera.arlo_device_id) {
                return Err(DomainError::InvalidConfig(format!(
                    "duplicate [[cameras]] arlo_device_id {}",
                    camera.arlo_device_id
                )));
            }
            if !names.insert(camera.stream_name.as_str()) {
                return Err(DomainError::InvalidConfig(format!(
                    "duplicate [[cameras]] stream_name {} (camera {})",
                    camera.stream_name.as_str(),
                    camera.arlo_device_id
                )));
            }
            camera.cooldown.validate(&camera.arlo_device_id)?;
        }
        if let Some(hls) = &self.output.hls {
            hls.validate()?;
        }
        for (name, bind) in [
            ("output.rtsp.bind", &self.output.rtsp.bind),
            ("output.metrics_bind", &self.output.metrics_bind),
            ("output.admin_bind", &self.output.admin_bind),
        ] {
            bind.parse::<std::net::SocketAddr>().map_err(|_| {
                DomainError::InvalidConfig(format!(
                    "{name} = '{bind}' is not an address:port (e.g. 127.0.0.1:9090)"
                ))
            })?;
        }
        Ok(())
    }
}

const fn default_debounce() -> u64 {
    60
}
const fn default_max_continuous_live() -> u64 {
    300
}
const fn default_user_view_probe() -> u64 {
    60
}
fn default_budget_reset() -> String {
    "00:00".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shipped example must always parse: it is the first thing a new
    /// user runs, and a stale key there is a boot failure in production.
    #[test]
    fn example_config_file_parses_matching_documented_defaults() {
        let raw = include_str!("../../../config/streamer.example.toml");
        let cfg: StreamerConfig = toml::from_str(raw).expect("example config must parse");

        assert_eq!(cfg.arlo.email, "owner@example.com");
        assert_eq!(cfg.arlo.password_env, "ARLO_PASSWORD");
        match &cfg.arlo.mfa {
            MfaConfig::Email(email) => {
                assert_eq!(email.host.as_deref(), Some("imap.example.com"));
                assert_eq!(email.password_env.as_deref(), Some("ARLO_IMAP_PASSWORD"));
                assert_eq!(email.port, default_imap_port());
            }
            other => panic!("example should ship email MFA, got {other:?}"),
        }
        assert_eq!(cfg.webrtc.ice_address_family, IceAddressFamily::default());
        assert_eq!(
            cfg.webrtc.live_stall_timeout_secs,
            DEFAULT_LIVE_STALL_TIMEOUT_SECS
        );
        assert_eq!(cfg.output.video_encoder, VideoEncoder::default());
        assert_eq!(cfg.output.rtsp.bind, default_rtsp_bind());
        assert_eq!(cfg.output.metrics_bind, default_metrics_bind());
        assert_eq!(cfg.output.admin_bind, default_admin_bind());
        assert!(cfg.output.hls.is_none() && cfg.output.dash.is_none());

        assert_eq!(cfg.cameras.len(), 1);
        let cam = &cfg.cameras[0];
        assert_eq!(cam.stream_name.as_str(), "front_door");
        assert!(cam.codec_hint.is_none());
        assert_eq!(cam.cooldown.debounce_secs, default_debounce());
        assert_eq!(
            cam.cooldown.max_continuous_live,
            default_max_continuous_live()
        );
        assert_eq!(cam.cooldown.daily_live_budget, 0);
        assert_eq!(cam.cooldown.budget_reset, default_budget_reset());
    }

    #[test]
    fn video_encoder_parses_lowercase_and_defaults_to_auto() {
        #[derive(Deserialize)]
        struct W {
            #[serde(default)]
            e: VideoEncoder,
        }
        assert_eq!(VideoEncoder::default(), VideoEncoder::Auto);
        assert_eq!(toml::from_str::<W>("").unwrap().e, VideoEncoder::Auto);
        for (name, want) in [
            ("auto", VideoEncoder::Auto),
            ("x264", VideoEncoder::X264),
            ("va", VideoEncoder::Va),
            ("vaapi", VideoEncoder::Vaapi),
            ("v4l2", VideoEncoder::V4l2),
            ("nvenc", VideoEncoder::Nvenc),
        ] {
            assert_eq!(
                toml::from_str::<W>(&format!("e = \"{name}\"")).unwrap().e,
                want
            );
        }
        assert!(toml::from_str::<W>("e = \"qsv\"").is_err());
        assert!(toml::from_str::<W>("e = \"X264\"").is_err());
    }

    #[test]
    fn unknown_keys_are_rejected_in_every_table() {
        for (table, key) in [
            ("[arlo]", "emial"),
            ("[arlo.mfa]", "pasword_env"),
            ("[webrtc]", "live_stall_timeout_sec"),
            ("[output]", "video_encodr"),
            ("[output.rtsp]", "bindd"),
            ("[cameras.cooldown]", "daily_live_budgetx"),
        ] {
            let raw = include_str!("../../../config/streamer.example.toml").to_string();
            let patched = raw.replacen(table, &format!("{table}\n{key} = \"x\""), 1);
            let err = toml::from_str::<StreamerConfig>(&patched).expect_err(key);
            assert!(err.to_string().contains(key), "{key}: {err}");
        }
    }

    #[test]
    fn validate_accepts_the_example_and_refuses_duplicates_and_ranges() {
        let raw = include_str!("../../../config/streamer.example.toml").to_string();
        let cfg: StreamerConfig = toml::from_str(&raw).expect("parses");
        cfg.validate().expect("the example validates");

        let mut dup_id = cfg.clone();
        dup_id.cameras.push(cfg.cameras[0].clone());
        assert!(
            dup_id
                .validate()
                .unwrap_err()
                .to_string()
                .contains("arlo_device_id")
        );

        let mut dup_name = cfg.clone();
        let mut second = cfg.cameras[0].clone();
        second.arlo_device_id = CameraId::new("OTHER");
        dup_name.cameras.push(second);
        assert!(
            dup_name
                .validate()
                .unwrap_err()
                .to_string()
                .contains("stream_name")
        );

        let mut zero_cap = cfg.clone();
        zero_cap.cameras[0].cooldown.max_continuous_live = 0;
        assert!(
            zero_cap
                .validate()
                .unwrap_err()
                .to_string()
                .contains("max_continuous_live")
        );

        let mut huge_budget = cfg.clone();
        huge_budget.cameras[0].cooldown.daily_live_budget = MAX_COOLDOWN_SECS + 1;
        assert!(
            huge_budget
                .validate()
                .unwrap_err()
                .to_string()
                .contains("daily_live_budget")
        );

        let mut probe_off = cfg;
        probe_off.cameras[0].cooldown.user_view_probe_secs = 0;
        probe_off
            .validate()
            .expect("0 disables the probe and is allowed");
    }

    #[test]
    fn cooldown_default_matches_documented_values() {
        let c = CooldownConfig::default();
        assert_eq!(c.debounce_secs, 60);
        assert_eq!(c.max_continuous_live, 300);
        assert_eq!(c.daily_live_budget, 0);
        assert_eq!(c.budget_reset, "00:00");
        assert_eq!(c.user_view_probe_secs, 60);
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
            MfaConfig::Sms(_) | MfaConfig::Push(_) => panic!("expected email mfa"),
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
            MfaConfig::Sms(_) | MfaConfig::Push(_) => panic!("expected email mfa"),
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
            MfaConfig::Email(_) | MfaConfig::Sms(_) => panic!("expected push mfa"),
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
            MfaConfig::Email(_) | MfaConfig::Sms(_) => panic!("expected push mfa"),
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
        assert_eq!(
            cfg.webrtc.live_stall_timeout_secs,
            DEFAULT_LIVE_STALL_TIMEOUT_SECS
        );
    }

    #[test]
    fn webrtc_table_without_stall_key_uses_default_timeout() {
        // A `[webrtc]` table that predates the key (every config written
        // before ADR 0004) must keep booting with the default.
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
        assert_eq!(cfg.webrtc.live_stall_timeout(), Duration::from_secs(10));
    }

    #[test]
    fn webrtc_parses_live_stall_timeout_override() {
        #[derive(Deserialize)]
        struct W {
            webrtc: WebrtcConfig,
        }
        let w: W = toml::from_str("[webrtc]\nlive_stall_timeout_secs = 30").unwrap();
        assert_eq!(w.webrtc.live_stall_timeout(), Duration::from_secs(30));
        assert_eq!(w.webrtc.ice_address_family, IceAddressFamily::Dual);
    }

    #[test]
    fn hls_output_values_below_floor_are_raised_and_others_kept() {
        let low = HlsOutput {
            dir: PathBuf::from("/tmp/hls"),
            segment_secs: 0,
            playlist_length: 1,
        };
        assert_eq!(low.effective_segment_secs(), MIN_HLS_SEGMENT_SECS);
        assert_eq!(low.effective_playlist_length(), MIN_HLS_PLAYLIST_LENGTH);
        let ok = HlsOutput {
            dir: PathBuf::from("/tmp/hls"),
            segment_secs: 4,
            playlist_length: 6,
        };
        assert_eq!(
            (ok.effective_segment_secs(), ok.effective_playlist_length()),
            (4, 6)
        );
    }

    #[test]
    fn live_stall_timeout_below_floor_is_raised_to_floor() {
        let cfg = WebrtcConfig {
            live_stall_timeout_secs: 0,
            ..WebrtcConfig::default()
        };
        assert_eq!(
            cfg.live_stall_timeout(),
            Duration::from_secs(MIN_LIVE_STALL_TIMEOUT_SECS)
        );
        let at_floor = WebrtcConfig {
            live_stall_timeout_secs: MIN_LIVE_STALL_TIMEOUT_SECS,
            ..WebrtcConfig::default()
        };
        assert_eq!(
            at_floor.live_stall_timeout(),
            Duration::from_secs(MIN_LIVE_STALL_TIMEOUT_SECS)
        );
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

    /// `kind = "sms"` was a unit variant: serde skipped any other key, so
    /// IMAP settings left behind looked active.
    #[test]
    fn sms_mfa_refuses_stray_keys() {
        #[derive(Debug, Deserialize)]
        struct M {
            mfa: MfaConfig,
        }
        let ok: M = toml::from_str("[mfa]\nkind = \"sms\"").unwrap();
        assert!(matches!(ok.mfa, MfaConfig::Sms(_)));
        let err = toml::from_str::<M>("[mfa]\nkind = \"sms\"\nhost = \"imap.example.com\"")
            .expect_err("a stray key must be refused");
        assert!(err.to_string().contains("host"), "{err}");
    }

    /// `playlist_length + 2` wrapped near `u32::MAX` to "keep every
    /// segment".
    #[test]
    fn hls_values_above_their_ceiling_are_refused_and_clamped() {
        let hls = |segment_secs, playlist_length| HlsOutput {
            dir: PathBuf::from("/tmp/hls"),
            segment_secs,
            playlist_length,
        };
        assert!(
            hls(MAX_HLS_SEGMENT_SECS, MAX_HLS_PLAYLIST_LENGTH)
                .validate()
                .is_ok()
        );
        assert!(hls(MAX_HLS_SEGMENT_SECS + 1, 6).validate().is_err());
        let wrap = hls(2, u32::MAX - 1);
        assert!(
            wrap.validate()
                .unwrap_err()
                .to_string()
                .contains("playlist_length")
        );
        assert_eq!(wrap.effective_playlist_length(), MAX_HLS_PLAYLIST_LENGTH);
        assert_eq!(
            hls(u32::MAX, 6).effective_segment_secs(),
            MAX_HLS_SEGMENT_SECS
        );
    }

    /// The binds used to be parsed after the Arlo login, in the tasks that
    /// serve them; a typo surfaced late or not at all.
    #[test]
    fn validate_refuses_a_listen_address_that_does_not_parse() {
        let raw = include_str!("../../../config/streamer.example.toml");
        let cfg: StreamerConfig = toml::from_str(raw).expect("parses");
        for (patch, name) in [
            (|c: &mut StreamerConfig| c.output.metrics_bind = "localhost:9090".into())
                as fn(&mut StreamerConfig),
            |c: &mut StreamerConfig| c.output.admin_bind = "0.0.0.0".into(),
            |c: &mut StreamerConfig| c.output.rtsp.bind = "8554".into(),
        ]
        .into_iter()
        .zip(["metrics_bind", "admin_bind", "rtsp.bind"])
        {
            let mut bad = cfg.clone();
            patch(&mut bad);
            let err = bad.validate().expect_err(name).to_string();
            assert!(err.contains(name), "{err}");
        }
    }
}
