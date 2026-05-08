//! Pure builders for `gst-launch`-style pipeline description strings.
//!
//! These are realized at runtime via `gstreamer::parse::launch_full`
//! (see `gst_pipeline.rs`). Keeping them as pure string functions makes
//! them trivially unit-testable and lets ops grep the launch syntax
//! without instantiating a pipeline.
//!
//! ## Splicing strategy (Phase 4)
//!
//! Phase 4 ships a **factory-restart on transition** strategy: each
//! camera's `RTSPMediaFactory` is bound to either an idle launch
//! string or a live launch string, and `attach_live` / `detach_live`
//! swap the binding. Existing RTSP clients reconnect within ~1 s.
//!
//! Frigate handles the reconnect transparently because its rtsp client
//! retries on EOS. Trade-off: a brief gap of black frames at the
//! transition; vastly simpler than a seamless `input-selector` splice.
//! The seamless variant remains a Phase 6 polish target — the
//! [`crate::splice::KeyframeWatcher`] scaffolding is in place for it.
//!
//! ## Audio
//!
//! Frigate always wants a complete audio track (silence or live):
//!
//! - **Idle**: `audiotestsrc wave=silence` → AAC encode.
//! - **Live**: pass-through the camera's AAC payload via
//!   `rtpmp4adepay` → `aacparse`.
//!
//! ## Output
//!
//! All branches end in a `tee` named `out_t` so the same encoded
//! stream feeds RTSP (mandatory), HLS (optional), and DASH (optional).
//! The RTSP server attaches via the GStreamer-RTSP-server's
//! `pay0`/`pay1` payload-type contract.

use std::path::Path;

use streamer_domain::camera::StreamName;
use streamer_domain::config::{DashOutput, HlsOutput, OutputConfig};
use streamer_domain::stream::Codec;

use crate::idle_source::{IDLE_FPS, IdleKind, SYNTHETIC_HEIGHT, SYNTHETIC_WIDTH};

/// Effective sink configuration, materialized per-camera.
///
/// File paths embed the [`StreamName`] so multiple cameras can share
/// the same root `dir` without colliding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputBranches {
    /// Output stream name (used as RTSP path component and HLS/DASH subdir).
    pub stream_name: StreamName,
    /// `/<stream_name>` — RTSP mount path under the embedded server.
    pub rtsp_mount_path: String,
    /// HLS branch config, present when `[output.hls]` is set.
    pub hls: Option<HlsBranchConfig>,
    /// DASH branch config, present when `[output.dash]` is set.
    pub dash: Option<DashBranchConfig>,
}

/// Resolved HLS sink parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HlsBranchConfig {
    /// Path to `index.m3u8`.
    pub playlist_location: String,
    /// `printf`-style segment filename pattern.
    pub segment_location: String,
    /// Target segment duration in seconds.
    pub target_duration: u32,
    /// Number of segments in the playlist window.
    pub playlist_length: u32,
}

/// Resolved DASH sink parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DashBranchConfig {
    /// Path to `manifest.mpd`.
    pub manifest_location: String,
    /// Target segment duration in seconds.
    pub target_duration: u32,
}

/// Build the RTSP mount path for a stream.
#[must_use]
pub fn rtsp_mount_path(name: &StreamName) -> String {
    format!("/{name}")
}

/// Materialize per-camera sink config from the global [`OutputConfig`].
#[must_use]
pub fn build_output_branches(name: &StreamName, output: &OutputConfig) -> OutputBranches {
    OutputBranches {
        stream_name: name.clone(),
        rtsp_mount_path: rtsp_mount_path(name),
        hls: output.hls.as_ref().map(|h| build_hls_branch(name, h)),
        dash: output.dash.as_ref().map(|d| build_dash_branch(name, d)),
    }
}

fn build_hls_branch(name: &StreamName, hls: &HlsOutput) -> HlsBranchConfig {
    let dir = format_dir(&hls.dir, name);
    HlsBranchConfig {
        playlist_location: format!("{dir}/index.m3u8"),
        segment_location: format!("{dir}/segment-%05d.ts"),
        target_duration: hls.segment_secs,
        playlist_length: hls.playlist_length,
    }
}

fn build_dash_branch(name: &StreamName, dash: &DashOutput) -> DashBranchConfig {
    let dir = format_dir(&dash.dir, name);
    DashBranchConfig {
        manifest_location: format!("{dir}/manifest.mpd"),
        target_duration: dash.segment_secs,
    }
}

fn format_dir(root: &Path, name: &StreamName) -> String {
    // `Path::display` is lossy on non-UTF-8, but every Linux deployment
    // we target is UTF-8 and the config schema is `PathBuf` not `OsStr`.
    format!("{}/{}", root.display(), name)
}

/// Build the idle-mode video chain, terminating in a parsed encoded
/// stream. The result is **not** payloaded — the caller appends
/// `! rtph26{4,5}pay name=pay0 pt=96` for RTSP, or hooks into a tee.
#[must_use]
pub fn idle_video_desc(idle: &IdleKind) -> String {
    match idle {
        IdleKind::JpegStill { .. } => idle_video_jpeg_desc(),
        IdleKind::Synthetic { overlay, .. } => idle_video_synthetic_desc(overlay),
    }
}

/// Idle video — JPEG still loop. The actual JPEG bytes are pushed via
/// `appsrc` at runtime; the description here covers the post-`appsrc`
/// chain.
#[must_use]
pub fn idle_video_jpeg_desc() -> String {
    format!(
        "appsrc name=idle_jpeg_src is-live=true format=time \
         caps=image/jpeg,framerate={IDLE_FPS}/1 \
         ! jpegdec ! videoconvert ! videorate \
         ! video/x-raw,framerate={IDLE_FPS}/1 \
         ! x264enc tune=zerolatency bitrate=512 key-int-max=1 \
         ! h264parse config-interval=1"
    )
}

/// Idle video — synthetic black frame with a `STANDBY · …` overlay.
#[must_use]
pub fn idle_video_synthetic_desc(overlay: &str) -> String {
    let escaped = escape_gst_text(overlay);
    format!(
        "videotestsrc pattern=black is-live=true \
         ! video/x-raw,width={SYNTHETIC_WIDTH},height={SYNTHETIC_HEIGHT},framerate={IDLE_FPS}/1 \
         ! textoverlay text=\"{escaped}\" valignment=bottom halignment=center font-desc=\"Sans 24\" \
         ! videoconvert \
         ! x264enc tune=zerolatency bitrate=512 key-int-max=1 \
         ! h264parse config-interval=1"
    )
}

/// Idle audio — silent AAC at 48 kHz / stereo (matches Arlo's output).
#[must_use]
pub fn idle_audio_desc() -> &'static str {
    "audiotestsrc wave=silence is-live=true \
     ! audio/x-raw,rate=48000,channels=2 \
     ! audioconvert ! audioresample \
     ! avenc_aac bitrate=64000 \
     ! aacparse"
}

/// Live video — the rtspsrc → depay → parse chain. Codec hint, when
/// supplied, picks the depay element directly; without it we pipe into
/// `parsebin` for runtime detection.
#[must_use]
pub fn live_video_desc(url: &str, codec_hint: Option<Codec>) -> String {
    let escaped_url = escape_gst_property(url);
    let depay_chain = match codec_hint {
        Some(Codec::H264) => "rtph264depay ! h264parse config-interval=1",
        Some(Codec::H265) => "rtph265depay ! h265parse config-interval=1",
        None => "parsebin",
    };
    format!(
        "rtspsrc location=\"{escaped_url}\" latency=200 protocols=tcp+udp \
         do-retransmission=true name=live_src \
         ! {depay_chain}"
    )
}

/// Live audio — pass-through Arlo's AAC.
#[must_use]
pub fn live_audio_desc() -> &'static str {
    "rtpmp4adepay ! aacparse"
}

/// Build the complete idle-mode launch string for a single camera —
/// suitable for `RTSPMediaFactory::set_launch`. Bundles video + audio
/// into the dual-payload (`pay0` = video, `pay1` = audio) layout that
/// `gst-rtsp-server` expects.
#[must_use]
pub fn idle_launch_string(idle: &IdleKind) -> String {
    format!(
        "( {video} ! rtph264pay name=pay0 pt=96 \
            {audio} ! rtpmp4apay name=pay1 pt=97 )",
        video = idle_video_desc(idle),
        audio = idle_audio_desc(),
    )
}

/// Build the live-mode launch string for a camera. The `rtspsrc` must
/// be reachable; if it isn't, the factory will surface an error on the
/// next client connect attempt.
#[must_use]
pub fn live_launch_string(url: &str, codec_hint: Option<Codec>) -> String {
    let pay = match codec_hint {
        Some(Codec::H265) => "rtph265pay name=pay0 pt=96",
        // Default to H.264 payloader for None — parsebin will produce
        // h264-parsed buffers when the actual codec is H.264.
        Some(Codec::H264) | None => "rtph264pay name=pay0 pt=96",
    };
    format!(
        "( {video} ! {pay} \
            rtspsrc location=\"{url_audio}\" latency=200 name=live_audio \
            ! {audio} ! rtpmp4apay name=pay1 pt=97 )",
        video = live_video_desc(url, codec_hint),
        url_audio = escape_gst_property(url),
        audio = live_audio_desc(),
    )
}

/// Escape a string for inclusion in a `gst-launch` text= property.
/// Backslashes and double quotes get escaped.
fn escape_gst_text(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Escape a value embedded in a `key="value"` property.
fn escape_gst_property(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use streamer_domain::config::{DashOutput, HlsOutput, OutputConfig, RtspOutput};

    fn name() -> StreamName {
        StreamName::parse("front_door").unwrap()
    }

    fn output(hls: bool, dash: bool) -> OutputConfig {
        OutputConfig {
            rtsp: RtspOutput {
                bind: "0.0.0.0:8554".to_string(),
            },
            hls: hls.then_some(HlsOutput {
                dir: PathBuf::from("/var/hls"),
                segment_secs: 4,
                playlist_length: 6,
            }),
            dash: dash.then_some(DashOutput {
                dir: PathBuf::from("/var/dash"),
                segment_secs: 2,
            }),
            metrics_bind: "127.0.0.1:9090".to_string(),
            admin_bind: "127.0.0.1:9091".to_string(),
        }
    }

    #[test]
    fn rtsp_mount_path_prefixes_with_slash() {
        assert_eq!(rtsp_mount_path(&name()), "/front_door");
    }

    #[test]
    fn build_output_branches_with_no_optional_sinks() {
        let b = build_output_branches(&name(), &output(false, false));
        assert_eq!(b.rtsp_mount_path, "/front_door");
        assert!(b.hls.is_none());
        assert!(b.dash.is_none());
    }

    #[test]
    fn build_output_branches_with_hls_uses_per_stream_dir() {
        let b = build_output_branches(&name(), &output(true, false));
        let hls = b.hls.expect("hls present");
        assert_eq!(hls.playlist_location, "/var/hls/front_door/index.m3u8");
        assert_eq!(hls.segment_location, "/var/hls/front_door/segment-%05d.ts");
        assert_eq!(hls.target_duration, 4);
        assert_eq!(hls.playlist_length, 6);
    }

    #[test]
    fn build_output_branches_with_dash_uses_per_stream_dir() {
        let b = build_output_branches(&name(), &output(false, true));
        let dash = b.dash.expect("dash present");
        assert_eq!(dash.manifest_location, "/var/dash/front_door/manifest.mpd");
        assert_eq!(dash.target_duration, 2);
    }

    #[test]
    fn idle_video_desc_jpeg_uses_appsrc_and_jpegdec() {
        let desc = idle_video_desc(&IdleKind::JpegStill {
            jpeg: bytes::Bytes::from_static(&[0]),
        });
        assert!(desc.contains("appsrc"));
        assert!(desc.contains("jpegdec"));
        assert!(desc.contains("x264enc"));
    }

    #[test]
    fn idle_video_desc_synthetic_includes_overlay_text() {
        let desc = idle_video_desc(&IdleKind::Synthetic {
            stream_name: name(),
            overlay: "STANDBY · front_door · 2026-05-08T10:00:00".to_string(),
        });
        assert!(desc.contains("videotestsrc"));
        assert!(desc.contains("textoverlay"));
        assert!(desc.contains("STANDBY"));
        assert!(desc.contains("front_door"));
    }

    #[test]
    fn idle_video_synthetic_desc_escapes_quotes_in_overlay() {
        let desc = idle_video_synthetic_desc("has \"quotes\"");
        // Backslash-escape the embedded double quotes.
        assert!(desc.contains("\\\"quotes\\\""));
    }

    #[test]
    fn idle_video_synthetic_desc_escapes_backslashes_in_overlay() {
        let desc = idle_video_synthetic_desc("path\\sub");
        assert!(desc.contains("path\\\\sub"));
    }

    #[test]
    fn idle_audio_desc_emits_silent_aac() {
        let desc = idle_audio_desc();
        assert!(desc.contains("audiotestsrc"));
        assert!(desc.contains("wave=silence"));
        assert!(desc.contains("avenc_aac"));
    }

    #[test]
    fn live_video_desc_with_h264_hint_uses_h264_depay() {
        let desc = live_video_desc("rtsps://camera.local/path", Some(Codec::H264));
        assert!(desc.contains("rtspsrc"));
        assert!(desc.contains("rtph264depay"));
        assert!(desc.contains("h264parse"));
        assert!(!desc.contains("parsebin"));
    }

    #[test]
    fn live_video_desc_with_h265_hint_uses_h265_depay() {
        let desc = live_video_desc("rtsps://camera.local/path", Some(Codec::H265));
        assert!(desc.contains("rtph265depay"));
        assert!(desc.contains("h265parse"));
    }

    #[test]
    fn live_video_desc_without_hint_uses_parsebin() {
        let desc = live_video_desc("rtsps://camera.local/path", None);
        assert!(desc.contains("parsebin"));
        assert!(!desc.contains("rtph264depay"));
    }

    #[test]
    fn live_video_desc_escapes_url_quotes() {
        let desc = live_video_desc("rtsps://host/path?\"q\"=1", Some(Codec::H264));
        assert!(desc.contains("\\\"q\\\""));
    }

    #[test]
    fn idle_launch_string_includes_pay0_and_pay1() {
        let s = idle_launch_string(&IdleKind::Synthetic {
            stream_name: name(),
            overlay: "X".to_string(),
        });
        assert!(s.contains("pay0"));
        assert!(s.contains("pay1"));
    }

    #[test]
    fn live_launch_string_h264_hint_uses_h264_payloader() {
        let s = live_launch_string("rtsps://camera/path", Some(Codec::H264));
        assert!(s.contains("rtph264pay"));
        assert!(!s.contains("rtph265pay"));
    }

    #[test]
    fn live_launch_string_h265_hint_uses_h265_payloader() {
        let s = live_launch_string("rtsps://camera/path", Some(Codec::H265));
        assert!(s.contains("rtph265pay"));
    }

    #[test]
    fn live_launch_string_without_hint_defaults_to_h264_payloader() {
        let s = live_launch_string("rtsps://camera/path", None);
        // No hint → safe default to H.264 payloader.
        assert!(s.contains("rtph264pay"));
    }

    #[test]
    fn escape_gst_text_escapes_quotes_and_backslashes() {
        assert_eq!(escape_gst_text("a\"b\\c"), "a\\\"b\\\\c");
    }

    #[test]
    fn escape_gst_property_escapes_quotes_and_backslashes() {
        assert_eq!(escape_gst_property("a\"b\\c"), "a\\\"b\\\\c");
    }

    #[test]
    fn escape_gst_text_passes_through_safe_input() {
        assert_eq!(escape_gst_text("plain text"), "plain text");
    }
}
