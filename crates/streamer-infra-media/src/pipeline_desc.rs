//! Pure builders for `gst-launch`-style pipeline description strings.
//!
//! These are realized at runtime via `gstreamer::parse::launch_full`
//! (see `gst_pipeline.rs`). Keeping them as pure string functions makes
//! them trivially unit-testable and lets ops grep the launch syntax
//! without instantiating a pipeline.
//!
//! ## Splicing strategy (ADR 0003)
//!
//! [`combined_launch_string`] is the one persistent launch per camera:
//! idle and decoded live video both reach an `input-selector` as raw
//! I420 at identical caps, and a single encoder downstream gives
//! clients one continuous H.264 stream with stable SPS/PPS. The Phase-4
//! builders that re-bound the factory between idle and live (clients
//! had to reconnect) were removed on 2026-09-28.
//!
//! ## Audio
//!
//! Frigate always wants a complete audio track (silence or live):
//!
//! - **Idle**: `audiotestsrc wave=silence`, always on.
//! - **Live**: the camera's Opus RTP, `rtpopusdepay ! opusdec`, mixed
//!   onto the silent bed by an `audiomixer`; one `avenc_aac` after it.
//!
//! ## Output
//!
//! The RTSP server attaches through gst-rtsp-server's `pay0` (video) /
//! `pay1` (audio) payloader contract.

use std::path::Path;

use streamer_domain::camera::StreamName;
use streamer_domain::config::{DashOutput, HlsOutput, OutputConfig, VideoEncoder};

use crate::idle_source::{IDLE_FPS, IdleKind, SYNTHETIC_HEIGHT, SYNTHETIC_WIDTH};

/// H.264 RTP payload type of the live video: pinned in our WebRTC offer
/// to Arlo (whose gateway answers 103) and on the live video appsrc's
/// caps. One constant, so the two cannot drift.
pub(crate) const LIVE_RTP_H264_PT: i32 = 103;

/// Framerate of the **unified** Phase-7 splice: both the idle branch
/// and the decoded live branch are forced to this rate before the
/// `input-selector`, so the single downstream `x264enc` sees a
/// continuous stream regardless of which branch is active.
pub(crate) const UNIFIED_FPS: u32 = 15;

/// GOP length of the downstream encoder, in frames. 2 s at
/// [`UNIFIED_FPS`]. A pad-probe-driven force-key-unit at each splice
/// keeps clients in sync regardless of GOP boundaries.
pub(crate) const UNIFIED_GOP: u32 = UNIFIED_FPS * 2;

/// Downstream encoder name (looked up by [`crate::gst_pipeline`] at
/// `media-configure` time so it can dispatch force-key-unit events on
/// each idle↔live splice). Both encoder backends use this name.
pub(crate) const UNIFIED_ENCODER_NAME: &str = "video_enc";

/// Target H.264 bitrate (kbit/s) for the downstream encoder — the same
/// units for both `x264enc` and `vaapih264enc`.
pub(crate) const VIDEO_BITRATE_KBPS: u32 = 2048;

/// Name of the `gdkpixbufoverlay` in the synthetic idle branch
/// (Phase 5). [`crate::gst_pipeline`] captures it at `media-configure`
/// and sets its `location` to the latest camera snapshot so the
/// STANDBY screen shows the last thumbnail instead of a black frame.
/// It's an **inline** filter (same class as `textoverlay`): with no
/// `location` set it's a transparent pass-through, so the idle branch
/// prerolls exactly as before until the first thumbnail arrives.
pub(crate) const IDLE_OVERLAY_NAME: &str = "idle_overlay";

/// Downstream audio encoder name — a named handle keeps the audio
/// wiring symmetric with the video side (no force-key-unit: AAC frames
/// are independent).
pub(crate) const UNIFIED_AUDIO_ENCODER_NAME: &str = "audio_enc";

/// Opus RTP payload type of the live audio: the audio transceiver's PT
/// in our WebRTC offer and the live audio appsrc's caps.
pub(crate) const LIVE_RTP_OPUS_PT: i32 = 111;

// ── Phase 8b (Opus audio bridging) — via `audiomixer` ────────────────
// Live audio uses an `audiomixer`, NOT a second `input-selector`. A
// second selector stalls gst-rtsp-server's media prepare (an empty
// inactive live pad never advances, so the SDP caps on `pay1` never
// resolve). `audiomixer` always produces output from the always-on
// silent bed, so `pay1` gets caps immediately and preroll completes.
// The camera's Opus is mixed onto silence (silence = 0, so the sum is
// just the live audio); when the WebRTC leg tears down the mixer
// simply stops receiving live buffers and reverts to silence — no
// active-pad switch, no detach flip. Raw audio is pinned to F32LE
// (avenc_aac's only accepted input) on every branch.
//
// History: a first attempt fed the same `appsrc → rtpopusdepay →
// opusdec` chain into a second `input-selector`. Its inactive pad never
// produced caps, so PAUSED preroll never completed and the shared media
// was rebuilt in a loop. The decode chain was never the problem; the
// selector was.

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
    /// Per-stream directory holding the playlist and its segments.
    pub dir: String,
    /// Path to `index.m3u8`.
    pub playlist_location: String,
    /// `printf`-style segment filename pattern.
    pub segment_location: String,
    /// Target segment duration in seconds.
    pub target_duration: u32,
    /// Number of segments in the playlist window.
    pub playlist_length: u32,
    /// Segments kept on disk: the playlist window plus
    /// [`HLS_SEGMENTS_BEYOND_PLAYLIST`], so a player still downloading
    /// the oldest listed segment never gets a 404.
    pub max_files: u32,
}

/// Segments `hlssink2` keeps beyond the playlist window.
pub const HLS_SEGMENTS_BEYOND_PLAYLIST: u32 = 2;
/// Playlist file name inside [`HlsBranchConfig::dir`].
pub const HLS_PLAYLIST_FILE: &str = "index.m3u8";
/// Segment file name prefix and suffix inside [`HlsBranchConfig::dir`].
pub const HLS_SEGMENT_PREFIX: &str = "segment-";
/// See [`HLS_SEGMENT_PREFIX`].
pub const HLS_SEGMENT_SUFFIX: &str = ".ts";

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
    let playlist_length = hls.effective_playlist_length();
    HlsBranchConfig {
        playlist_location: format!("{dir}/{HLS_PLAYLIST_FILE}"),
        segment_location: format!("{dir}/{HLS_SEGMENT_PREFIX}%05d{HLS_SEGMENT_SUFFIX}"),
        target_duration: hls.effective_segment_secs(),
        playlist_length,
        max_files: playlist_length + HLS_SEGMENTS_BEYOND_PLAYLIST,
        dir,
    }
}

/// Launch string of the HLS segmenter: an RTSP client of the camera's
/// own mount (`url`, on loopback) that repackages its H.264 + AAC into
/// `hlssink2` segments without re-encoding. Reading the RTSP output
/// keeps one splice for every output (ADR 0006); `hlssink2` cuts at the
/// encoder's keyframes (every `UNIFIED_GOP` frames) and deletes
/// segments beyond [`HlsBranchConfig::max_files`].
#[must_use]
pub fn hls_segmenter_launch(url: &str, hls: &HlsBranchConfig) -> String {
    format!(
        "rtspsrc name=src location=\"{url}\" protocols=tcp latency=0 \
         src. ! rtph264depay ! h264parse ! queue ! hls.video \
         src. ! rtpmp4adepay ! aacparse ! queue ! hls.audio \
         hlssink2 name=hls location=\"{segments}\" playlist-location=\"{playlist}\" \
                  target-duration={target} playlist-length={length} max-files={max}",
        url = escape_gst_text(url),
        segments = escape_gst_text(&hls.segment_location),
        playlist = escape_gst_text(&hls.playlist_location),
        target = hls.target_duration,
        length = hls.playlist_length,
        max = hls.max_files,
    )
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

/// Build the **Phase-7 unified-encoder** per-camera launch string — a
/// single persistent pipeline where the `input-selector` operates on
/// **raw I420** and a single `x264enc` downstream produces the H.264
/// output. Video-only splice; audio is silent AAC at all times (Opus
/// bridging deferred to Phase 8b — see the module-level note).
///
/// ```text
///   {idle raw I420 chain}                                 ! sel.sink_0
///   appsrc ! rtph264depay ! avdec_h264 ! normalize-caps   ! sel.sink_1
///   input-selector name=sel ! x264enc name=video_enc
///                           ! h264parse ! rtph264pay name=pay0
///   audiotestsrc wave=silence ...                         ! rtpmp4apay name=pay1
/// ```
///
/// - Both video branches share `video/x-raw, format=I420, {W}x{H},
///   framerate={UNIFIED_FPS}/1`; switching `active-pad` is a clean
///   frame-boundary swap.
/// - A force-key-unit event is sent to `video_enc` on each splice
///   (in [`crate::gst_pipeline`]) so the encoder emits an IDR right
///   after the content switch.
/// - Clients see one continuous H.264 stream with stable SPS/PPS.
#[must_use]
pub fn combined_launch_string(idle: &IdleKind, encoder: VideoEncoder) -> String {
    // `queue` before every selector sink pad: decouples per-branch
    // streaming threads and prevents an initially-quiet branch (the
    // live appsrc before any RTP arrives) from blocking downstream
    // preroll on the active branch.
    format!(
        "( {idle_raw} ! queue max-size-buffers=8 leaky=downstream ! sel.sink_0 \
            {live_decode} ! queue max-size-buffers=8 leaky=downstream ! sel.sink_1 \
            input-selector name=sel sync-streams=true cache-buffers=false \
            ! queue max-size-buffers=8 leaky=downstream \
            ! {video_enc} \
            ! h264parse config-interval=1 \
            ! rtph264pay name=pay0 pt=96 config-interval=1 \
            {idle_audio} ! queue max-size-buffers=32 leaky=downstream ! amix.sink_0 \
            {live_audio} ! queue max-size-buffers=32 leaky=downstream ! amix.sink_1 \
            audiomixer name=amix \
            ! audioconvert \
            ! avenc_aac name={aenc} bitrate=64000 \
            ! aacparse \
            ! rtpmp4apay name=pay1 pt=97 )",
        idle_raw = idle_raw_chain(idle),
        live_decode = live_decode_chain(),
        video_enc = video_encoder_segment(encoder),
        idle_audio = idle_audio_raw_chain(),
        live_audio = live_audio_decode_chain(),
        aenc = UNIFIED_AUDIO_ENCODER_NAME,
    )
}

/// The downstream H.264 encoder segment, selected by [`VideoEncoder`].
/// Both variants are named [`UNIFIED_ENCODER_NAME`] (so
/// [`crate::gst_pipeline`] can force-key-unit them at each splice) and
/// end in the same `byte-stream,alignment=au` caps, so the downstream
/// `h264parse ! rtph264pay` is identical either way.
///
/// The VAAPI path inserts `vaapipostproc` to upload the raw I420 to a
/// VA surface. It requires `gstreamer1.0-vaapi` and a `/dev/dri` render
/// node; validate on the target host (no software fallback here — if the
/// element is missing the media fails to construct, surfaced on the
/// pipeline bus).
fn video_encoder_segment(encoder: VideoEncoder) -> String {
    let name = UNIFIED_ENCODER_NAME;
    let gop = UNIFIED_GOP;
    let kbps = VIDEO_BITRATE_KBPS;
    match encoder {
        VideoEncoder::X264 => format!(
            "x264enc name={name} tune=zerolatency speed-preset=superfast \
                      bitrate={kbps} key-int-max={gop} \
             ! video/x-h264,stream-format=byte-stream,alignment=au"
        ),
        VideoEncoder::Vaapi => format!(
            "vaapipostproc \
             ! vaapih264enc name={name} rate-control=cbr bitrate={kbps} keyframe-period={gop} \
             ! video/x-h264,stream-format=byte-stream,alignment=au"
        ),
    }
}

/// Idle producer normalized to the **unified raw caps** (Phase 7).
/// The selector forwards raw frames to the single downstream encoder.
fn idle_raw_chain(idle: &IdleKind) -> String {
    match idle {
        IdleKind::JpegStill { .. } => format!(
            "appsrc name=idle_jpeg_src is-live=true format=time \
             caps=image/jpeg,framerate={IDLE_FPS}/1 \
             ! jpegdec ! videoconvert ! videoscale ! videorate \
             ! video/x-raw,format=I420,\
width={SYNTHETIC_WIDTH},height={SYNTHETIC_HEIGHT},framerate={UNIFIED_FPS}/1"
        ),
        IdleKind::Synthetic { overlay, .. } => {
            let escaped = escape_gst_text(overlay);
            // Pin the full unified caps at the end of the chain so
            // both branches present identical caps to the selector.
            // The intermediate width/height/framerate filter forces
            // videotestsrc to that resolution and rate; the trailing
            // filter adds `format=I420` once videoconvert has done
            // the format swap.
            format!(
                "videotestsrc pattern=black is-live=true \
                 ! video/x-raw,\
width={SYNTHETIC_WIDTH},height={SYNTHETIC_HEIGHT},framerate={UNIFIED_FPS}/1 \
                 ! textoverlay text=\"{escaped}\" valignment=bottom halignment=center \
                              font-desc=\"Sans 24\" \
                 ! gdkpixbufoverlay name={IDLE_OVERLAY_NAME} \
                 ! videoconvert \
                 ! video/x-raw,format=I420,\
width={SYNTHETIC_WIDTH},height={SYNTHETIC_HEIGHT},framerate={UNIFIED_FPS}/1"
            )
        }
    }
}

/// Live RTP → decode → normalize-to-unified-caps. Output raw I420
/// matching [`idle_raw_chain`]'s caps so the selector can splice
/// cleanly into the shared downstream encoder.
fn live_decode_chain() -> String {
    let pt = LIVE_RTP_H264_PT;
    format!(
        "appsrc name=live_rtp_src is-live=true do-timestamp=true format=time \
         caps=\"application/x-rtp,media=video,encoding-name=H264,\
clock-rate=90000,payload={pt}\" \
         ! rtph264depay ! avdec_h264 \
         ! videoconvert ! videoscale ! videorate \
         ! video/x-raw,format=I420,\
width={SYNTHETIC_WIDTH},height={SYNTHETIC_HEIGHT},framerate={UNIFIED_FPS}/1"
    )
}

/// Idle audio bed — silent 48 kHz stereo `F32LE`, always on. Feeds
/// `amix.sink_0` so the audio branch always produces output (and thus
/// `pay1` always has caps) regardless of whether live audio is
/// flowing. `F32LE` is `avenc_aac`'s only accepted input format.
fn idle_audio_raw_chain() -> &'static str {
    "audiotestsrc wave=silence is-live=true \
     ! audioconvert ! audioresample \
     ! audio/x-raw,format=F32LE,rate=48000,channels=2"
}

/// Live Opus RTP → decode → normalize to the unified raw audio caps.
/// A bare `appsrc` (fed by [`crate::webrtc_pipeline`]) carrying Opus
/// RTP, decoded and pinned to `F32LE` stereo so the `audiomixer` sees
/// matching caps on both pads. Mirrors [`live_decode_chain`] on the
/// video side. Until the first Opus buffer arrives this branch is
/// silent and the mixer emits the idle bed alone.
fn live_audio_decode_chain() -> String {
    let pt = LIVE_RTP_OPUS_PT;
    format!(
        "appsrc name=live_audio_rtp_src is-live=true do-timestamp=true format=time \
         caps=\"application/x-rtp,media=audio,encoding-name=OPUS,\
clock-rate=48000,payload={pt}\" \
         ! rtpopusdepay ! opusdec \
         ! audioconvert ! audioresample \
         ! audio/x-raw,format=F32LE,rate=48000,channels=2"
    )
}

/// Escape a string for inclusion in a `gst-launch` text= property.
/// Backslashes and double quotes get escaped.
fn escape_gst_text(s: &str) -> String {
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
            video_encoder: VideoEncoder::X264,
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
        assert_eq!(hls.dir, "/var/hls/front_door");
        assert_eq!(hls.playlist_location, "/var/hls/front_door/index.m3u8");
        assert_eq!(hls.segment_location, "/var/hls/front_door/segment-%05d.ts");
        assert_eq!(hls.target_duration, 4);
        assert_eq!(hls.playlist_length, 6);
        assert_eq!(hls.max_files, 6 + HLS_SEGMENTS_BEYOND_PLAYLIST);
    }

    #[test]
    fn build_output_branches_with_hls_raises_values_below_the_floor() {
        let mut out = output(true, false);
        if let Some(h) = out.hls.as_mut() {
            h.segment_secs = 0;
            h.playlist_length = 1;
        }
        let hls = build_output_branches(&name(), &out)
            .hls
            .expect("hls present");
        assert_eq!((hls.target_duration, hls.playlist_length), (1, 3));
    }

    #[test]
    fn hls_segmenter_launch_repackages_the_rtsp_mount_without_reencoding() {
        let hls = build_output_branches(&name(), &output(true, false))
            .hls
            .expect("hls present");
        let desc = hls_segmenter_launch("rtsp://127.0.0.1:8554/front_door", &hls);
        assert!(desc.contains(r#"rtspsrc name=src location="rtsp://127.0.0.1:8554/front_door""#));
        assert!(desc.contains("rtph264depay ! h264parse"));
        assert!(desc.contains("rtpmp4adepay ! aacparse"));
        assert!(desc.contains(r#"playlist-location="/var/hls/front_door/index.m3u8""#));
        assert!(desc.contains("target-duration=4 playlist-length=6 max-files=8"));
        for codec in [
            "x264enc",
            "vaapih264enc",
            "avenc_aac",
            "avdec_h264",
            "opusdec",
        ] {
            assert!(
                !desc.contains(codec),
                "the segmenter must not transcode ({codec}): {desc}"
            );
        }
    }

    #[test]
    fn build_output_branches_with_dash_uses_per_stream_dir() {
        let b = build_output_branches(&name(), &output(false, true));
        let dash = b.dash.expect("dash present");
        assert_eq!(dash.manifest_location, "/var/dash/front_door/manifest.mpd");
        assert_eq!(dash.target_duration, 2);
    }

    #[test]
    fn combined_launch_string_escapes_quotes_and_backslashes_in_overlay() {
        let idle = IdleKind::Synthetic {
            stream_name: name(),
            overlay: r#"say "hi" \ bye"#.to_string(),
        };
        let desc = combined_launch_string(&idle, VideoEncoder::X264);
        assert!(desc.contains(r#"text="say \"hi\" \\ bye""#), "{desc}");
    }

    #[test]
    fn escape_gst_text_escapes_quotes_and_backslashes() {
        assert_eq!(escape_gst_text("a\"b\\c"), "a\\\"b\\\\c");
    }

    #[test]
    fn escape_gst_text_passes_through_safe_input() {
        assert_eq!(escape_gst_text("plain text"), "plain text");
    }

    fn synth_idle() -> IdleKind {
        IdleKind::Synthetic {
            stream_name: name(),
            overlay: "STANDBY".to_string(),
        }
    }

    #[test]
    fn combined_launch_string_has_idle_and_live_video_branches_into_selector() {
        let s = combined_launch_string(&synth_idle(), VideoEncoder::X264);
        assert!(s.contains("sel.sink_0"));
        assert!(s.contains("appsrc name=live_rtp_src"));
        assert!(s.contains("rtph264depay"));
        assert!(s.contains("sel.sink_1"));
        assert!(s.contains("input-selector name=sel"));
        assert!(s.contains("sync-streams=true"));
    }

    #[test]
    fn combined_launch_string_has_exactly_one_video_encoder_and_payloader() {
        // Phase-7 contract: one downstream video encoder produces the
        // whole H.264 stream so VLC never sees an SPS/PPS change at
        // the splice.
        let s = combined_launch_string(&synth_idle(), VideoEncoder::X264);
        assert_eq!(s.matches("x264enc").count(), 1);
        assert!(s.contains(&format!("name={UNIFIED_ENCODER_NAME}")));
        assert_eq!(s.matches("rtph264pay").count(), 1);
        assert!(s.contains("rtph264pay name=pay0 pt=96"));
        assert_eq!(s.matches("h264parse").count(), 1);
    }

    #[test]
    fn combined_launch_string_x264_is_default_software_encoder() {
        let s = combined_launch_string(&synth_idle(), VideoEncoder::X264);
        assert!(s.contains(&format!("x264enc name={UNIFIED_ENCODER_NAME}")));
        assert!(s.contains(&format!("bitrate={VIDEO_BITRATE_KBPS}")));
        assert!(!s.contains("vaapi"));
    }

    #[test]
    fn combined_launch_string_vaapi_uses_hardware_encoder() {
        // VAAPI backend: vaapipostproc uploads to a VA surface, then the
        // hardware encoder (same name, so force-key-unit still works) and
        // the *same* downstream h264parse → rtph264pay.
        let s = combined_launch_string(&synth_idle(), VideoEncoder::Vaapi);
        assert!(s.contains(&format!("vaapih264enc name={UNIFIED_ENCODER_NAME}")));
        assert!(s.contains("vaapipostproc"));
        assert!(s.contains(&format!("bitrate={VIDEO_BITRATE_KBPS}")));
        assert!(!s.contains("x264enc"));
        assert!(s.contains("h264parse config-interval=1"));
        assert!(s.contains("rtph264pay name=pay0 pt=96"));
    }

    #[test]
    fn combined_launch_string_decodes_live_via_avdec_h264() {
        let s = combined_launch_string(&synth_idle(), VideoEncoder::X264);
        assert!(s.contains("avdec_h264"));
        assert!(s.contains("videoscale"));
        assert!(s.contains("videorate"));
    }

    #[test]
    fn synthetic_idle_carries_thumbnail_overlay() {
        // Phase 5: the synthetic idle branch has an inline
        // gdkpixbufoverlay (named for runtime capture) sized to the
        // full frame, sitting after the STANDBY textoverlay.
        let s = combined_launch_string(&synth_idle(), VideoEncoder::X264);
        assert!(s.contains(&format!("gdkpixbufoverlay name={IDLE_OVERLAY_NAME}")));
        // Sizing is applied at runtime (see gst_pipeline::apply_thumbnail_overlay)
        // because gdkpixbufoverlay ignores construction-time overlay-width/height
        // when a `location` is loaded later.
        // Overlay must sit after the text overlay and before the final
        // I420 convert so STANDBY text shows until a snapshot loads.
        let text = s.find("textoverlay").expect("textoverlay present");
        let pix = s
            .find("gdkpixbufoverlay")
            .expect("gdkpixbufoverlay present");
        assert!(text < pix, "textoverlay should precede gdkpixbufoverlay");
    }

    #[test]
    fn combined_launch_string_pins_unified_raw_caps_on_both_video_branches() {
        let s = combined_launch_string(&synth_idle(), VideoEncoder::X264);
        let expected = format!(
            "format=I420,width={SYNTHETIC_WIDTH},height={SYNTHETIC_HEIGHT},framerate={UNIFIED_FPS}/1"
        );
        assert!(
            s.matches(&expected).count() >= 2,
            "expected unified raw video caps on both branches in:\n{s}"
        );
    }

    #[test]
    fn combined_launch_string_emits_single_audio_pay1() {
        // One silent bed + one live Opus branch mix into one AAC pay1.
        let s = combined_launch_string(&synth_idle(), VideoEncoder::X264);
        assert!(s.contains("audiotestsrc"));
        assert!(s.contains("wave=silence"));
        assert_eq!(s.matches("rtpmp4apay").count(), 1);
        assert!(s.contains("rtpmp4apay name=pay1 pt=97"));
        assert_eq!(s.matches("avenc_aac").count(), 1);
    }

    #[test]
    fn combined_launch_string_bridges_live_audio_via_audiomixer() {
        // Phase 8b: live audio uses an audiomixer (never a second
        // input-selector, which stalls gst-rtsp-server prepare). The
        // silent bed feeds sink_0, the decoded Opus feeds sink_1.
        let s = combined_launch_string(&synth_idle(), VideoEncoder::X264);
        assert!(s.contains("audiomixer name=amix"));
        assert!(s.contains("amix.sink_0"));
        assert!(s.contains("amix.sink_1"));
        assert!(s.contains("appsrc name=live_audio_rtp_src"));
        assert!(s.contains("rtpopusdepay ! opusdec"));
        // No second input-selector anywhere.
        assert!(!s.contains("sel_a"));
        assert_eq!(s.matches("input-selector").count(), 1);
        // Both mixer inputs pinned to F32LE (avenc_aac's only format).
        assert!(s.matches("format=F32LE,rate=48000,channels=2").count() >= 2);
    }

    #[test]
    fn combined_launch_string_pins_live_h264_pt_103() {
        assert_eq!(LIVE_RTP_H264_PT, 103);
        let s = combined_launch_string(&synth_idle(), VideoEncoder::X264);
        assert!(s.contains("payload=103"));
        assert!(s.contains("encoding-name=H264"));
    }

    #[test]
    fn combined_launch_string_jpeg_idle_variant_builds() {
        let s = combined_launch_string(
            &IdleKind::JpegStill {
                jpeg: bytes::Bytes::from_static(&[0]),
            },
            VideoEncoder::X264,
        );
        assert!(s.contains("appsrc name=idle_jpeg_src"));
        assert!(s.contains("jpegdec"));
        assert!(s.contains("sel.sink_0"));
        assert!(s.contains("appsrc name=live_rtp_src"));
        assert!(s.contains("sel.sink_1"));
    }

    #[test]
    fn combined_launch_string_payloader_has_rtp_level_safety_net() {
        let s = combined_launch_string(&synth_idle(), VideoEncoder::X264);
        assert!(s.contains("rtph264pay name=pay0 pt=96 config-interval=1"));
    }

    #[test]
    fn combined_launch_string_encoder_uses_unified_gop() {
        let s = combined_launch_string(&synth_idle(), VideoEncoder::X264);
        assert!(s.contains(&format!("key-int-max={UNIFIED_GOP}")));
        assert!(s.contains("tune=zerolatency"));
        assert!(s.contains("speed-preset=superfast"));
    }
}
