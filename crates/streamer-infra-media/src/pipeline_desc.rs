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

use crate::live_rtp_sink::AacRtpFormat;
use streamer_domain::camera::StreamName;
use streamer_domain::config::{HlsOutput, OutputConfig};

use crate::encoder::VideoEncoding;

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

/// Name of the `textoverlay` carrying the synthetic idle frame's caption.
/// [`crate::gst_pipeline`] captures it at `media-configure` to switch the
/// caption (standby / live in the Arlo app) without rebuilding the media.
pub(crate) const IDLE_CAPTION_NAME: &str = "idle_caption";

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
/// AAC (RFC 3640) RTP payload type of the relayed app-view audio
/// (ADR 0007): the relay rewrites the stream's PT to it, and the AAC
/// appsrc's caps declare it.
pub(crate) const LIVE_RTP_AAC_PT: i32 = 98;
/// Name of the AAC audio `appsrc` in the unified pipeline.
pub(crate) const LIVE_AAC_SRC_NAME: &str = "live_aac_rtp_src";

/// The AAC format the appsrc starts with — AAC-LC, 16 kHz mono, as
/// Arlo's cameras send it — until a relay announces the stream's own
/// (`LiveAacSink::configure`).
#[must_use]
pub(crate) fn default_aac_format() -> AacRtpFormat {
    AacRtpFormat {
        clock_rate: 16000,
        channels: 1,
        config: "1408".to_string(),
        size_length: 13,
        index_length: 3,
        index_delta_length: 3,
    }
}

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
    /// Output stream name (used as RTSP path component and HLS subdir).
    pub stream_name: StreamName,
    /// `/<stream_name>` — RTSP mount path under the embedded server.
    pub rtsp_mount_path: String,
    /// HLS branch config, present when `[output.hls]` is set.
    pub hls: Option<HlsBranchConfig>,
}

/// Resolved HLS sink parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HlsBranchConfig {
    /// The configured HLS root (`output.hls.dir`); `dir` must be a real
    /// directory directly under it.
    pub root: String,
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
    }
}

fn build_hls_branch(name: &StreamName, hls: &HlsOutput) -> HlsBranchConfig {
    let dir = format_dir(&hls.dir, name);
    let playlist_length = hls.effective_playlist_length();
    HlsBranchConfig {
        root: hls.dir.display().to_string(),
        playlist_location: format!("{dir}/{HLS_PLAYLIST_FILE}"),
        // `splitmuxsink` runs the pattern through printf: a `%` in the
        // operator's path must be a literal there, or `%s` dereferences
        // the fragment id as a pointer.
        segment_location: format!(
            "{}/{HLS_SEGMENT_PREFIX}%05d{HLS_SEGMENT_SUFFIX}",
            dir.replace('%', "%%")
        ),
        target_duration: hls.effective_segment_secs(),
        playlist_length,
        max_files: playlist_length.saturating_add(HLS_SEGMENTS_BEYOND_PLAYLIST),
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

fn format_dir(root: &Path, name: &StreamName) -> String {
    // `Path::display` is lossy on non-UTF-8, but every Linux deployment
    // we target is UTF-8 and the config schema is `PathBuf` not `OsStr`.
    format!("{}/{}", root.display(), name)
}

/// Build the per-camera launch string — one persistent pipeline where
/// the `input-selector` operates on **raw I420** and a single encoder
/// (the camera's resolved [`VideoEncoding`]) produces the H.264 output. Audio is
/// an `audiomixer` of the silent bed, the WebRTC leg's Opus and the
/// app-view relay's AAC, encoded once to AAC (ADR 0003, 0007).
///
/// ```text
///   {idle raw I420 chain}                                 ! sel.sink_0
///   appsrc ! rtph264depay ! avdec_h264 ! normalize-caps   ! sel.sink_1
///   input-selector name=sel ! {encoder} name=video_enc
///                           ! h264parse ! rtph264pay name=pay0
///   audiotestsrc wave=silence                             ! amix.sink_0
///   appsrc (Opus RTP) ! rtpopusdepay ! opusdec            ! amix.sink_1
///   appsrc (AAC RTP)  ! rtpmp4gdepay ! avdec_aac          ! amix.sink_2
///   audiomixer name=amix ! avenc_aac ! rtpmp4apay name=pay1
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
pub fn combined_launch_string(idle: &IdleKind, encoding: VideoEncoding) -> String {
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
            {live_aac} ! queue max-size-buffers=32 leaky=downstream ! amix.sink_2 \
            audiomixer name=amix \
            ! audioconvert \
            ! avenc_aac name={aenc} bitrate=64000 \
            ! aacparse \
            ! rtpmp4apay name=pay1 pt=97 )",
        idle_raw = idle_raw_chain(idle),
        live_decode = live_decode_chain(),
        video_enc = encoding.segment(),
        idle_audio = idle_audio_raw_chain(),
        live_audio = live_audio_decode_chain(),
        live_aac = live_aac_decode_chain(),
        aenc = UNIFIED_AUDIO_ENCODER_NAME,
    )
}

/// Idle producer normalized to the **unified raw caps** (Phase 7).
/// The selector forwards raw frames to the single downstream encoder.
///
/// The picture is drawn [`IDLE_FPS`] times a second and `imagefreeze`
/// repeats each drawing up to the unified rate (ADR 0010): drawing the
/// full-frame snapshot 15 times a second cost a third of a core per
/// connected camera, for a picture that changes a few times an hour.
/// `allow-replace` takes each new drawing, so a new snapshot or caption
/// shows within one. The snapshot (`gdkpixbufoverlay`, scaled to fill)
/// goes under the caption: the other way round it covered it.
pub(crate) fn idle_raw_chain(idle: &IdleKind) -> String {
    let IdleKind::Synthetic { overlay, .. } = idle;
    let escaped = escape_gst_text(overlay);
    format!(
        "videotestsrc pattern=black is-live=true \
         ! video/x-raw,\
width={SYNTHETIC_WIDTH},height={SYNTHETIC_HEIGHT},framerate={IDLE_FPS}/1 \
         ! gdkpixbufoverlay name={IDLE_OVERLAY_NAME} \
         ! textoverlay name={IDLE_CAPTION_NAME} text=\"{escaped}\" valignment=bottom \
                      halignment=center font-desc=\"Sans 24\" \
         ! videoconvert \
         ! imagefreeze is-live=true allow-replace=true \
         ! video/x-raw,format=I420,\
width={SYNTHETIC_WIDTH},height={SYNTHETIC_HEIGHT},framerate={UNIFIED_FPS}/1"
    )
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

/// Relayed AAC RTP (ADR 0007) → depayload → decode → the unified raw
/// audio caps, onto the mixer's third pad. Silent until a relay pushes;
/// the caps follow the relayed stream's SDP (the pump sets them).
fn live_aac_decode_chain() -> String {
    let caps = default_aac_format().caps_string(LIVE_RTP_AAC_PT);
    format!(
        "appsrc name={LIVE_AAC_SRC_NAME} is-live=true do-timestamp=true format=time \
         caps=\"{caps}\" \
         ! rtpmp4gdepay ! aacparse ! avdec_aac \
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
    use crate::encoder::EncoderBackend;
    use std::path::PathBuf;
    use streamer_domain::config::{
        DEFAULT_VIDEO_BITRATE_KBPS, HlsOutput, OutputConfig, RtspOutput, VideoEncoder,
    };

    fn enc(backend: EncoderBackend) -> VideoEncoding {
        VideoEncoding::at_default_bitrate(backend)
    }

    fn name() -> StreamName {
        StreamName::parse("front_door").unwrap()
    }

    fn output(hls: bool) -> OutputConfig {
        OutputConfig {
            rtsp: RtspOutput {
                bind: "0.0.0.0:8554".to_string(),
            },
            hls: hls.then_some(HlsOutput {
                dir: PathBuf::from("/var/hls"),
                segment_secs: 4,
                playlist_length: 6,
            }),
            dash: None,
            video_encoder: VideoEncoder::X264,
            bitrate_kbps: DEFAULT_VIDEO_BITRATE_KBPS,
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
        let b = build_output_branches(&name(), &output(false));
        assert_eq!(b.rtsp_mount_path, "/front_door");
        assert!(b.hls.is_none());
    }

    #[test]
    fn build_output_branches_with_hls_uses_per_stream_dir() {
        let b = build_output_branches(&name(), &output(true));
        let hls = b.hls.expect("hls present");
        assert_eq!(hls.dir, "/var/hls/front_door");
        assert_eq!(hls.playlist_location, "/var/hls/front_door/index.m3u8");
        assert_eq!(hls.segment_location, "/var/hls/front_door/segment-%05d.ts");
        assert_eq!(hls.target_duration, 4);
        assert_eq!(hls.playlist_length, 6);
        assert_eq!(hls.max_files, 6 + HLS_SEGMENTS_BEYOND_PLAYLIST);
    }

    #[test]
    fn build_output_branches_escapes_percent_in_the_segment_pattern_only() {
        let out = HlsOutput {
            dir: PathBuf::from("/var/h%sls"),
            segment_secs: 4,
            playlist_length: 6,
        };
        let hls = build_hls_branch(&name(), &out);
        assert_eq!(hls.root, "/var/h%sls");
        assert_eq!(hls.dir, "/var/h%sls/front_door");
        assert_eq!(hls.playlist_location, "/var/h%sls/front_door/index.m3u8");
        assert_eq!(
            hls.segment_location,
            "/var/h%%sls/front_door/segment-%05d.ts"
        );
    }

    #[test]
    fn build_output_branches_with_hls_raises_values_below_the_floor() {
        let mut out = output(true);
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
        let hls = build_output_branches(&name(), &output(true))
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
    fn combined_launch_string_escapes_quotes_and_backslashes_in_overlay() {
        let idle = IdleKind::Synthetic {
            stream_name: name(),
            overlay: r#"say "hi" \ bye"#.to_string(),
        };
        let desc = combined_launch_string(&idle, enc(EncoderBackend::X264));
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
        let s = combined_launch_string(&synth_idle(), enc(EncoderBackend::X264));
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
        let s = combined_launch_string(&synth_idle(), enc(EncoderBackend::X264));
        assert_eq!(s.matches("x264enc").count(), 1);
        assert!(s.contains(&format!("name={UNIFIED_ENCODER_NAME}")));
        assert_eq!(s.matches("rtph264pay").count(), 1);
        assert!(s.contains("rtph264pay name=pay0 pt=96"));
        assert_eq!(s.matches("h264parse").count(), 1);
    }

    #[test]
    fn combined_launch_string_x264_is_default_software_encoder() {
        let s = combined_launch_string(&synth_idle(), enc(EncoderBackend::X264));
        assert!(s.contains(&format!("x264enc name={UNIFIED_ENCODER_NAME}")));
        assert!(s.contains(&format!("bitrate={DEFAULT_VIDEO_BITRATE_KBPS}")));
        assert!(!s.contains("vaapi"));
    }

    #[test]
    fn combined_launch_string_vaapi_uses_hardware_encoder() {
        // VAAPI backend: vaapipostproc uploads to a VA surface, then the
        // hardware encoder (same name, so force-key-unit still works) and
        // the *same* downstream h264parse → rtph264pay.
        let s = combined_launch_string(&synth_idle(), enc(EncoderBackend::Vaapi));
        assert!(s.contains(&format!("vaapih264enc name={UNIFIED_ENCODER_NAME}")));
        let v4l2 = combined_launch_string(&synth_idle(), enc(EncoderBackend::V4l2));
        assert!(v4l2.contains(&format!("v4l2h264enc name={UNIFIED_ENCODER_NAME}")));
        let nvenc = combined_launch_string(&synth_idle(), enc(EncoderBackend::Nvenc));
        assert!(nvenc.contains(&format!("nvh264enc name={UNIFIED_ENCODER_NAME}")));
        assert!(s.contains("vaapipostproc"));
        assert!(s.contains(&format!("bitrate={DEFAULT_VIDEO_BITRATE_KBPS}")));
        assert!(!s.contains("x264enc"));
        assert!(s.contains("h264parse config-interval=1"));
        assert!(s.contains("rtph264pay name=pay0 pt=96"));
    }

    #[test]
    fn combined_launch_string_decodes_live_via_avdec_h264() {
        let s = combined_launch_string(&synth_idle(), enc(EncoderBackend::X264));
        assert!(s.contains("avdec_h264"));
        assert!(s.contains("videoscale"));
        assert!(s.contains("videorate"));
    }

    #[test]
    fn synthetic_idle_carries_thumbnail_overlay() {
        // Phase 5: the synthetic idle branch has an inline
        // gdkpixbufoverlay (named for runtime capture), scaled to the full
        // frame at runtime (gst_pipeline::apply_thumbnail_overlay).
        let s = combined_launch_string(&synth_idle(), enc(EncoderBackend::X264));
        assert!(s.contains(&format!("gdkpixbufoverlay name={IDLE_OVERLAY_NAME}")));
        // The full-frame snapshot goes under the caption: drawn after it,
        // it covered the STANDBY / LIVE IN ARLO APP text.
        let pix = s
            .find("gdkpixbufoverlay")
            .expect("gdkpixbufoverlay present");
        let text = s.find("textoverlay").expect("textoverlay present");
        assert!(pix < text, "the caption must be drawn over the snapshot");
    }

    /// ADR 0010: the idle picture is drawn once a second and repeated up
    /// to the unified rate, so the selector still sees 15 fps on both
    /// sides.
    #[test]
    fn the_idle_picture_is_drawn_at_the_idle_rate_and_frozen_to_the_unified_one() {
        let s = combined_launch_string(&synth_idle(), enc(EncoderBackend::X264));
        let drawn =
            format!("width={SYNTHETIC_WIDTH},height={SYNTHETIC_HEIGHT},framerate={IDLE_FPS}/1");
        let draw = s.find(&drawn).expect("the drawing rate");
        let freeze = s
            .find("imagefreeze is-live=true allow-replace=true")
            .expect("imagefreeze");
        let caption = s.find("textoverlay").expect("caption");
        let unified = format!(
            "format=I420,width={SYNTHETIC_WIDTH},height={SYNTHETIC_HEIGHT},framerate={UNIFIED_FPS}/1"
        );
        let selector = s.find("sel.sink_0").expect("idle pad");
        let idle_caps = s[..selector]
            .rfind(&unified)
            .expect("unified caps on the idle side");
        assert!(
            draw < caption && caption < freeze && freeze < idle_caps,
            "{s}"
        );
        assert_eq!(IDLE_FPS, 1);
    }

    #[test]
    fn combined_launch_string_pins_unified_raw_caps_on_both_video_branches() {
        let s = combined_launch_string(&synth_idle(), enc(EncoderBackend::X264));
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
        let s = combined_launch_string(&synth_idle(), enc(EncoderBackend::X264));
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
        let s = combined_launch_string(&synth_idle(), enc(EncoderBackend::X264));
        assert!(s.contains("audiomixer name=amix"));
        assert!(s.contains("amix.sink_0"));
        assert!(s.contains("amix.sink_1"));
        assert!(s.contains("appsrc name=live_audio_rtp_src"));
        // The relayed app-view audio (AAC) has its own decode branch on sink_2.
        assert!(s.contains("amix.sink_2"));
        assert!(s.contains("appsrc name=live_aac_rtp_src"));
        assert!(s.contains("encoding-name=MPEG4-GENERIC"));
        assert!(s.contains("rtpmp4gdepay ! aacparse ! avdec_aac"));
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
        let s = combined_launch_string(&synth_idle(), enc(EncoderBackend::X264));
        assert!(s.contains("payload=103"));
        assert!(s.contains("encoding-name=H264"));
    }

    #[test]
    fn combined_launch_string_payloader_has_rtp_level_safety_net() {
        let s = combined_launch_string(&synth_idle(), enc(EncoderBackend::X264));
        assert!(s.contains("rtph264pay name=pay0 pt=96 config-interval=1"));
    }

    #[test]
    fn combined_launch_string_encoder_uses_unified_gop() {
        let s = combined_launch_string(&synth_idle(), enc(EncoderBackend::X264));
        assert!(s.contains(&format!("key-int-max={UNIFIED_GOP}")));
        assert!(s.contains("tune=zerolatency"));
        assert!(s.contains("speed-preset=superfast"));
    }
}
