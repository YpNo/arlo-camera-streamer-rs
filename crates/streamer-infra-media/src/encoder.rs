//! H.264 encoder backends and the startup probe behind
//! `output.video_encoder = "auto"` (ADR 0008).
//!
//! The unified pipeline runs one encoder per connected camera, so the
//! backend decides the per-camera CPU cost. The domain's
//! [`VideoEncoder`] is what the owner asked for; [`EncoderBackend`] is a
//! concrete choice the launch string can be built from. `auto` tries the
//! backends in [`EncoderBackend::AUTO_ORDER`]: a backend qualifies when
//! its elements are registered **and** a short dry run reaches
//! end-of-stream — a VAAPI plugin without a render node loads fine and
//! fails only when a pipeline starts. An explicit choice is dry-run too,
//! so a misconfigured host fails at boot rather than at the first client.
//!
//! Hardware notes recorded in the ADR: Raspberry Pi 4 / Zero 2 / CM4
//! encode through V4L2 M2M (`/dev/video11`); the Pi 5 has **no** H.264
//! hardware encoder and lands on x264; NVIDIA needs the container
//! toolkit; Intel/AMD need `/dev/dri` and the VA drivers.
//!
//! Intel's fixed-function encoder (`vah264lpenc`) is the only one Debian's
//! free driver offers from Skylake on, and without the GPU's `HuC` firmware
//! it encodes at constant quality only: `va` tries the full encoder, then
//! low power with bitrate control, then low power at constant quality. A
//! driver without CBR rejects that property value, so the dry run decides.

use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::time::Duration;

use gstreamer as gst;
use gstreamer::prelude::*;
use tracing::{debug, info, warn};

use streamer_domain::camera::CameraId;
use streamer_domain::config::{DEFAULT_VIDEO_BITRATE_KBPS, VideoEncoder};

use crate::error::MediaError;
use crate::pipeline_desc::{UNIFIED_ENCODER_NAME, UNIFIED_GOP};

/// How long a backend's dry run may take to reach end-of-stream.
const DRY_RUN_TIMEOUT: Duration = Duration::from_secs(3);
/// Frames pushed through the dry run; enough for an encoder to emit.
const DRY_RUN_FRAMES: u32 = 10;

/// The GPU render nodes live here; a node with no working encoder is
/// worth a warning when `auto` falls back to the CPU.
const DRI_DIR: &str = "/dev/dri";
/// Quantizer of the constant-quality low-power encoder: the driver's own
/// default, a moderate quality. The bitrate follows the scene.
pub const LOW_POWER_QP: u32 = 26;

/// Rate control of the `va` plugin's low-power encoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateControl {
    /// Constant bitrate at the camera's bitrate. On Intel the driver
    /// offers it only with the GPU's `HuC` firmware loaded.
    Cbr,
    /// Constant quality at [`LOW_POWER_QP`]; what a driver without `HuC`
    /// offers (Debian's free Intel driver on Skylake, for one).
    Cqp,
}

impl RateControl {
    const fn label(self) -> &'static str {
        match self {
            Self::Cbr => "rate-control=cbr",
            Self::Cqp => "rate-control=cqp",
        }
    }

    fn properties(self, kbps: u32) -> String {
        match self {
            Self::Cbr => format!("{} bitrate={kbps}", self.label()),
            Self::Cqp => format!("{} qpi={LOW_POWER_QP} qpp={LOW_POWER_QP}", self.label()),
        }
    }
}

/// A concrete H.264 encoder the launch string is built from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncoderBackend {
    /// Software `x264enc`; works everywhere, costs about 0.6 core per camera.
    X264,
    /// Intel/AMD through the `va` plugin (`vah264enc`, GStreamer ≥ 1.22).
    Va,
    /// The `va` plugin's low-power encoder (`vah264lpenc`), fed from
    /// system memory. Intel's fixed-function encoder: the only H.264
    /// encoder Debian's free driver offers on Skylake and later.
    VaLowPower(RateControl),
    /// Intel/AMD through the older `vaapi` plugin (`vaapih264enc`).
    Vaapi,
    /// V4L2 memory-to-memory (`v4l2h264enc`): Raspberry Pi 4 family and similar boards.
    V4l2,
    /// NVIDIA (`nvh264enc`).
    Nvenc,
}

impl EncoderBackend {
    /// The order `auto` tries: dedicated hardware first, software last.
    pub const AUTO_ORDER: [Self; 7] = [
        Self::Nvenc,
        Self::Va,
        Self::VaLowPower(RateControl::Cbr),
        Self::VaLowPower(RateControl::Cqp),
        Self::Vaapi,
        Self::V4l2,
        Self::X264,
    ];
    /// What `video_encoder = "va"` accepts, in order: the full encoder,
    /// then the low-power one with bitrate control, then without.
    const VA_ORDER: [Self; 3] = [
        Self::Va,
        Self::VaLowPower(RateControl::Cbr),
        Self::VaLowPower(RateControl::Cqp),
    ];

    /// The backends a configuration accepts, in the order they are tried.
    #[must_use]
    pub const fn candidates(config: VideoEncoder) -> &'static [Self] {
        match config {
            VideoEncoder::Auto => &Self::AUTO_ORDER,
            VideoEncoder::X264 => &[Self::X264],
            VideoEncoder::Va => &Self::VA_ORDER,
            VideoEncoder::Vaapi => &[Self::Vaapi],
            VideoEncoder::V4l2 => &[Self::V4l2],
            VideoEncoder::Nvenc => &[Self::Nvenc],
        }
    }

    /// The configuration name (`x264`, `va`, …), for logs.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::X264 => "x264",
            Self::Va | Self::VaLowPower(_) => "va",
            Self::Vaapi => "vaapi",
            Self::V4l2 => "v4l2",
            Self::Nvenc => "nvenc",
        }
    }

    /// The GStreamer plugin the encoder element comes from.
    const fn plugin(self) -> &'static str {
        match self {
            Self::X264 => "x264",
            Self::Va | Self::VaLowPower(_) => "va",
            Self::Vaapi => "vaapi",
            Self::V4l2 => "video4linux2",
            Self::Nvenc => "nvcodec",
        }
    }

    /// The GStreamer elements the backend's segment needs, the encoder last.
    #[must_use]
    pub const fn elements(self) -> &'static [&'static str] {
        match self {
            Self::X264 => &["x264enc"],
            Self::Va => &["vapostproc", "vah264enc"],
            Self::VaLowPower(_) => &["videoconvert", "vah264lpenc"],
            Self::Vaapi => &["vaapipostproc", "vaapih264enc"],
            Self::V4l2 => &["videoconvert", "v4l2h264enc"],
            Self::Nvenc => &["videoconvert", "nvh264enc"],
        }
    }

    /// The encoder element, with the rate control where the backend has a
    /// choice: what the boot log reports.
    #[must_use]
    pub fn describe(self) -> String {
        let element = self.elements().last().copied().unwrap_or_default();
        match self {
            Self::VaLowPower(rate) => format!("{element} {}", rate.label()),
            _ => element.to_string(),
        }
    }

    /// The encoder segment of the unified launch string. Every variant
    /// names its encoder `video_enc` (so the registry can force-key-unit
    /// it at each splice) and ends in the same
    /// `byte-stream,alignment=au` caps, so the downstream
    /// `h264parse ! rtph264pay` is identical.
    #[must_use]
    pub fn segment(self, kbps: u32) -> String {
        let name = UNIFIED_ENCODER_NAME;
        let gop = UNIFIED_GOP;
        let caps = "video/x-h264,stream-format=byte-stream,alignment=au";
        match self {
            Self::X264 => format!(
                "x264enc name={name} tune=zerolatency speed-preset=superfast \
                          bitrate={kbps} key-int-max={gop} \
                 ! {caps}"
            ),
            Self::Va => format!(
                "vapostproc \
                 ! vah264enc name={name} rate-control=cbr bitrate={kbps} key-int-max={gop} \
                 ! {caps}"
            ),
            // No `vapostproc`: a driver may offer the encoder without the
            // video processor (Skylake's does). NV12 from the CPU is cheap
            // at the unified size; no B-frames, for latency.
            Self::VaLowPower(rate) => format!(
                "videoconvert ! video/x-raw,format=NV12 \
                 ! vah264lpenc name={name} {rate} key-int-max={gop} b-frames=0 \
                 ! {caps}",
                rate = rate.properties(kbps)
            ),
            Self::Vaapi => format!(
                "vaapipostproc \
                 ! vaapih264enc name={name} rate-control=cbr bitrate={kbps} keyframe-period={gop} \
                 ! {caps}"
            ),
            // V4L2 controls are the kernel's: bitrate in bit/s, GOP as the
            // I-frame period; SPS/PPS repeated so a joining client decodes.
            Self::V4l2 => format!(
                "videoconvert ! video/x-raw,format=NV12 \
                 ! v4l2h264enc name={name} \
                   extra-controls=\"controls,video_bitrate={bps},h264_i_frame_period={gop},repeat_sequence_header=1\" \
                 ! {caps}",
                bps = kbps * 1000
            ),
            Self::Nvenc => format!(
                "videoconvert \
                 ! nvh264enc name={name} rc-mode=cbr bitrate={kbps} gop-size={gop} \
                 ! {caps}"
            ),
        }
    }
}

/// A resolved encoder and the bitrate it encodes at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VideoEncoding {
    /// The encoder.
    pub backend: EncoderBackend,
    /// Its bitrate in kbit/s; a constant-quality backend ignores it.
    pub bitrate_kbps: u32,
}

impl VideoEncoding {
    /// `backend` at the default bitrate.
    #[must_use]
    pub const fn at_default_bitrate(backend: EncoderBackend) -> Self {
        Self {
            backend,
            bitrate_kbps: DEFAULT_VIDEO_BITRATE_KBPS,
        }
    }

    /// The encoder segment of the unified launch string.
    #[must_use]
    pub fn segment(self) -> String {
        self.backend.segment(self.bitrate_kbps)
    }
}

/// How every camera encodes: one default, and the cameras configured
/// otherwise.
#[derive(Debug, Clone)]
pub struct EncoderPlan {
    default: VideoEncoding,
    cameras: HashMap<CameraId, VideoEncoding>,
}

impl EncoderPlan {
    /// Every camera encodes as `default`.
    #[must_use]
    pub fn uniform(default: VideoEncoding) -> Self {
        Self {
            default,
            cameras: HashMap::new(),
        }
    }

    /// `camera` encodes as `encoding` instead.
    #[must_use]
    pub fn with_camera(mut self, camera: CameraId, encoding: VideoEncoding) -> Self {
        self.cameras.insert(camera, encoding);
        self
    }

    /// How `camera` encodes.
    #[must_use]
    pub fn for_camera(&self, camera: &CameraId) -> VideoEncoding {
        self.cameras.get(camera).copied().unwrap_or(self.default)
    }
}

impl fmt::Display for EncoderBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Turn the configured encoder into a working backend: the first of
/// [`EncoderBackend::candidates`] whose elements are registered and whose
/// dry run succeeds (`auto` tries them all, `va` its three variants). Call
/// after `gstreamer::init()`; it blocks for the dry runs.
///
/// # Errors
///
/// [`MediaError::Pipeline`] when no candidate works: for an explicit
/// choice, with each candidate's reason.
pub fn resolve(config: VideoEncoder) -> Result<EncoderBackend, MediaError> {
    let candidates = EncoderBackend::candidates(config);
    let mut skipped = Vec::new();
    for &candidate in candidates {
        match verify(candidate) {
            Ok(()) => {
                announce(config, candidate, &skipped);
                return Ok(candidate);
            }
            Err(why) => {
                debug!(encoder = candidate.label(), element = %candidate.describe(), %why, "encoder skipped");
                skipped.push(format!("{}: {why}", candidate.describe()));
            }
        }
    }
    Err(MediaError::Pipeline(match config {
        VideoEncoder::Auto => "no H.264 encoder works on this host (not even x264enc)".into(),
        _ => format!(
            "video encoder '{}' is not usable on this host: {}",
            candidates.first().map_or("?", |c| c.label()),
            skipped.join("; ")
        ),
    }))
}

/// Log the choice, what it implies, and a CPU fallback on a host with a GPU.
fn announce(config: VideoEncoder, chosen: EncoderBackend, skipped: &[String]) {
    let element = chosen.describe();
    if config == VideoEncoder::Auto {
        info!(encoder = chosen.label(), %element, "video encoder (auto)");
    } else {
        info!(encoder = chosen.label(), %element, "video encoder");
    }
    if chosen == EncoderBackend::VaLowPower(RateControl::Cqp) {
        info!(
            qp = LOW_POWER_QP,
            "the driver offers no bitrate control for low-power encoding; constant quality, \
             the bitrate follows the scene"
        );
    }
    if config == VideoEncoder::Auto
        && chosen == EncoderBackend::X264
        && has_render_node(Path::new(DRI_DIR))
    {
        warn!(
            skipped = %skipped.join("; "),
            "a GPU render node is present but no hardware H.264 encoder works; encoding on \
             the CPU. \"no usable device\": is the device passed through and readable by this \
             user (its group), and does the driver encode H.264?"
        );
    }
}

/// Whether `dir` holds a GPU render node (`renderD128`, …).
fn has_render_node(dir: &Path) -> bool {
    std::fs::read_dir(dir).is_ok_and(|entries| {
        entries
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().starts_with("renderD"))
    })
}

/// Elements registered, then a dry run.
fn verify(backend: EncoderBackend) -> Result<(), String> {
    let missing: Vec<&str> = backend
        .elements()
        .iter()
        .copied()
        .filter(|name| gst::ElementFactory::find(name).is_none())
        .collect();
    if !missing.is_empty() {
        return Err(missing_reason(backend.plugin(), &missing));
    }
    dry_run(backend)
}

/// Why `missing` elements are absent. A hardware plugin registers its
/// encoders only for the devices it can open, so a loaded plugin without
/// them points at device access or the driver, not at a missing package.
fn missing_reason(plugin: &str, missing: &[&str]) -> String {
    let missing = missing.join(", ");
    if gst::Registry::get().find_plugin(plugin).is_some() {
        format!("the `{plugin}` plugin registered no {missing} (no usable device)")
    } else {
        format!("missing GStreamer element(s): {missing}")
    }
}

/// Encode a few test frames through the backend's segment: proves the
/// device behind the plugin (a render node, a V4L2 device, a GPU) is
/// reachable, which element registration alone does not.
fn dry_run(backend: EncoderBackend) -> Result<(), String> {
    let launch = format!(
        "videotestsrc num-buffers={DRY_RUN_FRAMES} \
         ! video/x-raw,format=I420,width=320,height=240,framerate=15/1 \
         ! videoconvert ! {} ! fakesink sync=false",
        backend.segment(DEFAULT_VIDEO_BITRATE_KBPS)
    );
    let pipeline = gst::parse::launch(&launch).map_err(|e| format!("launch: {e}"))?;
    let bus = pipeline
        .bus()
        .ok_or_else(|| "pipeline without a bus".to_string())?;
    let outcome = match pipeline.set_state(gst::State::Playing) {
        Ok(_) => wait_for_eos(&bus),
        Err(e) => Err(format!("cannot start: {e}")),
    };
    if let Err(e) = pipeline.set_state(gst::State::Null) {
        warn!(encoder = backend.label(), error = %e, "dry-run pipeline did not stop cleanly");
    }
    outcome
}

fn wait_for_eos(bus: &gst::Bus) -> Result<(), String> {
    let msg = bus
        .timed_pop_filtered(
            gst::ClockTime::from_mseconds(
                u64::try_from(DRY_RUN_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
            ),
            &[gst::MessageType::Eos, gst::MessageType::Error],
        )
        .ok_or_else(|| format!("no output within {DRY_RUN_TIMEOUT:?}"))?;
    match msg.view() {
        gst::MessageView::Eos(_) => Ok(()),
        gst::MessageView::Error(e) => Err(format!("{} ({:?})", e.error(), e.debug())),
        _ => Err("unexpected bus message".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPLICIT: [VideoEncoder; 5] = [
        VideoEncoder::X264,
        VideoEncoder::Va,
        VideoEncoder::Vaapi,
        VideoEncoder::V4l2,
        VideoEncoder::Nvenc,
    ];

    #[test]
    fn auto_order_ends_with_software_and_covers_every_explicit_candidate() {
        assert_eq!(
            EncoderBackend::candidates(VideoEncoder::Auto),
            &EncoderBackend::AUTO_ORDER
        );
        assert_eq!(
            EncoderBackend::AUTO_ORDER.last(),
            Some(&EncoderBackend::X264)
        );
        for config in EXPLICIT {
            for backend in EncoderBackend::candidates(config) {
                assert!(EncoderBackend::AUTO_ORDER.contains(backend), "{backend:?}");
            }
        }
    }

    /// An explicit name only ever resolves to a backend of that name.
    #[test]
    fn explicit_candidates_all_carry_the_configured_name() {
        let names = ["x264", "va", "vaapi", "v4l2", "nvenc"];
        for (config, name) in EXPLICIT.into_iter().zip(names) {
            let candidates = EncoderBackend::candidates(config);
            assert_ne!(candidates.len(), 0);
            for backend in candidates {
                assert_eq!(backend.label(), name, "{backend:?}");
            }
        }
    }

    /// `va` takes the full encoder first, then the low-power one with
    /// bitrate control, then constant quality.
    #[test]
    fn va_tries_the_full_encoder_then_low_power_cbr_then_cqp() {
        assert_eq!(
            EncoderBackend::candidates(VideoEncoder::Va),
            &[
                EncoderBackend::Va,
                EncoderBackend::VaLowPower(RateControl::Cbr),
                EncoderBackend::VaLowPower(RateControl::Cqp),
            ]
        );
    }

    #[test]
    fn low_power_segments_set_their_rate_control_and_no_b_frames() {
        let cbr = EncoderBackend::VaLowPower(RateControl::Cbr).segment(2048);
        assert!(
            cbr.contains("vah264lpenc name=video_enc rate-control=cbr bitrate=2048 "),
            "{cbr}"
        );
        let cqp = EncoderBackend::VaLowPower(RateControl::Cqp).segment(2048);
        assert!(cqp.contains("rate-control=cqp qpi=26 qpp=26 "), "{cqp}");
        for segment in [cbr, cqp] {
            assert!(
                segment.starts_with("videoconvert ! video/x-raw,format=NV12 "),
                "{segment}"
            );
            assert!(segment.contains(" b-frames=0 "), "{segment}");
            assert!(!segment.contains("vapostproc"), "{segment}");
        }
    }

    /// The configured bitrate reaches every backend that has one.
    #[test]
    fn every_bitrate_backend_encodes_at_the_bitrate_given() {
        for (backend, expected) in [
            (EncoderBackend::X264, "bitrate=1500 "),
            (EncoderBackend::Va, "bitrate=1500 "),
            (
                EncoderBackend::VaLowPower(RateControl::Cbr),
                "bitrate=1500 ",
            ),
            (EncoderBackend::Vaapi, "bitrate=1500 "),
            (EncoderBackend::Nvenc, "bitrate=1500 "),
            (EncoderBackend::V4l2, "video_bitrate=1500000,"),
        ] {
            let segment = backend.segment(1500);
            assert!(segment.contains(expected), "{backend:?}: {segment}");
        }
        let cqp = EncoderBackend::VaLowPower(RateControl::Cqp).segment(1500);
        assert!(
            !cqp.contains("bitrate="),
            "constant quality has none: {cqp}"
        );
    }

    #[test]
    fn the_plan_gives_each_camera_its_override_or_the_default() {
        let default = VideoEncoding::at_default_bitrate(EncoderBackend::X264);
        let door = VideoEncoding {
            backend: EncoderBackend::VaLowPower(RateControl::Cqp),
            bitrate_kbps: 1000,
        };
        let plan = EncoderPlan::uniform(default).with_camera(CameraId::new("DOOR"), door);

        assert_eq!(plan.for_camera(&CameraId::new("DOOR")), door);
        assert_eq!(plan.for_camera(&CameraId::new("YARD")), default);
        assert_eq!(default.bitrate_kbps, DEFAULT_VIDEO_BITRATE_KBPS);
        assert_eq!(door.segment(), door.backend.segment(1000));
    }

    #[test]
    fn describe_names_the_element_and_the_low_power_rate_control() {
        assert_eq!(EncoderBackend::X264.describe(), "x264enc");
        assert_eq!(EncoderBackend::Va.describe(), "vah264enc");
        assert_eq!(
            EncoderBackend::VaLowPower(RateControl::Cqp).describe(),
            "vah264lpenc rate-control=cqp"
        );
        assert_eq!(
            EncoderBackend::VaLowPower(RateControl::Cbr).describe(),
            "vah264lpenc rate-control=cbr"
        );
    }

    #[test]
    fn has_render_node_finds_a_render_node_only() {
        let dir = std::env::temp_dir().join(format!("encoder-dri-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("card0"), b"").unwrap();
        assert!(!has_render_node(&dir), "a card node is not a render node");
        std::fs::write(dir.join("renderD128"), b"").unwrap();
        assert!(has_render_node(&dir));
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(!has_render_node(&dir), "a missing directory has none");
    }

    /// A loaded plugin without the element points at the device, not at a
    /// missing package.
    #[test]
    fn missing_reason_tells_a_deviceless_plugin_from_a_missing_one() {
        gst::init().unwrap();
        let installed = missing_reason("coreelements", &["no-such-enc"]);
        assert!(
            installed == "the `coreelements` plugin registered no no-such-enc (no usable device)",
            "{installed}"
        );
        let absent = missing_reason("no-such-plugin", &["a", "b"]);
        assert_eq!(absent, "missing GStreamer element(s): a, b");
    }

    #[test]
    fn every_segment_names_the_encoder_and_ends_in_the_shared_caps() {
        for backend in EncoderBackend::AUTO_ORDER {
            let segment = backend.segment(2048);
            assert!(
                segment.contains(&format!("name={UNIFIED_ENCODER_NAME}")),
                "{backend}: {segment}"
            );
            assert!(
                segment.ends_with("video/x-h264,stream-format=byte-stream,alignment=au"),
                "{backend}: {segment}"
            );
            let encoder = backend.elements().last().expect("an encoder element");
            assert!(segment.contains(encoder), "{backend}: {segment}");
        }
        assert!(
            EncoderBackend::V4l2
                .segment(2048)
                .contains("video_bitrate=2048000")
        );
        assert!(EncoderBackend::Nvenc.segment(2048).contains("gop-size="));
    }

    #[test]
    fn labels_round_trip_the_configuration_names() {
        for (backend, label) in [
            (EncoderBackend::X264, "x264"),
            (EncoderBackend::Va, "va"),
            (EncoderBackend::VaLowPower(RateControl::Cbr), "va"),
            (EncoderBackend::VaLowPower(RateControl::Cqp), "va"),
            (EncoderBackend::Vaapi, "vaapi"),
            (EncoderBackend::V4l2, "v4l2"),
            (EncoderBackend::Nvenc, "nvenc"),
        ] {
            assert_eq!(backend.label(), label);
            assert_eq!(backend.to_string(), label);
        }
    }
}
