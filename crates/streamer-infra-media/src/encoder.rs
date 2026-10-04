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

use std::fmt;
use std::time::Duration;

use gstreamer as gst;
use gstreamer::prelude::*;
use tracing::{debug, info, warn};

use streamer_domain::config::VideoEncoder;

use crate::error::MediaError;
use crate::pipeline_desc::{UNIFIED_ENCODER_NAME, UNIFIED_GOP, VIDEO_BITRATE_KBPS};

/// How long a backend's dry run may take to reach end-of-stream.
const DRY_RUN_TIMEOUT: Duration = Duration::from_secs(3);
/// Frames pushed through the dry run; enough for an encoder to emit.
const DRY_RUN_FRAMES: u32 = 10;

/// A concrete H.264 encoder the launch string is built from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncoderBackend {
    /// Software `x264enc`; works everywhere, costs about 0.6 core per camera.
    X264,
    /// Intel/AMD through the `va` plugin (`vah264enc`, GStreamer ≥ 1.22).
    Va,
    /// Intel/AMD through the older `vaapi` plugin (`vaapih264enc`).
    Vaapi,
    /// V4L2 memory-to-memory (`v4l2h264enc`): Raspberry Pi 4 family and similar boards.
    V4l2,
    /// NVIDIA (`nvh264enc`).
    Nvenc,
}

impl EncoderBackend {
    /// The order `auto` tries: dedicated hardware first, software last.
    pub const AUTO_ORDER: [Self; 5] = [Self::Nvenc, Self::Va, Self::Vaapi, Self::V4l2, Self::X264];

    /// The backend an explicit configuration names; `None` for `auto`.
    #[must_use]
    pub const fn from_config(config: VideoEncoder) -> Option<Self> {
        match config {
            VideoEncoder::Auto => None,
            VideoEncoder::X264 => Some(Self::X264),
            VideoEncoder::Va => Some(Self::Va),
            VideoEncoder::Vaapi => Some(Self::Vaapi),
            VideoEncoder::V4l2 => Some(Self::V4l2),
            VideoEncoder::Nvenc => Some(Self::Nvenc),
        }
    }

    /// The configuration name (`x264`, `va`, …), for logs.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::X264 => "x264",
            Self::Va => "va",
            Self::Vaapi => "vaapi",
            Self::V4l2 => "v4l2",
            Self::Nvenc => "nvenc",
        }
    }

    /// The GStreamer elements the backend's segment needs.
    #[must_use]
    pub const fn elements(self) -> &'static [&'static str] {
        match self {
            Self::X264 => &["x264enc"],
            Self::Va => &["vapostproc", "vah264enc"],
            Self::Vaapi => &["vaapipostproc", "vaapih264enc"],
            Self::V4l2 => &["videoconvert", "v4l2h264enc"],
            Self::Nvenc => &["videoconvert", "nvh264enc"],
        }
    }

    /// The encoder segment of the unified launch string. Every variant
    /// names its encoder `video_enc` (so the registry can force-key-unit
    /// it at each splice) and ends in the same
    /// `byte-stream,alignment=au` caps, so the downstream
    /// `h264parse ! rtph264pay` is identical.
    #[must_use]
    pub fn segment(self) -> String {
        let name = UNIFIED_ENCODER_NAME;
        let gop = UNIFIED_GOP;
        let kbps = VIDEO_BITRATE_KBPS;
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

impl fmt::Display for EncoderBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Turn the configured encoder into a working backend. `auto` takes the
/// first of [`EncoderBackend::AUTO_ORDER`] whose elements are registered
/// and whose dry run succeeds; an explicit choice must pass the same
/// checks. Call after `gstreamer::init()`.
///
/// # Errors
///
/// [`MediaError::Pipeline`] when the explicit backend is unavailable or
/// fails its dry run, or when no backend works at all.
pub fn resolve(config: VideoEncoder) -> Result<EncoderBackend, MediaError> {
    if let Some(explicit) = EncoderBackend::from_config(config) {
        verify(explicit).map_err(|why| {
            MediaError::Pipeline(format!(
                "video encoder '{explicit}' is not usable on this host: {why}"
            ))
        })?;
        info!(encoder = explicit.label(), "video encoder");
        return Ok(explicit);
    }
    for candidate in EncoderBackend::AUTO_ORDER {
        match verify(candidate) {
            Ok(()) => {
                info!(encoder = candidate.label(), "video encoder (auto)");
                return Ok(candidate);
            }
            Err(why) => debug!(encoder = candidate.label(), %why, "encoder skipped"),
        }
    }
    Err(MediaError::Pipeline(
        "no H.264 encoder works on this host (not even x264enc)".into(),
    ))
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
        return Err(format!(
            "missing GStreamer element(s): {}",
            missing.join(", ")
        ));
    }
    dry_run(backend)
}

/// Encode a few test frames through the backend's segment: proves the
/// device behind the plugin (a render node, a V4L2 device, a GPU) is
/// reachable, which element registration alone does not.
fn dry_run(backend: EncoderBackend) -> Result<(), String> {
    let launch = format!(
        "videotestsrc num-buffers={DRY_RUN_FRAMES} \
         ! video/x-raw,format=I420,width=320,height=240,framerate=15/1 \
         ! videoconvert ! {} ! fakesink sync=false",
        backend.segment()
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

    #[test]
    fn auto_order_ends_with_software_and_covers_every_backend() {
        assert_eq!(
            EncoderBackend::AUTO_ORDER.last(),
            Some(&EncoderBackend::X264)
        );
        for config in [
            VideoEncoder::X264,
            VideoEncoder::Va,
            VideoEncoder::Vaapi,
            VideoEncoder::V4l2,
            VideoEncoder::Nvenc,
        ] {
            let backend = EncoderBackend::from_config(config).expect("explicit");
            assert!(EncoderBackend::AUTO_ORDER.contains(&backend), "{backend}");
        }
        assert_eq!(EncoderBackend::from_config(VideoEncoder::Auto), None);
    }

    #[test]
    fn every_segment_names_the_encoder_and_ends_in_the_shared_caps() {
        for backend in EncoderBackend::AUTO_ORDER {
            let segment = backend.segment();
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
                .segment()
                .contains("video_bitrate=2048000")
        );
        assert!(EncoderBackend::Nvenc.segment().contains("gop-size="));
    }

    #[test]
    fn labels_round_trip_the_configuration_names() {
        for (backend, label) in [
            (EncoderBackend::X264, "x264"),
            (EncoderBackend::Va, "va"),
            (EncoderBackend::Vaapi, "vaapi"),
            (EncoderBackend::V4l2, "v4l2"),
            (EncoderBackend::Nvenc, "nvenc"),
        ] {
            assert_eq!(backend.label(), label);
            assert_eq!(backend.to_string(), label);
        }
    }
}
