//! Shared fixtures for the GStreamer integration tests.

pub mod gateway;
pub mod probe;

use std::sync::Arc;

use gstreamer as gst;

use streamer_domain::camera::{CameraId, StreamName};
use streamer_domain::config::{
    CameraConfig, CooldownConfig, OutputConfig, RtspOutput, VideoEncoder, WebrtcConfig,
};
use streamer_infra_media::{GstMediaMultiplexer, GstPipelineRegistry, RtspServer};

/// Set to `1` where the plugins are guaranteed (CI): a missing element
/// then fails the test instead of skipping it.
const REQUIRE_ENV: &str = "STREAMER_REQUIRE_GST_IT";

/// Every element the idle pipeline, the WebRTC leg, the fake gateway
/// and the probe instantiate.
const ELEMENTS: &[&str] = &[
    "webrtcbin",
    "nicesrc",
    "dtlssrtpenc",
    "srtpdec",
    "x264enc",
    "avdec_h264",
    "opusenc",
    "opusdec",
    "avenc_aac",
    "audiomixer",
    "input-selector",
    "textoverlay",
    "gdkpixbufoverlay",
    "rtspsrc",
    "valve",
];

pub const CAMERA: &str = "IT_CAM";
pub const STREAM: &str = "it_cam";

/// Initialise GStreamer and report whether every element the tests
/// need is installed. Skips (returns `false`) when one is missing,
/// unless [`REQUIRE_ENV`] asks for a hard failure.
pub fn gstreamer_ready() -> bool {
    gst::init().expect("gst::init");
    let missing: Vec<&str> = ELEMENTS
        .iter()
        .copied()
        .filter(|e| gst::ElementFactory::find(e).is_none())
        .collect();
    if missing.is_empty() {
        return true;
    }
    assert!(
        std::env::var(REQUIRE_ENV).as_deref() != Ok("1"),
        "GStreamer elements missing: {missing:?}"
    );
    eprintln!("skipping: GStreamer elements missing: {missing:?}");
    false
}

/// The production media stack for one camera, on an ephemeral RTSP port.
pub struct Stack {
    pub media: GstMediaMultiplexer<GstPipelineRegistry>,
    pub camera: CameraId,
    server: Arc<RtspServer>,
}

impl Stack {
    pub fn new(webrtc: WebrtcConfig) -> Self {
        let server = RtspServer::start("127.0.0.1:0").expect("rtsp server");
        let registry = Arc::new(GstPipelineRegistry::new(server.clone(), VideoEncoder::X264));
        let camera = CameraId::new(CAMERA);
        let cameras = [CameraConfig {
            arlo_device_id: camera.clone(),
            stream_name: StreamName::parse(STREAM).expect("stream name"),
            codec_hint: None,
            cooldown: CooldownConfig::default(),
        }];
        let output = OutputConfig {
            rtsp: RtspOutput {
                bind: "127.0.0.1:0".to_string(),
            },
            hls: None,
            dash: None,
            video_encoder: VideoEncoder::X264,
            metrics_bind: "127.0.0.1:0".to_string(),
            admin_bind: "127.0.0.1:0".to_string(),
        };
        let media = GstMediaMultiplexer::new(registry, output, webrtc, &cameras);
        Self {
            media,
            camera,
            server,
        }
    }

    /// The camera's RTSP URL on the bound port.
    pub fn url(&self) -> String {
        let port = self.server.bound_port().expect("rtsp server bound");
        format!("rtsp://127.0.0.1:{port}/{STREAM}")
    }
}

/// The WebRTC settings with the stall timeout at its floor, so the stall
/// test finishes quickly.
pub fn fast_stall() -> WebrtcConfig {
    WebrtcConfig {
        live_stall_timeout_secs: 4,
        ..WebrtcConfig::default()
    }
}
