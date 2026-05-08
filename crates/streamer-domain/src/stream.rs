//! Live-stream source descriptors.
//!
//! The [`StreamSource`] is what `ArloStreamRequester::request_live`
//! returns to the orchestrator — a URL plus an optional codec hint.
//! The hint short-circuits the GStreamer probe on subsequent
//! activations once the codec is known.

use serde::{Deserialize, Serialize};

/// Video codec emitted by an Arlo camera.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    /// H.264 / AVC (most older Arlo models).
    H264,
    /// H.265 / HEVC (newer Arlo Pro / Ultra models).
    H265,
}

/// A live stream the media adapter must attach.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSource {
    /// Typically an `rtsps://…` URL produced by `ArloClient::start_stream`.
    pub url: String,
    /// Codec hint learned from a prior activation, if any. `None` triggers
    /// auto-detection on first attach.
    pub codec_hint: Option<Codec>,
}
