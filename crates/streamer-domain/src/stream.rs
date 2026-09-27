//! Live-stream source descriptors.
//!
//! [`SignalingAnswer`] is the result of a WebRTC offer/answer exchange
//! with the Arlo gateway, returned by
//! [`WebrtcSignaler::negotiate`](crate::port::WebrtcSignaler::negotiate).
//! The media adapter (GStreamer `webrtcbin`) generates the offer, hands
//! it to the signaler, and applies the returned answer SDP.
//!
//! [`Codec`] names the video codec a camera produces; the per-camera
//! `codec_hint` in the configuration lets the pipeline skip the
//! first-stream detection. [`IceAddressFamily`] is the ICE gathering
//! policy applied to every `webrtcbin`.

use serde::{Deserialize, Serialize};

/// One ICE (STUN/TURN) server the media adapter must configure on its
/// WebRTC peer **before** generating the offer. Sourced from Arlo's
/// `sipInfo` via [`WebrtcSignaler::ice_servers`](crate::port::WebrtcSignaler::ice_servers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IceServer {
    /// e.g. `stun:host:port` or `turn:host:port?transport=udp`.
    pub url: String,
    /// TURN long-term username (`None` for STUN).
    pub username: Option<String>,
    /// TURN long-term credential (`None` for STUN).
    pub credential: Option<String>,
}

/// The Arlo gateway's WebRTC SDP answer to our offer, plus the session
/// id it echoed back (needed for teardown / `sessionDisconnected`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalingAnswer {
    /// `FreeSWITCH`'s SDP answer, applied verbatim by the media adapter.
    pub answer_sdp: String,
    /// Opaque session id the gateway assigned to this live call.
    pub session_id: String,
}

/// Video codec emitted by an Arlo camera.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    /// H.264 / AVC (most older Arlo models).
    H264,
    /// H.265 / HEVC (newer Arlo Pro / Ultra models).
    H265,
}

/// Address-family policy the media adapter applies when configuring the
/// WebRTC ICE agent's local candidate gathering.
///
/// The default is [`Dual`](Self::Dual) — let libnice gather both IPv4
/// and IPv6 candidates. Some networks (broken IPv6 routing to the Arlo
/// gateway, restrictive corporate firewalls) benefit from
/// [`Ipv4`](Self::Ipv4), which restricts gathering to IPv4 only.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IceAddressFamily {
    /// Gather both IPv4 and IPv6 candidates (libnice default).
    #[default]
    Dual,
    /// Gather only IPv4 candidates. Recommended when IPv6 to the Arlo
    /// gateway is broken or slow to fail over.
    Ipv4,
}
