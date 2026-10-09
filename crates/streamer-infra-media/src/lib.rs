//! GStreamer-based [`MediaMultiplexer`] adapter.
//!
//! Each registered camera owns one entry in the embedded
//! `gst-rtsp-server`'s mount-point table. The factory at
//! `/<stream_name>` is bound to **one persistent** launch string
//! ([`pipeline_desc::combined_launch_string`]) that hosts both the idle
//! and live sources behind an `input-selector` (video) and an
//! `audiomixer` (audio):
//!
//! ```text
//!  idle video: synthetic STANDBY + thumbnail → raw I420 ─┐
//!  live video: appsrc(H.264 RTP) → decode → raw I420  ───┤→ sel → x264enc → pay0
//!  idle audio: audiotestsrc silence ─┐
//!  live audio: appsrc(Opus RTP) → decode ─┤→ audiomixer → avenc_aac → pay1
//! ```
//!
//! Idle↔live transitions flip the selector's `active-pad` on the first
//! decoded live frame (with a force-key-unit for a clean IDR); audio is
//! simply mixed onto the silent bed. Because it's **one persistent
//! pipeline**, connected RTSP clients (VLC *and* Frigate) see a
//! continuous stream with no reconnect. The live H.264 + Opus RTP is
//! delivered by the per-camera [`webrtc_pipeline`] leg. See
//! `docs/adr/0003-seamless-input-selector-splice.md`.
//!
//! ## Crate layout
//!
//! | Module          | Pure | Notes                                              |
//! |-----------------|:----:|----------------------------------------------------|
//! | [`error`]       |  ✓   | [`MediaError`] + mapping to [`DomainError`].       |
//! | [`elements`]    |      | The GStreamer elements checked at boot.            |
//! | [`idle_source`] |  ✓   | JPEG-still vs synthetic selection.                 |
//! | [`pipeline_desc`]|  ✓  | `gst-launch` description builders.                 |
//! | [`splice`]      |  ✓   | Keyframe / IDR detection helpers.                  |
//! | [`codec_cache`] |  ✓   | Per-camera codec-hint cache.                       |
//! | [`multiplexer`] |  ✓   | [`PipelineRegistry`] trait + [`GstMediaMultiplexer`].|
//! | [`rtsp`]        |      | Wraps `gst-rtsp-server` (integration-tested).      |
//! | [`gst_pipeline`]|      | Production [`PipelineRegistry`] (integration-tested).|
//! | [`live_rtp_sink`]|  ✓  | Per-camera RTP byte sink (`appsink` → consumer).   |
//! | [`live_watch`]  |  ✓   | Live-loss detector logic: RTP activity clock + stall rule (ADR 0004). |
//! | `rtsp_relay`    |  ✓*  | RTSP client relaying the user's app view into the live sinks (ADR 0007); pure parts unit-tested, the connection integration-tested. |
//! | [`webrtc_pipeline`]| | Per-camera `webrtcbin` live leg (integration-tested).|
//!
//! [`MediaMultiplexer`]: streamer_domain::port::MediaMultiplexer
//! [`DomainError`]: streamer_domain::error::DomainError

#![forbid(unsafe_code)]

pub mod codec_cache;
pub mod elements;
pub mod encoder;
pub mod error;
pub mod gst_pipeline;
mod hls;
pub mod idle_source;
pub mod live_rtp_sink;
pub mod live_watch;
pub mod multiplexer;
pub mod pipeline_desc;
pub mod rtsp;
mod rtsp_relay;
pub mod splice;
pub mod webrtc_pipeline;

pub use codec_cache::CodecCache;
pub use error::MediaError;
pub use gst_pipeline::{GstPipelineRegistry, prepare_thumbnail_dir};
pub use idle_source::{IDLE_FPS, IdleKind, SYNTHETIC_HEIGHT, SYNTHETIC_WIDTH, select_idle_source};
pub use live_rtp_sink::{
    AacFeed, AacRtpFormat, LiveAacSink, LiveRtpSink, LiveSinkReceivers, LiveSinks,
};
pub use live_watch::{RtpActivity, stall_verdict};
pub use multiplexer::{GstMediaMultiplexer, PipelineRegistry};
pub use pipeline_desc::{
    DashBranchConfig, HlsBranchConfig, OutputBranches, build_output_branches, rtsp_mount_path,
};
pub use rtsp::RtspServer;
pub use rtsp_relay::RelayTls;
pub use splice::{KeyframeWatcher, is_keyframe};
