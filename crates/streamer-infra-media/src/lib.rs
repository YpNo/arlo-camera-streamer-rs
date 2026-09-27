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
//! `docs/adr/0003-seamless-input-selector-splice.md`. The legacy
//! factory-restart builders remain in [`pipeline_desc`] but are off the
//! production path.
//!
//! ## Crate layout
//!
//! | Module          | Pure | Notes                                              |
//! |-----------------|:----:|----------------------------------------------------|
//! | [`error`]       |  ✓   | [`MediaError`] + mapping to [`DomainError`].       |
//! | [`idle_source`] |  ✓   | JPEG-still vs synthetic selection.                 |
//! | [`pipeline_desc`]|  ✓  | `gst-launch` description builders.                 |
//! | [`splice`]      |  ✓   | Keyframe / IDR detection helpers.                  |
//! | [`codec_cache`] |  ✓   | Per-camera codec-hint cache.                       |
//! | [`multiplexer`] |  ✓   | [`PipelineRegistry`] trait + [`GstMediaMultiplexer`].|
//! | [`rtsp`]        |      | Wraps `gst-rtsp-server` (excluded from coverage).  |
//! | [`gst_pipeline`]|      | Production [`PipelineRegistry`] (excluded from cov).|
//! | [`live_rtp_sink`]|  ✓  | Per-camera RTP byte sink (`appsink` → consumer).   |
//! | [`live_watch`]  |  ✓   | Live-loss detector logic: RTP activity clock + stall rule (ADR 0004). |
//! | [`webrtc_pipeline`]| | Per-camera `webrtcbin` live leg (excluded from cov).|
//!
//! [`MediaMultiplexer`]: streamer_domain::port::MediaMultiplexer
//! [`DomainError`]: streamer_domain::error::DomainError

#![forbid(unsafe_code)]

pub mod codec_cache;
pub mod error;
pub mod gst_pipeline;
pub mod idle_source;
pub mod live_rtp_sink;
pub mod live_watch;
pub mod multiplexer;
pub mod pipeline_desc;
pub mod rtsp;
pub mod splice;
pub mod webrtc_pipeline;

pub use codec_cache::CodecCache;
pub use error::MediaError;
pub use gst_pipeline::GstPipelineRegistry;
pub use idle_source::{IDLE_FPS, IdleKind, SYNTHETIC_HEIGHT, SYNTHETIC_WIDTH, select_idle_source};
pub use live_rtp_sink::{LiveRtpSink, LiveSinkReceivers, LiveSinks};
pub use live_watch::{RtpActivity, stall_verdict};
pub use multiplexer::{GstMediaMultiplexer, PipelineRegistry};
pub use pipeline_desc::{
    DashBranchConfig, HlsBranchConfig, OutputBranches, build_output_branches, idle_audio_desc,
    idle_launch_string, idle_video_desc, idle_video_jpeg_desc, idle_video_synthetic_desc,
    live_audio_desc, live_launch_string, live_video_desc, rtsp_mount_path,
};
pub use rtsp::RtspServer;
pub use splice::{KeyframeWatcher, is_keyframe};
