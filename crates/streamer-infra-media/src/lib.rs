//! GStreamer-based [`MediaMultiplexer`] adapter.
//!
//! Each registered camera owns one entry in the embedded
//! `gst-rtsp-server`'s mount-point table. The factory at
//! `/<stream_name>` is bound to one of two launch strings:
//!
//! ```text
//!  Idle  : ( videotestsrc + textoverlay → x264enc → rtph264pay name=pay0
//!          + audiotestsrc wave=silence  → avenc_aac → rtpmp4apay name=pay1 )
//!
//!  Live  : ( rtspsrc {camera_url} → {parsebin | rtph26{4,5}depay} → rtph26{4,5}pay name=pay0
//!          + rtspsrc {camera_url} → rtpmp4adepay → rtpmp4apay name=pay1 )
//! ```
//!
//! Transitioning between idle and live atomically swaps the factory's
//! launch string. Connected RTSP clients see a brief EOS and reconnect
//! within ~1 s — Frigate handles this transparently. The seamless
//! `input-selector` splice (with IDR-aligned pad probe) is scaffolded
//! by [`splice::KeyframeWatcher`] and remains a Phase 6 polish target.
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
pub mod multiplexer;
pub mod pipeline_desc;
pub mod rtsp;
pub mod splice;
pub mod webrtc_pipeline;

pub use codec_cache::CodecCache;
pub use error::MediaError;
pub use gst_pipeline::GstPipelineRegistry;
pub use idle_source::{IDLE_FPS, IdleKind, SYNTHETIC_HEIGHT, SYNTHETIC_WIDTH, select_idle_source};
pub use live_rtp_sink::LiveRtpSink;
pub use multiplexer::{GstMediaMultiplexer, PipelineRegistry};
pub use pipeline_desc::{
    DashBranchConfig, HlsBranchConfig, OutputBranches, build_output_branches, idle_audio_desc,
    idle_launch_string, idle_video_desc, idle_video_jpeg_desc, idle_video_synthetic_desc,
    live_audio_desc, live_launch_string, live_video_desc, rtsp_mount_path,
};
pub use rtsp::RtspServer;
pub use splice::{KeyframeWatcher, is_keyframe};
