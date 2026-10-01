//! Pure domain layer for the Arlo camera streamer.
//!
//! This crate contains only types, state-machine descriptors, and port
//! traits. It has zero I/O, no async runtime spawning, and no
//! infrastructure dependencies. Adapters in `streamer-infra-*` implement
//! the ports defined here against external systems (arlo-rs, GStreamer).
//!
//! # Layering
//!
//! - [`port`] declares the port traits (driven + driving).
//! - [`state`] holds the per-camera state machine descriptors.
//! - [`event`] models the inbound camera-event vocabulary.
//! - [`metrics`] models the application-layer instrumentation
//!   vocabulary (motion outcomes, splice outcomes, budget decisions).
//! - [`admin`] models the operational-control vocabulary (snapshots,
//!   error types) consumed by `/admin/*` HTTP routes.
//! - [`config`] holds TOML-deserializable configuration types.
//! - [`stream`] holds live-stream descriptors and the live-session
//!   handle / notifier pair (ADR 0004).
//! - [`camera`] holds identifier newtypes.
//! - [`error`] declares the domain error hierarchy.

#![forbid(unsafe_code)]

pub mod admin;
pub mod camera;
pub mod config;
pub mod error;
pub mod event;
pub mod metrics;
pub mod port;
pub mod state;
pub mod stream;

pub use admin::{AdminError, CameraSnapshot, SystemSnapshot};
pub use camera::{CameraId, DiscoveredDevice, StreamName};
pub use config::{
    ArloConfig, CameraConfig, CooldownConfig, DEFAULT_LIVE_STALL_TIMEOUT_SECS, DashOutput,
    EmailMfaConfig, HlsOutput, MIN_HLS_PLAYLIST_LENGTH, MIN_HLS_SEGMENT_SECS,
    MIN_LIVE_STALL_TIMEOUT_SECS, MfaConfig, OutputConfig, RtspOutput, StreamerConfig, WebrtcConfig,
};
pub use error::DomainError;
pub use event::{CameraEvent, ConnectionStatus};
pub use metrics::{BudgetDecision, MotionOutcome, SpliceOutcome};
pub use port::{
    AdminControl, ArloEventSource, ArloThumbnailSource, MediaMultiplexer, MetricsRecorder,
    UserViewSource, WebrtcSignaler,
};
pub use state::{CameraState, LiveLossReason, LiveSource, StateTransition};
pub use stream::{
    Codec, IceAddressFamily, IceServer, LiveLossNotifier, LiveSession, SignalingAnswer,
    WatchAlongUrl,
};
