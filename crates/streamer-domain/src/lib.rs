//! Pure domain layer for the Arlo camera streamer.
//!
//! This crate contains only types, state-machine descriptors, and port
//! traits. It has zero I/O, no async runtime spawning, and no
//! infrastructure dependencies. Adapters in `streamer-infra-*` implement
//! the ports defined here against external systems (rs-arlo, GStreamer).
//!
//! # Layering
//!
//! - [`port`] declares the *driven ports* (interfaces the application
//!   layer drives, implemented by infrastructure adapters).
//! - [`state`] holds the per-camera state machine descriptors.
//! - [`event`] models the inbound camera-event vocabulary.
//! - [`config`] holds TOML-deserializable configuration types.
//! - [`stream`] holds live-stream descriptors.
//! - [`camera`] holds identifier newtypes.
//! - [`error`] declares the domain error hierarchy.

#![forbid(unsafe_code)]

pub mod camera;
pub mod config;
pub mod error;
pub mod event;
pub mod port;
pub mod state;
pub mod stream;

pub use camera::{CameraId, StreamName};
pub use config::{
    ArloConfig, CameraConfig, CooldownConfig, DashOutput, HlsOutput, ImapMfaConfig, MfaConfig,
    OutputConfig, RtspOutput, StreamerConfig,
};
pub use error::DomainError;
pub use event::{CameraEvent, ConnectionStatus};
pub use port::{ArloEventSource, ArloStreamRequester, ArloThumbnailSource, MediaMultiplexer};
pub use state::{CameraState, StateTransition};
pub use stream::{Codec, StreamSource};
