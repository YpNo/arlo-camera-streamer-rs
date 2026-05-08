//! Application-layer use cases for the Arlo camera streamer.
//!
//! This crate is *infrastructure-free*: it depends only on
//! `streamer-domain` traits and is composed against concrete adapters
//! by `streamer-bin`.
//!
//! Module map:
//!
//! - [`mod@transition`] (Phase 1) — pure [`CameraState`] reducer
//! - [`debouncer`] (Phase 1) — monotonic per-camera motion / max-live timer
//! - [`budget`] (Phase 1) — wall-clock per-camera daily live-quota tracker
//! - [`orchestrator`] (Phase 3) — per-camera tokio task driving the
//!   state machine + ports
//! - [`router`] (Phase 3) — fans the shared SSE event stream to per-camera
//!   mailboxes
//! - [`system`] (Phase 3) — composition root: spawns N orchestrators
//!   plus the router, exposes graceful shutdown
//!
//! [`MediaMultiplexer`]: streamer_domain::port::MediaMultiplexer
//! [`CameraEvent`]: streamer_domain::event::CameraEvent
//! [`CooldownConfig`]: streamer_domain::config::CooldownConfig
//! [`CameraState`]: streamer_domain::state::CameraState

#![forbid(unsafe_code)]

pub mod budget;
pub mod debouncer;
pub mod orchestrator;
pub mod router;
pub mod system;
pub mod transition;

pub use budget::{BudgetVerdict, LiveBudgetTracker};
pub use debouncer::{DebouncerVerdict, MotionDebouncer};
pub use orchestrator::CameraOrchestrator;
pub use router::EventRouter;
pub use system::{MAILBOX_CAPACITY, StreamerSystem};
pub use transition::transition;
