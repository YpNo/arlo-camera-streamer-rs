//! Per-camera state machine descriptors.
//!
//! The state machine itself (transition function, debounce timer,
//! budget tracker) lives in `streamer-app`. This module declares only
//! the shape of states and transition signals so they can be referenced
//! from both layers and serialized for diagnostics.

use std::time::Duration;

/// Lifecycle state of a single camera as managed by the orchestrator.
///
/// Made non-exhaustive to allow future variants without breaking
/// downstream pattern-matching that should always have a default arm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CameraState {
    /// No upstream stream; idle source serves the output endpoints.
    Idle,
    /// Stream URL requested; awaiting SSE response and first IDR frame.
    Activating,
    /// Live source attached; selector is on the live pad.
    Live {
        /// Wall-clock seconds spent in `Live` for the current session.
        ///
        /// Compared against `CooldownConfig::max_continuous_live` to enforce
        /// the battery-protection cap.
        since_secs: u64,
    },
    /// Motion ended; holding live for `remaining` before reverting to idle.
    Cooling {
        /// Time left before the cooldown expires.
        ///
        /// Reset to the configured `debounce_secs` when a fresh motion event
        /// arrives within the cooldown window.
        remaining: Duration,
    },
    /// Daily live budget exhausted; motion is ignored until reset.
    BatteryProtect {
        /// Time until the daily budget resets and the camera returns to idle.
        reset_in: Duration,
    },
    /// Transient adapter failure; backing off before another attempt.
    Failed {
        /// Human-readable description of the failure for diagnostics.
        reason: String,
        /// Number of consecutive retries so far; informs exponential backoff.
        retries: u32,
    },
}

/// Signals that drive [`CameraState`] transitions. Emitted by adapters
/// (motion arriving, IDR seen) or by internal timers (cooldown elapsed,
/// budget exhausted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateTransition {
    /// A motion or audio trigger arrived from the event bus.
    MotionDetected,
    /// `ArloStreamRequester::request_live` returned a URL and
    /// `MediaMultiplexer::attach_live` accepted it.
    LiveAttached,
    /// First IDR frame observed on the live pad — the splice is safe.
    LiveReady,
    /// Cooldown timer expired without a re-trigger.
    CooldownExpired,
    /// `max_continuous_live` cap reached; force back to idle.
    MaxLiveExceeded,
    /// Daily live budget exhausted.
    BudgetExhausted,
    /// Daily budget reset (new day).
    BudgetReset,
    /// Adapter raised a transient failure.
    Failure(String),
    /// Backoff window elapsed; orchestrator may retry from `Idle`.
    BackoffElapsed,
}
