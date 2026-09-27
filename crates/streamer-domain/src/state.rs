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
    /// WebRTC offer sent; awaiting the gateway answer and the first IDR frame.
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
    /// `MediaMultiplexer::attach_live` succeeded (offer/answer
    /// negotiated via `WebrtcSignaler` and the live pad switched in).
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
    /// The media adapter reported the attached live source dead (see
    /// [`LiveSession`](crate::stream::LiveSession)). Only meaningful in
    /// `Live` / `Cooling`; ignored everywhere else.
    LiveLost(LiveLossReason),
}

/// Why an attached live source stopped delivering usable media.
///
/// Fieldless and `Copy` so it doubles as a bounded metrics label: the
/// transition metric's `signal` value is
/// [`signal_label`](Self::signal_label), one per variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LiveLossReason {
    /// No inbound video RTP for `webrtc.live_stall_timeout_secs` after
    /// the first packet — the gateway ended the call or the camera slept.
    RtpStalled,
    /// The ingestion pipeline posted an `ERROR` on its bus.
    PipelineError,
    /// The ingestion pipeline reached end-of-stream.
    EndOfStream,
    /// The peer or ICE connection reached a terminal state
    /// (`failed` / `closed`).
    PeerDisconnected,
    /// The adapter dropped its notifier without reporting. Treated as a
    /// loss so the camera never stays `Live` on a vanished session.
    AdapterDropped,
}

impl LiveLossReason {
    /// Stable kebab-case label for logs and metric label values.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::RtpStalled => "rtp-stalled",
            Self::PipelineError => "pipeline-error",
            Self::EndOfStream => "end-of-stream",
            Self::PeerDisconnected => "peer-disconnected",
            Self::AdapterDropped => "adapter-dropped",
        }
    }

    /// The `signal` label recorded on the state-transition metric for a
    /// [`StateTransition::LiveLost`] carrying this reason
    /// (`live-lost-<reason>`), so the reason is queryable without a
    /// second counter.
    #[must_use]
    pub const fn signal_label(self) -> &'static str {
        match self {
            Self::RtpStalled => "live-lost-rtp-stalled",
            Self::PipelineError => "live-lost-pipeline-error",
            Self::EndOfStream => "live-lost-end-of-stream",
            Self::PeerDisconnected => "live-lost-peer-disconnected",
            Self::AdapterDropped => "live-lost-adapter-dropped",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [LiveLossReason; 5] = [
        LiveLossReason::RtpStalled,
        LiveLossReason::PipelineError,
        LiveLossReason::EndOfStream,
        LiveLossReason::PeerDisconnected,
        LiveLossReason::AdapterDropped,
    ];

    #[test]
    fn live_loss_reason_labels_are_stable_and_kebab_case() {
        assert_eq!(LiveLossReason::RtpStalled.as_label(), "rtp-stalled");
        assert_eq!(LiveLossReason::PipelineError.as_label(), "pipeline-error");
        assert_eq!(LiveLossReason::EndOfStream.as_label(), "end-of-stream");
        assert_eq!(
            LiveLossReason::PeerDisconnected.as_label(),
            "peer-disconnected"
        );
        assert_eq!(LiveLossReason::AdapterDropped.as_label(), "adapter-dropped");
    }

    #[test]
    fn live_loss_reason_signal_labels_prefix_the_reason_and_are_distinct() {
        let mut seen = std::collections::HashSet::new();
        for r in ALL {
            let label = r.signal_label();
            assert_eq!(label, format!("live-lost-{}", r.as_label()));
            assert!(seen.insert(label), "duplicate signal label {label}");
        }
    }
}
