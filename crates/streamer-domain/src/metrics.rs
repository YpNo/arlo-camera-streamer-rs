//! Domain-level instrumentation vocabulary.
//!
//! The application layer emits these enums into a
//! [`MetricsRecorder`](crate::port::MetricsRecorder) implementation
//! provided by the infrastructure layer. Keeping the vocabulary in the
//! domain crate ensures the orchestrator never reaches up into the
//! Prometheus-specific adapter — it only knows about business outcomes.

/// Disposition of a single inbound motion / audio event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MotionOutcome {
    /// Event triggered (or extended) a live session.
    Triggered,
    /// Event was suppressed because the daily budget is exhausted.
    BudgetExhausted,
    /// Event arrived while already live or cooling — debouncer absorbed
    /// it (no state change).
    Absorbed,
    /// Event arrived in a transient failed state — ignored until
    /// backoff elapses.
    SuppressedFailed,
}

impl MotionOutcome {
    /// Stable kebab-case label for Prometheus labels and structured
    /// logs. Keep these short — Prometheus label values are queried
    /// frequently and inflate cardinality.
    #[must_use]
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Triggered => "triggered",
            Self::BudgetExhausted => "budget-exhausted",
            Self::Absorbed => "absorbed",
            Self::SuppressedFailed => "suppressed-failed",
        }
    }
}

/// Decision made by the daily-budget tracker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetDecision {
    /// Budget was checked and live time was granted.
    Granted,
    /// Budget refused: the daily quota is exhausted.
    Denied,
    /// Budget reset boundary crossed; available again.
    Reset,
}

impl BudgetDecision {
    /// Stable kebab-case label.
    #[must_use]
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Granted => "granted",
            Self::Denied => "denied",
            Self::Reset => "reset",
        }
    }
}

/// Outcome of an Idle → Live splice attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpliceOutcome {
    /// Live source attached and selector switched. The camera is
    /// streaming to Frigate.
    Success,
    /// `WebrtcSignaler::negotiate` failed (carried inside `attach_live`).
    RequestFailed,
    /// `MediaMultiplexer::attach_live` failed (e.g. SDP / IDR timeout).
    AttachFailed,
}

impl SpliceOutcome {
    /// Stable kebab-case label.
    #[must_use]
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::RequestFailed => "request-failed",
            Self::AttachFailed => "attach-failed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn motion_labels_are_stable_and_kebab_case() {
        assert_eq!(MotionOutcome::Triggered.as_label(), "triggered");
        assert_eq!(
            MotionOutcome::BudgetExhausted.as_label(),
            "budget-exhausted"
        );
        assert_eq!(MotionOutcome::Absorbed.as_label(), "absorbed");
        assert_eq!(
            MotionOutcome::SuppressedFailed.as_label(),
            "suppressed-failed"
        );
    }

    #[test]
    fn budget_labels_are_stable() {
        assert_eq!(BudgetDecision::Granted.as_label(), "granted");
        assert_eq!(BudgetDecision::Denied.as_label(), "denied");
        assert_eq!(BudgetDecision::Reset.as_label(), "reset");
    }

    #[test]
    fn splice_labels_are_stable() {
        assert_eq!(SpliceOutcome::Success.as_label(), "success");
        assert_eq!(SpliceOutcome::RequestFailed.as_label(), "request-failed");
        assert_eq!(SpliceOutcome::AttachFailed.as_label(), "attach-failed");
    }
}
