//! No-op [`MetricsRecorder`] used as a default in tests and as a
//! fallback when the binary is built without observability.
//!
//! Keeps the orchestrator constructor signature simple (the caller can
//! always pass `Arc::new(NoopRecorder)` instead of `None`).

use streamer_domain::camera::CameraId;
use streamer_domain::metrics::{BudgetDecision, MotionOutcome, SpliceOutcome};
use streamer_domain::port::MetricsRecorder;
use streamer_domain::state::CameraState;

/// Recorder that swallows every event. Zero overhead.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopRecorder;

impl MetricsRecorder for NoopRecorder {
    fn record_state_change(
        &self,
        _camera: &CameraId,
        _from: &CameraState,
        _to: &CameraState,
        _signal: &str,
    ) {
    }

    fn record_motion(&self, _camera: &CameraId, _outcome: MotionOutcome) {}

    fn record_budget(&self, _camera: &CameraId, _decision: BudgetDecision) {}

    fn record_splice(&self, _camera: &CameraId, _outcome: SpliceOutcome, _latency_ms: u64) {}

    fn record_failure(&self, _camera: &CameraId, _retries: u32) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn noop_recorder_is_a_valid_trait_object() {
        let r: Arc<dyn MetricsRecorder> = Arc::new(NoopRecorder);
        r.record_motion(&CameraId::new("X"), MotionOutcome::Triggered);
        // No assertions — the fact that it compiles + runs is the test.
    }
}
