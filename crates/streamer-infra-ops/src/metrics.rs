//! Prometheus metrics types.
//!
//! All metrics are owned by a single [`Metrics`] struct. Cloning a
//! gauge handle is cheap (it's an `Arc` internally) and thread-safe —
//! the connection-status watch task and the HTTP scrape handler share
//! the same handles.
//!
//! Naming follows the
//! [Prometheus conventions](https://prometheus.io/docs/practices/naming/):
//! `<namespace>_<subsystem>_<unit>`. Our namespace is `streamer`.

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use prometheus::{Encoder, IntCounterVec, IntGauge, IntGaugeVec, Registry, TextEncoder, opts};
use streamer_domain::camera::CameraId;
use streamer_domain::metrics::{BudgetDecision, MotionOutcome, SpliceOutcome};
use streamer_domain::port::MetricsRecorder;
use streamer_domain::state::CameraState;
use thiserror::Error;
use tracing::warn;

/// Errors raised by the metrics subsystem.
#[derive(Debug, Error)]
pub enum MetricsError {
    /// Failure constructing or registering a metric. Wraps the
    /// upstream `prometheus::Error`.
    #[error("prometheus error: {0}")]
    Prometheus(#[from] prometheus::Error),

    /// The gathered metrics contained non-UTF-8 bytes (should be
    /// impossible with the text encoder, but surfaced explicitly).
    #[error("metrics encoding produced non-UTF-8")]
    NonUtf8,
}

/// Prometheus metrics handle.
///
/// All gauge / counter handles are cheap to clone (internally an `Arc`)
/// and the registry holds its own clones, so we keep field-level
/// handles for direct mutation rather than re-fetching from the
/// registry by name on every update.
#[derive(Clone)]
#[allow(dead_code)]
pub struct Metrics {
    registry: Arc<Registry>,

    // ---------- System-level (Phase 5) ----------
    /// `1` when the Arlo event bus is connected, `0` otherwise.
    arlo_connected: IntGauge,
    /// Number of cameras declared in `[[cameras]]` (constant).
    cameras_configured: IntGauge,
    /// Process start time as Unix epoch seconds.
    started_at: IntGauge,
    /// Build info — value is always `1`; labels carry version metadata.
    build_info: IntGaugeVec,
    /// Boot instant for uptime computation on each scrape.
    boot_instant: Instant,
    /// Uptime gauge — refreshed on each `render` call.
    uptime_seconds: IntGauge,

    // ---------- Per-camera (Phase 6) ----------
    /// Current state per camera (`{camera, state}` → 1 for active row,
    /// 0 for the others). Cardinality: cameras × states.
    camera_state: IntGaugeVec,
    /// State-transition counter, labeled by `{camera, from, to, signal}`.
    state_transitions_total: IntCounterVec,
    /// Motion / audio events received per camera, labeled by outcome.
    motion_events_total: IntCounterVec,
    /// Daily-budget decisions per camera, labeled by decision.
    budget_decisions_total: IntCounterVec,
    /// Splice attempts per camera, labeled by outcome.
    splice_attempts_total: IntCounterVec,
    /// Splice latency histogram (ms) per camera, labeled by outcome.
    splice_latency_ms: prometheus::HistogramVec,
    /// Per-camera retry counter — incremented each time we enter
    /// `Failed { retries }` (so the rate gives transient-failure rate).
    retries_total: IntCounterVec,
}

/// All possible state labels emitted on `streamer_camera_state`.
/// Centralized so [`Metrics::new`] pre-creates every row and so unit
/// tests can iterate the full set.
const STATE_LABELS: &[&str] = &["idle", "activating", "live", "battery-protect", "failed"];

impl Metrics {
    /// Build a registry pre-populated with the full metric set.
    ///
    /// `cameras_configured` is the count of `[[cameras]]` blocks; it
    /// is set once and never changes. `version` is exposed as a label
    /// on `streamer_build_info`.
    ///
    /// # Errors
    ///
    /// Returns [`MetricsError::Prometheus`] if any gauge fails to
    /// register (duplicate names, invalid characters).
    #[allow(clippy::too_many_lines)] // metric registration is inherently linear
    pub fn new(cameras_configured: u32, version: &str) -> Result<Self, MetricsError> {
        let registry = Arc::new(Registry::new());

        // ---------- System-level ----------
        let arlo_connected = IntGauge::with_opts(opts!(
            "streamer_arlo_connected",
            "1 if connected to the Arlo event bus, 0 otherwise"
        ))?;
        let cameras_configured_g = IntGauge::with_opts(opts!(
            "streamer_cameras_configured",
            "Number of cameras declared in configuration"
        ))?;
        let started_at = IntGauge::with_opts(opts!(
            "streamer_started_at_unix_seconds",
            "Process start time as Unix epoch seconds"
        ))?;
        let uptime_seconds = IntGauge::with_opts(opts!(
            "streamer_uptime_seconds",
            "Process uptime in seconds (refreshed on each scrape)"
        ))?;
        let build_info = IntGaugeVec::new(
            opts!(
                "streamer_build_info",
                "Build metadata. Value is always 1; labels carry version."
            ),
            &["version"],
        )?;

        // ---------- Per-camera ----------
        let camera_state = IntGaugeVec::new(
            opts!(
                "streamer_camera_state",
                "Camera state — 1 on the active row, 0 elsewhere."
            ),
            &["camera", "state"],
        )?;
        let state_transitions_total = IntCounterVec::new(
            opts!(
                "streamer_state_transitions_total",
                "Per-camera state transitions, labeled by from/to/signal."
            ),
            &["camera", "from", "to", "signal"],
        )?;
        let motion_events_total = IntCounterVec::new(
            opts!(
                "streamer_motion_events_total",
                "Inbound motion / audio events, labeled by outcome."
            ),
            &["camera", "outcome"],
        )?;
        let budget_decisions_total = IntCounterVec::new(
            opts!(
                "streamer_budget_decisions_total",
                "Daily-budget decisions per camera."
            ),
            &["camera", "decision"],
        )?;
        let splice_attempts_total = IntCounterVec::new(
            opts!(
                "streamer_splice_attempts_total",
                "Idle→Live splice attempts per camera, labeled by outcome."
            ),
            &["camera", "outcome"],
        )?;
        let splice_latency_ms = prometheus::HistogramVec::new(
            prometheus::HistogramOpts::new(
                "streamer_splice_latency_ms",
                "Idle→Live splice wall-clock latency in milliseconds.",
            )
            .buckets(vec![
                50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0,
            ]),
            &["camera", "outcome"],
        )?;
        let retries_total = IntCounterVec::new(
            opts!(
                "streamer_retries_total",
                "Transient-failure retries per camera (Failed{retries})."
            ),
            &["camera"],
        )?;

        registry.register(Box::new(arlo_connected.clone()))?;
        registry.register(Box::new(cameras_configured_g.clone()))?;
        registry.register(Box::new(started_at.clone()))?;
        registry.register(Box::new(uptime_seconds.clone()))?;
        registry.register(Box::new(build_info.clone()))?;
        registry.register(Box::new(camera_state.clone()))?;
        registry.register(Box::new(state_transitions_total.clone()))?;
        registry.register(Box::new(motion_events_total.clone()))?;
        registry.register(Box::new(budget_decisions_total.clone()))?;
        registry.register(Box::new(splice_attempts_total.clone()))?;
        registry.register(Box::new(splice_latency_ms.clone()))?;
        registry.register(Box::new(retries_total.clone()))?;

        // Set the constants once; these don't change at runtime.
        cameras_configured_g.set(i64::from(cameras_configured));
        started_at.set(now_unix_seconds());
        build_info.with_label_values(&[version]).set(1);

        Ok(Self {
            registry,
            arlo_connected,
            cameras_configured: cameras_configured_g,
            started_at,
            build_info,
            boot_instant: Instant::now(),
            uptime_seconds,
            camera_state,
            state_transitions_total,
            motion_events_total,
            budget_decisions_total,
            splice_attempts_total,
            splice_latency_ms,
            retries_total,
        })
    }

    /// Pre-create the per-camera gauge rows so they appear at scrape
    /// time even before any event has fired. Idempotent.
    pub fn prewarm_camera(&self, camera: &CameraId) {
        for state in STATE_LABELS {
            let _ = self
                .camera_state
                .with_label_values(&[camera.as_str(), state]);
        }
    }

    /// Update the Arlo-bus connectivity gauge.
    pub fn set_arlo_connected(&self, connected: bool) {
        self.arlo_connected.set(i64::from(connected));
    }

    /// Borrow the underlying registry — useful when callers need to
    /// register additional collectors (e.g., the application layer's
    /// future `MetricsRecorder` impl).
    #[must_use]
    pub fn registry(&self) -> Arc<Registry> {
        self.registry.clone()
    }

    /// Render the current metric snapshot as Prometheus text exposition.
    ///
    /// # Errors
    ///
    /// Returns [`MetricsError::Prometheus`] if encoding fails or
    /// [`MetricsError::NonUtf8`] if the encoder produced non-UTF-8
    /// bytes (should be unreachable with `TextEncoder`).
    pub fn render(&self) -> Result<String, MetricsError> {
        // Refresh uptime at scrape time so it's monotonic without a
        // background task.
        let elapsed = self.boot_instant.elapsed().as_secs();
        self.uptime_seconds
            .set(i64::try_from(elapsed).unwrap_or(i64::MAX));

        let encoder = TextEncoder::new();
        let mut buf = Vec::with_capacity(1024);
        encoder.encode(&self.registry.gather(), &mut buf)?;
        String::from_utf8(buf).map_err(|_| MetricsError::NonUtf8)
    }
}

/// Map a [`CameraState`] variant to its short label.
///
/// Kept separate from `STATE_LABELS` so the compiler verifies
/// exhaustiveness — adding a new variant fails to compile here.
fn state_label(state: &CameraState) -> &'static str {
    match state {
        CameraState::Idle => "idle",
        CameraState::Activating => "activating",
        CameraState::Live => "live",
        CameraState::BatteryProtect { .. } => "battery-protect",
        CameraState::Failed { .. } => "failed",
    }
}

impl MetricsRecorder for Metrics {
    fn record_state_change(
        &self,
        camera: &CameraId,
        from: &CameraState,
        to: &CameraState,
        signal: &str,
    ) {
        let from_label = state_label(from);
        let to_label = state_label(to);

        // Flip the active-state gauge (zero out the old row, set the new).
        self.camera_state
            .with_label_values(&[camera.as_str(), from_label])
            .set(0);
        self.camera_state
            .with_label_values(&[camera.as_str(), to_label])
            .set(1);

        self.state_transitions_total
            .with_label_values(&[camera.as_str(), from_label, to_label, signal])
            .inc();
    }

    fn record_motion(&self, camera: &CameraId, outcome: MotionOutcome) {
        self.motion_events_total
            .with_label_values(&[camera.as_str(), outcome.as_label()])
            .inc();
    }

    fn record_budget(&self, camera: &CameraId, decision: BudgetDecision) {
        self.budget_decisions_total
            .with_label_values(&[camera.as_str(), decision.as_label()])
            .inc();
    }

    fn record_splice(&self, camera: &CameraId, outcome: SpliceOutcome, latency_ms: u64) {
        let label = outcome.as_label();
        self.splice_attempts_total
            .with_label_values(&[camera.as_str(), label])
            .inc();
        // f64 cast is fine; latency_ms is bounded by the splice timeout
        // in practice (well under 2^53).
        #[allow(clippy::cast_precision_loss)]
        self.splice_latency_ms
            .with_label_values(&[camera.as_str(), label])
            .observe(latency_ms as f64);
    }

    fn record_failure(&self, camera: &CameraId, _retries: u32) {
        self.retries_total
            .with_label_values(&[camera.as_str()])
            .inc();
    }
}

fn now_unix_seconds() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
        Err(e) => {
            warn!(error = %e, "system clock before UNIX_EPOCH; reporting 0");
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_succeeds_with_basic_inputs() {
        let m = Metrics::new(3, "0.1.0").expect("metrics build ok");
        let rendered = m.render().expect("render ok");
        assert!(rendered.contains("streamer_arlo_connected 0"));
        assert!(rendered.contains("streamer_cameras_configured 3"));
        assert!(rendered.contains("streamer_build_info{version=\"0.1.0\"} 1"));
    }

    #[test]
    fn set_arlo_connected_updates_gauge() {
        let m = Metrics::new(1, "0.1.0").unwrap();
        m.set_arlo_connected(true);
        assert!(m.render().unwrap().contains("streamer_arlo_connected 1"));
        m.set_arlo_connected(false);
        assert!(m.render().unwrap().contains("streamer_arlo_connected 0"));
    }

    #[test]
    fn render_includes_uptime_and_started_at() {
        let m = Metrics::new(0, "0.1.0").unwrap();
        let out = m.render().unwrap();
        assert!(out.contains("streamer_uptime_seconds"));
        assert!(out.contains("streamer_started_at_unix_seconds"));
    }

    #[test]
    fn render_text_format_is_well_formed() {
        // Each metric line should start with a name token. We can't
        // fully parse, but we can sanity-check no panic and non-empty.
        let m = Metrics::new(2, "1.2.3").unwrap();
        let out = m.render().unwrap();
        assert_ne!(out, "");
        assert!(out.lines().any(|l| l.starts_with("streamer_")));
    }

    #[test]
    fn registry_is_clonable_and_shared() {
        let m = Metrics::new(1, "0.1.0").unwrap();
        let r1 = m.registry();
        let r2 = m.registry();
        // Same Arc target.
        assert!(Arc::ptr_eq(&r1, &r2));
    }

    #[test]
    fn cameras_configured_zero_renders_zero() {
        let m = Metrics::new(0, "0.1.0").unwrap();
        assert!(
            m.render()
                .unwrap()
                .contains("streamer_cameras_configured 0")
        );
    }

    #[test]
    fn build_info_label_is_quoted() {
        let m = Metrics::new(1, "1.2.3-rc1").unwrap();
        let out = m.render().unwrap();
        assert!(out.contains("version=\"1.2.3-rc1\""));
    }

    // ---------- Per-camera recorder tests (Phase 6) ----------

    use std::time::Duration;
    use streamer_domain::state::CameraState;

    fn cam(id: &str) -> CameraId {
        CameraId::new(id)
    }

    #[test]
    fn prewarm_camera_seeds_all_state_rows() {
        let m = Metrics::new(1, "0.1.0").unwrap();
        m.prewarm_camera(&cam("CAM"));
        let out = m.render().unwrap();
        for state in STATE_LABELS {
            let needle = format!("streamer_camera_state{{camera=\"CAM\",state=\"{state}\"}} 0");
            assert!(
                out.contains(&needle),
                "expected pre-warm row {needle}, got:\n{out}"
            );
        }
    }

    #[test]
    fn record_state_change_flips_active_row_and_increments_counter() {
        let m = Metrics::new(1, "0.1.0").unwrap();
        let id = cam("CAM");
        m.record_state_change(
            &id,
            &CameraState::Idle,
            &CameraState::Activating,
            "motion-detected",
        );
        let out = m.render().unwrap();
        assert!(out.contains("streamer_camera_state{camera=\"CAM\",state=\"idle\"} 0"));
        assert!(out.contains("streamer_camera_state{camera=\"CAM\",state=\"activating\"} 1"));
        // Prometheus sorts labels alphabetically in the text encoding;
        // assert on the substring that does not depend on order.
        assert!(out.contains("streamer_state_transitions_total{"));
        assert!(out.contains("from=\"idle\""));
        assert!(out.contains("to=\"activating\""));
        assert!(out.contains("signal=\"motion-detected\""));
    }

    #[test]
    fn record_motion_increments_per_outcome() {
        let m = Metrics::new(1, "0.1.0").unwrap();
        let id = cam("CAM");
        m.record_motion(&id, MotionOutcome::Triggered);
        m.record_motion(&id, MotionOutcome::Triggered);
        m.record_motion(&id, MotionOutcome::Absorbed);
        let out = m.render().unwrap();
        assert!(
            out.contains("streamer_motion_events_total{camera=\"CAM\",outcome=\"triggered\"} 2")
        );
        assert!(
            out.contains("streamer_motion_events_total{camera=\"CAM\",outcome=\"absorbed\"} 1")
        );
    }

    #[test]
    fn record_budget_emits_decision_label() {
        let m = Metrics::new(1, "0.1.0").unwrap();
        let id = cam("CAM");
        m.record_budget(&id, BudgetDecision::Granted);
        m.record_budget(&id, BudgetDecision::Denied);
        let out = m.render().unwrap();
        assert!(
            out.contains("streamer_budget_decisions_total{camera=\"CAM\",decision=\"granted\"} 1")
        );
        assert!(
            out.contains("streamer_budget_decisions_total{camera=\"CAM\",decision=\"denied\"} 1")
        );
    }

    #[test]
    fn record_splice_increments_counter_and_observes_histogram() {
        let m = Metrics::new(1, "0.1.0").unwrap();
        let id = cam("CAM");
        m.record_splice(&id, SpliceOutcome::Success, 800);
        m.record_splice(&id, SpliceOutcome::AttachFailed, 5_000);
        let out = m.render().unwrap();
        assert!(
            out.contains("streamer_splice_attempts_total{camera=\"CAM\",outcome=\"success\"} 1")
        );
        assert!(out.contains("streamer_splice_latency_ms_bucket"));
        assert!(out.contains("streamer_splice_latency_ms_sum"));
    }

    #[test]
    fn record_failure_increments_retries_total() {
        let m = Metrics::new(1, "0.1.0").unwrap();
        let id = cam("CAM");
        m.record_failure(&id, 1);
        m.record_failure(&id, 2);
        let out = m.render().unwrap();
        assert!(out.contains("streamer_retries_total{camera=\"CAM\"} 2"));
    }

    #[test]
    fn state_label_covers_every_variant() {
        // Smoke test that the helper returns one of the known labels
        // for each variant of CameraState.
        let cases = [
            CameraState::Idle,
            CameraState::Activating,
            CameraState::Live,
            CameraState::BatteryProtect {
                reset_in: Duration::from_secs(1),
            },
            CameraState::Failed {
                reason: "x".into(),
                retries: 0,
            },
        ];
        for c in &cases {
            let l = state_label(c);
            assert!(STATE_LABELS.contains(&l), "unknown label {l}");
        }
    }

    #[test]
    fn metrics_implements_recorder_trait_object() {
        // Compile-time check that Metrics satisfies dyn MetricsRecorder.
        let m: Arc<dyn streamer_domain::port::MetricsRecorder> =
            Arc::new(Metrics::new(1, "0.1.0").unwrap());
        m.record_motion(&cam("X"), MotionOutcome::Triggered);
    }
}
