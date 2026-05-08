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

use prometheus::{Encoder, IntGauge, IntGaugeVec, Registry, TextEncoder, opts};
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
/// Several fields look dead-code (`cameras_configured`, `started_at`,
/// `build_info`) — they are set once at construction and then live in
/// the registry's collector list. The struct keeps a clone so future
/// callers (or a `MetricsRecorder` trait impl) can mutate them
/// without re-registering. Allow dead-code rather than re-fetching
/// gauges from the registry by name on every update.
#[derive(Clone)]
#[allow(dead_code)]
pub struct Metrics {
    registry: Arc<Registry>,
    /// `1` when the Arlo SSE bus is connected, `0` otherwise.
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
}

impl Metrics {
    /// Build a registry pre-populated with the Phase 5 metric set.
    ///
    /// `cameras_configured` is the count of `[[cameras]]` blocks; it
    /// is set once and never changes. `version` is exposed as a label
    /// on `streamer_build_info`.
    ///
    /// # Errors
    ///
    /// Returns [`MetricsError::Prometheus`] if any gauge fails to
    /// register (duplicate names, invalid characters).
    pub fn new(cameras_configured: u32, version: &str) -> Result<Self, MetricsError> {
        let registry = Arc::new(Registry::new());

        let arlo_connected = IntGauge::with_opts(opts!(
            "streamer_arlo_connected",
            "1 if connected to the Arlo SSE bus, 0 otherwise"
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

        registry.register(Box::new(arlo_connected.clone()))?;
        registry.register(Box::new(cameras_configured_g.clone()))?;
        registry.register(Box::new(started_at.clone()))?;
        registry.register(Box::new(uptime_seconds.clone()))?;
        registry.register(Box::new(build_info.clone()))?;

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
        })
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
        assert!(!out.is_empty());
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
}
