//! Port traits — the contracts between the application layer and the
//! outside world.
//!
//! - **Driven ports** (`ArloEventSource`, `ArloStreamRequester`,
//!   `ArloThumbnailSource`, `MediaMultiplexer`, `MetricsRecorder`) are
//!   implemented by infrastructure adapters in `streamer-infra-*`
//!   crates. The application layer drives them.
//! - **Driving ports** (`AdminControl`) are implemented by the
//!   application layer and called *into* by inbound adapters
//!   (the `/admin` axum routes in `streamer-infra-ops`).
//!
//! All port methods take `&self` so adapters can be wrapped in
//! `Arc<dyn Port>` and shared across tasks. Internal mutability is the
//! adapter's responsibility (e.g. `Arc<RwLock<…>>` around an
//! `ArloClient`, or an `mpsc::Sender` for command actors).

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

use crate::admin::{AdminError, CameraSnapshot, SystemSnapshot};
use crate::camera::CameraId;
use crate::error::DomainError;
use crate::event::{CameraEvent, ConnectionStatus};
use crate::metrics::{BudgetDecision, MotionOutcome, SpliceOutcome};
use crate::state::CameraState;
use crate::stream::StreamSource;

/// Subscription to the inbound event bus from Arlo cloud / local hub.
#[async_trait]
pub trait ArloEventSource: Send + Sync {
    /// Open a subscription to the camera event stream.
    ///
    /// The returned stream stays alive until the underlying transport
    /// is dropped or the upstream connection is permanently closed.
    /// Reconnection between transient outages is the adapter's job;
    /// the application layer observes it through [`Self::connection_status`].
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::AdapterTransport`] if the bus cannot be
    /// initialized (e.g. session expired and re-auth failed).
    async fn subscribe(&self) -> Result<BoxStream<'static, CameraEvent>, DomainError>;

    /// Watch the connection status of the upstream event bus.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::AdapterTransport`] when the watcher
    /// cannot be initialized.
    async fn connection_status(&self) -> Result<BoxStream<'static, ConnectionStatus>, DomainError>;
}

/// On-demand request for a live RTSPS stream URL from a specific camera.
#[async_trait]
pub trait ArloStreamRequester: Send + Sync {
    /// Request a live stream from `camera`.
    ///
    /// Implementations correlate the SSE event-bus response with the
    /// `startStream` POST and resolve once the URL is known. Callers
    /// must impose their own timeout via `tokio::time::timeout` if
    /// needed.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::AdapterTransport`] on network or
    /// protocol failure, or [`DomainError::UnknownCamera`] if the
    /// device id is not recognized by the upstream.
    async fn request_live(&self, camera: &CameraId) -> Result<StreamSource, DomainError>;
}

/// Per-camera last-known thumbnail (used as the idle still frame).
#[async_trait]
pub trait ArloThumbnailSource: Send + Sync {
    /// Fetch the most recent thumbnail JPEG for `camera`. Returns
    /// `Ok(None)` when the upstream has no image (newly provisioned
    /// device); callers should fall back to a synthetic frame.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::AdapterTransport`] on network failure.
    async fn last_thumbnail(&self, camera: &CameraId) -> Result<Option<Bytes>, DomainError>;
}

/// Output-side multiplexer: owns the per-camera GStreamer pipeline,
/// the embedded RTSP server, and any HLS / DASH sinks.
#[async_trait]
pub trait MediaMultiplexer: Send + Sync {
    /// Register a camera and bring up its idle pipeline. After this call
    /// the camera's output endpoints are reachable and serving the
    /// idle frame. Idempotent for the same `camera`.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::AdapterTransport`] on pipeline construction
    /// failure (e.g. GStreamer plugin missing).
    async fn register(&self, camera: &CameraId) -> Result<(), DomainError>;

    /// Attach a live source. Resolves once the live branch has emitted
    /// its first IDR frame and the output selector has been switched
    /// to the live pad — this is the moment Frigate sees motion-quality
    /// frames.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::AdapterTransport`] if the source cannot
    /// be attached or the splice fails (no IDR within timeout).
    async fn attach_live(&self, camera: &CameraId, source: StreamSource)
    -> Result<(), DomainError>;

    /// Detach the live source and revert to the idle frame.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::AdapterTransport`] if the live branch
    /// cannot be drained cleanly.
    async fn detach_live(&self, camera: &CameraId) -> Result<(), DomainError>;

    /// Refresh the idle still image (best-effort). Called on every
    /// `Live → Cooling → Idle` transition with a freshly fetched
    /// thumbnail so the next idle period reflects the latest scene.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::AdapterTransport`] if the JPEG cannot
    /// be decoded or pushed to the idle source.
    async fn refresh_thumbnail(&self, camera: &CameraId, jpeg: Bytes) -> Result<(), DomainError>;
}

/// Application-layer instrumentation sink.
///
/// The orchestrator and router emit business-level events here so the
/// infrastructure layer can translate them into Prometheus counters,
/// log lines, or any other observability backend.
///
/// All methods are intentionally **infallible** — instrumentation must
/// never break the hot path. Adapters log internally on failure.
pub trait MetricsRecorder: Send + Sync {
    /// A camera transitioned from `from` to `to` because of `signal`.
    /// Called *after* the new state is committed.
    ///
    /// `signal` is a short kebab-case string (`"motion-detected"`,
    /// `"cooldown-expired"`, etc.) — the domain crate keeps full enum
    /// privacy by accepting strings here so future variants don't ripple
    /// through the recorder.
    fn record_state_change(
        &self,
        camera: &CameraId,
        from: &CameraState,
        to: &CameraState,
        signal: &str,
    );

    /// A motion / audio event arrived from the bus.
    fn record_motion(&self, camera: &CameraId, outcome: MotionOutcome);

    /// The orchestrator made a budget decision (live granted / denied
    /// because exhausted / reset).
    fn record_budget(&self, camera: &CameraId, decision: BudgetDecision);

    /// The Idle → Live splice resolved with the given outcome.
    /// `latency_ms` is the wall-clock from the trigger to the call to
    /// `MediaMultiplexer::attach_live` returning.
    fn record_splice(&self, camera: &CameraId, outcome: SpliceOutcome, latency_ms: u64);

    /// Camera entered `Failed { retries }` — track for alerting.
    fn record_failure(&self, camera: &CameraId, retries: u32);
}

/// Inbound application-layer port for the admin HTTP surface.
///
/// Implemented by `streamer-app`. Called *by* `streamer-infra-ops`
/// when a `/admin/*` request lands on axum.
///
/// All methods are async because the app-layer impl forwards the call
/// to per-camera orchestrator tasks via `mpsc` and awaits the reply.
#[async_trait]
pub trait AdminControl: Send + Sync {
    /// Return a snapshot of the system: per-camera state, debouncer
    /// remaining, budget left, last failure, etc. Read-only — never
    /// blocks the orchestrators for long.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::Unavailable`] if any orchestrator is
    /// unresponsive (the call timed out waiting on its mailbox).
    async fn snapshot(&self) -> Result<SystemSnapshot, AdminError>;

    /// Snapshot of a single camera. Returns
    /// [`AdminError::UnknownCamera`] when not configured.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::UnknownCamera`] when `camera` is not
    /// configured or [`AdminError::Unavailable`] when the orchestrator
    /// task did not respond in time.
    async fn camera_snapshot(&self, camera: &CameraId) -> Result<CameraSnapshot, AdminError>;

    /// Force a camera into `Idle`, detaching any live stream.
    /// Idempotent: a no-op if already idle.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::UnknownCamera`] if `camera` is not
    /// configured, or [`AdminError::Unavailable`] on a control-plane
    /// transport failure.
    async fn force_idle(&self, camera: &CameraId) -> Result<(), AdminError>;

    /// Manually wake the camera (synthetic motion event), respecting
    /// the daily budget. Useful for healthcheck dashboards.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::UnknownCamera`] if `camera` is not
    /// configured, or [`AdminError::Unavailable`] on a control-plane
    /// transport failure.
    async fn manual_wake(&self, camera: &CameraId) -> Result<(), AdminError>;
}
