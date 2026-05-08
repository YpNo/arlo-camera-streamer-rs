//! Driven port traits. Implemented by infrastructure adapters in
//! `streamer-infra-arlo` (Arlo client wrapper) and `streamer-infra-media`
//! (GStreamer pipelines), composed against by the application layer.
//!
//! All port methods take `&self` so adapters can be wrapped in
//! `Arc<dyn Port>` and shared across orchestrator tasks. Internal
//! mutability is the adapter's responsibility (e.g. `Arc<RwLock<…>>`
//! around an `ArloClient`).

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

use crate::camera::CameraId;
use crate::error::DomainError;
use crate::event::{CameraEvent, ConnectionStatus};
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
