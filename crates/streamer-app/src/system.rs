//! Composition root for the application layer.
//!
//! [`StreamerSystem::spawn`] takes the four port handles plus the
//! [`StreamerConfig`] and brings up:
//!
//! - One [`CameraOrchestrator`] tokio task per `[[cameras]]` block.
//! - One [`EventRouter`] tokio task fanning the shared event bus into
//!   per-camera mailboxes.
//!
//! All tasks share a single [`CancellationToken`] so a graceful
//! shutdown propagates atomically. [`StreamerSystem::shutdown`] cancels
//! the token and awaits every spawned handle.
//!
//! # Supervision
//!
//! Every task runs under a supervisor: one that panics, or ends while
//! the token is not cancelled, cancels the token. The composition root
//! waits on that token next to the signal handler, so the daemon stops
//! as a whole (and exits non-zero) instead of running without a camera
//! or without the event bus while `/readyz` keeps answering. A panic is
//! logged and counted ([`StreamerSystem::panic_counter`]) even during the
//! drain, so a stop that lost a task still exits non-zero; a panicking
//! orchestrator first releases its camera, since its own exit path
//! (detach and teardown) never ran.
//!
//! # Channel sizing
//!
//! Each per-camera mailbox is sized at [`MAILBOX_CAPACITY`]. Motion
//! events arrive at most a few per minute on a busy camera, so 32 is
//! ample headroom — the [`EventRouter`] will log and drop on overflow
//! rather than block the shared bus.

#![allow(clippy::similar_names)]

use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use streamer_domain::camera::CameraId;

use streamer_domain::config::StreamerConfig;
use streamer_domain::error::DomainError;
use streamer_domain::event::CameraEvent;
use streamer_domain::port::{
    ArloEventSource, ArloThumbnailSource, MediaMultiplexer, MetricsRecorder, UserViewSource,
    WebrtcSignaler,
};

use crate::admin::{ADMIN_MAILBOX_CAPACITY, AdminCommand, AdminControlActor, AdminRoute};
use crate::metrics_noop::NoopRecorder;
use crate::orchestrator::CameraOrchestrator;
use crate::router::EventRouter;

/// Per-camera mailbox capacity. See module docs for sizing rationale.
pub const MAILBOX_CAPACITY: usize = 32;

/// How long a panicked orchestrator's camera release may take before the
/// supervisor gives up on it.
const PANIC_RELEASE_TIMEOUT: Duration = Duration::from_secs(5);

/// Handle to a running streamer system. Keep it alive for the lifetime
/// of the daemon; drop / call [`StreamerSystem::shutdown`] for graceful
/// teardown.
#[derive(Debug)]
pub struct StreamerSystem {
    handles: Vec<JoinHandle<()>>,
    shutdown: CancellationToken,
    admin: AdminControlActor,
    /// Shared with the connection-status watcher in `streamer-bin` so
    /// the admin snapshot reflects current bus state.
    arlo_connected: Arc<AtomicBool>,
    /// Tasks that panicked, during the run or the drain.
    panics: Arc<AtomicUsize>,
}

impl StreamerSystem {
    /// Spawn the orchestrator + router tasks, return a handle.
    ///
    /// # Errors
    ///
    /// - [`DomainError::AdapterTransport`] when [`ArloEventSource::subscribe`]
    ///   fails.
    /// - [`DomainError::InvalidConfig`] when a camera's
    ///   [`CooldownConfig`](streamer_domain::config::CooldownConfig)
    ///   is rejected by the budget tracker (e.g., bad `budget_reset`).
    pub async fn spawn(
        config: &StreamerConfig,
        event_source: Arc<dyn ArloEventSource>,
        signaler: Arc<dyn WebrtcSignaler>,
        thumbnails: Arc<dyn ArloThumbnailSource>,
        media: Arc<dyn MediaMultiplexer>,
        user_views: Arc<dyn UserViewSource>,
        metrics: Arc<dyn MetricsRecorder>,
        version: &'static str,
    ) -> Result<Self, DomainError> {
        config.validate()?;
        if config.cameras.is_empty() {
            return Err(DomainError::InvalidConfig(
                "no [[cameras]] configured — nothing to do".to_string(),
            ));
        }

        let shutdown = CancellationToken::new();
        let panics = Arc::new(AtomicUsize::new(0));
        let mut event_routes: HashMap<_, mpsc::Sender<CameraEvent>> = HashMap::new();
        let mut admin_routes: HashMap<_, AdminRoute> = HashMap::new();
        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        for camera_cfg in &config.cameras {
            let (event_tx, event_rx) = mpsc::channel(MAILBOX_CAPACITY);
            let (admin_tx, admin_rx) = mpsc::channel::<AdminCommand>(ADMIN_MAILBOX_CAPACITY);
            if event_routes
                .insert(camera_cfg.arlo_device_id.clone(), event_tx)
                .is_some()
            {
                // `validate()` refuses this first; kept as a guard so a
                // second orchestrator can never be spawned for one camera.
                return Err(DomainError::InvalidConfig(format!(
                    "duplicate [[cameras]] arlo_device_id {}",
                    camera_cfg.arlo_device_id
                )));
            }
            let orch = CameraOrchestrator::new(
                camera_cfg,
                signaler.clone(),
                thumbnails.clone(),
                media.clone(),
                user_views.clone(),
                metrics.clone(),
                event_rx,
                admin_rx,
                shutdown.child_token(),
            )?;
            admin_routes.insert(
                camera_cfg.arlo_device_id.clone(),
                AdminRoute {
                    sender: admin_tx,
                    snapshots: orch.snapshots(),
                },
            );
            handles.push(tokio::spawn(supervised(
                "orchestrator",
                camera_cfg.arlo_device_id.to_string(),
                release_on_panic(
                    orch.run(),
                    camera_cfg.arlo_device_id.clone(),
                    media.clone(),
                    signaler.clone(),
                ),
                shutdown.clone(),
                panics.clone(),
            )));
        }

        let events = event_source.subscribe().await?;
        let router = EventRouter::new(event_routes, metrics.clone());
        // The router gets the shared token itself: an upstream end must
        // cancel the orchestrators too, which a child token cannot.
        handles.push(tokio::spawn(supervised(
            "router",
            "event-bus".to_string(),
            router.run(events, shutdown.clone()),
            shutdown.clone(),
            panics.clone(),
        )));

        let arlo_connected = Arc::new(AtomicBool::new(false));
        let admin = AdminControlActor::new(admin_routes, version, arlo_connected.clone());

        info!(cameras = config.cameras.len(), "streamer system spawned");
        Ok(Self {
            handles,
            shutdown,
            admin,
            arlo_connected,
            panics,
        })
    }

    /// Spawn with no observability (passes `NoopRecorder`). Convenience
    /// wrapper for tests and minimal embeds.
    ///
    /// # Errors
    ///
    /// See [`StreamerSystem::spawn`].
    pub async fn spawn_no_metrics(
        config: &StreamerConfig,
        event_source: Arc<dyn ArloEventSource>,
        signaler: Arc<dyn WebrtcSignaler>,
        thumbnails: Arc<dyn ArloThumbnailSource>,
        media: Arc<dyn MediaMultiplexer>,
        user_views: Arc<dyn UserViewSource>,
    ) -> Result<Self, DomainError> {
        Self::spawn(
            config,
            event_source,
            signaler,
            thumbnails,
            media,
            user_views,
            Arc::new(NoopRecorder),
            "0.0.0",
        )
        .await
    }

    /// Borrow the admin-control actor. Keep a clone for the lifetime of
    /// the HTTP server.
    #[must_use]
    pub fn admin_control(&self) -> AdminControlActor {
        self.admin.clone()
    }

    /// Shared `Arc<AtomicBool>` reflecting the Arlo bus connection
    /// state. The composition root updates it from the connection
    /// watcher; the admin snapshot reads it.
    #[must_use]
    pub fn arlo_connected_flag(&self) -> Arc<AtomicBool> {
        self.arlo_connected.clone()
    }

    /// How many tasks panicked so far, the drain included. Take it before
    /// [`Self::shutdown`] and read it after: a stop that lost a task must
    /// not report success.
    #[must_use]
    pub fn panic_counter(&self) -> Arc<AtomicUsize> {
        self.panics.clone()
    }

    /// Cancel all tasks and wait for them to drain. Idempotent.
    pub async fn shutdown(self) {
        info!("shutting down streamer system");
        self.shutdown.cancel();
        for handle in self.handles {
            if let Err(e) = handle.await {
                warn!(error = %e, "task join failed");
            }
        }
        info!("streamer system shut down");
    }

    /// Cancellation token shared by every spawned task. Useful for
    /// callers that want to wire it to a `tokio::signal` handler.
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }
}

/// Run one actor task and make its end visible. A task that stops while
/// the system is still meant to run — a panic, or an exit the actor did
/// not expect — cancels the shared token so the daemon stops as a whole
/// instead of running without that actor. An exit after the token was
/// cancelled is the normal shutdown and stays quiet.
async fn supervised<F>(
    task: &'static str,
    name: String,
    fut: F,
    shutdown: CancellationToken,
    panics: Arc<AtomicUsize>,
) where
    F: Future<Output = ()>,
{
    let outcome = AssertUnwindSafe(fut).catch_unwind().await;
    // A panic is reported whatever the token says: one during the drain
    // used to vanish, and the process exited 0 with a session maybe open.
    if outcome.is_err() {
        panics.fetch_add(1, Ordering::Relaxed);
        error!(task, %name, "task panicked");
    }
    if shutdown.is_cancelled() {
        return;
    }
    if outcome.is_ok() {
        warn!(task, %name, "task ended while the system is running; stopping the system");
    } else {
        error!(task, %name, "stopping the system after the panic");
    }
    shutdown.cancel();
}

/// Run an orchestrator; if it panics, release its camera (detach the live
/// source, tear the Arlo session down) before passing the panic on. Its
/// own exit path never ran, and a session left open keeps the camera
/// streaming on battery. Both calls are idempotent, so releasing a camera
/// that was idle costs nothing.
async fn release_on_panic<F>(
    fut: F,
    camera: CameraId,
    media: Arc<dyn MediaMultiplexer>,
    signaler: Arc<dyn WebrtcSignaler>,
) where
    F: Future<Output = ()>,
{
    let Err(panic) = AssertUnwindSafe(fut).catch_unwind().await else {
        return;
    };
    error!(%camera, "orchestrator panicked; releasing its camera");
    let release = async {
        if let Err(e) = media.detach_live(&camera).await {
            warn!(%camera, error = %e, "detach after the panic failed");
        }
        if let Err(e) = signaler.teardown(&camera).await {
            warn!(%camera, error = %e, "teardown after the panic failed");
        }
    };
    if tokio::time::timeout(PANIC_RELEASE_TIMEOUT, release)
        .await
        .is_err()
    {
        warn!(%camera, "camera release after the panic timed out");
    }
    std::panic::resume_unwind(panic);
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use std::path::PathBuf;
    use streamer_domain::camera::{CameraId, StreamName};
    use streamer_domain::config::{
        ArloConfig, CameraConfig, CooldownConfig, EmailMfaConfig, MfaConfig, OutputConfig,
        RtspOutput, WebrtcConfig,
    };
    use streamer_domain::event::ConnectionStatus;
    use streamer_domain::stream::SignalingAnswer;

    // Minimal stub adapters for spawn-time wiring tests. The bus stays
    // open (pending): an ended bus stops the system, see `EndingEventSource`.
    struct StubEventSource;
    #[async_trait]
    impl ArloEventSource for StubEventSource {
        async fn subscribe(&self) -> Result<BoxStream<'static, CameraEvent>, DomainError> {
            Ok(Box::pin(futures::stream::pending()))
        }
        async fn connection_status(
            &self,
        ) -> Result<BoxStream<'static, ConnectionStatus>, DomainError> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    /// A bus that ends right away, as a dropped upstream connection does.
    struct EndingEventSource;
    #[async_trait]
    impl ArloEventSource for EndingEventSource {
        async fn subscribe(&self) -> Result<BoxStream<'static, CameraEvent>, DomainError> {
            Ok(Box::pin(futures::stream::empty()))
        }
        async fn connection_status(
            &self,
        ) -> Result<BoxStream<'static, ConnectionStatus>, DomainError> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    struct FailingEventSource;
    #[async_trait]
    impl ArloEventSource for FailingEventSource {
        async fn subscribe(&self) -> Result<BoxStream<'static, CameraEvent>, DomainError> {
            Err(DomainError::AdapterTransport("simulated".to_string()))
        }
        async fn connection_status(
            &self,
        ) -> Result<BoxStream<'static, ConnectionStatus>, DomainError> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    #[derive(Default)]
    struct StubSignaler {
        teardowns: AtomicUsize,
    }
    #[async_trait]
    impl WebrtcSignaler for StubSignaler {
        async fn negotiate(
            &self,
            _camera: &CameraId,
            _offer: &mut dyn streamer_domain::port::OfferBuilder,
        ) -> Result<SignalingAnswer, DomainError> {
            Ok(SignalingAnswer {
                answer_sdp: "v=0\r\n".to_string(),
                session_id: "sess".to_string(),
            })
        }
        async fn teardown(&self, _camera: &CameraId) -> Result<(), DomainError> {
            self.teardowns.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    struct StubThumbnails;
    #[async_trait]
    impl ArloThumbnailSource for StubThumbnails {
        async fn last_thumbnail(&self, _camera: &CameraId) -> Result<Option<Bytes>, DomainError> {
            Ok(None)
        }
    }

    /// Retains every notifier so a session never resolves as
    /// `AdapterDropped` behind the wiring tests' back.
    struct StubUserViews;
    #[async_trait]
    impl UserViewSource for StubUserViews {
        async fn watch_along_url(
            &self,
            _camera: &CameraId,
        ) -> Result<streamer_domain::stream::WatchAlongUrl, DomainError> {
            Err(DomainError::AdapterTransport("not in this stub".into()))
        }
    }

    #[derive(Default)]
    struct StubMedia {
        notifiers: std::sync::Mutex<Vec<streamer_domain::stream::LiveLossNotifier>>,
        detaches: AtomicUsize,
    }
    #[async_trait]
    impl MediaMultiplexer for StubMedia {
        async fn attach_user_view(
            &self,
            _camera: &CameraId,
            _url: &streamer_domain::stream::WatchAlongUrl,
        ) -> Result<streamer_domain::stream::LiveSession, DomainError> {
            Err(DomainError::AdapterTransport("not in this stub".into()))
        }
        async fn set_user_view_notice(
            &self,
            _camera: &CameraId,
            _shown: bool,
        ) -> Result<(), DomainError> {
            Ok(())
        }
        async fn register(&self, _camera: &CameraId) -> Result<(), DomainError> {
            Ok(())
        }
        async fn attach_live(
            &self,
            _camera: &CameraId,
            _signaler: &dyn WebrtcSignaler,
        ) -> Result<streamer_domain::stream::LiveSession, DomainError> {
            let (session, notifier) = streamer_domain::stream::LiveSession::new();
            self.notifiers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(notifier);
            Ok(session)
        }
        async fn detach_live(&self, _camera: &CameraId) -> Result<(), DomainError> {
            self.detaches.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        async fn refresh_thumbnail(
            &self,
            _camera: &CameraId,
            _jpeg: Bytes,
        ) -> Result<(), DomainError> {
            Ok(())
        }
    }

    fn one_camera_config() -> StreamerConfig {
        StreamerConfig {
            arlo: ArloConfig {
                email: "u@example.com".to_string(),
                password_env: "PW".to_string(),
                session_cache_path: PathBuf::from("/tmp/x.json"),
                app_version: "6.46.0".to_string(),
                watch_along_cert_sha256: None,
                mfa: MfaConfig::Email(EmailMfaConfig {
                    host: Some("h".to_string()),
                    provider: None,
                    user: Some("u".to_string()),
                    password_env: Some("IPW".to_string()),
                    port: 993,
                }),
            },
            output: OutputConfig {
                rtsp: RtspOutput {
                    bind: "0.0.0.0:8554".to_string(),
                },
                hls: None,
                dash: None,
                video_encoder: streamer_domain::config::VideoEncoder::X264,
                metrics_bind: "127.0.0.1:9090".to_string(),
                admin_bind: "127.0.0.1:9091".to_string(),
            },
            webrtc: WebrtcConfig::default(),
            cameras: vec![CameraConfig {
                arlo_device_id: CameraId::new("CAM"),
                stream_name: StreamName::parse("cam").unwrap(),
                codec_hint: None,
                cooldown: CooldownConfig::default(),
            }],
        }
    }

    #[tokio::test]
    async fn spawn_rejects_empty_camera_list() {
        let mut cfg = one_camera_config();
        cfg.cameras.clear();
        let err = StreamerSystem::spawn_no_metrics(
            &cfg,
            Arc::new(StubEventSource),
            Arc::new(StubSignaler::default()),
            Arc::new(StubThumbnails),
            Arc::new(StubMedia::default()),
            Arc::new(StubUserViews),
        )
        .await
        .expect_err("must reject empty camera list");
        assert!(matches!(err, DomainError::InvalidConfig(_)));
    }

    #[tokio::test]
    async fn spawn_propagates_subscribe_failure() {
        let cfg = one_camera_config();
        let err = StreamerSystem::spawn_no_metrics(
            &cfg,
            Arc::new(FailingEventSource),
            Arc::new(StubSignaler::default()),
            Arc::new(StubThumbnails),
            Arc::new(StubMedia::default()),
            Arc::new(StubUserViews),
        )
        .await
        .expect_err("must surface subscribe failure");
        assert!(matches!(err, DomainError::AdapterTransport(_)));
    }

    #[tokio::test]
    async fn spawn_then_shutdown_drains_cleanly() {
        let cfg = one_camera_config();
        let system = StreamerSystem::spawn_no_metrics(
            &cfg,
            Arc::new(StubEventSource),
            Arc::new(StubSignaler::default()),
            Arc::new(StubThumbnails),
            Arc::new(StubMedia::default()),
            Arc::new(StubUserViews),
        )
        .await
        .expect("spawn ok");

        // Cancellation token is shared with internal tasks.
        let token = system.cancellation_token();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!token.is_cancelled(), "a running system stays up");

        tokio::time::timeout(std::time::Duration::from_secs(2), system.shutdown())
            .await
            .expect("shutdown timed out");
    }

    #[tokio::test]
    async fn an_ended_event_bus_stops_the_whole_system() {
        let cfg = one_camera_config();
        let system = StreamerSystem::spawn_no_metrics(
            &cfg,
            Arc::new(EndingEventSource),
            Arc::new(StubSignaler::default()),
            Arc::new(StubThumbnails),
            Arc::new(StubMedia::default()),
            Arc::new(StubUserViews),
        )
        .await
        .expect("spawn ok");

        let token = system.cancellation_token();
        tokio::time::timeout(std::time::Duration::from_secs(2), token.cancelled())
            .await
            .expect("the system cancels itself when the bus ends");
        tokio::time::timeout(std::time::Duration::from_secs(2), system.shutdown())
            .await
            .expect("shutdown drains");
    }

    #[tokio::test]
    async fn supervised_cancels_the_token_when_the_task_panics() {
        let token = CancellationToken::new();
        let panics = Arc::new(AtomicUsize::new(0));
        supervised(
            "test",
            "panicking".to_string(),
            async { panic!("simulated actor bug") },
            token.clone(),
            panics.clone(),
        )
        .await;
        assert!(token.is_cancelled());
        assert_eq!(panics.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn supervised_cancels_the_token_when_the_task_ends_early() {
        let token = CancellationToken::new();
        let panics = Arc::new(AtomicUsize::new(0));
        supervised(
            "test",
            "quitting".to_string(),
            async {},
            token.clone(),
            panics.clone(),
        )
        .await;
        assert!(token.is_cancelled());
        assert_eq!(
            panics.load(Ordering::Relaxed),
            0,
            "an early end is not a panic"
        );
    }

    #[tokio::test]
    async fn supervised_leaves_a_cancelled_token_alone_on_a_normal_exit() {
        let token = CancellationToken::new();
        let inner = token.clone();
        let task = async move { inner.cancelled().await };
        token.cancel();
        let panics = Arc::new(AtomicUsize::new(0));
        supervised(
            "test",
            "draining".to_string(),
            task,
            token.clone(),
            panics.clone(),
        )
        .await;
        assert!(token.is_cancelled());
        assert_eq!(panics.load(Ordering::Relaxed), 0);
    }

    /// A panic during the drain used to return before anything looked at
    /// it: no log, and the process exited 0.
    #[tokio::test]
    async fn supervised_counts_a_panic_during_the_drain() {
        let token = CancellationToken::new();
        token.cancel();
        let panics = Arc::new(AtomicUsize::new(0));
        supervised(
            "test",
            "drain-panic".to_string(),
            async { panic!("panic in handle_shutdown") },
            token,
            panics.clone(),
        )
        .await;
        assert_eq!(panics.load(Ordering::Relaxed), 1);
    }

    /// A panicking orchestrator never runs its own exit path, so the
    /// supervisor releases the camera before passing the panic on.
    #[tokio::test]
    async fn release_on_panic_detaches_and_tears_down_then_rethrows() {
        let media = Arc::new(StubMedia::default());
        let signaler = Arc::new(StubSignaler::default());
        let outcome = AssertUnwindSafe(release_on_panic(
            async { panic!("adapter bug while live") },
            CameraId::new("CAM"),
            media.clone(),
            signaler.clone(),
        ))
        .catch_unwind()
        .await;

        assert!(outcome.is_err(), "the panic is passed on to the supervisor");
        assert_eq!(media.detaches.load(Ordering::Relaxed), 1);
        assert_eq!(signaler.teardowns.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn release_on_panic_leaves_a_normal_exit_alone() {
        let media = Arc::new(StubMedia::default());
        let signaler = Arc::new(StubSignaler::default());
        release_on_panic(
            async {},
            CameraId::new("CAM"),
            media.clone(),
            signaler.clone(),
        )
        .await;
        assert_eq!(media.detaches.load(Ordering::Relaxed), 0);
        assert_eq!(signaler.teardowns.load(Ordering::Relaxed), 0);
    }
}
