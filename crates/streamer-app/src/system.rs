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
//! # Channel sizing
//!
//! Each per-camera mailbox is sized at [`MAILBOX_CAPACITY`]. Motion
//! events arrive at most a few per minute on a busy camera, so 32 is
//! ample headroom — the [`EventRouter`] will log and drop on overflow
//! rather than block the shared bus.

#![allow(clippy::similar_names)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

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
        if config.cameras.is_empty() {
            return Err(DomainError::InvalidConfig(
                "no [[cameras]] configured — nothing to do".to_string(),
            ));
        }

        let shutdown = CancellationToken::new();
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
                warn!(
                    camera = %camera_cfg.arlo_device_id,
                    "duplicate [[cameras]] entry; later one wins"
                );
            }
            admin_routes.insert(
                camera_cfg.arlo_device_id.clone(),
                AdminRoute {
                    sender: admin_tx,
                    stream_name: camera_cfg.stream_name.clone(),
                },
            );
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
            handles.push(tokio::spawn(orch.run()));
        }

        let events = event_source.subscribe().await?;
        let router = EventRouter::new(event_routes);
        handles.push(tokio::spawn(router.run(events, shutdown.child_token())));

        let arlo_connected = Arc::new(AtomicBool::new(false));
        let admin = AdminControlActor::new(admin_routes, version, arlo_connected.clone());

        info!(cameras = config.cameras.len(), "streamer system spawned");
        Ok(Self {
            handles,
            shutdown,
            admin,
            arlo_connected,
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

    // Minimal stub adapters for spawn-time wiring tests.
    struct StubEventSource;
    #[async_trait]
    impl ArloEventSource for StubEventSource {
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

    struct StubSignaler;
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
            Arc::new(StubSignaler),
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
            Arc::new(StubSignaler),
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
            Arc::new(StubSignaler),
            Arc::new(StubThumbnails),
            Arc::new(StubMedia::default()),
            Arc::new(StubUserViews),
        )
        .await
        .expect("spawn ok");

        // Cancellation token is shared with internal tasks.
        let token = system.cancellation_token();
        assert!(!token.is_cancelled());

        tokio::time::timeout(std::time::Duration::from_secs(2), system.shutdown())
            .await
            .expect("shutdown timed out");
    }
}
