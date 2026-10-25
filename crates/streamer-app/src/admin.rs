//! Application-layer implementation of the [`AdminControl`] port.
//!
//! [`AdminControl`]: streamer_domain::port::AdminControl
//!
//! Each per-camera [`CameraOrchestrator`](crate::orchestrator::CameraOrchestrator)
//! task owns an inbound admin mailbox of [`AdminCommand`]s and publishes
//! its [`CameraSnapshot`] on a [`tokio::sync::watch`] channel. The
//! [`AdminControlActor`] holds the sender side and the snapshot receiver
//! of every configured camera, so it rejects unknown ids fast and reads
//! state without touching the actor task.
//!
//! ## Snapshots and the reply protocol
//!
//! Snapshots are **read**, never requested: the orchestrator publishes
//! one after every loop iteration, and a camera inside a WebRTC
//! negotiation (seconds) still reports `activating` instantly instead of
//! timing out. The mutating commands (`ForceIdle`, `ManualWake`) carry a
//! [`tokio::sync::oneshot::Sender`] and are acknowledged as soon as the
//! orchestrator dequeues them, then applied — the HTTP contract is 202
//! Accepted, not "done". The actor wraps that wait in a per-call timeout
//! so a stuck orchestrator surfaces as [`AdminError::Unavailable`]
//! rather than blocking the HTTP handler.
//!
//! ## Why an mpsc actor instead of `Arc<RwLock<…>>`?
//!
//! The orchestrator already owns its state without a lock — every
//! mutation runs serialized inside the task's `tokio::select!`. Adding
//! a `RwLock` to expose the state to the admin layer would create a
//! second mutation path and re-introduce the very races the actor
//! pattern eliminates.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;

use bytes::Bytes;
use streamer_domain::admin::{AdminError, CameraSnapshot, SystemSnapshot};
use streamer_domain::camera::CameraId;
use streamer_domain::error::DomainError;
use streamer_domain::port::{AdminControl, MediaMultiplexer};

/// Per-call admin timeout. The orchestrator should reply almost
/// instantly (it's only handling one command between two select-loop
/// iterations) — the timeout guards against a permanently stuck task.
const ADMIN_REPLY_TIMEOUT: Duration = Duration::from_secs(2);

/// The orchestrator's answer to a manual wake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeOutcome {
    /// Taken; it goes through the same guards as a motion pulse.
    Accepted,
    /// Refused: the previous session ended too recently.
    TooSoon {
        /// How long until a wake is accepted again.
        retry_in: std::time::Duration,
    },
}

/// Admin command sent from the [`AdminControlActor`] to a per-camera
/// orchestrator over its inbound admin mailbox.
#[derive(Debug)]
pub enum AdminCommand {
    /// Drop any live session and return to `Idle`. Idempotent.
    ForceIdle {
        /// One-shot acknowledgement channel.
        reply: oneshot::Sender<()>,
    },
    /// Inject a synthetic motion event (subject to the budget tracker and
    /// the re-activation interval).
    ManualWake {
        /// Whether the wake was taken or refused.
        reply: oneshot::Sender<WakeOutcome>,
    },
}

/// Routing entry: the admin sender and the orchestrator's published
/// snapshot (which carries the stream name).
#[derive(Debug, Clone)]
pub(crate) struct AdminRoute {
    pub(crate) sender: mpsc::Sender<AdminCommand>,
    pub(crate) snapshots: watch::Receiver<CameraSnapshot>,
}

/// Application-layer implementation of [`AdminControl`].
///
/// The actor is just a thin dispatcher — all the real work happens in
/// the per-camera orchestrator tasks. Construct it via
/// [`StreamerSystem::admin_control`](crate::system::StreamerSystem::admin_control)
/// after the system has spawned.
#[derive(Clone)]
pub struct AdminControlActor {
    routes: Arc<HashMap<CameraId, AdminRoute>>,
    boot_instant: std::time::Instant,
    version: &'static str,
    arlo_connected: Arc<std::sync::atomic::AtomicBool>,
    /// Read for the idle frame's thumbnail; never asked to act.
    media: Arc<dyn MediaMultiplexer>,
}

impl std::fmt::Debug for AdminControlActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // We deliberately omit `routes` (verbose, internal) and
        // `arlo_connected` (atomic; not stably formattable). The
        // visible fields are enough for log breadcrumbs.
        f.debug_struct("AdminControlActor")
            .field("cameras", &self.routes.len())
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

impl AdminControlActor {
    /// Construct from a routing table built by `StreamerSystem`.
    /// `arlo_connected` is shared with the connection-status watcher
    /// so the snapshot reflects the current bus state; `media` answers
    /// the thumbnail reads.
    pub(crate) fn new(
        routes: HashMap<CameraId, AdminRoute>,
        version: &'static str,
        arlo_connected: Arc<std::sync::atomic::AtomicBool>,
        media: Arc<dyn MediaMultiplexer>,
    ) -> Self {
        Self {
            routes: Arc::new(routes),
            boot_instant: std::time::Instant::now(),
            version,
            arlo_connected,
            media,
        }
    }

    fn route(&self, camera: &CameraId) -> Result<&AdminRoute, AdminError> {
        self.routes
            .get(camera)
            .ok_or_else(|| AdminError::UnknownCamera(camera.clone()))
    }

    /// Enqueue without waiting: a full mailbox is the documented
    /// back-pressure signal ([`ADMIN_MAILBOX_CAPACITY`]), not something an
    /// HTTP handler should block on.
    fn send(route: &AdminRoute, cmd: AdminCommand) -> Result<(), AdminError> {
        use mpsc::error::TrySendError;
        route.sender.try_send(cmd).map_err(|e| match e {
            TrySendError::Full(_) => AdminError::Unavailable("admin mailbox full".to_string()),
            TrySendError::Closed(_) => AdminError::Unavailable("orchestrator gone".to_string()),
        })
    }
}

/// Capacity of each per-camera admin mailbox. Admin commands are slow
/// (one HTTP request at a time per camera in practice), so this is
/// intentionally tiny — back-pressure surfaces immediately as
/// `Unavailable` instead of a queue of wakes applied long after their
/// callers gave up.
pub const ADMIN_MAILBOX_CAPACITY: usize = 2;

#[async_trait]
impl AdminControl for AdminControlActor {
    async fn snapshot(&self) -> Result<SystemSnapshot, AdminError> {
        let mut cameras: Vec<CameraSnapshot> = self
            .routes
            .values()
            .map(|route| route.snapshots.borrow().clone())
            .collect();
        cameras.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
        Ok(SystemSnapshot {
            version: self.version.to_string(),
            uptime_secs: self.boot_instant.elapsed().as_secs(),
            arlo_connected: self
                .arlo_connected
                .load(std::sync::atomic::Ordering::Relaxed),
            cameras,
        })
    }

    async fn camera_snapshot(&self, camera: &CameraId) -> Result<CameraSnapshot, AdminError> {
        let route = self.route(camera)?;
        Ok(route.snapshots.borrow().clone())
    }

    async fn force_idle(&self, camera: &CameraId) -> Result<(), AdminError> {
        let route = self.route(camera)?;
        let (tx, rx) = oneshot::channel();
        Self::send(route, AdminCommand::ForceIdle { reply: tx })?;
        timeout(ADMIN_REPLY_TIMEOUT, rx)
            .await
            .map_err(|_| AdminError::Unavailable("force-idle timeout".to_string()))?
            .map_err(|_| AdminError::Unavailable("orchestrator dropped reply".to_string()))
    }

    async fn thumbnail(&self, camera: &CameraId) -> Result<Option<Bytes>, AdminError> {
        self.route(camera)?;
        self.media
            .current_thumbnail(camera)
            .await
            .map_err(|e| match e {
                DomainError::UnknownCamera(_) => AdminError::UnknownCamera(camera.clone()),
                other => AdminError::Internal(other.to_string()),
            })
    }

    async fn manual_wake(&self, camera: &CameraId) -> Result<(), AdminError> {
        let route = self.route(camera)?;
        let (tx, rx) = oneshot::channel();
        Self::send(route, AdminCommand::ManualWake { reply: tx })?;
        let outcome = timeout(ADMIN_REPLY_TIMEOUT, rx)
            .await
            .map_err(|_| AdminError::Unavailable("manual-wake timeout".to_string()))?
            .map_err(|_| AdminError::Unavailable("orchestrator dropped reply".to_string()))?;
        match outcome {
            WakeOutcome::Accepted => Ok(()),
            WakeOutcome::TooSoon { retry_in } => Err(AdminError::RateLimited(format!(
                "last session ended too recently; retry in {} s",
                retry_in.as_secs()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use streamer_domain::camera::StreamName;

    fn cam(id: &str) -> CameraId {
        CameraId::new(id)
    }

    fn name(s: &str) -> StreamName {
        StreamName::parse(s).unwrap()
    }

    fn snapshot_of(id: &str, state: &str) -> CameraSnapshot {
        CameraSnapshot {
            id: cam(id),
            stream_name: name("front"),
            state: state.to_string(),
            live_secs_today: 0,
            daily_budget_secs: 0,
            live_source: None,
            last_failure: None,
            retries: 0,
            user_view: false,
        }
    }

    /// Holds the idle frames' thumbnails; nothing else is asked of it.
    #[derive(Default)]
    struct ThumbMedia {
        thumbs: HashMap<CameraId, Bytes>,
        fails: bool,
    }

    #[async_trait]
    impl MediaMultiplexer for ThumbMedia {
        async fn register(&self, _camera: &CameraId) -> Result<(), DomainError> {
            Ok(())
        }
        async fn attach_live(
            &self,
            _camera: &CameraId,
            _signaler: &dyn streamer_domain::port::WebrtcSignaler,
        ) -> Result<streamer_domain::stream::LiveSession, DomainError> {
            Err(DomainError::AdapterTransport("not in this stub".into()))
        }
        async fn attach_user_view(
            &self,
            _camera: &CameraId,
            _url: &streamer_domain::stream::WatchAlongUrl,
        ) -> Result<streamer_domain::stream::LiveSession, DomainError> {
            Err(DomainError::AdapterTransport("not in this stub".into()))
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
        async fn current_thumbnail(&self, camera: &CameraId) -> Result<Option<Bytes>, DomainError> {
            if self.fails {
                return Err(DomainError::AdapterTransport("registry gone".into()));
            }
            Ok(self.thumbs.get(camera).cloned())
        }
        async fn set_user_view_notice(
            &self,
            _camera: &CameraId,
            _shown: bool,
        ) -> Result<(), DomainError> {
            Ok(())
        }
    }

    fn media() -> Arc<dyn MediaMultiplexer> {
        Arc::new(ThumbMedia::default())
    }

    fn actor_with(media: ThumbMedia, cameras: &[&str]) -> AdminControlActor {
        let routes = cameras
            .iter()
            .map(|id| (cam(id), silent_route(id, "idle").0))
            .collect();
        AdminControlActor::new(
            routes,
            "0.3.0",
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
            Arc::new(media),
        )
    }

    #[tokio::test]
    async fn thumbnail_is_the_idle_frames_for_a_configured_camera() {
        let jpeg = Bytes::from_static(b"\xff\xd8jpeg");
        let media = ThumbMedia {
            thumbs: HashMap::from([(cam("CAM-A"), jpeg.clone())]),
            ..ThumbMedia::default()
        };
        let actor = actor_with(media, &["CAM-A", "CAM-B"]);

        assert_eq!(actor.thumbnail(&cam("CAM-A")).await.unwrap(), Some(jpeg));
        assert_eq!(actor.thumbnail(&cam("CAM-B")).await.unwrap(), None);
    }

    /// An id outside the config is refused before the media is asked.
    #[tokio::test]
    async fn thumbnail_of_an_unconfigured_camera_is_unknown() {
        let media = ThumbMedia {
            thumbs: HashMap::from([(cam("GHOST"), Bytes::from_static(b"x"))]),
            ..ThumbMedia::default()
        };
        let actor = actor_with(media, &["CAM-A"]);

        let err = actor.thumbnail(&cam("GHOST")).await.unwrap_err();
        assert!(matches!(err, AdminError::UnknownCamera(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_media_failure_is_an_internal_error() {
        let media = ThumbMedia {
            fails: true,
            ..ThumbMedia::default()
        };
        let actor = actor_with(media, &["CAM-A"]);

        let err = actor.thumbnail(&cam("CAM-A")).await.unwrap_err();
        assert!(matches!(err, AdminError::Internal(_)), "{err:?}");
    }

    /// A route whose orchestrator is silent: the admin mailbox is never
    /// read, the snapshot is whatever was last published.
    fn silent_route(id: &str, state: &str) -> (AdminRoute, mpsc::Receiver<AdminCommand>) {
        let (tx, rx) = mpsc::channel::<AdminCommand>(8);
        let (_snap_tx, snapshots) = watch::channel(snapshot_of(id, state));
        (
            AdminRoute {
                sender: tx,
                snapshots,
            },
            rx,
        )
    }

    #[tokio::test]
    async fn unknown_camera_returns_unknown_camera_error() {
        let actor = AdminControlActor::new(
            HashMap::new(),
            "0.1.0",
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            media(),
        );
        let err = actor.force_idle(&cam("MISSING")).await.unwrap_err();
        assert!(matches!(err, AdminError::UnknownCamera(_)));
    }

    #[tokio::test]
    async fn snapshot_includes_known_cameras_sorted_by_id() {
        let (route_b, _rx_b) = silent_route("CAM-B", "idle");
        let (route_a, _rx_a) = silent_route("CAM-A", "activating");
        let mut routes = HashMap::new();
        routes.insert(cam("CAM-B"), route_b);
        routes.insert(cam("CAM-A"), route_a);
        let actor = AdminControlActor::new(
            routes,
            "0.1.0",
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
            media(),
        );

        let sys = actor.snapshot().await.expect("snapshot ok");
        assert_eq!(sys.version, "0.1.0");
        assert!(sys.arlo_connected);
        let ids: Vec<&str> = sys.cameras.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, vec!["CAM-A", "CAM-B"]);
        assert_eq!(sys.cameras[0].state, "activating");
    }

    /// A camera deep inside a WebRTC negotiation cannot answer a mailbox
    /// command for seconds; its published snapshot is read at once.
    #[tokio::test]
    async fn camera_snapshot_reads_the_published_state_without_waiting_on_the_task() {
        let (route, _rx) = silent_route("CAM", "activating");
        let mut routes = HashMap::new();
        routes.insert(cam("CAM"), route);
        let actor = AdminControlActor::new(
            routes,
            "0.1.0",
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
            media(),
        );

        let started = std::time::Instant::now();
        let snap = actor.camera_snapshot(&cam("CAM")).await.expect("published");
        assert_eq!(snap.state, "activating");
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn camera_snapshot_follows_the_orchestrator_updates() {
        let (tx, _rx) = mpsc::channel::<AdminCommand>(8);
        let (snap_tx, snapshots) = watch::channel(snapshot_of("CAM", "idle"));
        let mut routes = HashMap::new();
        routes.insert(
            cam("CAM"),
            AdminRoute {
                sender: tx,
                snapshots,
            },
        );
        let actor = AdminControlActor::new(
            routes,
            "0.1.0",
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            media(),
        );

        assert_eq!(
            actor.camera_snapshot(&cam("CAM")).await.unwrap().state,
            "idle"
        );
        snap_tx.send_replace(snapshot_of("CAM", "live"));
        assert_eq!(
            actor.camera_snapshot(&cam("CAM")).await.unwrap().state,
            "live"
        );
    }

    #[tokio::test]
    async fn force_idle_propagates_orchestrator_ack() {
        let (tx, mut rx) = mpsc::channel::<AdminCommand>(8);
        tokio::spawn(async move {
            if let Some(AdminCommand::ForceIdle { reply }) = rx.recv().await {
                let _ = reply.send(());
            }
        });
        let mut routes = HashMap::new();
        routes.insert(
            cam("CAM"),
            AdminRoute {
                sender: tx,
                snapshots: watch::channel(snapshot_of("CAM", "idle")).1,
            },
        );
        let actor = AdminControlActor::new(
            routes,
            "0.1.0",
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
            media(),
        );
        actor.force_idle(&cam("CAM")).await.expect("ack");
    }

    #[tokio::test]
    async fn manual_wake_propagates_orchestrator_ack() {
        let (tx, mut rx) = mpsc::channel::<AdminCommand>(8);
        tokio::spawn(async move {
            if let Some(AdminCommand::ManualWake { reply }) = rx.recv().await {
                let _ = reply.send(WakeOutcome::Accepted);
            }
        });
        let mut routes = HashMap::new();
        routes.insert(
            cam("CAM"),
            AdminRoute {
                sender: tx,
                snapshots: watch::channel(snapshot_of("CAM", "idle")).1,
            },
        );
        let actor = AdminControlActor::new(
            routes,
            "0.1.0",
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
            media(),
        );
        actor.manual_wake(&cam("CAM")).await.expect("ack");
    }

    #[test]
    fn debug_impl_does_not_leak_internals() {
        let actor = AdminControlActor::new(
            HashMap::new(),
            "0.1.0",
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            media(),
        );
        let s = format!("{actor:?}");
        assert!(s.contains("cameras"));
    }

    #[tokio::test]
    async fn manual_wake_refusal_maps_to_rate_limited() {
        let (tx, mut rx) = mpsc::channel::<AdminCommand>(8);
        tokio::spawn(async move {
            if let Some(AdminCommand::ManualWake { reply }) = rx.recv().await {
                let _ = reply.send(WakeOutcome::TooSoon {
                    retry_in: Duration::from_secs(12),
                });
            }
        });
        let mut routes = HashMap::new();
        routes.insert(
            CameraId::new("CAM"),
            AdminRoute {
                sender: tx,
                snapshots: watch::channel(snapshot_of("CAM", "idle")).1,
            },
        );
        let actor = AdminControlActor::new(
            routes,
            "test",
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            media(),
        );
        let err = actor.manual_wake(&CameraId::new("CAM")).await.unwrap_err();
        assert!(
            matches!(err, AdminError::RateLimited(ref m) if m.contains("12 s")),
            "{err}"
        );
    }
}
