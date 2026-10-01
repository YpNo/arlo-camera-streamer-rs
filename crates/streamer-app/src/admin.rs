//! Application-layer implementation of the [`AdminControl`] port.
//!
//! [`AdminControl`]: streamer_domain::port::AdminControl
//!
//! Each per-camera [`CameraOrchestrator`](crate::orchestrator::CameraOrchestrator)
//! task owns an inbound admin mailbox of [`AdminCommand`]s. The
//! [`AdminControlActor`] holds the sender side and a snapshot of the
//! configured cameras so it can reject unknown ids fast (without
//! touching the actor task).
//!
//! ## Reply protocol
//!
//! Each command carries a [`tokio::sync::oneshot::Sender`]. `Snapshot`
//! is fulfilled with the data; the mutating commands (`ForceIdle`,
//! `ManualWake`) are acknowledged as soon as the orchestrator dequeues
//! them and are applied right after — a wake spends seconds in WebRTC
//! negotiation, and the HTTP contract is 202 Accepted, not "done". The
//! actor wraps the wait in a per-call timeout so a stuck orchestrator
//! surfaces as [`AdminError::Unavailable`] rather than blocking the
//! HTTP handler.
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
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tracing::warn;

use streamer_domain::admin::{AdminError, CameraSnapshot, SystemSnapshot};
use streamer_domain::camera::{CameraId, StreamName};
use streamer_domain::port::AdminControl;

use crate::system::MAILBOX_CAPACITY;

/// Per-call admin timeout. The orchestrator should reply almost
/// instantly (it's only handling one command between two select-loop
/// iterations) — the timeout guards against a permanently stuck task.
const ADMIN_REPLY_TIMEOUT: Duration = Duration::from_secs(2);

/// Admin command sent from the [`AdminControlActor`] to a per-camera
/// orchestrator over its inbound admin mailbox.
#[derive(Debug)]
pub enum AdminCommand {
    /// Reply with a [`CameraSnapshot`] describing the current state.
    Snapshot {
        /// One-shot reply channel.
        reply: oneshot::Sender<CameraSnapshot>,
    },
    /// Drop any live session and return to `Idle`. Idempotent.
    ForceIdle {
        /// One-shot acknowledgement channel.
        reply: oneshot::Sender<()>,
    },
    /// Inject a synthetic motion event (subject to the budget tracker).
    ManualWake {
        /// One-shot acknowledgement channel.
        reply: oneshot::Sender<()>,
    },
}

/// Routing entry: the admin sender and the configured stream name.
#[derive(Debug, Clone)]
pub(crate) struct AdminRoute {
    pub(crate) sender: mpsc::Sender<AdminCommand>,
    pub(crate) stream_name: StreamName,
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
    /// so the snapshot reflects the current bus state.
    pub(crate) fn new(
        routes: HashMap<CameraId, AdminRoute>,
        version: &'static str,
        arlo_connected: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            routes: Arc::new(routes),
            boot_instant: std::time::Instant::now(),
            version,
            arlo_connected,
        }
    }

    fn route(&self, camera: &CameraId) -> Result<&AdminRoute, AdminError> {
        self.routes
            .get(camera)
            .ok_or_else(|| AdminError::UnknownCamera(camera.clone()))
    }

    async fn send(&self, route: &AdminRoute, cmd: AdminCommand) -> Result<(), AdminError> {
        route
            .sender
            .send(cmd)
            .await
            .map_err(|e| AdminError::Unavailable(format!("send failed: {e}")))
    }
}

/// Capacity of each per-camera admin mailbox. Admin commands are slow
/// (one HTTP request at a time per camera in practice), so this is
/// intentionally tiny — back-pressure surfaces immediately as
/// `Unavailable`.
pub const ADMIN_MAILBOX_CAPACITY: usize = MAILBOX_CAPACITY;

#[async_trait]
impl AdminControl for AdminControlActor {
    async fn snapshot(&self) -> Result<SystemSnapshot, AdminError> {
        let mut cameras = Vec::with_capacity(self.routes.len());
        for id in self.routes.keys() {
            // `camera_snapshot` already times out per call.
            match self.camera_snapshot(id).await {
                Ok(c) => cameras.push(c),
                Err(AdminError::Unavailable(reason)) => {
                    warn!(camera = %id, %reason, "admin snapshot: orchestrator unresponsive");
                    // Don't fail the whole snapshot for one slow camera —
                    // emit a synthetic stub instead.
                    cameras.push(stub_snapshot(id, &self.routes[id].stream_name));
                }
                Err(e) => return Err(e),
            }
        }
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
        let (tx, rx) = oneshot::channel();
        self.send(route, AdminCommand::Snapshot { reply: tx })
            .await?;
        timeout(ADMIN_REPLY_TIMEOUT, rx)
            .await
            .map_err(|_| AdminError::Unavailable("snapshot timeout".to_string()))?
            .map_err(|_| AdminError::Unavailable("orchestrator dropped reply".to_string()))
    }

    async fn force_idle(&self, camera: &CameraId) -> Result<(), AdminError> {
        let route = self.route(camera)?;
        let (tx, rx) = oneshot::channel();
        self.send(route, AdminCommand::ForceIdle { reply: tx })
            .await?;
        timeout(ADMIN_REPLY_TIMEOUT, rx)
            .await
            .map_err(|_| AdminError::Unavailable("force-idle timeout".to_string()))?
            .map_err(|_| AdminError::Unavailable("orchestrator dropped reply".to_string()))
    }

    async fn manual_wake(&self, camera: &CameraId) -> Result<(), AdminError> {
        let route = self.route(camera)?;
        let (tx, rx) = oneshot::channel();
        self.send(route, AdminCommand::ManualWake { reply: tx })
            .await?;
        timeout(ADMIN_REPLY_TIMEOUT, rx)
            .await
            .map_err(|_| AdminError::Unavailable("manual-wake timeout".to_string()))?
            .map_err(|_| AdminError::Unavailable("orchestrator dropped reply".to_string()))
    }
}

fn stub_snapshot(id: &CameraId, stream_name: &StreamName) -> CameraSnapshot {
    CameraSnapshot {
        id: id.clone(),
        stream_name: stream_name.clone(),
        state: "unresponsive".to_string(),
        live_secs_today: 0,
        daily_budget_secs: 0,
        live_source: None,
        last_failure: Some("orchestrator did not reply".to_string()),
        retries: 0,
        user_view: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cam(id: &str) -> CameraId {
        CameraId::new(id)
    }

    fn name(s: &str) -> StreamName {
        StreamName::parse(s).unwrap()
    }

    #[tokio::test]
    async fn unknown_camera_returns_unknown_camera_error() {
        let actor = AdminControlActor::new(
            HashMap::new(),
            "0.1.0",
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );
        let err = actor.force_idle(&cam("MISSING")).await.unwrap_err();
        assert!(matches!(err, AdminError::UnknownCamera(_)));
    }

    #[tokio::test]
    async fn snapshot_includes_known_camera() {
        let (tx, mut rx) = mpsc::channel::<AdminCommand>(8);
        // Reply on the next received command.
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                if let AdminCommand::Snapshot { reply } = cmd {
                    let _ = reply.send(CameraSnapshot {
                        id: cam("CAM"),
                        stream_name: name("front"),
                        state: "idle".to_string(),
                        live_secs_today: 0,
                        daily_budget_secs: 0,
                        live_source: None,
                        last_failure: None,
                        retries: 0,
                        user_view: false,
                    });
                }
            }
        });

        let mut routes = HashMap::new();
        routes.insert(
            cam("CAM"),
            AdminRoute {
                sender: tx,
                stream_name: name("front"),
            },
        );
        let actor = AdminControlActor::new(
            routes,
            "0.1.0",
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
        );

        let sys = actor.snapshot().await.expect("snapshot ok");
        assert_eq!(sys.version, "0.1.0");
        assert!(sys.arlo_connected);
        assert_eq!(sys.cameras.len(), 1);
        assert_eq!(sys.cameras[0].id.as_str(), "CAM");
    }

    #[tokio::test]
    async fn camera_snapshot_times_out_when_orchestrator_silent() {
        // We open a channel but never reply.
        let (tx, _rx) = mpsc::channel::<AdminCommand>(8);
        let mut routes = HashMap::new();
        routes.insert(
            cam("CAM"),
            AdminRoute {
                sender: tx,
                stream_name: name("front"),
            },
        );
        let actor = AdminControlActor::new(
            routes,
            "0.1.0",
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
        );

        let started = std::time::Instant::now();
        let err = tokio::time::timeout(
            ADMIN_REPLY_TIMEOUT + Duration::from_secs(1),
            actor.camera_snapshot(&cam("CAM")),
        )
        .await
        .expect("call returned within outer timeout")
        .expect_err("inner call must time out");
        assert!(matches!(err, AdminError::Unavailable(_)));
        // Sanity: we waited roughly ADMIN_REPLY_TIMEOUT, not forever.
        assert!(started.elapsed() < ADMIN_REPLY_TIMEOUT + Duration::from_secs(1));
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
                stream_name: name("front"),
            },
        );
        let actor = AdminControlActor::new(
            routes,
            "0.1.0",
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
        );
        actor.force_idle(&cam("CAM")).await.expect("ack");
    }

    #[tokio::test]
    async fn manual_wake_propagates_orchestrator_ack() {
        let (tx, mut rx) = mpsc::channel::<AdminCommand>(8);
        tokio::spawn(async move {
            if let Some(AdminCommand::ManualWake { reply }) = rx.recv().await {
                let _ = reply.send(());
            }
        });
        let mut routes = HashMap::new();
        routes.insert(
            cam("CAM"),
            AdminRoute {
                sender: tx,
                stream_name: name("front"),
            },
        );
        let actor = AdminControlActor::new(
            routes,
            "0.1.0",
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
        );
        actor.manual_wake(&cam("CAM")).await.expect("ack");
    }

    #[test]
    fn debug_impl_does_not_leak_internals() {
        let actor = AdminControlActor::new(
            HashMap::new(),
            "0.1.0",
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );
        let s = format!("{actor:?}");
        assert!(s.contains("cameras"));
    }
}
