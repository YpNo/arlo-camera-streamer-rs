//! Single-stream → many-cameras event fan-out.
//!
//! The Arlo event bus delivers one [`CameraEvent`] stream covering all
//! devices. Each [`CameraOrchestrator`](crate::orchestrator::CameraOrchestrator)
//! task owns its own `mpsc::Receiver<CameraEvent>` filtered to its
//! [`CameraId`]. The [`EventRouter`] sits between them: it consumes
//! the shared stream once and dispatches each event to the right
//! orchestrator's mailbox.
//!
//! ## Backpressure policy
//!
//! Routes use `mpsc::Sender::try_send` (non-blocking). If a
//! per-camera mailbox is full the event is **dropped**, counted in
//! `streamer_events_dropped_total`, and reported by a `warn!` at most
//! once per [`DROP_WARN_INTERVAL`] per camera with the count since the
//! last one (the bus sets the volume, so one line per event could flood
//! the log). Dropping motion events is preferable to head-of-line blocking
//! the bus: a missed motion within an active live session is benign
//! (the debouncer is already keeping the camera live), and a missed
//! motion that would have started a session re-fires within seconds
//! on the next pulse from Arlo.
//!
//! Events for unknown devices (configured cameras only) are silently
//! ignored — Arlo accounts often have non-streaming devices (chimes,
//! basestations, doorbells) that emit on the same bus.

#![allow(clippy::single_match_else, clippy::similar_names)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{BoxStream, StreamExt};
use tokio::select;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, trace, warn};

use streamer_domain::camera::CameraId;
use streamer_domain::event::CameraEvent;
use streamer_domain::port::MetricsRecorder;

/// Shortest gap between two "mailbox full" warnings for one camera.
pub const DROP_WARN_INTERVAL: Duration = Duration::from_mins(1);

/// Routes events from a single shared stream to per-camera mailboxes.
pub struct EventRouter {
    routes: HashMap<CameraId, mpsc::Sender<CameraEvent>>,
    metrics: Arc<dyn MetricsRecorder>,
    /// Per camera: the drop warnings' rate limit.
    drops: HashMap<CameraId, DropLog>,
}

/// When a camera's last drop warning was logged, and the drops since.
#[derive(Default)]
struct DropLog {
    last_warn: Option<Instant>,
    dropped: u64,
}

impl EventRouter {
    /// Construct from a pre-built routing table (`CameraId → mailbox`).
    /// Typically built by [`crate::system::StreamerSystem`].
    #[must_use]
    pub fn new(
        routes: HashMap<CameraId, mpsc::Sender<CameraEvent>>,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self {
            routes,
            metrics,
            drops: HashMap::new(),
        }
    }

    /// Drive the router until cancelled or the upstream stream ends.
    ///
    /// An upstream end is fatal for the system: without the bus no camera
    /// ever wakes again, and a daemon that kept answering `/readyz` in that
    /// state would be a zombie. The router cancels `shutdown` (the
    /// system's shared token, not a child) so every orchestrator releases
    /// its session and the process stops.
    #[instrument(skip(self, events, shutdown), fields(cameras = self.routes.len()))]
    pub async fn run(
        mut self,
        mut events: BoxStream<'static, CameraEvent>,
        shutdown: CancellationToken,
    ) {
        loop {
            select! {
                biased;

                () = shutdown.cancelled() => {
                    debug!("shutdown requested");
                    return;
                }
                next = events.next() => match next {
                    Some(event) => self.dispatch(&event),
                    None => {
                        warn!("upstream event stream ended; stopping the system");
                        shutdown.cancel();
                        return;
                    }
                },
            }
        }
    }

    fn dispatch(&mut self, event: &CameraEvent) {
        let device_id = event.device_id();
        let Some(tx) = self.routes.get(device_id) else {
            trace!(%device_id, "event for unconfigured camera; dropped");
            return;
        };
        match tx.try_send(event.clone()) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                let device_id = device_id.clone();
                self.metrics.record_dropped_event(&device_id);
                self.note_drop(&device_id);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!(%device_id, "camera mailbox closed; dropping event");
            }
        }
    }

    /// Count a dropped event and warn about the camera's drops at most once
    /// per [`DROP_WARN_INTERVAL`]. Returns whether it warned.
    fn note_drop(&mut self, device_id: &CameraId) -> bool {
        let now = Instant::now();
        let log = self.drops.entry(device_id.clone()).or_default();
        log.dropped += 1;
        if log
            .last_warn
            .is_some_and(|at| now.duration_since(at) < DROP_WARN_INTERVAL)
        {
            return false;
        }
        warn!(
            %device_id,
            dropped = log.dropped,
            "camera mailbox full; dropping events (reported at most once a minute)"
        );
        log.last_warn = Some(now);
        log.dropped = 0;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn camera(id: &str) -> CameraId {
        CameraId::new(id)
    }

    #[tokio::test]
    async fn dispatches_to_matching_camera() {
        let (tx_a, mut rx_a) = mpsc::channel(8);
        let (tx_b, mut rx_b) = mpsc::channel(8);
        let mut routes = HashMap::new();
        routes.insert(camera("A"), tx_a);
        routes.insert(camera("B"), tx_b);

        let mut router = EventRouter::new(routes, Arc::new(crate::metrics_noop::NoopRecorder));
        router.dispatch(&CameraEvent::Motion {
            device_id: camera("A"),
        });
        router.dispatch(&CameraEvent::Motion {
            device_id: camera("B"),
        });

        let got_a = rx_a.recv().await.expect("A received");
        let got_b = rx_b.recv().await.expect("B received");
        assert_eq!(got_a.device_id(), &camera("A"));
        assert_eq!(got_b.device_id(), &camera("B"));
    }

    #[tokio::test]
    async fn drops_events_for_unconfigured_cameras() {
        let (tx_a, mut rx_a) = mpsc::channel(8);
        let mut routes = HashMap::new();
        routes.insert(camera("A"), tx_a);

        let mut router = EventRouter::new(routes, Arc::new(crate::metrics_noop::NoopRecorder));
        router.dispatch(&CameraEvent::Motion {
            device_id: camera("UNKNOWN"),
        });
        router.dispatch(&CameraEvent::Motion {
            device_id: camera("A"),
        });

        let got = rx_a.recv().await.expect("A received");
        assert_eq!(got.device_id(), &camera("A"));
        // No second event should be queued.
        assert!(rx_a.try_recv().is_err());
    }

    #[tokio::test]
    async fn drops_events_when_mailbox_full() {
        let (tx_a, mut rx_a) = mpsc::channel(1);
        let mut routes = HashMap::new();
        routes.insert(camera("A"), tx_a);

        let mut router = EventRouter::new(routes, Arc::new(crate::metrics_noop::NoopRecorder));
        // First fits; second overflows.
        router.dispatch(&CameraEvent::Motion {
            device_id: camera("A"),
        });
        router.dispatch(&CameraEvent::Audio {
            device_id: camera("A"),
        });

        let first = rx_a.recv().await.expect("first ok");
        assert!(matches!(first, CameraEvent::Motion { .. }));
        // The Audio event was dropped (mailbox full at second send).
        assert!(rx_a.try_recv().is_err());
    }

    #[tokio::test]
    async fn drops_events_when_mailbox_closed() {
        let (tx_a, rx_a) = mpsc::channel(8);
        drop(rx_a); // close the receiver
        let mut routes = HashMap::new();
        routes.insert(camera("A"), tx_a);

        let mut router = EventRouter::new(routes, Arc::new(crate::metrics_noop::NoopRecorder));
        // Should not panic; just logs at debug.
        router.dispatch(&CameraEvent::Motion {
            device_id: camera("A"),
        });
    }

    #[tokio::test]
    async fn run_exits_when_stream_ends() {
        let (tx_a, _rx_a) = mpsc::channel(8);
        let mut routes = HashMap::new();
        routes.insert(camera("A"), tx_a);

        let router = EventRouter::new(routes, Arc::new(crate::metrics_noop::NoopRecorder));
        let stream: BoxStream<'static, CameraEvent> = Box::pin(futures::stream::empty());
        let shutdown = CancellationToken::new();

        // Should return promptly when the stream ends, and take the
        // system down with it.
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            router.run(stream, shutdown.clone()),
        )
        .await
        .expect("router exited within timeout");
        assert!(shutdown.is_cancelled(), "an upstream end stops the system");
    }

    #[tokio::test]
    async fn run_exits_when_cancelled() {
        let (tx_a, _rx_a) = mpsc::channel(8);
        let mut routes = HashMap::new();
        routes.insert(camera("A"), tx_a);

        let router = EventRouter::new(routes, Arc::new(crate::metrics_noop::NoopRecorder));
        let stream: BoxStream<'static, CameraEvent> = Box::pin(futures::stream::pending());
        let shutdown = CancellationToken::new();

        let token = shutdown.clone();
        let handle = tokio::spawn(router.run(stream, shutdown));
        token.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("router exited within timeout")
            .expect("task did not panic");
    }

    #[tokio::test]
    async fn run_dispatches_events_from_stream() {
        let (tx_a, mut rx_a) = mpsc::channel(8);
        let mut routes = HashMap::new();
        routes.insert(camera("A"), tx_a);

        let events = vec![
            CameraEvent::Motion {
                device_id: camera("A"),
            },
            CameraEvent::Audio {
                device_id: camera("A"),
            },
        ];
        let stream: BoxStream<'static, CameraEvent> =
            Box::pin(futures::stream::iter(events.into_iter()));
        let shutdown = CancellationToken::new();

        let router = EventRouter::new(routes, Arc::new(crate::metrics_noop::NoopRecorder));
        let handle = tokio::spawn(router.run(stream, shutdown));

        let got1 = rx_a.recv().await.expect("first");
        assert!(matches!(got1, CameraEvent::Motion { .. }));
        let got2 = rx_a.recv().await.expect("second");
        assert!(matches!(got2, CameraEvent::Audio { .. }));

        // Stream ends → router exits naturally.
        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("router exited within timeout")
            .expect("task did not panic");
    }

    /// Counts drops for the metric.
    #[derive(Default)]
    struct DropCounter(std::sync::atomic::AtomicU32);

    impl MetricsRecorder for DropCounter {
        fn record_state_change(
            &self,
            _camera: &CameraId,
            _from: &streamer_domain::state::CameraState,
            _to: &streamer_domain::state::CameraState,
            _signal: &str,
        ) {
        }
        fn record_motion(&self, _camera: &CameraId, _outcome: streamer_domain::MotionOutcome) {}
        fn record_budget(&self, _camera: &CameraId, _decision: streamer_domain::BudgetDecision) {}
        fn record_splice(
            &self,
            _camera: &CameraId,
            _outcome: streamer_domain::SpliceOutcome,
            _latency_ms: u64,
        ) {
        }
        fn record_failure(&self, _camera: &CameraId, _retries: u32) {}
        fn record_dropped_event(&self, _camera: &CameraId) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// One warn per dropped event used to flood the log during a burst.
    #[tokio::test]
    async fn full_mailbox_drops_are_counted_and_warned_about_once_per_interval() {
        let (tx_a, _rx_a) = mpsc::channel(1);
        let mut routes = HashMap::new();
        routes.insert(camera("A"), tx_a);
        let metrics = Arc::new(DropCounter::default());
        let mut router = EventRouter::new(routes, metrics.clone());
        let motion = CameraEvent::Motion {
            device_id: camera("A"),
        };

        router.dispatch(&motion); // fills the mailbox
        for _ in 0..5 {
            router.dispatch(&motion);
        }

        assert_eq!(metrics.0.load(std::sync::atomic::Ordering::Relaxed), 5);
        assert!(!router.note_drop(&camera("A")), "still within the interval");
        assert!(
            router.note_drop(&camera("B")),
            "another camera reports at once"
        );
    }
}
