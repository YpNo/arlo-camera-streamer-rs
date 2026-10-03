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
//! per-camera mailbox is full the event is **dropped** with a `warn!`
//! log. Dropping motion events is preferable to head-of-line blocking
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

use futures::stream::{BoxStream, StreamExt};
use tokio::select;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, trace, warn};

use streamer_domain::camera::CameraId;
use streamer_domain::event::CameraEvent;

/// Routes events from a single shared stream to per-camera mailboxes.
pub struct EventRouter {
    routes: HashMap<CameraId, mpsc::Sender<CameraEvent>>,
}

impl EventRouter {
    /// Construct from a pre-built routing table (`CameraId → mailbox`).
    /// Typically built by [`crate::system::StreamerSystem`].
    #[must_use]
    pub fn new(routes: HashMap<CameraId, mpsc::Sender<CameraEvent>>) -> Self {
        Self { routes }
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
        self,
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

    fn dispatch(&self, event: &CameraEvent) {
        let device_id = event.device_id();
        let Some(tx) = self.routes.get(device_id) else {
            trace!(%device_id, "event for unconfigured camera; dropped");
            return;
        };
        match tx.try_send(event.clone()) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(%device_id, "camera mailbox full; dropping event");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!(%device_id, "camera mailbox closed; dropping event");
            }
        }
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

        let router = EventRouter::new(routes);
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

        let router = EventRouter::new(routes);
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

        let router = EventRouter::new(routes);
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

        let router = EventRouter::new(routes);
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

        let router = EventRouter::new(routes);
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

        let router = EventRouter::new(routes);
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

        let router = EventRouter::new(routes);
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
}
