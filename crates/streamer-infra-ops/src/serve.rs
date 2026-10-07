//! Listener loop shared by the ops and admin servers.
//!
//! `axum::serve` runs hyper without a timer, so hyper's header-read
//! timeout never fires and a peer that opens a connection and sends
//! nothing holds it for ever; it also accepts without bound. This loop
//! runs hyper with a timer and [`ServeLimits::header_read_timeout`], and
//! refuses connections past [`ServeLimits::max_connections`]. It speaks
//! HTTP/1 only: the header-read timeout covers every connection then,
//! whereas an HTTP/2 preface followed by silence would hold a permit
//! with no timeout at all, and nothing here needs HTTP/2. Both listeners
//! bind loopback by default; the limits matter the day one is exposed.
//!
//! One peer must not be able to take every slot: each connection lives at
//! most [`ServeLimits::max_connection_age`] (a client that pipelines and
//! never reads its replies held one for good, since the header timer is
//! not armed while hyper waits to flush), a peer holds at most
//! [`ServeLimits::max_per_peer`], and [`ServeLimits::loopback_reserve`]
//! slots are kept for loopback peers, so the container's own health check
//! still gets in when the shared slots are full.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::error::OpsError;

/// How long open connections get to finish after the shutdown signal.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
/// Pause after a failed `accept` (out of descriptors, a reset peer)
/// before trying again, so the loop never spins.
const ACCEPT_RETRY: Duration = Duration::from_millis(200);
/// Shortest gap between two "connections refused" warnings.
const SATURATION_WARN_INTERVAL: Duration = Duration::from_mins(1);

/// Bounds on one listener.
#[derive(Debug, Clone, Copy)]
pub struct ServeLimits {
    /// A connection that has not sent a full request head by then is
    /// closed.
    pub header_read_timeout: Duration,
    /// Connections open at once; further ones are closed on accept.
    pub max_connections: usize,
    /// Connections one remote address may hold at once. Loopback peers
    /// are not capped: they are this host.
    pub max_per_peer: usize,
    /// Extra slots only loopback peers may use once the shared ones are
    /// taken (the container's own `HEALTHCHECK`).
    pub loopback_reserve: usize,
    /// A connection is closed this long after it was accepted, whatever
    /// it is doing. Every request here is answered in milliseconds.
    pub max_connection_age: Duration,
}

impl Default for ServeLimits {
    fn default() -> Self {
        Self {
            header_read_timeout: Duration::from_secs(10),
            max_connections: 64,
            max_per_peer: 8,
            loopback_reserve: 4,
            max_connection_age: Duration::from_secs(30),
        }
    }
}

/// Connections held per remote address.
type PeerCounts = Arc<Mutex<HashMap<IpAddr, usize>>>;

/// The listener's connection slots: shared ones, a loopback reserve, and
/// per-peer counts.
struct Slots {
    shared: Arc<Semaphore>,
    reserve: Arc<Semaphore>,
    max_per_peer: usize,
    per_peer: PeerCounts,
}

/// One connection's slot; released when dropped.
struct Slot {
    _permit: OwnedSemaphorePermit,
    peer: Option<(PeerCounts, IpAddr)>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        if let Some((counts, ip)) = &self.peer {
            let mut counts = counts.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(n) = counts.get_mut(ip) {
                *n -= 1;
                if *n == 0 {
                    counts.remove(ip);
                }
            }
        }
    }
}

impl Slots {
    fn new(limits: &ServeLimits) -> Self {
        Self {
            shared: Arc::new(Semaphore::new(limits.max_connections)),
            reserve: Arc::new(Semaphore::new(limits.loopback_reserve)),
            max_per_peer: limits.max_per_peer,
            per_peer: Arc::default(),
        }
    }

    /// A slot for a connection from `ip`, or `None` when it must be
    /// refused.
    fn try_acquire(&self, ip: IpAddr) -> Option<Slot> {
        if ip.is_loopback() {
            let permit = self
                .shared
                .clone()
                .try_acquire_owned()
                .or_else(|_| self.reserve.clone().try_acquire_owned())
                .ok()?;
            return Some(Slot {
                _permit: permit,
                peer: None,
            });
        }
        let mut counts = self.per_peer.lock().unwrap_or_else(PoisonError::into_inner);
        let held = counts.get(&ip).copied().unwrap_or(0);
        if held >= self.max_per_peer {
            return None;
        }
        let permit = self.shared.clone().try_acquire_owned().ok()?;
        counts.insert(ip, held + 1);
        Some(Slot {
            _permit: permit,
            peer: Some((self.per_peer.clone(), ip)),
        })
    }
}

/// Bind `addr` for one of the servers. Called at boot, before anything is
/// spawned, so a port already in use fails the daemon instead of leaving
/// it running without its health or admin endpoint.
///
/// # Errors
///
/// [`OpsError::Bind`] naming the address.
pub async fn bind(addr: SocketAddr) -> Result<TcpListener, OpsError> {
    TcpListener::bind(addr)
        .await
        .map_err(|source| OpsError::Bind { addr, source })
}

/// Serve `app` on `listener` until `shutdown` is cancelled, then drain
/// the open connections for at most [`DRAIN_TIMEOUT`].
///
/// # Errors
///
/// [`OpsError::Serve`] when the listener's address cannot be read.
pub(crate) async fn serve(
    name: &'static str,
    listener: TcpListener,
    app: Router,
    limits: ServeLimits,
    shutdown: CancellationToken,
) -> Result<(), OpsError> {
    let addr: SocketAddr = listener.local_addr().map_err(OpsError::Serve)?;
    info!(%addr, server = name, "HTTP server listening");

    let mut builder = auto::Builder::new(TokioExecutor::new()).http1_only();
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(limits.header_read_timeout);
    let graceful = GracefulShutdown::new();
    let slots = Slots::new(&limits);
    let mut last_saturation_warn: Option<Instant> = None;

    loop {
        tokio::select! {
            biased;

            () = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(pair) => pair,
                    Err(e) => {
                        warn!(server = name, error = %e, "accept failed; retrying");
                        tokio::time::sleep(ACCEPT_RETRY).await;
                        continue;
                    }
                };
                let Some(slot) = slots.try_acquire(peer.ip()) else {
                    let now = Instant::now();
                    if last_saturation_warn.is_none_or(|at| now.duration_since(at) >= SATURATION_WARN_INTERVAL) {
                        warn!(server = name, %peer, "connection refused: connection limit reached (reported at most once a minute)");
                        last_saturation_warn = Some(now);
                    } else {
                        debug!(server = name, %peer, "connection refused: limit reached");
                    }
                    drop(stream);
                    continue;
                };
                // The peer's address for the handlers (axum's `ConnectInfo`).
                let app = app.clone().layer(axum::Extension(axum::extract::ConnectInfo(peer)));
                let service = TowerToHyperService::new(app);
                let conn = builder.serve_connection(TokioIo::new(stream), service).into_owned();
                let conn = graceful.watch(conn);
                let age = limits.max_connection_age;
                tokio::spawn(async move {
                    match tokio::time::timeout(age, conn).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => debug!(server = name, %peer, error = %e, "connection ended with an error"),
                        Err(_) => debug!(server = name, %peer, "connection closed at its maximum age"),
                    }
                    drop(slot);
                });
            }
        }
    }

    info!(server = name, "HTTP server shutting down");
    if tokio::time::timeout(DRAIN_TIMEOUT, graceful.shutdown())
        .await
        .is_err()
    {
        warn!(
            server = name,
            "connections still open after the drain timeout"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    async fn start(limits: ServeLimits) -> (SocketAddr, CancellationToken) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().route("/ping", get(|| async { "pong" }));
        let shutdown = CancellationToken::new();
        tokio::spawn(serve("test", listener, app, limits, shutdown.clone()));
        (addr, shutdown)
    }

    async fn read_to_end(sock: &mut TcpStream) -> Vec<u8> {
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(3), sock.read_to_end(&mut buf)).await;
        buf
    }

    #[tokio::test]
    async fn serve_answers_a_request() {
        let (addr, shutdown) = start(ServeLimits::default()).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"GET /ping HTTP/1.0\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let reply = read_to_end(&mut sock).await;
        let text = String::from_utf8_lossy(&reply);
        assert!(text.starts_with("HTTP/1.0 200"), "{text}");
        assert!(text.ends_with("pong"), "{text}");
        shutdown.cancel();
    }

    #[tokio::test]
    async fn serve_closes_a_connection_that_sends_no_request_head() {
        let limits = ServeLimits {
            header_read_timeout: Duration::from_millis(300),
            ..ServeLimits::default()
        };
        let (addr, shutdown) = start(limits).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"GET /ping HTTP/1.1\r\nHost: x")
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let reply = read_to_end(&mut sock).await;
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "closed by the timeout"
        );
        assert!(
            reply.is_empty() || String::from_utf8_lossy(&reply).contains("408"),
            "{:?}",
            String::from_utf8_lossy(&reply)
        );
        shutdown.cancel();
    }

    /// An HTTP/2 connection preface must not open a timeout-free session:
    /// the listener is HTTP/1 only, so the preface is a bad request line
    /// and the connection is answered or closed within the header timeout.
    #[tokio::test]
    async fn serve_does_not_keep_an_http2_preface_open() {
        let limits = ServeLimits {
            header_read_timeout: Duration::from_millis(300),
            ..ServeLimits::default()
        };
        let (addr, shutdown) = start(limits).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let _reply = read_to_end(&mut sock).await;
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the connection must end, not idle"
        );
        shutdown.cancel();
    }

    #[tokio::test]
    async fn serve_refuses_connections_past_the_cap() {
        let limits = ServeLimits {
            max_connections: 1,
            loopback_reserve: 0,
            ..ServeLimits::default()
        };
        let (addr, shutdown) = start(limits).await;
        let _held = TcpStream::connect(addr).await.unwrap();
        // Give the accept loop a turn to take the permit.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut refused = TcpStream::connect(addr).await.unwrap();
        refused
            .write_all(b"GET /ping HTTP/1.0\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let reply = read_to_end(&mut refused).await;
        assert!(reply.is_empty(), "{:?}", String::from_utf8_lossy(&reply));
        shutdown.cancel();
    }

    /// A client that never lets its replies drain held a slot for good:
    /// the header timer is not armed while hyper waits to flush.
    #[tokio::test]
    async fn serve_closes_a_connection_at_its_maximum_age() {
        let limits = ServeLimits {
            header_read_timeout: Duration::from_secs(10),
            max_connection_age: Duration::from_millis(300),
            ..ServeLimits::default()
        };
        let (addr, shutdown) = start(limits).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"GET /ping HTTP/1.1\r\nHost: x")
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let _ = read_to_end(&mut sock).await;
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "closed at the age limit, long before the 10 s header timeout"
        );
        shutdown.cancel();
    }

    /// With the shared slots taken, a loopback peer (the container's own
    /// health check) still gets in through the reserve.
    #[tokio::test]
    async fn serve_keeps_a_reserve_for_loopback_peers() {
        let limits = ServeLimits {
            max_connections: 1,
            loopback_reserve: 1,
            ..ServeLimits::default()
        };
        let (addr, shutdown) = start(limits).await;
        let _held = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut health = TcpStream::connect(addr).await.unwrap();
        health
            .write_all(b"GET /ping HTTP/1.0\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let reply = read_to_end(&mut health).await;
        assert!(String::from_utf8_lossy(&reply).ends_with("pong"));
        shutdown.cancel();
    }

    #[test]
    fn slots_cap_each_remote_peer_and_keep_the_reserve_for_loopback() {
        let slots = Slots::new(&ServeLimits {
            max_connections: 3,
            max_per_peer: 2,
            loopback_reserve: 1,
            ..ServeLimits::default()
        });
        let a: IpAddr = "192.0.2.1".parse().unwrap();
        let b: IpAddr = "192.0.2.2".parse().unwrap();
        let local: IpAddr = "127.0.0.1".parse().unwrap();

        let a1 = slots.try_acquire(a).expect("first");
        let _a2 = slots.try_acquire(a).expect("second");
        assert!(slots.try_acquire(a).is_none(), "one peer holds at most two");
        let _b1 = slots.try_acquire(b).expect("another peer still gets in");
        assert!(slots.try_acquire(b).is_none(), "the shared slots are full");
        let _local = slots.try_acquire(local).expect("loopback uses the reserve");
        assert!(
            slots.try_acquire(local).is_none(),
            "the reserve is one slot"
        );

        drop(a1);
        assert!(
            slots.try_acquire(a).is_some(),
            "a released slot counts back"
        );
    }
}
