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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::error::OpsError;

/// How long open connections get to finish after the shutdown signal.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
/// Pause after a failed `accept` (out of descriptors, a reset peer)
/// before trying again, so the loop never spins.
const ACCEPT_RETRY: Duration = Duration::from_millis(200);

/// Bounds on one listener.
#[derive(Debug, Clone, Copy)]
pub struct ServeLimits {
    /// A connection that has not sent a full request head by then is
    /// closed.
    pub header_read_timeout: Duration,
    /// Connections open at once; further ones are closed on accept.
    pub max_connections: usize,
}

impl Default for ServeLimits {
    fn default() -> Self {
        Self {
            header_read_timeout: Duration::from_secs(10),
            max_connections: 64,
        }
    }
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
    let permits = Arc::new(Semaphore::new(limits.max_connections));

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
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    debug!(server = name, %peer, "connection refused: limit reached");
                    drop(stream);
                    continue;
                };
                let service = TowerToHyperService::new(app.clone());
                let conn = builder.serve_connection(TokioIo::new(stream), service).into_owned();
                let conn = graceful.watch(conn);
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        debug!(server = name, %peer, error = %e, "connection ended with an error");
                    }
                    drop(permit);
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
}
