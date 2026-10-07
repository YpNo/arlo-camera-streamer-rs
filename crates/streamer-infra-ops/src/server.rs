//! axum-based HTTP server hosting the three ops endpoints.
//!
//! The server runs as a single tokio task and exits cleanly when the
//! [`tokio_util::sync::CancellationToken`] passed to
//! [`OpsServer::serve`] is cancelled.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::health::Readiness;
use crate::metrics::Metrics;

/// Composes [`Metrics`] + [`Readiness`] into the axum-handler shared
/// state.
#[derive(Clone)]
struct AppState {
    metrics: Arc<Metrics>,
    readiness: Arc<Readiness>,
}

/// Operational HTTP server.
pub struct OpsServer {
    state: AppState,
}

impl OpsServer {
    /// Build a server from the shared metrics + readiness handles.
    #[must_use]
    pub fn new(metrics: Arc<Metrics>, readiness: Arc<Readiness>) -> Self {
        Self {
            state: AppState { metrics, readiness },
        }
    }

    /// Build the axum router. Exposed for tests.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/metrics", get(handle_metrics))
            .route("/healthz", get(handle_healthz))
            .route("/readyz", get(handle_readyz))
            .with_state(self.state.clone())
    }

    /// Serve on `listener` (bound at boot, see [`crate::serve::bind`])
    /// until `shutdown` is cancelled, with the default
    /// [`ServeLimits`](crate::serve::ServeLimits).
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::OpsError`] if the server loop fails.
    pub async fn serve(
        self,
        listener: tokio::net::TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(), crate::error::OpsError> {
        let app = self.router();
        crate::serve::serve(
            "ops",
            listener,
            app,
            crate::serve::ServeLimits::default(),
            shutdown,
        )
        .await
    }
}

async fn handle_metrics(State(state): State<AppState>) -> Response {
    match state.metrics.render() {
        Ok(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            body,
        )
            .into_response(),
        Err(e) => {
            error!(error = %e, "failed to render metrics");
            (StatusCode::INTERNAL_SERVER_ERROR, "metrics render failed").into_response()
        }
    }
}

async fn handle_healthz() -> Response {
    (StatusCode::OK, "ok").into_response()
}

async fn handle_readyz(State(state): State<AppState>) -> Response {
    if state.readiness.is_ready() {
        (StatusCode::OK, "ready").into_response()
    } else {
        let detail = format!(
            "not ready: started={} arlo_connected={}",
            state.readiness.is_started(),
            state.readiness.is_arlo_connected(),
        );
        (StatusCode::SERVICE_UNAVAILABLE, detail).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use tower::ServiceExt;

    fn make_server(connected: bool, started: bool) -> OpsServer {
        let metrics = Arc::new(Metrics::new(2, "0.1.0").unwrap());
        let readiness = Arc::new(Readiness::new());
        if started {
            readiness.mark_started();
        }
        readiness.set_arlo_connected(connected);
        metrics.set_arlo_connected(connected);
        OpsServer::new(metrics, readiness)
    }

    #[tokio::test]
    async fn healthz_returns_200_unconditionally() {
        let server = make_server(false, false);
        let response = server
            .router()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(String::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_returns_503_when_not_started() {
        let server = make_server(true, false);
        let response = server
            .router()
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(String::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn readyz_returns_503_when_arlo_disconnected() {
        let server = make_server(false, true);
        let response = server
            .router()
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(String::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn readyz_returns_200_when_started_and_connected() {
        let server = make_server(true, true);
        let response = server
            .router()
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(String::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_returns_text_plain_with_streamer_metrics() {
        let server = make_server(true, true);
        let response = server
            .router()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(String::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(content_type.starts_with("text/plain"));
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("streamer_arlo_connected 1"));
        assert!(body_str.contains("streamer_cameras_configured 2"));
    }

    #[tokio::test]
    async fn unknown_route_returns_404() {
        let server = make_server(true, true);
        let response = server
            .router()
            .oneshot(Request::builder().uri("/nope").body(String::new()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
