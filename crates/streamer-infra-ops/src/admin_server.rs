//! Operational write API hosted on a separate bind address.
//!
//! Endpoints:
//!
//! | Method | Path                          | Description                       |
//! |--------|-------------------------------|-----------------------------------|
//! | GET    | `/admin/state`                | System-wide snapshot (JSON).      |
//! | GET    | `/admin/cameras/{id}`         | One-camera snapshot (JSON).       |
//! | POST   | `/admin/cameras/{id}/wake`   | Inject a synthetic motion event.  |
//! | POST   | `/admin/cameras/{id}/idle`   | Force the camera back to `Idle`.  |
//!
//! ## Authentication
//!
//! All routes require a `Authorization: Bearer <token>` header that
//! matches the token configured on [`AdminServer::new`]. A constant-
//! time comparison guards against timing-leak side channels. The check
//! is one layer over every route ([`require_bearer`]), so a route added
//! later cannot forget it, and it runs before any extractor.
//!
//! Tokens shorter than [`MIN_ADMIN_TOKEN_BYTES`] are rejected at
//! construction (a daemon should never expose write endpoints behind a
//! guessable token — fail fast at boot), and so are tokens a `Bearer`
//! header cannot carry (whitespace, a trailing newline from a file),
//! which would boot fine and never match.
//!
//! A rejected request is logged with its route and the peer's address,
//! at most once a minute per address at `warn` (a scan cannot flood the
//! log); never with the presented token.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{ConnectInfo, MatchedPath, Path, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use secrecy::{ExposeSecret, SecretString};
use streamer_domain::admin::AdminError;
use streamer_domain::camera::CameraId;
use streamer_domain::port::AdminControl;
use subtle::ConstantTimeEq;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::serve::ServeLimits;

/// Header name used for bearer auth. Lower-case so axum's case-
/// insensitive map lookups match the canonical form.
const AUTH_HEADER: &str = "authorization";
const BEARER_PREFIX: &str = "Bearer ";
/// Shortest admin token accepted (128 bits of a random token).
pub const MIN_ADMIN_TOKEN_BYTES: usize = 16;
/// Shortest gap between two `warn` lines for one peer's rejections.
const REJECTION_WARN_INTERVAL: Duration = Duration::from_mins(1);
/// Peers remembered for the rejection rate limit; the map is cleared past
/// it, so a scan from many addresses cannot grow it without bound.
const MAX_TRACKED_PEERS: usize = 1024;

/// Shared handler state.
#[derive(Clone)]
struct AppState {
    admin: Arc<dyn AdminControl>,
    /// Bearer token expected on every request. Kept as a secret (zeroed
    /// on drop, never printed) and exposed only for the comparison.
    token: Arc<SecretString>,
    /// When each peer's last rejection was logged at `warn`.
    rejections: Arc<Mutex<HashMap<IpAddr, Instant>>>,
}

/// Errors raised by [`AdminServer::new`].
#[derive(Debug, thiserror::Error)]
pub enum AdminServerError {
    /// The configured admin token was empty or whitespace-only.
    #[error("admin token must be non-empty")]
    EmptyToken,
    /// The configured admin token is shorter than [`MIN_ADMIN_TOKEN_BYTES`].
    #[error("admin token must be at least {MIN_ADMIN_TOKEN_BYTES} bytes")]
    ShortToken,
    /// The token holds whitespace (a trailing newline from a file) or a
    /// character a `Bearer` header cannot carry; it could never match.
    #[error(
        "admin token may only hold letters, digits and - . _ ~ + / = (no spaces or newline); generate one with `openssl rand -hex 32`"
    )]
    MalformedToken,
}

/// Check an admin token the way [`AdminServer::new`] does, so the boot
/// can refuse a bad one before the Arlo login.
///
/// # Errors
///
/// [`AdminServerError::EmptyToken`], [`AdminServerError::MalformedToken`]
/// or [`AdminServerError::ShortToken`]; none carries the token.
pub fn check_admin_token(token: &SecretString) -> Result<(), AdminServerError> {
    let raw = token.expose_secret();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AdminServerError::EmptyToken);
    }
    if !is_bearer_token(raw) {
        return Err(AdminServerError::MalformedToken);
    }
    if trimmed.len() < MIN_ADMIN_TOKEN_BYTES {
        return Err(AdminServerError::ShortToken);
    }
    Ok(())
}

/// Whether `token` is a `token68` (RFC 7235), what a `Bearer` header
/// carries: hyper also strips surrounding whitespace from header values,
/// so a token with any would never match.
fn is_bearer_token(token: &str) -> bool {
    token
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"-._~+/=".contains(&b))
}

/// HTTP server for the admin write surface.
pub struct AdminServer {
    state: AppState,
}

impl std::fmt::Debug for AdminServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminServer").finish_non_exhaustive()
    }
}

impl AdminServer {
    /// Build a server.
    ///
    /// # Errors
    ///
    /// Returns [`AdminServerError::EmptyToken`] if `token` is empty or
    /// whitespace-only, [`AdminServerError::ShortToken`] if it is shorter
    /// than [`MIN_ADMIN_TOKEN_BYTES`].
    pub fn new(
        admin: Arc<dyn AdminControl>,
        token: SecretString,
    ) -> Result<Self, AdminServerError> {
        check_admin_token(&token)?;
        Ok(Self {
            state: AppState {
                admin,
                token: Arc::new(token),
                rejections: Arc::default(),
            },
        })
    }

    /// Build the axum router: every route behind [`require_bearer`].
    /// Exposed for tests.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/admin/state", get(handle_state))
            .route("/admin/cameras/{id}", get(handle_camera_state))
            .route("/admin/cameras/{id}/wake", post(handle_wake))
            .route("/admin/cameras/{id}/idle", post(handle_idle))
            .route_layer(middleware::from_fn_with_state(
                self.state.clone(),
                require_bearer,
            ))
            .with_state(self.state.clone())
    }

    /// Serve on `listener` (bound at boot, see [`crate::serve::bind`])
    /// until `shutdown` is cancelled.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::OpsError`] on serve failure.
    pub async fn serve(
        self,
        listener: tokio::net::TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(), crate::error::OpsError> {
        let app = self.router();
        crate::serve::serve("admin", listener, app, ServeLimits::default(), shutdown).await
    }
}

/// The authentication layer over every admin route: a request without the
/// right bearer token gets the 401 before any extractor or handler runs.
async fn require_bearer(State(state): State<AppState>, req: Request, next: Next) -> Response {
    if check_auth(req.headers(), &state.token).is_some() {
        return next.run(req).await;
    }
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map_or("?", MatchedPath::as_str)
        .to_owned();
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| *addr);
    unauthorized(&state, req.method().as_str(), &route, peer)
}

/// Constant-time comparison of two byte slices (`subtle`). Returns
/// `true` iff equal; a length mismatch is the only early answer, and the
/// length of the real token is not a secret worth hiding.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && bool::from(a.ct_eq(b))
}

/// Validate the `Authorization` header. Returns `Some(())` on success,
/// `None` when missing / malformed / wrong token.
fn check_auth(headers: &HeaderMap, expected: &SecretString) -> Option<()> {
    let value = headers.get(AUTH_HEADER)?.to_str().ok()?;
    let provided = value.strip_prefix(BEARER_PREFIX)?;
    if ct_eq(provided.as_bytes(), expected.expose_secret().as_bytes()) {
        Some(())
    } else {
        None
    }
}

/// The `400` for a `{id}` path parameter outside the id rule: it is
/// refused at the trust boundary and never reaches a log line or the
/// actor. `e` names the rule broken, never the input.
fn bad_camera_id(e: &streamer_domain::error::DomainError) -> Response {
    debug!(error = %e, "admin: rejected camera id");
    (StatusCode::BAD_REQUEST, format!("invalid camera id: {e}")).into_response()
}

/// The 401 answer. The method, route template and peer are logged (never
/// the presented token or the caller-chosen path parameters) so a scan or
/// a stale token shows up in the log — at `warn` at most once a minute
/// per peer, at `debug` in between.
fn unauthorized(state: &AppState, method: &str, route: &str, peer: Option<SocketAddr>) -> Response {
    let ip = peer.map(|p| p.ip());
    if should_warn(&state.rejections, ip) {
        warn!(method, route, peer = ?peer, "admin: rejected unauthenticated request (next report for this peer in a minute at most)");
    } else {
        debug!(method, route, peer = ?peer, "admin: rejected unauthenticated request");
    }
    let mut resp = (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    resp.headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    resp
}

/// Whether a rejection from `ip` gets a `warn` line now.
fn should_warn(rejections: &Mutex<HashMap<IpAddr, Instant>>, ip: Option<IpAddr>) -> bool {
    let Some(ip) = ip else {
        return true;
    };
    let now = Instant::now();
    let mut seen = rejections.lock().unwrap_or_else(PoisonError::into_inner);
    if seen
        .get(&ip)
        .is_some_and(|at| now.duration_since(*at) < REJECTION_WARN_INTERVAL)
    {
        return false;
    }
    if seen.len() >= MAX_TRACKED_PEERS {
        seen.clear();
    }
    seen.insert(ip, now);
    true
}

fn admin_error_to_response(err: AdminError) -> Response {
    match err {
        AdminError::UnknownCamera(id) => {
            (StatusCode::NOT_FOUND, format!("unknown camera: {id}")).into_response()
        }
        AdminError::Unavailable(msg) => {
            warn!(error = %msg, "admin: orchestrator unavailable");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("unavailable: {msg}"),
            )
                .into_response()
        }
        AdminError::Internal(msg) => {
            error!(error = %msg, "admin: internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
        AdminError::RateLimited(msg) => (
            StatusCode::TOO_MANY_REQUESTS,
            format!("rate limited: {msg}"),
        )
            .into_response(),
    }
}

fn json_response<T: serde::Serialize>(value: &T) -> Response {
    match serde_json::to_string(value) {
        Ok(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(e) => {
            error!(error = %e, "admin: JSON encoding failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "encoding failed").into_response()
        }
    }
}

async fn handle_state(State(state): State<AppState>) -> Response {
    debug!("GET /admin/state");
    match state.admin.snapshot().await {
        Ok(s) => json_response(&s),
        Err(e) => admin_error_to_response(e),
    }
}

async fn handle_camera_state(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let id = match CameraId::parse(&id) {
        Ok(id) => id,
        Err(e) => return bad_camera_id(&e),
    };
    debug!(camera = %id, "GET /admin/cameras/{id}");
    match state.admin.camera_snapshot(&id).await {
        Ok(s) => json_response(&s),
        Err(e) => admin_error_to_response(e),
    }
}

async fn handle_wake(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let id = match CameraId::parse(&id) {
        Ok(id) => id,
        Err(e) => return bad_camera_id(&e),
    };
    info!(camera = %id, "POST /admin/cameras/{id}/wake");
    match state.admin.manual_wake(&id).await {
        Ok(()) => (StatusCode::ACCEPTED, "wake queued").into_response(),
        Err(e) => admin_error_to_response(e),
    }
}

async fn handle_idle(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let id = match CameraId::parse(&id) {
        Ok(id) => id,
        Err(e) => return bad_camera_id(&e),
    };
    info!(camera = %id, "POST /admin/cameras/{id}/idle");
    match state.admin.force_idle(&id).await {
        Ok(()) => (StatusCode::ACCEPTED, "idle queued").into_response(),
        Err(e) => admin_error_to_response(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::body::to_bytes;
    use axum::http::Request;
    use streamer_domain::admin::{CameraSnapshot, SystemSnapshot};
    use streamer_domain::camera::StreamName;
    use tokio::sync::Mutex;
    use tower::ServiceExt;

    /// Hand-written admin double that we can program from tests.
    #[derive(Default)]
    struct StubAdmin {
        force_idle_calls: Mutex<Vec<String>>,
        wake_calls: Mutex<Vec<String>>,
        snapshot_err: Mutex<Option<AdminError>>,
    }

    impl StubAdmin {
        fn arc() -> Arc<Self> {
            Arc::new(Self::default())
        }
    }

    #[async_trait]
    impl AdminControl for StubAdmin {
        async fn snapshot(&self) -> Result<SystemSnapshot, AdminError> {
            if let Some(e) = self.snapshot_err.lock().await.take() {
                return Err(e);
            }
            Ok(SystemSnapshot {
                version: "test".to_string(),
                uptime_secs: 1,
                arlo_connected: true,
                cameras: vec![],
            })
        }
        async fn camera_snapshot(&self, camera: &CameraId) -> Result<CameraSnapshot, AdminError> {
            if camera.as_str() == "MISSING" {
                return Err(AdminError::UnknownCamera(camera.clone()));
            }
            Ok(CameraSnapshot {
                id: camera.clone(),
                stream_name: StreamName::parse("front").unwrap(),
                state: "idle".to_string(),
                live_secs_today: 0,
                daily_budget_secs: 0,
                live_source: None,
                last_failure: None,
                retries: 0,
                user_view: false,
            })
        }
        async fn force_idle(&self, camera: &CameraId) -> Result<(), AdminError> {
            self.force_idle_calls
                .lock()
                .await
                .push(camera.as_str().to_string());
            Ok(())
        }
        async fn manual_wake(&self, camera: &CameraId) -> Result<(), AdminError> {
            self.wake_calls
                .lock()
                .await
                .push(camera.as_str().to_string());
            Ok(())
        }
    }

    const TOKEN: &str = "secret-token-of-sixteen-bytes-or-more";

    fn make_server() -> (AdminServer, Arc<StubAdmin>) {
        let admin = StubAdmin::arc();
        let srv = AdminServer::new(admin.clone(), SecretString::from(TOKEN)).unwrap();
        (srv, admin)
    }

    #[tokio::test]
    async fn empty_token_is_rejected_at_construction() {
        let admin = StubAdmin::arc();
        let err = AdminServer::new(admin, SecretString::from("   ")).unwrap_err();
        assert!(matches!(err, AdminServerError::EmptyToken));
    }

    #[tokio::test]
    async fn short_token_is_rejected_at_construction() {
        let admin = StubAdmin::arc();
        let err = AdminServer::new(admin, SecretString::from("fifteen-bytes-x")).unwrap_err();
        assert!(matches!(err, AdminServerError::ShortToken));
    }

    #[tokio::test]
    async fn camera_routes_answer_400_for_an_id_outside_the_rule() {
        let (server, admin) = make_server();
        for (method, uri) in [
            ("GET", "/admin/cameras/..%2F..%2Fetc"),
            ("POST", "/admin/cameras/forged%0Aline/wake"),
            ("POST", "/admin/cameras/a%20b/idle"),
        ] {
            let response = server
                .router()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(uri)
                        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                        .body(String::new())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{method} {uri}");
        }
        assert!(admin.wake_calls.lock().await.is_empty());
        assert!(admin.force_idle_calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn state_requires_bearer_auth() {
        let (server, _admin) = make_server();
        let response = server
            .router()
            .oneshot(
                Request::builder()
                    .uri("/admin/state")
                    .body(String::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(response.headers().contains_key(header::WWW_AUTHENTICATE));
    }

    #[tokio::test]
    async fn state_returns_json_with_correct_token() {
        let (server, _admin) = make_server();
        let response = server
            .router()
            .oneshot(
                Request::builder()
                    .uri("/admin/state")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(String::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let ct = response
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(ct.contains("application/json"));
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let parsed: SystemSnapshot = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.version, "test");
    }

    #[tokio::test]
    async fn camera_state_unknown_returns_404() {
        let (server, _admin) = make_server();
        let response = server
            .router()
            .oneshot(
                Request::builder()
                    .uri("/admin/cameras/MISSING")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(String::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn wake_calls_admin_and_returns_202() {
        let (server, admin) = make_server();
        let response = server
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/cameras/CAM1/wake")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(String::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let calls = admin.wake_calls.lock().await;
        assert_eq!(calls.as_slice(), ["CAM1"]);
    }

    #[tokio::test]
    async fn idle_calls_admin_and_returns_202() {
        let (server, admin) = make_server();
        let response = server
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/cameras/CAM1/idle")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(String::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let calls = admin.force_idle_calls.lock().await;
        assert_eq!(calls.as_slice(), ["CAM1"]);
    }

    #[tokio::test]
    async fn wrong_token_returns_401() {
        let (server, _admin) = make_server();
        let response = server
            .router()
            .oneshot(
                Request::builder()
                    .uri("/admin/state")
                    .header(header::AUTHORIZATION, "Bearer not-secret")
                    .body(String::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn malformed_auth_header_returns_401() {
        let (server, _admin) = make_server();
        let response = server
            .router()
            .oneshot(
                Request::builder()
                    .uri("/admin/state")
                    .header(header::AUTHORIZATION, "Basic AAAA")
                    .body(String::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn snapshot_unavailable_returns_503() {
        let (server, admin) = make_server();
        *admin.snapshot_err.lock().await = Some(AdminError::Unavailable("stuck".to_string()));
        let response = server
            .router()
            .oneshot(
                Request::builder()
                    .uri("/admin/state")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(String::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn unknown_admin_route_returns_404() {
        let (server, _admin) = make_server();
        let response = server
            .router()
            .oneshot(
                Request::builder()
                    .uri("/admin/nope")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(String::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn ct_eq_compares_bytes_and_lengths() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"abcd"));
        assert!(ct_eq(b"", b""));
    }

    /// Authentication used to be a call in each handler, and a route added
    /// without it would be open. Every route, with a valid camera id and
    /// with one outside the rule, answers 401 without the token.
    #[tokio::test]
    async fn every_admin_route_requires_the_token_before_anything_else() {
        let (server, admin) = make_server();
        for (method, uri) in [
            ("GET", "/admin/state"),
            ("GET", "/admin/cameras/CAM"),
            ("GET", "/admin/cameras/bad%20id"),
            ("POST", "/admin/cameras/CAM/wake"),
            ("POST", "/admin/cameras/CAM/idle"),
            ("POST", "/admin/cameras/bad%0Aid/idle"),
        ] {
            for auth in [
                None,
                Some("Bearer wrong-token-of-enough-bytes"),
                Some(TOKEN),
            ] {
                let mut req = Request::builder().method(method).uri(uri);
                if let Some(value) = auth {
                    req = req.header(header::AUTHORIZATION, value);
                }
                let response = server
                    .router()
                    .oneshot(req.body(String::new()).unwrap())
                    .await
                    .unwrap();
                assert_eq!(
                    response.status(),
                    StatusCode::UNAUTHORIZED,
                    "{method} {uri} with {auth:?}"
                );
            }
        }
        assert!(admin.wake_calls.lock().await.is_empty());
        assert!(admin.force_idle_calls.lock().await.is_empty());
    }

    /// A token read from a file kept its newline: it booted and could
    /// never match, since header values lose surrounding whitespace.
    #[tokio::test]
    async fn token_a_bearer_header_cannot_carry_is_rejected_at_construction() {
        for bad in [
            "0123456789abcdef0123\n",
            " 0123456789abcdef0123",
            "0123456789 abcdef0123",
            "0123456789abcdef0123é",
        ] {
            let err = AdminServer::new(StubAdmin::arc(), SecretString::from(bad)).unwrap_err();
            assert!(matches!(err, AdminServerError::MalformedToken), "{bad:?}");
        }
        assert!(
            AdminServer::new(
                StubAdmin::arc(),
                SecretString::from("aZ09-._~+/=aZ09-._~+/=")
            )
            .is_ok()
        );
    }

    #[test]
    fn rejections_warn_once_per_peer_per_interval() {
        let seen = std::sync::Mutex::new(HashMap::new());
        let a: IpAddr = "192.0.2.1".parse().unwrap();
        let b: IpAddr = "192.0.2.2".parse().unwrap();
        assert!(should_warn(&seen, Some(a)));
        assert!(!should_warn(&seen, Some(a)), "same peer within the minute");
        assert!(should_warn(&seen, Some(b)), "another peer");
        assert!(should_warn(&seen, None), "an unknown peer always warns");
    }
}
