//! Composition root for the Arlo camera streamer daemon.
//!
//! Subcommands: `run` (the default) starts the daemon; `list-devices`
//! signs in, lists the account's streamable devices and prints
//! `[[cameras]]` suggestions, then exits (see [`list_devices`]).
//!
//! Boot sequence of `run`:
//!
//! 1. Parse CLI (`--config /path/to/streamer.toml`).
//! 2. Initialize tracing subscriber from `RUST_LOG` (logs go to stderr).
//! 3. Load and parse the TOML config.
//! 4. Initialize GStreamer (must run before any `gstreamer::*` call).
//! 5. Build [`Metrics`] + [`Readiness`]; pre-warm per-camera state rows.
//! 6. Boot the arlo-rs client (`streamer_infra_arlo::boot::boot`) →
//!    construct the three Arlo port adapters from the shared
//!    [`std::sync::Arc<arlo_rs::client::ArloClient>`].
//! 7. Start the embedded RTSP server, build [`GstPipelineRegistry`],
//!    and wrap it in a [`GstMediaMultiplexer`].
//! 8. Spawn [`StreamerSystem`] (one orchestrator per camera + the
//!    event router) with the metrics recorder injected.
//! 9. Spawn the `connection_status` watcher → forwards Arlo bus state
//!    to readiness, metrics, and the admin actor's shared atomic.
//! 10. Spawn the ops HTTP server on `output.metrics_bind`
//!     (`/metrics`, `/healthz`, `/readyz`).
//! 11. Spawn the admin HTTP server on `output.admin_bind` (`/admin/*`),
//!     authed with the bearer token from `STREAMER_ADMIN_TOKEN`.
//! 12. Wait for SIGINT / SIGTERM or for the system token to be cancelled.
//! 13. Cancel the shared token, await all spawned tasks, drop the
//!     RTSP server.

#![forbid(unsafe_code)]
// Composition root has many `Arc<dyn …>` clones at boot.
#![allow(clippy::similar_names)]
// `tokio::select!` arms with terminal `None` paths are clearer as
// `match` than as `if let` because the surrounding macro already
// scopes the bindings.
#![allow(clippy::single_match_else)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use std::sync::atomic::{AtomicBool, Ordering};

use secrecy::SecretString;
use streamer_app::system::StreamerSystem;
use streamer_domain::config::StreamerConfig;
use streamer_domain::event::ConnectionStatus;
use streamer_domain::port::{
    AdminControl, ArloEventSource, ArloThumbnailSource, MetricsRecorder, UserViewSource,
    WebrtcSignaler,
};
use streamer_infra_arlo::{
    ArloEventSourceAdapter, ArloThumbnailSourceAdapter, ArloUserViewSourceAdapter,
    ArloWebrtcSignalerAdapter, DeviceRegistry, SnapshotUrlCache, boot::boot,
};
use streamer_infra_media::{GstMediaMultiplexer, GstPipelineRegistry, RtspServer};
use streamer_infra_ops::admin_server::check_admin_token;
use streamer_infra_ops::{AdminServer, Metrics, OpsServer, Readiness};

use crate::signals::ShutdownSignals;

/// Env var holding the bearer token for `/admin/*` routes.
const ADMIN_TOKEN_ENV: &str = "STREAMER_ADMIN_TOKEN";

/// Per-stage graceful-shutdown deadline. Any single drain step that
/// exceeds this is logged and abandoned so a stuck upstream teardown
/// (e.g. a WebRTC WS close that never acks) can never wedge process
/// exit. Generous enough for a clean drain under normal conditions.
const SHUTDOWN_STAGE_TIMEOUT: Duration = Duration::from_secs(8);
/// How long the runtime waits for blocking tasks at exit.
const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
/// The log filter when `RUST_LOG` is unset.
const DEFAULT_LOG_FILTER: &str = "info,arlo_camera_streamer=debug";
/// The log filter when `RUST_LOG` is set but does not parse.
const FALLBACK_LOG_FILTER: &str = "info";
/// Subdirectory beside the session cache that holds the idle thumbnails.
const THUMBNAIL_DIR_NAME: &str = "thumbnails";

mod healthcheck;
mod list_devices;
mod signals;

/// CLI arguments.
#[derive(Debug, Parser)]
#[command(
    name = "arlo-camera-streamer",
    version,
    about = "Bridges Arlo battery cameras to Frigate NVR via RTSP (and optional HLS)."
)]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(
        long,
        short,
        global = true,
        default_value = "/etc/arlo-streamer/streamer.toml"
    )]
    config: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Clone, Copy, Subcommand)]
enum Command {
    /// Run the streaming daemon (the default when no subcommand is given).
    Run,
    /// List the account's cameras with their device ids.
    ///
    /// Signs in to Arlo, lists the account's cameras and doorbells with
    /// their device ids, and prints a `[[cameras]]` block for each one not
    /// yet configured. Starts no server. Completes the MFA pairing, so the
    /// daemon's first start needs no OTP.
    ListDevices,
    /// Ask the running daemon's liveness endpoint; exit 0 when it answers.
    ///
    /// For the container `HEALTHCHECK`: needs no `wget` or `curl` in the
    /// image. Connects to `output.metrics_bind` on loopback.
    Healthcheck,
}

fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to start the async runtime")?;
    let outcome = runtime.block_on(async_main());
    // Dropping the runtime waits without limit for blocking tasks, such as
    // a pipeline teardown: one that hangs would hold the exit, and the
    // restart it should trigger, for ever.
    runtime.shutdown_timeout(RUNTIME_SHUTDOWN_TIMEOUT);
    outcome
}

async fn async_main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing()?;
    let config = load_config(&cli.config).await?;
    match cli.command.unwrap_or(Command::Run) {
        Command::Run => run_daemon(config).await,
        Command::ListDevices => list_devices::run(&config).await,
        Command::Healthcheck => healthcheck::run(&config).await,
    }
}

async fn run_daemon(config: StreamerConfig) -> Result<()> {
    info!(
        version = env!("CARGO_PKG_VERSION"),
        "starting arlo-camera-streamer"
    );
    // Before anything that can open an Arlo session: from here a SIGTERM
    // is held for the drain instead of killing the process.
    let signals = ShutdownSignals::install()?;
    gstreamer::init().context("failed to initialize GStreamer")?;
    info!("GStreamer initialized");
    info!(
        cameras = config.cameras.len(),
        rtsp_bind = %config.output.rtsp.bind,
        metrics_bind = %config.output.metrics_bind,
        "configuration loaded"
    );
    if let Err(e) = run(config, signals).await {
        error!(error = %e, "fatal error");
        return Err(e);
    }
    Ok(())
}

async fn run(config: StreamerConfig, mut signals: ShutdownSignals) -> Result<()> {
    // -- Observability handles --
    let cameras_count = u32::try_from(config.cameras.len()).unwrap_or(u32::MAX);
    let metrics = Arc::new(
        Metrics::new(cameras_count, env!("CARGO_PKG_VERSION"))
            .context("failed to construct metrics registry")?,
    );
    // Pre-warm per-camera gauges so Prometheus rows exist on first scrape.
    for cam in &config.cameras {
        metrics.prewarm_camera(&cam.arlo_device_id);
    }
    let readiness = Arc::new(Readiness::new());

    // -- Admin token --
    let admin_token = read_admin_token().context("failed to read admin token")?;

    // -- Listeners, before the Arlo login --
    //
    // A port in use fails the boot here instead of leaving the daemon
    // running without its health check or admin endpoint. The ops server
    // needs nothing from Arlo, so it serves at once: `/healthz` answers
    // during a long first login, `/readyz` reports not ready until the bus
    // is up.
    let ops_listener = bind_listener("metrics_bind", &config.output.metrics_bind).await?;
    let admin_listener = bind_listener("admin_bind", &config.output.admin_bind).await?;
    let ops_shutdown = CancellationToken::new();
    let ops_server = OpsServer::new(metrics.clone(), readiness.clone());
    let ops_task = {
        let ops_shutdown = ops_shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = ops_server.serve(ops_listener, ops_shutdown).await {
                error!(error = %e, "ops HTTP server exited with error");
            }
        })
    };

    // -- Arlo adapters --
    let ArloAdapters {
        event_source,
        signaler,
        thumbnails,
        user_views,
    } = arlo_adapters(&config).await?;

    // -- Media adapter --
    let rtsp_server =
        RtspServer::start(&config.output.rtsp.bind).context("failed to start RTSP server")?;
    // The encoder is probed on this host (ADR 0008): `auto` takes the first
    // working backend, an explicit one must work or the boot fails here.
    let video_encoder = streamer_infra_media::encoder::resolve(config.output.video_encoder)
        .context("no usable H.264 encoder")?;
    // Idle thumbnails live beside the session cache (owner-only), never
    // in the shared temp directory where another user could plant one.
    let thumbnail_dir = thumbnail_dir(&config);
    {
        // Filesystem work stays off the runtime's worker threads.
        let dir = thumbnail_dir.clone();
        tokio::task::spawn_blocking(move || streamer_infra_media::prepare_thumbnail_dir(&dir))
            .await
            .context("thumbnail directory task")?
            .with_context(|| format!("thumbnail directory {}", thumbnail_dir.display()))?;
    }
    let pipeline_registry = Arc::new(GstPipelineRegistry::new(
        rtsp_server.clone(),
        video_encoder,
        thumbnail_dir,
    ));
    let relay_tls =
        streamer_infra_media::RelayTls::from_config(config.arlo.watch_along_cert_sha256.as_deref())
            .context("watch-along TLS policy")?;
    let media: Arc<dyn streamer_domain::port::MediaMultiplexer> =
        Arc::new(GstMediaMultiplexer::new(
            pipeline_registry.clone(),
            config.output.clone(),
            config.webrtc.clone(),
            &config.cameras,
            relay_tls,
        ));

    // -- Application layer --
    let metrics_recorder: Arc<dyn MetricsRecorder> = metrics.clone();
    let system = StreamerSystem::spawn(
        &config,
        event_source.clone(),
        signaler,
        thumbnails,
        media,
        user_views,
        metrics_recorder,
        env!("CARGO_PKG_VERSION"),
    )
    .await
    .context("failed to spawn streamer system")?;
    readiness.mark_started();
    let shutdown = system.cancellation_token();
    let arlo_connected_flag = system.arlo_connected_flag();
    let admin_actor: Arc<dyn AdminControl> = Arc::new(system.admin_control());

    // -- Background watchers --
    let conn_task = spawn_connection_watcher(
        event_source.clone(),
        metrics.clone(),
        readiness.clone(),
        arlo_connected_flag,
        shutdown.clone(),
    );

    // -- Admin HTTP --
    let admin_addr = admin_listener.local_addr().ok();
    let admin_server =
        AdminServer::new(admin_actor, admin_token).context("failed to construct admin server")?;
    let admin_shutdown = shutdown.clone();
    let admin_task = tokio::spawn(async move {
        if let Err(e) = admin_server.serve(admin_listener, admin_shutdown).await {
            error!(error = %e, "admin HTTP server exited with error");
        }
    });
    info!(admin_addr = ?admin_addr, "admin endpoints available under /admin/*");

    info!("daemon ready; waiting for shutdown signal");

    // -- Wait for shutdown --
    //
    // The system cancels its own token when an actor dies or the event
    // bus ends (see `streamer_app::system`); that is a failure, reported
    // as a non-zero exit after the drain so a restart policy kicks in.
    let requested = tokio::select! {
        signal = signals.recv() => {
            warn!(signal, "stop requested; initiating graceful shutdown");
            true
        }
        () = shutdown.cancelled() => {
            warn!("the streamer system stopped on its own; initiating shutdown");
            false
        }
    };

    // -- Drain --
    //
    // Each stage is logged and bounded by `SHUTDOWN_STAGE_TIMEOUT` so a
    // single hung step can neither hide (no log) nor wedge the process
    // (no forced kill needed). Order matters: stop *producing* work
    // (cancel token → actors/router drain) before tearing down the
    // media plane, and stop the RTSP GLib loop **before** the final
    // `RtspServer` drop joins its thread — otherwise that join blocks
    // forever on a still-running loop.
    shutdown.cancel();
    ops_shutdown.cancel();

    // Taken before the drain consumes the system: a task that panicked,
    // even during the drain, makes the exit non-zero.
    let panics = system.panic_counter();
    drain_stage("streamer-system", system.shutdown()).await;
    drain_stage("connection-watcher", async {
        let _ = conn_task.await;
    })
    .await;
    drain_stage("ops-server", async {
        let _ = ops_task.await;
    })
    .await;
    drain_stage("admin-server", async {
        let _ = admin_task.await;
    })
    .await;

    // Tear down the media plane explicitly: remove RTSP mounts + abort
    // live pumps, then quit the GLib main loop. Without this the RTSP
    // server keeps rebuilding each camera's media on a ~20 s loop and
    // the loop thread never exits.
    drain_stage("pipeline-registry", pipeline_registry.shutdown()).await;
    rtsp_server.stop();
    drop(rtsp_server);

    let panicked = panics.load(Ordering::Relaxed);
    if panicked > 0 {
        anyhow::bail!("{panicked} task(s) panicked; see the log");
    }
    if !requested {
        anyhow::bail!(
            "the streamer system stopped on its own (an actor ended or the event bus closed); see the log"
        );
    }
    info!("graceful shutdown complete");
    Ok(())
}

/// Await a shutdown stage under [`SHUTDOWN_STAGE_TIMEOUT`], logging
/// entry, completion, and (if it elapses) the stall — so the daemon
/// log always shows exactly how far the drain got.
async fn drain_stage<F: std::future::Future<Output = ()>>(name: &str, fut: F) {
    info!(stage = name, "shutdown stage started");
    match tokio::time::timeout(SHUTDOWN_STAGE_TIMEOUT, fut).await {
        Ok(()) => info!(stage = name, "shutdown stage complete"),
        Err(_) => warn!(
            stage = name,
            timeout_s = SHUTDOWN_STAGE_TIMEOUT.as_secs(),
            "shutdown stage timed out; abandoning"
        ),
    }
}

/// Read the admin token from `STREAMER_ADMIN_TOKEN`. Fails if missing
/// or empty — we never want to run an unauthenticated admin surface.
/// The admin bearer token from the environment, as a secret (zeroed on
/// drop). Empty or short tokens fail the boot here, before anything
/// listens; the server re-checks the same rule.
fn read_admin_token() -> Result<SecretString> {
    admin_token_from(std::env::var(ADMIN_TOKEN_ENV))
}

/// Check the admin token read from the environment. The errors never
/// carry the value: `VarError::NotUnicode` prints it, and the boot error
/// chain is printed in full.
fn admin_token_from(value: Result<String, std::env::VarError>) -> Result<SecretString> {
    let token = match value {
        Ok(token) => SecretString::from(token),
        Err(std::env::VarError::NotPresent) => {
            anyhow::bail!("environment variable {ADMIN_TOKEN_ENV} must be set to enable /admin/*")
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            anyhow::bail!("environment variable {ADMIN_TOKEN_ENV} is not valid UTF-8")
        }
    };
    check_admin_token(&token)
        .map_err(|e| anyhow::anyhow!("environment variable {ADMIN_TOKEN_ENV}: {e}"))?;
    Ok(token)
}

/// Bind one ops listener from its config value (validated at load).
async fn bind_listener(name: &str, bind: &str) -> Result<tokio::net::TcpListener> {
    let addr: SocketAddr = bind
        .parse()
        .with_context(|| format!("invalid {name} '{bind}'"))?;
    streamer_infra_ops::serve::bind(addr)
        .await
        .with_context(|| format!("{name} {addr} cannot be bound"))
}

/// Subscribe to Arlo's `connection_status` watch and mirror it into
/// the readiness flag, metrics gauge, and the admin-actor's atomic.
fn spawn_connection_watcher(
    event_source: Arc<dyn ArloEventSource>,
    metrics: Arc<Metrics>,
    readiness: Arc<Readiness>,
    arlo_connected_flag: Arc<AtomicBool>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut stream = match event_source.connection_status().await {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, "could not subscribe to connection status");
                return;
            }
        };
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                next = stream.next() => match next {
                    Some(status) => {
                        let connected = matches!(status, ConnectionStatus::Connected);
                        readiness.set_arlo_connected(connected);
                        metrics.set_arlo_connected(connected);
                        arlo_connected_flag.store(connected, Ordering::Relaxed);
                        info!(?status, "arlo connection status changed");
                    }
                    None => {
                        warn!("connection-status stream ended");
                        readiness.set_arlo_connected(false);
                        metrics.set_arlo_connected(false);
                        arlo_connected_flag.store(false, Ordering::Relaxed);
                        break;
                    }
                },
            }
        }
    })
}

/// The Arlo-side ports, all on one authenticated client.
struct ArloAdapters {
    event_source: Arc<dyn ArloEventSource>,
    signaler: Arc<dyn WebrtcSignaler>,
    thumbnails: Arc<dyn ArloThumbnailSource>,
    user_views: Arc<dyn UserViewSource>,
}

/// Boot the Arlo client and build the four adapters on it.
async fn arlo_adapters(config: &StreamerConfig) -> Result<ArloAdapters> {
    let arlo_client = boot(&config.arlo)
        .await
        .context("failed to boot arlo-rs client")?;
    // One shared HTTP client (https only, bounded in time and redirects)
    // so connection pooling kicks in across cameras when fetching
    // presigned thumbnail URLs.
    let http = streamer_infra_arlo::thumbnails::http_client(concat!(
        "arlo-camera-streamer/",
        env!("CARGO_PKG_VERSION")
    ))
    .context("failed to build the thumbnail HTTP client")?;
    // One shared device cache: stream requests resolve CameraId → Device
    // through it instead of hitting get_devices() on every live request.
    let device_registry = Arc::new(DeviceRegistry::new(arlo_client.clone()));
    // Snapshot URLs announced on the bus, shared by the event adapter
    // (writer) and the thumbnail adapter (reader).
    let snapshots = Arc::new(SnapshotUrlCache::default());
    let event_source: Arc<dyn ArloEventSource> = Arc::new(ArloEventSourceAdapter::new(
        arlo_client.clone(),
        snapshots.clone(),
    ));
    let signaler: Arc<dyn WebrtcSignaler> = Arc::new(ArloWebrtcSignalerAdapter::new(
        arlo_client.clone(),
        device_registry.clone(),
    ));
    let thumbnails: Arc<dyn ArloThumbnailSource> = Arc::new(ArloThumbnailSourceAdapter::new(
        arlo_client.clone(),
        http,
        snapshots,
    ));
    // The user's live view in the Arlo app, relayed from its watch-along
    // stream (ADR 0007).
    let user_views: Arc<dyn UserViewSource> = Arc::new(ArloUserViewSourceAdapter::new(
        arlo_client,
        device_registry.clone(),
        &config.arlo.app_version,
    ));

    Ok(ArloAdapters {
        event_source,
        signaler,
        thumbnails,
        user_views,
    })
}

/// Where the idle thumbnails are written: a `thumbnails/` directory
/// beside the session cache, which the operator already keeps private.
fn thumbnail_dir(config: &StreamerConfig) -> std::path::PathBuf {
    config
        .arlo
        .session_cache_path
        .parent()
        .map(std::path::Path::to_path_buf)
        .unwrap_or_default()
        .join(THUMBNAIL_DIR_NAME)
}

async fn load_config(path: &std::path::Path) -> Result<StreamerConfig> {
    let raw = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read config file: {}", path.display()))?;
    // toml's own error text quotes the offending line, and with it a
    // password typed into the file; only its message and position go out.
    let cfg: StreamerConfig = toml::from_str(&raw).map_err(|e| {
        anyhow::anyhow!(
            "failed to parse config {}: {}{}",
            path.display(),
            e.message(),
            e.span()
                .map(|span| line_and_column(&raw, span.start))
                .unwrap_or_default()
        )
    })?;
    cfg.validate()
        .with_context(|| format!("invalid config: {}", path.display()))?;
    Ok(cfg)
}

/// ` (line L, column C)` of a byte offset in `text`, both 1-based.
fn line_and_column(text: &str, offset: usize) -> String {
    let before = text.get(..offset).unwrap_or(text);
    let line = before.matches('\n').count() + 1;
    let column = before.rsplit('\n').next().map_or(0, |l| l.chars().count()) + 1;
    format!(" (line {line}, column {column})")
}

fn init_tracing() -> Result<()> {
    let (filter, invalid) = log_filter(std::env::var(EnvFilter::DEFAULT_ENV).ok());
    // stderr keeps stdout for command output (`list-devices`).
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|e| anyhow::anyhow!("failed to init tracing: {e}"))?;
    if let Some(e) = invalid {
        warn!(error = %e, "RUST_LOG does not parse; logging at {FALLBACK_LOG_FILTER}");
    }
    Ok(())
}

/// The filter for `RUST_LOG`: the default when unset, and plain `info`
/// (plus the parse error, to report) when set but invalid. A typo used to
/// fall back silently to the debug default, more verbose than asked.
fn log_filter(spec: Option<String>) -> (EnvFilter, Option<String>) {
    match spec {
        None => (EnvFilter::new(DEFAULT_LOG_FILTER), None),
        Some(spec) => match EnvFilter::try_new(&spec) {
            Ok(filter) => (filter, None),
            Err(e) => (EnvFilter::new(FALLBACK_LOG_FILTER), Some(e.to_string())),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `VarError::NotUnicode` prints the value, and the boot error chain
    /// is printed in full: the token reached the container log.
    #[cfg(unix)]
    #[test]
    fn admin_token_that_is_not_utf8_is_refused_without_printing_it() {
        use std::os::unix::ffi::OsStringExt;
        let raw = std::ffi::OsString::from_vec(b"SECRET-0123456789\xff".to_vec());
        let err = admin_token_from(Err(std::env::VarError::NotUnicode(raw))).unwrap_err();
        let shown = format!("{err:?}");
        assert!(!shown.contains("SECRET"), "{shown}");
        assert!(shown.contains("UTF-8"), "{shown}");
    }

    #[test]
    fn admin_token_with_a_trailing_newline_is_refused_at_boot() {
        let err = admin_token_from(Ok("0123456789abcdef0123\n".to_string())).unwrap_err();
        assert!(format!("{err}").contains("no spaces or newline"), "{err}");
        assert!(admin_token_from(Ok("0123456789abcdef0123".to_string())).is_ok());
        assert!(admin_token_from(Err(std::env::VarError::NotPresent)).is_err());
    }

    /// toml's error text quotes the offending line, password included.
    #[tokio::test]
    async fn config_parse_error_shows_the_position_not_the_line() {
        let dir = std::env::temp_dir().join(format!("load-config-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("streamer.toml");
        let example = include_str!("../../../config/streamer.example.toml");
        let leaked = example.replacen("[arlo]", "[arlo]\npassword = \"hunter2-secret\"", 1);
        tokio::fs::write(&path, leaked).await.unwrap();

        let err = load_config(&path).await.unwrap_err().to_string();

        assert!(!err.contains("hunter2"), "{err}");
        assert!(err.contains("password"), "names the key: {err}");
        assert!(err.contains("line "), "points at the line: {err}");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[test]
    fn line_and_column_count_from_one() {
        assert_eq!(line_and_column("ab\ncd", 0), " (line 1, column 1)");
        assert_eq!(line_and_column("ab\ncd", 4), " (line 2, column 2)");
    }

    /// A typo in `RUST_LOG` used to fall back silently to the debug default.
    #[test]
    fn log_filter_falls_back_to_info_and_reports_an_invalid_spec() {
        let (_, unset) = log_filter(None);
        assert!(unset.is_none());
        let (_, valid) = log_filter(Some("info,streamer_app=debug".to_string()));
        assert!(valid.is_none());
        let (filter, invalid) = log_filter(Some("info,streamer_app=verbose[".to_string()));
        assert!(invalid.is_some(), "the parse error is reported");
        assert_eq!(filter.to_string(), FALLBACK_LOG_FILTER);
    }

    /// The listeners were bound inside their tasks: a port in use left
    /// the daemon running without its health check.
    #[tokio::test]
    async fn bind_listener_fails_on_a_port_in_use() {
        let taken = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = taken.local_addr().unwrap().to_string();
        let err = bind_listener("metrics_bind", &addr).await.unwrap_err();
        assert!(format!("{err:#}").contains("metrics_bind"), "{err:#}");
    }
}
