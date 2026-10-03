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
//! 12. Wait for `Ctrl-C` or for the system token to be cancelled.
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
use streamer_infra_ops::{AdminServer, Metrics, OpsServer, Readiness};

/// Env var holding the bearer token for `/admin/*` routes.
const ADMIN_TOKEN_ENV: &str = "STREAMER_ADMIN_TOKEN";

/// Per-stage graceful-shutdown deadline. Any single drain step that
/// exceeds this is logged and abandoned so a stuck upstream teardown
/// (e.g. a WebRTC WS close that never acks) can never wedge process
/// exit. Generous enough for a clean drain under normal conditions.
const SHUTDOWN_STAGE_TIMEOUT: Duration = Duration::from_secs(8);
/// Subdirectory beside the session cache that holds the idle thumbnails.
const THUMBNAIL_DIR_NAME: &str = "thumbnails";

mod list_devices;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing()?;
    let config = load_config(&cli.config)?;
    match cli.command.unwrap_or(Command::Run) {
        Command::Run => run_daemon(config).await,
        Command::ListDevices => list_devices::run(&config).await,
    }
}

async fn run_daemon(config: StreamerConfig) -> Result<()> {
    info!(
        version = env!("CARGO_PKG_VERSION"),
        "starting arlo-camera-streamer"
    );
    gstreamer::init().context("failed to initialize GStreamer")?;
    info!("GStreamer initialized");
    info!(
        cameras = config.cameras.len(),
        rtsp_bind = %config.output.rtsp.bind,
        metrics_bind = %config.output.metrics_bind,
        "configuration loaded"
    );
    if let Err(e) = run(config).await {
        error!(error = %e, "fatal error");
        return Err(e);
    }
    Ok(())
}

async fn run(config: StreamerConfig) -> Result<()> {
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
    streamer_infra_media::prepare_thumbnail_dir(&thumbnail_dir)
        .with_context(|| format!("thumbnail directory {}", thumbnail_dir.display()))?;
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

    // -- Ops HTTP --
    let ops_addr: SocketAddr = config
        .output
        .metrics_bind
        .parse()
        .with_context(|| format!("invalid metrics_bind '{}'", config.output.metrics_bind))?;
    let ops_server = OpsServer::new(metrics.clone(), readiness.clone());
    let ops_shutdown = shutdown.clone();
    let ops_task = tokio::spawn(async move {
        if let Err(e) = ops_server.serve(ops_addr, ops_shutdown).await {
            error!(error = %e, "ops HTTP server exited with error");
        }
    });

    // -- Admin HTTP --
    let admin_addr: SocketAddr = config
        .output
        .admin_bind
        .parse()
        .with_context(|| format!("invalid admin_bind '{}'", config.output.admin_bind))?;
    let admin_server =
        AdminServer::new(admin_actor, admin_token).context("failed to construct admin server")?;
    let admin_shutdown = shutdown.clone();
    let admin_task = tokio::spawn(async move {
        if let Err(e) = admin_server.serve(admin_addr, admin_shutdown).await {
            error!(error = %e, "admin HTTP server exited with error");
        }
    });
    info!(%admin_addr, "admin endpoints available under /admin/*");

    info!("daemon ready; waiting for shutdown signal");

    // -- Wait for shutdown --
    //
    // The system cancels its own token when an actor dies or the event
    // bus ends (see `streamer_app::system`); that is a failure, reported
    // as a non-zero exit after the drain so a restart policy kicks in.
    let requested = tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            warn!("Ctrl-C received; initiating graceful shutdown");
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
fn read_admin_token() -> Result<String> {
    let token = std::env::var(ADMIN_TOKEN_ENV).with_context(|| {
        format!("environment variable {ADMIN_TOKEN_ENV} must be set to enable /admin/*")
    })?;
    if token.trim().is_empty() {
        anyhow::bail!("environment variable {ADMIN_TOKEN_ENV} must not be empty");
    }
    Ok(token)
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

fn load_config(path: &std::path::Path) -> Result<StreamerConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file: {}", path.display()))?;
    let cfg: StreamerConfig = toml::from_str(&raw)
        .with_context(|| format!("failed to parse config: {}", path.display()))?;
    cfg.validate()
        .with_context(|| format!("invalid config: {}", path.display()))?;
    Ok(cfg)
}

fn init_tracing() -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,arlo_camera_streamer=debug"));
    // stderr keeps stdout for command output (`list-devices`).
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|e| anyhow::anyhow!("failed to init tracing: {e}"))?;
    Ok(())
}
