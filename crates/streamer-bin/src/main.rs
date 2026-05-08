//! Composition root for the Arlo camera streamer daemon.
//!
//! Boot sequence:
//!
//! 1. Parse CLI (`--config /path/to/streamer.toml`).
//! 2. Initialize tracing subscriber from `RUST_LOG`.
//! 3. Initialize GStreamer (must run before any `gstreamer::*` call).
//! 4. Load and parse the TOML config.
//! 5. Build [`Metrics`] + [`Readiness`].
//! 6. Boot the rs-arlo client (`streamer_infra_arlo::boot::boot`) →
//!    construct the three Arlo port adapters from the shared
//!    [`std::sync::Arc<rs_arlo::client::ArloClient>`].
//! 7. Start the embedded RTSP server, build [`GstPipelineRegistry`],
//!    and wrap it in a [`GstMediaMultiplexer`].
//! 8. Spawn [`StreamerSystem`] (one orchestrator per camera + the
//!    event router). This is the moment readiness becomes "started".
//! 9. Spawn the `connection_status` watcher → forwards Arlo bus state
//!    to readiness + metrics.
//! 10. Spawn the ops HTTP server on `output.metrics_bind` (Phase 5
//!     binds `/metrics`, `/healthz`, `/readyz` to the same address).
//! 11. Wait for `Ctrl-C` or for the system token to be cancelled.
//! 12. Cancel the shared token, await all spawned tasks, drop the
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

use anyhow::{Context, Result};
use clap::Parser;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use streamer_app::system::StreamerSystem;
use streamer_domain::config::StreamerConfig;
use streamer_domain::event::ConnectionStatus;
use streamer_domain::port::{ArloEventSource, ArloStreamRequester, ArloThumbnailSource};
use streamer_infra_arlo::{
    ArloEventSourceAdapter, ArloStreamRequesterAdapter, ArloThumbnailSourceAdapter, boot::boot,
};
use streamer_infra_media::{GstMediaMultiplexer, GstPipelineRegistry, RtspServer};
use streamer_infra_ops::{Metrics, OpsServer, Readiness};

/// CLI arguments.
#[derive(Debug, Parser)]
#[command(
    name = "arlo-camera-streamer",
    version,
    about = "Bridges Arlo battery cameras to Frigate NVR via RTSP / HLS / DASH."
)]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(long, short, default_value = "/etc/arlo-streamer/streamer.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing()?;
    info!(
        version = env!("CARGO_PKG_VERSION"),
        "starting arlo-camera-streamer"
    );

    gstreamer::init().context("failed to initialize GStreamer")?;
    info!("GStreamer initialized");

    let cli = Cli::parse();
    let config = load_config(&cli.config)?;
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
    let readiness = Arc::new(Readiness::new());

    // -- Arlo adapter trio --
    let arlo_client = boot(&config.arlo)
        .await
        .context("failed to boot rs-arlo client")?;
    // One shared reqwest client so connection pooling kicks in across
    // cameras when fetching presigned thumbnail URLs from S3.
    let http = reqwest::Client::builder()
        .user_agent(concat!("arlo-camera-streamer/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("failed to build reqwest client")?;
    let event_source: Arc<dyn ArloEventSource> =
        Arc::new(ArloEventSourceAdapter::new(arlo_client.clone()));
    let stream_requester: Arc<dyn ArloStreamRequester> =
        Arc::new(ArloStreamRequesterAdapter::new(arlo_client.clone()));
    let thumbnails: Arc<dyn ArloThumbnailSource> =
        Arc::new(ArloThumbnailSourceAdapter::new(arlo_client, http));

    // -- Media adapter --
    let rtsp_server =
        RtspServer::start(&config.output.rtsp.bind).context("failed to start RTSP server")?;
    let pipeline_registry = Arc::new(GstPipelineRegistry::new(rtsp_server.clone()));
    let media: Arc<dyn streamer_domain::port::MediaMultiplexer> =
        Arc::new(GstMediaMultiplexer::new(
            pipeline_registry.clone(),
            config.output.clone(),
            &config.cameras,
        ));

    // -- Application layer --
    let system = StreamerSystem::spawn(
        &config,
        event_source.clone(),
        stream_requester,
        thumbnails,
        media,
    )
    .await
    .context("failed to spawn streamer system")?;
    readiness.mark_started();
    let shutdown = system.cancellation_token();

    // -- Background watchers --
    let conn_task = spawn_connection_watcher(
        event_source.clone(),
        metrics.clone(),
        readiness.clone(),
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

    info!("daemon ready; waiting for shutdown signal");

    // -- Wait for shutdown --
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            warn!("Ctrl-C received; initiating graceful shutdown");
        }
        () = shutdown.cancelled() => {
            warn!("system cancellation triggered shutdown");
        }
    }

    // -- Drain --
    shutdown.cancel();
    system.shutdown().await;
    if let Err(e) = conn_task.await {
        warn!(error = %e, "connection-watcher join failed");
    }
    if let Err(e) = ops_task.await {
        warn!(error = %e, "ops-server join failed");
    }
    drop(rtsp_server);
    info!("graceful shutdown complete");
    Ok(())
}

/// Subscribe to Arlo's `connection_status` watch and mirror it into
/// the readiness flag + metrics gauge.
fn spawn_connection_watcher(
    event_source: Arc<dyn ArloEventSource>,
    metrics: Arc<Metrics>,
    readiness: Arc<Readiness>,
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
                        info!(?status, "arlo connection status changed");
                    }
                    None => {
                        warn!("connection-status stream ended");
                        readiness.set_arlo_connected(false);
                        metrics.set_arlo_connected(false);
                        break;
                    }
                },
            }
        }
    })
}

fn load_config(path: &std::path::Path) -> Result<StreamerConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file: {}", path.display()))?;
    let cfg: StreamerConfig = toml::from_str(&raw)
        .with_context(|| format!("failed to parse config: {}", path.display()))?;
    Ok(cfg)
}

fn init_tracing() -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,arlo_camera_streamer=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init()
        .map_err(|e| anyhow::anyhow!("failed to init tracing: {e}"))?;
    Ok(())
}
