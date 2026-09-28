//! Wrapper around `gstreamer-rtsp-server` for the embedded RTSP service.
//!
//! Phase 4 ships a thin facade:
//!
//! - [`RtspServer::start`] binds the server, attaches it to the default
//!   main context, and spins up a dedicated thread running a
//!   [`glib::MainLoop`] so dispatch keeps happening for the daemon's
//!   lifetime.
//! - [`RtspServer::install_factory`] / [`RtspServer::remove_mount`]
//!   register and tear down the per-camera launch-string factories
//!   used by [`crate::multiplexer::PipelineRegistry`].
//!
//! Runtime validation (binding 8554, serving a real client) is Phase 5
//! work — this module is excluded from coverage by design (see
//! `.github/workflows/ci.yml`).
//!
//! ## Why a dedicated main-loop thread?
//!
//! `gst-rtsp-server` dispatches its incoming-connection callbacks
//! through a `GMainLoop`. The Tokio runtime cannot drive it (the two
//! are different reactors), so we run a single `GLib` loop on its own
//! thread. The loop owns the `RTSPServer` source ID and stays alive
//! for the process lifetime.

#![allow(clippy::module_name_repetitions)]

use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use gstreamer::glib;
use gstreamer_rtsp_server::prelude::*;
use gstreamer_rtsp_server::{
    RTSPMedia, RTSPMediaFactory, RTSPMountPoints, RTSPServer, RTSPSuspendMode,
};
use tracing::{debug, info, warn};

use crate::error::MediaError;

/// Started, ready-to-serve RTSP server.
pub struct RtspServer {
    server: RTSPServer,
    mounts: RTSPMountPoints,
    /// Held so the main-loop thread isn't dropped while the server is alive.
    loop_thread: Mutex<Option<JoinHandle<()>>>,
    main_loop: glib::MainLoop,
    /// Host the server was bound to (brackets stripped), for
    /// [`loopback_url`](Self::loopback_url).
    host: String,
}

impl RtspServer {
    /// Start an RTSP server bound to `host:port` and attach to the
    /// default main context. A dedicated thread runs the main loop so
    /// incoming clients are dispatched independently of Tokio.
    ///
    /// `bind` accepts the same syntax as the TOML `[output.rtsp] bind`
    /// field (e.g. `"0.0.0.0:8554"`).
    ///
    /// # Errors
    ///
    /// Returns [`MediaError::Rtsp`] when the bind string is malformed,
    /// `mount_points()` is unavailable, or `attach` fails.
    pub fn start(bind: &str) -> Result<Arc<Self>, MediaError> {
        let (host, port) = parse_bind(bind)?;
        let server = RTSPServer::new();
        server.set_address(&host);
        server.set_service(&port);

        let mounts = server.mount_points().ok_or_else(|| {
            MediaError::Rtsp("RTSPServer::mount_points returned None".to_string())
        })?;

        server
            .attach(None)
            .map_err(|e| MediaError::Rtsp(format!("RTSPServer::attach failed: {e}")))?;

        let main_loop = glib::MainLoop::new(None, false);
        let loop_for_thread = main_loop.clone();
        let join = std::thread::Builder::new()
            .name("arlo-rtsp-loop".to_string())
            .spawn(move || {
                debug!("rtsp main loop starting");
                loop_for_thread.run();
                debug!("rtsp main loop exited");
            })
            .map_err(|e| MediaError::Rtsp(format!("failed to spawn main-loop thread: {e}")))?;

        info!(host = %host, port = %port, "rtsp server started");
        Ok(Arc::new(Self {
            server,
            mounts,
            loop_thread: Mutex::new(Some(join)),
            main_loop,
            host,
        }))
    }

    /// Install (or replace) the factory at `mount_path` with one whose
    /// `set_launch` is the supplied description. `shared = true` so all
    /// connected clients share a single backing pipeline.
    ///
    /// # Errors
    ///
    /// Returns [`MediaError::Rtsp`] only on internal facade failures
    /// (the GStreamer call itself is infallible at this layer; bad
    /// launch strings surface to clients on connect).
    pub fn install_factory(&self, mount_path: &str, launch: &str) -> Result<(), MediaError> {
        // Replace any previous factory at the same path so transitions are atomic.
        self.mounts.remove_factory(mount_path);
        let factory = RTSPMediaFactory::new();
        factory.set_launch(launch);
        factory.set_shared(true);
        self.mounts.add_factory(mount_path, factory);
        debug!(mount = %mount_path, "rtsp factory installed");
        Ok(())
    }

    /// Install (or replace) a factory whose backing media stays alive
    /// across client connects/disconnects (`suspend-mode=NONE`) and
    /// notifies `on_media_configure` once gst-rtsp-server has
    /// instantiated the pipeline (typically on first client connect).
    /// The hook is the registry's only way to capture handles to
    /// elements named in the launch string (e.g. `appsrc` /
    /// `input-selector`) — they don't exist before construction.
    ///
    /// The hook runs on the `GLib` main-loop thread and must not block.
    ///
    /// # Errors
    ///
    /// Same envelope as [`Self::install_factory`].
    pub fn install_factory_with_media_hook<F>(
        &self,
        mount_path: &str,
        launch: &str,
        on_media_configure: F,
    ) -> Result<(), MediaError>
    where
        F: Fn(&RTSPMedia) + Send + Sync + 'static,
    {
        self.mounts.remove_factory(mount_path);
        let factory = RTSPMediaFactory::new();
        factory.set_launch(launch);
        factory.set_shared(true);
        // Keep the pipeline running when the last client disconnects so
        // the appsrc/input-selector handles captured below stay valid
        // and so a re-connecting client picks up the existing splice
        // state without rebuilding.
        factory.set_suspend_mode(RTSPSuspendMode::None);
        factory.connect_media_configure(move |_factory, media| {
            on_media_configure(media);
        });
        self.mounts.add_factory(mount_path, factory);
        debug!(mount = %mount_path, "rtsp factory installed (with media hook)");
        Ok(())
    }

    /// Remove a mount point. Idempotent.
    pub fn remove_mount(&self, mount_path: &str) {
        self.mounts.remove_factory(mount_path);
        debug!(mount = %mount_path, "rtsp factory removed");
    }

    /// The TCP port the server listens on, which differs from the
    /// configured one when that was `0` (tests bind an ephemeral port).
    /// `None` before the socket is bound.
    #[must_use]
    pub fn bound_port(&self) -> Option<u16> {
        u16::try_from(self.server.bound_port()).ok()
    }

    /// URL this process reaches `mount_path` at: the bound port on the
    /// loopback address when the server listens on every interface.
    /// `None` before the socket is bound.
    #[must_use]
    pub fn loopback_url(&self, mount_path: &str) -> Option<String> {
        let port = self.bound_port()?;
        Some(format!(
            "rtsp://{}:{port}{mount_path}",
            loopback_host(&self.host)
        ))
    }

    /// Stop the main loop. Called during graceful shutdown.
    pub fn stop(&self) {
        if self.main_loop.is_running() {
            self.main_loop.quit();
            info!("rtsp main loop quit");
        }
    }
}

impl Drop for RtspServer {
    fn drop(&mut self) {
        self.stop();
        if let Ok(mut guard) = self.loop_thread.lock()
            && let Some(handle) = guard.take()
            && let Err(e) = handle.join()
        {
            warn!(?e, "rtsp main loop thread join failed");
        }
        // The `RTSPServer` itself is reference-counted by GLib and will
        // be released when its source is detached from the (now-stopped)
        // main context.
        let _ = &self.server;
    }
}

/// Split `host:port` into typed parts. Accepts IPv4 and bracketed-IPv6.
/// The host to dial for a server bound to `host`: wildcards map to the
/// loopback address of their family, IPv6 literals get brackets.
fn loopback_host(host: &str) -> String {
    match host {
        "0.0.0.0" => "127.0.0.1".to_string(),
        "::" => "[::1]".to_string(),
        h if h.contains(':') => format!("[{h}]"),
        h => h.to_string(),
    }
}

fn parse_bind(bind: &str) -> Result<(String, String), MediaError> {
    // Walk from the right so IPv6 colons don't confuse us.
    let (host, port) = bind
        .rsplit_once(':')
        .ok_or_else(|| MediaError::Rtsp(format!("bind missing ':port' — got '{bind}'")))?;
    if host.is_empty() {
        return Err(MediaError::Rtsp(format!(
            "bind missing host — got '{bind}'"
        )));
    }
    if port.is_empty() {
        return Err(MediaError::Rtsp(format!(
            "bind missing port — got '{bind}'"
        )));
    }
    if port.parse::<u16>().is_err() {
        return Err(MediaError::Rtsp(format!(
            "bind port not a u16 — got '{port}' in '{bind}'"
        )));
    }
    // Strip IPv6 brackets if present.
    let host = host.trim_start_matches('[').trim_end_matches(']');
    Ok((host.to_string(), port.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_host_maps_wildcards_and_brackets_ipv6() {
        assert_eq!(loopback_host("0.0.0.0"), "127.0.0.1");
        assert_eq!(loopback_host("::"), "[::1]");
        assert_eq!(loopback_host("fd00::5"), "[fd00::5]");
        assert_eq!(loopback_host("192.168.1.10"), "192.168.1.10");
        assert_eq!(loopback_host("localhost"), "localhost");
    }

    #[test]
    fn parse_bind_ipv4() {
        let (host, port) = parse_bind("0.0.0.0:8554").unwrap();
        assert_eq!(host, "0.0.0.0");
        assert_eq!(port, "8554");
    }

    #[test]
    fn parse_bind_ipv4_loopback() {
        let (host, port) = parse_bind("127.0.0.1:9000").unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, "9000");
    }

    #[test]
    fn parse_bind_ipv6_strips_brackets() {
        let (host, port) = parse_bind("[::1]:8554").unwrap();
        assert_eq!(host, "::1");
        assert_eq!(port, "8554");
    }

    #[test]
    fn parse_bind_rejects_missing_port() {
        let err = parse_bind("0.0.0.0").unwrap_err();
        assert!(matches!(err, MediaError::Rtsp(_)));
    }

    #[test]
    fn parse_bind_rejects_empty_port() {
        let err = parse_bind("0.0.0.0:").unwrap_err();
        assert!(matches!(err, MediaError::Rtsp(_)));
    }

    #[test]
    fn parse_bind_rejects_empty_host() {
        let err = parse_bind(":8554").unwrap_err();
        assert!(matches!(err, MediaError::Rtsp(_)));
    }

    #[test]
    fn parse_bind_rejects_non_u16_port() {
        let err = parse_bind("0.0.0.0:notaport").unwrap_err();
        assert!(matches!(err, MediaError::Rtsp(_)));
        let err = parse_bind("0.0.0.0:99999").unwrap_err();
        assert!(matches!(err, MediaError::Rtsp(_)));
    }
}
