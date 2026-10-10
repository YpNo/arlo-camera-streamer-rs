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
//! `tests/rtsp_sessions.rs` drives it with a raw RTSP client (session
//! expiry, the two pools, one media per mount); `tests/live_session.rs`
//! with real pipelines.
//!
//! ## Why a dedicated main-loop thread?
//!
//! `gst-rtsp-server` dispatches its incoming-connection callbacks
//! through a `GMainLoop`. The Tokio runtime cannot drive it (the two
//! are different reactors), so we run a single `GLib` loop on its own
//! thread. The loop owns the `RTSPServer` source ID and stays alive
//! for the process lifetime.
//!
//! ## Two servers, one set of mounts
//!
//! LAN clients use the configured address. The daemon's own HLS
//! segmenters use a second server on `127.0.0.1` with its own session
//! pool, so LAN clients holding every session cannot starve HLS. Both
//! serve the same mount points, and the factories key their shared media
//! on the path alone (gst-rtsp-server's default key includes the port),
//! so a camera keeps one pipeline whichever server a client came in
//! through.
//!
//! ## Session expiry
//!
//! gst-rtsp-server removes an expired session only when the application
//! asks its pool to: without that, every client that vanished without a
//! TEARDOWN kept its session for ever, and the pool filled up. Each pool
//! gets a watch that removes sessions [`SESSION_TIMEOUT_SECS`] after
//! their client's last request or RTCP report. gst-rtsp-server announces
//! any timeout other than RTSP's default 60 s in every `Session` header;
//! clients send keep-alives inside it (go2rtc every timeout − 5 s, ffmpeg
//! and `rtspsrc` at half of it).

#![allow(clippy::module_name_repetitions)]

use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// Most RTSP sessions kept at once, per server. A home deployment has a
/// handful; the cap stops a client flood from growing the pool without
/// bound.
pub const MAX_RTSP_SESSIONS: u32 = 64;
/// Seconds a session lives without a request or RTCP report from its
/// client. go2rtc, which Frigate reaches the stream through, sends a
/// keep-alive every timeout − 5 s.
pub const SESSION_TIMEOUT_SECS: u32 = 30;
/// Where the daemon's own clients (the HLS segmenters) connect: loopback,
/// an ephemeral port.
const INTERNAL_HOST: &str = "127.0.0.1";
/// How long [`RtspServer::start`] waits for its main loop to dispatch.
const LOOP_START_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

use gstreamer::glib;
use gstreamer_rtsp_server::prelude::*;
use gstreamer_rtsp_server::{
    RTSPMedia, RTSPMediaFactory, RTSPMountPoints, RTSPServer, RTSPSessionPool, RTSPSuspendMode,
};
use tracing::{debug, info, warn};

use crate::error::MediaError;

/// Session limits of both servers; [`Default`] is the production setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RtspLimits {
    /// Most sessions per server ([`MAX_RTSP_SESSIONS`]).
    pub max_sessions: u32,
    /// Seconds without a keep-alive before a session is removed
    /// ([`SESSION_TIMEOUT_SECS`]).
    pub session_timeout_secs: u32,
}

impl Default for RtspLimits {
    fn default() -> Self {
        Self {
            max_sessions: MAX_RTSP_SESSIONS,
            session_timeout_secs: SESSION_TIMEOUT_SECS,
        }
    }
}

/// Started, ready-to-serve RTSP server: the public one and its loopback
/// twin for the daemon's own clients.
pub struct RtspServer {
    server: RTSPServer,
    internal: RTSPServer,
    mounts: RTSPMountPoints,
    /// The session-expiry watches, destroyed with the server.
    reapers: Vec<glib::Source>,
    /// Held so the main-loop thread isn't dropped while the server is alive.
    loop_thread: Mutex<Option<JoinHandle<()>>>,
    main_loop: glib::MainLoop,
}

impl RtspServer {
    /// Start an RTSP server bound to `host:port` and attach to the
    /// default main context. A dedicated thread runs the main loop so
    /// incoming clients are dispatched independently of Tokio.
    ///
    /// Returns only once the loop is dispatching: a [`Self::stop`] that
    /// came before the loop ran was lost (`quit` on a loop not running
    /// yet does nothing), and dropping the server then joined a thread
    /// that never ended — a boot that failed right after this call hung
    /// the process instead of exiting.
    ///
    /// `bind` accepts the same syntax as the TOML `[output.rtsp] bind`
    /// field (e.g. `"0.0.0.0:8554"`).
    ///
    /// # Errors
    ///
    /// Returns [`MediaError::Rtsp`] when the bind string is malformed,
    /// `mount_points()` is unavailable, or `attach` fails.
    pub fn start(bind: &str) -> Result<Arc<Self>, MediaError> {
        Self::start_with(bind, RtspLimits::default())
    }

    /// [`Self::start`] with explicit session limits (tests use short ones).
    ///
    /// # Errors
    ///
    /// As [`Self::start`].
    pub fn start_with(bind: &str, limits: RtspLimits) -> Result<Arc<Self>, MediaError> {
        let (host, port) = parse_bind(bind)?;
        let server = RTSPServer::new();
        let mounts = server.mount_points().ok_or_else(|| {
            MediaError::Rtsp("RTSPServer::mount_points returned None".to_string())
        })?;
        let internal = RTSPServer::new();
        let reapers = vec![
            configure(&server, &host, &port, &mounts, limits)?,
            configure(&internal, INTERNAL_HOST, "0", &mounts, limits)?,
        ];

        let main_loop = glib::MainLoop::new(None, false);
        // Dispatched by the loop once it runs: proof that a later `quit`
        // will be seen.
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<()>(1);
        glib::idle_add_once(move || {
            let _ = ready_tx.send(());
        });
        let loop_for_thread = main_loop.clone();
        let join = std::thread::Builder::new()
            .name("arlo-rtsp-loop".to_string())
            .spawn(move || {
                debug!("rtsp main loop starting");
                loop_for_thread.run();
                debug!("rtsp main loop exited");
            })
            .map_err(|e| MediaError::Rtsp(format!("failed to spawn main-loop thread: {e}")))?;
        if ready_rx.recv_timeout(LOOP_START_TIMEOUT).is_err() {
            main_loop.quit();
            return Err(MediaError::Rtsp(format!(
                "rtsp main loop did not start within {} s",
                LOOP_START_TIMEOUT.as_secs()
            )));
        }

        info!(
            host = %host,
            port = %port,
            internal_port = internal.bound_port(),
            session_timeout_secs = limits.session_timeout_secs,
            "rtsp server started"
        );
        Ok(Arc::new(Self {
            server,
            internal,
            mounts,
            reapers,
            loop_thread: Mutex::new(Some(join)),
            main_loop,
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
        let factory = shared_factory(launch);
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
        let factory = shared_factory(launch);
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

    /// URL this process reaches `mount_path` at: the loopback server,
    /// whose session pool LAN clients cannot exhaust. `None` before its
    /// socket is bound.
    #[must_use]
    pub fn loopback_url(&self, mount_path: &str) -> Option<String> {
        let port = u16::try_from(self.internal.bound_port()).ok()?;
        Some(format!("rtsp://{INTERNAL_HOST}:{port}{mount_path}"))
    }

    /// Sessions currently held: the public server's pool, then the
    /// loopback one's.
    #[must_use]
    pub fn session_counts(&self) -> (u32, u32) {
        let count = |server: &RTSPServer| server.session_pool().map_or(0, |pool| pool.n_sessions());
        (count(&self.server), count(&self.internal))
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
        for reaper in &self.reapers {
            reaper.destroy();
        }
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

/// Bind `server` to `host:port` on the shared `mounts`, cap and expire its
/// sessions, and attach it to the default main context. Returns the
/// expiry watch, attached too.
fn configure(
    server: &RTSPServer,
    host: &str,
    port: &str,
    mounts: &RTSPMountPoints,
    limits: RtspLimits,
) -> Result<glib::Source, MediaError> {
    server.set_address(host);
    server.set_service(port);
    server.set_mount_points(Some(mounts));
    let pool = server
        .session_pool()
        .ok_or_else(|| MediaError::Rtsp("RTSPServer::session_pool returned None".to_string()))?;
    pool.set_max_sessions(limits.max_sessions);
    let timeout = limits.session_timeout_secs;
    server.connect_client_connected(move |_server, client| {
        client.connect_new_session(move |_client, session| {
            session.set_timeout(timeout);
        });
    });
    let reaper = expiry_watch(&pool);
    reaper.attach(None);
    server
        .attach(None)
        .map_err(|e| MediaError::Rtsp(format!("RTSPServer::attach failed: {e}")))?;
    Ok(reaper)
}

/// Removes `pool`'s expired sessions as they expire.
fn expiry_watch(pool: &RTSPSessionPool) -> glib::Source {
    pool.create_watch(
        Some("rtsp-session-expiry"),
        glib::Priority::DEFAULT,
        |pool| {
            let removed = pool.cleanup();
            if removed > 0 {
                debug!(removed, "expired rtsp sessions removed");
            }
            glib::ControlFlow::Continue
        },
    )
}

/// A shared factory for `launch` whose media is keyed on the path alone.
fn shared_factory(launch: &str) -> RTSPMediaFactory {
    let factory: RTSPMediaFactory = path_keyed::PathKeyedFactory::default().upcast();
    factory.set_launch(launch);
    factory.set_shared(true);
    factory
}

/// The shared-media key of a request URI: its path and query, without
/// the scheme, host and port, so both servers' clients of one mount
/// share its media.
fn media_key(request_uri: &str) -> String {
    let after_scheme = request_uri
        .split_once("://")
        .map_or(request_uri, |(_, rest)| rest);
    match after_scheme.find(['/', '?']) {
        Some(at) if after_scheme[at..].starts_with('/') => after_scheme[at..].to_string(),
        Some(at) => format!("/{}", &after_scheme[at..]),
        None => "/".to_string(),
    }
}

/// `RTSPMediaFactory` whose `gen_key` ignores the port.
mod path_keyed {
    use gstreamer::glib;
    use gstreamer_rtsp_server::RTSPMediaFactory;

    glib::wrapper! {
        pub struct PathKeyedFactory(ObjectSubclass<imp::PathKeyedFactory>)
            @extends RTSPMediaFactory;
    }

    impl Default for PathKeyedFactory {
        fn default() -> Self {
            glib::Object::new()
        }
    }

    mod imp {
        use gstreamer::glib;
        use gstreamer_rtsp_server::RTSPMediaFactory;
        use gstreamer_rtsp_server::subclass::prelude::*;

        #[derive(Default)]
        pub struct PathKeyedFactory;

        #[glib::object_subclass]
        impl ObjectSubclass for PathKeyedFactory {
            const NAME: &'static str = "ArloStreamerPathKeyedFactory";
            type Type = super::PathKeyedFactory;
            type ParentType = RTSPMediaFactory;
        }

        impl ObjectImpl for PathKeyedFactory {}

        impl RTSPMediaFactoryImpl for PathKeyedFactory {
            fn gen_key(&self, url: &gstreamer_rtsp::RTSPUrl) -> Option<glib::GString> {
                Some(super::super::media_key(&url.request_uri()).into())
            }
        }
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
    fn media_key_keeps_the_path_and_query_only() {
        assert_eq!(media_key("rtsp://127.0.0.1:8554/front_door"), "/front_door");
        assert_eq!(
            media_key("rtsp://192.168.1.10:41234/front_door"),
            "/front_door"
        );
        assert_eq!(media_key("rtsp://[::1]:8554/a/b?x=1"), "/a/b?x=1");
        assert_eq!(media_key("rtsp://host:8554"), "/");
        assert_eq!(media_key("rtsp://host:8554?x=1"), "/?x=1");
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
