//! Production [`PipelineRegistry`] backed by GStreamer + the embedded
//! RTSP server.
//!
//! ## Splice strategy (Phase 7 — unified encoder)
//!
//! Each camera owns **one persistent factory** whose
//! [`crate::pipeline_desc::combined_launch_string`] hosts:
//! - an idle branch producing **raw I420** at the unified caps,
//! - a live branch that decodes inbound H.264 RTP and normalizes it
//!   to the same raw caps,
//! - an `input-selector` switching between them,
//! - a **single** downstream `x264enc` producing the H.264 output.
//!
//! Because RTSP clients see one continuous encoder with one SPS/PPS,
//! idle↔live transitions never trigger a decoder re-initialization
//! on the client side. Each splice fires a force-key-unit at the
//! encoder so the next encoded frame is a clean IDR.
//!
//! Audio goes through an `audiomixer`: a silent bed always, plus the
//! camera's decoded Opus while live (Phase 8b — see the
//! `pipeline_desc` module-level note).
//!
//! Lifecycle (per camera):
//!
//! 1. [`register`](PipelineRegistry::register) installs the factory at `/<stream_name>` with
//!    a `media-configure` hook. `suspend-mode=None` is set so the
//!    pipeline stays running across client disconnects.
//! 2. On first client connect, gst-rtsp-server constructs the media
//!    and our hook captures handles to `appsrc name=live_rtp_src`,
//!    `input-selector name=sel` (sink pads), and the downstream
//!    encoder `x264enc name=video_enc`.
//! 3. [`attach_live_sink`](PipelineRegistry::attach_live_sink) hands back a
//!    [`LiveSinks`] pair — each
//!    receiver is drained by a tokio pump that pushes into the live
//!    `appsrc` of **whichever media currently exists** (looked up per
//!    buffer). A pad probe on `sel.sink_1` waits
//!    for the first decoded raw buffer, flips `active-pad = sink_1`,
//!    and force-key-units the encoder. The probe self-removes.
//! 4. [`detach_live_sink`](PipelineRegistry::detach_live_sink) flips `active-pad = sink_0`
//!    synchronously, force-key-units the encoder, and aborts the
//!    live pump.
//!
//! Media lifecycle vs live session: gst-rtsp-server builds the media
//! when the first client connects and unprepares it after the last one
//! leaves, so a live session can start with no media at all, or outlive
//! the media it started with. The wiring slot therefore always holds the
//! *current* media (set at `media-configure`, cleared at `unprepared`),
//! the pumps discard while it is empty, and a media configured while a
//! session is live gets its live switch armed at once. A client that
//! connects in the middle of a session sees the live video within one
//! keyframe interval.
//!
//! ## Thumbnail handling (Phase 5)
//!
//! [`refresh_thumbnail`](PipelineRegistry::refresh_thumbnail) persists the JPEG to a stable per-camera
//! file and points the synthetic idle branch's `gdkpixbufoverlay`
//! (`idle_overlay`) at it, so the STANDBY screen shows the camera's
//! last snapshot instead of a black frame. The overlay is an inline
//! filter: before the first snapshot it's a transparent pass-through,
//! so idle preroll is unchanged. A rebuilt media re-applies the file at
//! `media-configure`. The cached bytes are also kept in per-camera
//! state for a future `/admin/thumbnail/<cam>` endpoint.
//!
//! Exercised by `tests/live_session.rs`: a real RTSP client watches
//! the idle → live → idle splice and a client joining mid-session.

#![allow(clippy::module_name_repetitions)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Local;
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use gstreamer_rtsp_server::RTSPMedia;
use gstreamer_rtsp_server::prelude::*;
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, info, instrument, warn};

use streamer_domain::camera::CameraId;
use streamer_domain::thumbnail::check_thumbnail;

use crate::encoder::EncoderBackend;
use crate::error::MediaError;
use crate::hls::{HlsSegmenter, prepare_dir};
use crate::idle_source::{IdleKind, SYNTHETIC_HEIGHT, SYNTHETIC_WIDTH, standby_caption};
use crate::live_rtp_sink::{AacFeed, AacRtpFormat, LiveSinkReceivers, LiveSinks};
use crate::multiplexer::PipelineRegistry;
use crate::pipeline_desc::{
    HlsBranchConfig, IDLE_CAPTION_NAME, IDLE_OVERLAY_NAME, LIVE_AAC_SRC_NAME, LIVE_RTP_AAC_PT,
    OutputBranches, UNIFIED_ENCODER_NAME, combined_launch_string,
};
use crate::rtsp::RtspServer;

/// Live wiring captured from the gst-rtsp-server media on
/// `media-configure`. Cloning the handles is cheap (`GObject` reference
/// counting) and lets us share them between the `GLib` thread (where
/// they're produced) and tokio tasks (where they're consumed).
#[derive(Clone)]
struct LiveWiring {
    /// The media's top-level element: identifies which media this wiring
    /// belongs to, so an `unprepared` of an older media never clears the
    /// wiring of a newer one.
    media_element: gst::Element,
    appsrc: gst_app::AppSrc,
    selector: gst::Element,
    /// `sel.sink_0` — idle branch input.
    sink_idle: gst::Pad,
    /// `sel.sink_1` — live branch input.
    sink_live: gst::Pad,
    /// The shared downstream encoder (Phase 7). We dispatch a
    /// force-key-unit to it on every splice so the next encoded frame
    /// is an IDR — no garbage P-frames referencing the prior branch's
    /// content.
    encoder: gst::Element,
    /// The synthetic idle branch's `gdkpixbufoverlay` (Phase 5). Its
    /// `location` is set to the latest camera snapshot in
    /// [`GstPipelineRegistry::refresh_thumbnail`] so the STANDBY screen
    /// shows the last thumbnail. Present only for the synthetic idle
    /// variant; the JPEG-still idle already shows an image.
    idle_overlay: Option<gst::Element>,
    /// The synthetic idle branch's caption `textoverlay`; its `text` is
    /// switched by [`GstPipelineRegistry::set_idle_caption`].
    idle_caption: Option<gst::Element>,
    /// The live audio `appsrc` (`live_audio_rtp_src`, Phase 8b) — Opus
    /// RTP pushed here is decoded and mixed onto the silent bed by the
    /// `audiomixer`. No selector flip needed: the mixer reverts to
    /// silence when live audio stops.
    audio_appsrc: Option<gst_app::AppSrc>,
    /// The relayed-audio `appsrc` (`live_aac_rtp_src`, ADR 0007) — AAC
    /// RTP from the app-view relay, decoded onto the mixer's third pad.
    aac_appsrc: Option<gst_app::AppSrc>,
    /// The armed idle → live switch probe on `sink_live`, until it fires;
    /// a detach before the first live frame removes it.
    live_switch: PendingSwitch,
}

/// The id of a live-switch probe that has not fired yet. Whoever takes
/// it — the probe on the first live frame, or a detach — owns the switch.
type PendingSwitch = Arc<StdMutex<Option<gst::PadProbeId>>>;

/// Active live ingestion bookkeeping.
struct LiveSession {
    /// Drains the video sink's receiver into the video `appsrc`.
    video: tokio::task::JoinHandle<()>,
    /// Drains the audio sink's receiver into the audio `appsrc`.
    audio: tokio::task::JoinHandle<()>,
    /// Drains the AAC sink's receiver into the AAC `appsrc`.
    aac: tokio::task::JoinHandle<()>,
}

/// Per-camera state owned by [`GstPipelineRegistry`].
struct CameraEntry {
    mount_path: String,
    idle: IdleKind,
    /// Last known thumbnail JPEG. Applied to the idle overlay in
    /// `refresh_thumbnail` and retained for a future ops endpoint.
    last_thumbnail: Option<Bytes>,
    /// Set when gst-rtsp-server constructs a media for this factory.
    /// Read by `attach_live_sink` to find the appsrc + selector pads.
    /// `StdMutex` so the `GLib` callback (sync) and tokio tasks (which
    /// only hold it for read snapshots, no `await` in scope) can share.
    wiring: WiringSlot,
    /// `true` while a live session is attached. Read by the
    /// `media-configure` hook to arm the live switch on a media built in
    /// the middle of a session.
    live_active: Arc<AtomicBool>,
    /// Active live ingestion if any.
    session: Option<LiveSession>,
    /// Caption the idle frame should show, when it differs from the one
    /// in the launch string (`None`). Read by the `media-configure` hook
    /// so a media built later shows it too.
    caption: CaptionSlot,
    /// HLS segmenter when `[output.hls]` is set (ADR 0006). Held for its
    /// lifetime: dropping it stops the segmenter.
    _hls: Option<HlsSegmenter>,
}

/// The idle caption requested for a camera, shared with its
/// `media-configure` hook.
type CaptionSlot = Arc<StdMutex<Option<String>>>;

/// The wiring of the media currently serving this camera's clients, or
/// `None` while no client is connected. Shared by the `GLib` media hooks,
/// the tokio pumps and the registry.
type WiringSlot = Arc<StdMutex<Option<LiveWiring>>>;

/// Lock a mutex, recovering the data if a panicking holder poisoned it:
/// every critical section here is a plain read or a single assignment.
fn lock<T>(m: &StdMutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// GStreamer-backed [`PipelineRegistry`].
pub struct GstPipelineRegistry {
    server: Arc<RtspServer>,
    /// H.264 encoder backend resolved at boot ([`crate::encoder::resolve`]),
    /// baked into every camera's persistent launch string.
    video_encoder: EncoderBackend,
    /// Directory holding the per-camera snapshot JPEGs the idle overlay
    /// loads; prepared by [`prepare_thumbnail_dir`] before this exists.
    thumbnail_dir: PathBuf,
    state: RwLock<HashMap<CameraId, CameraEntry>>,
}

impl GstPipelineRegistry {
    /// Construct from a started RTSP server, the resolved video encoder
    /// backend and the directory for the idle thumbnails (see
    /// [`prepare_thumbnail_dir`]).
    #[must_use]
    pub fn new(
        server: Arc<RtspServer>,
        video_encoder: EncoderBackend,
        thumbnail_dir: PathBuf,
    ) -> Self {
        Self {
            server,
            video_encoder,
            thumbnail_dir,
            state: RwLock::new(HashMap::new()),
        }
    }

    /// Stable per-camera path where the latest snapshot JPEG is kept for
    /// the idle `gdkpixbufoverlay` to load. The id is one path component:
    /// every id from a trust boundary went through `CameraId::parse`
    /// (`[A-Za-z0-9_-]`).
    fn thumbnail_file_path(&self, camera: &CameraId) -> PathBuf {
        self.thumbnail_dir
            .join(format!("arlo-streamer-thumb-{camera}.jpg"))
    }

    /// Check that HLS can run for a camera about to be registered: the
    /// RTSP port is known and the per-stream directory is usable (created,
    /// stale files cleared). Runs before the mount is installed, so a
    /// failure leaves nothing behind. Returns the segmenter's source URL.
    async fn prepare_hls(
        &self,
        mount_path: &str,
        hls: &HlsBranchConfig,
    ) -> Result<String, MediaError> {
        let url = self
            .server
            .loopback_url(mount_path)
            .ok_or_else(|| MediaError::Rtsp("RTSP server not bound; HLS needs its port".into()))?;
        let prepared = hls.clone();
        tokio::task::spawn_blocking(move || prepare_dir(&prepared))
            .await
            .map_err(|e| MediaError::Pipeline(format!("HLS dir preparation task: {e}")))??;
        Ok(url)
    }

    fn refresh_overlay_timestamp(idle: &IdleKind) -> IdleKind {
        match idle {
            IdleKind::Synthetic { stream_name, .. } => {
                let ts = Local::now().format("%Y-%m-%dT%H:%M:%S").to_string();
                IdleKind::Synthetic {
                    stream_name: stream_name.clone(),
                    overlay: standby_caption(stream_name, &ts),
                }
            }
            // JPEG-still keeps its bytes; overlay is N/A.
            IdleKind::JpegStill { .. } => idle.clone(),
        }
    }

    /// Drop all mount points (called during graceful shutdown).
    pub async fn shutdown(&self) {
        let mut guard = self.state.write().await;
        for (cam, entry) in guard.drain() {
            self.server.remove_mount(&entry.mount_path);
            if let Some(session) = entry.session {
                session.video.abort();
                session.audio.abort();
                session.aac.abort();
            }
            debug!(camera = %cam, mount = %entry.mount_path, "mount removed during shutdown");
        }
    }
}

/// The per-camera handles the `media-configure` hook writes to or reads.
struct MediaHooks {
    wiring: WiringSlot,
    live_active: Arc<AtomicBool>,
    caption: CaptionSlot,
    thumbnail_path: PathBuf,
}

impl GstPipelineRegistry {
    /// The slow half of a registration, run without the registry lock:
    /// prepare the HLS directory, install the mount with its hook, start
    /// the segmenter. A failure after the mount is installed removes it
    /// again ([`MountGuard`]).
    async fn bring_up(
        &self,
        camera: &CameraId,
        idle: &IdleKind,
        launch: &str,
        outputs: OutputBranches,
        hooks: MediaHooks,
    ) -> Result<Option<HlsSegmenter>, MediaError> {
        let hls_url = match &outputs.hls {
            Some(hls) => Some(self.prepare_hls(&outputs.rtsp_mount_path, hls).await?),
            None => None,
        };
        let MediaHooks {
            wiring,
            live_active,
            caption,
            thumbnail_path: thumb_path_for_cb,
        } = hooks;
        discard_unsafe_thumbnail(&thumb_path_for_cb).await;
        let caption_for_cb = caption;
        let wiring_for_cb = wiring;
        let live_for_cb = live_active;
        let cam_for_cb = camera.clone();
        self.server.install_factory_with_media_hook(
            &outputs.rtsp_mount_path,
            launch,
            move |media: &RTSPMedia| {
                match build_live_wiring(media) {
                    Ok(w) => {
                        // Re-apply the last snapshot to a freshly-built
                        // media so a rebuild between live sessions keeps
                        // showing the thumbnail rather than reverting to
                        // the black STANDBY frame.
                        if let Some(overlay) = &w.idle_overlay {
                            load_thumbnail_detached(overlay.clone(), thumb_path_for_cb.clone());
                        }
                        if let (Some(text), Some(el)) =
                            (lock(&caption_for_cb).as_deref(), &w.idle_caption)
                        {
                            el.set_property("text", text);
                        }
                        // A client connected in the middle of a live
                        // session: splice the live video into this new
                        // media as soon as its first frame arrives.
                        if live_for_cb.load(Ordering::SeqCst) {
                            arm_live_switch(&w);
                            info!(camera = %cam_for_cb, "media built during a live session; live switch armed");
                        }
                        *lock(&wiring_for_cb) = Some(w);
                        debug!(camera = %cam_for_cb, "captured live wiring from media");
                    }
                    Err(e) => {
                        warn!(camera = %cam_for_cb, error = %e, "failed to capture live wiring");
                    }
                }
                // Attach a bus watch so we surface pipeline
                // `ERROR`/`WARNING` messages via `tracing`. Without
                // this the media dies silently and gst-rtsp-server
                // just rebuilds it, giving no clue why.
                attach_media_bus_watch(media, &cam_for_cb, wiring_for_cb.clone());
            },
        )?;
        // From here until the entry is stored, any failure must take the
        // mount down again: a published mount the registry does not know
        // would serve clients while attach/detach fail with UnknownCamera.
        let mut mount = MountGuard::new(self.server.clone(), outputs.rtsp_mount_path.clone());
        info!(mount = %outputs.rtsp_mount_path, idle = idle.kind_label(), "camera registered");

        let hls = match (outputs.hls, hls_url) {
            (Some(hls), Some(url)) => Some(HlsSegmenter::start(camera.clone(), url, hls)?),
            _ => None,
        };
        if outputs.dash.is_some() {
            warn!(
                camera = %camera,
                "DASH output is not supported and is ignored: without gst-plugins-rs, \
                 GStreamer's dashsink writes only MPEG-TS fragments that browser players \
                 reject, and never deletes them (ADR 0006); use [output.hls]"
            );
        }
        mount.disarm();
        Ok(hls)
    }
}

#[async_trait]
impl PipelineRegistry for GstPipelineRegistry {
    #[instrument(skip(self, idle, outputs), fields(camera = %camera))]
    async fn register(
        &self,
        camera: &CameraId,
        idle: IdleKind,
        outputs: OutputBranches,
    ) -> Result<(), MediaError> {
        // Refresh the standby clock so the very first frame after
        // register reflects the current time.
        let idle = Self::refresh_overlay_timestamp(&idle);
        let launch = combined_launch_string(&idle, self.video_encoder);
        debug!(camera = %camera, launch_string = %launch, "combined launch string");

        let wiring: WiringSlot = Arc::new(StdMutex::new(None));
        let live_active = Arc::new(AtomicBool::new(false));
        let caption: CaptionSlot = Arc::new(StdMutex::new(None));
        // Claim the camera under a short write lock: a concurrent
        // registration of the same id stops at the entry, and the other
        // cameras' attach/detach/thumbnail calls are not held up by the
        // slow part below (directories, factory, segmenter thread). The
        // entry is completed on success and removed on failure.
        {
            let mut state = self.state.write().await;
            if state.contains_key(camera) {
                return Err(MediaError::AlreadyRegistered(camera.to_string()));
            }
            state.insert(
                camera.clone(),
                CameraEntry {
                    mount_path: outputs.rtsp_mount_path.clone(),
                    idle: idle.clone(),
                    last_thumbnail: None,
                    wiring: wiring.clone(),
                    live_active: live_active.clone(),
                    session: None,
                    caption: caption.clone(),
                    _hls: None,
                },
            );
        }
        let hooks = MediaHooks {
            wiring,
            live_active,
            caption,
            thumbnail_path: self.thumbnail_file_path(camera),
        };
        match self.bring_up(camera, &idle, &launch, outputs, hooks).await {
            Ok(hls) => {
                if let Some(entry) = self.state.write().await.get_mut(camera) {
                    entry._hls = hls;
                }
                Ok(())
            }
            Err(e) => {
                self.state.write().await.remove(camera);
                Err(e)
            }
        }
    }

    #[instrument(skip(self), fields(camera = %camera))]
    async fn attach_live_sink(&self, camera: &CameraId) -> Result<LiveSinks, MediaError> {
        let mut guard = self.state.write().await;
        let entry = guard
            .get_mut(camera)
            .ok_or_else(|| MediaError::UnknownCamera(camera.to_string()))?;
        if entry.session.is_some() {
            return Err(MediaError::Pipeline(format!(
                "camera {camera} already in live mode"
            )));
        }

        let (sinks, rxs) = LiveSinks::new();
        let LiveSinkReceivers {
            video: video_rx,
            audio: audio_rx,
            aac: aac_rx,
        } = rxs;
        // Flag first, then look: a media configured concurrently either
        // sees the flag (and arms itself) or is already in the slot.
        entry.live_active.store(true, Ordering::SeqCst);
        if let Some(wiring) = lock(&entry.wiring).as_ref() {
            arm_live_switch(wiring);
        } else {
            info!(
                camera = %camera,
                "no RTSP client connected yet; the live video is spliced in when one connects"
            );
        }
        let session = LiveSession {
            video: spawn_live_pump(video_rx, entry.wiring.clone(), video_appsrc, "video"),
            // Audio has no idle↔live selector — the audiomixer blends the
            // live Opus onto silence; a media without the audio appsrc
            // (pre-8b launch) simply discards it.
            audio: spawn_live_pump(audio_rx, entry.wiring.clone(), audio_appsrc, "audio"),
            aac: spawn_aac_pump(aac_rx, entry.wiring.clone()),
        };
        entry.session = Some(session);
        info!(mount = %entry.mount_path, "live ingestion armed (video + audio)");
        Ok(sinks)
    }

    #[instrument(skip(self), fields(camera = %camera))]
    async fn detach_live_sink(&self, camera: &CameraId) -> Result<(), MediaError> {
        let mut guard = self.state.write().await;
        let entry = guard
            .get_mut(camera)
            .ok_or_else(|| MediaError::UnknownCamera(camera.to_string()))?;

        // Refresh the overlay clock so the standby timestamp updates
        // the next time the synthetic IDR cycle inlines a frame.
        entry.idle = Self::refresh_overlay_timestamp(&entry.idle);

        let Some(session) = entry.session.take() else {
            debug!(camera = %camera, "detach_live_sink: no active session (idempotent no-op)");
            return Ok(());
        };
        // Clear the flag before flipping back, so a media configured from
        // now on starts (and stays) on the idle branch.
        entry.live_active.store(false, Ordering::SeqCst);

        let wiring_snapshot = lock(&entry.wiring).clone();
        if let Some(wiring) = wiring_snapshot {
            // A switch armed for a live frame that never came would
            // otherwise wait on the pad (and fire on the next session's
            // first frame out of turn).
            if disarm_switch_probe(&wiring.sink_live, &wiring.live_switch) {
                debug!("pending live switch removed: no live frame arrived");
            }
            // Synchronous flip on the video selector: raw I420 frames
            // are independently complete, so any boundary works. A
            // force-key-unit on the encoder gives clients a clean IDR
            // right after the content swap.
            wiring
                .selector
                .set_property("active-pad", &wiring.sink_idle);
            force_keyframe(&wiring.encoder);
            debug!("input-selector flipped to sink_0 (idle); IDR forced on encoder");
        }

        // Aborting the pumps drops the receivers; the GStreamer
        // streaming thread's appsink callbacks see `LiveRtpSink::push`
        // return `false` and silently discard (no back-pressure). The
        // audiomixer stops receiving live buffers and reverts to the
        // silent bed — no explicit audio flip needed.
        session.video.abort();
        session.audio.abort();
        session.aac.abort();
        info!(mount = %entry.mount_path, "live ingestion released; idle restored");
        Ok(())
    }

    #[instrument(skip(self, jpeg), fields(camera = %camera, bytes = jpeg.len()))]
    async fn refresh_thumbnail(&self, camera: &CameraId, jpeg: Bytes) -> Result<(), MediaError> {
        // Persist the JPEG to a stable per-camera path, then point the
        // idle branch's `gdkpixbufoverlay` at it so the STANDBY screen
        // shows the last snapshot (Phase 5). The write is atomic
        // (temp + rename) so the overlay never reads a partial file.
        // Whatever the source, gdk-pixbuf decodes at the declared size:
        // the header is bounded before the file exists.
        check_thumbnail(&jpeg).map_err(|e| MediaError::InvalidThumbnail(e.to_string()))?;
        let path = self.thumbnail_file_path(camera);
        write_thumbnail_atomic(&path, &jpeg).await?;

        // Take the overlay under the lock, decode without it: the load
        // blocks for the decode, and attach/detach of every camera wait
        // on this lock.
        let overlay = {
            let mut guard = self.state.write().await;
            let entry = guard
                .get_mut(camera)
                .ok_or_else(|| MediaError::UnknownCamera(camera.to_string()))?;
            entry.last_thumbnail = Some(jpeg);
            lock(&entry.wiring)
                .as_ref()
                .and_then(|w| w.idle_overlay.clone())
        };

        // Apply to the running media if one is up. If no client has
        // connected yet the file is already in place and the overlay
        // is re-applied at the next `media-configure` (see `register`).
        let Some(overlay) = overlay else {
            debug!(
                path = %path.display(),
                "thumbnail stored; no live overlay yet (applied on next media build)"
            );
            return Ok(());
        };
        let shown = path.clone();
        tokio::task::spawn_blocking(move || apply_thumbnail_overlay(&overlay, &shown))
            .await
            .map_err(|e| MediaError::Pipeline(format!("thumbnail load task failed: {e}")))?;
        debug!(path = %path.display(), "thumbnail applied to idle overlay");
        Ok(())
    }

    async fn set_idle_caption(&self, camera: &CameraId, caption: String) -> Result<(), MediaError> {
        let guard = self.state.read().await;
        let entry = guard
            .get(camera)
            .ok_or_else(|| MediaError::UnknownCamera(camera.to_string()))?;
        if let Some(el) = lock(&entry.wiring)
            .as_ref()
            .and_then(|w| w.idle_caption.clone())
        {
            el.set_property("text", &caption);
        }
        debug!(camera = %camera, %caption, "idle caption set");
        *lock(&entry.caption) = Some(caption);
        Ok(())
    }
}

/// Connect diagnostic signals on the freshly-constructed `RTSPMedia`
/// so we can see, in the daemon log, whether the pipeline actually
/// reaches PLAYING, and when it gets torn down (`unprepared`).
///
/// We deliberately do **not** attach a `gst::Bus` watch here —
/// `media-configure` fires on a `gst_thread_pool` worker, but the
/// default `GLib` main context is owned by the RTSP main-loop
/// thread, so `Bus::add_watch_local` panics with
/// "main context already acquired". The `RTSPMedia` `GObject`
/// signals used below are dispatched via `GLib` signal emission,
/// which is thread-safe.
fn attach_media_bus_watch(media: &RTSPMedia, camera: &CameraId, wiring: WiringSlot) {
    let cam_prep = camera.clone();
    media.connect_prepared(move |_media| {
        info!(camera = %cam_prep, "RTSPMedia prepared (pipeline reached PLAYING)");
    });
    let cam_unprep = camera.clone();
    media.connect_unprepared(move |media| {
        // Forget this media's wiring (only if a newer media has not
        // replaced it yet): the pumps then discard instead of pushing
        // into a torn-down appsrc.
        let element = media.element();
        let mut slot = lock(&wiring);
        if slot.as_ref().is_some_and(|w| w.media_element == element) {
            *slot = None;
        }
        warn!(camera = %cam_unprep, "RTSPMedia unprepared (pipeline torn down)");
    });
    let cam_state = camera.clone();
    media.connect_new_state(move |_media, state| {
        debug!(camera = %cam_state, state, "RTSPMedia new-state");
    });
}

/// Look up the live appsrc + input-selector + branch sink pads inside
/// a freshly-constructed `RTSPMedia`'s pipeline.
fn build_live_wiring(media: &RTSPMedia) -> Result<LiveWiring, MediaError> {
    let media_element = media.element();
    let bin: gst::Bin = media_element
        .clone()
        .dynamic_cast::<gst::Bin>()
        .map_err(|_| MediaError::Pipeline("media element is not a Bin".into()))?;

    let appsrc_el = bin
        .by_name("live_rtp_src")
        .ok_or_else(|| MediaError::Pipeline("appsrc 'live_rtp_src' not found".into()))?;
    let appsrc: gst_app::AppSrc = appsrc_el
        .dynamic_cast::<gst_app::AppSrc>()
        .map_err(|_| MediaError::Pipeline("'live_rtp_src' is not an AppSrc".into()))?;
    bound_appsrc(&appsrc);

    let selector = bin
        .by_name("sel")
        .ok_or_else(|| MediaError::Pipeline("input-selector 'sel' not found".into()))?;
    let sink_idle = selector
        .static_pad("sink_0")
        .ok_or_else(|| MediaError::Pipeline("input-selector has no sink_0".into()))?;
    let sink_live = selector
        .static_pad("sink_1")
        .ok_or_else(|| MediaError::Pipeline("input-selector has no sink_1".into()))?;
    let encoder = bin.by_name(UNIFIED_ENCODER_NAME).ok_or_else(|| {
        MediaError::Pipeline(format!("encoder '{UNIFIED_ENCODER_NAME}' not found"))
    })?;
    // Optional: only the synthetic idle branch carries the overlays.
    let idle_overlay = bin.by_name(IDLE_OVERLAY_NAME);
    let idle_caption = bin.by_name(IDLE_CAPTION_NAME);

    // Optional: live audio appsrcs (Opus from WebRTC, AAC from the
    // relay), bounded like the video one.
    let audio_appsrc = bin
        .by_name("live_audio_rtp_src")
        .and_then(|el| el.dynamic_cast::<gst_app::AppSrc>().ok());
    if let Some(a) = &audio_appsrc {
        bound_appsrc(a);
    }
    let aac_appsrc = bin
        .by_name(LIVE_AAC_SRC_NAME)
        .and_then(|el| el.dynamic_cast::<gst_app::AppSrc>().ok());
    if let Some(a) = &aac_appsrc {
        bound_appsrc(a);
    }

    Ok(LiveWiring {
        media_element,
        appsrc,
        selector,
        sink_idle,
        sink_live,
        encoder,
        idle_overlay,
        idle_caption,
        audio_appsrc,
        aac_appsrc,
        live_switch: PendingSwitch::default(),
    })
}

/// Most bytes a live appsrc queues before it drops. RTP keeps arriving
/// while the live branch is not consuming (a selector flip, a stalled
/// decoder); two seconds of a 4 Mbit/s camera is about 1 MiB.
const LIVE_APPSRC_MAX_BYTES: u64 = 4 * 1024 * 1024;
/// Most buffers a live appsrc queues before it drops (one RTP packet
/// per buffer).
const LIVE_APPSRC_MAX_BUFFERS: u64 = 2048;

/// Make a live appsrc drop instead of growing or blocking. `block=false`
/// alone only keeps the push from blocking: without `leaky-type` the
/// queue grows past `max-bytes` whenever the consumer stops.
fn bound_appsrc(src: &gst_app::AppSrc) {
    src.set_property("block", false);
    src.set_property("max-bytes", LIVE_APPSRC_MAX_BYTES);
    src.set_property("max-buffers", LIVE_APPSRC_MAX_BUFFERS);
    src.set_property_from_str("leaky-type", "downstream");
}

/// What a [`MountGuard`] needs from the server, so the guard's drop
/// semantics are testable without GStreamer.
trait RemoveMount {
    fn remove_mount(&self, mount_path: &str);
}

impl RemoveMount for Arc<RtspServer> {
    fn remove_mount(&self, mount_path: &str) {
        RtspServer::remove_mount(self, mount_path);
    }
}

/// Removes a freshly installed mount unless disarmed: registration can
/// still fail after `install_factory_with_media_hook`, and the mount
/// must not outlive the failed registration.
struct MountGuard<S: RemoveMount> {
    server: S,
    mount_path: String,
    armed: bool,
}

impl<S: RemoveMount> MountGuard<S> {
    fn new(server: S, mount_path: String) -> Self {
        Self {
            server,
            mount_path,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<S: RemoveMount> Drop for MountGuard<S> {
    fn drop(&mut self) {
        if self.armed {
            warn!(mount = %self.mount_path, "registration failed after the mount was installed; removing it");
            self.server.remove_mount(&self.mount_path);
        }
    }
}

/// Owner-only mode for the thumbnail directory and files: the images
/// are loaded by gdk-pixbuf, so nobody else on the host may plant one.
#[cfg(unix)]
const THUMBNAIL_DIR_MODE: u32 = 0o700;
#[cfg(unix)]
const THUMBNAIL_FILE_MODE: u32 = 0o600;

/// Create the directory the idle thumbnails are written to and restrict
/// it to the owner. Called once at boot by the composition root, before
/// the registry is built; the files from a previous run are kept so the
/// first STANDBY frame already shows the last snapshot.
///
/// # Errors
///
/// [`MediaError::Pipeline`] when the directory cannot be created or is
/// not a directory. A failure to restrict its mode is logged, not fatal:
/// the directory may live on a filesystem without POSIX modes.
pub fn prepare_thumbnail_dir(dir: &Path) -> Result<(), MediaError> {
    std::fs::create_dir_all(dir).map_err(|e| {
        MediaError::Pipeline(format!(
            "thumbnail directory {} cannot be created: {e}",
            dir.display()
        ))
    })?;
    if !dir.is_dir() {
        return Err(MediaError::Pipeline(format!(
            "thumbnail path {} is not a directory",
            dir.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) =
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(THUMBNAIL_DIR_MODE))
        {
            warn!(path = %dir.display(), error = %e, "could not restrict thumbnail directory to owner-only");
        }
    }
    Ok(())
}

/// Atomically write `jpeg` to `path`: a sibling temp file is created
/// fresh (`create_new`, owner-only), filled, then renamed over `path`,
/// so the overlay never observes a half-written image and a file planted
/// at the temp path by someone else is never written through.
async fn write_thumbnail_atomic(path: &Path, jpeg: &Bytes) -> Result<(), MediaError> {
    use tokio::io::AsyncWriteExt;

    let tmp = path.with_extension("jpg.tmp");
    match tokio::fs::remove_file(&tmp).await {
        Ok(()) => debug!(path = %tmp.display(), "stale thumbnail temp file removed"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(MediaError::Pipeline(format!(
                "stale thumbnail temp file cannot be removed: {e}"
            )));
        }
    }
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(THUMBNAIL_FILE_MODE);
    let mut file = options
        .open(&tmp)
        .await
        .map_err(|e| MediaError::Pipeline(format!("thumbnail temp file create failed: {e}")))?;
    file.write_all(jpeg)
        .await
        .map_err(|e| MediaError::Pipeline(format!("thumbnail write failed: {e}")))?;
    file.flush()
        .await
        .map_err(|e| MediaError::Pipeline(format!("thumbnail flush failed: {e}")))?;
    drop(file);
    tokio::fs::rename(&tmp, path)
        .await
        .map_err(|e| MediaError::Pipeline(format!("thumbnail rename failed: {e}")))
}

/// Point a `gdkpixbufoverlay` at `path` and scale it to fill the idle
/// frame. Setting `location` makes the element reload the image
/// (unconditionally, even for the same path), so this both installs and
/// refreshes the on-air thumbnail. `overlay-width`/`overlay-height`
/// **must** be (re)set here rather than at pipeline construction:
/// gdkpixbufoverlay renders a runtime-loaded image at its native size
/// unless the target size is set together with the load, which would
/// otherwise leave a large snapshot cropped to the top-left corner.
fn apply_thumbnail_overlay(overlay: &gst::Element, path: &Path) {
    overlay.set_property("location", path.to_string_lossy().as_ref());
    overlay.set_property("overlay-width", i32::try_from(SYNTHETIC_WIDTH).unwrap_or(0));
    overlay.set_property(
        "overlay-height",
        i32::try_from(SYNTHETIC_HEIGHT).unwrap_or(0),
    );
}

/// Load the idle still into a media being configured on a thread of its
/// own: the `media-configure` hook runs on the RTSP server's thread, and
/// a decode there (tens of ms for a 4K snapshot, more on a Pi) holds up
/// the clients waiting on it. A missing file leaves the synthetic frame.
fn load_thumbnail_detached(overlay: gst::Element, path: PathBuf) {
    let spawned = std::thread::Builder::new()
        .name("thumbnail-load".to_string())
        .spawn(move || {
            if path.exists() {
                apply_thumbnail_overlay(&overlay, &path);
            }
        });
    if let Err(e) = spawned {
        warn!(error = %e, "thumbnail load thread not started; idle frame stays synthetic");
    }
}

/// Remove a still left by an earlier run that today's [`check_thumbnail`]
/// refuses (it may predate the check): every media build would decode it
/// until a refresh replaced it, and a refused refresh never does.
async fn discard_unsafe_thumbnail(path: &Path) {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "stored thumbnail unreadable");
            return;
        }
    };
    let Err(reason) = check_thumbnail(&bytes) else {
        return;
    };
    warn!(path = %path.display(), %reason, "stored thumbnail refused; removing it");
    if let Err(e) = tokio::fs::remove_file(path).await {
        warn!(path = %path.display(), error = %e, "refused thumbnail not removed");
    }
}

/// Arm the idle → live switch on the media `wiring` belongs to.
fn arm_live_switch(wiring: &LiveWiring) {
    arm_switch_probe(
        &wiring.sink_live,
        &wiring.selector,
        &wiring.encoder,
        &wiring.live_switch,
    );
}

/// Install a single-shot pad probe on the live branch's selector sink
/// pad; on the **first raw frame** it sets `active-pad` to that pad,
/// dispatches a force-key-unit to the downstream encoder (so its next
/// encoded frame is a fresh IDR after the content swap), and
/// self-removes.
///
/// The probe holds only weak references: it lives on a pad of the
/// selector it flips, and a strong one kept the selector, its pads and
/// the encoder alive after the media was torn down. Its id waits in
/// `pending` so a detach before the first live frame removes it
/// ([`disarm_switch_probe`]); a probe re-armed replaces the old one.
fn arm_switch_probe(
    sink_live: &gst::Pad,
    selector: &gst::Element,
    encoder: &gst::Element,
    pending: &PendingSwitch,
) {
    let selector = selector.downgrade();
    let encoder = encoder.downgrade();
    let claim = Arc::clone(pending);
    // Held while the probe is added, so a frame arriving at once waits
    // for the id instead of finding the slot empty and skipping the flip.
    let mut slot = lock(pending);
    if let Some(old) = slot.take() {
        sink_live.remove_probe(old);
    }
    *slot = sink_live.add_probe(gst::PadProbeType::BUFFER, move |pad, _info| {
        // Taking the id claims the switch; an empty slot means a detach
        // took it first and the camera is no longer live.
        if lock(&claim).take().is_none() {
            return gst::PadProbeReturn::Remove;
        }
        if let Some(selector) = selector.upgrade() {
            selector.set_property("active-pad", pad);
        }
        if let Some(encoder) = encoder.upgrade() {
            force_keyframe(&encoder);
        }
        debug!("input-selector flipped to sink_1 (live); IDR forced on encoder");
        gst::PadProbeReturn::Remove
    });
}

/// Remove a live-switch probe that has not fired yet. Returns whether
/// there was one.
fn disarm_switch_probe(sink_live: &gst::Pad, pending: &PendingSwitch) -> bool {
    let Some(id) = lock(pending).take() else {
        return false;
    };
    sink_live.remove_probe(id);
    true
}

/// Dispatch a downstream `GstForceKeyUnit` event to the encoder so
/// the next encoded frame is an IDR.
fn force_keyframe(encoder: &gst::Element) {
    let event = gst::event::CustomDownstream::new(
        gst::Structure::builder("GstForceKeyUnit")
            .field("all-headers", true)
            .build(),
    );
    let sent = encoder.send_event(event);
    debug!(sent, "force-key-unit dispatched to encoder");
}

/// Drain an RTP byte channel into the live `appsrc` of whichever media
/// currently serves the camera (`pick` selects video or audio). Each
/// `Bytes` becomes one `gst::Buffer`; the appsrc's `do-timestamp=true`
/// re-stamps with the pipeline clock so the inputs into the selector
/// share a timebase.
///
/// While no media exists the bytes are dropped, and a failed push (the
/// media is being torn down) is logged and dropped too: the pump keeps
/// draining so the WebRTC leg never back-pressures, and resumes feeding
/// the next media. Exits when the sink is dropped or the task aborted.
fn spawn_live_pump(
    mut rx: mpsc::Receiver<Bytes>,
    wiring: WiringSlot,
    pick: fn(&LiveWiring) -> Option<gst_app::AppSrc>,
    label: &'static str,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut failing = false;
        while let Some(bytes) = rx.recv().await {
            let target = lock(&wiring).as_ref().and_then(pick);
            let Some(appsrc) = target else {
                continue;
            };
            match appsrc.push_buffer(gst::Buffer::from_slice(bytes)) {
                Ok(_) => failing = false,
                Err(e) if !failing => {
                    debug!(error = %e, kind = label, "live push refused (media going down); dropping until the next media");
                    failing = true;
                }
                Err(_) => {}
            }
        }
        debug!(kind = label, "live RTP pump exited");
    })
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "same signature as audio_appsrc: both are passed as the `pick` of spawn_live_pump"
)]
fn video_appsrc(w: &LiveWiring) -> Option<gst_app::AppSrc> {
    Some(w.appsrc.clone())
}

fn audio_appsrc(w: &LiveWiring) -> Option<gst_app::AppSrc> {
    w.audio_appsrc.clone()
}

/// The AAC pump: like [`spawn_live_pump`] for the relayed audio, plus
/// the caps. The relay announces the stream's format once
/// ([`AacFeed::Format`]); the pump applies it to the AAC `appsrc` before
/// the first packet, and again whenever the media — hence the `appsrc`
/// — changes under a running relay.
fn spawn_aac_pump(
    mut rx: mpsc::Receiver<AacFeed>,
    wiring: WiringSlot,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut format: Option<AacRtpFormat> = None;
        let mut configured: Option<gst_app::AppSrc> = None;
        let mut failing = false;
        while let Some(item) = rx.recv().await {
            let bytes = match item {
                AacFeed::Format(f) => {
                    format = Some(f);
                    configured = None;
                    continue;
                }
                AacFeed::Rtp(bytes) => bytes,
            };
            let target = lock(&wiring).as_ref().and_then(|w| w.aac_appsrc.clone());
            let Some(appsrc) = target else {
                continue;
            };
            if configured.as_ref() != Some(&appsrc) {
                if let Some(f) = &format {
                    apply_aac_caps(&appsrc, f);
                }
                configured = Some(appsrc.clone());
            }
            match appsrc.push_buffer(gst::Buffer::from_slice(bytes)) {
                Ok(_) => failing = false,
                Err(e) if !failing => {
                    debug!(error = %e, kind = "aac", "live push refused (media going down); dropping until the next media");
                    failing = true;
                }
                Err(_) => {}
            }
        }
        debug!(kind = "aac", "live RTP pump exited");
    })
}

/// Set the AAC `appsrc`'s caps from the relayed stream's format.
/// Built with the typed builder, not parsed from text: the SDP-derived
/// values are field *values* and can never become field separators. The
/// SDP-derived fields are strings in GStreamer's RTP caps
/// (`rtpmp4gdepay` reads them with `gst_structure_get_string`).
fn apply_aac_caps(appsrc: &gst_app::AppSrc, format: &AacRtpFormat) {
    let caps = gst::Caps::builder("application/x-rtp")
        .field("media", "audio")
        .field("encoding-name", "MPEG4-GENERIC")
        .field(
            "clock-rate",
            i32::try_from(format.clock_rate).unwrap_or(i32::MAX),
        )
        .field("encoding-params", format.channels.to_string())
        .field("payload", LIVE_RTP_AAC_PT)
        .field("mode", "AAC-hbr")
        .field("config", format.config.as_str())
        .field("sizelength", format.size_length.to_string())
        .field("indexlength", format.index_length.to_string())
        .field("indexdeltalength", format.index_delta_length.to_string())
        .build();
    appsrc.set_caps(Some(&caps));
    debug!(
        clock_rate = format.clock_rate,
        channels = format.channels,
        "AAC appsrc caps set from the relayed stream"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Records the mounts removed through it. The real server needs a
    /// `GLib` main loop, which unit tests must not share across threads.
    #[derive(Clone, Default)]
    struct RemovalSpy(Arc<StdMutex<Vec<String>>>);

    impl RemoveMount for RemovalSpy {
        fn remove_mount(&self, mount_path: &str) {
            lock(&self.0).push(mount_path.to_string());
        }
    }

    /// A registration that fails after the mount is installed must take
    /// the mount down again; a successful one keeps it.
    #[test]
    fn mount_guard_removes_the_mount_unless_disarmed() {
        let spy = RemovalSpy::default();
        {
            let _armed = MountGuard::new(spy.clone(), "/guarded".to_string());
        }
        assert_eq!(*lock(&spy.0), vec!["/guarded".to_string()]);

        {
            let mut guard = MountGuard::new(spy.clone(), "/kept".to_string());
            guard.disarm();
        }
        assert_eq!(
            *lock(&spy.0),
            vec!["/guarded".to_string()],
            "a disarmed guard leaves the mount"
        );
    }

    /// An `input-selector` (not in a pipeline, streams not synchronised)
    /// with an idle pad and a live pad, plus a stand-in encoder.
    fn switch_fixture() -> Option<(gst::Element, gst::Pad, gst::Pad, gst::Element)> {
        gst::init().ok();
        let selector = gst::ElementFactory::make("input-selector")
            .property("sync-streams", false)
            .build()
            .ok()?;
        let encoder = gst::ElementFactory::make("identity").build().ok()?;
        let sink_idle = selector.request_pad_simple("sink_%u")?;
        let sink_live = selector.request_pad_simple("sink_%u")?;
        selector.set_property("active-pad", &sink_idle);
        Some((selector, sink_idle, sink_live, encoder))
    }

    fn active_pad(selector: &gst::Element) -> Option<gst::Pad> {
        selector.property::<Option<gst::Pad>>("active-pad")
    }

    /// The probe lives on a pad of the selector it flips: strong refs in
    /// it kept the selector, its pads and the encoder alive for good.
    #[test]
    fn live_switch_probe_keeps_no_reference_to_the_selector_or_encoder() {
        let Some((selector, sink_idle, sink_live, encoder)) = switch_fixture() else {
            eprintln!("input-selector not available here; skipping");
            return;
        };
        let pending = PendingSwitch::default();
        arm_switch_probe(&sink_live, &selector, &encoder, &pending);
        let (selector_w, encoder_w) = (selector.downgrade(), encoder.downgrade());

        drop((selector, sink_idle, sink_live, encoder));

        assert!(selector_w.upgrade().is_none(), "the selector must be freed");
        assert!(encoder_w.upgrade().is_none(), "the encoder must be freed");
    }

    #[test]
    fn live_switch_flips_on_the_first_live_frame() {
        let Some((selector, _sink_idle, sink_live, encoder)) = switch_fixture() else {
            eprintln!("input-selector not available here; skipping");
            return;
        };
        selector.set_state(gst::State::Paused).unwrap();
        let pending = PendingSwitch::default();
        arm_switch_probe(&sink_live, &selector, &encoder, &pending);

        let _ = sink_live.chain(gst::Buffer::new());

        assert_eq!(active_pad(&selector).as_ref(), Some(&sink_live));
        assert!(lock(&pending).is_none(), "the fired probe released its id");
        selector.set_state(gst::State::Null).unwrap();
    }

    /// An attach that failed before its first live frame left the probe
    /// on the pad, where the next session's first frame fired it.
    #[test]
    fn disarmed_live_switch_never_flips() {
        let Some((selector, sink_idle, sink_live, encoder)) = switch_fixture() else {
            eprintln!("input-selector not available here; skipping");
            return;
        };
        selector.set_state(gst::State::Paused).unwrap();
        let pending = PendingSwitch::default();
        arm_switch_probe(&sink_live, &selector, &encoder, &pending);

        assert!(disarm_switch_probe(&sink_live, &pending));
        assert!(!disarm_switch_probe(&sink_live, &pending), "only once");
        assert_eq!(
            Arc::strong_count(&pending),
            1,
            "the probe and its closure are removed from the pad"
        );
        let _ = sink_live.chain(gst::Buffer::new());

        assert_eq!(active_pad(&selector).as_ref(), Some(&sink_idle));
        selector.set_state(gst::State::Null).unwrap();
    }

    /// SOI and a baseline frame header declaring `width`×`height`.
    fn jpeg_header(width: u16, height: u16) -> Vec<u8> {
        let mut v = vec![0xFF, 0xD8, 0xFF, 0xC0, 0x00, 0x11, 0x08];
        v.extend(height.to_be_bytes());
        v.extend(width.to_be_bytes());
        v
    }

    /// A still stored before the size check existed would be decoded at
    /// every media build; registration removes it and keeps a sound one.
    #[tokio::test]
    async fn stored_thumbnail_declaring_a_huge_frame_is_removed() {
        let dir = std::env::temp_dir().join(format!("thumb-discard-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let huge = dir.join("huge.jpg");
        let sound = dir.join("sound.jpg");
        tokio::fs::write(&huge, jpeg_header(12000, 12000))
            .await
            .unwrap();
        tokio::fs::write(&sound, jpeg_header(1920, 1080))
            .await
            .unwrap();

        discard_unsafe_thumbnail(&huge).await;
        discard_unsafe_thumbnail(&sound).await;
        discard_unsafe_thumbnail(&dir.join("missing.jpg")).await;

        assert!(!huge.exists(), "the refused still is removed");
        assert!(sound.exists(), "a sound still is kept");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
