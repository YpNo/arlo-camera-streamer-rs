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
//! Audio is silent AAC at all times (Opus bridging deferred to
//! Phase 8b — see the `pipeline_desc` module-level note).
//!
//! Lifecycle (per camera):
//!
//! 1. [`Self::register`] installs the factory at `/<stream_name>` with
//!    a `media-configure` hook. `suspend-mode=None` is set so the
//!    pipeline stays running across client disconnects.
//! 2. On first client connect, gst-rtsp-server constructs the media
//!    and our hook captures handles to `appsrc name=live_rtp_src`,
//!    `input-selector name=sel` (sink pads), and the downstream
//!    encoder `x264enc name=video_enc`.
//! 3. [`Self::attach_live_sink`] hands back a [`LiveRtpSink`] — its
//!    receiver is drained by a tokio pump that calls
//!    `appsrc.push_buffer(...)`. A pad probe on `sel.sink_1` waits
//!    for the first decoded raw buffer, flips `active-pad = sink_1`,
//!    and force-key-units the encoder. The probe self-removes.
//! 4. [`Self::detach_live_sink`] flips `active-pad = sink_0`
//!    synchronously, force-key-units the encoder, and aborts the
//!    live pump.
//!
//! Edge case: if `attach_live_sink` is called before *any* client has
//! connected, the media is not yet constructed and no `appsrc` exists.
//! We spawn a discard pump in that case and log a warning; bytes are
//! dropped until a client connects. In the user's documented workflow
//! (an idle viewer is connected when motion fires) the wiring is
//! always present.
//!
//! ## Thumbnail handling (Phase 5)
//!
//! [`Self::refresh_thumbnail`] persists the JPEG to a stable per-camera
//! file and points the synthetic idle branch's `gdkpixbufoverlay`
//! (`idle_overlay`) at it, so the STANDBY screen shows the camera's
//! last snapshot instead of a black frame. The overlay is an inline
//! filter: before the first snapshot it's a transparent pass-through,
//! so idle preroll is unchanged. A rebuilt media re-applies the file at
//! `media-configure`. The cached bytes are also kept in per-camera
//! state for a future `/admin/thumbnail/<cam>` endpoint.
//!
//! This file is excluded from coverage in CI — it requires a running
//! GStreamer environment with `gst-rtsp-server` plugins, which is
//! integration-test territory.

#![allow(clippy::module_name_repetitions)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};

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

use crate::error::MediaError;
use crate::idle_source::{IdleKind, SYNTHETIC_HEIGHT, SYNTHETIC_WIDTH};
use crate::live_rtp_sink::{LiveSinkReceivers, LiveSinks};
use crate::multiplexer::PipelineRegistry;
use crate::pipeline_desc::{
    IDLE_OVERLAY_NAME, OutputBranches, UNIFIED_ENCODER_NAME, combined_launch_string,
};
use crate::rtsp::RtspServer;

/// Live wiring captured from the gst-rtsp-server media on
/// `media-configure`. Cloning the handles is cheap (`GObject` reference
/// counting) and lets us share them between the `GLib` thread (where
/// they're produced) and tokio tasks (where they're consumed).
#[derive(Clone)]
struct LiveWiring {
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
    /// The live audio `appsrc` (`live_audio_rtp_src`, Phase 8b) — Opus
    /// RTP pushed here is decoded and mixed onto the silent bed by the
    /// `audiomixer`. No selector flip needed: the mixer reverts to
    /// silence when live audio stops.
    audio_appsrc: Option<gst_app::AppSrc>,
}

/// Active live ingestion bookkeeping.
struct LiveSession {
    /// Drains the video sink's receiver into the video `appsrc`.
    video_pump: tokio::task::JoinHandle<()>,
    /// Drains the audio sink's receiver into the audio `appsrc`.
    audio_pump: tokio::task::JoinHandle<()>,
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
    wiring: Arc<StdMutex<Option<LiveWiring>>>,
    /// Active live ingestion if any.
    session: Option<LiveSession>,
}

/// GStreamer-backed [`PipelineRegistry`].
pub struct GstPipelineRegistry {
    server: Arc<RtspServer>,
    state: RwLock<HashMap<CameraId, CameraEntry>>,
}

impl GstPipelineRegistry {
    /// Construct from a started RTSP server.
    #[must_use]
    pub fn new(server: Arc<RtspServer>) -> Self {
        Self {
            server,
            state: RwLock::new(HashMap::new()),
        }
    }

    fn refresh_overlay_timestamp(idle: &IdleKind) -> IdleKind {
        match idle {
            IdleKind::Synthetic { stream_name, .. } => {
                let ts = Local::now().format("%Y-%m-%dT%H:%M:%S").to_string();
                IdleKind::Synthetic {
                    stream_name: stream_name.clone(),
                    overlay: format!("STANDBY · {stream_name} · {ts}"),
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
                session.video_pump.abort();
                session.audio_pump.abort();
            }
            debug!(camera = %cam, mount = %entry.mount_path, "mount removed during shutdown");
        }
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
        {
            let guard = self.state.read().await;
            if guard.contains_key(camera) {
                return Err(MediaError::AlreadyRegistered(camera.to_string()));
            }
        }
        // Refresh the standby clock so the very first frame after
        // register reflects the current time.
        let idle = Self::refresh_overlay_timestamp(&idle);
        let launch = combined_launch_string(&idle);
        debug!(camera = %camera, launch_string = %launch, "combined launch string");

        let wiring: Arc<StdMutex<Option<LiveWiring>>> = Arc::new(StdMutex::new(None));
        let wiring_for_cb = wiring.clone();
        let cam_for_cb = camera.clone();
        self.server.install_factory_with_media_hook(
            &outputs.rtsp_mount_path,
            &launch,
            move |media: &RTSPMedia| {
                match build_live_wiring(media) {
                    Ok(w) => {
                        // Re-apply the last snapshot to a freshly-built
                        // media so a rebuild between live sessions keeps
                        // showing the thumbnail rather than reverting to
                        // the black STANDBY frame.
                        if let Some(overlay) = &w.idle_overlay {
                            let path = thumbnail_file_path(&cam_for_cb);
                            if path.exists() {
                                apply_thumbnail_overlay(overlay, &path);
                            }
                        }
                        *wiring_for_cb.lock().expect("wiring poisoned") = Some(w);
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
                attach_media_bus_watch(media, &cam_for_cb);
            },
        )?;
        info!(mount = %outputs.rtsp_mount_path, idle = idle.kind_label(), "camera registered");

        if outputs.hls.is_some() || outputs.dash.is_some() {
            // Phase 5 hookup — HLS/DASH sinks need a separate top-level
            // pipeline (not the RTSP factory) because gst-rtsp-server
            // owns its own pipeline graph.
            warn!("HLS/DASH outputs configured but not yet wired (Phase 5)");
        }

        self.state.write().await.insert(
            camera.clone(),
            CameraEntry {
                mount_path: outputs.rtsp_mount_path,
                idle,
                last_thumbnail: None,
                wiring,
                session: None,
            },
        );
        Ok(())
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
        } = rxs;
        let wiring_snapshot = entry.wiring.lock().expect("wiring poisoned").clone();

        let session = if let Some(wiring) = wiring_snapshot {
            arm_live_switch(&wiring);
            let video_pump = spawn_pump_to_appsrc(video_rx, wiring.appsrc, "video");
            // Audio has no idle↔live selector — the audiomixer blends
            // the live Opus onto silence automatically. If the media
            // predates Phase 8b (no audio appsrc), discard audio bytes.
            let audio_pump = match wiring.audio_appsrc {
                Some(a) => spawn_pump_to_appsrc(audio_rx, a, "audio"),
                None => spawn_discard_pump(audio_rx),
            };
            LiveSession {
                video_pump,
                audio_pump,
            }
        } else {
            warn!(
                camera = %camera,
                "attach_live_sink before any RTSP client connected — \
                 live RTP will be discarded until a client connects \
                 (deferred wiring not implemented)"
            );
            LiveSession {
                video_pump: spawn_discard_pump(video_rx),
                audio_pump: spawn_discard_pump(audio_rx),
            }
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

        let wiring_snapshot = entry.wiring.lock().expect("wiring poisoned").clone();
        if let Some(wiring) = wiring_snapshot {
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
        session.video_pump.abort();
        session.audio_pump.abort();
        info!(mount = %entry.mount_path, "live ingestion released; idle restored");
        Ok(())
    }

    #[instrument(skip(self, jpeg), fields(camera = %camera, bytes = jpeg.len()))]
    async fn refresh_thumbnail(&self, camera: &CameraId, jpeg: Bytes) -> Result<(), MediaError> {
        // Persist the JPEG to a stable per-camera path, then point the
        // idle branch's `gdkpixbufoverlay` at it so the STANDBY screen
        // shows the last snapshot (Phase 5). The write is atomic
        // (temp + rename) so the overlay never reads a partial file.
        let path = thumbnail_file_path(camera);
        write_thumbnail_atomic(&path, &jpeg).await?;

        let mut guard = self.state.write().await;
        let entry = guard
            .get_mut(camera)
            .ok_or_else(|| MediaError::UnknownCamera(camera.to_string()))?;
        entry.last_thumbnail = Some(jpeg);

        // Apply to the running media if one is up. If no client has
        // connected yet the file is already in place and the overlay
        // is re-applied at the next `media-configure` (see `register`).
        let wiring = entry.wiring.lock().expect("wiring poisoned").clone();
        if let Some(overlay) = wiring.and_then(|w| w.idle_overlay) {
            apply_thumbnail_overlay(&overlay, &path);
            debug!(path = %path.display(), "thumbnail applied to idle overlay");
        } else {
            debug!(
                path = %path.display(),
                "thumbnail stored; no live overlay yet (applied on next media build)"
            );
        }
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
fn attach_media_bus_watch(media: &RTSPMedia, camera: &CameraId) {
    let cam_prep = camera.clone();
    media.connect_prepared(move |_media| {
        info!(camera = %cam_prep, "RTSPMedia prepared (pipeline reached PLAYING)");
    });
    let cam_unprep = camera.clone();
    media.connect_unprepared(move |_media| {
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
    let element = media.element();
    let bin: gst::Bin = element
        .dynamic_cast::<gst::Bin>()
        .map_err(|_| MediaError::Pipeline("media element is not a Bin".into()))?;

    let appsrc_el = bin
        .by_name("live_rtp_src")
        .ok_or_else(|| MediaError::Pipeline("appsrc 'live_rtp_src' not found".into()))?;
    let appsrc: gst_app::AppSrc = appsrc_el
        .dynamic_cast::<gst_app::AppSrc>()
        .map_err(|_| MediaError::Pipeline("'live_rtp_src' is not an AppSrc".into()))?;
    // Non-blocking push: full queue drops rather than back-pressuring
    // the pump task (and through it, the streaming thread).
    appsrc.set_property("block", false);

    let selector = bin
        .by_name("sel")
        .ok_or_else(|| MediaError::Pipeline("input-selector 'sel' not found".into()))?;
    let sink_idle = selector
        .static_pad("sink_0")
        .ok_or_else(|| MediaError::Pipeline("input-selector has no sink_0".into()))?;
    let sink_live = selector
        .static_pad("sink_1")
        .ok_or_else(|| MediaError::Pipeline("input-selector has no sink_1".into()))?;
    let encoder = bin
        .by_name(UNIFIED_ENCODER_NAME)
        .ok_or_else(|| MediaError::Pipeline(format!("encoder '{UNIFIED_ENCODER_NAME}' not found")))?;
    // Optional: only the synthetic idle branch carries the overlay.
    let idle_overlay = bin.by_name(IDLE_OVERLAY_NAME);

    // Optional: live audio appsrc (Phase 8b). Non-blocking push like
    // the video appsrc so a full queue drops instead of stalling.
    let audio_appsrc = bin
        .by_name("live_audio_rtp_src")
        .and_then(|el| el.dynamic_cast::<gst_app::AppSrc>().ok());
    if let Some(a) = &audio_appsrc {
        a.set_property("block", false);
    }

    Ok(LiveWiring {
        appsrc,
        selector,
        sink_idle,
        sink_live,
        encoder,
        idle_overlay,
        audio_appsrc,
    })
}

/// Stable per-camera path where the latest snapshot JPEG is persisted
/// for the idle `gdkpixbufoverlay` to load. Lives under the system temp
/// dir; the camera id is filename-safe (Arlo device ids are `[A-Z0-9]`).
fn thumbnail_file_path(camera: &CameraId) -> PathBuf {
    std::env::temp_dir().join(format!("arlo-streamer-thumb-{camera}.jpg"))
}

/// Atomically write `jpeg` to `path` (write a sibling temp file, then
/// rename) so the overlay never observes a half-written image.
async fn write_thumbnail_atomic(path: &Path, jpeg: &Bytes) -> Result<(), MediaError> {
    let tmp = path.with_extension("jpg.tmp");
    tokio::fs::write(&tmp, jpeg)
        .await
        .map_err(|e| MediaError::Pipeline(format!("thumbnail write failed: {e}")))?;
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
    overlay.set_property("overlay-height", i32::try_from(SYNTHETIC_HEIGHT).unwrap_or(0));
}

/// Install a single-shot pad probe on the live branch's selector
/// sink pad; on the **first raw frame** it sets `active-pad = sink_1`,
/// dispatches a force-key-unit to the downstream encoder (so its next
/// encoded frame is a fresh IDR after the content swap), and
/// self-removes.
fn arm_live_switch(wiring: &LiveWiring) {
    let selector = wiring.selector.clone();
    let sink_live = wiring.sink_live.clone();
    let encoder = wiring.encoder.clone();
    let fired = Arc::new(StdMutex::new(false));
    wiring
        .sink_live
        .add_probe(gst::PadProbeType::BUFFER, move |_pad, _info| {
            let mut already = fired.lock().expect("flag poisoned");
            if *already {
                return gst::PadProbeReturn::Ok;
            }
            *already = true;
            selector.set_property("active-pad", &sink_live);
            force_keyframe(&encoder);
            debug!("input-selector flipped to sink_1 (live); IDR forced on encoder");
            gst::PadProbeReturn::Remove
        });
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

/// Drain an RTP byte channel into an `AppSrc`. Each `Bytes` becomes
/// one `gst::Buffer`; the appsrc's `do-timestamp=true` re-stamps with
/// the pipeline clock so the inputs into the selector share a
/// timebase. Exits when the sink is dropped (channel closes) or when
/// the task is aborted.
fn spawn_pump_to_appsrc(
    mut rx: mpsc::Receiver<Bytes>,
    appsrc: gst_app::AppSrc,
    label: &'static str,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(bytes) = rx.recv().await {
            let buf = gst::Buffer::from_slice(bytes);
            if let Err(e) = appsrc.push_buffer(buf) {
                debug!(error = %e, kind = label, "appsrc.push_buffer failed; live pump stopping");
                break;
            }
        }
        debug!(kind = label, "live RTP pump exited");
    })
}

/// Discard pump used when no media has been constructed yet (no
/// client connected). Drains the channel so `LiveRtpSink::push` never
/// back-pressures the WebRTC ingestion thread.
fn spawn_discard_pump(mut rx: mpsc::Receiver<Bytes>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while rx.recv().await.is_some() {
            // intentional drop
        }
        debug!("discard pump exited");
    })
}
