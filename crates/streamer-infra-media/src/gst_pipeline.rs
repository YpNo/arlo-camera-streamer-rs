//! Production [`PipelineRegistry`] backed by GStreamer + the embedded
//! RTSP server.
//!
//! ## Splice strategy
//!
//! Phase 4 uses **factory-restart on transition** (see
//! [`crate::pipeline_desc`] doc): the per-camera RTSP factory is bound
//! to either the idle launch string or the live launch string, and
//! transitions swap the binding. Existing RTSP clients see a brief EOS
//! and reconnect within ~1 s; Frigate handles that transparently.
//!
//! Seamless `input-selector` splicing remains a Phase 6 polish target.
//! [`crate::splice::KeyframeWatcher`] is in place for it.
//!
//! ## Thumbnail handling
//!
//! `refresh_thumbnail` currently stores the JPEG bytes in per-camera
//! state for future use (e.g., a `/admin/thumbnail/<cam>` endpoint)
//! but does **not** push them into the running pipeline.
//! Wiring `appsrc` for JPEG-still idle output is Phase 5 work; the
//! synthetic overlay remains the on-air idle source.
//!
//! This file is excluded from coverage in CI — it requires a running
//! GStreamer environment with `gst-rtsp-server` plugins, which is
//! integration-test territory.

#![allow(clippy::module_name_repetitions)]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Local;
use tokio::sync::RwLock;
use tracing::{debug, info, instrument, warn};

use streamer_domain::camera::CameraId;
use streamer_domain::stream::StreamSource;

use crate::error::MediaError;
use crate::idle_source::IdleKind;
use crate::multiplexer::PipelineRegistry;
use crate::pipeline_desc::{OutputBranches, idle_launch_string, live_launch_string};
use crate::rtsp::RtspServer;

/// Per-camera state owned by [`GstPipelineRegistry`].
#[derive(Debug)]
struct CameraEntry {
    mount_path: String,
    idle: IdleKind,
    /// Last known thumbnail JPEG. Stored for ops endpoints; not yet
    /// wired into the live pipeline (Phase 5).
    last_thumbnail: Option<Bytes>,
    /// Whether the factory is currently bound to a live source.
    is_live: bool,
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
        let launch = idle_launch_string(&idle);
        self.server
            .install_factory(&outputs.rtsp_mount_path, &launch)?;
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
                is_live: false,
            },
        );
        Ok(())
    }

    #[instrument(skip(self, source), fields(camera = %camera, url = %source.url))]
    async fn set_live_source(
        &self,
        camera: &CameraId,
        source: StreamSource,
    ) -> Result<(), MediaError> {
        let mut guard = self.state.write().await;
        let entry = guard
            .get_mut(camera)
            .ok_or_else(|| MediaError::UnknownCamera(camera.to_string()))?;
        let launch = live_launch_string(&source.url, source.codec_hint);
        self.server.install_factory(&entry.mount_path, &launch)?;
        entry.is_live = true;
        info!(mount = %entry.mount_path, "switched to live source");
        Ok(())
    }

    #[instrument(skip(self), fields(camera = %camera))]
    async fn clear_live_source(&self, camera: &CameraId) -> Result<(), MediaError> {
        let mut guard = self.state.write().await;
        let entry = guard
            .get_mut(camera)
            .ok_or_else(|| MediaError::UnknownCamera(camera.to_string()))?;
        // Refresh overlay timestamp so the user sees a fresh standby clock.
        let refreshed = Self::refresh_overlay_timestamp(&entry.idle);
        let launch = idle_launch_string(&refreshed);
        self.server.install_factory(&entry.mount_path, &launch)?;
        entry.idle = refreshed;
        entry.is_live = false;
        info!(mount = %entry.mount_path, "reverted to idle source");
        Ok(())
    }

    #[instrument(skip(self, jpeg), fields(camera = %camera, bytes = jpeg.len()))]
    async fn refresh_thumbnail(&self, camera: &CameraId, jpeg: Bytes) -> Result<(), MediaError> {
        let mut guard = self.state.write().await;
        let entry = guard
            .get_mut(camera)
            .ok_or_else(|| MediaError::UnknownCamera(camera.to_string()))?;
        entry.last_thumbnail = Some(jpeg);
        // Phase 5: when JPEG-still idle is wired via appsrc, push the
        // bytes into the running idle source here.
        debug!("thumbnail stored (Phase 5 will push into appsrc)");
        Ok(())
    }
}

impl GstPipelineRegistry {
    /// Drop all mount points (called during graceful shutdown).
    pub async fn shutdown(&self) {
        let mut guard = self.state.write().await;
        for (cam, entry) in guard.drain() {
            self.server.remove_mount(&entry.mount_path);
            debug!(camera = %cam, mount = %entry.mount_path, "mount removed during shutdown");
        }
    }
}
