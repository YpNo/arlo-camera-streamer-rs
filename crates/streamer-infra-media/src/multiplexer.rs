//! [`MediaMultiplexer`] adapter generic over a [`PipelineRegistry`].
//!
//! This module owns the *orchestration* layer (idempotency,
//! camera→stream mapping, idle-source selection, error mapping) but
//! defers the actual pipeline + RTSP-server work to a
//! [`PipelineRegistry`] implementation. The production registry lives
//! in `gst_pipeline.rs`; tests use an in-memory `FakeRegistry`.
//!
//! ## Idempotency contract
//!
//! - `register(camera)` for an already-registered camera returns
//!   `Ok(())` and logs at debug — matches the port doc.
//! - `attach_live(unknown)` returns [`DomainError::UnknownCamera`].
//! - `detach_live(unknown)` returns [`DomainError::UnknownCamera`].
//! - `refresh_thumbnail(unknown)` returns [`DomainError::UnknownCamera`].

#![allow(clippy::similar_names)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Local;
use tokio::sync::RwLock;
use tracing::{debug, info, instrument, warn};

use streamer_domain::camera::{CameraId, StreamName};
use streamer_domain::config::{CameraConfig, OutputConfig};
use streamer_domain::error::DomainError;
use streamer_domain::port::MediaMultiplexer;
use streamer_domain::stream::{Codec, StreamSource};

use crate::codec_cache::CodecCache;
use crate::error::MediaError;
use crate::idle_source::{IdleKind, select_idle_source};
use crate::pipeline_desc::{OutputBranches, build_output_branches};

/// Trait that the GStreamer-backed registry implements. The seam keeps
/// the multiplexer testable without spinning up a real pipeline.
///
/// Implementations are responsible for:
/// - Building the per-camera pipeline + RTSP mount in `register`.
/// - Performing the idle ↔ live transition atomically in
///   `set_live_source` / `clear_live_source`.
/// - Pushing fresh JPEGs to the idle `appsrc` in `refresh_thumbnail`.
#[async_trait]
pub trait PipelineRegistry: Send + Sync {
    /// Bring up an idle pipeline for `camera`. Must be safe to call
    /// once per camera; calling twice for the same camera surfaces
    /// [`MediaError::AlreadyRegistered`] (the multiplexer translates
    /// that into `Ok(())` for the port-level contract).
    async fn register(
        &self,
        camera: &CameraId,
        idle: IdleKind,
        outputs: OutputBranches,
    ) -> Result<(), MediaError>;

    /// Switch the camera to live mode. The implementation owns the
    /// IDR-aligned splice (see [`crate::splice::KeyframeWatcher`]).
    async fn set_live_source(
        &self,
        camera: &CameraId,
        source: StreamSource,
    ) -> Result<(), MediaError>;

    /// Revert the camera to idle mode.
    async fn clear_live_source(&self, camera: &CameraId) -> Result<(), MediaError>;

    /// Replace the idle still frame for the camera (best-effort).
    async fn refresh_thumbnail(&self, camera: &CameraId, jpeg: Bytes) -> Result<(), MediaError>;
}

/// `MediaMultiplexer` implementation generic over a [`PipelineRegistry`].
pub struct GstMediaMultiplexer<R: PipelineRegistry> {
    registry: Arc<R>,
    output: OutputConfig,
    /// Camera → stream-name mapping captured at boot. Read-only after
    /// construction.
    cameras: HashMap<CameraId, StreamName>,
    /// Lazily-populated set of cameras whose pipelines have been built.
    registered: RwLock<HashSet<CameraId>>,
    /// Codec hint cache (seeded from config, updated by the registry
    /// after first parsebin emission).
    codec_cache: Arc<CodecCache>,
}

impl<R: PipelineRegistry> GstMediaMultiplexer<R> {
    /// Construct a multiplexer.
    ///
    /// Builds the camera→stream-name index and seeds the codec cache
    /// from the per-camera `codec_hint` field.
    pub fn new(registry: Arc<R>, output: OutputConfig, cameras: &[CameraConfig]) -> Self {
        let cam_map = cameras
            .iter()
            .map(|c| (c.arlo_device_id.clone(), c.stream_name.clone()))
            .collect();
        let codec_seed: HashMap<CameraId, Codec> = cameras
            .iter()
            .filter_map(|c| c.codec_hint.map(|h| (c.arlo_device_id.clone(), h)))
            .collect();
        Self {
            registry,
            output,
            cameras: cam_map,
            registered: RwLock::new(HashSet::new()),
            codec_cache: Arc::new(CodecCache::with_initial(codec_seed)),
        }
    }

    /// Borrow the codec cache (e.g., for a `/admin` snapshot endpoint).
    #[must_use]
    pub fn codec_cache(&self) -> Arc<CodecCache> {
        self.codec_cache.clone()
    }

    fn stream_name(&self, camera: &CameraId) -> Result<&StreamName, DomainError> {
        self.cameras
            .get(camera)
            .ok_or_else(|| DomainError::UnknownCamera(camera.to_string()))
    }

    async fn ensure_registered(&self, camera: &CameraId) -> Result<(), DomainError> {
        if self.registered.read().await.contains(camera) {
            Ok(())
        } else {
            Err(DomainError::UnknownCamera(camera.to_string()))
        }
    }
}

#[async_trait]
impl<R: PipelineRegistry> MediaMultiplexer for GstMediaMultiplexer<R> {
    #[instrument(skip(self), fields(camera = %camera))]
    async fn register(&self, camera: &CameraId) -> Result<(), DomainError> {
        let stream_name = self.stream_name(camera)?.clone();
        // Idempotent fast path: already up.
        if self.registered.read().await.contains(camera) {
            debug!("camera already registered; idempotent no-op");
            return Ok(());
        }
        let outputs = build_output_branches(&stream_name, &self.output);
        let timestamp = Local::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        // Initial idle source: synthetic until first thumbnail arrives.
        let idle = select_idle_source(None, &stream_name, &timestamp);
        match self.registry.register(camera, idle, outputs).await {
            Ok(()) => {
                self.registered.write().await.insert(camera.clone());
                info!("camera registered with idle pipeline");
                Ok(())
            }
            Err(MediaError::AlreadyRegistered(_)) => {
                // Adapter saw it first; we still record locally.
                self.registered.write().await.insert(camera.clone());
                debug!("registry reported already-registered; recording locally");
                Ok(())
            }
            Err(e) => {
                warn!(error = %e, "register failed");
                Err(e.into())
            }
        }
    }

    #[instrument(skip(self, source), fields(camera = %camera, url = %source.url))]
    async fn attach_live(
        &self,
        camera: &CameraId,
        source: StreamSource,
    ) -> Result<(), DomainError> {
        self.ensure_registered(camera).await?;
        // Apply cached codec hint when the orchestrator didn't supply one.
        let source = match source.codec_hint {
            Some(_) => source,
            None => StreamSource {
                url: source.url,
                codec_hint: self.codec_cache.get(camera).await,
            },
        };
        self.registry
            .set_live_source(camera, source)
            .await
            .map_err(Into::into)
    }

    #[instrument(skip(self), fields(camera = %camera))]
    async fn detach_live(&self, camera: &CameraId) -> Result<(), DomainError> {
        self.ensure_registered(camera).await?;
        self.registry
            .clear_live_source(camera)
            .await
            .map_err(Into::into)
    }

    #[instrument(skip(self, jpeg), fields(camera = %camera, bytes = jpeg.len()))]
    async fn refresh_thumbnail(&self, camera: &CameraId, jpeg: Bytes) -> Result<(), DomainError> {
        self.ensure_registered(camera).await?;
        if jpeg.is_empty() {
            return Err(MediaError::InvalidThumbnail("empty payload".to_string()).into());
        }
        self.registry
            .refresh_thumbnail(camera, jpeg)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use streamer_domain::config::{CameraConfig, CooldownConfig, OutputConfig, RtspOutput};
    use streamer_domain::stream::{Codec, StreamSource};

    /// In-memory recorder for `PipelineRegistry` calls.
    #[derive(Default)]
    struct FakeRegistry {
        registered: Mutex<Vec<(CameraId, IdleKind, OutputBranches)>>,
        live_set: Mutex<Vec<(CameraId, StreamSource)>>,
        live_clear: Mutex<Vec<CameraId>>,
        thumbs: Mutex<Vec<(CameraId, Bytes)>>,
        register_returns: Mutex<Option<MediaError>>,
        set_live_returns: Mutex<Option<MediaError>>,
    }

    impl FakeRegistry {
        fn rig_register_error(&self, e: MediaError) {
            *self.register_returns.lock().unwrap() = Some(e);
        }
        fn rig_set_live_error(&self, e: MediaError) {
            *self.set_live_returns.lock().unwrap() = Some(e);
        }
    }

    #[async_trait]
    impl PipelineRegistry for FakeRegistry {
        async fn register(
            &self,
            camera: &CameraId,
            idle: IdleKind,
            outputs: OutputBranches,
        ) -> Result<(), MediaError> {
            if let Some(e) = self.register_returns.lock().unwrap().take() {
                return Err(e);
            }
            self.registered
                .lock()
                .unwrap()
                .push((camera.clone(), idle, outputs));
            Ok(())
        }
        async fn set_live_source(
            &self,
            camera: &CameraId,
            source: StreamSource,
        ) -> Result<(), MediaError> {
            if let Some(e) = self.set_live_returns.lock().unwrap().take() {
                return Err(e);
            }
            self.live_set.lock().unwrap().push((camera.clone(), source));
            Ok(())
        }
        async fn clear_live_source(&self, camera: &CameraId) -> Result<(), MediaError> {
            self.live_clear.lock().unwrap().push(camera.clone());
            Ok(())
        }
        async fn refresh_thumbnail(
            &self,
            camera: &CameraId,
            jpeg: Bytes,
        ) -> Result<(), MediaError> {
            self.thumbs.lock().unwrap().push((camera.clone(), jpeg));
            Ok(())
        }
    }

    fn cam(id: &str) -> CameraId {
        CameraId::new(id)
    }

    fn name(s: &str) -> StreamName {
        StreamName::parse(s).unwrap()
    }

    fn output_config() -> OutputConfig {
        OutputConfig {
            rtsp: RtspOutput {
                bind: "0.0.0.0:8554".to_string(),
            },
            hls: None,
            dash: None,
            metrics_bind: "127.0.0.1:9090".to_string(),
            admin_bind: "127.0.0.1:9091".to_string(),
        }
    }

    fn cameras() -> Vec<CameraConfig> {
        vec![
            CameraConfig {
                arlo_device_id: cam("CAM_A"),
                stream_name: name("front_door"),
                codec_hint: Some(Codec::H265),
                cooldown: CooldownConfig::default(),
            },
            CameraConfig {
                arlo_device_id: cam("CAM_B"),
                stream_name: name("back_yard"),
                codec_hint: None,
                cooldown: CooldownConfig::default(),
            },
        ]
    }

    fn mux(reg: Arc<FakeRegistry>) -> GstMediaMultiplexer<FakeRegistry> {
        GstMediaMultiplexer::new(reg, output_config(), &cameras())
    }

    #[tokio::test]
    async fn register_brings_up_pipeline_with_synthetic_idle() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg.clone());
        m.register(&cam("CAM_A")).await.expect("register ok");

        let recorded = reg.registered.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        let (got_cam, idle, branches) = &recorded[0];
        assert_eq!(got_cam, &cam("CAM_A"));
        assert!(matches!(idle, IdleKind::Synthetic { .. }));
        assert_eq!(branches.rtsp_mount_path, "/front_door");
    }

    #[tokio::test]
    async fn register_is_idempotent_for_same_camera() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg.clone());
        m.register(&cam("CAM_A")).await.unwrap();
        m.register(&cam("CAM_A")).await.unwrap();
        // Second call short-circuits before hitting the registry.
        assert_eq!(reg.registered.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn register_unknown_camera_returns_unknown_camera_error() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg);
        let err = m.register(&cam("NOPE")).await.expect_err("must fail");
        assert!(matches!(err, DomainError::UnknownCamera(c) if c == "NOPE"));
    }

    #[tokio::test]
    async fn register_propagates_pipeline_error_as_adapter_transport() {
        let reg = Arc::new(FakeRegistry::default());
        reg.rig_register_error(MediaError::Pipeline("boom".to_string()));
        let m = mux(reg);
        let err = m.register(&cam("CAM_A")).await.expect_err("must fail");
        match err {
            DomainError::AdapterTransport(msg) => assert!(msg.contains("boom")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn register_swallows_already_registered_error_from_registry() {
        let reg = Arc::new(FakeRegistry::default());
        reg.rig_register_error(MediaError::AlreadyRegistered("CAM_A".to_string()));
        let m = mux(reg.clone());
        // Should succeed at the port level even though the registry returned AlreadyRegistered.
        m.register(&cam("CAM_A")).await.expect("ok");
        // And subsequent attaches see the camera as registered.
        m.attach_live(
            &cam("CAM_A"),
            StreamSource {
                url: "rtsps://x".to_string(),
                codec_hint: Some(Codec::H264),
            },
        )
        .await
        .expect("attach ok");
    }

    #[tokio::test]
    async fn attach_live_before_register_returns_unknown_camera() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg);
        let err = m
            .attach_live(
                &cam("CAM_A"),
                StreamSource {
                    url: "rtsps://x".to_string(),
                    codec_hint: None,
                },
            )
            .await
            .expect_err("must fail");
        assert!(matches!(err, DomainError::UnknownCamera(_)));
    }

    #[tokio::test]
    async fn attach_live_uses_explicit_codec_hint_when_present() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg.clone());
        m.register(&cam("CAM_A")).await.unwrap();
        m.attach_live(
            &cam("CAM_A"),
            StreamSource {
                url: "rtsps://x".to_string(),
                codec_hint: Some(Codec::H264),
            },
        )
        .await
        .unwrap();
        let calls = reg.live_set.lock().unwrap();
        assert_eq!(calls[0].1.codec_hint, Some(Codec::H264));
    }

    #[tokio::test]
    async fn attach_live_falls_back_to_cached_codec_hint_when_source_missing_one() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg.clone());
        // CAM_A was seeded with Codec::H265 in cameras().
        m.register(&cam("CAM_A")).await.unwrap();
        m.attach_live(
            &cam("CAM_A"),
            StreamSource {
                url: "rtsps://x".to_string(),
                codec_hint: None,
            },
        )
        .await
        .unwrap();
        let calls = reg.live_set.lock().unwrap();
        assert_eq!(calls[0].1.codec_hint, Some(Codec::H265));
    }

    #[tokio::test]
    async fn attach_live_with_no_hint_and_no_cache_passes_through_none() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg.clone());
        // CAM_B has no configured codec_hint.
        m.register(&cam("CAM_B")).await.unwrap();
        m.attach_live(
            &cam("CAM_B"),
            StreamSource {
                url: "rtsps://x".to_string(),
                codec_hint: None,
            },
        )
        .await
        .unwrap();
        let calls = reg.live_set.lock().unwrap();
        assert!(calls[0].1.codec_hint.is_none());
    }

    #[tokio::test]
    async fn attach_live_propagates_registry_failure() {
        let reg = Arc::new(FakeRegistry::default());
        reg.rig_set_live_error(MediaError::SpliceTimeout { timeout_secs: 5 });
        let m = mux(reg.clone());
        m.register(&cam("CAM_A")).await.unwrap();
        let err = m
            .attach_live(
                &cam("CAM_A"),
                StreamSource {
                    url: "rtsps://x".to_string(),
                    codec_hint: Some(Codec::H264),
                },
            )
            .await
            .expect_err("must fail");
        match err {
            DomainError::AdapterTransport(msg) => assert!(msg.contains("splice timeout")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn detach_live_before_register_returns_unknown_camera() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg);
        let err = m.detach_live(&cam("CAM_A")).await.expect_err("must fail");
        assert!(matches!(err, DomainError::UnknownCamera(_)));
    }

    #[tokio::test]
    async fn detach_live_clears_registry_when_known() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg.clone());
        m.register(&cam("CAM_A")).await.unwrap();
        m.detach_live(&cam("CAM_A")).await.unwrap();
        let cleared = reg.live_clear.lock().unwrap();
        assert_eq!(cleared.len(), 1);
        assert_eq!(cleared[0], cam("CAM_A"));
    }

    #[tokio::test]
    async fn refresh_thumbnail_rejects_empty_payload() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg);
        m.register(&cam("CAM_A")).await.unwrap();
        let err = m
            .refresh_thumbnail(&cam("CAM_A"), Bytes::new())
            .await
            .expect_err("must fail");
        match err {
            DomainError::AdapterTransport(msg) => {
                assert!(msg.contains("invalid thumbnail"));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn refresh_thumbnail_before_register_returns_unknown_camera() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg);
        let err = m
            .refresh_thumbnail(&cam("CAM_A"), Bytes::from_static(&[0xFF, 0xD8]))
            .await
            .expect_err("must fail");
        assert!(matches!(err, DomainError::UnknownCamera(_)));
    }

    #[tokio::test]
    async fn refresh_thumbnail_pushes_jpeg_to_registry() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg.clone());
        m.register(&cam("CAM_A")).await.unwrap();
        let jpeg = Bytes::from_static(&[0xFF, 0xD8, 0xFF, 0xE0]);
        m.refresh_thumbnail(&cam("CAM_A"), jpeg.clone())
            .await
            .unwrap();
        let pushed = reg.thumbs.lock().unwrap();
        assert_eq!(pushed.len(), 1);
        assert_eq!(pushed[0].0, cam("CAM_A"));
        assert_eq!(pushed[0].1, jpeg);
    }

    #[tokio::test]
    async fn codec_cache_is_seeded_from_camera_config() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg);
        let cache = m.codec_cache();
        assert_eq!(cache.get(&cam("CAM_A")).await, Some(Codec::H265));
        assert!(cache.get(&cam("CAM_B")).await.is_none());
    }
}
