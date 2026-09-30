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
use streamer_domain::config::{CameraConfig, OutputConfig, WebrtcConfig};
use streamer_domain::error::DomainError;
use streamer_domain::port::{MediaMultiplexer, WebrtcSignaler};
use streamer_domain::stream::{Codec, LiveLossNotifier, LiveSession, WatchAlongUrl};

use crate::codec_cache::CodecCache;
use crate::error::MediaError;
use crate::idle_source::{IdleKind, select_idle_source, standby_caption, user_view_caption};
use crate::live_rtp_sink::LiveSinks;
use crate::live_watch::setup_or_loss;
use crate::pipeline_desc::{OutputBranches, build_output_branches};
use crate::rtsp_relay::RtspRelay;
use crate::webrtc_pipeline::WebrtcLive;

/// Trait that the GStreamer-backed registry implements. The seam keeps
/// the multiplexer testable without spinning up a real pipeline.
///
/// Implementations are responsible for:
/// - Building the per-camera persistent pipeline + RTSP mount in
///   `register` (the Phase-6 [`crate::pipeline_desc::combined_launch_string`]:
///   idle + appsrc-live + input-selector + single payloader).
/// - Returning a [`LiveSinks`] pair from `attach_live_sink` and wiring the
///   IDR-aligned `input-selector` swap to live (see
///   [`crate::splice::KeyframeWatcher`]).
/// - Returning the camera to idle on `detach_live_sink` — also
///   IDR-aligned to avoid showing partial GOPs to clients.
/// - Pushing fresh JPEGs to the idle `appsrc` in `refresh_thumbnail`.
#[async_trait]
pub trait PipelineRegistry: Send + Sync {
    /// Bring up the persistent pipeline + RTSP mount for `camera`.
    /// Calling twice for the same camera surfaces
    /// [`MediaError::AlreadyRegistered`] (the multiplexer translates
    /// that into `Ok(())` for the port-level contract).
    async fn register(
        &self,
        camera: &CameraId,
        idle: IdleKind,
        outputs: OutputBranches,
    ) -> Result<(), MediaError>;

    /// Arm the camera's live ingestion. Returns a [`LiveSinks`] pair the
    /// caller pushes inbound H.264 RTP (`video`) and Opus RTP (`audio`)
    /// into. The implementation flips the camera's `input-selector` to
    /// the live video branch on the first live raw frame and mixes the
    /// live audio onto the silent bed — clients connected to the idle
    /// stream see a seamless transition (no EOS, no reconnect, no
    /// client-side decoder re-init).
    async fn attach_live_sink(&self, camera: &CameraId) -> Result<LiveSinks, MediaError>;

    /// Revert the camera to idle. The implementation flips the
    /// `input-selector` back on the next idle IDR and stops draining
    /// the live sink. Idempotent: calling on a camera that isn't live
    /// is `Ok(())`.
    async fn detach_live_sink(&self, camera: &CameraId) -> Result<(), MediaError>;

    /// Replace the idle still frame for the camera (best-effort).
    async fn refresh_thumbnail(&self, camera: &CameraId, jpeg: Bytes) -> Result<(), MediaError>;

    /// Replace the caption of the camera's idle frame, now and for any
    /// media built later.
    async fn set_idle_caption(&self, camera: &CameraId, caption: String) -> Result<(), MediaError>;
}

/// `MediaMultiplexer` implementation generic over a [`PipelineRegistry`].
pub struct GstMediaMultiplexer<R: PipelineRegistry> {
    registry: Arc<R>,
    output: OutputConfig,
    /// WebRTC ingestion knobs (ICE address family, …) applied to every
    /// camera's `WebrtcLive`. Read-only after construction.
    webrtc: WebrtcConfig,
    /// Camera → stream-name mapping captured at boot. Read-only after
    /// construction.
    cameras: HashMap<CameraId, StreamName>,
    /// Lazily-populated set of cameras whose pipelines have been built.
    registered: RwLock<HashSet<CameraId>>,
    /// Codec hint cache (seeded from config, updated by the registry
    /// after first parsebin emission).
    codec_cache: Arc<CodecCache>,
    /// Active per-camera live leg (the webrtcbin pipeline or the
    /// watch-along relay). Present only between an attach and
    /// `detach_live`. The live RTP byte sinks are owned by the registry;
    /// the multiplexer keeps the ingestion leg alive for the session.
    live: tokio::sync::Mutex<HashMap<CameraId, LiveLeg>>,
}

/// The ingestion side of a live session.
enum LiveLeg {
    Webrtc(WebrtcLive),
    Relay(RtspRelay),
}

impl LiveLeg {
    fn shutdown(&mut self) {
        match self {
            Self::Webrtc(leg) => leg.shutdown(),
            Self::Relay(leg) => leg.shutdown(),
        }
    }
}

impl<R: PipelineRegistry> GstMediaMultiplexer<R> {
    /// Construct a multiplexer.
    ///
    /// Builds the camera→stream-name index and seeds the codec cache
    /// from the per-camera `codec_hint` field.
    pub fn new(
        registry: Arc<R>,
        output: OutputConfig,
        webrtc: WebrtcConfig,
        cameras: &[CameraConfig],
    ) -> Self {
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
            webrtc,
            cameras: cam_map,
            registered: RwLock::new(HashSet::new()),
            codec_cache: Arc::new(CodecCache::with_initial(codec_seed)),
            live: tokio::sync::Mutex::new(HashMap::new()),
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

impl<R: PipelineRegistry> GstMediaMultiplexer<R> {
    /// The attach shared by both live legs: take the registry's live
    /// sinks, mint the session, race `setup` against its loss signal
    /// (ADR 0004: a detector that fires during setup fails the attach
    /// with its reason), and keep the leg for `detach_live`. A failed
    /// setup releases the sinks, or every later attach would be refused
    /// with "already in live mode".
    async fn arm_live<F, Fut>(
        &self,
        camera: &CameraId,
        setup: F,
    ) -> Result<LiveSession, DomainError>
    where
        F: FnOnce(LiveSinks, LiveLossNotifier) -> Fut,
        Fut: std::future::Future<Output = Result<LiveLeg, MediaError>>,
    {
        self.ensure_registered(camera).await?;
        let sinks = self.registry.attach_live_sink(camera).await?;
        let (mut session, notifier) = LiveSession::new();
        let leg = match setup_or_loss(setup(sinks, notifier), &mut session).await {
            Ok(leg) => leg,
            Err(e) => {
                if let Err(release) = self.registry.detach_live_sink(camera).await {
                    warn!(error = %release, "releasing the live sinks after a failed attach failed");
                }
                return Err(e.into());
            }
        };
        self.live.lock().await.insert(camera.clone(), leg);
        Ok(session)
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

    #[instrument(skip(self, signaler), fields(camera = %camera))]
    async fn attach_live(
        &self,
        camera: &CameraId,
        signaler: &dyn WebrtcSignaler,
    ) -> Result<LiveSession, DomainError> {
        // 1. The registry hands back the live RTP byte sinks for the
        // camera's persistent pipeline and arms its `input-selector`
        // flip → 2. `WebrtcLive::start` runs one `negotiate`: the
        // signaler fetches the call's ICE servers (sipInfo), webrtcbin
        // builds the offer with them, the signaler carries it to Arlo →
        // 3. answer applied, inbound RTP flows into the sinks. Connected
        // RTSP clients see a seamless switch idle → live at the next
        // live keyframe.
        let webrtc = &self.webrtc;
        self.arm_live(camera, |sinks, notifier| async move {
            WebrtcLive::start(camera, signaler, sinks, webrtc, notifier)
                .await
                .map(LiveLeg::Webrtc)
        })
        .await
    }

    #[instrument(skip(self), fields(camera = %camera, url = %url))]
    async fn attach_user_view(
        &self,
        camera: &CameraId,
        url: &WatchAlongUrl,
    ) -> Result<LiveSession, DomainError> {
        // Same splice as a WebRTC session; the source is the RTSP relay
        // of the view the user watches in the app (ADR 0007). Video
        // only: the app's audio is AAC, the live audio path is Opus, and
        // the silent bed covers it.
        let stall = self.webrtc.live_stall_timeout();
        self.arm_live(camera, |sinks, notifier| async move {
            RtspRelay::start(url, sinks.video, stall, notifier)
                .await
                .map(LiveLeg::Relay)
        })
        .await
    }

    #[instrument(skip(self), fields(camera = %camera))]
    async fn detach_live(&self, camera: &CameraId) -> Result<(), DomainError> {
        self.ensure_registered(camera).await?;
        // Arm the reverse splice: the registry flips its
        // `input-selector` back to the idle branch on the next idle
        // IDR. Then tear down the webrtcbin pipeline (its appsink
        // callback stops pushing into the sink; the registry's pump
        // task exits when the channel closes). The Arlo signaling
        // session is released by the orchestrator
        // (`WebrtcSignaler::teardown`, paired with this).
        let res = self.registry.detach_live_sink(camera).await;
        if let Some(mut leg) = self.live.lock().await.remove(camera) {
            leg.shutdown();
        }
        res.map_err(Into::into)
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

    #[instrument(skip(self), fields(camera = %camera))]
    async fn set_user_view_notice(
        &self,
        camera: &CameraId,
        shown: bool,
    ) -> Result<(), DomainError> {
        self.ensure_registered(camera).await?;
        let stream = self.stream_name(camera)?;
        let caption = if shown {
            user_view_caption(stream)
        } else {
            standby_caption(
                stream,
                &Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
            )
        };
        self.registry
            .set_idle_caption(camera, caption)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use streamer_domain::config::{
        CameraConfig, CooldownConfig, OutputConfig, RtspOutput, VideoEncoder,
    };
    use streamer_domain::stream::{Codec, SignalingAnswer};

    /// Minimal `WebrtcSignaler` double — `attach_live` is stubbed in
    /// Phase 1, so it never actually calls these.
    struct NoopSignaler;
    #[async_trait]
    impl WebrtcSignaler for NoopSignaler {
        async fn negotiate(
            &self,
            _camera: &CameraId,
            _offer: &mut dyn streamer_domain::port::OfferBuilder,
        ) -> Result<SignalingAnswer, DomainError> {
            Ok(SignalingAnswer {
                answer_sdp: "v=0\r\n".to_string(),
                session_id: "s".to_string(),
            })
        }
        async fn teardown(&self, _camera: &CameraId) -> Result<(), DomainError> {
            Ok(())
        }
    }

    /// In-memory recorder for `PipelineRegistry` calls.
    #[derive(Default)]
    struct FakeRegistry {
        registered: Mutex<Vec<(CameraId, IdleKind, OutputBranches)>>,
        attached: Mutex<Vec<CameraId>>,
        detached: Mutex<Vec<CameraId>>,
        thumbs: Mutex<Vec<(CameraId, Bytes)>>,
        captions: Mutex<Vec<(CameraId, String)>>,
        register_returns: Mutex<Option<MediaError>>,
    }

    impl FakeRegistry {
        fn rig_register_error(&self, e: MediaError) {
            *self.register_returns.lock().unwrap() = Some(e);
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
        async fn attach_live_sink(&self, camera: &CameraId) -> Result<LiveSinks, MediaError> {
            self.attached.lock().unwrap().push(camera.clone());
            // The receivers are dropped immediately — the multiplexer
            // test only exercises the `ensure_registered` guard (real
            // attach drives webrtcbin and isn't unit-tested).
            let (sinks, _rxs) = LiveSinks::new();
            Ok(sinks)
        }
        async fn detach_live_sink(&self, camera: &CameraId) -> Result<(), MediaError> {
            self.detached.lock().unwrap().push(camera.clone());
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

        async fn set_idle_caption(
            &self,
            camera: &CameraId,
            caption: String,
        ) -> Result<(), MediaError> {
            self.captions
                .lock()
                .unwrap()
                .push((camera.clone(), caption));
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
            video_encoder: VideoEncoder::X264,
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
        GstMediaMultiplexer::new(reg, output_config(), WebrtcConfig::default(), &cameras())
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
        // And the camera is then recorded as registered (detach no
        // longer errors with UnknownCamera).
        m.detach_live(&cam("CAM_A")).await.expect("registered");
    }

    #[tokio::test]
    async fn attach_live_before_register_returns_unknown_camera() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg);
        let err = m
            .attach_live(&cam("CAM_A"), &NoopSignaler)
            .await
            .expect_err("must fail");
        assert!(matches!(err, DomainError::UnknownCamera(_)));
    }

    // `attach_live` for a *registered* camera now drives a real
    // `webrtcbin` pipeline + live signaling — that needs GStreamer and
    // the camera, so it is exercised by the Phase 4 manual gate, not a
    // unit test (same rationale as `gst_pipeline.rs` coverage exclusion).
    // The pre-register guard above stays unit-tested.

    #[tokio::test]
    async fn detach_live_before_register_returns_unknown_camera() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg);
        let err = m.detach_live(&cam("CAM_A")).await.expect_err("must fail");
        assert!(matches!(err, DomainError::UnknownCamera(_)));
    }

    #[tokio::test]
    async fn detach_live_forwards_to_registry_when_known() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg.clone());
        m.register(&cam("CAM_A")).await.unwrap();
        m.detach_live(&cam("CAM_A")).await.unwrap();
        let detached = reg.detached.lock().unwrap();
        assert_eq!(detached.len(), 1);
        assert_eq!(detached[0], cam("CAM_A"));
    }

    #[tokio::test]
    async fn attach_user_view_before_register_returns_unknown_camera() {
        let m = mux(Arc::new(FakeRegistry::default()));
        let url = WatchAlongUrl::parse("rtsp://127.0.0.1:1/x").unwrap();
        let err = m.attach_user_view(&cam("CAM_A"), &url).await.unwrap_err();
        assert!(matches!(err, DomainError::UnknownCamera(_)));
    }

    #[tokio::test]
    async fn attach_user_view_unreachable_stream_fails_and_releases_the_sinks() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg.clone());
        m.register(&cam("CAM_A")).await.unwrap();
        // Port 1 on loopback: nothing listens, the connect is refused.
        let url = WatchAlongUrl::parse("rtsp://127.0.0.1:1/x").unwrap();
        let err = m.attach_user_view(&cam("CAM_A"), &url).await.unwrap_err();
        assert!(
            matches!(err, DomainError::AdapterTransport(ref msg) if msg.contains("rtsp relay")),
            "{err:?}"
        );
        assert_eq!(reg.attached.lock().unwrap().len(), 1);
        assert_eq!(
            reg.detached.lock().unwrap().len(),
            1,
            "sinks released after the failure"
        );
    }

    #[tokio::test]
    async fn set_user_view_notice_switches_between_app_and_standby_captions() {
        let reg = Arc::new(FakeRegistry::default());
        let m = mux(reg.clone());
        m.register(&cam("CAM_A")).await.unwrap();
        m.set_user_view_notice(&cam("CAM_A"), true).await.unwrap();
        m.set_user_view_notice(&cam("CAM_A"), false).await.unwrap();
        let captions = reg.captions.lock().unwrap();
        assert_eq!(
            captions[0],
            (cam("CAM_A"), "LIVE IN ARLO APP · front_door".to_string())
        );
        assert!(
            captions[1].1.starts_with("STANDBY · front_door · "),
            "{:?}",
            captions[1]
        );
    }

    #[tokio::test]
    async fn set_user_view_notice_before_register_returns_unknown_camera() {
        let m = mux(Arc::new(FakeRegistry::default()));
        let err = m
            .set_user_view_notice(&cam("CAM_A"), true)
            .await
            .expect_err("must fail");
        assert!(matches!(err, DomainError::UnknownCamera(_)));
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
