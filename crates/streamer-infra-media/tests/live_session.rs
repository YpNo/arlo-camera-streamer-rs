//! End-to-end live sessions through the real GStreamer stack, no camera.
//!
//! The production `GstMediaMultiplexer` + `GstPipelineRegistry` +
//! `RtspServer` run as in the daemon. A second `webrtcbin` stands in for
//! Arlo's gateway (see `support::gateway`) and an RTSP client stands in
//! for the viewer (see `support::probe`). What these tests prove, and the
//! unit tests cannot:
//!
//! - our offer negotiates with a non-bundled answerer and RTP flows;
//! - the idle → live → idle splice reaches a connected client without
//!   end-of-stream or error (the ADR 0003 promise);
//! - a client that connects in the middle of a session gets live video;
//! - a source that goes silent is reported as `RtpStalled` (ADR 0004);
//! - a call that dies during setup fails the attach with the detector's
//!   reason, well before the 20 s first-RTP timeout.
//!
//! Skipped when a GStreamer element is missing, unless
//! `STREAMER_REQUIRE_GST_IT=1` (set in CI). The tests share the default
//! `GLib` main context through the RTSP server, so they run one at a time.

mod support;

use std::time::Duration;

use streamer_domain::config::WebrtcConfig;
use streamer_domain::error::DomainError;
use streamer_domain::port::{MediaMultiplexer, WebrtcSignaler};
use streamer_domain::state::LiveLossReason;

use support::gateway::FakeGateway;
use support::probe::{RtspProbe, eventually};
use support::{Stack, fast_stall, gstreamer_ready};

/// Upper bound for a picture change seen by the client (media prepare,
/// first live keyframe, decode).
const PICTURE_TIMEOUT: Duration = Duration::from_secs(30);
/// The idle screen is black with a caption; the gateway sends white.
const IDLE_MAX_LUMA: u64 = 80;
const LIVE_MIN_LUMA: u64 = 180;
/// The first-RTP timeout in `webrtc_pipeline.rs`; an early loss must
/// beat it.
const FIRST_RTP_TIMEOUT: Duration = Duration::from_secs(20);

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn shows_idle(probe: &RtspProbe) -> bool {
    probe.frames() > 0 && probe.mean_luma() < IDLE_MAX_LUMA
}

fn shows_live(probe: &RtspProbe) -> bool {
    probe.mean_luma() > LIVE_MIN_LUMA
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_session_connected_client_sees_idle_live_idle_without_interruption() {
    let _serial = SERIAL.lock().await;
    if !gstreamer_ready() {
        return;
    }
    let stack = Stack::new(WebrtcConfig::default());
    stack.media.register(&stack.camera).await.expect("register");
    let probe = RtspProbe::connect(&stack.url());
    assert!(
        eventually(PICTURE_TIMEOUT, || shows_idle(&probe)).await,
        "client never showed the idle screen (frames={}, luma={})",
        probe.frames(),
        probe.mean_luma()
    );

    let gateway = FakeGateway::default();
    let session = stack
        .media
        .attach_live(&stack.camera, &gateway)
        .await
        .expect("attach_live");
    assert_eq!(gateway.negotiations(), 1);
    assert!(
        eventually(PICTURE_TIMEOUT, || shows_live(&probe)).await,
        "client never switched to live (luma={})",
        probe.mean_luma()
    );

    stack
        .media
        .detach_live(&stack.camera)
        .await
        .expect("detach_live");
    gateway.teardown(&stack.camera).await.expect("teardown");
    drop(session);
    assert!(
        eventually(PICTURE_TIMEOUT, || shows_idle(&probe)).await,
        "client never returned to idle (luma={})",
        probe.mean_luma()
    );
    assert!(!probe.interrupted(), "the splice interrupted the client");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_session_client_joining_mid_session_sees_live() {
    let _serial = SERIAL.lock().await;
    if !gstreamer_ready() {
        return;
    }
    let stack = Stack::new(WebrtcConfig::default());
    stack.media.register(&stack.camera).await.expect("register");
    let gateway = FakeGateway::default();
    // No client yet: the media does not exist while the session starts.
    let _session = stack
        .media
        .attach_live(&stack.camera, &gateway)
        .await
        .expect("attach_live");

    let probe = RtspProbe::connect(&stack.url());
    assert!(
        eventually(PICTURE_TIMEOUT, || shows_live(&probe)).await,
        "a client joining mid-session never got live video (frames={}, luma={})",
        probe.frames(),
        probe.mean_luma()
    );
    stack
        .media
        .detach_live(&stack.camera)
        .await
        .expect("detach_live");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_session_silent_source_reports_rtp_stalled() {
    let _serial = SERIAL.lock().await;
    if !gstreamer_ready() {
        return;
    }
    let config = fast_stall();
    let stall_timeout = config.live_stall_timeout();
    let stack = Stack::new(config);
    stack.media.register(&stack.camera).await.expect("register");
    let gateway = FakeGateway::default();
    let mut session = stack
        .media
        .attach_live(&stack.camera, &gateway)
        .await
        .expect("attach_live");

    gateway.stall_video();
    let reason = tokio::time::timeout(stall_timeout * 3, session.lost())
        .await
        .expect("no loss reported after the source went silent");
    assert_eq!(reason, LiveLossReason::RtpStalled);
    stack
        .media
        .detach_live(&stack.camera)
        .await
        .expect("detach_live");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_session_call_dropped_during_setup_fails_attach_with_reason() {
    let _serial = SERIAL.lock().await;
    if !gstreamer_ready() {
        return;
    }
    let stack = Stack::new(WebrtcConfig::default());
    stack.media.register(&stack.camera).await.expect("register");
    let gateway = FakeGateway::hanging_up();

    let started = std::time::Instant::now();
    let err = stack
        .media
        .attach_live(&stack.camera, &gateway)
        .await
        .expect_err("attach against a dead call must fail");
    let message = err.to_string();
    assert!(
        message.contains("live source lost during setup"),
        "unexpected attach error: {message}"
    );
    assert!(
        started.elapsed() < FIRST_RTP_TIMEOUT,
        "the loss took the whole first-RTP timeout ({:?})",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_session_refused_attach_keeps_camera_busy_and_frees_the_next_attach() {
    let _serial = SERIAL.lock().await;
    if !gstreamer_ready() {
        return;
    }
    let stack = Stack::new(WebrtcConfig::default());
    stack.media.register(&stack.camera).await.expect("register");

    let refused = stack
        .media
        .attach_live(&stack.camera, &FakeGateway::busy())
        .await
        .expect_err("a busy camera must refuse the attach");
    assert!(
        matches!(refused, DomainError::CameraBusy(_)),
        "CameraBusy must reach the orchestrator unchanged, got {refused:?}"
    );

    let gateway = FakeGateway::default();
    let _session = stack
        .media
        .attach_live(&stack.camera, &gateway)
        .await
        .expect("a failed attach must not block the next one");
    stack
        .media
        .detach_live(&stack.camera)
        .await
        .expect("detach_live");
}
