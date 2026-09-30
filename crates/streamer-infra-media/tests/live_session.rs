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

use streamer_domain::config::{HlsOutput, WebrtcConfig};
use streamer_domain::error::DomainError;
use streamer_domain::port::{MediaMultiplexer, WebrtcSignaler};
use streamer_domain::state::LiveLossReason;

use streamer_infra_media::pipeline_desc::HLS_SEGMENTS_BEYOND_PLAYLIST;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hls_output_segments_the_splice_bounds_disk_and_cleans_up() {
    let _serial = SERIAL.lock().await;
    if !gstreamer_ready() {
        return;
    }
    let root = std::env::temp_dir().join(format!("streamer-it-hls-{}", std::process::id()));
    let dir = root.join(support::STREAM);
    let hls = HlsOutput {
        dir: root.clone(),
        segment_secs: 1,
        playlist_length: 3,
    };
    let max_files = 3 + HLS_SEGMENTS_BEYOND_PLAYLIST;
    let stack = Stack::with_hls(WebrtcConfig::default(), hls);
    stack.media.register(&stack.camera).await.expect("register");

    let playlist = dir.join("index.m3u8");
    assert!(
        eventually(PICTURE_TIMEOUT, || playlist.exists()
            && segments(&dir).len() >= 2)
        .await,
        "no HLS playlist and segments in {}",
        dir.display()
    );

    let gateway = FakeGateway::default();
    let _session = stack
        .media
        .attach_live(&stack.camera, &gateway)
        .await
        .expect("attach_live");
    let live_segment = || {
        segments(&dir)
            .into_iter()
            .rev()
            .nth(1) // the newest one may still be written
            .and_then(|s| support::segment_mean_luma(&s))
            .is_some_and(|luma| luma > LIVE_MIN_LUMA)
    };
    assert!(
        eventually(PICTURE_TIMEOUT, live_segment).await,
        "the live picture never reached an HLS segment"
    );
    assert!(
        segments(&dir).len() <= max_files as usize + 1,
        "hlssink2 kept {} segments, expected at most {max_files}",
        segments(&dir).len()
    );

    stack
        .media
        .detach_live(&stack.camera)
        .await
        .expect("detach_live");
    drop(stack);
    assert!(
        eventually(PICTURE_TIMEOUT, || !playlist.exists()
            && segments(&dir).is_empty())
        .await,
        "the playlist and segments must be removed when the segmenter stops"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_view_notice_switches_the_caption_without_interrupting_the_client() {
    let _serial = SERIAL.lock().await;
    if !gstreamer_ready() {
        return;
    }
    let stack = Stack::new(WebrtcConfig::default());
    stack.media.register(&stack.camera).await.expect("register");
    let probe = RtspProbe::connect(&stack.url());
    assert!(
        eventually(PICTURE_TIMEOUT, || shows_idle(&probe)).await,
        "client never showed the idle screen"
    );

    for shown in [true, false, true] {
        stack
            .media
            .set_user_view_notice(&stack.camera, shown)
            .await
            .expect("set_user_view_notice");
        let frames = probe.frames();
        assert!(
            eventually(PICTURE_TIMEOUT, || probe.frames() > frames + 2).await,
            "frames stopped after the caption changed"
        );
    }
    assert!(shows_idle(&probe), "the caption stays on the idle screen");
    assert!(
        !probe.interrupted(),
        "a caption change interrupted the client"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_client_over_udp_joins_a_playing_media() {
    // With HLS on, the segmenter is always the first client, so every
    // viewer joins a media that is already playing (VLC over UDP).
    let _serial = SERIAL.lock().await;
    if !gstreamer_ready() {
        return;
    }
    let stack = Stack::new(WebrtcConfig::default());
    stack.media.register(&stack.camera).await.expect("register");
    let first = RtspProbe::connect(&stack.url());
    assert!(
        eventually(PICTURE_TIMEOUT, || shows_idle(&first)).await,
        "first client never showed the idle screen"
    );

    let second = RtspProbe::connect_over(&stack.url(), "udp");
    assert!(
        eventually(PICTURE_TIMEOUT, || shows_idle(&second)).await,
        "a UDP client joining a playing media got no picture (frames={})",
        second.frames()
    );
    assert!(!second.interrupted(), "the joining client saw an error");
    assert!(!first.interrupted(), "the first client was disturbed");
}

/// Segment files in `dir`, oldest first (names are zero-padded).
fn segments(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found: Vec<_> = std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|e| e == "ts"))
                .collect()
        })
        .unwrap_or_default();
    found.sort();
    found
}
