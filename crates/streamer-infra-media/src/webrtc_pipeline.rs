//! Per-camera GStreamer `webrtcbin` live pipeline.
//!
//! Arlo v3 live is a WebRTC call brokered by a **non-bundled
//! `FreeSWITCH`** gateway. `webrtcbin` (libnice/DTLS/SRTP) is the only
//! stack that negotiates it (proven against the live camera). This
//! module owns one persistent pipeline per active camera:
//!
//! ```text
//!  audiotestsrc(silence) ! opusenc ! rtpopuspay ! webrtcbin   (m0 sendrecv)
//!  webrtcbin (m1 recvonly H.264)  --pad-added(video)--> appsink → video sink
//!  webrtcbin (m0 recv Opus)       --pad-added(audio)--> appsink → audio sink
//!                                                          │ RTP bytes
//!                                                          ▼
//!                          appsrc pair inside the camera's persistent
//!                          gst-rtsp-server media (Phase 6.3 / 8b).
//! ```
//!
//! The offer is generated here, carried to Arlo via the domain
//! [`WebrtcSignaler`] (signaling-only arlo-rs), and the answer applied
//! verbatim. Inbound H.264 RTP is forwarded into `sinks.video` and the
//! camera's Opus RTP into `sinks.audio` (Phase 8b); the registry's pump
//! tasks drain each into the matching live appsrc on the persistent
//! pipeline (video → decode → I420 splice; audio → decode → mix onto
//! silence). Teardown of the Arlo signaling session is the
//! orchestrator's job (`WebrtcSignaler::teardown`, paired with detach);
//! `WebrtcLive::shutdown` only tears down the local webrtcbin pipeline.
//!
//! ## Live-loss detection (ADR 0004)
//!
//! Four detectors share one [`LiveLossNotifier`]; the first to fire
//! wins and the orchestrator returns the camera to idle:
//!
//! - a **stall watchdog** task: no inbound video RTP for
//!   `webrtc.live_stall_timeout_secs` after the first packet
//!   ([`crate::live_watch`] holds the pure logic);
//! - the **bus watch**: `ERROR` → `PipelineError`, `EOS` → `EndOfStream`;
//! - `webrtcbin` **`connection-state`** / **`ice-connection-state`**
//!   reaching `failed` / `closed` → `PeerDisconnected` (`disconnected`
//!   may recover per the WebRTC state machine and is only logged; a
//!   persistent one is caught by the watchdog).
//!
//! Exercised end to end by `tests/live_session.rs` against a local
//! `webrtcbin` standing in for Arlo's gateway; behaviour specific to
//! Arlo itself (TURN, SDP quirks) still needs the live gate.

#![allow(clippy::module_name_repetitions)]

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use gstreamer_sdp as gst_sdp;
use gstreamer_webrtc as gst_webrtc;
use tokio::sync::{Notify, mpsc};
use tracing::{debug, info, warn};

use streamer_domain::camera::CameraId;
use streamer_domain::config::WebrtcConfig;
use streamer_domain::port::WebrtcSignaler;
use streamer_domain::state::LiveLossReason;
use streamer_domain::stream::{IceAddressFamily, IceServer, LiveLossNotifier};

use crate::error::MediaError;
use crate::live_rtp_sink::{LiveRtpSink, LiveSinks};
use crate::live_watch::{RtpActivity, stall_verdict};

/// H.264 payload type pinned in our offer (Arlo's gateway answers 103).
const H264_PT: i32 = 103;
/// Opus payload type for the mandatory SIP audio leg.
const OPUS_PT: i32 = 111;
/// Periodic keyframe-request cadence (force-key-unit → PLI/FIR).
const KEYFRAME_INTERVAL: Duration = Duration::from_secs(3);
/// Max wait from `set-remote-description` to the first inbound RTP.
const FIRST_RTP_TIMEOUT_SECS: u64 = 20;
/// Max wait for ICE gathering to complete the local offer.
const OFFER_TIMEOUT: Duration = Duration::from_secs(20);
/// Name of the application message [`WebrtcLive::shutdown`] posts so the
/// bus-watch thread returns instead of blocking on a bus that will never
/// carry another message.
const BUS_WATCH_STOP: &str = "streamer-bus-watch-stop";

/// A running per-camera `webrtcbin` live session. Drop or
/// [`shutdown`](Self::shutdown) tears down the local pipeline (the
/// Arlo signaling session is released separately by the orchestrator
/// via [`WebrtcSignaler::teardown`]; the [`LiveRtpSink`] consumer's
/// lifetime is the caller's responsibility).
pub(crate) struct WebrtcLive {
    pipeline: gst::Pipeline,
    /// `Notify`-gated tasks (PLI keyframe pump) abort on drop.
    tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Set by the first [`shutdown`](Self::shutdown).
    stopped: bool,
}

impl WebrtcLive {
    /// Take ownership of `pipeline` before anything can fail, so every
    /// exit from [`start`](Self::start), cancellation included, shuts it
    /// down through `Drop`.
    fn owning(pipeline: gst::Pipeline) -> Self {
        Self {
            pipeline,
            tasks: Vec::new(),
            stopped: false,
        }
    }

    /// Stop the pipeline. Idempotent: the multiplexer calls it and `Drop`
    /// runs it again, when the bus is already flushing and would refuse
    /// the stop message.
    pub(crate) fn shutdown(&mut self) {
        if std::mem::replace(&mut self.stopped, true) {
            return;
        }
        for t in self.tasks.drain(..) {
            t.abort();
        }
        // Release the bus-watch thread *before* the bus goes flushing on
        // READY → NULL (a flushing bus drops posts, and a thread blocked
        // in `timed_pop` would otherwise outlive the session).
        if let Some(bus) = self.pipeline.bus() {
            let stop = gst::message::Application::new(gst::Structure::new_empty(BUS_WATCH_STOP));
            if let Err(e) = bus.post(stop) {
                debug!(error = %e, "bus-watch stop message not posted");
            }
        }
        if let Err(e) = self.pipeline.set_state(gst::State::Null) {
            debug!(error = %e, "webrtcbin pipeline → Null failed (ignored)");
        }
    }

    /// Build the pipeline, negotiate via `signaler`, and resolve once
    /// the first inbound H.264 RTP packet has been pushed into `sink`.
    /// The caller owns whatever drains `sink` (loopback today, an
    /// `appsrc` after Phase 6).
    ///
    /// `notifier` is the adapter half of the orchestrator's
    /// `LiveSession`; the death detectors described in the module docs
    /// fire it (first wins), during setup as well as once the session is
    /// up.
    ///
    /// Cancel-safe: dropping the future mid-setup (the multiplexer does
    /// when a detector fires first) stops the pipeline and releases the
    /// bus-watch thread.
    ///
    /// # Errors
    ///
    /// [`MediaError::Pipeline`] on GStreamer/element failure, or
    /// [`MediaError::SpliceTimeout`] if no RTP arrives in time.
    pub(crate) async fn start(
        camera: &CameraId,
        ice: &[IceServer],
        signaler: &dyn WebrtcSignaler,
        sinks: LiveSinks,
        cfg: &WebrtcConfig,
        notifier: LiveLossNotifier,
    ) -> Result<Self, MediaError> {
        let pipeline = gst::Pipeline::default();
        let mut live = Self::owning(pipeline.clone());
        let webrtcbin = make("webrtcbin")?;
        webrtcbin.set_property_from_str("bundle-policy", "none");
        webrtcbin.set_property("latency", 0u32);
        apply_ice_address_family(&webrtcbin, cfg.ice_address_family);
        apply_ice(&webrtcbin, ice);

        let audio_caps = rtp_caps("audio", "OPUS", OPUS_PT, 48000);
        let video_caps = rtp_caps("video", "H264", H264_PT, 90000);

        // m-line 0: audio Opus **sendrecv** (silence) — FreeSWITCH only
        // relays video once the SIP audio leg exists. Linking the send
        // chain to webrtcbin creates the (sendrecv) audio transceiver.
        let src = make("audiotestsrc")?;
        src.set_property_from_str("wave", "silence");
        src.set_property("is-live", true);
        let conv = make("audioconvert")?;
        let resample = make("audioresample")?;
        let opusenc = make("opusenc")?;
        let pay = make("rtpopuspay")?;
        pay.set_property("pt", u32::try_from(OPUS_PT).unwrap_or(111));
        let paycaps = make("capsfilter")?;
        paycaps.set_property("caps", &audio_caps);
        pipeline
            .add_many([&src, &conv, &resample, &opusenc, &pay, &paycaps, &webrtcbin])
            .map_err(|e| MediaError::Pipeline(format!("pipeline add: {e}")))?;
        gst::Element::link_many([&src, &conv, &resample, &opusenc, &pay, &paycaps])
            .map_err(|e| MediaError::Pipeline(format!("link audio chain: {e}")))?;
        // Request the send sink pad **with caps**. GStreamer 1.28
        // accepts a name-only `request_pad_simple("sink_%u")`, but
        // 1.22 (Debian 12) webrtcbin returns NULL unless the media caps
        // are supplied at request time — it needs them to create the
        // transceiver. Passing `audio_caps` is the portable path and is
        // harmless on 1.28; we fall back to the name-only request just
        // in case a build lacks the template lookup.
        let sink_templ = webrtcbin.pad_template("sink_%u").ok_or_else(|| {
            MediaError::Pipeline("webrtcbin has no 'sink_%u' pad template".into())
        })?;
        let wrb_sink = webrtcbin
            .request_pad(&sink_templ, None, Some(&audio_caps))
            .or_else(|| webrtcbin.request_pad_simple("sink_%u"))
            .ok_or_else(|| MediaError::Pipeline("webrtcbin has no sink request pad".into()))?;
        paycaps
            .static_pad("src")
            .ok_or_else(|| MediaError::Pipeline("capsfilter has no src pad".into()))?
            .link(&wrb_sink)
            .map_err(|e| MediaError::Pipeline(format!("link audio → webrtcbin: {e}")))?;

        // m-line 1: video recvonly H.264 (no send pad ⇒ explicit
        // transceiver). Order matters: audio link first, then this.
        let _vid_tr = webrtcbin.emit_by_name::<gst_webrtc::WebRTCRTPTransceiver>(
            "add-transceiver",
            &[
                &gst_webrtc::WebRTCRTPTransceiverDirection::Recvonly,
                &video_caps,
            ],
        );

        // Inbound video RTP → appsink → caller-supplied `LiveRtpSink`.
        // Audio pad is drained to `fakesink` (Opus bridging deferred
        // to Phase 8b — see `pipeline_desc` module-level note).
        let got_rtp = Arc::new(AtomicBool::new(false));
        let first_rtp = Arc::new(Notify::new());
        // Filled by `pad-added` once the video appsink is linked; the
        // keyframe pump targets this pad to send PLI/FIR upstream.
        let kf_pad: Arc<OnceLock<gst::glib::WeakRef<gst::Pad>>> = Arc::new(OnceLock::new());
        // Touched on every inbound video packet; read by the stall
        // watchdog spawned once the first packet has arrived.
        let activity = Arc::new(RtpActivity::new(Instant::now()));
        install_recv_branch(
            &pipeline,
            &webrtcbin,
            sinks,
            got_rtp.clone(),
            first_rtp.clone(),
            kf_pad.clone(),
            activity.clone(),
        );
        install_connection_watch(&webrtcbin, notifier.clone());

        // Negotiation: on-negotiation-needed → create-offer →
        // set-local-description; ship the full offer SDP once ICE
        // gathering completes (non-trickle — FreeSWITCH expects inline
        // candidates).
        let (offer_tx, mut offer_rx) = mpsc::unbounded_channel::<String>();
        install_negotiation(&webrtcbin, offer_tx);

        pipeline
            .set_state(gst::State::Playing)
            .map_err(|e| MediaError::Pipeline(format!("pipeline → Playing: {e}")))?;
        spawn_bus_watch(&pipeline, notifier.clone());

        exchange_sdp(camera, signaler, &webrtcbin, &mut offer_rx).await?;

        // Keyframe pump: periodic force-key-unit sent *upstream* into
        // the appsink sink pad (webrtcbin/rtpbin turns it into RTCP
        // PLI/FIR so Arlo emits a fresh IDR). Cheap insurance for fast
        // (re)start — and required: Arlo withholds video until PLI.
        live.tasks.push(spawn_keyframe_pump(kf_pad));

        // Resolve once inbound RTP is flowing (matches the port's
        // "attach resolves when the live branch produces frames").
        if !got_rtp.load(Ordering::Acquire) {
            tokio::time::timeout(
                Duration::from_secs(FIRST_RTP_TIMEOUT_SECS),
                first_rtp.notified(),
            )
            .await
            .map_err(|_| MediaError::SpliceTimeout {
                timeout_secs: FIRST_RTP_TIMEOUT_SECS,
            })?;
        }
        info!(%camera, "live webrtcbin ready; first RTP flowing");
        // Only now does silence mean anything: the watchdog counts from
        // the first packet, never from negotiation.
        live.tasks.push(spawn_stall_watchdog(
            activity,
            cfg.live_stall_timeout(),
            notifier,
        ));

        Ok(live)
    }
}

impl Drop for WebrtcLive {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Wait for the gathered local offer, carry it to Arlo through
/// `signaler`, and apply the answer to `webrtcbin`.
async fn exchange_sdp(
    camera: &CameraId,
    signaler: &dyn WebrtcSignaler,
    webrtcbin: &gst::Element,
    offer_rx: &mut mpsc::UnboundedReceiver<String>,
) -> Result<(), MediaError> {
    let offer_sdp = tokio::time::timeout(OFFER_TIMEOUT, offer_rx.recv())
        .await
        .map_err(|_| MediaError::Pipeline("timed out gathering local offer".into()))?
        .ok_or_else(|| MediaError::Pipeline("offer channel closed".into()))?;
    info!(%camera, bytes = offer_sdp.len(), "webrtcbin offer ready; negotiating with Arlo");

    let answer = signaler
        .negotiate(camera, offer_sdp)
        .await
        .map_err(|e| MediaError::Pipeline(format!("signaling negotiate: {e}")))?;

    let msg = gst_sdp::SDPMessage::parse_buffer(answer.answer_sdp.as_bytes())
        .map_err(|e| MediaError::Pipeline(format!("parse answer SDP: {e}")))?;
    let answer_desc =
        gst_webrtc::WebRTCSessionDescription::new(gst_webrtc::WebRTCSDPType::Answer, msg);
    webrtcbin.emit_by_name::<()>(
        "set-remote-description",
        &[&answer_desc, &gst::Promise::new()],
    );
    info!(%camera, session = %answer.session_id, "answer applied; awaiting first RTP");
    Ok(())
}

/// Make an element by factory name, mapping failure to [`MediaError`].
fn make(factory: &str) -> Result<gst::Element, MediaError> {
    gst::ElementFactory::make(factory)
        .build()
        .map_err(|e| MediaError::Pipeline(format!("make {factory}: {e}")))
}

fn rtp_caps(media: &str, encoding: &str, pt: i32, clock: i32) -> gst::Caps {
    gst::Caps::builder("application/x-rtp")
        .field("media", media)
        .field("encoding-name", encoding)
        .field("payload", pt)
        .field("clock-rate", clock)
        .build()
}

/// Restrict local ICE candidate gathering to IPv4 when
/// [`IceAddressFamily::Ipv4`] is configured. Passing an IPv4 wildcard
/// (`"0.0.0.0"`) to `GstWebRTCICE::add-local-ip-address` (action
/// signal, `GStreamer` 1.20+) tells libnice to bind on all IPv4
/// interfaces and skip IPv6 gathering entirely — useful on networks
/// where IPv6 to the Arlo gateway is broken or slow to fail over.
///
/// [`IceAddressFamily::Dual`] is a no-op — libnice's default gathers
/// both families.
fn apply_ice_address_family(webrtcbin: &gst::Element, family: IceAddressFamily) {
    match family {
        IceAddressFamily::Dual => {
            debug!("ICE: dual-stack candidate gathering (libnice default)");
        }
        IceAddressFamily::Ipv4 => {
            let ice: gst::glib::Object = webrtcbin.property("ice");
            let accepted: bool = ice.emit_by_name("add-local-ip-address", &[&"0.0.0.0"]);
            debug!(accepted, "ICE: IPv4-only (add-local-ip-address = 0.0.0.0)");
        }
    }
}

/// STUN via property, UDP TURN via `add-turn-server`. Domain
/// [`IceServer::url`] is `stun:host:port` / `turn:host:port?...`;
/// webrtcbin wants the `stun://` / `turn://user:pass@host` URI form.
fn apply_ice(webrtcbin: &gst::Element, ice: &[IceServer]) {
    for s in ice {
        if let Some(rest) = s.url.strip_prefix("stun:") {
            let uri = format!("stun://{rest}");
            webrtcbin.set_property("stun-server", &uri);
            debug!(%uri, "ICE: stun-server set");
        } else if let Some(rest) = s.url.strip_prefix("turn:") {
            let (Some(u), Some(c)) = (s.username.as_deref(), s.credential.as_deref()) else {
                continue;
            };
            let uri = format!("turn://{}:{}@{rest}", pct(u), pct(c));
            let ok: bool = webrtcbin.emit_by_name("add-turn-server", &[&uri]);
            debug!(host = %rest, accepted = ok, "ICE: UDP TURN added");
        }
    }
}

/// Percent-encode RFC 3986 userinfo so `turn://user:pass@host` parses
/// (Arlo creds contain `: / = +`).
fn pct(s: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
    }
    out
}

/// On the **video** src pad, forward raw RTP into `sinks.video`; on the
/// **audio** src pad (we offer audio sendrecv, so `FreeSWITCH` sends the
/// camera's Opus back), forward raw RTP into `sinks.audio` (Phase 8b).
/// Both go through an `appsink`; leaving either unlinked would cause
/// `GST_FLOW_NOT_LINKED` → "Internal data stream error" on that leg's
/// `nicesrc`. The persistent pipeline decodes each: H.264→I420 and
/// Opus→AAC.
#[allow(clippy::too_many_arguments)]
fn install_recv_branch(
    pipeline: &gst::Pipeline,
    webrtcbin: &gst::Element,
    sinks: LiveSinks,
    got_rtp: Arc<AtomicBool>,
    first_rtp: Arc<Notify>,
    kf_pad: Arc<OnceLock<gst::glib::WeakRef<gst::Pad>>>,
    activity: Arc<RtpActivity>,
) {
    let pipeline_w = pipeline.downgrade();
    webrtcbin.connect_pad_added(move |_wb, pad| {
        if pad.direction() != gst::PadDirection::Src {
            return;
        }
        let Some(pipeline) = pipeline_w.upgrade() else {
            return;
        };
        let is_video = pad
            .current_caps()
            .and_then(|c| c.structure(0).map(structure_is_video))
            .unwrap_or(false);
        if is_video {
            if let Err(e) = link_video_appsink(
                &pipeline,
                pad,
                sinks.video.clone(),
                got_rtp.clone(),
                first_rtp.clone(),
                &kf_pad,
                activity.clone(),
            ) {
                warn!(error = %e, "failed to attach video appsink");
            }
        } else {
            debug!("attaching audio recv leg (Opus RTP → audio sink)");
            if let Err(e) = link_audio_appsink(&pipeline, pad, sinks.audio.clone()) {
                warn!(error = %e, "failed to attach audio appsink");
            }
        }
    });
}

/// Forward the audio recv leg's Opus RTP into `sink` via an `appsink`
/// (Phase 8b). Mirrors [`link_video_appsink`] but with no keyframe pump
/// and no first-RTP gating — video drives the "live ready" signal;
/// audio is best-effort and joins whenever `FreeSWITCH` relays it.
fn link_audio_appsink(
    pipeline: &gst::Pipeline,
    pad: &gst::Pad,
    sink: LiveRtpSink,
) -> Result<(), MediaError> {
    let appsink = gst_app::AppSink::builder()
        .sync(false)
        .max_buffers(1)
        .drop(true)
        .build();
    appsink.set_callbacks(
        gst_app::AppSinkCallbacks::builder()
            .new_sample(move |appsink| {
                let sample = appsink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                let buf = sample.buffer().ok_or(gst::FlowError::Error)?;
                let map = buf.map_readable().map_err(|_| gst::FlowError::Error)?;
                let _ = sink.push(Bytes::copy_from_slice(map.as_slice()));
                Ok(gst::FlowSuccess::Ok)
            })
            .build(),
    );
    let appsink_el: gst::Element = appsink.upcast();
    pipeline
        .add(&appsink_el)
        .map_err(|e| MediaError::Pipeline(format!("add audio appsink: {e}")))?;
    appsink_el
        .sync_state_with_parent()
        .map_err(|e| MediaError::Pipeline(format!("audio appsink sync state: {e}")))?;
    let sink_pad = appsink_el
        .static_pad("sink")
        .ok_or_else(|| MediaError::Pipeline("audio appsink has no sink pad".into()))?;
    pad.link(&sink_pad)
        .map_err(|e| MediaError::Pipeline(format!("link webrtc audio → appsink: {e}")))?;
    debug!("audio appsink attached");
    Ok(())
}

fn structure_is_video(s: &gst::StructureRef) -> bool {
    s.get::<String>("media").is_ok_and(|m| m == "video")
        || s.get::<String>("encoding-name")
            .is_ok_and(|e| e.eq_ignore_ascii_case("H264"))
}

#[allow(clippy::too_many_arguments)]
fn link_video_appsink(
    pipeline: &gst::Pipeline,
    pad: &gst::Pad,
    sink: LiveRtpSink,
    got_rtp: Arc<AtomicBool>,
    first_rtp: Arc<Notify>,
    kf_pad: &OnceLock<gst::glib::WeakRef<gst::Pad>>,
    activity: Arc<RtpActivity>,
) -> Result<(), MediaError> {
    let appsink = gst_app::AppSink::builder()
        .sync(false)
        .max_buffers(1)
        .drop(true)
        .build();
    appsink.set_callbacks(
        gst_app::AppSinkCallbacks::builder()
            .new_sample(move |appsink| {
                let sample = appsink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                let buf = sample.buffer().ok_or(gst::FlowError::Error)?;
                let map = buf.map_readable().map_err(|_| gst::FlowError::Error)?;
                let bytes = Bytes::copy_from_slice(map.as_slice());
                activity.touch(Instant::now());
                if !got_rtp.swap(true, Ordering::AcqRel) {
                    first_rtp.notify_one();
                }
                // Drop rather than back-pressure the streaming thread;
                // the keyframe pump recovers playback if a burst is lost.
                let _ = sink.push(bytes);
                Ok(gst::FlowSuccess::Ok)
            })
            .build(),
    );
    let appsink_el: gst::Element = appsink.upcast();
    pipeline
        .add(&appsink_el)
        .map_err(|e| MediaError::Pipeline(format!("add appsink: {e}")))?;
    appsink_el
        .sync_state_with_parent()
        .map_err(|e| MediaError::Pipeline(format!("appsink sync state: {e}")))?;
    let sink_pad = appsink_el
        .static_pad("sink")
        .ok_or_else(|| MediaError::Pipeline("appsink has no sink pad".into()))?;
    pad.link(&sink_pad)
        .map_err(|e| MediaError::Pipeline(format!("link webrtc video → appsink: {e}")))?;
    // Keyframe pump sends force-key-unit *upstream* via this webrtcbin
    // src pad (`send_event` on a src pad routes upstream events into the
    // element → rtpbin → RTCP PLI).
    let _ = kf_pad.set(pad.downgrade());
    debug!("video appsink attached");
    Ok(())
}

/// Wire `on-negotiation-needed` → create-offer → set-local-description,
/// then ship the full SDP once ICE gathering completes.
fn install_negotiation(webrtcbin: &gst::Element, offer_tx: mpsc::UnboundedSender<String>) {
    let wb = webrtcbin.clone();
    webrtcbin.connect("on-negotiation-needed", false, move |_| {
        let wb2 = wb.clone();
        let promise = gst::Promise::with_change_func(move |reply| {
            let Ok(Some(reply)) = reply else {
                warn!("create-offer failed");
                return;
            };
            let Ok(offer) = reply.get::<gst_webrtc::WebRTCSessionDescription>("offer") else {
                warn!("create-offer reply missing offer");
                return;
            };
            wb2.emit_by_name::<()>("set-local-description", &[&offer, &gst::Promise::new()]);
        });
        wb.emit_by_name::<()>("create-offer", &[&None::<gst::Structure>, &promise]);
        None
    });

    let sent = Arc::new(AtomicBool::new(false));
    webrtcbin.connect_notify(Some("ice-gathering-state"), move |wb, _| {
        let st = wb.property::<gst_webrtc::WebRTCICEGatheringState>("ice-gathering-state");
        if st == gst_webrtc::WebRTCICEGatheringState::Complete && !sent.swap(true, Ordering::AcqRel)
        {
            let desc = wb.property::<gst_webrtc::WebRTCSessionDescription>("local-description");
            match desc.sdp().as_text() {
                Ok(text) => {
                    let _ = offer_tx.send(text.as_str().to_owned());
                }
                Err(e) => warn!(error = %e, "local-description SDP not stringifiable"),
            }
        }
    });
}

/// Periodic `GstForceKeyUnit` sent via `send_event` on the **webrtcbin
/// video src pad**. `gst_pad_send_event` routes an upstream event from a
/// *src* pad into the element (→ rtpbin), which emits RTCP PLI/FIR so
/// Arlo pushes a fresh IDR. (`push_event` on a src pad, or `send_event`
/// on the appsink *sink* pad, are both "wrong direction".) `Weak` so
/// this never keeps the pipeline alive; aborted on shutdown.
fn spawn_keyframe_pump(
    kf_pad: Arc<OnceLock<gst::glib::WeakRef<gst::Pad>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(KEYFRAME_INTERVAL);
        loop {
            ticker.tick().await;
            let Some(src_pad) = kf_pad.get().and_then(gst::glib::WeakRef::upgrade) else {
                continue;
            };
            let ev = gst::event::CustomUpstream::new(
                gst::Structure::builder("GstForceKeyUnit")
                    .field("all-headers", true)
                    .build(),
            );
            let _ = src_pad.send_event(ev);
        }
    })
}

/// Drain the pipeline bus; surface ERROR/EOS in the log and to the
/// live-loss notifier. The thread ends at EOS or on the
/// [`BUS_WATCH_STOP`] application message that `shutdown` posts.
fn spawn_bus_watch(pipeline: &gst::Pipeline, notifier: LiveLossNotifier) {
    let Some(bus) = pipeline.bus() else { return };
    std::thread::spawn(move || {
        for msg in bus.iter_timed(gst::ClockTime::NONE) {
            use gst::MessageView as V;
            match msg.view() {
                V::Error(e) => {
                    warn!(
                        src = ?e.src().map(gst::prelude::GstObjectExt::path_string),
                        error = %e.error(),
                        "webrtcbin pipeline error"
                    );
                    report_loss(&notifier, LiveLossReason::PipelineError);
                }
                V::Eos(_) => {
                    debug!("webrtcbin pipeline EOS");
                    report_loss(&notifier, LiveLossReason::EndOfStream);
                    break;
                }
                V::Application(app)
                    if app.structure().is_some_and(|s| s.name() == BUS_WATCH_STOP) =>
                {
                    break;
                }
                _ => {}
            }
        }
        debug!("webrtcbin bus watch exited");
    });
}

/// Watch `webrtcbin`'s peer-connection and ICE state; a terminal state
/// on either is a lost source. `disconnected` is only logged: the
/// WebRTC state machine allows recovery, and a persistent one is caught
/// by the stall watchdog within the configured timeout.
fn install_connection_watch(webrtcbin: &gst::Element, notifier: LiveLossNotifier) {
    let n = notifier.clone();
    webrtcbin.connect_notify(Some("connection-state"), move |wb, _| {
        use gst_webrtc::WebRTCPeerConnectionState as S;
        match wb.property::<S>("connection-state") {
            S::Failed | S::Closed => report_loss(&n, LiveLossReason::PeerDisconnected),
            S::Disconnected => debug!("peer connection disconnected (may recover)"),
            _ => {}
        }
    });
    webrtcbin.connect_notify(Some("ice-connection-state"), move |wb, _| {
        use gst_webrtc::WebRTCICEConnectionState as S;
        match wb.property::<S>("ice-connection-state") {
            S::Failed | S::Closed => report_loss(&notifier, LiveLossReason::PeerDisconnected),
            S::Disconnected => debug!("ICE disconnected (may recover)"),
            _ => {}
        }
    });
}

/// Stall watchdog: declares the source lost once no video RTP has
/// arrived for `timeout`. Sleeps exactly until the earliest instant the
/// verdict could change, so a healthy 30 fps source costs one wake-up
/// per `timeout`. Aborted by `WebrtcLive::shutdown`.
fn spawn_stall_watchdog(
    activity: Arc<RtpActivity>,
    timeout: Duration,
    notifier: LiveLossNotifier,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let silent = activity.silent_for(Instant::now());
            if let Some(reason) = stall_verdict(silent, timeout) {
                warn!(
                    silent_ms = silent.as_millis(),
                    "no inbound video RTP; live source stalled"
                );
                report_loss(&notifier, reason);
                break;
            }
            tokio::time::sleep(timeout.saturating_sub(silent)).await;
        }
    })
}

/// Deliver a loss report; a `false` return means another detector won
/// or the orchestrator already left live — worth a debug line on the
/// Frigate box to see which detectors agree, nothing more.
fn report_loss(notifier: &LiveLossNotifier, reason: LiveLossReason) {
    if !notifier.notify(reason) {
        debug!(
            reason = reason.as_label(),
            "live-loss report not delivered (already reported or session over)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `pct` is pure; the rest of this module needs a live GStreamer +
    // camera (exercised by the Phase 4 manual gate, like
    // `gst_pipeline.rs` / `rtsp.rs` — excluded from unit coverage).
    #[test]
    fn pct_encodes_reserved_userinfo() {
        assert_eq!(pct("ab-_.~"), "ab-_.~");
        assert_eq!(pct("a:b/c=d+e"), "a%3Ab%2Fc%3Dd%2Be");
    }
}
