//! A local stand-in for Arlo's `FreeSWITCH` gateway.
//!
//! [`FakeGateway`] implements [`WebrtcSignaler`] by answering our offer
//! with a second `webrtcbin` in the same process: non-bundled, audio
//! sendrecv (it drains our silent Opus), video sent from a live
//! `videotestsrc` at the pinned H.264 payload type. ICE runs over host
//! candidates only, so no STUN, TURN or network access is needed.
//!
//! The video leg passes through a `valve`; [`FakeGateway::stall_video`]
//! closes it to simulate a camera that stops sending while the call
//! stays up. [`FakeGateway::hanging_up`] builds one that answers and
//! then drops the call, so our ICE checks fail during setup;
//! [`FakeGateway::busy`] refuses the call with `CameraBusy`.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_sdp as gst_sdp;
use gstreamer_webrtc as gst_webrtc;
use tokio::sync::oneshot;

use streamer_domain::camera::CameraId;
use streamer_domain::error::DomainError;
use streamer_domain::port::{OfferBuilder, WebrtcSignaler};
use streamer_domain::stream::SignalingAnswer;

/// Upper bound for each signaling step (promise reply, ICE gathering).
const STEP_TIMEOUT: Duration = Duration::from_secs(15);

/// White frames, so a probe can tell live from the black idle screen.
const GATEWAY_LAUNCH: &str = "\
    webrtcbin name=gw bundle-policy=none latency=0 \
    videotestsrc is-live=true pattern=white \
      ! video/x-raw,width=320,height=240,framerate=15/1 ! videoconvert \
      ! x264enc tune=zerolatency speed-preset=ultrafast key-int-max=15 \
      ! video/x-h264,profile=constrained-baseline \
      ! rtph264pay pt=103 config-interval=-1 aggregate-mode=zero-latency \
      ! valve name=video_valve drop=false \
      ! application/x-rtp,media=video,encoding-name=H264,payload=103,clock-rate=90000 \
      ! gw. \
    audiotestsrc is-live=true wave=silence ! audioconvert ! audioresample \
      ! opusenc ! rtpopuspay pt=111 \
      ! application/x-rtp,media=audio,encoding-name=OPUS,payload=111,clock-rate=48000 \
      ! gw.";

struct Call {
    pipeline: gst::Pipeline,
    valve: gst::Element,
}

impl Drop for Call {
    fn drop(&mut self) {
        let _ = self.pipeline.set_state(gst::State::Null);
    }
}

/// See the module docs.
#[derive(Default)]
pub struct FakeGateway {
    call: Mutex<Option<Call>>,
    negotiations: Mutex<u32>,
    hang_up_after_answer: bool,
    busy: bool,
}

impl FakeGateway {
    /// A gateway that answers the offer and hangs up at once.
    pub fn hanging_up() -> Self {
        Self {
            hang_up_after_answer: true,
            ..Self::default()
        }
    }

    /// A gateway that refuses the call as Arlo does while the user
    /// streams the camera in the app (error 14001).
    pub fn busy() -> Self {
        Self {
            busy: true,
            ..Self::default()
        }
    }

    /// Stop sending video RTP; the call itself stays connected.
    pub fn stall_video(&self) {
        if let Some(call) = self.call.lock().expect("call lock").as_ref() {
            call.valve.set_property("drop", true);
        }
    }

    /// How many offers were answered.
    pub fn negotiations(&self) -> u32 {
        *self.negotiations.lock().expect("negotiations lock")
    }
}

#[async_trait]
impl WebrtcSignaler for FakeGateway {
    async fn negotiate(
        &self,
        _camera: &CameraId,
        offer: &mut dyn OfferBuilder,
    ) -> Result<SignalingAnswer, DomainError> {
        if self.busy {
            return Err(DomainError::CameraBusy(
                "RTSP Streaming in progress".to_string(),
            ));
        }
        // Host candidates only: no STUN or TURN to hand over.
        let offer_sdp = offer.build_offer(&[]).await?;
        let (call, answer_sdp) = answer(&offer_sdp)
            .await
            .map_err(DomainError::AdapterTransport)?;
        if self.hang_up_after_answer {
            drop(call);
        } else {
            *self.call.lock().expect("call lock") = Some(call);
        }
        *self.negotiations.lock().expect("negotiations lock") += 1;
        Ok(SignalingAnswer {
            answer_sdp,
            session_id: "fake-gateway-session".to_string(),
        })
    }

    async fn teardown(&self, _camera: &CameraId) -> Result<(), DomainError> {
        self.call.lock().expect("call lock").take();
        Ok(())
    }
}

/// Build the gateway pipeline, apply `offer_sdp`, and return the call
/// with its complete (non-trickle) answer SDP.
async fn answer(offer_sdp: &str) -> Result<(Call, String), String> {
    let pipeline = gst::parse::launch(GATEWAY_LAUNCH)
        .map_err(|e| format!("gateway launch: {e}"))?
        .downcast::<gst::Pipeline>()
        .map_err(|_| "gateway launch is not a pipeline".to_string())?;
    let webrtcbin = pipeline.by_name("gw").ok_or("no gw element")?;
    let valve = pipeline
        .by_name("video_valve")
        .ok_or("no video_valve element")?;
    drain_incoming(&pipeline, &webrtcbin);
    let gathered = gathering_complete(&webrtcbin);
    let call = Call { pipeline, valve };
    call.pipeline
        .set_state(gst::State::Playing)
        .map_err(|e| format!("gateway → Playing: {e}"))?;

    let sdp = gst_sdp::SDPMessage::parse_buffer(offer_sdp.as_bytes())
        .map_err(|e| format!("parse offer: {e}"))?;
    let offer = gst_webrtc::WebRTCSessionDescription::new(gst_webrtc::WebRTCSDPType::Offer, sdp);
    promise_reply(&webrtcbin, "set-remote-description", &offer).await?;

    let reply = promise_reply_with(&webrtcbin, "create-answer").await?;
    let answer = reply
        .get::<gst_webrtc::WebRTCSessionDescription>("answer")
        .map_err(|e| format!("create-answer reply: {e}"))?;
    promise_reply(&webrtcbin, "set-local-description", &answer).await?;

    tokio::time::timeout(STEP_TIMEOUT, gathered)
        .await
        .map_err(|_| "gateway ICE gathering timed out".to_string())?
        .map_err(|_| "gateway dropped before gathering completed".to_string())?;
    let local = webrtcbin.property::<gst_webrtc::WebRTCSessionDescription>("local-description");
    let text = local
        .sdp()
        .as_text()
        .map_err(|e| format!("answer SDP text: {e}"))?;
    Ok((call, text.clone()))
}

/// Our side sends silent Opus on the audio m-line; the gateway must
/// sink it or `nicesrc` fails with not-linked.
fn drain_incoming(pipeline: &gst::Pipeline, webrtcbin: &gst::Element) {
    let pipeline = pipeline.downgrade();
    webrtcbin.connect_pad_added(move |_, pad| {
        if pad.direction() != gst::PadDirection::Src {
            return;
        }
        let Some(pipeline) = pipeline.upgrade() else {
            return;
        };
        let Ok(sink) = gst::ElementFactory::make("fakesink")
            .property("sync", false)
            .build()
        else {
            return;
        };
        if pipeline.add(&sink).is_ok()
            && sink.sync_state_with_parent().is_ok()
            && let Some(sink_pad) = sink.static_pad("sink")
        {
            let _ = pad.link(&sink_pad);
        }
    });
}

/// Resolves once `webrtcbin` reports ICE gathering complete.
fn gathering_complete(webrtcbin: &gst::Element) -> oneshot::Receiver<()> {
    let (tx, rx) = oneshot::channel();
    let tx = Mutex::new(Some(tx));
    webrtcbin.connect_notify(Some("ice-gathering-state"), move |wb, _| {
        let state = wb.property::<gst_webrtc::WebRTCICEGatheringState>("ice-gathering-state");
        if state == gst_webrtc::WebRTCICEGatheringState::Complete
            && let Some(tx) = tx.lock().expect("gathering lock").take()
        {
            let _ = tx.send(());
        }
    });
    rx
}

/// Emit `signal(description, promise)` and wait for the promise.
async fn promise_reply(
    webrtcbin: &gst::Element,
    signal: &str,
    description: &gst_webrtc::WebRTCSessionDescription,
) -> Result<(), String> {
    let (tx, rx) = oneshot::channel();
    let promise = gst::Promise::with_change_func(move |_| {
        let _ = tx.send(());
    });
    webrtcbin.emit_by_name::<()>(signal, &[description, &promise]);
    tokio::time::timeout(STEP_TIMEOUT, rx)
        .await
        .map_err(|_| format!("{signal} timed out"))?
        .map_err(|_| format!("{signal} promise dropped"))
}

/// Emit `signal(None, promise)` and return the promise's reply.
async fn promise_reply_with(
    webrtcbin: &gst::Element,
    signal: &str,
) -> Result<gst::Structure, String> {
    let (tx, rx) = oneshot::channel();
    let promise = gst::Promise::with_change_func(move |reply| {
        let _ = tx.send(reply.ok().flatten().map(ToOwned::to_owned));
    });
    webrtcbin.emit_by_name::<()>(signal, &[&None::<gst::Structure>, &promise]);
    tokio::time::timeout(STEP_TIMEOUT, rx)
        .await
        .map_err(|_| format!("{signal} timed out"))?
        .map_err(|_| format!("{signal} promise dropped"))?
        .ok_or_else(|| format!("{signal} returned no reply"))
}
