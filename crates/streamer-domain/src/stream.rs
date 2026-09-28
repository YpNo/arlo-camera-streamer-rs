//! Live-stream source descriptors.
//!
//! [`SignalingAnswer`] is the result of a WebRTC offer/answer exchange
//! with the Arlo gateway, returned by
//! [`WebrtcSignaler::negotiate`](crate::port::WebrtcSignaler::negotiate).
//! The media adapter (GStreamer `webrtcbin`) generates the offer, hands
//! it to the signaler, and applies the returned answer SDP.
//!
//! [`Codec`] names the video codec a camera produces; the per-camera
//! `codec_hint` in the configuration lets the pipeline skip the
//! first-stream detection. [`IceAddressFamily`] is the ICE gathering
//! policy applied to every `webrtcbin`.
//!
//! [`LiveSession`] / [`LiveLossNotifier`] are the two halves of the
//! live-loss feedback path (ADR 0004): `attach_live` hands the
//! orchestrator a session handle, the media adapter keeps the notifier,
//! and the first death signal to fire flips the camera back to idle.

use std::sync::{Arc, Mutex, PoisonError};

use futures::channel::oneshot;
use serde::{Deserialize, Serialize};

use crate::state::LiveLossReason;

/// One ICE (STUN/TURN) server the media adapter must configure on its
/// WebRTC peer **before** generating the offer. Sourced from Arlo's
/// `sipInfo` and handed to the media adapter's
/// [`OfferBuilder`](crate::port::OfferBuilder) during
/// [`WebrtcSignaler::negotiate`](crate::port::WebrtcSignaler::negotiate).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IceServer {
    /// e.g. `stun:host:port` or `turn:host:port?transport=udp`.
    pub url: String,
    /// TURN long-term username (`None` for STUN).
    pub username: Option<String>,
    /// TURN long-term credential (`None` for STUN).
    pub credential: Option<String>,
}

/// The Arlo gateway's WebRTC SDP answer to our offer, plus the session
/// id it echoed back (needed for teardown / `sessionDisconnected`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalingAnswer {
    /// `FreeSWITCH`'s SDP answer, applied verbatim by the media adapter.
    pub answer_sdp: String,
    /// Opaque session id the gateway assigned to this live call.
    pub session_id: String,
}

/// Video codec emitted by an Arlo camera.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    /// H.264 / AVC (most older Arlo models).
    H264,
    /// H.265 / HEVC (newer Arlo Pro / Ultra models).
    H265,
}

/// Address-family policy the media adapter applies when configuring the
/// WebRTC ICE agent's local candidate gathering.
///
/// The default is [`Dual`](Self::Dual) — let libnice gather both IPv4
/// and IPv6 candidates. Some networks (broken IPv6 routing to the Arlo
/// gateway, restrictive corporate firewalls) benefit from
/// [`Ipv4`](Self::Ipv4), which restricts gathering to IPv4 only.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IceAddressFamily {
    /// Gather both IPv4 and IPv6 candidates (libnice default).
    #[default]
    Dual,
    /// Gather only IPv4 candidates. Recommended when IPv6 to the Arlo
    /// gateway is broken or slow to fail over.
    Ipv4,
}

/// Handle to one attached live session, returned by
/// [`MediaMultiplexer::attach_live`](crate::port::MediaMultiplexer::attach_live).
///
/// Owning it is the orchestrator's proof that the session is current.
/// Dropping it makes every later report from that session
/// unobservable, which is what makes a stale notifier from a previous
/// session harmless: there are no generation counters, the handle *is*
/// the identity.
pub struct LiveSession {
    lost: oneshot::Receiver<LiveLossReason>,
}

impl LiveSession {
    /// Mint a session handle and its adapter-side notifier.
    #[must_use]
    pub fn new() -> (Self, LiveLossNotifier) {
        let (tx, rx) = oneshot::channel();
        (
            Self { lost: rx },
            LiveLossNotifier {
                tx: Arc::new(Mutex::new(Some(tx))),
            },
        )
    }

    /// Resolve once the adapter reports the source lost.
    ///
    /// Resolves at most once with a real reason; drop the handle
    /// afterwards, because a resolved handle resolves again immediately
    /// with [`LiveLossReason::AdapterDropped`]. That is also what a
    /// notifier dropped without a report yields — fail closed, the
    /// camera never stays live on a vanished session.
    ///
    /// Cancel-safe: a report that arrives during a cancelled poll is
    /// kept for the next one.
    pub async fn lost(&mut self) -> LiveLossReason {
        (&mut self.lost)
            .await
            .unwrap_or(LiveLossReason::AdapterDropped)
    }
}

impl std::fmt::Debug for LiveSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveSession").finish_non_exhaustive()
    }
}

/// Adapter-side half of a [`LiveSession`]. Cloneable so every death
/// detector (stall watchdog, bus watcher, connection-state callback)
/// can hold one; the first [`notify`](Self::notify) wins.
#[derive(Clone)]
pub struct LiveLossNotifier {
    tx: Arc<Mutex<Option<oneshot::Sender<LiveLossReason>>>>,
}

impl LiveLossNotifier {
    /// Report the loss. Returns `false` when a report was already
    /// delivered or the session handle is gone (the orchestrator left
    /// `Live` on its own); callers log and move on.
    pub fn notify(&self, reason: LiveLossReason) -> bool {
        let sender = self
            .tx
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        match sender {
            Some(tx) => tx.send(reason).is_ok(),
            None => false,
        }
    }
}

impl std::fmt::Debug for LiveLossNotifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let armed = self
            .tx
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some();
        f.debug_struct("LiveLossNotifier")
            .field("armed", &armed)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    #[test]
    fn live_session_notify_resolves_lost_with_reason() {
        let (mut session, notifier) = LiveSession::new();
        assert!(notifier.notify(LiveLossReason::RtpStalled));
        assert_eq!(block_on(session.lost()), LiveLossReason::RtpStalled);
    }

    #[test]
    fn live_session_second_notify_is_noop_and_returns_false() {
        let (mut session, notifier) = LiveSession::new();
        let twin = notifier.clone();
        assert!(notifier.notify(LiveLossReason::PipelineError));
        assert!(!twin.notify(LiveLossReason::EndOfStream));
        assert_eq!(block_on(session.lost()), LiveLossReason::PipelineError);
    }

    #[test]
    fn live_session_dropped_notifier_resolves_adapter_dropped() {
        let (mut session, notifier) = LiveSession::new();
        drop(notifier);
        assert_eq!(block_on(session.lost()), LiveLossReason::AdapterDropped);
    }

    #[test]
    fn live_session_notify_after_handle_dropped_returns_false() {
        let (session, notifier) = LiveSession::new();
        drop(session);
        assert!(!notifier.notify(LiveLossReason::PeerDisconnected));
    }

    #[test]
    fn live_session_resolved_handle_resolves_again_as_adapter_dropped() {
        // Documents why the orchestrator must drop the handle after the
        // first resolution instead of polling it again.
        let (mut session, notifier) = LiveSession::new();
        notifier.notify(LiveLossReason::RtpStalled);
        let _ = block_on(session.lost());
        assert_eq!(block_on(session.lost()), LiveLossReason::AdapterDropped);
    }

    #[test]
    fn notifier_debug_reports_armed_state_without_leaking_channel() {
        let (_session, notifier) = LiveSession::new();
        assert!(format!("{notifier:?}").contains("armed: true"));
        notifier.notify(LiveLossReason::RtpStalled);
        assert!(format!("{notifier:?}").contains("armed: false"));
    }
}
