//! [`WebrtcSignaler`] implementation backed by arlo-rs's signaling
//! primitives (`sip_info` + `webrtc_negotiate` + `SignalingSocket`).
//!
//! v3 Arlo live is a WebRTC call brokered by a `FreeSWITCH` gateway. The
//! media adapter (GStreamer `webrtcbin`, in `streamer-infra-media`)
//! owns the peer connection and **generates the SDP offer**. One
//! [`negotiate`](WebrtcSignaler::negotiate) call:
//!
//! 1. `sip_info` (REST) for the call's SIP/ICE coordinates — Arlo error
//!    14001 here means the user is streaming the camera in the app;
//! 2. the media adapter's [`OfferBuilder`] builds the offer with the
//!    usable ICE servers ([`crate::ice`]);
//! 3. `webrtc_negotiate` (`POST /hmswebsocketproxy/initiateOffer` over
//!    the signaling WS) with the same coordinates → the gateway's answer.
//!
//! The coordinates live only for that call. The open [`SignalingSocket`]
//! is **owned here** per camera until
//! [`teardown`](WebrtcSignaler::teardown), which sends
//! `sessionDisconnected` so a camera is never left streaming (battery).

use std::collections::HashMap;
use std::sync::Arc;

use arlo_rs::client::ArloClient;
use arlo_rs::client::livestream::SignalingSocket;
use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::debug;

use streamer_domain::camera::CameraId;
use streamer_domain::error::DomainError;
use streamer_domain::port::{OfferBuilder, WebrtcSignaler};
use streamer_domain::stream::SignalingAnswer;

use crate::device_registry::DeviceRegistry;
use crate::error::arlo_to_domain;
use crate::ice::usable_ice_servers;

/// Adapter exposing arlo-rs's WebRTC signaling as the domain
/// [`WebrtcSignaler`] port, owning the per-camera signaling socket.
pub struct ArloWebrtcSignalerAdapter {
    client: Arc<ArloClient>,
    devices: Arc<DeviceRegistry>,
    /// Open signaling WS per camera id. `disconnect()` sends
    /// `sessionDisconnected` and closes the socket.
    sessions: Mutex<HashMap<String, SignalingSocket>>,
}

impl ArloWebrtcSignalerAdapter {
    /// Construct from a shared, authenticated [`ArloClient`] handle and
    /// the shared [`DeviceRegistry`] used to resolve camera ids.
    #[must_use]
    pub fn new(client: Arc<ArloClient>, devices: Arc<DeviceRegistry>) -> Self {
        Self {
            client,
            devices,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Remove a camera's signaling socket under the lock, then
    /// `disconnect()` it *outside* the lock (disconnect awaits a WS
    /// round-trip — must not block other cameras' negotiate/teardown).
    /// Pass `e` on, forgetting `camera`'s cached device first unless the
    /// camera is merely busy: a stale device (re-paired to another base
    /// station) fails here, and the next attempt refetches it.
    fn failed_for(&self, camera: &CameraId, e: DomainError) -> DomainError {
        if !matches!(e, DomainError::CameraBusy(_)) {
            self.devices.invalidate(camera);
        }
        e
    }

    async fn take_and_disconnect(&self, key: &str) {
        let sock = self.sessions.lock().await.remove(key);
        if let Some(s) = sock {
            s.disconnect().await;
        }
    }
}

#[async_trait]
impl WebrtcSignaler for ArloWebrtcSignalerAdapter {
    async fn negotiate(
        &self,
        camera: &CameraId,
        offer: &mut dyn OfferBuilder,
    ) -> Result<SignalingAnswer, DomainError> {
        // Idempotent: drop any stale session for this camera first.
        self.take_and_disconnect(camera.as_str()).await;
        let device = self.devices.resolve(camera).await?;
        let sip = self
            .client
            .sip_info(&device)
            .await
            .map_err(|e| self.failed_for(camera, arlo_to_domain(e)))?;
        let offer_sdp = offer
            .build_offer(&usable_ice_servers(&sip.ice_servers))
            .await?;
        let (answer, socket) = self
            .client
            .webrtc_negotiate(&sip, &offer_sdp)
            .await
            .map_err(arlo_to_domain)?;
        self.sessions
            .lock()
            .await
            .insert(camera.as_str().to_string(), socket);
        debug!(
            %camera,
            session_tail = %id_tail(&answer.session_id),
            "WebRTC signaling negotiated"
        );
        Ok(SignalingAnswer {
            answer_sdp: answer.answer_sdp,
            session_id: answer.session_id,
        })
    }

    async fn teardown(&self, camera: &CameraId) -> Result<(), DomainError> {
        self.take_and_disconnect(camera.as_str()).await;
        debug!(%camera, "WebRTC signaling session torn down");
        Ok(())
    }
}

/// The last four characters of a session id: enough to correlate log
/// lines, not enough to reuse the handle. Shorter ids are returned whole.
fn id_tail(id: &str) -> &str {
    let cut = id.char_indices().rev().nth(3).map_or(0, |(i, _)| i);
    &id[cut..]
}

#[cfg(test)]
mod tests {
    use super::id_tail;

    #[test]
    fn id_tail_keeps_four_characters_or_the_whole_short_id() {
        assert_eq!(id_tail("abcdef1234"), "1234");
        assert_eq!(id_tail("abc"), "abc");
        assert_eq!(id_tail(""), "");
        assert_eq!(id_tail("héllo"), "éllo");
    }
}
