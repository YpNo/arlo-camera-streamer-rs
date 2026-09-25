//! [`WebrtcSignaler`] implementation backed by arlo-rs's signaling
//! primitives (`sip_info` + `webrtc_negotiate` + `SignalingSocket`).
//!
//! v3 Arlo live is a WebRTC call brokered by a `FreeSWITCH` gateway. The
//! media adapter (GStreamer `webrtcbin`, in `streamer-infra-media`)
//! owns the peer connection and **generates the SDP offer**. This
//! adapter carries that offer to Arlo:
//!
//! 1. [`ice_servers`](WebrtcSignaler::ice_servers) — `sip_info` (REST)
//!    for the per-call SIP/ICE coordinates. The [`SipInfo`] is cached
//!    per camera so the immediately-following `negotiate` reuses it.
//! 2. [`negotiate`](WebrtcSignaler::negotiate) — `webrtc_negotiate`
//!    (`POST /hmswebsocketproxy/initiateOffer` over the signaling WS)
//!    → the gateway's SDP answer.
//!
//! The open [`SignalingSocket`] is **owned here** per camera until
//! [`teardown`](WebrtcSignaler::teardown), which sends
//! `sessionDisconnected` so a camera is never left streaming (battery).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use arlo_rs::client::ArloClient;
use arlo_rs::client::livestream::SignalingSocket;
use arlo_rs::models::sip::SipInfo;
use tokio::sync::Mutex;
use tracing::debug;

use streamer_domain::camera::CameraId;
use streamer_domain::error::DomainError;
use streamer_domain::port::WebrtcSignaler;
use streamer_domain::stream::{IceServer, SignalingAnswer};

use crate::device_registry::DeviceRegistry;
use crate::error::arlo_to_domain;

/// Adapter exposing arlo-rs's WebRTC signaling as the domain
/// [`WebrtcSignaler`] port, owning the per-camera signaling socket.
pub struct ArloWebrtcSignalerAdapter {
    client: Arc<ArloClient>,
    devices: Arc<DeviceRegistry>,
    /// `SipInfo` from the most recent `ice_servers` call, reused by the
    /// matching `negotiate` (avoids a second `sipInfo` round-trip).
    sip_cache: Mutex<HashMap<String, SipInfo>>,
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
            sip_cache: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Remove a camera's signaling socket under the lock, then
    /// `disconnect()` it *outside* the lock (disconnect awaits a WS
    /// round-trip — must not block other cameras' negotiate/teardown).
    async fn take_and_disconnect(&self, key: &str) {
        let sock = self.sessions.lock().await.remove(key);
        if let Some(s) = sock {
            s.disconnect().await;
        }
    }

    /// Fetch `sipInfo` for `camera` and cache it for the matching
    /// `negotiate`.
    async fn fetch_and_cache_sip(&self, camera: &CameraId) -> Result<SipInfo, DomainError> {
        let device = self.devices.resolve(camera).await?;
        let sip = self
            .client
            .sip_info(&device)
            .await
            .map_err(arlo_to_domain)?;
        self.sip_cache
            .lock()
            .await
            .insert(camera.as_str().to_string(), sip.clone());
        Ok(sip)
    }
}

#[async_trait]
impl WebrtcSignaler for ArloWebrtcSignalerAdapter {
    async fn ice_servers(&self, camera: &CameraId) -> Result<Vec<IceServer>, DomainError> {
        let sip = self.fetch_and_cache_sip(camera).await?;
        // webrtc-ice/libnice + Arlo only ever use the UDP TURN; the
        // `transport=tcp` TURN is unusable (proven live) — drop it.
        let servers = sip
            .ice_servers
            .data
            .iter()
            .filter(|s| {
                !(s.kind.eq_ignore_ascii_case("turn") && s.transport.as_deref() == Some("tcp"))
            })
            .map(|s| {
                let is_turn = s.kind.eq_ignore_ascii_case("turn");
                IceServer {
                    url: s.url(),
                    username: if is_turn { s.username.clone() } else { None },
                    credential: if is_turn { s.credential.clone() } else { None },
                }
            })
            .collect();
        Ok(servers)
    }

    async fn negotiate(
        &self,
        camera: &CameraId,
        offer_sdp: String,
    ) -> Result<SignalingAnswer, DomainError> {
        // Idempotent: drop any stale session for this camera first.
        self.take_and_disconnect(camera.as_str()).await;

        // Reuse the `SipInfo` cached by the preceding `ice_servers`
        // call; re-fetch if the caller skipped it (order-independent).
        let cached = self.sip_cache.lock().await.remove(camera.as_str());
        let sip = match cached {
            Some(s) => s,
            None => self.fetch_and_cache_sip(camera).await?,
        };

        let (answer, socket) = self
            .client
            .webrtc_negotiate(&sip, &offer_sdp)
            .await
            .map_err(arlo_to_domain)?;
        self.sessions
            .lock()
            .await
            .insert(camera.as_str().to_string(), socket);
        debug!(%camera, session = %answer.session_id, "WebRTC signaling negotiated");
        Ok(SignalingAnswer {
            answer_sdp: answer.answer_sdp,
            session_id: answer.session_id,
        })
    }

    async fn teardown(&self, camera: &CameraId) -> Result<(), DomainError> {
        self.take_and_disconnect(camera.as_str()).await;
        self.sip_cache.lock().await.remove(camera.as_str());
        debug!(%camera, "WebRTC signaling session torn down");
        Ok(())
    }
}
