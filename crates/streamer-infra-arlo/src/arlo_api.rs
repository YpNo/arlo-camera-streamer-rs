//! The arlo-rs calls the adapters make, behind one seam.
//!
//! The adapters' own logic — forgetting a cached device when a call for
//! it fails, closing a stale signaling socket before negotiating again,
//! recording snapshot URLs seen on the bus — sits around these calls.
//! [`ArloClient`] is the only production implementation; the tests use a
//! scripted fake instead of a live account, since the bus and signaling
//! only dial Arlo's own WSS hosts.

use arlo_rs::client::ArloClient;
use arlo_rs::client::livestream::{SignalingAnswer, SignalingSocket};
use arlo_rs::error::ArloError;
use arlo_rs::events::ConnectionState;
use arlo_rs::models::api::{Device, StreamUrl};
use arlo_rs::models::events::ArloEvent;
use arlo_rs::models::sip::SipInfo;
use async_trait::async_trait;
use tokio::sync::{broadcast, watch};

/// An open WebRTC signaling session, held until the call ends.
#[async_trait]
pub(crate) trait SignalingSession: Send {
    /// Tell the gateway the session is over and close it. Best-effort.
    async fn disconnect(self: Box<Self>);
}

#[async_trait]
impl SignalingSession for SignalingSocket {
    async fn disconnect(self: Box<Self>) {
        SignalingSocket::disconnect(*self).await;
    }
}

/// The Arlo cloud operations the adapters use.
#[async_trait]
pub(crate) trait ArloApi: Send + Sync {
    /// The account's devices.
    async fn devices(&self) -> Result<Vec<Device>, ArloError>;

    /// The WebRTC call parameters for `device`.
    async fn sip_info(&self, device: &Device) -> Result<SipInfo, ArloError>;

    /// Send `offer_sdp` and return the answer with the open session.
    async fn webrtc_negotiate(
        &self,
        sip: &SipInfo,
        offer_sdp: &str,
    ) -> Result<(SignalingAnswer, Box<dyn SignalingSession>), ArloError>;

    /// The stream URL Arlo hands a client identifying as `user_agent`.
    async fn stream_url_as(
        &self,
        device: &Device,
        user_agent: Option<&str>,
    ) -> Result<Option<StreamUrl>, ArloError>;

    /// The event bus's events (the bus starts on first use).
    async fn events(&self) -> Result<broadcast::Receiver<ArloEvent>, ArloError>;

    /// The event bus's connection state.
    async fn connection_states(&self) -> Result<watch::Receiver<ConnectionState>, ArloError>;
}

#[async_trait]
impl ArloApi for ArloClient {
    async fn devices(&self) -> Result<Vec<Device>, ArloError> {
        self.get_devices().await
    }

    async fn sip_info(&self, device: &Device) -> Result<SipInfo, ArloError> {
        ArloClient::sip_info(self, device).await
    }

    async fn webrtc_negotiate(
        &self,
        sip: &SipInfo,
        offer_sdp: &str,
    ) -> Result<(SignalingAnswer, Box<dyn SignalingSession>), ArloError> {
        let (answer, socket) = ArloClient::webrtc_negotiate(self, sip, offer_sdp).await?;
        Ok((answer, Box::new(socket)))
    }

    async fn stream_url_as(
        &self,
        device: &Device,
        user_agent: Option<&str>,
    ) -> Result<Option<StreamUrl>, ArloError> {
        self.get_stream_url_as(device, user_agent).await
    }

    async fn events(&self) -> Result<broadcast::Receiver<ArloEvent>, ArloError> {
        Ok(ArloClient::events(self).await?.subscribe())
    }

    async fn connection_states(&self) -> Result<watch::Receiver<ConnectionState>, ArloError> {
        Ok(ArloClient::events(self).await?.connection_state())
    }
}

/// A scripted [`ArloApi`] for the adapters' tests.
#[cfg(test)]
pub(crate) mod fake {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::{
        ArloApi, ArloError, ArloEvent, ConnectionState, Device, SignalingAnswer, SignalingSession,
        SipInfo, StreamUrl, async_trait, broadcast, watch,
    };

    /// How a scripted call ends.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Outcome {
        Ok,
        /// Arlo's "stream already in progress" (code 14001).
        Busy,
        /// Any other failure.
        Fails,
    }

    impl Outcome {
        fn result(self) -> Result<(), ArloError> {
            match self {
                Self::Ok => Ok(()),
                Self::Busy => Err(ArloError::ApiError {
                    code: 500,
                    error: Some(14_001),
                    message: "RTSP Streaming in progress".to_string(),
                }),
                Self::Fails => Err(ArloError::ScraperError("gateway unreachable".to_string())),
            }
        }
    }

    /// An Arlo device as the cloud lists it.
    pub(crate) fn device(id: &str) -> Device {
        Device {
            device_id: id.to_string(),
            parent_id: id.to_string(),
            device_type: "camera".to_string(),
            device_name: format!("cam-{id}"),
            unique_id: format!("uniq-{id}"),
            state: "provisioned".to_string(),
            mac_address: None,
            firm_version: None,
            hw_version: None,
            model_id: None,
            presigned_last_image_url: None,
            x_cloud_id: Some(format!("xc-{id}")),
            automation_revision: None,
            allowed_mqtt_topics: vec![],
            connectivity: None,
        }
    }

    fn sip_info(device_id: &str) -> SipInfo {
        serde_json::from_value(serde_json::json!({
            "sipCallInfo": {
                "id": "call", "calleeUri": "sip:x@livestream.arlo.com:443",
                "domain": "livestream.arlo.com", "port": 443, "password": "SIP-SECRET",
                "deviceId": device_id, "callId": "call-1"
            },
            "iceServers": { "data": [
                { "type": "stun", "domain": "stun.arlo.com", "port": "3478" }
            ] }
        }))
        .expect("sip info fixture")
    }

    struct FakeSession {
        disconnects: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SignalingSession for FakeSession {
        async fn disconnect(self: Box<Self>) {
            self.disconnects.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Scripted answers and call counters. Set the fields before sharing.
    pub(crate) struct FakeArlo {
        pub devices: Mutex<Vec<Device>>,
        pub devices_outcome: Outcome,
        /// How long `devices` takes, so concurrent callers overlap.
        pub devices_delay: Duration,
        pub sip_outcome: Outcome,
        pub negotiate_outcome: Outcome,
        pub stream_outcome: Outcome,
        pub stream_url: Option<String>,
        pub bus_outcome: Outcome,
        pub device_calls: AtomicUsize,
        pub negotiations: AtomicUsize,
        pub disconnects: Arc<AtomicUsize>,
        pub events: broadcast::Sender<ArloEvent>,
        pub connection: watch::Sender<ConnectionState>,
    }

    impl FakeArlo {
        /// Everything succeeds; `ids` are the listed devices.
        pub(crate) fn with_devices(ids: &[&str]) -> Self {
            Self {
                devices: Mutex::new(ids.iter().map(|id| device(id)).collect()),
                devices_outcome: Outcome::Ok,
                devices_delay: Duration::ZERO,
                sip_outcome: Outcome::Ok,
                negotiate_outcome: Outcome::Ok,
                stream_outcome: Outcome::Ok,
                stream_url: None,
                bus_outcome: Outcome::Ok,
                device_calls: AtomicUsize::new(0),
                negotiations: AtomicUsize::new(0),
                disconnects: Arc::new(AtomicUsize::new(0)),
                events: broadcast::channel(16).0,
                connection: watch::channel(ConnectionState::Connecting).0,
            }
        }

        pub(crate) fn calls(counter: &AtomicUsize) -> usize {
            counter.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ArloApi for FakeArlo {
        async fn devices(&self) -> Result<Vec<Device>, ArloError> {
            self.device_calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.devices_delay).await;
            self.devices_outcome.result()?;
            let listed = self
                .devices
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // `Device` is not `Clone`: rebuild what the adapters read.
            Ok(listed
                .iter()
                .map(|d| Device {
                    presigned_last_image_url: d.presigned_last_image_url.clone(),
                    ..device(&d.device_id)
                })
                .collect())
        }

        async fn sip_info(&self, device: &Device) -> Result<SipInfo, ArloError> {
            self.sip_outcome.result()?;
            Ok(sip_info(&device.device_id))
        }

        async fn webrtc_negotiate(
            &self,
            _sip: &SipInfo,
            offer_sdp: &str,
        ) -> Result<(SignalingAnswer, Box<dyn SignalingSession>), ArloError> {
            self.negotiations.fetch_add(1, Ordering::SeqCst);
            self.negotiate_outcome.result()?;
            let answer = SignalingAnswer {
                answer_sdp: format!("answer to {offer_sdp}"),
                session_id: "session-0123456789".to_string(),
            };
            let session = FakeSession {
                disconnects: self.disconnects.clone(),
            };
            Ok((answer, Box::new(session)))
        }

        async fn stream_url_as(
            &self,
            _device: &Device,
            _user_agent: Option<&str>,
        ) -> Result<Option<StreamUrl>, ArloError> {
            self.stream_outcome.result()?;
            Ok(self.stream_url.clone().map(StreamUrl))
        }

        async fn events(&self) -> Result<broadcast::Receiver<ArloEvent>, ArloError> {
            self.bus_outcome.result()?;
            Ok(self.events.subscribe())
        }

        async fn connection_states(&self) -> Result<watch::Receiver<ConnectionState>, ArloError> {
            self.bus_outcome.result()?;
            Ok(self.connection.subscribe())
        }
    }
}
