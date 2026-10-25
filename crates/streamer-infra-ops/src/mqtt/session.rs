//! The connection to the broker: TCP, TLS when asked, and the few MQTT
//! 3.1.1 packets a publish-only client needs (CONNECT with a last will,
//! PUBLISH at `QoS` 0, PINGREQ, DISCONNECT). Nothing is subscribed, so the
//! broker only ever answers CONNECT and PINGREQ, and one task can write
//! and then read.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use mqttbytes::QoS;
use mqttbytes::v4::{
    Connect, ConnectReturnCode, Disconnect, LastWill, Login, Packet, PingReq, Publish,
};
use rustls::pki_types::ServerName;
use secrecy::{ExposeSecret, SecretString};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use super::messages::Message;
use super::{Broker, MqttError, Session};
use streamer_domain::config::MqttEndpoint;

/// Seconds the broker waits without a packet before dropping us; a ping
/// goes out at half of it.
pub(crate) const KEEP_ALIVE_SECS: u16 = 30;
/// TCP connect, TLS handshake, CONNACK, PINGRESP: each must come this soon.
const STEP_TIMEOUT: Duration = Duration::from_secs(10);
/// A write must be done this soon: a broker that stopped reading (its
/// host vanished without closing the connection) would otherwise block
/// the bridge until the kernel gives up on TCP, about 15 minutes with no
/// keep-alive able to notice. Room for a 512 KiB picture on a slow link.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// Largest packet read from the broker. It only sends CONNACK and
/// PINGRESP; anything bigger is a misbehaving broker.
const MAX_INCOMING_BYTES: usize = 64 * 1024;

/// What the CONNECT carries.
pub(crate) struct ConnectParams {
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<SecretString>,
    /// Where the last will (`offline`) goes.
    pub will_topic: String,
    pub will_payload: &'static str,
}

/// Connects to the broker of `endpoint`, over TLS when it says so.
pub(crate) struct TcpBroker {
    endpoint: MqttEndpoint,
    tls: Option<(tokio_rustls::TlsConnector, ServerName<'static>)>,
    params: ConnectParams,
}

impl TcpBroker {
    /// # Errors
    ///
    /// [`MqttError::Tls`] when the endpoint asks for TLS and `tls` is
    /// missing or the host is not a valid TLS server name.
    pub(crate) fn new(
        endpoint: MqttEndpoint,
        tls: Option<Arc<rustls::ClientConfig>>,
        params: ConnectParams,
    ) -> Result<Self, MqttError> {
        let tls = match (endpoint.tls, tls) {
            (false, _) => None,
            (true, None) => return Err(MqttError::Tls("mqtts:// without a TLS setup".into())),
            (true, Some(config)) => {
                let name = ServerName::try_from(endpoint.host.clone()).map_err(|_| {
                    MqttError::Tls(format!("'{}' is not a TLS server name", endpoint.host))
                })?;
                Some((tokio_rustls::TlsConnector::from(config), name))
            }
        };
        Ok(Self {
            endpoint,
            tls,
            params,
        })
    }

    fn connect_packet(&self) -> Connect {
        let mut connect = Connect::new(self.params.client_id.clone());
        connect.keep_alive = KEEP_ALIVE_SECS;
        connect.clean_session = true;
        connect.last_will = Some(LastWill::new(
            self.params.will_topic.clone(),
            self.params.will_payload,
            QoS::AtMostOnce,
            true,
        ));
        // The packet types derive Debug with the password inside: this one
        // is encoded and dropped, never logged.
        connect.login = self.params.username.as_ref().map(|user| {
            let password = self
                .params
                .password
                .as_ref()
                .map(|p| p.expose_secret().to_string())
                .unwrap_or_default();
            Login::new(user.clone(), password)
        });
        connect
    }
}

#[async_trait]
impl Broker for TcpBroker {
    async fn connect(&self) -> Result<Box<dyn Session>, MqttError> {
        let address = (self.endpoint.host.as_str(), self.endpoint.port);
        let tcp = timeout(STEP_TIMEOUT, TcpStream::connect(address))
            .await
            .map_err(|_| MqttError::Timeout("TCP connect"))?
            .map_err(|e| MqttError::Io(e.to_string()))?;
        match &self.tls {
            None => open(tcp, self.connect_packet()).await,
            Some((connector, name)) => {
                let stream = timeout(STEP_TIMEOUT, connector.connect(name.clone(), tcp))
                    .await
                    .map_err(|_| MqttError::Timeout("TLS handshake"))?
                    .map_err(|e| MqttError::Tls(e.to_string()))?;
                open(stream, self.connect_packet()).await
            }
        }
    }
}

/// CONNECT on `stream`; the session once the broker accepted it.
async fn open<S>(stream: S, connect: Connect) -> Result<Box<dyn Session>, MqttError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut session = StreamSession::new(stream);
    session.handshake(connect).await?;
    Ok(Box::new(session))
}

/// A connected stream and its read buffer.
struct StreamSession<S> {
    stream: S,
    buffer: BytesMut,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> StreamSession<S> {
    fn new(stream: S) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(256),
        }
    }

    async fn write(
        &mut self,
        encode: impl FnOnce(&mut BytesMut) -> Result<usize, mqttbytes::Error>,
    ) -> Result<(), MqttError> {
        let mut out = BytesMut::new();
        encode(&mut out).map_err(|e| MqttError::Protocol(e.to_string()))?;
        timeout(WRITE_TIMEOUT, self.stream.write_all(&out))
            .await
            .map_err(|_| MqttError::Timeout("write"))?
            .map_err(|e| MqttError::Io(e.to_string()))
    }

    /// The next packet from the broker, within [`STEP_TIMEOUT`].
    async fn read_packet(&mut self, step: &'static str) -> Result<Packet, MqttError> {
        timeout(STEP_TIMEOUT, async {
            loop {
                match mqttbytes::v4::read(&mut self.buffer, MAX_INCOMING_BYTES) {
                    Ok(packet) => return Ok(packet),
                    Err(mqttbytes::Error::InsufficientBytes(_)) => {}
                    Err(e) => return Err(MqttError::Protocol(e.to_string())),
                }
                let read = self
                    .stream
                    .read_buf(&mut self.buffer)
                    .await
                    .map_err(|e| MqttError::Io(e.to_string()))?;
                if read == 0 {
                    return Err(MqttError::Io("the broker closed the connection".into()));
                }
            }
        })
        .await
        .map_err(|_| MqttError::Timeout(step))?
    }

    async fn handshake(&mut self, connect: Connect) -> Result<(), MqttError> {
        self.write(|out| connect.write(out)).await?;
        match self.read_packet("CONNACK").await? {
            Packet::ConnAck(ack) if ack.code == ConnectReturnCode::Success => Ok(()),
            Packet::ConnAck(ack) => Err(MqttError::Refused(format!("{:?}", ack.code))),
            _ => Err(MqttError::Protocol("expected CONNACK".into())),
        }
    }
}

#[async_trait]
impl<S: AsyncRead + AsyncWrite + Unpin + Send> Session for StreamSession<S> {
    async fn publish(&mut self, message: &Message) -> Result<(), MqttError> {
        let mut publish = Publish::new(
            message.topic.as_str(),
            QoS::AtMostOnce,
            message.payload.to_vec(),
        );
        publish.retain = true;
        self.write(|out| publish.write(out)).await
    }

    async fn keep_alive(&mut self) -> Result<(), MqttError> {
        self.write(|out| PingReq.write(out)).await?;
        match self.read_packet("PINGRESP").await? {
            Packet::PingResp => Ok(()),
            _ => Err(MqttError::Protocol("expected PINGRESP".into())),
        }
    }

    async fn disconnect(&mut self) {
        let _ = self.write(|out| Disconnect.write(out)).await;
        let _ = self.stream.shutdown().await;
    }
}

/// The TLS setup for `mqtts://`: the system roots, or the CA in `ca_pem`
/// for a private one, verified with the workspace's `ring` provider.
///
/// # Errors
///
/// [`MqttError::Tls`] when no root can be loaded or the PEM holds none.
pub(crate) fn tls_config(ca_pem: Option<&[u8]>) -> Result<Arc<rustls::ClientConfig>, MqttError> {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    match ca_pem {
        Some(pem) => {
            for cert in CertificateDer::pem_slice_iter(pem) {
                let cert = cert.map_err(|e| MqttError::Tls(format!("ca_file: {e}")))?;
                roots
                    .add(cert)
                    .map_err(|e| MqttError::Tls(format!("ca_file: {e}")))?;
            }
        }
        None => {
            roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
        }
    }
    if roots.is_empty() {
        return Err(MqttError::Tls(
            "no root certificate to verify the broker with".into(),
        ));
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| MqttError::Tls(e.to_string()))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(Arc::new(config))
}
