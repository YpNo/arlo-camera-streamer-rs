//! The connection to the broker: TCP, TLS when asked, and the few MQTT
//! 3.1.1 packets a publish-only client needs (CONNECT with a last will,
//! PUBLISH at `QoS` 0, PINGREQ, DISCONNECT). Nothing is subscribed, so the
//! broker only ever answers CONNECT and PINGREQ, and one task can write
//! and then read.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rustls::pki_types::ServerName;
use secrecy::SecretString;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use super::codec::{self, ConnectFields, Incoming};
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
/// Largest packet body read from the broker. It only sends CONNACK (two
/// bytes) and PINGRESP (none); anything bigger is a misbehaving broker.
const MAX_INCOMING_BYTES: usize = 16;

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

    /// The CONNECT, in a buffer zeroed on drop: it holds the password.
    fn connect_packet(&self) -> Result<zeroize::Zeroizing<Vec<u8>>, MqttError> {
        codec::connect(&ConnectFields {
            client_id: &self.params.client_id,
            keep_alive_secs: KEEP_ALIVE_SECS,
            will_topic: &self.params.will_topic,
            will_payload: self.params.will_payload.as_bytes(),
            username: self.params.username.as_deref(),
            password: self.params.password.as_ref(),
        })
        .map_err(|e| MqttError::Protocol(e.to_string()))
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
            None => open(tcp, &self.connect_packet()?).await,
            Some((connector, name)) => {
                let stream = timeout(STEP_TIMEOUT, connector.connect(name.clone(), tcp))
                    .await
                    .map_err(|_| MqttError::Timeout("TLS handshake"))?
                    .map_err(|e| MqttError::Tls(e.to_string()))?;
                open(stream, &self.connect_packet()?).await
            }
        }
    }
}

/// CONNECT on `stream`; the session once the broker accepted it.
async fn open<S>(stream: S, connect: &[u8]) -> Result<Box<dyn Session>, MqttError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut session = StreamSession::new(stream);
    session.handshake(connect).await?;
    Ok(Box::new(session))
}

/// A connected stream.
struct StreamSession<S> {
    stream: S,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> StreamSession<S> {
    const fn new(stream: S) -> Self {
        Self { stream }
    }

    /// Write `bytes` within [`WRITE_TIMEOUT`].
    async fn write(&mut self, bytes: &[u8]) -> Result<(), MqttError> {
        timeout(WRITE_TIMEOUT, self.stream.write_all(bytes))
            .await
            .map_err(|_| MqttError::Timeout("write"))?
            .map_err(|e| MqttError::Io(e.to_string()))
    }

    /// The next packet from the broker, within [`STEP_TIMEOUT`]: a fixed
    /// header, a remaining length of at most four bytes, and a body of
    /// at most [`MAX_INCOMING_BYTES`].
    async fn read_packet(&mut self, step: &'static str) -> Result<Incoming, MqttError> {
        timeout(STEP_TIMEOUT, async {
            let header = self.read_byte().await?;
            let mut total = 0;
            let mut index = 0;
            let len = loop {
                let byte = self.read_byte().await?;
                match codec::remaining_step(&mut total, index, byte) {
                    Ok(Some(len)) => break len,
                    Ok(None) => index += 1,
                    Err(e) => return Err(MqttError::Protocol(e.to_string())),
                }
            };
            if len > MAX_INCOMING_BYTES {
                return Err(MqttError::Protocol(format!(
                    "the broker sent a {len}-byte packet; only CONNACK and PINGRESP are expected"
                )));
            }
            let mut body = vec![0; len];
            self.stream
                .read_exact(&mut body)
                .await
                .map_err(|e| read_error(&e))?;
            codec::incoming(header, &body).map_err(|e| MqttError::Protocol(e.to_string()))
        })
        .await
        .map_err(|_| MqttError::Timeout(step))?
    }

    async fn read_byte(&mut self) -> Result<u8, MqttError> {
        self.stream.read_u8().await.map_err(|e| read_error(&e))
    }

    async fn handshake(&mut self, connect: &[u8]) -> Result<(), MqttError> {
        self.write(connect).await?;
        match self.read_packet("CONNACK").await? {
            Incoming::ConnAck { code: 0 } => Ok(()),
            Incoming::ConnAck { code } => Err(MqttError::Refused(codec::refusal_name(code).into())),
            Incoming::PingResp => Err(MqttError::Protocol("expected CONNACK".into())),
        }
    }
}

fn read_error(e: &std::io::Error) -> MqttError {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        MqttError::Io("the broker closed the connection".into())
    } else {
        MqttError::Io(e.to_string())
    }
}

#[async_trait]
impl<S: AsyncRead + AsyncWrite + Unpin + Send> Session for StreamSession<S> {
    async fn publish(&mut self, message: &Message) -> Result<(), MqttError> {
        let packet = codec::publish_retained(&message.topic, &message.payload)
            .map_err(|e| MqttError::Protocol(e.to_string()))?;
        self.write(&packet).await
    }

    async fn keep_alive(&mut self) -> Result<(), MqttError> {
        self.write(&codec::PINGREQ).await?;
        match self.read_packet("PINGRESP").await? {
            Incoming::PingResp => Ok(()),
            Incoming::ConnAck { .. } => Err(MqttError::Protocol("expected PINGRESP".into())),
        }
    }

    async fn disconnect(&mut self) {
        let _ = self.write(&codec::DISCONNECT).await;
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
