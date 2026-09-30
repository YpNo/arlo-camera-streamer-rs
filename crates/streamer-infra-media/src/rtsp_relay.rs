//! Relay of a live view the user started in the Arlo app (ADR 0007).
//!
//! Arlo hands the app identity a `rtsps://` watch-along URL of the view
//! ([`WatchAlongUrl`]). This module is the RTSP client that plays it:
//! `OPTIONS`, `DESCRIBE`, one `SETUP` for the video track over
//! TCP-interleaved, `PLAY`, then a read loop that forwards every video
//! RTP packet into the camera's [`LiveRtpSink`] — the same path the
//! WebRTC leg feeds, so the idle/live splice, HLS and the RTSP clients
//! see it unchanged. The payload type is rewritten to the one the live
//! `appsrc` declares.
//!
//! A hand-written client rather than `rtspsrc`: Arlo's server refused
//! `rtspsrc`'s `SETUP` (403) while this exchange, byte for byte as the
//! probe sent it, is accepted (2026-09-30), and the loop needs to own
//! keep-alives, RTCP receiver reports and the loss signals anyway.
//!
//! Certificate validation is off: the URL names a raw IP no certificate
//! can match. TLS still hides the exchange; the egress token in the URL
//! is the access control, as it is for the app.
//!
//! ## Loss detection
//!
//! The same [`LiveLossNotifier`] as the WebRTC leg, first report wins:
//! the shared stall watchdog ([`crate::live_watch`]), the server closing
//! the connection (`end-of-stream`), a transport error
//! (`peer-disconnected`).
//!
//! Pure pieces (framing, SDP, URL joining, payload-type rewrite, RTCP)
//! are unit-tested here; the connection is exercised by
//! `tests/live_session.rs` against the crate's own RTSP server.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{Notify, oneshot};
use tokio_rustls::TlsConnector;
use tracing::{debug, info, warn};

use streamer_domain::state::LiveLossReason;
use streamer_domain::stream::{LiveLossNotifier, WatchAlongUrl};

use crate::error::MediaError;
use crate::live_rtp_sink::LiveRtpSink;
use crate::live_watch::{RtpActivity, report_loss, spawn_stall_watchdog};
use crate::pipeline_desc::LIVE_RTP_H264_PT;

/// Per-request RTSP timeout (connect, handshake, each response).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Max wait from `PLAY` to the first video RTP packet.
const FIRST_RTP_TIMEOUT: Duration = Duration::from_secs(20);
/// `GET_PARAMETER` keep-alive cadence; RTSP sessions expire at 60 s.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(25);
/// RTCP receiver-report cadence on the interleaved RTCP channel.
const RTCP_INTERVAL: Duration = Duration::from_secs(5);
/// Time given to `TEARDOWN` on shutdown before the task is aborted.
const TEARDOWN_GRACE: Duration = Duration::from_secs(2);
/// Interleaved channels asked for in `SETUP`.
const RTP_CHANNEL: u8 = 0;
const RTCP_CHANNEL: u8 = 1;
const USER_AGENT: &str = concat!("arlo-camera-streamer/", env!("CARGO_PKG_VERSION"));
/// Our SSRC in receiver reports; any value distinct from the sender's.
const RECEIVER_SSRC: u32 = 0x4152_4c4f;

trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}

/// A running relay. Dropping it or calling [`shutdown`](Self::shutdown)
/// sends `TEARDOWN` (best effort) and stops the loop.
pub(crate) struct RtspRelay {
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
    watchdog: tokio::task::JoinHandle<()>,
}

impl RtspRelay {
    /// Connect, negotiate the video track, and resolve once the first
    /// video packet was pushed into `sink`. `notifier` is armed from the
    /// first request, so a loss during setup is reported as such.
    ///
    /// Cancel-safe: dropping the future closes the connection.
    ///
    /// # Errors
    ///
    /// [`MediaError::Relay`] for a refused or malformed exchange,
    /// [`MediaError::SpliceTimeout`] when no video arrives in time.
    pub(crate) async fn start(
        url: &WatchAlongUrl,
        sink: LiveRtpSink,
        stall_timeout: Duration,
        notifier: LiveLossNotifier,
    ) -> Result<Self, MediaError> {
        let mut client = Client::connect(url).await?;
        let base = client.describe(url.as_str()).await?;
        client.setup_video(&base).await?;
        client.play(&base.aggregate).await?;
        info!(url = %url, "watch-along stream playing; awaiting first video RTP");

        let activity = Arc::new(RtpActivity::new(Instant::now()));
        let first_rtp = Arc::new(Notify::new());
        let (stop_tx, stop_rx) = oneshot::channel();
        let task = tokio::spawn(run(
            client,
            base.aggregate,
            sink,
            activity.clone(),
            first_rtp.clone(),
            notifier.clone(),
            stop_rx,
        ));
        let mut relay = Self {
            stop: Some(stop_tx),
            task,
            watchdog: tokio::spawn(async {}),
        };
        if tokio::time::timeout(FIRST_RTP_TIMEOUT, first_rtp.notified())
            .await
            .is_err()
        {
            relay.shutdown();
            return Err(MediaError::SpliceTimeout {
                timeout_secs: FIRST_RTP_TIMEOUT.as_secs(),
            });
        }
        relay.watchdog = spawn_stall_watchdog(activity, stall_timeout, notifier);
        Ok(relay)
    }

    /// Stop the relay. Idempotent.
    pub(crate) fn shutdown(&mut self) {
        let Some(stop) = self.stop.take() else {
            return;
        };
        self.watchdog.abort();
        if stop.send(()).is_err() {
            // The loop already ended (loss reported); nothing to tear down.
            return;
        }
        let task = self.task.abort_handle();
        tokio::spawn(async move {
            tokio::time::sleep(TEARDOWN_GRACE).await;
            task.abort();
        });
    }
}

impl Drop for RtspRelay {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// What `DESCRIBE` yields: the aggregate control URL and the video
/// track's control URL.
struct Described {
    aggregate: String,
    video_control: String,
}

struct Response {
    code: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

struct Client {
    io: BufReader<Box<dyn Io>>,
    cseq: u32,
    session: Option<String>,
}

impl Client {
    async fn connect(url: &WatchAlongUrl) -> Result<Self, MediaError> {
        let parsed = url::Url::parse(url.as_str())
            .map_err(|e| MediaError::Relay(format!("watch-along URL: {e}")))?;
        let host = parsed
            .host_str()
            .ok_or_else(|| MediaError::Relay("watch-along URL has no host".into()))?
            .to_string();
        let tls = parsed.scheme() == "rtsps";
        let port = parsed.port().unwrap_or(if tls { 443 } else { 554 });
        let tcp = tokio::time::timeout(REQUEST_TIMEOUT, TcpStream::connect((host.as_str(), port)))
            .await
            .map_err(|_| MediaError::Relay(format!("connect to {host}:{port} timed out")))?
            .map_err(|e| MediaError::Relay(format!("connect to {host}:{port}: {e}")))?;
        let io: Box<dyn Io> = if tls {
            Box::new(tls_handshake(tcp, &host).await?)
        } else {
            Box::new(tcp)
        };
        Ok(Self {
            io: BufReader::new(io),
            cseq: 0,
            session: None,
        })
    }

    async fn request(
        &mut self,
        method: &str,
        uri: &str,
        extra: &[(&str, &str)],
    ) -> Result<Response, MediaError> {
        self.cseq += 1;
        let text = request_text(method, uri, self.cseq, self.session.as_deref(), extra);
        self.io
            .get_mut()
            .write_all(text.as_bytes())
            .await
            .map_err(|e| MediaError::Relay(format!("{method}: send: {e}")))?;
        let response = tokio::time::timeout(REQUEST_TIMEOUT, read_response(&mut self.io))
            .await
            .map_err(|_| {
                MediaError::Relay(format!("{method}: no response in {REQUEST_TIMEOUT:?}"))
            })??;
        if let Some(s) = response.header("Session") {
            self.session = Some(session_id(s).to_string());
        }
        debug!(method, code = response.code, "rtsp relay exchange");
        if response.code != 200 {
            return Err(MediaError::Relay(format!(
                "{method} answered {}",
                response.code
            )));
        }
        Ok(response)
    }

    async fn describe(&mut self, url: &str) -> Result<Described, MediaError> {
        self.request("OPTIONS", url, &[]).await?;
        let response = self
            .request("DESCRIBE", url, &[("Accept", "application/sdp")])
            .await?;
        let aggregate = response
            .header("Content-Base")
            .map_or_else(|| url.to_string(), str::to_string);
        let sdp = String::from_utf8_lossy(&response.body);
        let control = video_control(&sdp)
            .ok_or_else(|| MediaError::Relay("SDP has no H.264 video track".into()))?;
        Ok(Described {
            video_control: join_control(&aggregate, &control),
            aggregate,
        })
    }

    async fn setup_video(&mut self, base: &Described) -> Result<(), MediaError> {
        let transport = format!("RTP/AVP/TCP;unicast;interleaved={RTP_CHANNEL}-{RTCP_CHANNEL}");
        self.request("SETUP", &base.video_control, &[("Transport", &transport)])
            .await?;
        Ok(())
    }

    async fn play(&mut self, aggregate: &str) -> Result<(), MediaError> {
        self.request("PLAY", aggregate, &[("Range", "npt=0.000-")])
            .await?;
        Ok(())
    }

    /// A request whose response arrives in the read loop.
    async fn send_only(&mut self, method: &str, uri: &str) -> std::io::Result<()> {
        self.cseq += 1;
        let text = request_text(method, uri, self.cseq, self.session.as_deref(), &[]);
        self.io.get_mut().write_all(text.as_bytes()).await
    }

    async fn send_rtcp_receiver_report(&mut self) -> std::io::Result<()> {
        let report = rtcp_receiver_report(RECEIVER_SSRC);
        let frame = interleaved_frame(RTCP_CHANNEL, &report);
        self.io.get_mut().write_all(&frame).await
    }
}

/// The read loop: interleaved frames and stray RTSP messages until the
/// server closes, a transport error, or a stop request.
async fn run(
    mut client: Client,
    aggregate: String,
    sink: LiveRtpSink,
    activity: Arc<RtpActivity>,
    first_rtp: Arc<Notify>,
    notifier: LiveLossNotifier,
    mut stop: oneshot::Receiver<()>,
) {
    let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
    let mut rtcp = tokio::time::interval(RTCP_INTERVAL);
    keepalive.tick().await;
    rtcp.tick().await;
    let mut got_first = false;
    let end = loop {
        tokio::select! {
            _ = &mut stop => break None,
            _ = keepalive.tick() => {
                if let Err(e) = client.send_only("GET_PARAMETER", &aggregate).await {
                    break Some((LiveLossReason::PeerDisconnected, format!("keep-alive: {e}")));
                }
            }
            _ = rtcp.tick() => {
                if let Err(e) = client.send_rtcp_receiver_report().await {
                    break Some((LiveLossReason::PeerDisconnected, format!("rtcp: {e}")));
                }
            }
            next = read_next(&mut client.io) => match next {
                Ok(Incoming::Rtp(packet)) => {
                    activity.touch(Instant::now());
                    if !got_first {
                        got_first = true;
                        first_rtp.notify_one();
                    }
                    let _ = sink.push(packet);
                }
                Ok(Incoming::Other) => {}
                Ok(Incoming::ServerRequest { cseq }) => {
                    let reply = format!("RTSP/1.0 200 OK\r\nCSeq: {cseq}\r\n\r\n");
                    if let Err(e) = client.io.get_mut().write_all(reply.as_bytes()).await {
                        break Some((LiveLossReason::PeerDisconnected, format!("reply: {e}")));
                    }
                }
                Ok(Incoming::Closed) => break Some((LiveLossReason::EndOfStream, "server closed the stream".into())),
                Err(e) => break Some((LiveLossReason::PeerDisconnected, e)),
            }
        }
    };
    if let Some((reason, why)) = end {
        warn!(%why, "watch-along relay ended");
        report_loss(&notifier, reason);
    } else {
        let teardown = client.request("TEARDOWN", &aggregate, &[]);
        if let Err(e) = tokio::time::timeout(TEARDOWN_GRACE, teardown).await {
            debug!(error = %e, "watch-along TEARDOWN not confirmed");
        }
        debug!("watch-along relay stopped");
    }
}

enum Incoming {
    /// A video RTP packet, payload type already rewritten.
    Rtp(Bytes),
    /// RTCP, audio, or a response to one of our keep-alives.
    Other,
    /// A request from the server to acknowledge.
    ServerRequest {
        cseq: String,
    },
    Closed,
}

/// One interleaved frame or one RTSP message.
async fn read_next(io: &mut BufReader<Box<dyn Io>>) -> Result<Incoming, String> {
    let mut first = [0u8; 1];
    match io.read(&mut first).await {
        Ok(0) => return Ok(Incoming::Closed),
        Ok(_) => {}
        Err(e) => return Err(format!("read: {e}")),
    }
    if first[0] == b'$' {
        let mut header = [0u8; 3];
        io.read_exact(&mut header)
            .await
            .map_err(|e| format!("frame header: {e}"))?;
        let (channel, len) = interleaved_header(header);
        let mut payload = vec![0u8; len];
        io.read_exact(&mut payload)
            .await
            .map_err(|e| format!("frame payload: {e}"))?;
        if channel == RTP_CHANNEL && rewrite_payload_type(&mut payload, LIVE_RTP_H264_PT) {
            return Ok(Incoming::Rtp(Bytes::from(payload)));
        }
        return Ok(Incoming::Other);
    }
    let mut line = String::from(first[0] as char);
    io.read_line(&mut line)
        .await
        .map_err(|e| format!("message line: {e}"))?;
    let mut cseq = None;
    let mut content_length = 0usize;
    let mut header = String::new();
    loop {
        header.clear();
        io.read_line(&mut header)
            .await
            .map_err(|e| format!("message header: {e}"))?;
        let h = header.trim_end();
        if h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            if k.eq_ignore_ascii_case("CSeq") {
                cseq = Some(v.trim().to_string());
            } else if k.eq_ignore_ascii_case("Content-Length") {
                content_length = v.trim().parse().unwrap_or(0);
            }
        }
    }
    if content_length > 0 {
        let mut body = vec![0u8; content_length];
        io.read_exact(&mut body)
            .await
            .map_err(|e| format!("message body: {e}"))?;
    }
    if line.starts_with("RTSP/") {
        return Ok(Incoming::Other);
    }
    Ok(Incoming::ServerRequest {
        cseq: cseq.unwrap_or_else(|| "0".to_string()),
    })
}

async fn read_response(io: &mut BufReader<Box<dyn Io>>) -> Result<Response, MediaError> {
    let mut line = String::new();
    io.read_line(&mut line)
        .await
        .map_err(|e| MediaError::Relay(format!("status line: {e}")))?;
    let code = parse_status_code(&line)
        .ok_or_else(|| MediaError::Relay("malformed RTSP status line".into()))?;
    let mut headers = Vec::new();
    loop {
        line.clear();
        io.read_line(&mut line)
            .await
            .map_err(|e| MediaError::Relay(format!("header: {e}")))?;
        let h = line.trim_end();
        if h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    let len: usize = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("Content-Length"))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; len];
    if len > 0 {
        io.read_exact(&mut body)
            .await
            .map_err(|e| MediaError::Relay(format!("body: {e}")))?;
    }
    Ok(Response {
        code,
        headers,
        body,
    })
}

async fn tls_handshake(
    tcp: TcpStream,
    host: &str,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, MediaError> {
    let provider = rustls::crypto::ring::default_provider();
    let config = rustls::ClientConfig::builder_with_provider(provider.clone().into())
        .with_safe_default_protocol_versions()
        .map_err(|e| MediaError::Relay(format!("TLS config: {e}")))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify(provider)))
        .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| MediaError::Relay(format!("TLS server name: {e}")))?;
    tokio::time::timeout(
        REQUEST_TIMEOUT,
        TlsConnector::from(Arc::new(config)).connect(name, tcp),
    )
    .await
    .map_err(|_| MediaError::Relay("TLS handshake timed out".into()))?
    .map_err(|e| MediaError::Relay(format!("TLS handshake: {e}")))
}

/// Accepts any certificate (see the module docs). Signatures are still
/// verified, so the handshake is a real TLS handshake with the peer.
#[derive(Debug)]
struct NoVerify(rustls::crypto::CryptoProvider);

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

// ---- pure pieces -------------------------------------------------------

/// The text of one RTSP request.
fn request_text(
    method: &str,
    uri: &str,
    cseq: u32,
    session: Option<&str>,
    extra: &[(&str, &str)],
) -> String {
    use std::fmt::Write as _;
    let mut text =
        format!("{method} {uri} RTSP/1.0\r\nCSeq: {cseq}\r\nUser-Agent: {USER_AGENT}\r\n");
    if let Some(s) = session {
        let _ = write!(text, "Session: {s}\r\n");
    }
    for (k, v) in extra {
        let _ = write!(text, "{k}: {v}\r\n");
    }
    text.push_str("\r\n");
    text
}

/// `RTSP/1.0 200 OK` → `200`.
fn parse_status_code(line: &str) -> Option<u16> {
    let mut parts = line.split_whitespace();
    parts.next().filter(|p| p.starts_with("RTSP/"))?;
    parts.next()?.parse().ok()
}

/// The id part of a `Session` header (`id;timeout=60` → `id`).
fn session_id(header: &str) -> &str {
    header.split(';').next().unwrap_or(header).trim()
}

/// The `a=control:` value of the first `m=video` section whose payload
/// is H.264, or `None`.
fn video_control(sdp: &str) -> Option<String> {
    let mut in_video = false;
    let mut control = None;
    let mut h264 = false;
    for line in sdp.lines().map(str::trim_end) {
        if let Some(media) = line.strip_prefix("m=") {
            if in_video && control.is_some() && h264 {
                break;
            }
            in_video = media.starts_with("video ");
            control = None;
            h264 = false;
            continue;
        }
        if !in_video {
            continue;
        }
        if let Some(c) = line.strip_prefix("a=control:") {
            control = Some(c.trim().to_string());
        } else if let Some(map) = line.strip_prefix("a=rtpmap:")
            && map.to_ascii_uppercase().contains("H264/")
        {
            h264 = true;
        }
    }
    (in_video && h264).then_some(control).flatten()
}

/// Resolve a track control against the aggregate URL: absolute controls
/// are used as they are, relative ones appended with one `/`.
fn join_control(base: &str, control: &str) -> String {
    if control.contains("://") {
        return control.to_string();
    }
    if control == "*" {
        return base.to_string();
    }
    if base.ends_with('/') {
        format!("{base}{control}")
    } else {
        format!("{base}/{control}")
    }
}

/// `$`, channel, 16-bit big-endian length.
fn interleaved_header(header: [u8; 3]) -> (u8, usize) {
    (
        header[0],
        usize::from(u16::from_be_bytes([header[1], header[2]])),
    )
}

fn interleaved_frame(channel: u8, payload: &[u8]) -> Vec<u8> {
    let len = u16::try_from(payload.len()).unwrap_or(u16::MAX);
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&[b'$', channel]);
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&payload[..usize::from(len)]);
    frame
}

/// Set the RTP payload type, keeping the marker bit. `false` for a buffer
/// too short to be RTP or not version 2.
fn rewrite_payload_type(packet: &mut [u8], pt: i32) -> bool {
    let Ok(pt) = u8::try_from(pt) else {
        return false;
    };
    if packet.len() < 12 || packet[0] >> 6 != 2 {
        return false;
    }
    packet[1] = (packet[1] & 0x80) | (pt & 0x7f);
    true
}

/// A minimal RTCP receiver report with no report blocks (RFC 3550 §6.4.2):
/// enough to tell the server the receiver is alive.
fn rtcp_receiver_report(ssrc: u32) -> [u8; 8] {
    let mut report = [0u8; 8];
    report[0] = 0x80; // V=2, P=0, RC=0
    report[1] = 201; // RR
    report[2..4].copy_from_slice(&1u16.to_be_bytes()); // length in 32-bit words minus one
    report[4..8].copy_from_slice(&ssrc.to_be_bytes());
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    const SDP: &str = "v=0\r\no=- 1 1 IN IP4 0.0.0.0\r\ns=cam\r\nt=0 0\r\n\
        m=audio 0 RTP/AVP 98\r\na=control:trackid=2\r\na=rtpmap:98 MPEG4-GENERIC/16000/1\r\n\
        m=video 0 RTP/AVP 96\r\na=control:trackid=1\r\na=rtpmap:96 H264/90000\r\na=ssrc:1\r\n";

    #[test]
    fn video_control_picks_the_h264_video_track() {
        assert_eq!(video_control(SDP).as_deref(), Some("trackid=1"));
        let h265 = SDP.replace("H264/90000", "H265/90000");
        assert_eq!(video_control(&h265), None);
        assert_eq!(
            video_control("v=0\r\nm=audio 0 RTP/AVP 98\r\na=control:a\r\n"),
            None
        );
    }

    #[test]
    fn join_control_handles_relative_absolute_and_star() {
        assert_eq!(
            join_control("rtsps://h/live/x/", "trackid=1"),
            "rtsps://h/live/x/trackid=1"
        );
        assert_eq!(
            join_control("rtsps://h/live/x", "trackid=1"),
            "rtsps://h/live/x/trackid=1"
        );
        assert_eq!(join_control("rtsps://h/live/x", "rtsp://o/t"), "rtsp://o/t");
        assert_eq!(join_control("rtsps://h/live/x", "*"), "rtsps://h/live/x");
    }

    #[test]
    fn request_text_carries_cseq_agent_session_and_extras() {
        let text = request_text(
            "SETUP",
            "rtsp://h/t",
            3,
            Some("S1"),
            &[("Transport", "RTP/AVP/TCP")],
        );
        assert!(text.starts_with("SETUP rtsp://h/t RTSP/1.0\r\nCSeq: 3\r\n"));
        assert!(text.contains("Session: S1\r\n"));
        assert!(text.contains("Transport: RTP/AVP/TCP\r\n"));
        assert!(text.ends_with("\r\n\r\n"));
        assert!(!request_text("OPTIONS", "rtsp://h", 1, None, &[]).contains("Session"));
    }

    #[test]
    fn status_code_and_session_id_parse() {
        assert_eq!(parse_status_code("RTSP/1.0 403 Forbidden\r\n"), Some(403));
        assert_eq!(parse_status_code("HTTP/1.1 200 OK"), None);
        assert_eq!(parse_status_code("garbage"), None);
        assert_eq!(session_id("abc123;timeout=60"), "abc123");
        assert_eq!(session_id(" abc123 "), "abc123");
    }

    #[test]
    fn interleaved_framing_round_trips() {
        let frame = interleaved_frame(1, &[1, 2, 3]);
        assert_eq!(frame, [b'$', 1, 0, 3, 1, 2, 3]);
        assert_eq!(interleaved_header([0, 0x01, 0x02]), (0, 258));
    }

    #[test]
    fn rewrite_payload_type_keeps_the_marker_and_rejects_non_rtp() {
        let mut packet = vec![0x80, 0x80 | 0x60, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0xAA];
        assert!(rewrite_payload_type(&mut packet, 103));
        assert_eq!(packet[1], 0x80 | 0x67);
        let mut short = vec![0x80, 96];
        assert!(!rewrite_payload_type(&mut short, 103));
        let mut v1 = vec![0x40; 12];
        assert!(!rewrite_payload_type(&mut v1, 103));
        assert!(!rewrite_payload_type(&mut packet, 300));
    }

    #[test]
    fn rtcp_receiver_report_is_a_valid_empty_rr() {
        let rr = rtcp_receiver_report(0x0102_0304);
        assert_eq!(rr, [0x80, 201, 0, 1, 1, 2, 3, 4]);
    }
}
