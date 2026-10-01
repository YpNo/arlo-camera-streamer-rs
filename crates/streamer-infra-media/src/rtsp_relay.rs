//! Relay of a live view the user started in the Arlo app (ADR 0007).
//!
//! Arlo hands the app identity a `rtsps://` watch-along URL of the view
//! ([`WatchAlongUrl`]). This module is the RTSP client that plays it:
//! `OPTIONS`, `DESCRIBE`, one `SETUP` for the video track over
//! TCP-interleaved (on the channels the server assigns), `PLAY`, then
//! two tasks: a read loop that forwards every video RTP packet into the
//! camera's [`LiveRtpSink`] — the same path the WebRTC leg feeds, so the
//! idle/live splice, HLS and the RTSP clients see it unchanged — and a
//! write loop for keep-alives, receiver reports and acks. The read half
//! is owned by its task on purpose: a frame read interrupted by a timer
//! would leave the stream mid-frame. The payload type is rewritten to
//! the one the live `appsrc` declares.
//!
//! Bytes that are neither a frame nor an RTSP message end the relay with
//! a report of what preceded them and a bounded hex dump, so a framing
//! the server does differently can be read off the log.
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
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadHalf,
    WriteHalf,
};
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc, oneshot};
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
/// Server requests waiting for their ack from the writer.
const ACK_QUEUE: usize = 8;
/// Interleaved frames described in the log at debug, per relay.
const FIRST_FRAMES_LOGGED: u64 = 4;
/// Bytes shown in a desync report.
const DESYNC_DUMP_BYTES: usize = 24;
/// RTCP packet types (RFC 3550 §12.1): SR, RR, SDES, BYE, APP.
const RTCP_PT_FIRST: u8 = 200;
const RTCP_PT_LAST: u8 = 204;
/// An RTCP header: V/P/count, PT, 16-bit length in words minus one.
const RTCP_HEADER_LEN: usize = 4;
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

/// Read half of the connection, buffered for the line-based exchange.
type Reader = BufReader<ReadHalf<Box<dyn Io>>>;
type Writer = WriteHalf<Box<dyn Io>>;

/// The write half with the bookkeeping every request needs.
struct Link {
    writer: Writer,
    cseq: u32,
    session: Option<String>,
    /// Interleaved channels (RTP, RTCP) — ours until the server's
    /// `SETUP` answer assigns others.
    channels: (u8, u8),
}

impl Link {
    /// Write one request; the response is read by the caller or the
    /// read loop.
    async fn send(
        &mut self,
        method: &str,
        uri: &str,
        extra: &[(&str, &str)],
    ) -> std::io::Result<()> {
        self.cseq += 1;
        let text = request_text(method, uri, self.cseq, self.session.as_deref(), extra);
        self.writer.write_all(text.as_bytes()).await
    }

    async fn send_rtcp_receiver_report(&mut self) -> std::io::Result<()> {
        let report = rtcp_receiver_report(RECEIVER_SSRC);
        let frame = interleaved_frame(self.channels.1, &report);
        self.writer.write_all(&frame).await
    }

    /// Acknowledge a request the server sent us.
    async fn ack(&mut self, cseq: &str) -> std::io::Result<()> {
        let reply = format!("RTSP/1.0 200 OK\r\nCSeq: {cseq}\r\n\r\n");
        self.writer.write_all(reply.as_bytes()).await
    }
}

struct Client {
    reader: Reader,
    link: Link,
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
        let (reader, writer) = tokio::io::split(io);
        Ok(Self {
            reader: BufReader::new(reader),
            link: Link {
                writer,
                cseq: 0,
                session: None,
                channels: (RTP_CHANNEL, RTCP_CHANNEL),
            },
        })
    }

    async fn request(
        &mut self,
        method: &str,
        uri: &str,
        extra: &[(&str, &str)],
    ) -> Result<Response, MediaError> {
        self.link
            .send(method, uri, extra)
            .await
            .map_err(|e| MediaError::Relay(format!("{method}: send: {e}")))?;
        let response = tokio::time::timeout(REQUEST_TIMEOUT, read_response(&mut self.reader))
            .await
            .map_err(|_| {
                MediaError::Relay(format!("{method}: no response in {REQUEST_TIMEOUT:?}"))
            })??;
        if let Some(s) = response.header("Session") {
            self.link.session = Some(session_id(s).to_string());
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

    /// `SETUP` the video track and take the channels the server assigns
    /// (it may answer with others than the ones asked for).
    async fn setup_video(&mut self, base: &Described) -> Result<(), MediaError> {
        let transport = format!("RTP/AVP/TCP;unicast;interleaved={RTP_CHANNEL}-{RTCP_CHANNEL}");
        let response = self
            .request("SETUP", &base.video_control, &[("Transport", &transport)])
            .await?;
        let answered = response.header("Transport").unwrap_or_default();
        self.link.channels = interleaved_channels(answered).unwrap_or((RTP_CHANNEL, RTCP_CHANNEL));
        debug!(
            transport = answered,
            rtp_channel = self.link.channels.0,
            "video track set up"
        );
        Ok(())
    }

    async fn play(&mut self, aggregate: &str) -> Result<(), MediaError> {
        self.request("PLAY", aggregate, &[("Range", "npt=0.000-")])
            .await?;
        Ok(())
    }
}

/// The write side of the session: keep-alives, receiver reports, acks of
/// server requests, `TEARDOWN` on a stop. The read half lives in its own
/// task ([`read_loop`]) so that no timer ever interrupts a frame mid-read.
async fn run(
    client: Client,
    aggregate: String,
    sink: LiveRtpSink,
    activity: Arc<RtpActivity>,
    first_rtp: Arc<Notify>,
    notifier: LiveLossNotifier,
    mut stop: oneshot::Receiver<()>,
) {
    let Client { reader, mut link } = client;
    let (acks, mut pending_acks) = mpsc::channel(ACK_QUEUE);
    let rtp_channel = link.channels.0;
    let mut reading = tokio::spawn(read_loop(
        reader,
        rtp_channel,
        sink,
        activity,
        first_rtp,
        acks,
    ));
    let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
    let mut rtcp = tokio::time::interval(RTCP_INTERVAL);
    keepalive.tick().await;
    rtcp.tick().await;
    let end = loop {
        let sent = tokio::select! {
            _ = &mut stop => break None,
            ended = &mut reading => break Some(read_loop_outcome(ended)),
            _ = keepalive.tick() => link
                .send("GET_PARAMETER", &aggregate, &[])
                .await
                .map_err(|e| format!("keep-alive: {e}")),
            _ = rtcp.tick() => link
                .send_rtcp_receiver_report()
                .await
                .map_err(|e| format!("rtcp: {e}")),
            ack = pending_acks.recv() => match ack {
                Some(cseq) => link.ack(&cseq).await.map_err(|e| format!("ack: {e}")),
                // The read loop is over; collect its outcome.
                None => break Some(read_loop_outcome((&mut reading).await)),
            },
        };
        if let Err(why) = sent {
            break Some((LiveLossReason::PeerDisconnected, why));
        }
    };
    reading.abort();
    match end {
        Some((reason, why)) => {
            warn!(%why, "watch-along relay ended");
            report_loss(&notifier, reason);
        }
        None => teardown(&mut link, &aggregate).await,
    }
}

fn read_loop_outcome(
    joined: Result<(LiveLossReason, String), tokio::task::JoinError>,
) -> (LiveLossReason, String) {
    joined.unwrap_or_else(|e| (LiveLossReason::PeerDisconnected, format!("read loop: {e}")))
}

/// Best-effort `TEARDOWN` on a stop: written, not awaited, since the
/// read half is already gone.
async fn teardown(link: &mut Link, aggregate: &str) {
    match tokio::time::timeout(TEARDOWN_GRACE, link.send("TEARDOWN", aggregate, &[])).await {
        Ok(Ok(())) => debug!("watch-along relay stopped"),
        Ok(Err(e)) => debug!(error = %e, "watch-along TEARDOWN not sent"),
        Err(_) => debug!("watch-along TEARDOWN not sent in time"),
    }
}

/// Owns the read half: forwards the video RTP into the sink, hands the
/// server's requests over for an ack, and ends with the loss to report.
async fn read_loop(
    mut reader: Reader,
    rtp_channel: u8,
    sink: LiveRtpSink,
    activity: Arc<RtpActivity>,
    first_rtp: Arc<Notify>,
    acks: mpsc::Sender<String>,
) -> (LiveLossReason, String) {
    let mut stats = FrameStats::default();
    let mut got_first = false;
    loop {
        match read_next(&mut reader, rtp_channel, &mut stats).await {
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
                if acks.send(cseq).await.is_err() {
                    return (LiveLossReason::PeerDisconnected, "writer gone".into());
                }
            }
            Ok(Incoming::Closed) => {
                return (
                    LiveLossReason::EndOfStream,
                    "server closed the stream".into(),
                );
            }
            Err(why) => return (LiveLossReason::PeerDisconnected, why),
        }
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

/// What the read loop has seen so far: the first frames are logged, and
/// a desync report says what came before the unexpected bytes.
#[derive(Default)]
struct FrameStats {
    frames: u64,
    bare_rtcp: u64,
    last: Option<(u8, usize)>,
}

impl FrameStats {
    fn record_bare_rtcp(&mut self, len: usize) {
        self.bare_rtcp += 1;
        if self.bare_rtcp == 1 {
            debug!(len, "bare RTCP packet (no interleaved framing) accepted");
        }
    }

    fn record(&mut self, channel: u8, payload: &[u8]) {
        self.frames += 1;
        self.last = Some((channel, payload.len()));
        if self.frames <= FIRST_FRAMES_LOGGED {
            debug!(
                channel,
                len = payload.len(),
                rtp = %rtp_summary(payload),
                "interleaved frame"
            );
        }
    }

    fn last_label(&self) -> String {
        self.last.map_or_else(
            || "none".to_string(),
            |(channel, len)| format!("channel {channel}, {len} bytes"),
        )
    }
}

/// One interleaved frame or one RTSP message.
async fn read_next(
    io: &mut Reader,
    rtp_channel: u8,
    stats: &mut FrameStats,
) -> Result<Incoming, String> {
    let mut first = [0u8; 1];
    match io.read(&mut first).await {
        Ok(0) => return Ok(Incoming::Closed),
        Ok(_) => {}
        Err(e) => return Err(format!("read: {e}")),
    }
    match first[0] {
        b'$' => read_frame(io, rtp_channel, stats).await,
        b if b.is_ascii_uppercase() => read_message(io, b, stats).await,
        b => read_bare_rtcp(io, b, stats).await,
    }
}

/// Arlo's server sends its periodic RTCP sender reports **without** the
/// interleaved framing (`80 c8 00 06 …` straight after an RTP frame,
/// seen 2026-10-01; only the first one, at `PLAY`, is framed). RTCP
/// carries its own length, so a bare packet is skipped by it; anything
/// else is a desync and ends the relay with a report.
async fn read_bare_rtcp(
    io: &mut Reader,
    first: u8,
    stats: &mut FrameStats,
) -> Result<Incoming, String> {
    let mut head = [0u8; RTCP_HEADER_LEN - 1];
    io.read_exact(&mut head)
        .await
        .map_err(|e| format!("bare packet header: {e}"))?;
    let Some(len) = bare_rtcp_len(first, head) else {
        let mut shown = head.to_vec();
        shown.extend_from_slice(io.fill_buf().await.map_err(|e| format!("peek: {e}"))?);
        return Err(desync_report(first, &shown, stats));
    };
    let mut rest = vec![0u8; len - RTCP_HEADER_LEN];
    io.read_exact(&mut rest)
        .await
        .map_err(|e| format!("bare rtcp: {e}"))?;
    stats.record_bare_rtcp(len);
    Ok(Incoming::Other)
}

async fn read_frame(
    io: &mut Reader,
    rtp_channel: u8,
    stats: &mut FrameStats,
) -> Result<Incoming, String> {
    let mut header = [0u8; 3];
    io.read_exact(&mut header)
        .await
        .map_err(|e| format!("frame header: {e}"))?;
    let (channel, len) = interleaved_header(header);
    let mut payload = vec![0u8; len];
    io.read_exact(&mut payload)
        .await
        .map_err(|e| format!("frame payload: {e}"))?;
    stats.record(channel, &payload);
    if channel == rtp_channel && rewrite_payload_type(&mut payload, LIVE_RTP_H264_PT) {
        return Ok(Incoming::Rtp(Bytes::from(payload)));
    }
    Ok(Incoming::Other)
}

/// An RTSP message whose first byte was already read: a response to a
/// keep-alive, or a request from the server.
async fn read_message(io: &mut Reader, first: u8, stats: &FrameStats) -> Result<Incoming, String> {
    let mut line = vec![first];
    io.read_until(b'\n', &mut line)
        .await
        .map_err(|e| format!("message line: {e}"))?;
    let Ok(start) = String::from_utf8(line.clone()) else {
        return Err(desync_report(first, &line[1..], stats));
    };
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
    if start.starts_with("RTSP/") {
        return Ok(Incoming::Other);
    }
    Ok(Incoming::ServerRequest {
        cseq: cseq.unwrap_or_else(|| "0".to_string()),
    })
}

async fn read_response(io: &mut Reader) -> Result<Response, MediaError> {
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

/// The `interleaved=a-b` pair of a `Transport` header, if any.
fn interleaved_channels(transport: &str) -> Option<(u8, u8)> {
    let field = transport
        .split(';')
        .map(str::trim)
        .find_map(|p| p.strip_prefix("interleaved="))?;
    let (data, control) = field.split_once('-')?;
    Some((data.trim().parse().ok()?, control.trim().parse().ok()?))
}

/// Total length of an RTCP packet whose header starts with these bytes
/// (version 2, a known packet type), or `None`.
fn bare_rtcp_len(first: u8, head: [u8; RTCP_HEADER_LEN - 1]) -> Option<usize> {
    let known_type = (RTCP_PT_FIRST..=RTCP_PT_LAST).contains(&head[0]);
    (first >> 6 == 2 && known_type)
        .then(|| (usize::from(u16::from_be_bytes([head[1], head[2]])) + 1) * RTCP_HEADER_LEN)
}

/// `v2 pt=96 seq=1234 m=1` for an RTP packet, `not-rtp` otherwise.
fn rtp_summary(packet: &[u8]) -> String {
    if packet.len() < 12 || packet[0] >> 6 != 2 {
        return "not-rtp".to_string();
    }
    format!(
        "v2 pt={} seq={} m={}",
        packet[1] & 0x7f,
        u16::from_be_bytes([packet[2], packet[3]]),
        u8::from(packet[1] & 0x80 != 0)
    )
}

/// Describe bytes that are neither a frame nor an RTSP message: what
/// came before them, a bounded hex dump, and where the next `$` is.
/// Stream bytes carry no secret; the report goes to the log as it is.
fn desync_report(first: u8, ahead: &[u8], stats: &FrameStats) -> String {
    let dump = ahead
        .iter()
        .take(DESYNC_DUMP_BYTES)
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    let marker = ahead.iter().position(|&b| b == b'$').map_or_else(
        || "none buffered".to_string(),
        |i| format!("{} bytes ahead", i + 1),
    );
    format!(
        "unexpected byte 0x{first:02x} after {} frames and {} bare RTCP packets (last frame: {}); next bytes: {dump}; next '$' {marker}",
        stats.frames,
        stats.bare_rtcp,
        stats.last_label()
    )
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
    fn interleaved_channels_reads_the_servers_transport_answer() {
        assert_eq!(
            interleaved_channels("RTP/AVP/TCP;unicast;interleaved=2-3;ssrc=1A2B"),
            Some((2, 3))
        );
        assert_eq!(interleaved_channels("RTP/AVP/TCP;unicast"), None);
        assert_eq!(interleaved_channels("interleaved=x-1"), None);
        assert_eq!(interleaved_channels(""), None);
    }

    #[test]
    fn rtp_summary_describes_rtp_and_flags_the_rest() {
        let packet = [0x80, 0x80 | 0x60, 0x12, 0x34, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(rtp_summary(&packet), "v2 pt=96 seq=4660 m=1");
        assert_eq!(rtp_summary(&[0x80, 96]), "not-rtp");
        assert_eq!(rtp_summary(&[0x40; 12]), "not-rtp");
    }

    #[test]
    fn desync_report_dumps_bounded_hex_and_locates_the_marker() {
        let mut stats = FrameStats::default();
        stats.record(0, &[0x80; 12]);
        let mut ahead = vec![0xABu8; 30];
        ahead[27] = b'$';
        let report = desync_report(0x01, &ahead, &stats);
        assert!(report.starts_with(
            "unexpected byte 0x01 after 1 frames and 0 bare RTCP packets (last frame: channel 0, 12 bytes)"
        ));
        assert_eq!(report.matches("ab").count(), DESYNC_DUMP_BYTES);
        assert!(report.ends_with("next '$' 28 bytes ahead"));
        assert!(desync_report(0x01, &[], &FrameStats::default()).contains("(last frame: none)"));
        assert!(desync_report(0x01, &[1, 2], &stats).ends_with("next '$' none buffered"));
    }

    #[test]
    fn bare_rtcp_len_accepts_known_rtcp_headers_only() {
        assert_eq!(bare_rtcp_len(0x80, [0xc8, 0x00, 0x06]), Some(28));
        assert_eq!(bare_rtcp_len(0x81, [0xca, 0x00, 0x05]), Some(24));
        assert_eq!(bare_rtcp_len(0x80, [0x60, 0x00, 0x06]), None); // RTP, not RTCP
        assert_eq!(bare_rtcp_len(0x40, [0xc8, 0x00, 0x06]), None); // version 1
        assert_eq!(bare_rtcp_len(0x80, [0xcd, 0x00, 0x06]), None); // unknown type
    }

    /// A reader over an in-memory stream holding `bytes`, then EOF.
    fn reader_fed_with(bytes: &[u8]) -> Reader {
        let (ours, mut theirs) = tokio::io::duplex(4096);
        let io: Box<dyn Io> = Box::new(ours);
        let (read, _write) = tokio::io::split(io);
        let data = bytes.to_vec();
        tokio::spawn(async move { theirs.write_all(&data).await });
        BufReader::new(read)
    }

    #[tokio::test]
    async fn read_next_handles_frames_bare_rtcp_messages_and_reports_a_desync() {
        let rtp = [0x80, 0x60, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0xAA];
        let mut stream = interleaved_frame(0, &rtp);
        stream.extend_from_slice(&interleaved_frame(1, &rtcp_receiver_report(7)));
        // A bare sender report, as Arlo's server emits them.
        stream.extend_from_slice(&[0x80, 0xc8, 0x00, 0x06]);
        stream.extend_from_slice(&[0u8; 24]);
        stream.extend_from_slice(b"GET_PARAMETER rtsp://h RTSP/1.0\r\nCSeq: 9\r\n\r\n");
        stream.extend_from_slice(b"RTSP/1.0 200 OK\r\nCSeq: 3\r\nContent-Length: 2\r\n\r\nok");
        stream.extend_from_slice(&[0x01, 0x02, 0x03, b'$']);
        let mut reader = reader_fed_with(&stream);
        let mut stats = FrameStats::default();

        let Ok(Incoming::Rtp(packet)) = read_next(&mut reader, 0, &mut stats).await else {
            panic!("first frame is the RTP packet");
        };
        assert_eq!(packet[1] & 0x7f, u8::try_from(LIVE_RTP_H264_PT).unwrap());
        assert!(matches!(
            read_next(&mut reader, 0, &mut stats).await,
            Ok(Incoming::Other)
        ));
        assert!(matches!(
            read_next(&mut reader, 0, &mut stats).await,
            Ok(Incoming::Other)
        ));
        assert_eq!(stats.bare_rtcp, 1);
        let Ok(Incoming::ServerRequest { cseq }) = read_next(&mut reader, 0, &mut stats).await
        else {
            panic!("server request is handed over for an ack");
        };
        assert_eq!(cseq, "9");
        assert!(matches!(
            read_next(&mut reader, 0, &mut stats).await,
            Ok(Incoming::Other)
        ));
        let Err(why) = read_next(&mut reader, 0, &mut stats).await else {
            panic!("stray bytes end the loop");
        };
        assert!(
            why.starts_with("unexpected byte 0x01 after 2 frames and 1 bare RTCP packets"),
            "{why}"
        );
        assert!(why.contains("02 03 24"), "{why}");
        assert!(why.ends_with("next '$' 3 bytes ahead"), "{why}");
    }

    #[test]
    fn rtcp_receiver_report_is_a_valid_empty_rr() {
        let rr = rtcp_receiver_report(0x0102_0304);
        assert_eq!(rr, [0x80, 201, 0, 1, 1, 2, 3, 4]);
    }
}
