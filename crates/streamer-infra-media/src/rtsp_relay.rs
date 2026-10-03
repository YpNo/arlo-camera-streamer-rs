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
//! TLS: the certificate chain is verified against the system roots, and
//! only the hostname check is waived — the URL names a raw IP no
//! certificate can match. An operator can pin the end-entity certificate
//! instead (`arlo.watch_along_cert_sha256`) when Arlo's chain is not
//! public; the relay then refuses anything else. See [`RelayTls`].
//!
//! The server is not trusted by the parser either: every text line,
//! header count and `Content-Length` is bounded, and the binary framing
//! resyncs on the quirks Arlo's server has (bare RTCP, bare AAC).
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

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
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
use crate::live_rtp_sink::{AacRtpFormat, LiveSinks};
use crate::live_watch::{RtpActivity, report_loss, spawn_stall_watchdog};
use crate::pipeline_desc::{LIVE_RTP_AAC_PT, LIVE_RTP_H264_PT};

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
/// Longest RTSP text line accepted from the server (status, header).
const MAX_LINE_BYTES: usize = 8 * 1024;
/// Most header lines accepted in one RTSP message.
const MAX_HEADERS: usize = 64;
/// Largest message body accepted (`Content-Length`); an SDP is < 2 KiB.
const MAX_MESSAGE_BODY: usize = 64 * 1024;
/// RTCP packet types (RFC 3550 §12.1): SR, RR, SDES, BYE, APP.
const RTCP_PT_FIRST: u8 = 200;
const RTCP_PT_LAST: u8 = 204;
/// An RTCP header: V/P/count, PT, 16-bit length in words minus one.
const RTCP_HEADER_LEN: usize = 4;
/// A fixed RTP header.
const RTP_HEADER_LEN: usize = 12;
/// `$`, channel, 16-bit length.
const INTERLEAVED_HEADER_LEN: usize = 4;
/// Bytes a resync looks at before trusting a `$` header: the framing
/// plus the packet's version and payload-type bytes.
const FRAME_PROBE_LEN: usize = INTERLEAVED_HEADER_LEN + 2;
/// Unframed bytes a resync skips before giving the stream up.
const RESYNC_LIMIT: usize = 64 * 1024;
/// Interleaved channels asked for in `SETUP`: video, then audio.
const RTP_CHANNEL: u8 = 0;
const RTCP_CHANNEL: u8 = 1;
const AUDIO_RTP_CHANNEL: u8 = 2;
const AUDIO_RTCP_CHANNEL: u8 = 3;
/// RFC 3640 `AAC-hbr` AU-header widths; the bare-packet fallback needs them.
const AAC_HBR_SIZE_LENGTH: u32 = 13;
const AAC_HBR_INDEX_LENGTH: u32 = 3;
/// A bare AAC packet: RTP header, AU-headers-length, one AU header.
const BARE_AAC_HEAD_LEN: usize = RTP_HEADER_LEN + 4;
/// Largest bare AAC packet taken at face value (one AU of 16 kHz AAC-LC
/// is a few hundred bytes).
const BARE_AAC_MAX_LEN: usize = 4096;
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
        sinks: LiveSinks,
        stall_timeout: Duration,
        notifier: LiveLossNotifier,
        tls: &RelayTls,
    ) -> Result<Self, MediaError> {
        let mut client = Client::connect(url, tls).await?;
        let base = client.describe(url.as_str()).await?;
        client.setup_video(&base).await?;
        if let Some(audio) = &base.audio {
            sinks.aac.configure(audio.format.clone());
            client.setup_audio(audio).await;
        }
        client.play(&base.aggregate).await?;
        info!(url = %url, "watch-along stream playing; awaiting first video RTP");

        let activity = Arc::new(RtpActivity::new(Instant::now()));
        let first_rtp = Arc::new(Notify::new());
        let (stop_tx, stop_rx) = oneshot::channel();
        let aac_bare = base
            .audio
            .as_ref()
            .is_some_and(|a| hbr_default_widths(&a.format));
        let task = tokio::spawn(run(
            client,
            base.aggregate,
            sinks,
            aac_bare,
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
    /// The AAC audio track, when the SDP describes one we can decode.
    audio: Option<AudioTrack>,
}

/// An RFC 3640 AAC track of the SDP: its control URL and packetisation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct AudioTrack {
    control: String,
    format: AacRtpFormat,
}

/// Interleaved channel pairs (RTP, RTCP) the server assigned per track.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Channels {
    video: (u8, u8),
    audio: Option<(u8, u8)>,
}

/// What a channel number carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lane {
    VideoRtp,
    AudioRtp,
    Rtcp,
    Unknown,
}

impl Channels {
    const fn lane(self, channel: u8) -> Lane {
        if channel == self.video.0 {
            Lane::VideoRtp
        } else if channel == self.video.1 {
            Lane::Rtcp
        } else if let Some((rtp, rtcp)) = self.audio {
            if channel == rtp {
                Lane::AudioRtp
            } else if channel == rtcp {
                Lane::Rtcp
            } else {
                Lane::Unknown
            }
        } else {
            Lane::Unknown
        }
    }
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
    /// Interleaved channels per track — ours until the server's `SETUP`
    /// answers assign others.
    channels: Channels,
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
        let frame = interleaved_frame(self.channels.video.1, &report);
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

/// Whether `host` (as `url::Url::host_str` gives it, IPv6 in brackets)
/// is this machine's loopback.
fn is_loopback_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback())
}

impl Client {
    async fn connect(url: &WatchAlongUrl, tls_policy: &RelayTls) -> Result<Self, MediaError> {
        let parsed = url::Url::parse(url.as_str())
            .map_err(|e| MediaError::Relay(format!("watch-along URL: {e}")))?;
        let host = parsed
            .host_str()
            .ok_or_else(|| MediaError::Relay("watch-along URL has no host".into()))?
            .to_string();
        let tls = parsed.scheme() == "rtsps";
        // The URL carries the egress token: plaintext is refused unless
        // the policy allows it, and then only to this machine's loopback.
        if !tls {
            if !(tls_policy.plaintext_loopback && is_loopback_host(&host)) {
                return Err(MediaError::Relay(
                    "watch-along URL is plaintext rtsp; refused (rtsps:// expected)".into(),
                ));
            }
            warn!(
                "dialing plaintext rtsp to loopback; the egress token travels in clear on this host"
            );
        }
        let port = parsed.port().unwrap_or(if tls { 443 } else { 554 });
        let tcp = tokio::time::timeout(REQUEST_TIMEOUT, TcpStream::connect((host.as_str(), port)))
            .await
            .map_err(|_| MediaError::Relay(format!("connect to {host}:{port} timed out")))?
            .map_err(|e| MediaError::Relay(format!("connect to {host}:{port}: {e}")))?;
        let io: Box<dyn Io> = if tls {
            Box::new(tls_handshake(tcp, &host, tls_policy).await?)
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
                channels: Channels {
                    video: (RTP_CHANNEL, RTCP_CHANNEL),
                    audio: None,
                },
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
        let audio = audio_track(&sdp).map(|track| AudioTrack {
            control: join_control(&aggregate, &track.control),
            format: track.format,
        });
        if audio.is_none() {
            debug!("SDP has no AAC audio track; relaying the video only");
        }
        Ok(Described {
            video_control: join_control(&aggregate, &control),
            aggregate,
            audio,
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
        let pair = interleaved_channels(answered).unwrap_or((RTP_CHANNEL, RTCP_CHANNEL));
        if pair.0 == pair.1 {
            return Err(MediaError::Relay(
                "server assigned one interleaved channel to both RTP and RTCP".into(),
            ));
        }
        self.link.channels.video = pair;
        debug!(
            transport = answered,
            rtp_channel = pair.0,
            "video track set up"
        );
        Ok(())
    }

    /// `SETUP` the AAC track on the next channel pair. A refusal keeps
    /// the relay video-only rather than failing it: the audio is a
    /// bonus, and Arlo's server pushes it unframed anyway (see
    /// [`read_unframed`]).
    async fn setup_audio(&mut self, track: &AudioTrack) {
        let transport =
            format!("RTP/AVP/TCP;unicast;interleaved={AUDIO_RTP_CHANNEL}-{AUDIO_RTCP_CHANNEL}");
        match self
            .request("SETUP", &track.control, &[("Transport", &transport)])
            .await
        {
            Ok(response) => {
                let answered = response.header("Transport").unwrap_or_default();
                let pair = interleaved_channels(answered)
                    .unwrap_or((AUDIO_RTP_CHANNEL, AUDIO_RTCP_CHANNEL));
                if !channel_pairs_disjoint(self.link.channels.video, pair) {
                    warn!(
                        transport = answered,
                        "audio channels overlap the video's; relaying the video only"
                    );
                    return;
                }
                self.link.channels.audio = Some(pair);
                debug!(
                    transport = answered,
                    rtp_channel = pair.0,
                    "audio track set up"
                );
            }
            Err(e) => warn!(error = %e, "audio track refused; relaying the video only"),
        }
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
    sinks: LiveSinks,
    aac_bare: bool,
    activity: Arc<RtpActivity>,
    first_rtp: Arc<Notify>,
    notifier: LiveLossNotifier,
    mut stop: oneshot::Receiver<()>,
) {
    let Client { reader, mut link } = client;
    let (acks, mut pending_acks) = mpsc::channel(ACK_QUEUE);
    let channels = link.channels;
    // Abort-on-drop: if this task is itself aborted (the shutdown grace
    // timer) while a write below blocks, the read half must not live on
    // with the socket.
    let mut reading = AbortOnDrop(tokio::spawn(read_loop(
        reader, channels, sinks, aac_bare, activity, first_rtp, acks,
    )));
    let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
    let mut rtcp = tokio::time::interval(RTCP_INTERVAL);
    keepalive.tick().await;
    rtcp.tick().await;
    let end = loop {
        let sent = tokio::select! {
            _ = &mut stop => break None,
            ended = &mut reading.0 => break Some(read_loop_outcome(ended)),
            _ = keepalive.tick() => {
                timed_write("keep-alive", link.send("GET_PARAMETER", &aggregate, &[])).await
            }
            _ = rtcp.tick() => timed_write("rtcp", link.send_rtcp_receiver_report()).await,
            ack = pending_acks.recv() => match ack {
                Some(cseq) => timed_write("ack", link.ack(&cseq)).await,
                // The read loop is over; collect its outcome.
                None => break Some(read_loop_outcome((&mut reading.0).await)),
            },
        };
        if let Err(why) = sent {
            break Some((LiveLossReason::PeerDisconnected, why));
        }
    };
    drop(reading);
    match end {
        Some((reason, why)) => {
            warn!(%why, "watch-along relay ended");
            report_loss(&notifier, reason);
        }
        None => teardown(&mut link, &aggregate).await,
    }
}

/// A write to the server bounded by `REQUEST_TIMEOUT`: a peer that stops
/// reading must not hold the writer (and with it the relay) forever.
async fn timed_write(
    what: &str,
    write: impl std::future::Future<Output = std::io::Result<()>>,
) -> Result<(), String> {
    match tokio::time::timeout(REQUEST_TIMEOUT, write).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(format!("{what}: {e}")),
        Err(_) => Err(format!("{what}: write timed out after {REQUEST_TIMEOUT:?}")),
    }
}

/// Aborts the task when dropped, so a cancelled owner takes it along.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
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
    channels: Channels,
    sinks: LiveSinks,
    aac_bare: bool,
    activity: Arc<RtpActivity>,
    first_rtp: Arc<Notify>,
    acks: mpsc::Sender<String>,
) -> (LiveLossReason, String) {
    let mut stats = FrameStats {
        aac_bare,
        ..FrameStats::default()
    };
    let mut got_first = false;
    loop {
        match read_next(&mut reader, channels, &mut stats).await {
            Ok(Incoming::Rtp(packet)) => {
                activity.touch(Instant::now());
                if !got_first {
                    got_first = true;
                    first_rtp.notify_one();
                }
                let _ = sinks.video.push(packet);
            }
            Ok(Incoming::Audio(packet)) => {
                let _ = sinks.aac.push(packet);
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
    /// An AAC audio RTP packet, payload type already rewritten.
    Audio(Bytes),
    /// RTCP, an unknown channel, or a response to one of our keep-alives.
    Other,
    /// A request from the server to acknowledge.
    ServerRequest {
        cseq: String,
    },
    Closed,
}

/// What the read loop has seen so far: the first frames and resyncs are
/// logged, and a desync report says what came before the stray bytes.
#[derive(Default)]
struct FrameStats {
    frames: u64,
    bare_rtcp: u64,
    bare_aac: u64,
    resyncs: u64,
    last: Option<(u8, usize)>,
    /// Payload type of the stream's video RTP, learnt from its first
    /// packet; a resync trusts no `$` header whose packet carries another.
    rtp_pt: Option<u8>,
    /// Whether bare packets may be AAC-hbr audio worth parsing (the SDP
    /// described an AAC track with the default AU-header widths).
    aac_bare: bool,
}

impl FrameStats {
    fn record(&mut self, channel: u8, lane: Lane, payload: &[u8]) {
        self.frames += 1;
        self.last = Some((channel, payload.len()));
        if lane == Lane::VideoRtp && self.rtp_pt.is_none() && is_rtp(payload) {
            self.rtp_pt = Some(payload[1] & 0x7f);
        }
        if self.frames <= FIRST_FRAMES_LOGGED {
            debug!(
                channel,
                len = payload.len(),
                rtp = %rtp_summary(payload),
                "interleaved frame"
            );
        }
    }

    fn record_bare_aac(&mut self, len: usize) {
        self.bare_aac += 1;
        if self.bare_aac == 1 {
            debug!(
                len,
                "bare AAC audio packet (no interleaved framing) relayed"
            );
        }
    }

    fn record_bare_rtcp(&mut self, len: usize) {
        self.bare_rtcp += 1;
        if self.bare_rtcp == 1 {
            debug!(len, "bare RTCP packet (no interleaved framing) accepted");
        }
    }

    fn record_resync(&mut self, skipped: &[u8]) {
        self.resyncs += 1;
        if self.resyncs <= FIRST_FRAMES_LOGGED {
            debug!(
                skipped = skipped.len(),
                bytes = %hex(skipped),
                "unframed bytes skipped; frame boundary found again"
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
    channels: Channels,
    stats: &mut FrameStats,
) -> Result<Incoming, String> {
    let mut first = [0u8; 1];
    match io.read(&mut first).await {
        Ok(0) => return Ok(Incoming::Closed),
        Ok(_) => {}
        Err(e) => return Err(format!("read: {e}")),
    }
    match first[0] {
        b'$' => read_frame(io, channels, stats).await,
        b if b.is_ascii_uppercase() => read_message(io, b, stats).await,
        b => read_unframed(io, b, channels, stats).await,
    }
}

async fn read_frame(
    io: &mut Reader,
    channels: Channels,
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
    Ok(classify(channel, payload, channels, stats))
}

/// The frame's place in the relay: video RTP to the video sink, AAC RTP
/// to the audio sink, the rest noted and dropped.
fn classify(
    channel: u8,
    mut payload: Vec<u8>,
    channels: Channels,
    stats: &mut FrameStats,
) -> Incoming {
    let lane = channels.lane(channel);
    stats.record(channel, lane, &payload);
    match lane {
        Lane::VideoRtp if rewrite_payload_type(&mut payload, LIVE_RTP_H264_PT) => {
            Incoming::Rtp(Bytes::from(payload))
        }
        Lane::AudioRtp if rewrite_payload_type(&mut payload, LIVE_RTP_AAC_PT) => {
            Incoming::Audio(Bytes::from(payload))
        }
        _ => Incoming::Other,
    }
}

/// Arlo's server sends some packets **without** the interleaved framing
/// (seen 2026-10-01): its periodic RTCP sender reports (`80 c8 00 06 …`,
/// only the first one at `PLAY` is framed), and the AAC audio track's
/// RTP (`80 80 00 01 … 00 10 0e 00 …`, payload type 0), with or without
/// a `SETUP` for it. RTCP carries its own length and is skipped by it;
/// an AAC-hbr packet carries its AU size and is relayed as audio
/// ([`bare_aac_len`]); everything else is skipped up to the next `$`
/// header that checks out ([`resync`]).
async fn read_unframed(
    io: &mut Reader,
    first: u8,
    channels: Channels,
    stats: &mut FrameStats,
) -> Result<Incoming, String> {
    let mut head = [0u8; RTCP_HEADER_LEN - 1];
    io.read_exact(&mut head)
        .await
        .map_err(|e| format!("bare packet header: {e}"))?;
    if let Some(len) = bare_rtcp_len(first, head) {
        let mut rest = vec![0u8; len - RTCP_HEADER_LEN];
        io.read_exact(&mut rest)
            .await
            .map_err(|e| format!("bare rtcp: {e}"))?;
        stats.record_bare_rtcp(len);
        return Ok(Incoming::Other);
    }
    let mut carried = vec![first];
    carried.extend_from_slice(&head);
    if stats.aac_bare && first >> 6 == 2 {
        carried.resize(BARE_AAC_HEAD_LEN, 0);
        io.read_exact(&mut carried[RTCP_HEADER_LEN..])
            .await
            .map_err(|e| format!("bare packet head: {e}"))?;
        if let Some(len) = bare_aac_len(&carried) {
            carried.resize(len, 0);
            io.read_exact(&mut carried[BARE_AAC_HEAD_LEN..])
                .await
                .map_err(|e| format!("bare aac: {e}"))?;
            stats.record_bare_aac(len);
            rewrite_payload_type(&mut carried, LIVE_RTP_AAC_PT);
            return Ok(Incoming::Audio(Bytes::from(carried)));
        }
    }
    resync(io, carried, channels, stats).await
}

/// Skip bytes, `carried` first, until a `$` header that
/// [`frame_header_at`] accepts, then read that frame. Gives up with a
/// desync report after `RESYNC_LIMIT` bytes.
async fn resync(
    io: &mut Reader,
    carried: Vec<u8>,
    channels: Channels,
    stats: &mut FrameStats,
) -> Result<Incoming, String> {
    let mut window = carried;
    let mut skipped = Vec::new();
    loop {
        while window.len() < FRAME_PROBE_LEN {
            let mut byte = [0u8; 1];
            match io.read(&mut byte).await {
                Ok(0) => return Ok(Incoming::Closed),
                Ok(_) => window.push(byte[0]),
                Err(e) => return Err(format!("resync read: {e}")),
            }
        }
        if let Some((channel, len)) = frame_header_at(&window, channels, stats.rtp_pt) {
            stats.record_resync(&skipped);
            let mut payload = window.split_off(INTERLEAVED_HEADER_LEN);
            let probed = payload.len();
            payload.resize(len, 0);
            io.read_exact(&mut payload[probed..])
                .await
                .map_err(|e| format!("frame payload after resync: {e}"))?;
            return Ok(classify(channel, payload, channels, stats));
        }
        skipped.push(window.remove(0));
        if skipped.len() > RESYNC_LIMIT {
            return Err(desync_report(skipped[0], &skipped[1..], stats));
        }
    }
}

/// `(channel, length)` when `window` starts with a `$` header worth
/// trusting: one of our channels, a length that holds a packet header, a
/// version-2 packet, and on the video RTP channel the stream's payload
/// type (once known), on an RTCP channel a known RTCP type.
fn frame_header_at(window: &[u8], channels: Channels, rtp_pt: Option<u8>) -> Option<(u8, usize)> {
    if window.len() < FRAME_PROBE_LEN || window[0] != b'$' {
        return None;
    }
    let (channel, len) = interleaved_header([window[1], window[2], window[3]]);
    let (version, pt) = (window[4] >> 6, window[5] & 0x7f);
    let plausible = match channels.lane(channel) {
        Lane::VideoRtp => len >= RTP_HEADER_LEN && rtp_pt.is_none_or(|known| pt == known),
        Lane::AudioRtp => len >= RTP_HEADER_LEN,
        Lane::Rtcp => len >= RTCP_HEADER_LEN && (RTCP_PT_FIRST..=RTCP_PT_LAST).contains(&window[5]),
        Lane::Unknown => false,
    };
    (version == 2 && plausible).then_some((channel, len))
}

/// An RTSP message whose first byte was already read: a response to a
/// keep-alive, or a request from the server.
async fn read_message(io: &mut Reader, first: u8, stats: &FrameStats) -> Result<Incoming, String> {
    let mut line = vec![first];
    read_text_line(io, &mut line)
        .await
        .map_err(|e| format!("message line: {e}"))?;
    let Ok(start) = String::from_utf8(line.clone()) else {
        return Err(desync_report(first, &line[1..], stats));
    };
    let headers = read_headers(io)
        .await
        .map_err(|e| format!("message header: {e}"))?;
    let content_length = body_length(&headers)?;
    if content_length > 0 {
        let mut body = vec![0u8; content_length];
        io.read_exact(&mut body)
            .await
            .map_err(|e| format!("message body: {e}"))?;
    }
    if start.starts_with("RTSP/") {
        return Ok(Incoming::Other);
    }
    let cseq = header_value(&headers, "CSeq").map_or_else(|| "0".to_string(), str::to_string);
    Ok(Incoming::ServerRequest { cseq })
}

async fn read_response(io: &mut Reader) -> Result<Response, MediaError> {
    let mut raw = Vec::new();
    read_text_line(io, &mut raw)
        .await
        .map_err(|e| MediaError::Relay(format!("status line: {e}")))?;
    let code = parse_status_code(&String::from_utf8_lossy(&raw))
        .ok_or_else(|| MediaError::Relay("malformed RTSP status line".into()))?;
    let headers = read_headers(io)
        .await
        .map_err(|e| MediaError::Relay(format!("header: {e}")))?;
    let len = body_length(&headers).map_err(MediaError::Relay)?;
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

/// One `\n`-terminated text line appended to `buf`, at most
/// `MAX_LINE_BYTES` long; a server that never sends the newline is cut
/// off there instead of growing the buffer at link rate.
async fn read_text_line(io: &mut Reader, buf: &mut Vec<u8>) -> Result<(), String> {
    let start = buf.len();
    let limit = u64::try_from(MAX_LINE_BYTES + 1).unwrap_or(u64::MAX);
    (&mut *io)
        .take(limit)
        .read_until(b'\n', buf)
        .await
        .map_err(|e| format!("line read: {e}"))?;
    if buf.len() - start > MAX_LINE_BYTES {
        return Err(format!("RTSP line longer than {MAX_LINE_BYTES} bytes"));
    }
    Ok(())
}

/// Header lines up to the empty line, at most `MAX_HEADERS` of them.
async fn read_headers(io: &mut Reader) -> Result<Vec<(String, String)>, String> {
    let mut headers = Vec::new();
    loop {
        let mut raw = Vec::new();
        read_text_line(io, &mut raw).await?;
        if raw.is_empty() {
            return Err("connection closed inside the headers".into());
        }
        let line = String::from_utf8(raw).map_err(|_| "header line is not text".to_string())?;
        let h = line.trim_end();
        if h.is_empty() {
            return Ok(headers);
        }
        if headers.len() >= MAX_HEADERS {
            return Err(format!("more than {MAX_HEADERS} headers"));
        }
        if let Some((k, v)) = h.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
}

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// The body length a message announces, bounded: the server's number
/// sizes an allocation, so it is never taken at face value.
fn body_length(headers: &[(String, String)]) -> Result<usize, String> {
    let Some(value) = header_value(headers, "Content-Length") else {
        return Ok(0);
    };
    let len: usize = value
        .parse()
        .map_err(|_| "Content-Length is not a number".to_string())?;
    if len > MAX_MESSAGE_BODY {
        return Err(format!(
            "Content-Length {len} over the {MAX_MESSAGE_BODY}-byte limit"
        ));
    }
    Ok(len)
}

async fn tls_handshake(
    tcp: TcpStream,
    host: &str,
    policy: &RelayTls,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, MediaError> {
    let provider = rustls::crypto::ring::default_provider();
    let config = rustls::ClientConfig::builder_with_provider(provider.into())
        .with_safe_default_protocol_versions()
        .map_err(|e| MediaError::Relay(format!("TLS config: {e}")))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(policy.verifier()))
        .with_no_client_auth();
    let name = ServerName::try_from(host.to_string())
        .map_err(|e| MediaError::Relay(format!("TLS server name: {e}")))?;
    tokio::time::timeout(
        REQUEST_TIMEOUT,
        TlsConnector::from(Arc::new(config)).connect(name, tcp),
    )
    .await
    .map_err(|_| MediaError::Relay("TLS handshake timed out".into()))?
    .map_err(|e| MediaError::Relay(format!("TLS handshake: {e}")))
}

/// How the relay authenticates the watch-along host (ADR 0007).
///
/// Default: the certificate chain is verified against the system roots
/// and only the hostname check is waived, because Arlo hands out a raw
/// IP that no certificate names. Pinned: the end-entity certificate's
/// SHA-256 must match (`arlo.watch_along_cert_sha256`), for a chain the
/// system does not trust; the log prints the presented fingerprint when
/// a chain is refused, so the operator can copy it.
#[derive(Clone)]
pub struct RelayTls {
    chain: Arc<WebPkiServerVerifier>,
    pinned: Option<[u8; 32]>,
    /// Whether a plaintext `rtsp://` URL may be dialed when its host is
    /// this machine's loopback. Off in production; the integration tests
    /// relay from the crate's own server.
    plaintext_loopback: bool,
}

impl fmt::Debug for RelayTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RelayTls")
            .field("pinned", &self.pinned.map(fingerprint_hex))
            .field("plaintext_loopback", &self.plaintext_loopback)
            .finish_non_exhaustive()
    }
}

impl RelayTls {
    /// Chain verification with the hostname waived, or a certificate pin
    /// when `pinned_cert_sha256` (64 hex digits) is given.
    ///
    /// # Errors
    ///
    /// [`MediaError::Relay`] when the pin is not 32 bytes of hex or the
    /// system has no root certificates to verify against.
    pub fn from_config(pinned_cert_sha256: Option<&str>) -> Result<Self, MediaError> {
        let pinned = pinned_cert_sha256.map(parse_fingerprint).transpose()?;
        let loaded = rustls_native_certs::load_native_certs();
        for e in &loaded.errors {
            debug!(error = %e, "a system root certificate could not be loaded");
        }
        let mut roots = rustls::RootCertStore::empty();
        roots.add_parsable_certificates(loaded.certs);
        if roots.is_empty() {
            return Err(MediaError::Relay(
                "no system root certificates found for the watch-along TLS".into(),
            ));
        }
        let provider = rustls::crypto::ring::default_provider();
        let chain = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.into())
            .build()
            .map_err(|e| MediaError::Relay(format!("TLS verifier: {e}")))?;
        Ok(Self {
            chain,
            pinned,
            plaintext_loopback: false,
        })
    }

    /// Allow a plaintext `rtsp://` URL whose host is loopback. For tests
    /// and a local relay only: the URL carries the egress token, which
    /// then travels in clear on this host. Every such dial is logged.
    #[must_use]
    pub fn allowing_plaintext_to_loopback(mut self) -> Self {
        self.plaintext_loopback = true;
        self
    }

    fn verifier(&self) -> WatchAlongVerifier {
        WatchAlongVerifier {
            chain: self.chain.clone(),
            pinned: self.pinned,
        }
    }
}

/// Chain-verifying certificate check with the hostname waived, or a pin.
#[derive(Debug)]
struct WatchAlongVerifier {
    chain: Arc<WebPkiServerVerifier>,
    pinned: Option<[u8; 32]>,
}

impl ServerCertVerifier for WatchAlongVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let presented = cert_sha256(end_entity);
        if let Some(pin) = self.pinned {
            if pin == presented {
                return Ok(ServerCertVerified::assertion());
            }
            warn!(
                sha256 = %fingerprint_hex(presented),
                "watch-along certificate does not match arlo.watch_along_cert_sha256"
            );
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ));
        }
        match self.chain.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Ok(verified) => Ok(verified),
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForName
                | rustls::CertificateError::NotValidForNameContext { .. },
            )) => {
                debug!("watch-along certificate chain trusted; hostname check waived (raw IP)");
                Ok(ServerCertVerified::assertion())
            }
            Err(e) => {
                warn!(
                    error = %e,
                    sha256 = %fingerprint_hex(presented),
                    "watch-along certificate chain not trusted; set arlo.watch_along_cert_sha256 to pin it"
                );
                Err(e)
            }
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.chain.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.chain.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.chain.supported_verify_schemes()
    }
}

/// SHA-256 of a certificate's DER, the fingerprint `openssl x509
/// -fingerprint -sha256` prints (without the colons).
fn cert_sha256(cert: &CertificateDer<'_>) -> [u8; 32] {
    let digest = ring::digest::digest(&ring::digest::SHA256, cert.as_ref());
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    out
}

fn fingerprint_hex(bytes: [u8; 32]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::with_capacity(64), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// 64 hex digits (colons tolerated) → 32 bytes.
fn parse_fingerprint(text: &str) -> Result<[u8; 32], MediaError> {
    let hex: String = text.chars().filter(|c| *c != ':').collect();
    if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(MediaError::Relay(
            "arlo.watch_along_cert_sha256 must be 64 hex digits (SHA-256 of the certificate)"
                .into(),
        ));
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| MediaError::Relay("invalid hex in arlo.watch_along_cert_sha256".into()))?;
    }
    Ok(out)
}

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

/// The first `m=audio` section whose payload is RFC 3640 `MPEG4-GENERIC`
/// with an `AudioSpecificConfig` (`config=`): its control and format.
/// `sizelength` / `indexlength` / `indexdeltalength` default to the
/// `AAC-hbr` widths when the `fmtp` omits them.
fn audio_track(sdp: &str) -> Option<AudioTrack> {
    let mut in_audio = false;
    let mut control = None;
    let mut rtpmap: Option<(u32, u32)> = None;
    let mut fmtp: Option<String> = None;
    for line in sdp.lines().map(str::trim_end) {
        if let Some(media) = line.strip_prefix("m=") {
            if in_audio && rtpmap.is_some() && fmtp.is_some() {
                break;
            }
            in_audio = media.starts_with("audio ");
            control = None;
            rtpmap = None;
            fmtp = None;
            continue;
        }
        if !in_audio {
            continue;
        }
        if let Some(c) = line.strip_prefix("a=control:") {
            control = Some(c.trim().to_string());
        } else if let Some(map) = line.strip_prefix("a=rtpmap:") {
            rtpmap = aac_rtpmap(map);
        } else if let Some(f) = line.strip_prefix("a=fmtp:") {
            fmtp = f.split_once(' ').map(|(_, params)| params.to_string());
        }
    }
    if !in_audio {
        return None;
    }
    let (clock_rate, channels) = rtpmap?;
    let params = fmtp?;
    let param = |key: &str| -> Option<String> {
        params.split(';').map(str::trim).find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            k.trim()
                .eq_ignore_ascii_case(key)
                .then(|| v.trim().to_string())
        })
    };
    if param("mode").is_some_and(|m| !m.eq_ignore_ascii_case("AAC-hbr")) {
        return None;
    }
    let width =
        |key: &str, default: u32| param(key).and_then(|v| v.parse().ok()).unwrap_or(default);
    Some(AudioTrack {
        control: control.unwrap_or_else(|| "*".to_string()),
        format: AacRtpFormat {
            clock_rate,
            channels,
            config: valid_aac_config(param("config")?)?,
            size_length: width("sizelength", AAC_HBR_SIZE_LENGTH),
            index_length: width("indexlength", AAC_HBR_INDEX_LENGTH),
            index_delta_length: width("indexdeltalength", AAC_HBR_INDEX_LENGTH),
        },
    })
}

/// Longest `config=` (`AudioSpecificConfig` hex) accepted from an SDP.
const MAX_AAC_CONFIG_HEX: usize = 256;

/// The `config=` value only if it is what the caps expect — hex of an
/// `AudioSpecificConfig` — so a server cannot smuggle caps syntax
/// (`,`, `;`, quotes) through it.
fn valid_aac_config(config: String) -> Option<String> {
    let ok = !config.is_empty()
        && config.len().is_multiple_of(2)
        && config.len() <= MAX_AAC_CONFIG_HEX
        && config.bytes().all(|b| b.is_ascii_hexdigit());
    ok.then_some(config)
}

/// Whether two interleaved channel pairs can coexist: each pair uses two
/// distinct channels and the pairs share none.
fn channel_pairs_disjoint(a: (u8, u8), b: (u8, u8)) -> bool {
    a.0 != a.1 && b.0 != b.1 && a.0 != b.0 && a.0 != b.1 && a.1 != b.0 && a.1 != b.1
}

/// `(clock rate, channels)` of an `rtpmap` value naming `MPEG4-GENERIC`.
fn aac_rtpmap(map: &str) -> Option<(u32, u32)> {
    let (_, encoding) = map.trim().split_once(' ')?;
    let mut parts = encoding.split('/');
    if !parts.next()?.eq_ignore_ascii_case("MPEG4-GENERIC") {
        return None;
    }
    let clock_rate = parts.next()?.parse().ok()?;
    let channels = parts.next().map_or(Some(1), |c| c.parse().ok())?;
    Some((clock_rate, channels))
}

/// Whether the bare-packet fallback can read the AU size: it assumes the
/// `AAC-hbr` widths (13-bit size, 3-bit index).
fn hbr_default_widths(format: &AacRtpFormat) -> bool {
    format.size_length == AAC_HBR_SIZE_LENGTH && format.index_length == AAC_HBR_INDEX_LENGTH
}

/// Total length of a bare `AAC-hbr` RTP packet whose first
/// `BARE_AAC_HEAD_LEN` bytes are `head`: a version-2 header without CSRCs
/// or extension, an AU-headers-length of 16 bits (one AU), and that AU's
/// 13-bit size. `None` when the bytes do not read that way.
fn bare_aac_len(head: &[u8]) -> Option<usize> {
    if head.len() < BARE_AAC_HEAD_LEN || head[0] >> 6 != 2 || head[0] & 0x1f != 0 {
        return None;
    }
    let au_headers_bits = u16::from_be_bytes([head[12], head[13]]);
    if au_headers_bits != 16 {
        return None;
    }
    let au_size = usize::from(u16::from_be_bytes([head[14], head[15]]) >> 3);
    let total = BARE_AAC_HEAD_LEN + au_size;
    (au_size > 0 && total <= BARE_AAC_MAX_LEN).then_some(total)
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

/// Long enough for a fixed header and version 2.
fn is_rtp(packet: &[u8]) -> bool {
    packet.len() >= RTP_HEADER_LEN && packet[0] >> 6 == 2
}

/// Up to `DESYNC_DUMP_BYTES` bytes as space-separated hex.
fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(DESYNC_DUMP_BYTES)
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// `v2 pt=96 seq=1234 m=1` for an RTP packet, `not-rtp` otherwise.
fn rtp_summary(packet: &[u8]) -> String {
    if !is_rtp(packet) {
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
    let dump = hex(ahead);
    let marker = ahead.iter().position(|&b| b == b'$').map_or_else(
        || "none buffered".to_string(),
        |i| format!("{} bytes ahead", i + 1),
    );
    format!(
        "unexpected byte 0x{first:02x} after {} frames, {} bare RTCP packets and {} resyncs (last frame: {}); next bytes: {dump}; next '$' {marker}",
        stats.frames,
        stats.bare_rtcp,
        stats.resyncs,
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
    if !is_rtp(packet) {
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
        a=fmtp:98 streamtype=5;profile-level-id=1;mode=AAC-hbr;sizelength=13;indexlength=3;indexdeltalength=3;config=1408\r\n\
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
    fn channel_pairs_must_be_distinct_and_disjoint() {
        assert!(channel_pairs_disjoint((0, 1), (2, 3)));
        assert!(!channel_pairs_disjoint((0, 0), (2, 3)));
        assert!(!channel_pairs_disjoint((0, 1), (1, 2)));
        assert!(!channel_pairs_disjoint((0, 1), (3, 0)));
        assert!(!channel_pairs_disjoint((0, 1), (2, 2)));
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
        stats.record(0, Lane::VideoRtp, &[0x80; 12]);
        let mut ahead = vec![0xABu8; 30];
        ahead[27] = b'$';
        let report = desync_report(0x01, &ahead, &stats);
        assert!(report.starts_with(
            "unexpected byte 0x01 after 1 frames, 0 bare RTCP packets and 0 resyncs (last frame: channel 0, 12 bytes)"
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
    async fn read_next_handles_frames_bare_packets_messages_and_resyncs() {
        let rtp = [0x80, 0x60, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0xAA];
        let mut stream = interleaved_frame(0, &rtp);
        stream.extend_from_slice(&interleaved_frame(1, &rtcp_receiver_report(7)));
        // A bare sender report, as Arlo's server emits them.
        stream.extend_from_slice(&[0x80, 0xc8, 0x00, 0x06]);
        stream.extend_from_slice(&[0u8; 24]);
        stream.extend_from_slice(b"GET_PARAMETER rtsp://h RTSP/1.0\r\nCSeq: 9\r\n\r\n");
        stream.extend_from_slice(b"RTSP/1.0 200 OK\r\nCSeq: 3\r\nContent-Length: 2\r\n\r\nok");
        // Arlo's bare keep-alive, then a frame again: the loop resyncs.
        stream.extend_from_slice(&[0x80, 0x80, 0, 1, 0, 0, 0, 0, 0x24, 0x9a, 0xe6, 0x34]);
        let mut second = rtp;
        second[3] = 2;
        stream.extend_from_slice(&interleaved_frame(0, &second));
        // A framed AAC packet on the audio channel.
        let aac = [
            0x80, 0x62, 0, 7, 0, 0, 0, 0, 0, 0, 0, 2, 0x00, 0x10, 0x00, 0x08, 0xAA,
        ];
        stream.extend_from_slice(&interleaved_frame(2, &aac));
        // Junk holding a '$' on a wrong channel, then the end.
        stream.extend_from_slice(&[0x01, 0x02, 0x03, b'$', 0x07, 0, 1, 0x80]);
        let mut reader = reader_fed_with(&stream);
        let mut stats = FrameStats::default();
        let channels = both_tracks();

        let Ok(Incoming::Rtp(packet)) = read_next(&mut reader, channels, &mut stats).await else {
            panic!("first frame is the RTP packet");
        };
        assert_eq!(packet[1] & 0x7f, u8::try_from(LIVE_RTP_H264_PT).unwrap());
        assert_eq!(stats.rtp_pt, Some(0x60));
        assert!(matches!(
            read_next(&mut reader, channels, &mut stats).await,
            Ok(Incoming::Other)
        ));
        assert!(matches!(
            read_next(&mut reader, channels, &mut stats).await,
            Ok(Incoming::Other)
        ));
        assert_eq!(stats.bare_rtcp, 1);
        let Ok(Incoming::ServerRequest { cseq }) =
            read_next(&mut reader, channels, &mut stats).await
        else {
            panic!("server request is handed over for an ack");
        };
        assert_eq!(cseq, "9");
        assert!(matches!(
            read_next(&mut reader, channels, &mut stats).await,
            Ok(Incoming::Other)
        ));
        let Ok(Incoming::Rtp(packet)) = read_next(&mut reader, channels, &mut stats).await else {
            panic!("the frame after the bare keep-alive is found again");
        };
        assert_eq!(packet[3], 2, "second RTP packet, seq 2");
        assert_eq!((stats.resyncs, stats.frames), (1, 3));
        let Ok(Incoming::Audio(packet)) = read_next(&mut reader, channels, &mut stats).await else {
            panic!("the audio channel's frame goes to the AAC sink");
        };
        assert_eq!(packet[1] & 0x7f, u8::try_from(LIVE_RTP_AAC_PT).unwrap());
        assert_eq!(packet[3], 7, "audio seq kept");
        // Junk up to EOF: the stream is reported closed, not desynced.
        assert!(matches!(
            read_next(&mut reader, channels, &mut stats).await,
            Ok(Incoming::Closed)
        ));
    }

    fn both_tracks() -> Channels {
        Channels {
            video: (0, 1),
            audio: Some((2, 3)),
        }
    }

    #[test]
    fn frame_header_at_trusts_only_a_plausible_header() {
        let channels = Channels {
            video: (0, 1),
            audio: None,
        };
        // '$', channel 0, len 12, RTP v2 pt 96.
        let good = [b'$', 0, 0, 12, 0x80, 0x60];
        assert_eq!(frame_header_at(&good, channels, None), Some((0, 12)));
        assert_eq!(frame_header_at(&good, channels, Some(0x60)), Some((0, 12)));
        assert_eq!(frame_header_at(&good, channels, Some(0x61)), None);
        assert_eq!(
            frame_header_at(&[b'$', 0, 0, 11, 0x80, 0x60], channels, None),
            None
        );
        assert_eq!(
            frame_header_at(&[b'$', 2, 0, 12, 0x80, 0x60], channels, None),
            None
        );
        assert_eq!(
            frame_header_at(&[b'$', 0, 0, 12, 0x40, 0x60], channels, None),
            None
        );
        assert_eq!(
            frame_header_at(&[b'$', 1, 0, 28, 0x80, 0xc8], channels, None),
            Some((1, 28))
        );
        assert_eq!(
            frame_header_at(&[b'$', 1, 0, 28, 0x80, 0x60], channels, None),
            None
        );
        assert_eq!(frame_header_at(&good[..5], channels, None), None);
        // With an audio track set up, its channel is trusted on version alone.
        assert_eq!(
            frame_header_at(&[b'$', 2, 0, 12, 0x80, 0x00], both_tracks(), Some(0x60)),
            Some((2, 12))
        );
        assert_eq!(
            frame_header_at(&[b'$', 3, 0, 28, 0x80, 0xc8], both_tracks(), None),
            Some((3, 28))
        );
        assert_eq!(
            frame_header_at(&[0, 0, 0, 12, 0x80, 0x60], channels, None),
            None
        );
    }

    #[test]
    fn audio_track_parses_the_aac_track_of_the_sdp() {
        let track = audio_track(SDP).expect("aac track");
        assert_eq!(track.control, "trackid=2");
        assert_eq!(
            track.format,
            AacRtpFormat {
                clock_rate: 16000,
                channels: 1,
                config: "1408".into(),
                size_length: 13,
                index_length: 3,
                index_delta_length: 3,
            }
        );
        // Widths default to AAC-hbr when the fmtp omits them; channels to 1.
        let terse = SDP
            .replace(";sizelength=13;indexlength=3;indexdeltalength=3", "")
            .replace("MPEG4-GENERIC/16000/1", "MPEG4-GENERIC/44100");
        let track = audio_track(&terse).expect("terse track");
        assert_eq!((track.format.clock_rate, track.format.channels), (44100, 1));
        assert_eq!(track.format.size_length, 13);
    }

    #[test]
    fn audio_track_needs_a_config_and_the_hbr_mode() {
        assert!(audio_track(&SDP.replace(";config=1408", "")).is_none());
        // The config is hex or nothing: caps syntax cannot ride in it.
        assert!(audio_track(&SDP.replace("config=1408", "config=1408,media=video")).is_none());
        assert!(audio_track(&SDP.replace("config=1408", "config=140")).is_none());
        assert!(audio_track(&SDP.replace("config=1408", "config=zz08")).is_none());
        assert!(
            audio_track(&SDP.replace("config=1408", &format!("config={}", "ab".repeat(129))))
                .is_none()
        );
        assert!(audio_track(&SDP.replace("mode=AAC-hbr", "mode=AAC-lbr")).is_none());
        assert!(audio_track(&SDP.replace("MPEG4-GENERIC", "opus")).is_none());
        assert!(audio_track("v=0\r\nm=video 0 RTP/AVP 96\r\n").is_none());
    }

    #[test]
    fn bare_aac_len_reads_the_au_size_of_an_hbr_packet() {
        // Arlo's capture: V2, M=1, PT 0, seq 1; AU-headers-length 16; AU size 448.
        let mut head = vec![
            0x80, 0x80, 0, 1, 0, 0, 0, 0, 0x5a, 0x4a, 0xd4, 0x7b, 0x00, 0x10, 0x0e, 0x00,
        ];
        assert_eq!(bare_aac_len(&head), Some(16 + 448));
        head[12] = 0x00;
        head[13] = 0x20; // two AU headers: not the shape we parse
        assert_eq!(bare_aac_len(&head), None);
        let with_csrc = [
            0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0x00, 0x10, 0x0e, 0x00,
        ];
        assert_eq!(bare_aac_len(&with_csrc), None);
        assert_eq!(bare_aac_len(&head[..10]), None);
        let empty = [
            0x80, 0x80, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0x00, 0x10, 0x00, 0x00,
        ];
        assert_eq!(bare_aac_len(&empty), None);
    }

    #[tokio::test]
    async fn read_unframed_relays_a_bare_aac_packet_when_the_sdp_announced_aac() {
        let mut stream = vec![
            0x80, 0x80, 0, 5, 0, 0, 0x0c, 0x00, 1, 2, 3, 4, 0x00, 0x10, 0x00, 0x18,
        ];
        stream.extend_from_slice(&[0xAB; 3]); // AU of 3 bytes
        let rtp = [0x80, 0x60, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0xAA];
        stream.extend_from_slice(&interleaved_frame(0, &rtp));
        let mut reader = reader_fed_with(&stream);
        let mut stats = FrameStats {
            aac_bare: true,
            ..FrameStats::default()
        };
        let Ok(Incoming::Audio(packet)) = read_next(&mut reader, both_tracks(), &mut stats).await
        else {
            panic!("a bare AAC packet is relayed as audio");
        };
        assert_eq!(packet.len(), 19);
        assert_eq!(packet[1] & 0x7f, u8::try_from(LIVE_RTP_AAC_PT).unwrap());
        assert_eq!(stats.bare_aac, 1);
        assert!(matches!(
            read_next(&mut reader, both_tracks(), &mut stats).await,
            Ok(Incoming::Rtp(_))
        ));
        assert_eq!(stats.resyncs, 0, "no resync needed around a parsed packet");
    }

    #[tokio::test]
    async fn response_with_an_oversized_content_length_is_refused() {
        for header in [
            "Content-Length: 99999999",
            "Content-Length: 18446744073709551615",
        ] {
            let text = format!("RTSP/1.0 200 OK\r\nCSeq: 1\r\n{header}\r\n\r\n");
            let mut reader = reader_fed_with(text.as_bytes());
            let err = match read_response(&mut reader).await {
                Err(e) => e.to_string(),
                Ok(_) => panic!("{header} must be refused"),
            };
            assert!(err.contains("Content-Length"), "{header}: {err}");
        }
        // Within the limit the body is read as before.
        let mut reader = reader_fed_with(b"RTSP/1.0 200 OK\r\nContent-Length: 3\r\n\r\nabc");
        let response = read_response(&mut reader).await.expect("small body");
        assert_eq!(response.body, b"abc");
    }

    #[tokio::test]
    async fn overlong_lines_and_header_floods_are_refused() {
        let mut long = b"RTSP/1.0 200 OK\r\n".to_vec();
        long.extend(std::iter::repeat_n(b'a', MAX_LINE_BYTES + 100));
        let mut reader = reader_fed_with(&long);
        let err = match read_response(&mut reader).await {
            Err(e) => e.to_string(),
            Ok(_) => panic!("an endless line must be refused"),
        };
        assert!(err.contains("longer than"), "{err}");

        let mut flood = b"RTSP/1.0 200 OK\r\n".to_vec();
        for i in 0..(MAX_HEADERS + 10) {
            flood.extend_from_slice(format!("X-{i}: v\r\n").as_bytes());
        }
        flood.extend_from_slice(b"\r\n");
        let mut reader = reader_fed_with(&flood);
        let err = match read_response(&mut reader).await {
            Err(e) => e.to_string(),
            Ok(_) => panic!("a header flood must be refused"),
        };
        assert!(err.contains("headers"), "{err}");
    }

    #[test]
    fn certificate_pins_parse_as_64_hex_digits() {
        let hex = "ab".repeat(32);
        assert_eq!(parse_fingerprint(&hex).unwrap(), [0xab; 32]);
        let colons = (0..32).map(|_| "AB").collect::<Vec<_>>().join(":");
        assert_eq!(parse_fingerprint(&colons).unwrap(), [0xab; 32]);
        assert!(parse_fingerprint("abcd").is_err());
        assert!(parse_fingerprint(&"zz".repeat(32)).is_err());
        assert_eq!(fingerprint_hex([0x0f; 32]), "0f".repeat(32));
    }

    #[test]
    fn relay_tls_builds_from_the_system_roots_and_accepts_a_pin() {
        let plain = RelayTls::from_config(None).expect("system roots");
        assert!(plain.pinned.is_none());
        let pinned = RelayTls::from_config(Some(&"01".repeat(32))).expect("pin");
        assert_eq!(pinned.pinned, Some([1u8; 32]));
        assert!(RelayTls::from_config(Some("nope")).is_err());
        assert!(format!("{pinned:?}").contains("0101"));
    }

    #[tokio::test]
    async fn resync_gives_up_after_the_limit_with_a_report() {
        let junk = vec![0x01u8; RESYNC_LIMIT + 16];
        let mut reader = reader_fed_with(&junk);
        let mut stats = FrameStats::default();
        let Err(why) = read_next(&mut reader, both_tracks(), &mut stats).await else {
            panic!("endless junk ends the relay");
        };
        assert!(
            why.starts_with("unexpected byte 0x01 after 0 frames"),
            "{why}"
        );
        assert!(why.ends_with("next '$' none buffered"), "{why}");
    }

    #[test]
    fn rtcp_receiver_report_is_a_valid_empty_rr() {
        let rr = rtcp_receiver_report(0x0102_0304);
        assert_eq!(rr, [0x80, 201, 0, 1, 1, 2, 3, 4]);
    }

    #[test]
    fn is_loopback_host_accepts_only_this_machine() {
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("[::1]"));
        assert!(is_loopback_host("localhost"));
        assert!(!is_loopback_host("1.2.3.4"));
        assert!(!is_loopback_host("arlo.example"));
        assert!(!is_loopback_host("[fe80::1]"));
    }

    #[tokio::test]
    async fn connect_refuses_plaintext_rtsp_unless_the_policy_allows_loopback() {
        let policy = RelayTls::from_config(None).unwrap();
        for url in [
            "rtsp://192.0.2.1:554/live/x?egressToken=t",
            "rtsp://127.0.0.1:1/live/x?egressToken=t",
        ] {
            let url = WatchAlongUrl::parse(url).unwrap();
            let Err(err) = Client::connect(&url, &policy).await else {
                panic!("plaintext rtsp was accepted by the default policy");
            };
            assert!(err.to_string().contains("plaintext"), "{err}");
        }
        // The loopback allowance reaches the socket (refused: nothing
        // listens on port 1); a remote host stays refused before it.
        let allowing = policy.allowing_plaintext_to_loopback();
        let local = WatchAlongUrl::parse("rtsp://127.0.0.1:1/live/x?egressToken=t").unwrap();
        let Err(err) = Client::connect(&local, &allowing).await else {
            panic!("nothing listens on port 1");
        };
        assert!(!err.to_string().contains("plaintext"), "{err}");
        let remote = WatchAlongUrl::parse("rtsp://192.0.2.1:554/live/x?egressToken=t").unwrap();
        let Err(err) = Client::connect(&remote, &allowing).await else {
            panic!("plaintext rtsp to a remote host was accepted");
        };
        assert!(err.to_string().contains("plaintext"), "{err}");
    }
}
