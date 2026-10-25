//! The RTSP server's sessions: expired ones are removed, a keep-alive
//! keeps one, the timeout is announced, the HLS segmenters' loopback
//! server keeps its own pool, both servers share one media per mount, and
//! RTP goes over TCP only.
//!
//! A raw RTSP client drives the server, so each test decides when a
//! session is created, kept alive or abandoned (no TEARDOWN, as after a
//! crash or a network cut).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use streamer_infra_media::{RtspLimits, RtspServer};

/// Set to `1` in CI: a host without the plugins fails instead of skipping.
const REQUIRE_ENV: &str = "STREAMER_REQUIRE_GST_IT";
const MOUNT: &str = "/it";
const LAUNCH: &str = "( videotestsrc is-live=true ! x264enc tune=zerolatency \
                      ! rtph264pay name=pay0 pt=96 )";
/// Short limits, so expiry happens within a test.
const SHORT: RtspLimits = RtspLimits {
    max_sessions: 64,
    session_timeout_secs: 2,
};
const TCP_TRANSPORT: &str = "RTP/AVP/TCP;unicast;interleaved=0-1";
const UDP_TRANSPORT: &str = "RTP/AVP;unicast;client_port=50000-50001";
/// gst-rtsp-server grants a few seconds beyond the timeout.
const EXPIRY_WAIT: Duration = Duration::from_secs(15);

/// Tests share GStreamer's default main context: one at a time.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn gstreamer_ready() -> bool {
    gstreamer::init().expect("gst::init");
    if gstreamer::ElementFactory::find("x264enc").is_some() {
        return true;
    }
    assert!(
        std::env::var(REQUIRE_ENV).as_deref() != Ok("1"),
        "x264enc missing"
    );
    eprintln!("skipping: x264enc missing");
    false
}

/// One RTSP connection speaking just enough of the protocol.
struct RawClient {
    reader: BufReader<TcpStream>,
    cseq: u32,
    session: Option<String>,
}

/// A response's status code and headers (`Name: value` lines).
struct Response {
    status: u16,
    headers: String,
}

impl RawClient {
    fn connect(url: &str) -> Self {
        let authority = url
            .trim_start_matches("rtsp://")
            .split('/')
            .next()
            .expect("authority");
        let stream = TcpStream::connect(authority).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("timeout");
        Self {
            reader: BufReader::new(stream),
            cseq: 0,
            session: None,
        }
    }

    fn request(&mut self, method: &str, uri: &str, extra: &str) -> Response {
        self.cseq += 1;
        let session = self
            .session
            .as_ref()
            .map(|id| format!("Session: {id}\r\n"))
            .unwrap_or_default();
        let text = format!(
            "{method} {uri} RTSP/1.0\r\nCSeq: {}\r\n{session}{extra}\r\n",
            self.cseq
        );
        self.reader
            .get_mut()
            .write_all(text.as_bytes())
            .expect("write");
        self.read_response()
    }

    fn read_response(&mut self) -> Response {
        let mut status_line = String::new();
        self.reader.read_line(&mut status_line).expect("status");
        let status = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .expect("status code");
        let mut headers = String::new();
        let mut length = 0;
        loop {
            let mut line = String::new();
            self.reader.read_line(&mut line).expect("header");
            if line.trim().is_empty() {
                break;
            }
            if let Some(value) = line.strip_prefix("Content-Length:") {
                length = value.trim().parse().expect("length");
            }
            headers.push_str(&line);
        }
        let mut body = vec![0; length];
        self.reader.read_exact(&mut body).expect("body");
        Response { status, headers }
    }

    /// DESCRIBE then SETUP of the video track over TCP; the session id is
    /// kept for later requests. Returns the SETUP response.
    fn set_up(&mut self, url: &str) -> Response {
        self.set_up_offering(url, TCP_TRANSPORT)
    }

    /// DESCRIBE then SETUP of the video track, offering `transports` (a
    /// `Transport` header value, preferred first).
    fn set_up_offering(&mut self, url: &str, transports: &str) -> Response {
        let describe = self.request("DESCRIBE", url, "Accept: application/sdp\r\n");
        assert_eq!(describe.status, 200, "{}", describe.headers);
        let setup = self.request(
            "SETUP",
            &format!("{url}/stream=0"),
            &format!("Transport: {transports}\r\n"),
        );
        self.session = setup
            .headers
            .lines()
            .find_map(|line| line.strip_prefix("Session:"))
            .map(|value| {
                value
                    .split(';')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_string()
            });
        setup
    }
}

fn server(limits: RtspLimits) -> (Arc<RtspServer>, String) {
    let server = RtspServer::start_with("127.0.0.1:0", limits).expect("rtsp server");
    server.install_factory(MOUNT, LAUNCH).expect("factory");
    let port = server.bound_port().expect("bound");
    (server, format!("rtsp://127.0.0.1:{port}{MOUNT}"))
}

fn eventually(within: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    condition()
}

/// Every client that vanished without a TEARDOWN used to keep its session
/// for ever, until the pool was full and the NVR was refused.
#[test]
fn sessions_of_vanished_clients_expire() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !gstreamer_ready() {
        return;
    }
    let (server, url) = server(SHORT);
    for _ in 0..3 {
        let mut client = RawClient::connect(&url);
        assert_eq!(client.set_up(&url).status, 200);
    }
    assert_eq!(server.session_counts().0, 3);
    assert!(
        eventually(EXPIRY_WAIT, || server.session_counts().0 == 0),
        "sessions left: {:?}",
        server.session_counts()
    );
}

#[test]
fn a_keep_alive_keeps_the_session_and_the_timeout_is_announced() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !gstreamer_ready() {
        return;
    }
    let (server, url) = server(SHORT);
    let mut client = RawClient::connect(&url);
    let setup = client.set_up(&url);
    assert!(
        setup.headers.contains(";timeout=2"),
        "the Session header announces the timeout: {}",
        setup.headers
    );
    // Well past timeout plus slack, an OPTIONS a second, as go2rtc does.
    for _ in 0..10 {
        std::thread::sleep(Duration::from_secs(1));
        assert_eq!(client.request("OPTIONS", &url, "").status, 200);
    }
    assert_eq!(server.session_counts().0, 1, "the kept-alive session stays");
}

/// A LAN host holding every session of the public server must not stop
/// the daemon's own HLS segmenters.
#[test]
fn a_full_public_pool_leaves_the_loopback_server_its_sessions() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !gstreamer_ready() {
        return;
    }
    let one = RtspLimits {
        max_sessions: 1,
        ..SHORT
    };
    let (server, url) = server(one);
    let mut holder = RawClient::connect(&url);
    assert_eq!(holder.set_up(&url).status, 200);
    let mut refused = RawClient::connect(&url);
    assert_eq!(refused.set_up(&url).status, 503, "the public pool is full");

    let internal = server.loopback_url(MOUNT).expect("loopback url");
    assert_ne!(internal, url, "a server of its own");
    let mut segmenter = RawClient::connect(&internal);
    assert_eq!(segmenter.set_up(&internal).status, 200);
    assert_eq!(server.session_counts(), (1, 1));
}

/// A camera keeps one pipeline whichever server its clients came in
/// through: two would split the live switch from what HLS records.
#[test]
fn both_servers_share_one_media_per_mount() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !gstreamer_ready() {
        return;
    }
    let server = RtspServer::start_with("127.0.0.1:0", SHORT).expect("rtsp server");
    let built = Arc::new(AtomicUsize::new(0));
    let counter = built.clone();
    server
        .install_factory_with_media_hook(MOUNT, LAUNCH, move |_media| {
            counter.fetch_add(1, Ordering::SeqCst);
        })
        .expect("factory");
    let port = server.bound_port().expect("bound");
    let public = format!("rtsp://127.0.0.1:{port}{MOUNT}");
    let internal = server.loopback_url(MOUNT).expect("loopback url");

    let mut viewer = RawClient::connect(&public);
    assert_eq!(viewer.set_up(&public).status, 200);
    let mut segmenter = RawClient::connect(&internal);
    assert_eq!(segmenter.set_up(&internal).status, 200);

    assert_eq!(built.load(Ordering::SeqCst), 1, "one media for the mount");
}

/// A query on the mount's URL used to build a second media for
/// the camera, which took its live wiring and, once gone, left the
/// viewers' media without live video (third sweep, S3-M1).
#[test]
fn a_query_on_the_url_reuses_the_mount_s_media() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !gstreamer_ready() {
        return;
    }
    let server = RtspServer::start_with("127.0.0.1:0", SHORT).expect("rtsp server");
    let built = Arc::new(AtomicUsize::new(0));
    let counter = built.clone();
    server
        .install_factory_with_media_hook(MOUNT, LAUNCH, move |_media| {
            counter.fetch_add(1, Ordering::SeqCst);
        })
        .expect("factory");
    let port = server.bound_port().expect("bound");
    let url = format!("rtsp://127.0.0.1:{port}{MOUNT}");

    let mut viewer = RawClient::connect(&url);
    assert_eq!(viewer.set_up(&url).status, 200);
    for query in ["?x=1", "?x=2"] {
        let mut other = RawClient::connect(&url);
        let describe = other.request(
            "DESCRIBE",
            &format!("{url}{query}"),
            "Accept: application/sdp\r\n",
        );
        assert_eq!(describe.status, 200, "{query}: {}", describe.headers);
    }
    // A path below the mount is not served at all.
    let mut other = RawClient::connect(&url);
    let suffix = other.request("DESCRIBE", &format!("{url}/extra"), "");
    assert_eq!(suffix.status, 404);

    assert_eq!(built.load(Ordering::SeqCst), 1, "one media for the mount");
}

/// RTP over UDP never reaches a client through a container that
/// publishes the RTSP port over TCP only: a client trying UDP first (VLC,
/// GStreamer) waited 5 to 10 s for nothing before retrying over TCP.
/// Refused (461), it retries over TCP at once.
#[test]
fn rtp_over_udp_is_refused_and_the_retry_over_tcp_accepted() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !gstreamer_ready() {
        return;
    }
    let (_server, url) = server(SHORT);

    let mut client = RawClient::connect(&url);
    let refused = client.set_up_offering(&url, UDP_TRANSPORT);
    assert_eq!(refused.status, 461, "{}", refused.headers);
    let accepted = client.set_up_offering(&url, TCP_TRANSPORT);
    assert_eq!(accepted.status, 200, "{}", accepted.headers);
    assert!(
        accepted.headers.contains("RTP/AVP/TCP"),
        "{}",
        accepted.headers
    );
}
