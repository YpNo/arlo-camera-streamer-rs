//! `healthcheck` subcommand: the container's `HEALTHCHECK` without a
//! shell, `wget` or `curl` in the image. One plain HTTP/1.0 request to
//! the daemon's liveness endpoint on loopback; exit 0 on `200`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use streamer_domain::config::StreamerConfig;

/// Whole-check budget: connect, request and the answer.
const TIMEOUT: Duration = Duration::from_secs(4);
/// The liveness endpoint of the ops server.
const PATH: &str = "/healthz";
/// Largest answer read; the real one is a few dozen bytes.
const MAX_REPLY_BYTES: u64 = 4096;

/// Run the check against the configured ops bind.
///
/// # Errors
///
/// Any failure to connect, send or receive, or a non-`200` answer.
pub async fn run(config: &StreamerConfig) -> Result<()> {
    let bind: SocketAddr = config
        .output
        .metrics_bind
        .parse()
        .with_context(|| format!("invalid metrics_bind '{}'", config.output.metrics_bind))?;
    let target = loopback_target(bind);
    let status = tokio::time::timeout(TIMEOUT, fetch_status(target))
        .await
        .context("healthcheck timed out")??;
    if status != 200 {
        bail!("liveness endpoint answered HTTP {status}");
    }
    println!("ok");
    Ok(())
}

/// Where to reach a listener bound to `bind` from this host: an
/// unspecified address (`0.0.0.0`, `::`) becomes loopback.
fn loopback_target(bind: SocketAddr) -> SocketAddr {
    let ip = match bind.ip() {
        IpAddr::V4(v4) if v4.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(v6) if v6.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        other => other,
    };
    SocketAddr::new(ip, bind.port())
}

async fn fetch_status(target: SocketAddr) -> Result<u16> {
    let mut sock = TcpStream::connect(target)
        .await
        .with_context(|| format!("connect to {target}"))?;
    let request = format!(
        "GET {PATH} HTTP/1.0\r\nHost: {}\r\nConnection: close\r\n\r\n",
        host_header(target.ip())
    );
    sock.write_all(request.as_bytes())
        .await
        .context("send request")?;
    let mut reply = Vec::new();
    sock.take(MAX_REPLY_BYTES)
        .read_to_end(&mut reply)
        .await
        .context("read answer")?;
    parse_status(&reply).context("malformed HTTP answer")
}

/// The `Host` value for `ip`: an IPv6 address goes in brackets (RFC 7230
/// §5.4), or the server rejects the request line as malformed.
fn host_header(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => format!("[{v6}]"),
    }
}

/// The status code of an HTTP/1.x status line.
fn parse_status(reply: &[u8]) -> Option<u16> {
    let line = reply.split(|b| *b == b'\n').next()?;
    let text = std::str::from_utf8(line).ok()?;
    let mut parts = text.split_whitespace();
    let version = parts.next()?;
    if !version.starts_with("HTTP/1.") {
        return None;
    }
    parts.next()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_target_maps_unspecified_binds_to_loopback() {
        let any: SocketAddr = "0.0.0.0:9090".parse().unwrap();
        assert_eq!(loopback_target(any).to_string(), "127.0.0.1:9090");
        let any6: SocketAddr = "[::]:9090".parse().unwrap();
        assert_eq!(loopback_target(any6).to_string(), "[::1]:9090");
        let fixed: SocketAddr = "10.0.0.5:9090".parse().unwrap();
        assert_eq!(loopback_target(fixed), fixed);
    }

    #[test]
    fn host_header_brackets_ipv6() {
        assert_eq!(host_header("127.0.0.1".parse().unwrap()), "127.0.0.1");
        assert_eq!(host_header("::1".parse().unwrap()), "[::1]");
    }

    #[test]
    fn parse_status_reads_the_status_line_only() {
        assert_eq!(
            parse_status(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok"),
            Some(200)
        );
        assert_eq!(
            parse_status(b"HTTP/1.0 503 Service Unavailable\r\n"),
            Some(503)
        );
        assert_eq!(parse_status(b"garbage"), None);
        assert_eq!(parse_status(b""), None);
    }

    #[tokio::test]
    async fn fetch_status_returns_the_code_the_server_sends() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 256];
            let _ = sock.read(&mut buf).await;
            sock.write_all(b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .unwrap();
        });
        assert_eq!(fetch_status(addr).await.unwrap(), 200);
    }
}
