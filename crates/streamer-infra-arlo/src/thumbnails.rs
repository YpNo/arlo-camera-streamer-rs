//! [`ArloThumbnailSource`] implementation: the camera's latest snapshot
//! JPEG, fetched from its presigned URL.
//!
//! The URL comes from the [`SnapshotUrlCache`] when the event bus
//! announced a snapshot in the last [`SNAPSHOT_URL_MAX_AGE`] — the newest
//! image, no cloud round-trip — and otherwise from the device list
//! (`presigned_last_image_url`, one `get_devices()` call). A cached URL
//! that fails to fetch (expired) is forgotten and the device list is
//! used instead. URLs are presigned credentials and are never logged.
//!
//! The fetch treats the storage endpoint as untrusted: only `https` URLs
//! with a host are followed (both sources go through
//! [`is_https_with_host`]), the client built by [`http_client`] bounds
//! the request in time and redirects, the body is read up to
//! [`THUMBNAIL_MAX_BYTES`] and must start with the JPEG magic before it
//! reaches the image loader.
//!
//! [`SNAPSHOT_URL_MAX_AGE`]: crate::snapshot_cache::SNAPSHOT_URL_MAX_AGE

use std::sync::Arc;
use std::time::{Duration, Instant};

use arlo_rs::client::ArloClient;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use tracing::debug;

use streamer_domain::camera::CameraId;
use streamer_domain::error::DomainError;
use streamer_domain::port::ArloThumbnailSource;
use streamer_domain::thumbnail::check_thumbnail;

use crate::error::arlo_to_domain;
use crate::snapshot_cache::{SnapshotUrlCache, is_https_with_host};

/// Largest thumbnail body accepted. Arlo stills are a few hundred KiB;
/// anything bigger is not a snapshot and is not kept in memory.
pub const THUMBNAIL_MAX_BYTES: usize = 2 * 1024 * 1024;
/// Whole-request timeout (connect, headers and body) for one fetch. The
/// fetch runs inside the camera actor, so a stalled body would stall
/// that camera.
pub const THUMBNAIL_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
/// Connect timeout for one fetch.
pub const THUMBNAIL_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Redirect hops followed. A presigned URL answers directly; one hop
/// covers a bucket region redirect.
pub const THUMBNAIL_MAX_REDIRECTS: usize = 2;

/// Build the HTTP client the thumbnail adapter fetches with: `https`
/// only, bounded in time and in redirect hops. Shared across cameras so
/// connection pooling applies to the storage endpoints.
///
/// # Errors
///
/// Forwarded from the `reqwest` builder (TLS backend initialisation).
pub fn http_client(user_agent: &str) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .user_agent(user_agent)
        .timeout(THUMBNAIL_HTTP_TIMEOUT)
        .connect_timeout(THUMBNAIL_CONNECT_TIMEOUT)
        .redirect(reqwest::redirect::Policy::limited(THUMBNAIL_MAX_REDIRECTS))
        .https_only(true)
        .build()
}

/// Adapter that exposes the Arlo snapshot as the domain
/// [`ArloThumbnailSource`] port.
pub struct ArloThumbnailSourceAdapter {
    client: Arc<ArloClient>,
    http: reqwest::Client,
    snapshots: Arc<SnapshotUrlCache>,
}

impl ArloThumbnailSourceAdapter {
    /// Construct with a shared [`ArloClient`], a `reqwest` client and the
    /// snapshot URL cache the event adapter fills. Sharing the
    /// `reqwest::Client` across cameras lets connection pooling kick in
    /// for the S3 endpoints serving the images.
    #[must_use]
    pub fn new(
        client: Arc<ArloClient>,
        http: reqwest::Client,
        snapshots: Arc<SnapshotUrlCache>,
    ) -> Self {
        Self {
            client,
            http,
            snapshots,
        }
    }

    async fn fetch_jpeg(&self, url: &str, camera: &CameraId) -> Result<Bytes, DomainError> {
        fetch_jpeg(&self.http, url, camera).await
    }

    /// The device list's presigned URL for `camera`, or `None` when the
    /// device has none or it is not an `https` URL with a host.
    async fn device_list_url(&self, camera: &CameraId) -> Result<Option<String>, DomainError> {
        let devices = self.client.get_devices().await.map_err(arlo_to_domain)?;
        let url = devices
            .iter()
            .find(|d| d.device_id == camera.as_str())
            .and_then(|d| d.presigned_last_image_url.clone());
        Ok(match url {
            Some(u) if is_https_with_host(&u) => Some(u),
            Some(_) => {
                debug!(%camera, "device list thumbnail url refused: not https with a host");
                None
            }
            None => None,
        })
    }
}

/// GET `url` and return its body once it is known to be a JPEG no
/// larger than [`THUMBNAIL_MAX_BYTES`] whose header declares a frame the
/// idle overlay can decode safely ([`check_thumbnail`]: a small file can
/// declare a gigabyte of pixels). The URL must already be
/// validated; the client's policy (see [`http_client`]) bounds the
/// request itself.
async fn fetch_jpeg(
    http: &reqwest::Client,
    url: &str,
    camera: &CameraId,
) -> Result<Bytes, DomainError> {
    let resp = http
        .get(url)
        .send()
        .await
        .map_err(|e| transport_error("thumbnail GET failed", e))?;
    if !resp.status().is_success() {
        return Err(DomainError::AdapterTransport(format!(
            "thumbnail HTTP {} for {camera}",
            resp.status()
        )));
    }
    if let Some(len) = resp.content_length()
        && len > THUMBNAIL_MAX_BYTES as u64
    {
        return Err(DomainError::AdapterTransport(format!(
            "thumbnail for {camera} declares {len} bytes; the limit is {THUMBNAIL_MAX_BYTES}"
        )));
    }
    let bytes = read_body_capped(resp, THUMBNAIL_MAX_BYTES, camera).await?;
    check_thumbnail(&bytes).map_err(|e| {
        DomainError::AdapterTransport(format!(
            "thumbnail for {camera} refused: {e} ({} bytes)",
            bytes.len()
        ))
    })?;
    Ok(bytes)
}

/// Read the body chunk by chunk, failing as soon as it exceeds `cap` so
/// an endless or oversized body never accumulates in memory.
async fn read_body_capped(
    resp: reqwest::Response,
    cap: usize,
    camera: &CameraId,
) -> Result<Bytes, DomainError> {
    let mut body = BytesMut::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| transport_error("thumbnail body read failed", e))?;
        if body.len() + chunk.len() > cap {
            return Err(DomainError::AdapterTransport(format!(
                "thumbnail for {camera} exceeds {cap} bytes"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

#[async_trait]
impl ArloThumbnailSource for ArloThumbnailSourceAdapter {
    async fn last_thumbnail(&self, camera: &CameraId) -> Result<Option<Bytes>, DomainError> {
        if let Some(url) = self.snapshots.fresh(camera.as_str(), Instant::now()) {
            match self.fetch_jpeg(&url, camera).await {
                Ok(bytes) => {
                    debug!(%camera, source = "bus", bytes = bytes.len(), "idle snapshot fetched");
                    return Ok(Some(bytes));
                }
                Err(e) => {
                    debug!(%camera, error = %e, "cached snapshot URL failed; using the device list");
                    self.snapshots.forget(camera.as_str());
                }
            }
        }
        let Some(url) = self.device_list_url(camera).await? else {
            debug!(%camera, "no presigned thumbnail url available");
            return Ok(None);
        };
        let bytes = self.fetch_jpeg(&url, camera).await?;
        debug!(%camera, source = "device-list", bytes = bytes.len(), "idle snapshot fetched");
        Ok(Some(bytes))
    }
}

/// Wrap an HTTP error without its URL: `reqwest::Error`'s `Display`
/// appends `for url (…)`, and the URL is a presigned S3 link whose query
/// is the credential. The error class and the source chain are kept.
fn transport_error(what: &str, e: reqwest::Error) -> DomainError {
    DomainError::adapter_transport(format!("{what}: {}", e.without_url()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Serve one HTTP/1.1 response on a loopback port and return the
    /// URL to fetch. `body` is sent as-is after `extra_headers`; the
    /// connection closes afterwards, which ends a body without a
    /// `Content-Length`.
    async fn one_shot_server(status: &'static str, extra_headers: String, body: Vec<u8>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut req = Vec::new();
            let mut buf = [0u8; 1024];
            while !req.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = sock.read(&mut buf).await.unwrap();
                if n == 0 {
                    return;
                }
                req.extend_from_slice(&buf[..n]);
            }
            let head = format!("HTTP/1.1 {status}\r\nConnection: close\r\n{extra_headers}\r\n");
            sock.write_all(head.as_bytes()).await.unwrap();
            let _ = sock.write_all(&body).await;
            let _ = sock.shutdown().await;
        });
        format!("http://127.0.0.1:{port}/last.jpg?X-Amz-Signature=SECRET")
    }

    /// A plain-`http` client for the loopback server; the production
    /// client is `https` only (see `http_client_refuses_plain_http`).
    fn test_client() -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap()
    }

    fn cam() -> CameraId {
        CameraId::new("CAM1")
    }

    /// SOI and a baseline frame header declaring `width`×`height`.
    fn jpeg_header(width: u16, height: u16) -> Vec<u8> {
        let mut v = vec![0xFF, 0xD8, 0xFF, 0xC0, 0x00, 0x11, 0x08];
        v.extend(height.to_be_bytes());
        v.extend(width.to_be_bytes());
        v
    }

    /// A 640×480 JPEG header padded to `len` bytes.
    fn jpeg(len: usize) -> Vec<u8> {
        let mut v = jpeg_header(640, 480);
        v.resize(len, 0);
        v
    }

    #[tokio::test]
    async fn fetch_jpeg_returns_a_small_jpeg_body() {
        let body = jpeg(512);
        let url = one_shot_server(
            "200 OK",
            format!(
                "Content-Type: image/jpeg\r\nContent-Length: {}\r\n",
                body.len()
            ),
            body.clone(),
        )
        .await;
        let got = fetch_jpeg(&test_client(), &url, &cam()).await.unwrap();
        assert_eq!(&got[..], &body[..]);
    }

    #[tokio::test]
    async fn fetch_jpeg_rejects_a_declared_oversized_body_before_reading_it() {
        let declared = THUMBNAIL_MAX_BYTES + 1;
        let url = one_shot_server(
            "200 OK",
            format!("Content-Length: {declared}\r\n"),
            jpeg(16),
        )
        .await;
        let err = fetch_jpeg(&test_client(), &url, &cam()).await.unwrap_err();
        let text = err.to_string();
        assert!(text.contains("declares"), "{text}");
        assert!(!text.contains("SECRET"), "{text}");
    }

    #[tokio::test]
    async fn fetch_jpeg_rejects_an_undeclared_body_over_the_cap() {
        let url = one_shot_server("200 OK", String::new(), jpeg(THUMBNAIL_MAX_BYTES + 1)).await;
        let err = fetch_jpeg(&test_client(), &url, &cam()).await.unwrap_err();
        assert!(err.to_string().contains("exceeds"), "{err}");
    }

    #[tokio::test]
    async fn fetch_jpeg_rejects_bytes_that_are_not_a_jpeg() {
        let body = b"<html>not an image</html>".to_vec();
        let url = one_shot_server(
            "200 OK",
            format!(
                "Content-Type: image/jpeg\r\nContent-Length: {}\r\n",
                body.len()
            ),
            body,
        )
        .await;
        let err = fetch_jpeg(&test_client(), &url, &cam()).await.unwrap_err();
        assert!(err.to_string().contains("not a JPEG"), "{err}");
    }

    /// A 2.6 KB file declaring 12000×12000 took about 1 GB to decode.
    #[tokio::test]
    async fn fetch_jpeg_rejects_a_jpeg_declaring_a_huge_frame() {
        let mut body = jpeg_header(12000, 12000);
        body.resize(2669, 0);
        let url = one_shot_server(
            "200 OK",
            format!("Content-Length: {}\r\n", body.len()),
            body,
        )
        .await;
        let err = fetch_jpeg(&test_client(), &url, &cam()).await.unwrap_err();
        assert!(err.to_string().contains("12000x12000"), "{err}");
    }

    #[tokio::test]
    async fn fetch_jpeg_reports_a_non_success_status_without_the_url() {
        let url =
            one_shot_server("403 Forbidden", "Content-Length: 0\r\n".to_string(), vec![]).await;
        let err = fetch_jpeg(&test_client(), &url, &cam()).await.unwrap_err();
        let text = err.to_string();
        assert!(text.contains("HTTP 403"), "{text}");
        assert!(!text.contains("SECRET"), "{text}");
    }

    #[tokio::test]
    async fn http_client_refuses_plain_http() {
        let client = http_client("test").unwrap();
        let err = client
            .get("http://127.0.0.1:1/last.jpg?X-Amz-Signature=SECRET")
            .send()
            .await
            .expect_err("https only");
        assert!(err.is_builder(), "{err:?}");
        assert!(
            !transport_error("thumbnail GET failed", err)
                .to_string()
                .contains("SECRET")
        );
    }

    #[tokio::test]
    async fn transport_error_never_carries_the_presigned_url() {
        // Connection refused on a closed local port: a transport error that
        // reqwest decorates with the request URL.
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .unwrap();
        let err = client
            .get("http://127.0.0.1:1/thumb.jpg?X-Amz-Signature=SECRET")
            .send()
            .await
            .expect_err("port 1 refuses");
        assert!(
            err.to_string().contains("SECRET"),
            "precondition: reqwest echoes the URL"
        );
        let wrapped = transport_error("thumbnail GET failed", err).to_string();
        assert!(
            !wrapped.contains("SECRET") && !wrapped.contains("127.0.0.1"),
            "{wrapped}"
        );
        assert!(wrapped.contains("thumbnail GET failed"), "{wrapped}");
    }
}
