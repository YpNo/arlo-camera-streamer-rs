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
//! [`SNAPSHOT_URL_MAX_AGE`]: crate::snapshot_cache::SNAPSHOT_URL_MAX_AGE

use std::sync::Arc;
use std::time::Instant;

use arlo_rs::client::ArloClient;
use async_trait::async_trait;
use bytes::Bytes;
use tracing::debug;

use streamer_domain::camera::CameraId;
use streamer_domain::error::DomainError;
use streamer_domain::port::ArloThumbnailSource;

use crate::error::arlo_to_domain;
use crate::snapshot_cache::SnapshotUrlCache;

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
        let resp = self
            .http
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
        resp.bytes()
            .await
            .map_err(|e| transport_error("thumbnail body read failed", e))
    }

    async fn device_list_url(&self, camera: &CameraId) -> Result<Option<String>, DomainError> {
        let devices = self.client.get_devices().await.map_err(arlo_to_domain)?;
        Ok(devices
            .iter()
            .find(|d| d.device_id == camera.as_str())
            .and_then(|d| d.presigned_last_image_url.clone()))
    }
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
    DomainError::AdapterTransport(format!("{what}: {}", e.without_url()))
}

#[cfg(test)]
mod tests {
    use super::*;

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
