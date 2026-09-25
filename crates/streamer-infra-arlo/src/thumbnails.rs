//! [`ArloThumbnailSource`] implementation that pulls the camera's
//! `presigned_last_image_url` from the cloud device list, then HTTP
//! GETs the JPEG bytes.
//!
//! The pre-signed URL refreshes every time we hit `get_devices()`, so
//! we always fetch the device list on each thumbnail call. That's a
//! cloud round-trip per thumbnail, but it happens on the order of "a
//! few times per camera per day" (boot + each `Cooling → Idle`
//! transition) — well within rate limits.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use arlo_rs::client::ArloClient;
use tracing::debug;

use streamer_domain::camera::CameraId;
use streamer_domain::error::DomainError;
use streamer_domain::port::ArloThumbnailSource;

use crate::error::arlo_to_domain;

/// Adapter that exposes the Arlo cloud thumbnail URL as the domain
/// [`ArloThumbnailSource`] port.
pub struct ArloThumbnailSourceAdapter {
    client: Arc<ArloClient>,
    http: reqwest::Client,
}

impl ArloThumbnailSourceAdapter {
    /// Construct with a shared [`ArloClient`] and a `reqwest` client.
    /// Sharing the `reqwest::Client` across cameras is encouraged so
    /// connection pooling kicks in for the AWS S3 endpoints serving
    /// the thumbnails.
    #[must_use]
    pub fn new(client: Arc<ArloClient>, http: reqwest::Client) -> Self {
        Self { client, http }
    }
}

#[async_trait]
impl ArloThumbnailSource for ArloThumbnailSourceAdapter {
    async fn last_thumbnail(&self, camera: &CameraId) -> Result<Option<Bytes>, DomainError> {
        let devices = self.client.get_devices().await.map_err(arlo_to_domain)?;
        let url = devices
            .iter()
            .find(|d| d.device_id == camera.as_str())
            .and_then(|d| d.presigned_last_image_url.as_deref());
        let Some(url) = url else {
            debug!(%camera, "no presigned thumbnail url available");
            return Ok(None);
        };

        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| DomainError::AdapterTransport(format!("thumbnail GET failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(DomainError::AdapterTransport(format!(
                "thumbnail HTTP {} for {camera}",
                resp.status()
            )));
        }

        let bytes = resp.bytes().await.map_err(|e| {
            DomainError::AdapterTransport(format!("thumbnail body read failed: {e}"))
        })?;
        Ok(Some(bytes))
    }
}
