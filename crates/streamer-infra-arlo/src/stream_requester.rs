//! [`ArloStreamRequester`] implementation backed by
//! [`ArloClient::start_stream`].
//!
//! `start_stream` already correlates the SSE response with the POST
//! transaction and applies a 30 s timeout internally, so we don't add
//! any further coordination here — we just unwrap the resulting
//! `rtsps://…` URL into a domain [`StreamSource`] with no codec hint.
//! Codec auto-detection happens in `streamer-infra-media` when the
//! pipeline first attaches to the URL.

use std::sync::Arc;

use async_trait::async_trait;
use rs_arlo::client::ArloClient;

use streamer_domain::camera::CameraId;
use streamer_domain::error::DomainError;
use streamer_domain::port::ArloStreamRequester;
use streamer_domain::stream::StreamSource;

use crate::error::arlo_to_domain;

/// Adapter that exposes [`ArloClient::start_stream`] as the domain
/// [`ArloStreamRequester`] port.
pub struct ArloStreamRequesterAdapter {
    client: Arc<ArloClient>,
}

impl ArloStreamRequesterAdapter {
    /// Construct from a shared, authenticated [`ArloClient`] handle.
    #[must_use]
    pub fn new(client: Arc<ArloClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ArloStreamRequester for ArloStreamRequesterAdapter {
    async fn request_live(&self, camera: &CameraId) -> Result<StreamSource, DomainError> {
        let url = self
            .client
            .start_stream(camera.as_str())
            .await
            .map_err(arlo_to_domain)?;
        Ok(StreamSource {
            url: url.into_inner(),
            codec_hint: None,
        })
    }
}
