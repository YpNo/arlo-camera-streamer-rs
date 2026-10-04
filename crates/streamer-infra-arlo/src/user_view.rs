//! [`UserViewSource`] implementation: where the user's live view in the
//! Arlo app can be watched along (ADR 0007).
//!
//! Arlo answers the `get` stream query with a format per client identity.
//! Asked as the iOS app (`arlo_rs`'s `ios_app_user_agent`, the agent
//! pyaarlo documents) while the bus reports `userStreamActive`, it
//! returns the view's own `rtsps://` watch-along stream; asked as a
//! browser it returns a DASH URL that answers 502. The query reaches the
//! camera, so this adapter is only called when a view is known to run
//! (the orchestrator's user-view flag), never on a timer.

use std::sync::Arc;

use arlo_rs::client::ArloClient;
use arlo_rs::client::devices::ios_app_user_agent;
use async_trait::async_trait;
use tracing::debug;

use streamer_domain::camera::CameraId;
use streamer_domain::error::DomainError;
use streamer_domain::port::UserViewSource;
use streamer_domain::stream::WatchAlongUrl;

use crate::device_registry::DeviceRegistry;
use crate::error::arlo_to_domain;

/// Adapter exposing the app's watch-along stream as the domain
/// [`UserViewSource`] port.
pub struct ArloUserViewSourceAdapter {
    client: Arc<ArloClient>,
    devices: Arc<DeviceRegistry>,
    /// The Arlo app version the query identifies as
    /// (`arlo.app_version`).
    user_agent: String,
}

impl ArloUserViewSourceAdapter {
    /// Construct from the shared authenticated client, the device
    /// registry and the app version to identify as.
    #[must_use]
    pub fn new(client: Arc<ArloClient>, devices: Arc<DeviceRegistry>, app_version: &str) -> Self {
        Self {
            client,
            devices,
            user_agent: ios_app_user_agent(app_version),
        }
    }
}

#[async_trait]
impl UserViewSource for ArloUserViewSourceAdapter {
    async fn watch_along_url(&self, camera: &CameraId) -> Result<WatchAlongUrl, DomainError> {
        let device = self.devices.resolve(camera).await?;
        let answer = self
            .client
            .get_stream_url_as(&device, Some(&self.user_agent))
            .await
            .map_err(arlo_to_domain)?;
        let url = watch_along_from(answer.as_ref().map(arlo_rs::models::api::StreamUrl::as_str))?;
        debug!(%camera, url = %url, "watch-along stream obtained");
        Ok(url)
    }
}

/// Turn Arlo's answer into a relayable URL: no answer means no view is
/// running, a non-RTSP answer means Arlo did not treat us as the app.
fn watch_along_from(answer: Option<&str>) -> Result<WatchAlongUrl, DomainError> {
    let raw = answer.ok_or_else(|| {
        DomainError::AdapterTransport("Arlo returned no stream for the camera".to_string())
    })?;
    WatchAlongUrl::parse(raw).map_err(|_| {
        DomainError::AdapterTransport(
            "Arlo returned a stream that is not RTSP; the app identity was not honoured"
                .to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watch_along_from_accepts_the_apps_rtsps_answer() {
        let url = watch_along_from(Some(
            "rtsps://1.2.3.4:443/live/x?egressToken=T&watchalong=true",
        ))
        .expect("rtsps accepted");
        assert_eq!(url.redacted(), "rtsps://1.2.3.4:443/…");
    }

    #[test]
    fn watch_along_from_rejects_no_answer_and_dash_without_echoing_the_url() {
        let err = watch_along_from(None).unwrap_err();
        assert!(matches!(err, DomainError::AdapterTransport(_)));
        let err =
            watch_along_from(Some("https://h.arlo.com/x.mpd?egressToken=SECRET")).unwrap_err();
        assert!(matches!(err, DomainError::AdapterTransport(_)));
        assert!(!err.to_string().contains("SECRET"));
    }
}
