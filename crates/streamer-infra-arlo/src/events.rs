//! [`ArloEventSource`] implementation backed by `arlo_rs::events::EventBus`.
//!
//! Wraps the broadcast / watch channels exposed by arlo-rs behind
//! [`futures::Stream`]s, and translates each [`ArloEvent`] into the
//! domain [`CameraEvent`] vocabulary via
//! [`crate::event_mapper::map_event`].
//!
//! # Lag behavior
//!
//! `tokio::sync::broadcast` returns
//! [`tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged`]
//! when a slow consumer falls behind the channel's ring buffer.
//! We log a warning at `info` level and skip — losing N events is
//! recoverable because every event from a still-active camera will
//! re-fire on the next motion pulse, and connection-state changes are
//! observed via the dedicated watch channel.
//!
//! # Capture aid (ADR 0005)
//!
//! Every raw bus event is logged at `trace`; every unmapped `cameras/*`
//! event and every motion / audio pulse at `debug`; all **without
//! property values** (they
//! can carry presigned URLs and transaction ids): action, resource,
//! source, the sorted property keys and the `activityState` string. To
//! see what Arlo emits when a live view starts in the mobile app:
//!
//! ```text
//! RUST_LOG=info,streamer_infra_arlo::events=debug   # pulses + unmapped camera events
//! RUST_LOG=info,streamer_infra_arlo::events=trace   # every event
//! ```
//!
//! [`ArloEvent`]: arlo_rs::models::events::ArloEvent

use std::sync::Arc;
use std::time::Instant;

use arlo_rs::client::ArloClient;
use arlo_rs::events::ConnectionState as ArloConnectionState;
use arlo_rs::models::events::ArloEvent;
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use tokio_stream::wrappers::{BroadcastStream, WatchStream};
use tracing::{debug, trace, warn};

use streamer_domain::camera::CameraId;
use streamer_domain::error::DomainError;
use streamer_domain::event::{CameraEvent, ConnectionStatus};
use streamer_domain::port::ArloEventSource;

use crate::error::arlo_to_domain;
use crate::event_mapper::{activity_state, map_event, property_keys, snapshot_url};
use crate::snapshot_cache::SnapshotUrlCache;

/// Adapter that exposes the arlo-rs MQTT event bus as the domain
/// [`ArloEventSource`] port.
pub struct ArloEventSourceAdapter {
    client: Arc<ArloClient>,
    snapshots: Arc<SnapshotUrlCache>,
}

impl ArloEventSourceAdapter {
    /// Construct from a shared [`ArloClient`] handle. The client must
    /// already be authenticated; the adapter does not boot the bus by
    /// itself — `events()` does that on first use. Snapshot URLs seen on
    /// the bus are recorded in `snapshots`, shared with the thumbnail
    /// adapter.
    #[must_use]
    pub fn new(client: Arc<ArloClient>, snapshots: Arc<SnapshotUrlCache>) -> Self {
        Self { client, snapshots }
    }
}

#[async_trait]
impl ArloEventSource for ArloEventSourceAdapter {
    async fn subscribe(&self) -> Result<BoxStream<'static, CameraEvent>, DomainError> {
        let bus = self.client.events().await.map_err(arlo_to_domain)?;
        let rx = bus.subscribe();
        let snapshots = self.snapshots.clone();
        let stream = BroadcastStream::new(rx).filter_map(move |item| {
            let mapped = match item {
                Ok(event) => translate(&event, &snapshots),
                Err(err) => {
                    warn!(error = %err, "arlo event bus lagged; skipping");
                    None
                }
            };
            futures::future::ready(mapped)
        });
        Ok(Box::pin(stream))
    }

    async fn connection_status(&self) -> Result<BoxStream<'static, ConnectionStatus>, DomainError> {
        let bus = self.client.events().await.map_err(arlo_to_domain)?;
        let rx = bus.connection_state();
        let stream = WatchStream::new(rx).map(|state| map_connection_state(&state));
        Ok(Box::pin(stream))
    }
}

/// Map one bus event, recording its snapshot URL (if any) on the way.
fn translate(event: &ArloEvent, snapshots: &SnapshotUrlCache) -> Option<CameraEvent> {
    if let Some((device_id, url)) = snapshot_url(event) {
        // Recorded only under a valid camera id: the raw resource suffix
        // is cloud input, and every distinct one used to stay in the map.
        if let Ok(camera) = CameraId::parse(device_id) {
            if !snapshots.record(&camera, url, Instant::now()) {
                debug!(%camera, "snapshot URL rejected (not https to a public host)");
            }
        } else {
            debug!(
                device_id_bytes = device_id.len(),
                "snapshot URL for a malformed device id ignored"
            );
        }
    }
    let mapped = map_event(event);
    observe_raw(event, mapped.as_ref());
    mapped
}

/// Redacted diagnostics for the capture workflow (module docs): never
/// the property values.
fn observe_raw(event: &ArloEvent, mapped: Option<&CameraEvent>) {
    let keys = property_keys(event.properties.as_ref());
    let activity = activity_state(event.properties.as_ref());
    // Raw cloud strings go out through `Debug`, which escapes newlines
    // and control characters: the text format would print them as is,
    // and a resource like `cameras/x\nINFO …` forged a log line.
    trace!(
        action = ?event.action,
        resource = ?event.resource,
        source = ?event.source,
        ?keys,
        ?activity,
        ?mapped,
        "arlo bus event"
    );
    if let Some(kind) = mapped.and_then(pulse_kind) {
        debug!(resource = ?event.resource, kind, "camera trigger pulse");
    } else if mapped.is_none() && event.resource.starts_with("cameras/") {
        debug!(
            resource = ?event.resource,
            ?keys,
            ?activity,
            "unmapped camera event"
        );
    }
}

/// Short label for a trigger pulse, `None` for every other event.
const fn pulse_kind(event: &CameraEvent) -> Option<&'static str> {
    match event {
        CameraEvent::Motion { .. } => Some("motion"),
        CameraEvent::Audio { .. } => Some("audio"),
        _ => None,
    }
}

const fn map_connection_state(state: &ArloConnectionState) -> ConnectionStatus {
    match state {
        ArloConnectionState::Connecting => ConnectionStatus::Connecting,
        ArloConnectionState::Connected => ConnectionStatus::Connected,
        ArloConnectionState::Disconnected => ConnectionStatus::Disconnected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use streamer_domain::camera::CameraId;

    #[test]
    fn pulse_kind_labels_motion_and_audio_only() {
        let cam = || CameraId::new("CAM");
        assert_eq!(
            pulse_kind(&CameraEvent::Motion { device_id: cam() }),
            Some("motion")
        );
        assert_eq!(
            pulse_kind(&CameraEvent::Audio { device_id: cam() }),
            Some("audio")
        );
        assert_eq!(pulse_kind(&CameraEvent::Online { device_id: cam() }), None);
    }

    #[test]
    fn observe_raw_handles_mapped_and_unmapped_events_without_panicking() {
        let unmapped = ArloEvent {
            action: "is".to_string(),
            resource: "cameras/CAM".to_string(),
            publish_response: None,
            properties: Some(serde_json::json!({ "batteryLevel": 80, "activityState": "idle" })),
            source: Some("BASE".to_string()),
            trans_id: None,
            active_mode: None,
        };
        observe_raw(&unmapped, None);
        let mapped = CameraEvent::ManualStreamEnded {
            device_id: streamer_domain::camera::CameraId::new("CAM"),
        };
        observe_raw(&unmapped, Some(&mapped));
    }

    #[test]
    fn maps_all_connection_state_variants() {
        assert_eq!(
            map_connection_state(&ArloConnectionState::Connecting),
            ConnectionStatus::Connecting
        );
        assert_eq!(
            map_connection_state(&ArloConnectionState::Connected),
            ConnectionStatus::Connected
        );
        assert_eq!(
            map_connection_state(&ArloConnectionState::Disconnected),
            ConnectionStatus::Disconnected
        );
    }
}
