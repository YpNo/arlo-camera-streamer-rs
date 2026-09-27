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
//! [`ArloEvent`]: arlo_rs::models::events::ArloEvent

use std::sync::Arc;

use arlo_rs::client::ArloClient;
use arlo_rs::events::ConnectionState as ArloConnectionState;
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use tokio_stream::wrappers::{BroadcastStream, WatchStream};
use tracing::warn;

use streamer_domain::error::DomainError;
use streamer_domain::event::{CameraEvent, ConnectionStatus};
use streamer_domain::port::ArloEventSource;

use crate::error::arlo_to_domain;
use crate::event_mapper::map_event;

/// Adapter that exposes the arlo-rs MQTT event bus as the domain
/// [`ArloEventSource`] port.
pub struct ArloEventSourceAdapter {
    client: Arc<ArloClient>,
}

impl ArloEventSourceAdapter {
    /// Construct from a shared [`ArloClient`] handle. The client must
    /// already be authenticated; the adapter does not boot the bus by
    /// itself — `events()` does that on first use.
    #[must_use]
    pub fn new(client: Arc<ArloClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ArloEventSource for ArloEventSourceAdapter {
    async fn subscribe(&self) -> Result<BoxStream<'static, CameraEvent>, DomainError> {
        let bus = self.client.events().await.map_err(arlo_to_domain)?;
        let rx = bus.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(|item| async move {
            match item {
                Ok(event) => map_event(&event),
                Err(err) => {
                    warn!(error = %err, "arlo event bus lagged; skipping");
                    None
                }
            }
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
