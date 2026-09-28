//! Inbound camera events — normalized vocabulary the application layer
//! reasons about.
//!
//! Adapters translate vendor-specific events (e.g. `arlo_rs::ArloEvent`)
//! into these variants so that the orchestrator never sees Arlo-specific
//! shapes.

use crate::camera::CameraId;

/// A normalized event emitted by an Arlo device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CameraEvent {
    /// Motion-detected pulse from the device's PIR/ML stack.
    Motion {
        /// Camera that triggered the event.
        device_id: CameraId,
    },
    /// Audio-trigger pulse (e.g. baby cry, glass break) — same handling
    /// as motion in this MVP, kept distinct for future routing.
    Audio {
        /// Camera that triggered the event.
        device_id: CameraId,
    },
    /// The user opened a live view in the Arlo app
    /// (`activityState == "userStreamActive"`). Observed only: Arlo
    /// refuses a second transport while the app streams, so motion
    /// activations pause until the view ends (ADR 0005).
    ManualStream {
        /// Camera the user is watching.
        device_id: CameraId,
    },
    /// The camera reported `activityState == "idle"`. Also arrives after
    /// motion recordings and after the daemon's own sessions; the
    /// orchestrator only uses it to end a known user view.
    ManualStreamEnded {
        /// Camera that went idle.
        device_id: CameraId,
    },
    /// Arlo published a fresh snapshot of the camera (after motion or on
    /// request). The adapter keeps the snapshot's location; the
    /// application only decides whether to refresh the idle still now.
    SnapshotAvailable {
        /// Camera the snapshot shows.
        device_id: CameraId,
    },
    /// Device transitioned to online / reachable.
    Online {
        /// Camera whose connectivity changed.
        device_id: CameraId,
    },
    /// Device transitioned to offline / unreachable.
    Offline {
        /// Camera whose connectivity changed.
        device_id: CameraId,
    },
}

impl CameraEvent {
    /// Borrow the camera id this event applies to, regardless of variant.
    pub fn device_id(&self) -> &CameraId {
        match self {
            Self::Motion { device_id }
            | Self::Audio { device_id }
            | Self::ManualStream { device_id }
            | Self::ManualStreamEnded { device_id }
            | Self::SnapshotAvailable { device_id }
            | Self::Online { device_id }
            | Self::Offline { device_id } => device_id,
        }
    }
}

/// Connection status of the upstream Arlo MQTT event bus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    /// Initial state or attempting to (re)establish.
    Connecting,
    /// MQTT session up; events may flow.
    Connected,
    /// Stream closed; orchestrator should pause activations.
    Disconnected,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_id_accessor_covers_all_variants() {
        let id = CameraId::new("CAM");
        for ev in [
            CameraEvent::Motion {
                device_id: id.clone(),
            },
            CameraEvent::Audio {
                device_id: id.clone(),
            },
            CameraEvent::ManualStream {
                device_id: id.clone(),
            },
            CameraEvent::ManualStreamEnded {
                device_id: id.clone(),
            },
            CameraEvent::SnapshotAvailable {
                device_id: id.clone(),
            },
            CameraEvent::Online {
                device_id: id.clone(),
            },
            CameraEvent::Offline {
                device_id: id.clone(),
            },
        ] {
            assert_eq!(ev.device_id().as_str(), "CAM");
        }
    }
}
