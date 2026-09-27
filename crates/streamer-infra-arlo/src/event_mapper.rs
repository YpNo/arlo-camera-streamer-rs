//! Pure mapping from [`ArloEvent`] to the domain [`CameraEvent`]
//! vocabulary.
//!
//! The Arlo event bus delivers a wide variety of resource / action /
//! property combinations; the streamer only cares about a small subset:
//!
//! | Trigger                 | Resource pattern        | Property predicate                            |
//! |-------------------------|-------------------------|-----------------------------------------------|
//! | [`CameraEvent::Motion`] | `cameras/{device_id}`   | `motionDetected == true`                      |
//! | [`CameraEvent::Audio`]  | `cameras/{device_id}`   | `audioDetected == true`                       |
//! | [`CameraEvent::Online`] | `cameras/{device_id}`   | `connectionState == "available"`              |
//! | [`CameraEvent::Offline`]| `cameras/{device_id}`   | `connectionState == "unavailable"`            |
//!
//! Anything that doesn't match returns [`None`]. The orchestrator
//! never sees these — they're filtered out at the adapter boundary.

use arlo_rs::models::events::ArloEvent;
use serde_json::Value;

use streamer_domain::camera::CameraId;
use streamer_domain::event::CameraEvent;

const CAMERAS_PREFIX: &str = "cameras/";

/// Translate an [`ArloEvent`] to a domain [`CameraEvent`], returning
/// [`None`] when the event is uninteresting or malformed.
#[must_use]
pub fn map_event(event: &ArloEvent) -> Option<CameraEvent> {
    let device_id = extract_device_id(&event.resource)?;
    let props = event.properties.as_ref()?;

    if matches_bool_true(props, "motionDetected") {
        return Some(CameraEvent::Motion {
            device_id: CameraId::new(device_id),
        });
    }
    if matches_bool_true(props, "audioDetected") {
        return Some(CameraEvent::Audio {
            device_id: CameraId::new(device_id),
        });
    }
    if let Some(state) = props.get("connectionState").and_then(Value::as_str) {
        return match state {
            "available" => Some(CameraEvent::Online {
                device_id: CameraId::new(device_id),
            }),
            "unavailable" => Some(CameraEvent::Offline {
                device_id: CameraId::new(device_id),
            }),
            _ => None,
        };
    }
    None
}

fn extract_device_id(resource: &str) -> Option<&str> {
    resource
        .strip_prefix(CAMERAS_PREFIX)
        .filter(|id| !id.is_empty())
}

fn matches_bool_true(props: &Value, key: &str) -> bool {
    props.get(key).and_then(Value::as_bool) == Some(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    fn ev(resource: &str, properties: Option<Value>) -> ArloEvent {
        ArloEvent {
            action: "is".to_string(),
            resource: resource.to_string(),
            publish_response: None,
            properties,
            source: None,
            trans_id: None,
            active_mode: None,
        }
    }

    // ---------- Motion ----------

    #[test]
    fn maps_motion_detected() {
        let event = ev("cameras/CAM1", Some(json!({ "motionDetected": true })));
        let result = map_event(&event).expect("should map");
        assert_eq!(
            result,
            CameraEvent::Motion {
                device_id: CameraId::new("CAM1")
            }
        );
    }

    #[test]
    fn ignores_motion_detected_false() {
        let event = ev("cameras/CAM1", Some(json!({ "motionDetected": false })));
        assert!(map_event(&event).is_none());
    }

    // ---------- Audio ----------

    #[test]
    fn maps_audio_detected() {
        let event = ev("cameras/CAM2", Some(json!({ "audioDetected": true })));
        assert_eq!(
            map_event(&event),
            Some(CameraEvent::Audio {
                device_id: CameraId::new("CAM2")
            })
        );
    }

    // ---------- Connection state ----------

    #[rstest]
    #[case("available", CameraEvent::Online { device_id: CameraId::new("CAM3") })]
    #[case("unavailable", CameraEvent::Offline { device_id: CameraId::new("CAM3") })]
    fn maps_connection_state(#[case] state: &str, #[case] expected: CameraEvent) {
        let event = ev("cameras/CAM3", Some(json!({ "connectionState": state })));
        assert_eq!(map_event(&event), Some(expected));
    }

    #[test]
    fn ignores_unknown_connection_state() {
        let event = ev("cameras/CAM3", Some(json!({ "connectionState": "weird" })));
        assert!(map_event(&event).is_none());
    }

    // ---------- Resource filtering ----------

    #[rstest]
    #[case("modes")]
    #[case("subscriptions/USER123")]
    #[case("doorbells/DBELL1")]
    #[case("cameras/")] // empty device id
    #[case("camera/CAM1")] // singular, not the prefix we expect
    #[case("")]
    fn ignores_non_camera_resources(#[case] resource: &str) {
        let event = ev(resource, Some(json!({ "motionDetected": true })));
        assert!(map_event(&event).is_none());
    }

    // ---------- Property handling ----------

    #[test]
    fn ignores_event_without_properties() {
        let event = ev("cameras/CAM1", None);
        assert!(map_event(&event).is_none());
    }

    #[test]
    fn ignores_event_with_unknown_properties() {
        let event = ev(
            "cameras/CAM1",
            Some(json!({ "batteryLevel": 80, "signalStrength": 4 })),
        );
        assert!(map_event(&event).is_none());
    }

    #[test]
    fn motion_takes_precedence_over_other_keys() {
        // If both motionDetected and connectionState appear in one event,
        // motion wins (it's the actionable signal).
        let event = ev(
            "cameras/CAM1",
            Some(json!({
                "motionDetected": true,
                "connectionState": "available",
            })),
        );
        assert!(matches!(
            map_event(&event),
            Some(CameraEvent::Motion { .. })
        ));
    }

    #[test]
    fn ignores_non_bool_motion_value() {
        // Defensive: a malformed "motionDetected": "true" (string) is rejected.
        let event = ev("cameras/CAM1", Some(json!({ "motionDetected": "true" })));
        assert!(map_event(&event).is_none());
    }
}
