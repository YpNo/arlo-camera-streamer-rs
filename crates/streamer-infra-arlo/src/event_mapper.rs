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
//! | [`CameraEvent::ManualStream`] | `cameras/{device_id}` | `activityState == "userStreamActive"`     |
//! | [`CameraEvent::ManualStreamEnded`] | `cameras/{device_id}` | `activityState == "idle"`            |
//! | [`CameraEvent::Online`] | `cameras/{device_id}`   | `connectionState == "available"`              |
//! | [`CameraEvent::Offline`]| `cameras/{device_id}`   | `connectionState == "unavailable"`            |
//! | [`CameraEvent::SnapshotAvailable`] | `cameras/{device_id}` | `presignedLastImageUrl` is a non-empty string |
//!
//! Precedence is the table order: a payload carrying both a motion
//! flag and an activity state is the motion recording starting, and the
//! actionable signal wins. Other `activityState` values
//! (`alertStreamActive`, `startUserStream`, `fullFrameSnapshot`,
//! `startRecord`, `stopRecord`) map to nothing — `alertStreamActive` is
//! the consequence of a `motionDetected` that already arrived, and
//! mapping it too would double-count motion. The `activityState`
//! vocabulary is the pyaarlo one; confirmed against a live capture is
//! part of ADR 0005's checklist.
//!
//! Anything that doesn't match returns [`None`]. The orchestrator
//! never sees these — they're filtered out at the adapter boundary.

use arlo_rs::models::events::ArloEvent;
use serde_json::Value;

use streamer_domain::camera::CameraId;
use streamer_domain::event::CameraEvent;

const CAMERAS_PREFIX: &str = "cameras/";
const ACTIVITY_STATE: &str = "activityState";
const USER_STREAM_ACTIVE: &str = "userStreamActive";
const ACTIVITY_IDLE: &str = "idle";
const PRESIGNED_LAST_IMAGE_URL: &str = "presignedLastImageUrl";

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
    if let Some(mapped) = map_activity_state(device_id, props) {
        return Some(mapped);
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
    if snapshot_url(event).is_some() {
        return Some(CameraEvent::SnapshotAvailable {
            device_id: CameraId::new(device_id),
        });
    }
    None
}

/// `(device_id, url)` when the event announces a new snapshot of a
/// camera. The URL is a presigned credential: callers must not log it.
#[must_use]
pub fn snapshot_url(event: &ArloEvent) -> Option<(&str, &str)> {
    let device_id = extract_device_id(&event.resource)?;
    let url = event
        .properties
        .as_ref()?
        .get(PRESIGNED_LAST_IMAGE_URL)
        .and_then(Value::as_str)
        .filter(|u| !u.is_empty())?;
    Some((device_id, url))
}

fn map_activity_state(device_id: &str, props: &Value) -> Option<CameraEvent> {
    match activity_state(Some(props))? {
        USER_STREAM_ACTIVE => Some(CameraEvent::ManualStream {
            device_id: CameraId::new(device_id),
        }),
        ACTIVITY_IDLE => Some(CameraEvent::ManualStreamEnded {
            device_id: CameraId::new(device_id),
        }),
        _ => None,
    }
}

/// The `activityState` string of an event's properties, if any. Safe
/// to log: a small enum-like value, never a URL or a token.
#[must_use]
pub fn activity_state(props: Option<&Value>) -> Option<&str> {
    props?.get(ACTIVITY_STATE).and_then(Value::as_str)
}

/// The property keys of an event, sorted, for redacted diagnostics.
/// Keys only: property values can carry stream URLs and transaction ids.
#[must_use]
pub fn property_keys(props: Option<&Value>) -> Vec<&str> {
    let mut keys: Vec<&str> = props
        .and_then(Value::as_object)
        .map(|o| o.keys().map(String::as_str).collect())
        .unwrap_or_default();
    keys.sort_unstable();
    keys
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

    // ---------- Activity state (ADR 0005) ----------

    #[test]
    fn maps_user_stream_active_to_manual_stream() {
        let event = ev(
            "cameras/CAM4",
            Some(json!({ "activityState": "userStreamActive" })),
        );
        assert_eq!(
            map_event(&event),
            Some(CameraEvent::ManualStream {
                device_id: CameraId::new("CAM4")
            })
        );
    }

    #[test]
    fn maps_activity_idle_to_manual_stream_ended() {
        let event = ev("cameras/CAM4", Some(json!({ "activityState": "idle" })));
        assert_eq!(
            map_event(&event),
            Some(CameraEvent::ManualStreamEnded {
                device_id: CameraId::new("CAM4")
            })
        );
    }

    #[rstest]
    #[case("alertStreamActive")]
    #[case("startUserStream")]
    #[case("fullFrameSnapshot")]
    #[case("startRecord")]
    #[case("stopRecord")]
    #[case("")]
    fn ignores_other_activity_states(#[case] state: &str) {
        let event = ev("cameras/CAM4", Some(json!({ "activityState": state })));
        assert!(map_event(&event).is_none());
    }

    #[test]
    fn ignores_non_string_activity_state() {
        let event = ev("cameras/CAM4", Some(json!({ "activityState": 1 })));
        assert!(map_event(&event).is_none());
    }

    #[test]
    fn motion_takes_precedence_over_activity_state() {
        let event = ev(
            "cameras/CAM4",
            Some(json!({ "motionDetected": true, "activityState": "alertStreamActive" })),
        );
        assert!(matches!(
            map_event(&event),
            Some(CameraEvent::Motion { .. })
        ));
    }

    #[test]
    fn activity_state_takes_precedence_over_connection_state() {
        let event = ev(
            "cameras/CAM4",
            Some(json!({ "activityState": "userStreamActive", "connectionState": "available" })),
        );
        assert!(matches!(
            map_event(&event),
            Some(CameraEvent::ManualStream { .. })
        ));
    }

    #[test]
    fn property_keys_are_sorted_and_empty_without_properties() {
        assert!(property_keys(None).is_empty());
        let props = json!({ "zeta": 1, "alpha": "x", "mid": null });
        assert_eq!(property_keys(Some(&props)), vec!["alpha", "mid", "zeta"]);
        assert!(property_keys(Some(&json!("not an object"))).is_empty());
    }

    #[test]
    fn activity_state_reads_only_string_values() {
        assert_eq!(
            activity_state(Some(&json!({ "activityState": "idle" }))),
            Some("idle")
        );
        assert_eq!(activity_state(Some(&json!({ "activityState": 7 }))), None);
        assert_eq!(activity_state(None), None);
    }

    // ---------- Snapshot ----------

    #[test]
    fn maps_presigned_last_image_url_to_snapshot_available() {
        let event = ev(
            "cameras/CAM5",
            Some(json!({ "presignedLastImageUrl": "https://s3.example/x.jpg" })),
        );
        assert_eq!(
            map_event(&event),
            Some(CameraEvent::SnapshotAvailable {
                device_id: CameraId::new("CAM5")
            })
        );
        assert_eq!(
            snapshot_url(&event),
            Some(("CAM5", "https://s3.example/x.jpg"))
        );
    }

    #[rstest]
    #[case(json!({ "presignedLastImageUrl": "" }))]
    #[case(json!({ "presignedLastImageUrl": 7 }))]
    #[case(json!({ "batteryLevel": 80 }))]
    fn ignores_missing_or_malformed_snapshot_url(#[case] props: Value) {
        let event = ev("cameras/CAM5", Some(props));
        assert_eq!(snapshot_url(&event), None);
        assert!(map_event(&event).is_none());
    }

    #[test]
    fn snapshot_url_outside_a_camera_resource_is_ignored() {
        let event = ev(
            "mediaUploadNotification",
            Some(json!({ "presignedLastImageUrl": "https://s3.example/x.jpg" })),
        );
        assert_eq!(snapshot_url(&event), None);
    }

    #[test]
    fn motion_takes_precedence_over_a_snapshot_url() {
        let event = ev(
            "cameras/CAM5",
            Some(
                json!({ "motionDetected": true, "presignedLastImageUrl": "https://s3.example/x.jpg" }),
            ),
        );
        assert!(matches!(
            map_event(&event),
            Some(CameraEvent::Motion { .. })
        ));
        // The URL is still recorded by the adapter.
        assert!(snapshot_url(&event).is_some());
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
