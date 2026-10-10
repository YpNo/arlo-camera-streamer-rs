//! What the MQTT adapter publishes: topics, state documents, Home
//! Assistant discovery documents, and the record of what was last written.
//! Pure: no I/O.

use std::collections::HashMap;

use bytes::Bytes;
use serde_json::{Value, json};

use streamer_domain::admin::CameraSnapshot;
use streamer_domain::camera::CameraId;
use streamer_domain::config::MqttConfig;

/// One retained message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Message {
    pub topic: String,
    pub payload: Bytes,
}

impl Message {
    fn json(topic: String, value: &Value) -> Self {
        Self {
            topic,
            payload: Bytes::from(value.to_string()),
        }
    }

    /// An empty retained payload: the broker forgets the topic and Home
    /// Assistant removes the entity.
    fn cleared(topic: String) -> Self {
        Self {
            topic,
            payload: Bytes::new(),
        }
    }
}

/// `online` / `offline` on [`Layout::status`].
pub(crate) const ONLINE: &str = "online";
pub(crate) const OFFLINE: &str = "offline";

/// Where everything goes, from the `[mqtt]` settings.
#[derive(Debug, Clone)]
pub(crate) struct Layout {
    base: String,
    discovery: Option<String>,
}

/// The Home Assistant entities of one camera: component, object id.
const CAMERA_ENTITIES: [(&str, &str); 5] = [
    ("sensor", "state"),
    ("binary_sensor", "live"),
    ("binary_sensor", "user_view"),
    ("sensor", "live_secs_today"),
    ("camera", "idle_picture"),
];

impl Layout {
    pub(crate) fn new(config: &MqttConfig) -> Self {
        Self {
            base: config.base_topic.clone(),
            discovery: (!config.discovery_prefix.is_empty())
                .then(|| config.discovery_prefix.clone()),
        }
    }

    /// Availability: `online` / `offline`, retained; also the last will.
    pub(crate) fn status(&self) -> String {
        format!("{}/status", self.base)
    }

    fn bridge(&self) -> String {
        format!("{}/bridge", self.base)
    }

    fn state(&self, camera: &CameraId) -> String {
        format!("{}/{camera}/state", self.base)
    }

    fn thumbnail(&self, camera: &CameraId) -> String {
        format!("{}/{camera}/thumbnail", self.base)
    }

    fn discovery_topic(&self, component: &str, node: &str, object: &str) -> Option<String> {
        self.discovery
            .as_ref()
            .map(|prefix| format!("{prefix}/{component}/{node}/{object}/config"))
    }

    /// The broker's client id: one per base topic, so two daemons on one
    /// broker (with different base topics) do not take over each other's
    /// session.
    pub(crate) fn client_id(&self) -> String {
        format!("arlo-camera-streamer-{}", self.base.replace('/', "-"))
    }

    fn bridge_device_id(&self) -> String {
        format!("arlo_streamer_bridge_{}", self.base.replace('/', "_"))
    }
}

/// The availability message.
pub(crate) fn status(layout: &Layout, value: &str) -> Message {
    Message {
        topic: layout.status(),
        payload: Bytes::from(value.to_string()),
    }
}

/// The bridge's own state.
pub(crate) fn bridge_state(layout: &Layout, arlo_connected: bool, version: &str) -> Message {
    Message::json(
        layout.bridge(),
        &json!({ "arlo_connected": arlo_connected, "version": version }),
    )
}

/// One camera's state document.
pub(crate) fn camera_state(layout: &Layout, camera: &CameraSnapshot) -> Message {
    Message::json(
        layout.state(&camera.id),
        &json!({
            "stream_name": camera.stream_name.as_str(),
            "state": camera.state,
            "live": camera.state == "live",
            "live_source": camera.live_source,
            "user_view": camera.user_view,
            "live_secs_today": camera.live_secs_today,
            "daily_budget_secs": camera.daily_budget_secs,
            "last_failure": camera.last_failure,
        }),
    )
}

/// One camera's idle picture.
pub(crate) fn thumbnail(layout: &Layout, camera: &CameraId, jpeg: Bytes) -> Message {
    Message {
        topic: layout.thumbnail(camera),
        payload: jpeg,
    }
}

/// Discovery documents of the bridge device (none with discovery off).
pub(crate) fn bridge_discovery(layout: &Layout, version: &str) -> Vec<Message> {
    let node = layout.bridge_device_id();
    let Some(topic) = layout.discovery_topic("binary_sensor", &node, "arlo_connected") else {
        return Vec::new();
    };
    vec![Message::json(
        topic,
        &json!({
            "name": "Arlo connection",
            "unique_id": format!("{node}_arlo_connected"),
            "device_class": "connectivity",
            "state_topic": layout.bridge(),
            "value_template": "{{ 'ON' if value_json.arlo_connected else 'OFF' }}",
            "availability_topic": layout.status(),
            "device": {
                "identifiers": [node],
                "name": "Arlo camera streamer",
                "manufacturer": "arlo-camera-streamer",
                "sw_version": version,
            },
        }),
    )]
}

/// Discovery documents of one camera (none with discovery off); the idle
/// picture's only with `thumbnails`.
pub(crate) fn camera_discovery(
    layout: &Layout,
    camera: &CameraSnapshot,
    thumbnails: bool,
) -> Vec<Message> {
    let node = node_id(&camera.id);
    let device = json!({
        "identifiers": [node.clone()],
        "name": camera.stream_name.as_str(),
        "manufacturer": "Arlo",
        "model": "camera (arlo-camera-streamer)",
        "via_device": layout.bridge_device_id(),
    });
    let state_topic = layout.state(&camera.id);
    let entity = |object: &str, name: &str, extra: Value| {
        let mut doc = json!({
            "name": name,
            "unique_id": format!("{node}_{object}"),
            "availability_topic": layout.status(),
            "device": device.clone(),
        });
        if let (Some(map), Value::Object(more)) = (doc.as_object_mut(), extra) {
            map.extend(more);
        }
        doc
    };
    let mut documents = vec![
        (
            "sensor",
            "state",
            entity(
                "state",
                "State",
                json!({ "state_topic": state_topic, "value_template": "{{ value_json.state }}", "icon": "mdi:cctv" }),
            ),
        ),
        (
            "binary_sensor",
            "live",
            entity(
                "live",
                "Live",
                json!({ "state_topic": state_topic, "value_template": "{{ 'ON' if value_json.live else 'OFF' }}", "icon": "mdi:record-rec" }),
            ),
        ),
        (
            "binary_sensor",
            "user_view",
            entity(
                "user_view",
                "Viewed in the Arlo app",
                json!({ "state_topic": state_topic, "value_template": "{{ 'ON' if value_json.user_view else 'OFF' }}", "icon": "mdi:cellphone-play" }),
            ),
        ),
        (
            "sensor",
            "live_secs_today",
            entity(
                "live_secs_today",
                "Live time today",
                json!({ "state_topic": state_topic, "value_template": "{{ value_json.live_secs_today }}", "unit_of_measurement": "s", "device_class": "duration", "state_class": "total_increasing" }),
            ),
        ),
    ];
    if thumbnails {
        documents.push((
            "camera",
            "idle_picture",
            entity(
                "idle_picture",
                "Idle picture",
                json!({ "topic": layout.thumbnail(&camera.id) }),
            ),
        ));
    }
    documents
        .into_iter()
        .filter_map(|(component, object, doc)| {
            layout
                .discovery_topic(component, &node, object)
                .map(|topic| Message::json(topic, &doc))
        })
        .collect()
}

/// Empty retained messages for every topic of a camera no longer served:
/// its state, its picture, all its discovery documents.
pub(crate) fn removal(layout: &Layout, camera: &CameraId) -> Vec<Message> {
    let node = node_id(camera);
    let mut topics = vec![layout.state(camera), layout.thumbnail(camera)];
    topics.extend(
        CAMERA_ENTITIES
            .iter()
            .filter_map(|(component, object)| layout.discovery_topic(component, &node, object)),
    );
    topics.into_iter().map(Message::cleared).collect()
}

/// The camera's Home Assistant node id: from its device id only, so a
/// renamed stream keeps its entities.
fn node_id(camera: &CameraId) -> String {
    format!("arlo_streamer_{camera}")
}

/// What was last written to each topic during this connection: a message
/// is due when it differs. Written, not seen: an update lost with the
/// connection is due again on the next tick.
#[derive(Debug, Default)]
pub(crate) struct Written(HashMap<String, Bytes>);

impl Written {
    pub(crate) fn is_due(&self, message: &Message) -> bool {
        self.0.get(&message.topic) != Some(&message.payload)
    }

    pub(crate) fn record(&mut self, message: &Message) {
        self.0
            .insert(message.topic.clone(), message.payload.clone());
    }

    /// Forget everything: a new connection writes it all again (a broker
    /// restarted without persistence lost the retained messages).
    pub(crate) fn clear(&mut self) {
        self.0.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use streamer_domain::camera::StreamName;

    fn layout(discovery: &str) -> Layout {
        Layout::new(&MqttConfig {
            url: "mqtt://broker".into(),
            username: None,
            password_env: None,
            ca_file: None,
            base_topic: "arlo".into(),
            discovery_prefix: discovery.into(),
            thumbnails: false,
        })
    }

    fn snapshot(state: &str) -> CameraSnapshot {
        CameraSnapshot {
            id: CameraId::new("CAM1"),
            stream_name: StreamName::parse("front_door").unwrap(),
            state: state.into(),
            live_secs_today: 42,
            daily_budget_secs: 600,
            live_source: (state == "live").then(|| "motion".to_string()),
            last_failure: None,
            retries: 0,
            user_view: false,
        }
    }

    fn json_of(message: &Message) -> Value {
        serde_json::from_slice(&message.payload).expect("JSON payload")
    }

    #[test]
    fn the_state_document_is_keyed_by_device_id_and_carries_the_snapshot() {
        let message = camera_state(&layout("homeassistant"), &snapshot("live"));
        assert_eq!(message.topic, "arlo/CAM1/state");
        let doc = json_of(&message);
        assert_eq!(doc["state"], "live");
        assert_eq!(doc["live"], true);
        assert_eq!(doc["live_source"], "motion");
        assert_eq!(doc["stream_name"], "front_door");
        assert_eq!(doc["live_secs_today"], 42);
        assert_eq!(
            json_of(&camera_state(&layout(""), &snapshot("idle")))["live"],
            false
        );
    }

    #[test]
    fn discovery_ties_every_entity_to_the_cameras_device_and_availability() {
        let messages = camera_discovery(&layout("homeassistant"), &snapshot("idle"), false);
        let topics: Vec<&str> = messages.iter().map(|m| m.topic.as_str()).collect();
        assert_eq!(
            topics,
            [
                "homeassistant/sensor/arlo_streamer_CAM1/state/config",
                "homeassistant/binary_sensor/arlo_streamer_CAM1/live/config",
                "homeassistant/binary_sensor/arlo_streamer_CAM1/user_view/config",
                "homeassistant/sensor/arlo_streamer_CAM1/live_secs_today/config",
            ]
        );
        for message in &messages {
            let doc = json_of(message);
            assert_eq!(doc["availability_topic"], "arlo/status");
            assert_eq!(doc["state_topic"], "arlo/CAM1/state");
            assert_eq!(doc["device"]["identifiers"][0], "arlo_streamer_CAM1");
            assert!(
                doc["unique_id"]
                    .as_str()
                    .unwrap()
                    .starts_with("arlo_streamer_CAM1_")
            );
        }
    }

    /// The picture leaves the admin token's protection: only when asked.
    #[test]
    fn the_idle_picture_entity_exists_only_with_thumbnails_on() {
        let with = camera_discovery(&layout("homeassistant"), &snapshot("idle"), true);
        let picture = with.last().unwrap();
        assert_eq!(
            picture.topic,
            "homeassistant/camera/arlo_streamer_CAM1/idle_picture/config"
        );
        assert_eq!(json_of(picture)["topic"], "arlo/CAM1/thumbnail");
        assert_eq!(with.len(), 5);
    }

    #[test]
    fn discovery_off_publishes_no_discovery_documents() {
        assert_eq!(camera_discovery(&layout(""), &snapshot("idle"), true), []);
        assert_eq!(bridge_discovery(&layout(""), "0.3.0"), []);
        let removed = removal(&layout(""), &CameraId::new("CAM1"));
        assert_eq!(removed.len(), 2, "state and thumbnail only");
    }

    #[test]
    fn removal_clears_every_topic_the_camera_could_have() {
        let removed = removal(&layout("homeassistant"), &CameraId::new("OLD"));
        assert_eq!(removed.len(), 2 + CAMERA_ENTITIES.len());
        assert!(removed.iter().all(|m| m.payload.is_empty()));
        assert!(removed.iter().any(|m| m.topic == "arlo/OLD/state"));
        assert!(
            removed
                .iter()
                .any(|m| m.topic == "homeassistant/camera/arlo_streamer_OLD/idle_picture/config")
        );
    }

    #[test]
    fn the_bridge_is_a_connectivity_sensor_on_the_bridge_state() {
        let messages = bridge_discovery(&layout("homeassistant"), "0.3.0");
        let doc = json_of(&messages[0]);
        assert_eq!(doc["device_class"], "connectivity");
        assert_eq!(doc["state_topic"], "arlo/bridge");
        assert_eq!(doc["device"]["sw_version"], "0.3.0");
        let state = json_of(&bridge_state(&layout("x"), true, "0.3.0"));
        assert_eq!(state["arlo_connected"], true);
    }

    #[test]
    fn written_marks_a_message_due_until_the_same_payload_is_recorded() {
        let mut written = Written::default();
        let idle = camera_state(&layout("x"), &snapshot("idle"));
        let live = camera_state(&layout("x"), &snapshot("live"));
        assert!(written.is_due(&idle));
        written.record(&idle);
        assert!(!written.is_due(&idle));
        assert!(written.is_due(&live));
        written.clear();
        assert!(written.is_due(&idle));
    }

    #[test]
    fn client_ids_differ_per_base_topic() {
        assert_eq!(layout("x").client_id(), "arlo-camera-streamer-arlo");
        let nested = Layout {
            base: "home/arlo".into(),
            discovery: None,
        };
        assert_eq!(nested.client_id(), "arlo-camera-streamer-home-arlo");
    }
}
