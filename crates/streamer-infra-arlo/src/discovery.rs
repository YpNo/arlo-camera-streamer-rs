//! Account device discovery for the `list-devices` CLI.
//!
//! Lists the devices on the Arlo account through the authenticated
//! client and keeps the ones the streamer can serve. Read-only: the
//! device list is a cloud query that does not reach the cameras.
//!
//! The values are cloud input printed to a terminal and into a TOML
//! snippet the operator pastes: ids must pass [`CameraId::parse`] (a
//! device that fails is skipped with a warning), and names, kinds and
//! models lose their control and invisible characters, so a device named
//! with a newline cannot add lines to the configuration.

use arlo_rs::client::ArloClient;
use arlo_rs::models::api::Device;

use streamer_domain::camera::{CameraId, DiscoveredDevice};
use streamer_domain::error::{DomainError, sanitize_reason};
use tracing::warn;

use crate::error::arlo_to_domain;

/// Arlo device classes that carry a camera the streamer can serve.
/// Base stations, bridges and chimes are left out.
const STREAMABLE_KINDS: [&str; 3] = ["camera", "doorbell", "arloq"];

/// The account's streamable devices, sorted by display name.
///
/// # Errors
///
/// [`DomainError::AdapterTransport`] when the device list cannot be
/// fetched.
pub async fn discover_devices(client: &ArloClient) -> Result<Vec<DiscoveredDevice>, DomainError> {
    let devices = client.get_devices().await.map_err(arlo_to_domain)?;
    Ok(streamable(&devices))
}

fn streamable(devices: &[Device]) -> Vec<DiscoveredDevice> {
    let candidates = devices
        .iter()
        .filter(|d| STREAMABLE_KINDS.contains(&d.device_type.as_str()));
    let mut skipped = 0_usize;
    let mut found: Vec<DiscoveredDevice> = candidates
        .filter_map(|d| {
            let Ok(id) = CameraId::parse(&d.device_id) else {
                skipped += 1;
                return None;
            };
            Some(DiscoveredDevice {
                id,
                name: sanitize_reason(&d.device_name),
                kind: sanitize_reason(&d.device_type),
                model: d.model_id.as_deref().map(sanitize_reason),
            })
        })
        .collect();
    if skipped > 0 {
        warn!(
            skipped,
            "devices with an id outside [A-Za-z0-9_-]{{1,64}} were left out"
        );
    }
    found.sort_by_key(|d| d.name.to_lowercase());
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    fn devices() -> Vec<Device> {
        serde_json::from_str(
            r#"[
            {"deviceId":"CAM2","parentId":"BS1","deviceType":"camera","deviceName":"outdoor",
             "uniqueId":"u2","state":"provisioned","modelId":"VMC4041P"},
            {"deviceId":"BS1","parentId":"BS1","deviceType":"basestation","deviceName":"Hub",
             "uniqueId":"u0","state":"provisioned"},
            {"deviceId":"DB1","parentId":"DB1","deviceType":"doorbell","deviceName":"Door",
             "uniqueId":"u3","state":"provisioned"},
            {"deviceId":"CH1","parentId":"DB1","deviceType":"chime","deviceName":"Chime",
             "uniqueId":"u4","state":"provisioned"},
            {"deviceId":"CAM1","parentId":"CAM1","deviceType":"camera","deviceName":"Attic",
             "uniqueId":"u1","state":"provisioned"}
        ]"#,
        )
        .expect("fixture parses")
    }

    #[test]
    fn streamable_keeps_cameras_and_doorbells_sorted_by_name() {
        let found = streamable(&devices());
        let names: Vec<&str> = found.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["Attic", "Door", "outdoor"]);
    }

    #[test]
    fn streamable_maps_id_kind_and_model() {
        let found = streamable(&devices());
        let outdoor = found.iter().find(|d| d.name == "outdoor").expect("present");
        assert_eq!(outdoor.id, CameraId::new("CAM2"));
        assert_eq!(outdoor.kind, "camera");
        assert_eq!(outdoor.model.as_deref(), Some("VMC4041P"));
        let door = found.iter().find(|d| d.name == "Door").expect("present");
        assert_eq!(door.model, None);
    }

    #[test]
    fn streamable_of_an_account_without_cameras_is_empty() {
        assert_eq!(streamable(&[]).len(), 0);
    }

    /// A device name with a newline added live lines to the snippet the
    /// operator pastes; a quote in an id escaped its TOML string.
    #[test]
    fn streamable_strips_control_characters_and_skips_malformed_ids() {
        let devices: Vec<Device> = serde_json::from_str(
            r#"[
            {"deviceId":"CAM1","parentId":"CAM1","deviceType":"camera",
             "deviceName":"Attic\n[output.hls]\ndir = \"/etc\"\u001b[31m",
             "uniqueId":"u1","state":"provisioned"},
            {"deviceId":"CAM\"2","parentId":"CAM2","deviceType":"camera","deviceName":"Bad id",
             "uniqueId":"u2","state":"provisioned"}
        ]"#,
        )
        .expect("fixture parses");

        let found = streamable(&devices);

        assert_eq!(found.len(), 1, "the malformed id is left out");
        assert!(!found[0].name.contains('\n'), "{:?}", found[0].name);
        assert!(!found[0].name.contains('\u{1b}'), "{:?}", found[0].name);
    }
}
