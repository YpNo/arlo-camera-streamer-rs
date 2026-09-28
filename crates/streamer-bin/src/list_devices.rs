//! `list-devices` subcommand: sign in, list the account's streamable
//! devices, and suggest `[[cameras]]` blocks for the ones not configured.
//!
//! Starts no server and needs no admin token. It shares the daemon's
//! session cache and does not log out, so running it once completes the
//! MFA pairing and the daemon's first start needs no OTP.

use std::collections::HashSet;
use std::fmt::Write as _;

use anyhow::{Context, Result};

use streamer_domain::camera::{DiscoveredDevice, StreamName};
use streamer_domain::config::{CameraConfig, StreamerConfig};
use streamer_infra_arlo::{boot, discover_devices};

/// Authenticate, discover, print the report on stdout.
///
/// # Errors
///
/// Authentication or device-list failures, with context.
pub async fn run(config: &StreamerConfig) -> Result<()> {
    let client = boot(&config.arlo)
        .await
        .context("failed to sign in to Arlo")?;
    let devices = discover_devices(&client)
        .await
        .context("failed to fetch the Arlo device list")?;
    print!("{}", render(&devices, &config.cameras));
    Ok(())
}

/// The whole report: device table, unknown configured ids, snippet.
pub fn render(devices: &[DiscoveredDevice], configured: &[CameraConfig]) -> String {
    let mut out = render_table(devices, configured);
    out.push_str(&render_unknown(devices, configured));
    out.push_str(&render_snippet(devices, configured));
    out
}

fn render_table(devices: &[DiscoveredDevice], configured: &[CameraConfig]) -> String {
    if devices.is_empty() {
        return "No camera or doorbell found on this Arlo account.\n".to_string();
    }
    let mut out = format!(
        "Streamable devices on the Arlo account ({}):\n\n",
        devices.len()
    );
    let _ = writeln!(
        out,
        "  {:<16} {:<24} {:<10} {:<12} CONFIGURED AS",
        "DEVICE ID", "NAME", "KIND", "MODEL"
    );
    for d in devices {
        let as_stream = configured_stream(d, configured).unwrap_or("-");
        let _ = writeln!(
            out,
            "  {:<16} {:<24} {:<10} {:<12} {as_stream}",
            d.id.as_str(),
            d.name,
            d.kind,
            d.model.as_deref().unwrap_or("-")
        );
    }
    out
}

fn configured_stream<'a>(
    device: &DiscoveredDevice,
    configured: &'a [CameraConfig],
) -> Option<&'a str> {
    configured
        .iter()
        .find(|c| c.arlo_device_id == device.id)
        .map(|c| c.stream_name.as_str())
}

/// Configured ids the account does not have: a typo or a removed camera.
fn render_unknown(devices: &[DiscoveredDevice], configured: &[CameraConfig]) -> String {
    let mut out = String::new();
    for cam in configured {
        if !devices.iter().any(|d| d.id == cam.arlo_device_id) {
            let _ = writeln!(
                out,
                "\nWarning: [[cameras]] `{}` (stream `{}`) is not a streamable device on this account.",
                cam.arlo_device_id, cam.stream_name
            );
        }
    }
    out
}

fn render_snippet(devices: &[DiscoveredDevice], configured: &[CameraConfig]) -> String {
    let missing: Vec<&DiscoveredDevice> = devices
        .iter()
        .filter(|d| configured_stream(d, configured).is_none())
        .collect();
    if missing.is_empty() {
        return if devices.is_empty() {
            String::new()
        } else {
            "\nEvery streamable device is already configured.\n".to_string()
        };
    }
    let mut taken: HashSet<String> = configured
        .iter()
        .map(|c| c.stream_name.as_str().to_string())
        .collect();
    let mut out = "\nAdd the ones you want to expose to your configuration:\n".to_string();
    for d in missing {
        let name = unique_stream_name(d, &mut taken);
        let _ = write!(
            out,
            "\n# {} ({})\n[[cameras]]\narlo_device_id = \"{}\"\nstream_name    = \"{name}\"   # rtsp://<host>:8554/{name}\n",
            d.name, d.kind, d.id
        );
    }
    out
}

/// A suggested stream name not already used by the configuration or by
/// an earlier suggestion (`-2`, `-3`, … on collision).
fn unique_stream_name(device: &DiscoveredDevice, taken: &mut HashSet<String>) -> String {
    let base = StreamName::suggest(&device.name, &device.id)
        .as_str()
        .to_string();
    let mut candidate = base.clone();
    let mut n = 2;
    while taken.contains(&candidate) {
        candidate = format!("{base}-{n}");
        n += 1;
    }
    taken.insert(candidate.clone());
    candidate
}

#[cfg(test)]
mod tests {
    use super::*;
    use streamer_domain::camera::CameraId;
    use streamer_domain::config::CooldownConfig;

    fn device(id: &str, name: &str, kind: &str) -> DiscoveredDevice {
        DiscoveredDevice {
            id: CameraId::new(id),
            name: name.to_string(),
            kind: kind.to_string(),
            model: Some("VMC4041P".to_string()),
        }
    }

    fn camera(id: &str, stream: &str) -> CameraConfig {
        CameraConfig {
            arlo_device_id: CameraId::new(id),
            stream_name: StreamName::parse(stream).expect("valid"),
            codec_hint: None,
            cooldown: CooldownConfig::default(),
        }
    }

    #[test]
    fn render_lists_devices_and_marks_the_configured_one() {
        let devices = [
            device("CAM1", "Living Room", "camera"),
            device("DB1", "Door", "doorbell"),
        ];
        let out = render(&devices, &[camera("CAM1", "living")]);
        let cam_line = out.lines().find(|l| l.contains("CAM1")).expect("row");
        assert!(cam_line.trim_end().ends_with("living"));
        let door_line = out
            .lines()
            .find(|l| l.contains("DB1") && !l.contains('='))
            .expect("row");
        assert!(door_line.trim_end().ends_with('-'));
    }

    #[test]
    fn render_suggests_only_unconfigured_devices() {
        let devices = [
            device("CAM1", "Living Room", "camera"),
            device("DB1", "Front Door", "doorbell"),
        ];
        let out = render(&devices, &[camera("CAM1", "living")]);
        assert!(out.contains("arlo_device_id = \"DB1\""));
        assert!(out.contains("stream_name    = \"front-door\""));
        assert!(!out.contains("arlo_device_id = \"CAM1\""));
    }

    #[test]
    fn render_avoids_stream_name_collisions() {
        let devices = [
            device("A", "Garden", "camera"),
            device("B", "garden", "camera"),
        ];
        let out = render(&devices, &[camera("X", "garden")]);
        assert!(out.contains("stream_name    = \"garden-2\""));
        assert!(out.contains("stream_name    = \"garden-3\""));
    }

    #[test]
    fn render_warns_about_configured_ids_missing_from_the_account() {
        let out = render(
            &[device("CAM1", "Attic", "camera")],
            &[camera("TYPO", "attic")],
        );
        assert!(out.contains("Warning: [[cameras]] `TYPO`"));
    }

    #[test]
    fn render_says_when_everything_is_configured() {
        let out = render(
            &[device("CAM1", "Attic", "camera")],
            &[camera("CAM1", "attic")],
        );
        assert!(out.contains("Every streamable device is already configured."));
        assert!(!out.contains("[[cameras]]\n"));
    }

    #[test]
    fn render_of_an_empty_account() {
        let out = render(&[], &[]);
        assert_eq!(out, "No camera or doorbell found on this Arlo account.\n");
    }
}
