//! What the adapter has published, kept in the state directory: the
//! layout (base topic, discovery prefix) and the cameras. A camera
//! dropped from the configuration (or from the account), or everything
//! published under a layout no longer configured, is then cleared from
//! the broker on the next connect, instead of staying in Home Assistant
//! with its last state frozen.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use streamer_domain::camera::CameraId;
use tracing::warn;

use super::messages::Layout;

/// The record's file name, in the state directory.
pub const RECORD_FILE: &str = "mqtt-published.json";

/// What a record holds once read: the layout it was published under
/// (`None` for a record from before the layout was kept: then the
/// current one) and the cameras.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Recorded {
    pub layout: Option<Layout>,
    pub cameras: BTreeSet<CameraId>,
}

/// The file's format; a bare list is the release candidates' record.
#[derive(Deserialize)]
#[serde(untagged)]
enum OnDisk {
    Current {
        layout: Layout,
        cameras: Vec<String>,
    },
    Legacy(Vec<String>),
}

#[derive(Serialize)]
struct OnDiskRef<'a> {
    layout: &'a Layout,
    cameras: Vec<&'a str>,
}

/// The published record of one state directory.
#[derive(Debug, Clone)]
pub(crate) struct PublishedRecord {
    path: PathBuf,
}

impl PublishedRecord {
    pub(crate) fn in_dir(dir: &Path) -> Self {
        Self {
            path: dir.join(RECORD_FILE),
        }
    }

    /// What was last recorded; nothing when the file is missing. An
    /// unreadable or malformed record is reported and read as nothing
    /// (the ghosts it would have cleared stay until removed by hand).
    pub(crate) async fn load(&self) -> Recorded {
        let bytes = match tokio::fs::read(&self.path).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Recorded::default(),
            Err(e) => {
                warn!(path = %self.path.display(), error = %e, "MQTT published record unreadable");
                return Recorded::default();
            }
        };
        let (layout, ids) = match serde_json::from_slice::<OnDisk>(&bytes) {
            Ok(OnDisk::Current { layout, cameras }) => (Some(layout), cameras),
            Ok(OnDisk::Legacy(cameras)) => (None, cameras),
            Err(e) => {
                warn!(path = %self.path.display(), error = %e, "MQTT published record malformed");
                return Recorded::default();
            }
        };
        // Both become topic levels: only valid ones are kept.
        let layout = layout.filter(|layout| {
            let valid = layout.is_valid();
            if !valid {
                warn!(path = %self.path.display(), "MQTT published record names an invalid layout; ignored");
            }
            valid
        });
        Recorded {
            layout,
            cameras: ids
                .into_iter()
                .filter_map(|id| CameraId::parse(id).ok())
                .collect(),
        }
    }

    /// Replace the record with `layout` and `ids` (written to a temporary
    /// file, then renamed, so a crash never leaves half a record).
    ///
    /// # Errors
    ///
    /// The I/O error when the file cannot be written.
    pub(crate) async fn save(
        &self,
        layout: &Layout,
        ids: &BTreeSet<CameraId>,
    ) -> std::io::Result<()> {
        let record = OnDiskRef {
            layout,
            cameras: ids.iter().map(CameraId::as_str).collect(),
        };
        let json = serde_json::to_vec(&record).map_err(std::io::Error::other)?;
        let tmp = self.path.with_extension("json.tmp");
        tokio::fs::write(&tmp, json).await?;
        tokio::fs::rename(&tmp, &self.path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use streamer_domain::config::MqttConfig;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("mqtt-record-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn ids(list: &[&str]) -> BTreeSet<CameraId> {
        list.iter().map(|id| CameraId::new(*id)).collect()
    }

    fn layout(base: &str) -> Layout {
        Layout::new(&MqttConfig {
            url: "mqtt://broker".into(),
            username: None,
            password_env: None,
            ca_file: None,
            base_topic: base.into(),
            discovery_prefix: "homeassistant".into(),
            thumbnails: false,
        })
    }

    #[tokio::test]
    async fn a_saved_record_loads_back_with_its_layout_and_a_missing_one_is_empty() {
        let dir = scratch("roundtrip");
        let record = PublishedRecord::in_dir(&dir);
        assert_eq!(record.load().await, Recorded::default());

        record
            .save(&layout("arlo"), &ids(&["CAM1", "CAM2"]))
            .await
            .unwrap();

        let loaded = record.load().await;
        assert_eq!(loaded.cameras, ids(&["CAM1", "CAM2"]));
        assert_eq!(loaded.layout, Some(layout("arlo")));
        assert!(!dir.join("mqtt-published.json.tmp").exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 0.3.0 release candidates wrote a bare list: its cameras were
    /// published under the current layout.
    #[tokio::test]
    async fn a_bare_list_from_an_earlier_record_loads_without_a_layout() {
        let dir = scratch("legacy");
        let record = PublishedRecord::in_dir(&dir);
        std::fs::write(dir.join(RECORD_FILE), br#"["CAM1"]"#).unwrap();
        let loaded = record.load().await;
        assert_eq!(loaded.cameras, ids(&["CAM1"]));
        assert_eq!(loaded.layout, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Ids and the layout become topic levels: forged ones are dropped.
    #[tokio::test]
    async fn only_valid_ids_and_layouts_are_loaded_and_garbage_reads_as_none() {
        let dir = scratch("invalid");
        let record = PublishedRecord::in_dir(&dir);
        std::fs::write(dir.join(RECORD_FILE), br#"["CAM1", "a/#", "b+c"]"#).unwrap();
        assert_eq!(record.load().await.cameras, ids(&["CAM1"]));

        std::fs::write(
            dir.join(RECORD_FILE),
            br#"{"layout": {"base_topic": "a/#", "discovery_prefix": "homeassistant"}, "cameras": ["CAM1"]}"#,
        )
        .unwrap();
        let forged = record.load().await;
        assert_eq!(forged.layout, None, "a forged layout is not trusted");
        assert_eq!(forged.cameras, ids(&["CAM1"]));

        std::fs::write(dir.join(RECORD_FILE), b"not json").unwrap();
        assert_eq!(record.load().await, Recorded::default());
        std::fs::remove_dir_all(&dir).ok();
    }
}
