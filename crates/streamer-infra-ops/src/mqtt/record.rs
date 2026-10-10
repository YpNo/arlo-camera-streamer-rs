//! The cameras the adapter has published, kept in the state directory: a
//! camera dropped from the configuration (or from the account) is then
//! cleared from the broker on the next connect, instead of staying in
//! Home Assistant with its last state frozen.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use streamer_domain::camera::CameraId;
use tracing::warn;

/// The record's file name, in the state directory.
pub const RECORD_FILE: &str = "mqtt-published.json";

/// The published-cameras record of one state directory.
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

    /// The device ids last recorded; none when the file is missing. An
    /// unreadable or malformed record is reported and read as none (the
    /// ghosts it would have cleared stay until removed by hand).
    pub(crate) async fn load(&self) -> BTreeSet<CameraId> {
        let bytes = match tokio::fs::read(&self.path).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return BTreeSet::new(),
            Err(e) => {
                warn!(path = %self.path.display(), error = %e, "MQTT published record unreadable");
                return BTreeSet::new();
            }
        };
        match serde_json::from_slice::<Vec<String>>(&bytes) {
            // Only ids that are valid camera ids: they become topic levels.
            Ok(ids) => ids
                .into_iter()
                .filter_map(|id| CameraId::parse(id).ok())
                .collect(),
            Err(e) => {
                warn!(path = %self.path.display(), error = %e, "MQTT published record malformed");
                BTreeSet::new()
            }
        }
    }

    /// Replace the record with `ids` (written to a temporary file, then
    /// renamed, so a crash never leaves half a record).
    ///
    /// # Errors
    ///
    /// The I/O error when the file cannot be written.
    pub(crate) async fn save(&self, ids: &BTreeSet<CameraId>) -> std::io::Result<()> {
        let list: Vec<&str> = ids.iter().map(CameraId::as_str).collect();
        let json = serde_json::to_vec(&list).map_err(std::io::Error::other)?;
        let tmp = self.path.with_extension("json.tmp");
        tokio::fs::write(&tmp, json).await?;
        tokio::fs::rename(&tmp, &self.path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("mqtt-record-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn ids(list: &[&str]) -> BTreeSet<CameraId> {
        list.iter().map(|id| CameraId::new(*id)).collect()
    }

    #[tokio::test]
    async fn a_saved_record_loads_back_and_a_missing_one_is_empty() {
        let dir = scratch("roundtrip");
        let record = PublishedRecord::in_dir(&dir);
        assert!(record.load().await.is_empty());

        record.save(&ids(&["CAM1", "CAM2"])).await.unwrap();

        assert_eq!(record.load().await, ids(&["CAM1", "CAM2"]));
        assert!(!dir.join("mqtt-published.json.tmp").exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Ids become topic levels: a forged one (`#`, `/`) is dropped.
    #[tokio::test]
    async fn only_valid_camera_ids_are_loaded_and_garbage_reads_as_none() {
        let dir = scratch("invalid");
        let record = PublishedRecord::in_dir(&dir);
        std::fs::write(dir.join(RECORD_FILE), br#"["CAM1", "a/#", "b+c"]"#).unwrap();
        assert_eq!(record.load().await, ids(&["CAM1"]));

        std::fs::write(dir.join(RECORD_FILE), b"not json").unwrap();
        assert!(record.load().await.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }
}
