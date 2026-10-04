//! Latest snapshot URL per camera, as announced on the event bus.
//!
//! Arlo publishes `cameras/<id>` with a `presignedLastImageUrl` property
//! right after it takes a snapshot (on motion, or on request). The event
//! adapter records the URL here; the thumbnail adapter reads it back, so
//! refreshing the idle still needs no device-list round-trip and shows
//! the newest image. The URL is a presigned S3 credential: it lives only
//! in memory, is never logged, and expires, hence the freshness window.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use reqwest::Url;

/// How long a recorded URL is trusted. Presigned URLs expire; past this
/// the thumbnail adapter falls back to the device list.
pub const SNAPSHOT_URL_MAX_AGE: Duration = Duration::from_secs(600);

/// Whether `url` is an absolute `https` URL with a host: the only shape
/// a presigned snapshot URL is allowed to have, whichever way it reached
/// us (event bus or device list). Anything else is refused before an
/// HTTP client ever sees it.
#[must_use]
pub fn is_https_with_host(url: &str) -> bool {
    Url::parse(url).is_ok_and(|u| u.scheme() == "https" && u.host_str().is_some())
}

/// Per-camera latest snapshot URL. Shared by the event and thumbnail
/// adapters behind an `Arc`.
#[derive(Default)]
pub struct SnapshotUrlCache {
    entries: Mutex<HashMap<String, (String, Instant)>>,
}

impl SnapshotUrlCache {
    /// Record `url` for `device_id` as of `now`. Only `https` URLs with a
    /// host are accepted; anything else is ignored and `false` returned.
    pub fn record(&self, device_id: &str, url: &str, now: Instant) -> bool {
        let valid = is_https_with_host(url);
        if valid {
            self.lock()
                .insert(device_id.to_string(), (url.to_string(), now));
        }
        valid
    }

    /// The URL recorded for `device_id` if it is younger than
    /// [`SNAPSHOT_URL_MAX_AGE`] at `now`.
    #[must_use]
    pub fn fresh(&self, device_id: &str, now: Instant) -> Option<String> {
        self.lock()
            .get(device_id)
            .filter(|(_, at)| now.saturating_duration_since(*at) < SNAPSHOT_URL_MAX_AGE)
            .map(|(url, _)| url.clone())
    }

    /// Forget the URL for `device_id` (it failed to fetch).
    pub fn forget(&self, device_id: &str) {
        self.lock().remove(device_id);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, (String, Instant)>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Never prints the URLs (presigned credentials).
impl std::fmt::Debug for SnapshotUrlCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotUrlCache")
            .field("cameras", &self.lock().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "https://arlos3-prod-z1.s3.amazonaws.com/x/last.jpg?X-Amz-Signature=s";

    #[test]
    fn is_https_with_host_accepts_only_absolute_https_urls() {
        assert!(is_https_with_host(URL));
        assert!(!is_https_with_host("http://arlos3.example/last.jpg"));
        assert!(!is_https_with_host("https://"));
        // The WHATWG parser folds extra slashes: this one *has* a host.
        assert!(is_https_with_host("https:///last.jpg"));
        assert!(!is_https_with_host("file:///etc/passwd"));
        assert!(!is_https_with_host("/relative/last.jpg"));
        assert!(!is_https_with_host(""));
    }

    #[test]
    fn record_then_fresh_returns_the_url() {
        let cache = SnapshotUrlCache::default();
        let t0 = Instant::now();
        assert!(cache.record("CAM", URL, t0));
        assert_eq!(
            cache.fresh("CAM", t0 + Duration::from_secs(1)).as_deref(),
            Some(URL)
        );
        assert_eq!(cache.fresh("OTHER", t0), None);
    }

    #[test]
    fn fresh_expires_after_the_max_age() {
        let cache = SnapshotUrlCache::default();
        let t0 = Instant::now();
        cache.record("CAM", URL, t0);
        assert_eq!(cache.fresh("CAM", t0 + SNAPSHOT_URL_MAX_AGE), None);
    }

    #[test]
    fn a_newer_record_replaces_the_older_one() {
        let cache = SnapshotUrlCache::default();
        let t0 = Instant::now();
        cache.record("CAM", URL, t0);
        cache.record(
            "CAM",
            "https://example.com/new.jpg",
            t0 + Duration::from_secs(5),
        );
        assert_eq!(
            cache.fresh("CAM", t0 + Duration::from_secs(6)).as_deref(),
            Some("https://example.com/new.jpg")
        );
    }

    #[test]
    fn record_rejects_non_https_and_garbage() {
        let cache = SnapshotUrlCache::default();
        let t0 = Instant::now();
        assert!(!cache.record("CAM", "http://example.com/a.jpg", t0));
        assert!(!cache.record("CAM", "file:///etc/passwd", t0));
        assert!(!cache.record("CAM", "not a url", t0));
        assert_eq!(cache.fresh("CAM", t0), None);
    }

    #[test]
    fn forget_drops_the_url() {
        let cache = SnapshotUrlCache::default();
        let t0 = Instant::now();
        cache.record("CAM", URL, t0);
        cache.forget("CAM");
        assert_eq!(cache.fresh("CAM", t0), None);
    }

    #[test]
    fn debug_never_prints_urls() {
        let cache = SnapshotUrlCache::default();
        cache.record("CAM", URL, Instant::now());
        let dbg = format!("{cache:?}");
        assert!(!dbg.contains("amazonaws"));
        assert!(dbg.contains("cameras: 1"));
    }
}
