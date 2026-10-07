//! Latest snapshot URL per camera, as announced on the event bus.
//!
//! Arlo publishes `cameras/<id>` with a `presignedLastImageUrl` property
//! right after it takes a snapshot (on motion, or on request). The event
//! adapter records the URL here; the thumbnail adapter reads it back, so
//! refreshing the idle still needs no device-list round-trip and shows
//! the newest image. The URL is a presigned S3 credential: it lives only
//! in memory, is never logged, and expires, hence the freshness window.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use reqwest::Url;

use streamer_domain::camera::CameraId;

/// How long a recorded URL is trusted. Presigned URLs expire; past this
/// the thumbnail adapter falls back to the device list.
pub const SNAPSHOT_URL_MAX_AGE: Duration = Duration::from_secs(600);
/// Cameras remembered at once. The bus may announce any device of the
/// account; past this the oldest entry makes room.
const MAX_ENTRIES: usize = 64;

/// Name suffixes that only resolve inside a private network.
const LOCAL_SUFFIXES: &[&str] = &[
    "localhost",
    "local",
    "localdomain",
    "internal",
    "lan",
    "home",
    "home.arpa",
    "corp",
    "intranet",
];

/// Whether `url` may be fetched as a snapshot: absolute `https`, no
/// userinfo, and a host that is a public-looking DNS name — not an IP
/// literal, not a single label, not a private-network suffix. The bytes
/// come from the cloud (event bus or device list), and the daemon runs
/// inside the user's LAN: a URL naming `https://192.168.1.1/` made it
/// fetch from the router. Names that resolve to a private address are
/// refused by the client's resolver ([`crate::thumbnails::http_client`]).
/// The rule is re-applied on every redirect hop.
#[must_use]
pub fn is_snapshot_url(url: &str) -> bool {
    let Ok(u) = Url::parse(url) else {
        return false;
    };
    if u.scheme() != "https" || !u.username().is_empty() || u.password().is_some() {
        return false;
    }
    u.host_str().is_some_and(is_public_name)
}

fn is_public_name(host: &str) -> bool {
    if host.trim_matches(['[', ']']).parse::<IpAddr>().is_ok() {
        return false;
    }
    let name = host.trim_end_matches('.').to_ascii_lowercase();
    name.contains('.')
        && !LOCAL_SUFFIXES
            .iter()
            .any(|s| name == *s || name.ends_with(&format!(".{s}")))
}

/// Per-camera latest snapshot URL. Shared by the event and thumbnail
/// adapters behind an `Arc`. Keyed by validated [`CameraId`]s, expired
/// entries pruned on every record and the size capped, so a bus
/// announcing many devices cannot grow it for good.
#[derive(Default)]
pub struct SnapshotUrlCache {
    entries: Mutex<HashMap<CameraId, (String, Instant)>>,
}

impl SnapshotUrlCache {
    /// Record `url` for `camera` as of `now`. Only URLs passing
    /// [`is_snapshot_url`] are accepted; anything else is ignored and
    /// `false` returned.
    pub fn record(&self, camera: &CameraId, url: &str, now: Instant) -> bool {
        if !is_snapshot_url(url) {
            return false;
        }
        let mut entries = self.lock();
        entries.retain(|_, (_, at)| now.saturating_duration_since(*at) < SNAPSHOT_URL_MAX_AGE);
        if entries.len() >= MAX_ENTRIES
            && !entries.contains_key(camera)
            && let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, (_, at))| *at)
                .map(|(id, _)| id.clone())
        {
            entries.remove(&oldest);
        }
        entries.insert(camera.clone(), (url.to_string(), now));
        true
    }

    /// The URL recorded for `device_id` if it is younger than
    /// [`SNAPSHOT_URL_MAX_AGE`] at `now`.
    #[must_use]
    pub fn fresh(&self, camera: &CameraId, now: Instant) -> Option<String> {
        self.lock()
            .get(camera)
            .filter(|(_, at)| now.saturating_duration_since(*at) < SNAPSHOT_URL_MAX_AGE)
            .map(|(url, _)| url.clone())
    }

    /// Forget the URL for `camera` (it failed to fetch).
    pub fn forget(&self, camera: &CameraId) {
        self.lock().remove(camera);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<CameraId, (String, Instant)>> {
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

    fn cam(id: &str) -> CameraId {
        CameraId::new(id)
    }

    #[test]
    fn is_snapshot_url_accepts_only_absolute_https_urls() {
        assert!(is_snapshot_url(URL));
        assert!(!is_snapshot_url("http://arlos3.example/last.jpg"));
        assert!(!is_snapshot_url("https://"));
        // The WHATWG parser folds extra slashes: this one *has* a host,
        // a name that will simply not resolve.
        assert!(is_snapshot_url("https:///last.jpg"));
        assert!(!is_snapshot_url("file:///etc/passwd"));
        assert!(!is_snapshot_url("/relative/last.jpg"));
        assert!(!is_snapshot_url(""));
    }

    /// The daemon runs in the user's LAN: a cloud-supplied URL must not
    /// make it fetch from the router or another local service.
    #[test]
    fn is_snapshot_url_refuses_ip_literals_local_names_and_userinfo() {
        for url in [
            "https://192.168.1.1/last.jpg",
            "https://10.0.0.5:8443/x.jpg",
            "https://[::1]/x.jpg",
            "https://[fd00::1]/x.jpg",
            "https://2130706433/x.jpg",
            "https://localhost/x.jpg",
            "https://router/x.jpg",
            "https://nas.local/x.jpg",
            "https://nas.home.arpa/x.jpg",
            "https://printer.lan./x.jpg",
            "https://user:pw@arlos3-prod-z1.s3.amazonaws.com/x.jpg",
        ] {
            assert!(!is_snapshot_url(url), "{url} must be refused");
        }
    }

    #[test]
    fn record_then_fresh_returns_the_url() {
        let cache = SnapshotUrlCache::default();
        let t0 = Instant::now();
        assert!(cache.record(&cam("CAM"), URL, t0));
        assert_eq!(
            cache
                .fresh(&cam("CAM"), t0 + Duration::from_secs(1))
                .as_deref(),
            Some(URL)
        );
        assert_eq!(cache.fresh(&cam("OTHER"), t0), None);
    }

    #[test]
    fn fresh_expires_after_the_max_age() {
        let cache = SnapshotUrlCache::default();
        let t0 = Instant::now();
        cache.record(&cam("CAM"), URL, t0);
        assert_eq!(cache.fresh(&cam("CAM"), t0 + SNAPSHOT_URL_MAX_AGE), None);
    }

    #[test]
    fn a_newer_record_replaces_the_older_one() {
        let cache = SnapshotUrlCache::default();
        let t0 = Instant::now();
        cache.record(&cam("CAM"), URL, t0);
        cache.record(
            &cam("CAM"),
            "https://example.com/new.jpg",
            t0 + Duration::from_secs(5),
        );
        assert_eq!(
            cache
                .fresh(&cam("CAM"), t0 + Duration::from_secs(6))
                .as_deref(),
            Some("https://example.com/new.jpg")
        );
    }

    #[test]
    fn record_rejects_non_https_and_garbage() {
        let cache = SnapshotUrlCache::default();
        let t0 = Instant::now();
        assert!(!cache.record(&cam("CAM"), "http://example.com/a.jpg", t0));
        assert!(!cache.record(&cam("CAM"), "file:///etc/passwd", t0));
        assert!(!cache.record(&cam("CAM"), "not a url", t0));
        assert_eq!(cache.fresh(&cam("CAM"), t0), None);
    }

    #[test]
    fn forget_drops_the_url() {
        let cache = SnapshotUrlCache::default();
        let t0 = Instant::now();
        cache.record(&cam("CAM"), URL, t0);
        cache.forget(&cam("CAM"));
        assert_eq!(cache.fresh(&cam("CAM"), t0), None);
    }

    #[test]
    fn debug_never_prints_urls() {
        let cache = SnapshotUrlCache::default();
        cache.record(&cam("CAM"), URL, Instant::now());
        let dbg = format!("{cache:?}");
        assert!(!dbg.contains("amazonaws"));
        assert!(dbg.contains("cameras: 1"));
    }

    /// Entries were never pruned: every device the bus announced stayed.
    #[test]
    fn record_prunes_expired_entries_and_caps_the_size() {
        let cache = SnapshotUrlCache::default();
        let t0 = Instant::now();
        cache.record(&cam("OLD"), URL, t0);
        let later = t0 + SNAPSHOT_URL_MAX_AGE;
        cache.record(&cam("NEW"), URL, later);
        assert_eq!(cache.lock().len(), 1, "the expired entry is gone");

        for n in 0..MAX_ENTRIES + 10 {
            cache.record(
                &cam(&format!("C{n}")),
                URL,
                later + Duration::from_millis(n as u64),
            );
        }
        assert_eq!(cache.lock().len(), MAX_ENTRIES);
        assert!(
            cache
                .fresh(&cam(&format!("C{}", MAX_ENTRIES + 9)), later)
                .is_some()
        );
    }
}
