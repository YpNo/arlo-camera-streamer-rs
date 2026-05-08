//! Per-camera codec hint cache.
//!
//! On first attach of a live source, the GStreamer pipeline lets
//! `parsebin` auto-detect the elementary stream (H.264 vs H.265).
//! Subsequent attaches for the same camera can short-circuit that
//! detection by reading the cached hint, avoiding ~200–500 ms of
//! caps-renegotiation latency.
//!
//! Persistence to disk is **not** implemented in Phase 4. The cache is
//! seeded from `[[cameras]] codec_hint = "h265"` in TOML at boot and
//! then accumulates learnings during the process lifetime. A daemon
//! restart loses the in-flight learnings — acceptable, since they
//! re-establish on first activation. Disk persistence can be added in
//! Phase 6 when we add a state file.

use std::collections::HashMap;

use tokio::sync::RwLock;

use streamer_domain::camera::CameraId;
use streamer_domain::stream::Codec;

/// Thread-safe codec hint cache.
#[derive(Debug, Default)]
pub struct CodecCache {
    inner: RwLock<HashMap<CameraId, Codec>>,
}

impl CodecCache {
    /// Construct an empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a cache pre-loaded from the configured `codec_hint`s.
    #[must_use]
    pub fn with_initial(seed: HashMap<CameraId, Codec>) -> Self {
        Self {
            inner: RwLock::new(seed),
        }
    }

    /// Look up the cached codec for a camera. `None` means "no hint;
    /// let `parsebin` auto-detect on this attach".
    pub async fn get(&self, camera: &CameraId) -> Option<Codec> {
        self.inner.read().await.get(camera).copied()
    }

    /// Record a learning. Called by the GStreamer pipeline once
    /// `parsebin` has emitted its first parsed buffer with caps.
    pub async fn set(&self, camera: CameraId, codec: Codec) {
        self.inner.write().await.insert(camera, codec);
    }

    /// Snapshot the entire cache. Useful for ops endpoints.
    pub async fn snapshot(&self) -> HashMap<CameraId, Codec> {
        self.inner.read().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cam(id: &str) -> CameraId {
        CameraId::new(id)
    }

    #[tokio::test]
    async fn empty_cache_returns_none() {
        let cache = CodecCache::new();
        assert!(cache.get(&cam("X")).await.is_none());
    }

    #[tokio::test]
    async fn set_then_get_returns_codec() {
        let cache = CodecCache::new();
        cache.set(cam("X"), Codec::H265).await;
        assert_eq!(cache.get(&cam("X")).await, Some(Codec::H265));
    }

    #[tokio::test]
    async fn set_overwrites_previous_value() {
        let cache = CodecCache::new();
        cache.set(cam("X"), Codec::H264).await;
        cache.set(cam("X"), Codec::H265).await;
        assert_eq!(cache.get(&cam("X")).await, Some(Codec::H265));
    }

    #[tokio::test]
    async fn with_initial_seeds_cache() {
        let mut seed = HashMap::new();
        seed.insert(cam("A"), Codec::H264);
        seed.insert(cam("B"), Codec::H265);
        let cache = CodecCache::with_initial(seed);
        assert_eq!(cache.get(&cam("A")).await, Some(Codec::H264));
        assert_eq!(cache.get(&cam("B")).await, Some(Codec::H265));
    }

    #[tokio::test]
    async fn snapshot_returns_full_map() {
        let cache = CodecCache::new();
        cache.set(cam("A"), Codec::H264).await;
        cache.set(cam("B"), Codec::H265).await;
        let snap = cache.snapshot().await;
        assert_eq!(snap.len(), 2);
        assert_eq!(snap.get(&cam("A")), Some(&Codec::H264));
        assert_eq!(snap.get(&cam("B")), Some(&Codec::H265));
    }
}
