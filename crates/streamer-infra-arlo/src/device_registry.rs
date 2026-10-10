//! Shared, lazily-populated `CameraId → Device` cache.
//!
//! arlo-rs's v3 `start_stream` takes a full [`Device`] (it needs
//! `parent_id` for the `to:` field and `x_cloud_id` for the `xcloudId`
//! header), not a bare id string. Resolving a [`CameraId`] therefore
//! requires a `get_devices()` cloud round-trip. Those identity fields
//! (`device_id` / `parent_id` / `x_cloud_id`) are **stable** for the
//! life of a device, so we cache the device list once and only re-fetch
//! on a cache miss — a camera provisioned after boot is still picked up
//! on its first stream request. A camera re-paired to another base
//! station does change `parent_id`: the adapters [`invalidate`] its entry
//! when a call for it fails, so the next attempt refetches instead of
//! failing until a restart. Concurrent misses share one refetch.
//!
//! [`invalidate`]: DeviceRegistry::invalidate
//!
//! The cache stores `Arc<Device>` (arlo-rs's `Device` is not `Clone`):
//! [`DeviceRegistry::resolve`] clones the cheap `Arc`, releases the
//! lock, and only *then* lets the caller await `start_stream`. The
//! `std::sync::RwLock` is therefore never held across an `.await`.
//!
//! Note: the thumbnail adapter intentionally does **not** use this
//! cache — its `presigned_last_image_url` is a short-lived S3 URL that
//! must be re-fetched every call.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arlo_rs::client::ArloClient;
use arlo_rs::models::api::Device;

use streamer_domain::camera::CameraId;
use streamer_domain::error::DomainError;

use crate::arlo_api::ArloApi;
use crate::error::arlo_to_domain;

/// Shared cache mapping a [`CameraId`] to its full arlo-rs [`Device`].
pub struct DeviceRegistry {
    api: Arc<dyn ArloApi>,
    cache: RwLock<HashMap<String, Arc<Device>>>,
    /// Held across a refetch, so misses that arrive together wait for
    /// one device-list call instead of each making their own.
    refresh: tokio::sync::Mutex<()>,
}

impl DeviceRegistry {
    /// Build a registry backed by a shared, authenticated
    /// [`ArloClient`]. The cache starts empty and is populated on the
    /// first [`resolve`](Self::resolve).
    #[must_use]
    pub fn new(client: Arc<ArloClient>) -> Self {
        Self::with_api(client)
    }

    pub(crate) fn with_api(api: Arc<dyn ArloApi>) -> Self {
        Self {
            api,
            cache: RwLock::new(HashMap::new()),
            refresh: tokio::sync::Mutex::new(()),
        }
    }

    /// Resolve `camera` to its [`Device`], refreshing the cache once on
    /// a miss before giving up.
    ///
    /// # Errors
    ///
    /// - [`DomainError::AdapterTransport`] if the device-list fetch
    ///   fails.
    /// - [`DomainError::UnknownCamera`] if `camera` is absent even
    ///   after a fresh fetch.
    pub async fn resolve(&self, camera: &CameraId) -> Result<Arc<Device>, DomainError> {
        if let Some(device) = self.lookup(camera) {
            return Ok(device);
        }
        let _single_flight = self.refresh.lock().await;
        // Another miss may have refetched while this one waited.
        if let Some(device) = self.lookup(camera) {
            return Ok(device);
        }
        self.refresh().await?;
        self.lookup(camera)
            .ok_or_else(|| DomainError::UnknownCamera(camera.to_string()))
    }

    /// Forget `camera`'s cached device, so the next [`Self::resolve`]
    /// refetches the list. Called when a cloud call for the camera fails.
    pub fn invalidate(&self, camera: &CameraId) {
        self.cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(camera.as_str());
    }

    fn lookup(&self, camera: &CameraId) -> Option<Arc<Device>> {
        self.cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(camera.as_str())
            .cloned()
    }

    async fn refresh(&self) -> Result<(), DomainError> {
        let devices = self.api.devices().await.map_err(arlo_to_domain)?;
        let map: HashMap<String, Arc<Device>> = devices
            .into_iter()
            .map(|d| (d.device_id.clone(), Arc::new(d)))
            .collect();
        *self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = map;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arlo_api::fake::{FakeArlo, Outcome};
    use std::time::Duration;

    fn registry(fake: FakeArlo) -> (Arc<FakeArlo>, DeviceRegistry) {
        let fake = Arc::new(fake);
        (fake.clone(), DeviceRegistry::with_api(fake))
    }

    #[tokio::test]
    async fn resolve_unknown_camera_after_fetch_returns_unknown_camera() {
        let (_, registry) = registry(FakeArlo::with_devices(&["CAM1"]));

        let err = registry
            .resolve(&CameraId::new("NOPE"))
            .await
            .expect_err("missing camera must error");

        assert!(matches!(err, DomainError::UnknownCamera(id) if id == "NOPE"));
    }

    #[tokio::test]
    async fn resolve_known_camera_returns_matching_device() {
        let (_, registry) = registry(FakeArlo::with_devices(&["CAM1", "CAM2"]));

        let device = registry
            .resolve(&CameraId::new("CAM2"))
            .await
            .expect("known camera resolves");

        assert_eq!(device.device_id, "CAM2");
        assert_eq!(device.x_cloud_id.as_deref(), Some("xc-CAM2"));
    }

    #[tokio::test]
    async fn resolve_hit_does_not_refetch_after_first_population() {
        let (fake, registry) = registry(FakeArlo::with_devices(&["CAM1"]));

        registry
            .resolve(&CameraId::new("CAM1"))
            .await
            .expect("first");
        registry
            .resolve(&CameraId::new("CAM1"))
            .await
            .expect("second");

        assert_eq!(FakeArlo::calls(&fake.device_calls), 1);
    }

    #[tokio::test]
    async fn resolve_miss_refetches_once() {
        let (fake, registry) = registry(FakeArlo::with_devices(&["CAM1"]));

        let _ = registry.resolve(&CameraId::new("GHOST")).await;

        assert_eq!(FakeArlo::calls(&fake.device_calls), 1);
    }

    #[tokio::test]
    async fn resolve_propagates_fetch_failure() {
        let (_, registry) = registry(FakeArlo {
            devices_outcome: Outcome::Fails,
            ..FakeArlo::with_devices(&["CAM1"])
        });

        let err = registry
            .resolve(&CameraId::new("CAM1"))
            .await
            .expect_err("fetch failure propagates");

        assert!(matches!(err, DomainError::AdapterTransport(_)));
    }

    /// A camera re-paired to another base station kept failing on its
    /// stale `parent_id` until a restart.
    #[tokio::test]
    async fn invalidate_makes_the_next_resolve_refetch() {
        let (fake, registry) = registry(FakeArlo::with_devices(&["CAM1"]));
        let cam = CameraId::new("CAM1");

        registry.resolve(&cam).await.expect("first");
        registry.invalidate(&cam);
        registry.resolve(&cam).await.expect("after invalidate");

        assert_eq!(FakeArlo::calls(&fake.device_calls), 2);
    }

    #[tokio::test]
    async fn concurrent_misses_share_one_refetch() {
        let (fake, registry) = registry(FakeArlo {
            devices_delay: Duration::from_millis(50),
            ..FakeArlo::with_devices(&["CAM1", "CAM2"])
        });
        let (a, b) = (CameraId::new("CAM1"), CameraId::new("CAM2"));

        let (ra, rb) = tokio::join!(registry.resolve(&a), registry.resolve(&b));

        assert!(ra.is_ok() && rb.is_ok());
        assert_eq!(FakeArlo::calls(&fake.device_calls), 1);
    }
}
