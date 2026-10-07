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
use async_trait::async_trait;

use streamer_domain::camera::CameraId;
use streamer_domain::error::DomainError;

use crate::error::arlo_to_domain;

/// Thin seam over "list all Arlo devices", so the cache logic can be
/// unit-tested with a fake instead of a live `ArloClient`.
#[async_trait]
trait DeviceLister: Send + Sync {
    async fn list(&self) -> Result<Vec<Device>, DomainError>;
}

#[async_trait]
impl DeviceLister for ArloClient {
    async fn list(&self) -> Result<Vec<Device>, DomainError> {
        self.get_devices().await.map_err(arlo_to_domain)
    }
}

/// Shared cache mapping a [`CameraId`] to its full arlo-rs [`Device`].
pub struct DeviceRegistry {
    lister: Arc<dyn DeviceLister>,
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
        Self::with_lister(client)
    }

    fn with_lister(lister: Arc<dyn DeviceLister>) -> Self {
        Self {
            lister,
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
        let devices = self.lister.list().await?;
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn device(id: &str) -> Device {
        Device {
            device_id: id.to_string(),
            parent_id: id.to_string(),
            device_type: "camera".to_string(),
            device_name: format!("cam-{id}"),
            unique_id: format!("uniq-{id}"),
            state: "provisioned".to_string(),
            mac_address: None,
            firm_version: None,
            hw_version: None,
            model_id: None,
            presigned_last_image_url: None,
            x_cloud_id: Some(format!("xc-{id}")),
            automation_revision: None,
            allowed_mqtt_topics: vec![],
            connectivity: None,
        }
    }

    struct FakeLister {
        devices: Vec<&'static str>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl DeviceLister for FakeLister {
        async fn list(&self) -> Result<Vec<Device>, DomainError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.devices.iter().map(|id| device(id)).collect())
        }
    }

    struct FailingLister;

    #[async_trait]
    impl DeviceLister for FailingLister {
        async fn list(&self) -> Result<Vec<Device>, DomainError> {
            Err(DomainError::AdapterTransport("boom".to_string()))
        }
    }

    #[tokio::test]
    async fn resolve_unknown_camera_after_fetch_returns_unknown_camera() {
        let lister = Arc::new(FakeLister {
            devices: vec!["CAM1"],
            calls: AtomicUsize::new(0),
        });
        let registry = DeviceRegistry::with_lister(lister);

        let err = registry
            .resolve(&CameraId::new("NOPE"))
            .await
            .expect_err("missing camera must error");

        assert!(matches!(err, DomainError::UnknownCamera(id) if id == "NOPE"));
    }

    #[tokio::test]
    async fn resolve_known_camera_returns_matching_device() {
        let lister = Arc::new(FakeLister {
            devices: vec!["CAM1", "CAM2"],
            calls: AtomicUsize::new(0),
        });
        let registry = DeviceRegistry::with_lister(lister);

        let device = registry
            .resolve(&CameraId::new("CAM2"))
            .await
            .expect("known camera resolves");

        assert_eq!(device.device_id, "CAM2");
        assert_eq!(device.x_cloud_id.as_deref(), Some("xc-CAM2"));
    }

    #[tokio::test]
    async fn resolve_hit_does_not_refetch_after_first_population() {
        let lister = Arc::new(FakeLister {
            devices: vec!["CAM1"],
            calls: AtomicUsize::new(0),
        });
        let registry = DeviceRegistry::with_lister(lister.clone());

        registry
            .resolve(&CameraId::new("CAM1"))
            .await
            .expect("first");
        registry
            .resolve(&CameraId::new("CAM1"))
            .await
            .expect("second");

        assert_eq!(lister.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn resolve_miss_refetches_once() {
        let lister = Arc::new(FakeLister {
            devices: vec!["CAM1"],
            calls: AtomicUsize::new(0),
        });
        let registry = DeviceRegistry::with_lister(lister.clone());

        let _ = registry.resolve(&CameraId::new("GHOST")).await;

        assert_eq!(lister.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn resolve_propagates_fetch_failure() {
        let registry = DeviceRegistry::with_lister(Arc::new(FailingLister));

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
        let lister = Arc::new(FakeLister {
            devices: vec!["CAM1"],
            calls: AtomicUsize::new(0),
        });
        let registry = DeviceRegistry::with_lister(lister.clone());
        let cam = CameraId::new("CAM1");

        registry.resolve(&cam).await.expect("first");
        registry.invalidate(&cam);
        registry.resolve(&cam).await.expect("after invalidate");

        assert_eq!(lister.calls.load(Ordering::SeqCst), 2);
    }

    /// Takes a while to answer, so concurrent misses overlap.
    struct SlowLister {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl DeviceLister for SlowLister {
        async fn list(&self) -> Result<Vec<Device>, DomainError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(vec![device("CAM1"), device("CAM2")])
        }
    }

    #[tokio::test]
    async fn concurrent_misses_share_one_refetch() {
        let lister = Arc::new(SlowLister {
            calls: AtomicUsize::new(0),
        });
        let registry = DeviceRegistry::with_lister(lister.clone());
        let (a, b) = (CameraId::new("CAM1"), CameraId::new("CAM2"));

        let (ra, rb) = tokio::join!(registry.resolve(&a), registry.resolve(&b));

        assert!(ra.is_ok() && rb.is_ok());
        assert_eq!(lister.calls.load(Ordering::SeqCst), 1);
    }
}
