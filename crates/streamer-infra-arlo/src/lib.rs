//! `arlo-rs` adapter — implements the Arlo driven ports.
//!
//! Composes a single shared `Arc<arlo_rs::client::ArloClient>` behind
//! three independent port impls, plus a [`boot()`] entry point that
//! handles auth, session-cache restore, and IMAP MFA cold-start.
//!
//! Module map:
//!
//! - [`mod@boot`] — top-level orchestration: build + authenticate
//! - [`device_registry`] — shared `CameraId → Device` cache
//! - [`discovery`] — streamable devices on the account (`list-devices` CLI)
//! - [`event_mapper`] — pure `arlo_rs::ArloEvent` → domain `CameraEvent`
//! - [`events`] — [`ArloEventSource`](streamer_domain::port::ArloEventSource) impl
//! - [`snapshot_cache`] — latest snapshot URL per camera, filled from the bus
//! - [`stream_requester`] — [`WebrtcSignaler`](streamer_domain::port::WebrtcSignaler) impl
//! - [`thumbnails`] — [`ArloThumbnailSource`](streamer_domain::port::ArloThumbnailSource) impl
//! - [`error`] — `ArloError` → `DomainError` translation

#![forbid(unsafe_code)]

pub mod boot;
pub mod device_registry;
pub mod discovery;
pub mod error;
pub mod event_mapper;
pub mod events;
pub mod ice;
pub mod login_backoff;
pub mod snapshot_cache;
pub mod stream_requester;
pub mod thumbnails;
pub mod user_view;

/// The authenticated client `boot()` returns, for the composition root.
pub use arlo_rs::client::ArloClient;
pub use boot::boot;
pub use device_registry::DeviceRegistry;
pub use discovery::discover_devices;
pub use events::ArloEventSourceAdapter;
pub use login_backoff::LoginBackoff;
pub use snapshot_cache::SnapshotUrlCache;
pub use stream_requester::ArloWebrtcSignalerAdapter;
pub use thumbnails::ArloThumbnailSourceAdapter;
pub use user_view::ArloUserViewSourceAdapter;
