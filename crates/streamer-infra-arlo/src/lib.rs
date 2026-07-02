//! `rs-arlo` adapter — implements the Arlo driven ports.
//!
//! Composes a single shared `Arc<rs_arlo::client::ArloClient>` behind
//! three independent port impls, plus a [`boot()`] entry point that
//! handles auth, session-cache restore, and IMAP MFA cold-start.
//!
//! Module map:
//!
//! - [`mod@boot`] — top-level orchestration: build + authenticate
//! - [`device_registry`] — shared `CameraId → Device` cache
//! - [`event_mapper`] — pure `rs_arlo::ArloEvent` → domain `CameraEvent`
//! - [`events`] — [`ArloEventSource`](streamer_domain::port::ArloEventSource) impl
//! - [`stream_requester`] — [`WebrtcSignaler`](streamer_domain::port::WebrtcSignaler) impl
//! - [`thumbnails`] — [`ArloThumbnailSource`](streamer_domain::port::ArloThumbnailSource) impl
//! - [`error`] — `ArloError` → `DomainError` translation

#![forbid(unsafe_code)]

pub mod boot;
pub mod device_registry;
pub mod error;
pub mod event_mapper;
pub mod events;
pub mod stream_requester;
pub mod thumbnails;

pub use boot::boot;
pub use device_registry::DeviceRegistry;
pub use events::ArloEventSourceAdapter;
pub use stream_requester::ArloWebrtcSignalerAdapter;
pub use thumbnails::ArloThumbnailSourceAdapter;
