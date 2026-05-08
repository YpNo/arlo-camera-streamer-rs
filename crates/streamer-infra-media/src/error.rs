//! Adapter-internal error type and mapping to [`DomainError`].
//!
//! GStreamer-specific errors (e.g. `gstreamer::glib::Error`,
//! `gstreamer::StateChangeError`) are stringified at the call site
//! before being lifted into [`MediaError`]. This keeps the domain layer
//! free of any GStreamer dependency: the only thing that crosses the
//! crate boundary is a `DomainError`.

use streamer_domain::error::DomainError;
use thiserror::Error;

/// Errors raised by the media multiplexer adapter.
#[derive(Debug, Error)]
pub enum MediaError {
    /// Operation referenced a camera id that was never `register`-ed.
    #[error("camera not registered: {0}")]
    UnknownCamera(String),

    /// `register` was called twice for the same camera. Idempotency
    /// is the multiplexer's responsibility — the registry layer
    /// surfaces this so the multiplexer can swallow it.
    #[error("camera already registered: {0}")]
    AlreadyRegistered(String),

    /// Pipeline construction or state-change failure. Wraps the
    /// stringified GStreamer error.
    #[error("pipeline error: {0}")]
    Pipeline(String),

    /// `attach_live` did not observe an IDR within the configured
    /// timeout. The live source may be unreachable or emitting
    /// keyframes too rarely.
    #[error("splice timeout: no keyframe within {timeout_secs}s")]
    SpliceTimeout {
        /// Configured deadline that elapsed.
        timeout_secs: u64,
    },

    /// RTSP server (mount-point installation, port binding, etc.) failure.
    #[error("RTSP server error: {0}")]
    Rtsp(String),

    /// Thumbnail JPEG could not be decoded or pushed to the idle source.
    #[error("invalid thumbnail: {0}")]
    InvalidThumbnail(String),
}

impl From<MediaError> for DomainError {
    fn from(err: MediaError) -> Self {
        match err {
            MediaError::UnknownCamera(c) => Self::UnknownCamera(c),
            other => Self::AdapterTransport(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_camera_maps_to_domain_unknown_camera() {
        let err = MediaError::UnknownCamera("ABC".to_string());
        let domain: DomainError = err.into();
        assert!(matches!(domain, DomainError::UnknownCamera(c) if c == "ABC"));
    }

    #[test]
    fn already_registered_maps_to_adapter_transport() {
        let err = MediaError::AlreadyRegistered("ABC".to_string());
        let domain: DomainError = err.into();
        assert!(matches!(domain, DomainError::AdapterTransport(_)));
    }

    #[test]
    fn pipeline_error_maps_to_adapter_transport() {
        let err = MediaError::Pipeline("element creation failed".to_string());
        let domain: DomainError = err.into();
        match domain {
            DomainError::AdapterTransport(msg) => assert!(msg.contains("element creation")),
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn splice_timeout_includes_seconds_in_message() {
        let err = MediaError::SpliceTimeout { timeout_secs: 7 };
        assert!(err.to_string().contains("7s"));
    }

    #[test]
    fn rtsp_error_maps_to_adapter_transport() {
        let err = MediaError::Rtsp("bind 0.0.0.0:8554 failed".to_string());
        let domain: DomainError = err.into();
        assert!(matches!(domain, DomainError::AdapterTransport(_)));
    }

    #[test]
    fn invalid_thumbnail_maps_to_adapter_transport() {
        let err = MediaError::InvalidThumbnail("not a JPEG".to_string());
        let domain: DomainError = err.into();
        assert!(matches!(domain, DomainError::AdapterTransport(_)));
    }
}
