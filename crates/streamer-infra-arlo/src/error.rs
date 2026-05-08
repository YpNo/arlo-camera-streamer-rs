//! Translation layer between `rs_arlo::error::ArloError` and the
//! domain-level [`DomainError`].
//!
//! Keeps infrastructure error variants out of the domain crate while
//! preserving enough context for diagnostics. Specific variants
//! (`DeviceNotFound`) map to dedicated domain variants where the
//! application layer can act on them; the rest fall through to
//! [`DomainError::AdapterTransport`].

use rs_arlo::error::ArloError;
use streamer_domain::error::DomainError;

/// Map an [`ArloError`] into a [`DomainError`].
#[must_use]
pub fn arlo_to_domain(err: ArloError) -> DomainError {
    match err {
        ArloError::DeviceNotFound(d) => DomainError::UnknownCamera(d),
        other => DomainError::AdapterTransport(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_not_found_maps_to_unknown_camera() {
        let err = ArloError::DeviceNotFound("CAM1".to_string());
        match arlo_to_domain(err) {
            DomainError::UnknownCamera(id) => assert_eq!(id, "CAM1"),
            other => panic!("expected UnknownCamera, got {other:?}"),
        }
    }

    #[test]
    fn auth_error_maps_to_adapter_transport() {
        let err = ArloError::AuthError("bad creds".to_string());
        assert!(matches!(
            arlo_to_domain(err),
            DomainError::AdapterTransport(_)
        ));
    }

    #[test]
    fn timeout_maps_to_adapter_transport() {
        let err = ArloError::Timeout("rtsp 30s".to_string());
        assert!(matches!(
            arlo_to_domain(err),
            DomainError::AdapterTransport(_)
        ));
    }

    #[test]
    fn adapter_transport_preserves_error_message() {
        let err = ArloError::ScraperError("captcha required".to_string());
        match arlo_to_domain(err) {
            DomainError::AdapterTransport(msg) => assert!(msg.contains("captcha")),
            other => panic!("expected AdapterTransport, got {other:?}"),
        }
    }
}
