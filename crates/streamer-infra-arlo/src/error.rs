//! Translation layer between `arlo_rs::error::ArloError` and the
//! domain-level [`DomainError`].
//!
//! Keeps infrastructure error variants out of the domain crate while
//! preserving enough context for diagnostics. Specific variants
//! (`DeviceNotFound`) map to dedicated domain variants where the
//! application layer can act on them; the rest fall through to
//! [`DomainError::AdapterTransport`].

use arlo_rs::error::ArloError;
use streamer_domain::error::DomainError;

/// Arlo error code returned by `sipInfo` while the mobile app views the
/// camera over RTSP: "RTSP Streaming in progress, SIP Streaming is not
/// allowed, try after some time" (captured live 2026-09-27).
const ARLO_STREAM_BUSY: u32 = 14001;

/// Map an [`ArloError`] into a [`DomainError`].
#[must_use]
pub fn arlo_to_domain(err: ArloError) -> DomainError {
    if is_stream_busy(&err) {
        return DomainError::CameraBusy(err.to_string());
    }
    match err {
        ArloError::DeviceNotFound(d) => DomainError::UnknownCamera(d),
        other => DomainError::AdapterTransport(other.to_string()),
    }
}

/// arlo-rs keeps Arlo's code structured from 0.2.1 on; 0.2.0 only
/// carries it in the message's body excerpt, so both forms are checked.
fn is_stream_busy(err: &ArloError) -> bool {
    match err {
        ArloError::ApiError {
            error: Some(code), ..
        } => *code == ARLO_STREAM_BUSY,
        ArloError::ApiError { message, .. } => message.contains(r#""error":"14001""#),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_busy_code_maps_to_camera_busy() {
        let structured = ArloError::ApiError {
            code: 500,
            error: Some(ARLO_STREAM_BUSY),
            message: "RTSP Streaming in progress".to_string(),
        };
        assert!(matches!(
            arlo_to_domain(structured),
            DomainError::CameraBusy(_)
        ));
    }

    #[test]
    fn stream_busy_in_legacy_message_maps_to_camera_busy() {
        // arlo-rs 0.2.0 shape, captured live.
        let legacy = ArloError::ApiError {
            code: 500,
            error: None,
            message: r#"Envelope reports success=false; body: {"data":{"error":"14001","message":"RTSP Streaming in progress"},"success":false}"#.to_string(),
        };
        assert!(matches!(arlo_to_domain(legacy), DomainError::CameraBusy(_)));
    }

    #[test]
    fn other_api_errors_stay_adapter_transport() {
        let other = ArloError::ApiError {
            code: 500,
            error: Some(2059),
            message: "Base station is not responding.".to_string(),
        };
        assert!(matches!(
            arlo_to_domain(other),
            DomainError::AdapterTransport(_)
        ));
    }

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
