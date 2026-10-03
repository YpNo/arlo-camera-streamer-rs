//! Domain-layer error hierarchy.
//!
//! Per project guidelines:
//! - Libraries (this crate) define typed errors with `thiserror`.
//! - Applications (`streamer-bin`) bubble these up via `anyhow::Result`
//!   with `with_context` annotations.

use thiserror::Error;

/// Errors raised by the domain layer or surfaced by infrastructure
/// adapters when fulfilling a port contract.
#[derive(Debug, Error)]
pub enum DomainError {
    /// A stream name failed validation (empty or contains non URL-safe chars).
    #[error("invalid stream name: '{0}'")]
    InvalidStreamName(String),

    /// A camera id from a config file, an HTTP path or the event bus
    /// broke the id rule; the payload names the rule, never the input.
    #[error("invalid camera id: {0}")]
    InvalidCameraId(String),

    /// Configuration could not be parsed or violates an invariant.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Operation referenced a camera id not declared in configuration.
    #[error("unknown camera id: {0}")]
    UnknownCamera(String),

    /// Adapter-level transport failure (connection lost, timeout, etc.).
    /// Adapters wrap their own internal errors into this variant via the
    /// `String` payload to keep the domain decoupled from
    /// infrastructure-specific error types.
    #[error("adapter transport failure: {0}")]
    AdapterTransport(String),

    /// The camera is streaming to another client over a transport that
    /// excludes ours: Arlo refuses our WebRTC session (error 14001) while
    /// the user watches the camera in the mobile app. Not a fault — the
    /// application waits for the view to end instead of backing off.
    #[error("camera busy: {0}")]
    CameraBusy(String),
}

/// Longest adapter-sourced reason kept, in bytes. The text reaches the
/// `Failed` state, the admin API and the logs.
pub const MAX_REASON_BYTES: usize = 256;

impl DomainError {
    /// Wrap text that came from the network or a library into
    /// [`DomainError::AdapterTransport`], with control characters
    /// removed and the length bounded by [`MAX_REASON_BYTES`], so a
    /// remote message can neither forge log lines nor bloat the state.
    #[must_use]
    pub fn adapter_transport(reason: impl std::fmt::Display) -> Self {
        Self::AdapterTransport(sanitize_reason(&reason.to_string()))
    }
}

/// Drop control characters (newlines included) and truncate to
/// [`MAX_REASON_BYTES`] on a character boundary, marking the cut.
#[must_use]
pub fn sanitize_reason(raw: &str) -> String {
    let clean: String = raw.chars().filter(|c| !c.is_control()).collect();
    if clean.len() <= MAX_REASON_BYTES {
        return clean;
    }
    let mut cut = MAX_REASON_BYTES;
    while !clean.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}…", &clean[..cut])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_reason_strips_control_characters_and_truncates() {
        assert_eq!(sanitize_reason("plain text"), "plain text");
        assert_eq!(
            sanitize_reason("line\none\r\nforged\x1b[31m"),
            "lineoneforged[31m"
        );
        let long = "é".repeat(MAX_REASON_BYTES);
        let cut = sanitize_reason(&long);
        assert!(cut.ends_with('…'));
        assert!(cut.len() <= MAX_REASON_BYTES + '…'.len_utf8());
    }

    #[test]
    fn adapter_transport_constructor_sanitizes() {
        let err = DomainError::adapter_transport("boom\nERROR forged");
        assert!(matches!(err, DomainError::AdapterTransport(ref r) if r == "boomERROR forged"));
    }
}
