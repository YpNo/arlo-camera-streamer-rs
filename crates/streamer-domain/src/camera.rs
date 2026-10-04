//! Identifier newtypes for cameras and output streams.
//!
//! These wrappers prevent argument-order mistakes (`StreamName` and
//! `CameraId` are both strings under the hood) and centralize input
//! validation at the trust boundary, per project security guidelines.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::DomainError;

/// Longest camera id accepted at a trust boundary. Arlo device ids are
/// 13 to 20 characters; the margin covers other id shapes without
/// letting a path or a header carry arbitrary text.
pub const MAX_CAMERA_ID_LEN: usize = 64;

/// Stable identifier of an Arlo device, as reported by the cloud API.
///
/// Treated as opaque by the rest of the system; only `streamer-infra-arlo`
/// inspects its contents. The id ends up in log lines, in a file name
/// and in HTTP paths, so everything that crosses a trust boundary (the
/// configuration, the admin API, the event bus) goes through
/// [`CameraId::parse`], which allows `[A-Za-z0-9_-]{1,64}` only.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CameraId(String);

impl CameraId {
    /// Construct a `CameraId` without validation, for ids this process
    /// already trusts: values the cloud's device list reports (printed
    /// back to the operator) and test fixtures. Input from a config
    /// file, an HTTP path or the event bus goes through [`Self::parse`].
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Parse an id from an untrusted source.
    ///
    /// # Errors
    ///
    /// [`DomainError::InvalidCameraId`] when `input` is empty, longer
    /// than [`MAX_CAMERA_ID_LEN`] or has characters outside
    /// `[A-Za-z0-9_-]`. The message describes the rule broken, never the
    /// input.
    pub fn parse(input: impl AsRef<str>) -> Result<Self, DomainError> {
        let s = input.as_ref();
        if s.is_empty() {
            return Err(DomainError::InvalidCameraId("empty".to_string()));
        }
        if s.len() > MAX_CAMERA_ID_LEN {
            return Err(DomainError::InvalidCameraId(format!(
                "longer than {MAX_CAMERA_ID_LEN} characters"
            )));
        }
        if !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(DomainError::InvalidCameraId(
                "characters outside [A-Za-z0-9_-]".to_string(),
            ));
        }
        Ok(Self(s.to_string()))
    }

    /// Borrow the inner identifier as a `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CameraId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for CameraId {
    type Error = DomainError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<CameraId> for String {
    fn from(id: CameraId) -> Self {
        id.0
    }
}

/// User-facing name for an output stream — appears as a path component
/// in RTSP / HLS / DASH URLs.
///
/// Validated to contain only URL-safe characters: `[A-Za-z0-9_-]+`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct StreamName(String);

impl StreamName {
    /// Parse a stream name, rejecting empty input or non-URL-safe characters.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidStreamName`] when input is empty or
    /// contains characters outside `[A-Za-z0-9_-]`.
    pub fn parse(input: impl AsRef<str>) -> Result<Self, DomainError> {
        let s = input.as_ref();
        if s.is_empty() {
            return Err(DomainError::InvalidStreamName("empty".to_string()));
        }
        if !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(DomainError::InvalidStreamName(s.to_string()));
        }
        Ok(Self(s.to_string()))
    }

    /// Borrow the inner stream name as a `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Derive a valid stream name from a device's display name, for
    /// config suggestions: ASCII letters and digits are kept (lowercased),
    /// every other run of characters becomes one `-`, and leading or
    /// trailing dashes are trimmed. A name with nothing usable falls back
    /// to `camera-<last characters of the device id>`. Never fails.
    #[must_use]
    pub fn suggest(display_name: &str, device_id: &CameraId) -> Self {
        let slug = slugify(display_name);
        if slug.is_empty() {
            let id_slug = slugify(device_id.as_str());
            let tail_start = id_slug.len().saturating_sub(SUGGESTED_ID_TAIL);
            let tail = id_slug[tail_start..].trim_start_matches('-');
            return Self(format!("camera-{tail}").trim_end_matches('-').to_string());
        }
        Self(slug)
    }
}

/// Characters of the device id kept by [`StreamName::suggest`]'s fallback.
const SUGGESTED_ID_TAIL: usize = 5;

/// Lowercase ASCII alphanumerics; any other run of characters becomes a
/// single `-`; no leading or trailing `-`.
fn slugify(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for c in input.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.is_empty() && !out.ends_with('-') {
            out.push('-');
        }
    }
    out.trim_end_matches('-').to_string()
}

/// A streamable device found on the Arlo account (camera, doorbell, Arlo
/// Q), as reported by the cloud's device list. Feeds the `list-devices`
/// CLI that helps users fill in `[[cameras]]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredDevice {
    /// The id to put in `arlo_device_id`.
    pub id: CameraId,
    /// Name the user gave the device in the Arlo app.
    pub name: String,
    /// Arlo device class (`camera`, `doorbell`, `arloq`).
    pub kind: String,
    /// Hardware model id, when Arlo reports one.
    pub model: Option<String>,
}

impl fmt::Display for StreamName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for StreamName {
    type Error = DomainError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<StreamName> for String {
    fn from(value: StreamName) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("livingroom-camera", "livingroom-camera")]
    #[case("LOPEZ-Doorbell", "lopez-doorbell")]
    #[case("Front Door", "front-door")]
    #[case("  Garage  Cam! ", "garage-cam")]
    #[case("Caméra 2", "cam-ra-2")]
    #[case("back_yard", "back-yard")]
    fn stream_name_suggest_slugifies_display_names(#[case] name: &str, #[case] expected: &str) {
        let suggested = StreamName::suggest(name, &CameraId::new("A4ATEST0A1D73"));
        assert_eq!(suggested.as_str(), expected);
        assert!(StreamName::parse(suggested.as_str()).is_ok());
    }

    #[rstest]
    #[case("", "camera-a1d73")]
    #[case("!!!", "camera-a1d73")]
    #[case("éèà", "camera-a1d73")]
    fn stream_name_suggest_falls_back_to_the_device_id(#[case] name: &str, #[case] expected: &str) {
        let suggested = StreamName::suggest(name, &CameraId::new("A4ATEST0A1D73"));
        assert_eq!(suggested.as_str(), expected);
    }

    #[test]
    fn stream_name_suggest_with_unusable_name_and_id_is_still_valid() {
        let suggested = StreamName::suggest("?", &CameraId::new("??"));
        assert_eq!(suggested.as_str(), "camera");
        assert!(StreamName::parse(suggested.as_str()).is_ok());
    }

    #[test]
    fn camera_id_round_trips_through_display() {
        let id = CameraId::new("ABCD-1234");
        assert_eq!(id.as_str(), "ABCD-1234");
        assert_eq!(format!("{id}"), "ABCD-1234");
    }

    #[rstest]
    #[case("front_door")]
    #[case("back-yard")]
    #[case("Garage1")]
    #[case("a")]
    fn stream_name_parse_accepts_url_safe(#[case] input: &str) {
        let parsed = StreamName::parse(input).expect("should accept url-safe input");
        assert_eq!(parsed.as_str(), input);
    }

    #[rstest]
    #[case("")]
    #[case("front door")]
    #[case("front/door")]
    #[case("hello!")]
    #[case("café")]
    fn stream_name_parse_rejects_invalid(#[case] input: &str) {
        let err = StreamName::parse(input).expect_err("should reject");
        assert!(matches!(err, DomainError::InvalidStreamName(_)));
    }

    #[test]
    fn stream_name_display_matches_str() {
        let name = StreamName::parse("front_door").unwrap();
        assert_eq!(format!("{name}"), "front_door");
    }

    #[test]
    fn stream_name_try_from_string_validates() {
        assert!(StreamName::try_from("ok_name".to_string()).is_ok());
        assert!(StreamName::try_from("bad name".to_string()).is_err());
    }

    #[test]
    fn stream_name_into_string_returns_inner() {
        let name = StreamName::parse("back_yard").unwrap();
        let s: String = name.into();
        assert_eq!(s, "back_yard");
    }

    #[test]
    fn camera_id_parse_accepts_arlo_shaped_ids_and_refuses_the_rest() {
        assert!(CameraId::parse("A4K1234567ABC").is_ok());
        assert!(CameraId::parse("cam_front-1").is_ok());
        assert!(CameraId::parse("x".repeat(MAX_CAMERA_ID_LEN)).is_ok());
        for bad in ["", "../../x", "cam\nid", "cam id", "cam/1", "%0Aforged"] {
            let err = CameraId::parse(bad).unwrap_err();
            assert!(matches!(err, DomainError::InvalidCameraId(_)), "{bad:?}");
            assert!(
                !err.to_string().contains(bad.trim()) || bad.is_empty(),
                "{err}"
            );
        }
        assert!(CameraId::parse("x".repeat(MAX_CAMERA_ID_LEN + 1)).is_err());
    }

    #[test]
    fn camera_id_deserialize_goes_through_parse() {
        let ok: CameraId = serde_json::from_str("\"CAM1\"").unwrap();
        assert_eq!(ok.as_str(), "CAM1");
        assert!(serde_json::from_str::<CameraId>("\"../x\"").is_err());
        assert_eq!(serde_json::to_string(&ok).unwrap(), "\"CAM1\"");
    }
}
