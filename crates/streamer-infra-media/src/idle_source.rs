//! Idle still-frame source selection.
//!
//! When a camera is `Idle`, the output pipeline still needs to emit
//! frames so Frigate's RTSP client stays connected and detects no
//! motion. We have two strategies:
//!
//! 1. **JPEG still** — a recent thumbnail fetched from the camera. Looks
//!    like a frozen frame of the actual scene; ideal for ops UIs.
//! 2. **Synthetic** — a black 1280×720 frame with a `STANDBY · {name} ·
//!    {timestamp}` overlay. Used when no thumbnail is available
//!    (newly provisioned camera, transient cloud failure).
//!
//! The selection is a pure function: feed in the optional thumbnail and
//! the camera identity, get back the rendered [`IdleKind`]. The actual
//! GStreamer realization lives in `gst_pipeline.rs`.

use bytes::Bytes;

use streamer_domain::camera::StreamName;

/// Default resolution for the synthetic idle frame.
pub const SYNTHETIC_WIDTH: u32 = 1280;
/// Default resolution for the synthetic idle frame.
pub const SYNTHETIC_HEIGHT: u32 = 720;
/// Idle stream framerate. Must be > 0 so RTSP clients keep their
/// session alive; 1 fps is plenty for a still frame.
pub const IDLE_FPS: u32 = 1;

/// One of the two idle strategies. Constructed by [`select_idle_source`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdleKind {
    /// Loop the supplied JPEG at [`IDLE_FPS`].
    JpegStill {
        /// Raw JPEG bytes. Decoded once by the pipeline.
        jpeg: Bytes,
    },
    /// Render a black frame with the given overlay text.
    Synthetic {
        /// Camera name (used for log correlation, also displayed).
        stream_name: StreamName,
        /// Overlay text (already includes name + timestamp).
        overlay: String,
    },
}

impl IdleKind {
    /// Discriminator label for tracing / metrics.
    #[must_use]
    pub fn kind_label(&self) -> &'static str {
        match self {
            Self::JpegStill { .. } => "jpeg_still",
            Self::Synthetic { .. } => "synthetic",
        }
    }
}

/// Caption of the synthetic idle frame: `STANDBY · <stream> · <timestamp>`.
#[must_use]
pub fn standby_caption(stream_name: &StreamName, timestamp: &str) -> String {
    format!("STANDBY · {stream_name} · {timestamp}")
}

/// Caption shown while the user watches the camera in the Arlo app,
/// which is the one live view the daemon cannot relay (ADR 0005).
#[must_use]
pub fn user_view_caption(stream_name: &StreamName) -> String {
    format!("LIVE IN ARLO APP · {stream_name}")
}

/// Pure selection: prefer the thumbnail when present and non-empty,
/// otherwise fall back to a synthetic standby frame with overlay.
///
/// `timestamp` is supplied by the caller (typically formatted via
/// `chrono::Local::now`) so this function stays deterministic and unit-testable.
#[must_use]
pub fn select_idle_source(
    thumbnail: Option<Bytes>,
    stream_name: &StreamName,
    timestamp: &str,
) -> IdleKind {
    match thumbnail {
        Some(jpeg) if !jpeg.is_empty() => IdleKind::JpegStill { jpeg },
        _ => IdleKind::Synthetic {
            stream_name: stream_name.clone(),
            overlay: standby_caption(stream_name, timestamp),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name() -> StreamName {
        StreamName::parse("front_door").unwrap()
    }

    #[test]
    fn select_returns_jpeg_when_thumbnail_present_and_non_empty() {
        let jpeg = Bytes::from_static(&[0xFF, 0xD8, 0xFF, 0xE0, 0x00]);
        let idle = select_idle_source(Some(jpeg.clone()), &name(), "2026-05-08T10:00:00");
        match idle {
            IdleKind::JpegStill { jpeg: got } => assert_eq!(got, jpeg),
            IdleKind::Synthetic { .. } => panic!("expected JpegStill, got Synthetic"),
        }
    }

    #[test]
    fn select_falls_back_to_synthetic_when_thumbnail_missing() {
        let idle = select_idle_source(None, &name(), "2026-05-08T10:00:00");
        match idle {
            IdleKind::Synthetic {
                stream_name,
                overlay,
            } => {
                assert_eq!(stream_name.as_str(), "front_door");
                assert!(overlay.contains("STANDBY"));
                assert!(overlay.contains("front_door"));
                assert!(overlay.contains("2026-05-08T10:00:00"));
            }
            IdleKind::JpegStill { .. } => panic!("expected Synthetic, got JpegStill"),
        }
    }

    #[test]
    fn select_falls_back_to_synthetic_when_thumbnail_is_empty_bytes() {
        let idle = select_idle_source(Some(Bytes::new()), &name(), "2026-05-08T10:00:00");
        assert!(matches!(idle, IdleKind::Synthetic { .. }));
    }

    #[test]
    fn kind_label_distinguishes_variants() {
        let jpeg = IdleKind::JpegStill {
            jpeg: Bytes::from_static(&[0]),
        };
        assert_eq!(jpeg.kind_label(), "jpeg_still");
        let synthetic = IdleKind::Synthetic {
            stream_name: name(),
            overlay: String::new(),
        };
        assert_eq!(synthetic.kind_label(), "synthetic");
    }

    #[test]
    fn captions_name_the_stream_and_say_where_the_live_view_is() {
        let name = StreamName::parse("front_door").unwrap();
        assert_eq!(
            standby_caption(&name, "2026-09-29T21:00:00"),
            "STANDBY · front_door · 2026-09-29T21:00:00"
        );
        assert_eq!(user_view_caption(&name), "LIVE IN ARLO APP · front_door");
    }
}
