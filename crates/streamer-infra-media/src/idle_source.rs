//! The idle picture.
//!
//! When a camera is `Idle`, the output pipeline still emits frames so
//! Frigate's RTSP client stays connected and detects no motion: a black
//! 1280×720 frame, the camera's last snapshot scaled over it once one is
//! known (`gst_pipeline.rs`), and a caption: `STANDBY · {name} ·
//! {timestamp}`, or `LIVE IN ARLO APP · {name}` while the user watches in
//! the app. The picture is drawn [`IDLE_FPS`] times a second and repeated
//! up to the unified rate (ADR 0010).

use streamer_domain::camera::StreamName;

/// Width of the idle picture (the unified size).
pub const SYNTHETIC_WIDTH: u32 = 1280;
/// Height of the idle picture (the unified size).
pub const SYNTHETIC_HEIGHT: u32 = 720;
/// How often the idle picture is drawn. `imagefreeze` repeats each one up
/// to the unified rate, so the encoder and the splice see the same caps
/// on both sides of the selector; a new caption shows within one drawing.
pub const IDLE_FPS: u32 = 1;

/// What the idle branch draws. Constructed by [`select_idle_source`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdleKind {
    /// A black frame with the given caption (and the snapshot, once
    /// known, between them).
    Synthetic {
        /// Camera name (used for log correlation, also displayed).
        stream_name: StreamName,
        /// Caption text (already includes name + timestamp).
        overlay: String,
    },
}

impl IdleKind {
    /// Discriminator label for tracing / metrics.
    #[must_use]
    pub fn kind_label(&self) -> &'static str {
        match self {
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

/// The idle picture a camera starts with: the standby caption at
/// `timestamp`, supplied by the caller so this stays deterministic.
#[must_use]
pub fn select_idle_source(stream_name: &StreamName, timestamp: &str) -> IdleKind {
    IdleKind::Synthetic {
        stream_name: stream_name.clone(),
        overlay: standby_caption(stream_name, timestamp),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name() -> StreamName {
        StreamName::parse("front_door").unwrap()
    }

    #[test]
    fn a_camera_starts_on_the_standby_caption() {
        let IdleKind::Synthetic {
            stream_name,
            overlay,
        } = select_idle_source(&name(), "2026-05-08T10:00:00");
        assert_eq!(stream_name.as_str(), "front_door");
        assert_eq!(overlay, "STANDBY · front_door · 2026-05-08T10:00:00");
        assert_eq!(select_idle_source(&name(), "x").kind_label(), "synthetic");
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
