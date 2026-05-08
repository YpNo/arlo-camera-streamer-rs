//! IDR / keyframe detection helpers for the live-attach splice.
//!
//! When a live source is being attached, the multiplexer must wait
//! until the first IDR (instantaneous decoder refresh) frame arrives
//! before flipping the output pipeline to it. Switching mid-GOP would
//! send Frigate a stream that starts with delta frames — undecodable
//! until the next keyframe rolls in (potentially seconds later for an
//! Arlo camera with a 2 s keyframe interval).
//!
//! The decision is encoded in the buffer's flags: GStreamer marks a
//! delta frame with [`gstreamer::BufferFlags::DELTA_UNIT`]. A buffer
//! **without** that flag is, by definition, a keyframe / IDR.
//!
//! [`KeyframeWatcher`] maintains the "have we seen the first keyframe
//! yet?" state across multiple buffer arrivals. The actual pad probe
//! (which calls `record(buf.flags())` for every buffer) lives in
//! `gst_pipeline.rs`.

use gstreamer::BufferFlags;

/// Returns `true` when the supplied buffer flags describe a keyframe
/// (IDR for H.264/H.265). A keyframe is the absence of [`BufferFlags::DELTA_UNIT`].
#[must_use]
pub fn is_keyframe(flags: BufferFlags) -> bool {
    !flags.contains(BufferFlags::DELTA_UNIT)
}

/// Tracks whether a stream has yielded its first keyframe yet.
///
/// Single-pad scope: after `record` returns `true` once, all subsequent
/// `record` calls return `false`. Use [`Self::reset`] to re-arm after a
/// stream restart.
#[derive(Debug, Default)]
pub struct KeyframeWatcher {
    seen: bool,
}

impl KeyframeWatcher {
    /// New, un-armed watcher.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inspect a buffer's flags. Returns `true` **only on the first
    /// keyframe** observed. Subsequent calls return `false`.
    pub fn record(&mut self, flags: BufferFlags) -> bool {
        if self.seen {
            return false;
        }
        if is_keyframe(flags) {
            self.seen = true;
            return true;
        }
        false
    }

    /// Have we already observed (and signalled) a keyframe?
    #[must_use]
    pub fn has_seen_keyframe(&self) -> bool {
        self.seen
    }

    /// Re-arm so the next keyframe will trigger again.
    pub fn reset(&mut self) {
        self.seen = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_flags_are_keyframe() {
        assert!(is_keyframe(BufferFlags::empty()));
    }

    #[test]
    fn delta_unit_flag_means_not_keyframe() {
        assert!(!is_keyframe(BufferFlags::DELTA_UNIT));
    }

    #[test]
    fn other_flags_without_delta_unit_still_keyframe() {
        // A keyframe buffer may also carry HEADER, MARKER, etc.
        let flags = BufferFlags::HEADER | BufferFlags::MARKER;
        assert!(is_keyframe(flags));
    }

    #[test]
    fn delta_unit_combined_with_other_flags_is_not_keyframe() {
        let flags = BufferFlags::DELTA_UNIT | BufferFlags::HEADER;
        assert!(!is_keyframe(flags));
    }

    #[test]
    fn watcher_starts_unarmed() {
        let w = KeyframeWatcher::new();
        assert!(!w.has_seen_keyframe());
    }

    #[test]
    fn watcher_returns_true_on_first_keyframe_only() {
        let mut w = KeyframeWatcher::new();
        // Two delta frames — no signal.
        assert!(!w.record(BufferFlags::DELTA_UNIT));
        assert!(!w.record(BufferFlags::DELTA_UNIT));
        // Keyframe — signals.
        assert!(w.record(BufferFlags::empty()));
        // Subsequent keyframes — already armed, no signal.
        assert!(!w.record(BufferFlags::empty()));
    }

    #[test]
    fn watcher_remembers_after_signal() {
        let mut w = KeyframeWatcher::new();
        w.record(BufferFlags::empty());
        assert!(w.has_seen_keyframe());
    }

    #[test]
    fn reset_re_arms_watcher() {
        let mut w = KeyframeWatcher::new();
        w.record(BufferFlags::empty());
        assert!(w.has_seen_keyframe());
        w.reset();
        assert!(!w.has_seen_keyframe());
        // Re-armed: next keyframe triggers again.
        assert!(w.record(BufferFlags::empty()));
    }
}
