//! Pure building blocks of the live-loss detector (ADR 0004).
//!
//! The GStreamer-bound wiring in [`crate::webrtc_pipeline`] cannot run
//! on a workstation without GStreamer, so the decision logic lives here
//! with no `gstreamer` import and full unit coverage:
//!
//! - [`RtpActivity`] is the monotonic "last inbound video packet" clock,
//!   written from the `appsink` streaming thread with a single atomic
//!   store and read by the stall watchdog task.
//! - [`stall_verdict`] is the rule that turns a silence duration into a
//!   [`LiveLossReason::RtpStalled`], or not.
//!
//! Reporting goes straight through the domain's
//! [`LiveLossNotifier`](streamer_domain::stream::LiveLossNotifier), which
//! is already cloneable and first-wins; no adapter-side wrapper.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use streamer_domain::state::LiveLossReason;

/// Monotonic record of the most recent inbound video RTP packet.
///
/// Stores milliseconds since a fixed [`Instant`] anchor in an
/// `AtomicU64`, so the streaming-thread hot path never takes a lock and
/// wall-clock steps (NTP on a cold-booted Pi) cannot fake a stall.
#[derive(Debug)]
pub struct RtpActivity {
    anchor: Instant,
    last_ms: AtomicU64,
}

impl RtpActivity {
    /// Start the clock at `now`; counts as the last activity until the
    /// first [`touch`](Self::touch).
    #[must_use]
    pub fn new(now: Instant) -> Self {
        Self {
            anchor: now,
            last_ms: AtomicU64::new(0),
        }
    }

    /// Record an inbound packet at `now`.
    pub fn touch(&self, now: Instant) {
        self.last_ms
            .store(self.millis_since_anchor(now), Ordering::Relaxed);
    }

    /// How long the source has been silent as of `now`. Zero when `now`
    /// precedes the last recorded packet (clock reads from two threads
    /// may interleave).
    #[must_use]
    pub fn silent_for(&self, now: Instant) -> Duration {
        let now_ms = self.millis_since_anchor(now);
        let last_ms = self.last_ms.load(Ordering::Relaxed);
        Duration::from_millis(now_ms.saturating_sub(last_ms))
    }

    fn millis_since_anchor(&self, now: Instant) -> u64 {
        u64::try_from(now.saturating_duration_since(self.anchor).as_millis()).unwrap_or(u64::MAX)
    }
}

/// The stall rule: a source silent for at least `timeout` is lost.
#[must_use]
pub fn stall_verdict(silent_for: Duration, timeout: Duration) -> Option<LiveLossReason> {
    (silent_for >= timeout).then_some(LiveLossReason::RtpStalled)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> Instant {
        Instant::now()
    }

    #[test]
    fn rtp_activity_starts_silent_from_its_anchor() {
        let start = t0();
        let a = RtpActivity::new(start);
        assert_eq!(
            a.silent_for(start + Duration::from_millis(1500)),
            Duration::from_millis(1500)
        );
    }

    #[test]
    fn rtp_activity_touch_resets_silence() {
        let start = t0();
        let a = RtpActivity::new(start);
        a.touch(start + Duration::from_secs(5));
        assert_eq!(
            a.silent_for(start + Duration::from_secs(5) + Duration::from_millis(40)),
            Duration::from_millis(40)
        );
    }

    #[test]
    fn rtp_activity_silence_is_zero_when_now_precedes_last_touch() {
        let start = t0();
        let a = RtpActivity::new(start);
        a.touch(start + Duration::from_secs(2));
        assert_eq!(a.silent_for(start + Duration::from_secs(1)), Duration::ZERO);
    }

    #[test]
    fn stall_verdict_below_timeout_is_healthy() {
        assert_eq!(
            stall_verdict(Duration::from_millis(9999), Duration::from_secs(10)),
            None
        );
    }

    #[test]
    fn stall_verdict_at_timeout_boundary_is_stalled() {
        assert_eq!(
            stall_verdict(Duration::from_secs(10), Duration::from_secs(10)),
            Some(LiveLossReason::RtpStalled)
        );
        assert_eq!(
            stall_verdict(Duration::from_secs(11), Duration::from_secs(10)),
            Some(LiveLossReason::RtpStalled)
        );
    }
}
