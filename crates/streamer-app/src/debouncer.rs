//! Per-camera motion debouncer.
//!
//! This is the timer logic that decides *when* the orchestrator should
//! emit `CooldownExpired` or `MaxLiveExceeded` — entirely pure: no
//! tokio sleeps, no clock reads, no allocation. The orchestrator (Phase 3)
//! drives it by calling [`MotionDebouncer::on_motion`],
//! [`MotionDebouncer::on_live_attached`], and [`MotionDebouncer::poll`]
//! against `tokio::time::Instant::now()`, and uses
//! [`MotionDebouncer::next_deadline`] to schedule the wake-up sleep.
//!
//! ## Semantics
//!
//! Two caps run in parallel while a live session is active:
//!
//! - **Debounce window** — `debounce_secs` after the last motion event.
//!   A new motion within the window resets the timer.
//! - **Hard cap** — `max_continuous_live` after live attach. Cannot be
//!   reset by motion. Protects battery under sustained motion.
//!
//! Whichever cap fires first wins, and the verdict signals which one
//! tripped so the orchestrator can log + emit the matching
//! [`StateTransition`](streamer_domain::state::StateTransition).
//!
//! A **manual** (piggy-backed, ADR 0005) session started with
//! [`MotionDebouncer::on_manual_session`] has no debounce window: the
//! user may watch for minutes without any motion, so only the hard cap
//! applies and motion pulses are ignored until the session ends.
//!
//! Because the debouncer is monotonic, it operates in [`Instant`]
//! coordinates throughout — daily-reset wall-clock concerns belong to
//! [`crate::budget`].

use std::time::{Duration, Instant};

use streamer_domain::config::CooldownConfig;

/// Verdict emitted by [`MotionDebouncer::poll`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DebouncerVerdict {
    /// No live session in flight.
    Idle,
    /// Live session active and within both caps.
    KeepLive,
    /// `debounce_secs` elapsed since the last motion event.
    DebounceExpired,
    /// `max_continuous_live` hard cap hit (battery protection).
    MaxLiveExceeded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DebouncerState {
    Off,
    /// Motion-triggered: debounce window since `last_motion` plus the
    /// hard cap since `live_since`.
    Active {
        live_since: Instant,
        last_motion: Instant,
    },
    /// Piggy-backed on a user live view: hard cap only.
    Held {
        live_since: Instant,
    },
}

/// Stateful per-camera motion debouncer.
#[derive(Debug, Clone)]
pub struct MotionDebouncer {
    debounce: Duration,
    max_continuous: Duration,
    state: DebouncerState,
}

impl MotionDebouncer {
    /// Construct a debouncer from a [`CooldownConfig`].
    #[must_use]
    pub fn new(config: &CooldownConfig) -> Self {
        Self {
            debounce: Duration::from_secs(config.debounce_secs),
            max_continuous: Duration::from_secs(config.max_continuous_live),
            state: DebouncerState::Off,
        }
    }

    /// Record a motion pulse at `now`.
    ///
    /// In `Off` state this primes the debouncer for an upcoming live
    /// session — the orchestrator's `Idle → Activating → Live` transition
    /// will subsequently call [`Self::on_live_attached`] which is a
    /// no-op in `Active` state.
    pub fn on_motion(&mut self, now: Instant) {
        self.state = match self.state {
            DebouncerState::Off => DebouncerState::Active {
                live_since: now,
                last_motion: now,
            },
            DebouncerState::Active {
                live_since,
                last_motion: _,
            } => DebouncerState::Active {
                live_since,
                last_motion: now,
            },
            // A manual session has no debounce window to re-arm.
            DebouncerState::Held { live_since } => DebouncerState::Held { live_since },
        };
    }

    /// Start a manual (piggy-backed) session at `now`: only the hard
    /// cap applies from here on. Unconditional — it overrides any
    /// priming left by a motion pulse that arrived while the manual
    /// attach was in flight, or a stale session from before a
    /// `BatteryProtect` period.
    pub fn on_manual_session(&mut self, now: Instant) {
        self.state = DebouncerState::Held { live_since: now };
    }

    /// Record that the live source has been attached at `now`.
    /// Idempotent in `Active` state (only updates `live_since` if we
    /// were `Off`).
    pub fn on_live_attached(&mut self, now: Instant) {
        if matches!(self.state, DebouncerState::Off) {
            self.state = DebouncerState::Active {
                live_since: now,
                last_motion: now,
            };
        }
    }

    /// Reset the debouncer to its initial `Off` state. Called by the
    /// orchestrator on every transition into `CameraState::Idle`.
    pub fn on_idle(&mut self) {
        self.state = DebouncerState::Off;
    }

    /// Compute the verdict at `now`.
    #[must_use]
    pub fn poll(&self, now: Instant) -> DebouncerVerdict {
        match self.state {
            DebouncerState::Off => DebouncerVerdict::Idle,
            DebouncerState::Active {
                live_since,
                last_motion,
            } => {
                let live_elapsed = now.saturating_duration_since(live_since);
                let since_motion = now.saturating_duration_since(last_motion);
                if live_elapsed >= self.max_continuous {
                    DebouncerVerdict::MaxLiveExceeded
                } else if since_motion >= self.debounce {
                    DebouncerVerdict::DebounceExpired
                } else {
                    DebouncerVerdict::KeepLive
                }
            }
            DebouncerState::Held { live_since } => {
                if now.saturating_duration_since(live_since) >= self.max_continuous {
                    DebouncerVerdict::MaxLiveExceeded
                } else {
                    DebouncerVerdict::KeepLive
                }
            }
        }
    }

    /// Earliest [`Instant`] at which `poll(now)` will return a verdict
    /// other than [`DebouncerVerdict::KeepLive`] / [`DebouncerVerdict::Idle`].
    /// Returns `None` while in `Off` state.
    ///
    /// The orchestrator schedules a `tokio::time::sleep_until` against
    /// this deadline.
    #[must_use]
    pub fn next_deadline(&self, now: Instant) -> Option<Instant> {
        match self.state {
            DebouncerState::Off => None,
            DebouncerState::Active {
                live_since,
                last_motion,
            } => {
                let max_deadline = live_since + self.max_continuous;
                let debounce_deadline = last_motion + self.debounce;
                let next = max_deadline.min(debounce_deadline);
                Some(next.max(now))
            }
            DebouncerState::Held { live_since } => {
                Some((live_since + self.max_continuous).max(now))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(debounce: u64, max: u64) -> CooldownConfig {
        CooldownConfig {
            debounce_secs: debounce,
            max_continuous_live: max,
            daily_live_budget: 0,
            budget_reset: "00:00".to_string(),
        }
    }

    #[test]
    fn fresh_debouncer_is_idle() {
        let d = MotionDebouncer::new(&cfg(60, 300));
        let now = Instant::now();
        assert_eq!(d.poll(now), DebouncerVerdict::Idle);
        assert!(d.next_deadline(now).is_none());
    }

    #[test]
    fn motion_alone_starts_active_session() {
        let mut d = MotionDebouncer::new(&cfg(60, 300));
        let t0 = Instant::now();
        d.on_motion(t0);
        assert_eq!(d.poll(t0), DebouncerVerdict::KeepLive);
        assert!(d.next_deadline(t0).is_some());
    }

    #[test]
    fn live_attached_starts_active_session_when_off() {
        let mut d = MotionDebouncer::new(&cfg(60, 300));
        let t0 = Instant::now();
        d.on_live_attached(t0);
        assert_eq!(d.poll(t0), DebouncerVerdict::KeepLive);
    }

    #[test]
    fn live_attached_is_idempotent_in_active_state() {
        // Use a very long debounce so it can't mask the max-cap test.
        // Intent: confirm that calling on_live_attached() *after* an
        // on_motion() does NOT advance `live_since` past `t0`.
        let mut d = MotionDebouncer::new(&cfg(10_000, 300));
        let t0 = Instant::now();
        d.on_motion(t0);
        // Late on_live_attached() should be a no-op since we're already Active.
        let t1 = t0 + Duration::from_secs(30);
        d.on_live_attached(t1);
        // Just under the max cap → still KeepLive.
        let t2 = t0 + Duration::from_secs(299);
        assert_eq!(d.poll(t2), DebouncerVerdict::KeepLive);
        // Past the cap (measured from t0, NOT t1).
        let t3 = t0 + Duration::from_secs(301);
        assert_eq!(d.poll(t3), DebouncerVerdict::MaxLiveExceeded);
    }

    #[test]
    fn debounce_expires_after_window_without_motion() {
        let mut d = MotionDebouncer::new(&cfg(60, 300));
        let t0 = Instant::now();
        d.on_motion(t0);
        let t1 = t0 + Duration::from_secs(59);
        assert_eq!(d.poll(t1), DebouncerVerdict::KeepLive);
        let t2 = t0 + Duration::from_mins(1);
        assert_eq!(d.poll(t2), DebouncerVerdict::DebounceExpired);
    }

    #[test]
    fn motion_within_window_resets_debounce() {
        let mut d = MotionDebouncer::new(&cfg(60, 300));
        let t0 = Instant::now();
        d.on_motion(t0);
        let t1 = t0 + Duration::from_secs(50);
        d.on_motion(t1); // resets
        // 50 s after the second motion, we are still within the window.
        let t2 = t1 + Duration::from_secs(50);
        assert_eq!(d.poll(t2), DebouncerVerdict::KeepLive);
        // 60 s after the second motion, debounce expires.
        let t3 = t1 + Duration::from_mins(1);
        assert_eq!(d.poll(t3), DebouncerVerdict::DebounceExpired);
    }

    #[test]
    fn max_continuous_cap_overrides_debounce_reset() {
        let mut d = MotionDebouncer::new(&cfg(60, 120));
        let t0 = Instant::now();
        d.on_motion(t0);
        // Sustained motion every 30 s: each pulse resets debounce, but
        // the hard cap (120 s) ignores motion.
        for offset in (30..=120).step_by(30) {
            d.on_motion(t0 + Duration::from_secs(offset));
        }
        let t_cap = t0 + Duration::from_mins(2);
        assert_eq!(d.poll(t_cap), DebouncerVerdict::MaxLiveExceeded);
    }

    #[test]
    fn on_idle_returns_to_off_state() {
        let mut d = MotionDebouncer::new(&cfg(60, 300));
        let t0 = Instant::now();
        d.on_motion(t0);
        d.on_idle();
        assert_eq!(d.poll(t0 + Duration::from_secs(1)), DebouncerVerdict::Idle);
        assert!(d.next_deadline(t0).is_none());
    }

    #[test]
    fn next_deadline_picks_earlier_of_debounce_and_max() {
        // debounce 60 s, max 30 s → max wins.
        let mut d = MotionDebouncer::new(&cfg(60, 30));
        let t0 = Instant::now();
        d.on_motion(t0);
        let deadline = d.next_deadline(t0).expect("active");
        assert_eq!(deadline, t0 + Duration::from_secs(30));
    }

    #[test]
    fn next_deadline_clamped_to_now() {
        let mut d = MotionDebouncer::new(&cfg(10, 20));
        let t0 = Instant::now();
        d.on_motion(t0);
        // Polling far past the deadline — `next_deadline` should clamp
        // to `now` so the orchestrator doesn't sleep into the past.
        let t_past = t0 + Duration::from_mins(1);
        let deadline = d.next_deadline(t_past).expect("active");
        assert!(deadline >= t_past);
    }

    #[test]
    fn poll_after_idle_returns_idle() {
        let mut d = MotionDebouncer::new(&cfg(60, 300));
        d.on_idle();
        assert_eq!(d.poll(Instant::now()), DebouncerVerdict::Idle);
    }

    // ---------- Manual (piggy-backed) sessions — ADR 0005 ----------

    #[test]
    fn manual_session_ignores_debounce_window() {
        let t0 = Instant::now();
        let mut d = MotionDebouncer::new(&cfg(2, 300));
        d.on_manual_session(t0);
        assert_eq!(
            d.poll(t0 + Duration::from_secs(100)),
            DebouncerVerdict::KeepLive
        );
    }

    #[test]
    fn manual_session_hits_max_cap() {
        let t0 = Instant::now();
        let mut d = MotionDebouncer::new(&cfg(2, 10));
        d.on_manual_session(t0);
        assert_eq!(
            d.poll(t0 + Duration::from_secs(10)),
            DebouncerVerdict::MaxLiveExceeded
        );
    }

    #[test]
    fn manual_session_next_deadline_is_cap_only() {
        let t0 = Instant::now();
        let mut d = MotionDebouncer::new(&cfg(2, 10));
        d.on_manual_session(t0);
        assert_eq!(d.next_deadline(t0), Some(t0 + Duration::from_secs(10)));
    }

    #[test]
    fn motion_during_manual_session_does_not_arm_debounce() {
        let t0 = Instant::now();
        let mut d = MotionDebouncer::new(&cfg(2, 10));
        d.on_manual_session(t0);
        d.on_motion(t0 + Duration::from_secs(1));
        assert_eq!(
            d.poll(t0 + Duration::from_secs(5)),
            DebouncerVerdict::KeepLive
        );
        assert_eq!(d.next_deadline(t0), Some(t0 + Duration::from_secs(10)));
    }

    #[test]
    fn on_manual_session_overrides_stale_motion_priming() {
        let t0 = Instant::now();
        let mut d = MotionDebouncer::new(&cfg(2, 10));
        d.on_motion(t0);
        d.on_manual_session(t0 + Duration::from_secs(9));
        // The cap counts from the manual start, not the stale pulse.
        assert_eq!(
            d.poll(t0 + Duration::from_secs(12)),
            DebouncerVerdict::KeepLive
        );
    }

    #[test]
    fn on_idle_from_manual_session_returns_off() {
        let t0 = Instant::now();
        let mut d = MotionDebouncer::new(&cfg(2, 10));
        d.on_manual_session(t0);
        d.on_idle();
        assert_eq!(d.poll(t0), DebouncerVerdict::Idle);
        assert_eq!(d.next_deadline(t0), None);
    }

    #[test]
    fn on_live_attached_after_manual_session_is_noop() {
        let t0 = Instant::now();
        let mut d = MotionDebouncer::new(&cfg(2, 10));
        d.on_manual_session(t0);
        d.on_live_attached(t0 + Duration::from_secs(5));
        assert_eq!(d.next_deadline(t0), Some(t0 + Duration::from_secs(10)));
    }
}
