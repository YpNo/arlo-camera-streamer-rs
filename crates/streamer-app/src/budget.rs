//! Per-camera daily live-budget tracker.
//!
//! Implements the optional `daily_live_budget` cap declared in
//! [`CooldownConfig`]. Once exhausted the camera enters
//! `BatteryProtect`; the orchestrator emits `BudgetReset` when the
//! configured local-time clock rolls over.
//!
//! The tracker reasons in [`NaiveDateTime`] coordinates so callers
//! decide whether to feed it `Local::now().naive_local()` (production)
//! or constructed timestamps (tests). The unit is **wall-clock**: a
//! daemon restart or system clock skew can shift the budget window —
//! that's acceptable for a daily quota and intentional, since we want
//! the reset to align with calendar boundaries, not monotonic ticks.
//!
//! Sentinel: `daily_live_budget == 0` disables the quota entirely; in
//! that mode `poll()` always returns [`BudgetVerdict::Unlimited`].

use chrono::{Duration as ChronoDuration, NaiveDate, NaiveDateTime, NaiveTime};

use streamer_domain::config::CooldownConfig;
use streamer_domain::error::DomainError;

/// Verdict emitted by [`LiveBudgetTracker::poll`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BudgetVerdict {
    /// Quota disabled (`daily_live_budget == 0`).
    Unlimited,
    /// Quota active; `remaining` time may be spent before exhaustion.
    Available {
        /// Time left in the current daily window.
        remaining: ChronoDuration,
    },
    /// Quota exhausted for the current window. The orchestrator should
    /// emit `StateTransition::BudgetExhausted`. A subsequent
    /// [`LiveBudgetTracker::next_reset`] call indicates when the
    /// quota refills.
    Exhausted,
}

/// Stateful per-camera budget tracker.
#[derive(Debug, Clone)]
pub struct LiveBudgetTracker {
    daily_budget: ChronoDuration,
    reset_time: NaiveTime,
    spent_today: ChronoDuration,
    last_reset_date: NaiveDate,
    current_session_start: Option<NaiveDateTime>,
}

impl LiveBudgetTracker {
    /// Construct a tracker anchored at `now`.
    ///
    /// `now` should be the wall-clock moment of orchestrator boot —
    /// it's used to determine which side of the reset boundary the
    /// daemon started on, so a restart at 23:55 doesn't accidentally
    /// double the budget window.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidConfig`] when `budget_reset` is
    /// not a valid `HH:MM` string.
    pub fn new(config: &CooldownConfig, now: NaiveDateTime) -> Result<Self, DomainError> {
        let reset_time = parse_hhmm(&config.budget_reset)?;
        let daily_budget =
            ChronoDuration::seconds(i64::try_from(config.daily_live_budget).map_err(|_| {
                DomainError::InvalidConfig(format!(
                    "daily_live_budget {} too large to fit i64 seconds",
                    config.daily_live_budget
                ))
            })?);
        let last_reset_date = previous_reset(now, reset_time).date();
        Ok(Self {
            daily_budget,
            reset_time,
            spent_today: ChronoDuration::zero(),
            last_reset_date,
            current_session_start: None,
        })
    }

    /// Record that a live session has just started at `now`.
    pub fn on_live_started(&mut self, now: NaiveDateTime) {
        self.maybe_reset(now);
        self.current_session_start = Some(now);
    }

    /// Record that the live session has ended at `now`. Adds the
    /// session's duration to `spent_today`.
    pub fn on_live_ended(&mut self, now: NaiveDateTime) {
        self.maybe_reset(now);
        if let Some(start) = self.current_session_start.take() {
            // Clamp to the reset boundary if the session crossed it,
            // so we don't bill yesterday's spend against today's quota.
            let billable_start = previous_reset(now, self.reset_time).max(start);
            let session = now.signed_duration_since(billable_start);
            if session > ChronoDuration::zero() {
                self.spent_today += session;
            }
        }
    }

    /// Compute the current verdict at `now`. May trigger an internal
    /// reset if the wall clock has crossed the configured boundary.
    pub fn poll(&mut self, now: NaiveDateTime) -> BudgetVerdict {
        if self.daily_budget.is_zero() {
            return BudgetVerdict::Unlimited;
        }
        self.maybe_reset(now);
        let in_flight = self
            .current_session_start
            .map_or(ChronoDuration::zero(), |start| {
                let billable_start = previous_reset(now, self.reset_time).max(start);
                now.signed_duration_since(billable_start)
                    .max(ChronoDuration::zero())
            });
        let total = self.spent_today + in_flight;
        if total >= self.daily_budget {
            BudgetVerdict::Exhausted
        } else {
            BudgetVerdict::Available {
                remaining: self.daily_budget - total,
            }
        }
    }

    /// Total seconds spent in `Live` for the current day window.
    /// Used by the admin snapshot.
    #[must_use]
    pub fn spent_secs_today(&self) -> u64 {
        u64::try_from(self.spent_today.num_seconds().max(0)).unwrap_or(0)
    }

    /// Configured daily budget in seconds (`0` when disabled).
    /// Used by the admin snapshot.
    #[must_use]
    pub fn daily_budget_secs(&self) -> u64 {
        u64::try_from(self.daily_budget.num_seconds().max(0)).unwrap_or(0)
    }

    /// Wall-clock moment of the *next* reset relative to `now`.
    /// The orchestrator schedules a `BudgetReset` signal against this.
    #[must_use]
    pub fn next_reset(&self, now: NaiveDateTime) -> NaiveDateTime {
        let today_reset = now.date().and_time(self.reset_time);
        if now < today_reset {
            today_reset
        } else {
            (now + ChronoDuration::days(1))
                .date()
                .and_time(self.reset_time)
        }
    }

    fn maybe_reset(&mut self, now: NaiveDateTime) {
        let last_reset = previous_reset(now, self.reset_time);
        if last_reset.date() > self.last_reset_date {
            self.spent_today = ChronoDuration::zero();
            self.last_reset_date = last_reset.date();
        }
    }
}

/// Compute the most recent reset moment <= `now`, given the configured
/// daily reset time.
fn previous_reset(now: NaiveDateTime, reset_time: NaiveTime) -> NaiveDateTime {
    let today = now.date().and_time(reset_time);
    if now >= today {
        today
    } else {
        (now - ChronoDuration::days(1)).date().and_time(reset_time)
    }
}

fn parse_hhmm(input: &str) -> Result<NaiveTime, DomainError> {
    NaiveTime::parse_from_str(input, "%H:%M")
        .map_err(|e| DomainError::InvalidConfig(format!("invalid budget_reset '{input}': {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(budget: u64, reset: &str) -> CooldownConfig {
        CooldownConfig {
            debounce_secs: 60,
            max_continuous_live: 300,
            daily_live_budget: budget,
            budget_reset: reset.to_string(),
            user_view_probe_secs: 0,
        }
    }

    fn dt(y: i32, m: u32, d: u32, hh: u32, mm: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .expect("valid date")
            .and_hms_opt(hh, mm, 0)
            .expect("valid time")
    }

    // ---------- Construction & validation ----------

    #[test]
    fn rejects_invalid_reset_format() {
        let err = LiveBudgetTracker::new(&cfg(0, "25:99"), dt(2024, 1, 1, 12, 0))
            .expect_err("must reject");
        assert!(matches!(err, DomainError::InvalidConfig(_)));
    }

    #[test]
    fn accepts_valid_reset_format() {
        assert!(LiveBudgetTracker::new(&cfg(0, "00:00"), dt(2024, 1, 1, 12, 0)).is_ok());
        assert!(LiveBudgetTracker::new(&cfg(0, "23:59"), dt(2024, 1, 1, 12, 0)).is_ok());
    }

    // ---------- Unlimited mode ----------

    #[test]
    fn zero_budget_is_unlimited() {
        let mut t = LiveBudgetTracker::new(&cfg(0, "00:00"), dt(2024, 1, 1, 12, 0)).expect("valid");
        assert_eq!(t.poll(dt(2024, 1, 1, 12, 0)), BudgetVerdict::Unlimited);
        // Even after a session, still unlimited.
        t.on_live_started(dt(2024, 1, 1, 12, 0));
        t.on_live_ended(dt(2024, 1, 1, 13, 0));
        assert_eq!(t.poll(dt(2024, 1, 1, 13, 0)), BudgetVerdict::Unlimited);
    }

    // ---------- Spend tracking ----------

    #[test]
    fn ended_session_subtracts_from_remaining() {
        // 1 hour quota, reset at midnight.
        let mut t =
            LiveBudgetTracker::new(&cfg(3600, "00:00"), dt(2024, 1, 1, 12, 0)).expect("valid");
        t.on_live_started(dt(2024, 1, 1, 12, 0));
        t.on_live_ended(dt(2024, 1, 1, 12, 30)); // spent 30 min
        match t.poll(dt(2024, 1, 1, 12, 30)) {
            BudgetVerdict::Available { remaining } => {
                assert_eq!(remaining, ChronoDuration::seconds(1800));
            }
            other => panic!("expected Available, got {other:?}"),
        }
    }

    #[test]
    fn in_flight_session_counts_toward_budget() {
        let mut t =
            LiveBudgetTracker::new(&cfg(3600, "00:00"), dt(2024, 1, 1, 12, 0)).expect("valid");
        t.on_live_started(dt(2024, 1, 1, 12, 0));
        // No `on_live_ended` yet — but poll() should still account for the in-flight 30 min.
        match t.poll(dt(2024, 1, 1, 12, 30)) {
            BudgetVerdict::Available { remaining } => {
                assert_eq!(remaining, ChronoDuration::seconds(1800));
            }
            other => panic!("expected Available, got {other:?}"),
        }
    }

    #[test]
    fn exhausted_when_spend_meets_budget() {
        let mut t =
            LiveBudgetTracker::new(&cfg(3600, "00:00"), dt(2024, 1, 1, 12, 0)).expect("valid");
        t.on_live_started(dt(2024, 1, 1, 12, 0));
        t.on_live_ended(dt(2024, 1, 1, 13, 0)); // spent 60 min == quota
        assert_eq!(t.poll(dt(2024, 1, 1, 13, 0)), BudgetVerdict::Exhausted);
    }

    // ---------- Daily reset ----------

    #[test]
    fn budget_resets_at_configured_time() {
        // 30-min quota, resets at 06:00.
        let mut t =
            LiveBudgetTracker::new(&cfg(1800, "06:00"), dt(2024, 1, 1, 12, 0)).expect("valid");
        t.on_live_started(dt(2024, 1, 1, 12, 0));
        t.on_live_ended(dt(2024, 1, 1, 12, 30));
        assert_eq!(t.poll(dt(2024, 1, 1, 12, 30)), BudgetVerdict::Exhausted);
        // Cross the reset boundary (next day 06:00).
        let after_reset = dt(2024, 1, 2, 6, 0);
        match t.poll(after_reset) {
            BudgetVerdict::Available { remaining } => {
                assert_eq!(remaining, ChronoDuration::seconds(1800));
            }
            other => panic!("expected Available after reset, got {other:?}"),
        }
    }

    #[test]
    fn session_crossing_reset_only_bills_post_reset_part() {
        // 1-hour quota, resets at midnight. Session 23:30 → 00:30.
        let mut t =
            LiveBudgetTracker::new(&cfg(3600, "00:00"), dt(2024, 1, 1, 12, 0)).expect("valid");
        t.on_live_started(dt(2024, 1, 1, 23, 30));
        t.on_live_ended(dt(2024, 1, 2, 0, 30));
        // Only 30 min after midnight should count against the new day's quota.
        match t.poll(dt(2024, 1, 2, 0, 30)) {
            BudgetVerdict::Available { remaining } => {
                assert_eq!(remaining, ChronoDuration::seconds(1800));
            }
            other => panic!("expected Available, got {other:?}"),
        }
    }

    #[test]
    fn next_reset_returns_today_if_before_reset_time() {
        let t = LiveBudgetTracker::new(&cfg(3600, "06:00"), dt(2024, 1, 1, 5, 0)).expect("valid");
        assert_eq!(t.next_reset(dt(2024, 1, 1, 5, 0)), dt(2024, 1, 1, 6, 0));
    }

    #[test]
    fn next_reset_returns_tomorrow_after_reset_time() {
        let t = LiveBudgetTracker::new(&cfg(3600, "06:00"), dt(2024, 1, 1, 12, 0)).expect("valid");
        assert_eq!(t.next_reset(dt(2024, 1, 1, 12, 0)), dt(2024, 1, 2, 6, 0));
    }

    #[test]
    fn rejects_overflowing_daily_budget() {
        // u64::MAX seconds cannot fit in i64 → InvalidConfig.
        let cfg = CooldownConfig {
            debounce_secs: 60,
            max_continuous_live: 300,
            daily_live_budget: u64::MAX,
            budget_reset: "00:00".to_string(),
            user_view_probe_secs: 0,
        };
        let err = LiveBudgetTracker::new(&cfg, dt(2024, 1, 1, 12, 0)).expect_err("must reject");
        assert!(matches!(err, DomainError::InvalidConfig(_)));
    }
}
