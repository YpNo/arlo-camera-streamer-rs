//! Per-camera daily live-budget tracker.
//!
//! Implements the optional `daily_live_budget` cap declared in
//! [`CooldownConfig`]. Once exhausted the camera enters
//! `BatteryProtect`; the orchestrator emits `BudgetReset` when the
//! configured local-time clock rolls over.
//!
//! Two clocks, one job each ([`Moment`]): the **wall clock** places the
//! daily reset on the calendar (a daemon restart or a clock step can
//! shift the window, which is fine for a daily quota), and the
//! **monotonic clock** measures how long a session ran. Billing used to
//! subtract wall-clock instants, so an NTP step on an RTC-less box billed
//! hours for a short session (or nothing after a step back).
//!
//! Sentinel: `daily_live_budget == 0` disables the quota entirely; in
//! that mode `poll()` always returns [`BudgetVerdict::Unlimited`].

use std::time::Instant;

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

/// One instant seen by both clocks: `wall` decides which daily window it
/// belongs to, `mono` measures durations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Moment {
    /// Local wall-clock time.
    pub wall: NaiveDateTime,
    /// Monotonic time; immune to clock steps.
    pub mono: Instant,
}

/// Stateful per-camera budget tracker.
#[derive(Debug, Clone)]
pub struct LiveBudgetTracker {
    daily_budget: ChronoDuration,
    reset_time: NaiveTime,
    spent_today: ChronoDuration,
    last_reset_date: NaiveDate,
    current_session_start: Option<Moment>,
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
        let daily_budget = i64::try_from(config.daily_live_budget)
            .ok()
            .and_then(ChronoDuration::try_seconds)
            .ok_or_else(|| {
                DomainError::InvalidConfig(format!(
                    "daily_live_budget {} too large for a duration",
                    config.daily_live_budget
                ))
            })?;
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
    pub fn on_live_started(&mut self, now: Moment) {
        self.maybe_reset(now.wall);
        self.current_session_start = Some(now);
    }

    /// Record that the live session has ended at `now`. Adds the
    /// session's duration to `spent_today`.
    pub fn on_live_ended(&mut self, now: Moment) {
        self.maybe_reset(now.wall);
        if let Some(start) = self.current_session_start.take() {
            self.spent_today += self.billable(start, now);
        }
    }

    /// What a session from `start` to `now` costs today: the time it ran
    /// (monotonic), or only its part after the reset when it crossed the
    /// boundary, so yesterday's spend is not billed to today's quota.
    fn billable(&self, start: Moment, now: Moment) -> ChronoDuration {
        let ran = ChronoDuration::from_std(now.mono.saturating_duration_since(start.mono))
            .unwrap_or(ChronoDuration::MAX);
        let last_reset = previous_reset(now.wall, self.reset_time);
        if last_reset > start.wall {
            ran.min(now.wall.signed_duration_since(last_reset))
                .max(ChronoDuration::zero())
        } else {
            ran
        }
    }

    /// The session in flight's cost so far.
    fn in_flight(&self, now: Moment) -> ChronoDuration {
        self.current_session_start
            .map_or(ChronoDuration::zero(), |start| self.billable(start, now))
    }

    /// Charge `amount` against today's quota without a session: the cost
    /// of an activation itself (wake, negotiation), so repeated short
    /// sessions cannot drain the camera for free.
    pub fn charge(&mut self, now: NaiveDateTime, amount: ChronoDuration) {
        if self.daily_budget.is_zero() {
            return;
        }
        self.maybe_reset(now);
        self.spent_today += amount;
    }

    /// Time left in today's quota at `now`, counting the session in
    /// flight; `None` when the quota is disabled. Pure: no reset.
    #[must_use]
    pub fn remaining(&self, now: Moment) -> Option<ChronoDuration> {
        if self.daily_budget.is_zero() {
            return None;
        }
        let spent = self.spent_today + self.in_flight(now);
        Some((self.daily_budget - spent).max(ChronoDuration::zero()))
    }

    /// Compute the current verdict at `now`. May trigger an internal
    /// reset if the wall clock has crossed the configured boundary.
    pub fn poll(&mut self, now: Moment) -> BudgetVerdict {
        if self.daily_budget.is_zero() {
            return BudgetVerdict::Unlimited;
        }
        self.maybe_reset(now.wall);
        let total = self.spent_today + self.in_flight(now);
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

    /// Refill the quota when the reset instant moved to another day —
    /// in either direction: a clock stepped backwards across the
    /// boundary (RTC-less box before NTP, snapshot restore) used to leave
    /// the recorded date in the future and the quota never refilled.
    fn maybe_reset(&mut self, now: NaiveDateTime) {
        let last_reset = previous_reset(now, self.reset_time);
        if last_reset.date() != self.last_reset_date {
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

    /// A monotonic origin shared by a test, so `m()` moments keep both
    /// clocks in step unless a test steps the wall clock on purpose.
    fn origin() -> (NaiveDateTime, Instant) {
        thread_local! {
            static MONO: Instant = Instant::now();
        }
        (dt(2024, 1, 1, 0, 0), MONO.with(|m| *m))
    }

    /// The moment at wall time `y-m-d hh:mm`, with the monotonic clock in
    /// step with it.
    fn m(y: i32, mo: u32, d: u32, hh: u32, mm: u32) -> Moment {
        let wall = dt(y, mo, d, hh, mm);
        let (wall0, mono0) = origin();
        let offset = (wall - wall0).to_std().expect("after the origin");
        Moment {
            wall,
            mono: mono0 + offset,
        }
    }

    /// `at` with its wall clock stepped by `step` (the monotonic clock
    /// does not move).
    fn stepped(at: Moment, step: ChronoDuration) -> Moment {
        Moment {
            wall: at.wall + step,
            mono: at.mono,
        }
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
        assert_eq!(t.poll(m(2024, 1, 1, 12, 0)), BudgetVerdict::Unlimited);
        // Even after a session, still unlimited.
        t.on_live_started(m(2024, 1, 1, 12, 0));
        t.on_live_ended(m(2024, 1, 1, 13, 0));
        assert_eq!(t.poll(m(2024, 1, 1, 13, 0)), BudgetVerdict::Unlimited);
    }

    // ---------- Spend tracking ----------

    #[test]
    fn ended_session_subtracts_from_remaining() {
        // 1 hour quota, reset at midnight.
        let mut t =
            LiveBudgetTracker::new(&cfg(3600, "00:00"), dt(2024, 1, 1, 12, 0)).expect("valid");
        t.on_live_started(m(2024, 1, 1, 12, 0));
        t.on_live_ended(m(2024, 1, 1, 12, 30)); // spent 30 min
        match t.poll(m(2024, 1, 1, 12, 30)) {
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
        t.on_live_started(m(2024, 1, 1, 12, 0));
        // No `on_live_ended` yet — but poll() should still account for the in-flight 30 min.
        match t.poll(m(2024, 1, 1, 12, 30)) {
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
        t.on_live_started(m(2024, 1, 1, 12, 0));
        t.on_live_ended(m(2024, 1, 1, 13, 0)); // spent 60 min == quota
        assert_eq!(t.poll(m(2024, 1, 1, 13, 0)), BudgetVerdict::Exhausted);
    }

    // ---------- Daily reset ----------

    #[test]
    fn budget_resets_at_configured_time() {
        // 30-min quota, resets at 06:00.
        let mut t =
            LiveBudgetTracker::new(&cfg(1800, "06:00"), dt(2024, 1, 1, 12, 0)).expect("valid");
        t.on_live_started(m(2024, 1, 1, 12, 0));
        t.on_live_ended(m(2024, 1, 1, 12, 30));
        assert_eq!(t.poll(m(2024, 1, 1, 12, 30)), BudgetVerdict::Exhausted);
        // Cross the reset boundary (next day 06:00).
        let after_reset = m(2024, 1, 2, 6, 0);
        match t.poll(after_reset) {
            BudgetVerdict::Available { remaining } => {
                assert_eq!(remaining, ChronoDuration::seconds(1800));
            }
            other => panic!("expected Available after reset, got {other:?}"),
        }
    }

    /// A clock stepped backwards across the boundary (RTC-less box before
    /// NTP, a restored snapshot) used to leave the recorded reset date in
    /// the future, so the quota never refilled again.
    #[test]
    fn budget_resets_when_the_clock_steps_back_across_the_boundary() {
        let mut t =
            LiveBudgetTracker::new(&cfg(1800, "06:00"), dt(2024, 1, 5, 12, 0)).expect("valid");
        t.on_live_started(m(2024, 1, 5, 12, 0));
        t.on_live_ended(m(2024, 1, 5, 12, 30));
        assert_eq!(t.poll(m(2024, 1, 5, 12, 30)), BudgetVerdict::Exhausted);
        // The clock jumps back two days: a new day window, a fresh quota.
        match t.poll(m(2024, 1, 3, 12, 0)) {
            BudgetVerdict::Available { remaining } => {
                assert_eq!(remaining, ChronoDuration::seconds(1800));
            }
            other => panic!("expected Available after a backward step, got {other:?}"),
        }
        // And the quota keeps refilling at the following boundaries.
        t.on_live_started(m(2024, 1, 3, 12, 0));
        t.on_live_ended(m(2024, 1, 3, 12, 30));
        assert_eq!(t.poll(m(2024, 1, 3, 12, 30)), BudgetVerdict::Exhausted);
        assert!(matches!(
            t.poll(m(2024, 1, 4, 6, 0)),
            BudgetVerdict::Available { .. }
        ));
    }

    #[test]
    fn session_crossing_reset_only_bills_post_reset_part() {
        // 1-hour quota, resets at midnight. Session 23:30 → 00:30.
        let mut t =
            LiveBudgetTracker::new(&cfg(3600, "00:00"), dt(2024, 1, 1, 12, 0)).expect("valid");
        t.on_live_started(m(2024, 1, 1, 23, 30));
        t.on_live_ended(m(2024, 1, 2, 0, 30));
        // Only 30 min after midnight should count against the new day's quota.
        match t.poll(m(2024, 1, 2, 0, 30)) {
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

    #[test]
    fn charge_counts_against_today_and_remaining_tracks_the_session() {
        let mut t = LiveBudgetTracker::new(&cfg(100, "06:00"), dt(2024, 1, 1, 12, 0)).unwrap();
        assert_eq!(
            t.remaining(m(2024, 1, 1, 12, 0)),
            Some(ChronoDuration::seconds(100))
        );
        t.charge(dt(2024, 1, 1, 12, 0), ChronoDuration::seconds(15));
        t.on_live_started(m(2024, 1, 1, 12, 0));
        assert_eq!(
            t.remaining(m(2024, 1, 1, 12, 1)),
            Some(ChronoDuration::seconds(25))
        );
        assert_eq!(
            t.remaining(m(2024, 1, 1, 12, 5)),
            Some(ChronoDuration::zero())
        );
        assert_eq!(t.poll(m(2024, 1, 1, 12, 5)), BudgetVerdict::Exhausted);
        // Disabled quota: nothing to charge or report.
        let mut off = LiveBudgetTracker::new(&cfg(0, "06:00"), dt(2024, 1, 1, 12, 0)).unwrap();
        off.charge(dt(2024, 1, 1, 12, 0), ChronoDuration::seconds(15));
        assert_eq!(off.remaining(m(2024, 1, 1, 12, 0)), None);
        assert_eq!(off.poll(m(2024, 1, 1, 12, 0)), BudgetVerdict::Unlimited);
    }

    /// A clock stepped forward during a session (NTP on an RTC-less box)
    /// used to bill the step: hours for a minute of live.
    #[test]
    fn a_forward_clock_step_bills_only_the_time_the_session_ran() {
        let mut t = LiveBudgetTracker::new(&cfg(3600, "00:00"), dt(2024, 1, 1, 9, 0)).unwrap();
        let start = m(2024, 1, 1, 10, 0);
        t.on_live_started(start);
        let one_minute_later = m(2024, 1, 1, 10, 1);
        t.on_live_ended(stepped(one_minute_later, ChronoDuration::hours(5)));
        assert_eq!(t.spent_secs_today(), 60);
    }

    /// A step back used to bill nothing (a negative difference).
    #[test]
    fn a_backward_clock_step_still_bills_the_time_the_session_ran() {
        let mut t = LiveBudgetTracker::new(&cfg(3600, "00:00"), dt(2024, 1, 1, 9, 0)).unwrap();
        t.on_live_started(m(2024, 1, 1, 10, 0));
        let ten_minutes_later = m(2024, 1, 1, 10, 10);
        let back = stepped(ten_minutes_later, -ChronoDuration::minutes(30));
        assert_eq!(
            t.remaining(back),
            Some(ChronoDuration::minutes(50)),
            "in flight"
        );
        t.on_live_ended(back);
        assert_eq!(t.spent_secs_today(), 600);
    }
}
