//! Pure state-transition function for the per-camera state machine.
//!
//! Given a current [`CameraState`] and an incoming [`StateTransition`]
//! signal, [`transition`] returns the resulting state. The function is
//! deterministic, allocation-light, and free of side effects — the
//! orchestrator (Phase 3) calls it as a reducer and is responsible for
//! emitting the appropriate signals based on debouncer / budget /
//! adapter feedback.
//!
//! ## Transition matrix (Phase 1 v0.1)
//!
//! | From / Signal      | Motion | LiveAttached | LiveReady | CooldownExpired | MaxLiveExceeded | BudgetExhausted | BudgetReset | Failure | BackoffElapsed | LiveLost  | CameraBusy | UserViewStarted | UserViewEnded | UserViewUnavailable | UserViewProbe |
//! |--------------------|--------|--------------|-----------|-----------------|-----------------|-----------------|-------------|---------|----------------|-----------|------------|-----------------|---------------|---------------------|---------------|
//! | Idle               | Activ. | (ignored)    | (ignored) | (ignored)       | (ignored)       | BatteryProtect  | Idle        | Failed  | (ignored)      | (ignored) | (ignored)  | Activ.          | (ignored)     | (ignored)           | (ignored)     |
//! | Activating         | Activ. | Live         | Activ.    | (ignored)       | (ignored)       | BatteryProtect  | Activ.      | Failed  | (ignored)      | (ignored) | Idle       | Activ.          | (ignored)     | Idle                | (ignored)     |
//! | Live               | Live   | Live         | Live      | Idle            | Idle            | BatteryProtect  | Live        | Failed  | (ignored)      | Idle      | (ignored)  | Live            | Idle          | (ignored)           | Idle          |
//! | BatteryProtect     | (ign.) | (ignored)    | (ignored) | (ignored)       | (ignored)       | (ignored)       | Idle        | Failed  | (ignored)      | (ignored) | (ignored)  | (ignored)       | (ignored)     | (ignored)           | (ignored)     |
//! | Failed             | (ign.) | (ignored)    | (ignored) | (ignored)       | (ignored)       | (ignored)       | (ign.)      | Failed+ | Idle           | (ignored) | (ignored)  | (ignored)       | (ignored)     | (ignored)           | (ignored)     |
//!
//! `UserViewStarted` / `UserViewEnded` / `UserViewUnavailable` /
//! `UserViewProbe` (ADR 0007) drive the relay of a live view the user
//! started in the Arlo app: it activates from `Idle` only, ends when the
//! view ends, and a relay that cannot be set up returns to `Idle` without
//! backoff, like `CameraBusy`. A probe releases the stream (`Live →
//! Idle`) so the camera can report whether the view goes on; the
//! orchestrator relays again when no `idle` report follows. It emits
//! `UserViewEnded` and `UserViewProbe` only for a relay session.
//!
//! `CameraBusy` (ADR 0005) is an attach refused because the user is
//! watching the camera in the Arlo app. It returns to `Idle` without the
//! `Failed` backoff: nothing is broken, the camera is simply taken.
//!
//! `LiveLost` (ADR 0004) is the media adapter reporting a dead live
//! source. It is a plain return to `Idle`, not a `Failure`: the source
//! ending is not an adapter fault, the next motion re-activates through
//! the normal budget check, and the reason travels in the metric label.
//! In `Activating` the session handle does not exist yet, so the signal
//! cannot even be produced; the row is kept explicit for completeness.
//!
//! The `Failed → Failed (Failure)` transition increments the retry counter
//! so the orchestrator can apply exponential backoff before emitting
//! `BackoffElapsed`.

// The compact transition matrix above uses bare type names for legibility;
// backticking each cell would defeat the table's purpose.
#![allow(clippy::doc_markdown)]
// The reducer is organized per source state, so the catch-all
// `(_, _) => state.clone()` arm appears in several blocks. That layout is
// the readability win — collapsing across states would obscure which
// signals each state actually responds to.
#![allow(clippy::match_same_arms)]

use std::time::Duration;

use streamer_domain::state::{CameraState, StateTransition};

/// Apply `signal` to `state` and return the resulting state.
///
/// `BatteryProtect` and `Failed` initial sub-fields are populated with
/// zeroed sentinels — the orchestrator overwrites them with concrete
/// values (computed from `chrono::Local::now()` and `CooldownConfig`)
/// after this pure step returns.
#[must_use]
pub fn transition(state: &CameraState, signal: &StateTransition) -> CameraState {
    use CameraState as S;
    use StateTransition as T;

    match (state, signal) {
        // ---------- From Idle ----------
        (S::Idle, T::MotionDetected | T::UserViewStarted) => S::Activating,
        (S::Idle, T::BudgetExhausted) => battery_protect(),
        (S::Idle, T::Failure(reason)) => fresh_failed(reason),
        (S::Idle, _) => S::Idle,

        // ---------- From Activating ----------
        (S::Activating, T::LiveAttached) => S::Live,
        (S::Activating, T::CameraBusy | T::UserViewUnavailable) => S::Idle,
        (S::Activating, T::Failure(reason)) => fresh_failed(reason),
        (S::Activating, T::BudgetExhausted) => battery_protect(),
        (S::Activating, _) => S::Activating,

        // ---------- From Live ----------
        (
            S::Live,
            T::CooldownExpired
            | T::MaxLiveExceeded
            | T::LiveLost(_)
            | T::UserViewEnded
            | T::UserViewProbe,
        ) => S::Idle,
        (S::Live, T::BudgetExhausted) => battery_protect(),
        (S::Live, T::Failure(reason)) => fresh_failed(reason),
        (S::Live, _) => state.clone(),

        // ---------- From BatteryProtect ----------
        (S::BatteryProtect { .. }, T::BudgetReset) => S::Idle,
        (S::BatteryProtect { .. }, T::Failure(reason)) => fresh_failed(reason),
        (S::BatteryProtect { .. }, _) => state.clone(),

        // ---------- From Failed ----------
        (S::Failed { .. }, T::BackoffElapsed) => S::Idle,
        (S::Failed { reason: _, retries }, T::Failure(new_reason)) => S::Failed {
            reason: new_reason.clone(),
            retries: retries.saturating_add(1),
        },
        (S::Failed { .. }, _) => state.clone(),
    }
}

fn battery_protect() -> CameraState {
    CameraState::BatteryProtect {
        reset_in: Duration::ZERO,
    }
}

fn fresh_failed(reason: &str) -> CameraState {
    CameraState::Failed {
        reason: reason.to_string(),
        retries: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use streamer_domain::state::LiveLossReason;

    fn live() -> CameraState {
        CameraState::Live
    }

    fn battery() -> CameraState {
        CameraState::BatteryProtect {
            reset_in: Duration::from_hours(1),
        }
    }

    fn failed(retries: u32) -> CameraState {
        CameraState::Failed {
            reason: "boom".to_string(),
            retries,
        }
    }

    // ---------- Idle ----------

    #[test]
    fn idle_motion_activates() {
        assert_eq!(
            transition(&CameraState::Idle, &StateTransition::MotionDetected),
            CameraState::Activating
        );
    }

    #[test]
    fn idle_budget_exhausted_enters_battery_protect() {
        assert!(matches!(
            transition(&CameraState::Idle, &StateTransition::BudgetExhausted),
            CameraState::BatteryProtect { .. }
        ));
    }

    #[test]
    fn idle_failure_enters_failed_with_zero_retries() {
        let result = transition(
            &CameraState::Idle,
            &StateTransition::Failure("net down".to_string()),
        );
        assert!(matches!(result, CameraState::Failed { retries: 0, .. }));
    }

    #[rstest]
    #[case(StateTransition::LiveAttached)]
    #[case(StateTransition::LiveReady)]
    #[case(StateTransition::CooldownExpired)]
    #[case(StateTransition::MaxLiveExceeded)]
    #[case(StateTransition::BudgetReset)]
    #[case(StateTransition::BackoffElapsed)]
    fn idle_ignores_irrelevant_signals(#[case] signal: StateTransition) {
        assert_eq!(transition(&CameraState::Idle, &signal), CameraState::Idle);
    }

    // ---------- Activating ----------

    #[test]
    fn activating_live_attached_enters_live() {
        assert_eq!(
            transition(&CameraState::Activating, &StateTransition::LiveAttached),
            live()
        );
    }

    #[test]
    fn activating_failure_enters_failed() {
        let result = transition(
            &CameraState::Activating,
            &StateTransition::Failure("rtsp 401".to_string()),
        );
        assert!(matches!(result, CameraState::Failed { retries: 0, .. }));
    }

    #[rstest]
    #[case(StateTransition::MotionDetected)]
    #[case(StateTransition::LiveReady)]
    #[case(StateTransition::CooldownExpired)]
    #[case(StateTransition::MaxLiveExceeded)]
    #[case(StateTransition::BudgetReset)]
    #[case(StateTransition::BackoffElapsed)]
    fn activating_holds_on_irrelevant_signals(#[case] signal: StateTransition) {
        assert_eq!(
            transition(&CameraState::Activating, &signal),
            CameraState::Activating
        );
    }

    // ---------- Live ----------

    #[rstest]
    #[case(StateTransition::CooldownExpired)]
    #[case(StateTransition::MaxLiveExceeded)]
    fn live_returns_to_idle_on_cooldown_or_max(#[case] signal: StateTransition) {
        assert_eq!(transition(&live(), &signal), CameraState::Idle);
    }

    #[test]
    fn live_holds_on_motion() {
        let s = live();
        assert_eq!(transition(&s, &StateTransition::MotionDetected), s);
    }

    #[test]
    fn live_failure_enters_failed() {
        let result = transition(
            &live(),
            &StateTransition::Failure("pipeline crash".to_string()),
        );
        assert!(matches!(result, CameraState::Failed { retries: 0, .. }));
    }

    #[test]
    fn live_budget_exhausted_enters_battery_protect() {
        assert!(matches!(
            transition(&live(), &StateTransition::BudgetExhausted),
            CameraState::BatteryProtect { .. }
        ));
    }

    // ---------- LiveLost (ADR 0004) ----------

    #[rstest]
    #[case(LiveLossReason::RtpStalled)]
    #[case(LiveLossReason::PipelineError)]
    #[case(LiveLossReason::EndOfStream)]
    #[case(LiveLossReason::PeerDisconnected)]
    #[case(LiveLossReason::AdapterDropped)]
    fn live_live_lost_returns_idle_whatever_the_reason(#[case] reason: LiveLossReason) {
        assert_eq!(
            transition(&live(), &StateTransition::LiveLost(reason)),
            CameraState::Idle
        );
    }

    #[rstest]
    #[case(CameraState::Idle)]
    #[case(CameraState::Activating)]
    #[case(battery())]
    #[case(failed(2))]
    fn live_lost_is_ignored_outside_live(#[case] state: CameraState) {
        assert_eq!(
            transition(
                &state,
                &StateTransition::LiveLost(LiveLossReason::PeerDisconnected)
            ),
            state
        );
    }

    // ---------- User-view relay (ADR 0007) ----------

    #[test]
    fn idle_user_view_started_activates() {
        assert_eq!(
            transition(&CameraState::Idle, &StateTransition::UserViewStarted),
            CameraState::Activating
        );
    }

    #[test]
    fn activating_user_view_unavailable_returns_idle_without_failure() {
        assert_eq!(
            transition(
                &CameraState::Activating,
                &StateTransition::UserViewUnavailable
            ),
            CameraState::Idle
        );
    }

    #[test]
    fn live_user_view_ended_returns_idle() {
        assert_eq!(
            transition(&live(), &StateTransition::UserViewEnded),
            CameraState::Idle
        );
    }

    #[test]
    fn live_user_view_probe_returns_idle() {
        assert_eq!(
            transition(&live(), &StateTransition::UserViewProbe),
            CameraState::Idle
        );
    }

    #[rstest]
    #[case(live(), StateTransition::UserViewStarted)]
    #[case(CameraState::Idle, StateTransition::UserViewEnded)]
    #[case(CameraState::Idle, StateTransition::UserViewUnavailable)]
    #[case(CameraState::Idle, StateTransition::UserViewProbe)]
    #[case(CameraState::Activating, StateTransition::UserViewProbe)]
    #[case(battery(), StateTransition::UserViewStarted)]
    #[case(failed(1), StateTransition::UserViewStarted)]
    fn user_view_signals_are_ignored_elsewhere(
        #[case] state: CameraState,
        #[case] signal: StateTransition,
    ) {
        assert_eq!(transition(&state, &signal), state);
    }

    // ---------- CameraBusy (ADR 0005) ----------

    #[test]
    fn activating_camera_busy_returns_idle_without_failure() {
        assert_eq!(
            transition(&CameraState::Activating, &StateTransition::CameraBusy),
            CameraState::Idle
        );
    }

    #[rstest]
    #[case(CameraState::Idle)]
    #[case(live())]
    #[case(battery())]
    #[case(failed(1))]
    fn camera_busy_is_ignored_outside_activating(#[case] state: CameraState) {
        assert_eq!(transition(&state, &StateTransition::CameraBusy), state);
    }

    // ---------- BatteryProtect ----------

    #[test]
    fn battery_protect_resets_to_idle_on_budget_reset() {
        assert_eq!(
            transition(&battery(), &StateTransition::BudgetReset),
            CameraState::Idle
        );
    }

    #[test]
    fn battery_protect_ignores_motion() {
        let s = battery();
        assert_eq!(transition(&s, &StateTransition::MotionDetected), s);
    }

    #[test]
    fn battery_protect_failure_enters_failed() {
        let result = transition(&battery(), &StateTransition::Failure("crash".to_string()));
        assert!(matches!(result, CameraState::Failed { retries: 0, .. }));
    }

    // ---------- Failed ----------

    #[test]
    fn failed_backoff_elapsed_returns_to_idle() {
        assert_eq!(
            transition(&failed(3), &StateTransition::BackoffElapsed),
            CameraState::Idle
        );
    }

    #[test]
    fn failed_failure_increments_retries() {
        let result = transition(&failed(2), &StateTransition::Failure("again".to_string()));
        match result {
            CameraState::Failed { retries, reason } => {
                assert_eq!(retries, 3);
                assert_eq!(reason, "again");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn failed_retries_saturates_at_u32_max() {
        let result = transition(&failed(u32::MAX), &StateTransition::Failure("x".into()));
        assert!(matches!(
            result,
            CameraState::Failed {
                retries: u32::MAX,
                ..
            }
        ));
    }

    #[test]
    fn failed_holds_on_irrelevant_signals() {
        let s = failed(2);
        assert_eq!(transition(&s, &StateTransition::MotionDetected), s);
        assert_eq!(transition(&s, &StateTransition::LiveAttached), s);
        assert_eq!(transition(&s, &StateTransition::BudgetReset), s);
    }
}
