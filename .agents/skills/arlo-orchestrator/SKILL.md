---
name: arlo-orchestrator
description: Per-camera state machine in streamer-app. Use when changing how bus events (motion, user views, snapshots), admin commands, budget, backoff or live-loss reports drive attach/detach, or when adding a state, signal or transition.
---
# Arlo Orchestrator Skill

One `CameraOrchestrator` tokio task per camera (`crates/streamer-app/src/orchestrator.rs`).
The pure reducer `transition(state, signal)` (`transition.rs`) decides the next state;
the orchestrator commits it, then runs side effects in `apply_state_change`.
Follow-up signals go on a FIFO queue drained by `process_signals`, never by recursion.

## States and signals

States: `Idle`, `Activating`, `Live { since_secs }`, `BatteryProtect { reset_in }`,
`Failed { reason, retries }`. The session ends `debounce_secs` after the last motion
(or at `max_continuous_live`). A `Cooling` state was removed on 2026-09-28 because nothing
produced it; reintroduce one only with a real input behind it (`motionDetected: false`,
ignored today), after a capture shows how Arlo repeats `motionDetected` during sustained
motion.

| Signal | Effect |
|---|---|
| `MotionDetected` | `Idle → Activating`; in `Live` extends the session, never re-activates |
| `LiveAttached` | `Activating → Live` |
| `CooldownExpired`, `MaxLiveExceeded` | `Live → Idle` (detach + teardown + thumbnail refresh) |
| `LiveLost(reason)` | `Live → Idle`, same exit, **no backoff** (ADR 0004) |
| `CameraBusy` | `Activating → Idle`, **no backoff** (ADR 0005) |
| `Failure(reason)` | `→ Failed`, exponential backoff, then `BackoffElapsed → Idle` |
| `BudgetExhausted` / `BudgetReset` | `→ BatteryProtect` / back to `Idle` |

The metric `signal` label is `signal_label(&StateTransition)`; `LiveLost` carries its
reason as `live-lost-<reason>`. Adding a signal means: reducer row, doc matrix row,
`signal_label` arm, test.

## Invariants

- **Live handle**: `self.live: Option<LiveSession>` is `Some` iff state is `Live`.
  It is dropped in `process_signals` *before* any side effect awaits; dropping it is what
  makes late loss reports harmless (no generation counters).
- **Battery rule**: every exit from `Live` pairs `detach_live` with
  `WebrtcSignaler::teardown` (`stop_arlo_live`); a failed attach and shutdown call
  teardown too, since an `Activating` negotiation may have opened a session. Teardown
  must be idempotent. A leaked session keeps the camera streaming on battery.
- **One session per camera**: never attach while the user views in the Arlo app.
  `ManualStream` sets `user_view_until = now + USER_VIEW_HOLD` (120 s, refreshed by each
  report), `ManualStreamEnded` clears it. Motion during a view is counted as
  `MotionOutcome::SuppressedUserView` and starts nothing; a running session is left alone.
  An unreported view shows up as `DomainError::CameraBusy` (Arlo 14001) from `attach_live`.
- **Debouncer priming** only in `Idle|Live` (`motion_signal()`); priming in
  `Failed`/`BatteryProtect` left a stale hard-cap deadline that cut the next session short.
- **Snapshots** (`CameraEvent::SnapshotAvailable`, no URL in the domain) refresh the idle
  still only in `Idle|BatteryProtect|Failed` — the states where the still is on screen.
- **Admin commands** (wake, force-idle) are acked when dequeued, before acting: the reply
  timeout is 2 s and a WebRTC negotiation takes longer. Manual wake goes through
  `motion_signal()` like a real motion.

## Attach flow and its failures

`media.attach_live(camera, signaler)` owns the whole attach: the multiplexer calls
`signaler.ice_servers`, gets live sinks from the registry, and races `WebrtcLive::start`
against the session's loss signal. The orchestrator only maps the outcome:

- `Ok(session)` → `LiveAttached`, store the handle.
- `Err(CameraBusy)` → `CameraBusy`, mark the camera as viewed.
- anything else → `Failure(msg)`. Messages worth recognising in logs:
  `live source lost during setup: <reason>` (a detector fired before the first RTP) and
  `splice timeout: no keyframe within 20s` (no RTP at all).

## Time

Use the module's `fn now()` (`tokio::time::Instant::now().into_std()`), never
`std::time::Instant::now()`: tests run on `#[tokio::test(start_paused = true)]`, and a
mixed clock makes paused tests spin forever. Deadlines computed from wall-clock resets
(battery-protect) are rounded **up** to the millisecond.

## Tests

- Doubles in `orchestrator.rs` tests **must retain every `LiveLossNotifier`** they mint:
  a dropped notifier resolves the session as `AdapterDropped` and ends the live state.
- Fire a loss with the retained notifier, then `advance` the paused clock; assert on
  recorded transitions and on detach/teardown call counts (battery rule).
- Names follow `<unit>_<scenario>_<expectedOutcome>`.

## Logging

`info!(?from, ?to, ?signal, "state transition")` for every committed transition;
`warn!` for attach failures with `latency_ms`; `debug!` for suppressed or ignored inputs.
Never log presigned URLs, egress tokens or credentials.
