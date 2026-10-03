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

States: `Idle`, `Activating`, `Live`, `BatteryProtect { reset_in }`,
`Failed { reason, retries }`. The session ends `debounce_secs` after the last motion
pulse, or at `max_continuous_live` (300 s, kept on purpose for the battery), both timed
by the debouncer.

**How Arlo reports motion (captured 2026-09-28):** a pulse, not a state. While motion
lasts, the camera repeats `activityState: fullFrameSnapshot` → `motionDetected: true` →
`motionDetected: false` about 5 s later, every ~10 s (longest gap seen: 13 s). So:

- one long motion = a train of `true` pulses; each restarts the cooldown and keeps the
  same WebRTC session — that *is* the long capture followed by a cooldown;
- `false` ends a pulse, not the motion: never start a cooldown on it (the mapper ignores
  it on purpose), and a lost `false` changes nothing;
- `debounce_secs` must stay above the pulse gap (> 15 s);
- motion longer than the hard cap ends the session; the next pulse starts a new one.

`Cooling` and `Live::since_secs` were removed on 2026-09-28: nothing produced or read
them.

| Signal | Effect |
|---|---|
| `MotionDetected` | `Idle → Activating`; in `Live` extends the session, never re-activates |
| `LiveAttached` | `Activating → Live` |
| `CooldownExpired`, `MaxLiveExceeded` | `Live → Idle` (detach + teardown + thumbnail refresh) |
| `LiveLost(reason)` | `Live → Idle`, same exit, **no backoff** (ADR 0004) |
| `CameraBusy` | `Activating → Idle`, **no backoff** (ADR 0005) |
| `UserViewStarted` / `UserViewEnded` / `UserViewUnavailable` | Relay of the app view (ADR 0007): `Idle → Activating`, `Live → Idle`, `Activating → Idle` without backoff |
| `UserViewProbe` | The relay lets go (`Live → Idle`) to let the camera report `idle`; `probe_until` arms a 2 s grace in `Idle`, after which `UserViewStarted` relays again unless the report came |
| `Failure(reason)` | `→ Failed` from any state; `process_signals` arms `failed_deadline` on **every** entry (an `Offline` while idle used to strand the camera), exponential backoff, then `BackoffElapsed → Idle`; an `Online` report in `Failed` emits `BackoffElapsed` at once |
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
  Since ADR 0007 the view **is relayed**: `ManualStream` in `Idle` → `UserViewStarted` →
  `Activating` → `start_user_view_relay()` (`user_views.watch_along_url` then
  `media.attach_user_view`) → `Live` with `session_source = UserView`. No debouncer, no
  budget charge, but a **release deadline**: our RTSP session keeps the camera streaming
  after the app closes, and the bus never says `idle` while we hold it (captured
  2026-10-01). `relay_deadline = now + user_view_probe_secs` → `handle_deadline` →
  `UserViewProbe` → `Idle` with `probe_until = now + USER_VIEW_PROBE_GRACE` (2 s; the `idle` report takes 0.3–0.8 s); the
  `Idle` deadline then re-emits `UserViewStarted` (through `user_view_relay_signal`, so
  the retry guard applies) unless `ManualStreamEnded` cleared `probe_until` first. With
  `user_view_probe_secs = 0` the deadline is `max_continuous_live` → `MaxLiveExceeded`,
  not resumed. `user_view_active()` is also true during a relay and its probe, so the
  still keeps the notice; `user_view_notice_expiry()` returns `None` while relaying —
  a past instant there spun the select loop (found by a hanging paused test).
  `ManualStreamEnded` → `UserViewEnded` → `Idle`; a failure →
  `UserViewUnavailable` → `Idle` (no backoff) + 30 s `USER_VIEW_RETRY` guard, same after
  a `LiveLost`. A motion pulse during a relay is absorbed and must not prime the
  debouncer (it would end the relay through the cooldown).
- **User-view notice**: `sync_user_view_notice()` keeps `media.set_user_view_notice` in
  step with `user_view_active()`. Call it after every change of `user_view_until`
  (report, `idle`, `CameraBusy`); a select arm fires it when the hold runs out.
- **Debouncer priming** only in `Idle|Live` (`motion_signal()`); priming in
  `Failed`/`BatteryProtect` left a stale hard-cap deadline that cut the next session short.
- **Snapshots** (`CameraEvent::SnapshotAvailable`, no URL in the domain) refresh the idle
  still only in `Idle|BatteryProtect|Failed` — the states where the still is on screen.
- **Admin commands** (wake, force-idle) are acked when dequeued, before acting: the reply
  timeout is 2 s and a WebRTC negotiation takes longer. Manual wake goes through
  `motion_signal()` like a real motion.

## Attach flow and its failures

`media.attach_live(camera, signaler)` owns the whole attach: the multiplexer gets live
sinks from the registry and races `WebrtcLive::start` against the session's loss signal.
`start` makes **one** `signaler.negotiate(camera, &mut OfferBuilder)` call: the signaler
fetches `sipInfo` (14001 → `CameraBusy`), calls back `build_offer(&ice_servers)` so
webrtcbin gathers the offer with them, then carries it to Arlo. No coordinates are cached
between calls. Any failure releases the live sinks (`detach_live_sink`), or the next
attach would be refused with "already in live mode". The orchestrator only maps the
outcome:

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
