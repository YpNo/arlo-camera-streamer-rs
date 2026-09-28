# ADR 0004 — Live-loss feedback from the media adapter to the orchestrator

- **Status:** Accepted
- **Date:** 2026-09-27
- **Deciders:** Senior architect, project owner
- **Supersedes:** —
- **Superseded by:** —

## Context

Since [ADR 0003](./0003-seamless-input-selector-splice.md) the live
source is spliced into a persistent pipeline through an
`input-selector`. `MediaMultiplexer::attach_live` resolved once, when
the first RTP flowed, and the media adapter had no way to talk back to
the per-camera orchestrator afterwards. `webrtcbin` bus errors and EOS
were only logged, and `StateTransition` had no variant for a source
that stops.

When Arlo's gateway ended the call, the camera went to sleep, or the
network dropped, the selector stayed on a dead live pad: connected RTSP
clients saw a frozen frame until the debounce timer (60 s by default)
or the continuous-live cap (300 s) fired. Motion-triggered sessions
usually outlived the debounce window, which is why this stayed hidden.
The coming `ManualStream` feature makes it the default case: a live view
started in the Arlo app ends when the user closes the app, while our
piggy-backed session keeps the selector on the live branch.

## Decision

`attach_live` returns a **`LiveSession` handle** built on a
`futures::channel::oneshot` pair (the domain crate already depends on
`futures`; it gains no async runtime). The media adapter keeps the
cloneable `LiveLossNotifier` half and hands a clone to each death
detector; the first to fire wins:

| Detector | Reason |
|---|---|
| No inbound video RTP for `webrtc.live_stall_timeout_secs` after the first packet (default 10 s, floor 4 s) | `rtp-stalled` |
| `webrtcbin` bus `ERROR` | `pipeline-error` |
| `webrtcbin` bus `EOS` | `end-of-stream` |
| `connection-state` / `ice-connection-state` reaching `failed` or `closed` | `peer-disconnected` |
| Notifier dropped without a report (adapter vanished) | `adapter-dropped` |

The orchestrator holds the handle exactly while the camera is `Live`
(the invariant is enforced in `process_signals`, right after the state
is committed), awaits it as one `select!` arm that is pending
otherwise, and turns a resolution into `StateTransition::LiveLost`.
`LiveLost` maps `Live → Idle` and is ignored in every other state
(the never-produced `Cooling` state that shared these rows was removed
on 2026-09-28); the `Live → Idle` side effects are the existing ones (detach,
`WebrtcSignaler::teardown`, budget, thumbnail refresh), so the battery
symmetry rule is untouched.

**Session identity is structural.** Each `attach_live` mints a fresh
pair. Dropping the handle on every live exit makes a late report from
that session unobservable; a stale notifier from session 1 can never
touch session 2. No generation counters, no session ids.

The reason travels in the transition metric's `signal` label
(`live-lost-<reason>`), which keeps the cardinality bounded (cameras × 5)
and needs no new port method or counter.

## Consequences

### Positive

- A dead live source returns the camera to idle within the stall
  timeout instead of minutes, with the upstream session released.
- The path is exercised by paused-clock tests in `streamer-app` with
  in-memory doubles; the GStreamer wiring stays thin and the decision
  logic (`live_watch.rs`) is pure and covered.
- `ManualStream` can be built on it without a second mechanism.

### Negative / costs

- One port signature changes, so every `MediaMultiplexer`
  implementation (production and test doubles) moves together.
  **Test doubles must retain the notifier**; dropping it resolves the
  session as `adapter-dropped` and flips the orchestrator to idle.
- A loss during the attach window fails the attach, not the session:
  the multiplexer races `WebrtcLive::start` against the session handle
  (`live_watch::setup_or_loss`, added 2026-09-28), so the attach returns
  `live source lost during setup: <reason>` and the orchestrator takes
  its ordinary `Failure` path (backoff, teardown). Before that race the
  same loss surfaced only as the 20 s first-RTP timeout. `start` must
  therefore stay cancel-safe: `WebrtcLive` owns the pipeline from its
  first line and stops it on drop.
- One extra tokio task per live session (the watchdog wakes once per
  timeout on a healthy source).
- The GStreamer-side detectors need GStreamer and a WebRTC peer. Since
  2026-09-28 `streamer-infra-media/tests/live_session.rs` runs them
  against a local `webrtcbin` gateway (stall → `rtp-stalled`, call lost
  during setup → `peer-disconnected`); the Frigate box remains the
  check against Arlo itself. The stall timeout is a config key so the
  test can use the 4 s floor.

## Alternatives considered

- **A per-camera health stream on the port**
  (`live_health(camera) -> BoxStream<…>`). Rejected: keyed by camera,
  not session, so every event would need a session token to be
  correlated, and multi-value health has no consumer.
- **A sender injected into the adapter feeding the per-camera event
  mailbox as a new `CameraEvent` variant.** Rejected: mixes media health
  into the "what the device said" vocabulary, changes the composition
  root wiring, and the mailbox is bounded with drop-on-full, so a loss
  could be dropped under a motion burst, which is the bug being fixed.
- **A dedicated `record_live_lost` metrics port method and counter.**
  Rejected: a loss always transitions, so the transition metric with a
  reason-qualified signal label carries identical information.
- **Adapter-owned teardown on loss.** Rejected outright: breaks the
  documented symmetry and lets the orchestrator's state diverge from
  reality.
- **Escalating repeated short sessions to `Failed` with backoff.**
  Deferred: no observed need; churn is already bounded by the debouncer
  and the daily budget, and the reason label is the telemetry to decide
  later.
- **Treating ICE `disconnected` as a loss.** Rejected: the state machine
  allows recovery, and a persistent disconnect is bounded by the stall
  watchdog anyway.
