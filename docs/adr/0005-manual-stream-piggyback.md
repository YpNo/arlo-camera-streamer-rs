# ADR 0005 — Piggy-backing on a user's live view (manual stream)

- **Status:** Accepted for the event side; **the media side is superseded
  in practice** — see "Live capture, 2026-09-27" below. An ADR 0006 will
  decide the RTSP pick-up.
- **Date:** 2026-09-27
- **Deciders:** Senior architect, project owner
- **Supersedes:** —
- **Superseded by:** —

## Context

Two things wake an Arlo camera: its own motion/audio detection, and a
user opening a live view in the Arlo app. The daemon handled only the
first. When the user watched the camera from the phone, the NVR kept
showing the idle still, although the camera was already awake and
streaming on the user's account.

Arlo v3 live is WebRTC through a `FreeSWITCH` gateway: the camera keeps
one uplink and each viewer gets its own leg. The daemon can therefore
open its own leg while the app is watching, at no extra battery cost.
Two policies follow, both agreed with the owner:

- the daily live budget exists to protect the battery from **our**
  wake-ups; a piggy-backed session must not be charged;
- the session must end when the user stops watching. If Arlo keeps the
  uplink alive as long as any leg is attached, our leg would otherwise
  keep the camera awake after the app closed, which is the battery risk
  this feature must not introduce.

The wire signal is the camera's `activityState` property on the
`cameras/<id>` resource: `userStreamActive` while a user view runs,
`idle` when it ends. The names come from the pyaarlo ecosystem; the v3
MQTT bus carries the same payload shape as the retired SSE bus.

## Decision

- **Two new domain events**, `CameraEvent::ManualStream` and
  `CameraEvent::ManualStreamEnded`, mapped from
  `activityState == "userStreamActive"` / `"idle"`, with precedence
  motion > audio > activity state > connection state. Every other
  `activityState` value maps to nothing; in particular
  `alertStreamActive` (a motion recording) is not treated as motion,
  because the `motionDetected` flag that precedes it already is.
- **Two new signals**, `ManualStreamDetected` (activates from `Idle`
  **and** from `BatteryProtect`, since piggy-backing is free) and
  `ManualStreamEnded` (`Live | Cooling → Idle`).
- **The trigger is an orchestrator field**, `LiveTrigger::{Motion,
  Manual}`, set at activation and cleared on every exit, next to the
  ADR 0004 session handle. It decides:
  - timers: a manual session has no debounce window (the debouncer's
    `Held` state), only the `max_continuous_live` cap;
  - budget: `on_live_started` is never called for a manual session, so
    nothing is billed and `live_secs_today` excludes it;
  - motion pulses during a manual session are absorbed without polling
    the budget, so an exhausted budget cannot cut a free session short;
  - `ManualStreamEnded` becomes a signal only while the trigger is
    `Manual`; an `idle` report after a motion session or in `Idle` is a
    logged no-op.
- **Return to `BatteryProtect`.** After a manual session only, the exit
  to `Idle` re-polls the budget and emits `BudgetExhausted` when it is
  still exhausted, so a piggy-back that started in `BatteryProtect`
  lands back there. Motion sessions keep their behaviour unchanged.
- **Echo guard.** A `ManualStream` arriving within 5 s of our own live
  exit is dropped: if the camera reports our own leg as a user stream,
  the `userStreamActive` echo of a short session could otherwise start
  a phantom session that sustains itself through its own echoes.
- **End signals, in order:** the camera's `idle` report, then ADR 0004's
  `LiveLost` (stall watchdog), then the hard cap.
- The admin snapshot gains an optional `trigger` (`"motion"` /
  `"manual"`); the transition metric distinguishes the sessions through
  the `manual-stream-detected` / `manual-stream-ended` signal labels. No
  new counter, no new configuration key.
- The orchestrator now takes its monotonic clock from tokio, so the
  paused test clock drives every deadline and guard deterministically
  (the previous mix of clocks made timer tests spin in real time).

## Consequences

### Positive

- The NVR shows live video whenever the user is watching, for free.
- All policy lives in pure, tested code: reducer rows, debouncer state,
  orchestrator tests on a paused clock. No media-crate change.

### Negative / costs

- **The wire signal is a hypothesis until captured.** The adapter logs
  every unmapped `cameras/*` event with its property keys and activity
  state at `debug`, and every event at `trace`, never the values. The
  checklist to run from the phone, with `RUST_LOG=info,streamer_infra_arlo::events=debug`:
  1. Open a live view: does `cameras/<id>` publish `activityState ==
     "userStreamActive"` (exact key and value)?
  2. Close the app: does `idle` arrive **while our leg is attached**? If
     not, the end signal is `LiveLost` or the cap only.
  3. Wake through `/admin/.../wake` and force idle within a second: does
     our own attach/detach publish `userStreamActive` / `idle`? This
     decides whether the echo guard is load-bearing.
  4. Motion during a user view: does the state flip to
     `alertStreamActive` and back to `idle` while the user still
     watches? That would end our session early; the next
     `userStreamActive` re-attaches.
  5. Do `set`-action command echoes appear on the `out` topics?
- A user who opens the app during a motion session is not detected as
  such: the motion session keeps its trigger, its debounce and its bill.
- A full property dump carrying `activityState: "idle"` (reconnect,
  online re-announce) ends a manual session while the user may still be
  watching. It fails battery-safe; the next `userStreamActive` re-attaches.

## Alternatives considered

- **Trigger as a payload of `CameraState`** (`Activating { trigger }`,
  `Live { trigger, .. }`). Moves one reachable rule into the reducer at
  the cost of ~36 mechanical edits and a changed JSON/metric shape; the
  motion-absorb rule would stay in the orchestrator anyway. Rejected.
- **A generic budget re-check on every entry to `Idle`.** Would have
  turned an early `BudgetReset` timer (the deadline was truncated to
  whole seconds) into a hot loop until the wall clock crossed the reset,
  and silently changed motion behaviour. Rejected; the deadline is now
  rounded up and the re-check is manual-only.
- **Converting a motion session to manual when the app opens.** Split
  billing and a mid-flight timer switch for a marginal gain. Deferred.
- **Not mapping `idle`, relying on `LiveLost`.** Leaves the camera awake
  for the stall timeout, or for the whole cap if Arlo keeps the uplink
  for our leg. Rejected.
- **Mapping `alertStreamActive` as motion.** Double-counts motion; the
  capture may revisit it.
- **A separate `max_manual_live` cap or a new counter.** No evidence yet
  that the existing cap and the signal label are insufficient.

## Live capture, 2026-09-27

Run from the phone with the debug capture target. Findings:

1. **Wire signal confirmed.** Opening a live view publishes
   `cameras/<id>` with `activityState: "startUserStream"` (unmapped, as
   designed) followed ~200 ms later by `"userStreamActive"` (mapped to
   `ManualStream`). Closing the app publishes `"idle"` twice.
2. **Our own WebRTC leg is refused while the app streams.** The
   `sipInfo` call fails with Arlo error `14001`: *"RTSP Streaming in
   progress, SIP Streaming is not allowed, try after some time"*. The
   mobile app therefore streams through the legacy RTSP path, and Arlo
   allows one transport per camera at a time. The one-uplink-many-legs
   assumption behind this ADR does not hold across transports.
   Consequence: the piggy-back can only be an **RTSP pick-up** of the
   app's own stream, which is exactly what `ArloClient::get_stream_url`
   (action `get` on `/startStream`) exists for. That is the original
   feature request from the handoff, and it needs a second live-source
   kind in the media adapter (`rtspsrc` instead of `webrtcbin`).
3. **Echo guard exercised.** A second `userStreamActive` arrived 400 ms
   after the failed attach and was dropped by the guard.
4. **An MQTT event without `action`** was dropped by arlo-rs at the
   moment the app closed. arlo-rs now logs such events' keys and
   resource; the `peek_stream_url` probe in `../arlo-rs` shows both the
   peeked URL and the redacted bus events for the next capture.
5. Motion-triggered wakes still fail the same way while the app streams
   (pre-existing, now understood): the `Failed` state and its backoff
   absorb it.

What stays valid from this ADR: the two events, the trigger field, the
free budget, the cap-only timers, the `idle`-first end signal and the
echo guard. What changes: how the manual session's media is obtained.

## Second capture, 2026-09-28 (`peek_stream_url` probe)

1. **The `get` query on `/startStream` is not a passive read.** Polled on
   idle cameras it handed out fresh web sessions (a new session id per
   cycle) and every call was followed by the camera publishing its full
   state, i.e. the request reaches the camera. It must never run on a
   timer; arlo-rs's docs and probe were corrected accordingly.
2. **During a user view it returns a watch-along URL.** After
   `userStreamActive`, the query returned the user's own session id with
   `watchalong=true` on every call (only the per-call `egressToken`
   changes). This is Arlo's own mechanism for a second viewer.
3. **The URL is MPEG-DASH, not RTSP**:
   `https://weblivestream-<zone>.arlo.com:80/stream/<ip>/<camera>_<ts>.mpd?egressToken=…&watchalong=true`
   — the query identifies as the web client. Note the `https` scheme on
   port 80.
4. A query made within milliseconds of the user's session being created
   returned a fresh session instead of the watch-along; the next one
   returned the watch-along.

Direction for ADR 0006: on `ManualStream`, query once, accept only a
`watchalong=true` URL (one retry after 2 s), and feed it to a second
live-source kind in the media adapter (GStreamer DASH demux → H.264
decode → the same raw-I420 splice). Open questions before the design:
does GStreamer play the URL (TLS on port 80, token auth, segment
latency), and what codec does the manifest declare.

