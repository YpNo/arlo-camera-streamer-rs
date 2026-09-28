# ADR 0005 — User views in the Arlo app: observe, never compete

- **Status:** Accepted (revised 2026-09-28 after three live captures; the
  first version, a piggy-backed "manual" session, was built, captured
  against, and removed)
- **Date:** 2026-09-27, revised 2026-09-28
- **Deciders:** Senior architect, project owner
- **Supersedes:** —
- **Superseded by:** —

## Context

Two things wake an Arlo camera: its own motion/audio detection, and the
user opening a live view in the Arlo mobile app. The owner wanted the
NVR to show the user's view too. Three live captures established what
Arlo allows:

1. **Wire signal.** Opening a view publishes `cameras/<id>` with
   `activityState: "startUserStream"`, then `"userStreamActive"`;
   closing it publishes `"idle"`. The MQTT bus carries it reliably.
2. **One transport per camera.** While the app streams (over RTSP),
   `sipInfo` refuses our WebRTC session with Arlo error **14001**:
   "RTSP Streaming in progress, SIP Streaming is not allowed, try after
   some time". A second leg is not possible across transports.
3. **The app's stream cannot be joined.** The `get` query on
   `/startStream` returns a `watchalong=true` MPEG-DASH URL during a
   user view, but Arlo's load balancer answers **502** for it whatever
   the client identity (plain, browser headers, browser TLS emulation,
   fresh tokens). The same query on an idle camera hands out fresh
   sessions and reaches the camera, so it must never be polled.

The owner's decision: handling two stream sessions is not the goal. Keep
the events, never attach for a user view, and never fail into backoff
because of one.

## Decision

- The events stay: `CameraEvent::ManualStream` (`userStreamActive`) and
  `CameraEvent::ManualStreamEnded` (`idle`). They drive an orchestrator
  flag, not the state machine.
- **A user view never triggers an attach.**
- **While a user view is known, a motion pulse (or an admin wake) does
  not start a session.** It is recorded as `suppressed-user-view` on the
  motion counter. A session that was already running when the user
  opened the app continues and ends on its own debounce/cap.
- The flag is set on `ManualStream` for `USER_VIEW_HOLD` (120 s),
  refreshed by every report, and cleared by `ManualStreamEnded`. The
  hold keeps a lost `idle` report from pausing motion for good.
- **Missed views are caught at the attach.** The Arlo adapter maps error
  14001 to `DomainError::CameraBusy`; the orchestrator turns it into
  `StateTransition::CameraBusy` (`Activating → Idle`, no `Failed`
  backoff), releases the signaling, and sets the user-view flag.
- The admin snapshot gains `user_view` (omitted when false).

## Consequences

### Positive

- No pointless `sipInfo` calls and no backoff while the user watches;
  motion resumes the moment the view ends.
- The failure metrics stay meaningful: a busy camera is not a failure.
- Everything is pure, paused-clock-tested orchestrator logic; the media
  adapter is untouched.

### Negative / costs

- The NVR does not show the user's own view, and motion during it is
  not recorded by the daemon (Arlo would refuse the session anyway).
- 14001 is detected from the message text while the workspace requires
  arlo-rs 0.2.0; arlo-rs keeps the code structured from its next
  release, and the adapter checks both forms.

## Alternatives considered

- **Piggy-back with our own WebRTC leg** (the first version of this ADR:
  a free, cap-only "manual" session). Refused by Arlo (14001). Removed.
- **Join the app's view through the watch-along DASH URL.** Arlo's load
  balancer answers 502 for every client identity tried. Abandoned.
- **Fetch the app's RTSP stream by impersonating the mobile app.** Would
  need a capture of the app's pinned-TLS traffic; high effort, fragile.
  Not pursued.
- **Keep attaching and let `Failed` absorb the refusals.** Wastes an API
  call per motion pulse and hides real failures in the backoff metrics.
