# ADR 0005 — User views in the Arlo app: observe, never compete

- **Status:** Accepted for the rules below; superseded by
  [ADR 0007](./0007-relay-the-users-app-view.md) for what the NVR shows
  during a view (the view is relayed since 2026-10-01). Revised
  2026-09-28 after three live captures; the first version, a
  piggy-backed "manual" session, was built, captured against, and removed.
- **Date:** 2026-09-27, revised 2026-09-28
- **Deciders:** Senior architect, project owner
- **Supersedes:** —
- **Superseded by:** 0007 for the relay of the view; the suppression
  rules, the `CameraBusy` path and the notice stand

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
3. **The app's stream could not be joined** with a browser identity:
   the `get` query on `/startStream` returns a `watchalong=true`
   MPEG-DASH URL during a user view, and Arlo's load balancer answers
   **502** for it. The same query on an idle camera hands out fresh
   sessions and reaches the camera, so it must never be polled. (ADR
   0007 later found the stream behind the mobile-app identity.)

The decision then: handling two stream sessions is not the goal. Keep
the events, never attach our own session for a user view, and never
fail into backoff because of one. These rules still hold under 0007;
only "the NVR shows the still" became "the NVR shows the relayed view".

## Decision

- The events stay: `CameraEvent::ManualStream` (`userStreamActive`) and
  `CameraEvent::ManualStreamEnded` (`idle`). They drive an orchestrator
  flag, not the state machine.
- **A user view never triggers a WebRTC attach of ours.** (0007 attaches
  the view's own stream instead, which costs the camera nothing extra.)
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
- **The idle frame says where the live picture is** (added 2026-09-29):
  while the flag is set, its caption reads `LIVE IN ARLO APP · <stream>`
  instead of `STANDBY · …` (`MediaMultiplexer::set_user_view_notice`).
  The orchestrator syncs it on every flag change, including a
  `CameraBusy` refusal and the hold running out; the caption is kept per
  camera, so a media built later shows it too.

## Consequences

### Positive

- No pointless `sipInfo` calls and no backoff while the user watches;
  motion resumes the moment the view ends.
- The failure metrics stay meaningful: a busy camera is not a failure.
- Everything is pure, paused-clock-tested orchestrator logic; the media
  adapter is untouched.

### Negative / costs

- Motion during a view is not a session of ours (Arlo would refuse it);
  since 0007 the NVR shows the view itself, so nothing is lost on screen.
- 14001 is read from the structured error code arlo-rs 0.2.1 keeps.

## Alternatives considered

- **Piggy-back with our own WebRTC leg** (the first version of this ADR:
  a free, cap-only "manual" session). Refused by Arlo (14001). Removed.
- **Join the app's view through the watch-along DASH URL.** Arlo's load
  balancer answers 502 for every client identity tried. Abandoned.
- **Fetch the app's RTSP stream by impersonating the mobile app.** Judged
  to need a capture of the app's pinned-TLS traffic. It turned out to
  need only the app's `User-Agent` on the stream query — ADR 0007.
- **Keep attaching and let `Failed` absorb the refusals.** Wastes an API
  call per motion pulse and hides real failures in the backoff metrics.

## Re-examined 2026-09-29, resolved 2026-10-01

Every route to the view's stream was tried again on 2026-09-29 and failed
(14001, two 502s, `NO_ROUTE_DESTINATION`); the table is in ADR 0007's
context, which also records the route that worked: the same stream query
sent under the iOS app's `User-Agent` returns the view's RTSPS stream.
The probes that established the dead ends stay on the unpushed branches
`probe/force-start-during-view` (arlo-rs) and `probe/join-user-view`
(streamer); their one by-product, `force_start_stream` ignoring the URL
in `startUserStream`'s reply, is fixed in arlo-rs 0.2.2.
