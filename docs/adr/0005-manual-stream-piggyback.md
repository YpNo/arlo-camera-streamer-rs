# ADR 0005 — User views in the Arlo app: observe, never compete

- **Status:** Accepted (revised 2026-09-28 after three live captures; the
  first version, a piggy-backed "manual" session, was built, captured
  against, and removed)
- **Date:** 2026-09-27, revised 2026-09-28, re-examined 2026-09-29
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

- The NVR does not show the user's own view, and motion during it is
  not recorded by the daemon (Arlo would refuse the session anyway).
- 14001 is read from the structured error code arlo-rs 0.2.1 keeps.

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

## Re-examined 2026-09-29: the app's view cannot be relayed

The owner asked again whether the NVR can show a live view started in the
app when no motion session runs. Every route to that stream was tried,
during an app view, with the app's own view unaffected each time:

| Route | Result |
|---|---|
| WebRTC with `sipInfo/v2` (our normal leg) | Refused: 14001, "RTSP Streaming in progress" |
| `get_stream_url` watch-along DASH URL | 502 from Arlo's load balancer, three client variants |
| `startUserStream` (the RTSP start pyaarlo uses) | Accepted. Its reply (not the bus) carries a watch-along DASH URL, 502 again, plus `sipCallInfo` + `iceServers` |
| WebRTC with `startUserStream`'s `sipCallInfo` | Signaling connects; the gateway answers `code 3, NO_ROUTE_DESTINATION`, empty SDP |

The coordinates `startUserStream` hands out describe a call the camera
is not in (`callId` and `conferenceId` are null): the camera streams once,
over RTSP to the app, and Arlo exposes that stream to no client we can
act as. Impersonating the mobile app to obtain its RTSP URL remains the
only untried idea and stays rejected (unknown client identity, and a step
the owner did not ask for). The decision above stands.

The probes are kept, unpushed: arlo-rs branch `probe/force-start-during-view`
(`probe_force_start_during_view`, `start_user_stream`) and streamer
branch `probe/join-user-view` (`examples/join_user_view.rs`). They also
found that current Arlo answers `startUserStream` in the POST reply, which
arlo-rs's `force_start_stream` ignored (fixed on its own branch).

