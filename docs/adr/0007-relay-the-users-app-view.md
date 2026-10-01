# ADR 0007 — Relay the user's live view from the Arlo app

- **Status:** Accepted
- **Date:** 2026-10-01
- **Deciders:** Senior architect, project owner
- **Supersedes:** 0005 for what happens on a user view (its suppression
  rules and the `CameraBusy` path stand)
- **Superseded by:** —

## Context

ADR 0005 concluded that a live view the user starts in the Arlo app
cannot be shown on the NVR: our own WebRTC call is refused (14001) and
every watch-along URL Arlo handed a browser identity answered 502. Its
2026-09-29 addendum closed a third route (`startUserStream`'s call
coordinates: `NO_ROUTE_DESTINATION`).

The owner's pyaarlo contribution (twrecked/pyaarlo#166) pointed at the
missing piece: Arlo answers the same `get` stream query with a format
**per `User-Agent`**. Asked as the iOS app (`(iPhone15,2 18_1_1) iOS
Arlo <version>`, pyaarlo's `arlo` agent) during a view, Arlo returns the
view's own `rtsps://<ip>:443/vzmodulelive/<camera>_<ts>?egressToken=…&watchalong=true`
stream. Captures on 2026-09-30:

- a raw RTSP client plays it: OPTIONS, DESCRIBE, SETUP (TCP-interleaved),
  PLAY all 200, RTP flowing; the app's view is unaffected;
- GStreamer's `rtspsrc` is refused at SETUP (403) for a reason not
  identified — not the token: SETUP without it is accepted;
- the server's certificate cannot match a raw IP, so validation must be
  off for that host;
- (2026-10-01, first gate) the server sends its periodic RTCP sender
  reports **without** the interleaved `$` framing, only the first one at
  `PLAY` is framed; the client skips bare RTCP packets by their own
  length field. This may be why `rtspsrc` fails on this server.

## Decision

**Relay the view.** While the camera is idle and the bus reports
`userStreamActive`, the daemon fetches the watch-along URL as the app
identity (`UserViewSource`, `arlo.app_version` says which app version to
identify as) and attaches it as the live source
(`MediaMultiplexer::attach_user_view`): the same idle/live splice, RTSP
clients and HLS as a motion session, with a `LiveSource::UserView` tag.

- **Media:** `rtsp_relay.rs` is a hand-written RTSP client over TLS
  (validation off, token as the access control). It forwards the video
  track's RTP into the live sinks with the payload type rewritten, sends
  `GET_PARAMETER` keep-alives and RTCP receiver reports, and reports loss
  through the shared notifier (server closed → `end-of-stream`,
  transport error → `peer-disconnected`, silence → the stall watchdog).
  **Video only**: the app's audio is AAC where the live audio path is
  Opus; the silent bed covers it.
- **Orchestrator:** `UserViewStarted` (`Idle → Activating`),
  `UserViewEnded` (`Live → Idle`, emitted only for a relay session),
  `UserViewUnavailable` (`Activating → Idle`, no backoff). A relay has
  **no cooldown** (it ends with the view, a loss, or the hard cap is not
  involved) and is **not charged** to the daily budget: the camera
  streams for the app, not for us. A motion pulse during a relay is
  absorbed; once the view ends, the next pulse is an ordinary motion
  session. A running motion session is left alone when a view starts
  (ADR 0005). A failed or lost relay arms a 30 s retry guard, since the
  view reports repeat every ~10 s; the idle frame keeps the
  `LIVE IN ARLO APP` notice meanwhile.
- **Admin snapshot:** `live_source` (`"motion"` / `"user-view"`) while
  live.

## Consequences

### Positive

- The NVR shows what the user watches in the app, through the existing
  outputs, with no extra camera load and no Arlo signaling session.
- The suppression of motion during a view (ADR 0005) is now a relay of
  the view, and `CameraBusy` keeps catching the views the bus missed.

### Negative / risks

- **Depends on an undocumented behaviour**: Arlo keying the stream
  format on the app identity. If it stops, `UserViewSource` returns a
  non-RTSP answer, the relay is reported `user-view-unavailable`, and
  the daemon falls back to the notice, as before this ADR.
- **No certificate validation** for the watch-along host; TLS still
  hides the exchange and the token is single-session.
- **No audio** from the relayed view.
- **In-band SPS/PPS**: Arlo's SDP carries `sprop-parameter-sets`; if
  the stream does not repeat them in-band, the decoder shows nothing
  until it gets them. Unverified at the time of writing; the fix would
  be to pass them through the live `appsrc` caps.
- One more arlo-rs dependency surface (`get_stream_url_as`, released as
  0.2.2; the streamer builds against the local checkout meanwhile).

## Alternatives considered

- **`rtspsrc` with a `before-send` hook**: would reuse GStreamer's RTSP
  client, but the 403 it gets is unexplained and the loop needs to own
  keep-alives and loss signals anyway.
- **Relay nothing, keep the notice only** (ADR 0005 as revised).
  Rejected by the owner once a working source was found.
- **Audio through a second AAC branch**: deferred until the video relay
  has run on the Frigate box.
