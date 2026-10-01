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
- (2026-10-01, first gates) the server sends some packets **without**
  the interleaved `$` framing: its periodic RTCP sender reports (only
  the first one at `PLAY` is framed), each followed by a bare 12-byte
  RTP header (payload type 0, no payload — a keep-alive, presumably).
  The client skips bare RTCP by its own length field and resynchronises
  on the next `$` header that checks out (our channel, a sane length, a
  version-2 packet with the stream's payload type) for anything else.
  This may be why `rtspsrc` fails on this server.

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
  Opus; the silent bed covers it. (The audio *is* on the wire: Arlo
  pushes the AAC track's RTP unframed, payload type 0, without any
  `SETUP` for it — a later audio relay can start from there.)
- **Orchestrator:** `UserViewStarted` (`Idle → Activating`),
  `UserViewEnded` (`Live → Idle`, emitted only for a relay session),
  `UserViewUnavailable` (`Activating → Idle`, no backoff). A relay has
  **no cooldown** (it ends with the view or a loss) and is **not
  charged** to the daily budget: the camera streams for the app, not for
  us. It **is capped** at `max_continuous_live`, though (2026-10-01,
  third gate): our RTSP session counts as a viewer for Arlo's backend,
  so once the app closes its view the camera keeps streaming for us and
  never reports `idle`. The cap bounds that to the same five minutes a
  motion session gets; the camera idles when we let go, and a longer
  view in the app relays again at its next `startUserStream`. A motion pulse during a relay is
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
  outputs, with no Arlo signaling session of ours.
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
- **The relay prolongs the camera's streaming**: after the app closes
  its view, the camera streams for the daemon until the cap (up to
  `max_continuous_live`), and the NVR keeps showing live for that long.
  The bus gives no signal of the app leaving while we hold the stream;
  a shorter, relay-specific cap or a teardown-and-listen probe are the
  options if that is too long.
- **In-band SPS/PPS**: verified on 2026-10-01 — the stream repeats
  them, VLC decodes the relayed view without caps help.
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
