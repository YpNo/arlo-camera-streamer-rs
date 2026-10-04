# ADR 0007 — Relay the user's live view from the Arlo app

- **Status:** Accepted
- **Date:** 2026-10-01
- **Deciders:** Senior architect, project owner
- **Supersedes:** 0005 for what happens on a user view (its suppression
  rules and the `CameraBusy` path stand)
- **Superseded by:** —

## Context

ADR 0005 concluded that a live view the user starts in the Arlo app
cannot be shown on the NVR. Four routes were tried during an app view,
the app's own view unaffected each time:

| Route (2026-09-29) | Result |
|---|---|
| WebRTC with `sipInfo/v2` (our normal leg) | Refused: 14001, "RTSP Streaming in progress" |
| `get_stream_url` watch-along DASH URL, browser identity | 502 from Arlo's load balancer, three client variants |
| `startUserStream` (the RTSP start pyaarlo uses) | Accepted; its reply carries a watch-along DASH URL (502 again) plus `sipCallInfo` + `iceServers` |
| WebRTC with `startUserStream`'s `sipCallInfo` | Signaling connects; the gateway answers `code 3, NO_ROUTE_DESTINATION`, empty SDP |

The coordinates `startUserStream` hands out describe a call the camera
is not in: the camera streams once, over RTSP, to the app.

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
- the server's certificate cannot name a raw IP, so the hostname check
  cannot pass for that host;
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

- **Media:** `rtsp_relay.rs` is a hand-written RTSP client over TLS.
  The certificate chain is verified against the system roots and only
  the hostname check is waived (revised 2026-10-03 after the security
  sweep: the first version accepted any certificate, which let an
  on-path party read the egress token and substitute the feed); an
  operator can pin the end-entity certificate instead
  (`arlo.watch_along_cert_sha256`) when the chain is not public — the log
  prints the fingerprint to copy. Every text line, header count and
  `Content-Length` from the server is bounded. It forwards the video
  track's RTP into the live sinks with the payload type rewritten, sends
  `GET_PARAMETER` keep-alives and RTCP receiver reports, and reports loss
  through the shared notifier (server closed → `end-of-stream`,
  transport error → `peer-disconnected`, silence → the stall watchdog).
  **Audio too** (2026-10-02): the SDP's RFC 3640 `MPEG4-GENERIC` track
  is `SETUP` on the next channel pair and its RTP goes to a third mixer
  input (`live_aac_rtp_src` → `rtpmp4gdepay ! aacparse ! avdec_aac`),
  whose caps follow the stream's `rtpmap`/`fmtp` (`LiveAacSink`
  announces the format before the packets). Arlo pushes that track's
  RTP unframed, payload type 0, with or without a `SETUP`; a bare
  `AAC-hbr` packet carries its AU size, so the read loop relays it as
  audio instead of skipping it. A refused audio `SETUP` keeps the relay
  video-only.
- **Orchestrator:** `UserViewStarted` (`Idle → Activating`),
  `UserViewEnded` (`Live → Idle`, emitted only for a relay session),
  `UserViewUnavailable` (`Activating → Idle`, no backoff). A relay has
  **no cooldown** (it ends with the view or a loss) and is **not
  charged** to the daily budget: the camera streams for the app, not for
  us. But our RTSP session counts as a viewer for Arlo's backend
  (2026-10-01, third gate): once the app closes its view the camera
  keeps streaming for us and never reports `idle`. So the relay
  **probes**: every `user_view_probe_secs` (60 s) it lets go of the
  stream (`UserViewProbe`, `Live → Idle`, the still shows the notice)
  and listens for the camera's `idle` report for a 2 s grace — the
  report came 0.34 to 0.8 s after our `TEARDOWN` over four gates. A report means
  the app had left; none means the view goes on and the relay resumes
  through the ordinary `UserViewStarted` path, about 1.5 s later. With
  probing off (`0`) the relay is capped at `max_continuous_live`
  instead. A motion pulse during a relay is
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
- **Hostname check waived** for the watch-along host: a certificate
  that chains to a public root is accepted whatever name it carries. If
  Arlo's chain turns out not to be public, the relay refuses it until the
  operator pins the fingerprint (fail closed, one config line).
- **Audio depends on in-band framing or the AU header**: an AAC track
  with other AU-header widths than `AAC-hbr`'s is decoded only when the
  server frames it.
- **The relay prolongs the camera's streaming** by up to one probe
  interval after the app closes its view, and the NVR shows live for
  that long; the bus gives no signal of the app leaving while we hold
  the stream. Each probe costs the viewers a gap of about seven
  seconds (grace plus re-attach, about 2.5 s measured).
- **A late `idle` report** (later than the grace) makes the re-attach
  query reach a camera that has just stopped, which starts it again
  for one more segment. Not seen so far (340 ms).
- **In-band SPS/PPS**: verified on 2026-10-01 — the stream repeats
  them, VLC decodes the relayed view without caps help.
- One more arlo-rs dependency surface (`get_stream_url_as`, released as
  0.2.2 on 2026-10-02).

## Alternatives considered

- **`rtspsrc` with a `before-send` hook**: would reuse GStreamer's RTSP
  client, but the 403 it gets is unexplained and the loop needs to own
  keep-alives and loss signals anyway.
- **Relay nothing, keep the notice only** (ADR 0005 as revised).
  Rejected by the owner once a working source was found.
- **Transcoding the AAC to Opus inside the relay** to reuse the Opus
  input: a decode and an encode per relay for nothing; a third mixer
  input decodes once.
