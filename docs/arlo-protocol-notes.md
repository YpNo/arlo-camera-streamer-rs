# Arlo protocol notes

What this project learned about Arlo's cloud while building the streamer
and [`arlo-rs`](https://github.com/YpNo/arlo-rs), gathered in one place.
None of it is documented by Arlo: it comes from captures of the web
dashboard (`my.arlo.com`) and the iOS app, from
[pyaarlo](https://github.com/twrecked/pyaarlo), and from live runs against
the owner's cameras between 2026-09-25 and 2026-10-10. Arlo can change
any of it without notice; the dates say when each point was last seen to
hold.

Never paste what these exchanges carry into an issue or a log: access
tokens, cookies, per-call SIP passwords, egress tokens, presigned URLs.

## Login

- The web dashboard's login is a six-step OAuth ceremony: `login`,
  `getFactors`, `startAuth`, `finishAuth`, `validateAccessToken`,
  `session/v3`. arlo-rs mimics it request for request, headers included,
  over a client that presents Chrome's TLS fingerprint (JA4): Arlo's
  Cloudflare edge screens clients by it.
- A **trusted browser** skips the second factor: the device id the
  client presents plus its cookie jar. Keep both across restarts (the
  session cache) and a stale token is renewed without a code. A new
  device id gets code 9204 or 9261 ("Invalid factor data", 2026-10-03)
  from the trusted-browser probe: that means "not trusted yet", not a
  failure.
- Arlo's Cloudflare edge answers a burst of logins with HTTP 429, error
  1015, and each new attempt prolongs the block (2026-10-09, a crash
  loop). It lifts after a while without attempts.
- A newer login on the same account can invalidate an older session:
  the cached token is then refused with 401 and the trusted browser
  logs in again without a code (2026-10-10).

## Devices

`GET /hmsweb/v2/users/devices` lists every device. The fields the
streamer relies on:

| Field | Use |
|---|---|
| `deviceId` | The camera's id, in every call and event about it. |
| `parentId` | The base station it is paired to; the `to:` of a camera request. A re-paired camera changes it, so a cached copy goes stale. |
| `xCloudId` | The broker key of its events, and the `xcloudId` header of its calls. |
| `deviceType` | `camera`, `doorbell` and `arloq` stream; base stations, bridges and chimes do not. |
| `presignedLastImageUrl` | A short-lived HTTPS URL of the last snapshot (JPEG). |
| `allowedMqttTopics` | The broker's own grant of topics for the device, when present. |

## Event bus

- The legacy server-sent-events channel (`/hmsweb/client/subscribe`)
  answers 403 under the v3 API. Events come over **MQTT over WebSocket**
  instead: `session/v3` names the broker (`wss://mqtt-cluster-…`), and
  the access token is the MQTT password.
- Device events are published under the device's **`xCloudId`**, not its
  `deviceId`. The broad `d/<xCloudId>/out/#` filter is refused for an
  account that shares the device rather than owning it; the explicit
  per-resource topics the dashboard subscribes to (or the device's
  `allowedMqttTopics`) work for both. The user inbox is
  `u/<userId>/in/#`. A device without an `xCloudId` gets no events.
- A camera event has `resource: "cameras/<deviceId>"` and a
  `properties` object. The ones that matter here: `motionDetected`,
  `audioDetected`, `connectionState`, `batteryLevel`,
  `presignedLastImageUrl`, and `activityState`: `userStreamActive` when
  someone watches in the app, `idle` when the camera stops streaming,
  and `alertStreamActive`, `startUserStream`, `fullFrameSnapshot`,
  `startRecord`, `stopRecord`, which carry nothing a motion event did not
  already say.
- A video doorbell (Essential Video Doorbell, 2026-10-12 to 10-21) is
  its own base station and publishes on two resources: its camera
  events (`motionDetected`, `activityState`, `fullFrameSnapshotAvailable`
  with `presignedFullFrameSnapshotUrl`, `batteryLevel`) under
  `cameras/<deviceId>`, and its ring (`buttonPressed: true`, no "released"
  event) and `sipCallActive` under `doorbells/<deviceId>`.
- **A shared account never gets a video doorbell's camera events.** Its
  `allowedMqttTopics` for the doorbell hold `out/doorbells/<id>/#` and
  per-resource topics, no `out/cameras/<id>/#`, and the broker refuses
  that filter in the SUBACK. Motion went missing with the doorbell
  armed and the app notifying; the ring arrived. The owner account's
  grant is `d/<xCloudId>/out/#` per device, and the doorbell's motion,
  snapshots and ring all arrive. The cameras are unaffected. pyaarlo
  users report the same (hass-aarlo #542, #1169).
- What the shared account does get from the doorbell: `basestation/
  wakeup` and an empty `basestation/is` (both answers to the app's
  requests, addressed to the app's user), and `u/<userId>/in/library/add`
  (`mediaUploadNotification`, with the device id and a last image)
  25 to 55 s after a recording, when the account is told of one.
- The legacy SSE stream (`GET /hmsweb/client/subscribe`, the API
  headers, `Accept: text/event-stream`) answers 200 and `{"status":
  "connected"}`. In one run of six it then carried the doorbell's motion
  to the shared account and closed right after; in the other five it
  stayed silent, the owner account included. Not a usable channel.
- pyaarlo's base-station ping (`notify` with `{"action": "set",
  "resource": "subscriptions/<userId>_web", "properties": {"devices":
  [<id>]}}`, every 60 s) is answered on `out/subscriptions/…` but adds a
  second subscription: every camera event then arrives twice. It does
  not bring the doorbell's motion to a shared account.
- The `idle` report after a stream ends arrives 0.34 to 0.8 s after the
  last viewer leaves (four runs, 2026-10-01).

## Live view over WebRTC

1. `GET /hmsweb/users/devices/sipInfo/v2` (with the `xcloudId` header)
   returns the call: the SIP callee, a per-call password, and the ICE
   servers (STUN and TURN).
2. A WebSocket to `wss://<domain>:7443/` (subprotocol `sip`, `Origin:
   https://my.arlo.com`) tunnels HTTP requests: `POST
   /hmswebsocketproxy/initiateOffer` with the SDP offer answers `200 OK`
   with the SDP answer.
3. `POST /hmswebsocketproxy/sessionDisconnected` on the same socket ends
   the call. Send it on every exit, failures included: it is what tells
   the gateway the call is over, and the camera streams on battery.

What the gateway accepts:

- It is a **non-bundled FreeSWITCH**: one transport per media line, no
  `BUNDLE`. GStreamer's `webrtcbin` negotiates it; the pure-Rust
  `webrtc` stack could not (2026-09).
- The offer has an Opus audio line (`sendrecv`; the streamer sends
  silence) and an H.264 video line (`recvonly`). The answer sends H.264
  and the camera's Opus audio.
- **TURN over TCP does not work**; only UDP TURN relays carry media.
  The streamer drops the TCP entries from the ICE server list.
- While the user watches the camera in the app, `sipInfo/v2` is refused
  with error **14001**, "RTSP Streaming in progress": the camera streams
  to one viewer at a time.

## Watching along with the app

Ways to see what the user watches in the app, tried during a view
(2026-09-29 and 30; ADR 0007 has the details):

| Route | Result |
|---|---|
| WebRTC with `sipInfo/v2` | 14001 |
| The stream query, browser identity | A DASH URL that answers 502 |
| `startUserStream` (pyaarlo's RTSP start) | Accepted: a DASH URL (502) plus SIP coordinates whose call the gateway answers `NO_ROUTE_DESTINATION` |
| The stream query, **iOS app identity** | The view's own `rtsps://<ip>:443/vzmodulelive/…?egressToken=…&watchalong=true` stream: it plays |

Arlo keys the stream format on the **User-Agent**: the same query asked
as the iOS app (`(iPhone15,2 18_1_1) iOS Arlo <version>`) during a view
returns an RTSPS stream any client can join without disturbing the app.

About that RTSPS server:

- It is addressed by IP. Its certificate chains to a public root
  (DigiCert Global G2 TLS RSA SHA256 2020 CA1) and names only
  `*.wowza.arlo.com` (subject Arlo Technologies, Inc.; seen 2026-10-12,
  valid to 2027-01-03), not the IP. The relay requires a name under
  `arlo.com` instead.
- A plain RTSP client plays it (OPTIONS, DESCRIBE, SETUP over
  TCP-interleaved, PLAY). GStreamer's `rtspsrc` is refused at SETUP with
  403, for a reason not identified; the token is not it.
- It sends packets **without** the `$` interleaved framing: every RTCP
  sender report after the first, each followed by a bare 12-byte RTP
  header (payload type 0, no payload, apparently a keep-alive), and the
  AAC audio track's RTP (payload type 0, AU-size header), whether or not
  that track was set up. A client has to resynchronise on the next
  plausible `$` header.
- The audio track is RFC 3640 `MPEG4-GENERIC` (`AAC-hbr`); the streamer
  saw 16 kHz mono. The video repeats SPS and PPS in band (2026-10-01).
- **Our session counts as a viewer.** Once the app closes its view the
  camera keeps streaming for us and never reports `idle`. The streamer
  lets go of the stream every 60 s and listens for the `idle` report to
  learn whether the app is still there.

## Snapshots

- `presignedLastImageUrl` (device list and bus) points at a short-lived
  HTTPS URL on an Arlo storage host (`arlolastimage-…arlo.com`); the
  path and query are the credential. The device list's URL is refreshed
  on each call; the bus announces a new one after a recording.
- The JPEG's header can declare any frame size: decode only after
  checking it (a 2.6 KB file declaring 12000×12000 took about 1 GB to
  decode).

## Credits

pyaarlo (twrecked) documented the app identity, the activity states and
much of the REST surface this work started from; the watch-along
discovery came out of the owner's contribution there
([twrecked/pyaarlo#166](https://github.com/twrecked/pyaarlo/issues/166)).
