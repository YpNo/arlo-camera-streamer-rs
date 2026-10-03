---
name: live-validation
description: Manual live gate against a real Arlo camera — build the release binary in the rust-build distrobox, run it, watch with VLC, and read the log lines that prove each path (motion session, stall, user view, reconnect, idle snapshot). Use after any change to the GStreamer-bound files or to the event/orchestration paths, and when asking the owner for a live check.
---
# Live Validation Skill

The GStreamer-bound files (`webrtc_pipeline.rs`, `gst_pipeline.rs`, `rtsp.rs`) are excluded
from coverage, so only a live run proves them. The owner runs the camera; the agent
prepares the exact commands and says which log lines to look for.

## Build and run

The tool shell cannot run the camera session; hand these to the owner.

```bash
distrobox enter rust-build -- bash -lc 'cd ~/workspace/arlo-camera-streamer/arlo-camera-streamer-rs && LIBCLANG_PATH=/usr/lib/llvm-19/lib mise exec -- cargo build --release -p arlo-camera-streamer'
```

```bash
RUST_LOG=info,arlo_camera_streamer=debug,streamer_infra_media=debug,streamer_infra_arlo=debug ./target/release/arlo-camera-streamer --config config/streamer.toml
```

Credentials come from the environment (`ARLO_PASSWORD`, `ARLO_IMAP_PASSWORD`,
`STREAMER_ADMIN_TOKEN`). Logs go to stderr. `list-devices` (same `--config`) signs in,
completes the MFA pairing and prints `[[cameras]]` snippets without starting servers.

Watch with VLC: `rtsp://<host>:8554/<stream_name>` (bind from `[output.rtsp]`).
Manual wake without motion: `POST /admin/cameras/<id>/wake` on `admin_bind` with the
bearer token.

**Stale binary check first.** Before reading a surprising log, confirm the binary is
the one just built: compare its mtime with the last build, or grep it for a string the
change introduced (`strings target/release/arlo-camera-streamer | grep '<new log text>'`).
A stale binary has already cost a validation round.

## What each path must log

| Path | Expected lines, in order |
|---|---|
| Boot / encoder | `video encoder (auto) encoder=<name>` (or `video encoder encoder=<name>` for an explicit one) before the RTSP server starts; `encoder skipped` at debug for each backend auto rejected. A wrong explicit name ends the boot with `is not usable on this host: …` |
| Boot / login | `arlo-rs session restored from cache` (silent) or `no valid cached session — running MFA cold-start` → `Trusted browser accepted by Arlo — no OTP required` / the factor's lines → `arlo-rs authentication complete`. Anything asking for a code on a routine restart is a bug or a lost cache: see the `arlo-mfa-login` skill |
| Motion pulses (`streamer_infra_arlo::events=debug`, `streamer_app=debug`) | `camera trigger pulse` per `true` (about every 10 s while motion lasts); `trigger pulse … outcome=absorbed session_ends_in_ms=…` shows the cooldown restarting |
| Motion session | `state transition … to=Activating` → `webrtcbin offer ready` → `answer applied; awaiting first RTP` → `live webrtcbin ready; first RTP flowing` → `to=Live` → (cooldown) `to=Idle` → `webrtcbin bus watch exited` |
| Stall (kill the source, e.g. cut the camera's network) | `no inbound video RTP; live source stalled` about `live_stall_timeout_secs` after the last packet → `signal=LiveLost(RtpStalled)` → `to=Idle` |
| Loss during setup (block outbound UDP to Arlo TURN) | `attach_live failed … live source lost during setup: peer-disconnected` quickly, not after 20 s |
| App view relayed (ADR 0007) | `signal=UserViewStarted` → `watch-along stream obtained` (debug) → `watch-along stream playing; awaiting first video RTP` → `relaying the user's live view from the Arlo app` → `to=Live`; VLC shows the app's picture and plays its sound (`audio track set up` or, on Arlo's server, `bare AAC audio packet (no interleaved framing) relayed`). Every `user_view_probe_secs` (60 s): `releasing the relayed view to learn whether the app still views` → `signal=UserViewProbe` → `to=Idle` (still with the notice for ~2 s) → `no idle report after the probe; the app still views, relaying again` → `signal=UserViewStarted` → `to=Live`. Closing the app: **nothing** until the next probe (our session keeps the camera streaming; the bus stays silent), then after the probe's release `user view in the Arlo app ended … after_probe=true` and no re-attach. With `user_view_probe_secs = 0`: `relay of the user's view reached the hard cap` → `signal=MaxLiveExceeded` → `to=Idle` at `max_continuous_live`. A view closed *before* the relay attached: `signal=UserViewEnded` → `to=Idle`. A relay that fails: `user view not relayed; idle frame keeps the notice` → `signal=UserViewUnavailable`. Framing check: `video track set up transport=…` (channels the server assigned) and up to four `interleaved frame channel=… len=… rtp=v2 pt=… seq=…` lines; `bare RTCP packet (no interleaved framing) accepted` and `unframed bytes skipped; frame boundary found again skipped=12` are normal (Arlo's server); a `watch-along relay ended why=unexpected byte …` line carries the hex of the bytes the parser could not place — paste it as it is, it holds no secret |
| User view the daemon cannot relay | VLC's idle frame reads `LIVE IN ARLO APP`; `user is watching in the Arlo app; motion activations paused`; motion then logs `not activating` (metric `suppressed-user-view`); `user view … ended; motion activations resumed` |
| View the bus did not report | `camera busy with a user view in the Arlo app; not activating` (Arlo 14001), back to `Idle` without backoff |
| VLC reconnect mid-session | one `live push refused` per pump, then `media built during a live session; live switch armed`, live video within ~1 keyframe interval |
| Idle still | `idle snapshot fetched source="bus"` (or `"device-list"` fallback) → `thumbnail applied to idle overlay` |

Harmless: `gupnp … 1900: Address already in use` at live start, and
`Sticky event misordering, got 'segment' before 'caps'` when a UDP client (VLC) joins a
media already playing (for instance after the HLS segmenter).

## Rules for live probes

- Never poll `get_stream_url` (action `get`): it wakes idle cameras. Probes must be
  event-driven.
- Never log or paste presigned URLs, egress tokens, cookies or credentials. Event capture
  (`RUST_LOG=streamer_infra_arlo::events=debug`) logs property **keys** only.
- Record each validated path in `docs/adr/HANDOFF.md` and `.agent/JOURNAL.md` with the
  date and the log evidence; say plainly which paths were only unit-tested.
