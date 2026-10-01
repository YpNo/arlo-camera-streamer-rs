# Session Handoff — arlo-camera-streamer-rs

> **For the next AI assistant.** Point-in-time snapshot of the project, the
> open design work and the agreed plan. Verify against `git log`, the ADRs
> and the code before acting on any claim below.

---

## 1. What this repo is

A Rust daemon that bridges battery-powered Arlo cameras to a 24/7 NVR
(Frigate, VLC, go2rtc) through a local RTSP server **without draining the
camera battery**. It decouples the NVR's continuous pull from Arlo's
event-driven push:

- **Idle**: serves a locally generated 1 fps still (last thumbnail over a
  synthetic STANDBY frame).
- **Motion event on the MQTT bus** → **Activating** → WebRTC offer/answer
  with Arlo's `FreeSWITCH` gateway → the live H.264 is spliced into the
  RTSP output.
- **Cooldown / Battery-protect**: revert to the idle still.

The splice runs inside **one persistent RTSP pipeline per camera** with an
`input-selector` on raw video and an `audiomixer` on audio, so connected
clients never disconnect ([ADR 0003](./0003-seamless-input-selector-splice.md)).

---

## 2. Layout — 6 crates, hexagonal

| Crate | Role |
|---|---|
| `streamer-domain` | Pure types + port traits. No I/O. |
| `streamer-app` | Orchestrator, state machine, budget/debouncer, `AdminControlActor`, `NoopRecorder`. |
| `streamer-infra-arlo` | `arlo-rs` adapter (MQTT event bus, WebRTC signaling, thumbnails, device registry). |
| `streamer-infra-media` | GStreamer: persistent pipeline, `webrtcbin`, `input-selector`, RTSP server. |
| `streamer-infra-ops` | axum HTTP: `/metrics`, `/healthz`, `/readyz`, `/admin/*`. Prometheus recorder. |
| `streamer-bin` | Composition root — `main.rs`. |

### Ports (`streamer-domain/src/port.rs`) — the source of truth

| Trait | Direction | Impl |
|---|---|---|
| `ArloEventSource` | driven | `ArloEventSourceAdapter` |
| `WebrtcSignaler` | driven | `ArloWebrtcSignalerAdapter` — one `negotiate(camera, &mut dyn OfferBuilder)` per attempt; the media side builds the offer from the ICE servers it is handed |
| `ArloThumbnailSource` | driven | `ArloThumbnailSourceAdapter` |
| `MediaMultiplexer` | driven | `GstMediaMultiplexer` |
| `MetricsRecorder` | driven | `Metrics` (infra-ops) / `NoopRecorder` (app) |
| `AdminControl` | **driving** | `AdminControlActor` — called by axum |

There is **no URL-based stream port** any more: Arlo v3 live is WebRTC, each
viewer negotiates its own SDP session, nothing returns a URL. The event bus is
**MQTT over WebSocket** (the legacy SSE endpoint answers 403); the wire shape
of each event is unchanged, which is why `event_mapper.rs` still works.

### Dependencies worth knowing

- `arlo-rs` **0.2.1 from crates.io** (published 2026-09-26, sibling checkout
  at `../arlo-rs`, MSRV 1.98.1, browser-less `wreq` transport). A commented
  `[patch.crates-io]` block at the end of `Cargo.toml` is the local override.
- Toolchain **1.98.1** everywhere (`rust-toolchain.toml`, `mise.toml`, CI,
  Dockerfile, workspace `rust-version`) — it follows arlo-rs.

### ADRs

- `0001-factory-restart-splice` — superseded by 0003.
- `0002-rtsp-only-output-v1` — superseded by 0006 for HLS.
- `0003-seamless-input-selector-splice` — accepted; production splice.
- `0004-live-lost-feedback` — accepted; `LiveSession` handle + `LiveLost` transition.
- `0005-manual-stream-piggyback` — accepted (revised): observe user views, never compete. Re-examined 2026-09-29: all four routes to the app's stream fail (14001, 502, 502, `NO_ROUTE_DESTINATION`); it cannot be relayed.
- `0006-hls-output-via-loopback-segmenter` — accepted; HLS from a loopback RTSP client of each camera, no re-encode; DASH unsupported (stock dashsink writes only TS, never prunes).
- `0007-relay-the-users-app-view` — accepted 2026-10-01; the app's view relayed from the RTSPS stream Arlo hands the iOS-app identity (`rtsp_relay.rs`, own RTSP client, TLS validation off). Needs arlo-rs `get_stream_url_as` (0.2.2; local `[patch]` meanwhile). Not yet run on the Frigate box.

---

## 3. What ships (by phase)

Phases 1–8c are the original build-out; see `git log --oneline` for the
commits. Phase 9 is this session's work.

| Phase | Delivered |
|---|---|
| 1 | Tokio + tracing + config parser + Arlo auth (email / push / SMS MFA). |
| 2 | Event bus + mapping to domain `CameraEvent`. |
| 3 | Per-camera state machine, debouncer, daily budget, backoff. |
| 4 | GStreamer media adapter: embedded gst-rtsp-server, factory registry. |
| 5 | Composition root + ops HTTP, readiness, metrics, idle thumbnail from Arlo. |
| 6 | Per-camera `MetricsRecorder`, `AdminControl` + `/admin/*`, Dockerfile, README, ADRs 0001–0002. |
| 7 | V3 pivot: WebRTC ingest via `webrtcbin`, `WebrtcSignaler` port, ICE from `sipInfo`, `[webrtc]` config. |
| 8 / 8b / 8c | Seamless `input-selector` splice (ADR 0003), live audio via `audiomixer`, `x264` / `vaapi` encoder choice. |
| 9 | Follow arlo-rs 0.2.0: rename, browser-less transport, `SecretString`; toolchain 1.98.1; crates.io dependency; Dockerfile build deps + working `HEALTHCHECK`; example config fixed and covered by a test; SSE/Chromium wording purged; `CHANGELOG.md`; `CONTRIBUTING.md` rewritten for this repo. |

### Operational surface

- `rtsp://<host>:8554/<stream_name>` — what Frigate/VLC consume.
- `:9090` — `GET /metrics`, `GET /healthz`, `GET /readyz` (ready iff started + bus connected).
- `:9091` — `GET /admin/state`, `GET /admin/cameras/{id}`,
  `POST /admin/cameras/{id}/wake`, `POST /admin/cameras/{id}/idle`
  (bearer auth via `STREAMER_ADMIN_TOKEN`).

---

## 4. Agreed plan (2026-09-27) and where we are

Order agreed with the user; items 1–5 are done.

1. **Unblock CI** — done (toolchain, crates.io dep, image deps, no
   `-A dead_code`). PR `feat/init-v1` → `main` (#2) runs all 8 checks
   green, media crate included.
2. **Docs/config hygiene** — done (see Phase 9).
3. **`LiveLost` feedback path + ADR 0004** — done (2026-09-27). `attach_live`
   returns a `LiveSession`; the orchestrator holds it while `Live`
   and awaits it in `select!`; `StateTransition::LiveLost(reason)` maps to
   `Idle`; detectors: stall watchdog (`webrtc.live_stall_timeout_secs`),
   bus ERROR/EOS, connection-state failed/closed. Pure logic in
   `streamer-infra-media/src/live_watch.rs`; the webrtcbin wiring is blind
   until CI / the Frigate box runs it (see ADR §Consequences). A loss
   during setup fails the attach with its reason (`setup_or_loss`,
   2026-09-28) instead of the 20 s first-RTP timeout.
4. **Two event contexts** — done 2026-09-28 (ADR 0005, revised). The
   piggy-back idea was built, then refuted by three live captures: Arlo
   refuses our WebRTC leg while the app streams (error 14001) and answers
   502 for the watch-along DASH URL. Owner's decision: observe only.
   `ManualStream` / `ManualStreamEnded` drive a `user_view_until` flag
   (120 s hold, refreshed by reports); while set, motion does not start a
   session (`suppressed-user-view`), a running session is left alone, and
   a refused attach (`CameraBusy`, mapped from 14001) returns to idle
   without backoff. arlo-rs branch `feat/stream-peek-probe` carries the
   probe, the `data.error` envelope fix and the `mediaUploadNotification`
   decode fix; released as 0.2.1 and adopted 2026-09-28 (message-text fallback removed). `mediaUploadNotification` carries
   a fresh `presignedLastImageUrl` (top-level, not kept by arlo-rs's
   `ArloEvent`); the idle still instead follows the `cameras/<id>`
   `presignedLastImageUrl` property event (done 2026-09-28,
   `snapshot_cache.rs`).
5. **`list-devices` CLI subcommand** — done 2026-09-28. `streamer-bin/src/list_devices.rs`
   (pure rendering, tested) on top of `streamer_infra_arlo::discover_devices`
   and `StreamName::suggest`. Shares the daemon's session cache, never logs
   out. Logs now go to stderr.

Optional cleanups noted during the review: `Cooling` was declared but
never produced; removed 2026-09-28, along with `Live::since_secs`
(never updated). A capture the same day settled the cooldown question:
Arlo repeats a motion pulse (`true`, then `false` ~5 s later) about
every 10 s while motion lasts, so the existing "`debounce_secs` after the
last pulse" rule already gives one long capture plus a cooldown; `false`
ends a pulse, not the motion. The hard cap stays at 300 s (owner's call). Pulse logging validated live the same day: the session ended exactly `debounce_secs` after the last pulse. The two-step `ice_servers` + `negotiate` protocol and its
per-camera `SipInfo` cache were folded into one `negotiate` call on
2026-09-28.

---

## 5. Known gaps

| Item | Notes |
|---|---|
| Coverage gate | 86 % (measured 88.25 % on 2026-09-28, GStreamer files counted since the integration tests). Still excluded: `streamer-bin` and the infra-arlo network wrappers (`boot`, `events`, `stream_requester`, `thumbnails`). |
| App-view relay | Built 2026-10-01 (ADR 0007), integration-tested against the crate's RTSP server; live gate pending: first VLC check of a relayed view, and whether Arlo's H.264 carries SPS/PPS in-band (else pass `sprop-parameter-sets` through the appsrc caps). Audio not relayed (AAC vs Opus path). `[patch.crates-io]` on until arlo-rs 0.2.2. |
| HLS / DASH | HLS wired 2026-09-28 (ADR 0006, `hls.rs`), integration-tested (live picture in a segment, retention, cleanup); not yet run on the Frigate box. DASH unsupported: parsed, warned, ignored. |
| Integration test vs a real gateway | `tests/live_session.rs` (2026-09-28) replaces the planned recorded-session fixture: DTLS keys are per call, so a recording cannot be replayed; a local `webrtcbin` answers instead. It proves negotiation, the splice seen by a client, mid-session join, stall and setup loss. Arlo-specific quirks (TURN, SDP shape drift) still need the Frigate box. |
| Live session vs RTSP client lifecycle | **Fixed 2026-09-28.** The pumps push into the *current* media's appsrc (slot set at `media-configure`, cleared at `unprepared`), discard while none exists, and a media built during a live session gets its switch armed. Validated live 2026-09-28: VLC disconnected and reconnected mid-session and got the live video back ("media built during a live session; live switch armed"). |
| Dependency currency | `mockall` 0.15 and `rstest` 0.27 adopted 2026-09-28; everything else within one minor of latest on 2026-09-27. |

---

## 6. GStreamer / pipeline gotchas — hard-won lessons

Verified in production; do not rediscover:

- **`audiomixer`, not a second `input-selector`, for audio.** A second
  selector stalls `gst-rtsp-server`'s media-prepare. Working since `b4c5553`.
- **`avenc_aac` requires `F32LE`.** `S16LE` yields "not-negotiated".
- **Never `bus.add_watch_local()` inside `media-configure`** ("no default
  main context"). Use `bus.set_sync_handler` for diagnostics.
- **You cannot fully validate `gst-rtsp-server` prepare on macOS.** Pipeline
  changes need Linux: since 2026-09-27 the `rust-build` distrobox has GStreamer
  1.26 dev packages, so the media crate builds and unit-tests locally; the
  pipeline itself still needs a camera.
- **`gupnp … 1900: Address already in use` at live start is harmless.**
- **`Sticky event misordering, got 'segment' before 'caps'` is harmless.**
  gst-rtsp-server logs it (a GLib warning, one per pad) when a UDP client
  joins a media already playing, e.g. VLC after the HLS segmenter.
- **VAAPI needs `/dev/dri`** in Docker (`--device /dev/dri`).
- **Arlo's TCP TURN is unusable**; the signaler adapter drops it and keeps
  UDP only (proven live).

---

## 7. Parked ideas

1. A `.claude/skills/arlo-otp/` skill codifying the MFA ceremony.
2. A reverse-engineering write-up: SDP shape, `sessionDisconnected` quirks,
   TURN UDP-only observation.
3. Auto-populate `[[cameras]]` from the device inventory when the section is
   empty (follow-up to item 5 above).

---

## 8. How to build / test / run

```bash
# On this workstation every cargo command runs inside the rust-build
# distrobox (libclang for BoringSSL, GStreamer 1.26 dev headers), through
# mise for the pinned toolchain. The whole workspace builds there:
distrobox enter rust-build
export LIBCLANG_PATH=/usr/lib/llvm-19/lib
mise exec -- cargo fmt --all -- --check
mise exec -- cargo clippy --workspace --all-targets --all-features -- -D warnings
mise exec -- cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" mise exec -- cargo doc --workspace --no-deps --all-features
mise exec -- cargo build --release -p arlo-camera-streamer
cargo audit && cargo deny check          # fine on the host

# Container image
podman build -t arlo-camera-streamer:local .

# Run
export ARLO_PASSWORD="…" ARLO_IMAP_PASSWORD="…"
export STREAMER_ADMIN_TOKEN="$(openssl rand -hex 32)"
export RUST_LOG=info,arlo_camera_streamer=debug
./target/release/arlo-camera-streamer --config config/streamer.example.toml
```

Runtime deps: GStreamer 1.22+ with base/good/bad/ugly/libav/rtsp/nice
(optional vaapi). No browser. Live-validated 2026-09-27 on GStreamer 1.26
(Debian trixie): motion → live → cooldown, and the ADR 0004 stall path
(`live-lost-rtp-stalled` 4 s after the last packet).

---

## 9. Provenance

Rewritten 2026-09-27 by Claude Fable 5.1 on branch `feat/init-v1` after
the arlo-rs 0.2.0 follow-up commits. The previous version (Claude Opus 4.7,
state at `cf84ad4`) predated the rename, the browser-less transport and the
MQTT bus, and its §4 design assumed a URL-returning stream API that no
longer exists.
