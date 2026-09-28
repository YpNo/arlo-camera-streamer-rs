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
- **Cooling / Battery-protect**: revert to the idle still.

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
| `WebrtcSignaler` | driven | `ArloWebrtcSignalerAdapter` |
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
- `0002-rtsp-only-output-v1` — accepted; HLS/DASH parsed but not wired.
- `0003-seamless-input-selector-splice` — accepted; production splice.
- `0004-live-lost-feedback` — accepted; `LiveSession` handle + `LiveLost` transition.
- `0005-manual-stream-piggyback` — accepted; user live view → free piggy-backed session, wire signal to confirm by capture.

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
   `-A dead_code`). **Open the PR `feat/init-v1` → `main`**: GitHub Actions
   has never run on this repository (it triggers on `main` and PRs only), so
   the coverage gate, the media-crate build and the doc job are unproven.
2. **Docs/config hygiene** — done (see Phase 9).
3. **`LiveLost` feedback path + ADR 0004** — done (2026-09-27). `attach_live`
   returns a `LiveSession`; the orchestrator holds it while `Live|Cooling`
   and awaits it in `select!`; `StateTransition::LiveLost(reason)` maps to
   `Idle`; detectors: stall watchdog (`webrtc.live_stall_timeout_secs`),
   bus ERROR/EOS, connection-state failed/closed. Pure logic in
   `streamer-infra-media/src/live_watch.rs`; the webrtcbin wiring is blind
   until CI / the Frigate box runs it (see ADR §Consequences). Follow-up:
   race the first-RTP wait against an early loss so an attach that dies
   during ICE fails with the real reason instead of the 20 s timeout.
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
   a fresh `presignedLastImageUrl` usable for idle thumbnails.
5. **`list-devices` CLI subcommand** — done 2026-09-28. `streamer-bin/src/list_devices.rs`
   (pure rendering, tested) on top of `streamer_infra_arlo::discover_devices`
   and `StreamName::suggest`. Shares the daemon's session cache, never logs
   out. Logs now go to stderr.

Optional cleanups noted during the review: `Cooling` is declared but never
produced by `transition()` (either produce it in the debounce window or
drop it); the two-step `ice_servers` + `negotiate` protocol hides a
per-camera cache in the adapter — fold the coordinates into one call if
item 3 touches the port anyway.

---

## 5. Known gaps

| Item | Notes |
|---|---|
| CI has never run | Open the PR. First run: tests + clippy green on ubuntu-latest (media crate compiles there); doc links, coverage exclusions and Sonar config fixed after it. All 8 checks green on the third run; gate raised to the measured 84 %. |
| HLS / DASH sinks not wired | ADR 0002; config accepts and warns. |
| No recorded-session integration test | Would need a canned SDP offer/answer + RTP fixture. |
| Live session vs RTSP client lifecycle | **Fixed 2026-09-28.** The pumps push into the *current* media's appsrc (slot set at `media-configure`, cleared at `unprepared`), discard while none exists, and a media built during a live session gets its switch armed. Validated live 2026-09-28: VLC disconnected and reconnected mid-session and got the live video back ("media built during a live session; live switch armed"). |
| Dependency currency | All within one minor of latest on 2026-09-27; `mockall` 0.15 / `rstest` 0.27 are the only minor bumps pending (dev-deps). |

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
# distrobox (libclang for BoringSSL). Only the non-media crates build there:
distrobox enter rust-build
export LIBCLANG_PATH=/usr/lib/llvm-19/lib
P="-p streamer-domain -p streamer-app -p streamer-infra-arlo -p streamer-infra-ops"
cargo fmt --all -- --check
cargo clippy $P --all-targets --all-features -- -D warnings
cargo test $P --all-features
RUSTDOCFLAGS="-D warnings" cargo doc $P --no-deps --all-features
cargo audit && cargo deny check          # fine on the host

# Full workspace (needs GStreamer dev headers): CI, or the Docker image
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
