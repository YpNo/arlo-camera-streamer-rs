# arlo-camera-streamer

A Rust daemon that bridges battery-powered Arlo cameras to a 24/7 NVR
(Frigate, ZoneMinder, Shinobi, Home Assistant) via RTSP — without
draining the camera battery.

## Overview

Frigate expects a continuous RTSP stream so it can run motion detection
and recording. Arlo battery cameras are sleepy by design: a continuous
pull would flatten the battery in under 24 hours.

`arlo-camera-streamer` solves the contradiction by **decoupling
Frigate's pull from Arlo's push**:

| Phase | Frigate sees | Camera state |
|-------|--------------|--------------|
| Idle  | Looped JPEG / synthetic frame at 1 fps | Sleeping |
| Activating | Frame freezes briefly | `start_stream` in flight |
| Live  | Real H.264/H.265 from the camera, until `debounce_secs` after the last motion | Awake |
| Battery-protect | Idle frame returns | Sleeping (quota exhausted) |
| Viewed in app | You watch the camera in the Arlo app: the daemon relays the app's own stream, picture and sound (ADR 0007); if Arlo hands out none, the idle frame reads `LIVE IN ARLO APP`. Motion resumes when you close the app | Awake (because of you) |

The transition is driven by Arlo's **MQTT event bus**: when the camera
fires a motion event, the daemon negotiates a WebRTC session with Arlo's
gateway, splices the live H.264 into the persistent GStreamer pipeline,
and reverts to idle once the camera goes quiet, or as soon as the live
source itself dies (ADR 0004).

## Architecture

Hexagonal Rust workspace, six crates:

```
streamer-domain        — pure types & port traits (no I/O)
streamer-app           — orchestrator + state machine + budget tracker
streamer-infra-arlo    — adapter for the arlo-rs client
streamer-infra-media   — GStreamer pipelines + embedded RTSP server
streamer-infra-ops     — /metrics, /healthz, /readyz, /admin/*
streamer-bin           — composition root (the daemon binary)
```

Read [`crates/streamer-domain/src/port.rs`](./crates/streamer-domain/src/port.rs)
to see the contracts. The ADRs document the load-bearing decisions:

- [ADR index](./docs/adr/README.md) and the [validation record](./docs/VALIDATION.md) of what has run against a real camera.
- [ADR 0001 — Factory-restart splice](./docs/adr/0001-factory-restart-splice.md): *superseded by 0003*.
- [ADR 0002 — RTSP-only output for v1](./docs/adr/0002-rtsp-only-output-v1.md): *superseded by 0006 for HLS*; DASH stays out.
- [ADR 0003 — Seamless input-selector splice](./docs/adr/0003-seamless-input-selector-splice.md): one persistent pipeline per camera, idle and live swapped at a frame boundary, clients never disconnect.
- [ADR 0004 — Live-loss feedback](./docs/adr/0004-live-lost-feedback.md): a dead live source returns the camera to idle through the state machine, with its reason.
- [ADR 0005 — User views: observe, never compete](./docs/adr/0005-user-views-observe-never-compete.md): no WebRTC session of ours while the app views; Arlo's 14001 is handled without backoff.
- [ADR 0006 — HLS output](./docs/adr/0006-hls-output-via-loopback-segmenter.md): HLS is written by a loopback RTSP client of each camera, no re-encode.
- [ADR 0007 — Relay the user's app view](./docs/adr/0007-relay-the-users-app-view.md): a live view started in the Arlo app is relayed, picture and sound, from the RTSPS stream Arlo hands the app identity; no cooldown, no budget charge; the relay lets go of the stream every `user_view_probe_secs` so the camera can report whether the app still views.
- [ADR 0008 — Choosing the H.264 encoder per host](./docs/adr/0008-video-encoder-selection.md): `video_encoder = "auto"` dry-runs NVIDIA, Intel/AMD, V4L2 and x264 at boot and keeps the first that works.

## Prerequisites

- **Rust** ≥ 1.98.1 (`rustup toolchain install 1.98.1`; the MSRV follows arlo-rs).
- **GStreamer 1.22+** with the plugin set below.
  - `gstreamer1.0-plugins-base`
  - `gstreamer1.0-plugins-good` (also provides `gdkpixbufoverlay` for the idle thumbnail)
  - `gstreamer1.0-plugins-bad` (`webrtcbin`, DTLS/SRTP)
  - `gstreamer1.0-plugins-ugly`
  - `gstreamer1.0-libav` (`avdec_h264`, `avenc_aac`)
  - **`gstreamer1.0-nice`** — libnice ICE for `webrtcbin`. **Required for live streaming.** Without it, live fails at motion with `pipeline error: webrtcbin has no sink request pad` (the idle stream still works, which makes it easy to miss).
  - the `gst-rtsp-server` library (Debian: `libgstrtspserver-1.0-0`).
  - *Optional* GPU encoding plugins — Intel/AMD: `gstreamer1.0-vaapi` plus the VA drivers (`intel-media-va-driver` / `mesa-va-drivers`; the newer `va` plugin ships in `gstreamer1.0-plugins-bad`); Raspberry Pi 4 family: `v4l2h264enc` from `gstreamer1.0-plugins-good`; NVIDIA: `nvh264enc` from `gstreamer1.0-plugins-bad` with the driver libraries. `video_encoder = "auto"` picks whatever works (see [Performance](#performance), [ADR 0008](./docs/adr/0008-video-encoder-selection.md)).
- An Arlo cloud account with at least one camera.
- An IMAP mailbox you can poll for the Arlo MFA OTP, or be ready to
  type the OTP on stdin (cold-start only).

### Runtime dependencies — quick install

**Debian/Ubuntu:**

```bash
sudo apt-get update && sudo apt-get install -y \
  gstreamer1.0-plugins-base gstreamer1.0-plugins-good \
  gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly \
  gstreamer1.0-libav gstreamer1.0-nice \
  libgstrtspserver-1.0-0 gstreamer1.0-tools
# optional — Intel/AMD hardware H.264 encode:
sudo apt-get install -y gstreamer1.0-vaapi
```

**macOS (Homebrew):** the `gstreamer` formula bundles every plugin
above (including `gst-rtsp-server` and libnice) in one package.

```bash
brew install gstreamer
```

Sanity-check the WebRTC transport plugins are present (these are the
ones most often missing on a fresh box):

```bash
for e in webrtcbin nicesrc dtlssrtpenc srtpenc; do \
  printf '%-12s ' "$e"; gst-inspect-1.0 "$e" >/dev/null 2>&1 && echo OK || echo MISSING; done
```

## Installation

### From source

```bash
cargo build --release --package arlo-camera-streamer
sudo install -m 0755 target/release/arlo-camera-streamer /usr/local/bin/
```

### Docker (recommended)

Every release publishes an image to GitHub's registry, built from the
`Dockerfile` at the repo root: non-root, `tini` as PID 1, every GStreamer
plugin the pipelines need, scanned with Trivy before it is tagged.

```bash
docker pull ghcr.io/ypno/arlo-camera-streamer-rs:v0.1.0
```

Tags: `v<version>` (pin this one), `latest` (the newest release), and
`sha-<commit>`. `linux/amd64` is always built; `linux/arm64` (Raspberry
Pi) is added when the repository variable `IMAGE_PLATFORMS` lists it.
To build it yourself instead:

```bash
docker build -t arlo-camera-streamer:dev .
```

## Configuration

Copy [`config/streamer.example.toml`](./config/streamer.example.toml)
to `/etc/arlo-streamer/streamer.toml` (or another preferred location) and edit the marked sections.
**Secrets never live in this file** — only the *names* of env vars
holding the values. The file is checked before anything starts: an
unknown key (a typo) is an error, two cameras may not share an
`arlo_device_id` or a `stream_name`, and the cooldown values must be in
range (`debounce_secs` and `max_continuous_live` 1 to 86400 s; the
budget and probe 0 to 86400 s).

| Section / key                       | Type           | Default              | Notes                                                    |
|-------------------------------------|----------------|----------------------|----------------------------------------------------------|
| `arlo.email`                        | string         | (required)           | Arlo cloud account email.                                |
| `arlo.password_env`                 | string         | (required)           | Env var name holding the password.                       |
| `arlo.session_cache_path`           | path           | (required)           | Persisted session token — survives restarts. The idle thumbnails live in a `thumbnails/` directory beside it (created owner-only at boot). |
| `arlo.watch_along_cert_sha256`      | hex string     | (unset)              | Pin the watch-along host's certificate (SHA-256) instead of verifying its chain; only when the log says the chain is not trusted (ADR 0007). |
| `arlo.app_version`                  | string         | `6.46.0`             | Arlo app version the daemon identifies as when fetching the stream of a view you started in the app (ADR 0007). |
| `arlo.mfa.kind`                     | `email`/`push`/`sms` | (required)     | Second factor. `email` and `push` run headless; `sms` prompts on stdin. |
| `arlo.mfa.host` / `.provider` / `.user` / `.password_env` / `.port` | strings | (port: 993) | IMAP mailbox for `kind = "email"`; without them the OTP is prompted on stdin. |
| `arlo.mfa.poll_interval_secs` / `.timeout_secs` | u64  | `3` / `120`          | Approval polling for `kind = "push"`.                    |
| `webrtc.ice_address_family`         | `dual`/`ipv4`  | `dual`               | ICE candidate gathering; `ipv4` when IPv6 to Arlo is broken. |
| `webrtc.live_stall_timeout_secs`    | u64            | `10` (floor 4)       | Seconds without inbound video before the live source is declared lost and the output returns to idle (ADR 0004). |
| `output.video_encoder`              | `auto`/`x264`/`va`/`vaapi`/`v4l2`/`nvenc` | `auto` | `auto` probes the host at boot (NVIDIA, Intel/AMD, V4L2, then x264); an explicit name fails the boot when it does not work (see Performance, ADR 0008). |
| `output.rtsp.bind`                  | `host:port`    | `0.0.0.0:8554`       | Embedded RTSP server.                                    |
| `output.metrics_bind`               | `host:port`    | `127.0.0.1:9090`     | Prometheus + healthchecks.                               |
| `output.admin_bind`                 | `host:port`    | `127.0.0.1:9091`     | `/admin/*` write API.                                    |
| `output.hls.dir`                    | path           | (off)                | Enables HLS: `<dir>/<stream_name>/index.m3u8` + segments (ADR 0006). Serve it with any web server. Keeps each camera's encoder running. |
| `output.hls.segment_secs`           | u32            | `2` (floor 1)        | Target segment length; segments cut at keyframes (every 2 s). |
| `output.hls.playlist_length`        | u32            | `10` (floor 3)       | Segments in the playlist; two more are kept on disk.      |
| `output.dash`                       | table          | (ignored)            | Parsed for compatibility, not supported: see ADR 0006.    |
| `[[cameras]]`                       | array          | `[]`                 | One block per camera — see example.                      |
| `cameras.codec_hint`                | `h264`/`h265`  | (auto)               | Skip first-stream codec detection.                       |
| `cameras.cooldown.debounce_secs`    | u64            | `60`                 | Hold-live debounce after last motion.                    |
| `cameras.cooldown.max_continuous_live` | u64         | `300`                | Hard cap on continuous live (battery protection); also caps a relayed app view when probing is off. |
| `cameras.cooldown.user_view_probe_secs` | u64        | `60`                 | How often a relayed app view is released for a few seconds to learn whether the app still views (our session keeps the camera streaming). `0` = never, cap only. |
| `cameras.cooldown.daily_live_budget` | u64           | `0`                  | `0` = unlimited; otherwise total live secs/day.          |
| `cameras.cooldown.budget_reset`     | `HH:MM`        | `00:00`              | Local-clock reset.                                       |

### Required environment variables

| Variable | Purpose |
|----------|---------|
| `ARLO_PASSWORD` (or whatever you put in `arlo.password_env`) | Arlo cloud password. |
| `ARLO_IMAP_PASSWORD` (or whatever you put in `arlo.mfa.password_env`) | IMAP password. |
| `STREAMER_ADMIN_TOKEN` | **Required.** Bearer token for `/admin/*`. |
| `RUST_LOG` | (optional) Tracing filter, e.g. `info,arlo_camera_streamer=debug`. |

## Usage

### First run: find your device ids

Arlo identifies cameras by opaque device ids. `list-devices` signs in with
your configuration, lists the account's cameras and doorbells, and prints a
ready-to-paste `[[cameras]]` block for each one not configured yet. It
starts no server and needs no admin token. It also completes the MFA
pairing, so the daemon's first start needs no OTP.

```bash
export ARLO_PASSWORD="…"
export ARLO_IMAP_PASSWORD="…"
arlo-camera-streamer list-devices --config /etc/arlo-streamer/streamer.toml
```

It also warns about configured `arlo_device_id` values the account does
not have, which is how typos show up. Logs go to stderr, the report to
stdout. The factor choice, the Docker form of this command, the log lines
that prove the pairing and what to do when a restart asks for a code again
are in the login runbook, [`.agents/skills/arlo-mfa-login/SKILL.md`](./.agents/skills/arlo-mfa-login/SKILL.md).

### Run the daemon

```bash
export ARLO_PASSWORD="…"
export ARLO_IMAP_PASSWORD="…"
export STREAMER_ADMIN_TOKEN="$(openssl rand -hex 32)"
arlo-camera-streamer --config /etc/arlo-streamer/streamer.toml
```

`run` is the default subcommand, so `arlo-camera-streamer run --config …`
is equivalent.

The process exits `0` on Ctrl-C / SIGTERM. It exits non-zero when it stops
on its own — the Arlo event bus ended, or a camera task ended or panicked —
after releasing every live session; run it under a restart policy
(`restart: on-failure` in Compose, `Restart=on-failure` in systemd).

### Docker run

```bash
docker run -d \
  --name arlo-camera-streamer \
  -p 8554:8554 -p 9090:9090 -p 9091:9091 \
  -v /etc/arlo-streamer:/etc/arlo-streamer:ro \
  -v /var/lib/arlo-streamer:/var/lib/arlo-streamer \
  -e ARLO_PASSWORD \
  -e ARLO_IMAP_PASSWORD \
  -e STREAMER_ADMIN_TOKEN \
  ghcr.io/ypno/arlo-camera-streamer-rs:v0.1.0
```

Add the encoder device your box has, and `video_encoder = "auto"` will
use it: `--device /dev/dri` for an Intel/AMD GPU, `--device /dev/video11`
on a Raspberry Pi 4 / Zero 2 / CM4, `--gpus all` with the NVIDIA
container toolkit for NVENC. A Raspberry Pi 5 has no H.264 hardware
encoder; it runs x264 in software.

### Frigate integration

Point Frigate at the bridge's RTSP endpoint:

```yaml
go2rtc:
  streams:
    front_door:
      - rtsp://arlo-camera-streamer:8554/front_door

cameras:
  front_door:
    ffmpeg:
      inputs:
        - path: rtsp://127.0.0.1:8554/front_door  # via go2rtc, or direct
          roles: [detect, record]
    detect:
      enabled: True
      fps: 5  # Arlo streams are low FPS — keep detection cheap.
```

Stream URL pattern: `rtsp://<host>:<rtsp.bind>/<cameras.stream_name>`.

### Operational endpoints

| Endpoint                              | Auth     | Purpose                                |
|---------------------------------------|----------|----------------------------------------|
| `GET /metrics` (port 9090)            | None     | Prometheus exposition.                 |
| `GET /healthz` (port 9090)            | None     | Liveness — `200 OK` while running.     |
| `GET /readyz`  (port 9090)            | None     | Ready iff started **and** Arlo bus connected. |
| `GET /admin/state` (port 9091)        | Bearer   | System snapshot (JSON); per-camera state as the camera task last published it, so a camera mid-negotiation still answers (`activating`). |
| `GET /admin/cameras/{id}` (port 9091) | Bearer   | Per-camera snapshot.                   |
| `POST /admin/cameras/{id}/wake` (port 9091) | Bearer | Inject a synthetic motion event; `429` within 30 s of the previous session's end. Each activation also costs 15 s of the daily budget. |
| `POST /admin/cameras/{id}/idle` (port 9091) | Bearer | Force the camera back to `Idle`. |

```bash
curl -H "Authorization: Bearer $STREAMER_ADMIN_TOKEN" \
     http://localhost:9091/admin/state | jq
```

### Selected metrics

| Metric                              | Type       | Labels                          |
|-------------------------------------|------------|---------------------------------|
| `streamer_arlo_connected`           | gauge      | —                               |
| `streamer_camera_state`             | gauge      | `camera`, `state`               |
| `streamer_state_transitions_total`  | counter    | `camera`, `from`, `to`, `signal`|
| `streamer_motion_events_total`      | counter    | `camera`, `outcome`             |
| `streamer_budget_decisions_total`   | counter    | `camera`, `decision`            |
| `streamer_splice_attempts_total`    | counter    | `camera`, `outcome`             |
| `streamer_splice_latency_ms`        | histogram  | `camera`, `outcome`             |
| `streamer_retries_total`            | counter    | `camera`                        |

## Operational warnings

- **Disable the Arlo mobile app's motion *recording*** for every
  camera exposed here. Arlo's cloud-recording job competes with our
  `start_stream` and will delay or block live activations.
- **Battery drain is real.** A `daily_live_budget` of 0 means no cap.
  Start with `300–600` secs/day per camera until you've measured.
- **MFA via IMAP is one-shot per cold-start.** Once the session token
  in `session_cache_path` is valid, the IMAP poll is dormant. If you
  rotate the Arlo password the cache is invalidated and IMAP MFA runs
  again — make sure that mailbox is reachable.
- **RTSP latency** is typically 2–4 s during live, dominated by the
  Arlo cloud's H.264 chunk size, not by us.
- **Splice strategy is a seamless `input-selector`.** Idle↔Live
  transitions happen inside one persistent RTSP pipeline (both branches
  decoded to raw and re-encoded by a single downstream encoder), so
  connected clients — VLC *and* Frigate — see one continuous stream
  with no reconnect. See [ADR 0003](./docs/adr/0003-seamless-input-selector-splice.md)
  (supersedes ADR 0001). The cost is an always-on encoder — see
  [Performance](#performance).

## Performance

Measured on an Intel i5-6500T (4 cores, Skylake) with **software**
`x264enc` at 720p15, one camera:

| State                            | CPU (share of one core) |
|----------------------------------|-------------------------|
| No RTSP client connected         | ~0%                     |
| Idle stream, client connected    | ~0.6 core               |
| Live stream                      | ~0.85 core              |

The dominant cost is the **always-on H.264 encoder**: it runs whenever a
client is connected (Frigate stays connected 24/7), so idle and live
cost almost the same — live only adds the decode (~0.25 core). CPU
therefore scales with the number of **connected** cameras, not with how
many are live. On a 4-core box that is a ceiling of roughly **6 idle /
4–5 concurrently live** cameras.

For more cameras, let the GPU encode. With the default
`[output] video_encoder = "auto"` the daemon probes the host at boot and
takes the first encoder that works, in this order:

| Backend | Hardware | Element | Needs |
|---|---|---|---|
| `nvenc` | NVIDIA | `nvh264enc` | NVIDIA driver, container toolkit (`--gpus all`) |
| `va` | Intel / AMD | `vah264enc` | `/dev/dri`, VA drivers (in the image) |
| `vaapi` | Intel / AMD, older stacks | `vaapih264enc` | same as `va` |
| `v4l2` | Raspberry Pi 4 / Zero 2 / CM4 | `v4l2h264enc` | `/dev/video11` |
| `x264` | any CPU | `x264enc` | nothing; ~0.6 core per camera |

A probe is a short dry run, so a plugin whose device is missing (VA
without a render node) is skipped rather than chosen. The log says which
one won: `video encoder (auto) encoder=va`. Name a backend explicitly to
pin it; then the boot fails if it does not work on that host. The
Raspberry Pi 5 has no H.264 hardware encoder and runs x264 (one or two
cameras at 720p15 are fine). A Google Coral accelerates Frigate's
detection, not this daemon's encoding. Details and the measured paths:
[ADR 0008](./docs/adr/0008-video-encoder-selection.md).

> The `gupnp … 1900: Address already in use` warnings at live start are
> harmless — libnice's UPnP probe colliding with local bridge
> interfaces; live streaming is unaffected.

## Testing

```bash
cargo test  --workspace --all-features      # unit + integration tests
cargo clippy --workspace -- -D warnings     # lints (zero-warning policy)
cargo fmt   --all -- --check                # formatting
cargo audit                                  # CVE scan (cargo-audit needed)
cargo deny  check                            # license + duplicate scan
```

`streamer-infra-media/tests/live_session.rs` runs real live sessions
through the production media stack with no camera: a second `webrtcbin`
plays Arlo's gateway and an RTSP client checks what a viewer sees
(idle → live → idle on one connection, a client joining mid-session, a
stalled source, a call lost during setup). It needs the GStreamer
runtime plugins (base, good, bad, ugly, libav, nice) and skips when one
is missing; set `STREAMER_REQUIRE_GST_IT=1` to make that a failure, as
CI does.

## CI/CD

GitHub Actions workflows live in [`.github/workflows/`](./.github/workflows/).
The `ci.yml` pipeline is staged: format → clippy → tests (with the GStreamer
integration tests) → coverage gate (86 %; raised deliberately, never above
what is held) and SonarCloud, with the rustdoc check beside the tests and
cargo-deny in parallel. A failed stage skips the costlier ones, docs-only
changes do not run it, the weekly schedule runs cargo-deny only, and
Renovate's PRs skip coverage and SonarCloud. After every gate, a push to
`main` whose `Cargo.toml` version has no release yet gets a GitHub release
tagged `v<version>`, then the container image for that version: built per
platform on native runners, each platform scanned with Trivy (a fixable
CRITICAL finding stops the release), pushed by digest, and tagged
`ghcr.io/ypno/arlo-camera-streamer-rs:v<version>`, `:latest` and
`:sha-<commit>`. A push without a version bump publishes nothing.

Releasing is therefore: bump `version` in `Cargo.toml`, move the
`[Unreleased]` changelog entries under the new version, merge.

## Security

- Secrets are read from env vars at boot (`fail fast` on missing).
- The `/admin/*` surface refuses to start without a non-empty
  `STREAMER_ADMIN_TOKEN`. Rotate it by rotating the env var and
  restarting. Rejected requests are logged with their route, never the
  presented token.
- Thumbnails are fetched over `https` only, with timeouts, a redirect
  limit, a 2 MiB cap and a JPEG magic check, and written owner-only
  beside the session cache. Presigned URLs, session ids and tokens are
  never logged.
- TLS termination for RTSP is the deployer's responsibility — bind
  `output.rtsp.bind` to `127.0.0.1` and front it with go2rtc / nginx
  if you need encrypted RTSP.
- See [SECURITY.md](./SECURITY.md) to report vulnerabilities.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md). One concern per PR; conventional
commits; >80 % coverage on new code; clippy clean.

## Changelog

See [CHANGELOG.md](./CHANGELOG.md) (Keep-a-Changelog, semver).

## License

MIT OR Apache-2.0 — at your option.
