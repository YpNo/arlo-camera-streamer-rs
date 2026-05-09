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
| Live  | Real H.264/H.265 from the camera | Awake |
| Cooling | Live continues for `debounce_secs` | Awake |
| Battery-protect | Idle frame returns | Sleeping (quota exhausted) |

The transition is driven by Arlo's **SSE event bus**: when the camera
fires a motion event, the daemon requests a live RTSPS stream, splices
it into the GStreamer pipeline, and reverts to idle once the camera
goes quiet.

## Architecture

Hexagonal Rust workspace, six crates:

```
streamer-domain        — pure types & port traits (no I/O)
streamer-app           — orchestrator + state machine + budget tracker
streamer-infra-arlo    — adapter for the rs-arlo client
streamer-infra-media   — GStreamer pipelines + embedded RTSP server
streamer-infra-ops     — /metrics, /healthz, /readyz, /admin/*
streamer-bin           — composition root (the daemon binary)
```

Read [`crates/streamer-domain/src/port.rs`](./crates/streamer-domain/src/port.rs)
to see the contracts. Two ADRs document the load-bearing decisions:

- [docs/adr/0001-factory-restart-splice.md](./docs/adr/0001-factory-restart-splice.md)
- [docs/adr/0002-rtsp-only-output-v1.md](./docs/adr/0002-rtsp-only-output-v1.md)

## Prerequisites

- **Rust** ≥ 1.95 (`rustup toolchain install 1.95.0`).
- **GStreamer 1.22+** with the standard plugin set:
  - `gstreamer1.0-plugins-base`
  - `gstreamer1.0-plugins-good`
  - `gstreamer1.0-plugins-bad`
  - `gstreamer1.0-plugins-ugly`
  - `gstreamer1.0-libav`
  - `gstreamer1.0-rtsp` (the rtsp-server library)
- An Arlo cloud account with at least one camera.
- An IMAP mailbox you can poll for the Arlo MFA OTP, or be ready to
  type the OTP on stdin (cold-start only).

## Installation

### From source

```bash
cargo build --release --package arlo-camera-streamer
sudo install -m 0755 target/release/arlo-camera-streamer /usr/local/bin/
```

### Docker (recommended)

A multi-stage `Dockerfile` is at the repo root. It runs as non-root,
drops `tini` as PID 1, and ships every GStreamer plugin needed.

```bash
docker build -t arlo-camera-streamer:dev .
```

## Configuration

Copy [`config/streamer.example.toml`](./config/streamer.example.toml)
to `/etc/arlo-streamer/streamer.toml` and edit the marked sections.
**Secrets never live in this file** — only the *names* of env vars
holding the values.

| Section / key                       | Type           | Default              | Notes                                                    |
|-------------------------------------|----------------|----------------------|----------------------------------------------------------|
| `arlo.email`                        | string         | (required)           | Arlo cloud account email.                                |
| `arlo.password_env`                 | string         | (required)           | Env var name holding the password.                       |
| `arlo.session_cache_path`           | path           | (required)           | Persisted session token — survives restarts.             |
| `arlo.mfa.kind`                     | `imap`/`stdin` | (required)           | Production: `imap`. First run / debugging: `stdin`.      |
| `arlo.mfa.host` / `.user` / `.password_env` / `.port` | strings | (port: 993) | IMAP creds when `kind = "imap"`.                |
| `output.rtsp.bind`                  | `host:port`    | `0.0.0.0:8554`       | Embedded RTSP server.                                    |
| `output.metrics_bind`               | `host:port`    | `127.0.0.1:9090`     | Prometheus + healthchecks.                               |
| `output.admin_bind`                 | `host:port`    | `127.0.0.1:9091`     | `/admin/*` write API.                                    |
| `[[cameras]]`                       | array          | `[]`                 | One block per camera — see example.                      |
| `cameras.cooldown.debounce_secs`    | u64            | `60`                 | Hold-live debounce after last motion.                    |
| `cameras.cooldown.max_continuous_live` | u64         | `300`                | Hard cap on continuous live (battery protection).        |
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

```bash
export ARLO_PASSWORD="…"
export ARLO_IMAP_PASSWORD="…"
export STREAMER_ADMIN_TOKEN="$(openssl rand -hex 32)"
arlo-camera-streamer --config /etc/arlo-streamer/streamer.toml
```

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
  arlo-camera-streamer:dev
```

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
| `GET /admin/state` (port 9091)        | Bearer   | System snapshot (JSON).                |
| `GET /admin/cameras/{id}` (port 9091) | Bearer   | Per-camera snapshot.                   |
| `POST /admin/cameras/{id}/wake` (port 9091) | Bearer | Inject a synthetic motion event. |
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
- **Splice strategy is factory-restart.** When transitioning Idle→Live
  the RTSP factory is rebuilt, which means Frigate sees a brief
  reconnect (~1 s). This is intentional — see ADR 0001.

## Testing

```bash
cargo test  --workspace --all-features      # unit + integration tests
cargo clippy --workspace -- -D warnings     # lints (zero-warning policy)
cargo fmt   --all -- --check                # formatting
cargo audit                                  # CVE scan (cargo-audit needed)
cargo deny  check                            # license + duplicate scan
```

GStreamer-dependent tests (`streamer-infra-media`) require system
GStreamer; CI runs them on Linux runners with the plugin set installed.

## CI/CD

GitHub Actions workflows live in [`.github/workflows/`](./.github/workflows/).
The `ci.yml` pipeline runs lint → typecheck → test → coverage gate
(>80 %) → security scan on every PR.

## Security

- Secrets are read from env vars at boot (`fail fast` on missing).
- The `/admin/*` surface refuses to start without a non-empty
  `STREAMER_ADMIN_TOKEN`. Rotate it by rotating the env var and
  restarting.
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
